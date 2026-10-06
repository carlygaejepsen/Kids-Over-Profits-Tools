"""
North Carolina MHLCS Public Records Scraper

Loads the starter NC youth facilities workbook, matches each licensed facility
to the public records directory on the NC DHHS MHLCS site, and OCRs every
inspection/report PDF linked from the facility page before posting the
normalized facility/report payloads to the Kids Over Profits inspections API.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
try:
    from curl_cffi import requests as cf_requests
    from curl_cffi.requests.exceptions import RequestException as CurlRequestException
    _HAVE_CURL_CFFI = True
except ImportError:
    cf_requests = None
    _HAVE_CURL_CFFI = False
from bs4 import BeautifulSoup
from pdf2image import convert_from_bytes
import pytesseract
from openpyxl import load_workbook

from inspection_api_client import post_facilities_to_api
from kop_paths import kop_repo_dir, report_cache_dir
from scraper_state import load_state, merge_new_ids, save_state, seen_from_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("KOP_DATA_API_KEY", "CHANGE_ME")
STATE_FILE = Path(os.getenv("NC_STATE_FILE", ".nc_state.json"))

WORKBOOK_FALLBACKS = [
    Path.cwd() / "nc_youth_facilities.xlsx",
    Path.cwd() / "nc_youth_facilities.csv",
    Path(__file__).with_name("nc_youth_facilities.xlsx"),
    Path(__file__).with_name("nc_youth_facilities.csv"),
    kop_repo_dir() / "nc_youth_facilities.xlsx",
    kop_repo_dir() / "nc_youth_facilities.csv",
]

RESULTS_URL = "https://info.ncdhhs.gov/dhsr/mhlcs/sods/results.asp"

PDF_CACHE_DIR = report_cache_dir("NC_PDF_CACHE", "nc_pdfs", Path(".nc_pdf_cache"))
OCR_CACHE_DIR = report_cache_dir("NC_OCR_CACHE", "nc_ocr", Path(".nc_ocr_cache"))

TESSERACT_CMD = os.getenv("TESSERACT_CMD")
POPPLER_PATH = os.getenv("POPPLER_PATH")
OCR_DPI = int(os.getenv("NC_OCR_DPI", "175"))
if TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

if POPPLER_PATH:
    logger.info("Using POPPLER_PATH=%s", POPPLER_PATH)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) KOP-NC-scraper"

# Network errors to swallow-and-continue. curl_cffi raises its own exception
# hierarchy, so include it when available.
NETWORK_ERRORS = (requests.RequestException,) + (
    (CurlRequestException,) if _HAVE_CURL_CFFI else ()
)


def make_session():
    """Build the HTTP session for the Cloudflare-protected NC DHHS site.

    info.ncdhhs.gov sits behind Cloudflare, which 403-blocks plain requests
    regardless of User-Agent (it checks the TLS/JA3 fingerprint). curl_cffi with
    Chrome impersonation presents a real browser fingerprint and passes, so use
    it when available. Without it, fall back to requests — the run will then 403
    with a clear hint to install curl_cffi.
    """
    if _HAVE_CURL_CFFI:
        # Don't set a custom User-Agent: impersonation installs matching browser
        # headers, and a contradicting UA would re-trip Cloudflare.
        return cf_requests.Session(impersonate="chrome")
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    return session


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    if not text or text.lower() == "none":
        return ""
    return re.sub(r"\s+", " ", text)


def norm(value: object) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    for token in (" llc", " inc", " ltd", " company", " corporation", " corp", " llp", " pc"):
        text = text.replace(token, " ")
    return re.sub(r"\s+", " ", text).strip()


def format_date(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    for pattern in ("%d-%b-%y", "%d-%b-%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, pattern).strftime("%m/%d/%Y")
        except ValueError:
            continue
    return text


def find_source_file(explicit_path: Optional[str]) -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if path.exists():
            return path
        raise FileNotFoundError(path)

    for candidate in WORKBOOK_FALLBACKS:
        if candidate.exists():
            return candidate.resolve()

    raise FileNotFoundError("Could not find nc_youth_facilities.xlsx. Pass --input or set NC_SOURCE_FILE.")


def load_workbook_rows(source_file: Path) -> List[Dict[str, object]]:
    if source_file.suffix.lower() == ".csv":
        import csv

        with source_file.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]

    workbook = load_workbook(source_file, read_only=True, data_only=True)
    worksheet = workbook[workbook.sheetnames[0]]
    rows = worksheet.iter_rows(values_only=True)

    try:
        headers = [clean_text(value) for value in next(rows)]
    except StopIteration:
        return []

    records: List[Dict[str, object]] = []
    for raw_row in rows:
        record: Dict[str, object] = {}
        for header, value in zip(headers, raw_row):
            if header:
                record[header] = value
        records.append(record)
    return records


def group_workbook_rows(rows: Iterable[Dict[str, object]]) -> Dict[str, List[Dict[str, object]]]:
    grouped: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        license_number = clean_text(row.get("License #"))
        if license_number:
            grouped[license_number].append(row)
    return grouped


def pick_representative_row(rows: List[Dict[str, object]]) -> Dict[str, object]:
    def completeness(row: Dict[str, object]) -> int:
        return sum(1 for value in row.values() if clean_text(value))

    return max(rows, key=completeness)


def fetch_directory(session: requests.Session) -> List[Dict[str, str]]:
    response = session.get(RESULTS_URL, timeout=60)
    if response.status_code == 403 and not _HAVE_CURL_CFFI:
        raise RuntimeError(
            "NC DHHS (info.ncdhhs.gov) is behind Cloudflare and returned 403. "
            "Install curl_cffi to pass the browser check:  pip install curl_cffi"
        )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    entries: List[Dict[str, str]] = []
    for anchor in soup.select('a[href*="facility.asp?fid="]'):
        row = anchor.find_parent("tr")
        if not row:
            continue
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
        if len(cells) < 4:
            continue
        href = urljoin(RESULTS_URL, anchor.get("href", ""))
        fid_match = re.search(r"fid=(\d+)", href)
        if not fid_match:
            continue
        # The directory is one table per county, under <h3>Alamance County</h3>.
        county_heading = row.find_previous("h3")
        county = re.sub(r"\s+County$", "", county_heading.get_text(" ", strip=True)) if county_heading else ""
        entries.append(
            {
                "fid": fid_match.group(1),
                "name": cells[0],
                "address": cells[1],
                "city": cells[2],
                "zip": cells[3],
                "county": county,
                "url": href,
            }
        )
    return entries


STREET_WORDS = {
    "street": "st", "road": "rd", "drive": "dr", "avenue": "ave", "av": "ave", "lane": "ln", "court": "ct",
    "circle": "cir", "place": "pl", "boulevard": "blvd", "highway": "hwy", "parkway": "pkwy", "terrace": "ter",
    "trail": "trl", "north": "n", "south": "s", "east": "e", "west": "w",
}


def norm_street(value: object) -> str:
    """'723 North Fisher Street, Suite 2' -> '723 n fisher st'."""
    text = re.sub(r"[^a-z0-9 ]+", " ", clean_text(value).lower())
    text = re.split(r"\b(?:suite|ste|unit|apt|bldg|building|room)\b", text)[0]
    return " ".join(STREET_WORDS.get(word, word) for word in text.split())


def same_street(a: str, b: str) -> bool:
    """Same house number and nearly the same street name."""
    if not a or not b:
        return False
    num_a, _, rest_a = a.partition(" ")
    num_b, _, rest_b = b.partition(" ")
    if not re.match(r"^\d", num_a) or num_a != num_b:
        return False
    return rest_a == rest_b or SequenceMatcher(None, rest_a, rest_b).ratio() >= 0.8


def same_name(a: str, b: str) -> bool:
    """Name match without the old loose substring rule: a town name or a
    company name alone inside another facility's name is not a match (that
    is how 'Clinton' matched an adult day centre in Clinton)."""
    if not a or not b:
        return False
    if a == b:
        return True
    shorter, longer = sorted((a, b), key=len)
    if len(shorter.split()) >= 2 and len(shorter) >= 10 and re.search(rf"\b{re.escape(shorter)}\b", longer):
        # The longer name may only add a home number or a site word, never a
        # different home of the same company ("Falcon Crest 2" vs "Falcon Crest 3").
        extra = re.sub(rf"\b{re.escape(shorter)}\b", " ", longer).split()
        return not any(word.isdigit() for word in extra) and len(extra) <= 2
    return SequenceMatcher(None, a, b).ratio() >= 0.92


def score_match(record: Dict[str, object], entry: Dict[str, str]) -> float:
    """0 = not this facility. A match needs the street address (house number
    and street) in the same town or zip, or the facility's own name in the
    same town or zip. County must agree when both sides have one."""
    county = norm(record.get("County ") or record.get("County"))
    if county and entry.get("county") and norm(entry["county"]) != county:
        return 0.0

    entry_street = norm_street(entry["address"])
    entry_city = norm(entry["city"])
    entry_zip = clean_text(entry["zip"])[:5]
    entry_name = norm(entry["name"])

    # Only the site address counts; the "Facility" (mailing) address is often a
    # PO box or the company's office, shared by all its homes.
    prefix = "Site" if clean_text(record.get("Site Address")) else "Facility"
    street = norm_street(record.get(f"{prefix} Address"))
    city = norm(record.get(f"{prefix} City"))
    zip_code = clean_text(record.get(f"{prefix} Zip"))[:5]
    if not ((city and city == entry_city) or (zip_code and zip_code == entry_zip)):
        return 0.0

    names = [norm(record.get("DBA Name")), norm(record.get("Name of Licensee Legal Name"))]
    name_hit = same_name(names[0], entry_name) or bool(names[1] and names[1] == entry_name)
    if same_street(street, entry_street):
        return 100.0 if name_hit else 95.0
    return 85.0 if name_hit else 0.0


def match_candidates(record: Dict[str, object], entries: List[Dict[str, str]], limit: int = 3) -> List[Tuple[float, Dict[str, str]]]:
    """Directory entries that can be this license, best first. Each one is
    still checked against the program codes on its page before it is used."""
    scored = [(score_match(record, entry), entry) for entry in entries]
    scored = [item for item in scored if item[0] > 0]
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[:limit]


# Program codes (10A NCAC 27G) whose rules are written for children or adolescents.
# The page cuts Services at about 75 characters ("...Summer Developmental Day
# Services for"), so the code decides, not the wording. .4100 (parents in
# recovery, their children living with them) is a program for adults.
YOUTH_PROGRAM_CODES = {
    "27G.1300", "27G.1400", "27G.1700", "27G.1800", "27G.1900", "27G.2200", "27G.5200", "27G.5600B", "27G.5600D",
}
YOUTH_AGE_VALUES = {"MINOR", "MIN_ADL", "C&ADOL", "CHILD", "ADOL", "CHILDREN"}
YOUTH_WORDS = re.compile(r"\b(child|children|adolescents?|minors?|youth|juveniles?)\b", re.I)


def parse_program_rows(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Every row of the facility page's program table (Program code,
    Services, Age, Facility Type, Disability Category)."""
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        header = [cell.get_text(" ", strip=True).lower() for cell in rows[0].find_all(["td", "th"])]
        if not header or not header[0].startswith("program code"):
            continue
        programs = []
        for row in rows[1:]:
            cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
            if len(cells) < 5 or not cells[0]:
                continue
            programs.append(
                {"code": cells[0], "services": cells[1], "age": cells[2], "facility_type": cells[3], "disability": cells[4]}
            )
        return programs
    return []


def program_serves_minors(program: Dict[str, str]) -> bool:
    if program.get("code") in YOUTH_PROGRAM_CODES:
        return True
    if clean_text(program.get("age")).upper() in YOUTH_AGE_VALUES:
        return True
    # "...for Individuals with Substance Abuse Disorders and their Children" is a
    # program for parents; their children live there with them.
    services = re.sub(r"\band their children\b", "", clean_text(program.get("services")), flags=re.I)
    return bool(YOUTH_WORDS.search(services))


def serves_minors(programs: List[Dict[str, str]], workbook_rows: Iterable[Dict[str, object]] = ()) -> bool:
    """True when the facility page lists a program for children or
    adolescents, or the licence workbook marks the same program code as
    taking minors (Age Code MINOR / MIN_ADL; the page's Age column is mostly
    blank). Adult-only facilities are never posted."""
    if any(program_serves_minors(program) for program in programs):
        return True
    page_codes = {program["code"] for program in programs}
    for row in workbook_rows:
        if clean_text(row.get("Program Code")) in page_codes and clean_text(row.get("Age Code")).upper() in YOUTH_AGE_VALUES:
            return True
    return False


def parse_facility_metadata(soup: BeautifulSoup) -> Dict[str, object]:
    facility_heading = soup.find("h3")
    facility_name = facility_heading.get_text(" ", strip=True) if facility_heading else ""

    # Describe the facility by its program for minors when it has several.
    programs = parse_program_rows(soup)
    main_program = next((program for program in programs if program_serves_minors(program)), programs[0] if programs else {})
    services = main_program.get("services", "")
    facility_type = main_program.get("facility_type", "")
    disability_category = main_program.get("disability", "")

    contact_text = soup.get_text(" ", strip=True)
    contact_match = re.search(r"In Care of:\s*(.*?)\s*Phone:\s*\(?([0-9\-\)\(\s]+)", contact_text)
    contact_name = clean_text(contact_match.group(1)) if contact_match else ""
    phone = clean_text(contact_match.group(2)) if contact_match else ""

    address_match = re.search(
        r"Facility Address\s*(.*?)\s*Mailing Address",
        contact_text,
    )
    facility_address = clean_text(address_match.group(1)) if address_match else ""

    mailing_match = re.search(r"Mailing Address\s*(.*?)\s*Contact Information", contact_text)
    mailing_address = clean_text(mailing_match.group(1)) if mailing_match else ""

    county_match = re.search(r"([A-Za-z ]+ County)", contact_text)
    county = clean_text(county_match.group(1)) if county_match else ""

    return {
        "facility_name": facility_name,
        "services": services,
        "facility_type": facility_type,
        "disability_category": disability_category,
        "contact_name": contact_name,
        "phone": phone,
        "facility_address": facility_address,
        "mailing_address": mailing_address,
        "county": county,
        "programs": programs,
    }


def fetch_pdf_bytes(session: requests.Session, pdf_url: str) -> Optional[bytes]:
    """Download a report PDF into memory and archive it to the Drive folder.

    The Drive copy is written once and only read back when the source no
    longer serves the document: reading it would pull it into Drive for
    Desktop's local cache.
    """
    if not pdf_url:
        return None

    cache_name = Path(urlparse(pdf_url).path).name
    cache_path = PDF_CACHE_DIR / cache_name

    logger.info("  PDF download start: %s", pdf_url)
    start = time.monotonic()
    content: Optional[bytes] = None
    try:
        response = session.get(pdf_url, timeout=120)
        response.raise_for_status()
        content = response.content
    except NETWORK_ERRORS as exc:
        logger.warning("  PDF download failed for %s after %.1fs: %s", pdf_url, time.monotonic() - start, exc)
    if content is not None and not content.startswith(b"%PDF"):
        logger.warning("  Non-PDF response for %s", pdf_url)
        content = None

    if content is None:
        try:
            archived = cache_path.read_bytes()
        except OSError:
            return None
        if archived.startswith(b"%PDF"):
            logger.info("  using archived copy: %s (%d KB)", cache_name, len(archived) // 1024)
            return archived
        return None

    logger.info("  PDF downloaded: %s (%d KB, %.1fs)", cache_name, len(content) // 1024, time.monotonic() - start)
    try:
        already_archived = cache_path.stat().st_size == len(content)
    except OSError:
        already_archived = False
    if not already_archived:
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(content)
    return content


def cached_ocr_text(cache_key: str) -> Optional[str]:
    cache_path = OCR_CACHE_DIR / f"{cache_key}.txt"
    if not cache_path.exists():
        return None
    try:
        text = cache_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        logger.warning("  OCR cache unreadable (%s), re-running OCR: %s", exc, cache_key)
        try:
            cache_path.unlink()
        except OSError:
            pass
        return None
    logger.info("  OCR cache hit: %s", cache_key)
    return text


def ocr_pdf_bytes(pdf_bytes: bytes, cache_key: str) -> str:
    OCR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = OCR_CACHE_DIR / f"{cache_key}.txt"
    cached = cached_ocr_text(cache_key)
    if cached is not None:
        return cached

    logger.info("  OCR rasterize start: %s (dpi=%d, %d KB)", cache_key, OCR_DPI, len(pdf_bytes) // 1024)
    raster_start = time.monotonic()
    images = convert_from_bytes(pdf_bytes, dpi=OCR_DPI, poppler_path=POPPLER_PATH or None)
    logger.info("  OCR rasterized %d pages in %.1fs: %s", len(images), time.monotonic() - raster_start, cache_key)

    text_parts: List[str] = []
    ocr_start = time.monotonic()
    for page_num, image in enumerate(images, start=1):
        page_start = time.monotonic()
        try:
            page_text = pytesseract.image_to_string(image)
        except Exception as exc:
            logger.warning("  OCR failed for %s page %d: %s", cache_key, page_num, exc)
            break
        page_elapsed = time.monotonic() - page_start
        if page_elapsed > 15.0:
            logger.warning("  OCR slow page: %s page %d took %.1fs", cache_key, page_num, page_elapsed)
        else:
            logger.info("  OCR page %d/%d done (%.1fs, %d chars): %s", page_num, len(images), page_elapsed, len(page_text), cache_key)
        text_parts.append(page_text)

    logger.info("  OCR total %.1fs (%d pages): %s", time.monotonic() - ocr_start, len(text_parts), cache_key)

    text = "\n\n".join(part.strip() for part in text_parts if part).strip()
    if text:
        cache_path.write_text(text, encoding="utf-8")
    return text


def fetch_facility_page(session: requests.Session, facility_url: str) -> BeautifulSoup:
    response = session.get(facility_url, timeout=60)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def parse_reports(session: requests.Session, facility_url: str, fid: str, soup: Optional[BeautifulSoup] = None) -> List[Dict[str, object]]:
    if soup is None:
        soup = fetch_facility_page(session, facility_url)

    tables = soup.find_all("table")
    if len(tables) < 3:
        return []

    report_rows = tables[2].find_all("tr")
    reports: List[Dict[str, object]] = []
    pdf_rows = [row for row in report_rows[1:] if row.find("a", href=True)]
    logger.info("  fid=%s has %d report rows", fid, len(pdf_rows))

    for pdf_index, row in enumerate(report_rows[1:], start=1):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
        if len(cells) < 4:
            continue

        link = row.find("a", href=True)
        if not link:
            continue

        pdf_url = urljoin(facility_url, link.get("href", ""))
        pdf_name = Path(urlparse(pdf_url).path).name or f"{fid}-{cells[2]}"
        report_id = re.sub(r"[^A-Za-z0-9._-]+", "-", pdf_name)
        logger.info("  report %d/%d (fid=%s): %s", pdf_index, len(pdf_rows), fid, report_id)
        report_start = time.monotonic()
        # OCR text first: the PDF is only fetched when it has not been read yet.
        ocr_text = cached_ocr_text(report_id)
        if ocr_text is None:
            pdf_bytes = fetch_pdf_bytes(session, pdf_url)
            ocr_text = ocr_pdf_bytes(pdf_bytes, report_id) if pdf_bytes else ""
        logger.info("  report %d/%d done in %.1fs (%d OCR chars): %s", pdf_index, len(pdf_rows), time.monotonic() - report_start, len(ocr_text), report_id)

        report_date = cells[2]
        inspection_type = cells[0]
        document_type = cells[1]
        pages = cells[3]

        summary = f"{document_type} - {inspection_type}".strip(" -")

        reports.append(
            {
                "report_id": report_id,
                "report_date": report_date,
                "raw_content": ocr_text,
                "content_length": len(ocr_text),
                "summary": summary,
                "categories": {
                    "inspection_type": inspection_type,
                    "document_type": document_type,
                    "inspection_date": report_date,
                    "pages": pages,
                    "pdf_url": pdf_url,
                    "fid": fid,
                },
            }
        )

    return reports


def build_facility_payload(record: Dict[str, object], entry: Dict[str, str], metadata: Dict[str, object], reports: List[Dict[str, object]]) -> Dict[str, object]:
    program_codes = sorted({clean_text(row.get("Program Code")) for row in [record] if clean_text(row.get("Program Code"))})
    program_code_type = clean_text(record.get("Program Code Type"))
    facility_type = metadata.get("facility_type") or clean_text(record.get("Facility Type"))

    return {
        "facility_info": {
            "facility_name": metadata.get("facility_name") or clean_text(record.get("DBA Name")) or clean_text(record.get("Name of Licensee Legal Name")),
            "program_name": clean_text(record.get("License #")),
            "program_category": facility_type or program_code_type,
            "full_address": metadata.get("facility_address") or _join_address(record.get("Site Address"), record.get("Site City"), record.get("Site State"), record.get("Site Zip")),
            "phone": metadata.get("phone") or clean_text(record.get("Facility Contact Number")),
            "bed_capacity": clean_text(record.get("Beds")) or clean_text(record.get("Total Bed Count")),
            "executive_director": metadata.get("contact_name") or clean_text(record.get("Facility Contact Name")),
            "license_exp_date": format_date(record.get("Expiry Date")),
            "relicense_visit_date": "",
            "action": "Licensed",
        },
        "reports": reports,
        "source": {
            "fid": entry["fid"],
            "public_records_url": entry["url"],
            "county": metadata.get("county") or clean_text(record.get("County ")),
            "services": metadata.get("services"),
            "disability_category": metadata.get("disability_category"),
            "workbook_program_codes": program_codes,
            "programs": metadata.get("programs") or [],
        },
    }


def _join_address(*parts: object) -> str:
    values = [clean_text(part) for part in parts if clean_text(part)]
    return " ".join(values)


def scrape(source_file: Path, limit: Optional[int] = None, full: bool = False) -> Tuple[List[Dict[str, object]], Dict[str, List[str]], List[Dict[str, object]]]:
    session = make_session()

    rows = load_workbook_rows(source_file)
    grouped_rows = group_workbook_rows(rows)
    directory_entries = fetch_directory(session)
    logger.info("Loaded %d workbook rows (%d license groups) and %d directory entries", len(rows), len(grouped_rows), len(directory_entries))

    state = load_state(STATE_FILE)
    seen = {} if full else seen_from_state(state)
    new_ids: Dict[str, List[str]] = {}

    facilities: List[Dict[str, object]] = []
    unmatched: List[Dict[str, object]] = []

    # Strongest matches claim their directory entry first, and an entry goes to
    # one licence only: four A Place of My Own licences once all landed on the
    # one fid whose name held the company name.
    planned = []
    for license_number, group_rows in grouped_rows.items():
        representative = pick_representative_row(group_rows)
        candidates = match_candidates(representative, directory_entries)
        planned.append((candidates[0][0] if candidates else 0.0, license_number, group_rows, representative, candidates))
    planned.sort(key=lambda item: (-item[0], item[1]))

    claimed: Dict[str, str] = {}
    pages: Dict[str, BeautifulSoup] = {}
    skipped_adult: List[Dict[str, object]] = []

    for index, (_, license_number, group_rows, representative, candidates) in enumerate(planned, start=1):
        workbook_codes = {clean_text(row.get("Program Code")) for row in group_rows if clean_text(row.get("Program Code"))}
        entry = None
        metadata: Dict[str, object] = {}
        soup = None
        reason = "no directory entry at this address or name in the same town"
        for score, candidate in candidates:
            if candidate["fid"] in claimed:
                reason = f"directory entry fid={candidate['fid']} already belongs to {claimed[candidate['fid']]}"
                continue
            try:
                soup = pages.get(candidate["fid"]) or fetch_facility_page(session, candidate["url"])
            except NETWORK_ERRORS as exc:
                logger.warning("[%s/%s] failed to fetch facility page: %s", index, license_number, exc)
                reason = f"facility page failed: {exc}"
                continue
            pages[candidate["fid"]] = soup
            candidate_metadata = parse_facility_metadata(soup)
            page_codes = {program["code"] for program in candidate_metadata["programs"]}
            # The licence's program code must be on the page; otherwise this is a
            # different facility that shares a town and part of a name.
            if workbook_codes and not (workbook_codes & page_codes):
                reason = f"fid={candidate['fid']} ({candidate['name']}) runs {sorted(page_codes)}, licence is {sorted(workbook_codes)}"
                continue
            entry, metadata = candidate, candidate_metadata
            break

        if not entry:
            unmatched.append(
                {
                    "license_number": license_number,
                    "facility_name": clean_text(representative.get("DBA Name")) or clean_text(representative.get("Name of Licensee Legal Name")),
                    "site_address": clean_text(representative.get("Site Address")),
                    "county": clean_text(representative.get("County ")),
                    "reason": reason,
                }
            )
            continue

        fid = entry["fid"]
        claimed[fid] = license_number
        if not serves_minors(metadata["programs"], group_rows):
            logger.info("[%s/%s] %s (fid=%s) - adult-only programs %s, skipped", index, license_number, entry["name"], fid,
                        [program["code"] for program in metadata["programs"]])
            skipped_adult.append({"license_number": license_number, "fid": fid, "facility_name": entry["name"],
                                  "programs": metadata["programs"]})
            continue

        facility_url = entry["url"]
        seen_for_fid = seen.get(fid, set())

        logger.info("[%s/%s] %s (fid=%s) - reading reports", index, license_number, entry["name"], fid)
        facility_start = time.monotonic()
        try:
            reports = parse_reports(session, facility_url, fid, soup=soup)
        except NETWORK_ERRORS as exc:
            logger.warning("[%s/%s] failed to read reports: %s", index, license_number, exc)
            continue
        logger.info("[%s/%s] %s (fid=%s) - facility processing took %.1fs, %d reports parsed", index, license_number, entry["name"], fid, time.monotonic() - facility_start, len(reports))

        new_reports = [report for report in reports if report["report_id"] and report["report_id"] not in seen_for_fid]
        if not new_reports:
            logger.info("[%s/%s] %s - no new OCR reports", index, license_number, entry["name"])
            continue

        facility_payload = build_facility_payload(representative, entry, metadata, new_reports)
        facilities.append(facility_payload)
        new_ids[fid] = [report["report_id"] for report in new_reports]

        logger.info(
            "[%s/%s] %s - %d OCR reports",
            index,
            license_number,
            entry["name"],
            len(new_reports),
        )

        if limit is not None and len(facilities) >= limit:
            break

    if skipped_adult:
        logger.info("Skipped %d facilities whose programs are all for adults", len(skipped_adult))
    return facilities, new_ids, unmatched


def save_to_api(facilities: List[Dict[str, object]], api_url: str) -> bool:
    result = post_facilities_to_api(
        api_url=api_url,
        api_key=API_KEY,
        state="NC",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=180,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="North Carolina MHLCS public records OCR scraper")
    parser.add_argument("--input", help="Path to nc_youth_facilities.xlsx or a CSV export of it")
    parser.add_argument("--api-url", default=API_URL, help="Override the inspections write endpoint")
    parser.add_argument("--limit", type=int, help="Only post the first N matched facilities")
    parser.add_argument("--no-post", action="store_true", help="Parse, match, and OCR but do not post to the API")
    parser.add_argument("--full", action="store_true", help="Ignore the saved seen-state and reprocess all matched reports")
    args = parser.parse_args()

    source_file = find_source_file(args.input)
    logger.info("Using NC source file: %s", source_file)

    facilities, new_ids, unmatched = scrape(source_file, limit=args.limit, full=args.full)

    logger.info("Matched %d facilities with OCR reports", len(facilities))
    if unmatched:
        logger.info("Unmatched workbook rows: %d", len(unmatched))
        logger.info("First unmatched sample: %s", unmatched[0])

    if args.no_post:
        logger.info("--no-post supplied; skipping API write")
        return

    if not facilities:
        logger.info("No new OCR reports found")
        return

    logger.info("Posting %d facilities to the API", len(facilities))
    if save_to_api(facilities, api_url=args.api_url):
        state = load_state(STATE_FILE)
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
        logger.info("NC OCR scrape saved successfully")
    else:
        logger.error("API save failed")


if __name__ == "__main__":
    main()