"""
Washington DOH Residential Treatment Facility Inspection Scraper

Scrapes the DOH Facility Inspections & Investigations search, downloads
each inspection/investigation/enforcement PDF, extracts and parses the
text, then POSTs to the KOP inspections API for MySQL storage.

Source: https://doh.wa.gov/licenses-permits-and-certificates/facilities-z/
        facilities-inspections-and-investigations-search
        (facility type = Residential Treatment Facility License, id=2879)
"""

import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pdfplumber
import requests
from bs4 import BeautifulSoup

from inspection_api_client import post_facilities_to_api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── KOP API configuration ───────────────────────────────────────────
API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("INSPECTIONS_API_KEY", "CHANGE_ME")

# ── DOH search configuration ────────────────────────────────────────
SEARCH_URL = (
    "https://doh.wa.gov/licenses-permits-and-certificates/"
    "facilities-z/facilities-inspections-and-investigations-search"
)

# Facility types to scrape: (label, target_id, max_pages_to_check)
# max_pages is a safety cap — scraper stops early when it hits an empty page.
FACILITY_TYPES = [
    ("Residential Treatment Facility", 2879, 10),
    ("Behavioral Health Agency", 2869, 20),
]

PDF_CACHE_DIR = Path(__file__).parent / "wa_pdfs"

# KOP programs that are DOH-licensed (RTF or BHA). Juvenile detentions,
# CSD community facilities (Canyon View, Oakridge, etc.), and JR/JJR
# facilities (Echo Glen, Green Hill) are licensed by DCYF/DSHS, not DOH,
# so they're not on HELMS. Only keeping DOH-licensed entries here.
KOP_DOH_PROGRAMS = [
    "Daybreak Youth Services",
    "Newport Academy",  # covers Port Townsend, Seattle, Axis branches
    "Pearl Youth Residence",
    "Sea Mar Renacer",
    "Sundown M Ranch",
    "Tamarack Center",
    "Two Rivers Landing",
    # KOP programs not currently on DOH (newly licensed, DCYF-licensed, or
    # different licensure type): reSTART Life, Excelsior Youth Center,
    # Flying H Youth Ranch, Morning Star Boys Ranch, Ryther Child Center,
    # Smokey Point Behavioral Hospital, Center for Discovery.
    # Add them here if they later appear on DOH HELMS search.
]

# Tokens ignored for overlap scoring — they appear in many unrelated
# facility names and cause false matches (e.g. "Pierce County" matching
# "Pierce County Alliance Thurston County Drug Court").
_STOPWORDS = {
    "county", "washington", "wa", "inc", "llc", "pllc", "corp", "the",
    "and", "for", "services", "service", "health", "behavioral", "mental",
    "treatment", "center", "centers", "facility", "residence", "hospital",
    "clinic", "campus", "program", "programs", "agency", "care",
}


def _normalize(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _significant_tokens(s: str) -> set:
    return {t for t in _normalize(s).split() if len(t) >= 3 and t not in _STOPWORDS}


_KOP_TOKEN_SETS = [(p, _significant_tokens(p)) for p in KOP_DOH_PROGRAMS]


def matches_kop_program(facility_name: str) -> Optional[str]:
    """
    Return matching KOP program name, else None.

    Matches when all significant tokens of the (shorter) KOP name appear
    in the DOH facility name after stopword filtering. E.g. KOP "Newport
    Academy" matches DOH "Newport Academy - Seattle" (both tokens present)
    but does not match DOH "Sea Mar Turning Point Adult Residential"
    (sea mar is the only shared token, and it's generic).
    """
    if not facility_name:
        return None
    fac_tokens = _significant_tokens(facility_name)
    if not fac_tokens:
        return None

    best: Tuple[int, Optional[str]] = (0, None)
    for prog, prog_tokens in _KOP_TOKEN_SETS:
        if not prog_tokens:
            continue
        # Require ALL significant KOP tokens to appear in the facility name.
        if prog_tokens <= fac_tokens:
            score = len(prog_tokens)
            if score > best[0]:
                best = (score, prog)
    return best[1]

# Map HTML column classes to report categories stored on each report.
REPORT_CATEGORY_COLUMNS = [
    ("views-field-field-state-inspection-fullhtml", "state_inspection"),
    ("views-field-field-state-investigate-fullhtml", "state_investigation"),
    ("views-field-field-fed-inspection-fullhtml", "federal_inspection"),
    ("views-field-field-fed-investigate-fullhtml", "federal_investigation"),
    ("views-field-field-facility-enforcement", "enforcement"),
]


# ── PDF text parsing ────────────────────────────────────────────────

def extract_pdf_text(path: Path) -> str:
    """Extract all page text from a PDF."""
    try:
        with pdfplumber.open(path) as pdf:
            pages = [(p.extract_text() or "") for p in pdf.pages]
        return "\n".join(pages).strip()
    except Exception as e:
        logger.warning(f"  PDF extract failed for {path.name}: {e}")
        return ""


def parse_inspection_text(text: str) -> Dict:
    """
    Pull structured fields out of a WA DOH inspection/investigation PDF.

    The form places values above their labels, e.g.:
        ONGOING - ROUTINE 02/06/2024 GLD03
        Inspection Type Inspection Onsite Dates Inspector
        X2024-59 RTF.FS.00001084 Co-occurring Services,
        Inspection Number License Number RTF Service Types
    """
    parsed: Dict = {
        "inspection_number": "",
        "license_number": "",
        "inspection_type": "",
        "inspection_date": "",
        "inspector": "",
        "administrator": "",
        "service_types": "",
        "report_type": "",
        "report_date": "",
    }
    if not text:
        return parsed

    # Report type (first line after header usually)
    header_match = re.search(
        r"(Inspection Report|Investigation Report|Enforcement Report|Report)",
        text[:400],
    )
    if header_match:
        parsed["report_type"] = header_match.group(1)

    # Top-of-report date like "March 14, 2024"
    date_match = re.search(
        r"(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+\d{1,2},\s*\d{4}",
        text[:1500],
    )
    if date_match:
        parsed["report_date"] = date_match.group(0)

    # Inspection Number / License Number / RTF Service Types (line above labels)
    insp_match = re.search(
        r"([A-Z]?\d{4}-\d+)\s+(RTF\.FS\.\d+)\s+([^\n]*?)\n\s*Inspection Number\s+License Number",
        text,
    )
    if insp_match:
        parsed["inspection_number"] = insp_match.group(1).strip()
        parsed["license_number"] = insp_match.group(2).strip()
        parsed["service_types"] = insp_match.group(3).strip().rstrip(",")

    # Inspection Type / Dates / Inspector (line above those labels)
    type_match = re.search(
        r"([^\n]+?)\s+(\d{1,2}/\d{1,2}/\d{2,4})(?:\s*[-–]\s*\d{1,2}/\d{1,2}/\d{2,4})?\s+"
        r"(\S+)\s*\n\s*Inspection Type\s+Inspection Onsite Dates",
        text,
    )
    if type_match:
        parsed["inspection_type"] = type_match.group(1).strip()
        parsed["inspection_date"] = type_match.group(2).strip()
        parsed["inspector"] = type_match.group(3).strip()

    # Administrator (line before the "Agency Name and Address Administrator" label)
    admin_match = re.search(
        r"([^\n]+?)\n\s*Agency Name and Address\s+Administrator", text
    )
    if admin_match:
        line = admin_match.group(1).strip()
        # The line has: "<facility name>, <address> <zip> <administrator name>"
        # Pull the trailing administrator name: last 2-4 capitalized tokens.
        admin = re.search(
            r"([A-Z][a-zA-Z\.'-]+(?:\s+[A-Z][a-zA-Z\.'-]+){1,3})\s*$", line
        )
        if admin:
            parsed["administrator"] = admin.group(1).strip()

    return parsed


def extract_deficiencies(text: str) -> List[str]:
    """
    Very light deficiency extraction — grabs lines that look like citations
    (WAC codes, "Deficiency", numbered findings). Kept in raw form so the
    frontend can render them as a bullet list without losing context.
    """
    if not text:
        return []

    deficiencies = []
    # WAC or RCW regulatory citations followed by text
    for m in re.finditer(
        r"(WAC|RCW)\s*\d{3}-\d{2,3}-\d{3,4}[^\n]*(?:\n(?!(?:WAC|RCW|Inspector|Findings|Conclusion))[^\n]+){0,6}",
        text,
    ):
        snippet = re.sub(r"\s+", " ", m.group(0)).strip()
        if len(snippet) > 20:
            deficiencies.append(snippet)

    # De-dupe while preserving order
    seen = set()
    out = []
    for d in deficiencies:
        key = d[:120]
        if key not in seen:
            seen.add(key)
            out.append(d)
    return out


# ── Search page scraping ────────────────────────────────────────────

class WAInspectionScraper:
    def __init__(self, pdf_dir: Path = PDF_CACHE_DIR):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
        })
        self.pdf_dir = pdf_dir
        self.pdf_dir.mkdir(exist_ok=True)
        self.all_facilities: List[Dict] = []

    def fetch_page(self, facility_type_id: int, page: int) -> str:
        params = {
            "field_facility_type_target_id": facility_type_id,
            "page": page,
        }
        resp = self.session.get(SEARCH_URL, params=params, timeout=60)
        resp.raise_for_status()
        return resp.text

    def parse_page(self, html: str) -> List[Dict]:
        """
        Return one dict per facility row with PDF URLs grouped by category.
        """
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            return []

        facilities = []
        for tr in table.find_all("tr"):
            name_td = tr.find("td", class_="views-field-field-location-name")
            if not name_td:
                continue  # header row
            facility_name = name_td.get_text(strip=True)
            if not facility_name:
                continue

            license_td = tr.find("td", class_="views-field-field-plan-number")
            license_number = license_td.get_text(strip=True) if license_td else ""

            city_td = tr.find("td", class_="views-field-views-conditional-field")
            city = city_td.get_text(strip=True) if city_td else ""

            reports_by_category: Dict[str, List[Tuple[str, str]]] = {}
            for col_class, category in REPORT_CATEGORY_COLUMNS:
                col_td = tr.find("td", class_=col_class)
                if not col_td:
                    continue
                links = []
                for a in col_td.find_all("a", href=True):
                    url = a["href"].strip()
                    if ".pdf" not in url.lower():
                        continue
                    if url.startswith("/"):
                        url = "https://doh.wa.gov" + url
                    report_num = a.get_text(strip=True)
                    report_num = re.sub(r"\s*\(PDF\)\s*$", "", report_num)
                    links.append((report_num, url))
                if links:
                    reports_by_category[category] = links

            facilities.append({
                "facility_name": facility_name,
                "license_number": license_number,
                "city": city,
                "reports_by_category": reports_by_category,
            })

        return facilities

    def download_pdf(self, url: str) -> Optional[Path]:
        """Download to cache dir; return path. Skip if already cached."""
        filename = url.rsplit("/", 1)[-1]
        filename = re.sub(r"[^A-Za-z0-9._-]", "_", filename)
        dest = self.pdf_dir / filename
        if dest.exists() and dest.stat().st_size > 0:
            return dest
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            time.sleep(0.5)  # be polite
            return dest
        except requests.RequestException as e:
            logger.warning(f"  download failed {url}: {e}")
            return None

    def build_report(
        self,
        report_num: str,
        url: str,
        category: str,
    ) -> Optional[Dict]:
        pdf_path = self.download_pdf(url)
        if not pdf_path:
            return None

        text = extract_pdf_text(pdf_path)
        parsed = parse_inspection_text(text)
        deficiencies = extract_deficiencies(text)

        return {
            "report_id": report_num or pdf_path.stem,
            "report_date": parsed["report_date"] or parsed["inspection_date"],
            "raw_content": text,
            "content_length": len(text),
            "summary": (
                f"{category.replace('_', ' ').title()}"
                + (f" — {parsed['inspection_type']}" if parsed["inspection_type"] else "")
                + (f" ({parsed['inspection_date']})" if parsed["inspection_date"] else "")
            ).strip(),
            "categories": {
                "report_category": category,
                "report_type": parsed["report_type"],
                "inspection_number": parsed["inspection_number"] or report_num,
                "license_number": parsed["license_number"],
                "inspection_type": parsed["inspection_type"],
                "inspection_date": parsed["inspection_date"],
                "inspector": parsed["inspector"],
                "administrator": parsed["administrator"],
                "service_types": parsed["service_types"],
                "pdf_url": url,
                "deficiencies": deficiencies,
                "violation_count": len(deficiencies),
            },
        }

    def scrape(self) -> List[Dict]:
        logger.info("Starting WA DOH scrape")
        all_rows: List[Dict] = []

        for label, ftype_id, max_pages in FACILITY_TYPES:
            logger.info(f"=== {label} (type={ftype_id}) ===")
            collected = 0
            for page in range(max_pages):
                try:
                    html = self.fetch_page(ftype_id, page)
                except requests.RequestException as e:
                    logger.error(f"  page {page} fetch failed: {e}")
                    continue
                rows = self.parse_page(html)
                if not rows:
                    logger.info(f"  page {page + 1}: no more results, stopping")
                    break
                for r in rows:
                    r["facility_type_label"] = label
                logger.info(f"  page {page + 1}: {len(rows)} facilities")
                all_rows.extend(rows)
                collected += len(rows)
                time.sleep(1)
            logger.info(f"  total {label}: {collected}")

        # Filter to KOP programs only
        filtered: List[Dict] = []
        for r in all_rows:
            match = matches_kop_program(r["facility_name"])
            if match:
                r["kop_match"] = match
                filtered.append(r)
            else:
                logger.debug(f"  skipping (no KOP match): {r['facility_name']}")

        logger.info(
            f"Total DOH facilities: {len(all_rows)} — "
            f"matched to KOP list: {len(filtered)}"
        )
        for r in filtered:
            logger.info(f"  ✓ {r['facility_name']} → KOP: {r['kop_match']}")

        for i, row in enumerate(filtered, start=1):
            name = row["facility_name"]
            logger.info(f"[{i}/{len(filtered)}] {name} ({row['license_number']})")

            reports: List[Dict] = []
            for category, links in row["reports_by_category"].items():
                for report_num, url in links:
                    report = self.build_report(report_num, url, category)
                    if report:
                        reports.append(report)

            administrator = ""
            for r in reports:
                admin = r["categories"].get("administrator")
                if admin:
                    administrator = admin
                    break

            facility_info = {
                "facility_name": name,
                "program_name": row["license_number"],
                "program_category": row.get("facility_type_label", "Residential Treatment Facility"),
                "full_address": row["city"],
                "phone": "",
                "bed_capacity": "",
                "executive_director": administrator,
                "license_exp_date": "",
                "relicense_visit_date": "",
                "action": "",
            }

            self.all_facilities.append({
                "facility_info": facility_info,
                "reports": reports,
            })
            logger.info(f"  {len(reports)} reports")

        logger.info(f"Scraping complete: {len(self.all_facilities)} facilities")
        return self.all_facilities


# ── API posting ─────────────────────────────────────────────────────

def save_to_api(facilities: List[Dict]) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="WA",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main():
    scraper = WAInspectionScraper()
    facilities = scraper.scrape()

    if not facilities:
        logger.warning("No facilities scraped")
        return

    total_reports = sum(len(f["reports"]) for f in facilities)
    logger.info(
        f"Scraped {len(facilities)} facilities, {total_reports} reports "
        "— posting to API"
    )
    if save_to_api(facilities):
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed — check logs above")


if __name__ == "__main__":
    main()
