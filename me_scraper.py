"""
Maine behavioral health organization licensing survey scraper.

Source: the state's licence lookup (ALMS Online), board 6706 "Behavioral
Health", Department of Health and Human Services, Division of Licensing and
Certification:

  https://www.pfr.maine.gov/almsonline/almsquery/searchcompany.aspx?board=6706

ASP.NET WebForms, plain requests with a session, no login:

  GET  searchcompany.aspx?board=6706      the form (one redirect, session cookie)
  POST the form's own action               every hidden input (the view state is
       split over __VIEWSTATE, __VIEWSTATE1 ... __VIEWSTATE10), the checked
       radios, the regulator and the search button. The "Services" list box is
       left out: it has no blank option and an empty value is an error page.
  GET  SearchResults.aspx?PageNumber=N     25 rows a page, each with a link
       ShowDetail.aspx?SearchResultToken=<hex>
  GET  ExportToCSV.aspx                    the whole result set (facility fields)
  GET  ShowDetail.aspx?SearchResultToken=  a licence: its Inspections table
       (Date, Type, Status) and under an inspection its "Inspection
       Communications" (Type, Sent/Received Date, Document links)
  GET  ShowInspectionEventCommDetail.aspx?SearchResultToken=   the PDF

Tokens are opaque and short-lived: nothing with a token is ever stored. The
saved page copies have their tokens blanked, and a report's link is the
search page.

The licence is the organization's, not a site's, and most organizations on
the board serve adults, so scope is the allowlist in me_scope.json (licence
number, name, why). Within an allowed organization every survey is posted:
one report per inspection row, with its documents (statement of deficiencies,
no-deficiency statement, plan of correction) attached. Documents are online
from late 2024; an older inspection is its date, type and outcome only.

The children's residential care facility licence (Office of Child and Family
Services) is a different licence and is not published anywhere.

The state adds documents to an inspection later (the plan of correction
arrives after the statement), so the state file keeps a content hash per
report and a report is posted again when its hash changes.
"""

import argparse
import csv
import gzip
import hashlib
import io
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pdfplumber
import requests
from bs4 import BeautifulSoup

from inspection_api_client import post_facilities_to_api
from kop_paths import report_cache_dir
from report_store import ReportStore
from scraper_state import load_state, save_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("KOP_DATA_API_KEY", "CHANGE_ME")
STATE_FILE = Path(os.getenv("ME_STATE_FILE", ".me_state.json"))
SCOPE_FILE = Path(os.getenv("ME_SCOPE_FILE", Path(__file__).parent / "me_scope.json"))

SEARCH_URL = "https://www.pfr.maine.gov/almsonline/almsquery/searchcompany.aspx?board=6706"
FIELD = "ctl00$ctl00$mainContent$mainContent$"
BOARD = "6706"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 0.8
TOKEN = re.compile(r"(SearchResultToken=)[0-9A-Za-z%+/=_-]+")

_reports: Optional[ReportStore] = None


def report_store() -> ReportStore:
    """The PDF store, made on first use (finding the Drive folder can ask the
    user to start Google Drive)."""
    global _reports
    if _reports is None:
        _reports = ReportStore("ME_PDF_CACHE", "me_pdfs", Path(__file__).parent / "me_pdfs")
    return _reports


# ── Fetch layer ──────────────────────────────────────────────────────────────


class MEClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call = 0.0
        self.results_url = ""

    def _pause(self) -> None:
        wait = REQUEST_GAP - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """One request with retries on timeouts, connection errors and 5xx."""
        delay = 2.0
        for attempt in range(1, 5):
            self._pause()
            try:
                response = self.session.request(method, url, timeout=120, **kwargs)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                if attempt == 4:
                    raise
                logger.warning(f"  {exc.__class__.__name__}; retrying in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue
            if response.status_code >= 500 and attempt < 4:
                logger.warning(f"  HTTP {response.status_code}; retrying in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue
            response.raise_for_status()
            return response
        raise RuntimeError("unreachable")

    @staticmethod
    def _form_fields(form) -> Dict[str, str]:
        """Hidden and text inputs plus the checked radios. Unchecked boxes and
        the list boxes are left out, as a browser leaves them out."""
        fields: Dict[str, str] = {}
        for tag in form.find_all("input"):
            name = tag.get("name")
            kind = (tag.get("type") or "text").lower()
            if not name:
                continue
            if kind in ("hidden", "text"):
                fields[name] = tag.get("value", "")
            elif kind == "radio" and tag.has_attr("checked"):
                fields[name] = tag.get("value", "on")
        return fields

    def search(self, extra: Optional[Dict[str, str]] = None) -> List[Dict[str, str]]:
        """Run the board search and return every result row, all pages.
        `extra` adds form fields by short name (scLicenseNo, scCompanyName,
        scShowHistory ...)."""
        page = self._request("GET", SEARCH_URL)
        soup = BeautifulSoup(page.text, "html.parser")
        form = soup.find("form")
        if form is None:
            raise RuntimeError(f"No search form at {SEARCH_URL}")
        data = self._form_fields(form)
        data[FIELD + "scRegulator"] = BOARD
        data[FIELD + "btnSearch"] = "Search"
        for key, value in (extra or {}).items():
            data[FIELD + key] = value
        response = self._request("POST", urljoin(page.url, form.get("action") or ""), data=data)
        self.results_url = response.url
        rows: List[Dict[str, str]] = []
        number = 1
        while True:
            found = parse_results(response.text, response.url)
            rows.extend(found)
            number += 1
            if not found or f"PageNumber={number}" not in response.text:
                break
            response = self._request("GET", urljoin(self.results_url, f"SearchResults.aspx?PageNumber={number}"))
        return rows

    def export_csv(self) -> str:
        """The last search's whole result set as CSV (same session)."""
        response = self._request("GET", urljoin(self.results_url or SEARCH_URL, "ExportToCSV.aspx"))
        response.encoding = response.encoding or "utf-8"
        return response.text

    def detail(self, url: str) -> str:
        response = self._request("GET", url)
        response.encoding = response.encoding or "utf-8"
        return response.text

    def document(self, url: str) -> Optional[bytes]:
        try:
            response = self._request("GET", url)
        except requests.RequestException as exc:
            logger.warning(f"  document download failed ({exc.__class__.__name__}); skipped for this run")
            return None
        if not response.content.startswith(b"%PDF"):
            logger.warning(f"  document is not a PDF ({response.headers.get('content-type', '?')}); skipped")
            return None
        return response.content


def one_line(value: str) -> str:
    value = (value or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def parse_results(html: str, base: str) -> List[Dict[str, str]]:
    """Result rows: Name, Number, Location, Profession, Status and the detail
    link (which carries a token: used at once, never stored)."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for link in soup.find_all("a", href=re.compile(r"ShowDetail\.aspx", re.I)):
        tr = link.find_parent("tr")
        if tr is None:
            continue
        cells = [one_line(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
        if len(cells) < 5:
            continue
        rows.append({
            "name": cells[0], "number": cells[1], "location": cells[2],
            "profession": cells[3], "status": cells[4],
            "detail_url": urljoin(base, link["href"]),
        })
    return rows


def parse_csv(text: str) -> Dict[str, Dict[str, str]]:
    """The CSV keyed by licence number. Two names can share a licence (Good
    Will Home Association and Good Will-Hinckley): the first row is kept and
    the other names go in `other_names`. The email column is never read."""
    licences: Dict[str, Dict[str, str]] = {}
    for row in csv.DictReader(io.StringIO(text.lstrip("\ufeff"))):
        number = one_line(row.get("License Number") or "")
        if not number:
            continue
        record = {
            "licence": number,
            "sort_name": one_line(row.get("Sort Name") or ""),
            "name": one_line(row.get("Mailing Name") or row.get("Sort Name") or ""),
            "profession": one_line(row.get("Profession") or ""),
            "status": one_line(row.get("Status") or ""),
            "street": ", ".join(x for x in (one_line(row.get(f"Address Line {i}") or "") for i in (1, 2, 3, 4)) if x),
            "city": one_line(row.get("City") or ""),
            "state": one_line(row.get("State") or ""),
            "zip": one_line(row.get("Zip") or ""),
            "county": one_line(row.get("County") or ""),
            "phone": one_line(row.get("Phone") or ""),
            "expires": us_date(row.get("Expiration Date") or ""),
            "authorities": one_line(row.get("Authorities") or ""),
        }
        if number in licences:
            known = licences[number]
            for name in (record["sort_name"], record["name"]):
                if name and name not in (known["sort_name"], known["name"]) and name not in known["other_names"]:
                    known["other_names"].append(name)
            if record["authorities"] and record["authorities"] not in known["authorities"]:
                known["authorities"] += "; " + record["authorities"]
            continue
        record["other_names"] = []
        if record["sort_name"] and record["sort_name"] != record["name"]:
            record["other_names"].append(record["sort_name"])
        licences[number] = record
    return licences


def us_date(value: str) -> str:
    match = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", value or "")
    if not match:
        return ""
    try:
        return datetime(int(match.group(3)), int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")
    except ValueError:
        return ""


# ── The licence page ─────────────────────────────────────────────────────────


def is_detail_page(html: str) -> bool:
    return 'id="detailPage"' in html and "License Number:" in html


def strip_tokens(html: str) -> str:
    """The page with every token blanked and the ASP.NET hidden state gone:
    what is saved, and what the change fingerprint is taken from."""
    html = TOKEN.sub(r"\1", html)
    return re.sub(r'(<input[^>]+type="hidden"[^>]+value=")[^"]{40,}(")', r"\1\2", html)


def page_fingerprint(html: str) -> str:
    return hashlib.sha1(strip_tokens(html).encode("utf-8")).hexdigest()


def direct_rows(table) -> List[Any]:
    rows = []
    for child in table.find_all(["tbody", "tr"], recursive=False):
        if child.name == "tr":
            rows.append(child)
        else:
            rows.extend(child.find_all("tr", recursive=False))
    return rows


def parse_detail(html: str, base: str = SEARCH_URL) -> Dict[str, Any]:
    """Licence attributes, services and the inspections with their documents.
    The communications table is nested in the row after its inspection, so
    only the inspection table's direct rows are read."""
    soup = BeautifulSoup(html, "html.parser")
    attributes: Dict[str, str] = {}
    first_group = soup.find("div", class_="Attributes")
    if first_group is not None:
        for row in first_group.find_all("div", class_="attributeRow", recursive=False):
            cells = row.find_all("div", class_="attributeCell", recursive=False)
            if len(cells) == 2:
                key = one_line(cells[0].get_text(" ", strip=True)).rstrip(":")
                if key:
                    attributes[key] = one_line(cells[1].get_text(", ", strip=True)).strip(", ")
    name_tag = soup.find("h2", class_="Name")

    services = []
    group = soup.find("div", class_="Authorities")
    if group is not None and group.find("table") is not None:
        for tr in direct_rows(group.find("table")):
            cells = tr.find_all("td", recursive=False)
            if len(cells) < 3:
                continue
            extra = {}
            if len(cells) > 3:
                for row in cells[3].find_all("div", class_="attributeRow"):
                    pair = row.find_all("div", class_="attributeCell", recursive=False)
                    if len(pair) == 2:
                        extra[one_line(pair[0].get_text(" ", strip=True)).rstrip(":")] = one_line(pair[1].get_text(" ", strip=True))
            services.append({
                "service": one_line(cells[0].get_text(" ", strip=True)),
                "issued": us_date(cells[1].get_text()),
                "status": one_line(cells[2].get_text(" ", strip=True)),
                **{k.lower().replace(" ", "_"): v for k, v in extra.items()},
            })

    inspections: List[Dict[str, Any]] = []
    unparsed: List[str] = []
    group = soup.find("div", class_="InspectionEvents")
    table = group.find("table") if group is not None else None
    if table is not None:
        for tr in direct_rows(table):
            cells = tr.find_all("td", recursive=False)
            if not cells:
                continue
            if len(cells) == 3:
                date = us_date(cells[0].get_text())
                if not date:
                    unparsed.append(f"inspection row with no date: {one_line(tr.get_text(' ', strip=True))[:80]!r}")
                    continue
                inspections.append({
                    "date": date,
                    "type": one_line(cells[1].get_text(" ", strip=True)),
                    "status": one_line(cells[2].get_text(" ", strip=True)),
                    "documents": [],
                })
                continue
            nested = cells[0].find("table")
            if nested is None or not inspections:
                unparsed.append(f"row not understood: {one_line(tr.get_text(' ', strip=True))[:80]!r}")
                continue
            for doc_row in direct_rows(nested):
                doc_cells = doc_row.find_all("td", recursive=False)
                if len(doc_cells) != 3:
                    continue
                links = doc_cells[2].find_all("a", href=True)
                if not links:
                    unparsed.append(f"communication with no document on {inspections[-1]['date']}")
                for link in links:
                    inspections[-1]["documents"].append({
                        "kind": one_line(doc_cells[0].get_text(" ", strip=True)),
                        "date": us_date(doc_cells[1].get_text()),
                        "title": one_line(link.get_text(" ", strip=True)),
                        "url": urljoin(base, link["href"]) if "SearchResultToken=" in link["href"] and not link["href"].endswith("SearchResultToken=") else "",
                    })
    return {
        "name": one_line(name_tag.get_text(" ", strip=True)) if name_tag else "",
        "attributes": attributes,
        "services": services,
        "inspections": inspections,
        "unparsed": unparsed,
    }


class PageStore:
    """Gzipped copies of each licence page, tokens blanked:
    <base>/<licence>/<YYYY-MM-DD>.html.gz, written when the content changed."""

    def __init__(self, base: Optional[Path] = None):
        self.base = base or report_cache_dir("ME_HTML_CACHE", "me_html", Path(__file__).parent / "me_html")

    def latest(self, licence: str) -> Optional[Path]:
        try:
            copies = sorted((self.base / licence).glob("*.html.gz"))
        except OSError:
            return None
        return copies[-1] if copies else None

    @staticmethod
    def read(path: Path) -> str:
        return gzip.decompress(path.read_bytes()).decode("utf-8")

    def save(self, licence: str, html: str, fingerprint: str, known: Optional[str]) -> bool:
        if not is_detail_page(html):
            raise ValueError(f"{licence}: not a licence page; not saved")
        if known is None:
            last = self.latest(licence)
            if last is not None:
                try:
                    known = page_fingerprint(self.read(last))
                except (OSError, ValueError) as exc:
                    logger.warning(f"  could not read {last}: {exc}")
        if known == fingerprint:
            return False
        folder = self.base / licence
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{datetime.now():%Y-%m-%d}.html.gz").write_bytes(
            gzip.compress(strip_tokens(html).encode("utf-8"), mtime=0))
        return True


# ── Documents ────────────────────────────────────────────────────────────────

KIND_ORDER = {"SOD WITH DEFICIENCIES": 0, "NO DEFICIENCIES SOD": 1, "PLAN OF CORRECTION": 2}
KIND_LABELS = {
    "SOD WITH DEFICIENCIES": "Statement of deficiencies",
    "NO DEFICIENCIES SOD": "No-deficiency statement",
    "PLAN OF CORRECTION": "Plan of correction",
}


def clean_text(value: str) -> str:
    """PDF text with the fonts' unmapped apostrophes and dashes repaired."""
    value = (value or "").replace("\ufffd", "'").replace("\xa0", " ")
    return value.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')


def extract_pdf(path: Path) -> Dict[str, Any]:
    """Each page's text and its tables (cells whole: the statement of
    deficiencies sets finding, plan and completion date side by side, and
    plain text extraction interleaves them). A page with no text is OCR'd."""
    pages: List[str] = []
    tables: List[List[List[List[str]]]] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            pages.append(clean_text(page.extract_text() or ""))
            page_tables = []
            try:
                for table in page.extract_tables():
                    page_tables.append([[clean_text(cell or "") for cell in row] for row in table])
            except Exception as exc:  # a malformed page
                logger.warning(f"  table extraction failed on a page of {path.name}: {exc}")
            tables.append(page_tables)
    ocr_pages: List[int] = []
    for index, text in enumerate(pages):
        if len(text.strip()) < 20:
            read = ocr_page(path, index + 1)
            if read.strip():
                text = pages[index] = clean_text(read)
                ocr_pages.append(index + 1)
        if len(text) >= 150 and english_score(text) < 0.12:
            # A scan that was fed in upside down (its text layer, or the OCR of
            # it, is nonsense): read the picture turned and keep that if it is
            # English.
            for turn in (180, 90, 270):
                read = ocr_page(path, index + 1, turn)
                if read.strip() and english_score(read) > english_score(text) + 0.1:
                    pages[index] = clean_text(read)
                    if index + 1 not in ocr_pages:
                        ocr_pages.append(index + 1)
                    break
    result: Dict[str, Any] = {"text": "\n".join(pages).strip(), "pages": pages, "tables": tables}
    if ocr_pages:
        result["ocr_pages"] = ocr_pages
    return result


COMMON_WORDS = {"the", "and", "of", "to", "in", "for", "was", "is", "that", "with", "on", "by", "this", "has", "not", "be",
                "are", "were", "as", "an", "or", "from", "at", "all", "each", "must", "which", "client", "records"}


def english_score(text: str) -> float:
    """Share of the words that are common English ones; upside-down or
    garbled text scores near zero."""
    words = re.findall(r"[a-z]{2,}", (text or "").lower())
    return sum(1 for w in words if w in COMMON_WORDS) / len(words) if words else 0.0


def ocr_page(path: Path, number: int, rotate: int = 0) -> str:
    try:
        import pytesseract
    except ImportError:
        logger.warning(f"  {path.name} has a scanned page and pytesseract is not installed")
        return ""
    tesseract = os.getenv("TESSERACT_CMD") or r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    if Path(tesseract).exists():
        pytesseract.pytesseract.tesseract_cmd = tesseract
    try:
        with pdfplumber.open(path) as pdf:
            image = pdf.pages[number - 1].to_image(resolution=200).original
        if rotate:
            image = image.rotate(rotate, expand=True)
        return pytesseract.image_to_string(image)
    except Exception as exc:
        logger.warning(f"  OCR failed for {path.name} page {number}: {exc}")
        return ""


SURVEY_NUMBER = re.compile(r"\b(20\d{2})\s*-\s*([A-Z]{2,5})\s*-\s*(\d{3,6})\b")
SOD_HEADER = re.compile(r"STATEMENT\s+OF\s+DEFICIENCIES", re.I)
COLUMN_HEADER = re.compile(r"^\s*Summary\s+Statement\s+of\s+Deficiencies", re.I)
NOT_MET = re.compile(r"This\s+(?:has|have)\s+not\s+been\s+met\s+as\s+evidenced\s+by\s*:?", re.I)
SECTION_LINE = re.compile(r"^\s*(SECTION\s+\d+[A-Z]?)\s*[.:]?\s*(.*)$", re.I)
PAGE_FOOTER = re.compile(r"^\s*(?:Page\s+\d+\s+of\s+\d+\b.*|\(Signature on each page\))\s*$", re.I)
IN_COMPLIANCE = re.compile(
    r"\bis\s+in\s+substantial\s+compliance\b|\bno\s+deficienc(?:y|ies)\b|\bin\s+compliance\s+with\b", re.I)
NOT_IN_COMPLIANCE = re.compile(r"is\s+not,?\s+in\s+substantial\s+compliance|out\s+of\s+compliance", re.I)

# Signs that a document is about a program for adults. Only an unmistakable
# statement counts; a document that also names children or youth is left
# unmarked.
ADULT_SIGNS = re.compile(
    r"\badults?\b|\bPNMI\s+Appendix\s+[EF]\b|\bAssertive\s+Community\s+Treatment\b|\bACT\s+team\b"
    r"|\bSection\s+17\b|\bSection\s+97\s+Appendix\s+[EF]\b|\bolder\s+adults?\b|\bOpioid\s+Treatment\s+Program\b"
    r"|\bMethadone\b|\bDriver\s+Education\s+and\s+Evaluation\b", re.I)
YOUTH_SIGNS = re.compile(
    r"\bchild(?:ren)?(?:'s)?\b|\byouths?\b|\badolescents?\b|\bminors?\b|\bjuveniles?\b|\bguardians?\b|\bparents?\b"
    r"|\bstudents?\b|\bSection\s+28\b|\bSection\s+65\b|\bAppendix\s+D\b|\bteen(?:s|agers?)?\b", re.I)

# The README's privacy rule: a date of birth, a named child, a record number.
PRIVATE_SIGNS = [
    # A date of birth is a value after the words; the rule's own wording ("identification
    # data, including name, address, telephone number and date of birth") is not one.
    ("date of birth", re.compile(
        r"\bD\.?O\.?B\b\.?\s*[:#-]?\s*(?:\d|[A-Z][a-z]{2,8}\s+\d)"
        r"|\bdate\s+of\s+birth\s*(?:is|was|:)?\s*[:#-]?\s*(?:\d|[A-Z][a-z]{2,8}\s+\d)"
        r"|\bborn\s+(?:on\s+)?(?:\d|[A-Z][a-z]{2,8}\s+\d)", re.I)),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("record number", re.compile(
        r"\b(?:MaineCare|Medicaid|medical\s+record|MRN|client\s+ID|member\s+ID|case)\s*(?:ID|No\.?|number|#)?\s*[:#]?\s*[A-Z]?\d{6,}\b",
        re.I)),
    ("named client", re.compile(
        r"\b(?:[Cc]lient|[Rr]esident|[Yy]outh|[Cc]hild|[Mm]inor|[Pp]atient|[Ss]tudent|[Cc]onsumer)\s+"
        r"(?:named\s+|name\s+is\s+|identified\s+as\s+)[A-Z][a-z]+")),
]


def privacy_hits(text: str) -> List[str]:
    return [label for label, pattern in PRIVATE_SIGNS if pattern.search(text or "")]


def survey_numbers(*texts: str) -> List[str]:
    """'2026-BHP-3436' from the header and the file name; a bare companion
    number after an ampersand ('3436 & 3438') takes the first one's prefix."""
    found: List[str] = []
    for text in texts:
        for match in SURVEY_NUMBER.finditer(text or ""):
            number = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
            if number not in found:
                found.append(number)
            tail = (text or "")[match.end():match.end() + 40]
            for extra in re.finditer(r"^\s*(?:&|and|,|/)\s*(?:Companion\s+)?(\d{3,6})\b(?!\s*-)", tail, re.I):
                more = f"{match.group(1)}-{match.group(2)}-{extra.group(1)}"
                if more not in found:
                    found.append(more)
    return found


def clean_noise(value: str) -> str:
    """A stray glyph the form's signature field leaves before or after a line."""
    return re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9)]+$", "", value or "").strip()


def header_field(text: str, label: str, stop: str) -> str:
    match = re.search(label + r"\s*:?\s*(.*?)\s*(?:" + stop + r"|\n|$)", text, re.I)
    return one_line(match.group(1)) if match else ""


def sod_rows(tables: List[List[List[List[str]]]]) -> List[List[str]]:
    """The statement's body rows across every page: [statement, plan,
    completion]. A page's table opens with the form's header rows on page
    one; rows after 'Summary Statement of Deficiencies' are body, and so is
    every row of a continuation page."""
    rows: List[List[str]] = []
    for page_tables in tables:
        for table in page_tables:
            body_from = 0
            is_sod = False
            for index, row in enumerate(table):
                cells = [c or "" for c in row]
                if any(COLUMN_HEADER.match(c) for c in cells):
                    body_from = index + 1
                    is_sod = True
                    break
            if not is_sod:
                widest = max((len(r) for r in table), default=0)
                # A continuation page: three columns and no form header.
                if widest != 3 or any(SOD_HEADER.search(c or "") for r in table for c in r):
                    continue
                if not rows:
                    continue
            for row in table[body_from:]:
                cells = [(c or "").strip() for c in row] + ["", "", ""]
                if any(cells[:3]):
                    rows.append(cells[:3])
    return rows


def strip_footer(text: str) -> str:
    return "\n".join(l for l in (text or "").split("\n") if not PAGE_FOOTER.match(l)).strip()


def parse_deficiencies(rows: List[List[str]]) -> List[Dict[str, str]]:
    """Split the statement column into deficiencies. Each one is a rule (under
    a 'SECTION n. TITLE' heading), 'This has not been met as evidenced by:',
    and the finding. A row's plan and completion date belong to the
    deficiency whose text that row starts in."""
    deficiencies: List[Dict[str, str]] = []
    current: Optional[Dict[str, Any]] = None
    section = ""
    pending_rule: List[str] = []
    mode = "rule"

    def start() -> Dict[str, Any]:
        nonlocal current, pending_rule, mode
        current = {"section": section, "rule_lines": pending_rule, "finding_lines": [], "plans": [], "dates": []}
        deficiencies.append(current)
        pending_rule = []
        mode = "finding"
        return current

    for statement, plan, completion in rows:
        row_owner: Optional[Dict[str, Any]] = current if mode == "finding" else None
        for raw in strip_footer(statement).split("\n"):
            line = raw.strip()
            if not line:
                continue
            heading = SECTION_LINE.match(line)
            if heading and (line.upper() == line or (len(line) < 90 and re.match(r"^Section\s+\d+[A-Z]?\.\s+[A-Z]", line))):
                section = one_line(f"{heading.group(1).upper()}. {heading.group(2)}").rstrip(". ")
                pending_rule = []
                mode = "rule"
                continue
            if NOT_MET.search(line):
                before = NOT_MET.split(line)[0].strip()
                if before:
                    pending_rule.append(before)
                if mode == "finding" and current is not None and not pending_rule:
                    # A second "not met" under the same rule: the rule text is
                    # the lines since the last finding ended, which this
                    # parser could not tell from the finding. Keep one entry.
                    continue
                start()
                if row_owner is None:
                    row_owner = current
                continue
            if mode == "rule" and pending_rule and IMPLICIT_FINDING.match(line):
                # The form version that has no "This has not been met" line:
                # the finding opens "Based on record review ...".
                start()
                if row_owner is None:
                    row_owner = current
                current["finding_lines"].append(line)
                continue
            if mode == "finding" and current is not None:
                # A new rule inside the same section starts with its letter or
                # number ("K. Right to ...", "1. The organization ..."), after
                # a finding has begun. It is held as the next rule until the
                # next "not met" claims it.
                if looks_like_rule_start(line) and current["finding_lines"]:
                    pending_rule = [line]
                    mode = "rule"
                    continue
                current["finding_lines"].append(line)
            else:
                pending_rule.append(line)
        if row_owner is None:
            row_owner = current
        if row_owner is not None:
            if strip_footer(plan):
                row_owner["plans"].append(strip_footer(plan))
            if strip_footer(completion):
                row_owner["dates"].append(one_line(strip_footer(completion)))
    if mode == "rule" and pending_rule and current is not None and looks_like_rule_start(pending_rule[0]):
        # A held "rule" that no finding claimed was finding text after all.
        current["finding_lines"].extend(pending_rule)
    out = []
    for d in deficiencies:
        finding = "\n".join(d["finding_lines"]).strip()
        finding = re.sub(r"^Finding\s*:\s*", "", finding)
        out.append({
            "section": d["section"],
            "rule_text": rule_text(d["rule_lines"]),
            "finding": paragraphs(finding),
            "plan": paragraphs("\n".join(d["plans"])),
            "completion_date": "; ".join(dict.fromkeys(d["dates"])),
        })
    return [d for d in out if d["finding"] or d["rule_text"]]


IMPLICIT_FINDING = re.compile(r"^Based\s+on\s+(?:record|observation|intake|interview|review|a\s|the\s)", re.I)
RULE_INTRO = re.compile(r"^.{0,200}?(?:Licensing\s+Rule\.|substantial\s+compliance[^.]*\.)\s*", re.I)


def rule_text(lines: List[str]) -> str:
    """The rule's wording, without the form's opening sentence ('X is not in
    substantial compliance with 10-144 CMR Ch. 123 ...') and its lead-in."""
    text = one_line(" ".join(lines))
    if re.search(r"substantial\s+compliance", text[:250], re.I):
        text = RULE_INTRO.sub("", text, count=1)
    return re.sub(r"^(?:The\s+following\s+requirements\s+were\s+not\s+met\s*:?\s*)", "", text, flags=re.I).strip()


def looks_like_rule_start(line: str) -> bool:
    """'K. Right to confidentiality.' or '3. The organization must ...':
    an outline marker followed by rule wording, not a dated finding."""
    match = re.match(r"^(?:[A-Z]|\d{1,2})\.\s+(?:\(?[a-z0-9]{1,3}\)\s*)?([A-Z][a-z]+)", line)
    if not match:
        return False
    return not re.match(r"^(?:[A-Z]|\d{1,2})\.\s+(?:On|In|Client|Record|Staff|Based|The\s+surveyor|During|Review)\b", line)


def paragraphs(text: str) -> str:
    """Join wrapped lines into paragraphs; a line that ends a sentence before
    a line starting a new one keeps its break."""
    lines = [l.strip() for l in (text or "").split("\n") if l.strip()]
    out: List[str] = []
    for line in lines:
        if out and not (re.search(r"[.:;?!]$", out[-1]) and re.match(r"^(?:[A-Z0-9(\"']|Finding)", line)
                        and (len(out[-1]) < 60 or re.match(r"^(?:On\s+\d|Finding|Based\s+on|\d+[.)]|[A-Z][.)]\s)", line))):
            out[-1] = out[-1] + " " + line
        else:
            out.append(line)
    return "\n".join(out)


def classify_document(kind: str, title: str, text: str) -> str:
    """'sod', 'no_deficiency', 'plan' or '' (not a licensing report)."""
    kind = (kind or "").upper()
    has_form = bool(SOD_HEADER.search(text))
    has_letter = bool(re.search(r"Plan\s+of\s+Correction|Statement\s+of\s+Deficienc|Licensing\s+and\s+Certification"
                                r"|substantial\s+compliance|Behavioral\s+Health\s+Organizations?\s+Licensing\s+Rule", text, re.I))
    if not (has_form or has_letter):
        return ""
    if kind == "PLAN OF CORRECTION":
        return "plan"
    if kind == "NO DEFICIENCIES SOD":
        return "no_deficiency"
    if kind == "SOD WITH DEFICIENCIES":
        return "sod"
    if re.search(r"\bPOC\b|plan\s+of\s+correction", title, re.I):
        return "plan"
    if NOT_IN_COMPLIANCE.search(text) or NOT_MET.search(text):
        return "sod"
    if IN_COMPLIANCE.search(text):
        return "no_deficiency"
    return ""


def parse_document(doc: Dict[str, str], extracted: Dict[str, Any]) -> Dict[str, Any]:
    """Header fields and, for a statement or a plan, its deficiencies."""
    text = extracted.get("text") or ""
    tables = extracted.get("tables") or []
    kind = classify_document(doc["kind"], doc["title"], text)
    form_at = SOD_HEADER.search(text)
    form_text = text[form_at.start():] if form_at else text
    survey_line = ""
    if form_at:
        # The line naming the survey sits within a few lines of the form title,
        # before or after the completion date ("Complaint Survey 2026-BHP-3436",
        # "Biennial Survey", "Desk Review"); the form's layout varies.
        for candidate in text[form_at.end():].split(chr(10))[:6]:
            line = clean_noise(one_line(candidate))
            if re.match(r"^(?:Name\s+of\s+Organization|Administrator|Date\s+Completed)", line, re.I):
                if re.match(r"^Name\s+of\s+Organization", line, re.I):
                    break
                continue
            if re.search(r"survey|review|visit|inspection|\d{4}-[A-Z]{2,5}-\d", line, re.I) and not SOD_HEADER.search(line):
                survey_line = line
                break
    letter = re.search(r"\bA\s+([A-Z][A-Za-z /-]{2,60}?)\s+(?:Survey\s+)?was\s+completed\s+for\s+your", text)
    survey_kind = survey_line or (one_line(letter.group(1)) if letter else "")
    site = header_field(form_text, r"(?:Residential\s+Site(?:\(s\))?(?:\s+for\s+complaint)?\s+as\s+applicable"
                                   r"|Residential\s+site\s+for\s+complaint\s+if\s+applicable)", r"Summary\s+Statement")
    site = site.strip(" .,;")
    if re.fullmatch(r"(?:n\s*/?\s*a\.?|none|not\s+applicable)?", site, re.I):
        site = ""
    parsed: Dict[str, Any] = {
        "doc_type": kind,
        "date_completed": us_date(header_field(form_text, r"Date\s+Completed", r"STATEMENT")),
        "survey_kind": clean_noise(re.sub(r"\s*(?:&\s*Companion\s*)?" + SURVEY_NUMBER.pattern, "", survey_kind).strip(" &,-")),
        "survey_numbers": survey_numbers(survey_line, doc["title"]),
        "is_complaint": bool(re.search(r"\bcomplaint\b", f"{survey_line} {doc['title']} {survey_kind}", re.I)),
        "site": site,
        "deficiencies": [],
        "ocr": bool(extracted.get("ocr_pages")),
    }
    if kind in ("sod", "plan"):
        parsed["deficiencies"] = parse_deficiencies(sod_rows(tables))
    return parsed


def adult_only(texts: List[str]) -> bool:
    joined = "\n".join(texts)
    return bool(ADULT_SIGNS.search(joined)) and not YOUTH_SIGNS.search(joined)


# ── Reports ──────────────────────────────────────────────────────────────────


def slug(value: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", (value or "").lower())).strip("-") or "inspection"


def plural(count: int, word: str, many: str = "") -> str:
    return f"{count} {word if count == 1 else (many or word + 's')}"


def assign_report_ids(inspections: List[Dict[str, Any]]) -> None:
    """<YYYYMMDD>-<type slug>, '-2' for a second of the same type on one day.
    The page lists newest first; numbering runs oldest first so a later
    inspection never renumbers an earlier one."""
    used: Counter = Counter()
    for inspection in reversed(inspections):
        base = f"{inspection['date'].replace('-', '')}-{slug(inspection['type'])}"
        used[base] += 1
        inspection["report_id"] = base if used[base] == 1 else f"{base}-{used[base]}"


def ordered_documents(documents: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return sorted(documents, key=lambda d: (d["date"] or "9999", KIND_ORDER.get(d["kind"].upper(), 9), d["title"]))


def build_report(inspection: Dict[str, Any], docs: List[Dict[str, Any]], held: int) -> Dict[str, Any]:
    """One report for an inspection row. `docs` are its posted documents:
    {kind, date, title, archive_name, parsed, text}."""
    outcome = inspection["status"].upper()
    statements = [d for d in docs if d["parsed"]["doc_type"] == "sod"]
    plans = [d for d in docs if d["parsed"]["doc_type"] == "plan"]
    # The plan of correction is the statement with the provider's column
    # filled in, so its rows are the fullest version of the deficiencies.
    deficiencies: List[Dict[str, str]] = []
    for source in plans + statements:
        if source["parsed"]["deficiencies"]:
            deficiencies = source["parsed"]["deficiencies"]
            break
    if statements and plans and statements[0]["parsed"]["deficiencies"] and deficiencies is not statements[0]["parsed"]["deficiencies"]:
        # Take the finding from the state's own statement where the two agree
        # on the count (the plan's copy can carry the provider's handwriting).
        base = statements[0]["parsed"]["deficiencies"]
        if len(base) == len(deficiencies):
            deficiencies = [{**b, "plan": p["plan"], "completion_date": p["completion_date"]} for b, p in zip(base, deficiencies)]
    numbers: List[str] = []
    for d in docs:
        for number in d["parsed"]["survey_numbers"]:
            if number not in numbers:
                numbers.append(number)
    site = next((d["parsed"]["site"] for d in docs if d["parsed"]["site"]), "")
    survey_kind = next((d["parsed"]["survey_kind"] for d in docs if d["parsed"]["survey_kind"]), "")
    is_complaint = any(d["parsed"]["is_complaint"] for d in docs)
    adult = bool(docs) and adult_only([d["text"] for d in docs])
    categories: Dict[str, Any] = {
        "inspection_type": inspection["type"],
        "outcome": outcome,
        "is_complaint": is_complaint,
        "survey_kind": survey_kind,
        "survey_numbers": numbers,
        "site": site,
        "adult_program": adult,
        "documents": [{"kind": d["kind"], "date": d["date"], "title": d["title"], "archive_name": d["archive_name"]}
                      for d in docs],
        "deficiencies": deficiencies,
        "deficiency_count": len(deficiencies),
    }
    if held:
        categories["documents_held_back"] = held

    blocks: List[str] = []
    for index, d in enumerate(deficiencies, start=1):
        lines = [f"Deficiency {index}: {d['section']}".rstrip(": ")]
        if d["rule_text"]:
            lines.append("Rule: " + d["rule_text"])
        if d["finding"]:
            lines.append("Finding: " + d["finding"])
        if d["plan"]:
            lines.append("Plan of correction: " + d["plan"])
        if d["completion_date"]:
            lines.append("Completion: " + d["completion_date"])
        blocks.append("\n".join(lines))
    if not deficiencies:
        # Nothing parsed: keep the documents' own text so search and the
        # reader still have it.
        for d in docs:
            if d["parsed"]["doc_type"] in ("sod", "plan", "no_deficiency") and d["text"]:
                blocks.append(f"{KIND_LABELS.get(d['kind'].upper(), d['kind'].title())} ({d['title']})\n{d['text']}")
    heading = f"{display_name(inspection['type'])} on {inspection['date']}: {outcome.lower()}"
    if survey_kind or numbers:
        heading += "\n" + " ".join(x for x in (survey_kind, ", ".join(numbers)) if x)
    if site:
        heading += f"\nResidential site: {site}"
    text = "\n\n".join([heading] + blocks) if docs else ""

    label = "Complaint survey" if is_complaint else display_name(inspection["type"])
    if deficiencies:
        sections = list(dict.fromkeys(d["section"] for d in deficiencies if d["section"]))
        summary = f"{label}: {plural(len(deficiencies), 'deficiency', 'deficiencies')}"
        if sections:
            summary += ": " + "; ".join(sections[:4]) + ("; ..." if len(sections) > 4 else "")
    elif outcome == "ACCEPTED PLAN OF CORRECTION":
        summary = f"{label}: deficiencies cited, plan of correction accepted"
    elif outcome == "NO DEFICIENCIES":
        summary = f"{label}: no deficiencies"
    else:
        summary = f"{label}: {outcome.lower() or 'no outcome recorded'}"
    return {
        "report_id": inspection["report_id"],
        "report_date": inspection["date"],
        "report_url": SEARCH_URL,
        "raw_content": text,
        "content_length": len(text),
        "summary": summary,
        "categories": categories,
    }


def is_flagged(report: Dict[str, Any]) -> bool:
    categories = report["categories"]
    return categories["outcome"] == "ACCEPTED PLAN OF CORRECTION" or categories["deficiency_count"] > 0


def report_hash(report: Dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(report, sort_keys=True).encode("utf-8")).hexdigest()


# ── Names ────────────────────────────────────────────────────────────────────

KEEP_UPPER = {"LLC", "INC", "LLP", "PLLC", "PC", "PA", "USA", "II", "III", "IV", "NFI", "YMCA", "YWCA", "ME",
              "PO", "NE", "NW", "SE", "SW", "US", "AMHC", "CHCS"}
SMALL_WORDS = {"of", "and", "the", "for", "at", "in", "on", "a", "an", "to", "by"}
BRANDS = {"kidspeace": "KidsPeace", "Good Will-hinckley": "Good Will-Hinckley"}


def display_name(name: str) -> str:
    """Title-case what the state writes in capitals."""
    name = one_line(name)
    letters = [c for c in name if c.isalpha()]
    if not letters or any(c.islower() for c in letters):
        return name

    def fix(match: "re.Match") -> str:
        word = match.group(0)
        if word in KEEP_UPPER or len(word) == 1:
            return word
        if re.search(r"\d$", name[: match.start()]) and word in ("ST", "ND", "RD", "TH"):
            return word.lower()
        lowered = word.lower()
        if name[: match.start()].strip() and name[: match.start()].endswith(" ") and lowered in SMALL_WORDS:
            return lowered
        titled = lowered[0].upper() + lowered[1:]
        return re.sub(r"^(Mc)([a-z])", lambda m: m.group(1) + m.group(2).upper(), titled)

    titled = re.sub(r"[A-Za-z][A-Za-z']*", fix, name)
    for plain, brand in BRANDS.items():
        titled = re.sub(rf"\b{re.escape(plain)}\b", brand, titled, flags=re.I)
    return titled


def format_phone(value: str) -> str:
    match = re.match(r"^\+?1?\s*\(?(\d{3})\)?[\s.-]*(\d{3})[\s.-]*(\d{4})(.*)$", one_line(value))
    if not match:
        return one_line(value)
    return f"({match.group(1)}) {match.group(2)}-{match.group(3)}{(' ' + match.group(4).strip()) if match.group(4).strip() else ''}"


PROFESSIONS = {
    "MENTAL HEALTH ORGANIZATION": "Behavioral health organization (mental health)",
    "SUBSTANCE USE ORGANIZATION": "Behavioral health organization (substance use)",
}


def program_category(profession: str) -> str:
    upper = (profession or "").upper()
    if upper in PROFESSIONS:
        return PROFESSIONS[upper]
    if "SUBSTANCE" in upper:
        return PROFESSIONS["SUBSTANCE USE ORGANIZATION"]
    if "MENTAL" in upper:
        return PROFESSIONS["MENTAL HEALTH ORGANIZATION"]
    return f"Behavioral health organization ({display_name(profession).lower()})" if profession else "Behavioral health organization"


def facility_info(record: Dict[str, Any], scope_entry: Dict[str, Any], listed: bool) -> Dict[str, str]:
    status = display_name(record.get("status") or "")
    if not listed:
        status = f"No longer listed by the state{f' (last status: {status})' if status else ''}"
    zip_code = record.get("zip") or ""
    tail = " ".join(x for x in (record.get("state") or "ME", zip_code) if x)
    address = ", ".join(x for x in (display_name(record.get("street") or ""), display_name(record.get("city") or ""), tail) if x)
    return {
        "facility_name": scope_entry.get("display_name") or display_name(record.get("name") or scope_entry.get("name") or ""),
        "program_name": record["licence"],
        "program_category": program_category(record.get("profession") or ""),
        "full_address": address,
        "phone": format_phone(record.get("phone") or ""),
        "bed_capacity": "",
        "executive_director": "",
        "license_exp_date": record.get("expires") or "",
        "relicense_visit_date": "",
        "action": status,
    }


# ── Scraper ──────────────────────────────────────────────────────────────────


def load_scope(path: Path = SCOPE_FILE) -> Dict[str, Dict[str, Any]]:
    """Licence number -> entry, for the entries marked "in"."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return {entry["licence"]: entry for entry in data.get("operators", []) if entry.get("scope") == "in"}


class MEScraper:
    def __init__(self, client: Optional[MEClient] = None, pages: Optional[PageStore] = None,
                 reports: Optional[ReportStore] = None):
        self.client = client
        self.pages = pages
        self.reports = reports or report_store()
        self.stats: Counter = Counter()
        self.types: Counter = Counter()
        self.outcomes: Counter = Counter()
        self.kinds: Counter = Counter()
        self.sites: Counter = Counter()
        self.unparsed: List[str] = []
        self.held: List[str] = []
        self.not_reports: List[str] = []

    def fetch_document(self, name: str, doc: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """The cached extraction, or download, archive and extract. The cache
        entry remembers the document's title: when the state reorders an
        inspection's documents the name can come to mean another file, and
        the stale entry is then replaced."""
        cached = self.reports.cached_extract(name)
        if cached is not None and cached.get("source_title") == doc["title"]:
            return cached
        data = None
        if self.client is not None and doc.get("url"):
            data = self.client.document(doc["url"])
            if data:
                self.stats["downloaded"] += 1
        if data:
            self.reports.archive(name, data)
        elif cached is None:
            data = self.reports.archived_bytes(name)
        if not data:
            return None
        with self.reports.working_copy(data, name) as path:
            result = extract_pdf(path)
        result["source_title"] = doc["title"]
        if result.get("text"):
            self.reports.save_extract(name, result)
        return result

    def drop_archive(self, name: str) -> None:
        try:
            (self.reports.archive_dir / name).unlink()
        except OSError:
            pass

    def take(self, licence: str, html: str, record: Dict[str, Any], scope_entry: Dict[str, Any],
             listed: bool) -> Dict[str, Any]:
        parsed = parse_detail(html)
        for problem in parsed["unparsed"]:
            self.unparsed.append(f"{licence}: {problem}")
        attributes = parsed["attributes"]
        if not record.get("expires") and attributes.get("Expiration Date"):
            record = {**record, "expires": us_date(attributes["Expiration Date"])}
        if not record.get("status") and attributes.get("Status"):
            record = {**record, "status": attributes["Status"]}
        if not record.get("name"):
            record = {**record, "name": parsed["name"]}
        inspections = parsed["inspections"]
        assign_report_ids(inspections)
        reports = []
        for inspection in inspections:
            self.stats["inspections"] += 1
            self.types[inspection["type"] or "(empty)"] += 1
            self.outcomes[inspection["status"] or "(empty)"] += 1
            docs: List[Dict[str, Any]] = []
            held = 0
            for index, doc in enumerate(ordered_documents(inspection["documents"]), start=1):
                name = f"{licence}_{inspection['report_id']}_{index}.pdf"
                self.stats["documents_listed"] += 1
                extracted = self.fetch_document(name, doc)
                if not extracted or not extracted.get("text"):
                    self.stats["documents_no_text"] += 1
                    self.unparsed.append(f"{licence} {inspection['report_id']}: no text for {doc['title']!r}")
                    continue
                text = extracted["text"]
                parsed_doc = parse_document(doc, extracted)
                if not parsed_doc["doc_type"]:
                    self.stats["not_a_report"] += 1
                    self.not_reports.append(f"{licence} {inspection['report_id']}: {doc['kind']} {doc['title']!r}")
                    logger.warning(f"  {name} is not a licensing report; not posted, archive copy removed")
                    self.drop_archive(name)
                    held += 1
                    continue
                hits = privacy_hits(text)
                if hits:
                    self.stats["held_private"] += 1
                    self.held.append(f"{licence} {inspection['report_id']}: {doc['title']!r} ({', '.join(hits)})")
                    logger.warning(f"  {name} held back ({', '.join(hits)}); not posted, archive copy removed")
                    self.drop_archive(name)
                    held += 1
                    continue
                self.kinds[doc["kind"] or "(empty)"] += 1
                if extracted.get("ocr_pages"):
                    self.stats["documents_ocr"] += 1
                if parsed_doc["doc_type"] == "sod" and not parsed_doc["deficiencies"]:
                    self.stats["sod_without_deficiency"] += 1
                    self.unparsed.append(f"{licence} {inspection['report_id']}: statement with no parsed deficiency ({doc['title']!r})")
                docs.append({"kind": doc["kind"], "date": doc["date"], "title": doc["title"],
                             "archive_name": name, "parsed": parsed_doc, "text": text})
            report = build_report(inspection, docs, held)
            categories = report["categories"]
            self.stats["complaint_surveys"] += bool(categories["is_complaint"])
            self.stats["adult_reports"] += bool(categories["adult_program"])
            self.stats["deficiencies"] += categories["deficiency_count"]
            self.stats["reports_with_documents"] += bool(docs)
            if categories["site"]:
                self.sites[categories["site"]] += 1
            reports.append(report)
        reports.sort(key=lambda r: r["report_date"], reverse=True)
        if parsed["services"]:
            # The licence's services ride on the newest report (the page reads
            # them from there), as Pennsylvania's unit fields do.
            if reports:
                reports[0]["categories"]["services"] = parsed["services"]
        self.stats["licences"] += 1
        return {"facility_info": facility_info(record, scope_entry, listed), "reports": reports}

    def scrape_live(self, scope: Dict[str, Dict], known: Dict[str, Dict], page_hashes: Dict[str, str],
                    limit: int = 0, only: Optional[set] = None,
                    csv_copy: Optional[Path] = None) -> Tuple[List[Dict], Dict[str, Dict], Dict[str, str]]:
        today = datetime.now().strftime("%Y-%m-%d")
        rows = self.client.search()
        if not rows:
            raise RuntimeError(f"The search returned no licences; check {SEARCH_URL} by hand")
        csv_text = self.client.export_csv()
        records = parse_csv(csv_text)
        if csv_copy is not None:
            save_csv_copy(csv_copy, csv_text)
        self.stats["listed_rows"] = len(rows)
        self.stats["listed_licences"] = len(records)
        logger.info(f"{len(rows)} result rows, {len(records)} licences in the CSV")
        links: Dict[str, str] = {}
        for row in rows:
            links.setdefault(row["number"], row["detail_url"])

        registry: Dict[str, Dict] = {lic: dict(rec) for lic, rec in known.items()}
        for licence in scope:
            if licence in records:
                registry[licence] = {**records[licence], "last_listed": today}
        wanted = [lic for lic in scope if not only or lic in only]
        wanted.sort(key=lambda lic: (registry.get(lic, {}).get("name") or scope[lic].get("name") or "").lower())
        if limit:
            wanted = wanted[:limit]

        facilities: List[Dict] = []
        fingerprints: Dict[str, str] = {}
        for index, licence in enumerate(wanted, start=1):
            record = registry.get(licence) or {"licence": licence, "name": scope[licence].get("name", "")}
            listed = licence in links
            logger.info(f"[{index}/{len(wanted)}] {record.get('name', '')} ({licence})" + ("" if listed else " [not listed]"))
            url = links.get(licence)
            if not url:
                # Not in the board's list any more: ask for it by number.
                found = self.client.search({"scLicenseNo": licence})
                url = next((r["detail_url"] for r in found if r["number"] == licence), None)
                if not url:
                    logger.warning("  the state no longer shows this licence; what is on the site stays")
                    self.stats["licences_missing"] += 1
                    continue
            try:
                html = self.client.detail(url)
            except requests.RequestException as exc:
                logger.error(f"  page failed: {exc.__class__.__name__}")
                self.stats["page_errors"] += 1
                continue
            if not is_detail_page(html):
                logger.error("  not a licence page; skipped")
                self.stats["page_errors"] += 1
                continue
            fingerprint = page_fingerprint(html)
            fingerprints[licence] = fingerprint
            if self.pages is not None:
                try:
                    if self.pages.save(licence, html, fingerprint, page_hashes.get(licence)):
                        self.stats["pages_saved"] += 1
                except (OSError, ValueError) as exc:
                    logger.error(f"  could not save the page copy: {exc}")
                    fingerprints.pop(licence, None)
            # Documents are fetched inside take(), straight after the page,
            # while its tokens are fresh.
            facilities.append(self.take(licence, html, record, scope[licence], listed))
        return facilities, registry, fingerprints

    def scrape_saved(self, base: Path, scope: Dict[str, Dict], known: Dict[str, Dict], limit: int = 0,
                     only: Optional[set] = None) -> List[Dict]:
        """Rebuild from the newest saved copy of each licence page and the
        cached document extractions; no requests."""
        store = PageStore(base)
        wanted = [lic for lic in scope if not only or lic in only]
        wanted.sort(key=lambda lic: (known.get(lic, {}).get("name") or scope[lic].get("name") or "").lower())
        if limit:
            wanted = wanted[:limit]
        newest_listing = max((r.get("last_listed", "") for r in known.values()), default="")
        facilities = []
        for licence in wanted:
            latest = store.latest(licence)
            if latest is None:
                logger.warning(f"  {licence}: no saved page")
                continue
            record = known.get(licence) or {"licence": licence, "name": scope[licence].get("name", "")}
            listed = bool(newest_listing) and record.get("last_listed", "") == newest_listing
            facilities.append(self.take(licence, store.read(latest), record, scope[licence], listed))
        return facilities

    def print_stats(self, facilities: List[Dict], posted: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        logger.info("── Maine run summary ──")
        if self.stats["listed_licences"]:
            logger.info(f"board list: {self.stats['listed_rows']} rows, {self.stats['listed_licences']} licences")
        logger.info(f"operators: {self.stats['licences']} (not shown by the state: {self.stats['licences_missing']}, "
                    f"page errors: {self.stats['page_errors']}, page copies saved: {self.stats['pages_saved']})")
        logger.info(f"inspections: {self.stats['inspections']} (flagged: {sum(1 for r in reports if is_flagged(r))}, "
                    f"with documents: {self.stats['reports_with_documents']})")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        logger.info(f"inspection types: {dict(self.types.most_common())}")
        logger.info(f"outcomes: {dict(self.outcomes.most_common())}")
        logger.info(f"documents listed: {self.stats['documents_listed']}, downloaded this run: {self.stats['downloaded']}, "
                    f"read by OCR: {self.stats['documents_ocr']}, no text: {self.stats['documents_no_text']}")
        logger.info(f"documents posted by kind: {dict(self.kinds.most_common())}")
        logger.info(f"deficiencies parsed: {self.stats['deficiencies']}; statements with no parsed deficiency: "
                    f"{self.stats['sod_without_deficiency']}")
        logger.info(f"complaint surveys: {self.stats['complaint_surveys']}; reports marked adult: {self.stats['adult_reports']}")
        logger.info(f"sites named: {len(self.sites)} in {sum(self.sites.values())} reports")
        for site, count in self.sites.most_common():
            logger.info(f"  {site}: {count}")
        logger.info(f"documents that are not licensing reports (not posted): {self.stats['not_a_report']}")
        for line in self.not_reports:
            logger.warning(f"  {line}")
        logger.info(f"documents held back by the privacy check (not posted): {self.stats['held_private']}")
        for line in self.held:
            logger.warning(f"  {line}")
        logger.info(f"unparsed: {len(self.unparsed)}")
        for line in self.unparsed:
            logger.warning(f"  {line}")
        logger.info(f"to post: {len(posted)} operators, {sum(len(f['reports']) for f in posted)} new or changed reports")


def save_csv_copy(path: Path, text: str) -> None:
    """The board list without its email column."""
    rows = list(csv.reader(io.StringIO(text.lstrip("\ufeff"))))
    if not rows:
        return
    drop = [i for i, name in enumerate(rows[0]) if "email" in name.lower()]
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    for row in rows:
        writer.writerow([cell for i, cell in enumerate(row) if i not in drop])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(out.getvalue(), encoding="utf-8")


def select_changed(facilities: List[Dict], inspections: Dict[str, Dict[str, Dict]],
                   info_hashes: Dict[str, str]) -> Tuple[List[Dict], Dict[str, Dict[str, Dict]], Dict[str, str]]:
    """Reports that are new or changed (a document added to an old inspection
    changes its hash), and operators whose details changed."""
    posted, new_state, new_info = [], {}, {}
    for facility in facilities:
        licence = facility["facility_info"]["program_name"]
        known = inspections.get(licence, {})
        changed = [r for r in facility["reports"] if (known.get(r["report_id"]) or {}).get("hash") != report_hash(r)]
        info_hash = hashlib.sha1(json.dumps(facility["facility_info"], sort_keys=True).encode("utf-8")).hexdigest()
        if not changed and info_hashes.get(licence) == info_hash:
            continue
        gone = sorted(set(known) - {r["report_id"] for r in facility["reports"]})
        if gone:
            logger.info(f"  {licence}: {len(gone)} inspections no longer shown by the state (kept on the site)")
        posted.append({"facility_info": facility["facility_info"], "reports": changed})
        new_state[licence] = {r["report_id"]: {
            "hash": report_hash(r),
            "documents": [d["archive_name"] for d in r["categories"]["documents"]],
        } for r in changed}
        new_info[licence] = info_hash
    return posted, new_state, new_info


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    """What inspections-read.php would return for these operators."""
    shaped = [{
        "facility_info": f["facility_info"],
        "reports": [{**r, "is_structured": True} for r in f["reports"]],
    } for f in facilities]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "ME",
        "scraped_timestamp": timestamp,
        "scraping_notes": {"total_reports": sum(len(f["reports"]) for f in shaped)},
        "facilities": shaped,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=1)
    if "SearchResultToken=" in text:
        raise RuntimeError("A search token reached the payload; nothing written")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    logger.info(f"Wrote {path}")


def save_to_api(facilities: List[Dict], timestamp: str) -> bool:
    if "SearchResultToken=" in json.dumps(facilities):
        logger.error("A search token reached the payload; not posted")
        return False
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="ME",
        scraped_timestamp=timestamp,
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Maine behavioral health organization licensing surveys")
    parser.add_argument("--full", action="store_true", help=f"Ignore the report hashes in {STATE_FILE} and post everything")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N operators (by name)")
    parser.add_argument("--licence", action="append", default=[], help="Only this licence number (repeatable)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return (every report) to this JSON file")
    parser.add_argument("--from-saved", type=Path, metavar="DIR",
                        help="Rebuild from the saved page copies in DIR (the me_html folder) and the cached "
                             "document text; no state site requests")
    parser.add_argument("--csv-copy", type=Path, help="Save the board's licence list (without its email column) here")
    args = parser.parse_args()

    scope = load_scope()
    logger.info(f"{len(scope)} licences in scope ({SCOPE_FILE.name})")
    state = load_state(STATE_FILE)
    timestamp = datetime.now().isoformat(timespec="seconds")
    only = set(args.licence) or None
    scraper = MEScraper()
    logger.info(f"PDFs go to {scraper.reports.archive_dir}")

    if args.from_saved:
        facilities = scraper.scrape_saved(args.from_saved, scope, state.get("licences", {}), args.limit, only)
    else:
        scraper.client = MEClient()
        scraper.pages = PageStore()
        logger.info(f"Page copies go to {scraper.pages.base}")
        facilities, registry, fingerprints = scraper.scrape_live(
            scope, state.get("licences", {}), state.get("pages", {}), args.limit, only, args.csv_copy)
        # Not tied to a post: the registry remembers the licences, the page
        # fingerprints what was last saved.
        state["licences"] = registry
        state.setdefault("pages", {}).update(fingerprints)
        save_state(STATE_FILE, state)

    known = {} if args.full else state.get("inspections", {})
    info_hashes = {} if args.full else state.get("info", {})
    posted, new_state, new_info = select_changed(facilities, known, info_hashes)
    scraper.print_stats(facilities, posted)

    if args.out:
        write_out(args.out, facilities, timestamp)
    if not posted:
        logger.info("No new or changed reports since last run")
        return
    if args.no_post:
        logger.info("Skipping API POST because --no-post was set; report hashes not advanced")
        return
    if save_to_api(posted, timestamp):
        stored = state.setdefault("inspections", {})
        for licence, by_id in new_state.items():
            stored.setdefault(licence, {}).update(by_id)
        state.setdefault("info", {}).update(new_info)
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- report hashes not advanced")


if __name__ == "__main__":
    main()
