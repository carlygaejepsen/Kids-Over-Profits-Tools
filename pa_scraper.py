"""
Pennsylvania children's residential facility inspection summary scraper.

Source: the PA Department of Human Services, Human Services Provider
Directory, https://www.humanservices.dhs.pa.gov/HUMAN_SERVICE_PROVIDER_DIRECTORY/
(ASP.NET MVC, plain requests, no login):

  POST Home/HumanServicesProviderDirectorySearchResult   one search per service
       code (a fresh __RequestVerificationToken each time), one unpaginated table
  GET  Home/AzureInspVioltnReprtSearchResults?id=<licence id>   a unit's reports
  GET  Home/GetAzureFile?directory=inspectionsummary&filename=<YYYYMMDD_lic>.pdf

Each licensed unit (a cottage, a group home, a secure unit) is one facility
row: the state inspects and cites each unit on its own, and one legal entity
can hold dozens (George Junior Republic 37, KidsPeace 18).

Each PDF is a "Licensing Inspection Summary - Public": a letter page (clean
inspection, citations with a plan of correction to send, or the plan of
correction accepted / implemented), then one block per citation: regulation,
requirement text, description of violation, plan of correction, completion
date and on-site verification. The state blanks names and dates inside the
letter and the narratives; the gaps are the source's.

Only currently licensed units are listed by the search, so the state file
keeps every licence id ever seen and keeps asking for its reports (the report
list answers by id) so a closed unit's reports are still picked up.
"""

import argparse
import html
import json
import logging
import os
import re
import time
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pdfplumber
import requests
from bs4 import BeautifulSoup

from inspection_api_client import post_facilities_to_api
from report_store import ReportStore, extract_with_cache
from scraper_state import load_state, merge_new_ids, save_state, seen_from_state

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
STATE_FILE = Path(os.getenv("PA_STATE_FILE", ".pa_state.json"))
REPORTS = ReportStore("PA_PDF_CACHE", "pa_pdfs", Path(__file__).parent / "pa_pdfs")

HOST = "https://www.humanservices.dhs.pa.gov"
BASE = HOST + "/HUMAN_SERVICE_PROVIDER_DIRECTORY"
SEARCH_URL = BASE + "/Home/HumanServicesProviderDirectorySearchResult"
REPORT_LIST_URL = BASE + "/Home/AzureInspVioltnReprtSearchResults?id={lic}&facilityName=x"
REPORT_URL = BASE + "/Home/GetAzureFile?directory=inspectionsummary&filename={name}"

# Children's residential service codes (row counts on 2026-09-30). Out of
# scope: 52 private children and youth agencies (foster care and adoption),
# 44 supervised independent living, 45 day treatment, 64 host homes and the
# adult and disability codes.
SERVICE_CODES = {
    "36": "Residential Services",      # 55 Pa. Code 3800, 501 rows
    "41": "Transitional Living",       # 64
    "40": "Secure Detention",          # 17
    "39": "Secure Care",               # 9
    "42": "Outdoor Program",           # 3
    "43": "Mobile Program",            # 1
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 0.7
MAX_QUEUED = 150  # downloaded documents waiting for OCR


# ── Fetch layer ──────────────────────────────────────────────────────────────


class PAClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call = 0.0

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
                response = self.session.request(method, url, timeout=180, **kwargs)
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
            return response
        raise RuntimeError("unreachable")

    def search(self, code: str) -> List[Dict]:
        """Every listed unit for one service code."""
        page = self._request("GET", BASE + "/")
        page.raise_for_status()
        token = BeautifulSoup(page.text, "html.parser").find(
            "input", {"name": "__RequestVerificationToken"})
        if not token or not token.get("value"):
            raise RuntimeError("No __RequestVerificationToken on the directory's search page")
        response = self._request("POST", SEARCH_URL, data={
            "__RequestVerificationToken": token["value"],
            "ReturnSearchScreen": "", "ProgramOffice": "", "ServiceCode": code,
            "ServiceCodeSub": "", "Region": "", "FacilityName": "", "City": "",
            "County": "", "ZipCode": "", "LicenseStatusType": "",
        })
        response.raise_for_status()
        return parse_search(response.text, code)

    def report_files(self, lic: str) -> List[Tuple[str, str]]:
        """(file name, file date as listed) for a unit, newest first."""
        response = self._request("GET", REPORT_LIST_URL.format(lic=lic))
        response.raise_for_status()
        return parse_report_list(response.text)

    def pdf(self, name: str) -> Optional[bytes]:
        try:
            response = self._request("GET", REPORT_URL.format(name=name))
        except requests.RequestException as exc:
            logger.warning(f"  {name}: download failed ({exc}); skipped for this run")
            return None
        if response.status_code == 404:
            logger.warning(f"  {name}: 404 at the source")
            return None
        if response.status_code != 200 or not response.content.startswith(b"%PDF"):
            logger.warning(f"  {name}: HTTP {response.status_code}, not a PDF; skipped for this run")
            return None
        return response.content


def cell_lines(td) -> List[str]:
    # The directory double-encodes some names ("HOME &amp;amp; COMMUNITY").
    return [re.sub(r"\s+", " ", html.unescape(x)).strip() for x in td.get_text("\n").split("\n") if x.strip()]


def parse_search(page: str, code: str) -> List[Dict]:
    soup = BeautifulSoup(page, "html.parser")
    table = soup.find("table")
    units: List[Dict] = []
    if not table:
        return units
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue
        status_cell = tds[5].get_text("\n")
        lic = re.search(r"\[(\w+)\]", status_cell)
        if not lic:
            continue
        units.append({
            "lic": lic.group(1),
            "service_code": code,
            "service_type": " ".join(tds[0].get_text().split()),
            "program_office": " ".join(tds[1].get_text().split()),
            **parse_name_cell(cell_lines(tds[2])),
            "capacity": " ".join(tds[3].get_text().split()),
            "operation": " ".join(tds[4].get_text().split()),
            "status": (cell_lines(tds[5]) or [""])[0],
            **parse_licence_cell(" ".join(tds[6].get_text(" ").split())),
        })
    return units


def parse_name_cell(lines: List[str]) -> Dict[str, str]:
    """Unit name, legal entity, street lines, 'CITY,' 'PA' 'ZIP', Phone:, County:, Region:."""
    out = {"unit": "", "entity": "", "street": "", "city": "", "zip": "",
           "phone": "", "county": "", "region": ""}
    rest: List[str] = []
    for line in lines:
        labelled = re.match(r"(Phone|County|Region):\s*(.*)$", line)
        if labelled:
            out[labelled.group(1).lower()] = labelled.group(2).strip()
        else:
            rest.append(line)
    if rest:
        out["unit"] = rest.pop(0)
    if rest:
        out["entity"] = rest.pop(0)
    # The city line ends with a comma; the state and zip follow it.
    city_at = next((i for i, l in enumerate(rest) if l.endswith(",")), None)
    if city_at is not None:
        out["street"] = ", ".join(rest[:city_at])
        out["city"] = rest[city_at].rstrip(",").strip()
        tail = " ".join(rest[city_at + 1:])
        zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", tail)
        out["zip"] = zip_match.group(1) if zip_match else ""
    else:
        out["street"] = ", ".join(rest)
    return out


def parse_licence_cell(value: str) -> Dict[str, str]:
    """'FULL 2/24/2026 - 2/24/2027' -> type and ISO dates."""
    match = re.match(r"(.*?)\s*(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})", value)
    if not match:
        return {"licence_type": value, "licence_start": "", "licence_end": ""}
    return {
        "licence_type": match.group(1).strip(),
        "licence_start": us_date(match.group(2)),
        "licence_end": us_date(match.group(3)),
    }


def parse_report_list(page: str) -> List[Tuple[str, str]]:
    files: List[Tuple[str, str]] = []
    for match in re.finditer(r"filename=([\w.-]+?\.pdf)", html.unescape(page), re.I):
        name = match.group(1)
        if name not in (f for f, _ in files):
            files.append((name, ""))
    return files


def us_date(value: str) -> str:
    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", value or "")
    if not match:
        return ""
    try:
        return datetime(int(match.group(3)), int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def file_date(name: str) -> str:
    match = re.match(r"(\d{4})(\d{2})(\d{2})_", name)
    if not match:
        return ""
    try:
        return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).strftime("%Y-%m-%d")
    except ValueError:
        return ""


# ── PDF extraction ───────────────────────────────────────────────────────────


OCR_DPI = 200


def find_tesseract() -> str:
    if os.getenv("TESSERACT_CMD"):
        return os.getenv("TESSERACT_CMD")
    default = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    return str(default) if default.exists() else ""


def find_poppler() -> str:
    if os.getenv("POPPLER_PATH"):
        return os.getenv("POPPLER_PATH")
    found = sorted(Path("C:/tools").glob("poppler-*/Library/bin"), reverse=True) if Path("C:/tools").exists() else []
    return str(found[0]) if found else ""


def extract_pdf(path: Path) -> Dict:
    """Each page's text; the parser strips footers and joins them. Reports
    before mid-2019 are scans with no text layer, and later ones often have a
    scanned letter or plan of correction among text pages; every page with no
    text is OCR'd on its own."""
    pages: List[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    blank = [i for i, p in enumerate(pages) if len(p.strip()) < 20]
    ocr: List[int] = []
    for i in blank:
        text = ocr_page(path, i + 1)
        if text.strip():
            pages[i] = text
            ocr.append(i + 1)
    result: Dict[str, Any] = {"text": "\n".join(pages).strip(), "pages": pages}
    if ocr:
        result["ocr_pages"] = ocr
        result["ocr"] = len(ocr) == len(pages)
    return result


def ocr_page(path: Path, number: int) -> str:
    try:
        import pytesseract
        from pdf2image import convert_from_path
    except ImportError:
        logger.warning(f"  {path.name} has scanned pages and pytesseract/pdf2image are not installed")
        return ""
    tesseract = find_tesseract()
    if tesseract:
        pytesseract.pytesseract.tesseract_cmd = tesseract
    try:
        images = convert_from_path(str(path), dpi=OCR_DPI, first_page=number, last_page=number,
                                   poppler_path=find_poppler() or None)
    except Exception:
        # Poppler rejects a few malformed files ("Invalid page count 0") that
        # pdfplumber opens; render the page through pdfplumber instead.
        try:
            with pdfplumber.open(path) as pdf:
                images = [pdf.pages[number - 1].to_image(resolution=OCR_DPI).original]
        except Exception as exc:
            logger.warning(f"  OCR failed for {path.name} page {number}: {exc}")
            return ""
    try:
        return "\n".join(pytesseract.image_to_string(image) for image in images)
    except Exception as exc:  # tesseract missing or failing
        logger.warning(f"  OCR failed for {path.name} page {number}: {exc}")
        return ""


# ── Parsing ──────────────────────────────────────────────────────────────────

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}
LONG_DATE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b",
    re.I,
)
DATE = r"\d{1,2}/\d{1,2}/\d{2,4}"


def iso_date(value: str) -> str:
    """The first date in `value` as YYYY-MM-DD, or ''."""
    if not value:
        return ""
    long_match = LONG_DATE.search(value)
    num_match = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4}|\d{2})\b", value)
    if long_match and (not num_match or long_match.start() < num_match.start()):
        try:
            return datetime(int(long_match.group(3)), MONTHS[long_match.group(1).lower()],
                            int(long_match.group(2))).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    if num_match:
        year = int(num_match.group(3))
        if year < 100:
            year += 2000
        try:
            return datetime(year, int(num_match.group(1)), int(num_match.group(2))).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""


def one_line(value: str) -> str:
    value = (value or "").replace(" ", " ").replace("�", "'")
    return re.sub(r"\s+", " ", value).strip()


FOOTER = re.compile(
    r"^\s*(?:\d{1,2}/\d{1,2}/\d{4}\s+)?www\.dhs\.pa\.gov\s+\d+\s+of\s+\d+\s*$"
    r"|^\s*Page\s+\d+\s+of\s+\d+\s*$",
    re.I,
)


def clean_pages(pages: List[str]) -> str:
    """The pages joined, without the per-page footer and the running header
    ('ABRAXAS ACADEMY 14405') that citation pages repeat, so a citation
    running across a page break reads as one."""
    out: List[str] = []
    for page in pages:
        lines = [l.rstrip() for l in (page or "").replace("\r", "").split("\n")]
        lines = [l for l in lines if not FOOTER.match(l)]
        # Running header: the first non-empty line, the unit name and the
        # five-digit licence, on a page that then starts a regulation block.
        first = next((i for i, l in enumerate(lines) if l.strip()), None)
        if first is not None and re.match(r"^[A-Z0-9][A-Z0-9 .,'&/()#-]{1,90}\s\d{5}\s*$", lines[first].strip()):
            lines.pop(first)
        out.append("\n".join(lines).strip())
    return "\n".join(p for p in out if p).strip()


SECTION = re.compile(r"^((?:\d{4}|20)[.,]\d{1,3}[a-z]?)\s+([A-Za-z][^\n]{1,100}?)\s*$")
REQUIREMENT = re.compile(r"^\d{1,2}\s*[.,]\s*(?:\S{0,3}\s*e\s*[qg]\s*u?\s*i\s*r|55\s+PA\s+Code\s+Chapter)", re.I)
REG_LINE = re.compile(r"^((?:\d{4}|20)[.,]\d{1,3}[a-z]?(?:[.,][0-9a-z]{1,4})+)[.,]?\s+(.*)$", re.I)
VIOLATION = re.compile(r"^\S{0,6}\s*(?:D\s*e\s*s)?\S*\s*c?\s*r?\s*i?\s*p\s*t\s*i\s*o\s*n\s+of\s+Violation\b"
                       r"|^Area\s+of\s+Non-?\s*Compliance\b", re.I)
PLAN = re.compile(r"^(?:Plan of Correction|POC Submission|Provider'?s Plan of Correct\w*(?: Action)?(?: or Response)?)\b"
                  r"\s*:?\s*(.*)$", re.I)
COMPLETION = re.compile(r"^(?:Licensee'?s Proposed Overall |Directed )?Completion Date\s*:?\s*(.*)$", re.I)
VERIFY = re.compile(r"^(?:On[- ]?site Verification|POC (?=Verified)|Status of Correction\s*:?)\s*(.*)$", re.I)
PLAN_STATUS = re.compile(r"^(Do Not Accept|Not Accept\w*|Accept\w*|Directed|Pending|Rejected)", re.I)
VERIFY_STATUS = re.compile(r"^(Not Implemented|Partially Implemented[^(]*?|Implemented|Verified|Pending)", re.I)


def parse_citations(text: str) -> List[Dict]:
    """Citation blocks of the Licensing Inspection Summary (mid-2019 on):

        3800.31 Notification of Rights and Grievance Procedures
        1. Requirements
        3800.31.d. <regulation text>
        Description of Violation
        <finding>
        Plan of Correction Accept ( - 03/28/2025)
        <plan>
        Licensee's Proposed Overall Completion Date: 03/28/2025
        On-site Verification Implemented ( - 03/28/2025)
        <note>
    """
    citations: List[Dict] = []
    current: Optional[Dict] = None
    field_name: Optional[str] = None
    section = ("", "")
    for raw in text.split("\n"):
        line = raw.strip()
        if not line:
            continue
        heading = SECTION.match(line)
        if heading and field_name == "plan" and heading.group(1) == section[0]:
            continue  # providers often head their plan with the section name
        if heading and not REG_LINE.match(line):
            title = heading.group(2)
            if re.search(r"\(continued\)\s*$", title, re.I):
                continue
            if not re.search(r"[.;:]$", title) and len(title.split()) <= 12:
                section = (heading.group(1).replace(",", "."), one_line(title))
                field_name = None
                continue
        # OCR leaves form debris after the label ("1, 55 PA Code Chapter : | Senne ...").
        if REQUIREMENT.match(line) and (len(line) < 40 or re.search(r"55\s+PA\s+Code\s+Chapter", line, re.I)):
            current = {"regulation": "", "section": section[0], "title": section[1],
                       "regulation_text": "", "violation": "", "plan": "", "plan_status": "",
                       "plan_date": "", "completion_date": "", "verification": "",
                       "verification_status": "", "verification_date": ""}
            citations.append(current)
            field_name = "regulation_text"
            continue
        if current is None:
            continue
        if field_name == "regulation_text" and not current["regulation"]:
            reg = REG_LINE.match(line)
            if reg:
                current["regulation"] = reg.group(1).replace(",", ".").rstrip(".")
                current["regulation_text"] = reg.group(2)
                continue
        if VIOLATION.match(line) and len(line) < 80:
            field_name = "violation"
            continue
        plan = PLAN.match(line)
        if plan and field_name in ("violation", "regulation_text", "plan", None):
            rest = plan.group(1)
            status = PLAN_STATUS.match(rest)
            if status:
                current["plan_status"] = status.group(1).title()
                current["plan_date"] = iso_date(rest)
            elif rest and not re.match(r"^\(?POC\)?", rest):
                current["plan"] = rest
            field_name = "plan"
            continue
        completion = COMPLETION.match(line)
        if completion:
            rest = completion.group(1)
            current["completion_date"] = iso_date(rest) or current["completion_date"]
            # "Completion Date: 06/10/2022 POC Verified 8/10/22"
            verified = re.search(r"POC\s+Verified\s*(.*)$", rest, re.I)
            if verified:
                current["verification_status"] = "Verified"
                current["verification_date"] = iso_date(verified.group(1))
            field_name = None
            continue
        verify = VERIFY.match(line)
        if verify:
            rest = verify.group(1)
            status = VERIFY_STATUS.match(rest)
            if status:
                current["verification_status"] = one_line(status.group(1)).title()
            current["verification_date"] = iso_date(rest)
            field_name = "verification"
            continue
        if field_name:
            current[field_name] = (current[field_name] + "\n" + line).strip()
    for citation in citations:
        for key in ("regulation_text", "violation", "plan", "verification"):
            citation[key] = one_line(citation[key])
        if not citation["regulation"]:
            # OCR splits "3800.\n202. Appropriate use ..." across lines.
            split = re.match(r"\s*3800\s*[.;,]?\s*(\d{1,3})\b", citation["regulation_text"])
            citation["regulation"] = citation["section"] or (f"3800.{split.group(1)}" if split else "")
    return [c for c in citations if c["regulation"] or c["violation"]]


def empty_citation(regulation: str = "", title: str = "") -> Dict[str, str]:
    section = re.match(r"\d{4}\.\d+", regulation)
    return {"regulation": regulation, "section": section.group(0) if section else regulation, "title": title,
            "regulation_text": "", "violation": "", "plan": "", "plan_status": "",
            "plan_date": "", "completion_date": "", "verification": "",
            "verification_status": "", "verification_date": ""}


# A regulation as the scanned forms write it: "3800.16(b)", "3800. 143 (e) (6)",
# "$800.132(e)" (OCR reads the 3 as $ or 8 now and then), "3800.243-9".
# Chapter 3800 mostly; now and then chapter 20 (licensing in general).
OLD_REG = re.compile(r"(?<![\d.])([3$58]800(?=\s*[.;,:])|20(?=\s*\.\s*\d))\s*[.;,:]\s*(\d{1,3})\s*((?:[({]\s*\{?[a-z0-9]{1,4}\s*[)}]\s*)*)(?:-\s*(\d{1,2}))?", re.I)


def old_regulation(match: "re.Match") -> str:
    parts = "".join(f".{p}" for p in re.findall(r"[({]\s*\{?([a-z0-9]{1,4})\s*[)}]", match.group(3) or "", re.I))
    extra = f".{match.group(4)}" if match.group(4) else ""
    chapter = "20" if match.group(1) == "20" else "3800"
    return f"{chapter}.{match.group(2)}{parts.lower()}{extra}"


def parse_numbered_form(text: str) -> List[Dict]:
    """The scanned Licensing Inspection Summary of about 2012 to 2019:

        1. REGULATION 55 Pa.Code §3800
        3800.132(e) - A fire drill shall be held during sleeping hours ...
        2a. DESCRIPTION OF VIOLATION
        <finding>
        3. PLAN OF CORRECTION (POC) (Attach pages as necessary ...)
        Include steps to correct the violation ... will be completed.
        <plan>
        Repeat Violation: No Date(s) of Previous Violation(s):
    """
    starts = [m for m in re.finditer(r"(?im)^\s*\S{0,3}\s*\.?,?\s*REGULATION\b.*$", text)]
    citations: List[Dict] = []
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(text)
        block = text[start.end():end]
        reg = OLD_REG.search(block[:400])
        citation = empty_citation(old_regulation(reg) if reg else "")
        desc = re.search(r"(?im)^.{0,8}DES\S*\s*RIPTION\s+OF\s+VIOLATION.*$", block)
        plan = re.search(r"(?im)^.{0,8}PLAN\s+OF\s+CORR\S*.*$", block)
        if reg:
            reg_end = desc.start() if desc else min(len(block), reg.end() + 600)
            citation["regulation_text"] = one_line(block[reg.end():reg_end]).lstrip("-: ")[:1200]
        if desc:
            violation_end = plan.start() if plan and plan.start() > desc.end() else len(block)
            citation["violation"] = one_line(block[desc.end():violation_end])
        if plan:
            rest = block[plan.end():]
            # Skip the form's instructions, which end "... by which the steps will be completed."
            instructions = re.search(r"will\s+be\s+comp\S*\s*\.?", rest[:500], re.I)
            if instructions:
                rest = rest[instructions.end():]
            stop = re.search(r"(?im)^.{0,4}(Repeat\s+Violation|Signature\s+of|Plan\s+of\s+correction\s+implementation)", rest)
            citation["plan"] = one_line(rest[: stop.start()] if stop else rest[:3000])
        repeat = re.search(r"Repeat\s+Violation\s*:?\s*(Yes|No)\b", block, re.I)
        if repeat:
            citation["repeat"] = repeat.group(1).lower() == "yes"
        if citation["regulation"] or citation["violation"]:
            citations.append(citation)
    return citations


FINDING_WORDS = re.compile(
    r"\b(?:was found|were found|did not|does not|was not|were not|had not|has not|no documentation|"
    r"not have|failed|observed|lacked|missing|there was|there were)\b", re.I)


def parse_table_form(text: str) -> List[Dict]:
    """The scanned Licensing/Approval/Registration Inspection Summary of about
    2009 to 2012, a six-column table (regulation, non-compliance area,
    correction required, date, provider's plan, status) that OCR reads column
    by column. Only the regulation is certain; the finding is the first
    paragraph after it that reads like one."""
    start = re.search(
        r"(?i)(issuing the following citations|areas? of non-?compliance (?:were|was) observed|"
        r"following (?:areas? of )?non-?compliance|NON-?COMPLIANCE AREA)", text) \
        or re.search(r"(?i)INSPECTION\s+SUMMARY", text)
    if not start:
        return []
    body = text[start.end():]
    # The provider's own plan pages ("In RE: 3800.243-9 ...") repeat the numbers.
    stop = re.search(r"(?im)^\s*(?:In\s+RE\s*:|Plan of Correction\s*/\s*Response)", body)
    if stop:
        body = body[: stop.start()]
    citations: List[Dict] = []
    seen: Set[str] = set()
    matches = [m for m in re.finditer(r"(?m)^\s*(?:\d\s*[.,]\s*55\s*PA\s*CODE\s*)?" + OLD_REG.pattern, body, re.I)]
    for index, match in enumerate(matches):
        inner = OLD_REG.search(match.group(0))
        regulation = old_regulation(inner)
        if regulation in seen:
            continue
        seen.add(regulation)
        end = matches[index + 1].start() if index + 1 < len(matches) else min(len(body), match.end() + 2500)
        chunk = body[match.end():end]
        if "|" in chunk.split("\n", 1)[0]:
            # OCR kept the table's rows: the finding is each line's first column.
            first_column = []
            for line in chunk.split("\n"):
                if not line.strip():
                    break
                first_column.append(re.sub(r"^\s*\(\w{1,3}\)\s*", "", line.split("|")[0]))
            paragraphs = [one_line(" ".join(first_column))]
        else:
            paragraphs = [one_line(p) for p in re.split(r"\n\s*\n", chunk) if one_line(p)]
        finding = next((p for p in paragraphs[:5] if FINDING_WORDS.search(p)), "")
        citation = empty_citation(regulation)
        citation["violation"] = finding
        citations.append(citation)
    return citations


KIND_LABELS = {
    "citation": "Citations",
    "followup": "Plan of correction verified",
    "clean": "No citations",
    "sanction": "Licence action",
    "licence": "Licence issued",
    "waiver": "Waiver granted",
    "other": "Document",
}

SANCTION = re.compile(
    r"(?:first|second|third|fourth)\s+provisional\s+(?:license|certificate)"
    r"|issu\w+\s+(?:of\s+)?(?:a|your)\s+provisional\s+(?:license|certificate)"
    r"|provisional\s+(?:license|certificate)\s+(?:is|has|will)\s+(?:being|been|be)\s+issued"
    r"|revocation\s+of\s+(?:your|the)"
    r"|(?:decision|intent)\s+to\s+(?:revoke|refuse|not\s+renew)"
    r"|refus\w+\s+to\s+(?:issue|renew)"
    r"|non-?renewal\s+of"
    r"|not\s+renewing\s+your\s+licen[cs]e",
    re.I,
)
CITED_LETTER = re.compile(
    r"(?<!\bno\s)(?<!\bNo\s)(?:citations|violations)\b(?:(?!\bno\b|\bnot\b).){0,200}?\bwere\s+found"
    r"|noted:?\s+(?:areas?\s+of\s+non-?compliance|deficiencies)"
    r"|issuing\s+the\s+following\s+citations"
    r"|areas?\s+of\s+non-?compliance\s+(?:were|was)\s+observed",
    re.I | re.S,
)
FOLLOWUP_LETTER = re.compile(
    r"plan\s+of\s+corrections?\s+(?:is|are)\s+fully\s+implemented"
    r"|Plan\s+of\s+Correction\b.{0,160}?\b(?:has\s+been|is)\s+(?:reviewed\s+and\s+)?(?:has\s+been\s+)?accepted"
    r"|approving\s+your\s+(?:action\s+plan|plan\s+of\s+correction)",
    re.I | re.S,
)
WAIVER_LETTER = re.compile(r"waiver\s+of\s+55\s+Pa\.?\s*Code.{0,400}?\bis\s+hereby\s+granted", re.I | re.S)
CLEAN_LETTER = re.compile(
    r"no\s+regulatory\s+citations\s+have\s+been\s+identified"
    r"|No\s+(?:violations|deficiencies)\s+(?:were\s+)?found"
    r"|No\s+regulatory\s+violations\s+have\s+been\s+identified"
    r"|(?:is|be)\s+in\s+(?:substantial\s+)?compliance\s+with\s+(?:the\s+)?regulations"
    r"|found\s+the\s+above[- ]?(?:named\s+)?(?:facility|agency),?\s+to\s+be\s+in\s+compliance"
    r"|No\s+Deficiencies\s+Identified"
    r"|in\s+complete\s+compliance\s+with"
    r"|observed\s+compliance\s+with\s+the\s+regulations",
    re.I,
)
LICENCE_LETTER = re.compile(
    r"license\s+is\s+being\s+issued\s+in\s+response\s+to\s+your\s+application"
    r"|revised\s+Certificate\s+of\s+Compliance"
    r"|CERTIFICATE\s+OF\s+(?:C\s*)?OMPLIANCE",
    re.I,
)


def form_of(text: str) -> str:
    """'summary' (the form since mid-2019, mixed-case labels), 'numbered'
    (scanned, capitalised numbered labels) or 'table' (scanned, 2009-2012)."""
    if re.search(r"Description\s+of\s+Violation|Agency\s*/\s*Facility\s+Information|No\s+Deficiencies\s+Identified"
                 r"|Area\s+of\s+Non-?\s*Compliance", text):
        return "summary"
    if re.search(r"(?m)^.{0,8}DES\S*\s*RIPTION\s+OF\s+VIOLATION", text):
        return "numbered"
    if re.search(r"(?i)LICENSING\s*/\s*APPROVAL\s*/\s*REGISTRATION|NON-?COMPLIANCE\s+AREA|areas?\s+of\s+non-?compliance", text):
        return "table"
    return "summary"


def parse_all_citations(text: str) -> Tuple[str, List[Dict]]:
    form = form_of(text)
    if form == "numbered":
        return form, parse_numbered_form(text)
    if form == "table":
        return form, parse_table_form(text)
    return form, parse_citations(text)


def classify(text: str, citations: List[Dict]) -> str:
    flat = one_line(text)
    if SANCTION.search(flat):
        return "sanction"
    # "no regulatory violations were found": a "no" up to two words before
    # the noun cancels the match.
    cited = any(not re.search(r"\b(?:no|not\s+any)\s+(?:\S+\s+){0,2}$", flat[max(0, m.start() - 40):m.start()], re.I)
                for m in CITED_LETTER.finditer(flat))
    if FOLLOWUP_LETTER.search(flat) and not cited:
        return "followup"
    if cited or citations:
        return "citation"
    if CLEAN_LETTER.search(flat):
        return "clean"
    if WAIVER_LETTER.search(flat):
        return "waiver"
    if LICENCE_LETTER.search(flat):
        return "licence"
    return "other"


def inspection_info(text: str) -> Dict[str, str]:
    """The Agency/Facility and Inspection Information blocks (mid-2019 on),
    or the older forms' equivalents."""
    info = {"inspection_type": "", "inspection_start": "", "inspection_end": "", "notice": "",
            "narrative": ""}
    # One field at a time: OCR of a scanned information page interleaves them
    # with the licence lines.
    start = re.search(r"Start\s+Date\s*[:;]?\s*(" + DATE + r")", text, re.I)
    if start:
        info["inspection_start"] = iso_date(start.group(1))
        end = re.search(r"End\s+Date\s*[:;]?\s*(" + DATE + r")", text[start.end():start.end() + 200], re.I)
        info["inspection_end"] = iso_date(end.group(1)) if end else ""
        kind = re.search(r"(?<!Up )(?<!Up)\bType\s*[:;]\s*([A-Za-z][A-Za-z /-]{1,40}?)\s+(?:Notice\b|\n|$)", text, re.I)
        info["inspection_type"] = one_line(kind.group(1))[:60] if kind else ""
        notice = re.search(r"Notice\s*[:;]\s*([^\n]{1,40})", text, re.I)
        info["notice"] = one_line(notice.group(1)) if notice else ""
    else:
        reason = re.search(r"Reason\(?s\)?\s+for\s+Inspection\(?s?\)?\s*\n+\s*([^\n]+)", text, re.I)
        if reason:
            info["inspection_type"] = one_line(reason.group(1))[:60]
        notice = re.search(r"Notice\s*[:;]\s*(Announced|Unannounced)", text, re.I)
        if notice:
            info["notice"] = notice.group(1).title()
        onsite = re.search(r"On-?Site\s+Inspections?\s+Dates.*?\n\s*(" + DATE + r")", text, re.I)
        letter = re.search(r"(?:inspections?|review|evaluation)\s+(?:conducted\s+)?(?:on\s+)?(?:of\s+the\s+above[^\n]*?on\s+)?"
                           r"((?:January|February|March|April|May|June|July|August|September|October|November|December)"
                           r"\s+\d{1,2}[^,]{0,30},?\s+\d{4}|" + DATE + r")", one_line(text[:4000]), re.I)
        info["inspection_start"] = iso_date(onsite.group(1)) if onsite else (iso_date(letter.group(1)) if letter else "")
    narrative = re.search(r"Inspection\s+Narrative\s*\n(.*?)(?:\n\s*Inspections?\s*/\s*Reviews|\Z)", text, re.I | re.S)
    if narrative:
        info["narrative"] = one_line(narrative.group(1))[:2000]
    return info


def letter_date(text: str) -> str:
    head = text[:1500]
    mailing = re.search(r"MAILING\s+DATE\s*:?\s*([^\n]+)", head, re.I)
    if mailing and iso_date(mailing.group(1)):
        return iso_date(mailing.group(1))
    match = LONG_DATE.search(head)
    return iso_date(match.group(0)) if match else ""


def all_dates(text: str) -> List[str]:
    found: List[str] = []
    for match in LONG_DATE.finditer(text):
        found.append(iso_date(match.group(0)))
    for match in re.finditer(r"\b\d{1,2}/\d{1,2}/\d{4}\b", text):
        found.append(iso_date(match.group(0)))
    return [d for d in found if d]


def report_date_for(name: str, text: str, categories: Dict) -> Tuple[str, bool]:
    """The date in the file name, which the state keys by hand: some scans
    are filed under the right day and month but the wrong year (a 2017
    inspection filed as 20080104, a 2013 one as 20070323). Only for scanned
    documents (the text ones carry their date in the footer): when the
    file's year appears nowhere in the document, the date with the same day
    and month in the document's own year wins, or else the document's own
    inspection or letter date."""
    named = file_date(name)
    if not named or not categories.get("ocr") or named[:4] in text:
        return named, False
    own = categories.get("inspection_start") or categories.get("letter_date") or ""
    same_day = [d for d in all_dates(text) if d[4:] == named[4:] and d[:4] != named[:4]]
    if own:
        match = next((d for d in same_day if d[:4] == own[:4]), "")
        return match or own, True
    if same_day:
        return same_day[0], True
    return named, False


def parse_report(name: str, pages: List[str], ocr: bool) -> Dict:
    text = clean_pages(pages)
    form, citations = parse_all_citations(text)
    kind = classify(text, citations)
    info = inspection_info(text)
    categories: Dict[str, Any] = {
        "kind": kind,
        "form": form,
        "ocr": bool(ocr),
        "letter_date": letter_date(text),
        **info,
        "citations": citations,
        "citation_count": len(citations),
        "repeat_count": sum(1 for c in citations if c.get("repeat")),
    }
    return {"text": text, "categories": categories}


PREVIEW_CHARS = 240


def shorten(text: str, limit: int = PREVIEW_CHARS) -> str:
    if len(text) <= limit:
        return text
    return re.sub(r"\s+\S*$", "", text[: limit - 1]) + "…"


def split_detail(categories: Dict) -> None:
    """Keep the list light: about 9,700 reports load at once on the page
    (inspections-read.php ?lite=1). Each citation keeps its regulation,
    title, statuses and the first PREVIEW_CHARS of the violation;
    categories.detail holds the narrative and, per citation in the same
    order, the full violation, requirement, plan and verification. Lite
    lists leave detail out and send it with the report's text on open."""
    compact: List[Dict] = []
    detail: List[Dict] = []
    for c in categories["citations"]:
        short = shorten(c["violation"])
        compact.append({k: v for k, v in {
            "regulation": c["regulation"],
            "title": c["title"],
            "violation": short,
            "plan_status": c["plan_status"],
            "completion_date": c["completion_date"],
            "verification_status": c["verification_status"],
            "repeat": c.get("repeat"),
        }.items() if v})
        detail.append({k: v for k, v in {
            "violation": c["violation"] if short != c["violation"] else "",
            "regulation_text": c["regulation_text"],
            "plan": c["plan"],
            "plan_date": c["plan_date"],
            "verification": c["verification"],
            "verification_date": c["verification_date"],
        }.items() if v})
    categories["citations"] = compact
    categories["detail"] = {"narrative": categories.pop("narrative", ""), "citations": detail}


def summarize(categories: Dict) -> str:
    count = categories["citation_count"]
    regs = list(dict.fromkeys(c["regulation"] for c in categories["citations"] if c["regulation"]))
    listed = ", ".join(regs[:6]) + (", ..." if len(regs) > 6 else "")
    noun = "citation" if count == 1 else "citations"
    kind = categories["kind"]
    if kind == "sanction":
        return "Licence action" + (f"; {count} {noun}: {listed}" if count else "")
    if kind == "followup":
        return f"Plan of correction verified for {count} {noun}" + (f": {listed}" if listed else "")
    if kind == "citation":
        return f"{count} {noun}: {listed}" if count else "Citations (not read from the document)"
    if kind == "clean":
        return "No citations"
    if kind == "licence":
        return "Licence issued"
    if kind == "waiver":
        return "Waiver of a regulation granted"
    return "Document"


# ── Names ────────────────────────────────────────────────────────────────────

KEEP_UPPER = {"LLC", "INC", "LLP", "PLLC", "PC", "USA", "II", "III", "IV", "VI", "VII", "VIII",
              "IX", "XI", "XII", "YMCA", "YWCA", "PRTF", "RTF", "RTC", "ITU", "CRC", "YDC", "YFC",
              "ABC", "JJC", "PA", "CHOP", "UPMC", "YAP", "CYS", "IBS"}
SMALL_WORDS = {"of", "and", "the", "for", "at", "in", "on", "a", "an", "to", "by"}
PLAIN_ABBREVIATIONS = {"ST", "MT", "FT", "DR", "RD", "PL", "LN", "CT", "JR", "SR", "BLDG", "BLVD", "HWY", "PKWY", "TWP"}
CORPORATE = re.compile(r"[\s,]+(?:INC|INCORPORATED|LLC|L\.L\.C|CORP|CORPORATION|CO|LTD|LP|PC)\.?$", re.I)
GENERIC = {"THE", "INC", "LLC", "OF", "AND", "FOR", "IN", "PA", "PENNSYLVANIA", "CORP", "CORPORATION",
           "COMPANY", "CO", "SERVICES", "SERVICE", "CENTER", "CENTERS", "GROUP", "HOME", "HOMES",
           "COUNTY", "YOUTH", "CHILDREN", "CHILDRENS", "FAMILY", "FAMILIES", "HUMAN"}


def display_name(name: str) -> str:
    """Title-case names the state writes in capitals; keep INC, LLC, roman
    numerals and initials."""
    name = one_line(name)
    letters = [c for c in name if c.isalpha()]
    if not letters or any(c.islower() for c in letters):
        return name

    def fix(match: "re.Match") -> str:
        word = match.group(0)
        if word in KEEP_UPPER or len(word) == 1:
            return word
        if re.search(r"\d$", name[: match.start()]) and word.upper() in ("ST", "ND", "RD", "TH"):
            return word.lower()  # 25TH -> 25th
        # Initialisms such as GJR or YDC (short, no vowel) stay in capitals.
        if 2 <= len(word) <= 4 and not re.search(r"[AEIOUY']", word) and word not in PLAIN_ABBREVIATIONS:
            return word
        lowered = word.lower()
        before = name[: match.start()]
        if before.endswith(" ") and before.strip() and lowered in SMALL_WORDS:
            return lowered
        titled = lowered[0].upper() + lowered[1:]
        return re.sub(r"^(Mc)([a-z])", lambda m: m.group(1) + m.group(2).upper(), titled)

    titled = re.sub(r"[A-Za-z][A-Za-z']*", fix, name)
    for plain, brand in BRANDS.items():
        titled = re.sub(rf"\b{plain}\b", brand, titled, flags=re.I)
    return titled


# Names whose owners write them with inner capitals.
BRANDS = {"kidspeace": "KidsPeace"}


def name_words(value: str) -> Set[str]:
    words = re.findall(r"[A-Z0-9]+", re.sub(r"'S\b", "", value.upper()))
    return {w for w in words if w not in GENERIC and len(w) > 1}


def facility_name(unit: str, entity: str) -> str:
    """The unit name alone when it already says whose it is (it shares a
    distinguishing word with the legal entity, or there is no entity);
    otherwise '<Legal Entity>: <Unit>', so a list sorted by name keeps an
    entity's units together and units named '1' or 'BENTON COTTAGE' say
    whose they are."""
    unit, entity = one_line(unit), one_line(entity)
    if not entity or unit.upper() == entity.upper():
        return display_name(unit or entity)
    if not unit:
        return display_name(entity)
    if name_words(unit) & name_words(entity):
        return display_name(unit)
    return f"{display_name(CORPORATE.sub('', entity).rstrip(' ,'))}: {display_name(unit)}"


def format_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return value or ""


def full_address(unit: Dict) -> str:
    street = display_name(unit.get("street") or "")
    city = display_name(unit.get("city") or "")
    tail = " ".join(x for x in ("PA", unit.get("zip") or "") if x)
    return ", ".join(x for x in (street, city, tail) if x)


# ── Scraper ──────────────────────────────────────────────────────────────────


UNIT_FIELDS = ("legal_entity", "unit", "service_type", "region", "county")
DROP_WHEN_EMPTY = ("ocr", "date_corrected", "repeat_count", "counts_as_violation", "followup_of",
                   "notice", "inspection_end", "inspection_type", "inspection_start")


def prune_categories(categories: Dict, newest: bool) -> None:
    """The page lists every report of the state at once, so each report
    carries only what differs: the unit's entity, service type, region and
    county ride on its newest report of the run (the page reads them from
    there), the file name is the report_id, and empty or default values are
    left out. The letter date goes with the detail."""
    for key in DROP_WHEN_EMPTY:
        if not categories.get(key):
            categories.pop(key, None)
    if categories.get("form") == "summary":
        categories.pop("form")
    categories.pop("file_name", None)
    if not categories.get("date_corrected"):
        categories.pop("file_date", None)
    letter = categories.pop("letter_date", "")
    if letter:
        categories.setdefault("detail", {})["letter_date"] = letter
    if not newest:
        for key in UNIT_FIELDS:
            categories.pop(key, None)


def repaired_entity(entity: str, reports: List[Dict]) -> str:
    """The directory now and then has a broken legal entity name
    ("260618ILDREN'S HOME OF EASTON INC"). Then the newest summary's
    'Legal Entity ... Name:' line is used instead; '' when nothing to repair."""
    if not re.match(r"^\s*\d{3,}[A-Z]", entity or ""):
        return ""
    for report in sorted(reports, key=lambda r: r["report_date"], reverse=True):
        found = re.search(r"Legal\s+Entity\s*(?:Name)?\s*\n?\s*Name\s*[:;]\s*([^\n]+)", report["raw_content"])
        if found and one_line(found.group(1)):
            return one_line(found.group(1)).rstrip(".")
    return ""


def inspection_key(report: Dict) -> str:
    return report["categories"].get("inspection_start") or report["report_date"]


def mark_violations(reports: List[Dict], cited_before: Set[str]) -> Tuple[Set[str], int]:
    """Set categories.counts_as_violation. A citation document or a licence
    action counts. A follow-up (the plan of correction verified) repeats the
    citations of an inspection: it counts only when no citation document for
    that inspection is on the state's list, which is the usual case, because
    the state replaces the summary with its verified version. Returns the
    inspection keys that now have a counted document, and how many
    follow-ups were counted for want of their citation document."""
    cited = set(cited_before)
    for report in reports:
        c = report["categories"]
        if c["kind"] in ("citation", "sanction"):
            cited.add(inspection_key(report))
    counted_followups = 0
    for report in reports:
        c = report["categories"]
        if c["kind"] in ("citation", "sanction"):
            c["counts_as_violation"] = True
        elif c["kind"] == "followup" and c["citation_count"]:
            key = inspection_key(report)
            c["counts_as_violation"] = key not in cited
            if key in cited:
                c["followup_of"] = key
            else:
                counted_followups += 1
                cited.add(key)
        else:
            c["counts_as_violation"] = False
    return cited, counted_followups


class PAScraper:
    def __init__(self, client: Optional[PAClient] = None, reports: ReportStore = REPORTS,
                 ocr_workers: int = 4):
        self.client = client or PAClient()
        self.reports = reports
        self.ocr_workers = max(1, ocr_workers)
        # Tesseract starts 4 threads per page by default; with several pages
        # at once that oversubscribes the CPU. One thread per process, one
        # process per core, reads more pages a minute.
        if self.ocr_workers > 1:
            os.environ.setdefault("OMP_THREAD_LIMIT", "1")
        self.stats: Counter = Counter()
        self.forms: Counter = Counter()
        self.counted_followups = 0
        self.pool: Optional[ThreadPoolExecutor] = None

    def listed_units(self, codes: List[str]) -> Dict[str, Dict]:
        units: Dict[str, Dict] = {}
        for code in codes:
            found = self.client.search(code)
            logger.info(f"service code {code} ({SERVICE_CODES.get(code, '?')}): {len(found)} units")
            for unit in found:
                units.setdefault(unit["lic"], unit)
        return units

    def submit_many(self, names: List[str]) -> Dict[str, Any]:
        """Download what is not cached, one at a time, and hand the extraction
        to the shared pool (OCR of a scanned report takes seconds a page), so
        the next unit's downloads go on while this one's scans are read.
        Values are an extraction, None, or a Future of one."""
        if self.pool is None:
            self.pool = ThreadPoolExecutor(max_workers=self.ocr_workers)
        results: Dict[str, Any] = {}
        for name in names:
            cached = self.reports.cached_extract(name)
            if cached is not None:
                results[name] = cached
                continue
            data = self.client.pdf(name) or self.reports.archived_bytes(name)
            if not data:
                results[name] = None
                continue
            self.reports.archive(name, data)
            self.stats["downloaded"] += 1
            results[name] = self.pool.submit(self._extract_bytes, name, data)
        return results

    @staticmethod
    def resolve(value: Any, name: str) -> Optional[Dict]:
        if isinstance(value, Future):
            try:
                return value.result()
            except Exception as exc:  # a broken PDF
                logger.warning(f"  {name}: extraction failed ({exc})")
                return None
        return value

    def close(self) -> None:
        if self.pool is not None:
            self.pool.shutdown(wait=True)
            self.pool = None

    def _extract_bytes(self, name: str, data: bytes) -> Dict:
        with self.reports.working_copy(data, name) as path:
            result = extract_pdf(path)
        if result.get("text"):
            self.reports.save_extract(name, result)
        return result

    def build_report(self, name: str, extracted: Dict, unit: Dict) -> Dict:
        parsed = parse_report(name, extracted.get("pages") or [extracted.get("text", "")],
                              bool(extracted.get("ocr")))
        categories = parsed["categories"]
        text = parsed["text"]
        report_date, corrected = report_date_for(name, text, categories)
        categories.update({
            "file_name": name,
            "file_date": file_date(name),
            "date_corrected": corrected,
            "legal_entity": display_name(unit.get("entity") or ""),
            "unit": display_name(unit.get("unit") or ""),
            "service_type": display_name(unit.get("service_type") or ""),
            "region": display_name(unit.get("region") or ""),
            "county": display_name(unit.get("county") or ""),
        })
        self.stats[f"kind_{categories['kind']}"] += 1
        self.forms[categories["form"]] += 1
        if categories["ocr"]:
            self.stats["ocr_documents"] += 1
        if corrected:
            self.stats["date_corrected"] += 1
        if categories["kind"] in ("citation", "followup", "sanction") and not categories["citation_count"]:
            self.stats[f"{categories['kind']}_without_citations"] += 1
        if len(extracted.get("pages") or []) > 1 and categories["kind"] == "other":
            self.stats["other_multi_page"] += 1
        self.stats["citations"] += categories["citation_count"]
        summary = summarize(categories)
        split_detail(categories)
        return {
            "report_id": name[:-4] if name.lower().endswith(".pdf") else name,
            "report_date": report_date,
            "report_url": REPORT_URL.format(name=name),
            "raw_content": text,
            "content_length": len(text),
            "summary": summary,
            "categories": categories,
        }

    def facility_info(self, unit: Dict, listed: bool) -> Dict:
        status = display_name(unit.get("status") or "")
        if not listed:
            status = f"No longer listed by the state{f' (last status: {status})' if status else ''}"
        return {
            "facility_name": facility_name(unit.get("unit") or "", unit.get("entity") or ""),
            "program_name": unit["lic"],
            "program_category": display_name(unit.get("service_type") or ""),
            "full_address": full_address(unit),
            "phone": format_phone(unit.get("phone") or ""),
            "bed_capacity": unit.get("capacity") or "",
            "executive_director": "",
            "license_exp_date": unit.get("licence_end") or "",
            "relicense_visit_date": "",
            "action": status,
        }

    def start_unit(self, unit: Dict, seen: Set[str], since: int = 0) -> Optional[Dict[str, Any]]:
        """List a unit's files and start fetching the new ones."""
        try:
            files = [name for name, _ in self.client.report_files(unit["lic"])]
        except requests.RequestException as exc:
            logger.error(f"  report list failed: {exc}")
            return None
        self.stats["files_listed"] += len(files)
        fresh = [f for f in files if f[:-4] not in seen]
        if since:
            fresh = [f for f in fresh if f[:4].isdigit() and int(f[:4]) >= since]
        if not fresh:
            return None
        return {"fresh": fresh, "jobs": self.submit_many(fresh)}

    @staticmethod
    def pending_jobs(started: Optional[Dict[str, Any]]) -> int:
        if not started:
            return 0
        return sum(1 for v in started["jobs"].values() if isinstance(v, Future) and not v.done())

    def scrape_unit(self, unit: Dict, listed: bool, seen: Set[str], cited_before: Set[str],
                    since: int = 0) -> Tuple[Optional[Dict], List[str], Set[str]]:
        return self.finish_unit(unit, listed, self.start_unit(unit, seen, since), cited_before)

    def finish_unit(self, unit: Dict, listed: bool, started: Optional[Dict[str, Any]],
                    cited_before: Set[str]) -> Tuple[Optional[Dict], List[str], Set[str]]:
        """Wait for a unit's extractions and build its facility record."""
        if not started:
            return None, [], cited_before
        reports = []
        for name in started["fresh"]:
            got = self.resolve(started["jobs"].get(name), name)
            if not got or not got.get("text"):
                self.stats["no_text"] += 1
                logger.warning(f"  no text for {name}")
                continue
            reports.append(self.build_report(name, got, unit))
        if not reports:
            return None, [], cited_before
        repaired = repaired_entity(unit.get("entity") or "", reports)
        if repaired:
            logger.info(f"  legal entity {unit.get('entity')!r} read from the documents as {repaired!r}")
            unit = {**unit, "entity": repaired}
            for report in reports:
                report["categories"]["legal_entity"] = display_name(repaired)
        reports.sort(key=lambda r: r["report_date"])
        cited, counted = mark_violations(reports, cited_before)
        self.counted_followups += counted
        reports.sort(key=lambda r: r["report_date"], reverse=True)
        for index, report in enumerate(reports):
            prune_categories(report["categories"], newest=index == 0)
        facility = {"facility_info": self.facility_info(unit, listed), "reports": reports}
        return facility, [r["report_id"] for r in reports], cited

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        counted = sum(1 for r in reports if r["categories"].get("counts_as_violation"))
        logger.info("── Pennsylvania run summary ──")
        logger.info(f"units with new reports: {len(facilities)}")
        logger.info(f"reports: {len(reports)} (counted as violations: {counted}); "
                    f"files listed: {self.stats['files_listed']}; downloaded: {self.stats['downloaded']}")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        for kind in KIND_LABELS:
            n = self.stats.get(f"kind_{kind}", 0)
            share = f" ({100.0 * n / len(reports):.1f}%)" if reports else ""
            logger.info(f"  {kind}: {n}{share}")
        logger.info(f"forms: {dict(self.forms)}; fully scanned (OCR): {self.stats['ocr_documents']}")
        logger.info(f"citations parsed: {self.stats['citations']}")
        for kind in ("citation", "followup", "sanction"):
            logger.info(f"  {kind} documents with no citation parsed: {self.stats.get(f'{kind}_without_citations', 0)}")
        logger.info(f"multi-page documents left as 'other': {self.stats['other_multi_page']}")
        logger.info(f"follow-ups counted because their citation document is not listed: {self.counted_followups}")
        logger.info(f"report dates taken from the document (file name year wrong): {self.stats['date_corrected']}")
        logger.info(f"documents without text: {self.stats['no_text']}")


# ── Output ───────────────────────────────────────────────────────────────────


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = [{
        "facility_info": f["facility_info"],
        "reports": [{**r, "is_structured": True} for r in f["reports"]],
    } for f in facilities]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "PA",
        "scraped_timestamp": timestamp,
        "scraping_notes": {"total_reports": sum(len(f["reports"]) for f in shaped)},
        "facilities": shaped,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"Wrote {path}")


def save_to_api(facilities: List[Dict], timestamp: str) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="PA",
        scraped_timestamp=timestamp,
        facilities=facilities,
        timeout=180,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Pennsylvania children's residential inspection summaries")
    parser.add_argument("--full", action="store_true", help=f"Ignore the seen reports in {STATE_FILE}")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N units (by service code, then name)")
    parser.add_argument("--unit", action="append", default=[], help="Only this licence id, e.g. 448090 (repeatable)")
    parser.add_argument("--service-codes", default=",".join(SERVICE_CODES),
                        help=f"Comma-separated service codes (default {','.join(SERVICE_CODES)})")
    parser.add_argument("--since", type=int, default=0,
                        help="Only files whose name dates from this year on (stage the first run newest years first)")
    parser.add_argument("--batch", type=int, default=40, help="Units per API post; the state advances after each")
    parser.add_argument("--ocr-workers", type=int, default=os.cpu_count() or 4, help="Parallel OCR of scanned reports")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    cited_state: Dict[str, List[str]] = state.setdefault("cited_inspections", {})
    licences: Dict[str, Dict] = state.setdefault("licences", {})
    timestamp = datetime.now().isoformat(timespec="seconds")
    codes = [c.strip() for c in args.service_codes.split(",") if c.strip()]

    scraper = PAScraper(ocr_workers=args.ocr_workers)
    listed = scraper.listed_units(codes)
    today = datetime.now().strftime("%Y-%m-%d")
    for lic, unit in listed.items():
        licences[lic] = {**unit, "last_listed": today}
    # Units no longer listed (closed, revoked): their reports still answer by id.
    units: List[Tuple[Dict, bool]] = [(u, True) for u in listed.values()]
    units += [(u, False) for lic, u in sorted(licences.items())
              if lic not in listed and u.get("service_code") in codes]
    if args.unit:
        wanted = set(args.unit)
        units = [(u, l) for u, l in units if u["lic"] in wanted]
    order = {code: i for i, code in enumerate(codes)}
    units.sort(key=lambda item: (order.get(item[0].get("service_code"), 99),
                                 facility_name(item[0].get("unit") or "", item[0].get("entity") or "").lower()))
    if args.limit:
        units = units[: args.limit]
    # The licence registry is not tied to a post: it only remembers ids.
    save_state(STATE_FILE, state)
    logger.info(f"{len(units)} units to visit ({sum(1 for _, l in units if not l)} no longer listed)")

    all_facilities: List[Dict] = []
    batch: List[Dict] = []
    batch_ids: Dict[str, List[str]] = {}
    batch_cited: Dict[str, Set[str]] = {}
    failed_batches = 0

    def flush() -> None:
        nonlocal failed_batches
        if batch and not args.no_post:
            if save_to_api(list(batch), timestamp):
                merge_new_ids(state, batch_ids)
                for lic, keys in batch_cited.items():
                    cited_state[lic] = sorted(keys)
                save_state(STATE_FILE, state)
                logger.info(f"Posted {len(batch)} units; seen reports advanced")
            else:
                failed_batches += 1
                logger.error("API save failed for this batch -- its seen reports not advanced")
        batch.clear()
        batch_ids.clear()
        batch_cited.clear()

    def finish(item: Tuple[Dict, bool, Optional[Dict]]) -> None:
        unit, is_listed, started = item
        lic = unit["lic"]
        facility, ids, cited = scraper.finish_unit(unit, is_listed, started, set(cited_state.get(lic, [])))
        if not facility:
            return
        all_facilities.append(facility)
        batch.append(facility)
        batch_ids[lic] = ids
        batch_cited[lic] = cited
        if len(batch) >= args.batch:
            flush()

    # Units finish in order; downloads run ahead of the OCR pool by at most
    # MAX_QUEUED documents.
    queue: deque = deque()
    try:
        for index, (unit, is_listed) in enumerate(units, start=1):
            lic = unit["lic"]
            logger.info(f"[{index}/{len(units)}] {facility_name(unit.get('unit') or '', unit.get('entity') or '')} "
                        f"({lic}, code {unit.get('service_code')})" + ("" if is_listed else " [no longer listed]"))
            queue.append((unit, is_listed, scraper.start_unit(unit, seen.get(lic, set()), since=args.since)))
            while queue and (scraper.pending_jobs(queue[0][2]) == 0
                             or sum(scraper.pending_jobs(q[2]) for q in queue) > MAX_QUEUED):
                finish(queue.popleft())
        while queue:
            finish(queue.popleft())
    finally:
        scraper.close()
    flush()

    scraper.print_stats(all_facilities)
    if args.out:
        write_out(args.out, all_facilities, timestamp)
    if not all_facilities:
        logger.info("No new reports since last run")
    elif args.no_post:
        logger.info("Skipping API POST because --no-post was set; seen reports not advanced")
    elif failed_batches:
        logger.error(f"{failed_batches} batch(es) failed; run again to post them")
    else:
        logger.info("Data saved to database successfully!")


if __name__ == "__main__":
    main()
