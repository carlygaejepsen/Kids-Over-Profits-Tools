"""
Michigan child caring institution licensing report scraper.

Source: the MDHHS Division of Child Welfare Licensing public search,
https://michildwelfarepubliclicensingsearch.michigan.gov/licagencysrch/
It is a Salesforce Experience Cloud site whose pages call one anonymous Apex
endpoint; the scraper calls it directly:

  getAgenciesDetail   {}                          every licensed agency
  getContentDetails   {"recordId": agencyId}      that agency's documents
  getContentBaseData  {"contentDocumentId": id}   one PDF, base64

Every agency whose AgencyType does not start with "Child Placing Agency" is
taken (institutions, court operated facilities, therapeutic group homes).
Each document is one report: special investigation reports (a complaint, the
interviews and a finding per allegation), and renewal, interim and original
licensing study reports. The PDFs are text PDFs read with pdfplumber; the
allegation blocks are two-column tables, read from the table rows.

Michigan lets a licensee ask for violation reports to be taken down after two
years, so the PDFs are archived to the FileBird Drive folder `mi_pdfs` and the
archived copy is the link readers use (the state has no direct document URL).

Only currently licensed agencies are listed; the state file keeps the last
agencyId per licence so a closed facility's documents are still asked for.
"""

import argparse
import base64
import json
import logging
import os
import re
import tempfile
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import certifi
import pdfplumber
import requests

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
STATE_FILE = Path(os.getenv("MI_STATE_FILE", ".mi_state.json"))
REPORTS = ReportStore("MI_PDF_CACHE", "mi_pdfs", Path(__file__).parent / "mi_pdfs")

SITE = "https://michildwelfarepubliclicensingsearch.michigan.gov/licagencysrch/"
APEX_URL = SITE + "webruntime/api/apex/execute?language=en-US&asGuest=true&htmlEncode=false"
AGENCY_PAGE = SITE + "agency-detail-page?agency={agency_id}"
# Id of the Apex class COM_CWLicensingSearchController. Recovered from the
# site's scripts at run time when the state redeploys and this stops working.
CLASS_ID = os.getenv("MI_CLASS_ID", "@udd/01p8z0000009E4V")
CONTROLLER = "COM_CWLicensingSearchController"

# Child placing agencies (foster care and adoption) are out of scope.
EXCLUDED_TYPE_PREFIXES = ("Child Placing Agency",)

# The server does not send its intermediate certificate; verify against
# certifi plus this copy of it (Sectigo Public Server Authentication CA OV R36,
# from http://crt.sectigo.com/SectigoPublicServerAuthenticationCAOVR36.crt).
INTERMEDIATE_PEM = Path(__file__).parent / "certs" / "sectigo_ov_r36.pem"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 0.5


class ApexClassError(Exception):
    """The Apex class id was rejected ("The Apex request is invalid.")."""


# ── Fetch layer ──────────────────────────────────────────────────────────────


def build_ca_bundle() -> str:
    """certifi's roots plus the intermediate the server leaves out, in a temp file."""
    bundle = Path(certifi.where()).read_text(encoding="utf-8")
    if INTERMEDIATE_PEM.exists():
        bundle += "\n" + INTERMEDIATE_PEM.read_text(encoding="utf-8")
    else:
        logger.warning(f"Intermediate certificate missing: {INTERMEDIATE_PEM}")
    fd, path = tempfile.mkstemp(prefix="kop_mi_ca_", suffix=".pem")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(bundle)
    return path


class MIClient:
    def __init__(self, class_id: str = CLASS_ID):
        self.class_id = class_id
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.session.verify = build_ca_bundle()
        self._last_call = 0.0
        self._recovered = False

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
            except requests.exceptions.SSLError as exc:
                raise RuntimeError(
                    "TLS verification failed for the Michigan licensing site. The server "
                    "does not send its intermediate certificate and the copy in "
                    f"{INTERMEDIATE_PEM} may be out of date: open the site's certificate, "
                    "read the 'CA Issuers' URL from its Authority Information Access "
                    "field, download that certificate, convert it to PEM and replace the "
                    f"file. ({exc})"
                ) from exc
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

    def _execute(self, method: str, params: Dict) -> Any:
        body = {
            "namespace": "",
            "classname": self.class_id,
            "method": method,
            "isContinuation": False,
            "params": params,
            "cacheable": False,
        }
        response = self._request("POST", APEX_URL, json=body)
        if response.status_code == 400 and "Apex request is invalid" in response.text:
            raise ApexClassError(response.text[:200])
        response.raise_for_status()
        return response.json()

    def call(self, method: str, params: Dict) -> Any:
        try:
            return self._execute(method, params)
        except ApexClassError:
            if self._recovered:
                raise
            self._recovered = True
            old = self.class_id
            self.class_id = self.find_class_id()
            logger.warning(f"Apex class id {old} was rejected; using {self.class_id} from the site")
            return self._execute(method, params)

    def find_class_id(self) -> str:
        """Read the controller's class id from the site's view scripts."""
        page = self._request("GET", SITE).text
        paths = re.findall(
            r"webruntime/view/[^\"'\s]+?/(?:home_1_view|agency_Detail_Page_1_view)[^\"'\s]*",
            page,
        )
        ids: List[str] = []
        for path in dict.fromkeys(paths):
            script = self._request("GET", SITE + path.lstrip("/")).text
            for match in re.finditer(CONTROLLER, script):
                window = script[max(0, match.start() - 400): match.end() + 400]
                found = re.findall(r"@udd/01p[0-9A-Za-z]+", window)
                if found:
                    # The id nearest the controller name.
                    found.sort(key=lambda value: abs(window.find(value) - 400))
                    return found[0]
            ids += re.findall(r"@udd/01p[0-9A-Za-z]+", script)
        if len(set(ids)) == 1:
            return ids[0]
        raise RuntimeError(
            f"Could not find the {CONTROLLER} class id in the site's scripts "
            f"({len(paths)} view scripts, candidate ids: {sorted(set(ids))}). "
            "Open the search page with the browser's network tab and copy the "
            "'classname' of an apex/execute request into MI_CLASS_ID."
        )

    # -- the three methods --

    def agencies(self) -> List[Dict]:
        data = self.call("getAgenciesDetail", {})
        return data["returnValue"]["objectData"]["responseResult"] or []

    def documents(self, agency_id: str) -> List[Dict]:
        data = self.call("getContentDetails", {"recordId": agency_id})
        value = data.get("returnValue") or {}
        return value.get("contentVersionRes") or []

    def pdf(self, document_id: str) -> Optional[bytes]:
        """The document's PDF bytes. The site now and then answers with
        something that is not the document (seen 2026-09-30, fine on the next
        ask), so a bad answer is asked for again before giving up; nothing
        failed is cached, so the next run tries once more."""
        problem = ""
        for attempt in range(3):
            if attempt:
                time.sleep(3 * attempt)
            try:
                data = self.call(
                    "getContentBaseData",
                    {"contentDocumentId": document_id, "actionName": "download"},
                )
            except (requests.RequestException, ValueError) as exc:
                problem = f"download failed: {exc}"
                continue
            encoded = data.get("returnValue") if isinstance(data, dict) else None
            if not isinstance(encoded, str) or not encoded:
                problem = "no document returned"
                continue
            try:
                raw = base64.b64decode(encoded, validate=True)
            except ValueError:
                problem = f"not base64 ({encoded[:40]!r})"
                continue
            if not raw.startswith(b"%PDF"):
                problem = f"not a PDF ({raw[:12]!r})"
                continue
            return raw
        logger.warning(f"  {document_id}: {problem}; skipped for this run")
        return None


# ── PDF extraction ───────────────────────────────────────────────────────────


def extract_pdf(path: Path) -> Dict:
    """Text plus the rows of every table, pages joined in order."""
    pages: List[str] = []
    rows: List[List[str]] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
            for table in page.extract_tables():
                for row in table:
                    rows.append([(cell or "").strip() for cell in row])
    text = "\n".join(pages).strip()
    if not text:
        # A scanned report (3 of 1,889 on 2026-09-30): OCR it as nc_scraper does.
        text = ocr_pdf(path)
        return {"text": text, "rows": [], "ocr": bool(text)}
    return {"text": text, "rows": rows}


def ocr_pdf(path: Path) -> str:
    try:
        import pytesseract
        from pdf2image import convert_from_path
    except ImportError:
        logger.warning(f"  {path.name} is a scan and pytesseract/pdf2image are not installed")
        return ""
    if os.getenv("TESSERACT_CMD"):
        pytesseract.pytesseract.tesseract_cmd = os.getenv("TESSERACT_CMD")
    try:
        images = convert_from_path(str(path), dpi=200, poppler_path=os.getenv("POPPLER_PATH") or None)
        return "\n".join(pytesseract.image_to_string(image) for image in images).strip()
    except Exception as exc:  # poppler or tesseract missing or failing
        logger.warning(f"  OCR failed for {path.name}: {exc}")
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
NUMERIC_DATE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")


def iso_date(value: str) -> str:
    """The first date in `value` as YYYY-MM-DD, or ''."""
    if not value:
        return ""
    match = LONG_DATE.search(value)
    if match:
        month = MONTHS[match.group(1).lower()]
        try:
            return datetime(int(match.group(3)), month, int(match.group(2))).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    match = NUMERIC_DATE.search(value)
    if match:
        try:
            return datetime(int(match.group(3)), int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""


def clean(value: str) -> str:
    value = (value or "").replace("�", "'").replace(" ", " ")
    return re.sub(r"[ \t]+", " ", value).strip()


def one_line(value: str) -> str:
    return re.sub(r"\s+", " ", clean(value)).strip()


def field(text: str, label: str) -> str:
    """The value after `label:` on its line."""
    match = re.search(rf"(?im)^\s*{label}\s*:?[ \t]*(.*)$", text)
    return one_line(match.group(1)) if match else ""


DOC_TYPE_LABELS = {
    "special_investigation": "Special investigation",
    "renewal": "Renewal inspection",
    "interim": "Interim inspection",
    "original": "Original licensing study",
    "other": "Report",
}


LETTER_TYPES = [
    ("special_investigation", r"Special\s+Investigation\s+Report"),
    ("renewal", r"Renewal\s+(?:Inspection|Licensing\s+Study)\s+Report"),
    ("interim", r"Interim\s+(?:Inspection|Licensing\s+Study)\s+Report"),
    ("original", r"Original\s+(?:Licensing\s+Study|License)\s+Report"),
]


def classify(text: str, title: str) -> str:
    # The cover letter names the report: "Attached is the Renewal Inspection Report".
    attached = re.search(r"Attached\s+(?:is|are)\s+the\s+(.{0,80})", text[:5000], re.I | re.S)
    if attached:
        named = one_line(attached.group(1))
        for doc_type, pattern in LETTER_TYPES:
            if re.match(pattern, named, re.I):
                return doc_type
    head = text[:4000]
    if re.search(r"SPECIAL\s+INVESTIGATION\s+REPORT", head, re.I):
        return "special_investigation"
    for doc_type, pattern in (
        ("renewal", r"Renewal\s+(?:Inspection|Licensing\s+Study|Report|Special\s+Evaluation)"),
        ("interim", r"Interim\s+(?:Inspection|Licensing|Report)"),
        ("original", r"Original\s+(?:Licensing|License|Report|Application)"),
    ):
        if re.search(pattern, head, re.I):
            return doc_type
    upper = (title or "").upper()
    if re.search(r"\d{4}SIC\d|C\d+SI\b|_SIR\b|_SIR_|\bSIR\b|SPECIAL INV", upper):
        return "special_investigation"
    if "RNWL" in upper or "RENEWAL" in upper:
        return "renewal"
    if "INSP" in upper or "INTERIM" in upper:
        return "interim"
    if "ORIG" in upper:
        return "original"
    return "other"


def cover_letter(text: str) -> str:
    """The text before the report form's department heading."""
    match = re.search(r"MICHIGAN DEPARTMENT OF HEALTH AND HUMAN SERVICES\s*\n\s*DIVISION OF CHILD", text)
    if not match:
        match = re.search(r"(?m)^\s*I\.\s+IDENTIFYING INFORMATION", text)
    return text[: match.start()] if match else text[:2500]


def cover_outcome(letter: str) -> str:
    """The cover letter's summary: the sentences after 'Attached is the ...'."""
    match = re.search(r"Attached\s+is\s+the\b(.*?)(?:Please\s+review|Please\s+contact|If\s+you\s+have|Sincerely)", letter, re.S | re.I)
    if not match:
        return ""
    body = one_line(match.group(1))
    # The sentence after "... Report for the above referenced facility."; the
    # rest is how to submit a corrective action plan, which the full text keeps.
    parts = re.split(r"(?<=[.])\s+", body)
    return parts[1].strip() if len(parts) > 1 else ""


def needs_cap(letter: str) -> bool:
    letter = one_line(letter)
    if re.search(r"corrective action plan", letter, re.I):
        if re.search(r"(?:no|not)\s+(?:\w+\s+){0,4}corrective action plan", letter, re.I):
            return False
        return True
    return False


ROW_LABELS = [
    ("rule_title", re.compile(r"^rule\s*code\s*title$")),
    ("rule", re.compile(r"^rule\s*code\s*(?:title\s*)?(?:&|and)?\s*section(?:\s*description)?$")),
    ("violation_type", re.compile(r"^violation\s*type$")),
    ("allegation", re.compile(r"^allegation$")),
    ("investigation", re.compile(r"^investigation$")),
    ("analysis", re.compile(r"^analysis$")),
    ("conclusion", re.compile(r"^conclusion$")),
]


def row_label(label: str) -> Optional[str]:
    key = re.sub(r"\s+", " ", label).strip().lower()
    for name, pattern in ROW_LABELS:
        if pattern.match(key):
            return name
    return None


def parse_allegation_rows(rows: List[List[str]]) -> List[Dict]:
    """Allegation blocks from the section III two-column tables (2022 onward)."""
    blocks: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None
    last_key: Optional[str] = None
    for row in rows:
        cells = [c for c in row]
        if len(cells) < 2:
            continue
        label, value = cells[0], "\n".join(c for c in cells[1:] if c)
        key = row_label(label) if label else None
        if label and key is None:
            last_key = None
            continue
        if key is None:
            if current is not None and last_key:
                current[last_key] = (current.get(last_key, "") + "\n" + value).strip()
            continue
        starts_block = (
            current is None
            or key == "rule_title"
            or (key in current and key != last_key)
            or (key == "rule" and current.get("conclusion"))
        )
        if starts_block:
            current = {}
            blocks.append(current)
        current[key] = (current.get(key, "") + "\n" + value).strip() if key in current else value
        last_key = key
    return inherit_allegations([b for b in blocks if b.get("allegation") or b.get("conclusion") or b.get("rule")])


TEXT_LABEL = re.compile(
    r"^\s*(ALLEGATIONS?|ADDITIONAL FINDINGS?|INVESTIGATION|APPLICABLE RULES?|ANALYSIS|CONCLUSIONS?)\s*:?[ \t]*(.*)$"
)
TEXT_KEYS = {
    "ALLEGATION": "allegation", "ALLEGATIONS": "allegation",
    "ADDITIONAL FINDING": "allegation", "ADDITIONAL FINDINGS": "allegation",
    "INVESTIGATION": "investigation", "APPLICABLE RULE": "rule", "APPLICABLE RULES": "rule",
    "ANALYSIS": "analysis", "CONCLUSION": "conclusion", "CONCLUSIONS": "conclusion",
}


def parse_allegation_text(text: str) -> List[Dict]:
    """Allegation blocks from the older text form (to 2021): capitalised labels
    ALLEGATION: / INVESTIGATION: / APPLICABLE RULE / ANALYSIS: / CONCLUSION:.
    One allegation can be followed by several rule, analysis, conclusion runs."""
    start = re.search(r"(?m)^\s*(?:ALLEGATIONS?|ADDITIONAL FINDINGS?|INVESTIGATION)\s*:", text)
    if not start:
        return []
    body = text[start.start():]
    stop = re.search(r"(?m)^\s*[IVX]+\.\s+RECOMMENDATION", body)
    if stop:
        body = body[:stop.start()]
    blocks: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None
    key: Optional[str] = None
    for line in body.split("\n"):
        match = TEXT_LABEL.match(line)
        if match:
            key = TEXT_KEYS[match.group(1)]
            if current is None or key in current or (key in ("allegation", "rule") and current.get("conclusion")):
                current = {}
                blocks.append(current)
            current[key] = match.group(2).strip()
            continue
        if current is not None and key:
            current[key] = (current.get(key, "") + "\n" + line).strip()
    return inherit_allegations([b for b in blocks if b.get("conclusion") or b.get("rule")])


def inherit_allegations(blocks: List[Dict]) -> List[Dict]:
    """A rule run with no allegation of its own belongs to the one before it."""
    previous = ""
    for block in blocks:
        if block.get("allegation"):
            previous = block["allegation"]
        elif previous:
            block["allegation"] = previous
    return blocks


# Seen in the corpus: "Violation Established", "REPEAT VIOLATION ESTABLISHED SIR ...",
# "Repeated Violation Established", "REPEAT VIOLATION 2026SIC...", "VIOLATON
# ESTABLISHED", "Not Violation Established", "No Violation Established",
# "Allegation 1 and Allegation 2 REPEAT VIOLATION ESTABLISHED ...".
CONCLUSION_HEAD = re.compile(
    r"^\s*(?:Allegations?\s*#?\s*\d+(?:\s*(?:and|&|,)\s*(?:Allegations?\s*#?\s*)?\d+)*\s*[^\w\s]*\s*)?"
    r"((?:Repeat(?:ed)?\s+)?(?:No\s+|Not\s+)?Viola?t\w*(?:\s+(?:Not\s+)?Estab\w*)?)[\s.:;,'-]*",
    re.I,
)
ESTABLISHED = {"Violation Established", "Repeat Violation Established"}


def split_conclusion(value: str) -> Tuple[str, str]:
    """'REPEAT VIOLATION ESTABLISHED SIR 2021C0102011, CAP approved ...' ->
    ('Repeat Violation Established', 'SIR 2021C0102011, CAP approved ...')."""
    value = one_line(value)
    match = CONCLUSION_HEAD.match(value)
    if not match:
        return value, ""
    words = match.group(1).lower().split()
    repeat = words[0].startswith("repeat")
    negative = "not" in words or "no" in words
    if not negative and not repeat and not any(w.startswith("estab") for w in words):
        return value, ""  # a bare "Violation ..." says nothing either way
    if negative:
        head = "Violation Not Established"
    else:
        head = "Repeat Violation Established" if repeat else "Violation Established"
    return head, value[match.end():].strip()


def rule_code(rule_text: str) -> str:
    first = one_line(rule_text.split("\n", 1)[0]) if rule_text else ""
    match = re.search(r"((?:CCI|CPA|MCL|R)\s*(?:Rule\s*)?[\d.]+(?:\([^)]*\))?)", first, re.I)
    if match:
        return one_line(match.group(1))
    match = re.search(r"\b(\d{3}\.\d+[a-z]?)\b", first)
    return match.group(1) if match else first[:60]


def is_established(conclusion: str) -> bool:
    return split_conclusion(conclusion)[0] in ESTABLISHED


def section(text: str, start: str, stops: List[str]) -> str:
    match = re.search(start, text, re.I | re.M)
    if not match:
        return ""
    rest = text[match.end():]
    end = len(rest)
    for stop in stops:
        found = re.search(stop, rest, re.I | re.M)
        if found and found.start() < end:
            end = found.start()
    return rest[:end].strip()


def recommendation(text: str) -> str:
    body = section(text, r"^\s*[IVX]+\.\s+RECOMMENDATION\S*", [r"_{5,}", r"^\s*Approved By"])
    return one_line(body)


RULE_LINE = re.compile(
    r"(?m)^\s*(?:CCI\s+Rule|Rule|R\.?|MCL)\s*(\d{3}\.\d{2,}[a-z]?)\b[ \t]*(.*)$"
)


def violation_section(text: str) -> str:
    """Where an inspection report lists what it cites: 'C. Rule/Statutory
    Violations' (newer form) or the findings paragraph 'in compliance with all
    applicable rules ... except for the following:' (older form)."""
    body = section(
        text,
        r"^\s*[A-Z]\.\s+Rule\s*/?\s*Statutory\s+Violations[^\n]*$",
        [r"^\s*[IVX]+\.\s", r"^\s*[D-H]\.\s+[A-Z][a-z]"],
    )
    if body:
        return body
    findings = section(text, r"^\s*[IVX]+\.\s+DESCRIPTION OF FINDINGS[^\n]*$", [r"^\s*[IVX]+\.\s"])
    match = re.search(r"except\s+for\s+the\s+following\s*:?", findings, re.I)
    if not match:
        return ""
    rest = findings[match.end():]
    stop = re.search(r"(?m)^\s*2\.\)", rest)
    return rest[: stop.start()] if stop else rest


def cited_rules(text: str) -> List[Dict[str, str]]:
    """Rules an inspection report cites, with their titles, in order."""
    body = violation_section(text)
    rules: Dict[str, str] = {}
    for match in RULE_LINE.finditer(body):
        number = match.group(1)
        if number not in rules:
            rules[number] = one_line(match.group(2)).rstrip(".")[:160]
    return [{"rule": number, "title": title} for number, title in rules.items()]


def parse_report(text: str, rows: List[List[str]], title: str) -> Dict:
    doc_type = classify(text, title)
    letter = cover_letter(text)
    ident = section(text, r"^\s*I\.\s+IDENTIFYING INFORMATION", [r"^\s*II\.\s"]) or text

    categories: Dict[str, Any] = {
        "doc_type": doc_type,
        "title": title or "",
        "letter_date": iso_date(letter[:600]),
        "si_number": (field(ident, r"Special Investigation\s*#") or field(ident, r"Investigation\s*#")
                      or field(letter, r"SI\s*#") or field(letter, r"Investigation\s*#")),
        "intake_date": iso_date(field(ident, r"Special Investigation Intake Date")
                                or field(ident, r"Complaint Receipt Date")),
        "inspection_date": iso_date(field(ident, r"(?:Date of )?(?:On-?site )?Inspection Date")
                                    or field(ident, r"Inspection Date\(s\)")),
        "licensee": field(ident, r"Licensee Group Organization") or field(ident, r"Licensee Name"),
        "chief_administrator": field(ident, r"Chief Administrator") or field(ident, r"Administrator"),
        "capacity": field(ident, r"Capacity"),
        "program_type": field(ident, r"Program Type"),
        "cap_required": needs_cap(letter),
        "outcome": cover_outcome(letter),
        "allegations": [],
        "violations_established": 0,
        "cited_rules": [],
        "recommendation": recommendation(text),
    }

    if doc_type == "special_investigation":
        blocks = parse_allegation_rows(rows) or parse_allegation_text(text)
        for block in blocks:
            conclusion, note = split_conclusion(block.get("conclusion", ""))
            rule_text = block.get("rule", "")
            rule_title = one_line(block.get("rule_title", ""))
            if not rule_title:
                # Older form: "R 400.4112 Criminal history check ...; staff qualifications."
                first = RULE_LINE.search(rule_text)
                if first:
                    rule_title = one_line(first.group(2)).rstrip(".")[:160]
                    if not rule_title:
                        # Table form: "CCI Rule 400.4150\nIncident reporting\n(1) Any ..."
                        after = rule_text[first.end():].strip().split("\n", 1)[0].strip()
                        if after and not after.startswith("(") and len(after) <= 100:
                            rule_title = one_line(after).rstrip(".")
            categories["allegations"].append({
                "rule": rule_code(rule_text),
                "rule_title": rule_title,
                "allegation": one_line(block.get("allegation", ""))[:600],
                "conclusion": conclusion,
                "conclusion_note": note,
            })
        categories["violations_established"] = sum(
            1 for a in categories["allegations"] if is_established(a["conclusion"])
        )
        rules = [re.sub(r"^\D+", "", a["rule"]) for a in categories["allegations"]
                 if is_established(a["conclusion"])]
        categories["cited_rules"] = [r for r in dict.fromkeys(rules) if r]
    else:
        cited = cited_rules(text)
        categories["cited_rules"] = [c["rule"] for c in cited]
        categories["violations"] = cited

    return categories


def summarize(categories: Dict) -> str:
    label = DOC_TYPE_LABELS.get(categories["doc_type"], "Report")
    if categories["doc_type"] == "special_investigation":
        # One allegation can be checked against several rules; count allegations.
        outcome: Dict[str, bool] = {}
        for index, allegation in enumerate(categories["allegations"]):
            key = allegation["allegation"] or f"#{index}"
            outcome[key] = outcome.get(key, False) or is_established(allegation["conclusion"])
        total = len(outcome)
        established = sum(1 for value in outcome.values() if value)
        if total:
            noun = "allegation" if total == 1 else "allegations"
            return f"{label}: {established} of {total} {noun} established"
        return label
    if categories["cap_required"] or categories["cited_rules"]:
        count = len(categories["cited_rules"])
        if count:
            return f"{label}: {count} rule{'s' if count != 1 else ''} cited, corrective action plan required"
        return f"{label}: corrective action plan required"
    return f"{label}: in compliance"


def is_flagged(categories: Dict) -> bool:
    if categories["violations_established"] > 0:
        return True
    if categories["doc_type"] != "special_investigation":
        return bool(categories["cap_required"] or categories["cited_rules"])
    return False


# ── Names ────────────────────────────────────────────────────────────────────

KEEP_UPPER = {"LLC", "INC", "LLP", "PLLC", "PC", "USA", "II", "III", "IV", "VI", "VII", "VIII",
              "IX", "XI", "XII", "MDHHS", "DHHS", "YMCA", "CCI", "ICF", "PRTF", "RTC", "BJ"}
SMALL_WORDS = {"of", "and", "the", "for", "at", "in", "on", "a", "an", "to", "by"}


def display_name(name: str) -> str:
    """Title-case names written in full capitals; leave mixed-case names alone."""
    name = one_line(name)
    letters = [c for c in name if c.isalpha()]
    if not letters or any(c.islower() for c in letters):
        return name

    def fix(word: str, first: bool) -> str:
        bare = re.sub(r"[^A-Za-z.]", "", word)
        if bare.rstrip(".") in KEEP_UPPER or re.fullmatch(r"(?:[A-Z]\.)+[A-Z]?\.?", bare) or len(bare.rstrip(".")) == 1:
            return word
        lowered = word.lower()
        if not first and lowered in SMALL_WORDS:
            return lowered
        # Capitalise after hyphens and slashes; keep "'s" lower.
        return re.sub(r"(^|[-/(\"])([a-z])", lambda m: m.group(1) + m.group(2).upper(), lowered)

    words = name.split(" ")
    return " ".join(fix(word, i == 0) for i, word in enumerate(words))


def format_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return value or ""


def full_address(agency: Dict) -> str:
    """'3030 Long Lane Rd, Evart, Osceola, MI 49639, USA' -> without the county and country."""
    address = re.sub(r",\s*(?:USA|UNITED STATES)$", "", one_line(agency.get("Address") or ""), flags=re.I)
    county = one_line(agency.get("County") or "")
    if county:
        address = re.sub(rf",\s*{re.escape(county)}(?=,\s*MI\b)", "", address, flags=re.I)
    return address


# ── Scraper ──────────────────────────────────────────────────────────────────


def in_scope(agency: Dict) -> bool:
    agency_type = agency.get("AgencyType") or ""
    return not agency_type.startswith(EXCLUDED_TYPE_PREFIXES)


class MIScraper:
    def __init__(self, client: Optional[MIClient] = None, reports: ReportStore = REPORTS):
        self.client = client or MIClient()
        self.reports = reports
        self.stats: Counter = Counter()
        self.conclusions: Counter = Counter()
        self.skipped_types: Counter = Counter()

    def fetch_report(self, license_number: str, doc: Dict) -> Optional[Dict]:
        document_id = doc.get("ContentDocumentId") or ""
        extension = (doc.get("FileExtension") or "").lower()
        if not document_id:
            return None
        if extension and extension != "pdf":
            self.skipped_types[extension] += 1
            logger.warning(f"  {document_id} is a .{extension} file; skipped")
            return None
        archive_name = f"{license_number}_{document_id}.pdf"
        extracted = extract_with_cache(
            self.reports,
            archive_name,
            fetch=lambda: self.client.pdf(document_id),
            extract=extract_pdf,
        )
        if not extracted or not extracted.get("text"):
            self.stats["no_text"] += 1
            logger.warning(f"  no text for {archive_name}")
            return None
        if classify(extracted["text"], doc.get("Title") or "") == "other":
            # Not a licensing report: agencies' own attachments (a seclusion
            # observation sheet on 2026-09-30) can name a resident, where the
            # reports use "Youth A". Never posted, and the archived copy is
            # removed so the nightly sync does not put it on the site.
            self.stats["not_a_report"] += 1
            logger.warning(f"  {archive_name} is not a licensing report; not posted, archive copy removed")
            try:
                (self.reports.archive_dir / archive_name).unlink()
            except OSError:
                pass
            return None
        return self.build_report(doc, extracted, archive_name)

    def build_report(self, doc: Dict, extracted: Dict, archive_name: str) -> Dict:
        text = extracted.get("text", "")
        title = doc.get("Title") or ""
        categories = parse_report(text, extracted.get("rows") or [], title)
        categories["archive_name"] = archive_name
        created = iso_date_from_iso(doc.get("CreatedDate") or "")
        report_date = categories.pop("letter_date") or created
        categories["posted_date"] = created

        self.stats[categories["doc_type"]] += 1
        if categories["doc_type"] == "special_investigation" and not categories["allegations"]:
            self.stats["si_without_allegations"] += 1
        for allegation in categories["allegations"]:
            self.conclusions[allegation["conclusion"] or "(empty)"] += 1

        return {
            "report_id": doc["ContentDocumentId"],
            "report_date": report_date,
            "report_url": "",
            "raw_content": text,
            "content_length": len(text),
            "summary": summarize(categories),
            "categories": categories,
            "is_flagged": is_flagged(categories),
        }

    def facility_info(self, agency: Dict, reports: List[Dict], listed: bool) -> Dict:
        newest = sorted(reports, key=lambda r: r["report_date"], reverse=True)
        director = next((r["categories"]["chief_administrator"] for r in newest
                         if r["categories"].get("chief_administrator")), "")
        capacity = next((r["categories"]["capacity"] for r in newest
                         if r["categories"].get("capacity")), "")
        status = agency.get("LicenseStatus") or ""
        if not listed:
            status = f"No longer listed by the state{f' (last status: {status})' if status else ''}"
        return {
            "facility_name": display_name(agency.get("AgencyName") or agency.get("LicenseNumber") or ""),
            "program_name": agency.get("LicenseNumber") or "",
            "program_category": agency.get("AgencyType") or "",
            "full_address": full_address(agency),
            "phone": format_phone(agency.get("Phone") or ""),
            "bed_capacity": capacity,
            "executive_director": director,
            "license_exp_date": agency.get("LicenseExpirationDate") or "",
            "relicense_visit_date": "",
            "action": status,
        }

    def scrape(
        self,
        seen: Dict[str, Set[str]],
        known_agencies: Dict[str, Dict],
        limit: int = 0,
        only: Optional[Set[str]] = None,
    ) -> Tuple[List[Dict], Dict[str, List[str]], Dict[str, Dict]]:
        listed = [a for a in self.client.agencies() if in_scope(a)]
        logger.info(f"{len(listed)} in-scope agencies listed by the state")
        by_license = {a["LicenseNumber"]: a for a in listed if a.get("LicenseNumber")}
        agencies: List[Tuple[Dict, bool]] = [(a, True) for a in by_license.values()]
        # Facilities that dropped off the list: their documents may still answer by id.
        for license_number, stored in sorted(known_agencies.items()):
            if license_number not in by_license and stored.get("agencyId"):
                agencies.append((stored, False))
        if only:
            agencies = [(a, l) for a, l in agencies
                        if a.get("LicenseNumber") in only or a.get("agencyId") in only]
        agencies.sort(key=lambda item: (item[0].get("AgencyName") or "").lower())
        if limit:
            agencies = agencies[:limit]

        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        registry: Dict[str, Dict] = {}
        for index, (agency, is_listed) in enumerate(agencies, start=1):
            license_number = agency["LicenseNumber"]
            record = {k: agency.get(k) for k in (
                "agencyId", "AgencyName", "AgencyType", "Address", "City", "County", "ZipCode",
                "Phone", "LicenseNumber", "LicenseStatus", "LicenseEffectiveDate",
                "LicenseExpirationDate", "LicenseeGroupOrganizationName")}
            if is_listed:
                record["last_listed"] = datetime.now().strftime("%Y-%m-%d")
            else:
                record["last_listed"] = agency.get("last_listed", "")
            registry[license_number] = record

            logger.info(f"[{index}/{len(agencies)}] {agency.get('AgencyName')} ({license_number})"
                        + ("" if is_listed else " [no longer listed]"))
            try:
                docs = self.client.documents(agency["agencyId"])
            except (requests.RequestException, ValueError, KeyError) as exc:
                logger.error(f"  document list failed: {exc}")
                continue
            already = seen.get(license_number, set())
            fresh = [d for d in docs if d.get("ContentDocumentId") and d["ContentDocumentId"] not in already]
            self.stats["documents_listed"] += len(docs)
            if not fresh:
                continue
            reports = []
            for doc in fresh:
                report = self.fetch_report(license_number, doc)
                if report:
                    report["report_url"] = AGENCY_PAGE.format(agency_id=agency["agencyId"])
                    reports.append(report)
            if not reports:
                continue
            reports.sort(key=lambda r: r["report_date"], reverse=True)
            facilities.append({
                "facility_info": self.facility_info(agency, reports, is_listed),
                "reports": reports,
            })
            new_ids[license_number] = [r["report_id"] for r in reports]
        return facilities, new_ids, registry

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        logger.info("── Michigan run summary ──")
        logger.info(f"facilities with new reports: {len(facilities)}")
        logger.info(f"reports: {len(reports)} (flagged: {sum(1 for r in reports if r['is_flagged'])})")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        for doc_type in DOC_TYPE_LABELS:
            logger.info(f"  {doc_type}: {self.stats.get(doc_type, 0)}")
        if reports:
            share = 100.0 * self.stats.get("other", 0) / len(reports)
            logger.info(f"  share in 'other': {share:.1f}%")
        logger.info(f"special investigations with no allegation block: {self.stats.get('si_without_allegations', 0)}")
        logger.info(f"documents without text: {self.stats.get('no_text', 0)}")
        logger.info(f"documents that are not licensing reports (skipped): {self.stats.get('not_a_report', 0)}")
        if self.skipped_types:
            logger.info(f"non-PDF documents skipped: {dict(self.skipped_types)}")
        logger.info("conclusion values:")
        for value, count in self.conclusions.most_common():
            logger.info(f"  {count:5d}  {value}")


def iso_date_from_iso(value: str) -> str:
    match = re.match(r"(\d{4}-\d{2}-\d{2})", value or "")
    return match.group(1) if match else ""


def strip_internal(facilities: List[Dict]) -> List[Dict]:
    """Drop fields that are only for this script before posting."""
    out = []
    for facility in facilities:
        out.append({
            "facility_info": facility["facility_info"],
            "reports": [{k: v for k, v in r.items() if k != "is_flagged"} for r in facility["reports"]],
        })
    return out


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = []
    for facility in strip_internal(facilities):
        shaped.append({
            "facility_info": facility["facility_info"],
            "reports": [
                {**report, "is_structured": True} for report in facility["reports"]
            ],
        })
    payload = {
        "total_facilities": len(shaped),
        "source_state": "MI",
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
        state="MI",
        scraped_timestamp=timestamp,
        facilities=strip_internal(facilities),
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Michigan child caring institution licensing reports")
    parser.add_argument("--full", action="store_true", help=f"Ignore the seen reports in {STATE_FILE}")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N agencies (by name)")
    parser.add_argument("--agency", action="append", default=[],
                        help="Only this licence number or agencyId (repeatable)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    timestamp = datetime.now().isoformat(timespec="seconds")

    scraper = MIScraper()
    facilities, new_ids, registry = scraper.scrape(
        seen=seen,
        known_agencies=state.get("agencies", {}),
        limit=args.limit,
        only=set(args.agency) or None,
    )
    scraper.print_stats(facilities)

    # The agency registry is not tied to a post: it only remembers ids.
    state.setdefault("agencies", {}).update(registry)
    save_state(STATE_FILE, state)

    if args.out:
        write_out(args.out, facilities, timestamp)
    if not facilities:
        logger.info("No new reports since last run")
        return
    if args.no_post:
        logger.info("Skipping API POST because --no-post was set; seen reports not advanced")
        return
    if save_to_api(facilities, timestamp):
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- seen reports not advanced")


if __name__ == "__main__":
    main()
