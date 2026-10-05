"""
Washington DOH Residential Treatment Facility Inspection Scraper

Scrapes the DOH Facility Inspections & Investigations search, downloads
each inspection/investigation/enforcement PDF, extracts and parses the
text, then POSTs to the KOP inspections API for MySQL storage.

Source: https://doh.wa.gov/licenses-permits-and-certificates/facilities-z/
        facilities-inspections-and-investigations-search
        (facility type = Residential Treatment Facility License, id=2879)
"""

import argparse
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pdfplumber
import requests
from bs4 import BeautifulSoup

try:  # OCR for scanned pages; without it a scan keeps its (often unreadable) text layer
    import pytesseract
    from pytesseract import Output
    _TESSERACT = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    if _TESSERACT.exists():
        pytesseract.pytesseract.tesseract_cmd = str(_TESSERACT)
except ImportError:
    pytesseract = None

from inspection_api_client import post_facilities_to_api
from report_store import ReportStore, extract_with_cache
from scraper_state import load_state, merge_new_ids, save_state, seen_from_state

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
API_KEY = os.getenv("KOP_DATA_API_KEY", "CHANGE_ME")

# ── DOH search configuration ────────────────────────────────────────
SEARCH_URL = (
    "https://doh.wa.gov/licenses-permits-and-certificates/"
    "facilities-z/facilities-inspections-and-investigations-search"
)

# Facility types to scrape: (label, target_id, max_pages_to_check)
# max_pages is a safety cap — scraper stops early when it hits an empty page.
# As of 2026-09 the listing runs 6 RTF pages and 34 BHA pages (25 rows each);
# the old 20-page BHA cap silently dropped every facility past page 20 (Pearl Youth Residence is on page 22).
FACILITY_TYPES = [
    ("Residential Treatment Facility", 2879, 30),
    ("Behavioral Health Agency", 2869, 80),
]

REPORTS = ReportStore("WA_PDF_CACHE", "wa_pdfs", Path(__file__).parent / "wa_pdfs")
STATE_FILE = Path(os.getenv("WA_STATE_FILE", ".wa_state.json"))

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

# How the PDFs are read. Bump when extract_pdf() changes: every cached
# extraction made by an older version is read again from the archived PDF.
EXTRACT_VERSION = 2

# The Statement of Deficiency form is a table of three columns: the rule cited,
# the inspector's findings, and the facility's plan of correction. Read line by
# line across the page (pdfplumber's extract_text), the columns come out spliced
# together mid-sentence, so findings are read column by column instead, by the
# column rules or, failing those, the heading positions. Scanned pages whose
# text layer is noise, or that have none, are read with Tesseract.

COMMON = set(
    "the of and to a in was that for is on with as by staff not be this were at from have an are or "
    "facility patient youth review record interview based had did their which".split()
)
RULE_START = re.compile(r"^\s*(?:WAC|RCW)\s*\d{2,3}[-.]\d")
# A deficiency's findings open "Based on ..." (investigations) or "[This] Washington
# Administrative Code was not met as evidenced by:" (inspections) ...
OPENING = re.compile(
    # (inspectors also write "Washington Administration Code", "WAC was not met as ...").
    r"^\s*(?:Based\s+on\b|.{0,60}?\b(?:was|is|were|are)\s+not\s+met\s+as\b)", re.I)
# ... beside a rule citation, or an inspection's deficiency number ("1015 Resident rights").
ROW_START = re.compile(r"^\s*(?:(?:WAC|RCW)\s*\d{2,3}[-.]\d|\d{3,4}\s+[A-Z])")
# Page furniture repeated on continuation pages, never a finding.
FURNITURE = re.compile(
    r"^(?:Page \d+ of \d+|Statement of Deficiency Report|Department of Health|P\.O\. Box|TEL:)\b"
    r"|^(?:Deficiency Number and Rule Reference|(?:Observation )?Findings(?: included)?:?|Plan of Correction)\s*$",
    re.I)


def text_is_readable(text: str) -> bool:
    """False for a scanned page whose embedded text layer is noise."""
    toks = re.findall(r"[A-Za-z]+", text or "")
    if len(toks) < 15:
        return True  # too little to judge; a short page, a signature, a blank
    return sum(t.lower() in COMMON for t in toks) / len(toks) > 0.12


def ocr_words(page) -> Tuple[List[Dict], float, float]:
    """Words read from the page image, upright, in points: (words, width, height)."""
    # 300 dpi, but no side over 3,500 pixels: a few scans are poster-sized and Tesseract runs out of memory.
    dpi = min(300, 3500 * 72 / max(page.width, page.height))
    img = page.to_image(resolution=dpi).original
    try:
        osd = pytesseract.image_to_osd(img, output_type=Output.DICT)
        angle = int(osd.get("rotate", 0))
    except Exception:
        angle = 0
    if angle:
        img = img.rotate(-angle, expand=True)
    data = pytesseract.image_to_data(img, output_type=Output.DICT, config="--psm 3")
    scale = 72 / dpi
    words = []
    for i, t in enumerate(data["text"]):
        t = (t or "").strip()
        if not t or float(data["conf"][i]) < 0:
            continue
        x0 = data["left"][i] * scale
        top = data["top"][i] * scale
        words.append({"text": t, "x0": x0, "x1": x0 + data["width"][i] * scale,
                      "top": top, "bottom": top + data["height"][i] * scale})
    return words, img.width * scale, img.height * scale


def lines_of(words: List[Dict], tol: float = 3.0) -> List[Dict]:
    """Words grouped into lines: [{'top', 'x0', 'text'}], top to bottom."""
    out: List[Dict] = []
    for w in sorted(words, key=lambda w: (round(w["top"]), w["x0"])):
        if out and abs(out[-1]["top"] - w["top"]) <= tol:
            out[-1]["words"].append(w)
        else:
            out.append({"top": w["top"], "words": [w]})
    for line in out:
        line["words"].sort(key=lambda w: w["x0"])
        line["x0"] = line["words"][0]["x0"]
        line["text"] = " ".join(w["text"] for w in line["words"])
    return out


def column_bounds(page, words: List[Dict], from_pdf: bool) -> Optional[Tuple[float, float, float, float]]:
    """(rule left, findings left, plan left, right edge) from the column rules, else the headings."""
    if from_pdf:
        rules = sorted({round(r["x0"]) for r in page.rects if r["height"] > 100 and r["width"] < 4})
        if len(rules) == 4:
            return tuple(float(x) for x in rules)
    heads = {}
    for line in lines_of(words):
        texts = [w["text"] for w in line["words"]]
        if "Findings" in texts and "Plan" in texts:
            # The older form heads the middle column "Observation Findings".
            for w in line["words"]:
                if w["text"] in ("Deficiency", "Observation", "Findings", "Plan") and w["text"] not in heads:
                    heads[w["text"]] = w["x0"]
            if "Observation" in heads and heads["Observation"] < heads.get("Findings", 1e9):
                heads["Findings"] = heads.pop("Observation")
            heads.pop("Observation", None)
            break
    if len(heads) == 3:
        width = page.width if from_pdf else max(w["x1"] for w in words) + 10
        return heads["Deficiency"] - 6, heads["Findings"] - 6, heads["Plan"] - 6, width
    return None


def table_top(words: List[Dict]) -> float:
    """Below the heading row when the page has it, else the top of the page."""
    for line in lines_of(words):
        texts = [w["text"] for w in line["words"]]
        if "Findings" in texts and "Plan" in texts and "Correction" in texts:
            return line["top"] + 8
    return 0.0


def findings_of_fact(text: str) -> List[Dict]:
    """An enforcement document's numbered FINDINGS OF FACT (1.1, 1.2, ...), up to the conclusions of law."""
    m = re.search(r"\bFINDINGS\s+OF\s+FACTS?\b(.*?)(?:\bCONCLUSIONS?\s+OF\s+LAW\b|$)", text, re.S)
    if not m:
        return []
    body = m.group(1)
    # Page footers and running heads of a legal pleading.
    body = re.sub(r"(?im)^.*\b(?:PAGE\s+\d+\s+OF\s+\d+|NOTICE OF INTENT|SUMMARY ACTION ORDER|STATEMENT OF CHARGES)\b.*$", "", body)
    body = re.sub(r"(?im)^\s*NO\.\s*M\d{4}-\d+.*$", "", body)
    out = []
    for part in re.split(r"(?m)^\s*(?=\d\.\d{1,2}\s+\S)", body):
        para = re.sub(r"\s+", " ", re.sub(r"^\s*\d\.\d{1,2}\s+", "", part)).strip()
        if len(para) >= 40:
            out.append({"rule": "Findings of fact", "rule_text": "", "findings": para, "plan": ""})
    return out


def extract_pdf(path: Path) -> Dict:
    """{'text': all page text, 'findings': [{rule, rule_text, findings, plan}], 'ocr_pages': n}."""
    texts: List[str] = []
    deficiencies: List[Dict] = []
    carried: Dict[str, Tuple[float, float, float, float]] = {}
    ocr_pages = 0
    current: Optional[Dict] = None
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            layer = page.extract_text() or ""
            from_pdf = True
            words = None
            # OCR a page whose text layer is noise, or an image-only page with no text layer at all.
            needs_ocr = not text_is_readable(layer) or (len(re.findall(r"[A-Za-z]{2,}", layer)) < 15 and page.images)
            if needs_ocr and pytesseract is not None:
                try:
                    words, _, _ = ocr_words(page)
                except Exception:
                    words = None  # OCR failed on this page: keep its text layer
            if words is not None:
                from_pdf = False
                ocr_pages += 1
                texts.append("\n".join(l["text"] for l in lines_of(words)))
            else:
                words = page.extract_words()
                texts.append(layer)
            if not words:
                continue
            # Column positions carry over to continuation pages, kept apart for text-layer and OCR pages
            # (OCR coordinates come from the upright page image, not the PDF's own).
            found = column_bounds(page, words, from_pdf)
            key = "pdf" if from_pdf else "ocr"
            if found:
                carried[key] = found
            bounds = carried.get(key)
            if not bounds:
                continue
            left, mid, right, edge = bounds
            top = table_top(words)
            cols = {"rule": [], "findings": [], "plan": []}
            for w in words:
                if w["top"] < top:
                    continue
                cx = (w["x0"] + w["x1"]) / 2
                if left <= cx < mid:
                    cols["rule"].append(w)
                elif mid <= cx < right:
                    cols["findings"].append(w)
                elif right <= cx < edge:
                    cols["plan"].append(w)
            # A deficiency starts where its findings open beside a citation or deficiency number.
            # One deficiency often cites several rules in its left column, so a citation alone
            # does not start one.
            rule_starts = [l["top"] for l in lines_of(cols["rule"]) if ROW_START.match(l["text"])]
            starts = [l["top"] for l in lines_of(cols["findings"])
                      if OPENING.match(l["text"]) and any(abs(l["top"] - r) < 6 for r in rule_starts)]
            events = []
            for kind in cols:
                for l in lines_of(cols[kind]):
                    if FURNITURE.match(l["text"]):
                        continue
                    t = l["top"]
                    # Lines of a row are a point or two apart across columns: snap them to the row's start.
                    for s0 in starts:
                        if s0 - 6 <= t < s0:
                            t = s0
                    events.append((t, kind, l["text"]))
            for top_, kind, text in sorted(events, key=lambda e: (e[0], ("rule", "findings", "plan").index(e[1]))):
                if any(abs(top_ - s0) < 0.01 for s0 in starts) and (current is None or current.get("_at") != (id(page), top_)):
                    current = {"rule": [], "findings": [], "plan": [], "_at": (id(page), top_)}
                    deficiencies.append(current)
                if current is None:
                    continue  # text above the first deficiency (the form's header)
                current[kind].append(text)
    out = []
    for d in deficiencies:
        rule = " ".join(d["rule"]).strip()
        findings = "\n".join(d["findings"]).strip()
        if not findings:
            continue
        m = re.match(r"((?:WAC|RCW)\s*[\d.\-]+(?:\([^)]*\))*\s*[^.]{0,120}\.?)", rule)
        out.append({"rule": (m.group(1) if m else rule[:160]).strip(), "rule_text": rule,
                    "findings": findings, "plan": "\n".join(d["plan"]).strip()})
    text = "\n".join(texts).strip()
    if not out:
        out = findings_of_fact(text)
    return {"text": text, "findings": out, "ocr_pages": ocr_pages}


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


def mentions_case(text: str, case_number: str) -> bool:
    """True when the document text names this case number (not as part of a longer one)."""
    return re.search(r"(?<![\w-])" + re.escape(case_number) + r"(?![\w-])", text or "") is not None


def build_unlinked_report(report_num: str, url: str, category: str, owner_case: str) -> Dict:
    """A case DOH lists with another case's PDF: keep the case, drop the borrowed document."""
    label = category.replace("_", " ").title()
    return {
        "report_id": report_num,
        "report_date": "",
        "raw_content": "",
        "content_length": 0,
        "summary": f"{label} — DOH has not published this case's document",
        "categories": {
            "report_category": category,
            "report_type": "",
            "inspection_number": report_num,
            "license_number": "",
            "inspection_type": "",
            "inspection_date": "",
            "inspector": "",
            "administrator": "",
            "service_types": "",
            "pdf_url": "",
            "listed_pdf_url": url,
            "document_owner_case": owner_case,
            "deficiencies": [],
            "violation_count": 0,
        },
    }


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
    def __init__(self, reports: ReportStore = REPORTS):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
        })
        self.reports = reports
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

    def download_pdf(self, url: str) -> Optional[bytes]:
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            time.sleep(0.5)  # be polite
            return resp.content
        except requests.RequestException as e:
            logger.warning(f"  download failed {url}: {e}")
            return None

    def pdf_text(self, url: str) -> Tuple[Optional[Dict], str]:
        """(extraction, file name) of the report PDF: {'text', 'findings',
        'ocr_pages'}, or None when no document could be had. The PDF itself goes
        to the Drive folder."""
        filename = re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])
        extracted = extract_with_cache(
            self.reports,
            filename,
            fetch=lambda: self.download_pdf(url),
            extract=extract_pdf,
            version=EXTRACT_VERSION,
        )
        return extracted, filename

    def build_report(
        self,
        report_num: str,
        url: str,
        category: str,
        sharers: Optional[List[str]] = None,
    ) -> Optional[Dict]:
        extracted, filename = self.pdf_text(url)
        if extracted is None:
            return None
        text = extracted.get("text") or ""

        # DOH sometimes links several case numbers to one case's PDF (twelve
        # Pearl Youth Residence cases all point at 2023-11257.pdf). Only the
        # case named in the document gets its text; the others are recorded
        # without a document so they don't repeat another case's findings.
        if report_num and sharers and len(sharers) > 1 and not mentions_case(text, report_num):
            owners = [n for n in sharers if n != report_num and mentions_case(text, n)]
            if owners:
                logger.warning(f"  {report_num}: DOH links it to case {owners[0]}'s document; storing without it")
                return build_unlinked_report(report_num, url, category, owners[0])
        parsed = parse_inspection_text(text)
        deficiencies = extract_deficiencies(text)

        return {
            "report_id": report_num or Path(filename).stem,
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
                # The inspector's findings, each with the rule it cites, read column by column
                # (or an enforcement document's findings of fact). The facility's plan is not kept.
                "findings": [{"rule": f["rule"], "findings": f["findings"]} for f in extracted.get("findings", [])],
                "ocr_pages": extracted.get("ocr_pages", 0),
            },
        }

    def scrape(self, seen: Optional[Dict[str, Set[str]]] = None
               ) -> Tuple[List[Dict], Dict[str, List[str]]]:
        """Scrape WA DOH, skipping reports whose report_num is in `seen`.

        State is keyed by facility_name. The PDF download and parse only run
        for reports we haven't seen — but reports listed without a report_num
        in the HTML always download (we'd have to read the PDF stem to know).
        """
        seen = seen or {}
        new_ids: Dict[str, List[str]] = {}
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
            seen_for_facility = seen.get(name, set())
            logger.info(f"[{i}/{len(filtered)}] {name} ({row['license_number']})")

            reports: List[Dict] = []
            ids_for_facility: List[str] = []
            skipped = 0
            sharers_by_url: Dict[str, List[str]] = {}
            for links in row["reports_by_category"].values():
                for report_num, url in links:
                    if report_num:
                        sharers_by_url.setdefault(url, []).append(report_num)
            for category, links in row["reports_by_category"].items():
                for report_num, url in links:
                    if report_num and report_num in seen_for_facility:
                        skipped += 1
                        continue
                    report = self.build_report(report_num, url, category, sharers_by_url.get(url))
                    if report:
                        reports.append(report)
                        if report.get("report_id"):
                            ids_for_facility.append(report["report_id"])

            if not reports:
                if skipped:
                    logger.info(f"  no new reports ({skipped} already seen)")
                continue

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
            if ids_for_facility:
                new_ids[name] = ids_for_facility
            extra = f" ({skipped} already seen)" if skipped else ""
            logger.info(f"  {len(reports)} new reports{extra}")

        logger.info(f"Scraping complete: {len(self.all_facilities)} facilities")
        return self.all_facilities, new_ids


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


def write_out(path: Path, facilities: List[Dict]) -> None:
    """What inspections-read.php would return for these facilities."""
    import json
    payload = {
        "source_state": "WA",
        "scraped_timestamp": datetime.now().isoformat(),
        "facilities": [{"facility_info": f["facility_info"], "reports": f["reports"]} for f in facilities],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"Wrote {path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true",
                    help=f"Ignore {STATE_FILE} and re-process all PDFs")
    ap.add_argument("--no-post", action="store_true", help="Scrape and read, but post nothing to the API")
    ap.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = ap.parse_args()

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)

    scraper = WAInspectionScraper()
    facilities, new_ids = scraper.scrape(seen=seen)

    if not facilities:
        logger.info("No new reports since last run")
        return

    total_reports = sum(len(f["reports"]) for f in facilities)
    total_findings = sum(len(r["categories"].get("findings") or []) for f in facilities for r in f["reports"])
    logger.info(f"Scraped {len(facilities)} facilities, {total_reports} new reports, {total_findings} findings read")
    if args.out:
        write_out(args.out, facilities)
    if args.no_post:
        logger.info("--no-post: nothing posted, state not advanced")
        return
    logger.info("Posting to API")
    if save_to_api(facilities):
        logger.info("Data saved to database successfully!")
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
    else:
        logger.error("API save failed — state not advanced")


if __name__ == "__main__":
    main()
