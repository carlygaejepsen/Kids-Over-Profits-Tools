"""
West Virginia health-facility survey scraper (youth providers).

Source: the Office of Health Facility Licensure and Certification (OHFLAC)
lookup, https://ohflac.wvdhhr.org/Apps/Lookup/FacilitySearch

  POST Lookup/FacilitySearch            the facility list (DataTables JSON)
  GET  Lookup/SurveyHistory/<id>        a facility's surveys back to 2001
                                        (HTTP 500 = the facility has none)
  GET  SurveyForms/Display2567?...      one statement of deficiencies, a PDF
                                        generated on request

OHFLAC licenses the behavioral health side of a provider and certifies
psychiatric residential treatment facilities. The group home licence itself
is held by the Bureau for Social Services, which publishes nothing, so a group
home appears here only through its operator's behavioral health licence.

Scope: the list has no field for the population served, so the facilities are
an explicit allowlist, wv_scope.json beside this file (decision "included").
Records that look like youth providers and are not in that file are logged.

The template trap: every PDF carries a second text layer left over from the
form template (a 2004 deficiency, tag C 173, on state forms; nursing home tag
F 156 on federal forms). It is drawn in an embedded Arial subset and hidden;
the real report is drawn in the standard fonts Helvetica and Helvetica-Bold.
Only characters in those two fonts are read. A share of the reports is also
OCR'd from the rendered page and compared, so a wrong filter cannot pass
unnoticed (see print_stats).

The PDFs are archived to the FileBird Drive folder `wv_pdfs` as <saveas>.pdf.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pdfplumber
import requests
from bs4 import BeautifulSoup

from inspection_api_client import post_facilities_to_api
from report_store import EXTRACT_CACHE_ROOT, ReportStore, extract_with_cache
from scraper_state import load_state, merge_new_ids, save_state, seen_from_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
for noisy in ("pdfminer", "pdfminer.pdfpage", "pdfminer.pdfinterp", "pdfminer.cmapdb", "pdfplumber"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("KOP_DATA_API_KEY", "CHANGE_ME")
STATE_FILE = Path(os.getenv("WV_STATE_FILE", ".wv_state.json"))
SCOPE_FILE = Path(os.getenv("WV_SCOPE_FILE", Path(__file__).parent / "wv_scope.json"))
REPORTS: Optional[ReportStore] = None  # made in main(): locating the Drive folder can ask a question

BASE = "https://ohflac.wvdhhr.org/Apps/"
SEARCH_URL = BASE + "Lookup/FacilitySearch"
HISTORY_URL = BASE + "Lookup/SurveyHistory/{id}"
DETAILS_URL = BASE + "Lookup/FacilityDetails/{id}"
PDF_URL = BASE + "SurveyForms/Display2567"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
# The PDFs are generated per request: one request a second, never in parallel.
REQUEST_GAP = float(os.getenv("WV_REQUEST_GAP", "1.0"))
# A survey list fetched this recently is reused, so a run that stopped half
# way does not ask for every list again.
HISTORY_TTL_HOURS = float(os.getenv("WV_HISTORY_TTL_HOURS", "12"))
MAX_DOWNLOAD_FAILURES = 5
# A cited survey this recent with no plan of correction yet is asked for
# again on later runs: the state adds the plan after the survey.
PLAN_WAIT_DAYS = 180

FED_CODES = {
    "06*": "Psychiatric residential treatment facility",
    "89*": "Behavioral health centre",
}
CATEGORY_LABELS = {
    "prtf": "Psychiatric residential treatment facility",
    "residential": "Behavioral health centre (youth provider)",
    "community": "Behavioral health centre (youth community services agency)",
}

# The columns the state's own page asks for (facsearch-2.0.js).
LIST_COLUMNS = [
    "DT_RowId", "Name", "LegalName", "Admin", "StateKey", "Status", "OpenedDate", "ClosedDate",
    "ContactInformation", "Street", "Street2", "City", "State", "ZIP", "County",
    "PhoneNumberExport", "FAXNumberExport", "FacType", "SubType", "Abbreviation", "LicenseType",
    "Number", "EffectiveDate", "ExpiresDate", "LicensedBedCount", "CertifiedBedCount",
    "CurrentSSIBedCount", "Medicare", "Medicaid", "SSI", "Licensure", "Longitude", "Latitude",
    "Funding", "TotalBeds", "Code",
]

# Names that look like a youth provider; used only to warn about records the
# scope file has not decided on.
YOUTH_PATTERN = re.compile(
    r"youth|child|adolesc|\bboys\b|\bgirls?\b|academy|school|juvenile|teen|ranch|pressley|"
    r"davis.stuart|burlington united|bumfs|stepping stone|crittenton|cammack|board of child|kvc|"
    r"elkins mountain|st\. john|golden girl|home base|new river|potomac center|genesis youth|"
    r"try.again|daymark|braley|family connection|necco|village network|jacob.s ladder",
    re.I,
)


# ── Fetch layer ──────────────────────────────────────────────────────────────


class SiteDown(Exception):
    pass


class WVClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call = 0.0
        self.requests_made = 0

    def _pause(self) -> None:
        wait = REQUEST_GAP - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, method: str, url: str, retry_5xx: bool = True, **kwargs) -> requests.Response:
        """One request at a time, with retries on timeouts and connection errors
        (and on 5xx unless the caller reads 500 as an answer)."""
        delay = 5.0
        for attempt in range(1, 5):
            self._pause()
            self.requests_made += 1
            try:
                response = self.session.request(method, url, timeout=180, **kwargs)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                if attempt == 4:
                    raise
                logger.warning(f"  {exc.__class__.__name__}; retrying in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue
            if response.status_code >= 500 and retry_5xx and attempt < 4:
                logger.warning(f"  HTTP {response.status_code}; retrying in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue
            return response
        raise RuntimeError("unreachable")

    def facility_list(self, fed_code: str) -> List[Dict]:
        """Every record of one facility type, all statuses."""
        body = {
            "draw": 1,
            "columns": [
                {"data": column, "name": "", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}}
                for column in LIST_COLUMNS
            ],
            "order": [{"column": 1, "dir": "asc"}],
            "start": 0,
            "length": -1,
            "search": {"value": "", "regex": False},
            "FedCode": fed_code,
            "NameFilter": "",
            "CountyFilter": "",
            "AdvCountyFilter": "",
            "LegalNameFilter": "",
            "StatusFilter": "active,closed,pending",
            "ContactFilter": "",
            "WithinDistance": "",
            "ZIPFilter": "",
            "PaymentFilter": "",
            "BedsFilter": ",",
            "LicenseFilter": "",
            "ApprovedAMAPs": 0,
        }
        response = self._request(
            "POST", SEARCH_URL, data=json.dumps(body),
            headers={"Content-Type": "application/json; charset=utf-8",
                     "X-Requested-With": "XMLHttpRequest"},
        )
        response.raise_for_status()
        data = response.json()
        if "data" not in data:
            raise RuntimeError(f"Facility list for {fed_code} had no data: {str(data)[:200]}")
        save_raw(f"list-{fed_code.strip('*')}.json", response.text)
        return data["data"]

    def survey_history(self, facility_id: int) -> Tuple[int, List[Dict]]:
        """(HTTP status, surveys). The page answers 500 for a facility with no
        surveys, so 500 is not retried; the caller stops if every one is 500."""
        response = self._request("GET", HISTORY_URL.format(id=facility_id), retry_5xx=False)
        if response.status_code == 500:
            return 500, []
        response.raise_for_status()
        return response.status_code, parse_history(response.text)

    def pdf(self, survey: Dict) -> Optional[bytes]:
        params = {"survID": survey["survid"], "stype": survey["form"], "saveAsName": survey["saveas"]}
        problem = ""
        for attempt in range(3):
            if attempt:
                time.sleep(5 * attempt)
            try:
                response = self._request("GET", PDF_URL, params=params)
            except requests.RequestException as exc:
                problem = f"download failed: {exc}"
                continue
            if response.status_code != 200:
                problem = f"HTTP {response.status_code}"
                continue
            if not response.content.startswith(b"%PDF"):
                problem = f"not a PDF ({response.content[:20]!r})"
                continue
            return response.content
        logger.warning(f"  {survey['saveas']}: {problem}; skipped for this run")
        return None


def raw_dir() -> Path:
    path = EXTRACT_CACHE_ROOT / "wv_raw"
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_raw(name: str, text: str) -> None:
    try:
        (raw_dir() / name).write_text(text, encoding="utf-8")
    except OSError as exc:
        logger.warning(f"Could not save {name}: {exc}")


MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}


def parse_history(html: str) -> List[Dict]:
    """The surveys on a SurveyHistory page: one entry per report button."""
    soup = BeautifulSoup(html, "html.parser")
    surveys: List[Dict] = []
    for body in soup.select("div.panel-body"):
        year = (body.get("id") or "").strip()
        if not re.fullmatch(r"(19|20)\d{2}", year):
            continue
        for row in body.select("div.clearfix"):
            left = row.select_one(".pull-left")
            if not left:
                continue
            kinds = [s.get_text(strip=True) for s in left.select(".sr-only")]
            for span in left.select(".sr-only"):
                span.extract()
            label = re.sub(r"\s+", " ", left.get_text(" ", strip=True)).strip()
            match = re.match(r"([A-Za-z]+)\s+(\d{1,2})\s*-\s*(.*)$", label)
            if not match or match.group(1).lower() not in MONTHS:
                logger.warning(f"  survey row not understood: {label!r}")
                continue
            try:
                date = datetime(int(year), MONTHS[match.group(1).lower()], int(match.group(2)))
            except ValueError:
                continue
            for button in row.select("button.launch"):
                survid = (button.get("data-survid") or "").strip()
                form = (button.get("data-survtype") or "").strip()
                saveas = (button.get("data-saveas") or "").strip()
                if not survid or form not in ("State", "Federal") or not saveas:
                    logger.warning(f"  report button not understood: {button.attrs}")
                    continue
                surveys.append({
                    "survid": survid,
                    "form": form,
                    "event_id": (button.get("data-eventid") or "").strip(),
                    "saveas": saveas,
                    "date": date.strftime("%Y-%m-%d"),
                    "survey_type": match.group(3).strip() or "Survey",
                    "survey_kind": kinds[0] if kinds else "",
                })
    return surveys


# ── PDF extraction ───────────────────────────────────────────────────────────

# The real report is drawn in these two standard (not embedded) fonts. Every
# other font in the file is an embedded Arial subset: the form's printed
# labels and the template's stale deficiency text.
REAL_FONTS = {"Helvetica", "Helvetica-Bold"}

# Column edges of the 2567 form, in PDF points (the form's ruled lines).
X_SUMMARY, X_RIGHT_TAG, X_PLAN, X_DATE = 64.0, 291.0, 341.0, 536.0
BODY_TOP, BODY_BOTTOM = 184.0, 656.0

TAG_RE = re.compile(r"^\{?\s*[A-Z]{1,3}\s?\d{3,4}\s*\}?$")


def is_real_char(obj: Dict) -> bool:
    return obj.get("object_type") != "char" or obj.get("fontname") in REAL_FONTS


def is_template_char(obj: Dict) -> bool:
    return obj.get("object_type") != "char" or obj.get("fontname") not in REAL_FONTS


def tokens(text: str) -> Set[str]:
    return {t for t in re.findall(r"[a-z0-9]{3,}", (text or "").lower())}


def sampled_for_ocr(name: str, share: float) -> bool:
    if share >= 1:
        return True
    if share <= 0:
        return False
    bucket = int(hashlib.sha1(name.encode("utf-8")).hexdigest()[:8], 16) / 0xFFFFFFFF
    return bucket < share


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


def ocr_pdf(path: Path) -> str:
    """What a reader sees: the rendered pages through Tesseract, one at a time."""
    try:
        import pytesseract
        from pdf2image import convert_from_path, pdfinfo_from_path
    except ImportError:
        logger.warning("  pytesseract/pdf2image are not installed; no OCR comparison")
        return ""
    tesseract = find_tesseract()
    if tesseract:
        pytesseract.pytesseract.tesseract_cmd = tesseract
    poppler = find_poppler() or None
    try:
        count = int(pdfinfo_from_path(str(path), poppler_path=poppler).get("Pages", 0))
        pages = []
        for number in range(1, count + 1):
            images = convert_from_path(str(path), dpi=200, first_page=number, last_page=number,
                                       poppler_path=poppler)
            pages.extend(pytesseract.image_to_string(image) for image in images)
            del images
        return "\n".join(pages).strip()
    except Exception as exc:  # poppler or tesseract missing or failing
        logger.warning(f"  OCR failed for {path.name}: {exc}")
        return ""


def extract_pdf(path: Path, ocr: bool = False) -> Dict:
    """The real-font words of every page with their positions, plus what is
    needed to check the font filter: the fonts seen, the template layer's
    words, and (when asked) the OCR text of the rendered pages."""
    pages: List[Dict] = []
    fonts: Counter = Counter()
    template_body: Set[str] = set()
    template_other: Set[str] = set()
    template_tags: Set[str] = set()
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            for char in page.chars:
                fonts[char.get("fontname") or "?"] += 1
            # The body box: the ruled vertical line between the tag and summary columns.
            verticals = [l for l in page.lines
                         if abs(l["x0"] - l["x1"]) < 1 and 62 <= l["x0"] <= 66 and l["bottom"] - l["top"] > 200]
            body_top = min((l["top"] for l in verticals), default=BODY_TOP)
            body_bottom = max((l["bottom"] for l in verticals), default=BODY_BOTTOM)
            words = []
            for w in page.filter(is_real_char).extract_words(extra_attrs=["fontname"]):
                words.append([round(w["x0"], 1), round(w["top"], 1), round(w["x1"], 1), w["text"],
                              1 if "Bold" in (w.get("fontname") or "") else 0])
            for w in page.filter(is_template_char).extract_words():
                in_body = body_top < w["top"] < body_bottom - 4
                (template_body if in_body else template_other).update(tokens(w["text"]))
            left = [w for w in page.filter(is_template_char).extract_words()
                    if w["x0"] < X_SUMMARY and body_top < w["top"] < body_bottom]
            left.sort(key=lambda w: (round(w["top"]), w["x0"]))
            for a, b in zip(left, left[1:]):
                if abs(a["top"] - b["top"]) < 3 and TAG_RE.match(f"{a['text']} {b['text']}"):
                    template_tags.add(f"{a['text']} {b['text']}")
            pages.append({"words": words, "body_top": round(body_top, 1), "body_bottom": round(body_bottom, 1),
                          "width": float(page.width), "height": float(page.height)})
    text = " ".join(w[3] for p in pages for w in p["words"])
    result = {
        "text": text or "[no text in the report layer]",
        "pages": pages,
        "fonts": dict(fonts),
        "template_body": sorted(template_body),
        "template_other": sorted(template_other),
        "template_tags": sorted(template_tags),
        "form_ok": "deficiencies" in template_other and "statement" in template_other,
    }
    if ocr:
        result["ocr_text"] = ocr_pdf(path)
    return result


def ocr_comparison(extracted: Dict) -> Optional[Dict]:
    """How the font-filtered text compares with OCR of the rendered pages."""
    ocr_text = extracted.get("ocr_text")
    if not ocr_text:
        return None
    real = tokens(" ".join(w[3] for p in extracted["pages"] for w in p["words"]))
    seen = tokens(ocr_text)
    stale = set(extracted.get("template_body") or []) - real - set(extracted.get("template_other") or [])
    labels = set(extracted.get("template_other") or [])
    extra = seen - real - labels - set(extracted.get("template_body") or [])
    return {
        "real_tokens": len(real),
        "recall": (len(real & seen) / len(real)) if real else 1.0,
        "stale_tokens": len(stale),
        "stale_seen": (len(stale & seen) / len(stale)) if stale else 0.0,
        "extra": (len(extra) / len(seen)) if seen else 0.0,
    }


# ── Parsing ──────────────────────────────────────────────────────────────────


def one_line(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\u00a0", " ").replace("\ufffd", " ")).strip()


def column_lines(words: List[List], lo: float, hi: float) -> List[Tuple[float, str]]:
    """(top, text) of the lines made by the words whose left edge is in [lo, hi)."""
    picked = sorted((w for w in words if lo <= w[0] < hi), key=lambda w: (w[1], w[0]))
    lines: List[Tuple[float, List[List]]] = []
    for word in picked:
        if lines and abs(word[1] - lines[-1][0]) <= 3.5:
            lines[-1][1].append(word)
        else:
            lines.append((word[1], [word]))
    return [(top, " ".join(w[3] for w in sorted(ws, key=lambda w: w[0]))) for top, ws in lines]


def normal_tag(value: str) -> str:
    """'{N 142}' / 'N142' -> 'N 142'."""
    match = re.match(r"\{?\s*([A-Z]{1,3})\s?(\d{3,4})\s*\}?$", value.strip())
    return f"{match.group(1)} {match.group(2)}" if match else value.strip()


def paragraphs(lines: List[Tuple[int, float, str]]) -> List[str]:
    """Join (page, top, text) lines into paragraphs: a gap of more than a line
    and a half, or a page turn after a finished sentence, starts a new one."""
    out: List[str] = []
    previous: Optional[Tuple[int, float, str]] = None
    for line in lines:
        page, top, text = line
        new = previous is None
        if previous is not None:
            if page == previous[0]:
                new = top - previous[1] > 15.5
            else:
                new = bool(re.search(r"[.:;!?\"\u201d)]$", previous[2]))
        if new:
            out.append(text)
        else:
            out[-1] += " " + text
        previous = line
    return [one_line(p) for p in out if one_line(p)]


CONTINUED = re.compile(r"^Continued\s+From\s+page\s+\d+\s*", re.I)
NOT_MET = re.compile(r"\bis\s+not\s+met\s+as\s+evidenced\s+by\s*:?", re.I)
DATE_TOKEN = re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b")
NO_DEFICIENCIES = re.compile(
    r"no\s+(?:\w+\s+){0,3}deficienc(?:y|ies)\s+(?:were|was|are|is)?\s*(?:cited|found|identified|noted|written)"
    r"|deficiency[- ]free|in\s+(?:full\s+|substantial\s+)?compliance\s+with",
    re.I,
)


def parse_pages(pages: List[Dict]) -> Dict:
    """Header fields and the tags of one 2567 from its positioned words."""
    header: Dict[str, str] = {}
    tags: List[Dict] = []
    preamble: List[Tuple[int, float, str]] = []
    current: Optional[Dict] = None

    for number, page in enumerate(pages):
        words = page["words"]
        top_edge, bottom_edge = page.get("body_top", BODY_TOP), page.get("body_bottom", BODY_BOTTOM)
        if number == 0:
            bold = [w for w in words if w[4] and w[1] < top_edge]
            dates = [w[3] for w in bold if w[0] > 480 and DATE_TOKEN.fullmatch(w[3])]
            header["survey_completed"] = dates[0] if dates else ""
            header["provider_id"] = " ".join(w[3] for w in bold if 150 <= w[0] < 290 and w[1] < 100)
            name_lines = column_lines(bold, 0, 240)
            header["provider_name"] = one_line(" ".join(t for top, t in name_lines if top > 100))
            address = column_lines([w for w in bold if w[1] > 100], 240, 600)
            header["provider_address"] = one_line(", ".join(t for _, t in address))
        footer = [w for w in words if w[1] >= bottom_edge]
        for w in footer:
            if re.fullmatch(r"WV[0-9A-Z]{4,}", w[3]) and not header.get("facility_key"):
                header["facility_key"] = w[3]

        body = [w for w in words if top_edge - 1 <= w[1] < bottom_edge - 2]
        # A tag starts where its id stands in the left tag column or the right
        # one: at the foot of a page the form prints only the right one.
        starts: List[Tuple[float, str, bool]] = []
        for lo, hi in ((0, X_SUMMARY), (X_RIGHT_TAG, X_PLAN)):
            for top, text in column_lines(body, lo, hi):
                if TAG_RE.match(text.strip()) and not any(abs(top - s[0]) <= 6 for s in starts):
                    starts.append((top, normal_tag(text), text.strip().startswith("{")))
        starts.sort()
        other_left = [(top, text) for top, text in column_lines(body, 0, X_SUMMARY)
                      if not TAG_RE.match(text.strip())]
        summary = column_lines(body, X_SUMMARY, X_RIGHT_TAG)
        plan = column_lines(body, X_PLAN, X_DATE)
        dates = column_lines(body, X_DATE, 10000)

        def owner(top: float) -> Optional[Dict]:
            found = None
            for start in page_tags:
                if start["top"] - 6 <= top:
                    found = start["tag"]
            return found if found is not None else carried

        carried = current
        page_tags: List[Dict] = []
        for top, tag_id, braces in starts:
            first = next((text for line_top, text in summary if abs(line_top - top) <= 5), "")
            if carried is not None and not page_tags and tag_id == carried["tag"]:
                # The first tag of a page repeating the last one of the page
                # before: the same tag carried over.
                page_tags.append({"top": top, "tag": carried})
                continue
            if page_tags and page_tags[-1]["tag"]["tag"] == tag_id and CONTINUED.match(first):
                page_tags.append({"top": top, "tag": page_tags[-1]["tag"]})
                continue
            tag = {"tag": tag_id, "braces": braces, "scope": "", "summary": [], "plan": [], "dates": [],
                   "title_top": (number, top)}
            tags.append(tag)
            page_tags.append({"top": top, "tag": tag})
        for top, text in other_left:
            target = owner(top)
            if target is not None and re.match(r"SS\s*=", text):
                target["scope"] = one_line(text)
        for top, text in summary:
            target = owner(top)
            text = CONTINUED.sub("", text).strip()
            if not text:
                continue
            if target is None:
                preamble.append((number, top, text))
            else:
                target["summary"].append((number, top, text))
        for top, text in plan:
            target = owner(top)
            if target is not None:
                target["plan"].append((number, top, text))
        for top, text in dates:
            target = owner(top)
            if target is not None:
                target["dates"].extend(DATE_TOKEN.findall(text))
        if page_tags:
            current = page_tags[-1]["tag"]

    parsed: List[Dict] = []
    for tag in tags:
        lines = tag["summary"]
        title_page, title_top = tag["title_top"]
        # The title: the lines from the tag's own line to the first paragraph gap.
        title_lines: List[str] = []
        rest = list(lines)
        previous_top: Optional[float] = None
        while rest and rest[0][0] == title_page:
            page_no, top, text = rest[0]
            if previous_top is None:
                if abs(top - title_top) > 5:
                    break
            elif top - previous_top > 15.5:
                break
            title_lines.append(text)
            previous_top = top
            rest.pop(0)
        title = one_line(" ".join(title_lines))
        paras = paragraphs(rest)
        regulation: List[str] = []
        finding: List[str] = list(paras)
        joined = "\n".join(paras)
        marker = NOT_MET.search(joined)
        if marker:
            regulation = [p for p in joined[:marker.end()].split("\n") if p.strip()]
            finding = [p for p in joined[marker.end():].split("\n") if p.strip()]
        else:
            for index, para in enumerate(paras):
                if re.match(r"\(?[\dA-Za-z]{0,2}\)?\.?\s*Based\s+on\b", para) and index > 0:
                    regulation, finding = paras[:index], paras[index:]
                    break
        plan_text = "\n".join(paragraphs(tag["plan"]))
        listed_only = False
        if not marker and not regulation and not plan_text and tag["dates"]:
            # A revisit lists each earlier tag with its rule and the date it
            # was corrected: no finding, no plan, a date in the last column.
            regulation, finding, listed_only = paras, [], True
        number_part = tag["tag"].split(" ")[-1]
        initial = bool(re.fullmatch(r"0+", number_part)) or title.lower().startswith("initial comments")
        parsed.append({
            "tag": tag["tag"],
            "title": title,
            "scope": tag["scope"],
            "initial": initial,
            "corrected": bool(tag["braces"]) or listed_only,
            "split": "marker" if marker else ("based_on" if regulation and not listed_only else ""),
            "regulation": "\n".join(regulation),
            "finding": "\n".join(finding),
            "plan": plan_text,
            "completion_date": tag["dates"][0] if tag["dates"] else "",
        })

    # The state's generator prints one plan of correction under every tag of a
    # survey; keep it on the deficiency and drop the copy under the comments.
    for tag in parsed:
        if tag["initial"] and tag["plan"] and any(
                other is not tag and other["plan"] == tag["plan"] for other in parsed):
            tag["plan"] = ""
            tag["completion_date"] = ""

    return {"header": header, "tags": parsed, "preamble": "\n".join(paragraphs(preamble))}


def iso_from_us(value: str) -> str:
    match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{2,4})$", value or "")
    if not match:
        return ""
    year = int(match.group(3))
    if year < 100:
        year += 2000 if year < 50 else 1900
    try:
        return datetime(year, int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def report_text(survey: Dict, parsed: Dict) -> str:
    """The report as a reader would read it: comments, then each tag."""
    header = parsed["header"]
    out = [
        f"{survey['form']} 2567: statement of deficiencies and plan of correction",
        f"Provider: {header.get('provider_name', '')}",
        f"Address: {header.get('provider_address', '')}",
        f"Survey: {survey['survey_type']}, completed {header.get('survey_completed') or survey['date']}",
    ]
    if parsed["preamble"]:
        out += ["", parsed["preamble"]]
    for tag in parsed["tags"]:
        head = f"{tag['tag']}  {tag['title']}".strip()
        if tag["corrected"]:
            head += "  (previously cited, shown as corrected)"
        if tag["scope"]:
            head += f"  [{tag['scope']}]"
        out += ["", head]
        if tag["regulation"]:
            out.append(tag["regulation"])
        if tag["finding"]:
            out.append(tag["finding"])
        if tag["plan"]:
            date = f" (completion date {tag['completion_date']})" if tag["completion_date"] else ""
            out += ["", f"Provider's plan of correction{date}:", tag["plan"]]
        elif tag["completion_date"]:
            out.append(f"Completion date: {tag['completion_date']}")
    return "\n".join(out).strip()


# Things that must not be public. A hit holds the report back for the owner.
PRIVACY_CHECKS = [
    ("date of birth", re.compile(r"\b(?:D\.?O\.?B\.?|date\s+of\s+birth|born\s+on)\b[^.\n]{0,20}\d{1,2}/\d{1,2}/\d{2,4}", re.I)),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("record number", re.compile(
        r"\b(?:medical\s+record|MRN|MR|chart|case|medicaid|client\s+id|consumer\s+id)\s*(?:number|no\.?|#)\s*:?\s*[A-Z]?\d{4,}", re.I)),
    ("named person", re.compile(
        r"\b(?:[Rr]esident|[Cc]lient|[Cc]onsumer|[Pp]atient|[Yy]outh|[Cc]hild|[Ss]tudent|[Mm]inor)\s+"
        r"(?:named\s+)?([A-Z][a-z]{2,})\s+([A-Z][a-z]{2,})\b")),
]
# Capitalised words that follow "Resident"/"Client" in the forms and are not names.
NOT_NAMES = {
    "Rights", "Record", "Records", "Care", "Council", "Advocate", "Services", "Service", "Treatment",
    "Plan", "Plans", "Assessment", "Assessments", "Interview", "Interviews", "Review", "Reviews",
    "File", "Files", "Chart", "Charts", "Sample", "Census", "Handbook", "Grievance", "Funds",
    "Behavior", "Behaviour", "Safety", "Protection", "Abuse", "Neglect", "Health", "Welfare",
    "Development", "Placement", "Protective", "Advocacy", "Center", "Centre", "Home", "Homes",
    "Number", "Program", "Programs", "Academy", "Residential", "Based", "During", "Stated",
    "Was", "Had", "Has", "Did", "Does", "Will", "The", "And", "Who", "With", "Education",
    "Information", "Involvement", "Participation", "Supervision", "Staff", "Ratio", "Status",
    "Medication", "Medications", "Individual", "Service", "Support", "Supports", "Family",
    "Specific", "Training", "Orientation", "Satisfaction", "Survey", "Surveys", "Incident",
    "Incidents", "Observation", "Observations", "Discharge", "Admission", "Intake", "Master",
    "Crisis", "Emergency", "Property", "Personal", "Initial", "Annual", "Comprehensive",
}


def privacy_hits(text: str) -> List[str]:
    hits: List[str] = []
    for label, pattern in PRIVACY_CHECKS:
        for match in pattern.finditer(text):
            if label == "named person":
                if match.group(1) in NOT_NAMES or match.group(2) in NOT_NAMES:
                    continue
            hits.append(f"{label}: {one_line(match.group(0))[:80]}")
            break
    return hits


# ── Names ────────────────────────────────────────────────────────────────────

KEEP_UPPER = {"LLC", "INC", "LLP", "PLLC", "PC", "USA", "II", "III", "IV", "VI", "VII", "VIII",
              "IX", "KVC", "WV", "WVU", "PRTF", "NYAP", "IOP", "RESA", "DBA", "YMCA"}
SMALL_WORDS = {"of", "and", "the", "for", "at", "in", "on", "a", "an", "to", "by"}


def display_name(name: str) -> str:
    """Title-case names written in full capitals; leave mixed-case names alone."""
    name = one_line(name)
    letters = [c for c in name if c.isalpha()]
    if not letters or any(c.islower() for c in letters):
        return name

    def fix(word: str, first: bool) -> str:
        bare = re.sub(r"[^A-Za-z.]", "", word)
        if bare.rstrip(".") in KEEP_UPPER or re.fullmatch(r"(?:[A-Z]\.)+[A-Z]?\.?", bare):
            return word
        lowered = word.lower()
        if not first and lowered in SMALL_WORDS:
            return lowered
        cased = re.sub(r"(^|[-/(\"])([a-z])", lambda m: m.group(1) + m.group(2).upper(), lowered)
        return re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), cased)

    words = name.split(" ")
    return " ".join(fix(word, i == 0) for i, word in enumerate(words))


def format_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return ""


# ── Scope ────────────────────────────────────────────────────────────────────


def load_scope(path: Path = SCOPE_FILE) -> Dict[int, Dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    records = data.get("records") if isinstance(data, dict) else None
    if not isinstance(records, list):
        raise ValueError(f"{path} must contain a records list")

    scope: Dict[int, Dict] = {}
    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"{path} record {index} must be an object")
        try:
            facility_id = int(record["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{path} record {index} has an invalid id") from exc
        if facility_id <= 0 or facility_id in scope:
            raise ValueError(f"{path} record {index} has a non-positive or duplicate id: {facility_id}")
        if not one_line(str(record.get("name") or "")):
            raise ValueError(f"{path} record {index} has no name")
        if record.get("decision") not in {"included", "excluded", "unsure"}:
            raise ValueError(f"{path} record {index} has an invalid decision")
        scope[facility_id] = record
    if not any(record["decision"] == "included" for record in scope.values()):
        raise ValueError(f"{path} has no included facilities")
    return scope


# ── Scraper ──────────────────────────────────────────────────────────────────


class WVScraper:
    def __init__(self, client: Optional[WVClient] = None, reports: Optional[ReportStore] = None,
                 ocr_share: float = 0.05):
        self.client = client or WVClient()
        self.reports = reports or REPORTS
        self.ocr_share = ocr_share
        self.stats: Counter = Counter()
        self.by_type: Counter = Counter()
        self.by_form: Counter = Counter()
        self.tag_counts: Counter = Counter()
        self.held: List[str] = []
        self.not_reports: List[str] = []
        self.unparsed: List[str] = []
        self.stale: List[str] = []
        self.ocr_failures: List[Tuple[str, Dict]] = []
        self.ocr_unavailable: List[str] = []
        self.odd_fonts: Counter = Counter()
        self.ocr_checks: List[Tuple[str, Dict]] = []
        self.date_mismatches: List[str] = []
        self.download_failures = 0

    # -- survey lists --

    def history(self, facility_id: int) -> Tuple[int, List[Dict]]:
        cache = raw_dir() / f"history-{facility_id}.json"
        try:
            stored = json.loads(cache.read_text(encoding="utf-8"))
            age = datetime.now() - datetime.fromisoformat(stored["fetched"])
            if age < timedelta(hours=HISTORY_TTL_HOURS):
                return stored["status"], stored["surveys"]
        except (OSError, ValueError, KeyError):
            pass
        status, surveys = self.client.survey_history(facility_id)
        cache.write_text(json.dumps({"fetched": datetime.now().isoformat(timespec="seconds"),
                                     "status": status, "surveys": surveys}), encoding="utf-8")
        return status, surveys

    # -- one report --

    def fetch_report(self, row: Dict, survey: Dict, refresh: bool = False) -> Optional[Dict]:
        archive_name = f"{survey['saveas']}.pdf"
        want_ocr = sampled_for_ocr(archive_name, self.ocr_share)
        if refresh:
            try:
                self.reports._extract_path(archive_name).unlink()
            except OSError:
                pass

        def fetch() -> Optional[bytes]:
            data = self.client.pdf(survey)
            if data:
                self.download_failures = 0
                self.stats["downloaded"] += 1
            else:
                self.download_failures += 1
                if self.download_failures >= MAX_DOWNLOAD_FAILURES:
                    raise SiteDown(f"{MAX_DOWNLOAD_FAILURES} report downloads failed in a row; stopping. "
                                   "Rerun to pick up where this left off.")
            return data

        extracted = extract_with_cache(self.reports, archive_name, fetch=fetch,
                                       extract=lambda path: extract_pdf(path, ocr=want_ocr))
        if not extracted or not extracted.get("pages"):
            self.stats["no_document"] += 1
            return None
        if want_ocr and not extracted.get("ocr_text"):
            self.ocr_unavailable.append(archive_name)
            logger.warning(f"  {archive_name} held back: sampled OCR was unavailable")
            try:
                (self.reports.archive_dir / archive_name).unlink()
            except FileNotFoundError:
                pass
            return None
        if not extracted.get("form_ok"):
            # Not a 2567: never posted, and the archived copy is removed so the
            # nightly sync does not put it on the site.
            self.not_reports.append(archive_name)
            logger.warning(f"  {archive_name} is not a statement of deficiencies; not posted")
            try:
                (self.reports.archive_dir / archive_name).unlink()
            except OSError:
                pass
            return None
        return self.build_report(row, survey, extracted, archive_name)

    def build_report(self, row: Dict, survey: Dict, extracted: Dict, archive_name: str) -> Optional[Dict]:
        parsed = parse_pages(extracted["pages"])
        tags = parsed["tags"]
        text = report_text(survey, parsed)

        for font, count in (extracted.get("fonts") or {}).items():
            if font not in REAL_FONTS and "Arial" not in font:
                self.odd_fonts[font] += 1
        comparison = ocr_comparison(extracted)
        if comparison:
            self.ocr_checks.append((archive_name, comparison))
            if comparison["recall"] < 0.9:
                self.ocr_failures.append((archive_name, comparison))
                logger.warning(f"  {archive_name} held back: OCR word recall below 90%")
                try:
                    (self.reports.archive_dir / archive_name).unlink()
                except FileNotFoundError:
                    pass
                return None
        ocr_text = re.sub(r"[^A-Z0-9]", "", (extracted.get("ocr_text") or "").upper())
        stale = [
            t["tag"] for t in tags
            if t["tag"] in ("C 173", "F 156")
            and t["tag"].replace(" ", "") not in ocr_text
        ]
        if stale:
            self.stale.append(f"{archive_name} ({survey['date']}): {', '.join(stale)}")
            logger.warning(f"  {archive_name} held back: template tag not visible in OCR")
            try:
                (self.reports.archive_dir / archive_name).unlink()
            except FileNotFoundError:
                pass
            return None

        hits = privacy_hits(text)
        if hits:
            self.held.append(f"{archive_name} ({display_name(row['Name'])}, {survey['date']}): {'; '.join(hits)}")
            logger.warning(f"  {archive_name} held back: {'; '.join(hits)}")
            # Held reports stay off the site as well: no archived copy for the sync.
            try:
                (self.reports.archive_dir / archive_name).unlink()
            except OSError:
                pass
            return None

        cited = [t for t in tags if not t["initial"] and not t["corrected"]]
        corrected = [t for t in tags if t["corrected"] and not t["initial"]]
        comments = "\n".join(t["finding"] or t["regulation"] for t in tags if t["initial"]).strip()
        if not comments:
            comments = parsed["preamble"]
        body_words = sum(1 for p in extracted["pages"] for w in p["words"]
                         if p.get("body_top", BODY_TOP) <= w[1] < p.get("body_bottom", BODY_BOTTOM))
        if not tags and body_words > 5:
            self.unparsed.append(f"{archive_name} ({survey['date']}): {body_words} words, no tag found")

        completed = iso_from_us(parsed["header"].get("survey_completed", ""))
        if completed and completed != survey["date"]:
            self.date_mismatches.append(f"{archive_name}: list {survey['date']}, form {completed}")

        is_complaint = "complaint" in survey["survey_type"].lower()
        flagged = bool(cited)
        if cited:
            outcome = f"{len(cited)} deficienc{'y' if len(cited) == 1 else 'ies'} cited"
        elif corrected:
            outcome = f"{len(corrected)} earlier deficienc{'y' if len(corrected) == 1 else 'ies'} shown as corrected"
        elif NO_DEFICIENCIES.search(comments) or tags:
            outcome = "no deficiencies cited"
        else:
            outcome = "no findings on the form"

        short_tags = [{
            "tag": t["tag"],
            "regulation": t["title"],
            "scope": t["scope"],
            "finding": t["finding"][:300],
            "has_plan": bool(t["plan"]),
            "completion_date": t["completion_date"],
            "corrected": t["corrected"],
        } for t in tags if not t["initial"]]
        detail_tags = [{
            "tag": t["tag"],
            "regulation": "\n".join(x for x in (t["title"], t["regulation"]) if x),
            "finding": t["finding"],
            "plan": t["plan"],
            "completion_date": t["completion_date"],
        } for t in tags if not t["initial"]]

        categories: Dict[str, Any] = {
            "survey_type": survey["survey_type"],
            "form": survey["form"],
            "is_complaint": is_complaint,
            "event_id": survey["event_id"],
            "legal_name": one_line(row.get("LegalName") or ""),
            "tags": short_tags,
            "tag_count": len(cited),
            "corrected_count": len(corrected),
            "outcome": "cited" if cited else ("corrected" if corrected else "clean"),
            "comments": comments[:600],
            "archive_name": archive_name,
            "detail": {"comments": comments, "tags": detail_tags},
        }

        self.by_type[survey["survey_type"]] += 1
        self.by_form[survey["form"]] += 1
        self.tag_counts[len(cited)] += 1

        return {
            "report_id": f"{survey['survid']}-{survey['form']}",
            "report_date": survey["date"],
            "report_url": DETAILS_URL.format(id=row["DT_RowId"]),
            "raw_content": text,
            "content_length": len(text),
            "summary": f"{survey['survey_type']}: {outcome}",
            "categories": categories,
            "is_flagged": flagged,
            "awaiting_plan": bool(cited) and not any(t["plan"] for t in cited),
        }

    @staticmethod
    def facility_info(row: Dict, scope: Dict) -> Dict:
        street = one_line(" ".join(x for x in (row.get("Street"), row.get("Street2")) if x))
        city = display_name((row.get("City") or "").upper())
        address = ", ".join(x for x in (display_name(street.upper()), city) if x)
        if address:
            address += f", WV {one_line(row.get('ZIP') or '')}".rstrip()
        try:
            beds = int(row.get("LicensedBedCount") or 0)
        except (TypeError, ValueError):
            beds = 0
        return {
            "facility_name": display_name((row.get("Name") or "").upper()),
            "program_name": f"WV-{row['DT_RowId']}",
            "program_category": CATEGORY_LABELS.get(scope.get("category", ""), CATEGORY_LABELS["residential"]),
            "full_address": address,
            "phone": format_phone(row.get("Phone") or row.get("PhoneNumberExport") or ""),
            "bed_capacity": str(beds) if beds > 0 else "",
            "executive_director": "",
            "license_exp_date": row.get("ExpiresDate") or "",
            "relicense_visit_date": "",
            "action": one_line(row.get("Status") or ""),
        }

    def scrape(self, seen: Dict[str, Set[str]], awaiting: Dict[str, List[str]], limit: int = 0,
               only: Optional[Set[str]] = None, refresh_all: bool = False
               ) -> Tuple[List[Dict], Dict[str, List[str]], Dict[str, List[str]]]:
        scope = load_scope()
        included = {fid: record for fid, record in scope.items() if record.get("decision") == "included"}
        rows: Dict[int, Dict] = {}
        for fed_code in FED_CODES:
            listed = self.client.facility_list(fed_code)
            logger.info(f"{len(listed)} records of type {fed_code} ({FED_CODES[fed_code]})")
            for row in listed:
                rows[int(row["DT_RowId"])] = row
        undecided = [r for fid, r in rows.items() if fid not in scope
                     and (str(r.get("Code") or "").startswith("06")
                          or YOUTH_PATTERN.search(f"{r.get('Name')} {r.get('LegalName')}"))
                     and "RESCARE" not in f"{r.get('Name')} {r.get('LegalName')}".upper()]
        for row in undecided:
            self.stats["undecided"] += 1
            logger.warning(f"Not in {SCOPE_FILE.name}, looks like a youth provider: "
                           f"{row['DT_RowId']} {row.get('Name')} ({row.get('LegalName')}, {row.get('Status')})")
        missing = [fid for fid in included if fid not in rows]
        for fid in missing:
            logger.warning(f"In scope but no longer listed by the state: {fid} {included[fid].get('name')}")

        targets = [rows[fid] for fid in included if fid in rows]
        if only:
            targets = [r for r in targets if str(r["DT_RowId"]) in only]
        targets.sort(key=lambda r: ((r.get("Name") or "").lower(), r["DT_RowId"]))
        if limit:
            targets = targets[:limit]

        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        still_waiting: Dict[str, List[str]] = {}
        answered = errors = 0
        cutoff = (datetime.now() - timedelta(days=PLAN_WAIT_DAYS)).strftime("%Y-%m-%d")
        for index, row in enumerate(targets, start=1):
            fid = int(row["DT_RowId"])
            key = str(fid)
            status, surveys = self.history(fid)
            if status == 500:
                errors += 1
            else:
                answered += 1
            if index >= 15 and answered == 0:
                raise SiteDown("Every survey list so far answered HTTP 500; the site looks down. Nothing posted.")
            logger.info(f"[{index}/{len(targets)}] {row.get('Name')} ({fid}): "
                        + ("no surveys (HTTP 500)" if status == 500 else f"{len(surveys)} surveys"))
            self.stats["surveys_listed"] += len(surveys)
            if not surveys:
                continue
            already = seen.get(key, set())
            waiting = set(awaiting.get(key, []))
            reports: List[Dict] = []
            for survey in surveys:
                report_id = f"{survey['survid']}-{survey['form']}"
                refresh = report_id in waiting
                if report_id in already and not refresh:
                    continue
                report = self.fetch_report(row, survey, refresh=refresh or refresh_all)
                if report:
                    reports.append(report)
            if not reports:
                continue
            reports.sort(key=lambda r: r["report_date"], reverse=True)
            facilities.append({"facility_info": self.facility_info(row, included[fid]), "reports": reports})
            new_ids[key] = [r["report_id"] for r in reports]
            pending = [r["report_id"] for r in reports if r["awaiting_plan"] and r["report_date"] >= cutoff]
            if pending:
                still_waiting[key] = pending
        self.stats["facilities_no_surveys"] = errors
        return facilities, new_ids, still_waiting

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        logger.info("-- West Virginia run summary --")
        logger.info(f"requests made: {self.client.requests_made}, PDFs downloaded: {self.stats.get('downloaded', 0)}")
        logger.info(f"surveys listed for the scope: {self.stats.get('surveys_listed', 0)}; "
                    f"scope records with no surveys: {self.stats.get('facilities_no_surveys', 0)}")
        logger.info(f"facilities with new reports: {len(facilities)}")
        logger.info(f"reports: {len(reports)} (flagged: {sum(1 for r in reports if r['is_flagged'])})")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        logger.info("reports per survey type:")
        for value, count in self.by_type.most_common():
            logger.info(f"  {count:5d}  {value}")
        logger.info(f"reports per form: {dict(self.by_form)}")
        logger.info("deficiency tags per report (tags: reports): "
                    + ", ".join(f"{k}: {v}" for k, v in sorted(self.tag_counts.items())))
        logger.info(f"reports with the template's tag (C 173 / F 156) as a finding: {len(self.stale)}")
        for line in self.stale:
            logger.info(f"  {line}")
        logger.info(f"reports held back by low OCR recall: {len(self.ocr_failures)}")
        for name, comparison in self.ocr_failures:
            logger.warning(f"  {name}: word recall {comparison['recall']:.2f}")
        logger.info(f"reports held back because sampled OCR was unavailable: {len(self.ocr_unavailable)}")
        for name in self.ocr_unavailable:
            logger.warning(f"  {name}")
        logger.info(f"reports with words but no tag (unparsed): {len(self.unparsed)}")
        for line in self.unparsed:
            logger.info(f"  {line}")
        logger.info(f"documents that are not a 2567 (not posted): {len(self.not_reports)}")
        for line in self.not_reports:
            logger.info(f"  {line}")
        logger.info(f"reports held back by the privacy check (not posted): {len(self.held)}")
        for line in self.held:
            logger.info(f"  {line}")
        logger.info(f"documents not available: {self.stats.get('no_document', 0)}")
        if self.odd_fonts:
            logger.warning(f"fonts that are neither the report's nor the template's: {dict(self.odd_fonts)}")
        logger.info(f"survey date differs between the list and the form: {len(self.date_mismatches)}")
        for line in self.date_mismatches[:20]:
            logger.info(f"  {line}")
        if self.ocr_checks:
            bad = [(n, c) for n, c in self.ocr_checks if c["recall"] < 0.9]
            mean = sum(c["recall"] for _, c in self.ocr_checks) / len(self.ocr_checks)
            logger.info(f"OCR comparison: {len(self.ocr_checks)} reports, mean share of the filtered words "
                        f"OCR also read {mean:.3f}, reports below 90% word recall: {len(bad)}")
            for name, c in bad:
                logger.warning(f"  {name}: recall {c['recall']:.2f}")
        if self.stats.get("undecided"):
            logger.warning(f"{self.stats['undecided']} listed records look like youth providers and are "
                           f"not in {SCOPE_FILE.name}; add them as included or excluded")


INTERNAL_KEYS = ("is_flagged", "awaiting_plan")


def strip_internal(facilities: List[Dict]) -> List[Dict]:
    return [{
        "facility_info": f["facility_info"],
        "reports": [{k: v for k, v in r.items() if k not in INTERNAL_KEYS} for r in f["reports"]],
    } for f in facilities]


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = [{
        "facility_info": f["facility_info"],
        "reports": [{**r, "is_structured": True} for r in f["reports"]],
    } for f in strip_internal(facilities)]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "WV",
        "scraped_timestamp": timestamp,
        "scraping_notes": {"total_reports": sum(len(f["reports"]) for f in shaped)},
        "facilities": shaped,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"Wrote {path}")


def save_to_api(facilities: List[Dict], timestamp: str) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="WV",
        scraped_timestamp=timestamp,
        facilities=strip_internal(facilities),
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    global REPORTS
    parser = argparse.ArgumentParser(description="Scrape West Virginia OHFLAC survey reports for youth providers")
    parser.add_argument("--full", action="store_true", help=f"Ignore the seen reports in {STATE_FILE}")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-download and re-extract every report (also ignores seen reports)")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N facilities in scope (by name)")
    parser.add_argument("--facility", action="append", default=[],
                        help="Only this OHFLAC facility id (repeatable)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    parser.add_argument("--ocr-share", type=float, default=float(os.getenv("WV_OCR_SHARE", "0.05")),
                        help="Share of newly downloaded reports also read by OCR and compared (default 0.05)")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be zero (unlimited) or a positive integer")
    if not 0 <= args.ocr_share <= 1:
        parser.error("--ocr-share must be between 0 and 1")
    if any(not value.isdecimal() or int(value) <= 0 for value in args.facility):
        parser.error("--facility values must be positive numeric OHFLAC ids")

    REPORTS = ReportStore("WV_PDF_CACHE", "wv_pdfs", Path(__file__).parent / "wv_pdfs")
    logger.info(f"Report PDFs are archived to {REPORTS.archive_dir}")

    state = load_state(STATE_FILE)
    seen = {} if args.full or args.refresh else seen_from_state(state)
    awaiting = {} if args.full or args.refresh else state.get("awaiting_plan", {})
    timestamp = datetime.now().isoformat(timespec="seconds")

    scraper = WVScraper(reports=REPORTS, ocr_share=args.ocr_share)
    try:
        facilities, new_ids, still_waiting = scraper.scrape(
            seen=seen, awaiting=awaiting, limit=args.limit, only=set(args.facility) or None,
            refresh_all=args.refresh)
    except SiteDown as exc:
        logger.error(str(exc))
        raise SystemExit(1) from exc
    scraper.print_stats(facilities)

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
        waiting = state.setdefault("awaiting_plan", {})
        for key in new_ids:
            waiting.pop(key, None)
        waiting.update(still_waiting)
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- seen reports not advanced")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
