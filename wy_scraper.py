"""
Wyoming youth residential provider findings scraper.

Two sources, one payload (state WY):

  dfs  Department of Family Services, one page of accordions (one per
       certified provider) linking to Google Drive:
       https://dfs.wyo.gov/providers/substitute-care/notice-of-non-compliance-findings-and-facility-visits/
       Every link is one document. All of them are scans with no text layer:
         Notice of Non-Compliance (form SCL-305)  typed; read by OCR into the
             allegation, the finding and the rules violated.
         Facility Visit (form SCL-300)            HANDWRITTEN; never
             transcribed. Posted as a document only (label, date, link).
         anything else (corrective action plan responses, recertifications)
             posted with its label; text by OCR only for typed pages.
       Some providers also link a Drive folder ("All documents"); every folder
       is listed and files the page lacks are taken. The folders also hold
       second copies of page documents under other ids; those are recognised
       (same bytes, or the same kind and date) and not posted twice.

  wdh  Department of Health, Healthcare Licensing and Surveys public search
       (https://ohlssurvey.health.wyo.gov/PublicSearch), a JSON API with no
       login: federal CMS-2567 surveys of psychiatric residential treatment
       facilities (facility type 16), open and closed. The form is a
       two-column table (findings left, plan of correction right); it is read
       by column position so the two never interleave.

Only current providers are on the Family Services page, and a provider's
documents leave with it. So every PDF is archived the moment it is found
(ReportStore -> the FileBird Drive folder `wy_pdfs`), and the state file keeps
every provider and Drive id ever seen: a provider that drops off the page is
still built from the archive and the local extract cache.

Privacy: OCR text is checked before it goes in the payload (a date of birth,
a named child, a record number). A hit holds the whole report back: it is
listed in the run report, left out of the payload, and its archived copy is
moved out of the Drive folder (to .report_extract_cache/wy_held/) so the
nightly archive sync cannot publish it.
"""

import argparse
import difflib
import hashlib
import html as htmllib
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pdfplumber
import requests

from inspection_api_client import post_facilities_to_api
from report_store import EXTRACT_CACHE_ROOT, ReportStore, extract_with_cache
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
DFS_STATE_FILE = Path(os.getenv("WY_DFS_STATE_FILE", ".wy_dfs_state.json"))
WDH_STATE_FILE = Path(os.getenv("WY_WDH_STATE_FILE", ".wy_wdh_state.json"))
REPORTS = ReportStore("WY_PDF_CACHE", "wy_pdfs", Path(__file__).parent / "wy_pdfs")
HELD_DIR = EXTRACT_CACHE_ROOT / "wy_held"

DFS_PAGE = "https://dfs.wyo.gov/providers/substitute-care/notice-of-non-compliance-findings-and-facility-visits/"
DRIVE_DOWNLOAD = "https://drive.google.com/uc?export=download&id={id}"
DRIVE_DOWNLOAD_CONFIRM = "https://drive.usercontent.google.com/download?id={id}&export=download&confirm=t"
DRIVE_VIEW = "https://drive.google.com/file/d/{id}/view"
DRIVE_FOLDER = "https://drive.google.com/embeddedfolderview?id={id}"

WDH_SITE = "https://ohlssurvey.health.wyo.gov"
WDH_FACILITIES = WDH_SITE + "/api/FacilitySearch/Search"
WDH_SURVEYS = WDH_SITE + "/api/FacilitySurveySearch/Search"
WDH_DOWNLOAD = WDH_SITE + "/api/Survey/Download/{id}"
WDH_PRTF_TYPE = 16

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
PAGE_GAP = 1.0     # the state sites
DRIVE_GAP = 2.0    # Google Drive downloads and folder listings

OCR_DPI = int(os.getenv("WY_OCR_DPI", "250"))
# A page whose OCR words average below this confidence is handwriting or a
# bad scan; its text is not kept. Typed notices read at about 90.
TYPED_CONFIDENCE = float(os.getenv("WY_TYPED_CONFIDENCE", "78"))
TYPED_MIN_WORDS = 25
NOT_TRANSCRIBED = "(handwritten form; not transcribed)"
VISIT_SUMMARY = "Facility visit (handwritten form; open the document to read it)"

# Optional: a folder that receives a small picture of each visit form's
# "Reason" row, for checking the checkbox reading by eye.
REASON_CROPS = os.getenv("WY_REASON_CROPS", "").strip()


# ── Fetch layer ──────────────────────────────────────────────────────────────


class Fetcher:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last: Dict[str, float] = {}
        self.counts: Counter = Counter()   # requests made, by kind
        self.signin: Set[str] = set()      # Drive files that need a Google sign-in

    def _pause(self, bucket: str, gap: float) -> None:
        wait = gap - (time.monotonic() - self._last.get(bucket, 0.0))
        if wait > 0:
            time.sleep(wait)
        self._last[bucket] = time.monotonic()

    def request(self, method: str, url: str, bucket: str, gap: float, **kwargs) -> requests.Response:
        """One request with retries on timeouts, connection errors and 5xx."""
        delay = 3.0
        for attempt in range(1, 5):
            self._pause(bucket, gap)
            self.counts[f"{bucket} {method}"] += 1
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
            return response
        raise RuntimeError("unreachable")

    # -- Family Services --

    def dfs_page(self) -> str:
        response = self.request("GET", DFS_PAGE, "dfs", PAGE_GAP)
        response.raise_for_status()
        return response.text

    def drive_folder(self, folder_id: str) -> str:
        response = self.request("GET", DRIVE_FOLDER.format(id=folder_id), "drive", DRIVE_GAP)
        response.raise_for_status()
        return response.text

    def drive_pdf(self, file_id: str) -> Optional[bytes]:
        """The file's bytes when Drive answers with a PDF. Drive answers with
        an HTML page for a file too large to virus-scan (asked again with the
        confirm URL) or when the download quota is hit (given up for this run;
        nothing failed is cached, so the next run asks again)."""
        problem = ""
        for url in (DRIVE_DOWNLOAD.format(id=file_id), DRIVE_DOWNLOAD_CONFIRM.format(id=file_id)):
            try:
                response = self.request("GET", url, "drive", DRIVE_GAP)
            except requests.RequestException as exc:
                problem = f"download failed: {exc}"
                continue
            if response.status_code != 200:
                problem = f"HTTP {response.status_code}"
                continue
            if response.content.startswith(b"%PDF"):
                return response.content
            if response.content[:3] == b"\xff\xd8\xff" or response.content[:4] == b"\x89PNG":
                # A photo of a form. The archive sync takes PDFs only, so the
                # picture is wrapped in a one-page PDF (same pixels).
                converted = image_to_pdf(response.content)
                if converted:
                    return converted
            kind = response.headers.get("Content-Type", "")
            if b"accounts.google.com" in response.content and b"Sign in" in response.content:
                # Not shared with the public: nothing to download, now or later.
                self.signin.add(file_id)
                logger.warning(f"  {file_id}: Drive asks for a sign-in (not shared); left out")
                return None
            problem = f"not a PDF ({kind or response.content[:12]!r})"
        logger.warning(f"  {file_id}: {problem}; skipped for this run")
        return None

    # -- Health --

    def wdh_search(self, url: str, extra: Dict) -> List[Dict]:
        body = {
            "pageNum": 0, "pageSize": 2000, "sortBy": [], "groupBy": [], "searchText": "",
            "searchType": "basic", "additionalParams": {}, "explicitFilters": [], "includeFields": [],
        }
        body.update(extra)
        response = self.request("POST", url, "wdh", PAGE_GAP, json=body)
        response.raise_for_status()
        data = response.json()
        return (data.get("Entries") if isinstance(data, dict) else data) or []

    def wdh_pdf(self, survey_id: int) -> Optional[bytes]:
        try:
            response = self.request("GET", WDH_DOWNLOAD.format(id=survey_id), "wdh", PAGE_GAP)
        except requests.RequestException as exc:
            logger.warning(f"  survey {survey_id}: download failed: {exc}")
            return None
        if response.status_code != 200 or not response.content.startswith(b"%PDF"):
            logger.warning(f"  survey {survey_id}: HTTP {response.status_code}, not a PDF; skipped for this run")
            return None
        return response.content


def image_to_pdf(data: bytes) -> Optional[bytes]:
    import io
    from PIL import Image
    try:
        image = Image.open(io.BytesIO(data)).convert("RGB")
        out = io.BytesIO()
        image.save(out, "PDF", resolution=200.0)
        return out.getvalue()
    except Exception as exc:
        logger.warning(f"  could not wrap an image in a PDF: {exc}")
        return None


# ── Dates and small helpers ──────────────────────────────────────────────────

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}
MONTH_ABBR = {m[:3]: i for m, i in MONTHS.items()}
MONTH_RE = (r"(January|February|March|April|May|June|July|August|September|October|November|December"
            r"|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept?|Oct|Nov|Dec)")
# "February 26, 2026", "August, 10, 2022", "April 29. 2025", "October\n30,2024",
# and the page's typo "June 1, 20226".
LONG_DATE = re.compile(MONTH_RE + r"\.?,?\s+(\d{1,2})(?:st|nd|rd|th)?\s*[,.]?\s*(\d{4,5})(?!\d)", re.I)
# "08-13-2025", "3-08-2024", "07-25-22", "4/19/23"
NUMERIC_DATE = re.compile(r"(?<![\d/-])(\d{1,2})[-/.](\d{1,2})[-/.](\d{4}|\d{2})(?![\d/-])")


def _year(value: str) -> Optional[int]:
    if len(value) == 2:
        return 2000 + int(value)
    if len(value) == 5:
        # "20226": one digit typed twice.
        for i in range(4):
            if value[i] == value[i + 1]:
                value = value[:i] + value[i + 1:]
                break
        else:
            return None
    year = int(value)
    return year if 1990 <= year <= datetime.now().year + 1 else None


def all_dates(value: str) -> List[str]:
    """Every full date in `value` as YYYY-MM-DD, in order of appearance."""
    found: List[Tuple[int, str]] = []
    for match in LONG_DATE.finditer(value or ""):
        month = MONTHS.get(match.group(1).lower()) or MONTH_ABBR.get(match.group(1).lower()[:3])
        year = _year(match.group(3))
        if month and year:
            try:
                found.append((match.start(), datetime(year, month, int(match.group(2))).strftime("%Y-%m-%d")))
            except ValueError:
                pass
    for match in NUMERIC_DATE.finditer(value or ""):
        year = _year(match.group(3))
        if year:
            try:
                found.append((match.start(), datetime(year, int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")))
            except ValueError:
                pass
    return [d for _, d in sorted(found)]


def one_line(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip()


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def format_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return ""


# ── Family Services: the page ────────────────────────────────────────────────

HEAD_RE = re.compile(r'<div[^>]*class="accordions-head[^"]*"[^>]*\bmain-text="([^"]*)"', re.S)
ANCHOR_RE = re.compile(r'<a\b[^>]*\bhref="([^"]+)"[^>]*>(.*?)</a>', re.S | re.I)
DRIVE_FILE_RE = re.compile(r"drive\.google\.com/(?:file/d/|open\?id=|uc\?(?:[^\"]*&)?id=)([\w-]{20,})")
DRIVE_FOLDER_RE = re.compile(r"drive\.google\.com/drive/(?:u/\d+/)?folders/([\w-]{20,})")


def parse_dfs_page(page: str) -> List[Dict]:
    """[{raw_name, docs: [{id, label}], folders: [id]}] in page order."""
    heads = list(HEAD_RE.finditer(page))
    facilities = []
    for index, head in enumerate(heads):
        end = heads[index + 1].start() if index + 1 < len(heads) else len(page)
        block = page[head.end():end]
        # The last accordion runs on into the page footer: stop at the end of
        # its content block.
        if index + 1 == len(heads):
            stop = re.search(r"</div>\s*</div>\s*</div>", block)
            if stop:
                block = block[:stop.start()]
        docs: Dict[str, str] = {}
        folders: List[str] = []
        for anchor in ANCHOR_RE.finditer(block):
            href = htmllib.unescape(anchor.group(1))
            label = one_line(htmllib.unescape(re.sub(r"<[^>]+>", " ", anchor.group(2))))
            folder = DRIVE_FOLDER_RE.search(href)
            if folder:
                if folder.group(1) not in folders:
                    folders.append(folder.group(1))
                continue
            file = DRIVE_FILE_RE.search(href)
            if not file:
                continue
            # The same id is often linked again around a stray line break,
            # with no text: keep the first label that says something.
            if file.group(1) not in docs or (label and not docs[file.group(1)]):
                docs[file.group(1)] = label
        facilities.append({
            "raw_name": one_line(htmllib.unescape(head.group(1))),
            "docs": [{"id": i, "label": label} for i, label in docs.items()],
            "folders": folders,
        })
    return facilities


FOLDER_ENTRY_RE = re.compile(
    r'<div class="flip-entry" id="entry-([\w-]+)".*?<a href="([^"]+)".*?class="flip-entry-title">(.*?)</div>',
    re.S,
)


def parse_drive_folder(listing: str) -> Tuple[List[Dict], List[str]]:
    """(files [{id, label}], subfolder ids) of an embedded folder listing."""
    files, folders = [], []
    for entry in FOLDER_ENTRY_RE.finditer(listing):
        href = htmllib.unescape(entry.group(2))
        title = one_line(htmllib.unescape(re.sub(r"<[^>]+>", " ", entry.group(3))))
        if DRIVE_FOLDER_RE.search(href) or "/folders/" in href:
            folders.append(entry.group(1))
        elif "/file/d/" in href:
            files.append({"id": entry.group(1), "label": title})
        # Google Docs, Sheets and the like are not report scans: left out.
    return files, folders


def clean_facility_name(raw: str) -> Tuple[str, str]:
    """('Trinity Teen Solutions', 'Currently not accepting placements as of
    09/28/2022'). A short parenthesis is part of the name ("(Riverton)",
    "(C-V)", "(YAHA)"); a note in a sentence is not."""
    name = re.sub(r"\s*--+\s*", " - ", raw)
    note = ""
    match = re.search(r"\s*\(([^()]*)\)\s*$", name)
    if match and (len(match.group(1).split()) > 3 or re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", match.group(1))):
        note = one_line(match.group(1))
        name = name[:match.start()]
    return one_line(name), note


# The page has one undivided list; the kind of provider is known by name.
DFS_CATEGORIES = [
    (r"\bBOCES\b", "BOCES residential school"),
    (r"\bDetention\b", "Juvenile detention center"),
    (r"\bCrisis Center\b", "Crisis center"),
    (r"\bGroup Home\b|\bYouth Home\b|Van Vleck House|Youth Alternative Home|Youth Development Services|Milestone",
     "Group home"),
    (r"Cathedral Home|Central Wyoming Counseling|Chrysalis|Meadowlark|Red Top Meadows|St\. Joseph|"
     r"Trinity Teen|Wyoming Behavioral Institute|YES House|Normative Services",
     "Residential treatment center"),
]


def dfs_category(name: str) -> str:
    for pattern, label in DFS_CATEGORIES:
        if re.search(pattern, name, re.I):
            return label
    return "Substitute care provider"


def label_kind(label: str) -> str:
    """What the state's link text says the document is: notice, visit, other
    or '' (a bare date or no text)."""
    text = label.lower()
    if re.search(r"non-?\s*compl\w+|scl-?\s*305", text):
        return "notice"
    if re.search(r"\bvisits?\b|\.(?:jpe?g|png)$", text):
        return "visit"   # a photo of a visit form is named like "RJDC 4.25.2023.jpg"
    if re.search(r"corrective action|action plan|\bcap\b|recertification|inspection|response|plan", text):
        return "other"
    return ""


def tidy_label(label: str) -> str:
    return one_line(re.sub(r"\.pdf$", "", label or "", flags=re.I))


# ── OCR ──────────────────────────────────────────────────────────────────────


def _find_tesseract() -> str:
    if os.getenv("TESSERACT_CMD"):
        return os.getenv("TESSERACT_CMD")
    default = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    return str(default) if default.exists() else ""


def _find_poppler() -> str:
    if os.getenv("POPPLER_PATH"):
        return os.getenv("POPPLER_PATH")
    root = Path("C:/tools")
    found = sorted(root.glob("poppler-*/Library/bin"), reverse=True) if root.exists() else []
    return str(found[0]) if found else ""


def _ocr_modules():
    import pytesseract
    from pdf2image import convert_from_path
    tesseract = _find_tesseract()
    if tesseract:
        pytesseract.pytesseract.tesseract_cmd = tesseract
    return pytesseract, convert_from_path


def page_images(path: Path) -> List[Any]:
    _, convert_from_path = _ocr_modules()
    return convert_from_path(str(path), dpi=OCR_DPI, poppler_path=_find_poppler() or None)


def ocr_page(image) -> Dict:
    """{'text', 'confidence', 'words', 'data'}: the page's lines rebuilt from
    Tesseract's word table, the mean word confidence and the table itself."""
    pytesseract, _ = _ocr_modules()
    data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)
    lines: Dict[Tuple[int, int, int], List[str]] = {}
    order: List[Tuple[int, int, int]] = []
    confidences: List[float] = []
    for i, word in enumerate(data["text"]):
        if not word.strip():
            continue
        try:
            confidence = float(data["conf"][i])
        except (TypeError, ValueError):
            confidence = -1.0
        if confidence < 0:
            continue
        key = (data["block_num"][i], data["par_num"][i], data["line_num"][i])
        if key not in lines:
            lines[key] = []
            order.append(key)
        lines[key].append(word)
        confidences.append(confidence)
    out: List[str] = []
    previous: Optional[Tuple[int, int, int]] = None
    for key in order:
        if previous is not None and key[:2] != previous[:2]:
            out.append("")
        out.append(" ".join(lines[key]))
        previous = key
    return {
        "text": "\n".join(out).strip(),
        "confidence": sum(confidences) / len(confidences) if confidences else 0.0,
        "words": len(confidences),
        "data": data,
    }


VISIT_FORM = re.compile(r"SCL\s*-?\s*300\b|FACILITY\s+VISIT|Observations\s*/\s*Comments|Provider\s+Comments", re.I)
NOTICE_FORM = re.compile(r"SCL\s*-?\s*305\b|NOTICE\s+OF\s+NON-?\s*COMPLIANCE", re.I)


def form_of(text: str) -> str:
    head = text[:1500]
    # The visit form's own header names it; its fine print ("notice of ...
    # non-compliance") must not make it a notice.
    if re.search(r"SCL\s*-?\s*300\b|Facility\s+Site\s+Visit\s+Form", head, re.I):
        return "SCL-300"
    if NOTICE_FORM.search(head):
        return "SCL-305"
    if VISIT_FORM.search(text):
        return "SCL-300"
    return ""


REASON_WORDS = {
    "unannounced": re.compile(r"^unannounced$", re.I),
    "complaint": re.compile(r"^complaint$", re.I),
    "change": re.compile(r"^change$", re.I),
    "compliance": re.compile(r"^compliance$", re.I),
    "technical": re.compile(r"^t?echnical$", re.I),
}


def reason_boxes(image, data: Dict, name: str) -> Dict[str, float]:
    """Ink in the checkbox left of each printed reason on the visit form:
    {reason: share of dark pixels}. Numbers only; nothing is read from the
    handwriting. The reasons sit in the upper half of the page."""
    width, height = image.size
    grey = image.convert("L")
    found: Dict[str, Tuple[int, int, int, int]] = {}
    for i, word in enumerate(data["text"]):
        token = re.sub(r"[^A-Za-z]", "", word or "")
        if not token or data["top"][i] > height * 0.6:
            continue
        for reason, pattern in REASON_WORDS.items():
            if reason not in found and pattern.match(token):
                found[reason] = (data["left"][i], data["top"][i], data["width"][i], data["height"][i])
    out: Dict[str, float] = {}
    for reason, (left, top, _w, h) in found.items():
        size = max(h, int(OCR_DPI * 0.11))
        box = (max(0, left - int(size * 1.75)), max(0, top - int(size * 0.25)),
               max(1, left - int(size * 0.25)), min(height, top + int(size * 1.25)))
        if box[2] - box[0] < 8:
            continue
        crop = grey.crop(box)
        pixels = list(crop.getdata())
        out[reason] = round(sum(1 for p in pixels if p < 140) / max(1, len(pixels)), 4)
    if REASON_CROPS and found:
        try:
            tops = [v[1] for v in found.values()]
            row = image.crop((0, max(0, min(tops) - 40), width, min(height, max(tops) + 90)))
            Path(REASON_CROPS).mkdir(parents=True, exist_ok=True)
            row.save(Path(REASON_CROPS) / f"{Path(name).stem}.png")
        except OSError:
            pass
    return out


def extract_dfs(path: Path) -> Dict:
    """What a Family Services PDF holds. A visit form (SCL-300) is never
    transcribed: the result carries the form number, the page count and the
    checkbox ink only. Other documents keep the text of their typed pages."""
    digest = hashlib.md5(path.read_bytes()).hexdigest()
    # A text layer would make OCR unnecessary; none was seen in 2026.
    layer = ""
    try:
        with pdfplumber.open(path) as pdf:
            page_count = len(pdf.pages)
            layer = "\n\n".join((page.extract_text() or "") for page in pdf.pages).strip()
    except Exception:
        page_count = 0
    if len(layer) > 400:
        form = form_of(layer)
        if form == "SCL-300":
            return {"text": NOT_TRANSCRIBED, "has_text": False, "form": form, "pages": page_count,
                    "ocr": False, "md5": digest}
        return {"text": layer, "has_text": True, "form": form, "pages": page_count,
                "typed_pages": page_count, "ocr": False, "md5": digest}

    try:
        images = page_images(path)
    except Exception as exc:  # poppler missing or a malformed file
        logger.warning(f"  could not render {path.name}: {exc}")
        return {"text": "", "md5": digest}
    if not images:
        return {"text": "", "md5": digest}
    try:
        first = ocr_page(images[0])
    except Exception as exc:
        logger.warning(f"  OCR failed for {path.name}: {exc}")
        return {"text": "", "md5": digest}
    form = form_of(first["text"])
    base = {"form": form, "pages": len(images), "ocr": True, "md5": digest}
    if form == "SCL-300":
        base.update({"text": NOT_TRANSCRIBED, "has_text": False,
                     "reason_ink": reason_boxes(images[0], first["data"], path.name)})
        return base

    texts: List[str] = []
    confidences: List[float] = []
    skipped = 0
    for index, image in enumerate(images):
        page = first if index == 0 else ocr_page(image)
        if page["words"] == 0:
            continue  # the blank back of a sheet
        if VISIT_FORM.search(page["text"]) and not NOTICE_FORM.search(page["text"][:1500]):
            skipped += 1  # a visit form attached to something else
            continue
        if page["confidence"] < TYPED_CONFIDENCE or page["words"] < TYPED_MIN_WORDS:
            # Handwriting, a signature page or a poor scan. A notice's own
            # pages are kept whatever they score: the form is typed.
            if form != "SCL-305" or page["words"] < 5:
                skipped += 1
                continue
        texts.append(page["text"])
        confidences.append(page["confidence"])
    text = "\n\n".join(texts).strip()
    base.update({
        "text": text or NOT_TRANSCRIBED,
        "has_text": bool(text),
        "typed_pages": len(texts),
        "skipped_pages": skipped,
        "confidence": round(sum(confidences) / len(confidences), 1) if confidences else 0.0,
    })
    return base


# ── Family Services: the notice ──────────────────────────────────────────────

FOUND_ON = re.compile(
    r"found\s+on\s+the\s+alleged\s+v\w+\W{0,4}s?\W{0,3}.{0,220}?for\s+Children\s*[:;.]?",
    re.I | re.S,
)
DATE_OF_ALLEGATION = re.compile(r"Date\s+of\s+Alleg\w+\s*[:;.|]?", re.I)
RULES_VIOLATED = re.compile(r"(?:Rules?\s+)?Violated\s*[:;.|]", re.I)
EXPLANATION = re.compile(r"Explanation\s+for\b", re.I)
RECEIVED_ON = re.compile(r"received\s+on\s*[:;.]?\s*", re.I)
CERTIFICATION_OF = re.compile(r"Certification\s+of\s+Providers", re.I)
FINDING_SENTENCE = re.compile(
    r"(Evidence\s+(?:does\s+not\s+|did\s+not\s+|doesn't\s+)?sup\s*ports?\b[^\n]{0,120})", re.I)
FINDING_LABEL = re.compile(r"FINDING\s*:\s*(NON\W*COMPLIANCE|COMPLIANCE)\b", re.I)
NONCOMPLIANCE_WORD = re.compile(r"non\W*c\s*o\s*m\s*p\s*l\s*i\s*a\s*n\s*c\s*e", re.I)
SUPPORTS = re.compile(r"evidence\s+supports?\b.{0,60}?non-compliance", re.I | re.S)
NOT_SUPPORTS = re.compile(r"(?:does|did)\s*(?:not|n't)\s+support|unsupported|not\s+supported", re.I)


def clean_finding(value: str) -> str:
    """The form's finding line as the state words it, whatever the OCR did to
    it ('Evidence supports findings of non-comp liance, |')."""
    text = one_line(value)
    negative = bool(re.search(r"(?:does|did)\s*(?:not|n't)|not\s+support", text, re.I))
    article = "a finding of" if re.search(r"\ba\s+finding", text, re.I) else "findings of"
    return f"Evidence {'does not support' if negative else 'supports'} {article} non-compliance."


CHAPTER = re.compile(r"\bChapter\s+(\d{1,2})\b", re.I)
SECTION = re.compile(r"\bSection\s+(\d{1,3})\s*[.:,]?\s*([A-Z][^\n]{2,120})?", re.I)


def _junk_line(line: str) -> bool:
    """OCR debris between form fields (': n:', '|', 'a')."""
    letters = re.sub(r"[^A-Za-z]", "", line)
    return len(letters) < 3


def parse_notice(text: str) -> Dict:
    """Allegation, dates, finding and rules of an SCL-305 notice from its OCR text."""
    out: Dict[str, Any] = {
        "received_date": "", "allegation": "", "allegation_date": "", "finding": "",
        "non_compliance": False, "rules": [],
    }
    received = RECEIVED_ON.search(text)
    if received:
        dates = all_dates(one_line(text[received.end(): received.end() + 60]))
        out["received_date"] = dates[0] if dates else ""

    rules_at = RULES_VIOLATED.search(text)
    date_at = DATE_OF_ALLEGATION.search(text)
    found = FOUND_ON.search(text)
    if found:
        certification = CERTIFICATION_OF.search(text, found.end())
        stops = [x.start() for x in (date_at, rules_at, certification) if x and x.start() > found.end()]
        finding_after = FINDING_SENTENCE.search(text, found.end())
        if finding_after:
            stops.append(finding_after.start())
        body = text[found.end(): min(stops) if stops else found.end() + 900]
        lines = [one_line(line) for line in body.split("\n")]
        lines = [l for l in lines if not _junk_line(l) and not re.fullmatch(r"(?:\W*)(?:Allegation|Finding)s?\W*", l, re.I)]
        allegation = one_line(" ".join(lines))
        allegation = re.sub(r"^(?:Allegation\W+)", "", allegation, flags=re.I)
        allegation = re.sub(r"\s*\bDate\s+of\b.*$", "", allegation, flags=re.I)   # the next field, run together
        allegation = re.sub(r"(?<=[.!?])\s+(?:Reported|Allegation)\W*$", "", allegation, flags=re.I)
        allegation = one_line(allegation.replace("_", " ")).lstrip("|;:,. ")
        out["allegation"] = allegation[:1200]

    if date_at:
        dates = all_dates(one_line(text[date_at.end(): date_at.end() + 50]))
        out["allegation_date"] = dates[0] if dates else ""

    head = text[: rules_at.start()] if rules_at else text
    finding = None
    for match in FINDING_SENTENCE.finditer(head):
        finding = match  # the last one before the rules is the form's finding line
    if not finding:
        finding = FINDING_SENTENCE.search(text)
    label = FINDING_LABEL.search(text)
    if not finding and label:
        out["finding"] = "Finding: " + ("non-compliance." if "non" in label.group(1).lower() else "compliance.")
        out["non_compliance"] = "non" in label.group(1).lower()
    if finding:
        out["finding"] = clean_finding(finding.group(1))
        out["non_compliance"] = bool(SUPPORTS.search(out["finding"])) and not NOT_SUPPORTS.search(out["finding"])

    if rules_at:
        stop = EXPLANATION.search(text, rules_at.end())
        block = text[rules_at.end(): stop.start() if stop else rules_at.end() + 2500]
        chapter = ""
        rules: List[Dict[str, str]] = []
        for line in block.split("\n"):
            line = one_line(line)
            chapter_match = CHAPTER.search(line)
            if chapter_match:
                chapter = chapter_match.group(1)
            section = SECTION.search(line)
            if section and (line.lower().startswith("section") or chapter_match):
                title = one_line(section.group(2) or "").rstrip(".")
                rule = {"chapter": chapter, "section": section.group(1), "title": title[:140]}
                if not any(r["chapter"] == rule["chapter"] and r["section"] == rule["section"] for r in rules):
                    rules.append(rule)
        out["rules"] = rules
    return out


# ── Privacy ──────────────────────────────────────────────────────────────────

# Words that follow "youth", "child" and so on in capitals without being a
# person: provider names, rule headings, places.
NOT_A_NAME = {
    "crisis", "center", "home", "homes", "house", "alternative", "development", "services", "service",
    "care", "protective", "protection", "welfare", "rights", "handbook", "records", "record", "safety",
    "health", "supervision", "treatment", "wyoming", "department", "family", "placement", "abuse",
    "neglect", "discipline", "grievance", "the", "and", "was", "were", "had", "has", "who", "that",
    "with", "from", "during", "while", "when", "after", "before", "did", "does", "said", "stated",
    "reported", "interviews", "interview", "program", "programs", "residential", "group", "detention",
    "boces", "academy", "association", "inc", "act", "code", "chapter", "section", "plan", "support",
    "emergency", "management", "restraint", "seclusion", "medication", "file", "files", "intake",
    "advocate", "advocacy", "council", "court", "county", "school", "education", "staff", "ratio",
    "ratios", "behavior", "policy", "policies", "training", "requirements", "requirement", "caring",
    "placing", "agency", "agencies", "solutions", "meadows", "institute", "behavioral", "this", "there",
    "they", "these", "those", "in", "on", "at", "is", "to", "of", "for", "by", "or", "as", "it", "he",
    "she", "a", "an", "if", "no", "not", "all", "any", "each", "may", "shall", "will", "must",
    "corrective", "action", "actions", "certification", "administrative", "rules", "rule", "violation",
    "violations", "notice", "finding", "findings", "evidence", "investigation", "allegation", "compliance",
    "non", "facility", "provider", "providers", "substitute", "children", "reported", "report", "access",
}
NAMED_PERSON = re.compile(
    r"\b(?:youth|resident|child|client|student|minor|juvenile|patient|girl|boy|daughter|son)s?"
    r"(?:[ \t]+named|[ \t]*,)?[ \t]+([A-Z][a-z]{2,})\s+([A-Z][a-z]{2,})\b"
)
PRIVACY_PATTERNS = [
    ("date of birth", re.compile(r"\bD\.?\s?O\.?\s?B\b\.?|\bdate\s+of\s+birth\b|\bbirth\s*date\b|\bborn\s+on\b", re.I)),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b|\bSSN\b|social\s+security\s+(?:number|no)", re.I)),
    ("record number", re.compile(
        r"\b(?:case|record|client|medicaid|medical\s+record|MRN|patient|youth|resident)\s*"
        r"(?:ID|number|no\.?|#)\s*[:#]?\s*[A-Z]{0,3}\d{4,}", re.I)),
]


# Documents the owner read and released after the check held them (a false
# positive), by Drive file id. Never add one without the owner's word.
OWNER_RELEASED = {
    # Meadowlark Academy notice 2024-04-04: "calling a youth Miley Cyrus" (owner, 2026-10-02).
    "10K2OFKGMb406oMQ0795PMDq-D8r_QyOA",
}


def privacy_hits(text: str) -> List[str]:
    """Names of the checks `text` trips; empty when it can be posted."""
    hits: List[str] = []
    for name, pattern in PRIVACY_PATTERNS:
        if pattern.search(text):
            hits.append(name)
    for match in NAMED_PERSON.finditer(text):
        if match.group(1).lower() in NOT_A_NAME or match.group(2).lower() in NOT_A_NAME:
            continue
        hits.append("a named young person")
        break
    return hits


# ── Health department: the CMS-2567 ──────────────────────────────────────────

# The form is a table: tag, findings, tag again, plan of correction, date. The
# column edges are measured on each page from the printed header ("PREFIX"
# twice, "COMPLETION"), because a scan is placed differently on every page.
# Findings and plan are kept as two streams, and the plan is tied to its tag
# by the tag number printed again beside it, so skew between the columns
# cannot hand a plan to the wrong finding.
FALLBACK_EDGES = {"p1": 0.051, "p2": 0.497, "date": 0.870}
BODY_TOP_FALLBACK, BODY_BOTTOM_FALLBACK = 183 / 792, 656 / 792
TAG_RE = re.compile(r"^\{?([A-Z]{1,2})\s?(\d{3,4})\}?$")
TAG_ONE = re.compile(r"^\{?([A-Z]{1,2})([O0-9]{3,4})\}?$")
NOT_MET = re.compile(
    r"This\s+(?:STANDARD|ELEMENT|CONDITION|REQUIREMENT|RULE)\s+(?:is\s+)?not\s+met\s+as\s+evidenced\s+by\s*:?", re.I)
CONTINUED = re.compile(r"\s*C\w{2,8}\s+F\w{2,5}\s+page", re.I)


def _calibrate(words: List[Dict], width: float, height: float) -> Dict[str, Any]:
    """Column edges and the body's top and bottom for one page, in page units."""
    head = [w for w in words if w["top"] < 0.4 * height]
    prefixes = sorted((w for w in head if re.sub(r"[^A-Za-z]", "", w["text"]).upper() in ("PREFIX", "PREFI", "REFIX")),
                      key=lambda w: w["x0"])
    completion = [w for w in head if re.sub(r"[^A-Za-z]", "", w["text"]).upper().startswith("COMPLET")]
    edges: Dict[str, Any] = {
        "p1": FALLBACK_EDGES["p1"] * width, "p2": FALLBACK_EDGES["p2"] * width,
        "date": FALLBACK_EDGES["date"] * width, "top": BODY_TOP_FALLBACK * height,
        "bottom": BODY_BOTTOM_FALLBACK * height, "found": False}
    if len(prefixes) >= 2 and prefixes[-1]["x0"] - prefixes[0]["x0"] > 0.25 * width:
        edges["p1"], edges["p2"] = prefixes[0]["x0"], prefixes[-1]["x0"]
        edges["found"] = True
        tags = [w for w in head if w["text"].upper() == "TAG" and abs(w["x0"] - prefixes[0]["x0"]) < 0.06 * width
                and 0 < w["top"] - prefixes[0]["top"] < 0.04 * height]
        edges["top"] = (tags[0]["top"] if tags else prefixes[0]["top"] + 0.012 * height) + 0.034 * height
    if completion:
        edges["date"] = completion[0]["x0"] - 0.015 * width
    def header_words(names, right: bool) -> List[float]:
        return [w["top"] for w in head if w["top"] < 0.3 * height
                and re.sub(r"[^A-Za-z]", "", w["text"]).upper() in names and (w["x0"] >= edges["p2"]) == right]

    edges["top_right"] = edges["top"]
    left_ends = header_words(("INFORMATION", "IDENTIFYING", "REGULATORY"), False)
    if left_ends and not edges["found"]:
        edges["top"] = max(left_ends) + 0.012 * height
        edges["top_right"] = edges["top"]
    right_ends = header_words(("APPROPRIATE", "DEFICIENCY", "DEFICIENC"), True)
    if right_ends:
        edges["top_right"] = max(edges["top"], max(right_ends) + 0.008 * height)
    foot = [w["top"] for w in words if w["top"] > 0.6 * height
            and re.match(r"CMS|continuation|LABORATORY|PROVIDERSUPPLIER", re.sub(r"[^A-Za-z0-9-]", "", w["text"]), re.I)]
    for w in words:
        if w["top"] > 0.6 * height and w["text"] == "Any" and any(
                v["text"].lower().startswith("deficien") and abs(v["top"] - w["top"]) < 0.006 * height
                and 0 < v["x0"] - w["x0"] < 0.15 * width for v in words):
            foot.append(w["top"])
    if foot:
        edges["bottom"] = min(foot) - 0.004 * height
    return edges


def _lines(words: List[Dict], height: float) -> List[List[Dict]]:
    lines: List[List[Dict]] = []
    last = None
    for word in sorted(words, key=lambda w: (w["top"], w["x0"])):
        if lines and last is not None and abs(last - word["top"]) <= height * 0.0045:
            lines[-1].append(word)
        else:
            lines.append([word])
        last = word["top"]
    return [sorted(line, key=lambda w: w["x0"]) for line in lines]


def _tag_code(letters: str, digits: str) -> str:
    """'N0137' / 'NO137' / 'N 137' -> 'N 137'; E tags keep four digits ('E 0001')."""
    if len(letters) == 2 and letters[1] == "O":
        letters, digits = letters[0], "0" + digits  # "NO 137": N0137 read as two words
    digits = digits.replace("O", "0")
    if len(letters) != 1 or not digits.isdigit():
        return ""
    number = digits.lstrip("0") or "0"
    return f"{letters} {number.zfill(4 if letters == 'E' else 3)}"


def _clean_token(text: str) -> str:
    return re.sub(r"^[^\w{]+|[^\w}]+$", "", text or "")


def _leading_tag(line: List[Dict], limit: float, slack: float) -> Tuple[str, int]:
    """('A 148', tokens used) when the line opens with a tag number in the tag
    column. Scan debris before it (';', '|') is skipped and counted as used."""
    skip = 0
    while skip < len(line) - 1 and not _clean_token(line[skip]["text"]) and line[skip]["x0"] <= limit:
        skip += 1
    rest = line[skip:]
    first = rest[0]
    if first["x0"] > limit:
        return "", 0
    one = TAG_ONE.match(_clean_token(first["text"]))
    if one:
        code = _tag_code(one.group(1), one.group(2))
        return (code, skip + 1) if code else ("", 0)
    if len(rest) > 1 and rest[1]["x0"] <= limit + slack:
        both = TAG_RE.match(_clean_token(first["text"]) + " " + _clean_token(rest[1]["text"]))
        if both:
            code = _tag_code(both.group(1), both.group(2))
            return (code, skip + 2) if code else ("", 0)
    return "", 0


def _page_streams(words: List[Dict], width: float, height: float) -> Dict[str, Any]:
    edges = _calibrate(words, width, height)
    body = [w for w in words if edges["top"] <= w["top"] <= edges["bottom"]]
    mid = edges["p2"] - 0.006 * width
    left = [w for w in body if w["x0"] < mid]
    right = [w for w in body if mid <= w["x0"] < edges["date"] and w["top"] >= edges["top_right"]]
    dates = [w for w in body if w["x0"] >= edges["date"] and w["top"] >= edges["top_right"]]
    out: Dict[str, Any] = {"left": [], "right": [], "dates": [], "calibrated": edges["found"]}
    for line in _lines(left, height):
        tag, used = _leading_tag(line, edges["p1"] + 0.075 * width, 0.05 * width)
        out["left"].append((tag, " ".join(w["text"] for w in line[used:])))
    for line in _lines(right, height):
        tag, used = _leading_tag(line, edges["p2"] + 0.06 * width, 0.05 * width)
        out["right"].append((tag, " ".join(w["text"] for w in line[used:])))
    out["dates"] = [" ".join(w["text"] for w in line) for line in _lines(dates, height)]
    out["plain"] = "\n".join(" ".join(w["text"] for w in line) for line in _lines(words, height))
    return out


def _tag_column_words(image, pytesseract) -> List[Dict]:
    """Words of the two tag columns read on their own: a scan's printed tag
    numbers are small, bold and close to the column rule, and the page-wide
    read often drops them (the findings beside them read fine)."""
    width, height = image.size
    found: List[Dict] = []
    for left, right in ((0.05, 0.165), (0.47, 0.575)):
        x0, y0 = int(left * width), int(0.2 * height)
        crop = image.crop((x0, y0, int(right * width), int(0.9 * height)))
        crop = crop.resize((crop.size[0] * 2, crop.size[1] * 2))
        for psm in ("6", "11"):
            data = pytesseract.image_to_data(crop, config=f"--psm {psm}", output_type=pytesseract.Output.DICT)
            for i, token in enumerate(data["text"]):
                clean = _clean_token(token)
                if clean and float(data["conf"][i]) > 0 and (
                        TAG_ONE.match(clean) or re.fullmatch(r"[A-Z]{1,2}", clean) or re.fullmatch(r"\d{3,4}", clean)):
                    word = {"text": token, "x0": x0 + data["left"][i] / 2, "top": y0 + data["top"][i] / 2}
                    if not any(f["text"] == word["text"] and abs(f["x0"] - word["x0"]) < 25
                               and abs(f["top"] - word["top"]) < 25 for f in found):
                        found.append(word)
    digits = [f for f in found if re.fullmatch(r"\d{3,4}", _clean_token(f["text"]))]
    return [f for f in found
            if not re.fullmatch(r"[A-Z]{1,2}", _clean_token(f["text"]))
            or any(abs(d["top"] - f["top"]) < 20 and 0 < d["x0"] - f["x0"] < 150 for d in digits)]


def read_2567(path: Path) -> Dict:
    """The form's columns, page by page. Text PDFs are read by word position;
    a scanned form is OCR'd and its words placed the same way."""
    pages: List[Dict] = []
    ocr = False
    with pdfplumber.open(path) as pdf:
        page_count = len(pdf.pages)
        has_text = any((page.extract_text() or "").strip() for page in pdf.pages)
        if has_text:
            for page in pdf.pages:
                words = [{"text": w["text"], "x0": w["x0"], "top": w["top"]} for w in page.extract_words()]
                pages.append(_page_streams(words, page.width, page.height))
    if not has_text:
        ocr = True
        pytesseract, _ = _ocr_modules()
        for image in page_images(path):
            data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)
            words = []
            for i, token in enumerate(data["text"]):
                if token.strip() and float(data["conf"][i]) >= 0:
                    words.append({"text": token, "x0": data["left"][i], "top": data["top"][i]})
            extra = [x for x in _tag_column_words(image, pytesseract)
                     if not any(w["text"] == x["text"] and abs(w["x0"] - x["x0"]) < 25 and abs(w["top"] - x["top"]) < 25
                                for w in words)]
            pages.append(_page_streams(words + extra, image.size[0], image.size[1]))
        page_count = len(pages)
    return {"streams": pages, "pages": page_count, "ocr": ocr}


def parse_2567(pages: List[Dict], tie_plans: bool = True) -> Dict:
    """Tags with their regulation, findings and plan of correction. With
    tie_plans False (a scan, whose small tag numbers beside the plan are
    often misread) the plan is returned whole as plan_text instead of being
    handed to a tag it may not belong to."""
    loose: List[str] = []
    tags: List[Dict[str, Any]] = []
    by_code: Dict[str, Dict[str, Any]] = {}
    current: Optional[Dict[str, Any]] = None
    plan_to: Optional[Dict[str, Any]] = None
    for page in pages:
        for tag_cell, text in page["left"]:
            if tag_cell:
                if tag_cell in by_code:
                    # "N 137 Continued From page 1": the same tag again.
                    current = by_code[tag_cell]
                    if text and not CONTINUED.match(text):
                        current["lines"].append(text)
                    continue
                current = {"tag": tag_cell, "lines": [], "plan": [], "completion": ""}
                tags.append(current)
                by_code.setdefault(tag_cell, current)
                if text and not CONTINUED.match(text):
                    current["lines"].append(text)
                continue
            if current is not None and text and not CONTINUED.match(text):
                current["lines"].append(text)
        for tag_cell, text in page["right"]:
            if tag_cell:
                plan_to = by_code.get(tag_cell) or current
            target = plan_to or current
            if text and not tie_plans:
                loose.append(text)
            elif text and target is not None:
                target["plan"].append(text)
        for text in page["dates"]:
            dates = all_dates(text)
            if dates and current is not None and not current["completion"]:
                current["completion"] = dates[0]
    out = _finish_tags(tags)
    out["plan_text"] = unwrap("\n".join(loose))
    return out


CFR_LINE = re.compile(r"^.{0,6}?CFR", re.I)
CFR_PREFIX = re.compile(r"^.{0,6}?CFR\s*[(\[{]?s?[)\]}j]?\s*[:;]?\s*", re.I)


def _finish_tags(tags: List[Dict[str, Any]]) -> Dict:
    initial = ""
    out_tags: List[Dict[str, Any]] = []
    for tag in tags:
        lines = tag["lines"]
        # Title: the leading lines in capitals.
        title_lines = []
        while lines and not CFR_LINE.match(lines[0]) and lines[0].upper() == lines[0] and len(title_lines) < 4:
            title_lines.append(lines.pop(0))
        title = one_line(" ".join(title_lines))
        title = re.sub(r"\s+[A-Z]{1,2}\s?[O0-9]{3,4}$", "", title)
        cfr = ""
        if lines and CFR_LINE.match(lines[0]):
            cfr = one_line(CFR_PREFIX.sub("", lines.pop(0)))
        body = "\n".join(lines).strip()
        if re.fullmatch(r"[A-Z]{1,2} 0{3,4}", tag["tag"]) or re.match(r"INITIAL COMMENTS", title, re.I):
            initial = (initial + "\n" + re.sub(r"^Initial\s+Comments\s*", "", body, flags=re.I)).strip()
            continue
        split = NOT_MET.search(body)
        if not title and not cfr and not split and out_tags:
            # A tag number misread by OCR, or a heading-less continuation: it
            # belongs to the tag before it, not a tag of its own.
            out_tags[-1]["evidence"] += "\n" + unwrap(body)
            if tag["plan"]:
                out_tags[-1]["plan"] += "\n" + unwrap("\n".join(tag["plan"]))
            continue
        regulation = body[: split.start()].strip() if split else ""
        evidence = body[split.end():].strip() if split else body
        out_tags.append({
            "tag": tag["tag"],
            "title": title,
            "cfr": cfr,
            "regulation": one_line(regulation),
            "evidence": unwrap(evidence),
            "plan": unwrap("\n".join(tag["plan"])),
            "completion": tag["completion"],
        })
    return {
        "initial_comments": unwrap(re.sub(r"\b\w{0,3}nitial\s+Comments\b|\b[A-Z]\s?[O0]{3,4}\b", " ", initial)),
        "complaint_intakes": sorted(set(re.findall(r"\bWY\d{6,}\b", initial))),
        "tags": out_tags,
    }


def unwrap(text: str) -> str:
    """Join a column's wrapped lines into paragraphs: a new paragraph starts
    at a numbered or lettered item."""
    paragraphs: List[str] = []
    for line in (text or "").split("\n"):
        line = one_line(line)
        if not line:
            continue
        if paragraphs and not re.match(r"^(?:\d{1,2}\.|[a-z]\.|\([a-z0-9]{1,3}\))\s", line):
            paragraphs[-1] += " " + line
        else:
            paragraphs.append(line)
    return "\n".join(paragraphs)


def extract_wdh(path: Path) -> Dict:
    form = read_2567(path)
    parsed = parse_2567(form["streams"], tie_plans=not form["ocr"])
    parts: List[str] = []
    if parsed["initial_comments"]:
        parts.append("INITIAL COMMENTS\n" + parsed["initial_comments"])
    for tag in parsed["tags"]:
        block = f"{tag['tag']} {tag['title']}".strip()
        if tag["cfr"]:
            block += f"\nCFR(s): {tag['cfr']}"
        if tag["regulation"]:
            block += "\n" + tag["regulation"]
        if tag["evidence"]:
            block += "\nThis is not met as evidenced by:\n" + tag["evidence"]
        if tag["plan"]:
            block += "\nPROVIDER'S PLAN OF CORRECTION\n" + tag["plan"]
        parts.append(block)
    if parsed.get("plan_text"):
        parts.append("PROVIDER'S PLAN OF CORRECTION\n" + parsed["plan_text"])
    text = "\n\n".join(parts).strip()
    if not text:
        # A one-page form with nothing in the table (no deficiencies), or a
        # letter: keep whatever the page says.
        with pdfplumber.open(path) as pdf:
            text = "\n".join((page.extract_text() or "") for page in pdf.pages).strip()
    if not text:
        # A scan the table reader found nothing in: the page lines as read,
        # columns mixed. Kept so search can find it; the page shows no tags.
        text = "\n\n".join(p["plain"] for p in form["streams"]).strip()
    return {"text": text.replace("\ufffd", "").replace("\x0c", " "), "pages": form["pages"], "ocr": form["ocr"], **parsed}


# ── Scraper ──────────────────────────────────────────────────────────────────


class WYScraper:
    def __init__(self, fetcher: Optional[Fetcher] = None, reports: ReportStore = REPORTS):
        self.fetch = fetcher or Fetcher()
        self.reports = reports
        self.stats: Counter = Counter()
        self.findings: Counter = Counter()
        self.unparsed: List[str] = []
        self.undated: List[str] = []
        self.held: List[str] = []
        self.failed: List[str] = []
        self.duplicates: List[str] = []
        self.reason_ink: List[Dict[str, float]] = []
        # --reparse: read the archived copy before asking the source, so a
        # parser fix after clearing the extract cache costs no download.
        self.reparse = False
        self.retry_unavailable = False
        self.unavailable: List[str] = []

    def pdf_bytes(self, name: str, fetch):
        if self.reparse:
            data = self.reports.archived_bytes(name)
            if data:
                return data
        return fetch()

    # -- Family Services --

    def hold(self, archive_name: str) -> None:
        """Move a held document's archived copy out of the Drive folder."""
        source = self.reports.archive_dir / archive_name
        try:
            data = source.read_bytes()
        except OSError:
            return
        try:
            HELD_DIR.mkdir(parents=True, exist_ok=True)
            (HELD_DIR / archive_name).write_bytes(data)
            source.unlink()
        except OSError as exc:
            logger.warning(f"  could not move held document {archive_name}: {exc}")

    def unhold(self, archive_name: str) -> None:
        """Put a document back in the archive when the check that held it no
        longer holds it (the rule changed)."""
        held = HELD_DIR / archive_name
        if not held.exists() or (self.reports.archive_dir / archive_name).exists():
            return
        try:
            self.reports.archive(archive_name, held.read_bytes())
            held.unlink()
        except OSError as exc:
            logger.warning(f"  could not restore {archive_name} to the archive: {exc}")

    def dfs_report(self, facility: str, doc: Dict, extracted: Dict) -> Optional[Dict]:
        file_id = doc["id"]
        label = tidy_label(doc.get("label", ""))
        archive_name = f"{file_id}.pdf"
        form = extracted.get("form", "")
        from_label = label_kind(label)
        kind = {"SCL-305": "notice", "SCL-300": "visit"}.get(form) or from_label or "other"
        text = extracted.get("text", "") if extracted.get("has_text") else ""
        if kind == "visit":
            text = ""  # never transcribed, whatever the scan held
        label_dates = all_dates(label)

        categories: Dict[str, Any] = {
            "source": "DFS",
            "kind": kind,
            "label": label,
            "form": form,
            "ocr": bool(text) and bool(extracted.get("ocr")),
            "pages": extracted.get("pages", 0),
            "listed": doc.get("listed", "page"),
            "archive_name": archive_name,
        }
        report_date = max(label_dates) if label_dates else ""
        summary = label or "Document"

        if kind == "notice":
            notice = parse_notice(text) if text else parse_notice("")
            categories.update({
                "allegation": notice["allegation"],
                "allegation_date": notice["allegation_date"],
                "received_date": notice["received_date"],
                "finding": notice["finding"],
                "non_compliance": notice["non_compliance"],
                "rules": notice["rules"],
            })
            # The notice is signed by hand, so its own date is not in the OCR
            # text: the label carries it, beside the allegation date. The
            # notice date is the later of the two.
            self.stats["notice"] += 1
            if notice["finding"]:
                self.findings[notice["finding"]] += 1
            if not notice["allegation"] or not notice["finding"]:
                missing = [w for w, v in (("allegation", notice["allegation"]), ("finding", notice["finding"])) if not v]
                self.unparsed.append(f"{facility} | {label or file_id} | {file_id} | no {' or '.join(missing)}"
                                     + ("" if text else " (no readable text)"))
            summary = "Notice of non-compliance" + (f": {notice['allegation'][:220]}" if notice["allegation"] else "")
        elif kind == "visit":
            self.stats["visit"] += 1
            summary = VISIT_SUMMARY
            if extracted.get("reason_ink"):
                self.reason_ink.append(extracted["reason_ink"])
        else:
            self.stats["other"] += 1
            if not label:
                summary = "Document (the state's link gives no title)"

        hits = privacy_hits(text) if text and file_id not in OWNER_RELEASED else []
        if hits:
            self.stats["held_privacy"] += 1
            self.held.append(f"{facility} | {label or file_id} | {file_id} | {', '.join(hits)}")
            logger.warning(f"  {archive_name} held back ({', '.join(hits)}); not posted, archive copy moved")
            self.hold(archive_name)
            return None

        self.unhold(archive_name)
        if not report_date:
            self.undated.append(f"{facility} | {label or '(no label)'} | {file_id}")

        return {
            "report_id": file_id,
            "report_date": report_date,
            "report_url": DRIVE_VIEW.format(id=file_id),
            "raw_content": text,
            "content_length": len(text),
            "summary": summary,
            "categories": categories,
            "is_flagged": kind == "notice" and bool(categories.get("non_compliance")),
            "_md5": extracted.get("md5", ""),
        }

    def is_copy(self, report: Dict, kept: List[Dict]) -> Optional[str]:
        """The id of the report this one repeats (a folder copy of a page
        document), or None."""
        cats = report["categories"]
        for other in kept:
            if report["_md5"] and report["_md5"] == other["_md5"]:
                return other["report_id"]
        if cats.get("listed") != "folder":
            return None
        for other in kept:
            theirs = other["categories"]
            if theirs["kind"] != cats["kind"] or not report["report_date"] or other["report_date"] != report["report_date"]:
                continue
            if cats["kind"] in ("visit", "other"):
                return other["report_id"]
            if cats["kind"] == "notice":
                a, b = cats.get("allegation", ""), theirs.get("allegation", "")
                # A folder copy whose scan lost the allegation cannot be told
                # from the page document of the same date; two with text must match.
                if not a or not b or difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio() >= 0.85:
                    return other["report_id"]
        return None

    def scrape_dfs(self, seen: Dict[str, Set[str]], registry: Dict[str, Dict], limit: int = 0,
                   only: Optional[List[str]] = None, folders: bool = True) -> Tuple[List[Dict], Dict[str, List[str]]]:
        listed = parse_dfs_page(self.fetch.dfs_page())
        logger.info(f"Family Services page: {len(listed)} providers, "
                    f"{sum(len(f['docs']) for f in listed)} file links, "
                    f"{sum(len(f['folders']) for f in listed)} folder links")
        today = datetime.now().strftime("%Y-%m-%d")

        by_program: Dict[str, Dict] = {}
        order: List[str] = []
        for item in listed:
            name, note = clean_facility_name(item["raw_name"])
            ids = {d["id"] for d in item["docs"]}
            # A renamed provider keeps its program_name: found again by a
            # shared Drive id, else by name.
            # (Not one already taken by an accordion earlier on this page: two
            # providers can link the same document.)
            program = next((p for p, e in registry.items() if p not in order and ids & set(e.get("files", {}))), None) \
                or next((p for p, e in registry.items() if p not in order and e.get("name") == name), None) \
                or f"DFS-{slugify(name)}"
            entry = registry.setdefault(program, {"files": {}, "folders": []})
            entry.update({"name": name, "note": note, "last_listed": today})
            for folder in item["folders"]:
                if folder not in entry["folders"]:
                    entry["folders"].append(folder)
            for doc in item["docs"]:
                known = entry["files"].setdefault(doc["id"], {"first_seen": today, "listed": "page"})
                known.update({"label": doc["label"] or known.get("label", ""), "listed": "page", "last_listed": today})
            by_program[program] = entry
            if program not in order:   # one provider listed under two accordions: one record
                order.append(program)
        # Providers the page no longer lists: their documents are still ours.
        for program, entry in sorted(registry.items()):
            if program not in by_program:
                by_program[program] = entry
                order.append(program)

        if only:
            wanted = [w.lower() for w in only]
            order = [p for p in order if any(w in by_program[p]["name"].lower() or w == p.lower() for w in wanted)]
        if limit:
            order = order[:limit]

        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        for index, program in enumerate(order, start=1):
            entry = by_program[program]
            is_listed = entry.get("last_listed") == today
            name = entry["name"]
            logger.info(f"[{index}/{len(order)}] {name} ({program})" + ("" if is_listed else " [no longer listed]"))

            if folders and is_listed:
                pending = list(entry["folders"])
                visited: Set[str] = set()
                while pending:
                    folder = pending.pop(0)
                    if folder in visited or len(visited) >= 12:
                        continue
                    visited.add(folder)
                    try:
                        files, subfolders = parse_drive_folder(self.fetch.drive_folder(folder))
                    except requests.RequestException as exc:
                        logger.warning(f"  folder {folder} could not be listed: {exc}")
                        self.stats["folders_failed"] += 1
                        continue
                    self.stats["folders_listed"] += 1
                    pending += subfolders
                    for file in files:
                        if file["id"] not in entry["files"]:
                            entry["files"][file["id"]] = {
                                "first_seen": today, "listed": "folder", "label": file["label"], "last_listed": today}
                            self.stats["folder_only_files"] += 1
                        else:
                            entry["files"][file["id"]]["last_listed"] = today

            already = seen.get(program, set())
            copies = entry.setdefault("copies", {})
            kept: List[Dict] = []
            fresh: List[Dict] = []
            # Page documents first, so a folder copy is the one set aside.
            docs = sorted(entry["files"].items(), key=lambda kv: kv[1].get("listed") != "page")
            self.stats["documents_listed"] += len(docs)
            for file_id, meta in docs:
                doc = {"id": file_id, "label": meta.get("label", ""), "listed": meta.get("listed", "page")}
                archive_name = f"{file_id}.pdf"
                if meta.get("unavailable") and not self.retry_unavailable:
                    self.unavailable.append(f"{name} | {doc['label'] or file_id} | {file_id}")
                    continue
                if file_id in already or file_id in copies:
                    cached = self.reports.cached_extract(archive_name)
                    if cached and file_id in already:
                        earlier = self.dfs_report_quiet(name, doc, cached)
                        if earlier:
                            kept.append(earlier)
                    continue
                extracted = extract_with_cache(
                    self.reports, archive_name,
                    fetch=lambda file_id=file_id, archive_name=archive_name: self.pdf_bytes(archive_name, lambda: self.fetch.drive_pdf(file_id)),
                    extract=extract_dfs,
                )
                if not extracted and file_id in self.fetch.signin:
                    meta["unavailable"] = today
                    self.unavailable.append(f"{name} | {doc['label'] or file_id} | {file_id}")
                    continue
                if not extracted or not extracted.get("text"):
                    self.stats["failed"] += 1
                    self.failed.append(f"{name} | {doc['label'] or file_id} | {file_id}")
                    continue
                meta.pop("unavailable", None)
                report = self.dfs_report(name, doc, extracted)
                if not report:
                    continue
                original = self.is_copy(report, kept)
                if original:
                    copies[file_id] = original
                    self.stats["copies"] += 1
                    self.stats[report["categories"]["kind"]] -= 1
                    self.duplicates.append(f"{name} | {doc['label'] or file_id} | {file_id} = {original}")
                    self._forget(name, file_id)
                    continue
                kept.append(report)
                fresh.append(report)

            if not fresh and not (is_listed and not already and not entry["files"]):
                continue
            fresh.sort(key=lambda r: r["report_date"], reverse=True)
            status = "Certified provider on the state's list"
            if entry.get("note"):
                status = entry["note"]
            if not is_listed:
                status = f"No longer on the state's list (last listed {entry.get('last_listed', 'unknown')})"
            facilities.append({
                "facility_info": {
                    "facility_name": name,
                    "program_name": program,
                    "program_category": dfs_category(name),
                    "full_address": "",
                    "phone": "",
                    "bed_capacity": "",
                    "executive_director": "",
                    "license_exp_date": "",
                    "relicense_visit_date": "",
                    "action": status,
                },
                "reports": fresh,
            })
            if fresh:
                new_ids[program] = [r["report_id"] for r in fresh]
        return facilities, new_ids

    def dfs_report_quiet(self, facility: str, doc: Dict, extracted: Dict) -> Optional[Dict]:
        """A report already posted, rebuilt only so copies can be told from it."""
        saved = (Counter(self.stats), Counter(self.findings), list(self.unparsed), list(self.undated),
                 list(self.held), list(self.reason_ink))
        try:
            text = extracted.get("text", "") if extracted.get("has_text") else ""
            label = tidy_label(doc.get("label", ""))
            kind = {"SCL-305": "notice", "SCL-300": "visit"}.get(extracted.get("form", "")) or label_kind(label) or "other"
            dates = all_dates(label)
            cats = {"kind": kind, "listed": doc.get("listed", "page")}
            if kind == "notice" and text:
                cats["allegation"] = parse_notice(text)["allegation"]
            return {"report_id": doc["id"], "report_date": max(dates) if dates else "",
                    "categories": cats, "_md5": extracted.get("md5", "")}
        finally:
            self.stats, self.findings, self.unparsed, self.undated, self.held, self.reason_ink = saved

    def _forget(self, facility: str, file_id: str) -> None:
        """Take a set-aside copy out of the lists the run report prints."""
        for bucket in (self.unparsed, self.undated):
            bucket[:] = [line for line in bucket if f"| {file_id}" not in line]

    # -- Health --

    def scrape_wdh(self, seen: Dict[str, Set[str]], registry: Dict[str, Dict], limit: int = 0,
                   only: Optional[List[str]] = None, types: Optional[List[int]] = None,
                   ) -> Tuple[List[Dict], Dict[str, List[str]]]:
        types = types or [WDH_PRTF_TYPE]
        entries = [e for e in self.fetch.wdh_search(WDH_FACILITIES, {}) if e.get("FacilityTypeId") in types]
        logger.info(f"Health department: {len(entries)} facilities of type {types}")
        today = datetime.now().strftime("%Y-%m-%d")
        for e in entries:
            program = f"WDH-{e['FacilityId']}"
            registry[program] = {
                "FacilityId": e["FacilityId"], "FacilityName": one_line(e.get("FacilityName") or ""),
                "FacilityType": e.get("FacilityType") or "", "AddressLine1": e.get("AddressLine1") or "",
                "City": e.get("City") or "", "County": e.get("County") or "", "Phone": e.get("Phone") or "",
                "OpenClosed": e.get("OpenClosed") or "", "last_listed": today,
            }
        records = sorted(registry.values(), key=lambda r: r.get("FacilityName", "").lower())
        if only:
            wanted = [w.lower() for w in only]
            records = [r for r in records
                       if any(w in r["FacilityName"].lower() or w == f"wdh-{r['FacilityId']}" for w in wanted)]
        if limit:
            records = records[:limit]
        if not records:
            return [], {}

        surveys = self.fetch.wdh_search(WDH_SURVEYS, {
            "facilityId": [r["FacilityId"] for r in records],
            "sortBy": [{"key": "SurveyDate", "order": "desc"}],
        })
        by_facility: Dict[int, List[Dict]] = {}
        for survey in surveys:
            by_facility.setdefault(survey.get("FacilityId"), []).append(survey)
        self.stats["surveys_listed"] += len(surveys)

        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        for index, record in enumerate(records, start=1):
            program = f"WDH-{record['FacilityId']}"
            logger.info(f"[{index}/{len(records)}] {record['FacilityName']} ({program})")
            already = seen.get(program, set())
            reports: List[Dict] = []
            for survey in by_facility.get(record["FacilityId"], []):
                report_id = f"wdh-{survey['SurveyId']}"
                if report_id in already:
                    continue
                report = self.wdh_report(record, survey)
                if report:
                    reports.append(report)
            if not reports:
                continue
            reports.sort(key=lambda r: r["report_date"], reverse=True)
            address = one_line(record.get("AddressLine1", ""))
            city = one_line(record.get("City", ""))
            status = {"OPEN": "Open", "CLOSED": "Closed"}.get((record.get("OpenClosed") or "").upper(), "")
            if record.get("last_listed") != today:
                status = f"No longer listed by the state (last listed {record.get('last_listed', 'unknown')})"
            facilities.append({
                "facility_info": {
                    "facility_name": record["FacilityName"],
                    "program_name": program,
                    "program_category": "Psychiatric residential treatment facility",
                    "full_address": ", ".join(p for p in (address, f"{city}, WY" if city else "") if p),
                    "phone": format_phone(record.get("Phone", "")),
                    "bed_capacity": "",
                    "executive_director": "",
                    "license_exp_date": "",
                    "relicense_visit_date": "",
                    "action": status,
                },
                "reports": reports,
            })
            new_ids[program] = [r["report_id"] for r in reports]
        return facilities, new_ids

    def wdh_report(self, record: Dict, survey: Dict) -> Optional[Dict]:
        survey_id = survey["SurveyId"]
        archive_name = f"wdh-{survey_id}.pdf"
        extracted = extract_with_cache(
            self.reports, archive_name,
            fetch=lambda: self.pdf_bytes(archive_name, lambda: self.fetch.wdh_pdf(survey_id)),
            extract=extract_wdh,
        )
        if not extracted or not extracted.get("text"):
            self.stats["failed"] += 1
            self.failed.append(f"{record['FacilityName']} | survey {survey_id}")
            return None
        text = extracted["text"]
        tags = extracted.get("tags") or []
        hits = privacy_hits(text)
        if hits:
            self.stats["held_privacy"] += 1
            self.held.append(f"{record['FacilityName']} | survey {survey_id} | {', '.join(hits)}")
            logger.warning(f"  {archive_name} held back ({', '.join(hits)}); not posted, archive copy moved")
            self.hold(archive_name)
            return None
        self.unhold(archive_name)
        survey_type = one_line(survey.get("DocumentType") or "") or "Survey"
        revisit = bool(survey.get("IsRevisit"))
        self.stats["survey"] += 1
        count = len(tags)
        says_clean = bool(re.search(
            r"no\s+deficienc\w*\s+(?:were\s+)?(?:identified|cited|found)|in\s+compliance\s+with|deficiency\s+free", text, re.I))
        says_cited = bool(NOT_MET.search(text))
        if count:
            outcome = f"{count} deficienc{'y' if count == 1 else 'ies'} cited"
        elif says_cited:
            outcome = "deficiencies cited (tags not read from the scan; open the document)"
        elif says_clean:
            outcome = "no deficiencies cited"
        else:
            outcome = "findings not read from the scan (open the document)"
            self.unparsed.append(f"{record['FacilityName']} | survey {survey_id} | no tags and no 'no deficiencies' statement")
        summary = f"{survey_type}{' (revisit)' if revisit else ''}: {outcome}"
        return {
            "report_id": f"wdh-{survey_id}",
            "report_date": (survey.get("SurveyDate") or "")[:10],
            "report_url": WDH_DOWNLOAD.format(id=survey_id),
            "raw_content": text,
            "content_length": len(text),
            "summary": summary,
            "categories": {
                "source": "WDH",
                "kind": "survey",
                "survey_type": survey_type,
                "is_revisit": revisit,
                "initial_comments": extracted.get("initial_comments", ""),
                "plan_text": extracted.get("plan_text", ""),
                "complaint_intakes": extracted.get("complaint_intakes") or [],
                "tags": tags,
                "tag_count": count,
                "outcome": "cited" if (count or says_cited) else ("clean" if says_clean else "unread"),
                "ocr": bool(extracted.get("ocr")),
                "pages": extracted.get("pages", 0),
                "archive_name": archive_name,
            },
            "is_flagged": count > 0 or says_cited,
            "_md5": "",
        }

    # -- Report --

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        kinds = Counter(r["categories"]["kind"] for r in reports)
        logger.info("-- Wyoming run summary --")
        logger.info(f"facilities in the payload: {len(facilities)} "
                    f"({sum(1 for f in facilities if not f['reports'])} with no documents)")
        logger.info(f"reports: {len(reports)} (flagged: {sum(1 for r in reports if r['is_flagged'])})")
        for kind in ("notice", "visit", "other", "survey"):
            logger.info(f"  {kind}: {kinds.get(kind, 0)}")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        logger.info(f"documents listed (page and folders): {self.stats.get('documents_listed', 0)}; "
                    f"folders listed: {self.stats.get('folders_listed', 0)} "
                    f"(failed: {self.stats.get('folders_failed', 0)}); "
                    f"files only in a folder: {self.stats.get('folder_only_files', 0)}")
        logger.info(f"folder copies of documents already taken (not posted twice): {self.stats.get('copies', 0)}")
        logger.info(f"health surveys listed: {self.stats.get('surveys_listed', 0)}")
        logger.info(f"requests made this run: {dict(self.fetch.counts)}")
        notices = [r for r in reports if r["categories"]["kind"] == "notice"]
        logger.info(f"notices parsed: {len(notices)}; with allegation and finding: "
                    f"{sum(1 for r in notices if r['categories'].get('allegation') and r['categories'].get('finding'))}; "
                    f"with rules: {sum(1 for r in notices if r['categories'].get('rules'))}")
        logger.info(f"documents where the allegation or the finding could not be found: {len(self.unparsed)}")
        self.unparsed = list(dict.fromkeys(self.unparsed))
        for line in self.unparsed:
            logger.info(f"  UNPARSED {line}")
        logger.info("finding sentences:")
        for value, count in self.findings.most_common():
            logger.info(f"  {count:4d}  {value}")
        logger.info(f"labels with no readable date (posted undated): {len(self.undated)}")
        for line in self.undated:
            logger.info(f"  UNDATED {line}")
        logger.info(f"held back by the privacy check (not posted): {len(self.held)}")
        for line in self.held:
            logger.info(f"  HELD {line}")
        logger.info(f"documents the state links but does not share (Drive asks for a sign-in; left out, retried only with --full): {len(self.unavailable)}")
        for line in self.unavailable:
            logger.info(f"  UNSHARED {line}")
        logger.info(f"documents that could not be downloaded or read (retried next run): {len(self.failed)}")
        for line in self.failed:
            logger.info(f"  FAILED {line}")
        if self.reason_ink:
            logger.info(f"visit forms with checkbox ink measured: {len(self.reason_ink)} (not used; see the plan)")


def strip_internal(facilities: List[Dict]) -> List[Dict]:
    """Drop fields that are only for this script before posting."""
    return [{
        "facility_info": facility["facility_info"],
        "reports": [{k: v for k, v in r.items() if k not in ("is_flagged", "_md5")} for r in facility["reports"]],
    } for facility in facilities]


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = [{
        "facility_info": facility["facility_info"],
        "reports": [{**report, "is_structured": True} for report in facility["reports"]],
    } for facility in strip_internal(facilities)]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "WY",
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
        state="WY",
        scraped_timestamp=timestamp,
        facilities=strip_internal(facilities),
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Wyoming youth residential provider findings")
    parser.add_argument("--source", choices=("dfs", "wdh", "all"), default="all",
                        help="dfs: Family Services notices and visits; wdh: Health department surveys")
    parser.add_argument("--full", action="store_true", help="Ignore the seen reports in the state files")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N providers of each source")
    parser.add_argument("--facility", action="append", default=[],
                        help="Only providers whose name contains this, or this program_name (repeatable)")
    parser.add_argument("--no-folders", action="store_true",
                        help="Family Services: do not list the Drive folders, page links only")
    parser.add_argument("--wdh-types", default=str(WDH_PRTF_TYPE),
                        help="Health department facility type ids, comma separated (default 16, PRTF)")
    parser.add_argument("--reparse", action="store_true",
                        help="Read archived PDFs before the sources (after clearing .report_extract_cache/wy_pdfs)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()

    timestamp = datetime.now().isoformat(timespec="seconds")
    scraper = WYScraper()
    scraper.reparse = args.reparse
    scraper.retry_unavailable = args.full
    logger.info(f"PDF archive folder: {scraper.reports.archive_dir}")
    facilities: List[Dict] = []
    pending: List[Tuple[Path, Dict, Dict[str, List[str]]]] = []

    if args.source in ("dfs", "all"):
        state = load_state(DFS_STATE_FILE)
        seen = {} if args.full else seen_from_state(state)
        found, new_ids = scraper.scrape_dfs(
            seen, state.setdefault("facilities", {}), limit=args.limit,
            only=args.facility or None, folders=not args.no_folders)
        # The registry (every provider and Drive id ever seen) is not tied to
        # a post: it only remembers what to ask for.
        save_state(DFS_STATE_FILE, state)
        facilities += found
        pending.append((DFS_STATE_FILE, state, new_ids))

    if args.source in ("wdh", "all"):
        state = load_state(WDH_STATE_FILE)
        seen = {} if args.full else seen_from_state(state)
        types = [int(t) for t in args.wdh_types.split(",") if t.strip().isdigit()]
        try:
            found, new_ids = scraper.scrape_wdh(
                seen, state.setdefault("facilities", {}), limit=args.limit,
                only=args.facility or None, types=types)
        except (requests.RequestException, ValueError) as exc:
            logger.error(f"Health department source failed: {exc}")
            found, new_ids = [], {}
        save_state(WDH_STATE_FILE, state)
        facilities += found
        pending.append((WDH_STATE_FILE, state, new_ids))

    scraper.print_stats(facilities)
    if args.out:
        write_out(args.out, facilities, timestamp)
    if not any(f["reports"] for f in facilities):
        logger.info("No new reports since last run")
        return
    if args.no_post:
        logger.info("Skipping API POST because --no-post was set; seen reports not advanced")
        return
    if save_to_api(facilities, timestamp):
        for path, state, new_ids in pending:
            merge_new_ids(state, new_ids)
            save_state(path, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- seen reports not advanced")


if __name__ == "__main__":
    main()
