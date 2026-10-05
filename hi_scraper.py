"""
Hawaii special treatment facility and therapeutic living program inspections.

Source: the Department of Health, Office of Health Care Assurance (OHCA),
State Licensing Section, "Healthcare Facilities Inspection Reports"
https://health.hawaii.gov/ohca/inspection-reports/

One static WordPress page holds a Ninja Tables table of every state-licensed
care home and facility (about 2,300 rows: adult care homes, foster family
homes, assisted living...). Columns: name, facility type, then one column of
report links per year from 2023 ("Not Required" when the facility was not yet
licensed). Only the types STF (special treatment facility) and TLP
(therapeutic living program) are read, and of those only the facilities that
hi_scope.json lists as "in": adult and youth programs share both licences, so
scope is a youth allowlist. A facility on the page that the scope file does
not list is reported at the end of the run and not scraped.

Each link is one statement of deficiencies and plan of correction (Hawaii
Administrative Rules chapter 11-98). The form: a cover page (facility name,
address, inspection date and kind, "NO DEFICIENCIES" in the table when nothing
was cited), then for each deficiency a PART 1 page (the rule, the findings and
how the facility corrected it) and a PART 2 page (the same rule and findings,
and the facility's plan to keep it from happening again), then a signature
page. The table has three columns (rules and findings, plan of correction,
completion date); each is read on its own, by crop, so the facility's answer
never interleaves with the state's findings:
  text PDFs  the column edges are the table's vertical rules (pdfplumber
             edges) between the printed headers;
  scans      most statements with deficiencies are scans of the signed form.
             The page is read once by Tesseract to find the headers, the
             vertical rules are found by dark-pixel columns between them, and
             each column is cropped and read on its own. A plan column read at
             low confidence is handwriting and is not transcribed.

The page's roster PDFs (State Licensing Section page) give each facility's
licence number, address, phone and licence expiry.

Every PDF is archived through ReportStore to the Drive folder `hi_pdfs/` and
its extraction cached locally; the page and rosters are saved to
.report_extract_cache/hi_pages/ on every fetch (`--cached` reads them back
without asking the state).

Only statements of deficiencies are posted; any other document is logged as
not_a_report. The text that would be posted is checked for a date of birth,
a named young person and a record number; a hit holds the report back, lists
it, and moves its archived copy to .report_extract_cache/hi_held/.
"""

import argparse
import difflib
import glob
import html as htmllib
import json
import logging
import os
import re
import shutil
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
import requests

from inspection_api_client import post_facilities_to_api
from report_store import EXTRACT_CACHE_ROOT, ReportStore, extract_with_cache
from scraper_state import load_state, merge_new_ids, save_state, seen_from_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("KOP_DATA_API_KEY", "CHANGE_ME")
STATE_FILE = Path(os.getenv("HI_STATE_FILE", ".hi_state.json"))
SCOPE_FILE = Path(__file__).parent / "hi_scope.json"
REPORTS = ReportStore("HI_PDF_CACHE", "hi_pdfs", Path(__file__).parent / "hi_pdfs")
PAGES_DIR = EXTRACT_CACHE_ROOT / "hi_pages"
HELD_DIR = EXTRACT_CACHE_ROOT / "hi_held"

SITE = "https://health.hawaii.gov"
INDEX_URL = SITE + "/ohca/inspection-reports/"
LICENSING_URL = SITE + "/ohca/state-licensing-section/"
ROSTER_LINK = re.compile(
    r'href="(https://health\.hawaii\.gov/ohca/files/[^"]*?(Special-Treatment-Facilit|Therapeutic-Living-Program)[^"]*?\.pdf)"',
    re.I)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
REQUEST_GAP = 0.8
TYPES = ("STF", "TLP")
TYPE_NAMES = {"STF": "Special treatment facility", "TLP": "Therapeutic living program"}

OCR_DPI = int(os.getenv("HI_OCR_DPI", "250"))
# A plan column whose answer words (the printed prompts left out) average
# below this OCR confidence is handwriting. Typed answers read at 85 to 95.
TYPED_CONFIDENCE = float(os.getenv("HI_TYPED_CONFIDENCE", "75"))
HANDWRITTEN = "(handwritten; not transcribed, open the document to read it)"
EXTRACT_VERSION = 1


# ── Fetch layer ──────────────────────────────────────────────────────────────


class Fetcher:
    def __init__(self, cached: bool = False) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.cached = cached
        self._last = 0.0
        self.count = 0

    def get(self, url: str) -> requests.Response:
        delay = 3.0
        for attempt in range(1, 5):
            wait = REQUEST_GAP - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            self.count += 1
            try:
                response = self.session.get(url, timeout=120)
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

    def saved(self, name: str, url: str, binary: bool = False) -> bytes:
        """The page or roster: fetched and saved, or (with --cached) the
        newest saved copy."""
        PAGES_DIR.mkdir(parents=True, exist_ok=True)
        if self.cached:
            found = sorted(PAGES_DIR.glob(f"{name}-*"))
            if found:
                logger.info(f"  {name}: saved copy {found[-1].name}")
                return found[-1].read_bytes()
            logger.info(f"  {name}: no saved copy, fetching")
        response = self.get(url)
        response.raise_for_status()
        if binary and not response.content.startswith(b"%PDF"):
            raise ValueError(f"{url} is not a PDF")
        suffix = ".pdf" if binary else ".html"
        (PAGES_DIR / f"{name}-{datetime.now():%Y%m%d}{suffix}").write_bytes(response.content)
        return response.content

    def pdf(self, url: str) -> Optional[bytes]:
        try:
            response = self.get(url)
        except requests.RequestException as exc:
            logger.warning(f"  download failed: {exc}")
            return None
        if response.status_code != 200 or not response.content.startswith(b"%PDF"):
            logger.warning(f"  HTTP {response.status_code}, not a PDF: {url}")
            return None
        return response.content


# ── The index page ───────────────────────────────────────────────────────────

ROW_RE = re.compile(r'<tr\s+data-row_id="(\d+)"[^>]*>(.*?)</tr>', re.S)
CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
LINK_RE = re.compile(r'<a\b[^>]*\bhref="([^"]+)"[^>]*>(.*?)</a>', re.S | re.I)
HEAD_RE = re.compile(r"<thead.*?</thead>", re.S)


def strip_tags(value: str) -> str:
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", value or ""))).strip()


def parse_index(page: str) -> List[Dict[str, Any]]:
    """Rows of type STF or TLP: {row, name, type, links: [{url, label, column}]}."""
    head = HEAD_RE.search(page)
    columns: List[str] = []
    if head:
        columns = [strip_tags(c) for c in re.findall(r"<th[^>]*>(.*?)</th>", head.group(0), re.S)]
    years: List[str] = []
    for title in columns[2:]:
        m = re.search(r"(20\d\d)", title)
        years.append(m.group(1) if m else title)
    rows: List[Dict[str, Any]] = []
    for row_id, body in ROW_RE.findall(page):
        cells = CELL_RE.findall(body)
        if len(cells) < 3:
            continue
        kind = strip_tags(cells[1]).upper()
        if kind not in TYPES:
            continue
        links = []
        for i, cell in enumerate(cells[2:]):
            for href, label in LINK_RE.findall(cell):
                links.append({
                    "url": htmllib.unescape(href).strip(),
                    "label": strip_tags(label),
                    "column": years[i] if i < len(years) else "",
                })
        rows.append({"row": row_id, "name": strip_tags(cells[0]), "type": kind, "links": links})
    return rows


# ── Rosters (licence numbers, addresses) ─────────────────────────────────────

ROSTER_HEADS = ["FACILITY", "ADDRESS", "CITY", "ZIP", "ISLAND", "LIC", "LIC", "PHONE"]
ROSTER_KEYS = ["name", "street", "city", "zip", "island", "expires", "licence", "phone"]


def parse_roster(data: bytes) -> List[Dict[str, str]]:
    """The roster table, read by column position from its header words."""
    out: List[Dict[str, str]] = []
    import io
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(keep_blank_chars=False, use_text_flow=False)
            header = [w for w in words if w["text"].upper() in ("FACILITY", "ADDRESS", "CITY", "ZIP", "ISLAND", "LIC", "PHONE")]
            anchor = next((w for w in header if w["text"].upper() == "ADDRESS"), None)
            if not anchor:
                continue
            # The title above ("Special Treatment Facility") is not the header row.
            top = anchor["top"]
            header = [w for w in header if abs(w["top"] - top) < 4]
            header.sort(key=lambda w: w["x0"])
            if [w["text"].upper() for w in header] != ROSTER_HEADS:
                logger.warning(f"  roster header not as expected: {[w['text'] for w in header]}")
                continue
            edges = [w["x0"] - 3 for w in header] + [page.width + 1]
            lines: Dict[int, List[Dict]] = {}
            for w in words:
                if w["top"] <= top + 4:
                    continue
                lines.setdefault(round(w["top"] / 3), []).append(w)
            for key in sorted(lines):
                cells = [""] * len(ROSTER_KEYS)
                for w in sorted(lines[key], key=lambda w: w["x0"]):
                    for i in range(len(ROSTER_KEYS)):
                        if edges[i] <= w["x0"] < edges[i + 1]:
                            cells[i] = (cells[i] + " " + w["text"]).strip()
                            break
                row = dict(zip(ROSTER_KEYS, cells))
                if re.match(r"^\d+-(STF|TLP)$", row["licence"]):
                    out.append(row)
    return out


def format_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}" if len(digits) == 10 else (value or "")


def us_to_iso(value: str) -> str:
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$", value or "")
    return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}" if m else ""


# ── Dates ────────────────────────────────────────────────────────────────────

MONTHS = {m: i for i, m in enumerate(
    "january february march april may june july august september october november december".split(), 1)}
LONG_DATE = re.compile(r"\b([A-Z][a-z]{2,8})\.?\s+(\d{1,2})\s*[,.]?\s*(\d{4})\b")


def label_date(label: str) -> str:
    """'08.22.24 Initial' / '1.28.25' -> ISO; '' when the label has no date."""
    m = re.search(r"\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b", label or "")
    if not m:
        return ""
    month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if year < 100:
        year += 2000
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def long_date(value: str) -> str:
    m = LONG_DATE.search(value or "")
    if not m:
        return ""
    month = MONTHS.get(m.group(1).lower()) or next(
        (n for name, n in MONTHS.items() if name.startswith(m.group(1).lower()[:3])), 0)
    try:
        return datetime(int(m.group(3)), month, int(m.group(2))).strftime("%Y-%m-%d") if month else ""
    except ValueError:
        return ""


def short_dates(value: str) -> List[str]:
    found = []
    for m in re.finditer(r"\b(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{2,4})\b", value or ""):
        year = int(m.group(3))
        year += 2000 if year < 100 else 0
        try:
            found.append(datetime(year, int(m.group(1)), int(m.group(2))).strftime("%Y-%m-%d"))
        except ValueError:
            continue
    return found


# ── Reading a page: three columns ────────────────────────────────────────────


def _find_tesseract() -> str:
    if os.getenv("TESSERACT_CMD"):
        return os.getenv("TESSERACT_CMD")
    default = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    return str(default) if default.exists() else ""


def _find_poppler() -> str:
    if os.getenv("POPPLER_PATH"):
        return os.getenv("POPPLER_PATH")
    found = sorted(glob.glob("C:/tools/poppler-*/Library/bin"), reverse=True)
    return found[0] if found else ""


def _pytesseract():
    import pytesseract
    tesseract = _find_tesseract()
    if tesseract:
        pytesseract.pytesseract.tesseract_cmd = tesseract
    return pytesseract


def _headers(words: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    """Positions of the table's printed headers in a list of words
    ({text, x0, x1, top, bottom}): RULES (CRITERIA) | PLAN OF CORRECTION |
    Completion Date. None when the page has no such table (cover, signature)."""
    def find(pattern: str, after: float = -1.0, near: Optional[float] = None) -> Optional[Dict]:
        for w in words:
            if re.fullmatch(pattern, w["text"].strip(), re.I) and w["x0"] > after:
                if near is None or abs(w["top"] - near) < 40:
                    return w
        return None
    rules = find(r"[|\\(\[]*R\s?ULES")
    if rules:
        plan = find(r"PLAN(?:OF)?", rules["x1"], rules["top"])
    else:
        # A header OCR did not read: PLAN OF CORRECTION, side by side.
        plan = next((w for w in words if re.fullmatch(r"PLAN(?:OF)?", w["text"].strip())
                     and any(re.fullmatch(r"CORRECTION", v["text"].strip()) and abs(v["top"] - w["top"]) < 30
                             and 0 < v["x0"] - w["x1"] < 200 for v in words)), None)
    if not plan:
        return None
    anchor = rules or plan
    criteria = find(r"\(?CRITERIA\)?", rules["x0"] - 1, rules["top"]) if rules else None
    correction = find(r"CORRECTION", plan["x0"] - 1, anchor["top"])
    # "Completion Date": OCR often garbles one of the two words.
    completion = find(r"[|]*Com\w{4,9}[|]*", (correction or plan)["x1"], anchor["top"])
    date_word = find(r"[|_]*Date[|]*", (correction or plan)["x1"], anchor["top"] + 25)
    if completion:
        date_x0 = completion["x0"]
    elif date_word:
        date_x0 = date_word["x0"]
    else:
        return None
    bottom = max(w["bottom"] for w in (rules, plan, completion, date_word) if w)
    return {
        "rules_x0": rules["x0"] if rules else None,
        "rules_x1": (criteria or rules)["x1"] if rules else None,
        "plan_x0": plan["x0"],
        "plan_x1": (correction or plan)["x1"],
        "date_x0": date_x0,
        "top": min(anchor["top"], plan["top"]),
        "bottom": bottom,
    }


def _edge_between(xs: List[float], low: float, high: float, prefer: str) -> Optional[float]:
    inside = [x for x in xs if low <= x <= high]
    if not inside:
        return None
    return max(inside) if prefer == "right" else min(inside)


def read_text_page(page) -> Dict[str, Any]:
    words = page.extract_words()
    full = page.extract_text() or ""
    heads = _headers(words)
    result: Dict[str, Any] = {"ocr": False, "full": full, "columns": None}
    if not heads or heads.get("rules_x0") is None:
        return result
    verticals = sorted({round(e["x0"], 1) for e in page.edges
                        if e.get("orientation") == "v" and (e["bottom"] - e["top"]) > 40})
    b0 = _edge_between(verticals, 0, heads["rules_x0"] - 1, "right") or 0.0
    b1 = _edge_between(verticals, heads["rules_x1"], heads["plan_x0"], "left") or (heads["rules_x1"] + heads["plan_x0"]) / 2
    b2 = _edge_between(verticals, heads["plan_x1"], heads["date_x0"], "right") or (heads["plan_x1"] + heads["date_x0"]) / 2
    top = heads["bottom"] + 1
    cols = {}
    for key, (x0, x1) in (("rules", (b0, b1)), ("plan", (b1, b2)), ("date", (b2, page.width))):
        crop = page.crop((x0 + 0.5, top, min(x1 - 0.5, page.width), page.height))
        cols[key] = crop.extract_text() or ""
    result["columns"] = cols
    return result


def _dark_columns(image, top: int, bottom: int, window: int = 14):
    """For each x, the share of rows in the band with a dark pixel within
    `window` pixels to its right. A table rule on a skewed scan wanders over
    several pixel columns; the window keeps it whole."""
    import numpy as np
    gray = np.asarray(image.convert("L"))
    band = (gray[max(0, top):max(top + 1, bottom), :] < 140).astype(np.int32)
    sums = np.cumsum(np.pad(band, ((0, 0), (1, 0))), axis=1)
    width = band.shape[1]
    start = np.arange(width)
    end = np.minimum(start + window, width)
    return ((sums[:, end] - sums[:, start]) > 0).mean(axis=0)


def _line_between(profile, low: int, high: int, prefer: str) -> Optional[int]:
    low, high = max(0, int(low)), min(len(profile) - 1, int(high))
    if high <= low:
        return None
    window = profile[low:high + 1]
    peak = float(window.max())
    if peak < 0.35:
        return None
    hits = [low + i for i, v in enumerate(window) if v >= peak * 0.6]
    # Runs of neighbouring hits are one rule; its middle is the rule.
    runs: List[List[int]] = []
    for x in hits:
        if runs and x - runs[-1][-1] <= 2:
            runs[-1].append(x)
        else:
            runs.append([x])
    run = runs[-1] if prefer == "right" else runs[0]
    return (run[0] + run[-1]) // 2


def _ocr_words(image, psm: int = 3) -> Tuple[List[Dict[str, Any]], str, float]:
    pt = _pytesseract()
    data = pt.image_to_data(image, config=f"--psm {psm}", output_type=pt.Output.DICT)
    words, lines, order, confs = [], {}, [], []
    for i, text in enumerate(data["text"]):
        if not text.strip():
            continue
        try:
            conf = float(data["conf"][i])
        except (TypeError, ValueError):
            conf = -1.0
        if conf < 0:
            continue
        words.append({"text": text, "x0": data["left"][i], "x1": data["left"][i] + data["width"][i],
                      "top": data["top"][i], "bottom": data["top"][i] + data["height"][i], "conf": conf})
        key = (data["block_num"][i], data["par_num"][i], data["line_num"][i])
        if key not in lines:
            lines[key] = []
            order.append(key)
        lines[key].append(text)
        confs.append(conf)
    out, previous = [], None
    for key in order:
        if previous is not None and key[:2] != previous[:2]:
            out.append("")
        out.append(" ".join(lines[key]))
        previous = key
    return words, "\n".join(out).strip(), (sum(confs) / len(confs) if confs else 0.0)


# The form's printed prompts in the plan column. They are typed, so they
# read with high confidence even when the facility's answer is handwritten;
# the answer's confidence is measured without them.
PROMPT_WORDS = set(
    "PART 1 2 I II DID YOU CORRECT THE DEFICIENCY USE THIS SPACE TO TELL US HOW CORRECTED "
    "FUTURE PLAN EXPLAIN YOUR WHAT WILL DO ENSURE THAT IT DOESN'T DOESN’T HAPPEN AGAIN".split())


def answer_confidence(words: List[Dict[str, Any]]) -> Tuple[float, int]:
    answer = [w for w in words if w["text"].strip(".,:;?|") not in PROMPT_WORDS]
    if not answer:
        return 100.0, 0
    return sum(w["conf"] for w in answer) / len(answer), len(answer)


def read_scan_page(image) -> Dict[str, Any]:
    words, full, conf = _ocr_words(image)
    result: Dict[str, Any] = {"ocr": True, "full": full, "confidence": round(conf, 1), "columns": None}
    heads = _headers(words)
    if not heads:
        return result
    width, height = image.size
    profile = _dark_columns(image, int(heads["bottom"]) + 10, height - int(height * 0.08))
    if heads.get("rules_x0") is None:
        # OCR missed "RULES (CRITERIA)": the rules column ends at the strong
        # rule left of "PLAN".
        right = _line_between(profile, heads["plan_x0"] - width * 0.25, heads["plan_x0"] - 3, "right")
        if right is None:
            return result
        heads["rules_x1"] = right - 3
        heads["rules_x0"] = right - width * 0.2
    # Rule positions are the left edge of a 14 px window; +7 is its middle.
    b0 = _line_between(profile, heads["rules_x0"] - width * 0.15, heads["rules_x0"] - 20, "right")
    b0 = b0 + 7 if b0 is not None else 0
    b1 = _line_between(profile, heads["rules_x1"] + 3, heads["plan_x0"] - 3, "left")
    b2 = _line_between(profile, heads["plan_x1"] + 3, heads["date_x0"] - 2, "right")
    b1 = b1 + 7 if b1 is not None else None
    b2 = b2 + 7 if b2 is not None else None
    b1 = b1 if b1 is not None else int((heads["rules_x1"] + heads["plan_x0"]) / 2)
    b2 = b2 if b2 is not None else int((heads["plan_x1"] + heads["date_x0"]) / 2)
    top = int(heads["bottom"]) + 8
    cols, confs = {}, {}
    for key, (x0, x1) in (("rules", (b0, b1)), ("plan", (b1, b2)), ("date", (b2, width))):
        crop = image.crop((x0 + 3, top, max(x0 + 4, x1 - 3), height))
        found, text, c = _ocr_words(crop, psm=4 if key != "date" else 6)
        cols[key], confs[key] = text, round(c, 1)
        if key == "plan":
            answer, count = answer_confidence(found)
            result["answer_confidence"] = round(answer, 1)
            result["answer_words"] = count
    result["columns"] = cols
    result["column_confidence"] = confs
    result["edges"] = [b0, b1, b2]
    return result


def extract_pdf(path: Path) -> Dict[str, Any]:
    pages: List[Optional[Dict[str, Any]]] = []
    scans: List[int] = []
    with pdfplumber.open(str(path)) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            if len(text.strip()) >= 40:
                pages.append(read_text_page(page))
            else:
                pages.append(None)
                scans.append(i)
    if scans:
        from pdf2image import convert_from_path
        for i in scans:
            problem = None
            for dpi in (OCR_DPI, 200):
                try:
                    images = convert_from_path(str(path), dpi=dpi, first_page=i + 1, last_page=i + 1,
                                               poppler_path=_find_poppler() or None)
                    pages[i] = read_scan_page(images[0])
                    problem = None
                    break
                except Exception as exc:  # Tesseract out of memory, a broken page, no OCR install
                    problem = exc
                    logger.warning(f"  page {i + 1}: OCR failed at {dpi} dpi: {str(exc)[:120]}")
                    time.sleep(2)
            if problem is not None:
                # Nothing is cached for an empty extraction: retried next run.
                return {"text": "", "pages": [], "error": str(problem)[:300]}
    full = "\n\n".join((p or {}).get("full", "") for p in pages)
    return {"version": EXTRACT_VERSION, "text": full, "pages": pages}


# ── Parsing the statement ────────────────────────────────────────────────────

RULE_CITE = re.compile(r"(?:^|[\s|\[(])[§$S5&]?\s?(11\s?[-–]\s?\d{2,3}\s?[-–]\s?\d{1,3}(?:\.\d+)?)\b")
FINDINGS = re.compile(r"^\W{0,3}[FE]\s?I\s?N\s?D\s?I\s?N\s?G\s?S?\b[:.]?", re.I | re.M)
PART = re.compile(r"\b[PF]ART\s*([12Il|]{1,2}|[12])\b", re.I)
BOILERPLATE = [
    r"[PF]ART\s*[12Il|]{1,2}\b",
    r"DID\s+YOU\s+CORRECT\s+THE\s+DEFICIENCY\??",
    r"USE\s+THIS\s+SPACE\s+TO\s+TELL\s+US\s+HOW\s+YOU",
    r"CORRECTED\s+THE\s+DEFICIENCY",
    r"F\s?UTURE\s+PLAN",
    r"USE\s+THIS\s+SPACE\s+TO\s+EXPLAIN\s+YOUR\s+FUTURE",
    r"PLAN\s*:\s*WHAT\s+WILL\s+YOU\s+DO\s+TO\s+ENSURE\s+THAT",
    r"IT\s+DOESN.?T\s+HAPPEN\s+AGAIN\??",
    r"\d{2}/\d{2}/\d{2},\s*Rev\b.*",
]
BOILERPLATE_RE = re.compile(r"(?:" + "|".join(BOILERPLATE) + r")", re.I)
NOT_PRACTICAL = re.compile(
    r"Correcting\s+the\s+deficiency\s+after.the.fact\s+is\s+not\s+practical\s*/?\s*appropriate\.?\s*"
    r"For\s+this\s+deficiency,?\s+only\s+a\s+future\s+plan\s+is\s+required\.?", re.I | re.S)
FORM_FOOTER = re.compile(r"(?:\d{2}/\d{2}/\d{2},?\s*)?Rev\s+\d{2}/\d{2}/\d{2}(?:,\s*\d{2}/\d{2}/\d{2})*")


def tidy(value: str) -> str:
    """Join a column's lines into paragraphs: blank lines split them, and
    short noise lines (a lone glyph from a checkbox or a scanner speck) go."""
    paragraphs, current = [], []
    for line in (value or "").split("\n"):
        line = line.strip().strip("|").strip()
        if not line:
            if current:
                paragraphs.append(" ".join(current))
                current = []
            continue
        if len(re.sub(r"[^A-Za-z0-9]", "", line)) < 3 and not re.match(r"^[-*•]", line):
            continue
        current.append(line)
    if current:
        paragraphs.append(" ".join(current))
    text = "\n".join(paragraphs)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def strip_plan(value: str) -> Tuple[str, Optional[int], bool]:
    """(plan text, part 1/2 or None, 'only a future plan is required')."""
    part = None
    m = PART.search(value or "")
    if m:
        part = 2 if re.fullmatch(r"2|II|Il|lI|ll|\|\|", m.group(1)) else 1
    not_practical = bool(NOT_PRACTICAL.search(re.sub(r"\s+", " ", value or "")))
    text = NOT_PRACTICAL.sub(" ", re.sub(r"[ \t]+", " ", value or ""))
    # The prompts are printed in capitals across several lines; drop them line by line.
    kept = []
    for line in text.split("\n"):
        if BOILERPLATE_RE.fullmatch(line.strip().strip("|-—_~ .").strip()):
            continue
        line = BOILERPLATE_RE.sub("", line)
        kept.append(line)
    text = "\n".join(kept)
    text = re.sub(r"(?im)^\s*(?:Correcting the deficiency|after-the-fact is not|practical/appropriate\. For|"
                  r"this deficiency, only a future|plan is required\.)\s*$", "", text)
    return tidy(text), part, not_practical


def split_rules(value: str) -> List[Dict[str, str]]:
    """A rules column -> [{rule, heading, rule_text, finding}] (normally one)."""
    text = FORM_FOOTER.sub("", value or "")
    starts = [m for m in RULE_CITE.finditer(text)]
    # A cite inside the findings ("see 11-98-12") is not a new deficiency: a
    # deficiency starts with a cite at the start of a line.
    starts = [m for m in starts if text.rfind("\n", 0, m.start(1)) >= text.rfind(".", 0, m.start(1)) - 1
              and re.match(r"^\W{0,6}$", text[text.rfind("\n", 0, m.start(1)) + 1:m.start(1)].replace("§", "").strip() or "")]
    blocks: List[Dict[str, str]] = []
    if not starts:
        found = FINDINGS.search(text)
        if found:
            return [{"rule": "", "heading": "", "rule_text": tidy(text[:found.start()]), "finding": tidy(text[found.end():])}]
        return [{"rule": "", "heading": "", "rule_text": "", "finding": tidy(text), "continued": "1"}] if tidy(text) else []
    for n, m in enumerate(starts):
        end = starts[n + 1].start() if n + 1 < len(starts) else len(text)
        line_start = text.rfind("\n", 0, m.start(1)) + 1
        chunk = text[line_start:end] if n else text[line_start:end]
        cite = re.sub(r"\s", "", m.group(1)).replace("–", "-")
        body = chunk[chunk.find(m.group(1)) + len(m.group(1)):]
        found = FINDINGS.search(body)
        before, finding = (body[:found.start()], body[found.end():]) if found else (body, "")
        first, _, rest = before.strip().partition("\n")
        blocks.append({
            "rule": "HAR §" + cite,
            "heading": tidy(first),
            "rule_text": tidy(rest),
            "finding": tidy(finding),
        })
    return blocks


def norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


def same_deficiency(a: Dict[str, Any], b: Dict[str, str]) -> bool:
    if a["rule"] and b["rule"] and norm(a["rule"]) != norm(b["rule"]):
        return False
    fa, fb = norm(a["finding"])[:300], norm(b["finding"])[:300]
    if not fa or not fb:
        return bool(a["rule"]) and a["rule"] == b["rule"] and not (fa and fb)
    return difflib.SequenceMatcher(None, fa, fb).ratio() >= 0.7


COVER_NAME = re.compile(r"F\w{5,8}.{0,3}s?\s*Name\s*[:;.]?\s*(.+?)\s+CHAPTER\s+(\d+)", re.I)
COVER_DATE = re.compile(r"Inspection\s+Date\s*[:;.]?\s*([A-Z][a-z]{2,8}\.?\s+\d{1,2}\s*[,.]?\s*\d{4})\s*([A-Za-z][A-Za-z /&-]{0,40})?", re.I)
COVER_ADDRESS = re.compile(r"Address\s*[:;.]?\s*(.*?)(?:\s+Inspection\s+Date|\n|$)", re.I)
ADDRESS_LINE = re.compile(r"^\s*\W{0,3}(\d[\d-]*\s+[A-Z][^\n]*?Hawaii\s+\d{5})", re.I | re.M)
NO_DEFICIENCIES = re.compile(r"\bNO\s+DEFICIENCIES\b", re.I)
STATEMENT = re.compile(r"STATEMENT\s+OF\s+DEFICIENCIES", re.I)


def parse_statement(extracted: Dict[str, Any]) -> Dict[str, Any]:
    pages = [p for p in extracted.get("pages") or [] if p]
    first = pages[0]["full"] if pages else ""
    out: Dict[str, Any] = {
        "is_statement": bool(STATEMENT.search(first)) or bool(
            re.search(r"Office\s+of\s+Health\s+Care\s+Assurance", first, re.I) and re.search(r"PLAN\s+OF\s+CORRECTION", first, re.I)),
        "facility": "", "chapter": "", "address": "", "inspection_date": "", "inspection_type": "",
        "no_deficiencies": False, "deficiencies": [], "ocr": any(p.get("ocr") for p in pages),
    }
    m = COVER_NAME.search(first)
    if m:
        out["facility"] = m.group(1).strip(" :;.|")
        out["chapter"] = m.group(2)
    m = COVER_DATE.search(first)
    if m:
        out["inspection_date"] = long_date(m.group(1))
        kind = (m.group(2) or "").strip()
        kind = re.split(r"\s{2,}|\s+[|\"']|\s+\d", kind)[0].strip(" -")
        out["inspection_type"] = kind.title() if re.match(r"(?i)^(annual|initial|complaint|follow|revisit|re-?inspection|change|special|relocation|unannounced)", kind) else ""
    m = ADDRESS_LINE.search(first) or COVER_ADDRESS.search(first)
    if m and re.match(r"^\W{0,2}\d", m.group(1)):
        out["address"] = re.sub(r"\s+", " ", m.group(1)).strip(" ,'\u2018\u2019")
    # "NO DEFICIENCIES" in the cover table (the scan's form or the text form).
    if any(NO_DEFICIENCIES.search(p["full"]) for p in pages[:2]):
        out["no_deficiencies"] = True

    deficiencies: List[Dict[str, Any]] = []
    previous: Optional[Dict[str, Any]] = None
    for number, page in enumerate(pages[1:], 2):
        cols = page.get("columns")
        if not cols:
            continue
        blocks = split_rules(cols.get("rules", ""))
        if blocks and not blocks[0]["rule"] and not blocks[0].get("continued"):
            # OCR lost the cite in the column crop; the whole-page read may have it.
            cite = RULE_CITE.search(page.get("full", ""))
            if cite:
                blocks[0]["rule"] = "HAR \u00a7" + re.sub(r"\s", "", cite.group(1)).replace("\u2013", "-")
        plan, part, not_practical = strip_plan(cols.get("plan", ""))
        handwritten = bool(page.get("ocr")) and page.get("answer_words", 0) >= 4 \
            and page.get("answer_confidence", 100) < TYPED_CONFIDENCE
        if handwritten:
            plan = HANDWRITTEN
        dates = short_dates(cols.get("date", ""))
        if not blocks:
            continue
        for block in blocks:
            if block.get("continued") and previous is not None:
                # The finding runs on from the page before.
                previous["finding"] = (previous["finding"] + "\n" + block["finding"]).strip()
                target = previous
            else:
                target = next((d for d in deficiencies if same_deficiency(d, block)), None)
                if target is None:
                    target = {"rule": block["rule"], "heading": block["heading"], "rule_text": block["rule_text"],
                              "finding": block["finding"], "correction": "", "future_plan": "",
                              "completion_dates": [], "only_future_plan": False, "pages": []}
                    deficiencies.append(target)
                elif len(block["finding"]) > len(target["finding"]) and not handwritten:
                    target["finding"] = block["finding"]
            target["pages"].append(number)
            if not_practical:
                target["only_future_plan"] = True
            if plan:
                # PART 1 answers "did you correct it", PART 2 is the future plan.
                # When OCR lost the word PART, a deficiency's second page is part 2.
                which = part or (2 if (target["correction"] or target["only_future_plan"]) else 1)
                field = "future_plan" if which == 2 else "correction"
                if not target[field]:
                    target[field] = plan
                elif plan != HANDWRITTEN and target[field] != HANDWRITTEN and plan not in target[field]:
                    target[field] = (target[field] + "\n" + plan).strip()
            for d in dates:
                if d not in target["completion_dates"]:
                    target["completion_dates"].append(d)
            previous = target
    # A block with no finding and no rule is a stray (a scanner speck read as text).
    out["deficiencies"] = [d for d in deficiencies if d["finding"] or d["rule"]]
    for d in out["deficiencies"]:
        d.pop("pages", None)
    return out


# ── Privacy ──────────────────────────────────────────────────────────────────

PRIVACY = [
    ("date of birth", re.compile(
        r"\bD\.?O\.?B\b\.?\s*[:#-]?\s*(?:\d|[A-Z][a-z]{2,8}\s+\d)"
        r"|\bdate\s+of\s+birth\s*(?:is|was|:)?\s*[:#-]?\s*(?:\d|[A-Z][a-z]{2,8}\s+\d)"
        r"|\bborn\s+(?:on\s+)?(?:\d|[A-Z][a-z]{2,8}\s+\d)", re.I)),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("record number", re.compile(
        r"\b(?:Medicaid|QUEST|medical\s+record|MRN|MR|client\s+ID|member\s+ID|case|record)\s*(?:ID|No\.?|number|#)\s*[:#]?\s*[A-Z]?\d{5,}\b",
        re.I)),
    ("named young person", re.compile(
        r"\b(?:[Cc]lient|[Rr]esident|[Yy]outh|[Cc]hild|[Mm]inor|[Pp]atient|[Ss]tudent|[Cc]onsumer)s?\s+"
        r"(?:named\s+|name\s+is\s+|identified\s+as\s+|[A-Z]\.\s?[A-Z]\.\s)[A-Z]?[a-z]*")),
]


def privacy_hits(text: str) -> List[str]:
    return [label for label, pattern in PRIVACY if pattern.search(text or "")]


# ── Scope ────────────────────────────────────────────────────────────────────


def load_scope() -> Dict[str, Dict[str, Any]]:
    data = json.loads(SCOPE_FILE.read_text(encoding="utf-8"))
    return {str(entry["row"]): entry for entry in data.get("facilities", [])}


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-")


def archive_name(url: str) -> str:
    """'.../ohca/files/2024/11/Bobby-Benson-Center-STF-7.25.24.pdf' ->
    '2024-11_Bobby-Benson-Center-STF-7.25.24.pdf' (unique: the state's own path)."""
    m = re.search(r"/files/(\d{4})/(\d{2})/([^/?#]+)$", url)
    name = f"{m.group(1)}-{m.group(2)}_{m.group(3)}" if m else url.rsplit("/", 1)[-1]
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)


# ── The scraper ──────────────────────────────────────────────────────────────


class HIScraper:
    def __init__(self, fetcher: Fetcher) -> None:
        self.fetch = fetcher
        self.reports = REPORTS
        self.stats: Counter = Counter()
        self.unlisted: List[str] = []
        self.unsure: List[str] = []
        self.renamed: List[str] = []
        self.not_a_report: List[str] = []
        self.held: List[str] = []
        self.failed: List[str] = []
        self.unread: List[str] = []
        self.date_mismatch: List[str] = []
        self.handwritten: List[str] = []
        self.dates: List[str] = []
        self.kinds: Counter = Counter()

    def rosters(self) -> Dict[str, Dict[str, str]]:
        try:
            page = self.fetch.saved("licensing-section", LICENSING_URL).decode("utf-8", "replace")
        except (requests.RequestException, ValueError) as exc:
            logger.warning(f"State Licensing Section page failed ({exc}); no roster details this run")
            return {}
        out: Dict[str, Dict[str, str]] = {}
        for url, kind in dict.fromkeys(ROSTER_LINK.findall(page)):
            name = "roster-" + ("stf" if kind.lower().startswith("special") else "tlp")
            try:
                rows = parse_roster(self.fetch.saved(name, url, binary=True))
            except (requests.RequestException, ValueError) as exc:
                logger.warning(f"  roster {url} failed: {exc}")
                continue
            logger.info(f"  {name}: {len(rows)} licences ({url})")
            for row in rows:
                row["roster_url"] = url
                out[row["licence"]] = row
        return out

    def scrape(self, seen: Dict[str, set], state: Dict[str, Any], limit: int = 0,
               only: Optional[List[str]] = None) -> Tuple[List[Dict], Dict[str, List[str]]]:
        scope = load_scope()
        page = self.fetch.saved("inspection-reports", INDEX_URL).decode("utf-8", "replace")
        rows = parse_index(page)
        links = sum(len(r["links"]) for r in rows)
        logger.info(f"Index: {len(rows)} STF/TLP facilities, {links} report links")
        self.stats["page facilities"] = len(rows)
        self.stats["page links"] = links
        roster = self.rosters()

        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        registry = state.setdefault("facilities", {})
        chosen = []
        for row in rows:
            entry = scope.get(row["row"])
            if not entry:
                self.unlisted.append(f"row {row['row']} {row['name']} ({row['type']}, {len(row['links'])} reports)")
                continue
            if norm(entry.get("page_name", entry["name"])) != norm(row["name"]):
                self.renamed.append(f"row {row['row']}: scope file says {entry.get('page_name', entry['name'])!r}, page says {row['name']!r}")
            self.stats[f"scope {entry['scope']}"] += 1
            if entry["scope"] == "unsure":
                self.unsure.append(f"{row['name']} ({len(row['links'])} reports): {entry['why']}")
            if entry["scope"] != "in":
                continue
            if only and not any(o.lower() in (row["name"] + " " + entry.get("licence", "")).lower() for o in only):
                continue
            chosen.append((row, entry))
        if limit:
            chosen = chosen[:limit]

        for row, entry in chosen:
            facility = self.build_facility(row, entry, roster, seen, registry)
            if facility is None:
                continue
            key = facility["facility_info"]["program_name"]
            new_ids[key] = [r["report_id"] for r in facility["reports"]]
            facilities.append(facility)
        return facilities, new_ids

    def build_facility(self, row: Dict, entry: Dict, roster: Dict[str, Dict[str, str]],
                       seen: Dict[str, set], registry: Dict[str, Any]) -> Optional[Dict]:
        licence = entry.get("licence") or ""
        program_name = licence or f"OHCA-row-{row['row']}"
        info_roster = roster.get(licence) if licence else None
        name = entry.get("display_name") or entry["name"]
        known = registry.setdefault(program_name, {"row": row["row"], "name": name, "reports": {}})
        logger.info(f"{name} [{program_name}]: {len(row['links'])} documents")

        address = ""
        if info_roster:
            address = f"{info_roster['street']}, {info_roster['city']}, HI {info_roster['zip']}"
        licence_type = licence.split("-")[-1] if licence else row["type"]
        info = {
            "facility_name": name,
            "program_name": program_name,
            "program_category": TYPE_NAMES.get(licence_type, TYPE_NAMES.get(row["type"], "")),
            "full_address": address,
            "phone": format_phone(info_roster["phone"]) if info_roster else "",
            "license_exp_date": us_to_iso(info_roster["expires"]) if info_roster else "",
            "action": "On the state's licence roster" if info_roster else "Not on the state's current licence roster",
        }

        reports: List[Dict] = []
        used_ids: Dict[str, int] = {}
        for link in row["links"]:
            label_iso = label_date(link["label"])
            base = label_iso or archive_name(link["url"]).rsplit(".", 1)[0].lower()
            used_ids[base] = used_ids.get(base, 0) + 1
            report_id = base if used_ids[base] == 1 else f"{base}-{used_ids[base]}"
            name_in_archive = archive_name(link["url"])
            fingerprint = link["url"]
            if report_id in seen.get(program_name, set()) and known["reports"].get(report_id) == fingerprint:
                self.stats["already posted"] += 1
                continue
            extracted = extract_with_cache(self.reports, name_in_archive,
                                           lambda u=link["url"]: self.fetch.pdf(u), extract_pdf)
            if not extracted or not extracted.get("text"):
                self.failed.append(f"{name} {link['label']} {link['url']}")
                continue
            parsed = parse_statement(extracted)
            where = f"{name} {link['label']} ({link['url']})"
            if not parsed["is_statement"]:
                self.not_a_report.append(where)
                self.kinds["not_a_report"] += 1
                continue
            report = self.build_report(row, entry, link, report_id, label_iso, name_in_archive, parsed, where)
            if report is None:
                continue
            known["reports"][report_id] = fingerprint
            reports.append(report)
        reports.sort(key=lambda r: r["report_date"], reverse=True)
        if not info["full_address"]:
            # Not on the roster (closed): the newest statement's own address,
            # from a text PDF before an OCR read.
            ranked = sorted(reports, key=lambda r: (not r["categories"]["ocr"], r["report_date"]), reverse=True)
            info["full_address"] = next((r["categories"]["address_on_document"] for r in ranked
                                         if r["categories"]["address_on_document"]), "")
        return {"facility_info": info, "reports": reports}

    def build_report(self, row, entry, link, report_id, label_iso, archive, parsed, where) -> Optional[Dict]:
        deficiencies = parsed["deficiencies"]
        report_date = label_iso or parsed["inspection_date"]
        if label_iso and parsed["inspection_date"] and label_iso != parsed["inspection_date"]:
            self.date_mismatch.append(f"{where}: label {label_iso}, document {parsed['inspection_date']}")
        if parsed["no_deficiencies"] and not deficiencies:
            kind = "no_deficiencies"
        elif deficiencies:
            kind = "deficiencies"
        else:
            kind = "unread"
            self.unread.append(where)
        self.kinds[kind] += 1
        if any(d["correction"] == HANDWRITTEN or d["future_plan"] == HANDWRITTEN for d in deficiencies):
            self.handwritten.append(where)

        inspection_type = parsed["inspection_type"] or ("Initial" if re.search(r"initial", link["label"], re.I) else "")
        lines: List[str] = []
        for d in deficiencies:
            lines.append(f"{d['rule']} {d['heading']}".strip())
            if d["rule_text"]:
                lines.append(d["rule_text"])
            if d["finding"]:
                lines.append("Findings: " + d["finding"])
            if d["correction"] and d["correction"] != HANDWRITTEN:
                lines.append("Correction: " + d["correction"])
            if d["future_plan"] and d["future_plan"] != HANDWRITTEN:
                lines.append("Future plan: " + d["future_plan"])
            lines.append("")
        raw = "\n".join(lines).strip() if deficiencies else (
            "No deficiencies." if kind == "no_deficiencies" else "")

        hits = privacy_hits(raw)
        if hits:
            self.held.append(f"{where}: {', '.join(hits)}")
            self.hold(archive)
            return None

        type_word = (inspection_type + " inspection").strip().capitalize() if inspection_type else "Inspection"
        if kind == "no_deficiencies":
            summary = f"{type_word}: no deficiencies"
        elif kind == "deficiencies":
            summary = f"{type_word}: {len(deficiencies)} deficienc{'y' if len(deficiencies) == 1 else 'ies'}"
        else:
            summary = f"{type_word}: statement could not be read; open the document"
        if report_date:
            self.dates.append(report_date)
        return {
            "report_id": report_id,
            "report_date": report_date,
            "report_url": link["url"],
            "raw_content": raw,
            "content_length": len(raw),
            "summary": summary,
            "categories": {
                "kind": kind,
                "inspection_type": inspection_type,
                "label": link["label"],
                "year_column": link["column"],
                "chapter": parsed["chapter"],
                "name_on_document": parsed["facility"],
                "address_on_document": parsed["address"],
                "deficiency_count": len(deficiencies),
                "deficiencies": deficiencies,
                "ocr": parsed["ocr"],
                "archive_name": archive,
                "page_row": row["row"],
                "page_type": row["type"],
            },
        }

    def hold(self, archive: str) -> None:
        source = self.reports.archive_dir / archive
        try:
            if source.exists():
                HELD_DIR.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source), str(HELD_DIR / archive))
        except OSError as exc:
            logger.warning(f"  could not move held copy {archive}: {exc}")

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        logger.info("=" * 70)
        logger.info(f"requests made: {self.fetch.count}")
        for key, value in sorted(self.stats.items()):
            logger.info(f"{key}: {value}")
        logger.info(f"facilities in payload: {len(facilities)}; reports: {len(reports)}")
        logger.info(f"reports by kind: {dict(self.kinds)}")
        flagged = sum(1 for r in reports if r["categories"]["kind"] == "deficiencies")
        cited = sum(r["categories"]["deficiency_count"] for r in reports)
        logger.info(f"flagged (statements with deficiencies): {flagged}; deficiencies cited: {cited}")
        logger.info(f"read by OCR: {sum(1 for r in reports if r['categories']['ocr'])}")
        if self.dates:
            logger.info(f"date range: {min(self.dates)} to {max(self.dates)}")
        for title, items in (
            ("STF/TLP facilities on the page that hi_scope.json does not list (not scraped)", self.unlisted),
            ("unsure in hi_scope.json (not scraped; owner to decide)", self.unsure),
            ("scope rows whose page name changed", self.renamed),
            ("not_a_report (left out)", self.not_a_report),
            ("held back by the privacy check (not posted)", self.held),
            ("statements whose deficiencies could not be read (posted as unread)", self.unread),
            ("plans of correction in handwriting (not transcribed)", self.handwritten),
            ("label date differs from the document's inspection date", self.date_mismatch),
            ("could not be downloaded or read (retried next run)", self.failed),
        ):
            logger.info(f"{title}: {len(items)}")
            for line in items:
                logger.info(f"  {line}")


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    shaped = [{
        "facility_info": f["facility_info"],
        "reports": [{**r, "is_structured": True} for r in f["reports"]],
    } for f in facilities]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "HI",
        "scraped_timestamp": timestamp,
        "scraping_notes": {"total_reports": sum(len(f["reports"]) for f in shaped)},
        "facilities": shaped,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"Wrote {path}")


def save_to_api(facilities: List[Dict], timestamp: str) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL, api_key=API_KEY, state="HI", scraped_timestamp=timestamp,
        facilities=facilities, timeout=120, info=logger.info, error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Hawaii OHCA special treatment facility and therapeutic living program inspections")
    parser.add_argument("--full", action="store_true", help="Ignore the seen reports in the state file")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N in-scope facilities")
    parser.add_argument("--facility", action="append", default=[], help="Only facilities whose name or licence contains this (repeatable)")
    parser.add_argument("--cached", action="store_true", help="Read the index page and rosters from the saved copies")
    parser.add_argument("--reparse", action="store_true", help="Clear the cached extractions first (PDFs come from the source or the archive)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()

    if args.reparse and REPORTS.extract_dir.exists():
        shutil.rmtree(REPORTS.extract_dir)
    timestamp = datetime.now().isoformat(timespec="seconds")
    scraper = HIScraper(Fetcher(cached=args.cached))
    logger.info(f"PDF archive folder: {scraper.reports.archive_dir}")
    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    facilities, new_ids = scraper.scrape(seen, state, limit=args.limit, only=args.facility or None)
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
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- seen reports not advanced")


if __name__ == "__main__":
    main()
