"""
Maryland residential child care inspection summary scraper.

Source: Maryland Department of Human Services, Office of Licensing and
Monitoring, https://dhs.maryland.gov/licensing-and-monitoring/. The reports
sit in a public file browser (plain requests, no login):

  GET https://dhs.maryland.gov/documents/?dir=Licensing-and-Monitoring/Reports
      one folder per provider (the legal entity)
  GET ...?dir=Licensing-and-Monitoring/Reports/<Provider>
      program-type subfolders: RCC, TFC, Adoption, ILP, CPA
  GET ...?dir=Licensing-and-Monitoring/Reports/<Provider>/RCC
      the PDFs (plain GET of documents/<path>)

Only the RCC (residential child care) folders are read. Each PDF is a
"Residential Child Care Report Summary" of two or three pages: the provider,
a table of the sites inspected (name and address, licence number, capacity,
census, licence expiry, date of inspection), the type of inspection, the
licence status and two blocks of COMAR citations, each with the site, the
regulation, a one-line comment and (since the 10/2021 form) a status:
  1. violations that "MAY present safety risks for children";
  2. violations that "DO NOT present imminent safety risks".

The form is a set of ruled tables, so it is read cell by cell
(pdfplumber find_tables), never from extract_text(), which mixes the label
column into the citations.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, unquote

import pdfplumber
import requests
from bs4 import BeautifulSoup

from inspection_api_client import post_facilities_to_api
from report_store import ReportStore
from scraper_state import load_state, merge_new_ids, save_state, seen_from_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("KOP_DATA_API_KEY", "CHANGE_ME")
STATE_FILE = Path(os.getenv("MD_STATE_FILE", ".md_state.json"))

BROWSER_URL = "https://dhs.maryland.gov/documents/"
REPORTS_DIR = "Licensing-and-Monitoring/Reports"
PROGRAM_FOLDER = "RCC"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 0.7

_REPORTS: Optional[ReportStore] = None


def report_store() -> ReportStore:
    """Made on first use: resolving the Drive folder can ask the user to open
    Google Drive, which importing this module should not do."""
    global _REPORTS
    if _REPORTS is None:
        _REPORTS = ReportStore("MD_PDF_CACHE", "md_pdfs", Path(__file__).parent / "md_pdfs")
    return _REPORTS


# ── Fetch layer ──────────────────────────────────────────────────────────────


class MDClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call = 0.0
        self.requests_made = 0

    def _pause(self) -> None:
        wait = REQUEST_GAP - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, url: str, **kwargs) -> requests.Response:
        """One GET with retries on timeouts, connection errors and 5xx."""
        delay = 2.0
        for attempt in range(1, 5):
            self._pause()
            self.requests_made += 1
            try:
                response = self.session.get(url, timeout=120, **kwargs)
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

    def listing(self, folder: str) -> Tuple[List[str], List[str]]:
        """(subfolder paths, file paths) of one folder; paths are decoded and
        relative to the file browser's root."""
        response = self._request(BROWSER_URL, params={"dir": folder})
        response.raise_for_status()
        return parse_listing(response.text, folder)

    def pdf(self, path: str) -> Optional[bytes]:
        try:
            response = self._request(pdf_url(path))
        except requests.RequestException as exc:
            logger.warning(f"  {path}: download failed ({exc}); skipped for this run")
            return None
        if response.status_code != 200 or not response.content.startswith(b"%PDF"):
            logger.warning(f"  {path}: HTTP {response.status_code}, not a PDF; skipped for this run")
            return None
        return response.content


def pdf_url(path: str) -> str:
    return BROWSER_URL + quote(path, safe="/")


def parse_listing(page: str, folder: str) -> Tuple[List[str], List[str]]:
    """The browser marks every entry as <li data-name data-href>: a folder's
    data-href is "?dir=<path>", a file's is its path under documents/."""
    soup = BeautifulSoup(page, "html.parser")
    folders: List[str] = []
    files: List[str] = []
    for item in soup.find_all("li", attrs={"data-href": True}):
        name = (item.get("data-name") or "").strip()
        href = item["data-href"].strip()
        if not name or name == "..":
            continue
        if "?dir=" in href:
            path = unquote(href.split("?dir=", 1)[1])
            if path.startswith(folder.rstrip("/") + "/"):
                folders.append(path)
        elif not re.match(r"^(?:https?:|javascript:|#)", href, re.I):
            files.append(unquote(href))
    return folders, files


# ── PDF extraction ───────────────────────────────────────────────────────────


OCR_DPI = 300
EXTRACT_VERSION = 2


def extract_pdf(path: Path) -> Dict:
    """Each page's words with their positions and its ruled tables with every
    cell's box, so a parser fix never needs the PDF again. About a fifth of
    the reports are scans (a landscape form scanned sideways): those pages
    are turned upright, their rules found in the image and their words read
    by OCR, into the same shape."""
    pages: List[Dict[str, Any]] = []
    ocr_pages: List[int] = []
    with pdfplumber.open(path) as pdf:
        for number, page in enumerate(pdf.pages, start=1):
            words = [[round(w["x0"], 1), round(w["top"], 1), round(w["x1"], 1), round(w["bottom"], 1), w["text"]]
                     for w in page.extract_words(x_tolerance=2, y_tolerance=2)]
            if len(words) < 15 and page.images:
                scanned = ocr_page(page)
                if scanned and len(scanned["words"]) > len(words):
                    pages.append(scanned)
                    ocr_pages.append(number)
                    continue
            tables = []
            for table in page.find_tables():
                tables.append({
                    "bbox": [round(v, 1) for v in table.bbox],
                    "cells": [[([round(v, 1) for v in cell] if cell else None) for cell in row.cells]
                              for row in table.rows],
                })
            pages.append({"width": float(page.width), "height": float(page.height),
                          "words": words, "tables": tables})
    result: Dict[str, Any] = {
        "version": EXTRACT_VERSION,
        "text": "\n".join(page_text(p["words"]) for p in pages).strip(),
        "pages": pages,
    }
    if ocr_pages:
        result["ocr_pages"] = ocr_pages
    return result


def find_tesseract() -> str:
    if os.getenv("TESSERACT_CMD"):
        return os.getenv("TESSERACT_CMD")
    default = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    return str(default) if default.exists() else ""


def ocr_page(page) -> Optional[Dict[str, Any]]:
    """A scanned page as {width, height, words, tables, ocr}, in points of the
    upright page. None when the OCR tools are missing or fail."""
    try:
        import cv2
        import numpy as np
        import pytesseract
    except ImportError:
        logger.warning("  scanned page and opencv/pytesseract are not installed; page left unread")
        return None
    tesseract = find_tesseract()
    if tesseract:
        pytesseract.pytesseract.tesseract_cmd = tesseract
    try:
        image = page.to_image(resolution=OCR_DPI).original.convert("L")
        best = None
        # The form is landscape; a portrait image is the form on its side.
        turns = (90, 270) if image.height > image.width else (0, 180)
        for turn in turns:
            upright = image.rotate(turn, expand=True) if turn else image
            gray = deskew(cv2, np, np.array(upright))
            cleaned, edges = ruled_lines(cv2, np, gray)
            data = pytesseract.image_to_data(cleaned, config="--psm 11", output_type=pytesseract.Output.DICT)
            words = []
            score = 0.0
            for text, conf, left, top, width, height in zip(
                    data["text"], data["conf"], data["left"], data["top"], data["width"], data["height"]):
                text = (text or "").strip()
                conf = float(conf)
                if not text or conf < 0:
                    continue
                if conf < 35 and not re.search(r"[A-Za-z0-9]{2}", text):
                    continue
                scale = 72.0 / OCR_DPI
                words.append([round(left * scale, 1), round(top * scale, 1), round((left + width) * scale, 1),
                              round((top + height) * scale, 1), text])
                if conf > 70 and re.fullmatch(r"[A-Za-z]{4,}", text):
                    score += 1
            if best is None or score > best[0]:
                best = (score, words, edges, upright.size, cleaned)
            if score > 25:
                break  # plainly upright; the other turn would read as noise
        if best is None:
            return None
        _, words, edges, size, cleaned = best
        tables = tables_from_edges(edges)
        words += lone_numbers(pytesseract, cleaned, tables, words)
        return {
            "width": round(size[0] * 72.0 / OCR_DPI, 1), "height": round(size[1] * 72.0 / OCR_DPI, 1),
            "words": words, "tables": tables, "ocr": True,
        }
    except Exception as exc:  # tesseract missing, a broken image
        logger.warning(f"  OCR failed: {exc}")
        return None


def lone_numbers(pytesseract, cleaned, tables: List[Dict[str, Any]], words: List[List[Any]]) -> List[List[Any]]:
    """Page-wide OCR skips a digit alone in a cell (a capacity of 8, a census
    of 0). Each small cell left empty that has ink in it is read again on
    its own, digits only."""
    scale = OCR_DPI / 72.0
    found: List[List[Any]] = []
    for table in tables:
        for row in table["cells"]:
            for box in row:
                if box is None or box[2] - box[0] > 110 or box[3] - box[1] > 60:
                    continue
                if any(box[0] <= (w[0] + w[2]) / 2.0 <= box[2] and box[1] <= (w[1] + w[3]) / 2.0 <= box[3] for w in words):
                    continue
                x0, y0, x1, y1 = (int(v * scale) for v in box)
                crop = cleaned[y0 + 8:y1 - 8, x0 + 8:x1 - 8]
                if crop.size == 0 or int((crop < 128).sum()) < 40:
                    continue
                data = pytesseract.image_to_data(
                    crop, config="--psm 7 -c tessedit_char_whitelist=0123456789",
                    output_type=pytesseract.Output.DICT)
                read = [(t.strip(), float(c)) for t, c in zip(data["text"], data["conf"]) if t.strip()]
                if len(read) == 1 and read[0][1] >= 60 and re.fullmatch(r"\d{1,3}", read[0][0]):
                    middle_x, middle_y = (box[0] + box[2]) / 2.0, (box[1] + box[3]) / 2.0
                    found.append([round(middle_x - 3, 1), round(middle_y - 5, 1), round(middle_x + 3, 1),
                                  round(middle_y + 5, 1), read[0][0]])
    return found


def deskew(cv2, np, gray):
    """Turn the scan by the slope of its long horizontal rules: a page fed a
    degree askew puts a rule's two ends in different rows of the table."""
    binary = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY_INV, 25, 12)
    horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_RECT, (90, 1)))
    count, labels, stats, _ = cv2.connectedComponentsWithStats(horizontal, connectivity=8)
    angles = []
    for i in range(1, count):
        if stats[i, cv2.CC_STAT_WIDTH] < 600:
            continue
        ys, xs = np.nonzero(labels == i)
        slope = np.polyfit(xs, ys, 1)[0]
        angles.append(float(np.degrees(np.arctan(slope))))
    if not angles:
        return gray
    angle = float(np.median(angles))
    if abs(angle) < 0.08 or abs(angle) > 8:
        return gray
    height, width = gray.shape
    matrix = cv2.getRotationMatrix2D((width / 2.0, height / 2.0), angle, 1.0)
    return cv2.warpAffine(gray, matrix, (width, height), flags=cv2.INTER_LINEAR, borderValue=255)


def ruled_lines(cv2, np, gray) -> Tuple[Any, List[Dict[str, float]]]:
    """The table rules of a scanned page as edges (points), and the image
    with the rules painted out so they do not disturb the OCR."""
    binary = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY_INV, 25, 12)
    horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_RECT, (90, 1)))
    vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_RECT, (1, 70)))
    scale = 72.0 / OCR_DPI
    edges: List[Dict[str, float]] = []
    for mask, orientation in ((horizontal, "h"), (vertical, "v")):
        count, _, stats, _ = cv2.connectedComponentsWithStats(mask, connectivity=8)
        for i in range(1, count):
            x, y, w, h = (int(stats[i, k]) for k in range(4))
            if orientation == "h":
                if w < 150 or h > 25:
                    continue
                mid = (y + h / 2.0) * scale
                edges.append({"x0": x * scale, "x1": (x + w) * scale, "top": mid, "bottom": mid,
                              "width": w * scale, "height": 0.0, "orientation": "h", "object_type": "line"})
            else:
                if h < 70 or w > 25:
                    continue
                mid = (x + w / 2.0) * scale
                edges.append({"x0": mid, "x1": mid, "top": y * scale, "bottom": (y + h) * scale,
                              "width": 0.0, "height": h * scale, "orientation": "v", "object_type": "line"})
    lines = cv2.dilate(cv2.bitwise_or(horizontal, vertical), np.ones((3, 3), np.uint8))
    cleaned = gray.copy()
    cleaned[lines > 0] = 255
    return cleaned, edges


def tables_from_edges(edges: List[Dict[str, float]]) -> List[Dict[str, Any]]:
    """Cells from the rules, by pdfplumber's own table finder."""
    from pdfplumber import table as plumber

    merged = plumber.merge_edges(edges, snap_x_tolerance=5, snap_y_tolerance=5,
                                 join_x_tolerance=8, join_y_tolerance=8)
    intersections = plumber.edges_to_intersections(merged, x_tolerance=6, y_tolerance=6)
    cells = plumber.intersections_to_cells(intersections)
    out = []
    for group in plumber.cells_to_tables(cells):
        table = plumber.Table(None, group)
        out.append({
            "bbox": [round(v, 1) for v in table.bbox],
            "cells": [[([round(v, 1) for v in cell] if cell else None) for cell in row.cells]
                      for row in table.rows],
        })
    return out


def join_words(words: List[List[Any]]) -> str:
    """Words of one cell or page in reading order, one line per text line.
    The PDFs split some words with no gap at all ("C" "ontinued"); those are
    put back together."""
    if not words:
        return ""
    ordered = sorted(words, key=lambda w: ((w[1] + w[3]) / 2.0, w[0]))
    lines: List[List[List[Any]]] = []
    for word in ordered:
        middle = (word[1] + word[3]) / 2.0
        if lines and abs(middle - lines[-1][0][5]) <= max(3.0, (word[3] - word[1]) * 0.45):
            lines[-1].append(word + [middle])
        else:
            lines.append([word + [middle]])
    out = []
    for line in lines:
        line.sort(key=lambda w: w[0])
        text = line[0][4]
        for before, word in zip(line, line[1:]):
            height = max(before[3] - before[1], 1.0)
            glued = (word[0] - before[2]) < 0.04 * height
            text += ("" if glued else " ") + word[4]
        out.append(text)
    return "\n".join(out)


def page_text(words: List[List[Any]]) -> str:
    return join_words(words)


def table_rows(page: Dict[str, Any]) -> List[Tuple[List[List[Any]], Dict[str, Any]]]:
    """Every table of a page as (rows of cell texts, the table), top to
    bottom; a text is None where a merged cell covers the place. A table drawn inside another table's
    cell (the state's form does this around long citation numbers) is left
    out: the outer table already holds its words."""
    tables = sorted(page.get("tables") or [], key=lambda t: (t["bbox"][1], t["bbox"][0]))

    def inside(inner: List[float], outer: List[float]) -> bool:
        return (inner is not outer and inner[0] >= outer[0] - 2 and inner[1] >= outer[1] - 2
                and inner[2] <= outer[2] + 2 and inner[3] <= outer[3] + 2
                and (inner[2] - inner[0]) < 0.8 * (outer[2] - outer[0]))

    tables = [t for t in tables if not any(inside(t["bbox"], other["bbox"]) for other in tables)]
    words = page.get("words") or []
    out = []
    for table in tables:
        rows = []
        for row in table["cells"]:
            cells: List[Any] = []
            for box in row:
                if box is None:
                    cells.append(None)
                    continue
                held = [w for w in words
                        if box[0] - 1 <= (w[0] + w[2]) / 2.0 <= box[2] + 1 and box[1] - 1 <= (w[1] + w[3]) / 2.0 <= box[3] + 1]
                cells.append(join_words(held))
            rows.append(cells)
        out.append((rows, table))
    return out


# ── Parsing ──────────────────────────────────────────────────────────────────

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}
NUM_DATE = re.compile(r"(?<!\d)(\d{1,2})\s*[./-]\s*(\d{1,2})\s*[./-]\s*(\d{4}|\d{2})(?!\d)")
RUN_DATE = re.compile(r"(?<!\d)(\d{1,2})/(\d{2})(\d{4})(?!\d)")  # "10/262022", a slash left out
LONG_DATE = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b", re.I)
COMAR = re.compile(r"^\W{0,2}\d{2}\s*\.\s*\d{2}")
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+\s?@\s?[A-Za-z0-9-]+(?:\s?\.\s?[A-Za-z0-9-]+)+")
NONE_STATED = re.compile(
    r"^\W*(?:0|no|none|n\s*/?\s*a)\b\s*(?:comar\s+)?(?:violations?|citations?)?\s*(?:were\s+)?"
    r"(?:found|noted|cited|observed)?\W*$", re.I)


def clean(value: Any) -> str:
    """One line; the PDFs' unmapped apostrophes, dashes and quotes (read as
    U+FFFD) put back by position."""
    text = str(value or "").replace("\xa0", " ").replace("’", "'").replace("‘", "'")
    text = text.replace("“", '"').replace("”", '"').replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"(?<=\w)�(?=\w)", "'", text)
    text = re.sub(r"(?<=\s)�(?=\s)", "-", text)
    text = re.sub(r"(?:(?<=\s)|^)�(?=\w)", '"', text)
    text = re.sub(r"(?<=[\w.,;!?])�", lambda m: '"' if text[: m.start()].count('"') % 2 else "'", text)
    return text.replace("�", "").strip()


def squash(value: Any) -> str:
    """Lowercase letters, digits and # only: labels compare equal however
    the PDF or OCR spaced them."""
    return re.sub(r"[^a-z0-9#]", "", str(value or "").lower())


def iso_date(value: str) -> str:
    """The first date in `value` as YYYY-MM-DD, or ''. The state writes
    5.21.25, 6/16/2021, 03-23-22, April 4, 2024."""
    value = value or ""
    long_match = LONG_DATE.search(value)
    num_match = NUM_DATE.search(value) or RUN_DATE.search(value)
    try:
        if long_match and (not num_match or long_match.start() < num_match.start()):
            return datetime(int(long_match.group(3)), MONTHS[long_match.group(1).lower()[:3]],
                            int(long_match.group(2))).strftime("%Y-%m-%d")
        if num_match:
            year = int(num_match.group(3))
            if year < 100:
                year += 2000
            if not 2000 <= year <= datetime.now().year + 6:
                return ""
            return datetime(year, int(num_match.group(1)), int(num_match.group(2))).strftime("%Y-%m-%d")
    except ValueError:
        return ""
    return ""


def strip_dates(value: str) -> str:
    value = LONG_DATE.sub(" ", value or "")
    value = NUM_DATE.sub(" ", value)
    return clean(RUN_DATE.sub(" ", value))


def all_rows(extracted: Dict) -> List[Dict[str, Any]]:
    """Every table row in reading order: {page, table, cells, boxes, words}.
    cells holds None where a merged neighbour covers the place."""
    out = []
    for p, page in enumerate(extracted.get("pages") or []):
        for t, (rows, table) in enumerate(table_rows(page)):
            for cells, boxes in zip(rows, table["cells"]):
                out.append({"page": p, "table": t, "cells": cells, "boxes": boxes, "words": page.get("words") or []})
    return out


KEY_LABELS = [
    ("provider", re.compile(r"^providerorgani[sz]ation$")),
    ("administrator", re.compile(r"^nameof(?:certified)?(?:program|chief)administrator$")),
    ("email", re.compile(r"^emailof")),
    ("contracting_agency", re.compile(r"^contractingagency")),
    ("licensing_agency", re.compile(r"^licensingagency$")),
    ("license_type", re.compile(r"^licen[sc]etype$")),
    ("inspection_type", re.compile(r"^typeofinspection$")),
    ("license_status", re.compile(r"^currentstatusoflicen[sc]e$")),
]

SITE_COLUMNS = [
    ("name", re.compile(r"^(?:name/?address|sitename)")),
    ("capacity", re.compile(r"^licen[sc]ecapacity")),
    ("contract_limit", re.compile(r"^(?:total)?dhscontract")),
    ("dhs", re.compile(r"^dhscensus")),
    ("djs", re.compile(r"^djscensus")),
    ("other", re.compile(r"^othercensus")),
    ("licence", re.compile(r"^licen[sc]e#")),
    ("date", re.compile(r"^dateof")),
]

SAFETY_LABEL = re.compile(r"whichmaypresent")
OTHER_LABEL = re.compile(r"whichdonot")
LABEL_TEXTS = (
    "thisproviderwascitedforthelistedcomarviolationswhichmaypresentsafetyrisksforchildrenbasedonimpactscope"
    "andfrequencytheseissuesareeitherresolvedoracorrectiveactionplanhasbeenimplemented",
    "thisproviderwascitedforthelistedcomarviolationswhichdonotpresentimminentsafetyrisksforchildrenbasedon"
    "impactscopeandfrequency",
)


def block_of(keys: List[str]) -> str:
    for key in keys:
        if SAFETY_LABEL.search(key):
            return "safety"
        if OTHER_LABEL.search(key):
            return "other"
    return ""


def is_label(text: str) -> bool:
    """The left-hand label of a citation block, or a piece of it carried
    over to the next page ("based on impact, scope, and frequency.")."""
    key = squash(text)
    return len(key) >= 8 and any(key in label for label in LABEL_TEXTS)


def value_beside(row: Dict[str, Any], at: int) -> str:
    """A label's value: the next filled cell, or (a few 2019 forms rule the
    labels only) the words on the label's line to its right."""
    value = next((clean(c) for c in row["cells"][at + 1:] if c is not None and clean(c)), "")
    if value:
        return value
    box = row["boxes"][at] if at < len(row["boxes"]) else None
    if not box or any(b is not None for b in row["boxes"][at + 1:]):
        return ""
    beside = [w for w in row["words"] if w[0] >= box[2] - 1 and box[1] - 1 <= (w[1] + w[3]) / 2.0 <= box[3] + 1]
    return clean(join_words(beside))


def new_parsed(form: str) -> Dict[str, Any]:
    return {"form": form, "sites": [], "safety": [], "other": [], "unrated": [], "stated_none": [],
            "has_status": False, "has_safety_block": False, "found_citation_table": False,
            "signed_dates": [], "dropped_rows": []}


def parse_landscape(extracted: Dict) -> Dict[str, Any]:
    """The "Residential Child Care Report Summary" (mid-2019 on): ruled
    tables on landscape pages. Since the 10/2021 revision the citations come
    in two blocks and carry a status."""
    out = new_parsed("summary")
    site_map: Optional[Dict[int, str]] = None
    site_table: Optional[Tuple[int, int]] = None
    cite_map: Optional[Dict[str, int]] = None
    cite_width = 0
    block = ""
    last: Optional[Dict[str, str]] = None
    in_staff = False
    for row in all_rows(extracted):
        raw = row["cells"]
        cells = [clean(c) for c in raw if c is not None]
        if not any(cells):
            continue
        keys = [squash(c) for c in cells]
        filled = [k for k in keys if k]
        if not filled:
            continue  # punctuation or rules only
        first = filled[0]
        # Staff table: names, roles, emails. Only the dates signed are read.
        if (first.startswith("officeoflicensingandmonitoringstaff") or ("role" in filled and "email" in filled)
                or filled[:2] == ["name", "role"]):
            in_staff = True
            cite_map = None
            site_map = None
            continue
        if in_staff:
            signed = iso_date(cells[-1]) or next((iso_date(c) for c in reversed(cells) if "@" not in c and iso_date(c)), "")
            if signed:
                out["signed_dates"].append(signed)
            continue
        if any("@" in c for c in cells) and not any(COMAR.match(c) for c in cells):
            continue  # an email row; never read
        is_cite_header = any(k.startswith("comarcitation") for k in keys) and "comment" in keys
        # Key and value on one row.
        labelled = next(((name, i) for i, c in enumerate(raw) if c is not None
                         for name, rx in KEY_LABELS if rx.match(squash(c))), None)
        if labelled and not is_cite_header:
            name, at = labelled
            value = value_beside(row, at)
            if name != "email" and value and "@" not in value and not out.get(name):
                out[name] = value
            continue
        # Site table.
        if any(SITE_COLUMNS[0][1].match(k) for k in keys):
            site_map = {}
            for i, cell in enumerate(raw):
                key = squash(cell)
                for name, rx in SITE_COLUMNS:
                    if key and rx.match(key):
                        site_map[i] = name
            site_table = (row["page"], row["table"])
            continue
        if site_map and site_table == (row["page"], row["table"]):
            site = {name: clean(raw[i]) if i < len(raw) and raw[i] is not None else ""
                    for i, name in site_map.items()}
            if re.sub(r"[\W_]+", "", site.get("name", "")) and len([v for v in site.values() if v]) >= 2:
                out["sites"].append(site)
            continue
        # Citation table.
        if is_cite_header:
            cite_map = {}
            for i, cell in enumerate(raw):
                key = squash(cell)
                if key == "rccsite":
                    cite_map["site"] = i
                elif key.startswith("comarcitation") and "citation" not in cite_map and (
                        len(key) < 30 and not is_label(cell or "")):
                    cite_map["citation"] = i
                elif key == "comment":
                    cite_map["comment"] = i
                elif key.startswith("citationstatus"):
                    cite_map["status"] = i
                    out["has_status"] = True
            cite_width = len(raw)
            out["found_citation_table"] = True
            block = block_of(keys) or block
            out["has_safety_block"] = out["has_safety_block"] or block == "safety"
            last = None
            continue
        new_block = block_of(keys)
        if new_block and new_block != block:
            block = new_block
            last = None
            out["has_safety_block"] = out["has_safety_block"] or block == "safety"
        if cite_map is None:
            continue
        if first.startswith("comarcitations") or first.startswith("capcorrectiveactionplan"):
            continue
        item = read_citation_row(raw, cite_map if len(raw) == cite_width else None)
        if item is None:
            continue
        if item == "none":
            out["stated_none"].append(block or "other")
            continue
        if not item["citation"] and not item["site"]:
            if last is not None:
                # The rest of a comment that ran over the page.
                last["comment"] = clean(last["comment"] + " " + item["comment"])
                if item["status"] and not last["status"]:
                    last["status"] = item["status"]
                continue
            if len(item["comment"]) < 15:
                out["dropped_rows"].append(item["comment"])
                continue
        if not COMAR.match(item["citation"]) and len(item["comment"]) < 8:
            out["dropped_rows"].append(" | ".join(v for v in item.values() if v))
            continue
        if not any(ch.isdigit() for ch in item["citation"]) and not OCR_WORD.search(item["comment"]):
            # A scan's table rules read as letters ("ee ee Ee eee"): no number, no real word.
            out["dropped_rows"].append(" | ".join(v for v in item.values() if v))
            continue
        out[block or "other"].append(item)
        last = item
    return out


OCR_WORD = re.compile(r"[A-Za-z]{5,}")


def read_citation_row(row: List[Any], columns: Optional[Dict[str, int]]) -> Any:
    """One row of the citation table -> {site, citation, comment, status},
    'none' for the "No COMAR violations." row, None for an empty row. With no
    column map (the table carried over to a page with other rules) the COMAR
    number anchors the row: the site is before it, the comment and status
    after."""
    def at(name: str) -> str:
        i = columns.get(name, -1) if columns else -1
        return clean(row[i]) if 0 <= i < len(row) and row[i] is not None else ""

    if columns:
        item = {"site": at("site"), "citation": at("citation"), "comment": at("comment"), "status": at("status")}
    else:
        cells = [clean(c) for c in row if c is not None]
        cells = ["" if is_label(c) else c for c in cells]
        hit = next((i for i, c in enumerate(cells) if COMAR.match(c)), None)
        texts = [c for c in cells if c]
        if not texts:
            return None
        if hit is None:
            status = texts[-1] if len(texts) > 1 and re.fullmatch(r"(?i)cap|resolved|cap\s*/\s*resolved", texts[-1]) else ""
            item = {"site": "", "citation": "", "comment": " ".join(texts[:-1] if status else texts), "status": status}
        else:
            before = [c for c in cells[:hit] if c]
            after = cells[hit + 1:]
            item = {"site": before[-1] if before else "", "citation": cells[hit],
                    "comment": after[0] if after else "", "status": " ".join(c for c in after[1:] if c)}
    if is_label(item["site"]):
        item["site"] = ""
    if not any(item.values()):
        return None
    # "No COMAR violations.", "NONE" or "0" in any column of an otherwise empty row.
    texts = [v for v in (item["site"], item["citation"], item["comment"]) if v]
    if texts and all(NONE_STATED.match(v) for v in texts):
        return "none"
    if not item["citation"] and not item["comment"]:
        return None  # a site name or a status alone on a row says nothing
    return item


def text_field(text: str, label: str, stop: str = r"\n") -> str:
    match = re.search(label + r"\s*:?[ \t]*(.*?)\s*(?:" + stop + r"|$)", text, re.I | re.S)
    return clean(match.group(1)) if match else ""


def checked(text: str, label: str) -> str:
    """'Yes', 'No' or '' from "<label>: Yes X No" / "Yes No X"."""
    match = re.search(label + r"\s*:?\s*Yes[ \t_]*([Xx]?)[ \t_]*No\b[ \t_]*([Xx]?)", text, re.I)
    if not match:
        return ""
    if match.group(1):
        return "Yes"
    if match.group(2):
        return "No"
    return ""


PORTRAIT_SITE = ["name", "gender", "age", "capacity", "contract_limit", "licence", "date"]
SPLIT_CITATION = re.compile(r"^(\d{2}\.[\d.\-]+[A-Za-z]?(?:\s*\([^)]{1,4}\)|\s+[A-Z](?=\s*\(|\s+[A-Z0-9]))*)\s+(\S.*)$")


def parse_portrait(extracted: Dict, text: str) -> Dict[str, Any]:
    """The "Residential Child Care Programs Report" used until mid-2019: a
    portrait page of labelled lines, a site table and a Violation(s) /
    Findings table. Its citations name no site and carry no severity."""
    out = new_parsed("2019")
    out["provider"] = text_field(text, r"Provider\s+Organi[sz]ation")
    out["licensing_agency"] = text_field(text, r"Licensing\s+Agency", r"Contracting\s+Agency|\n")
    out["contracting_agency"] = text_field(text, r"Contracting\s+Agency\(?s?\)?")
    out["administrator"] = text_field(text, r"Program\s+Administrator", r"Certification|\n")
    out["inspection_type"] = text_field(text, r"Type\s+of\s+Inspection")
    out["license_status"] = text_field(text, r"Current\s+Status\s+of\s+License")
    out["violation_checked"] = checked(text, r"Current\s+COMAR\s+Violation")
    out["cap_checked"] = checked(text, r"Corrective\s+Action\s+Plan")
    out["cap_date"] = iso_date(text_field(text, r"date\s+of\s+CAP"))
    # "Coordinator: <name> Date: 6/6/2019 Email: ..." -- only the date is read.
    out["signed_dates"] = [d for d in (iso_date(m) for m in re.findall(r"\bDate\s*:\s*([^\n]{0,14})", text)) if d]

    mode = ""
    for row in all_rows(extracted):
        cells = [clean(c) for c in row["cells"] if c is not None]
        keys = [squash(c) for c in cells]
        texts = [c for c in cells if c]
        if "sitename" in keys:
            mode = "sites"
            continue
        if any(k.startswith("violation") for k in keys) and any(k.startswith("finding") for k in keys):
            mode = "violations"
            out["found_citation_table"] = True
            continue
        if not texts or any("@" in c for c in texts):
            continue
        if mode == "sites":
            if all(re.fullmatch(r"(?:age)?range|capacity|contract|limit|exp\.?date|date|inspection", k) for k in keys if k):
                continue  # the rest of the header
            if len(texts) == len(PORTRAIT_SITE):
                out["sites"].append(dict(zip(PORTRAIT_SITE, texts)))
            elif len(texts) >= 2:
                site = {"name": texts[0], "date": texts[-1] if len(texts) > 2 else ""}
                site["licence"] = next((v for v in texts[1:] if "#" in v), "")
                out["sites"].append(site)
        elif mode == "violations":
            if all(NONE_STATED.match(c) for c in texts):
                out["stated_none"].append("unrated")
                continue
            hit = next((i for i, c in enumerate(texts) if COMAR.match(c)), None)
            item = {"site": "", "citation": "", "comment": " ".join(texts), "status": ""}
            if hit is not None and len(texts) > 1:
                item["citation"] = texts[hit]
                item["comment"] = " ".join(texts[:hit] + texts[hit + 1:])
            elif hit is not None:
                # The number and the finding typed into one cell.
                split = SPLIT_CITATION.match(texts[0])
                if split:
                    item["citation"], item["comment"] = split.group(1), split.group(2)
                else:
                    item["citation"], item["comment"] = texts[0], ""
            out["unrated"].append(item)
    return out


def document_text(extracted: Dict) -> str:
    return "\n".join(page_text(p.get("words") or []) for p in extracted.get("pages") or [])


def parse_report(extracted: Dict) -> Dict[str, Any]:
    pages = extracted.get("pages") or []
    text = document_text(extracted)
    key = squash(text)
    if "residentialchildcareprogramsreport" in key or (
            pages and pages[0].get("width", 0) < pages[0].get("height", 0) and "violation(s)" in text.lower()):
        parsed = parse_portrait(extracted, text)
    else:
        parsed = parse_landscape(extracted)
    parsed["is_report"] = bool(
        "residentialchildcarereportsummary" in key or "residentialchildcareprogramsreport" in key
        or "rccreportsummary" in key or (parsed["sites"] and parsed["found_citation_table"]))
    parsed["ocr"] = bool(extracted.get("ocr_pages"))
    return parsed


# ── Reports ──────────────────────────────────────────────────────────────────

INSPECTION_TYPES = [
    ("Quarterly", re.compile(r"quarter|qtly")),
    ("Re-licensure", re.compile(r"relic|renew")),
    ("Mid-licensure", re.compile(r"^mid|midlic")),
    ("Periodic", re.compile(r"period")),
]


def inspection_type(value: str) -> str:
    key = squash(value)
    for name, rx in INSPECTION_TYPES:
        if rx.search(key):
            return name
    return clean(value)


def tidy_status(value: str) -> str:
    """ACTIVE -> Active, RE-LICENSED -> Re-licensed; mixed case left alone."""
    value = clean(value)
    letters = [c for c in value if c.isalpha()]
    if letters and all(c.isupper() for c in letters[: max(1, len(value.split("(")[0].replace(" ", "")))]):
        head, sep, tail = value.partition("(")
        value = head.capitalize() + sep + tail
    key = squash(value)
    if key in ("relicensed", "relicensure"):
        return "Re-licensed"
    return value


def number(value: str) -> Optional[int]:
    return int(value) if re.fullmatch(r"\d{1,4}", value or "") else None


def tidy_site(site: Dict[str, str]) -> Dict[str, Any]:
    licence_text = site.get("licence", "")
    licence = strip_dates(licence_text)
    if not re.search(r"\d", licence):
        licence = ""
    date_text = site.get("date", "")
    visited = iso_date(date_text)
    out: Dict[str, Any] = {"name": site.get("name", "")}
    if licence:
        out["licence"] = licence
    expiry = iso_date(licence_text)
    if expiry:
        out["license_exp"] = expiry
    for key in ("capacity", "dhs", "djs", "other"):
        value = number(site.get(key, ""))
        if value is not None:
            out[key] = value
    for key in ("gender", "age"):
        if site.get(key):
            out[key] = site[key]
    if visited:
        out["date"] = visited
    elif re.search(r"[A-Za-z]{3}", date_text):
        out["note"] = date_text  # "Site closed", "Currently Not in Use", "Empty"
    return out


def plural(count: int, word: str, many: str = "") -> str:
    return f"{count} {word if count == 1 else (many or word + 's')}"


BLOCK_TITLES = {
    "safety": "Violations which may present safety risks for children",
    "other": "Violations which do not present imminent safety risks for children",
    "unrated": "Violations cited (the 2019 form does not rate them)",
}


def flatten(categories: Dict[str, Any], report_date: str) -> str:
    """The summary written out plainly: what site search and the severe
    finding scan read. No administrator, staff names or email addresses."""
    lines = [f"Residential child care report summary, {categories['inspection_type'] or 'inspection'}, {report_date}",
             f"Provider: {categories['provider']}"]
    if categories.get("license_type"):
        lines.append(f"Licence type: {categories['license_type']}")
    if categories.get("license_status"):
        lines.append(f"Current status of licence: {categories['license_status']}")
    for site in categories["sites"]:
        bits = [site["name"]]
        if site.get("licence"):
            bits.append(f"licence {site['licence']}")
        if "capacity" in site:
            bits.append(f"capacity {site['capacity']}")
        if site.get("date"):
            bits.append(f"inspected {site['date']}")
        if site.get("note"):
            bits.append(site["note"])
        lines.append("Site: " + ", ".join(bits))
    for block in ("safety", "other", "unrated"):
        items = categories[f"{block}_citations"]
        if not items:
            continue
        lines.append("")
        lines.append(BLOCK_TITLES[block] + ":")
        for item in items:
            head = " ".join(v for v in (item["citation"], f"({item['site']})" if item["site"] else "") if v)
            tail = f" [{item['status']}]" if item["status"] else ""
            lines.append(f"{head}: {item['comment']}{tail}" if head else f"{item['comment']}{tail}")
    if not categories["citation_count"]:
        lines.append("")
        lines.append("No COMAR violations.")
    return "\n".join(lines)


# Things that must not be public (README, "Only licensing reports are posted").
PRIVACY_CHECKS = [
    ("email address", re.compile(r"@")),
    ("date of birth", re.compile(r"\bD\.?O\.?B\b|date\s+of\s+birth|\bborn\s+on\b", re.I)),
    ("record number", re.compile(
        r"\b\d{3}-\d{2}-\d{4}\b|\b(?:case|client|record|medicaid|medical\s+assistance|ssn|social\s+security)\s*"
        r"(?:#|no\.?|number|id)\s*:?\s*[A-Z]*\d{3,}", re.I)),
    # A young person named, or pointed to by initials: "S.L.'s prescription",
    # "youth JB", "in the record (IB)".
    ("initials of a person", re.compile(r"(?<![A-Za-z.])[A-Z]\.\s?[A-Z]\.(?:\s?[A-Z]\.)?(?![A-Za-z])")),
    ("initials of a person", re.compile(r"\((?!(?:CAP|MAR|CPR|CPS|DSS|DJS|DHS|DHR|TB|PPD|ISP|ITP|IEP|PRN|CJIS|FBI|GAP|JCT|MDB|RCC|TGH|ALU|RVP|OTC|MVA|RCYCP|CMT|CES|DETP|POC|N/?A)\))[A-Z]{2,3}\)")),
    ("a young person identified", re.compile(
        r"\b(?:youth|child|resident|client|minor)(?:'s)?\s+(?:[A-Z]{2,3}\b(?<!CPR)(?<!MAR)(?<!TB)(?<!ISP)(?<!ID)|[A-Z]\.[A-Z]\b)")),
]
NOT_INITIALS = re.compile(r"^(?:U\.\s?S\.|P\.\s?O\.|D\.\s?C\.|M\.\s?D\.|R\.\s?N\.|A\.\s?M\.|P\.\s?M\.|N\.\s?A\.|E\.\s?G\.|I\.\s?E\.|I\.\s?D\.|J\.\s?S\.|L\.\s?P\.\s?N\.)$", re.I)


def privacy_hits(text: str) -> List[str]:
    hits = []
    for name, rx in PRIVACY_CHECKS:
        for match in rx.finditer(text):
            found = match.group(0)
            if name == "initials of a person" and NOT_INITIALS.match(found.strip()):
                continue
            around = text[max(0, match.start() - 30): match.end() + 30].replace("\n", " ")
            hits.append(f"{name}: ...{around}...")
            break
    return hits


def build_report(parsed: Dict[str, Any], path: str, archive_name: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """(report, problem). problem is '' or why the document is left out."""
    if not parsed.get("is_report"):
        return None, "not_a_report"
    if not parsed.get("found_citation_table"):
        return None, "unparsed: no citation table found"
    sites = [tidy_site(s) for s in parsed["sites"]]
    visit_dates = sorted(s["date"] for s in sites if s.get("date"))
    today = datetime.now().strftime("%Y-%m-%d")
    visit_dates = [d for d in visit_dates if d <= today]
    signed = sorted(d for d in parsed.get("signed_dates") or [] if d <= today)
    if visit_dates:
        report_date, date_source = visit_dates[-1], "site"
        # A typed year that is off ("3/14/02" for 2022) shows against the signatures.
        if signed and abs(int(report_date[:4]) - int(signed[0][:4])) > 1:
            report_date, date_source = signed[0], "signed"
    elif signed:
        report_date, date_source = signed[0], "signed"
    else:
        return None, "unparsed: no inspection date"
    provider = parsed.get("provider") or ""
    if not provider:
        return None, "unparsed: no provider organization"

    blocks = {name: [dict(item) for item in parsed[name]] for name in ("safety", "other", "unrated")}
    count = sum(len(items) for items in blocks.values())
    cap_count = sum(1 for items in blocks.values() for item in items if re.search(r"\bCAP\b", item["status"], re.I))
    raw_type = clean(parsed.get("inspection_type") or "")
    kind = inspection_type(raw_type)
    census = {}
    for key in ("dhs", "djs", "other"):
        values = [s[key] for s in sites if key in s]
        if values:
            census[key] = sum(values)
    form = parsed["form"]
    if form == "summary" and (parsed["has_status"] or parsed["has_safety_block"]):
        form = "summary-2021"
    categories: Dict[str, Any] = {
        "provider": provider,
        "inspection_type": kind,
        "license_type": clean(parsed.get("license_type") or ""),
        "license_status": tidy_status(parsed.get("license_status") or ""),
        "contracting_agency": clean(parsed.get("contracting_agency") or ""),
        "licensing_agency": clean(parsed.get("licensing_agency") or ""),
        "form": form,
        "sites": sites,
        "census": census,
        "safety_citations": blocks["safety"],
        "other_citations": blocks["other"],
        "unrated_citations": blocks["unrated"],
        "safety_count": len(blocks["safety"]),
        "other_count": len(blocks["other"]),
        "unrated_count": len(blocks["unrated"]),
        "citation_count": count,
        "cap_count": cap_count,
        "file_name": path.rsplit("/", 1)[-1],
    }
    if raw_type and squash(raw_type) != squash(kind):
        categories["inspection_type_text"] = raw_type
    if date_source != "site":
        categories["date_source"] = date_source
    if parsed.get("ocr"):
        categories["ocr"] = True
    if archive_name != categories["file_name"]:
        categories["archive_name"] = archive_name
    if parsed["form"] == "2019":
        if parsed.get("cap_checked"):
            categories["cap_required"] = parsed["cap_checked"] == "Yes"
        if parsed.get("cap_date"):
            categories["cap_date"] = parsed["cap_date"]
        if parsed.get("violation_checked") == "Yes" and not count:
            return None, "unparsed: the form ticks 'Current COMAR Violation: Yes' and no citation was read"

    label = f"{kind} inspection" if kind and not re.search(r"inspection|evaluation|conference|report", kind, re.I) else (kind or "Inspection")
    if count:
        summary = f"{label}: {plural(count, 'citation')}"
        if blocks["safety"]:
            summary += f", {len(blocks['safety'])} that may present safety risks"
    else:
        summary = f"{label}: no COMAR violations"
    text = flatten(categories, report_date)
    report = {
        "report_id": hashlib.sha1(path.encode("utf-8")).hexdigest()[:12],
        "report_date": report_date,
        "report_url": pdf_url(path),
        "raw_content": text,
        "content_length": len(text),
        "summary": summary,
        "categories": categories,
    }
    hits = privacy_hits(json.dumps({k: v for k, v in report.items() if k != "report_url"}, ensure_ascii=False))
    if hits:
        return None, "held: " + "; ".join(hits)
    return report, ""


def copy_signature(report: Dict[str, Any]) -> str:
    """Date, sites and citations: equal for two files holding the same report."""
    c = report["categories"]
    return json.dumps([report["report_date"], c["inspection_type"], c["sites"], c["safety_citations"],
                       c["other_citations"], c["unrated_citations"]], sort_keys=True)


def is_flagged(report: Dict[str, Any]) -> bool:
    return report["categories"]["citation_count"] > 0


# ── Facilities ───────────────────────────────────────────────────────────────


def provider_name(folder: str) -> str:
    """The state's folder name as a name: "Children_s Home, Inc, The" ->
    "The Children's Home, Inc"; "..., Inc_2E" -> "..., Inc."."""
    name = folder.replace("_2E", ".").replace("_2C", ",").replace("_26", "&")
    name = re.sub(r"(?<=[A-Za-z])_s\b", "'s", name).replace("_", " ")
    name = re.sub(r"\s+", " ", name).strip()
    moved = re.match(r"^(.*?),\s*The$", name)
    if moved:
        name = "The " + moved.group(1)
    return name


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def facility_info(folder: str, reports: List[Dict[str, Any]], administrator: str) -> Dict[str, str]:
    """One facility row per provider (see the module note on sites). The
    details come from the provider's newest report in this run."""
    newest = max(reports, key=lambda r: r["report_date"])
    categories = newest["categories"]
    sites = categories["sites"]
    capacities = [s["capacity"] for s in sites if "capacity" in s]
    capacity = ""
    if capacities:
        total, largest = sum(capacities), max(capacities)
        # A campus row that already totals its cottages is not added to them.
        capacity = str(largest if len(capacities) > 1 and largest >= total - largest else total)
    expiries = sorted(s["license_exp"] for s in sites if s.get("license_exp"))
    return {
        "facility_name": provider_name(folder),
        "program_name": "MD-" + slug(folder),
        "program_category": categories.get("license_type") or "Residential child care",
        "full_address": "",
        "phone": "",
        "bed_capacity": capacity,
        "executive_director": administrator if "@" not in administrator else "",
        "license_exp_date": expiries[-1] if expiries else "",
        "relicense_visit_date": "",
        "action": categories.get("license_status") or "",
    }


# ── Scraper ──────────────────────────────────────────────────────────────────


class MDScraper:
    def __init__(self, client: Optional[MDClient] = None, reports: Optional[ReportStore] = None):
        self.client = client or MDClient()
        self.reports = reports or report_store()
        self.stats: Counter = Counter()
        self.types: Counter = Counter()
        self.forms: Counter = Counter()
        self.left_out: List[str] = []
        self.archive_names: Dict[str, str] = {}

    def provider_files(self) -> Dict[str, List[str]]:
        """{provider folder name: [PDF paths]} for every provider with an RCC folder."""
        providers, _ = self.client.listing(REPORTS_DIR)
        if not providers:
            raise RuntimeError(f"No provider folders listed at {BROWSER_URL}?dir={REPORTS_DIR}; check the page by hand")
        self.stats["providers_listed"] = len(providers)
        out: Dict[str, List[str]] = {}
        for provider in providers:
            folders, _ = self.client.listing(provider)
            wanted = [f for f in folders if squash(f.rsplit("/", 1)[-1]) == squash(PROGRAM_FOLDER)]
            if not wanted:
                continue
            files: List[str] = []
            for folder in wanted:
                inner, found = self.client.listing(folder)
                files += found
                for deeper in inner:  # a subfolder inside RCC
                    files += self.client.listing(deeper)[1]
            name = provider.rsplit("/", 1)[-1]
            out[name] = [f for f in files if f.lower().endswith(".pdf")]
            self.stats["other_files"] += len(files) - len(out[name])
            logger.info(f"{name}: {len(out[name])} RCC reports listed")
        return out

    def archive_name(self, path: str) -> str:
        """The file's own name; with its provider in front if two providers
        ever use the same name (the archive folder is flat)."""
        base = path.rsplit("/", 1)[-1]
        key = base.lower()
        owner = self.archive_names.setdefault(key, path)
        return base if owner == path else f"{slug(path.split('/')[-3])}__{base}"

    def extraction(self, path: str, name: str) -> Optional[Dict]:
        cached = self.reports.cached_extract(name)
        if cached is not None and sum(len(p.get("words") or []) for p in cached.get("pages") or []) >= 40:
            return cached
        data = self.client.pdf(path)
        if data:
            self.reports.archive(name, data)
            self.stats["downloaded"] += 1
        else:
            data = self.reports.archived_bytes(name)
            if not data:
                return None
        with self.reports.working_copy(data, name) as copy:
            result = extract_pdf(copy)
        if result.get("text"):
            self.reports.save_extract(name, result)
        return result

    def scrape(self, seen: Dict[str, Set[str]], limit: int = 0) -> Tuple[List[Dict], Dict[str, List[str]]]:
        listed = self.provider_files()
        self.stats["providers_with_rcc"] = len(listed)
        self.stats["providers_with_files"] = sum(1 for files in listed.values() if files)
        folders = sorted(folder for folder, files in listed.items() if files)
        if limit:
            folders = folders[:limit]
        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        for index, folder in enumerate(folders, start=1):
            logger.info(f"[{index}/{len(folders)}] {folder}")
            reports: List[Dict[str, Any]] = []
            copies: Set[str] = set()
            administrator = ("", "")
            for path in sorted(listed[folder]):
                self.stats["files_listed"] += 1
                report_id = hashlib.sha1(path.encode("utf-8")).hexdigest()[:12]
                if report_id in seen.get(folder, set()):
                    continue
                name = self.archive_name(path)
                try:
                    extracted = self.extraction(path, name)
                except Exception as exc:  # a broken PDF
                    logger.warning(f"  {name}: extraction failed ({exc})")
                    extracted = None
                if not extracted or not extracted.get("text"):
                    self.stats["no_text"] += 1
                    self.left_out.append(f"{path}: no text could be read")
                    continue
                parsed = parse_report(extracted)
                report, problem = build_report(parsed, path, name)
                if parsed.get("dropped_rows"):
                    self.stats["dropped_rows"] += len(parsed["dropped_rows"])
                if report is None:
                    self.stats[problem.split(":")[0]] += 1
                    self.left_out.append(f"{path}: {problem}")
                    logger.warning(f"  left out {name}: {problem}")
                    continue
                copy_key = copy_signature(report)
                if copy_key in copies:
                    # The state posts some reports again under another file name.
                    self.stats["duplicate_copies"] += 1
                    self.left_out.append(f"{path}: duplicate copy of a report already read")
                    continue
                copies.add(copy_key)
                self.count(report, parsed)
                reports.append(report)
                if report["report_date"] >= administrator[0] and parsed.get("administrator"):
                    administrator = (report["report_date"], clean(parsed["administrator"]))
            if not reports:
                continue
            reports.sort(key=lambda r: r["report_date"], reverse=True)
            facilities.append({"facility_info": facility_info(folder, reports, administrator[1]), "reports": reports})
            new_ids[folder] = [r["report_id"] for r in reports]
        return facilities, new_ids

    def count(self, report: Dict[str, Any], parsed: Dict[str, Any]) -> None:
        categories = report["categories"]
        self.stats["reports"] += 1
        self.types[categories["inspection_type"] or "(not stated)"] += 1
        self.forms[categories["form"]] += 1
        self.stats["ocr_reports"] += bool(categories.get("ocr"))
        self.stats["multi_site_reports"] += len(categories["sites"]) > 1
        self.stats["no_site_table"] += not categories["sites"]
        self.stats["date_from_signature"] += categories.get("date_source") == "signed"
        self.stats["flagged"] += is_flagged(report)
        self.stats["with_safety_citations"] += categories["safety_count"] > 0
        for block in ("safety", "other", "unrated"):
            self.stats[f"{block}_citations"] += categories[f"{block}_count"]
        self.stats["cap_citations"] += categories["cap_count"]
        for block in ("safety", "other", "unrated"):
            for item in categories[f"{block}_citations"]:
                self.stats["citations_without_number"] += not item["citation"]
                self.stats["citations_without_comment"] += not item["comment"]
                self.stats["citations_naming_a_site"] += bool(item["site"])

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports)
        s = self.stats
        logger.info("── Maryland run summary ──")
        logger.info(f"provider folders: {s['providers_listed']}, with an RCC folder: {s['providers_with_rcc']}, "
                    f"with files: {s['providers_with_files']}; requests made: {self.client.requests_made}")
        logger.info(f"files listed: {s['files_listed']} (not PDFs: {s['other_files']}); downloaded: {s['downloaded']}")
        logger.info(f"facilities (one per provider): {len(facilities)}; reports: {len(reports)}; flagged: {s['flagged']}")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        logger.info(f"reports by inspection type: {dict(self.types.most_common())}")
        logger.info(f"reports by form: {dict(self.forms.most_common())}; read by OCR: {s['ocr_reports']}")
        logger.info(f"reports with several sites: {s['multi_site_reports']}; with no site table: {s['no_site_table']}; "
                    f"dated from the signatures: {s['date_from_signature']}")
        logger.info(f"citations: {s['safety_citations']} may present safety risks (in {s['with_safety_citations']} reports), "
                    f"{s['other_citations']} do not present imminent risks, {s['unrated_citations']} unrated (2019 form); "
                    f"with status CAP: {s['cap_citations']}")
        logger.info(f"citations naming a site: {s['citations_naming_a_site']}; without a COMAR number: "
                    f"{s['citations_without_number']}; without a comment: {s['citations_without_comment']}; "
                    f"table rows dropped as noise: {s['dropped_rows']}")
        logger.info(f"left out: {len(self.left_out)} (not a report: {s['not_a_report']}, unparsed: {s['unparsed']}, "
                    f"held for privacy: {s['held']}, no text: {s['no_text']}, "
                    f"duplicate copies: {s['duplicate_copies']})")
        for line in self.left_out:
            logger.warning(f"  {line}")


# ── Output ───────────────────────────────────────────────────────────────────


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = [{
        "facility_info": f["facility_info"],
        "reports": [{**r, "is_structured": True} for r in f["reports"]],
    } for f in facilities]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "MD",
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
        state="MD",
        scraped_timestamp=timestamp,
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Maryland residential child care inspection summaries")
    parser.add_argument("--full", action="store_true", help=f"Ignore the seen reports in {STATE_FILE}")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N providers (by folder name)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    timestamp = datetime.now().isoformat(timespec="seconds")

    scraper = MDScraper()
    logger.info(f"PDFs are archived in {scraper.reports.archive_dir}")
    facilities, new_ids = scraper.scrape(seen, args.limit)
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
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- state not advanced")


if __name__ == "__main__":
    main()
