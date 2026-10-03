"""
Iowa Psychiatric Medical Institution for Children (PMIC) survey scraper.

Source: the Iowa Department of Inspections, Appeals and Licensing (DIAL)
health facilities database, https://dia-hfd.iowa.gov/ (ASP.NET Core, plain
requests, no login). Every POST carries the anti-forgery token of the page
before it:

  GET  /                                   cookie + token
  POST /Home/EntityPublicAdvancedSearch    TypeVals=11 (PMIC), StateVals=0
  POST /Home/EntitySearchAjax              every field of the result page's
                                           "entitysearch" form + DataTables
                                           fields; StatusVals 153 active,
                                           155 closed
  POST /home/VisitListAjax?id=<entity>     that institution's survey visits
  GET  /Home/ViewReport?fileName=<name>    one report PDF

One institution = one facility (program_name "IA-<entity id>"); one survey
visit = one report (report_id = the visit id). The PDFs are the federal
CMS-2567 statement of deficiencies: the surveyor's findings in the left
column and the provider's plan of correction in the right. pdfplumber's
extract_text() would interleave the two, so the words are kept with their
positions and split at the form's own column rules.

Flagged = the state's own violation counts for the visit (violationsFed +
violationsState > 0), which do not depend on reading the PDF.

The state replaces a visit's file when the plan of correction arrives (the
file name carries the upload time), so the state file remembers
"<visit id>|<file name>|<fed>|<state>" and a visit whose file or counts
changed is posted again under the same report_id.
"""

import argparse
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

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
STATE_FILE = Path(os.getenv("IA_STATE_FILE", ".ia_state.json"))
REPORTS: Optional[ReportStore] = None

BASE = "https://dia-hfd.iowa.gov"
REPORT_URL = BASE + "/Home/ViewReport?{query}"
FACILITY_URL = BASE + "/Home/PublicEntityDetails?recordid={entity_id}"
ENTITY_TYPE = "11"  # Psychiatric Medical Institutions for Children
STATUSES = (("153", "Active"), ("155", "Closed"))
PROGRAM_CATEGORY = "Psychiatric Medical Institution for Children"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 0.7
FINDING_PREVIEW = 800
PLAN_PREVIEW = 400


# ── Fetch layer ──────────────────────────────────────────────────────────────


class IAClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call = 0.0
        self._result_page = ""
        self._token = ""

    def _pause(self) -> None:
        wait = REQUEST_GAP - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """One request, with retries on timeouts, connection errors and 5xx."""
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
            return response
        raise RuntimeError("unreachable")

    @staticmethod
    def _token_of(page: str) -> str:
        field = BeautifulSoup(page, "html.parser").find("input", {"name": "__RequestVerificationToken"})
        if not field or not field.get("value"):
            raise RuntimeError("No __RequestVerificationToken on the Iowa page; the site has changed")
        return field["value"]

    def start(self) -> None:
        """Home page, then the PMIC search; keeps the result page and its token."""
        home = self._request("GET", BASE + "/")
        home.raise_for_status()
        result = self._request("POST", BASE + "/Home/EntityPublicAdvancedSearch", data={
            "TypeVals": ENTITY_TYPE,
            "StateVals": "0",
            "__RequestVerificationToken": self._token_of(home.text),
        })
        result.raise_for_status()
        self._result_page = result.text
        self._token = self._token_of(result.text)

    def _search_form(self) -> Dict[str, Any]:
        """Every field of the result page's search form, as the page posts it."""
        form = BeautifulSoup(self._result_page, "html.parser").find("form", id="entitysearch")
        if form is None:
            raise RuntimeError("No 'entitysearch' form on the Iowa result page; the site has changed")
        data: Dict[str, Any] = {}
        for field in form.find_all("input"):
            if field.get("name") and field.get("type") not in ("checkbox", "radio", "submit", "button"):
                data[field["name"]] = field.get("value", "")
        for select in form.find_all("select"):
            chosen = [o.get("value") for o in select.find_all("option", selected=True)]
            if select.get("name") and chosen:
                data[select["name"]] = chosen if len(chosen) > 1 else chosen[0]
        return data

    @staticmethod
    def _datatable(length: int) -> Dict[str, str]:
        return {"draw": "1", "start": "0", "length": str(length),
                "search[value]": "", "search[regex]": "false"}

    def entities(self, status_value: str) -> List[Dict]:
        if not self._result_page:
            self.start()
        data = self._search_form()
        data.update(self._datatable(500))
        data["PublicEntitySearch.StatusVals"] = status_value
        response = self._request("POST", BASE + "/Home/EntitySearchAjax", data=data)
        response.raise_for_status()
        body = response.json()
        rows = body.get("data") or []
        total = body.get("recordsTotal")
        if isinstance(total, int) and total > len(rows):
            raise RuntimeError(f"Iowa lists {total} institutions but returned only {len(rows)}")
        return rows

    def visits(self, entity_id: Any) -> List[Dict]:
        if not self._token:
            self.start()
        data = self._datatable(1000)
        data["__RequestVerificationToken"] = self._token
        response = self._request("POST", BASE + f"/home/VisitListAjax?id={entity_id}", data=data)
        response.raise_for_status()
        return response.json().get("data") or []

    def pdf(self, file_name: str) -> Optional[bytes]:
        """The report's bytes, or None when the site does not return a PDF."""
        problem = ""
        url = REPORT_URL.format(query=urlencode({"fileName": file_name}))
        for attempt in range(3):
            if attempt:
                time.sleep(3 * attempt)
            try:
                response = self._request("GET", url)
            except requests.RequestException as exc:
                problem = f"download failed: {exc}"
                continue
            if response.status_code == 404:
                problem = "HTTP 404"
                break
            if response.status_code != 200:
                problem = f"HTTP {response.status_code}"
                continue
            if not response.content.startswith(b"%PDF"):
                problem = f"not a PDF ({response.content[:12]!r})"
                continue
            return response.content
        logger.warning(f"  {file_name}: {problem}; skipped for this run")
        return None


# ── PDF extraction (parser-independent: words with positions) ────────────────


def find_tesseract() -> str:
    for candidate in (os.getenv("TESSERACT_CMD"), r"C:\Program Files\Tesseract-OCR\tesseract.exe"):
        if candidate and Path(candidate).exists():
            return candidate
    return ""


def ocr_words(page) -> List[List[Any]]:
    """Words of a scanned page, in PDF points, read with Tesseract."""
    try:
        import pytesseract
    except ImportError:
        return []
    command = find_tesseract()
    if command:
        pytesseract.pytesseract.tesseract_cmd = command
    resolution = 200
    try:
        image = page.to_image(resolution=resolution).original
        data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)
    except Exception as exc:  # Tesseract missing or failing
        logger.warning(f"  OCR failed: {exc}")
        return []
    scale = 72.0 / resolution
    words = []
    for i, text in enumerate(data["text"]):
        text = (text or "").strip()
        if not text:
            continue
        try:
            confidence = float(data["conf"][i])
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence < 30:
            continue
        x0 = data["left"][i] * scale
        top = data["top"][i] * scale
        words.append([round(x0, 1), round(top, 1), round(x0 + data["width"][i] * scale, 1),
                      round(top + data["height"][i] * scale, 1), text])
    return words


def extract_pdf(path: Path) -> Dict:
    """Per page: its words with positions and its vertical rules. The parse
    runs from this, so a parser fix never needs the PDF again."""
    pages: List[Dict] = []
    texts: List[str] = []
    ocr_pages = 0
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            words = [[round(w["x0"], 1), round(w["top"], 1), round(w["x1"], 1), round(w["bottom"], 1), w["text"]]
                     for w in page.extract_words()]
            ocr = False
            if len(words) < 5:
                scanned = ocr_words(page)
                if scanned:
                    words, ocr = scanned, True
                    ocr_pages += 1
            vlines = [[round(l["x0"], 1), round(l["top"], 1), round(l["bottom"], 1)]
                      for l in page.lines if abs(l["x0"] - l["x1"]) < 1 and l["bottom"] - l["top"] > 40]
            vlines += [[round(r["x0"], 1), round(r["top"], 1), round(r["bottom"], 1)]
                       for r in page.rects if r["x1"] - r["x0"] < 2 and r["bottom"] - r["top"] > 40]
            pages.append({"width": round(float(page.width), 1), "height": round(float(page.height), 1),
                          "vlines": vlines, "words": words, "ocr": ocr})
            texts.append(" ".join(w[4] for w in words))
    return {"text": "\n".join(texts).strip(), "pages": pages, "ocr_pages": ocr_pages}


# ── Reading the CMS-2567 form ────────────────────────────────────────────────
#
# Body columns of the form, left to right, in PDF points on a letter page:
# tag | summary statement of deficiencies | tag again | provider's plan of
# correction | completion date. The rules between them are read from the page
# when it has them (text PDFs); a scanned page is lined up by the two "PREFIX"
# words of the column headings instead.

DEFAULT_COLUMNS = (18.0, 64.0, 291.0, 340.0, 536.0, 588.0)
DEFAULT_BODY_TOP = 183.0
DEFAULT_BODY_BOTTOM = 656.0
LEFT_PREFIX_X = 33.0
RIGHT_PREFIX_X = 305.0
PROVIDERS_X = 384.0
OCR_NOISE_RE = re.compile(r"^[|\[\]{}_~]+$")

TAG_RE = re.compile(r"^\{?\s*([A-Z])\s?([0-9O]{1,4})\s*\}?$")
# The form introduced in 2026 prints the left tag twice over: "EE0030 0030".
OVERPRINT_TAG_RE = re.compile(r"^([A-Z])\1(\d{4})\s+\2$")
TAG_LABEL_RE =re.compile(r"^\W{0,2}([A-Z])[\s-]?0?(\d{3})\b")
DATE_RE = re.compile(r"^\(?(\d{1,2})\s?[/-]\s?(\d{1,2})\s?[/-]\s?(\d{2}|\d{4})\)?[.,;]?$")
CONTINUED_RE = re.compile(r"^\S{5,12}\s+From\s+page\s*\d*\s*$", re.I)   # OCR: "Goritinued From page 3"
NOT_MET_RE = re.compile(
    r"This\s+(?:STANDARD|ELEMENT|CONDITION|REQUIREMENT|RULE)"
    r"\s+is\s+not\s+met(?:\s+as\s+evidenced\s+by)?\s*:?\s*", re.I)
CFR_RE = re.compile(r"^CFR\(s\)\s*:\s*(.*)$", re.I)
COMPLAINT_RE = re.compile(r"\b(\d{5,6}\s?-\s?[A-Z])\b")
STAMPS = {"poc", "ok", "0k", "pod"}


def normal_tag(letter: str, digits: str) -> str:
    digits = digits.replace("O", "0")
    if len(digits) == 4 and digits.startswith("0"):
        digits = digits[1:]
    return f"{letter} {digits}"


def normal_code(value: str) -> str:
    """The state's 'N140' / 'N0140' as the form writes it, 'N 140'."""
    match = re.match(r"^\s*([A-Za-z])\s?0?(\d{3})\s*$", value or "")
    return f"{match.group(1).upper()} {match.group(2)}" if match else (value or "").strip()


def cluster(values: List[float], gap: float = 3.0) -> List[float]:
    out: List[List[float]] = []
    for value in sorted(values):
        if out and value - out[-1][-1] <= gap:
            out[-1].append(value)
        else:
            out.append([value])
    return [sum(group) / len(group) for group in out]


def rule_columns(page: Dict) -> Optional[Tuple[float, ...]]:
    """The six column rules, when the page has them where the form puts them."""
    xs = cluster([v[0] for v in page.get("vlines") or [] if v[2] - v[1] > 200])
    found = []
    for want in DEFAULT_COLUMNS:
        near = [x for x in xs if abs(x - want) <= 5]
        if not near:
            return None
        found.append(min(near, key=lambda x: abs(x - want)))
    return tuple(found)


def prefix_columns(page: Dict) -> Optional[Tuple[float, ...]]:
    """Columns of a page without rules (a scan, or the 2026 form), from the
    column headings: the two 'PREFIX' words, or the left one and
    "PROVIDER'S" when OCR lost the right one."""
    upper = [w for w in page["words"] if w[1] < page["height"] * 0.4]
    marks = sorted((w for w in upper if w[4].upper().strip("().,|") in ("PREFIX", "PREEIX")), key=lambda w: w[0])
    for left in marks:
        for right in marks:
            if right[0] - left[0] > 150 and abs(right[1] - left[1]) < 12:
                scale = (right[0] - left[0]) / (RIGHT_PREFIX_X - LEFT_PREFIX_X)
                if 0.8 < scale < 1.25:
                    return tuple(left[0] + (x - LEFT_PREFIX_X) * scale for x in DEFAULT_COLUMNS)
    providers = [w for w in upper if w[4].upper().startswith("PROVIDER'S")]
    for left in marks:
        for right in providers:
            if right[0] - left[0] > 200 and 0 <= left[1] - right[1] < 20:
                scale = (right[0] - left[0]) / (PROVIDERS_X - LEFT_PREFIX_X)
                if 0.8 < scale < 1.25:
                    return tuple(left[0] + (x - LEFT_PREFIX_X) * scale for x in DEFAULT_COLUMNS)
    return None


def group_lines(words: List[List[Any]]) -> List[Dict]:
    """Words -> lines (top to bottom), each {'y', 'text', 'words'}."""
    lines: List[Dict] = []
    for word in sorted(words, key=lambda w: ((w[1] + w[3]) / 2, w[0])):
        if OCR_NOISE_RE.match(word[4]):
            continue
        y = (word[1] + word[3]) / 2
        if lines and abs(y - lines[-1]["y"]) <= 3.5:
            lines[-1]["words"].append(word)
        else:
            lines.append({"y": y, "words": [word]})
    for line in lines:
        line["words"].sort(key=lambda w: w[0])
        line["text"] = " ".join(w[4] for w in line["words"]).strip()
    return [line for line in lines if line["text"]]


def clean_text(value: str) -> str:
    value = (value or "").replace("�", "-").replace(" ", " ")
    return re.sub(r"[ \t]+", " ", value).strip()


def join_lines(lines: List[Dict]) -> str:
    """Lines -> text; a gap of more than a line and a half starts a paragraph."""
    gaps = sorted(b["y"] - a["y"] for a, b in zip(lines, lines[1:])
                  if not b.get("page_start") and 6 < b["y"] - a["y"] < 20)
    pitch = gaps[len(gaps) // 2] if gaps else 11.4
    out: List[str] = []
    for index, line in enumerate(lines):
        text = line["text"]
        if not out:
            out.append(text)
            continue
        previous = lines[index - 1]
        if line.get("page_start"):
            new_paragraph = bool(re.search(r"[.:;?!]$", previous["text"]))
        else:
            new_paragraph = line["y"] - previous["y"] > pitch * 1.45
        if new_paragraph:
            out.append(text)
        else:
            out[-1] += " " + text
    return "\n".join(clean_text(p) for p in out if p.strip())


def iso_from_us(value: str) -> str:
    match = DATE_RE.match(value.strip())
    if not match:
        return ""
    month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
    if year < 100:
        year += 2000
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def is_form_page(page: Dict, text: str) -> bool:
    if rule_columns(page) or prefix_columns(page):
        return True
    return bool(re.search(r"CMS-2567|STATEMENT OF DEFICIENCIES|SUMMARY STATEMENT", text))


def read_form(extracted: Dict) -> Dict:
    """Split every form page at its columns and follow the tags down the pages.

    Returns the surveyor's blocks ({'tag', 'lines'}), the plan-side lines and
    completion dates with the block each sits beside, the text of pages that
    are not the form (a plan of correction attached as a letter), and how each
    page's columns were found."""
    blocks: List[Dict] = []
    right: List[Tuple[Optional[int], Dict]] = []
    right_dates: List[Tuple[Optional[int], str]] = []
    attachment: List[str] = []
    layouts: Counter = Counter()
    form_pages = 0
    columns_before: Optional[Tuple[float, ...]] = None

    for page in extracted.get("pages") or []:
        words = page.get("words") or []
        if not words:
            continue
        page_text = " ".join(w[4] for w in words)
        if not is_form_page(page, page_text):
            layouts["attachment"] += 1
            attachment.append(join_lines(group_lines(words)))
            continue
        form_pages += 1
        columns = rule_columns(page)
        if columns:
            layouts["rules"] += 1
        else:
            columns = prefix_columns(page)
            if columns:
                layouts["headings"] += 1
            elif columns_before:
                columns = columns_before
                layouts["previous_page"] += 1
            else:
                columns = DEFAULT_COLUMNS
                layouts["default"] += 1
        columns_before = columns
        scale = (columns[5] - columns[0]) / (DEFAULT_COLUMNS[5] - DEFAULT_COLUMNS[0])

        # Body: below the column headings, above the signature line or footer.
        upper = [w for w in words if w[1] < page["height"] * 0.4]
        heading = [w for w in upper if w[4].upper().strip(".,") in ("DEFICIENCY)", "INFORMATION)")]
        has_heading = any(w[4].upper().strip("().,") == "PREFIX" for w in upper)
        if heading:
            top = max(w[3] for w in heading) + 2
        elif has_heading or not page.get("vlines"):
            top = DEFAULT_BODY_TOP * scale
        else:
            # A continuation sheet printed without the heading box.
            banner = [w for w in words if w[1] < 120 and w[4].upper() in ("MEDICARE", "MEDICAID", "PRINTED:")]
            top = (max(w[3] for w in banner) + 2) if banner else 48.0
        footer = [w for w in words if w[1] > page["height"] * 0.7
                  and (w[4].upper().startswith("LABORATORY") or w[4].upper().startswith("CMS-2567")
                       or re.match(r"^\(?X6\)?$", w[4].upper()))]
        bottom = (min(w[1] for w in footer) - 2) if footer else DEFAULT_BODY_BOTTOM * scale + 8

        cols: List[List[List[Any]]] = [[], [], [], [], []]
        for word in words:
            mid = (word[1] + word[3]) / 2
            if mid < top or mid > bottom:
                continue
            x = word[0]
            if x < columns[1] - 3:
                cols[0].append(word)
            elif x < columns[2] - 3:
                cols[1].append(word)
            elif x < columns[3] - 3:
                cols[2].append(word)
            elif x < columns[4] - 4:
                cols[3].append(word)
            else:
                cols[4].append(word)

        markers: List[Tuple[float, str]] = []
        for line in group_lines(cols[0]):
            match = TAG_RE.match(line["text"]) or OVERPRINT_TAG_RE.match(line["text"])
            if match:
                markers.append((line["y"], normal_tag(match.group(1), match.group(2))))
        # The column right of the summary repeats the tag, and a provider
        # typing on a scan labels the plan there. It names the plan's tag
        # (below); it opens a block only where OCR lost the left tag: a tag
        # the left column does not have, with no left tag on its row.
        left_tags = {tag for _, tag in markers}
        labels: List[Tuple[float, str]] = []
        for line in group_lines(cols[2]):
            match = TAG_RE.match(line["text"])
            if not match:
                continue
            tag = normal_tag(match.group(1), match.group(2))
            labels.append((line["y"], tag))
            if tag not in left_tags and not any(abs(y - line["y"]) < 10 for y, _ in markers):
                markers.append((line["y"], tag))
        markers.sort()

        before = len(blocks) - 1 if blocks else None   # the block running on from the page before
        starts: Dict[int, int] = {}                    # marker index -> block index

        # Left side: the surveyor's text, tag by tag. A tag never follows
        # itself, so the same tag again (the "Continued From page" row, and
        # the row under it when the tag starts the page) is the same block.
        first_on_page = True
        for line in group_lines(cols[1]):
            at = [i for i, (y, _) in enumerate(markers) if y <= line["y"] + 5]
            if at:
                marker = at[-1]
                if marker not in starts:
                    tag = markers[marker][1]
                    if not (blocks and blocks[-1]["tag"] == tag):
                        blocks.append({"tag": tag, "lines": []})
                    starts[marker] = len(blocks) - 1
                index = starts[marker]
            elif blocks:
                index = len(blocks) - 1
            else:
                blocks.append({"tag": "", "lines": []})
                index = 0
                before = 0
            if CONTINUED_RE.match(line["text"]):
                continue
            entry = {"y": line["y"], "text": line["text"]}
            if first_on_page and blocks[index]["lines"]:
                entry["page_start"] = True
            first_on_page = False
            blocks[index]["lines"].append(entry)
        # A tag whose summary text was not read still opens a block.
        for i, (_, tag) in enumerate(markers):
            if i not in starts:
                known = [n for n, b in enumerate(blocks) if b["tag"] == tag]
                if known:
                    starts[i] = known[-1]
                else:
                    blocks.append({"tag": tag, "lines": []})
                    starts[i] = len(blocks) - 1

        def block_at(y: float) -> Optional[int]:
            at = [i for i, (marker_y, _) in enumerate(markers) if marker_y <= y + 6]
            if at:
                return starts[at[-1]]
            if before is not None:
                return before
            return starts[0] if markers else None

        # Right side: the provider's plan and the completion dates.
        plan_words = list(cols[3])
        for line in group_lines(cols[2]):
            if not TAG_RE.match(line["text"]) and not re.fullmatch(r"[{}\[\]|()]*", line["text"]):
                plan_words += line["words"]
        for word in cols[4]:
            iso = iso_from_us(word[4])
            if iso:
                right_dates.append((block_at((word[1] + word[3]) / 2), iso))
            else:
                plan_words.append(word)
        first_on_page = True
        plan_lines = [l for l in group_lines(plan_words) if l["text"].lower().strip(".:") not in STAMPS]
        for line in sorted(plan_lines + [{"y": y - 4, "text": "", "label": tag} for y, tag in labels],
                           key=lambda l: l["y"]):
            entry = {"y": line["y"], "text": line["text"]}
            if line.get("label"):
                entry["label"] = line["label"]
            elif first_on_page:
                entry["page_start"] = True
                first_on_page = False
            right.append((block_at(line["y"] + (4 if line.get("label") else 0)), entry))

    return {"blocks": blocks, "right": right, "right_dates": right_dates,
            "attachment": "\n\n".join(a for a in attachment if a),
            "form_pages": form_pages, "layouts": dict(layouts)}


def split_block(lines: List[Dict]) -> Dict[str, str]:
    """A tag's text -> title, CFR citation, what the rule requires, the finding."""
    cfr_at = next((i for i, l in enumerate(lines[:8]) if CFR_RE.match(l["text"])), None)
    cfr = ""
    if cfr_at is not None:
        title_lines = lines[:cfr_at]
        cfr = clean_text(CFR_RE.match(lines[cfr_at]["text"]).group(1))
        rest = lines[cfr_at + 1:]
    else:
        # No CFR line (initial comments, state rules): the first line, and any
        # lines of capitals straight under it, are the title.
        count = 1 if lines else 0
        while (0 < count < len(lines) and count < 4
               and lines[count]["text"].upper() == lines[count]["text"]
               and lines[count]["y"] - lines[count - 1]["y"] < 16
               and not lines[count].get("page_start")):
            count += 1
        title_lines = lines[:count]
        rest = lines[count:]
    title = clean_text(" ".join(l["text"] for l in title_lines))
    body = join_lines(rest)
    not_met = NOT_MET_RE.search(body)
    if not_met:
        requirement = body[:not_met.start()].strip()
        finding = body[not_met.end():].strip()
    else:
        requirement = ""
        finding = body
    return {"title": title, "cfr": cfr, "requirement": requirement, "finding": finding}


def shorten(text: str, limit: int) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return re.sub(r"\s+\S*$", "", text[:limit]).rstrip(",;:") + "…"


def is_comments(block: Dict) -> bool:
    """'N 000' and the like: initial comments, not a deficiency."""
    if block.get("unnamed"):
        return False
    return not block["tag"] or block["tag"].endswith("000")


def split_missed_tags(blocks: List[Dict]) -> List[Dict]:
    """Where OCR lost a tag in the left column its deficiency ran on into the
    block above. A federal deficiency always has a 'CFR(s):' line under its
    title, so a second one inside a block starts a new, unnamed block at the
    lines of capitals above it."""
    out: List[Dict] = []
    for block in blocks:
        lines = block["lines"]
        cuts: List[int] = []
        own_seen = is_comments(block)   # a deficiency's first CFR line is its own
        for i, line in enumerate(lines):
            if not CFR_RE.match(line["text"]):
                continue
            if not own_seen and i < 8:
                own_seen = True
                continue
            own_seen = True
            start = i
            while (start > 0 and i - start < 4 and re.search(r"[A-Z]{3}", lines[start - 1]["text"])
                   and lines[start - 1]["text"].upper() == lines[start - 1]["text"]):
                start -= 1
            if start < i and (not cuts or start > cuts[-1]):
                cuts.append(start)
        if not cuts:
            out.append(block)
            continue
        edges = [0] + cuts + [len(lines)]
        for n, (a, b) in enumerate(zip(edges, edges[1:])):
            if n == 0:
                out.append({**block, "lines": lines[a:b]})
            else:
                out.append({"tag": "", "unnamed": True, "lines": lines[a:b], "split_from": id(block)})
    return out


def parse_report(extracted: Dict, state_codes: Optional[List[str]] = None) -> Dict:
    """Everything the page needs from one report PDF. `state_codes` is the
    state's own list of the tags cited at the visit ("N 140"), used to name a
    deficiency whose tag OCR could not read."""
    form = read_form(extracted)
    original = form["blocks"]
    blocks = split_missed_tags(original)
    # Plan lines were filed by the index of the block they sat beside; after a
    # split, beside the first piece (heights inside one block are not kept).
    first_piece: Dict[int, int] = {}
    position = 0
    for index, block in enumerate(original):
        first_piece[index] = position
        position += 1
        while position < len(blocks) and blocks[position].get("split_from") == id(block):
            position += 1

    unnamed = [b for b in blocks if b.get("unnamed")]
    named_tags = {b["tag"] for b in blocks if b["tag"]}
    spare = [c for c in (state_codes or []) if c not in named_tags]
    guessed = 0
    if unnamed and len(spare) == len(unnamed):
        # The form lists deficiencies in tag order, as the state's list does.
        for block, code in zip(unnamed, sorted(spare)):
            block["tag"] = code
            block["tag_from_state_list"] = True
            guessed += 1

    known = {b["tag"] for b in blocks if b["tag"] and not is_comments(b)}
    real = [i for i, b in enumerate(blocks) if not is_comments(b)]

    def forward(index: Optional[int]) -> Optional[int]:
        # Nothing is planned against the initial comments: text beside them
        # belongs to the deficiency that follows.
        if not real:
            return None
        if index is None:
            return real[0]
        index = first_piece.get(index, index)
        if is_comments(blocks[index]):
            later = [i for i in real if i > index]
            return later[0] if later else real[-1]
        return index

    # Plans go by height on the page, unless the plan names its tag (in the
    # tag column beside it, or "N145- All staff will ..."), which holds until
    # the next tag row.
    plan_lines: Dict[int, List[Dict]] = {}
    named: Optional[int] = None
    named_at: Optional[int] = None
    for index, line in form["right"]:
        index = forward(index)
        tag = line.get("label") or ""
        if not tag:
            label = TAG_LABEL_RE.match(line["text"])
            if label:
                tag = f"{label.group(1)} {label.group(2)}"
        if tag and tag in known:
            named = max(i for i, b in enumerate(blocks) if b["tag"] == tag)
            named_at = index
        if named is not None and index not in (named_at, named):
            named = None
        target = named if named is not None else index
        if target is not None and line["text"]:
            plan_lines.setdefault(target, []).append(line)
    dates: Dict[int, List[str]] = {}
    for index, iso in form["right_dates"]:
        index = forward(index)
        if index is not None:
            dates.setdefault(index, []).append(iso)

    intro_parts: List[str] = []
    tags: List[Dict] = []
    detail_tags: List[Dict] = []
    for index, block in enumerate(blocks):
        parts = split_block(block["lines"])
        if is_comments(block):
            if parts["title"] and not re.match(r"initial\s+comments", parts["title"], re.I):
                intro_parts.append(parts["title"])
            if parts["finding"]:
                intro_parts.append(parts["finding"])
            continue
        plan = join_lines(plan_lines.get(index, []))
        entry = {
            "tag": block["tag"],
            "regulation": " ".join(v for v in (parts["cfr"], parts["title"]) if v),
            "finding": shorten(parts["finding"], FINDING_PREVIEW),
            "plan": shorten(plan, PLAN_PREVIEW),
            "completion_date": max(dates[index]) if dates.get(index) else "",
        }
        if block.get("tag_from_state_list"):
            entry["tag_from_state_list"] = True
        tags.append(entry)
        detail_tags.append({"requirement": parts["requirement"], "finding": parts["finding"], "plan": plan})

    left_text = []
    for block in blocks:
        body = join_lines(block["lines"])
        if block["tag"] or body:
            left_text.append((block["tag"] + " " if block["tag"] else "") + body)
    survey_text = "\n\n".join(left_text).strip()
    complaints = list(dict.fromkeys(re.sub(r"\s", "", m) for m in COMPLAINT_RE.findall(survey_text)))
    return {
        "intro": "\n".join(intro_parts).strip(),
        "tags": tags,
        "detail_tags": detail_tags,
        "complaint_numbers": complaints,
        "survey_text": survey_text,
        "plan_text": "\n\n".join(join_lines(plan_lines[i]) for i in sorted(plan_lines)),
        "attachment": form["attachment"],
        "form_pages": form["form_pages"],
        "layouts": form["layouts"],
        "unnamed_tags": sum(1 for t in tags if not t["tag"]),
        "tags_named_from_state_list": guessed,
    }


# The state renumbers residents in public reports. These checks still catch
# identifiers or names that appear in the extracted text unexpectedly.
PRIVACY_CHECKS = (
    ("date of birth", re.compile(
        r"\b(?:D\.?O\.?B\.?|date\s+of\s+birth|born\s+on)\b\W{0,12}"
        r"(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", re.I)),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("record number", re.compile(
        r"\b(?:medical\s+record|MRN|client\s+ID|consumer\s+ID|case\s+number)\s*[:#]?\s*[A-Z]?\d{4,}", re.I)),
    ("possible named resident", re.compile(
        r"\b(?:Resident|Youth|Child|Patient|Student)\s+(?:named\s+)?"
        r"[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}\b")),
)


# Words after "Resident", "Child" ... that make a title, not a name
# ("Resident Case File", "Child Abuse Assessment", "Youth Service Worker").
NOT_A_NAME = {
    "abuse", "advocate", "assessment", "care", "case", "council", "counselor", "development", "family",
    "file", "files", "health", "plan", "program", "protective", "record", "records", "rights", "safety",
    "service", "services", "specialist", "treatment", "welfare", "worker", "workers", "protection",
    "saving", "institute", "immediate", "response", "full", "procedure", "intervention", "form",
}


def privacy_hits(text: str) -> List[str]:
    hits = []
    for label, pattern in PRIVACY_CHECKS:
        for match in pattern.finditer(text or ""):
            words = match.group(0).split()[-2:]
            if label == "possible named resident" and any(w.lower() in NOT_A_NAME for w in words):
                continue
            hits.append(label)
            break
    return hits


def one_line(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def report_store() -> ReportStore:
    global REPORTS
    if REPORTS is None:
        REPORTS = ReportStore("IA_PDF_CACHE", "ia_pdfs", Path(__file__).parent / "ia_pdfs")
    return REPORTS


class IAScraper:
    def __init__(self, client: Optional[IAClient] = None, reports: Optional[ReportStore] = None):
        self.client = client or IAClient()
        self.reports = reports or report_store()
        self.stats: Counter = Counter()
        self.held: List[str] = []
        self.unparsed: List[str] = []
        self.failed_downloads: List[str] = []
        self.types: Counter = Counter()
        self.parsed_tags = 0
        self.count_mismatches: List[str] = []
        self.state_zero: List[str] = []

    @staticmethod
    def facility_info(entity: Dict) -> Dict:
        address = ", ".join(one_line(entity.get(k)) for k in ("addressLine1", "addressLine2") if one_line(entity.get(k)))
        city = one_line(entity.get("city"))
        county = one_line(entity.get("county"))
        zip_code = one_line(entity.get("zip"))
        zip_code = zip_code[:5] if len(zip_code) >= 5 else zip_code
        locality = ", ".join(part for part in (city, f"{county} County" if county else "") if part)
        if locality:
            address = f"{address}, {locality}" if address else locality
        if zip_code:
            address = f"{address}, IA {zip_code}" if address else f"IA {zip_code}"
        status = one_line(entity.get("status"))
        if entity.get("dateDeleted"):
            status = f"{status} (closed {one_line(entity['dateDeleted'])})".strip()
        capacity = entity.get("capacityCount")
        try:
            capacity_text = str(int(capacity)) if capacity not in (None, "") and int(capacity) > 0 else ""
        except (TypeError, ValueError):
            capacity_text = ""
        phone = re.sub(r"\D", "", one_line(entity.get("dayPhone")))
        if len(phone) == 10:
            phone = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
        return {
            "facility_name": one_line(entity.get("name")),
            "program_name": f"IA-{entity['id']}",
            "program_category": one_line(entity.get("typeName")) or PROGRAM_CATEGORY,
            "full_address": address,
            "phone": phone,
            "bed_capacity": capacity_text,
            "executive_director": "",
            "license_exp_date": one_line(entity.get("expirationDate")),
            "relicense_visit_date": "",
            "action": status,
        }

    def build_report(self, entity: Dict, visit: Dict, extracted: Optional[Dict]) -> Dict:
        parsed = parse_report(extracted or {})
        try:
            violations_fed = max(0, int(visit.get("violationsFed") or 0))
            violations_state = max(0, int(visit.get("violationsState") or 0))
        except (TypeError, ValueError):
            violations_fed = violations_state = 0
        codes = []
        scanned_citations = visit.get("scannedCitation") or []
        if isinstance(scanned_citations, (str, dict)):
            scanned_citations = [scanned_citations]
        for code in scanned_citations:
            if isinstance(code, dict):
                code = code.get("code") or code.get("ruleCode") or code.get("citation") or ""
            code = normal_code(one_line(code))
            if code and code not in codes:
                codes.append(code)

        tags = parsed["tags"]
        self.parsed_tags += len(tags)
        total_cited = violations_fed + violations_state
        if len(tags) != total_cited and (tags or total_cited):
            self.count_mismatches.append(
                f"{entity['name']} visit {visit['id']}: state counts {total_cited}, parsed {len(tags)}")
        if parsed["form_pages"] and not tags and total_cited:
            self.unparsed.append(f"{entity['name']} visit {visit['id']}: {total_cited} violations, no tags parsed")

        body_parts = [part for part in (
            parsed["intro"],
            parsed["survey_text"],
            parsed["plan_text"],
            parsed["attachment"],
        ) if part]
        raw_content = "\n\n".join(dict.fromkeys(body_parts))
        if not raw_content and not extracted:
            raw_content = "The state lists this survey visit but has not published a report PDF."

        hits = privacy_hits(raw_content)
        if hits:
            self.held.append(f"{entity['name']} visit {visit['id']}: {', '.join(hits)}")
            return {}

        visit_type = one_line(visit.get("visitType")) or "Survey"
        # The state's counts govern, except where they say 0 and the form itself
        # cites tags (two visits on 2026-10-03): the form is the record.
        state_zero = total_cited == 0 and bool(tags)
        if state_zero:
            self.state_zero.append(f"{entity['name']} visit {visit['id']}: state counts 0, the form cites {len(tags)}")
        shown = total_cited or (len(tags) if state_zero else 0)
        flagged = shown > 0
        if flagged:
            citations = ", ".join(codes or [tag["tag"] for tag in tags if tag["tag"]])
            summary = f"{visit_type}: {shown} {'deficiency' if shown == 1 else 'deficiencies'}"
            if citations:
                summary += f" ({citations})"
        else:
            summary = f"{visit_type}: no deficiencies"

        enforcement = {
            key: one_line(visit.get(key))
            for key in ("certificationActions", "licensureActions", "fineNumber")
            if one_line(visit.get(key))
        }
        categories = {
            "visit_type": visit_type,
            "is_complaint": "complaint" in visit_type.lower(),
            "is_revisit": bool(visit.get("isRevisit")),
            "violations_fed": violations_fed,
            "violations_state": violations_state,
            "complaint_numbers": parsed["complaint_numbers"],
            "tags": tags,
            "tag_count": shown,
            "state_count_zero": state_zero,
            "enforcement": enforcement,
            "detail": {
                "intro": parsed["intro"],
                "tags": parsed["detail_tags"],
                "plan_text": parsed["plan_text"],
                "attachment": parsed["attachment"],
                "form_pages": parsed["form_pages"],
                "layouts": parsed["layouts"],
            },
        }
        return {
            "report_id": str(visit["id"]),
            "report_date": one_line(visit.get("visitDate"))[:10],
            "report_url": (REPORT_URL.format(query=urlencode({"fileName": visit["scannedReport"]}))
                           if visit.get("scannedReport") else FACILITY_URL.format(entity_id=entity["id"])),
            "raw_content": raw_content,
            "content_length": len(raw_content),
            "summary": summary,
            "categories": categories,
            "is_flagged": flagged,
            "has_text": bool(raw_content),
        }

    def fetch_report(self, entity: Dict, visit: Dict, refresh: bool = False) -> Optional[Dict]:
        file_name = one_line(visit.get("scannedReport"))
        if not file_name:
            self.stats["visits_without_pdf"] += 1
            report = self.build_report(entity, visit, None)
            return report or None
        archive_name = file_name if file_name.lower().endswith(".pdf") else f"{file_name}.pdf"
        store_name = archive_name
        if refresh:
            try:
                self.reports._extract_path(store_name).unlink()
            except FileNotFoundError:
                pass
        extracted = extract_with_cache(
            self.reports,
            store_name,
            fetch=lambda: self.client.pdf(file_name),
            extract=extract_pdf,
        )
        if not extracted or not extracted.get("pages"):
            self.failed_downloads.append(f"{entity['name']} visit {visit['id']}: {file_name}")
            return None
        parsed = parse_report(extracted)
        if not parsed["form_pages"]:
            self.stats["not_a_form"] += 1
            self.held.append(f"{entity['name']} visit {visit['id']}: PDF did not contain a readable CMS-2567 form")
            try:
                (self.reports.archive_dir / store_name).unlink()
            except FileNotFoundError:
                pass
            return None
        report = self.build_report(entity, visit, extracted)
        if not report:
            try:
                (self.reports.archive_dir / store_name).unlink()
            except FileNotFoundError:
                pass
            return None
        self.stats["reports_downloaded"] += 1
        self.stats["ocr_pages"] += extracted.get("ocr_pages", 0)
        return report

    def scrape(self, seen: Dict[str, Set[str]], limit: int = 0,
               only: Optional[Set[str]] = None, refresh: bool = False
               ) -> Tuple[List[Dict], Dict[str, List[str]]]:
        entities: Dict[str, Dict] = {}
        for status_code, _ in STATUSES:
            rows = self.client.entities(status_code)
            self.stats["entities_returned"] += len(rows)
            for entity in rows:
                entities.setdefault(str(entity["id"]), entity)

        targets = sorted(entities.values(), key=lambda row: (one_line(row.get("name")).casefold(), str(row["id"])))
        if only:
            targets = [entity for entity in targets if str(entity["id"]) in only]
        if limit:
            targets = targets[:limit]
        self.stats["entities_visited"] = len(targets)
        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        for index, entity in enumerate(targets, start=1):
            entity_id = str(entity["id"])
            visits = self.client.visits(entity_id)
            self.stats["visits_listed"] += len(visits)
            reports = []
            known = seen.get(entity_id, set())
            for visit in visits:
                report_id = str(visit["id"])
                if report_id in known and not refresh:
                    continue
                report = self.fetch_report(entity, visit, refresh=refresh)
                if report:
                    reports.append(report)
            if reports:
                reports.sort(key=lambda row: row["report_date"], reverse=True)
                facilities.append({"facility_info": self.facility_info(entity), "reports": reports})
                new_ids[entity_id] = [row["report_id"] for row in reports]
            logger.info(f"[{index}/{len(targets)}] {entity.get('name')} ({entity_id}): "
                        f"{len(visits)} visits, {len(reports)} new reports")
        return facilities, new_ids

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [report for facility in facilities for report in facility["reports"]]
        dates = sorted(report["report_date"] for report in reports if report["report_date"])
        logger.info("-- Iowa run summary --")
        logger.info(f"institutions listed: {self.stats['entities_returned']}; visited: {self.stats['entities_visited']}; "
                    f"visits listed: {self.stats['visits_listed']}")
        logger.info(f"reports: {len(reports)}; flagged by state violation count: "
                    f"{sum(1 for report in reports if report['is_flagged'])}")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        logger.info(f"visit types: {dict(Counter(r['categories']['visit_type'] for r in reports))}")
        logger.info(f"parsed tags: {self.parsed_tags}; missing PDFs: {len(self.failed_downloads)}; "
                    f"visits without PDFs: {self.stats['visits_without_pdf']}; OCR pages: {self.stats['ocr_pages']}")
        logger.info(f"reports with state/tag-count disagreement: {len(self.count_mismatches)}")
        for line in self.count_mismatches:
            logger.warning(f"  {line}")
        logger.info(f"visits flagged from the form although the state counts 0: {len(self.state_zero)}")
        for line in self.state_zero:
            logger.warning(f"  {line}")
        logger.info(f"unparsed reports: {len(self.unparsed)}")
        for line in self.unparsed:
            logger.warning(f"  {line}")
        logger.info(f"held back for review: {len(self.held)}")
        for line in self.held:
            logger.warning(f"  {line}")
        for line in self.failed_downloads:
            logger.warning(f"  download failed: {line}")


def strip_internal(facilities: List[Dict]) -> List[Dict]:
    return [{
        "facility_info": facility["facility_info"],
        "reports": [{key: value for key, value in report.items() if key != "is_flagged"}
                    for report in facility["reports"]],
    } for facility in facilities]


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    payload = {
        "total_facilities": len(facilities),
        "source_state": "IA",
        "scraped_timestamp": timestamp,
        "scraping_notes": {"total_reports": sum(len(f["reports"]) for f in facilities)},
        "facilities": [{
            "facility_info": f["facility_info"],
            "reports": [{**r, "is_structured": True} for r in f["reports"]],
        } for f in strip_internal(facilities)],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"Wrote {path}")


def save_to_api(facilities: List[Dict], timestamp: str) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="IA",
        scraped_timestamp=timestamp,
        facilities=strip_internal(facilities),
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Iowa PMIC inspection reports")
    parser.add_argument("--full", action="store_true", help=f"Ignore the seen reports in {STATE_FILE}")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-download and re-extract reports, including those already seen")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N institutions")
    parser.add_argument("--entity", action="append", default=[], help="Only this institution id (repeatable)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be zero (unlimited) or a positive integer")
    if any(not value.isdecimal() or int(value) <= 0 for value in args.entity):
        parser.error("--entity values must be positive numeric Iowa ids")

    state = load_state(STATE_FILE)
    seen = {} if args.full or args.refresh else seen_from_state(state)
    scraper = IAScraper()
    logger.info(f"PDFs are archived to {scraper.reports.archive_dir}")
    timestamp = datetime.now().isoformat(timespec="seconds")
    try:
        facilities, new_ids = scraper.scrape(
            seen=seen, limit=args.limit, only=set(args.entity) or None, refresh=args.refresh)
    except (requests.RequestException, RuntimeError, ValueError) as exc:
        logger.error(f"Iowa scrape failed: {exc}")
        raise SystemExit(1) from exc
    scraper.print_stats(facilities)
    if args.out:
        write_out(args.out, facilities, timestamp)
    if not facilities:
        if scraper.failed_downloads:
            raise SystemExit(1)
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
        raise SystemExit(1)


if __name__ == "__main__":
    main()
