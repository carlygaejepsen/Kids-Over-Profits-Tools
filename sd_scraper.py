"""
South Dakota youth care provider licensing document scraper.

Source: SD Department of Social Services, Office of Licensing and
Accreditation, public portal https://olapublic.sd.gov/youth-care-provider-search/
Plain requests, no login:

  list     GET /youth-care-provider-search/?search=true&providerType=Youth+Care
           every provider in one page, one tr.provider-search-row each
  profile  GET /youth-care-program-profile/<id>?phone=<digits>   (link as given)
           the provider's fields and a Documents section in four groups
  PDF      GET /api/mcase/attachments/<id>

Scope: Residential Treatment, Intensive Residential Treatment, Group Care,
Shelter Care and Independent Living. Child placement agencies are left out.
The page's status filter is not used: it hides operational providers.

One document = one report. The Program Certificate group is skipped. What is
posted: licensing studies (a checklist of rule sections, each item answered
Yes / No / N/A with the reviewer's comments), corrective action plans and
compliance plans (per item the rule, the finding and the correction), and the
fire, health and safety inspection forms. The portal has no complaint or
investigation documents, and nothing here is labelled as one.

A document that is not one of those kinds, or whose text carries something
that must not be public (a date of birth, a named child, a record number), is
logged, left out of the payload, removed from the archive folder and listed in
the run summary.

The list shows current providers only, so the state file keeps every profile
link ever seen and a provider that leaves the list is still visited.
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
STATE_FILE = Path(os.getenv("SD_STATE_FILE", ".sd_state.json"))
REPORTS = ReportStore("SD_PDF_CACHE", "sd_pdfs", Path(__file__).parent / "sd_pdfs")

SITE = "https://olapublic.sd.gov"
LIST_URL = SITE + "/youth-care-provider-search/"
LIST_PARAMS = {"search": "true", "providerType": "Youth Care"}
ATTACHMENT_URL = SITE + "/api/mcase/attachments/{attachment_id}"

IN_SCOPE = {
    "residential treatment",
    "intensive residential treatment",
    "group care",
    "shelter care",
    "independent living",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 0.7

# Bump when extract_pdf() changes what it returns: cached extractions of an
# older version are rebuilt from the archived PDF, not downloaded again.
EXTRACT_VERSION = 2
WORD_GAP = 0.5


# ── Fetch layer ──────────────────────────────────────────────────────────────


class SDClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call = 0.0

    def _pause(self) -> None:
        wait = REQUEST_GAP - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, url: str, **kwargs) -> requests.Response:
        """One GET with retries on timeouts, connection errors, 429 and 5xx."""
        delay = 3.0
        for attempt in range(1, 5):
            self._pause()
            try:
                response = self.session.get(url, timeout=90, **kwargs)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                if attempt == 4:
                    raise
                logger.warning(f"  {exc.__class__.__name__}; retrying in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue
            if (response.status_code >= 500 or response.status_code == 429) and attempt < 4:
                logger.warning(f"  HTTP {response.status_code}; retrying in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue
            return response
        raise RuntimeError("unreachable")

    def provider_list(self) -> str:
        response = self._request(LIST_URL, params=LIST_PARAMS)
        response.raise_for_status()
        return response.text

    def profile(self, href: str) -> str:
        response = self._request(href if href.startswith("http") else SITE + href)
        response.raise_for_status()
        return response.text

    def pdf(self, attachment_id: str) -> Optional[bytes]:
        try:
            response = self._request(ATTACHMENT_URL.format(attachment_id=attachment_id))
        except requests.RequestException as exc:
            logger.warning(f"  attachment {attachment_id}: download failed: {exc}")
            return None
        if response.status_code != 200:
            logger.warning(f"  attachment {attachment_id}: HTTP {response.status_code}")
            return None
        if not response.content.startswith(b"%PDF"):
            kind = response.headers.get("Content-Type", "")
            logger.warning(f"  attachment {attachment_id} is not a PDF ({kind}); skipped")
            return None
        return response.content


# ── HTML parsing ─────────────────────────────────────────────────────────────


def one_line(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\u00a0", " ")).strip()


def parse_list(page: str) -> List[Dict[str, str]]:
    """Every provider row: profile id, link as given, name, address, phone, category."""
    soup = BeautifulSoup(page, "html.parser")
    providers = []
    for row in soup.select("tr.provider-search-row"):
        link = row.find("a", href=re.compile(r"program-profile/\d+"))
        if not link:
            continue
        cells = [one_line(td.get_text(" ")) for td in row.find_all("td")]
        match = re.search(r"program-profile/(\d+)", link["href"])
        providers.append({
            "id": match.group(1),
            "href": link["href"],
            "name": one_line(link.get_text(" ")),
            "address": cells[1] if len(cells) > 1 else "",
            "phone": cells[2] if len(cells) > 2 else "",
            "category": cells[-1] if cells else "",
        })
    return providers


def us_date(value: str) -> str:
    """'06/08/2026' anywhere in `value` -> '2026-06-08', else ''."""
    match = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", value or "")
    if not match:
        return ""
    try:
        return datetime(int(match.group(3)), int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def parse_profile(page: str) -> Dict[str, Any]:
    """The profile's labelled fields and its documents by group."""
    soup = BeautifulSoup(page, "html.parser")
    main = soup.find("main") or soup
    fields: Dict[str, str] = {}
    for bold in main.find_all("b"):
        label = one_line(bold.get_text(" ")).rstrip(":")
        parent = bold.parent
        if not label or parent is None:
            continue
        whole = one_line(parent.get_text(" "))
        value = one_line(whole[len(one_line(bold.get_text(" "))):]) if whole.startswith(one_line(bold.get_text(" "))) else ""
        fields.setdefault(label, value)
    for label_tag in main.find_all("label"):
        label = one_line(label_tag.get_text(" ")).rstrip(":")
        value_tag = label_tag.find_next_sibling(["span", "p"])
        if label and value_tag is not None:
            fields.setdefault(label, one_line(value_tag.get_text(" ")))

    documents: List[Dict[str, str]] = []
    for link in main.find_all("a", href=re.compile(r"/attachments/\d+")):
        attachment_id = re.search(r"/attachments/(\d+)", link["href"]).group(1)
        item = link.find_parent(class_="list-group-item") or link.parent
        title_tag = item.find("h6") if item else None
        line_tag = item.find("small") if item else None
        group_tag = link.find_previous("h4")
        type_line = one_line(line_tag.get_text(" ")) if line_tag else ""
        doc_type = one_line(re.sub(r"\s*-?\s*\d{1,2}/\d{1,2}/\d{4}\s*$", "", type_line))
        documents.append({
            "id": attachment_id,
            "title": one_line(title_tag.get_text(" ")) if title_tag else "",
            "type_line": type_line,
            "doc_type": doc_type,
            "date": us_date(type_line),
            "group": one_line(group_tag.get_text(" ")) if group_tag else "",
        })
    return {"fields": fields, "documents": documents}


# ── PDF extraction ───────────────────────────────────────────────────────────


def find_tesseract() -> str:
    if os.getenv("TESSERACT_CMD"):
        return os.getenv("TESSERACT_CMD") or ""
    default = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    return str(default) if default.exists() else ""


def ocr_page(page) -> str:
    """OCR one pdfplumber page (a scanned inspection form)."""
    try:
        import pytesseract
    except ImportError:
        return ""
    tesseract = find_tesseract()
    if tesseract:
        pytesseract.pytesseract.tesseract_cmd = tesseract
    try:
        return pytesseract.image_to_string(page.to_image(resolution=200).original)
    except Exception as exc:  # tesseract missing or failing
        logger.warning(f"  OCR failed on page {page.page_number}: {exc}")
        return ""


def page_lines(page) -> List[List[Any]]:
    """The page's words grouped into lines: [[top, [[x0, x1, text], ...]], ...].

    x_tolerance 0.5: the state's PDFs set words closer together than
    pdfplumber's default of 3 allows for, which runs a line into one word
    (1 is still too wide for the fire and health form). Positions are kept
    because the 2024 study form answers with a tick under a YES or a NO
    column, and only the tick's x position says which.
    """
    words = page.extract_words(x_tolerance=WORD_GAP, y_tolerance=3, keep_blank_chars=False)
    words.sort(key=lambda w: (w["top"], w["x0"]))
    lines: List[List[Any]] = []
    for word in words:
        middle = (word["top"] + word["bottom"]) / 2
        entry = [round(word["x0"], 1), round(word["x1"], 1), word["text"]]
        if lines and abs(middle - lines[-1][0]) <= 4:
            lines[-1][1].append(entry)
        else:
            lines.append([round(middle, 1), [entry]])
    for line in lines:
        line[1].sort(key=lambda w: w[0])
    return lines


def page_tables(page) -> List[List[Any]]:
    """[[top, bottom, rows], ...]: the 2025 study form sets each section
    heading as a one-row table (number, title and rules, "Requirement Met")."""
    out: List[List[Any]] = []
    try:
        tables = page.find_tables()
    except Exception:  # a malformed page; the text is still read
        return out
    for table in tables:
        try:
            rows = table.extract(x_tolerance=WORD_GAP)
        except Exception:
            continue
        cleaned = [[one_line(cell or "") for cell in row] for row in rows]
        out.append([round(table.bbox[1], 1), round(table.bbox[3], 1), cleaned])
    return out


def extract_pdf(path: Path) -> Dict:
    """Text, word positions and tables per page; scanned pages are OCR'd."""
    pages: List[Dict[str, Any]] = []
    ocr_pages = 0
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            lines = page_lines(page)
            text = "\n".join(" ".join(w[2] for w in words) for _, words in lines)
            entry: Dict[str, Any] = {
                "width": round(float(page.width), 1),
                "lines": lines,
                "tables": page_tables(page),
            }
            if len(text.strip()) < 20:
                scanned = ocr_page(page)
                if scanned.strip():
                    entry["ocr"] = True
                    entry["lines"] = []
                    text = scanned
                    ocr_pages += 1
            entry["text"] = text
            pages.append(entry)
    text = "\n".join(p["text"] for p in pages).strip()
    return {"version": EXTRACT_VERSION, "text": text, "pages": pages, "ocr_pages": ocr_pages}


def load_extract(store: ReportStore, name: str, fetch) -> Optional[Dict]:
    """The cached extraction when it is of this version; an older one is
    rebuilt from the archived PDF, so a change to extract_pdf() never means a
    second download. Otherwise download, extract and archive."""
    cached = store.cached_extract(name)
    if cached is not None:
        if cached.get("version") == EXTRACT_VERSION:
            return cached
        data = store.archived_bytes(name)
        if data:
            with store.working_copy(data, name) as path:
                result = extract_pdf(path)
            if result.get("text"):
                store.save_extract(name, result)
            return result
        try:
            (store.extract_dir / f"{name}.json").unlink()
        except OSError:
            pass
    return extract_with_cache(store, name, fetch=fetch, extract=extract_pdf)


# ── Parsing: shared ──────────────────────────────────────────────────────────

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}
LONG_DATE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b",
    re.I,
)
SHORT_DATE = re.compile(r"\b(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{2,4})\b")
PAGE_LINE = re.compile(r"^(?:Page\s+\d+(?:\s+of\s+\d+)?|\d{1,2})$", re.I)
EMAIL = re.compile(r"\s*\(?\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\)?")

ANSWER_WORDS = {"yes": "Yes", "no": "No", "n/a": "N/A", "na": "N/A"}
TICKS = {"✓", "✔", "√", "", ""}
ITEM_LABEL = re.compile(r"^(?:([A-Z])|(\d{1,2})|([a-z]))\.$|^\(([a-z])\)$")
RULE_START = re.compile(r"(?:\d{2}:\d{2}:\d{2}|\bSDCL\b|\bARSD\b|\b42\s*C\.?F\.?R\b)")
# Wording in a section's comments that says the reviewer found a problem.
COMMENT_FLAG = re.compile(r"see\s+(?:the\s+)?corrective\s+action|compliance\s+plan", re.I)
COMMENT_HINT = re.compile(
    r"\bexcept\b|\bdid not\b|\bwere not\b|\bwas not\b|\bnot available\b|\bnot (?:contain|completed|developed)\b",
    re.I,
)
# "did not use volunteers" trips COMMENT_HINT in nearly every study; it is not a problem.
COMMENT_NO_VOLUNTEERS = re.compile(
    r"\b(?:did not|were not)\s+(?:use|utilize|used|utilized)\b[^.]*\bvolunteer|\bvolunteers\s+were\s+not\s+(?:used|utilized)\b",
    re.I,
)


def iso_date(value: str) -> str:
    """The first date in `value` as YYYY-MM-DD, or ''."""
    if not value:
        return ""
    match = LONG_DATE.search(value)
    if match:
        try:
            return datetime(int(match.group(3)), MONTHS[match.group(1).lower()],
                            int(match.group(2))).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    match = SHORT_DATE.search(value)
    if match:
        year = int(match.group(3))
        if year < 100:
            year += 2000
        try:
            return datetime(year, int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""


def doc_lines(extracted: Dict) -> List[Dict[str, Any]]:
    """Every line of the document in order, without page numbers:
    {text, words [[x0, x1, text]], width, page, top}. OCR pages have no
    word positions."""
    out: List[Dict[str, Any]] = []
    for number, page in enumerate(extracted.get("pages") or []):
        width = page.get("width") or 612.0
        if page.get("lines"):
            for top, words in page["lines"]:
                text = " ".join(w[2] for w in words)
                if PAGE_LINE.match(text.strip()):
                    continue
                out.append({"text": text, "words": words, "width": width, "page": number, "top": top})
        else:
            for raw in (page.get("text") or "").split("\n"):
                text = one_line(raw)
                if not text or PAGE_LINE.match(text):
                    continue
                out.append({"text": text, "words": [], "width": width, "page": number, "top": 0})
    return out


def clean_text(extracted: Dict) -> str:
    """The document text for raw_content: page numbers and e-mail addresses out."""
    lines = [EMAIL.sub("", line["text"]).rstrip() for line in doc_lines(extracted)]
    return "\n".join(lines).strip()


def column_answer(words: List[List[Any]]) -> Tuple[str, List[List[Any]]]:
    """A Yes / No / N/A standing in the answer column: the line's last word,
    set apart from the text before it (or alone on the line). 'on each level.
    No' at the end of a wrapped sentence is not an answer."""
    if not words:
        return "", words
    last = words[-1]
    answer = ANSWER_WORDS.get(last[2].lower())
    if not answer or last[2] in ("no", "na", "NO", "YES"):
        return "", words
    if len(words) == 1:
        return answer, []
    if last[0] - words[-2][1] >= 8:
        return answer, words[:-1]
    return "", words


def split_title_rule(value: str) -> Tuple[str, str]:
    """'Insurance - 67:42:01:35' -> ('Insurance', '67:42:01:35')."""
    value = one_line(value)
    match = RULE_START.search(value)
    if not match:
        return value.strip(" -–:"), ""
    return value[:match.start()].strip(" -–:,;"), value[match.start():].strip(" -–:")


# ── Parsing: licensing studies ───────────────────────────────────────────────


def header_boxes(page: Dict) -> List[Tuple[float, float, str, str]]:
    """(top, bottom, number, title and rules) of each section heading table."""
    boxes = []
    for top, bottom, rows in page.get("tables") or []:
        for row in rows:
            cells = [c for c in row if c]
            if len(cells) >= 2 and re.fullmatch(r"\d{1,2}\.", cells[0]) and "Requirement" in cells[-1]:
                boxes.append((top, bottom, cells[0].rstrip("."), " ".join(cells[1:-1])))
                break
    return boxes


def parse_study(extracted: Dict) -> Dict[str, Any]:
    """Sections, their items and answers, comments and the recommendation,
    from either study form.

    2025 form: each section heading is a one-row table ("N.", title and rules,
    "Requirement Met" as the column heading); items end in Yes / No / N/A.
    2024 form ("LICENSING RENEWAL STUDY"): numbered headings, YES and NO
    column headings, a tick under one of them or N/A.
    """
    lines = doc_lines(extracted)
    pages = extracted.get("pages") or []
    old_form = any(
        any(w[2] == "YES" for w in line["words"]) and any(w[2] == "NO" for w in line["words"])
        for line in lines
    )
    boxes = {number: header_boxes(page) for number, page in enumerate(pages)}
    section_x = None
    if old_form:
        starts = [line["words"][0][0] for line in lines
                  if line["words"] and re.fullmatch(r"\d{1,2}\.", line["words"][0][2])]
        section_x = min(starts) if starts else None

    sections: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    item: Optional[Dict[str, Any]] = None
    mode = "items"
    header_tail = 0
    yes_x = no_x = None
    recommendation: List[str] = []
    in_recommendation = False
    seen_boxes: Set[Tuple[int, float]] = set()

    def open_section(number: str, heading: str) -> Dict[str, Any]:
        for section in sections:
            if section["number"] == number:
                return section
        title, rule = split_title_rule(heading)
        section = {"number": number, "title": title, "rule": rule, "items": [], "comments": []}
        sections.append(section)
        return section

    for line in lines:
        words = list(line["words"])
        text = line["text"].strip()

        if in_recommendation:
            if re.match(r"^(?:Signatures?\b|Completed By)", text, re.I):
                break
            recommendation.append(text)
            continue

        # A 2025 heading table: skip its lines, open (or continue) the section.
        box = next((b for b in boxes.get(line["page"], [])
                    if b[0] - 3 <= line["top"] <= b[1] + 3), None) if words else None
        if box:
            key = (line["page"], box[0])
            if key not in seen_boxes:
                seen_boxes.add(key)
                current = open_section(box[2], box[3])
                item = None
                mode = "items"
            continue

        if re.match(r"^(?:\d{1,2}\.\s*)?Recommendations?\b", text, re.I) and len(text) < 400:
            in_recommendation = True
            rest = re.sub(r"^(?:\d{1,2}\.\s*)?Recommendations?(?:\s*/\s*Corrective Action)?\s*:?\s*", "", text, flags=re.I)
            if rest:
                recommendation.append(rest)
            continue

        if old_form and words:
            marks = [w for w in words if w[2] in ("YES", "NO")]
            if len(marks) >= 2:
                yes_x = next(((w[0] + w[1]) / 2 for w in marks if w[2] == "YES"), yes_x)
                no_x = next(((w[0] + w[1]) / 2 for w in marks if w[2] == "NO"), no_x)
                words = [w for w in words if w not in marks]
                text = " ".join(w[2] for w in words)
                if not words:
                    continue
            first = words[0]
            expected = str(int(sections[-1]["number"]) + 1) if sections else "1"
            if (re.fullmatch(r"\d{1,2}\.", first[2]) and first[2].rstrip(".") == expected
                    and section_x is not None and first[0] <= section_x + 12):
                current = open_section(expected, " ".join(w[2] for w in words[1:]))
                item = None
                mode = "items"
                header_tail = 2
                continue
            if header_tail and current is not None:
                # A heading that wraps: more rule numbers before the first item.
                if not ITEM_LABEL.match(first[2]) and not re.match(r"comments?\b", text, re.I) \
                        and RULE_START.search(text) and len(text) < 90:
                    current["rule"] = one_line(current["rule"] + " " + text)
                    header_tail -= 1
                    continue
                header_tail = 0

        if current is None:
            continue

        if re.match(r"^comments?\s*:?", text, re.I) and not text.lower().startswith("comments on"):
            mode = "comments"
            rest = re.sub(r"^comments?\s*:?\s*", "", text, flags=re.I)
            if rest:
                current["comments"].append(rest)
            continue

        label = ITEM_LABEL.match(words[0][2]) if words else None
        if mode == "comments":
            # The 2024 form puts comments between a section's lettered blocks.
            ends = bool(old_form and label and label.group(1) and section_x is not None
                        and words[0][0] <= section_x + 45)
            if not ends:
                current["comments"].append(text)
                continue
            mode = "items"

        answer = ""
        if old_form:
            if yes_x is not None and no_x is not None:
                kept = []
                for index, word in enumerate(words):
                    middle = (word[0] + word[1]) / 2
                    in_columns = yes_x - 30 <= middle <= no_x + 40
                    column = "Yes" if abs(middle - yes_x) <= abs(middle - no_x) else "No"
                    if word[2] in TICKS:
                        answer = answer or column
                    elif in_columns and word[2] in ("N/A", "NA", "n/a"):
                        answer = answer or "N/A"
                    elif (in_columns and word[2] in ("Yes", "No") and index == len(words) - 1
                          and (index == 0 or word[0] - words[index - 1][1] >= 8)):
                        answer = answer or word[2]
                    else:
                        kept.append(word)
                if (len(kept) >= 2 and kept[-2][2] == "See" and kept[-1][2].lower().startswith("comment")
                        and kept[-2][0] >= yes_x - 60):
                    answer = answer or "See comments"
                    kept = kept[:-2]
                words = kept
        else:
            answer, words = column_answer(words)
        text = " ".join(w[2] for w in words).strip()

        if label and words:
            level = 1 if label.group(1) else 2 if label.group(2) else 3
            item = {"level": level, "label": words[0][2], "text": " ".join(w[2] for w in words[1:]), "answer": ""}
            current["items"].append(item)
        elif item is not None and text:
            item["text"] = one_line(item["text"] + " " + text)
        if answer and item is not None and not item["answer"]:
            item["answer"] = answer

    not_met: List[Dict[str, Any]] = []
    answers: Counter = Counter()
    for section in sections:
        comment = one_line(" ".join(section["comments"]))
        unmet = []
        for index, entry in enumerate(section["items"]):
            answers[entry["answer"] or "(none)"] += 1
            if entry["answer"] not in ("No", "See comments"):
                continue
            parent = next((p["text"] for p in reversed(section["items"][:index])
                           if p["level"] < entry["level"]), "")
            unmet.append({"text": entry["text"], "under": parent.rstrip(":"), "answer": entry["answer"]})
        by_comment = bool(COMMENT_FLAG.search(comment))
        if unmet or by_comment:
            not_met.append({
                "section": f"{section['number']}. {section['title']}".strip(),
                "rule": section["rule"],
                "items": unmet,
                "comment": comment,
                "basis": "answer" if unmet else "comment",
            })

    text = extracted.get("text") or ""
    head = "\n".join(text.split("\n")[:8])
    program_type = ""
    match = re.search(r"STUDY\s*\n\s*(?:for\s+)?([A-Z][A-Z /,\-]+)\n", head)
    if match:
        program_type = one_line(match.group(1)).title().replace(" For ", " for ")
    visit = ""
    match = re.search(r"Date of On-?Site Visit:?[\s.]*([\d /]{6,16})", text, re.I)
    if match:
        visit = iso_date(match.group(1).replace(" ", ""))
    else:
        match = re.search(r"([A-Z][a-z]+ \d{1,2}, \d{4})\s*\n\s*Date of Site Visit", text)
        if match:
            visit = iso_date(match.group(1))
    recommendation_text = one_line(re.sub(r"^COMMENTS?\s*:?\s*", "", " ".join(recommendation), flags=re.I))
    numbers = [int(s["number"]) for s in sections]
    return {
        "form": "2024" if old_form else "2025",
        "program_type": program_type,
        "site_visit_date": visit,
        "section_count": len(sections),
        "not_met": not_met,
        "not_met_count": len(not_met),
        "recommendation": recommendation_text,
        "_answers": dict(answers),
        "_contiguous": numbers == list(range(1, len(numbers) + 1)) and bool(numbers),
        "_hints": [
            f"{s['number']}. {s['title']}: {one_line(' '.join(s['comments']))[:260]}"
            for s in sections
            if COMMENT_HINT.search(COMMENT_NO_VOLUNTEERS.sub("", " ".join(s["comments"])))
            and not COMMENT_FLAG.search(" ".join(s["comments"]))
            and not any(i["answer"] in ("No", "See comments") for i in s["items"])
        ],
    }


# ── Parsing: corrective action and compliance plans ──────────────────────────

RULE_CODE = re.compile(r"\b\d{2}:\d{2}:\d{2}:\d{2}(?:\.\d+)?|\b\d{2}-\d{1,2}-\d{1,2}(?:\.\d+)?(?=\D)")
PLAN_LABELS = [
    ("rule", r"Administrative Rule"),
    ("finding", r"Summary of Non-Compliance Finding"),
    ("corrective_action", r"Corrective Action"),
    ("corrections_needed", r"Corrections to be Made"),
    ("corrective_action", r"Corrections Made"),
    ("evidence", r"Supporting Evidence"),
    ("maintained", r"How Maintained"),
]
PLAN_STOP = re.compile(
    r"^(?:Position Responsible:|Anticipated Completion Date:|SIGNATURES\b|Your signature below)", re.I)


def between(text: str, start: str, stops: List[str]) -> str:
    match = re.search(start, text, re.I)
    if not match:
        return ""
    rest = text[match.end():]
    end = len(rest)
    for stop in stops:
        found = re.search(stop, rest, re.I)
        if found and found.start() < end:
            end = found.start()
    return rest[:end].strip()


def paragraphs(value: str) -> str:
    """Hard-wrapped lines joined; blank lines kept as paragraph breaks."""
    parts = [one_line(p) for p in re.split(r"\n\s*\n", value or "")]
    return "\n".join(p for p in parts if p)


def parse_plan(extracted: Dict) -> Dict[str, Any]:
    if extracted.get("ocr_pages"):
        text = "\n".join(l for l in (extracted.get("text") or "").split("\n") if not PAGE_LINE.match(l.strip()))
    else:
        text = "\n".join(line["text"] for line in doc_lines(extracted))
    head = text[:400]
    plan_type = "compliance" if re.search(r"COMPLIANCE PLAN", head[:60], re.I) else "corrective_action"
    out: Dict[str, Any] = {"plan_type": plan_type, "status": "", "date_issued": "", "completion_date": "", "items": []}

    issued = re.search(r"Date Issued\s+(.+?)\s+Status\s+(.+)", text)
    if issued:
        out["form"] = "2025"
        out["date_issued"] = iso_date(issued.group(1))
        out["status"] = one_line(issued.group(2))
        done = re.search(r"COMPLETION DATE:\s*(.+)", text)
        out["completion_date"] = iso_date(done.group(1)) if done else ""
        chunks = re.split(r"(?m)^(?:Corrective Action Plan|Compliance Plan Action)\s*#\s*\d+\s*$", text)[1:]
        for chunk in chunks:
            fields: Dict[str, List[str]] = {}
            key = None
            for line in chunk.split("\n"):
                stripped = line.strip()
                if PLAN_STOP.match(stripped):
                    break
                matched = False
                for name, label in PLAN_LABELS:
                    found = re.match(rf"^{label}:\s*(.*)$", stripped)
                    if found:
                        key = name
                        fields.setdefault(key, [])
                        if found.group(1):
                            fields[key].append(found.group(1))
                        matched = True
                        break
                if not matched and key:
                    fields[key].append(stripped)
            rule_lines = fields.get("rule") or []
            code = rule_lines[0] if rule_lines and RULE_CODE.search(rule_lines[0] + " ") and len(rule_lines[0]) < 40 else ""
            out["items"].append({
                "rule": code,
                "rule_text": one_line(" ".join(rule_lines[1:] if code else rule_lines)),
                "finding": one_line(" ".join(fields.get("finding") or [])),
                "action_needed": one_line(" ".join(fields.get("corrections_needed") or [])),
                "corrective_action": one_line(" ".join(fields.get("corrective_action") or [])),
                "evidence": one_line(" ".join(fields.get("evidence") or [])),
                "maintained": one_line(" ".join(fields.get("maintained") or [])),
            })
    else:
        # The 2024 form: one block of rules, one finding, one plan.
        out["form"] = "2024"
        rules = between(text, r"following\s+Administrative\s+Rules?\s+of\s+South\s+Dakota\s*:?", [r"Non-?Compliance Finding\s*:"])
        finding = between(text, r"Non-?Compliance Finding\s*:[ .]*", [r"Action Needed\s*:"])
        needed = between(text, r"Action Needed\s*:", [r"Submit plan by\s*:"])
        plan = between(text, r"Corrective Action Plan \(Attach documents if needed\)\s*:",
                       [r"Date Corrective Action Plan Implemented", r"Date of Expected Completion",
                        r"Your signature below"])
        codes = []
        for line in rules.split("\n"):
            found = RULE_CODE.match(line.strip() + " ")
            if found and found.group(0) not in codes:
                codes.append(found.group(0))
        if finding or rules:
            out["items"].append({
                "rule": ", ".join(codes),
                "rule_text": paragraphs(rules),
                "finding": paragraphs(finding),
                "action_needed": paragraphs(needed),
                "corrective_action": paragraphs(plan),
                "evidence": "",
                "maintained": "",
            })
    out["item_count"] = len(out["items"])
    return out


# ── Parsing: inspections ─────────────────────────────────────────────────────

ALL_MET = re.compile(r"All applicable requirements of this inspection have been met\s*:?\s*(Yes|No)?", re.I)
ROMAN_HEADING = re.compile(r"^([IVX]{1,4})\.\s+([A-Z][A-Za-z &/,\-]+?)(?:\s*\(ARSD.*)?(?:\s*Requirement\s*Met)?$")


def parse_inspection(extracted: Dict) -> Dict[str, Any]:
    """Fire, health and food service inspection forms. The typed form (2025
    on) answers each item Yes / No / N/A; the 2024 forms are scans filled in by
    hand, whose ticks cannot be read."""
    pages = extracted.get("pages") or []
    scanned = bool(pages) and (extracted.get("ocr_pages") or 0) * 2 >= len(pages)
    text = extracted.get("text") or ""
    out: Dict[str, Any] = {
        "scanned": scanned, "failed": [], "failed_count": 0, "comments": [],
        "all_met": "", "result": "unread", "inspection_date": "", "answered": 0,
    }
    if scanned:
        return out
    match = re.search(r"Date of Inspection\s+([A-Z][a-z]+ \d{1,2}, \d{4}|\d{1,2}/\d{1,2}/\d{4})", text)
    if match:
        out["inspection_date"] = iso_date(match.group(1))
    lines = doc_lines(extracted)
    part = ""
    section = ""
    item: Optional[Dict[str, str]] = None
    comment: Optional[List[str]] = None
    answers: Counter = Counter()
    # An answer read from the end of a question's first line; undone when the
    # next line carries on the sentence ("on each level. No / more than 75 feet").
    tentative: Optional[Dict[str, str]] = None

    def close_comment() -> None:
        nonlocal comment
        if comment is not None:
            body = one_line(" ".join(comment))
            if body and body.lower() not in ("none", "n/a", "na"):
                out["comments"].append({"section": section or part, "text": body})
        comment = None

    for line in lines:
        words = list(line["words"])
        text_line = line["text"].strip()
        if not words:
            continue
        met = ALL_MET.search(text_line)
        if met:
            close_comment()
            out["all_met"] = (met.group(1) or "").title()
            item = None
            continue
        if text_line in ("FIRE & LIFE SAFETY", "ENVIRONMENTAL HEALTH", "FOOD SERVICE"):
            close_comment()
            part = text_line.title().replace("&", "and")
            item = None
            continue
        heading = ROMAN_HEADING.match(text_line)
        if heading and heading.group(2).upper() == heading.group(2):
            close_comment()
            section = f"{part}: {one_line(heading.group(2)).title()}" if part else one_line(heading.group(2)).title()
            item = None
            continue
        if re.match(r"^COMMENTS\b:?", text_line):
            close_comment()
            comment = [re.sub(r"^COMMENTS\b:?\s*", "", text_line)]
            item = None
            continue
        if re.match(r"^(?:Recommendations|Signature of|Other\b)", text_line):
            close_comment()
            item = None
            continue
        if tentative is not None:
            if words[0][2][:1].islower() and tentative is item:
                answers[item["answer"]] -= 1
                if item["answer"] == "No" and out["failed"] and out["failed"][-1] is item:
                    out["failed"].pop()
                item["text"] = one_line(item["text"] + " " + item["answer"])
                item["answer"] = ""
            tentative = None
        answer, rest = column_answer(words)
        labelled = bool(rest) and bool(re.fullmatch(r"\d{1,2}[a-z]?\.|\([a-z]\)", rest[0][2]))
        if not answer and labelled and len(rest) > 2 and rest[-1][2] in ("Yes", "No", "N/A"):
            # The portal's own export sets the answer straight after the question's first line.
            answer, rest = rest[-1][2], rest[:-1]
            from_export = True
        else:
            from_export = False
        if comment is not None and not labelled and not answer:
            comment.append(text_line)
            continue
        if labelled:
            close_comment()
            item = {"text": " ".join(w[2] for w in rest[1:]), "answer": ""}
        elif item is not None and rest:
            item["text"] = one_line(item["text"] + " " + " ".join(w[2] for w in rest))
        if answer and item is not None and not item["answer"]:
            item["answer"] = answer
            answers[answer] += 1
            if from_export:
                tentative = item
            if answer == "No":
                item["section"] = section or part
                out["failed"].append(item)
    close_comment()
    out["failed"] = [{"section": f["section"], "text": one_line(f["text"])} for f in out["failed"]]
    out["failed_count"] = len(out["failed"])
    out["answered"] = sum(answers.values())
    if out["answered"] >= 10:
        # The "all applicable requirements met" box reads No on every typed
        # form, even with every item Yes, so only items answered No count.
        out["result"] = "failed_items" if out["failed"] else "all_met"
    return out


# ── What a document is, and what must stay out ───────────────────────────────

KIND_LABELS = {
    "licensing_study": "Licensing study",
    "corrective_action_plan": "Corrective action plan",
    "inspection": "Fire and health inspection",
    "other": "Document",
}


def classify(text: str) -> str:
    """The document's kind from its own text; '' when it is none of the kinds
    this scraper posts."""
    head = text[:900]
    if re.search(r"LICENS(?:E|ING)\s+(?:RENEWAL\s+)?STUDY", head):
        return "licensing_study"
    if re.search(r"Corrective\s+Action\s+Plan|COMPLIANCE\s+PLAN", head[:300], re.I) \
            and not re.search(r"INSPECTION\s+FORM", head[:300]):
        return "corrective_action_plan"
    if re.search(r"INSPECTION\s+FORM|Inspection\s*\|\s*Fire-Health", head) \
            or re.search(r"FIRE\s*&\s*LIFE\s+SAFETY|fire escape plans are posted", text, re.I):
        return "inspection"
    return ""


PRIVATE_PATTERNS = [
    ("date of birth", re.compile(
        r"\b(?:D\.?O\.?B\.?|date\s+of\s+birth|birth\s*date)\b\W{0,6}"
        r"(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[A-Z][a-z]+\s+\d{1,2},\s+\d{4})", re.I)),
    ("named child", re.compile(
        r"\b(?:[Rr]esident|[Cc]hild|[Yy]outh|[Cc]lient|[Ss]tudent|[Mm]inor|[Pp]atient)(?:'s|’s)?\s+[Nn]ame\s*[:\-]\s*"
        r"[A-Z][a-z]+\s+[A-Z][a-z]+")),
    ("record number", re.compile(
        r"\b(?:medicaid|case|client|resident|medical\s+record|record|chart|FACIS)\s*(?:number|no\.?|#|id)\s*[:#]?\s*[A-Z]{0,3}\d{5,}",
        re.I)),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
]


def private_hits(text: str) -> List[str]:
    """What in the text must not be public; an empty list when nothing."""
    hits = []
    for name, pattern in PRIVATE_PATTERNS:
        match = pattern.search(text)
        if match:
            hits.append(f"{name}: {one_line(text[max(0, match.start() - 40): match.end() + 20])}")
    return hits


NAME_STOP = {"the", "of", "and", "for", "inc", "llc", "at", "program", "center", "home", "services",
             "youth", "residential", "treatment", "ilpp", "lss", "house"}
NAME_PATTERNS = [
    re.compile(r"(?:AGENCY\s+)?NAME:\s*(.+)", re.I),
    re.compile(r"Provider(?:'s|’s)?\s+Name\s+(.+?)(?:\s+Provider\s+Number.*)?$", re.I | re.M),
    re.compile(r"Program Name\s+(.+)", re.I),
]


def name_tokens(value: str) -> Set[str]:
    return {t for t in re.findall(r"[a-z]+", value.lower().replace("’", "").replace("'", ""))
            if len(t) > 2 and t not in NAME_STOP}


def named_program(text: str, facility_name: str) -> str:
    """The program a typed form names, when it shares no word with the
    facility the state filed it under; '' otherwise."""
    head = text[:900]
    for pattern in NAME_PATTERNS:
        match = pattern.search(head)
        if not match:
            continue
        named = one_line(match.group(1))
        named = re.sub(r"\s*\(R[^)]*\)?.*$|\s+R\d+.*$|\s+Category\s.*$", "", named).strip(" .")
        if not named:
            continue
        mine, theirs = name_tokens(facility_name), name_tokens(named)
        initials = "".join(w[0] for w in re.findall(r"[A-Za-z]+", facility_name)).lower()
        if theirs and mine and not (mine & theirs) and not any(t in initials or initials.startswith(t) for t in theirs) \
                and not any(t in facility_name.lower() for t in theirs):
            return named
        return ""
    return ""


def summarize(categories: Dict[str, Any]) -> str:
    kind = categories["kind"]
    if kind == "licensing_study":
        count = categories["not_met_count"]
        if count:
            return f"Licensing study: {count} section{'s' if count != 1 else ''} with a problem noted"
        return "Licensing study: no problems noted"
    if kind == "corrective_action_plan":
        label = "Compliance plan" if categories.get("plan_type") == "compliance" else "Corrective action plan"
        count = categories["item_count"]
        return f"{label}: {count} finding{'s' if count != 1 else ''}" if count else label
    if kind == "inspection":
        if categories.get("result") == "failed_items":
            count = categories["failed_count"]
            if count:
                return f"Fire and health inspection: {count} item{'s' if count != 1 else ''} not met"
            return "Fire and health inspection: not all requirements met"
        if categories.get("result") == "all_met":
            return "Fire and health inspection: all applicable requirements met"
        return "Fire and health inspection (scanned form)"
    return "Document"


def is_flagged(categories: Dict[str, Any]) -> bool:
    kind = categories["kind"]
    if kind == "corrective_action_plan":
        return True
    if kind == "licensing_study":
        return categories["not_met_count"] > 0
    if kind == "inspection":
        return categories.get("result") == "failed_items"
    return False


# ── Scraper ──────────────────────────────────────────────────────────────────

SKIP_GROUPS = {"program certificate"}


def format_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return one_line(value)


class SDScraper:
    def __init__(self, client: Optional[SDClient] = None, reports: ReportStore = REPORTS):
        self.client = client or SDClient()
        self.reports = reports
        self.stats: Counter = Counter()
        self.held_back: List[str] = []
        self.notes: List[str] = []
        self.inspection_titles: Counter = Counter()

    def hold_back(self, name: str, why: str) -> None:
        """Keep a document out of the payload and out of the archive folder
        (the nightly sync would otherwise put the copy on the site)."""
        self.held_back.append(why)
        logger.warning(f"  held back: {why}")
        try:
            (self.reports.archive_dir / name).unlink()
        except OSError:
            pass

    def build_report(self, provider_name: str, doc: Dict[str, str]) -> Optional[Dict]:
        name = f"{doc['id']}.pdf"
        label = f"{provider_name}, {doc['type_line'] or doc['title']} (attachment {doc['id']})"
        extracted = load_extract(self.reports, name, fetch=lambda: self.client.pdf(doc["id"]))
        if not extracted or not (extracted.get("text") or "").strip():
            self.stats["no_text"] += 1
            self.notes.append(f"{label}: no text could be read; not posted")
            return None
        scanned = bool(extracted.get("ocr_pages"))
        text = EMAIL.sub("", extracted["text"]) if scanned else clean_text(extracted)
        kind = classify(text)
        if not kind:
            self.stats["not_a_report"] += 1
            self.hold_back(name, f"{label} is not a study, plan or inspection form")
            return None
        hits = private_hits(text)
        if hits:
            self.stats["private"] += 1
            self.hold_back(name, f"{label} carries {'; '.join(hits)}")
            return None

        categories: Dict[str, Any] = {
            "kind": kind,
            "title": doc["title"],
            "group": doc["group"],
            "doc_type": doc["doc_type"],
            "archive_name": name,
        }
        if kind == "licensing_study":
            parsed = parse_study(extracted)
            contiguous = parsed.pop("_contiguous")
            if not parsed["section_count"]:
                self.stats["study_unparsed"] += 1
                self.notes.append(f"{label}: no sections could be read")
            elif not contiguous:
                self.notes.append(f"{label}: section numbers are not 1..{parsed['section_count']}")
            for answer, count in parsed.pop("_answers").items():
                self.stats[f"answer {answer}"] += count
            for hint in parsed.pop("_hints"):
                self.notes.append(f"{label}: comment worth a look, not flagged: {hint}")
            if parsed["not_met_count"]:
                self.stats["studies_flagged"] += 1
                self.stats["study_sections_flagged"] += parsed["not_met_count"]
            categories.update(parsed)
        elif kind == "corrective_action_plan":
            parsed = parse_plan(extracted)
            self.stats["plan_items"] += parsed["item_count"]
            if not parsed["item_count"]:
                self.stats["plan_unparsed"] += 1
                self.notes.append(f"{label}: no plan items could be read")
            categories.update(parsed)
        else:
            parsed = parse_inspection(extracted)
            parsed.pop("answered", None)
            self.inspection_titles[f"{doc['doc_type'] or doc['title']} | "
                                   f"{'scanned' if parsed['scanned'] else 'typed'} | {parsed['result']}"] += 1
            categories.update(parsed)
        if not scanned:
            other = named_program(text, provider_name)
            if other:
                categories["named_program"] = other
                self.notes.append(f"{label}: the form names another program, {other}")
        group = doc["group"].lower()
        expected = "corrective_action_plan" if group.startswith("compliance plans") else \
            {"licensing studies": "licensing_study", "inspections": "inspection"}.get(group)
        if expected != kind:
            self.notes.append(f"{label}: filed under '{doc['group']}' but reads as {kind}")

        self.stats[kind] += 1
        report_date = doc["date"] or categories.get("site_visit_date") or categories.get("date_issued") \
            or categories.get("inspection_date") or ""
        if not report_date:
            self.notes.append(f"{label}: no date")
        return {
            "report_id": doc["id"],
            "report_date": report_date,
            "report_url": ATTACHMENT_URL.format(attachment_id=doc["id"]),
            "raw_content": text,
            "content_length": len(text),
            "summary": summarize(categories),
            "categories": categories,
            "is_flagged": is_flagged(categories),
        }

    @staticmethod
    def facility_info(provider: Dict[str, str], fields: Dict[str, str], listed: bool) -> Dict[str, str]:
        status = fields.get("Status") or ""
        if not listed:
            status = f"No longer listed by the state{f' (last status: {status})' if status else ''}"
        address = re.sub(r",\s*USA$", "", fields.get("Physical Address") or provider.get("address") or "")
        return {
            "facility_name": fields.get("Program Name") or provider.get("name") or "",
            "program_name": f"SD-{provider['id']}",
            "program_category": fields.get("Program Category") or provider.get("category") or "",
            "full_address": address,
            "phone": format_phone(fields.get("Phone Number") or provider.get("phone") or ""),
            "bed_capacity": fields.get("Total Capacity") or "",
            "executive_director": "",
            "license_exp_date": "",
            "relicense_visit_date": "",
            "action": status,
        }

    def scrape(
        self,
        seen: Dict[str, Set[str]],
        known: Dict[str, Dict],
        limit: int = 0,
    ) -> Tuple[List[Dict], Dict[str, List[str]], Dict[str, Dict]]:
        listed = parse_list(self.client.provider_list())
        self.stats["providers_listed"] = len(listed)
        for provider in listed:
            self.stats[f"listed: {provider['category']}"] += 1
        in_scope = [p for p in listed if p["category"].lower() in IN_SCOPE]
        logger.info(f"{len(listed)} providers listed, {len(in_scope)} in scope")
        providers: List[Tuple[Dict, bool]] = [(p, True) for p in in_scope]
        listed_ids = {p["id"] for p in listed}
        # Providers that left the list: their profile may still answer.
        for provider_id, stored in sorted(known.items()):
            if provider_id not in listed_ids and stored.get("href"):
                providers.append(({**stored, "id": provider_id}, False))
        if limit:
            providers = providers[:limit]

        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        registry: Dict[str, Dict] = {}
        for index, (provider, is_listed) in enumerate(providers, start=1):
            logger.info(f"[{index}/{len(providers)}] {provider['name']} ({provider['id']})"
                        + ("" if is_listed else " [no longer listed]"))
            try:
                profile = parse_profile(self.client.profile(provider["href"]))
            except requests.RequestException as exc:
                logger.error(f"  profile failed: {exc}")
                self.notes.append(f"{provider['name']}: profile could not be fetched ({exc})")
                continue
            fields = profile["fields"]
            category = fields.get("Program Category") or provider.get("category") or ""
            if category.lower() not in IN_SCOPE:
                continue
            registry[provider["id"]] = {
                "href": provider["href"],
                "name": fields.get("Program Name") or provider.get("name") or "",
                "category": category,
                "address": provider.get("address") or "",
                "phone": provider.get("phone") or "",
                "last_listed": datetime.now().strftime("%Y-%m-%d") if is_listed else provider.get("last_listed", ""),
            }
            self.stats["providers_visited"] += 1
            info = self.facility_info(provider, fields, is_listed)
            already = seen.get(provider["id"], set())
            reports: List[Dict] = []
            for doc in profile["documents"]:
                self.stats[f"listed in group: {doc['group']}"] += 1
                if doc["group"].lower() in SKIP_GROUPS or doc["id"] in already:
                    continue
                report = self.build_report(info["facility_name"], doc)
                if report:
                    reports.append(report)
            if not profile["documents"]:
                self.notes.append(f"{info['facility_name']}: no documents on the profile")
            if not reports:
                continue
            reports.sort(key=lambda r: r["report_date"], reverse=True)
            facilities.append({"facility_info": info, "reports": reports})
            new_ids[provider["id"]] = [r["report_id"] for r in reports]
        return facilities, new_ids, registry

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        logger.info("-- South Dakota run summary --")
        logger.info(f"providers listed: {self.stats['providers_listed']}, in scope and visited: "
                    f"{self.stats['providers_visited']}")
        for key in sorted(k for k in self.stats if k.startswith("listed: ")):
            logger.info(f"  {key[8:]}: {self.stats[key]}")
        for key in sorted(k for k in self.stats if k.startswith("listed in group: ")):
            logger.info(f"  documents in '{key[17:]}': {self.stats[key]}")
        logger.info(f"facilities with new reports: {len(facilities)}")
        logger.info(f"reports: {len(reports)} (flagged: {sum(1 for r in reports if r['is_flagged'])})")
        logger.info(f"text in all: {sum(r['content_length'] for r in reports):,} characters")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        for kind in KIND_LABELS:
            flagged = sum(1 for r in reports if r["categories"]["kind"] == kind and r["is_flagged"])
            logger.info(f"  {kind}: {self.stats.get(kind, 0)} (flagged: {flagged})")
        logger.info(f"studies with a section not met: {self.stats.get('studies_flagged', 0)} "
                    f"({self.stats.get('study_sections_flagged', 0)} sections)")
        logger.info("study answers: " + ", ".join(
            f"{key[7:]} {self.stats[key]}" for key in sorted(self.stats) if key.startswith("answer ")))
        logger.info(f"studies with no sections read: {self.stats.get('study_unparsed', 0)}")
        logger.info(f"plan items: {self.stats.get('plan_items', 0)}; plans with no items read: "
                    f"{self.stats.get('plan_unparsed', 0)}")
        logger.info("inspection forms (type | scanned or typed | result):")
        for title, count in sorted(self.inspection_titles.items()):
            logger.info(f"  {count:3d}  {title}")
        logger.info(f"documents without text: {self.stats.get('no_text', 0)}")
        logger.info(f"held back: {len(self.held_back)} (not a report: {self.stats.get('not_a_report', 0)}, "
                    f"private content: {self.stats.get('private', 0)})")
        for line in self.held_back:
            logger.info(f"  HELD BACK  {line}")
        for line in self.notes:
            logger.info(f"  NOTE  {line}")


def strip_internal(facilities: List[Dict]) -> List[Dict]:
    """Drop fields that are only for this script before posting."""
    return [{
        "facility_info": facility["facility_info"],
        "reports": [{k: v for k, v in r.items() if k != "is_flagged"} for r in facility["reports"]],
    } for facility in facilities]


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = [{
        "facility_info": facility["facility_info"],
        "reports": [{**report, "is_structured": True} for report in facility["reports"]],
    } for facility in strip_internal(facilities)]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "SD",
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
        state="SD",
        scraped_timestamp=timestamp,
        facilities=strip_internal(facilities),
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape South Dakota youth care provider licensing documents")
    parser.add_argument("--full", action="store_true", help=f"Ignore the seen documents in {STATE_FILE}")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N providers")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    timestamp = datetime.now().isoformat(timespec="seconds")

    scraper = SDScraper()
    facilities, new_ids, registry = scraper.scrape(seen=seen, known=state.get("providers", {}), limit=args.limit)
    scraper.print_stats(facilities)

    # The provider registry is not tied to a post: it only remembers profile links.
    state.setdefault("providers", {}).update(registry)
    save_state(STATE_FILE, state)

    if args.out:
        write_out(args.out, facilities, timestamp)
    if not facilities:
        logger.info("No new documents since last run")
        return
    if args.no_post:
        logger.info("Skipping API POST because --no-post was set; seen documents not advanced")
        return
    if save_to_api(facilities, timestamp):
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- seen documents not advanced")


if __name__ == "__main__":
    main()
