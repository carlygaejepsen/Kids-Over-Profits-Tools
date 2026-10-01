"""
Idaho children's residential licensing survey scraper.

Source: Idaho Department of Health and Welfare, Division of Licensing and
Certification, Children's Residential Licensing. The department keeps its
survey documents in a public Laserfiche WebLink repository:

  residential facilities  https://publicdocuments.dhw.idaho.gov/WebLink/Browse.aspx?id=19853&dbid=0&repo=PUBLIC-DOCUMENTS
  outdoor programs        https://publicdocuments.dhw.idaho.gov/WebLink/Browse.aspx?id=19852&dbid=0&repo=PUBLIC-DOCUMENTS
  provider list (one PDF) https://publicdocuments.dhw.idaho.gov/WebLink/Browse.aspx?id=20035&dbid=0&repo=PUBLIC-DOCUMENTS

Each top folder holds one folder per facility, and each facility folder one
PDF per survey: "Approved POC-<date>" (the statement of deficiencies with the
facility's accepted plan of correction) or "No Deficiencies Letter-<date>".
One document is one report.

  POST FolderListingService.aspx/GetFolderListing2   a folder's entries (JSON)
  GET  ElectronicFile.aspx?docid=<entryId>           one PDF

The statements are four-column tables (rule, finding, plan of correction,
date). They are read with pdfplumber's table extraction, never extract_text(),
which interleaves the columns line by line. A row that runs over a page break
comes back as a second row with an empty rule cell and is joined to the row
before it.

Only licensing documents are posted. A document that is neither a statement
of deficiencies nor a no-deficiency letter, a statement whose table could not
be read, and anything whose text trips the privacy check (a date of birth, a
named child, a record number) is logged, kept out of the payload, removed
from the archive folder and listed in the run report for the owner.

The folder tree is the only facility list: the state file keeps every folder
id seen, so a facility whose folder leaves the list is still asked for by id,
and nothing is ever deleted on our side.
"""

import argparse
import io
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
STATE_FILE = Path(os.getenv("ID_STATE_FILE", ".id_state.json"))
REPORTS = ReportStore("ID_PDF_CACHE", "id_pdfs", Path(__file__).parent / "id_pdfs")

BASE = "https://publicdocuments.dhw.idaho.gov/WebLink/"
LISTING_URL = BASE + "FolderListingService.aspx/GetFolderListing2"
FILE_URL = BASE + "ElectronicFile.aspx?docid={entry_id}&dbid=0&repo=PUBLIC-DOCUMENTS"
VIEW_URL = BASE + "DocView.aspx?id={entry_id}&dbid=0&repo=PUBLIC-DOCUMENTS"
REPO = "PUBLIC-DOCUMENTS"

# Top folders in scope, with the programme category used when a document does
# not name one. The agency surveys folder (19851, adoption and foster
# agencies) is out of scope.
TOP_FOLDERS = {
    19853: "Children's Residential Care Facility",
    19852: "Outdoor Program",
}
PROVIDER_LIST_FOLDER = 20035

TYPE_FOLDER = 0
TYPE_DOCUMENT = -2
PAGE_SIZE = 200

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 0.7


# ── Fetch layer ──────────────────────────────────────────────────────────────


class IDClient:
    def __init__(self, store: Optional[ReportStore] = None, cached_only: bool = False) -> None:
        # `store` keeps every folder listing beside the report extractions; with
        # `cached_only` (--cached) listings come from there and the state's site
        # is not asked for anything, so a parser fix costs it no requests.
        self.store = store
        self.cached_only = cached_only
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call = 0.0
        self.requests_made = 0

    def _pause(self) -> None:
        wait = REQUEST_GAP - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """One request at a time, with retries on timeouts, connection errors and 5xx."""
        delay = 2.0
        for attempt in range(1, 5):
            self._pause()
            self.requests_made += 1
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

    def listing(self, folder_id: int) -> Dict:
        """A folder's name, path and every entry, paged until totalEntries is covered."""
        cache_name = f"_listing_{int(folder_id)}"
        if self.cached_only:
            cached = self.store.cached_extract(cache_name) if self.store else None
            if cached is None:
                raise RuntimeError(f"--cached: folder {folder_id} has no saved listing; run once without --cached")
            return cached
        entries: List[Dict] = []
        start = 0
        name = path = ""
        while True:
            response = self._request("POST", LISTING_URL, json={
                "repoName": REPO, "folderId": int(folder_id), "getNewListing": True,
                "start": start, "end": start + PAGE_SIZE, "sortColumn": "", "sortAscending": True,
            })
            response.raise_for_status()
            data = (response.json() or {}).get("data") or {}
            results = [entry for entry in (data.get("results") or []) if entry]
            entries += results
            name = data.get("name") or name
            path = data.get("path") or path
            start += PAGE_SIZE
            if start >= int(data.get("totalEntries") or 0) or not results:
                break
        result = {"name": name, "path": path, "entries": entries}
        if self.store:
            self.store.save_extract(cache_name, result)
        return result

    def pdf(self, entry_id: int) -> Optional[bytes]:
        """The document's PDF bytes, or None (nothing failed is cached, so the
        next run asks again)."""
        if self.cached_only:
            logger.warning(f"  {entry_id}: not in the cache and --cached is set; not downloaded")
            return None
        try:
            response = self._request("GET", FILE_URL.format(entry_id=entry_id))
        except requests.RequestException as exc:
            logger.warning(f"  {entry_id}: download failed: {exc}")
            return None
        if response.status_code != 200 or not response.content.startswith(b"%PDF"):
            logger.warning(f"  {entry_id}: not a PDF (HTTP {response.status_code}, "
                           f"{response.headers.get('content-type', '')})")
            return None
        return response.content


def entry_times(entry: Dict) -> Tuple[str, str]:
    """(created, modified) as the listing gives them: the last two data items."""
    data = entry.get("data") or []
    if len(data) >= 2:
        return str(data[-2] or ""), str(data[-1] or "")
    return "", ""


# ── PDF extraction ───────────────────────────────────────────────────────────


def nonspace(value: str) -> int:
    return len(re.sub(r"\s+", "", value or ""))


def extract_pages(pdf: Any) -> Dict:
    """Per page: the text and every table as rows of cells. `text` is all the
    pages joined; the parser reads the tables, the text is for classification,
    the letters and the privacy check."""
    pages: List[Dict] = []
    for page in pdf.pages:
        text = page.extract_text() or ""
        tables = []
        for table in page.extract_tables():
            tables.append([[(cell or "").strip() for cell in row] for row in table])
        pages.append({
            "text": text,
            "tables": tables,
            "text_chars": nonspace(text),
            "table_chars": sum(nonspace(cell) for table in tables for row in table for cell in row),
        })
    return {"text": "\n".join(p["text"] for p in pages).strip(), "pages": pages}


def extract_pdf(path: Path) -> Dict:
    with pdfplumber.open(path) as pdf:
        return extract_pages(pdf)


# ── Parsing ──────────────────────────────────────────────────────────────────

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}
MONTH_NAMES = "January|February|March|April|May|June|July|August|September|October|November|December"
LONG_DATE = re.compile(rf"\b({MONTH_NAMES})\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s+(\d{{4}})\b", re.I)
NUMERIC_DATE = re.compile(r"(?<!\d)(\d{1,2})[/_.-](\d{1,2})[/_.-](\d{4}|\d{2})(?!\d)")


def clean(value: str) -> str:
    value = (value or "").replace(" ", " ").replace("’", "'").replace("‘", "'")
    value = value.replace("“", '"').replace("”", '"').replace("�", "'")
    # A symbol-font glyph the PDF has no Unicode for comes out as U+FFFF: a
    # bullet in the lists, a dash inside "Weeks 2-4".
    value = re.sub("(?<=\\d)\uffff(?=\\d)", "-", value).replace("\uffff", "\u2022")
    return re.sub(r"[ \t]+", " ", value).strip()


def one_line(value: str) -> str:
    return re.sub(r"\s+", " ", clean(value)).strip()


def make_iso(year: int, month: int, day: int) -> str:
    if year < 100:
        year += 2000
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def numeric_dates(value: str) -> List[str]:
    """Every m/d/yyyy (or m_d_yyyy, m-d-yy) in `value` as ISO, in order."""
    out = []
    for match in NUMERIC_DATE.finditer(value or ""):
        iso = make_iso(int(match.group(3)), int(match.group(1)), int(match.group(2)))
        if iso:
            out.append(iso)
    return out


def long_dates(value: str) -> List[str]:
    out = []
    for match in LONG_DATE.finditer(value or ""):
        iso = make_iso(int(match.group(3)), MONTHS[match.group(1).lower()], int(match.group(2)))
        if iso:
            out.append(iso)
    return out


def plausible(iso: str) -> bool:
    return bool(iso) and "2000-01-01" <= iso <= datetime.now().strftime("%Y-%m-%d")


# -- statement of deficiencies --

HEADER_LABELS = {
    "agency": "agency",
    "organization": "agency",
    "agency type": "agency_type",
    "organization type": "agency_type",
    "region(s)": "region",
    "region": "region",
    "survey dates": "survey_dates",
    "survey date": "survey_dates",
    "survey date(s)": "survey_dates",
    "license": "license",
    "license(s)": "license",
    "license #": "license",
    "license(s) granted": "license_granted",
    "license granted": "license_granted",
}

# IDAPA 16.04.18 (children's agencies and residential licensing) rule numbers:
# "16.04.18.411.02.b", "16.04.18.416.03", sometimes written "IDAPA 16.04.18.411"
# or "16.04.18 411.02". Idaho Code sections ("39-1210(4)") are cited too.
# One 2025 statement numbers its rows ("1", "4." on a line above the rule),
# and the 2021 form cites the rule's section alone ("546.02 Personnel
# Records: 546.02c Documents verifying ...").
RULE_START = re.compile(
    r"^\s*(?:\d{1,2}\.?\s+)?(?:(?:IDAPA|Idapa)\s*)?("
    r"\d{2}\.\d{2}\.\d{2}[.\s]\s*\d{3}(?:\.(?:\d{1,3}|[a-z]|[ivx]{1,4})(?![A-Za-z0-9]))*|"
    r"(?:Idaho Code\s*)?(?:IC\s*)?(?:§\s*)?\d{2}-\d{3,4}[A-Z]?(?:\([0-9a-z]+\))*|"
    r"\d{3}\.\d{2}[a-z]?(?=\s+[A-Z]))"
)
REPEAT = re.compile(r"\brepeat(?:ed)?\s+deficienc", re.I)
LICENSE_NUMBER = re.compile(r"\b(CRL|CTOP|COP|CA)\s*-\s*(\d{2,6})\b", re.I)


def checked_option(value: str) -> str:
    """The ticked box of the 2021 form's '[ ] 6 - Month Provisional [x] 1 - Year
    Full' (ballot box characters U+2610 and U+2612) -> '1-Year Full'."""
    value = value or ""
    match = re.search("[☒☑✓✔]\\s*([^☐☑☒]*)", value)
    if not match:
        return value.replace("☐", "").strip()
    return re.sub(r"\s*-\s*", "-", one_line(match.group(1)))


def header_fields(pages: List[Dict], text: str) -> Dict[str, str]:
    """The header block: label/value pairs from the small table above the
    deficiencies, else from the text lines."""
    fields: Dict[str, str] = {}
    for page in pages[:1]:
        for table in page.get("tables") or []:
            for row in table:
                cells = [one_line(c) for c in row]
                for index in range(0, len(cells) - 1):
                    label = cells[index].rstrip(":").strip().lower()
                    if cells[index].endswith(":") and label in HEADER_LABELS:
                        fields.setdefault(HEADER_LABELS[label], cells[index + 1])
    head = text[:1500]
    patterns = {
        "agency": r"(?:Agency|Organization):\s*(.*?)\s+Region\(?s?\)?:",
        "region": r"Region\(?s?\)?:[ \t]*([^\n]*)",
        "agency_type": r"(?:Agency|Organization) Type:\s*(.*?)\s+Survey Dates?(?:\(s\))?:",
        "survey_dates": r"Survey Dates?(?:\(s\))?:[ \t]*([^\n]*)",
        "license": r"(?m)^License(?:\(s\))?(?: #)?:\s*(.*?)\s+License\(?s?\)? Granted:",
        "license_granted": r"License\(?s?\)? Granted:[ \t]*([^\n]*)",
    }
    for key, pattern in patterns.items():
        if not fields.get(key):
            match = re.search(pattern, head, re.I | re.S)
            if match:
                fields[key] = one_line(match.group(1))
    if fields.get("license_granted"):
        fields["license_granted"] = checked_option(fields["license_granted"])
    return fields


def is_deficiency_header(row: List[str]) -> bool:
    return bool(row) and re.match(r"rule\s+reference", one_line(row[0]), re.I) is not None


def is_signature_row(row: List[str]) -> bool:
    joined = one_line(" ".join(row))
    return bool(re.search(
        r"(?:Agency|Organization|Department) Representative|Date Submitted|Date Approved", joined, re.I))


def heading_columns(row: List[str]) -> Optional[List[Optional[int]]]:
    """Positions of rule, finding, plan and date from the heading row. One
    2023 statement was saved with its date column cut off the page: its table
    reads rule, finding, an empty column, plan, and has no dates at all."""
    found: Dict[str, int] = {}
    for index, cell in enumerate(row):
        label = one_line(cell).lower()
        if label.startswith("rule reference"):
            found.setdefault("rule", index)
        elif label.startswith("finding"):
            found.setdefault("finding", index)
        elif "plan of correction" in label:
            found.setdefault("plan", index)
        elif label.startswith("date to be"):
            found.setdefault("date", index)
    if "rule" not in found or "finding" not in found or "plan" not in found:
        return None
    return [found["rule"], found["finding"], found["plan"], found.get("date")]


def deficiency_rows(pages: List[Dict]) -> Tuple[List[List[str]], List[int]]:
    """The four-column rows of the deficiency table across all pages, header
    and signature rows dropped. Also the (1-based) pages that have text but
    gave no table row at all."""
    rows: List[List[str]] = []
    empty_pages: List[int] = []
    # The header block above the deficiencies is a four-column table too
    # ("Agency:", name, "Region(s):", number): rows count only once the
    # "Rule Reference/Text" heading has been passed.
    has_heading = any(is_deficiency_header(row) for page in pages
                      for table in page.get("tables") or [] for row in table)
    started = not has_heading
    columns: List[Optional[int]] = [0, 1, 2, 3]   # where rule, finding, plan and date sit in a row
    width = 4
    for number, page in enumerate(pages, start=1):
        found = 0
        for table in page.get("tables") or []:
            for row in table:
                found += 1
                if is_deficiency_header(row):
                    started = True
                    columns = heading_columns(row) or columns
                    width = len(row)
                    continue
                if not started or is_signature_row(row) or not any(row):
                    continue
                if one_line(row[0]).endswith(":") and one_line(row[0]).rstrip(":").lower() in HEADER_LABELS:
                    continue
                # Two statements have a heading table of six or seven columns
                # (merged cells) whose first row shares that width, then plain
                # four-column rows on the pages after.
                if len(row) == width:
                    rows.append([row[index] if index is not None else "" for index in columns])
                elif len(row) == 4:
                    rows.append(list(row))
        # A page holding only its page number (seen once, a trailing blank
        # page) has nothing to read.
        if not found and page.get("text_chars", 0) > 20:
            empty_pages.append(number)
    return rows, empty_pages


def split_rule(cell: str) -> Tuple[str, str]:
    """'16.04.18.411.02.b SERVICE PLANS. 02. ...' -> (number, text)."""
    text = one_line(cell)
    match = RULE_START.match(text)
    if not match:
        return "", text
    number = re.sub(r"\s*\.\s*", ".", match.group(1).strip())
    number = re.sub(r"^(\d{2}\.\d{2}\.\d{2})\s+", r"\1.", number).rstrip(".")
    return number, text[match.end():].lstrip(" .:-")


def join_cell(first: str, second: str) -> str:
    if not second:
        return first
    return (first + "\n" + second).strip() if first else second


def parse_deficiencies(pages: List[Dict]) -> Tuple[List[Dict], List[int], int, List[List[str]]]:
    """Deficiencies in order; pages with text and no table row; the number of
    rows joined to the one before (a row split by a page break); the joined
    rows as read, for the check in unread_lines()."""
    rows, empty_pages = deficiency_rows(pages)
    merged: List[List[str]] = []
    joined = 0
    for row in rows:
        starts = bool(RULE_START.match(one_line(row[0])))
        if merged and not starts:
            # The rest of a row cut by a page break: every cell continues the
            # cell above it (the rule cell too, when the rule text ran over).
            merged[-1] = [join_cell(a, b) for a, b in zip(merged[-1], row)]
            joined += 1
        elif not starts and not row[0] and not row[1]:
            # The column headings wrapped onto further rows ("(Please refer
            # to the Statement of", "Corrected") before the first deficiency.
            continue
        else:
            merged.append(list(row))
    out: List[Dict] = []
    for row in merged:
        rule, rule_text = split_rule(row[0])
        finding = clean_block(row[1])
        # An unfilled Word date field prints its placeholder.
        date_text = re.sub(r"Click or tap to enter a date\.?", "", one_line(row[3]), flags=re.I).strip()
        dates = numeric_dates(date_text) or long_dates(date_text)
        # One date becomes ISO; several ("1.1- 2/23/23 2.1- 2/15/23"), or
        # words ("Immediately"), stay as the facility wrote them.
        single = len(dates) == 1 and len(date_text) <= 20
        plan = clean_block(row[2])
        date_value = dates[0] if single else date_text
        words = re.sub(rf"(?:{MONTH_NAMES})|[\d./_-]+|[:,;()]", " ", date_text, flags=re.I).split()
        if len(date_text) > 60 and len(words) > 8:
            # A facility wrote its plan (or a note) into the date column: the
            # words belong to the plan, and a date they start with stays the date.
            lead = re.match(r"\s*(\d{1,2}/\d{1,2}/\d{4})\b[\s:.,;-]*", date_text)
            date_value = numeric_dates(lead.group(1))[0] if lead else ""
            extra = date_text[lead.end():] if lead else date_text
            plan = (plan + "\n" if plan else "") + "(Written in the date column:) " + extra
        out.append({
            "rule": rule,
            "rule_text": rule_text,
            "finding": finding,
            "plan": plan,
            "date_to_correct": date_value,
            "repeat": bool(REPEAT.search(finding)),
        })
    return out, empty_pages, joined, merged


def unread_lines(pages: List[Dict], merged: List[List[str]]) -> List[str]:
    """Lines of the statement's page text that are in no parsed deficiency:
    the check that each cell came out whole. Header, column headings, page
    numbers and the signature block are expected and left out."""
    parsed = re.sub(r"\s+", "", clean(" ".join(cell for row in merged for cell in row)))
    # The signature block is a table of its own and is not a deficiency.
    signature = re.sub(r"\s+", "", clean(" ".join(
        cell for page in pages for table in page.get("tables") or []
        for row in table if is_signature_row(row) for cell in row)))
    missing = []
    for page in pages:
        for line in (page.get("text") or "").split("\n"):
            squeezed = re.sub(r"\s+", "", clean(line))
            if len(squeezed) < 12:
                continue
            if re.search(r"Statement of Deficiencies|^(?:Agency|Organization)|^License|Rule Reference|"
                         r"\(Please refer|Deficiencies cover letter|Agency Representative|"
                         r"Organization Representative|By entering my name|plan of correction as "
                         r"|Date Submitted|services for a maximum|through seventeen|1 - Year Full|"
                         r"Department Representative|Risk Assessment|^(?:Gold|Silver|Bronze):|"
                         r"^(?:corrective )?actions? necessary|^corrective action\.|^heightened monitoring|"
                         r"^monitoring or corrective action|enter a date\.|"
                         r"IDAHO DEPARTMENT OF|HEALTH & WELFARE|DIVISION OF LICENSING",
                         one_line(line), re.I):
                continue
            if squeezed in parsed or squeezed in signature:
                continue
            # Text extraction runs the columns together on one line: the line
            # is read when it splits into runs of words that each sit in a
            # parsed cell.
            current, ok = "", True
            for word in clean(line).split(" "):
                if current + word in parsed:
                    current += word
                elif word in parsed:
                    current = word
                else:
                    ok = False
                    break
            if not ok:
                missing.append(one_line(line))
    return missing


def clean_block(cell: str) -> str:
    """A table cell as running text: the PDF's line wraps joined, the plan's
    numbered questions ("1. What actions ...") kept on their own lines."""
    lines = [clean(line) for line in (cell or "").split("\n")]
    lines = [line for line in lines if line]
    out: List[str] = []
    for line in lines:
        if out and not re.match(r"^\d{1,2}[.)]\s", line):
            out[-1] += " " + line
        else:
            out.append(line)
    return "\n".join(out)


def survey_end(survey_dates: str) -> str:
    """The last day of the survey as ISO, or '' when the header is loose
    ("October 2021 Electronic")."""
    dates = [d for d in numeric_dates(survey_dates) + long_dates(survey_dates) if plausible(d)]
    if dates:
        return max(dates)
    # "4/10-11/2024", "4/10 & 4/11/2024": a day range sharing one year.
    match = re.search(r"(\d{1,2})/(\d{1,2})\s*(?:-|&|and|to)\s*(?:(\d{1,2})/)?(\d{1,2})/(\d{4})", survey_dates or "")
    if match:
        month = int(match.group(3) or match.group(1))
        return make_iso(int(match.group(5)), month, int(match.group(4)))
    return ""


# -- no-deficiency letter --

NO_DEFICIENCIES = re.compile(r"\b(?:zero|no)\s+deficienc(?:y|ies)\b", re.I)


def letter_fields(text: str) -> Dict[str, str]:
    """Letter date, licence end date and the address block of the letter."""
    body = clean(text)
    fields = {"letter_date": "", "license_end": "", "address": "", "administrator": ""}
    head = long_dates(body[:800]) or numeric_dates(body[:800])
    if head and plausible(head[0]):
        fields["letter_date"] = head[0]
    match = re.search(
        r"(?:expire|expires|expiration|end(?:s|ing)?|through|until|valid\s+(?:through|until))\D{0,40}?"
        rf"((?:{MONTH_NAMES})\s+\d{{1,2}},?\s+\d{{4}}|\d{{1,2}}/\d{{1,2}}/\d{{4}})",
        body, re.I)
    if match:
        found = long_dates(match.group(1)) or numeric_dates(match.group(1))
        fields["license_end"] = found[0] if found else ""
    # The address block: the lines between the date line and the salutation
    # ("Dear ...") or subject line, ending with "City, ID 83xxx".
    lines = [one_line(line) for line in body.split("\n")]
    for index, line in enumerate(lines[:40]):
        if re.search(r",\s*(?:ID|Idaho)\s+8\d{4}(?:-\d{4})?$", line, re.I) and index >= 1:
            street = lines[index - 1]
            if re.match(r"^(?:P\.?\s*O\.?\s*Box|\d)", street, re.I):
                city_line = re.sub(r",\s*Idaho\s+", ", ID ", line, flags=re.I)
                fields["address"] = f"{street}, {re.sub(r'-0000$', '', city_line)}"
                if index >= 3 and LONG_DATE.search(" ".join(lines[max(0, index - 6):index - 2])):
                    fields["administrator"] = ""
                break
    return fields


# -- privacy check --

# Things that must not be public: a date of birth, a record number, a child
# named in full. The state writes "Child #1", "resident A" or initials; a hit
# holds the document back for the owner to read.
SAFE_AFTER_ROLE = (
    r"Rights?|Care|Records?|Files?|Handbooks?|Protective|Protection|Welfare|Services?|Safety|"
    r"Abuse|Development|Treatment|Supervision|Ratio|Program|Programs|Placement|Grievance|"
    r"Advocate|Specialist|Counselor|Worker|Manager|Coordinator|Director|Staff|Mentor|"
    r"Intake|Admission|Discharge|Service|Plan|Plans|Medication|Medical|Health|Homes?|"
    r"Ranch|Foundation|Academy|Center|Village|House|Residential|Agency|Number|Name|"
    r"Was|Were|Is|Has|Had|Did|Does|Will|Who|In|On|At|To|And|Or|The|A|An|No|Not|One|Two|Three"
)
PRIVACY_PATTERNS = [
    ("date of birth", re.compile(
        r"\b(?:D\.?O\.?B\.?|date of birth|birth\s*date|born(?:\s+on)?)\b\W{0,6}"
        rf"(?:\d{{1,2}}[/-]\d{{1,2}}[/-]\d{{2,4}}|(?:{MONTH_NAMES})\s+\d{{1,2}},?\s+\d{{4}})", re.I)),
    ("social security number", re.compile(r"(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)")),
    ("record number", re.compile(
        r"\b(?:medicaid|medical record|case|client|resident|child|youth|MRN|record)\s*"
        r"(?:id(?:entification)?\s*)?(?:number|no\.|#)\s*:?\s*[A-Z]{0,3}-?\d{5,}", re.I)),
    ("named child", re.compile(
        r"\b(?:child|children|resident|youth|client|student|minor|juvenile|kid|boy|girl)\s+"
        rf"(?:named\s+|identified as\s+)?(?!(?:{SAFE_AFTER_ROLE})\b)([A-Z][a-z]{{2,}}\s+[A-Z][a-z]{{2,}})\b")),
    ("named child", re.compile(r"\b(?:child|resident|youth|client|student)\s+(?:named|identified as)\s+[A-Z][a-z]+")),
]


def privacy_hits(text: str) -> List[str]:
    hits = []
    for label, pattern in PRIVACY_PATTERNS:
        match = pattern.search(text or "")
        if match:
            start = max(0, match.start() - 40)
            hits.append(f"{label}: ...{one_line(text[start:match.end() + 40])}...")
    return hits


# -- complaint wording --

COMPLAINT_WORDS = re.compile(r"\b(complaints?|investigat\w+|follow[- ]?up)\b", re.I)


def complaint_wording(header_text: str, findings: List[str]) -> List[str]:
    """Snippets where the header or a finding uses complaint, investigation or
    follow-up, so the run report shows whether complaint surveys are posted."""
    out: List[str] = []
    for source, text in [("header", header_text)] + [("finding", f) for f in findings]:
        match = COMPLAINT_WORDS.search(text or "")
        if match:
            start = max(0, match.start() - 60)
            note = f"{source}: ...{one_line(text[start:match.end() + 90])}..."
            if note not in out:
                out.append(note)
    return out


# -- names --

FORMER_NAME = re.compile(r"\s*\(\s*(?:fka|f/k/a|f\.k\.a\.?|formerly(?:\s+known\s+as)?)\b[\s:.,-]*([^)]*)\)\s*$", re.I)


def split_folder_name(name: str) -> Tuple[str, str]:
    """'Summit Youth Academy  (fka Patriot Center)' -> ('Summit Youth Academy', 'Patriot Center')."""
    name = one_line(name)
    match = FORMER_NAME.search(name)
    if match:
        return name[:match.start()].strip(), one_line(match.group(1))
    return name, ""


def name_key(name: str) -> str:
    name = split_folder_name(name)[0].lower().replace("&", " and ")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", name)).strip()


def document_kind_from_name(name: str) -> str:
    lowered = one_line(name).lower()
    if lowered.startswith("approved poc"):
        return "deficiencies"
    if lowered.startswith("no deficienc"):
        return "no_deficiencies"
    return "other"


# ── Provider list ────────────────────────────────────────────────────────────


def format_zip(value: str) -> str:
    digits = re.sub(r"[^\d-]", "", value or "")
    digits = re.sub(r"-0000$", "", digits)
    return digits.rstrip("-")


def parse_provider_rows(pages: List[Dict]) -> List[Dict]:
    """Rows of the provider list PDF: name, address, city, zip, phone, licence
    type, male/female/co-ed beds, ages."""
    providers: List[Dict] = []
    for page in pages:
        for table in page.get("tables") or []:
            for row in table:
                cells = [one_line(c) for c in row]
                if len(cells) != 10 or not cells[0] or cells[0].lower().startswith("agency name"):
                    continue
                beds = [int(c) if c.isdigit() else 0 for c in cells[6:9]]
                zip_code = format_zip(cells[3].replace(" ", ""))
                city = re.sub(r"\s+", " ", cells[2]).replace("d' Alene", "d'Alene").replace("d Alene", "d'Alene")
                address = ", ".join(part for part in [cells[1], city, ("ID " + zip_code).strip()] if part)
                providers.append({
                    "name": cells[0],
                    "address": address,
                    "phone": re.sub(r"\s+", "", cells[4]),
                    "license_type": cells[5],
                    "male_beds": beds[0], "female_beds": beds[1], "coed_beds": beds[2],
                    "beds": sum(beds),
                    "ages": "" if cells[9] == "0 to 0" else cells[9],
                })
    return providers


# ── Scraper ──────────────────────────────────────────────────────────────────


class IDScraper:
    def __init__(self, client: Optional[IDClient] = None, reports: ReportStore = REPORTS,
                 cached_only: bool = False):
        self.client = client or IDClient(store=reports, cached_only=cached_only)
        self.reports = reports
        self.stats: Counter = Counter()
        self.granted: Counter = Counter()
        self.deficiencies_per_doc: Counter = Counter()
        self.held: List[Dict] = []
        self.empty_table_pages: List[str] = []
        self.low_coverage: List[str] = []
        self.unread: List[str] = []
        self.risk: Counter = Counter()
        self.complaint_notes: List[str] = []
        self.loose_dates: List[str] = []
        self.unmatched_providers: List[str] = []
        self.ambiguous_providers: List[str] = []
        self.shared_licences: List[str] = []
        self.providers_without_folder: List[str] = []
        self.downloads = 0

    # -- listing --

    def facility_folders(self, known: Dict[str, Dict]) -> List[Dict]:
        """Every facility folder under the top folders, plus known folders the
        state no longer lists (still asked for by id)."""
        folders: List[Dict] = []
        listed_ids: Set[str] = set()
        for top_id, category in TOP_FOLDERS.items():
            listing = self.client.listing(top_id)
            logger.info(f"{listing['name'] or top_id}: {len(listing['entries'])} entries")
            for entry in listing["entries"]:
                if entry.get("type") == TYPE_FOLDER:
                    created, modified = entry_times(entry)
                    folders.append({
                        "id": str(entry["entryId"]), "name": entry.get("name") or "", "top": top_id,
                        "category": category, "listed": True, "created": created, "modified": modified,
                    })
                    listed_ids.add(str(entry["entryId"]))
                elif entry.get("type") == TYPE_DOCUMENT:
                    # A document outside any facility folder belongs to no facility.
                    self.hold(entry, f"(top folder {top_id})", "loose document in a top folder", remove=False)
        for folder_id, stored in sorted(known.items()):
            if folder_id not in listed_ids:
                top = int(stored.get("top") or 19853)
                folders.append({
                    "id": folder_id, "name": stored.get("name") or "", "top": top,
                    "category": TOP_FOLDERS.get(top, "Children's Residential Care Facility"),
                    "listed": False, "created": "", "modified": "",
                    "last_listed": stored.get("last_listed", ""),
                })
        return folders

    def folder_documents(self, folder_id: str, depth: int = 0) -> List[Dict]:
        """A facility folder's documents; sub-folders (none on 2026-10-01) are walked too."""
        listing = self.client.listing(int(folder_id))
        docs: List[Dict] = []
        for entry in listing["entries"]:
            if entry.get("type") == TYPE_DOCUMENT:
                docs.append(entry)
            elif entry.get("type") == TYPE_FOLDER and depth < 2:
                self.stats["subfolders"] += 1
                docs += self.folder_documents(str(entry["entryId"]), depth + 1)
            else:
                self.stats["other_entries"] += 1
        return docs

    # -- provider list --

    def provider_list(self) -> List[Dict]:
        try:
            listing = self.client.listing(PROVIDER_LIST_FOLDER)
        except (requests.RequestException, ValueError) as exc:
            logger.warning(f"provider list folder failed: {exc}")
            return []
        docs = [e for e in listing["entries"] if e.get("type") == TYPE_DOCUMENT]
        if not docs:
            logger.warning("provider list folder is empty")
            return []
        entry = docs[0]
        modified = entry_times(entry)[1]
        # Cached beside the report extractions; the PDF is not a report and is
        # not archived.
        cache_name = f"_provider_list_{entry['entryId']}"
        cached = self.reports.cached_extract(cache_name)
        if not cached or cached.get("modified") != modified:
            data = self.client.pdf(entry["entryId"])
            if not data:
                return (cached or {}).get("providers") or []
            self.downloads += 1
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                extracted = extract_pages(pdf)
            cached = {"modified": modified, "providers": parse_provider_rows(extracted["pages"])}
            self.reports.save_extract(cache_name, cached)
        logger.info(f"provider list ({entry.get('name')}, modified {modified}): {len(cached['providers'])} rows")
        return cached["providers"]

    # -- documents --

    def hold(self, entry: Dict, folder_name: str, reason: str, remove: bool = True) -> None:
        entry_id = entry.get("entryId")
        self.stats["held_back"] += 1
        self.held.append({
            "entry_id": entry_id, "facility": folder_name, "document": entry.get("name") or "",
            "reason": reason, "url": VIEW_URL.format(entry_id=entry_id),
        })
        logger.warning(f"  HELD BACK {entry_id} ({entry.get('name')}): {reason}")
        if remove:
            # Not posted, and the archived copy is removed so the nightly
            # archive sync does not put it on the site.
            try:
                (self.reports.archive_dir / f"{entry_id}.pdf").unlink()
            except OSError:
                pass

    def extraction(self, entry: Dict) -> Optional[Dict]:
        entry_id = entry["entryId"]
        archive_name = f"{entry_id}.pdf"
        modified = entry_times(entry)[1]
        cached = self.reports.cached_extract(archive_name)
        if cached is not None and cached.get("modified") not in (None, "", modified):
            # The state replaced the document: read it again.
            logger.info(f"  {entry_id} changed on {modified}; fetching again")
            try:
                (self.reports.extract_dir / f"{archive_name}.json").unlink()
                (self.reports.archive_dir / archive_name).unlink()
            except OSError:
                pass

        def fetch() -> Optional[bytes]:
            self.downloads += 1
            return self.client.pdf(entry_id)

        def extract(path: Path) -> Dict:
            result = extract_pdf(path)
            result["modified"] = modified
            return result

        return extract_with_cache(self.reports, archive_name, fetch=fetch, extract=extract)

    def build_report(self, entry: Dict, folder: Dict, former_name: str) -> Optional[Dict]:
        entry_id = entry["entryId"]
        doc_name = one_line(entry.get("name") or "")
        label = f"{folder['name']} / {doc_name}"
        extension = (entry.get("extension") or "").lower()
        if extension and extension != "pdf":
            self.hold(entry, folder["name"], f"a .{extension} file, not a PDF", remove=False)
            return None
        extracted = self.extraction(entry)
        if not extracted or not extracted.get("text"):
            self.stats["no_text"] += 1
            self.hold(entry, folder["name"], "no text could be read (a scan, or the download failed)", remove=False)
            return None

        text = clean(extracted["text"])
        pages = extracted.get("pages") or []
        named = document_kind_from_name(doc_name)
        head = one_line(text[:700])
        if re.search(r"Statement of Deficiencies", head, re.I) and any(
                is_deficiency_header(row) for p in pages for t in p.get("tables") or [] for row in t):
            kind = "deficiencies"
        elif re.search(r"Statement of Deficiencies", head, re.I) and named == "deficiencies":
            kind = "deficiencies"
        elif NO_DEFICIENCIES.search(text) and not re.search(r"Rule Reference", text, re.I):
            kind = "no_deficiencies"
        else:
            kind = "other"
        if kind == "other":
            self.stats["not_a_report"] += 1
            self.hold(entry, folder["name"], "neither a statement of deficiencies nor a no-deficiency letter")
            return None
        if named != kind:
            self.stats["name_and_content_differ"] += 1
            logger.warning(f"  {label}: named as {named}, reads as {kind}")

        name_dates = [d for d in numeric_dates(doc_name) if plausible(d)]
        name_date = name_dates[0] if name_dates else ""
        created, modified = entry_times(entry)
        categories: Dict[str, Any] = {
            "kind": kind,
            "document_name": doc_name,
            "survey_dates": "",
            "license": "",
            "license_granted": "",
            "region": "",
            "agency_type": "",
            "deficiencies": [],
            "deficiency_count": 0,
            "repeat_count": 0,
            "archive_name": f"{entry_id}.pdf",
            "posted_date": (numeric_dates(created) or [""])[0],
        }
        if former_name:
            categories["former_name"] = former_name

        if kind == "deficiencies":
            fields = header_fields(pages, text)
            deficiencies, empty_pages, joined, merged_rows = parse_deficiencies(pages)
            self.stats["rows_joined_across_pages"] += joined
            for number in empty_pages:
                self.empty_table_pages.append(f"{label} (entry {entry_id}) page {number}")
            for line in unread_lines(pages, merged_rows):
                self.unread.append(f"{label} (entry {entry_id}): {line[:140]}")
            for number, page in enumerate(pages, start=1):
                if page.get("text_chars", 0) > 200 and page.get("table_chars", 0) < 0.6 * page["text_chars"]:
                    self.low_coverage.append(
                        f"{label} (entry {entry_id}) page {number}: "
                        f"{page.get('table_chars', 0)} of {page['text_chars']} characters in tables")
            if not deficiencies:
                self.stats["statement_without_rows"] += 1
                self.hold(entry, folder["name"], "statement of deficiencies whose table could not be read")
                return None
            license_match = LICENSE_NUMBER.search(fields.get("license", "")) or None
            categories.update({
                "survey_dates": fields.get("survey_dates", ""),
                "license": (f"{license_match.group(1).upper()}-{license_match.group(2)}" if license_match
                            else one_line(fields.get("license", ""))),
                "license_granted": fields.get("license_granted", ""),
                "region": fields.get("region", ""),
                "agency_type": fields.get("agency_type", ""),
                "agency": fields.get("agency", ""),
                "deficiencies": deficiencies,
                "deficiency_count": len(deficiencies),
                "repeat_count": sum(1 for d in deficiencies if d["repeat"]),
            })
            # The 2026 form drops "Region(s)" and adds a risk rating at the foot
            # of the statement: Gold (full compliance), Silver (substantial
            # compliance) or Bronze (areas of non-compliance).
            risk = re.search(r"Risk Assessment:\s*(?:Risk Assessment:\s*)?(Gold|Silver|Bronze)\b", text)
            if risk:
                categories["risk_assessment"] = risk.group(1)
                self.risk[risk.group(1)] += 1
            submitted = re.search(r"Date Submitted:\s*([^\n]*)", text, re.I)
            if submitted:
                dates = numeric_dates(submitted.group(1))
                categories["plan_submitted"] = dates[0] if dates else ""
            end = survey_end(categories["survey_dates"])
            if plausible(end):
                report_date, source = end, "survey"
            else:
                report_date, source = name_date, "document_name"
                self.loose_dates.append(f"{label}: survey dates '{categories['survey_dates']}', using {name_date or 'nothing'}")
            self.granted[categories["license_granted"] or "(empty)"] += 1
            self.deficiencies_per_doc[len(deficiencies)] += 1
            self.stats["deficiency_rows"] += len(deficiencies)
            self.stats["repeat_rows"] += categories["repeat_count"]
            self.stats["rows_without_rule_number"] += sum(1 for d in deficiencies if not d["rule"])
            header_text = text[:text.find("Rule Reference")] if "Rule Reference" in text else text[:400]
            for note in complaint_wording(header_text, [d["finding"] for d in deficiencies]):
                self.complaint_notes.append(f"{label}: {note}")
            blocks = []
            for index, d in enumerate(deficiencies, start=1):
                blocks.append("\n".join(part for part in [
                    f"Deficiency {index}",
                    f"Rule: {d['rule']} {d['rule_text']}".strip(),
                    f"Finding: {d['finding']}",
                    f"Plan of correction: {d['plan']}" if d["plan"] else "",
                    f"Date to be corrected: {d['date_to_correct']}" if d["date_to_correct"] else "",
                ] if part))
            header_lines = [
                "Children's Residential Licensing - Statement of Deficiencies",
                f"Agency: {categories['agency'] or folder['name']}",
                f"Agency type: {categories['agency_type']}" if categories["agency_type"] else "",
                f"Survey dates: {categories['survey_dates']}" if categories["survey_dates"] else "",
                f"License: {categories['license']}" if categories["license"] else "",
                f"License(s) granted: {categories['license_granted']}" if categories["license_granted"] else "",
                f"Region(s): {categories['region']}" if categories["region"] else "",
            ]
            raw_content = "\n".join(line for line in header_lines if line) + "\n\n" + "\n\n".join(blocks)
            rules = [d["rule"] for d in deficiencies if d["rule"]]
            count = len(deficiencies)
            summary = f"{count} deficienc{'y' if count == 1 else 'ies'}"
            if categories["repeat_count"]:
                summary += f" ({categories['repeat_count']} repeat)"
            if rules:
                summary += ": " + ", ".join(dict.fromkeys(rules))
            checked = "\n".join([header_text] + [d["finding"] + "\n" + d["plan"] for d in deficiencies])
        else:
            fields = letter_fields(text)
            categories["license_end"] = fields["license_end"]
            categories["letter_address"] = fields["address"]
            license_match = LICENSE_NUMBER.search(text)
            if license_match:
                categories["license"] = f"{license_match.group(1).upper()}-{license_match.group(2)}"
            if fields["letter_date"]:
                report_date, source = fields["letter_date"], "letter"
            else:
                report_date, source = name_date, "document_name"
                self.loose_dates.append(f"{label}: no letter date, using {name_date or 'nothing'}")
            raw_content = text
            summary = "No deficiencies"
            checked = text
            for note in complaint_wording(text, []):
                self.complaint_notes.append(f"{label}: {note}")

        hits = privacy_hits(checked)
        if hits:
            self.stats["privacy_held"] += 1
            self.hold(entry, folder["name"], "privacy check: " + " | ".join(hits))
            return None
        if not report_date:
            self.stats["no_date"] += 1
            self.hold(entry, folder["name"], "no date in the document or its name", remove=False)
            return None
        categories["date_source"] = source
        self.stats[kind] += 1
        return {
            "report_id": str(entry_id),
            "report_date": report_date,
            "report_url": VIEW_URL.format(entry_id=entry_id),
            "raw_content": raw_content,
            "content_length": len(raw_content),
            "summary": summary,
            "categories": categories,
            "is_flagged": kind == "deficiencies",
            "modified": modified,
        }

    # -- facilities --

    def match_provider(self, folder_name: str, providers: List[Dict]) -> Optional[Dict]:
        """The provider list row for a folder. A provider listed at several
        addresses under one name (one folder, one licence: Mountaintop Behavior
        Health) is one facility with several sites: the addresses are joined and
        the beds shown per site, since the list does not say whether the rows
        repeat one licensed capacity or add up."""
        key = name_key(folder_name)
        matches = [p for p in providers if name_key(p["name"]) == key]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            self.ambiguous_providers.append(
                f"{folder_name}: {len(matches)} provider list rows ({'; '.join(p['address'] for p in matches)})"
                " -> kept as one facility with several sites")
            bed_counts = [p["beds"] for p in matches]
            if len(set(bed_counts)) == 1:
                beds_text = f"{bed_counts[0]} at each of {len(matches)} sites"
            else:
                beds_text = " + ".join(str(n) for n in bed_counts) + " (by site)"
            ages = sorted({p["ages"] for p in matches if p["ages"]})
            return {
                **matches[0],
                "address": "; ".join(p["address"] for p in matches),
                "beds": sum(bed_counts),
                "beds_text": beds_text,
                "ages": ages[0] if len(ages) == 1 else " / ".join(ages),
                "sites": [{"address": p["address"], "beds": p["beds"], "ages": p["ages"]} for p in matches],
            }
        self.unmatched_providers.append(folder_name)
        return None

    def facility_info(self, folder: Dict, reports: List[Dict], provider: Optional[Dict],
                      program_name: str, on_list: bool = True) -> Dict:
        newest = sorted(reports, key=lambda r: r["report_date"], reverse=True)
        name = split_folder_name(folder["name"])[0]
        statements = [r for r in newest if r["categories"]["kind"] == "deficiencies"]
        letters = [r for r in newest if r["categories"]["kind"] == "no_deficiencies"]
        category = folder["category"]
        doc_type = next((r["categories"]["agency_type"] for r in statements if r["categories"].get("agency_type")), "")
        if folder["top"] == 19853 and doc_type:
            category = doc_type
        address = provider["address"] if provider else next(
            (r["categories"]["letter_address"] for r in letters if r["categories"].get("letter_address")), "")
        # The newest licence decision: a letter renews the licence; a statement
        # says what was granted, except surveys outside the renewal cycle
        # (complaints, follow-ups), which say "N/A" and decide nothing.
        action = ""
        for report in newest:
            if report["categories"]["kind"] == "no_deficiencies":
                action = "License renewed"
                break
            granted = report["categories"].get("license_granted") or ""
            if granted and granted.upper() != "N/A":
                action = f"{granted} license"
                break
        if not folder["listed"]:
            action = "No longer listed by the state" + (f" (last: {action})" if action else "")
        elif not on_list:
            # The state still keeps the folder, but the provider list of the
            # run date does not name the provider: closed, no longer licensed
            # for children's residential care, or renamed. Which one the
            # documents do not say, so only the fact is recorded.
            action = "Not on the state's current provider list" + (f" (last: {action})" if action else "")
        return {
            "facility_name": name,
            "program_name": program_name,
            "program_category": category,
            "full_address": address,
            "phone": provider["phone"] if provider else "",
            "bed_capacity": (provider.get("beds_text") or str(provider["beds"])) if provider and provider["beds"] else "",
            "executive_director": "",
            "license_exp_date": next((r["categories"]["license_end"] for r in newest[:1]
                                      if r["categories"].get("license_end")), ""),
            "relicense_visit_date": newest[0]["report_date"] if newest else "",
            "action": action,
        }

    def scrape(
        self,
        seen: Dict[str, Set[str]],
        state: Dict,
        limit: int = 0,
    ) -> Tuple[List[Dict], Dict[str, List[str]], Dict[str, Dict], Dict[str, str], Dict[str, str]]:
        known_folders = state.get("folders", {})
        program_names = dict(state.get("program_names", {}))
        posted_modified = state.get("docs", {})
        folders = self.facility_folders(known_folders)
        folders.sort(key=lambda f: f["name"].lower())
        logger.info(f"{sum(1 for f in folders if f['listed'])} facility folders listed by the state"
                    f", {sum(1 for f in folders if not f['listed'])} known and no longer listed")
        if limit:
            folders = folders[:limit]
        providers = self.provider_list()

        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        registry: Dict[str, Dict] = {}
        new_modified: Dict[str, str] = {}
        for index, folder in enumerate(folders, start=1):
            logger.info(f"[{index}/{len(folders)}] {folder['name']} ({folder['id']})"
                        + ("" if folder["listed"] else " [no longer listed]"))
            registry[folder["id"]] = {
                "name": folder["name"], "top": folder["top"],
                "last_listed": datetime.now().strftime("%Y-%m-%d") if folder["listed"] else folder.get("last_listed", ""),
            }
            try:
                docs = self.folder_documents(folder["id"])
            except (requests.RequestException, ValueError, KeyError) as exc:
                logger.error(f"  document list failed: {exc}")
                continue
            self.stats["documents_listed"] += len(docs)
            if not docs:
                continue
            already = seen.get(folder["id"], set())
            fresh_ids = {
                str(d["entryId"]) for d in docs
                if str(d["entryId"]) not in already
                or posted_modified.get(str(d["entryId"]), entry_times(d)[1]) != entry_times(d)[1]
            }
            if not fresh_ids:
                continue
            # Every document of the facility is read (from the local cache
            # after the first run) so the facility's licence, type and status
            # come from its newest documents; only the fresh ones are posted.
            former_name = split_folder_name(folder["name"])[1]
            reports = []
            for doc in docs:
                report = self.build_report(doc, folder, former_name)
                if report:
                    reports.append(report)
            if not reports:
                continue
            reports.sort(key=lambda r: r["report_date"], reverse=True)

            if folder["id"] not in program_names:
                licence = next((r["categories"]["license"] for r in reports
                                if r["categories"]["kind"] == "deficiencies"
                                and LICENSE_NUMBER.fullmatch(r["categories"].get("license") or "")), "")
                program_names[folder["id"]] = licence or f"ID-{folder['id']}"
            licences = {r["categories"]["license"] for r in reports if r["categories"].get("license")}
            if len(licences) > 1:
                self.stats["facilities_with_several_licences"] += 1
                logger.info(f"  licences across documents: {sorted(licences)}")

            provider = self.match_provider(folder["name"], providers) if providers else None
            if provider:
                # On the newest report only, as the facility's current details.
                reports[0]["categories"]["provider"] = {
                    "ages": provider["ages"], "male_beds": provider["male_beds"],
                    "female_beds": provider["female_beds"], "coed_beds": provider["coed_beds"],
                    "license_type": provider["license_type"], "listed": True,
                }
                if provider.get("sites"):
                    reports[0]["categories"]["provider"]["sites"] = provider["sites"]
            elif providers:
                reports[0]["categories"]["provider"] = {"listed": False}
            info = self.facility_info(folder, reports, provider, program_names[folder["id"]],
                                      on_list=bool(provider) or not providers)
            posting = [r for r in reports if r["report_id"] in fresh_ids]
            if not posting:
                continue
            for report in posting:
                new_modified[report["report_id"]] = report["modified"]
            facilities.append({"facility_info": info, "reports": posting})
            new_ids[folder["id"]] = [r["report_id"] for r in posting]
        self.note_shared_licences(facilities)
        if providers:
            wanted = {name_key(f["name"]) for f in folders}
            self.providers_without_folder = sorted({
                p["name"] for p in providers
                if name_key(p["name"]) not in wanted and "residential" in p["license_type"].lower()})
        chosen = {fid: program_names[fid] for fid in new_ids}
        return facilities, new_ids, registry, chosen, new_modified

    def note_shared_licences(self, facilities: List[Dict]) -> None:
        """Folders whose documents carry the same licence number (a renamed or
        taken-over programme keeps its licence): each such facility's newest
        report names the others, so the page can say so. Nothing is merged: a
        name belongs to its own years."""
        by_licence: Dict[str, List[Dict]] = {}
        for facility in facilities:
            for report in facility["reports"]:
                licence = report["categories"].get("license") or ""
                if LICENSE_NUMBER.fullmatch(licence):
                    group = by_licence.setdefault(licence, [])
                    if facility not in group:
                        group.append(facility)
        for licence, group in sorted(by_licence.items()):
            if len(group) < 2:
                continue
            names = [f["facility_info"]["facility_name"] for f in group]
            self.shared_licences.append(f"{licence}: " + "; ".join(names))
            for facility in group:
                own = facility["facility_info"]["facility_name"]
                newest = max(facility["reports"], key=lambda r: r["report_date"])
                newest["categories"]["same_license_as"] = [
                    {"name": other["facility_info"]["facility_name"], "license": licence,
                     "first": min(r["report_date"] for r in other["reports"]),
                     "last": max(r["report_date"] for r in other["reports"])}
                    for other in group if other["facility_info"]["facility_name"] != own]

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        log = logger.info
        log("-- Idaho run summary --")
        log(f"requests made: {self.client.requests_made} (PDFs downloaded: {self.downloads})")
        log(f"PDF archive folder: {self.reports.archive_dir}")
        log(f"documents listed: {self.stats.get('documents_listed', 0)}")
        log(f"facilities with reports to post: {len(facilities)}")
        log(f"reports: {len(reports)} (flagged: {sum(1 for r in reports if r['is_flagged'])})")
        if dates:
            log(f"date range: {dates[0]} to {dates[-1]}")
        by_year = Counter(d[:4] for d in dates)
        log("by year: " + ", ".join(f"{y} ({n})" for y, n in sorted(by_year.items())))
        log(f"  deficiencies: {self.stats.get('deficiencies', 0)}")
        log(f"  no_deficiencies: {self.stats.get('no_deficiencies', 0)}")
        log(f"  other (held back, not a licensing report): {self.stats.get('not_a_report', 0)}")
        log(f"deficiencies in all: {self.stats.get('deficiency_rows', 0)}, repeats: {self.stats.get('repeat_rows', 0)}")
        log("deficiencies per statement: " + ", ".join(
            f"{count}: {docs}" for count, docs in sorted(self.deficiencies_per_doc.items())))
        log(f"rows joined across a page break: {self.stats.get('rows_joined_across_pages', 0)}")
        log(f"deficiencies with no rule number read: {self.stats.get('rows_without_rule_number', 0)}")
        log(f"documents named one kind and reading as the other: {self.stats.get('name_and_content_differ', 0)}")
        log(f"facilities whose documents carry more than one licence number: "
            f"{self.stats.get('facilities_with_several_licences', 0)}")
        log(f"sub-folders inside facility folders: {self.stats.get('subfolders', 0)}")
        log("'License(s) Granted' values:")
        for value, count in self.granted.most_common():
            log(f"  {count:4d}  {value}")
        log("risk assessment (2026 form): " + (", ".join(f"{k} {v}" for k, v in self.risk.most_common()) or "none"))
        log(f"pages with text and no table row: {len(self.empty_table_pages)}")
        for line in self.empty_table_pages:
            log(f"  {line}")
        log(f"lines of page text found in no parsed deficiency: {len(self.unread)}")
        for line in self.unread:
            log(f"  {line}")
        log(f"pages where under 60% of the text is inside table cells: {len(self.low_coverage)}")
        for line in self.low_coverage:
            log(f"  {line}")
        log(f"dates taken from the document name: {len(self.loose_dates)}")
        for line in self.loose_dates:
            log(f"  {line}")
        log(f"complaint, investigation or follow-up wording: {len(self.complaint_notes)}")
        for line in self.complaint_notes:
            log(f"  {line}")
        log(f"folders with no provider list row: {len(self.unmatched_providers)}")
        for line in self.unmatched_providers:
            log(f"  {line}")
        log(f"folders matching more than one provider list row: {len(self.ambiguous_providers)}")
        for line in self.ambiguous_providers:
            log(f"  {line}")
        log(f"provider list residential rows with no survey folder: {len(self.providers_without_folder)}")
        for line in self.providers_without_folder:
            log(f"  {line}")
        log(f"licence numbers shared by several folders: {len(self.shared_licences)}")
        for line in self.shared_licences:
            log(f"  {line}")
        log(f"documents held back (not posted): {len(self.held)}")
        for item in self.held:
            log(f"  {item['facility']} / {item['document']} ({item['url']}): {item['reason']}")


INTERNAL_KEYS = {"is_flagged", "modified"}


def strip_internal(facilities: List[Dict]) -> List[Dict]:
    """Drop fields that are only for this script before posting."""
    out = []
    for facility in facilities:
        out.append({
            "facility_info": facility["facility_info"],
            "reports": [{k: v for k, v in r.items() if k not in INTERNAL_KEYS} for r in facility["reports"]],
        })
    return out


def write_out(path: Path, facilities: List[Dict], timestamp: str, held: List[Dict]) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = []
    for facility in strip_internal(facilities):
        shaped.append({
            "facility_info": facility["facility_info"],
            "reports": [{**report, "is_structured": True} for report in facility["reports"]],
        })
    payload = {
        "total_facilities": len(shaped),
        "source_state": "ID",
        "scraped_timestamp": timestamp,
        "scraping_notes": {
            "total_reports": sum(len(f["reports"]) for f in shaped),
            "held_back": held,
        },
        "facilities": shaped,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"Wrote {path}")


def save_to_api(facilities: List[Dict], timestamp: str) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="ID",
        scraped_timestamp=timestamp,
        facilities=strip_internal(facilities),
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Idaho children's residential licensing surveys")
    parser.add_argument("--full", action="store_true", help=f"Ignore the seen reports in {STATE_FILE}")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--cached", action="store_true",
                        help="Read folder listings and PDFs from the local cache only; the state's site is not contacted")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N facility folders (by name)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    if args.full:
        state = {**state, "docs": {}}
    timestamp = datetime.now().isoformat(timespec="seconds")

    scraper = IDScraper(cached_only=args.cached)
    facilities, new_ids, registry, program_names, new_modified = scraper.scrape(
        seen=seen, state=state, limit=args.limit)
    scraper.print_stats(facilities)

    # The folder registry is not tied to a post: it only remembers folder ids,
    # so a facility whose folder leaves the list is still asked for.
    state.setdefault("folders", {}).update(registry)
    save_state(STATE_FILE, state)

    if args.out:
        write_out(args.out, facilities, timestamp, scraper.held)
    if not facilities:
        logger.info("No new reports since last run")
        return
    if args.no_post:
        logger.info("Skipping API POST because --no-post was set; seen reports not advanced")
        return
    if save_to_api(facilities, timestamp):
        merge_new_ids(state, new_ids)
        # A facility's program_name is fixed once it has been posted: a change
        # would split the facility into two rows.
        for folder_id, program_name in program_names.items():
            state.setdefault("program_names", {}).setdefault(folder_id, program_name)
        state.setdefault("docs", {}).update(new_modified)
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- seen reports not advanced")


if __name__ == "__main__":
    main()
