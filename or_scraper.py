"""
Oregon ODHS Children's Care Licensing report scraper.

This scraper targets the public SharePoint document library that backs the
Oregon report pages the user identified:

- https://www.oregon.gov/odhs/licensing/childrens-care-agencies/Pages/rc.aspx
- https://www.oregon.gov/odhs/licensing/childrens-care-agencies/Pages/tbs.aspx

The public pages themselves are client-rendered SharePoint views. The scraper
calls the same anonymous SharePoint SOAP endpoint behind those pages to get the
report rows directly, downloads the linked PDFs, extracts text, then posts the
grouped facility/report payload to the shared inspections API.

Complaints: ODHS publishes no complaint documents per program. The same library
holds a quarterly "Child Caring Agency Legislative Report" (Pages/reports.aspx)
listing every abuse report substantiated at a child caring agency. Each one is
posted as its own report with categories.kind = "complaint", under the
provider's name (see ABUSE_PROVIDER_NAMES).
"""

import argparse
import html
import json
import logging
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from xml.etree import ElementTree as ET

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

BASE_URL = "https://www.oregon.gov/odhs/licensing/childrens-care-agencies"
REPORT_LIBRARY_NAME = "reports"
AGENCY_LIST_NAME = "agencies"
REPORTS = ReportStore("OR_PDF_CACHE", "or_pdfs", Path(__file__).parent / "or_pdfs")
STATE_FILE = Path(os.getenv("OR_STATE_FILE", ".or_state.json"))

VIEWS = [
    {
        "code": "RC",
        "page_path": "/Pages/rc.aspx",
        # SharePoint view slug (basename of the view's Forms/<slug>.aspx URL).
        # The numeric GUID is recreated whenever ODHS rebuilds the view, so it is
        # resolved at runtime from this slug; view_id is only a last-resort fallback.
        "view_slug": "rc",
        "view_id": "{BAB5E212-1FE4-4320-8D15-E523E2EDBF3B}",
        "program_type": "(RC) Residential Care Programs",
    },
    {
        "code": "TBS",
        "page_path": "/Pages/tbs.aspx",
        "view_slug": "tbs",
        "view_id": "{CD4CE9D6-BC46-496F-9824-63ED05D7C569}",
        "program_type": "(TBS) Therapeutic Boarding Schools",
    },
]
VIEWS_BY_CODE = {view["code"]: view for view in VIEWS}

OREGON_INLINE_STOP_PATTERNS = [
    r"Date of site visit",
    r"Date of Unannounced",
    r"Executive Director",
    r"Program Director(?:\(s\))?",
    r"(?:Juvenile Services|Clinical|Assistant|Residential) Director",
    r"Residential Manager",
    r"Board Chairperson",
    r"Licensing Coordinator",
    r"Other Regulatory or Accrediting Agencies",
    r"Purpose",
    r"Program Compliance",
    r"Program Description(?:\(s\))?",
    r"Program type and services",
    r"Capacity and age-range",
    r"Capacity and Age Range",
    r"Funding sources",
    r"Contracts and sources for referrals",
    r"Average length of stay",
    r"Average daily population served",
    r"Number of children served annually",
    r"Use of seclusion or restraint",
    r"Interviews, Observations",
    r"Program Strengths",
    r"Program Challenges",
    r"Changes that have occurred in the last 2 years",
    r"Changes that have occurred in the last two years",
    r"Lawsuits",
    r"Grievances and complaints filed in the last two years",
    r"Corrective Actions and Timeframes",
    r"Recommendations",
    r"Exceptions",
    r"Changes in License",
    r"Summary of Review",
]

OREGON_BLOCK_SECTION_LABELS = {
    "interview_summary": [r"Interview Summary"],
    "observations": [r"Observations"],
    "previous_findings": [r"Previous Findings"],
    "new_findings": [r"New Findings from Site Visit Comments"],
}

OREGON_BLOCK_SECTION_STOP_PATTERNS = [
    r"Interview Summary",
    r"Observations",
    r"Corrective Actions and Timeframes",
    r"Recommendations",
    r"Exceptions",
    r"Changes in License",
    r"Summary of Review",
    r"Program Strengths",
    r"Program Challenges",
    r"Changes that have occurred in the last 2 years",
    r"Changes that have occurred in the last two years",
    r"Lawsuits",
    r"Grievances and complaints filed in the last two years",
    # Form-footer boilerplate that should never be captured as section content
    r"Please submit the following",
    r"Licensing Coordinator(?:'s)?\s+Signature",
    r"Manager Review",
]

OREGON_FINDINGS_SECTION_LABELS = [
    r"Summary of Review",
    r"Previous Findings",
    r"New Findings from Site Visit Comments",
]


def clean_text(value: Optional[str]) -> str:
    return html.unescape(str(value or "")).strip()


def split_sharepoint_value(value: Optional[str]) -> List[str]:
    clean = clean_text(value)
    if not clean:
        return []
    parts = [part.strip() for part in clean.split(";#") if part.strip()]
    return parts


def sharepoint_lookup_id(value: Optional[str]) -> str:
    parts = split_sharepoint_value(value)
    if parts and parts[0].isdigit():
        return parts[0]
    return ""


def sharepoint_lookup_label(value: Optional[str]) -> str:
    parts = split_sharepoint_value(value)
    if len(parts) >= 2 and parts[0].isdigit():
        return parts[1]
    if len(parts) == 1:
        return "" if parts[0].isdigit() else parts[0]
    if parts:
        return " | ".join(parts)
    return clean_text(value)


def strip_placeholder(value: Optional[str]) -> str:
    text = clean_text(value)
    if not text or text.upper() == "N/A":
        return ""
    return text


def normalize_report_date(value: Optional[str]) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%m/%-d/%Y", "%m/%-d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    # Windows/Python on this machine may not support %-d, so try a regex fallback.
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", raw)
    if m:
        month, day, year = m.groups()
        if len(year) == 2:
            year = f"20{year}"
        try:
            return datetime(int(year), int(month), int(day)).strftime("%m/%d/%Y")
        except ValueError:
            return raw
    return raw


def parse_meta_info(raw_meta: Optional[str]) -> Dict[str, str]:
    meta: Dict[str, str] = {}
    text = html.unescape(raw_meta or "").replace("\r", "\n")
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"^\d+;#", "", line)
        if ":" not in line:
            continue
        key, remainder = line.split(":", 1)
        value = remainder.split("|", 1)[1] if "|" in remainder else remainder
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if key not in meta or (not meta[key] and value):
            meta[key] = value
    return meta


def build_pdf_url(file_ref: Optional[str]) -> str:
    path = sharepoint_lookup_label(file_ref).lstrip("/")
    if not path:
        return ""
    return f"https://www.oregon.gov/{path}"


def extract_primary_url(raw_value: Optional[str]) -> str:
    text = clean_text(raw_value)
    if not text:
        return ""
    match = re.search(r"https?://[^\s,]+", text)
    return match.group(0) if match else text.split(",", 1)[0].strip()


def best_nonempty(values: Iterable[str]) -> str:
    for value in values:
        if value:
            return value
    return ""


def pick_most_common(values: Iterable[str]) -> str:
    filtered = [value for value in values if value]
    if not filtered:
        return ""
    return Counter(filtered).most_common(1)[0][0]


def sort_key_for_report_date(value: str) -> tuple:
    try:
        return (0, datetime.strptime(value, "%m/%d/%Y"))
    except ValueError:
        return (1, value or "")


def extract_pdf_text(path: Path) -> str:
    try:
        with pdfplumber.open(path) as pdf:
            pages = [(page.extract_text() or "") for page in pdf.pages]
        return "\n".join(page for page in pages if page).strip()
    except Exception as exc:
        logger.warning(f"  PDF extract failed for {path.name}: {exc}")
        return ""


# ---------------------------------------------------------------------------
# Substantiated abuse reports (the quarterly "Child Caring Agency Legislative
# Report"). ODHS publishes no per-program complaint documents; each quarter it
# lists every abuse report it substantiated at a child caring agency: report
# number, provider, incident date, abuse type, whether injury, sexual abuse or
# death resulted, a narrative and the corrective actions. The PDFs come in
# three table layouts (2021-2024 Q3 four columns with the labels inside the
# text cells, 2024 Q4 onward a header row plus a narrative table, 2025 Q3 one
# label/value row per field), and a long entry continues in a bare two-cell
# table on the next page.
# ---------------------------------------------------------------------------

ABUSE_EXTRACT_VERSION = 1   # bump after changing the table reader, so cached extracts are redone
ABUSE_REPORT_ID_RE = re.compile(r"\bCC[A-Z]\s?\d{5,}[A-Z]?\b")
ABUSE_STATS_ROW_RE = re.compile(r"^(Reporting time frame|The total number|The number of|Measure$)", re.I)
ABUSE_HEADER_FRAGMENT_RE = re.compile(
    r"^(Report/\s*Allegation|Provider|Approx(imate|\.)?(\s+(incident\s+)?date.*)?|incident|date|Abuse type|"
    r"Did (physical|phys\.|reportable).*|injury, sexual|abuse or death|result\?|"
    r"Nature of abuse and brief narrative|Corrective actions taken or ordered by the|"
    r"Department, and outcome)$",
    re.I,
)
ABUSE_NARRATIVE_LABEL_RE = re.compile(r"^Nature of abuse and brief\s+narrative\s*:?\s*", re.I)
ABUSE_CORRECTIVE_LABEL_RE = re.compile(
    r"^Corrective actions taken\s+or ordered by the\s+Department, and\s+outcome\s*:?\s*", re.I
)
ABUSE_KV_LABELS = [
    (re.compile(r"^Report/\s*allegation$", re.I), "report_number"),
    (re.compile(r"^Provider$", re.I), "provider"),
    (re.compile(r"^Approximate (incident )?date( abuse occurred)?$", re.I), "incident_date"),
    (re.compile(r"^Abuse type$", re.I), "abuse_type"),
    (re.compile(r"^Did (physical|reportable) injury, sexual abuse or death result\?$", re.I), "injury_result"),
    (re.compile(r"^Nature of abuse and brief narrative:?$", re.I), "narrative"),
    (re.compile(r"^Corrective actions taken or ordered by the Department, and outcome:?$", re.I), "corrective_actions"),
]
# Abuse types as ORS 418.257 names them, for the early reports that have no
# "Abuse type" column and only name it in the narrative.
ABUSE_TYPES = [
    "Neglect", "Sexual Abuse", "Physical Abuse", "Wrongful Restraint", "Involuntary Seclusion",
    "Threat of Harm", "Financial Exploitation", "Verbal Abuse", "Mental Injury", "Abandonment",
    "Sexual Exploitation", "Maltreatment",
]


# Facility name each provider's abuse reports are filed under. ODHS spells the
# same provider several ways and often names only the agency. A name that
# matches a site-visit program (same spelling as its facility_name) puts the
# abuse reports beside that program's visits on /or-reports/; an agency named
# without a program stays at the agency, never guessed onto one of its homes.
# A provider not listed is filed under the name as printed and logged.
ABUSE_REPORTS_SOURCE_PAGE = f"{BASE_URL}/Pages/reports.aspx"
ABUSE_REPORTS_STATE_KEY = "ODHS quarterly legislative reports"
ABUSE_REPORTS_PROGRAM_NAME = "ODHS substantiated abuse reports"
ABUSE_REPORTS_CATEGORY = "Child caring agency"
ABUSE_PROVIDER_NAMES = {
    "adapt": "ADAPT",
    "bob belloni ranch": "Bob Belloni Ranch",
    "connections365": "Connections365",
    "dragonfly adventures": "Dragonfly Adventures",
    "janus youth programs": "Janus Youth Programs",
    "janus youth programs cordero house": "Janus Youth Programs - Cordero House",
    "jasper mountain": "Jasper Mountain",
    "josephine county juvenile shelter": "Josephine County Juvenile Shelter",
    "looking glass community services": "Looking Glass Community Services",
    "madrona recovery": "Madrona Recovery",
    "maple star oregon": "Maple Star Oregon",
    "team bailey": "Team Bailey",
    "trillium family services": "Trillium Family Services",
    "youth progress association": "Youth Progress Association",
    "albertina kerr": "Albertina Kerr Centers",
    "albertina kerr centers": "Albertina Kerr Centers",
    "family solutions": "Family Solutions",
    "greater oregon behavioral health inc": "Greater Oregon Behavioral Health Inc. (GOBHI)",
    "greater oregon behaviorial health inc": "Greater Oregon Behavioral Health Inc. (GOBHI)",
    "j bar j": "J Bar J Youth Services",
    "j bar j youth services": "J Bar J Youth Services",
    "jasper safe center": "Jasper Mountain - SAFE Center",
    "jasper mountain safe center": "Jasper Mountain - SAFE Center",
    "looking glass regional crisis center": "Looking Glass Community Services - RCC",
    "looking glass pathway for girls": "Looking Glass Community Services - Pathway for Girls",
    "morrison center": "Morrison Child and Family Services",
    "nara youth residential treatment center": "Native American Rehabilitation Association of the Northwest, Inc. (NARA)",
    "new roads community counseling solutions": "Community Counseling Solutions - New Roads",
    "next door inc": "The Next Door",
    "parrott creek child and family services": "Parrott Creek Children and Family Svs",
    "rimrock trails": "Rimrock Trails Treatment Services",
    "rimrock trails atc": "Rimrock Trails Treatment Services",
    "st marys home": "St Mary's Home for Boys",
    "st mary s home for boys": "St Mary's Home for Boys",
    "trillium": "Trillium Family Services",
    "trillium farm home": "Trillium Family Services - Children's Farm Home",
    "trillium children s farm home": "Trillium Family Services - Children's Farm Home",
    "trillium parry center": "Parry Center",
    "trillium sagebrush": "Trillium Family Services - Sagebrush",
}


def _abuse_cell(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _abuse_join(existing: str, more: str) -> str:
    return f"{existing} {more}".strip() if more else existing


def _abuse_is_header(cell: str) -> bool:
    """A column heading or a bare field label, never report text."""
    return bool(
        ABUSE_HEADER_FRAGMENT_RE.match(cell)
        or (ABUSE_NARRATIVE_LABEL_RE.match(cell) and not ABUSE_NARRATIVE_LABEL_RE.sub("", cell))
        or (ABUSE_CORRECTIVE_LABEL_RE.match(cell) and not ABUSE_CORRECTIVE_LABEL_RE.sub("", cell))
    )


def _abuse_stray(cell: str) -> bool:
    """A lone word the table grid cut off a neighbouring column's line."""
    return bool(cell) and " " not in cell and not re.search(r"[.!?]$", cell)


def _abuse_type_label(raw: str) -> str:
    """"WrongfulRestraint" (the type printed after the report number, its
    line break lost) -> "Wrongful Restraint"."""
    squeezed = re.sub(r"\s+", "", raw or "").lower()
    for name in ABUSE_TYPES:
        if name.replace(" ", "").lower() == squeezed:
            return name
    return (raw or "").strip()


def parse_abuse_report_tables(tables: List[List[List[Optional[str]]]]) -> List[Dict[str, str]]:
    """Entries of one legislative report, from its tables in reading order.

    Each entry: report_number, allegation_count, provider, incident_date,
    abuse_type, injury_result, narrative, corrective_actions (all as printed).
    """
    entries: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None
    expect_narrative = False   # a "Nature of abuse" header row was read; the texts follow
    last_kv_field = ""         # label/value layout: the field a bare continuation row extends
    kv_mode = False            # the current entry is in the label/value layout

    def start(report_number: str) -> Dict[str, str]:
        number = _abuse_cell(report_number)
        count = ""
        m = re.search(r"\((\d+)\s*allegations?\)", number, re.I)
        if m:
            count = m.group(1)
            number = number[:m.start()].strip()
        # 2023 Q3 and 2024 Q1 print the abuse type after the number: "CCA230031/ Neglect".
        number, _, abuse_type = number.partition("/")
        entry = {
            "report_number": number.replace(" ", ""), "allegation_count": count, "provider": "",
            "incident_date": "", "abuse_type": _abuse_type_label(abuse_type), "injury_result": "", "narrative": "",
            "corrective_actions": "",
        }
        entries.append(entry)
        return entry

    for table in tables:
        for row in table:
            present = [_abuse_cell(c) for c in row if c is not None]
            cells = [c for c in present if c]
            if not cells:
                continue
            if ABUSE_STATS_ROW_RE.match(cells[0]):
                # The restraint and seclusion totals close the report.
                return entries

            # Label/value layout (2025 Q3): one field per row.
            kv_field = next((f for rx, f in ABUSE_KV_LABELS if rx.match(cells[0])), "")
            if kv_field == "report_number" and len(cells) == 2 and ABUSE_REPORT_ID_RE.search(cells[1]):
                current = start(cells[1])
                expect_narrative, last_kv_field, kv_mode = False, "report_number", True
                continue
            if kv_field and kv_field != "report_number" and current is not None and (
                    (len(cells) == 2 and not _abuse_is_header(cells[1])) or (len(cells) == 1 and kv_mode)):
                value = cells[1] if len(cells) == 2 else ""
                current[kv_field] = _abuse_join(current[kv_field], value)
                expect_narrative, last_kv_field = False, kv_field
                continue

            # Column layouts: the row under the header holds the report.
            if ABUSE_REPORT_ID_RE.match(cells[0]) and len(cells) >= 3:
                current = start(cells[0])
                expect_narrative, last_kv_field, kv_mode = False, "", False
                current["provider"] = cells[1]
                current["incident_date"] = cells[2]
                if len(cells) >= 5:
                    current["abuse_type"] = cells[3]
                    current["injury_result"] = cells[4]
                elif len(cells) == 4:
                    current["injury_result"] = cells[3]
                continue

            # 2021-2024 Q3: label and text share a cell.
            labelled = False
            for cell in cells:
                if ABUSE_NARRATIVE_LABEL_RE.match(cell) and ABUSE_NARRATIVE_LABEL_RE.sub("", cell):
                    if current is not None:
                        current["narrative"] = _abuse_join(current["narrative"], ABUSE_NARRATIVE_LABEL_RE.sub("", cell))
                    labelled = True
                elif ABUSE_CORRECTIVE_LABEL_RE.match(cell) and ABUSE_CORRECTIVE_LABEL_RE.sub("", cell):
                    if current is not None:
                        current["corrective_actions"] = _abuse_join(
                            current["corrective_actions"], ABUSE_CORRECTIVE_LABEL_RE.sub("", cell))
                    labelled = True
            if labelled:
                expect_narrative, last_kv_field = False, ""
                continue

            if all(_abuse_is_header(c) for c in cells) or (
                    len(cells) >= 3 and re.match(r"^Report/\s*Allegation$", cells[0], re.I)):
                if any(ABUSE_NARRATIVE_LABEL_RE.match(c) for c in cells):
                    expect_narrative = True
                continue

            if current is None:
                continue
            # Narrative | corrective actions, or the rest of them after a page break.
            if len(present) == 2:
                if last_kv_field and not present[0]:
                    current[last_kv_field] = _abuse_join(current[last_kv_field], present[1])
                else:
                    if not _abuse_stray(present[0]):
                        current["narrative"] = _abuse_join(current["narrative"], present[0])
                    if not _abuse_stray(present[1]):
                        current["corrective_actions"] = _abuse_join(current["corrective_actions"], present[1])
                expect_narrative = False
            elif len(cells) == 1 and (expect_narrative or current["narrative"]) and not last_kv_field:
                current["narrative"] = _abuse_join(current["narrative"], cells[0])
                expect_narrative = False
            elif len(cells) == 1 and last_kv_field:
                current[last_kv_field] = _abuse_join(current[last_kv_field], cells[0])

    return entries


def extract_abuse_report_entries(path: Path) -> Dict[str, Any]:
    """Entries of a legislative report PDF, plus how many report numbers its
    text holds so a layout the table reader misses shows up in the log."""
    try:
        with pdfplumber.open(path) as pdf:
            tables: List[List[List[Optional[str]]]] = []
            texts: List[str] = []
            for page in pdf.pages:
                tables.extend(page.extract_tables() or [])
                texts.append(page.extract_text() or "")
    except Exception as exc:
        logger.warning(f"  PDF extract failed for {path.name}: {exc}")
        return {}
    text = "\n".join(texts)
    cut = re.search(r"Restraint and Involuntary Seclusion Report|Reporting time frame", text)
    body = text[:cut.start()] if cut else text
    numbers = {m.group(0).replace(" ", "") for m in ABUSE_REPORT_ID_RE.finditer(body)}
    return {"text": text, "entries": parse_abuse_report_tables(tables), "numbers_in_text": len(numbers)}


def abuse_types_of(entry: Dict[str, str]) -> List[str]:
    """Abuse types of an entry: its "Abuse type" cell, else the ones its
    narrative names."""
    source = entry.get("abuse_type") or entry.get("narrative") or ""
    found = [(m.start(), name) for name in ABUSE_TYPES
             for m in [re.search(rf"\b{re.escape(name)}\b", source, re.I)] if m]
    return [name for _, name in sorted(found)]


def abuse_incident_date(printed: str, quarter: str) -> str:
    """MM/DD/YYYY for the report row: the incident date when one is printed
    ("03/2021" = the 1st), else the first day of the report's quarter."""
    raw = (printed or "").strip()
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", raw)
    if m:
        return normalize_report_date(m.group(0))
    m = re.search(r"\b(\d{1,2})/(\d{4})\b", raw)
    if m and 1 <= int(m.group(1)) <= 12:
        return f"{int(m.group(1)):02d}/01/{m.group(2)}"
    m = re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b.*?\b((?:19|20)\d{2})\b", raw, re.I)
    if m:
        month = "jan feb mar apr may jun jul aug sep oct nov dec".split().index(m.group(1).lower()) + 1
        return f"{month:02d}/01/{m.group(2)}"
    if re.fullmatch(r"(?:19|20)\d{2}", raw):
        return f"01/01/{raw}"
    m = re.fullmatch(r"(\d{4})-?Q([1-4])", (quarter or "").strip(), re.I)
    if m:
        return f"{(int(m.group(2)) - 1) * 3 + 1:02d}/01/{m.group(1)}"
    return ""


def norm_provider_name(name: str) -> str:
    n = (name or "").lower().replace("’", "'")
    n = re.sub(r"\([^)]*\)", " ", n)
    n = re.sub(r"\s*&\s*", " and ", n)
    n = re.sub(r"[^a-z0-9\s]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def abuse_provider_facility_name(provider: str) -> Tuple[str, bool]:
    """(facility name the provider's abuse reports go under, known provider?)."""
    known = ABUSE_PROVIDER_NAMES.get(norm_provider_name(provider))
    if known:
        return known, True
    printed = re.sub(r"\s*[–—]\s*", " - ", (provider or "").replace("’", "'")).strip()
    return printed, False


def build_abuse_report(entry: Dict[str, str], source: Dict[str, str]) -> Dict:
    """One substantiated abuse report as an inspections API report record.
    `source` is the quarterly PDF: quarter, pdf_url, file_name."""
    types = abuse_types_of(entry)
    type_label = ", ".join(types) or entry.get("abuse_type", "")
    harm = entry.get("injury_result", "")
    lines = [
        f"Substantiated abuse report {entry['report_number']}",
        f"Provider: {entry['provider']}",
        f"Approximate incident date: {entry['incident_date']}",
        f"Abuse type: {type_label}" if type_label else "",
        f"Did reportable injury, sexual abuse or death result? {harm}" if harm else "",
        f"Nature of abuse: {entry['narrative']}" if entry["narrative"] else "",
        f"Corrective actions and outcome: {entry['corrective_actions']}" if entry["corrective_actions"] else "",
        f"Source: ODHS Child Caring Agency Legislative Report, {source['quarter']}",
    ]
    raw_content = "\n".join(line for line in lines if line)
    categories = {
        "kind": "complaint",
        "report_type": "Substantiated abuse report",
        "finding": "Substantiated",
        "report_number": entry["report_number"],
        "provider": entry["provider"],
        "incident_date": entry["incident_date"],
        "abuse_type": type_label,
        "abuse_types": types,
        "allegation_count": entry.get("allegation_count", ""),
        "injury_result": harm,
        "harm_resulted": bool(re.match(r"\s*yes", harm, re.I)),
        "narrative": entry["narrative"],
        "corrective_actions": entry["corrective_actions"],
        "quarter": source["quarter"],
        "source_page": ABUSE_REPORTS_SOURCE_PAGE,
        "pdf_url": source["pdf_url"],
        "file_name": source["file_name"],
    }
    return {
        "report_id": entry["report_number"],
        "report_date": abuse_incident_date(entry["incident_date"], source["quarter"]),
        "report_url": source["pdf_url"],
        "pdf_url": source["pdf_url"],
        "raw_content": raw_content,
        "content_length": len(raw_content),
        "summary": f"Substantiated abuse report: {type_label}" if type_label else "Substantiated abuse report",
        "categories": categories,
    }


def build_abuse_facilities(parsed_quarters: List[Tuple[Dict[str, str], List[Dict[str, str]]]]) -> List[Dict]:
    """Facility payloads from [(source, entries)] of every quarterly report.
    One facility per provider; a report number printed in two quarters is
    kept once, from the later one."""
    by_facility: Dict[str, Dict[str, Dict]] = defaultdict(dict)
    for source, entries in parsed_quarters:
        for entry in entries:
            if not entry.get("report_number") or not entry.get("provider"):
                logger.warning(f"  {source['file_name']}: skipped an entry without a report number or provider")
                continue
            name, _ = abuse_provider_facility_name(entry["provider"])
            by_facility[name][entry["report_number"]] = build_abuse_report(entry, source)

    facilities = []
    for name in sorted(by_facility):
        reports = sorted(
            by_facility[name].values(),
            key=lambda report: (sort_key_for_report_date(report["report_date"]), report["report_id"]),
        )
        facilities.append({
            "facility_info": {
                "facility_name": name,
                # Its own row beside the program's site-visit row (the API keys a
                # facility on name + program_name), so posting abuse reports never
                # overwrites a licensed program's details.
                "program_name": ABUSE_REPORTS_PROGRAM_NAME,
                "program_category": ABUSE_REPORTS_CATEGORY,
                "full_address": "",
                "phone": "",
                "bed_capacity": "",
                "executive_director": "",
                "license_exp_date": "",
                "relicense_visit_date": "",
                "action": "",
                "agency_name": name,
            },
            "reports": reports,
        })
    return facilities


def extract_checklist_findings(path: Path) -> Optional[List[Dict[str, str]]]:
    """
    Parse Oregon checklist PDFs using pdfplumber's table extractor so we read
    the actual column grid — Rule | Yes | No | N/A | Corrective Actions/Comments
    — rather than guessing column positions from flattened text.

    Returns a list of {rule, excerpt} dicts for every row where the 'No' column
    is checked (or where the comment contains a CORRECTIVE ACTION), or None when
    the PDF is not in checklist format.

    Structural challenges handled:
    - Rule numbers appear in column 1 (standalone header row) OR column 0 (combined).
    - Tables span page breaks: the continuation table on the next page has no
      header row.  Column indices are persisted and reused for headerless tables.
    - Some tables have data rows BEFORE their header row (rows from the previous
      section that didn't fit on the prior page).  All rows are processed, not
      just rows after the header.
    - "Not a finding" notes sometimes appear in the same comment cell as a real
      corrective action.  Only the "not a finding" paragraph is stripped.
    """
    RULE_RE = re.compile(r"\b\d{3}-\d{3}-\d{4}")
    found_checklist = False
    current_rule: Optional[str] = None
    raw_findings: List[Dict[str, str]] = []

    # Persist column config across tables so continuation tables (no header row)
    # can still be processed.
    active_no_col: Optional[int] = None
    active_comment_col: Optional[int] = None

    def _cell(row: list, idx: Optional[int]) -> str:
        if idx is None or idx >= len(row):
            return ""
        return str(row[idx] or "").strip()

    def _clean_comment(text: str) -> str:
        """Remove 'not a finding' paragraphs (and their preceding context paragraph)
        from a comment cell while preserving genuine corrective action text."""
        if not text:
            return ""
        if not re.search(r"not\s+a\s+finding", text, re.IGNORECASE):
            return text
        parts = re.split(r"\*{3,}", text)
        kept: List[str] = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if re.search(r"not\s+a\s+finding", part, re.IGNORECASE):
                # Also remove the preceding context paragraph that this note refers to.
                if kept:
                    kept.pop()
                continue
            kept.append(part)
        return "\n".join(kept)

    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    if not table:
                        continue

                    # PASS 1: scan the entire table for a Yes / No / N/A header row.
                    # (The header may appear after some data rows when the previous
                    # section spilled onto this page without a new header.)
                    header_idx: Optional[int] = None
                    no_col = comment_col = None
                    for row_idx, row in enumerate(table):
                        cells = [str(c or "").strip() for c in row]
                        if "Yes" in cells and "No" in cells and "N/A" in cells:
                            no_col = cells.index("No")
                            for j, c in enumerate(cells):
                                if "corrective" in c.lower() or "comment" in c.lower():
                                    comment_col = j
                                    break
                            if comment_col is None:
                                comment_col = len(cells) - 1
                            header_idx = row_idx
                            found_checklist = True
                            active_no_col = no_col
                            active_comment_col = comment_col
                            break

                    # No header found — reuse the persisted config from the previous
                    # table (continuation table that crosses a page break).
                    if no_col is None:
                        if (active_no_col is not None
                                and table[0]
                                and len(table[0]) > active_no_col):
                            no_col = active_no_col
                            comment_col = active_comment_col
                        else:
                            continue  # No usable config yet; skip.

                    # PASS 2: process every row (including rows before the header).
                    for row_idx, row in enumerate(table):
                        if row_idx == header_idx or not row:
                            continue

                        # Skip internal section-separator rows that repeat the
                        # Yes / No / N/A header label mid-table.
                        cells = [str(c or "").strip() for c in row]
                        if "Yes" in cells and "No" in cells and "N/A" in cells:
                            continue

                        col0 = _cell(row, 0)
                        col1 = _cell(row, 1)
                        no_val = _cell(row, no_col)
                        raw_comment = _cell(row, comment_col)

                        # Rule numbers appear in col 0 (combined rule+description)
                        # or in col 1 (standalone rule-number row, description in col 0
                        # of subsequent rows).
                        m0 = RULE_RE.search(col0) if col0 else None
                        m1 = RULE_RE.search(col1) if col1 else None

                        if m1 and not m0:
                            # Standalone rule-number row — update tracker, no checkbox.
                            current_rule = m1.group(0)
                            continue
                        if m0:
                            current_rule = m0.group(0)

                        # Continuation row: no description, no checkbox, but has a
                        # comment.  Append the comment text to the last finding so
                        # multi-row corrective actions are not lost.
                        if not col0 and no_val == "" and raw_comment and raw_findings:
                            comment_clean = _clean_comment(raw_comment)
                            if comment_clean:
                                last = raw_findings[-1]
                                if comment_clean[:40] not in last["excerpt"]:
                                    last["excerpt"] = (
                                        last["excerpt"] + " " + comment_clean
                                    ).strip()
                            continue

                        # Require an explicit colon to distinguish "CORRECTIVE ACTION:"
                        # in cell content from the column header "Corrective Actions/Comments".
                        has_corrective = bool(
                            re.search(r"CORRECTIVE\s+ACTION\s*:", raw_comment, re.IGNORECASE)
                        )
                        is_no_checked = no_val.upper() == "X"

                        if (is_no_checked or has_corrective) and current_rule:
                            comment_clean = _clean_comment(raw_comment)
                            # Skip rows whose entire comment is a "not a finding" note.
                            if not comment_clean and re.search(
                                r"not\s+a\s+finding", raw_comment, re.IGNORECASE
                            ):
                                continue
                            desc = re.sub(r"^\d{3}-\d{3}-\d{4}\S*\s*", "", col0).strip()
                            parts = [p for p in (desc, comment_clean) if p]
                            excerpt = " ".join(parts).strip()
                            if excerpt:
                                raw_findings.append({"rule": current_rule, "excerpt": excerpt})

    except Exception as exc:
        logger.warning(f"  Table checklist parsing failed for {path.name}: {exc}")
        return None

    if not found_checklist:
        return None

    # Deduplicate while preserving order.
    seen: set = set()
    deduped: List[Dict[str, str]] = []
    for f in raw_findings:
        key = (f["rule"], f["excerpt"][:160])
        if key not in seen:
            seen.add(key)
            deduped.append(f)
    return deduped


def normalize_oregon_pdf_text(text: Optional[str]) -> str:
    normalized = (text or "").replace("\r", "\n").replace("\xa0", " ")
    replacements = {
        "\uf0b7": "- ",
        "\uf0fc": "",
        "\u2018": "'",   # left single quotation mark
        "\u2019": "'",   # right single quotation mark / apostrophe
        "\u201c": '"',   # left double quotation mark
        "\u201d": '"',   # right double quotation mark
        "\u2610": "[ ]",
        "\u2611": "[x]",
        "\u25cf": "- ",
        "\u2022": "- ",
        "\u2013": "-",
        "\u2014": "-",
    }
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)

    normalized = re.sub(r"(?im)^\s*\d+\s*\|?\s*P\s*a\s*g\s*e.*$", "", normalized)
    normalized = re.sub(r"(?im)^\s*Form Rev\..*$", "", normalized)
    normalized = re.sub(r"(?im)^\s*\(rev\.[^)]+\)\s*$", "", normalized)
    normalized = re.sub(r"(?im)^\s*I:\\LICENSING\\.*$", "", normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def collapse_inline_whitespace(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", clean_text(value)).strip(" :-")


def clean_section_text(value: Optional[str]) -> str:
    cleaned = clean_text(value)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip(" \n:-")


def truncate_at_embedded_label(
    value: Optional[str],
    stop_patterns: Optional[List[str]] = None,
) -> str:
    text = clean_text(value)
    if not text:
        return ""

    stop_patterns = stop_patterns or OREGON_INLINE_STOP_PATTERNS
    cut_at: Optional[int] = None
    for label in stop_patterns:
        match = re.search(rf"\s+(?={label}\s*:)", text, flags=re.IGNORECASE)
        if not match:
            continue
        if cut_at is None or match.start() < cut_at:
            cut_at = match.start()

    if cut_at is not None:
        text = text[:cut_at]

    return text.strip()


def extract_labeled_block(
    text: str,
    labels: List[str],
    stop_patterns: Optional[List[str]] = None,
) -> str:
    if not text:
        return ""

    stop_patterns = stop_patterns or OREGON_INLINE_STOP_PATTERNS
    stop_re = "|".join(stop_patterns)

    for label in labels:
        pattern = re.compile(
            rf"(?is)(?:^|\n|\s){label}\s*:\s*(.+?)(?=\n(?:{stop_re})\s*:?|\Z)"
        )
        match = pattern.search(text)
        if match:
            return clean_section_text(
                truncate_at_embedded_label(match.group(1), stop_patterns)
            )
    return ""


def extract_named_block_section(text: str, labels: List[str]) -> str:
    if not text:
        return ""

    stop_re = "|".join(OREGON_BLOCK_SECTION_STOP_PATTERNS)
    for label in labels:
        pattern = re.compile(
            rf"(?is)(?:^|\n){label}\s*:?\s*(.+?)(?=(?:\n)(?:{stop_re})\s*:?\s*|\Z)"
        )
        match = pattern.search(text)
        if match:
            return clean_section_text(match.group(1))
    return ""


def extract_findings(text: str, no_column_rules: Optional[set] = None) -> List[Dict[str, str]]:
    if not text:
        return []

    # Detect checklist-style PDFs (Yes/No/N/A column format). In these
    # documents every rule has an X mark regardless of compliance status, so
    # we must only treat a rule as a finding when it carries a corrective
    # action — which appears exclusively next to rules checked "No".
    is_checklist = bool(re.search(r"Yes\s+No\s+N/A\s+Corrective", text, re.IGNORECASE))

    # For checklist PDFs where we have coordinate data, search the full
    # normalised text so that observation comments (which live in the main
    # checklist body, not necessarily in the summary section) are captured in
    # the excerpt.  For all other cases use only the named findings sections.
    if is_checklist and no_column_rules is not None:
        search_text = text
    else:
        findings_scope_parts = [
            extract_named_block_section(text, [label])
            for label in OREGON_FINDINGS_SECTION_LABELS
        ]
        search_text = "\n\n".join(part for part in findings_scope_parts if part)
        if not search_text:
            return []

    heading_positions = [
        match.start()
        for pattern in OREGON_BLOCK_SECTION_STOP_PATTERNS
        for match in re.finditer(rf"(?im)^(?:{pattern})\b", search_text)
    ]
    findings: List[Dict[str, str]] = []
    matches = list(
        re.finditer(
            r"\b\d{3}-\d{3}-\d{4}(?:\([^)]+\))?(?:\s*&\s*\([^)]+\))*(?:\s*\([^)]+\))*",
            search_text,
        )
    )

    for idx, match in enumerate(matches):
        start = match.start()
        next_starts = [m.start() for m in matches[idx + 1 : idx + 2]]
        next_starts.extend(pos for pos in heading_positions if pos > start)
        end = min(next_starts) if next_starts else len(search_text)
        snippet = clean_section_text(search_text[start:end])
        if not snippet:
            continue

        rule = collapse_inline_whitespace(match.group(0))
        rule_base_m = re.match(r"\d{3}-\d{3}-\d{4}", rule)

        # In checklist-format reports only rules with a No-column checkbox are
        # violations.  Use coordinate data when available; fall back to the
        # CORRECTIVE ACTION: heuristic when coordinate parsing was not possible.
        if is_checklist:
            if no_column_rules is not None:
                # Coordinate-based: trust the column position.
                if not rule_base_m or rule_base_m.group(0) not in no_column_rules:
                    continue
            elif not re.search(r"CORRECTIVE\s+ACTION\s*:", snippet, re.IGNORECASE):
                # Text heuristic fallback.
                continue

        # Skip items the report explicitly marks as not a finding.
        if re.search(r"not\s+a\s+finding", snippet, re.IGNORECASE):
            continue

        snippet = re.sub(r"(?im)^\s*Repeat\s+Comments\b.*$", "", snippet)
        snippet = re.sub(r"\bYes\?\s*No\?\b", "", snippet)
        # Strip "Yes No N/A Corrective Actions/Comments" column header lines.
        snippet = re.sub(r"(?im)^Yes\s+No\s+N/A\b.*$", "", snippet)
        # Strip standalone checkbox X marks (not part of a word or rule code).
        snippet = re.sub(r"(?<![A-Za-z0-9])X(?![A-Za-z0-9])", "", snippet)
        # Strip compliant sub-rule lines: lines that are now empty or whitespace-only
        # after X removal (they had nothing but the checkbox mark).
        snippet = re.sub(r"\n[ \t]*\n", "\n", snippet)
        snippet = re.sub(r"\s{2,}", " ", snippet).strip()
        if not snippet:
            continue
        findings.append({
            "rule": rule,
            "excerpt": snippet,
        })

    deduped: List[Dict[str, str]] = []
    seen = set()
    for finding in findings:
        key = (finding["rule"], finding["excerpt"][:160])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(finding)
    return deduped


def parse_oregon_report_text(
    text: str,
    no_column_rules: Optional[set] = None,
    checklist_findings: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    normalized = normalize_oregon_pdf_text(text)
    if not normalized:
        return {
            "report_title": "",
            "facility_type": "",
            "licensee": "",
            "executive_director": "",
            "program_director": "",
            "board_chairperson": "",
            "visit_date": "",
            "licensing_coordinator": "",
            "other_regulatory_agencies": "",
            "purpose": "",
            "program_compliance": "",
            "program_description": "",
            "program_services": "",
            "capacity_age_range": "",
            "funding_sources": "",
            "contracts_and_referrals": "",
            "average_length_of_stay": "",
            "average_daily_population_served": "",
            "number_of_children_served_annually": "",
            "use_of_seclusion_or_restraint": "",
            "interviews_observations": "",
            "program_strengths": "",
            "program_challenges": "",
            "changes_in_last_two_years": "",
            "lawsuits": "",
            "grievances_and_complaints": "",
            "interview_summary": "",
            "observations": "",
            "recommendations": "",
            "exceptions": "",
            "changes_in_license": "",
            "previous_findings": "",
            "new_findings": "",
            "findings": [],
            "finding_count": 0,
        }

    lines = [line.strip() for line in normalized.splitlines() if line.strip()]
    report_title = lines[0] if lines else ""
    facility_type = ""
    if len(lines) > 1 and ":" not in lines[1]:
        facility_type = lines[1]

    parsed: Dict[str, Any] = {
        "report_title": report_title,
        "facility_type": facility_type,
        "licensee": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Licensee", r"Licensed Agency", r"License Holder"])),
        "executive_director": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Executive Director"])),
        "program_director": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Program Director(?:\(s\))?"])),
        "board_chairperson": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Board Chairperson"])),
        "visit_date": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Date of site visit", r"Date of Unannounced"])),
        "licensing_coordinator": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Licensing Coordinator"])),
        "other_regulatory_agencies": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Other Regulatory or Accrediting Agencies"])),
        "purpose": clean_section_text(extract_labeled_block(normalized, [r"Purpose"])),
        "program_compliance": clean_section_text(extract_labeled_block(normalized, [r"Program Compliance"])),
        "program_description": clean_section_text(extract_labeled_block(normalized, [r"Program Description(?:\(s\))?"])),
        "program_services": clean_section_text(extract_labeled_block(normalized, [r"Program type and services"])),
        "capacity_age_range": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Capacity and age-range", r"Capacity and Age Range"])),
        "funding_sources": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Funding sources"])),
        "contracts_and_referrals": clean_section_text(extract_labeled_block(normalized, [r"Contracts and sources for referrals"])),
        "average_length_of_stay": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Average length of stay"])),
        "average_daily_population_served": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Average daily population served"])),
        "number_of_children_served_annually": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Number of children served annually"])),
        "use_of_seclusion_or_restraint": collapse_inline_whitespace(extract_labeled_block(normalized, [r"Use of seclusion or restraint"])),
        "interviews_observations": clean_section_text(extract_labeled_block(normalized, [r"Interviews, Observations"])),
        "program_strengths": clean_section_text(extract_labeled_block(normalized, [r"Program Strengths"])),
        "program_challenges": clean_section_text(extract_labeled_block(normalized, [r"Program Challenges"])),
        "changes_in_last_two_years": clean_section_text(extract_labeled_block(normalized, [r"Changes that have occurred in the last 2 years", r"Changes that have occurred in the last two years"])),
        "lawsuits": clean_section_text(extract_labeled_block(normalized, [r"Lawsuits"])),
        "grievances_and_complaints": clean_section_text(extract_labeled_block(normalized, [r"Grievances and complaints filed in the last two years"])),
        "recommendations": clean_section_text(extract_labeled_block(normalized, [r"Recommendations"])),
        "exceptions": clean_section_text(extract_labeled_block(normalized, [r"Exceptions"])),
        "changes_in_license": clean_section_text(extract_labeled_block(normalized, [r"Changes in License"])),
    }

    for key, labels in OREGON_BLOCK_SECTION_LABELS.items():
        parsed[key] = extract_named_block_section(normalized, labels)

    if checklist_findings is not None:
        # Table-based extraction already produced clean, column-aware findings.
        parsed["findings"] = checklist_findings
    else:
        parsed["findings"] = extract_findings(normalized, no_column_rules=no_column_rules)
    parsed["finding_count"] = len(parsed["findings"])
    return parsed


class ORFacilityScraper:
    """Scrape Oregon ODHS RC and TBS reports from the public SharePoint library."""

    def __init__(self, reports: ReportStore = REPORTS):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Origin": "https://www.oregon.gov",
        })
        self.reports = reports
        self.all_facilities: List[Dict] = []
        # slug -> live view GUID, lazily populated from Views.asmx
        self._view_id_by_slug: Optional[Dict[str, str]] = None

    def _post_soap(
        self,
        service_name: str,
        action_name: str,
        inner_xml: str,
        referer_path: str,
    ) -> bytes:
        body = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:xsd="http://www.w3.org/2001/XMLSchema">'
            "<soap:Body>"
            f'<{action_name} xmlns="http://schemas.microsoft.com/sharepoint/soap/">'
            f"{inner_xml}"
            f"</{action_name}>"
            "</soap:Body>"
            "</soap:Envelope>"
        )
        response = self.session.post(
            f"{BASE_URL}/_vti_bin/{service_name}.asmx",
            data=body.encode("utf-8"),
            headers={
                "Content-Type": "text/xml;charset='utf-8'",
                "Referer": f"{BASE_URL}{referer_path}",
            },
            timeout=60,
        )
        response.raise_for_status()
        return response.content

    def _load_view_ids(self) -> Dict[str, str]:
        """Map report-library view slugs to their current GUIDs.

        ODHS periodically deletes and recreates the per-program views, which
        changes their GUIDs and makes a hard-coded view_id 500 with
        "View does not exist". Querying Views.asmx for the live collection keeps
        the scraper self-healing. Keyed by both the view's URL basename and its
        DisplayName (lowercased) so either spelling resolves.
        """
        if self._view_id_by_slug is not None:
            return self._view_id_by_slug

        mapping: Dict[str, str] = {}
        try:
            xml_bytes = self._post_soap(
                service_name="Views",
                action_name="GetViewCollection",
                inner_xml=f"<listName>{REPORT_LIBRARY_NAME}</listName>",
                referer_path="/Pages/rc.aspx",
            )
            root = ET.fromstring(xml_bytes)
            for elem in root.iter():
                if not elem.tag.endswith("View"):
                    continue
                guid = elem.attrib.get("Name")
                if not guid:
                    continue
                display = (elem.attrib.get("DisplayName") or "").strip().lower()
                url = elem.attrib.get("Url") or ""
                url_slug = url.rsplit("/", 1)[-1].rsplit(".", 1)[0].strip().lower()
                for slug in (display, url_slug):
                    if slug:
                        mapping[slug] = guid
        except (requests.RequestException, ET.ParseError) as exc:
            logger.warning(f"Could not resolve live Oregon view IDs ({exc}); using fallback GUIDs")

        self._view_id_by_slug = mapping
        return mapping

    def _resolve_view_id(self, view: Dict) -> str:
        """Return the live GUID for a view, falling back to the hard-coded one."""
        slug = (view.get("view_slug") or "").lower()
        live = self._load_view_ids().get(slug)
        if live and live != view.get("view_id"):
            logger.info(f"{view['code']}: resolved live view id {live} (was {view.get('view_id')})")
        return live or view["view_id"]

    def _fetch_agency_websites(self) -> Dict[str, str]:
        xml_bytes = self._post_soap(
            service_name="Lists",
            action_name="GetListItems",
            inner_xml=(
                f"<listName>{AGENCY_LIST_NAME}</listName>"
                "<queryOptions><QueryOptions>"
                "<IncludeAttachmentUrls>TRUE</IncludeAttachmentUrls>"
                "</QueryOptions></queryOptions>"
            ),
            referer_path="/Pages/agencies.aspx",
        )
        root = ET.fromstring(xml_bytes)
        websites: Dict[str, str] = {}
        for elem in root.iter():
            if not elem.tag.endswith("row"):
                continue
            agency_name = sharepoint_lookup_label(elem.attrib.get("ows_Title"))
            website = extract_primary_url(elem.attrib.get("ows_Website0"))
            if agency_name and website:
                websites[agency_name] = website
        return websites

    def _fetch_view_rows(self, view: Dict) -> List[Dict]:
        all_rows: List[Dict] = []
        next_page_token: Optional[str] = None
        view_id = self._resolve_view_id(view)

        while True:
            paging = (
                f'<Paging ListItemCollectionPositionNext="{html.escape(next_page_token)}" />'
                if next_page_token else ""
            )
            xml_bytes = self._post_soap(
                service_name="Lists",
                action_name="GetListItems",
                inner_xml=(
                    f"<listName>{REPORT_LIBRARY_NAME}</listName>"
                    f"<viewName>{view_id}</viewName>"
                    "<rowLimit>500</rowLimit>"
                    "<queryOptions><QueryOptions>"
                    "<IncludeAttachmentUrls>TRUE</IncludeAttachmentUrls>"
                    f"{paging}"
                    "</QueryOptions></queryOptions>"
                ),
                referer_path=view["page_path"],
            )
            root = ET.fromstring(xml_bytes)
            rows = [dict(elem.attrib) for elem in root.iter() if elem.tag.endswith("row")]
            all_rows.extend(rows)

            # Check for next page
            data_elem = next(
                (elem for elem in root.iter() if elem.tag.endswith("data")), None
            )
            next_page_token = (
                data_elem.attrib.get("ListItemCollectionPositionNext")
                if data_elem is not None else None
            )
            if not next_page_token:
                break

        logger.info(f"{view['code']}: fetched {len(all_rows)} report rows")
        return all_rows

    def _enrich_row(self, row: Dict, view: Dict, agency_websites: Dict[str, str]) -> Dict:
        meta = parse_meta_info(row.get("ows_MetaInfo"))

        agency_name = (
            strip_placeholder(sharepoint_lookup_label(row.get("ows_Agency0")))
            or strip_placeholder(sharepoint_lookup_label(meta.get("Agency0")))
        )
        report_type = (
            strip_placeholder(sharepoint_lookup_label(row.get("ows_Report_x002d_Type")))
            or strip_placeholder(meta.get("Report-Type"))
        )
        report_date = normalize_report_date(
            sharepoint_lookup_label(row.get("ows_Title")) or meta.get("vti_title")
        )
        program_lookup_raw = meta.get("Program-Name") or ""
        program_name = strip_placeholder(sharepoint_lookup_label(program_lookup_raw))
        if not program_name:
            program_name = strip_placeholder(sharepoint_lookup_label(meta.get("Program Name")))
        program_id = sharepoint_lookup_id(program_lookup_raw) or sharepoint_lookup_id(meta.get("Program"))

        return {
            "agency_name": agency_name,
            "agency_website": agency_websites.get(agency_name, ""),
            "program_name": program_name,
            "program_id": program_id,
            "program_type": (
                strip_placeholder(sharepoint_lookup_label(meta.get("Program Type")))
                or view["program_type"]
            ),
            "report_type": report_type,
            "report_date": report_date,
            "report_id": clean_text(row.get("ows_ID")),
            "report_unique_id": sharepoint_lookup_label(row.get("ows_UniqueId")),
            "file_name": sharepoint_lookup_label(row.get("ows_FileLeafRef")),
            "pdf_url": build_pdf_url(row.get("ows_FileRef")),
            "view_code": view["code"],
            "source_page": f"{BASE_URL}{view['page_path']}",
            "meta": meta,
        }

    @staticmethod
    def infer_program_identity(entries: List[Dict]) -> None:
        def _norm(s: str) -> str:
            n = s.lower().strip()
            n = re.sub(r"\s*[-–—]\s*", " ", n)
            n = re.sub(r"\s*&\s*", " and ", n)
            n = re.sub(r"[^\w\s]", "", n)
            return re.sub(r"\s+", " ", n)

        # Group by normalised agency name so name variants share a bucket.
        grouped: Dict[tuple, List[Dict]] = defaultdict(list)
        for entry in entries:
            grouped[(_norm(entry["agency_name"]), entry["view_code"])].append(entry)

        for group_entries in grouped.values():
            unique_program_ids = {entry["program_id"] for entry in group_entries if entry["program_id"]}
            inferred_id = next(iter(unique_program_ids)) if len(unique_program_ids) == 1 else ""

            # Normalise names for uniqueness check so "Adapt Deer Creek" and
            # "Adapt - Deer Creek" count as the same name.
            names_with_norm = [
                (e["program_name"], _norm(e["program_name"]))
                for e in group_entries if e["program_name"]
            ]
            unique_norm_names = {norm for _, norm in names_with_norm}
            if len(unique_norm_names) == 1:
                name_counter: Counter = Counter(name for name, _ in names_with_norm)
                inferred_name = name_counter.most_common(1)[0][0] if name_counter else ""
            else:
                inferred_name = ""

            for entry in group_entries:
                if not entry["program_id"] and inferred_id:
                    entry["program_id"] = inferred_id
                if not entry["program_name"] and inferred_name:
                    entry["program_name"] = inferred_name

    def download_pdf(self, url: str, filename: str) -> Optional[bytes]:
        logger.info(f"  Downloading {filename}")
        try:
            response = self.session.get(
                url,
                headers={"Referer": f"{BASE_URL}/Pages/rc.aspx"},
                timeout=60,
            )
            response.raise_for_status()
            time.sleep(0.2)
            return response.content
        except requests.RequestException as exc:
            logger.warning(f"  download failed {url}: {exc}")
            return None

    def extract_pdf(self, url: str) -> Dict:
        """Text and checklist findings of the report PDF; the PDF itself goes
        to the Drive folder."""
        if not url:
            return {}
        filename = re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])
        extracted = extract_with_cache(
            self.reports,
            filename,
            fetch=lambda: self.download_pdf(url, filename),
            extract=lambda path: {
                "text": extract_pdf_text(path),
                # Table-based extraction; None for non-checklist PDFs.
                "checklist_findings": extract_checklist_findings(path),
            },
        )
        return extracted or {}

    def _build_report(self, entry: Dict) -> Dict:
        extracted = self.extract_pdf(entry["pdf_url"])
        extracted_text = extracted.get("text", "")
        checklist_findings = extracted.get("checklist_findings")
        if extracted_text:
            raw_content = extracted_text
        else:
            fallback_lines = [
                f"Agency: {entry['agency_name']}",
                f"Program: {entry['program_name'] or 'N/A'}",
                f"Program Type: {entry['program_type']}",
                f"Report Type: {entry['report_type']}",
                f"Report Date: {entry['report_date']}",
                f"PDF URL: {entry['pdf_url']}",
            ]
            raw_content = "\n".join(line for line in fallback_lines if line.strip())

        parsed = parse_oregon_report_text(raw_content, checklist_findings=checklist_findings)
        findings = parsed.get("findings") or []
        parsed_report_type = best_nonempty([
            parsed.get("report_title", ""),
            entry["report_type"],
        ])
        summary = best_nonempty([
            (
                f"{entry['report_type']} - {len(findings)} finding"
                f"{'' if len(findings) == 1 else 's'}"
                if entry["report_type"] and findings
                else ""
            ),
            f"{entry['report_type']} - {entry['report_date']}" if entry["report_type"] and entry["report_date"] else "",
            entry["report_type"],
            entry["report_date"],
            entry["file_name"],
        ])
        categories = {
            "agency_name": entry["agency_name"],
            "agency_website": entry["agency_website"],
            "program_name": entry["program_name"],
            "program_id": entry["program_id"],
            "program_type": entry["program_type"],
            "report_type": entry["report_type"],
            "view_code": entry["view_code"],
            "source_page": entry["source_page"],
            "pdf_url": entry["pdf_url"],
            "file_name": entry["file_name"],
            "sharepoint_unique_id": entry["report_unique_id"],
        }
        categories.update(parsed)

        return {
            "report_id": entry["report_id"] or entry["report_unique_id"] or entry["file_name"],
            "report_date": entry["report_date"],
            "pdf_url": entry["pdf_url"],
            "raw_content": raw_content,
            "content_length": len(raw_content),
            "summary": summary,
            "categories": categories,
        }

    def build_facilities_from_entries(self, entries: List[Dict]) -> List[Dict]:
        def _norm_key(name: str) -> str:
            """Collapse punctuation/spacing differences for grouping only."""
            n = name.lower().strip()
            n = re.sub(r"\s*[-–—]\s*", " ", n)   # "Adapt - Deer Creek" → "adapt deer creek"
            n = re.sub(r"\s*&\s*", " and ", n)    # "Child & Family" → "child and family"
            n = re.sub(r"[^\w\s]", "", n)          # drop remaining punctuation
            n = re.sub(r"\s+", " ", n)
            return n

        grouped_entries: Dict[tuple, List[Dict]] = defaultdict(list)
        for entry in entries:
            identity = (
                entry["program_name"]   # prefer human-readable name over bare numeric ID
                or entry["program_id"]
                or entry["agency_name"]
            )
            agency_norm   = _norm_key(entry["agency_name"])
            identity_norm = _norm_key(identity)
            # Only use identity as a distinguishing key when it adds real
            # information — i.e. it's not purely numeric (a bare SharePoint ID)
            # and it's not just a re-statement of the agency name.
            if re.match(r"^\d+$", identity.strip()) or identity_norm == agency_norm:
                group_key = (agency_norm, entry["view_code"])
            else:
                group_key = (agency_norm, entry["view_code"], identity_norm)
            grouped_entries[group_key].append(entry)

        facilities: List[Dict] = []
        total_facilities = len(grouped_entries)
        for fac_idx, grouped in enumerate(grouped_entries.values(), 1):
            grouped.sort(
                key=lambda entry: (
                    sort_key_for_report_date(entry["report_date"]),
                    entry["report_id"],
                )
            )
            reports = [self._build_report(entry) for entry in grouped]

            _BAD_PROGRAM_NAMES = {
                "ays", "bend", "castle", "gap", "phoenix",
                "residential adolescent sud", "residential program",
                "sage", "youth residential treatment center (yrtc)",
            }

            facility_name = pick_most_common(
                entry["program_name"] for entry in grouped
                if entry["program_name"].lower().strip() not in _BAD_PROGRAM_NAMES
            )
            if not facility_name:
                for report in reversed(reports):
                    licensee = (report.get("categories") or {}).get("licensee", "")
                    if licensee:
                        facility_name = licensee
                        break
            if not facility_name:
                facility_name = grouped[0]["agency_name"]
                facility_name = grouped[0]["agency_name"]

            logger.info(
                f"[{fac_idx}/{total_facilities}] {facility_name} "
                f"— {len(grouped)} report(s)"
            )
            program_identifier = best_nonempty(
                entry["program_id"] for entry in grouped
            ) or facility_name
            program_category = pick_most_common(entry["program_type"] for entry in grouped)
            agency_name = grouped[0]["agency_name"]
            agency_website = best_nonempty(entry["agency_website"] for entry in grouped)

            facility_info = {
                "facility_name": facility_name,
                "program_name": str(program_identifier),
                "program_category": program_category,
                "full_address": "",
                "phone": "",
                "bed_capacity": "",
                "executive_director": "",
                "license_exp_date": "",
                "relicense_visit_date": "",
                "action": "",
                "agency_name": agency_name,
            }
            if agency_website:
                facility_info["website"] = agency_website

            facilities.append({
                "facility_info": facility_info,
                "reports": reports,
            })

        return facilities

    @staticmethod
    def _entry_report_id(entry: Dict) -> str:
        """Same fallback chain that _build_report uses for report_id."""
        return (entry.get("report_id")
                or entry.get("report_unique_id")
                or entry.get("file_name")
                or "")

    def scrape(self, view_codes: Optional[List[str]] = None,
               seen: Optional[Dict[str, Set[str]]] = None
               ) -> Tuple[List[Dict], Dict[str, List[str]]]:
        """Scrape OR, skipping entries whose report_id is already in `seen`.

        State is keyed by agency_name (the most stable per-row field; multiple
        SharePoint views can share an agency). Filtering happens before PDF
        download/parse, so already-seen reports cost nothing.
        """
        selected_codes = view_codes or [view["code"] for view in VIEWS]
        selected_views = [VIEWS_BY_CODE[code] for code in selected_codes if code in VIEWS_BY_CODE]
        if not selected_views:
            raise ValueError(f"No valid Oregon view codes requested: {view_codes}")

        seen = seen or {}
        new_ids: Dict[str, List[str]] = {}
        skipped = 0

        logger.info("Starting OR scrape")
        logger.info(f"Views: {', '.join(view['code'] for view in selected_views)}")

        agency_websites = self._fetch_agency_websites()
        all_entries: List[Dict] = []

        for view in selected_views:
            rows = self._fetch_view_rows(view)
            for row in rows:
                if not build_pdf_url(row.get("ows_FileRef")).lower().endswith(".pdf"):
                    continue
                entry = self._enrich_row(row, view, agency_websites)
                report_id = self._entry_report_id(entry)
                agency = entry.get("agency_name", "")
                if report_id and report_id in seen.get(agency, set()):
                    skipped += 1
                    continue
                all_entries.append(entry)
                if report_id:
                    new_ids.setdefault(agency, []).append(report_id)

        if skipped:
            logger.info(f"Skipped {skipped} reports already seen in state")

        self.infer_program_identity(all_entries)
        self.all_facilities = self.build_facilities_from_entries(all_entries)
        logger.info(
            f"Scraping complete: {len(self.all_facilities)} facilities, {len(all_entries)} new reports"
        )
        return self.all_facilities, new_ids

    def _fetch_legislative_rows(self) -> List[Dict]:
        """Rows of the quarterly legislative reports, oldest quarter first."""
        xml_bytes = self._post_soap(
            service_name="Lists",
            action_name="GetListItems",
            inner_xml=(
                f"<listName>{REPORT_LIBRARY_NAME}</listName>"
                "<query><Query><Where><Contains>"
                '<FieldRef Name="FileLeafRef" /><Value Type="Text">-leg</Value>'
                "</Contains></Where></Query></query>"
                "<rowLimit>2000</rowLimit>"
            ),
            referer_path="/Pages/reports.aspx",
        )
        root = ET.fromstring(xml_bytes)
        rows = []
        for elem in root.iter():
            if not elem.tag.endswith("row"):
                continue
            row = dict(elem.attrib)
            file_name = sharepoint_lookup_label(row.get("ows_FileLeafRef"))
            # The file name is the steady part ("2026q2-leg.pdf"); the row's type
            # columns are left blank on some quarters.
            m = re.fullmatch(r"(\d{4})q([1-4])-leg\.pdf", file_name, re.I)
            if not m:
                continue
            rows.append({
                "row_id": clean_text(row.get("ows_ID")),
                "modified": clean_text(row.get("ows_Modified")),
                "quarter": f"{m.group(1)}-Q{m.group(2)}",
                "file_name": file_name,
                "pdf_url": build_pdf_url(row.get("ows_FileRef")),
            })
        rows.sort(key=lambda r: r["quarter"])
        logger.info(f"Abuse reports: {len(rows)} quarterly legislative reports listed")
        return rows

    def scrape_abuse_reports(self, seen: Optional[Dict[str, Set[str]]] = None
                             ) -> Tuple[List[Dict], Dict[str, List[str]]]:
        """Substantiated abuse reports from the quarterly legislative reports
        not read yet. State keeps each quarterly PDF by row id and modified
        time, so a quarter ODHS reissues is read again."""
        seen_keys = (seen or {}).get(ABUSE_REPORTS_STATE_KEY, set())
        parsed_quarters: List[Tuple[Dict[str, str], List[Dict[str, str]]]] = []
        new_keys: List[str] = []
        unknown: Set[str] = set()

        for row in self._fetch_legislative_rows():
            state_key = f"{row['row_id']}@{row['modified']}"
            if state_key in seen_keys:
                continue
            # A reissued quarter keeps its file name, so the archived copy and
            # the cached extract carry the day ODHS last changed it.
            stamp = re.sub(r"\D", "", row["modified"])[:8]
            archive_name = row["file_name"].replace(".pdf", f"_{stamp}.pdf") if stamp else row["file_name"]
            extracted = extract_with_cache(
                self.reports,
                archive_name,
                fetch=lambda row=row: self.download_pdf(row["pdf_url"], row["file_name"]),
                extract=extract_abuse_report_entries,
                version=ABUSE_EXTRACT_VERSION,
            ) or {}
            entries = extracted.get("entries") or []
            in_text = extracted.get("numbers_in_text", 0)
            if "entries" not in extracted:
                logger.warning(f"  {row['file_name']}: could not be read; it will be retried next run")
                continue
            if len(entries) != in_text:
                # Not marked read: a layout the table reader does not know yet.
                logger.warning(
                    f"  {row['file_name']}: read {len(entries)} abuse reports but its text holds "
                    f"{in_text} report numbers; posting what was read, the quarter will be retried"
                )
            else:
                new_keys.append(state_key)
            logger.info(f"  {row['quarter']}: {len(entries)} substantiated abuse report(s)")
            for entry in entries:
                if not abuse_provider_facility_name(entry.get("provider", ""))[1]:
                    unknown.add(entry.get("provider", ""))
            parsed_quarters.append((row, entries))

        facilities = build_abuse_facilities(parsed_quarters)
        for provider in sorted(p for p in unknown if p):
            logger.warning(
                f"  New abuse report provider, filed under its printed name: {provider} "
                "(add it to ABUSE_PROVIDER_NAMES if it is a program already listed)"
            )
        logger.info(
            f"Abuse reports: {sum(len(f['reports']) for f in facilities)} report(s) "
            f"for {len(facilities)} provider(s)"
        )
        return facilities, ({ABUSE_REPORTS_STATE_KEY: new_keys} if new_keys else {})


def save_to_api(facilities: List[Dict]) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="OR",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Oregon ODHS RC/TBS report PDFs and the substantiated abuse reports "
                    "in the quarterly legislative reports"
    )
    parser.add_argument(
        "--views",
        nargs="+",
        choices=sorted(VIEWS_BY_CODE),
        default=[view["code"] for view in VIEWS],
        help="Subset of Oregon program views to scrape",
    )
    parser.add_argument(
        "--no-post",
        action="store_true",
        help="Scrape and cache PDFs without POSTing results to the inspections API",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--no-abuse-reports",
        action="store_true",
        help="Skip the substantiated abuse reports (quarterly legislative reports)",
    )
    parser.add_argument(
        "--abuse-reports-only",
        action="store_true",
        help="Only the substantiated abuse reports, no site visit reports",
    )
    parser.add_argument(
        "--out",
        help="Also write the facilities that would be posted to this JSON file",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Ignore {STATE_FILE} and re-scan all reports",
    )
    args = parser.parse_args()

    state = load_state(STATE_FILE)
    if args.replace:
        logger.warning(
            "--replace is deprecated and ignored; Oregon now posts incrementally "
            "without clearing existing database rows."
        )
    full = args.full
    seen = {} if full else seen_from_state(state)

    scraper = ORFacilityScraper()
    facilities: List[Dict] = []
    new_ids: Dict[str, List[str]] = {}
    if not args.abuse_reports_only:
        facilities, new_ids = scraper.scrape(view_codes=args.views, seen=seen)
    if not args.no_abuse_reports:
        abuse_facilities, abuse_ids = scraper.scrape_abuse_reports(seen=seen)
        facilities = facilities + abuse_facilities
        new_ids.update(abuse_ids)

    facilities_to_post = [f for f in facilities if f["reports"]]
    if args.out:
        Path(args.out).write_text(
            json.dumps({"state": "OR", "facilities": facilities_to_post}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info(f"Wrote {len(facilities_to_post)} facilities to {args.out}")
    if not facilities_to_post:
        logger.info("No new reports since last run")
        return

    logger.info(f"Posting {len(facilities_to_post)} facilities with new reports")
    if args.no_post:
        logger.info("Skipping API POST because --no-post was set; state not advanced")
        return
    if save_to_api(facilities_to_post):
        logger.info("Data saved to database successfully!")
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
    else:
        logger.error("API save failed -- state not advanced")


if __name__ == "__main__":
    main()
