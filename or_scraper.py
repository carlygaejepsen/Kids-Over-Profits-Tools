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
"""

import argparse
import html
import logging
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from xml.etree import ElementTree as ET

import pdfplumber
import requests

from inspection_api_client import post_facilities_to_api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("INSPECTIONS_API_KEY", "CHANGE_ME")

BASE_URL = "https://www.oregon.gov/odhs/licensing/childrens-care-agencies"
REPORT_LIBRARY_NAME = "reports"
AGENCY_LIST_NAME = "agencies"
PDF_CACHE_DIR = Path(__file__).parent / "or_pdfs"

VIEWS = [
    {
        "code": "RC",
        "page_path": "/Pages/rc.aspx",
        "view_id": "{AC5902E1-7D1D-4B71-8909-8B7ACB46849D}",
        "program_type": "(RC) Residential Care Programs",
    },
    {
        "code": "TBS",
        "page_path": "/Pages/tbs.aspx",
        "view_id": "{D1F93F33-6BF8-46BD-8DAA-C95E3A5BC80D}",
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
            rf"(?is)(?:^|\n|\s){label}\s*:\s*(.+?)(?=(?:\n|\s)(?:{stop_re})\s*:?|\Z)"
        )
        match = pattern.search(text)
        if match:
            return clean_section_text(match.group(1))
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


def extract_findings(text: str) -> List[Dict[str, str]]:
    if not text:
        return []

    findings_scope_parts = [
        extract_named_block_section(text, [label])
        for label in OREGON_FINDINGS_SECTION_LABELS
    ]
    findings_scope = "\n\n".join(part for part in findings_scope_parts if part)
    if not findings_scope:
        return []

    heading_positions = [
        match.start()
        for pattern in OREGON_BLOCK_SECTION_STOP_PATTERNS
        for match in re.finditer(rf"(?im)^(?:{pattern})\b", findings_scope)
    ]
    findings: List[Dict[str, str]] = []
    matches = list(
        re.finditer(
            r"\b\d{3}-\d{3}-\d{4}(?:\([^)]+\))?(?:\s*&\s*\([^)]+\))*(?:\s*\([^)]+\))*",
            findings_scope,
        )
    )

    for idx, match in enumerate(matches):
        start = match.start()
        next_starts = [m.start() for m in matches[idx + 1 : idx + 2]]
        next_starts.extend(pos for pos in heading_positions if pos > start)
        end = min(next_starts) if next_starts else len(findings_scope)
        snippet = clean_section_text(findings_scope[start:end])
        if not snippet:
            continue

        rule = collapse_inline_whitespace(match.group(0))
        snippet = re.sub(r"(?im)^\s*Repeat\s+Comments\b.*$", "", snippet)
        snippet = re.sub(r"\bYes\?\s*No\?\b", "", snippet)
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


def parse_oregon_report_text(text: str) -> Dict[str, Any]:
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

    parsed["findings"] = extract_findings(normalized)
    parsed["finding_count"] = len(parsed["findings"])
    return parsed


class ORFacilityScraper:
    """Scrape Oregon ODHS RC and TBS reports from the public SharePoint library."""

    def __init__(self, pdf_dir: Path = PDF_CACHE_DIR):
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
        self.pdf_dir = pdf_dir
        self.pdf_dir.mkdir(exist_ok=True)
        self.all_facilities: List[Dict] = []

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
        xml_bytes = self._post_soap(
            service_name="Lists",
            action_name="GetListItems",
            inner_xml=(
                f"<listName>{REPORT_LIBRARY_NAME}</listName>"
                f"<viewName>{view['view_id']}</viewName>"
                "<queryOptions><QueryOptions>"
                "<IncludeAttachmentUrls>TRUE</IncludeAttachmentUrls>"
                "</QueryOptions></queryOptions>"
            ),
            referer_path=view["page_path"],
        )
        root = ET.fromstring(xml_bytes)
        rows = [dict(elem.attrib) for elem in root.iter() if elem.tag.endswith("row")]
        logger.info(f"{view['code']}: fetched {len(rows)} report rows")
        return rows

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
        grouped: Dict[tuple, List[Dict]] = defaultdict(list)
        for entry in entries:
            grouped[(entry["agency_name"], entry["view_code"])].append(entry)

        for group_entries in grouped.values():
            unique_program_ids = {entry["program_id"] for entry in group_entries if entry["program_id"]}
            unique_program_names = {entry["program_name"] for entry in group_entries if entry["program_name"]}
            inferred_id = next(iter(unique_program_ids)) if len(unique_program_ids) == 1 else ""
            inferred_name = next(iter(unique_program_names)) if len(unique_program_names) == 1 else ""
            for entry in group_entries:
                if not entry["program_id"] and inferred_id:
                    entry["program_id"] = inferred_id
                if not entry["program_name"] and inferred_name:
                    entry["program_name"] = inferred_name

    def download_pdf(self, url: str) -> Optional[Path]:
        if not url:
            return None
        filename = re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])
        dest = self.pdf_dir / filename
        if dest.exists() and dest.stat().st_size > 0:
            logger.debug(f"  [cached] {filename}")
            return dest
        logger.info(f"  Downloading {filename}")
        try:
            response = self.session.get(
                url,
                headers={"Referer": f"{BASE_URL}/Pages/rc.aspx"},
                timeout=60,
            )
            response.raise_for_status()
            dest.write_bytes(response.content)
            time.sleep(0.2)
            return dest
        except requests.RequestException as exc:
            logger.warning(f"  download failed {url}: {exc}")
            return None

    def _build_report(self, entry: Dict) -> Dict:
        pdf_path = self.download_pdf(entry["pdf_url"])
        extracted_text = extract_pdf_text(pdf_path) if pdf_path else ""
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

        parsed = parse_oregon_report_text(raw_content)
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
        grouped_entries: Dict[tuple, List[Dict]] = defaultdict(list)
        for entry in entries:
            identity = (
                entry["program_id"]
                or entry["program_name"]
                or entry["agency_name"]
            )
            grouped_entries[(entry["agency_name"], entry["view_code"], identity)].append(entry)

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

    def scrape(self, view_codes: Optional[List[str]] = None) -> List[Dict]:
        selected_codes = view_codes or [view["code"] for view in VIEWS]
        selected_views = [VIEWS_BY_CODE[code] for code in selected_codes if code in VIEWS_BY_CODE]
        if not selected_views:
            raise ValueError(f"No valid Oregon view codes requested: {view_codes}")

        logger.info("Starting OR scrape")
        logger.info(f"Views: {', '.join(view['code'] for view in selected_views)}")

        agency_websites = self._fetch_agency_websites()
        all_entries: List[Dict] = []

        for view in selected_views:
            rows = self._fetch_view_rows(view)
            for row in rows:
                if not build_pdf_url(row.get("ows_FileRef")).lower().endswith(".pdf"):
                    continue
                all_entries.append(self._enrich_row(row, view, agency_websites))

        self.infer_program_identity(all_entries)
        self.all_facilities = self.build_facilities_from_entries(all_entries)
        logger.info(
            f"Scraping complete: {len(self.all_facilities)} facilities, {len(all_entries)} reports"
        )
        return self.all_facilities


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
    parser = argparse.ArgumentParser(description="Scrape Oregon ODHS RC/TBS report PDFs")
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
    args = parser.parse_args()

    scraper = ORFacilityScraper()
    facilities = scraper.scrape(view_codes=args.views)

    if facilities:
        logger.info(f"Scraped {len(facilities)} facilities")
        if args.no_post:
            logger.info("Skipping API POST because --no-post was set")
            return
        logger.info("Posting Oregon data to API")
        if save_to_api(facilities):
            logger.info("Data saved to database successfully!")
        else:
            logger.error("API save failed -- check logs above")
    else:
        logger.warning("No facilities scraped")


if __name__ == "__main__":
    main()
