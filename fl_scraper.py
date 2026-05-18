"""
Florida scraper.

Florida youth residential care is split across three independent regulators, so
this scraper dispatches on --source:

    python fl_scraper.py --source djj  [--categories residential,detention,prea,spep] [--full]
    python fl_scraper.py --source ahca [--full]
    python fl_scraper.py --source dcf  # raises NotImplementedError — no public source

Both implemented sources POST under state="FL". `program_name` is namespaced by
agency prefix (DJJ-/AHCA-) so the same facility name across agencies doesn't
collide on the inspections_facilities unique key. DCF Residential Group Care
has no unified public source as of May 2026 — see FLDCFScraper docstring.
"""
import argparse
import hashlib
import json
import logging
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote_plus, unquote, urljoin, urlparse

import pdfplumber
import requests
from bs4 import BeautifulSoup, Tag

try:
    import pymupdf  # type: ignore
except ImportError:  # pragma: no cover — fallback gracefully when pymupdf isn't installed
    pymupdf = None

from inspection_api_client import post_facilities_to_api
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

DJJ_BASE = "https://www.djj.state.fl.us"
DJJ_RESIDENTIAL_DIR = "/programs-facilities/residential-facilities"
DJJ_DETENTION_DIR = "/programs-facilities/detention-centers"
DJJ_QI_INDEX = "/partners-providers-staff/monitoring-and-quality-improvement/reports"
DJJ_PREA_INDEX = "/partners-providers-staff/prison-rape-elimination-act-prea/completed-prea-audit-reports"
DJJ_SPEP_OPEN = "/research/standardized-program-evaluation-protocol-spep/residential-spep-reports-current-programs"
DJJ_SPEP_CLOSED = "/research/standardized-program-evaluation-protocol-spep/residential-spep-reports-closed-programs"

DJJ_PDF_CACHE = Path(__file__).parent / "fl_pdfs"
DJJ_STATE_FILE = Path(os.getenv("FL_DJJ_STATE_FILE", ".fl_djj_state.json"))
AHCA_STATE_FILE = Path(os.getenv("FL_AHCA_STATE_FILE", ".fl_ahca_state.json"))
DCF_STATE_FILE = Path(os.getenv("FL_DCF_STATE_FILE", ".fl_dcf_state.json"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)

DJJ_CATEGORIES = ("residential", "detention", "prea", "spep")

DEFAULT_WORKERS = int(os.getenv("FL_WORKERS", "5"))
DEFAULT_PDF_TIMEOUT = int(os.getenv("FL_PDF_TIMEOUT", "120"))

AHCA_FHF_BASE = "https://quality.healthfinder.fl.gov"
AHCA_FHF_SEARCH = f"{AHCA_FHF_BASE}/Facility-Search/FacilityLocateSearch"
AHCA_FHF_HANDLER = f"{AHCA_FHF_SEARCH}?handler=AdvancedSearch"
AHCA_DM_WEB_BASE = "https://apps.ahca.myflorida.com/dm_web/"

# Each entry maps a FHF facility-type code to (dm_web client_code, full label).
# RTC covers Residential Treatment Centers AND Therapeutic Group Homes per
# Florida statute — Therapeutic Group Homes are a 12-bed-or-fewer subtype of
# RTC. RTF and Crisis cover adjacent facility types; expand here when adding.
AHCA_FACILITY_TYPES: Dict[str, Tuple[str, str]] = {
    "RTC": ("57", "Residential Treatment Center for Children and Adolescents"),
}

# Tokens stripped during normalization of program names so common naming-style
# differences ("Alachua Academy" vs "Alachua Youth Academy" vs "AlachuaRJDC")
# collapse to the same key for fuzzy matching.
_NORM_DROP_WORDS = {
    "academy", "youth", "treatment", "center", "centre", "facility", "facilities",
    "juvenile", "detention", "regional", "rjdc", "jdc", "school", "group",
    "home", "homes", "residential", "the", "of", "and", "for", "boys", "girls",
    "girl", "boy", "florida", "fl", "program", "programs", "secure", "hardware",
    "ydc", "yda", "service", "services", "correctional", "correction",
    "intervention", "transition",
}


def _normalize_key(text: str) -> str:
    """Collapse a program/facility name to a comparable key for fuzzy matching.

    Different DJJ pages spell the same facility differently (e.g. "Alachua
    Academy" vs "AlachuaRJDC" vs "Alachua Youth Academy"); the directory slug
    is usually the most reliable join target.
    """
    if not text:
        return ""
    # Insert a space at every lower→upper transition so "AlachuaRJDC" → "Alachua RJDC".
    cleaned = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    cleaned = re.sub(r"\([^)]*\)", " ", cleaned).lower()
    cleaned = re.sub(r"[\W_]+", " ", cleaned)
    tokens = [t for t in cleaned.split() if t and t not in _NORM_DROP_WORDS]
    return " ".join(tokens).strip()


def _slugify(text: str) -> str:
    cleaned = re.sub(r"[^\w\s-]", "", text.lower())
    cleaned = re.sub(r"\s+", "-", cleaned.strip())
    return re.sub(r"-+", "-", cleaned)


def _stable_report_id(url: str) -> str:
    """Pick a stable, human-readable report ID from a PDF URL.

    `/content/download/{id}/file/...` URLs have a CMS-assigned numeric ID we
    can use directly. External S3 URLs don't, so we fall back to the decoded
    filename, which is unique enough within FL DJJ's data set.
    """
    cms_match = re.search(r"/content/download/(\d+)/", url)
    if cms_match:
        return cms_match.group(1)
    filename = unquote(urlparse(url).path.rsplit("/", 1)[-1])
    if filename:
        return f"s3-{filename}"
    return "s3-" + hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]


def _fy_to_date(fy_label: str) -> str:
    """Convert a fiscal-year label ("FY 24-25", "FY2425", "FY24-25") to the
    FY-end date in mm/dd/yyyy form.

    DJJ QI reports cover an entire fiscal year (July 1 – June 30); reporting
    an end-of-FY date keeps frontend "newest first" sort correct without
    pretending we know the actual site-visit date.
    """
    digits = re.findall(r"\d{2,4}", fy_label or "")
    if not digits:
        return ""
    second = digits[-1]
    if len(second) == 4:
        year = int(second)
    elif len(second) == 2:
        year = 2000 + int(second)
    else:
        return ""
    return f"06/30/{year}"


def _fy_label_normalize(raw: str) -> str:
    digits = re.findall(r"\d{2,4}", raw or "")
    if len(digits) >= 2:
        a, b = digits[0][-2:], digits[1][-2:]
        return f"FY{a}-{b}"
    if len(digits) == 1 and len(digits[0]) == 4:
        a, b = digits[0][:2], digits[0][2:]
        return f"FY{a}-{b}"
    return (raw or "").strip()


def _absolute_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(DJJ_BASE, href)


# Labels that appear in DJJ facility profile pages. Order matters for the
# split parser below — longer labels are tried first so "Director Title:"
# binds before "Director:".
DJJ_PROFILE_LABELS = [
    "Risk Levels", "Risk Level",
    "Operating Capacity", "Capacity",
    "Mailing Address", "Address",
    "Director Title", "Director",
    "View QI Reports", "View SPEP Reports", "View MQI Reports",
    "State Owned", "Services", "Directions",
    "Phone", "Telephone", "Fax",
    "Email", "Website",
    "County", "Circuit", "Region", "Gender",
    "Program Type",
]
_DJJ_PROFILE_LABEL_RE = re.compile(
    r"\b(" + "|".join(re.escape(lbl) for lbl in sorted(DJJ_PROFILE_LABELS, key=len, reverse=True)) + r")\s*:\s*",
    re.IGNORECASE,
)


def _parse_profile_labels(text: str) -> Dict[str, str]:
    """Cut a flat profile text blob into label→value pairs using a fixed
    label vocabulary. Last occurrence wins (later mentions are rare and
    typically more authoritative — e.g., page footer rewrites)."""
    out: Dict[str, str] = {}
    matches = list(_DJJ_PROFILE_LABEL_RE.finditer(text))
    for i, m in enumerate(matches):
        label = m.group(1).strip().lower()
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        out[label] = text[start:end].strip()
    return out


_PHONE_PATTERN = re.compile(
    r"\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]\d{4}(?:\s*(?:ext\.?|x)\s*\d+)?",
    re.IGNORECASE,
)


def _extract_phone(raw: str) -> str:
    if not raw:
        return ""
    m = _PHONE_PATTERN.search(raw)
    return m.group(0).strip() if m else ""


_FL_ADDRESS_PATTERN = re.compile(r"\d{1,6}\s+[A-Za-z0-9 .,'\-]+?,\s*FL\s+\d{5}(?:-\d{4})?")


def _extract_florida_address(text: str) -> str:
    """Fallback for DJJ profile pages, which include the street address as
    bare text without an `Address:` label."""
    if not text:
        return ""
    m = _FL_ADDRESS_PATTERN.search(text)
    return re.sub(r"\s+", " ", m.group(0)).strip() if m else ""


def _safe_field(value: Any, max_len: int) -> str:
    """Truncate a string field to a max length, stripping whitespace. Empty
    string in / out for falsy values. Defensive belt for the MySQL columns
    behind the inspections API — we don't know every column's limit, so
    keep generous caps and tighten phone/fax explicitly."""
    if not value:
        return ""
    s = str(value).strip()
    return s if len(s) <= max_len else s[:max_len].rstrip()


def _strip_status_suffix(name: str) -> Tuple[str, str]:
    """Split "Foo Academy (Program Closed)" → ("Foo Academy", "Program Closed")."""
    if not name:
        return "", ""
    match = re.search(r"\(([^)]+)\)\s*$", name)
    status = match.group(1).strip() if match else ""
    base = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
    return base, status


# DJJ QI reports use a uniform per-indicator format:
#   "1.01 Initial Background Screening * Satisfactory"
#   "2.03 Written Consent of Youth ... Limited Compliance"
#   "5.13 Tool Inventory and Management Failed Compliance"
# The asterisk marks critical indicators. The first ~5000 chars are a glossary
# / legend section where the words "Failed Compliance" appear definitionally;
# the real ratings live in the per-indicator detail pages further in. We trim
# the glossary by skipping lines until the first numbered indicator appears.
_DJJ_INDICATOR_RE = re.compile(
    r"^\s*(\d+\.\d{1,2}[a-z]?)\s+(.+?)\s*\*?\s*"
    r"(Satisfactory|Limited\s+Compliance|Failed\s+Compliance)\s*$",
    re.MULTILINE,
)
_DJJ_RATING_NORM = {
    "satisfactory": "Satisfactory",
    "limited compliance": "Limited Compliance",
    "failed compliance": "Failed Compliance",
}


def extract_djj_qi_findings(text: str) -> List[Dict[str, str]]:
    """Pull (indicator, name, rating) rows from a DJJ QI PDF and return only the
    ones rated Limited or Failed — those are the actionable findings."""
    if not text:
        return []
    findings: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for match in _DJJ_INDICATOR_RE.finditer(text):
        indicator_id = match.group(1)
        name = re.sub(r"\s+", " ", match.group(2)).strip().rstrip("*").strip()
        rating_raw = re.sub(r"\s+", " ", match.group(3)).strip().lower()
        rating = _DJJ_RATING_NORM.get(rating_raw, match.group(3).strip())
        if rating == "Satisfactory":
            continue
        key = (indicator_id, rating)
        if key in seen:
            continue
        seen.add(key)
        findings.append({
            "rule": f"Indicator {indicator_id}",
            "excerpt": name,
            "rating": rating,
        })
    return findings


# PREA audits classify each standard (§115.xxx) as Exceeds / Meets / Does Not
# Meet Standard. Two PDF formats appear in the wild:
#   1. Template format (older PREA cycles): every determination is printed with
#      a checkbox prefix, only one is filled. Looks like:
#           ☐ Exceeds Standard
#           ☒ Meets Standard
#           ☐ Does Not Meet Standard
#      The signal for a real finding is "☒ Does Not Meet Standard" — the filled
#      box (U+2612) immediately before the label.
#   2. Narrative format (newer cycles): only the selected determination text is
#      rendered, prefixed by an "Auditor Overall Determination" header.
# We catch real DNMS findings in both formats.
_PREA_STANDARD_RE = re.compile(r"\b(115\.\d{2,3})\b\s+([^\n]{1,160})")
_PREA_CHECKED_DNMS_RE = re.compile(r"☒\s*Does Not Meet Standard", re.IGNORECASE)
_PREA_NARRATIVE_DET_RE = re.compile(
    r"Auditor Overall (?:Compliance )?Determination[^\n]*\n[\s\S]{0,400}?"
    r"(Exceeds Standard|Does Not Meet Standard|Meets Standard)",
    re.IGNORECASE,
)


def _prea_slice_has_dnms(slice_text: str) -> bool:
    """Return True iff the slice contains a real (selected) DNMS determination."""
    if _PREA_CHECKED_DNMS_RE.search(slice_text):
        return True
    det = _PREA_NARRATIVE_DET_RE.search(slice_text)
    if det and det.group(1).strip().lower() == "does not meet standard":
        return True
    return False


_PREA_SUBCLAUSE_RE = re.compile(r"^\(?[a-z0-9]{1,3}\)?$", re.IGNORECASE)


def _prea_collect_titles(standards: List["re.Match[str]"]) -> Dict[str, str]:
    """For each §115.xxx standard, pick the longest meaningful trailing text we
    see across all occurrences — the first mention of a standard has its title,
    later mentions just have sub-clause indicators like "(a)" or "(i)"."""
    titles: Dict[str, str] = {}
    for m in standards:
        sid = m.group(1)
        candidate = re.sub(r"\s+", " ", m.group(2)).strip().rstrip(",;:")
        # Skip pure sub-clause indicators ("(a)", "(i)", "1") and very short fragments.
        if _PREA_SUBCLAUSE_RE.match(candidate) or len(candidate) < 8:
            continue
        if sid not in titles or len(candidate) > len(titles[sid]):
            titles[sid] = candidate[:240]
    return titles


def extract_djj_prea_findings(text: str) -> List[Dict[str, str]]:
    """Walk a PREA audit PDF and emit one finding per standard rated 'Does Not
    Meet Standard'. Recognises both the templated checkbox layout (☒/☐ next to
    each option) and the narrative layout that prints only the selected
    determination.

    Caveat: some older auditor templates (year2cycle3-era) have data-entry
    errors where both 'Meets' and 'Does Not Meet' are checked on the same
    standard. We treat any ☒ on DNMS as a finding — the auditor's intent is
    ambiguous in those cases, and surfacing the flag is safer than dropping it.
    """
    if not text:
        return []
    standards = list(_PREA_STANDARD_RE.finditer(text))
    titles = _prea_collect_titles(standards)

    findings: List[Dict[str, str]] = []
    seen: Set[str] = set()
    for i, std_match in enumerate(standards):
        standard_id = std_match.group(1)
        if standard_id in seen:
            continue
        start = std_match.end()
        end = standards[i + 1].start() if i + 1 < len(standards) else len(text)
        slice_text = text[start:end]
        if not _prea_slice_has_dnms(slice_text):
            continue
        seen.add(standard_id)
        title = titles.get(standard_id, "")
        findings.append({
            "rule": f"PREA §{standard_id}",
            "excerpt": title or f"Standard §{standard_id}",
            "rating": "Does Not Meet Standard",
        })
    return findings


def _extract_pdf_text_pymupdf(path: Path) -> str:
    """Fast fallback extractor. Much faster than pdfplumber on layout-heavy
    PDFs (240x+ on PREA checkbox templates) but doesn't preserve column
    alignment as cleanly — pdfplumber's line-layout output is what the QI
    indicator-row regex relies on, so we use this only when pdfplumber wedges."""
    if pymupdf is None:
        return ""
    try:
        doc = pymupdf.open(path)
        text = "\n".join(page.get_text() for page in doc)
        doc.close()
        return text.strip()
    except Exception as exc:
        logger.warning(f"  pymupdf fallback failed for {path.name}: {exc}")
        return ""


def extract_pdf_text(path: Path, timeout: int = DEFAULT_PDF_TIMEOUT) -> str:
    """Run pdfplumber under a soft timeout. If pdfplumber wedges on a
    malformed/image-heavy PDF, fall back to pymupdf (no timeout — pymupdf is
    fast on everything). Portable to Windows (no SIGALRM dependency)."""
    result: Dict[str, str] = {"text": ""}

    def _run() -> None:
        try:
            with pdfplumber.open(path) as pdf:
                pages = [(page.extract_text() or "") for page in pdf.pages]
            result["text"] = "\n".join(p for p in pages if p).strip()
        except Exception as exc:
            logger.warning(f"  pdfplumber extract failed for {path.name}: {exc}")

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        logger.warning(
            f"  pdfplumber TIMED OUT after {timeout}s for {path.name}; "
            "falling back to pymupdf"
        )
        return _extract_pdf_text_pymupdf(path)
    if not result["text"] and pymupdf is not None:
        # pdfplumber returned empty (parser exception swallowed above) — try the fallback
        return _extract_pdf_text_pymupdf(path)
    return result["text"]


class FLDJJScraper:
    def __init__(
        self,
        pdf_dir: Path = DJJ_PDF_CACHE,
        workers: int = DEFAULT_WORKERS,
        pdf_timeout: int = DEFAULT_PDF_TIMEOUT,
    ):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.pdf_dir = pdf_dir
        self.pdf_dir.mkdir(exist_ok=True)
        self._html_cache: Dict[str, str] = {}
        self.all_facilities: List[Dict] = []
        self.workers = max(1, workers)
        self.pdf_timeout = max(1, pdf_timeout)

    def _get_html(self, path: str) -> str:
        url = _absolute_url(path) if path.startswith("/") else path
        if url in self._html_cache:
            return self._html_cache[url]
        logger.debug(f"GET {url}")
        response = self.session.get(url, timeout=60)
        response.raise_for_status()
        self._html_cache[url] = response.text
        time.sleep(0.2)
        return response.text

    def _pick_directory_table(self, soup: BeautifulSoup, required_headers: Iterable[str]) -> Optional[Tag]:
        required = {h.lower() for h in required_headers}
        for table in soup.find_all("table"):
            header_row = table.find("tr")
            if not header_row:
                continue
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]
            if required.issubset(set(headers)):
                return table
        return None

    def fetch_residential_directory(self) -> List[Dict]:
        soup = BeautifulSoup(self._get_html(DJJ_RESIDENTIAL_DIR), "html.parser")
        table = self._pick_directory_table(soup, ["name", "region", "county", "risk"])
        if not table:
            logger.warning("Residential directory table not found")
            return []

        records: List[Dict] = []
        rows = table.find_all("tr")
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 7:
                continue
            link = cells[0].find("a")
            name = (link.get_text(strip=True) if link else cells[0].get_text(strip=True)) or ""
            href = link["href"] if (link and link.has_attr("href")) else ""
            if not name:
                continue
            slug = href.rstrip("/").rsplit("/", 1)[-1] if href else _slugify(name)
            provider_link = cells[7].find("a") if len(cells) > 7 else None
            provider_name = (
                provider_link.get_text(strip=True)
                if provider_link
                else (cells[7].get_text(strip=True) if len(cells) > 7 else "")
            )
            provider_name = re.sub(r"\s*\(opens in new window\)\s*$", "", provider_name).strip()
            records.append({
                "facility_type": "residential",
                "name": name,
                "slug": slug,
                "profile_url": _absolute_url(href),
                "region": cells[1].get_text(strip=True),
                "county": cells[2].get_text(strip=True),
                "city": cells[3].get_text(strip=True),
                "circuit": cells[4].get_text(strip=True),
                "gender": cells[5].get_text(strip=True),
                "risk_level": cells[6].get_text(strip=True),
                "provider_name": provider_name,
                "provider_url": provider_link["href"] if (provider_link and provider_link.has_attr("href")) else "",
            })
        logger.info(f"Residential directory: {len(records)} facilities")
        return records

    def fetch_detention_directory(self) -> List[Dict]:
        soup = BeautifulSoup(self._get_html(DJJ_DETENTION_DIR), "html.parser")
        table = self._pick_directory_table(soup, ["name", "city", "region"])
        if not table:
            logger.warning("Detention directory table not found")
            return []

        records: List[Dict] = []
        rows = table.find_all("tr")
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 5:
                continue
            link = cells[0].find("a")
            name = (link.get_text(strip=True) if link else cells[0].get_text(strip=True)) or ""
            href = link["href"] if (link and link.has_attr("href")) else ""
            if not name:
                continue
            slug = href.rstrip("/").rsplit("/", 1)[-1] if href else _slugify(name)
            records.append({
                "facility_type": "detention",
                "name": name,
                "slug": slug,
                "profile_url": _absolute_url(href),
                "counties_served": cells[1].get_text(strip=True),
                "city": cells[2].get_text(strip=True),
                "phone": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                "fax": cells[4].get_text(strip=True) if len(cells) > 4 else "",
                "region": cells[5].get_text(strip=True) if len(cells) > 5 else "",
                "gender": "",
                "risk_level": "Detention",
                "county": "",
                "circuit": "",
                "provider_name": "",
                "provider_url": "",
            })
        logger.info(f"Detention directory: {len(records)} facilities")
        return records

    def fetch_facility_profile(self, record: Dict) -> Dict:
        """Enrich a directory record with address/phone/capacity/director from
        the profile page. Best-effort: missing fields stay empty."""
        if not record.get("profile_url"):
            return record
        try:
            html = self._get_html(record["profile_url"])
        except Exception as exc:
            logger.warning(f"  Profile fetch failed for {record['slug']}: {exc}")
            return record

        soup = BeautifulSoup(html, "html.parser")
        # The profile body collapses to flat text — labels are single-space
        # separated rather than wrapped in distinct paragraphs, so we rely on
        # the fixed label vocabulary in DJJ_PROFILE_LABELS to slice it up.
        text_blob = " ".join(
            p.get_text(" ", strip=True) for p in soup.find_all(["p", "div", "li"])
        )
        text_blob = re.sub(r"\s+", " ", text_blob)
        labels = _parse_profile_labels(text_blob)

        capacity_raw = labels.get("operating capacity") or labels.get("capacity", "")
        bed_match = re.match(r"\s*(\d+)", capacity_raw)
        phone = _extract_phone(labels.get("phone") or labels.get("telephone", ""))
        fax = _extract_phone(labels.get("fax", ""))

        record.setdefault(
            "address",
            labels.get("mailing address")
            or labels.get("address")
            or _extract_florida_address(text_blob),
        )
        record.setdefault("phone", phone)
        record.setdefault("fax", fax)
        record.setdefault("capacity_raw", capacity_raw)
        record.setdefault("bed_capacity", bed_match.group(1) if bed_match else "")
        record.setdefault("director", labels.get("director", ""))
        record.setdefault("director_title", labels.get("director title", ""))
        record.setdefault("services", labels.get("services", ""))
        record.setdefault("state_owned", labels.get("state owned", ""))
        record.setdefault("email", labels.get("email", ""))
        return record

    def fetch_qi_index(self) -> List[Dict]:
        """Parse Residential and Detention QI report tables.

        Each table has a Program column followed by per-FY columns. A cell may
        contain a PDF anchor, a parenthetical status note, or be empty.
        """
        soup = BeautifulSoup(self._get_html(DJJ_QI_INDEX), "html.parser")
        entries: List[Dict] = []

        for heading in soup.find_all(re.compile(r"^h[1-6]$")):
            heading_text = heading.get_text(" ", strip=True).lower()
            if "residential" in heading_text and "program" in heading_text:
                facility_type, report_type = "residential", "QI Residential"
            elif "detention" in heading_text:
                facility_type, report_type = "detention", "QI Detention"
            else:
                continue

            table = heading.find_next("table")
            if not table:
                continue

            rows = table.find_all("tr")
            if not rows:
                continue
            header_cells = rows[0].find_all(["th", "td"])
            fy_labels = [_fy_label_normalize(c.get_text(" ", strip=True)) for c in header_cells]

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                program_raw = cells[0].get_text(" ", strip=True)
                program_name, program_status = _strip_status_suffix(program_raw)
                if not program_name:
                    continue
                for idx in range(1, len(cells)):
                    cell = cells[idx]
                    link = cell.find("a", href=True)
                    if not link:
                        continue
                    pdf_url = _absolute_url(link["href"])
                    if ".pdf" not in pdf_url.lower():
                        continue
                    fy_label = fy_labels[idx] if idx < len(fy_labels) else ""
                    link_text = link.get_text(" ", strip=True)
                    status_in_cell = re.search(r"(FINAL|MQI|Re-?Review|INTERIM)", link_text, re.IGNORECASE)
                    entries.append({
                        "report_type": report_type,
                        "facility_type_hint": facility_type,
                        "program_name": program_name,
                        "program_status": program_status,
                        "fiscal_year": fy_label,
                        "report_date": _fy_to_date(fy_label),
                        "status": status_in_cell.group(1).upper() if status_in_cell else "",
                        "pdf_url": pdf_url,
                        "file_name": unquote(pdf_url.rsplit("/", 1)[-1].split("?", 1)[0]),
                        "report_id": _stable_report_id(pdf_url),
                    })

        logger.info(f"QI index: {len(entries)} report links")
        return entries

    def fetch_prea_index(self) -> List[Dict]:
        """Scope PDF links to the single audit-reports table inside <main> —
        otherwise sidebar/related-document PDFs leak into the index."""
        soup = BeautifulSoup(self._get_html(DJJ_PREA_INDEX), "html.parser")
        main = soup.find("main") or soup
        scope = main.find("table") or main
        entries: List[Dict] = []
        seen_urls: Set[str] = set()
        for link in scope.find_all("a", href=True):
            href = link["href"]
            if ".pdf" not in href.lower():
                continue
            pdf_url = _absolute_url(href)
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)
            link_text = link.get_text(" ", strip=True)
            filename = unquote(pdf_url.rsplit("/", 1)[-1].split("?", 1)[0])
            # Filenames often look like "florida_alachua-academy_year2cycle3.pdf"
            stem = filename.rsplit(".", 1)[0]
            program_from_filename = re.sub(r"(?i)^florida[_-]", "", stem)
            program_from_filename = re.sub(r"(?i)[_-]year\d+cycle\d+.*$", "", program_from_filename)
            program_from_filename = re.sub(r"(?i)[_-]?(final|interim|prea[-_]?audit[-_]?report).*$", "", program_from_filename)
            program_from_filename = re.sub(r"[_-]+", " ", program_from_filename).strip()
            program_name = program_from_filename or link_text or stem
            cycle_match = re.search(r"(?i)year\s*(\d+)\s*cycle\s*(\d+)", stem)
            interim_or_final = "INTERIM" if re.search(r"(?i)\binterim\b", stem) else (
                "FINAL" if re.search(r"(?i)\bfinal\b", stem) else ""
            )
            entries.append({
                "report_type": "PREA",
                "facility_type_hint": "",
                "program_name": program_name,
                "program_status": "",
                "fiscal_year": "",
                "report_date": "",
                "status": interim_or_final,
                "cycle": f"Year {cycle_match.group(1)} Cycle {cycle_match.group(2)}" if cycle_match else "",
                "pdf_url": pdf_url,
                "file_name": filename,
                "report_id": _stable_report_id(pdf_url),
            })
        logger.info(f"PREA index: {len(entries)} report links")
        return entries

    def fetch_spep_index(self) -> List[Dict]:
        """Each program is an accordion <li> in <main>: a `div.question` holds
        the program name, and a nested table has FY rows with one or more PDF
        anchors per row (one per evidence-based service)."""
        entries: List[Dict] = []
        seen_urls: Set[str] = set()
        for path in (DJJ_SPEP_OPEN, DJJ_SPEP_CLOSED):
            try:
                html = self._get_html(path)
            except Exception as exc:
                logger.warning(f"SPEP page fetch failed ({path}): {exc}")
                continue
            soup = BeautifulSoup(html, "html.parser")
            main = soup.find("main") or soup
            program_status = "Closed" if "closed" in path.lower() else "Open"

            for li in main.find_all("li"):
                question = li.find("div", class_="question")
                table = li.find("table")
                if not (question and table):
                    continue
                program_name = question.get_text(" ", strip=True)
                if not program_name:
                    continue
                rows = table.find_all("tr")
                # First row is the header (Fiscal Year | Evidence-Based Service).
                for row in rows[1:]:
                    cells = row.find_all(["td", "th"])
                    if len(cells) < 2:
                        continue
                    fy_label = _fy_label_normalize(cells[0].get_text(" ", strip=True))
                    for link in cells[1].find_all("a", href=True):
                        if ".pdf" not in link["href"].lower():
                            continue
                        pdf_url = _absolute_url(link["href"])
                        if pdf_url in seen_urls:
                            continue
                        seen_urls.add(pdf_url)
                        service_type = link.get_text(" ", strip=True)
                        filename = unquote(pdf_url.rsplit("/", 1)[-1].split("?", 1)[0])
                        entries.append({
                            "report_type": "SPEP",
                            "facility_type_hint": "residential",
                            "program_name": program_name,
                            "program_status": program_status,
                            "fiscal_year": fy_label,
                            "report_date": _fy_to_date(fy_label),
                            "status": "",
                            "service_type": service_type,
                            "pdf_url": pdf_url,
                            "file_name": filename,
                            "report_id": _stable_report_id(pdf_url),
                            "source_page": _absolute_url(path),
                        })
        logger.info(f"SPEP index: {len(entries)} report links")
        return entries

    @staticmethod
    def _build_match_index(facilities: List[Dict]) -> Dict[str, Dict]:
        index: Dict[str, Dict] = {}
        for fac in facilities:
            for candidate in (fac.get("name", ""), fac.get("slug", "")):
                key = _normalize_key(candidate)
                if key and key not in index:
                    index[key] = fac
        return index

    def _match_to_facility(self, entry: Dict, match_index: Dict[str, Dict]) -> Optional[Dict]:
        entry_key = _normalize_key(entry["program_name"])
        if not entry_key:
            return None
        if entry_key in match_index:
            return match_index[entry_key]
        # Token-set fallback: a directory key whose tokens fully contain the
        # entry's tokens (or vice versa) is treated as a match.
        entry_tokens = set(entry_key.split())
        if not entry_tokens:
            return None
        for key, fac in match_index.items():
            key_tokens = set(key.split())
            if not key_tokens:
                continue
            overlap = entry_tokens & key_tokens
            if not overlap:
                continue
            smaller = min(len(entry_tokens), len(key_tokens))
            if smaller and len(overlap) / smaller >= 0.8:
                return fac
        return None

    def download_pdf(self, url: str) -> Optional[Path]:
        if not url:
            return None
        safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", unquote(url.rsplit("/", 1)[-1].split("?", 1)[0]))
        dest = self.pdf_dir / safe_name
        if dest.exists() and dest.stat().st_size > 0:
            return dest
        logger.info(f"  Downloading {safe_name}")
        try:
            response = self.session.get(url, timeout=120)
            response.raise_for_status()
            dest.write_bytes(response.content)
            time.sleep(0.2)
            return dest
        except requests.RequestException as exc:
            logger.warning(f"  download failed {url}: {exc}")
            return None

    def _build_report(self, entry: Dict, facility: Optional[Dict]) -> Dict:
        pdf_path = self.download_pdf(entry["pdf_url"])
        raw_content = extract_pdf_text(pdf_path, timeout=self.pdf_timeout) if pdf_path else ""
        if not raw_content:
            fallback = [
                f"Program: {entry.get('program_name', '')}",
                f"Report Type: {entry.get('report_type', '')}",
                f"Fiscal Year: {entry.get('fiscal_year', '')}",
                f"Status: {entry.get('status', '')}",
                f"PDF URL: {entry.get('pdf_url', '')}",
            ]
            raw_content = "\n".join(line for line in fallback if line.strip())

        report_type = entry.get("report_type", "")
        findings: List[Dict[str, str]] = []
        if report_type in ("QI Residential", "QI Detention"):
            findings = extract_djj_qi_findings(raw_content)
        elif report_type == "PREA":
            findings = extract_djj_prea_findings(raw_content)

        summary_parts = [report_type]
        if entry.get("fiscal_year"):
            summary_parts.append(entry["fiscal_year"])
        if entry.get("status"):
            summary_parts.append(entry["status"])
        if entry.get("cycle"):
            summary_parts.append(entry["cycle"])
        if findings:
            summary_parts.append(f"{len(findings)} finding{'' if len(findings) == 1 else 's'}")
        summary = " - ".join(p for p in summary_parts if p)

        categories: Dict[str, Any] = {
            "source": "DJJ",
            "report_type": report_type,
            "status": entry.get("status", ""),
            "fiscal_year": entry.get("fiscal_year", ""),
            "pdf_url": entry.get("pdf_url", ""),
            "file_name": entry.get("file_name", ""),
            "program_name_raw": entry.get("program_name", ""),
            "program_status_note": entry.get("program_status", ""),
            "findings": findings,
            "finding_count": len(findings),
        }
        if entry.get("cycle"):
            categories["cycle"] = entry["cycle"]
        if entry.get("service_type"):
            categories["service_type"] = entry["service_type"]
        if entry.get("source_page"):
            categories["source_page"] = entry["source_page"]
        if facility is None:
            categories["unmatched"] = True

        return {
            "report_id": entry["report_id"],
            "report_date": entry.get("report_date", ""),
            "raw_content": raw_content,
            "content_length": len(raw_content),
            "summary": summary,
            "categories": categories,
        }

    def _facility_payload(self, facility: Dict, reports: List[Dict]) -> Dict:
        # Caps match the inspection_facilities schema (phone VARCHAR(50),
        # facility_name VARCHAR(500), program_category VARCHAR(255), etc.).
        # full_address is TEXT and stays uncapped.
        facility_info: Dict[str, Any] = {
            "facility_name": _safe_field(facility["name"], 500),
            "program_name": _safe_field(f"DJJ-{facility['slug']}", 500),
            "program_category": _safe_field(facility.get("risk_level", ""), 255),
            "full_address": facility.get("address", "") or ", ".join(
                p for p in (facility.get("city", ""), "FL") if p
            ),
            "phone": _safe_field(facility.get("phone", ""), 50),
            "bed_capacity": _safe_field(facility.get("bed_capacity", ""), 50),
            "executive_director": _safe_field(facility.get("director", ""), 255),
            "license_exp_date": "",
            "relicense_visit_date": "",
            "action": _safe_field(
                "Program Closed"
                if "closed" in facility.get("status_note", "").lower()
                else "Active",
                255,
            ),
            "agency_source": "DJJ",
            "facility_type": facility.get("facility_type", ""),
            "operator": facility.get("provider_name", ""),
            "county": facility.get("county", ""),
            "circuit": facility.get("circuit", ""),
            "gender": facility.get("gender", ""),
            "region": facility.get("region", ""),
            "profile_url": facility.get("profile_url", ""),
        }
        if facility.get("provider_url"):
            facility_info["website"] = facility["provider_url"]
        return {"facility_info": facility_info, "reports": reports}

    def _unmatched_payload(self, entry: Dict, reports: List[Dict]) -> Dict:
        program = entry.get("program_name") or entry.get("file_name") or "Unknown DJJ Program"
        slug = _slugify(program) or hashlib.sha1(program.encode("utf-8")).hexdigest()[:10]
        facility_info = {
            "facility_name": _safe_field(program, 500),
            "program_name": _safe_field(f"DJJ-unmatched-{slug}", 500),
            "program_category": _safe_field(entry.get("report_type", ""), 255),
            "full_address": "",
            "phone": "",
            "bed_capacity": "",
            "executive_director": "",
            "license_exp_date": "",
            "relicense_visit_date": "",
            "action": "",
            "agency_source": "DJJ",
            "facility_type": entry.get("facility_type_hint", ""),
        }
        return {"facility_info": facility_info, "reports": reports}

    def scrape(
        self,
        categories: Iterable[str],
        seen: Optional[Dict[str, Set[str]]] = None,
        fetch_profiles: bool = True,
        limit: int = 0,
    ) -> Tuple[List[Dict], Dict[str, List[str]]]:
        seen = seen or {}
        categories = {c.lower() for c in categories}

        residential = self.fetch_residential_directory()
        detention = self.fetch_detention_directory() if "detention" in categories else []
        directory = residential + detention
        if fetch_profiles:
            logger.info("Enriching facility metadata from profile pages…")
            for fac in directory:
                self.fetch_facility_profile(fac)
        match_index = self._build_match_index(directory)

        all_entries: List[Dict] = []
        if "residential" in categories or "detention" in categories:
            all_entries.extend(
                e for e in self.fetch_qi_index()
                if (e["facility_type_hint"] == "residential" and "residential" in categories)
                or (e["facility_type_hint"] == "detention" and "detention" in categories)
            )
        if "prea" in categories:
            all_entries.extend(self.fetch_prea_index())
        if "spep" in categories:
            all_entries.extend(self.fetch_spep_index())

        grouped: Dict[str, List[Dict]] = defaultdict(list)
        unmatched: List[Dict] = []
        new_ids: Dict[str, List[str]] = defaultdict(list)
        skipped = 0

        for entry in all_entries:
            facility = self._match_to_facility(entry, match_index)
            key = facility["slug"] if facility else f"unmatched-{_slugify(entry.get('program_name', ''))}"
            if entry["report_id"] in seen.get(key, set()):
                skipped += 1
                continue
            if facility:
                grouped[facility["slug"]].append(entry)
            else:
                unmatched.append(entry)
            new_ids[key].append(entry["report_id"])

        if skipped:
            logger.info(f"Skipped {skipped} reports already in state")

        facilities_payload: List[Dict] = []
        slug_to_record = {f["slug"]: f for f in directory}
        matched_slugs = list(grouped.keys())
        # Group unmatched entries by their normalized program name so a single
        # phantom program with three PDFs becomes one facility, not three.
        unmatched_by_program: Dict[str, List[Dict]] = defaultdict(list)
        for entry in unmatched:
            unmatched_by_program[_slugify(entry.get("program_name", "")) or "unknown"].append(entry)
        unmatched_slugs = list(unmatched_by_program.keys())

        # Truncate before PDF downloads so smoke tests stay fast.
        if limit:
            matched_slugs = matched_slugs[:limit]
            remaining = max(0, limit - len(matched_slugs))
            unmatched_slugs = unmatched_slugs[:remaining]

        # Flatten matched + unmatched into a single work list. Keep entries
        # sorted within each key so `pool.map()` (which preserves input order)
        # yields per-facility report lists in stable date order.
        work: List[Tuple[str, Optional[Dict], Dict]] = []
        for slug in matched_slugs:
            entries = sorted(
                grouped[slug],
                key=lambda e: (e.get("report_date", ""), e.get("report_id", "")),
            )
            for entry in entries:
                work.append((slug, slug_to_record[slug], entry))
        for prog_slug in unmatched_slugs:
            entries = sorted(
                unmatched_by_program[prog_slug],
                key=lambda e: (e.get("report_date", ""), e.get("report_id", "")),
            )
            for entry in entries:
                work.append((prog_slug, None, entry))

        total_facilities = len(matched_slugs) + len(unmatched_slugs)
        total_reports = len(work)
        logger.info(
            f"Building {total_reports} reports across {total_facilities} facilities "
            f"({self.workers} worker thread{'s' if self.workers != 1 else ''}, "
            f"PDF timeout {self.pdf_timeout}s — first full run can still take a while)"
        )

        counter_lock = threading.Lock()
        counter = {"n": 0}

        def _process(item: Tuple[str, Optional[Dict], Dict]) -> Tuple[str, Dict]:
            key, facility, entry = item
            report = self._build_report(entry, facility)
            with counter_lock:
                counter["n"] += 1
                n = counter["n"]
            logger.info(
                f"  ({n}/{total_reports}) [{entry.get('report_type', '')}] "
                f"{entry.get('file_name', '')}"
            )
            return key, report

        results_by_key: Dict[str, List[Dict]] = defaultdict(list)
        if work:
            with ThreadPoolExecutor(max_workers=self.workers) as pool:
                for key, report in pool.map(_process, work):
                    results_by_key[key].append(report)

        for slug in matched_slugs:
            facilities_payload.append(
                self._facility_payload(slug_to_record[slug], results_by_key[slug])
            )
        for prog_slug in unmatched_slugs:
            facilities_payload.append(
                self._unmatched_payload(
                    unmatched_by_program[prog_slug][0], results_by_key[prog_slug]
                )
            )

        self.all_facilities = facilities_payload
        if unmatched:
            logger.warning(
                f"{len(unmatched)} reports could not be matched to a facility "
                f"(grouped into {len(unmatched_by_program)} synthetic facilities)"
            )

        logger.info(
            f"Scrape complete: {len(facilities_payload)} facilities, "
            f"{sum(len(f['reports']) for f in facilities_payload)} new reports"
        )
        return facilities_payload, dict(new_ids)


class FLAHCAScraper:
    """AHCA Residential Treatment Center scraper.

    Two endpoints:
      • FloridaHealthFinder (`quality.healthfinder.fl.gov`) — Razor Pages site
        that embeds the full facility list as JSON in the HTML returned from
        the AdvancedSearch POST. No DataTables AJAX needed.
      • dm_web (`apps.ahca.myflorida.com/dm_web/`) — classic ASP.NET; the
        `facility_inspection_details.aspx` page returns a structured deficiency
        table keyed by AHCA File Number + client code.
    """

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._dm_session_id = ""

    def fetch_fhf_facilities(self, type_code: str) -> List[Dict]:
        """Submit a Razor-Pages search for one facility type and return the
        embedded JSON facility list. The result page renders this same array
        client-side via jQuery DataTables — we just grab it directly."""
        resp = self.session.get(AHCA_FHF_SEARCH, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        form = soup.find("form", id="AdvancedSearchForm")
        if not form:
            raise RuntimeError("FHF: AdvancedSearchForm not found")
        fields = {
            inp.get("name"): inp.get("value", "")
            for inp in form.find_all("input")
            if inp.get("name")
        }
        fields["FacilityTypeSelection"] = type_code

        post = self.session.post(AHCA_FHF_HANDLER, data=fields, timeout=60)
        post.raise_for_status()
        match = re.search(
            r"(\[\s*\{[^}]*FileNumber[^}]*\}.*?\])",
            post.text,
            re.DOTALL,
        )
        if not match:
            logger.warning(f"FHF: no embedded facility array for type={type_code}")
            return []
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            logger.error(f"FHF: failed to parse embedded JSON for type={type_code}: {exc}")
            return []

    def _open_dm_session(self) -> str:
        resp = self.session.get(AHCA_DM_WEB_BASE, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        m = re.search(r"\(S\((\w+)\)\)", resp.url)
        if not m:
            raise RuntimeError(f"dm_web: could not extract session id from {resp.url}")
        return m.group(1)

    def fetch_inspection_deficiencies(
        self, file_number: str, client_code: str, provider_type_label: str, name: str
    ) -> List[Dict]:
        """Fetch `facility_inspection_details.aspx` and parse the gridView
        deficiency rows. Returns an empty list when the facility has no
        recorded surveys (common for newly licensed facilities)."""
        if not self._dm_session_id:
            self._dm_session_id = self._open_dm_session()

        url = (
            f"{AHCA_DM_WEB_BASE}(S({self._dm_session_id}))/facility_inspection_details.aspx"
            f"?client_code={client_code}"
            f"&file_number={file_number}"
            f"&provider_name={quote_plus(name or '')}"
            f"&provider_type={quote_plus(provider_type_label)}"
        )
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning(f"  dm_web fetch failed for file_number={file_number}: {exc}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        grid = soup.find("table", id="gridView")
        if not grid:
            return []

        deficiencies: List[Dict] = []
        for row in grid.find_all("tr")[1:]:
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) < 6:
                continue
            deficiencies.append({
                "survey_date": cells[0],
                "inspection_type": cells[1],
                "track_id": cells[2],
                "deficiency": cells[3],
                "requirement_description": cells[4],
                "correction_date": cells[5],
            })
        return deficiencies

    @staticmethod
    def _normalize_survey_date(raw: str) -> str:
        """Pass through dates already in mm/dd/yyyy. Stored as a sortable
        string upstream — the frontend re-parses as needed."""
        raw = (raw or "").strip()
        return raw

    def _build_reports_from_deficiencies(self, deficiencies: List[Dict]) -> List[Dict]:
        """Group deficiency rows by (Survey Date + Track ID) — each group is
        one inspection visit and becomes one report row in our schema."""
        groups: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
        for d in deficiencies:
            groups[(d["survey_date"], d["track_id"])].append(d)

        reports: List[Dict] = []
        for (survey_date, track_id), items in groups.items():
            inspection_type = items[0]["inspection_type"]
            lines = [
                f"Inspection: {inspection_type}",
                f"Survey Date: {survey_date}",
                f"Track ID: {track_id}",
                f"Deficiencies: {len(items)}",
                "",
            ]
            for d in items:
                lines.append(
                    f"  [{d['deficiency']}] {d['requirement_description']}"
                    f"  (Correction: {d['correction_date']})"
                )
            raw_content = "\n".join(lines)
            report_id = f"{track_id or 'NOID'}-{survey_date or 'NODATE'}"
            reports.append({
                "report_id": report_id,
                "report_date": self._normalize_survey_date(survey_date),
                "raw_content": raw_content,
                "content_length": len(raw_content),
                "summary": f"{inspection_type} — {len(items)} deficienc{'y' if len(items) == 1 else 'ies'}",
                "categories": {
                    "source": "AHCA",
                    "report_type": inspection_type,
                    "track_id": track_id,
                    "survey_date": survey_date,
                    "deficiency_count": len(items),
                    "deficiencies": items,
                },
            })
        reports.sort(key=lambda r: (r.get("report_date", ""), r["report_id"]))
        return reports

    def _facility_payload(self, fhf: Dict, reports: List[Dict]) -> Dict:
        addr_parts = [
            fhf.get("Address", "").strip(),
            fhf.get("Address2", "").strip(),
            fhf.get("City", "").strip(),
            f"FL {fhf.get('Zip', '').strip()}".strip(),
        ]
        full_address = ", ".join(p for p in addr_parts if p and p != "FL")
        is_closed = str(fhf.get("IsClosed", "")).lower() == "true"
        return {
            "facility_info": {
                "facility_name": _safe_field(fhf.get("Name", "") or f"AHCA File #{fhf.get('FileNumber', '')}", 500),
                "program_name": _safe_field(f"AHCA-{fhf.get('FileNumber', '')}", 500),
                "program_category": _safe_field(fhf.get("FacilityType", ""), 255),
                "full_address": full_address,
                "phone": _safe_field(fhf.get("PhoneNumber", ""), 50),
                "bed_capacity": _safe_field(fhf.get("BedCount", ""), 50),
                "executive_director": "",
                "license_exp_date": "",
                "relicense_visit_date": "",
                "action": _safe_field("Closed" if is_closed else "Active", 255),
                "agency_source": "AHCA",
                "facility_type_code": fhf.get("ClientCode", ""),
                "license_number": fhf.get("LicenseNumber", ""),
                "license_id": fhf.get("LicenseID", ""),
                "license_status": fhf.get("LicenseStatus", ""),
                "profile_url": (
                    f"{AHCA_FHF_BASE}/Facility-Provider/Profile/?LID={fhf.get('LicenseID', '')}"
                    if fhf.get("LicenseID") else ""
                ),
            },
            "reports": reports,
        }

    def scrape(
        self,
        seen: Optional[Dict[str, Set[str]]] = None,
        limit: int = 0,
        type_codes: Optional[List[str]] = None,
    ) -> Tuple[List[Dict], Dict[str, List[str]]]:
        seen = seen or {}
        new_ids: Dict[str, List[str]] = defaultdict(list)
        all_facilities: List[Dict] = []

        type_codes = type_codes or list(AHCA_FACILITY_TYPES.keys())
        for type_code in type_codes:
            if type_code not in AHCA_FACILITY_TYPES:
                logger.warning(f"Unknown AHCA facility type code: {type_code}")
                continue
            client_code, provider_type_label = AHCA_FACILITY_TYPES[type_code]
            logger.info(f"Fetching FHF facilities for type={type_code} ({provider_type_label})")
            recs = self.fetch_fhf_facilities(type_code)
            logger.info(f"  {len(recs)} facilities returned")

            for i, rec in enumerate(recs):
                if limit and len(all_facilities) >= limit:
                    break
                file_no = rec.get("FileNumber", "")
                name = rec.get("Name", "") or f"AHCA File #{file_no}"
                logger.info(
                    f"  [{i+1}/{len(recs)}] {name} (file={file_no}, status={rec.get('LicenseStatus','')})"
                )
                deficiencies = self.fetch_inspection_deficiencies(
                    file_number=file_no,
                    client_code=client_code,
                    provider_type_label=provider_type_label,
                    name=name,
                )
                reports = self._build_reports_from_deficiencies(deficiencies)
                # Incremental filter: skip already-posted reports.
                key = file_no
                new_reports = [
                    r for r in reports
                    if r["report_id"] not in seen.get(key, set())
                ]
                if not new_reports:
                    continue
                all_facilities.append(self._facility_payload(rec, new_reports))
                new_ids[key].extend(r["report_id"] for r in new_reports)

        logger.info(
            f"AHCA scrape complete: {len(all_facilities)} facilities with new reports, "
            f"{sum(len(f['reports']) for f in all_facilities)} new reports"
        )
        return all_facilities, dict(new_ids)


class FLDCFScraper:
    """DCF Residential Group Care scraper — not implementable from a single
    public data source.

    Recon (May 2026) confirmed that:
      • CARES (`caressearch.myflfamilies.com`, API at `caresapi.myflfamilies.com`)
        is the *Child Care Facility* search — Heartland Educational Group,
        Goddard School, etc. The only `providerType` it returns is "Child Care
        Facility". RGC providers do not appear.
      • DCF's Residential Group Care Licensing page is overview-only with no
        linked provider directory.
      • Provider lists are maintained by each of FL's ~17 Community-Based Care
        (CBC) lead agencies under separate contracts — there is no unified
        public registry comparable to DJJ's or AHCA's.

    DJJ + AHCA together already cover the institutionally-relevant Florida
    facilities for KOP (residential commitment programs and licensed RTC /
    Therapeutic Group Homes). If a public RGC source ever materializes, fill
    this class in then.
    """

    def scrape(self, *_, **__) -> Tuple[List[Dict], Dict[str, List[str]]]:
        raise NotImplementedError(
            "DCF Residential Group Care has no unified public source — DCF "
            "Office of Licensing does not publish a provider directory, and "
            "CARES covers child-care facilities only. See class docstring."
        )


def save_to_api(facilities: List[Dict]) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="FL",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def _run_djj(args: argparse.Namespace) -> None:
    state = load_state(DJJ_STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    categories = [c.strip().lower() for c in args.categories.split(",") if c.strip()]
    invalid = [c for c in categories if c not in DJJ_CATEGORIES]
    if invalid:
        raise SystemExit(f"Unknown DJJ categories: {invalid}. Valid: {DJJ_CATEGORIES}")

    scraper = FLDJJScraper(workers=args.workers, pdf_timeout=args.pdf_timeout)
    facilities, new_ids = scraper.scrape(
        categories=categories,
        seen=seen,
        fetch_profiles=not args.no_profiles,
        limit=args.limit,
    )
    if args.limit:
        kept_keys = {
            fac["facility_info"]["program_name"].removeprefix("DJJ-")
            for fac in facilities
        }
        new_ids = {k: v for k, v in new_ids.items() if k in kept_keys}

    facilities_to_post = [f for f in facilities if f["reports"]]
    if not facilities_to_post:
        logger.info("No new reports since last run")
        return

    logger.info(f"Posting {len(facilities_to_post)} facilities with new reports")
    if args.no_post:
        logger.info("--no-post set; skipping API write (state not advanced)")
        return
    if save_to_api(facilities_to_post):
        merge_new_ids(state, new_ids)
        save_state(DJJ_STATE_FILE, state)
        logger.info("Saved successfully")
    else:
        logger.error("API save failed — state not advanced")


def _run_ahca(args: argparse.Namespace) -> None:
    state = load_state(AHCA_STATE_FILE)
    seen = {} if args.full else seen_from_state(state)

    scraper = FLAHCAScraper()
    facilities, new_ids = scraper.scrape(seen=seen, limit=args.limit)

    facilities_to_post = [f for f in facilities if f["reports"]]
    if not facilities_to_post:
        logger.info("No new reports since last run")
        return

    logger.info(f"Posting {len(facilities_to_post)} facilities with new reports")
    if args.no_post:
        logger.info("--no-post set; skipping API write (state not advanced)")
        return
    if save_to_api(facilities_to_post):
        merge_new_ids(state, new_ids)
        save_state(AHCA_STATE_FILE, state)
        logger.info("Saved successfully")
    else:
        logger.error("API save failed — state not advanced")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Florida youth-facility data (DJJ / AHCA / DCF)")
    parser.add_argument("--source", required=True, choices=("djj", "ahca", "dcf"),
                        help="Which Florida data source to scrape")
    parser.add_argument("--categories", default=",".join(DJJ_CATEGORIES),
                        help=f"(DJJ only) comma-separated subset of {DJJ_CATEGORIES}")
    parser.add_argument("--full", action="store_true",
                        help="Ignore the local state file and re-scan all reports")
    parser.add_argument("--no-post", action="store_true",
                        help="Scrape and cache PDFs without POSTing to the inspections API")
    parser.add_argument("--no-profiles", action="store_true",
                        help="(DJJ only) skip per-facility profile fetches (faster smoke test)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Post at most N facilities (for smoke testing)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent PDF download/parse workers (default {DEFAULT_WORKERS}; env FL_WORKERS)")
    parser.add_argument("--pdf-timeout", type=int, default=DEFAULT_PDF_TIMEOUT,
                        help=f"Seconds before abandoning pdfplumber on a single PDF (default {DEFAULT_PDF_TIMEOUT}; env FL_PDF_TIMEOUT)")
    args = parser.parse_args()

    if args.source == "djj":
        _run_djj(args)
    elif args.source == "ahca":
        _run_ahca(args)
    elif args.source == "dcf":
        FLDCFScraper().scrape()


if __name__ == "__main__":
    main()
