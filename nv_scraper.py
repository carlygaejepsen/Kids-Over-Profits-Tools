"""
Nevada DPBH (ALiS / Aithent) Licensee + Inspection Scraper

Scrapes residential facilities for minors licensed by:
  - DSS Child Care Licensing (CCP):  Institutions (NRS 432A)
  - HCQC / Nevada Health Authority (HHF): PRTF, Drug/Alcohol Treatment, Recovery Centers

Both share the same public ASP.NET WebForms search at nvdpbh.aithent.com.
For each facility, we follow the SODPublicView page to collect inspection records
(date, number, reason, grade) and post the resulting facility+report dicts to the
KidsOverProfits inspections API.
"""

import argparse
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

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

STATE_FILE = Path(os.getenv("NV_STATE_FILE", ".nv_state.json"))

SEARCH_URL = "https://nvdpbh.aithent.com/Protected/LIC/LicenseeSearch.aspx?Program=HF&PubliSearch=Y"
DETAIL_URL_TMPL = (
    "https://nvdpbh.aithent.com/Protected/INS/SODPublicView.aspx"
    "?LicenseeId={licensee_id}&Program={program}&CredentialType={credential_type}"
    "&LicenseeType={licensee_type}&mode=&EntityType={entity_type}"
    "&LicenseNumber={license_number}&AddressTypeCode={address_type_code}"
    "&LicenseId={license_id}&LikePopUp=&Mode=V&IsPopUp=Y"
)

# Business-Unit / License-Type combinations that house TTI-relevant residential
# facilities for minors. Adding more types here is the main scope dial.
TARGETS = [
    {"agency": "DSS-CCL", "business_unit": "CCP", "license_type": "INS"},   # Institution (RCCI / shelter)
    {"agency": "HCQC",    "business_unit": "HHF", "license_type": "PRTF"},  # Psychiatric Residential Treatment
    {"agency": "HCQC",    "business_unit": "HHF", "license_type": "ADA"},   # Drug/Alcohol Treatment
    {"agency": "HCQC",    "business_unit": "HHF", "license_type": "RCF"},   # Recovery Center
]

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KOP-NV-scraper)"

# Credential-type substrings that unambiguously indicate adult-only facilities.
# Checked case-insensitively against hCredentialType (and hfName as a fallback).
_ADULT_CREDENTIAL_KEYWORDS = [
    "adult",
    "senior",
    "geriatric",
    "assisted living",
    "nursing home",
    "skilled nursing",
    "memory care",
]

# Credential-type substrings that confirm a facility serves minors.
# Used for HCQC/HHF types where the search returns mixed-age results.
_MINOR_CREDENTIAL_KEYWORDS = [
    "adolescent",
    "youth",
    "juvenile",
    "child",
    "minor",
    "teen",
    "pediatric",
]


def _is_minor_serving(row: Dict[str, str], agency: str) -> bool:
    """Return True if this facility plausibly serves minors.

    CCP/DSS-CCL institutions are governed by NRS 432A (child care), so they
    are always included.  HCQC/HHF facilities need active confirmation because
    the same license type (ADA, RCF) covers both adolescent and adult programs.
    """
    if agency == "DSS-CCL":
        return True

    cred = (row.get("hCredentialType", "") or "").lower()
    name = (row.get("hfName", "") or "").lower()
    combined = cred + " " + name

    # Explicit adult indicators → exclude
    if any(kw in combined for kw in _ADULT_CREDENTIAL_KEYWORDS):
        return False

    # Explicit minor indicators → include
    if any(kw in combined for kw in _MINOR_CREDENTIAL_KEYWORDS):
        return True

    # PRTF is defined by CMS as serving individuals under 21 → always include.
    if "prtf" in cred or "psychiatric residential" in cred:
        return True

    # For ADA / RCF with no clear age signal: exclude by default.
    # These types produce mostly adult recovery homes in Nevada.
    return False

# ASP.NET form field names (long, used everywhere)
F_BU       = "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$ddlBusinessUnit"
F_ENTITY   = "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$ddlEntity"
F_LIC_TYPE = "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$cmbLicenseType"
F_COUNTY   = "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$cmbCounty"
F_SEARCH_BUTTON = "ctl00$ContentPlaceHolder1$CommonLinkButton1"
F_RESULTS_GRID  = "ctl00$ContentPlaceHolder1$ucLicenseeSearchResult$ResultsGrid"
F_SOD_GRID      = "ctl00$ContentPlaceHolder1$ucSODgrid$ResultsGrid"


def smart_title_case(text: Optional[str]) -> str:
    """Title-case a name while preserving common acronyms.

    NV facility names come back ALL CAPS from ALiS (e.g. CHILD HAVEN).
    """
    if not text:
        return ""
    text = text.strip()
    if not text:
        return ""

    keep_upper = {
        "LLC", "INC", "INC.", "CORP", "CORP.", "LTD", "LTD.", "LP", "LLP", "PC", "PA",
        "II", "III", "IV", "V", "VI", "USA", "US", "NV", "DBA",
    }
    lowercase = {"a", "an", "and", "as", "at", "by", "for", "in", "of", "on", "or", "the", "to"}
    parts = text.split()
    out = []
    for i, w in enumerate(parts):
        cleaned = re.sub(r"[^A-Za-z]", "", w).upper()
        if cleaned in keep_upper:
            out.append(w.upper())
        elif i > 0 and w.lower() in lowercase:
            out.append(w.lower())
        else:
            out.append(w.capitalize())
    return " ".join(out)


def _viewstate(soup: BeautifulSoup) -> Dict[str, str]:
    out = {}
    for k in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        el = soup.find("input", {"name": k})
        out[k] = el.get("value", "") if el else ""
    return out


def _base_search_form(business_unit: str, license_type: str) -> Dict[str, str]:
    return {
        F_BU: business_unit,
        F_ENTITY: "B",
        "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$txtLastName": "",
        "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$txtFirstName": "",
        "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$txtLicenseNo": "",
        F_LIC_TYPE: license_type,
        "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$txtCity": "",
        F_COUNTY: "All",
        "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$txtZip": "",
        "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$txtPhoneNumber": "",
        "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$ClickEvenORAdd": "Hide",
        "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$AddressClick": "Hide",
        "ctl00$ContentPlaceHolder1$hdnAllowPublicSearchImport": "",
        "ctl00$ContentPlaceHolder1$hdnSearchResult": "N",
    }


def _post(session: requests.Session, url: str, form: Dict[str, str], timeout: int = 180) -> BeautifulSoup:
    r = session.post(url, data=form, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def _run_search(session: requests.Session, business_unit: str, license_type: str) -> BeautifulSoup:
    """Execute the cascade + search and return the first results page."""
    r = session.get(SEARCH_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    form = _base_search_form(business_unit, license_type)
    form["__EVENTARGUMENT"] = ""

    # Cascade 1: select Business Unit (populates Entity dropdown)
    f1 = dict(form, **_viewstate(soup), __EVENTTARGET=F_BU)
    f1[F_ENTITY] = ""
    soup = _post(session, SEARCH_URL, f1)

    # Cascade 2: select Entity = B (Agency) — enables Search button
    f2 = dict(form, **_viewstate(soup), __EVENTTARGET=F_ENTITY)
    soup = _post(session, SEARCH_URL, f2)

    # Search
    f3 = dict(form, **_viewstate(soup), __EVENTTARGET=F_SEARCH_BUTTON)
    soup = _post(session, SEARCH_URL, f3)
    return soup


def _row_to_dict(row) -> Optional[Dict[str, str]]:
    """Extract the hidden-field metadata + visible disciplinary flag from one result row."""
    hidden = {}
    for inp in row.find_all("input", type="hidden"):
        name = inp.get("id", "") or ""
        # The id has a long prefix; keep the trailing part after ResultsGrid_ctlNN_
        m = re.search(r"_ctl\d+_(.+)$", name)
        if m:
            hidden[m.group(1)] = (inp.get("value", "") or "").strip()

    if not hidden.get("hLicenseeId") or not hidden.get("hLicenseId"):
        return None

    cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
    # Visible columns (verified by probe):
    # 0 Name (with hidden inputs), 1 Credential Type, 2 Credential Number, 3 Status,
    # 4 Expiration Date, 5 Disciplinary (Y/N), 6 Address, 7 Phone, 8 First Issue Date,
    # 9 Primary Contact Name, 10 Primary Contact Role, 11 Action (View Detail link)
    hidden["_visible_disciplinary"] = cells[5] if len(cells) > 5 else "N"
    hidden["_visible_first_issue"]  = cells[8] if len(cells) > 8 else ""
    hidden["_visible_role"]         = cells[10] if len(cells) > 10 else ""
    return hidden


def _parse_results_page(soup: BeautifulSoup) -> List[Dict[str, str]]:
    div = soup.find("div", id="ctl00_ContentPlaceHolder1_ucLicenseeSearchResult_divScroll")
    if not div:
        return []
    rows = div.find_all("tr")
    out = []
    for r in rows:
        # Skip header row (no hidden inputs) and pagination row
        rd = _row_to_dict(r)
        if rd:
            out.append(rd)
    return out


def _total_pages(soup: BeautifulSoup) -> int:
    fc = soup.find("span", id="ctl00_ContentPlaceHolder1_ucLicenseeSearchResult_FooterCount")
    if not fc:
        return 1
    m = re.search(r"of\s+(\d+)\s+records", fc.get_text() or "")
    if not m:
        return 1
    total = int(m.group(1))
    # ALiS shows 10 per page
    return max(1, (total + 9) // 10)


def _paginate_search(session: requests.Session, soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Walk all pages of a search result, returning every row's dict."""
    rows = _parse_results_page(soup)
    pages = _total_pages(soup)
    logger.info("  page 1/%d: +%d facilities", pages, len(rows))

    all_rows = list(rows)
    for page_num in range(2, pages + 1):
        # Build a postback to the results grid with EVENTARGUMENT='Page$N'
        # Reuse the previous form fields by reading them from the current soup
        form = _form_state_from_soup(soup)
        form["__EVENTTARGET"] = F_RESULTS_GRID
        form["__EVENTARGUMENT"] = f"Page${page_num}"
        soup = _post(session, SEARCH_URL, form)
        page_rows = _parse_results_page(soup)
        logger.info("  page %d/%d: +%d facilities", page_num, pages, len(page_rows))
        all_rows.extend(page_rows)
        time.sleep(0.5)
    return all_rows


def _form_state_from_soup(soup: BeautifulSoup) -> Dict[str, str]:
    """Collect every <input> name/value from a rendered page so we can echo them in the next POST.

    ASP.NET expects every form field to be present in the postback or it may drop state.
    """
    out: Dict[str, str] = {}
    for inp in soup.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        ttype = (inp.get("type") or "").lower()
        if ttype in ("checkbox", "radio") and not inp.has_attr("checked"):
            continue
        out[name] = inp.get("value", "") or ""
    for sel in soup.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True)
        if opt is None:
            opt = sel.find("option")
        out[name] = opt.get("value", "") if opt else ""
    for ta in soup.find_all("textarea"):
        name = ta.get("name")
        if name:
            out[name] = ta.get_text() or ""
    return out


def _prime_detail_session(session: requests.Session, soup: BeautifulSoup, row_index: int) -> None:
    """POST the View Detail link for one row so the server stashes LicenseeId in session.

    SODPublicView.aspx requires this server-side state — without it we get a session-timeout page.
    `row_index` is 0-based within the current page's grid (ctl02 = first data row).
    """
    form = _form_state_from_soup(soup)
    # ASP.NET grids label data rows ctl02, ctl03, ... (ctl01 is the header)
    form["__EVENTTARGET"] = (
        f"ctl00$ContentPlaceHolder1$ucLicenseeSearchResult$ResultsGrid$ctl{row_index + 2:02d}$lnkViewDeatilPublic"
    )
    form["__EVENTARGUMENT"] = ""
    session.post(SEARCH_URL, data=form, timeout=120)


def _fetch_inspections(session: requests.Session, row: Dict[str, str], agency: str) -> List[Dict]:
    """GET SODPublicView.aspx using the row's hidden fields, walk all pages of the SOD grid."""
    url = DETAIL_URL_TMPL.format(
        licensee_id=row["hLicenseeId"],
        program=row.get("hfProgram", ""),
        credential_type=row.get("hLicenseTypeCode", ""),
        licensee_type=row.get("hLicenseeType", "B"),
        entity_type=row.get("hdnentityType", "LSE"),
        license_number=row.get("hfLicenseNumber", ""),
        address_type_code=row.get("HfAddressTypeCode", "PHL"),
        license_id=row.get("hLicenseId", ""),
    )
    r = session.get(url, timeout=60, headers={"Referer": SEARCH_URL})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    inspections = []
    page_num = 1
    while True:
        page_inspections = _parse_sod_grid(soup, row, agency)
        inspections.extend(page_inspections)
        # Look for next page link inside the SOD grid
        next_target_arg = _next_sod_page_arg(soup, page_num + 1)
        if not next_target_arg:
            break
        page_num += 1
        form = _form_state_from_soup(soup)
        form["__EVENTTARGET"] = F_SOD_GRID
        form["__EVENTARGUMENT"] = next_target_arg
        r = session.post(url, data=form, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        time.sleep(0.4)
    return inspections


def _next_sod_page_arg(soup: BeautifulSoup, page_num: int) -> Optional[str]:
    grid = soup.find("table", id="ctl00_ContentPlaceHolder1_ucSODgrid_ResultsGrid")
    if not grid:
        return None
    target_arg = f"Page${page_num}"
    for a in grid.find_all("a"):
        href = a.get("href", "") or ""
        if target_arg in href:
            return target_arg
    return None


def _parse_sod_grid(soup: BeautifulSoup, row: Dict[str, str], agency: str) -> List[Dict]:
    grid = soup.find("table", id="ctl00_ContentPlaceHolder1_ucSODgrid_ResultsGrid")
    if not grid:
        return []
    rows = grid.find_all("tr")
    if not rows:
        return []

    # Map header text → column index. CCL has an "Inspection Reason" column;
    # HCQC does not — so we cannot use fixed indices.
    header_cells = rows[0].find_all(["th", "td"])
    col = {}
    for i, c in enumerate(header_cells):
        key = re.sub(r"\W+", "_", c.get_text(" ", strip=True).lower()).strip("_")
        col[key] = i

    def cell(cells, key):
        idx = col.get(key)
        if idx is None or idx >= len(cells):
            return ""
        return cells[idx].get_text(" ", strip=True)

    out = []
    for tr in rows[1:]:
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue  # pagination or spacer row

        date_time      = cell(cells, "inspection_date_time")
        inspection_no  = cell(cells, "inspection_number")
        event_id       = cell(cells, "event_id")
        grade          = cell(cells, "grade")
        reason         = cell(cells, "inspection_reason")
        docs_cell      = cell(cells, "document_s") or cell(cells, "documents")
        if not inspection_no:
            continue

        m = re.search(r"\((\d+)\)", docs_cell)
        doc_count      = int(m.group(1)) if m else 0

        # Split date+time
        date_only, _, time_only = date_time.partition(" ")
        # Tighten up "01/15/2026 1:30 PM" → date "01/15/2026" time "1:30 PM"

        out.append({
            "report_id":      inspection_no,
            "report_date":    date_only,
            "raw_content": (
                f"Inspection #{inspection_no}\n"
                f"Date/Time: {date_time}\n"
                f"Reason: {reason}\n"
                f"Grade: {grade or 'N/A'}\n"
                f"Event ID: {event_id or 'N/A'}\n"
                f"Documents available: {doc_count}"
            ),
            "content_length": 0,  # filled below
            "summary":        reason or "Inspection",
            "categories": {
                "agency":           agency,
                "credential_type":  row.get("hCredentialType", ""),
                "license_number":   row.get("hfLicenseNumberToDisplay", ""),
                "inspection_reason": reason,
                "grade":            grade or None,
                "event_id":         event_id or None,
                "inspection_time":  time_only or None,
                "doc_count":        doc_count,
            },
        })
    for r_ in out:
        r_["content_length"] = len(r_["raw_content"])
    return out


def _row_to_facility_info(row: Dict[str, str]) -> Dict[str, str]:
    return {
        "facility_name":       smart_title_case(row.get("hfName", "")),
        "program_name":        row.get("hfLicenseNumberToDisplay", "") or row.get("hfLicenseNumber", ""),
        "program_category":    smart_title_case(row.get("hCredentialType", "")),
        "full_address":        smart_title_case(row.get("hPrimaryAddress", "")),
        "phone":               row.get("hPhoneNumber", ""),
        "executive_director":  smart_title_case(row.get("hContactName", "")),
        "license_exp_date":    row.get("hExpiryDate", ""),
        "action":              row.get("hdnStatusCode", ""),
    }


class NVFacilityScraper:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    def scrape(self, seen: Optional[Dict[str, Set[str]]] = None) -> Tuple[List[Dict], Dict[str, List[str]]]:
        seen = seen or {}
        facilities: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}

        for target in TARGETS:
            agency  = target["agency"]
            bu      = target["business_unit"]
            lic     = target["license_type"]
            logger.info("=== %s / %s / %s ===", agency, bu, lic)
            try:
                soup = _run_search(self.session, bu, lic)
            except requests.RequestException as exc:
                logger.error("Search failed for %s/%s: %s", bu, lic, exc)
                continue

            rows = _paginate_search(self.session, soup)
            rows = [r for r in rows if _is_minor_serving(r, agency)]
            logger.info("  total rows after minor-serving filter: %d", len(rows))

            for i, row in enumerate(rows):
                name = row.get("hfName", "?")
                key = row.get("hfLicenseNumberToDisplay") or row.get("hLicenseeId") or name
                already = seen.get(key, set())

                # Re-run the search so the View Detail postback uses a current ViewState
                # for THIS row (the row index changes per page, so we resync per facility).
                # For efficiency, we only re-search when crossing a page boundary —
                # within a page we can prime once and reuse.
                # Simpler: re-run search for each row. ALiS pages are small & cached.
                try:
                    fresh_soup = _run_search(self.session, bu, lic)
                    page_size = 10
                    target_page = (i // page_size) + 1
                    if target_page > 1:
                        form = _form_state_from_soup(fresh_soup)
                        form["__EVENTTARGET"] = F_RESULTS_GRID
                        form["__EVENTARGUMENT"] = f"Page${target_page}"
                        fresh_soup = _post(self.session, SEARCH_URL, form)
                    row_in_page = i % page_size
                    _prime_detail_session(self.session, fresh_soup, row_in_page)
                    inspections = _fetch_inspections(self.session, row, agency)
                except requests.RequestException as exc:
                    logger.warning("  detail fetch failed for %s: %s", name, exc)
                    inspections = []

                logger.info("  [%d/%d] %s — %d inspections", i + 1, len(rows), name, len(inspections))

                # Filter to new inspections only (skip ones we already posted)
                new_inspections = [r for r in inspections if r["report_id"] not in already]
                if not new_inspections and key in seen:
                    # No new reports for an already-seen facility — skip the facility entirely.
                    # (The facility row itself doesn't change often; the upsert is harmless,
                    # but there's no point sending no-op writes.)
                    continue

                facility = {
                    "facility_info": _row_to_facility_info(row),
                    "reports": new_inspections,
                }
                facilities.append(facility)
                if new_inspections:
                    new_ids[key] = [r["report_id"] for r in new_inspections]

                time.sleep(0.5)

        return facilities, new_ids


def save_to_api(facilities: List[Dict], replace: bool = False) -> bool:
    if API_KEY == "CHANGE_ME":
        logger.error("KOP_DATA_API_KEY env var is not set — refusing to POST")
        return False
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="NV",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=120,
        replace=replace,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def _is_minor_serving_facility(facility: Dict) -> bool:
    """Same keyword logic as _is_minor_serving(), applied to stored API-format dicts.

    Used by --purge to filter already-posted facilities without re-scraping ALiS.
    """
    info = facility.get("facility_info") or {}
    cred = (info.get("program_category") or "").lower()
    name = (info.get("facility_name") or "").lower()
    combined = cred + " " + name

    if any(kw in combined for kw in _ADULT_CREDENTIAL_KEYWORDS):
        return False
    if any(kw in combined for kw in _MINOR_CREDENTIAL_KEYWORDS):
        return True
    if "prtf" in cred or "psychiatric residential" in cred:
        return True
    # Credential types that contain drug/alcohol/recovery but no age signal → adult default
    hcqc_markers = {"drug", "alcohol", "recovery", "treatment", "substance"}
    if any(m in cred for m in hcqc_markers):
        return False
    # No HCQC markers → CCP institution type (child care under NRS 432A) → include
    return True


def purge_adults() -> None:
    """Read all current NV facilities from the API, drop adult-only ones, rewrite the state."""
    if API_KEY == "CHANGE_ME":
        logger.error("KOP_DATA_API_KEY env var is not set — refusing to POST")
        return

    read_url = API_URL.replace("inspections-write.php", "inspections-read.php")
    logger.info("Fetching current NV facilities from %s", read_url)
    try:
        resp = requests.get(read_url, params={"state": "NV"}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.error("Failed to read current NV data: %s", exc)
        return

    all_facilities = data.get("facilities") or []
    logger.info("Found %d facilities in the database", len(all_facilities))

    kept    = [f for f in all_facilities if     _is_minor_serving_facility(f)]
    removed = [f for f in all_facilities if not _is_minor_serving_facility(f)]

    logger.info("Keeping %d minor-serving facilities", len(kept))
    for f in removed:
        name = (f.get("facility_info") or {}).get("facility_name", "?")
        cred = (f.get("facility_info") or {}).get("program_category", "?")
        logger.info("  REMOVING: %s (%s)", name, cred)

    if not removed:
        logger.info("Nothing to remove — database is already clean")
        return

    logger.info("Rewriting NV database with replace=True (%d facilities)", len(kept))
    if save_to_api(kept, replace=True):
        logger.info("Purge complete. Removed %d adult/non-minor facilities.", len(removed))
    else:
        logger.error("API save failed during purge — database unchanged")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true",
                    help=f"Ignore {STATE_FILE} and re-post every inspection (use after schema changes)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Scrape but do not POST to the API")
    ap.add_argument("--purge", action="store_true",
                    help="Read current NV DB, drop adult/non-minor facilities, rewrite with replace=True")
    args = ap.parse_args()

    if args.purge:
        purge_adults()
        return

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    if args.full:
        logger.info("--full: ignoring %s", STATE_FILE)
    else:
        logger.info("loaded %d facilities from %s", len(seen), STATE_FILE)

    scraper = NVFacilityScraper()
    facilities, new_ids = scraper.scrape(seen=seen)

    if not facilities:
        logger.info("No new reports since last run")
        return

    total_reports = sum(len(f.get("reports", [])) for f in facilities)
    logger.info("Scraped %d facilities, %d new reports", len(facilities), total_reports)

    if args.dry_run:
        logger.info("--dry-run: skipping API POST")
        return

    if save_to_api(facilities):
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
        logger.info("State saved to %s", STATE_FILE)
    else:
        logger.error("API save failed; state file NOT updated")


if __name__ == "__main__":
    main()
