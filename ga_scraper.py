"""
Georgia RCCL (Residential Child Care Licensing) TRAILS Scraper

Georgia DHS Office of Inspector General publishes residential child care
licensing surveys through the "TRAILS" public portal at
``rcctrails.dhs.ga.gov``. The portal is a classic ASP.NET WebForms app with a
Telerik RadGrid, gated behind a terms-of-use acceptance page.

Flow reverse-engineered from the portal:

  1. GET  /public/publiclanding.aspx        -> terms page (VIEWSTATE + chkAccept)
  2. POST the acceptance -> /Public/PublicFacilitiesSearch.aspx
  3. POST a search per program type (Child Caring Institution, Child Placing
     Agency, etc.) with empty name filters -> RadGrid of facilities
  4. Page through the RadGrid, collecting every FACID
  5. GET  /Public/ViewFacilityDetails.aspx?FACID=...&REQUEST_FROM=PUBLIC
         -> facility info + a grid of surveys
         (EventID, Survey Start/Exit Date, Survey Type, Status, Under Appeal)
  6. GET  /Manage/SurveyShell/ViewSODReport.aspx?EID=<EventID>
         -> the Statement of Deficiencies report (HTML, sometimes a PDF)

Each survey becomes one report; the survey metadata always lands in
``categories`` even when the SOD body cannot be extracted, so nothing is
silently dropped.

OPERATIONAL NOTE — the portal's AWS load balancer blocks datacenter/VPN IP
ranges with a bare ``403 Forbidden`` (no CAPTCHA, no WAF challenge). curl_cffi
browser impersonation does NOT get past it, and neither does a real headless
browser: the block is on IP reputation, not the TLS/JA3 fingerprint. Run this
scraper from a residential IP with any VPN turned OFF. If every request 403s,
that is the cause.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests

try:
    from curl_cffi import requests as cf_requests
    from curl_cffi.requests.exceptions import RequestException as CurlRequestException
    _HAVE_CURL_CFFI = True
except ImportError:
    cf_requests = None
    _HAVE_CURL_CFFI = False

from bs4 import BeautifulSoup

try:
    import pdfplumber
    _HAVE_PDFPLUMBER = True
except ImportError:
    pdfplumber = None
    _HAVE_PDFPLUMBER = False

from inspection_api_client import post_facilities_to_api
from scraper_state import load_state, merge_new_ids, save_state, seen_from_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("KOP_DATA_API_KEY", "CHANGE_ME")
STATE_FILE = Path(os.getenv("GA_STATE_FILE", ".ga_state.json"))

BASE = "https://rcctrails.dhs.ga.gov"
LANDING_URL = f"{BASE}/public/publiclanding.aspx"
SEARCH_URL = f"{BASE}/Public/PublicFacilitiesSearch.aspx"
DETAIL_URL = f"{BASE}/Public/ViewFacilityDetails.aspx"
SOD_URL = f"{BASE}/Manage/SurveyShell/ViewSODReport.aspx"

# Program types offered by the search dropdown (ddlProgType), mapped to the
# numeric option values the portal expects. ASP.NET event validation 500s on
# any posted value that is not one of these registered option values, so the
# label text must never be posted directly.
PROGRAM_TYPES = {
    "Child Caring Institution": "1",
    "Child Placing Agency": "2",
    "Children's Transition Care Center": "3",
    "Maternity Home": "4",
    "Runaway and Homeless Youth Program": "5",
    "Outdoor Child Caring Program": "6",
    "Commercial Sexual Exploitation Recovery Center": "9",
    "Qualified Residential Treatment Program": "10",
}

NETWORK_ERRORS = (requests.RequestException,) + (
    (CurlRequestException,) if _HAVE_CURL_CFFI else ()
)


# ── HTTP session ──────────────────────────────────────────────────────────────


def make_session():
    """Session for the TRAILS portal.

    curl_cffi Chrome impersonation is used when available (it can't defeat the
    IP block, but it is the closest to a real browser and cheap to keep). Falls
    back to plain requests otherwise.
    """
    if _HAVE_CURL_CFFI:
        return cf_requests.Session(impersonate="chrome")
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            )
        }
    )
    return session


def _get(session, url: str, **kwargs) -> "requests.Response":
    kwargs.setdefault("timeout", 60)
    response = session.get(url, **kwargs)
    _raise_if_ip_blocked(response)
    response.raise_for_status()
    return response


def _post(session, url: str, data: Dict[str, str], **kwargs) -> "requests.Response":
    kwargs.setdefault("timeout", 60)
    response = session.post(url, data=data, **kwargs)
    _raise_if_ip_blocked(response)
    response.raise_for_status()
    return response


def _raise_if_ip_blocked(response) -> None:
    if response.status_code == 403 and "awselb" in response.headers.get("server", "").lower():
        raise RuntimeError(
            "TRAILS returned 403 from the AWS load balancer. The portal blocks "
            "datacenter/VPN IP ranges - turn off any VPN and run from a "
            "residential connection."
        )


# ── ASP.NET WebForms helpers ──────────────────────────────────────────────────


def _hidden_fields(soup: BeautifulSoup) -> Dict[str, str]:
    """Collect every hidden <input> (VIEWSTATE, EVENTVALIDATION, ClientState…)."""
    fields: Dict[str, str] = {}
    for inp in soup.select("input[type=hidden]"):
        name = inp.get("name")
        if name:
            fields[name] = inp.get("value", "") or ""
    return fields


def _clean(value: object) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    if not text or text.lower() == "none":
        return ""
    return re.sub(r"\s+", " ", text)


# ── Portal navigation ─────────────────────────────────────────────────────────


def accept_terms(session) -> BeautifulSoup:
    """Accept the terms-of-use gate and return the search page soup."""
    landing = _get(session, LANDING_URL)
    soup = BeautifulSoup(landing.text, "html.parser")
    fields = _hidden_fields(soup)

    # Check the acceptance box and fire the RadButton's postback
    # (__doPostBack('ctl00$ContentPlaceHolder1$btnProceed','')).
    fields["ctl00$ContentPlaceHolder1$chkAccept"] = "on"
    fields["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$btnProceed"
    fields["__EVENTARGUMENT"] = ""

    resp = _post(session, LANDING_URL, fields)
    # The RadButton carries a NavigateUrl to the search page; some server
    # configs redirect, others require a follow-up GET. Normalize by GETting
    # the search page (the session cookie now carries the acceptance flag).
    if "PublicFacilitiesSearch" not in resp.url:
        resp = _get(session, SEARCH_URL)
    return BeautifulSoup(resp.text, "html.parser")


def search_program_type(session, search_soup: BeautifulSoup, program_type: str) -> Tuple[List[Dict[str, str]], BeautifulSoup]:
    """POST a search for one program type; return (facilities, result soup)."""
    fields = _hidden_fields(search_soup)
    prefix = "ctl00$ContentPlaceHolder1$"
    fields[f"{prefix}ddlProgType"] = PROGRAM_TYPES[program_type]
    for f in ("txtFacName", "txtPhone", "txtAddress", "txtCity", "txtCounty", "txtZip"):
        fields[f"{prefix}{f}"] = ""
    # RadButton search trigger.
    fields[f"{prefix}btnSearch_input"] = "Search"
    fields["__EVENTTARGET"] = ""
    fields["__EVENTARGUMENT"] = ""

    resp = _post(session, SEARCH_URL, fields)
    soup = BeautifulSoup(resp.text, "html.parser")
    facilities = _parse_result_grid(soup)
    return facilities, soup


def _parse_result_grid(soup: BeautifulSoup) -> List[Dict[str, str]]:
    grid = soup.find("table", id="ctl00_ContentPlaceHolder1_radFacility_ctl00")
    if not grid:
        return []

    facilities: List[Dict[str, str]] = []
    # Data rows carry 12 cells: FACID, Active Facility (the name), Program
    # Type code, Services Provided, Address, City, State, County, Zip, Email,
    # Active Date, and a trailing "File a Complaint" link.
    for row in grid.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 12:
            continue
        link = row.find("a", href=re.compile(r"FACID=", re.I))
        facid = ""
        if link:
            m = re.search(r"FACID=([A-Za-z0-9]+)", link["href"], re.I)
            if m:
                facid = m.group(1)
        if not facid:
            # First cell usually holds the FACID as text even without a link.
            facid = _clean(cells[0].get_text())
        if not facid or facid.upper() == "FACID":
            continue
        facilities.append(
            {
                "facid": facid,
                "name": _clean(cells[1].get_text()),
                "program_type_code": _clean(cells[2].get_text()),
                "services_provided": _clean(cells[3].get_text()),
                "address": _clean(cells[4].get_text()),
                "city": _clean(cells[5].get_text()),
                "state": _clean(cells[6].get_text()),
                "county": _clean(cells[7].get_text()),
                "zip": _clean(cells[8].get_text()),
                "email": _clean(cells[9].get_text()),
                "active_date": _clean(cells[10].get_text()),
            }
        )
    return facilities


def _next_page_target(soup: BeautifulSoup, current_page: int) -> Optional[str]:
    """Find the __doPostBack target for the next page number in the RadGrid pager."""
    next_label = str(current_page + 1)
    for a in soup.find_all("a", href=re.compile(r"__doPostBack")):
        if a.get_text(strip=True) == next_label:
            m = re.search(r"__doPostBack\('([^']+)'", a["href"])
            if m:
                return m.group(1)
    # The pager window only shows 10 numeric links; past that, advance with the
    # grid's "next page" button. On the final page the postback returns the same
    # rows, which the caller's no-new-facilities guard turns into a stop.
    btn = soup.find("input", class_="rgPageNext")
    if btn and btn.get("name"):
        return btn["name"]
    return None


def collect_facilities(session, search_soup: BeautifulSoup, program_type: str, max_pages: int = 60) -> List[Dict[str, str]]:
    """Search a program type and page through every RadGrid result page."""
    facilities, soup = search_program_type(session, search_soup, program_type)
    for f in facilities:
        f["program_type"] = program_type  # full label; grid only carries a code
    seen_facids = {f["facid"] for f in facilities}
    logger.info("  %s: page 1 -> %d facilities", program_type, len(facilities))

    page = 1
    while page < max_pages:
        target = _next_page_target(soup, page)
        if not target:
            break
        fields = _hidden_fields(soup)
        fields["__EVENTTARGET"] = target
        fields["__EVENTARGUMENT"] = ""
        try:
            resp = _post(session, SEARCH_URL, fields)
        except NETWORK_ERRORS as exc:
            logger.warning("  %s: pagination stopped at page %d: %s", program_type, page, exc)
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        page_facilities = _parse_result_grid(soup)
        new = [f for f in page_facilities if f["facid"] not in seen_facids]
        if not new:
            break
        for f in new:
            seen_facids.add(f["facid"])
        facilities.extend(new)
        for f in new:
            f["program_type"] = program_type
        page += 1
        logger.info("  %s: page %d -> %d facilities (total %d)", program_type, page, len(new), len(facilities))
        time.sleep(0.5)

    return facilities


# ── Facility detail + surveys ─────────────────────────────────────────────────


def parse_facility_detail(session, facid: str) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """Return (facility_info_fields, surveys) for one FACID."""
    resp = _get(session, DETAIL_URL, params={"FACID": facid, "REQUEST_FROM": "PUBLIC"})
    soup = BeautifulSoup(resp.text, "html.parser")

    info: Dict[str, str] = {"facid": facid}
    # Header labels are ASP.NET <span> controls; harvest anything id'd lbl*.
    for span in soup.find_all("span"):
        sid = (span.get("id") or "").lower()
        if "lbl" in sid:
            info[sid.split("_")[-1]] = _clean(span.get_text())

    surveys: List[Dict[str, str]] = []
    grid = soup.find("table", id="ctl00_ContentPlaceHolder1_radFacility_ctl00")
    if grid:
        for row in grid.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
            if len(cells) < 6:
                continue
            event_id = _clean(cells[0])
            if not event_id or event_id.upper() == "EVENTID":
                continue
            surveys.append(
                {
                    "event_id": event_id,
                    "survey_start": _clean(cells[1]),
                    "survey_exit": _clean(cells[2]),
                    "survey_type": _clean(cells[3]),
                    "survey_status": _clean(cells[4]),
                    "under_appeal": _clean(cells[5]),
                }
            )
    return info, surveys


def fetch_sod_report(session, event_id: str) -> str:
    """Fetch the Statement of Deficiencies for an EventID; return extracted text.

    The report page can render as HTML or serve a PDF. Handle both; return "" if
    nothing extractable (survey metadata is still captured by the caller).
    """
    try:
        resp = _get(session, SOD_URL, params={"EID": event_id})
    except NETWORK_ERRORS as exc:
        logger.warning("    SOD fetch failed for %s: %s", event_id, exc)
        return ""

    content_type = resp.headers.get("content-type", "").lower()
    content = resp.content

    if "pdf" in content_type or content[:4] == b"%PDF":
        return _extract_pdf_text(content, event_id)

    soup = BeautifulSoup(resp.text, "html.parser")

    # If the HTML shell just links/embeds a PDF, pull that instead.
    pdf_link = soup.find(["a", "iframe", "embed"], src=re.compile(r"\.pdf", re.I)) or soup.find(
        "a", href=re.compile(r"\.pdf", re.I)
    )
    if pdf_link:
        pdf_href = pdf_link.get("src") or pdf_link.get("href")
        try:
            pdf_resp = _get(session, urljoin(SOD_URL, pdf_href))
            return _extract_pdf_text(pdf_resp.content, event_id)
        except NETWORK_ERRORS as exc:
            logger.warning("    SOD PDF fetch failed for %s: %s", event_id, exc)

    # Otherwise treat the report body as HTML text.
    body = soup.find("form") or soup.body or soup
    return re.sub(r"\n{3,}", "\n\n", body.get_text("\n", strip=True))


def _extract_pdf_text(pdf_bytes: bytes, event_id: str) -> str:
    if not pdf_bytes.startswith(b"%PDF"):
        return ""
    if not _HAVE_PDFPLUMBER:
        logger.warning("    pdfplumber not installed; cannot extract SOD PDF for %s", event_id)
        return ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            parts = [page.extract_text() or "" for page in pdf.pages]
        return "\n\n".join(p.strip() for p in parts if p.strip()).strip()
    except Exception as exc:  # pdfplumber raises a variety of errors on odd PDFs
        logger.warning("    SOD PDF parse failed for %s: %s", event_id, exc)
        return ""


# ── Payload assembly ──────────────────────────────────────────────────────────


def build_facility_payload(
    listing: Dict[str, str],
    info: Dict[str, str],
    reports: List[Dict[str, object]],
) -> Dict[str, object]:
    full_address = " ".join(
        v for v in [listing.get("address"), listing.get("city"), listing.get("state"), listing.get("zip")] if v
    )
    facility_name = listing.get("name") or listing["facid"]

    return {
        "facility_info": {
            "facility_name": facility_name,
            "program_name": listing["facid"],  # FACID is the stable unique key
            "program_category": listing.get("program_type") or listing.get("program_type_code", ""),
            "full_address": full_address,
            "phone": info.get("phone", ""),
            "bed_capacity": info.get("capacity", ""),
            "executive_director": info.get("director", "") or info.get("administrator", ""),
            "license_exp_date": "",
            "relicense_visit_date": "",
            "action": "",
        },
        "reports": reports,
        "source": {
            "facid": listing["facid"],
            "program_type": listing.get("program_type") or listing.get("program_type_code", ""),
            "services_provided": listing.get("services_provided", ""),
            "county": listing.get("county", ""),
            "email": listing.get("email", ""),
            "active_date": listing.get("active_date", ""),
            "detail_url": f"{DETAIL_URL}?FACID={listing['facid']}&REQUEST_FROM=PUBLIC",
        },
    }


def build_report(survey: Dict[str, str], raw_content: str) -> Dict[str, object]:
    report_date = survey.get("survey_exit") or survey.get("survey_start", "")
    summary = f"{survey.get('survey_type', '')} - {survey.get('survey_status', '')}".strip(" -")
    return {
        "report_id": survey["event_id"],
        "report_date": report_date,
        "raw_content": raw_content,
        "content_length": len(raw_content),
        "summary": summary,
        "categories": {
            "event_id": survey["event_id"],
            "survey_type": survey.get("survey_type", ""),
            "survey_status": survey.get("survey_status", ""),
            "survey_start_date": survey.get("survey_start", ""),
            "survey_exit_date": survey.get("survey_exit", ""),
            "under_appeal": survey.get("under_appeal", ""),
            "sod_url": f"{SOD_URL}?EID={survey['event_id']}",
        },
    }


# ── Orchestration ─────────────────────────────────────────────────────────────


def scrape(
    limit: Optional[int] = None,
    program_types: Optional[List[str]] = None,
    seen: Optional[Dict[str, set]] = None,
    fetch_sod: bool = True,
) -> Tuple[List[Dict[str, object]], Dict[str, List[str]]]:
    session = make_session()
    seen = seen or {}
    new_ids: Dict[str, List[str]] = {}
    types = program_types or list(PROGRAM_TYPES)
    unknown = [t for t in types if t not in PROGRAM_TYPES]
    if unknown:
        raise SystemExit(
            f"Unknown program type(s): {unknown}. Valid: {list(PROGRAM_TYPES)}"
        )

    search_soup = accept_terms(session)

    listings: List[Dict[str, str]] = []
    for program_type in types:
        try:
            listings.extend(collect_facilities(session, search_soup, program_type))
        except NETWORK_ERRORS as exc:
            logger.warning("Search failed for %s: %s", program_type, exc)
    # De-dup by FACID (a facility can appear once per type only, but be safe).
    unique = {l["facid"]: l for l in listings}
    logger.info("Collected %d unique facilities across %d program types", len(unique), len(types))

    facilities: List[Dict[str, object]] = []
    for index, (facid, listing) in enumerate(sorted(unique.items()), start=1):
        logger.info("[%d/%d] %s", index, len(unique), facid)
        try:
            info, surveys = parse_facility_detail(session, facid)
        except NETWORK_ERRORS as exc:
            logger.warning("  detail fetch failed for %s: %s", facid, exc)
            continue

        seen_for_fac = seen.get(facid, set())
        new_surveys = [s for s in surveys if s["event_id"] and s["event_id"] not in seen_for_fac]
        if not new_surveys:
            continue

        reports: List[Dict[str, object]] = []
        for survey in new_surveys:
            raw = fetch_sod_report(session, survey["event_id"]) if fetch_sod else ""
            reports.append(build_report(survey, raw))
            time.sleep(0.3)

        facilities.append(build_facility_payload(listing, info, reports))
        new_ids[facid] = [s["event_id"] for s in new_surveys]

        if limit is not None and len(facilities) >= limit:
            logger.info("Reached --limit %d", limit)
            break

    return facilities, new_ids


def save_to_api(facilities: List[Dict[str, object]], api_url: str) -> bool:
    result = post_facilities_to_api(
        api_url=api_url,
        api_key=API_KEY,
        state="GA",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=180,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Georgia RCCL TRAILS public records scraper")
    parser.add_argument("--api-url", default=API_URL, help="Override the inspections write endpoint")
    parser.add_argument("--limit", type=int, help="Only post the first N facilities")
    parser.add_argument("--program-type", action="append", help="Restrict to specific program type(s); repeatable")
    parser.add_argument("--no-sod", action="store_true", help="Skip fetching SOD report bodies (metadata only, fast smoke test)")
    parser.add_argument("--no-post", action="store_true", help="Scrape but do not post to the API")
    parser.add_argument("--full", action="store_true", help="Ignore saved seen-state and reprocess all surveys")
    args = parser.parse_args()

    if not _HAVE_CURL_CFFI:
        logger.warning("curl_cffi not installed; using plain requests. Install it if the portal blocks you: pip install curl_cffi")

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)

    facilities, new_ids = scrape(
        limit=args.limit,
        program_types=args.program_type,
        seen=seen,
        fetch_sod=not args.no_sod,
    )

    logger.info("Scraped %d facilities with new surveys", len(facilities))

    if args.no_post:
        logger.info("--no-post supplied; skipping API write")
        return
    if not facilities:
        logger.info("No new surveys since last run")
        return

    logger.info("Posting %d facilities to the API", len(facilities))
    if save_to_api(facilities, api_url=args.api_url):
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
        logger.info("GA scrape saved successfully")
    else:
        logger.error("API save failed -- state not advanced")


if __name__ == "__main__":
    main()
