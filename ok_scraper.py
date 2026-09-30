"""
Oklahoma residential program and shelter monitoring scraper.

Source: OKDHS Child Care Services, Residential and Child-Placing Agency
Licensing. Classic ASP.NET WebForms on plain HTTP (the facility host does not
answer on HTTPS); plain requests, no login.

  1. The residential locator lists the programs of one type, 30 to a page:
       GET  http://www.publicview.okdhs.org/ResidentialLocator/Default.aspx
       POST the same URL with every hidden input, the program type
            (K85 residential program, K84 shelter) and the search button;
            it redirects to ChildCareFacilities.aspx, which holds the grid.
       Paging posts that page's own hidden inputs back to
       ChildCareFacilities.aspx with __EVENTTARGET=...GridView1 and
       __EVENTARGUMENT=Page$Next (posting it to Default.aspx returns no grid).
  2. Each program's page (plain GET by case number) holds the general
     information, a monitoring summary (every visit with the regulations cited,
     what was observed, the plan to correct and whether it was numerous,
     repeated or serious) and a complaint summary (substantiated complaints
     only; anything rising to abuse or neglect goes to Child Welfare Services
     and is not published there).

One report per visit and one per complaint. The page shows only a rolling 36
months, so a visit that ages out is gone from the source: every fetched page
is saved gzipped to the FileBird Drive folder `ok_html/<case>/<date>.html.gz`
when its content changed, and `--from-saved` rebuilds the payload from those
copies. Nothing is ever deleted from the site.

The state edits entries after a visit (a correction date or plan appears
later), so the state file keeps a content hash per report, as tx_scraper.py
does, and a report is posted again when its hash changes; the write API
updates the row in place.
"""

import argparse
import gzip
import hashlib
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from inspection_api_client import post_facilities_to_api
from kop_paths import report_cache_dir
from scraper_state import load_state, save_state

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
STATE_FILE = Path(os.getenv("OK_STATE_FILE", ".ok_state.json"))

LOCATOR_URL = "http://www.publicview.okdhs.org/ResidentialLocator/Default.aspx"
FACILITY_URL = ("http://residentialchildplacingview.okdhs.org/ResidentialView/"
                "ResidentialView.aspx?CaseNumber={case}")
GRID = "ctl00$ContentPlaceHolder1$GridView1"
PROGRAM_TYPES = {"K85": "Residential Program", "K84": "Shelter Program"}
CASE_NUMBER = re.compile(r"^K8[45]\d{7}$")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 0.7


# ── Fetch layer ──────────────────────────────────────────────────────────────


class OKClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call = 0.0

    def _pause(self) -> None:
        wait = REQUEST_GAP - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """One request with retries on timeouts, connection errors and 5xx."""
        delay = 2.0
        for attempt in range(1, 5):
            self._pause()
            try:
                response = self.session.request(method, url, timeout=90, **kwargs)
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
            response.raise_for_status()
            return response
        raise RuntimeError("unreachable")

    @staticmethod
    def _form_fields(soup: BeautifulSoup) -> Dict[str, str]:
        fields: Dict[str, str] = {}
        for tag in soup.find_all("input"):
            name = tag.get("name")
            kind = (tag.get("type") or "text").lower()
            if not name:
                continue
            if kind in ("hidden", "text"):
                fields[name] = tag.get("value", "")
            elif kind in ("radio", "checkbox") and tag.has_attr("checked"):
                fields[name] = tag.get("value", "on")
        return fields

    def programs(self, program_type: str) -> List[Dict[str, str]]:
        """Every grid row for one program type, following Page$Next."""
        soup = BeautifulSoup(self._request("GET", LOCATOR_URL).text, "html.parser")
        data = self._form_fields(soup)
        data.update({
            "ctl00$ContentPlaceHolder1$rblProgramType": program_type,
            "ctl00$ContentPlaceHolder1$btnSearch": "Search for Child Care",
        })
        response = self._request("POST", LOCATOR_URL, data=data)
        rows: Dict[str, Dict[str, str]] = {}
        for page in range(1, 50):
            soup = BeautifulSoup(response.text, "html.parser")
            grid = soup.find("table", id=re.compile("GridView1"))
            if grid is None:
                raise RuntimeError(f"No results grid for {program_type} on page {page} ({response.url})")
            for row in parse_grid(grid):
                rows.setdefault(row["case_number"], {**row, "program_type": program_type})
            if "Page$Next" not in str(grid):
                break
            data = self._form_fields(soup)
            data.update({"__EVENTTARGET": GRID, "__EVENTARGUMENT": "Page$Next"})
            action = (soup.find("form") or {}).get("action") or response.url
            response = self._request("POST", urljoin(response.url, action), data=data)
        return list(rows.values())

    def facility_page(self, case_number: str) -> str:
        """The program's page. The site now and then answers 200 with an error
        page ("You have encountered an error. Please try again.", 2 of 93 on
        2026-09-30, fine on the next ask); that is asked for again, and raised
        as a request error if it persists so it is never saved or parsed."""
        for attempt in range(4):
            if attempt:
                time.sleep(5 * attempt)
            response = self._request("GET", FACILITY_URL.format(case=case_number))
            response.encoding = response.encoding or "utf-8"
            if is_program_page(response.text):
                return response.text
            logger.warning(f"  {case_number}: the state site answered with an error page; asking again")
        raise requests.RequestException(f"{case_number}: error page after 4 tries")


def is_program_page(html: str) -> bool:
    """A real program page carries its case number span; the error page does not."""
    return 'id="lblCaseNumber"' in html and "You have encountered an error" not in html


def parse_grid(grid) -> List[Dict[str, str]]:
    """Rows of the locator grid; the pager is a nested table and is skipped."""
    out = []
    rows = grid.find_all("tr", recursive=False)
    if not rows and grid.tbody is not None:
        rows = grid.tbody.find_all("tr", recursive=False)
    for tr in rows:
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 8:
            continue
        case_number = cells[0].get_text(strip=True)
        if not CASE_NUMBER.match(case_number):
            continue
        texts = [one_line(c.get_text(" ", strip=True)) for c in cells]
        out.append({
            "case_number": case_number,
            "subtype": texts[1].rstrip(",").strip(),
            "grid_name": texts[2],
            "address": texts[3],
            "city": texts[4],
            "zip": texts[5],
            "phone": texts[6],
            "capacity": texts[7],
        })
    return out


# ── Saved pages ──────────────────────────────────────────────────────────────

# Parts of the page that change on every request (clock, the rolling "since"
# dates, ASP.NET hidden state), left out when deciding whether it changed.
VOLATILE_SPANS = ("lblDate", "lblTime", "lblMontVisit", "lblNewComplaintsSinceDate")


def page_fingerprint(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for span_id in VOLATILE_SPANS:
        tag = soup.find(id=span_id)
        if tag is not None:
            tag.clear()
    for tag in soup.find_all("input", attrs={"type": "hidden"}):
        tag.decompose()
    return hashlib.sha1(str(soup).encode("utf-8")).hexdigest()


class PageStore:
    """Gzipped copies of each program page: <base>/<case>/<YYYY-MM-DD>.html.gz,
    written only when the page's content changed since the last copy.
    ReportStore is PDF-oriented; this is its HTML counterpart."""

    def __init__(self, base: Optional[Path] = None):
        self.base = base or report_cache_dir("OK_HTML_CACHE", "ok_html", Path(__file__).parent / "ok_html")

    def latest(self, case_number: str) -> Optional[Path]:
        folder = self.base / case_number
        try:
            copies = sorted(folder.glob("*.html.gz"))
        except OSError:
            return None
        return copies[-1] if copies else None

    @staticmethod
    def read(path: Path) -> str:
        return gzip.decompress(path.read_bytes()).decode("utf-8")

    def save(self, case_number: str, html: str, fingerprint: str, known: Optional[str]) -> bool:
        """Save unless the last copy has the same fingerprint. `known` is the
        fingerprint the state file remembers, which spares reading Drive. A
        second changed copy on one day replaces that day's copy."""
        if not is_program_page(html):
            raise ValueError(f"{case_number}: not a program page; not saved")
        if known is None:
            last = self.latest(case_number)
            if last is not None:
                try:
                    known = page_fingerprint(self.read(last))
                except (OSError, ValueError) as exc:
                    logger.warning(f"  could not read {last}: {exc}")
        if known == fingerprint:
            return False
        folder = self.base / case_number
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{datetime.now():%Y-%m-%d}.html.gz").write_bytes(
            gzip.compress(html.encode("utf-8"), mtime=0))
        return True


# ── Parsing ──────────────────────────────────────────────────────────────────


def one_line(value: str) -> str:
    value = (value or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def iso_date(value: str) -> str:
    match = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", value or "")
    if not match:
        return ""
    try:
        return datetime(int(match.group(3)), int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def span_text(soup: BeautifulSoup, span_id: str) -> str:
    tag = soup.find(id=span_id)
    return one_line(tag.get_text(" ", strip=True)) if tag else ""


VISIT_COLUMNS = {
    "requirement": "requirement",
    "regulation description": "description",
    "noncompliance observed": "observed",
    "plan to correct": "plan",
    "correction date": "correction_date",
    "nrs": "nrs",
}
COMPLAINT_COLUMNS = {
    "requirement": "requirement",
    "description": "description",
    "allegation description": "observed",
    "plan to correct": "plan",
    "allegation findings": "finding",
}
NO_FINDINGS = re.compile(r"no\s+non-?\s*compliances?\s+observed", re.I)
# A complaint header whose table holds only this (1 of 151 on 2026-09-30):
# nothing to show, so it is counted and not posted.
NO_DATA = re.compile(r"^\s*no\s+data\s+on\s+file\s*$", re.I)


def is_header(tag) -> bool:
    return tag.name == "span" and bool(re.search(r"_(lblVisitDate|lblComplaintReceived)$", tag.get("id") or ""))


def is_grid(tag) -> bool:
    return tag.name == "table" and tag.get("id") in ("gvNewMonitoringVisits", "gvNewComplaints")


def read_grid(table, columns: Dict[str, str]) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """(items, problem). [] for the "No non-compliances observed" table;
    None with a problem when the table is neither."""
    rows = table.find_all("tr", recursive=False)
    header = next((tr for tr in rows if tr.find("th")), None)
    if header is None:
        if NO_FINDINGS.search(table.get_text(" ", strip=True)):
            return [], ""
        if NO_DATA.match(table.get_text(" ", strip=True)):
            return [], "no data on file"
        return None, f"table without a header: {one_line(table.get_text(' ', strip=True))[:80]!r}"
    names = [one_line(th.get_text(" ", strip=True)).lower() for th in header.find_all("th")]
    keys = [columns.get(name) for name in names]
    unknown = [name for name, key in zip(names, keys) if key is None]
    if unknown:
        return None, f"unknown columns {unknown}"
    items = []
    for tr in rows:
        if tr is header:
            continue
        cells = tr.find_all("td", recursive=False)
        if not cells:
            continue
        if len(cells) != len(keys):
            return None, f"row with {len(cells)} cells for {len(keys)} columns"
        item = {key: one_line(cell.get_text(" ", strip=True)) for key, cell in zip(keys, cells)}
        if "correction_date" in item:
            item["correction_date"] = iso_date(item["correction_date"]) or item["correction_date"]
        if "nrs" in item:
            item["nrs"] = item["nrs"].strip().lower() == "yes"
        items.append(item)
    if not items:
        return None, "table with a header and no rows"
    return items, ""


def parse_page(html: str) -> Dict[str, Any]:
    """The program's details, visits and complaints, in page order. Any header
    that did not pair with a table, or table that did not parse, is listed in
    `unparsed` (a clean page has none)."""
    soup = BeautifulSoup(html, "html.parser")
    info = {name: span_text(soup, f"lbl{name}") for name in (
        "CaseNumber", "FacilityName", "Director", "EmailAddress", "FacilityPhoneNumber",
        "CountyOfFacility", "FacilityType", "ProgramSubtype", "WorkerName",
        "RegionPhoneNumber", "Capacity", "ServiceProvided")}
    location = soup.find(id="lblLocation")
    if location is not None:
        lines = [one_line(part) for part in location.get_text("\n").split("\n")]
        info["Location"] = ", ".join(line for line in lines if line)
    else:
        info["Location"] = ""

    visits: List[Dict[str, Any]] = []
    complaints: List[Dict[str, Any]] = []
    unparsed: List[str] = []
    no_data: List[str] = []
    pending: Optional[Dict[str, Any]] = None

    def close_pending(reason: str) -> None:
        nonlocal pending
        if pending is not None:
            unparsed.append(f"{pending['kind']} {pending['date_text']}: {reason}")
            pending = None

    for tag in soup.find_all(lambda t: is_header(t) or is_grid(t)):
        if is_header(tag):
            close_pending("no table after the header")
            span_id = tag["id"]
            if span_id.endswith("_lblVisitDate"):
                prefix = span_id[: -len("_lblVisitDate")]
                pending = {
                    "kind": "visit",
                    "date_text": one_line(tag.get_text()),
                    "visit_type": span_text(soup, f"{prefix}_lblVisitType"),
                    "purpose": span_text(soup, f"{prefix}_lblPurposeOfVisit"),
                }
            else:
                pending = {"kind": "complaint", "date_text": one_line(tag.get_text())}
            continue
        expected = "visit" if tag["id"] == "gvNewMonitoringVisits" else "complaint"
        if pending is None or pending["kind"] != expected:
            close_pending(f"followed by a {expected} table")
            unparsed.append(f"{expected} table with no header")
            continue
        items, problem = read_grid(tag, VISIT_COLUMNS if expected == "visit" else COMPLAINT_COLUMNS)
        if problem == "no data on file":
            no_data.append(f"{expected} {pending['date_text']}")
            pending = None
            continue
        if items is None:
            close_pending(problem)
            continue
        entry = {**pending, "date": iso_date(pending["date_text"]), "items": items}
        if not entry["date"]:
            close_pending("unreadable date")
            continue
        (visits if expected == "visit" else complaints).append(entry)
        pending = None
    close_pending("no table after the header")

    return {
        "info": info,
        "monitoring_since": iso_date(span_text(soup, "lblMontVisit")),
        "complaints_since": iso_date(span_text(soup, "lblNewComplaintsSinceDate")),
        "visits": visits,
        "complaints": complaints,
        "unparsed": unparsed,
        "no_data": no_data,
    }


# ── Reports ──────────────────────────────────────────────────────────────────


def plural(count: int, word: str, many: str = "") -> str:
    return f"{count} {word if count == 1 else (many or word + 's')}"


def flatten(kind: str, heading: str, items: List[Dict[str, Any]]) -> str:
    blocks = [heading]
    if not items and kind == "visit" and not heading.lower().startswith("attempted"):
        blocks.append("No non-compliances observed.")
    for item in items:
        lines = [" ".join(p for p in (item.get("requirement"), item.get("description")) if p)]
        if item.get("observed"):
            lines.append(("Allegation: " if kind == "complaint" else "Observed: ") + item["observed"])
        if item.get("plan"):
            lines.append("Plan to correct: " + item["plan"])
        if item.get("correction_date"):
            lines.append("Correction date: " + item["correction_date"])
        if kind == "visit":
            lines.append("Numerous, repeated or serious: " + ("Yes" if item.get("nrs") else "No"))
        if item.get("finding"):
            lines.append("Finding: " + item["finding"])
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def visit_report(visit: Dict[str, Any], report_id: str, url: str) -> Dict[str, Any]:
    items = visit["items"]
    nrs = sum(1 for item in items if item.get("nrs"))
    visit_type = visit["visit_type"] or "Monitoring"
    label = f"{visit_type} visit" + (f" ({visit['purpose']})" if visit["purpose"] else "")
    if items:
        summary = f"{label}: {plural(len(items), 'non-compliance')}"
        if nrs:
            summary += f", {nrs} numerous, repeated or serious"
    elif visit_type.lower() == "attempted":
        # The visit did not take place; the page shows the no-findings text anyway.
        summary = f"{label}: not carried out"
    else:
        summary = f"{label}: no non-compliances observed"
    categories = {
        "kind": "visit",
        "visit_type": visit["visit_type"],
        "purpose": visit["purpose"],
        "finding": "",
        "items": items,
        "item_count": len(items),
        "nrs_count": nrs,
    }
    text = flatten("visit", f"{label} on {visit['date']}", items)
    return {
        "report_id": report_id,
        "report_date": visit["date"],
        "report_url": url,
        "raw_content": text,
        "content_length": len(text),
        "summary": summary,
        "categories": categories,
    }


def complaint_report(complaint: Dict[str, Any], report_id: str, url: str) -> Dict[str, Any]:
    items = [{**item, "nrs": False} for item in complaint["items"]]
    findings = list(dict.fromkeys(item.get("finding", "") for item in items if item.get("finding")))
    first = items[0].get("requirement", "") if items else ""
    summary = "Substantiated complaint" + (f": {first}" if first else "")
    if len(items) > 1:
        summary += f" and {plural(len(items) - 1, 'more requirement')}"
    categories = {
        "kind": "complaint",
        "visit_type": "",
        "purpose": "",
        "finding": findings[0] if findings else "",
        "findings": findings,
        "items": items,
        "item_count": len(items),
        "nrs_count": 0,
    }
    text = flatten("complaint", f"Complaint received {complaint['date']}", items)
    return {
        "report_id": report_id,
        "report_date": complaint["date"],
        "report_url": url,
        "raw_content": text,
        "content_length": len(text),
        "summary": summary,
        "categories": categories,
    }


def build_reports(parsed: Dict[str, Any], case_number: str) -> List[Dict[str, Any]]:
    url = FACILITY_URL.format(case=case_number)
    reports = []
    used: Counter = Counter()

    def unique(base: str) -> str:
        used[base] += 1
        return base if used[base] == 1 else f"{base}-{used[base]}"

    for visit in parsed["visits"]:
        base = f"visit-{visit['date'].replace('-', '')}-{visit['visit_type'] or 'unknown'}"
        reports.append(visit_report(visit, unique(re.sub(r"[^a-z0-9-]", "", base.lower())), url))
    for complaint in parsed["complaints"]:
        first = complaint["items"][0] if complaint["items"] else {}
        digest = hashlib.sha1(
            (first.get("requirement", "") + "\n" + first.get("observed", "")).encode("utf-8")
        ).hexdigest()[:8]
        reports.append(complaint_report(
            complaint, unique(f"complaint-{complaint['date'].replace('-', '')}-{digest}"), url))
    reports.sort(key=lambda r: r["report_date"], reverse=True)
    return reports


def is_flagged(report: Dict[str, Any]) -> bool:
    categories = report["categories"]
    return categories["kind"] == "complaint" or categories["item_count"] > 0


def report_hash(report: Dict[str, Any]) -> str:
    """Fingerprint of what gets posted for a report, to spot later edits."""
    return hashlib.sha1(json.dumps(report, sort_keys=True).encode("utf-8")).hexdigest()


KEEP_UPPER = {"LLC", "RTC", "PRTF", "DDSD", "II", "III", "IV", "USA", "YMCA", "OKDHS", "OJA"}
SMALL_WORDS = {"of", "and", "the", "for", "at", "in", "on", "a", "an", "to", "by"}


def display_name(name: str) -> str:
    """Title-case names the state wrote in full capitals (about half, e.g.
    "TULSA BOYS HOME"); leave mixed-case names alone."""
    name = one_line(name)
    letters = [c for c in name if c.isalpha()]
    if not letters or any(c.islower() for c in letters):
        return name

    def fix(word: str, first: bool) -> str:
        bare = re.sub(r"[^A-Za-z]", "", word)
        if bare in KEEP_UPPER or len(bare) == 1:
            return word
        lowered = word.lower()
        if not first and lowered in SMALL_WORDS:
            return lowered
        return re.sub(r"(^|[-/(\"])([a-z])", lambda m: m.group(1) + m.group(2).upper(), lowered)

    return " ".join(fix(word, i == 0) for i, word in enumerate(name.split(" ")))


def facility_info(parsed: Dict[str, Any], case_number: str, listed: bool, last_listed: str) -> Dict[str, str]:
    info = parsed["info"]
    kind, subtype = info.get("FacilityType", ""), info.get("ProgramSubtype", "")
    category = f"{kind}: {subtype}" if subtype and subtype.lower() != kind.lower() else (kind or subtype)
    if listed:
        action = "Licensed"
    else:
        action = f"No longer listed (last seen {last_listed})" if last_listed else "No longer listed"
    return {
        "facility_name": display_name(info.get("FacilityName", "")) or case_number,
        "program_name": case_number,
        "program_category": category,
        "full_address": info.get("Location", ""),
        "phone": info.get("FacilityPhoneNumber", ""),
        "bed_capacity": info.get("Capacity", ""),
        "executive_director": info.get("Director", ""),
        "license_exp_date": "",
        "relicense_visit_date": "",
        "action": action,
    }


# ── Scraper ──────────────────────────────────────────────────────────────────


class OKScraper:
    def __init__(self, client: Optional[OKClient] = None, pages: Optional[PageStore] = None):
        self.client = client
        self.pages = pages
        self.stats: Counter = Counter()
        self.values: Dict[str, Counter] = {"visit_type": Counter(), "purpose": Counter(), "finding": Counter()}
        self.unparsed: List[str] = []

    def listing(self) -> Dict[str, Dict[str, str]]:
        rows: Dict[str, Dict[str, str]] = {}
        for program_type, label in PROGRAM_TYPES.items():
            found = self.client.programs(program_type)
            if not found:
                # The locator changed or answered with an error page; carrying on
                # would only visit the programs the state file already knows.
                raise RuntimeError(f"The locator listed no {label} programs; check {LOCATOR_URL} by hand")
            logger.info(f"{label} ({program_type}): {len(found)} listed")
            self.stats[f"listed_{program_type}"] = len(found)
            for row in found:
                rows[row["case_number"]] = row
        return rows

    def take(self, case_number: str, html: str, listed: bool, last_listed: str) -> Optional[Dict[str, Any]]:
        """Parse one page into a facility with every report (none filtered)."""
        parsed = parse_page(html)
        if not parsed["info"].get("FacilityName") and not parsed["visits"] and not parsed["complaints"]:
            self.stats["empty_pages"] += 1
            logger.warning(f"  {case_number}: the page came back empty")
            return None
        self.stats["programs"] += 1
        for problem in parsed["unparsed"]:
            self.unparsed.append(f"{case_number}: {problem}")
        self.stats["no_data_on_file"] += len(parsed["no_data"])
        for visit in parsed["visits"]:
            self.stats["visits"] += 1
            self.stats["visits_with_items"] += bool(visit["items"])
            self.stats["visit_items"] += len(visit["items"])
            self.stats["nrs_items"] += sum(1 for item in visit["items"] if item.get("nrs"))
            self.values["visit_type"][visit["visit_type"] or "(empty)"] += 1
            self.values["purpose"][visit["purpose"] or "(empty)"] += 1
        for complaint in parsed["complaints"]:
            self.stats["complaints"] += 1
            self.stats["complaint_items"] += len(complaint["items"])
            for item in complaint["items"]:
                self.values["finding"][item.get("finding") or "(empty)"] += 1
        return {
            "facility_info": facility_info(parsed, case_number, listed, last_listed),
            "reports": build_reports(parsed, case_number),
        }

    def scrape_live(self, known: Dict[str, Dict], page_hashes: Dict[str, str], limit: int = 0,
                    only: Optional[set] = None) -> Tuple[List[Dict], Dict[str, Dict], Dict[str, str]]:
        """Fetch, save and parse every listed program plus every one the state
        file knows. Returns (facilities with all their reports, the program
        registry, page fingerprints)."""
        today = datetime.now().strftime("%Y-%m-%d")
        listed = self.listing()
        registry: Dict[str, Dict] = {}
        for case_number, row in listed.items():
            registry[case_number] = {**known.get(case_number, {}), **row, "last_listed": today}
        for case_number, record in known.items():
            registry.setdefault(case_number, dict(record))
        cases = sorted(registry)
        if only:
            cases = [c for c in cases if c in only]
        if limit:
            cases = cases[:limit]

        facilities: List[Dict] = []
        fingerprints: Dict[str, str] = {}
        for index, case_number in enumerate(cases, start=1):
            record = registry[case_number]
            is_listed = case_number in listed
            logger.info(f"[{index}/{len(cases)}] {case_number} {record.get('grid_name', '')}"
                        + ("" if is_listed else " [no longer listed]"))
            try:
                html = self.client.facility_page(case_number)
            except requests.RequestException as exc:
                logger.error(f"  page failed: {exc}")
                self.stats["page_errors"] += 1
                continue
            fingerprint = page_fingerprint(html)
            fingerprints[case_number] = fingerprint
            if self.pages is not None:
                try:
                    if self.pages.save(case_number, html, fingerprint, page_hashes.get(case_number)):
                        self.stats["pages_saved"] += 1
                except (OSError, ValueError) as exc:
                    logger.error(f"  could not save the page copy: {exc}")
                    fingerprints.pop(case_number, None)
            facility = self.take(case_number, html, is_listed, record.get("last_listed", ""))
            if facility:
                facilities.append(facility)
        return facilities, registry, fingerprints

    def scrape_saved(self, base: Path, known: Dict[str, Dict], limit: int = 0,
                     only: Optional[set] = None) -> List[Dict]:
        """Rebuild from the newest saved copy of each program's page. A program
        counts as listed when it was on the state's list at the latest run."""
        newest_listing = max((r.get("last_listed", "") for r in known.values()), default="")
        cases = sorted(p.name for p in base.iterdir() if p.is_dir() and CASE_NUMBER.match(p.name))
        if only:
            cases = [c for c in cases if c in only]
        if limit:
            cases = cases[:limit]
        store = PageStore(base)
        facilities = []
        for case_number in cases:
            latest = store.latest(case_number)
            if latest is None:
                continue
            last_listed = known.get(case_number, {}).get("last_listed", "")
            is_listed = bool(newest_listing) and last_listed == newest_listing
            html = store.read(latest)
            if not is_program_page(html):
                logger.warning(f"  {case_number}: {latest.name} is not a program page; skipped")
                self.stats["empty_pages"] += 1
                continue
            facility = self.take(case_number, html, is_listed, last_listed)
            if facility:
                facilities.append(facility)
        return facilities

    def print_stats(self, facilities: List[Dict], posted: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        logger.info("── Oklahoma run summary ──")
        for program_type in PROGRAM_TYPES:
            if f"listed_{program_type}" in self.stats:
                logger.info(f"listed {program_type}: {self.stats[f'listed_{program_type}']}")
        logger.info(f"programs parsed: {self.stats['programs']} (empty pages: {self.stats['empty_pages']}, "
                    f"page errors: {self.stats['page_errors']}, page copies saved: {self.stats['pages_saved']})")
        logger.info(f"visits: {self.stats['visits']}, with non-compliances: {self.stats['visits_with_items']}, "
                    f"items: {self.stats['visit_items']}, NRS items: {self.stats['nrs_items']}")
        logger.info(f"complaints: {self.stats['complaints']} ({self.stats['complaint_items']} items; "
                    f"'No data on file', not posted: {self.stats['no_data_on_file']})")
        logger.info(f"reports: {len(reports)} (flagged: {sum(1 for r in reports if is_flagged(r))})")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        for name, counter in self.values.items():
            logger.info(f"{name} values: {dict(counter.most_common())}")
        logger.info(f"unparsed blocks: {len(self.unparsed)}")
        for problem in self.unparsed:
            logger.warning(f"  {problem}")
        logger.info(f"to post: {len(posted)} facilities, {sum(len(f['reports']) for f in posted)} new or changed reports")


def select_changed(facilities: List[Dict], hashes: Dict[str, Dict[str, str]],
                   info_hashes: Dict[str, str]) -> Tuple[List[Dict], Dict[str, Dict[str, str]], Dict[str, str]]:
    """Keep reports that are new or changed, and facilities whose details
    changed even with no report to post. Returns (to post, their report
    hashes, their info hashes)."""
    posted, new_hashes, new_info = [], {}, {}
    for facility in facilities:
        case_number = facility["facility_info"]["program_name"]
        known = hashes.get(case_number, {})
        changed = [r for r in facility["reports"] if known.get(r["report_id"]) != report_hash(r)]
        info_hash = hashlib.sha1(json.dumps(facility["facility_info"], sort_keys=True).encode("utf-8")).hexdigest()
        if not changed and info_hashes.get(case_number) == info_hash:
            continue
        gone = sorted(set(known) - {r["report_id"] for r in facility["reports"]})
        if gone:
            logger.info(f"  {case_number}: {len(gone)} reports no longer shown by the state (kept on the site)")
        posted.append({"facility_info": facility["facility_info"], "reports": changed})
        new_hashes[case_number] = {r["report_id"]: report_hash(r) for r in changed}
        new_info[case_number] = info_hash
    return posted, new_hashes, new_info


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = [{
        "facility_info": f["facility_info"],
        "reports": [{**r, "is_structured": True} for r in f["reports"]],
    } for f in facilities]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "OK",
        "scraped_timestamp": timestamp,
        "scraping_notes": {"total_reports": sum(len(f["reports"]) for f in shaped)},
        "facilities": shaped,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"Wrote {path}")


def save_to_api(facilities: List[Dict], timestamp: str) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="OK",
        scraped_timestamp=timestamp,
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Oklahoma residential program and shelter monitoring")
    parser.add_argument("--full", action="store_true", help=f"Ignore the report hashes in {STATE_FILE} and post everything")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N programs (by case number)")
    parser.add_argument("--case", action="append", default=[], help="Only this case number (repeatable)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return (every report) to this JSON file")
    parser.add_argument("--from-saved", type=Path, metavar="DIR",
                        help="Rebuild from the saved page copies in DIR (the ok_html folder); no state site requests")
    args = parser.parse_args()

    state = load_state(STATE_FILE)
    timestamp = datetime.now().isoformat(timespec="seconds")
    only = set(args.case) or None
    scraper = OKScraper()

    if args.from_saved:
        facilities = scraper.scrape_saved(args.from_saved, state.get("programs", {}), args.limit, only)
    else:
        scraper.client = OKClient()
        scraper.pages = PageStore()
        logger.info(f"Page copies go to {scraper.pages.base}")
        facilities, registry, fingerprints = scraper.scrape_live(
            state.get("programs", {}), state.get("pages", {}), args.limit, only)
        # Not tied to a post: the registry remembers case numbers, the page
        # fingerprints what was last saved.
        state["programs"] = registry
        state.setdefault("pages", {}).update(fingerprints)
        save_state(STATE_FILE, state)

    hashes = {} if args.full else state.get("hashes", {})
    info_hashes = {} if args.full else state.get("info", {})
    posted, new_hashes, new_info = select_changed(facilities, hashes, info_hashes)
    scraper.print_stats(facilities, posted)

    if args.out:
        write_out(args.out, facilities, timestamp)
    if not posted:
        logger.info("No new or changed reports since last run")
        return
    if args.no_post:
        logger.info("Skipping API POST because --no-post was set; report hashes not advanced")
        return
    if save_to_api(posted, timestamp):
        stored = state.setdefault("hashes", {})
        for case_number, by_id in new_hashes.items():
            stored.setdefault(case_number, {}).update(by_id)
        state.setdefault("info", {}).update(new_info)
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- report hashes not advanced")


if __name__ == "__main__":
    main()
