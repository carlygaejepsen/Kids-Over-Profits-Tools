"""
Ohio residential agency compliance report scraper.

Source: the Department of Children and Youth agency search,
https://odjfs2.my.site.com/FindFosterCareAdoptionAgencies/s/
A Salesforce Experience Cloud (Aura) site; its pages call the Apex class
DCYAgencySearchHelper anonymously and the scraper calls it directly:

  getFosterCareAdoptionAgenciesForMapView  {"selectedFilters": "<json>"}  agencies and their facilities
  getAgencyDetails                         {"agencyId": "500533"}         one agency with facilities[]
  getAgencyComplianceReports               {"agencyId": "500533"}         [{fileName, fileURL}]

The Aura context (fwuid and the app's loaded hash) changes when Salesforce
deploys, so it is read from the search page on every run and read again when
a call is refused.

Scope: agencies that operate at least one residential facility (a row with
isFacility true in the list). Foster-only and adoption-only agencies are out.

Reports belong to the agency, not to one of its facilities, so there is one
facility row per agency (program_name = its OFCLA number) and the agency's
facilities travel in every report's categories.facilities.

A report is one review number (AR-00001359). The state posts it as a main PDF
and, often, an "Additional Findings" PDF; both are read into the one report
and both are archived to the FileBird Drive folder `oh_pdfs`
(<review>.pdf, <review>-additional.pdf).

A fileURL is a Salesforce content-delivery page, not a PDF. The download asks
that page's own viewer for the version id and then for the file (see
OHClient.pdf).

Only reports dated 2025-07-01 or later are online. Only current agencies are
listed; the state file keeps every agency id seen, and nothing is deleted on
our side.
"""

import argparse
import json
import logging
import os
import re
import time
import urllib.parse
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pdfplumber
import requests

from inspection_api_client import post_facilities_to_api
from report_store import EXTRACT_CACHE_ROOT, ReportStore, extract_with_cache
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
STATE_FILE = Path(os.getenv("OH_STATE_FILE", ".oh_state.json"))
REPORTS = ReportStore("OH_PDF_CACHE", "oh_pdfs", Path(__file__).parent / "oh_pdfs")
# Agency details and report lists, kept for a few hours so an interrupted run
# picks up where it stopped without asking the state again.
API_CACHE_DIR = EXTRACT_CACHE_ROOT / "oh_api"
API_CACHE_HOURS = float(os.getenv("OH_API_CACHE_HOURS", "12"))

SITE = "https://odjfs2.my.site.com/FindFosterCareAdoptionAgencies/s/"
PAGE_URI = "/FindFosterCareAdoptionAgencies/s/"
AURA_URL = SITE + "sfsites/aura?r=1&aura.ApexAction.execute=1"
COMMUNITY_APP = "siteforce:communityApp"
CONTENT_APP = "forceContent:contentDistributionApp"
CONTROLLER = "DCYAgencySearchHelper"
CONTENT_ACTION = (
    "serviceComponent://ui.content.components.forceContent.contentDistributionViewer."
    "ContentDistributionViewerController/ACTION$getContentDistributionInfo"
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = float(os.getenv("OH_REQUEST_GAP", "1.0"))

MAX_ATTEMPTS = 6   # backoff 3, 6, 12, 24, 48 s: rides out a short outage

SEEN_ADDITIONAL = "+additional"   # state marker: the report was posted with its additional file


class AuraContextError(Exception):
    """The Aura call was refused (stale fwuid, or a page that is not JSON)."""


# ── Fetch layer ──────────────────────────────────────────────────────────────


class OHClient:
    def __init__(self) -> None:
        self.session = self._new_session()
        self._last_call = 0.0
        self._context: Optional[Dict] = None
        self._content_context: Dict[str, Dict] = {}
        self.requests_made = 0

    @staticmethod
    def _new_session() -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        return session

    def _pause(self) -> None:
        wait = REQUEST_GAP - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
        """One request, with retries on timeouts, connection errors, 429 and 5xx."""
        delay = 3.0
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._pause()
            self.requests_made += 1
            try:
                response = session.request(method, url, timeout=120, **kwargs)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                if attempt == MAX_ATTEMPTS:
                    raise
                logger.warning(f"  {exc.__class__.__name__}; retrying in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue
            if (response.status_code >= 500 or response.status_code == 429) and attempt < MAX_ATTEMPTS:
                logger.warning(f"  HTTP {response.status_code}; retrying in {delay:.0f}s")
                time.sleep(delay)
                delay *= 2
                continue
            return response
        raise RuntimeError("unreachable")

    # -- the agency search --

    @staticmethod
    def _read_context(html: str, app: str) -> Dict:
        text = urllib.parse.unquote(html)
        fwuid = re.search(r'"fwuid":"([^"]+)"', text)
        loaded = re.search(r'"APPLICATION@markup://' + re.escape(app) + r'":"([^"]+)"', text)
        if not fwuid or not loaded:
            raise RuntimeError(
                f"Could not read the Aura context (fwuid / {app} hash) from the page. "
                "The site's markup may have changed: open it with the browser's network "
                "tab and compare an 'aura' request's aura.context with what this reads."
            )
        return {
            "mode": "PROD",
            "fwuid": fwuid.group(1),
            "app": app,
            "loaded": {f"APPLICATION@markup://{app}": loaded.group(1)},
            "dn": [],
            "globals": {},
            "uad": True,
        }

    def context(self, refresh: bool = False) -> Dict:
        if self._context is None or refresh:
            response = self._request(self.session, "GET", SITE)
            response.raise_for_status()
            self._context = self._read_context(response.text, COMMUNITY_APP)
        return self._context

    def _execute(self, method: str, params: Optional[Dict]) -> Any:
        action: Dict[str, Any] = {
            "namespace": "",
            "classname": CONTROLLER,
            "method": method,
            "cacheable": False,
            "isContinuation": False,
        }
        if params is not None:
            action["params"] = params
        message = {"actions": [{
            "id": "1;a",
            "descriptor": "aura://ApexActionController/ACTION$execute",
            "callingDescriptor": "UNKNOWN",
            "params": action,
        }]}
        response = self._request(self.session, "POST", AURA_URL, data={
            "message": json.dumps(message),
            "aura.context": json.dumps(self.context()),
            "aura.pageURI": PAGE_URI,
            "aura.token": "null",
        })
        try:
            data = response.json()
            first = data["actions"][0]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            # A stale fwuid is answered with "*/{"event":{"descriptor":
            # "markup://aura:clientOutOfSync" ...", which is not JSON.
            raise AuraContextError(f"HTTP {response.status_code}: {response.text[:160]!r}") from exc
        if first.get("state") != "SUCCESS":
            raise AuraContextError(f"{method}: {first.get('state')} {json.dumps(first.get('error'))[:300]}")
        return (first.get("returnValue") or {}).get("returnValue")

    def apex(self, method: str, params: Optional[Dict] = None) -> Any:
        try:
            return self._execute(method, params)
        except AuraContextError as exc:
            logger.warning(f"Aura call refused ({exc}); reading the context from the page again")
            self.context(refresh=True)
            return self._execute(method, params)

    def agency_rows(self) -> List[Dict]:
        # selectedFilters is a JSON *string* with all seven keys, or the call
        # succeeds with nothing in it.
        filters = json.dumps({
            "selectedAgencyTypes": [],
            "selectedFacilityType": "",
            "selectedCategory": "",
            "selectedCounty": "",
            "selectedAgencyName": "",
            "selectedAgencyZip": "",
            "selectedRadius": "",
        })
        value = self.apex("getFosterCareAdoptionAgenciesForMapView", {"selectedFilters": filters})
        return (value or {}).get("agencyList") or []

    def details(self, agency_number: str) -> Optional[Dict]:
        return self.apex("getAgencyDetails", {"agencyId": agency_digits(agency_number)})

    def report_files(self, agency_number: str) -> List[Dict]:
        return self.apex("getAgencyComplianceReports", {"agencyId": agency_digits(agency_number)}) or []

    # -- one report file --

    def _download(self, file_url: str) -> bytes:
        match = re.match(r"(https://[^/]+)/sfc/p/([^/]+)(/a/[^/]+/[^/?#]+)", file_url)
        if not match:
            raise ValueError(f"not a content-delivery link: {file_url}")
        host, org, suffix = match.groups()
        session = self._new_session()
        referer = {"Referer": host + "/sfc/p/"}

        # 1. The link itself (sets the delivery cookies).
        self._request(session, "GET", file_url).raise_for_status()
        # 2. The page the link redirects to in a browser: recordId and orgId.
        page = self._request(session, "POST", host + "/sfc/p/",
                             data={"compositePageName": org + suffix},
                             headers={"Referer": file_url, "Origin": host})
        page.raise_for_status()
        record = re.search(r"recordId:'([^']+)'", page.text)
        org_id = re.search(r"orgId:'([^']+)'", page.text)
        if not record or not org_id:
            raise ValueError("no recordId/orgId on the delivery page")

        # 3. The viewer app's own Aura context (kept for the run), then its
        #    getContentDistributionInfo action: versionId and viewId.
        base = f"{host}/sfc/ld/{org}{suffix}"
        info = None
        for attempt in range(2):
            context = self._content_context.get(host)
            if context is None:
                app = self._request(
                    session, "GET",
                    base + "/forceContent/contentDistributionApp.app?aura.format=JSON&aura.formatAdapter=LIGHTNING_OUT",
                    headers=referer)
                app.raise_for_status()
                context = self._read_context(app.text, CONTENT_APP)
                self._content_context[host] = context
            message = {"actions": [{
                "id": "7;a",
                "descriptor": CONTENT_ACTION,
                "callingDescriptor": "UNKNOWN",
                "params": {"recordId": record.group(1), "isInternalView": "", "dpt": ""},
            }]}
            answer = self._request(
                session, "POST",
                base + "/aura?r=0&ui-content-components-forceContent-contentDistributionViewer"
                       ".ContentDistributionViewer.getContentDistributionInfo=1",
                data={
                    "message": json.dumps(message, separators=(",", ":")),
                    "aura.context": json.dumps(context, separators=(",", ":")),
                    "aura.pageURI": f"/sfc/p/#{org}{suffix}",
                    "aura.token": "null",
                },
                headers=referer)
            try:
                first = answer.json()["actions"][0]
                if first.get("state") != "SUCCESS":
                    raise ValueError(str(first.get("error"))[:200])
                info = first["returnValue"]
                break
            except (ValueError, KeyError, IndexError, TypeError) as exc:
                self._content_context.pop(host, None)   # stale context: read it again once
                if attempt:
                    raise ValueError(f"viewer call refused: {exc}") from exc
        if not info or not info.get("versionId"):
            raise ValueError("no versionId for the file")

        # 4. The file.
        url = (f"{host}/sfc/dist/version/download/?oid={org_id.group(1)}&ids={info['versionId']}"
               f"&d={urllib.parse.quote(suffix, safe='')}&operationContext=DELIVERY"
               f"&viewId={info.get('viewId') or ''}&dpt=")
        pdf = self._request(session, "GET", url, headers=referer)
        pdf.raise_for_status()
        if not pdf.content.startswith(b"%PDF"):
            raise ValueError(f"not a PDF ({pdf.content[:12]!r})")
        return pdf.content

    def pdf(self, file_url: str) -> Optional[bytes]:
        problem = ""
        for attempt in range(3):
            if attempt:
                time.sleep(5 * attempt)
            try:
                return self._download(file_url)
            except (requests.RequestException, ValueError) as exc:
                problem = str(exc)
        logger.warning(f"  download failed ({problem}); skipped for this run: {file_url}")
        return None


def agency_digits(agency_number: str) -> str:
    """'OFCLA-500533' -> '500533' (leading zeros kept: 'OFCLA-0128' -> '0128')."""
    return re.sub(r"^\D+", "", agency_number or "")


# ── PDF extraction ───────────────────────────────────────────────────────────


def extract_pdf(path: Path) -> Dict:
    """The text, pages joined in order. The tables in these reports have no
    ruling lines; their rows come out whole in reading order (the counts sit at
    the end of a question's first line), so the text is what the parser reads."""
    pages: List[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    return {"text": "\n".join(pages).strip(), "pages": len(pages)}


# ── Parsing ──────────────────────────────────────────────────────────────────

FILE_NAME = re.compile(
    r"^Compliance Report\s*-\s*(Full|Focused|Other) Review\s*\((AR-\d+)\)\s*(-\s*Additional Findings)?\s*\.pdf$",
    re.I,
)
REVIEW_LINE = re.compile(r"(?m)^\s*(Full|Focused|Other) Review\s*-\s*Review Number\s*(AR-\d+)\s*$")
LONG_DATE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),?\s+(\d{4})\b"
)
MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"], start=1)}

PAGE_LINE = re.compile(r"^Page \d+ of \d+$")
COUNTS_END = re.compile(r"^(.*?)\s*\b(\d+)\s+(\d+)\s+(\d+)\s+(\d+)(?:\s+(\d+(?:\.\d+)?)\s*%)?$")
QUESTION_COUNTS = re.compile(r"^Question\s*:-\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)(?:\s+(\d+(?:\.\d+)?)\s*%)?$")
QUESTION_TA = re.compile(r"^Question\s*:-\s*(\d+)$")
TOOL_HEAD = re.compile(r"^(.+?)\s+Y\s+N\s+T/A\s+N/A\s+%\s*Compliance$")
TOOL_TA = re.compile(r"^(.+?)\s+TA$")
RECORD = re.compile(r"^Record\s+(\d+)\b\s*(.*)$")
# An Ohio Administrative Code rule: 5180:2-9-42(B)(9), 5180:2-5-09.1, with the
# stray spaces the reports carry ("5180:2-09- 12(B)(6)", "5180:2-5-20 (K)(5)").
RULE = re.compile(r"\b\d{4}:\s?\d+-\s?\d+-\s?\d+(?:\.\d+)?(?:\s?\((?:[A-Za-z0-9]+|\([A-Za-z0-9]+\))+\))*")

RESIDENTIAL_TOOL = re.compile(r"residential", re.I)


def iso_long_date(value: str, last: bool = False) -> str:
    """The first (or last) 'September 24, 2025' in `value` as 2025-09-24."""
    found = LONG_DATE.findall(value or "")
    if not found:
        return ""
    month, day, year = found[-1] if last else found[0]
    try:
        return datetime(int(year), MONTHS[month], int(day)).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def one_line(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace(" ", " ")).strip()


def header_field(text: str, label: str) -> str:
    match = re.search(rf"(?m)^\s*{label}\s*:[ \t]*(.*)$", text)
    return one_line(match.group(1)) if match else ""


def split_rule(question: str) -> Tuple[str, str]:
    """('32. If due ..., is there documentation ... in compliance with:', '5180:2-9-42(B)(9)')."""
    question = one_line(question)
    rules = [re.sub(r"\s+", "", m.group(0)) for m in RULE.finditer(question)]
    # Take the citations off the end of the question; ones inside it stay.
    stripped = question
    while True:
        tail = None
        for match in RULE.finditer(stripped):
            tail = match
        if not tail or stripped[tail.end():].strip(" ;,.") != "":
            break
        stripped = stripped[:tail.start()].rstrip(" ;,")
        stripped = re.sub(r"\s+(?:and|&)$", "", stripped)
    return (stripped or question), ", ".join(dict.fromkeys(rules))


def number_or_none(value: Optional[str]) -> Optional[float]:
    return float(value) if value not in (None, "") else None


def parse_document(text: str) -> Dict:
    """One PDF (a main file or an Additional Findings file) into its parts."""
    lines = [one_line(line) for line in text.split("\n")]
    lines = [line for line in lines if line and not PAGE_LINE.match(line)]

    review = REVIEW_LINE.search(text)
    universe = header_field(text, "UNIVERSE PERIOD")
    doc: Dict[str, Any] = {
        "review_type": review.group(1).title() if review else "",
        "review_number": review.group(2) if review else "",
        "agency": header_field(text, "AGENCY NAME") or header_field(text, "AGENCY"),
        "universe_period": universe,
        "universe_end": iso_long_date(universe, last=True),
        "generated": iso_long_date(header_field(text, "DATE REPORT GENERATED")),
        "specialist": header_field(text, "LICENSING SPECIALIST"),
        "tools": [],          # [{tool, records}] from the record list on the first pages
        "findings": [],       # noncompliance summaries (main file)
        "assistance": [],     # technical assistance summary
        "table_rows": [],     # every row of the "Compliance Summary for <tool>" tables
        "sections": [],
    }

    mode = "records"
    tool = ""
    item: Optional[Dict] = None      # the finding or assistance item being read
    record: Optional[Dict] = None
    part = ""                        # 'question' | 'reason' | 'comment'
    row: Optional[Dict] = None       # the table row being read
    record_tool: Optional[Dict] = None

    def close_row() -> None:
        nonlocal row
        if row is not None:
            row["question"], row["rule"] = split_rule(row["question"])
            doc["table_rows"].append(row)
            row = None

    for line in lines:
        # -- section switches --
        if re.match(r"^Summary of Findings of Noncompliance\s*\W\s*CAP Needed$", line, re.I):
            close_row(); mode, tool, item, record = "cap", "", None, None
            doc["sections"].append("cap"); continue
        if re.match(r"^Summary of Findings of Noncompliance\s*\W\s*CAP Not Needed$", line, re.I):
            close_row(); mode, tool, item, record = "nocap", "", None, None
            doc["sections"].append("nocap"); continue
        if re.match(r"^Summary of Noncompliance\b", line, re.I):
            close_row(); mode, item, record = "header", None, None
            continue
        if re.match(r"^Summary of Technical Assistance$", line, re.I):
            # The heading comes twice: as the page title and above the items.
            close_row()
            if mode == "ta_header":
                mode, tool = "ta", ""
                doc["sections"].append("ta")
            else:
                mode = "ta_header"
            item, record = None, None
            continue
        table = re.match(r"^Compliance Summary for (.+)$", line, re.I)
        if table:
            close_row(); mode, tool, item, record = "table", one_line(table.group(1)), None, None
            doc["sections"].append("table:" + tool); continue
        if line == "Signatures":
            close_row(); mode, item, record = "header", None, None
            continue

        if mode == "records":
            if RECORD.match(line) or line == "[REDACTED]":
                if record_tool is not None:
                    record_tool["records"] += 1
                continue
            if (line == "Agency Records" or REVIEW_LINE.match(line)
                    or re.match(r"^(AGENCY|UNIVERSE PERIOD)\s*:", line)):
                continue
            record_tool = {"tool": line, "records": 0}
            doc["tools"].append(record_tool)
            continue

        if mode in ("cap", "nocap", "ta"):
            head = TOOL_HEAD.match(line) if mode != "ta" else TOOL_TA.match(line)
            if head and part != "comment_open":
                tool, item, record, part = one_line(head.group(1)), None, None, ""
                continue
            counts = QUESTION_COUNTS.match(line) if mode != "ta" else QUESTION_TA.match(line)
            if counts:
                if mode == "ta":
                    item = {"tool": tool, "question": "", "count": int(counts.group(1)), "records": []}
                    doc["assistance"].append(item)
                else:
                    y, n, ta, na = (int(counts.group(i)) for i in range(1, 5))
                    item = {
                        "tool": tool, "question": "", "y_count": y, "n_count": n,
                        "ta_count": ta, "na_count": na, "reviewed": y + n + ta,
                        "compliance_pct": number_or_none(counts.group(5)),
                        "cap_needed": mode == "cap", "records": [],
                    }
                    doc["findings"].append(item)
                record, part = None, "question"
                continue
            if item is None:
                continue
            rec = RECORD.match(line)
            if rec and (part in ("question", "comment", "reason") ) and not rec.group(2):
                record = {"record": int(rec.group(1)), "reason": "", "comment": ""}
                item["records"].append(record)
                part = "record"
                continue
            reason = re.match(r"^Reason\s*:-\s*(.*)$", line)
            if reason and record is not None:
                record["reason"] = reason.group(1)
                part = "reason"
                continue
            comment = re.match(r"^Comments?\s*:-\s*(.*)$", line)
            if comment and record is not None:
                record["comment"] = comment.group(1)
                part = "comment"
                continue
            if part == "question":
                item["question"] = (item["question"] + " " + line).strip()
            elif part == "reason" and record is not None:
                record["reason"] = (record["reason"] + " " + line).strip()
            elif part == "comment" and record is not None:
                record["comment"] = (record["comment"] + " " + line).strip()
            continue

        if mode == "table":
            if (REVIEW_LINE.match(line) or re.match(r"^(AGENCY|UNIVERSE PERIOD|DATE REPORT GENERATED|LICENSING SPECIALIST)\s*:", line)
                    or re.match(r"^Summary of ", line) or re.match(r"^Question\s+Y\s+N\s+T/A\s+N/A", line)):
                continue
            counts = COUNTS_END.match(line)
            if counts and counts.group(1):
                close_row()
                y, n, ta, na = (int(counts.group(i)) for i in range(2, 6))
                row = {
                    "tool": tool, "question": counts.group(1), "y_count": y, "n_count": n,
                    "ta_count": ta, "na_count": na, "reviewed": y + n + ta,
                    "compliance_pct": number_or_none(counts.group(6)),
                }
                continue
            if row is not None:
                row["question"] += " " + line
            continue
    close_row()

    for finding in doc["findings"]:
        finding["question"], finding["rule"] = split_rule(finding["question"])
    for entry in doc["assistance"]:
        entry["question"], entry["rule"] = split_rule(entry["question"])
    return doc


def is_stub(doc: Dict) -> bool:
    """A file with a header and nothing else (the usual Other Review main file)."""
    return not doc["sections"] and not doc["tools"]


def is_residential(tool: str, rule: str) -> bool:
    """A finding from a residential tool (Child in Residential, On-Site
    Residential, ...) or one citing chapter 2-9, the residential facility rules."""
    if RESIDENTIAL_TOOL.search(tool or ""):
        return True
    rules = [r for r in (rule or "").split(", ") if r]
    return bool(rules) and all(re.match(r"^\d{4}:2-0?9-", r) for r in rules)


# Things that must not be public. The state redacts record names itself
# ("Record 1 [REDACTED]"); a report whose text still trips one of these is
# held back and listed for the owner.
PRIVACY_CHECKS = [
    ("date of birth", re.compile(
        r"(?i)\b(?:D\.?O\.?B\.?|date of birth|birth\s?date|born(?: on)?)\b\W{0,8}"
        r"(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|(?:January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+\d{1,2},?\s+\d{4})")),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("record number", re.compile(
        r"(?i)\b(?:SACWIS|person|client|case|intake|medicaid|youth|child)\s*(?:ID|I\.D\.|#|no\.?|number)\s*[:#]?\s*\d{5,}")),
    ("named child", re.compile(
        r"\b(?:youth|child|resident|client|minor|teen|foster child|foster youth)\s*,?\s+"
        r"(?:named\s+|known as\s+)?(?!The\b|In\b|On\b|At\b)[A-Z][a-z]+\s+[A-Z][a-z]+\b(?!\s+(?:Tool|Questions|Services|County))")),
]


def privacy_hits(text: str) -> List[str]:
    hits = []
    for label, pattern in PRIVACY_CHECKS:
        match = pattern.search(text)
        if match:
            start = max(0, match.start() - 40)
            hits.append(f"{label}: ...{one_line(text[start:match.end() + 40])}...")
    return hits


# ── Names ────────────────────────────────────────────────────────────────────


def clean_address(value: str) -> str:
    return re.sub(r"\s+,", ",", one_line(value))


def format_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return value or ""


# ── Scraper ──────────────────────────────────────────────────────────────────


class OHScraper:
    def __init__(self, client: Optional[OHClient] = None, reports: ReportStore = REPORTS,
                 refresh: bool = False):
        self.client = client or OHClient()
        self.reports = reports
        self.refresh = refresh
        self.stats: Counter = Counter()
        self.tools: Counter = Counter()
        self.finding_tools: Counter = Counter()
        self.findings_per_report: Counter = Counter()
        self.facility_types: Counter = Counter()
        self.held_back: List[str] = []
        self.not_reports: List[str] = []
        self.stubs: List[str] = []
        self.unparsed: List[str] = []
        self.notes: List[str] = []
        self.failed: List[str] = []

    # -- agency details and report list, cached for a few hours --

    def agency_data(self, number: str) -> Optional[Dict]:
        path = API_CACHE_DIR / f"{number}.json"
        if not self.refresh and path.exists():
            try:
                cached = json.loads(path.read_text(encoding="utf-8"))
                fetched = datetime.fromisoformat(cached["fetched"])
                if datetime.now() - fetched < timedelta(hours=API_CACHE_HOURS):
                    return cached
            except (OSError, ValueError, KeyError):
                pass
        details = self.client.details(number)
        files = self.client.report_files(number)
        data = {"fetched": datetime.now().isoformat(timespec="seconds"), "details": details, "files": files}
        API_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        return data

    # -- one PDF --

    def fetch_document(self, archive_name: str, entry: Dict, review_number: str) -> Optional[Dict]:
        """The parsed document, or None when it could not be had or is not the
        report its name says."""
        extracted = extract_with_cache(
            self.reports,
            archive_name,
            fetch=lambda: self.client.pdf(entry["fileURL"]),
            extract=extract_pdf,
        )
        if not extracted:
            self.stats["download_failed"] += 1
            return None
        text = extracted.get("text") or ""
        if not text:
            self.stats["no_text"] += 1
            logger.warning(f"  no text in {archive_name}")
            return None
        doc = parse_document(text)
        if doc["review_number"] != review_number:
            # Not the compliance report the file name promises: never posted,
            # and the archived copy is removed so the nightly sync does not
            # put it on the site.
            self.stats["not_a_report"] += 1
            self.not_reports.append(f"{archive_name} ({entry.get('fileName')}): no 'Review Number {review_number}' heading")
            logger.warning(f"  {archive_name} is not a compliance report; not posted, archive copy removed")
            try:
                (self.reports.archive_dir / archive_name).unlink()
            except OSError:
                pass
            return None
        doc["text"] = text
        doc["pages"] = extracted.get("pages")
        return doc

    # -- one review (main file + additional findings file) --

    def build_report(self, number: str, review_number: str, group: Dict, facilities: List[Dict]) -> Optional[Dict]:
        main_entry, extra_entry = group.get("main"), group.get("additional")
        main = self.fetch_document(f"{review_number}.pdf", main_entry, review_number) if main_entry else None
        extra = (self.fetch_document(f"{review_number}-additional.pdf", extra_entry, review_number)
                 if extra_entry else None)
        if main_entry and main is None:
            self.stats["main_missing"] += 1
        if extra_entry and extra is None:
            self.stats["additional_missing"] += 1
        if main is None and extra is None:
            return None
        if (main_entry and main is None) or (extra_entry and extra is None):
            # Half a report would be posted under an id that is then "seen":
            # leave the whole review for the next run.
            self.stats["incomplete_reviews"] += 1
            logger.warning(f"  {review_number}: one of its files could not be read; left for the next run")
            return None

        docs = [d for d in (main, extra) if d]
        texts = []
        if main:
            texts.append(main["text"])
        if extra:
            texts.append("ADDITIONAL FINDINGS\n\n" + extra["text"])
        raw = "\n\n".join(texts)

        hits = privacy_hits(raw)
        if hits:
            self.stats["held_back"] += 1
            self.held_back.append(f"{number} {review_number}: " + "; ".join(hits))
            logger.warning(f"  {review_number} held back (privacy check): {hits[0]}")
            for name in (f"{review_number}.pdf", f"{review_number}-additional.pdf"):
                try:
                    (self.reports.archive_dir / name).unlink()
                except OSError:
                    pass
            return None

        head = main or extra
        review_type = head["review_type"] or group.get("type", "")
        findings: List[Dict] = []
        for finding in (main["findings"] if main else []):
            findings.append(self.shape_finding(finding, "main"))
        if main:
            # Rows with an N in the main file's tables that the noncompliance
            # summaries do not carry (should be none).
            known = {(f["tool"], f["question"][:60]) for f in main["findings"]}
            for row in main["table_rows"]:
                if row["n_count"] > 0 and (row["tool"], row["question"][:60]) not in known:
                    self.stats["table_only_findings"] += 1
                    findings.append(self.shape_finding({**row, "cap_needed": None, "records": []}, "main"))
        for row in (extra["table_rows"] if extra else []):
            if row["n_count"] > 0:
                findings.append(self.shape_finding({**row, "cap_needed": None, "records": []}, "additional"))

        assistance = []
        for entry in (main["assistance"] if main else []):
            assistance.append({
                "tool": entry["tool"], "question": entry["question"], "rule": entry["rule"],
                "count": entry["count"],
                "comments": [self.shape_comment(r) for r in entry["records"]],
            })
        # An additional finding marked T/A and not N is technical assistance.
        for row in (extra["table_rows"] if extra else []):
            if row["n_count"] == 0 and row["ta_count"] > 0:
                assistance.append({
                    "tool": row["tool"], "question": row["question"], "rule": row["rule"],
                    "count": row["ta_count"], "comments": [],
                })

        tools = main["tools"] if main else []
        for tool in tools:
            self.tools[tool["tool"]] += 1
        for finding in findings:
            self.finding_tools[finding["tool"]] += 1

        stub = bool(main) and is_stub(main) and not extra
        if stub:
            self.stubs.append(f"{number} {review_number} ({review_type})")
        for doc in docs:
            has_sections = bool(doc["sections"])
            if has_sections and not doc["table_rows"] and not doc["findings"] and not doc["assistance"]:
                self.unparsed.append(f"{number} {review_number}: sections {doc['sections'][:4]} but nothing read")
        if main and not is_stub(main) and not main["sections"] and main["tools"]:
            self.stats["records_only"] += 1

        generated = next((d["generated"] for d in docs if d["generated"]), "")
        universe_end = next((d["universe_end"] for d in docs if d["universe_end"]), "")
        categories: Dict[str, Any] = {
            "review_type": review_type,
            "review_number": review_number,
            "universe_period": next((d["universe_period"] for d in docs if d["universe_period"]), ""),
            "specialist": next((d["specialist"] for d in docs if d["specialist"]), ""),
            "date_basis": "generated" if generated else ("universe_end" if universe_end else ""),
            "findings": findings,
            "finding_count": len(findings),
            "cap_finding_count": sum(1 for f in findings if f["cap_needed"]),
            "residential_finding_count": sum(1 for f in findings if f["residential"]),
            "assistance_count": len(assistance),
            "tools": tools,
            "has_additional": bool(extra),
            "stub": stub,
            "facilities": facilities,
            "archive_names": [name for name, d in ((f"{review_number}.pdf", main),
                                                   (f"{review_number}-additional.pdf", extra)) if d],
            "additional_url": extra_entry["fileURL"] if extra and main else "",
            # Left out of lite lists by inspections-read.php, returned with the text.
            "detail": {"technical_assistance": assistance},
        }

        self.stats["reports"] += 1
        self.stats["type_" + (review_type or "unknown")] += 1
        self.findings_per_report[len(findings)] += 1
        if findings:
            self.stats["flagged"] += 1

        return {
            "report_id": review_number,
            "report_date": generated or universe_end,
            "report_url": (main_entry or extra_entry)["fileURL"],
            "raw_content": raw,
            "content_length": len(raw),
            "summary": summarize(categories),
            "categories": categories,
            "is_flagged": bool(findings),
        }

    @staticmethod
    def shape_comment(record: Dict) -> Dict:
        return {"record": record["record"], "reason": one_line(record["reason"]),
                "comment": one_line(record["comment"])}

    def shape_finding(self, finding: Dict, source: str) -> Dict:
        tool = finding["tool"]
        return {
            "tool": tool,
            "question": finding["question"],
            "rule": finding.get("rule", ""),
            "n_count": finding["n_count"],
            "reviewed": finding["reviewed"],
            "compliance_pct": finding["compliance_pct"],
            "cap_needed": finding.get("cap_needed"),
            "source": source,
            "residential": is_residential(tool, finding.get("rule", "")),
            "comments": [self.shape_comment(r) for r in finding.get("records", [])],
        }

    # -- an agency --

    def facility_info(self, number: str, record: Dict, listed: bool) -> Dict:
        count = len(record.get("facilities") or [])
        status = "Active suspension" if record.get("suspended") else "Certified"
        if not listed:
            status = "No longer listed by the state"
        return {
            "facility_name": one_line(record.get("name") or number),
            "program_name": number,
            "program_category": f"Residential agency ({count} {'facility' if count == 1 else 'facilities'})",
            "full_address": clean_address(record.get("address") or ""),
            "phone": "",
            "bed_capacity": "",
            "executive_director": "",
            "license_exp_date": "",
            "relicense_visit_date": "",
            "action": status,
        }

    def group_files(self, number: str, files: List[Dict]) -> Dict[str, Dict]:
        """{review number: {type, main, additional}} from the state's file list."""
        groups: Dict[str, Dict] = {}
        for entry in files:
            name = one_line(entry.get("fileName") or "")
            self.stats["files_listed"] += 1
            match = FILE_NAME.match(name)
            if not match or not entry.get("fileURL"):
                # Not one of the report kinds this scraper posts.
                self.stats["not_a_report"] += 1
                self.not_reports.append(f"{number}: {name or '(no name)'} (unrecognised file name, not downloaded)")
                logger.warning(f"  unrecognised file name, skipped: {name}")
                continue
            review_number = match.group(2).upper()
            slot = "additional" if match.group(3) else "main"
            group = groups.setdefault(review_number, {"type": match.group(1).title()})
            if slot in group:
                self.stats["duplicate_files"] += 1
                self.notes.append(f"{number} {review_number}: a second {slot} file is listed ({name}); the first is used")
                continue
            group[slot] = entry
        return groups

    def scrape(
        self,
        seen: Dict[str, Set[str]],
        known_agencies: Dict[str, Dict],
        limit: int = 0,
        only: Optional[Set[str]] = None,
    ) -> Tuple[List[Dict], Dict[str, List[str]], Dict[str, Dict]]:
        rows = self.client.agency_rows()
        facility_rows = [r for r in rows if r.get("isFacility")]
        parents = {r.get("agencyId") for r in facility_rows if r.get("agencyId")}
        # The agency's own row is the one whose account number is its agency id
        # (a branch office is a second row under the same agency id).
        listed = {r["agencyId"]: r for r in rows
                  if not r.get("isFacility") and r.get("agencyId") in parents
                  and r.get("accountNumber") == r.get("agencyId")}
        for missing in sorted(parents - set(listed)):
            # A facility row whose agency has no row of its own: still in scope.
            first = next(r for r in facility_rows if r.get("agencyId") == missing)
            listed[missing] = {"agencyId": missing,
                               "agencyName": re.sub(r"\s*\(facility\)\s*$", "", first.get("agencyName") or "", flags=re.I)}
        logger.info(f"{len(rows)} rows listed by the state: {len(facility_rows)} facilities "
                    f"of {len(listed)} agencies in scope")
        self.stats["rows_listed"] = len(rows)
        self.stats["facility_rows"] = len(facility_rows)
        self.stats["agencies_in_scope"] = len(listed)

        agencies: List[Tuple[str, str, bool]] = [
            (number, row.get("agencyName") or "", True) for number, row in listed.items()]
        for number, stored in sorted(known_agencies.items()):
            if number not in listed:
                agencies.append((number, stored.get("name") or "", False))
        if only:
            agencies = [a for a in agencies if a[0] in only or agency_digits(a[0]) in only]
        agencies.sort(key=lambda a: a[1].lower())
        if limit:
            agencies = agencies[:limit]

        facilities_out: List[Dict] = []
        new_ids: Dict[str, List[str]] = {}
        registry: Dict[str, Dict] = {}
        today = datetime.now().strftime("%Y-%m-%d")
        for index, (number, name, is_listed) in enumerate(agencies, start=1):
            try:
                logger.info(f"[{index}/{len(agencies)}] {name} ({number})" + ("" if is_listed else " [no longer listed]"))
                stored = dict(known_agencies.get(number) or {})
                try:
                    data = self.agency_data(number)
                except (requests.RequestException, AuraContextError, RuntimeError) as exc:
                    logger.error(f"  agency calls failed: {exc}")
                    self.stats["agency_failed"] += 1
                    self.failed.append(f"{number} {name}: {exc.__class__.__name__}: {str(exc)[:160]}")
                    if stored:
                        registry[number] = stored
                    continue
                details = data.get("details")
                if details:
                    facilities = [{
                        "name": one_line(f.get("name") or ""),
                        "type": one_line(f.get("facilityType") or ""),
                        "address": clean_address(f.get("address") or ""),
                        "county": one_line(f.get("county") or ""),
                        "suspended": bool(f.get("hasActiveFacilitySuspension")),
                    } for f in (details.get("facilities") or [])]
                    record = {
                        "name": one_line(details.get("agencyName") or name),
                        "address": details.get("agencyBusinessAddress") or "",
                        "county": details.get("agencyCountyServed") or "",
                        "agency_type": details.get("agencyType") or "",
                        "suspended": bool(details.get("hasActiveAgencySuspension")),
                        "facilities": facilities,
                        "last_listed": today if is_listed else stored.get("last_listed", ""),
                    }
                else:
                    # getAgencyDetails answers null for an agency that left the list.
                    record = stored or {"name": name, "facilities": []}
                    if is_listed:
                        self.notes.append(f"{number} {name}: listed, but getAgencyDetails returned nothing")
                        record["last_listed"] = today
                registry[number] = record
                for facility in record.get("facilities") or []:
                    self.facility_types[facility.get("type") or "(none)"] += 1
                if record.get("suspended"):
                    self.stats["agencies_suspended"] += 1

                groups = self.group_files(number, data.get("files") or [])
                if not groups:
                    self.stats["agencies_without_reports"] += 1
                    continue
                already = seen.get(number, set())
                reports = []
                for review_number in sorted(groups):
                    group = groups[review_number]
                    if review_number in already and (
                            "additional" not in group or review_number + SEEN_ADDITIONAL in already):
                        continue
                    report = self.build_report(number, review_number, group, [
                        {k: v for k, v in f.items() if k != "suspended" or v} for f in record.get("facilities") or []])
                    if report:
                        reports.append(report)
                if not reports:
                    continue
                reports.sort(key=lambda r: r["report_date"], reverse=True)
                facilities_out.append({
                    "facility_info": self.facility_info(number, record, is_listed and bool(details)),
                    "reports": reports,
                })
                ids = []
                for report in reports:
                    ids.append(report["report_id"])
                    if report["categories"]["has_additional"]:
                        ids.append(report["report_id"] + SEEN_ADDITIONAL)
                new_ids[number] = ids
            except Exception as exc:  # one agency never stops the run
                self.stats["agency_failed"] += 1
                self.failed.append(f"{number} {name}: {exc.__class__.__name__}: {str(exc)[:160]}")
                logger.error(f"  {number} skipped for this run: {exc.__class__.__name__}: {exc}")
                if number in known_agencies:
                    registry.setdefault(number, dict(known_agencies[number]))
        return facilities_out, new_ids, registry

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        log = logger.info
        log("── Ohio run summary ──")
        log(f"requests made to the state: {self.client.requests_made}")
        log(f"agencies in scope: {self.stats.get('agencies_in_scope', 0)} "
            f"({self.stats.get('facility_rows', 0)} facility rows of {self.stats.get('rows_listed', 0)} rows)")
        log(f"agencies with no report files: {self.stats.get('agencies_without_reports', 0)}")
        log(f"agencies with new reports: {len(facilities)}")
        log(f"files listed: {self.stats.get('files_listed', 0)}")
        log(f"reports: {len(reports)} (flagged: {sum(1 for r in reports if r['is_flagged'])})")
        if dates:
            log(f"date range: {dates[0]} to {dates[-1]}")
        log(f"reports with no date: {sum(1 for r in reports if not r['report_date'])}")
        log(f"dated by the universe period's end (no 'date generated'): "
            f"{sum(1 for r in reports if r['categories']['date_basis'] == 'universe_end')}")
        for key in sorted(k for k in self.stats if k.startswith("type_")):
            log(f"  {key[5:]} Review: {self.stats[key]}")
        log(f"reports with an additional findings file: {sum(1 for r in reports if r['categories']['has_additional'])}")
        log(f"reports with only an additional findings file: "
            f"{sum(1 for r in reports if r['categories']['archive_names'] == [r['report_id'] + '-additional.pdf'])}")
        log("findings per report (findings: reports):")
        for count in sorted(self.findings_per_report):
            log(f"  {count:3d}: {self.findings_per_report[count]}")
        log(f"findings: {sum(r['categories']['finding_count'] for r in reports)} "
            f"(residential: {sum(r['categories']['residential_finding_count'] for r in reports)}, "
            f"CAP needed: {sum(r['categories']['cap_finding_count'] for r in reports)})")
        log(f"technical assistance items: {sum(r['categories']['assistance_count'] for r in reports)}")
        log(f"findings read only from a table (not in a noncompliance summary): {self.stats.get('table_only_findings', 0)}")
        log(f"stub main file with no additional file: {len(self.stubs)}")
        for line in self.stubs:
            log(f"  {line}")
        log(f"main files with a record list and no summaries: {self.stats.get('records_only', 0)}")
        log(f"unparsed documents: {len(self.unparsed)}")
        for line in self.unparsed:
            log(f"  {line}")
        log(f"downloads failed: {self.stats.get('download_failed', 0)}; no text: {self.stats.get('no_text', 0)}; "
            f"reviews left for the next run: {self.stats.get('incomplete_reviews', 0)}")
        log(f"agencies skipped after errors (retried next run): {len(self.failed)}")
        for line in self.failed:
            log(f"  {line}")
        log(f"not compliance reports (skipped): {len(self.not_reports)}")
        for line in self.not_reports:
            log(f"  {line}")
        log(f"held back by the privacy check: {len(self.held_back)}")
        for line in self.held_back:
            log(f"  {line}")
        for line in self.notes:
            log(f"note: {line}")
        log("tools in the record lists (reports that used each):")
        for tool, count in self.tools.most_common():
            log(f"  {count:4d}  {tool}")
        log("findings by tool:")
        for tool, count in self.finding_tools.most_common():
            log(f"  {count:4d}  {tool}")
        log("facility types (of the agencies visited):")
        for kind, count in self.facility_types.most_common():
            log(f"  {count:4d}  {kind}")


def summarize(categories: Dict) -> str:
    label = f"{categories['review_type'] or 'Compliance'} Review"
    count = categories["finding_count"]
    if count:
        text = f"{label}: {count} finding{'s' if count != 1 else ''} of noncompliance"
        residential = categories["residential_finding_count"]
        if residential:
            text += f" ({residential} residential)"
    elif categories["stub"]:
        text = f"{label}: no findings published"
    else:
        text = f"{label}: no findings of noncompliance"
    if categories["assistance_count"]:
        n = categories["assistance_count"]
        text += f", {n} technical assistance item{'s' if n != 1 else ''}"
    return text


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
        "source_state": "OH",
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
        state="OH",
        scraped_timestamp=timestamp,
        facilities=strip_internal(facilities),
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Ohio residential agency compliance reports")
    parser.add_argument("--full", action="store_true", help=f"Ignore the seen reports in {STATE_FILE}")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N agencies (by name)")
    parser.add_argument("--agency", action="append", default=[],
                        help="Only this agency (OFCLA-500533 or 500533; repeatable)")
    parser.add_argument("--refresh", action="store_true",
                        help="Ask the state for every agency's details and report list again "
                             f"(they are otherwise reused for {API_CACHE_HOURS:g} hours)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    timestamp = datetime.now().isoformat(timespec="seconds")

    scraper = OHScraper(refresh=args.refresh)
    facilities, new_ids, registry = scraper.scrape(
        seen=seen,
        known_agencies=state.get("agencies", {}),
        limit=args.limit,
        only=set(args.agency) or None,
    )
    scraper.print_stats(facilities)

    # The agency registry is not tied to a post: it only remembers who was listed.
    state.setdefault("agencies", {}).update(registry)
    save_state(STATE_FILE, state)

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
        logger.error("API save failed -- seen reports not advanced")


if __name__ == "__main__":
    main()
