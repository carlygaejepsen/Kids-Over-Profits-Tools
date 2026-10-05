"""
Colorado CDPHE health facility inspection scraper (slow browser click-driver).

Source: the health department's "Find and compare facilities" Tableau Server
workbook (guest access), linked from
https://cdphe.colorado.gov/health-facilities/find-and-compare-facilities :

  https://cohealthviz.dphe.state.co.us/t/HealthFacilitiesPublic/views/HealthFacilitySearchSite/<view>

Views: 1 facility search, 2 a facility's inspections and occurrences, 3A a
citation's text, 3B its regulation, 3C the facility's plan of correction,
4 an occurrence's description. Every view is drawn on the server, so the
page carries no data; the state shows the last three years only.

SAFETY. On 2026-10-05 scripted replays of the dashboard's session commands
ran unfiltered queries over the state's whole citation-text table and the
server stopped answering for at least twenty minutes. So:

  * Citation text is read ONLY by clicking through the dashboard in real
    Chrome (Playwright, channel="chrome"), the way a visitor does: view 2 for
    one facility and one inspection, click the inspection, click the
    citation, open tab 3A, check that the page's own header names exactly
    that inspection and citation, click the text, Download > Data. If the
    header does not name them, nothing is downloaded. Same for 3C.
  * Never replay session commands with requests for 3A/3B/3C/4.
  * The only non-browser calls are the two proven cheap ones: the view-1
    facility list as CSV, and per facility the view-2 session with
    "Facility ID" set, where selecting every inspection and reading the
    inspection and citation lists through the View Data commands returns a
    few dozen rows.
  * One page, one action at a time, at least ACTION_GAP (5 s) between
    dashboard actions. The whole run stops (state written, no retry) when a
    response takes longer than 30 s, a response is 5xx, or two actions in a
    row fail. A run reads at most --max-citations citations (default 40), so
    a first load is spread over several runs. Run it in the evening,
    Mountain time.
  * Everything is cached under .report_extract_cache/co/, keyed by facility,
    inspection and citation, so nothing is fetched twice.

Scope: co_scope.json (in / out / unsure with reasons); only "in" is read.

Payload: one facility per dashboard Facility ID (program_name "CO-<id>"),
one report per inspection (report_id = the state's inspection ID). A report
is posted only when every citation of the inspection has been read. Each
citation carries its code, title, scope and severity, the surveyor's text and
the facility's plan of correction when the dashboard gives one. "0000" and
"9999" are the inspection's opening and closing comments, not citations.
Flagged = at least one citation other than 0000/9999.

Self-reported occurrences (view 4) are not read yet: they need their own
click path, proven the same way first.
"""

import argparse
import csv
import io
import json
import logging
import os
import re
import time
import urllib.parse as up
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from inspection_api_client import post_facilities_to_api
from scraper_state import load_state, merge_new_ids, save_state, seen_from_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("KOP_DATA_API_KEY", "CHANGE_ME")
HERE = Path(__file__).parent
STATE_FILE = Path(os.getenv("CO_STATE_FILE", str(HERE / ".co_state.json")))
SCOPE_FILE = HERE / "co_scope.json"
CACHE = Path(os.getenv("CO_CACHE", str(HERE / ".report_extract_cache" / "co")))

HOST = "https://cohealthviz.dphe.state.co.us"
SITE = "HealthFacilitiesPublic"
WORKBOOK = "HealthFacilitySearchSite"
VIEW_SEARCH = "1_HealthFacilitySearchSite"
VIEW_LIST = "2_FacilitysListofInspectionsandOccurrences"
DASH_LIST = "2. Facility's List of Inspections and Occurrences"
DASH_TEXT = "3A. Citation's Text"
DASH_PLAN = "3C. Facility Plan of Correction"
ZONE_INSPECTIONS = "9"
SOURCE_PAGE = "https://cdphe.colorado.gov/health-facilities/find-and-compare-facilities"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/129.0 Safari/537.36")
REQUEST_GAP = 1.0        # between the cheap list calls
ACTION_GAP = 5.0         # between dashboard actions in the browser
SLOW_LIMIT = 30.0        # a response slower than this stops the run
LIST_MAX_AGE_DAYS = 7    # re-read a facility's inspection list after this
VIEWPORT = {"width": 1600, "height": 2400}

# Positions on view 2 and 3A/3C at the viewport above (the dashboard is a
# fixed 895 px wide, centred). With "Inspection ID" in the URL the inspection
# list has one row at the top; the citation list stretches its rows over the
# pane, so row i of k sits at CIT_TOP + (i + 0.5) * (CIT_BOTTOM - CIT_TOP) / k.
INSPECTION_ROW = (600, 1072)
CIT_X, CIT_TOP, CIT_BOTTOM = 1100, 1074, 1560
TEXT_CLICK = (790, 498)

COMMENT_CODES = {"0000", "9999"}


class StopRun(Exception):
    """The state's server is slow or failing: stop everything, no retry."""


def one_line(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def iso_date(value: str) -> str:
    match = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})", value or "")
    if not match:
        return ""
    try:
        return datetime(int(match.group(3)), int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def view_url(view: str, params: Optional[Dict[str, str]] = None) -> str:
    query = {":embed": "y", ":showVizHome": "no"}
    query.update(params or {})
    return f"{HOST}/t/{SITE}/views/{WORKBOOK}/{view}?" + up.urlencode(query, quote_via=up.quote)


def facility_page_url(facility_id: str) -> str:
    return f"{HOST}/t/{SITE}/views/{WORKBOOK}/{VIEW_LIST}?" + up.urlencode({"Facility ID": facility_id}, quote_via=up.quote)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


# ── Cheap calls (requests): the facility list and per-facility lists ─────────


def view_data_rows(body: Dict) -> List[Dict[str, str]]:
    """Rows of a get-view-data-dialog-tab-pres-model answer, by field caption."""
    result = body["vqlCmdResponse"]["cmdResultList"][0]["commandReturn"]
    model = result.get("viewDataDialogTabPresModel")
    if not model:
        return []
    page = model["viewDataTablePagePresModel"]
    values: List[Any] = []
    for segment in page["dataDictionary"]["dataSegments"].values():
        for column in segment["dataColumns"]:
            values.extend(column["dataValues"])
    columns = page["viewDataColumnValuesPresModels"]
    rows = []
    for index in range(page["pageInfo"]["uRowCount"]):
        rows.append({c["fieldCaption"]: one_line(values[c["formatValIdxs"][index]]) for c in columns})
    return rows


class ListClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self._last = 0.0

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        wait = REQUEST_GAP - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        started = time.monotonic()
        try:
            response = self.session.request(method, url, timeout=SLOW_LIMIT, **kwargs)
        except requests.exceptions.Timeout as exc:
            raise StopRun(f"no answer within {SLOW_LIMIT:.0f} s: {url[:120]}") from exc
        except requests.exceptions.ConnectionError as exc:
            raise StopRun(f"connection failed: {exc}") from exc
        finally:
            self._last = time.monotonic()
        if response.status_code >= 500:
            raise StopRun(f"HTTP {response.status_code} from {url[:120]}")
        if time.monotonic() - started > SLOW_LIMIT:
            raise StopRun(f"answer took {time.monotonic() - started:.0f} s: {url[:120]}")
        return response

    def facilities(self, refresh: bool) -> List[Dict[str, str]]:
        path = CACHE / "facilities.csv"
        fresh = path.exists() and time.time() - path.stat().st_mtime < 86400
        if refresh or not fresh:
            response = self._request("GET", f"{HOST}/t/{SITE}/views/{WORKBOOK}/{VIEW_SEARCH}.csv")
            response.raise_for_status()
            if "Facility ID" not in response.text[:400]:
                raise RuntimeError("The facility CSV has no 'Facility ID' column; the dashboard has changed")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(response.text, encoding="utf-8")
        text = path.read_text(encoding="utf-8").lstrip("﻿")
        return [{k: one_line(v) for k, v in row.items()} for row in csv.DictReader(io.StringIO(text))]

    def facility_lists(self, facility_id: str) -> Dict[str, Any]:
        """Inspections and citation rows of one facility (view 2, 'Facility ID'
        set). Selecting every inspection is what fills the citation list."""
        url = view_url(VIEW_LIST, {"Facility ID": facility_id})
        # The guest cookie comes from the plain search page: GETs of view 2
        # with a filter in the URL hung for 30 s+ several times on 2026-10-05
        # while the session calls below answered in a second.
        page = self._request("GET", view_url(VIEW_SEARCH))
        page.raise_for_status()
        query = url.split("?", 1)[1]
        root = f"{HOST}/vizql/t/{SITE}/w/{WORKBOOK}/v/{VIEW_LIST}"
        ajax = {"X-Requested-With": "XMLHttpRequest"}
        start = self._request("POST", f"{root}/startSession/viewing?{query}", headers=ajax)
        start.raise_for_status()
        info = start.json()
        session_id = info["sessionid"]
        sticky = info["stickySessionKey"]
        boot = self._request("POST", f"{root}/bootstrapSession/sessions/{session_id}", headers=ajax, data={
            "worksheetPortSize": json.dumps({"w": 895, "h": 7000}),
            "dashboardPortSize": json.dumps({"w": 895, "h": 7000}),
            "clientDimension": json.dumps({"w": 1600, "h": 1100}),
            "renderMapsClientSide": "true", "isBrowserRendering": "true", "browserRenderingThreshold": "100",
            "formatDataValueLocally": "false", "clientNum": "", "navType": "Nav", "navSrc": "Top",
            "devicePixelRatio": "1", "clientRenderPixelLimit": "16000000",
            "allowAutogenWorksheetPhoneLayouts": "true", "sheet_id": info["sheetId"],
            "showParams": json.dumps({"checkpoint": False, "refresh": False, "refreshUnmodified": False}),
            "stickySessionKey": sticky if isinstance(sticky, str) else json.dumps(sticky),
            "filterTileSize": "200", "locale": "en_US", "language": "en", "verboseMode": "false",
            ":session_feature_flags": "{}", "keychain_version": "1",
        })
        boot.raise_for_status()
        match = re.search(r"sqlproxy\.[a-z0-9]+", boot.text)
        if not match:
            raise RuntimeError("No datasource in the view-2 bootstrap; the dashboard has changed")
        datasource = match.group(0)

        def command(namespace: str, name: str, fields: Dict[str, Any]) -> Dict:
            files = {k: (None, v if isinstance(v, str) else json.dumps(v)) for k, v in fields.items()}
            response = self._request("POST", f"{root}/sessions/{session_id}/commands/{namespace}/{name}",
                                     files=files, headers=ajax)
            response.raise_for_status()
            return response.json()

        def rows_of(worksheet: str) -> List[Dict[str, str]]:
            visual = {"worksheet": worksheet, "dashboard": DASH_LIST}
            command("tabdoc", "launch-hybrid-view-data-dialog",
                    {"dataProviderType": "selection", "visualIdPresModel": visual})
            return view_data_rows(command("tabdoc", "get-view-data-dialog-tab-pres-model", {
                "dataProviderType": "selection", "viewDataTableId": "", "isSummaryTable": "true",
                "datasource": datasource, "connectionName": datasource, "sqlQuery": "", "tableName": "",
                "visualIdPresModel": visual, "topN": "5000"}))

        inspections = rows_of("List of Inspections")
        citations: List[Dict[str, str]] = []
        if inspections:
            command("tabsrv", "select-region-no-return-server", {
                "worksheet": "List of Inspections", "dashboard": DASH_LIST,
                "vizRegionRect": {"x": 0, "y": 0, "w": 5000, "h": 100000, "r": "viz"},
                "mouseAction": "simple", "zoneId": ZONE_INSPECTIONS, "zoneSelectionType": "replace"})
            citations = rows_of("List of Citations")
        known = {row.get("Inspection ID") for row in inspections}
        stray = [row for row in citations if row.get("Inspection ID") not in known]
        if stray:
            raise RuntimeError(f"{facility_id}: {len(stray)} citation rows name inspections the facility does not list")
        return {"fetched": datetime.now().isoformat(timespec="seconds"),
                "inspections": inspections, "citations": citations}


# ── The click-driver (real Chrome) ───────────────────────────────────────────


class Clicker:
    """One Chrome page; every dashboard action waits ACTION_GAP first and is
    followed by a health check over every request the page made."""

    def __init__(self, headless: bool = True, shots: Optional[Path] = None) -> None:
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self.browser = self._pw.chromium.launch(channel="chrome", headless=headless)
        self.context = self.browser.new_context(viewport=VIEWPORT, user_agent=USER_AGENT)
        self.page = self.context.new_page()
        self.shots = shots
        self.pending: Dict[int, Tuple[float, str]] = {}
        self.problems: List[str] = []
        self.view_data: List[Dict] = []
        self.slowest = 0.0
        self.context.on("request", self._on_request)
        self.context.on("requestfinished", self._on_finished)
        self.context.on("requestfailed", self._on_failed)
        self.context.on("response", self._on_response)

    def close(self) -> None:
        try:
            self.browser.close()
        finally:
            self._pw.stop()

    def _on_request(self, request) -> None:
        if "cohealthviz" in request.url:
            self.pending[id(request)] = (time.monotonic(), request.url)

    def _on_finished(self, request) -> None:
        started, url = self.pending.pop(id(request), (None, request.url))
        if started is not None:
            took = time.monotonic() - started
            self.slowest = max(self.slowest, took)
            if took > SLOW_LIMIT:
                self.problems.append(f"{took:.0f} s for {url[-120:]}")

    def _on_failed(self, request) -> None:
        self.pending.pop(id(request), None)
        if "cohealthviz" in request.url and "/commands/" in request.url:
            self.problems.append(f"request failed ({request.failure}): {request.url[-120:]}")

    def _on_response(self, response) -> None:
        if "cohealthviz" not in response.url:
            return
        if response.status >= 500:
            self.problems.append(f"HTTP {response.status} from {response.url[-120:]}")
        if "get-view-data-dialog-tab-pres-model" in response.url:
            try:
                size = int(response.headers.get("content-length") or 0)
            except ValueError:
                size = 0
            if size > 2_000_000:
                self.problems.append(f"view data answer of {size} bytes: the selection was not narrowed")
                return
            try:
                self.view_data.append({"post": response.request.post_data or "", "body": response.json()})
            except Exception as exc:  # noqa: BLE001 - a broken body is a failed action
                self.problems.append(f"unreadable view data answer: {exc}")

    def check(self, label: str) -> None:
        now = time.monotonic()
        hung = [url for started, url in self.pending.values() if now - started > SLOW_LIMIT]
        if hung:
            self.problems.append(f"no answer after {SLOW_LIMIT:.0f} s: {hung[0][-120:]}")
        if self.problems:
            raise StopRun(f"{label}: " + "; ".join(self.problems[:3]))
        if self.shots:
            self.shots.mkdir(parents=True, exist_ok=True)
            self.page.screenshot(path=str(self.shots / f"{int(time.time())}_{label}.png"))

    def act(self, label: str, action) -> None:
        time.sleep(ACTION_GAP)
        action()
        time.sleep(ACTION_GAP)
        self.check(label)

    def header_text(self) -> str:
        return one_line(self.page.locator("body").inner_text(timeout=SLOW_LIMIT * 1000))

    def download_data(self, label: str) -> List[Dict[str, str]]:
        """Download > Data on the selected mark; the rows come from the answer
        the page itself receives."""
        before = len(self.view_data)
        time.sleep(ACTION_GAP)
        self.page.locator('[data-tb-test-id="viz-viewer-toolbar-button-download"], '
                          '[data-tb-test-id="download-ToolbarButton"]').first.click(timeout=SLOW_LIMIT * 1000)
        time.sleep(2)
        item = self.page.get_by_text("Data", exact=True).first
        if (item.get_attribute("aria-disabled") or "").lower() == "true" or "disabled" in (item.get_attribute("class") or ""):
            raise ActionFailed(f"{label}: Download > Data is disabled (nothing selected)")
        with self.context.expect_page(timeout=SLOW_LIMIT * 1000) as popup:
            item.click(timeout=SLOW_LIMIT * 1000)
        window = popup.value
        deadline = time.monotonic() + SLOW_LIMIT
        while len(self.view_data) == before and time.monotonic() < deadline and not self.problems:
            time.sleep(0.5)
        try:
            window.close()
        except Exception:  # noqa: BLE001
            pass
        self.check(label)
        if len(self.view_data) == before:
            raise ActionFailed(f"{label}: the View Data window brought no data")
        return view_data_rows(self.view_data[-1]["body"])

    def read_citation(self, facility_id: str, inspection: str, code: str, index: int, count: int,
                      want_plan: bool = True) -> Dict[str, Any]:
        """One citation, by the visitor's path. Raises ActionFailed when the
        page does not show exactly this citation (nothing is downloaded then)."""
        page = self.page
        self.view_data.clear()
        url = view_url(VIEW_LIST, {"Facility ID": facility_id, "Inspection ID": inspection})
        self.act("load", lambda: page.goto(url, wait_until="load", timeout=SLOW_LIMIT * 2 * 1000))
        text = self.header_text()
        if facility_id not in text or "List of Inspections" not in text:
            raise ActionFailed(f"{facility_id}/{inspection}: view 2 did not open on this facility")
        self.act("inspection", lambda: page.mouse.click(*INSPECTION_ROW))
        row_height = (CIT_BOTTOM - CIT_TOP) / max(count, 1)
        y = CIT_TOP + (index + 0.5) * row_height
        self.act("citation", lambda: page.mouse.click(CIT_X, y))
        self.act("tab-3A", lambda: page.get_by_text(DASH_TEXT).first.click(timeout=SLOW_LIMIT * 1000))
        self._confirm(facility_id, inspection, code, "3A")
        self.act("text-3A", lambda: page.mouse.click(*TEXT_CLICK))
        rows = self.download_data("data-3A")
        result: Dict[str, Any] = {"text_rows": self._own_rows(rows, inspection, code, "3A")}
        if want_plan:
            try:
                self.act("tab-3C", lambda: page.get_by_text("3C. Facility Plan").first.click(timeout=SLOW_LIMIT * 1000))
                self._confirm(facility_id, inspection, code, "3C")
                self.act("text-3C", lambda: page.mouse.click(*TEXT_CLICK))
                result["plan_rows"] = self._own_rows(self.download_data("data-3C"), inspection, code, "3C")
            except StopRun:
                raise
            except Exception as exc:  # noqa: BLE001 - the text is kept; the plan is noted as unread
                if self.problems:
                    raise StopRun("; ".join(self.problems[:3])) from exc
                result["plan_error"] = one_line(exc)[:300]
        return result

    def _confirm(self, facility_id: str, inspection: str, code: str, tab: str) -> None:
        text = self.header_text()
        want = [f"Facility ID: {facility_id}", f"Inspection ID: {inspection}", f"Citation Code: {code}"]
        missing = [w for w in want if not re.search(re.escape(w) + r"(?![\w,])", text)]
        if missing or " more" in text.split("Notes")[0][-400:]:
            raise ActionFailed(f"{facility_id}/{inspection}/{code}: tab {tab} does not show exactly this "
                               f"citation ({', '.join(missing) or 'several values'}); nothing downloaded")

    @staticmethod
    def _own_rows(rows: List[Dict[str, str]], inspection: str, code: str, tab: str) -> List[Dict[str, str]]:
        own = [r for r in rows if r.get("Inspection ID") == inspection and r.get("Citation Code") == code]
        if not own or len(own) != len(rows):
            raise ActionFailed(f"{inspection}/{code}: tab {tab} data held {len(rows)} rows, {len(own)} of this citation")
        return own


class ActionFailed(Exception):
    """One action did not do what it should; two in a row stop the run."""


# ── Building the payload ─────────────────────────────────────────────────────

PRIVACY_CHECKS = (
    ("date of birth", re.compile(
        r"\b(?:D\.?O\.?B\.?|date\s+of\s+birth|born\s+on)\b\W{0,12}"
        r"(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", re.I)),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("record number", re.compile(
        r"\b(?:medical\s+record|MRN|client\s+ID|patient\s+ID|account)\s*(?:number|no\.?|#)?\s*[:#]?\s*[A-Z]?\d{5,}", re.I)),
    ("possible named patient", re.compile(
        r"\b(?:Resident|Youth|Child|Patient|Client|Student)\s+(?:named\s+)?[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}\b")),
)
NOT_A_NAME = {
    "abuse", "advocate", "assessment", "care", "case", "council", "counselor", "family", "file", "files",
    "health", "plan", "program", "protective", "record", "records", "rights", "safety", "service", "services",
    "treatment", "worker", "protection", "identified", "observation", "safety", "census", "room", "unit",
    "education", "medical", "medication", "admission", "discharge", "interview", "list", "grievance",
}


def privacy_hits(text: str) -> List[str]:
    hits = []
    for label, pattern in PRIVACY_CHECKS:
        for match in pattern.finditer(text or ""):
            words = match.group(0).split()[-2:]
            if label == "possible named patient" and any(w.lower() in NOT_A_NAME for w in words):
                continue
            hits.append(label)
            break
    return hits


def joined_text(rows: List[Dict[str, str]], prefix: str) -> str:
    """The sheet splits long text over 'Citation Text', 'Citation Text 2'...;
    join the fields that start with the prefix, in field order."""
    if not rows:
        return ""
    row = rows[0]
    parts = [value for key, value in row.items() if key.lower().startswith(prefix.lower()) and value]
    return "\n".join(dict.fromkeys(parts)).strip()


def citation_entry(code: str, title: str, cached: Dict) -> Dict[str, str]:
    text_rows = cached.get("text_rows") or []
    plan_rows = cached.get("plan_rows") or []
    first = text_rows[0] if text_rows else {}
    return {
        "code": code,
        "title": title,
        "scope_severity": one_line(first.get("Scope and Severity (S/S)")),
        "text": joined_text(text_rows, "Citation Text"),
        "plan": joined_text(plan_rows, "Plan of Correction") or joined_text(plan_rows, "POC")
                or joined_text(plan_rows, "Facility Plan"),
    }


def citation_path(facility_id: str, inspection: str, code: str) -> Path:
    return CACHE / facility_id / "citations" / f"{inspection}_{code}.json"


class COScraper:
    def __init__(self, max_citations: int = 40, headless: bool = True, shots: Optional[Path] = None,
                 want_plan: bool = True) -> None:
        self.lists = ListClient()
        self.max_citations = max_citations
        self.headless = headless
        self.shots = shots
        self.want_plan = want_plan
        self.stats: Counter = Counter()
        self.held: List[str] = []
        self.failures: List[str] = []
        self.unlisted: List[str] = []
        self.stopped = ""
        self.slowest = 0.0

    def scope(self, facilities: List[Dict[str, str]]) -> List[Dict]:
        doc = read_json(SCOPE_FILE) or {}
        listed = {row["id"]: row for row in doc.get("facilities") or []}
        types = set(doc.get("types") or [])
        for row in facilities:
            if row.get("Facility Type Name") in types and row["Facility ID"] not in listed:
                self.unlisted.append(f"{row['Facility ID']} {row['Facility Name']} ({row['Facility Type Name']})")
        by_id = {row["Facility ID"]: row for row in facilities}
        chosen = []
        for entry in listed.values():
            if entry.get("scope") == "in":
                if entry["id"] not in by_id:
                    self.failures.append(f"{entry['id']} {entry['name']}: in scope but no longer on the dashboard")
                    continue
                chosen.append(by_id[entry["id"]])
        self.stats["in_scope"] = len(chosen)
        self.stats["unsure"] = sum(1 for e in listed.values() if e.get("scope") == "unsure")
        self.stats["out"] = sum(1 for e in listed.values() if e.get("scope") == "out")
        return sorted(chosen, key=lambda r: r["Facility Name"])

    def lists_for(self, facility_id: str, refresh: bool) -> Dict:
        path = CACHE / facility_id / "lists.json"
        cached = read_json(path)
        if cached and not refresh:
            age = datetime.now() - datetime.fromisoformat(cached["fetched"])
            if age.days < LIST_MAX_AGE_DAYS:
                return cached
        fresh = self.lists.facility_lists(facility_id)
        write_json(path, fresh)
        self.stats["lists_fetched"] += 1
        return fresh

    def todo(self, facility_id: str, lists: Dict) -> List[Tuple[str, str, int, int]]:
        by_inspection: Dict[str, List[str]] = {}
        for row in lists["citations"]:
            by_inspection.setdefault(row["Inspection ID"], []).append(row["Citation Code"])
        out = []
        for inspection, codes in by_inspection.items():
            ordered = sorted(codes)
            for index, code in enumerate(ordered):
                if ordered.count(code) > 1:
                    continue   # the same code twice: positions are ambiguous, left for a person
                if not citation_path(facility_id, inspection, code).exists():
                    out.append((inspection, code, index, len(ordered)))
        return out

    def run(self, only: Optional[List[str]], limit: int, refresh_lists: bool, read_text: bool) -> List[Dict]:
        facilities = self.lists.facilities(refresh=False)
        targets = self.scope(facilities)
        if only:
            targets = [t for t in targets if t["Facility ID"] in only]
        if limit:
            targets = targets[:limit]
        clicker: Optional[Clicker] = None
        read = 0
        failed_in_a_row = 0
        all_lists: Dict[str, Dict] = {}
        try:
            for facility in targets:
                fid = facility["Facility ID"]
                lists = self.lists_for(fid, refresh_lists)
                all_lists[fid] = lists
                todo = self.todo(fid, lists)
                self.stats["citations_listed"] += len(lists["citations"])
                self.stats["citations_waiting"] += len(todo)
                logger.info(f"{facility['Facility Name']} ({fid}): {len(lists['inspections'])} inspections, "
                            f"{len(lists['citations'])} citation rows, {len(todo)} not read yet")
                if not read_text:
                    continue
                for inspection, code, index, count in todo:
                    if read >= self.max_citations:
                        break
                    if clicker is None:
                        clicker = Clicker(headless=self.headless, shots=self.shots)
                    started = time.monotonic()
                    try:
                        result = clicker.read_citation(fid, inspection, code, index, count, self.want_plan)
                    except StopRun:
                        raise
                    except Exception as exc:  # noqa: BLE001 - ActionFailed or a Playwright timeout
                        if clicker.problems:
                            raise StopRun("; ".join(clicker.problems[:3])) from exc
                        failed_in_a_row += 1
                        exc = f"{fid}/{inspection}/{code}: {exc}"
                        self.failures.append(str(exc))
                        logger.warning(f"  {exc}")
                        if failed_in_a_row >= 2:
                            raise StopRun("two actions in a row failed")
                        continue
                    failed_in_a_row = 0
                    result["fetched"] = datetime.now().isoformat(timespec="seconds")
                    write_json(citation_path(fid, inspection, code), result)
                    read += 1
                    self.stats["citations_read"] += 1
                    self.stats["citations_waiting"] -= 1
                    logger.info(f"  read {inspection} {code} in {time.monotonic() - started:.0f} s "
                                f"({read}/{self.max_citations} this run)")
        except StopRun as exc:
            self.stopped = str(exc)
            logger.error(f"STOPPED: {exc}. State written; not retrying. Check the dashboard by hand before the next run.")
        finally:
            if clicker is not None:
                self.slowest = clicker.slowest
                clicker.close()
        built = []
        for facility in targets:
            fid = facility["Facility ID"]
            lists = all_lists.get(fid) or read_json(CACHE / fid / "lists.json")
            if not lists:
                continue
            reports = self.build_reports(facility, lists)
            if reports:
                built.append({"facility_info": self.facility_info(facility), "reports": reports})
        return built

    @staticmethod
    def facility_info(row: Dict[str, str]) -> Dict[str, str]:
        return {
            "facility_name": row["Facility Name"],
            "program_name": f"CO-{row['Facility ID']}",
            "program_category": row.get("Facility Type Name", ""),
            "full_address": row.get("Facility Address", "").rstrip("-").strip(),
            "phone": "" if "Not Ava" in row.get("Facility Phone", "") else row.get("Facility Phone", ""),
            "bed_capacity": "",
            "executive_director": "",
            "license_exp_date": "",
            "relicense_visit_date": "",
            "action": row.get("Operating Status", ""),
        }

    def build_reports(self, facility: Dict[str, str], lists: Dict) -> List[Dict]:
        fid = facility["Facility ID"]
        by_inspection: Dict[str, List[Dict[str, str]]] = {}
        for row in lists["citations"]:
            by_inspection.setdefault(row["Inspection ID"], []).append(row)
        reports = []
        for inspection in lists["inspections"]:
            inspection_id = inspection["Inspection ID"]
            rows = sorted(by_inspection.get(inspection_id, []), key=lambda r: r["Citation Code"])
            cached = [read_json(citation_path(fid, inspection_id, r["Citation Code"])) for r in rows]
            if not rows or any(c is None for c in cached):
                self.stats["inspections_incomplete"] += 1
                continue
            entries = [citation_entry(r["Citation Code"], r["Citation Title"], c) for r, c in zip(rows, cached)]
            comments = [e for e in entries if e["code"] in COMMENT_CODES]
            citations = [e for e in entries if e["code"] not in COMMENT_CODES]
            plans_missing = sum(1 for e in citations if not e["plan"])
            parts = []
            for entry in comments:
                if entry["text"]:
                    parts.append(entry["text"])
            for entry in citations:
                block = f"{entry['code']} {entry['title']}"
                if entry["scope_severity"] and entry["scope_severity"] != "N/A":
                    block += f" (scope and severity {entry['scope_severity']})"
                block += "\n" + entry["text"]
                if entry["plan"]:
                    block += "\n\nPlan of correction: " + entry["plan"]
                parts.append(block.strip())
            raw = "\n\n".join(p for p in parts if p).strip()
            hits = privacy_hits(raw)
            if hits:
                self.held.append(f"{facility['Facility Name']} ({fid}) inspection {inspection_id}: {', '.join(hits)}")
                continue
            kind = one_line(inspection.get("Inspection Type")) or "Inspection"
            if citations:
                summary = (f"{kind}: {len(citations)} {'citation' if len(citations) == 1 else 'citations'} ("
                           + ", ".join(f"{e['code']} {e['title']}" for e in citations) + ")")
            else:
                summary = f"{kind}: no citations"
            reports.append({
                "report_id": inspection_id,
                "report_date": iso_date(inspection.get("Inspection Date", "")),
                "report_url": facility_page_url(fid),
                "raw_content": raw,
                "content_length": len(raw),
                "summary": summary,
                "categories": {
                    "kind": "inspection",
                    "inspection_type": kind,
                    "is_complaint": "complaint" in kind.lower(),
                    "citation_count": len(citations),
                    "citations": citations,
                    "comments": "\n\n".join(e["text"] for e in comments if e["text"]),
                    "plans_missing": plans_missing,
                },
                "is_flagged": bool(citations),
                "has_text": bool(raw),
            })
            self.stats["inspections_built"] += 1
        reports.sort(key=lambda r: r["report_date"], reverse=True)
        return reports

    def print_stats(self, facilities: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        logger.info("-- Colorado run summary --")
        logger.info(f"scope: {self.stats['in_scope']} in, {self.stats['unsure']} unsure (not read), {self.stats['out']} out")
        logger.info(f"citation rows listed: {self.stats['citations_listed']}; read this run: {self.stats['citations_read']}; "
                    f"still to read: {self.stats['citations_waiting']}")
        logger.info(f"facilities with complete inspections: {len(facilities)}; inspections built: {len(reports)}; "
                    f"flagged: {sum(1 for r in reports if r['is_flagged'])}; "
                    f"incomplete (citations not read yet): {self.stats['inspections_incomplete']}")
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}")
        logger.info(f"slowest dashboard response this run: {self.slowest:.1f} s")
        if self.stopped:
            logger.error(f"run stopped: {self.stopped}")
        for line in self.failures:
            logger.warning(f"  failed: {line}")
        logger.info(f"held back for review: {len(self.held)}")
        for line in self.held:
            logger.warning(f"  held: {line}")
        if self.unlisted:
            logger.warning(f"facilities of the scoped types that co_scope.json does not list: {len(self.unlisted)}")
            for line in self.unlisted:
                logger.warning(f"  {line}")


def strip_internal(facilities: List[Dict]) -> List[Dict]:
    return [{"facility_info": f["facility_info"],
             "reports": [{k: v for k, v in r.items() if k != "is_flagged"} for r in f["reports"]]}
            for f in facilities]


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    payload = {
        "total_facilities": len(facilities),
        "source_state": "CO",
        "scraped_timestamp": timestamp,
        "scraping_notes": {"total_reports": sum(len(f["reports"]) for f in facilities)},
        "facilities": [{"facility_info": f["facility_info"],
                        "reports": [{**r, "is_structured": True} for r in f["reports"]]}
                       for f in strip_internal(facilities)],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"Wrote {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Colorado CDPHE inspections (slow browser click-driver)")
    parser.add_argument("--full", action="store_true", help=f"Ignore the posted reports in {STATE_FILE}")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N in-scope facilities")
    parser.add_argument("--facility", action="append", default=[], help="Only this dashboard Facility ID (repeatable)")
    parser.add_argument("--max-citations", type=int, default=40, help="Citations to read in the browser this run (default 40)")
    parser.add_argument("--lists-only", action="store_true", help="Refresh the inspection lists, read no citation text")
    parser.add_argument("--refresh-lists", action="store_true", help="Re-read inspection lists younger than a week")
    parser.add_argument("--no-plan", action="store_true", help="Skip tab 3C (plans of correction)")
    parser.add_argument("--headed", action="store_true", help="Show the Chrome window")
    parser.add_argument("--shots", type=Path, help="Save a screenshot after every action into this folder")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    args = parser.parse_args()
    if args.limit < 0 or args.max_citations < 0:
        parser.error("--limit and --max-citations must be zero or positive")
    hour = datetime.now().astimezone().hour
    logger.info(f"Local time {datetime.now():%H:%M}; Colorado is two hours behind Eastern. "
                "Prefer evenings, Mountain time.")
    scraper = COScraper(max_citations=args.max_citations, headless=not args.headed, shots=args.shots,
                        want_plan=not args.no_plan)
    timestamp = datetime.now().isoformat(timespec="seconds")
    try:
        facilities = scraper.run(only=args.facility or None, limit=args.limit,
                                 refresh_lists=args.refresh_lists, read_text=not args.lists_only)
    except StopRun as exc:
        logger.error(f"STOPPED: {exc}. Not retrying.")
        raise SystemExit(2) from exc
    except (requests.RequestException, RuntimeError, ValueError) as exc:
        logger.error(f"Colorado scrape failed: {exc}")
        raise SystemExit(1) from exc
    scraper.print_stats(facilities)
    if args.out:
        write_out(args.out, facilities, timestamp)
    if scraper.stopped:
        raise SystemExit(2)
    if not facilities:
        logger.info("No complete inspections yet")
        return
    if args.no_post:
        logger.info("Skipping API POST because --no-post was set; posted reports not advanced")
        return
    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)
    fresh = []
    new_ids: Dict[str, List[str]] = {}
    for facility in facilities:
        key = facility["facility_info"]["program_name"]
        reports = [r for r in facility["reports"] if r["report_id"] not in seen.get(key, set())]
        if reports:
            fresh.append({**facility, "reports": reports})
            new_ids[key] = [r["report_id"] for r in reports]
    if not fresh:
        logger.info("Nothing new to post")
        return
    result = post_facilities_to_api(api_url=API_URL, api_key=API_KEY, state="CO", scraped_timestamp=timestamp,
                                    facilities=strip_internal(fresh), timeout=120,
                                    info=logger.info, error=logger.error)
    if result.get("success"):
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- posted reports not advanced")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
