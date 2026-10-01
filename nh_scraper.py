"""
New Hampshire residential child care licensing visit scraper.

Source: NH DHHS Child Care Licensing Unit search (Salesforce Visualforce),
https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch. Plain requests,
no login. (The host without the hyphen returns 502.)

  1. The list is one Visualforce remoting call: GET the search page, read the
     JSON inside RemotingProviderImpl({...}) for vf.vid and the csrf, ns, ver
     and authorization of NH_ChildCareSearchClass.retrieveAccountRecords, then
     POST /nhccis/apexremote with 29 arguments, the fourth being the program
     type "Residential Child Care Program". Rows are at result.v[], each row's
     fields under .v.
  2. Each program's page (GET NH_childcaresearchaccountdetail?id=<account id>)
     has a "Licensing History" table: review date, type of review, level of
     compliance (met / reviewed), a "View Detail" link carrying the visit id,
     and now and then a "Visit Documents" link for a visit kept in the state's
     previous system.
  3. A visit's detail is a ViewState postback to the same URL
     (AJAXREQUEST=_viewRoot, the form id, the id of the script component that
     defines getNonComplianceItem, selectedVisitId). It returns the licensor,
     the date of visit, the date the corrective action was accepted and, per
     domain, every rule reviewed with Compliant or Non-Compliant; each
     non-compliance carries the inspector's observations and the program's
     corrective action plan.
  4. A visit document is a Salesforce content-delivery link: four requests
     (the link, the composite page, the Lightning app bootstrap, the Aura
     getContentDistributionInfo action) and then the download.

One report per visit; report_id is the visit id, program_name the account id.
The state shows only the previous three years, so a visit that ages out is
gone from the source: the list, every program page and every visit detail is
saved gzipped to the FileBird Drive folder `nh_html/` when its content changed,
and `--from-saved` rebuilds the payload from those copies with no requests.
Nothing is ever deleted from the site.

A corrective action plan and its acceptance date arrive after the visit, so
the state file keeps a content hash per visit (as ok_scraper.py does) and a
visit is posted again when its hash changes; the write API updates the row.

Only licensing visits are posted. A visit document that is not a statement of
findings, a visit whose detail could not be read, and any visit whose text
trips the privacy check (a date of birth, a named child, a record number) is
left out of the payload and listed in the run summary for the owner.
"""

import argparse
import gzip
import hashlib
import html as html_lib
import json
import logging
import os
import re
import time
import urllib.parse
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from inspection_api_client import post_facilities_to_api
from kop_paths import report_cache_dir
from report_store import ReportStore
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
STATE_FILE = Path(os.getenv("NH_STATE_FILE", ".nh_state.json"))

BASE = "https://new-hampshire.my.site.com/nhccis/"
SEARCH_PAGE = BASE + "NH_ChildCareSearch"
REMOTE_URL = BASE + "apexremote"
PROGRAM_URL = BASE + "NH_childcaresearchaccountdetail?id={account}"
REMOTE_CLASS = "NH_ChildCareSearchClass"
REMOTE_METHOD = "retrieveAccountRecords"
PROGRAM_TYPE = "Residential Child Care Program"
PROGRAM_CATEGORY = "Residential child care program"
ACCOUNT_ID = re.compile(r"^001[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$")
VISIT_ID = re.compile(r"getNonComplianceItem\('([A-Za-z0-9]{15,18})'\)")
VIEWSTATE_FIELDS = ("ViewState", "ViewStateVersion", "ViewStateMAC", "ViewStateCSRF")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 0.8


# ── Fetch layer ──────────────────────────────────────────────────────────────


class NHClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call = 0.0

    def _pause(self) -> None:
        wait = REQUEST_GAP - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, method: str, url: str, session: Optional[requests.Session] = None,
                 **kwargs) -> requests.Response:
        """One request with retries on timeouts, connection errors and 5xx."""
        delay = 3.0
        kwargs.setdefault("timeout", 120)
        for attempt in range(1, 5):
            self._pause()
            try:
                response = (session or self.session).request(method, url, **kwargs)
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

    def listing(self) -> List[Dict[str, Any]]:
        """Every residential program's account row."""
        page = self._request("GET", SEARCH_PAGE).text
        match = re.search(r"RemotingProviderImpl\((\{.*?\})\)\);", page, re.S)
        if not match:
            raise RuntimeError(f"No Visualforce remoting block on {SEARCH_PAGE}; the page changed")
        config = json.loads(match.group(1))
        methods = [m for m in config["actions"][REMOTE_CLASS]["ms"] if m["name"] == REMOTE_METHOD]
        if not methods:
            raise RuntimeError(f"{REMOTE_CLASS}.{REMOTE_METHOD} is no longer offered by the search page")
        method = methods[0]
        # 29 arguments: empty strings or false, except the fourth (program type).
        data: List[Any] = (["", "", "", PROGRAM_TYPE] + [""] * 9 + [False, False] + [""] * 7 + [False] * 7)
        body = {
            "action": REMOTE_CLASS, "method": REMOTE_METHOD, "data": data, "type": "rpc", "tid": 2,
            "ctx": {"csrf": method["csrf"], "vid": config["vf"]["vid"], "ns": method["ns"],
                    "ver": method["ver"], "authorization": method["authorization"]},
        }
        response = self._request("POST", REMOTE_URL, json=body, headers={
            "Referer": SEARCH_PAGE, "X-User-Agent": "Visualforce-Remoting",
            "X-Requested-With": "XMLHttpRequest"})
        answer = response.json()[0]
        if answer.get("statusCode") != 200:
            raise RuntimeError(f"The list call answered {answer.get('statusCode')}: {answer.get('message')}")
        return list_rows(answer)

    def program_page(self, account: str) -> str:
        response = self._request("GET", PROGRAM_URL.format(account=account))
        response.encoding = "utf-8"
        return response.text

    def visit_detail(self, account: str, page_html: str, visit_id: str,
                     viewstate: Optional[Dict[str, str]] = None) -> str:
        """The postback behind "View Detail". `viewstate` overrides the page's
        own hidden fields with those of the previous answer."""
        url = PROGRAM_URL.format(account=account)
        form = re.search(r'<form id="([^"]+)"[^>]*action="/nhccis/NH_childcaresearchaccountdetail', page_html)
        component = re.search(r'<script id="([^"]+)" type="text/javascript">getNonComplianceItem=', page_html)
        if not form or not component:
            raise ValueError(f"{account}: the program page has no visit detail form")
        data = {"AJAXREQUEST": "_viewRoot", form.group(1): form.group(1),
                component.group(1): component.group(1), "selectedVisitId": visit_id}
        data.update(viewstate or viewstate_fields(page_html))
        response = self._request("POST", url, data=data, headers={"Referer": url})
        response.encoding = "utf-8"
        return response.text

    def visit_document(self, url: str) -> Tuple[bytes, str]:
        """(bytes, title) of a Salesforce content-delivery document. Its own
        session: the delivery host sets cookies the search does not need."""
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        match = re.match(r"(https://[^/]+)/sfc/p/([^/]+)(/a/[^/]+/[^/?#]+)", url)
        if not match:
            raise ValueError(f"not a content-delivery link: {url}")
        host, org, suffix = match.groups()
        self._request("GET", url, session=session)
        shell = self._request("POST", host + "/sfc/p/", session=session,
                              data={"compositePageName": org + suffix},
                              headers={"Referer": url, "Origin": host}).text
        record = re.search(r"recordId:'([^']+)'", shell)
        org_id = re.search(r"orgId:'([^']+)'", shell)
        if not record or not org_id:
            raise ValueError("the delivery page names no record")
        base = f"{host}/sfc/ld/{org}{suffix}"
        app = self._request(
            "GET", base + "/forceContent/contentDistributionApp.app?aura.format=JSON&aura.formatAdapter=LIGHTNING_OUT",
            session=session, headers={"Referer": host + "/sfc/p/"}).text
        fwuid = re.search(r'"fwuid":"([^"]+)"', app)
        loaded = re.search(r'"APPLICATION@markup://forceContent:contentDistributionApp":"([^"]+)"', app)
        if not fwuid or not loaded:
            raise ValueError("the delivery app gave no framework id")
        context = {"mode": "PROD", "fwuid": fwuid.group(1), "app": "forceContent:contentDistributionApp",
                   "loaded": {"APPLICATION@markup://forceContent:contentDistributionApp": loaded.group(1)},
                   "dn": [], "globals": {}, "uad": True}
        message = {"actions": [{
            "id": "7;a",
            "descriptor": ("serviceComponent://ui.content.components.forceContent.contentDistributionViewer."
                           "ContentDistributionViewerController/ACTION$getContentDistributionInfo"),
            "callingDescriptor": "UNKNOWN",
            "params": {"recordId": record.group(1), "isInternalView": "", "dpt": ""}}]}
        info = self._request(
            "POST",
            base + "/aura?r=0&ui-content-components-forceContent-contentDistributionViewer."
                   "ContentDistributionViewer.getContentDistributionInfo=1",
            session=session,
            data={"message": json.dumps(message, separators=(",", ":")),
                  "aura.context": json.dumps(context, separators=(",", ":")),
                  "aura.pageURI": f"/sfc/p/#{org}{suffix}", "aura.token": "null"},
            headers={"Referer": host + "/sfc/p/",
                     "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
        ).json()["actions"][0]["returnValue"]
        download = (f"{host}/sfc/dist/version/download/?oid={org_id.group(1)}&ids={info['versionId']}"
                    f"&d={urllib.parse.quote(suffix, safe='')}&operationContext=DELIVERY"
                    f"&viewId={info.get('viewId', '')}&dpt=")
        response = self._request("GET", download, session=session, headers={"Referer": host + "/sfc/p/"})
        title = one_line(str(info.get("title") or info.get("name") or ""))
        return response.content, title


def list_rows(answer: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The fields the scraper keeps from the list call. Email__c is dropped
    here so it is never saved, posted or logged."""
    rows = []
    for entry in (answer.get("result") or {}).get("v") or []:
        fields = entry.get("v") or {}
        if not fields.get("Id"):
            continue
        rows.append({key: fields.get(key) for key in (
            "Id", "Name", "ShippingStreet", "ShippingCity", "ShippingPostalCode", "Phone",
            "Capacity__c", "License_Status__c", "Licensed__c")})
    return rows


def viewstate_fields(text: str) -> Dict[str, str]:
    fields = {}
    for key in VIEWSTATE_FIELDS:
        match = re.search(r'name="com\.salesforce\.visualforce\.%s" value="([^"]*)"' % key, text)
        if match:
            fields["com.salesforce.visualforce." + key] = html_lib.unescape(match.group(1))
    return fields


def is_program_page(text: str) -> bool:
    return "getNonComplianceItem=" in text and "com.salesforce.visualforce.ViewState" in text


def is_visit_detail(text: str) -> bool:
    """A real answer carries the modal with its header (a visit with no rule
    reviewed, 0 / 0, has no domain rows); an expired ViewState answers with
    the page or an empty span instead."""
    return "visiItemDetail" in text and "Date of Visit" in text and "Licensor Assigned" in text


# ── Saved pages ──────────────────────────────────────────────────────────────


def text_fingerprint(markup: str) -> str:
    """Hash of what a reader would see: scripts, styles and hidden inputs (the
    ViewState changes on every request) are left out."""
    soup = BeautifulSoup(markup, "html.parser")
    for tag in soup.find_all(["script", "style", "input", "head"]):
        tag.decompose()
    return hashlib.sha1(one_line(soup.get_text(" ")).encode("utf-8")).hexdigest()


class PageStore:
    """Gzipped copies of what the state served:
         <base>/_list/list_<YYYY-MM-DD>.json.gz
         <base>/<account id>/program_<YYYY-MM-DD>.html.gz
         <base>/<account id>/visit_<visit id>_<YYYY-MM-DD>.html.gz
    written only when the content changed since the last copy. A second
    changed copy on one day replaces that day's copy."""

    def __init__(self, base: Optional[Path] = None):
        self.base = base or report_cache_dir("NH_HTML_CACHE", "nh_html", Path(__file__).parent / "nh_html")

    def copies(self, folder: str, stem: str) -> List[Path]:
        try:
            return sorted((self.base / folder).glob(f"{stem}_????-??-??.*.gz"))
        except OSError:
            return []

    def latest(self, folder: str, stem: str) -> Optional[Path]:
        copies = self.copies(folder, stem)
        return copies[-1] if copies else None

    @staticmethod
    def read(path: Path) -> str:
        return gzip.decompress(path.read_bytes()).decode("utf-8")

    def save(self, folder: str, stem: str, text: str, fingerprint: str, known: Optional[str],
             extension: str = "html") -> bool:
        """Save unless the last copy has the same fingerprint. `known` is the
        fingerprint the state file remembers, which spares reading Drive."""
        if known is None:
            last = self.latest(folder, stem)
            if last is not None:
                try:
                    saved = self.read(last)
                    known = list_fingerprint(json.loads(saved)) if extension == "json" else text_fingerprint(saved)
                except (OSError, ValueError) as exc:
                    logger.warning(f"  could not read {last}: {exc}")
        if known == fingerprint:
            return False
        target = self.base / folder
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{stem}_{datetime.now():%Y-%m-%d}.{extension}.gz").write_bytes(
            gzip.compress(text.encode("utf-8"), mtime=0))
        return True


def list_fingerprint(rows: List[Dict[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda r: str(r.get("Id")))
    return hashlib.sha1(json.dumps(ordered, sort_keys=True).encode("utf-8")).hexdigest()


# ── Parsing ──────────────────────────────────────────────────────────────────


def one_line(value: str) -> str:
    value = (value or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def block_text(tag) -> str:
    """Text of a narrative cell, keeping the writer's line breaks."""
    if tag is None:
        return ""
    for br in tag.find_all("br"):
        br.replace_with("\n")
    lines = [one_line(line) for line in tag.get_text().split("\n")]
    return "\n".join(line for line in lines if line)


def iso_date(value: str) -> str:
    match = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", value or "")
    if not match:
        return ""
    try:
        return datetime(int(match.group(3)), int(match.group(1)), int(match.group(2))).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def compliance_pair(value: str) -> Optional[Tuple[int, int]]:
    match = re.search(r"(\d+)\s*/\s*(\d+)", value or "")
    return (int(match.group(1)), int(match.group(2))) if match else None


def rule_label(value: str) -> str:
    """"He-C.4001.15.(ag)," -> "He-C 4001.15(ag)", the way the rule is cited."""
    value = one_line(value).strip(" ,;")
    value = re.sub(r"^(He-[A-Z])\.\s*", r"\1 ", value)
    value = re.sub(r"\.\s*\(", "(", value)
    # The state now and then leaves "He-C" off ("4001.23(b)(3)").
    return "He-C " + value if re.match(r"^4001\.\d", value) else value


def split_rule(name: str) -> Tuple[str, str]:
    """"He-C.4001.15.(ag) : All medications ..." -> (rule, what it requires)."""
    name = one_line(name)
    head, sep, rest = name.partition(" : ")
    if sep and len(head) <= 60:
        return rule_label(head), rest.strip()
    return "", name


def parse_program(page_html: str) -> Dict[str, Any]:
    """The licensing history table of a program page, in page order."""
    soup = BeautifulSoup(page_html, "html.parser")
    name_tag = soup.select_one("h1 span.fontStyleName")
    visits: List[Dict[str, Any]] = []
    unparsed: List[str] = []
    table = None
    for candidate in soup.find_all("table"):
        heads = [one_line(th.get_text(" ")) for th in candidate.find_all("th")]
        if heads[:3] == ["Review Date", "Type of Review", "Level of Compliance"]:
            table = candidate
            break
    rows = table.tbody.find_all("tr", recursive=False) if table is not None and table.tbody else []
    for tr in rows:
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 5:
            if one_line(tr.get_text(" ")):
                unparsed.append(f"history row with {len(cells)} cells: {one_line(tr.get_text(' '))[:80]!r}")
            continue
        date_text = one_line(cells[0].get_text(" "))
        match = VISIT_ID.search(str(cells[3]))
        documents = [{"url": a["href"], "text": one_line(a.get_text(" "))}
                     for a in cells[4].find_all("a", href=True)]
        visit = {
            "visit_id": match.group(1) if match else "",
            "date_text": date_text,
            "date": iso_date(date_text),
            "visit_type": one_line(cells[1].get_text(" ")),
            "compliance": compliance_pair(cells[2].get_text(" ")),
            "documents": documents,
        }
        if not visit["date"]:
            unparsed.append(f"history row with an unreadable date: {one_line(tr.get_text(' '))[:80]!r}")
            continue
        if not visit["visit_id"] and not documents:
            unparsed.append(f"history row {date_text} {visit['visit_type']}: no detail and no document")
            continue
        visits.append(visit)
    return {
        "name": one_line(name_tag.get_text(" ")) if name_tag else "",
        "has_table": table is not None,
        "visits": visits,
        "unparsed": unparsed,
    }


def header_value(soup, label: str) -> str:
    tag = soup.find("b", string=re.compile(r"^\s*%s\s*$" % re.escape(label)))
    if tag is None or tag.parent is None or tag.parent.parent is None:
        return ""
    cell = tag.parent.parent
    return one_line(cell.get_text(" ").replace(tag.get_text(), "", 1))


def parse_visit(detail_html: str) -> Dict[str, Any]:
    """One visit's detail: header, domains, every rule reviewed. `problems`
    lists anything the parser could not account for."""
    soup = BeautifulSoup(detail_html, "html.parser")
    root = soup.find(id=re.compile(r"visiItemDetail$")) or soup
    problems: List[str] = []

    heading = root.find("div", class_="boldLabel")
    kind = one_line(heading.get_text(" ")) if heading else ""
    announced = ""
    if heading is not None and heading.parent is not None:
        announced = one_line(heading.parent.get_text(" ").replace(heading.get_text(), "", 1))

    domains: List[Dict[str, Any]] = []
    rules: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    last_rule: Optional[Dict[str, Any]] = None

    outer = None
    for candidate in root.find_all("table"):
        heads = [one_line(th.get_text(" ")) for th in candidate.find("thead").find_all("th")] \
            if candidate.find("thead") else []
        if heads[:2] == ["Domain Category", "Level of Compliance"]:
            outer = candidate
            break
    header = {
        "kind": kind,
        "announced": announced,
        "licensor": header_value(root, "Licensor Assigned"),
        "visit_date": iso_date(header_value(root, "Date of Visit")),
        "cap_accepted_date": iso_date(header_value(root, "Date Corrective Action Accepted")),
        "documents": detail_documents(root),
    }
    if outer is None or outer.tbody is None:
        return {**header, "domains": [], "rules": [], "problems": ["no domain table"]}

    for tr in outer.tbody.find_all("tr", recursive=False):
        name_cell = tr.find("td", attrs={"data-label": "Domain Category"}, recursive=False)
        if name_cell is not None:
            name_tag = name_cell.find(class_="categoryName")
            pair = compliance_pair((tr.find("td", attrs={"data-label": "Level of Compliance"}) or name_cell).get_text(" "))
            current = {"name": one_line((name_tag or name_cell).get_text(" ")),
                       "met": pair[0] if pair else None, "reviewed": pair[1] if pair else None,
                       "rows": 0, "not_met": 0}
            if pair is None:
                problems.append(f"domain {current['name']!r}: no compliance numbers")
            domains.append(current)
            continue
        if current is None:
            problems.append("rule rows before any domain")
            continue
        for row in tr.find_all("tr"):
            if "nonComplaintStatementClass" in (row.get("class") or []):
                if row.find_parent("tr", class_="nonComplaintStatementClass") is not None:
                    continue
                if last_rule is None:
                    problems.append(f"domain {current['name']!r}: a statement with no rule before it")
                    continue
                if last_rule["observations"] or last_rule["corrective_action_plan"]:
                    problems.append(f"{last_rule['rule']}: a second statement for one rule")
                    continue
                last_rule["observations"] = block_text(row.find("div", id="Non_Compliance"))
                last_rule["directed_cap"] = block_text(row.find("div", id="DirectedCap"))
                last_rule["corrective_action_plan"] = block_text(row.find("div", id="CorrectiveAction"))
                known = {"Observations", "Directed CAP", "Corrective Action Plan"}
                for cell in row.find_all("td", attrs={"data-label": True}):
                    if cell["data-label"] not in known:
                        problems.append(f"{last_rule['rule']}: statement column {cell['data-label']!r} not read")
                continue
            name_cell = row.find("td", attrs={"data-label": "Visit Item Name"}, recursive=False)
            if name_cell is None:
                continue
            first = name_cell.find("span")
            name = one_line((first or name_cell).get_text(" "))
            result_cell = row.find("td", attrs={"data-label": "Result"}, recursive=False)
            result = one_line(result_cell.get_text(" ")) if result_cell else ""
            rule, rule_text = split_rule(name)
            reg_cell = row.find("td", attrs={"data-label": "Associated Regulations"}, recursive=False)
            regulations = []
            if reg_cell is not None:
                for label in reg_cell.find_all("label", class_="ma__tooltip__open"):
                    cited = rule_label(label.get_text(" "))
                    if cited and cited not in regulations:
                        regulations.append(cited)
                # The tooltip repeats the rule; when the item name was cut
                # short (255 characters) the tooltip may hold more.
                for tip in reg_cell.select(".ma__tooltip__message p"):
                    tip_rule, tip_text = split_rule(tip.get_text(" "))
                    if tip_rule == rule and len(tip_text) > len(rule_text):
                        rule_text = tip_text
            # Since 2025 the item name is the rule's text alone and the rule
            # number is the regulation label; before, the name began with it.
            if not rule and regulations:
                rule = regulations[0]
            entry = {
                "rule": rule, "rule_text": rule_text, "domain": current["name"],
                "regulations": regulations,
                "high_risk": "High_Risk" in str(row),
                "result": result, "observations": "", "directed_cap": "", "corrective_action_plan": "",
            }
            if result not in KNOWN_RESULTS:
                problems.append(f"{rule or name[:40]}: result {result!r}")
            current["rows"] += 1
            current["not_met"] += not_met(result)
            rules.append(entry)
            last_rule = entry

    return {**header, "domains": domains, "rules": rules, "problems": problems}


# "Founded, Problem Resolved" is a complaint finding: the rule was broken and
# the program had already put it right, so no corrective action plan is asked
# for. The state counts it as not met in the level of compliance.
KNOWN_RESULTS = {"Compliant", "Non-Compliant", "Founded, Problem Resolved"}
DOCUMENT_LINK = re.compile(r"^https://[^/]+/sfc/p/[^/]+/a/[^/]+/[^/?#]+")


def not_met(result: str) -> bool:
    return one_line(result).lower() != "compliant"


def detail_documents(root) -> List[Dict[str, str]]:
    """The "Document Title" links under a visit's detail: the statement of
    findings the state generates for the visit, and anything else filed with it."""
    found = []
    for link in root.find_all("a", href=DOCUMENT_LINK):
        found.append({"url": link["href"], "text": one_line(link.get_text(" "))})
    return found


# ── Visit documents ──────────────────────────────────────────────────────────

# Nearly every visit with a finding has a "Statement of Findings" PDF under its
# detail (text, generated by the state's system; named CCRB-<licence>-Visit-
# <number>). It carries what the page leaves out: the licence number, the issue
# and due dates, each plan's completion date, the rule numbers of visits
# carried over from the previous system (blank on the page) and the
# observation behind a "Founded, Problem Resolved" complaint finding. A visit
# from the previous system may instead link a scanned, hand-completed statement.

OCR_DPI = 200
PAGE_FOOTER = re.compile(r"^\s*Page\s+\d+\s+of\s+\d+\s*$", re.I)
STATEMENT_MARK = re.compile(r"STATEMENT\s+OF\s+FINDINGS", re.I)
ITEM_START = re.compile(r"^Regulation\s+number\s*:\s*(.*)$", re.I)
ITEM_LABEL = re.compile(r"^(Observation|Directed CAP|Corrective Action Plan|Completion Date)\s*:\s*(.*)$")
SECTION_HEAD = re.compile(r"^(?:[IVX]+\.\s*)(Non-Compliance List|Founded\s*,\s*Problem Resolved)\s*$", re.I)
LABEL_KEYS = {"Observation": "observations", "Directed CAP": "directed_cap",
              "Corrective Action Plan": "corrective_action_plan", "Completion Date": "completion_date"}


def find_tesseract() -> str:
    if os.getenv("TESSERACT_CMD"):
        return os.getenv("TESSERACT_CMD")
    default = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    return str(default) if default.exists() else ""


def find_poppler() -> str:
    if os.getenv("POPPLER_PATH"):
        return os.getenv("POPPLER_PATH")
    found = sorted(Path("C:/tools").glob("poppler-*/Library/bin"), reverse=True) if Path("C:/tools").exists() else []
    return str(found[0]) if found else ""


def ocr_page(path: Path, number: int) -> str:
    try:
        import pytesseract
        from pdf2image import convert_from_path
    except ImportError:
        logger.warning(f"  {path.name} is a scan and pytesseract/pdf2image are not installed")
        return ""
    tesseract = find_tesseract()
    if tesseract:
        pytesseract.pytesseract.tesseract_cmd = tesseract
    try:
        images = convert_from_path(str(path), dpi=OCR_DPI, first_page=number, last_page=number,
                                   poppler_path=find_poppler() or None)
        return "\n".join(pytesseract.image_to_string(image) for image in images)
    except Exception as exc:  # Poppler or Tesseract missing or failing
        logger.warning(f"  OCR failed for {path.name} page {number}: {exc}")
        return ""


def extract_pdf(path: Path) -> Dict[str, Any]:
    """Each page's text; a page with no text layer is OCR'd."""
    import pdfplumber
    pages: List[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    ocr = []
    for index, text in enumerate(pages):
        if len(text.strip()) < 20:
            read = ocr_page(path, index + 1)
            if read.strip():
                pages[index] = read
                ocr.append(index + 1)
    return {"text": "\n".join(pages).strip(), "page_count": len(pages), "ocr_pages": ocr}


def statement_date(value: str) -> str:
    return iso_date(value) if re.search(r"\d{4}", value or "") else ""


def parse_statement(text: str) -> Dict[str, Any]:
    """A generated statement of findings: its header and every item."""
    lines = [one_line(line) for line in text.split("\n")]
    lines = [line for line in lines if line and not PAGE_FOOTER.match(line)]

    def field(label: str) -> str:
        match = re.search(r"^%s\s*:[ \t]*(.*)$" % label, "\n".join(lines), re.M | re.I)
        return one_line(match.group(1)) if match else ""

    items: List[Dict[str, Any]] = []
    section = ""
    current: Optional[Dict[str, Any]] = None
    key = ""
    for line in lines:
        head = SECTION_HEAD.match(line)
        if head:
            section = "Founded, Problem Resolved" if head.group(1).lower().startswith("founded") else "Non-Compliant"
            current = None
            continue
        start = ITEM_START.match(line)
        if start:
            current = {"rule": rule_label(start.group(1)), "result": section or "Non-Compliant",
                       "rule_text": [], "observations": [], "directed_cap": [],
                       "corrective_action_plan": [], "completion_date": []}
            items.append(current)
            key = "rule_text"
            continue
        if current is None:
            continue
        label = ITEM_LABEL.match(line)
        if label and (label.group(1) != "Corrective Action Plan" or key != "corrective_action_plan"):
            key = LABEL_KEYS[label.group(1)]
            line = label.group(2)
        if line:
            current[key].append(line)
    for item in items:
        for name in ("rule_text", "observations", "directed_cap", "corrective_action_plan", "completion_date"):
            item[name] = one_line(" ".join(item[name]))
        # Older statements repeat the rule number in front of its text.
        repeated, rest = split_rule(item["rule_text"])
        if repeated and repeated == item["rule"]:
            item["rule_text"] = rest
        item["completion_date"] = statement_date(item["completion_date"])
    return {
        "issue_date": statement_date(field("ISSUE DATE")),
        "visit_date": statement_date(field("VISIT DATE")),
        "visit_type": field("VISIT TYPE"),
        "cap_due_date": statement_date(field("CORRECTIVE ACTION PLAN DUE DATE")),
        "license_number": field(r"License No\.?"),
        "items": items,
    }


def text_key(value: str, length: int = 60) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())[:length]


def merge_statement(rules: List[Dict[str, Any]], statement: Dict[str, Any]) -> List[str]:
    """Fill what the page left blank from the statement's items: the rule
    number and text of a visit from the previous system, the observation of a
    founded complaint, the completion date. The page's own text always wins.
    Returns what could not be paired."""
    problems = []
    open_items = list(statement["items"])
    cited = [rule for rule in rules if not_met(rule["result"])]

    def take(test) -> Optional[Dict[str, Any]]:
        for item in open_items:
            if test(item):
                open_items.remove(item)
                return item
        return None

    pairs = []
    for rule in cited:
        seen_key = text_key(rule["observations"], 400)
        item = take(lambda i: seen_key and text_key(i["observations"], 400) == seen_key)
        pairs.append([rule, item])
    for pair in pairs:
        if pair[1] is None and pair[0]["rule"]:
            wanted = text_key(pair[0]["rule"])
            pair[1] = take(lambda i: text_key(i["rule"]) == wanted)
    for pair in pairs:
        if pair[1] is None and pair[0]["observations"]:
            seen_key = text_key(pair[0]["observations"])
            pair[1] = take(lambda i: text_key(i["observations"]) == seen_key)
    for pair in pairs:
        if pair[1] is None and pair[0]["rule_text"]:
            wanted = text_key(pair[0]["rule_text"], 40)
            pair[1] = take(lambda i: text_key(i["rule_text"], 40) == wanted)
    # One rule with no number left and one statement item left, for the same
    # visit: the page's narrative was revised after the statement was issued
    # (Pine Haven 2023-12-11), the two are one finding.
    lone = [pair for pair in pairs if pair[1] is None]
    if len(lone) == 1 and len(open_items) == 1 and not lone[0][0]["rule"]:
        lone[0][1] = take(lambda i: True)
    for rule, item in pairs:
        if item is None:
            problems.append(f"{rule['rule'] or 'a rule with no number'}: not found in the statement of findings")
            continue
        for name in ("rule", "rule_text", "observations", "directed_cap", "corrective_action_plan"):
            if not rule[name] and item[name]:
                rule[name] = item[name]
        rule["completion_date"] = item["completion_date"]
    for item in open_items:
        problems.append(f"statement item {item['rule']} is not on the page")
    return problems


RULE_SHAPE = re.compile(r"^He-[A-Z] \d{4}(?:\.\d+)?(?:\([A-Za-z0-9]{1,3}\))*$")


def document_name(visit_id: str, label: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", label).strip("_") or "document"
    if not safe.upper().startswith("CCRB-"):
        safe = f"{visit_id}_{safe}"
    return safe + ".pdf"



# ── Privacy ──────────────────────────────────────────────────────────────────

# The state writes "Staff A" and "Resident A". Anything that looks like a date
# of birth, a record number or a child's name holds the whole visit back.
CHILD_NOUN = r"(?:resident|child|youth|student|client|juvenile|minor|girl|boy|teen|adolescent|patient)"
NOT_A_NAME = {
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
    "U", "V", "W", "X", "Y", "Z", "The", "This", "That", "These", "Those", "Staff", "Program", "Director",
    "Executive", "Rights", "Handbook", "Care", "Services", "Records", "Record", "File", "Files", "Advocate",
    "Council", "Supervision", "Safety", "Treatment", "Plan", "Plans", "Policy", "Manual", "Welfare",
    "Protection", "Abuse", "And", "Or", "In", "On", "At", "Is", "Was", "Were", "Has", "Had", "Will", "Who",
    "To", "For", "With", "From", "By", "Of", "If", "When", "While", "After", "Before", "During", "Upon",
    "Did", "Not", "No", "Any", "All", "Each", "Their", "They", "It", "As", "Are", "Be", "Should", "Shall",
    "May", "Must", "Can", "Could", "Would", "Also", "An", "One", "Two", "Three", "Four", "Five", "Six",
    "DCYF", "DHHS", "CCLU", "NH", "MPA", "TCI", "CPR", "ID", "IEP", "ISP", "RN", "LNA", "MAR", "PRN",
    "Per", "Via", "Staffing", "Ratio", "Ratios", "Grievance", "Medication", "Medications", "Incident",
    "Management", "Development", "Behavior", "Behaviors", "Support", "Supports", "Center", "Home",
    "Academy", "School", "Unit", "Room", "Rooms", "Area", "Areas", "Interviews", "Interview", "Statements",
    "Life", "Counselor", "Counselors", "Advisor", "Advisors", "Bill", "Assistance",
    # Parts of a program's own name ("Oasis Teen Shelter", "Youth Villages").
    "Shelter", "Shelters", "House", "Houses", "Village", "Villages", "Ranch", "Campus", "Residence",
    "Residential", "Facility", "Facilities", "Hospital", "Programs",
}
# What a statement of findings prints around the findings: the running page
# footer (pdfplumber interleaves it into a sentence that crosses a page) and the
# program header, whose name may hold a child noun ("Teen Shelter").
PAGE_MARK = re.compile(r"\bPage\s+\d+\s+of\s+\d+\b", re.I)
HEADER_LINE = re.compile(
    r"^\s*(?:Name of the program|Program address|License No\.?|Licensing Coordinator)\s*:.*$", re.I | re.M)
PRIVACY_PATTERNS = [
    ("date of birth", re.compile(r"\b(?:d\.?\s?o\.?\s?b\b\.?|date\s+of\s+birth|birth\s?date|born\s+on)", re.I)),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b|\bSSN\b|social\s+security\s+(?:number|no)", re.I)),
    ("record number", re.compile(
        r"\b(?:medical\s+record|MRN|case|record|client|medicaid|file|intake|referral)\s*"
        r"(?:number|no\.?|#|id)\s*[:#]?\s*[A-Z]{0,3}-?\d{3,}", re.I)),
    ("child named", re.compile(r"\b%ss?\s*(?:\(|,)?\s*(?:named|name\s+is|known\s+as)\s+[A-Z]" % CHILD_NOUN, re.I)),
]
NAMED_CHILD = re.compile(r"\b(%s)\s+([A-Z][A-Za-z'.-]*)(?:\s+([A-Z][A-Za-z'.-]*))?" % CHILD_NOUN, re.I)


def privacy_hits(text: str) -> List[str]:
    """Reasons the text must not be public; [] when none."""
    text = HEADER_LINE.sub("", PAGE_MARK.sub(" ", text or ""))
    hits = []
    for label, pattern in PRIVACY_PATTERNS:
        match = pattern.search(text)
        if match:
            hits.append(f"{label}: {one_line(text[max(0, match.start() - 30):match.end() + 30])!r}")
    for match in NAMED_CHILD.finditer(text):
        first = match.group(2).strip(".'-")
        # "Resident A", "Resident A1", "Resident #2" and plain words are fine;
        # a capitalised word that is not a known label reads as a name.
        if not first or first in NOT_A_NAME or re.fullmatch(r"[A-Z]{1,2}\d*", first) or not first[0].isupper():
            continue
        if not re.fullmatch(r"[A-Z][a-z]+", first):
            continue
        # A sentence start ("... the resident. Staff ...") is not a name.
        between = text[match.start(1) + len(match.group(1)):match.start(2)]
        if "." in between:
            continue
        hits.append(f"possible child's name: {one_line(text[max(0, match.start() - 30):match.end() + 30])!r}")
        break
    return hits


# ── Reports ──────────────────────────────────────────────────────────────────


def plural(count: int, word: str, many: str = "") -> str:
    return f"{count} {word if count == 1 else (many or word + 's')}"


def visit_label(visit_type: str) -> str:
    """"Licensed Complaint Visit" -> "Complaint visit"."""
    label = re.sub(r"^Licen[sc]ed\s+", "", one_line(visit_type), flags=re.I) or "Licensing visit"
    if not re.search(r"\bvisit\b", label, re.I):
        label += " visit"
    return label[0].upper() + label[1:].lower()


def is_complaint(visit_type: str) -> bool:
    return "complaint" in (visit_type or "").lower()


def build_report(row: Dict[str, Any], detail: Dict[str, Any], account: str) -> Dict[str, Any]:
    items = [{
        "rule": rule["rule"],
        "rule_text": rule["rule_text"],
        "domain": rule["domain"],
        "result": rule["result"],
        "high_risk": rule["high_risk"],
        "observations": rule["observations"],
        "directed_cap": rule["directed_cap"],
        "corrective_action_plan": rule["corrective_action_plan"],
        "completion_date": rule.get("completion_date", ""),
    } for rule in detail["rules"] if not_met(rule["result"])]
    statement = detail.get("statement") or {}
    pair = row.get("compliance")
    if pair is None:
        pair = (sum(d["met"] or 0 for d in detail["domains"]), sum(d["reviewed"] or 0 for d in detail["domains"]))
    met, reviewed = pair
    visit_type = row["visit_type"] or (detail["kind"] + " Visit" if detail["kind"] else "")
    label = visit_label(visit_type)
    if items:
        cited = list(dict.fromkeys(item["rule"] for item in items if item["rule"]))
        shown = ", ".join(cited[:3]) + (f" and {len(cited) - 3} more" if len(cited) > 3 else "")
        summary = f"{label}: {len(items)} of {plural(reviewed, 'rule')} not met" + (f" ({shown})" if shown else "")
    elif not reviewed:
        summary = f"{label}: no rules reviewed"
    else:
        summary = f"{label}: all {plural(reviewed, 'rule')} reviewed were met"
    categories = {
        "visit_type": visit_type,
        "is_complaint": is_complaint(visit_type),
        "announced": detail["announced"],
        "compliance": {"met": met, "reviewed": reviewed},
        "licensor": detail["licensor"],
        "visit_date": detail["visit_date"],
        "cap_accepted_date": detail["cap_accepted_date"],
        "license_number": statement.get("license_number", ""),
        "issue_date": statement.get("issue_date", ""),
        "cap_due_date": statement.get("cap_due_date", ""),
        "domains": [{"name": d["name"], "met": d["met"], "reviewed": d["reviewed"]} for d in detail["domains"]],
        "items": items,
        "item_count": len(items),
        "resolved_count": sum(1 for item in items if item["result"] != "Non-Compliant"),
        "rules_reviewed": reviewed,
        "documents": detail.get("archived", []),
    }
    blocks = [f"{label} on {row['date']}" + (f" ({detail['announced'].lower()})" if detail["announced"] else "")]
    if not items:
        blocks.append(f"All {plural(reviewed, 'rule')} reviewed were met." if reviewed else "No rules reviewed.")
    for item in items:
        lines = [" ".join(p for p in (item["rule"], item["rule_text"]) if p)]
        lines.append("Domain: " + item["domain"])
        lines.append("Result: " + item["result"])
        if item["observations"]:
            lines.append("Observations: " + item["observations"])
        if item["directed_cap"]:
            lines.append("Directed corrective action: " + item["directed_cap"])
        if item["corrective_action_plan"]:
            lines.append("Corrective action plan: " + item["corrective_action_plan"])
        if item["completion_date"]:
            lines.append("Completion date: " + item["completion_date"])
        blocks.append("\n".join(lines))
    if detail["cap_accepted_date"]:
        blocks.append("Corrective action accepted: " + detail["cap_accepted_date"])
    text = "\n\n".join(blocks)
    return {
        "report_id": row["visit_id"],
        "report_date": row["date"],
        "report_url": PROGRAM_URL.format(account=account),
        "raw_content": text,
        "content_length": len(text),
        "summary": summary,
        "categories": categories,
    }


def is_flagged(report: Dict[str, Any]) -> bool:
    return report["categories"]["item_count"] > 0


def report_hash(report: Dict[str, Any]) -> str:
    """Fingerprint of what gets posted for a visit, to spot later edits."""
    return hashlib.sha1(json.dumps(report, sort_keys=True).encode("utf-8")).hexdigest()


def facility_info(row: Dict[str, Any], account: str, listed: bool, last_listed: str,
                  page_name: str = "") -> Dict[str, str]:
    street = one_line(str(row.get("ShippingStreet") or ""))
    city = one_line(str(row.get("ShippingCity") or ""))
    postal = one_line(str(row.get("ShippingPostalCode") or ""))
    tail = " ".join(p for p in ("NH", postal) if p) if (city or street) else ""
    address = ", ".join(p for p in (street, city, tail) if p)
    capacity = row.get("Capacity__c")
    if isinstance(capacity, float) and capacity.is_integer():
        capacity = int(capacity)
    if listed:
        action = one_line(str(row.get("License_Status__c") or "")) or "Listed"
    else:
        action = f"No longer listed (last seen {last_listed})" if last_listed else "No longer listed"
    return {
        "facility_name": one_line(str(row.get("Name") or "")) or page_name or account,
        "program_name": account,
        "program_category": PROGRAM_CATEGORY,
        "full_address": address,
        "phone": one_line(str(row.get("Phone") or "")),
        "bed_capacity": "" if capacity in (None, "") else str(capacity),
        "executive_director": "",
        "license_exp_date": "",
        "relicense_visit_date": "",
        "action": action,
    }


# ── Scraper ──────────────────────────────────────────────────────────────────


class NHScraper:
    def __init__(self, client: Optional[NHClient] = None, pages: Optional[PageStore] = None):
        self.client = client
        self.pages = pages
        self.stats: Counter = Counter()
        self.visit_types: Counter = Counter()
        self.kinds: Counter = Counter()
        self.items_per_visit: Counter = Counter()
        self.mismatches: List[str] = []
        self.unparsed: List[str] = []
        self.held_back: List[str] = []
        self.documents: List[str] = []
        self.results: Counter = Counter()
        self.store: Optional[ReportStore] = None

    # -- documents ---------------------------------------------------------

    def report_store(self) -> ReportStore:
        if self.store is None:
            self.store = ReportStore("NH_PDF_CACHE", "nh_pdfs", Path(__file__).parent / "nh_pdfs")
        return self.store

    def document(self, account: str, visit_id: str, where: str, link: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """What one visit document is: fetched once, read, classified, and
        archived for the site only when it is a statement of findings that
        passes the privacy check. Anything else is kept beside the saved pages
        (never in the public folder) and listed for the owner."""
        store = self.report_store()
        name = document_name(visit_id, link["text"])
        cache_key = name[:-4] + "__" + hashlib.sha1(link["url"].encode("utf-8")).hexdigest()[:10]
        cached = store.cached_extract(cache_key)
        if cached is not None:
            return self.recheck(account, cached)
        if self.client is None:
            self.stats["documents_not_cached"] += 1
            self.unparsed.append(f"{where}: document {link['text']!r} is not in the local cache (run live once)")
            return None
        try:
            data, title = self.client.visit_document(link["url"])
        except (requests.RequestException, ValueError, KeyError, IndexError, TypeError) as exc:
            logger.error(f"  document {link['text']!r} failed: {exc}")
            self.stats["document_errors"] += 1
            self.unparsed.append(f"{where}: document {link['text']!r} could not be downloaded ({exc})")
            return None
        self.stats["documents_downloaded"] += 1
        record: Dict[str, Any] = {"name": name, "title": title or link["text"], "bytes": len(data),
                                  "kind": "other", "text": "", "page_count": 0, "ocr_pages": [], "privacy": []}
        if data.startswith(b"%PDF"):
            try:
                with store.working_copy(data, name) as path:
                    record.update(extract_pdf(path))
            except Exception as exc:  # a PDF pdfplumber cannot open
                logger.warning(f"  could not read {name}: {exc}")
            if STATEMENT_MARK.search(record["text"]):
                record["kind"] = "scanned_statement" if record["ocr_pages"] else "statement"
            record["privacy"] = privacy_hits(record["text"])
        public = record["kind"] != "other" and not record["privacy"]
        try:
            if public:
                store.archive(name, data)
            elif self.pages is not None:
                folder = self.pages.base / account
                folder.mkdir(parents=True, exist_ok=True)
                (folder / ("held_" + name)).write_bytes(data)
        except OSError as exc:
            logger.error(f"  could not save {name}: {exc}")
            return record
        if record["text"]:
            store.save_extract(cache_key, record)
        return record

    def recheck(self, account: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """A cached extraction is judged again with the current privacy check
        (it may have been tightened since). A document the check now passes
        goes to the archive from the copy kept beside the saved pages; one it
        newly holds is taken out of the archive."""
        if record.get("kind") == "other" or not record.get("text"):
            return record
        record["privacy"] = privacy_hits(record["text"])
        store = self.report_store()
        archived = store.archive_dir / record["name"]
        if record["privacy"]:
            try:
                if archived.exists():
                    archived.unlink()
                    logger.warning(f"  removed {record['name']} from the archive folder (the privacy check holds it)")
            except OSError:
                pass
            return record
        try:
            if archived.exists():
                return record
            held = [self.pages.base / account / ("held_" + record["name"])] if self.pages is not None else []
            held = [h for h in held if h.exists()]
            if held:
                store.archive(record["name"], held[0].read_bytes())
                held[0].unlink()
                self.stats["documents_released"] += 1
                logger.info(f"  {record['name']} passes the privacy check now; archived")
            else:
                record["kind"] = "unarchived_" + record["kind"]
        except OSError as exc:
            logger.error(f"  could not archive {record['name']}: {exc}")
        return record

    def read_documents(self, account: str, where: str, visit: Dict[str, Any], detail: Dict[str, Any]) -> None:
        """Attach the visit's documents to its parsed detail: the generated
        statement fills the blanks, every public document is linked."""
        detail["archived"] = []
        links = list(visit["documents"])
        links += [d for d in detail.get("documents", []) if d["url"] not in {l["url"] for l in links}]
        candidates: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for link in links:
            self.stats["visit_documents"] += 1
            record = self.document(account, visit["visit_id"], where, link)
            if record is None:
                continue
            self.stats["documents_" + record["kind"]] += 1
            if record["kind"].startswith("unarchived_"):
                self.held_back.append(f"{where}: document {record['title']!r} passes the privacy check but has no "
                                      f"copy to archive (run live once); not linked")
                continue
            if record["kind"] == "other":
                self.stats["not_a_report"] += 1
                self.held_back.append(f"{where}: document {record['title']!r} is not a statement of findings "
                                      f"(not linked, kept beside the saved pages)")
                continue
            if record["privacy"]:
                self.stats["documents_privacy_held"] += 1
                self.held_back.append(f"{where}: document {record['title']!r}: privacy check: "
                                      + "; ".join(record["privacy"]))
                continue
            detail["archived"].append({"name": record["name"], "kind": record["kind"]})
            statement = parse_statement(record["text"])
            if record["kind"] == "scanned_statement":
                # OCR: only a well-formed rule number is taken from it.
                for item in statement["items"]:
                    if not RULE_SHAPE.match(item["rule"]):
                        item["rule"] = ""
            candidates.append((record, statement))
        # One statement fills the blanks: a generated (text) one with items
        # first, then a scan with items, then whatever is left.
        candidates.sort(key=lambda c: (c[0]["kind"] != "statement", not c[1]["items"]))
        if candidates:
            record, statement = candidates[0]
            detail["statement"] = statement
            if not statement["license_number"]:
                self.unparsed.append(f"{where}: statement {record['name']} has no licence number")
            for problem in merge_statement(detail["rules"], statement):
                self.stats["statement_unpaired"] += 1
                self.unparsed.append(f"{where}: {problem}")

    def withdraw_documents(self, detail: Dict[str, Any]) -> None:
        """A visit held back takes its archived documents with it."""
        for entry in detail.get("archived", []):
            try:
                (self.report_store().archive_dir / entry["name"]).unlink()
                logger.warning(f"  removed {entry['name']} from the archive folder (its visit is held back)")
            except OSError:
                pass

    # -- one program -------------------------------------------------------

    def take(self, account: str, row: Dict[str, Any], listed: bool, last_listed: str,
             history: List[Dict[str, Any]], details: Dict[str, str], page_name: str = "") -> Dict[str, Any]:
        """Build a facility from its history rows and the visit details read
        for them. Every decision to leave a visit out is recorded."""
        info = facility_info(row, account, listed, last_listed, page_name)
        name = info["facility_name"]
        reports = []
        self.stats["programs"] += 1
        for visit in history:
            self.stats["visits_listed"] += 1
            self.visit_types[visit["visit_type"] or "(empty)"] += 1
            where = f"{name} {visit['date']} {visit['visit_type']}"
            for document in visit["documents"]:
                self.documents.append(f"{where}: {document['text']}")
            if not visit["visit_id"]:
                self.stats["document_only_visits"] += 1
                self.held_back.append(f"{where}: no detail on the state page, only a visit document (not posted)")
                continue
            markup = details.get(visit["visit_id"])
            if markup is None:
                self.stats["detail_missing"] += 1
                self.unparsed.append(f"{where} ({visit['visit_id']}): detail not fetched")
                continue
            detail = parse_visit(markup)
            for problem in detail["problems"]:
                if problem != "no domain table" or visit.get("compliance") not in (None, (0, 0)):
                    self.unparsed.append(f"{where} ({visit['visit_id']}): {problem}")
            if not detail["domains"] and visit.get("compliance") != (0, 0):
                self.stats["detail_empty"] += 1
                self.unparsed.append(f"{where} ({visit['visit_id']}): no domains in the detail")
                continue
            self.read_documents(account, where, visit, detail)
            self.check(where, visit, detail)
            for rule in detail["rules"]:
                self.results[rule["result"]] += 1
            report = build_report(visit, detail, account)
            hits = privacy_hits(report["raw_content"])
            if hits:
                self.stats["privacy_held"] += 1
                self.held_back.append(f"{where} ({visit['visit_id']}): privacy check: " + "; ".join(hits))
                self.withdraw_documents(detail)
                continue
            self.kinds[detail["kind"] + (" / " + detail["announced"] if detail["announced"] else "")] += 1
            self.items_per_visit[report["categories"]["item_count"]] += 1
            self.stats["items"] += report["categories"]["item_count"]
            self.stats["high_risk_items"] += sum(1 for i in report["categories"]["items"] if i["high_risk"])
            self.stats["items_without_plan"] += sum(
                1 for i in report["categories"]["items"] if not i["corrective_action_plan"])
            reports.append(report)
        reports.sort(key=lambda r: (r["report_date"], r["report_id"]), reverse=True)
        return {"facility_info": info, "reports": reports}

    def check(self, where: str, visit: Dict[str, Any], detail: Dict[str, Any]) -> None:
        """Compliance numbers against the parsed rows: reviewed minus met must
        equal the non-compliant items, at the visit and in every domain."""
        cited = sum(d["not_met"] for d in detail["domains"])
        rows = sum(d["rows"] for d in detail["domains"])
        met = sum(d["met"] or 0 for d in detail["domains"])
        reviewed = sum(d["reviewed"] or 0 for d in detail["domains"])
        pair = visit.get("compliance")
        if pair is None:
            self.mismatches.append(f"{where}: the history row has no compliance numbers")
        elif pair != (met, reviewed):
            self.mismatches.append(f"{where}: history says {pair[0]} / {pair[1]}, domains add up to {met} / {reviewed}")
        if reviewed - met != cited:
            self.mismatches.append(f"{where}: reviewed minus met is {reviewed - met}, rows not met {cited}")
        if rows != reviewed:
            self.mismatches.append(f"{where}: {reviewed} rules reviewed, {rows} rule rows parsed")
        for domain in detail["domains"]:
            if domain["reviewed"] is None:
                continue
            if domain["reviewed"] - domain["met"] != domain["not_met"] or domain["rows"] != domain["reviewed"]:
                self.mismatches.append(
                    f"{where}: domain {domain['name']!r} says {domain['met']} / {domain['reviewed']}, "
                    f"parsed {domain['rows']} rows, {domain['not_met']} non-compliant")
        if detail["visit_date"] and detail["visit_date"] != visit["date"]:
            self.stats["date_differs"] += 1
            self.mismatches.append(f"{where}: detail says date of visit {detail['visit_date']}")
        for rule in detail["rules"]:
            if not_met(rule["result"]) and not rule["observations"]:
                self.mismatches.append(f"{where}: {rule['rule']} is {rule['result']} with no observations")
            if not_met(rule["result"]) and not rule["rule"]:
                self.mismatches.append(f"{where}: a {rule['result']} item has no rule number")

    # -- live --------------------------------------------------------------

    def fetch_details(self, account: str, page_html: str, history: List[Dict[str, Any]],
                      fingerprints: Dict[str, str], known: Dict[str, str]) -> Dict[str, str]:
        """Post each visit of one page load in turn. An answer without the
        detail, or with another visit's numbers, gets a fresh page and one
        more try."""
        details: Dict[str, str] = {}
        viewstate: Optional[Dict[str, str]] = None
        for visit in history:
            visit_id = visit["visit_id"]
            if not visit_id:
                continue
            markup = None
            for attempt in range(3):
                try:
                    answer = self.client.visit_detail(account, page_html, visit_id, viewstate)
                except (requests.RequestException, ValueError) as exc:
                    logger.error(f"  visit {visit_id} failed: {exc}")
                    self.stats["visit_errors"] += 1
                    break
                if is_visit_detail(answer) and self.answers(visit, answer):
                    markup = answer
                    viewstate = viewstate_fields(answer) or None
                    break
                logger.warning(f"  visit {visit_id}: no usable detail in the answer; loading the page again")
                self.stats["page_reloads"] += 1
                try:
                    page_html = self.client.program_page(account)
                except requests.RequestException as exc:
                    logger.error(f"  page reload failed: {exc}")
                    break
                viewstate = None
            if markup is None:
                continue
            details[visit_id] = markup
            key = f"{account}/{visit_id}"
            fingerprint = text_fingerprint(markup)
            if self.pages is not None:
                try:
                    if self.pages.save(account, f"visit_{visit_id}", markup, fingerprint, known.get(key)):
                        self.stats["visit_copies_saved"] += 1
                    fingerprints[key] = fingerprint
                except OSError as exc:
                    logger.error(f"  could not save the visit copy: {exc}")
        return details

    @staticmethod
    def answers(visit: Dict[str, Any], markup: str) -> bool:
        """True when the detail's domain totals are the history row's, so one
        visit's answer is never filed under another."""
        pair = visit.get("compliance")
        if pair is None:
            return True
        found = [compliance_pair(td.get_text(" ")) for td in BeautifulSoup(markup, "html.parser").find_all(
            "td", attrs={"data-label": "Level of Compliance"})]
        found = [p for p in found if p]
        if not found:
            return pair == (0, 0)
        return (sum(p[0] for p in found), sum(p[1] for p in found)) == pair

    def scrape_live(self, known: Dict[str, Dict], page_hashes: Dict[str, str], limit: int = 0,
                    only: Optional[set] = None) -> Tuple[List[Dict], Dict[str, Dict], Dict[str, str]]:
        """Fetch, save and parse every listed program plus every one the state
        file knows. Returns (facilities with all their reports, the program
        registry, page fingerprints)."""
        today = datetime.now().strftime("%Y-%m-%d")
        rows = self.client.listing()
        if not rows:
            raise RuntimeError(f"The list call returned no programs; check {SEARCH_PAGE} by hand")
        logger.info(f"{PROGRAM_TYPE}: {len(rows)} listed")
        self.stats["listed"] = len(rows)
        fingerprints: Dict[str, str] = {}
        if self.pages is not None:
            fingerprint = list_fingerprint(rows)
            try:
                self.pages.save("_list", "list", json.dumps(rows, ensure_ascii=False, indent=1), fingerprint,
                                page_hashes.get("_list"), extension="json")
                fingerprints["_list"] = fingerprint
            except OSError as exc:
                logger.error(f"could not save the list copy: {exc}")

        listed = {row["Id"]: row for row in rows}
        registry: Dict[str, Dict] = {}
        for account, row in listed.items():
            registry[account] = {**known.get(account, {}), **row, "last_listed": today}
        for account, record in known.items():
            registry.setdefault(account, dict(record))
        accounts = sorted(registry, key=lambda a: (one_line(str(registry[a].get("Name") or "")).lower(), a))
        if only:
            accounts = [a for a in accounts if a in only]
        if limit:
            accounts = accounts[:limit]

        facilities: List[Dict] = []
        for index, account in enumerate(accounts, start=1):
            record = registry[account]
            is_listed = account in listed
            logger.info(f"[{index}/{len(accounts)}] {account} {one_line(str(record.get('Name') or ''))}"
                        + ("" if is_listed else " [no longer listed]"))
            try:
                page_html = self.client.program_page(account)
            except requests.RequestException as exc:
                logger.error(f"  page failed: {exc}")
                self.stats["page_errors"] += 1
                continue
            if not is_program_page(page_html):
                # An account the state has withdrawn answers without the history form.
                logger.warning(f"  {account}: not a program page (withdrawn or an error page); not saved")
                self.stats["empty_pages"] += 1
                continue
            parsed = parse_program(page_html)
            if not parsed["has_table"]:
                self.unparsed.append(f"{account}: no licensing history table")
            for problem in parsed["unparsed"]:
                self.unparsed.append(f"{account}: {problem}")
            fingerprint = text_fingerprint(page_html)
            if self.pages is not None:
                try:
                    if self.pages.save(account, "program", page_html, fingerprint, page_hashes.get(account)):
                        self.stats["page_copies_saved"] += 1
                    fingerprints[account] = fingerprint
                except OSError as exc:
                    logger.error(f"  could not save the page copy: {exc}")
            details = self.fetch_details(account, page_html, parsed["visits"], fingerprints, page_hashes)
            facilities.append(self.take(account, record, is_listed, record.get("last_listed", ""),
                                        parsed["visits"], details, parsed["name"]))
        return facilities, registry, fingerprints

    # -- saved copies ------------------------------------------------------

    def scrape_saved(self, base: Path, known: Dict[str, Dict], limit: int = 0,
                     only: Optional[set] = None) -> List[Dict]:
        """Rebuild from the saved copies: the newest list, each program's
        history rows from every saved page (the newest copy of a row wins, so
        a visit the state no longer shows is still built) and the newest
        detail of each visit."""
        store = PageStore(base)
        rows: Dict[str, Dict[str, Any]] = {}
        newest_list = store.latest("_list", "list")
        list_date = ""
        if newest_list is not None:
            rows = {row["Id"]: row for row in json.loads(store.read(newest_list))}
            list_date = re.search(r"(\d{4}-\d{2}-\d{2})", newest_list.name).group(1)
        self.stats["listed"] = len(rows)
        folders = sorted(p.name for p in base.iterdir() if p.is_dir() and ACCOUNT_ID.match(p.name))
        records = {a: {**known.get(a, {}), **rows.get(a, {})} for a in folders}
        accounts = sorted(folders, key=lambda a: (one_line(str(records[a].get("Name") or "")).lower(), a))
        if only:
            accounts = [a for a in accounts if a in only]
        if limit:
            accounts = accounts[:limit]
        facilities = []
        for account in accounts:
            history: Dict[str, Dict[str, Any]] = {}
            page_name = ""
            for copy in reversed(store.copies(account, "program")):
                page_html = store.read(copy)
                if not is_program_page(page_html):
                    logger.warning(f"  {account}: {copy.name} is not a program page; skipped")
                    continue
                parsed = parse_program(page_html)
                page_name = page_name or parsed["name"]
                if not history:
                    for problem in parsed["unparsed"]:
                        self.unparsed.append(f"{account}: {problem}")
                for visit in parsed["visits"]:
                    history.setdefault(visit["visit_id"] or f"document-{visit['date']}", visit)
            details = {}
            for visit_id in [v for v in history if not v.startswith("document-")]:
                latest = store.latest(account, f"visit_{visit_id}")
                if latest is not None:
                    details[visit_id] = store.read(latest)
            record = records[account]
            is_listed = account in rows
            last_listed = list_date if is_listed else known.get(account, {}).get("last_listed", "")
            facilities.append(self.take(account, record, is_listed, last_listed,
                                        list(history.values()), details, page_name))
        return facilities

    # -- report ------------------------------------------------------------

    def print_stats(self, facilities: List[Dict], posted: List[Dict]) -> None:
        reports = [r for f in facilities for r in f["reports"]]
        dates = sorted(r["report_date"] for r in reports if r["report_date"])
        complaints = [r for r in reports if r["categories"]["is_complaint"]]
        logger.info("── New Hampshire run summary ──")
        logger.info(f"listed: {self.stats['listed']}; programs parsed: {self.stats['programs']} "
                    f"(page errors: {self.stats['page_errors']}, not program pages: {self.stats['empty_pages']}, "
                    f"page copies saved: {self.stats['page_copies_saved']}, "
                    f"visit copies saved: {self.stats['visit_copies_saved']}, "
                    f"page reloads: {self.stats['page_reloads']}, visit errors: {self.stats['visit_errors']})")
        logger.info(f"visits listed by the state: {self.stats['visits_listed']}; reports built: {len(reports)} "
                    f"(flagged: {sum(1 for r in reports if is_flagged(r))}, "
                    f"complaint visits: {len(complaints)}, of them flagged: "
                    f"{sum(1 for r in complaints if is_flagged(r))})")
        logger.info(f"programs with no visit: {sum(1 for f in facilities if not f['reports'])}")
        by_year = Counter(d[:4] for d in dates)
        if dates:
            logger.info(f"date range: {dates[0]} to {dates[-1]}; by year: {dict(sorted(by_year.items()))}")
        logger.info(f"visits per type: {dict(self.visit_types.most_common())}")
        logger.info(f"detail headings: {dict(self.kinds.most_common())}")
        logger.info(f"items per visit (items: visits): {dict(sorted(self.items_per_visit.items()))}")
        logger.info(f"non-compliant items: {self.stats['items']} (high risk: {self.stats['high_risk_items']}, "
                    f"no corrective action plan yet: {self.stats['items_without_plan']})")
        logger.info(f"results of every rule reviewed: {dict(self.results.most_common())}")
        logger.info(f"visit documents: {self.stats['visit_documents']} (downloaded this run: "
                    f"{self.stats['documents_downloaded']}, statements of findings: {self.stats['documents_statement']}, "
                    f"scanned statements: {self.stats['documents_scanned_statement']}, "
                    f"not a report: {self.stats['not_a_report']}, held by the privacy check: "
                    f"{self.stats['documents_privacy_held']}, errors: {self.stats['document_errors']}, "
                    f"not in the cache: {self.stats['documents_not_cached']})")
        for line in self.documents:
            logger.info(f"  on the history row: {line}")
        logger.info(f"compliance mismatches: {len(self.mismatches)}")
        for line in self.mismatches:
            logger.warning(f"  {line}")
        logger.info(f"unparsed: {len(self.unparsed)}")
        for line in self.unparsed:
            logger.warning(f"  {line}")
        logger.info(f"held back (not in the payload): {len(self.held_back)}")
        for line in self.held_back:
            logger.warning(f"  {line}")
        logger.info(f"to post: {len(posted)} facilities, {sum(len(f['reports']) for f in posted)} new or changed reports")


def select_changed(facilities: List[Dict], hashes: Dict[str, Dict[str, str]],
                   info_hashes: Dict[str, str]) -> Tuple[List[Dict], Dict[str, Dict[str, str]], Dict[str, str]]:
    """Keep visits that are new or changed, and facilities whose details
    changed even with no visit to post. Returns (to post, their visit hashes,
    their info hashes)."""
    posted, new_hashes, new_info = [], {}, {}
    for facility in facilities:
        account = facility["facility_info"]["program_name"]
        known = hashes.get(account, {})
        changed = [r for r in facility["reports"] if known.get(r["report_id"]) != report_hash(r)]
        info_hash = hashlib.sha1(json.dumps(facility["facility_info"], sort_keys=True).encode("utf-8")).hexdigest()
        if not changed and info_hashes.get(account) == info_hash:
            continue
        gone = sorted(set(known) - {r["report_id"] for r in facility["reports"]})
        if gone:
            logger.info(f"  {account}: {len(gone)} visits no longer shown by the state (kept on the site)")
        posted.append({"facility_info": facility["facility_info"], "reports": changed})
        new_hashes[account] = {r["report_id"]: report_hash(r) for r in changed}
        new_info[account] = info_hash
    return posted, new_hashes, new_info


def write_out(path: Path, facilities: List[Dict], timestamp: str) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = [{
        "facility_info": f["facility_info"],
        "reports": [{**r, "is_structured": True} for r in f["reports"]],
    } for f in facilities]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "NH",
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
        state="NH",
        scraped_timestamp=timestamp,
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape New Hampshire residential child care licensing visits")
    parser.add_argument("--full", action="store_true", help=f"Ignore the visit hashes in {STATE_FILE} and post everything")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N programs (by name)")
    parser.add_argument("--account", action="append", default=[], help="Only this account id (repeatable)")
    parser.add_argument("--out", type=Path, help="Write what the read API would return (every visit) to this JSON file")
    parser.add_argument("--from-saved", type=Path, metavar="DIR",
                        help="Rebuild from the saved copies in DIR (the nh_html folder); no state site requests")
    args = parser.parse_args()

    state = load_state(STATE_FILE)
    timestamp = datetime.now().isoformat(timespec="seconds")
    only = set(args.account) or None
    scraper = NHScraper()

    if args.from_saved:
        scraper.pages = PageStore(args.from_saved)  # where held documents are kept
        facilities = scraper.scrape_saved(args.from_saved, state.get("programs", {}), args.limit, only)
    else:
        scraper.client = NHClient()
        scraper.pages = PageStore()
        logger.info(f"Page copies go to {scraper.pages.base}")
        facilities, registry, fingerprints = scraper.scrape_live(
            state.get("programs", {}), state.get("pages", {}), args.limit, only)
        # Not tied to a post: the registry remembers account ids and the last
        # name seen, the fingerprints what was last saved.
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
        logger.info("No new or changed visits since last run")
        return
    if args.no_post:
        logger.info("Skipping API POST because --no-post was set; visit hashes not advanced")
        return
    if save_to_api(posted, timestamp):
        stored = state.setdefault("hashes", {})
        for account, by_id in new_hashes.items():
            stored.setdefault(account, {}).update(by_id)
        state.setdefault("info", {}).update(new_info)
        save_state(STATE_FILE, state)
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed -- visit hashes not advanced")


if __name__ == "__main__":
    main()
