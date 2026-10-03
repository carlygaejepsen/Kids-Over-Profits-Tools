"""
Virginia inspection scraper (two agencies, one payload, state = VA).

  --source vdss   Department of Social Services, children's residential
                  facility search. Plain GET pages: the list, one page per
                  facility, one page per inspection (inspector's comments and
                  the violations cited). About 19 facilities.
                  https://www.dss.virginia.gov/licensed-care/search-licensing-programs/childrens-residential-facility-search/

  --source dbhds  Department of Behavioral Health and Developmental Services,
                  Office of Licensing provider search (ASP.NET WebForms behind
                  an Azure gateway with a JavaScript challenge). A headless
                  Playwright Chromium passes the challenge once and hands its
                  clearance cookie and User-Agent to `requests`, which does
                  everything else; the browser runs again only on a 403.
                  One facility = one licensed service (licence 630-14-001).
                  One report = one inspection row or one investigation; the
                  finalized corrective action plan, when the state shows one,
                  is a generated PDF read column by column.
                  https://vadbhdsv7prod.glsuite.us/GLSuiteWeb/Clients/vadbhds/Public/ProviderSearch/ProviderSearchSearch.aspx

  --source all    Both (default).

Flags: --full, --no-post, --limit N (facilities per source), --out file.json
(what the read API would return, both sources in one file), --service-type
(repeatable, replaces the DBHDS service types searched), --licence (repeatable,
only this VDSS licence id or DBHDS licence number).

The DBHDS walk is long (about 140 services, a click per plan) and resumable:
every service page is saved gzipped beside the PDFs, every plan's extraction is
cached in .report_extract_cache/va_pdfs/, and a service read less than
VA_DBHDS_MAX_AGE_HOURS ago (default 20) with nothing left to fetch is not
visited again. A plan appears only once finalized, so rows without one are
asked again (at most weekly) for 18 months after their date.

Privacy: every text that would be published is checked for a date of birth, a
name beside "DOB", a record number, a social security number or a private
street address. A hit keeps the whole report out of the payload; held reports
are listed at the end of the run and in the --out file's scraping_notes.

State files: .va_vdss_state.json, .va_dbhds_state.json (advance only after a
successful post).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html as html_lib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

from inspection_api_client import post_facilities_to_api
from report_store import EXTRACT_CACHE_ROOT, ReportStore, extract_with_cache
from scraper_state import load_state, merge_new_ids, save_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
BASE_DIR = Path(__file__).resolve().parent
VDSS_STATE_FILE = Path(os.getenv("VA_VDSS_STATE_FILE", ".va_vdss_state.json"))
DBHDS_STATE_FILE = Path(os.getenv("VA_DBHDS_STATE_FILE", ".va_dbhds_state.json"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
REQUEST_GAP = 1.0

VDSS_URL = ("https://www.dss.virginia.gov/licensed-care/search-licensing-programs/"
            "childrens-residential-facility-search/")
VDSS_CACHE = EXTRACT_CACHE_ROOT / "va_vdss"
# An inspection page this old is read from the cache; newer ones are asked
# again, since the state edits a page for a while after posting it.
VDSS_REFETCH_DAYS = 120

DBHDS_HOST = "https://vadbhdsv7prod.glsuite.us"
DBHDS_SEARCH_URL = (DBHDS_HOST + "/GLSuiteWeb/Clients/vadbhds/Public/ProviderSearch/"
                    "ProviderSearchSearch.aspx")
DBHDS_PROFILE = Path(os.getenv(
    "VA_BROWSER_PROFILE",
    str(Path(os.environ.get("LOCALAPPDATA", str(BASE_DIR))) / "KidsOverProfits" / "va-browser-profile"),
))
DBHDS_MAX_AGE_HOURS = float(os.getenv("VA_DBHDS_MAX_AGE_HOURS", "20"))
DBHDS_RECHECK_DAYS = 7          # a row with no plan is not asked more often
DBHDS_RECHECK_MONTHS = 18       # ... and not at all this long after its date
DBHDS_SERVICE_CACHE = EXTRACT_CACHE_ROOT / "va_pdfs" / "_services"

# Service types searched (the dropdown's own labels), with the short label
# shown on the page.
DBHDS_SERVICE_TYPES: Dict[str, str] = {
    "MH Psychiatric Residential Treatment Facility (PRTF) Service for Children and Adolescents":
        "Psychiatric residential treatment facility (DBHDS)",
    "MH Residential Therapeutic Group Home Service for Children and Adolescents":
        "Therapeutic group home (DBHDS)",
    "MH Residential Crisis Stabilization Service for Children and Adolescents":
        "Residential crisis stabilization (DBHDS)",
    "SA Residential Clinically Managed Medium-Intensity Service - ASAM Level 3.5 for Children and Adolescents":
        "Substance use residential, ASAM 3.5 (DBHDS)",
    "SA Residential Clinically Managed Low-Intensity Service - ASAM Level 3.1 for Children and Adolescents":
        "Substance use residential, ASAM 3.1 (DBHDS)",
    "SA Medically Monitored Intensive Inpatient Service - ASAM Level 3.7 for Children and Adolescents":
        "Substance use inpatient, ASAM 3.7 (DBHDS)",
    "MH Inpatient Psychiatric Service for Children and Adolescents":
        "Inpatient psychiatric service (DBHDS)",
}

_REPORTS: Optional[ReportStore] = None


def report_store() -> ReportStore:
    """The PDF archive; resolved on first use (it may ask for Google Drive)."""
    global _REPORTS
    if _REPORTS is None:
        _REPORTS = ReportStore("VA_PDF_CACHE", "va_pdfs", BASE_DIR / "va_pdfs")
        logger.info(f"Plan PDFs are archived in {_REPORTS.archive_dir}")
    return _REPORTS


# ── Small helpers ────────────────────────────────────────────────────────────


def one_line(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def tidy(value: Any) -> str:
    """Trim each line, keep line breaks, collapse runs of blank lines."""
    text = html_lib.unescape(str(value or "")).replace("\r", "").replace(" ", " ")
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def iso_date(value: str) -> str:
    """'07/28/2026', '7/28/26', '07-28-2026', 'Feb. 28, 2028' -> '2026-07-28'; '' if unreadable."""
    text = one_line(value)
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", text)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000
        try:
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    m = re.search(r"([A-Za-z]{3})[a-z]*\.?\s+(\d{1,2}),\s*(\d{4})", text)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%b %d %Y").strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return ""


def plausible_dates(value: str) -> List[str]:
    """ISO dates in a comma-separated list, without mistyped years."""
    low, high = "2000-01-01", (datetime.now() + timedelta(days=366)).strftime("%Y-%m-%d")
    dates = [iso_date(x) for x in str(value or "").split(",")]
    return [d for d in dates if d and low <= d <= high]


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")


def shorten(text: str, limit: int = 240) -> str:
    flat = one_line(text)
    if len(flat) <= limit:
        return flat
    return re.sub(r"\s+\S*$", "", flat[: limit - 1]) + "…"


def content_hash(report: Dict) -> str:
    body = json.dumps({k: report.get(k) for k in ("report_date", "raw_content", "summary", "categories")},
                      sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]


# ── Privacy check ────────────────────────────────────────────────────────────

_STREET = (r"(?:Street|St|Road|Rd|Avenue|Ave|Drive|Dr|Lane|Ln|Court|Ct|Boulevard|Blvd|Circle|Cir|"
           r"Place|Pl|Terrace|Trail|Parkway|Pkwy|Highway|Hwy|Turnpike|Pike)")
PRIVACY_PATTERNS: List[Tuple[str, "re.Pattern"]] = [
    ("date of birth", re.compile(
        r"\b(?:D\.?O\.?B\.?|date\s+of\s+birth|birth\s*date|born(?:\s+on)?)\s*[:#-]?\s*\(?\s*"
        r"(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[A-Z][a-z]{2,8}\.?\s+\d{1,2},?\s+\d{4})", re.I)),
    ("name with DOB", re.compile(r"\b[A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+\s*[,(]?\s*D\.?O\.?B\b")),
    ("record number", re.compile(
        r"\b(?:MRN|medical\s+record\s+(?:number|no\.?|#)|Medicaid\s+(?:ID|number|no\.?|#)|"
        r"(?:client|patient|resident|case)\s+(?:ID|record)\s*(?:number|no\.?|#))\s*[:#]?\s*[A-Z]{0,3}\d{4,}", re.I)),
    ("social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("street address", re.compile(r"\b\d{2,5}\s+(?:[NSEW]\.?\s+)?(?:[A-Z][a-z]+\s+){1,3}" + _STREET + r"\b\.?")),
]


def privacy_hits(text: str, own_addresses: List[str]) -> List[str]:
    """Things in `text` that must not be public. A street address is only a
    hit when it is not the facility's or provider's own (house number
    compared)."""
    own_numbers = set()
    for address in own_addresses:
        own_numbers.update(re.findall(r"\b\d{2,5}\b", address or ""))
    hits = []
    for label, pattern in PRIVACY_PATTERNS:
        for m in pattern.finditer(text or ""):
            found = one_line(m.group(0))
            if label == "street address":
                number = re.match(r"\d+", found).group(0)
                if number in own_numbers:
                    continue
            hits.append(f"{label}: {found[:80]}")
    return hits


# ── VDSS ─────────────────────────────────────────────────────────────────────

# The standing paragraphs that end every inspector's comment (plans of
# correction, appeal rights, where the findings are posted). The first group
# is cut from the start of its line; the second from the phrase itself, since
# it can follow the one sentence that gives a complaint's outcome ("The
# evidence gathered during the investigation did not support the allegation").
VDSS_BOILERPLATE = re.compile(
    r"\n[^\n]*(?:The evidence gathered during the inspection|Compliance with all applicable regulations)"
    r"|\s*(?:The inspection summary will be posted to the|The department(?:'|’)s inspection findings are subject"
    r"|Please Note: A copy of the findings|The licensee should retain a copy of this document"
    r"|For more information about the VDSS Licensing Programs|Should you have any questions, please contact)", re.S)
VDSS_NO_PLAN = re.compile(r"^\s*Not available online", re.I)


class VDSSClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self._last = 0.0
        self.requests_made = 0

    def get(self, params: Dict[str, Any]) -> str:
        last_error: Optional[Exception] = None
        for attempt in range(3):
            wait = REQUEST_GAP - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            try:
                resp = self.session.get(VDSS_URL, params=params, timeout=90)
                self._last = time.time()
                self.requests_made += 1
                resp.raise_for_status()
                resp.encoding = resp.encoding or "utf-8"
                return resp.text
            except requests.RequestException as exc:
                self._last = time.time()
                last_error = exc
                logger.warning(f"  VDSS request failed ({exc}); attempt {attempt + 1} of 3")
                time.sleep(5 * (attempt + 1))
        raise RuntimeError(f"VDSS request failed: {last_error}")


def vdss_content(page: str) -> BeautifulSoup:
    """The page without the commented-out debugging blocks."""
    return BeautifulSoup(re.sub(r"<!--.*?-->", "", page, flags=re.S), "html.parser")


def vdss_parse_list(page: str) -> List[str]:
    ids = []
    for lic in re.findall(r"licenseId=(\d+)", page):
        if lic not in ids:
            ids.append(lic)
    if not ids:
        # Fallback: the raw search response the page carries in a comment.
        ids = list(dict.fromkeys(re.findall(r"&quot;licenseId&quot;:\s*(\d+)", page)))
    return ids


def _label_table(soup: BeautifulSoup) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"], recursive=False)
        if len(cells) == 2:
            label = one_line(cells[0].get_text(" ")).rstrip(":")
            if label and len(label) < 40:
                fields.setdefault(label, one_line(cells[1].get_text(" ")))
    return fields


def _heading_block(soup: BeautifulSoup) -> Tuple[str, str, str]:
    """(name, address, phone) from the h2 and the paragraph under it."""
    root = soup.find(class_="contentContainer") or soup
    h2 = None
    for candidate in root.find_all("h2"):
        if candidate.find_next_sibling("table") is not None or candidate.find_next("table") is not None:
            h2 = candidate
            break
    if h2 is None:
        return "", "", ""
    name = one_line(h2.get_text(" "))
    address, phone = "", ""
    p = h2.find_next_sibling("p")
    if p is not None:
        lines = [one_line(x) for x in p.get_text("\n").split("\n") if one_line(x)]
        phones = [x for x in lines if re.fullmatch(r"[()\d\s.-]{10,}", x)]
        phone = phones[0] if phones else ""
        address = ", ".join(x for x in lines if x not in phones)
    return name, address, phone


def tidy_address(address: str) -> str:
    """'330 East Leicester Street, WINCHESTER, VA 22601' -> city in title case."""
    def fix(match: "re.Match") -> str:
        return ", " + match.group(1).title() + ", VA"
    return re.sub(r",\s*([A-Z][A-Z .'-]+),\s*VA\b", fix, one_line(address))


def vdss_parse_facility(page: str, lic: str) -> Dict:
    soup = vdss_content(page)
    name, address, phone = _heading_block(soup)
    fields = _label_table(soup)
    inspections = []
    seen = set()
    for a in soup.find_all("a", href=re.compile(r"inspectionNumber=\d+")):
        number = re.search(r"inspectionNumber=(\d+)", a["href"]).group(1)
        if number in seen or "#" in a["href"]:
            continue
        tr = a.find_parent("tr")
        cells = [one_line(td.get_text(" ")) for td in tr.find_all("td")] if tr else []
        if len(cells) < 3:
            continue
        seen.add(number)
        inspections.append({
            "number": number,
            "dates": cells[0],
            "complaint": cells[1].upper().startswith("Y"),
            "violations": cells[2].upper().startswith("Y"),
        })
    return {
        "lic": lic,
        "name": name,
        "address": tidy_address(address),
        "phone": phone,
        "facility_type": fields.get("Facility Type", ""),
        "license_type": fields.get("License Type", ""),
        "expires": iso_date(fields.get("Expiration Date", "")),
        "administrator": fields.get("Administrator", ""),
        "capacity": fields.get("Capacity", ""),
        "ages": fields.get("Ages", ""),
        "inspector": re.sub(r":?\s*\(?\d{3}\)?[\s.-]*\d{3}[\s.-]*\d{4}.*$", "", fields.get("Inspector", "")).strip(),
        "inspections": inspections,
    }


def vdss_split_violation(description: str) -> Tuple[str, str]:
    """'Violation:\\n...\\nFindings:\\n1. ...' -> (violation, findings)."""
    text = tidy(description)
    text = re.sub(r"^\s*Violation\s*:\s*", "", text, flags=re.I)
    parts = re.split(r"\n\s*(?:Findings?|Evidence)\s*:?\s*(?:\n|(?=\d))", text, maxsplit=1, flags=re.I)
    if len(parts) == 2 and parts[0].strip():
        return parts[0].strip(), parts[1].strip()
    parts = re.split(r"\b(?:Findings?|Evidence)\s*:\s*", text, maxsplit=1)
    if len(parts) == 2 and parts[0].strip():
        return parts[0].strip(), parts[1].strip()
    # No label: the numbered list is the findings.
    parts = re.split(r"\n\s*(?=\(?1[.)]\s*\S)", text, maxsplit=1)
    if len(parts) == 2 and parts[0].strip():
        return parts[0].strip(), parts[1].strip()
    return text, ""


def vdss_parse_inspection(page: str) -> Dict:
    soup = vdss_content(page)
    root = soup.find(class_="inspection-details") or soup
    fields = _label_table(root)
    out: Dict[str, Any] = {
        "dates": fields.get("Inspection Dates", ""),
        "complaint": fields.get("Complaint Related", "").upper().startswith("Y"),
        "inspector": re.sub(r":?\s*\(?\d{3}\)?[\s.-]*\d{3}[\s.-]*\d{4}.*$", "", fields.get("Inspector", "")).strip(),
        "areas": [], "comments": "", "violations": [], "other": {}, "boilerplate_cut": False,
    }
    for h3 in root.find_all("h3"):
        title = one_line(h3.get_text(" "))
        chunks: List[Any] = []
        for sib in h3.find_next_siblings():
            if sib.name == "h3":
                break
            chunks.append(sib)
        if re.match(r"Areas Reviewed", title, re.I):
            for node in chunks:
                for li in node.find_all("li") or [node]:
                    for line in li.get_text("\n").split("\n"):
                        if one_line(line):
                            out["areas"].append(one_line(line))
        elif re.match(r"Comments", title, re.I):
            text = tidy("\n\n".join(node.get_text("\n") for node in chunks))
            cut = VDSS_BOILERPLATE.search("\n" + text)
            if cut:
                text = ("\n" + text)[: cut.start()].strip()
                out["boilerplate_cut"] = True
            out["comments"] = text
        elif re.match(r"Violations", title, re.I):
            for node in chunks:
                blocks = [node] if node.find("h4", recursive=False) else node.find_all(
                    lambda t: t.name == "div" and t.find("h4", recursive=False))
                for block in blocks:
                    violation: Dict[str, str] = {"standard": "", "description": "", "findings": "", "plan": ""}
                    section = ""
                    for child in block.find_all(["h4", "p"], recursive=False):
                        text = tidy(child.get_text("\n"))
                        if child.name == "h4":
                            m = re.match(r"Standard\s*:\s*(.*)", text, re.I | re.S)
                            if m:
                                violation["standard"] = one_line(m.group(1))
                                section = "description"
                            elif re.match(r"Plan of Correction", text, re.I):
                                section = "plan"
                            else:
                                section = "description"
                            continue
                        if section == "plan":
                            if not VDSS_NO_PLAN.match(text):
                                violation["plan"] = (violation["plan"] + "\n\n" + text).strip()
                        else:
                            text = re.sub(r"^Description\s*:\s*", "", text, flags=re.I)
                            violation["description"] = (violation["description"] + "\n\n" + text).strip()
                    if violation["standard"] or violation["description"]:
                        violation["description"], violation["findings"] = vdss_split_violation(
                            violation["description"])
                        if not violation["plan"]:
                            del violation["plan"]
                        out["violations"].append(violation)
        else:
            text = tidy("\n\n".join(node.get_text("\n") for node in chunks))
            if text:
                out["other"][title] = text
    return out


class VDSSScraper:
    def __init__(self) -> None:
        self.client = VDSSClient()
        self.stats: Dict[str, int] = {}
        self.holds: List[Dict] = []
        self.unparsed: List[str] = []

    def bump(self, key: str, n: int = 1) -> None:
        self.stats[key] = self.stats.get(key, 0) + n

    def listed(self) -> List[str]:
        page = self.client.get({"facilityName": "", "location": "", "zipCode": "", "page": 1,
                                "perPage": 100, "sort": "asc", "endpoint": "crf"})
        ids = vdss_parse_list(page)
        total = re.search(r"&quot;total&quot;:\s*(\d+)", page)
        if total and int(total.group(1)) > len(ids):
            logger.warning(f"VDSS list says {total.group(1)} facilities but {len(ids)} ids were read")
        return ids

    def inspection(self, lic: str, row: Dict) -> Optional[Dict]:
        """Parsed inspection, from the cache when the visit is old enough."""
        cache = VDSS_CACHE / f"{lic}_{row['number']}.html.gz"
        last = iso_date(row["dates"].split(",")[-1])
        old = bool(last) and last < (datetime.now() - timedelta(days=VDSS_REFETCH_DAYS)).strftime("%Y-%m-%d")
        page = ""
        if old and cache.exists():
            try:
                page = gzip.decompress(cache.read_bytes()).decode("utf-8")
            except (OSError, ValueError):
                page = ""
        if not page:
            page = self.client.get({"action": "inspection", "licenseId": lic,
                                    "inspectionNumber": row["number"], "endpoint": "crf"})
            self.bump("pages_fetched")
            # Only the inspection itself is kept: a parser fix then needs no
            # second download.
            main = re.search(r'<div class="inspection-details">.*?(?=<span id="d\.en\.|<div class="cookie-banner")',
                             page, re.S)
            if main:
                VDSS_CACHE.mkdir(parents=True, exist_ok=True)
                cache.write_bytes(gzip.compress(main.group(0).encode("utf-8")))
        parsed = vdss_parse_inspection(page)
        if not iso_date(parsed["dates"]) and not parsed["comments"] and not parsed["violations"]:
            return None
        return parsed

    def build_report(self, facility: Dict, row: Dict, parsed: Dict) -> Dict:
        lic = facility["lic"]
        dates = plausible_dates(parsed["dates"]) or plausible_dates(row["dates"])
        date_corrected = False
        if not dates:
            # The state mistyped the date ("04/10/0204"): take the visit date
            # the inspector's comments give.
            dates = plausible_dates(" , ".join(re.findall(
                r"\d{1,2}/\d{1,2}/\d{2,4}|[A-Z][a-z]{2,8}\.? \d{1,2}, \d{4}", parsed["comments"])))[:1]
            date_corrected = bool(dates)
        violations = parsed["violations"]
        complaint = bool(parsed["complaint"] or row["complaint"])
        kind = re.search(r"Type of inspection:\s*([^\n]{2,60})", parsed["comments"] or "", re.I) \
            or re.match(r"(?:An?\s+)?(?:unannounced\s+|onsite\s+|mandated\s+)*"
                        r"(monitoring|renewal|complaint|initial|focused)\s+inspection",
                        parsed["comments"] or "", re.I)
        categories: Dict[str, Any] = {
            "source": "vdss",
            "kind": "inspection",
            "complaint_related": complaint,
            "inspection_type": one_line(kind.group(1)).capitalize() if kind else "",
            "inspection_dates": dates,
            "inspector": parsed["inspector"],
            "areas_reviewed": parsed["areas"],
            "comments": parsed["comments"],
            "violations": violations,
            "violation_count": len(violations),
            "listed_with_violations": bool(row["violations"]),
        }
        if parsed["other"]:
            categories["other_sections"] = parsed["other"]
        if date_corrected:
            categories["date_corrected"] = True
            categories["date_as_published"] = one_line(parsed["dates"])
        pieces = []
        if parsed["comments"]:
            pieces.append("Comments:\n" + parsed["comments"])
        for title, text in parsed["other"].items():
            pieces.append(f"{title}:\n{text}")
        for v in violations:
            block = f"Standard: {v['standard']}\nViolation: {v['description']}"
            if v.get("findings"):
                block += f"\nFindings:\n{v['findings']}"
            if v.get("plan"):
                block += f"\nPlan of correction:\n{v['plan']}"
            pieces.append(block)
        raw = "\n\n".join(pieces)
        label = "Complaint inspection" if complaint else "Inspection"
        summary = (f"{label}: {len(violations)} violation{'s' if len(violations) != 1 else ''}"
                   if violations else f"{label}: no violations")
        return {
            "report_id": f"vdss-{row['number']}",
            "report_date": dates[-1] if dates else "",
            "report_url": f"{VDSS_URL}?action=inspection&licenseId={lic}&inspectionNumber={row['number']}&endpoint=crf",
            "raw_content": raw,
            "content_length": len(raw),
            "summary": summary,
            "categories": categories,
        }

    def scrape_facility(self, lic: str, known: Optional[Dict]) -> Optional[Dict]:
        page = self.client.get({"licenseId": lic, "endpoint": "crf"})
        facility = vdss_parse_facility(page, lic)
        if not facility["name"]:
            if known:
                logger.warning(f"  licence {lic} ({known.get('name')}) no longer answers")
            else:
                logger.warning(f"  licence {lic}: no facility on the page")
            self.bump("facility_pages_empty")
            return None
        reports = []
        for row in facility["inspections"]:
            parsed = self.inspection(lic, row)
            if not parsed:
                self.unparsed.append(f"VDSS {facility['name']} inspection {row['number']}")
                continue
            report = self.build_report(facility, row, parsed)
            if not report["report_date"]:
                self.unparsed.append(f"VDSS {facility['name']} inspection {row['number']} (no date)")
                continue
            if row["violations"] and not parsed["violations"]:
                self.unparsed.append(
                    f"VDSS {facility['name']} inspection {row['number']}: listed with violations, none read")
            hits = privacy_hits(report["raw_content"], [facility["address"]])
            if hits:
                self.holds.append({"source": "vdss", "facility": facility["name"], "licence": f"VDSS-{lic}",
                                   "report_id": report["report_id"], "report_date": report["report_date"],
                                   "url": report["report_url"], "hits": hits})
                logger.warning(f"  HELD (privacy) {report['report_id']}: {hits}")
                continue
            self.bump("reports")
            self.bump("flagged" if parsed["violations"] else "clean")
            if report["categories"]["complaint_related"]:
                self.bump("complaint")
            if parsed["boilerplate_cut"]:
                self.bump("boilerplate_cut")
            reports.append(report)
        reports.sort(key=lambda r: r["report_date"], reverse=True)
        info = {
            "facility_name": facility["name"],
            "program_name": f"VDSS-{lic}",
            "program_category": "Children's Residential Facility (VDSS)",
            "full_address": facility["address"],
            "phone": facility["phone"],
            "bed_capacity": facility["capacity"],
            "executive_director": facility["administrator"],
            "license_exp_date": facility["expires"],
            "relicense_visit_date": "",
            "action": facility["license_type"],
        }
        return {"facility_info": info, "reports": reports, "_meta": facility}


def run_vdss(args: argparse.Namespace, timestamp: str) -> Tuple[List[Dict], VDSSScraper]:
    state = load_state(VDSS_STATE_FILE)
    licences: Dict[str, Dict] = state.setdefault("licences", {})
    hashes: Dict[str, Dict[str, str]] = state.setdefault("hashes", {})
    scraper = VDSSScraper()
    listed = scraper.listed()
    logger.info(f"VDSS: {len(listed)} facilities listed")
    ids = list(listed) + [lic for lic in sorted(licences) if lic not in listed]
    if args.licence:
        ids = [lic for lic in ids if lic in args.licence or f"VDSS-{lic}" in args.licence]
    if args.limit:
        ids = ids[: args.limit]

    today = datetime.now().strftime("%Y-%m-%d")
    facilities: List[Dict] = []
    new_ids: Dict[str, List[str]] = {}
    new_hashes: Dict[str, Dict[str, str]] = {}
    for index, lic in enumerate(ids, start=1):
        try:
            facility = scraper.scrape_facility(lic, licences.get(lic))
        except Exception as exc:  # one facility must not end the run
            logger.error(f"[{index}/{len(ids)}] VDSS licence {lic}: {exc}")
            scraper.bump("facility_errors")
            continue
        if not facility:
            continue
        meta = facility.pop("_meta")
        logger.info(f"[{index}/{len(ids)}] {meta['name']} (VDSS-{lic}): {len(facility['reports'])} inspections"
                    + ("" if lic in listed else " [no longer listed]"))
        licences[lic] = {"name": meta["name"], "last_listed": today if lic in listed
                         else licences.get(lic, {}).get("last_listed", "")}
        fresh = [r for r in facility["reports"]
                 if args.full or hashes.get(lic, {}).get(r["report_id"]) != content_hash(r)]
        if not fresh:
            continue
        facilities.append({"facility_info": facility["facility_info"], "reports": fresh})
        new_ids[lic] = [r["report_id"] for r in fresh]
        new_hashes[lic] = {r["report_id"]: content_hash(r) for r in fresh}
    # The licence registry only remembers ids; it is not tied to a post.
    save_state(VDSS_STATE_FILE, state)

    if facilities and not args.no_post:
        if save_to_api(facilities, timestamp):
            merge_new_ids(state, new_ids)
            for lic, values in new_hashes.items():
                hashes.setdefault(lic, {}).update(values)
            save_state(VDSS_STATE_FILE, state)
            logger.info("VDSS: posted; state advanced")
        else:
            logger.error("VDSS: API save failed -- state not advanced")
    return facilities, scraper


# ── DBHDS: getting in ────────────────────────────────────────────────────────


def mint_clearance() -> Tuple[str, str, str, str]:
    """Pass the gateway's JavaScript challenge in a headless Chromium and
    return (cookie name, value, domain, User-Agent)."""
    from playwright.sync_api import sync_playwright

    DBHDS_PROFILE.mkdir(parents=True, exist_ok=True)
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (DBHDS_PROFILE / name).unlink()
        except OSError:
            pass
    started = time.time()
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(str(DBHDS_PROFILE), headless=True)
        try:
            page = context.pages[0] if context.pages else context.new_page()
            # A stale clearance in the profile would be reused; the challenge
            # is cheap, so always start clean.
            context.clear_cookies()
            page.goto(DBHDS_SEARCH_URL, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_selector("#ContentPlaceHolder1_ddlServiceType", timeout=90000)
            agent = page.evaluate("navigator.userAgent")
            cookies = [c for c in context.cookies() if c["name"] == "appgw_azwaf_jsclearance"]
        finally:
            context.close()
    if not cookies:
        raise RuntimeError("DBHDS: the browser got in but no appgw_azwaf_jsclearance cookie was set")
    cookie = cookies[0]
    logger.info(f"DBHDS: clearance cookie minted in {time.time() - started:.0f} s")
    return cookie["name"], cookie["value"], cookie["domain"], agent


class Page:
    def __init__(self, resp: requests.Response):
        self.url = resp.url
        self.text = resp.text
        self.soup = BeautifulSoup(resp.text, "html.parser")

    def kind(self) -> str:
        form = self.soup.find("form")
        action = (form.get("action") if form else "") or ""
        for name in ("InvestigationDetails", "ServiceDetails", "SearchDetails", "SearchResults", "SearchSearch"):
            if name in action:
                return name
        return ""

    def form(self, extra: Dict[str, str]) -> Dict[str, str]:
        data: Dict[str, str] = {}
        for node in self.soup.find_all("input"):
            name = node.get("name")
            if name and node.get("type") in ("hidden", "text"):
                data[name] = node.get("value", "")
        for select in self.soup.find_all("select"):
            if select.get("name"):
                option = select.find("option", selected=True)
                if option is None:
                    data[select["name"]] = ""
                else:
                    data[select["name"]] = option.get("value") if option.get("value") is not None else option.get_text()
        data.update(extra)
        return data

    def action(self) -> str:
        return urljoin(self.url, self.soup.find("form").get("action"))


class Lost(Exception):
    """The site answered with a page other than the one the step leads to."""


class DBHDSSite:
    def __init__(self) -> None:
        self.session: Optional[requests.Session] = None
        self._last = 0.0
        self.requests_made = 0
        self.mints = 0
        self.provider_addresses: Dict[str, str] = {}

    def mint(self) -> None:
        name, value, domain, agent = mint_clearance()
        if self.session is None:
            self.session = requests.Session()
        self.session.headers["User-Agent"] = agent
        self.session.cookies.set(name, value, domain=domain)
        self.mints += 1

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        if self.session is None:
            self.mint()
        last_error: Optional[Exception] = None
        for attempt in range(4):
            wait = REQUEST_GAP - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            try:
                resp = self.session.request(method, url, timeout=120, **kwargs)
            except requests.RequestException as exc:
                self._last = time.time()
                last_error = exc
                logger.warning(f"  DBHDS request failed ({exc}); attempt {attempt + 1} of 4")
                time.sleep(10 * (attempt + 1))
                continue
            self._last = time.time()
            self.requests_made += 1
            if resp.status_code == 403:
                logger.info("  DBHDS answered 403: minting a new clearance cookie")
                self.mint()
                continue
            if resp.status_code >= 500 and attempt < 3:
                last_error = RuntimeError(f"HTTP {resp.status_code}")
                time.sleep(10 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp
        raise RuntimeError(f"DBHDS request failed: {last_error or '403 after a fresh cookie'}")

    def get_form(self) -> Page:
        return Page(self.request("GET", DBHDS_SEARCH_URL))

    def submit(self, page: Page, extra: Dict[str, str]) -> Page:
        return Page(self.request("POST", page.action(), data=page.form(extra)))

    def search(self, service_type: str) -> Page:
        form = self.get_form()
        select = form.soup.find("select", id="ContentPlaceHolder1_ddlServiceType")
        if select is None:
            raise Lost("search form without the Service Type list")
        values = {one_line(o.get_text()): (o.get("value") if o.get("value") is not None else o.get_text())
                  for o in select.find_all("option")}
        if service_type not in values:
            raise KeyError(service_type)
        page = self.submit(form, {select["name"]: values[service_type],
                                  "ctl00$ContentPlaceHolder1$btnSubmit": "Submit"})
        if page.kind() == "SearchSearch" and "did not return any results" in page.text:
            return page  # no licences of this type today; parse_results() finds no rows
        if page.kind() != "SearchResults":
            raise Lost(f"search for {service_type!r} landed on {page.kind() or page.url}")
        return page

    def service_types(self) -> List[str]:
        form = self.get_form()
        select = form.soup.find("select", id="ContentPlaceHolder1_ddlServiceType")
        return [one_line(o.get_text()) for o in select.find_all("option")] if select else []

    def open_service(self, service_type: str, provider_number: str, service_code: str) -> Page:
        """Search, open the provider, open the service licence."""
        results = self.search(service_type)
        button = None
        for row in parse_results(results):
            if row["provider_number"] == provider_number:
                button = row
                break
        if button is None:
            raise Lost(f"provider {provider_number} is not in the results for {service_type!r}")
        details = self.submit(results, {button["button_name"]: button["button_value"]})
        if details.kind() != "SearchDetails":
            raise Lost(f"provider {provider_number} landed on {details.kind() or details.url}")
        self.provider_addresses[provider_number] = parse_provider_address(details)
        for node in details.soup.find_all("input", type="submit"):
            if "dtgProviderDetail" in (node.get("name") or "") and node.get("value") == service_code:
                page = self.submit(details, {node["name"]: node["value"]})
                if page.kind() != "ServiceDetails":
                    raise Lost(f"service {service_code} landed on {page.kind() or page.url}")
                return page
        raise Lost(f"provider {provider_number} has no service {service_code}")

    def pdf_from(self, page: Page) -> Optional[bytes]:
        """The plan PDF a "View CAP" response points at, or None.

        The response is the same page plus a script opening
        SubmitOpen.aspx?fileName=<url of a temporary PDF>. Investigation plans
        come with backslashes in that URL ("/UI\\Common\\Report\\<guid>.pdf"):
        a browser turns them into slashes, requests would send %5C and get a
        500, so they are turned here.
        """
        m = re.search(r"fileName=([^\"'&)<>\s]+?\.pdf)", page.text, re.I)
        if not m:
            return None
        url = unquote(m.group(1)).replace("\\", "/")
        if not url.lower().startswith(DBHDS_HOST.lower() + "/"):
            logger.warning(f"  plan URL on another host, not fetched: {url[:80]}")
            return None
        resp = self.request("GET", url)
        return resp.content if resp.content.startswith(b"%PDF") else None


# ── DBHDS: page parsers ──────────────────────────────────────────────────────


def _span(soup: BeautifulSoup, ident: str, sep: str = " ") -> str:
    node = soup.find(id=f"ContentPlaceHolder1_{ident}")
    if node is None:
        return ""
    return ", ".join(one_line(x) for x in node.get_text("\n").split("\n") if one_line(x)) if sep == "," \
        else one_line(node.get_text(" "))


def _grid(soup: BeautifulSoup, ident: str) -> List[Tuple[List[str], Any]]:
    table = soup.find("table", id=f"ContentPlaceHolder1_{ident}")
    rows = []
    if table is not None:
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if cells:
                rows.append(([one_line(td.get_text(" ")) for td in cells], tr.find("input", type="submit")))
    return rows


def parse_results(page: Page) -> List[Dict]:
    rows = []
    for cells, button in _grid(page.soup, "dtgProviderSearchResults"):
        if button is None or len(cells) < 8:
            continue
        rows.append({
            "provider": one_line(button.get("value")),
            "button_name": button["name"], "button_value": button.get("value", ""),
            "provider_number": cells[1], "status": cells[2], "service_code": cells[3],
            "location": cells[4], "city": cells[5], "zip": cells[6], "region": cells[7],
        })
    return rows


def parse_service(page: Page) -> Dict:
    soup = page.soup
    inspections = []
    for index, (cells, button) in enumerate(_grid(soup, "dtgInspections")):
        inspections.append({"date": iso_date(cells[0]), "purpose": cells[1] if len(cells) > 1 else "",
                            "button": button["name"] if button is not None else "", "row": index})
    investigations = []
    for cells, button in _grid(soup, "dtgInvestigations"):
        if len(cells) >= 3:
            investigations.append({"received": iso_date(cells[0]), "number": cells[1], "closed": iso_date(cells[2]),
                                   "button": button["name"] if button is not None else ""})
    return {
        "provider": _span(soup, "lblProvName"),
        "contact": _span(soup, "lblACName"),
        "phone": _span(soup, "lblACPhone"),
        "service": _span(soup, "lblProgName"),
        "licence": _span(soup, "lblLicenseNumber"),
        "licensed_as": _span(soup, "lblLicensedAs"),
        "effective": iso_date(_span(soup, "lblEffectiveDate")),
        "expires": iso_date(_span(soup, "lblExpirationDate")),
        "stipulations": _span(soup, "lblStipulations"),
        "status": _span(soup, "lblLicenseStatus"),
        "license_type": _span(soup, "lblLicenseType"),
        "locations": [{"name": c[0], "city": c[1] if len(c) > 1 else "", "zip": c[2] if len(c) > 2 else ""}
                      for c, _ in _grid(soup, "dtgLocations")],
        "inspections": inspections,
        "investigations": investigations,
    }


def parse_provider_address(page: Page) -> str:
    return _span(page.soup, "lblAdd", ",")


def assign_report_ids(inspections: List[Dict]) -> None:
    """insp-<YYYYMMDD>-<purpose>; '-2' on a clash, counted from the bottom of
    the state's list so an id does not move when a row is added on top."""
    used: Dict[str, int] = {}
    for row in reversed(inspections):
        base = f"insp-{(row['date'] or 'undated').replace('-', '')}-{slug(row['purpose']) or 'inspection'}"
        used[base] = used.get(base, 0) + 1
        row["report_id"] = base if used[base] == 1 else f"{base}-{used[base]}"


# ── DBHDS: corrective action plan PDFs ───────────────────────────────────────

CAP_COLUMNS = ("standard", "comp", "noncompliance", "actions", "planned")
COMP_LABELS = {"N": "Non compliance", "NS": "Non compliance, systemic", "C": "Substantial compliance",
               "ND": "Non determined"}
BULLET = re.compile(r"^(?:[•◦▪‣·*-]|\(?\d{1,2}[.)]|\(?[a-z][.)])\s")


def _column_text(crop) -> str:
    """Text of one table cell: wrapped lines joined, a new paragraph at a
    vertical gap or a bullet."""
    try:
        lines = crop.extract_text_lines(layout=False, strip=True, return_chars=False)
    except Exception:
        text = crop.extract_text() or ""
        return html_lib.unescape(text).strip()
    out: List[str] = []
    previous_bottom: Optional[float] = None
    previous_height = 0.0
    previous_right = 0.0
    left, _, right, _ = crop.bbox
    short = right - 0.3 * (right - left)   # a line ending left of this was not wrapped
    for line in lines:
        text = html_lib.unescape(line["text"]).strip()
        if not text:
            continue
        height = line["bottom"] - line["top"]
        gap = (line["top"] - previous_bottom) if previous_bottom is not None else 0.0
        new_paragraph = previous_bottom is None or gap > 0.55 * max(height, previous_height) or BULLET.match(text) \
            or re.match(r"^(?:PR\)|OLR\))", text) or previous_right < short
        if new_paragraph or not out:
            out.append(text)
        else:
            out[-1] += " " + text
        previous_bottom, previous_height, previous_right = line["bottom"], height, line["x1"]
    return "\n".join(out)


def extract_cap(path: Path) -> Dict:
    """Read a plan PDF: its header fields, the plain text, and the table as
    cells. The table's rows are boxes that run across page breaks (their rules
    are drawn beyond the page edge), so each page gives one segment per box,
    marked as a continuation when the box began on an earlier page."""
    import pdfplumber

    segments: List[Dict] = []
    texts: List[str] = []
    problems: List[str] = []
    table_columns: Optional[List[float]] = None   # the citation table's rules, from its first box
    with pdfplumber.open(str(path)) as pdf:
        page_count = len(pdf.pages)
        for number, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            texts.append(text)
            words = page.extract_words()
            header = [w for w in words if w["text"].startswith("Standard(s)")]
            if not header:
                if number == 1:
                    problems.append("no table header on page 1")
                continue
            body_top = header[0]["bottom"] + 4
            boxes: Dict[Tuple[int, int], List[float]] = {}
            for line in page.lines:
                if abs(line["x0"] - line["x1"]) < 1 and line["bottom"] - line["top"] > 8:
                    boxes.setdefault((round(line["top"]), round(line["bottom"])), []).append(line["x0"])
            page_segments = []
            for (top, bottom), xs in boxes.items():
                columns: List[float] = []
                for x in sorted(xs):
                    if not columns or x - columns[-1] > 3:
                        columns.append(x)
                if len(columns) != 6:
                    continue
                if table_columns is None:
                    table_columns = columns
                elif any(abs(x - y) > 3 for x, y in zip(columns, table_columns)):
                    continue   # the signature box under the table
                clip_top, clip_bottom = max(top, body_top), min(bottom, page.height)
                if clip_bottom - clip_top < 4:
                    continue
                cells = []
                for i in range(5):
                    crop = page.crop((columns[i] + 0.5, clip_top, columns[i + 1] - 0.5, clip_bottom))
                    cells.append(_column_text(crop))
                if not any(cells):
                    continue
                page_segments.append({"page": number, "top": clip_top, "cont": top < body_top - 2, "cols": cells})
            page_segments.sort(key=lambda s: s["top"])
            for segment in page_segments:
                del segment["top"]
            segments.extend(page_segments)
    full = "\n".join(texts)

    def field(pattern: str) -> str:
        m = re.search(pattern, full)
        return one_line(m.group(1)) if m else ""

    return {
        "text": full,
        "pages": page_count,
        "is_cap": "CORRECTIVE ACTION PLAN" in full,
        "licence": field(r"License #:\s*([\w-]+)"),
        "inspection_date": iso_date(field(r"Date of Inspection:\s*([\d/-]+)")),
        "investigation_id": field(r"Investigation ID:\s*(\S+)"),
        "organization": field(r"Organization Name:\s*(.*?)\s*Program Type/Facility Name:"),
        "program": field(r"Program Type/Facility Name:\s*([^\n]*)"),
        "segments": segments,
        "problems": problems,
    }


STANDARD_CODE = re.compile(
    r"^(\d{1,2}\s?VAC\s?\d+-\d+-\d+\.?(?:\s*(?:[A-Z]{1,2}\.|\(\d+\)|\d+\.|[a-z]\.|\([a-z]\)))*)\s*(?:-\s*)?(.*)$", re.S)


def split_actions(text: str) -> Tuple[str, str, str, str]:
    """(provider's answers, Office of Licensing responses, last status, its date)."""
    provider: List[str] = []
    licensing: List[str] = []
    current = provider
    for line in text.split("\n"):
        if re.match(r"^OLR\)", line):
            current = licensing
            current.append(line)
        elif re.match(r"^PR\)", line):
            current = provider
            current.append(line)
        else:
            current.append(line)
    status, when = "", ""
    for line in licensing:
        m = re.match(r"^OLR\)\s*(Partially Accepted|Not Accepted|Accepted)\b\s*(\d{1,2}/\d{1,2}/\d{2,4})?", line, re.I)
        if m:
            status, when = m.group(1).title(), iso_date(m.group(2) or "")
    return "\n".join(provider).strip(), "\n".join(licensing).strip(), status, when


def parse_citations(segments: List[Dict]) -> List[Dict]:
    rows: List[List[str]] = []
    for segment in segments:
        cells = segment["cols"]
        if segment["cont"] and rows:
            for i in range(5):
                if cells[i]:
                    rows[-1][i] = (rows[-1][i] + "\n" + cells[i]).strip()
        else:
            rows.append(list(cells))
    citations = []
    for cells in rows:
        standard_text = one_line(cells[0])
        m = STANDARD_CODE.match(standard_text)
        standard = one_line(m.group(1)) if m else ""
        requirement = one_line(m.group(2)) if m else standard_text
        comp = one_line(cells[1]).upper()
        noncompliance = cells[2].strip()
        location = ""
        split = re.split(r"This regulation was NOT MET as evidenced by:\s*", noncompliance, maxsplit=1)
        if len(split) == 2:
            location, noncompliance = one_line(split[0]), split[1].strip()
        provider, licensing, status, when = split_actions(cells[3])
        citations.append({
            "standard": standard,
            "standard_text": requirement,
            "comp": comp,
            "location": location,
            "noncompliance": noncompliance,
            "provider_action": provider,
            "licensing_response": licensing,
            "response_status": status,
            "response_date": when,
            "planned_date": one_line(cells[4]),
        })
    return citations


# ── DBHDS: the walk ──────────────────────────────────────────────────────────


def months_ago(months: int) -> str:
    return (datetime.now() - timedelta(days=months * 30.5)).strftime("%Y-%m-%d")


class DBHDSScraper:
    def __init__(self) -> None:
        self.site = DBHDSSite()
        self.stats: Dict[str, int] = {}
        self.holds: List[Dict] = []
        self.unparsed: List[str] = []
        self.not_a_report: List[str] = []
        self.citations_per_plan: List[int] = []

    def bump(self, key: str, n: int = 1) -> None:
        self.stats[key] = self.stats.get(key, 0) + n

    # -- cache of what each service page said ------------------------------

    @staticmethod
    def cache_path(licence: str) -> Path:
        return DBHDS_SERVICE_CACHE / f"{licence}.json"

    def load_cache(self, licence: str) -> Dict:
        try:
            return json.loads(self.cache_path(licence).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def save_cache(self, licence: str, cache: Dict) -> None:
        DBHDS_SERVICE_CACHE.mkdir(parents=True, exist_ok=True)
        self.cache_path(licence).write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    def save_page(self, licence: str, page: Page) -> None:
        """The service details page, gzipped beside the PDFs (dated, so the
        lists can be re-read without the site)."""
        try:
            folder = report_store().archive_dir / "service-pages"
            folder.mkdir(parents=True, exist_ok=True)
            body = re.sub(r'(<input type="hidden" name="__(?:VIEWSTATE|EVENTVALIDATION)"[^>]*value=")[^"]*',
                          r"\1", page.text)
            (folder / f"{licence}_{datetime.now():%Y-%m-%d}.html.gz").write_bytes(
                gzip.compress(body.encode("utf-8")))
        except OSError as exc:
            logger.warning(f"  could not save the service page for {licence}: {exc}")

    # -- which rows still need the site --------------------------------------

    @staticmethod
    def due(check: Optional[Dict], date: str) -> bool:
        """Ask the site for this row's plan? Not when one is held; otherwise
        when never asked, or asked over a week ago and the row is under 18
        months old."""
        if not check:
            return True
        if check.get("cap"):
            return False
        if date and date < months_ago(DBHDS_RECHECK_MONTHS):
            return False
        asked = check.get("checked", "")
        return asked < (datetime.now() - timedelta(days=DBHDS_RECHECK_DAYS)).strftime("%Y-%m-%d")

    def have_extract(self, name: str) -> bool:
        return report_store().cached_extract(name) is not None

    def fetch_plan(self, name: str, page: Page) -> Optional[Dict]:
        """Extraction of the plan a View CAP response points at (cached)."""
        store = report_store()
        got = extract_with_cache(store, name, lambda: self.site.pdf_from(page), extract_cap)
        if got is not None:
            self.bump("plans_downloaded")
        return got

    # -- one service ----------------------------------------------------------

    def visit(self, target: Dict, cache: Dict) -> Dict:
        """Open the service on the site, read its lists, fetch what is due."""
        licence_guess = target["licence"]
        open_args = (target["service_type"], target["provider_number"], target["service_code"])
        page = self.site.open_service(*open_args)
        service = parse_service(page)
        licence = service["licence"] or licence_guess
        self.save_page(licence, page)
        assign_report_ids(service["inspections"])
        checks: Dict[str, Dict] = cache.get("checks", {})
        today = datetime.now().strftime("%Y-%m-%d")

        for row in service["inspections"]:
            rid = row["report_id"]
            name = f"{licence}_{rid}.pdf"
            if not row["button"]:
                checks[rid] = {"checked": today, "cap": False, "button": False}
                continue
            if self.have_extract(name):
                checks[rid] = {"checked": today, "cap": True}
                continue
            if not self.due(checks.get(rid), row["date"]):
                continue
            for attempt in range(2):
                try:
                    answer = self.site.submit(page, {row["button"]: "View CAP"})
                    if answer.kind() != "ServiceDetails":
                        raise Lost(f"View CAP landed on {answer.kind() or answer.url}")
                    page = answer
                    got = self.fetch_plan(name, answer)
                    checks[rid] = {"checked": today, "cap": bool(got and got.get("text"))}
                    if not got:
                        self.bump("clicks_without_pdf")
                    break
                except Lost as exc:
                    logger.warning(f"  {licence} {rid}: {exc}; reopening the service")
                    page = self.site.open_service(*open_args)
            # Saved after every plan, so an interrupted run resumes here.
            cache.update({"checks": checks})
            self.save_cache(licence, {**cache, "service": service, "target": target})

        details: Dict[str, Dict] = cache.get("investigations", {})
        for inv in service["investigations"]:
            number = inv["number"]
            known = details.get(number, {})
            date = inv["closed"] or inv["received"]
            names = known.get("names") or []
            if names and all(self.have_extract(n) for n in names):
                continue
            if known and not self.due({"checked": known.get("checked", ""), "cap": False}, date):
                continue
            if not inv["button"]:
                continue
            try:
                inv_page = self.site.submit(page, {inv["button"]: "Investigation Details"})
                if inv_page.kind() != "InvestigationDetails":
                    raise Lost(f"Investigation Details landed on {inv_page.kind() or inv_page.url}")
                rows = _grid(inv_page.soup, "dtgCAP")
                record = {"checked": today, "names": [], "start": "", "end": "", "program": "", "rows": len(rows)}
                for index, (cells, button) in enumerate(rows):
                    if len(cells) >= 6:
                        record["start"] = record["start"] or iso_date(cells[4])
                        record["end"] = iso_date(cells[5]) or record["end"]
                        record["program"] = record["program"] or cells[3]
                    if button is None:
                        continue
                    name = f"{licence}_{number}{'' if index == 0 else '-' + str(index + 1)}.pdf"
                    if not self.have_extract(name):
                        answer = self.site.submit(inv_page, {button["name"]: button.get("value", "View CAP")})
                        if answer.kind() != "InvestigationDetails":
                            raise Lost(f"investigation View CAP landed on {answer.kind() or answer.url}")
                        inv_page = answer
                        got = self.fetch_plan(name, answer)
                        if not (got and got.get("text")):
                            self.bump("clicks_without_pdf")
                            continue
                    record["names"].append(name)
                details[number] = record
                back = inv_page.soup.find("input", id="ContentPlaceHolder1_btnBack")
                page = self.site.submit(inv_page, {back["name"]: back.get("value", "")}) if back is not None else inv_page
                if page.kind() != "ServiceDetails":
                    page = self.site.open_service(*open_args)
            except Lost as exc:
                logger.warning(f"  {licence} investigation {number}: {exc}; reopening the service")
                page = self.site.open_service(*open_args)
            cache.update({"investigations": details})
            self.save_cache(licence, {**cache, "service": service, "target": target})

        cache.update({"service": service, "target": target, "checks": checks, "investigations": details,
                      "fetched": datetime.now().isoformat(timespec="seconds")})
        cache["provider_address"] = (self.site.provider_addresses.get(target["provider_number"])
                                     or cache.get("provider_address", ""))
        self.save_cache(licence, cache)
        return cache

    def fresh(self, cache: Dict) -> bool:
        fetched = cache.get("fetched", "")
        if not fetched or not cache.get("service"):
            return False
        try:
            age = datetime.now() - datetime.fromisoformat(fetched)
        except ValueError:
            return False
        return age < timedelta(hours=DBHDS_MAX_AGE_HOURS)

    # -- payload ----------------------------------------------------------------

    def plan_block(self, names: List[str], licence: str, rid: str) -> Tuple[List[Dict], List[str], List[str]]:
        """(citations, archive names that hold a plan, problems)."""
        citations: List[Dict] = []
        held: List[str] = []
        problems: List[str] = []
        for name in names:
            got = report_store().cached_extract(name)
            if not got:
                continue
            if not got.get("is_cap"):
                self.not_a_report.append(f"{name}: not a corrective action plan")
                continue
            if got.get("licence") and got["licence"] != licence:
                problems.append(f"{name}: plan is for licence {got['licence']}")
                self.not_a_report.append(f"{name}: the plan PDF names licence {got['licence']}")
                continue
            found = parse_citations(got.get("segments") or [])
            if not found:
                problems.append(f"{name}: no table rows read ({got.get('pages')} pages)")
            citations.extend(found)
            held.append(name)
        return citations, held, problems

    def build_facility(self, cache: Dict) -> Optional[Dict]:
        service, target = cache.get("service") or {}, cache.get("target") or {}
        licence = service.get("licence") or target.get("licence")
        if not licence:
            return None
        locations = service.get("locations") or []
        provider = service.get("provider") or target.get("provider", "")
        label = DBHDS_SERVICE_TYPES.get(service.get("service", ""), "") or target.get("label", "") \
            or (service.get("service", "") + " (DBHDS)")
        address = cache.get("provider_address", "")
        location_names = [f"{l['name']} ({l['city']})" if l.get("city") else l["name"] for l in locations]
        own_addresses = [address]
        common = {"source": "dbhds", "provider": provider, "locations": location_names}
        reports: List[Dict] = []

        def finish(report: Dict, citations: List[Dict], held_names: List[str], problems: List[str]) -> None:
            cats = report["categories"]
            cited = [c for c in citations if c["comp"] != "C"]
            cats["has_cap"] = bool(held_names)
            cats["citation_count"] = len(cited)
            cats["citations"] = [{
                "standard": c["standard"], "comp": c["comp"], "location": c["location"],
                "noncompliance": shorten(c["noncompliance"]), "response_status": c["response_status"],
                "response_date": c["response_date"], "planned_date": c["planned_date"],
            } for c in citations]
            if citations:
                cats["detail"] = {"citations": [{
                    "standard_text": c["standard_text"], "noncompliance": c["noncompliance"],
                    "provider_action": c["provider_action"], "licensing_response": c["licensing_response"],
                } for c in citations]}
            if held_names:
                cats["archive_name"] = held_names[0]
                if len(held_names) > 1:
                    cats["archive_names"] = held_names
            blocks = []
            for c in citations:
                blocks.append("\n".join(x for x in [
                    f"Standard cited: {c['standard']} {c['standard_text']}".strip(),
                    f"Compliance: {c['comp']} ({COMP_LABELS.get(c['comp'], 'not stated')})",
                    f"Location: {c['location']}" if c["location"] else "",
                    "Description of noncompliance:\n" + c["noncompliance"],
                    "Actions to be taken (provider):\n" + c["provider_action"] if c["provider_action"] else "",
                    "Office of Licensing response:\n" + c["licensing_response"] if c["licensing_response"] else "",
                    f"Planned completion date: {c['planned_date']}" if c["planned_date"] else "",
                ] if x))
            raw = "\n\n".join(blocks)
            report["raw_content"] = raw
            report["content_length"] = len(raw)
            what = "Investigation" if cats["kind"] == "investigation" else (cats.get("purpose") or "Inspection")
            if cited:
                report["summary"] = f"{what}: {len(cited)} standard{'s' if len(cited) != 1 else ''} cited"
            elif held_names:
                report["summary"] = f"{what}: plan posted, no citation read"
            else:
                report["summary"] = f"{what}: no finalized plan posted"
            for problem in problems:
                self.unparsed.append(f"DBHDS {licence} {report['report_id']}: {problem}")
            hits = privacy_hits(raw, own_addresses)
            if hits:
                self.holds.append({"source": "dbhds", "facility": facility_name, "licence": licence,
                                   "report_id": report["report_id"], "report_date": report["report_date"],
                                   "archive_name": cats.get("archive_name", ""), "hits": hits})
                logger.warning(f"  HELD (privacy) {licence} {report['report_id']}: {hits}")
                return
            if not report["report_date"]:
                self.unparsed.append(f"DBHDS {licence} {report['report_id']}: no date")
                return
            kind = cats["kind"]
            self.bump(f"{kind}s")
            self.bump(f"{kind}s_with_plan" if held_names else f"{kind}s_without_plan")
            if cited:
                self.bump("flagged")
                self.citations_per_plan.append(len(cited))
            elif held_names:
                self.bump("plans_without_citations")
            reports.append(report)

        facility_name = dbhds_facility_name(provider, locations)
        search_url = DBHDS_SEARCH_URL

        inspections = service.get("inspections") or []
        if inspections and "report_id" not in inspections[0]:
            assign_report_ids(inspections)
        for row in inspections:
            rid = row["report_id"]
            citations, held_names, problems = self.plan_block([f"{licence}_{rid}.pdf"], licence, rid)
            report = {"report_id": rid, "report_date": row["date"], "report_url": search_url,
                      "categories": {**common, "kind": "inspection", "purpose": row["purpose"]}}
            finish(report, citations, held_names, problems)
        for inv in service.get("investigations") or []:
            known = (cache.get("investigations") or {}).get(inv["number"], {})
            citations, held_names, problems = self.plan_block(known.get("names") or [], licence, inv["number"])
            cats = {**common, "kind": "investigation", "purpose": "Investigation",
                    "received": inv["received"], "closed": inv["closed"],
                    "inspection_start": known.get("start", ""), "inspection_end": known.get("end", "")}
            report = {"report_id": inv["number"], "report_date": inv["closed"] or inv["received"],
                      "report_url": search_url, "categories": cats}
            finish(report, citations, held_names, problems)

        reports.sort(key=lambda r: r["report_date"], reverse=True)
        info = {
            "facility_name": facility_name,
            "program_name": licence,
            "program_category": label,
            "full_address": address,
            "phone": service.get("phone", ""),
            "bed_capacity": "",
            "executive_director": service.get("contact", ""),
            "license_exp_date": service.get("expires", ""),
            "relicense_visit_date": "",
            "action": ", ".join(x for x in [service.get("status", ""), service.get("license_type", "")] if x),
        }
        return {"facility_info": info, "reports": reports}


def dbhds_facility_name(provider: str, locations: List[Dict]) -> str:
    """The location name when the licence has a single location, else the
    provider's. VA_NAME_RULE=provider-location tries "Provider - Location"
    instead (for the name-match comparison)."""
    rule = os.getenv("VA_NAME_RULE", "location")
    names = list(dict.fromkeys(one_line(l.get("name")) for l in locations if one_line(l.get("name"))))
    if len(names) != 1:
        return provider
    location = names[0]
    if rule == "provider-location":
        if slug(location) == slug(provider) or slug(provider) in slug(location):
            return location
        return f"{provider} - {location}"
    return location


def run_dbhds(args: argparse.Namespace, timestamp: str) -> Tuple[List[Dict], DBHDSScraper]:
    state = load_state(DBHDS_STATE_FILE)
    services: Dict[str, Dict] = state.setdefault("services", {})
    hashes: Dict[str, Dict[str, str]] = state.setdefault("hashes", {})
    has_cap: Dict[str, Dict[str, bool]] = state.setdefault("has_cap", {})
    scraper = DBHDSScraper()
    site = scraper.site
    wanted_types = args.service_type or list(DBHDS_SERVICE_TYPES)

    # 1. The searches: every (provider, service licence) in scope.
    targets: Dict[str, Dict] = {}
    for service_type in wanted_types:
        try:
            results = site.search(service_type)
        except KeyError:
            logger.error(f"DBHDS: no service type {service_type!r} in the search form; close ones: "
                         + "; ".join(t for t in site.service_types()
                                     if re.search(r"child|adolesc", t, re.I))[:1500])
            scraper.bump("service_types_missing")
            continue
        rows = parse_results(results)
        logger.info(f"DBHDS: {len(rows)} location rows for {service_type}")
        scraper.bump("location_rows", len(rows))
        for row in rows:
            licence = f"{row['provider_number']}-{row['service_code']}"
            target = targets.setdefault(licence, {
                "licence": licence, "service_type": service_type, "label": DBHDS_SERVICE_TYPES.get(service_type, ""),
                "provider": row["provider"], "provider_number": row["provider_number"],
                "service_code": row["service_code"], "grid_status": row["status"], "region": row["region"],
            })
            target.setdefault("grid_locations", []).append(row["location"])
    today = datetime.now().strftime("%Y-%m-%d")
    for licence, target in targets.items():
        services[licence] = {k: target[k] for k in ("service_type", "provider", "provider_number", "service_code")}
        services[licence]["last_listed"] = today
    save_state(DBHDS_STATE_FILE, state)

    order = sorted(targets.values(), key=lambda t: (wanted_types.index(t["service_type"]), t["provider"].lower(),
                                                    t["service_code"]))
    if args.licence:
        order = [t for t in order if t["licence"] in args.licence]
    if args.limit:
        order = order[: args.limit]
    logger.info(f"DBHDS: {len(order)} service licences to read")

    # 2. The walk. Provider addresses come from the provider page, which
    # open_service passes through; they are read once and cached.
    facilities: List[Dict] = []
    all_built: List[Tuple[str, Dict]] = []
    for index, target in enumerate(order, start=1):
        licence = target["licence"]
        cache = scraper.load_cache(licence)
        try:
            if scraper.fresh(cache) and not args.refresh:
                scraper.bump("services_from_cache")
            else:
                cache = scraper.visit(target, cache)
                scraper.bump("services_visited")
        except Exception as exc:  # one service must not end the run
            logger.error(f"[{index}/{len(order)}] {licence} {target['provider']}: {exc}")
            scraper.bump("service_errors")
            if not cache.get("service"):
                continue
        facility = scraper.build_facility(cache)
        if not facility:
            continue
        with_plan = sum(1 for r in facility["reports"] if r["categories"].get("has_cap"))
        logger.info(f"[{index}/{len(order)}] {facility['facility_info']['facility_name']} ({licence}): "
                    f"{len(facility['reports'])} reports, {with_plan} with a plan")
        all_built.append((licence, facility))

    new_ids: Dict[str, List[str]] = {}
    new_hashes: Dict[str, Dict[str, str]] = {}
    for licence, facility in all_built:
        fresh = [r for r in facility["reports"]
                 if args.full or hashes.get(licence, {}).get(r["report_id"]) != content_hash(r)]
        if not fresh:
            continue
        facilities.append({"facility_info": facility["facility_info"], "reports": fresh})
        new_ids[licence] = [r["report_id"] for r in fresh]
        new_hashes[licence] = {r["report_id"]: content_hash(r) for r in fresh}

    if facilities and not args.no_post:
        if save_to_api(facilities, timestamp):
            merge_new_ids(state, new_ids)
            for licence, values in new_hashes.items():
                hashes.setdefault(licence, {}).update(values)
            for facility in facilities:
                licence = facility["facility_info"]["program_name"]
                for r in facility["reports"]:
                    has_cap.setdefault(licence, {})[r["report_id"]] = bool(r["categories"].get("has_cap"))
            save_state(DBHDS_STATE_FILE, state)
            logger.info("DBHDS: posted; state advanced")
        else:
            logger.error("DBHDS: API save failed -- state not advanced")
    return facilities, scraper


# ── Output ───────────────────────────────────────────────────────────────────


def write_out(path: Path, facilities: List[Dict], timestamp: str, notes: Dict) -> None:
    """What inspections-read.php would return for these facilities."""
    shaped = [{
        "facility_info": f["facility_info"],
        "reports": [{**r, "is_structured": True} for r in f["reports"]],
    } for f in facilities]
    payload = {
        "total_facilities": len(shaped),
        "source_state": "VA",
        "scraped_timestamp": timestamp,
        "scraping_notes": {"total_reports": sum(len(f["reports"]) for f in shaped), **notes},
        "facilities": shaped,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"Wrote {path}")


def save_to_api(facilities: List[Dict], timestamp: str) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=os.getenv("KOP_DATA_API_KEY", "CHANGE_ME"),
        state="VA",
        scraped_timestamp=timestamp,
        facilities=facilities,
        timeout=180,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def date_range(facilities: List[Dict]) -> str:
    dates = sorted(r["report_date"] for f in facilities for r in f["reports"] if r["report_date"])
    return f"{dates[0]} to {dates[-1]}" if dates else "none"


def print_vdss(facilities: List[Dict], scraper: VDSSScraper) -> None:
    s = scraper.stats
    logger.info("── Virginia VDSS run summary ──")
    logger.info(f"facilities: {len(facilities)}; inspections: {s.get('reports', 0)} "
                f"(with violations: {s.get('flagged', 0)}, without: {s.get('clean', 0)}, "
                f"complaint related: {s.get('complaint', 0)})")
    logger.info(f"date range: {date_range(facilities)}")
    logger.info(f"comments with the standing boilerplate cut: {s.get('boilerplate_cut', 0)}; "
                f"inspection pages fetched this run: {s.get('pages_fetched', 0)}; requests: {scraper.client.requests_made}")
    logger.info(f"unparsed: {len(scraper.unparsed)}; privacy holds: {len(scraper.holds)}")
    for line in scraper.unparsed:
        logger.info(f"  unparsed: {line}")


def print_dbhds(facilities: List[Dict], scraper: DBHDSScraper) -> None:
    s = scraper.stats
    per = scraper.citations_per_plan
    logger.info("── Virginia DBHDS run summary ──")
    logger.info(f"service licences: {len(facilities)} (visited {s.get('services_visited', 0)}, "
                f"from cache {s.get('services_from_cache', 0)}, errors {s.get('service_errors', 0)}); "
                f"location rows in the searches: {s.get('location_rows', 0)}")
    logger.info(f"inspections: {s.get('inspections', 0)} (with a plan {s.get('inspections_with_plan', 0)}, "
                f"without {s.get('inspections_without_plan', 0)})")
    logger.info(f"investigations: {s.get('investigations', 0)} (with a plan {s.get('investigations_with_plan', 0)}, "
                f"without {s.get('investigations_without_plan', 0)})")
    logger.info(f"flagged (citations read): {s.get('flagged', 0)}; plans with no citation read: "
                f"{s.get('plans_without_citations', 0)}")
    if per:
        ordered = sorted(per)
        logger.info(f"citations per plan: total {sum(per)}, median {ordered[len(ordered) // 2]}, max {ordered[-1]}")
    logger.info(f"date range: {date_range(facilities)}")
    logger.info(f"plans downloaded this run: {s.get('plans_downloaded', 0)}; View CAP clicks that gave no PDF: "
                f"{s.get('clicks_without_pdf', 0)}; requests: {scraper.site.requests_made}; "
                f"cookies minted: {scraper.site.mints}")
    logger.info(f"table extraction problems: {len(scraper.unparsed)}; not a plan: {len(scraper.not_a_report)}; "
                f"privacy holds: {len(scraper.holds)}")
    for line in scraper.unparsed[:60]:
        logger.info(f"  unparsed: {line}")
    for line in scraper.not_a_report[:60]:
        logger.info(f"  not a report: {line}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Virginia VDSS and DBHDS licensing findings")
    parser.add_argument("--source", choices=("vdss", "dbhds", "all"), default="all")
    parser.add_argument("--full", action="store_true", help="Ignore the state files and re-post every report")
    parser.add_argument("--no-post", action="store_true", help="Do not POST to the inspections API")
    parser.add_argument("--limit", type=int, default=0, help="Only the first N facilities of each source")
    parser.add_argument("--out", type=Path, help="Write what the read API would return to this JSON file")
    parser.add_argument("--service-type", action="append", default=[],
                        help="DBHDS service type to search, as the form labels it (repeatable; replaces the list)")
    parser.add_argument("--licence", action="append", default=[],
                        help="Only this VDSS licence id or DBHDS licence number (repeatable)")
    parser.add_argument("--refresh", action="store_true",
                        help="DBHDS: visit every service even if it was read in the last "
                             f"{DBHDS_MAX_AGE_HOURS:g} hours")
    args = parser.parse_args()

    timestamp = datetime.now().isoformat(timespec="seconds")
    facilities: List[Dict] = []
    holds: List[Dict] = []
    notes: Dict[str, Any] = {}
    vdss = dbhds = None
    vdss_facilities: List[Dict] = []
    dbhds_facilities: List[Dict] = []
    if args.source in ("vdss", "all"):
        vdss_facilities, vdss = run_vdss(args, timestamp)
        facilities += vdss_facilities
        holds += vdss.holds
    if args.source in ("dbhds", "all"):
        dbhds_facilities, dbhds = run_dbhds(args, timestamp)
        facilities += dbhds_facilities
        holds += dbhds.holds
        notes["not_a_report"] = dbhds.not_a_report
        notes["dbhds_unparsed"] = dbhds.unparsed
    if vdss is not None:
        print_vdss(vdss_facilities, vdss)
        notes["vdss_unparsed"] = vdss.unparsed
    if dbhds is not None:
        print_dbhds(dbhds_facilities, dbhds)
    notes["privacy_holds"] = holds
    if holds:
        logger.info(f"── Held back by the privacy check ({len(holds)}), for the owner to review ──")
        for hold in holds:
            logger.info(f"  {hold['source']} {hold['facility']} ({hold['licence']}) {hold['report_id']} "
                        f"{hold['report_date']}: {'; '.join(hold['hits'][:3])}")
    if args.out:
        write_out(args.out, facilities, timestamp, notes)
    if not facilities:
        logger.info("No new reports since last run")
    elif args.no_post:
        logger.info("Skipping API POST because --no-post was set; state not advanced")


if __name__ == "__main__":
    main()
