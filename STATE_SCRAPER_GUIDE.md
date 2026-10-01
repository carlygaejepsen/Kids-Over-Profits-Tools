# State Scraper Implementation Guide

How the inspection data pipeline works end-to-end, and how to add a new state.

## Architecture Overview

```
State Scraper (Python)
    |
    | POST JSON (facilities + reports)
    v
inspections-write.php  -->  MySQL (inspection_facilities + inspection_reports)
    |
    | GET ?state=XX
    v
inspections-read.php  -->  JSON response
    |
    v
xx_reports.js (frontend)  -->  rendered HTML on kidsoverprofits.org
```

All states share the same API endpoints, database tables, and `inspection_api_client.py` posting logic. Each state only needs:
1. A Python scraper that collects data and calls `post_facilities_to_api()`
2. A JS file that fetches from `inspections-read.php?state=XX` and renders it

## Data Schema

### What the scraper produces

Each scraper builds a list of facility dicts in this shape:

```python
{
    "facility_info": {
        "facility_name": "Example Home",          # required
        "program_name": "LIC-12345",              # unique ID (license #, operation #, etc.)
        "program_category": "Residential Care",
        "full_address": "123 Main St, City, ST 12345",
        "phone": "(555) 123-4567",
        "bed_capacity": "24",
        "executive_director": "Jane Smith",
        "license_exp_date": "12/31/2025",
        "relicense_visit_date": "01/15/2025",
        "action": "Active",                       # status field
    },
    "reports": [
        {
            "report_id": "INSP-001",              # unique within facility
            "report_date": "03/15/2025",
            "raw_content": "Narrative text...",
            "content_length": 142,
            "summary": "Annual inspection - 3 deficiencies",
            "categories": { ... },                 # state-specific structured data (stored as JSON)
        }
    ]
}
```

The `categories` dict is flexible -- each state puts whatever structured data the frontend needs. It gets stored as `categories_json` in MySQL and returned as-is by the read API.

### Database tables

**`inspection_facilities`** -- one row per facility per state
- Unique key: `(state, facility_name, program_name)`
- Upserts on scrape -- existing facilities get updated, not duplicated

**`inspection_reports`** -- one row per report/inspection
- Unique key: `(facility_id, report_id)`
- `categories_json` column stores the full `categories` dict as JSON

### API endpoints

**Write** (`api/inspections-write.php`) -- receives POST from scrapers:
```json
{
    "api_key": "...",
    "state": "XX",
    "scraped_timestamp": "2025-03-15T10:30:00",
    "facilities": [ ... ]
}
```

**Read** (`api/inspections-read.php?state=XX`) -- returns data for frontend:
```json
{
    "total_facilities": 118,
    "source_state": "AZ",
    "facilities": [
        {
            "facility_info": { ... },
            "reports": [
                {
                    "report_id": "...",
                    "report_date": "...",
                    "categories": { ... }
                }
            ]
        }
    ]
}
```

## Incremental Scrape State

Most production scrapers now run incrementally by default instead of reposting every report on every run.

- Each scraper keeps a local JSON state file such as `.az_state.json`, `.ct_state.json`, or `.or_state.json`.
- State files are local runtime artifacts and are gitignored.
- State only advances after a successful API write. If the POST fails, the scraper logs `state not advanced` and will retry those same reports next run.
- `--full` bypasses the saved state and forces a complete re-scan/re-post.

Two state patterns are in use:

### Seen-ID state

Use this when the source does not provide a reliable "modified since" filter.

```json
{
  "seen": {
    "facility-or-program-key": ["report-1", "report-2"]
  }
}
```

This is used by AZ, CA, CT, OR, TX, UT, and WA. The key should be the most stable identifier available for that scraper (facility ID, operation ID, agency name, etc.).

### Cursor state

Use this when the upstream source can filter server-side by modification date or timestamp.

```json
{
  "last_run": {
    "facility-slug": "2025-09-08T15:41:03"
  }
}
```

This is currently used by AR, where the Disability Rights Arkansas WordPress API supports `modified_after`.

## Existing Implementations

### CT (Connecticut)
- **Source:** Single HTML table at `licensefacilities.dcf.ct.gov`
- **Method:** `requests` + BeautifulSoup (no browser needed)
- **Scraper:** `ct_scraper.py` -- parses HTML table rows into facilities, extracts report content from cells, categorizes structured DCF reports
- **Frontend:** `js/inspections/ct_reports.js` -- renders structured report categories (areas covered, non-compliance, corrective actions, recommendations)
- **Key detail:** Incremental state is keyed by `facility_name`; already-posted `report_id`s are filtered out unless `--full` is used.

### TX (Texas)
- **Source:** TX HHS Childcare Search at `childcare.hhs.texas.gov`
- **Method:** `requests` against the site's internal JSON API (same endpoints the React frontend calls)
- **API pattern:** Get auth token -> search by operation number -> get compliance history
- **Scraper:** `tx_scraper.py` -- three HTTP calls per facility, maps deficiency fields to match the CSV column names the frontend expects
- **Frontend:** `js/inspections/tx_reports.js` -- renders TX citation fields (Standard Number, Risk Level, Deficiency Narrative, Correction Narrative, etc.)
- **Key detail:** `categories` stores TX-specific fields like `Citation Date`, `Standard Risk Level`, `Sections Violated`, etc.
- **Incremental behavior:** TX still fetches full compliance history for each operation, then filters out already-seen deficiencies before POSTing.

### AZ (Arizona)
- **Source:** AZ Care Check at `azcarecheck.azdhs.gov` (Salesforce Lightning Community)
- **Method:** `requests` against Salesforce Aura/Apex endpoints (no browser needed)
- **API pattern:** Call Apex controllers directly via POST to `/s/sfsites/aura`
- **Scraper:** `az_scraper.py` -- calls `getFacilityDetails`, `getFacilityOrLicenseInspections`, and `getInspectionItemSODWrap`
- **Frontend:** `js/inspections/az_reports.js` -- renders inspection list with deficiency rule/evidence/findings; falls back to legacy JSON files if API is empty
- **Key detail:** Salesforce Aura calls need a `fwuid` context string that may change when Salesforce deploys updates. If the scraper starts failing, capture a fresh `fwuid` from the browser network tab.
- **Incremental behavior:** State is keyed by facility ID, and deficiency-item lookups only run for inspections that have not already been posted.

### AR (Arkansas)
- **Source:** Disability Rights Arkansas WordPress REST API at `disabilityrightsar.org/wp-json/wp/v2`, with linked Google Drive PDFs
- **Method:** `requests` + WordPress REST + `pdfplumber` with optional OCR fallback for image-only PDFs
- **Scraper:** `ar_scraper.py` -- fetches DRA document posts by facility category, downloads/caches PDFs and extracted text, then builds reports from the document metadata and PDF content
- **Key detail:** AR uses cursor-based incremental state (`last_run`) per category slug and passes that value to the upstream `modified_after` parameter. PDFs and extracted text are cached locally to avoid repeat downloads/work.

### CA (California)
- **Source:** California Community Care Licensing transparency endpoint at `.../api/FacilityReports`
- **Method:** `requests` against the report endpoint plus HTML parsing for the returned report bodies
- **Scraper:** `ca_scraper.py` -- walks report indices per facility, parses continuation-heavy reports, and maps them to the shared inspections payload
- **Key detail:** Incremental state is keyed by facility ID and synthetic report ID (`{facility_id}-{index}`). Because the scraper skips already-seen indices before requesting them, use `--full` if the source ever backfills or reorders older reports.

### OR (Oregon)
- **Source:** Public Oregon ODHS SharePoint-backed report pages for RC and TBS programs
- **Method:** `requests` against the anonymous SharePoint SOAP endpoint, then PDF download/text extraction
- **Scraper:** `or_scraper.py` -- reads SharePoint rows directly, groups reports by agency/program, and posts the resulting facility payloads
- **Key detail:** Incremental state is keyed by `agency_name`, and filtering happens before PDF download/parse, so already-seen reports cost almost nothing on reruns. Oregon no longer supports destructive replace mode; use `--full` only when you intentionally want to re-scan all reports without clearing existing database rows.

### WA (Washington)
- **Source:** WA DOH facility inspections and investigations search
- **Method:** `requests` + BeautifulSoup for search results, then PDF download and `pdfplumber` extraction
- **Scraper:** `wa_scraper.py` -- collects DOH search rows for residential treatment and behavioral health facility types, filters to KOP programs, then builds reports from linked PDFs
- **Key detail:** Incremental state is keyed by facility name and report number. Reports missing a report number in the HTML cannot be skipped cheaply and still require PDF inspection.

### FL (Florida — DCF, not implementable)
- **Status:** Closed. `FLDCFScraper` is a stub that raises `NotImplementedError` and documents why.
- **Why:** DCF Office of Licensing does not publish a public Residential Group Care provider directory. CARES (`caressearch.myflfamilies.com`, API at `caresapi.myflfamilies.com`) reverse-engineers cleanly via an anonymous token flow, but its scope is Child Care Facilities only — every record returns `providerType: "Child Care Facility"` (e.g. Heartland Educational Group, Marita's Playgroup, Goddard School). Residential Group Care providers are licensed under Ch. 409.175 F.S. but their list is held internally and contracted out via ~17 regional Community-Based Care (CBC) lead agencies. No unified equivalent of DJJ's or AHCA's public datasets exists as of 2026-05.
- **If a public source ever materializes:** fill in `FLDCFScraper` and add a section here.

### FL (Florida — AHCA Phase 2)
- **Source:** Florida Agency for Health Care Administration (AHCA) — public facility locator at `quality.healthfinder.fl.gov` plus the inspection details viewer at `apps.ahca.myflorida.com/dm_web/`. Covers **Residential Treatment Centers for Children and Adolescents** (which includes Therapeutic Group Homes per Florida statute). ~36 facilities.
- **Method:** `requests` only — no PDF parsing required for this source.
- **FHF endpoint:** modern Razor Pages site. The AdvancedSearch handler returns HTML with the full facility list embedded as a JSON array in the page (rendered client-side by jQuery DataTables). The scraper just regex-extracts the array; no DataTables AJAX or pagination needed.
- **dm_web endpoint:** classic ASP.NET. `facility_inspection_details.aspx?file_number=...&client_code=57&provider_type=...&provider_name=...` returns a `gridView` table of deficiency rows (Survey Date · Inspection Type · Track ID · Deficiency · Requirement Description · Correction Date). The dm_web session ID is captured from the redirect on first hit and reused for every facility.
- **Scraper:** `fl_scraper.py --source ahca`. One inspection visit = one report row, grouped by (Survey Date + Track ID). All deficiencies for that visit are bundled into `categories.deficiencies` for the frontend.
- **Incremental state:** `.fl_ahca_state.json`, seen-ID keyed by AHCA File Number; `report_id = "{track_id}-{survey_date}"` so the same Track ID with a follow-up correction date still counts as the same report.
- **Adding more facility types:** extend `AHCA_FACILITY_TYPES` in `fl_scraper.py`. FHF dropdown values to consider next: `RTF` (Residential Treatment Facility — adult), `Crisis` (Crisis Stabilization Unit), `GH` (APD Licensed Group Home).
- **Useful flags:** `--full`, `--no-post`, `--limit N`. Workers/PDF timeout flags don't apply (AHCA has no PDFs).
- **First-run expectation:** ~1 minute (36 facilities × 1 dm_web request each).

### FL (Florida — DJJ Phase 1)
- **Source:** Florida Department of Juvenile Justice (`www.djj.state.fl.us`) — residential and detention facility directories plus QI, PREA, and SPEP report indices
- **Method:** `requests` + BeautifulSoup over static HTML index pages, then PDF download + `pdfplumber` extraction (no SOAP/auth needed)
- **Scraper:** `fl_scraper.py` dispatches on `--source`. Only `--source djj` is implemented; AHCA and DCF are stubs raising `NotImplementedError` until Phases 2 and 3 land. DJJ categories are selectable via `--categories residential,detention,prea,spep` (default: all four).
- **Key detail:** Incremental state is keyed by facility directory slug (e.g. `broward-youth-treatment-center`), or by `unmatched-<program-slug>` for reports whose program name didn't fuzzy-match the directory. Unmatched reports surface in the API as synthetic facilities flagged with `categories.unmatched = true` — they're not silently dropped. `program_name` is namespaced as `DJJ-<slug>` so the future AHCA/DCF phases can't collide with DJJ on the `(state, facility_name, program_name)` unique key.
- **PDF storage:** Each PDF is downloaded into memory, its text extracted from a temporary file that is deleted straight after, and the PDF written once to the `fl_pdfs` subfolder of the FileBird Google Drive folder (`report_store.py`). The extracted text is cached locally in `.report_extract_cache/fl_pdfs/`; reruns read that and never open the archived PDFs.
- **Useful flags beyond `--full`:** `--no-post` (cache + parse only, no API write), `--no-profiles` (skip per-facility profile fetches — faster smoke test), `--limit N` (post at most N facilities — for end-to-end testing on production), `--workers N` (concurrent PDF download/parse workers; default 5, env `FL_WORKERS`), `--pdf-timeout N` (seconds before abandoning pdfplumber on a single PDF; default 120, env `FL_PDF_TIMEOUT`).
- **First-run expectation:** The full DJJ scrape is ~1250 PDFs (≈90 QI + ~265 PREA + ~850 SPEP + ~40 detention QI). With the default 5 workers, expect roughly 1-2 hours wall-clock for an empty cache. Subsequent runs use `.report_extract_cache/fl_pdfs/` + `.fl_djj_state.json` and complete in minutes.

### GA (Georgia RCCL TRAILS public portal)
- **Source:** Georgia DHS Office of Inspector General "TRAILS" portal at `rcctrails.dhs.ga.gov` — Residential Child Care Licensing surveys (Statements of Deficiency)
- **Method:** `requests`/`curl_cffi` against the ASP.NET WebForms flow (no browser). Terms-acceptance postback → per-program-type search → Telerik RadGrid pagination → facility detail → SOD report per EventID. PDF SOD bodies are extracted with `pdfplumber`; HTML bodies are parsed inline.
- **Scraper:** `ga_scraper.py` — searches all seven RCCL program types (Child Caring Institution, Child Placing Agency, Children's Transition Care Center, Maternity Home, Runaway and Homeless Youth Program, Outdoor Child Caring Program, Maternity Supportive Housing Residence), collects every FACID, then pulls each facility's survey grid. One survey = one report; `report_id` is the EventID.
- **Key detail:** The public search grid column headed "Active Facility" is actually the facility **name**; the grid exposes name, address, county, email, and operating status but no phone/capacity (those aren't published). `program_name` = FACID (stable unique key). Survey metadata (type, status, dates, under-appeal) always lands in `categories` even when the SOD body can't be extracted.
- **IP block caveat:** The portal's AWS load balancer returns a bare `403 Forbidden` for datacenter/VPN IP ranges — this is IP-reputation based, so curl_cffi impersonation and real headless browsers are both blocked. **Run from a residential IP with any VPN turned off.** The scraper raises a clear error naming this cause when it sees an `awselb` 403.
- **Useful flags:** `--limit N`, `--program-type "..."` (repeatable), `--no-sod` (metadata-only smoke test), `--no-post`, `--full`.
- **Status:** Parsers validated against archived portal HTML; the stateful WebForms postback flow (acceptance + RadGrid pagination) still needs one live validation run from an unblocked IP.

### MI (Michigan child welfare licensing)
- **Source:** MDHHS Division of Child Welfare Licensing public search at `michildwelfarepubliclicensingsearch.michigan.gov/licagencysrch/` (Salesforce Experience Cloud)
- **Method:** `requests` against the site's anonymous Apex endpoint (`webruntime/api/apex/execute`), three methods of `COM_CWLicensingSearchController`: `getAgenciesDetail` (every licensed agency), `getContentDetails` (an agency's documents), `getContentBaseData` (one PDF as base64). Text PDFs, read with `pdfplumber` (text plus table rows).
- **Scraper:** `mi_scraper.py` takes every agency whose type is not "Child Placing Agency" (about 110: private, government and state child caring institutions, court operated facilities, therapeutic group homes). One document = one report; `report_id` is the `ContentDocumentId`, `program_name` the licence number.
- **Report types** (`categories.doc_type`, from the cover letter's "Attached is the ... Report"): `special_investigation` (complaint investigations: per allegation the rule, allegation and conclusion, from the section III tables since 2022 or the capitalised ALLEGATION:/APPLICABLE RULE/CONCLUSION: text before), `renewal`, `interim`, `original` (cited rules from "C. Rule/Statutory Violations" or the older "except for the following:" findings; `cap_required` from the cover letter). Flagged = a violation established, or an inspection with a corrective action plan or a cited rule.
- **TLS:** the server leaves out its intermediate certificate, so the scraper verifies against certifi plus `certs/sectigo_ov_r36.pem`. If verification fails, the error names the fix (fetch the leaf certificate's AIA "CA Issuers" URL). Never `verify=False`.
- **Class id:** `@udd/01p8z0000009E4V` (override `MI_CLASS_ID`). On "The Apex request is invalid." the scraper reads the new id from the site's view scripts and retries once.
- **PDF storage:** `ReportStore("MI_PDF_CACHE", "mi_pdfs", ...)`, archived as `<licence>_<ContentDocumentId>.pdf`. The state has no direct document URL and lets licensees ask for violation reports to be taken down after two years, so the archived copy on the site is the reader's link (`categories.archive_name`).
- **Incremental state:** `.mi_state.json`, seen-ID keyed by licence number, plus `agencies`: the last known record per licence, so a facility that drops off the list still has its documents asked for by id.
- **Useful flags:** `--full`, `--no-post`, `--limit N`, `--agency <licence or agencyId>` (repeatable), `--out file.json` (what the read API would return, for testing the page).
- **First-run expectation:** about 1,900 PDFs, one request every 0.5 s, roughly 1.5 hours; reruns read `.report_extract_cache/mi_pdfs/`.

### OK (Oklahoma OKDHS residential and shelter monitoring)
- **Source:** OKDHS Child Care Services residential locator, `http://www.publicview.okdhs.org/ResidentialLocator/Default.aspx` (list), and each program's page `http://residentialchildplacingview.okdhs.org/ResidentialView/ResidentialView.aspx?CaseNumber=<case>`. Plain HTTP only (the facility host does not answer on HTTPS); no login, CAPTCHA or WAF.
- **Method:** `requests` + BeautifulSoup against ASP.NET WebForms. Search posts every hidden input plus `rblProgramType` (`K85` residential program, `K84` shelter) and redirects to `ChildCareFacilities.aspx`; paging posts **that** page's hidden inputs back to `ChildCareFacilities.aspx` with `__EVENTTARGET=ctl00$ContentPlaceHolder1$GridView1`, `__EVENTARGUMENT=Page$Next` (posting to Default.aspx returns no grid). Case numbers are `K84`/`K85` plus 7 digits.
- **Scraper:** `ok_scraper.py`, 93 programs on 2026-09-30 (72 residential, 21 shelters). One report per monitoring visit (`visit-YYYYMMDD-<full|partial|attempted>`, `-2` for a second on the same day) and one per substantiated complaint (`complaint-YYYYMMDD-<8 hex of sha1(first requirement + allegation)>`). `categories`: `kind`, `visit_type`, `purpose`, `finding`, `items` [{requirement, description, observed, plan, correction_date, nrs, finding}], `item_count`, `nrs_count`. `program_name` = case number; all-caps names are title-cased.
- **Rolling window:** the state shows only 36 months. Every fetched page is saved gzipped to the Drive folder `ok_html/<case>/<YYYY-MM-DD>.html.gz` when its content changed (clock, "since" dates and hidden inputs ignored), and `--from-saved <ok_html dir>` rebuilds the payload from those copies with no requests. Run it monthly at the least.
- **Edits:** the state adds correction dates and plans after a visit, so `.ok_state.json` keeps a content hash per report (as `tx_scraper.py` does) and re-posts a report whose hash changed. Reports the state no longer shows are logged and left on the site. It also keeps `programs` (every case number seen, so a program that leaves the list is still visited) and `pages` (last saved page fingerprints).
- **Error page:** the site sometimes answers 200 with "You have encountered an error. Please try again."; it is asked again up to 4 times and never saved or parsed. A complaint whose table says only "No data on file" is counted and not posted.
- **Useful flags:** `--full`, `--no-post`, `--limit N`, `--case <case number>` (repeatable), `--out file.json`, `--from-saved DIR`.

### PA (Pennsylvania DHS Licensing Inspection Summaries)
- **Source:** DHS Human Services Provider Directory, `https://www.humanservices.dhs.pa.gov/HUMAN_SERVICE_PROVIDER_DIRECTORY/` (ASP.NET MVC, plain `requests`, no login). One POST per service code with a fresh `__RequestVerificationToken`; each unit's report list is a GET by the bracketed licence id (`Home/AzureInspVioltnReprtSearchResults?id=448090`); each report a GET of `Home/GetAzureFile?directory=inspectionsummary&filename=YYYYMMDD_<lic>.pdf`.
- **Scope:** service codes 36 residential services (PRTFs are here too: Sarah A Reed, Bradley Center, Foundations), 41 transitional living, 40 secure detention, 39 secure care, 42 outdoor, 43 mobile; 595 units on 2026-09-30. Override with `--service-codes`. Devereux's children's center is an OMHSAS inpatient unit, out of scope.
- **One facility per licensed unit** (`program_name` = licence id). `facility_name` = the unit name when it shares a distinguishing word with the legal entity, else `"<Entity>: <Unit>"`; capitals are title-cased, a broken entity in the directory ("260618ILDREN'S HOME...") is read from the summaries' "Legal Entity Name:".
- **Forms:** the text summary since mid-2019 (and a 2019-2020 variant: "1. 55 PA Code Chapter / Area of Non-Compliance / Provider's Plan of Corrective Action / Status of Correction"); the scanned numbered form of about 2012-2019 ("1. REGULATION / 2a. DESCRIPTION OF VIOLATION / 3. PLAN OF CORRECTION"); the scanned table of about 2009-2012 (regulation numbers certain, findings best effort). Every page with no text is OCR'd (Tesseract from Program Files or `TESSERACT_CMD`, Poppler from `C:/tools/poppler-*` or `POPPLER_PATH`, pdfplumber rendering when Poppler rejects a file) on a pool of `--ocr-workers` (default: one per core, one Tesseract thread each) while the next unit downloads.
- **Kinds** (`categories.kind`): `citation`, `followup` (plan of correction verified), `sanction` (provisional licence, revocation, non-renewal), `clean`, `licence`, `waiver`, `other`. `counts_as_violation`: citations and sanctions, and follow-ups whose inspection has no citation document listed; the state usually replaces a summary with its verified version, so most follow-ups are the only record (772 of 846 from 2019 on).
- **Payload size:** long text (narrative; per citation the full violation, requirement, plan, verification) goes in `categories.detail`, which `inspections-read.php` leaves out of `?lite=1` lists and returns with `?text=`. The 2019+ list is 3.8 MB (0.36 MB gzipped).
- **Dates:** `report_date` is the file name's date, except scans filed under the wrong year (a 2017 inspection as `20080104_...`): then the document's own date.
- **Incremental state:** `.pa_state.json`, seen-ID keyed by licence id, `licences` (every unit ever listed, so a closed unit is still visited) and `cited_inspections` (inspection keys with a counted document, so a later follow-up is not counted twice).
- **Useful flags:** `--full`, `--no-post`, `--limit N`, `--unit <licence id>` (repeatable), `--since YYYY`, `--batch N` (units per post; the state advances after each), `--ocr-workers N`, `--out file.json`.
- **First run:** 2019 onward is about 4,000 documents and an hour; everything before 2019 is about 6,000 mostly scanned documents and several hours of OCR. Run long passes as a detached process.

### NC (North Carolina MHLCS public records)
- **Source:** NC DHHS Division of Health Service Regulation Mental Health Licensure and Certification Section public records directory at `results.asp`, with facility pages at `facility.asp?fid=...`
- **Method:** `requests` + BeautifulSoup to read the directory and facility pages, then OCR each linked inspection PDF with `pdf2image` + `pytesseract`
- **Scraper:** `nc_scraper.py` uses the workbook as the seed list, matches each license to the public directory, and posts OCRed inspection reports for each matched facility
- **Key detail:** Every report PDF is OCRed, even when the PDF also contains embedded text. The workbook is only the facility seed list; the report content comes from the public NC inspection PDFs
- **Useful flags:** `--input`, `--limit N`, `--no-post`, `--full`

### UT (Utah OCR/CSV export)
- **Source:** Utah facility JSON endpoint at `ccl.utah.gov`
- **Method:** `requests` JSON fetches plus CSV export
- **Script:** `utah_citation_scraper.py` -- writes OCR-enhanced checklist data to JSON plus a flattened CSV export for newly observed inspections
- **Key detail:** This script is not part of the WordPress inspections API pipeline, but it uses the same seen-ID state pattern keyed by facility ID and inspection date, so reruns only process newly observed inspections unless `--full` is used.

## Adding a New State

### Step 1: Reverse-engineer the data source

Before writing any code, figure out how to get the data:

1. Open the state's facility/inspection search site
2. Open browser DevTools -> Network tab
3. Search for a facility and watch the requests
4. Look for JSON API endpoints behind the frontend (most modern sites have them)
5. If no API exists, check if it's a static HTML page (use `requests` + BeautifulSoup)
6. Playwright/browser automation is a last resort -- it's slow and fragile

**What to look for:**
- REST/JSON APIs the frontend calls (check XHR/Fetch requests in DevTools)
- Salesforce Aura endpoints (`/s/sfsites/aura`)
- GraphQL endpoints
- Direct data downloads (CSV, Excel)
- Static HTML tables

### Step 2: Write the scraper

Create `xx_scraper.py` following this pattern:

```python
"""
[State] Facility Scraper
"""
import argparse
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import requests

from inspection_api_client import post_facilities_to_api
from scraper_state import load_state, merge_new_ids, save_state, seen_from_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("INSPECTIONS_API_KEY", "CHANGE_ME")
STATE_FILE = Path(os.getenv("XX_STATE_FILE", ".xx_state.json"))

FACILITY_IDS = [...]  # list of IDs to scrape


class XXFacilityScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        self.all_facilities: List[Dict] = []

    def _get_facility(self, facility_id: str) -> Dict:
        """Fetch facility info from the state's API."""
        # ... state-specific API calls ...
        return {
            "facility_name": "...",
            "program_name": "...",  # unique identifier
            # ... other fields ...
        }

    def _get_reports(self, facility_id: str) -> List[Dict]:
        """Fetch inspection/report data."""
        # ... state-specific API calls ...
        return [{
            "report_id": "...",
            "report_date": "...",
            "raw_content": "...",
            "content_length": 0,
            "summary": "...",
            "categories": {
                # Put whatever structured data the frontend needs here.
                # This is stored as JSON and returned as-is by the read API.
            },
        }]

    def scrape(
        self,
        facility_ids: Optional[List[str]] = None,
        seen: Optional[Dict[str, Set[str]]] = None,
    ) -> Tuple[List[Dict], Dict[str, List[str]]]:
        ids = facility_ids or FACILITY_IDS
        seen = seen or {}
        new_ids: Dict[str, List[str]] = {}
        logger.info(f"Starting XX scrape for {len(ids)} facilities")

        for i, fid in enumerate(ids):
            logger.info(f"[{i+1}/{len(ids)}] {fid}")
            try:
                facility_info = self._get_facility(fid)
                reports = [
                    r for r in self._get_reports(fid)
                    if r.get("report_id") and r["report_id"] not in seen.get(fid, set())
                ]
                if not reports:
                    continue
                self.all_facilities.append({
                    "facility_info": facility_info,
                    "reports": reports,
                })
                new_ids[fid] = [r["report_id"] for r in reports if r.get("report_id")]
            except Exception as e:
                logger.error(f"  ERROR: {e}")
                continue

        logger.info(f"Scraping complete: {len(self.all_facilities)} facilities")
        return self.all_facilities, new_ids


def save_to_api(facilities: List[Dict]) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="XX",  # <-- your state code
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true",
                    help=f"Ignore {STATE_FILE} and re-post all reports")
    args = ap.parse_args()

    state = load_state(STATE_FILE)
    seen = {} if args.full else seen_from_state(state)

    scraper = XXFacilityScraper()
    facilities, new_ids = scraper.scrape(seen=seen)
    if not facilities:
        logger.info("No new reports since last run")
        return

    logger.info(f"Scraped {len(facilities)} facilities -- posting to API")
    if save_to_api(facilities):
        merge_new_ids(state, new_ids)
        save_state(STATE_FILE, state)
        logger.info("Saved successfully!")
    else:
        logger.error("API save failed -- state not advanced")


if __name__ == "__main__":
    main()
```

If the upstream source supports a reliable `modified_after` / `updated_since` filter, prefer a cursor-based `last_run` state like `ar_scraper.py` instead of seen-ID filtering.

### Step 3: Write the frontend JS

Create `js/inspections/xx_reports.js`. The basic structure:

1. Fetch from `inspections-read.php?state=XX`
2. Convert the API response into your rendering format
3. Group facilities by first letter for the alphabet filter
4. Render facility cards with expandable inspection/report details
5. Sort reports by date (newest first)

Copy the closest existing state's JS as a starting point and modify the rendering to match whatever fields your state's `categories` contains.

### Step 4: Register the page in WordPress

Add the JS file to the theme's enqueue and create a WordPress page template that loads it. Follow the pattern of the existing state pages.

## Shared Utilities

### `inspection_api_client.py`

Handles all API posting logic. You never need to modify this file. It:
- Splits large payloads into batches (750KB cap per request)
- Retries with smaller batches on HTTP 413 or 500
- Logs progress and errors

Usage:
```python
from inspection_api_client import post_facilities_to_api

result = post_facilities_to_api(
    api_url=API_URL,
    api_key=API_KEY,
    state="XX",
    scraped_timestamp=datetime.now().isoformat(),
    facilities=my_facilities_list,
    timeout=120,
    info=logger.info,
    error=logger.error,
)
```

### `scraper_state.py`

Shared helpers for local incremental state files:

- `load_state(path)` -- returns `{}` if the file is missing or invalid
- `save_state(path, state)` -- writes the updated JSON file
- `seen_from_state(state)` -- converts `{"seen": {key: [...]}}` into sets for fast lookups
- `merge_new_ids(state, new_ids)` -- merges only newly posted report IDs back into state

Use `merge_new_ids()` only after a successful downstream write. That keeps reruns restart-safe.

## Tips

- **Prefer APIs over scraping HTML.** Most state sites have JSON APIs behind their frontends. Check the Network tab before writing a scraper.
- **Use `requests`, not Playwright.** Browser automation is 100x slower and breaks when sites update their UI. Every state so far has had a direct API.
- **Default to incremental runs.** Treat `--full` as an explicit maintenance mode, not the default behavior.
- **Pick a stable dedupe key.** Good choices are facility IDs, operation IDs, agency names, or upstream slugs. Bad choices are display strings that frequently change.
- **Advance state only after success.** Never write seen IDs or date cursors before the API POST (or CSV export) succeeds.
- **Handle None values.** State APIs often return `null` for optional fields. Use `value or ""` instead of `value` to avoid `TypeError` on string operations.
- **The `categories` dict is your escape hatch.** Each state's data is different. Put whatever structured data the frontend needs into `categories` -- it's stored as JSON and passed through unchanged.
- **Use cursor state when the source supports it.** Server-side date filters are much cheaper than fetching everything and deduping locally.
- **Sort reports newest-first.** Do this in the frontend JS when converting API data. Watch out for date formats that `new Date()` can't parse (date ranges, non-standard formats).
- **Test with 2-3 facilities first.** Run `scraper.scrape(facility_ids=["id1", "id2"])` before doing the full run.
- **Use `--full` after parser changes or suspected backfills.** Especially important for index-based sources like CA, where older content could shift positions.
- **Salesforce sites** use the Aura framework. The `fwuid` in the context string changes on deploys. If the AZ scraper breaks, open the site in a browser, check the Network tab for an `aura` request, and copy the new `fwuid`.
