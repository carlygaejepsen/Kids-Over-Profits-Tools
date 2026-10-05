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
- **Source:** Florida Agency for Health Care Administration (AHCA) — public facility locator at `quality.healthfinder.fl.gov` plus the inspection details viewer at `apps.ahca.myflorida.com/dm_web/`.
- **Facility types** (`--types`, default `RTC,CSU,PSYCH`; table `AHCA_FACILITY_TYPES` in `fl_scraper.py`):
  - `RTC` — Residential Treatment Centers for Children and Adolescents, which includes Therapeutic Group Homes per Florida statute. ~36 facilities.
  - `CSU` — Crisis Stabilization Units, kept to the children's/youth units by name (CSUs are licensed for all ages).
  - `PSYCH` — Hospitals whose name says behavioral / psychiatric / mental health: the private psychiatric hospitals with adolescent units.
  - `RTF`, `SRT` — adult Residential Treatment Facilities and Short-term Residential Treatment; off by default, for young adult programs.
  - `--all-ages` turns the CSU/hospital name filters off. Every run logs the names it left out.
- **Method:** `requests` only — no PDF parsing required for this source.
- **FHF endpoint:** modern Razor Pages site. The scraper reads the Facility Type dropdown first and finds each type by its known value or, failing that, by label (so a renamed value still resolves; a missing type is logged and skipped with the options it saw). The AdvancedSearch handler returns HTML with the full facility list embedded as a JSON array, decoded with `raw_decode` from its opening bracket.
- **dm_web endpoint:** classic ASP.NET. `facility_inspection_details.aspx?file_number=...&client_code=...&provider_type=...&provider_name=...` returns a `gridView` table of deficiency rows (Survey Date · Inspection Type · Track ID · Deficiency · Requirement Description · Correction Date). The client code comes from each facility's `ClientCode` in the FHF JSON (RTC falls back to `57`). Long histories span several grid pages; the scraper follows the pager by postback (`__EVENTARGUMENT=Page$N`, cap `FL_AHCA_MAX_PAGES`, default 60). Before 2026-10 only page 1 was read and the pager row was stored as a survey dated "1234567": run `--source ahca --types RTC --full` once to post the missing pages.
- **Scraper:** `fl_scraper.py --source ahca` (launcher row "Florida (AHCA)"); `--source all` runs DJJ then AHCA. One inspection visit = one report row, grouped by (Survey Date + Track ID). All deficiencies for that visit are bundled into `categories.deficiencies`; `categories.facility_type` names the type key.
- **Incremental state:** `.fl_ahca_state.json`, seen-ID keyed by AHCA File Number; `report_id = "{track_id}-{survey_date}"` so the same Track ID with a follow-up correction date still counts as the same report. A facility licensed under two types is read once.
- **Useful flags:** `--types`, `--all-ages`, `--full`, `--no-post`, `--limit N`. Workers/PDF timeout flags don't apply (AHCA has no PDFs).
- **Tests:** `python test_fl_ahca.py` (offline fixtures: dropdown, embedded JSON, paged grid, filters).

### FL (Florida — DJJ Phase 1)
- **Source:** Florida Department of Juvenile Justice (`www.djj.state.fl.us`) — residential and detention facility directories plus QI, PREA, and SPEP report indices
- **Method:** `requests` + BeautifulSoup over static HTML index pages, then PDF download + `pdfplumber` extraction (no SOAP/auth needed)
- **Scraper:** `fl_scraper.py` dispatches on `--source`. `--source djj` and `--source ahca` are implemented (`all` runs both); DCF is a stub raising `NotImplementedError` (see above). DJJ categories are selectable via `--categories residential,detention,prea,spep` (default: all four).
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

### NH (New Hampshire child care licensing visits)
- **Source:** NH DHHS Child Care Licensing Unit search, `https://new-hampshire.my.site.com/nhccis/NH_ChildCareSearch` (Salesforce Visualforce, plain `requests`, no login; the host without the hyphen returns 502).
- **Method:** the list is a Visualforce remoting call (`POST /nhccis/apexremote`, `NH_ChildCareSearchClass.retrieveAccountRecords`, 29 arguments, the fourth `"Residential Child Care Program"`; `csrf`, `vid`, `ns`, `ver`, `authorization` read from `RemotingProviderImpl({...})` on the search page; rows at `result.v[]`, fields under `.v`). Each program's page `NH_childcaresearchaccountdetail?id=<account id>` lists its visits (`getNonComplianceItem('a4l...')`); a visit's detail is a ViewState postback to that page (`AJAXREQUEST=_viewRoot`, `selectedVisitId`). ViewState is per page load: post a program's visits in turn and reload the page when a postback comes back without the detail. Each rule appears twice in the HTML (an accessibility copy); de-duplicate by rule number.
- **Scraper:** `nh_scraper.py`, 24 programs and 146 visits on 2026-10-01. One report per visit (`report_id` = visit id, `report_url` = the program page). `program_name` = account id. `categories`: `visit_type`, `is_complaint`, `announced`, `compliance {met, reviewed}`, `licensor`, `cap_accepted_date`, `items[] {rule, rule_text, domain, result, observations, corrective_action_plan, ...}` (rules not met only: "Non-Compliant" and "Founded, Problem Resolved", as the state's compliance level counts them), `item_count`, `rules_reviewed`, `documents`.
- **Statements of findings:** the "Visit Documents" PDFs are archived through `ReportStore` to the Drive folder `nh_pdfs/`; scans are OCR'd and fill blanks only (rule numbers must match the rule pattern; the page's own text always wins). A document held by the privacy check is not archived, linked or read, and the visit posts from the page data alone.
- **Rolling window:** the state shows three years and drops programs that lose their licence. Every page and visit response is saved gzipped to the Drive folder `nh_html/<account id>/`; `--from-saved DIR` rebuilds the payload with no requests. Run it monthly at the least.
- **Edits:** corrective action plans and acceptance dates arrive after a visit, so `.nh_state.json` keeps a content hash per visit and re-posts a visit whose hash changed.
- **Useful flags:** `--full`, `--no-post`, `--limit N`, `--account <id>` (repeatable), `--out file.json`, `--from-saved DIR`.

### NC (North Carolina MHLCS public records)
- **Source:** NC DHHS Division of Health Service Regulation Mental Health Licensure and Certification Section public records directory at `results.asp`, with facility pages at `facility.asp?fid=...`
- **Method:** `requests` + BeautifulSoup to read the directory and facility pages, then OCR each linked inspection PDF with `pdf2image` + `pytesseract`
- **Scraper:** `nc_scraper.py` uses the workbook as the seed list, matches each license to the public directory, and posts OCRed inspection reports for each matched facility
- **Key detail:** Every report PDF is OCRed, even when the PDF also contains embedded text. The workbook is only the facility seed list; the report content comes from the public NC inspection PDFs
- **Useful flags:** `--input`, `--limit N`, `--no-post`, `--full`

### WY (Wyoming Family Services findings and health department PRTF surveys)
- **Source 1:** DFS `https://dfs.wyo.gov/providers/substitute-care/notice-of-non-compliance-findings-and-facility-visits/`, one page of accordions (`main-text="<provider>"`) linking Google Drive files and folders. Files download from `https://drive.google.com/uc?export=download&id=<id>` (check the body starts with `%PDF`; Drive answers HTML for quota, virus-scan and sign-in pages); folders list without login at `https://drive.google.com/embeddedfolderview?id=<id>`, and files a folder holds that the page lacks are taken too (copies of page documents are recognised and not posted twice). 2 s between Drive requests.
- **Source 2:** WDH `https://ohlssurvey.health.wyo.gov` JSON API (`api/FacilitySearch/Search`, `api/FacilitySurveySearch/Search`, `sortBy` must be a list; `api/Survey/Download/<SurveyId>` is the CMS-2567 PDF). Facility type 16 (PRTF) by default, `--wdh-types` to widen.
- **Scraper:** `wy_scraper.py --source dfs|wdh|all`, state files `.wy_dfs_state.json` and `.wy_wdh_state.json`. 26 facilities and 413 documents on 2026-10-01. DFS `program_name` = `DFS-<slug>` fixed once assigned (two providers that share documents stay separate records); WDH `program_name` = `WDH-<facility id>`. `report_id` = the Drive file id or `wdh-<SurveyId>`.
- **Kinds** (`categories.kind`): `notice` (SCL-305, typed scan, OCR'd: `allegation`, `allegation_date`, `finding`, `rules[]`), `visit` (SCL-300, handwritten: never transcribed, no text posted, always neutral; the SCL-300 header wins over the form's "serves as notice" fine print), `other` (corrective action plans, recertifications), `survey` (2567; scanned surveys' plans of correction posted as one block, `outcome` cited/clean/unread). A notice with no finding read is neutral, never clean.
- **Photos:** JPEG photos of visit forms are wrapped as one-page PDFs (Pillow) so the archive sync, which takes PDFs only, picks them up.
- **Archive:** everything through `ReportStore` to the Drive folder `wy_pdfs/`; the state removes a provider's documents when it leaves the list. Held documents move to `.report_extract_cache/wy_held/`. A file Drive will not share (sign-in page) is marked `unavailable` and retried only with `--full`.
- **Useful flags:** `--full`, `--no-post`, `--limit N`, `--facility NAME` (repeatable), `--no-folders`, `--reparse` (read archived PDFs before the source), `--out file.json`. Run it monthly.

### UT (Utah OCR/CSV export)
- **Source:** Utah facility JSON endpoint at `ccl.utah.gov`
- **Method:** `requests` JSON fetches plus CSV export
- **Script:** `utah_citation_scraper.py` -- writes OCR-enhanced checklist data to JSON plus a flattened CSV export for newly observed inspections
- **Key detail:** This script is not part of the WordPress inspections API pipeline, but it uses the same seen-ID state pattern keyed by facility ID and inspection date, so reruns only process newly observed inspections unless `--full` is used.

### ID (Idaho Health and Welfare children's residential surveys)
- **Source:** the department's public Laserfiche WebLink repository: residential facility folders under `Browse.aspx?id=19853`, the wilderness folder `id=19852` (Blue Fire), the provider list PDF `id=20035` (`https://publicdocuments.dhw.idaho.gov/WebLink/`, plain `requests`, no login). Folders list by `POST FolderListingService.aspx/GetFolderListing2` (`{"repoName":"PUBLIC-DOCUMENTS","folderId":<id>,"getNewListing":true,"start":0,"end":500,...}`, `type` 0 folder, -2 document); a document downloads from `ElectronicFile.aspx?docid=<entryId>&dbid=0&repo=PUBLIC-DOCUMENTS`, and `DocView.aspx?id=<entryId>` is the human link.
- **Scraper:** `id_scraper.py`, 40 facilities and 156 documents on 2026-10-01. One report per document: statements of deficiencies (`kind: deficiencies`; per deficiency the rule, finding, rule text, plan of correction, correct-by date, repeat flag; tables read with `extract_tables()` and joined across page breaks) and no-deficiency letters (`kind: no_deficiencies`). `report_date` = the survey end date. Facility names are the state's folder names; address, licence, beds and ages come from the provider list.
- **Unlisted folders:** a folder with no provider-list row keeps its reports, with `action` "Not on the state's current provider list" and `categories.provider.listed=false`; folders sharing a licence number stay separate and name each other in `categories.same_license_as`. A folder matching two provider rows (Mountaintop) is one facility with `categories.provider.sites`.
- **Archive:** PDFs through `ReportStore` to the Drive folder `id_pdfs/`. State file `.id_state.json` (seen reports and the folder registry).
- **Useful flags:** `--full`, `--no-post`, `--cached` (folder listings and PDFs from the cache, no requests), `--limit N`, `--out file.json`.

### ME (Maine behavioral health organization surveys)
- **Source:** the state licence lookup `https://www.pfr.maine.gov/almsonline/almsquery/searchcompany.aspx?board=6706` (form posts; the board's full licence list as CSV, each licence's page with its survey history, and since late 2024 the survey documents as PDFs). Plain `requests`, no login.
- **Scope:** an operator allowlist in `me_scope.json` (licence, name, `scope` in/out/unsure, why; settled by the owner 2026-10-01). The licence covers a whole organization, so every survey of an allowed licence is posted, and one whose document names only adult programs gets `adult_program: true` (hidden on the page by default). Every licence not listed is out.
- **Scraper:** `me_scraper.py`, 16 licences and 400 surveys on 2026-10-02. One facility per licence (`program_name` = licence number). Documents: statements of deficiencies, no-deficiency statements and plans of correction, OCR'd where scanned (upside-down pages re-read rotated). Flagged = outcome "Accepted plan of correction" or a parsed deficiency; waived surveys are neutral.
- **Archive:** PDFs through `ReportStore` to the Drive folder `me_pdfs/`; pages saved for `--from-saved`. The date of birth check needs a value after the words (the rule text names "date of birth").
- **Useful flags:** `--full`, `--no-post`, `--limit N`, `--licence <number>` (repeatable), `--out file.json`, `--from-saved DIR`, `--csv-copy FILE`.

### OH (Ohio Department of Children and Youth compliance reviews)
- **Source:** the agency search `https://odjfs2.my.site.com/FindFosterCareAdoptionAgencies/s/` (Salesforce Aura: `getFosterCareAdoptionAgenciesForMapView` for the list, `getAgencyDetails` per agency; report PDFs through Salesforce content delivery links, four requests each). Plain `requests`, no login.
- **Scope:** agencies that run group homes or children's residential centers (159 agencies, 303 facilities on 2026-10-01); foster-only agencies are out. Reports exist only from July 2025.
- **Scraper:** `oh_scraper.py`, one facility row per agency (`program_name` = the OFCLA agency id), its facilities listed in the categories. One report per compliance review (Full, Focused, Other), joined with its "Additional Findings" file. Findings of noncompliance flag a report; technical assistance items are kept apart and never flag.
- **Robustness:** six retries with backoff (3 to 48 s), then the agency is skipped and listed; agency lists cached 12 hours (`--refresh` asks again); PDFs cached by review number.
- **Archive:** PDFs through `ReportStore` to the Drive folder `oh_pdfs/`. Run it monthly: the state keeps posting.
- **Useful flags:** `--full`, `--no-post`, `--limit N`, `--agency <id>` (repeatable), `--refresh`, `--out file.json`.

### WV (West Virginia OHFLAC surveys)
- **Source:** OHFLAC's public facility-search JSON, per-facility survey-history HTML, and generated State/Federal CMS-2567 PDFs.
- **Scope:** `wv_scope.json` is an explicit allowlist. Only `decision: "included"` records are scraped; the 16 `unsure` entries are excluded until the owner settles them.
- **Scraper:** `wv_scraper.py`; report extraction keeps the Helvetica report layer and filters the stale embedded Arial template layer. Calibration covered 55 varied reports (35 State, 20 Federal; 2000-2026): mean OCR word recall was 98.1%, minimum 90.3%, with no unexpected fonts. A deterministic 5% sample is also OCR'd; sampled reports with unavailable OCR or below 90% recall are held for review. A C 173/F 156 finding is held only when it is absent from OCR, since a real 2016 C 173 finding was confirmed in the visible report.
- **Archive:** PDFs through `ReportStore` to `wv_pdfs/`, synced by `api/sync-inspection-archive.php` to the site's `wv` archive directory.
- **Useful flags:** `--full`, `--refresh` (re-download/re-extract all reports), `--no-post`, `--limit N`, `--facility <id>` (repeatable), `--ocr-share 0.05`, `--out file.json`.
- **First post:** do not post until the owner resolves the unsure scope rows, approves the first post, and confirms Drive capacity for the archive.

### IA (Iowa DIAL psychiatric medical institutions for children)
- **Source:** the Department of Inspections, Appeals and Licensing health facilities database `https://dia-hfd.iowa.gov/` (ASP.NET Core; plain `requests`, every POST carries the anti-forgery token of the page before it). `POST /Home/EntitySearchAjax` lists PMICs (type 11, active and closed), `POST /home/VisitListAjax?id=<entity>` a facility's visits, `GET /Home/ViewReport?fileName=<name>` one CMS-2567 PDF.
- **Scraper:** `ia_scraper.py`, 46 institutions and 246 visits (2018-09 to 2026-08) on 2026-10-03. One facility per institution (`program_name` "IA-<entity id>"), one report per visit (`report_id` = visit id). The 2567's two columns (findings, plan of correction) are split at the form's rules, scans read by OCR. Flagged = the state's own violation counts (federal + state > 0), not the parsed tags; 15 visits where the parsed tag count differs are listed at the end of a run.
- **Re-posting:** the state replaces a visit's file when the plan of correction arrives, so the state file keeps `<visit>|<file>|<fed>|<state>` and a changed visit is posted again under the same `report_id`.
- **Archive:** PDFs through `ReportStore` to the Drive folder `ia_pdfs/`. State file `.ia_state.json`.
- **Useful flags:** `--full`, `--refresh`, `--no-post`, `--limit N`, `--entity <id>` (repeatable), `--out file.json`.

### MD (Maryland DHS residential child care inspection summaries)
- **Source:** the Office of Licensing and Monitoring public file browser `https://dhs.maryland.gov/documents/?dir=Licensing-and-Monitoring/Reports` (plain `requests`): one folder per provider, only its `RCC` subfolder read.
- **Scraper:** `md_scraper.py`, 20 providers and 155 reports (2019-01 to 2025-05) on 2026-10-03. One facility per provider; a report lists the sites inspected and each citation names its site. Three forms: the 2019 report (unrated citations), the 2019-2021 summary and the 10/2021 summary (citations in two blocks, "may present safety risks for children" and "do not present imminent safety risks", each with a status). Tables are read cell by cell (`find_tables`), never from `extract_text()`. Flagged = at least one citation.
- **Privacy:** a report whose text carries a person's initials, a date of birth or a record number is left out whole and listed at the end of the run (15 on 2026-10-03, initials of youths and staff in citation comments).
- **Archive:** PDFs through `ReportStore` to the Drive folder `md_pdfs/`. State file `.md_state.json`.
- **Useful flags:** `--full`, `--no-post`, `--limit N`, `--out file.json`.

### SD (South Dakota DSS youth care provider documents)
- **Source:** the Office of Licensing and Accreditation portal `https://olapublic.sd.gov/youth-care-provider-search/` (plain `requests`): the provider list, each provider's profile with its Documents section, PDFs from `/api/mcase/attachments/<id>`.
- **Scope:** residential treatment, intensive residential treatment, group care, shelter care and independent living; child placement agencies out. The list shows current providers only, so the state file keeps every profile link seen.
- **Scraper:** `sd_scraper.py`, 27 providers and 118 documents (2024-05 to 2026-09) on 2026-10-03. One report per document, `categories.kind` licensing_study (each rule section answered Yes/No/N/A; flagged when one is No), corrective_action_plan (always flagged) or inspection (fire, health and safety forms; flagged on a failed item; scanned DPS forms are kept unread). Program certificates are skipped. The portal has no complaint documents.
- **Privacy:** a document carrying a date of birth, a named child or a record number is left out and removed from the archive folder.
- **Archive:** PDFs through `ReportStore` to the Drive folder `sd_pdfs/`. State file `.sd_state.json`.
- **Useful flags:** `--full`, `--no-post`, `--limit N`, `--out file.json`.

### VA (Virginia VDSS and DBHDS residential licensing)
- **Sources:** VDSS children's residential facility inspection pages at `https://www.dss.virginia.gov/licensed-care/search-licensing-programs/childrens-residential-facility-search/`; DBHDS youth residential service searches at `https://vadbhdsv7prod.glsuite.us/GLSuiteWeb/Clients/vadbhds/Public/ProviderSearch/ProviderSearchSearch.aspx` (Playwright only mints the gateway clearance cookie; the scraper then uses `requests`).
- **Scope:** VDSS children's residential facilities and DBHDS psychiatric residential, therapeutic group home, crisis stabilization and youth substance-use services. DBHDS developmental-disability services are excluded.
- **Scraper:** `va_scraper.py --source vdss|dbhds|all`. VDSS publishes inspection findings but not plans of correction. DBHDS records one inspection or investigation per service licence; only finalized plans are available, and its online records begin in late 2021. A report without a DBHDS plan is neutral, not a clean inspection.
- **Privacy:** records containing a date of birth, likely full resident name, medical record number or an individual street address are held out for review.
- **Archive:** report PDFs through `ReportStore` to `va_pdfs/`; the state keeps independent `.va_vdss_state.json` and `.va_dbhds_state.json` cursors.
- **Useful flags:** `--source vdss|dbhds|all`, `--full`, `--no-post`, `--limit N`, `--out file.json`, `--licence <id>` (repeatable), `--service-type <label>` (repeatable), and `--refresh`.
- **DBHDS plans:** a plan whose form reads "No Violation" is a clean result, not a parse failure. A service type with no licences answers "Your search did not return any results" and is read as empty.
- **Slow walk:** each plan is a "View CAP" click, about 5 minutes per DBHDS service licence (165 on 2026-10-03), so a first full run takes 10+ hours. It resumes from the saved service pages and cached plan extractions; run it detached (`Start-Process`).
- **First post:** do not post either source until the owner approves it and reviews any privacy holds.

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
- **Make failures visible.** A failed fetch or downstream write must produce a nonzero process exit, not a success-shaped empty run.
- **Handle None values.** State APIs often return `null` for optional fields. Use `value or ""` instead of `value` to avoid `TypeError` on string operations.
- **The `categories` dict is your escape hatch.** Each state's data is different. Put whatever structured data the frontend needs into `categories` -- it's stored as JSON and passed through unchanged.
- **Use cursor state when the source supports it.** Server-side date filters are much cheaper than fetching everything and deduping locally.
- **Sort reports newest-first.** Do this in the frontend JS when converting API data. Watch out for date formats that `new Date()` can't parse (date ranges, non-standard formats).
- **Test with 2-3 facilities first.** Run `scraper.scrape(facility_ids=["id1", "id2"])` before doing the full run.
- **Use `--full` after suspected backfills.** For parser changes in a scraper with a separate extraction cache, use its cache-refresh option (WV: `--refresh`) as well as bypassing incremental state.
- **Salesforce sites** use the Aura framework. The `fwuid` in the context string changes on deploys. If the AZ scraper breaks, open the site in a browser, check the Network tab for an `aura` request, and copy the new `fwuid`.
