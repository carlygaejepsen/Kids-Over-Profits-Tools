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

### UT (Utah CSV export)
- **Source:** Utah facility JSON endpoint at `ccl.utah.gov`
- **Method:** `requests` JSON fetches plus CSV export
- **Script:** `utah_citation_scraper.v2.py` -- writes one CSV row per facility with up to `MAX_INSPECTIONS` new inspections
- **Key detail:** This script is not part of the WordPress inspections API pipeline, but it now uses the same seen-ID state pattern so reruns only write newly observed inspection dates unless `--full` is used.

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
