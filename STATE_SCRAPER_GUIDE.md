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

## Existing Implementations

### CT (Connecticut)
- **Source:** Single HTML table at `licensefacilities.dcf.ct.gov`
- **Method:** `requests` + BeautifulSoup (no browser needed)
- **Scraper:** `ct_scraper.py` -- parses HTML table rows into facilities, extracts report content from cells, categorizes structured DCF reports
- **Frontend:** `js/inspections/ct_reports.js` -- renders structured report categories (areas covered, non-compliance, corrective actions, recommendations)

### TX (Texas)
- **Source:** TX HHS Childcare Search at `childcare.hhs.texas.gov`
- **Method:** `requests` against the site's internal JSON API (same endpoints the React frontend calls)
- **API pattern:** Get auth token -> search by operation number -> get compliance history
- **Scraper:** `tx_scraper.py` -- three HTTP calls per facility, maps deficiency fields to match the CSV column names the frontend expects
- **Frontend:** `js/inspections/tx_reports.js` -- renders TX citation fields (Standard Number, Risk Level, Deficiency Narrative, Correction Narrative, etc.)
- **Key detail:** `categories` stores TX-specific fields like `Citation Date`, `Standard Risk Level`, `Sections Violated`, etc.

### AZ (Arizona)
- **Source:** AZ Care Check at `azcarecheck.azdhs.gov` (Salesforce Lightning Community)
- **Method:** `requests` against Salesforce Aura/Apex endpoints (no browser needed)
- **API pattern:** Call Apex controllers directly via POST to `/s/sfsites/aura`
- **Scraper:** `az_scraper.py` -- calls `getFacilityDetails`, `getFacilityOrLicenseInspections`, and `getInspectionItemSODWrap`
- **Frontend:** `js/inspections/az_reports.js` -- renders inspection list with deficiency rule/evidence/findings; falls back to legacy JSON files if API is empty
- **Key detail:** Salesforce Aura calls need a `fwuid` context string that may change when Salesforce deploys updates. If the scraper starts failing, capture a fresh `fwuid` from the browser network tab.

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
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import requests

from inspection_api_client import post_facilities_to_api

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("INSPECTIONS_API_KEY", "CHANGE_ME")

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

    def scrape(self, facility_ids: Optional[List[str]] = None) -> List[Dict]:
        ids = facility_ids or FACILITY_IDS
        logger.info(f"Starting XX scrape for {len(ids)} facilities")

        for i, fid in enumerate(ids):
            logger.info(f"[{i+1}/{len(ids)}] {fid}")
            try:
                facility_info = self._get_facility(fid)
                reports = self._get_reports(fid)
                self.all_facilities.append({
                    "facility_info": facility_info,
                    "reports": reports,
                })
            except Exception as e:
                logger.error(f"  ERROR: {e}")
                continue

        logger.info(f"Scraping complete: {len(self.all_facilities)} facilities")
        return self.all_facilities


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
    scraper = XXFacilityScraper()
    facilities = scraper.scrape()
    if facilities:
        logger.info(f"Scraped {len(facilities)} facilities -- posting to API")
        if save_to_api(facilities):
            logger.info("Saved successfully!")
        else:
            logger.error("API save failed")
    else:
        logger.warning("No facilities scraped")


if __name__ == "__main__":
    main()
```

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

## Tips

- **Prefer APIs over scraping HTML.** Most state sites have JSON APIs behind their frontends. Check the Network tab before writing a scraper.
- **Use `requests`, not Playwright.** Browser automation is 100x slower and breaks when sites update their UI. Every state so far has had a direct API.
- **Handle None values.** State APIs often return `null` for optional fields. Use `value or ""` instead of `value` to avoid `TypeError` on string operations.
- **The `categories` dict is your escape hatch.** Each state's data is different. Put whatever structured data the frontend needs into `categories` -- it's stored as JSON and passed through unchanged.
- **Sort reports newest-first.** Do this in the frontend JS when converting API data. Watch out for date formats that `new Date()` can't parse (date ranges, non-standard formats).
- **Test with 2-3 facilities first.** Run `scraper.scrape(facility_ids=["id1", "id2"])` before doing the full run.
- **Salesforce sites** use the Aura framework. The `fwuid` in the context string changes on deploys. If the AZ scraper breaks, open the site in a browser, check the Network tab for an `aura` request, and copy the new `fwuid`.
