# Kids Over Profits Tools

Python scrapers and support utilities for collecting state inspection and citation data, then posting normalized facility/report payloads to the Kids Over Profits inspections API.

## Main files

- `*_scraper.py`: state-specific scrapers and exporters
- `inspection_api_client.py`: shared batched POST helper for the WordPress/MySQL API
- `scraper_state.py`: shared helpers for incremental scrape state files
- `STATE_SCRAPER_GUIDE.md`: architecture notes and the pattern for adding a new state

## Incremental scraping

Most active state scrapers now run incrementally by default. Each scraper keeps a local JSON state file such as `.az_state.json` or `.or_state.json`, skips reports already posted, and only advances that state after a successful write.

- Use `--full` to ignore the saved state and force a complete re-scan/re-post.
- `ar_scraper.py` uses a per-category `last_run` cursor and the upstream WordPress `modified_after` filter.
- `az_scraper.py`, `ca_scraper.py`, `ct_scraper.py`, `or_scraper.py`, `tx_scraper.py`, and `wa_scraper.py` track previously posted report IDs.
- `utah_citation_scraper.py` now uses the same pattern for OCR-enhanced Utah exports, keyed by facility ID and inspection date.
- `or_scraper.py` is incremental-only; use `--full` to re-scan Oregon reports without clearing existing rows.
- `nc_scraper.py` scrapes the NC DHSR MHLCS public records directory and OCRs the inspection PDFs linked from each facility page, using the workbook as the seed list.

## Local artifacts

Per-scraper state files are gitignored. Some scrapers also keep local PDF/text caches and generated CSV outputs in the repo working tree as runtime artifacts.

## Running

Set `INSPECTIONS_API_KEY` before posting to the live API. `INSPECTIONS_API_URL` is optional and defaults to the production endpoint.

Examples:

```bash
python az_scraper.py
python az_scraper.py --full
python or_scraper.py --views TBS --no-post
```

See `STATE_SCRAPER_GUIDE.md` for the shared data model, API contract, and implementation notes by state.
