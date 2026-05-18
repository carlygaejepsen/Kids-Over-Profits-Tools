"""Re-process the PREA PDFs that timed out during the previous --full rescrape.

Reads fl_rescrape.log to find the 28 file_names that pdfplumber wedged on,
re-runs the PREA index parser to get their report metadata, rebuilds the
reports (now using the pymupdf fallback when pdfplumber times out), and POSTs
just those facilities back to the inspections API. Doesn't touch the state
file — the existing entries get upserted with the now-populated raw_content
and structured findings.
"""
import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from fl_scraper import (
    FLDJJScraper,
    save_to_api,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

LOG_PATH = Path("fl_rescrape.log")
TIMEOUT_RE = re.compile(r"TIMED OUT.*for (\S+\.pdf);")


def main() -> None:
    if not LOG_PATH.exists():
        raise SystemExit(f"{LOG_PATH} not found in current directory")

    timeout_files = set()
    for line in LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
        m = TIMEOUT_RE.search(line)
        if m:
            timeout_files.add(m.group(1))
    logger.info(f"Found {len(timeout_files)} timed-out PDFs in the previous run log")
    if not timeout_files:
        logger.info("Nothing to reprocess")
        return

    scraper = FLDJJScraper()
    # Need the directory match index to assign each PREA entry to a facility.
    residential = scraper.fetch_residential_directory()
    detention = scraper.fetch_detention_directory()
    for fac in residential + detention:
        scraper.fetch_facility_profile(fac)
    directory = residential + detention
    slug_to_record = {f["slug"]: f for f in directory}
    match_index = scraper._build_match_index(directory)

    # Pull only the PREA index — every timeout was a PREA PDF
    prea_entries = scraper.fetch_prea_index()
    targeted = [e for e in prea_entries if e["file_name"] in timeout_files]
    logger.info(f"Matched {len(targeted)} index entries to timed-out filenames")
    missing = timeout_files - {e["file_name"] for e in targeted}
    if missing:
        logger.warning(f"{len(missing)} timed-out filenames not found in current PREA index:")
        for fn in sorted(missing):
            logger.warning(f"  {fn}")

    # Group by facility (matched) or by synthetic unmatched
    grouped: dict = defaultdict(list)
    unmatched: list = []
    for entry in targeted:
        fac = scraper._match_to_facility(entry, match_index)
        if fac:
            grouped[fac["slug"]].append(entry)
        else:
            unmatched.append(entry)

    facilities_payload = []
    for slug, entries in grouped.items():
        entries.sort(key=lambda e: (e.get("report_date", ""), e.get("report_id", "")))
        reports = [scraper._build_report(e, slug_to_record[slug]) for e in entries]
        facilities_payload.append(scraper._facility_payload(slug_to_record[slug], reports))

    unmatched_by_program: dict = defaultdict(list)
    for entry in unmatched:
        from fl_scraper import _slugify
        unmatched_by_program[_slugify(entry.get("program_name", "")) or "unknown"].append(entry)
    for prog_slug, entries in unmatched_by_program.items():
        entries.sort(key=lambda e: (e.get("report_date", ""), e.get("report_id", "")))
        reports = [scraper._build_report(e, None) for e in entries]
        facilities_payload.append(scraper._unmatched_payload(entries[0], reports))

    logger.info(
        f"Built {sum(len(f['reports']) for f in facilities_payload)} reports across "
        f"{len(facilities_payload)} facilities — POSTing to API"
    )
    if save_to_api(facilities_payload):
        logger.info("Reprocess saved successfully")
    else:
        logger.error("API save failed")


if __name__ == "__main__":
    main()
