"""
Arkansas PRTF Scraper — Disability Rights Arkansas

Pulls all PRTF facility documents from disabilityrightsar.org via the WP REST API,
downloads the linked Google Drive PDFs, extracts text with pdfplumber, and posts
the data to the Kids-Over-Profits inspections API.
"""
import argparse
import io
import logging
import os
import re
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from inspection_api_client import post_facilities_to_api

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    import pytesseract
    from pdf2image import convert_from_bytes
except ImportError:
    pytesseract = None
    convert_from_bytes = None

# Optional: point at custom binaries via env vars
TESSERACT_CMD = os.getenv("TESSERACT_CMD")
POPPLER_PATH = os.getenv("POPPLER_PATH")
if TESSERACT_CMD and pytesseract:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
OCR_DPI = int(os.getenv("OCR_DPI", "250"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# pdfminer (under pdfplumber) is very chatty about missing CropBox / font issues
for noisy in ("pdfminer", "pdfminer.pdfpage", "pdfminer.pdfinterp",
              "pdfminer.cmapdb", "pdfplumber"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("INSPECTIONS_API_KEY", "CHANGE_ME")

DRA_BASE = "https://disabilityrightsar.org/wp-json/wp/v2"
PDF_CACHE_DIR = Path(os.getenv("AR_PDF_CACHE", ".ar_pdf_cache"))
TEXT_CACHE_DIR = Path(os.getenv("AR_TEXT_CACHE", ".ar_text_cache"))

# Map DRA category slug -> display facility name. The REST term names are short
# ("Centers Little Rock") so we expand them here for the public-facing UI.
FACILITY_NAMES = {
    "centers-little-rock":     "Centers for Youth and Families - Little Rock",
    "centers-monticello":      "Centers for Youth and Families - Monticello",
    "delta":                   "Delta Family Services",
    "little-creek":            "Little Creek Behavioral Health",
    "methodist-dacus":         "United Methodist Children's Home - Dacus (Bono)",
    "methodist-little-rock":   "United Methodist Children's Home - Little Rock",
    "millcreek":               "Millcreek Behavioral Health / Habilitation Center, Inc.",
    "perimeter-forrest-city-2":"Perimeter Behavioral of Forrest City",
    "perimeter-ozarks-2":      "Perimeter Behavioral of the Ozarks",
    "perimeter-west-memphis-2":"Perimeter Behavioral of West Memphis",
    "timber-ridge":            "Timber Ridge / NeuroRestorative Timber Ridge",
    "yellow-rock":             "Yellow Rock Behavioral Health (formerly Piney Ridge)",
    "youth-home":              "Youth Home",
}

DRIVE_FILE_RE = re.compile(r"drive\.google\.com/file/d/([A-Za-z0-9_-]+)")


def strip_html(s: str) -> str:
    if not s:
        return ""
    return unescape(re.sub(r"<[^>]+>", "", s)).strip()


def parse_date_from_title(title: str) -> str:
    """Most DRA titles start with a date like '9/8/2025' or '02/19/2025'."""
    m = re.match(r"\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", title)
    return m.group(1) if m else ""


def doc_type_from_title(title: str) -> str:
    """'9/8/2025 Police Report' -> 'Police Report'."""
    cleaned = re.sub(r"^\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\s*", "", title).strip()
    return cleaned or "Document"


class DRAScraper:
    def __init__(self, download_pdfs: bool = True, ocr: bool = True):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; KOP-AR-Scraper/1.0)",
        })
        self.download_pdfs = download_pdfs
        self.ocr = ocr and (pytesseract is not None) and (convert_from_bytes is not None)
        self.tag_cache: Dict[int, str] = {}
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        TEXT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        if ocr and not self.ocr:
            logger.warning("OCR requested but pytesseract/pdf2image not installed; "
                           "scans will have empty raw_content.")

    # --- WP REST helpers -------------------------------------------------

    def _get(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        for attempt in range(3):
            try:
                r = self.session.get(url, params=params, timeout=60)
                if r.status_code == 200:
                    return r
                logger.warning(f"  GET {url} -> {r.status_code} (attempt {attempt+1})")
            except requests.RequestException as e:
                logger.warning(f"  GET {url} failed: {e} (attempt {attempt+1})")
            time.sleep(2 ** attempt)
        r.raise_for_status()
        return r

    def _load_tags(self) -> None:
        """Pull all doc_tags terms once, into id->name cache."""
        page = 1
        while True:
            r = self._get(f"{DRA_BASE}/doc_tags",
                          params={"per_page": 100, "page": page})
            data = r.json()
            if not data:
                break
            for t in data:
                self.tag_cache[t["id"]] = t["name"]
            if len(data) < 100:
                break
            page += 1
        logger.info(f"Loaded {len(self.tag_cache)} doc_tags")

    def _list_documents(self, category_id: int) -> List[Dict]:
        docs: List[Dict] = []
        page = 1
        while True:
            r = self._get(
                f"{DRA_BASE}/dlp_document",
                params={"doc_categories": category_id, "per_page": 100,
                        "page": page, "_embed": "false"},
            )
            batch = r.json()
            if not batch:
                break
            docs.extend(batch)
            total_pages = int(r.headers.get("X-WP-TotalPages", "1"))
            if page >= total_pages:
                break
            page += 1
        return docs

    def _list_categories(self) -> List[Dict]:
        r = self._get(f"{DRA_BASE}/doc_categories", params={"per_page": 100})
        return r.json()

    # --- PDF handling ----------------------------------------------------

    def _drive_id(self, url: str) -> Optional[str]:
        if not url:
            return None
        m = DRIVE_FILE_RE.search(url)
        return m.group(1) if m else None

    def _download_pdf(self, drive_id: str) -> Optional[bytes]:
        cache_path = PDF_CACHE_DIR / f"{drive_id}.pdf"
        if cache_path.exists():
            return cache_path.read_bytes()

        url = f"https://drive.google.com/uc?export=download&id={drive_id}"
        try:
            r = self.session.get(url, timeout=120, allow_redirects=True)
        except requests.RequestException as e:
            logger.warning(f"    PDF download failed for {drive_id}: {e}")
            return None
        if r.status_code != 200 or not r.content:
            logger.warning(f"    PDF download {drive_id} -> {r.status_code}")
            return None
        # Drive sometimes returns an HTML interstitial for large files
        if r.content[:4] != b"%PDF":
            logger.debug(f"    {drive_id}: non-PDF response (likely Drive interstitial)")
            return None
        cache_path.write_bytes(r.content)
        return r.content

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        if not pdfplumber:
            return ""
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
            return "\n\n".join(pages).strip()
        except Exception as e:
            logger.debug(f"    pdfplumber failed: {e}")
            return ""

    def _ocr_pdf(self, pdf_bytes: bytes, drive_id: str) -> str:
        if not self.ocr:
            return ""
        try:
            kwargs = {"dpi": OCR_DPI}
            if POPPLER_PATH:
                kwargs["poppler_path"] = POPPLER_PATH
            images = convert_from_bytes(pdf_bytes, **kwargs)
        except Exception as e:
            logger.warning(f"    pdf2image failed for {drive_id}: {e}")
            return ""
        pages = []
        for img in images:
            try:
                pages.append(pytesseract.image_to_string(img))
            except Exception as e:
                logger.warning(f"    tesseract failed for {drive_id}: {e}")
                return ""
        return "\n\n".join(pages).strip()

    def _process_pdf(self, drive_url: str) -> Tuple[str, str]:
        """Returns (raw_text, drive_id_or_empty)."""
        drive_id = self._drive_id(drive_url)
        if not drive_id or not self.download_pdfs:
            return "", drive_id or ""

        text_cache = TEXT_CACHE_DIR / f"{drive_id}.txt"
        if text_cache.exists():
            return text_cache.read_text(encoding="utf-8", errors="replace"), drive_id

        pdf_bytes = self._download_pdf(drive_id)
        if not pdf_bytes:
            return "", drive_id

        text = self._extract_pdf_text(pdf_bytes)
        if not text and self.ocr:
            logger.info(f"    OCR fallback for {drive_id}")
            text = self._ocr_pdf(pdf_bytes, drive_id)

        if text:
            text_cache.write_text(text, encoding="utf-8")
        return text, drive_id

    # --- main scrape -----------------------------------------------------

    def _build_facility(self, slug: str, term_name: str, docs: List[Dict]) -> Dict:
        reports: List[Dict] = []
        for d in docs:
            title = strip_html(d.get("title", {}).get("rendered", ""))
            excerpt = strip_html(d.get("excerpt", {}).get("rendered", ""))
            drive_url = d.get("download_url", "") or ""
            tags = [self.tag_cache.get(tid, str(tid)) for tid in d.get("doc_tags", [])]

            raw_text, drive_id = self._process_pdf(drive_url)
            report_date = parse_date_from_title(title) or (d.get("date", "") or "")[:10]

            reports.append({
                "report_id": d.get("slug") or str(d.get("id")),
                "report_date": report_date,
                "report_url": d.get("link", ""),
                "raw_content": raw_text,
                "content_length": len(raw_text),
                "is_structured": False,
                "summary": excerpt,
                "categories": {
                    "doc_type": doc_type_from_title(title),
                    "tags": tags,
                    "pdf_url": drive_url,
                    "drive_file_id": drive_id,
                    "doc_page_url": d.get("link", ""),
                    "post_date": d.get("date", ""),
                    "modified_date": d.get("modified", ""),
                },
            })

        return {
            "facility_info": {
                "facility_name": FACILITY_NAMES.get(slug, term_name),
                "program_name": f"DRA-{slug}",
                "program_category": "Psychiatric Residential Treatment Facility",
                "full_address": "",
                "phone": "",
                "executive_director": "",
                "bed_capacity": "",
                "license_exp_date": "",
                "relicense_visit_date": "",
                "action": "",
            },
            "reports": reports,
        }

    def scrape(self, slugs: Optional[List[str]] = None) -> List[Dict]:
        self._load_tags()
        categories = self._list_categories()
        if slugs:
            categories = [c for c in categories if c["slug"] in slugs]

        results: List[Dict] = []
        for i, cat in enumerate(categories, 1):
            slug = cat["slug"]
            logger.info(f"[{i}/{len(categories)}] {slug} ({cat.get('count', '?')} docs)")
            docs = self._list_documents(cat["id"])
            logger.info(f"  fetched {len(docs)} documents")
            facility = self._build_facility(slug, cat["name"], docs)
            results.append(facility)
            text_extracted = sum(1 for r in facility["reports"] if r["content_length"] > 0)
            logger.info(f"  extracted text from {text_extracted}/{len(facility['reports'])} PDFs")
        return results


def save_to_api(facilities: List[Dict]) -> bool:
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="AR",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=180,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slugs", nargs="*", help="Limit to these facility slugs")
    ap.add_argument("--no-pdfs", action="store_true",
                    help="Skip PDF download / text extraction")
    ap.add_argument("--no-ocr", action="store_true",
                    help="Skip OCR fallback for image-only PDFs")
    ap.add_argument("--no-post", action="store_true",
                    help="Skip posting to API (dry run)")
    args = ap.parse_args()

    if not pdfplumber and not args.no_pdfs:
        logger.warning("pdfplumber not installed — PDF text will be empty. "
                       "Install with: pip install pdfplumber")

    scraper = DRAScraper(download_pdfs=not args.no_pdfs, ocr=not args.no_ocr)
    facilities = scraper.scrape(slugs=args.slugs)

    if not facilities:
        logger.warning("No facilities scraped")
        return

    total_reports = sum(len(f["reports"]) for f in facilities)
    logger.info(f"Scraped {len(facilities)} facilities, {total_reports} reports")

    if args.no_post:
        logger.info("Dry run — not posting to API")
        return

    if save_to_api(facilities):
        logger.info("Data saved to database successfully!")
    else:
        logger.error("API save failed — check logs above")


if __name__ == "__main__":
    main()
