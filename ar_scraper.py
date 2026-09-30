"""
Arkansas PRTF Scraper — Disability Rights Arkansas

Pulls PRTF facility documents from disabilityrightsar.org via the WP REST API,
downloads the PDFs, extracts text with pdfplumber (OCR for scans), and posts
the data to the Kids-Over-Profits inspections API. Two DRA collections:

  documents  The document library (dlp_document, 2023 on): PDFs on Google
             Drive, one doc category per facility.
  prtf       The older PRTF database (prtf posts, 2019-2024): PDFs uploaded to
             DRA's media library, tagged by facility, incident type and record
             category, with DRA's own summary. Records whose PDF text matches a
             library document already scraped are skipped, since DRA posted
             many 2023-24 documents in both.
"""
import argparse
import hashlib
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
from kop_paths import report_cache_dir
from scraper_state import load_state, save_state

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    import pytesseract
    from pdf2image import convert_from_bytes, pdfinfo_from_bytes
except ImportError:
    pytesseract = None
    convert_from_bytes = None
    pdfinfo_from_bytes = None

# Optional: point at custom binaries via env vars
TESSERACT_CMD = os.getenv("TESSERACT_CMD")
POPPLER_PATH = os.getenv("POPPLER_PATH")
if TESSERACT_CMD and pytesseract:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
OCR_DPI = int(os.getenv("OCR_DPI", "250"))
# Long scans are OCR'd up to this many pages; every page is rendered alone.
OCR_MAX_PAGES = int(os.getenv("OCR_MAX_PAGES", "60"))
# A run stops (and posts nothing) after this many downloads in a row fail,
# rather than saving records with no text while the network is down.
MAX_DOWNLOAD_FAILURES = 5

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
API_KEY = os.getenv("KOP_DATA_API_KEY", "CHANGE_ME")

DRA_BASE = "https://disabilityrightsar.org/wp-json/wp/v2"
PDF_CACHE_DIR = report_cache_dir("AR_PDF_CACHE", "ar_pdfs", Path(".ar_pdf_cache"))
TEXT_CACHE_DIR = Path(os.getenv("AR_TEXT_CACHE", ".ar_text_cache"))
STATE_FILE = Path(os.getenv("AR_STATE_FILE", ".ar_state.json"))

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

# PRTF database facility term slug -> the document library's category slug, so
# both collections file under one facility (program_name DRA-<category slug>).
PRTF_FACILITY_SLUGS = {
    "centers-for-youth-and-families-little-rock": "centers-little-rock",
    "centers-for-youth-and-families-monticello":  "centers-monticello",
    "delta-family-services":                      "delta",
    "little-creek":                               "little-creek",
    "methodist-dacus":                            "methodist-dacus",
    "methodist-little-rock":                      "methodist-little-rock",
    "millcreek":                                  "millcreek",
    "perimeter-of-forrest-city":                  "perimeter-forrest-city-2",
    "perimeter-of-the-ozarks":                    "perimeter-ozarks-2",
    "perimeter-of-west-memphis":                  "perimeter-west-memphis-2",
    "piney-ridge-treatment-center":               "yellow-rock",
    "timber-ridge":                               "timber-ridge",
    "youth-home":                                 "youth-home",
}

# PRTF incident types that name the kind of document rather than what happened;
# the first one on a record becomes its doc_type.
PRTF_DOC_KINDS = (
    "Notice of Incident", "Visit Compliance Report", "Licensing Compliance Record",
    "Licensing Follow Up", "Complaint Survey with POC", "Complaint Survey",
    "Revisit Survey with POC", "Validation Survey with POC", "Validation Survey",
    "Inspection of Care Report", "Information Report", "Monitor Visit",
    "Corrective Action Agreement", "Corrective Action Plan", "Notice of Sanction",
)

# PRTF incident types meaning the state found a violation. The library marks
# these documents with its "Citation" tag, which is what the site flags.
PRTF_CITATION_TYPES = {
    "Complaint Survey with POC", "Revisit Survey with POC", "Validation Survey with POC",
    "Complaint Founded", "Notice of Sanction", "Corrective Action Plan",
    "Corrective Action Agreement", "Corrective Action Agreement & Appeal",
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


def text_fingerprint(text: str) -> str:
    """Hash of a document's words, to spot the same PDF posted twice."""
    words = re.findall(r"[a-z0-9]+", (text or "").lower())
    return hashlib.sha1(" ".join(words).encode("utf-8")).hexdigest() if len(words) >= 20 else ""


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

    def _list_documents(self, category_id: int,
                        modified_after: Optional[str] = None) -> List[Dict]:
        docs: List[Dict] = []
        page = 1
        while True:
            params = {"doc_categories": category_id, "per_page": 100,
                      "page": page, "_embed": "false"}
            if modified_after:
                params["modified_after"] = modified_after
            r = self._get(f"{DRA_BASE}/dlp_document", params=params)
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
        """OCR a scanned PDF one page at a time. Rendering every page at once
        held a long scan's images in memory together and ran the machine out
        of memory (2026-09-30)."""
        if not self.ocr:
            return ""
        kwargs = {"dpi": OCR_DPI}
        if POPPLER_PATH:
            kwargs["poppler_path"] = POPPLER_PATH
        try:
            info = pdfinfo_from_bytes(pdf_bytes, poppler_path=POPPLER_PATH or None)
            page_count = int(info.get("Pages", 0))
        except Exception as e:
            logger.warning(f"    pdfinfo failed for {drive_id}: {e}")
            return ""
        if page_count > OCR_MAX_PAGES:
            logger.warning(f"    {drive_id}: {page_count} pages, OCR of the first {OCR_MAX_PAGES} only")
        pages = []
        for number in range(1, min(page_count, OCR_MAX_PAGES) + 1):
            try:
                images = convert_from_bytes(pdf_bytes, first_page=number, last_page=number, **kwargs)
                pages.extend(pytesseract.image_to_string(img) for img in images)
                del images
            except (Exception, MemoryError) as e:
                logger.warning(f"    OCR failed on page {number} of {drive_id}: {e}")
        return "\n\n".join(p for p in pages if p).strip()

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

        return self._facility_shell(slug, term_name, reports)

    @staticmethod
    def _facility_shell(slug: str, term_name: str, reports: List[Dict]) -> Dict:
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

    # --- PRTF database (older records) -------------------------------------

    def _all_pages(self, base: str, params: Dict, label: str = "") -> List[Dict]:
        out: List[Dict] = []
        page = 1
        while True:
            r = self._get(f"{DRA_BASE}/{base}", params={**params, "per_page": 100, "page": page})
            batch = r.json()
            out.extend(batch)
            pages = int(r.headers.get("X-WP-TotalPages", "1"))
            if label and (page % 10 == 0 or page == pages):
                logger.info(f"  {label}: page {page} of {pages}")
            if not batch or page >= pages:
                break
            page += 1
        return out

    def _prtf_pdf_text(self, media: Dict) -> Tuple[str, Optional[bytes]]:
        """(text, PDF bytes) of one PRTF media PDF. The bytes are None when the
        text came from the cache; the PDF was archived on that earlier run."""
        cache_key = f"media-{media['id']}"
        text_cache = TEXT_CACHE_DIR / f"{cache_key}.txt"
        if text_cache.exists():
            return text_cache.read_text(encoding="utf-8", errors="replace"), None
        if not self.download_pdfs:
            return "", None
        try:
            r = self.session.get(media["source_url"], timeout=120)
            self.download_failures = 0
        except requests.RequestException as e:
            logger.warning(f"    PDF download failed for media {media['id']}: {e}")
            self.download_failures = getattr(self, "download_failures", 0) + 1
            if self.download_failures >= MAX_DOWNLOAD_FAILURES:
                raise RuntimeError(f"{MAX_DOWNLOAD_FAILURES} PDF downloads failed in a row; "
                                   "is the network down? Stopping without posting. "
                                   "Rerun to pick up where this left off.")
            return "", None
        if r.status_code != 200 or r.content[:4] != b"%PDF":
            logger.warning(f"    PDF download media {media['id']} -> {r.status_code}")
            return "", None
        text = self._extract_pdf_text(r.content)
        if not text and self.ocr:
            logger.info(f"    OCR fallback for media {media['id']}")
            text = self._ocr_pdf(r.content, cache_key)
        if text:
            text_cache.write_text(text, encoding="utf-8")
        return text, r.content

    @staticmethod
    def _archive_prtf_pdf(media: Dict, content: bytes) -> None:
        """Write the PDF to the Drive folder as dra-media-<id>.pdf, the name the
        site's archive link looks up."""
        archive = PDF_CACHE_DIR / f"dra-media-{media['id']}.pdf"
        try:
            if archive.stat().st_size == len(content):
                return
        except OSError:
            pass
        archive.write_bytes(content)

    def _library_fingerprints(self) -> set:
        """Fingerprints of the document library's PDFs (their cached text)."""
        return {fp for path in TEXT_CACHE_DIR.glob("*.txt") if not path.name.startswith("media-")
                for fp in [text_fingerprint(path.read_text(encoding="utf-8", errors="replace"))] if fp}

    def scrape_prtf(self, slugs: Optional[List[str]] = None,
                    modified_after: Optional[str] = None) -> Tuple[List[Dict], str]:
        """PRTF database records as facilities (merged with the library's by
        facility slug on the site). Returns (facilities, newest modified)."""
        logger.info("PRTF database: listing DRA's records (a few minutes before processing starts)")
        names = lambda terms: {t["id"]: unescape(t["name"]) for t in terms}
        facilities = {t["id"]: t["slug"] for t in self._all_pages("facility", {})}
        incident_types = names(self._all_pages("incident_type", {}))
        record_categories = names(self._all_pages("record_category", {}))

        params = {"orderby": "modified", "order": "asc"}
        if modified_after:
            params["modified_after"] = modified_after
        posts = self._all_pages("prtf", params, label="records")
        logger.info(f"PRTF database: {len(posts)} records" + (f" since {modified_after}" if modified_after else ""))
        if not posts:
            return [], ""

        media_by_post: Dict[int, List[Dict]] = {}
        ids = [p["id"] for p in posts]
        for i in range(0, len(ids), 100):
            if i % 1000 == 0:
                logger.info(f"  finding PDFs: records {i + 1}-{min(i + 1000, len(ids))} of {len(ids)}")
            for m in self._all_pages("media", {"parent": ",".join(map(str, ids[i:i + 100])),
                                               "mime_type": "application/pdf",
                                               "_fields": "id,source_url,post"}):
                media_by_post.setdefault(m["post"], []).append(m)

        library = self._library_fingerprints()
        by_facility: Dict[str, List[Dict]] = {}
        duplicates = unmapped = 0
        started = time.monotonic()
        logger.info(f"PRTF database: processing {len(posts)} records. Already-read ones go fast; "
                    "new ones are downloaded and read, scans by OCR (slow). Nothing is posted "
                    "until all are done; if stopped, a rerun picks up where this left off.")
        for n, post in enumerate(posts, 1):
            if n % 25 == 0 or n == len(posts):
                kept = sum(len(v) for v in by_facility.values())
                elapsed = time.monotonic() - started
                logger.info(f"  PRTF {n}/{len(posts)}: {kept} kept, {duplicates} duplicates, "
                            f"{elapsed / 60:.0f} min so far")
            fac_slugs = [facilities.get(f, "") for f in post.get("facility", [])]
            slug = next((PRTF_FACILITY_SLUGS[f] for f in fac_slugs if f in PRTF_FACILITY_SLUGS), "")
            if not slug:
                unmapped += 1
                continue
            if slugs and slug not in slugs:
                continue
            pdfs = sorted(media_by_post.get(post["id"], []), key=lambda m: m["id"])
            fetched = [self._prtf_pdf_text(m) for m in pdfs]
            texts = [t for t, _ in fetched]
            raw_text = "\n\n".join(t for t in texts if t)
            # Already a library document: skip it, and keep no second copy.
            if any(text_fingerprint(t) in library for t in texts if t):
                duplicates += 1
                continue
            for media, (_, content) in zip(pdfs, fetched):
                if content:
                    self._archive_prtf_pdf(media, content)

            types = [incident_types.get(t, "") for t in post.get("incident_type", [])]
            types = [t for t in types if t]
            cats = [record_categories.get(c, "") for c in post.get("record_category", [])]
            if "Police Report" in cats:
                doc_type = "Police Report"
            else:
                doc_type = next((t for t in types if t in PRTF_DOC_KINDS), "") or (cats[0] if cats else "Document")
            tags = [t for t in types if t != doc_type]
            if any(t in PRTF_CITATION_TYPES for t in types):
                tags.append("Citation")
            if "Police Report" in cats and "Police Report" not in tags:
                tags.append("Police Report")

            title = strip_html(post.get("title", {}).get("rendered", ""))
            post_date = (post.get("date") or "")[:10]
            m = re.match(r"(\d{4})-(\d{2})-(\d{2})", post_date)
            report_date = f"{m[2]}/{m[3]}/{m[1]}" if m and m[1] >= "2000" else parse_date_from_title(title.replace(".", "/"))
            first = pdfs[0] if pdfs else {}
            by_facility.setdefault(slug, []).append({
                "report_id": f"prtf-{post.get('slug') or post['id']}",
                "report_date": report_date,
                "report_url": post.get("link", ""),
                "raw_content": raw_text,
                "content_length": len(raw_text),
                "is_structured": False,
                "summary": strip_html(post.get("content", {}).get("rendered", "")),
                "categories": {
                    "doc_type": doc_type,
                    "tags": tags,
                    "record_category": ", ".join(cats),
                    "pdf_url": first.get("source_url", ""),
                    "more_pdf_urls": [m["source_url"] for m in pdfs[1:]],
                    "archive_name": f"dra-media-{first['id']}.pdf" if first else "",
                    "doc_page_url": post.get("link", ""),
                    "post_date": post.get("date", ""),
                    "modified_date": post.get("modified", ""),
                    "collection": "DRA PRTF database",
                },
            })

        logger.info(f"PRTF database: {sum(len(v) for v in by_facility.values())} records kept, "
                    f"{duplicates} already in the document library, {unmapped} with no known facility")
        results = [self._facility_shell(slug, FACILITY_NAMES.get(slug, slug), reports)
                   for slug, reports in sorted(by_facility.items())]
        return results, max((p.get("modified", "") for p in posts), default="")

    def scrape(self, slugs: Optional[List[str]] = None,
               last_run: Optional[Dict[str, str]] = None
               ) -> Tuple[List[Dict], Dict[str, str]]:
        self._load_tags()
        categories = self._list_categories()
        if slugs:
            categories = [c for c in categories if c["slug"] in slugs]

        last_run = last_run or {}
        new_cursors: Dict[str, str] = {}

        results: List[Dict] = []
        for i, cat in enumerate(categories, 1):
            slug = cat["slug"]
            cursor = last_run.get(slug)
            if cursor:
                logger.info(f"[{i}/{len(categories)}] {slug} (since {cursor})")
            else:
                logger.info(f"[{i}/{len(categories)}] {slug} "
                            f"({cat.get('count', '?')} docs, full)")
            docs = self._list_documents(cat["id"], modified_after=cursor)
            logger.info(f"  fetched {len(docs)} documents")
            facility = self._build_facility(slug, cat["name"], docs)
            results.append(facility)
            if docs:
                max_mod = max((d.get("modified", "") for d in docs), default="")
                if max_mod:
                    new_cursors[slug] = max_mod
            text_extracted = sum(1 for r in facility["reports"] if r["content_length"] > 0)
            logger.info(f"  extracted text from {text_extracted}/{len(facility['reports'])} PDFs")
        return results, new_cursors


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
    ap.add_argument("--full", action="store_true",
                    help=f"Ignore {STATE_FILE} and re-scan all documents")
    ap.add_argument("--source", choices=("both", "documents", "prtf"), default="both",
                    help="documents = the 2023+ document library, prtf = the older PRTF database")
    args = ap.parse_args()

    if not pdfplumber and not args.no_pdfs:
        logger.warning("pdfplumber not installed — PDF text will be empty. "
                       "Install with: pip install pdfplumber")

    state = load_state(STATE_FILE)
    last_run = {} if args.full else state.get("last_run", {})

    scraper = DRAScraper(download_pdfs=not args.no_pdfs, ocr=not args.no_ocr)
    facilities: List[Dict] = []
    new_cursors: Dict[str, str] = {}
    # The library runs first: its cached text is what PRTF records are
    # checked against for duplicates.
    if args.source in ("both", "documents"):
        facilities, new_cursors = scraper.scrape(slugs=args.slugs, last_run=last_run)
    if args.source in ("both", "prtf"):
        prtf_facilities, prtf_cursor = scraper.scrape_prtf(slugs=args.slugs,
                                                           modified_after=last_run.get("prtf"))
        facilities += prtf_facilities
        if prtf_cursor and not args.slugs:
            new_cursors["prtf"] = prtf_cursor

    if not facilities:
        logger.warning("No facilities scraped")
        return

    total_reports = sum(len(f["reports"]) for f in facilities)
    facilities_to_post = [f for f in facilities if f["reports"]]
    logger.info(f"Scraped {len(facilities)} facilities, "
                f"{total_reports} new/changed reports")

    if not facilities_to_post:
        logger.info("No new or changed documents since last run")
        if new_cursors and not args.no_post:
            state.setdefault("last_run", {}).update(new_cursors)
            save_state(STATE_FILE, state)
        return

    if args.no_post:
        logger.info("Dry run — not posting to API; state not advanced")
        return

    if save_to_api(facilities_to_post):
        logger.info("Data saved to database successfully!")
        state.setdefault("last_run", {}).update(new_cursors)
        save_state(STATE_FILE, state)
    else:
        logger.error("API save failed — state not advanced")


if __name__ == "__main__":
    main()
