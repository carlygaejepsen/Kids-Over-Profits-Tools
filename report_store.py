"""Download -> extract -> archive for scraped inspection report PDFs.

The PDFs belong in the FileBird Google Drive folder, not on this machine.
Drive for Desktop (Stream mode) keeps a local copy of every file it writes
or reads on the I: mount, so a scraper that re-opens archived PDFs on each
run pulls the whole archive back into Drive's local cache. The flow here
keeps the Drive folder write-only in normal runs:

  1. extracted data (text, parsed findings) is cached locally as small JSON;
     a cache hit never touches the PDF;
  2. on a miss the PDF is downloaded into memory, extracted from a temporary
     file that is deleted straight after, and the extraction cached;
  3. the PDF is written once to the Drive folder (skipped when an identical
     copy is already there) and nothing else keeps it locally.

The archived copy is only read back when the source no longer serves the
document.
"""

from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

from kop_paths import report_cache_dir

BASE_DIR = Path(__file__).resolve().parent
EXTRACT_CACHE_ROOT = Path(os.environ.get("KOP_EXTRACT_CACHE", BASE_DIR / ".report_extract_cache"))


class ReportStore:
    def __init__(self, env_name: str, drive_subdir: str, local_fallback: Path):
        self.archive_dir = report_cache_dir(env_name, drive_subdir, local_fallback)
        self.extract_dir = EXTRACT_CACHE_ROOT / drive_subdir

    # -- extracted data (local, small) ------------------------------------

    def _extract_path(self, name: str) -> Path:
        return self.extract_dir / f"{name}.json"

    def cached_extract(self, name: str) -> Optional[dict]:
        path = self._extract_path(name)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None

    def save_extract(self, name: str, data: dict) -> None:
        self.extract_dir.mkdir(parents=True, exist_ok=True)
        self._extract_path(name).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    # -- PDFs (Drive folder, written once) ----------------------------------

    def archive(self, name: str, data: bytes) -> Path:
        """Write the PDF to the Drive folder unless an identical copy exists.

        Only stat() is used to check, which does not download the file.
        """
        dest = self.archive_dir / name
        try:
            if dest.stat().st_size == len(data):
                return dest
        except OSError:
            pass
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return dest

    def archived_bytes(self, name: str) -> Optional[bytes]:
        """Read an archived PDF back; for when the source has dropped it."""
        path = self.archive_dir / name
        try:
            data = path.read_bytes()
        except OSError:
            return None
        return data if data.startswith(b"%PDF") else None

    @staticmethod
    @contextmanager
    def working_copy(data: bytes, name: str) -> Iterator[Path]:
        """A temporary file for extractors that need a path; deleted on exit."""
        suffix = Path(name).suffix or ".pdf"
        fd, tmp = tempfile.mkstemp(prefix="kop_report_", suffix=suffix)
        path = Path(tmp)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            yield path
        finally:
            try:
                path.unlink()
            except OSError:
                pass


def extract_with_cache(store: ReportStore, name: str, fetch, extract) -> Optional[dict]:
    """Return the cached extraction for `name`, or fetch, extract and archive it.

    fetch() returns the PDF bytes from the source (None on failure);
    extract(path) returns a JSON-serialisable dict with a "text" key, cached
    only when that text is non-empty. Returns None when no
    document could be had from the source or the archive.
    """
    cached = store.cached_extract(name)
    if cached is not None:
        return cached

    data = fetch()
    if data:
        store.archive(name, data)
    else:
        data = store.archived_bytes(name)
        if not data:
            return None

    with store.working_copy(data, name) as path:
        result: Any = extract(path)
    # An empty extraction (a timed-out or failed parse) is retried next run.
    if result.get("text"):
        store.save_extract(name, result)
    return result
