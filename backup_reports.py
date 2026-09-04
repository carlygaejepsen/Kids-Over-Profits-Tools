"""Push locally cached inspection reports to the FileBird Google Drive folder.

Scrapers write reports straight to the Drive folder whenever Google Drive for
Desktop is mounted (see kop_paths.report_cache_dir). This tool handles the
backlog that accumulated in the local cache directories while it wasn't:

    python backup_reports.py            # scan: show what is/isn't backed up
    python backup_reports.py --migrate  # upload missing files, then delete
                                        # local copies verified on Drive

A local file is considered backed up when a file with the same name and size
exists in the matching Drive subfolder. Size mismatches are reported and left
untouched in both places.

Notes:
- The local caches live inside OneDrive and are mostly dehydrated
  placeholders; copying one makes OneDrive re-download it first, so a full
  migration moves gigabytes in both directions. Run it on a good connection
  and let it finish.
- --migrate deletes a local file only after the Drive copy exists with the
  same size. Deleting the local copy also removes it from OneDrive — that is
  intentional: the Drive/FileBird folder becomes the single cloud home, and
  FileBird imports it into the kidsoverprofits.org media library.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from kop_paths import GOOGLE_DRIVE_BASE

BASE_DIR = Path(__file__).resolve().parent

# local cache directory (relative to this repo) -> subfolder of GOOGLE_DRIVE_BASE
REPORT_CACHES = [
    (".nc_pdf_cache", "nc_pdfs"),
    (".nc_ocr_cache", "nc_ocr"),
    (".ar_pdf_cache", "ar_pdfs"),
    ("fl_pdfs", "fl_pdfs"),
    ("or_pdfs", "or_pdfs"),
    ("wa_pdfs", "wa_pdfs"),
    ("checklists", "ut_checklists"),
]


def list_files(root: Path) -> dict[str, Path]:
    """Map path-relative-to-root -> Path for every file under root."""
    if not root.is_dir():
        return {}
    return {
        str(path.relative_to(root)): path
        for path in root.rglob("*")
        if path.is_file()
    }


def compare(local_dir: Path, drive_dir: Path):
    local = list_files(local_dir)
    drive = list_files(drive_dir)

    backed_up: list[Path] = []
    missing: list[tuple[str, Path]] = []
    mismatched: list[str] = []

    for rel, path in sorted(local.items()):
        drive_path = drive.get(rel)
        if drive_path is None:
            missing.append((rel, path))
        elif drive_path.stat().st_size == path.stat().st_size:
            backed_up.append(path)
        else:
            mismatched.append(rel)

    return backed_up, missing, mismatched


def migrate_pair(local_dir: Path, drive_dir: Path) -> tuple[int, int, int]:
    """Upload missing files, then delete verified local copies.

    Returns (uploaded, deleted, failed).
    """
    backed_up, missing, mismatched = compare(local_dir, drive_dir)
    uploaded = deleted = failed = 0

    for rel, path in missing:
        dest = drive_dir / rel
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest)
            if dest.stat().st_size != path.stat().st_size:
                raise OSError(f"size mismatch after copy: {dest}")
        except OSError as exc:
            print(f"    FAILED upload {rel}: {exc}")
            failed += 1
            continue
        uploaded += 1
        backed_up.append(path)

    for path in backed_up:
        try:
            path.unlink()
        except OSError as exc:
            print(f"    FAILED delete {path.name}: {exc}")
            failed += 1
            continue
        deleted += 1

    for rel in mismatched:
        print(f"    size mismatch, left in place: {rel}")

    return uploaded, deleted, failed


def human_mb(paths) -> str:
    total = sum(p.stat().st_size for p in paths)
    return f"{total / 1_048_576:,.1f} MB"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="upload missing files to Drive, then delete local copies verified there",
    )
    args = parser.parse_args()

    if not GOOGLE_DRIVE_BASE.exists():
        print(f"Google Drive folder not found: {GOOGLE_DRIVE_BASE}")
        print(
            "Start Google Drive for Desktop (H: must be mounted) and re-run. "
            "Nothing was changed."
        )
        return 1

    grand_failed = 0
    for local_name, drive_subdir in REPORT_CACHES:
        local_dir = BASE_DIR / local_name
        drive_dir = GOOGLE_DRIVE_BASE / drive_subdir
        if not local_dir.is_dir() or not any(local_dir.iterdir()):
            continue

        print(f"{local_name} -> {drive_dir}")
        if args.migrate:
            uploaded, deleted_count, failed = migrate_pair(local_dir, drive_dir)
            grand_failed += failed
            print(f"    uploaded {uploaded}, deleted {deleted_count} local copies, {failed} failures")
            if not any(local_dir.iterdir()):
                # OneDrive can hold a lock on the freshly emptied directory;
                # leaving the empty dir behind is fine.
                try:
                    local_dir.rmdir()
                    print("    local directory now empty, removed")
                except OSError:
                    print("    local directory now empty (left in place, locked)")
        else:
            backed_up, missing, mismatched = compare(local_dir, drive_dir)
            print(
                f"    already on Drive (safe to delete locally): {len(backed_up)}"
                f" ({human_mb(backed_up)})"
            )
            print(
                f"    not yet on Drive (would upload): {len(missing)}"
                f" ({human_mb(p for _, p in missing)})"
            )
            if mismatched:
                print(f"    size mismatches (left alone): {len(mismatched)}")

    if not args.migrate:
        print("\nScan only — run with --migrate to upload and delete local copies.")
    return 1 if grand_failed else 0


if __name__ == "__main__":
    sys.exit(main())
