"""Locate the Kids-Over-Profits web repo from the Tools repo.

The scrapers live in this Tools repo but read/write files in the separate
Kids-Over-Profits web repo (the `.env`, the NC workbook, `js/data/*.json`).
Historically the web repo sat next to the Tools repo under `.../GitHub/`, so
the code hardcoded `Path(__file__).parents[2] / "Kids-Over-Profits"`. The web
repo has since moved to `~/source/repos/Kids-Over-Profits`, which broke every
scraper that resolved a path that way.

Rather than hardcode a single location, resolve it at runtime so moving the
repo again doesn't require touching every scraper. Set `KOP_REPO_DIR` to
override.
"""

import os
from pathlib import Path

# Files/dirs that only exist in the web repo — used to confirm a candidate is
# actually the repo and not just an empty directory that happens to exist.
_MARKERS = (".env.example", "js/data", "nc_youth_facilities.xlsx")


def _looks_like_kop_repo(path: Path) -> bool:
    return path.is_dir() and any((path / marker).exists() for marker in _MARKERS)


def _candidates() -> list[Path]:
    candidates: list[Path] = []

    override = os.environ.get("KOP_REPO_DIR", "").strip()
    if override:
        candidates.append(Path(override).expanduser())

    home = Path.home()
    candidates += [
        # Current canonical location.
        home / "source" / "repos" / "Kids-Over-Profits",
        # Legacy sibling-of-Tools layout (.../GitHub/Kids-Over-Profits).
        Path(__file__).resolve().parents[2] / "Kids-Over-Profits",
        home / "OneDrive" / "Documents" / "GitHub" / "Kids-Over-Profits",
    ]
    return candidates


def kop_repo_dir() -> Path:
    """Return the Kids-Over-Profits web repo directory.

    Returns the first candidate that looks like the repo. If none can be
    verified (e.g. the repo isn't cloned yet), returns the preferred canonical
    location so callers still have a Path to report in error messages.
    """
    candidates = _candidates()
    for candidate in candidates:
        if _looks_like_kop_repo(candidate):
            return candidate
    return candidates[0]


# Google Drive for Desktop mount of the folder FileBird imports into the
# kidsoverprofits.org media library. Downloaded inspection reports belong here,
# not on this machine: writing into it uploads them to Drive, and FileBird
# turns them into site documents.
_DRIVE_FOLDER_NAME = "FileBird Cloud - kidsoverprofits.org"


def _find_drive_base() -> Path:
    """Locate the FileBird folder on whatever letter Drive for Desktop mounts.

    The mount letter is per-account and has changed before (H:, then G:), so
    scan every drive rather than hardcode one. KOP_DRIVE_BASE overrides. When
    nothing is found, return the historical H: path so callers still have a
    Path to test .exists() on and report in messages.
    """
    override = os.environ.get("KOP_DRIVE_BASE", "").strip()
    if override:
        return Path(override).expanduser()

    # Folder-style mounts live under the profile as "My Drive (email)".
    candidates = [
        mount / _DRIVE_FOLDER_NAME for mount in sorted(Path.home().glob("My Drive*"))
    ]
    candidates += [
        Path(f"{letter}:\\My Drive") / _DRIVE_FOLDER_NAME
        for letter in "DEFGHIJKLMNOPQRSTUVWXYZ"
    ]

    for candidate in candidates:
        try:
            if candidate.is_dir():
                return candidate
        except OSError:
            continue

    return Path(r"H:\My Drive") / _DRIVE_FOLDER_NAME


GOOGLE_DRIVE_BASE = _find_drive_base()


def report_cache_dir(env_name: str, drive_subdir: str, local_fallback: Path) -> Path:
    """Resolve where a scraper stores downloaded reports.

    Preference order: the env override, the FileBird Google Drive folder
    (so reports land in the cloud with no local copy), then the scraper's
    historical local directory when Drive for Desktop isn't mounted. Local
    accumulations can later be pushed to Drive with backup_reports.py.
    """
    env_value = os.environ.get(env_name, "").strip()
    if env_value:
        return Path(env_value).expanduser()
    if GOOGLE_DRIVE_BASE.exists():
        return GOOGLE_DRIVE_BASE / drive_subdir
    return Path(local_fallback)
