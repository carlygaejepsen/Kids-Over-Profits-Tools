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
import subprocess
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


# --- Google Drive and OneDrive must be running ------------------------------
# When Drive for Desktop is closed the Drive folder vanishes and reports are
# saved on this machine instead; when OneDrive is closed, files in this repo
# that are only in the cloud can't be read. Both went unnoticed on 2026-09-30
# (1.6 GB of PDFs saved locally). So the tools ask: a box names the program to
# open, Retry checks again, Cancel carries on the old way.

_asked: dict = {}


def _ask_retry(title: str, message: str) -> bool:
    """Show a Retry/Cancel box (a console prompt if there is no desktop).
    True means Retry."""
    try:
        import ctypes
        MB_RETRYCANCEL, MB_ICONWARNING, MB_TOPMOST, IDRETRY = 0x5, 0x30, 0x40000, 4
        answer = ctypes.windll.user32.MessageBoxW(
            None, message, title, MB_RETRYCANCEL | MB_ICONWARNING | MB_TOPMOST)
        return answer == IDRETRY
    except Exception:
        pass
    try:
        reply = input(f"\n{title}\n{message}\nPress Enter to retry, or type c then Enter to carry on: ")
    except (EOFError, OSError):
        return False
    return reply.strip().lower() != "c"


def _is_running(image_name: str) -> bool:
    try:
        out = subprocess.run(["tasklist", "/FI", f"IMAGENAME eq {image_name}", "/NH"],
                             capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError):
        return True  # can't tell, so don't ask
    return image_name.lower() in out.lower()


def ensure_google_drive() -> bool:
    """Ask until the Drive folder is there. False when the user chose Cancel;
    asked once per run."""
    global GOOGLE_DRIVE_BASE
    if "drive" in _asked:
        return _asked["drive"]
    while True:
        GOOGLE_DRIVE_BASE = _find_drive_base()
        if GOOGLE_DRIVE_BASE.exists():
            _asked["drive"] = True
            return True
        if not _ask_retry(
            "Google Drive isn't running",
            "Open Google Drive for Desktop from the Start menu, wait until the "
            "I: drive appears, then click Retry.\n\n"
            "Cancel carries on without it: downloaded reports are saved on this "
            "computer instead, and backup_reports.py --migrate can move them to "
            "Drive later.",
        ):
            print("Google Drive isn't running; saving reports on this computer for now.")
            _asked["drive"] = False
            return False


def ensure_onedrive() -> bool:
    """Ask until OneDrive is running, when this repo is inside OneDrive. False
    when the user chose Cancel; asked once per run."""
    if "onedrive" in _asked:
        return _asked["onedrive"]
    if "onedrive" not in str(Path(__file__).resolve()).lower():
        _asked["onedrive"] = True
        return True
    while not _is_running("OneDrive.exe"):
        if not _ask_retry(
            "OneDrive isn't running",
            "These tools live in your OneDrive folder, and files that are only in "
            "the cloud can't be read until OneDrive is running. Open OneDrive from "
            "the Start menu, then click Retry.\n\n"
            "Cancel carries on without it; anything that needs OneDrive fails and "
            "can be retried later.",
        ):
            print("OneDrive isn't running; carrying on, some files may fail.")
            _asked["onedrive"] = False
            return False
    _asked["onedrive"] = True
    return True


def report_cache_dir(env_name: str, drive_subdir: str, local_fallback: Path) -> Path:
    """Resolve where a scraper stores downloaded reports.

    Preference order: the env override, the FileBird Google Drive folder
    (so reports land in the cloud with no local copy), then the scraper's
    historical local directory. When Drive for Desktop or OneDrive isn't
    running, the user is asked to open it first (Retry), or to carry on with
    the local directory (Cancel); backup_reports.py can move those later.
    """
    env_value = os.environ.get(env_name, "").strip()
    if env_value:
        return Path(env_value).expanduser()
    ensure_onedrive()
    if ensure_google_drive():
        return GOOGLE_DRIVE_BASE / drive_subdir
    return Path(local_fallback)
