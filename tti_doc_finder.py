#!/usr/bin/env python3
r"""
tti_doc_finder.py
Find TTI-related documents on this machine that are NOT already in your
FileBird library, and file the missing ones into it.

Only PDFs and Word documents are considered: .pdf, .docx, .doc

TWO WAYS TO GET A DOCUMENT INTO THE LIBRARY

  upload   Copies it into the Google Drive folder that mirrors the library:

               <drive letter>:\My Drive\FileBird Cloud - kidsoverprofits.org\<folder>

           Drive then syncs it and the FileBird/Drive sync imports it. Same route
           the scrapers and backup_reports.py use. Quick, but it is only done when
           that sync actually runs - if the sync is misbehaving the document sits
           in the folder and never reaches the site.

  stage    Copies it into a folder with a manifest.json for the importer in the web
           repo. You scp that folder up and run api/import-documents.php, which puts
           each document in the media library and files it in its FileBird folder,
           skipping anything whose md5 is already there. Nothing to sync, nothing to
           wait for. Use this when the Drive sync can't be trusted.

Either way you pick what goes, a batch at a time, before anything moves.

NOTHING IS EVER UPLOADED WITHOUT YOU SAYING SO
    A scan only writes down what it found. You then look the candidates over in
    small batches and approve the ones you want, and only those get copied in.
    Every decision is remembered in .tti_doc_finder_state.json next to this
    script, so you can stop after five documents and pick it up next week. Two
    copies can be open at once - the window and a command prompt - and saving
    keeps what the other one decided rather than writing over it.

WHERE YOU LEFT OFF IS PICKED UP FOR YOU
    Every command starts by loading the last sitting's results, and the pickers
    lead with them: review what's waiting, upload what you approved, or scan
    again over the same folders with the same options (`scan --fresh` to start
    that over). If .tti_doc_finder_state.json has gone missing, the newest report
    CSV from an earlier scan is read back in instead, so a lost history file
    doesn't mean scanning the machine again. Only PDFs and Word documents come
    back: an older report has .txt, .md, .html and spreadsheets in it, from when
    this tool offered those too, and those rows are left where they are.

        py tti_doc_finder.py prune             # and if any did get in, this drops
        py tti_doc_finder.py prune --dry-run   # them from the review list

    A scan says what it's doing while it does it - files looked at, documents,
    TTI-related, and where it has got to - on one line in the console, and in a
    small window when you started by double-clicking.

YOU READ EACH DOCUMENT BEFORE YOU APPROVE IT
    A filename isn't always enough to say yes to, so reviewing shows you the
    document itself:

        py tti_doc_finder.py review --window   the whole list in a window: filter
                                               by name, select as many rows as you
                                               like and approve or turn down the
                                               lot at once; one row on its own
                                               shows its first pages
        py tti_doc_finder.py review            the same in the console, where
                                               'o' opens it in your PDF reader,
                                               't' prints the first pages and
                                               'd' shows it in its folder

    Either way it's the document in front of you, not just its name, and nothing
    is copied anywhere until you've said yes to it.

NAMES YOU NEVER NEED TO SEE TWICE
    Some documents are a yes on the strength of the name alone. Anything whose
    filename contains one of your auto-approve rules is approved the moment it
    turns up - by a scan, or when the history is loaded - and never reaches the
    review list. PREA audits are the rule you start with, and -, _ and spaces are
    all the same to it, so prea-audit, PREA_Audit and "PREA Audit" all count.

        py tti_doc_finder.py auto-approve                    # what the rules are
        py tti_doc_finder.py auto-approve "consent decree"   # add one, apply it now
        py tti_doc_finder.py auto-approve --remove "..."     # think better of it

    They still wait in the approved list until you upload or stage them.

QUICK START
    Double-click it, or run it with no arguments, and you get the window:

        python tti_doc_finder.py            (the same as: ... tti_doc_finder.py ui)

    Four tabs, and nothing leaves your machine from any of them without you
    saying so:

        Review   everything waiting, as a list. Filter by name, select as many
                 rows as you like, approve or turn down the lot in one go. One
                 row on its own shows its first pages, with the document itself
                 a click away.
        Scan     the folders to look in and the library to compare against, the
                 options, and a running count while it works.
        Send     copy the approved ones into the Drive folder, or stage them for
                 api/import-documents.php, or write a spreadsheet instead.
        Rules    the names that get approved on sight, tidying up, past scans.

    Everything there has a command too:
        py tti_doc_finder.py scan                  # find candidates, send nothing
        py tti_doc_finder.py review --window       # the list: filter, select, approve
        py tti_doc_finder.py review                # one at a time in the console
        py tti_doc_finder.py auto-approve "prea-audit"   # never ask me about these
        py tti_doc_finder.py upload                # copy 25 approved ones to Drive
        py tti_doc_finder.py stage                 # ...or build a folder for the importer
        py tti_doc_finder.py status                # what's waiting
        py tti_doc_finder.py history               # past scans

    Prefer a spreadsheet to the console?
        py tti_doc_finder.py export                # a CSV of everything waiting
        ... put y or n in the 'decision' column, fix 'filebird_folder', save ...
        py tti_doc_finder.py approve-from "tti_to_review_20260923_1530.csv"
        py tti_doc_finder.py upload --batch 10

    WINDOWS DEFAULTS (when you don't pass --scan)
        * your home folder (contents read)
        * every other fixed / removable drive except the Windows drive (contents read)
        * the Google Drive letter (file and folder NAMES only, so a streamed
          Drive isn't forced to download everything; override with --read-drive-contents)
        The FileBird folder itself is always excluded from the scan.

    Optional, for reading text inside PDFs:   pip install pypdf
    (.docx is read with no extra installs; .doc is scanned as raw bytes.)

HOW IT DECIDES
    1. Relevance: a file is "TTI-related" if its filename, its parent folder names,
       or its text content match the keyword list below (score >= --min-score).
    2. Already in FileBird?  Compared by exact content (size, then SHA-256), so renamed
       copies are still recognised. Only files whose size collides get hashed, so a
       streamed Google Drive downloads almost nothing.
    3. Which folder?  The names of your existing FileBird folders are matched against
       the filename and the folders the file sits in. The most specific match wins
       (a sub-folder beats its parent). No confident match -> the inbox folder.
    4. Output: a CSV, sorted with the most likely missing documents on top, plus an
       entry in the review list for each one.

STATUSES IN THE CSV
    MISSING                   not in FileBird by content or by name
    SAME_NAME_DIFFERENT_FILE  FileBird has a file with this name, but the content differs
                              (probably another version - worth a look)
    POSSIBLE_MATCH_SAME_SIZE  same size as a FileBird file but one side is cloud-only,
                              so it wasn't hashed (rerun with --hash-cloud to settle it)
    IN_FILEBIRD               only listed if you pass --include-matched

WHAT REACHES THE REVIEW LIST
    MISSING documents only, one copy each, skipping anything that could not be read
    or verified. Add --upload-same-name to also offer the other-version files.
    Identical copies in two places are offered once - the one whose name and folder
    file it best.

DECISIONS  (in .tti_doc_finder_state.json, keyed by SHA-256 so renames don't lose them)
    pending    found, not looked at yet
    approved   you said yes; the next `upload` or `stage` will take it
    rejected   you said no; never offered again
    approved   ...or an auto-approve rule matched its name, which its note says
    uploaded   copied into the Drive folder, with the path it landed at
    staged     copied into a staging folder for api/import-documents.php
    failed     the copy didn't work (retry with `upload --retry-failed`)
"""

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    # Same repo: knows which drive letter Google Drive is mounted on today.
    from kop_paths import GOOGLE_DRIVE_BASE
except Exception:
    GOOGLE_DRIVE_BASE = None

IS_WIN = os.name == "nt"
if sys.stdout is None or sys.stderr is None:      # started with pythonw: there is nowhere to print
    class _Nowhere:
        def write(self, *_a): pass
        def flush(self): pass
        def isatty(self): return False
    sys.stdout = sys.stdout or _Nowhere()
    sys.stderr = sys.stderr or _Nowhere()
for _stream in (sys.stdout, sys.stderr):          # odd characters in paths must never crash a run
    try:
        _stream.reconfigure(errors="replace")
    except Exception:
        pass


def desktop_or_home():
    """Where a file you're meant to notice should go."""
    home = os.path.expanduser("~")
    for spot in (os.path.join(home, "Desktop"), os.path.join(home, "OneDrive", "Desktop")):
        if os.path.isdir(spot):
            return spot
    return home


def _lp(path):
    """Windows: make paths longer than MAX_PATH openable."""
    if IS_WIN and len(path) > 240 and not path.startswith("\\\\?\\"):
        path = os.path.abspath(path)
        if path.startswith("\\\\"):
            return "\\\\?\\UNC\\" + path[2:]
        return "\\\\?\\" + path
    return path


def windows_drives():
    """[{root, label, gdrive}] for fixed + removable drives. Empty list off Windows."""
    if not IS_WIN:
        return []
    out = []
    try:
        import ctypes
        k = ctypes.windll.kernel32
        k.SetErrorMode(1)                                  # no "insert a disk" pop-ups
        mask = k.GetLogicalDrives()
        for i in range(26):
            if not (mask >> i) & 1:
                continue
            root = f"{chr(65 + i)}:\\"
            if k.GetDriveTypeW(ctypes.c_wchar_p(root)) not in (2, 3):   # removable, fixed
                continue
            label = ctypes.create_unicode_buffer(261)
            fs = ctypes.create_unicode_buffer(261)
            if not k.GetVolumeInformationW(ctypes.c_wchar_p(root), label, 261, None, None, None, fs, 261):
                continue                                   # empty card reader etc.
            gdrive = "google drive" in label.value.lower() or (
                os.path.isdir(root + "My Drive") and fs.value.upper().startswith("FAT"))
            out.append({"root": root, "label": label.value, "gdrive": gdrive})
    except Exception:
        pass
    return out


def default_scan_plan():
    """Return (roots_to_scan, roots_where_only_names_are_read)."""
    home = os.path.expanduser("~")
    roots, names_only = [home], []
    sysdrive = (os.environ.get("SystemDrive", "C:") + "\\").upper()
    for d in windows_drives():
        if d["gdrive"]:
            roots.append(d["root"]); names_only.append(d["root"])
        elif d["root"].upper() != sysdrive:
            roots.append(d["root"])
    # drop roots nested inside another root
    roots = [r for r in roots if not any(r != o and _is_under(r, o) for o in roots)]
    return roots, names_only


def find_filebird_folders(max_depth=3):
    """Look for folders with 'filebird' in the name on Google Drive mounts / mirrored Drive."""
    if GOOGLE_DRIVE_BASE is not None:
        # kop_paths already knows today's mount letter; trust it and skip the search.
        try:
            if GOOGLE_DRIVE_BASE.is_dir():
                return [str(GOOGLE_DRIVE_BASE)]
        except OSError:
            pass
    starts = [d["root"] for d in windows_drives() if d["gdrive"]]
    home = os.path.expanduser("~")
    starts += [p for p in (os.path.join(home, "My Drive"), os.path.join(home, "Google Drive")) if os.path.isdir(p)]
    found = []
    for start in starts:
        stack = [(start, 0)]
        while stack:
            d, depth = stack.pop()
            try:
                with os.scandir(d) as it:
                    for e in it:
                        if e.is_dir(follow_symlinks=False) and not e.name.startswith((".", "$")):
                            if "filebird" in e.name.lower().replace(" ", ""):
                                found.append(e.path)
                            elif depth < max_depth:
                                stack.append((e.path, depth + 1))
            except OSError:
                continue
    return found


# --------------------------------------------------------------------------------------
# KEYWORDS  (term, weight)   3 = specific to the TTI, 2 = industry phrase, 1 = weak signal
# Add your own with --keywords my_terms.txt  (one per line, optional ",weight")
# --------------------------------------------------------------------------------------
TERMS = {
    3: [
        # orgs / networks / parent companies
        "kids over profits", "natsap", "wwasp", "wwasps", "world wide association of specialty programs",
        "aspen education", "cedu", "synanon", "straight inc", "straight incorporated", "the seed inc",
        "elan school", "provo canyon", "sequel youth", "sequel tsi", "sequel pomegranate",
        "universal health services", "uhs", "acadia healthcare", "embark behavioral",
        "family help & wellness", "family help and wellness", "fhw", "altior",
        "youth services international", "ysi", "hyde school", "trails carolina",
        "teen challenge", "agape boarding", "turn-about ranch", "turnabout ranch",
        "diamond ranch academy", "island view", "spring creek lodge", "tranquility bay",
        "casa by the sea", "cross creek", "ivy ridge", "mount bachelor academy",
        "rocky mountain academy", "cascade school", "north star expeditions",
        "challenger foundation", "roloff", "rebekah home", "bluefire wilderness",
        "open sky wilderness", "second nature wilderness", "aspiro", "visionquest",
        "vision quest", "devereux", "rite of passage", "abraxas", "cornerstone programs",
        "woodbury reports", "struggling teens", "breaking code silence", "unsilenced",
        "ieca", "independent educational consultants association",
        "stop institutional child abuse", "sicaa", "aurora center for healing", "gooned",
        # unmistakable phrases
        "prea audit", "prison rape elimination",
        "troubled teen", "troubled teen industry", "tti", "wilderness therapy",
        "therapeutic boarding school", "therapeutic boarding", "attack therapy",
        "emotional growth school", "youth transport", "teen transport", "gooning",
    ],
    2: [
        "residential treatment", "residential treatment center", "residential treatment facility",
        "wilderness program", "behavior modification", "behaviour modification",
        "educational consultant", "congregate care", "youth residential", "boot camp",
        "psychiatric residential", "prtf", "qrtp", "level system", "licensing inspection",
        "prea", "licensing violation", "statement of deficiencies", "plan of correction",
        "institutional child abuse", "foia", "public records request", "aversive",
    ],
    1: [
        "seclusion", "restraint", "juvenile", "licensing", "inspection report", "deficiency",
        "group home", "boarding school", "adjudicated", "survivor", "maltreatment",
        "child abuse", "neglect", "lawsuit", "complaint", "deposition", "paris hilton",
    ],
}

# File types ---------------------------------------------------------------------------
# Documents only. Everything else on the machine is ignored outright.
DOC_EXTS = {".pdf", ".docx", ".doc"}

# Where documents go when no folder name matches them.
DEFAULT_INBOX = "Doc Finder Inbox"
DEFAULT_LIBRARY_HINT = r"I:\My Drive\FileBird Cloud - kidsoverprofits.org"

SKIP_DIR_NAMES = {
    "node_modules", "__pycache__", "site-packages", "venv", "env", "appdata",
    "program files", "program files (x86)", "programdata", "windows", "$recycle.bin",
    "system volume information", "applications", "system", "caches", "cache",
    "steamapps", "lib", "bin", "obj", "dist", "build", "vendor",
    # Windows
    "windows.old", "$windows.~bt", "$windows.~ws", "$winreagent", "$sysreset", "msocache",
    "perflogs", "recovery", "windowsapps", "onedrivetemp", "intel", "amd", "nvidia",
    "config.msi", "documents and settings", "drivers", "xboxgames",
}

MAX_CONTENT_BYTES = 60 * 1024 * 1024   # don't open files bigger than this for text
MAX_TEXT_CHARS = 300_000               # only search the first N characters
PDF_MAX_PAGES = 6


# --------------------------------------------------------------------------------------
# Keyword matching
# --------------------------------------------------------------------------------------
def _norm_term(s):
    return re.sub(r"[\s_\-.]+", " ", s.lower()).strip()


class Matcher:
    def __init__(self, weighted_terms):
        self.weights = {}
        for term, w in weighted_terms:
            t = _norm_term(term)
            if t:
                self.weights[t] = max(w, self.weights.get(t, 0))
        parts = []
        for t in sorted(self.weights, key=len, reverse=True):
            parts.append(r"[\s_\-.]+".join(re.escape(p) for p in t.split(" ")))
        self.rx = re.compile(
            r"(?<![A-Za-z0-9])(?:" + "|".join(parts) + r")(?:e?s)?(?![A-Za-z0-9])",
            re.IGNORECASE,
        )

    def find(self, text):
        """Return {term: weight} for distinct terms found in text."""
        hits = {}
        for m in self.rx.finditer(text):
            t = _norm_term(m.group(0))
            for cand in (t, t[:-1] if t.endswith("s") else None, t[:-2] if t.endswith("es") else None):
                if cand and cand in self.weights:
                    hits[cand] = self.weights[cand]
                    break
        return hits


# --------------------------------------------------------------------------------------
# File helpers
# --------------------------------------------------------------------------------------
def is_cloud_only(st):
    """Best-effort: is this a cloud placeholder whose bytes aren't on disk?"""
    attrs = getattr(st, "st_file_attributes", 0)          # Windows
    if attrs & (0x1000 | 0x40000 | 0x400000):             # OFFLINE | RECALL_ON_OPEN | RECALL_ON_DATA_ACCESS
        return True
    if getattr(st, "st_flags", 0) & 0x40000000:           # macOS SF_DATALESS
        return True
    return False


def norm_name(filename):
    """Normalise a filename so 'Copy of Report (1).PDF' ~ 'report.pdf' ~ 'Report-1'... (WordPress-style too)."""
    stem, ext = os.path.splitext(filename)
    s = stem.lower()
    s = re.sub(r"^copy of\s+", "", s)
    s = re.sub(r"\s*-\s*copy(\s*\(\d+\))?$", "", s)
    s = re.sub(r"\s*\(\d+\)$", "", s)
    s = re.sub(r"-(scaled|\d{2,4}x\d{2,4})$", "", s)      # WordPress image variants
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s + ext.lower()


_hash_cache = {}


def sha256(path):
    if path in _hash_cache:
        return _hash_cache[path]
    h = hashlib.sha256()
    try:
        with open(_lp(path), "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        digest = h.hexdigest()
    except OSError:
        digest = None
    _hash_cache[path] = digest
    return digest


def walk(root, skip_prefixes, include_hidden):
    """Yield (path, DirEntry, stat) for every file under root. Never follows symlinks."""
    stack = [root]
    home_library = os.path.join(os.path.expanduser("~"), "Library")
    while stack:
        d = stack.pop()
        try:
            it = os.scandir(d)
        except OSError:
            continue
        with it:
            for e in it:
                try:
                    if e.is_symlink():
                        continue
                    if e.is_dir(follow_symlinks=False):
                        low = e.name.lower()
                        if low in SKIP_DIR_NAMES:
                            continue
                        if not include_hidden and e.name.startswith((".", "$", "~")):
                            continue
                        p = e.path
                        if os.path.normcase(p) == os.path.normcase(home_library):
                            # skip macOS ~/Library, except where Google Drive / iCloud live
                            cs = os.path.join(p, "CloudStorage")
                            if os.path.isdir(cs):
                                stack.append(cs)
                            continue
                        if any(_is_under(p, sp) for sp in skip_prefixes):
                            continue
                        stack.append(p)
                    elif e.is_file(follow_symlinks=False):
                        yield e.path, e, e.stat(follow_symlinks=False)
                except OSError:
                    continue


def _is_under(path, parent):
    path = os.path.normcase(os.path.abspath(path))
    parent = os.path.normcase(os.path.abspath(parent))
    return path == parent or path.startswith(parent.rstrip("\\/") + os.sep)


# --------------------------------------------------------------------------------------
# Saying what it's doing while it does it
#
# A scan of a whole machine takes minutes, and a tool that prints nothing for five of
# them looks broken. One line keeps itself up to date in the console, and the same
# words go in the little window when you started by double-clicking.
# --------------------------------------------------------------------------------------
def _human_secs(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60:02d}s"
    return f"{seconds // 3600}h {(seconds % 3600) // 60:02d}m"


def _shorten(text, width):
    """Trim a path from the left, so the part that tells you where you are survives."""
    text = str(text)
    if width < 8 or len(text) <= width:
        return text
    return "..." + text[-(width - 3):]


class Progress:
    """A running count you can watch, throttled by the clock rather than by file count.

    Checking the time once per file costs nothing; formatting a line and drawing it
    does, so that only happens a couple of times a second however fast the drive is.
    """

    def __init__(self, window=None, interval=0.4, heartbeat=20.0):
        self.window = window            # a GuiProgress, or None for console only
        self.interval = interval
        self.heartbeat = heartbeat      # how often to print when there's no terminal to redraw
        self.t0 = time.time()
        self.last = 0.0
        self.printed = 0.0
        self.tty = bool(getattr(sys.stderr, "isatty", lambda: False)())
        self.open_line = False

    def _width(self):
        try:
            return max(40, shutil.get_terminal_size((100, 25)).columns - 1)
        except Exception:
            return 99

    def stage(self, name):
        """A new phase of the work. Ends the line the last one was updating."""
        self.end_line()
        print(name, file=sys.stderr, flush=True)
        if self.window:
            self.window.stage(name)
        self.last = self.printed = 0.0

    def tick(self, detail="", force=False, **counts):
        """counts are shown as '12,345 files - 87 documents'; detail is where we are."""
        now = time.time()
        if not force and now - self.last < self.interval:
            return
        self.last = now
        text = "  " + " - ".join(f"{v:,} {k.replace('_', ' ')}" for k, v in counts.items())
        text += f" - {_human_secs(now - self.t0)}"
        if self.window:
            self.window.tick(text.strip(), detail)
        if self.tty:
            width = self._width()
            if detail:
                text += "   " + _shorten(detail, max(12, width - len(text) - 3))
            print("\r" + text[:width].ljust(width), end="", file=sys.stderr, flush=True)
            self.open_line = True
        elif force or now - self.printed >= self.heartbeat:
            self.printed = now
            print(text, file=sys.stderr, flush=True)

    def end_line(self):
        """Leave the running line where it is and start printing below it."""
        if self.open_line:
            print(file=sys.stderr, flush=True)
            self.open_line = False

    def note(self, text):
        self.end_line()
        print(text, file=sys.stderr, flush=True)
        if self.window:
            self.window.stage(text)


class GuiProgress:
    """The same counts in a small window, for when there's no console to look at."""

    def __init__(self, tk, ttk, root, title="TTI Doc Finder - working"):
        self.tk = tk
        self.win = tk.Toplevel(root)
        self.win.title(title)
        self.win.resizable(False, False)
        self.win.protocol("WM_DELETE_WINDOW", lambda: None)   # closing it wouldn't stop the scan
        self.stage_var = tk.StringVar(value="Starting...")
        self.count_var = tk.StringVar(value="")
        self.detail_var = tk.StringVar(value="")
        tk.Label(self.win, textvariable=self.stage_var, anchor="w", justify="left",
                 wraplength=520, font=("Segoe UI", 10, "bold")).pack(fill="x", padx=16, pady=(16, 6))
        self.bar = ttk.Progressbar(self.win, mode="indeterminate", length=520)
        self.bar.pack(padx=16, pady=2)
        self.bar.start(60)
        tk.Label(self.win, textvariable=self.count_var, anchor="w").pack(fill="x", padx=16, pady=(8, 0))
        tk.Label(self.win, textvariable=self.detail_var, anchor="w", justify="left",
                 wraplength=520, fg="#555555").pack(fill="x", padx=16, pady=(2, 16))
        tk.Label(self.win, text="Nothing is uploaded by a scan - you choose what goes up afterwards.",
                 anchor="w", fg="#777777").pack(fill="x", padx=16, pady=(0, 12))
        self._pump()

    def _pump(self):
        """The scan runs on this thread, so the window only redraws when we let it."""
        try:
            self.win.update()
        except Exception:
            pass

    def stage(self, name):
        self.stage_var.set(name)
        self.detail_var.set("")
        self._pump()

    def tick(self, counts, detail=""):
        self.count_var.set(counts)
        self.detail_var.set(_shorten(detail, 90))
        self._pump()

    def close(self):
        try:
            self.bar.stop()
            self.win.destroy()
        except Exception:
            pass


# --------------------------------------------------------------------------------------
# Text extraction (best effort, never raises)
# --------------------------------------------------------------------------------------
_TAG = re.compile(r"<[^>]+>")

try:
    from pypdf import PdfReader  # optional
    import logging
    logging.getLogger("pypdf").setLevel(logging.CRITICAL)
except Exception:  # pragma: no cover
    PdfReader = None


def _zip_xml_text(path, wanted):
    out, total = [], 0
    with zipfile.ZipFile(_lp(path)) as z:
        for n in z.namelist():
            if wanted(n):
                out.append(_TAG.sub(" ", z.read(n).decode("utf-8", "ignore")))
                total += len(out[-1])
                if total > MAX_TEXT_CHARS:
                    break
    return " ".join(out)


def extract_text(path, ext, size):
    """Return (text, note). note is '' | 'no-text' | 'too-big' | 'unreadable' | 'needs-pypdf'."""
    if size > MAX_CONTENT_BYTES:
        return "", "too-big"
    try:
        if ext == ".pdf":
            if PdfReader is None:
                return "", "needs-pypdf"
            r = PdfReader(_lp(path), strict=False)
            pages = r.pages[:PDF_MAX_PAGES]
            text = " ".join((pg.extract_text() or "") for pg in pages)
            return (text, "") if text.strip() else ("", "no-text")
        if ext == ".docx":
            text = _zip_xml_text(path, lambda n: n.startswith("word/") and n.endswith(".xml"))
        else:  # .doc - no parser, but the words are in there as plain bytes
            with open(_lp(path), "rb") as f:
                raw = f.read(2 * 1024 * 1024)
            text = raw.decode("latin-1", "ignore") + " " + raw.decode("utf-16-le", "ignore")
        text = text[:MAX_TEXT_CHARS]
        return (text, "") if text.strip() else ("", "no-text")
    except Exception:
        return "", "unreadable"


# --------------------------------------------------------------------------------------
# Looking at a document before deciding about it
#
# The filename and the folder it sits in are often enough, and when they aren't you
# want the thing itself: the first pages as text right there, or the document open in
# whatever normally opens it.
# --------------------------------------------------------------------------------------
TEXT_NOTES = {
    "no-text": "No text in this one - a scan of paper, most likely. Open it to see the pages.",
    "too-big": "Too big to read in here. Open it instead.",
    "unreadable": "This one wouldn't open for reading. It may be damaged.",
    "needs-pypdf": "Reading PDFs needs pypdf:  pip install pypdf",
    "gone": "The file isn't at that path any more.",
    "cloud": "Still in the cloud - OneDrive or Drive hasn't put this one on the PC yet, "
             "so there's nothing here to read. Open it and it comes down.",
}


def document_preview(path, limit=6000):
    """(text, note) - the start of the document, ready to put in front of you."""
    if not path or not os.path.isfile(_lp(path)):
        return "", TEXT_NOTES["gone"]
    ext = os.path.splitext(path)[1].lower()
    try:
        st = os.stat(_lp(path))
    except OSError as exc:
        return "", str(exc)
    if is_cloud_only(st):
        # Opening a placeholder either stalls on the download or fails outright, and
        # either way it isn't damaged - say so rather than crying wolf.
        return "", TEXT_NOTES["cloud"]
    text, note = extract_text(path, ext, st.st_size)
    text = re.sub(r"[ \t]+", " ", text or "")
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text).strip()
    return text[:limit], TEXT_NOTES.get(note, "")


def open_document(path):
    """Open it in whatever normally opens it. Returns '' or why it didn't."""
    if not path or not os.path.isfile(_lp(path)):
        return TEXT_NOTES["gone"]
    try:
        if IS_WIN:
            os.startfile(path)                                  # the reader you already use
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception as exc:
        return str(exc)
    return ""


def open_folder(path):
    """Show the document in its folder. Returns '' or why it didn't."""
    folder = path if os.path.isdir(path) else os.path.dirname(path)
    if not os.path.isdir(folder):
        return TEXT_NOTES["gone"]
    try:
        if IS_WIN:
            subprocess.Popen(["explorer", f"/select,{os.path.normpath(path)}"])
        elif sys.platform == "darwin":
            subprocess.Popen(["open", "-R", path])
        else:
            subprocess.Popen(["xdg-open", folder])
    except Exception as exc:
        return str(exc)
    return ""


# --------------------------------------------------------------------------------------
# Main work
# --------------------------------------------------------------------------------------
def index_filebird(folders, include_hidden, progress=None):
    by_size, by_name, count = defaultdict(list), defaultdict(list), 0
    per_folder = defaultdict(int)
    for folder in folders:
        for path, entry, st in walk(folder, [], include_hidden=True):
            rec = {"path": path, "size": st.st_size, "cloud": is_cloud_only(st), "name": norm_name(entry.name)}
            by_size[st.st_size].append(rec)
            by_name[rec["name"]].append(rec)
            per_folder[os.path.normcase(os.path.dirname(path))] += 1
            count += 1
            if progress:
                progress.tick(detail=os.path.dirname(path), indexed=count)
    if progress:
        progress.end_line()
    return by_size, by_name, count, per_folder


# --------------------------------------------------------------------------------------
# Which FileBird folder does a document belong in?
# --------------------------------------------------------------------------------------
_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
    "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
    "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
    "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming",
    "district of columbia", "washington dc",
}

# Folder names too generic to file a document on by themselves. A more specific
# folder still wins when one matches, so blocking these only sends the leftovers
# to the inbox instead of dumping them in, say, "Lawsuits".
GENERIC_FOLDERS = _STATES | {
    "media", "templates", "website", "history", "volunteer", "documents", "images",
    "uploads", "misc", "other", "archive", "archives", "drafts", "in progress", "reports",
    "photos", "videos", "audio", "scans", "new folder", "untitled folder", "transcripts",
    "lawsuits", "litigation", "other litigation", "legislation", "legislative",
    "investigations", "inspections", "handbooks", "confidential", "corporate",
    "criticism", "directories", "disability", "affiliates", "foia", "news",
    "news coverage", "news clippings", "news articles", "survivors", "staff",
    "marketing", "strategy", "research", "general", "files", "docs", "pdf", "pdfs",
    "canada", "ireland", "israel", "jamaica", "costa rica", "mexico", "samoa",
    "board of directors", "property records", "academic reports", "parent guides",
    # scraper output folders - the pipelines fill these, hand-found documents don't
    # belong in them (see backup_reports.py REPORT_CACHES)
    "checklists", "ut checklists", "az inspections", "az reports", "dra reports",
    "tx csv", "nc pdfs", "nc ocr", "ar pdfs", "fl pdfs", "or pdfs", "wa pdfs",
}


def _matcher_or_none(names):
    """A Matcher over these names, or None - an empty Matcher would match everything."""
    return Matcher([(n, 1) for n in names]) if names else None


class FolderIndex:
    """Every folder inside the FileBird library, matchable by name."""

    def __init__(self, roots, per_folder_counts):
        self.roots = [os.path.abspath(r) for r in roots]
        self.by_norm = defaultdict(list)
        self.all = []
        for root in self.roots:
            for dirpath, dirnames, _ in os.walk(root):
                dirnames[:] = [d for d in dirnames if not d.startswith((".", "$", "~"))]
                for name in dirnames:
                    abs_path = os.path.join(dirpath, name)
                    rec = {
                        "abs": abs_path,
                        "rel": os.path.relpath(abs_path, root).replace("\\", "/"),
                        "name": name,
                        "norm": _norm_term(name),
                        "files": per_folder_counts.get(os.path.normcase(abs_path), 0),
                    }
                    rec["depth"] = rec["rel"].count("/") + 1
                    self.all.append(rec)
                    self.by_norm[rec["norm"]].append(rec)
        # When two folders share a name, prefer the one that already holds files,
        # then the shallower one. (The library carries duplicate empty folders.)
        for recs in self.by_norm.values():
            recs.sort(key=lambda r: (-r["files"], r["depth"]))

        usable = [n for n in self.by_norm if n not in GENERIC_FOLDERS and not n.isdigit()]
        # UHS, YSI, FHW and friends are real folders, but three letters match by
        # accident far too easily inside a long document - names only for them.
        self.matcher = _matcher_or_none([n for n in usable if len(n) >= 4])
        self.short_matcher = _matcher_or_none([n for n in usable if len(n) == 3])

    def __len__(self):
        return len(self.all)

    def _hits(self, text, allow_short):
        if not text:
            return []
        hits = list(self.matcher.find(text)) if self.matcher else []
        if allow_short and self.short_matcher:
            hits += list(self.short_matcher.find(text))
        # Longest name first: "Discovery Schools of Virginia" beats "Discovery".
        return sorted(hits, key=len, reverse=True)

    def choose(self, filename, parents_text, content_text):
        """Return (rel_folder, matched_folder_name, confidence). confidence is
        'filename' | 'folder' | 'content' | '' (nothing matched)."""
        for text, confidence in ((filename, "filename"), (parents_text, "folder"), (content_text, "content")):
            hits = self._hits(text, allow_short=confidence != "content")
            if hits:
                rec = self.by_norm[hits[0]][0]
                return rec["rel"], rec["name"], confidence
        return "", "", ""


def unique_destination(folder, filename):
    """A path inside `folder` that isn't taken: 'Report.pdf' -> 'Report (2).pdf'."""
    stem, ext = os.path.splitext(filename)
    candidate = os.path.join(folder, filename)
    n = 2
    while os.path.exists(_lp(candidate)):
        candidate = os.path.join(folder, f"{stem} ({n}){ext}")
        n += 1
    return candidate


def upload(src, dest_dir, filename):
    """Copy src into dest_dir (creating it). Returns (dest_path, error)."""
    try:
        os.makedirs(_lp(dest_dir), exist_ok=True)
        dest = unique_destination(dest_dir, filename)
        shutil.copy2(_lp(src), _lp(dest))
        if os.path.getsize(_lp(dest)) != os.path.getsize(_lp(src)):
            return dest, "size mismatch after copy"
        return dest, ""
    except OSError as exc:
        return "", str(exc)


# --------------------------------------------------------------------------------------
# Scan history / decisions
#
# Every candidate a scan turns up is remembered here, so you can look at them in
# small batches, at your own pace, across as many sittings as you like. A document
# is keyed by its SHA-256, so moving or renaming it doesn't lose your decision.
# --------------------------------------------------------------------------------------
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".tti_doc_finder_state.json")

PENDING, APPROVED, REJECTED = "pending", "approved", "rejected"
UPLOADED, STAGED, FAILED = "uploaded", "staged", "failed"
DECISIONS = (PENDING, APPROVED, REJECTED, UPLOADED, STAGED, FAILED)

# What a 'decision' column can say, whether you typed it or the tool wrote it.
# None means "no answer given", which leaves an existing decision alone.
DECISION_WORDS = {"y": APPROVED, "yes": APPROVED, APPROVED: APPROVED,
                  "n": REJECTED, "no": REJECTED, REJECTED: REJECTED,
                  "s": PENDING, "skip": PENDING, PENDING: PENDING, "": None,
                  UPLOADED: UPLOADED, STAGED: STAGED, FAILED: FAILED}

# Names you never need to look at twice: anything matching one of these is approved
# the moment it turns up. Yours live in the history file - see the `auto-approve`
# command - and this is what a new history file starts with.
DEFAULT_AUTO_APPROVE = ["prea-audit"]

# The scan options worth remembering, so the next scan can simply repeat the last one.
SCAN_SETTINGS = ("min_score", "keywords", "keywords_from_folders", "no_content",
                 "read_drive_contents", "hash_cloud", "include_matched", "include_hidden",
                 "inbox_folder", "file_on_content", "upload_same_name")


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _flat(text):
    """Lowercased with -, _ and spaces flattened, so 'PREA_Audit' matches 'prea-audit'."""
    return re.sub(r"[-_\s]+", " ", (text or "").lower()).strip()


def doc_ext(name):
    """The extension the tool judges a file by - '.pdf', '.docx', '.doc' or something else."""
    return os.path.splitext(name or "")[1].lower()


def _num(value, default=0):
    try:
        return type(default)(float(value))
    except (TypeError, ValueError):
        return default


def find_recent_reports(limit=6):
    """Report CSVs from earlier runs, newest first, in the places the tool writes them."""
    home = os.path.expanduser("~")
    places = [os.path.dirname(os.path.abspath(__file__)), os.getcwd(), home,
              os.path.join(home, "Desktop"), os.path.join(home, "Downloads"),
              os.path.join(home, "OneDrive", "Desktop")]
    seen, found = set(), []
    for place in places:
        key = os.path.normcase(os.path.abspath(place))
        if key in seen or not os.path.isdir(place):
            continue
        seen.add(key)
        try:
            with os.scandir(place) as it:
                for e in it:
                    low = e.name.lower()
                    if not low.endswith(".csv"):
                        continue
                    if low.startswith(("tti_missing_from_filebird", "tti_to_review")):
                        found.append((e.stat().st_mtime, e.path))
        except OSError:
            continue
    found.sort(reverse=True)
    return [p for _, p in found[:limit]]


class Store:
    """The scan history and your decisions, kept in one JSON file."""

    def __init__(self, path=STATE_FILE):
        self.path = path
        self.just_approved = []              # what the auto-approve rules caught this time
        self._touched, self._dropped = set(), set()   # what this copy changed, for merging on save
        self._rules_changed = False
        self.data = {"version": 1, "library": "", "runs": [], "documents": {}}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict) and "documents" in loaded:
                    self.data = loaded
            except (OSError, ValueError) as exc:
                backup = path + ".broken"
                print(f"WARNING: {path} is unreadable ({exc}); starting fresh, old copy at {backup}",
                      file=sys.stderr)
                try:
                    os.replace(path, backup)
                except OSError:
                    pass

    def save(self):
        """Write the history back, keeping what another copy of the tool has decided.

        The review window and the command line are often open at once, so the file may
        have moved on since this copy read it. Only what this copy actually changed is
        written over the top of it - everyone else's answers stay where they are.
        """
        data = self._merged_with_disk()
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=1, ensure_ascii=False)
            os.replace(tmp, self.path)
        except OSError as exc:
            print(f"WARNING: could not save {self.path}: {exc}", file=sys.stderr)
            return
        self.data = data                      # now in step with everyone else
        self._touched, self._dropped, self._rules_changed = set(), set(), False

    def _merged_with_disk(self):
        try:
            with open(self.path, encoding="utf-8") as f:
                disk = json.load(f)
        except (OSError, ValueError):
            return self.data
        if not isinstance(disk, dict) or "documents" not in disk:
            return self.data

        docs = dict(disk.get("documents") or {})
        for k in self._dropped:
            docs.pop(k, None)
        for k in self._touched:               # what this copy decided, added or re-filed
            if k in self.docs:
                docs[k] = self.docs[k]
        runs, seen = [], set()
        for r in list(disk.get("runs") or []) + list(self.data.get("runs") or []):
            rid = r.get("id", "")
            if rid and rid in seen:
                continue
            seen.add(rid)
            runs.append(r)
        out = dict(disk)
        out["version"] = 1
        out["documents"] = docs
        out["runs"] = runs[-200:]
        out["library"] = self.data.get("library") or disk.get("library", "")
        if self._rules_changed or "auto_approve" not in out:
            out["auto_approve"] = self.data.get("auto_approve", list(DEFAULT_AUTO_APPROVE))
        return out

    def touch(self, key):
        """Say this document's record was changed here, so a save keeps the change."""
        self._touched.add(key)

    def drop(self, key):
        """Take a document out of the history for good."""
        self.docs.pop(key, None)
        self._dropped.add(key)
        self._touched.discard(key)

    # -- documents ---------------------------------------------------------------------
    @property
    def docs(self):
        return self.data["documents"]

    @staticmethod
    def key(digest, path, size):
        """SHA-256 when we have it, otherwise the path and size."""
        return digest or f"nohash:{os.path.normcase(os.path.abspath(path))}:{size}"

    def record(self, row, run_id):
        """Add or refresh a candidate. An existing decision is never overwritten."""
        k = Store.key(row["_digest"], row["path"], row["size_kb"])
        doc = self.docs.get(k)
        if doc is None and row["_digest"]:
            # Read back from a report, so it has no hash yet: same document, keep the
            # decision it already carries and let it live under its real key from now on.
            stale = Store.key("", row["path"], row["size_kb"])
            if stale in self.docs:
                doc = self.docs.pop(stale)
                self.docs[k] = doc
        if doc is None:
            doc = {"decision": PENDING, "first_seen": run_id, "decided_at": "", "uploaded_to": "", "note": ""}
            self.docs[k] = doc
        self._touched.add(k)
        doc.update({
            "last_seen": run_id,
            "path": row["path"],
            "filename": row["filename"],
            "size_kb": row["size_kb"],
            "modified": row["modified"],
            "score": row["score"],
            "status": row["status"],
            "matched_terms": row["matched_terms"],
            "folder": row["filebird_folder"],
            "folder_matched_in": row["folder_matched_in"],
            "verified": row["_verified"],
        })
        return k

    def decide(self, k, decision, folder=None, note=""):
        doc = self.docs.get(k)
        if doc is None:
            return False
        doc["decision"] = decision
        doc["decided_at"] = _now()
        self._touched.add(k)
        if folder:
            doc["folder"] = folder
        if note:
            doc["note"] = note
        return True

    def with_decision(self, *decisions):
        """[(key, doc)] in the order you'd want to look at them: best matches first."""
        out = [(k, d) for k, d in self.docs.items() if d.get("decision") in decisions]
        out.sort(key=lambda kd: (kd[1].get("folder_matched_in", "") == "",
                                 -kd[1].get("score", 0), kd[1].get("filename", "").lower()))
        return out

    # -- names you don't need to look at twice ------------------------------------------
    def auto_approve_rules(self):
        """The bits of filename that approve a document on sight."""
        rules = self.data.get("auto_approve")
        if rules is None:
            rules = list(DEFAULT_AUTO_APPROVE)
            self.data["auto_approve"] = rules
        return rules

    def apply_auto_approve(self):
        """Approve every waiting document whose name matches a rule. Returns [(rule, n)].

        Run whenever the history is loaded and at the end of every scan, so a matching
        document is approved the moment it turns up and never reaches the review list.
        """
        rules = [(r, _flat(r)) for r in self.auto_approve_rules() if _flat(r)]
        hits = defaultdict(int)
        for key, d in self.docs.items():
            if d.get("decision") != PENDING:
                continue
            name = _flat(d.get("filename") or os.path.basename(d.get("path", "")))
            for rule, flat in rules:
                if flat in name:
                    d["decision"] = APPROVED
                    d["decided_at"] = _now()
                    d["note"] = f"approved automatically (matched '{rule}')"
                    self._touched.add(key)
                    hits[rule] += 1
                    break
        self.just_approved = sorted(hits.items())
        if hits:
            self.save()
        return self.just_approved

    def counts(self):
        c = defaultdict(int)
        for d in self.docs.values():
            c[d.get("decision", PENDING)] += 1
        return c

    # -- reading an earlier run's results back in --------------------------------------
    def adopt_report(self, path):
        """Put the candidates in a report CSV back in the history. Returns (taken, skipped).

        Rows the scan offered carry a doc_id. A report from before doc_ids were
        written down is taken too: the rows a scan would have offered (MISSING, one
        copy each) come back keyed by path and size, the same way an unhashed file is
        keyed, so the next scan recognises them and keeps whatever you decided.
        Anything already in the history keeps the decision it has here.

        Only PDFs and Word documents come back. An older report will have .txt, .md,
        .html and the rest in it, from when this tool offered those too; they are
        counted and left where they are.
        """
        taken, skipped = 0, 0
        try:
            with open(path, encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames or "path" not in reader.fieldnames:
                    return 0, 0
                for row in reader:
                    k = (row.get("doc_id") or "").strip()
                    if not k:
                        if row.get("status") != "MISSING" or row.get("duplicate_of_local_file"):
                            continue
                        if not (row.get("path") or "").strip():
                            continue
                        k = Store.key("", row["path"], _num(row.get("size_kb"), 0.0))
                    if k in self.docs:
                        continue
                    if doc_ext(row.get("filename") or row.get("path", "")) not in DOC_EXTS:
                        skipped += 1
                        continue
                    decision = DECISION_WORDS.get((row.get("decision") or "").strip().lower(), PENDING)
                    self.docs[k] = {
                        "decision": decision or PENDING,
                        "first_seen": "from report", "last_seen": "from report",
                        "decided_at": "", "uploaded_to": row.get("uploaded_to", ""),
                        "note": f"read back from {os.path.basename(path)}",
                        "path": row.get("path", ""), "filename": row.get("filename", ""),
                        "size_kb": _num(row.get("size_kb"), 0.0), "modified": row.get("modified", ""),
                        "score": _num(row.get("score"), 0), "status": row.get("status", "MISSING"),
                        "matched_terms": row.get("matched_terms", ""),
                        "folder": row.get("filebird_folder") or DEFAULT_INBOX,
                        "folder_matched_in": row.get("folder_matched_in", ""),
                        "verified": True,
                    }
                    self._touched.add(k)
                    taken += 1
        except (OSError, ValueError, csv.Error):
            return taken, skipped
        return taken, skipped

    def autoload(self):
        """Empty history but a report from an earlier scan lying about? Read it back in.

        Losing .tti_doc_finder_state.json shouldn't mean scanning the whole machine
        again. Nothing is moved either way: the candidates come back exactly as a scan
        would have left them, waiting for you to look at them.
        """
        if self.docs:
            return None
        for report in find_recent_reports():
            taken, skipped = self.adopt_report(report)
            if taken:
                self.save()
                return report, taken, skipped
        return None

    # -- runs --------------------------------------------------------------------------
    def add_run(self, entry):
        self.data["runs"].append(entry)
        self.data["runs"] = self.data["runs"][-200:]      # keep the log from growing forever

    @property
    def runs(self):
        return self.data["runs"]

    def last_run(self):
        return self.data["runs"][-1] if self.data["runs"] else None

    def last_settings(self):
        """The folders and options the last scan used, so the next one can repeat it."""
        last = self.last_run()
        settings = dict((last or {}).get("settings") or {})
        if settings and not settings.get("filebird") and last.get("library"):
            settings["filebird"] = [last["library"]]
        return settings

    # -- the library the last scan used ------------------------------------------------
    def library(self, override=None):
        """The FileBird folder to file into: what you passed, else what the last scan used.

        Deliberately does NOT go looking for a library of its own. Uploading copies
        files into somebody's real document library, so the target has to be one you
        named or one a scan of yours established - never a lucky guess.
        """
        for candidate in (override, self.data.get("library")):
            if candidate and os.path.isdir(candidate):
                return candidate
        return ""

    def folder_choices(self, limit=6000):
        """Every folder in the library as it is actually spelled, to pick from."""
        lib = self.library()
        out = []
        if lib and os.path.isdir(lib):
            for dirpath, dirnames, _ in os.walk(lib):
                for d in dirnames:
                    out.append(os.path.relpath(os.path.join(dirpath, d), lib).replace("\\", "/"))
                if len(out) >= limit:
                    break
        return sorted(out, key=str.lower)

    def known_folders(self):
        """Every folder path in the library, lowercased, for catching typos."""
        lib = self.library()
        if not lib or not os.path.isdir(lib):
            return set()
        out = set()
        for dirpath, dirnames, _ in os.walk(lib):
            for d in dirnames:
                out.add(os.path.relpath(os.path.join(dirpath, d), lib).replace("\\", "/").lower())
        return out


def load_previous(announce=True):
    """The history from earlier sittings, recovered from a report CSV if it went missing."""
    store = Store()
    adopted = store.autoload()
    if adopted and announce:
        report, taken, skipped = adopted
        print(f"\nNo scan history was saved, so I read your last report back in:\n"
              f"  {report}\n  {taken:,} documents recovered - nothing was uploaded."
              + (f"\n  {skipped:,} rows left out: not PDFs or Word documents.\n" if skipped else "\n"),
              file=sys.stderr)
    for rule, n in store.apply_auto_approve():
        if announce:
            print(f"  {n:,} documents approved automatically: '{rule}' is in the name.", file=sys.stderr)
    return store, adopted


def non_documents(store):
    """[(key, doc)] for everything in the history that isn't a PDF or Word document.

    Anything already sent is left out: its record says where it went, and that
    stays true whatever the file was.
    """
    return [(k, d) for k, d in store.docs.items()
            if d.get("decision") not in (UPLOADED, STAGED)
            and doc_ext(d.get("filename") or d.get("path", "")) not in DOC_EXTS]


def prune_others(args):
    """Drop everything that isn't a PDF or Word document from the review list.

    An earlier version of this tool offered .txt, .md, .html, spreadsheets and the
    rest, so a report from back then drags them along. This clears them out in one
    go. Only the review list is touched - no file on disk is opened or moved.
    """
    store, _ = load_previous()
    doomed = non_documents(store)
    if not doomed:
        out = "\nNothing to get rid of: everything waiting is a PDF or Word document.\n"
        print(out, file=sys.stderr)
        return out

    by_ext, by_decision = defaultdict(int), defaultdict(int)
    for _, d in doomed:
        by_ext[doc_ext(d.get("filename") or d.get("path", "")) or "(no extension)"] += 1
        by_decision[d.get("decision", PENDING)] += 1
    lines = [f"\n{len(doomed):,} entries aren't PDFs or Word documents:"]
    lines += [f"  {ext:<16} {n:>7,}" for ext, n in sorted(by_ext.items(), key=lambda kv: -kv[1])]
    lines.append("  " + ", ".join(f"{n:,} {name}" for name, n in sorted(by_decision.items())))

    if args.dry_run:
        lines.append("\nNothing was changed. Run it again without --dry-run to drop them.\n")
        out = "\n".join(lines)
        print(out, file=sys.stderr)
        return out

    for k, _ in doomed:
        store.drop(k)
    store.save()
    c = store.counts()
    lines += ["", f"Dropped. {c[PENDING]:,} documents are still waiting for review"
                  f", {c[APPROVED]:,} approved.", ""]
    out = "\n".join(lines)
    print(out, file=sys.stderr)
    return out


def auto_approve_cmd(args):
    """Show, add to or drop from the list of names that get approved on sight."""
    store, _ = load_previous(announce=False)
    rules = store.auto_approve_rules()
    text = (args.text or "").strip()
    lines = []

    if text and args.remove:
        keep = [r for r in rules if _flat(r) != _flat(text)]
        if len(keep) == len(rules):
            lines.append(f"'{text}' isn't one of your rules.")
        else:
            store.data["auto_approve"] = keep
            store._rules_changed = True
            rules = keep
            lines.append(f"Dropped '{text}'. Documents already approved by it stay approved -\n"
                         "turn them down in the review list if you want them back out.")
        store.save()
    elif text:
        if any(_flat(r) == _flat(text) for r in rules):
            lines.append(f"'{text}' is already one of your rules.")
        else:
            rules.append(text)
            store._rules_changed = True
            store.save()
            lines.append(f"Added '{text}'.")
        hits = store.apply_auto_approve()
        lines += [f"  {n:,} documents approved: '{rule}' is in the name." for rule, n in hits] or \
                 ["  Nothing waiting matches it yet."]

    waiting = store.with_decision(PENDING)
    lines.append("\nApproved on sight, wherever the name contains:")
    lines += [f"  {r}" for r in rules] or ["  (nothing - every document waits for you)"]
    lines.append(f"\n{len(waiting):,} documents are still waiting for review.")
    lines.append("Add another with:  py tti_doc_finder.py auto-approve \"prea-audit\"\n")
    out = "\n".join(lines)
    print(out, file=sys.stderr)
    return out


def resume_lines(store):
    """What an earlier sitting left behind, in the order you'd want to hear about it."""
    c, lines = store.counts(), []
    last = store.last_run()
    if last:
        lines.append(f"Last scan {last.get('when', '?')}: {last.get('documents_seen', 0):,} documents looked at, "
                     f"{last.get('new_candidates', 0):,} new candidates.")
    if c[PENDING]:
        lines.append(f"{c[PENDING]:,} documents are waiting for you to review.")
    others = len(non_documents(store))
    if others:
        lines.append(f"{others:,} of them aren't PDFs or Word documents (from an older scan).")
    if c[APPROVED]:
        lines.append(f"{c[APPROVED]:,} are approved and waiting to go up.")
    if c[FAILED]:
        lines.append(f"{c[FAILED]:,} failed to upload last time.")
    if c[UPLOADED] or c[STAGED]:
        lines.append(f"{c[UPLOADED]:,} already copied to Drive"
                     + (f", {c[STAGED]:,} staged for the server" if c[STAGED] else "") + ".")
    return lines


def reuse_last_settings(args, store, announce=True):
    """Fill in from the last scan whatever this command line didn't say.

    Only blanks are filled in - anything you pass on the command line wins, and
    `--fresh` skips this altogether. Folders that have gone (an unplugged drive,
    a Drive letter that moved) are dropped rather than failing the scan.
    """
    settings = store.last_settings()
    if not settings:
        return {}
    used, dropped = [], []

    def folders(key):
        keep = []
        for p in settings.get(key) or []:
            (keep if os.path.isdir(p) else dropped).append(p)
        return keep

    if not getattr(args, "filebird", None):
        args.filebird = folders("filebird")
        used += args.filebird
    if not getattr(args, "scan", None):
        args.scan = folders("scan")
        used += args.scan
    if not getattr(args, "names_only_under", None):
        args.names_only_under = list(settings.get("names_only_under") or [])
    # options: take the remembered value wherever the command line left the default
    if getattr(args, "min_score", 2) == 2 and settings.get("min_score"):
        args.min_score = settings["min_score"]
    if not getattr(args, "keywords", None) and settings.get("keywords"):
        if os.path.isfile(settings["keywords"]):
            args.keywords = settings["keywords"]
    if getattr(args, "inbox_folder", DEFAULT_INBOX) == DEFAULT_INBOX and settings.get("inbox_folder"):
        args.inbox_folder = settings["inbox_folder"]
    for flag in ("keywords_from_folders", "no_content", "read_drive_contents", "hash_cloud",
                 "include_matched", "include_hidden", "file_on_content", "upload_same_name"):
        if settings.get(flag) and not getattr(args, flag, False):
            setattr(args, flag, True)
    if announce and (used or dropped):
        print("Repeating your last scan's setup:", file=sys.stderr)
        for p in used:
            print(f"  {p}", file=sys.stderr)
        for p in dropped:
            print(f"  (gone, skipping: {p})", file=sys.stderr)
        print("  Pass --scan / --filebird to change it, or --fresh to start over.\n", file=sys.stderr)
    return settings


def classify(path, size, cloud, name, by_size, by_name, hash_cloud):
    """Return (status, near_match_path, method)."""
    same_size = by_size.get(size, []) if size > 0 else []
    for ref in same_size:                       # same name + same byte size: accept without reading
        if ref["name"] == name:
            return "IN_FILEBIRD", ref["path"], "same name + size"
    unverified = None
    for ref in same_size:                       # same size, different name: compare content
        if (cloud or ref["cloud"]) and not hash_cloud:
            unverified = ref
            continue
        h1, h2 = sha256(path), sha256(ref["path"])
        if h1 and h1 == h2:
            return "IN_FILEBIRD", ref["path"], "identical content"
    if unverified:
        return "POSSIBLE_MATCH_SAME_SIZE", unverified["path"], "same size, not hashed"
    if name in by_name:
        return "SAME_NAME_DIFFERENT_FILE", by_name[name][0]["path"], "name only"
    return "MISSING", "", ""


def run(args, progress=None):
    t0 = time.time()
    progress = progress or Progress()
    filebird = [os.path.abspath(os.path.expanduser(p)) for p in args.filebird]
    for p in filebird:
        if not os.path.isdir(p):
            sys.exit(f"FileBird folder not found: {p}")
    names_only_under = [os.path.abspath(os.path.expanduser(p)) for p in args.names_only_under]
    if args.scan:
        scan_roots = [os.path.abspath(os.path.expanduser(p)) for p in args.scan]
        auto_names_only = [d["root"] for d in windows_drives() if d["gdrive"]]
    else:
        scan_roots, auto_names_only = default_scan_plan()
    if not args.read_drive_contents:
        names_only_under += auto_names_only
    print("Will scan:", file=sys.stderr)
    for r in scan_roots:
        tag = "  (names only)" if any(_is_under(r, p) for p in names_only_under) else ""
        print(f"  {r}{tag}", file=sys.stderr)

    # keywords
    weighted = [(t, w) for w, ts in TERMS.items() for t in ts]
    if args.keywords:
        with open(args.keywords, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                term, _, w = line.rpartition(",") if re.search(r",\s*[123]\s*$", line) else (line, "", "3")
                weighted.append((term.strip(), int(w)))

    progress.stage("Indexing the FileBird library...")
    by_size, by_name, n_ref, per_folder = index_filebird(filebird, args.include_hidden, progress)
    folders = FolderIndex(filebird, per_folder)
    progress.note(f"  {n_ref:,} files in {len(folders):,} folders.")

    if args.keywords_from_folders:
        extra = [(f["norm"], 3) for f in folders.all
                 if len(f["norm"]) >= 6 and f["norm"] not in GENERIC_FOLDERS and not f["norm"].isdigit()]
        print(f"Added {len(set(extra))} keywords from FileBird folder names.", file=sys.stderr)
        weighted += extra
    matcher = Matcher(weighted)

    if PdfReader is None and not args.no_content:
        print("NOTE: pypdf isn't installed, so PDFs are judged by filename/folder only. "
              "Run `pip install pypdf` and rerun for a much better result.", file=sys.stderr)

    out_path = os.path.abspath(args.out)
    this_script = os.path.abspath(__file__)
    rows, scanned, docs, relevant = [], 0, 0, 0

    for n_root, root in enumerate(scan_roots, 1):
        if not os.path.isdir(root):
            progress.note(f"  (skipping, not a folder: {root})")
            continue
        progress.stage(f"Scanning {root}   ({n_root} of {len(scan_roots)})")
        for path, entry, st in walk(root, filebird, args.include_hidden):
            scanned += 1
            progress.tick(detail=path, files=scanned, documents=docs, TTI_related=relevant)
            ext = os.path.splitext(entry.name)[1].lower()
            if ext not in DOC_EXTS:
                continue
            if path in (out_path, this_script) or entry.name.startswith("~$"):
                continue
            docs += 1

            cloud = is_cloud_only(st)
            no_read = cloud or any(_is_under(path, p) for p in names_only_under)
            # --- relevance ---------------------------------------------------------
            score, terms, where, text = 0, {}, [], ""
            h = matcher.find(entry.name)
            if h:
                score += 2 * sum(h.values()); terms.update(h); where.append("filename")
            parents = " / ".join(Path(path).parent.parts[-3:])
            h = {k: v for k, v in matcher.find(parents).items() if k not in terms}
            if h:
                score += sum(h.values()); terms.update(h); where.append("folder")
            note = ""
            if args.no_content:
                note = "content not read (--no-content)"
            elif no_read:
                note = "cloud / Drive letter, content not read"
            else:
                text, note = extract_text(path, ext, st.st_size)
                h = {k: v for k, v in matcher.find(text).items() if k not in terms}
                if h:
                    score += sum(h.values()); terms.update(h); where.append("content")
            if score < args.min_score:
                continue
            relevant += 1

            # --- already in FileBird? ----------------------------------------------
            status, near, method = classify(path, st.st_size, no_read, norm_name(entry.name),
                                            by_size, by_name, args.hash_cloud)
            if status == "IN_FILEBIRD" and not args.include_matched:
                continue
            # Which of several identical copies to keep is decided once they are all
            # in and can be ranked against each other (see below).
            digest = sha256(path) if status != "IN_FILEBIRD" and not no_read else ""

            # --- which FileBird folder does it belong in? --------------------------
            rel, folder_match, confidence = folders.choose(entry.name, parents, text)
            if confidence == "content" and not args.file_on_content:
                rel = ""            # keep the suggestion in the CSV, but don't file on it
            rows.append({
                "status": status,
                "score": score,
                "matched_terms": "; ".join(sorted(terms, key=lambda k: -terms[k])),
                "matched_in": "+".join(where),
                "path": path,
                "folder": os.path.dirname(path),
                "filename": entry.name,
                "ext": ext,
                "size_kb": round(st.st_size / 1024, 1),
                "modified": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d"),
                "content_note": note,
                "filebird_near_match": near,
                "match_method": method,
                "duplicate_of_local_file": "",
                "_digest": digest,
                "filebird_folder": rel or args.inbox_folder,
                "folder_matched_name": folder_match,
                "folder_matched_in": confidence,
                "uploaded_to": "",
                "upload_note": "",
                "_verified": (not no_read) or args.hash_cloud,
            })

    progress.tick(force=True, files=scanned, documents=docs, TTI_related=relevant)
    progress.stage(f"Sorting {len(rows):,} candidates and writing the report...")

    order = {"MISSING": 0, "SAME_NAME_DIFFERENT_FILE": 1, "POSSIBLE_MATCH_SAME_SIZE": 2, "IN_FILEBIRD": 3}
    inboxed = lambda r: r["filebird_folder"] == args.inbox_folder
    rank = lambda r: (order[r["status"]], inboxed(r), -r["score"], r["path"].lower())

    # Several identical copies of one document: upload the one that files itself
    # best (a real folder beats the inbox), and mark the rest as its duplicates.
    rows.sort(key=rank)
    keeper = {}
    for r in rows:
        digest = r["_digest"]
        if digest:
            r["duplicate_of_local_file"] = keeper.get(digest, "")
            keeper.setdefault(digest, r["path"])

    rows.sort(key=lambda r: (order[r["status"]], bool(r["duplicate_of_local_file"])) + rank(r)[1:])

    # --- remember what we found, without touching the library -------------------------
    sendable = {"MISSING"} | ({"SAME_NAME_DIFFERENT_FILE"} if args.upload_same_name else set())
    library = filebird[0]
    store = Store()
    store.data["library"] = library
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_here, already_decided, filed_to_inbox = 0, 0, 0
    for r in rows:
        if r["status"] not in sendable:
            r["upload_note"] = "not missing"
            continue
        if r["duplicate_of_local_file"]:
            r["upload_note"] = "duplicate of another copy found in this scan"
            continue
        if not r["_verified"]:
            r["upload_note"] = "not verified against FileBird (rerun with --hash-cloud)"
            continue
        k = store.record(r, run_id)
        r["doc_id"] = k
        r["decision"] = store.docs[k]["decision"]
        was_known = store.docs[k]["first_seen"] != run_id      # incl. ones read back from a report
        if r["decision"] != PENDING:
            already_decided += 1
            r["upload_note"] = f"already {r['decision']}"
            continue
        r["upload_note"] = "waiting for review"
        if not was_known:
            new_here += 1
            if r["filebird_folder"] == args.inbox_folder:
                filed_to_inbox += 1

    counts = defaultdict(int)
    for r in rows:
        counts[r["status"]] += 1
    unique_missing = sum(1 for r in rows if r["status"] == "MISSING" and not r["duplicate_of_local_file"])
    out_path = write_report(rows, out_path)
    store.add_run({
        "id": run_id,
        "when": _now(),
        "seconds": round(time.time() - t0),
        "library": library,
        "scan_roots": scan_roots,
        "files_seen": scanned,
        "documents_seen": docs,
        "relevant": relevant,
        "missing": counts["MISSING"],
        "new_candidates": new_here,
        "report": out_path,
        # everything the next scan needs to repeat this one without asking again
        "settings": dict({k: getattr(args, k, None) for k in SCAN_SETTINGS},
                         filebird=filebird, scan=scan_roots,
                         names_only_under=list(args.names_only_under)),
    })
    auto = store.apply_auto_approve()          # names you've said you never need to see
    store.save()

    waiting = len(store.with_decision(PENDING))
    summary = (
        f"\nScan done in {time.time() - t0:,.0f}s.\n"
        f"  Files looked at:          {scanned:,}  ({docs:,} PDF/Word)\n"
        f"  TTI-related:              {relevant:,}\n"
        f"  MISSING from FileBird:    {counts['MISSING']:,}  ({unique_missing:,} unique)\n"
        f"  Same name, different file:{counts['SAME_NAME_DIFFERENT_FILE']:>6,}\n"
        f"  Possible match (unhashed):{counts['POSSIBLE_MATCH_SAME_SIZE']:>6,}\n"
        f"  New since your last scan: {new_here:,}"
        + (f"  ({already_decided:,} already decided)" if already_decided else "") + "\n"
        f"    matched to a folder:    {new_here - filed_to_inbox:,}\n"
        f"    into '{args.inbox_folder}': {filed_to_inbox:,}\n"
        + "".join(f"  Approved on sight ('{rule}'): {n:,}\n" for rule, n in auto)
        + f"  Waiting for review:       {waiting:,}\n"
        f"  Library: {library}\n"
        f"  Report:  {out_path}\n"
        f"\nNothing was uploaded. Next, go through them:\n"
        f"    py tti_doc_finder.py review --window\n"
    )
    print(summary, file=sys.stderr)
    return summary


REPORT_FIELDS = ["decision", "status", "score", "matched_terms", "matched_in", "path", "folder", "filename",
                 "ext", "size_kb", "modified", "content_note", "filebird_near_match", "match_method",
                 "duplicate_of_local_file", "filebird_folder", "folder_matched_name", "folder_matched_in",
                 "uploaded_to", "upload_note", "doc_id"]


def write_report(rows, out_path):
    """Write the CSV, falling back to the home folder when the chosen path is unwritable."""
    for target in (out_path, os.path.join(os.path.expanduser("~"), os.path.basename(out_path))):
        try:
            with open(target, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=REPORT_FIELDS, extrasaction="ignore")
                w.writeheader()
                w.writerows(rows)
            return target
        except OSError as exc:
            print(f"WARNING: could not write {target}: {exc}", file=sys.stderr)
    return out_path


# --------------------------------------------------------------------------------------
# Reviewing
# --------------------------------------------------------------------------------------
def describe(doc, n=None, total=None):
    where = doc.get("folder_matched_in") or "no match"
    head = f"[{n}/{total}] " if n else ""
    return (f"{head}{doc.get('filename', '?')}\n"
            f"      from: {doc.get('path', '?')}\n"
            f"      into: {doc.get('folder', '?')}   ({where}, score {doc.get('score', 0)})\n"
            f"      {doc.get('size_kb', 0):,.1f} KB, modified {doc.get('modified', '?')}"
            + (f", {doc['status']}" if doc.get("status") != "MISSING" else "")
            + (f"\n      terms: {doc['matched_terms'][:90]}" if doc.get("matched_terms") else ""))


def review(args):
    """Walk the pending candidates in batches and record a decision for each."""
    store, _ = load_previous()
    pending = store.with_decision(PENDING)
    if not pending:
        print("Nothing is waiting for review. Run a scan first:  py tti_doc_finder.py scan", file=sys.stderr)
        return "Nothing to review."
    if not sys.stdin or not sys.stdin.isatty():
        sys.exit("Reviewing needs a terminal. Open a command prompt and run:  py tti_doc_finder.py review\n"
                 "Or mark the CSV's 'decision' column and use:  py tti_doc_finder.py approve-from <file.csv>")

    known = store.known_folders()
    print(f"\n{len(pending):,} documents waiting, {args.batch} at a time.\n"
          "  o = open it in your reader    t = show the first pages here\n"
          "  y = yes, add it     n = no, never       s = skip for now\n"
          "  f = change folder   d = show it in its folder\n"
          "  Y = yes to the rest of this batch\n"
          "  q = stop here (everything decided so far is saved)\n", file=sys.stderr)

    approved_now, decided = 0, 0
    stopped = False
    for start in range(0, len(pending), args.batch):
        batch = pending[start:start + args.batch]
        print(f"\n----- batch {start // args.batch + 1} of "
              f"{(len(pending) + args.batch - 1) // args.batch} -----", file=sys.stderr)
        yes_to_rest = False
        for i, (k, doc) in enumerate(batch, 1):
            if yes_to_rest:
                store.decide(k, APPROVED); approved_now += 1; decided += 1
                continue
            print("\n" + describe(doc, start + i, len(pending)), file=sys.stderr)
            while True:
                try:
                    answer = input("  o/t/y/n/s/f/Y/q > ").strip()
                except (EOFError, KeyboardInterrupt):
                    answer = "q"
                if answer in ("o", "d"):
                    err = (open_document if answer == "o" else open_folder)(doc.get("path", ""))
                    print(f"  {err}" if err else "  opening...", file=sys.stderr)
                    continue
                if answer == "t":
                    text, note = document_preview(doc.get("path", ""), limit=2500)
                    print(("\n" + text if text else "") + (f"\n  ({note})" if note else "") + "\n",
                          file=sys.stderr)
                    continue
                if answer == "f":
                    new_folder = input("  FileBird folder (e.g. UHS/Provo Canyon School) > ").strip().strip("/\\")
                    if not new_folder:
                        continue
                    new_folder = new_folder.replace("\\", "/")
                    if known and new_folder.lower() not in known:
                        near = [f for f in sorted(known) if new_folder.lower() in f][:5]
                        print(f"  '{new_folder}' isn't a folder in the library yet - it will be created.",
                              file=sys.stderr)
                        if near:
                            print("  Did you mean: " + ", ".join(near), file=sys.stderr)
                        if input("  Use it anyway? [y/N] > ").strip().lower() != "y":
                            continue
                    doc["folder"] = new_folder
                    print(f"  -> will go into {new_folder}", file=sys.stderr)
                    continue
                if answer in ("y", "n", "s", "Y", "q", ""):
                    break
                print("  Please answer o, t, y, n, s, f, d, Y or q.", file=sys.stderr)
            if answer == "q":
                stopped = True
                break
            if answer in ("y", "Y"):
                store.decide(k, APPROVED); approved_now += 1; decided += 1
                if answer == "Y":
                    yes_to_rest = True
            elif answer == "n":
                store.decide(k, REJECTED); decided += 1
            # "s" or Enter: leave it pending
        store.save()
        if stopped:
            break
        if start + args.batch < len(pending):
            try:
                if input("\n  Carry on to the next batch? [Y/n] > ").strip().lower() == "n":
                    break
            except (EOFError, KeyboardInterrupt):
                break

    store.save()
    c = store.counts()
    summary = (f"\nReviewed {decided:,} documents ({approved_now:,} approved).\n"
               f"  Approved and waiting to upload: {c[APPROVED]:,}\n"
               f"  Still to review:                {c[PENDING]:,}\n"
               f"  Turned down:                    {c[REJECTED]:,}\n"
               + (f"\nNext:  py tti_doc_finder.py upload\n" if c[APPROVED] else ""))
    print(summary, file=sys.stderr)
    return summary


def review_in_window(args):
    """`review --window`: the same reviewer the pickers use, straight from the command line."""
    try:
        import tkinter as tk
        from tkinter import messagebox, ttk
    except Exception:
        sys.exit("A window needs tkinter, which isn't available here. Run `review` on its own instead.")
    store, _ = load_previous()
    if not store.with_decision(PENDING):
        print("Nothing is waiting for review. Run a scan first:  py tti_doc_finder.py scan", file=sys.stderr)
        return "Nothing to review."
    root = tk.Tk()
    root.withdraw()
    summary = gui_review(tk, ttk, root, args, store)
    if store.counts()[APPROVED] and messagebox.askyesno("TTI Doc Finder", "Upload the approved ones now?"):
        summary += upload_approved(argparse.Namespace(
            filebird=[], batch=args.batch, all=False, retry_failed=False))
        messagebox.showinfo("TTI Doc Finder", summary)
    root.destroy()
    return summary


def approve_from_csv(args):
    """Take decisions from the 'decision' column of a report you edited in a spreadsheet."""
    store = Store()
    if not store.docs:
        # The history is gone but the CSV in front of us is a scan's results: take them back.
        taken, skipped = store.adopt_report(args.csv)
        if taken:
            print(f"\nNo scan history was saved, so the {taken:,} documents in this CSV "
                  "were read back in first."
                  + (f"\n{skipped:,} of its rows aren't PDFs or Word documents and were left out.\n"
                     if skipped else "\n"), file=sys.stderr)
    allowed = DECISION_WORDS
    changed, unknown, bad = 0, 0, 0
    with open(args.csv, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            k = (row.get("doc_id") or "").strip()
            if not k:
                continue
            if k not in store.docs:
                unknown += 1
                continue
            want = allowed.get((row.get("decision") or "").strip().lower(), "?")
            if want == "?":
                bad += 1
                continue
            if want is None:
                continue
            folder = (row.get("filebird_folder") or "").strip()
            if store.decide(k, want, folder=folder or None):
                changed += 1
    store.save()
    c = store.counts()
    summary = (f"\nRead {args.csv}\n"
               f"  Decisions applied:              {changed:,}\n"
               + (f"  Rows not in the history:        {unknown:,}\n" if unknown else "")
               + (f"  Rows with an unclear decision:  {bad:,}\n" if bad else "")
               + f"  Approved and waiting to upload: {c[APPROVED]:,}\n")
    print(summary, file=sys.stderr)
    return summary


# --------------------------------------------------------------------------------------
# Uploading the approved documents, a batch at a time
# --------------------------------------------------------------------------------------
def upload_approved(args):
    store, _ = load_previous()
    queue = store.with_decision(APPROVED)
    if args.retry_failed:
        queue += store.with_decision(FAILED)
    if not queue:
        print("Nothing is approved yet. Review some first:  py tti_doc_finder.py review", file=sys.stderr)
        return "Nothing approved to upload."

    library = store.library(args.filebird[0] if args.filebird else None)
    if not library:
        sys.exit("I don't know which FileBird library to upload into.\n"
                 "Run a scan first, or name it explicitly:\n"
                 f'    py tti_doc_finder.py upload --filebird "{DEFAULT_LIBRARY_HINT}"')

    limit = len(queue) if args.all else min(args.batch, len(queue))
    interactive = bool(sys.stdin) and sys.stdin.isatty() and getattr(args, "ask", True)
    print(f"\n{len(queue):,} approved, uploading {limit:,} into {library}\n", file=sys.stderr)

    done, failed, gone = 0, 0, 0
    for i, (k, doc) in enumerate(queue[:limit], 1):
        src = doc.get("path", "")
        if not os.path.isfile(_lp(src)):
            store.decide(k, FAILED, note="the file is no longer at that path")
            print(f"  [{i}/{limit}] MISSING ON DISK  {doc.get('filename')}", file=sys.stderr)
            gone += 1
            continue
        dest_dir = os.path.join(library, *doc.get("folder", "").split("/"))
        dest, err = upload(src, dest_dir, doc.get("filename", os.path.basename(src)))
        if err:
            store.decide(k, FAILED, note=err)
            failed += 1
            print(f"  [{i}/{limit}] FAILED  {doc.get('filename')}: {err}", file=sys.stderr)
        else:
            store.decide(k, UPLOADED)
            store.docs[k]["uploaded_to"] = dest
            done += 1
            print(f"  [{i}/{limit}] {doc.get('filename')}  ->  {doc.get('folder')}", file=sys.stderr)
        if i % 10 == 0:
            store.save()
    store.save()

    c = store.counts()
    summary = (f"\nUploaded {done:,} documents into {library}.\n"
               + (f"  Failed:                {failed:,}\n" if failed else "")
               + (f"  No longer on disk:     {gone:,}\n" if gone else "")
               + f"  Still approved to go:  {c[APPROVED]:,}\n"
               + f"  Still to review:       {c[PENDING]:,}\n"
               + f"  Uploaded all together: {c[UPLOADED]:,}\n"
               + ("\nGoogle Drive will sync them; FileBird files them on the site.\n" if done else ""))
    if c[APPROVED] and not args.all:
        summary += "Run the same command again for the next batch.\n"
    print(summary, file=sys.stderr)
    if c[APPROVED] and not args.all and interactive:
        try:
            if input("Upload the next batch now? [y/N] > ").strip().lower() == "y":
                return summary + upload_approved(args)
        except (EOFError, KeyboardInterrupt):
            pass
    return summary


def stage_approved(args):
    """Build a folder + manifest.json for api/import-documents.php on the server.

    The Drive folder route relies on the FileBird/Drive sync noticing the new file.
    This one doesn't: you scp the folder up and the importer puts each document in
    the media library and files it, skipping anything whose md5 is already there.
    """
    store, _ = load_previous()
    queue = store.with_decision(APPROVED)
    if not queue:
        print("Nothing is approved yet. Review some first:  py tti_doc_finder.py review", file=sys.stderr)
        return "Nothing approved to stage."
    limit = len(queue) if args.all else min(args.batch, len(queue))

    out_dir = os.path.abspath(os.path.expanduser(args.out))
    try:
        os.makedirs(_lp(out_dir), exist_ok=True)
    except OSError as exc:
        sys.exit(f"Could not make the staging folder {out_dir}: {exc}")

    manifest, deep, gone, taken = [], [], 0, set()
    for k, doc in queue[:limit]:
        src = doc.get("path", "")
        if not os.path.isfile(_lp(src)):
            store.decide(k, FAILED, note="the file is no longer at that path")
            gone += 1
            continue
        parts = [p for p in doc.get("folder", "").split("/") if p]
        if not parts:
            continue
        # import-documents.php understands one level of parent, and the parent has
        # to exist at the top of the tree already.
        folder, parent = parts[-1], (parts[-2] if len(parts) == 2 else "")
        if len(parts) > 2:
            deep.append((doc.get("filename", ""), doc.get("folder", "")))

        name = os.path.basename(src)
        stem, ext = os.path.splitext(name)
        n = 2
        while name.lower() in taken:              # one flat folder, so names must be unique
            name = f"{stem} ({n}){ext}"
            n += 1
        taken.add(name.lower())
        try:
            shutil.copy2(_lp(src), _lp(os.path.join(out_dir, name)))
        except OSError as exc:
            store.decide(k, FAILED, note=str(exc))
            print(f"  FAILED to stage {name}: {exc}", file=sys.stderr)
            continue
        manifest.append({"file": name, "title": stem, "folder": folder, "parent": parent})
        store.decide(k, STAGED, note=out_dir)

    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=1, ensure_ascii=False)
    store.save()

    c = store.counts()
    summary = (f"\nStaged {len(manifest):,} documents in {out_dir}\n"
               + (f"  No longer on disk:     {gone:,}\n" if gone else "")
               + f"  Still approved to go:  {c[APPROVED]:,}\n"
               "\nSend them up and import (dry run first, then 'apply'):\n"
               f"    scp -r \"{out_dir}\" kop_nixihost:~/staged-docs\n"
               "    ssh kop_nixihost\n"
               "    /opt/cpanel/ea-php82/root/usr/bin/php "
               "public_html/wp-content/themes/child/api/import-documents.php ~/staged-docs\n"
               "    ...same again with 'apply' on the end once the dry run looks right.\n")
    if deep:
        summary += (f"\n{len(deep)} documents sit more than two folders deep, and the importer only\n"
                    "understands one parent. Their 'parent' was left blank - fix them in\n"
                    "manifest.json before importing, or they'll land at the top level:\n"
                    + "".join(f"    {n}  ({f})\n" for n, f in deep[:10]))
    print(summary, file=sys.stderr)
    return summary


def show_status(args):
    store, _ = load_previous()
    c = store.counts()
    lines = ["", "Scan history:  " + STATE_FILE, ""]
    lines.append(f"  Waiting for review:    {c[PENDING]:,}")
    lines.append(f"  Approved, not yet up:  {c[APPROVED]:,}")
    lines.append(f"  Copied to Drive:       {c[UPLOADED]:,}")
    if c[STAGED]:
        lines.append(f"  Staged for the server: {c[STAGED]:,}")
    lines.append(f"  Turned down:           {c[REJECTED]:,}")
    others = len(non_documents(store))
    if others:
        lines.append(f"  Not PDF/Word at all:   {others:,}   (clear them out: prune)")
    if c[FAILED]:
        lines.append(f"  Failed to upload:      {c[FAILED]:,}   (retry: upload --retry-failed)")
    if store.runs:
        last = store.runs[-1]
        lines += ["", f"  Last scan: {last['when']}  -  {last.get('documents_seen', 0):,} documents looked at, "
                      f"{last.get('new_candidates', 0):,} new"]
        for root in (last.get("settings") or {}).get("scan", [])[:6]:
            lines.append(f"             scanned {root}")
        if last.get("report"):
            lines.append(f"             report  {last['report']}")
    out = "\n".join(lines) + "\n"
    print(out, file=sys.stderr)
    return out


def show_history(args):
    store = Store()
    if not store.runs:
        print("No scans recorded yet.", file=sys.stderr)
        return "No scans recorded yet."
    lines = ["", f"{'when':17} {'secs':>5} {'docs':>8} {'relevant':>9} {'missing':>8} {'new':>6}  report"]
    for r in store.runs[-args.limit:]:
        lines.append(f"{r.get('when', ''):17} {r.get('seconds', 0):>5,} {r.get('documents_seen', 0):>8,} "
                     f"{r.get('relevant', 0):>9,} {r.get('missing', 0):>8,} {r.get('new_candidates', 0):>6,}  "
                     f"{os.path.basename(r.get('report', ''))}")
        for root in r.get("scan_roots", [])[:4]:
            lines.append(f"{'':17} scanned {root}")
    out = "\n".join(lines) + "\n"
    print(out, file=sys.stderr)
    return out


def export_pending(args):
    """Write the documents in one state back out to a CSV you can mark up."""
    store, _ = load_previous()
    wanted = DECISIONS if args.decision == "all" else (args.decision,)
    rows = [dict(d, doc_id=k, decision=d.get("decision", PENDING),
                 filebird_folder=d.get("folder", ""), path=d.get("path", ""))
            for k, d in store.with_decision(*wanted)]
    out = write_report(rows, os.path.abspath(args.out))
    print(f"\n{len(rows):,} documents written to {out}\n"
          "Mark the 'decision' column y or n (change 'filebird_folder' if you like), save, then:\n"
          f"    py tti_doc_finder.py approve-from \"{out}\"\n", file=sys.stderr)
    return out




# --------------------------------------------------------------------------------------
# The window
#
# Everything the commands do, in one place: scan for documents, work through what's
# waiting, send the approved ones, and keep the rules that decide things for you.
# Opened by double-clicking the script, or with `py tti_doc_finder.py ui`.
# --------------------------------------------------------------------------------------
GREY, LINK = "#666666", "#0B5FA5"


class ReviewPanel:
    """The waiting documents as a list, worked through as many at a time as you like.

    Filter by name, select a row or five hundred (Ctrl-click, Shift-click, or "Select
    all shown"), and approve or turn down the lot in one go. One row selected on its
    own shows its first pages, and the document itself is a click away, so nothing has
    to be approved unseen. Every answer is saved as it's given.
    """

    def __init__(self, tk, ttk, parent, store, on_change=None):
        self.tk, self.ttk, self.store, self.on_change = tk, ttk, store, on_change
        self.rows = {}                         # tree row id -> (key, doc)
        self.pending = []
        self.tally = {"approved": 0, "rejected": 0}
        self.current = ""                      # the path the Open buttons act on
        self.read_job = None

        top = tk.Frame(parent)
        top.pack(fill="x", padx=14, pady=(12, 6))
        tk.Label(top, text="Show only names containing:").pack(side="left")
        self.filter_var = tk.StringVar()
        tk.Entry(top, textvariable=self.filter_var, width=32).pack(side="left", padx=(6, 10))
        tk.Button(top, text="Select all shown", command=self.select_all).pack(side="left")
        tk.Button(top, text="Refresh", command=self.reload).pack(side="left", padx=6)
        self.tally_label = tk.Label(top, text="", fg=GREY)
        self.tally_label.pack(side="left", padx=12)

        body = tk.Frame(parent)
        body.pack(fill="both", expand=True, padx=14)
        left = tk.Frame(body)
        left.pack(side="left", fill="both", expand=True)
        right = tk.Frame(body, width=430)
        right.pack(side="right", fill="both", padx=(14, 0))
        right.pack_propagate(False)

        self.tree = ttk.Treeview(left, columns=("name", "folder", "kb", "score", "terms"),
                                 show="headings", selectmode="extended")
        for col, title, width, anchor in (("name", "document", 340, "w"), ("folder", "goes into", 180, "w"),
                                          ("kb", "KB", 70, "e"), ("score", "score", 55, "e"),
                                          ("terms", "matched on", 190, "w")):
            self.tree.heading(col, text=title)
            self.tree.column(col, width=width, anchor=anchor)
        ybar = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=ybar.set)
        ybar.pack(side="right", fill="y")
        self.tree.pack(side="left", fill="both", expand=True)

        self.head = tk.Label(right, anchor="w", justify="left", wraplength=410,
                             font=("Segoe UI", 10, "bold"))
        self.head.pack(fill="x")
        self.meta = tk.Label(right, anchor="w", justify="left", wraplength=410, fg="#444444")
        self.meta.pack(fill="x", pady=(2, 6))
        openrow = tk.Frame(right)
        openrow.pack(fill="x")
        self.status = tk.Label(right, anchor="w", fg=GREY, wraplength=410)
        tk.Button(openrow, text="Open the document", command=self.open_it).pack(side="left")
        tk.Button(openrow, text="Show it in its folder", command=self.show_it).pack(side="left", padx=(8, 0))
        self.status.pack(fill="x", pady=(4, 6))
        pbar = tk.Scrollbar(right)
        pbar.pack(side="right", fill="y")
        self.preview = tk.Text(right, wrap="word", takefocus=0, yscrollcommand=pbar.set,
                               bg="#FFFFFF", fg="#222222", relief="solid", borderwidth=1)
        self.preview.pack(fill="both", expand=True)
        pbar.config(command=self.preview.yview)

        frow = tk.Frame(parent)
        frow.pack(fill="x", padx=14, pady=(10, 2))
        tk.Label(frow, text="File them into:").pack(side="left")
        self.folder_var = tk.StringVar()
        self.folder_box = ttk.Combobox(frow, textvariable=self.folder_var, width=52)
        self.folder_box.pack(side="left", padx=8)
        tk.Button(frow, text="Set for selected", command=self.set_folder).pack(side="left")

        brow = tk.Frame(parent)
        brow.pack(fill="x", padx=14, pady=(8, 14))
        tk.Button(brow, text="Approve selected", width=22,
                  command=lambda: self.decide(APPROVED)).pack(side="left")
        tk.Button(brow, text="Turn down selected", width=22,
                  command=lambda: self.decide(REJECTED)).pack(side="left", padx=8)

        self.tree.bind("<<TreeviewSelect>>", self.show_selected)
        parent.bind_all("<Control-a>", lambda e: self.select_all())
        self.filter_var.trace_add("write", lambda *a: self.fill())
        self.reload()

    # -- the list ----------------------------------------------------------------------
    def reload(self, store=None):
        """Read the waiting documents again, after a scan or someone else's decisions."""
        if store is not None:
            self.store = store
        self.pending = self.store.with_decision(PENDING)
        self.folder_box.configure(values=self.store.folder_choices())
        self.fill()

    def fill(self):
        want = _flat(self.filter_var.get())
        self.tree.delete(*self.tree.get_children())
        self.rows.clear()
        for k, d in self.pending:
            if d.get("decision") != PENDING:
                continue                       # answered earlier in this sitting
            if want and want not in _flat(f"{d.get('filename', '')} {d.get('path', '')} "
                                          f"{d.get('matched_terms', '')}"):
                continue
            iid = self.tree.insert("", "end", values=(
                d.get("filename", ""), d.get("folder", ""), f"{d.get('size_kb', 0):,.0f}",
                d.get("score", 0), (d.get("matched_terms", "") or "")[:70]))
            self.rows[iid] = (k, d)
        self.retally()
        self.show_selected()

    def retally(self):
        waiting = sum(1 for _, d in self.pending if d.get("decision") == PENDING)
        picked = len(self.tree.selection())
        self.tally_label.config(
            text=f"{len(self.tree.get_children()):,} shown of {waiting:,} waiting"
                 + (f"   -   {picked:,} selected" if picked else "")
                 + f"   -   {self.tally['approved']:,} approved, "
                   f"{self.tally['rejected']:,} turned down here")

    def select_all(self):
        self.tree.selection_set(self.tree.get_children())
        self.tree.focus_set()

    # -- answering ---------------------------------------------------------------------
    def decide(self, decision):
        picked = self.tree.selection()
        if not picked:
            self.status.config(text="Nothing is selected.")
            return
        for iid in picked:
            k, _ = self.rows.get(iid, (None, None))
            if k and self.store.decide(k, decision):
                self.tally["approved" if decision == APPROVED else "rejected"] += 1
            self.rows.pop(iid, None)
            self.tree.delete(iid)
        self.store.save()
        self.retally()
        self.show_selected()
        if self.on_change:
            self.on_change()

    def set_folder(self):
        folder = self.folder_var.get().strip().strip("/\\").replace("\\", "/")
        picked = self.tree.selection()
        if not folder or not picked:
            self.status.config(text="Pick some rows and a folder first.")
            return
        for iid in picked:
            k, d = self.rows.get(iid, (None, None))
            if not k:
                continue
            d["folder"] = folder
            self.store.touch(k)
            self.tree.set(iid, "folder", folder)
        self.store.save()
        self.status.config(text=f"{len(picked):,} filed into {folder}")

    # -- the one under the cursor ------------------------------------------------------
    def open_it(self):
        self.status.config(text=open_document(self.current) or "opening...")

    def show_it(self):
        self.status.config(text=open_folder(self.current) or "")

    def show_selected(self, *_):
        picked = self.tree.selection()
        self.retally()
        if self.read_job:
            self.tree.after_cancel(self.read_job)
            self.read_job = None
        if len(picked) != 1:
            self.current = ""
            self.head.config(text=f"{len(picked):,} selected" if picked else "Nothing selected")
            self.meta.config(text="Select one row on its own to read it." if picked else "")
            self.preview.delete("1.0", "end")
            return
        k, d = self.rows[picked[0]]
        self.current = d.get("path", "")
        self.head.config(text=d.get("filename", "?"))
        self.meta.config(text=f"{d.get('path', '?')}\n{d.get('size_kb', 0):,.1f} KB, "
                              f"modified {d.get('modified', '?')}   -   score {d.get('score', 0)}, "
                              f"matched on {d.get('folder_matched_in') or 'nothing in particular'}")
        self.folder_var.set(d.get("folder", "") or DEFAULT_INBOX)
        self.status.config(text="")
        self.preview.delete("1.0", "end")
        self.preview.insert("1.0", "Reading the document...")
        # arrowing down a long list shouldn't open every file on the way past
        self.read_job = self.tree.after(250, lambda p=self.current: self.read_into_preview(p))

    def read_into_preview(self, path):
        self.read_job = None
        if path != self.current:
            return
        text, note = document_preview(path)
        self.preview.delete("1.0", "end")
        self.preview.insert("1.0", text or "")
        if note:
            self.preview.insert("end" if text else "1.0", ("\n\n" if text else "") + f"[{note}]")

    def summary(self):
        c = self.store.counts()
        return (f"\nDecided {self.tally['approved'] + self.tally['rejected']:,} documents "
                f"({self.tally['approved']:,} approved, {self.tally['rejected']:,} turned down).\n"
                f"  Approved and waiting to upload: {c[APPROVED]:,}\n"
                f"  Still to review:                {c[PENDING]:,}\n")


def gui_review(tk, ttk, root, args, store=None):
    """`review --window`: the review list on its own, for when that's all you want."""
    store = store or Store()
    if not store.with_decision(PENDING):
        return "Nothing waiting for review."
    win = tk.Toplevel(root)
    win.title("TTI Doc Finder - review")
    win.geometry("1240x780")
    panel = ReviewPanel(tk, ttk, win, store)
    tk.Button(win, text="Done", width=12, command=win.destroy).pack(pady=(0, 12))
    win.update_idletasks()
    try:
        win.grab_set()
    except Exception:
        pass
    win.focus_force()
    root.wait_window(win)
    store.save()
    summary = panel.summary()
    print(summary, file=sys.stderr)
    return summary


class QueuedProgress:
    """Stands in for the progress window while a scan runs on its own thread.

    Nothing here touches a widget: the words go on a queue and the window picks them
    up in its own time, because tkinter belongs to the thread that made it.
    """

    def __init__(self, q):
        self.q = q

    def stage(self, name):
        self.q.put(("stage", name, ""))

    def tick(self, counts, detail=""):
        self.q.put(("tick", counts, detail))

    def close(self):
        pass


def app(args=None):
    """One window for the whole job."""
    try:
        import queue
        import threading
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
    except Exception:
        sys.exit("The window needs tkinter, which isn't available here.\n"
                 "Everything it does has a command too - run with --help to see them.")

    store, adopted = load_previous()
    root = tk.Tk()
    root.title("TTI Doc Finder")
    root.geometry("1300x860")
    live = {"scanning": False, "queue": None, "store": store}

    # --- the numbers along the top ----------------------------------------------------
    header = tk.Frame(root)
    header.pack(fill="x", padx=16, pady=(14, 6))
    tk.Label(header, text="Documents about the TTI that aren't in FileBird yet",
             font=("Segoe UI", 13, "bold"), anchor="w").pack(fill="x")
    counts_label = tk.Label(header, anchor="w", fg="#222222", font=("Segoe UI", 10))
    counts_label.pack(fill="x", pady=(4, 0))
    library_label = tk.Label(header, anchor="w", fg=GREY)
    library_label.pack(fill="x")

    tabs = ttk.Notebook(root)
    tabs.pack(fill="both", expand=True, padx=12, pady=6)
    review_tab, scan_tab, send_tab, rules_tab = (tk.Frame(tabs) for _ in range(4))
    for frame, title in ((review_tab, "Review"), (scan_tab, "Scan"),
                         (send_tab, "Send"), (rules_tab, "Rules")):
        tabs.add(frame, text=f"  {title}  ")

    footer = tk.Label(root, anchor="w", fg=GREY)
    footer.pack(fill="x", padx=16, pady=(0, 10))

    def say(text):
        footer.config(text=text)

    def refresh_counts():
        c = live["store"].counts()
        others = len(non_documents(live["store"]))
        counts_label.config(
            text=f"{c[PENDING]:,} waiting for review     {c[APPROVED]:,} approved, not yet sent     "
                 f"{c[UPLOADED]:,} copied to Drive     {c[STAGED]:,} staged     "
                 f"{c[REJECTED]:,} turned down"
                 + (f"     {others:,} not PDF/Word" if others else ""))
        library_label.config(text="Library: " + (live["store"].library() or
                                                 "not known yet - a scan settles it"))
        approved_label.config(text=f"{c[APPROVED]:,} documents are approved and waiting to go.")
        prune_button.config(text=f"Get rid of the {others:,} that aren't PDFs or Word documents"
                                 if others else "Nothing here but PDFs and Word documents")
        prune_button.config(state="normal" if others else "disabled")

    # ==================================================================================
    # Review
    # ==================================================================================
    panel = ReviewPanel(tk, ttk, review_tab, store, on_change=lambda: refresh_counts())

    # ==================================================================================
    # Scan
    # ==================================================================================
    settings = store.last_settings()
    scan_body = tk.Frame(scan_tab)
    scan_body.pack(fill="both", expand=True, padx=14, pady=12)

    def folder_list(parent, title, hint):
        tk.Label(parent, text=title, font=("Segoe UI", 10, "bold"), anchor="w").pack(fill="x")
        tk.Label(parent, text=hint, fg=GREY, anchor="w", justify="left", wraplength=520).pack(fill="x")
        row = tk.Frame(parent)
        row.pack(fill="x", pady=(4, 10))
        box = tk.Listbox(row, height=5)
        box.pack(side="left", fill="both", expand=True)
        buttons = tk.Frame(row)
        buttons.pack(side="left", padx=8)
        return box, buttons

    left_col = tk.Frame(scan_body)
    left_col.pack(side="left", fill="both", expand=True)
    right_col = tk.Frame(scan_body, width=420)
    right_col.pack(side="right", fill="both", padx=(16, 0))
    right_col.pack_propagate(False)

    lib_box, lib_buttons = folder_list(
        left_col, "Your FileBird library",
        "The folder on your local Google Drive that mirrors the site's media library.")
    scan_box, scan_buttons = folder_list(
        left_col, "Where to look",
        "Whole drives are fine. The Google Drive letter is read by name only, so a "
        "streamed Drive isn't forced to download everything.")

    def add_to(box, title):
        d = filedialog.askdirectory(title=title)
        if d:
            box.insert("end", os.path.normpath(d))

    def drop_from(box):
        for i in reversed(box.curselection()):
            box.delete(i)

    def fill_box(box, paths):
        box.delete(0, "end")
        for p in paths:
            box.insert("end", p)

    tk.Button(lib_buttons, text="Add...", width=10,
              command=lambda: add_to(lib_box, "Pick your FileBird folder")).pack()
    tk.Button(lib_buttons, text="Remove", width=10, command=lambda: drop_from(lib_box)).pack(pady=4)
    tk.Button(lib_buttons, text="Find it", width=10,
              command=lambda: fill_box(lib_box, find_filebird_folders())).pack()
    tk.Button(scan_buttons, text="Add...", width=10,
              command=lambda: add_to(scan_box, "Pick a folder or drive to scan")).pack()
    tk.Button(scan_buttons, text="Remove", width=10, command=lambda: drop_from(scan_box)).pack(pady=4)
    tk.Button(scan_buttons, text="Recommended", width=10,
              command=lambda: fill_box(scan_box, default_scan_plan()[0])).pack()

    fill_box(lib_box, settings.get("filebird") or find_filebird_folders())
    fill_box(scan_box, settings.get("scan") or default_scan_plan()[0])

    opts = tk.LabelFrame(right_col, text=" Options ", padx=10, pady=8)
    opts.pack(fill="x")
    score_row = tk.Frame(opts)
    score_row.pack(fill="x", pady=(0, 6))
    tk.Label(score_row, text="Relevance needed:").pack(side="left")
    score_var = tk.IntVar(value=settings.get("min_score") or 2)
    tk.Spinbox(score_row, from_=1, to=20, width=4, textvariable=score_var).pack(side="left", padx=6)
    tk.Label(score_row, text="(higher = fewer, surer)", fg=GREY).pack(side="left")
    flags = {}
    for key, label in (("no_content", "Filenames and folders only - much faster"),
                       ("keywords_from_folders", "Use your FileBird folder names as keywords too"),
                       ("include_hidden", "Look in hidden folders"),
                       ("read_drive_contents", "Open files on the Google Drive letter (downloads them)"),
                       ("hash_cloud", "Hash cloud-only files to settle 'possible match' (downloads them)"),
                       ("upload_same_name", "Also offer other versions of documents FileBird has")):
        flags[key] = tk.BooleanVar(value=bool(settings.get(key)))
        tk.Checkbutton(opts, text=label, variable=flags[key], anchor="w",
                       wraplength=380, justify="left").pack(fill="x")
    inbox_row = tk.Frame(opts)
    inbox_row.pack(fill="x", pady=(6, 0))
    tk.Label(inbox_row, text="No folder matches ->").pack(side="left")
    inbox_var = tk.StringVar(value=settings.get("inbox_folder") or DEFAULT_INBOX)
    tk.Entry(inbox_row, textvariable=inbox_var, width=22).pack(side="left", padx=6)

    go_row = tk.Frame(right_col)
    go_row.pack(fill="x", pady=10)
    scan_button = tk.Button(go_row, text="Start the scan", width=18)
    scan_button.pack(side="left")
    tk.Label(go_row, text="Nothing is uploaded by a scan.", fg=GREY).pack(side="left", padx=8)

    bar = ttk.Progressbar(right_col, mode="indeterminate")
    bar.pack(fill="x")
    stage_label = tk.Label(right_col, anchor="w", justify="left", wraplength=400)
    stage_label.pack(fill="x", pady=(6, 0))
    tick_label = tk.Label(right_col, anchor="w", fg=GREY, justify="left", wraplength=400)
    tick_label.pack(fill="x")
    log = tk.Text(right_col, height=12, wrap="word", bg="#FFFFFF", relief="solid", borderwidth=1)
    log.pack(fill="both", expand=True, pady=(8, 0))

    def log_write(text):
        log.insert("end", text.rstrip() + "\n")
        log.see("end")

    def start_scan():
        libs = list(lib_box.get(0, "end"))
        roots = list(scan_box.get(0, "end"))
        if not libs:
            messagebox.showwarning("TTI Doc Finder", "Pick your FileBird library folder first.")
            return
        if not roots:
            messagebox.showwarning("TTI Doc Finder", "Pick at least one folder to look in.")
            return
        scan_args = argparse.Namespace(
            filebird=libs, scan=roots, fresh=True, min_score=score_var.get(),
            keywords=settings.get("keywords"), inbox_folder=inbox_var.get().strip() or DEFAULT_INBOX,
            names_only_under=list(settings.get("names_only_under") or []),
            include_matched=False, file_on_content=False,
            out=os.path.join(desktop_or_home(),
                             f"tti_missing_from_filebird_{datetime.now():%Y%m%d_%H%M}.csv"),
            **{k: v.get() for k, v in flags.items()})
        q = queue.Queue()
        live.update(scanning=True, queue=q)
        scan_button.config(state="disabled", text="Scanning...")
        bar.start(60)
        log_write(f"--- scan started {_now()} ---")

        def work():
            try:
                q.put(("done", run(scan_args, progress=Progress(window=QueuedProgress(q))), ""))
            except BaseException as exc:                    # sys.exit() included
                q.put(("failed", f"{exc}" or exc.__class__.__name__, ""))

        threading.Thread(target=work, daemon=True).start()
        root.after(120, drain)

    def drain():
        q = live["queue"]
        if q is None:
            return
        while True:
            try:
                kind, a, b = q.get_nowait()
            except queue.Empty:
                break
            if kind == "stage":
                stage_label.config(text=a)
                log_write(a)
            elif kind == "tick":
                tick_label.config(text=f"{a}\n{_shorten(b, 70)}")
            else:
                live.update(scanning=False, queue=None)
                bar.stop()
                scan_button.config(state="normal", text="Start the scan")
                stage_label.config(text="Finished." if kind == "done" else "The scan stopped.")
                tick_label.config(text="")
                log_write(a)
                live["store"] = Store()
                panel.reload(live["store"])
                refresh_counts()
                if kind == "done":
                    say("Scan finished - the Review tab has what it found.")
                    tabs.select(review_tab)
                else:
                    messagebox.showerror("TTI Doc Finder", a)
                return
        if live["scanning"]:
            root.after(120, drain)

    scan_button.config(command=start_scan)

    # ==================================================================================
    # Send
    # ==================================================================================
    send_body = tk.Frame(send_tab)
    send_body.pack(fill="both", expand=True, padx=16, pady=14)
    approved_label = tk.Label(send_body, anchor="w", font=("Segoe UI", 11))
    approved_label.pack(fill="x")
    tk.Label(send_body, fg=GREY, anchor="w", justify="left", wraplength=900, text=
             "Copying into the Drive folder is the quick way: Drive syncs it and FileBird files it, "
             "as long as that sync is behaving.\nStaging builds a folder with a manifest.json for "
             "api/import-documents.php instead, which doesn't rely on the sync at all.").pack(
        fill="x", pady=(4, 12))

    batch_row = tk.Frame(send_body)
    batch_row.pack(fill="x", pady=(0, 10))
    tk.Label(batch_row, text="How many at a time:").pack(side="left")
    batch_var = tk.IntVar(value=25)
    tk.Spinbox(batch_row, from_=1, to=5000, width=6, textvariable=batch_var).pack(side="left", padx=6)
    all_var = tk.BooleanVar(value=False)
    tk.Checkbutton(batch_row, text="all of them", variable=all_var).pack(side="left")
    retry_var = tk.BooleanVar(value=False)
    tk.Checkbutton(batch_row, text="retry the ones that failed", variable=retry_var).pack(side="left", padx=10)

    send_log = tk.Text(send_body, height=18, wrap="word", bg="#FFFFFF", relief="solid", borderwidth=1)

    def run_and_log(fn, *, kind):
        try:
            summary = fn()
        except BaseException as exc:
            summary = f"{kind} stopped: {exc}"
        send_log.insert("end", summary.rstrip() + "\n\n")
        send_log.see("end")
        live["store"] = Store()
        panel.reload(live["store"])
        refresh_counts()
        say(summary.strip().splitlines()[0] if summary.strip() else "")

    def do_upload():
        run_and_log(lambda: upload_approved(argparse.Namespace(
            filebird=list(lib_box.get(0, "end")), batch=batch_var.get(), all=all_var.get(),
            retry_failed=retry_var.get(), ask=False)), kind="Upload")

    def do_stage():
        out = filedialog.askdirectory(title="Where should the staging folder go?")
        if not out:
            return
        out = os.path.join(out, f"tti-staged-{datetime.now():%Y%m%d_%H%M}")
        run_and_log(lambda: stage_approved(argparse.Namespace(
            out=out, batch=batch_var.get(), all=all_var.get())), kind="Staging")

    def do_export():
        out = os.path.join(desktop_or_home(), f"tti_to_review_{datetime.now():%Y%m%d_%H%M}.csv")
        path = export_pending(argparse.Namespace(decision=PENDING, out=out))
        send_log.insert("end", f"Wrote {path}\nMark the 'decision' column y or n, save, then use "
                               f"approve-from on it.\n\n")
        send_log.see("end")
        say(f"Wrote {path}")

    send_buttons = tk.Frame(send_body)
    send_buttons.pack(fill="x", pady=(0, 10))
    tk.Button(send_buttons, text="Copy them into the Drive folder", width=32,
              command=do_upload).pack(side="left")
    tk.Button(send_buttons, text="Stage them for the server", width=28,
              command=do_stage).pack(side="left", padx=10)
    tk.Button(send_buttons, text="Write a spreadsheet instead", width=26,
              command=do_export).pack(side="left")
    send_log.pack(fill="both", expand=True)

    # ==================================================================================
    # Rules
    # ==================================================================================
    rules_body = tk.Frame(rules_tab)
    rules_body.pack(fill="both", expand=True, padx=16, pady=14)
    tk.Label(rules_body, text="Approved on sight", font=("Segoe UI", 11, "bold"), anchor="w").pack(fill="x")
    tk.Label(rules_body, fg=GREY, anchor="w", justify="left", wraplength=900, text=
             "A document whose name contains one of these is approved the moment it turns up and "
             "never reaches the review list.\n-, _ and spaces are all the same, so prea-audit, "
             "PREA_Audit and \"PREA Audit\" all count.").pack(fill="x", pady=(4, 8))

    rule_row = tk.Frame(rules_body)
    rule_row.pack(fill="x")
    rules_box = tk.Listbox(rule_row, height=6, width=50)
    rules_box.pack(side="left")
    rule_buttons = tk.Frame(rule_row)
    rule_buttons.pack(side="left", padx=10)
    new_rule = tk.StringVar()
    tk.Entry(rule_buttons, textvariable=new_rule, width=28).pack()

    def refresh_rules():
        rules_box.delete(0, "end")
        for r in live["store"].auto_approve_rules():
            rules_box.insert("end", r)

    def add_rule():
        text = new_rule.get().strip()
        if not text:
            return
        store_now = live["store"]
        if any(_flat(r) == _flat(text) for r in store_now.auto_approve_rules()):
            say(f"'{text}' is already a rule.")
            return
        store_now.auto_approve_rules().append(text)
        store_now._rules_changed = True
        store_now.save()
        hits = store_now.apply_auto_approve()
        n = sum(c for _, c in hits)
        new_rule.set("")
        refresh_rules()
        panel.reload(store_now)
        refresh_counts()
        say(f"Added '{text}' - {n:,} waiting documents approved by it.")

    def drop_rule():
        picked = rules_box.curselection()
        if not picked:
            return
        text = rules_box.get(picked[0])
        store_now = live["store"]
        store_now.data["auto_approve"] = [r for r in store_now.auto_approve_rules()
                                          if _flat(r) != _flat(text)]
        store_now._rules_changed = True
        store_now.save()
        refresh_rules()
        say(f"Dropped '{text}'. What it already approved stays approved.")

    tk.Button(rule_buttons, text="Add this rule", width=24, command=add_rule).pack(pady=(6, 0))
    tk.Button(rule_buttons, text="Drop the selected rule", width=24, command=drop_rule).pack(pady=6)

    tk.Label(rules_body, text="Tidying up", font=("Segoe UI", 11, "bold"), anchor="w").pack(
        fill="x", pady=(18, 0))
    tk.Label(rules_body, fg=GREY, anchor="w", justify="left", wraplength=900, text=
             "An older scan offered .txt, .md, .html and spreadsheets as well, and a report from "
             "back then brings them along. This drops them from the review list - no file on disk "
             "is touched.").pack(fill="x", pady=(4, 8))
    prune_button = tk.Button(rules_body, width=52)
    prune_button.pack(anchor="w")

    def do_prune():
        summary = prune_others(argparse.Namespace(dry_run=False))
        live["store"] = Store()
        panel.reload(live["store"])
        refresh_counts()
        refresh_rules()
        messagebox.showinfo("TTI Doc Finder", summary)

    prune_button.config(command=do_prune)

    tk.Label(rules_body, text="Past scans", font=("Segoe UI", 11, "bold"), anchor="w").pack(
        fill="x", pady=(18, 4))
    history = tk.Text(rules_body, height=10, wrap="none", bg="#FFFFFF", relief="solid", borderwidth=1)
    history.pack(fill="both", expand=True)
    history.insert("1.0", show_history(argparse.Namespace(limit=12)))
    history.config(state="disabled")

    # --- and away we go ---------------------------------------------------------------
    refresh_rules()
    refresh_counts()
    if adopted:
        report, taken, skipped = adopted
        say(f"Read {taken:,} documents back from {os.path.basename(report)}"
            + (f" ({skipped:,} rows weren't documents)" if skipped else ""))
    else:
        say("Ready." if store.docs else "Nothing found yet - the Scan tab starts one.")
    for rule, n in store.just_approved:
        log_write(f"{n:,} documents approved on sight ('{rule}').")
    tabs.select(review_tab if store.with_decision(PENDING) else scan_tab)
    root.mainloop()
    return ""

# --------------------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Find PDFs and Word documents about the TTI that aren't in your FileBird library yet, "
                    "look them over in batches, and file the ones you approve.",
        epilog="Run with no arguments at all for the point-and-click version.")
    sub = ap.add_subparsers(dest="command")

    def add_library(p):
        p.add_argument("--filebird", action="append", default=[], metavar="DIR",
                       help="the FileBird folder on your local Google Drive (repeatable). "
                            "Found automatically when you leave it out.")

    sc = sub.add_parser("scan", help="look for documents and add them to the review list (uploads nothing)")
    add_library(sc)
    sc.add_argument("--scan", action="append", default=[], metavar="DIR",
                    help="folder/drive to scan (repeatable). Default: your home folder and the other drives")
    sc.add_argument("--out", default=f"tti_missing_from_filebird_{datetime.now():%Y%m%d_%H%M}.csv",
                    help="where to write the report CSV")
    sc.add_argument("--fresh", action="store_true",
                    help="don't reuse the folders and options from your last scan")
    sc.add_argument("--min-score", type=int, default=2, help="relevance threshold (default 2; raise to cut noise)")
    sc.add_argument("--keywords", metavar="FILE", help="extra keywords, one per line, optional ',1|2|3' weight")
    sc.add_argument("--keywords-from-folders", action="store_true",
                    help="also use FileBird sub-folder names (program names) as keywords")
    sc.add_argument("--no-content", action="store_true", help="filenames/folders only - much faster")
    sc.add_argument("--names-only-under", action="append", default=[], metavar="DIR",
                    help="don't open files under this path (e.g. a streamed Google Drive mount)")
    sc.add_argument("--read-drive-contents", action="store_true",
                    help="Windows: also open files on the Google Drive letter (downloads them if streamed)")
    sc.add_argument("--hash-cloud", action="store_true",
                    help="allow hashing cloud-only files (forces them to download)")
    sc.add_argument("--include-matched", action="store_true", help="also list files that ARE in FileBird")
    sc.add_argument("--include-hidden", action="store_true", help="also scan hidden (dot) folders")
    sc.add_argument("--inbox-folder", default=DEFAULT_INBOX, metavar="NAME",
                    help=f"FileBird folder for documents with no confident match (default: '{DEFAULT_INBOX}')")
    sc.add_argument("--file-on-content", action="store_true",
                    help="also file a document when the folder name only appears in its text, not its name/path")
    sc.add_argument("--upload-same-name", action="store_true",
                    help="also offer SAME_NAME_DIFFERENT_FILE documents (other versions of something "
                         "FileBird already has)")

    rv = sub.add_parser("review", help="go through the waiting documents, reading each one first")
    rv.add_argument("--batch", type=int, default=25, help="how many to show before pausing (default 25)")
    rv.add_argument("--window", action="store_true",
                    help="review in a window, with the start of each document in front of you")

    af = sub.add_parser("approve-from", help="take decisions from a report CSV you marked up in a spreadsheet")
    af.add_argument("csv", help="the CSV, with y or n in its 'decision' column")

    ex = sub.add_parser("export", help="write the waiting documents to a CSV you can mark up")
    ex.add_argument("--decision", default=PENDING, choices=list(DECISIONS) + ["all"],
                    help="which documents to write out (default: pending)")
    ex.add_argument("--out", default=f"tti_to_review_{datetime.now():%Y%m%d_%H%M}.csv")

    up = sub.add_parser("upload", help="copy the approved documents into the Drive FileBird folder, "
                                       "a batch at a time (needs the Drive/FileBird sync to be working)")
    add_library(up)
    up.add_argument("--batch", type=int, default=25, help="how many to upload per run (default 25)")
    up.add_argument("--all", action="store_true", help="upload every approved document, no batching")
    up.add_argument("--retry-failed", action="store_true", help="also retry the ones that failed before")

    stg = sub.add_parser("stage", help="put the approved documents and a manifest.json in a folder for "
                                       "api/import-documents.php (doesn't rely on the Drive sync)")
    stg.add_argument("--out", default=os.path.join(os.path.expanduser("~"),
                                                   f"tti-staged-{datetime.now():%Y%m%d_%H%M}"),
                     help="the staging folder to build (default: one in your home folder)")
    stg.add_argument("--batch", type=int, default=25, help="how many to stage per run (default 25)")
    stg.add_argument("--all", action="store_true", help="stage every approved document, no batching")

    aa = sub.add_parser("auto-approve", help="names that get approved on sight, with no review "
                                            f"(you start with: {', '.join(DEFAULT_AUTO_APPROVE)})")
    aa.add_argument("text", nargs="?", help="a bit of filename to add, e.g. \"prea-audit\"")
    aa.add_argument("--remove", action="store_true", help="drop that rule instead of adding it")

    pr = sub.add_parser("prune", help="drop everything that isn't a PDF or Word document from "
                                      "the review list (an older scan offered other file types)")
    pr.add_argument("--dry-run", action="store_true", help="say what would go, change nothing")

    sub.add_parser("ui", help="open the window: scan, review, send, rules, all in one place "
                              "(this is what double-clicking the script does)")

    sub.add_parser("status", help="how many are waiting, approved, uploaded")

    hi = sub.add_parser("history", help="past scans")
    hi.add_argument("--limit", type=int, default=20, help="how many runs to show (default 20)")

    args = ap.parse_args()

    if args.command is None or args.command == "ui":
        app()
        return

    if args.command == "scan":
        # Whatever the last sitting left behind is loaded before anything else, so a
        # scan adds to it instead of starting from nothing.
        store, _ = load_previous()
        for line in resume_lines(store):
            print("  " + line, file=sys.stderr)
        if not args.fresh:
            reuse_last_settings(args, store)
        if not args.filebird:
            found = find_filebird_folders()
            if not found:
                sys.exit("Could not find your FileBird folder on Google Drive.\n"
                         "Pass it with --filebird " + repr(DEFAULT_LIBRARY_HINT))
            args.filebird = found
            print(f"Using FileBird library: {found[0]}", file=sys.stderr)
        run(args)
    elif args.command == "review":
        review_in_window(args) if args.window else review(args)
    elif args.command == "approve-from":
        approve_from_csv(args)
    elif args.command == "export":
        export_pending(args)
    elif args.command == "upload":
        upload_approved(args)
    elif args.command == "stage":
        stage_approved(args)
    elif args.command == "auto-approve":
        auto_approve_cmd(args)
    elif args.command == "prune":
        prune_others(args)
    elif args.command == "status":
        show_status(args)
    elif args.command == "history":
        show_history(args)


if __name__ == "__main__":
    main()
