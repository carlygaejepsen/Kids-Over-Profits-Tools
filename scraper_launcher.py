"""
KOP Scraper Launcher
====================
Desktop UI for running state inspection scrapers monthly.

Usage:
    python scraper_launcher.py
    (or double-click scraper_launcher.bat for no-console launch)

Styled with the Kids Over Profits campaign palette (see AGENTS.md):
navy/midnight/teal on sand, with orange/chartreuse/coral accents.
"""

import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import ttk, scrolledtext

# ── Paths ─────────────────────────────────────────────────────────────────────

THIS_DIR   = Path(__file__).parent.resolve()
STATE_FILE = THIS_DIR / "scraper_launcher_state.json"

TOOLS_DIR = THIS_DIR
KOP_DIR   = Path(r"C:\Users\daniu\OneDrive\Documents\GitHub\Kids-Over-Profits")
LOCAL_APPDATA_DIR = Path(os.environ.get("LOCALAPPDATA", str(THIS_DIR)))
MN_BROWSER_PROFILE = LOCAL_APPDATA_DIR / "KidsOverProfits" / "mn-browser-profile"

SCRAPERS = [
    {"name": "Arkansas",    "key": "AR", "script": TOOLS_DIR / "ar_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "Arizona",     "key": "AZ", "script": TOOLS_DIR / "az_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "California",  "key": "CA", "script": TOOLS_DIR / "ca_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "Connecticut", "key": "CT", "script": TOOLS_DIR / "ct_scraper.py",               "cwd": TOOLS_DIR},
    {
        "name": "Minnesota",
        "key": "MN",
        "script": KOP_DIR / "scripts" / "mn_scraper.py",
        "cwd": KOP_DIR,
        # Keep Playwright's persistent profile out of the synced repo to avoid
        # OneDrive/lock-file interference during Chromium startup.
        "env_defaults": {"MN_BROWSER_PROFILE": str(MN_BROWSER_PROFILE)},
    },
    {"name": "Oregon",      "key": "OR", "script": TOOLS_DIR / "or_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "Texas",       "key": "TX", "script": TOOLS_DIR / "tx_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "Utah",        "key": "UT", "script": TOOLS_DIR / "utah_citation_scraper.py",    "cwd": TOOLS_DIR},
    {"name": "Washington",  "key": "WA", "script": TOOLS_DIR / "wa_scraper.py",               "cwd": TOOLS_DIR},
]

# ── KOP palette (from AGENTS.md) ──────────────────────────────────────────────

SOFT_YELLOW   = "#FFF5CB"
MINT          = "#B6E3D4"
TEAL          = "#33A7B5"
NAVY          = "#000080"
MIDNIGHT      = "#000435"
ORANGE        = "#EF9034"
WHITE         = "#FFFFFF"
CHARTREUSE    = "#B2E102"
SPRING_YELLOW = "#ECF385"
CORAL         = "#FE8088"
SAND          = "#F2EEDF"
POWDER_BLUE   = "#AEE0ED"
BUBBLEGUM     = "#FC8ED6"

# Derived contrast-safe variants (the palette lacks explicit darks for success/error text)
SUCCESS_TEXT = "#1f7a56"   # dark teal-green, readable on SAND
ERROR_TEXT   = "#8c1f2e"   # darkened coral, readable on SAND

FONT_TITLE   = ("Segoe UI Semibold", 16)
FONT_SUBTITLE= ("Segoe UI",          10)
FONT_HEADER  = ("Segoe UI Semibold", 10)
FONT_BODY    = ("Segoe UI",          10)
FONT_MONO    = ("Consolas",          9)


if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    PROCESS_SUSPEND_RESUME = 0x0800
    SPI_GETWORKAREA = 0x0030
    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
    _user32 = ctypes.WinDLL("user32", use_last_error=True)

    class RECT(ctypes.Structure):
        _fields_ = [
            ("left", wintypes.LONG),
            ("top", wintypes.LONG),
            ("right", wintypes.LONG),
            ("bottom", wintypes.LONG),
        ]

    _OpenProcess = _kernel32.OpenProcess
    _OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _OpenProcess.restype = wintypes.HANDLE

    _CloseHandle = _kernel32.CloseHandle
    _CloseHandle.argtypes = [wintypes.HANDLE]
    _CloseHandle.restype = wintypes.BOOL

    _NtSuspendProcess = _ntdll.NtSuspendProcess
    _NtSuspendProcess.argtypes = [wintypes.HANDLE]
    _NtSuspendProcess.restype = wintypes.DWORD

    _NtResumeProcess = _ntdll.NtResumeProcess
    _NtResumeProcess.argtypes = [wintypes.HANDLE]
    _NtResumeProcess.restype = wintypes.DWORD

    _SystemParametersInfoW = _user32.SystemParametersInfoW
    _SystemParametersInfoW.argtypes = [wintypes.UINT, wintypes.UINT, wintypes.LPVOID, wintypes.UINT]
    _SystemParametersInfoW.restype = wintypes.BOOL


def set_process_paused(pid: int, paused: bool) -> None:
    if sys.platform == "win32":
        handle = _OpenProcess(PROCESS_SUSPEND_RESUME, False, pid)
        if not handle:
            raise OSError(ctypes.get_last_error(), f"OpenProcess failed for pid {pid}")
        try:
            status = _NtSuspendProcess(handle) if paused else _NtResumeProcess(handle)
            if status != 0:
                verb = "pause" if paused else "resume"
                raise OSError(f"Could not {verb} pid {pid} (NTSTATUS 0x{status:08X})")
        finally:
            _CloseHandle(handle)
        return

    os.kill(pid, signal.SIGSTOP if paused else signal.SIGCONT)


def get_work_area(window) -> tuple[int, int, int, int]:
    if sys.platform == "win32":
        rect = RECT()
        if _SystemParametersInfoW(SPI_GETWORKAREA, 0, ctypes.byref(rect), 0):
            return rect.left, rect.top, rect.right - rect.left, rect.bottom - rect.top
    return 0, 0, window.winfo_screenwidth(), window.winfo_screenheight()


def fit_window_to_screen(window, desired_width: int, desired_height: int,
                         min_width: int = 320, min_height: int = 320,
                         margin: int = 12) -> None:
    window.update_idletasks()
    left, top, work_width, work_height = get_work_area(window)
    frame_width = 0
    titlebar_height = 0

    if window.winfo_ismapped():
        frame_width = max(0, window.winfo_rootx() - window.winfo_x())
        titlebar_height = max(0, window.winfo_rooty() - window.winfo_y())

    decoration_width = frame_width * 2
    decoration_height = titlebar_height + frame_width
    available_width = max(320, work_width - decoration_width - (margin * 2))
    available_height = max(320, work_height - decoration_height - (margin * 2))

    min_width = min(max(320, min_width), available_width)
    min_height = min(max(320, min_height), available_height)
    window.minsize(int(min_width), int(min_height))

    width = min(max(int(desired_width), int(min_width)), available_width)
    height = min(max(int(desired_height), int(min_height)), available_height)

    outer_width = width + decoration_width
    outer_height = height + decoration_height
    min_x = left + margin
    min_y = top + margin
    max_x = left + work_width - outer_width - margin
    max_y = top + work_height - outer_height - margin

    x = left + max(margin, (work_width - outer_width) // 2)
    y = top + max(margin, (work_height - outer_height) // 2)
    x = min(max(min_x, x), max_x)
    y = min(max(min_y, y), max_y)
    window.geometry(f"{width}x{height}+{x}+{y}")


# ── Persistence ───────────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def format_last_run(entry):
    if not entry:
        return "Never run"
    ts = entry.get("timestamp", "")
    try:
        dt = datetime.fromisoformat(ts)
        when = dt.strftime("%b %d, %Y  %I:%M %p")
    except Exception:
        when = ts
    status = "✓ OK" if entry.get("success") else "✗ FAILED"
    fac = entry.get("facilities_saved", 0)
    rep = entry.get("reports_saved", 0)
    stats = f"  —  {fac} facilities, {rep} reports" if (fac or rep) else ""
    return f"{when}   {status}{stats}"


# ── Small styled helpers ──────────────────────────────────────────────────────

class KopButton(tk.Button):
    """Flat button styled with palette colors."""
    def __init__(self, parent, text, command, variant="primary", **kw):
        palettes = {
            "primary":   {"bg": NAVY,    "fg": WHITE, "ab": MIDNIGHT, "af": WHITE},
            "teal":      {"bg": TEAL,    "fg": WHITE, "ab": NAVY,     "af": WHITE},
            "accent":    {"bg": ORANGE,  "fg": WHITE, "ab": MIDNIGHT, "af": WHITE},
            "ghost":     {"bg": SAND,    "fg": MIDNIGHT, "ab": SPRING_YELLOW, "af": MIDNIGHT},
        }
        p = palettes.get(variant, palettes["primary"])
        kw.setdefault("padx", 14)
        kw.setdefault("pady", 6)
        super().__init__(
            parent, text=text, command=command,
            bg=p["bg"], fg=p["fg"],
            activebackground=p["ab"], activeforeground=p["af"],
            bd=0, relief="flat",
            cursor="hand2",
            font=FONT_HEADER,
            **kw,
        )


class Divider(tk.Frame):
    def __init__(self, parent, color=TEAL, height=2, **kw):
        super().__init__(parent, bg=color, height=height, **kw)


# ── Main app ──────────────────────────────────────────────────────────────────

class ScraperLauncher:
    def __init__(self, root):
        self.root = root
        root.title("KOP Scraper Launcher")
        root.configure(bg=SAND)
        fit_window_to_screen(root, desired_width=960, desired_height=860,
                             min_width=760, min_height=560)

        self.state         = load_state()
        self.check_vars    = {}
        self.status_labels = {}
        self.row_frames    = {}
        self.pause_buttons = {}
        self.running       = set()
        self.paused        = set()
        self.processes: dict = {}
        self.log_queue     = queue.Queue()
        self.output_buffers: dict = {}  # key -> list of output lines from last run

        self._build_ui()
        self._refresh_statuses()
        self.root.update()
        self._fit_main_window()
        self.root.after_idle(self._fit_main_window)
        self._poll_log_queue()

    # ---------- UI construction ----------

    def _fit_main_window(self):
        req_width = self.root.winfo_reqwidth()
        req_height = self.root.winfo_reqheight()
        log_req_height = self.log_text.winfo_reqheight()

        # The log pane is the only large flexible region. Keep a reasonable
        # minimum log height, but do not let the window shrink below the width
        # needed for the action buttons and scraper rows.
        min_log_height = 160
        min_width = max(760, req_width)
        min_height = max(560, req_height - max(0, log_req_height - min_log_height))

        desired_width = min(960, max(min_width, req_width))
        desired_height = min(860, max(min_height, req_height))
        fit_window_to_screen(self.root, desired_width=desired_width, desired_height=desired_height,
                             min_width=min_width, min_height=min_height)

    def _build_ui(self):
        # Header band — Midnight Blue with White title
        header = tk.Frame(self.root, bg=MIDNIGHT)
        header.pack(fill="x")
        tk.Label(header, text="KOP Scraper Launcher",
                 bg=MIDNIGHT, fg=WHITE, font=FONT_TITLE,
                 padx=20).pack(anchor="w", pady=(8, 0))
        tk.Label(header, text="Monthly inspection scraper control panel",
                 bg=MIDNIGHT, fg=POWDER_BLUE, font=FONT_SUBTITLE,
                 padx=20).pack(anchor="w", pady=(0, 8))
        # Chartreuse accent stripe under the header
        Divider(self.root, color=CHARTREUSE, height=3).pack(fill="x")

        # Body
        body = tk.Frame(self.root, bg=SAND, padx=16, pady=8)
        body.pack(fill="both", expand=True)

        # ── Scrapers card ─────────────────────────────────────────────
        card = tk.Frame(body, bg=WHITE, highlightbackground=TEAL,
                        highlightthickness=1, bd=0)
        card.pack(fill="x")

        card_title = tk.Frame(card, bg=WHITE)
        card_title.pack(fill="x", padx=14, pady=(6, 3))
        tk.Label(card_title, text="Scrapers", bg=WHITE, fg=MIDNIGHT,
                 font=FONT_HEADER).pack(side="left")
        tk.Label(card_title, text=f"{len(SCRAPERS)} configured",
                 bg=WHITE, fg=TEAL, font=FONT_SUBTITLE).pack(side="right")
        Divider(card, color=MINT, height=1).pack(fill="x", padx=14)

        list_frame = tk.Frame(card, bg=WHITE, padx=14, pady=4)
        list_frame.pack(fill="x")

        # Column headers
        hdr = tk.Frame(list_frame, bg=WHITE)
        hdr.pack(fill="x", pady=(1, 2))
        tk.Label(hdr, text="", bg=WHITE, width=3).pack(side="left")
        tk.Label(hdr, text="Scraper", bg=WHITE, fg=MIDNIGHT,
                 font=FONT_HEADER, width=18, anchor="w").pack(side="left")
        tk.Label(hdr, text="Last Run", bg=WHITE, fg=MIDNIGHT,
                 font=FONT_HEADER, anchor="w").pack(side="left", padx=8)

        for scraper in SCRAPERS:
            self._build_row(list_frame, scraper)

        # ── Bulk actions bar ──────────────────────────────────────────
        actions = tk.Frame(body, bg=SAND, pady=6)
        actions.pack(fill="x")

        self.parallel_var = tk.BooleanVar(value=False)
        chk = tk.Checkbutton(actions, text="Run in parallel",
                             variable=self.parallel_var,
                             bg=SAND, fg=MIDNIGHT, font=FONT_BODY,
                             activebackground=SAND, activeforeground=MIDNIGHT,
                             selectcolor=WHITE, bd=0,
                             highlightthickness=0)
        chk.pack(side="left")

        KopButton(actions, "Run Selected", self._run_selected,
                  variant="primary").pack(side="left", padx=(16, 6))
        KopButton(actions, "Run All", self._run_all,
                  variant="teal").pack(side="left", padx=6)
        KopButton(actions, "Clear Log", self._clear_log,
                  variant="ghost").pack(side="right")

        # ── Output card ───────────────────────────────────────────────
        log_card = tk.Frame(body, bg=WHITE, highlightbackground=TEAL,
                            highlightthickness=1, bd=0)
        log_card.pack(fill="both", expand=True)

        log_title = tk.Frame(log_card, bg=WHITE)
        log_title.pack(fill="x", padx=14, pady=(10, 4))
        tk.Label(log_title, text="Output", bg=WHITE, fg=MIDNIGHT,
                 font=FONT_HEADER).pack(side="left")
        tk.Label(log_title, text="live — prefixed with scraper code",
                 bg=WHITE, fg=TEAL, font=FONT_SUBTITLE).pack(side="right")
        Divider(log_card, color=MINT, height=1).pack(fill="x", padx=14)

        log_wrap = tk.Frame(log_card, bg=WHITE, padx=10, pady=10)
        log_wrap.pack(fill="both", expand=True)
        self.log_text = scrolledtext.ScrolledText(
            log_wrap, wrap="word", font=FONT_MONO,
            state="disabled",
            bg=MIDNIGHT, fg=WHITE,
            insertbackground=CHARTREUSE,
            relief="flat", bd=0, padx=10, pady=8,
        )
        self.log_text.pack(fill="both", expand=True)
        # Tags for colorized status lines
        self.log_text.tag_config("start",   foreground=CHARTREUSE)
        self.log_text.tag_config("ok",      foreground=MINT)
        self.log_text.tag_config("err",     foreground=CORAL)
        self.log_text.tag_config("system",  foreground=POWDER_BLUE)

        # Footer
        footer = tk.Frame(self.root, bg=SAND)
        footer.pack(fill="x", padx=16, pady=(0, 10))
        tk.Label(footer,
                 text=f"state: {STATE_FILE.name}",
                 bg=SAND, fg="#888", font=FONT_SUBTITLE).pack(side="right")

    def _build_row(self, parent, scraper):
        """One checkbox + name + last-run + action buttons row."""
        key = scraper["key"]
        row = tk.Frame(parent, bg=WHITE)
        row.pack(fill="x", pady=1)
        self.row_frames[key] = row

        var = tk.BooleanVar()
        self.check_vars[key] = var
        tk.Checkbutton(row, variable=var,
                       bg=WHITE, activebackground=WHITE,
                       selectcolor=SPRING_YELLOW,
                       bd=0, highlightthickness=0).pack(side="left")

        tk.Label(row, text=scraper["name"], bg=WHITE, fg=MIDNIGHT,
                 font=FONT_BODY, width=18, anchor="w").pack(side="left")

        status_lbl = tk.Label(row, text="—", bg=WHITE, fg="#888",
                              font=FONT_BODY, anchor="w")
        status_lbl.pack(side="left", padx=8, fill="x", expand=True)
        self.status_labels[key] = status_lbl

        KopButton(row, "Results", lambda k=key: self._show_results(k),
                  variant="ghost", pady=3).pack(side="right", padx=(0, 4))
        pause_btn = KopButton(row, "Pause", lambda k=key: self._toggle_pause(k),
                              variant="accent", pady=3)
        pause_btn.pack(side="right", padx=(0, 4))
        self.pause_buttons[key] = pause_btn
        KopButton(row, "Run", lambda s=scraper: self._run_one(s),
                  variant="teal", pady=3).pack(side="right")

    # ---------- Status / logging ----------

    def _refresh_statuses(self):
        for scraper in SCRAPERS:
            key = scraper["key"]
            lbl = self.status_labels[key]
            row = self.row_frames[key]
            pause_btn = self.pause_buttons[key]
            if key in self.running:
                lbl.configure(text="Running…", fg=ORANGE)
                row.configure(bg=SOFT_YELLOW)
                for child in row.winfo_children():
                    if isinstance(child, (tk.Label, tk.Checkbutton)):
                        try:
                            child.configure(bg=SOFT_YELLOW)
                        except tk.TclError:
                            pass
                pause_btn.configure(text="Pause")
                if key in self.paused:
                    lbl.configure(text="Paused", fg=NAVY)
                    row.configure(bg=POWDER_BLUE)
                    pause_btn.configure(text="Resume")
                    for child in row.winfo_children():
                        if isinstance(child, (tk.Label, tk.Checkbutton)):
                            try:
                                child.configure(bg=POWDER_BLUE)
                            except tk.TclError:
                                pass
            else:
                entry = self.state.get(key)
                text  = format_last_run(entry)
                color = "#888"
                if entry:
                    color = SUCCESS_TEXT if entry.get("success") else ERROR_TEXT
                lbl.configure(text=text, fg=color)
                row.configure(bg=WHITE)
                pause_btn.configure(text="Pause")
                for child in row.winfo_children():
                    if isinstance(child, (tk.Label, tk.Checkbutton)):
                        try:
                            child.configure(bg=WHITE)
                        except tk.TclError:
                            pass

    def _log(self, prefix, line, tag=None):
        self.log_queue.put((f"[{prefix}] {line}\n", tag))

    def _poll_log_queue(self):
        while True:
            try:
                msg, tag = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_text.configure(state="normal")
            if tag:
                self.log_text.insert("end", msg, tag)
            else:
                self.log_text.insert("end", msg)
            self.log_text.see("end")
            self.log_text.configure(state="disabled")
        self.root.after(100, self._poll_log_queue)

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    # ---------- Run control ----------

    def _run_one(self, scraper):
        if scraper["key"] in self.running:
            self._log(scraper["key"], "Already running — skipping.", tag="system")
            return
        threading.Thread(target=self._execute, args=(scraper,), daemon=True).start()

    def _active_process(self, key):
        proc = self.processes.get(key)
        if not proc:
            return None
        if proc.poll() is not None:
            self.processes.pop(key, None)
            self.paused.discard(key)
            return None
        return proc

    def _toggle_pause(self, key):
        if key in self.paused:
            self._resume_one(key)
        else:
            self._pause_one(key)

    def _pause_one(self, key):
        proc = self._active_process(key)
        if not proc:
            self._log(key, "Nothing running to pause.", tag="system")
            return
        if key in self.paused:
            self._log(key, "Already paused.", tag="system")
            return
        try:
            set_process_paused(proc.pid, True)
        except Exception as exc:
            self._log(key, f"ERROR: could not pause process: {exc}", tag="err")
            return
        self.paused.add(key)
        self._refresh_statuses()
        self._log(key, f"Paused (pid {proc.pid})", tag="system")

    def _resume_one(self, key):
        proc = self._active_process(key)
        if not proc:
            self._log(key, "Nothing paused to resume.", tag="system")
            return
        if key not in self.paused:
            self._log(key, "Scraper is not paused.", tag="system")
            return
        try:
            set_process_paused(proc.pid, False)
        except Exception as exc:
            self._log(key, f"ERROR: could not resume process: {exc}", tag="err")
            return
        self.paused.discard(key)
        self._refresh_statuses()
        self._log(key, f"Resumed (pid {proc.pid})", tag="system")

    def _run_selected(self):
        selected = [s for s in SCRAPERS if self.check_vars[s["key"]].get()]
        if not selected:
            self._log("system", "Nothing selected.", tag="system")
            return
        self._run_batch(selected)

    def _run_all(self):
        self._run_batch(SCRAPERS)

    def _run_batch(self, scrapers):
        if self.parallel_var.get():
            for s in scrapers:
                self._run_one(s)
        else:
            def sequential():
                for s in scrapers:
                    if s["key"] not in self.running:
                        self._execute(s)
            threading.Thread(target=sequential, daemon=True).start()

    def _execute(self, scraper):
        key    = scraper["key"]
        script = Path(scraper["script"])
        cwd    = Path(scraper["cwd"])
        env    = os.environ.copy()

        for env_key, env_value in scraper.get("env_defaults", {}).items():
            env.setdefault(env_key, env_value)

        if not script.exists():
            self._log(key, f"ERROR: script not found — {script}", tag="err")
            self._mark_done(key, False)
            return

        self.running.add(key)
        self.root.after(0, self._refresh_statuses)
        self._log(key, f"▶ Starting {scraper['name']} ({script.name})", tag="start")
        buffer = []
        self.output_buffers[key] = buffer

        try:
            proc = subprocess.Popen(
                [sys.executable, "-X", "utf8", "-u", str(script)],
                cwd=str(cwd),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                encoding="utf-8",
                errors="replace",
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                env=env,
            )
            self.processes[key] = proc
            if proc.stdout is None:
                raise RuntimeError("Could not capture scraper output stream")
            for line in proc.stdout:
                stripped = line.rstrip()
                buffer.append(stripped)
                if len(buffer) > 2000:
                    buffer.pop(0)
                self._log(key, stripped)
            code = proc.wait()
            ok   = (code == 0)
            self._log(key, f"{'✓' if ok else '✗'} Finished (exit {code})",
                      tag="ok" if ok else "err")
            self._mark_done(key, ok)
        except Exception as e:
            self._log(key, f"ERROR: {e}", tag="err")
            self._mark_done(key, False)
        finally:
            self.processes.pop(key, None)
            self.paused.discard(key)
            self.running.discard(key)
            self.root.after(0, self._refresh_statuses)

    def _mark_done(self, key, ok):
        stats = self._parse_stats(self.output_buffers.get(key, []))
        self.state[key] = {
            "timestamp": datetime.now().isoformat(),
            "success": ok,
            "output": self.output_buffers.get(key, [])[-1000:],
            **stats,
        }
        save_state(self.state)

    @staticmethod
    def _parse_stats(output: list) -> dict:
        facilities, reports = 0, 0
        for line in reversed(output):
            # "API saved 45 facilities, 171 reports"
            m = re.search(r"API saved (\d+) facilit\w+,\s*(\d+) report", line)
            if m:
                return {"facilities_saved": int(m.group(1)), "reports_saved": int(m.group(2))}
            # "Scraped 45 facilities, 171 reports"
            m = re.search(r"Scraped (\d+) facilit\w+.*?(\d+) report", line)
            if m:
                facilities = int(m.group(1))
                reports = int(m.group(2))
        return {"facilities_saved": facilities, "reports_saved": reports}

    def _show_results(self, key: str) -> None:
        entry = self.state.get(key)
        scraper_name = next((s["name"] for s in SCRAPERS if s["key"] == key), key)

        win = tk.Toplevel(self.root)
        win.title(f"{scraper_name} — Last Run Results")
        win.configure(bg=SAND)
        fit_window_to_screen(win, desired_width=820, desired_height=580,
                             min_width=600, min_height=420)

        # ── Header ────────────────────────────────────────────────────────
        hdr = tk.Frame(win, bg=MIDNIGHT)
        hdr.pack(fill="x")
        tk.Label(hdr, text=f"{scraper_name} — Last Run",
                 bg=MIDNIGHT, fg=WHITE, font=FONT_TITLE, padx=16).pack(anchor="w", pady=(12, 2))
        if entry:
            ok = entry.get("success")
            tk.Label(hdr,
                     text="✓ Succeeded" if ok else "✗ Failed",
                     bg=MIDNIGHT, fg=MINT if ok else CORAL,
                     font=FONT_SUBTITLE, padx=16).pack(anchor="w", pady=(0, 12))
        else:
            tk.Label(hdr, text="No run recorded yet",
                     bg=MIDNIGHT, fg=POWDER_BLUE, font=FONT_SUBTITLE, padx=16).pack(anchor="w", pady=(0, 12))
        Divider(win, color=CHARTREUSE, height=3).pack(fill="x")

        # ── Stats strip ───────────────────────────────────────────────────
        if entry:
            stats_frame = tk.Frame(win, bg=WHITE, padx=16, pady=10)
            stats_frame.pack(fill="x")
            ts = entry.get("timestamp", "")
            try:
                when = datetime.fromisoformat(ts).strftime("%B %d, %Y at %I:%M %p")
            except Exception:
                when = ts
            tk.Label(stats_frame, text=f"Run at: {when}",
                     bg=WHITE, fg=MIDNIGHT, font=FONT_BODY).pack(anchor="w")
            fac = entry.get("facilities_saved", 0)
            rep = entry.get("reports_saved", 0)
            if fac or rep:
                tk.Label(stats_frame,
                         text=f"Facilities saved: {fac}    Reports saved: {rep}",
                         bg=WHITE, fg=TEAL, font=FONT_HEADER).pack(anchor="w", pady=(4, 0))
            Divider(win, color=MINT, height=1).pack(fill="x", padx=16)

        # ── Log output ────────────────────────────────────────────────────
        log_outer = tk.Frame(win, bg=WHITE)
        log_outer.pack(fill="both", expand=True, padx=12, pady=(8, 0))
        tk.Label(log_outer, text="Output Log", bg=WHITE, fg=MIDNIGHT,
                 font=FONT_HEADER).pack(anchor="w", padx=4, pady=(0, 4))
        log_text = scrolledtext.ScrolledText(
            log_outer, wrap="word", font=FONT_MONO,
            bg=MIDNIGHT, fg=WHITE, relief="flat", bd=0, padx=8, pady=6,
        )
        log_text.pack(fill="both", expand=True)

        lines = (entry or {}).get("output", [])
        if lines:
            log_text.insert("1.0", "\n".join(lines))
        else:
            log_text.insert("1.0", "(no output recorded — run this scraper to capture logs)")
        log_text.configure(state="disabled")
        log_text.see("end")

        # ── Close button ──────────────────────────────────────────────────
        tk.Button(win, text="Close", command=win.destroy,
                  bg=NAVY, fg=WHITE, font=FONT_BODY,
                  bd=0, relief="flat", padx=20, pady=7).pack(pady=10)


def main():
    root = tk.Tk()
    ScraperLauncher(root)
    root.mainloop()


if __name__ == "__main__":
    main()
