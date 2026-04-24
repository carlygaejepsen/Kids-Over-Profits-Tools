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
import queue
import re
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

SCRAPERS = [
    {"name": "Arkansas",    "key": "AR", "script": TOOLS_DIR / "ar_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "Arizona",     "key": "AZ", "script": TOOLS_DIR / "az_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "California",  "key": "CA", "script": TOOLS_DIR / "ca_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "Connecticut", "key": "CT", "script": TOOLS_DIR / "ct_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "Minnesota",   "key": "MN", "script": KOP_DIR / "scripts" / "mn_scraper.py",     "cwd": KOP_DIR},
    {"name": "Oregon",      "key": "OR", "script": TOOLS_DIR / "or_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "Texas",       "key": "TX", "script": TOOLS_DIR / "tx_scraper.py",               "cwd": TOOLS_DIR},
    {"name": "Utah",        "key": "UT", "script": TOOLS_DIR / "utah_citation_scraper.v2.py", "cwd": TOOLS_DIR},
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
        super().__init__(
            parent, text=text, command=command,
            bg=p["bg"], fg=p["fg"],
            activebackground=p["ab"], activeforeground=p["af"],
            bd=0, relief="flat", padx=14, pady=6,
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
        root.minsize(760, 560)
        root.configure(bg=SAND)
        # Center on screen
        root.update_idletasks()
        _w, _h = 900, 720
        _x = max(0, (root.winfo_screenwidth() - _w) // 2)
        _y = max(0, (root.winfo_screenheight() - _h) // 2)
        root.geometry(f"{_w}x{_h}+{_x}+{_y}")

        self.state         = load_state()
        self.check_vars    = {}
        self.status_labels = {}
        self.row_frames    = {}
        self.running       = set()
        self.log_queue     = queue.Queue()
        self.output_buffers: dict = {}  # key -> list of output lines from last run

        self._build_ui()
        self._refresh_statuses()
        self._poll_log_queue()

    # ---------- UI construction ----------

    def _build_ui(self):
        # Header band — Midnight Blue with White title
        header = tk.Frame(self.root, bg=MIDNIGHT)
        header.pack(fill="x")
        tk.Label(header, text="KOP Scraper Launcher",
                 bg=MIDNIGHT, fg=WHITE, font=FONT_TITLE,
                 padx=20).pack(anchor="w", pady=(14, 0))
        tk.Label(header, text="Monthly inspection scraper control panel",
                 bg=MIDNIGHT, fg=POWDER_BLUE, font=FONT_SUBTITLE,
                 padx=20).pack(anchor="w", pady=(0, 14))
        # Chartreuse accent stripe under the header
        Divider(self.root, color=CHARTREUSE, height=3).pack(fill="x")

        # Body
        body = tk.Frame(self.root, bg=SAND, padx=16, pady=16)
        body.pack(fill="both", expand=True)

        # ── Scrapers card ─────────────────────────────────────────────
        card = tk.Frame(body, bg=WHITE, highlightbackground=TEAL,
                        highlightthickness=1, bd=0)
        card.pack(fill="x")

        card_title = tk.Frame(card, bg=WHITE)
        card_title.pack(fill="x", padx=14, pady=(12, 6))
        tk.Label(card_title, text="Scrapers", bg=WHITE, fg=MIDNIGHT,
                 font=FONT_HEADER).pack(side="left")
        tk.Label(card_title, text=f"{len(SCRAPERS)} configured",
                 bg=WHITE, fg=TEAL, font=FONT_SUBTITLE).pack(side="right")
        Divider(card, color=MINT, height=1).pack(fill="x", padx=14)

        list_frame = tk.Frame(card, bg=WHITE, padx=14, pady=8)
        list_frame.pack(fill="x")

        # Column headers
        hdr = tk.Frame(list_frame, bg=WHITE)
        hdr.pack(fill="x", pady=(2, 4))
        tk.Label(hdr, text="", bg=WHITE, width=3).pack(side="left")
        tk.Label(hdr, text="Scraper", bg=WHITE, fg=MIDNIGHT,
                 font=FONT_HEADER, width=18, anchor="w").pack(side="left")
        tk.Label(hdr, text="Last Run", bg=WHITE, fg=MIDNIGHT,
                 font=FONT_HEADER, anchor="w").pack(side="left", padx=8)

        for scraper in SCRAPERS:
            self._build_row(list_frame, scraper)

        # ── Bulk actions bar ──────────────────────────────────────────
        actions = tk.Frame(body, bg=SAND, pady=14)
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
        """One checkbox + name + last-run + Run button row."""
        key = scraper["key"]
        row = tk.Frame(parent, bg=WHITE)
        row.pack(fill="x", pady=3)
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
                  variant="ghost").pack(side="right", padx=(0, 4))
        KopButton(row, "Run", lambda s=scraper: self._run_one(s),
                  variant="teal").pack(side="right")

    # ---------- Status / logging ----------

    def _refresh_statuses(self):
        for scraper in SCRAPERS:
            key = scraper["key"]
            lbl = self.status_labels[key]
            row = self.row_frames[key]
            if key in self.running:
                lbl.configure(text="Running…", fg=ORANGE)
                row.configure(bg=SOFT_YELLOW)
                for child in row.winfo_children():
                    if isinstance(child, (tk.Label, tk.Checkbutton)):
                        try:
                            child.configure(bg=SOFT_YELLOW)
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
                [sys.executable, "-u", str(script)],
                cwd=str(cwd),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                encoding="utf-8",
                errors="replace",
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
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
        win.minsize(600, 420)
        win.update_idletasks()
        _w, _h = 820, 580
        _x = max(0, (win.winfo_screenwidth() - _w) // 2)
        _y = max(0, (win.winfo_screenheight() - _h) // 2)
        win.geometry(f"{_w}x{_h}+{_x}+{_y}")

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
