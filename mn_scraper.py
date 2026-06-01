"""
MN DHS Children's Residential Facilities Scraper
=================================================
Uses your real Chrome browser (with existing session cookies) to visit each
facility detail page and fetch inspection documents. Saves progress as it
goes so you can resume if interrupted.

BEFORE RUNNING: Close Chrome. Playwright needs to open your Chrome profile,
and Chrome can only run in one instance at a time.

Required:
    MN_LICENSE_CSV        Path to CSV export from DHS Licensing Lookup
    INSPECTIONS_API_BASE  e.g. https://kidsoverprofits.org/wp-content/themes/child
    KOP_DATA_API_KEY      API key for inspections-write.php

Optional:
    MN_LIMIT_IDS          Only scrape first N facilities (for testing)
    MN_CHROME_USER_DATA   Path to Chrome User Data dir (auto-detected if not set)
"""

import asyncio
import csv
import io
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, parse_qsl, urlencode, urlparse

from playwright.async_api import async_playwright

# Fix Windows console encoding
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

try:
    from dotenv import load_dotenv
    # Config (MN_LICENSE_CSV, keys) lives in the sibling Kids-Over-Profits repo's
    # .env, with a same-dir .env as an optional override. The launcher also injects
    # these vars when it starts the scraper; this keeps standalone runs working.
    _here = Path(__file__).resolve()
    for _env in (_here.parent / ".env", _here.parents[2] / "Kids-Over-Profits" / ".env"):
        if _env.exists():
            load_dotenv(_env)
            break
except ImportError:
    pass

# ── CONFIG ────────────────────────────────────────────────────────────────────

MN_LICENSE_CSV = os.environ.get("MN_LICENSE_CSV", "").strip()
API_BASE       = os.environ.get("INSPECTIONS_API_BASE", "https://kidsoverprofits.org/wp-content/themes/child")
API_KEY        = os.environ.get("KOP_DATA_API_KEY", "CHANGE_ME")
MN_LIMIT_IDS   = int(os.environ.get("MN_LIMIT_IDS", "0") or 0)

BROWSER_PROFILE = Path(os.environ.get(
    "MN_BROWSER_PROFILE",
    str(Path(__file__).parent / ".mn-browser-profile"),
))

PROGRESS_FILE = Path(__file__).parent / "mn_progress.json"
# The website reads its MN data from the Kids-Over-Profits repo's js/data dir.
OUTPUT_JSON   = Path(__file__).resolve().parents[2] / "Kids-Over-Profits" / "js" / "data" / "mn_reports.json"

DOC_BASE    = "https://www.dhs.state.mn.us"
DETAIL_BASE = "https://licensinglookup.dhs.state.mn.us/Details.aspx?l={}"
DELAY_MS    = 1200   # ms between page loads

# ── HELPERS ───────────────────────────────────────────────────────────────────

def clean(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("\xa0", " ")).strip()


def canonicalize_doc_url(url):
    if not url:
        return ""
    url = clean(url)
    if url.startswith("/main/idcplg"):
        url = urljoin(DOC_BASE, url)
    # Always use https
    url = re.sub(r"^http://", "https://", url)
    p = urlparse(url)
    if "dhs.state.mn.us" in p.netloc and "/main/idcplg" in p.path and p.query:
        pairs = parse_qsl(p.query, keep_blank_values=True)
        keyed = dict(pairs)
        ordered = [(k, keyed[k]) for k in ("IdcService", "RevisionSelectionMethod", "dDocName") if k in keyed]
        ordered += [(k, v) for k, v in pairs if k not in ("IdcService", "RevisionSelectionMethod", "dDocName")]
        url = f"{p.scheme}://{p.netloc}{p.path}?{urlencode(ordered)}"
    return url


def extract_doc_urls(html):
    from html import unescape
    found = []
    found += re.findall(r"https?://www\.dhs\.state\.mn\.us/main/idcplg\?[^\"'\s<>]+", html, re.I)
    for m in re.findall(r"/main/idcplg\?[^\"'\s<>]+", html, re.I):
        found.append(urljoin(DOC_BASE, m))
    for token in re.findall(r"\b(LLO_[A-Za-z0-9_-]+)\b", html):
        found.append(
            f"{DOC_BASE}/main/idcplg?IdcService=GET_DYNAMIC_CONVERSION"
            f"&RevisionSelectionMethod=LatestReleased&dDocName={token}"
        )
    seen, dedup = set(), []
    for u in found:
        c = canonicalize_doc_url(unescape(u))  # unescape &amp; → &
        if c and c not in seen:
            seen.add(c)
            dedup.append(c)
    return dedup


def load_csv(path):
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            lid = re.sub(r"\D", "", clean(row.get("License Number", "")))
            if not lid:
                continue
            parts = [clean(row.get(k, "")) for k in ("AddressLine1", "AddressLine2", "AddressLine3")]
            city, state, zip_ = clean(row.get("City", "")), clean(row.get("State", "")), clean(row.get("Zip", ""))
            addr = ", ".join(p for p in parts if p)
            if city:
                addr += f", {city}, {state} {zip_}".rstrip()
            rows.append({
                "license_id": lid,
                "facility_info": {
                    "facility_name":        clean(row.get("Name of Program", "")) or f"License {lid}",
                    "program_name":         lid,
                    "program_category":     clean(row.get("License Type", "Children's Residential Facility")),
                    "full_address":         addr,
                    "phone":                clean(row.get("Phone", "")),
                    "bed_capacity":         clean(row.get("Capacity", "")),
                    "executive_director":   clean(row.get("License Holder", "")),
                    "license_exp_date":     clean(row.get("Expiration Date", "")),
                    "relicense_visit_date": "",
                    "action":               clean(row.get("License Status", "")),
                },
            })
    return rows


def load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_progress(data):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def build_report(license_id, idx, doc_url, content):
    date_m = re.search(
        r"(\w+ \d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})",
        content[:600],
    )
    report_date = clean(date_m.group(1)) if date_m else ""

    snippet = content[:400].lower()
    doc_type = "Document"
    for kw, dt in [
        ("correction order", "Correction Order"),
        ("maltreatment",     "Maltreatment Finding"),
        ("violation",        "Notice of Violation"),
        ("inspection",       "Inspection Report"),
        ("compliance",       "Compliance Report"),
        ("notice",           "Notice"),
        ("order",            "Order"),
    ]:
        if kw in snippet or kw in doc_url.lower():
            doc_type = dt
            break

    tags = [tag for kw, tag in [("violation", "violation"), ("correction", "correction order"), ("maltreatment", "maltreatment")] if kw in snippet or kw in doc_url.lower()]

    # Build summary: skip the letter header, use the first paragraph after "Dear X:"
    summary = ""
    dear_m = re.search(r"Dear\s+\w[^:\n]*:", content, re.IGNORECASE)
    if dear_m:
        body_start = content[dear_m.end():].strip()
        # Take text up to the first blank line or 400 chars
        first_para = re.split(r'\n\n', body_start)[0].replace('\n', ' ').strip()
        summary = first_para[:400]
        if len(first_para) > 400:
            # End at last sentence boundary
            cut = first_para[:400].rfind('.')
            summary = first_para[:cut + 1] if cut > 100 else first_para[:400]
    if not summary:
        summary = content[:200].strip()

    return {
        "report_id":      f"MN-{license_id}-{idx:03d}",
        "report_date":    report_date,
        "report_url":     doc_url,
        "raw_content":    content,
        "content_length": len(content),
        "is_structured":  False,
        "summary":        summary,
        "categories": {
            "doc_type":     doc_type,
            "tags":         tags,
            "doc_page_url": doc_url,
        },
    }


async def wait_for_captcha(page, label="page"):
    """If hit with a CAPTCHA, wait up to 3 min for manual solve."""
    for _ in range(36):
        await page.wait_for_timeout(5000)
        try:
            content = await page.content()
        except Exception:
            continue
        if "radware captcha" not in content.lower() and "validate.perfdrive.com" not in page.url:
            print(f"    Challenge cleared on {label}.")
            return True
    print(f"    Timed out waiting for challenge on {label}.")
    return False


async def is_captcha(page):
    try:
        content = await page.content()
    except Exception:
        return False
    return "radware captcha" in content.lower() or "validate.perfdrive.com" in page.url


async def goto_safe(page, url):
    """Navigate to URL, handling CAPTCHA if needed. Returns page content or None."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(800)
    except Exception as e:
        # 4xx/5xx still loads content — only warn on real failures
        if "net::" not in str(e) and "ERR_HTTP" not in str(e):
            print(f"    Load warning: {e}")
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=5000)
        except Exception:
            pass

    if await is_captcha(page):
        print(f"    CAPTCHA — please solve it in the browser window (waiting up to 3 min)...")
        if not await wait_for_captcha(page, url[:60]):
            return None

    try:
        return await page.content()
    except Exception:
        return None


# ── MAIN ──────────────────────────────────────────────────────────────────────

def free_browser_profile(profile: Path) -> None:
    """Release the dedicated Chromium profile before launching.

    The browser runs off-screen, so an interrupted run (launcher closed, process
    killed/paused, machine sleep) leaves an invisible Chromium alive that keeps
    the profile locked. The next launch then dies instantly with Chromium
    exit 21 / "profile in use" (TargetClosedError). Kill any leftover Chromium
    still holding *this* profile and clear stale singleton locks. The profile is
    private to this scraper, so anything using it is a leftover. Best-effort.
    """
    if sys.platform == "win32":
        kill = (
            "$p=$env:KOP_MN_PROFILE;"
            "Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" |"
            " Where-Object { $_.CommandLine -like \"*$p*\" } |"
            " ForEach-Object { try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop } catch {} }"
        )
        try:
            subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", kill],
                env={**os.environ, "KOP_MN_PROFILE": str(profile)},
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                capture_output=True, timeout=30,
            )
        except Exception as exc:
            print(f"(could not sweep leftover browsers: {exc})")

    for name in ("lockfile", "SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (profile / name).unlink()
        except OSError:
            pass


async def run():
    if not MN_LICENSE_CSV:
        print("ERROR: MN_LICENSE_CSV is not set. Add it to .env or set it as an environment variable.")
        sys.exit(1)

    print(f"Loading CSV: {MN_LICENSE_CSV}")
    facilities = load_csv(MN_LICENSE_CSV)
    print(f"Loaded {len(facilities)} facilities")

    if MN_LIMIT_IDS > 0:
        facilities = facilities[:MN_LIMIT_IDS]
        print(f"Limiting to first {len(facilities)} (MN_LIMIT_IDS={MN_LIMIT_IDS})")

    progress = load_progress()
    with_prior = sum(1 for f in facilities if f["license_id"] in progress)
    if with_prior:
        print(f"{with_prior}/{len(facilities)} have prior data; checking for new inspections\n")

    print(f"Browser profile: {BROWSER_PROFILE}")
    print("(Profile is saved between runs — you only need to solve CAPTCHA once)\n")

    BROWSER_PROFILE.mkdir(parents=True, exist_ok=True)
    # Clear any leftover off-screen Chromium / stale lock from an interrupted run,
    # otherwise this launch fails with "profile in use" (exit 21).
    free_browser_profile(BROWSER_PROFILE)

    # Off-screen by default so the Chromium window doesn't steal focus or sit
    # on top of other work. Set MN_BROWSER_VISIBLE=1 to launch it on-screen
    # (needed when DHS throws a CAPTCHA that you have to solve manually).
    mn_visible = os.environ.get("MN_BROWSER_VISIBLE", "").strip().lower() in {"1", "true", "yes"}
    chromium_args = ["--disable-blink-features=AutomationControlled"]
    if not mn_visible:
        chromium_args.append("--window-position=-32000,-32000")
        chromium_args.append("--window-size=1280,900")
        print("(Browser launched off-screen. Set MN_BROWSER_VISIBLE=1 to show it — needed if a CAPTCHA appears.)")

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_PROFILE),
            headless=False,
            args=chromium_args,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = context.pages[0] if context.pages else await context.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        for i, fac in enumerate(facilities, 1):
            lid = fac["license_id"]
            name = fac["facility_info"]["facility_name"]

            existing_entry = progress.get(lid) or {}
            existing_reports = list(existing_entry.get("reports") or [])
            seen_urls = {r.get("report_url") for r in existing_reports if r.get("report_url")}

            print(f"[{i}/{len(facilities)}] {name} (license {lid})")

            # Visit detail page to find document URLs
            detail_url = DETAIL_BASE.format(lid)
            content = await goto_safe(page, detail_url)
            if content is None:
                print(f"  Skipping {lid} (CAPTCHA not cleared)")
                if lid not in progress:
                    progress[lid] = {"facility_info": fac["facility_info"], "reports": []}
                    save_progress(progress)
                continue

            doc_urls = extract_doc_urls(content)
            new_doc_urls = [u for u in doc_urls if u not in seen_urls]
            print(f"  Found {len(doc_urls)} document link(s); {len(new_doc_urls)} new")

            if not new_doc_urls:
                # Refresh facility_info in case CSV fields changed; keep existing reports.
                progress[lid] = {"facility_info": fac["facility_info"], "reports": existing_reports}
                save_progress(progress)
                await page.wait_for_timeout(DELAY_MS)
                continue

            # Fetch each NEW document in the same browser session.
            # Index continues after existing reports so old report_ids stay stable.
            new_reports = []
            for offset, doc_url in enumerate(new_doc_urls, 1):
                j = len(existing_reports) + offset
                doc_content = await goto_safe(page, doc_url)
                if doc_content is None:
                    print(f"  Doc {j}: skipped (CAPTCHA)")
                    continue

                # Extract visible text, preserving paragraph structure
                raw = await page.evaluate("document.body.innerText")
                # Normalize line endings, strip each line, collapse 3+ blank lines to 2
                lines = [l.strip() for l in re.sub(r'\r\n|\r', '\n', raw).split('\n')]
                text = re.sub(r'\n{3,}', '\n\n', '\n'.join(lines)).strip()
                if len(text) < 50:
                    print(f"  Doc {j}: too short, skipping")
                    continue

                new_reports.append(build_report(lid, j, doc_url, text))
                print(f"  Doc {j}: {new_reports[-1]['categories']['doc_type']} ({len(text)} chars)")
                await page.wait_for_timeout(DELAY_MS)

            all_reports = existing_reports + new_reports
            progress[lid] = {"facility_info": fac["facility_info"], "reports": all_reports}
            save_progress(progress)
            print(f"  -> {len(new_reports)} new report(s) saved (total: {len(all_reports)})\n")

            await page.wait_for_timeout(DELAY_MS)

        await context.close()

    # Build final output
    all_facilities = [progress[f["license_id"]] for f in facilities if f["license_id"] in progress]
    total_reports = sum(len(f["reports"]) for f in all_facilities)
    print(f"\nComplete: {len(all_facilities)} facilities, {total_reports} reports total")

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "facilities": all_facilities,
            "scraped_timestamp": datetime.now(timezone.utc).isoformat(),
            "scraping_notes": {
                "total_facilities": len(all_facilities),
                "total_reports": total_reports,
                "source": "MN DHS Licensing Lookup",
            },
        }, f, indent=2, ensure_ascii=False)
    print(f"Saved: {OUTPUT_JSON}")

    import requests as req
    write_url = f"{API_BASE.rstrip('/')}/api/inspections-write.php"
    payload = {
        "api_key": API_KEY,
        "state": "MN",
        "scraped_timestamp": datetime.now(timezone.utc).isoformat(),
        "facilities": all_facilities,
    }
    print(f"\nPOSTing to {write_url} ...")
    try:
        resp = req.post(write_url, json=payload, timeout=120, verify=False)
        print(f"  {resp.status_code}: {resp.text[:300]}")
    except Exception as e:
        print(f"  POST failed: {e}")


if __name__ == "__main__":
    asyncio.run(run())
