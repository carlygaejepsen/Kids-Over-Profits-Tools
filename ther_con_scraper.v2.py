from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import time

BASE = "https://www.therapeuticconsulting.org"
START = f"{BASE}/find-a-referring-pro"

OUTFILE = "profiles.csv"
HEADLESS = True         # set to False if you want to watch it render
PAUSE_SEC = 0.3         # polite crawl delay between pages

FIELDNAMES = [
    "name", "credentials", "company", "email", "website",
    "city", "state", "primary_specialty", "secondary_specialty",
    "membership_level", "profile_url"
]

def parse_directory_html(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tbody tr[role='row']")
    return rows

def extract_directory_row(tr):
    name_a = tr.select_one("td[data-label='Name'] a")
    name = name_a.get_text(strip=True) if name_a else ""
    profile_url = urljoin(BASE, name_a["href"]) if name_a and name_a.has_attr("href") else ""

    def cell(label):
        el = tr.select_one(f"td[data-label='{label}']")
        return el.get_text(strip=True) if el else ""

    return {
        "name": name,
        "city": cell("City"),
        "state": cell("State"),
        "primary_specialty": cell("Primary Specialty"),
        "secondary_specialty": cell("Secondary Specialty"),
        "membership_level": cell("Membership Level"),
        "profile_url": profile_url,
    }

def extract_profile_fields(html):
    soup = BeautifulSoup(html, "html.parser")

    # Strict positional mapping per your spec: [name, credentials, company, email, website]
    ps = [p.get_text(strip=True) for p in soup.select(".sqs-block-content p") if p.get_text(strip=True)]

    credentials = ps[1] if len(ps) > 1 else ""
    company     = ps[2] if len(ps) > 2 else ""

    email = ""
    website = ""
    for a in soup.select(".sqs-block-content a[href]"):
        href = a["href"].strip()
        if href.startswith("mailto:"):
            email = href[len("mailto:"):].rstrip("?")
        elif href.startswith("http"):
            website = href

    return {
        "credentials": credentials,
        "company": company,
        "email": email,
        "website": website,
    }

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        # Load directory and wait for table rows to appear
        page.goto(START, wait_until="domcontentloaded")
        page.wait_for_selector("tbody tr[role='row']", timeout=15000)

        directory_html = page.content()
        trs = parse_directory_html(directory_html)
        if not trs:
            raise RuntimeError("No directory rows found after JS render.")

        with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()

            for tr in trs:
                base = extract_directory_row(tr)
                if not base["profile_url"]:
                    # Write what we have; leave profile fields blank
                    writer.writerow({**base, **{"credentials":"", "company":"", "email":"", "website":""}})
                    continue

                # Visit profile, wait for content block, extract details
                page.goto(base["profile_url"], wait_until="domcontentloaded")
                # Some profiles are light; wait on any content block or fall through
                try:
                    page.wait_for_selector(".sqs-block-content", timeout=10000)
                except:
                    pass

                profile_html = page.content()
                profile = extract_profile_fields(profile_html)

                writer.writerow({**base, **profile})
                time.sleep(PAUSE_SEC)

        browser.close()
        print(f"Wrote {OUTFILE}")

if __name__ == "__main__":
    main()