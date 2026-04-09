import asyncio
import csv
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright

facility_ids = [
    "001cs00000WoDzyAAF",
    "001cs00000WoDzkAAF",
    "001cs00000WoAt6AAF",
    "001cs00000WoCLPAA3",
    "001cs00000WoBr1AAF",
    "001cs00000Wo8OtAAJ",
    "001cs00000Wo8bUAAR",
    "001cs00000Wo8OrAAJ",
    "001cs00000WoBr0AAF",
    "001cs00000WoAt9AAF",
    "001cs00000WoDSsAAN",
    "001cs00000WoAt8AAF",
    "001cs00000Wo4m9AAB",
    "001cs00000Wo9AKAAZ",
    "001cs00000Wo86wAAB",
    "001cs00000Wo9X1AAJ",
    "001cs00000Wo6GAAAZ",
    "001cs00000WoC6MAAV",
    "0018y000008fB0sAAE",
    "001cs00000WoC6NAAV",
    "001cs00000WoB37AAF",
    "001cs00000Wo9WaAAJ",
    "001cs00000WoGW9AAN",
    "001cs00000WoFldAAF",
    "001cs00000Wo6bUAAR",
    "001cs00000WoB38AAF",
    "001cs00000Wo540AAB",
    "001cs00000WnyTQAAZ",
    "001cs00000WnyTRAAZ",
    "001cs00000WoGWAAA3",
    "001cs00000WnyTeAAJ",
    "001cs00000WnyTfAAJ",
    "001cs00000WnyTgAAJ",
    "001cs00000WoEFPAA3",
    "001cs00000WoEFQAA3",
    "001cs00000WoEFLAA3",
    "001cs00000WoEFMAA3",
    "001cs00000WoEFOAA3",
    "001cs00000WnyTSAAZ",
    "001cs00000WoEFRAA3",
    "001cs00000WnyThAAJ",
    "001cs00000WnyTiAAJ",
    "001cs00000WnyTjAAJ",
    "001cs00000Wo8dxAAB",
    "001cs00000WoCXnAAN",
    "001cs00000WoCXoAAN",
    "001cs00000Wo5XmAAJ",
    "001cs00000Wo8dyAAB",
    "001cs00000Wo8wBAAR",
    "001cs00000WoGguAAF",
    "001cs00000WoCXcAAN",
    "001cs00000WoCXdAAN",
    "001cs00000Wo8e0AAB",
    "001cs00000WnyTaAAJ",
    "001cs00000WoCXeAAN",
    "001cs00000WnyTdAAJ",
    "001cs00000WoC3jAAF",
    "001cs00000WnyTbAAJ",
    "001cs00000WoCXfAAN",
    "001cs00000WoBcdAAF",
    "001cs00000WoCJYAA3",
    "001cs00000WoFmHAAV",
    "001cs00000WoEiXAAV",
    "001cs00000WoCJoAAN",
    "001cs00000WoESjAAN",
    "001cs00000Wo6s4AAB",
    "001cs00000Wo87nAAB",
    "001cs00000WoEyqAAF",
    "001cs00000Wo8dzAAB",
    "001cs00000WoCXgAAN",
    "001cs00000WoDTqAAN",
    "001cs00000WoGgvAAF",
    "001cs00000WoCXhAAN",
    "001cs00000Wo6bWAAR",
    "001cs00000WoCXiAAN",
    "001cs00000Wo8w9AAB",
    "001cs00000Wo5bxAAB",
    "001cs00000Wo5H5AAJ",
    "001cs00000WoCJZAA3",
    "001cs00000Wo9rKAAR",
    "001cs00000WoCXjAAN",
    "001cs00000Wo8OoAAJ",
    "001cs00000Wo9X0AAJ",
    "001cs00000Wo8dwAAB",
    "001cs00000Wo9pkAAB",
    "001cs00000Wo4VOAAZ",
    "0018y000008f8SeAAI",
    "001cs00000WoCXlAAN",
    "001cs00000WoEyOAAV",
    "001cs00000WoEDuAAN",
    "001cs00000Wo7FjAAJ",
    "001cs00000WoBceAAF",
    "001cs00000Wo6YRAAZ",
    "001cs00000WoFnjAAF",
    "001cs00000WoB3CAAV",
    "001cs00000Wo7UCAAZ",
    "001cs00000Wo6YSAAZ",
    "001cs00000WoB0lAAF",
    "001cs00000Wo9qpAAB",
    "001cs00000Wo8wAAAR",
    "001cs00000WoCXRAA3",
    "001cs00000WoC3kAAF",
    "001cs00000Wo6s5AAB",
    "001cs00000WoEEaAAN",
    "001cs00000WoFaiAAF",
    "001cs00000WoB0kAAF",
    "001cs00000WoATWAA3",
    "001cs00000WoB3DAAV",
    "001cs00000WoCXmAAN",
    "001cs00000WoAtZAAV",
    "001cs00000Wo8vFAAR",
    "001cs00000WoDChAAN",
    "001cs00000WoCKBAA3",
    "001cs00000WoC6KAAV",
    "001cs00000WoB35AAF",
    "001cs00000WoFnyAAF",
    "001cs00000WoB2VAAV",
    "001cs00000WoGexAAF"
]

output_file = "inspection_ids.csv"

async def scrape_inspection_ids():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        with open(output_file, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["FacilityId", "InspectionId"])

            for fid in facility_ids:
                print(f"🏥 {fid}", flush=True)
                url = f"https://azcarecheck.azdhs.gov/s/facility-details?facilityId={fid}&activeTab=Inspections"
                await page.goto(url, wait_until="networkidle")

                try:
                    await page.wait_for_selector("table tbody tr", timeout=5000)
                    rows = await page.query_selector_all("table tbody tr")

                    for row in rows:
                        link_el = await row.query_selector("a[href]")
                        if link_el:
                            href = await link_el.get_attribute("href")
                            if href:
                                qs = parse_qs(urlparse(href).query)
                                insp_id = qs.get("inspectionId", [""])[0]
                                if insp_id:
                                    print(f"  → {insp_id}", flush=True)
                                    writer.writerow([fid, insp_id])

                except:
                    print("  NO INSPECTIONS FOUND", flush=True)

        await browser.close()

    print(f"\n✅ Done. Saved to {output_file}")

if __name__ == "__main__":
    asyncio.run(scrape_inspection_ids())