import asyncio
from playwright.async_api import async_playwright

inspections = [
("001cs00000WoDzyAAF", "a1Ics00000JYaxNEAT"),
("001cs00000WoDzyAAF", "a1Ics00000K9I0bEAF"),
("001cs00000WoDzyAAF", "a1Ics00000G5WG4EAN"),
("001cs00000WoDzkAAF", "a1Ics00000JZ6cIEAT"),
("001cs00000WoDzkAAF", "a1Ics00000KK4WzEAL"),
("001cs00000WoDzkAAF", "a1Ics00000G5WFiEAN"),
("001cs00000WoAt6AAF", "a1Ics00000I3XUfEAN"),
("001cs00000WoCLPAA3", "a1Ics00000G5VAQEA3"),
("001cs00000WoCLPAA3", "a1Ics00000G5VAPEA3"),
("001cs00000WoBr1AAF", "a1Ics00000NrcQ6EAJ"),
("001cs00000WoBr1AAF", "a1Ics00000NrcbUEAR"),
("001cs00000WoBr1AAF", "a1Ics00000G5ULBEA3"),
("001cs00000Wo8OtAAJ", "a1Ics00000G7SxdEAF"),
("001cs00000Wo8OtAAJ", "a1Ics00000G5ZtgEAF"),

]

BASE_URL = "https://azcarecheck.azdhs.gov/s/inspection-print-view"

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1280, "height": 2000})

        name_tracker = {}  # track how many times each facility name is used

        for fac_id, insp_id in inspections:
            url = f"{BASE_URL}?facilityId={fac_id}&inspectionId={insp_id}"
            print(f"🖨️ Printing {fac_id} / {insp_id}")
            await page.goto(url, wait_until="domcontentloaded")

            # Ensure dynamic content has rendered
            try:
                await page.get_by_text("Inspection Date", exact=False).first.wait_for(timeout=30000)
            except:
                await page.wait_for_timeout(3000)

            await page.emulate_media(media="screen")

            # Scrape facility name
            try:
                facility_name = (await page.text_content(
                    "lightning-formatted-text[c-azccfacilitydetailstab_hcifacilitydetails]"
                ) or "Facility").strip()
            except:
                facility_name = "Facility"

            # Make a safe filename: replace spaces/slashes
            safe_fac = facility_name.replace(" ", "-")

            # Numbering for duplicates
            count = name_tracker.get(safe_fac, 0)
            pdf_name = f"{safe_fac}_report.pdf" if count == 0 else f"{safe_fac}_report_{count}.pdf"
            name_tracker[safe_fac] = count + 1

            await page.pdf(
                path=pdf_name,
                format="A4",
                print_background=True,
                prefer_css_page_size=True
            )
            print(f"   → Saved {pdf_name}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())