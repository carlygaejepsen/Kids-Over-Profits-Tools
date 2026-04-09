import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urljoin

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


BASE_DETAILS = "https://azcarecheck.azdhs.gov/s/facility-details?facilityId={fid}"

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for fid in facility_ids:
            print(f"🏥 {fid} → loading page...")
            await page.goto(BASE_DETAILS.format(fid=fid), wait_until="domcontentloaded")

            # Get facility name
            try:
                facility_name = (await page.text_content(
                    "lightning-formatted-text[c-azccfacilitydetailstab_hcifacilitydetails]"
                ) or fid).strip().replace(" ", "-")
            except:
                facility_name = fid

            # Click the paperclip icon
            try:
                await page.locator("lightning-icon[icon-name='utility:attach']").click(timeout=5000)
            except:
                print("   ⚠️ No attach icon found.")
                continue

            # Wait for modal to appear
            try:
                await page.wait_for_selector("section[role='dialog']", timeout=5000)
            except:
                print("   ⚠️ Modal didn’t appear.")
                continue

            # Grab all PDF links inside the modal
            pdf_links = await page.eval_on_selector_all(
                "section[role='dialog'] a[href$='.pdf']",
                "els => els.map(e => ({ href: e.href, label: e.textContent.trim() }))"
            )

            if not pdf_links:
                print("   ⚠️ No PDFs found.")
                continue

            for idx, file in enumerate(pdf_links, 1):
                pdf_url = urljoin(page.url, file["href"])
                label = file["label"] or f"file{idx}"
                safe_label = label.replace(" ", "-").replace("/", "-")
                file_name = f"{facility_name}_{safe_label}.pdf"

                print(f"   📄 {file_name}")
                pdf_bytes = await page.evaluate(
                    "url => fetch(url).then(res => res.arrayBuffer())", pdf_url
                )
                with open(file_name, "wb") as f:
                    f.write(bytearray(pdf_bytes))

            # Optional: close the modal
            try:
                await page.click("section[role='dialog'] button[title='Close']")
            except:
                pass

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())