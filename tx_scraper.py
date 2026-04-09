from playwright.sync_api import sync_playwright
import csv
import io
import re
import time
import os
import shutil

from inspection_api_client import post_facilities_to_api

# Replace with your operation IDs
OPERATION_IDS = [
"1773",
"5541",
"6065",
"254646",
"516810",
"522278",
"827024",
"1247466",
"1409086",
"1540737",
"1562665",
"1659892",
"1668161",
"1700503",
"1714518",
"1724502",
"1762810",
"1769798",
"1782716",
"853346",
"1509926",
"204641",
"215752",
"1693667",
"1693740",
"531665",
"1711621",
"1145426",
"1773300",
"817874",
"1765540",
"1738882",
"503875",
"1784559",
"1786879",
"1791476",
"1800159",
"1807875",
"1808042",
"1818188",
"1803258",
"1809887",
"1812895",
"1814618",
"290",
"5913",
"6005",
"15267",
"27177",
"42721",
"185528",
"191133",
"245627",
"541473",
"846369",
"858505",
"859225",
"1033546",
"1105326",
"1559917",
"1692813",
"1708504",
"1715609",
"1760975",
"1761134",
"1763000",
"228620",
"1693629",
"1533621",
"1721847",
"1774248",
"823154",
"1735762",
"838067",
"839957",
"511519",
"1757679",
"1784695",
"1797723",
"1799271",
"1796998",
"1797609",
"1797707",
"1803478",
"1813025",
"1165",
"5599",
"6040",
"6787",
"13621",
"23871",
"54326",
"154645",
"844802",
"847985",
"849411",
"1063806",
"1244326",
"1667912",
"1684228",
"1689854",
"1724162",
"1743981",
"1769797",
"1776121",
"1698799",
"1705965",
"1706626",
"1693628",
"1530337",
"1681396",
"892238",
"1773996",
"556189",
"1752752",
"1746008",
"1738202",
"505173",
"1760485",
"1797726",
"1813303",
"1811815",
"1819452",
"1805359",
"1813027",
"1806666",
"1806667",
"5761",
"6085",
"24719",
"50218",
"170567",
"254110",
"254538",
"254729",
"256371",
"812341",
"1658979",
"1678450",
"1689755",
"1692025",
"1701291",
"1714813",
"1761811",
"1767744",
"66232",
"851881",
"852537",
"1672415",
"1704326",
"1726200",
"1000050",
"1663308",
"1717490",
"1773956",
"817512",
"1687429",
"1763576",
"1736601",
"1784246",
"1786371",
"1786553",
"1786589",
"1797720",
"1797733",
"1795126",
"1797648",
"1808575",
"1815891",
"1803139",
"1803261",
"1803477",
"1809921",
"1545",
"5914",
"7184",
"18027",
"36943",
"244459",
"849589",
"877478",
"1632090",
"1658373",
"1675992",
"1690684",
"1701592",
"1763004",
"1763171",
"1769316",
"69531",
"1699024",
"1700373",
"1506800",
"1710607",
"1572157",
"1779839",
"1780336",
"1764006",
"1752267",
"1753012",
"1746886",
"1760486",
"1787751",
"1786312",
"1790926",
"1791182",
"1793529",
"1793844",
"1798274",
"1795441",
"1808412",
"1808635",
"1803493",
"1805546",
"1814507",
"184",
"360",
"1639",
"5570",
"178491",
"181054",
"251707",
"813238",
"1268266",
"1496362",
"1502634",
"1594840",
"1701205",
"1714475",
"1715401",
"1723466",
"1724559",
"1734404",
"1741334",
"1744038",
"1744228",
"1754771",
"1768076",
"855318",
"1704643",
"1706226",
"1731240",
"204485",
"1695157",
"1681475",
"980590",
"1000466",
"1721455",
"1721595",
"1617305",
"1716949",
"1764644",
"840381",
"1788833",
"1789393",
"1808915",
"1811997",
"1810414",
"1804296",
"1804766",
"1806078",
"1800545",
"1806655",
"1535",
"2043",
"40724",
"53712",
"517689",
"540238",
"848614",
"860617",
"860873",
"888971",
"1709632",
"1724200",
"1741914",
"1756205",
"1762558",
"66554",
"68824",
"1729764",
"1508898",
"1722378",
"1716984",
"554314",
"1752756",
"1759648",
"1792300",
"1794219",
"1795211",
"1796056",
"1811084",
"1811154",
"1810727",
"1807650",
"1808079",
"1805111",
"1812028",
"1812174",
"1802135",
"1802552",
"34018",
"255369",
"255675",
"515678",
"538034",
"827335",
"844396",
"1064466",
"1665832",
"1666134",
"1684373",
"1692091",
"1719866",
"1750110",
"1770328",
"1729779",
"1732146",
"224541",
"1680842",
"1681260",
"975248",
"1663041",
"1720357",
"1720367",
"1721433",
"1722710",
"1716988",
"1764642",
"1746293",
"836843",
"1788783",
"1785648",
"1808814",
"1810048",
"1810057",
"1808031",
"1805676",
"1801598",
"1812637",
"1806668",
"1806675",
"1807081",
"1802456",
]

API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = "CHANGE_ME"  # Must match the key in your PHP endpoint


def parse_csv_to_citations(csv_path):
    """Parse a downloaded deficiency CSV into a list of citation dicts."""
    citations = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                citations.append({k: (v or "").strip() for k, v in row.items()})
    except Exception as e:
        print(f"  Error parsing CSV {csv_path}: {e}")
    return citations


def scrape_facility_info(page):
    """Extract facility details from the search-result / details page."""
    info = {}
    try:
        # The facility name usually appears as a heading on the details page
        name_el = page.locator("h1, h2, .operation-name, .facility-name").first
        if name_el.count() > 0:
            info["facility_name"] = name_el.inner_text().strip()

        # Try to grab the info table/fields visible on the details page
        field_map = {
            "Operation #": "operation_num",
            "Type": "program_category",
            "Address": "full_address",
            "City": "city",
            "County": "county",
            "Phone": "phone",
            "Capacity": "bed_capacity",
            "Ages Served": "ages_served",
            "Status": "action",
            "Issue Date": "license_exp_date",
        }
        for label, key in field_map.items():
            try:
                el = page.locator(f"text='{label}' >> xpath=../following-sibling::*[1]").first
                if el.count() > 0:
                    info[key] = el.inner_text().strip()
            except Exception:
                pass
    except Exception as e:
        print(f"  Warning: could not scrape facility info: {e}")
    return info


def save_to_api(facilities):
    """POST all collected facility data to the live site."""
    from datetime import datetime

    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="TX",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=120,
        info=print,
        error=print,
    )
    return bool(result.get("success"))


def run():
    downloads_path = r"C:\Users\daniu\Downloads"
    all_facilities = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            downloads_path=downloads_path
        )
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for i, op_id in enumerate(OPERATION_IDS):
            try:
                print(f"\n=== Processing {op_id} ({i+1}/{len(OPERATION_IDS)}) ===")

                # 1. Go to search page and wait
                print("Step 1: Loading search page...")
                page.goto("https://childcare.hhs.texas.gov/Public/ChildCareSearch")
                time.sleep(5)

                # 2. Select radio button
                radio_button = page.get_by_role("radio", name=re.compile(r"24[\s\-]?hr.*Residential", re.I))
                radio_button.scroll_into_view_if_needed()
                time.sleep(1)
                radio_button.click(force=True)
                time.sleep(3)

                # 3. Click "By Provider"
                by_provider_button = page.locator("button#by-provider")
                by_provider_button.scroll_into_view_if_needed()
                time.sleep(1)
                by_provider_button.click(force=True)
                time.sleep(3)

                # 4. Fill operation number
                operation_input = page.locator("input[placeholder*='Operation Number']")
                operation_input.scroll_into_view_if_needed()
                time.sleep(1)
                operation_input.fill(op_id)
                time.sleep(2)

                # 5. Click search
                by_pro_search = page.locator("button.by-pro-search")
                by_pro_search.scroll_into_view_if_needed()
                time.sleep(1)
                by_pro_search.click(force=True)
                time.sleep(10)

                # 6. Click "More Details/Compliance History"
                more_details_selectors = [
                    "div.ux-btn-label-wrapper:has-text('More Details/Compliance History')",
                    "button:has-text('More Details/Compliance History')",
                    ".filter-button:has-text('More Details')",
                    "button.filter-button"
                ]
                more_details_button = None
                for selector in more_details_selectors:
                    try:
                        button = page.locator(selector).first
                        if button.count() > 0:
                            more_details_button = button
                            break
                    except:
                        continue
                if more_details_button is None:
                    print("  More Details button not found, skipping")
                    continue
                more_details_button.scroll_into_view_if_needed()
                time.sleep(2)
                more_details_button.click(force=True)
                time.sleep(5)

                # --- Scrape facility info from the details page ---
                facility_info = scrape_facility_info(page)
                facility_info.setdefault("facility_name", f"Operation #{op_id}")
                facility_info["program_name"] = op_id  # use op_id as unique key

                # 7. Click compliance history
                compliance_button = page.locator("div.ux-btn-label-wrapper:has-text('View Full Compliance History')")
                if compliance_button.count() == 0:
                    print("  Compliance History button not found, skipping")
                    continue
                compliance_button.scroll_into_view_if_needed()
                time.sleep(1)
                compliance_button.click(force=True)
                time.sleep(5)

                # 8. Click Deficiencies tab
                deficiencies_tab = page.locator("li.react-tabs__tab:has-text('Deficiencies')")
                if deficiencies_tab.count() == 0:
                    print("  Deficiencies tab not found, skipping")
                    continue
                deficiencies_tab.scroll_into_view_if_needed()
                time.sleep(1)
                deficiencies_tab.click(force=True)
                time.sleep(5)

                # 9. Click download dropdown
                download_dropdown = None
                for locator in (
                    page.get_by_role("button", name=re.compile(r"Download Deficiencies", re.I)),
                    page.locator("div.Multi-Select__control:has-text('Download Deficiencies')")
                ):
                    if locator.count() > 0:
                        download_dropdown = locator.first
                        break
                if download_dropdown is None:
                    print("  Download dropdown not found, skipping")
                    continue
                download_dropdown.scroll_into_view_if_needed()
                time.sleep(1)
                download_dropdown.click(force=True)

                menu = page.locator("div.Multi-Select__menu").first
                try:
                    menu.wait_for(state="visible", timeout=5000)
                except Exception:
                    print("  Download menu did not appear, skipping")
                    continue

                # 10. Select CSV option and download
                csv_option = menu.locator("div[role='option']", has_text=re.compile(r"CSV", re.I)).first
                if csv_option.count() == 0:
                    csv_option = page.locator("div[role='option']", has_text=re.compile(r"CSV", re.I)).first
                if csv_option.count() == 0:
                    print("  CSV option not found, skipping")
                    continue

                with page.expect_download(timeout=60000) as download_info:
                    csv_option.scroll_into_view_if_needed()
                    time.sleep(1)
                    csv_option.click(force=True)

                download = download_info.value
                original_path = download.path()
                if not original_path:
                    print("  Download completed but no file path returned")
                    continue

                final_path = os.path.join(downloads_path, f"{op_id}.csv")
                shutil.copy2(original_path, final_path)

                # --- Parse CSV into citations and build facility record ---
                citations = parse_csv_to_citations(final_path)
                print(f"  Parsed {len(citations)} citations for {facility_info['facility_name']}")

                # Convert each citation row into a "report" for the shared schema
                reports = []
                for cit in citations:
                    report_id = f"{op_id}_{cit.get('Citation Date', '')}_{cit.get('Standard Number / Description', '')}"
                    # Make a unique-ish report_id from the citation fields
                    report_id = re.sub(r'[^a-zA-Z0-9_]', '', report_id)[:100]
                    reports.append({
                        "report_id": report_id,
                        "report_date": cit.get("Citation Date", ""),
                        "raw_content": cit.get("Deficiency Narrative", ""),
                        "content_length": len(cit.get("Deficiency Narrative", "")),
                        "summary": cit.get("Standard Number / Description", ""),
                        "categories": cit,  # store the full citation row as categories
                    })

                all_facilities.append({
                    "facility_info": facility_info,
                    "reports": reports,
                })

            except Exception as e:
                print(f"  ERROR processing {op_id}: {str(e)}")
                continue

        browser.close()

    # --- POST everything to the API ---
    if all_facilities:
        print(f"\nScraped {len(all_facilities)} facilities total")
        save_to_api(all_facilities)
    else:
        print("\nNo facilities scraped")


if __name__ == "__main__":
    run()
