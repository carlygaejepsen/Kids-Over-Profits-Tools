from playwright.sync_api import sync_playwright
import re
import time
import os
import shutil

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

def run():
    # Hardcoded path to your Downloads folder
    downloads_path = r"C:\Users\daniu\Downloads"
    
    with sync_playwright() as p:
        # Configure browser to download to a specific directory
        browser = p.chromium.launch(
            headless=False,
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
                print("✔ Search page loaded")
                
                # 2. Select radio button - scroll to it first
                print("Step 2: Selecting care type...")
                radio_button = page.get_by_role("radio", name=re.compile(r"24[\s\-]?hr.*Residential", re.I))
                radio_button.scroll_into_view_if_needed()
                time.sleep(1)
                radio_button.click(force=True)
                time.sleep(3)
                print("✔ Care type selected")
                
                # 3. Click "By Provider" button - scroll to it first
                print("Step 3: Clicking By Provider...")
                by_provider_button = page.locator("button#by-provider")
                by_provider_button.scroll_into_view_if_needed()
                time.sleep(1)
                by_provider_button.click(force=True)
                time.sleep(3)
                print("✔ By Provider clicked")
                
                # 4. Fill operation number
                print(f"Step 4: Entering operation ID {op_id}...")
                operation_input = page.locator("input[placeholder*='Operation Number']")
                operation_input.scroll_into_view_if_needed()
                time.sleep(1)
                operation_input.fill(op_id)
                time.sleep(2)
                print(f"✔ Entered: {op_id}")
                
                # 5. Click search button - scroll to it first
                print("Step 5: Clicking search...")
                by_pro_search = page.locator("button.by-pro-search")
                by_pro_search.scroll_into_view_if_needed()
                time.sleep(1)
                by_pro_search.click(force=True)
                time.sleep(10)  # Increased wait time for search
                print("✔ Search completed")
                
                # 6. Look for "More Details/Compliance History" button with better error handling
                print("Step 6: Looking for More Details button...")
                print(f"Current URL: {page.url}")
                
                # Try multiple possible selectors for the More Details button
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
                            print(f"Found button with selector: {selector}")
                            break
                    except:
                        continue
                
                if more_details_button is None:
                    print("❌ More Details button not found with any selector!")
                    continue
                
                more_details_button.scroll_into_view_if_needed()
                time.sleep(2)
                more_details_button.click(force=True)
                time.sleep(5)
                print("✔ Clicked 'More Details/Compliance History'")
                
                # 7. Click compliance history - scroll to it first
                print("Step 7: Looking for View Full Compliance History...")
                compliance_button = page.locator("div.ux-btn-label-wrapper:has-text('View Full Compliance History')")
                
                if compliance_button.count() == 0:
                    print("❌ Compliance History button not found!")
                    continue
                
                compliance_button.scroll_into_view_if_needed()
                time.sleep(1)
                compliance_button.click(force=True)
                time.sleep(5)
                print("✔ Clicked compliance history")
                
                # 8. Click Deficiencies tab - scroll to it first
                print("Step 8: Looking for Deficiencies tab...")
                deficiencies_tab = page.locator("li.react-tabs__tab:has-text('Deficiencies')")
                
                if deficiencies_tab.count() == 0:
                    print("❌ Deficiencies tab not found!")
                    continue
                
                deficiencies_tab.scroll_into_view_if_needed()
                time.sleep(1)
                deficiencies_tab.click(force=True)
                time.sleep(5)
                print("✔ Clicked Deficiencies tab")
                
                # 9. Click download dropdown - scroll to it first
                print("Step 9: Looking for download dropdown...")
                download_dropdown = page.locator("div.Multi-Select__control:has-text('Download Deficiencies')")
                
                if download_dropdown.count() == 0:
                    print("❌ Download dropdown not found!")
                    continue
                
                download_dropdown.scroll_into_view_if_needed()
                time.sleep(1)
                download_dropdown.click(force=True)
                time.sleep(2)
                print("✔ Download dropdown clicked")
                
                # 10. Select CSV option and handle download
                print("Step 10: Selecting CSV option...")
                with page.expect_download(timeout=30000) as download_info:
                    csv_option = page.locator("div[role='option']").nth(1)
                    csv_option.scroll_into_view_if_needed()
                    time.sleep(1)
                    csv_option.click(force=True)
                    print("✔ CSV option clicked, waiting for download...")
                    
                download = download_info.value
                
                # Get the original download path
                original_path = download.path()
                print(f"Original download path: {original_path}")
                
                # Copy to your downloads folder with the desired name
                final_path = os.path.join(downloads_path, f"{op_id}.csv")
                shutil.copy2(original_path, final_path)
                
                # Verify the file was created
                if os.path.exists(final_path):
                    print(f"✅ Successfully copied to: {final_path}")
                    file_size = os.path.getsize(final_path)
                    print(f"File size: {file_size} bytes")
                else:
                    print(f"❌ Failed to copy file to: {final_path}")
                    
            except Exception as e:
                print(f"❌ ERROR processing {op_id}: {str(e)}")
                print(f"Current URL: {page.url}")
                print("Continuing to next operation ID...")
                continue
            
        browser.close()

if __name__ == "__main__":
    run()