from playwright.sync_api import sync_playwright
import os, re

URL = "https://mt-reports.com/portal/FacilityDetails.aspx"

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # flip to True once stable
        page = browser.new_page()
        os.makedirs("downloads", exist_ok=True)

        # 1. Load the page
        page.goto(URL)

        # 2. Select license type = 3756 (Private Alternative Adolescent Residential or Outdoor Program)
        page.select_option("#MainContent_ddlLicenseType", "3756")

        # 3. Click Search
        page.click("#MainContent_btnSearch")
        page.locator("#iTable").wait_for()  # wait for results table

        # 4. Count facilities
        facility_links = page.locator("a[id*='lbOrganizationName']")
        count = facility_links.count()
        print(f"Found {count} facilities")

        # 5. Loop through facilities
        for i in range(count):
            # reload search results each loop (ASP.NET redraws DOM)
            page.goto(URL)
            page.select_option("#MainContent_ddlLicenseType", "3756")
            page.click("#MainContent_btnSearch")
            page.locator("#iTable").wait_for()
            facility_links = page.locator("a[id*='lbOrganizationName']")

            name = facility_links.nth(i).inner_text().strip()
            print(f"\nFacility: {name}")

            # click into facility detail
            facility_links.nth(i).click()
            page.locator("#MainContent_repSurveys").wait_for()

            # 6. Grab survey links
            survey_links = page.locator("a[onclick*='SurveyGenerator']")
            for j in range(survey_links.count()):
                onclick = survey_links.nth(j).get_attribute("onclick")
                sid = re.search(r"SurveyGenerator\('(\d+)'\)", onclick).group(1)

                # 7. Download PDF
                with page.expect_download() as dl_info:
                    survey_links.nth(j).click()
                download = dl_info.value
                filename = f"{name}_{sid}.pdf".replace(" ", "_")
                download.save_as(os.path.join("downloads", filename))
                print(f"Saved {filename}")

        browser.close()

if __name__ == "__main__":
    run()