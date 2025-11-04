from playwright.sync_api import sync_playwright
import os, re

URL = "https://mt-reports.com/portal/FacilityDetails.aspx"

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # flip to True once stable
        page = browser.new_page()
        os.makedirs("downloads", exist_ok=True)

        # 1. Load the page
        page.goto(URL, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")

        # 2. Select license type = 3756 (Private Alternative Adolescent Residential or Outdoor Program)
        dropdown = page.locator("#MainContent_ddlLicenseType")
        dropdown.wait_for(state="visible", timeout=10000)
        dropdown.select_option("3756")
        print("Selected license type 3756")

        # 4. Handle reCAPTCHA if present (manual intervention needed)
        # Check if reCAPTCHA frame exists
        recaptcha_solved = False
        try:
            recaptcha_frame = page.frame_locator("iframe[title*='reCAPTCHA']")
            recaptcha_checkbox = recaptcha_frame.locator(".recaptcha-checkbox-border")
            if recaptcha_checkbox.count() > 0:
                print("\n" + "="*60)
                print("reCAPTCHA DETECTED!")
                print("Please solve the reCAPTCHA in the browser window.")
                print("The script will wait up to 60 seconds...")
                print("="*60 + "\n")

                # Wait for user to solve reCAPTCHA - check every 2 seconds
                for i in range(30):  # 60 seconds total
                    try:
                        # Check if checkbox is checked
                        if page.locator("iframe[title*='reCAPTCHA']").count() == 0:
                            # reCAPTCHA might have disappeared after solving
                            recaptcha_solved = True
                            break
                        page.wait_for_timeout(2000)
                        print(f"Waiting for reCAPTCHA... ({i*2} seconds)")
                    except:
                        pass

                if recaptcha_solved:
                    print("reCAPTCHA appears to be solved!")
                else:
                    print("Proceeding anyway - reCAPTCHA may still need solving")
        except Exception as e:
            print(f"reCAPTCHA check error (may not be present): {e}")

        # 5. Click Search button (it's a link, not a button)
        search_btn = page.locator("#MainContent_lbSearch")
        search_btn.wait_for(state="visible", timeout=10000)
        search_btn.click()
        print("Clicked search button")

        # 6. Wait for results table
        print("Waiting for results to load...")
        results = page.locator("#iTable")
        try:
            results.wait_for(state="visible", timeout=60000)
            print("Results table loaded successfully!")
        except Exception as e:
            # Take a screenshot to see what's on the page
            page.screenshot(path="error_no_results.png")
            print("ERROR: Results table did not appear. Screenshot saved to error_no_results.png")
            print("This usually means reCAPTCHA was not solved or search failed.")
            raise

        # 4. Count facilities
        facility_links = page.locator("a[id*='lbOrganizationName']")
        count = facility_links.count()
        print(f"Found {count} facilities")

        # 6. Loop through facilities
        for i in range(count):
            # reload search results each loop (ASP.NET redraws DOM)
            page.goto(URL, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")

            dropdown = page.locator("#MainContent_ddlLicenseType")
            dropdown.wait_for(state="visible", timeout=10000)
            dropdown.select_option("3756")

            search_btn = page.locator("#MainContent_lbSearch")
            search_btn.wait_for(state="visible", timeout=10000)
            search_btn.click()

            results = page.locator("#iTable")
            results.wait_for(state="visible", timeout=60000)
            facility_links = page.locator("a[id*='lbOrganizationName']")

            name = facility_links.nth(i).inner_text().strip()
            print(f"\nFacility: {name}")

            # click into facility detail
            facility_links.nth(i).click()
            surveys_container = page.locator("#MainContent_repSurveys")
            surveys_container.wait_for(state="visible", timeout=60000)

            # 7. Grab survey links
            survey_links = page.locator("a[onclick*='SurveyGenerator']")
            survey_count = survey_links.count()
            print(f"Found {survey_count} surveys")

            for j in range(survey_count):
                onclick = survey_links.nth(j).get_attribute("onclick")
                match = re.search(r"SurveyGenerator\('(\d+)'\)", onclick)
                if not match:
                    print(f"Skipping survey {j} - no ID found")
                    continue
                sid = match.group(1)

                # 8. Download PDF
                try:
                    with page.expect_download(timeout=30000) as dl_info:
                        survey_links.nth(j).click()
                    download = dl_info.value
                    filename = f"{name}_{sid}.pdf".replace(" ", "_")
                    download.save_as(os.path.join("downloads", filename))
                    print(f"Saved {filename}")
                except Exception as e:
                    print(f"Failed to download survey {sid}: {e}")

        browser.close()

if __name__ == "__main__":
    run()