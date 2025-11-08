from playwright.sync_api import sync_playwright
import os, re

URL = "https://mt-reports.com/portal/FacilityDetails.aspx"

# List of license types to download
# Add or remove license type IDs as needed
LICENSE_TYPES = [
    "3756",  # Private Alternative Adolescent Residential or Outdoor Program
    # Add more license type IDs here as needed
    # Example: "1234", "5678", etc.
]

def process_license_type(page, license_type, first_load=False):
    """Process all facilities for a given license type"""
    print(f"\n{'='*60}")
    print(f"Processing License Type: {license_type}")
    print(f"{'='*60}\n")

    # Navigate to the page
    page.goto(URL, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    # Select license type
    dropdown = page.locator("#MainContent_ddlLicenseType")
    dropdown.wait_for(state="visible", timeout=10000)
    dropdown.select_option(license_type)
    print(f"Selected license type {license_type}")

    # Handle reCAPTCHA only on first load
    if first_load:
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

    # Click Search button (it's a link, not a button)
    search_btn = page.locator("#MainContent_lbSearch")
    search_btn.wait_for(state="visible", timeout=10000)
    search_btn.click()
    print("Clicked search button")

    # Wait for results table
    print("Waiting for results to load...")
    results = page.locator("#iTable")
    try:
        results.wait_for(state="visible", timeout=60000)
        print("Results table loaded successfully!")
    except Exception as e:
        # Take a screenshot to see what's on the page
        page.screenshot(path=f"error_no_results_{license_type}.png")
        print(f"ERROR: Results table did not appear. Screenshot saved to error_no_results_{license_type}.png")
        print("This usually means reCAPTCHA was not solved or search failed.")
        print("Skipping this license type...")
        return

    # Count facilities
    facility_links = page.locator("a[id*='lbOrganizationName']")
    count = facility_links.count()
    print(f"Found {count} facilities")

    if count == 0:
        print("No facilities found for this license type, skipping...")
        return

    # Loop through facilities
    for i in range(count):
        print(f"\nFacility {i+1}/{count}:")

        # If not the first iteration, go back to search results
        if i > 0:
            print("  Going back to search results...")
            page.go_back(wait_until="networkidle", timeout=60000)
            page.wait_for_load_state("domcontentloaded")
            # Re-fetch facility links after navigation
            facility_links = page.locator("a[id*='lbOrganizationName']")

        name = facility_links.nth(i).inner_text().strip()
        print(f"  Name: {name}")

        # click into facility detail and wait for navigation
        print("  Clicking facility link...")
        with page.expect_navigation(wait_until="networkidle", timeout=60000):
            facility_links.nth(i).click()

        print("  Navigated to facility page, waiting for surveys...")

        # Wait for page to fully load
        page.wait_for_load_state("domcontentloaded")

        # Wait for surveys container - try multiple selectors
        surveys_found = False
        possible_selectors = ["#MainContent_repSurveys", "[id*='repSurveys']", ".survey", "[id*='Survey']"]

        for selector in possible_selectors:
            try:
                container = page.locator(selector).first
                if container.count() > 0:
                    surveys_found = True
                    print(f"  Found surveys with selector: {selector}")
                    break
            except:
                continue

        if not surveys_found:
            print("  WARNING: No survey container found - this facility may not have surveys")
            print(f"  Skipping...")
            continue

        # Grab survey links
        survey_links = page.locator("a[onclick*='SurveyGenerator']")
        survey_count = survey_links.count()
        print(f"  Found {survey_count} surveys")

        if survey_count == 0:
            print("  No surveys found for this facility, skipping...")
            continue

        for j in range(survey_count):
            onclick = survey_links.nth(j).get_attribute("onclick")
            match = re.search(r"SurveyGenerator\('(\d+)'\)", onclick)
            if not match:
                print(f"    Skipping survey {j+1} - no ID found")
                continue
            sid = match.group(1)

            # Download PDF
            try:
                with page.expect_download(timeout=30000) as dl_info:
                    survey_links.nth(j).click()
                download = dl_info.value
                # Sanitize filename - remove/replace problematic characters
                safe_name = re.sub(r'[<>:"/\\|?*]', '_', name)
                filename = f"{license_type}_{safe_name}_{sid}.pdf"
                filepath = os.path.join("downloads", filename)
                download.save_as(filepath)
                print(f"    [OK] Saved survey {j+1}/{survey_count}: {filename}")
            except Exception as e:
                print(f"    [FAIL] Failed to download survey {sid}: {e}")


def run():
    with sync_playwright() as p:
        # Launch browser with args to appear more like a real user
        browser = p.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )

        # Create context with realistic user agent and other settings
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )

        page = context.new_page()

        # Add script to remove webdriver flag
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        os.makedirs("downloads", exist_ok=True)

        # Process each license type
        for idx, license_type in enumerate(LICENSE_TYPES):
            try:
                process_license_type(page, license_type, first_load=(idx == 0))
            except Exception as e:
                print(f"\n[ERROR] Failed to process license type {license_type}: {e}")
                print("Continuing to next license type...")
                continue

        print("\n" + "="*60)
        print("Download complete! Check the 'downloads' folder for PDFs.")
        print("="*60)
        context.close()
        browser.close()

if __name__ == "__main__":
    run()
