import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import os
import re
import time

URL = "https://mt-reports.com/portal/FacilityDetails.aspx"

# List of license types to download
# Add or remove license type IDs as needed
LICENSE_TYPES = [
    "3756",  # Private Alternative Adolescent Residential or Outdoor Program
    # Add more license type IDs here as needed
    # Example: "1234", "5678", etc.
]

def process_license_type(driver, license_type, first_load=False):
    """Process all facilities for a given license type"""
    print(f"\n{'='*60}")
    print(f"Processing License Type: {license_type}")
    print(f"{'='*60}\n")

    # Only navigate to the page on first load
    if first_load:
        driver.get(URL)
        time.sleep(2)  # Give page time to load
    else:
        # For subsequent license types, navigate back to search form
        # Use back button to preserve session and avoid new reCAPTCHA
        print("Navigating back to search form...")
        for _ in range(10):  # Go back up to 10 times to get to search form
            try:
                # Check if we're already on the search page by looking for the dropdown
                driver.find_element(By.ID, "MainContent_ddlLicenseType")
                print("Already on search form")
                break
            except:
                # Not on search form yet, go back
                driver.back()
                time.sleep(1)

    # Select license type
    try:
        dropdown_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "MainContent_ddlLicenseType"))
        )
        dropdown = Select(dropdown_element)
        dropdown.select_by_value(license_type)
        print(f"Selected license type {license_type}")
        time.sleep(1)
    except Exception as e:
        print(f"ERROR: Could not select license type: {e}")
        return

    # Handle reCAPTCHA only on first load
    if first_load:
        print("\n" + "="*60)
        print("CHECKING FOR reCAPTCHA...")
        print("If you see a reCAPTCHA, please solve it manually.")
        print("The script will wait up to 2 minutes...")
        print("="*60 + "\n")

        # Wait a bit for reCAPTCHA to potentially appear
        time.sleep(3)

        # Check if reCAPTCHA iframe exists
        recaptcha_frames = driver.find_elements(By.XPATH, "//iframe[contains(@title, 'reCAPTCHA') or contains(@src, 'recaptcha')]")

        if recaptcha_frames:
            print(f"reCAPTCHA detected! ({len(recaptcha_frames)} frame(s) found)")
            print("Please solve it in the browser window.")
            print("Waiting for you to complete it...\n")

            # Wait up to 120 seconds for user to solve
            solved = False
            for i in range(60):
                time.sleep(2)
                # Check if reCAPTCHA is still present
                recaptcha_frames = driver.find_elements(By.XPATH, "//iframe[contains(@title, 'reCAPTCHA') or contains(@src, 'recaptcha')]")

                # Also check if the search button is now enabled/clickable
                try:
                    search_btn = driver.find_element(By.ID, "MainContent_lbSearch")
                    if search_btn.is_enabled():
                        print("✓ reCAPTCHA solved! Search button is now enabled.")
                        solved = True
                        break
                except:
                    pass

                if not recaptcha_frames:
                    print("✓ reCAPTCHA frames cleared!")
                    solved = True
                    break

                if i % 5 == 0 and i > 0:
                    print(f"  Still waiting... ({i*2} seconds elapsed)")

            if not solved:
                print("⚠ WARNING: Timeout waiting for reCAPTCHA. Attempting to continue anyway...")

            time.sleep(3)  # Extra time after solving to ensure page is ready
        else:
            print("No reCAPTCHA detected, proceeding...")

    # Click Search button
    try:
        search_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "MainContent_lbSearch"))
        )
        search_btn.click()
        print("Clicked search button")
        time.sleep(2)

        # Check if a new reCAPTCHA appeared after clicking search
        recaptcha_check = driver.find_elements(By.XPATH, "//iframe[contains(@title, 'reCAPTCHA') or contains(@src, 'recaptcha')]")
        if recaptcha_check and not first_load:
            print("\n⚠ WARNING: New reCAPTCHA appeared after clicking search!")
            print("This shouldn't happen. Please solve it manually.")
            print("Waiting for you to solve it...")
            for i in range(30):
                time.sleep(2)
                recaptcha_check = driver.find_elements(By.XPATH, "//iframe[contains(@title, 'reCAPTCHA') or contains(@src, 'recaptcha')]")
                if not recaptcha_check:
                    print("✓ reCAPTCHA solved!")
                    break
                if i % 5 == 0 and i > 0:
                    print(f"  Still waiting... ({i*2} seconds)")
            time.sleep(2)

    except Exception as e:
        print(f"ERROR: Could not click search button: {e}")
        return

    # Wait for results table
    print("Waiting for results to load...")
    try:
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.ID, "iTable"))
        )
        print("Results table loaded successfully!")
    except Exception as e:
        # Take a screenshot to see what's on the page
        driver.save_screenshot(f"error_no_results_{license_type}.png")
        print(f"ERROR: Results table did not appear. Screenshot saved to error_no_results_{license_type}.png")
        print("This usually means reCAPTCHA was not solved or search failed.")
        print("Skipping this license type...")
        return

    # Count facilities
    facility_links = driver.find_elements(By.XPATH, "//a[contains(@id, 'lbOrganizationName')]")
    count = len(facility_links)
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
            driver.back()
            time.sleep(2)

            # Wait for results table to reload
            try:
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.ID, "iTable"))
                )
            except:
                print("  ERROR: Could not reload results table")
                break

            # Re-fetch facility links after navigation
            facility_links = driver.find_elements(By.XPATH, "//a[contains(@id, 'lbOrganizationName')]")

        try:
            name = facility_links[i].text.strip()
            print(f"  Name: {name}")

            # Click into facility detail
            print("  Clicking facility link...")
            facility_links[i].click()
            time.sleep(3)  # Wait for navigation

            print("  Navigated to facility page, waiting for surveys...")

            # Wait for surveys container - try multiple selectors
            surveys_found = False
            possible_selectors = [
                (By.ID, "MainContent_repSurveys"),
                (By.CSS_SELECTOR, "[id*='repSurveys']"),
                (By.CSS_SELECTOR, ".survey"),
                (By.CSS_SELECTOR, "[id*='Survey']")
            ]

            for selector_type, selector_value in possible_selectors:
                try:
                    elements = driver.find_elements(selector_type, selector_value)
                    if elements:
                        surveys_found = True
                        print(f"  Found surveys container with selector: {selector_value}")
                        break
                except:
                    continue

            if not surveys_found:
                print("  WARNING: No survey container found - this facility may not have surveys")
                print(f"  Skipping...")
                continue

            # Grab survey links
            survey_links = driver.find_elements(By.XPATH, "//a[contains(@onclick, 'SurveyGenerator')]")
            survey_count = len(survey_links)
            print(f"  Found {survey_count} surveys")

            if survey_count == 0:
                print("  No surveys found for this facility, skipping...")
                continue

            # Process each survey
            for j in range(survey_count):
                # Re-fetch survey links in case DOM changed
                survey_links = driver.find_elements(By.XPATH, "//a[contains(@onclick, 'SurveyGenerator')]")

                onclick = survey_links[j].get_attribute("onclick")
                match = re.search(r"SurveyGenerator\('(\d+)'\)", onclick)
                if not match:
                    print(f"    Skipping survey {j+1} - no ID found")
                    continue
                sid = match.group(1)

                # Download PDF
                try:
                    # Set up download waiting
                    # Get the current number of files in downloads folder
                    downloads_dir = os.path.join(os.getcwd(), "downloads")
                    before_files = set(os.listdir(downloads_dir)) if os.path.exists(downloads_dir) else set()

                    # Click the survey link
                    survey_links[j].click()
                    print(f"    Clicked survey {j+1}/{survey_count}, waiting for download...")

                    # Wait for new file to appear (up to 30 seconds)
                    downloaded = False
                    for _ in range(30):
                        time.sleep(1)
                        if os.path.exists(downloads_dir):
                            after_files = set(os.listdir(downloads_dir))
                            new_files = after_files - before_files
                            # Filter out .crdownload or .tmp files
                            complete_files = [f for f in new_files if not f.endswith(('.crdownload', '.tmp', '.part'))]
                            if complete_files:
                                # Rename the file
                                downloaded_file = complete_files[0]
                                old_path = os.path.join(downloads_dir, downloaded_file)

                                # Sanitize filename
                                safe_name = re.sub(r'[<>:"/\\|?*]', '_', name)
                                filename = f"{license_type}_{safe_name}_{sid}.pdf"
                                new_path = os.path.join(downloads_dir, filename)

                                # Rename if different
                                if old_path != new_path:
                                    try:
                                        os.rename(old_path, new_path)
                                    except:
                                        # File might already exist, use the old name
                                        filename = downloaded_file

                                print(f"    [OK] Saved survey {j+1}/{survey_count}: {filename}")
                                downloaded = True
                                break

                    if not downloaded:
                        print(f"    [FAIL] Download timed out for survey {sid}")

                except Exception as e:
                    print(f"    [FAIL] Failed to download survey {sid}: {e}")

        except Exception as e:
            print(f"  ERROR processing facility: {e}")
            continue


def run():
    # Set up Chrome options
    options = uc.ChromeOptions()

    # Set download directory
    downloads_dir = os.path.join(os.getcwd(), "downloads")
    os.makedirs(downloads_dir, exist_ok=True)

    prefs = {
        "download.default_directory": downloads_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True  # Don't open PDFs in browser
    }
    options.add_experimental_option("prefs", prefs)

    # Additional options to avoid detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")

    print("Launching browser...")
    print("Auto-downloading ChromeDriver for your Chrome version...")
    # Let undetected-chromedriver auto-detect and download the correct version
    driver = uc.Chrome(options=options)

    # Set window size
    driver.set_window_size(1920, 1080)

    try:
        print(f"\nWill process {len(LICENSE_TYPES)} license type(s): {', '.join(LICENSE_TYPES)}\n")

        # Process each license type
        for idx, license_type in enumerate(LICENSE_TYPES):
            try:
                print(f"\n[{idx + 1}/{len(LICENSE_TYPES)}] Starting license type: {license_type}")
                process_license_type(driver, license_type, first_load=(idx == 0))
                print(f"[{idx + 1}/{len(LICENSE_TYPES)}] Completed license type: {license_type}")
            except Exception as e:
                print(f"\n[ERROR] Failed to process license type {license_type}: {e}")
                print("Continuing to next license type...")
                continue

        print("\n" + "="*60)
        print("Download complete! Check the 'downloads' folder for PDFs.")
        print("="*60)

    finally:
        print("\nClosing browser in 5 seconds...")
        time.sleep(5)
        driver.quit()

if __name__ == "__main__":
    run()
