import os
import time
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
# The base URL of the database, without page numbers.
BASE_URL = "https://disabilityrightsar.org/prtf-database/"

# The folder where PDFs will be saved. This will be created if it doesn't exist.
DOWNLOAD_FOLDER = "DRA_Reports"

# Delay between page requests in seconds to be respectful to the server.
REQUEST_DELAY = 1
# --- END OF CONFIGURATION ---


def download_file(url, folder_path):
    """
    Downloads a file from a URL into a specified folder using a streaming request.
    Args:
        url (str): The URL of the file to download.
        folder_path (str): The path to the folder to save the file in.
    """
    try:
        # Extract filename from the end of the URL.
        local_filename = url.split('/')[-1].split('?')[0]
        # Sanitize filename to be safe for file systems
        safe_filename = "".join([c for c in local_filename if c.isalpha() or c.isdigit() or c in ('.', '-', '_')]).rstrip()
        
        if not safe_filename:
            safe_filename = f"downloaded_report_{int(time.time())}.pdf"

        file_path = os.path.join(folder_path, safe_filename)

        if os.path.exists(file_path):
            print(f"  -> Skipping (already exists): {safe_filename}")
            return

        print(f"  -> Downloading: {safe_filename}")
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"  -> Saved: {safe_filename}")

    except requests.exceptions.RequestException as e:
        print(f"  -> FAILED to download {url}. Reason: {e}")
    except Exception as e:
        print(f"  -> An unexpected error occurred during download of {url}. Reason: {e}")


def main():
    """
    Main function to orchestrate the web scraping and downloading process.
    """
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)
        print(f"Created download folder: {DOWNLOAD_FOLDER}")

    page_counter = 1

    # Main loop to iterate through pages by constructing URLs.
    while True:
        # Construct the URL for the current page.
        if page_counter == 1:
            current_url = BASE_URL
        else:
            current_url = f"{BASE_URL}page/{page_counter}/"
        
        print(f"\n--- Processing Page {page_counter}: {current_url} ---")

        try:
            # Make the HTTP request to the page.
            response = requests.get(current_url, timeout=15)
            # If the page doesn't exist, the server returns a 404, which raises an error.
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print("  -> Page returned 404 Not Found. Reached end of database.")
            else:
                print(f"  -> HTTP Error: {e}")
            break # Exit the loop on any HTTP error.
        except requests.exceptions.RequestException as e:
            print(f"  -> A network error occurred: {e}")
            break

        # Parse the page's HTML.
        soup = BeautifulSoup(response.text, 'html.parser')
        
        pdf_links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if '.pdf' in href.lower():
                full_url = urljoin(BASE_URL, href)
                pdf_links.add(full_url)
        
        # If no PDF links are found on a valid page (status 200),
        # it's another indicator that we have reached the end.
        if not pdf_links:
            print("  -> No PDF links found on this page. Assuming end of database.")
            break

        print(f"  -> Found {len(pdf_links)} unique PDF links.")
        for link in pdf_links:
            download_file(link, DOWNLOAD_FOLDER)
        
        # Increment for the next iteration.
        page_counter += 1
        # Pause to be respectful to the website's server.
        time.sleep(REQUEST_DELAY)

    print("\n--- Process complete. ---")

if __name__ == "__main__":
    main()
