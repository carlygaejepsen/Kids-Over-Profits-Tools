import requests
from bs4 import BeautifulSoup
import csv
import time

def scrape_natsap_directory():
    """
    Scrapes the NATSAP individual professional membership directory using precise, confirmed
    HTML selectors and saves the data to a CSV file.
    """
    url = "https://members.natsap.org/individualprofessionalmembershipdirectory/FindStartsWith?term=%23%21"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print("Fetching data from NATSAP directory...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the URL: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    # --- FINAL, PRECISE SELECTOR ---
    # Find all the individual card containers using the exact class combination.
    member_cards = soup.find_all('div', class_='card gz-directory-card Rank10')
    
    if not member_cards:
        print("Could not find any member cards with class 'card gz-directory-card Rank10'. The website structure may have changed.")
        return

    print(f"Found {len(member_cards)} members. Processing now...")

    with open('natsap_members.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Name', 'Title', 'Company', 'Phone', 'Website', 'City', 'State']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for card in member_cards:
            # Initialize all variables to be safe
            name, title, company, phone, website, city, state = '', '', '', '', '', '', ''

            # --- Extract Data from within the card body ---
            card_body = card.find('div', class_='card-body gz-directory-card-body')
            if card_body:
                # Name: Found in the 'a' tag within the h5.card-title
                if name_tag := card_body.find('h5', class_='card-title gz-card-title'):
                    if a_tag := name_tag.find('a'):
                        name = a_tag.text.strip()
                
                # Title: Found in the span.gz-list-title
                if title_tag := card_body.find('span', class_='gz-list-title'):
                    title = title_tag.text.strip()

                # Company: Found in the span.gz-list-org-name
                if company_tag := card_body.find('span', class_='gz-list-org-name'):
                    company = company_tag.text.strip()

                # Phone: Found in the li.gz-card-phone
                if phone_li := card_body.find('li', class_='list-group-item gz-card-phone'):
                    phone = phone_li.text.strip()
                
                # Website: Found in the 'a' tag's href within li.gz-card-website
                if website_li := card_body.find('li', class_='list-group-item gz-card-website'):
                    if a_tag := website_li.find('a'):
                        website = a_tag.get('href', '')

            # --- Extract Location from the card's parent container ---
            parent_container = card.find_parent('div', class_='Rank10 gz-grid-col')
            if parent_container:
                # Find all text nodes that are direct children of the parent
                location_texts = parent_container.find_all(string=True, recursive=False)
                full_location = ' '.join(location_texts).strip()
                if ',' in full_location:
                    parts = full_location.split(',')
                    city = parts[0].strip()
                    state = parts[1].strip()

            writer.writerow({
                'Name': name,
                'Title': title,
                'Company': company,
                'Phone': phone,
                'Website': website,
                'City': city,
                'State': state
            })
            
            time.sleep(0.05)

    print("\nScraping complete!")
    print("Data has been saved to 'natsap_members.csv' in the same directory as this script.")

if __name__ == "__main__":
    # To run this script, you need to install the required libraries:
    # pip install requests
    # pip install beautifulsoup4
    scrape_natsap_directory()
