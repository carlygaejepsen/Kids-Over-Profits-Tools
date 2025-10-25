import requests
from bs4 import BeautifulSoup
import csv
import time

def scrape_natsap_programs():
    """
    Scrapes multiple NATSAP program directory pages using precise, user-provided
    HTML selectors and saves the data to a single CSV file.
    """
    urls_to_scrape = [
        {
            "url": "https://members.natsap.org/program-school-directory/Search/residential-treatment-center-576798",
            "category": "Residential Treatment Center",
            "is_young_adult": False
        },
        {
            "url": "https://members.natsap.org/program-school-directory/Search/specialty-psychiatric-behavioral-hospital-576805",
            "category": "Specialty Psychiatric/Behavioral Hospital",
            "is_young_adult": False
        },
        {
            "url": "https://members.natsap.org/program-school-directory/Search/wilderness-therapy-program-576799",
            "category": "Wilderness Therapy Program",
            "is_young_adult": False
        },
        {
            "url": "https://members.natsap.org/program-school-directory/Search/therapeutic-boarding-school-576795",
            "category": "Therapeutic Boarding School",
            "is_young_adult": False
        },
        {
            "url": "https://members.natsap.org/program-school-directory/Search/transitional-independent-living-program-576802",
            "category": "Transitional Independent Living Program",
            "is_young_adult": False
        },
        {
            "url": "https://members.natsap.org/program-school-directory/Search/young-adult-program-serving-18-only-576797",
            "category": "Young Adult Program",
            "is_young_adult": True
        }
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    with open('natsap_programs.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Program_Name', 'Category', 'City', 'State', 'Phone', 'Website']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for item in urls_to_scrape:
            url, category, is_young_adult = item["url"], item["category"], item["is_young_adult"]
            
            print(f"\nFetching data for category: {category}...")
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"  Error fetching {url}: {e}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            program_cards = soup.find_all('div', class_='card gz-directory-card Rank10')
            
            if not program_cards:
                print(f"  Could not find any program cards on the page for {category}.")
                continue

            print(f"  Found {len(program_cards)} programs. Processing...")

            for card in program_cards:
                name, city, state, phone, website = '', '', '', '', ''

                card_body = card.find('div', class_='card-body gz-directory-card-body')
                if card_body:
                    # Name: from h5.card-title.gz-card-title
                    if name_tag := card_body.find('h5', class_='card-title gz-card-title'):
                        if a_tag := name_tag.find('a'):
                            name = a_tag.text.strip()
                            if is_young_adult:
                                name += " (18+)"
                    
                    # Address: from li.list-group-item.gz-card-address
                    if address_li := card_body.find('li', class_='list-group-item gz-card-address'):
                        # The address is usually in the format "City, ST 12345"
                        full_address = address_li.text.strip()
                        if ',' in full_address:
                            city_part, state_part = full_address.split(',', 1)
                            city = city_part.strip()
                            # State is usually the first two letters of the remaining string
                            state = state_part.strip().split(' ')[0]

                    # Phone: from li.list-group-item.gz-card-phone
                    if phone_li := card_body.find('li', class_='list-group-item gz-card-phone'):
                        phone = phone_li.text.strip()
                    
                    # Website: from a tag in li.list-group-item.gz-card-website
                    if website_li := card_body.find('li', class_='list-group-item gz-card-website'):
                        if a_tag := website_li.find('a'):
                            website = a_tag.get('href', '')

                writer.writerow({
                    'Program_Name': name,
                    'Category': category,
                    'City': city,
                    'State': state,
                    'Phone': phone,
                    'Website': website
                })
                
                time.sleep(0.05)

    print("\n-----------------------------------------")
    print("Scraping complete!")
    print("All program data has been saved to 'natsap_programs.csv'.")

if __name__ == "__main__":
    # To run this script, you need to install the required libraries:
    # pip install requests
    # pip install beautifulsoup4
    scrape_natsap_programs()
