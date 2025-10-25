import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin, urlparse
import re

def read_search_terms(filename="Educational Consultants - Crowdsourced_Names.csv"):
    """
    Reads the consultant CSV and creates a set of all names and firms to search for.
    """
    search_terms = set()
    try:
        with open(filename, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Add last name
                if last_name := row.get('Last Name', '').strip():
                    search_terms.add(last_name.lower())
                
                # Add firm name
                if firm := row.get('Firm', '').strip():
                    if firm.lower() not in ['self-employed', '-']:
                        search_terms.add(firm.lower())

        print(f"Successfully read {len(search_terms)} unique search terms (last names and firms).")
        return search_terms
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
        return set()

def crawl_and_search(session, base_url, search_terms, writer):
    """
    Crawls a single website and searches the text of each page for the search terms.
    """
    pages_to_visit = {base_url}
    visited_pages = set()
    max_pages_per_site = 25 # Limit to prevent getting stuck on huge sites

    while pages_to_visit and len(visited_pages) < max_pages_per_site:
        current_url = pages_to_visit.pop()
        if current_url in visited_pages:
            continue

        print(f"  -> Crawling: {current_url}")
        visited_pages.add(current_url)

        try:
            response = session.get(current_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = soup.get_text().lower()

            # Check for any of the search terms in the page text
            for term in search_terms:
                if term in page_text:
                    print(f"    !!!! HIT FOUND !!!! Term: '{term.title()}' on page {current_url}")
                    writer.writerow({
                        'Source_Website': base_url,
                        'Page_URL_of_Hit': current_url,
                        'Search_Term_Found': term.title()
                    })

            # Find new internal links to crawl on the same domain
            base_netloc = urlparse(base_url).netloc
            for link in soup.find_all('a', href=True):
                absolute_url = urljoin(base_url, link['href'])
                if urlparse(absolute_url).netloc == base_netloc:
                    pages_to_visit.add(absolute_url)

        except (requests.exceptions.RequestException, requests.exceptions.Timeout):
            print(f"  -> Failed to load or timed out: {current_url}")
            continue
        
        time.sleep(1) # Be polite to the servers

def main():
    """Main function to run the targeted crawler."""
    
    # The list of websites you provided to search within
    websites_to_search = [

     "https://www.allkindsoftherapy.com/",
     "https://strugglingteens.com/",
     "https://www.prnewswire.com/"
    
    "http://www.preparetobloom.com/",
    "http://www.clearviewhorizon.com/",
     "http://www.healingconnectionsconsulting.com/",
     "https://theschoolsolution.com/",
     "http://www.campusdirection.com/",
     "http://www.you-can-succeed.org/",
     "https://emergingyoungadults.com/",
     "http://www.drburdick.com/",
     "http://www.oneoakconsult.com/",
     "http://pathwaypartners.info/",
     "http://cphprockville.com/",
     "https://www.levellife.me/",
     "https://sites.google.com/view/edusphereconsulting/about",
     "https://personalizedfamilysolutions.com/",
     "http://www.dobconsult.com/",
     "http://www.deandoering.com/",
     "https://www.castlehiounseling.org/",
     "http://www.quebec-consulting.com/",
     "http://brandielliott.com/",
     "http://www.educationaldirections.com/",
     "http://www.atlantapsychological.com/",
     "http://crossroadspsych.net/",
     "https://www.asgedconsulting.com/",
     "http://www.newchaptersconsulting.com/",
     "http://www.newcastleeducationalconsultants.com/",
     "http://www.therapeuticec.com/",
     "https://studentsolutions.com/",
     "https://bethhillmancoaching.com/",
     "http://www.educationalconsulting.com/",
     "https://www.journeytowardpeace.com/",
     "https://www.listoneducation.com/",
     "http://www.freudianslipcovers.com/",
     "http://www.maryloumarcus.com/",
     "http://masonconsult.com/",
     "https://www.mcmillaneducation.com/",
     "https://www.thebertramgroup.com/",
     "https://www.crossmanconsulting.com/",
     "http://www.consultredwood.com/",
     "http://www.incite-coaching.com/",
     "http://collegelsp.com/",
     "http://www.kathynauta.com/",
     "http://www.thechrissyconcept.com/",
     "http://www.cloverleaconsulting.com/",
     "http://www.johnhuie.com/",
     "https://www.sisuhealingpartners.com/",
     "http://www.northlightfamily.com/",
     "http://www.jraeducationalconsulting.com/",
     "http://www.dariarockholz.com/",
     "http://drsonya.com/",
     "http://www.aoplacement.com/",
     "http://schnitzerassociates.com/",
     "http://www.collegepossibilities.com/",
     "http://echusa.com/",
     "http://www.spettconsulting.com/",
     "http://www.jetedconsulting.com/",
     "http://listoneducation.com/",
     "http://www.livestronghouse.com/",
     "http://www.rosemarytippett.com/",
     "http://www.thecoastalinstitute.org/",
     "http://alternativesuccess.com/",
     "http://www.iepguardians.org/",
     "http://www.theaspiregroup.com/",
     "https://stuckersmithweatherly.com/",
     "https://www.acircleofhope.net/",
     "http://www.acircleofhope.net/",
     "http://www.wrighteducationalconsulting.com/",
     "https://www.hopestreamcommunity.org/", 
	 "http://www.alpineacademy.org/",
	 "http://www.arch.org/",
	 "http://www.ascendhc.com/",
	 "http://bnitreatment.com/",
	 "http://www.thecascadeprograms.com/",
	 "http://www.confluencevt.com/",
	 "http://www.constellationbh.com/",
	 "http://cooperriis.org/",
	 "http://www.discoveryranch.net/",
	 "http://www.discoveryranchsouth.com/",
	 "http://www.elevationsrtc.com/",
	 "http://familyfirstas.com/",
	 "http://www.fulsheartransition.com/",
	 "http://www.gatewayacademy.net/",
	 "http://www.havenwoodacademy.org/",
	 "http://www.heritagertc.org/",
	 "https://www.imperialhealinghouse.com/",
	 "http://www.intermountainresidential.org/",
	 "https://tlc4kids.org/journey-academy/",
	 "http://www.kaizenrtc.com/",
	 "http://www.kivaranch.com/",
	 "http://www.kolobcanyonrtc.com/",
	 "http://www.laeuropaacademy.com/",
	 "http://www.lifeskillssouthflorida.com/",
	 "http://www.lindnercenter.org/",
	 "http://www.loganriver.com/",
	 "http://www.maplelakeacademy.com/",
	 "http://www.meridell.com/",
	 "http://www.moonridgeacademy.com/",
	 "http://www.mountainlakeacademy.org/",
	 "http://www.nbiweston.com/",
	 "http://www.newhavenrtc.com/",
	 "http://www.collegesupportnw.com/",
	 "https://www.oasisascent.com/",
	 "http://www.oxbowacademy.net/",
	 "http://www.pathatstonesummit.com/",
	 "http://www.projectpatch.org/",
	 "http://www.redoakrecovery.com/",
	 "http://www.rogersbh.org/",
	 "http://sierrasagetreatmentcenter.com/",
	 "http://www.solsticertc.com/",
	 "http://www.stonewaterrecovery.com/",
	 "http://www.summitachievement.com/",
	 "http://www.sunrisertc.com/",
	 "http://www.tamarack.org/",
	 "http://www.telos.org/",
	 "http://www.triumphyouthservices.com/",
	 "http://www.uintaacademy.com/",
	 "http://www.thewedikoschool.org/",
	 "http://www.wellspring.org/",
	 "http://www.westbridge.org/",
	 "http://www.youthcare.com/",
	 "https://www.healthcare.utah.edu/hmhi",
	 "http://www.viewpointcenter.com/",
	 "http://www.vivetreatment.com/",
	 "http://www.anasazi.org/",
	 "http://www.blueridgewilderness.com/",
	 "http://www.bluefirewilderness.com/",
	 "http://www.legacyoutdooradventures.com/",
	 "http://www.redcliffascent.com/",
	 "http://www.second-nature.com/",
	 "http://www.summitachievement.com/",
	 "http://www.truenorthevolution.com/",
	 "http://www.theblackmountainacademy.com/",
	 "http://www.buildingbridgesinc.net/",
	 "http://www.chamberlainschool.org/",
	 "http://www.cherokeecreek.net/",
	 "http://www.chrysalisschoolmontana.com/",
	 "http://www.clearviewhorizon.com/",
	 "http://www.evangelhouse.com/",
	 "http://www.groveschool.org/",
	 "http://www.heartspring.org/",
	 "http://www.inbalanceacademy.com/",
	 "http://www.mountainlakeacademy.org/",
	 "http://tkds.org/",
	 "http://www.valleyviewschool.org/",
	 "http://www.whetstoneacademy.com/",
	 "http://www.atthecrossroads.com/",
	 "http://lifetutors.com/",
	 "http://www.nbiweston.com/",
	 "http://www.optimumperformanceinstitute.com/",
	 "http://www.prnforfamilies.com/",
	 "http://www.resiliencerecoveryresources.com/",
	 "http://www.aimhouse.com/",
	 "http://www.atthecrossroads.com/",
	 "http://www.confluencevt.com/",
	 "http://www.cornerstonesofmaine.com/",
	 "http://www.echo-springs.com/",
	 "http://www.fulsheartransition.com/",
	 "https://www.healthcare.utah.edu/hmhi/treatments/comprehensive-assessment-treatment",
	 "http://www.legacyoutdooradventures.com/",
	 "http://lifetutors.com/",
	 "http://www.collegesupportnw.com/",
	 "http://www.pacificquest.org/",
	 "http://www.redoakrecovery.com/",
	 "http://www.wellspring.org/",
	 "http://www.westbridge.org/",
   
    ]

    search_terms = read_search_terms()
    if not search_terms:
        return

    with open('edcon_hits_on_websites.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Source_Website', 'Page_URL_of_Hit', 'Search_Term_Found']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        with requests.Session() as session:
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            })
            
            for i, site_url in enumerate(websites_to_search):
                print(f"\n--- Processing site {i+1}/{len(websites_to_search)}: {site_url} ---")
                crawl_and_search(session, site_url, search_terms, writer)

    print("\n-----------------------------------------")
    print("Targeted crawl complete!")
    print("All found connections have been saved to 'edcon_hits_on_websites.csv'.")

if __name__ == "__main__":
    main()
