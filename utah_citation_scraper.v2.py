import requests
import csv
import time
from datetime import datetime

# Configuration
FACILITY_IDS = [96697, 93201, 93220, 93242, 93243, 93266,
    93245, 99864, 94203, 94202,
    93281, 112281, 93761, 93248, 93247, 93323, 93321, 99192,
    93341, 93342, 99846, 93343, 94407, 93403,
    93420, 93421, 93408, 93244, 93860,
    93412,  93413, 93443, 93981, 93636, 93416, 93415, 93414, 
    93501, 93484, 95545, 110140, 93487,
    117274, 117277, 93488, 94923, 93490,
    99843, 93491, 98769, 105460, 94205,
    98822, 93493, 93494, 93503, 93496, 93521, 93522, 93524, 99506,
    98834, 101496, 93527, 93528, 93529, 93530, 93533, 93531, 93532, 93534,
    93541, 99011, 98019, 97996, 97576, 95546, 95960, 93711, 93712, 95041,
    93542, 93537, 93823, 95810, 119530, 119535, 119533, 93560, 93640, 
    93623, 93624, 94216, 93625,  93635, 93661, 93662, 93637, 93639,
    93660, 98533, 99272, 99058, 98507, 98194, 106078, 93666,
    107485, 98883, 93687, 93686, 93688, 94380, 96994, 93692,
    93694, 94206, 93695, 93696, 93697, 98254, 93700, 98250, 93698, 93699,
    110301, 93701, 93703,
    93702, 93241, 105000, 93262, 93264, 93261, 93263, 93704, 93708,
    95883, 93715, 93940, 111725, 93717,
    93721, 104399, 93762, 93724, 93728, 93727, 93725, 93726, 93763]  
OUTPUT_FILE = "utah_citations_{datetime.now().strftime('%Y-%m')}.csv"
REQUEST_DELAY = 1  # Seconds between requests
MAX_INSPECTIONS = 20  # Maximum number of inspections per facility to include

def fetch_facility_data(facility_id):
    """Fetch JSON data for a single facility with robust error handling"""
    url = f"https://ccl.utah.gov/ccl/public/facilities/{facility_id}.json"
    try:
        print(f"🔍 Fetching {facility_id}...", end=" ", flush=True)
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise HTTP errors
        print("✅ Success")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed (Error: {str(e)})")
        return None

def format_address(address):
    """Convert address dict to single string"""
    parts = [
        address.get('addressOne', ''),
        address.get('city', ''),
        address.get('state', ''),
        address.get('zipCode', '')
    ]
    return ', '.join(filter(None, parts))

def main():
    print(f"🚀 Starting data export ({len(FACILITY_IDS)} facilities)")
    
    # Prepare CSV header
    headers = [
        'Facility ID', 'Name', 'Address',
        'Regulation Date', 'Expiration Date', 'Conditional'
    ]
    # Add dynamic inspection columns
    for i in range(1, MAX_INSPECTIONS + 1):
        headers.extend([
            f'Inspection {i} Date',
            f'Inspection {i} Type',
            f'Inspection {i} Findings'
        ])
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        for facility_id in FACILITY_IDS:
            data = fetch_facility_data(facility_id)
            if not data:
                continue
            
            # Base info
            row = [
                facility_id,
                data.get('name', ''),
                format_address(data.get('address', {})),
                data.get('initialRegulationDate', ''),
                data.get('expirationDate', ''),
                data.get('conditional', False)
            ]
            
            # Add inspections (up to MAX_INSPECTIONS)
            inspections = data.get('inspections', [])[:MAX_INSPECTIONS]
            for inspection in inspections:
                findings = [
                    f"{f.get('ruleNumber', '?')}: {f.get('ruleDescription', '')}... | Finding: {f.get('findingText', '')}..."
                    for f in inspection.get('findings', [])
                ]
                row.extend([
                    inspection.get('inspectionDate', ''),
                    inspection.get('inspectionTypes', ''),
                    ' | '.join(findings) if findings else 'None'
                ])
            
            # Pad with empty columns if fewer than MAX_INSPECTIONS
            while len(row) < len(headers):
                row.extend(['', '', ''])
            
            writer.writerow(row)
            time.sleep(REQUEST_DELAY)
    
    print(f"\n🎉 Done! Data saved to {OUTPUT_FILE}")
    print("💡 Pro tip: Open in Excel and use Text-to-Columns if needed")

if __name__ == "__main__":
    main()