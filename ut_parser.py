import json
import os
import pdfplumber
import re
from io import BytesIO

# EasyOCR imports (optional, only if needed)
try:
    import easyocr
    from pdf2image import convert_from_bytes
    EASYOCR_AVAILABLE = True
except ImportError:
    print("EasyOCR not available. Will only use text extraction.")
    EASYOCR_AVAILABLE = False

def extract_data_from_text(text, method="text"):
    """Extract capacity, census, contact person, and licensor from text"""
    if not text or len(text.strip()) == 0:
        return {'capacity': None, 'census': None, 'contact_person': None, 'licensor': None}
    
    capacity = None
    census = None
    contact_person = None
    licensor = None
    
    if method == "easyocr":
        # OCR patterns
        capacity_pattern = re.search(r'Approved.*?Capacity.*?[:：]?\s*(\d+)', text, re.IGNORECASE | re.DOTALL)
        if capacity_pattern:
            capacity = int(capacity_pattern.group(1))
        
        census_pattern = re.search(r'Present.*?Residents.*?[:：]?\s*(\d+)', text, re.IGNORECASE | re.DOTALL)
        if census_pattern:
            census = int(census_pattern.group(1))
    else:
        # Text extraction patterns for CAPACITY
        capacity_patterns = [
            r'Approved\s*\n?\s*Capacity\s*:?\s*(\d+)',
            r'Approved\s+Capacity\s*:?\s*(\d+)',
        ]
        
        for pattern in capacity_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                capacity = int(match.group(1))
                break
        
        # Text extraction patterns for CENSUS
        census_patterns = [
            r'#\s*of\s*Present\s*\n?\s*Residents[/\\]?Clients\s*:?\s*(\d+)',
            r'#\s*of\s*Present\s+Residents[/\\]?Clients\s*:?\s*(\d+)',
            r'Present\s*\n?\s*Residents[/\\]?Clients\s*:?\s*(\d+)',
        ]
        
        for pattern in census_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                census = int(match.group(1))
                break

    # Contact person patterns
    contact_patterns = [
        r'Name of Individual Informed.*?Inspection:?\s*([^\n\r]+)',
        r'Individual Informed.*?:?\s*([A-Za-z][^\n\r]*)',
    ]
    
    for pattern in contact_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            contact_person = match.group(1).strip()
            contact_person = re.sub(r'\s+', ' ', contact_person)
            break
    
    # Licensor patterns
    licensor_patterns = [
        r'Licensor\(?s?\)?\s*Conducting.*?Inspection:?\s*([^\n\r]+?)(?:\s+OL Staff|$)',
        r'Licensor.*?:?\s*([A-Za-z][^\n\r]*?)(?:\s+OL Staff|$)',
    ]
    
    for pattern in licensor_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            licensor = match.group(1).strip()
            licensor = re.sub(r'\s+', ' ', licensor)
            break
    
    return {
        'capacity': capacity,
        'census': census,
        'contact_person': contact_person,
        'licensor': licensor
    }

def extract_checklist_data(pdf_path):
    """Extract data from a PDF file"""
    try:
        with open(pdf_path, 'rb') as f:
            pdf_content = f.read()
            
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            if len(pdf.pages) == 0:
                return {'capacity': None, 'census': None, 'contact_person': None, 'licensor': None, 'extraction_method': 'no_pages'}
            
            first_page = pdf.pages[0]
            text = first_page.extract_text()
            
            if text and len(text.strip()) > 0:
                result = extract_data_from_text(text, method="text")
                
                if result['capacity'] is not None or result['census'] is not None or result['contact_person'] is not None or result['licensor'] is not None:
                    result['extraction_method'] = 'text'
                    return result
            
            # Try OCR if text extraction failed
            print(f"      Text extraction failed, trying OCR...")
            
            if EASYOCR_AVAILABLE:
                try:
                    if not hasattr(extract_checklist_data, '_reader'):
                        print("      Initializing EasyOCR...")
                        extract_checklist_data._reader = easyocr.Reader(['en'])
                    
                    images = convert_from_bytes(pdf_content, first_page=1, last_page=1, dpi=300)
                    if images:
                        import numpy as np
                        
                        pil_image = images[0]
                        rotations = [0, 90, 180, 270]
                        best_result = None
                        best_text = ""
                        best_angle = 0
                        
                        for angle in rotations:
                            rotated_img = pil_image.rotate(angle, expand=True)
                            img_array = np.array(rotated_img)
                            
                            results = extract_checklist_data._reader.readtext(img_array)
                            ocr_text = ' '.join([result[1] for result in results])
                            
                            if len(ocr_text) > len(best_text):
                                best_text = ocr_text
                                best_angle = angle
                                
                                test_result = extract_data_from_text(ocr_text, method="easyocr")
                                if (test_result['capacity'] is not None or test_result['census'] is not None or 
                                    test_result['contact_person'] is not None or test_result['licensor'] is not None):
                                    best_result = test_result
                                    break
                        
                        if best_text and len(best_text.strip()) > 0:
                            if best_result:
                                result = best_result
                            else:
                                result = extract_data_from_text(best_text, method="easyocr")
                            
                            result['extraction_method'] = f'easyocr_rotated_{best_angle}'
                            return result
                        
                except Exception as ocr_error:
                    print(f"      EasyOCR failed: {ocr_error}")
            
            return {'capacity': None, 'census': None, 'contact_person': None, 'licensor': None, 'extraction_method': 'all_failed'}
                
    except Exception as e:
        print(f"      Error parsing PDF: {e}")
        return {'capacity': None, 'census': None, 'contact_person': None, 'licensor': None, 'extraction_method': 'error'}

def main():
    # Load existing JSON
    INPUT_FILE = "./ut_reports.json"  # Change this to your JSON filename
    OUTPUT_FILE = "ut_reports.json"
    
    print(f"Loading {INPUT_FILE}...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        facilities_data = json.load(f)
    
    print(f"Found {len(facilities_data)} facilities")
    
    # Re-parse all checklists
    total_checklists = 0
    parsed_checklists = 0
    
    for facility in facilities_data:
        facility_id = facility['facility_id']
        print(f"\nProcessing facility {facility_id}...")
        
        for inspection in facility['inspections']:
            for checklist in inspection['checklists']:
                total_checklists += 1
                pdf_file = checklist.get('pdf_file')
                
                if pdf_file and os.path.exists(pdf_file):
                    print(f"  Re-parsing {pdf_file}...")
                    
                    # Extract data
                    extracted = extract_checklist_data(pdf_file)
                    
                    # Update checklist with new data
                    checklist['capacity'] = extracted['capacity']
                    checklist['census'] = extracted['census']
                    checklist['contact_person'] = extracted['contact_person']
                    checklist['licensor'] = extracted['licensor']
                    checklist['extraction_method'] = extracted['extraction_method']
                    
                    print(f"    Capacity={extracted['capacity']}, Census={extracted['census']}, Method={extracted['extraction_method']}")
                    parsed_checklists += 1
                else:
                    print(f"  PDF file not found: {pdf_file}")
    
    # Save updated JSON
    print(f"\nSaving to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(facilities_data, f, indent=2, ensure_ascii=False)
    
    print(f"\nDone! Re-parsed {parsed_checklists}/{total_checklists} checklists")
    print(f"Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()