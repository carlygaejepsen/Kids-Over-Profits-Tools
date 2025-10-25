import requests
import json
import time
import os
from datetime import datetime
import pdfplumber
import re
from io import BytesIO

# OCR imports
try:
    import pytesseract
    from pdf2image import convert_from_bytes
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

def extract_data_from_text(text, method="text"):
    """Extract census, contact person, and licensor from text using multiple pattern sets"""
    if not text or len(text.strip()) == 0:
        return {'census': None, 'contact_person': None, 'licensor': None}
    
    census = None
    contact_person = None
    licensor = None
    
    if method == "text":
        # Original patterns for regular text extraction
        # Pattern 1: Numbers on separate lines
        census_pattern1 = re.search(r'Approved # of Present\s*\n\s*(\d+)', text)
        if census_pattern1:
            census = int(census_pattern1.group(1))
        else:
            # Pattern 2: Numbers on same line (no capacity first)
            census_pattern2 = re.search(r'Approved # of Present\s+(\d+)', text)
            if census_pattern2:
                census = int(census_pattern2.group(1))
            else:
                # Pattern 3: Original pattern (capacity then census)
                census_pattern3 = re.search(r'Approved # of Present\s+\d+\s+(\d+)', text)
                if census_pattern3:
                    census = int(census_pattern3.group(1))
    
    elif method == "ocr":
        # OCR patterns for visual table format
        # Look for "# of Present Residents/Clients: 10"
        census_pattern_ocr1 = re.search(r'# of Present.*?Residents.*?Clients.*?(\d+)', text, re.IGNORECASE | re.DOTALL)
        if census_pattern_ocr1:
            census = int(census_pattern_ocr1.group(1))
        else:
            # Alternative OCR pattern
            census_pattern_ocr2 = re.search(r'Present.*?(\d+)', text, re.IGNORECASE)
            if census_pattern_ocr2:
                census = int(census_pattern_ocr2.group(1))
        
        # Also try the original patterns in case OCR text is clean
        if census is None:
            census_pattern_fallback = re.search(r'Approved.*?Present.*?(\d+)', text, re.IGNORECASE)
            if census_pattern_fallback:
                census = int(census_pattern_fallback.group(1))
    
    # Contact person patterns (work for both methods)
    contact_match = re.search(r'Name of Individual Informed of (?:this )?Inspection:?\s*([^\n\r]+)', text, re.IGNORECASE)
    if contact_match:
        contact_person = contact_match.group(1).strip()
    
    # Licensor patterns (work for both methods)  
    licensor_match = re.search(r'Licensor\(?s?\)?\s*Conducting (?:this )?Inspection:?\s*([^\n\r]+?)(?:\s+OL Staff|$)', text, re.IGNORECASE)
    if licensor_match:
        licensor = licensor_match.group(1).strip()
    
    return {
        'census': census,
        'contact_person': contact_person,
        'licensor': licensor
    }

def extract_checklist_data(pdf_content):
    """Extract data with OCR fallback for complete failures"""
    try:
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            if len(pdf.pages) == 0:
                return {'census': None, 'contact_person': None, 'licensor': None, 'extraction_method': 'no_pages'}
            
            # Try regular text extraction first
            first_page = pdf.pages[0]
            text = first_page.extract_text()
            
            if text and len(text.strip()) > 0:
                result = extract_data_from_text(text, method="text")
                
                # If we got any data, return it
                if result['census'] is not None or result['contact_person'] is not None or result['licensor'] is not None:
                    result['extraction_method'] = 'text'
                    return result
            
            # If regular extraction got no data, try OCR
            if OCR_AVAILABLE:
                try:
                    images = convert_from_bytes(pdf_content, first_page=1, last_page=1, dpi=300)
                    if images:
                        ocr_text = pytesseract.image_to_string(images[0], config='--psm 6')
                        
                        if ocr_text and len(ocr_text.strip()) > 0:
                            result = extract_data_from_text(ocr_text, method="ocr")
                            result['extraction_method'] = 'ocr'
                            return result
                        
                except Exception as ocr_error:
                    print(f"      OCR failed: {ocr_error}")
            
            # Return whatever we got from regular extraction, even if empty
            result = extract_data_from_text(text, method="text") if text else {'census': None, 'contact_person': None, 'licensor': None}
            result['extraction_method'] = 'text_only'
            return result
                
    except Exception as e:
        print(f"      Error parsing PDF: {e}")
        return {'census': None, 'contact_person': None, 'licensor': None, 'extraction_method': 'error'}

# Test with the visual format we just saw
if __name__ == "__main__":
    print("Testing enhanced extraction with OCR patterns...")
    
    # Test with the checklist that has the table format
    test_checklist_id = 571436
    
    pdf_url = f"https://ccl.utah.gov/ccl/public/checklist/{test_checklist_id}?dl=1"
    response = requests.get(pdf_url, timeout=10)
    
    if response.status_code == 200:
        result = extract_checklist_data(response.content)
        print(f"\nTest result for checklist {test_checklist_id}:")
        print(f"Census: {result['census']}")
        print(f"Contact: {result['contact_person']}")
        print(f"Licensor: {result['licensor']}")
        print(f"Method: {result['extraction_method']}")
        
        # Expected results based on visual inspection:
        print(f"\nExpected: Census=10, Contact='Kimberly England', Licensor='Josilyn Bertrand, Mary Bokinskie'")
    else:
        print(f"Failed to download test checklist: {response.status_code}")
