import requests
import json
import time
import os
from datetime import datetime
import pdfplumber
import re
from io import BytesIO

# EasyOCR imports
try:
    import easyocr
    from pdf2image import convert_from_bytes
    EASYOCR_AVAILABLE = True
except ImportError:
    print("EasyOCR not available. Install with: pip install easyocr pdf2image")
    EASYOCR_AVAILABLE = False
def extract_data_from_text(text, method="text"):
    """Extract census, contact person, and licensor from text using a single pattern for census"""
    if not text or len(text.strip()) == 0:
        return {'census': None, 'contact_person': None, 'licensor': None}
    
    census = None
    contact_person = None
    licensor = None
    
    if method == "easyocr":
        # --- EDITED SECTION ---
        # Updated pattern based on your new requirement.
        # This regex finds the number located *between* the words "Present" and "Capacity".
        pattern = r'Present.*?(\d+).*?Capacity'
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            census = int(match.group(1))
            print(f"      Pattern matched: {match.group(0)}")
            print(f"      Extracted census: {census}")
            
    else:
        # Original text extraction patterns
        census_pattern1 = re.search(r'Approved # of Present\s*\n\s*(\d+)', text)
        if census_pattern1:
            census = int(census_pattern1.group(1))
        else:
            census_pattern2 = re.search(r'Approved # of Present\s+(\d+)', text)
            if census_pattern2:
                census = int(census_pattern2.group(1))
            else:
                census_pattern3 = re.search(r'Approved # of Present\s+\d+\s+(\d+)', text)
                if census_pattern3:
                    census = int(census_pattern3.group(1))

    # Contact person patterns (work for both methods)
    contact_patterns = [
        r'Name of Individual Informed.*?Inspection:?\s*([^\n\r]+)',
        r'Individual Informed.*?:?\s*([A-Za-z][^\n\r]*)',
    ]
    
    for pattern in contact_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            contact_person = match.group(1).strip()
            contact_person = re.sub(r'\s+', ' ', contact_person)  # Clean up spaces
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
            licensor = re.sub(r'\s+', ' ', licensor)  # Clean up spaces
            break
    
    return {
        'census': census,
        'contact_person': contact_person,
        'licensor': licensor
    }

def extract_checklist_data(pdf_content):
    """Extract data with EasyOCR fallback"""
    try:
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            if len(pdf.pages) == 0:
                return {'census': None, 'contact_person': None, 'licensor': None, 'extraction_method': 'no_pages'}
            
            # Try regular text extraction first
            first_page = pdf.pages[0]
            text = first_page.extract_text()
            
            if text and len(text.strip()) > 0:
                result = extract_data_from_text(text, method="text")
                
                # If we got any useful data, return it
                if result['census'] is not None or result['contact_person'] is not None or result['licensor'] is not None:
                    result['extraction_method'] = 'text'
                    return result
            
            # If regular extraction failed, try EasyOCR
            if EASYOCR_AVAILABLE:
                try:
                    # Initialize EasyOCR reader (this might be slow first time)
                    if not hasattr(extract_checklist_data, '_reader'):
                        extract_checklist_data._reader = easyocr.Reader(['en'])
                    
                    # Convert first page to image
                    images = convert_from_bytes(pdf_content, first_page=1, last_page=1, dpi=300)
                    if images:
                        import numpy as np
                        from PIL import Image
                        
                        pil_image = images[0]
                        
                        # Try different rotations to handle sideways PDFs
                        rotations = [0, 90, 180, 270]
                        best_result = None
                        best_text = ""
                        best_angle = 0
                        
                        for angle in rotations:
                            # Rotate the image
                            rotated_img = pil_image.rotate(angle, expand=True)
                            img_array = np.array(rotated_img)
                            
                            # Use EasyOCR on rotated image
                            results = extract_checklist_data._reader.readtext(img_array)
                            ocr_text = ' '.join([result[1] for result in results])
                            
                            # Check if this rotation gives better results
                            # More text usually means better OCR recognition
                            if len(ocr_text) > len(best_text):
                                best_text = ocr_text
                                best_angle = angle
                                
                                # Try to extract data to see if we get meaningful results
                                test_result = extract_data_from_text(ocr_text, method="easyocr")
                                if (test_result['census'] is not None or 
                                    test_result['contact_person'] is not None or 
                                    test_result['licensor'] is not None):
                                    best_result = test_result
                                    break  # Found good data, stop trying rotations
                        
                        if best_text and len(best_text.strip()) > 0:
                            print(f"      EasyOCR extracted {len(best_text)} characters (rotation: {best_angle}°)")
                            
                            if best_result:
                                result = best_result
                            else:
                                result = extract_data_from_text(best_text, method="easyocr")
                            
                            result['extraction_method'] = f'easyocr_rotated_{best_angle}'
                            return result
                        else:
                            print("      EasyOCR found no text at any rotation")
                            
                except Exception as ocr_error:
                    print(f"      EasyOCR failed: {ocr_error}")
            
            # Return whatever we got from regular extraction
            result = extract_data_from_text(text, method="text") if text else {'census': None, 'contact_person': None, 'licensor': None}
            result['extraction_method'] = 'text_only'
            return result
                
    except Exception as e:
        print(f"      Error parsing PDF: {e}")
        return {'census': None, 'contact_person': None, 'licensor': None, 'extraction_method': 'error'}

# Test function
if __name__ == "__main__":
    print("Testing EasyOCR extraction...")
    print("Note: First run will be slow as EasyOCR downloads models")
    
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
        
        print(f"\nExpected: Census=10, Contact='Kimberly England', Licensor='Josilyn Bertrand, Mary Bokinskie'")
    else:
        print(f"Failed to download test checklist: {response.status_code}")
