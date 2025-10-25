import re
import pdfplumber

def extract_checklist_data(pdf_content):
    """Extract census, contact person, and licensor from checklist PDF"""
    try:
        with pdfplumber.open(pdf_content) as pdf:
            full_text = ""
            for page in pdf.pages:
                full_text += page.extract_text() + "\n"
        
        # Extract the three fields
        census = None
        contact_person = None
        licensor = None
        
        # Look for census - "Approved # of Present\n130 22" format
        census_match = re.search(r'Approved # of Present\s+\d+\s+(\d+)', full_text)
        if census_match:
            census = int(census_match.group(1))
        
        # Look for individual informed
        contact_match = re.search(r'Name of Individual Informed of this Inspection:\s*([^\n\r]+)', full_text)
        if contact_match:
            contact_person = contact_match.group(1).strip()
        
        # Look for licensor - stop at the next field
        licensor_match = re.search(r'Licensor\(s\) Conducting this Inspection:\s*([^O]+?)(?=\s+OL Staff|\n|$)', full_text)
        if licensor_match:
            licensor = licensor_match.group(1).strip()
        
        return {
            'census': census,
            'contact_person': contact_person,
            'licensor': licensor
        }
    
    except Exception as e:
        print(f"Error parsing PDF: {e}")
        return {'census': None, 'contact_person': None, 'licensor': None}

# Test it
with open("test_checklist_662483.pdf", "rb") as f:
    result = extract_checklist_data(f)
    print(result)