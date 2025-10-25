import os
import fitz  # PyMuPDF
import json
import re
from pathlib import Path
from datetime import datetime

# --- Configuration ---
# Patterns for the main header information, updated for more precision.
EXTRACTION_PATTERNS = {
    "legal_name": r"Legal Name\s*\n\s*([^\n]+)",
    "facility_status": r"Facility Status\s*\n\s*([^\n]+)",
    "address": r"Address\s*\n\s*([^\n]+)",
    "license_status": r"License Status\s*\n\s*([^\n]+)",
    "phone": r"Phone\s*\n\s*([^\n]+)",
    "license_number": r"License\s*\n\s*([^\n]+)",
    "max_licensed_capacity": r"Maximum Licensed Capacity\s*\n\s*([^\n]+)",
    "license_effective_date": r"License Effective\s*\n\s*([^\n]+)",
    "chief_administrative_officer": r"Chief Administrative Officer\s*\n\s*([^\n]+)",
    "license_expires_date": r"License Expires\s*\n\s*([^\n]+)",
    "owner_licensee": r"Owner / Licensee\s*\n\s*([^\n]+)",
    "inspection_number": r"Inspection #\s*\n\s*([^\n]+)",
    "inspection_date": r"Inspection Date\(s\)\s*\n\s*([^\n]+)",
    "inspection_type": r"Inspection Type\s*\n\s*([^\n]+)",
    "certificate_number": r"Certificate Number\s*\n\s*([^\n]+)",
}


def extract_text_from_pdf(pdf_path):
    """
    Extracts all text from a given PDF file using PyMuPDF.

    Args:
        pdf_path (str): The full path to the PDF file.

    Returns:
        str: The extracted text content, or None if an error occurs.
    """
    try:
        with fitz.open(pdf_path) as doc:
            text = ""
            for page in doc:
                text += page.get_text("text")
        return text
    except Exception as e:
        print(f"  [Error] Could not extract text from {pdf_path.name}: {e}")
        return None

def extract_data_with_regex(text, patterns):
    """
    Extracts data from text using a dictionary of regex patterns.
    It handles the header fields and then processes the complex deficiency sections separately.

    Args:
        text (str): The text to search within.
        patterns (dict): A dictionary where keys are field names and
                         values are regex pattern strings for the header.

    Returns:
        dict: A dictionary with all the extracted data.
    """
    extracted_data = {}
    # First, extract all the simple header fields
    for field_name, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Clean up the matched group to avoid unwanted text
            clean_text = match.group(1).strip()
            # A specific fix for license_number grabbing wrong text
            if field_name == 'license_number' and '# to view' in clean_text:
                 # This is a common error, we try to find the real license number elsewhere
                 real_license_match = re.search(r"License\s+([A-Z]{2}\d+)", text, re.IGNORECASE)
                 if real_license_match:
                     clean_text = real_license_match.group(1).strip()
                 else:
                     clean_text = "Not Found" # Or keep the incorrect one as a flag
            extracted_data[field_name] = clean_text
        else:
            extracted_data[field_name] = None
            
    # --- FINAL LOGIC FOR DEFICIENCIES ---
    # 1. Split the document into chunks using "Statement of Deficiency" as a separator.
    chunks = re.split(r"(?i)\s*Statement of Deficiency\s*", text)[1:]
    
    if not chunks:
        extracted_data["deficiencies"] = "no deficiencies"
    else:
        deficiency_list = []
        for chunk in chunks:
            # 2. Within each chunk, find the "Rule" and "Evidence" labels.
            rule_label_match = re.search(r"Rule", chunk, re.IGNORECASE)
            evidence_label_match = re.search(r"Evidence", chunk, re.IGNORECASE)

            if not rule_label_match or not evidence_label_match:
                continue

            # 3. The Rule text is everything BETWEEN the two labels.
            rule_text = chunk[rule_label_match.end():evidence_label_match.start()].strip()
            
            # 4. The main block is everything AFTER the "Evidence" label.
            main_block = chunk[evidence_label_match.end():].strip()
            
            # The rest of the logic for splitting findings remains the same.
            findings_text = ""
            findings_split = re.split(r"Findings include:", main_block, maxsplit=1, flags=re.IGNORECASE)
            if len(findings_split) > 1:
                evidence_text = findings_split[0].strip()
                findings_text = findings_split[1].strip()
            else:
                evidence_text = main_block.strip()

            deficiency_list.append({
                "rule": re.sub(r'\s+', ' ', rule_text),
                "evidence": re.sub(r'\s+', ' ', evidence_text),
                "findings": re.sub(r'\s+', ' ', findings_text)
            })
        
        extracted_data["deficiencies"] = deficiency_list
        
    return extracted_data


def process_pdf_folder(input_folder, output_folder):
    """
    Processes all PDF files in a folder, extracts data using regex,
    and saves the output as JSON.

    Args:
        input_folder (pathlib.Path): Path to the folder containing PDF files.
        output_folder (pathlib.Path): Path to the folder where JSON files will be saved.
    """
    if not output_folder.exists():
        output_folder.mkdir()
        print(f"Created output directory: {output_folder}")

    pdf_files = list(input_folder.glob("*.pdf"))
    total_files = len(pdf_files)
    print(f"Found {total_files} PDF files to process.")
    
    processed_count = 0
    for i, pdf_path in enumerate(pdf_files):
        print(f"\n--- Processing file {i+1}/{total_files}: {pdf_path.name} ---")
        
        datestamp = datetime.now().strftime("%m%d%Y")
        json_filename = f"{pdf_path.stem}_{datestamp}.json"        
        json_path = output_folder / json_filename

        if json_path.exists():
            print(f"  [Skipped] Output file already exists: {json_filename}")
            continue

        # 1. Extract text
        print("  Step 1: Extracting text...")
        text = extract_text_from_pdf(pdf_path)
        if not text or not text.strip():
            print("  [Warning] No text found or extracted.")
            continue
        
        # Clean the text to remove form feed characters
        text = text.replace('\x0c', ' ')

        # 2. Extract structured data using RegEx
        print("  Step 2: Extracting data with defined patterns...")
        structured_data = extract_data_with_regex(text, EXTRACTION_PATTERNS)
        
        # 3. Save the result
        if structured_data:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(structured_data, f, indent=4)
            print(f"  [Success] Saved structured data to {json_filename}")
            processed_count += 1
        else:
            print("  [Failed] Could not extract data from this file.")

    print(f"\n--- Batch processing complete! Processed {processed_count}/{total_files} files. ---")


if __name__ == "__main__":
    print("--- Running PDF Extractor v13.0 (with column-aware parsing) ---")
    
    # Automatically use the directory where the script is located
    current_directory = Path(__file__).resolve().parent
    
    # Both input and output folders are the same as the script's directory
    process_pdf_folder(current_directory, current_directory)