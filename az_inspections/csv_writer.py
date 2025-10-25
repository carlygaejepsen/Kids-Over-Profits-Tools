import os
import json
import csv

def process_json_files(input_folder, output_csv_path):
    """
    Reads all JSON files from a folder, flattens the data, and writes it to a single CSV file.
    """
    # These are the headers for the final CSV file.
    headers = [
        "legal_name", "facility_status", "address", "license_status", "phone",
        "license_number", "max_licensed_capacity", "license_effective_date",
        "chief_administrative_officer", "license_expires_date", "owner_licensee",
        "inspection_number", "inspection_date", "inspection_type", "certificate_number",
        "deficiency_rule", "deficiency_evidence", "deficiency_findings"
    ]

    all_rows = []

    # Find all .json files in the input folder.
    try:
        json_files = [f for f in os.listdir(input_folder) if f.lower().endswith(".json")]
        print(f"Found {len(json_files)} JSON files to process.")
    except FileNotFoundError:
        print(f"[Error] ❌ The input folder was not found at: {input_folder}")
        return # Stop the script if the folder doesn't exist.

    # Loop through each JSON file.
    for filename in json_files:
        json_path = os.path.join(input_folder, filename)
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"  [Warning] Skipping {filename} due to an error: {e}")
            continue

        # Extract the main information.
        base_info = {key: data.get(key, "") for key in headers if not key.startswith('deficiency_')}
        
        # Get the list of deficiencies.
        deficiencies = data.get("deficiencies")

        # Process deficiencies.
        if deficiencies == "no deficiencies" or not deficiencies:
            # If there are no deficiencies, create a single row with a note.
            row = base_info.copy()
            row["deficiency_rule"] = "no deficiencies"
            all_rows.append(row)
        else:
            # If there are deficiencies, create a new row for each one.
            for deficiency in deficiencies:
                row = base_info.copy()
                row["deficiency_rule"] = deficiency.get("rule", "")
                row["deficiency_evidence"] = deficiency.get("evidence", "")
                row["deficiency_findings"] = deficiency.get("findings", "")
                all_rows.append(row)

    # Write all the collected data to the CSV file.
    try:
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n[Success] ✅ Successfully created CSV file at: {output_csv_path}")
    except Exception as e:
        print(f"\n[Error] ❌ Could not write to CSV file: {e}")


# --- Main execution block ---
if __name__ == "__main__":
    
    # --- 1. SET YOUR FOLDER AND FILE PATHS HERE ---
    # Use the 'r' before the string to handle Windows paths correctly.
    input_folder_path = r"C:\Users\daniu\OneDrive\Documents\GitHub\Kids-Over-Profits\Scripts\az_inspections\AZ_Reports"
    output_csv_path = r"C:\Users\daniu\OneDrive\Documents\GitHub\Kids-Over-Profits\Scripts\az_inspections\az_inspections.csv"
    # ---------------------------------------------

    print("--- Running JSON to CSV Converter ---")
    
    # --- 2. THE SCRIPT RUNS FROM HERE ---
    process_json_files(input_folder_path, output_csv_path)