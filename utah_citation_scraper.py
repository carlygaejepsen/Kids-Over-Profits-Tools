"""Utah citation scraper with OCR-enhanced checklist parsing."""

from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import pdfplumber
import requests

# EasyOCR imports
try:
    import easyocr
    from pdf2image import convert_from_bytes

    EASYOCR_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    print("EasyOCR not available. Install with: pip install easyocr pdf2image")
    EASYOCR_AVAILABLE = False


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "middle"
CHECKLIST_DIR = BASE_DIR / "checklists"
OUTPUT_DIR.mkdir(exist_ok=True)
CHECKLIST_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "ut_reports_with_ocr.json"
OUTPUT_CSV = OUTPUT_DIR / f"utah_citations_{datetime.now().strftime('%m-%d-%Y')}.csv"

# Configuration
DEBUG_EXTRACTION = True  # Set to True to see detailed extraction debug info
SAVE_EXTRACTED_TEXT = True  # Set to True to save extracted text for manual review
TEST_MODE = False  # Set to True to only process first 3 facilities for testing
TEXT_OUTPUT_DIR = BASE_DIR / "extracted_text"
if SAVE_EXTRACTED_TEXT:
    TEXT_OUTPUT_DIR.mkdir(exist_ok=True)

FACILITY_IDS = [
    96697,
    93201,
    93220,
    93242,
    93243,
    93266,
    93245,
    99864,
    94203,
    94202,
    93281,
    112281,
    93761,
    93248,
    93247,
    93323,
    93321,
    99192,
    93341,
    93342,
    99846,
    93343,
    94407,
    93403,
    93420,
    93421,
    93408,
    93244,
    93860,
    93412,
    93413,
    93443,
    93981,
    93636,
    93416,
    93415,
    93414,
    93501,
    93484,
    95545,
    110140,
    93487,
    117274,
    117277,
    93488,
    94923,
    93490,
    99843,
    93491,
    98769,
    105460,
    94205,
    98822,
    93493,
    93494,
    93503,
    93496,
    93521,
    93522,
    93524,
    99506,
    98834,
    101496,
    93527,
    93528,
    93529,
    93530,
    93533,
    93531,
    93532,
    93534,
    93541,
    99011,
    98019,
    97996,
    97576,
    95546,
    95960,
    93711,
    93712,
    95041,
    93542,
    93537,
    93823,
    95810,
    119530,
    119535,
    119533,
    93560,
    93640,
    93623,
    93624,
    94216,
    93625,
    93635,
    93661,
    93662,
    93637,
    93639,
    93660,
    98533,
    99272,
    99058,
    98507,
    98194,
    106078,
    93666,
    107485,
    98883,
    93687,
    93686,
    93688,
    94380,
    96994,
    93692,
    93694,
    94206,
    93695,
    93696,
    93697,
    98254,
    93700,
    98250,
    93698,
    93699,
    110301,
    93701,
    93703,
    93702,
    93241,
    105000,
    93262,
    93264,
    93261,
    93263,
    93704,
    93708,
    95883,
    93715,
    93940,
    111725,
    93717,
    93721,
    104399,
    93762,
    93724,
    93728,
    93727,
    93725,
    93726,
    93763,
]
REQUEST_DELAY = 1  # Seconds between requests
MAX_INSPECTIONS = 20  # Maximum number of inspections per facility to include


ChecklistResult = Dict[str, Optional[Any]]
FacilityRecord = Dict[str, Any]


def extract_data_from_text(text: str, method: str = "text", debug: bool = False) -> ChecklistResult:
    """Extract census, capacity, contact person, and licensor from text using multiple pattern sets."""

    if not text or not text.strip():
        return {"census": None, "capacity": None, "contact_person": None, "licensor": None}

    census: Optional[int] = None
    capacity: Optional[int] = None
    contact_person: Optional[str] = None
    licensor: Optional[str] = None

    if method == "easyocr":
        # OCR-specific pattern for table format
        # Try to find "Present [number] Capacity [number]"
        pattern = r"Present.*?(\d+).*?Capacity.*?(\d+)"
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            census = int(match.group(1))
            capacity = int(match.group(2))
            if debug:
                print(f"      DEBUG OCR: Census={census}, Capacity={capacity}")
        elif debug:
            print(f"      DEBUG OCR: Pattern failed")
    else:
        # Try multiple patterns in order of specificity
        patterns_tried = []

        # Pattern 1: Table format "Approved # | Capacity | Present" with values below
        table_pattern = re.search(
            r"Approved\s*#.*?Capacity.*?Present.*?\n\s*(\d+)\s+(\d+)\s+(\d+)",
            text,
            re.IGNORECASE | re.DOTALL
        )
        if table_pattern:
            capacity = int(table_pattern.group(2))
            census = int(table_pattern.group(3))
            if debug:
                print(f"      DEBUG P1-Table: Approved={table_pattern.group(1)}, "
                      f"Capacity={capacity}, Census={census}")
        else:
            patterns_tried.append("P1-Table")

            # Pattern 2: Alternate table order "Present | Capacity | Approved #"
            table_pattern2 = re.search(
                r"Present.*?Capacity.*?Approved\s*#.*?\n\s*(\d+)\s+(\d+)\s+(\d+)",
                text,
                re.IGNORECASE | re.DOTALL
            )
            if table_pattern2:
                census = int(table_pattern2.group(1))
                capacity = int(table_pattern2.group(2))
                if debug:
                    print(f"      DEBUG P2-TableAlt: Census={census}, "
                          f"Capacity={capacity}, Approved={table_pattern2.group(3)}")
            else:
                patterns_tried.append("P2-TableAlt")

                # Pattern 2b: Try to find Present and Capacity on same line or nearby
                # Format: "Present: 12  Capacity: 30" or similar
                combined_pattern = re.search(
                    r"Present[:\s]+(\d+).*?Capacity[:\s]+(\d+)",
                    text,
                    re.IGNORECASE | re.DOTALL
                )
                if combined_pattern:
                    census = int(combined_pattern.group(1))
                    capacity = int(combined_pattern.group(2))
                    if debug:
                        print(f"      DEBUG P2b-Combined: Census={census}, Capacity={capacity}")
                else:
                    patterns_tried.append("P2b-Combined")

                # Pattern 3-7: Various "Present" formats
                present_patterns = [
                    (r"Present\s*\n\s*(\d+)", "P3-Present\\n"),
                    (r"Present\s*[:;]\s*(\d+)", "P4-Present:"),
                    (r"Present\s+(\d+)(?:\s|$)", "P5-Present_"),
                    (r"Present\s*[|]\s*(\d+)", "P6-Present|"),
                    (r"(?:^|\n)Present.*?(\d+)", "P7-PresentAny"),
                ]

                for pattern, label in present_patterns:
                    match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                    if match:
                        census = int(match.group(1))
                        # Try multiple capacity patterns in order (including variations)
                        capacity_patterns = [
                            (r"(?:Licensed\s+)?Capacity\s*\n\s*(\d+)", "Cap\\n"),
                            (r"(?:Licensed\s+|Approved\s+|Max(?:imum)?\s+)?Capacity\s*[:;\s]+(\d+)", "Cap:"),
                            (r"(?:Licensed\s+)?Capacity\s*[|]\s*(\d+)", "Cap|"),
                            (r"(?:Licensed\s+|Approved\s+|Max(?:imum)?\s+)?Capacity\s+(\d+)", "Cap_"),
                            (r"(?:Licensed|Approved|Max|Maximum)?\s*Capacity.*?(\d+)", "CapAny"),
                            (r"Approved\s*#\s*\n\s*\d+\s+(\d+)", "ApprAlt"),  # Approved # then two numbers
                        ]
                        cap_found = False
                        for cap_pattern, cap_label in capacity_patterns:
                            capacity_match = re.search(cap_pattern, text, re.IGNORECASE)
                            if capacity_match:
                                capacity = int(capacity_match.group(1))
                                cap_found = True
                                if debug:
                                    print(f"      DEBUG {label}: Census={census}, Capacity={capacity} (via {cap_label})")
                                break
                        if not cap_found and debug:
                            print(f"      DEBUG {label}: Census={census}, Capacity={capacity} (not found)")
                        break
                    patterns_tried.append(label)
                else:
                    # Pattern 8-11: "# of Present" or "# Present" formats
                    num_present_patterns = [
                        (r"#\s*of\s*Present\s+(\d+)(?:\s|$)", "P8-#ofPresent"),
                        (r"#\s*Present\s+(\d+)(?:\s|$)", "P9-#Present"),
                        (r"Approved.*?Present\s+\d+\s+(\d+)", "P10-Skip1st"),
                        (r"(?:number|count|total)\s+present[:\s]*(\d+)", "P11-NumPresent"),
                    ]

                    for pattern, label in num_present_patterns:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            census = int(match.group(1))
                            # Try comprehensive capacity patterns
                            capacity_patterns = [
                                (r"(?:Licensed\s+)?Capacity\s*\n\s*(\d+)", "Cap\\n"),
                                (r"(?:Licensed\s+|Approved\s+|Max(?:imum)?\s+)?Capacity\s*[:;\s]+(\d+)", "Cap:"),
                                (r"(?:Licensed\s+)?Capacity\s*[|]\s*(\d+)", "Cap|"),
                                (r"(?:Licensed\s+|Approved\s+|Max(?:imum)?\s+)?Capacity\s+(\d+)", "Cap_"),
                                (r"(?:Licensed|Approved|Max|Maximum)?\s*Capacity.*?(\d+)", "CapAny"),
                                (r"Approved\s*#\s*\n\s*\d+\s+(\d+)", "ApprAlt"),
                            ]
                            cap_found = False
                            for cap_pattern, cap_label in capacity_patterns:
                                capacity_match = re.search(cap_pattern, text, re.IGNORECASE)
                                if capacity_match:
                                    capacity = int(capacity_match.group(1))
                                    cap_found = True
                                    break
                            if debug:
                                cap_status = f"(via {cap_label})" if cap_found else "(not found)"
                                print(f"      DEBUG {label}: Census={census}, Capacity={capacity} {cap_status}")
                            break
                        patterns_tried.append(label)
                    else:
                        # Pattern 12: Look for "census" keyword
                        census_kw = re.search(r"census[:\s]*(\d+)", text, re.IGNORECASE)
                        if census_kw:
                            census = int(census_kw.group(1))
                            # Try comprehensive capacity patterns
                            cap_patterns = [
                                (r"(?:Licensed\s+)?Capacity\s*\n\s*(\d+)", "Cap\\n"),
                                (r"(?:Licensed\s+|Approved\s+|Max(?:imum)?\s+)?Capacity\s*[:;\s]+(\d+)", "Cap:"),
                                (r"(?:Licensed\s+)?Capacity\s*[|]\s*(\d+)", "Cap|"),
                                (r"(?:Licensed\s+|Approved\s+|Max(?:imum)?\s+)?Capacity\s+(\d+)", "Cap_"),
                                (r"(?:Licensed|Approved|Max|Maximum)?\s*Capacity.*?(\d+)", "CapAny"),
                            ]
                            cap_found = False
                            for cap_pat, cap_lbl in cap_patterns:
                                cap_match = re.search(cap_pat, text, re.IGNORECASE)
                                if cap_match:
                                    capacity = int(cap_match.group(1))
                                    cap_found = True
                                    break
                            if debug:
                                cap_status = f"(via {cap_lbl})" if cap_found else "(not found)"
                                print(f"      DEBUG P12-Census: Census={census}, Capacity={capacity} {cap_status}")
                        else:
                            patterns_tried.append("P12-Census")

                            # Pattern 13: Fallback - find "present" and take the first number near it
                            present_nearby = re.search(r"present.{0,20}?(\d+)", text, re.IGNORECASE | re.DOTALL)
                            if present_nearby:
                                census = int(present_nearby.group(1))
                                # Try comprehensive capacity patterns
                                cap_patterns = [
                                    (r"(?:Licensed\s+)?Capacity\s*\n\s*(\d+)", "Cap\\n"),
                                    (r"(?:Licensed\s+|Approved\s+|Max(?:imum)?\s+)?Capacity\s*[:;\s]+(\d+)", "Cap:"),
                                    (r"(?:Licensed\s+)?Capacity\s*[|]\s*(\d+)", "Cap|"),
                                    (r"(?:Licensed\s+|Approved\s+|Max(?:imum)?\s+)?Capacity\s+(\d+)", "Cap_"),
                                    (r"(?:Licensed|Approved|Max|Maximum)?\s*Capacity.*?(\d+)", "CapAny"),
                                ]
                                cap_found = False
                                for cap_pat, cap_lbl in cap_patterns:
                                    cap_match = re.search(cap_pat, text, re.IGNORECASE)
                                    if cap_match:
                                        capacity = int(cap_match.group(1))
                                        cap_found = True
                                        break
                                if debug:
                                    cap_status = f"(via {cap_lbl})" if cap_found else "(not found)"
                                    print(f"      DEBUG P13-Nearby: Census={census}, Capacity={capacity} {cap_status}")
                            else:
                                patterns_tried.append("P13-Nearby")
                                if debug:
                                    print(f"      DEBUG: No pattern matched. Tried: {', '.join(patterns_tried)}")
                                    # Show text sample to help debug
                                    lines = text.split('\n')[:15]
                                    print(f"      DEBUG: First 15 lines:\n      " + "\n      ".join(lines))

    contact_patterns = [
        r"Name of Individual Informed.*?Inspection:?\s*([^\n\r]+)",
        r"Individual Informed.*?:?\s*([A-Za-z][^\n\r]*)",
    ]
    for pattern in contact_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            contact_person = re.sub(r"\s+", " ", match.group(1).strip())
            break

    licensor_patterns = [
        r"Licensor\(?s?\)?\s*Conducting.*?Inspection:?\s*([^\n\r]+?)(?:\s+OL Staff|$)",
        r"Licensor.*?:?\s*([A-Za-z][^\n\r]*?)(?:\s+OL Staff|$)",
    ]
    for pattern in licensor_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            licensor = re.sub(r"\s+", " ", match.group(1).strip())
            break

    # Validation: Flag suspicious cases where census > capacity
    validation_warning = None
    if census is not None and capacity is not None:
        if census > capacity:
            validation_warning = f"⚠️ SUSPICIOUS: Census ({census}) > Capacity ({capacity})"
            if debug:
                print(f"      {validation_warning}")

    result = {
        "census": census,
        "capacity": capacity,
        "contact_person": contact_person,
        "licensor": licensor
    }
    if validation_warning:
        result["validation_warning"] = validation_warning

    return result


def extract_checklist_data(pdf_content: bytes, checklist_id: Optional[int] = None) -> ChecklistResult:
    """Extract data with EasyOCR fallback ONLY when regular extraction completely fails."""

    try:
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            if not pdf.pages:
                return {
                    "census": None,
                    "capacity": None,
                    "contact_person": None,
                    "licensor": None,
                    "extraction_method": "no_pages",
                }

            first_page = pdf.pages[0]
            text = first_page.extract_text() or ""

            # Save extracted text for debugging
            if SAVE_EXTRACTED_TEXT and checklist_id and text.strip():
                text_file = TEXT_OUTPUT_DIR / f"checklist_{checklist_id}_text.txt"
                text_file.write_text(text, encoding="utf-8")

            if text.strip():
                result = extract_data_from_text(text, method="text", debug=DEBUG_EXTRACTION)
                if any(result.values()):
                    result["extraction_method"] = "text"
                    return result

            print("      Regular extraction failed completely, trying OCR...")

            if EASYOCR_AVAILABLE:
                try:
                    if not hasattr(extract_checklist_data, "_reader"):
                        print("      Initializing EasyOCR...")
                        extract_checklist_data._reader = easyocr.Reader(["en"])  # type: ignore[attr-defined]

                    images = convert_from_bytes(pdf_content, first_page=1, last_page=1, dpi=300)
                    if images:
                        import numpy as np

                        pil_image = images[0]
                        rotations = [0, 90, 180, 270]
                        best_result: Optional[ChecklistResult] = None
                        best_text = ""
                        best_angle = 0

                        for angle in rotations:
                            rotated_img = pil_image.rotate(angle, expand=True)
                            img_array = np.array(rotated_img)

                            results = extract_checklist_data._reader.readtext(img_array)  # type: ignore[attr-defined]
                            ocr_text = " ".join(result[1] for result in results)

                            if len(ocr_text) > len(best_text):
                                best_text = ocr_text
                                best_angle = angle

                                test_result = extract_data_from_text(ocr_text, method="easyocr", debug=DEBUG_EXTRACTION)
                                if any(test_result.values()):
                                    best_result = test_result
                                    break

                        if best_text.strip():
                            print(
                                f"      EasyOCR extracted {len(best_text)} characters (rotation: {best_angle}\N{DEGREE SIGN})"
                            )

                            # Save OCR extracted text for debugging
                            if SAVE_EXTRACTED_TEXT and checklist_id:
                                ocr_file = TEXT_OUTPUT_DIR / f"checklist_{checklist_id}_ocr.txt"
                                ocr_file.write_text(best_text, encoding="utf-8")

                            result = best_result or extract_data_from_text(best_text, method="easyocr", debug=DEBUG_EXTRACTION)
                            result["extraction_method"] = f"easyocr_rotated_{best_angle}"
                            return result
                        print("      EasyOCR found no text at any rotation")
                except Exception as ocr_error:  # pragma: no cover - logging only
                    print(f"      EasyOCR failed: {ocr_error}")

            return {
                "census": None,
                "capacity": None,
                "contact_person": None,
                "licensor": None,
                "extraction_method": "all_failed",
            }
    except Exception as error:  # pragma: no cover - logging only
        print(f"      Error parsing PDF: {error}")
        return {
            "census": None,
            "capacity": None,
            "contact_person": None,
            "licensor": None,
            "extraction_method": "error",
        }


def download_with_retry(url: str, max_attempts: int = 3, timeout: int = 30) -> Optional[requests.Response]:
    """Download with retry logic and longer timeout."""

    for attempt in range(max_attempts):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.ReadTimeout:
            print(f"      Timeout on attempt {attempt + 1}/{max_attempts}")
            if attempt == max_attempts - 1:
                return None
            time.sleep(5)
        except requests.exceptions.RequestException as error:
            print(f"      Request failed on attempt {attempt + 1}/{max_attempts}: {error}")
            if attempt == max_attempts - 1:
                return None
            time.sleep(2)
    return None


def fetch_facility_data(facility_id: int) -> Optional[Dict[str, Any]]:
    """Fetch JSON data for a single facility with robust error handling."""

    url = f"https://ccl.utah.gov/ccl/public/facilities/{facility_id}.json"
    response = download_with_retry(url)
    if response:
        print(f"🔍 Fetching {facility_id}... ✅ Success")
        return response.json()
    print(f"🔍 Fetching {facility_id}... ❌ Failed after retries")
    return None


def format_address(address: Dict[str, Any]) -> str:
    """Convert address dict to single string."""

    parts = [
        address.get("addressOne", ""),
        address.get("city", ""),
        address.get("state", ""),
        address.get("zipCode", ""),
    ]
    return ", ".join(filter(None, parts))


def build_checklist_summary(checklists: List[ChecklistResult]) -> str:
    """Create a human-readable summary for checklist data."""

    summaries = []
    for checklist in checklists:
        parts = []
        checklist_id = checklist.get("checklist_id")
        if checklist_id is not None:
            parts.append(str(checklist_id))
        census = checklist.get("census")
        if census is not None:
            parts.append(f"Census={census}")
        capacity = checklist.get("capacity")
        if capacity is not None:
            parts.append(f"Capacity={capacity}")
        contact = checklist.get("contact_person")
        if contact:
            parts.append(f"Contact={contact}")
        licensor = checklist.get("licensor")
        if licensor:
            parts.append(f"Licensor={licensor}")
        method = checklist.get("extraction_method")
        if method:
            parts.append(f"Method={method}")
        warning = checklist.get("validation_warning")
        if warning:
            parts.append(f"WARNING: {warning}")
        if parts:
            summaries.append("; ".join(parts))
    return " | ".join(summaries)


def write_csv_output(facilities_data: List[FacilityRecord]) -> None:
    """Write a flattened CSV summary for easy spreadsheet review."""

    headers = [
        "Facility ID",
        "Name",
        "Address",
        "Regulation Date",
        "Expiration Date",
        "Conditional",
    ]
    for index in range(1, MAX_INSPECTIONS + 1):
        headers.extend(
            [
                f"Inspection {index} Date",
                f"Inspection {index} Type",
                f"Inspection {index} Findings",
                f"Inspection {index} Checklists",
            ]
        )

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

        for facility in facilities_data:
            row: List[str] = [
                facility.get("facility_id", ""),
                facility.get("name", ""),
                facility.get("address", ""),
                facility.get("regulation_date", ""),
                facility.get("expiration_date", ""),
                facility.get("conditional", ""),
            ]

            inspections = facility.get("inspections", [])[:MAX_INSPECTIONS]
            for inspection in inspections:
                findings = [
                    f"{finding.get('rule_number', '?')}: {finding.get('rule_description', '')} | Finding: {finding.get('finding_text', '')}"
                    for finding in inspection.get("findings", [])
                    if any(finding.values())
                ]
                inspection_types = inspection.get("inspection_types", "")
                if isinstance(inspection_types, list):
                    inspection_types = ", ".join(inspection_types)

                row.extend(
                    [
                        inspection.get("inspection_date", ""),
                        inspection_types,
                        " || ".join(findings) if findings else "None",
                        build_checklist_summary(inspection.get("checklists", [])),
                    ]
                )

            while len(row) < len(headers):
                row.append("")

            writer.writerow(row)


def main() -> None:
    facility_list = FACILITY_IDS[:3] if TEST_MODE else FACILITY_IDS
    mode_str = " (TEST MODE - first 3 facilities only)" if TEST_MODE else ""
    print(f"🚀 Starting data export with OCR fallback ({len(facility_list)} facilities){mode_str}")

    facilities_data: List[FacilityRecord] = []

    for facility_id in facility_list:
        data = fetch_facility_data(facility_id)
        if not data:
            continue

        facility_record: FacilityRecord = {
            "facility_id": facility_id,
            "name": data.get("name", ""),
            "address": format_address(data.get("address", {})),
            "regulation_date": data.get("initialRegulationDate", ""),
            "expiration_date": data.get("expirationDate", ""),
            "conditional": data.get("conditional", False),
            "inspections": [],
        }

        inspections = data.get("inspections", [])[:MAX_INSPECTIONS]
        for inspection in inspections:
            findings = [
                {
                    "rule_number": finding.get("ruleNumber", ""),
                    "rule_description": finding.get("ruleDescription", ""),
                    "finding_text": finding.get("findingText", ""),
                }
                for finding in inspection.get("findings", [])
            ]

            inspection_record: FacilityRecord = {
                "inspection_date": inspection.get("inspectionDate", ""),
                "inspection_types": inspection.get("inspectionTypes", ""),
                "findings": findings,
                "checklists": [],
            }

            checklist_ids = inspection.get("checklistIds", [])
            print(f"  Found {len(checklist_ids)} checklists to process")

            for checklist_id in checklist_ids:
                try:
                    pdf_url = f"https://ccl.utah.gov/ccl/public/checklist/{checklist_id}?dl=1"
                    pdf_response = download_with_retry(pdf_url)

                    if pdf_response:
                        checklist_data = extract_checklist_data(pdf_response.content, checklist_id=checklist_id)
                        checklist_data["checklist_id"] = checklist_id

                        pdf_path = CHECKLIST_DIR / f"facility_{facility_id}_checklist_{checklist_id}.pdf"
                        pdf_path.write_bytes(pdf_response.content)
                        checklist_data["pdf_file"] = str(pdf_path)

                        inspection_record["checklists"].append(checklist_data)

                        # Build output message with census, capacity, and warnings
                        msg_parts = [f"📋 Checklist {checklist_id}"]
                        msg_parts.append(f"Census={checklist_data.get('census')}")
                        msg_parts.append(f"Capacity={checklist_data.get('capacity')}")
                        msg_parts.append(f"Method={checklist_data.get('extraction_method', 'unknown')}")

                        # Add warning if present
                        if checklist_data.get("validation_warning"):
                            msg_parts.append(checklist_data.get("validation_warning"))

                        print("    " + ", ".join(msg_parts))
                    else:
                        print(f"    ❌ Failed to download checklist {checklist_id} after retries")
                except Exception as error:  # pragma: no cover - logging only
                    print(f"    ❌ Error with checklist {checklist_id}: {error}")

            facility_record["inspections"].append(inspection_record)

        facilities_data.append(facility_record)
        time.sleep(REQUEST_DELAY)

    OUTPUT_JSON.write_text(json.dumps(facilities_data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n🎉 Done! Data saved to {OUTPUT_JSON}")
    print(f"📊 Exported {len(facilities_data)} facilities")

    write_csv_output(facilities_data)
    print(f"📑 CSV summary saved to {OUTPUT_CSV}")
    print("💡 Pro tip: Use a JSON viewer or 'python -m json.tool' for pretty printing")


if __name__ == "__main__":
    main()
