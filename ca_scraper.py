"""
California Community Care Licensing Report Parser
Complete version with all continuation logic properly integrated
"""

import re
import json
import time
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Any

def smart_title_case(text):
    """Convert text to title case while preserving acronyms and handling exceptions"""
    if not text:
        return ""
    
    # Words that should stay lowercase (except at beginning of sentence)
    lowercase_words = {
        'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'from', 'in', 'into', 'is',
        'of', 'on', 'or', 'the', 'to', 'with', 'within', 'without', 'per', 'via',
        'vs', 'upon', 'under', 'over', 'through', 'between', 'among', 'across'
    }
    
    # Common acronyms and abbreviations that should stay uppercase
    uppercase_words = {
        'LLC', 'INC', 'CORP', 'LTD', 'LP', 'LLP', 'PC', 'PA',  # Business
        'MD', 'DO', 'RN', 'LVN', 'CNA', 'MSW', 'LCSW', 'PhD', 'DDS', 'DVM',  # Medical/Professional
        'CCR', 'CFR', 'USC', 'ILS', 'HSC', 'WIC',  # Legal codes
        'ID', 'SSN', 'DOB', 'POC', 'FAQ', 'URL', 'API', 'HTML', 'PDF',  # Technical
        'US', 'USA', 'UK', 'CA', 'NY', 'TX', 'FL',  # Geographic
        'AM', 'PM', 'EST', 'PST', 'GMT', 'UTC',  # Time
        'CEO', 'CFO', 'COO', 'CTO', 'VP', 'HR', 'IT', 'QA', 'PR',  # Corporate titles
        'FBI', 'CIA', 'NSA', 'FDA', 'CDC', 'OSHA', 'EPA', 'FTC'  # Government agencies
    }
    
    # Split into sentences to handle capitalization properly
    sentences = re.split(r'([.!?]+\s*)', text)
    result_sentences = []
    
    for sentence in sentences:
        if not sentence.strip():
            result_sentences.append(sentence)
            continue
            
        # Check if this is likely all caps text that needs conversion
        if sentence.isupper() and len([c for c in sentence if c.isalpha()]) > 3:
            # Split into words
            words = sentence.split()
            result_words = []
            
            for i, word in enumerate(words):
                # Preserve punctuation
                leading_punct = re.match(r'^[^\w]*', word).group()
                trailing_punct = re.search(r'[^\w]*$', word).group()
                core_word = word[len(leading_punct):len(word)-len(trailing_punct) if trailing_punct else len(word)]
                
                if not core_word:
                    result_words.append(word)
                    continue
                
                # Check if it's an acronym (preserve uppercase)
                if core_word.upper() in uppercase_words:
                    result_words.append(leading_punct + core_word.upper() + trailing_punct)
                # Check if it's a lowercase word (but capitalize if first word)
                elif core_word.lower() in lowercase_words and i > 0:
                    result_words.append(leading_punct + core_word.lower() + trailing_punct)
                # Regular title case
                else:
                    result_words.append(leading_punct + core_word.capitalize() + trailing_punct)
            
            result_sentences.append(' '.join(result_words))
        else:
            result_sentences.append(sentence)
    
    return ''.join(result_sentences)

def clean_text(text):
    """Clean text and apply smart title casing to all-caps content"""
    if not text:
        return ""
    
    # Handle common UTF-8 artifacts
    text = text.replace('\xa0', ' ')  # Non-breaking space
    text = text.replace('\u2019', "'")  # Right single quote
    text = text.replace('\u2018', "'")  # Left single quote
    text = text.replace('\u201c', '"')  # Left double quote
    text = text.replace('\u201d', '"')  # Right double quote
    text = text.replace('\u2013', '-')  # En dash
    text = text.replace('\u2014', '-')  # Em dash
    text = text.replace('\u2026', '...')  # Ellipsis
    
    # Handle mangled UTF-8 (these show up when UTF-8 is incorrectly decoded as Latin-1)
    text = text.replace('Ã¢â¬Â¦', '...')  # Mangled ellipsis
    text = text.replace('Ã¢â¬â¢', "'")  # Mangled apostrophe
    text = text.replace('â€™', "'")  # Another mangled apostrophe
    text = text.replace('â€œ', '"')  # Mangled left quote
    text = text.replace('â€', '"')  # Mangled right quote
    text = text.replace('â€"', '-')  # Mangled em dash
    text = text.replace('â€"', '-')  # Mangled en dash
    text = text.replace('Â', '')
    
    # Apply smart title casing
    text = smart_title_case(text)
    
    return text

class CaliforniaCCLParser:
    """Parser for California Community Care Licensing facility reports"""
    
    def __init__(self, facility_ids: List[str]):
        """Initialize with list of facility IDs to process"""
        self.facility_ids = [fid for fid in facility_ids if fid]
        self.base_url = "https://www.ccld.dss.ca.gov/transparencyapi/api/FacilityReports"
    
    def fetch_reports(self, facility_id: str, max_reports: int = 50) -> List[Dict[str, Any]]:
        """Fetch all reports for a facility using index pagination"""
        reports = []
        index = 0
        consecutive_errors = 0
        
        print(f"Starting to fetch reports for facility {facility_id}")
        
        while index < max_reports and consecutive_errors < 3:
            try:
                url = f"{self.base_url}?facNum={facility_id}&inx={index}"
                response = requests.get(url, timeout=30)
                
                if response.status_code == 404 or not response.text.strip():
                    break
                
                if response.status_code == 200:
                    parsed = self.parse_report(response.text)
                    if parsed and parsed.get('facility_number'):
                        parsed['facility_id'] = facility_id
                        parsed['report_index'] = index
                        reports.append(parsed)
                        consecutive_errors = 0
                        index += 1
                    else:
                        consecutive_errors += 1
                        index += 1
                else:
                    consecutive_errors += 1
                    index += 1
                        
            except Exception as e:
                print(f"  Exception at index {index}: {e}")
                consecutive_errors += 1
                index += 1
                
                if consecutive_errors >= 3:
                    break
        
        print(f"  Found {len(reports)} reports")
        return reports
    
    # Helper methods for continuation handling
    def _is_line_numbers_row(self, text):
        """Check if text is just line numbers (1 2 3 4 5 6 7 8 9)"""
        cleaned = clean_text(text.strip())
        return bool(re.match(r'^[\d\s]+$', cleaned) and len(cleaned) < 50)
    
    def _check_for_continuation(self, text):
        """Check if text indicates content continues"""
        if not text:
            return False
        
        continuation_markers = [
            'CONTINUED ON NEXT PAGE',
            '****CONTINUED',
            'Continued on next page',
            'as evidenced by:',
            'as follows:',
            'This requirement is not met:',
            'Plan of Correction:'
        ]
        
        text_upper = text.upper()
        for marker in continuation_markers:
            if marker.upper() in text_upper:
                # Check if marker is at the end or followed by little content
                if text.rstrip().upper().endswith(marker.upper()):
                    return True
                # Check if it's near the end
                if marker.upper() in text[-100:].upper():
                    # Make sure there's not much content after it
                    after_marker = text.upper().split(marker.upper())[-1]
                    if len(after_marker.strip()) < 50:
                        return True
        return False
    
    def _merge_continued_content(self, parts: List[str]) -> str:
        """Merge content parts from continued sections"""
        if not parts:
            return ""
        
        merged = ' '.join(parts)
        
        # Clean up artifacts from page breaks
        merged = re.sub(r'\*{4}CONTINUED.*?(?:PAGE|page).*?\d+-C', '', merged, flags=re.IGNORECASE)
        merged = re.sub(r'Continued on next page', '', merged, flags=re.IGNORECASE)
        merged = re.sub(r'See next page', '', merged, flags=re.IGNORECASE)
        
        # Clean up duplicate spaces and line numbers
        merged = re.sub(r'\s+', ' ', merged)
        merged = re.sub(r'\s*\d+\s+(?=[A-Z])', ' ', merged)  # Remove orphaned line numbers
        
        return merged.strip()
    
    def parse_report(self, html_content: str) -> Dict[str, Any]:
        """Parse a single report HTML document"""
        soup = BeautifulSoup(html_content, 'html.parser')
        text = clean_text(soup.get_text())
        
        if "FACILITY EVALUATION REPORT" in text:
            report_type = "Facility Evaluation"
            form_number = "LIC809"
        elif "COMPLAINT INVESTIGATION REPORT" in text:
            report_type = "Complaint Investigation"
            form_number = "LIC9099"
        else:
            return {"error": "Unknown report type"}
            
        data = { "report_type": report_type, "form_number": form_number }
        
        data.update(self._extract_header(text))
        data.update(self._extract_facility_info(soup))
        data.update(self._extract_visit_info(text))
        data.update(self._extract_personnel(text))
        
        narrative_content = self._extract_narrative_content(soup, text)
        if narrative_content:
            data.update(narrative_content)
        
        if report_type == "Complaint Investigation":
            data.update(self._extract_complaint_info(soup, text))
        
        deficiencies = self._extract_deficiencies(soup)
        if deficiencies:
            data["deficiencies"] = deficiencies
        return data
    
    def _extract_header(self, text: str) -> Dict[str, Any]:
        """Extract header information"""
        data = {}
        match = re.search(r'Facility Number:\s*(\d+)', text)
        if match: data["facility_number"] = match.group(1)
        match = re.search(r'Report Date:.*?(\d{2}/\d{2}/\d{4})', text)
        if match: data["report_date"] = match.group(1)
        match = re.search(r'Date Signed:.*?(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\s+[AP]M)', text)
        if match: data["date_signed"] = match.group(1)
        return data
    
    def _extract_facility_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract facility information"""
        data = {}
        text_with_newlines = clean_text(soup.get_text(separator='\n'))

        name_pattern = r'FACILITY NAME:.*?\n(.*?)\n'
        name_match = re.search(name_pattern, text_with_newlines, re.MULTILINE)
        if name_match:
            data["facility_name"] = name_match.group(1).strip()

        admin_pattern = r'ADMINISTRATOR:.*?\n(.*?)\n'
        admin_match = re.search(admin_pattern, text_with_newlines, re.MULTILINE)
        if admin_match:
            data["administrator"] = admin_match.group(1).strip()

        type_pattern = r'FACILITY TYPE:\s*(\d+)'
        type_match = re.search(type_pattern, text_with_newlines)
        if type_match:
            type_code = type_match.group(1)
            data["facility_type_code"] = type_code
            data["facility_type_name"] = self._decode_facility_type(type_code)

        capacity_pattern = r'CAPACITY:\s*(\d+)'
        capacity_match = re.search(capacity_pattern, text_with_newlines)
        if capacity_match:
            data["capacity"] = int(capacity_match.group(1))

        census_pattern = r'CENSUS:\s*(\d+)'
        census_match = re.search(census_pattern, text_with_newlines)
        if census_match:
            data["census"] = int(census_match.group(1))
        return data
    
    def _extract_visit_info(self, text: str) -> Dict[str, Any]:
        """Extract visit information"""
        data = {}
        
        visit_types = ["Prelicensing", "Case Management", "Complaint Investigation", 
                      "Annual", "Renewal", "Case Management - Other"]
        for vtype in visit_types:
            if vtype in text:
                data["visit_type"] = vtype
                break
        
        match = re.search(r'(?:VISIT )?DATE:\s*(\d{2}/\d{2}/\d{4})', text)
        if match:
            data["visit_date"] = match.group(1)
        
        if "Unannounced" in text:
            data["announced_status"] = "Unannounced"
        elif "Announced" in text:
            data["announced_status"] = "Announced"
        
        match = re.search(r'TIME BEGAN:\s*(\d{1,2}:\d{2}\s*[AP]M)', text)
        if match:
            data["time_began"] = match.group(1)
        
        match = re.search(r'TIME COMPLETED:\s*(\d{1,2}:\d{2}\s*[AP]M)', text)
        if match:
            data["time_completed"] = match.group(1)
        
        match = re.search(r'MET WITH:(.*?)(?:TIME|$)', text, re.DOTALL)
        if match:
            met_with = re.sub(r'\s+', ' ', match.group(1)).strip()
            data["met_with"] = met_with
        return data
    
    def _extract_personnel(self, text: str) -> Dict[str, Any]:
        """Extract personnel information"""
        data = {}
        
        match = re.search(r"SUPERVISOR'S NAME:\s*(.*?)(?:TELEPHONE|$)", text)
        if match:
            data["supervisor_name"] = match.group(1).strip()
        
        match = re.search(r"LICENSING EVALUATOR NAME:\s*(.*?)(?:TELEPHONE|$)", text)
        if match:
            data["evaluator_name"] = match.group(1).strip()
        
        return data
    
    def _extract_narrative_content(self, soup: BeautifulSoup, text: str) -> Dict[str, Any]:
        """Extract narrative content with continuation handling"""
        data = {}
        narrative_parts = []
        expecting_continuation = False
        
        for table in soup.find_all('table'):
            if 'NARRATIVE' in table.get_text():
                rows = table.find_all('tr')
                
                for row in rows:
                    row_text = clean_text(row.get_text())
                    
                    # Check for continuation
                    if self._check_for_continuation(row_text):
                        expecting_continuation = True
                    
                    for td in row.find_all('td'):
                        td_text = clean_text(td.get_text().strip())
                        
                        # Skip line numbers
                        if self._is_line_numbers_row(td_text):
                            continue
                        
                        if (len(td_text) > 50 and 
                            not re.match(r'^[\d\s]+$', td_text) and
                            'NARRATIVE' not in td_text):
                            # Remove leading numbers
                            td_text = re.sub(r'^\d{1,3}\s+', '', td_text)
                            narrative_parts.append(td_text)
        
        if narrative_parts:
            data["narrative"] = self._merge_continued_content(narrative_parts)
        return data
    
    def _extract_allegation_summaries(self, soup: BeautifulSoup) -> List[str]:
        """Extract allegations with continuation handling"""
        all_allegation_parts = []
        
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            in_allegation_section = False
            expecting_continuation = False
            
            for i, row in enumerate(rows):
                row_text = clean_text(row.get_text().strip())
                
                # Check for continuation
                if self._check_for_continuation(row_text):
                    expecting_continuation = True
                
                # Look for ALLEGATION header - updated to handle (S): format
                if ('ALLEGATION' in row_text.upper() and 
                    (':' in row_text or '(S):' in row_text.upper())):
                    in_allegation_section = True
                    
                    # Check if next row is line numbers
                    if i + 1 < len(rows):
                        next_row_text = clean_text(rows[i + 1].get_text().strip())
                        if self._is_line_numbers_row(next_row_text):
                            i += 1  # Skip line numbers row
                    continue
                
                if in_allegation_section:
                    # Skip line numbers
                    if self._is_line_numbers_row(row_text):
                        continue
                    
                    # Stop at investigation findings (unless expecting continuation)
                    if not expecting_continuation and 'INVESTIGATION' in row_text.upper():
                        in_allegation_section = False
                        continue
                    
                    # Extract allegation text
                    cells = row.find_all('td')
                    for cell in cells:
                        cell_text = clean_text(cell.get_text().strip())
                        
                        if self._is_line_numbers_row(cell_text):
                            continue
                        
                        # Remove leading numbers but preserve the allegation text
                        cell_text = re.sub(r'^\s*\d{1,3}\s+', '', cell_text)
                        
                        if cell_text and len(cell_text) > 5:  # Lowered threshold
                            # Skip common non-allegations but keep actual allegation content
                            skip_keywords = ['INVESTIGATION FINDINGS', 'SUPERVISOR', 'TELEPHONE', 
                                           'LICENSING EVALUATOR', 'TIME BEGAN', 'TIME COMPLETED']
                            if not any(kw in cell_text.upper() for kw in skip_keywords):
                                # Check if this looks like an allegation (not status words)
                                if not cell_text.upper() in ['SUBSTANTIATED', 'unsubstantiated']:
                                    all_allegation_parts.append(cell_text)
                                    
                                    if self._check_for_continuation(cell_text):
                                        expecting_continuation = True
        
        # Process and clean allegations
        unique_allegations = []
        if all_allegation_parts:
            # Join all parts and then split on natural breaks
            full_text = ' '.join(all_allegation_parts)
            
            # Split on common patterns that indicate separate allegations
            # Look for patterns like "1. text 2. text" or separate sentences
            allegations = []
            
            # Try splitting on numbered patterns first
            numbered_pattern = re.split(r'\s+\d+\s+(?=[A-Za-z])', full_text)
            if len(numbered_pattern) > 1:
                allegations = numbered_pattern
            else:
                # If no clear numbering, split on sentence boundaries for long text
                if len(full_text) > 100:
                    allegations = re.split(r'(?<=[.!?])\s+(?=[A-Z])', full_text)
                else:
                    allegations = [full_text]
            
            for allegation in allegations:
                allegation = allegation.strip()
                # Clean up any remaining artifacts
                allegation = re.sub(r'^\d+\s*', '', allegation)  # Remove leading numbers
                allegation = re.sub(r'\s+', ' ', allegation)     # Normalize whitespace
                
                if allegation and len(allegation) > 10:
                    # Avoid duplicates
                    if allegation not in unique_allegations:
                        unique_allegations.append(allegation)
        
        return unique_allegations

    def _extract_investigation_findings(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract investigation findings with proper continuation handling"""
        all_findings_parts = []
        
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            in_findings_section = False
            expecting_continuation = False
            
            for i, row in enumerate(rows):
                row_text = clean_text(row.get_text().strip())
                
                # Check for continuation markers in current row
                if self._check_for_continuation(row_text):
                    expecting_continuation = True
                
                # Look for INVESTIGATION FINDINGS header
                if 'INVESTIGATION FINDINGS' in row_text.upper():
                    in_findings_section = True
                    continue
                
                if in_findings_section:
                    # Skip line numbers rows
                    if self._is_line_numbers_row(row_text):
                        continue
                    
                    # Check for section boundaries - but be more specific
                    stop_markers = ['SUPERVISOR\'S NAME:', 'LICENSING EVALUATOR NAME:', 
                                  'NARRATIVE', 'DEFICIENCY INFORMATION']
                    should_stop = False
                    for marker in stop_markers:
                        if marker in row_text.upper():
                            # Only stop if NOT expecting continuation
                            if not expecting_continuation:
                                should_stop = True
                                break
                    
                    if should_stop:
                        in_findings_section = False
                        expecting_continuation = False
                        continue
                    
                    # Extract findings text from cells
                    cells = row.find_all('td')
                    for cell in cells:
                        cell_text = clean_text(cell.get_text().strip())
                        
                        # Skip line numbers
                        if self._is_line_numbers_row(cell_text):
                            continue
                        
                        # Remove leading numbers but keep the content
                        cell_text = re.sub(r'^\s*\d{1,3}\s+', '', cell_text)
                        
                        if cell_text and len(cell_text) > 20:  # Meaningful content threshold
                            # Skip obvious non-findings content
                            skip_content = ['Substantiated Estimated Days', 'SUPERVISOR\'S NAME',
                                          'LICENSING EVALUATOR', 'TELEPHONE']
                            if not any(skip in cell_text for skip in skip_content):
                                all_findings_parts.append(cell_text)
                                
                                # Check this cell for continuation
                                if self._check_for_continuation(cell_text):
                                    expecting_continuation = True
        
        # Also check text-based extraction for findings as fallback
        text = clean_text(soup.get_text())
        if "INVESTIGATION FINDINGS:" in text and not all_findings_parts:
            start_marker = "INVESTIGATION FINDINGS:"
            start_pos = text.find(start_marker)
            if start_pos != -1:
                start_pos += len(start_marker)
                
                # Find end position - look for supervisor section or narrative
                end_markers = ["SUPERVISOR'S NAME:", "Substantiated Estimated Days", 
                              "NARRATIVE", "DEFICIENCY INFORMATION"]
                end_pos = len(text)
                for marker in end_markers:
                    marker_pos = text.find(marker, start_pos)
                    if marker_pos != -1 and marker_pos < end_pos:
                        end_pos = marker_pos
                
                raw_text = text[start_pos:end_pos]
                lines = raw_text.split('\n')
                
                findings_text = []
                for line in lines:
                    cleaned_line = re.sub(r'^\s*\d{1,3}\s+', '', line)
                    cleaned_line = cleaned_line.strip()
                    
                    if cleaned_line and not cleaned_line.isdigit() and len(cleaned_line) > 20:
                        if 'CONTINUED ON NEXT PAGE' not in cleaned_line.upper():
                            findings_text.append(cleaned_line)
                
                if findings_text:
                    all_findings_parts.extend(findings_text)
        
        if all_findings_parts:
            return self._merge_continued_content(all_findings_parts)
        return None
    
    def _extract_complaint_info(self, soup: BeautifulSoup, text: str) -> Dict[str, Any]:
        """Extract complaint-specific information"""
        data = {}
        text = clean_text(text)
        
        match = re.search(r'COMPLAINT CONTROL NUMBER:\s*([\w\-]+)', text)
        if match:
            data["complaint_control_number"] = match.group(1)
        
        if "Substantiated" in text and "Unsubstantiated" not in text:
            data["complaint_status"] = "SUBSTANTIATED"
        elif "Unsubstantiated" in text:
            data["complaint_status"] = "unsubstantiated"
        
        match = re.search(r'complaint received.*?on\s*(\d{2}/\d{2}/\d{4})', text, re.IGNORECASE)
        if match:
            data["complaint_received_date"] = match.group(1)
        
        findings = self._extract_investigation_findings(soup)
        if findings:
            data["investigation_findings"] = findings
        
        allegations = self._extract_allegation_summaries(soup)
        if allegations:
            data["allegations"] = allegations
        
        return data
    
    def _extract_deficiencies(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract deficiencies with POCs handling continuations"""
        deficiencies = []
        text = clean_text(soup.get_text())
        
        for table in soup.find_all('table'):
            table_text = table.get_text()
            if "DEFICIENCIES" in table_text and "PLAN OF CORRECTION" in table_text:
                rows = table.find_all('tr')
                
                current_deficiency = {}
                deficiency_text = []
                poc_text = []
                expecting_def_continuation = False
                expecting_poc_continuation = False
                
                header_row = None
                deficiency_col_idx = None
                poc_col_idx = None
                
                for i, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'])
                    
                    # Identify header row
                    if any('DEFICIENCIES' in cell.get_text() for cell in cells):
                        header_row = i
                        for idx, cell in enumerate(cells):
                            cell_text = clean_text(cell.get_text().upper())
                            if 'DEFICIENCIES' in cell_text:
                                deficiency_col_idx = idx
                            elif 'PLAN OF CORRECTION' in cell_text:
                                poc_col_idx = idx
                        continue
                    
                    if deficiency_col_idx is None:
                        continue
                    
                    # Skip line numbers row
                    if header_row is not None and i == header_row + 1:
                        row_text = clean_text(row.get_text())
                        if self._is_line_numbers_row(row_text):
                            continue
                    
                    # Process data rows
                    if len(cells) > max(deficiency_col_idx or 0, poc_col_idx or 0):
                        first_cell = clean_text(cells[0].get_text().strip())
                        
                        # Check if new deficiency (unless expecting continuation)
                        is_new_deficiency = False
                        if not (expecting_def_continuation or expecting_poc_continuation):
                            if "Type" in first_cell or re.search(r'CCR\s*\d+|ILS[,\s]+\d+|^\d{5}', first_cell):
                                is_new_deficiency = True
                        
                        if is_new_deficiency:
                            # Save previous deficiency
                            if current_deficiency and current_deficiency.get("section_cited"):
                                if deficiency_text:
                                    desc = self._merge_continued_content(deficiency_text)
                                    current_deficiency["description"] = desc
                                if poc_text:
                                    poc = self._merge_continued_content(poc_text)
                                    current_deficiency["plan_of_correction"] = poc
                                deficiencies.append(current_deficiency)
                            
                            # Start new deficiency
                            current_deficiency = {}
                            deficiency_text = []
                            poc_text = []
                            expecting_def_continuation = False
                            expecting_poc_continuation = False
                            
                            # Extract metadata
                            type_match = re.search(r'Type\s+([A-Z])', first_cell)
                            if type_match:
                                current_deficiency["deficiency_type"] = type_match.group(1)
                            
                            poc_match = re.search(r'(\d{2}/\d{2}/\d{4})', first_cell)
                            if poc_match:
                                current_deficiency["poc_due_date"] = poc_match.group(1)
                            
                            section_match = (
                                re.search(r'ILS[,\s]+(\d+(?:\.\d+)?(?:\([a-z]\))?)', first_cell) or
                                re.search(r'CCR\s*(\d+(?:\.\d+)?(?:\([a-z]\))?)', first_cell) or
                                re.search(r'(\d{5}(?:\.\d+)?(?:\([a-z]\))?)', first_cell)
                            )
                            if section_match:
                                current_deficiency["section_cited"] = section_match.group(1)
                        
                        # Extract deficiency text
                        if deficiency_col_idx is not None and len(cells) > deficiency_col_idx:
                            def_text = clean_text(cells[deficiency_col_idx].get_text().strip())
                            
                            if not self._is_line_numbers_row(def_text):
                                # Check for continuation
                                if self._check_for_continuation(def_text):
                                    expecting_def_continuation = True
                                else:
                                    expecting_def_continuation = False
                                
                                def_text = re.sub(r'^\d{1,3}\s+', '', def_text)
                                
                                if def_text and not re.match(r'^[\d\s]+$', def_text):
                                    deficiency_text.append(def_text)
                        
                        # Extract POC text
                        if poc_col_idx is not None and len(cells) > poc_col_idx:
                            poc = clean_text(cells[poc_col_idx].get_text().strip())
                            
                            if not self._is_line_numbers_row(poc):
                                # Check for continuation
                                if self._check_for_continuation(poc):
                                    expecting_poc_continuation = True
                                else:
                                    expecting_poc_continuation = False
                                
                                poc = re.sub(r'^\d{1,3}\s+', '', poc)
                                
                                if poc and not re.match(r'^[\d\s]+$', poc):
                                    poc_text.append(poc)
                
                # Save last deficiency
                if current_deficiency and current_deficiency.get("section_cited"):
                    if deficiency_text:
                        desc = self._merge_continued_content(deficiency_text)
                        current_deficiency["description"] = desc
                    if poc_text:
                        poc = self._merge_continued_content(poc_text)
                        current_deficiency["plan_of_correction"] = poc
                    deficiencies.append(current_deficiency)
        
        # Fallback regex patterns
        deficiency_pattern = r'(\d{5}(?:\.\d+)?(?:\([a-z]\))?)\s+([^:]+):\s+([^T]+?)(?:This requirement|The facility|$)'
        text_deficiencies = re.findall(deficiency_pattern, text, re.DOTALL)
        
        for section, title, description in text_deficiencies:
            if not any(d.get("section_cited") == section for d in deficiencies):
                deficiencies.append({
                    "section_cited": section,
                    "title": title.strip(),
                    "description": description.strip()
                })
        
        return deficiencies
    
    def _decode_facility_type(self, code: str) -> str:
        """Decode facility type codes"""
        types = {
            "733": "Short Term Residential Therapeutic Program (STRTP)",
            "730": "Group Home",
            "727": "Small Family Home",
            "734": "Enhanced Behavioral Support Home"
        }
        return types.get(code, f"Type {code}")
    
    def process_all_facilities(self) -> List[Dict[str, Any]]:
        """Process all facilities in the list"""
        all_reports = []
        total = len(self.facility_ids)
        
        for i, fac_id in enumerate(self.facility_ids, 1):
            # full_url is created here as a local variable
            full_url = f"{self.base_url}/{fac_id}"
            print(f"Processing {i}/{total}: {fac_id}")
            
            try:
                reports = self.fetch_reports(fac_id)
                if reports:
                    for report in reports:
                        # source_url is just a dictionary key being added
                        report["source_url"] = full_url
                    all_reports.extend(reports)
                else:
                    all_reports.append({
                        "facility_id": fac_id,
                        "source_url": full_url,  # Dictionary key
                        "status": "No reports found"
                    })
                
                if i % 10 == 0:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"  Error: {e}")
                all_reports.append({
                    "facility_id": fac_id,
                    "error": str(e)
                })
        
        return all_reports
    
    def save_json(self, data: List[Dict], filename: str = "ccl_reports.json"):
        """Save results as JSON with error handling"""
        try:
            print(f"Attempting to save {len(data)} records to {filename}")
            
            # Check if data is empty
            if not data:
                print("Warning: No data to save!")
                return
            
            # Get current working directory
            import os
            current_dir = os.getcwd()
            print(f"Current directory: {current_dir}")
            
            # Check if we can write to the current directory
            if not os.access(current_dir, os.W_OK):
                print("Error: No write permission in current directory!")
                return
            
            # Try to write the file
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            
            # Verify the file was created and check its size
            if os.path.exists(filename):
                file_size = os.path.getsize(filename)
                print(f"Successfully saved {len(data)} reports to {filename} ({file_size} bytes)")
            else:
                print(f"Error: File {filename} was not created!")
                
        except PermissionError:
            print(f"Error: Permission denied when trying to write {filename}")
        except IOError as e:
            print(f"Error: IO error when writing {filename}: {e}")
        except Exception as e:
            print(f"Error: Unexpected error saving {filename}: {e}")
            print(f"Error type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
"""
# Main execution
if __name__ == "__main__":
    # Test with a couple facilities first
    test_ids = ["435390005", "374690035"]  # Using the facility from your example document
    
    print("Testing with 2 facilities...")
    parser = CaliforniaCCLParser(test_ids)
    reports = parser.process_all_facilities()
    
    if reports:
        parser.save_json(reports, "test_output.json")
        print("\nTest complete! Check test_output.json")
        print("If results look good, uncomment the full batch processing below")
 """
if __name__ == "__main__":
    facility_ids = ["547207220",
    "374690035",
    "347006128",
    "075650177",
    "487006130",
    "487006584",
    "306090054",
    "306090058",
    "374690073",
    "075650162",
    "397006143",
    "198209741",
    "075650161",
    "374690045",
    "197803340",
    "197803679",
    "071405405",
    "297002929",
    "347000489",
    "347002085",
    "577005657",
    "577006647",
    "445390045",
    "445390054",
    "107207221",
    "107207211",
    "157806107",
    "197807060",
    "107207240",
    "107207279",
    "435390001",
    "197806910",
    "374690011",
    "347006569",
    "397006121",
    "445390020",
    "134690103",
    "198209660",
    "487006088",
    "347006146",
    "507207246",
    "198209723",
    "374690084",
    "347006090",
    "347006562",
    "336428075",
    "445390051",
    "127006575",
    "487006144",
    "374690028",
    "374690091",
    "107207258",
    "347006573",
    "107207257",
    "107207286",
    "197807032",
    "107204151",
    "015650067",
    "015650113",
    "197805592",
    "075650148",
    "198206911",
    "430707159",
    "435201005",
    "157807026",
    "075650187",
    "366401649",
    "366403207",
    "366402331",
    "366412000",
    "216804300",
    "455090046",
    "397003309",
    "198207503",
    "075650160",
    "435202522",
    "306090110",
    "306090073",
    "107207217",
    "107207278",
    "157807023",
    "198209449",
    "198209649",
    "198209719",
    "198209744",
    "198209764",
    "198209795",
    "198209850",
    "198209769",
    "107206837",
    "337900185",
    "198209858",
    "397006129",
    "507207209",
    "075600773",
    "157806105",
    "337900254",
    "198209205",
    "425801573",
    "337900006",
    "336427936",
    "197806914",
    "198209656",
    "198209657",
    "198209658",
    "198209659",
    "075650105",
    "507207182",
    "435202266",
    "366000008",
    "198209872",
    "198209839",
    "198209788",
    "198209792",
    "198209620",
    "198209807",
    "198209709",
    "397006002",
    "015650069",
    "306001372",
    "306000382",
    "355390010",
    "397005976",
    "157807022",
    "157807027",
    "347003317",
    "347003035",
    "347006598",
    "075650061",
    "198209776",
    "198209724",
    "075650137",
    "445200283",
    "367800006",
    "107201334",
    "198209760",
    "197805988",
    "565800460",
    "015650017",
    "415650041",
    "565800022",
    "300602437",
    "191801425",
    "340312684",
    "197806686",
    "075650136",
    "306090121",
    "374603909",
    "197806866",
    "317006040",
    "075650142",
    "107207165",
    "306004077",
    "306090084",
    "306001480",
    "075600628",
    "216803317",
    "347006071",
    "306090103",
    "306090043",
    "487004421",
    "275201941",
    "197806873",
    "367800023",
    "306004089",
    "306090007",
    "306004813",
    "300600336",
    "306003903",
    "306004172",
    "397006126",
    "397006131",
    "286890106",
    "198209804",
    "075650119",
    "075650106",
    "075650180",
    "198207892",
    "197608637",
    "197806919",
    "198207221",
    "198207220",
    "336428089",
    "198209661",
    "197606579",
    "385650046",
    "565802314",
    "198209703",
    "306003690",
    "347003423",
    "347003603",
    "347001976",
    "374603372",
    "075650134",
    "075650181",
    "435390032",
    "435390042",
    "435390063",
    "374602222",
    "374690008",
    "107207150",
    "347006572",
    "435390026",
    "435390033",
    "435202697",
    "198209294",
    "198209706",
    "374690017",
    "347006651",
    "347006135",
    "374690026",
    "374690027",
    "374690077",
    "198209705",
    "075650151",
    "435390017",
    "075650153",
    "075650154",
    "198209708",
    "198209704",
    "157806081",
    "360906517",
    "397006656",
    "015650088",
    "198204052",
    "198209796",
    "374690025",
    "198207997",
    "547203404",
    "547207260",
    "197806308",
    "490100320",
    "157202792",
    "547207234",
    "198209742",
    "198209669",
    "075650139",
    "197807027",
    "507207205",
    "075650185",
    "015650006",
    "015650052",
    "075650113",
    "347006663",
    "347006634",
    "075650138",
    "075650167",
    "337900070",
    "306090105",
    "306090117",
    "337900057",
    "198209766",
    "198209867",
    "337900233",
    "198209834",
    "197805600",
    "107206866",
    "306090042",
    "191501937",
    "397006132",
    "507207229",
    "191220817",
    "198209612",
    "198208378",
    "198209422",
    "397006627",
    "517006616",
    "197806546",
    "487006085",
    "198209622",
    "197806754",
    "487005946",
    "197806545",
    "306003691",
    "507206806",
    "191501961",
    "366000007",
    "397006629",
    "397006637",
    "397006127",
    "374690050",
    "415600322",
    "487004410",
    "397005348",
    "397005954",
    "397006561",
    "397006046",
    "397004926",
    "397004110",
    "366000054",
    "415650003",
    "198209664",
    "198209773",
    "547202393",
    "565802328",
    "565802325",
    "366406349",
    "317006155",
    "317006148",
    "317006156",
    "317001955",
    "347001884",
    "347006067",
    "197805930",
    "207204092",
    "157806077",
    "565802327",
    "397005376",
    "198209749",
    "216890104",
    "507004599",
    "374603441",
    "125000571",
    "306090122",
    "075650107",
    "455090026",
    "435390043",
    "435390057",
    "415650044",
    "374690062",
    "374690078",
    "374690057",
    "374690036",
    "415650039",
    "415650018",
    "397006648",
    "415650020",
    "367800004",
    "015650057",
    "198209614",
    "191500468",
    "374600648",
    "374603052",
    "197807021",
    "366409518",
    "157806020",
    "336427937",
    "107207169",
    "487006602",
    "374690079",
    "374690082",
    "374690080",
    "198209767",
    "198209772",
    "198209784",
    "198209778",
    "198209854",
    "075650149",
    "075650122",
    "198209654",
    "075650120",
    "197603205",
    "198209771",
    "198205206",
    "290300461",
    "347006574",
    "347006655",
    "107207237",
    "107207255",
    "107207243",
    "337900136",
    "337900118",
    "337900137",
    "496890076",
    "496890100",
    "496890074",
    "496803801",
    "496890061",
    "496890077",
    "496890062",
    "496890056",
    "496890087",
    "496890065",
    "496890083",
    "496800005",
    "496803428",
    "496890088",
    "157200402",
    "157200949",
    "366000013",
    "366000052",
    "157807031",
    "197806879",
    "347006611",
    "157806059",
    "374600210",
    "374600200",
    "374600204",
    "374601789",
    "374600208",
    "374602811",
    "097004896",
    "107207245",
    "107207244",
    "525002832",
    "306004815",
    "306004818",
    "306004843",
    "306004053",
    "306004180",
    "015650125",
    "565802317",
    "565802318",
    "097006142",
    "565802319",
    "097006141",
    "216890000",
    "015650159",
    "216890001",
    "306090011",
    "306090014",
    "306090009",
    "075650169",
    "565802326",
    "306090080",
    "374690053",
    "198209768",
    "374690048",
    "374690049",
    "097006140",
    "015650119",
    "306090035",
    "306090027",
    "216890066",
    "306090036",
    "216890064",
    "306090026",
    "565802321",
    "015650118",
    "198209624",
    "198207731",
    "435390008",
    "435390007",
    "425800427",
    "425800630",
    "336401648",
    "425802107",
    "337900202",
    "191502141",
    "075650102",
    "075650116",
    "306004820",
    "306090119",
    "306004831",
    "306000465",
    "198209783",
    "198209824",
    "198209847",
    "336402216",
    "336423427",
    "374690110",
    "306004549",
    "366428048",
    "366000045",
    "430703245",
    "435200211",
    "197806816",
    "198209636",
    "075650178",
    "198209646",
    "198209645",
    "198209647",
    "198209648",
    "198209754",
    "216800046",
    "216800047",
    "216890069",
    "347006583",
    "411408919",
    "347004337",
    "197805858",
    "157806039",
    "198209874",
    "107207199",
    "445202486",
    "374603912",
    "075650140",
    "198208520",
    "347005977",
    "075650141",
    "347005978",
    "374603931",
    "347005979",
    "405802268",
    "198209758",
    "015650135",
    "374603945",
    "198209409",
    "198209710",
    "198209633",
    "198209761",
    "157806062",
    "157804916",
    "157806069",
    "374690058",
    "336428077",
    "336410208",
    "435390050",
    "198209748",
    "347006603",
    "336410092",
    "015650111",
    "374603355",
    "197806309",
    "428209725",
    "455002131",
    "451370849",
    "455002260",
    "515090048",
    "397006128",
    "336408395",
    "197807058",
    "366405825",
    "367900069",
    "397006654",
    "015650133",
    "107206730",
    "397005192",
    "306090053",
    "306090052",
    "157806006",
    "157806012",
    "306004842",
    "306004817",
    "306004816",
    "306003867",
    "374602508",
    "157806073",
    "157806008",
    "157806038",
    "496890079",
    "374690034",
    "374690051",
    "374601549",
    "397006604",
    "336426732",
    "107200595",
    "306004227",
    "425800479",
    "374690104",
    "107202484",
    "487006660",
    "366428085",
    "198202799",
    "374690021",
    "366400906",
    "337900143",
    "337900223",
    "336402460",
    "435200178",
    "374601427",
    "075650036",
    "374603915",
    "374600598",
    "487006150",
    "075650143",
    "075650157",
    "107204021",
    "557005987",
    "374603412",
    "015650070",
    "216890093",
    "097006566",
    "097006564",
    "097006565",
    "015650122",
    "415650034",
    "075650123",
    "191600883",
    "557003630",
    "197603054",
    "197604421",
    "107207270",
    "306090101",
    "198209650",
    "191820097",
    "415600170",
    "347006662",
    "496890086",
    "496803349",
    "347006094",
    "300611898",
    "015650123",
    "415650001",
    "197807019",
    "015650114",
    "367900097",
    "507207277",
    "405802270",
    "236803444",
    "236801975",
    "405802311",
    "175002769",
    "405801411",
    "407690066",
    "198207508",
    "075650038",
    "198209734",
    "198207879",
    "207207233",
    "347005981",
    "286890080",
    "198209653",
    "565802310",
    "198205006",
    "197603673",
    "198209745",
    "198209770",
    "374602955",
    "374602952",
    "198209786",
    "367900077",
    "197806882",
    "198207974",
    "197806804",
    "198208265",
    "337900218",
    "337900207",
    "496890099",
    "197805986",
    "107202849",
    "157807029",
    "157807018",
    "370808617",
    "410508685",
    "306090032",
    "374604000",
    "107207170",
    "435390031",
    "107207223",
    "107200329",
    "107200421",
    "425802125",
    "198209846",
    "198209810",
    "401703381",
    "445201743",
    "366401135",
    "430707436",
    "430707151",
    "428209702",
    "075650092",
    "336402915",
    "336408688",
    "374690039",
    "374690033",
    "198209799",
    "507002607",
    "198209805",
    "198209865",
    "500311568",
    "500310324",
    "500314443",
    "500311277",
    "107206686",
    "397006624",
    "107202349",
    "247201359",
    "336403863",
    "157806093",
    "075650179",
    "337900199",
    "337900200",
    "337900298",
    "337900299",
    "157807000",
    "157807001",
    "157807002",
    "157807003",
    "157807005",
    "157807012",
    "157807008",
    "075650164",
    "075650165",
    "360911223",
    "366400636",
    "366405639",
    "366401098",
    "366401709",
    "500305469",
    "500308363",
    "198209831",
    "366426729",
    "336426158",
    "366426844",
    "198209866",
    "198209720",
    "198207575",
    "198204767",
    "198208206",
    "198208201",
    "360900096",
    "191500098",
    "367800013",
    "367800015",
    "367800014",
    "367800012",
    "197807040",
    "191800953",
    "547207272",
    "197803661",
    "337900296",
    "397006653",
    "107207215",
    "107207250",
    "107207216",
    "157806085",
    "157806104",
    "107207261",
    "337900226",
    "397006567",
    "397006154",
    "198209833",
    "157806044",
    "366401123",
    "415600309",
    "374603234",
    "374690043",
    "360911241",
    "197804217",
    "198209634",
    "567698001",
    "565800021",
    "107207228",
    "107202512",
    "107203143",
    "107207276",
    "367800003",
    "107202346",
    "374601510",
    "577003959",
    "374690099",
    "374603292",
    "374690031",
    "374602641",
    "347006096",
    "374690076",
    "374601477",
    "330908393",
    "330908390",
    "330908391",
    "350701021",
    "355201170",
    "355200625",
    "355200873",
    "425800981",
    "297003442",
    "297002555",
    "297006118",
    "198209800",
    "337900091",
    "306000902",
    "306000901",
    "337900092",
    "306000509",
    "330902381",
    "397002553",
    "390300131",
    "397002554",
    "397002552",
    "390332238",
    "366425817",
    "347006056",
    "455090041",
    "397006595",
    "390311459",
    "374602611",
    "374602603",
    "015650117",
    "337900051",
    "336425813",
    "336426251",
    "366405647",
    "366407775",
    "547200700",
    "367900029",
    "366000042",
    "300612972",
    "198209509",
    "198209665",
    "198209666",
    "198209667",
    "157201294",
    "157806794",
    "157807016",
    "198203559",
    "198209576",
    "198208930",
    "367800018",
    "198203822",
    "197605014",
    "198204471",
    "197804638",
    "198209250",
    "197690020",
    "197690021",
    "197690022",
    "198209785",
    "367800034",
    "367800040",
    "107200453",
    "107200940",
    "337900142",
    "366406712",
    "198207746",
    "198209613",
    "198209626",
    "507001576",
    "045090025",
    "015650084",
    "496890067",
    "216890092",
    "216890091",
    "385650044",
    "380500183",
    "197806909",
    "366426271",
    "197806370",
    "366426270",
    "197806374",
    "197806346",
    "197806348",
    "197806347",
    "507002176",
    "507001666",
    "507003353",
    "507003893",
    "415650019",
    "198209818",
    "198209870",
    "198209828",
    "185000467",
    "045002532",
    "397000639",
    "360900845",
    "360900339",
    "360900272",
    "336406377",
    "336400048",
    "336402245",
    "366402086",
    "366407025",
    "367900192",
    "015600316",
    "157806049",
    "374602535",
    "191200236",
    "191290123",
    "198209746",
    "565801649",
    "336403949",
    "011400218",
    "374602631",
    "198200050",
    "198206276",
    "107204098",
    "107204244",
    "366408739",
    "367900099",
    "347001028",
    "347000275",
    "347000223",
    "347000792",
    "347001397",
    "191592695",
    "490101336",
    "490101337",
    "490102637",
    "496802071",
    "500313263",
    "336426160",
    "337900103",
    "336423724",
    "337900234",
    "565801731",
    "565801693",
    "445200555",
    "496890102",
    "247202591",
    "306001431",
    "306001481",
    "107202669",
    "336427799",
    "337900285",
    "197804907",
    "191501972",
    "507207227",
    "107202385",
    "340300163",
    "197804150",
    "191500101",
    "197801967",
    "197802215",
    "197804065",
    "197807003",
    "197806998",
    "198209763",
    "191200313",
    "360911242",
    "425802135",
    "191601689",
    "075650173",
    "167206801",
    "157806055",
    "157200493",
    "015650060",
    "075650172",
    "015650136",
    "015650132",
    "015650142",
    "075650065",
    "071440880",
    "565801831",
    "565802309",
    "567806504",
    "337900269",
    "097005992",
    "317005938",
    "317000017",
    "310317515",
    "310311378",
    "317000875",
    "317006630",
    "071440124",
    "397004186",
    "075650005",
    "075650053",
    "075650074",
    "075650182",
    "075600414",
    "366000017",
    "366000035",
    "336403698",
    "337900246",
    "337900294",
    "337900295",
    "107202620",
    "107207213",
    "107207226",
    "306000793",
    "300613291",
    "360911229",
    "191502075",
    "247200747",
    "100406164",
    "107202606",
    "487005982",
    "487005059",
    "372008440",
    "015600503",
    "198209759",
    "198209803",
    "198209802",
    "198209813",
    "198209814",
    "198209856",
    "198209651",
    "198209652",
    "198209736",
    "107202634",
    "275390069",
    "435390038",
    "347001340",
    "340309856",
    "347000093",
    "297006089",
    "057006576",
    "107202348",
    "337900281",
    "300603549",
    "300606868",
    "370801541",
    "374600197",
    "336402151",
    "336403968",
    "336410417",
    "191800491",
    "198209827",
    "191593081",
    "360908565",
    "337900264",
    "157804682",
    "565801564",
    "565801655",
    "347006621",
    "330911240",
    "050303501",
    "430700482",
    "425802141",
    "300603063",
    "300605577",
    "300606064",
    "075650183",
    "455002153",
    "455001035",
    "455001037",
    "191801986",
    "191604301",
    "191890971",
    "191201124",
    "197600766",
    "425802143",
    "015650080",
    "198209477",
    "075650097",
    "487006136",
    "487003584",
    "075600345",
    "340314605",
    "347006002",
    "347005962",
    "347006038",
    "340313193",
    "340312747",
    "347001788",
    "347005964",
    "347005963",
    "340317015",
    "347005967",
    "270702624",
    "275200693",
    "336426106",
    "191290246",
    "197605935",
    "191220837",
    "191221387",
    "191202003",
    "191221881",
    "191221975",
    "191220863",
    "337900304",
    "360906534",
    "336408020",
    "336413103",
    "336411179",
    "336407591",
    "570304441",
    "577005990",
    "107207232",
    "198209668",
    "198209740",
    "197606825",
    "107206872",
    "107200789",
    "200405478",
    "107201149",
    "100407405",
    "107206310",
    "107206311",
    "100406294",
    "198209829",
    "107202885",
    "336408176",
    "360911251",
    "100404782",
    "100403988",
    "100405477",
    "107204178",
    "107202597",
    "107200602",
    "107206848",
    "100406223",
    "100404635",
    "100404234",
    "547207201",
    "247202586",
    "247203971",
    "337900308",
    "191202023",
    "198209750",
    "075650166",
    "337900208",
    "236803806",
    "236803809",
    "125090039",
    "236890071",
    "236890052",
    "236801878",
    "236890059",
    "236890060",
    "347006659",
    "347006658",
    "455000235",
    "336403699",
    "366408470",
    "435390029",
    "306090095",
    "306090096",
    "306090097",
    "057001447",
    "198209644",
    "198209701",
    "306004844",
    "306004825",
    "027202889",
    "027005048",
    "090314963",
    "097006661",
    "015600412",
    "015600744",
    "015650058",
    "340300577",
    "397006623",
    "374602245",
    "385650001",
    "385650002",
    "015650138",
    "075650186",
    "197805170",
    "197804961",
    "374690087",
    "374690086",
    "306090050",
    "500312835",
    "275202098",
    "360906507",
    "247200865",
    "157806106",
    "157807007",
    "157806101",
    "547207231",
    "336428079",
    "507001670",
    "015650129",
    "015650130",
    "565802440",
    "496803769",
    "385650047",
    "425802142",
    "337900268",
    "227206624",
    "227207195",
    "500306051",
    "507000819",
    "367900148",
    "336406702",
    "366408260",
    "306001333",
    "306001388",
    "300605693",
    "300607208",
    "435390047",
    "360910260",
    "360910261",
    "360911127",
    "366402532",
    "198209857",
    "336402176",
    "336405644",
    "337900307",
    "157201308",
    "157806088",
    "191802087",
    "216801396",
    "547202542",
    "547202724",
    "175090049",
    "360911286",
    "435390048",
    "097006065",
    "097005932",
    "097006084",
    "097005985",
    "097000262",
    "097007133",
    "097006138",
    "090317911",
    "097006086",
    "097006597",
    "097006137",
    "097006619",
    "198209755",
    "374601272",
    "374603969",
    "198209868",
    "374603970",
    "496801036",
    "405802310",
    "198209817",
    "366401747",
    "191591941",
    "360900416",
    "565801787",
    "565801634",
    "500311481",
    "306004463",
    "306004464",
    "445202189",
    "015650089",
    "150406795",
    "157201293",
    "150407513",
    "496890084",
    "207200734",
    "200404880",
    "134690030",
    "134603561",
    "134603562",
    "374603652",
    "374603865",
    "374602412",
    "306090023",
    "306090024",
    "496800507",
    "496801792",
    "496803137",
    "496800259",
    "496803033",
    "490108438",
    "198209631",
    "198209629",
    "198209630",
    "198209632",
    "198209628",
    "191600721",
    "247203412",
    "157806092",
    "157807004",
    "191800260",
    "157804733",
    "057006577",
    "107203239",
    "306090114",
    "336427952",
    "071440601",
    "071407569",
    "075650085",
    "075600070",
    "198209849",
    "157804795",
    "157807014",
    "157807021",
    "401703443",
    "198209721",
    "107207224",
    "337900306",
    "374690111",
    "337900195",
    "374690114",
    "337900245",
    "198209836",
    "374690075",
    "367900188",
    "337900286",
    "075650176",
    "337900232",
    "367900141",
    "337900061",
    "337900253",
    "197807055",
    "198209840",
    "337900222",
    "366000039",
    "367900152",
    "107207122",
    "337900309",
    "337900255",
    "347006652",
    "197807042",
    "337900203",
    "367800009",
    "366000020",
    "306001386",
    "306090120",
    "337900273",
    "336425822",
    "197801042",
    "198208525",
    "337900263",
    "157807028",
    "367900161",
    "397002621",
    "397004618",
    "374600979",
    "374601705",
    "157807030",
    "198209852",
    "337900278",
    "347006650",
    "337900271",
    "337900244",
    "337900162",
    "337900250",
    "374602772",
    "337900085",
    "374690096",
    "366000028",
    "367900012",
    "397005980",
    "197807071",
    "337900016",
    "198209809",
    "337900180",
    "337900240",
    "337900140",
    "337900217",
    "337900293",
    "337900300",
    "337900212",
    "337900259",
    "337900109",
    "337900205",
    "337900228",
    "367900176",
    "367900120",
    "367800039",
    "337900098",
    "337900277",
    "337900146",
    "367900146",
    "198209085",
    "197807061",
    "366000015",
    "347006642",
    "337900219",
    "366000025",
    "397003324",
    "337900274",
    "198209789",
    "191804171",
    "337900310",
    "405800635",
    "337900314",
    "337900111",
    "306003943",
    "198209860",
    "367900116",
    "198209751",
    "236803762",
    "236890068",
    "370808434",
    "347006570",
    "198209864",
    "486800630",
    "198209823",
    "075650135",
    "366000046",
    "367800028",
    "197800319",
    "367800027",
    "197805073",
    "337900086",
    "317006052",
    "367900066",
    "367900053",
    "306004789",
    "191601461",
    "317006570",
    "405801548",
    "367900067",
    "367900167",
    "191505141",
    "367800029",
    "337900028",
    "125000557",
    "337900209",
    "397006585",
    "547202441",
    "337900216",
    "337900275",
    "336412221",
    "337900270",
    "340313161",
    "337900248",
    "198209843",
    "367800022",
    "366000002",
    "336428049",
    "366000022",
    "337900002",
    "337900074",
    "337900169",
    "197807082",
    "340317929",
    "337900225",
    "360906057",
    "336423833",
    "565802422",
    "198209851",
    "075650104",
    "075650170",
    "337900022",
    "337900060",
    "366000034",
    "337900235",
    "366000011",
    "075650152",
    "337900251",
    "175002329",
    "366000031",
    "367900045",
    "347006599",
    "366000049",
    "306090013",
    "547200406",
    "336428054",
    "337900151",
    "337900163",
    "366402126",
    "198209781",
    "317006107",
    "347006636",
    "337900097",
    "198209862",
    "337900145",
    "337900241",
    "347005145",
    "198209861",
    "374603964",
    "367900106",
    "366425463",
    "367900122",
    "366000033",
    "337900313",
    "337900210",
    "107203497",
    "198209837",
    "366000030",
    "347006633",
    "198209753",
    "374601162",
    "197806849",
    "366428093",
    "367900027",
    "367900132",
    "366000023",
    "337900231",
    "367800041",
    "337900010",
    "374690085",
    "366428088",
    "198209835",
    "374603876",
    "336425461",
    "337900037",
    "336428032",
    "075650174",
    "198209863",
    "366425465",
    "366000014",
    "317001269",
    "197807075",
    "286800454",
    "367900104",
    "197807053",
    "310312274",
    "367900194",
    "198208939",
    "198209752",
    "075650184",
    "366000043",
    "366425109",
    "075650147",
    "330909463",
    "337900129",
    "015650137",
    "157806103",
    "157807011",
    "197801868",
    "198209815",
    "198209830",
    "198209855",
    "337900243",
    "366000044",
    "366000019",
    "367900170",
    "337900260",
    "367900196",
    "366424296",
    "507004129",
    "198207751",
    "366000003",
    "337900174",
    "337900080",
    "577006638",
    "367900088",
    "367900164",
    "157807032",
    "347006615",
    "197807080",
    "337900073",
    "337900237",
    "197807070",
    "397006609",
    "366000051",
    "337900252",
    "197807051",
    "337900150",
    "374602089",
    "337900236",
    "337900297",
    "337900215",
    "191604214",
    "337900283",
    "198209853",
    "337900312",
    "337900114",
    "367900189",
    "367900130",
    "366000029",
    "197807068",
    "157807019",
    "367900089",
    "340312893",
    "337900289",
    "336427393",
    "366000021",
    "366000016",
    "367900071",
    "198209794",
    "197807066",
    "367800008",
    "197807030",
    "374690092",
    "367900182",
    "198209816",
    "337900107",
    "336407769",
    "366000041",
    "397001245",
    "337900102",
    "198209777",
    "374602815",
    "367800030",
    "367900084",
    "337900213",
    "157807025",
    "366428083",
    "337900038",
    "366000047",
    "197807065",
    "337900087",
    "157806102",
    "507207288",
    "337900158",
    "337900149",
    "337900242",
    "337900128",
    "367900147",
    "198209825",
    "337900134",
    "198209811",
    "337900125",
    "337900191",
    "336425208",
    "337900001",
    "337900206",
    "565800213",
    "198209822",
    "337900276",
    "337900123",
    "337900305",
    "367900119",
    "366000027",
    "336427963",
    "336428086",
    "337900198",
    "367900198",
    "336428094",
    "367900063",
    "367900181",
    "157202626",
    "480103037",
    "397006644",
    "336428068",
    "587006578",
    "275390068",
    "280106012",
    "337900230",
    "337900272",
    "197804864",
    "366000040",
    "191601169",
    "366000024",
    "198209801",
    "075650168",
    "347006571",
    "337900282",
    "337900291",
    "197807079",
    "337900288",
    "337900239",
    "366000009",
    "337900292",
    "367900082",
    "374690065",
    "374690100",
    "366000018",
    "337900256",
    "075650171",
    "337900177",
    "367900165",
    "337900105",
    "337900284",
    "336427892",
    "191222165",
    "198209842",
    "337900117",
    "367800025",
    "337900020",
    "366000001",
    "367900115",
    "397006125",
    "337900311",
    "337900204",
    "367900065",
    "367900131",
    "337900075",
    "455000040",
    "198209793",
    "198209616",
    "075650146",
    "337900287",
    "367900160",
    "198209821",
    "347006133",
    "198209790",
    "306090087",
    "366000038",
    "337900083",
    "075600050",
    "367900059",
    "197807076",
    "317002055",
    "198209844",
    "337900183",
    "337900011",
    "367800032",
    "198209848",
    "337900138",
    "337900302",
    "347006641",
    "337900290",
    "367900121",
    "374690071",
    "366000053",
    "337900159",
    "567804842",
    "337900042",
    "397006645",
    "547203264",
    "336423834",
    "347002700",
    "306004195",
    "367900017",
    "337900171",
    "336428082",
    "157807024",
    "337900249",
    "337900267",
    "198207148",
    "337900168",
    "337900224",
    "337900279",
    "337900303",
    "337900093",
    "367900054",
    "198209787",
    "367800033",
    "367900108",
    "367900186",
    "405802312",
    "337900211",
    "397002900",
    "337900262",
    "366428092",
    "337900003",
    "337900081",
    "337900214",
    "306090030",
    "157807020",
    "198209873",
    "367800037",
    "337900261",
    "367800035",
    "198209791",
    "197807033",
    "336428059",
    "306090124",
    "306003839",
    "366412482",
    "337900153",
    "337900257",
    "337900301",
    "337900155",
    "337900175",
    "337900229",
    "337900201",
    "366000048",
    "337900247",
    "337900127",
    "367900166",
    "306004590",
    "198209808",
    "198209765",
    "367900090",
    "337900113",
    "337900266",
    "337900265",
    "317006044",
    "336403678",
    "366000010",
    "198209782",
    "075650158",
    "198201396",
    "198209832",
    "198209871",
    "360910092",
    "337900220",
    "347006649",
    "306090083",
    "374690089",
    "390311420",
    "397006612",
    "197607251",
    "374600869",
    "197603588",
    "366000026",
    "198208356",
    "367800031",
    "367900044",
    "197601452",
    "405801525",
    "015650120",
    "367900019",
    "374690094",
    "337900144",
    "015650141",
    "337900179",
    "337900076",
    "197807078",
    "366000032",
    "157806098",
    "374603968",
    "134603967",
    "347006074",
    "347006618",
    "347006593",
    "347006594",
    "347006586",
    "198209762",
    "198209775",
    "198209737",
    "198209739",
    "198209718",
    "198209715",
    "198209726",
    "198209727",
    "198209728",
    "198209716",
    "198209738",
    "198209637",
    "347006632",
    "198209638",
    "397006063",
    "374690029",
    "306090006",
    "198209662",
    "347006639",
    "347006605",
    "347006631",
    "198209747",
    "347006620",
    "347006657",
    "347006592",
    "347006614",
    "347006608",
    "415650038",
    "415650040",
    "198209757",
    "198209756",
    "496890002",
    "198209717",
    "198209635",
    "107207225",
    "107207204",
    "107207202",
    "107207251",
    "015202448",
    "198209841",
    "015202495",
    "487006580",
    "487005958",
    "216803574",
    "075202523",
    "317006130",
    "317006094",
    "347006070",
    "547207192",
    "507207176",
    "565802329",
    "157806089",
    "157807009",
    "366000006",
    "107206670",
    "198209623",
    "247206870",
    "397005943",
    "397006159",
    "565801976",
    "336426843",
    "366427778",
    "337900172",
    "198208665",
    "015202391",
    "435200948",
    "366426449",
    "198209819",
    "015650131",
    "075650175",
    "197807081",
    "374603568",
    "565802331",
    "565801918",
    "507207164",
    "385650045",
    "045002495",
    "515090038",
    "525002461",
    "515090050",
    "547207263",
    "157807006",
    "198209711",
    "507206800",
    "198209655",
    "547207147",
    "198208082",
    "198209729",
    "337900046",
    "367900178",
    "197806264",
    "565802315",
    "337900018",
    "198209608",
    "198209730",
    "198209797",
    "565802330",
    "337900055",
    "075650163",
    "075650144",
    "347006626",
    "125090043",
    "515090035",
    "585090052",
    "125002475",
    "107207214",
    "107207252",
    "337900064",
    "487006635",
    "487006596",
    "347006147",
    "185002587",
    "297003927",
    "347006091",
    "577005957",
    "157807015",
    "177006145",
    "585001902",
    "455002797",
    "397005605",
    "405801903",
    "487005267",
    "015650108",
    "198207916",
    "487005103",
    "197601539",
    "197807064",
    "347006045",
    "198208801",
    "015202353",
    "107207256",
    "366000050",
    "565801893",
    "496890085",
    "306090037",
    "445202441",
    "367900026",
    "197807049",
    "197807077",
    "198209869",
    "015202678",
    "374604002",
    "197690008",
    "198209735",
    "337900258",
    "347006106",
    "347006134",
    "337900154",
    "337900227",
    "015650124",
    "337900041",
    "455002596",
    "306090085",
    "306090025",
    "336428047",
    "565802316",
    "247207212",
    "015650128",
    "487005974",
    "197807035",
    "374690052",
    "425802144",
    "374603479",
    "374690038",
    "374690066",
    "306004585",
    "198209743",
    "045001092",
    "374690072",
    "337900036",
    "197806301",
    "197807043",
    "306001254",
    "336426104",
    "397005878",
    "337900280",
    "075202371",
    "397005994",
    "397006122",
    "275390040",
    "275202687",
    "198207896",
    "197806752",
    "198209262",
    "366428099",
    "547207203",
    "547207265",
    "407698002",
    "367800038",
    "107207264",
    "198209605",
    "567698002",
    "198209845",
    "198209859",
    "107207126",
    "435390071",
    "015202130",
    "125002775",
    "236890094",
    "236803524",
    "045002829",
    "045090037",
    "085002081",
    "125001556",
    "198209064",
    "015650126",
    "015650139",
    "374603938",
    "374690090",
    "374603854",
    "045090021",
    "045090028",
    "227206709",
    "507207268",
    "374604005",
    "015650121",
    "435202144",
    "198207863",
    "547207219",
    "198209806",
    "198209820",
    "496803445",
    "445202375",
    "445390049",
    "337900133",
    "367900133",
    "015650140",
    "385650042",
    "317006116",
    "317006139",
    "317005664",
    "435390012",
    "547207247",
    "157806082",
    "374690069",
    "157806999",
    "374603903",
    "197806454",
    "336426017",
    "374690061",
    "374603488",
    "045090031",
    "045001528",
    "015650115",
    "015650134",
    "435390065",
    "435390030",
    "275390021",
    "337900079",
    "075650145",
    "075650156",
    "435390059",
    "435390067",
    "435390037",
    "367900062",
    "435202702",
    "435390055",
    "435390056",
    "435390025",
    "107207218",
    "337900068",
    "435390053",
    "435390005",
    "374690042",
    "300605543",
    "385650031",
    "015650116",
    "197690063",
    "385650043",
    "500312751",
    "507207210",
    "300611748",
    "367900008",
    "367900190",
    "428209722",
    "367900126",
    "367900157",
    "367900105",
    "015650127",
    "107206899",
    "496800048",
    "306090118",
    "306003469",
    "306004821",
    "347005983",
    ]

    batch_size = 100
    total_report_count = 0

    # Split the full list into smaller batches ---
    id_batches = [facility_ids[i:i + batch_size] for i in range(0, len(facility_ids), batch_size)]
    total_batches = len(id_batches)
    print(f"Full list of {len(facility_ids)} IDs has been split into {total_batches} batches of up to {batch_size} each.")

    # --- Step 4: Loop through each batch and process it ---
    for i, batch_of_ids in enumerate(id_batches, 1):
        print(f"\n{'='*25}\n--- Processing Batch {i} of {total_batches} ---\n{'='*25}")

        # Initialize the parser with the current batch of IDs
        parser = CaliforniaCCLParser(batch_of_ids)
        
        # Run the process for this batch
        batch_reports_data = parser.process_all_facilities()
        # Update report indices to be consecutive across batches
        for report in batch_reports_data:
            if 'report_index' in report:
                report['report_index'] = total_report_count
                total_report_count += 1
        print(f"\n--- Batch {i} Complete. Found {len(batch_reports_data)} total reports. ---")

        # Save the results for this batch to uniquely named files
        if batch_reports_data:
            json_filename = f"ccl_reports_batch_{i}.json"
            parser.save_json(batch_reports_data, filename=json_filename)
        else:
            print(f"No data was collected for batch {i}, so no files were saved.")
        
        # A polite pause between batches to not overwhelm the server
        if i < total_batches:
            print("\nPausing for 5 seconds before the next batch...")
            time.sleep(5)

    print(f"\n\nAll {total_batches} batches have been processed.")
