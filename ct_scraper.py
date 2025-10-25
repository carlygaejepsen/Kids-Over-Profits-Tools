import requests
import re
import json
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional

# Set up logging for better debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DCFFacilityScraper:
    def __init__(self):
        self.url = "https://licensefacilities.dcf.ct.gov/listing_CCF.asp"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def fetch_page(self) -> Optional[str]:
        """
        Fetch the page content with proper error handling
        """
        try:
            logger.info(f"Fetching data from {self.url}")
            response = self.session.get(self.url, timeout=30)
            response.raise_for_status()
            
            # Ensure we have the expected content
            if "Program Category" not in response.text:
                raise ValueError("Page does not contain expected table headers")
            
            logger.info(f"Successfully fetched {len(response.text)} characters")
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page: {e}")
            return None
        except ValueError as e:
            logger.error(f"Content validation error: {e}")
            return None
    
    def parse_html(self, html_content: str) -> Optional[BeautifulSoup]:
        """
        Parse HTML with BeautifulSoup and validate structure
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for the actual table structure
            table = soup.find('table')
            if not table:
                logger.error("No table found in HTML")
                return None
            
            logger.info("Found table structure")
            return soup
            
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return None
    
    def extract_table_data(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Extract facility data using proper HTML table parsing
        """
        facilities = []
        
        # Find all tables and examine them
        tables = soup.find_all('table')
        logger.info(f"Found {len(tables)} tables total")
        
        target_table = None
        
        # Look through all tables to find the one with data
        for i, table in enumerate(tables):
            logger.info(f"Examining table {i}:")
            
            # Check if this table has the expected headers
            headers = table.find_all('th')
            header_texts = [th.get_text(strip=True) for th in headers]
            logger.info(f"  Table {i} headers: {header_texts}")
            
            # Check if this table has "Program Category" header
            if any('Program Category' in header for header in header_texts):
                logger.info(f"  Table {i} contains 'Program Category' - this looks like our data table!")
                target_table = table
                break
            
            # Also check the structure - data tables should have multiple rows
            rows = table.find_all('tr')
            logger.info(f"  Table {i} has {len(rows)} rows")
        
        if not target_table:
            logger.error("Could not find the main data table with 'Program Category' header")
            return facilities
        
        logger.info("Found target data table")
        
        # Find all rows in the target table
        all_rows = target_table.find_all('tr')
        logger.info(f"Found {len(all_rows)} total rows in target table")
        
        # Find where data starts (skip header rows)
        data_start_index = 1  # Usually first row is headers
        
        # Get data rows
        data_rows = all_rows[data_start_index:]
        logger.info(f"Processing {len(data_rows)} data rows starting from row {data_start_index}")
        
        for i, row in enumerate(data_rows):
            try:
                facility = self._parse_table_row(row, i)
                if facility:
                    facilities.append(facility)
            except Exception as e:
                logger.warning(f"Error parsing row {i}: {e}")
                continue
        
        logger.info(f"Successfully parsed {len(facilities)} facilities")
        return facilities
    
    def _extract_cell_text(self, cell) -> str:
        """
        Extract clean text from a table cell, preserving line breaks for multi-line content
        """
        if not cell:
            return ""
        
        # Get text content but preserve line breaks for parsing
        text = cell.get_text(separator='\n', strip=True)
        
        # Handle common HTML entities
        text = text.replace('\xa0', ' ').replace('\t', ' ')
        
        return text
    
    def _parse_facility_name_cell(self, cell) -> Dict:
        """
        Parse the facility name cell - extract name from first <b> tag, 
        then capture everything until the next <b> tag as full address
        """
        if not cell:
            return {
                "facility_name": "",
                "full_address": "",
                "phone": ""
            }
        
        # Extract facility name from first <b> tag
        first_bold = cell.find('b')
        facility_name = first_bold.get_text(strip=True) if first_bold else ""
        
        # Get all text after the first bold tag until the next bold tag
        full_address = ""
        phone = ""
        
        if first_bold:
            # Get all siblings after the first bold tag
            current = first_bold.next_sibling
            address_parts = []
            
            while current:
                if current.name == 'b':  # Stop at next bold tag (Phone:)
                    break
                elif current.name == 'br':
                    address_parts.append(' ')
                elif current.string:
                    # Clean up text, replace &nbsp; with spaces
                    text = current.string.replace('\xa0', ' ').strip()
                    if text:
                        address_parts.append(text)
                elif hasattr(current, 'get_text'):
                    text = current.get_text().replace('\xa0', ' ').strip()
                    if text:
                        address_parts.append(text)
                
                current = current.next_sibling
            
            # Join address parts and clean up
            full_address = ''.join(address_parts).strip()
            full_address = re.sub(r'\s+', ' ', full_address)  # Normalize whitespace
        
        # Extract phone number from the second bold tag area
        phone_bold = cell.find('b', string=re.compile(r'Phone:', re.IGNORECASE))
        if phone_bold and phone_bold.parent:
            phone_text = phone_bold.parent.get_text()
            phone_match = re.search(r'\(?\d{3}\)?[-.\s]*\d{3}[-.\s]*\d{4}', phone_text)
            if phone_match:
                phone = phone_match.group()
        
        return {
            "facility_name": facility_name.strip(),
            "full_address": full_address.strip(),
            "phone": phone.strip()
        }
        
    def _parse_table_row(self, row, row_index: int) -> Optional[Dict]:
        """
        Parse a single table row into facility data
        """
        cells = row.find_all(['td', 'th'])
        
        if len(cells) < 10:  # Should have at least 10 columns based on the header
            logger.warning(f"Row {row_index}: Insufficient cells ({len(cells)})")
            return None
        
        try:
            # Extract and parse the facility name cell (contains name, address, phone)
            facility_name_cell = cells[2] if len(cells) > 2 else None
            parsed_facility_info = self._parse_facility_name_cell(facility_name_cell)
            
            # Extract basic facility information from other cells
            facility_info = {
                "program_category": self._extract_cell_text(cells[1]) if len(cells) > 1 else "",
                "program_name": self._extract_cell_text(cells[3]) if len(cells) > 3 else "",
                "executive_director": self._extract_cell_text(cells[4]) if len(cells) > 4 else "",
                "bed_capacity": self._extract_cell_text(cells[5]) if len(cells) > 5 else "",
                "license_exp_date": self._extract_cell_text(cells[6]) if len(cells) > 6 else "",
                "relicense_visit_date": self._extract_cell_text(cells[7]) if len(cells) > 7 else "",
                "action": self._extract_cell_text(cells[8]) if len(cells) > 8 else ""
            }
            
            # Merge the parsed facility info
            facility_info.update(parsed_facility_info)
            
            # Validate essential fields
            if not facility_info["facility_name"] or len(facility_info["facility_name"]) < 3:
                logger.warning(f"Row {row_index}: Invalid facility name")
                return None
            
            # Extract reports from the last cell (Report column)
            reports = []
            if len(cells) > 10:
                report_cell = cells[10]
                reports = self._extract_reports_from_cell(report_cell)
            
            facility = {
                "facility_info": facility_info,
                "reports": reports
            }
            
            logger.debug(f"Parsed facility: {facility_info['facility_name']} with {len(reports)} reports")
            return facility
            
        except Exception as e:
            logger.error(f"Error parsing row {row_index}: {e}")
            return None
    
    def _extract_reports_from_cell(self, cell) -> List[Dict]:
        """
        Extract and split individual reports from the report cell
        """
        if not cell:
            return []
        
        reports = []
        
        # Get all text content from the cell
        cell_text = cell.get_text()
        
        logger.debug(f"Report cell text preview: {cell_text[:200]}...")
        
        # First, try the +++pattern+++
        pattern1 = r'(\d{4,})\+\+\+([^+]+?)\+\+\+(.*?)(?=\d{4,}\+\+\+|$)'
        matches1 = re.findall(pattern1, cell_text, re.DOTALL)
        
        for match in matches1:
            report_id = match[0].strip()
            report_date = match[1].strip()
            raw_content = match[2].strip()
            
            if report_id and report_date:
                report_date = self._clean_date(report_date)
                if report_date:
                    # Parse and categorize the report content
                    categorized_content = self._categorize_report_content(raw_content)
                    
                    report = {
                        "report_id": report_id,
                        "report_date": report_date,
                        "raw_content": raw_content,
                        "content_length": len(raw_content),
                        "categories": categorized_content["categories"],
                        "summary": categorized_content["summary"]
                    }
                    reports.append(report)
        
        # If no reports found with +++, try alternative patterns
        if not reports:
            # Look for links or other report indicators
            links = cell.find_all('a')
            for link in links:
                link_text = link.get_text(strip=True)
                href = link.get('href', '')
                
                # Extract report ID from link text or href
                report_id_match = re.search(r'\d{4,}', link_text + ' ' + href)
                if report_id_match:
                    # For link-based reports, we might not have full content to categorize
                    report = {
                        "report_id": report_id_match.group(),
                        "report_date": "",  # May not be available in link format
                        "raw_content": link_text,
                        "report_url": href,
                        "content_length": len(link_text),
                        "categories": {"link_only": link_text},
                        "summary": f"Report available via link: {href}"
                    }
                    reports.append(report)
        
        return reports
    
    def _categorize_report_content(self, content: str) -> Dict:
        """
        Parse and categorize different sections within report content
        """
        if not content:
            return {"categories": {}, "summary": ""}
        
        categories = {}
        
        # Clean the content first
        content = self._clean_report_content(content)
        
        # Check if this is a structured DCF report
        is_structured = self._is_structured_report(content)
        
        if is_structured:
            # Extract structured sections
            logger.debug("Processing structured DCF report")
            
            # Extract "List of Areas / Topics covered during visit:" or "Areas / Topics covered during visit:"
            areas_topics = self._extract_areas_topics(content)
            if areas_topics:
                categories["areas_topics_covered"] = areas_topics
            
            # Extract corrective actions (with variations in heading)
            corrective_actions = self._extract_corrective_actions(content)
            if corrective_actions:
                categories["corrective_actions"] = corrective_actions
            
            # Extract recommendations section (if present)
            recommendations = self._extract_recommendations(content)
            if recommendations:
                categories["recommendations"] = recommendations
            
            # Extract non-compliance issues
            non_compliance = self._extract_non_compliance(content)
            if non_compliance:
                categories["regulatory_non_compliance"] = non_compliance
            
            # Extract basic facility info from the report
            facility_info = self._extract_facility_info_from_report(content)
            if facility_info:
                categories["visit_details"] = facility_info
            
            # Extract any incident information
            incident_info = self._extract_incident_info(content)
            if incident_info:
                categories["incidents"] = incident_info
        else:
            # Not a structured report - dump the whole content
            logger.debug("Processing unstructured report - dumping full content")
            categories["full_report_content"] = content
            categories["report_type"] = "unstructured"
        
        # Generate a summary
        summary = self._generate_report_summary(content, categories)
        
        return {
            "categories": categories,
            "summary": summary,
            "is_structured": is_structured
        }
    
    def _is_structured_report(self, content: str) -> bool:
        """
        Determine if this is a structured DCF report based on key indicators
        """
        structure_indicators = [
            r'Areas?\s*/\s*Topics covered during visit:',
            r'Areas of regulatory non-compliance identified during this visit:',
            r'NAME OF FACILITY / PROGRAM:',
            r'TIME OF VISIT \(FROM - TO\):',
            r'Field Visit Reporting Form'
        ]
        
        indicator_count = 0
        for pattern in structure_indicators:
            if re.search(pattern, content, re.IGNORECASE):
                indicator_count += 1
        
        # If we find at least 3 of these indicators, consider it structured
        return indicator_count >= 3
    
    def _extract_areas_topics(self, content: str) -> List[str]:
        """
        Extract the bulleted list under "List of Areas / Topics covered during visit:" or "Areas / Topics covered during visit:"
        """
        areas_topics = []
        
        # Look for either variation of the section header
        patterns = [
            r'List of Areas / Topics covered during visit:\s*\n?(.*?)(?=\n[A-Z][^•\n]*:|During the past quarter|$)',
            r'Areas / Topics covered during visit:\s*\n?(.*?)(?=\n[A-Z][^•\n]*:|During the past quarter|$)'
        ]
        
        section_content = None
        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                section_content = match.group(1)
                break
        
        if section_content:
            # Split by bullet points (• or other common bullet characters)
            bullet_pattern = r'[•\*\-]\s*([^\n•\*\-]+(?:\n(?!\s*[•\*\-])[^\n]*)*)'
            bullets = re.findall(bullet_pattern, section_content, re.MULTILINE)
            
            for bullet in bullets:
                # Clean up the bullet point text
                cleaned_bullet = re.sub(r'\s+', ' ', bullet.strip())
                if len(cleaned_bullet) > 5:  # Only include substantial content
                    areas_topics.append(cleaned_bullet)
        
        return areas_topics
    
    def _extract_corrective_actions(self, content: str) -> List[str]:
        """
        Extract corrective actions section (with variations in heading)
        """
        corrective_actions = []
        
        patterns = [
            r'Corrective Actions implemented as a result of previous visit:\s*\n?(.*?)(?=\n[A-Z][^•\n]*:|$)',
            r'Corrections implemented as a result of previous visit:\s*\n?(.*?)(?=\n[A-Z][^•\n]*:|$)'
        ]
        
        section_content = None
        for pattern in patterns:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                section_content = match.group(1)
                break
        
        if section_content:
            # Check if it says "Not applicable", "N/A", or similar
            if re.search(r'not applicable|none|n/a', section_content, re.IGNORECASE):
                corrective_actions.append("Not applicable")
            else:
                # Split by bullet points
                bullet_pattern = r'[•\*\-]\s*([^\n•\*\-]+(?:\n(?!\s*[•\*\-])[^\n]*)*)'
                bullets = re.findall(bullet_pattern, section_content, re.MULTILINE)
                
                for bullet in bullets:
                    cleaned_bullet = re.sub(r'\s+', ' ', bullet.strip())
                    if len(cleaned_bullet) > 3:
                        corrective_actions.append(cleaned_bullet)
                
                # If no bullets found, treat the whole section as one item
                if not corrective_actions and section_content.strip():
                    cleaned_content = re.sub(r'\s+', ' ', section_content.strip())
                    corrective_actions.append(cleaned_content)
        
        return corrective_actions
    
    def _extract_recommendations(self, content: str) -> List[str]:
        """
        Extract recommendations section
        """
        recommendations = []
        
        pattern = r'Recommendations:\s*\([^)]+\)\s*\n?(.*?)(?=\n[A-Z][^•\n]*:|$)'
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        
        if match:
            section_content = match.group(1)
            
            # Check if it says "N/A" or similar
            if re.search(r'not applicable|none|n/a', section_content, re.IGNORECASE):
                recommendations.append("N/A")
            else:
                # Split by bullet points
                bullet_pattern = r'[•\*\-]\s*([^\n•\*\-]+(?:\n(?!\s*[•\*\-])[^\n]*)*)'
                bullets = re.findall(bullet_pattern, section_content, re.MULTILINE)
                
                for bullet in bullets:
                    cleaned_bullet = re.sub(r'\s+', ' ', bullet.strip())
                    if len(cleaned_bullet) > 3:
                        recommendations.append(cleaned_bullet)
                
                # If no bullets found, treat the whole section as one item
                if not recommendations and section_content.strip():
                    cleaned_content = re.sub(r'\s+', ' ', section_content.strip())
                    recommendations.append(cleaned_content)
        
        return recommendations
    
    def _extract_non_compliance(self, content: str) -> List[Dict]:
        """
        Extract areas of regulatory non-compliance with regulation citations
        """
        non_compliance = []
        
        pattern = r'Areas of regulatory non-compliance identified during this visit:\s*\n?(.*?)(?=Please submit|$)'
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        
        if match:
            section_content = match.group(1).strip()
            
            # Check for any variation of "no violations"
            no_violation_patterns = [
                r'none',
                r'not applicable', 
                r'\bn/?a\b',
                r'n/a'
            ]
            
            is_no_violation = False
            for pattern in no_violation_patterns:
                if re.search(pattern, section_content, re.IGNORECASE):
                    is_no_violation = True
                    break
            
            if is_no_violation:
                non_compliance.append({"type": "none", "description": "None"})
            else:
                # Parse structured non-compliance (with regulation citations)
                citation_pattern = r'([A-Z][^:]+):\s*([^\.]+\.)\s*(.+?)(?=[A-Z][^:]+:|$)'
                citations = re.findall(citation_pattern, section_content, re.DOTALL)
                
                for citation in citations:
                    area_type = citation[0].strip()
                    regulation = citation[1].strip()
                    description = citation[2].strip()
                    
                    non_compliance.append({
                        "area_type": area_type,
                        "regulation": regulation,
                        "description": re.sub(r'\s+', ' ', description)
                    })
                
                # If no structured citations found, try bullet points
                if not non_compliance:
                    bullet_pattern = r'[•\*\-]\s*([^\n•\*\-]+(?:\n(?!\s*[•\*\-])[^\n]*)*)'
                    bullets = re.findall(bullet_pattern, section_content, re.MULTILINE)
                    
                    for bullet in bullets:
                        cleaned_bullet = re.sub(r'\s+', ' ', bullet.strip())
                        if len(cleaned_bullet) > 5:
                            non_compliance.append({
                                "type": "general",
                                "description": cleaned_bullet
                            })
                
                # Last resort - but only if it doesn't contain any "no violation" words
                if not non_compliance and section_content.strip():
                    cleaned_content = re.sub(r'\s+', ' ', section_content.strip())
                    non_compliance.append({
                        "type": "general", 
                        "description": cleaned_content
                    })
        
        return non_compliance
    
    def _extract_incident_info(self, content: str) -> List[str]:
        """
        Extract incident information from quarterly summaries
        """
        incidents = []
        
        # Look for incident-related text
        incident_pattern = r'During the past quarter[^\.]*incident[^\.]*\.(.*?)(?=\n[A-Z][^•\n]*:|$)'
        matches = re.findall(incident_pattern, content, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            cleaned_incident = re.sub(r'\s+', ' ', match.strip())
            if len(cleaned_incident) > 10:
                incidents.append(cleaned_incident)
        
        return incidents
    
    def _extract_facility_info_from_report(self, content: str) -> Dict:
        """
        Extract basic visit details from the report header
        """
        visit_info = {}
        
        # Extract facility name
        facility_match = re.search(r'NAME OF FACILITY / PROGRAM:\s*([^\n]+)', content, re.IGNORECASE)
        if facility_match:
            visit_info["facility_name"] = facility_match.group(1).strip()
        
        # Extract visit time and date
        time_match = re.search(r'TIME OF VISIT \(FROM - TO\):\s*([^\s]+).*?DATE:\s*([^\n]+)', content, re.IGNORECASE)
        if time_match:
            visit_info["visit_time"] = time_match.group(1).strip()
            visit_info["visit_date"] = time_match.group(2).strip()
        
        # Extract agency personnel
        personnel_pattern = r'AGENCY PERSONNEL WHO PARTICIPATED:\s*\n.*?\n(.*?)(?=\n(?:List of )?Areas|$)'
        personnel_match = re.search(personnel_pattern, content, re.DOTALL | re.IGNORECASE)
        if personnel_match:
            personnel_section = personnel_match.group(1)
            # Extract job titles and names
            personnel_lines = [line.strip() for line in personnel_section.split('\n') 
                             if line.strip() and 'Name' not in line and 'Job Title' not in line]
            visit_info["personnel"] = personnel_lines
        
        return visit_info
    
    def _generate_report_summary(self, content: str, categories: Dict) -> str:
        """
        Generate a brief summary of the report based on categorized content
        """
        if not content:
            return ""
        
        summary_parts = []
        
        # Check if structured or unstructured
        if not categories.get("is_structured", True):
            word_count = len(content.split())
            return f"Unstructured report with {word_count} words of content."
        
        # Add visit type info
        if categories.get("visit_details", {}).get("visit_date"):
            visit_date = categories["visit_details"]["visit_date"]
            summary_parts.append(f"Visit conducted on {visit_date}.")
        
        # Add areas covered count
        areas_count = len(categories.get("areas_topics_covered", []))
        if areas_count > 0:
            summary_parts.append(f"Visit covered {areas_count} main areas/topics.")
        
        # Add compliance status
        non_compliance = categories.get("regulatory_non_compliance", [])
        if non_compliance:
            if len(non_compliance) == 1 and non_compliance[0].get("type") == "none":
                summary_parts.append("No compliance issues identified.")
            else:
                summary_parts.append(f"Found {len(non_compliance)} compliance issues.")
        
        # Add corrective actions status
        corrective_actions = categories.get("corrective_actions", [])
        if corrective_actions and corrective_actions[0].lower() not in ["not applicable", "n/a"]:
            summary_parts.append(f"Implemented {len(corrective_actions)} corrective actions.")
        
        # Add recommendations if present
        recommendations = categories.get("recommendations", [])
        if recommendations and recommendations[0].lower() != "n/a":
            summary_parts.append(f"Includes {len(recommendations)} recommendations.")
        
        return " ".join(summary_parts)
    
    def _clean_date(self, date_str: str) -> Optional[str]:
        """
        Clean and validate date strings
        """
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        # Validate basic date format (MM/DD/YYYY or similar)
        date_pattern = r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}'
        if re.search(date_pattern, date_str):
            return date_str
        
        # Handle other date formats if needed
        return date_str if date_str else None
    
    def _clean_report_content(self, content: str) -> str:
        """
        Clean report content while preserving structure
        """
        if not content:
            return ""
        
        # Clean up extra whitespace but preserve paragraph breaks
        lines = content.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if line:
                cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    def scrape(self) -> Optional[Dict]:
        """
        Main scraping method
        """
        logger.info("Starting DCF facility scraping")
        
        # Fetch page
        html_content = self.fetch_page()
        if not html_content:
            return None
        
        # Parse HTML
        soup = self.parse_html(html_content)
        if not soup:
            return None
        
        # Extract data
        facilities = self.extract_table_data(soup)
        
        if not facilities:
            logger.error("No facilities found")
            return None
        
        result = {
            "total_facilities": len(facilities),
            "scraped_timestamp": "2024-09-22T12:00:00Z",
            "source_url": self.url,
            "scraping_notes": {
                "parser": "BeautifulSoup HTML table parsing",
                "total_reports": sum(len(f.get("reports", [])) for f in facilities)
            },
            "facilities": facilities
        }
        
        logger.info(f"Scraping completed: {len(facilities)} facilities, {result['scraping_notes']['total_reports']} total reports")
        return result

def save_to_json(data: Dict, filename: str = "ct_reports.json") -> bool:
    """
    Save data to JSON with proper formatting
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
        logger.info(f"Data saved to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving to JSON: {e}")
        return False

def main():
    """
    Main execution function
    """
    scraper = DCFFacilityScraper()
    
    data = scraper.scrape()
    
    if data:
        print(f"\nSuccessfully scraped {data['total_facilities']} facilities")
        print(f"Total reports found: {data['scraping_notes']['total_reports']}")
        
        # Save to JSON
        if save_to_json(data):
            print("Data saved successfully!")
        
        # Show detailed sample
        if data['facilities']:
            sample = data['facilities'][0]
            print(f"\nSample facility:")
            print(f"   Name: {sample['facility_info']['facility_name']}")
            print(f"   Address: {sample['facility_info'].get('full_address', 'N/A')}")
            print(f"   Phone: {sample['facility_info'].get('phone', 'N/A')}")
            print(f"   Program: {sample['facility_info']['program_name']}")
            print(f"   Director: {sample['facility_info']['executive_director']}")
            print(f"   Capacity: {sample['facility_info']['bed_capacity']}")
            print(f"   Reports: {len(sample['reports'])}")
            
            if sample['reports']:
                report = sample['reports'][0]
                print(f"\nSample report:")
                print(f"   ID: {report['report_id']}")
                print(f"   Date: {report.get('report_date', 'N/A')}")
                print(f"   Length: {report['content_length']} characters")
                
                # Handle both old and new report content formats
                content_preview = ""
                if 'raw_content' in report:
                    content_preview = report['raw_content'][:200]
                elif 'report_content' in report:
                    content_preview = report['report_content'][:200]
                else:
                    content_preview = "No content available"
                
                print(f"   Preview: {content_preview}...")
                
                # Show categories if available
                if 'categories' in report and report['categories']:
                    print(f"   Categories found: {list(report['categories'].keys())}")
                    if 'summary' in report:
                        print(f"   Summary: {report['summary']}")
    else:
        print("Failed to scrape data")
        logger.error("Scraping failed - check logs for details")

if __name__ == "__main__":
    main()