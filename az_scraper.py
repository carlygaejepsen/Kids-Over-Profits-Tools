"""
Arizona ADHS Care Check Facility Scraper

Fetches facility and inspection/deficiency data from the AZ Care Check
Salesforce Aura API, then POSTs to the inspections API for MySQL storage.

No browser needed — calls the public Salesforce Apex endpoints directly.
"""

import html
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional

import requests

from inspection_api_client import post_facilities_to_api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── KOP API configuration ───────────────────────────────────────────
API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("INSPECTIONS_API_KEY", "CHANGE_ME")

# ── AZ Care Check Salesforce Aura endpoints ─────────────────────────
AURA_URL = "https://azcarecheck.azdhs.gov/s/sfsites/aura"
AURA_CONTEXT = {
    "mode": "PROD",
    "fwuid": "TXFWNVprQUZzQnEtNXVXYTFLQ2ppdzJEa1N5enhOU3R5QWl2VzNveFZTbGcxMy4tMjE0NzQ4MzY0OC4xMzEwNzIwMA",
    "app": "siteforce:communityApp",
    "loaded": {
        "APPLICATION@markup://siteforce:communityApp": "1537_wmTAUxhOaM_47EClrN56Dw",
    },
    "dn": [],
    "globals": {},
    "uad": True,
}

# ── Facility IDs to scrape ───────────────────────────────────────────
FACILITY_IDS = [
    "001cs00000WoDzyAAF", "001cs00000WoDzkAAF", "001cs00000WoAt6AAF",
    "001cs00000WoCLPAA3", "001cs00000WoBr1AAF", "001cs00000Wo8OtAAJ",
    "001cs00000Wo8bUAAR", "001cs00000Wo8OrAAJ", "001cs00000WoBr0AAF",
    "001cs00000WoAt9AAF", "001cs00000WoDSsAAN", "001cs00000WoAt8AAF",
    "001cs00000Wo4m9AAB", "001cs00000Wo9AKAAZ", "001cs00000Wo86wAAB",
    "001cs00000Wo9X1AAJ", "001cs00000Wo6GAAAZ", "001cs00000WoC6MAAV",
    "0018y000008fB0sAAE", "001cs00000WoC6NAAV", "001cs00000WoB37AAF",
    "001cs00000Wo9WaAAJ", "001cs00000WoGW9AAN", "001cs00000WoFldAAF",
    "001cs00000Wo6bUAAR", "001cs00000WoB38AAF", "001cs00000Wo540AAB",
    "001cs00000WnyTQAAZ", "001cs00000WnyTRAAZ", "001cs00000WoGWAAA3",
    "001cs00000WnyTeAAJ", "001cs00000WnyTfAAJ", "001cs00000WnyTgAAJ",
    "001cs00000WoEFPAA3", "001cs00000WoEFQAA3", "001cs00000WoEFLAA3",
    "001cs00000WoEFMAA3", "001cs00000WoEFOAA3", "001cs00000WnyTSAAZ",
    "001cs00000WoEFRAA3", "001cs00000WnyThAAJ", "001cs00000WnyTiAAJ",
    "001cs00000WnyTjAAJ", "001cs00000Wo8dxAAB", "001cs00000WoCXnAAN",
    "001cs00000WoCXoAAN", "001cs00000Wo5XmAAJ", "001cs00000Wo8dyAAB",
    "001cs00000Wo8wBAAR", "001cs00000WoGguAAF", "001cs00000WoCXcAAN",
    "001cs00000WoCXdAAN", "001cs00000Wo8e0AAB", "001cs00000WnyTaAAJ",
    "001cs00000WoCXeAAN", "001cs00000WnyTdAAJ", "001cs00000WoC3jAAF",
    "001cs00000WnyTbAAJ", "001cs00000WoCXfAAN", "001cs00000WoBcdAAF",
    "001cs00000WoCJYAA3", "001cs00000WoFmHAAV", "001cs00000WoEiXAAV",
    "001cs00000WoCJoAAN", "001cs00000WoESjAAN", "001cs00000Wo6s4AAB",
    "001cs00000Wo87nAAB", "001cs00000WoEyqAAF", "001cs00000Wo8dzAAB",
    "001cs00000WoCXgAAN", "001cs00000WoDTqAAN", "001cs00000WoGgvAAF",
    "001cs00000WoCXhAAN", "001cs00000Wo6bWAAR", "001cs00000WoCXiAAN",
    "001cs00000Wo8w9AAB", "001cs00000Wo5bxAAB", "001cs00000Wo5H5AAJ",
    "001cs00000WoCJZAA3", "001cs00000Wo9rKAAR", "001cs00000WoCXjAAN",
    "001cs00000Wo8OoAAJ", "001cs00000Wo9X0AAJ", "001cs00000Wo8dwAAB",
    "001cs00000Wo9pkAAB", "001cs00000Wo4VOAAZ", "0018y000008f8SeAAI",
    "001cs00000WoCXlAAN", "001cs00000WoEyOAAV", "001cs00000WoEDuAAN",
    "001cs00000Wo7FjAAJ", "001cs00000WoBceAAF", "001cs00000Wo6YRAAZ",
    "001cs00000WoFnjAAF", "001cs00000WoB3CAAV", "001cs00000Wo7UCAAZ",
    "001cs00000Wo6YSAAZ", "001cs00000WoB0lAAF", "001cs00000Wo9qpAAB",
    "001cs00000Wo8wAAAR", "001cs00000WoCXRAA3", "001cs00000WoC3kAAF",
    "001cs00000Wo6s5AAB", "001cs00000WoEEaAAN", "001cs00000WoFaiAAF",
    "001cs00000WoB0kAAF", "001cs00000WoATWAA3", "001cs00000WoB3DAAV",
    "001cs00000WoCXmAAN", "001cs00000WoAtZAAV", "001cs00000Wo8vFAAR",
    "001cs00000WoDChAAN", "001cs00000WoCKBAA3", "001cs00000WoC6KAAV",
    "001cs00000WoB35AAF", "001cs00000WoFnyAAF", "001cs00000WoB2VAAV",
    "001cs00000WoGexAAF",
]


def _strip_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", " ", text)
    clean = html.unescape(clean)
    return re.sub(r"\s+", " ", clean).strip()


class AZFacilityScraper:
    """Fetches AZ ADHS facility and inspection data via Salesforce Aura API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        })
        self._action_id = 0
        self.all_facilities: List[Dict] = []

    def _call_apex(
        self,
        classname: str,
        method: str,
        params: Dict,
        cacheable: bool = True,
    ) -> any:
        """Call a Salesforce Apex controller method via the Aura framework."""
        self._action_id += 1
        import json

        message = json.dumps({
            "actions": [{
                "id": f"{self._action_id};a",
                "descriptor": "aura://ApexActionController/ACTION$execute",
                "callingDescriptor": "UNKNOWN",
                "params": {
                    "namespace": "",
                    "classname": classname,
                    "method": method,
                    "params": params,
                    "cacheable": cacheable,
                    "isContinuation": False,
                },
            }],
        })

        resp = self.session.post(
            f"{AURA_URL}?r={self._action_id}&aura.ApexAction.execute=1",
            data={
                "message": message,
                "aura.context": json.dumps(AURA_CONTEXT),
                "aura.pageURI": "/s/",
                "aura.token": "null",
            },
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()
        action = result["actions"][0]

        if action["state"] != "SUCCESS":
            errors = action.get("error", [])
            msg = errors[0].get("message", "unknown") if errors else "unknown"
            raise RuntimeError(f"{classname}.{method} failed: {msg}")

        rv = action["returnValue"]
        return rv.get("returnValue") if isinstance(rv, dict) else rv

    def _get_facility_details(self, facility_id: str) -> Dict:
        """Fetch facility details."""
        return self._call_apex(
            "AZCCFacilityDetailsTabController",
            "getFacilityDetails",
            {"facilityId": facility_id},
        )

    def _get_inspections(self, facility_id: str) -> List[Dict]:
        """Fetch the inspection list for a facility."""
        return self._call_apex(
            "AZCCInspectionHistoryController",
            "getFacilityOrLicenseInspections",
            {"facilityId": facility_id, "licenseId": None},
        )

    def _get_inspection_items(self, inspection_id: str) -> List[Dict]:
        """Fetch deficiency items for a specific inspection."""
        result = self._call_apex(
            "AZCCInspectionHistoryController",
            "getInspectionItemSODWrap",
            {"InspectionId": inspection_id},
            cacheable=False,
        )
        return result.get("inspectionItems", []) if isinstance(result, dict) else []

    @staticmethod
    def _build_facility_info(details: Dict) -> Dict:
        """Map AZ facility details to the inspections schema."""
        phone = details.get("phone") or ""
        # Clean phone format — might have "tel:" prefix
        phone = phone.replace("tel:", "").strip()
        if phone and len(phone) == 10:
            phone = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"

        return {
            "facility_name": details.get("legalName") or "",
            "program_name": details.get("license") or details.get("externalFacilitySearchId") or "",
            "program_category": details.get("facilityType") or "",
            "full_address": details.get("address") or "",
            "phone": phone,
            "bed_capacity": str(details.get("totalCapacity") or ""),
            "executive_director": details.get("chiefAdministrativeOfficer") or "",
            "license_exp_date": details.get("expirationDate") or "",
            "relicense_visit_date": details.get("effectiveDate") or "",
            "action": details.get("facilityStatus") or "",
        }

    @staticmethod
    def _build_reports(inspections: List[Dict], items_by_inspection: Dict[str, List[Dict]]) -> List[Dict]:
        """Convert AZ inspections + deficiency items into the reports schema."""
        reports = []

        for insp in inspections:
            insp_id = insp.get("inspectionId") or insp.get("Id") or ""
            insp_date = insp.get("inspectionDates") or ""
            insp_type = insp.get("inspectionType") or ""
            insp_name = insp.get("inspectionName") or ""
            cert_num = insp.get("certificateNumber") or ""
            initial_comments = insp.get("initialComments") or ""

            items = items_by_inspection.get(insp_id, [])

            # Build deficiency list matching the format az_reports.js expects
            deficiencies = []
            for item in items:
                rule_raw = item.get("ItemDescription__c") or item.get("Name") or ""
                evidence_raw = item.get("Evidence__c") or ""

                # Split evidence into evidence + findings
                evidence_text = _strip_html(evidence_raw)
                findings_text = ""
                findings_split = re.split(
                    r"Findings include:", evidence_text, maxsplit=1, flags=re.IGNORECASE
                )
                if len(findings_split) > 1:
                    evidence_text = findings_split[0].strip()
                    findings_text = findings_split[1].strip()

                deficiencies.append({
                    "rule": _strip_html(rule_raw),
                    "evidence": evidence_text,
                    "findings": findings_text,
                })

            # categories matches what az_reports.js reads from the JSON files
            categories = {
                "inspection_number": insp_name,
                "inspection_date": insp_date,
                "inspection_type": insp_type,
                "certificate_number": cert_num,
                "deficiencies": deficiencies,
            }

            reports.append({
                "report_id": insp_name or insp_id,
                "report_date": insp_date,
                "raw_content": initial_comments,
                "content_length": len(initial_comments),
                "summary": f"{insp_type} - {insp_date}" if insp_type else insp_date,
                "categories": categories,
            })

        return reports

    def scrape(self, facility_ids: Optional[List[str]] = None) -> List[Dict]:
        """Scrape all facilities and return the collected list."""
        ids = facility_ids or FACILITY_IDS
        logger.info(f"Starting AZ scrape for {len(ids)} facilities")

        for i, fid in enumerate(ids):
            logger.info(f"[{i+1}/{len(ids)}] Facility {fid}")
            try:
                # Step 1: Get facility details
                details = self._get_facility_details(fid)
                facility_info = self._build_facility_info(details)
                logger.info(f"  {facility_info['facility_name']}")

                # Step 2: Get inspection list
                inspections = self._get_inspections(fid)
                logger.info(f"  {len(inspections)} inspections")

                # Step 3: Get deficiency items for each inspection
                items_by_inspection: Dict[str, List[Dict]] = {}
                for insp in inspections:
                    insp_id = insp.get("inspectionId") or insp.get("Id") or ""
                    if not insp_id:
                        continue
                    try:
                        items = self._get_inspection_items(insp_id)
                        items_by_inspection[insp_id] = items
                    except Exception as e:
                        logger.warning(f"    Could not get items for {insp_id}: {e}")

                # Step 4: Build reports
                reports = self._build_reports(inspections, items_by_inspection)
                total_deficiencies = sum(
                    len(r["categories"].get("deficiencies", []))
                    for r in reports
                )
                logger.info(f"  {len(reports)} reports, {total_deficiencies} deficiencies")

                self.all_facilities.append({
                    "facility_info": facility_info,
                    "reports": reports,
                })

            except requests.exceptions.RequestException as e:
                logger.error(f"  HTTP error on {fid}: {e}")
                continue
            except Exception as e:
                logger.error(f"  ERROR on {fid}: {e}")
                continue

        logger.info(f"Scraping complete: {len(self.all_facilities)} facilities")
        return self.all_facilities


# ── API posting ──────────────────────────────────────────────────────

def save_to_api(facilities: List[Dict]) -> bool:
    """POST all collected facility data to the live site for MySQL storage."""
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="AZ",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main():
    scraper = AZFacilityScraper()
    facilities = scraper.scrape()

    if facilities:
        logger.info(f"Scraped {len(facilities)} facilities — posting to API")
        if save_to_api(facilities):
            logger.info("Data saved to database successfully!")
        else:
            logger.error("API save failed — check logs above")
    else:
        logger.warning("No facilities scraped")


if __name__ == "__main__":
    main()
