"""
Texas HHS Childcare Facility Scraper

Fetches deficiency/citation data from the TX HHS internal JSON API
for 24-Hour Residential operations, then POSTs to the inspections API
for storage in MySQL (same pipeline as the CT scraper).

No browser automation needed — uses requests against the same API
endpoints that the childcare.hhs.texas.gov React app calls.
"""

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

# ── API configuration ────────────────────────────────────────────────
API_URL = os.getenv(
    "INSPECTIONS_API_URL",
    "https://kidsoverprofits.org/wp-content/themes/child/api/inspections-write.php",
)
API_KEY = os.getenv("INSPECTIONS_API_KEY", "CHANGE_ME")

# ── TX HHS endpoints ────────────────────────────────────────────────
TX_BASE = "https://childcare.hhs.texas.gov/__endpoint"
TX_TOKEN_URL = f"{TX_BASE}/public/security/token"
TX_SEARCH_URL = f"{TX_BASE}/ps/res24Care/providers"
TX_DETAILS_URL = f"{TX_BASE}/ps/res24Care/operationDetails"  # /{providerId}
TX_HISTORY_URL = f"{TX_BASE}/ps/daycare/providerOpHistory"  # /{providerId}

# ── Operation IDs to scrape ──────────────────────────────────────────
OPERATION_IDS = [
    "1773", "5541", "6065", "254646", "516810", "522278", "827024",
    "1247466", "1409086", "1540737", "1562665", "1659892", "1668161",
    "1700503", "1714518", "1724502", "1762810", "1769798", "1782716",
    "853346", "1509926", "204641", "215752", "1693667", "1693740",
    "531665", "1711621", "1145426", "1773300", "817874", "1765540",
    "1738882", "503875", "1784559", "1786879", "1791476", "1800159",
    "1807875", "1808042", "1818188", "1803258", "1809887", "1812895",
    "1814618",
    "290", "5913", "6005", "15267", "27177", "42721", "185528",
    "191133", "245627", "541473", "846369", "858505", "859225",
    "1033546", "1105326", "1559917", "1692813", "1708504", "1715609",
    "1760975", "1761134", "1763000", "228620", "1693629", "1533621",
    "1721847", "1774248", "823154", "1735762", "838067", "839957",
    "511519", "1757679", "1784695", "1797723", "1799271", "1796998",
    "1797609", "1797707", "1803478", "1813025",
    "1165", "5599", "6040", "6787", "13621", "23871", "54326",
    "154645", "844802", "847985", "849411", "1063806", "1244326",
    "1667912", "1684228", "1689854", "1724162", "1743981", "1769797",
    "1776121", "1698799", "1705965", "1706626", "1693628", "1530337",
    "1681396", "892238", "1773996", "556189", "1752752", "1746008",
    "1738202", "505173", "1760485", "1797726", "1813303", "1811815",
    "1819452", "1805359", "1813027", "1806666", "1806667",
    "5761", "6085", "24719", "50218", "170567", "254110", "254538",
    "254729", "256371", "812341", "1658979", "1678450", "1689755",
    "1692025", "1701291", "1714813", "1761811", "1767744", "66232",
    "851881", "852537", "1672415", "1704326", "1726200", "1000050",
    "1663308", "1717490", "1773956", "817512", "1687429", "1763576",
    "1736601", "1784246", "1786371", "1786553", "1786589", "1797720",
    "1797733", "1795126", "1797648", "1808575", "1815891", "1803139",
    "1803261", "1803477", "1809921",
    "1545", "5914", "7184", "18027", "36943", "244459", "849589",
    "877478", "1632090", "1658373", "1675992", "1690684", "1701592",
    "1763004", "1763171", "1769316", "69531", "1699024", "1700373",
    "1506800", "1710607", "1572157", "1779839", "1780336", "1764006",
    "1752267", "1753012", "1746886", "1760486", "1787751", "1786312",
    "1790926", "1791182", "1793529", "1793844", "1798274", "1795441",
    "1808412", "1808635", "1803493", "1805546", "1814507",
    "184", "360", "1639", "5570", "178491", "181054", "251707",
    "813238", "1268266", "1496362", "1502634", "1594840", "1701205",
    "1714475", "1715401", "1723466", "1724559", "1734404", "1741334",
    "1744038", "1744228", "1754771", "1768076", "855318", "1704643",
    "1706226", "1731240", "204485", "1695157", "1681475", "980590",
    "1000466", "1721455", "1721595", "1617305", "1716949", "1764644",
    "840381", "1788833", "1789393", "1808915", "1811997", "1810414",
    "1804296", "1804766", "1806078", "1800545", "1806655",
    "1535", "2043", "40724", "53712", "517689", "540238", "848614",
    "860617", "860873", "888971", "1709632", "1724200", "1741914",
    "1756205", "1762558", "66554", "68824", "1729764", "1508898",
    "1722378", "1716984", "554314", "1752756", "1759648", "1792300",
    "1794219", "1795211", "1796056", "1811084", "1811154", "1810727",
    "1807650", "1808079", "1805111", "1812028", "1812174", "1802135",
    "1802552",
    "34018", "255369", "255675", "515678", "538034", "827335",
    "844396", "1064466", "1665832", "1666134", "1684373", "1692091",
    "1719866", "1750110", "1770328", "1729779", "1732146", "224541",
    "1680842", "1681260", "975248", "1663041", "1720357", "1720367",
    "1721433", "1722710", "1716988", "1764642", "1746293", "836843",
    "1788783", "1785648", "1808814", "1810048", "1810057", "1808031",
    "1805676", "1801598", "1812637", "1806668", "1806675", "1807081",
    "1802456",
]

# Minimal search payload matching the React app's format
_SEARCH_TEMPLATE = {
    "operationNumber": "",
    "operationName": "",
    "providerName": "",
    "address": "",
    "city": "",
    "sortColumn": "",
    "sortOrder": "ASC",
    "pageSize": "50",
    "pageNumber": 1,
    "includeApplicants": False,
    "providerAdrressOpt": "",
    "nearMeAddress": "",
    "commuteFromAddress": "",
    "commuteToAddress": "",
    "latLong": [],
    "radius": "",
    "genResOpPrgm": [],
    "providerTypes": [],
    "issuanceTypes": [],
    "agesServed": "",
    "gendersServed": [],
    "mealOptions": [],
    "schedulesServed": [],
    "programTypes": [],
    "treatmentServices": [],
    "programmaticServices": [],
    "cpaServices": [],
}


class TXFacilityScraper:
    """Fetches TX HHS 24-Hour Residential facility deficiency data via JSON API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        self.all_facilities: List[Dict] = []

    def _get_token(self):
        """Fetch a bearer token from the TX HHS public token endpoint."""
        resp = self.session.get(TX_TOKEN_URL, timeout=15)
        resp.raise_for_status()
        token = resp.json()["data"]["token"]
        self.session.headers["Authorization"] = token
        logger.info("Obtained TX HHS auth token")

    def _search_provider(self, op_id: str) -> Optional[Dict]:
        """Search for a provider by operation number. Returns provider dict or None."""
        payload = {**_SEARCH_TEMPLATE, "operationNumber": op_id}
        resp = self.session.post(TX_SEARCH_URL, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        providers = data.get("response", [])
        if not providers:
            return None
        return providers[0]

    def _get_compliance_history(self, provider_id: int) -> Dict:
        """Fetch the full compliance history for a provider."""
        resp = self.session.get(
            f"{TX_HISTORY_URL}/{provider_id}", timeout=30
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _build_facility_info(provider: Dict) -> Dict:
        """Map the provider search result to our facility_info schema."""
        phone = provider.get("phoneNumber", "")
        if phone and len(phone) == 10:
            phone = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"

        return {
            "facility_name": provider.get("providerName", ""),
            "program_name": str(provider.get("providerNum", "")),
            "program_category": provider.get("providerType", ""),
            "full_address": provider.get("fullAddress", ""),
            "city": provider.get("city", ""),
            "county": provider.get("countyName", ""),
            "phone": phone,
            "bed_capacity": str(provider.get("ttlCpcty", "")),
            "ages_served": provider.get("agesServed", ""),
            "action": provider.get("issuanceType", ""),
            "license_exp_date": (provider.get("issuanceDate") or "")[:10],
        }

    @staticmethod
    def _format_date(iso_str: Optional[str]) -> str:
        """Convert ISO datetime like '2023-11-07T01:30:00.000+00:00' to 'MM/DD/YYYY'."""
        if not iso_str:
            return ""
        try:
            # Handle the +00:00 timezone offset
            clean = iso_str.split("T")[0]
            dt = datetime.strptime(clean, "%Y-%m-%d")
            return dt.strftime("%m/%d/%Y")
        except (ValueError, IndexError):
            return iso_str

    @staticmethod
    def _build_reports(op_id: str, history: Dict) -> List[Dict]:
        """Convert deficiency details from the history API into report records.

        Each deficiency becomes one report. The `categories` dict mirrors the
        TX CSV column names so the frontend JS renders them correctly.
        """
        deficiencies = history.get("deficienciesDetail", [])
        reports = []

        for deficiency in deficiencies:
            citation_date = TXFacilityScraper._format_date(
                deficiency.get("citationdate")
            )
            standard = deficiency.get("nbrStndrd") or ""
            description = deficiency.get("descStndrd") or ""
            narrative = deficiency.get("txtNrrtv") or ""
            correction_narrative = deficiency.get("txtCrrctvEvalNrrtv") or ""
            risk_level = deficiency.get("stdRiskLvl") or ""
            corrected = "Yes" if deficiency.get("indCrrctdInspctn") == "Y" else "No"
            correction_date = TXFacilityScraper._format_date(
                deficiency.get("dtVrfd")
            )

            # Build a unique report ID
            raw_id = f"{op_id}_{citation_date}_{standard}"
            report_id = re.sub(r"[^a-zA-Z0-9_]", "", raw_id)[:100]

            # Categories dict uses the same keys the TX frontend expects
            categories = {
                "Citation Date": citation_date,
                "Standard Number / Description": (
                    f"{standard} - {description}" if standard else description
                ),
                "Category": deficiency.get("descSection", ""),
                "Sections Violated": standard,
                "Standard Risk Level": risk_level,
                "Corrected at Inspection": corrected,
                "Date Correction Evaluated": correction_date,
                "Deficiency Narrative": narrative,
                "Correction Narrative": correction_narrative,
            }

            reports.append({
                "report_id": report_id,
                "report_date": citation_date,
                "raw_content": narrative,
                "content_length": len(narrative),
                "summary": f"{standard} - {description}" if standard else description,
                "categories": categories,
            })

        return reports

    def scrape(self, operation_ids: Optional[List[str]] = None) -> List[Dict]:
        """Scrape all operations and return the collected facility list."""
        ids = operation_ids or OPERATION_IDS
        logger.info(f"Starting TX scrape for {len(ids)} operations")

        self._get_token()

        for i, op_id in enumerate(ids):
            logger.info(f"[{i+1}/{len(ids)}] Operation {op_id}")
            try:
                # Step 1: Search for the provider to get providerId + facility info
                provider = self._search_provider(op_id)
                if not provider:
                    logger.warning(f"  No provider found for operation {op_id}")
                    continue

                provider_id = provider["providerId"]
                facility_info = self._build_facility_info(provider)
                logger.info(f"  Found: {facility_info['facility_name']} (id={provider_id})")

                # Step 2: Get compliance history with deficiency details
                history = self._get_compliance_history(provider_id)
                reports = self._build_reports(op_id, history)
                logger.info(f"  {len(reports)} deficiencies")

                self.all_facilities.append({
                    "facility_info": facility_info,
                    "reports": reports,
                })

            except requests.exceptions.RequestException as e:
                logger.error(f"  HTTP error on {op_id}: {e}")
                # Re-auth in case the token expired
                try:
                    self._get_token()
                except Exception:
                    pass
                continue
            except Exception as e:
                logger.error(f"  ERROR on {op_id}: {e}")
                continue

        logger.info(f"Scraping complete: {len(self.all_facilities)} facilities")
        return self.all_facilities


# ── API posting ──────────────────────────────────────────────────────

def save_to_api(facilities: List[Dict]) -> bool:
    """POST all collected facility data to the live site for MySQL storage."""
    result = post_facilities_to_api(
        api_url=API_URL,
        api_key=API_KEY,
        state="TX",
        scraped_timestamp=datetime.now().isoformat(),
        facilities=facilities,
        timeout=120,
        info=logger.info,
        error=logger.error,
    )
    return bool(result.get("success"))


def main():
    scraper = TXFacilityScraper()
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
