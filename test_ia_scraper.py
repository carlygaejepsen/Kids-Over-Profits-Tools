import unittest
from types import SimpleNamespace
from unittest.mock import Mock

import ia_scraper


class IowaScraperTests(unittest.TestCase):
    def test_facility_info_normalizes_address_capacity_and_phone(self):
        info = ia_scraper.IAScraper.facility_info({
            "id": 42,
            "name": "Youth Home",
            "typeName": "Psychiatric Medical Institution for Children",
            "addressLine1": "10 Main Street",
            "addressLine2": None,
            "city": "Des Moines",
            "county": "Polk",
            "zip": "503090000",
            "capacityCount": "12",
            "dayPhone": "5155550100",
            "status": "Closed",
            "dateDeleted": "2025-01-01",
        })

        self.assertEqual(info["program_name"], "IA-42")
        self.assertEqual(info["full_address"], "10 Main Street, Des Moines, Polk County, IA 50309")
        self.assertEqual(info["bed_capacity"], "12")
        self.assertEqual(info["phone"], "(515) 555-0100")
        self.assertEqual(info["action"], "Closed (closed 2025-01-01)")

    def test_build_report_uses_state_violation_counts_and_complaint_metadata(self):
        scraper = ia_scraper.IAScraper(client=object(), reports=object())
        report = scraper.build_report(
            {"id": 42, "name": "Youth Home"},
            {
                "id": 123,
                "visitDate": "2025-02-03T00:00:00",
                "visitType": "Recertification, Complaint",
                "scannedReport": "report.pdf",
                "scannedCitation": "N145",
                "violationsFed": 2,
                "violationsState": 1,
                "isRevisit": "",
            },
            {
                "pages": [],
                "ocr_pages": 0,
            },
        )

        self.assertEqual(report["report_id"], "123")
        self.assertEqual(report["report_date"], "2025-02-03")
        self.assertIn("3 deficiencies", report["summary"])
        self.assertIn("N 145", report["summary"])
        self.assertTrue(report["is_flagged"])
        self.assertTrue(report["categories"]["is_complaint"])
        self.assertEqual(report["categories"]["tag_count"], 3)
        self.assertEqual(report["report_url"], "https://dia-hfd.iowa.gov/Home/ViewReport?fileName=report.pdf")

    def test_build_report_for_visit_without_pdf_has_facility_link(self):
        report = ia_scraper.IAScraper(client=object(), reports=object()).build_report(
            {"id": 7, "name": "Youth Home"},
            {"id": 91, "visitDate": "2024-01-02", "visitType": "Incident"},
            None,
        )

        self.assertFalse(report["is_flagged"])
        self.assertEqual(
            report["report_url"],
            "https://dia-hfd.iowa.gov/Home/PublicEntityDetails?recordid=7",
        )
        self.assertIn("has not published", report["raw_content"])

    def test_scrape_deduplicates_active_and_closed_lists_and_filters_seen(self):
        entity = {"id": 5, "name": "Youth Home", "status": "Active"}
        client = SimpleNamespace(
            entities=Mock(side_effect=[[entity], [{**entity, "status": "Closed"}]]),
            visits=Mock(return_value=[
                {"id": 1, "visitDate": "2025-01-01"},
                {"id": 2, "visitDate": "2025-01-02"},
            ]),
        )
        scraper = ia_scraper.IAScraper(client=client, reports=object())
        scraper.fetch_report = Mock(side_effect=lambda _entity, visit, **_kwargs: {
            "report_id": str(visit["id"]),
            "report_date": visit["visitDate"],
        })

        facilities, new_ids = scraper.scrape(seen={"5": {"1"}})

        self.assertEqual(len(facilities), 1)
        self.assertEqual(facilities[0]["facility_info"]["action"], "Active")
        self.assertEqual(new_ids, {"5": ["2"]})
        self.assertEqual(scraper.fetch_report.call_count, 1)

    def test_privacy_check_holds_identifiers_and_possible_names(self):
        self.assertTrue(ia_scraper.privacy_hits("DOB: 01/02/2007"))
        self.assertTrue(ia_scraper.privacy_hits("Resident Jane Doe was interviewed"))
        self.assertFalse(ia_scraper.privacy_hits("Resident #1 was interviewed"))


if __name__ == "__main__":
    unittest.main()
