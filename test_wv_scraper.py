import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import wv_scraper


class WestVirginiaScopeTests(unittest.TestCase):
    def test_repository_scope_is_valid(self):
        scope = wv_scraper.load_scope()
        self.assertTrue(scope)
        self.assertTrue(all(
            record["decision"] in {"included", "excluded", "unsure"}
            for record in scope.values()
        ))

    def test_rejects_duplicate_ids_and_unknown_decisions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scope.json"
            path.write_text(json.dumps({"records": [
                {"id": 1, "name": "One", "decision": "included"},
                {"id": "1", "name": "Duplicate", "decision": "excluded"},
            ]}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate id"):
                wv_scraper.load_scope(path)

            path.write_text(json.dumps({"records": [
                {"id": 2, "name": "Two", "decision": "pending"},
            ]}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "invalid decision"):
                wv_scraper.load_scope(path)

    def test_scrape_uses_only_included_rows_and_refreshes_extracts(self):
        included = {"decision": "included", "category": "residential"}
        scope = {
            1: included,
            2: {"decision": "unsure", "category": "residential"},
            3: {"decision": "excluded", "category": "residential"},
        }
        rows = [
            {"DT_RowId": fid, "Name": f"Facility {fid}", "Status": "Active"}
            for fid in scope
        ]
        client = SimpleNamespace(
            facility_list=Mock(return_value=rows),
            requests_made=0,
        )
        scraper = wv_scraper.WVScraper(client=client, reports=object())
        scraper.history = Mock(return_value=(200, [{
            "survid": "survey-1",
            "form": "State",
            "date": "2025-01-01",
            "survey_type": "Complaint Survey",
        }]))
        scraper.fetch_report = Mock(return_value={
            "report_id": "survey-1-State",
            "report_date": "2025-01-01",
            "awaiting_plan": False,
        })

        with patch.object(wv_scraper, "load_scope", return_value=scope):
            facilities, _, _ = scraper.scrape({}, {}, refresh_all=True)

        self.assertEqual([f["facility_info"]["program_name"] for f in facilities], ["WV-1"])
        scraper.history.assert_called_once_with(1)
        scraper.fetch_report.assert_called_once()
        self.assertTrue(scraper.fetch_report.call_args.kwargs["refresh"])

    def test_template_tag_report_is_held_when_ocr_cannot_confirm_it(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "template.pdf"
            archive.write_bytes(b"%PDF test")
            scraper = wv_scraper.WVScraper(
                client=object(),
                reports=SimpleNamespace(archive_dir=Path(directory)),
            )
            tag = {
                "tag": "C 173",
                "title": "Stale template finding",
                "scope": "",
                "initial": False,
                "corrected": False,
                "regulation": "",
                "finding": "Template text",
                "plan": "",
                "completion_date": "",
            }
            parsed = {"header": {}, "tags": [tag], "preamble": ""}
            survey = {"form": "State", "survey_type": "Complaint Survey", "date": "2025-01-01"}

            with patch.object(wv_scraper, "parse_pages", return_value=parsed):
                report = scraper.build_report(
                    {"DT_RowId": 1, "Name": "Facility", "LegalName": "Facility"},
                    {**survey, "survid": "survey-1", "event_id": "event-1"},
                    {"pages": [], "template_tags": [], "ocr_text": "Visible report text without that citation"},
                    archive.name,
                )

        self.assertIsNone(report)
        self.assertFalse(archive.exists())
        self.assertEqual(len(scraper.stale), 1)

    def test_real_template_number_is_kept_when_ocr_confirms_it(self):
        tag = {
            "tag": "C 173",
            "title": "Health and Safety",
            "scope": "",
            "initial": False,
            "corrected": False,
            "regulation": "",
            "finding": "A finding in the visible report",
            "plan": "",
            "completion_date": "",
        }
        parsed = {"header": {}, "tags": [tag], "preamble": ""}
        scraper = wv_scraper.WVScraper(client=object(), reports=object())
        survey = {
            "form": "State",
            "survey_type": "Complaint Survey",
            "date": "2016-04-20",
            "survid": "survey-2",
            "event_id": "event-2",
        }

        with patch.object(wv_scraper, "parse_pages", return_value=parsed):
            report = scraper.build_report(
                {"DT_RowId": 2, "Name": "Facility", "LegalName": "Facility"},
                survey,
                {"pages": [], "template_tags": ["C 173"], "ocr_text": "Report includes C173"},
                "report.pdf",
            )

        self.assertIsNotNone(report)
        self.assertEqual(report["categories"]["tags"][0]["tag"], "C 173")

    def test_sampled_report_is_held_when_ocr_is_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            scraper = wv_scraper.WVScraper(
                client=SimpleNamespace(pdf=Mock(return_value=b"%PDF test")),
                reports=SimpleNamespace(archive_dir=Path(directory)),
                ocr_share=1,
            )
            survey = {
                "saveas": "sample",
                "survid": "survey-3",
                "form": "State",
                "event_id": "event-3",
                "date": "2025-01-01",
                "survey_type": "Complaint Survey",
            }
            with patch.object(wv_scraper, "extract_with_cache", return_value={
                "pages": [{"words": []}],
                "form_ok": True,
                "ocr_text": "",
            }):
                report = scraper.fetch_report({"DT_RowId": 1}, survey)

        self.assertIsNone(report)
        self.assertEqual(scraper.ocr_unavailable, ["sample.pdf"])


if __name__ == "__main__":
    unittest.main()
