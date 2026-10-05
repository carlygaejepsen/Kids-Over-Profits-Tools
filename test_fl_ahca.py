"""Offline tests for the Florida AHCA scraper (fl_scraper.FLAHCAScraper).

No network: the FloridaHealthFinder search page, its embedded facility list
and the dm_web deficiency grid (with its pager) are fixtures shaped like the
live pages. Run: python test_fl_ahca.py
"""
import json
import unittest
from unittest.mock import MagicMock

import fl_scraper
from fl_scraper import AHCA_FACILITY_TYPES, FLAHCAScraper

SEARCH_PAGE = """
<form id="AdvancedSearchForm">
  <input type="hidden" name="__RequestVerificationToken" value="tok">
  <select name="FacilityTypeSelection">
    <option value="">-- Select --</option>
    <option value="ALF">Assisted Living Facility</option>
    <option value="Crisis">Crisis Stabilization Unit</option>
    <option value="Hospital">Hospital</option>
    <option value="RTC">Residential Treatment Center for Children and Adolescents</option>
    <option value="RTF">Residential Treatment Facility</option>
    <option value="SRT">Short Term Residential Treatment Facility</option>
  </select>
</form>
"""

RECORDS = [
    {"FileNumber": "100", "Name": "Sunrise Children's Crisis Stabilization Unit [CCSU]",
     "ClientCode": "60", "FacilityType": "Crisis Stabilization Unit"},
    {"FileNumber": "101", "Name": "Lakeside Adult CSU", "ClientCode": "60",
     "FacilityType": "Crisis Stabilization Unit"},
]
RESULT_PAGE = "<script>var data = " + json.dumps(RECORDS) + ";\n$('#t').DataTable({data: data});</script>"


def grid_page(rows, pages, current):
    trs = "".join(
        "<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows
    )
    links = "".join(
        f"<td>{p}</td>" if p == current else
        f"<td><a href=\"javascript:__doPostBack('gridView','Page${p}')\">{p}</a></td>"
        for p in pages
    )
    return f"""<form><input type="hidden" name="__VIEWSTATE" value="vs{current}">
    <input type="hidden" name="__EVENTVALIDATION" value="ev{current}">
    <table id="gridView"><tr><th>Survey Date</th><th>Inspection Type</th><th>Track ID</th>
    <th>Deficiency</th><th>Requirement Description</th><th>Correction Date</th></tr>
    {trs}<tr><td colspan="6"><table><tr>{links}</tr></table></td></tr></table></form>"""


def row(date, track, code):
    return [date, "Licensure", track, code, f"OPERATING STDS - {code} rule", "01/02/2025"]


class TestAhca(unittest.TestCase):
    def test_type_values_resolved_from_dropdown(self):
        s = FLAHCAScraper()
        s.session = MagicMock()
        s.session.get.return_value = MagicMock(text=SEARCH_PAGE, status_code=200)
        _, options = s._search_form()
        resolve = FLAHCAScraper.resolve_type_value
        self.assertEqual(resolve("RTC", options)[0], "RTC")
        self.assertEqual(resolve("CSU", options)[0], "Crisis")
        self.assertEqual(resolve("PSYCH", options)[0], "Hospital")
        self.assertEqual(resolve("RTF", options)[0], "RTF")
        self.assertEqual(resolve("SRT", options)[0], "SRT")
        # Values renamed on the site: found by label instead.
        renamed = {"X1": "Crisis Stabilization Unit", "X2": "Hospital", "X3": "Residential Treatment Center for Children and Adolescents"}
        self.assertEqual(resolve("CSU", renamed)[0], "X1")
        self.assertEqual(resolve("PSYCH", renamed)[0], "X2")
        self.assertEqual(resolve("RTC", renamed)[0], "X3")
        self.assertEqual(resolve("SRT", renamed)[0], "")
        # Dropdown unreadable: the known value is still tried.
        self.assertEqual(resolve("RTC", {})[0], "RTC")

    def test_embedded_array_with_brackets_in_names(self):
        recs = FLAHCAScraper.extract_facility_array(RESULT_PAGE)
        self.assertEqual([r["FileNumber"] for r in recs], ["100", "101"])
        self.assertEqual(FLAHCAScraper.extract_facility_array("<p>none</p>"), [])

    def test_youth_and_psych_filters(self):
        keep = FLAHCAScraper.keep_record
        self.assertTrue(keep(RECORDS[0], "CSU"))
        self.assertFalse(keep(RECORDS[1], "CSU"))
        self.assertTrue(keep(RECORDS[1], "CSU", all_ages=True))
        self.assertTrue(keep({"Name": "River Point Behavioral Health"}, "PSYCH"))
        self.assertTrue(keep({"Name": "Sandy Pines Psychiatric Hospital"}, "PSYCH"))
        self.assertFalse(keep({"Name": "Tampa General Hospital"}, "PSYCH"))
        self.assertTrue(keep({"Name": "Anything"}, "RTC"))

    def test_grid_skips_header_and_pager(self):
        rows, pages = FLAHCAScraper.parse_deficiency_grid(
            grid_page([row("03/04/2024", "T1", "A100"), row("03/04/2024", "T1", "A101")], [1, 2, 3], 1)
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["survey_date"], "03/04/2024")
        self.assertEqual(pages, {2, 3})

    def test_every_grid_page_is_read(self):
        page_html = {
            1: grid_page([row("03/04/2024", "T1", "A100")], [1, 2, 3], 1),
            2: grid_page([row("03/04/2024", "T1", "A101")], [1, 2, 3], 2),
            3: grid_page([row("05/06/2023", "T0", "A200")], [1, 2, 3], 3),
        }
        s = FLAHCAScraper()
        s._dm_session_id = "abc"
        s.session = MagicMock()
        s.session.get.return_value = MagicMock(text=page_html[1])
        posted = []

        def post(url, data, timeout):
            posted.append(data)
            n = int(data["__EVENTARGUMENT"].split("$")[1])
            return MagicMock(text=page_html[n])

        s.session.post.side_effect = post
        rows = s.fetch_inspection_deficiencies("100", "60", "Crisis Stabilization Unit", "X")
        self.assertEqual([r["deficiency"] for r in rows], ["A100", "A101", "A200"])
        self.assertEqual([p["__EVENTARGUMENT"] for p in posted], ["Page$2", "Page$3"])
        self.assertEqual(posted[0]["__VIEWSTATE"], "vs1")
        self.assertEqual(posted[1]["__VIEWSTATE"], "vs2")
        reports = s._build_reports_from_deficiencies(rows)
        self.assertEqual(len(reports), 2)
        visit = [r for r in reports if r["report_id"] == "T1-03/04/2024"][0]
        self.assertEqual(visit["categories"]["deficiency_count"], 2)

    def test_scrape_end_to_end(self):
        s = FLAHCAScraper()
        s._search_form = MagicMock(return_value=({}, {"Crisis": "Crisis Stabilization Unit"}))
        s.fetch_fhf_facilities = MagicMock(return_value=RECORDS)
        s.fetch_inspection_deficiencies = MagicMock(return_value=[
            {"survey_date": "03/04/2024", "inspection_type": "Licensure", "track_id": "T1",
             "deficiency": "A100", "requirement_description": "x", "correction_date": ""},
        ])
        facilities, new_ids = s.scrape(type_codes=["CSU"], seen={"100": set()})
        self.assertEqual(len(facilities), 1)
        info = facilities[0]["facility_info"]
        self.assertEqual(info["program_name"], "AHCA-100")
        self.assertEqual(info["program_category"], "Crisis Stabilization Unit")
        self.assertEqual(facilities[0]["reports"][0]["categories"]["facility_type"], "CSU")
        self.assertEqual(new_ids, {"100": ["T1-03/04/2024"]})
        call = s.fetch_inspection_deficiencies.call_args.kwargs
        self.assertEqual(call["client_code"], "60")
        # Already seen: nothing new.
        facilities, _ = s.scrape(type_codes=["CSU"], seen={"100": {"T1-03/04/2024"}})
        self.assertEqual(facilities, [])

    def test_types_table(self):
        for key in fl_scraper.AHCA_DEFAULT_TYPES:
            self.assertIn(key, AHCA_FACILITY_TYPES)


if __name__ == "__main__":
    unittest.main(verbosity=2)
