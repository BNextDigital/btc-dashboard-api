"""Regression checks for official source units and the aligned M2 composite."""
import csv
import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import requests
from shared import global_m2 as m2


def ecb_csv(rows):
    keys = sorted(set().union(*(r.keys() for r in rows)))
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=keys)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def fixture():
    # Different growth paths ensure this cannot accidentally use US growth alone.
    series = {r: {} for r in m2.REGIONS}
    fx = {}
    for period, factor in [("2025-08", 1), ("2026-07", 1.1), ("2026-08", 1.2)]:
        for region, value in {"us": 20000, "eurozone": 16000000, "china": 300, "japan": 12000000}.items():
            series[region][period] = {"native_value": value * (1 if region == "us" else factor)}
        fx[period] = {"USD": 1.2, "CNY": 8.4, "JPY": 180}
    return series, fx


class CompositeTests(unittest.TestCase):
    def test_units_growth_and_provenance(self):
        series, fx = fixture()
        result = m2.aggregate(series, fx, today=date(2026, 10, 3))
        expected = 20000 + 16000 * 1.2 * 1.2 + 300 * 1.2 * 1000 / 7 + 12000000 * 1.2 * .1 / 150
        self.assertAlmostEqual(result["global_m2_bil"], expected, places=3)
        before = 20000 + 16000 * 1.2 + 300 * 1000 / 7 + 12000000 * .1 / 150
        self.assertAlmostEqual(result["yoy_growth_pct"], (expected / before - 1) * 100, places=5)
        self.assertEqual(result["data_quality"]["status"], "good")
        self.assertEqual(result["components"]["japan"]["native_unit"], "100 million JPY")
        self.assertEqual(result["fx"]["period"], "2026-08")

    def test_aligns_to_common_period(self):
        series, fx = fixture()
        del series["china"]["2026-08"]
        result = m2.aggregate(series, fx, today=date(2026, 10, 3))
        self.assertEqual(result["as_of"], "2026-07")
        self.assertTrue(all(c["period"] == "2026-07" for c in result["components"].values()))
        self.assertIsNone(result["mom_growth_pct"])  # June is missing; August is not a substitute.
        self.assertEqual(result["data_quality"]["status"], "degraded")

    def test_zero_growth_is_preserved(self):
        series, fx = fixture()
        for observations in series.values():
            observations["2026-08"] = observations["2026-07"] = observations["2025-08"]
        result = m2.aggregate(series, fx, today=date(2026, 10, 3))
        self.assertEqual(result["mom_growth_pct"], 0)
        self.assertEqual(result["yoy_growth_pct"], 0)

    def test_fx_is_historical_not_current(self):
        series, fx = fixture()
        base = m2.aggregate(series, fx, today=date(2026, 10, 3))
        fx["2025-08"]["USD"] *= 2
        result = m2.aggregate(series, fx, today=date(2026, 10, 3))
        self.assertEqual(result["global_m2_bil"], base["global_m2_bil"])
        self.assertLess(result["yoy_growth_pct"], base["yoy_growth_pct"])

    def test_missing_region_and_stale_data_never_produce_total(self):
        series, fx = fixture()
        missing = m2.aggregate({**series, "china": {}}, fx, today=date(2026, 10, 3))
        self.assertIsNone(missing["global_m2_bil"])
        self.assertEqual(missing["data_quality"]["regions_available"], 3)
        self.assertEqual(missing["data_quality"]["status"], "unavailable")
        stale = m2.aggregate(series, fx, today=date(2027, 1, 3))
        self.assertIsNone(stale["global_m2_bil"])
        self.assertEqual(stale["data_quality"]["status"], "stale")

    def test_current_month_and_incomplete_fx_are_excluded(self):
        series, fx = fixture()
        for obs in series.values():
            obs["2026-10"] = {"native_value": 999}
        fx["2026-10"] = {"USD": 1.2, "CNY": 8.4, "JPY": 180}
        del fx["2026-08"]["CNY"]
        result = m2.aggregate(series, fx, today=date(2026, 10, 3))
        self.assertEqual(result["as_of"], "2026-07")


class ParserTests(unittest.TestCase):
    def test_ecb_units_are_checked(self):
        row = {"KEY": f"BSI.{m2.ECB_M2}", "UNIT": "EUR", "UNIT_MULT": "6", "TIME_PERIOD": "2026-08", "OBS_VALUE": "16000000"}
        self.assertEqual(m2.parse_ecb_csv(ecb_csv([row]))["2026-08"]["native_value"], 16000000)
        for key, value in [("UNIT_MULT", "0"), ("UNIT", "USD"), ("KEY", "M3")]:
            with self.assertRaises(ValueError):
                m2.parse_ecb_csv(ecb_csv([{**row, key: value}]))

    def test_boj_nested_values_and_nulls(self):
        payload = {"STATUS": 200, "NEXTPOSITION": None, "RESULTSET": [{"SERIES_CODE": m2.BOJ_CODE, "UNIT": "100 million yen", "FREQUENCY": "MONTHLY", "VALUES": {"SURVEY_DATES": [202607, 202608], "VALUES": [None, 12963895]}}]}
        self.assertEqual(m2.parse_boj(payload), {"2026-08": {"native_value": 12963895}})
        payload["RESULTSET"][0]["UNIT"] = "yen"
        with self.assertRaises(ValueError):
            m2.parse_boj(payload)

    def test_pboc_month_quarter_half_year_and_year(self):
        for title, expected in [("August\u00a02026", "2026-08"), ("Q1 2026", "2026-03"), ("H1 2026", "2026-06"), ("Q1-Q3 2025", "2025-09"), ("2025", "2025-12")]:
            self.assertEqual(m2.pboc_period(f"Financial Statistics Report ({title})"), expected)
        text = '<script>Financial Statistics Report (2020)</script><h2>Financial Statistics Report (August&nbsp;2026)</h2><p>At end-August, broad money supply (M2) stood at RMB356.81 trillion.</p>'
        self.assertEqual(m2.parse_pboc_report(text, "2026-08"), {"native_value": 356.81})
        with self.assertRaises(ValueError):
            m2.parse_pboc_report(text.replace("trillion", "billion"), "2026-08")
        with self.assertRaises(ValueError):
            m2.parse_pboc_report(text, "2026-07")

    def test_pboc_pagination_uses_next_page_not_last(self):
        pages = {
            m2.PBOC_INDEX: '<a title="Financial Statistics Report (August 2026)" href="aug/index.html">August</a><a tagname="48b09237-18.html">Last</a><a tagname="48b09237-2.html">Next</a>',
            m2.PBOC_INDEX.replace("index.html", "48b09237-2.html"): '<a title="Financial Statistics Report (August 2025)" href="old/index.html">August</a>',
        }
        seen = []
        def get(url):
            seen.append(url)
            if url in pages:
                text = pages[url]
            else:
                year = 2025 if '/old/' in url else 2026
                text = f'<h2>Financial Statistics Report (August {year})</h2><p>broad money supply (M2) stood at RMB300 trillion.</p>'
            return type('Response', (), {"text": text})()
        with patch.object(m2, "_get", side_effect=get):
            result = m2.fetch_pboc("2025-08")
        self.assertEqual(set(result), {"2025-08", "2026-08"})
        self.assertFalse(any('-18.html' in u for u in seen))

    def test_failed_pboc_refresh_preserves_observation_with_warning(self):
        text = '<a title="Financial Statistics Report (August 2026)" href="aug/index.html">August</a>'
        def get(url):
            if url == m2.PBOC_INDEX:
                return type('Response', (), {"text": text})()
            raise requests.Timeout('upstream unavailable')
        previous = {"2026-08": {"native_value": 356.81}}
        with patch.object(m2, '_get', side_effect=get):
            result = m2.fetch_pboc('2026-08', previous)
        self.assertEqual(result['2026-08']['native_value'], 356.81)
        self.assertIn('2026-08', result['2026-08']['source_refresh_warning'])

    def test_nonfinite_and_nonpositive_observations_rejected(self):
        for value in [float('nan'), float('inf'), 0, -1, True]:
            with self.assertRaises(ValueError):
                m2._positive(value)


class CacheTests(unittest.TestCase):
    def test_incomplete_cache_file_is_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            (cache / 'china.json').write_text('{"version":1}')
            obs = {"2026-08": {"native_value": 356.81}}
            result, status = m2._cached_source('china', lambda old: obs, cache, '2025-08')
            self.assertEqual(result, obs)
            self.assertFalse(status['cache_fallback'])

    def test_success_survives_restart_and_source_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            obs = {"2026-08": {"native_value": 356.81}}
            with patch.object(m2.time, "time", return_value=100000):
                m2._cached_source("china", lambda old: obs, cache, "2025-08")
            # Fresh persisted cache does not refetch after restart.
            with patch.object(m2.time, "time", return_value=100001):
                result, status = m2._cached_source("china", lambda old: self.fail('unexpected fetch'), cache, "2025-08")
            self.assertEqual(result, obs)
            self.assertFalse(status['cache_fallback'])
            def failure(old):
                raise requests.Timeout('upstream unavailable')
            with patch.object(m2.time, "time", return_value=200000):
                result, status = m2._cached_source("china", failure, cache, "2025-08")
            self.assertEqual(result, obs)
            self.assertTrue(status['cache_fallback'])
            self.assertIn('error', status)
            self.assertEqual(json.loads((cache / 'china.json').read_text())['fetched_at'], 100000)


class FormatterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import leading_routes
        cls.routes = leading_routes

    def test_sqlite_history_excludes_legacy_units(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'leading_history.db'
            with sqlite3.connect(db_path) as db:
                db.execute('CREATE TABLE global_m2_history (date TEXT, global_m2_usd REAL)')
                db.execute("INSERT INTO global_m2_history VALUES ('2026-10-01', 52956680643)")
            series, fx = fixture()
            raw = m2.aggregate(series, fx, today=date(2026, 10, 3))
            with patch.object(self.routes, 'DATA_DIR', Path(directory)), patch.object(self.routes, 'LEADING_DB_PATH', db_path):
                self.routes.format_global_m2(raw)
                history = self.routes.get_leading_history('global_m2', 90)
            self.assertEqual(history['count'], 1)
            self.assertEqual(history['rows'][0]['global_m2_usd'], raw['global_m2_bil'])
            with sqlite3.connect(db_path) as db:
                self.assertEqual(db.execute('SELECT global_m2_usd FROM global_m2_history').fetchone()[0], 52956680643)

    def test_partial_source_warnings_suppress_directional_alerts(self):
        series, fx = fixture()
        series['china']['2026-08']['source_refresh_warning'] = 'latest report refresh failed'
        raw = m2.aggregate(series, fx, today=date(2026, 10, 3))
        self.assertEqual(raw['data_quality']['status'], 'degraded')
        with patch.object(self.routes, '_upsert'):
            self.assertEqual(self.routes.format_global_m2(raw)['alert_level'], 'none')

    def test_unavailable_growth_is_not_classified_as_tightening(self):
        series, fx = fixture()
        raw = m2.aggregate({**series, "china": {}}, fx, today=date(2026, 10, 3))
        with patch.object(self.routes, '_upsert') as upsert:
            result = self.routes.format_global_m2(raw)
        self.assertEqual(result['global_m2'], '–')
        self.assertEqual(result['alert_level'], 'none')
        self.assertIn('error', result)
        upsert.assert_not_called()

    def test_legacy_keys_numeric_fields_and_new_history(self):
        series, fx = fixture()
        raw = m2.aggregate(series, fx, today=date(2026, 10, 3))
        with patch.object(self.routes, '_upsert') as upsert:
            result = self.routes.format_global_m2(raw)
        self.assertTrue(result['global_m2'].endswith('T'))
        self.assertEqual(result['global_m2_bil'], raw['global_m2_bil'])
        self.assertEqual(upsert.call_args[0][0], 'global_m2_history_v2')
        self.assertNotIn('fx_assumptions', result)


if __name__ == '__main__':
    unittest.main()
