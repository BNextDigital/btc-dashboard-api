"""Dated ETF basket/history regression tests; no live Yahoo requests."""
import json
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import etf_aum_routes as aum

NOW = datetime(2026, 10, 4, 1, 0, tzinfo=timezone.utc)


def observation(day="2026-10-02", price=10, count=1_000_000, shares_day=None):
    closes = {t: {day: price} for t in aum.ETF_TICKERS}
    shares = {t: {shares_day or day: count} for t in aum.ETF_TICKERS}
    return aum._align_components(closes, shares, date(2026, 10, 3))


class AlignmentTests(unittest.TestCase):
    def setUp(self):
        self.closes = {t: {"2026-10-02": 10} for t in aum.ETF_TICKERS}
        self.shares = {t: {"2026-10-01": 1_000_000} for t in aum.ETF_TICKERS}

    def test_complete_basket_dated_inputs_and_bounded_share_lag(self):
        result = aum._align_components(self.closes, self.shares, date(2026, 10, 3))
        self.assertEqual(result["date"], "2026-10-02")
        self.assertEqual(result["total_aum"], 80_000_000)
        self.assertEqual(result["components"]["IBIT"]["shares_as_of"], "2026-10-01")

    def test_partial_price_basket_is_not_a_total(self):
        self.closes["FBTC"] = {"2026-10-01": 10}
        result = aum._align_components(self.closes, self.shares, date(2026, 10, 3))
        self.assertIsNone(result["total_aum"])
        self.assertEqual(result["missing"], ["FBTC"])

    def test_future_and_old_shares_are_rejected(self):
        for day in ("2026-10-03", "2026-09-24"):
            self.shares["IBIT"] = {day: 1_000_000}
            result = aum._align_components(self.closes, self.shares, date(2026, 10, 3))
            self.assertIsNone(result["total_aum"])
            self.assertIn("IBIT", result["missing"])

    def test_stale_closes_and_invalid_values_are_rejected(self):
        result = aum._align_components(self.closes, self.shares, date(2026, 10, 8))
        self.assertTrue(result["stale"])
        self.assertIsNone(result["total_aum"])
        for value in (0, -1, float("nan"), float("inf")):
            self.closes["IBIT"]["2026-10-02"] = value
            self.assertIsNone(aum._align_components(self.closes, self.shares, date(2026, 10, 3))["total_aum"])

    def test_split_after_share_observation_requires_new_share_count(self):
        splits = {"IBIT": {"2026-10-02": 2}}
        result = aum._align_components(self.closes, self.shares, date(2026, 10, 3), splits=splits)
        self.assertIn("IBIT", result["missing"])
        self.shares["IBIT"] = {"2026-10-02": 2_000_000}
        result = aum._align_components(self.closes, self.shares, date(2026, 10, 3), splits=splits)
        self.assertEqual(result["total_aum"], 90_000_000)

    def test_provider_intraday_bar_excluded_and_unadjusted_close_requested(self):
        days = pd.to_datetime(["2026-10-01", "2026-10-02"])
        raw = pd.concat({"Close": pd.DataFrame({t: [10, 20] for t in aum.ETF_TICKERS}, index=days)}, axis=1)
        ticker = Mock()
        ticker.get_shares_full.return_value = pd.Series([1_000_000, 2_000_000], index=days)
        with patch.object(aum.yf, "download", return_value=raw) as download, patch.object(aum.yf, "Ticker", return_value=ticker):
            result = aum._fetch_aum(datetime(2026, 10, 2, 18, 0, tzinfo=timezone.utc))
            self.assertEqual(result["date"], "2026-10-01")
            self.assertEqual(result["total_aum"], 80_000_000)
            self.assertFalse(download.call_args.kwargs["auto_adjust"])
            self.assertTrue(download.call_args.kwargs["actions"])
            self.assertEqual(ticker.get_shares_full.call_count, 8)
            result = aum._fetch_aum(datetime(2026, 10, 2, 21, 0, tzinfo=timezone.utc))
            self.assertEqual(result["date"], "2026-10-02")
            self.assertEqual(result["total_aum"], 320_000_000)

    def test_incomplete_split_session_does_not_restate_previous_market_cap(self):
        days = pd.to_datetime(["2026-10-02", "2026-10-05"])
        raw = pd.concat({
            "Close": pd.DataFrame({t: [5, 5] for t in aum.ETF_TICKERS}, index=days),
            "Stock Splits": pd.DataFrame({t: [0, 2] for t in aum.ETF_TICKERS}, index=days),
        }, axis=1)
        ticker = Mock()
        ticker.get_shares_full.return_value = pd.Series([1_000_000, 2_000_000], index=days)
        with patch.object(aum.yf, "download", return_value=raw), patch.object(aum.yf, "Ticker", return_value=ticker):
            result = aum._fetch_aum(datetime(2026, 10, 5, 18, 0, tzinfo=timezone.utc))
            self.assertEqual(result["date"], "2026-10-02")
            self.assertEqual(result["total_aum"], 80_000_000)

    def test_overflowing_component_or_total_never_becomes_json_infinity(self):
        for t in aum.ETF_TICKERS:
            self.closes[t]["2026-10-02"] = 1e308
            self.shares[t]["2026-10-01"] = 10
        result = aum._align_components(self.closes, self.shares, date(2026, 10, 3))
        self.assertIsNone(result["total_aum"])
        json.dumps(result, allow_nan=False)
        for t in aum.ETF_TICKERS:
            self.shares[t]["2026-10-01"] = 1
        result = aum._align_components(self.closes, self.shares, date(2026, 10, 3))
        self.assertIsNone(result["total_aum"])
        json.dumps(result, allow_nan=False)

    def test_provider_failure_returns_missing_data_without_fallback_to_undated_info(self):
        with patch.object(aum.yf, "download", side_effect=RuntimeError("rate limited")), patch.object(aum.yf, "Ticker", side_effect=RuntimeError("rate limited")):
            result = aum._fetch_aum(NOW)
            self.assertIsNone(result["total_aum"])
            self.assertEqual(set(result["missing"]), set(aum.ETF_TICKERS))
            self.assertEqual(len(result["errors"]), 9)


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "aum.db")
        for p in (patch.object(aum, "AUM_DB", self.path), patch.object(aum, "_now", return_value=NOW)):
            p.start()
            self.addCleanup(p.stop)
        aum.flush_etf_aum_cache()
        self.addCleanup(aum.flush_etf_aum_cache)

    def build(self, snapshot=None):
        with patch.object(aum, "_fetch_aum", return_value=snapshot or observation()):
            result = aum._build_etf_aum()
        json.dumps(result, allow_nan=False)
        return result

    def store(self, day, price=10):
        snap = observation(day=day, price=price)
        # Align freshness to the historical observation itself for fixture storage.
        snap["total_aum"] = sum(row["market_cap"] for row in snap["components"].values())
        aum._store_snapshot(snap)

    def test_poll_date_and_weekends_never_create_synthetic_rows(self):
        self.build()
        aum.flush_etf_aum_cache()
        self.build()
        history = aum._fetch_history()
        self.assertEqual([r["date"] for r in history], ["2026-10-02"])

    def test_new_current_point_is_in_spark_and_sparse_history_has_no_percentile(self):
        result = self.build()
        self.assertEqual(result["spark"], [.08])
        self.assertEqual(result["spark_dates"], ["2026-10-02"])
        self.assertIsNone(result["percentile"])
        self.assertEqual(result["alert_level"], "none")
        self.assertEqual(result["d7_pct"], "—")
        self.assertEqual(result["d30_pct"], "—")

    def test_legacy_rows_preserved_but_never_used(self):
        with sqlite3.connect(self.path) as conn:
            conn.execute("CREATE TABLE aum_snapshots(date TEXT, total_aum REAL)")
            conn.executemany("INSERT INTO aum_snapshots VALUES (?, ?)", [(f"2026-09-{d:02}", 83469720480) for d in range(1, 31)])
        result = self.build()
        self.assertEqual(result["history_samples"], 1)
        self.assertEqual(result["d30_pct"], "—")
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM aum_snapshots").fetchone()[0], 30)

    def test_calendar_baselines_signed_deltas_and_true_zero(self):
        self.store("2026-09-25", 8)
        self.store("2026-09-02", 12)
        result = self.build()
        self.assertEqual(result["comparison_dates"], {"d7": "2026-09-25", "d30": "2026-09-02"})
        self.assertEqual(result["d7_chg"], "+$16M")
        self.assertEqual(result["d7_pct"], "+25.0%")
        self.assertEqual(result["d30_chg"], "-$16M")
        self.assertEqual(result["d30_pct"], "-16.7%")
        aum.flush_etf_aum_cache()
        result = self.build(observation(price=8))
        self.assertEqual(result["d7_pct"], "+0.0%")
        self.assertEqual(result["d7_chg"], "+$0M")

    def test_baseline_weekend_tolerance_and_long_gap(self):
        self.store("2026-09-01", 8)  # Target September 2; use prior session.
        result = self.build()
        self.assertEqual(result["comparison_dates"]["d30"], "2026-09-01")
        self.assertEqual(result["comparison_dates"]["d7"], None)
        rows = [{"date": "2026-09-20", "total_aum": 1}]
        self.assertIsNone(aum._baseline(rows, "2026-10-02", 7))

    def test_missing_source_preserves_last_good_as_stale_without_alerts(self):
        self.store("2026-10-01", 12)
        partial = observation()
        partial["total_aum"] = None
        partial["missing"] = ["IBIT"]
        partial["components"].pop("IBIT")
        result = self.build(partial)
        self.assertEqual(result["data_quality"]["status"], "stale")
        self.assertEqual(result["as_of"], "2026-10-01")
        self.assertEqual(result["total_aum_raw"], 96_000_000)
        self.assertEqual(result["etf_count"], 8)
        self.assertIsNone(result["percentile"])
        self.assertEqual(result["d7_pct"], "—")
        self.assertEqual(len(aum._fetch_history()), 1)

    def test_missing_source_without_history_has_no_total_or_alert(self):
        partial = observation()
        partial.update(total_aum=None, components={}, missing=list(aum.ETF_TICKERS))
        result = self.build(partial)
        self.assertEqual(result["data_quality"]["status"], "unavailable")
        self.assertIsNone(result["total_aum_raw"])
        self.assertEqual(result["total_aum"], "—")
        self.assertEqual(result["alert_level"], "none")
        self.assertEqual(aum._fetch_history(), [])

    def test_flat_real_history_percentile_is_neutral_and_requires_time_coverage(self):
        end = date(2026, 10, 2)
        for days in range(90):
            day = end - timedelta(days=days)
            if day.weekday() < 5:
                self.store(day.isoformat())
        result = self.build()
        self.assertGreaterEqual(result["history_samples"], 60)
        self.assertEqual(result["percentile"], 50)
        self.assertEqual(result["alert_level"], "none")
        aum.flush_etf_aum_cache()
        result = self.build(observation(price=20))
        self.assertGreaterEqual(result["percentile"], 90)
        self.assertEqual(result["alert"], "Market cap near 90-day highs")

    def test_partial_snapshots_and_inconsistent_totals_cannot_be_persisted(self):
        snap = observation()
        snap["total_aum"] += 1_000_000
        with self.assertRaises(ValueError):
            aum._store_snapshot(snap)
        snap = observation()
        snap["components"].pop("IBIT")
        with self.assertRaises(ValueError):
            aum._store_snapshot(snap)
        self.assertEqual(aum._fetch_history(), [])

    def test_cache_flush_and_repeated_reads(self):
        with patch.object(aum, "_fetch_aum", return_value=observation()) as fetch:
            first = aum._build_etf_aum()
            self.assertIs(aum._build_etf_aum(), first)
            self.assertEqual(fetch.call_count, 1)
            aum.flush_etf_aum_cache()
            aum._build_etf_aum()
            self.assertEqual(fetch.call_count, 2)


if __name__ == "__main__":
    unittest.main()
