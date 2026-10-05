"""CNH source identity and severity/direction regression tests."""
import json
import unittest
from unittest.mock import patch

import pandas as pd
import forex_routes as fx
from shared import yf_cache


def series(value, count=30):
    return pd.Series([value] * count, index=pd.date_range("2026-08-24", periods=count, freq="B"))


def wind(cnh, **others):
    # Explicitly neutral other inputs isolate the CNH contribution.
    cards = {"dxy": {}, "eurusd": {}, "usdjpy": {}, "em": {"avg_percentile": 50}}
    cards.update(others)
    return fx._build_wind_assessment(usdcnh=cnh, **cards)


class CNHCardTests(unittest.TestCase):
    def test_strong_cnh_is_notable_tailwind_not_weakness(self):
        card = fx._build_usdcnh_card(series(6.6987))
        self.assertEqual(card["alert_level"], "notable")
        self.assertEqual(card["wind_effect"], "tailwind")
        self.assertIn("CNH strong", card["alert"])
        self.assertEqual(card["source"], "yFinance: CNH=X (offshore USD/CNH)")
        result = wind(card)
        self.assertEqual(result["headwinds"], [])
        self.assertEqual(len(result["tailwinds"]), 1)
        self.assertEqual(result["direction"], "Mild Tailwind")
        self.assertNotIn("capital inflows", card["alert"])
        self.assertNotIn("PBOC support", card["alert"])

    def test_level_band_boundaries_preserve_severity_and_direction(self):
        for value, severity, effect in [
            (6.8999, "notable", "tailwind"), (6.90, "none", "neutral"),
            (7.0999, "none", "neutral"), (7.10, "none", "neutral"),
            (7.2499, "none", "neutral"), (7.25, "notable", "headwind"),
            (7.3999, "notable", "headwind"), (7.40, "extreme", "headwind"),
            (7.60, "extreme", "headwind"),
        ]:
            with self.subTest(value=value):
                card = fx._build_usdcnh_card(series(value))
                self.assertEqual((card["alert_level"], card["wind_effect"]), (severity, effect))
                result = wind(card)
                self.assertEqual(len(result["headwinds"]), int(effect == "headwind"))
                self.assertEqual(len(result["tailwinds"]), int(effect == "tailwind"))

    def test_missing_short_or_invalid_series_cannot_contribute_wind(self):
        for values in [None, series(7.50, 4), pd.Series(dtype=float),
                       series(0), series(-7), series(float("nan")),
                       series(float("inf")), series(float("-inf"))]:
            with self.subTest(values=None if values is None else values.tolist()[:1]):
                card = fx._build_usdcnh_card(values)
                self.assertIn("error", card)
                self.assertEqual(card["alert_level"], "none")
                self.assertEqual(card["wind_effect"], "neutral")
                self.assertEqual(wind(card)["direction"], "Neutral")
                json.dumps(card, allow_nan=False)

    def test_invalid_current_is_not_replaced_with_earlier_valid_price(self):
        values = series(7.5)
        values.iloc[-1] = float("nan")
        card = fx._build_usdcnh_card(values)
        self.assertIn("error", card)
        self.assertNotIn("current_raw", card)
        self.assertEqual(wind(card)["headwinds"], [])

    def test_severity_or_wording_alone_cannot_create_a_headwind(self):
        for card in [
            {"alert_level": "notable", "alert": "CNH strong", "current_raw": 6.6987},
            {"alert_level": "extreme", "alert": "CNH weak"},
            {"alert_level": "notable", "wind_effect": "unknown"},
            {"alert_level": "extreme", "wind_effect": "headwind", "error": "unavailable"},
        ]:
            self.assertEqual(wind(card)["headwinds"], [])
            self.assertEqual(wind(card)["tailwinds"], [])
        # Classification should survive a wording/translation change.
        card = fx._build_usdcnh_card(series(7.5))
        card["alert"] = "Different display copy"
        self.assertEqual(len(wind(card)["headwinds"]), 1)

    def test_neutral_band_stays_neutral_despite_falling_momentum(self):
        values = series(7.35)
        values.iloc[-1] = 7.0
        card = fx._build_usdcnh_card(values)
        self.assertLess(float(card["d20_pct"].strip("%")), 0)
        self.assertEqual(card["wind_effect"], "neutral")
        self.assertEqual(wind(card)["direction"], "Neutral")

    def test_strong_cnh_with_eur_tailwind_changes_summary_without_false_stress(self):
        result = wind(fx._build_usdcnh_card(series(6.6987)), eurusd={"current_raw": 1.12})
        self.assertEqual(result["direction"], "Tailwind")
        self.assertEqual(result["headwinds"], [])
        self.assertEqual(len(result["tailwinds"]), 2)

    def test_other_headwinds_and_mixed_conditions_are_preserved(self):
        result = wind(fx._build_usdcnh_card(series(6.6987)),
                      dxy={"alert_level": "extreme", "alert": "DXY very strong"},
                      usdjpy={"carry_level": "extreme"},
                      em={"alert_level": "extreme", "avg_percentile": 95})
        self.assertEqual(result["direction"], "Strong Headwind")
        self.assertEqual(len(result["headwinds"]), 3)
        self.assertEqual(len(result["tailwinds"]), 1)
        self.assertFalse(any("CNH weak" in entry for entry in result["headwinds"]))


class CNHSourceTests(unittest.TestCase):
    def test_shared_cache_downloads_offshore_symbol(self):
        self.assertEqual(yf_cache.ALL_TICKERS["usdcnh"], "CNH=X")
        raw = pd.concat({"Close": pd.DataFrame({"CNH=X": [6.8] * 10, "CNY=X": [7.5] * 10},
            index=pd.date_range("2026-09-21", periods=10, freq="B"))}, axis=1)
        with patch.object(yf_cache.yf, "download", return_value=raw) as download, patch.object(yf_cache, "release_memory"), patch("builtins.print"):
            result = yf_cache._fetch()
        symbols = download.call_args.args[0]
        self.assertIn("CNH=X", symbols)
        self.assertNotIn("CNY=X", symbols)
        self.assertEqual(result["usdcnh"]["values"], [6.8] * 10)

    def test_missing_cnh_does_not_fall_back_to_onshore_cny(self):
        raw = pd.concat({"Close": pd.DataFrame({"CNY=X": [6.8] * 10},
            index=pd.date_range("2026-09-21", periods=10, freq="B"))}, axis=1)
        with patch.object(yf_cache.yf, "download", return_value=raw), patch.object(yf_cache, "release_memory"), patch("builtins.print"):
            result = yf_cache._fetch()
        self.assertIsNone(result["usdcnh"])
        self.assertEqual(wind(fx._build_usdcnh_card(result["usdcnh"]))["direction"], "Neutral")

    def test_route_uses_corrected_card_and_summary_and_keeps_cache_contract(self):
        fx.flush_forex_cache()
        self.addCleanup(fx.flush_forex_cache)
        fixtures = {"dxy": series(102), "eurusd": series(1.12),
                    "usdjpy": series(145), "usdcnh": series(6.6987)}
        with patch.object(fx, "_yf", side_effect=fixtures.get), patch.object(fx, "_fred", return_value=None), patch.object(fx, "_build_em_basket", return_value={"avg_percentile": 50}):
            result = fx.get_forex_metrics()
            self.assertEqual(result["usdcnh"]["wind_effect"], "tailwind")
            self.assertFalse(any("CNH weak" in x for x in result["wind"]["headwinds"]))
            self.assertTrue(any("CNH strong" in x for x in result["wind"]["tailwinds"]))
            self.assertIs(fx.get_forex_metrics(), result)
            self.assertEqual(set(result), {"updated_at", "dxy", "eurusd", "usdjpy", "usdcnh", "em_fx", "fxvol", "carry", "wind"})
            json.dumps(result, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
