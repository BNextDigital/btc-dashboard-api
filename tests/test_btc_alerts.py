"""Regression tests for false BTC alerts in overrides and persisted snapshots."""
import copy
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import Request, Response
from formatters import format_etf_flow
from shared.btc_alerts import classify_alert, normalize_metrics, summarize_alerts, build_causal


def fixture():
    names = {
        "etf_flow": "ETF Flow", "exchange_netflow": "Exchange Netflow",
        "lth_supply": "LTH Supply Change", "realized_cap": "Realized Cap Growth",
        "cme_basis": "CME Basis (Annualized)", "price_move": "Price Move",
        "volume": "Volume", "funding": "Funding", "open_interest": "Open Interest",
        "stablecoin_supply": "Stablecoin Supply", "btc_dominance": "BTC Dominance",
    }
    metrics = {key: {"name": name, "current": "1", "alert": "—",
                        "alert_level": "none", "pattern": "—"}
               for key, name in names.items()}
    for key in ("etf_flow", "exchange_netflow", "lth_supply"):
        metrics[key].update(alert="No alert", alert_level="notable")
    metrics["realized_cap"].update(alert="New high - fifteenth consecutive fresh high", alert_level="notable")
    metrics["cme_basis"].update(alert="Basis compressed — carry trade unattractive", alert_level="notable")
    return metrics


class ClassificationTests(unittest.TestCase):
    def test_no_alert_and_missing_labels_override_stale_severity(self):
        for label in (None, "", "  ", "—", "–", "-", "No alert", " NO  ALERT \n", "no alerts", "None", "No data", "Error"):
            for severity in (None, "notable", "extreme", "neutral"):
                with self.subTest(label=label, severity=severity):
                    self.assertEqual(classify_alert(label, severity), "none")

    def test_real_alerts_and_numeric_threshold_severity_survive(self):
        self.assertEqual(classify_alert("Extreme inflow"), "extreme")
        self.assertEqual(classify_alert("New high - fifteenth consecutive fresh high"), "notable")
        self.assertEqual(classify_alert("Backwardation — futures below spot", "extreme"), "extreme")
        self.assertEqual(classify_alert("Strong acceleration", "extreme"), "extreme")
        self.assertEqual(classify_alert("Accumulation"), "neutral")
        self.assertEqual(classify_alert("Normal"), "neutral")
        self.assertEqual(classify_alert("No alert threshold configured for this signal"), "notable")

    def test_etf_formatter_repairs_stale_level_override(self):
        result = format_etf_flow(31_700_000, 10_000_000, 10_000_000, 50,
                                 _alert_override="No alert", _alert_level_override="notable")
        self.assertEqual(result["alert"], "No alert")
        self.assertEqual(result["alert_level"], "none")
        result = format_etf_flow(1, 1, 1, 50, _alert_override="Strong acceleration", _alert_level_override="extreme")
        self.assertEqual(result["alert_level"], "extreme")

    def test_snapshot_replay_removes_three_false_signals(self):
        metrics = fixture()
        before = copy.deepcopy(metrics)
        result = summarize_alerts(metrics)
        self.assertEqual(result["notable_count"], 2)
        self.assertEqual(result["total_alerts"], 2)
        self.assertEqual(result["extreme_count"], 0)
        self.assertEqual(result["structure"], "Notable signals — monitor closely")
        self.assertEqual({a["metric"] for a in result["active_alerts"]}, {"Realized Cap Growth", "CME Basis (Annualized)"})
        causal = build_causal(metrics, "2026-10-03T12:00:00+00:00")
        self.assertEqual(causal["chain"][0]["weight"], "moderate")
        self.assertEqual(causal["contradiction"], "Monitor for developing contradictions as signals evolve.")
        self.assertEqual(causal["generated_at"], "2026-10-03T12:00:00+00:00")
        self.assertEqual(metrics, before)

    def test_unavailable_metrics_and_empty_summary(self):
        metrics = {"missing": {"name": "Missing", "current": "—", "alert": "No data", "alert_level": "extreme", "_unavailable": True}}
        self.assertEqual(normalize_metrics(metrics)["missing"]["alert_level"], "none")
        self.assertEqual(summarize_alerts(metrics)["total_alerts"], 0)
        self.assertEqual(summarize_alerts({})["structure"], "No significant alerts active")

    def test_mixed_real_levels_keep_order_and_counts(self):
        metrics = fixture()
        metrics["cme_basis"].update(alert="Backwardation — futures below spot", alert_level="extreme")
        metrics["lth_supply"].update(alert="Accumulation", alert_level="neutral")
        result = summarize_alerts(metrics)
        self.assertEqual((result["extreme_count"], result["notable_count"], result["total_alerts"]), (1, 1, 3))
        self.assertEqual([a["level"] for a in result["active_alerts"]], ["extreme", "notable", "neutral"])
        self.assertEqual(result["structure"], "Extreme cme basis (annualized) signal")


class APITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_dir = tempfile.TemporaryDirectory()
        # main initializes SQLite at import. Keep it isolated from real user data.
        with patch.dict(os.environ, {"DATA_DIR": cls.data_dir.name}):
            cls.main = importlib.import_module("main")
            cls.snapshot = importlib.import_module("snapshot_api")

    @classmethod
    def tearDownClass(cls):
        cls.data_dir.cleanup()

    def setUp(self):
        self.metrics = fixture()
        self.original = copy.deepcopy(self.metrics)
        self.overrides = {k: dict(self.metrics[k]) for k in ("etf_flow", "exchange_netflow", "lth_supply")}
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "overrides.json"
        self.path.write_text(json.dumps(self.overrides))
        self.patches = [
            patch.object(self.main, "OVERRIDE_FILE", str(self.path)),
            patch.object(self.snapshot, "OVERRIDE_FILE", self.path),
            patch.object(self.main, "get_shared_coingecko", return_value={}),
            patch.object(self.main, "_build_metrics_cached", return_value=self.metrics),
        ]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)

    def test_existing_override_files_corrected_on_read_without_rewrite(self):
        saved = self.path.read_text()
        for api in (self.main, self.snapshot):
            self.assertEqual(api._load_overrides()["etf_flow"]["alert_level"], "none")
        self.assertEqual(self.main.get_metrics()["etf_flow"]["alert_level"], "none")
        self.assertEqual(self.main.get_summary()["notable_count"], 2)
        self.assertEqual(self.main.get_causal()["chain"][0]["weight"], "moderate")
        self.assertEqual(self.path.read_text(), saved)
        self.assertEqual(self.metrics, self.original)

    def test_text_only_override_does_not_inherit_previous_level(self):
        self.path.write_text(json.dumps({"etf_flow": {"alert": "No alert"}}))
        self.metrics["etf_flow"].update(alert="Extreme inflow", alert_level="extreme")
        self.assertEqual(self.main._apply_overrides(self.metrics)["etf_flow"]["alert_level"], "none")
        self.assertEqual(self.snapshot._apply_btc_metric_overrides(self.metrics)["etf_flow"]["alert_level"], "none")
        self.assertEqual(self.metrics["etf_flow"]["alert_level"], "extreme")

    def test_new_manual_overrides_use_same_classifier(self):
        for api in (self.main, self.snapshot):
            override = api.MetricOverride(metric="etf_flow", current="+31.70M USD", d7="—", vs30d="—", percentile=50, alert="No alert", pattern="—")
            with patch.object(api, "upsert_metric"), patch.object(api, "_save_overrides") as save:
                api.set_manual_override(override)
                self.assertEqual(save.call_args.args[0]["etf_flow"]["alert_level"], "none")

    def test_history_rows_use_same_classifier(self):
        for api in (self.main, self.snapshot):
            with patch.object(api, "get_entry", side_effect=lambda key, date: self.overrides.get(key)):
                result = api.get_metrics_history("2026-10-03")
                self.assertEqual(result["metrics"]["etf_flow"]["alert_level"], "none")

    def test_legacy_routes_and_bundle_rebuild_stale_summary_and_causal(self):
        routes = {"/metrics": self.metrics,
                  "/summary": {"notable_count": 5, "total_alerts": 5},
                  "/causal": {"chain": [], "contradiction": "stale", "generated_at": "2026-10-03T12:00:00+00:00"}}
        saved = copy.deepcopy(routes)
        snapshot = {"routes": routes, "generated_at": "2026-10-03T12:00:00+00:00"}
        with patch.object(self.snapshot, "get_snapshot_route", side_effect=routes.get), patch.object(self.snapshot, "load_snapshot", return_value=snapshot):
            metrics = self.snapshot.snapshot_compatibility_route("metrics")
            summary = self.snapshot.snapshot_compatibility_route("summary")
            causal = self.snapshot.snapshot_compatibility_route("causal")
            self.assertEqual(metrics["etf_flow"]["alert_level"], "none")
            self.assertEqual(summary["notable_count"], 2)
            self.assertEqual(causal["chain"][0]["weight"], "moderate")
            request = Request({"type": "http", "headers": []})
            response = Response()
            bundle = self.snapshot.get_dashboard_bundle("btc", request, response)
            self.assertEqual(bundle["metrics"], metrics)
            self.assertEqual(bundle["summary"], summary)
            self.assertEqual(bundle["causal"], causal)
            request = Request({"type": "http", "headers": [(b"if-none-match", response.headers["etag"].encode())]})
            self.assertEqual(self.snapshot.get_dashboard_bundle("btc", request, Response()).status_code, 304)
            self.path.write_text(json.dumps({"etf_flow": {**self.overrides["etf_flow"], "alert": "Extreme inflow", "alert_level": "extreme"}}))
            changed = self.snapshot.get_dashboard_bundle("btc", request, Response())
            self.assertIsInstance(changed, dict)
            self.assertNotEqual(changed["revision"], bundle["revision"])
            self.assertEqual(changed["summary"]["extreme_count"], 1)
            self.assertEqual(changed["causal"]["chain"][0]["weight"], "extreme")
        self.assertEqual(routes, saved)


if __name__ == "__main__":
    unittest.main()
