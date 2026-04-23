from __future__ import annotations
from typing import Optional
import sqlite3
import time
from datetime import datetime, timezone
"""
BTC Decision Dashboard API.

Tries real APIs first. If any call returns None (network failure,
rate limit, bad key), falls back to the mock values from Step 6
so the dashboard never shows a broken card.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from formatters import (
    format_etf_flow, format_funding, format_open_interest,
    format_exchange_netflow, format_volume, format_price_move,
    format_realized_cap, format_lth_supply,
)
from data_sources import (
    fetch_exchange_netflow, fetch_realized_cap,
    fetch_funding, fetch_open_interest,
    fetch_etf_flow, fetch_lth_supply,
    fetch_price_and_volume,
    _fetch_coingecko_all,
    _fetch_coingecko_derivatives,
    _cached_get,
    COINGECKO_BASE,
    _coingecko_headers,
    fetch_btc_news,
    get_shared_coingecko,
)

from pydantic import BaseModel
from datetime import datetime, timezone
import json
import os
import threading
from oi_history import init_db, store_snapshot, get_snapshots, get_snapshot_count, prune_old_snapshots
from manual_history import (
    init_db as init_history_db,
    upsert_metric,
    get_history,
    get_entry,
    get_all_dates,
    get_summary_stats,
    get_row_count,
)


app = FastAPI(title="BTC Decision Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ─── OI Polling Job ────────────────────────────────────────────────────────

def _poll_oi() -> None:
    """
    Background thread that snapshots BTC OI every 15 minutes.
    Runs forever — started once on server startup.
    After 7 days this replaces the hardcoded percentile range
    with a real percentile computed from actual history.
    """
    INTERVAL = 240 * 60  # 15 minutes in seconds

    print("[oi_poller] Starting OI polling job — interval 15 minutes")

    while True:
        try:
            markets = _fetch_coingecko_derivatives()
            if markets:
                total_oi = sum(m["open_interest"] for m in markets)
                store_snapshot(total_oi)
                count = get_snapshot_count()
                print(f"[oi_poller] Stored OI snapshot: ${total_oi/1e9:.1f}B — {count} total snapshots")
                prune_old_snapshots(keep_days=90)
        except Exception as e:
            print(f"[oi_poller] Error: {e}")

        time.sleep(INTERVAL)


# Initialise DB and start polling thread on server startup
init_db()
init_history_db() 
threading.Thread(target=_poll_oi, daemon=True).start()


# ─── Mock fallbacks (Step 6 values) ───────────────────────────────────────
# Used whenever a live API call returns None.

MOCK = {
    "etf_flow":         dict(current_daily=450_000_000, last_7d_sum=2_100_000_000, avg_30d=1_135_000_000, percentile_90d=88),
    "funding":          dict(current_rate=0.00035, avg_7d=0.00021, avg_30d=0.000126, percentile_90d=92),
    "open_interest":    dict(current_usd=12_400_000_000, growth_7d_pct=0.18, growth_30d_pct=0.25, percentile_90d=85),
    "exchange_netflow": dict(current_btc=-12_000, sum_7d_btc=-28_000, avg_30d_btc=16_500, percentile_90d=20),
    "volume":           dict(ratio_30d=1.6, ratio_7d=1.2, percentile_90d=87, price_change_pct=0.052),
    "price_move":       dict(daily_change_pct=0.052, week_change_pct=0.088, avg_daily_30d=0.031, percentile_90d=80),
    "realized_cap":     dict(growth_pct=0.028, growth_7d_pct=0.019, avg_30d_pct=0.006, percentile_90d=76),
    "lth_supply":       dict(change_7d_btc=45_000, change_30d_btc=120_000, change_30d_pct=0.008, percentile_90d=72),
}

def get(live, key):
    """Return live data if available, mock otherwise. Logs which was used."""
    if live is not None:
        print(f"[metrics] {key}: LIVE")
        return live
    print(f"[metrics] {key}: MOCK fallback")
    return MOCK[key]


@app.get("/")
def root():
    return {"service": "btc-dashboard-api", "status": "ok"}


@app.get("/metrics")
def get_metrics():
    cg = get_shared_coingecko()
    overrides = _load_overrides()

    netflow_raw  = fetch_exchange_netflow()
    realized_raw = fetch_realized_cap(chart=cg["chart"])
    funding_raw  = fetch_funding(markets=cg["derivatives"])
    oi_raw       = fetch_open_interest(markets=cg["derivatives"])
    etf_raw      = fetch_etf_flow()
    lth_raw      = fetch_lth_supply()
    price_raw, volume_raw = fetch_price_and_volume(
        chart=cg["chart"], ohlcv=cg["ohlcv"]
    )

    def resolve(key, formatter, raw, mock_key):
        """Use manual override if present, otherwise API/mock."""
        if key in overrides:
            o = overrides[key]
            return {**o, "_is_override": True}
        return formatter(**get(raw, mock_key))

    return {
        "etf_flow":         resolve("etf_flow",         format_etf_flow,         etf_raw,      "etf_flow"),
        "funding":          resolve("funding",           format_funding,          funding_raw,  "funding"),
        "open_interest":    resolve("open_interest",     format_open_interest,    oi_raw,       "open_interest"),
        "exchange_netflow": resolve("exchange_netflow",  format_exchange_netflow, netflow_raw,  "exchange_netflow"),
        "volume":           resolve("volume",            format_volume,           volume_raw,   "volume"),
        "price_move":       resolve("price_move",        format_price_move,       price_raw,    "price_move"),
        "realized_cap":     resolve("realized_cap",      format_realized_cap,     realized_raw, "realized_cap"),
        "lth_supply":       resolve("lth_supply",        format_lth_supply,       lth_raw,      "lth_supply"),
    }

@app.get("/summary")
def get_summary():
    cg = get_shared_coingecko()
    price_raw, volume_raw = fetch_price_and_volume(
        chart=cg["chart"], ohlcv=cg["ohlcv"]
    )
    realized_raw = fetch_realized_cap(chart=cg["chart"])
    funding_raw  = fetch_funding(markets=cg["derivatives"])
    oi_raw       = fetch_open_interest(markets=cg["derivatives"])
    etf_raw      = fetch_etf_flow()

    metrics = {
        "funding":       format_funding(**get(funding_raw,       "funding")),
        "open_interest": format_open_interest(**get(oi_raw,      "open_interest")),
        "volume":        format_volume(**get(volume_raw,         "volume")),
        "price_move":    format_price_move(**get(price_raw,      "price_move")),
        "realized_cap":  format_realized_cap(**get(realized_raw, "realized_cap")),
        "etf_flow":      format_etf_flow(**get(etf_raw,         "etf_flow")),
    }

    active_alerts = []
    for metric_id, m in metrics.items():
        if m["alert"] != "—" and m["alert_level"] != "none":
            active_alerts.append({
                "metric":  m["name"],
                "alert":   m["alert"],
                "level":   m["alert_level"],
                "current": m["current"],
            })

    level_order = {"extreme": 0, "notable": 1, "neutral": 2}
    active_alerts.sort(key=lambda a: level_order.get(a["level"], 3))

    extreme_count = sum(1 for a in active_alerts if a["level"] == "extreme")
    notable_count = sum(1 for a in active_alerts if a["level"] == "notable")

    if extreme_count >= 2:
        structure = "Multiple extreme signals active"
    elif extreme_count == 1 and notable_count >= 2:
        structure = "One extreme signal with elevated backdrop"
    elif extreme_count == 1:
        structure = f"Extreme {active_alerts[0]['metric'].lower()} signal"
    elif notable_count >= 3:
        structure = "Broad notable signals across metrics"
    elif notable_count >= 1:
        structure = "Notable signals — monitor closely"
    else:
        structure = "No significant alerts active"

    return {
        "structure":     structure,
        "extreme_count": extreme_count,
        "notable_count": notable_count,
        "active_alerts": active_alerts,
        "total_alerts":  len(active_alerts),
    }


@app.get("/causal")
def get_causal():
    cg = get_shared_coingecko()
    price_raw, volume_raw = fetch_price_and_volume(
        chart=cg["chart"], ohlcv=cg["ohlcv"]
    )
    realized_raw = fetch_realized_cap(chart=cg["chart"])
    funding_raw  = fetch_funding(markets=cg["derivatives"])
    oi_raw       = fetch_open_interest(markets=cg["derivatives"])
    etf_raw      = fetch_etf_flow()

    metrics = {
        "funding":       format_funding(**get(funding_raw,       "funding")),
        "open_interest": format_open_interest(**get(oi_raw,      "open_interest")),
        "volume":        format_volume(**get(volume_raw,         "volume")),
        "price_move":    format_price_move(**get(price_raw,      "price_move")),
        "realized_cap":  format_realized_cap(**get(realized_raw, "realized_cap")),
        "etf_flow":      format_etf_flow(**get(etf_raw,         "etf_flow")),
    }

    def weight_from_level(level: str) -> str:
        return {"extreme": "extreme", "notable": "strong", "neutral": "moderate"}.get(level, "moderate")

    def derive_state(m: dict) -> str:
        alert   = m.get("alert",   "—")
        pattern = m.get("pattern", "—")
        current = m.get("current", "—")
        if alert != "—":
            base = alert.lower()
            return f"{base} · {pattern.lower()}" if pattern != "—" else base
        return pattern.lower() if pattern != "—" else f"at {current}"

    chain = [
        {
            "label":  "ETF & institutional flow",
            "state":  derive_state(metrics["etf_flow"]),
            "weight": weight_from_level(metrics["etf_flow"]["alert_level"]),
        },
        {
            "label":  "Price action",
            "state":  derive_state(metrics["price_move"]),
            "weight": weight_from_level(metrics["price_move"]["alert_level"]),
        },
        {
            "label":  "Volume",
            "state":  derive_state(metrics["volume"]),
            "weight": weight_from_level(metrics["volume"]["alert_level"]),
        },
        {
            "label":  "Funding",
            "state":  derive_state(metrics["funding"]),
            "weight": weight_from_level(metrics["funding"]["alert_level"]),
        },
        {
            "label":  "Capital (realized cap)",
            "state":  derive_state(metrics["realized_cap"]),
            "weight": weight_from_level(metrics["realized_cap"]["alert_level"]),
        },
    ]

    return {
        "chain":         chain,
        "contradiction": _derive_contradiction(metrics),
        "generated_at":  datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health")
def health():
    cg = get_shared_coingecko()
    price_raw, volume_raw = fetch_price_and_volume(
        chart=cg["chart"], ohlcv=cg["ohlcv"]
    )
    realized_raw = fetch_realized_cap(chart=cg["chart"])
    funding_raw  = fetch_funding(markets=cg["derivatives"])
    oi_raw       = fetch_open_interest(markets=cg["derivatives"])

    return {
        "exchange_netflow": "ok" if fetch_exchange_netflow() else "failed",
        "realized_cap":     "ok" if realized_raw             else "failed",
        "funding":          "ok" if funding_raw              else "failed",
        "open_interest":    "ok" if oi_raw                   else "failed",
        "etf_flow":         "ok" if fetch_etf_flow()         else "failed",
        "lth_supply":       "ok" if fetch_lth_supply()       else "failed",
        "price_move":       "ok" if price_raw                else "failed",
        "volume":           "ok" if volume_raw               else "failed",
    }




@app.get("/price")
def get_price():
    data = _cached_get(
        f"{COINGECKO_BASE}/simple/price",
        _coingecko_headers(),
        {
            "ids": "bitcoin",
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        }
    )
    if data and "bitcoin" in data:
        price      = data["bitcoin"]["usd"]
        change_24h = data["bitcoin"]["usd_24h_change"]
        return {
            "price":      f"${price:,.0f}",
            "change_24h": f"{change_24h:+.2f}%",
        }
    return {"price": "—", "change_24h": "—"}

# ─── Judgment Panel ────────────────────────────────────────────────────────

JUDGMENT_FILE = "judgment_log.json"

class JudgmentEntry(BaseModel):
    read:        str
    supports:    str
    contradicts: str
    invalidates: str
    plan:        str
    risk:        str | None = None


def _load_judgments() -> list:
    if not os.path.exists(JUDGMENT_FILE):
        return []
    try:
        with open(JUDGMENT_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


def _save_judgments(entries: list) -> None:
    with open(JUDGMENT_FILE, "w") as f:
        json.dump(entries, f, indent=2)


@app.post("/judgment")
def save_judgment(entry: JudgmentEntry):
    """
    Saves a judgment entry with current timestamp and market snapshot.
    Called when user clicks 'Commit to log' in the dashboard.
    """
    entries = _load_judgments()

    # Snapshot current market state alongside the judgment
    # so you can review what the market looked like when you decided
    cg_chart, cg_ohlcv   = _fetch_coingecko_all()
    cg_derivatives        = _fetch_coingecko_derivatives()
    price_data            = _cached_get(
        f"{COINGECKO_BASE}/simple/price",
        _coingecko_headers(),
        {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"}
    )

    btc_price = "—"
    if price_data and "bitcoin" in price_data:
        btc_price = f"${price_data['bitcoin']['usd']:,.0f}"

    new_entry = {
        **entry.dict(),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "btc_price":   btc_price,
        "id":          len(entries) + 1,
    }

    entries.append(new_entry)
    _save_judgments(entries)

    return {"status": "ok", "id": new_entry["id"], "timestamp": new_entry["timestamp"]}


@app.get("/judgment")
def get_judgments():
    """Returns all saved judgment entries, most recent first."""
    entries = _load_judgments()
    return list(reversed(entries))


@app.get("/judgment/{entry_id}")
def get_judgment(entry_id: int):
    """Returns a single judgment entry by ID."""
    entries = _load_judgments()
    for e in entries:
        if e.get("id") == entry_id:
            return e
    return {"error": "not found"}

# ─── Manual Override ───────────────────────────────────────────────────────
# Allows manually updating metric cards from screenshot-extracted data.
# Used for Exchange Netflow and LTH Supply which require paywalled APIs.
# Claude extracts values from CryptoQuant/CoinGlass screenshots using
# the BTC Capital Dashboard skill, then POSTs them here.

OVERRIDE_FILE = "manual_overrides.json"

OVERRIDEABLE_METRICS = {
    "exchange_netflow",
    "lth_supply",
    "etf_flow",
    "realized_cap",
    "funding",
    "open_interest",
}

class MetricOverride(BaseModel):
    metric:    str
    current:   str
    d7:        str
    vs30d:     str
    percentile: int
    alert:     str
    pattern:   str
    source:    Optional[str] = None   # e.g. "CryptoQuant · tooltip exact"
    baseline_date: Optional[str] = None  # e.g. "2026-04-17"
    notes:     Optional[str] = None


def _load_overrides() -> dict:
    if not os.path.exists(OVERRIDE_FILE):
        return {}
    try:
        with open(OVERRIDE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_overrides(data: dict) -> None:
    with open(OVERRIDE_FILE, "w") as f:
        json.dump(data, f, indent=2)


@app.post("/manual-override")
def set_manual_override(override: MetricOverride):
    """
    Stores a manually extracted metric value.
    The /metrics endpoint checks this store and uses overrides
    when present, falling back to API/mock data otherwise.
    """
    if override.metric not in OVERRIDEABLE_METRICS:
        return {"error": f"Unknown metric '{override.metric}'. Valid: {list(OVERRIDEABLE_METRICS)}"}

    overrides = _load_overrides()
    overrides[override.metric] = {
        "current":       override.current,
        "d7":            override.d7,
        "vs30d":         override.vs30d,
        "percentile":    override.percentile,
        "alert":         override.alert,
        "alert_level":   _classify_alert_level(override.alert),
        "pattern":       override.pattern,
        "source":        override.source or "Manual override",
        "baseline_date": override.baseline_date,
        "notes":         override.notes,
        "updated_at":    datetime.now(timezone.utc).isoformat(),
        "name":          _metric_display_name(override.metric),
        "category":      _metric_category(override.metric),
        "current_dir":   _infer_direction(override.current),
        "spark":         [],
    }
    # Also write to SQLite history for long-term retention
    upsert_metric(
        metric     = override.metric,
        date       = override.baseline_date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        current    = override.current,
        d7         = override.d7,
        vs30d      = override.vs30d,
        percentile = override.percentile,
        alert      = override.alert,
        pattern    = override.pattern,
        source     = override.source or "Manual override",
        notes      = override.notes or "",
    )
    _save_overrides(overrides)

    return {
        "status":  "ok",
        "metric":  override.metric,
        "updated": overrides[override.metric]["updated_at"],
    }


@app.get("/manual-override")
def get_manual_overrides():
    """Returns all active manual overrides with their timestamps."""
    return _load_overrides()


@app.delete("/manual-override/{metric}")
def clear_manual_override(metric: str):
    """Removes a manual override — metric reverts to API/mock data."""
    overrides = _load_overrides()
    if metric in overrides:
        del overrides[metric]
        _save_overrides(overrides)
        return {"status": "ok", "cleared": metric}
    return {"status": "not_found", "metric": metric}


# ─── Override helpers ──────────────────────────────────────────────────────

def _classify_alert_level(alert: str) -> str:
    if alert == "—" or not alert:
        return "none"
    if "Extreme" in alert:
        return "extreme"
    if alert in ("Accumulation", "Normal"):
        return "neutral"
    return "notable"


def _metric_display_name(metric: str) -> str:
    return {
        "exchange_netflow": "Exchange Netflow",
        "lth_supply":       "LTH Supply Change",
        "etf_flow":         "ETF Flow",
        "realized_cap":     "Realized Cap Growth",
        "funding":          "Funding",
        "open_interest":    "Open Interest",
    }.get(metric, metric)


def _metric_category(metric: str) -> str:
    return {
        "exchange_netflow": "On-chain",
        "lth_supply":       "On-chain",
        "etf_flow":         "Flow",
        "realized_cap":     "On-chain",
        "funding":          "Derivatives",
        "open_interest":    "Derivatives",
    }.get(metric, "—")


def _infer_direction(current: str) -> str:
    """Infers up/down/flat from the current value string."""
    if not current:
        return "flat"
    stripped = current.replace(",", "").replace(" ", "")
    if stripped.startswith("+") or stripped.startswith("$") and not stripped.startswith("-"):
        return "up"
    if stripped.startswith("-"):
        return "down"
    return "flat"


# ─── Trade Log ─────────────────────────────────────────────────────────────

TRADELOG_FILE = "trade_log.json"

class TradeLogEntry(BaseModel):
    structure:   str
    capital:     str
    read:        str
    contradiction: str
    plan:        str
    risk:        str
    result:      Optional[str] = None
    bias_flag:   Optional[str] = None


def _load_trade_logs() -> list:
    if not os.path.exists(TRADELOG_FILE):
        return []
    try:
        with open(TRADELOG_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


def _save_trade_logs(entries: list) -> None:
    with open(TRADELOG_FILE, "w") as f:
        json.dump(entries, f, indent=2)


@app.post("/trade-log")
def add_trade_log(entry: TradeLogEntry):
    """
    Saves a new trade log entry with timestamp and market snapshot.
    """
    entries = _load_trade_logs()

    price_data = _cached_get(
        f"{COINGECKO_BASE}/simple/price",
        _coingecko_headers(),
        {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"}
    )

    btc_price = "—"
    if price_data and "bitcoin" in price_data:
        btc_price = f"${price_data['bitcoin']['usd']:,.0f}"

    new_entry = {
        **entry.dict(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "btc_price": btc_price,
        "id":        len(entries) + 1,
        "date":      datetime.now(timezone.utc).strftime("%b %d"),
    }

    entries.append(new_entry)
    _save_trade_logs(entries)

    return {"status": "ok", "id": new_entry["id"], "timestamp": new_entry["timestamp"]}


@app.get("/trade-log")
def get_trade_logs():
    """Returns all trade log entries, most recent first."""
    entries = _load_trade_logs()
    return list(reversed(entries))


@app.patch("/trade-log/{entry_id}")
def update_trade_log(entry_id: int, result: Optional[str] = None, bias_flag: Optional[str] = None):
    """
    Updates result and bias_flag on an existing entry after trade closes.
    """
    entries = _load_trade_logs()
    for e in entries:
        if e.get("id") == entry_id:
            if result is not None:
                e["result"] = result
            if bias_flag is not None:
                e["bias_flag"] = bias_flag
            _save_trade_logs(entries)
            return {"status": "ok", "id": entry_id}
    return {"error": "not found"}

@app.get("/oi-history")
def get_oi_history():
    """Shows OI snapshot history stats and recent values."""
    from oi_history import get_snapshots, get_snapshot_count, get_latest_snapshot
    snapshots = get_snapshots(days=35)
    latest    = get_latest_snapshot()
    count     = get_snapshot_count()

    return {
        "total_snapshots":    count,
        "history_days":       round(len(snapshots) * 15 / 60 / 24, 1),
        "latest":             latest,
        "using_real_history": count >= 48,
        "snapshots_needed_for_real_history": max(0, 48 - count),
        "recent_5": snapshots[-5:] if snapshots else [],
    }
@app.get("/news")
def get_news():
    """
    Returns top 3 BTC-relevant news items from CoinGecko.
    Cached for 90 seconds to avoid rate limiting.
    """
    news = fetch_btc_news()
    if not news:
        return {
            "items": [
                {"title": "No recent BTC news found", "source": "—", "time": "—", "tag": "—", "url": "#"},
            ]
        }
    return {"items": news}


def _derive_contradiction(metrics: dict) -> str:
    """
    Identifies the main structural contradiction from live metrics.
    Looks for tension between capital/flow signals and
    derivatives/positioning signals.
    """
    funding_level  = metrics["funding"]["alert_level"]
    cap_level      = metrics["realized_cap"]["alert_level"]
    etf_level      = metrics["etf_flow"]["alert_level"]
    oi_level       = metrics["open_interest"]["alert_level"]
    volume_pattern = metrics["volume"].get("pattern", "—")
    funding_alert  = metrics["funding"].get("alert", "—").lower()

    # Extreme shorting + strong capital inflow = classic tension
    if "shorting" in funding_alert and cap_level in ("notable", "extreme"):
        return "Extreme short positioning against strong capital inflow — leverage and spot diverging."

    # Extreme leverage + weak capital = crowded trade with no backing
    if "leverage" in funding_alert and cap_level == "none":
        return "Elevated leverage with no corresponding capital inflow — positioning appears speculative."

    # High OI + absorption volume = supply being absorbed by buyers
    if oi_level in ("notable", "extreme") and volume_pattern == "Absorption":
        return "Large open position base with absorption volume — significant supply being absorbed."

    # Strong ETF inflow + extreme funding = institutional vs retail tension
    if etf_level in ("notable", "extreme") and "leverage" in funding_alert:
        return "Institutional inflow (ETF) alongside elevated retail leverage — capital quality diverging."

    # High OI + distribution = positioning at risk
    if oi_level in ("notable", "extreme") and volume_pattern == "Distribution":
        return "Large open positions with distribution volume — crowded trade showing supply pressure."

    # No strong contradiction
    active_signals = [
        m for m in metrics.values()
        if m.get("alert_level") in ("notable", "extreme")
    ]
    if len(active_signals) >= 3:
        return "Multiple signals elevated simultaneously — broad market activation across metrics."
    if len(active_signals) == 0:
        return "No significant contradictions — market structure is neutral across monitored metrics."

    return "Monitor for developing contradictions as signals evolve."

# ─── Trade Execution Log ───────────────────────────────────────────────────

EXECUTION_FILE = "trade_execution.json"

class TradeExecutionEntry(BaseModel):
    planned_entry:   float
    actual_entry:    float
    size_btc:        float
    max_drawdown_pct: float
    current_volume:  float
    market_state:    str  # Green / Yellow / Red


def _load_executions() -> list:
    if not os.path.exists(EXECUTION_FILE):
        return []
    try:
        with open(EXECUTION_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


def _save_executions(entries: list) -> None:
    with open(EXECUTION_FILE, "w") as f:
        json.dump(entries, f, indent=2)


@app.post("/trade-execution")
def add_trade_execution(entry: TradeExecutionEntry):
    """
    Saves a quantitative trade execution entry.
    Computes slippage, max drawdown price, and volume benchmarks server-side.
    """
    entries = _load_executions()

    # Computed fields
    slippage           = round(entry.actual_entry - entry.planned_entry, 2)
    max_drawdown_price = round(entry.actual_entry * (1 - entry.max_drawdown_pct / 100), 2)
    vol_0_5x           = round(entry.current_volume * 0.5, 4)
    vol_1_5x           = round(entry.current_volume * 1.5, 4)
    vol_2_0x           = round(entry.current_volume * 2.0, 4)

    # Fetch current BTC price for context
    price_data = _cached_get(
        f"{COINGECKO_BASE}/simple/price",
        _coingecko_headers(),
        {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"}
    )
    btc_price = "—"
    if price_data and "bitcoin" in price_data:
        btc_price = f"${price_data['bitcoin']['usd']:,.0f}"

    new_entry = {
        # Inputs
        "planned_entry":    entry.planned_entry,
        "actual_entry":     entry.actual_entry,
        "size_btc":         entry.size_btc,
        "max_drawdown_pct": entry.max_drawdown_pct,
        "current_volume":   entry.current_volume,
        "market_state":     entry.market_state,
        # Computed
        "slippage":           slippage,
        "max_drawdown_price": max_drawdown_price,
        "vol_0_5x":           vol_0_5x,
        "vol_1_5x":           vol_1_5x,
        "vol_2_0x":           vol_2_0x,
        # Context
        "btc_price_at_entry": btc_price,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "date":               datetime.now(timezone.utc).strftime("%b %d"),
        "id":                 len(entries) + 1,
    }

    entries.append(new_entry)
    _save_executions(entries)

    return {"status": "ok", "id": new_entry["id"], "computed": {
        "slippage":           slippage,
        "max_drawdown_price": max_drawdown_price,
        "vol_0_5x":           vol_0_5x,
        "vol_1_5x":           vol_1_5x,
        "vol_2_0x":           vol_2_0x,
    }}


@app.get("/trade-execution")
def get_trade_executions():
    """Returns all trade execution entries, most recent first."""
    entries = _load_executions()
    return list(reversed(entries))

# ─── Manual History ────────────────────────────────────────────────────────

class BackfillEntry(BaseModel):
    metric:     str
    date:       str   # YYYY-MM-DD
    current:    str
    d7:         str
    vs30d:      str
    percentile: int
    alert:      str
    pattern:    str
    source:     Optional[str] = None
    notes:      Optional[str] = None
    raw_value:  Optional[float] = None
    raw_unit:   Optional[str] = None


@app.post("/history/backfill")
def backfill_history(entries: list[BackfillEntry]):
    """
    Backfills historical data for one or more metrics.
    Accepts an array of entries, each with a specific date.
    Use this to manually add past data from screenshots.
    """
    saved = []
    for entry in entries:
        upsert_metric(
            metric     = entry.metric,
            date       = entry.date,
            current    = entry.current,
            d7         = entry.d7,
            vs30d      = entry.vs30d,
            percentile = entry.percentile,
            alert      = entry.alert,
            pattern    = entry.pattern,
            source     = entry.source or "Backfill",
            notes      = entry.notes or "",
            raw_value  = entry.raw_value,
            raw_unit   = entry.raw_unit or "",
        )
        saved.append({"metric": entry.metric, "date": entry.date})

    return {"status": "ok", "saved": len(saved), "entries": saved}


@app.get("/history/{metric}")
def get_metric_history(metric: str, days: int = 90):
    """
    Returns historical data for a metric.
    Query param: days (default 90, max 365)
    """
    days = min(days, 365)
    history = get_history(metric, days)
    return {
        "metric":  metric,
        "count":   len(history),
        "entries": history,
    }


@app.get("/history/{metric}/{date}")
def get_metric_on_date(metric: str, date: str):
    """Returns data for a specific metric on a specific date (YYYY-MM-DD)."""
    entry = get_entry(metric, date)
    if not entry:
        return {"error": f"No data for {metric} on {date}"}
    return entry


@app.get("/history")
def get_history_summary():
    """
    Returns a summary of all stored history —
    row counts, date ranges, and top alerts per metric.
    """
    counts = get_row_count()
    summaries = {}
    for metric in ["exchange_netflow", "lth_supply", "etf_flow",
                   "realized_cap", "funding", "open_interest"]:
        summaries[metric] = get_summary_stats(metric)

    return {
        "total_rows": sum(counts.values()),
        "by_metric":  counts,
        "summaries":  summaries,
    }