"""
snapshot_api.py — lightweight always-on BTC dashboard API.

This server intentionally does NOT import main.py, pandas, NumPy, or yFinance.
Market analytics are read from /app/data/latest_snapshot.json, produced by the
short-lived collector.py process.

Dynamic user-owned data (judgments, trade logs, manual history, historical
metric lookups) remains live and writable without loading the analytics stack.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from shared.snapshot_store import (
    get_snapshot_route,
    load_snapshot,
    snapshot_age_seconds,
    snapshot_exists,
    snapshot_path,
)

from manual_history import (
    get_entry,
    get_history,
    get_row_count,
    get_summary_stats,
    init_db as init_history_db,
    upsert_metric,
)
from oi_history import (
    get_latest_snapshot,
    get_snapshot_count,
    get_snapshots,
    init_db as init_oi_db,
)


DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

COLLECTOR_INTERVALS = {
    "fast": max(
        300,
        int(os.getenv("FAST_COLLECTOR_INTERVAL_SECONDS", "900")),
    ),
    "market": max(
        900,
        int(os.getenv("MARKET_COLLECTOR_INTERVAL_SECONDS", "1800")),
    ),
    "hourly": max(
        1800,
        int(os.getenv("HOURLY_COLLECTOR_INTERVAL_SECONDS", "3600")),
    ),
    "slow": max(
        3600,
        int(os.getenv("SLOW_COLLECTOR_INTERVAL_SECONDS", "14400")),
    ),
}

COLLECTOR_TIMEOUTS = {
    "fast": max(
        120,
        int(os.getenv("FAST_COLLECTOR_TIMEOUT_SECONDS", "600")),
    ),
    "market": max(
        300,
        int(os.getenv("MARKET_COLLECTOR_TIMEOUT_SECONDS", "900")),
    ),
    "hourly": max(
        300,
        int(os.getenv("HOURLY_COLLECTOR_TIMEOUT_SECONDS", "900")),
    ),
    "slow": max(
        300,
        int(os.getenv("SLOW_COLLECTOR_TIMEOUT_SECONDS", "900")),
    ),
}

COLLECTOR_RETRY_SECONDS = max(
    60,
    int(os.getenv("COLLECTOR_RETRY_SECONDS", "300")),
)

DB_PATH = DATA_DIR / "basis_history.db"
STABLECOIN_DB_PATH = DATA_DIR / "stablecoin_history.db"
DOMINANCE_DB_PATH = DATA_DIR / "btc_dominance_history.db"
OVERRIDE_FILE = DATA_DIR / "manual_overrides.json"

JUDGMENT_FILE = Path(os.getenv("JUDGMENT_FILE", "judgment_log.json"))
TRADELOG_FILE = Path(os.getenv("TRADELOG_FILE", "trade_log.json"))
EXECUTION_FILE = Path(os.getenv("EXECUTION_FILE", "trade_execution.json"))

_collector_lock = threading.Lock()
_stop_event = threading.Event()


def _run_collector(mode: str) -> bool:
    """
    Run one disposable collector mode.

    Collector modes never overlap. When a collector exits, Linux reclaims
    the pandas/yFinance/native allocations created by that child process.
    """
    if not _collector_lock.acquire(blocking=False):
        print(f"[snapshot_api] collector busy; skipping {mode}")
        return False

    timeout = COLLECTOR_TIMEOUTS[mode]

    try:
        started = time.time()
        print(f"[snapshot_api] starting {mode} collector subprocess")

        completed = subprocess.run(
            [sys.executable, "collector.py", mode],
            cwd=str(Path(__file__).resolve().parent),
            timeout=timeout,
            check=False,
        )

        elapsed = time.time() - started

        print(
            f"[snapshot_api] {mode} collector exited "
            f"code={completed.returncode} after {elapsed:.2f}s"
        )

        return completed.returncode == 0

    except subprocess.TimeoutExpired:
        print(
            f"[snapshot_api] {mode} collector exceeded "
            f"{timeout}s timeout"
        )
        return False

    except Exception as exc:
        print(f"[snapshot_api] {mode} collector launch error: {exc}")
        return False

    finally:
        _collector_lock.release()


def _initial_delay(mode: str, interval: int) -> float:
    """
    Resume each cadence from persisted collection metadata after a redeploy.

    On the first Phase 3 bootstrap, stagger the modes so they do not all
    compete for CPU, memory, SQLite, and external APIs at startup.
    """
    snapshot = load_snapshot()

    if isinstance(snapshot, dict):
        collections = snapshot.get("collections", {})

        if isinstance(collections, dict):
            info = collections.get(mode, {})

            if isinstance(info, dict):
                last = info.get("generated_unix")

                if isinstance(last, (int, float)):
                    age = max(0.0, time.time() - float(last))
                    return max(0.0, interval - age)

    return {
        "fast": 0.0,
        "market": 20.0,
        "hourly": 40.0,
        "slow": 60.0,
    }[mode]


def _collector_loop() -> None:
    """
    Schedule fast, market, hourly, and slow collector modes independently.
    """
    next_runs = {
        mode: time.monotonic() + _initial_delay(mode, interval)
        for mode, interval in COLLECTOR_INTERVALS.items()
    }

    print(
        "[snapshot_api] collector schedule — "
        + ", ".join(
            f"{mode}={interval}s"
            for mode, interval in COLLECTOR_INTERVALS.items()
        )
    )

    while not _stop_event.is_set():
        mode = min(next_runs, key=next_runs.get)

        delay = max(
            0.0,
            next_runs[mode] - time.monotonic(),
        )

        if _stop_event.wait(delay):
            return

        success = _run_collector(mode)
        interval = COLLECTOR_INTERVALS[mode]

        next_runs[mode] = time.monotonic() + (
            interval
            if success
            else min(COLLECTOR_RETRY_SECONDS, interval)
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_history_db()
    init_oi_db()

    thread = threading.Thread(
        target=_collector_loop,
        daemon=True,
        name="snapshot-collector-scheduler",
    )
    thread.start()

    yield

    _stop_event.set()


app = FastAPI(
    title="BTC Decision Dashboard Snapshot API",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://btc-dashboard-production-689a.up.railway.app",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Snapshot helpers ────────────────────────────────────────────────────────

def _snapshot_price() -> str:
    data = get_snapshot_route("/price")
    if isinstance(data, dict):
        return str(data.get("price") or "—")
    return "—"


def _live_btc_price_data() -> dict:
    """
    Fetch current BTC price without importing the heavy analytics stack.

    Trade/judgment timestamps keep their existing live-price semantics while
    the general market analytics remain snapshot-backed.
    """
    params = urlencode({
        "ids": "bitcoin",
        "vs_currencies": "usd",
        "include_24hr_change": "true",
    })
    url = f"https://api.coingecko.com/api/v3/simple/price?{params}"

    headers = {"User-Agent": "btc-dashboard/1.0"}
    key = os.getenv("COINGECKO_API_KEY")
    if key:
        headers["x-cg-demo-api-key"] = key

    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
        bitcoin = payload.get("bitcoin", {})
        price = bitcoin.get("usd")
        change = bitcoin.get("usd_24h_change")
        if price is not None:
            return {
                "price": f"${float(price):,.0f}",
                "change_24h": (
                    f"{float(change):+.2f}%"
                    if change is not None
                    else "—"
                ),
            }
    except Exception as exc:
        print(f"[snapshot_api] live BTC price failed: {exc}")

    fallback = get_snapshot_route("/price")
    if isinstance(fallback, dict):
        return fallback
    return {"price": "—", "change_24h": "—"}


def _live_btc_price() -> str:
    return str(_live_btc_price_data().get("price") or "—")


def _load_json_list(path: Path) -> list:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def _save_json_list(path: Path, entries: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _load_overrides() -> dict:
    try:
        with OVERRIDE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_overrides(data: dict) -> None:
    OVERRIDE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = OVERRIDE_FILE.with_name(
        f".{OVERRIDE_FILE.name}.{os.getpid()}.tmp"
    )
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, OVERRIDE_FILE)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _metric_display_name(metric: str) -> str:
    return {
        "exchange_netflow": "Exchange Netflow",
        "lth_supply": "LTH Supply Change",
        "etf_flow": "ETF Flow",
        "realized_cap": "Realized Cap Growth",
        "funding": "Funding",
        "open_interest": "Open Interest",
        "cme_basis": "CME Basis (Annualized)",
        "stablecoin_supply": "Stablecoin Supply",
        "btc_dominance": "BTC Dominance",
    }.get(metric, metric)


def _metric_category(metric: str) -> str:
    return {
        "exchange_netflow": "On-chain",
        "lth_supply": "On-chain",
        "etf_flow": "Flow",
        "realized_cap": "On-chain",
        "funding": "Derivatives",
        "open_interest": "Derivatives",
        "cme_basis": "Derivatives · Cash & Carry",
        "stablecoin_supply": "Liquidity",
        "btc_dominance": "Market Structure",
    }.get(metric, "—")


def _infer_direction(current: str) -> str:
    stripped = (current or "").replace(",", "").replace(" ", "")
    if stripped.startswith("+"):
        return "up"
    if stripped.startswith("-"):
        return "down"
    return "flat"


def _classify_alert_level(alert: str) -> str:
    if alert == "—" or not alert:
        return "none"
    if "Extreme" in alert:
        return "extreme"
    if alert in ("Accumulation", "Normal"):
        return "neutral"
    return "notable"


def _fmt_billions(v: float) -> str:
    if v >= 1_000_000_000_000:
        return f"${v / 1_000_000_000_000:.2f}T"
    return f"${v / 1_000_000_000:.1f}B"


# ── Service/status routes ──────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "service": "btc-dashboard-api",
        "mode": "snapshot",
        "status": "ok",
        "snapshot_age_s": (
            round(snapshot_age_seconds())
            if snapshot_age_seconds() is not None
            else None
        ),
    }


@app.get("/snapshot")
def get_snapshot():
    data = load_snapshot()
    if data is None:
        raise HTTPException(
            status_code=503,
            detail="Market snapshot not available yet",
        )
    return data


@app.get("/health")
def health():
    data = load_snapshot()
    age = snapshot_age_seconds()
    return {
        "status": "ok" if data else "warming",
        "snapshot_available": data is not None,
        "snapshot_path": str(snapshot_path()),
        "snapshot_age_s": round(age) if age is not None else None,
        "collector_intervals_s": COLLECTOR_INTERVALS,
        "snapshot_errors": (
            data.get("errors", {})
            if isinstance(data, dict)
            else {}
        ),
    }


@app.get("/cache/status")
def cache_status():
    data = load_snapshot()
    age = snapshot_age_seconds()
    return {
        "snapshot": {
            "available": data is not None,
            "age_s": round(age) if age is not None else None,
            "generated_at": (
                data.get("generated_at")
                if isinstance(data, dict)
                else None
            ),
            "route_count": (
                data.get("route_count")
                if isinstance(data, dict)
                else 0
            ),
            "error_count": (
                data.get("error_count")
                if isinstance(data, dict)
                else 0
            ),
        }
    }


@app.get("/price")
def get_price():
    return _live_btc_price_data()


# ── Historical metric lookup (stdlib + SQLite only) ───────────────────────

@app.get("/metrics/history")
def get_metrics_history(date: str):
    result = {}

    for metric in [
        "exchange_netflow",
        "lth_supply",
        "etf_flow",
        "realized_cap",
        "funding",
        "open_interest",
    ]:
        entry = get_entry(metric, date)
        if entry:
            result[metric] = {
                "name": _metric_display_name(metric),
                "category": _metric_category(metric),
                "current": entry.get("current", "—"),
                "current_dir": _infer_direction(entry.get("current", "")),
                "d7": entry.get("d7", "—"),
                "vs30d": entry.get("vs30d", "—"),
                "percentile": entry.get("percentile", 0),
                "alert": entry.get("alert", "—"),
                "alert_level": _classify_alert_level(
                    entry.get("alert", "—")
                ),
                "pattern": entry.get("pattern", "—"),
                "source": entry.get("source", "—"),
                "spark": [],
                "_is_historical": True,
                "_date": date,
            }

    import sqlite3

    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT * FROM cme_basis WHERE date = ?",
                (date,),
            ).fetchone()
        if row:
            annualized, raw_basis, futures_px, spot_px, days_exp = (
                row[1], row[2], row[3], row[4], row[5]
            )
            result["cme_basis"] = {
                "name": "CME Basis (Annualized)",
                "category": "Derivatives · Cash & Carry",
                "current": f"{annualized:+.2f}%",
                "current_dir": (
                    "up" if annualized > 12
                    else "down" if annualized < 5
                    else "flat"
                ),
                "d7": "—",
                "vs30d": "—",
                "percentile": 0,
                "alert": "—",
                "alert_level": "none",
                "pattern": (
                    f"{days_exp}d to expiry · "
                    f"{raw_basis:.2f}% raw premium"
                ),
                "spark": [],
                "futures_px": round(futures_px, 2),
                "spot_px": round(spot_px, 2),
                "raw_basis": round(raw_basis, 4),
                "days_to_exp": days_exp,
                "_is_historical": True,
                "_date": date,
            }
    except Exception:
        pass

    try:
        with sqlite3.connect(STABLECOIN_DB_PATH) as conn:
            row = conn.execute(
                "SELECT * FROM stablecoin_supply WHERE date = ?",
                (date,),
            ).fetchone()
        if row:
            usdt, usdc, total = row[1], row[2], row[3]
            result["stablecoin_supply"] = {
                "name": "Stablecoin Supply",
                "category": "Liquidity · USDT + USDC",
                "current": _fmt_billions(total),
                "current_dir": "flat",
                "d7": "—",
                "vs30d": "—",
                "percentile": 0,
                "alert": "—",
                "alert_level": "none",
                "pattern": (
                    f"USDT {_fmt_billions(usdt)} · "
                    f"USDC {_fmt_billions(usdc)}"
                ),
                "spark": [],
                "usdt": _fmt_billions(usdt),
                "usdc": _fmt_billions(usdc),
                "usdt_share": round(usdt / total * 100, 1) if total else 0,
                "usdc_share": round(usdc / total * 100, 1) if total else 0,
                "usdt_7d": "—",
                "usdc_7d": "—",
                "_is_historical": True,
                "_date": date,
            }
    except Exception:
        pass

    try:
        with sqlite3.connect(DOMINANCE_DB_PATH) as conn:
            row = conn.execute(
                "SELECT * FROM btc_dominance WHERE date = ?",
                (date,),
            ).fetchone()
        if row:
            dom, btc_c, tot_c = row[1], row[2], row[3]
            result["btc_dominance"] = {
                "name": "BTC Dominance",
                "category": "Market Structure · USD",
                "current": f"{dom:.2f}%",
                "current_dir": "flat",
                "d7": "—",
                "vs30d": "—",
                "percentile": 0,
                "alert": "—",
                "alert_level": "none",
                "pattern": f"{dom:.1f}% of total crypto market cap",
                "spark": [],
                "btc_cap": _fmt_billions(btc_c),
                "alt_cap": _fmt_billions(tot_c - btc_c),
                "total_cap": _fmt_billions(tot_c),
                "btc_share": round(dom, 1),
                "alt_share": round(100 - dom, 1),
                "dominance_pct": round(dom, 2),
                "_is_historical": True,
                "_date": date,
            }
    except Exception:
        pass

    if not result:
        return {
            "error": f"No data found for {date}",
            "metrics": {},
            "count": 0,
            "date": date,
        }

    return {
        "date": date,
        "metrics": result,
        "count": len(result),
    }


# ── Judgment / trade logs ─────────────────────────────────────────────────

class JudgmentEntry(BaseModel):
    read: str
    supports: str
    contradicts: str
    invalidates: str
    plan: str
    risk: Optional[str] = None


@app.post("/judgment")
def save_judgment(entry: JudgmentEntry):
    entries = _load_json_list(JUDGMENT_FILE)
    new_entry = {
        **entry.dict(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "btc_price": _live_btc_price(),
        "id": len(entries) + 1,
    }
    entries.append(new_entry)
    _save_json_list(JUDGMENT_FILE, entries)
    return {
        "status": "ok",
        "id": new_entry["id"],
        "timestamp": new_entry["timestamp"],
    }


@app.get("/judgment")
def get_judgments():
    return list(reversed(_load_json_list(JUDGMENT_FILE)))


@app.get("/judgment/{entry_id}")
def get_judgment(entry_id: int):
    for entry in _load_json_list(JUDGMENT_FILE):
        if entry.get("id") == entry_id:
            return entry
    return {"error": "not found"}


class TradeLogEntry(BaseModel):
    structure: str
    capital: str
    read: str
    contradiction: str
    plan: str
    risk: str
    result: Optional[str] = None
    bias_flag: Optional[str] = None


@app.post("/trade-log")
def add_trade_log(entry: TradeLogEntry):
    entries = _load_json_list(TRADELOG_FILE)
    new_entry = {
        **entry.dict(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "btc_price": _live_btc_price(),
        "id": len(entries) + 1,
        "date": datetime.now(timezone.utc).strftime("%b %d"),
    }
    entries.append(new_entry)
    _save_json_list(TRADELOG_FILE, entries)
    return {
        "status": "ok",
        "id": new_entry["id"],
        "timestamp": new_entry["timestamp"],
    }


@app.get("/trade-log")
def get_trade_logs():
    return list(reversed(_load_json_list(TRADELOG_FILE)))


@app.patch("/trade-log/{entry_id}")
def update_trade_log(
    entry_id: int,
    result: Optional[str] = None,
    bias_flag: Optional[str] = None,
):
    entries = _load_json_list(TRADELOG_FILE)
    for entry in entries:
        if entry.get("id") == entry_id:
            if result is not None:
                entry["result"] = result
            if bias_flag is not None:
                entry["bias_flag"] = bias_flag
            _save_json_list(TRADELOG_FILE, entries)
            return {"status": "ok", "id": entry_id}
    return {"error": "not found"}


class TradeExecutionEntry(BaseModel):
    planned_entry: float
    actual_entry: float
    size_btc: float
    max_drawdown_pct: float
    current_volume: float
    market_state: str


@app.post("/trade-execution")
def add_trade_execution(entry: TradeExecutionEntry):
    entries = _load_json_list(EXECUTION_FILE)

    slippage = round(entry.actual_entry - entry.planned_entry, 2)
    max_drawdown_price = round(
        entry.actual_entry * (1 - entry.max_drawdown_pct / 100),
        2,
    )

    new_entry = {
        "planned_entry": entry.planned_entry,
        "actual_entry": entry.actual_entry,
        "size_btc": entry.size_btc,
        "max_drawdown_pct": entry.max_drawdown_pct,
        "current_volume": entry.current_volume,
        "market_state": entry.market_state,
        "slippage": slippage,
        "max_drawdown_price": max_drawdown_price,
        "vol_0_5x": round(entry.current_volume * 0.5, 4),
        "vol_1_5x": round(entry.current_volume * 1.5, 4),
        "vol_2_0x": round(entry.current_volume * 2.0, 4),
        "btc_price_at_entry": _live_btc_price(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "date": datetime.now(timezone.utc).strftime("%b %d"),
        "id": len(entries) + 1,
    }

    entries.append(new_entry)
    _save_json_list(EXECUTION_FILE, entries)

    return {
        "status": "ok",
        "id": new_entry["id"],
        "computed": {
            "slippage": slippage,
            "max_drawdown_price": max_drawdown_price,
            "vol_0_5x": new_entry["vol_0_5x"],
            "vol_1_5x": new_entry["vol_1_5x"],
            "vol_2_0x": new_entry["vol_2_0x"],
        },
    }


@app.get("/trade-execution")
def get_trade_executions():
    return list(reversed(_load_json_list(EXECUTION_FILE)))


# ── Manual overrides / history ─────────────────────────────────────────────

OVERRIDEABLE_METRICS = {
    "exchange_netflow",
    "lth_supply",
    "etf_flow",
    "realized_cap",
    "funding",
    "open_interest",
    "cme_basis",
    "stablecoin_supply",
    "btc_dominance",
}


class MetricOverride(BaseModel):
    metric: str
    current: str
    d7: str
    vs30d: str
    percentile: int
    alert: str
    pattern: str
    source: Optional[str] = None
    baseline_date: Optional[str] = None
    notes: Optional[str] = None


@app.post("/manual-override")
def set_manual_override(override: MetricOverride):
    if override.metric not in OVERRIDEABLE_METRICS:
        return {
            "error": (
                f"Unknown metric '{override.metric}'. "
                f"Valid: {sorted(OVERRIDEABLE_METRICS)}"
            )
        }

    overrides = _load_overrides()
    overrides[override.metric] = {
        "current": override.current,
        "d7": override.d7,
        "vs30d": override.vs30d,
        "percentile": override.percentile,
        "alert": override.alert,
        "alert_level": _classify_alert_level(override.alert),
        "pattern": override.pattern,
        "source": override.source or "Manual override",
        "baseline_date": override.baseline_date,
        "notes": override.notes,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "name": _metric_display_name(override.metric),
        "category": _metric_category(override.metric),
        "current_dir": _infer_direction(override.current),
        "spark": [],
    }

    upsert_metric(
        metric=override.metric,
        date=(
            override.baseline_date
            or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ),
        current=override.current,
        d7=override.d7,
        vs30d=override.vs30d,
        percentile=override.percentile,
        alert=override.alert,
        pattern=override.pattern,
        source=override.source or "Manual override",
        notes=override.notes or "",
    )
    _save_overrides(overrides)

    return {
        "status": "ok",
        "metric": override.metric,
        "updated": overrides[override.metric]["updated_at"],
    }


@app.get("/manual-override")
def get_manual_overrides():
    return _load_overrides()


@app.delete("/manual-override/{metric}")
def clear_manual_override(metric: str):
    overrides = _load_overrides()
    if metric in overrides:
        del overrides[metric]
        _save_overrides(overrides)
        return {"status": "ok", "cleared": metric}
    return {"status": "not_found", "metric": metric}


@app.get("/manual-history/latest")
def get_latest_manual_history():
    import sqlite3

    try:
        conn = sqlite3.connect(DATA_DIR / "manual_history.db")
        rows = conn.execute(
            """
            SELECT metric, date, current, d7, vs30d, percentile,
                   alert, pattern, source, notes
            FROM metric_history
            WHERE (metric, date) IN (
                SELECT metric, MAX(date)
                FROM metric_history
                GROUP BY metric
            )
            ORDER BY metric
            """
        ).fetchall()
        conn.close()
        cols = [
            "metric",
            "date",
            "current",
            "d7",
            "vs30d",
            "percentile",
            "alert",
            "pattern",
            "source",
            "notes",
        ]
        return {"entries": [dict(zip(cols, row)) for row in rows]}
    except Exception as exc:
        return {"entries": [], "error": str(exc)}


class BackfillEntry(BaseModel):
    metric: str
    date: str
    current: str
    d7: str
    vs30d: str
    percentile: int
    alert: str
    pattern: str
    source: Optional[str] = None
    notes: Optional[str] = None
    raw_value: Optional[float] = None
    raw_unit: Optional[str] = None


@app.post("/history/backfill")
def backfill_history(entries: list[BackfillEntry]):
    saved = []
    for entry in entries:
        upsert_metric(
            metric=entry.metric,
            date=entry.date,
            current=entry.current,
            d7=entry.d7,
            vs30d=entry.vs30d,
            percentile=entry.percentile,
            alert=entry.alert,
            pattern=entry.pattern,
            source=entry.source or "Backfill",
            notes=entry.notes or "",
            raw_value=entry.raw_value,
            raw_unit=entry.raw_unit or "",
        )
        saved.append({"metric": entry.metric, "date": entry.date})

    return {
        "status": "ok",
        "saved": len(saved),
        "entries": saved,
    }


@app.get("/history/{metric}")
def get_metric_history(metric: str, days: int = 90):
    days = min(days, 365)
    history = get_history(metric, days)
    return {
        "metric": metric,
        "count": len(history),
        "entries": history,
    }


@app.get("/history/{metric}/{date}")
def get_metric_on_date(metric: str, date: str):
    entry = get_entry(metric, date)
    if not entry:
        return {"error": f"No data for {metric} on {date}"}
    return entry


@app.get("/history")
def get_history_summary():
    counts = get_row_count()
    summaries = {}
    for metric in [
        "exchange_netflow",
        "lth_supply",
        "etf_flow",
        "realized_cap",
        "funding",
        "open_interest",
    ]:
        summaries[metric] = get_summary_stats(metric)

    return {
        "total_rows": sum(counts.values()),
        "by_metric": counts,
        "summaries": summaries,
    }


@app.get("/oi-history")
def get_oi_history():
    snapshots = get_snapshots(days=35)
    latest = get_latest_snapshot()
    count = get_snapshot_count()

    return {
        "total_snapshots": count,
        "history_days": round(len(snapshots) * 15 / 60 / 24, 1),
        "latest": latest,
        "using_real_history": count >= 48,
        "snapshots_needed_for_real_history": max(0, 48 - count),
        "recent_5": snapshots[-5:] if snapshots else [],
    }


@app.get("/db/summary")
def get_db_summary():
    import sqlite3

    summary = {}

    try:
        counts = get_row_count()
        stats = {}
        for metric in [
            "exchange_netflow",
            "lth_supply",
            "etf_flow",
            "realized_cap",
            "funding",
            "open_interest",
        ]:
            stats[metric] = get_summary_stats(metric)
        summary["manual_history"] = {
            "row_counts": counts,
            "stats": stats,
        }
    except Exception as exc:
        summary["manual_history"] = {"error": str(exc)}

    for key, db_path, table in [
        ("cme_basis", DB_PATH, "cme_basis"),
        (
            "stablecoin_supply",
            STABLECOIN_DB_PATH,
            "stablecoin_supply",
        ),
        (
            "btc_dominance",
            DOMINANCE_DB_PATH,
            "btc_dominance",
        ),
    ]:
        try:
            with sqlite3.connect(db_path) as conn:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                oldest = conn.execute(
                    f"SELECT MIN(date) FROM {table}"
                ).fetchone()[0]
                newest = conn.execute(
                    f"SELECT MAX(date) FROM {table}"
                ).fetchone()[0]
                dates = [
                    row[0]
                    for row in conn.execute(
                        f"SELECT date FROM {table} "
                        "ORDER BY date DESC LIMIT 10"
                    ).fetchall()
                ]
            summary[key] = {
                "count": count,
                "oldest": oldest,
                "newest": newest,
                "recent_10": dates,
            }
        except Exception as exc:
            summary[key] = {"error": str(exc)}

    try:
        summary["oi_history"] = {
            "count": get_snapshot_count(),
            "recent_5": get_snapshots(days=1),
        }
    except Exception as exc:
        summary["oi_history"] = {"error": str(exc)}

    return summary


# ── Snapshot-backed compatibility layer ────────────────────────────────────

@app.get("/{full_path:path}")
def snapshot_compatibility_route(full_path: str):
    """
    Serve legacy market-data GET paths from the latest collector snapshot.

    Existing frontend pages can keep calling /metrics, /macro/metrics,
    /leading/all, /liquidity/depth, etc. while the always-on process remains
    free of pandas/yFinance.
    """
    path = "/" + full_path
    value = get_snapshot_route(path)

    if value is None:
        raise HTTPException(
            status_code=404,
            detail=f"Route not present in current snapshot: {path}",
        )

    # Manual overrides should remain immediately visible on /metrics rather
    # than waiting for the next collector cycle.
    if path == "/metrics" and isinstance(value, dict):
        overrides = _load_overrides()
        if overrides:
            value = dict(value)
            for key, override in overrides.items():
                if key in value and isinstance(value[key], dict):
                    value[key] = {
                        **value[key],
                        **override,
                        "_is_override": True,
                    }

    return value
