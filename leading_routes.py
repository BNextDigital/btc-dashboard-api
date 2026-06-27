"""
leading_routes.py — Leading signal indicators for BTC Dashboard

Indicators & data sources:
  1. BTC Options IV Term Structure & Risk Reversal  — Deribit public REST (no key)
  2. Coinbase Premium Index                          — CryptoQuant (CRYPTOQUANT_API_KEY)
  3. Cumulative Funding Rate (30d)                   — CoinGecko derivatives (no extra key)
  4. Global M2 Money Supply                          — FRED (FRED_API_KEY)
  5. CFTC Commitment of Traders — BTC Futures        — CFTC public Socrata API (no key)
  6. Tether Mint/Burn Events                         — CoinGecko (extends existing stablecoin data)
  7. Breakeven Inflation Rates (TIPS Spread)         — FRED (FRED_API_KEY)
  8. CME Basis Enhanced (trend + DTX-normalized)     — Extends existing basis data in main.py

Architecture note:
  Indicators 7 and 8 extend existing routes (macro_routes.py, main.py) but the
  fetch/format logic is all here for clean reference. Wire into those files as noted
  in the "Integration" comments throughout.

Setup:
  1. FRED_API_KEY already set for macro_routes.py — same key works here
  2. CRYPTOQUANT_API_KEY already set — covers Coinbase Premium
  3. No other new keys needed

Add to bottom of main.py:
    from leading_routes import leading_router
    app.include_router(leading_router)

Endpoints:
    GET /leading/options           — IV term structure & risk reversal
    GET /leading/coinbase-premium  — Coinbase premium index
    GET /leading/funding-cumulative — 30d cumulative funding cost
    GET /leading/global-m2         — Global M2 (US + EU + China + Japan)
    GET /leading/cot               — CFTC COT leveraged fund positioning
    GET /leading/tether-mints      — Tether mint/burn delta
    GET /leading/breakevens        — TIPS breakeven inflation rates
    GET /leading/basis-enhanced    — CME basis with trend + DTX normalization
    GET /leading/all               — All 8 indicators in one call
    GET /leading/cache/flush       — Force cache refresh
"""

from __future__ import annotations
import os, time, sqlite3, requests, threading
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
from fastapi import APIRouter
# At the top of leading_routes.py, add:
from data_sources import get_shared_coingecko, COINGECKO_BASE, _coingecko_headers, _cached_get

leading_router = APIRouter(prefix="/leading")

# ── Config ───────────────────────────────────────────────────────────────────

FRED_API_KEY       = os.getenv("FRED_API_KEY", "")
CRYPTOQUANT_KEY    = os.getenv("CRYPTOQUANT_API_KEY", "")
COINGECKO_KEY      = os.getenv("COINGECKO_API_KEY", "")
DATA_DIR           = Path(os.getenv("DATA_DIR", "./data"))
LEADING_DB_PATH    = DATA_DIR / "leading_history.db"

CRYPTOQUANT_BASE   = "https://api.cryptoquant.com/v1/btc"
COINGECKO_BASE     = "https://api.coingecko.com/api/v3"
DERIBIT_BASE       = "https://www.deribit.com/api/v2/public"
CFTC_API_BASE      = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
FRED_BASE          = "https://api.stlouisfed.org/fred/series/observations"

# FRED series for breakeven inflation
FRED_BREAKEVEN = {
    "be_5y":    "T5YIE",    # 5-year breakeven inflation rate
    "be_10y":   "T10YIE",   # 10-year breakeven inflation rate
    "be_5y5y":  "T5YIFR",   # 5y5y forward inflation expectation
}

# Cache: refresh once after 10AM EST each day (same pattern as macro_routes.py)
EST = timezone(timedelta(hours=-5))

def _last_10am_est() -> datetime:
    now_est    = datetime.now(EST)
    today_10am = now_est.replace(hour=10, minute=0, second=0, microsecond=0)
    if now_est < today_10am:
        return today_10am - timedelta(days=1)
    return today_10am

def _cache_is_stale(cache: dict) -> bool:
    if not cache["data"]:
        return True
    cache_time = datetime.fromtimestamp(cache["ts"], tz=timezone.utc)
    return cache_time < _last_10am_est()

# Individual caches — some refresh more often than daily
_options_cache    = {"data": None, "ts": 0.0}   # 15 min TTL (market hours)
_premium_cache    = {"data": None, "ts": 0.0}   # 15 min TTL
_funding_cache    = {"data": None, "ts": 0.0}   # 8 hr TTL
_m2_cache         = {"data": None, "ts": 0.0}   # daily (FRED data is monthly)
_cot_cache        = {"data": None, "ts": 0.0}   # daily (weekly releases)
_tether_cache     = {"data": None, "ts": 0.0}   # daily
_breakeven_cache  = {"data": None, "ts": 0.0}   # daily
_basis_enh_cache  = {"data": None, "ts": 0.0}   # 15 min TTL

OPTIONS_TTL  = 15 * 60
PREMIUM_TTL  = 15 * 60
FUNDING_TTL  = 8  * 3600
BASIS_TTL    = 15 * 60


# ── SQLite ────────────────────────────────────────────────────────────────────

def _leading_db() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(LEADING_DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS options_history (
            date TEXT PRIMARY KEY, iv_7d REAL, iv_30d REAL,
            term_spread REAL, risk_reversal_25d REAL, stored_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS coinbase_premium_history (
            date TEXT PRIMARY KEY, premium_pct REAL, avg_24h REAL, stored_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS funding_cumulative_history (
            date TEXT PRIMARY KEY, daily_rate REAL,
            cumulative_7d REAL, cumulative_30d REAL, stored_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS global_m2_history (
            date TEXT PRIMARY KEY,
            us_m2 REAL, eurozone_m2 REAL, china_m2 REAL, japan_m2 REAL,
            global_m2_usd REAL, mom_growth REAL, yoy_growth REAL, stored_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cot_history (
            date TEXT PRIMARY KEY,
            lev_long REAL, lev_short REAL, lev_net REAL,
            asset_mgr_long REAL, asset_mgr_short REAL,
            total_oi REAL, lev_net_pct REAL, stored_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tether_mint_history (
            date TEXT PRIMARY KEY,
            usdt_supply REAL, daily_change REAL,
            large_mint_flag INTEGER, stored_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS breakeven_history (
            date TEXT PRIMARY KEY,
            be_5y REAL, be_10y REAL, be_5y5y REAL, stored_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS basis_enhanced_history (
            date TEXT PRIMARY KEY,
            annualized REAL, dtx_normalized REAL, trend_5d TEXT, stored_at TEXT
        )
    """)
    conn.commit()
    return conn


def _upsert(table: str, row: dict) -> None:
    """Generic upsert for any leading_history table."""
    conn = _leading_db()
    try:
        cols   = list(row.keys())
        placeholders = ", ".join("?" * len(cols))
        updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "date")
        conn.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT(date) DO UPDATE SET {updates}",
            list(row.values())
        )
        conn.commit()
    except Exception as e:
        print(f"[leading] upsert error ({table}): {e}")
        conn.rollback()
    finally:
        conn.close()


def _query_history(table: str, columns: list[str], n_days: int) -> list[dict]:
    conn = _leading_db()
    try:
        col_str = ", ".join(columns)
        rows = conn.execute(
            f"SELECT {col_str} FROM {table} ORDER BY date DESC LIMIT ?", (n_days,)
        ).fetchall()
        return [dict(zip(columns, r)) for r in rows]
    except Exception as e:
        print(f"[leading] query error ({table}): {e}")
        return []
    finally:
        conn.close()


# ── Shared fetch helpers ──────────────────────────────────────────────────────

def _safe_get(url: str, headers: dict = None, params: dict = None):
    try:
        r = requests.get(url, headers=headers or {}, params=params, timeout=15)
        if r.status_code == 429:
            time.sleep(30)
            r = requests.get(url, headers=headers or {}, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[leading] GET {url} failed: {e}")
        return None

def _cq_headers() -> dict:
    return {"Authorization": f"Bearer {CRYPTOQUANT_KEY}"}

def _cg_headers() -> dict:
    return {"x-cg-demo-api-key": COINGECKO_KEY}

def _days_ago(n: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime("%Y-%m-%d")

def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _pct_rank(values: list, current: float) -> int | None:
    if not values or len(values) < 5:
        return None
    return round(sum(1 for v in values if v < current) / len(values) * 100)

def _fetch_fred(series_id: str, n_days: int = 365) -> list[tuple]:
    """Returns [(date_str, float), ...] oldest→newest. Skips FRED '.' missing values."""
    if not FRED_API_KEY:
        return []
    end   = date.today()
    start = end - timedelta(days=n_days + 60)
    try:
        resp = requests.get(FRED_BASE, params={
            "series_id":         series_id,
            "observation_start": start.isoformat(),
            "observation_end":   end.isoformat(),
            "api_key":           FRED_API_KEY,
            "file_type":         "json",
        }, timeout=20)
        resp.raise_for_status()
        pairs = []
        for o in resp.json().get("observations", []):
            try:
                pairs.append((o["date"], float(o["value"])))
            except (ValueError, KeyError):
                pass
        return pairs
    except Exception as e:
        print(f"[leading] FRED {series_id} error: {e}")
        return []


# ════════════════════════════════════════════════════════════════════════════
# INDICATOR 1 — BTC Options: IV Term Structure & Risk Reversal
# Source: Deribit public REST (no API key required)
# Lead time: 3-7 days
# ════════════════════════════════════════════════════════════════════════════

def _get_deribit_instruments() -> list[dict]:
    """Fetch all active BTC option instruments."""
    data = _safe_get(f"{DERIBIT_BASE}/get_instruments",
                     params={"currency": "BTC", "kind": "option", "expired": "false"})
    if not data:
        return []
    return data.get("result", [])


def _find_atm_instrument(instruments: list[dict], target_days: int, spot: float) -> dict | None:
    """
    Find the nearest expiry to target_days out, then the ATM strike at that expiry.
    Returns the instrument dict or None.
    """
    now_ts = time.time() * 1000  # Deribit uses ms timestamps

    # Group by expiry, keep those within ±5 days of target
    by_expiry: dict[float, list] = {}
    for ins in instruments:
        exp_ts  = ins.get("expiration_timestamp", 0)
        days_to = (exp_ts - now_ts) / 86400000
        if abs(days_to - target_days) <= 5 and ins.get("option_type") == "call":
            by_expiry.setdefault(exp_ts, []).append((abs(ins["strike"] - spot), ins))

    if not by_expiry:
        return None

    # Pick closest expiry
    closest_exp = min(by_expiry.keys(), key=lambda e: abs((e - now_ts) / 86400000 - target_days))
    # Pick ATM (closest strike to spot)
    _, atm_ins  = min(by_expiry[closest_exp], key=lambda t: t[0])
    return atm_ins


def _get_iv_for_instrument(instrument_name: str) -> float | None:
    """Fetch mark IV for a single instrument from order book."""
    data = _safe_get(f"{DERIBIT_BASE}/get_order_book",
                     params={"instrument_name": instrument_name, "depth": 1})
    if not data:
        return None
    result = data.get("result", {})
    return result.get("mark_iv")   # annualized IV in percent


def _get_risk_reversal_25d(instruments: list[dict], spot: float, target_days: int) -> float | None:
    """
    Approximate 25-delta risk reversal:
    RR = IV(25d call) - IV(25d put)
    Uses strikes closest to 25-delta: roughly spot * exp(±0.3 * sqrt(T))
    """
    T = target_days / 365
    import math
    # 25-delta call strike ≈ spot * e^(+0.3*sqrt(T)), put ≈ spot * e^(-0.3*sqrt(T))
    call_target = spot * math.exp(+0.3 * math.sqrt(T))
    put_target  = spot * math.exp(-0.3 * math.sqrt(T))

    now_ts = time.time() * 1000

    best_call, best_put = None, None
    best_call_dist, best_put_dist = float("inf"), float("inf")

    for ins in instruments:
        exp_ts  = ins.get("expiration_timestamp", 0)
        days_to = (exp_ts - now_ts) / 86400000
        if abs(days_to - target_days) > 5:
            continue
        strike = ins.get("strike", 0)
        if ins.get("option_type") == "call":
            dist = abs(strike - call_target)
            if dist < best_call_dist:
                best_call_dist = dist
                best_call = ins
        elif ins.get("option_type") == "put":
            dist = abs(strike - put_target)
            if dist < best_put_dist:
                best_put_dist = dist
                best_put = ins

    if not best_call or not best_put:
        return None

    call_iv = _get_iv_for_instrument(best_call["instrument_name"])
    put_iv  = _get_iv_for_instrument(best_put["instrument_name"])

    if call_iv is None or put_iv is None:
        return None
    return round(call_iv - put_iv, 2)


def fetch_options_metrics() -> dict | None:
    """Fetch BTC spot, then 7d/30d ATM IV and 30d 25-delta risk reversal."""
    # Get BTC spot price
    price_data = _safe_get(f"{COINGECKO_BASE}/simple/price",
                           _cg_headers(),
                           {"ids": "bitcoin", "vs_currencies": "usd"})
    if not price_data:
        return None
    spot = price_data.get("bitcoin", {}).get("usd")
    if not spot:
        return None

    instruments = _get_deribit_instruments()
    if not instruments:
        return None

    atm_7d  = _find_atm_instrument(instruments, target_days=7,  spot=spot)
    atm_30d = _find_atm_instrument(instruments, target_days=30, spot=spot)

    iv_7d  = _get_iv_for_instrument(atm_7d["instrument_name"])  if atm_7d  else None
    iv_30d = _get_iv_for_instrument(atm_30d["instrument_name"]) if atm_30d else None
    rr_25d = _get_risk_reversal_25d(instruments, spot, target_days=30)

    return {
        "spot":             spot,
        "iv_7d":            iv_7d,
        "iv_30d":           iv_30d,
        "term_spread":      round(iv_7d - iv_30d, 2) if (iv_7d and iv_30d) else None,
        "risk_reversal_25d": rr_25d,
        "atm_7d_instrument":  atm_7d["instrument_name"]  if atm_7d  else None,
        "atm_30d_instrument": atm_30d["instrument_name"] if atm_30d else None,
    }


def format_options_metrics(raw: dict | None) -> dict:
    if not raw:
        return {"error": "Options data unavailable — Deribit fetch failed", "lead_time": "3-7 days"}

    iv_7d  = raw.get("iv_7d")
    iv_30d = raw.get("iv_30d")
    spread = raw.get("term_spread")
    rr     = raw.get("risk_reversal_25d")

    # Term structure alert
    if spread is not None and iv_7d is not None and iv_30d is not None:
        if iv_7d > iv_30d:
            ts_alert = "IV inversion — directional move likely within 3-7 days"
            ts_level = "extreme"
        elif spread < -5:
            ts_alert = "Term structure steep — low near-term volatility expected"
            ts_level = "neutral"
        else:
            ts_alert = "—"
            ts_level = "none"
    else:
        ts_alert, ts_level = "—", "none"

    # Risk reversal alert
    if rr is not None:
        if rr < -5:
            rr_alert = "Put skew extreme — sentiment exhaustion, contrarian bullish"
            rr_level = "extreme"
        elif rr > 5:
            rr_alert = "Call skew extreme — long crowding, contrarian bearish"
            rr_level = "extreme"
        elif rr < -2:
            rr_alert = "Moderate put skew — mild bearish lean"
            rr_level = "notable"
        elif rr > 2:
            rr_alert = "Moderate call skew — mild bullish lean"
            rr_level = "notable"
        else:
            rr_alert = "—"
            rr_level = "none"
    else:
        rr_alert, rr_level = "—", "none"

    result = {
        "name":       "BTC Options IV & Risk Reversal",
        "category":   "Derivatives · Options",
        "lead_time":  "3-7 days",
        "iv_7d":      round(iv_7d,  2) if iv_7d  else None,
        "iv_30d":     round(iv_30d, 2) if iv_30d else None,
        "term_spread": spread,
        "term_structure_label": (
            "Inverted (near-term vol > far-term)" if (spread and spread > 0) else
            "Normal (near-term vol < far-term)"
        ),
        "risk_reversal_25d": rr,
        "ts_alert":  ts_alert,
        "ts_level":  ts_level,
        "rr_alert":  rr_alert,
        "rr_level":  rr_level,
        # Surfaces the single strongest alert for card display
        "alert":     ts_alert if ts_level == "extreme" else rr_alert,
        "alert_level": "extreme" if (ts_level == "extreme" or rr_level == "extreme") else
                       "notable" if (ts_level == "notable" or rr_level == "notable") else "none",
        "atm_7d_instrument":  raw.get("atm_7d_instrument"),
        "atm_30d_instrument": raw.get("atm_30d_instrument"),
        "spot": raw.get("spot"),
    }

    # Persist to SQLite
    _upsert("options_history", {
        "date":              _today(),
        "iv_7d":             iv_7d,
        "iv_30d":            iv_30d,
        "term_spread":       spread,
        "risk_reversal_25d": rr,
        "stored_at":         datetime.utcnow().isoformat(),
    })
    return result


def _build_options() -> dict:
    global _options_cache
    now = time.time()
    if _options_cache["data"] and (now - _options_cache["ts"]) < OPTIONS_TTL:
        return _options_cache["data"]
    result = format_options_metrics(fetch_options_metrics())
    _options_cache = {"data": result, "ts": now}
    return result


# ════════════════════════════════════════════════════════════════════════════
# INDICATOR 2 — Coinbase Premium Index
# Source: CryptoQuant (CRYPTOQUANT_API_KEY already set)
# Lead time: 1-3 days
# ════════════════════════════════════════════════════════════════════════════

def fetch_coinbase_premium() -> dict | None:
    if not CRYPTOQUANT_KEY:
        return None
    url  = f"{CRYPTOQUANT_BASE}/market-data/coinbase-premium-index"
    data = _safe_get(url, _cq_headers(), {
        "window": "hour",
        "from":   _days_ago(7),
        "to":     _today(),
        "limit":  168,
    })
    if not data:
        return None
    try:
        rows   = data["data"]["values"]
        values = [r["value"] for r in rows if r.get("value") is not None]
        if not values:
            return None
        current = values[-1]
        avg_24h = sum(values[-24:]) / min(24, len(values))
        avg_7d  = sum(values) / len(values)
        # Trend: compare last 6h vs prior 6h
        trend   = (sum(values[-6:]) / 6) - (sum(values[-12:-6]) / 6) if len(values) >= 12 else 0
        return {
            "current":   round(current, 4),
            "avg_24h":   round(avg_24h, 4),
            "avg_7d":    round(avg_7d,  4),
            "trend_6h":  round(trend,   4),
            "values":    values[-48:],  # last 48h for sparkline
        }
    except (KeyError, IndexError, TypeError) as e:
        print(f"[leading] coinbase_premium parse error: {e}")
        return None


def format_coinbase_premium(raw: dict | None) -> dict:
    if not raw:
        return {"error": "Coinbase premium unavailable — CryptoQuant key may need upgrade", "lead_time": "1-3 days"}

    current = raw["current"]
    avg_24h = raw["avg_24h"]
    trend   = raw["trend_6h"]
    values  = raw.get("values", [])

    # History for percentile
    hist = _query_history("coinbase_premium_history", ["date", "premium_pct", "avg_24h"], 90)
    hist_vals = [r["premium_pct"] for r in hist if r["premium_pct"] is not None]
    pctile = _pct_rank(hist_vals, current)

    # Alert
    if current > 0.1 and trend > 0:
        alert = "US buyers aggressive — 1-3 day lead signal"
        level = "extreme"
    elif current < -0.1 and trend < 0:
        alert = "US demand weak — selling pressure"
        level = "notable"
    elif avg_24h < 0 and current > 0:
        alert = "Premium flip — watch for price follow-through"
        level = "notable"
    elif current > 0.05:
        alert = "Mild positive premium — US buyers active"
        level = "neutral"
    else:
        alert = "—"
        level = "none"

    # Sparkline: normalize values to 0-100
    if values:
        min_v, max_v = min(values), max(values)
        rng = max_v - min_v
        spark = [round((v - min_v) / rng * 100) if rng else 50 for v in values[-12:]]
    else:
        spark = []

    result = {
        "name":       "Coinbase Premium Index",
        "category":   "Market Structure · Exchange",
        "lead_time":  "1-3 days",
        "current":    f"{current:+.4f}%",
        "avg_24h":    f"{avg_24h:+.4f}%",
        "avg_7d":     f"{raw['avg_7d']:+.4f}%",
        "trend_6h":   f"{trend:+.4f}%",
        "percentile": pctile,
        "alert":      alert,
        "alert_level": level,
        "pattern":    (
            "Coinbase trading at premium — US institutional bid" if current > 0.05 else
            "Coinbase at discount — US selling or demand absent" if current < -0.05 else
            "Premium near zero — no directional bias"
        ),
        "spark": spark,
    }

    _upsert("coinbase_premium_history", {
        "date":      _today(),
        "premium_pct": current,
        "avg_24h":   avg_24h,
        "stored_at": datetime.utcnow().isoformat(),
    })
    return result


def _build_coinbase_premium() -> dict:
    global _premium_cache
    now = time.time()
    if _premium_cache["data"] and (now - _premium_cache["ts"]) < PREMIUM_TTL:
        return _premium_cache["data"]
    result = format_coinbase_premium(fetch_coinbase_premium())
    _premium_cache = {"data": result, "ts": now}
    return result


# ════════════════════════════════════════════════════════════════════════════
# INDICATOR 3 — Cumulative 30-Day Funding Rate
# Source: CoinGecko derivatives (already used in main.py)
# Lead time: 1-5 days (exhaustion signal)
# ════════════════════════════════════════════════════════════════════════════

def fetch_funding_cumulative() -> dict | None:
    """
    Fetches current 8h funding rate from CoinGecko derivatives,
    then reads 30 days of stored daily rates from SQLite to compute
    cumulative cost. Stores today's daily rate for future calculations.
    """
# Fetch current funding rate from shared CoinGecko cache
    try:
        shared    = get_shared_coingecko()
        data      = shared.get("derivatives")
        if not data:
            return None
        btc_perps = [
            m for m in data
            if m.get("index_id") == "BTC"
            and m.get("contract_type") == "perpetual"
            and m.get("funding_rate") is not None
        ]
        if not btc_perps:
            return None
        # Volume-weighted average funding rate
        total_oi  = sum(m.get("open_interest", 0) for m in btc_perps)
        if total_oi == 0:
            avg_rate = sum(m["funding_rate"] for m in btc_perps) / len(btc_perps)
        else:
            avg_rate = sum(m["funding_rate"] * m.get("open_interest", 0) for m in btc_perps) / total_oi
        # 8h rate → daily rate (3 settlements per day)
        daily_rate = avg_rate * 3
    except Exception as e:
        print(f"[leading] funding fetch error: {e}")
        return None

    # Store today's daily rate
    _upsert("funding_cumulative_history", {
        "date":           _today(),
        "daily_rate":     round(daily_rate, 6),
        "cumulative_7d":  None,
        "cumulative_30d": None,
        "stored_at":      datetime.utcnow().isoformat(),
    })

    # Read last 30 days and calculate cumulative
    hist = _query_history(
        "funding_cumulative_history",
        ["date", "daily_rate", "cumulative_7d", "cumulative_30d"], 30
    )
    daily_rates = [r["daily_rate"] for r in hist if r["daily_rate"] is not None]

    cum_7d  = round(sum(daily_rates[:7]),  4) if len(daily_rates) >= 7  else None
    cum_30d = round(sum(daily_rates[:30]), 4) if len(daily_rates) >= 30 else None
    cum_available = round(sum(daily_rates), 4)

    # Update today's row with cumulative values
    _upsert("funding_cumulative_history", {
        "date":           _today(),
        "daily_rate":     round(daily_rate, 6),
        "cumulative_7d":  cum_7d,
        "cumulative_30d": cum_30d,
        "stored_at":      datetime.utcnow().isoformat(),
    })

    return {
        "current_8h_rate": round(avg_rate, 6),
        "daily_rate":      round(daily_rate, 6),
        "cumulative_7d":   cum_7d,
        "cumulative_30d":  cum_30d,
        "cum_available":   cum_available,
        "days_available":  len(daily_rates),
        "daily_rates":     daily_rates[:14],   # last 14 days for sparkline
    }


def format_funding_cumulative(raw: dict | None) -> dict:
    if not raw:
        return {"error": "Funding rate data unavailable", "lead_time": "1-5 days"}

    daily    = raw["daily_rate"]
    cum_7d   = raw["cumulative_7d"]
    cum_30d  = raw["cumulative_30d"]
    cum_use  = cum_30d if cum_30d is not None else raw["cum_available"]
    days_avail = raw["days_available"]

    # Alert
    if cum_30d is not None and cum_30d > 3.0:
        alert = "Long exhaustion threshold — longs paid 3%+ in 30d, mechanical unwind risk"
        level = "extreme"
    elif cum_30d is not None and cum_30d < -1.0:
        alert = "Short squeeze fuel — shorts paying sustained premium"
        level = "extreme"
    elif daily < 0 and (cum_7d or 0) > 0:
        alert = "Funding flip — sentiment shifting from long to short"
        level = "notable"
    elif cum_30d is not None and cum_30d > 1.5:
        alert = "Longs paying elevated cumulative funding — watch for exhaustion"
        level = "notable"
    else:
        alert = "—"
        level = "none"

    # Sparkline
    rates = raw.get("daily_rates", [])
    if rates:
        min_v, max_v = min(rates), max(rates)
        rng   = max_v - min_v
        spark = [round((v - min_v) / rng * 100) if rng else 50 for v in rates[-12:]]
    else:
        spark = []

    caveat = f" ({days_avail}d data, building to 30d)" if days_avail < 30 else ""

    return {
        "name":         "Cumulative Funding Rate",
        "category":     "Derivatives · Funding",
        "lead_time":    "1-5 days",
        "current_8h":   f"{raw['current_8h_rate']:+.4f}%",
        "daily_rate":   f"{daily:+.4f}%",
        "cumulative_7d":  f"{cum_7d:+.3f}%" if cum_7d is not None else f"Building…{caveat}",
        "cumulative_30d": f"{cum_use:+.3f}%{caveat}",
        "alert":        alert,
        "alert_level":  level,
        "pattern": (
            "Longs paying high cumulative cost — exhaustion watch" if (cum_30d or 0) > 2 else
            "Shorts dominating funding — short squeeze setup" if (cum_30d or 0) < -0.5 else
            "Funding neutral — no structural bias"
        ),
        "spark": spark,
    }


def _build_funding_cumulative() -> dict:
    global _funding_cache
    now = time.time()
    if _funding_cache["data"] and (now - _funding_cache["ts"]) < FUNDING_TTL:
        return _funding_cache["data"]
    result = format_funding_cumulative(fetch_funding_cumulative())
    _funding_cache = {"data": result, "ts": now}
    return result


# ════════════════════════════════════════════════════════════════════════════
# INDICATOR 4 — Global M2 Money Supply
# Source: FRED (FRED_API_KEY)
# Lead time: 8-12 weeks
# ════════════════════════════════════════════════════════════════════════════

# FX conversion: approximate USD equivalents for non-USD series
# FRED series already in USD for eurozone; China/Japan need FX conversion
# We use a rough static FX table that can be overridden manually if needed
APPROX_FX_USD = {
    "cny_per_usd": 7.25,    # update manually if major move
    "jpy_per_usd": 155.0,
    "eur_per_usd": 1.08,
}

FRED_M2_SERIES = {
    "us":       "M2SL",               # Billions of USD, monthly, NSA
    "eurozone": "MABMM301EZM189S",    # Millions of EUR, monthly
    "china":    "MABMM301CNM189S",    # Millions of CNY, monthly
    "japan":    "MABMM301JPM189S",    # Millions of JPY, monthly
}


def fetch_global_m2() -> dict | None:
    if not FRED_API_KEY:
        return None

    series = {}
    for region, sid in FRED_M2_SERIES.items():
        pairs = _fetch_fred(sid, n_days=550)  # ~18 months for YoY
        if pairs:
            series[region] = pairs
        else:
            print(f"[leading] M2 FRED fetch empty for {region} ({sid})")
            series[region] = []

    # Need at least US data
    if not series.get("us"):
        return None

    def _latest_val(pairs):
        return pairs[-1][1] if pairs else None

    def _val_n_months_ago(pairs, n):
        """Roughly n months back (n*30 days)."""
        if not pairs:
            return None
        target_date = date.today() - timedelta(days=n * 30)
        # Find closest entry
        best = min(pairs, key=lambda p: abs(
            (date.fromisoformat(p[0]) - target_date).days
        ))
        return best[1]

    # Convert to USD billions
    us_bil  = (_latest_val(series["us"]) or 0)  # already in billions USD
    ez_bil  = (_latest_val(series["eurozone"]) or 0) / 1e3 * APPROX_FX_USD["eur_per_usd"]  # millions EUR → billions USD
    cn_bil  = (_latest_val(series["china"])    or 0) / 1e3 / APPROX_FX_USD["cny_per_usd"]  # millions CNY → billions USD
    jp_bil  = (_latest_val(series["japan"])    or 0) / 1e3 / APPROX_FX_USD["jpy_per_usd"]  # millions JPY → billions USD

    global_m2 = us_bil + ez_bil + cn_bil + jp_bil

    # MoM and YoY growth (using US as primary driver for now)
    us_1m_ago  = _val_n_months_ago(series["us"], 1)
    us_12m_ago = _val_n_months_ago(series["us"], 12)
    mom = ((us_bil - us_1m_ago)  / us_1m_ago  * 100) if us_1m_ago  else None
    yoy = ((us_bil - us_12m_ago) / us_12m_ago * 100) if us_12m_ago else None

    # Last date available
    last_date = series["us"][-1][0] if series["us"] else "Unknown"

    return {
        "us_m2_bil":       round(us_bil,  1),
        "eurozone_m2_bil": round(ez_bil,  1),
        "china_m2_bil":    round(cn_bil,  1),
        "japan_m2_bil":    round(jp_bil,  1),
        "global_m2_bil":   round(global_m2, 1),
        "mom_growth_pct":  round(mom, 3) if mom else None,
        "yoy_growth_pct":  round(yoy, 3) if yoy else None,
        "last_date":       last_date,
        "fx_assumptions":  APPROX_FX_USD,
    }


def format_global_m2(raw: dict | None) -> dict:
    if not raw:
        return {"error": "Global M2 unavailable — check FRED_API_KEY", "lead_time": "8-12 weeks"}

    yoy = raw.get("yoy_growth_pct")
    mom = raw.get("mom_growth_pct")

    if yoy is not None and yoy > 8:
        alert = "Global M2 expanding — historical BTC tailwind with 8-12 week lag"
        level = "extreme"
    elif yoy is not None and yoy < 2:
        alert = "Global M2 contracting — historical BTC headwind with 8-12 week lag"
        level = "notable"
    elif mom is not None and mom > 0:
        alert = "M2 momentum turning — watch for BTC follow 2-3 months out"
        level = "neutral"
    else:
        alert = "—"
        level = "none"

    result = {
        "name":        "Global M2 Money Supply",
        "category":    "Macro · Liquidity",
        "lead_time":   "8-12 weeks",
        "global_m2":   f"${raw['global_m2_bil']:,.0f}B",
        "us_m2":       f"${raw['us_m2_bil']:,.0f}B",
        "eurozone_m2": f"${raw['eurozone_m2_bil']:,.0f}B",
        "china_m2":    f"${raw['china_m2_bil']:,.0f}B",
        "japan_m2":    f"${raw['japan_m2_bil']:,.0f}B",
        "mom_growth":  f"{mom:+.2f}%" if mom is not None else "–",
        "yoy_growth":  f"{yoy:+.2f}%" if yoy is not None else "–",
        "data_lag_note": "FRED data lags ~6 weeks. This is a regime indicator, not a short-term signal.",
        "last_date":   raw.get("last_date", "Unknown"),
        "alert":       alert,
        "alert_level": level,
        "pattern": (
            "M2 expansion — risk-on regime historically follows" if (yoy or 0) > 5 else
            "M2 deceleration — tightening conditions" if (yoy or 0) < 3 else
            "M2 stable — neutral regime backdrop"
        ),
        "fx_assumptions": raw.get("fx_assumptions"),
    }

    _upsert("global_m2_history", {
        "date":          _today(),
        "us_m2":         raw["us_m2_bil"],
        "eurozone_m2":   raw["eurozone_m2_bil"],
        "china_m2":      raw["china_m2_bil"],
        "japan_m2":      raw["japan_m2_bil"],
        "global_m2_usd": raw["global_m2_bil"],
        "mom_growth":    raw.get("mom_growth_pct"),
        "yoy_growth":    raw.get("yoy_growth_pct"),
        "stored_at":     datetime.utcnow().isoformat(),
    })
    return result


def _build_global_m2() -> dict:
    global _m2_cache
    if not _cache_is_stale(_m2_cache):
        return _m2_cache["data"]
    result = format_global_m2(fetch_global_m2())
    _m2_cache = {"data": result, "ts": time.time()}
    return result


# ════════════════════════════════════════════════════════════════════════════
# INDICATOR 5 — CFTC Commitment of Traders (BTC Futures)
# Source: CFTC public Socrata API (no key required)
# Lead time: 1-2 weeks
# ════════════════════════════════════════════════════════════════════════════

CFTC_BTC_COMMODITY_CODE = "133741"   # CME BTC futures


def fetch_cot_btc() -> dict | None:
    """
    Fetches 52 weeks of COT data for BTC futures from the CFTC Socrata API.
    Leveraged fund positioning is the primary signal.
    """
    try:
        resp = requests.get(CFTC_API_BASE, params={
            "$where": f"cftc_commodity_code='{CFTC_BTC_COMMODITY_CODE}'",
            "$order": "report_date_as_yyyy_mm_dd DESC",
            "$limit": 52,
        }, timeout=20)
        resp.raise_for_status()
        rows = resp.json()
    except Exception as e:
        print(f"[leading] CFTC fetch error: {e}")
        return None

    if not rows:
        return None

    try:
        latest = rows[0]
        lev_long  = float(latest.get("lev_money_positions_long_all",  0))
        lev_short = float(latest.get("lev_money_positions_short_all", 0))
        lev_net   = lev_long - lev_short
        am_long   = float(latest.get("asset_mgr_positions_long_all",  0))
        am_short  = float(latest.get("asset_mgr_positions_short_all", 0))
        total_oi  = float(latest.get("open_interest_all", 0))
        lev_net_pct = (lev_net / total_oi * 100) if total_oi else None
        report_date = latest.get("report_date_as_yyyy_mm_dd", "")[:10]

        # 52-week range of lev_net_pct for percentile
        all_lev_nets = []
        for r in rows[1:]:
            try:
                l = float(r.get("lev_money_positions_long_all",  0))
                s = float(r.get("lev_money_positions_short_all", 0))
                oi = float(r.get("open_interest_all", 1))
                all_lev_nets.append((l - s) / oi * 100)
            except (ValueError, TypeError, ZeroDivisionError):
                pass

        return {
            "lev_long":      lev_long,
            "lev_short":     lev_short,
            "lev_net":       lev_net,
            "asset_mgr_long":  am_long,
            "asset_mgr_short": am_short,
            "total_oi":      total_oi,
            "lev_net_pct":   round(lev_net_pct, 2) if lev_net_pct is not None else None,
            "all_lev_nets":  all_lev_nets,
            "report_date":   report_date,
        }
    except (KeyError, ValueError, TypeError) as e:
        print(f"[leading] COT parse error: {e}")
        return None


def format_cot_btc(raw: dict | None) -> dict:
    if not raw:
        return {"error": "CFTC COT data unavailable", "lead_time": "1-2 weeks"}

    lev_net_pct  = raw.get("lev_net_pct")
    all_nets     = raw.get("all_lev_nets", [])
    pctile       = _pct_rank(all_nets, lev_net_pct) if lev_net_pct is not None else None

    # Alert
    if pctile is not None and pctile >= 90:
        alert = "Leveraged funds max long — historically precedes correction"
        level = "extreme"
    elif pctile is not None and pctile <= 10:
        alert = "Leveraged funds max short — historically precedes short squeeze"
        level = "extreme"
    elif pctile is not None and pctile >= 75:
        alert = "Leveraged funds heavily long — crowding risk"
        level = "notable"
    elif pctile is not None and pctile <= 25:
        alert = "Leveraged funds net short — contrarian setup developing"
        level = "notable"
    else:
        alert = "—"
        level = "none"

    # Previous week's net for flip detection
    prev_net = all_nets[0] if all_nets else None
    flip_note = ""
    if prev_net is not None and lev_net_pct is not None:
        if prev_net < 0 and lev_net_pct > 0:
            flip_note = "Positioning flip long — trend change signal"
        elif prev_net > 0 and lev_net_pct < 0:
            flip_note = "Positioning flip short — trend change signal"

    result = {
        "name":          "CFTC COT — BTC Futures",
        "category":      "Positioning · Institutional",
        "lead_time":     "1-2 weeks",
        "report_date":   raw.get("report_date"),
        "lev_long":      f"{raw['lev_long']:,.0f}",
        "lev_short":     f"{raw['lev_short']:,.0f}",
        "lev_net":       f"{raw['lev_net']:+,.0f}",
        "lev_net_pct":   f"{lev_net_pct:+.1f}% of OI" if lev_net_pct is not None else "–",
        "asset_mgr_net": f"{raw['asset_mgr_long'] - raw['asset_mgr_short']:+,.0f}",
        "total_oi":      f"{raw['total_oi']:,.0f}",
        "percentile":    pctile,
        "alert":         alert if not flip_note else flip_note,
        "alert_level":   level,
        "flip_note":     flip_note,
        "pattern": (
            "Leveraged funds crowded long — contrarian caution" if (pctile or 0) >= 75 else
            "Leveraged funds crowded short — contrarian opportunity" if (pctile or 0) <= 25 else
            "Positioning neutral — no crowding signal"
        ),
        "release_cadence": "Weekly, Fridays ~3:30pm ET. Data reflects prior Tuesday positions.",
    }

    _upsert("cot_history", {
        "date":          _today(),
        "lev_long":      raw["lev_long"],
        "lev_short":     raw["lev_short"],
        "lev_net":       raw["lev_net"],
        "asset_mgr_long":  raw["asset_mgr_long"],
        "asset_mgr_short": raw["asset_mgr_short"],
        "total_oi":      raw["total_oi"],
        "lev_net_pct":   lev_net_pct,
        "stored_at":     datetime.utcnow().isoformat(),
    })
    return result


def _build_cot() -> dict:
    global _cot_cache
    if not _cache_is_stale(_cot_cache):
        return _cot_cache["data"]
    result = format_cot_btc(fetch_cot_btc())
    _cot_cache = {"data": result, "ts": time.time()}
    return result


# ════════════════════════════════════════════════════════════════════════════
# INDICATOR 6 — Tether Mint/Burn Events
# Source: CoinGecko (same endpoint used in main.py for stablecoin supply)
# Lead time: 1-7 days
# Integration: also extend store_stablecoin_snapshot() in main.py to store daily_change
# ════════════════════════════════════════════════════════════════════════════

LARGE_MINT_THRESHOLD_USD = 500_000_000   # $500M in one day

def fetch_tether_mints() -> dict | None:
    """
    Fetches current USDT market cap from shared CoinGecko cache and computes
    daily delta by comparing to yesterday's stored value in SQLite.
    """
    try:
        data = _cached_get(
            f"{COINGECKO_BASE}/simple/price",
            _cg_headers(),
            {"ids": "tether", "vs_currencies": "usd", "include_market_cap": "true"}
        )
        usdt_now = data.get("tether", {}).get("usd_market_cap") if data else None
        if not usdt_now:
            return None
    except Exception as e:
        print(f"[leading] tether fetch error: {e}")
        return None

    # Look up yesterday's stored value
    hist = _query_history("tether_mint_history", ["date", "usdt_supply", "daily_change"], 30)
    yesterday_supply = hist[0]["usdt_supply"] if hist else None
    daily_change = round(usdt_now - yesterday_supply, 0) if yesterday_supply else None

    # 7-day cumulative net
    changes_30d = [r["daily_change"] for r in hist if r["daily_change"] is not None]
    cum_7d  = round(sum(changes_30d[:7]),  0) if len(changes_30d) >= 7  else None
    cum_30d = round(sum(changes_30d[:30]), 0) if len(changes_30d) >= 30 else None

    large_mint = daily_change is not None and daily_change > LARGE_MINT_THRESHOLD_USD

    # Store today's snapshot
    _upsert("tether_mint_history", {
        "date":            _today(),
        "usdt_supply":     round(usdt_now, 0),
        "daily_change":    daily_change,
        "large_mint_flag": int(large_mint),
        "stored_at":       datetime.utcnow().isoformat(),
    })

    return {
        "usdt_now":      usdt_now,
        "daily_change":  daily_change,
        "cum_7d":        cum_7d,
        "cum_30d":       cum_30d,
        "large_mint":    large_mint,
        "days_available": len(changes_30d) + 1,
    }


def format_tether_mints(raw: dict | None) -> dict:
    if not raw:
        return {"error": "Tether mint data unavailable", "lead_time": "1-7 days"}

    daily   = raw.get("daily_change")
    cum_7d  = raw.get("cum_7d")
    cum_30d = raw.get("cum_30d")

    def _fmt_usd(v):
        if v is None: return "–"
        sign = "+" if v >= 0 else ""
        if abs(v) >= 1e9:  return f"{sign}${abs(v)/1e9:.2f}B"
        if abs(v) >= 1e6:  return f"{sign}${abs(v)/1e6:.0f}M"
        return f"{sign}${v:,.0f}"

    # Alert
    if raw.get("large_mint"):
        alert = f"Large Tether mint ({_fmt_usd(daily)}) — new capital entering ecosystem, 1-7 day lead"
        level = "extreme"
    elif cum_7d is not None and cum_7d < -1_000_000_000:
        alert = "Sustained stablecoin burn — capital exiting or deploying"
        level = "notable"
    elif daily is not None and daily > 250_000_000:
        alert = "Meaningful mint — elevated stablecoin issuance"
        level = "notable"
    else:
        alert = "—"
        level = "none"

    return {
        "name":         "Tether Mint / Burn",
        "category":     "Stablecoin · Liquidity",
        "lead_time":    "1-7 days",
        "usdt_supply":  f"${raw['usdt_now']/1e9:.1f}B",
        "daily_change": _fmt_usd(daily),
        "cum_7d":       _fmt_usd(cum_7d),
        "cum_30d":      _fmt_usd(cum_30d),
        "large_mint_today": raw.get("large_mint", False),
        "days_available": raw.get("days_available", 1),
        "alert":        alert,
        "alert_level":  level,
        "pattern": (
            "Active minting — new liquidity entering crypto" if (daily or 0) > 200_000_000 else
            "Net burn — liquidity leaving or being deployed" if (daily or 0) < -200_000_000 else
            "Stable supply — no significant mint/burn signal"
        ),
        "note": "Daily delta requires 2 consecutive daily snapshots to calculate. Accuracy improves over time.",
    }


def _build_tether_mints() -> dict:
    global _tether_cache
    if not _cache_is_stale(_tether_cache):
        return _tether_cache["data"]
    result = format_tether_mints(fetch_tether_mints())
    _tether_cache = {"data": result, "ts": time.time()}
    return result


# ════════════════════════════════════════════════════════════════════════════
# INDICATOR 7 — Breakeven Inflation Rates (TIPS Spread)
# Source: FRED (same key as macro_routes.py)
# Integration: add to macro_routes.py /macro/metrics response, or use standalone here
# Lead time: real-time regime diagnostic
# ════════════════════════════════════════════════════════════════════════════

def fetch_breakeven_inflation() -> dict | None:
    if not FRED_API_KEY:
        return None
    result = {}
    for key, series_id in FRED_BREAKEVEN.items():
        pairs = _fetch_fred(series_id, n_days=400)
        if pairs:
            result[key] = pairs
        else:
            result[key] = []
    if not result.get("be_10y"):
        return None
    return result


def format_breakeven_inflation(raw: dict | None) -> dict:
    if not raw:
        return {"error": "Breakeven inflation unavailable — check FRED_API_KEY", "lead_time": "Real-time"}

    def _latest(pairs):
        return pairs[-1][1] if pairs else None

    def _d5_chg(pairs):
        if not pairs or len(pairs) < 6: return None
        vals = [p[1] for p in pairs]
        return round(vals[-1] - vals[-6], 3)

    def _d20_chg(pairs):
        if not pairs or len(pairs) < 21: return None
        vals = [p[1] for p in pairs]
        return round(vals[-1] - vals[-21], 3)

    be_5y   = _latest(raw.get("be_5y",   []))
    be_10y  = _latest(raw.get("be_10y",  []))
    be_5y5y = _latest(raw.get("be_5y5y", []))

    d5_10y  = _d5_chg(raw.get("be_10y", []))
    d20_10y = _d20_chg(raw.get("be_10y", []))

    # Alert
    if be_5y5y is not None and be_5y5y > 2.5:
        alert = "Long-term inflation anchor breaking — stagflation risk"
        level = "extreme"
    elif be_10y is not None and be_10y > 3.0:
        alert = "Inflation expectations elevated — Fed constrained from cutting"
        level = "extreme"
    elif d5_10y is not None and d5_10y > 0.15:
        alert = "Inflation expectations rising — real yield falling, potential BTC tailwind"
        level = "notable"
    elif d5_10y is not None and d5_10y < -0.15:
        alert = "Disinflation fear — growth scare, not rate relief"
        level = "notable"
    else:
        alert = "—"
        level = "none"

    result = {
        "name":         "Breakeven Inflation (TIPS Spread)",
        "category":     "Macro · Real Yields",
        "lead_time":    "Real-time regime diagnostic",
        "be_5y":        f"{be_5y:.2f}%"   if be_5y   else "–",
        "be_10y":       f"{be_10y:.2f}%"  if be_10y  else "–",
        "be_5y5y":      f"{be_5y5y:.2f}%" if be_5y5y else "–",
        "d5_10y":       f"{d5_10y:+.3f}%" if d5_10y  else "–",
        "d20_10y":      f"{d20_10y:+.3f}%" if d20_10y else "–",
        "interpretation": (
            "Rising breakevens = market pricing in more inflation = real yields falling = "
            "historical BTC tailwind. Falling breakevens = disinflation or growth scare."
        ),
        "alert":       alert,
        "alert_level": level,
        "pattern": (
            "Breakevens elevated — inflation expectations anchored high" if (be_10y or 0) > 2.8 else
            "Breakevens moderate — inflation expectations near target" if (be_10y or 0) > 2.0 else
            "Breakevens low — disinflation / deflation risk priced"
        ),
        "source": "FRED: T5YIE, T10YIE, T5YIFR",
    }

    _upsert("breakeven_history", {
        "date":      _today(),
        "be_5y":     be_5y,
        "be_10y":    be_10y,
        "be_5y5y":   be_5y5y,
        "stored_at": datetime.utcnow().isoformat(),
    })
    return result


def _build_breakevens() -> dict:
    global _breakeven_cache
    if not _cache_is_stale(_breakeven_cache):
        return _breakeven_cache["data"]
    result = format_breakeven_inflation(fetch_breakeven_inflation())
    _breakeven_cache = {"data": result, "ts": time.time()}
    return result


# ════════════════════════════════════════════════════════════════════════════
# INDICATOR 8 — CME Basis Enhanced (trend + DTX normalization)
# Source: Extends existing format_cme_basis() in main.py
# Integration note: this route fetches independently; to avoid duplicate yFinance
# calls, consider importing basis data from main.py's existing cache.
# Lead time: 1-3 days
# ════════════════════════════════════════════════════════════════════════════

def fetch_basis_enhanced() -> dict | None:
    """
    Fetches CME BTC futures via yFinance (same as main.py) and adds:
    - 5-day trend direction
    - Days-to-expiry normalized basis (annualized is already DTX-adjusted)
    - 5-day momentum signal
    Designed to be called from main.py's existing basis flow if preferred.
    """
    try:
        import yfinance as yf
        from datetime import date as dt_date
        import math

        btc_future = yf.Ticker("BTC=F")
        hist       = btc_future.history(period="10d")
        if hist.empty:
            return None

        futures_px = float(hist["Close"].iloc[-1])
        info       = btc_future.info or {}

        # Spot price from CoinGecko
        spot_data = requests.get(
            f"{COINGECKO_BASE}/simple/price",
            headers=_cg_headers(),
            params={"ids": "bitcoin", "vs_currencies": "usd"},
            timeout=10
        ).json()
        spot_px = spot_data.get("bitcoin", {}).get("usd")
        if not spot_px:
            return None

        # Expiry: third Friday of next quarterly month (Mar/Jun/Sep/Dec)
        today = dt_date.today()
        quarterly_months = [3, 6, 9, 12]
        next_q = next(
            (m for m in quarterly_months + [m + 12 for m in quarterly_months]
             if (today.year + (m > 12)) * 100 + (m if m <= 12 else m - 12) > today.year * 100 + today.month),
            None
        )
        if not next_q:
            return None
        exp_year  = today.year + (next_q > 12)
        exp_month = next_q if next_q <= 12 else next_q - 12
        # Third Friday
        first_day  = dt_date(exp_year, exp_month, 1)
        first_fri  = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
        third_fri  = first_fri + timedelta(weeks=2)
        days_to_exp = max(1, (third_fri - today).days)

        raw_basis    = (futures_px - spot_px) / spot_px * 100
        annualized   = raw_basis * (365 / days_to_exp)

        # Read last 10 basis snapshots for trend
        hist_rows = _query_history(
            "basis_enhanced_history",
            ["date", "annualized", "dtx_normalized", "trend_5d"], 10
        )
        recent_vals = [r["annualized"] for r in hist_rows if r["annualized"] is not None]
        trend_5d    = "rising"  if (len(recent_vals) >= 3 and annualized > recent_vals[2]) else \
                      "falling" if (len(recent_vals) >= 3 and annualized < recent_vals[2]) else \
                      "flat"

        return {
            "futures_px":  futures_px,
            "spot_px":     spot_px,
            "raw_basis":   round(raw_basis, 4),
            "annualized":  round(annualized, 2),
            "days_to_exp": days_to_exp,
            "trend_5d":    trend_5d,
            "recent_vals": recent_vals[:5],
        }
    except Exception as e:
        print(f"[leading] basis_enhanced error: {e}")
        return None


def format_basis_enhanced(raw: dict | None) -> dict:
    if not raw:
        return {"error": "CME basis data unavailable", "lead_time": "1-3 days"}

    annualized = raw["annualized"]
    trend      = raw["trend_5d"]
    dtx        = raw["days_to_exp"]

    # Base alert (same thresholds as main.py)
    if annualized < 0:
        base_alert = "Backwardation — futures below spot"
        level = "extreme"
    elif annualized < 5:
        base_alert = "Basis compressed — carry trade unattractive"
        level = "notable"
    elif annualized > 20:
        base_alert = "Extreme basis — cash/carry highly attractive"
        level = "extreme"
    elif annualized > 15:
        base_alert = "Elevated basis — above normal carry premium"
        level = "notable"
    else:
        base_alert = "—"
        level = "none"

    # Trend enhancement
    trend_note = (
        f"Basis {trend} over 5 days — "
        f"{'institutional demand building' if trend == 'rising' else 'carry trade compressing' if trend == 'falling' else 'basis stable'}"
    )

    result = {
        "name":        "CME Basis Enhanced",
        "category":    "Derivatives · Cash & Carry",
        "lead_time":   "1-3 days",
        "annualized":  f"{annualized:+.2f}%",
        "raw_basis":   f"{raw['raw_basis']:+.4f}%",
        "days_to_exp": dtx,
        "trend_5d":    trend,
        "trend_note":  trend_note,
        "futures_px":  raw["futures_px"],
        "spot_px":     raw["spot_px"],
        "alert":       base_alert,
        "alert_level": level,
        "pattern": f"{base_alert} · {trend_note}",
        "spark":   raw.get("recent_vals", []),
    }

    _upsert("basis_enhanced_history", {
        "date":          _today(),
        "annualized":    annualized,
        "dtx_normalized": annualized,    # already DTX-normalized (annualized)
        "trend_5d":      trend,
        "stored_at":     datetime.utcnow().isoformat(),
    })
    return result


def _build_basis_enhanced() -> dict:
    global _basis_enh_cache
    now = time.time()
    if _basis_enh_cache["data"] and (now - _basis_enh_cache["ts"]) < BASIS_TTL:
        return _basis_enh_cache["data"]
    result = format_basis_enhanced(fetch_basis_enhanced())
    _basis_enh_cache = {"data": result, "ts": now}
    return result


# ════════════════════════════════════════════════════════════════════════════
# BACKGROUND FUNDING POLLER
# Stores daily funding snapshots every 8 hours (matching settlement cadence)
# Starts automatically when this module is imported.
# ════════════════════════════════════════════════════════════════════════════

def _poll_funding() -> None:
    """Background thread: stores funding rate snapshot every 8 hours."""
    INTERVAL = 8 * 3600
    print("[funding_poller] Starting — interval 8 hours")
    while True:
        try:
            raw = fetch_funding_cumulative()
            if raw:
                print(f"[funding_poller] Stored daily_rate={raw['daily_rate']:+.4f}% cum_30d={raw['cum_30d']}")
            else:
                print("[funding_poller] No data returned")
        except Exception as e:
            print(f"[funding_poller] Error: {e}")
        time.sleep(INTERVAL)

threading.Thread(target=_poll_funding, daemon=True).start()


# ════════════════════════════════════════════════════════════════════════════
# ROUTES
# ════════════════════════════════════════════════════════════════════════════

@leading_router.get("/options")
def get_options():
    """BTC IV term structure and 25-delta risk reversal. Deribit public API."""
    return _build_options()

@leading_router.get("/coinbase-premium")
def get_coinbase_premium():
    """Coinbase vs aggregate BTC price premium. CryptoQuant."""
    return _build_coinbase_premium()

@leading_router.get("/funding-cumulative")
def get_funding_cumulative():
    """Cumulative 7d and 30d funding cost. CoinGecko derivatives."""
    return _build_funding_cumulative()

@leading_router.get("/global-m2")
def get_global_m2():
    """Global M2 (US + EU + China + Japan) with MoM and YoY growth. FRED."""
    return _build_global_m2()

@leading_router.get("/cot")
def get_cot():
    """CFTC COT — BTC futures leveraged fund positioning. Public CFTC API."""
    return _build_cot()

@leading_router.get("/tether-mints")
def get_tether_mints():
    """Tether daily mint/burn delta. CoinGecko stablecoin supply."""
    return _build_tether_mints()

@leading_router.get("/breakevens")
def get_breakevens():
    """TIPS breakeven inflation rates (5Y, 10Y, 5Y5Y forward). FRED."""
    return _build_breakevens()

@leading_router.get("/basis-enhanced")
def get_basis_enhanced():
    """CME BTC basis with 5-day trend and DTX normalization. yFinance."""
    return _build_basis_enhanced()

@leading_router.get("/all")
def get_all_leading():
    """
    All 8 leading indicators in one call. Each indicator fetches independently
    and returns a null-safe result even if its source is down.
    """
    return {
        "updated_at":          datetime.utcnow().isoformat() + "Z",
        "options":             _build_options(),
        "coinbase_premium":    _build_coinbase_premium(),
        "funding_cumulative":  _build_funding_cumulative(),
        "global_m2":           _build_global_m2(),
        "cot":                 _build_cot(),
        "tether_mints":        _build_tether_mints(),
        "breakevens":          _build_breakevens(),
        "basis_enhanced":      _build_basis_enhanced(),
    }

@leading_router.get("/cache/flush")
def flush_leading_cache():
    """Force all leading indicator caches to refresh on next request."""
    global _options_cache, _premium_cache, _funding_cache, _m2_cache
    global _cot_cache, _tether_cache, _breakeven_cache, _basis_enh_cache
    _options_cache = _premium_cache = _funding_cache = _m2_cache = {"data": None, "ts": 0.0}
    _cot_cache = _tether_cache = _breakeven_cache = _basis_enh_cache = {"data": None, "ts": 0.0}
    return {"flushed": True, "caches": [
        "options", "coinbase_premium", "funding_cumulative", "global_m2",
        "cot", "tether_mints", "breakevens", "basis_enhanced"
    ]}

@leading_router.get("/history/{indicator}")
def get_leading_history(indicator: str, days: int = 90):
    """
    Historical snapshots for any leading indicator.
    indicator: options | coinbase_premium | funding | global_m2 | cot | tether | breakevens | basis
    """
    table_map = {
        "options":         ("options_history",            ["date", "iv_7d", "iv_30d", "term_spread", "risk_reversal_25d"]),
        "coinbase_premium":("coinbase_premium_history",   ["date", "premium_pct", "avg_24h"]),
        "funding":         ("funding_cumulative_history", ["date", "daily_rate", "cumulative_7d", "cumulative_30d"]),
        "global_m2":       ("global_m2_history",          ["date", "global_m2_usd", "mom_growth", "yoy_growth"]),
        "cot":             ("cot_history",                 ["date", "lev_long", "lev_short", "lev_net", "lev_net_pct"]),
        "tether":          ("tether_mint_history",         ["date", "usdt_supply", "daily_change", "large_mint_flag"]),
        "breakevens":      ("breakeven_history",           ["date", "be_5y", "be_10y", "be_5y5y"]),
        "basis":           ("basis_enhanced_history",      ["date", "annualized", "trend_5d"]),
    }
    if indicator not in table_map:
        return {"error": f"Unknown indicator '{indicator}'. Valid: {list(table_map.keys())}"}
    table, cols = table_map[indicator]
    rows = _query_history(table, cols, days)
    return {"indicator": indicator, "rows": rows, "count": len(rows)}
