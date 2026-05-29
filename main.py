from __future__ import annotations
from typing import Optional
import time
import threading
import json
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd

from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from macro_routes import macro_router
from contextlib import asynccontextmanager


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
from oi_history import (
    init_db, store_snapshot, get_snapshots,
    get_snapshot_count, prune_old_snapshots,
)
from manual_history import (
    init_db as init_history_db,
    upsert_metric,
    get_history,
    get_entry,
    get_all_dates,
    get_summary_stats,
    get_row_count,
)
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ─── App ───────────────────────────────────────────────────────────────────

# lifespan goes here, before app = FastAPI()
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        from macro_routes import _build_macro_metrics
        _build_macro_metrics()
    except Exception:
        pass
    yield

# then app uses it
app = FastAPI(title="BTC Decision Dashboard API")
app = FastAPI(lifespan=lifespan)

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

app.include_router(macro_router)
# ─── CME Basis — SQLite history ────────────────────────────────────────────

DB_PATH = DATA_DIR / "basis_history.db"


def init_basis_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cme_basis (
                date        TEXT PRIMARY KEY,
                annualized  REAL,
                raw_basis   REAL,
                futures_px  REAL,
                spot_px     REAL,
                days_expiry INTEGER
            )
        """)
        conn.commit()


def store_basis_snapshot(
    annualized: float, raw_basis: float,
    futures_px: float, spot_px: float, days_expiry: int,
):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO cme_basis
                (date, annualized, raw_basis, futures_px, spot_px, days_expiry)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (today, annualized, raw_basis, futures_px, spot_px, days_expiry),
        )
        conn.commit()


def query_basis_history(days: int) -> list:
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT date, annualized FROM cme_basis ORDER BY date DESC LIMIT ?",
            (days,),
        ).fetchall()


init_basis_db()

STABLECOIN_DB_PATH = DATA_DIR / "stablecoin_history.db"

def init_stablecoin_db():
    with sqlite3.connect(STABLECOIN_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stablecoin_supply (
                date         TEXT PRIMARY KEY,
                usdt_supply  REAL,
                usdc_supply  REAL,
                total_supply REAL
            )
        """)
        conn.commit()

def store_stablecoin_snapshot(usdt: float, usdc: float):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total = usdt + usdc
    with sqlite3.connect(STABLECOIN_DB_PATH) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO stablecoin_supply
               (date, usdt_supply, usdc_supply, total_supply)
               VALUES (?, ?, ?, ?)""",
            (today, usdt, usdc, total),
        )
        conn.commit()

def query_stablecoin_history(days: int) -> list:
    with sqlite3.connect(STABLECOIN_DB_PATH) as conn:
        return conn.execute(
            """SELECT date, usdt_supply, usdc_supply, total_supply
               FROM stablecoin_supply ORDER BY date DESC LIMIT ?""",
            (days,),
        ).fetchall()

init_stablecoin_db()

DOMINANCE_DB_PATH = DATA_DIR / "btc_dominance_history.db"

def init_dominance_db():
    with sqlite3.connect(DOMINANCE_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS btc_dominance (
                date             TEXT PRIMARY KEY,
                dominance_pct    REAL,
                btc_market_cap   REAL,
                total_market_cap REAL
            )
        """)
        conn.commit()

def store_dominance_snapshot(dominance_pct: float, btc_cap: float, total_cap: float):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with sqlite3.connect(DOMINANCE_DB_PATH) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO btc_dominance
               (date, dominance_pct, btc_market_cap, total_market_cap)
               VALUES (?, ?, ?, ?)""",
            (today, dominance_pct, btc_cap, total_cap),
        )
        conn.commit()

def query_dominance_history(days: int) -> list:
    with sqlite3.connect(DOMINANCE_DB_PATH) as conn:
        return conn.execute(
            """SELECT date, dominance_pct, btc_market_cap, total_market_cap
               FROM btc_dominance ORDER BY date DESC LIMIT ?""",
            (days,),
        ).fetchall()

init_dominance_db()

# ─── CME Basis — fetch & format ────────────────────────────────────────────

def fetch_cme_basis() -> dict:
    try:
        import yfinance as yf

        fut = yf.Ticker("BTC=F")
        spot = yf.Ticker("BTC-USD")

        fut_info = fut.info
        spot_info = spot.info

        futures_px = fut_info.get("regularMarketPrice") or fut_info.get("previousClose")
        spot_px = spot_info.get("regularMarketPrice") or spot_info.get("previousClose")

        if not futures_px or not spot_px:
            return {"cme_basis": {"error": "price unavailable"}}

        expiry_ts = fut_info.get("expireDate")
        if expiry_ts:
            expiry_dt = datetime.fromtimestamp(expiry_ts, tz=timezone.utc)
            days_to_exp = max((expiry_dt - datetime.now(timezone.utc)).days, 1)
            annualized = ((futures_px / spot_px) - 1) * (365 / days_to_exp) * 100
        else:
            days_to_exp = 30
            annualized = ((futures_px / spot_px) - 1) * 12 * 100

        raw_basis = ((futures_px / spot_px) - 1) * 100

        return {
            "cme_basis": {
                "futures_px": futures_px,
                "spot_px": spot_px,
                "raw_basis": raw_basis,
                "annualized": annualized,
                "days_to_exp": days_to_exp,
            }
        }
    except Exception as e:
        return {"cme_basis": {"error": str(e)}}


def format_cme_basis(
    annualized: float, raw_basis: float,
    futures_px: float, spot_px: float,
    days_to_exp: int, **kwargs,
) -> dict:
    store_basis_snapshot(annualized, raw_basis, futures_px, spot_px, days_to_exp)

    # 7d change
    hist_7 = query_basis_history(7)
    if len(hist_7) >= 2:
        oldest = hist_7[-1][1]
        d7_delta = annualized - oldest
        d7_str = f"{d7_delta:+.1f}% vs {len(hist_7) - 1}d ago"
    else:
        d7_str = "accumulating history"

    # vs 30d avg
    hist_30 = query_basis_history(30)
    if len(hist_30) >= 5:
        avg_30 = sum(r[1] for r in hist_30) / len(hist_30)
        vs30_str = f"{annualized - avg_30:+.1f}% vs {avg_30:.1f}% 30d avg"
    else:
        vs30_str = "accumulating history"

    # 90d percentile
    hist_90 = query_basis_history(90)
    values = [r[1] for r in hist_90]
    if len(values) >= 5:
        pctl = round(sum(1 for v in values if v < annualized) / len(values) * 100)
    else:
        pctl = 50

    # Sparkline
    spark = [r[1] for r in reversed(query_basis_history(10))]

    # Alert thresholds
    if annualized < 0:
        alert = "Backwardation — futures below spot"
        alert_level = "extreme"
        pattern = "Backwardation — historically rare bearish futures structure"
    elif annualized < 5:
        alert = "Basis compressed — carry trade unattractive"
        alert_level = "notable"
        pattern = "Low contango — limited institutional carry demand"
    elif annualized > 20:
        alert = "Extreme basis — cash/carry highly attractive"
        alert_level = "extreme"
        pattern = f"Extreme contango — {days_to_exp}d to expiry · {raw_basis:.2f}% raw premium"
    elif annualized > 15:
        alert = "Elevated basis — above normal carry premium"
        alert_level = "notable"
        pattern = f"Strong contango — {days_to_exp}d to expiry · {raw_basis:.2f}% raw premium"
    else:
        alert = "—"
        alert_level = "none"
        pattern = f"Normal contango — {days_to_exp}d to expiry · {raw_basis:.2f}% raw premium"

    return {
        "name": "CME Basis (Annualized)",
        "category": "Derivatives · Cash & Carry",
        "current": f"{annualized:+.2f}%",
        "current_dir": "up" if annualized > 12 else "down" if annualized < 5 else "flat",
        "d7": d7_str,
        "vs30d": vs30_str,
        "percentile": pctl,
        "alert": alert,
        "alert_level": alert_level,
        "pattern": pattern,
        "spark": spark,
        "futures_px": round(futures_px, 2),
        "spot_px": round(spot_px, 2),
        "raw_basis": round(raw_basis, 4),
        "days_to_exp": days_to_exp,
    }


# ─── OI Polling ────────────────────────────────────────────────────────────

def _poll_oi() -> None:
    INTERVAL = 240 * 60
    print("[oi_poller] Starting OI polling job — interval 15 minutes")
    while True:
        try:
            markets = _fetch_coingecko_derivatives()
            if markets:
                total_oi = sum(m["open_interest"] for m in markets)
                store_snapshot(total_oi)
                count = get_snapshot_count()
                print(f"[oi_poller] Stored OI snapshot: ${total_oi / 1e9:.1f}B — {count} total snapshots")
                prune_old_snapshots(keep_days=90)
        except Exception as e:
            print(f"[oi_poller] Error: {e}")
        time.sleep(INTERVAL)


init_db()
init_history_db()
threading.Thread(target=_poll_oi, daemon=True).start()

def fetch_stablecoin_supply() -> dict:
    try:
        data = _cached_get(
            f"{COINGECKO_BASE}/simple/price",
            _coingecko_headers(),
            {
                "ids":               "tether,usd-coin",
                "vs_currencies":     "usd",
                "include_market_cap": "true",
            },
        )
        if not data:
            return {"stablecoin_supply": None}

        usdt = data.get("tether",   {}).get("usd_market_cap", 0)
        usdc = data.get("usd-coin", {}).get("usd_market_cap", 0)

        if not usdt and not usdc:
            return {"stablecoin_supply": None}

        return {"stablecoin_supply": {"usdt": usdt, "usdc": usdc}}
    except Exception as e:
        print(f"[stablecoin] fetch error: {e}")
        return {"stablecoin_supply": None}

def _fmt_billions(v: float) -> str:
    if v >= 1_000_000_000_000:
        return f"${v / 1_000_000_000_000:.2f}T"
    return f"${v / 1_000_000_000:.1f}B"

def format_stablecoin_supply(usdt: float, usdc: float, **kwargs) -> dict:
    total = usdt + usdc
    store_stablecoin_snapshot(usdt, usdc)

    # History
    hist_7  = query_stablecoin_history(7)
    hist_30 = query_stablecoin_history(30)
    hist_90 = query_stablecoin_history(90)

    # 7d change
    if len(hist_7) >= 2:
        oldest_total = hist_7[-1][3]
        d7_delta     = total - oldest_total
        d7_pct       = (d7_delta / oldest_total * 100) if oldest_total else 0
        d7_str       = f"{_fmt_billions(abs(d7_delta))} ({d7_pct:+.1f}%) vs {len(hist_7)-1}d ago"
        d7_str       = ("+" if d7_delta >= 0 else "-") + d7_str
    else:
        d7_delta = 0
        d7_pct   = 0
        d7_str   = "accumulating history"

    # vs 30d avg
    if len(hist_30) >= 5:
        avg_30   = sum(r[3] for r in hist_30) / len(hist_30)
        vs30_delta = total - avg_30
        vs30_pct   = (vs30_delta / avg_30 * 100) if avg_30 else 0
        vs30_str   = f"{vs30_pct:+.1f}% vs {_fmt_billions(avg_30)} 30d avg"
    else:
        avg_30   = total
        vs30_str = "accumulating history"

    # 90d percentile
    totals_90 = [r[3] for r in hist_90]
    if len(totals_90) >= 5:
        pctl = round(sum(1 for v in totals_90 if v < total) / len(totals_90) * 100)
    else:
        pctl = 50

    # Sparkline (last 10, chronological)
    spark = [r[3] / 1e9 for r in reversed(query_stablecoin_history(10))]

    # Individual 7d changes
    if len(hist_7) >= 2:
        usdt_7d_delta = usdt - hist_7[-1][1]
        usdc_7d_delta = usdc - hist_7[-1][2]
        usdt_7d_str   = f"{'+' if usdt_7d_delta >= 0 else ''}{_fmt_billions(abs(usdt_7d_delta))}"
        usdc_7d_str   = f"{'+' if usdc_7d_delta >= 0 else ''}{_fmt_billions(abs(usdc_7d_delta))}"
    else:
        usdt_7d_str = "—"
        usdc_7d_str = "—"

    # Alert thresholds (based on 7d % change)
    if d7_pct > 10:
        alert       = "Rapid liquidity expansion — strong capital staging"
        alert_level = "extreme"
        pattern     = f"7d +{d7_pct:.1f}% — aggressive stablecoin minting, capital entering crypto"
    elif d7_pct > 5:
        alert       = "Liquidity expanding — capital staging into crypto"
        alert_level = "notable"
        pattern     = f"7d +{d7_pct:.1f}% — above-normal stablecoin growth, bullish liquidity backdrop"
    elif d7_pct < -10:
        alert       = "Rapid liquidity contraction — capital exiting or deploying"
        alert_level = "extreme"
        pattern     = f"7d {d7_pct:.1f}% — large stablecoin burn, capital rotating out or into BTC"
    elif d7_pct < -5:
        alert       = "Liquidity contracting — deployment or outflow signal"
        alert_level = "notable"
        pattern     = f"7d {d7_pct:.1f}% — stablecoin supply shrinking, watch for direction"
    elif pctl >= 90:
        alert       = "Supply at 90d high — peak dry powder"
        alert_level = "notable"
        pattern     = "Maximum liquidity available — historically precedes deployment into risk assets"
    else:
        alert       = "—"
        alert_level = "none"
        pattern     = "Stable supply — neutral liquidity backdrop"

    usdt_share = round(usdt / total * 100, 1) if total else 0
    usdc_share = round(usdc / total * 100, 1) if total else 0

    return {
        "name":        "Stablecoin Supply",
        "category":    "Liquidity · USDT + USDC",
        "current":     _fmt_billions(total),
        "current_dir": "up" if d7_delta > 0 else "down" if d7_delta < 0 else "flat",
        "d7":          d7_str,
        "vs30d":       vs30_str,
        "percentile":  pctl,
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     pattern,
        "spark":       spark,
        # Breakdown fields for frontend card
        "usdt":        _fmt_billions(usdt),
        "usdc":        _fmt_billions(usdc),
        "usdt_raw":    usdt,
        "usdc_raw":    usdc,
        "usdt_share":  usdt_share,
        "usdc_share":  usdc_share,
        "usdt_7d":     usdt_7d_str,
        "usdc_7d":     usdc_7d_str,
    }

def fetch_btc_dominance() -> dict:
    try:
        data = _cached_get(
            f"{COINGECKO_BASE}/global",
            _coingecko_headers(),
            {},
        )
        if not data or "data" not in data:
            return {"btc_dominance": None}

        gd              = data["data"]
        dominance_pct   = gd.get("market_cap_percentage", {}).get("btc", 0)
        total_cap       = gd.get("total_market_cap", {}).get("usd", 0)
        btc_cap         = total_cap * (dominance_pct / 100) if total_cap else 0

        return {"btc_dominance": {
            "dominance_pct":   dominance_pct,
            "btc_market_cap":  btc_cap,
            "total_market_cap": total_cap,
        }}
    except Exception as e:
        print(f"[btc_dominance] fetch error: {e}")
        return {"btc_dominance": None}

def format_btc_dominance(dominance_pct: float, btc_market_cap: float, total_market_cap: float, **kwargs) -> dict:
    store_dominance_snapshot(dominance_pct, btc_market_cap, total_market_cap)

    alt_cap     = total_market_cap - btc_market_cap
    alt_share   = round(100 - dominance_pct, 1)
    btc_share   = round(dominance_pct, 1)

    # 7d change
    hist_7 = query_dominance_history(7)
    if len(hist_7) >= 2:
        oldest   = hist_7[-1][1]
        d7_delta = dominance_pct - oldest
        d7_str   = f"{d7_delta:+.2f}pp vs {len(hist_7)-1}d ago"
    else:
        d7_delta = 0
        d7_str   = "accumulating history"

    # vs 30d avg
    hist_30 = query_dominance_history(30)
    if len(hist_30) >= 5:
        avg_30   = sum(r[1] for r in hist_30) / len(hist_30)
        vs30_str = f"{dominance_pct - avg_30:+.2f}pp vs {avg_30:.1f}% avg"
    else:
        avg_30   = dominance_pct
        vs30_str = "accumulating history"

    # 90d percentile
    hist_90 = query_dominance_history(90)
    values  = [r[1] for r in hist_90]
    if len(values) >= 5:
        pctl = round(sum(1 for v in values if v < dominance_pct) / len(values) * 100)
    else:
        pctl = 50

    # Sparkline
    spark = [r[1] for r in reversed(query_dominance_history(10))]

    # Alert thresholds
    if dominance_pct >= 70:
        alert       = "Extreme dominance — capital consolidating in BTC"
        alert_level = "extreme"
        pattern     = f"{dominance_pct:.1f}% — historically precedes major altcoin distribution or BTC local top"
    elif dominance_pct >= 60:
        alert       = "Elevated dominance — altcoin season unlikely near-term"
        alert_level = "notable"
        pattern     = f"{dominance_pct:.1f}% — BTC capturing majority of new capital inflow"
    elif dominance_pct <= 40:
        alert       = "Extreme low dominance — peak altcoin season conditions"
        alert_level = "extreme"
        pattern     = f"{dominance_pct:.1f}% — capital heavily rotated into alts, BTC lagging"
    elif dominance_pct <= 50:
        alert       = "Low dominance — altcoin season conditions forming"
        alert_level = "notable"
        pattern     = f"{dominance_pct:.1f}% — capital rotating from BTC to altcoins"
    else:
        alert       = "—"
        alert_level = "none"
        pattern     = f"{dominance_pct:.1f}% — neutral zone, no strong rotation signal"

    # Rising/falling direction
    if d7_delta > 0.5:
        dir_ = "up"
    elif d7_delta < -0.5:
        dir_ = "down"
    else:
        dir_ = "flat"

    return {
        "name":            "BTC Dominance",
        "category":        "Market Structure · USD",
        "current":         f"{dominance_pct:.2f}%",
        "current_dir":     dir_,
        "d7":              d7_str,
        "vs30d":           vs30_str,
        "percentile":      pctl,
        "alert":           alert,
        "alert_level":     alert_level,
        "pattern":         pattern,
        "spark":           spark,
        "btc_cap":         _fmt_billions(btc_market_cap),
        "alt_cap":         _fmt_billions(alt_cap),
        "total_cap":       _fmt_billions(total_market_cap),
        "btc_share":       btc_share,
        "alt_share":       alt_share,
        "dominance_pct":   round(dominance_pct, 2),
    }

# ─── Crypto Proxy Stocks ───────────────────────────────────────────────────

PROXY_TICKERS = {
    "MSTR": "Strategy",
    "COIN": "Coinbase",
    "HOOD": "Robinhood",
    "XYZ":  "Block",
    "PYPL": "PayPal",
}

_proxy_cache: dict = {"data": None, "ts": 0.0}
PROXY_CACHE_TTL   = 300  # 5 minutes — yfinance calls are slow
_metrics_cache: dict = {"data": None, "ts": 0.0}
METRICS_CACHE_TTL = 60  # seconds


def _pearson(a: np.ndarray, b: np.ndarray) -> float:
    n = min(len(a), len(b))
    if n < 3:
        return 0.0
    try:
        return float(np.corrcoef(a[-n:], b[-n:])[0, 1])
    except Exception:
        return 0.0


def _cross_corr_lag(stock: np.ndarray, btc: np.ndarray, max_lag: int = 5) -> tuple[int, float]:
    """Returns (best_lag, best_corr). Positive lag = stock lags BTC."""
    best_lag, best_corr = 0, _pearson(stock, btc)
    for lag in range(-max_lag, max_lag + 1):
        if lag == 0:
            continue
        if lag > 0:
            s, b = stock[lag:], btc[:-lag]
        else:
            s, b = stock[:lag], btc[-lag:]
        c = _pearson(s, b)
        if abs(c) > abs(best_corr):
            best_corr, best_lag = c, lag
    return best_lag, best_corr


def fetch_crypto_proxies() -> dict:
    now = time.time()
    if _proxy_cache["data"] and now - _proxy_cache["ts"] < PROXY_CACHE_TTL:
        return _proxy_cache["data"]

    try:
        import yfinance as yf

        # Fetch closes individually — more reliable than multi-ticker download
        closes: dict[str, pd.Series] = {}
        all_tickers = list(PROXY_TICKERS.keys()) + ["BTC-USD"]

        for ticker in all_tickers:
            hist = yf.Ticker(ticker).history(period="6mo", interval="1d")
            if not hist.empty:
                closes[ticker] = hist["Close"]

        if "BTC-USD" not in closes:
            return {"crypto_proxies": None}

        btc_close   = closes["BTC-USD"]
        btc_returns = btc_close.pct_change().dropna().values

        results = {}
        for ticker, name in PROXY_TICKERS.items():
            if ticker not in closes:
                continue

            sc  = closes[ticker]
            ret = sc.pct_change().dropna().values

            # Price change
            p0      = float(sc.iloc[-1])
            p1d     = float(sc.iloc[-2])  if len(sc) >= 2  else p0
            p7d     = float(sc.iloc[-8])  if len(sc) >= 8  else float(sc.iloc[0])
            ch_1d   = (p0 / p1d  - 1) * 100
            ch_7d   = (p0 / p7d  - 1) * 100

            # Correlations
            corr_7d  = _pearson(ret[-7:],  btc_returns[-7:])
            corr_30d = _pearson(ret[-30:], btc_returns[-30:])
            corr_90d = _pearson(ret[-90:], btc_returns[-90:])

            # Lead / lag (based on 30d window)
            lag, _ = _cross_corr_lag(ret[-30:], btc_returns[-30:])

            if abs(lag) <= 1:
                lead_lag_label = "Lockstep"
            elif lag > 0:
                lead_lag_label = f"Lags BTC ~{lag}d"
            else:
                lead_lag_label = f"Leads BTC ~{abs(lag)}d"

            # Regime label
            c = abs(corr_30d)
            regime = (
                "Lockstep"      if c >= 0.80 else
                "Strong"        if c >= 0.65 else
                "Moderate"      if c >= 0.45 else
                "Weak"          if c >= 0.20 else
                "Decorrelated"
            )

            # Sparkline — last 30 days, normalised to start = 0
            recent = sc.iloc[-30:].values
            base   = recent[0] if recent[0] != 0 else 1
            spark  = [round(float(v / base * 100 - 100), 2) for v in recent]

            results[ticker] = {
                "ticker":          ticker,
                "name":            name,
                "price":           f"${p0:,.2f}",
                "change_1d":       f"{ch_1d:+.2f}%",
                "change_7d":       f"{ch_7d:+.2f}%",
                "change_1d_raw":   round(ch_1d, 2),
                "change_7d_raw":   round(ch_7d, 2),
                "corr_7d":         round(corr_7d,  3),
                "corr_30d":        round(corr_30d, 3),
                "corr_90d":        round(corr_90d, 3),
                "lead_lag_label":  lead_lag_label,
                "lead_lag_days":   lag,
                "regime":          regime,
                "spark":           spark,
            }

        result = {"crypto_proxies": results}
        _proxy_cache["data"] = result
        _proxy_cache["ts"]   = now
        return result

    except Exception as e:
        print(f"[crypto_proxies] fetch error: {e}")
        return {"crypto_proxies": None}

# ─── Mock fallbacks ────────────────────────────────────────────────────────

MOCK = {
    "etf_flow":         dict(current_daily=450_000_000, last_7d_sum=2_100_000_000, avg_30d=1_135_000_000, percentile_90d=88),
    "funding":          dict(current_rate=0.00035, avg_7d=0.00021, avg_30d=0.000126, percentile_90d=92),
    "open_interest":    dict(current_usd=12_400_000_000, growth_7d_pct=0.18, growth_30d_pct=0.25, percentile_90d=85),
    "exchange_netflow": dict(current_btc=-12_000, sum_7d_btc=-28_000, avg_30d_btc=16_500, percentile_90d=20),
    "volume":           dict(ratio_30d=1.6, ratio_7d=1.2, percentile_90d=87, price_change_pct=0.052),
    "price_move":       dict(daily_change_pct=0.052, week_change_pct=0.088, avg_daily_30d=0.031, percentile_90d=80),
    "realized_cap":     dict(growth_pct=0.028, growth_7d_pct=0.019, avg_30d_pct=0.006, percentile_90d=76),
    "lth_supply":       dict(change_7d_btc=45_000, change_30d_btc=120_000, change_30d_pct=0.008, percentile_90d=72),
    "cme_basis":        dict(annualized=8.5, raw_basis=0.35, futures_px=95000.0, spot_px=94667.0, days_to_exp=30),
    "stablecoin_supply": dict(usdt=143_000_000_000, usdc=60_000_000_000),
    "btc_dominance": dict(dominance_pct=62.5, btc_market_cap=1_850_000_000_000, total_market_cap=2_960_000_000_000),
}


def get(live, key):
    if live is not None:
        print(f"[metrics] {key}: LIVE")
        return live
    print(f"[metrics] {key}: MOCK fallback")
    return MOCK[key]

from datetime import datetime, timezone, timedelta

EST = timezone(timedelta(hours=-5))  # use -4 for EDT if you want to follow DST

def _last_10am_est() -> datetime:
    """Returns the most recent 10AM EST as a UTC-aware datetime."""
    now_est = datetime.now(EST)
    today_10am = now_est.replace(hour=10, minute=0, second=0, microsecond=0)
    if now_est < today_10am:
        # haven't hit 10am today yet — last refresh was yesterday at 10am
        return today_10am - timedelta(days=1)
    return today_10am

def _cache_is_stale(cache: dict) -> bool:
    if not cache["data"]:
        return True
    last_refresh = _last_10am_est()
    cache_time = datetime.fromtimestamp(cache["ts"], tz=timezone.utc)
    return cache_time < last_refresh


# ─── Override helpers ──────────────────────────────────────────────────────

OVERRIDE_FILE = "manual_overrides.json"

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
        "cme_basis":        "CME Basis (Annualized)",
        "stablecoin_supply": "Stablecoin Supply",   # display name
        "btc_dominance": "BTC Dominance",
    }.get(metric, metric)


def _metric_category(metric: str) -> str:
    return {
        "exchange_netflow": "On-chain",
        "lth_supply":       "On-chain",
        "etf_flow":         "Flow",
        "realized_cap":     "On-chain",
        "funding":          "Derivatives",
        "open_interest":    "Derivatives",
        "cme_basis":        "Derivatives · Cash & Carry",
        "stablecoin_supply": "Liquidity",  
        "btc_dominance": "Market Structure",# category
    }.get(metric, "—")


def _infer_direction(current: str) -> str:
    if not current:
        return "flat"
    stripped = current.replace(",", "").replace(" ", "")
    if stripped.startswith("+"):
        return "up"
    if stripped.startswith("-"):
        return "down"
    return "flat"


def _apply_overrides(metrics: dict) -> dict:
    """Merge manual overrides into a metrics dict in-place."""
    for key, override in _load_overrides().items():
        if key in metrics:
            metrics[key] = {
                **metrics[key],
                "alert":       override.get("alert",       metrics[key].get("alert")),
                "alert_level": override.get("alert_level", metrics[key].get("alert_level")),
                "pattern":     override.get("pattern",     metrics[key].get("pattern")),
                "current":     override.get("current",     metrics[key].get("current")),
                "d7":          override.get("d7",          metrics[key].get("d7")),
                "vs30d":       override.get("vs30d",       metrics[key].get("vs30d")),
                "percentile":  override.get("percentile",  metrics[key].get("percentile")),
            }
    return metrics


# ─── Shared metric builder ─────────────────────────────────────────────────

def _build_metrics(cg: dict) -> dict:
    """Fetch and format all metrics. Used by /metrics, /summary, /causal."""
   # netflow_raw           = fetch_exchange_netflow()
    netflow_raw           = None
    realized_raw          = None
    cme_raw               = fetch_cme_basis().get("cme_basis")
   # realized_raw          = fetch_realized_cap(chart=cg["chart"])
    funding_raw           = fetch_funding(markets=cg["derivatives"])
    oi_raw                = fetch_open_interest(markets=cg["derivatives"])
    etf_raw               = fetch_etf_flow()
    lth_raw               = fetch_lth_supply()
    price_raw, volume_raw = fetch_price_and_volume(
        chart=cg["chart"], ohlcv=cg["ohlcv"])
    stablecoin_raw = fetch_stablecoin_supply().get("stablecoin_supply")
    dominance_raw = fetch_btc_dominance().get("btc_dominance")

    return {
        "etf_flow":         format_etf_flow(**get(etf_raw,      "etf_flow")),
        "funding":          format_funding(**get(funding_raw,    "funding")),
        "open_interest":    format_open_interest(**get(oi_raw,   "open_interest")),
        "exchange_netflow": format_exchange_netflow(**get(netflow_raw, "exchange_netflow")),
        "volume":           format_volume(**get(volume_raw,      "volume")),
        "price_move":       format_price_move(**get(price_raw,   "price_move")),
        "realized_cap":     format_realized_cap(**get(realized_raw, "realized_cap")),
        "lth_supply":       format_lth_supply(**get(lth_raw,    "lth_supply")),
        "cme_basis":        format_cme_basis(**get(cme_raw,     "cme_basis")),
        "stablecoin_supply": format_stablecoin_supply(**get(stablecoin_raw, "stablecoin_supply")),
        "btc_dominance": format_btc_dominance(**get(dominance_raw, "btc_dominance")),
    }
def _build_metrics_cached(cg: dict) -> dict:
    now = time.time()
    if not _cache_is_stale(_metrics_cache):
        return _metrics_cache["data"]
    result = _build_metrics(cg)
    _metrics_cache["data"] = result
    _metrics_cache["ts"] = now
    return result

# ─── Routes ────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"service": "btc-dashboard-api", "status": "ok"}


@app.get("/metrics")
def get_metrics():
    cg = get_shared_coingecko()
    overrides = _load_overrides()
    metrics = _build_metrics_cached(cg)

    def resolve(key):
        if key in overrides:
            return {**overrides[key], "_is_override": True}
        return metrics[key]

    return {key: resolve(key) for key in metrics}

@app.get("/metrics/history")
def get_metrics_history(date: str):
    """
    Returns metric snapshot for a specific date from all SQLite stores.
    Covers: manual history, CME basis, stablecoin supply, BTC dominance.
    """
    result = {}

    # ── 1. Manual history (netflow, LTH, ETF, realized cap, funding, OI) ──
    for metric in ["exchange_netflow", "lth_supply", "etf_flow",
                   "realized_cap", "funding", "open_interest"]:
        entry = get_entry(metric, date)
        if entry:
            result[metric] = {
                "name":           _metric_display_name(metric),
                "category":       _metric_category(metric),
                "current":        entry.get("current", "—"),
                "current_dir":    _infer_direction(entry.get("current", "")),
                "d7":             entry.get("d7", "—"),
                "vs30d":          entry.get("vs30d", "—"),
                "percentile":     entry.get("percentile", 0),
                "alert":          entry.get("alert", "—"),
                "alert_level":    _classify_alert_level(entry.get("alert", "—")),
                "pattern":        entry.get("pattern", "—"),
                "source":         entry.get("source", "—"),
                "spark":          [],
                "_is_historical": True,
                "_date":          date,
            }

    # ── 2. CME Basis ────────────────────────────────────────────────────────
    # Schema: date, annualized, raw_basis, futures_px, spot_px, days_expiry
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT * FROM cme_basis WHERE date = ?", (date,)
        ).fetchone()
    if row:
        annualized, raw_basis, futures_px, spot_px, days_exp = row[1], row[2], row[3], row[4], row[5]
        result["cme_basis"] = {
            "name":           "CME Basis (Annualized)",
            "category":       "Derivatives · Cash & Carry",
            "current":        f"{annualized:+.2f}%",
            "current_dir":    "up" if annualized > 12 else "down" if annualized < 5 else "flat",
            "d7":             "—",
            "vs30d":          "—",
            "percentile":     0,
            "alert":          "—",
            "alert_level":    "none",
            "pattern":        f"{days_exp}d to expiry · {raw_basis:.2f}% raw premium",
            "spark":          [],
            "futures_px":     round(futures_px, 2),
            "spot_px":        round(spot_px, 2),
            "raw_basis":      round(raw_basis, 4),
            "days_to_exp":    days_exp,
            "_is_historical": True,
            "_date":          date,
        }

    # ── 3. Stablecoin Supply ────────────────────────────────────────────────
    # Schema: date, usdt_supply, usdc_supply, total_supply
    with sqlite3.connect(STABLECOIN_DB_PATH) as conn:
        row = conn.execute(
            "SELECT * FROM stablecoin_supply WHERE date = ?", (date,)
        ).fetchone()
    if row:
        usdt, usdc, total = row[1], row[2], row[3]
        result["stablecoin_supply"] = {
            "name":           "Stablecoin Supply",
            "category":       "Liquidity · USDT + USDC",
            "current":        _fmt_billions(total),
            "current_dir":    "flat",
            "d7":             "—",
            "vs30d":          "—",
            "percentile":     0,
            "alert":          "—",
            "alert_level":    "none",
            "pattern":        f"USDT {_fmt_billions(usdt)} · USDC {_fmt_billions(usdc)}",
            "spark":          [],
            "usdt":           _fmt_billions(usdt),
            "usdc":           _fmt_billions(usdc),
            "usdt_share":     round(usdt / total * 100, 1) if total else 0,
            "usdc_share":     round(usdc / total * 100, 1) if total else 0,
            "usdt_7d":        "—",
            "usdc_7d":        "—",
            "_is_historical": True,
            "_date":          date,
        }

    # ── 4. BTC Dominance ────────────────────────────────────────────────────
    # Schema: date, dominance_pct, btc_market_cap, total_market_cap
    with sqlite3.connect(DOMINANCE_DB_PATH) as conn:
        row = conn.execute(
            "SELECT * FROM btc_dominance WHERE date = ?", (date,)
        ).fetchone()
    if row:
        dom, btc_c, tot_c = row[1], row[2], row[3]
        result["btc_dominance"] = {
            "name":           "BTC Dominance",
            "category":       "Market Structure · USD",
            "current":        f"{dom:.2f}%",
            "current_dir":    "flat",
            "d7":             "—",
            "vs30d":          "—",
            "percentile":     0,
            "alert":          "—",
            "alert_level":    "none",
            "pattern":        f"{dom:.1f}% of total crypto market cap",
            "spark":          [],
            "btc_cap":        _fmt_billions(btc_c),
            "alt_cap":        _fmt_billions(tot_c - btc_c),
            "total_cap":      _fmt_billions(tot_c),
            "btc_share":      round(dom, 1),
            "alt_share":      round(100 - dom, 1),
            "dominance_pct":  round(dom, 2),
            "_is_historical": True,
            "_date":          date,
        }

    if not result:
        return {
            "error":   f"No data found for {date}",
            "metrics": {},
            "count":   0,
            "date":    date,
        }

    return {
        "date":    date,
        "metrics": result,
        "count":   len(result),
    }


@app.get("/summary")
def get_summary():
    cg = get_shared_coingecko()
    metrics = _apply_overrides(_build_metrics_cached(cg))

    active_alerts = []
    for m in metrics.values():
        if m.get("alert") != "—" and m.get("alert_level") != "none":
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
    metrics = _apply_overrides(_build_metrics_cached(cg))

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
        {
            "label":  "CME basis (cash & carry)",
            "state":  derive_state(metrics["cme_basis"]),
            "weight": weight_from_level(metrics["cme_basis"]["alert_level"]),
        },
        {
            "label":  "Stablecoin liquidity (USDT + USDC)",
            "state":  derive_state(metrics["stablecoin_supply"]),
            "weight": weight_from_level(metrics["stablecoin_supply"]["alert_level"]),
        },
        {
            "label":  "BTC dominance",
            "state":  derive_state(metrics["btc_dominance"]),
            "weight": weight_from_level(metrics["btc_dominance"]["alert_level"]),
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
    cme_raw      = fetch_cme_basis()

    return {
        "exchange_netflow": "ok" if fetch_exchange_netflow() else "failed",
        "realized_cap":     "ok" if realized_raw             else "failed",
        "funding":          "ok" if funding_raw              else "failed",
        "open_interest":    "ok" if oi_raw                   else "failed",
        "etf_flow":         "ok" if fetch_etf_flow()         else "failed",
        "lth_supply":       "ok" if fetch_lth_supply()       else "failed",
        "price_move":       "ok" if price_raw                else "failed",
        "volume":           "ok" if volume_raw               else "failed",
        "cme_basis":        "ok" if "error" not in cme_raw.get("cme_basis", {}) else "failed",
    }


@app.get("/price")
def get_price():
    data = _cached_get(
        f"{COINGECKO_BASE}/simple/price",
        _coingecko_headers(),
        {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"},
    )
    if data and "bitcoin" in data:
        price      = data["bitcoin"]["usd"]
        change_24h = data["bitcoin"]["usd_24h_change"]
        return {"price": f"${price:,.0f}", "change_24h": f"{change_24h:+.2f}%"}
    return {"price": "—", "change_24h": "—"}


@app.get("/news")
def get_news():
    news = fetch_btc_news()
    if not news:
        return {"items": [{"title": "No recent BTC news found", "source": "—", "time": "—", "tag": "—", "url": "#"}]}
    return {"items": news}

@app.get("/crypto-proxies")
def get_crypto_proxies():
    """
    Returns price, 7d/30d/90d BTC correlation, and lead/lag
    for the 5 S&P 500 crypto-exposed stocks.
    Cached for 5 minutes — yfinance calls are slow.
    """
    result = fetch_crypto_proxies()
    if not result.get("crypto_proxies"):
        return {"error": "Could not fetch proxy stock data", "stocks": {}}
    return result


# ─── Contradiction engine ──────────────────────────────────────────────────

def _derive_contradiction(metrics: dict) -> str:
    funding_level  = metrics["funding"]["alert_level"]
    cap_level      = metrics["realized_cap"]["alert_level"]
    etf_level      = metrics["etf_flow"]["alert_level"]
    oi_level       = metrics["open_interest"]["alert_level"]
    volume_pattern = metrics["volume"].get("pattern", "—")
    funding_alert  = metrics["funding"].get("alert", "—").lower()
    basis_level    = metrics["cme_basis"]["alert_level"]
    basis_alert    = metrics["cme_basis"].get("alert", "—").lower()

    if "shorting" in funding_alert and cap_level in ("notable", "extreme"):
        return "Extreme short positioning against strong capital inflow — leverage and spot diverging."

    if "leverage" in funding_alert and cap_level == "none":
        return "Elevated leverage with no corresponding capital inflow — positioning appears speculative."

    if oi_level in ("notable", "extreme") and volume_pattern == "Absorption":
        return "Large open position base with absorption volume — significant supply being absorbed."

    if etf_level in ("notable", "extreme") and "leverage" in funding_alert:
        return "Institutional inflow (ETF) alongside elevated retail leverage — capital quality diverging."

    if basis_level == "extreme" and "leverage" in funding_alert:
        return "Extreme CME basis alongside elevated funding — institutional carry demand meeting retail leverage."

    if "backwardation" in basis_alert and etf_level in ("notable", "extreme"):
        return "Futures backwardation despite ETF inflow — unusual structure, spot demand not translating to futures premium."

    if oi_level in ("notable", "extreme") and volume_pattern == "Distribution":
        return "Large open positions with distribution volume — crowded trade showing supply pressure."

    active_signals = [m for m in metrics.values() if m.get("alert_level") in ("notable", "extreme")]
    if len(active_signals) >= 3:
        return "Multiple signals elevated simultaneously — broad market activation across metrics."
    if len(active_signals) == 0:
        return "No significant contradictions — market structure is neutral across monitored metrics."

    return "Monitor for developing contradictions as signals evolve."


# ─── Judgment Panel ────────────────────────────────────────────────────────

JUDGMENT_FILE = "judgment_log.json"


class JudgmentEntry(BaseModel):
    read:        str
    supports:    str
    contradicts: str
    invalidates: str
    plan:        str
    risk:        Optional[str] = None


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
    entries = _load_judgments()

    price_data = _cached_get(
        f"{COINGECKO_BASE}/simple/price",
        _coingecko_headers(),
        {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"},
    )
    btc_price = "—"
    if price_data and "bitcoin" in price_data:
        btc_price = f"${price_data['bitcoin']['usd']:,.0f}"

    new_entry = {
        **entry.dict(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "btc_price": btc_price,
        "id":        len(entries) + 1,
    }
    entries.append(new_entry)
    _save_judgments(entries)
    return {"status": "ok", "id": new_entry["id"], "timestamp": new_entry["timestamp"]}


@app.get("/judgment")
def get_judgments():
    return list(reversed(_load_judgments()))


@app.get("/judgment/{entry_id}")
def get_judgment(entry_id: int):
    for e in _load_judgments():
        if e.get("id") == entry_id:
            return e
    return {"error": "not found"}


# ─── Manual Override ───────────────────────────────────────────────────────

class MetricOverride(BaseModel):
    metric:        str
    current:       str
    d7:            str
    vs30d:         str
    percentile:    int
    alert:         str
    pattern:       str
    source:        Optional[str] = None
    baseline_date: Optional[str] = None
    notes:         Optional[str] = None


@app.post("/manual-override")
def set_manual_override(override: MetricOverride):
    if override.metric not in OVERRIDEABLE_METRICS:
        return {"error": f"Unknown metric '{override.metric}'. Valid: {sorted(OVERRIDEABLE_METRICS)}"}

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
    return {"status": "ok", "metric": override.metric, "updated": overrides[override.metric]["updated_at"]}


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


# ─── Trade Log ─────────────────────────────────────────────────────────────

TRADELOG_FILE = "trade_log.json"


class TradeLogEntry(BaseModel):
    structure:     str
    capital:       str
    read:          str
    contradiction: str
    plan:          str
    risk:          str
    result:        Optional[str] = None
    bias_flag:     Optional[str] = None


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
    entries = _load_trade_logs()

    price_data = _cached_get(
        f"{COINGECKO_BASE}/simple/price",
        _coingecko_headers(),
        {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"},
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
    return list(reversed(_load_trade_logs()))


@app.patch("/trade-log/{entry_id}")
def update_trade_log(entry_id: int, result: Optional[str] = None, bias_flag: Optional[str] = None):
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


# ─── Trade Execution ───────────────────────────────────────────────────────

EXECUTION_FILE = "trade_execution.json"


class TradeExecutionEntry(BaseModel):
    planned_entry:    float
    actual_entry:     float
    size_btc:         float
    max_drawdown_pct: float
    current_volume:   float
    market_state:     str


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
    entries = _load_executions()

    slippage           = round(entry.actual_entry - entry.planned_entry, 2)
    max_drawdown_price = round(entry.actual_entry * (1 - entry.max_drawdown_pct / 100), 2)
    vol_0_5x           = round(entry.current_volume * 0.5, 4)
    vol_1_5x           = round(entry.current_volume * 1.5, 4)
    vol_2_0x           = round(entry.current_volume * 2.0, 4)

    price_data = _cached_get(
        f"{COINGECKO_BASE}/simple/price",
        _coingecko_headers(),
        {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"},
    )
    btc_price = "—"
    if price_data and "bitcoin" in price_data:
        btc_price = f"${price_data['bitcoin']['usd']:,.0f}"

    new_entry = {
        "planned_entry":      entry.planned_entry,
        "actual_entry":       entry.actual_entry,
        "size_btc":           entry.size_btc,
        "max_drawdown_pct":   entry.max_drawdown_pct,
        "current_volume":     entry.current_volume,
        "market_state":       entry.market_state,
        "slippage":           slippage,
        "max_drawdown_price": max_drawdown_price,
        "vol_0_5x":           vol_0_5x,
        "vol_1_5x":           vol_1_5x,
        "vol_2_0x":           vol_2_0x,
        "btc_price_at_entry": btc_price,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "date":               datetime.now(timezone.utc).strftime("%b %d"),
        "id":                 len(entries) + 1,
    }
    entries.append(new_entry)
    _save_executions(entries)
    return {"status": "ok", "id": new_entry["id"], "computed": {
        "slippage": slippage, "max_drawdown_price": max_drawdown_price,
        "vol_0_5x": vol_0_5x, "vol_1_5x": vol_1_5x, "vol_2_0x": vol_2_0x,
    }}


@app.get("/trade-execution")
def get_trade_executions():
    return list(reversed(_load_executions()))


# ─── OI History ────────────────────────────────────────────────────────────

@app.get("/oi-history")
def get_oi_history():
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


# ─── Manual History ────────────────────────────────────────────────────────

class BackfillEntry(BaseModel):
    metric:     str
    date:       str
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
    days = min(days, 365)
    history = get_history(metric, days)
    return {"metric": metric, "count": len(history), "entries": history}


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
    for metric in ["exchange_netflow", "lth_supply", "etf_flow",
                   "realized_cap", "funding", "open_interest"]:
        summaries[metric] = get_summary_stats(metric)
    return {
        "total_rows": sum(counts.values()),
        "by_metric":  counts,
        "summaries":  summaries,
    }


@app.get("/db/summary")
def get_db_summary():
    """Quick overview of all historical data across every SQLite store."""
    summary = {}

    # Manual history (netflow, LTH, ETF, realized cap, funding, OI)
    try:
        counts = get_row_count()
        stats  = {}
        for metric in ["exchange_netflow", "lth_supply", "etf_flow",
                       "realized_cap", "funding", "open_interest"]:
            s = get_summary_stats(metric)
            stats[metric] = s
        summary["manual_history"] = {
            "row_counts": counts,
            "stats":      stats,
        }
    except Exception as e:
        summary["manual_history"] = {"error": str(e)}

    # CME Basis
    try:
        with sqlite3.connect(DB_PATH) as conn:
            count  = conn.execute("SELECT COUNT(*) FROM cme_basis").fetchone()[0]
            oldest = conn.execute("SELECT MIN(date) FROM cme_basis").fetchone()[0]
            newest = conn.execute("SELECT MAX(date) FROM cme_basis").fetchone()[0]
            dates  = [r[0] for r in conn.execute(
                "SELECT date FROM cme_basis ORDER BY date DESC LIMIT 10"
            ).fetchall()]
        summary["cme_basis"] = {"count": count, "oldest": oldest, "newest": newest, "recent_10": dates}
    except Exception as e:
        summary["cme_basis"] = {"error": str(e)}

    # Stablecoin
    try:
        with sqlite3.connect(STABLECOIN_DB_PATH) as conn:
            count  = conn.execute("SELECT COUNT(*) FROM stablecoin_supply").fetchone()[0]
            oldest = conn.execute("SELECT MIN(date) FROM stablecoin_supply").fetchone()[0]
            newest = conn.execute("SELECT MAX(date) FROM stablecoin_supply").fetchone()[0]
            dates  = [r[0] for r in conn.execute(
                "SELECT date FROM stablecoin_supply ORDER BY date DESC LIMIT 10"
            ).fetchall()]
        summary["stablecoin_supply"] = {"count": count, "oldest": oldest, "newest": newest, "recent_10": dates}
    except Exception as e:
        summary["stablecoin_supply"] = {"error": str(e)}

    # BTC Dominance
    try:
        with sqlite3.connect(DOMINANCE_DB_PATH) as conn:
            count  = conn.execute("SELECT COUNT(*) FROM btc_dominance").fetchone()[0]
            oldest = conn.execute("SELECT MIN(date) FROM btc_dominance").fetchone()[0]
            newest = conn.execute("SELECT MAX(date) FROM btc_dominance").fetchone()[0]
            dates  = [r[0] for r in conn.execute(
                "SELECT date FROM btc_dominance ORDER BY date DESC LIMIT 10"
            ).fetchall()]
        summary["btc_dominance"] = {"count": count, "oldest": oldest, "newest": newest, "recent_10": dates}
    except Exception as e:
        summary["btc_dominance"] = {"error": str(e)}

    # OI snapshots
    try:
        from oi_history import get_snapshot_count, get_snapshots
        summary["oi_history"] = {
            "count":    get_snapshot_count(),
            "recent_5": get_snapshots(days=1),
        }
    except Exception as e:
        summary["oi_history"] = {"error": str(e)}

    return summary

@app.get("/debug/funding")
def debug_funding():
    markets = _fetch_coingecko_derivatives()
    if not markets:
        return {"error": "no markets"}
    btc_perps = [
        m for m in markets
        if m.get("index_id") == "BTC"
        and m.get("contract_type") == "perpetual"
        and m.get("funding_rate") is not None
    ]
    return {
        "btc_perp_count": len(btc_perps),
        "sample": [{"market": m.get("market"), "symbol": m.get("symbol"), "funding_rate": m.get("funding_rate"), "oi": m.get("open_interest")} for m in btc_perps[:10]]
    }

@app.get("/cache/flush")
def flush_metrics_cache():
    global _metrics_cache
    _metrics_cache = {"data": None, "ts": 0.0}
    return {"flushed": True, "cache": "metrics"}
