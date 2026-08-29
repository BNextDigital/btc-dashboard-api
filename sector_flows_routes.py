"""
sector_flows_routes.py v3 — Comprehensive Sector Capital Flow Matrix

CHANGES FROM v2:
  - Migrated to shared yf_cache for all Close-price metrics (SMA, RS, percentile)
  - Separate focused OHLCV fetch for volume-based metrics (MFI, OBV, vol momentum)
  - 22 sector cards across 10 groups (Technology, Financials, Consumer, etc.)
  - Technology split: Broad Tech, Semiconductors, AI Infrastructure, Cloud/SaaS,
    Communication Services
  - Financials split: Broad, Large Banks, Regional Banks, Capital Markets,
    Fintech & Payments
  - Consumer split: Discretionary, Staples, Retail, Luxury
  - Individual name cards (top 3 per sector) with RS, SMA position, vol momentum
  - 4 leading indicator spread cards: HYG/LQD, KRE/KBE, XLP/XLY, SOXX/EWY
  - Rotation matrix: all sectors ranked by RS vs SPY (5d + 20d)
  - Regime synthesis: composite risk-on/risk-off read from all signals
  - CFTC COT: Gold, Bonds, Crude futures positioning
  - Dual output structure: grouped (for UI) + flat sectors dict (for tables)
  - Designed for future use as standalone equities tool, not BTC-only

NOTE: Healthcare individual names (UNH, JNJ, PFE) and select Industrials names
(CAT, HON) are not yet in ALL_TICKERS — add them and update the sector
definitions' "names" lists when ready.

Endpoints:
  GET /sector-flows/metrics       — full matrix (sectors + leading + rotation + COT)
  GET /sector-flows/rotation      — rotation matrix only (lightweight poll)
  GET /sector-flows/leading       — leading indicator spreads only
  GET /sector-flows/cot           — CFTC COT positioning only
  GET /sector-flows/cache/flush   — flush sector + OHLCV caches
"""

import io
import csv
import math
import os
import time
import zipfile
from datetime import date, datetime

import pandas as pd
import requests
import yfinance as yf
from fastapi import APIRouter

from shared.yf_cache import ALL_TICKERS
from shared.yf_cache import get_series as _yf

# ── Router ────────────────────────────────────────────────────────────────────

sector_flows_router = APIRouter(prefix="/sector-flows")

# ── Config ────────────────────────────────────────────────────────────────────

FLOW_CACHE_TTL  = 300    # 5 min
COT_CACHE_TTL   = 3600   # 1 hour — COT is weekly
OHLCV_CACHE_TTL = 300    # 5 min
N_DAYS          = 60     # lookback for OHLCV fetch

# ── Sector definitions ────────────────────────────────────────────────────────
#
# primary_etf / secondary_etf : keys in shared yf_cache ALL_TICKERS
# names                       : individual stock keys in ALL_TICKERS
# tier                        : 1=critical daily, 2=confirm/deny, 3=structural context
# btc_signal                  : what this sector signals for BTC regime

SECTOR_DEFINITIONS: dict[str, dict] = {

    # ── Technology ────────────────────────────────────────────────────────────
    "tech_broad": {
        "name":          "Broad Technology",
        "group":         "technology",
        "primary_etf":   "xlk",
        "secondary_etf": "qqq",
        "names":         ["nvda", "aapl", "msft"],
        "index_ref":     "nasdaq",
        "btc_signal":    "Risk-on baseline — tech momentum directly correlates with BTC risk appetite",
        "tier":          1,
    },
    "semiconductors": {
        "name":          "Semiconductors",
        "group":         "technology",
        "primary_etf":   "soxx",
        "secondary_etf": "smh",
        "names":         ["nvda", "amd", "arm"],
        "index_ref":     "nasdaq",
        "btc_signal":    "AI cycle barometer — SOXX leads risk regime, direct Korea supply chain link",
        "tier":          1,
    },
    "ai_infrastructure": {
        "name":          "AI Infrastructure",
        "group":         "technology",
        "primary_etf":   "smh",
        "secondary_etf": None,
        "names":         ["nvda", "smci", "arm"],
        "index_ref":     "nasdaq",
        "btc_signal":    "Data center capex = liquidity deployment signal — NVDA earnings move the regime",
        "tier":          2,
    },
    "cloud_saas": {
        "name":          "Cloud / SaaS",
        "group":         "technology",
        "primary_etf":   "skyy",
        "secondary_etf": "igv",
        "names":         ["msft", "googl", "meta"],
        "index_ref":     "nasdaq",
        "btc_signal":    "Rate-sensitive long-duration asset — multiple compression precedes broad risk-off",
        "tier":          2,
    },
    "comm_services": {
        "name":          "Communication Services",
        "group":         "technology",
        "primary_etf":   "xlc",
        "secondary_etf": None,
        "names":         ["meta", "googl", "tsla"],
        "index_ref":     "nasdaq",
        "btc_signal":    "Ad spend = consumer health proxy + institutional risk appetite signal",
        "tier":          2,
    },

    # ── Financials ────────────────────────────────────────────────────────────
    "fin_broad": {
        "name":          "Broad Financials",
        "group":         "financials",
        "primary_etf":   "xlf",
        "secondary_etf": "kbe",
        "names":         ["jpm", "gs", "ms"],
        "index_ref":     "spx",
        "btc_signal":    "Credit conditions and Fed transmission — tightening = BTC headwind",
        "tier":          1,
    },
    "large_banks": {
        "name":          "Large Banks",
        "group":         "financials",
        "primary_etf":   "kbe",
        "secondary_etf": "xlf",
        "names":         ["jpm", "bac", "gs"],
        "index_ref":     "spx",
        "btc_signal":    "Wholesale credit + prime brokerage — GS/MS are institutional crypto access points",
        "tier":          1,
    },
    "regional_banks": {
        "name":          "Regional Banks",
        "group":         "financials",
        "primary_etf":   "kre",
        "secondary_etf": None,
        "names":         ["wfc", "wal", "zion"],
        "index_ref":     "spx",
        "btc_signal":    "Stress canary — KRE breakdown precedes systemic risk-off by days (SVB 2023)",
        "tier":          1,
    },
    "capital_markets": {
        "name":          "Capital Markets",
        "group":         "financials",
        "primary_etf":   "iai",
        "secondary_etf": None,
        "names":         ["gs", "ms", "bac"],
        "index_ref":     "spx",
        "btc_signal":    "Risk appetite gauge — open capital markets = institutional crypto participation",
        "tier":          2,
    },
    "fintech_payments": {
        "name":          "Fintech & Payments",
        "group":         "financials",
        "primary_etf":   "ipay",
        "secondary_etf": None,
        "names":         ["v", "ma", "pypl"],
        "index_ref":     "spx",
        "btc_signal":    "Payment velocity = consumer spending health + digital money flow signal",
        "tier":          2,
    },

    # ── Healthcare ────────────────────────────────────────────────────────────
    "healthcare": {
        "name":          "Healthcare",
        "group":         "healthcare",
        "primary_etf":   "xlv",
        "secondary_etf": "xbi",
        "names":         [],                # Add unh, jnj, pfe to ALL_TICKERS to populate
        "index_ref":     "spx",
        "btc_signal":    "Defensive rotation magnet — XLV inflows = institutional de-risking forming",
        "tier":          3,
    },

    # ── Industrials ───────────────────────────────────────────────────────────
    "industrials": {
        "name":          "Industrials",
        "group":         "industrials",
        "primary_etf":   "xli",
        "secondary_etf": "iyt",
        "names":         [],                # Add cat, hon to ALL_TICKERS to populate
        "index_ref":     "dji",
        "btc_signal":    "Economic cycle signal — IYT transports lead by 4-6 weeks",
        "tier":          3,
    },
    "transports": {
        "name":          "Transports",
        "group":         "industrials",
        "primary_etf":   "iyt",
        "secondary_etf": None,
        "names":         [],
        "index_ref":     "dji",
        "btc_signal":    "Leading economic indicator — goods movement precedes earnings revisions",
        "tier":          2,
    },

    # ── Energy ────────────────────────────────────────────────────────────────
    "energy": {
        "name":          "Energy",
        "group":         "energy",
        "primary_etf":   "xle",
        "secondary_etf": "oih",
        "names":         [],
        "index_ref":     "spx",
        "btc_signal":    "Inflation regime signal — rising energy = cost pressure = Fed hawkish = BTC headwind",
        "tier":          2,
    },

    # ── Materials ─────────────────────────────────────────────────────────────
    "materials": {
        "name":          "Materials",
        "group":         "materials",
        "primary_etf":   "xlb",
        "secondary_etf": None,
        "names":         [],
        "index_ref":     "spx",
        "btc_signal":    "China demand + inflation cycle — copper is 4-6 week leading economic indicator",
        "tier":          3,
    },

    # ── Consumer ──────────────────────────────────────────────────────────────
    "consumer_disc": {
        "name":          "Consumer Discretionary",
        "group":         "consumer",
        "primary_etf":   "xly",
        "secondary_etf": None,
        "names":         ["tsla", "aapl", "meta"],
        "index_ref":     "spx",
        "btc_signal":    "Risk appetite proxy — discretionary spend rises with asset prices and crypto",
        "tier":          2,
    },
    "consumer_staples": {
        "name":          "Consumer Staples",
        "group":         "consumer",
        "primary_etf":   "xlp",
        "secondary_etf": None,
        "names":         [],
        "index_ref":     "spx",
        "btc_signal":    "Defensive rotation magnet — XLP outperforming XLY = risk-off forming",
        "tier":          2,
    },
    "consumer_retail": {
        "name":          "Consumer Retail",
        "group":         "consumer",
        "primary_etf":   "xrt",
        "secondary_etf": None,
        "names":         [],
        "index_ref":     "spx",
        "btc_signal":    "Broad spending breadth — equal-weight surfaces small retailer stress early",
        "tier":          3,
    },
    "consumer_luxury": {
        "name":          "Consumer Luxury",
        "group":         "consumer",
        "primary_etf":   "glux",
        "secondary_etf": None,
        "names":         [],
        "index_ref":     "spx",
        "btc_signal":    "Wealth effect + China consumer confidence — luxury leads asset price cycles",
        "tier":          3,
    },

    # ── Real Estate ───────────────────────────────────────────────────────────
    "real_estate": {
        "name":          "Real Estate",
        "group":         "real_estate",
        "primary_etf":   "xlre",
        "secondary_etf": "vnq",
        "names":         [],
        "index_ref":     "spx",
        "btc_signal":    "Rate sensitivity — XLRE falling = real rates rising = BTC duration headwind",
        "tier":          3,
    },

    # ── Utilities ─────────────────────────────────────────────────────────────
    "utilities": {
        "name":          "Utilities",
        "group":         "utilities",
        "primary_etf":   "xlu",
        "secondary_etf": None,
        "names":         [],
        "index_ref":     "spx",
        "btc_signal":    "Defensive + AI power demand — XLU rising = defensive rotation OR data center build",
        "tier":          3,
    },

    # ── Korea / Asia ──────────────────────────────────────────────────────────
    "korea_semis": {
        "name":          "Korea / Semis",
        "group":         "international",
        "primary_etf":   "ewy",
        "secondary_etf": "soxx",
        "names":         ["nvda", "amd", "arm"],
        "index_ref":     "nasdaq",
        "btc_signal":    "Asia supply chain — EWY weakness = semi cycle cooling = risk-off forming",
        "tier":          2,
    },

    # ── Bonds ─────────────────────────────────────────────────────────────────
    "bonds_long": {
        "name":          "Long Duration Bonds",
        "group":         "bonds",
        "primary_etf":   "tlt",
        "secondary_etf": "ief",
        "names":         [],
        "index_ref":     "spx",
        "btc_signal":    "Risk-off anchor — TLT bid = duration buying = flight from risk assets including BTC",
        "tier":          1,
    },

    # ── Commodities ───────────────────────────────────────────────────────────
    "gold": {
        "name":          "Gold",
        "group":         "commodities",
        "primary_etf":   "gold",
        "secondary_etf": None,
        "names":         [],
        "index_ref":     "spx",
        "btc_signal":    "Macro hedge — gold + BTC both rise in dollar debasement and stress regimes",
        "tier":          1,
    },

    # ── Crypto ────────────────────────────────────────────────────────────────
    "crypto": {
        "name":          "Crypto Proxies",
        "group":         "crypto",
        "primary_etf":   "btc_usd",
        "secondary_etf": None,
        "names":         ["mstr", "coin", "hood"],
        "index_ref":     "nasdaq",
        "btc_signal":    "Native signal — proxy stock flows confirm or lead BTC spot moves",
        "tier":          1,
    },
}

# ── OHLCV ticker list — focused subset for volume-based metrics ───────────────
# Maps yf symbols (not cache keys) — these are passed directly to yf.download()

OHLCV_TICKERS: list[str] = [
    # Technology ETFs
    "XLK", "SOXX", "SMH", "SKYY", "IGV", "XLC",
    # Financial ETFs
    "XLF", "KBE", "KRE", "IAI", "IPAY",
    # Other sector ETFs
    "XLV", "XBI", "XLI", "IYT", "XLE", "OIH",
    "XLB", "XLY", "XLP", "XRT", "GLUX",
    "XLRE", "VNQ", "XLU",
    # International
    "EWY",
    # Bonds + Credit
    "TLT", "IEF", "HYG", "LQD",
    # Benchmarks
    "SPY", "QQQ", "IWM",
    # Key individual names with high signal value
    "NVDA", "AMD", "AAPL", "MSFT", "META", "GOOGL", "TSLA",
    "JPM", "BAC", "GS", "MS", "WFC", "WAL", "ZION",
    "V", "MA", "COF", "AXP", "PYPL",
    "MSTR", "COIN", "HOOD",
    "GC=F", "BTC-USD",
]

# ── COT report codes (CFTC Financial Futures) ─────────────────────────────────
COT_CODES: dict[str, str] = {
    "gold":   "088691",    # Gold futures (COMEX)
    "bonds":  "020601",    # 30-Year T-Bond (CBOT)
    "crude":  "067651",    # WTI Crude Oil (NYMEX)
}

# ── Caches ────────────────────────────────────────────────────────────────────

_flow_cache:  dict = {"data": None, "ts": 0.0}
_ohlcv_cache: dict = {"data": None, "ts": 0.0}
_cot_cache:   dict = {"data": None, "ts": 0.0}

# ── Sanitize ──────────────────────────────────────────────────────────────────

def _san(val):
    if val is None:
        return None
    try:
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    except (TypeError, ValueError):
        return None

# ── Close-price helpers (use shared yf_cache series) ─────────────────────────

def _sma(series: pd.Series, window: int) -> float | None:
    if series is None or len(series) < window:
        return None
    return float(series.tail(window).mean())


def _pct_from_sma(current: float, sma: float | None) -> float | None:
    if sma is None or sma == 0:
        return None
    return round((current - sma) / sma * 100, 2)


def _pct_rank(vals: list[float], current: float) -> int:
    if not vals or len(vals) < 5:
        return 50
    return round(sum(1 for v in vals if v < current) / len(vals) * 100)


def _rs(series: pd.Series, spy: pd.Series, window: int) -> float | None:
    """Continuous relative strength: sector return minus SPY return over window."""
    if series is None or spy is None:
        return None
    if len(series) < window + 1 or len(spy) < window + 1:
        return None
    try:
        s_ret = (float(series.iloc[-1]) - float(series.iloc[-window - 1])) / float(series.iloc[-window - 1]) * 100
        b_ret = (float(spy.iloc[-1])    - float(spy.iloc[-window - 1]))    / float(spy.iloc[-window - 1])    * 100
        return round(s_ret - b_ret, 2)
    except Exception:
        return None


def _chg_pct(series: pd.Series, window: int) -> float | None:
    if series is None or len(series) < window + 1:
        return None
    try:
        return round(
            (float(series.iloc[-1]) - float(series.iloc[-window - 1]))
            / float(series.iloc[-window - 1]) * 100, 2
        )
    except Exception:
        return None


def _spark_close(series: pd.Series, n: int = 20) -> list[float]:
    if series is None or len(series) == 0:
        return []
    return [round(float(v), 4) for v in series.tail(n).tolist()]

# ── OHLCV fetch + cache ───────────────────────────────────────────────────────

def _get_ohlcv() -> dict[str, pd.DataFrame | None]:
    """
    Focused OHLCV download for volume-based metrics (MFI, OBV, vol momentum).
    Separate from shared yf_cache which is Close-only.
    """
    global _ohlcv_cache
    now = time.time()
    if _ohlcv_cache["data"] and (now - _ohlcv_cache["ts"]) < OHLCV_CACHE_TTL:
        return _ohlcv_cache["data"]

    result: dict[str, pd.DataFrame | None] = {t: None for t in OHLCV_TICKERS}
    try:
        print(f"[sector_flows] Fetching OHLCV for {len(OHLCV_TICKERS)} tickers…")
        raw = yf.download(
            OHLCV_TICKERS,
            period=f"{N_DAYS}d",
            auto_adjust=True,
            progress=False,
            threads=True,
            group_by="ticker",
        )
        for ticker in OHLCV_TICKERS:
            try:
                if ticker in raw.columns.get_level_values(0):
                    df = raw[ticker].dropna(how="all")
                    result[ticker] = df if len(df) > 20 else None
            except Exception as e:
                print(f"[sector_flows] OHLCV parse {ticker}: {e}")

        n_ok = sum(1 for v in result.values() if v is not None)
        print(f"[sector_flows] OHLCV OK — {n_ok}/{len(OHLCV_TICKERS)} loaded")

    except Exception as e:
        print(f"[sector_flows] OHLCV bulk error: {e}")

    _ohlcv_cache["data"] = result
    _ohlcv_cache["ts"]   = now
    return result


def _ohlcv_for(key: str, ohlcv: dict) -> pd.DataFrame | None:
    """Look up OHLCV DataFrame by yf_cache key, resolving to yfinance symbol."""
    symbol = ALL_TICKERS.get(key, key.upper())
    return ohlcv.get(symbol)

# ── Volume-based metric calculators ───────────────────────────────────────────

def _calc_mfi(df: pd.DataFrame, period: int = 14) -> float | None:
    """Money Flow Index (0–100). Volume-weighted RSI."""
    if df is None or len(df) < period + 1:
        return None
    try:
        tp      = (df["High"] + df["Low"] + df["Close"]) / 3
        mf      = tp * df["Volume"]
        pos_mf  = neg_mf = 0.0
        for i in range(1, period + 1):
            if tp.iloc[-i] > tp.iloc[-i - 1]:
                pos_mf += float(mf.iloc[-i])
            else:
                neg_mf += float(mf.iloc[-i])
        if neg_mf == 0:
            return 100.0
        return round(100 - (100 / (1 + pos_mf / neg_mf)), 2)
    except Exception as e:
        print(f"[sector_flows] MFI error: {e}")
        return None


def _calc_obv_series(df: pd.DataFrame) -> list[float] | None:
    """Build cumulative OBV series."""
    if df is None or len(df) < 2:
        return None
    try:
        obv, vals = 0.0, []
        for i in range(len(df)):
            if i > 0:
                if df["Close"].iloc[i] > df["Close"].iloc[i - 1]:
                    obv += float(df["Volume"].iloc[i])
                elif df["Close"].iloc[i] < df["Close"].iloc[i - 1]:
                    obv -= float(df["Volume"].iloc[i])
            vals.append(obv)
        return vals
    except Exception as e:
        print(f"[sector_flows] OBV series error: {e}")
        return None


def _calc_obv_zscore(df: pd.DataFrame, window: int = 20) -> float | None:
    """
    OBV z-score: how many std devs current OBV is from its rolling mean.
    Comparable across assets regardless of absolute volume scale.
    Positive = accumulation. Negative = distribution.
    """
    vals = _calc_obv_series(df)
    if not vals or len(vals) < window + 1:
        return None
    try:
        recent   = vals[-(window + 1):-1]
        mean     = sum(recent) / len(recent)
        variance = sum((x - mean) ** 2 for x in recent) / len(recent)
        std      = variance ** 0.5
        if std == 0:
            return 0.0
        return round((vals[-1] - mean) / std, 2)
    except Exception as e:
        print(f"[sector_flows] OBV z-score error: {e}")
        return None


def _calc_obv_normalized(df: pd.DataFrame, window: int = 20) -> float | None:
    """OBV normalized to −100/+100 over trailing window."""
    vals = _calc_obv_series(df)
    if not vals or len(vals) < window:
        return None
    try:
        recent = vals[-window:]
        lo, hi = min(recent), max(recent)
        if hi == lo:
            return 0.0
        return round(((vals[-1] - lo) / (hi - lo)) * 200 - 100, 1)
    except Exception:
        return None


def _calc_vol_momentum(df: pd.DataFrame) -> float | None:
    """5d avg volume / 20d avg volume. >1.2 = elevated participation."""
    if df is None or len(df) < 20:
        return None
    try:
        v5  = float(df["Volume"].tail(5).mean())
        v20 = float(df["Volume"].tail(20).mean())
        return round(v5 / v20, 2) if v20 > 0 else None
    except Exception:
        return None

# ── Flow signal composite ─────────────────────────────────────────────────────

def _flow_signal(
    mfi: float | None,
    vol_mom: float | None,
    rs_5d: float | None,
) -> str:
    if mfi is None or vol_mom is None:
        return "Insufficient data"
    score = 0
    if   mfi > 65:      score += 2
    elif mfi > 55:      score += 1
    elif mfi < 35:      score -= 2
    elif mfi < 45:      score -= 1
    if   vol_mom > 1.3: score += 2
    elif vol_mom > 1.1: score += 1
    elif vol_mom < 0.7: score -= 2
    elif vol_mom < 0.9: score -= 1
    if rs_5d is not None:
        if   rs_5d > 2:  score += 1
        elif rs_5d < -2: score -= 1
    if   score >= 4:  return "Heavy Inflow"
    if   score >= 2:  return "Strong Inflow"
    if   score >= 1:  return "Mild Inflow"
    if   score <= -4: return "Heavy Outflow"
    if   score <= -2: return "Strong Outflow"
    if   score <= -1: return "Mild Outflow"
    return "Stable"


def _flow_alert_level(signal: str) -> str:
    if "Heavy"  in signal: return "extreme"
    if "Strong" in signal: return "notable"
    if "Mild"   in signal: return "none"
    return "none"

# ── Sector card builder ───────────────────────────────────────────────────────

def _build_sector_card(
    sector_key: str,
    defn: dict,
    spy: pd.Series | None,
    ohlcv: dict,
) -> dict:
    etf_key  = defn["primary_etf"]
    etf_key2 = defn.get("secondary_etf")
    series   = _yf(etf_key)
    series2  = _yf(etf_key2) if etf_key2 else None
    df       = _ohlcv_for(etf_key, ohlcv)

    card: dict = {
        "key":           sector_key,
        "name":          defn["name"],
        "group":         defn["group"],
        "primary_etf":   ALL_TICKERS.get(etf_key, etf_key).upper(),
        "secondary_etf": ALL_TICKERS.get(etf_key2, "").upper() if etf_key2 else None,
        "tier":          defn["tier"],
        "btc_signal":    defn["btc_signal"],
        "index_ref":     defn.get("index_ref"),
    }

    if series is None or len(series) < 5:
        card["error"] = f"{etf_key.upper()} unavailable"
        return card

    vals    = series.tolist()
    current = float(series.iloc[-1])

    # SMA levels
    sma20  = _sma(series, 20)
    sma50  = _sma(series, 50)
    sma200 = _sma(series, 200)

    # % from each SMA
    pct_sma20  = _san(_pct_from_sma(current, sma20))
    pct_sma50  = _san(_pct_from_sma(current, sma50))
    pct_sma200 = _san(_pct_from_sma(current, sma200))

    # Relative strength vs SPY
    rs_5d  = _san(_rs(series, spy, 5))
    rs_20d = _san(_rs(series, spy, 20))

    # Price changes
    chg_1d  = _san(_chg_pct(series, 1))
    chg_5d  = _san(_chg_pct(series, 5))
    chg_20d = _san(_chg_pct(series, 20))

    # Percentile rank
    pctile = _pct_rank(vals, current)

    # Volume-based metrics (OHLCV required)
    mfi      = _san(_calc_mfi(df))
    obv_z    = _san(_calc_obv_zscore(df))
    obv_norm = _san(_calc_obv_normalized(df))
    vol_mom  = _san(_calc_vol_momentum(df))

    # Flow composite
    signal      = _flow_signal(mfi, vol_mom, rs_5d)
    flow_alert  = _flow_alert_level(signal)

    # SMA position alert
    sma_alert = "Normal"
    if pct_sma200 is not None:
        if   pct_sma200 < -10: sma_alert = "Far below 200d SMA"
        elif pct_sma200 < -5:  sma_alert = "Below 200d SMA"
        elif pct_sma200 > 15:  sma_alert = "Well above 200d SMA"
        elif pct_sma200 > 10:  sma_alert = "Above 200d SMA"

    card.update({
        "current":            round(current, 2),
        "chg_1d":             chg_1d,
        "chg_5d":             chg_5d,
        "chg_20d":            chg_20d,
        "sma20":              round(sma20,  2) if sma20  else None,
        "sma50":              round(sma50,  2) if sma50  else None,
        "sma200":             round(sma200, 2) if sma200 else None,
        "pct_from_sma20":     pct_sma20,
        "pct_from_sma50":     pct_sma50,
        "pct_from_sma200":    pct_sma200,
        "rs_5d":              rs_5d,
        "rs_20d":             rs_20d,
        "percentile":         pctile,
        "mfi":                mfi,
        "obv_zscore":         obv_z,
        "obv_normalized":     obv_norm,
        "volume_momentum":    vol_mom,
        "flow_signal":        signal,
        "flow_alert":         flow_alert,
        "sma_alert":          sma_alert,
        "spark":              _spark_close(series, 20),
    })

    # Primary vs secondary ETF 5d RS divergence
    if series2 is not None and len(series2) >= 6 and len(series) >= 6:
        try:
            p1 = float(series.iloc[-1])  / float(series.iloc[-6])
            p2 = float(series2.iloc[-1]) / float(series2.iloc[-6])
            card["primary_vs_secondary_5d"] = _san(round((p1 - p2) * 100, 2))
        except Exception:
            pass

    return card

# ── Individual name card builder ──────────────────────────────────────────────

def _build_name_card(
    key: str,
    spy: pd.Series | None,
    ohlcv: dict,
) -> dict:
    series = _yf(key)
    df     = _ohlcv_for(key, ohlcv)
    symbol = ALL_TICKERS.get(key, key.upper())

    if series is None or len(series) < 5:
        return {"key": key, "ticker": symbol, "error": "unavailable"}

    current  = float(series.iloc[-1])
    vals     = series.tolist()
    sma50    = _sma(series, 50)
    sma200   = _sma(series, 200)
    rs_5d    = _san(_rs(series, spy, 5))
    rs_20d   = _san(_rs(series, spy, 20))
    chg_5d   = _san(_chg_pct(series, 5))
    pctile   = _pct_rank(vals, current)
    vol_mom  = _san(_calc_vol_momentum(df))
    obv_z    = _san(_calc_obv_zscore(df))

    return {
        "key":             key,
        "ticker":          symbol,
        "current":         round(current, 2),
        "chg_5d":          chg_5d,
        "rs_5d":           rs_5d,
        "rs_20d":          rs_20d,
        "above_sma50":     (current > sma50)  if sma50  else None,
        "above_sma200":    (current > sma200) if sma200 else None,
        "pct_from_sma50":  _san(_pct_from_sma(current, sma50)),
        "pct_from_sma200": _san(_pct_from_sma(current, sma200)),
        "percentile":      pctile,
        "volume_momentum": vol_mom,
        "obv_zscore":      obv_z,
    }

# ── Leading indicator spread cards ────────────────────────────────────────────

def _build_hyg_lqd_spread(ohlcv: dict) -> dict:
    """
    HYG/LQD ratio — single most important leading indicator in the stack.
    Falling ratio = credit stress = risk-off forming 2-4 weeks ahead.
    """
    hyg_df = ohlcv.get("HYG")
    lqd_df = ohlcv.get("LQD")

    if hyg_df is None or lqd_df is None:
        return {"name": "HYG/LQD Credit Spread", "error": "HYG or LQD unavailable"}

    try:
        common = hyg_df["Close"].dropna().index.intersection(lqd_df["Close"].dropna().index)
        if len(common) < 21:
            return {"name": "HYG/LQD Credit Spread", "error": "Insufficient overlapping data"}

        ratio   = hyg_df["Close"].loc[common] / lqd_df["Close"].loc[common]
        current = round(float(ratio.iloc[-1]), 4)
        d5_chg  = _san(round(float(ratio.iloc[-1] - ratio.iloc[-6]),  4)) if len(ratio) >= 6  else None
        d20_chg = _san(round(float(ratio.iloc[-1] - ratio.iloc[-21]), 4)) if len(ratio) >= 21 else None
        vals    = ratio.tolist()
        pctile  = _pct_rank(vals, current)

        if d5_chg is not None:
            if   d5_chg > 0.005:  trend, alert_level = "Risk-On — HY outperforming IG",     "none"
            elif d5_chg < -0.005: trend, alert_level = "Risk-Off — HY underperforming IG",  "notable"
            else:                 trend, alert_level = "Neutral",                            "none"
        else:
            trend, alert_level = "—", "none"

        if pctile <= 15 and d5_chg is not None and d5_chg < -0.01:
            alert_level = "extreme"
            trend       = "Extreme credit stress — spreads blowing out"

        return {
            "name":        "HYG/LQD Credit Spread",
            "description": "HY vs IG bond ETF ratio. Falling = credit stress forming. Leads equity by 2-4 weeks.",
            "ratio":       current,
            "d5_chg":      d5_chg,
            "d20_chg":     d20_chg,
            "percentile":  pctile,
            "trend":       trend,
            "alert_level": alert_level,
            "btc_signal":  "Most important leading indicator — credit tightening precedes BTC sell pressure",
            "spark":       [round(float(v), 4) for v in ratio.tail(20).tolist()],
        }
    except Exception as e:
        return {"name": "HYG/LQD Credit Spread", "error": str(e)}


def _build_kre_kbe_spread(ohlcv: dict) -> dict:
    """
    KRE/KBE ratio — regional vs large bank stress canary.
    Falling ratio = regional stress forming before systemic risk-off.
    Preceded SVB collapse by days in March 2023.
    """
    kre_df = ohlcv.get("KRE")
    kbe_df = ohlcv.get("KBE")

    if kre_df is None or kbe_df is None:
        return {"name": "KRE/KBE Bank Stress", "error": "KRE or KBE unavailable"}

    try:
        common = kre_df["Close"].dropna().index.intersection(kbe_df["Close"].dropna().index)
        if len(common) < 21:
            return {"name": "KRE/KBE Bank Stress", "error": "Insufficient overlapping data"}

        ratio   = kre_df["Close"].loc[common] / kbe_df["Close"].loc[common]
        current = round(float(ratio.iloc[-1]), 4)
        d5_chg  = _san(round(float(ratio.iloc[-1] - ratio.iloc[-6]),  4)) if len(ratio) >= 6  else None
        d20_chg = _san(round(float(ratio.iloc[-1] - ratio.iloc[-21]), 4)) if len(ratio) >= 21 else None
        vals    = ratio.tolist()
        pctile  = _pct_rank(vals, current)

        if pctile <= 10:
            stress, alert_level = "Extreme regional stress — systemic risk elevated", "extreme"
        elif d5_chg is not None and d5_chg < -0.005:
            stress, alert_level = "Regional stress forming — KRE underperforming large banks", "notable"
        elif d5_chg is not None and d5_chg > 0.005:
            stress, alert_level = "Regional banks recovering", "none"
        else:
            stress, alert_level = "Stable — no regional divergence", "none"

        return {
            "name":        "KRE/KBE Bank Stress",
            "description": "Regional vs large bank ETF ratio. Falling = stress canary. Preceded SVB by days.",
            "ratio":       current,
            "d5_chg":      d5_chg,
            "d20_chg":     d20_chg,
            "percentile":  pctile,
            "stress":      stress,
            "alert_level": alert_level,
            "btc_signal":  "Regional bank stress = credit contraction risk = BTC liquidity headwind",
            "spark":       [round(float(v), 4) for v in ratio.tail(20).tolist()],
        }
    except Exception as e:
        return {"name": "KRE/KBE Bank Stress", "error": str(e)}


def _build_xlp_xly_ratio(ohlcv: dict) -> dict:
    """
    XLP/XLY ratio — defensive rotation signal.
    Rising = staples outperforming discretionary = risk-off rotation forming.
    """
    xlp_df = ohlcv.get("XLP")
    xly_df = ohlcv.get("XLY")

    if xlp_df is None or xly_df is None:
        return {"name": "XLP/XLY Rotation", "error": "XLP or XLY unavailable"}

    try:
        common = xlp_df["Close"].dropna().index.intersection(xly_df["Close"].dropna().index)
        if len(common) < 21:
            return {"name": "XLP/XLY Rotation", "error": "Insufficient overlapping data"}

        ratio   = xlp_df["Close"].loc[common] / xly_df["Close"].loc[common]
        current = round(float(ratio.iloc[-1]), 4)
        d5_chg  = _san(round(float(ratio.iloc[-1] - ratio.iloc[-6]),  4)) if len(ratio) >= 6  else None
        d20_chg = _san(round(float(ratio.iloc[-1] - ratio.iloc[-21]), 4)) if len(ratio) >= 21 else None
        vals    = ratio.tolist()
        pctile  = _pct_rank(vals, current)

        if pctile >= 85:
            rotation, alert_level = "Extreme defensive rotation — broad institutional de-risking", "extreme"
        elif d5_chg is not None and d5_chg > 0.003:
            rotation, alert_level = "Defensive rotation forming — staples outperforming", "notable"
        elif d5_chg is not None and d5_chg < -0.003:
            rotation, alert_level = "Risk-on rotation — discretionary leading staples",  "none"
        else:
            rotation, alert_level = "Neutral — no strong rotation signal",               "none"

        return {
            "name":        "XLP/XLY Rotation",
            "description": "Staples vs Discretionary ratio. Rising = defensive rotation. Confirms risk-off early.",
            "ratio":       current,
            "d5_chg":      d5_chg,
            "d20_chg":     d20_chg,
            "percentile":  pctile,
            "rotation":    rotation,
            "alert_level": alert_level,
            "btc_signal":  "Defensive rotation = institutional de-risking = BTC headwind",
            "spark":       [round(float(v), 4) for v in ratio.tail(20).tolist()],
        }
    except Exception as e:
        return {"name": "XLP/XLY Rotation", "error": str(e)}


def _build_soxx_ewy_spread() -> dict:
    """
    SOXX vs EWY relative strength — US semi vs Korea semi divergence.
    Korea diverging lower = Asia supply chain stress before US prices react.
    """
    soxx = _yf("soxx")
    ewy  = _yf("ewy")
    spy  = _yf("spy")

    if soxx is None or ewy is None:
        return {"name": "SOXX/EWY Semi Divergence", "error": "SOXX or EWY unavailable"}

    rs_soxx_5d  = _san(_rs(soxx, spy, 5))  if spy is not None else None
    rs_ewy_5d   = _san(_rs(ewy,  spy, 5))  if spy is not None else None
    rs_soxx_20d = _san(_rs(soxx, spy, 20)) if spy is not None else None
    rs_ewy_20d  = _san(_rs(ewy,  spy, 20)) if spy is not None else None

    divergence_5d = None
    if rs_ewy_5d is not None and rs_soxx_5d is not None:
        divergence_5d = _san(round(rs_ewy_5d - rs_soxx_5d, 2))

    if divergence_5d is not None:
        if   divergence_5d < -3: signal, alert_level = "Korea lagging US semis — Asia supply chain stress",   "notable"
        elif divergence_5d > 3:  signal, alert_level = "Korea leading US semis — Asia demand recovering",     "none"
        else:                    signal, alert_level = "SOXX and EWY tracking together — no divergence",      "none"
    else:
        signal, alert_level = "—", "none"

    return {
        "name":           "SOXX/EWY Semi Divergence",
        "description":    "US Semiconductors vs Korea ETF relative performance. Divergence signals supply chain stress.",
        "soxx_rs_5d":     rs_soxx_5d,
        "soxx_rs_20d":    rs_soxx_20d,
        "ewy_rs_5d":      rs_ewy_5d,
        "ewy_rs_20d":     rs_ewy_20d,
        "divergence_5d":  divergence_5d,
        "signal":         signal,
        "alert_level":    alert_level,
        "btc_signal":     "Korea semi stress = AI supply chain risk = tech risk-off = BTC headwind",
    }

# ── Rotation matrix ───────────────────────────────────────────────────────────

def _build_rotation_matrix(sectors: dict) -> list[dict]:
    """
    All sectors ranked by 5d RS vs SPY descending.
    The single clearest view of where capital is rotating right now.
    """
    rows = []
    for key, card in sectors.items():
        if "error" in card or card.get("rs_5d") is None:
            continue
        rows.append({
            "key":          key,
            "name":         card["name"],
            "group":        card["group"],
            "tier":         card.get("tier"),
            "rs_5d":        card["rs_5d"],
            "rs_20d":       card.get("rs_20d"),
            "chg_5d":       card.get("chg_5d"),
            "mfi":          card.get("mfi"),
            "obv_zscore":   card.get("obv_zscore"),
            "flow_signal":  card.get("flow_signal"),
            "flow_alert":   card.get("flow_alert"),
            "percentile":   card.get("percentile"),
        })
    rows.sort(key=lambda r: r["rs_5d"] or 0, reverse=True)
    return rows

# ── Regime synthesis ──────────────────────────────────────────────────────────

def _build_regime_read(
    sectors: dict,
    leading: dict,
    rotation_matrix: list,
) -> dict:
    """
    Synthesize all sector signals into a top-level regime read.
    Weights: credit > bank stress > defensive rotation > semi/Korea > sector RS.
    """
    signals:  list[dict] = []
    risk_off: int        = 0
    risk_on:  int        = 0

    # Credit spread — most important, highest weight
    hyg_lqd = leading.get("hyg_lqd", {})
    if hyg_lqd.get("alert_level") == "extreme":
        signals.append({"dir": "risk_off", "text": f"Credit extreme — {hyg_lqd.get('trend', '')}", "weight": 3})
        risk_off += 3
    elif hyg_lqd.get("alert_level") == "notable":
        signals.append({"dir": "risk_off", "text": f"Credit stress forming — {hyg_lqd.get('trend', '')}", "weight": 2})
        risk_off += 2
    elif hyg_lqd.get("trend", "").startswith("Risk-On"):
        signals.append({"dir": "risk_on", "text": f"Credit supportive — {hyg_lqd.get('trend', '')}", "weight": 1})
        risk_on += 1

    # Bank stress
    kre_kbe = leading.get("kre_kbe", {})
    if kre_kbe.get("alert_level") == "extreme":
        signals.append({"dir": "risk_off", "text": f"Bank stress extreme — {kre_kbe.get('stress', '')}", "weight": 3})
        risk_off += 3
    elif kre_kbe.get("alert_level") == "notable":
        signals.append({"dir": "risk_off", "text": f"Bank stress canary — {kre_kbe.get('stress', '')}", "weight": 2})
        risk_off += 2

    # Defensive rotation
    xlp_xly = leading.get("xlp_xly", {})
    if xlp_xly.get("alert_level") == "extreme":
        signals.append({"dir": "risk_off", "text": f"Defensive rotation extreme — {xlp_xly.get('rotation', '')}", "weight": 2})
        risk_off += 2
    elif xlp_xly.get("alert_level") == "notable":
        signals.append({"dir": "risk_off", "text": f"Defensive rotation forming — {xlp_xly.get('rotation', '')}", "weight": 1})
        risk_off += 1
    elif "risk-on" in xlp_xly.get("rotation", "").lower():
        signals.append({"dir": "risk_on", "text": f"Rotation risk-on — {xlp_xly.get('rotation', '')}", "weight": 1})
        risk_on += 1

    # Semi / Korea divergence
    soxx_ewy = leading.get("soxx_ewy", {})
    if soxx_ewy.get("alert_level") == "notable":
        signals.append({"dir": "risk_off", "text": f"Semi/Korea divergence — {soxx_ewy.get('signal', '')}", "weight": 1})
        risk_off += 1

    # Tier 1 sector RS signals
    tier1_keys = {k for k, d in SECTOR_DEFINITIONS.items() if d["tier"] == 1}
    for row in rotation_matrix:
        if row["key"] not in tier1_keys:
            continue
        rs = row.get("rs_5d") or 0
        if rs > 2:
            signals.append({"dir": "risk_on",  "text": f"{row['name']} leading SPY +{rs:.1f}% (5d)", "weight": 1})
            risk_on  += 1
        elif rs < -2:
            signals.append({"dir": "risk_off", "text": f"{row['name']} lagging SPY {rs:.1f}% (5d)", "weight": 1})
            risk_off += 1

    # Regime classification
    if   risk_off >= 6: regime, color = "Risk-Off",     "extreme"
    elif risk_off >= 4: regime, color = "Caution",      "notable"
    elif risk_off >= 2: regime, color = "Mild Caution", "notable"
    elif risk_on  >= 4: regime, color = "Risk-On",      "none"
    elif risk_on  >= 2: regime, color = "Mild Risk-On", "none"
    else:               regime, color = "Neutral",      "none"

    if risk_off >= 3:
        btc_read = "Sector environment risk-off — credit and rotation signals are headwinds for BTC."
    elif risk_on >= 3 and risk_off == 0:
        btc_read = "Sector environment broadly supportive — capital flowing into risk assets including crypto proxies."
    else:
        btc_read = "Sector environment mixed — no strong directional signal from capital flows. Focus on BTC-specific metrics."

    return {
        "regime":      regime,
        "color_level": color,
        "risk_on":     risk_on,
        "risk_off":    risk_off,
        "signals":     signals,
        "btc_read":    btc_read,
    }

# ── COT fetch ─────────────────────────────────────────────────────────────────

def _fetch_cot_data() -> dict:
    """
    CFTC Commitments of Traders — leveraged money (hedge fund) net positioning.
    Gold, Bonds, Crude. Released weekly with 3-day lag.
    Extreme net long / short = positioning risk = potential reversal signal.
    """
    global _cot_cache
    now = time.time()
    if _cot_cache["data"] and (now - _cot_cache["ts"]) < COT_CACHE_TTL:
        return _cot_cache["data"]

    result: dict = {}
    year = date.today().year

    for attempt_year in [year, year - 1]:
        url = f"https://www.cftc.gov/files/dea/history/fut_fin_txt_{attempt_year}.zip"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()

            zf      = zipfile.ZipFile(io.BytesIO(resp.content))
            fname   = next(n for n in zf.namelist() if n.endswith(".txt"))
            content = zf.read(fname).decode("latin-1")
            reader  = csv.DictReader(io.StringIO(content))

            rows_by_code: dict[str, list] = {}
            for row in reader:
                code = row.get("CFTC_Contract_Market_Code", "").strip()
                for asset, asset_code in COT_CODES.items():
                    if code == asset_code:
                        rows_by_code.setdefault(asset, []).append(row)

            for asset, rows in rows_by_code.items():
                rows.sort(key=lambda r: r.get("Report_Date_as_YYYY-MM-DD", ""))
                recent = rows[-20:]
                if not recent:
                    continue

                def _net(r: dict) -> int | None:
                    try:
                        l = int(r.get("Lev_Money_Positions_Long_All",  "0").replace(",", ""))
                        s = int(r.get("Lev_Money_Positions_Short_All", "0").replace(",", ""))
                        return l - s
                    except Exception:
                        return None

                net_now  = _net(recent[-1])
                net_prev = _net(recent[-2]) if len(recent) >= 2 else None
                nets     = [_net(r) for r in recent if _net(r) is not None]
                pctile   = (
                    round(sum(1 for v in nets if v < net_now) / len(nets) * 100)
                    if nets and net_now is not None else None
                )
                wk_chg = (net_now - net_prev) if (net_now is not None and net_prev is not None) else None

                def _cot_alert(net: int | None, pct: int | None) -> str:
                    if net is None:                              return "—"
                    if pct is not None and pct >= 80:           return "Extreme net long"
                    if pct is not None and pct <= 20:           return "Extreme net short"
                    return "Net long" if net > 0 else "Net short"

                result[asset] = {
                    "net_position": net_now,
                    "wk_chg":      wk_chg,
                    "percentile":  pctile,
                    "report_date": recent[-1].get("Report_Date_as_YYYY-MM-DD", "—"),
                    "alert":       _cot_alert(net_now, pctile),
                }

            if result:
                break

        except Exception as e:
            print(f"[sector_flows] COT error ({attempt_year}): {e}")
            continue

    if not result:
        result = {"error": "COT data unavailable"}

    _cot_cache["data"] = result
    _cot_cache["ts"]   = now
    return result

# ── Main builder ──────────────────────────────────────────────────────────────

def _build_sector_flows() -> dict:
    # Shared SPY Close series for all RS calculations
    spy = _yf("spy")

    # OHLCV data for volume-based metrics (focused fetch, cached separately)
    ohlcv = _get_ohlcv()

    # Build all sector cards
    sectors: dict[str, dict] = {}
    for sector_key, defn in SECTOR_DEFINITIONS.items():
        card = _build_sector_card(sector_key, defn, spy, ohlcv)

        # Attach individual name cards
        name_keys = defn.get("names", [])
        if name_keys:
            card["top_names"] = {
                k: _build_name_card(k, spy, ohlcv)
                for k in name_keys
            }

        sectors[sector_key] = card

    # Group sectors for UI rendering
    groups: dict[str, dict] = {}
    for sector_key, card in sectors.items():
        group = card.get("group", "other")
        groups.setdefault(group, {})[sector_key] = card

    # Leading indicator spreads
    leading = {
        "hyg_lqd":  _build_hyg_lqd_spread(ohlcv),
        "kre_kbe":  _build_kre_kbe_spread(ohlcv),
        "xlp_xly":  _build_xlp_xly_ratio(ohlcv),
        "soxx_ewy": _build_soxx_ewy_spread(),
    }

    # Rotation matrix — all sectors ranked by RS
    rotation_matrix = _build_rotation_matrix(sectors)

    # Composite regime read
    regime_read = _build_regime_read(sectors, leading, rotation_matrix)

    return {
        "updated_at":      datetime.utcnow().isoformat() + "Z",
        "groups":          groups,           # nested by group — for section-based UI
        "sectors":         sectors,          # flat dict — for sortable tables, heatmaps
        "leading":         leading,
        "rotation_matrix": rotation_matrix,
        "regime_read":     regime_read,
        "cot":             _fetch_cot_data(),
    }

# ── Routes ────────────────────────────────────────────────────────────────────

@sector_flows_router.get("/metrics")
def get_sector_flows_metrics():
    """Full sector matrix — all groups, leading indicators, rotation, regime read, COT."""
    global _flow_cache
    now = time.time()
    if _flow_cache["data"] and (now - _flow_cache["ts"]) < FLOW_CACHE_TTL:
        return _flow_cache["data"]
    data = _build_sector_flows()
    _flow_cache["data"] = data
    _flow_cache["ts"]   = now
    return data


@sector_flows_router.get("/rotation")
def get_rotation_matrix():
    """Rotation matrix only — all sectors ranked by RS vs SPY. Lightweight."""
    global _flow_cache
    now = time.time()
    if _flow_cache["data"] and (now - _flow_cache["ts"]) < FLOW_CACHE_TTL:
        return {"rotation_matrix": _flow_cache["data"]["rotation_matrix"]}
    data = _build_sector_flows()
    _flow_cache["data"] = data
    _flow_cache["ts"]   = now
    return {"rotation_matrix": data["rotation_matrix"]}


@sector_flows_router.get("/leading")
def get_leading_indicators():
    """Leading indicator spreads only — HYG/LQD, KRE/KBE, XLP/XLY, SOXX/EWY."""
    global _flow_cache
    now = time.time()
    if _flow_cache["data"] and (now - _flow_cache["ts"]) < FLOW_CACHE_TTL:
        return {
            "leading":     _flow_cache["data"]["leading"],
            "regime_read": _flow_cache["data"]["regime_read"],
        }
    data = _build_sector_flows()
    _flow_cache["data"] = data
    _flow_cache["ts"]   = now
    return {"leading": data["leading"], "regime_read": data["regime_read"]}


@sector_flows_router.get("/cot")
def get_cot():
    """CFTC COT positioning — Gold, Bonds, Crude. Cached 1hr (weekly release)."""
    return _fetch_cot_data()


@sector_flows_router.get("/cache/flush")
def flush_sector_flows_cache():
    """Flush both the sector flows cache and the OHLCV cache."""
    global _flow_cache, _ohlcv_cache
    _flow_cache  = {"data": None, "ts": 0.0}
    _ohlcv_cache = {"data": None, "ts": 0.0}
    return {"flushed": True}
