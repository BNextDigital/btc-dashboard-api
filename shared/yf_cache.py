"""
shared/yf_cache.py — Centralised yFinance data layer

One bulk yf.download() call covers every dashboard route.
All route files call get_series(key) instead of fetching independently.

─────────────────────────────────────────────────────────────────────
SETUP
  1. Copy this file to btc-dashboard-api/shared/yf_cache.py
  2. Create btc-dashboard-api/shared/__init__.py  (empty file)
  3. In each route file replace the local _fetch_bulk() call:

     # Before
     yf_data = _fetch_bulk(n_days=252)
     series  = yf_data.get("^VIX")

     # After
     from shared.yf_cache import get_series, get_all
     series = get_series("vix")          # by key
     yf_data = get_all()                 # full dict, same as before

─────────────────────────────────────────────────────────────────────
TO ADD A NEW TICKER
  1. Add one entry to ALL_TICKERS below.
  2. Call get_series("your_key") in the route file.
  3. Done — no other changes needed.

─────────────────────────────────────────────────────────────────────
CACHE BEHAVIOUR
  TTL    : 5 minutes (matches the most frequent dashboard refresh)
  Scope  : process-wide — all routes share one copy of the data
  Thread : a lock prevents simultaneous redundant downloads
  Warmup : call warm_cache() from main.py startup to pre-fetch on boot

─────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import threading
import time
from datetime import datetime

import pandas as pd
import yfinance as yf

# ── Master ticker registry ────────────────────────────────────────────────────
#
# key          : short string used by route files (get_series("vix"))
# yf_symbol    : ticker string passed to yf.download()
# description  : for documentation only
#
ALL_TICKERS: dict[str, str] = {

    # ── US Equity Indices ──────────────────────────────────────────────────
    "spx":          "^GSPC",        # S&P 500
    "nasdaq":       "^IXIC",        # Nasdaq Composite
    "qqq":          "QQQ",          # Nasdaq 100 ETF
    "iwm":          "^RUT",         # Russell 2000

    # ── Equity ETFs & Breadth ──────────────────────────────────────────────
    "spy":          "SPY",          # S&P 500 cap-weighted (breadth denominator)
    "rsp":          "RSP",          # S&P 500 equal-weighted (breadth numerator)
    "tlt":          "TLT",          # 20Y Treasury bond ETF

    # ── Sector ETFs ───────────────────────────────────────────────────────
    "soxx":         "SOXX",         # Semiconductors
    "xlf":          "XLF",          # Financials / Banks
    "xlk":          "XLK",          # Technology
    "xle":          "XLE",          # Energy
    "xlu":          "XLU",          # Utilities
    "xlre":         "XLRE",         # Real Estate
    "iyt":          "IYT",          # Transports

    # ── Credit ETFs (sector flows v2) ─────────────────────────────────────
    "hyg":          "HYG",          # High-yield bond ETF
    "lqd":          "LQD",          # Investment-grade bond ETF

    # ── Volatility ────────────────────────────────────────────────────────
    "vix":          "^VIX",         # CBOE VIX (equity vol)
    "vxn":          "^VXN",         # CBOE VXN (Nasdaq vol)
    "evz":          "^EVZ",         # CBOE Euro FX Vol

    # ── US Treasury Yields (yFinance) ─────────────────────────────────────
    "yield_1y":     "^IRX",         # 13-week proxy for 1Y
    "yield_5y":     "^FVX",         # 5Y
    "yield_10y":    "^TNX",         # 10Y

    # ── FX — Major Pairs ──────────────────────────────────────────────────
    "dxy":          "DX-Y.NYB",     # US Dollar Index
    "eurusd":       "EURUSD=X",     # EUR/USD
    "usdjpy":       "JPY=X",        # USD/JPY
    "usdcnh":       "USDCNH=X",        # USD/CNH

    # ── FX — Emerging Markets ─────────────────────────────────────────────
    "usdbrl":       "BRL=X",        # USD/BRL — Brazil
    "usdmxn":       "MXN=X",        # USD/MXN — Mexico
    "usdinr":       "INR=X",        # USD/INR — India
    "usdkrw":       "KRW=X",        # USD/KRW — South Korea
    "usdzar":       "ZAR=X",        # USD/ZAR — South Africa

    # ── Energy Futures ────────────────────────────────────────────────────
    "wti":          "CL=F",         # WTI Crude Oil
    "brent":        "BZ=F",         # Brent Crude Oil
    "natgas":       "NG=F",         # Natural Gas
    "gasoline":     "RB=F",         # RBOB Gasoline

    # ── Metals Futures ────────────────────────────────────────────────────
    "gold":         "GC=F",         # Gold
    "silver":       "SI=F",         # Silver
    "copper":       "HG=F",         # Copper
    "platinum":     "PL=F",         # Platinum

    # ── Grain Futures ────────────────────────────────────────────────────
    "wheat":        "ZW=F",         # Wheat
    "corn":         "ZC=F",         # Corn
    "soybeans":     "ZS=F",         # Soybeans

    # ── Crypto ───────────────────────────────────────────────────────────
    "btc_usd":      "BTC-USD",      # Bitcoin spot (sector flows)

    # ── Crypto Proxy Stocks ───────────────────────────────────────────────
    "mstr":         "MSTR",         # MicroStrategy
    "coin":         "COIN",         # Coinbase
    "hood":         "HOOD",         # Robinhood
    "mara":         "MARA",         # Marathon Digital
    "pypl":         "PYPL",         # PayPal
}

# ── Lookback ──────────────────────────────────────────────────────────────────
#
# 300 trading days (~14 months) covers:
#   - 200d SMA  (commodity + equity cards)
#   - 252d percentile rank (1 trading year)
#   - 90d percentile with comfortable headroom
#
N_DAYS = 300

# ── Cache ─────────────────────────────────────────────────────────────────────

CACHE_TTL = 300   # 5 minutes — matches fastest dashboard refresh cycle

_cache: dict = {
    "data":       None,       # {key: pd.Series | None}
    "ts":         0.0,        # epoch seconds of last successful fetch
    "updated_at": None,       # ISO string for /health endpoints
}
_lock = threading.Lock()


# ── Public API ────────────────────────────────────────────────────────────────

def get_series(key: str) -> pd.Series | None:
    """
    Return a pd.Series of daily Close prices for the given key.
    Fetches/refreshes the shared cache if stale.
    Returns None if the ticker failed or key is unknown.

    Example:
        from shared.yf_cache import get_series
        vix = get_series("vix")
    """
    data = _get_or_refresh()
    return data.get(key)


def get_all() -> dict[str, pd.Series | None]:
    """
    Return the full {key: pd.Series | None} dict.
    Drop-in replacement for each route's _fetch_bulk() return value,
    as long as the route switches to the canonical key names above.

    Example:
        from shared.yf_cache import get_all
        yf_data = get_all()
        gold_series = yf_data.get("gold")
    """
    return _get_or_refresh()


def cache_age_seconds() -> float:
    """How many seconds since the last successful fetch."""
    return time.time() - _cache["ts"]


def cache_updated_at() -> str | None:
    """ISO timestamp of last successful fetch, or None if never fetched."""
    return _cache["updated_at"]


def flush() -> None:
    """Force next call to re-fetch from yFinance. Thread-safe."""
    with _lock:
        _cache["data"] = None
        _cache["ts"]   = 0.0
        _cache["updated_at"] = None


def warm_cache() -> None:
    """
    Pre-fetch on startup so the first real request is instant.
    Call from main.py:

        from shared.yf_cache import warm_cache
        import threading
        threading.Thread(target=warm_cache, daemon=True).start()
    """
    _get_or_refresh()


# ── Internal ──────────────────────────────────────────────────────────────────

def _is_stale() -> bool:
    return _cache["data"] is None or (time.time() - _cache["ts"]) > CACHE_TTL


def _get_or_refresh() -> dict[str, pd.Series | None]:
    """Return cache if fresh; otherwise fetch under lock."""
    if not _is_stale():
        return _cache["data"]

    with _lock:
        # Re-check inside lock — another thread may have refreshed while we waited
        if not _is_stale():
            return _cache["data"]

        _cache["data"]       = _fetch()
        _cache["ts"]         = time.time()
        _cache["updated_at"] = datetime.utcnow().isoformat() + "Z"

    return _cache["data"]


def _fetch() -> dict[str, pd.Series | None]:
    """
    Single bulk yf.download() call for all tickers.
    Returns {key: pd.Series(close prices) | None}.
    """
    result: dict[str, pd.Series | None] = {k: None for k in ALL_TICKERS}

    symbols   = list(ALL_TICKERS.values())
    key_by_sym = {v: k for k, v in ALL_TICKERS.items()}

    try:
        print(f"[yf_cache] Fetching {len(symbols)} tickers ({N_DAYS}d)…")
        raw   = yf.download(
            symbols,
            period=f"{N_DAYS}d",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        close = raw["Close"] if "Close" in raw.columns else raw

        for symbol, key in key_by_sym.items():
            if symbol in close.columns:
                s = close[symbol].dropna()
                result[key] = s if len(s) >= 5 else None
            # else: stays None (ticker may be temporarily unavailable)

        successes = sum(1 for v in result.values() if v is not None)
        print(f"[yf_cache] OK — {successes}/{len(symbols)} tickers loaded")

    except Exception as e:
        print(f"[yf_cache] Bulk download error: {e}")
        # result stays all-None — route files handle None gracefully

    return result
