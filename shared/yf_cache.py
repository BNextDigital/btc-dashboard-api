"""
shared/yf_cache.py — Centralised yFinance data layer

One bulk yf.download() call covers every dashboard route.
All route files call get_series(key) instead of fetching independently.

─────────────────────────────────────────────────────────────────────
MEMORY FIX (July 2026)
  Previously the cache stored pd.Series objects directly. pandas Series
  carry significant overhead (~10–20× the raw float64 array size) and
  Python's GC rarely returns that memory to the OS between refreshes,
  causing steady RAM growth over days/weeks.

  Fix: _fetch() now converts each Series to a plain Python list[float]
  before storing. get_series() wraps it back into a pd.Series on the
  way out, so all callers are unaffected. The cache dict itself holds
  only lightweight Python lists between refresh cycles.

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
  Storage: plain list[float] internally — pd.Series reconstructed on read
  Thread : a lock prevents simultaneous redundant downloads; yFinance uses 4 workers
  Warmup : call warm_cache() from main.py startup to pre-fetch on boot

─────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import gc
import threading
import time
from datetime import datetime

import pandas as pd
import yfinance as yf

from shared.memory_utils import release_memory

# ── Master ticker registry ────────────────────────────────────────────────────
#
# key          : short string used by route files (get_series("vix"))
# yf_symbol    : ticker string passed to yf.download()
#
ALL_TICKERS: dict[str, str] = {

    # ── US Equity Indices ─────────────────────────────────────────────────
    "spx":      "^GSPC",
    "nasdaq":   "^IXIC",
    "qqq":      "QQQ",
    "iwm":      "^RUT",
    "dji":      "^DJI",

    # ── Broad Market ETFs ─────────────────────────────────────────────────
    "spy":      "SPY",
    "rsp":      "RSP",
    "tlt":      "TLT",
    "ief":      "IEF",

    # ── Technology — ETFs ─────────────────────────────────────────────────
    "xlk":      "XLK",
    "soxx":     "SOXX",
    "smh":      "SMH",
    "igv":      "IGV",
    "skyy":     "SKYY",
    "xlc":      "XLC",

    # ── Technology — Individual Names ─────────────────────────────────────
    "nvda":     "NVDA",
    "amd":      "AMD",
    "arm":      "ARM",
    "smci":     "SMCI",
    "aapl":     "AAPL",
    "meta":     "META",
    "googl":    "GOOGL",
    "tsla":     "TSLA",
    "msft":     "MSFT",

    # ── Financials — ETFs ─────────────────────────────────────────────────
    "xlf":      "XLF",
    "kbe":      "KBE",
    "kre":      "KRE",
    "iai":      "IAI",
    "kie":      "KIE",
    "ipay":     "IPAY",

    # ── Financials — Large Banks ───────────────────────────────────────────
    "jpm":      "JPM",
    "bac":      "BAC",
    "wfc":      "WFC",
    "c":        "C",
    "gs":       "GS",
    "ms":       "MS",

    # ── Financials — Regional Banks ────────────────────────────────────────
    "wal":      "WAL",
    "zion":     "ZION",

    # ── Financials — Payments & Credit ────────────────────────────────────
    "v":        "V",
    "ma":       "MA",
    "cof":      "COF",
    "axp":      "AXP",

    # ── Healthcare — ETFs ─────────────────────────────────────────────────
    "xlv":      "XLV",
    "xbi":      "XBI",

    # ── Industrials — ETFs ────────────────────────────────────────────────
    "xli":      "XLI",
    "iyt":      "IYT",

    # ── Energy — ETFs ─────────────────────────────────────────────────────
    "xle":      "XLE",
    "oih":      "OIH",

    # ── Materials — ETFs ──────────────────────────────────────────────────
    "xlb":      "XLB",

    # ── Consumer — ETFs ───────────────────────────────────────────────────
    "xly":      "XLY",
    "xlp":      "XLP",
    "xrt":      "XRT",
    "glux":     "GLUX",

    # ── Real Estate — ETFs ────────────────────────────────────────────────
    "xlre":     "XLRE",
    "vnq":      "VNQ",

    # ── Utilities — ETFs ──────────────────────────────────────────────────
    "xlu":      "XLU",

    # ── Credit ETFs ───────────────────────────────────────────────────────
    "hyg":      "HYG",
    "lqd":      "LQD",

    # ── Volatility ────────────────────────────────────────────────────────
    "vix":      "^VIX",
    "vxn":      "^VXN",

    # ── US Treasury Yields ────────────────────────────────────────────────
    "yield_1y": "^IRX",
    "yield_5y": "^FVX",
    "yield_10y":"^TNX",

    # ── FX — Major Pairs ──────────────────────────────────────────────────
    "dxy":      "DX-Y.NYB",
    "eurusd":   "EURUSD=X",
    "usdjpy":   "JPY=X",
    "usdcnh":   "CNY=X",

    # ── FX — Emerging Markets ─────────────────────────────────────────────
    "usdbrl":   "BRL=X",
    "usdmxn":   "MXN=X",
    "usdinr":   "INR=X",
    "usdkrw":   "KRW=X",
    "usdzar":   "ZAR=X",

    # ── Korea / Asia ──────────────────────────────────────────────────────
    "ewy":      "EWY",

    # ── Energy Futures ────────────────────────────────────────────────────
    "wti":      "CL=F",
    "brent":    "BZ=F",
    "natgas":   "NG=F",
    "gasoline": "RB=F",

    # ── Metals Futures ────────────────────────────────────────────────────
    "gold":     "GC=F",
    "silver":   "SI=F",
    "copper":   "HG=F",
    "platinum": "PL=F",

    # ── Grain Futures ─────────────────────────────────────────────────────
    "wheat":    "ZW=F",
    "corn":     "ZC=F",
    "soybeans": "ZS=F",

    # ── Crypto ────────────────────────────────────────────────────────────
    "btc_usd":      "BTC-USD",
    "btc_futures":  "BTC=F",

    # ── Crypto Proxy Stocks ───────────────────────────────────────────────
    "mstr":     "MSTR",
    "coin":     "COIN",
    "hood":     "HOOD",
    "mara":     "MARA",
    "pypl":     "PYPL",
    "xyz":      "XYZ",
}

# ── Lookback ──────────────────────────────────────────────────────────────────
#
# 252 trading days (1 trading year) is enough for:
#   - 200d SMA with a small buffer
#   - 252d percentile rank
#   - 90d percentile with comfortable headroom
#
# Previously 300d — trimmed to 252d to reduce the DataFrame allocation
# size on every refresh cycle (~16% smaller download).
#
N_DAYS = 252

# ── Cache ─────────────────────────────────────────────────────────────────────
#
# Internal storage is list[float] + list[str] (dates), NOT pd.Series.
# get_series() reconstructs a pd.Series on the way out so callers are
# unaffected. Storing lists instead of Series cuts per-key memory by
# ~10–20× and allows Python's GC to fully reclaim the previous cache
# on each refresh.
#

CACHE_TTL = 300   # 5 minutes

_cache: dict = {
    # {key: {"values": list[float], "dates": list[str]} | None}
    "data":       None,
    "ts":         0.0,
    "updated_at": None,
}
_lock = threading.Lock()


# ── Public API ────────────────────────────────────────────────────────────────

def get_series(key: str) -> pd.Series | None:
    """
    Return a pd.Series of daily Close prices for the given key.
    The Series is reconstructed from the internal list cache on each call —
    callers receive a normal pd.Series and need no changes.

    Returns None if the ticker failed or the key is unknown.
    """
    data = _get_or_refresh()
    entry = data.get(key)
    if entry is None:
        return None
    return pd.Series(entry["values"], index=pd.to_datetime(entry["dates"]), name=key)


def get_all() -> dict[str, pd.Series | None]:
    """
    Return the full {key: pd.Series | None} dict.
    Drop-in replacement for each route's _fetch_bulk() return value.
    Series objects are freshly reconstructed from the list cache.
    """
    data = _get_or_refresh()
    result: dict[str, pd.Series | None] = {}
    for key, entry in data.items():
        if entry is None:
            result[key] = None
        else:
            result[key] = pd.Series(
                entry["values"],
                index=pd.to_datetime(entry["dates"]),
                name=key,
            )
    return result


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
    gc.collect()


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


def _get_or_refresh() -> dict:
    """Return internal list cache if fresh; otherwise fetch under lock."""
    if not _is_stale():
        return _cache["data"]

    with _lock:
        # Re-check inside lock — another thread may have refreshed while we waited
        if not _is_stale():
            return _cache["data"]

        new_data = _fetch()

        # Explicitly delete old cache data before replacing so GC can
        # reclaim the previous allocation immediately, not on next cycle.
        old = _cache["data"]
        _cache["data"] = None
        del old
        gc.collect()

        _cache["data"]       = new_data
        _cache["ts"]         = time.time()
        _cache["updated_at"] = datetime.utcnow().isoformat() + "Z"

    return _cache["data"]


def _fetch() -> dict[str, dict | None]:
    """
    Single bulk yf.download() call for all tickers, capped at 4 download workers.

    Returns {key: {"values": list[float], "dates": list[str]} | None}.

    Storing as plain Python lists (not pd.Series / pd.DataFrame) means
    the large intermediate DataFrame from yf.download() can be fully
    garbage-collected after this function returns, rather than keeping
    300 × N_TICKERS floats alive in multiple Series wrappers.
    """
    result: dict[str, dict | None] = {k: None for k in ALL_TICKERS}

    symbols    = list(ALL_TICKERS.values())
    key_by_sym = {v: k for k, v in ALL_TICKERS.items()}

    try:
        print(f"[yf_cache] Fetching {len(symbols)} tickers ({N_DAYS}d)…")

        raw = yf.download(
            symbols,
            period=f"{N_DAYS}d",
            auto_adjust=True,
            progress=False,
            threads=4,
        )
        close = raw["Close"] if "Close" in raw.columns else raw

        for symbol, key in key_by_sym.items():
            if symbol in close.columns:
                s = close[symbol].dropna()
                if len(s) >= 5:
                    # Convert to plain lists immediately — drop the Series wrapper
                    result[key] = {
                        "values": [float(v) for v in s.values],
                        "dates":  [str(d.date()) for d in s.index],
                    }
                # else: stays None

        successes = sum(1 for v in result.values() if v is not None)
        print(f"[yf_cache] OK — {successes}/{len(symbols)} tickers loaded")

    except Exception as e:
        print(f"[yf_cache] Bulk download error: {e}")
        # result stays all-None — route files handle None gracefully

    finally:
        # Explicitly drop the large intermediate DataFrame, then return any
        # now-free native/glibc heap pages to Linux. This is important on
        # Railway because RSS can otherwise remain hundreds of MB above the
        # actual live working set after a yFinance/pandas refresh.
        try:
            del raw, close
        except NameError:
            pass
        release_memory("yf_cache refresh")

    return result
