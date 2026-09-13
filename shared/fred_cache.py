"""
shared/fred_cache.py — Centralised FRED data layer

One per-series cache replaces 4 copy-pasted _fred() fetcher functions
spread across macro_routes.py, liquidity_routes.py, forex_routes.py,
growth_inflation_routes.py, and leading_routes.py.

─────────────────────────────────────────────────────────────────────
SETUP
  Same shared/ package as yf_cache.py — no extra install needed.

─────────────────────────────────────────────────────────────────────
USAGE IN ROUTE FILES

  from shared.fred_cache import get_series, get_series_df

  # Returns [(date_str, float), ...] oldest-first — same as current
  obs = get_series("BAMLH0A0HYM2")

  # Returns pd.Series(values, index=dates) — for routes using pandas
  s = get_series_df("T10YIE")

─────────────────────────────────────────────────────────────────────
TO ADD A NEW SERIES
  1. Add one entry to ALL_SERIES below (assigns frequency → TTL).
  2. Call get_series("YOUR_SERIES_ID") anywhere.
  3. Done.

─────────────────────────────────────────────────────────────────────
TTL POLICY
  FRED updates on a per-series schedule. Polling faster wastes API
  quota and adds latency for no gain.

  daily     →  1 hour   (rates, spreads, breakevens, WTI, SOFR)
  weekly    →  4 hours  (claims, M2, reserves, TGA, RRP, gasoline)
  monthly   → 12 hours  (CPI, PCE, payrolls, GDP, sentiment, ISM)
  quarterly → 24 hours  (GDP revision)

─────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import requests

# ── Config ────────────────────────────────────────────────────────────────────

FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_CACHE_DIR = Path(
    os.getenv(
        "FRED_CACHE_DIR",
        str(Path(os.getenv("DATA_DIR", "/app/data")) / "fred_cache"),
    )
)

# ── Frequency type ────────────────────────────────────────────────────────────

Frequency = Literal["daily", "weekly", "monthly", "quarterly"]

TTL_BY_FREQ: dict[Frequency, int] = {
    "daily":     3_600,       #  1 hour
    "weekly":   14_400,       #  4 hours
    "monthly":  43_200,       # 12 hours
    "quarterly": 86_400,      # 24 hours
}

# Default observation window per frequency
# (how many obs to request — FRED returns newest-first then we reverse)
N_OBS_BY_FREQ: dict[Frequency, int] = {
    "daily":      365,    # ~1 year of trading days
    "weekly":     156,    # 3 years of weekly data
    "monthly":     60,    # 5 years of monthly data
    "quarterly":   20,    # 5 years of quarterly data
}

# ── Master series registry ────────────────────────────────────────────────────
#
# series_id  : FRED series identifier (used as cache key and API param)
# frequency  : controls TTL and default observation count
#
ALL_SERIES: dict[str, Frequency] = {

    # ── Yield curve (daily) ───────────────────────────────────────────────
    "DGS1MO":           "daily",    # 1-month Treasury
    "DGS3MO":           "daily",    # 3-month Treasury
    "DGS6MO":           "daily",    # 6-month Treasury
    "DGS1":             "daily",    # 1-year Treasury
    "DGS2":             "daily",    # 2-year Treasury
    "DGS3":             "daily",    # 3-year Treasury
    "DGS5":             "daily",    # 5-year Treasury
    "DGS7":             "daily",    # 7-year Treasury
    "DGS10":            "daily",    # 10-year Treasury
    "DGS20":            "daily",    # 20-year Treasury
    "DGS30":            "daily",    # 30-year Treasury
    "DFII10":           "daily",  # 10Y TIPS real yield
    "DFF":              "daily",  # daily Effective Fed Funds Rate

    # ── Credit spreads (daily) ────────────────────────────────────────────
    "BAMLH0A0HYM2":     "daily",    # HY OAS (high-yield spread)
    "BAMLC0A0CM":       "daily",    # IG OAS (investment-grade spread)

    # ── Breakeven inflation (daily) ───────────────────────────────────────
    "T5YIE":            "daily",    # 5Y breakeven — growth + leading routes
    "T10YIE":           "daily",    # 10Y breakeven — growth + leading routes
    "T5YIFR":           "daily",    # 5Y5Y forward breakeven

    # ── Short-term rates (daily) ──────────────────────────────────────────
    "SOFR":             "daily",    # Secured Overnight Financing Rate
    "FEDFUNDS":         "daily",    # Effective Fed Funds Rate (monthly release, daily key)

    # ── FX (daily) ────────────────────────────────────────────────────────
    "DTWEXBGS":         "daily",    # Broad trade-weighted USD index (forex route)

    # ── Energy prices (daily / weekly) ───────────────────────────────────
    "DCOILWTICO":       "daily",    # WTI crude spot (growth route)
    "GASREGCOVW":       "weekly",   # US retail gasoline price (weekly)

    # ── Liquidity — Fed balance sheet (weekly) ────────────────────────────
    "WRESBAL":          "weekly",   # Reserve balances at Fed
    "WTREGEN":          "weekly",   # Treasury General Account
    "RRPONTSYD":        "daily",    # Overnight Reverse Repo (daily)
    "M2SL":             "weekly",   # M2 money supply

    # ── Employment (weekly) ───────────────────────────────────────────────
    "ICSA":             "weekly",   # Initial jobless claims
    "CCSA":             "weekly",   # Continuing claims

    # ── Employment (monthly) ─────────────────────────────────────────────
    "PAYEMS":           "monthly",  # Non-farm payrolls
    "UNRATE":           "monthly",  # Unemployment rate
    "JTSJOL":           "monthly",  # JOLTS job openings
    "CES0500000003":    "monthly",  # Average hourly earnings

    # ── Inflation (monthly) ───────────────────────────────────────────────
    "CPIAUCSL":         "monthly",  # CPI all items
    "CPILFESL":         "monthly",  # Core CPI (ex food & energy)
    "PCEPI":            "monthly",  # PCE price index
    "PCEPILFE":         "monthly",  # Core PCE — Fed's primary target
    "PPIFID":           "monthly",  # PPI final demand
    "CUSR0000SEHA":     "monthly",  # CPI rent of primary residence
    "CUSR0000SEHC":     "monthly",  # CPI owners' equivalent rent

    # ── Activity / sentiment (monthly) ───────────────────────────────────
    "RSAFS":            "monthly",  # Retail sales
    "UMCSENT":          "monthly",  # U Michigan consumer sentiment
    "MICH":             "monthly",  # U Michigan inflation expectations (1Y)
    "ISPMAN":             "monthly",  # ISM Manufacturing PMI

    # ── GDP (quarterly) ───────────────────────────────────────────────────
    "A191RL1Q225SBEA":  "quarterly", # Real GDP growth rate (annualised)
}

# ── Per-series cache ──────────────────────────────────────────────────────────
#
# Each series gets its own entry:
#   { "data": [(date_str, float), ...] | None, "ts": float, "lock": Lock }
#
_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()   # guards _cache dict creation only


def _get_entry(series_id: str) -> dict:
    """Get or create a per-series cache entry. Thread-safe."""
    if series_id not in _cache:
        with _cache_lock:
            if series_id not in _cache:
                _cache[series_id] = {
                    "data": None,
                    "ts":   0.0,
                    "requested_n": 0,
                    "lock": threading.Lock(),
                }
    return _cache[series_id]


# ── Public API ────────────────────────────────────────────────────────────────

def get_series(
    series_id: str,
    n_obs: int | None = None,
    *,
    force_refresh: bool = False,
) -> list[tuple[str, float]]:
    """
    Return [(date_str, float), ...] oldest-first for a FRED series.
    Refreshes cache if stale based on the series' frequency TTL.
    Returns [] if FRED_API_KEY is missing or the request fails.

    Example:
        from shared.fred_cache import get_series
        hy_oas = get_series("BAMLH0A0HYM2")
        breakeven_10y = get_series("T10YIE")
    """
    return _get_or_refresh(series_id, n_obs, force_refresh=force_refresh)


def get_series_df(
    series_id: str,
    n_obs: int | None = None,
    *,
    force_refresh: bool = False,
):
    """
    Return a pd.Series(values, index=date_strings) for routes using pandas.
    Returns None if no data available.

    Example:
        from shared.fred_cache import get_series_df
        import pandas as pd
        s = get_series_df("DGS10")   # pd.Series of 10Y yield
    """
    import pandas as pd
    obs = _get_or_refresh(series_id, n_obs, force_refresh=force_refresh)
    if not obs:
        return None
    dates, vals = zip(*obs)
    return pd.Series(list(vals), index=list(dates), name=series_id)


def flush(series_id: str | None = None) -> None:
    """
    Flush one series or all series from cache.
    Call from /cache/flush endpoints.

    Example:
        from shared.fred_cache import flush
        flush("T10YIE")   # flush one
        flush()           # flush all
    """
    if series_id is not None:
        entry = _cache.get(series_id)
        if entry:
            with entry["lock"]:
                entry["data"] = None
                entry["ts"]   = 0.0
                entry["requested_n"] = 0
        try:
            _cache_path(series_id).unlink()
        except OSError:
            pass
    else:
        with _cache_lock:
            for entry in _cache.values():
                entry["data"] = None
                entry["ts"]   = 0.0
                entry["requested_n"] = 0
        try:
            for path in FRED_CACHE_DIR.glob("*.json"):
                path.unlink()
        except OSError:
            pass


def status() -> dict:
    """
    Return cache status for every known series.
    Useful for a /health or /fred-cache/status debug endpoint.

    Returns:
        {
          "BAMLH0A0HYM2": {"age_s": 42, "n_obs": 250, "ttl": 3600, "stale": False},
          ...
        }
    """
    now = time.time()
    out = {}
    for sid, freq in ALL_SERIES.items():
        entry = _cache.get(sid)
        ttl   = TTL_BY_FREQ[freq]
        if entry and entry["data"] is not None:
            age   = round(now - entry["ts"])
            out[sid] = {
                "age_s":  age,
                "n_obs":  len(entry["data"]),
                "ttl":    ttl,
                "stale":  age > ttl,
                "freq":   freq,
            }
        else:
            out[sid] = {"age_s": None, "n_obs": 0, "ttl": ttl, "stale": True, "freq": freq}
    return out


# ── Internal ──────────────────────────────────────────────────────────────────

def _ttl(series_id: str) -> int:
    freq = ALL_SERIES.get(series_id, "daily")
    return TTL_BY_FREQ[freq]


def _default_n_obs(series_id: str) -> int:
    freq = ALL_SERIES.get(series_id, "daily")
    return N_OBS_BY_FREQ[freq]


def _is_stale(entry: dict, series_id: str, requested_n: int) -> bool:
    return (
        entry["data"] is None
        or (time.time() - entry["ts"]) > _ttl(series_id)
        or int(entry.get("requested_n", 0)) < requested_n
    )


def _cache_path(series_id: str) -> Path:
    safe_id = "".join(char for char in series_id if char.isalnum() or char in "-_")
    return FRED_CACHE_DIR / f"{safe_id}.json"


def _load_persistent(series_id: str) -> dict | None:
    try:
        with _cache_path(series_id).open("r", encoding="utf-8") as cache_file:
            payload = json.load(cache_file)
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            return None
        return {
            "data": [tuple(item) for item in payload["data"]],
            "ts": float(payload.get("ts", 0)),
            "requested_n": int(payload.get("requested_n", 0)),
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _write_persistent(series_id: str, entry: dict) -> None:
    FRED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(series_id)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = {
        "ts": entry["ts"],
        "requested_n": entry.get("requested_n", 0),
        "data": entry["data"],
    }
    try:
        with tmp_path.open("w", encoding="utf-8") as cache_file:
            json.dump(payload, cache_file, ensure_ascii=False, separators=(",", ":"))
            cache_file.flush()
            os.fsync(cache_file.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def _get_or_refresh(
    series_id: str,
    n_obs: int | None,
    *,
    force_refresh: bool = False,
) -> list[tuple[str, float]]:
    entry = _get_entry(series_id)
    n = n_obs or _default_n_obs(series_id)

    if entry["data"] is None:
        persisted = _load_persistent(series_id)
        if persisted is not None:
            entry.update(persisted)

    if not force_refresh and not _is_stale(entry, series_id, n):
        return entry["data"]

    with entry["lock"]:
        # Re-check inside lock
        if not force_refresh and not _is_stale(entry, series_id, n):
            return entry["data"]

        stale_data = entry["data"]
        data = _fetch(series_id, n)
        if data:
            entry["data"] = data
            entry["ts"] = time.time()
            entry["requested_n"] = n
            _write_persistent(series_id, entry)
        elif stale_data is not None:
            print(f"[fred_cache] {series_id}: returning persisted stale data")
            entry["data"] = stale_data
            # Suppress duplicate retries from other routes in this process.
            # The persisted timestamp remains unchanged, so the next collector
            # process still attempts a real refresh.
            entry["ts"] = time.time()
            entry["requested_n"] = n
        else:
            entry["data"] = []
            entry["ts"] = time.time()
            entry["requested_n"] = n

    return entry["data"]


def _fetch(series_id: str, n_obs: int) -> list[tuple[str, float]]:
    """
    Single FRED API call for one series.
    Returns [(date_str, float), ...] oldest-first. Empty list on failure.
    FRED returns "." for missing values (weekends/holidays) — silently skipped.
    """
    if not FRED_API_KEY:
        print(f"[fred_cache] FRED_API_KEY not set — cannot fetch {series_id}")
        return []

    try:
        r = requests.get(
            FRED_BASE,
            params={
                "series_id":  series_id,
                "api_key":    FRED_API_KEY,
                "file_type":  "json",
                "sort_order": "desc",      # newest first so limit cuts the tail
                "limit":      n_obs,
            },
            timeout=15,
        )
        r.raise_for_status()

        obs    = r.json().get("observations", [])
        result = []
        for o in reversed(obs):            # reverse → oldest-first
            try:
                result.append((o["date"], float(o["value"])))
            except (ValueError, KeyError):
                pass                       # "." missing value → skip

        print(f"[fred_cache] {series_id}: {len(result)} obs fetched")
        return result

    except Exception as e:
        print(f"[fred_cache] Error fetching {series_id}: {e}")
        return []
