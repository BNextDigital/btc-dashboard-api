"""
shared/yf_core_cache.py — tiny yFinance cache for BTC-only fast collectors.

The 15-minute collector only needs BTC spot daily closes and CME BTC futures
for basis calculations. Keeping these two symbols separate prevents a single
basis lookup from downloading the full ~92-ticker cross-asset universe.
"""

from __future__ import annotations

import gc
import threading
import time
from datetime import datetime

import pandas as pd
import yfinance as yf

from shared.memory_utils import release_memory


CORE_TICKERS = {
    "btc_usd": "BTC-USD",
    "btc_futures": "BTC=F",
}

N_DAYS = 252
CACHE_TTL = 15 * 60

_cache = {
    "data": None,
    "ts": 0.0,
    "updated_at": None,
}
_lock = threading.Lock()


def get_series(key: str) -> pd.Series | None:
    if key not in CORE_TICKERS:
        return None

    data = _get_or_refresh()
    entry = data.get(key)
    if entry is None:
        return None

    return pd.Series(
        entry["values"],
        index=pd.to_datetime(entry["dates"]),
        name=key,
    )


def cache_age_seconds() -> float:
    return time.time() - _cache["ts"]


def cache_updated_at() -> str | None:
    return _cache["updated_at"]


def flush() -> None:
    with _lock:
        _cache["data"] = None
        _cache["ts"] = 0.0
        _cache["updated_at"] = None
    gc.collect()


def _is_stale() -> bool:
    return (
        _cache["data"] is None
        or (time.time() - _cache["ts"]) > CACHE_TTL
    )


def _get_or_refresh() -> dict:
    if not _is_stale():
        return _cache["data"]

    with _lock:
        if not _is_stale():
            return _cache["data"]

        data = _fetch()
        _cache["data"] = data
        _cache["ts"] = time.time()
        _cache["updated_at"] = datetime.utcnow().isoformat() + "Z"
        return data


def _fetch() -> dict[str, dict | None]:
    result: dict[str, dict | None] = {key: None for key in CORE_TICKERS}
    symbols = list(CORE_TICKERS.values())
    key_by_symbol = {symbol: key for key, symbol in CORE_TICKERS.items()}

    raw = None
    close = None
    try:
        print(f"[yf_core] Fetching {len(symbols)} BTC tickers ({N_DAYS}d)…")
        raw = yf.download(
            symbols,
            period=f"{N_DAYS}d",
            auto_adjust=True,
            progress=False,
            threads=2,
        )
        close = raw["Close"] if "Close" in raw.columns else raw

        for symbol, key in key_by_symbol.items():
            if symbol not in close.columns:
                continue
            series = close[symbol].dropna()
            if len(series) < 5:
                continue
            result[key] = {
                "values": [float(v) for v in series.values],
                "dates": [str(d.date()) for d in series.index],
            }

        successes = sum(value is not None for value in result.values())
        print(f"[yf_core] OK — {successes}/{len(symbols)} tickers loaded")
    except Exception as exc:
        print(f"[yf_core] download error: {exc}")
    finally:
        try:
            del raw, close
        except Exception:
            pass
        release_memory("yf_core refresh")

    return result
