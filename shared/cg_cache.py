"""
shared/cg_cache.py — Shared CoinGecko cache layer 
===================================================
Centralises CoinGecko endpoints called by more than one route file.

PROBLEM SOLVED:
  /derivatives returns ~3,000 tickers for all coins.
  main.py, eth_routes.py, and sol_routes.py each fetched it independently —
  three identical network calls on every cache miss, hitting the free-tier
  30 req/min limit and tripling bandwidth on every refresh cycle.

ENDPOINTS CONSOLIDATED HERE:
  /derivatives  — fetched once, all three route files filter from the same list
  /global       — global market data (stablecoin supply, BTC dominance)

ENDPOINTS THAT STAY IN EACH ROUTE FILE (coin-specific, can't share):
  /coins/{id}       — market data differs per coin
  /coins/{id}/ohlc  — OHLCV history differs per coin

USAGE:
  # Replace the private _cg() in each route file with the shared helper:
  from shared.cg_cache import cg_request as _cg

  # Replace each file's own /derivatives fetch with one shared call:
  from shared.cg_cache import get_weighted_funding_oi

  # In eth_routes.py / sol_routes.py:
  def fetch_eth_derivatives() -> dict:
      return get_weighted_funding_oi("ETH")   # ← was its own /derivatives call

  # In data_sources.py (BTC):
  def _fetch_coingecko_derivatives() -> list:
      return get_derivatives()                 # ← was its own /derivatives call

SETUP:
  Place this file at:  btc-dashboard-api/shared/cg_cache.py
  Create (if missing): btc-dashboard-api/shared/__init__.py
"""

from __future__ import annotations
import json
import os
import threading
import time
from pathlib import Path
import requests

# ── Config ─────────────────────────────────────────────────────────────────────

CG_BASE = "https://api.coingecko.com/api/v3"
TTL     = 300    # 5 min — consistent with route-file cache TTLs

_lock   = threading.Lock()   # FastAPI is threaded; lock prevents duplicate fetches on cache miss

# ── Per-endpoint caches ────────────────────────────────────────────────────────

_derivatives_cache: dict = {"data": None, "ts": 0.0}
_global_cache:      dict = {"data": None, "ts": 0.0}
_markets_cache:     dict = {"data": None, "ts": 0.0}

MARKET_ASSET_IDS = (
    "bitcoin",
    "ethereum",
    "solana",
    "tether",
    "usd-coin",
)

HISTORY_TTL = max(
    3600,
    int(os.getenv("COINGECKO_HISTORY_TTL_SECONDS", "14400")),
)
GLOBAL_TTL = max(
    900,
    int(os.getenv("COINGECKO_GLOBAL_TTL_SECONDS", "3600")),
)
_history_cache_path = Path(
    os.getenv(
        "COINGECKO_HISTORY_CACHE_PATH",
        str(
            Path(os.getenv("DATA_DIR", "/app/data"))
            / "coingecko_history_cache.json"
        ),
    )
)
_history_cache: dict[str, dict] | None = None


# ── Shared HTTP helper ────────────────────────────────────────────────────────
#
# Replaces the private _cg() / _cg_get() defined identically in:
#   main.py / data_sources.py, eth_routes.py, sol_routes.py
#
# Import and alias:
#   from shared.cg_cache import cg_request as _cg

def cg_request(path: str, params: dict = None) -> dict | list:
    """
    Single CoinGecko request helper — auth, logging, raise on error.
    All route files should import this instead of defining their own.
    """
    headers = {"User-Agent": "btc-dashboard/1.0"}
    key = os.getenv("COINGECKO_API_KEY", "")
    if key:
        # COINGECKO_API_KEY is the Demo key used throughout this project.
        # Sending it as a Pro key leaves Railway on CoinGecko's anonymous
        # shared-IP allowance, which is easily rate limited and blanks every
        # ETH/SOL CoinGecko-backed metric at once.
        headers["x-cg-demo-api-key"] = key
    r = requests.get(f"{CG_BASE}{path}", params=params or {}, headers=headers, timeout=15)
    if not r.ok:
        # 429 = rate limit. Callers handle stale-cache fallback.
        print(f"[cg_cache] HTTP {r.status_code} for {path} — {r.text[:120]}")
        r.raise_for_status()
    return r.json()


# ── /coins/markets — shared current state across dashboard assets ───────────

def get_asset_markets() -> dict[str, dict]:
    """Fetch BTC, ETH, SOL, USDT and USDC current state in one request."""
    now = time.time()

    with _lock:
        if (
            _markets_cache["data"] is not None
            and now - _markets_cache["ts"] < TTL
        ):
            return _markets_cache["data"]

        try:
            response = cg_request(
                "/coins/markets",
                params={
                    "vs_currency": "usd",
                    "ids": ",".join(MARKET_ASSET_IDS),
                    "price_change_percentage": "24h,7d,30d",
                    "sparkline": "false",
                },
            )
            if not isinstance(response, list):
                raise ValueError(f"unexpected response type: {type(response)}")

            data = {
                str(row.get("id")): row
                for row in response
                if isinstance(row, dict) and row.get("id")
            }
            _markets_cache["data"] = data
            _markets_cache["ts"] = now
            print(f"[cg_cache] asset markets refreshed — {len(data)} assets")
            return data
        except Exception as exc:
            print(f"[cg_cache] asset markets fetch error: {exc}")
            if _markets_cache["data"] is not None:
                age = int(now - _markets_cache["ts"])
                print(f"[cg_cache] returning stale asset markets (age {age}s)")
                return _markets_cache["data"]
            return {}


def get_asset_market(asset_id: str) -> dict:
    """Return one asset from the shared multi-asset market response."""
    value = get_asset_markets().get(asset_id, {})
    return value if isinstance(value, dict) else {}


def _load_history_cache() -> dict[str, dict]:
    global _history_cache
    if _history_cache is not None:
        return _history_cache

    try:
        with _history_cache_path.open("r", encoding="utf-8") as cache_file:
            data = json.load(cache_file)
        _history_cache = data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        _history_cache = {}

    return _history_cache


def _write_history_cache(data: dict[str, dict]) -> None:
    _history_cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _history_cache_path.with_name(
        f".{_history_cache_path.name}.{os.getpid()}.tmp"
    )
    try:
        with tmp_path.open("w", encoding="utf-8") as cache_file:
            json.dump(data, cache_file, ensure_ascii=False, separators=(",", ":"))
            cache_file.flush()
            os.fsync(cache_file.fileno())
        os.replace(tmp_path, _history_cache_path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def get_market_chart(asset_id: str, days: int = 30) -> dict:
    """
    Return a daily market chart with a disk-backed four-hour cache.

    Collector processes are intentionally disposable, so an in-memory TTL
    alone would refetch these mostly-daily series every 15 minutes.
    """
    now = time.time()
    key = f"market_chart:{asset_id}:{days}"

    with _lock:
        cache = _load_history_cache()
        cached = cache.get(key, {})
        if (
            isinstance(cached, dict)
            and isinstance(cached.get("data"), dict)
            and now - float(cached.get("ts", 0)) < HISTORY_TTL
        ):
            return cached["data"]

        try:
            response = cg_request(
                f"/coins/{asset_id}/market_chart",
                params={
                    "vs_currency": "usd",
                    "days": str(days),
                    "interval": "daily",
                },
            )
            if not isinstance(response, dict):
                raise ValueError(f"unexpected response type: {type(response)}")

            cache[key] = {"data": response, "ts": now}
            _write_history_cache(cache)
            print(f"[cg_cache] {asset_id} {days}d market chart refreshed")
            return response
        except Exception as exc:
            print(f"[cg_cache] {asset_id} market chart fetch error: {exc}")
            stale = cached.get("data") if isinstance(cached, dict) else None
            return stale if isinstance(stale, dict) else {}


# ── /derivatives — shared across BTC, ETH, SOL ───────────────────────────────

def get_derivatives() -> list[dict]:
    """
    All unexpired derivative tickers from CoinGecko, cached for TTL seconds.

    Returns the raw list — callers filter for their coin:
        btc = [d for d in get_derivatives() if d.get("base","").upper() == "BTC"]
        eth = [d for d in get_derivatives() if d.get("base","").upper() == "ETH"]
        sol = [d for d in get_derivatives() if d.get("base","").upper() == "SOL"]

    Or use get_weighted_funding_oi(coin) for the pre-computed result.
    """
    now = time.time()

    with _lock:
        if _derivatives_cache["data"] is not None and now - _derivatives_cache["ts"] < TTL:
            return _derivatives_cache["data"]
        try:
            data = cg_request("/derivatives", params={"include_tickers": "unexpired"})
            if not isinstance(data, list):
                raise ValueError(f"unexpected response type: {type(data)}")
            _derivatives_cache["data"] = data
            _derivatives_cache["ts"]   = now
            print(f"[cg_cache] derivatives refreshed — {len(data)} tickers")
            return data
        except Exception as e:
            print(f"[cg_cache] derivatives fetch error: {e}")
            if _derivatives_cache["data"] is not None:
                age = int(now - _derivatives_cache["ts"])
                print(f"[cg_cache] returning stale derivatives (age {age}s)")
                return _derivatives_cache["data"]
            return []   # all callers handle empty list gracefully


# Reference exchanges used for funding rate calculation.
# Matches BTC's data_sources.py REFERENCE_EXCHANGES whitelist.
# These three have the deepest OI and most reliable funding data across BTC, ETH, SOL.
REFERENCE_EXCHANGES = {"Binance (Futures)", "Bybit (Futures)", "OKX (Futures)"}


def get_weighted_funding_oi(coin: str) -> dict:
    """
    Filter the shared /derivatives cache for one coin and return OI-weighted
    funding rate + total open interest USD.

    Mirrors BTC's fetch_funding() logic in data_sources.py exactly:
      - Filters by index_id (not "base" — that field does not exist in this response)
      - Restricts to REFERENCE_EXCHANGES (Binance, Bybit, OKX)
      - Removes clamped boundary rates (CoinGecko caps at ±0.01 = ±1%)
      - OI-weighted average, not simple mean

    coin — "BTC" | "ETH" | "SOL" (case-insensitive)
    Returns: {"funding": float | None, "open_interest_usd": float | None}
    """
    coin = coin.upper()
    all_tickers = get_derivatives()

    valid = [
        t for t in all_tickers
        if t.get("index_id", "").upper() == coin           # correct field — not "base"
        and t.get("contract_type") == "perpetual"
        and t.get("market") in REFERENCE_EXCHANGES         # top-3 exchanges only
        and t.get("funding_rate") is not None
        and t.get("open_interest", 0) > 0
        # Note: BTC's data_sources.py filters != 0.01 as a clamp guard, but 0.01%/8h
        # is a valid real rate for SOL/ETH — removing that filter here.
    ]

    if not valid:
        return {"funding": None, "open_interest_usd": None}

    # OI-weighted funding rate (matches data_sources.py pattern)
    total_oi  = sum(float(t.get("open_interest") or 0) for t in valid)
    w_funding = (
        sum(float(t.get("funding_rate") or 0) * float(t.get("open_interest") or 0)
            for t in valid)
        / total_oi if total_oi else 0.0
    ) / 100   # CoinGecko returns funding_rate as %, convert to decimal

    # open_interest field confirmed from /derivatives response — no open_interest_usd
    return {"funding": w_funding, "open_interest_usd": total_oi or None}


# ── /global — stablecoin supply, BTC dominance ───────────────────────────────

def get_global() -> dict:
    """
    CoinGecko /global market data, cached across collector processes.
    Returns the inner `data` dict directly.

    Key fields:
        market_cap_percentage          — {"btc": 58.3, "eth": 12.1, ...}
        total_market_cap               — {"usd": 3.2e12}
        total_volume                   — {"usd": ...}
        active_cryptocurrencies        — int
        markets                        — int

    Usage in main.py / data_sources.py:
        from shared.cg_cache import get_global
        global_data = get_global()
        btc_dom  = global_data.get("market_cap_percentage", {}).get("btc")
        sol_dom  = global_data.get("market_cap_percentage", {}).get("sol")
    """
    now = time.time()

    with _lock:
        if _global_cache["data"] is not None and now - _global_cache["ts"] < TTL:
            return _global_cache["data"]

        cache = _load_history_cache()
        cached = cache.get("global", {})
        if (
            isinstance(cached, dict)
            and isinstance(cached.get("data"), dict)
            and now - float(cached.get("ts", 0)) < GLOBAL_TTL
        ):
            _global_cache["data"] = cached["data"]
            _global_cache["ts"] = now
            return cached["data"]

        try:
            resp = cg_request("/global")
            data = resp.get("data", {}) if isinstance(resp, dict) else {}
            _global_cache["data"] = data
            _global_cache["ts"]   = now
            cache["global"] = {"data": data, "ts": now}
            _write_history_cache(cache)
            print("[cg_cache] global refreshed")
            return data
        except Exception as e:
            print(f"[cg_cache] global fetch error: {e}")
            if _global_cache["data"] is not None:
                return _global_cache["data"]
            stale = cached.get("data") if isinstance(cached, dict) else None
            if isinstance(stale, dict):
                return stale
            return {}


# ── Exchange Spot Tickers — North American Premium ────────────────────────────
#
# MODULAR PAIR REGISTRY
# To add a new exchange pair: add one entry to each side.
# Keys must match CoinGecko exchange IDs (check /exchanges endpoint).
# "onshore"  = North American / regulated USD venues
# "offshore" = Global / USDT-denominated venues
#
# Current build: Coinbase Pro (gdax) vs Binance (binance)
# Future candidates:
#   onshore:  "kraken", "gemini", "bitstamp"
#   offshore: "bybit_spot", "okex", "gate"

PREMIUM_PAIRS: dict[str, list[dict]] = {
    "onshore": [
        {"exchange_id": "gdax",    "label": "Coinbase", "pair": "BTC/USD"},
    ],
    "offshore": [
        {"exchange_id": "binance", "label": "Binance",  "pair": "BTC/USDT"},
    ],
}

# Separate TTL for exchange tickers — these update frequently, but we're
# already on a 60s dashboard refresh, so 60s here avoids hammering CG free tier.
_PREMIUM_TTL = 60

_premium_cache: dict = {"data": None, "ts": 0.0}


def get_exchange_spot_prices() -> dict:
    """
    Fetch last trade price for each exchange in PREMIUM_PAIRS.
    Returns:
    {
        "onshore":  [{"label": "Coinbase", "exchange_id": "gdax",    "pair": "BTC/USD",  "price": 105432.10}],
        "offshore": [{"label": "Binance",  "exchange_id": "binance", "pair": "BTC/USDT", "price": 105418.55}],
    }
    Returns None values for any exchange that fails — caller handles gracefully.

    CACHE POLICY: only caches when at least one side returned a real price.
    Failed fetches are NOT cached so the next call retries immediately.
    """
    now = time.time()

    with _lock:
        if _premium_cache["data"] is not None and now - _premium_cache["ts"] < _PREMIUM_TTL:
            return _premium_cache["data"]

        result: dict[str, list] = {"onshore": [], "offshore": []}

        for side, pairs in PREMIUM_PAIRS.items():
            for pair_cfg in pairs:
                exchange_id = pair_cfg["exchange_id"]
                target_pair = pair_cfg["pair"]          # e.g. "BTC/USD"
                label       = pair_cfg["label"]

                price = None
                try:
                    # /exchanges/{id}/tickers?coin_ids=bitcoin&depth=false
                    # Returns {"name": "...", "tickers": [...]}
                    # Each ticker has top-level "base", "target", "last" fields —
                    # confirmed from live CoinGecko response 2026-07-16.
                    data    = cg_request(
                        f"/exchanges/{exchange_id}/tickers",
                        params={"coin_ids": "bitcoin", "depth": "false"},
                    )
                    tickers = data.get("tickers", []) if isinstance(data, dict) else []

                    # Match on base/target at top level (not nested in market{})
                    base_want, quote_want = target_pair.split("/")
                    match = next(
                        (t for t in tickers
                         if t.get("base",   "").upper() == base_want.upper()
                         and t.get("target", "").upper() == quote_want.upper()
                         and not t.get("is_anomaly", False)   # skip flagged anomaly tickers
                         and not t.get("is_stale",   False)), # skip stale tickers
                        None,
                    )
                    if match:
                        price = match.get("last")
                        print(f"[cg_cache] premium: {label} {target_pair} = {price} "
                              f"(stale={match.get('is_stale')}, anomaly={match.get('is_anomaly')})")
                    else:
                        print(f"[cg_cache] premium: {label} {target_pair} — no matching ticker "
                              f"(got {len(tickers)} tickers, bases: "
                              f"{list(set(t.get('base','') for t in tickers[:10]))})")

                except Exception as e:
                    print(f"[cg_cache] premium fetch error ({exchange_id}): {e}")

                result[side].append({
                    **pair_cfg,
                    "price": float(price) if price is not None else None,
                })

        # Only cache if at least one side got a real price.
        # A fully-null result means the fetch failed (rate limit, network, etc.)
        # and should not be stored — next call will retry immediately.
        any_price = any(
            e["price"] is not None
            for side in result.values()
            for e in side
        )
        if any_price:
            _premium_cache["data"] = result
            _premium_cache["ts"]   = now
            print(f"[cg_cache] premium cache updated")
        else:
            print(f"[cg_cache] premium: all prices null — skipping cache, will retry next call")

        return result


def get_north_american_premium() -> dict:
    """
    Computes the North American BTC premium from PREMIUM_PAIRS.

    Returns:
    {
        "premium_usd":    12.55,         # onshore - offshore (USD)
        "premium_bps":    1.2,           # basis points
        "premium_pct":    0.012,         # as a raw float (0.012 = 0.012%)
        "onshore_price":  105432.10,     # avg of all onshore venues
        "offshore_price": 105418.55,     # avg of all offshore venues
        "onshore_label":  "Coinbase",    # single label or "Avg (N)" if multiple
        "offshore_label": "Binance",
        "pairs": { ... }                 # raw per-exchange prices for debug
    }
    Returns None values if either side has no valid price.
    """
    raw = get_exchange_spot_prices()

    def avg_price(entries: list[dict]) -> tuple[float | None, str]:
        valid = [e for e in entries if e["price"] is not None]
        if not valid:
            return None, "—"
        avg   = sum(e["price"] for e in valid) / len(valid)
        label = valid[0]["label"] if len(valid) == 1 else f"Avg ({len(valid)})"
        return avg, label

    onshore_price,  onshore_label  = avg_price(raw["onshore"])
    offshore_price, offshore_label = avg_price(raw["offshore"])

    if onshore_price is None or offshore_price is None:
        return {
            "premium_usd":    None,
            "premium_bps":    None,
            "premium_pct":    None,
            "onshore_price":  onshore_price,
            "offshore_price": offshore_price,
            "onshore_label":  onshore_label,
            "offshore_label": offshore_label,
            "pairs":          raw,
            "error":          "price unavailable for one or both sides",
        }

    premium_usd = onshore_price - offshore_price
    premium_pct = (premium_usd / offshore_price) * 100   # in percent
    premium_bps = premium_pct * 100                       # basis points

    return {
        "premium_usd":    round(premium_usd, 2),
        "premium_bps":    round(premium_bps, 2),
        "premium_pct":    round(premium_pct, 4),
        "onshore_price":  round(onshore_price, 2),
        "offshore_price": round(offshore_price, 2),
        "onshore_label":  onshore_label,
        "offshore_label": offshore_label,
        "pairs":          raw,
    }


# ── Cache status — wire to /health or /cache/status endpoint ─────────────────

def cache_status() -> dict:
    """
    Snapshot of cache health. Add to your /health endpoint:

        from shared.cg_cache import cache_status as cg_status
        @app.get("/cache/status")
        def get_cache_status():
            return {"cg": cg_status(), ...}
    """
    now     = time.time()
    deriv   = _derivatives_cache
    glob    = _global_cache
    premium = _premium_cache
    return {
        "derivatives": {
            "loaded":  deriv["data"] is not None,
            "tickers": len(deriv["data"]) if deriv["data"] else 0,
            "age_s":   int(now - deriv["ts"]) if deriv["ts"] else None,
            "stale":   bool(deriv["ts"] and now - deriv["ts"] > TTL),
        },
        "global": {
            "loaded": glob["data"] is not None,
            "age_s":  int(now - glob["ts"]) if glob["ts"] else None,
            "stale":  bool(glob["ts"] and now - glob["ts"] > TTL),
        },
        "premium": {
            "loaded": premium["data"] is not None,
            "age_s":  int(now - premium["ts"]) if premium["ts"] else None,
            "stale":  bool(premium["ts"] and now - premium["ts"] > _PREMIUM_TTL),
        },
        "ttl_s": TTL,
    }
