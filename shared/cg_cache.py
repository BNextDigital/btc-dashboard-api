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
import os, time, threading
import requests

# ── Config ─────────────────────────────────────────────────────────────────────

CG_BASE = "https://api.coingecko.com/api/v3"
TTL     = 300    # 5 min — consistent with route-file cache TTLs

_lock   = threading.Lock()   # FastAPI is threaded; lock prevents duplicate fetches on cache miss

# ── Per-endpoint caches ────────────────────────────────────────────────────────

_derivatives_cache: dict = {"data": None, "ts": 0.0}
_global_cache:      dict = {"data": None, "ts": 0.0}


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
    headers = {}
    key = os.getenv("COINGECKO_API_KEY", "")
    if key:
        headers["x-cg-pro-api-key"] = key
    r = requests.get(f"{CG_BASE}{path}", params=params or {}, headers=headers, timeout=15)
    if not r.ok:
        # 429 = rate limit. Callers handle stale-cache fallback.
        print(f"[cg_cache] HTTP {r.status_code} for {path} — {r.text[:120]}")
        r.raise_for_status()
    return r.json()


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
    CoinGecko /global market data, cached for TTL seconds.
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
        try:
            resp = cg_request("/global")
            data = resp.get("data", {}) if isinstance(resp, dict) else {}
            _global_cache["data"] = data
            _global_cache["ts"]   = now
            print("[cg_cache] global refreshed")
            return data
        except Exception as e:
            print(f"[cg_cache] global fetch error: {e}")
            if _global_cache["data"] is not None:
                return _global_cache["data"]
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
                    # Returns list of tickers; we filter for our target pair.
                    data = cg_request(
                        f"/exchanges/{exchange_id}/tickers",
                        params={"coin_ids": "bitcoin", "depth": "false"},
                    )
                    tickers = data.get("tickers", []) if isinstance(data, dict) else []

                    # Match on base/target — CoinGecko uses "BTC" / "USD" or "USDT"
                    base_want, quote_want = target_pair.split("/")
                    match = next(
                        (t for t in tickers
                         if t.get("base", "").upper()   == base_want.upper()
                         and t.get("target", "").upper() == quote_want.upper()),
                        None,
                    )
                    if match:
                        price = match.get("last")   # last trade price as float

                    print(f"[cg_cache] premium: {label} {target_pair} = {price}")

                except Exception as e:
                    print(f"[cg_cache] premium fetch error ({exchange_id}): {e}")

                result[side].append({
                    **pair_cfg,
                    "price": float(price) if price is not None else None,
                })

        _premium_cache["data"] = result
        _premium_cache["ts"]   = now
        return result


def get_north_american_premium() -> dict:
    """
    Computes the North American BTC premium from PREMIUM_PAIRS.

    Returns:
    {
        "premium_usd":   12.55,          # onshore - offshore (USD)
        "premium_bps":   1.2,            # basis points
        "premium_pct":   0.012,          # as a raw float (0.012 = 0.012%)
        "onshore_price": 105432.10,      # avg of all onshore venues
        "offshore_price": 105418.55,     # avg of all offshore venues
        "onshore_label": "Coinbase",     # single label or "Avg (N)" if multiple
        "offshore_label": "Binance",
        "pairs": { ... }                 # raw per-exchange prices for debug
    }
    Returns None if either side has no valid price.
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
    now = time.time()
    deriv    = _derivatives_cache
    glob     = _global_cache
    premium  = _premium_cache
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
