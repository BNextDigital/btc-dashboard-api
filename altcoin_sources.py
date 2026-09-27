from __future__ import annotations
import os, time
from typing import Any
import requests

BINANCE_BASE = os.getenv("BINANCE_SPOT_BASE", "https://data-api.binance.vision")
CMC_BASE = "https://pro-api.coinmarketcap.com"
CMC_KEY = os.getenv("CMC_API_KEY", "").strip()
COINPAPRIKA_BASE = os.getenv("COINPAPRIKA_BASE", "https://api.coinpaprika.com")

STABLE_ASSETS = {"USDT","USDC","FDUSD","TUSD","USDP","DAI","BUSD","USDS","PYUSD","EUR","EURC","AEUR"}
DUPLICATES = {"BTC","WBTC","BTCB","WETH","STETH","BETH","WBETH"}
_cache: dict[str, tuple[float, Any]] = {}

def _get(url: str, *, params: dict | None = None, headers: dict | None = None, ttl: int = 0, timeout: int = 20):
    key = f"{url}|{sorted((params or {}).items())}"
    now = time.time()
    cached = _cache.get(key)
    if ttl and cached and now - cached[0] < ttl:
        return cached[1]
    r = requests.get(url, params=params or {}, headers=headers or {"User-Agent":"btc-dashboard/altcoins"}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if ttl:
        _cache[key] = (now, data)
    return data

def _f(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None

def fetch_binance_universe() -> list[dict]:
    payload = _get(f"{BINANCE_BASE}/api/v3/exchangeInfo", ttl=21600)
    out = []
    for row in payload.get("symbols", []):
        if row.get("status") != "TRADING" or row.get("quoteAsset") != "USDT":
            continue
        if row.get("isSpotTradingAllowed") is False:
            continue
        asset = str(row.get("baseAsset") or "").upper()
        symbol = str(row.get("symbol") or "").upper()
        reason = None
        if asset in STABLE_ASSETS:
            reason = "stablecoin_or_fiat"
        elif asset in DUPLICATES:
            reason = "wrapped_staked_or_reference_duplicate"
        elif asset.endswith(("UP","DOWN","BULL","BEAR")):
            reason = "leveraged_token"
        out.append({"symbol":symbol,"asset":asset,"quote_asset":"USDT","included":reason is None,"exclusion_reason":reason})
    return out

def fetch_binance_prices() -> dict[str, float]:
    rows = _get(f"{BINANCE_BASE}/api/v3/ticker/price", ttl=60)
    out = {}
    for row in rows if isinstance(rows, list) else []:
        try:
            out[str(row["symbol"]).upper()] = float(row["price"])
        except (KeyError, TypeError, ValueError):
            pass
    return out

def _cmc_get(path: str, *, params: dict | None = None, ttl: int = 300) -> dict:
    if CMC_KEY:
        url = f"{CMC_BASE}{path}"
        headers = {"Accept":"application/json","X-CMC_PRO_API_KEY":CMC_KEY}
    else:
        url = f"{CMC_BASE}/public-api{path}"
        headers = {"Accept":"application/json","User-Agent":"btc-dashboard/altcoins"}
    data = _get(url, params=params, headers=headers, ttl=ttl)
    if not isinstance(data, dict):
        raise ValueError("Unexpected CoinMarketCap response")
    status = data.get("status")
    if isinstance(status, dict) and status.get("error_code") not in (None,0,"0"):
        raise RuntimeError(status.get("error_message") or "CoinMarketCap error")
    return data

def fetch_cmc_global_latest() -> dict:
    data = (_cmc_get("/v1/global-metrics/quotes/latest").get("data") or {})
    usd = ((data.get("quote") or {}).get("USD") or {})
    return {
        "altcoin_market_cap": _f(usd.get("altcoin_market_cap")),
        "total_market_cap": _f(usd.get("total_market_cap")),
        "btc_dominance": _f(data.get("btc_dominance")),
        "eth_dominance": _f(data.get("eth_dominance")),
        "timestamp": usd.get("last_updated") or data.get("last_updated"),
        "source":"CoinMarketCap",
    }

def fetch_cmc_altseason_latest() -> dict:
    data = (_cmc_get("/v1/altcoin-season-index/latest", ttl=900).get("data") or {})
    return {"value":_f(data.get("altcoin_index")),"timestamp":data.get("snapshot_time"),"source":"CoinMarketCap"}

def fetch_cmc_index_latest(name: str) -> dict:
    name = name.lower()
    if name not in {"cmc20","cmc100"}:
        raise ValueError("Unsupported index")
    data = (_cmc_get(f"/v3/index/{name}-latest").get("data") or {})
    return {"value":_f(data.get("value")),"change_24h_pct":_f(data.get("value_24h_percentage_change")),"timestamp":data.get("last_update"),"source":"CoinMarketCap"}

def fetch_cmc_global_history(time_start: str) -> list[dict]:
    if not CMC_KEY:
        raise RuntimeError("CMC_API_KEY required for global historical metrics")
    data = (_cmc_get("/v1/global-metrics/quotes/historical", params={"time_start":time_start,"interval":"1d"}, ttl=14400).get("data") or {})
    out = []
    for row in data.get("quotes", []):
        usd = ((row.get("quote") or {}).get("USD") or {})
        out.append({"timestamp":row.get("timestamp"),"altcoin_market_cap":_f(usd.get("altcoin_market_cap")),"total_market_cap":_f(usd.get("total_market_cap")),"btc_dominance":_f(row.get("btc_dominance"))})
    return out

def fetch_coinpaprika_tickers() -> list[dict]:
    data = _get(f"{COINPAPRIKA_BASE}/v1/tickers", params={"quotes":"USD"}, ttl=900, timeout=30)
    return data if isinstance(data, list) else []
