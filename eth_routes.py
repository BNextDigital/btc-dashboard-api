"""
eth_routes.py — Ethereum Dashboard Backend Routes
==================================================
Drop-in addition to existing btc-dashboard-api/main.py.

INTEGRATION (2 lines in main.py):
    from eth_routes import eth_router
    app.include_router(eth_router)

DATA SOURCES (all free — no new API keys required):
    CoinGecko       — ETH price, volume, funding, OI, market cap (already integrated)
    DeFiLlama       — Ethereum DeFi TVL, L2 TVL, protocol breakdown, DEX volume
    yFinance        — CME ETH futures basis (ETH=F)
    beaconcha.in    — ETH staking / validator stats (free, no key)
    Cloudflare RPC  — Live gas price in gwei (public ETH RPC, no key)

METRIC SCHEMA (identical to BTC /metrics and SOL /sol/metrics):
    {current, d7, vs30d, percentile, alert, level, pattern}

OVERRIDES:
    Reads eth_overrides.json from DATA_DIR — same pattern as BTC/SOL.
    POST /eth/manual-override
    DELETE /eth/manual-override/{metric}
"""

from __future__ import annotations
import os, time, sqlite3, json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel
import requests

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False


# ── Config ─────────────────────────────────────────────────────────────────────

eth_router = APIRouter(prefix="/eth", tags=["Ethereum"])

DATA_DIR        = Path(os.getenv("DATA_DIR", "./data"))
ETH_DB_PATH     = DATA_DIR / "eth_history.db"
ETH_TVL_DB_PATH = DATA_DIR / "eth_tvl_history.db"
ETH_OVERRIDES   = DATA_DIR / "eth_overrides.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CG_BASE        = "https://api.coingecko.com/api/v3"
DEFILLAMA_BASE = "https://api.llama.fi"
BEACON_BASE    = "https://beaconcha.in/api/v1"
CF_ETH_RPC     = "https://cloudflare-eth.com"   # free public Ethereum RPC

# Ethereum L2s tracked for L2 TVL
L2_CHAINS = ["Arbitrum", "Base", "Optimism", "zkSync Era", "Linea", "Scroll", "Polygon zkEVM"]


# ── Database init ──────────────────────────────────────────────────────────────

def _init_dbs():
    with sqlite3.connect(ETH_DB_PATH) as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS eth_basis_history (
                date TEXT PRIMARY KEY,
                basis_pct REAL,
                spot_price REAL,
                futures_price REAL,
                days_to_expiry INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS eth_btc_ratio_history (
                date TEXT PRIMARY KEY,
                eth_btc_ratio REAL,
                eth_price REAL,
                btc_price REAL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS eth_gas_history (
                date TEXT PRIMARY KEY,
                gas_gwei REAL
            )
        """)
    with sqlite3.connect(ETH_TVL_DB_PATH) as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS eth_tvl_history (
                date TEXT PRIMARY KEY,
                mainnet_tvl REAL,
                l2_tvl REAL,
                dex_volume_24h REAL
            )
        """)

_init_dbs()


# ── CoinGecko helpers ──────────────────────────────────────────────────────────

def _cg(path: str, params: dict = None) -> dict:
    headers = {}
    key = os.getenv("COINGECKO_API_KEY", "")
    if key:
        headers["x-cg-pro-api-key"] = key
    r = requests.get(f"{CG_BASE}{path}", params=params or {}, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()


def fetch_eth_market() -> dict:
    """ETH price, volume, market cap, % changes — CoinGecko."""
    d  = _cg("/coins/ethereum", params={
        "localization": "false", "tickers": "false",
        "market_data": "true", "community_data": "false", "developer_data": "false",
    })
    md = d.get("market_data", {})
    return {
        "price_usd":      md.get("current_price", {}).get("usd"),
        "price_btc":      md.get("current_price", {}).get("btc"),   # ETH/BTC ratio
        "change_24h":     md.get("price_change_percentage_24h"),
        "change_7d":      md.get("price_change_percentage_7d"),
        "change_30d":     md.get("price_change_percentage_30d"),
        "volume_24h":     md.get("total_volume", {}).get("usd"),
        "market_cap":     md.get("market_cap", {}).get("usd"),
        "ath":            md.get("ath", {}).get("usd"),
        "ath_change_pct": md.get("ath_change_percentage", {}).get("usd"),
    }


def fetch_eth_derivatives() -> dict:
    """ETH perp funding rate + OI from CoinGecko derivatives."""
    try:
        tickers = _cg("/derivatives", params={"include_tickers": "unexpired"})
        eth     = [t for t in tickers if t.get("base", "").upper() == "ETH"]
        if not eth:
            return {"funding": None, "open_interest_usd": None}
        total_oi  = sum(float(t.get("open_interest_usd") or 0) for t in eth)
        w_funding = (
            sum(float(t.get("funding_rate") or 0) * float(t.get("open_interest_usd") or 0) for t in eth)
            / total_oi if total_oi else 0
        )
        return {"funding": w_funding, "open_interest_usd": total_oi}
    except Exception:
        return {"funding": None, "open_interest_usd": None}


def fetch_eth_ohlcv(days: int = 30) -> list[dict]:
    data = _cg("/coins/ethereum/ohlc", params={"vs_currency": "usd", "days": days})
    return [{"ts": d[0], "open": d[1], "high": d[2], "low": d[3], "close": d[4]} for d in data]


# ── DeFiLlama helpers ─────────────────────────────────────────────────────────

def fetch_eth_mainnet_tvl() -> dict:
    """Ethereum mainnet DeFi TVL from DeFiLlama."""
    try:
        history = requests.get(f"{DEFILLAMA_BASE}/v2/historicalChainTvl/Ethereum", timeout=15).json()
        if not history:
            return {"tvl_usd": None}
        hist     = sorted(history, key=lambda x: x.get("date", 0))
        cur      = hist[-1].get("tvl")
        d7_ago   = hist[-8].get("tvl")  if len(hist) >= 8  else None
        d30_ago  = hist[-31].get("tvl") if len(hist) >= 31 else None
        last_90  = [r["tvl"] for r in hist[-90:] if r.get("tvl")]
        pct      = _percentile(cur, last_90) if cur and last_90 else 50
        return {
            "tvl_usd": cur, "tvl_7d_ago": d7_ago, "tvl_30d_ago": d30_ago,
            "percentile": pct, "history_30d": last_90[-30:],
        }
    except Exception as e:
        return {"tvl_usd": None, "error": str(e)}


def fetch_eth_protocol_breakdown() -> list[dict]:
    """Top Ethereum mainnet protocols by TVL."""
    try:
        protos = requests.get(f"{DEFILLAMA_BASE}/protocols", timeout=15).json()
        eth    = [p for p in protos if "Ethereum" in (p.get("chains") or [])]
        eth.sort(key=lambda x: x.get("tvl", 0), reverse=True)
        return [{"name": p["name"], "tvl": p.get("tvl"), "category": p.get("category")}
                for p in eth[:10]]
    except Exception:
        return []


def fetch_eth_l2_tvl() -> dict:
    """
    Total Layer 2 TVL from DeFiLlama.
    Fetches /v2/chains and filters for known Ethereum L2s.
    """
    try:
        chains = requests.get(f"{DEFILLAMA_BASE}/v2/chains", timeout=15).json()
        l2s    = [c for c in chains if c.get("name") in L2_CHAINS]
        l2s.sort(key=lambda x: x.get("tvl", 0), reverse=True)
        total  = sum(c.get("tvl", 0) for c in l2s)
        return {
            "total_l2_tvl": total,
            "chains": [
                {"name": c["name"], "tvl": c.get("tvl")}
                for c in l2s
            ],
        }
    except Exception as e:
        return {"total_l2_tvl": None, "error": str(e)}


def fetch_eth_dex_volume() -> dict:
    """Ethereum DEX volume from DeFiLlama."""
    try:
        r = requests.get(f"{DEFILLAMA_BASE}/overview/dexs/Ethereum", timeout=15).json()
        return {"dex_volume_24h": r.get("total24h"), "dex_volume_7d": r.get("total7d")}
    except Exception:
        return {"dex_volume_24h": None, "dex_volume_7d": None}


# ── ETH Staking (beaconcha.in) ─────────────────────────────────────────────────

def fetch_eth_staking() -> dict:
    """
    ETH staking data from beaconcha.in free API.
    Returns validator count, total staked ETH, approximate staking rate.
    """
    try:
        resp = requests.get(f"{BEACON_BASE}/epoch/latest", timeout=10).json()
        data = resp.get("data", {})
        validators  = data.get("validatorscount", 0)
        staked_eth  = validators * 32  # 32 ETH per validator
        # ETH circulating supply ~120M (approximate)
        ETH_SUPPLY  = 120_000_000
        staking_rate = round(staked_eth / ETH_SUPPLY * 100, 1) if ETH_SUPPLY else None
        # Top validator pool concentration (Nakamoto proxy)
        return {
            "active_validators": validators,
            "staked_eth":        round(staked_eth / 1e6, 2),  # in millions
            "staking_rate_pct":  staking_rate,
        }
    except Exception as e:
        return {
            "active_validators": None,
            "staked_eth": None,
            "staking_rate_pct": None,
            "_mock": True,
            "error": str(e),
        }


# ── Gas Price (Cloudflare public RPC) ─────────────────────────────────────────

def fetch_eth_gas() -> dict:
    """
    Live ETH gas price from Cloudflare's free public Ethereum RPC.
    No API key required.
    """
    try:
        resp = requests.post(CF_ETH_RPC, json={
            "jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 1
        }, timeout=8).json()
        gas_hex  = resp.get("result", "0x0")
        gas_gwei = round(int(gas_hex, 16) / 1e9, 1)
        return {"gas_gwei": gas_gwei}
    except Exception as e:
        return {"gas_gwei": None, "error": str(e)}


# ── CME ETH Basis (yFinance) ──────────────────────────────────────────────────

def fetch_eth_cme_basis() -> dict:
    """
    CME ETH futures annualized basis via yFinance.
    Ticker: ETH=F (verify — may need to be ETH1=F for continuous contract)
    Same calculation as BTC basis in main.py.
    """
    if not HAS_YF:
        return {"basis_pct": None, "error": "yfinance not installed"}
    try:
        from datetime import datetime, timezone
        spot_price = yf.Ticker("ETH-USD").fast_info.last_price
        fut_ticker = yf.Ticker("ETH=F")
        fut_price  = fut_ticker.fast_info.last_price
        if not spot_price or not fut_price:
            return {"basis_pct": None, "error": "Price fetch returned None"}
        expiry_ts = fut_ticker.info.get("expireDate")
        if expiry_ts:
            expiry    = datetime.fromtimestamp(expiry_ts, tz=timezone.utc)
            days_left = max((expiry - datetime.now(timezone.utc)).days, 1)
        else:
            days_left = 30
        basis_pct = ((fut_price / spot_price) - 1) * (365 / days_left) * 100
        return {
            "basis_pct": basis_pct, "spot_price": spot_price,
            "futures_price": fut_price, "days_to_expiry": days_left,
        }
    except Exception as e:
        return {"basis_pct": None, "error": str(e)}


# ── Formatters ────────────────────────────────────────────────────────────────

def _percentile(value: float, series: list[float]) -> float:
    if not series or value is None:
        return 50.0
    return round(sum(1 for v in series if v < value) / len(series) * 100, 1)


def _fmt_usd(v, signed: bool = False) -> str:
    if v is None: return "—"
    prefix = ("+" if v >= 0 else "") if signed else ""
    abs_v  = abs(v)
    if abs_v >= 1e9:  return f"{prefix}${abs_v/1e9:.1f}B"  if v >= 0 else f"-${abs_v/1e9:.1f}B"
    if abs_v >= 1e6:  return f"{prefix}${abs_v/1e6:.0f}M"  if v >= 0 else f"-${abs_v/1e6:.0f}M"
    return f"{prefix}${v:,.0f}"


def _mock(metric_id: str) -> dict:
    return {
        "current": "—", "d7": "—", "vs30d": "—",
        "percentile": 50, "alert": "No data", "level": "none",
        "pattern": f"{metric_id}: data source not connected", "_mock": True,
    }


def format_eth_price_move(c24: float, c7d: float, c30d: float) -> dict:
    if c24 is None: return _mock("price_move")
    abs24 = abs(c24)
    # ETH is less volatile than SOL, thresholds slightly lower than BTC
    if abs24 > 7:   alert, level = "Extreme move",  "extreme"
    elif abs24 > 4: alert, level = "Large move",    "notable"
    elif abs24 > 2.5: alert, level = "Notable move", "notable"
    else:           alert, level = "—",              "none"
    sign    = "↑" if c24 >= 0 else "↓"
    pattern = f"{'Recovery' if (c7d or 0) > 0 else 'Decline'}"
    if c7d:  pattern += f" — 7d: {c7d:+.1f}%"
    if c30d: pattern += f", 30d: {c30d:+.1f}%"
    return {
        "current": f"{sign}{abs(c24):.1f}%", "d7": f"{c7d:+.1f}%" if c7d else "—",
        "vs30d": f"{c30d:+.1f}%" if c30d else "—",
        "percentile": min(95, abs24 * 10), "alert": alert, "level": level, "pattern": pattern,
    }


def format_eth_volume(vol: float, ohlcv: list) -> dict:
    if vol is None: return _mock("volume")
    vols     = [c.get("close", 0) for c in ohlcv[-30:] if c.get("close")]
    avg_30d  = sum(vols) / len(vols) if vols else vol
    ratio    = vol / avg_30d if avg_30d else 1
    pct      = _percentile(vol, [c.get("close", 0) for c in ohlcv if c.get("close")])
    if ratio > 2.0:   alert, level = "Extreme activity", "extreme"
    elif ratio > 1.5: alert, level = "High activity",    "notable"
    else:             alert, level = "—",                 "none"
    return {
        "current": _fmt_usd(vol), "d7": _fmt_usd(vol * 7),
        "vs30d": f"{(ratio-1)*100:+.0f}% vs avg",
        "percentile": pct, "alert": alert, "level": level,
        "pattern": f"{ratio:.1f}x 30d avg" + (" — volume surge" if ratio > 1.5 else ""),
    }


def format_eth_funding(funding_rate: float) -> dict:
    if funding_rate is None: return _mock("funding")
    pct_8h = funding_rate * 100
    ann    = pct_8h * 3 * 365
    if pct_8h > 0.06:    alert, level = "Extreme leverage",    "extreme"
    elif pct_8h > 0.035: alert, level = "High leverage",       "notable"
    elif pct_8h < -0.03: alert, level = "Extreme short bias",  "extreme"
    elif pct_8h < 0:     alert, level = "Short bias",          "notable"
    else:                alert, level = "—",                    "none"
    return {
        "current": f"{pct_8h:.4f}%", "d7": "—", "vs30d": "—",
        "percentile": min(95, abs(pct_8h) * 1200),
        "alert": alert, "level": level,
        "pattern": f"Annual equiv: {ann:.0f}% — {'high leverage' if pct_8h > 0.035 else 'moderate leverage'}",
    }


def format_eth_open_interest(oi: float) -> dict:
    if oi is None: return _mock("open_interest")
    # ETH OI historically: normal $5-10B, elevated >12B
    if oi > 14e9:  alert, level = "Extreme build-up", "extreme"
    elif oi > 10e9: alert, level = "Rapid build-up",  "notable"
    else:          alert, level = "—",                 "none"
    return {
        "current": _fmt_usd(oi), "d7": "—", "vs30d": "—",
        "percentile": min(90, oi / 14e9 * 90),
        "alert": alert, "level": level,
        "pattern": "Connect OI history DB for percentile tracking",
    }


def format_eth_cme_basis(basis_pct: float) -> dict:
    if basis_pct is None: return _mock("cme_basis")
    if basis_pct < 0:      alert, level = "Backwardation",  "extreme"
    elif basis_pct < 3:    alert, level = "Compressed",     "notable"
    elif basis_pct > 18:   alert, level = "Extreme carry",  "extreme"
    elif basis_pct > 12:   alert, level = "Elevated",       "notable"
    else:                  alert, level = "—",               "none"
    return {
        "current": f"{basis_pct:.1f}%", "d7": "—", "vs30d": "—",
        "percentile": min(95, basis_pct * 5),
        "alert": alert, "level": level,
        "pattern": f"{'Healthy carry' if 3 <= basis_pct <= 12 else 'Outside normal 3-12% range'} — CME ETH=F vs spot",
    }


def format_eth_defi_tvl(tvl: float, tvl_7d: float, tvl_30d: float, pct: float) -> dict:
    if tvl is None: return _mock("defi_tvl")
    d7_chg   = tvl - tvl_7d  if tvl_7d  else None
    vs30_pct = ((tvl / tvl_30d) - 1) * 100 if tvl_30d else None
    if d7_chg and d7_chg > 0 and pct > 70:
        alert, level = "TVL acceleration", "notable"
    elif d7_chg and d7_chg < -0.08 * tvl:
        alert, level = "TVL contraction",  "notable"
    else:
        alert, level = "—", "none"
    return {
        "current": _fmt_usd(tvl), "d7": _fmt_usd(d7_chg, signed=True) if d7_chg else "—",
        "vs30d": f"{vs30_pct:+.1f}%" if vs30_pct else "—",
        "percentile": pct, "alert": alert, "level": level,
        "pattern": f"{'Capital inflow' if d7_chg and d7_chg > 0 else 'Capital outflow'} — DeFiLlama Ethereum chain TVL",
    }


def format_eth_l2_tvl(total_l2: float) -> dict:
    if total_l2 is None: return _mock("l2_tvl")
    # L2 TVL historically: normal $8-15B, elevated >20B
    if total_l2 > 30e9:   alert, level = "L2 expansion extreme", "notable"
    elif total_l2 > 20e9: alert, level = "L2 growth elevated",   "notable"
    else:                 alert, level = "—",                      "none"
    return {
        "current": _fmt_usd(total_l2), "d7": "—", "vs30d": "—",
        "percentile": min(90, total_l2 / 30e9 * 90),
        "alert": alert, "level": level,
        "pattern": "Arbitrum + Base + Optimism + zkSync · ETH scaling flywheel",
    }


def format_eth_staking(staking_rate_pct: float, validators: int) -> dict:
    if staking_rate_pct is None: return _mock("staking_rate")
    if staking_rate_pct > 35:   alert, level = "Very high lock-up",  "notable"
    elif staking_rate_pct < 20: alert, level = "Low participation",  "notable"
    else:                       alert, level = "—",                   "none"
    return {
        "current": f"{staking_rate_pct:.1f}%", "d7": "—", "vs30d": "—",
        "percentile": min(90, staking_rate_pct * 2.5),
        "alert": alert, "level": level,
        "pattern": f"{validators:,} active validators · ~32 ETH per validator",
    }


def format_eth_btc_ratio(price_btc: float) -> dict:
    if price_btc is None: return _mock("eth_btc_ratio")
    # ETH/BTC ratio: alt season signal
    # Historical range roughly 0.02 (bear) to 0.08 (alt season peak)
    pct = _percentile(price_btc, [0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06, 0.07, 0.08])
    if price_btc > 0.065:   alert, level = "Alt season signal",   "notable"
    elif price_btc < 0.025: alert, level = "BTC dominance extreme", "notable"
    else:                   alert, level = "—",                      "none"
    regime = "Alt season territory" if price_btc > 0.055 else "BTC dominance" if price_btc < 0.03 else "Neutral zone"
    return {
        "current": f"{price_btc:.5f}", "d7": "—", "vs30d": "—",
        "percentile": pct, "alert": alert, "level": level,
        "pattern": f"{regime} — rising ratio = capital rotating into ETH",
    }


def format_eth_gas(gas_gwei: float) -> dict:
    if gas_gwei is None: return _mock("gas_price")
    if gas_gwei > 80:    alert, level = "Congestion — high demand",  "extreme"
    elif gas_gwei > 30:  alert, level = "Elevated activity",         "notable"
    elif gas_gwei < 3:   alert, level = "Very low — network idle",   "notable"
    else:                alert, level = "—",                          "none"
    return {
        "current": f"{gas_gwei:.1f} gwei", "d7": "—", "vs30d": "—",
        "percentile": min(95, gas_gwei * 1.2),
        "alert": alert, "level": level,
        "pattern": "Live gas price · Cloudflare ETH RPC · congestion = DeFi demand",
    }


# ── Shared CoinGecko cache ────────────────────────────────────────────────────

_cg_cache:  dict = {"data": None, "ts": 0.0}
_met_cache: dict = {"data": None, "ts": 0.0}
CG_TTL  = 60
MET_TTL = 60


def _get_cg() -> dict:
    now = time.time()
    if _cg_cache["data"] and now - _cg_cache["ts"] < CG_TTL:
        return _cg_cache["data"]
    result = {**fetch_eth_market(), **fetch_eth_derivatives(), "ohlcv": fetch_eth_ohlcv(30)}
    _cg_cache.update({"data": result, "ts": now})
    return result


def _build_metrics() -> dict:
    cg     = _get_cg()
    basis  = fetch_eth_cme_basis()
    tvl    = fetch_eth_mainnet_tvl()
    l2     = fetch_eth_l2_tvl()
    stk    = fetch_eth_staking()
    gas    = fetch_eth_gas()

    return {
        "price_move":  format_eth_price_move(cg.get("change_24h"), cg.get("change_7d"), cg.get("change_30d")),
        "volume":      format_eth_volume(cg.get("volume_24h"), cg.get("ohlcv", [])),
        "funding":     format_eth_funding(cg.get("funding")),
        "open_interest": format_eth_open_interest(cg.get("open_interest_usd")),
        "cme_basis":   format_eth_cme_basis(basis.get("basis_pct")),
        "defi_tvl":    format_eth_defi_tvl(
            tvl.get("tvl_usd"), tvl.get("tvl_7d_ago"), tvl.get("tvl_30d_ago"), tvl.get("percentile", 50)
        ),
        "l2_tvl":      format_eth_l2_tvl(l2.get("total_l2_tvl")),
        "staking_rate": format_eth_staking(stk.get("staking_rate_pct"), stk.get("active_validators") or 0),
        "eth_btc_ratio": format_eth_btc_ratio(cg.get("price_btc")),
        "gas_price":   format_eth_gas(gas.get("gas_gwei")),
    }


def _build_metrics_cached() -> dict:
    now = time.time()
    if _met_cache["data"] and now - _met_cache["ts"] < MET_TTL:
        return _met_cache["data"]
    result = _build_metrics()
    _met_cache.update({"data": result, "ts": now})
    return result


# ── Manual override system ────────────────────────────────────────────────────

def _load_overrides() -> dict:
    if ETH_OVERRIDES.exists():
        try: return json.loads(ETH_OVERRIDES.read_text())
        except Exception: return {}
    return {}


class OverridePayload(BaseModel):
    metric:        str
    current:       str
    d7:            str
    vs30d:         str
    percentile:    float
    alert:         str
    level:         str
    pattern:       str
    source:        Optional[str] = "manual"
    baseline_date: Optional[str] = None


# ── Routes ────────────────────────────────────────────────────────────────────

@eth_router.get("/")
def eth_root():
    return {"service": "eth-dashboard", "status": "ok", "version": "0.1.0"}


@eth_router.get("/metrics")
def eth_metrics():
    """All ETH metric cards — same schema as /metrics (BTC) and /sol/metrics."""
    metrics   = _build_metrics_cached()
    overrides = _load_overrides()
    return {
        k: {**metrics[k], "_is_override": True} if k in overrides else metrics[k]
        for k in metrics
    }


@eth_router.get("/price")
def eth_price():
    cg = _get_cg()
    return {
        "price":      cg.get("price_usd"),
        "price_btc":  cg.get("price_btc"),
        "change_24h": cg.get("change_24h"),
        "change_7d":  cg.get("change_7d"),
        "market_cap": cg.get("market_cap"),
        "ath":        cg.get("ath"),
        "ath_pct":    cg.get("ath_change_pct"),
    }


@eth_router.get("/tvl")
def eth_tvl():
    """Ethereum mainnet + L2 TVL from DeFiLlama."""
    return {
        "mainnet":   fetch_eth_mainnet_tvl(),
        "l2":        fetch_eth_l2_tvl(),
        "protocols": fetch_eth_protocol_breakdown()[:8],
        "dex":       fetch_eth_dex_volume(),
    }


@eth_router.get("/staking")
def eth_staking():
    """ETH staking stats from beaconcha.in free API."""
    return fetch_eth_staking()


@eth_router.get("/gas")
def eth_gas():
    """Live gas price in gwei — Cloudflare public ETH RPC."""
    return fetch_eth_gas()


@eth_router.get("/cme-basis")
def eth_cme_basis():
    """CME ETH futures annualized basis (ETH=F via yFinance)."""
    raw    = fetch_eth_cme_basis()
    metric = format_eth_cme_basis(raw.get("basis_pct"))
    return {**metric, **raw}


@eth_router.get("/structural")
def eth_structural():
    """
    ETH structural thesis data — monetary model, L2 flywheel, ETH/BTC signal.
    Static context + live ratios. No key required.
    """
    cg  = _get_cg()
    stk = fetch_eth_staking()
    l2  = fetch_eth_l2_tvl()
    gas = fetch_eth_gas()
    return {
        "eth_btc_ratio":     cg.get("price_btc"),
        "staking_rate_pct":  stk.get("staking_rate_pct"),
        "staked_eth_M":      stk.get("staked_eth"),
        "active_validators": stk.get("active_validators"),
        "l2_total_tvl":      l2.get("total_l2_tvl"),
        "l2_chains":         l2.get("chains", []),
        "gas_gwei":          gas.get("gas_gwei"),
        # EIP-1559 context (connect etherscan/ultrasound.money for live burn)
        "burn_note": "Wire to ultrasound.money API or Etherscan for live burn rate",
        # ETF context
        "etf_note": "Spot ETH ETF launched May 2024 — use manual override for weekly flow data",
    }


@eth_router.get("/summary")
def eth_summary():
    """Market state bar — structure label + extreme/notable counts."""
    m        = _build_metrics_cached()
    extreme  = sum(1 for v in m.values() if v.get("level") == "extreme")
    notable  = sum(1 for v in m.values() if v.get("level") == "notable")
    neutral  = len(m) - extreme - notable
    if   extreme >= 2:                    structure = "EXTREME"
    elif extreme >= 1 or notable >= 4:    structure = "ELEVATED"
    elif notable >= 2:                    structure = "RECOVERY"
    else:                                 structure = "NEUTRAL"
    return {"structure": structure, "extreme": extreme, "notable": notable, "neutral": neutral}


@eth_router.get("/manual-override")
def eth_get_overrides():
    return _load_overrides()


@eth_router.post("/manual-override")
def eth_set_override(payload: OverridePayload):
    overrides = _load_overrides()
    overrides[payload.metric] = payload.dict()
    ETH_OVERRIDES.write_text(json.dumps(overrides, indent=2))
    _met_cache["data"] = None
    return {"status": "ok", "metric": payload.metric, "overrides_active": len(overrides)}


@eth_router.delete("/manual-override/{metric}")
def eth_clear_override(metric: str):
    overrides = _load_overrides()
    removed   = metric in overrides
    if removed:
        del overrides[metric]
        ETH_OVERRIDES.write_text(json.dumps(overrides, indent=2))
        _met_cache["data"] = None
    return {"status": "ok", "cleared": metric, "was_active": removed}


@eth_router.get("/db/summary")
def eth_db_summary():
    results = {}
    for name, path in [("eth_history", ETH_DB_PATH), ("eth_tvl_history", ETH_TVL_DB_PATH)]:
        if path.exists():
            with sqlite3.connect(path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [r[0] for r in cur.fetchall()]
                results[name] = {}
                for t in tables:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    results[name][t] = cur.fetchone()[0]
        else:
            results[name] = "not initialized"
    return results
