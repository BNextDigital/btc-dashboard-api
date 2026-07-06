"""
sol_routes.py — Solana Dashboard Backend Routes
================================================
Drop-in addition to existing btc-dashboard-api/main.py.

INTEGRATION (2 lines in main.py):
    from sol_routes import sol_router
    app.include_router(sol_router)

DATA SOURCES (all free — no new API keys required):
    CoinGecko    — SOL price, volume, funding, OI, dominance  (already integrated)
    DeFiLlama    — Solana TVL, protocol breakdown, DEX volume  (free, no key)
    yFinance     — CME SOL futures basis (SOL=F)               (already installed)
    Solana RPC   — Validator staking stats                     (public RPC, no key)

METRIC SCHEMA (identical to BTC /metrics):
    {current, d7, vs30d, percentile, alert, level, pattern}

OVERRIDES:
    Same manual override pattern as BTC — reads sol_overrides.json from DATA_DIR
    POST /sol/manual-override  →  same Screenshot Override panel flow
"""

from __future__ import annotations
import os, time, sqlite3, json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel
import requests

from shared.cg_cache import cg_request as _cg_shared, get_weighted_funding_oi as _cg_derivs

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False


# ── Config ─────────────────────────────────────────────────────────────────────

sol_router = APIRouter(prefix="/sol", tags=["Solana"])

DATA_DIR        = Path(os.getenv("DATA_DIR", "./data"))
SOL_DB_PATH     = DATA_DIR / "sol_history.db"
SOL_TVL_DB_PATH = DATA_DIR / "sol_tvl_history.db"
SOL_OVERRIDES   = DATA_DIR / "sol_overrides.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CG_BASE       = "https://api.coingecko.com/api/v3"
DEFILLAMA_BASE = "https://api.llama.fi"
SOL_RPC        = "https://api.mainnet-beta.solana.com"


# ── Database init ──────────────────────────────────────────────────────────────

def _init_dbs():
    with sqlite3.connect(SOL_DB_PATH) as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS sol_basis_history (
                date TEXT PRIMARY KEY,
                basis_pct REAL,
                spot_price REAL,
                futures_price REAL,
                days_to_expiry INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS sol_dominance_history (
                date TEXT PRIMARY KEY, dominance_pct REAL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS sol_stablecoin_history (
                date TEXT PRIMARY KEY, total_usd REAL
            )
        """)
    with sqlite3.connect(SOL_TVL_DB_PATH) as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS sol_tvl_history (
                date TEXT PRIMARY KEY,
                tvl_usd REAL,
                dex_volume_24h REAL
            )
        """)

_init_dbs()


# ── CoinGecko helpers ──────────────────────────────────────────────────────────

def _cg(path: str, params: dict = None) -> dict:
    """Delegates to shared CoinGecko helper — keeps local call sites unchanged."""
    return _cg_shared(path, params)


def fetch_sol_market() -> dict:
    """Price, volume, market cap, % changes from CoinGecko."""
    d  = _cg("/coins/solana", params={
        "localization": "false", "tickers": "false",
        "market_data": "true", "community_data": "false", "developer_data": "false",
    })
    md = d.get("market_data", {})
    return {
        "price_usd":      md.get("current_price", {}).get("usd"),
        "change_24h":     md.get("price_change_percentage_24h"),
        "change_7d":      md.get("price_change_percentage_7d"),
        "change_30d":     md.get("price_change_percentage_30d"),
        "volume_24h":     md.get("total_volume", {}).get("usd"),
        "market_cap":     md.get("market_cap", {}).get("usd"),
        "ath":            md.get("ath", {}).get("usd"),
        "ath_change_pct": md.get("ath_change_percentage", {}).get("usd"),
    }


def fetch_sol_derivatives() -> dict:
    """SOL perp funding + OI — shared /derivatives cache (one call for BTC+ETH+SOL)."""
    return _cg_derivs("SOL")


def fetch_sol_ohlcv(days: int = 30) -> list[dict]:
    """SOL OHLCV for benchmark history — used in percentile calcs."""
    data = _cg("/coins/solana/ohlc", params={"vs_currency": "usd", "days": days})
    return [{"ts": d[0], "open": d[1], "high": d[2], "low": d[3], "close": d[4]}
            for d in data]


# ── DeFiLlama helpers ─────────────────────────────────────────────────────────

def fetch_sol_tvl() -> dict:
    """Solana DeFi TVL (chain-level) from DeFiLlama."""
    try:
        history = requests.get(
            f"{DEFILLAMA_BASE}/v2/historicalChainTvl/Solana", timeout=15
        ).json()
        if not history:
            return {"tvl_usd": None}
        hist = sorted(history, key=lambda x: x.get("date", 0))
        cur       = hist[-1].get("tvl")
        d7_ago    = hist[-8].get("tvl")  if len(hist) >= 8  else None
        d30_ago   = hist[-31].get("tvl") if len(hist) >= 31 else None
        last_90   = [r["tvl"] for r in hist[-90:] if r.get("tvl")]
        pct       = _percentile(cur, last_90) if cur and last_90 else 50
        return {
            "tvl_usd": cur, "tvl_7d_ago": d7_ago, "tvl_30d_ago": d30_ago,
            "percentile": pct, "history_30d": last_90[-30:],
        }
    except Exception as e:
        return {"tvl_usd": None, "error": str(e)}


def fetch_sol_protocol_breakdown() -> list[dict]:
    """Top Solana protocols by TVL."""
    try:
        protos = requests.get(f"{DEFILLAMA_BASE}/protocols", timeout=15).json()
        sol    = [p for p in protos if "Solana" in (p.get("chains") or [])]
        sol.sort(key=lambda x: x.get("tvl", 0), reverse=True)
        return [{"name": p["name"], "tvl": p.get("tvl"), "category": p.get("category")}
                for p in sol[:10]]
    except Exception:
        return []


def fetch_sol_dex_volume() -> dict:
    """Solana DEX volume from DeFiLlama dex overview."""
    try:
        r = requests.get(f"{DEFILLAMA_BASE}/overview/dexs/Solana", timeout=15).json()
        return {"dex_volume_24h": r.get("total24h"), "dex_volume_7d": r.get("total7d")}
    except Exception:
        return {"dex_volume_24h": None, "dex_volume_7d": None}


def fetch_sol_stablecoin_supply() -> dict:
    """USDC + USDT supply on Solana from DeFiLlama stablecoins endpoint."""
    try:
        chains = requests.get(f"{DEFILLAMA_BASE}/stablecoinchains", timeout=15).json()
        sol    = next((c for c in chains if c.get("name") == "Solana"), None)
        if not sol:
            return {"total_usd": None}
        total = sol.get("totalCirculatingUSD", {})
        return {"total_usd": (total.get("peggedUSD") or 0) + (total.get("peggedEUR") or 0)}
    except Exception:
        return {"total_usd": None}


# ── CME SOL Basis (yFinance) ──────────────────────────────────────────────────

def fetch_sol_cme_basis() -> dict:
    """
    CME SOL futures annualized basis via yFinance.
    Ticker: SOL=F  (verify against yfinance — may need SOL1=F)
    Same calculation as BTC basis in main.py.
    """
    if not HAS_YF:
        return {"basis_pct": None, "error": "yfinance not installed"}
    try:
        from datetime import timezone
        spot_price  = yf.Ticker("SOL-USD").fast_info.last_price
        fut_ticker  = yf.Ticker("SOL=F")
        fut_price   = fut_ticker.fast_info.last_price
        if not spot_price or not fut_price:
            return {"basis_pct": None, "error": "Price fetch returned None"}
        expiry_ts = fut_ticker.info.get("expireDate")
        if expiry_ts:
            from datetime import datetime
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


# ── Staking (Solana public RPC) ───────────────────────────────────────────────

def fetch_sol_staking() -> dict:
    """
    Validator staking data from public Solana RPC — no API key required.
    Returns active validator count, total stake, approximate staking rate.
    """
    try:
        resp = requests.post(SOL_RPC, json={
            "jsonrpc": "2.0", "id": 1, "method": "getVoteAccounts",
            "params": [{"commitment": "confirmed"}],
        }, timeout=15).json()
        data        = resp.get("result", {})
        current     = data.get("current", [])
        delinquent  = data.get("delinquent", [])
        active_stake  = sum(v.get("activatedStake", 0) for v in current)
        total_stake   = active_stake + sum(v.get("activatedStake", 0) for v in delinquent)
        # ~580M SOL total supply (approximate — use supply endpoint for exact)
        SOL_SUPPLY    = 580_000_000 * 1_000_000_000  # lamports
        staking_rate  = round(active_stake / SOL_SUPPLY * 100, 2) if SOL_SUPPLY else None
        top5 = sorted(current, key=lambda x: x.get("activatedStake", 0), reverse=True)[:5]
        top5_pct = round(sum(v.get("activatedStake", 0) for v in top5) / total_stake * 100, 1) if total_stake else None
        return {
            "active_validators": len(current),
            "delinquent_validators": len(delinquent),
            "active_stake_sol": round(active_stake / 1e9, 0),
            "staking_rate_pct": staking_rate,
            "top5_concentration_pct": top5_pct,
        }
    except Exception as e:
        return {"error": str(e), "_mock": True, "staking_rate_pct": 64.8, "active_validators": 1463}


# ── Formatters ────────────────────────────────────────────────────────────────

def _percentile(value: float, series: list[float]) -> float:
    if not series or value is None:
        return 50.0
    return round(sum(1 for v in series if v < value) / len(series) * 100, 1)


def _fmt_usd(v, signed=False) -> str:
    if v is None: return "—"
    prefix = ("+" if v >= 0 else "") if signed else ""
    abs_v  = abs(v)
    if abs_v >= 1e9:  return f"{prefix}${abs_v/1e9:.1f}B" if v >= 0 else f"-${abs_v/1e9:.1f}B"
    if abs_v >= 1e6:  return f"{prefix}${abs_v/1e6:.0f}M" if v >= 0 else f"-${abs_v/1e6:.0f}M"
    return f"{prefix}${v:,.0f}"

def _mock(metric_id: str) -> dict:
    return {
        "current": "—", "d7": "—", "vs30d": "—",
        "percentile": 50, "alert": "No data", "level": "none",
        "pattern": f"{metric_id}: data source not connected",
        "_mock": True,
    }


def format_price_move(c24: float, c7d: float, c30d: float) -> dict:
    if c24 is None: return _mock("price_move")
    abs24 = abs(c24)
    if abs24 > 8:   alert, level = "Extreme move",  "extreme"
    elif abs24 > 5: alert, level = "Large move",    "notable"
    elif abs24 > 3: alert, level = "Notable move",  "notable"
    else:           alert, level = "—",              "none"
    sign = "↑" if c24 >= 0 else "↓"
    pattern = f"{'Recovery' if (c7d or 0) > 0 else 'Decline'}"
    if c7d:  pattern += f" — 7d: {c7d:+.1f}%"
    if c30d: pattern += f", 30d: {c30d:+.1f}%"
    return {
        "current": f"{sign}{abs(c24):.1f}%", "d7": f"{c7d:+.1f}%" if c7d else "—",
        "vs30d": f"{c30d:+.1f}%" if c30d else "—",
        "percentile": min(95, abs24 * 8), "alert": alert, "level": level, "pattern": pattern,
    }


def format_volume(vol: float, ohlcv: list) -> dict:
    if vol is None: return _mock("volume")
    vols_30d  = [c.get("close", 0) for c in ohlcv[-30:] if c.get("close")]
    avg_30d   = sum(vols_30d) / len(vols_30d) if vols_30d else vol
    ratio     = vol / avg_30d if avg_30d else 1
    pct       = _percentile(vol, [c.get("close", 0) for c in ohlcv if c.get("close")])
    if ratio > 2.0:   alert, level = "Extreme activity", "extreme"
    elif ratio > 1.5: alert, level = "High activity",    "notable"
    else:             alert, level = "—",                 "none"
    return {
        "current": _fmt_usd(vol), "d7": _fmt_usd(vol * 7),
        "vs30d": f"{(ratio-1)*100:+.0f}% vs avg",
        "percentile": pct, "alert": alert, "level": level,
        "pattern": f"{ratio:.1f}x 30d avg" + (" — volume surge" if ratio > 1.5 else ""),
    }


def format_funding(funding_rate: float) -> dict:
    if funding_rate is None: return _mock("funding")
    pct_8h  = funding_rate * 100
    ann     = pct_8h * 3 * 365
    if pct_8h > 0.07:   alert, level = "Extreme leverage",    "extreme"
    elif pct_8h > 0.04: alert, level = "High leverage",       "notable"
    elif pct_8h < -0.03:alert, level = "Extreme short bias",  "extreme"
    elif pct_8h < 0:    alert, level = "Short bias",          "notable"
    else:               alert, level = "—",                    "none"
    return {
        "current": f"{pct_8h:.4f}%", "d7": "—", "vs30d": "—",
        "percentile": min(95, abs(pct_8h) * 1000),
        "alert": alert, "level": level,
        "pattern": f"Annual equiv: {ann:.0f}% — {'high leverage' if pct_8h > 0.04 else 'moderate'}",
    }


def format_open_interest(oi: float) -> dict:
    if oi is None: return _mock("open_interest")
    # Thresholds based on historical SOL OI ranges
    if oi > 4e9:   alert, level = "Extreme build-up", "extreme"
    elif oi > 3e9: alert, level = "Rapid build-up",   "notable"
    else:          alert, level = "—",                  "none"
    return {
        "current": _fmt_usd(oi), "d7": "—", "vs30d": "—",
        "percentile": min(90, oi / 4e9 * 90),
        "alert": alert, "level": level,
        "pattern": "Connect OI history DB for percentile · /sol/db/summary to check",
    }


def format_cme_basis(basis_pct: float) -> dict:
    if basis_pct is None: return _mock("cme_basis")
    if basis_pct < 0:      alert, level = "Backwardation",  "extreme"
    elif basis_pct < 5:    alert, level = "Compressed",     "notable"
    elif basis_pct > 20:   alert, level = "Extreme carry",  "extreme"
    elif basis_pct > 15:   alert, level = "Elevated",       "notable"
    else:                  alert, level = "—",               "none"
    return {
        "current": f"{basis_pct:.1f}%", "d7": "—", "vs30d": "—",
        "percentile": min(95, basis_pct * 4),
        "alert": alert, "level": level,
        "pattern": f"{'Healthy carry' if 5 <= basis_pct <= 15 else 'Outside normal 5-15% range'} — CME SOL=F vs spot",
    }


def format_defi_tvl(tvl: float, tvl_7d: float, tvl_30d: float, pct: float) -> dict:
    if tvl is None: return _mock("defi_tvl")
    d7_chg  = tvl - tvl_7d  if tvl_7d  else None
    vs30_pct = ((tvl / tvl_30d) - 1) * 100 if tvl_30d else None
    if d7_chg and d7_chg > 0 and pct > 75:
        alert, level = "TVL acceleration", "notable"
    elif d7_chg and d7_chg < -0.1 * tvl:
        alert, level = "TVL contraction",  "notable"
    else:
        alert, level = "—", "none"
    return {
        "current": _fmt_usd(tvl), "d7": _fmt_usd(d7_chg, signed=True) if d7_chg else "—",
        "vs30d": f"{vs30_pct:+.1f}%" if vs30_pct else "—",
        "percentile": pct, "alert": alert, "level": level,
        "pattern": f"{'Capital inflow' if d7_chg and d7_chg > 0 else 'Capital outflow'} — DeFiLlama Solana chain TVL",
    }


def format_dex_volume(vol_24h: float, vol_7d: float) -> dict:
    if vol_24h is None: return _mock("dex_volume")
    avg_7d = vol_7d / 7 if vol_7d else vol_24h
    ratio  = vol_24h / avg_7d if avg_7d else 1
    if ratio > 2.0:   alert, level = "Extreme DEX activity", "extreme"
    elif ratio > 1.5: alert, level = "High DEX activity",    "notable"
    else:             alert, level = "—",                     "none"
    return {
        "current": _fmt_usd(vol_24h), "d7": _fmt_usd(vol_7d) if vol_7d else "—",
        "vs30d": f"{(ratio-1)*100:+.0f}% vs 7d avg",
        "percentile": min(95, ratio * 45), "alert": alert, "level": level,
        "pattern": "Jupiter aggregates ~70% of Solana DEX order flow",
    }


def format_stablecoin(total: float) -> dict:
    if total is None: return _mock("stablecoin_sol")
    return {
        "current": _fmt_usd(total), "d7": "—", "vs30d": "—",
        "percentile": 50, "alert": "—", "level": "none",
        "pattern": "USDC + USDT on Solana — OUSD will add to this when live (H2 2026)",
    }


def format_dominance(dominance_pct: float) -> dict:
    if dominance_pct is None: return _mock("dominance")
    if dominance_pct > 3:     alert, level = "High dominance", "notable"
    elif dominance_pct < 1:   alert, level = "Low dominance",  "notable"
    else:                     alert, level = "—",               "none"
    return {
        "current": f"{dominance_pct:.2f}%", "d7": "—", "vs30d": "—",
        "percentile": min(95, dominance_pct * 30), "alert": alert, "level": level,
        "pattern": f"SOL market cap dominance — {'recovering' if dominance_pct > 1.5 else 'depressed'}",
    }


# ── Shared CoinGecko cache ─────────────────────────────────────────────────────

_cg_cache:  dict = {"data": None, "ts": 0.0}
_met_cache: dict = {"data": None, "ts": 0.0}
CG_TTL  = 300   # 5 min — reduces CoinGecko calls; free tier is 30 req/min shared across all route files
MET_TTL = 300


def _get_cg() -> dict:
    now = time.time()
    if _cg_cache["data"] and now - _cg_cache["ts"] < CG_TTL:
        return _cg_cache["data"]
    try:
        result = {**fetch_sol_market(), **fetch_sol_derivatives(), "ohlcv": fetch_sol_ohlcv(30)}
        _cg_cache.update({"data": result, "ts": now})
        return result
    except Exception as e:
        print(f"[sol] CoinGecko fetch failed: {e}")
        if _cg_cache["data"]:
            print(f"[sol] Returning stale cache (age {int(now - _cg_cache['ts'])}s)")
            return _cg_cache["data"]
        return {}


def _build_metrics() -> dict:
    cg    = _get_cg()
    basis = fetch_sol_cme_basis()
    tvl   = fetch_sol_tvl()
    dex   = fetch_sol_dex_volume()
    stbl  = fetch_sol_stablecoin_supply()
    stk   = fetch_sol_staking()

    # dominance from market cap (approx — CoinGecko global gives exact)
    try:
        global_d = _cg("/global")
        dominance = global_d.get("data", {}).get("market_cap_percentage", {}).get("sol")
    except Exception:
        dominance = None

    return {
        "price_move":    format_price_move(cg.get("change_24h"), cg.get("change_7d"), cg.get("change_30d")),
        "volume":        format_volume(cg.get("volume_24h"), cg.get("ohlcv", [])),
        "funding":       format_funding(cg.get("funding")),
        "open_interest": format_open_interest(cg.get("open_interest_usd")),
        "cme_basis":     format_cme_basis(basis.get("basis_pct")),
        "defi_tvl":      format_defi_tvl(tvl.get("tvl_usd"), tvl.get("tvl_7d_ago"), tvl.get("tvl_30d_ago"), tvl.get("percentile", 50)),
        "dex_volume":    format_dex_volume(dex.get("dex_volume_24h"), dex.get("dex_volume_7d")),
        "stablecoin_sol": format_stablecoin(stbl.get("total_usd")),
        "staking_rate":  {
            "current": f"{stk.get('staking_rate_pct', 64.8):.1f}%",
            "d7": "—", "vs30d": "—", "percentile": 55,
            "alert": "—", "level": "none",
            "pattern": f"{stk.get('active_validators', '—')} active validators · top-5 concentration {stk.get('top5_concentration_pct', '—')}%",
        },
        "dominance": format_dominance(dominance),
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
    if SOL_OVERRIDES.exists():
        try: return json.loads(SOL_OVERRIDES.read_text())
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

@sol_router.get("/")
def sol_root():
    return {"service": "sol-dashboard", "status": "ok", "version": "0.1.0"}


@sol_router.get("/metrics")
def sol_metrics():
    """
    All SOL metric cards — same schema as BTC /metrics.
    Overrides applied last (same pattern as BTC dashboard).
    """
    metrics   = _build_metrics_cached()
    overrides = _load_overrides()
    return {
        k: {**metrics[k], "_is_override": True} if k in overrides else metrics[k]
        for k in metrics
    }


@sol_router.get("/price")
def sol_price():
    """SOL spot price + % changes."""
    cg = _get_cg()
    return {
        "price":      cg.get("price_usd"),
        "change_24h": cg.get("change_24h"),
        "change_7d":  cg.get("change_7d"),
        "market_cap": cg.get("market_cap"),
        "ath":        cg.get("ath"),
        "ath_pct":    cg.get("ath_change_pct"),
    }


@sol_router.get("/tvl")
def sol_tvl():
    """Solana DeFi TVL from DeFiLlama — chain-level + protocol breakdown."""
    return {
        "chain_tvl":  fetch_sol_tvl(),
        "protocols":  fetch_sol_protocol_breakdown()[:8],
        "dex_volume": fetch_sol_dex_volume(),
    }


@sol_router.get("/stablecoin")
def sol_stablecoin():
    """Stablecoin supply on Solana — primary OUSD thesis indicator."""
    return fetch_sol_stablecoin_supply()


@sol_router.get("/staking")
def sol_staking():
    """
    Validator staking data from public Solana RPC.
    No API key required — calls api.mainnet-beta.solana.com.
    """
    return fetch_sol_staking()


@sol_router.get("/cme-basis")
def sol_cme_basis():
    """CME SOL futures annualized basis (SOL=F via yFinance)."""
    raw    = fetch_sol_cme_basis()
    metric = format_cme_basis(raw.get("basis_pct"))
    return {**metric, **raw}


@sol_router.get("/ousd-status")
def ousd_status():
    """
    OUSD investment thesis tracker.
    Static until OUSD goes live on-chain (H2 2026).
    Wire to on-chain supply tracking once OUSD launches.
    """
    return {
        "status":            "pre_launch",
        "expected_live":     "H2 2026",
        "announced":         "2026-06-30",
        "partner_count":     140,
        "native_chains":     ["Solana", "Stellar", "Base", "Polygon"],
        "confirmed_partners": [
            "Visa", "Mastercard", "Stripe", "American Express",
            "Google", "Shopify", "BlackRock", "BNY", "Standard Chartered",
            "Coinbase", "Aave", "Solana Foundation",
        ],
        "open_questions": [
            "Reserve custodian — unpublished",
            "Attestation cadence — not confirmed",
            "Go-live date — H2 2026 expected, not fixed",
            "Management fee — not disclosed",
        ],
        "thesis_signals": {
            "confirms": [
                "Solana chosen as native chain — day-one deployment",
                "Stripe making OUSD default for business transactions",
                "140+ partner signatories including Tier 1 banks and payment networks",
                "Stablecoin supply on Solana accelerating pre-launch",
            ],
            "invalidates": [
                "Launch delayed beyond H2 2026",
                "Reserve composition or attestation below USDC standards",
                "Partner integration rate at go-live << 140 signatories",
                "Solana de-prioritized post-launch in favor of other chains",
            ],
        },
        "_last_updated": "2026-07-03",
    }


@sol_router.get("/summary")
def sol_summary():
    """Market state bar — structure label + extreme/notable counts."""
    m        = _build_metrics_cached()
    extreme  = sum(1 for v in m.values() if v.get("level") == "extreme")
    notable  = sum(1 for v in m.values() if v.get("level") == "notable")
    neutral  = len(m) - extreme - notable
    if   extreme >= 2:          structure = "EXTREME"
    elif extreme >= 1 or notable >= 4: structure = "ELEVATED"
    elif notable >= 2:          structure = "RECOVERY"
    else:                       structure = "NEUTRAL"
    return {"structure": structure, "extreme": extreme, "notable": notable, "neutral": neutral}


@sol_router.get("/manual-override")
def sol_get_overrides():
    return _load_overrides()


@sol_router.post("/manual-override")
def sol_set_override(payload: OverridePayload):
    """Apply manual screenshot override — same flow as BTC dashboard."""
    overrides = _load_overrides()
    overrides[payload.metric] = payload.dict()
    SOL_OVERRIDES.write_text(json.dumps(overrides, indent=2))
    # Bust cache so next /sol/metrics call reflects the override
    _met_cache["data"] = None
    return {"status": "ok", "metric": payload.metric, "overrides_active": len(overrides)}


@sol_router.delete("/manual-override/{metric}")
def sol_clear_override(metric: str):
    overrides = _load_overrides()
    removed   = metric in overrides
    if removed:
        del overrides[metric]
        SOL_OVERRIDES.write_text(json.dumps(overrides, indent=2))
        _met_cache["data"] = None
    return {"status": "ok", "cleared": metric, "was_active": removed}


@sol_router.get("/db/summary")
def sol_db_summary():
    """Row counts for all SOL SQLite databases."""
    results = {}
    for name, path in [("sol_history", SOL_DB_PATH), ("sol_tvl_history", SOL_TVL_DB_PATH)]:
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

@sol_router.get("/debug/derivatives")
def sol_debug_derivatives():
    """
    Mirrors BTC's /debug/funding endpoint.
    Call this to inspect what CoinGecko actually returns for SOL perps —
    field names, exchange names, and index_id values — before trusting
    any filter logic.

    Hit: GET /sol/debug/derivatives
    """
    from shared.cg_cache import get_derivatives, REFERENCE_EXCHANGES

    all_tickers = get_derivatives()

    if not all_tickers:
        return {"error": "get_derivatives() returned empty — check cg_cache logs"}

    # ── Broad SOL search — no filter assumptions ───────────────────────────
    # Search for SOL across every string field so we can see what naming
    # convention CoinGecko actually uses (index_id, base, symbol, etc.)
    sol_candidates = [
        t for t in all_tickers
        if any(
            "SOL" in str(v).upper()
            for v in t.values()
            if isinstance(v, str)
        )
    ]

    # ── Strict filter — what get_weighted_funding_oi("SOL") actually does ─
    sol_strict = [
        t for t in all_tickers
        if t.get("index_id", "").upper() == "SOL"
        and t.get("contract_type") == "perpetual"
        and t.get("market") in REFERENCE_EXCHANGES
        and t.get("funding_rate") is not None
        and t.get("open_interest", 0) > 0
        and t.get("funding_rate") != 0.01
        and t.get("funding_rate") != -0.01
    ]

    # ── Show the keys present on the first SOL candidate ──────────────────
    sample_keys = list(sol_candidates[0].keys()) if sol_candidates else []

    return {
        "total_tickers_in_cache":  len(all_tickers),
        "sol_candidates_broad":    len(sol_candidates),
        "sol_strict_filter_match": len(sol_strict),
        "reference_exchanges":     list(REFERENCE_EXCHANGES),
        "field_keys_on_record":    sample_keys,
        "candidates": [
            {
                "market":        t.get("market"),
                "symbol":        t.get("symbol"),
                "index_id":      t.get("index_id"),
                "base":          t.get("base"),        # may not exist
                "contract_type": t.get("contract_type"),
                "funding_rate":  t.get("funding_rate"),
                "open_interest": t.get("open_interest"),
            }
            for t in sol_candidates[:15]
        ],
        "strict_matches": [
            {
                "market":       t.get("market"),
                "symbol":       t.get("symbol"),
                "index_id":     t.get("index_id"),
                "funding_rate": t.get("funding_rate"),
                "open_interest":t.get("open_interest"),
            }
            for t in sol_strict
        ],
    }
