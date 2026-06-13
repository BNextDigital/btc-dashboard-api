"""
equity_routes.py — Equity Internals & Commercial Districts

Replaces sector_flows_routes.py with a framework-aligned implementation.

"Index level = total commercial activity. Breadth = how many shops are active.
 Weak breadth means concentration in a few names."

Instruments:
  Indices:    S&P 500, Nasdaq, Russell 2000
  Breadth:    Equal-weight vs cap-weight spread (RSP/SPY ratio), new highs/lows proxy
  Key sectors: Semis (SOXX), Banks (XLF), Transports (IYT), QQQ
  Context:    SPY vs RSP divergence as breadth signal

KEEP the router prefix at /sector-flows so main.py registration is unchanged.
Add the new /equity/* routes as additional endpoints.

Data: yFinance bulk download, 252d history
Cache: 10 minutes

Setup:
  Replace btc-dashboard-api/sector_flows_routes.py with this file.
  main.py registration stays the same:
    from equity_routes import sector_flows_router
    app.include_router(sector_flows_router)

Endpoints:
  GET /sector-flows/metrics     — legacy endpoint (returns equity internals)
  GET /equity/metrics           — same data, new namespace
  GET /equity/cache/flush
"""

import os
import time
import math
from datetime import datetime
from fastapi import APIRouter
import yfinance as yf
import pandas as pd

# ── Routers ───────────────────────────────────────────────────────────────────
# Keep /sector-flows for backward compat, add /equity as canonical
sector_flows_router = APIRouter()   # no prefix — mount both paths below

# ── Config ────────────────────────────────────────────────────────────────────
CACHE_TTL   = 600   # 10 minutes
_cache: dict = {"data": None, "ts": 0.0}

# ── Tickers ───────────────────────────────────────────────────────────────────
TICKERS = {
    # ── Major Indices ──────────────────────────────────────────────────────
    "spx":      "^GSPC",      # S&P 500 — total commercial activity
    "qqq":      "QQQ",        # Nasdaq 100 ETF — tech district
    "iwm":      "^RUT",       # Russell 2000 — small-cap health
    "nasdaq":   "^IXIC",      # Nasdaq composite
    # ── Breadth proxies ────────────────────────────────────────────────────
    "spy":      "SPY",        # S&P 500 cap-weighted ETF (for RSP/SPY ratio)
    "rsp":      "RSP",        # S&P 500 equal-weighted ETF — breadth proxy
    # ── Key sectors ────────────────────────────────────────────────────────
    "soxx":     "SOXX",       # Semiconductors — Philly Semi ETF
    "xlf":      "XLF",        # Financials / Banks
    "iyt":      "IYT",        # Transports (Dow Jones Transport ETF)
    "xlk":      "XLK",        # Technology
    "xle":      "XLE",        # Energy
    "xlu":      "XLU",        # Utilities (defensive)
    # ── Volatility ─────────────────────────────────────────────────────────
    "vix":      "^VIX",       # CBOE VIX
}

# City district metaphor labels
DISTRICT_LABELS = {
    "spx":   "Total Commercial Activity",
    "qqq":   "Tech & Growth District",
    "iwm":   "Small Business District",
    "soxx":  "Semiconductor Quarter",
    "xlf":   "Banking District",
    "iyt":   "Transport & Logistics Hub",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _san(v) -> float | None:
    """Sanitize NaN/Inf to None."""
    if v is None:
        return None
    try:
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def _pct_rank(vals: list[float], current: float) -> int:
    if not vals or len(vals) < 5:
        return 50
    return round(sum(1 for v in vals if v < current) / len(vals) * 100)


def _spark(vals: list[float], n: int = 20) -> list[float]:
    trimmed = vals[-n:] if len(vals) >= n else vals
    return [round(v, 2) for v in trimmed]


def _chg_pct(a: float, b: float) -> float | None:
    if b == 0:
        return None
    return round((a - b) / b * 100, 2)

# ── yFinance fetch ────────────────────────────────────────────────────────────

def _fetch_bulk(n_days: int = 252) -> dict[str, pd.Series | None]:
    """Bulk download close prices for all tickers."""
    tickers = list(TICKERS.values())
    result  = {k: None for k in TICKERS}
    try:
        raw   = yf.download(tickers, period=f"{n_days}d",
                             auto_adjust=True, progress=False, threads=True)
        close = raw["Close"] if "Close" in raw.columns else raw
        for key, ticker in TICKERS.items():
            if ticker in close.columns:
                s = close[ticker].dropna()
                result[key] = s if len(s) >= 10 else None
    except Exception as e:
        print(f"[equity] yFinance bulk error: {e}")
    return result

# ── Card builders ─────────────────────────────────────────────────────────────

def _index_card(key: str, series: pd.Series | None, name: str) -> dict:
    """Build a metric card for a major index."""
    district = DISTRICT_LABELS.get(key, "")
    if series is None or len(series) < 5:
        return {"key": key, "name": name, "district_label": district, "error": "unavailable"}

    vals    = series.tolist()
    current = vals[-1]
    d5      = vals[-6]   if len(vals) >= 6   else vals[0]
    d20     = vals[-21]  if len(vals) >= 21  else vals[0]
    d252    = vals[-252] if len(vals) >= 252 else vals[0]
    pctile  = _pct_rank(vals, current)

    chg5   = _chg_pct(current, d5)
    chg20  = _chg_pct(current, d20)
    chg_yoy = _chg_pct(current, d252)

    # Simple trend label
    if chg20 is not None and chg20 >= 5:
        trend, trend_level = "Uptrend", "none"
    elif chg20 is not None and chg20 >= 0:
        trend, trend_level = "Mild uptrend", "none"
    elif chg20 is not None and chg20 >= -5:
        trend, trend_level = "Mild downtrend", "notable"
    else:
        trend, trend_level = "Downtrend", "extreme"

    # SMA context
    sma20  = sum(vals[-20:]) / min(len(vals), 20)
    sma50  = sum(vals[-50:]) / min(len(vals), 50)
    sma200 = sum(vals[-200:]) / min(len(vals), 200)
    above_200 = current > sma200
    above_50  = current > sma50

    return {
        "key":           key,
        "name":          name,
        "district_label": district,
        "current":       round(current, 2),
        "d5_pct":        f"{chg5:+.2f}%" if chg5 is not None else "—",
        "d20_pct":       f"{chg20:+.2f}%" if chg20 is not None else "—",
        "yoy_pct":       f"{chg_yoy:+.1f}%" if chg_yoy is not None else "—",
        "percentile":    pctile,
        "trend":         trend,
        "trend_level":   trend_level,
        "sma20":         round(sma20, 2),
        "sma50":         round(sma50, 2),
        "sma200":        round(sma200, 2),
        "above_200sma":  above_200,
        "above_50sma":   above_50,
        "spark":         _spark(vals),
    }


def _sector_card(key: str, series: pd.Series | None, name: str,
                 spx_series: pd.Series | None) -> dict:
    """Build a sector card with relative performance vs SPX."""
    district = DISTRICT_LABELS.get(key, "")
    if series is None or len(series) < 5:
        return {"key": key, "name": name, "district_label": district, "error": "unavailable"}

    vals    = series.tolist()
    current = vals[-1]
    d5      = vals[-6]  if len(vals) >= 6  else vals[0]
    d20     = vals[-21] if len(vals) >= 21 else vals[0]
    pctile  = _pct_rank(vals, current)

    chg5  = _chg_pct(current, d5)
    chg20 = _chg_pct(current, d20)

    # Relative performance vs SPX
    rel5d, rel20d = None, None
    if spx_series is not None and len(spx_series) >= 6:
        spx_vals = spx_series.tolist()
        spx_chg5  = _chg_pct(spx_vals[-1], spx_vals[-6])
        spx_chg20 = _chg_pct(spx_vals[-1], spx_vals[-21]) if len(spx_vals) >= 21 else None
        if chg5 is not None and spx_chg5 is not None:
            rel5d  = round(chg5 - spx_chg5, 2)
        if chg20 is not None and spx_chg20 is not None:
            rel20d = round(chg20 - spx_chg20, 2)

    # Alert based on relative performance
    if rel5d is not None and rel5d >= 2:
        alert_level, alert = "none", "Leading market — capital rotating in"
    elif rel5d is not None and rel5d <= -2:
        alert_level, alert = "notable", "Lagging market — capital rotating out"
    else:
        alert_level, alert = "none", "In line with market"

    # Key sector-specific reads
    if key == "soxx":
        if rel5d is not None and rel5d >= 3:
            alert = "Semis leading — risk appetite strong, tech cycle healthy"
            alert_level = "none"
        elif rel5d is not None and rel5d <= -3:
            alert = "Semis lagging — growth concern or sector rotation away from tech"
            alert_level = "notable"
    elif key == "xlf":
        if chg5 is not None and chg5 >= 2:
            alert = "Banks rising — credit stress easing, yield curve supportive"
            alert_level = "none"
        elif chg5 is not None and chg5 <= -2:
            alert = "Banks falling — credit stress or yield curve pressure"
            alert_level = "notable"
    elif key == "iyt":
        if rel5d is not None and rel5d <= -2:
            alert = "Transports lagging — economic activity slowdown signal"
            alert_level = "notable"

    return {
        "key":           key,
        "name":          name,
        "district_label": district,
        "current":       round(current, 2),
        "d5_pct":        f"{chg5:+.2f}%" if chg5 is not None else "—",
        "d20_pct":       f"{chg20:+.2f}%" if chg20 is not None else "—",
        "rel_5d":        f"{rel5d:+.2f}%" if rel5d is not None else "—",
        "rel_20d":       f"{rel20d:+.2f}%" if rel20d is not None else "—",
        "rel_5d_raw":    rel5d,
        "percentile":    pctile,
        "alert":         alert,
        "alert_level":   alert_level,
        "spark":         _spark(vals),
    }


def _build_breadth(spy: pd.Series | None, rsp: pd.Series | None) -> dict:
    """
    Market breadth via RSP/SPY ratio.
    RSP = equal-weight S&P 500. SPY = cap-weight.
    When RSP/SPY rises, more stocks participating (healthy breadth).
    When RSP/SPY falls, market increasingly driven by mega-caps (narrow breadth).
    """
    if spy is None or rsp is None or len(spy) < 20 or len(rsp) < 20:
        return {"error": "Breadth data unavailable", "alert_level": "none", "alert": "—"}

    # Compute ratio series (align by index)
    ratio = (rsp / spy).dropna()
    if len(ratio) < 5:
        return {"error": "Insufficient breadth history", "alert_level": "none", "alert": "—"}

    ratio_vals = ratio.tolist()
    current_ratio = ratio_vals[-1]
    d20_ratio     = ratio_vals[-21] if len(ratio_vals) >= 21 else ratio_vals[0]
    d60_ratio     = ratio_vals[-61] if len(ratio_vals) >= 61 else ratio_vals[0]

    ratio_chg20 = _chg_pct(current_ratio, d20_ratio)
    ratio_chg60 = _chg_pct(current_ratio, d60_ratio)
    pctile      = _pct_rank(ratio_vals, current_ratio)

    # Breadth read
    if ratio_chg20 is not None and ratio_chg20 >= 1.0:
        breadth_label = "Expanding"
        breadth_desc  = "Equal-weight outperforming — broad participation, healthy market internals"
        alert_level   = "none"
    elif ratio_chg20 is not None and ratio_chg20 <= -1.0:
        breadth_label = "Narrowing"
        breadth_desc  = "Cap-weight outperforming — concentration in mega-caps, few shops open"
        alert_level   = "notable"
    elif ratio_chg20 is not None and ratio_chg20 <= -2.0:
        breadth_label = "Very Narrow"
        breadth_desc  = "Severe concentration — index level driven by a handful of names, breadth warning"
        alert_level   = "extreme"
    else:
        breadth_label = "Neutral"
        breadth_desc  = "Breadth stable — no strong divergence between equal- and cap-weight"
        alert_level   = "none"

    return {
        "label":        breadth_label,
        "description":  breadth_desc,
        "alert_level":  alert_level,
        "rsp_spy_ratio": round(current_ratio, 4),
        "ratio_20d_chg": f"{ratio_chg20:+.2f}%" if ratio_chg20 is not None else "—",
        "ratio_60d_chg": f"{ratio_chg60:+.2f}%" if ratio_chg60 is not None else "—",
        "percentile":   pctile,
        "spark":        _spark(ratio_vals),
        "note":         "RSP/SPY rising = more stocks participating (healthy). Falling = mega-cap concentration (fragile).",
    }


def _market_assessment(indices: dict, sectors: dict, breadth: dict, vix_series) -> dict:
    """
    Top-level commercial district health read.
    """
    headwinds, tailwinds = [], []

    spx = indices.get("spx", {})
    iwm = indices.get("iwm", {})
    qqq = indices.get("qqq", {})

    # SPX trend
    spx_trend = spx.get("trend_level", "none")
    if spx_trend == "extreme":
        headwinds.append("S&P 500 in downtrend — commercial district contracting")
    elif spx_trend == "none" and spx.get("above_200sma"):
        tailwinds.append("S&P 500 above 200 SMA — long-term trend intact")

    # Small-cap health
    iwm_20d = None
    if iwm and not iwm.get("error"):
        iwm_20d_str = iwm.get("d20_pct", "—")
        try:
            iwm_20d = float(iwm_20d_str.replace("%", "").replace("+", ""))
        except ValueError:
            pass
    if iwm_20d is not None and iwm_20d <= -5:
        headwinds.append("Russell 2000 weak — small businesses under pressure, risk appetite fading")
    elif iwm_20d is not None and iwm_20d >= 5:
        tailwinds.append("Russell 2000 strong — broad risk appetite, small-cap participation")

    # Breadth
    bl = breadth.get("label", "")
    if bl == "Narrowing" or bl == "Very Narrow":
        headwinds.append(f"Breadth {bl.lower()} — few shops driving the index")
    elif bl == "Expanding":
        tailwinds.append("Breadth expanding — many shops active, healthy internals")

    # Semis
    soxx = sectors.get("soxx", {})
    if soxx and not soxx.get("error"):
        rel = soxx.get("rel_5d_raw")
        if rel is not None and rel >= 3:
            tailwinds.append("Semis leading market — tech cycle healthy, risk-on signal")
        elif rel is not None and rel <= -3:
            headwinds.append("Semis lagging — tech cycle concern, watch for broader risk-off")

    # Banks
    xlf = sectors.get("xlf", {})
    if xlf and not xlf.get("error"):
        xlf_alert = xlf.get("alert_level", "none")
        if xlf_alert == "notable":
            headwinds.append("Banks lagging — credit stress or yield curve pressure")

    # VIX
    if vix_series is not None and len(vix_series) >= 2:
        vix_vals = vix_series.tolist()
        vix_cur  = vix_vals[-1]
        vix_d5   = vix_vals[-6] if len(vix_vals) >= 6 else vix_vals[0]
        if vix_cur >= 25:
            headwinds.append(f"VIX {vix_cur:.1f} — elevated fear, market stress")
        elif vix_cur <= 15:
            tailwinds.append(f"VIX {vix_cur:.1f} — low volatility, complacent/calm conditions")

    n_hw, n_tw = len(headwinds), len(tailwinds)
    if n_hw >= 3:
        regime, level = "Distressed — Multiple Districts Shuttering", "extreme"
        read = "Broad deterioration in equity internals. Risk appetite fading across indices, sectors, and breadth. Historically a challenging environment for BTC."
    elif n_hw >= 2:
        regime, level = "Weakening Commercial Activity", "notable"
        read = "Equity internals softening. Breadth narrowing or key sectors lagging. BTC may hold if ETF inflows remain strong, but macro headwind building."
    elif n_tw >= 3:
        regime, level = "Healthy — Districts Open & Active", "none"
        read = "Broad equity strength with expanding breadth and sector participation. Risk appetite is supportive. Positive backdrop for BTC."
    elif n_tw >= 1 and n_hw == 0:
        regime, level = "Solid — Main Districts Active", "none"
        read = "Equity internals broadly healthy. No major stress signals. Neutral-to-positive backdrop for BTC."
    else:
        regime, level = "Mixed Signals", "none"
        read = "Some districts active, others quiet. No strong directional signal from equity internals. Focus on BTC-specific flows."

    return {
        "regime":       regime,
        "regime_level": level,
        "read":         read,
        "headwinds":    headwinds,
        "tailwinds":    tailwinds,
    }

# ── Main builder ──────────────────────────────────────────────────────────────

def _build_equity_metrics() -> dict:
    global _cache
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    yf_data = _fetch_bulk(n_days=252)

    spx_series = yf_data.get("spx")

    indices = {
        "spx":    _index_card("spx",    spx_series,          "S&P 500"),
        "qqq":    _index_card("qqq",    yf_data.get("qqq"),  "Nasdaq 100"),
        "iwm":    _index_card("iwm",    yf_data.get("iwm"),  "Russell 2000"),
        "nasdaq": _index_card("nasdaq", yf_data.get("nasdaq"),"Nasdaq Composite"),
    }

    sectors = {
        "soxx": _sector_card("soxx", yf_data.get("soxx"), "Semiconductors (SOXX)", spx_series),
        "xlf":  _sector_card("xlf",  yf_data.get("xlf"),  "Banks / Financials (XLF)", spx_series),
        "iyt":  _sector_card("iyt",  yf_data.get("iyt"),  "Transports (IYT)", spx_series),
        "xlk":  _sector_card("xlk",  yf_data.get("xlk"),  "Technology (XLK)", spx_series),
        "xle":  _sector_card("xle",  yf_data.get("xle"),  "Energy (XLE)", spx_series),
        "xlu":  _sector_card("xlu",  yf_data.get("xlu"),  "Utilities (XLU)", spx_series),
    }

    breadth = _build_breadth(yf_data.get("spy"), yf_data.get("rsp"))

    vix_card = None
    vix_series = yf_data.get("vix")
    if vix_series is not None and len(vix_series) >= 5:
        vix_vals = vix_series.tolist()
        vix_cur  = vix_vals[-1]
        vix_d5   = vix_vals[-6] if len(vix_vals) >= 6 else vix_vals[0]
        vix_d20  = vix_vals[-21] if len(vix_vals) >= 21 else vix_vals[0]
        pctile   = _pct_rank(vix_vals, vix_cur)

        if vix_cur >= 30:
            vix_alert, vix_level = "Fear/panic — market stress elevated", "extreme"
        elif vix_cur >= 20:
            vix_alert, vix_level = "Elevated — above normal uncertainty", "notable"
        elif vix_cur <= 13:
            vix_alert, vix_level = "Complacent — historically low, watch for mean reversion", "notable"
        else:
            vix_alert, vix_level = "Normal range", "none"

        vix_card = {
            "current":    round(vix_cur, 2),
            "d5_chg":     f"{vix_cur - vix_d5:+.2f}",
            "d20_chg":    f"{vix_cur - vix_d20:+.2f}",
            "percentile": pctile,
            "alert":      vix_alert,
            "alert_level": vix_level,
            "spark":      _spark(vix_vals),
        }

    assessment = _market_assessment(indices, sectors, breadth, vix_series)

    result = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "indices":    indices,
        "sectors":    sectors,
        "breadth":    breadth,
        "vix":        vix_card,
        "assessment": assessment,
        # Legacy field for any existing consumers of /sector-flows/metrics
        "legacy_sectors": {
            k: {"name": v.get("name", k), "ticker": k,
                "current": v.get("current"), "change_5d": None,
                "flow_signal": "Stable", "mfi": None, "obv": None,
                "volume_momentum": None, "relative_performance": v.get("rel_5d_raw")}
            for k, v in sectors.items()
        },
    }

    _cache["data"] = result
    _cache["ts"]   = now
    return result

# ── Routes ────────────────────────────────────────────────────────────────────

@sector_flows_router.get("/sector-flows/metrics")
def get_sector_flows_metrics():
    """Legacy endpoint — returns equity internals. Kept for backward compat."""
    return _build_equity_metrics()


@sector_flows_router.get("/equity/metrics")
def get_equity_metrics():
    """Equity internals: indices, sectors, breadth, VIX, market assessment."""
    return _build_equity_metrics()


@sector_flows_router.get("/equity/cache/flush")
def flush_equity_cache():
    global _cache
    _cache = {"data": None, "ts": 0.0}
    return {"flushed": True}

# ── Registration ──────────────────────────────────────────────────────────────
#
#   Replace existing sector_flows_routes.py with this file.
#   main.py stays the same — just change the import:
#
#   OLD: from sector_flows_routes import sector_flows_router
#   NEW: from equity_routes import sector_flows_router
#        app.include_router(sector_flows_router)
#
# Endpoints:
#   GET /sector-flows/metrics   (legacy)
#   GET /equity/metrics         (canonical)
#   GET /equity/cache/flush
