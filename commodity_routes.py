"""
commodity_routes.py — Commodities: Energy & Materials

"Oil reflects energy demand or supply shocks.
 Copper reflects industrial activity.
 Gold reflects insurance demand or real yield conditions."

Instruments:
  Energy:    WTI Crude (CL=F), Natural Gas (NG=F), Gasoline (RB=F)
  Metals:    Gold (GC=F), Silver (SI=F), Copper (HG=F)
  Grains:    Wheat (ZW=F), Corn (ZC=F), Soybeans (ZS=F)

All via yFinance futures (front month). Daily prices.
Cache: 10 minutes.

Setup:
  1. Copy to btc-dashboard-api/commodity_routes.py
  2. In main.py:
       from commodity_routes import commodity_router
       app.include_router(commodity_router)

Endpoints:
  GET /commodities/metrics
  GET /commodities/cache/flush
"""

import os
import time
import math
from datetime import datetime
from fastapi import APIRouter
import pandas as pd

from shared.yf_cache import get_series as _yf

# ── Router ────────────────────────────────────────────────────────────────────
commodity_router = APIRouter(prefix="/commodities")

# ── Config ────────────────────────────────────────────────────────────────────
CACHE_TTL    = 600   # 10 minutes
_cache: dict = {"data": None, "ts": 0.0}

# ── Tickers ───────────────────────────────────────────────────────────────────
TICKERS = {
    # Energy
    "wti":      ("CL=F",  "WTI Crude",    "$",    "bbl"),
    "natgas":   ("NG=F",  "Natural Gas",  "$",    "MMBtu"),
    "gasoline": ("RB=F",  "RBOB Gasoline","$",    "gal"),
    # Metals
    "gold":     ("GC=F",  "Gold",         "$",    "oz"),
    "silver":   ("SI=F",  "Silver",       "$",    "oz"),
    "copper":   ("HG=F",  "Copper",       "$",    "lb"),
    # Grains
    "wheat":    ("ZW=F",  "Wheat",        "¢",    "bu"),
    "corn":     ("ZC=F",  "Corn",         "¢",    "bu"),
    "soybeans": ("ZS=F",  "Soybeans",     "¢",    "bu"),
}

# Metaphor labels
METAPHOR_LABELS = {
    "wti":      "Energy demand / supply shock signal",
    "natgas":   "Heating & power cost signal",
    "gasoline": "Consumer energy cost",
    "gold":     "Insurance demand / real yield conditions",
    "silver":   "Industrial + monetary hybrid",
    "copper":   "Industrial activity barometer",
    "wheat":    "Food cost pressure",
    "corn":     "Agricultural input cost",
    "soybeans": "Global protein demand",
}

# BTC impact notes per commodity
BTC_NOTES = {
    "wti":      "Rising oil → inflationary pressure → Fed stays tight → headwind for BTC.",
    "natgas":   "High natgas → energy cost spike → input inflation signal.",
    "gasoline": "Retail gasoline = consumer inflation headline. Rising = CPI upside risk.",
    "gold":     "Gold rising with BTC = anti-fiat/debasement narrative active. Gold rising, BTC falling = pure risk-off.",
    "silver":   "Silver outperforming gold = industrial demand strong. Underperforming = risk-off.",
    "copper":   "Dr. Copper: rising = global growth expanding. Falling = contraction signal.",
    "wheat":    "Food inflation is sticky and politically sensitive. High wheat → CPI pressure.",
    "corn":     "Corn feeds livestock and powers ethanol. Broad agricultural inflation proxy.",
    "soybeans": "Soy reflects China demand and global agricultural conditions.",
}

# Alert thresholds (percentile-based + absolute level)
ALERT_CONFIG = {
    "wti":      {"high_pct": 75, "extreme_pct": 90, "high_label": "Energy elevated — inflationary input",    "extreme_label": "Energy very high — strong inflation pressure"},
    "natgas":   {"high_pct": 75, "extreme_pct": 90, "high_label": "Natgas elevated",                          "extreme_label": "Natgas very high — energy cost spike"},
    "gasoline": {"high_pct": 75, "extreme_pct": 90, "high_label": "Gasoline elevated — consumer cost rising", "extreme_label": "Gasoline very high — consumer inflation pressure"},
    "gold":     {"high_pct": 75, "extreme_pct": 90, "high_label": "Gold elevated — insurance demand active",  "extreme_label": "Gold at highs — strong safe-haven / debasement bid"},
    "silver":   {"high_pct": 75, "extreme_pct": 90, "high_label": "Silver elevated",                          "extreme_label": "Silver very high"},
    "copper":   {"high_pct": 70, "extreme_pct": 85, "high_label": "Copper elevated — industrial demand firm", "extreme_label": "Copper surging — strong global growth signal"},
    "wheat":    {"high_pct": 75, "extreme_pct": 90, "high_label": "Wheat elevated — food inflation risk",     "extreme_label": "Wheat very high — food price spike"},
    "corn":     {"high_pct": 75, "extreme_pct": 90, "high_label": "Corn elevated",                            "extreme_label": "Corn very high"},
    "soybeans": {"high_pct": 75, "extreme_pct": 90, "high_label": "Soybeans elevated",                        "extreme_label": "Soybeans very high — global demand surge"},
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _san(v) -> float | None:
    if v is None: return None
    try:
        return None if (math.isnan(v) or math.isinf(v)) else v
    except (TypeError, ValueError):
        return None


def _pct_rank(vals: list[float], current: float) -> int:
    if not vals or len(vals) < 5: return 50
    return round(sum(1 for v in vals if v < current) / len(vals) * 100)


def _spark(vals: list[float], n: int = 20) -> list[float]:
    trimmed = vals[-n:] if len(vals) >= n else vals
    return [round(v, 3) for v in trimmed]


def _chg_pct(a: float, b: float) -> float | None:
    if b == 0: return None
    return round((a - b) / b * 100, 2)

# ── Shared yFinance fetch ─────────────────────────────────────────────────────

def _fetch_bulk(n_days: int = 252) -> dict[str, pd.Series | None]:
    """Read commodity series from the process-wide shared yFinance cache."""
    return {key: _yf(key) for key in TICKERS}

# ── Card builder ──────────────────────────────────────────────────────────────

def _commodity_card(key: str, series: pd.Series | None) -> dict:
    ticker, name, prefix, unit = TICKERS[key]
    metaphor  = METAPHOR_LABELS.get(key, "")
    btc_note  = BTC_NOTES.get(key, "")
    cfg       = ALERT_CONFIG.get(key, {})

    if series is None or len(series) < 5:
        return {
            "key": key, "name": name, "metaphor": metaphor,
            "current": "—", "error": "yFinance unavailable",
        }

    vals    = series.tolist()
    current = vals[-1]
    d5      = vals[-6]   if len(vals) >= 6   else vals[0]
    d20     = vals[-21]  if len(vals) >= 21  else vals[0]
    d252    = vals[-252] if len(vals) >= 252 else vals[0]
    pctile  = _pct_rank(vals, current)

    chg5   = _chg_pct(current, d5)
    chg20  = _chg_pct(current, d20)
    chg_yoy = _chg_pct(current, d252)

    # Alert based on percentile rank
    hi_pct  = cfg.get("high_pct", 75)
    ext_pct = cfg.get("extreme_pct", 90)
    lo_pct  = 25

    if pctile >= ext_pct:
        alert_level, alert = "extreme", cfg.get("extreme_label", f"{name} at 52w high")
    elif pctile >= hi_pct:
        alert_level, alert = "notable", cfg.get("high_label", f"{name} elevated")
    elif pctile <= lo_pct:
        alert_level, alert = "none", f"{name} depressed — near 52w low"
    else:
        alert_level, alert = "none", "Normal range"

    # Trend pattern
    if chg20 is not None:
        if chg20 >= 5:
            pattern = f"Rising strongly +{chg20:.1f}% (20d)"
        elif chg20 >= 2:
            pattern = f"Rising +{chg20:.1f}% (20d)"
        elif chg20 <= -5:
            pattern = f"Falling {chg20:.1f}% (20d)"
        elif chg20 <= -2:
            pattern = f"Declining {chg20:.1f}% (20d)"
        else:
            pattern = f"Ranging {chg20:+.1f}% (20d)"
    else:
        pattern = "—"

    # Format current value
    if prefix == "¢":
        curr_str = f"{prefix}{current:.2f}/{unit}"
    else:
        curr_str = f"{prefix}{current:.2f}/{unit}"

    return {
        "key":         key,
        "name":        name,
        "ticker":      ticker,
        "metaphor":    metaphor,
        "btc_note":    btc_note,
        "current":     curr_str,
        "current_raw": round(current, 3),
        "d5_pct":      f"{chg5:+.2f}%" if chg5 is not None else "—",
        "d20_pct":     f"{chg20:+.2f}%" if chg20 is not None else "—",
        "yoy_pct":     f"{chg_yoy:+.1f}%" if chg_yoy is not None else "—",
        "percentile":  pctile,
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     pattern,
        "spark":       _spark(vals),
    }

# ── Cross-commodity reads ─────────────────────────────────────────────────────

def _copper_gold_ratio(
    copper: dict, gold: dict,
    copper_series: pd.Series | None,
    gold_series: pd.Series | None,
) -> dict:
    """
    Copper/Gold ratio — economic activity vs safe-haven demand.
    Rising ratio = growth optimism. Falling ratio = risk-off / growth fear.

    Percentile-based, computed from the actual historical ratio series
    (Cu $/lb ÷ Au $/oz × 1000), rather than fixed absolute thresholds.

    The previous version used hardcoded cutoffs (>0.35 / >0.25 / <0.15)
    that were calibrated for the *unscaled* ratio (~0.0005-0.002), but the
    code scales by 1000 before comparing (~0.5-2.5 in practice). At that
    scale the ratio almost always exceeds 0.35, so the read was stuck on
    "elevated — growth optimism" regardless of where conditions actually
    sat historically — especially once gold re-rated sharply higher and
    compressed the ratio toward the low end of its own range. Percentile
    rank against the trailing series self-calibrates as both metals move.
    """
    cu = copper.get("current_raw")
    au = gold.get("current_raw")

    if cu is None or au is None or au == 0:
        return {"ratio": "—", "read": "Data unavailable", "alert_level": "none"}

    # Copper is in $/lb, Gold in $/oz — scale by 1000 for a readable ratio
    current_ratio = round(cu / au * 1000, 4)

    # Build the historical ratio series from the same lookback window used
    # for the individual commodity cards, so the percentile reflects the
    # ratio's own trailing range rather than a fixed cutoff.
    ratio_series: list[float] | None = None
    if copper_series is not None and gold_series is not None:
        try:
            aligned = pd.concat(
                [copper_series.rename("cu"), gold_series.rename("au")],
                axis=1, join="inner",
            ).dropna()
            if len(aligned) >= 10:
                ratio_series = (aligned["cu"] / aligned["au"] * 1000).tolist()
        except Exception as e:
            print(f"[commodities] copper/gold ratio series error: {e}")
            ratio_series = None

    pctile = _pct_rank(ratio_series, current_ratio) if ratio_series else 50

    if pctile >= 75:
        read  = "Ratio elevated vs its own range — industrial demand strong relative to gold, growth optimism"
        level = "none"
    elif pctile <= 25:
        read  = "Ratio depressed vs its own range — gold dominating, growth fears elevated, risk-off"
        level = "notable"
    else:
        read  = "Ratio in normal range — balanced growth / safe-haven demand"
        level = "none"

    result = {
        "ratio":       f"{current_ratio:.4f}",
        "ratio_raw":   current_ratio,
        "read":        read,
        "alert_level": level,
        "note":        "Cu/Au rising = growth confidence. Falling = defensiveness / risk-off. Percentile is vs the ratio's own 52w range.",
    }
    if ratio_series:
        result["percentile"] = pctile
        result["spark"] = _spark(ratio_series, 20)
    return result


def _energy_complex_read(wti: dict, natgas: dict) -> dict:
    """Synthesize energy complex signal."""
    wti_pct = wti.get("percentile", 50)
    ng_pct  = natgas.get("percentile", 50)

    avg = (wti_pct + ng_pct) / 2

    if avg >= 80:
        read  = "Energy complex elevated — inflationary input costs high, Fed constraint reinforced"
        level = "extreme"
    elif avg >= 65:
        read  = "Energy prices above normal — moderate inflation pressure from energy"
        level = "notable"
    elif avg <= 30:
        read  = "Energy complex soft — disinflationary input, supportive for Fed easing"
        level = "none"
    else:
        read  = "Energy prices in normal range — neutral inflation signal"
        level = "none"

    return {"read": read, "alert_level": level, "avg_percentile": round(avg)}


def _commodity_assessment(cards: dict) -> dict:
    """
    Top-level commodity read for BTC macro context.
    """
    headwinds, tailwinds = [], []

    wti   = cards.get("energy", {}).get("wti", {})
    cu    = cards.get("metals", {}).get("copper", {})
    gold  = cards.get("metals", {}).get("gold", {})
    wheat = cards.get("grains", {}).get("wheat", {})

    # Energy
    if wti.get("alert_level") in ("extreme", "notable"):
        headwinds.append(f"WTI crude {wti.get('alert', '')} — energy inflation pressure")
    elif wti.get("percentile", 50) <= 30:
        tailwinds.append("WTI soft — energy disinflationary, Fed has more room")

    # Copper (growth signal)
    cu_pct = cu.get("percentile", 50)
    if cu_pct >= 75:
        tailwinds.append("Copper elevated — industrial demand firm, global growth supportive")
    elif cu_pct <= 25:
        headwinds.append("Copper weak — industrial demand contracting, global growth concern")

    # Gold (safe haven / debasement)
    gold_alert = gold.get("alert_level", "none")
    gold_pct   = gold.get("percentile", 50)
    if gold_alert in ("extreme", "notable") and gold_pct >= 75:
        # Gold high can be bullish OR bearish for BTC depending on narrative
        tailwinds.append("Gold elevated — debasement narrative active, may support BTC")

    # Food/grain inflation
    if wheat.get("alert_level") in ("extreme", "notable"):
        headwinds.append("Grain prices elevated — food inflation adds to CPI stickiness")

    # Overall
    n_hw, n_tw = len(headwinds), len(tailwinds)
    if n_hw >= 2:
        regime, level = "Commodity Headwinds", "notable"
        read = "Energy and/or grain prices elevated — commodity complex adding to inflationary pressures. Reinforces Fed hawkishness."
    elif n_tw >= 2:
        regime, level = "Commodity Tailwinds", "none"
        read = "Soft energy + strong copper = disinflationary and growth-positive. Ideal commodity backdrop for BTC and risk assets."
    elif n_tw == 1 and n_hw == 0:
        regime, level = "Mild Tailwinds", "none"
        read = "Commodity complex broadly neutral with mild positive signals. No major headwinds."
    else:
        regime, level = "Neutral", "none"
        read = "Commodity complex sending mixed signals. Monitor copper (growth) and WTI (inflation) for direction."

    return {
        "regime":       regime,
        "regime_level": level,
        "read":         read,
        "headwinds":    headwinds,
        "tailwinds":    tailwinds,
    }

# ── Main builder ──────────────────────────────────────────────────────────────

def _build_commodity_metrics() -> dict:
    global _cache
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    yf_data = _fetch_bulk(n_days=252)

    energy = {
        "wti":      _commodity_card("wti",      yf_data.get("wti")),
        "natgas":   _commodity_card("natgas",   yf_data.get("natgas")),
        "gasoline": _commodity_card("gasoline", yf_data.get("gasoline")),
    }
    metals = {
        "gold":   _commodity_card("gold",   yf_data.get("gold")),
        "silver": _commodity_card("silver", yf_data.get("silver")),
        "copper": _commodity_card("copper", yf_data.get("copper")),
    }
    grains = {
        "wheat":    _commodity_card("wheat",    yf_data.get("wheat")),
        "corn":     _commodity_card("corn",     yf_data.get("corn")),
        "soybeans": _commodity_card("soybeans", yf_data.get("soybeans")),
    }

    all_cards = {"energy": energy, "metals": metals, "grains": grains}

    cu_gold  = _copper_gold_ratio(
        metals["copper"], metals["gold"],
        yf_data.get("copper"), yf_data.get("gold"),
    )
    energy_r = _energy_complex_read(energy["wti"], energy["natgas"])
    assessment = _commodity_assessment(all_cards)

    result = {
        "updated_at":      datetime.utcnow().isoformat() + "Z",
        "energy":          energy,
        "metals":          metals,
        "grains":          grains,
        "copper_gold":     cu_gold,
        "energy_complex":  energy_r,
        "assessment":      assessment,
    }

    _cache["data"] = result
    _cache["ts"]   = now
    return result

# ── Routes ────────────────────────────────────────────────────────────────────

@commodity_router.get("/metrics")
def get_commodity_metrics():
    """
    Returns energy (WTI, natgas, gasoline), metals (gold, silver, copper),
    grains (wheat, corn, soybeans), copper/gold ratio, and commodity assessment.
    """
    return _build_commodity_metrics()


@commodity_router.get("/cache/flush")
def flush_commodity_cache():
    global _cache
    _cache = {"data": None, "ts": 0.0}
    return {"flushed": True}

# ── Registration ──────────────────────────────────────────────────────────────
#
#   from commodity_routes import commodity_router
#   app.include_router(commodity_router)
#
# Endpoints:
#   GET /commodities/metrics
#   GET /commodities/cache/flush
