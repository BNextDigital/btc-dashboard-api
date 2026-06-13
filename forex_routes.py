"""
forex_routes.py — USD & Forex Dashboard backend

"FX is wind between cities."

Instruments covered:
  DXY       — overall wind direction (DX-Y.NYB via yFinance)
  EUR/USD   — Europe block, primary DXY counterweight
  USD/JPY   — carry trade barometer (cheap JPY funding)
  USD/CNH   — Asia risk signal, China stress indicator
  EM FX     — Emerging market basket (BRL, MXN, INR, KRW, ZAR)
  FX Vol    — ^EVZ Euro FX vol proxy + FRED DTWEXBGS broad dollar index
  Carry     — Derived: USD/JPY momentum + JPY-vs-peers cross stress signal

Data source: yFinance (all pairs) + FRED DTWEXBGS (broad trade-weighted USD)
Cache: 5 minutes (FX ticks frequently but analysis is daily-resolution)

Setup:
  1. Copy to btc-dashboard-api/forex_routes.py
  2. In main.py add:
       from forex_routes import forex_router
       app.include_router(forex_router)

Endpoints:
  GET /forex/metrics            — all pairs + EM basket + carry + wind assessment
  GET /forex/cache/flush
"""

import os
import time
import requests
from datetime import datetime, date, timedelta
from fastapi import APIRouter
import yfinance as yf
import pandas as pd

# ── Router ────────────────────────────────────────────────────────────────────
forex_router = APIRouter(prefix="/forex")

# ── Config ────────────────────────────────────────────────────────────────────
FRED_API_KEY  = os.getenv("FRED_API_KEY", "")
FRED_BASE     = "https://api.stlouisfed.org/fred/series/observations"
CACHE_TTL     = 300   # 5 minutes
_cache: dict  = {"data": None, "ts": 0.0}

# ── yFinance tickers ──────────────────────────────────────────────────────────
YF_PAIRS = {
    # Core pairs
    "dxy":      "DX-Y.NYB",   # Dollar index
    "eurusd":   "EURUSD=X",   # EUR/USD (rises = USD weaker)
    "usdjpy":   "JPY=X",      # USD/JPY (rises = USD stronger vs JPY)
    "usdcnh":   "CNH=X",      # USD/CNH (rises = USD stronger vs CNH)
    # EM FX (all quoted as USD per unit of EM ccy — or as units of EM per USD)
    "usdbrl":   "BRL=X",      # USD/BRL — Brazil
    "usdmxn":   "MXN=X",      # USD/MXN — Mexico
    "usdinr":   "INR=X",      # USD/INR — India
    "usdkrw":   "KRW=X",      # USD/KRW — South Korea
    "usdzar":   "ZAR=X",      # USD/ZAR — South Africa
    # FX Volatility
    "evz":      "^EVZ",       # CBOE Euro FX Volatility (FX vol proxy)
}

# FRED broad dollar index (trade-weighted, monthly — for long-run context)
FRED_BROAD_USD = "DTWEXBGS"   # Nominal Broad Dollar Index (daily, 2006-present)

# City metaphor labels
CITY_LABELS = {
    "dxy":    "Overall Wind Direction",
    "eurusd": "European Wind Gauge",
    "usdjpy": "Carry Trade Barometer",
    "usdcnh": "Asia Risk Signal",
    "em_fx":  "Emerging Market Gusts",
    "fxvol":  "Wind Turbulence Index",
    "carry":  "Carry Trade Health",
}

# EM pairs where HIGHER = stronger USD = headwind for EM
EM_PAIRS = ["usdbrl", "usdmxn", "usdinr", "usdkrw", "usdzar"]
EM_LABELS = {
    "usdbrl": "BRL", "usdmxn": "MXN", "usdinr": "INR",
    "usdkrw": "KRW", "usdzar": "ZAR"
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _pct_rank(series: list[float], current: float) -> int:
    if not series or len(series) < 5:
        return 50
    return round(sum(1 for v in series if v < current) / len(series) * 100)


def _spark(vals: list[float], n: int = 12) -> list[float]:
    return [round(v, 4) for v in (vals[-n:] if len(vals) >= n else vals)]


def _chg_pct(current: float, prev: float) -> float:
    if prev == 0:
        return 0.0
    return round((current - prev) / prev * 100, 2)


def _fmt_pair(v: float | None, decimals: int = 4) -> str:
    if v is None:
        return "—"
    return f"{v:.{decimals}f}"


def _delta_str(current: float, prev: float, decimals: int = 4) -> str:
    d = current - prev
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.{decimals}f}"

# ── FRED fetcher ──────────────────────────────────────────────────────────────

def _fred_series(series_id: str, n_obs: int = 120) -> list[tuple[str, float]]:
    if not FRED_API_KEY:
        return []
    try:
        r = requests.get(
            FRED_BASE,
            params={
                "series_id":  series_id,
                "api_key":    FRED_API_KEY,
                "file_type":  "json",
                "sort_order": "desc",
                "limit":      n_obs,
            },
            timeout=10,
        )
        r.raise_for_status()
        obs = r.json().get("observations", [])
        result = []
        for o in reversed(obs):
            try:
                result.append((o["date"], float(o["value"])))
            except (ValueError, KeyError):
                pass
        return result
    except Exception as e:
        print(f"[forex] FRED {series_id} error: {e}")
        return []

# ── yFinance bulk fetch ───────────────────────────────────────────────────────

def _fetch_yf_bulk(n_days: int = 252) -> dict[str, pd.Series | None]:
    """Bulk download — returns {key: pd.Series of close prices}."""
    tickers = list(YF_PAIRS.values())
    result  = {k: None for k in YF_PAIRS}
    try:
        raw   = yf.download(tickers, period=f"{n_days}d",
                             auto_adjust=True, progress=False, threads=True)
        close = raw["Close"] if "Close" in raw.columns else raw

        for key, ticker in YF_PAIRS.items():
            if ticker in close.columns:
                s = close[ticker].dropna()
                result[key] = s if len(s) >= 5 else None
            else:
                result[key] = None
    except Exception as e:
        print(f"[forex] yFinance bulk error: {e}")
    return result

# ── Card builders ─────────────────────────────────────────────────────────────

def _build_dxy_card(series: pd.Series | None) -> dict:
    if series is None or len(series) < 5:
        return {"name": "DXY", "city_label": CITY_LABELS["dxy"], "error": "yFinance unavailable"}

    vals    = series.tolist()
    current = round(vals[-1], 2)
    d5      = vals[-6]  if len(vals) >= 6  else vals[0]
    d20     = vals[-21] if len(vals) >= 21 else vals[0]
    d252    = vals[-252] if len(vals) >= 252 else vals[0]
    pctile  = _pct_rank(vals, current)

    chg5_pct  = _chg_pct(current, d5)
    chg20_pct = _chg_pct(current, d20)

    # DXY level interpretation (recent historical range ~98–108)
    if current >= 108:
        alert_level, alert = "extreme", "DXY very strong — severe global headwind"
    elif current >= 104:
        alert_level, alert = "notable", "DXY elevated — headwind for risk assets and BTC"
    elif current <= 96:
        alert_level, alert = "notable", "DXY weak — tailwind for global risk assets"
    elif current <= 100:
        alert_level, alert = "none", "DXY soft — mild easing of USD pressure"
    else:
        alert_level, alert = "none", "DXY neutral — no strong directional wind"

    # Trend pattern
    if chg20_pct >= 2:
        pattern = f"Rising +{chg20_pct:.1f}% (20d) — dollar gaining, tightening global conditions"
    elif chg20_pct <= -2:
        pattern = f"Falling {chg20_pct:.1f}% (20d) — dollar softening, easing external pressure"
    else:
        pattern = f"Ranging {chg20_pct:+.1f}% (20d) — no strong directional move"

    return {
        "name":        "DXY",
        "city_label":  CITY_LABELS["dxy"],
        "current":     _fmt_pair(current, 2),
        "current_raw": current,
        "d5_chg":      _delta_str(current, d5, 2),
        "d5_pct":      f"{chg5_pct:+.2f}%",
        "d20_chg":     _delta_str(current, d20, 2),
        "d20_pct":     f"{chg20_pct:+.2f}%",
        "yoy_pct":     f"{_chg_pct(current, d252):+.1f}%",
        "percentile":  pctile,
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     pattern,
        "spark":       _spark(vals, 20),
        "source":      "yFinance: DX-Y.NYB",
        "note":        "Rising DXY = strong headwind for BTC and global risk assets. Falling DXY = tailwind.",
    }


def _build_pair_card(
    key: str, name: str, series: pd.Series | None,
    city_label: str,
    level_thresholds: list[tuple[float, str, str]],  # [(value, alert_level, alert_text), ...]
    direction_note: str,      # "higher = USD stronger" or "lower = USD stronger"
    btc_note: str,
    decimals: int = 4,
) -> dict:
    """Generic FX pair card builder."""
    if series is None or len(series) < 5:
        return {"name": name, "city_label": city_label, "error": "yFinance unavailable"}

    vals    = series.tolist()
    current = round(vals[-1], decimals)
    d5      = vals[-6]  if len(vals) >= 6  else vals[0]
    d20     = vals[-21] if len(vals) >= 21 else vals[0]
    d252    = vals[-252] if len(vals) >= 252 else vals[0]
    pctile  = _pct_rank(vals, current)

    chg5_pct  = _chg_pct(current, d5)
    chg20_pct = _chg_pct(current, d20)
    chg_yoy   = _chg_pct(current, d252)

    # Alert from threshold table
    alert_level, alert = "none", "Normal range"
    for thresh, lvl, txt in level_thresholds:
        if current >= thresh:
            alert_level, alert = lvl, txt
            break

    # Trend pattern
    if abs(chg20_pct) >= 3:
        direction = "Rising" if chg20_pct > 0 else "Falling"
        pattern = f"{direction} {chg20_pct:+.1f}% (20d)"
    else:
        pattern = f"Ranging {chg20_pct:+.1f}% (20d)"

    return {
        "name":        name,
        "city_label":  city_label,
        "current":     _fmt_pair(current, decimals),
        "current_raw": current,
        "d5_chg":      _delta_str(current, d5, decimals),
        "d5_pct":      f"{chg5_pct:+.2f}%",
        "d20_chg":     _delta_str(current, d20, decimals),
        "d20_pct":     f"{chg20_pct:+.2f}%",
        "yoy_pct":     f"{chg_yoy:+.1f}%",
        "percentile":  pctile,
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     pattern,
        "direction_note": direction_note,
        "spark":       _spark(vals, 20),
        "note":        btc_note,
    }


def _build_eurusd_card(series: pd.Series | None) -> dict:
    return _build_pair_card(
        key="eurusd",
        name="EUR/USD",
        series=series,
        city_label=CITY_LABELS["eurusd"],
        # EUR/USD: higher = USD weaker = tailwind. Lower = USD stronger = headwind.
        level_thresholds=[
            (1.12, "notable", "EUR/USD strong — USD significantly weakened"),
            (1.09, "none",    "EUR/USD elevated — mild USD softness"),
            (1.00, "none",    "EUR/USD near parity — neutral"),
            (0.00, "notable", "EUR/USD weak — USD dominant vs Europe"),
        ],
        direction_note="Higher = weaker USD (tailwind). Lower = stronger USD (headwind).",
        btc_note="EUR/USD is the largest DXY component (~58%). Rising EUR/USD = DXY falling = favorable for BTC.",
        decimals=4,
    )


def _build_usdjpy_card(series: pd.Series | None) -> dict:
    card = _build_pair_card(
        key="usdjpy",
        name="USD/JPY",
        series=series,
        city_label=CITY_LABELS["usdjpy"],
        # USD/JPY: higher = USD stronger vs JPY. JPY weakness = carry trade alive.
        # Sharp JPY strengthening (USD/JPY falling) = carry unwind risk.
        level_thresholds=[
            (158, "extreme", "JPY very weak — extreme carry trade exposure, unwind risk elevated"),
            (150, "notable", "JPY weak — carry trade crowded, watch for BOJ intervention"),
            (140, "none",    "USD/JPY elevated — carry trade active"),
            (130, "none",    "USD/JPY moderate — carry trade normalizing"),
            (0,   "notable", "JPY strong — potential carry unwind underway"),
        ],
        direction_note="Rising = JPY weakening (carry alive). Falling fast = carry unwind risk.",
        btc_note="Sharp JPY strengthening forces carry unwind → asset sales across risk assets including BTC.",
        decimals=2,
    )
    # Override: add carry-specific fields
    if series is not None and len(series) >= 6:
        vals = series.tolist()
        current = vals[-1]
        d5  = vals[-6] if len(vals) >= 6 else vals[0]
        d20 = vals[-21] if len(vals) >= 21 else vals[0]
        chg5  = _chg_pct(current, d5)
        chg20 = _chg_pct(current, d20)

        # Carry unwind signal: rapid JPY appreciation = USD/JPY falling sharply
        if chg5 <= -3:
            carry_signal = "UNWIND RISK — JPY surging, carry positions under pressure"
            carry_level  = "extreme"
        elif chg5 <= -1.5:
            carry_signal = "Caution — JPY strengthening, watch carry exposure"
            carry_level  = "notable"
        elif chg5 >= 1.5:
            carry_signal = "Carry expanding — JPY weakening, low rate borrowing attractive"
            carry_level  = "none"
        else:
            carry_signal = "Carry stable"
            carry_level  = "none"

        card["carry_signal"] = carry_signal
        card["carry_level"]  = carry_level
        card["chg5_abs"]     = round(current - d5, 2)

    return card


def _build_usdcnh_card(series: pd.Series | None) -> dict:
    return _build_pair_card(
        key="usdcnh",
        name="USD/CNH",
        series=series,
        city_label=CITY_LABELS["usdcnh"],
        # USD/CNH: higher = CNH weaker = Asia stress / USD dominance
        level_thresholds=[
            (7.40, "extreme", "CNH very weak — significant Asia stress / USD dominance"),
            (7.25, "notable", "CNH weak — elevated USD pressure on China"),
            (7.10, "none",    "USD/CNH elevated — watch for PBOC response"),
            (6.90, "none",    "USD/CNH moderate — within recent range"),
            (0,    "notable", "CNH strong — China capital inflows or PBOC support"),
        ],
        direction_note="Rising = weaker CNH, more Asia stress. Falling = CNH strengthening.",
        btc_note="Weak CNH signals regional stress or capital outflows — historically correlated with crypto selling pressure in Asia.",
        decimals=4,
    )


def _build_em_basket(yf_data: dict) -> dict:
    """
    Build EM FX basket — aggregate stress signal from 5 EM pairs.
    All quoted as units of EM currency per 1 USD. Higher = weaker EM = USD stronger.
    """
    em_pairs_data = {}
    stress_scores = []

    for key in EM_PAIRS:
        series = yf_data.get(key)
        label  = EM_LABELS[key]
        if series is None or len(series) < 6:
            em_pairs_data[label] = {"error": "unavailable"}
            continue

        vals    = series.tolist()
        current = round(vals[-1], 4)
        d5      = vals[-6]  if len(vals) >= 6  else vals[0]
        d20     = vals[-21] if len(vals) >= 21 else vals[0]
        pctile  = _pct_rank(vals, current)

        chg5_pct  = _chg_pct(current, d5)
        chg20_pct = _chg_pct(current, d20)

        # Stress: rising USD vs EM = stress. Higher percentile = more USD dominance.
        stress_scores.append(pctile)

        em_pairs_data[label] = {
            "current":    _fmt_pair(current, 4),
            "current_raw": current,
            "d5_pct":     f"{chg5_pct:+.2f}%",
            "d20_pct":    f"{chg20_pct:+.2f}%",
            "percentile": pctile,
            "spark":      _spark(vals, 12),
        }

    # Aggregate EM stress
    if stress_scores:
        avg_pctile = round(sum(stress_scores) / len(stress_scores))
        if avg_pctile >= 80:
            alert_level, alert = "extreme", "EM FX under severe pressure — broad USD dominance"
        elif avg_pctile >= 60:
            alert_level, alert = "notable", "EM FX stressed — USD strengthening broadly"
        elif avg_pctile <= 20:
            alert_level, alert = "none",    "EM FX rallying — USD headwinds easing"
        elif avg_pctile <= 40:
            alert_level, alert = "none",    "EM FX recovering — mild USD softness"
        else:
            alert_level, alert = "none",    "EM FX neutral — mixed signals"
    else:
        avg_pctile, alert_level, alert = 50, "none", "Insufficient data"

    return {
        "name":        "EM FX Basket",
        "city_label":  CITY_LABELS["em_fx"],
        "pairs":       em_pairs_data,
        "avg_percentile": avg_pctile,
        "alert":       alert,
        "alert_level": alert_level,
        "note":        "Rising EM FX pairs (vs USD) = EM stress = USD dominance = headwind for global risk assets.",
    }


def _build_fxvol_card(evz_series: pd.Series | None, broad_usd_obs: list) -> dict:
    """
    FX Volatility card.
    Primary: ^EVZ (CBOE Euro FX Vol — implied vol on EUR/USD options)
    Context: FRED DTWEXBGS broad trade-weighted dollar (trend context)
    """
    card: dict = {
        "name":       "FX Volatility",
        "city_label": CITY_LABELS["fxvol"],
    }

    # EVZ
    if evz_series is not None and len(evz_series) >= 5:
        vals    = evz_series.tolist()
        current = round(vals[-1], 2)
        d5      = vals[-6]  if len(vals) >= 6  else vals[0]
        d20     = vals[-21] if len(vals) >= 21 else vals[0]
        pctile  = _pct_rank(vals, current)

        if current >= 12:
            alert_level, alert = "extreme", "EUR/USD vol elevated — turbulent FX conditions"
        elif current >= 9:
            alert_level, alert = "notable", "FX vol rising — increasing currency uncertainty"
        elif current <= 5:
            alert_level, alert = "none", "FX vol compressed — calm conditions"
        else:
            alert_level, alert = "none", "FX vol normal"

        card.update({
            "evz":         f"{current:.2f}",
            "evz_raw":     current,
            "evz_d5":      _delta_str(current, d5, 2),
            "evz_d20":     _delta_str(current, d20, 2),
            "percentile":  pctile,
            "alert":       alert,
            "alert_level": alert_level,
            "spark":       _spark(vals, 20),
        })
    else:
        card.update({
            "evz": "—",
            "alert": "FX vol data unavailable",
            "alert_level": "none",
            "percentile": None,
        })

    # Broad USD from FRED (trend context only — weekly)
    if broad_usd_obs:
        broad_vals   = [v for _, v in broad_usd_obs]
        broad_current = round(broad_vals[-1], 2)
        broad_d52w   = broad_vals[-53] if len(broad_vals) >= 53 else broad_vals[0]
        broad_yoy    = _chg_pct(broad_current, broad_d52w)

        card["broad_usd"]     = f"{broad_current:.2f}"
        card["broad_usd_yoy"] = f"{broad_yoy:+.1f}% YoY"
        card["broad_note"]    = "FRED DTWEXBGS — nominal broad dollar vs 26 trading partners"
    else:
        card["broad_usd"]  = "—"

    card["note"] = "Rising FX vol = turbulent conditions. Sustained high vol often precedes deleveraging across risk assets."
    return card


def _build_carry_card(usdjpy_card: dict, usdcnh_card: dict, dxy_card: dict) -> dict:
    """
    Carry trade composite assessment.
    Carry is alive when: JPY weak (high USD/JPY), vol low, EM FX stable.
    Carry unwind risk when: JPY surging, vol spiking, EM FX stress.
    """
    carry_signal = usdjpy_card.get("carry_signal", "—")
    carry_level  = usdjpy_card.get("carry_level", "none")

    # Supplement with DXY trend
    dxy_raw = dxy_card.get("current_raw")
    dxy_pct = dxy_card.get("percentile", 50)

    if carry_level == "extreme":
        summary = "CARRY UNWIND — JPY surging. Forced asset sales likely across risk assets including BTC."
        btc_impact = "bearish"
    elif carry_level == "notable" and (dxy_pct or 50) >= 70:
        summary = "Carry under pressure — JPY strengthening + strong USD. Watch for risk-off cascade."
        btc_impact = "bearish"
    elif carry_level == "none" and (dxy_pct or 50) <= 40:
        summary = "Carry conditions supportive — JPY weak, USD not surging. Low-rate funding accessible."
        btc_impact = "neutral"
    else:
        summary = "Carry stable — no imminent unwind signal."
        btc_impact = "neutral"

    return {
        "name":        "Carry Trade Health",
        "city_label":  CITY_LABELS["carry"],
        "signal":      carry_signal,
        "alert_level": carry_level,
        "summary":     summary,
        "btc_impact":  btc_impact,
        "note":        "JPY carry: borrow cheap yen, invest elsewhere. Rapid JPY strengthening forces unwind → sell-off across risk assets.",
    }


def _build_wind_assessment(
    dxy: dict, eurusd: dict, usdjpy: dict, usdcnh: dict, em: dict
) -> dict:
    """
    Top-level wind direction summary for BTC.
    Aggregates all FX signals into a directional read.
    """
    headwinds = []
    tailwinds = []

    # DXY
    dxy_lvl = dxy.get("alert_level", "none")
    if dxy_lvl == "extreme" and "strong" in dxy.get("alert", "").lower():
        headwinds.append("DXY elevated — severe global headwind")
    elif dxy_lvl == "notable" and "strong" in dxy.get("alert", "").lower():
        headwinds.append("DXY elevated — notable headwind for risk assets")
    elif dxy_lvl in ("none", "notable") and "weak" in dxy.get("alert", "").lower():
        tailwinds.append("DXY weak — global USD pressure easing")

    # EUR/USD (proxy for DXY inverse)
    eur_raw = eurusd.get("current_raw")
    if eur_raw and eur_raw >= 1.10:
        tailwinds.append("EUR/USD strong — USD meaningfully weaker vs Europe")
    elif eur_raw and eur_raw <= 1.02:
        headwinds.append("EUR/USD weak — USD dominant vs European block")

    # JPY carry
    carry_lvl = usdjpy.get("carry_level", "none")
    if carry_lvl == "extreme":
        headwinds.append("JPY carry unwind — forced asset liquidation risk")
    elif carry_lvl == "notable":
        headwinds.append("JPY strengthening — carry trade under pressure")

    # CNH
    cnh_lvl = usdcnh.get("alert_level", "none")
    if cnh_lvl in ("extreme", "notable"):
        headwinds.append("CNH weak — Asia stress / USD dominance in region")

    # EM FX
    em_lvl = em.get("alert_level", "none")
    if em_lvl == "extreme":
        headwinds.append("EM FX broadly stressed — global USD tightening")
    elif em_lvl == "none" and em.get("avg_percentile", 50) <= 30:
        tailwinds.append("EM FX recovering — USD pressure easing globally")

    # Determine overall
    if len(headwinds) >= 3:
        direction = "Strong Headwind"
        color_level = "extreme"
        read = "Multiple FX stress signals active — USD dominance is meaningful for BTC and global risk assets."
    elif len(headwinds) >= 2:
        direction = "Moderate Headwind"
        color_level = "notable"
        read = "USD pressure building across FX markets. Monitor JPY and CNH for escalation."
    elif len(tailwinds) >= 2:
        direction = "Tailwind"
        color_level = "none"
        read = "USD retreating on multiple fronts. FX conditions supportive for risk assets."
    elif len(tailwinds) >= 1 and len(headwinds) == 0:
        direction = "Mild Tailwind"
        color_level = "none"
        read = "USD softening but no strong directional signal yet."
    else:
        direction = "Neutral"
        color_level = "none"
        read = "FX wind mixed — no clear directional bias from currency markets."

    return {
        "direction":   direction,
        "color_level": color_level,
        "read":        read,
        "headwinds":   headwinds,
        "tailwinds":   tailwinds,
    }

# ── Main builder ──────────────────────────────────────────────────────────────

def _build_forex_metrics() -> dict:
    global _cache
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    # Fetch all in parallel via bulk yFinance
    yf_data = _fetch_yf_bulk(n_days=252)

    # FRED broad dollar (optional, tolerates failure)
    broad_usd_obs = _fred_series(FRED_BROAD_USD, n_obs=120)

    # Build cards
    dxy    = _build_dxy_card(yf_data.get("dxy"))
    eurusd = _build_eurusd_card(yf_data.get("eurusd"))
    usdjpy = _build_usdjpy_card(yf_data.get("usdjpy"))
    usdcnh = _build_usdcnh_card(yf_data.get("usdcnh"))
    em     = _build_em_basket(yf_data)
    fxvol  = _build_fxvol_card(yf_data.get("evz"), broad_usd_obs)
    carry  = _build_carry_card(usdjpy, usdcnh, dxy)
    wind   = _build_wind_assessment(dxy, eurusd, usdjpy, usdcnh, em)

    result = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "dxy":        dxy,
        "eurusd":     eurusd,
        "usdjpy":     usdjpy,
        "usdcnh":     usdcnh,
        "em_fx":      em,
        "fxvol":      fxvol,
        "carry":      carry,
        "wind":       wind,
    }

    _cache["data"] = result
    _cache["ts"]   = now
    return result

# ── Routes ────────────────────────────────────────────────────────────────────

@forex_router.get("/metrics")
def get_forex_metrics():
    """
    Returns DXY, EUR/USD, USD/JPY, USD/CNH, EM basket, FX vol, carry health,
    and top-level wind assessment for BTC.
    """
    return _build_forex_metrics()


@forex_router.get("/cache/flush")
def flush_forex_cache():
    global _cache
    _cache = {"data": None, "ts": 0.0}
    return {"flushed": True}

# ── Registration ──────────────────────────────────────────────────────────────
#
#   from forex_routes import forex_router
#   app.include_router(forex_router)
#
# Endpoints:
#   GET /forex/metrics
#   GET /forex/cache/flush
