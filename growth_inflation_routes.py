"""
growth_inflation_routes.py — Growth & Inflation Dashboard backend

Two metaphors unified:
  INFLATION = pipe temperature
    Too hot → Fed cannot ease → risk assets under pressure
    Cooling  → space for easing → supportive for risk assets
    Too-fast decline → demand collapse → recession risk

  GROWTH/EMPLOYMENT = city income & activity
    Balanced preferred: growth slowing from hot, inflation easing,
    employment stable, no recession signal

All data from FRED (free API key). Monthly/quarterly releases —
cache is 4 hours (data doesn't change intraday).

FRED series used:
  Inflation:
    CPIAUCSL    — CPI headline (monthly, % change)
    CPILFESL    — Core CPI ex food/energy (monthly)
    PCEPI       — PCE headline (monthly)
    PCEPILFE    — Core PCE (monthly) ← Fed's primary target
    PPIFID      — PPI Final Demand (monthly)
    CES0500000003 — Avg Hourly Earnings, private (monthly)
    T5YIE       — 5Y Breakeven inflation (daily)
    T10YIE      — 10Y Breakeven inflation (daily)
    CUSR0000SEHA  — CPI Rent of primary residence (monthly)
    CUSR0000SEHC  — CPI OER / Owners' Equivalent Rent (monthly)
    DCOILWTICO  — WTI crude oil (daily)
    GASREGCOVW  — US retail gasoline avg (weekly)

  Growth / Employment:
    PAYEMS      — Nonfarm payrolls (monthly, thousands)
    UNRATE      — Unemployment rate (monthly, %)
    ICSA        — Initial jobless claims (weekly — fastest signal)
    CCSA        — Continuing claims (weekly)
    JTSJOL      — JOLTS job openings (monthly)
    A191RL1Q225SBEA — Real GDP growth rate (quarterly, %)
    RSAFS       — Retail sales (monthly)
    UMCSENT     — U Michigan consumer sentiment (monthly)
    MICH        — U Michigan inflation expectations (monthly)
    NAPM        — ISM Manufacturing PMI (monthly) [if available]

Setup:
  1. Existing FRED_API_KEY env var (same key as macro_routes.py)
  2. Copy to btc-dashboard-api/growth_inflation_routes.py
  3. In main.py:
       from growth_inflation_routes import growth_router
       app.include_router(growth_router)

Endpoints:
  GET /growth/metrics   — full payload: inflation + growth + pipe/city reads
  GET /growth/cache/flush
"""

import os
import time
import requests
from datetime import datetime, date, timedelta
from fastapi import APIRouter

# ── Router ────────────────────────────────────────────────────────────────────
growth_router = APIRouter(prefix="/growth")

# ── Config ────────────────────────────────────────────────────────────────────
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"
CACHE_TTL    = 14400   # 4 hours — monthly/weekly data, no need to poll more
_cache: dict = {"data": None, "ts": 0.0}

# ── FRED series map ───────────────────────────────────────────────────────────
# All series IDs with metadata for formatting
FRED_SERIES = {
    # ── Inflation ──────────────────────────────────────────────────────────
    "cpi":       ("CPIAUCSL",      "monthly",   "index"),   # level index, we compute YoY
    "core_cpi":  ("CPILFESL",      "monthly",   "index"),
    "pce":       ("PCEPI",         "monthly",   "index"),
    "core_pce":  ("PCEPILFE",      "monthly",   "index"),   # Fed's primary target
    "ppi":       ("PPIFID",        "monthly",   "index"),
    "wages":     ("CES0500000003", "monthly",   "dollars"), # avg hourly earnings
    "be_5y":     ("T5YIE",         "daily",     "pct"),     # 5Y breakeven
    "be_10y":    ("T10YIE",        "daily",     "pct"),     # 10Y breakeven
    "rent":      ("CUSR0000SEHA",  "monthly",   "index"),   # rent of primary residence
    "oer":       ("CUSR0000SEHC",  "monthly",   "index"),   # owners' equivalent rent
    "oil":       ("DCOILWTICO",    "daily",     "dollars"), # WTI crude
    "gasoline":  ("GASREGCOVW",    "weekly",    "dollars"), # retail gasoline
    # ── Growth / Employment ────────────────────────────────────────────────
    "payrolls":  ("PAYEMS",        "monthly",   "thousands"),
    "unrate":    ("UNRATE",        "monthly",   "pct"),
    "claims":    ("ICSA",          "weekly",    "thousands"),
    "cont_claims":("CCSA",         "weekly",    "thousands"),
    "jolts":     ("JTSJOL",        "monthly",   "thousands"),
    "gdp":       ("A191RL1Q225SBEA","quarterly","pct"),
    "retail":    ("RSAFS",         "monthly",   "millions"),
    "sentiment": ("UMCSENT",       "monthly",   "index"),
    "inf_exp":   ("MICH",          "monthly",   "pct"),     # 1Y inflation expectations
    "ism":       ("NAPM",          "monthly",   "index"),   # ISM manufacturing PMI
}

# ── City/pipe metaphor labels ─────────────────────────────────────────────────
CITY_LABELS = {
    "cpi":        "Headline Pipe Temperature",
    "core_cpi":   "Core Pipe Temperature",
    "pce":        "Fed's Thermometer",
    "core_pce":   "Fed's Primary Target",
    "ppi":        "Upstream Pipe Pressure",
    "wages":      "Labor Cost Pressure",
    "be_5y":      "Market's 5Y Temperature Expectation",
    "be_10y":     "Market's 10Y Temperature Expectation",
    "rent":       "Housing Pipe Temperature",
    "oer":        "OER Pipe (Sticky)",
    "oil":        "Fuel Cost (Energy Pressure)",
    "gasoline":   "Retail Fuel Price",
    "payrolls":   "City Job Creation",
    "unrate":     "City Unemployment Rate",
    "claims":     "Weekly Job Loss Signal",
    "cont_claims":"Sustained Unemployment Pressure",
    "jolts":      "City Job Opening Count",
    "gdp":        "City Total Output Growth",
    "retail":     "Consumer Spending Activity",
    "sentiment":  "Citizen Confidence Index",
    "inf_exp":    "Citizens' Inflation Expectations",
    "ism":        "Factory Activity Gauge",
}

# ── FRED fetcher ──────────────────────────────────────────────────────────────

def _fred(series_id: str, n_obs: int = 60) -> list[tuple[str, float]]:
    """
    Fetch last n_obs observations from FRED.
    Returns [(date_str, float), ...] oldest-first, missing values skipped.
    """
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
            timeout=12,
        )
        r.raise_for_status()
        obs = r.json().get("observations", [])
        result = []
        for o in reversed(obs):   # oldest-first
            try:
                result.append((o["date"], float(o["value"])))
            except (ValueError, KeyError):
                pass
        return result
    except Exception as e:
        print(f"[growth] FRED {series_id}: {e}")
        return []

# ── Helpers ───────────────────────────────────────────────────────────────────

def _pct_rank(vals: list[float], current: float) -> int:
    if not vals or len(vals) < 3:
        return 50
    return round(sum(1 for v in vals if v < current) / len(vals) * 100)


def _spark(vals: list[float], n: int = 12) -> list[float]:
    return [round(v, 3) for v in (vals[-n:] if len(vals) >= n else vals)]


def _yoy(obs: list[tuple[str, float]]) -> float | None:
    """Compute YoY % change from index series (12 monthly obs apart)."""
    vals = [v for _, v in obs]
    if len(vals) < 13:
        return None
    curr = vals[-1]
    prev = vals[-13]   # 12 months ago
    if prev == 0:
        return None
    return round((curr - prev) / prev * 100, 2)


def _mom(obs: list[tuple[str, float]]) -> float | None:
    """Month-over-month % change."""
    vals = [v for _, v in obs]
    if len(vals) < 2:
        return None
    curr, prev = vals[-1], vals[-2]
    if prev == 0:
        return None
    return round((curr - prev) / prev * 100, 2)


def _sign(v: float | None) -> str:
    if v is None:
        return ""
    return "+" if v >= 0 else ""


def _fmt_pct(v: float | None, digits: int = 1) -> str:
    if v is None:
        return "—"
    return f"{_sign(v)}{v:.{digits}f}%"


def _fmt_num(v: float | None, digits: int = 1) -> str:
    if v is None:
        return "—"
    return f"{v:.{digits}f}"

# ── Inflation card builder ────────────────────────────────────────────────────

def _inflation_card(
    key: str,
    obs: list[tuple[str, float]],
    target: float | None = None,         # Fed target (2.0 for core PCE)
    is_rate: bool = False,               # True for breakevens — already in % form
    unit: str = "%",
) -> dict:
    """
    Generic inflation metric card.
    For index series: compute YoY and MoM.
    For rate series (breakevens): use latest value directly.
    """
    city_label = CITY_LABELS.get(key, "")
    if not obs:
        return {
            "key": key, "city_label": city_label,
            "current": "—", "error": "FRED unavailable",
        }

    vals = [v for _, v in obs]
    latest_date = obs[-1][0]
    current_val = vals[-1]

    if is_rate:
        # Breakeven — value is already % annualized
        current_pct = round(current_val, 2)
        yoy_chg     = None
        mom_chg     = _mom(obs)   # daily series, so "mom" = recent delta
        pctile      = _pct_rank(vals, current_val)
    else:
        # Index-level series — compute YoY and MoM
        current_pct = _yoy(obs)
        mom_chg     = _mom(obs)
        pctile      = _pct_rank(
            [_yoy(obs[:i]) or 0 for i in range(13, len(obs)+1)],
            current_pct or 0
        )
        yoy_chg = current_pct

    # Pipe temperature alert
    if current_pct is None:
        alert_level, alert = "none", "Insufficient history"
    elif target is not None:
        above = current_pct - target
        if above >= 2.0:
            alert_level, alert = "extreme", f"Pipes overheating +{above:.1f}% above target — Fed cannot ease"
        elif above >= 1.0:
            alert_level, alert = "notable", f"Elevated +{above:.1f}% above target — policy stays tight"
        elif above >= 0:
            alert_level, alert = "none", f"Approaching target — cooling in progress"
        else:
            alert_level, alert = "none", f"At or below {target}% target — pipe temperature normalizing"
    else:
        # No target — use absolute level heuristics
        if current_pct >= 6:
            alert_level, alert = "extreme", "Very hot — severe policy constraint"
        elif current_pct >= 4:
            alert_level, alert = "notable", "Elevated — above comfortable range"
        elif current_pct <= 0:
            alert_level, alert = "notable", "Deflation risk — demand collapse signal"
        elif current_pct <= 1:
            alert_level, alert = "none", "Very cool — below typical comfort range"
        else:
            alert_level, alert = "none", "Normal range"

    # Trend pattern
    if mom_chg is not None:
        if mom_chg >= 0.3:
            pattern = f"Re-accelerating — MoM {_fmt_pct(mom_chg, 2)} — pipe heating up"
        elif mom_chg >= 0.1:
            pattern = f"Stable — MoM {_fmt_pct(mom_chg, 2)}"
        elif mom_chg <= -0.1:
            pattern = f"Cooling — MoM {_fmt_pct(mom_chg, 2)} — pipe temperature falling"
        else:
            pattern = f"Flat — MoM {_fmt_pct(mom_chg, 2)}"
    else:
        pattern = "—"

    return {
        "key":          key,
        "city_label":   city_label,
        "current":      _fmt_pct(current_pct) if current_pct is not None else "—",
        "current_raw":  current_pct,
        "latest_date":  latest_date,
        "mom":          _fmt_pct(mom_chg, 2),
        "mom_raw":      mom_chg,
        "yoy":          _fmt_pct(yoy_chg) if not is_rate else "—",
        "target":       _fmt_pct(target) if target is not None else "—",
        "percentile":   pctile,
        "alert":        alert,
        "alert_level":  alert_level,
        "pattern":      pattern,
        "spark":        _spark(vals),
    }

# ── Commodity card builder ────────────────────────────────────────────────────

def _commodity_card(key: str, obs: list[tuple[str, float]], unit: str = "$") -> dict:
    city_label = CITY_LABELS.get(key, "")
    if not obs:
        return {"key": key, "city_label": city_label, "current": "—", "error": "FRED unavailable"}

    vals    = [v for _, v in obs]
    current = vals[-1]
    latest_date = obs[-1][0]
    d5      = vals[-6]   if len(vals) >= 6   else vals[0]
    d20     = vals[-21]  if len(vals) >= 21  else vals[0]
    d252    = vals[-253] if len(vals) >= 253  else vals[0]
    pctile  = _pct_rank(vals, current)

    def pct(a, b): return round((a - b) / b * 100, 1) if b else None

    chg5  = pct(current, d5)
    chg20 = pct(current, d20)
    chg_yoy = pct(current, d252)

    if current >= 90:
        alert_level, alert = "extreme", "Energy prices elevated — inflationary pressure"
    elif current >= 75:
        alert_level, alert = "notable", "Energy prices high — adding pipe temperature"
    elif current <= 50:
        alert_level, alert = "none", "Energy prices soft — disinflationary input"
    else:
        alert_level, alert = "none", "Energy prices moderate"

    return {
        "key":          key,
        "city_label":   city_label,
        "current":      f"{unit}{current:.2f}",
        "current_raw":  round(current, 2),
        "latest_date":  latest_date,
        "d5_pct":       _fmt_pct(chg5, 1),
        "d20_pct":      _fmt_pct(chg20, 1),
        "yoy_pct":      _fmt_pct(chg_yoy, 1),
        "percentile":   pctile,
        "alert":        alert,
        "alert_level":  alert_level,
        "spark":        _spark(vals, 20),
    }

# ── Employment card builder ───────────────────────────────────────────────────

def _employment_card(
    key: str,
    obs: list[tuple[str, float]],
    invert_alert: bool = False,  # True for unemployment, claims — rising is bad
    unit: str = "",
    precision: int = 0,
) -> dict:
    """
    Generic employment/growth card.
    invert_alert: if True, HIGH values are bearish (unemployment, claims)
    """
    city_label = CITY_LABELS.get(key, "")
    if not obs:
        return {"key": key, "city_label": city_label, "current": "—", "error": "FRED unavailable"}

    vals    = [v for _, v in obs]
    current = vals[-1]
    latest_date = obs[-1][0]
    prev    = vals[-2] if len(vals) >= 2 else vals[0]
    prev_3m = vals[-4] if len(vals) >= 4 else vals[0]
    prev_12m = vals[-13] if len(vals) >= 13 else vals[0]
    pctile  = _pct_rank(vals, current)

    chg_mom = round(current - prev, precision)
    chg_3m  = round(current - prev_3m, precision)
    chg_yoy = round(current - prev_12m, precision)

    # Effective percentile for alert (inverted for unemployment/claims)
    effective_pctile = (100 - pctile) if invert_alert else pctile

    if effective_pctile >= 80:
        alert_level = "extreme"
    elif effective_pctile >= 65:
        alert_level = "notable"
    elif effective_pctile <= 20:
        alert_level = "none"
    else:
        alert_level = "none"

    # Key-specific alerts
    if key == "unrate":
        if current >= 5.0:
            alert_level, alert = "extreme", f"{current:.1f}% — elevated unemployment, recession risk"
        elif current >= 4.5:
            alert_level, alert = "notable", f"{current:.1f}% — labor market softening"
        elif current <= 3.5:
            alert_level, alert = "none", f"{current:.1f}% — tight labor market"
        else:
            alert_level, alert = "none", f"{current:.1f}% — normal range"
    elif key == "claims":
        if current >= 300:
            alert_level, alert = "extreme", f"{current:.0f}K claims — labor market deteriorating"
        elif current >= 250:
            alert_level, alert = "notable", f"{current:.0f}K claims — elevated, watch trend"
        elif current <= 200:
            alert_level, alert = "none", f"{current:.0f}K claims — healthy"
        else:
            alert_level, alert = "none", f"{current:.0f}K claims — normal range"
    elif key == "payrolls":
        if chg_mom <= 0:
            alert_level, alert = "extreme", "Payrolls negative — potential recession signal"
        elif chg_mom < 100:
            alert_level, alert = "notable", f"+{chg_mom:.0f}K — below-trend job creation"
        elif chg_mom > 300:
            alert_level, alert = "none", f"+{chg_mom:.0f}K — strong job creation"
        else:
            alert_level, alert = "none", f"+{chg_mom:.0f}K — healthy trend"
    elif key == "gdp":
        if current < 0:
            alert_level, alert = "extreme", f"{current:.1f}% — contraction (recession territory)"
        elif current < 1.0:
            alert_level, alert = "notable", f"{current:.1f}% — stagnation, near-zero growth"
        elif current >= 3.0:
            alert_level, alert = "none", f"{current:.1f}% — above-trend growth"
        else:
            alert_level, alert = "none", f"{current:.1f}% — moderate growth"
    elif key == "ism":
        if current < 45:
            alert_level, alert = "extreme", f"ISM {current:.1f} — manufacturing contraction"
        elif current < 50:
            alert_level, alert = "notable", f"ISM {current:.1f} — below expansion threshold"
        elif current >= 55:
            alert_level, alert = "none", f"ISM {current:.1f} — strong expansion"
        else:
            alert_level, alert = "none", f"ISM {current:.1f} — moderate expansion"
    elif key == "sentiment":
        if current <= 60:
            alert_level, alert = "extreme", f"Sentiment {current:.0f} — recession-level pessimism"
        elif current <= 75:
            alert_level, alert = "notable", f"Sentiment {current:.0f} — below normal confidence"
        elif current >= 95:
            alert_level, alert = "none", f"Sentiment {current:.0f} — elevated consumer confidence"
        else:
            alert_level, alert = "none", f"Sentiment {current:.0f} — normal range"
    else:
        alert = f"{current:,.{precision}f}{unit}"

    # Format values
    if precision == 0:
        curr_str = f"{current:,.0f}{unit}"
        mom_str  = f"{_sign(chg_mom)}{chg_mom:,.0f}{unit}"
        yoy_str  = f"{_sign(chg_yoy)}{chg_yoy:,.0f}{unit}"
    else:
        curr_str = f"{current:.{precision}f}{unit}"
        mom_str  = f"{_sign(chg_mom)}{chg_mom:.{precision}f}{unit}"
        yoy_str  = f"{_sign(chg_yoy)}{chg_yoy:.{precision}f}{unit}"

    return {
        "key":          key,
        "city_label":   city_label,
        "current":      curr_str,
        "current_raw":  current,
        "latest_date":  latest_date,
        "mom":          mom_str,
        "mom_raw":      chg_mom,
        "qoq":          f"{_sign(chg_3m)}{chg_3m:,.{precision}f}{unit}",
        "yoy":          yoy_str,
        "percentile":   pctile,
        "alert":        alert,
        "alert_level":  alert_level,
        "spark":        _spark(vals),
    }

# ── Pipe temperature assessment ───────────────────────────────────────────────

def _pipe_assessment(cards: dict) -> dict:
    """
    Synthesize inflation cards into a pipe temperature read.
    Key question: Is inflation cooling cleanly, re-accelerating, or collapsing?
    """
    core_pce_raw = cards.get("core_pce", {}).get("current_raw")
    core_cpi_raw = cards.get("core_cpi", {}).get("current_raw")
    be_10y_raw   = cards.get("be_10y",  {}).get("current_raw")
    wages_raw    = cards.get("wages",   {}).get("current_raw")  # YoY wage growth

    # Momentum signals
    signals = []
    headwinds, tailwinds = [], []

    if core_pce_raw is not None:
        if core_pce_raw >= 3.5:
            headwinds.append(f"Core PCE {core_pce_raw:.1f}% — Fed's target still far out of reach")
        elif core_pce_raw >= 2.5:
            headwinds.append(f"Core PCE {core_pce_raw:.1f}% — above 2% target, policy remains restrictive")
        elif core_pce_raw <= 2.0:
            tailwinds.append(f"Core PCE {core_pce_raw:.1f}% — at or below Fed target")
        else:
            signals.append(f"Core PCE {core_pce_raw:.1f}% — cooling toward target")

    if be_10y_raw is not None:
        if be_10y_raw >= 2.8:
            headwinds.append(f"10Y breakeven {be_10y_raw:.2f}% — market pricing persistent inflation")
        elif be_10y_raw >= 2.4:
            signals.append(f"10Y breakeven {be_10y_raw:.2f}% — inflation expectations elevated but stable")
        else:
            tailwinds.append(f"10Y breakeven {be_10y_raw:.2f}% — market expects inflation to normalize")

    if wages_raw is not None:
        if wages_raw >= 5.0:
            headwinds.append(f"Wages +{wages_raw:.1f}% YoY — labor cost spiral risk")
        elif wages_raw <= 3.5:
            tailwinds.append(f"Wages +{wages_raw:.1f}% YoY — wage pressure easing")
        else:
            signals.append(f"Wages +{wages_raw:.1f}% YoY — moderate, consistent with 2% inflation")

    # Overall regime
    n_hw, n_tw = len(headwinds), len(tailwinds)
    if n_hw >= 2:
        regime       = "Pipes Overheating"
        regime_level = "extreme"
        regime_read  = "Multiple inflation indicators remain elevated. Fed cannot ease without risking re-ignition. Risk assets face sustained policy headwind."
    elif n_hw == 1 and n_tw == 0:
        regime       = "Elevated — Cooling Slowly"
        regime_level = "notable"
        regime_read  = "Inflation above target but trending down. Policy stays tight until progress is more convincing. BTC can still rally but macro headwind persists."
    elif n_tw >= 2:
        regime       = "Healthy Cooling"
        regime_level = "none"
        regime_read  = "Inflation normalizing without demand collapse. This is the goldilocks path — creates space for policy easing and supports risk assets."
    elif n_tw == 1:
        regime       = "Cooling — Progress Made"
        regime_level = "none"
        regime_read  = "Disinflation underway. If sustained without growth shock, opens door to policy easing. Watch core PCE for confirmation."
    else:
        regime       = "Uncertain"
        regime_level = "none"
        regime_read  = "Mixed signals. Monitor month-over-month trend in core PCE and breakevens for direction."

    return {
        "regime":       regime,
        "regime_level": regime_level,
        "regime_read":  regime_read,
        "headwinds":    headwinds,
        "tailwinds":    tailwinds,
        "signals":      signals,
        "fed_target":   "2.0% Core PCE (primary) · 2.0% Core CPI (reference)",
    }

# ── City income assessment ────────────────────────────────────────────────────

def _city_assessment(cards: dict) -> dict:
    """
    Synthesize growth/employment into a city income read.
    Goldilocks: growth slowing from hot, inflation easing, employment stable, no recession.
    """
    payrolls_raw  = cards.get("payrolls",  {}).get("mom_raw")
    unrate_raw    = cards.get("unrate",    {}).get("current_raw")
    claims_raw    = cards.get("claims",    {}).get("current_raw")
    gdp_raw       = cards.get("gdp",       {}).get("current_raw")
    ism_raw       = cards.get("ism",       {}).get("current_raw")
    sentiment_raw = cards.get("sentiment", {}).get("current_raw")

    headwinds, tailwinds, signals = [], [], []

    if gdp_raw is not None:
        if gdp_raw < 0:
            headwinds.append(f"GDP {gdp_raw:.1f}% — economy in contraction")
        elif gdp_raw < 1.5:
            headwinds.append(f"GDP {gdp_raw:.1f}% — growth stalling, near-recession pace")
        elif gdp_raw >= 3.0:
            signals.append(f"GDP {gdp_raw:.1f}% — above-trend (watch for inflation re-ignition)")
        else:
            tailwinds.append(f"GDP {gdp_raw:.1f}% — moderate growth, sustainable pace")

    if payrolls_raw is not None:
        if payrolls_raw <= 0:
            headwinds.append("Payrolls negative — job market contracting")
        elif payrolls_raw < 100:
            headwinds.append(f"Payrolls +{payrolls_raw:.0f}K — below-trend, softening labor")
        elif payrolls_raw > 300:
            signals.append(f"Payrolls +{payrolls_raw:.0f}K — strong (may keep wages/inflation elevated)")
        else:
            tailwinds.append(f"Payrolls +{payrolls_raw:.0f}K — healthy job creation")

    if claims_raw is not None:
        if claims_raw >= 300:
            headwinds.append(f"Claims {claims_raw:.0f}K — rapid labor market deterioration")
        elif claims_raw >= 250:
            headwinds.append(f"Claims {claims_raw:.0f}K — rising, watch for acceleration")
        elif claims_raw <= 200:
            tailwinds.append(f"Claims {claims_raw:.0f}K — low, labor market healthy")

    if ism_raw is not None:
        if ism_raw < 45:
            headwinds.append(f"ISM {ism_raw:.1f} — manufacturing in contraction")
        elif ism_raw < 50:
            signals.append(f"ISM {ism_raw:.1f} — below expansion threshold")
        else:
            tailwinds.append(f"ISM {ism_raw:.1f} — manufacturing expanding")

    if sentiment_raw is not None:
        if sentiment_raw <= 65:
            headwinds.append(f"Consumer sentiment {sentiment_raw:.0f} — recession-level pessimism")
        elif sentiment_raw >= 90:
            tailwinds.append(f"Consumer sentiment {sentiment_raw:.0f} — elevated confidence")

    n_hw, n_tw = len(headwinds), len(tailwinds)

    if n_hw >= 3:
        regime       = "Recession Risk"
        regime_level = "extreme"
        regime_read  = "Multiple growth indicators deteriorating. Recession risk elevated. In this environment, rate cuts may come but from fear not relief — initially bearish for BTC before any policy pivot benefit."
    elif n_hw >= 2:
        regime       = "Slowdown Underway"
        regime_level = "notable"
        regime_read  = "Growth slowing meaningfully. Not yet recessionary but momentum is fading. Fed may shift rhetoric. Watch claims for acceleration — that's the earliest weekly signal."
    elif n_tw >= 3:
        regime       = "Goldilocks"
        regime_level = "none"
        regime_read  = "Growth moderating from hot, employment stable, no recession signal. This is the ideal environment — inflation can cool while the city keeps earning income. BTC historically performs well here."
    elif n_tw >= 1 and n_hw == 0:
        regime       = "Solid Growth"
        regime_level = "none"
        regime_read  = "Economy in decent shape. Growth healthy enough that Fed rate cut may be delayed, but no recession risk. Mixed macro backdrop for BTC — not a headwind, but no immediate catalyst."
    else:
        regime       = "Mixed Signals"
        regime_level = "none"
        regime_read  = "Growth data sending mixed messages. Monitor claims (weekly), ISM, and retail sales as real-time indicators. Monthly payrolls will be the next key data point."

    return {
        "regime":       regime,
        "regime_level": regime_level,
        "regime_read":  regime_read,
        "headwinds":    headwinds,
        "tailwinds":    tailwinds,
        "signals":      signals,
        "goldilocks_check": {
            "growth_slowing":  gdp_raw is not None and 1.5 <= gdp_raw <= 2.5,
            "employment_stable": claims_raw is not None and claims_raw <= 240,
            "no_recession":    gdp_raw is not None and gdp_raw >= 0,
        },
    }

# ── Main builder ──────────────────────────────────────────────────────────────

def _build_metrics() -> dict:
    global _cache
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    # Fetch all FRED series — use n_obs tailored to frequency
    N_MONTHLY    = 36   # 3 years of monthly
    N_DAILY      = 252  # 1 year of daily
    N_WEEKLY     = 104  # 2 years of weekly
    N_QUARTERLY  = 20   # 5 years of quarterly

    obs = {}
    freq_map = {
        "cpi":       N_MONTHLY, "core_cpi":   N_MONTHLY, "pce":       N_MONTHLY,
        "core_pce":  N_MONTHLY, "ppi":        N_MONTHLY, "wages":     N_MONTHLY,
        "be_5y":     N_DAILY,   "be_10y":     N_DAILY,
        "rent":      N_MONTHLY, "oer":        N_MONTHLY,
        "oil":       N_DAILY,   "gasoline":   N_WEEKLY,
        "payrolls":  N_MONTHLY, "unrate":     N_MONTHLY, "claims":    N_WEEKLY,
        "cont_claims": N_WEEKLY,"jolts":      N_MONTHLY, "gdp":       N_QUARTERLY,
        "retail":    N_MONTHLY, "sentiment":  N_MONTHLY, "inf_exp":   N_MONTHLY,
        "ism":       N_MONTHLY,
    }

    for key, (series_id, freq, unit) in FRED_SERIES.items():
        obs[key] = _fred(series_id, n_obs=freq_map[key])

    # ── Inflation cards ──
    inflation = {
        "cpi":      _inflation_card("cpi",      obs["cpi"],      target=None),
        "core_cpi": _inflation_card("core_cpi", obs["core_cpi"], target=None),
        "pce":      _inflation_card("pce",      obs["pce"],      target=None),
        "core_pce": _inflation_card("core_pce", obs["core_pce"], target=2.0),
        "ppi":      _inflation_card("ppi",      obs["ppi"],      target=None),
        "wages":    _inflation_card("wages",    obs["wages"],    target=None),
        "be_5y":    _inflation_card("be_5y",    obs["be_5y"],    is_rate=True),
        "be_10y":   _inflation_card("be_10y",   obs["be_10y"],   is_rate=True),
        "rent":     _inflation_card("rent",     obs["rent"],     target=None),
        "oer":      _inflation_card("oer",      obs["oer"],      target=None),
    }

    energy = {
        "oil":      _commodity_card("oil",      obs["oil"],      unit="$"),
        "gasoline": _commodity_card("gasoline", obs["gasoline"], unit="$"),
    }

    # ── Growth / employment cards ──
    growth = {
        "payrolls":   _employment_card("payrolls",   obs["payrolls"],   unit="K",  precision=0),
        "unrate":     _employment_card("unrate",     obs["unrate"],     unit="%",  precision=1, invert_alert=True),
        "claims":     _employment_card("claims",     obs["claims"],     unit="K",  precision=0, invert_alert=True),
        "cont_claims":_employment_card("cont_claims",obs["cont_claims"],unit="K",  precision=0, invert_alert=True),
        "jolts":      _employment_card("jolts",      obs["jolts"],      unit="K",  precision=0),
        "gdp":        _employment_card("gdp",        obs["gdp"],        unit="%",  precision=1),
        "retail":     _employment_card("retail",     obs["retail"],     unit="M",  precision=0),
        "sentiment":  _employment_card("sentiment",  obs["sentiment"],  unit="",   precision=1),
        "inf_exp":    _employment_card("inf_exp",    obs["inf_exp"],    unit="%",  precision=1),
        "ism":        _employment_card("ism",        obs["ism"],        unit="",   precision=1),
    }

    # ── Assessments ──
    # Pass YoY values for wages into pipe assessment
    if obs.get("wages"):
        wages_yoy = _yoy(obs["wages"])
        if wages_yoy and inflation.get("wages"):
            inflation["wages"]["current_raw"] = wages_yoy

    pipe  = _pipe_assessment(inflation)
    city  = _city_assessment(growth)

    result = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "inflation":  inflation,
        "energy":     energy,
        "growth":     growth,
        "pipe":       pipe,
        "city":       city,
    }

    _cache["data"] = result
    _cache["ts"]   = now
    return result

# ── Routes ────────────────────────────────────────────────────────────────────

@growth_router.get("/metrics")
def get_growth_metrics():
    """
    Returns inflation metrics (CPI, PCE, PPI, wages, breakevens, rent, energy)
    + growth metrics (payrolls, unemployment, claims, GDP, ISM, retail, sentiment)
    + pipe temperature assessment + city income assessment.
    """
    return _build_metrics()


@growth_router.get("/cache/flush")
def flush_cache():
    global _cache
    _cache = {"data": None, "ts": 0.0}
    return {"flushed": True}

# ── Registration ──────────────────────────────────────────────────────────────
#
#   from growth_inflation_routes import growth_router
#   app.include_router(growth_router)
#
# Endpoints:
#   GET /growth/metrics
#   GET /growth/cache/flush
