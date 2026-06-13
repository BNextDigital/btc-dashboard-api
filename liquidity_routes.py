"""
liquidity_routes.py — Dollar Liquidity Dashboard backend

Fetches and formats US dollar liquidity data for the /liquidity/* endpoints.

FRED series used (all free, weekly or daily):
  WRESBAL   — Fed Reserve Balances (weekly, Wed)
  WTREGEN   — Treasury General Account (weekly, Wed)
  RRPONTSYD — Overnight Reverse Repo (daily)
  SOFR      — Secured Overnight Financing Rate (daily)
  FEDFUNDS  — Effective Federal Funds Rate (monthly, use as context)
  M2SL      — M2 Money Supply (weekly)

Net Liquidity = Reserves + RRP - TGA  (the "water actually flowing" composite)

Setup:
  1. FRED API key already configured as FRED_API_KEY env var (used by macro_routes.py)
  2. Copy this file to btc-dashboard-api/liquidity_routes.py
  3. In main.py add:
       from liquidity_routes import liquidity_router
       app.include_router(liquidity_router)
  4. Add nav link to /liquidity in all three page.tsx files

Endpoints:
  GET /liquidity/metrics   — all six cards + net liquidity composite
  GET /liquidity/history   — last N days from SQLite (for sparkline history)
  GET /liquidity/cache/flush
"""

import os
import time
import sqlite3
import requests
from datetime import datetime, date, timedelta, timezone
from fastapi import APIRouter

# ── Router ────────────────────────────────────────────────────────────────────
liquidity_router = APIRouter(prefix="/liquidity")

# ── Config ────────────────────────────────────────────────────────────────────
FRED_API_KEY    = os.getenv("FRED_API_KEY", "")
DATA_DIR        = os.getenv("DATA_DIR", "./data")
LIQUIDITY_DB    = os.path.join(DATA_DIR, "liquidity_history.db")
CACHE_TTL       = 3600   # 1 hour — FRED data is weekly/daily, no need to hit it more
FRED_BASE       = "https://api.stlouisfed.org/fred/series/observations"

# FRED series IDs
SERIES = {
    "reserves": "WRESBAL",     # Reserve Balances — weekly, MILLIONS USD
    "tga":      "WTREGEN",     # Treasury General Account — weekly, MILLIONS USD
    "rrp":      "RRPONTSYD",   # Overnight RRP — daily, billions USD
    "sofr":     "SOFR",        # SOFR rate — daily, percent
    "effr":     "FEDFUNDS",    # Effective Fed Funds Rate — monthly, percent
    "m2":       "M2SL",        # M2 Money Supply — weekly, billions USD
}

# Unit scale factors — raw FRED values need conversion for display
# WRESBAL and WTREGEN arrive in millions of USD; everything else in billions
MILLIONS_SERIES = {"reserves", "tga"}   # divide by 1000 to get billions

# City metaphor subtitles
CITY_LABELS = {
    "reserves": "Main Reservoir",
    "tga":      "Treasury's Big Bucket",
    "rrp":      "Reserve Water Tank",
    "sofr":     "Water Pressure Gauge",
    "effr":     "Core Short-End Cost",
    "m2":       "Total City Water Supply",
    "net_liquidity": "Water Actually Flowing",
}

# ── Cache ─────────────────────────────────────────────────────────────────────
_cache: dict = {"data": None, "ts": 0.0}

# ── SQLite ────────────────────────────────────────────────────────────────────

def _db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(LIQUIDITY_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS liquidity_snapshots (
            date            TEXT PRIMARY KEY,
            reserves        REAL,
            tga             REAL,
            rrp             REAL,
            sofr            REAL,
            effr            REAL,
            m2              REAL,
            net_liquidity   REAL,
            stored_at       TEXT
        )
    """)
    conn.commit()
    return conn


def _store_snapshot(snap: dict):
    conn = _db()
    today = date.today().isoformat()
    conn.execute("""
        INSERT INTO liquidity_snapshots
            (date, reserves, tga, rrp, sofr, effr, m2, net_liquidity, stored_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(date) DO UPDATE SET
            reserves=excluded.reserves, tga=excluded.tga,
            rrp=excluded.rrp, sofr=excluded.sofr,
            effr=excluded.effr, m2=excluded.m2,
            net_liquidity=excluded.net_liquidity,
            stored_at=excluded.stored_at
    """, (
        today,
        snap.get("reserves"), snap.get("tga"), snap.get("rrp"),
        snap.get("sofr"), snap.get("effr"), snap.get("m2"),
        snap.get("net_liquidity"),
        datetime.utcnow().isoformat(),
    ))
    conn.commit()
    conn.close()


def _fetch_history_rows(n_days: int = 90) -> list[dict]:
    try:
        conn = _db()
        rows = conn.execute("""
            SELECT date, reserves, tga, rrp, sofr, effr, m2, net_liquidity
            FROM liquidity_snapshots
            ORDER BY date DESC
            LIMIT ?
        """, (n_days,)).fetchall()
        conn.close()
        cols = ["date", "reserves", "tga", "rrp", "sofr", "effr", "m2", "net_liquidity"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception:
        return []

# ── FRED fetcher ──────────────────────────────────────────────────────────────

def _fred_series(series_id: str, n_obs: int = 120) -> list[tuple[str, float]]:
    """
    Returns [(date_str, value), ...] oldest-first, dropping missing values.
    n_obs: number of observations to request (weekly series → ~2 years at 120)
    """
    if not FRED_API_KEY:
        print(f"[liquidity] No FRED_API_KEY — {series_id} will be None")
        return []
    try:
        r = requests.get(
            FRED_BASE,
            params={
                "series_id":       series_id,
                "api_key":         FRED_API_KEY,
                "file_type":       "json",
                "sort_order":      "desc",
                "limit":           n_obs,
                "observation_start": (date.today() - timedelta(days=n_obs * 10)).isoformat(),
            },
            timeout=12,
        )
        r.raise_for_status()
        obs = r.json().get("observations", [])
        result = []
        for o in reversed(obs):   # oldest-first
            try:
                val = float(o["value"])
                result.append((o["date"], val))
            except (ValueError, KeyError):
                pass   # "." = missing
        return result
    except Exception as e:
        print(f"[liquidity] FRED fetch error {series_id}: {e}")
        return []

# ── Formatters ────────────────────────────────────────────────────────────────

def _pct_rank(series: list[float], current: float) -> int:
    if not series:
        return 50
    return round(sum(1 for v in series if v < current) / len(series) * 100)


def _fmt_billions(v: float | None) -> str:
    """Format a value that is already in billions USD."""
    if v is None:
        return "—"
    if abs(v) >= 1000:
        return f"${v/1000:.2f}T"
    if abs(v) < 1:
        return f"${v:.2f}B"
    return f"${v:.1f}B"


def _fmt_millions(v: float | None) -> str:
    """Format a value that arrives in millions USD (WRESBAL, WTREGEN)."""
    if v is None:
        return "—"
    b = v / 1000  # millions → billions
    if abs(b) >= 1000:
        return f"${b/1000:.2f}T"
    return f"${b:.0f}B"


def _fmt_rate(v: float | None, suffix: str = "%") -> str:
    if v is None:
        return "—"
    return f"{v:.2f}{suffix}"


def _spark(series: list[float], n: int = 12) -> list[float]:
    """Last n values, for sparkline (chronological)."""
    return series[-n:] if len(series) >= n else series


def _delta_str_millions(current: float, prev: float) -> str:
    """Delta string for WRESBAL/WTREGEN — raw values in millions, display in billions."""
    delta_b = (current - prev) / 1000  # millions → billions
    sign = "+" if delta_b >= 0 else ""
    if abs(delta_b) >= 1000:
        return f"{sign}${delta_b/1000:.2f}T"
    return f"{sign}${delta_b:.0f}B"


def _delta_str_billions(current: float, prev: float) -> str:
    """Delta string for series already in billions (RRP, M2)."""
    delta = current - prev
    sign = "+" if delta >= 0 else ""
    if abs(delta) >= 1000:
        return f"{sign}${delta/1000:.2f}T"
    if abs(delta) < 1:
        return f"{sign}${delta:.2f}B"
    return f"{sign}${delta:.1f}B"


def _build_reserves_card(obs: list[tuple[str, float]]) -> dict:
    vals = [v for _, v in obs]
    if not vals:
        return {"error": "FRED unavailable", "city_label": CITY_LABELS["reserves"]}
    current = vals[-1]
    d4w  = vals[-5]  if len(vals) >= 5  else vals[0]   # ~4 weeks
    d13w = vals[-14] if len(vals) >= 14 else vals[0]   # ~13 weeks

    pctile = _pct_rank(vals, current)

    # Raw values in MILLIONS — thresholds scaled accordingly
    # $3.5T = 3,500,000M  |  $3.0T = 3,000,000M  |  $2.5T = 2,500,000M
    if current >= 3_500_000:
        alert_level, alert = "none", "Ample — reservoir well stocked"
    elif current >= 3_000_000:
        alert_level, alert = "none", "Adequate — above critical floor"
    elif current >= 2_500_000:
        alert_level, alert = "notable", "Tightening — approaching stress zone"
    else:
        alert_level, alert = "extreme", "Scarce — below $2.5T stress threshold"

    trend_4w_b = (current - d4w) / 1000  # convert delta to billions for display
    if trend_4w_b > 100:
        pattern = f"Rising +${trend_4w_b:.0f}B (4w) — reservoir refilling"
    elif trend_4w_b < -100:
        pattern = f"Draining −${abs(trend_4w_b):.0f}B (4w) — watch for pressure"
    else:
        pattern = "Stable (4w) — no significant drainage"

    return {
        "name":        "Fed Reserve Balances",
        "city_label":  CITY_LABELS["reserves"],
        "current":     _fmt_millions(current),
        "current_raw": current,
        "current_raw_b": current / 1000,   # billions — used by net liquidity
        "d4w":         _delta_str_millions(current, d4w),
        "d13w":        _delta_str_millions(current, d13w),
        "percentile":  pctile,
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     pattern,
        "spark":       [v / 1000 for v in _spark(vals)],  # display in billions
        "source":      f"FRED: {SERIES['reserves']}",
    }


def _build_tga_card(obs: list[tuple[str, float]]) -> dict:
    vals = [v for _, v in obs]
    if not vals:
        return {"error": "FRED unavailable", "city_label": CITY_LABELS["tga"]}
    current = vals[-1]
    d4w  = vals[-5]  if len(vals) >= 5  else vals[0]
    d13w = vals[-14] if len(vals) >= 14 else vals[0]

    pctile = _pct_rank(vals, current)

    # TGA rising = Treasury hoarding = drains reserves = bearish liquidity
    # TGA falling = Treasury spending = injects reserves = bullish liquidity
    # Raw values in MILLIONS — $800B = 800,000M  |  $500B = 500,000M  |  $100B = 100,000M
    if current >= 800_000:
        alert_level, alert = "notable", "High balance — Treasury withholding liquidity"
    elif current >= 500_000:
        alert_level, alert = "none", "Moderate — normal operating range"
    elif current <= 100_000:
        alert_level, alert = "extreme", "Near-empty — debt ceiling or spending surge"
    else:
        alert_level, alert = "none", "Low — Treasury injecting liquidity"

    trend_4w_b = (current - d4w) / 1000  # delta in billions
    if trend_4w_b > 100:
        pattern = f"Rising +${trend_4w_b:.0f}B (4w) — Treasury filling bucket, draining reserves"
    elif trend_4w_b < -100:
        pattern = f"Falling −${abs(trend_4w_b):.0f}B (4w) — Treasury spending, injecting liquidity"
    else:
        pattern = "Stable (4w)"

    return {
        "name":        "Treasury General Account",
        "city_label":  CITY_LABELS["tga"],
        "current":     _fmt_millions(current),
        "current_raw": current,
        "current_raw_b": current / 1000,   # billions — used by net liquidity
        "d4w":         _delta_str_millions(current, d4w),
        "d13w":        _delta_str_millions(current, d13w),
        "percentile":  pctile,
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     pattern,
        "spark":       [v / 1000 for v in _spark(vals)],  # display in billions
        "source":      f"FRED: {SERIES['tga']}",
        "note":        "Rising TGA = drains reserves. Falling TGA = injects liquidity.",
    }


def _build_rrp_card(obs: list[tuple[str, float]]) -> dict:
    vals = [v for _, v in obs]
    if not vals:
        return {"error": "FRED unavailable", "city_label": CITY_LABELS["rrp"]}
    current = vals[-1]
    d5  = vals[-6]   if len(vals) >= 6  else vals[0]
    d20 = vals[-21]  if len(vals) >= 21 else vals[0]

    pctile = _pct_rank(vals, current)

    # RRPONTSYD is already in billions USD — thresholds are correct as-is
    # RRP high = liquidity parked at Fed = not in system = tighter
    # RRP near zero = buffer depleted, system more exposed
    if current >= 500:
        alert_level, alert = "notable", "High — large liquidity buffer parked at Fed"
    elif current >= 100:
        alert_level, alert = "none", "Moderate — buffer present"
    elif current <= 25:
        alert_level, alert = "extreme", "Near zero — buffer depleted, system exposed"
    else:
        alert_level, alert = "none", "Low but present"

    trend_5d = current - d5
    if trend_5d > 50:
        pattern = f"Rising +${trend_5d:.1f}B (5d) — liquidity moving into RRP (tightening)"
    elif trend_5d < -50:
        pattern = f"Falling −${abs(trend_5d):.1f}B (5d) — liquidity leaving RRP (easing)"
    else:
        pattern = "Stable (5d)"

    return {
        "name":        "Overnight Reverse Repo (RRP)",
        "city_label":  CITY_LABELS["rrp"],
        "current":     _fmt_billions(current),
        "current_raw": current,
        "current_raw_b": current,   # already billions
        "d5d":         _delta_str_billions(current, d5),
        "d20d":        _delta_str_billions(current, d20),
        "percentile":  pctile,
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     pattern,
        "spark":       _spark(vals),
        "source":      f"FRED: {SERIES['rrp']}",
        "note":        "High RRP = cash parked at Fed. Draining RRP = moving into system.",
    }


def _build_sofr_card(obs: list[tuple[str, float]]) -> dict:
    vals = [v for _, v in obs]
    if not vals:
        return {"error": "FRED unavailable", "city_label": CITY_LABELS["sofr"]}
    current = vals[-1]
    d5  = vals[-6]  if len(vals) >= 6  else vals[0]
    d20 = vals[-21] if len(vals) >= 21 else vals[0]

    pctile = _pct_rank(vals, current)

    # SOFR vs IORB — IORB moves with Fed funds target; update if Fed hikes/cuts again
    # Current IORB after 2025 cuts: ~4.40% (Fed funds target 4.25–4.50%)
    iorb_proxy = 4.40

    if current > iorb_proxy + 0.10:
        alert_level, alert = "extreme", f"Above IORB proxy — repo market stressed"
    elif current > iorb_proxy:
        alert_level, alert = "notable", "Slightly above IORB — mild pressure"
    elif current < iorb_proxy - 0.15:
        alert_level, alert = "notable", "Below IORB — abundant liquidity"
    else:
        alert_level, alert = "none", "Within normal band"

    trend = current - d5
    bps = round(trend * 100)
    pattern = f"5d: {'+' if bps >= 0 else ''}{bps}bp"

    return {
        "name":       "SOFR",
        "city_label": CITY_LABELS["sofr"],
        "current":    _fmt_rate(current),
        "current_raw": current,
        "d5d":        f"{'+' if trend >= 0 else ''}{round(trend*100)}bp",
        "d20d":       f"{'+' if (current-d20) >= 0 else ''}{round((current-d20)*100)}bp",
        "percentile": pctile,
        "alert":      alert,
        "alert_level": alert_level,
        "pattern":    pattern,
        "spark":      _spark(vals),
        "source":     f"FRED: {SERIES['sofr']}",
        "note":       "SOFR above IORB signals repo market stress — key water pressure gauge.",
    }


def _build_effr_card(obs: list[tuple[str, float]]) -> dict:
    vals = [v for _, v in obs]
    if not vals:
        return {"error": "FRED unavailable", "city_label": CITY_LABELS["effr"]}
    current = vals[-1]
    d1m = vals[-2]  if len(vals) >= 2  else vals[0]
    d3m = vals[-4]  if len(vals) >= 4  else vals[0]

    pctile = _pct_rank(vals, current)

    if current >= 5.0:
        alert_level, alert = "notable", "Restrictive — high cost of short-term capital"
    elif current >= 4.0:
        alert_level, alert = "none", "Moderately tight"
    elif current <= 1.0:
        alert_level, alert = "none", "Accommodative — cheap short-term capital"
    else:
        alert_level, alert = "none", "Neutral range"

    trend = current - d1m
    if abs(trend) >= 0.25:
        pattern = f"Rate {'cut' if trend < 0 else 'hike'} detected: {'+' if trend >= 0 else ''}{trend:.2f}%"
    elif abs(trend) >= 0.01:
        pattern = f"Drifting {'+' if trend >= 0 else ''}{round(trend*100)}bp (1m)"
    else:
        pattern = "Unchanged (1m)"

    return {
        "name":       "Effective Fed Funds Rate",
        "city_label": CITY_LABELS["effr"],
        "current":    _fmt_rate(current),
        "current_raw": current,
        "d1m":        f"{'+' if trend >= 0 else ''}{trend:.2f}%",
        "d3m":        f"{'+' if (current-d3m) >= 0 else ''}{(current-d3m):.2f}%",
        "percentile": pctile,
        "alert":      alert,
        "alert_level": alert_level,
        "pattern":    pattern,
        "spark":      _spark(vals),
        "source":     f"FRED: {SERIES['effr']}",
        "note":       "Monthly rate. Defines the cost floor for short-term dollar borrowing.",
    }


def _build_m2_card(obs: list[tuple[str, float]]) -> dict:
    vals = [v for _, v in obs]
    if not vals:
        return {"error": "FRED unavailable", "city_label": CITY_LABELS["m2"]}
    current = vals[-1]
    d13w = vals[-14] if len(vals) >= 14 else vals[0]   # ~13 weeks
    d52w = vals[-53] if len(vals) >= 53 else vals[0]   # ~52 weeks

    pctile = _pct_rank(vals, current)

    yoy_pct = (current - d52w) / d52w * 100 if d52w else 0

    # Calibrated to historical M2 growth norms (~4-5% is healthy/normal post-pandemic)
    if yoy_pct >= 10:
        alert_level, alert = "extreme", f"M2 expanding fast +{yoy_pct:.1f}% YoY — well above trend"
    elif yoy_pct >= 6:
        alert_level, alert = "notable", f"M2 growing +{yoy_pct:.1f}% YoY — above-trend expansion"
    elif yoy_pct >= 2:
        alert_level, alert = "none", f"M2 +{yoy_pct:.1f}% YoY — normal growth"
    elif yoy_pct <= -2:
        alert_level, alert = "extreme", f"M2 contracting {yoy_pct:.1f}% YoY — rare, historically bearish"
    elif yoy_pct <= 0:
        alert_level, alert = "notable", f"M2 flat/declining {yoy_pct:.1f}% YoY — tightening total supply"
    else:
        alert_level, alert = "none", f"M2 +{yoy_pct:.1f}% YoY — below-trend but positive"

    qoq_delta = current - d13w
    if qoq_delta > 200:
        pattern = f"QoQ +${qoq_delta:.0f}B — money supply accelerating"
    elif qoq_delta < -100:
        pattern = f"QoQ −${abs(qoq_delta):.0f}B — supply contracting"
    else:
        pattern = f"QoQ {'+' if qoq_delta >= 0 else ''}${qoq_delta:.0f}B — stable"

    return {
        "name":        "M2 Money Supply",
        "city_label":  CITY_LABELS["m2"],
        "current":     _fmt_billions(current),
        "current_raw": current,
        "yoy":         f"{'+' if yoy_pct >= 0 else ''}{yoy_pct:.1f}%",
        "qoq":         _delta_str_billions(current, d13w),
        "percentile":  pctile,
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     pattern,
        "spark":       _spark(vals),
        "source":      f"FRED: {SERIES['m2']}",
        "note":        "Total dollar liquidity in the broader system. Rising M2 historically leads BTC by 3–6m.",
    }


def _build_net_liquidity(reserves: dict, tga: dict, rrp: dict) -> dict:
    """
    Net Liquidity = Fed Reserves + RRP - TGA
    All components normalized to BILLIONS before summing.
    Reserves and TGA arrive from FRED in millions → divide by 1000.
    RRP already in billions.
    """
    # Use pre-converted billions fields where available, fall back to raw conversion
    r_b   = reserves.get("current_raw_b")   # billions
    t_b   = tga.get("current_raw_b")        # billions
    rrp_b = rrp.get("current_raw_b")        # billions (already correct)

    if any(v is None for v in [r_b, t_b, rrp_b]):
        return {
            "name":       "Net Liquidity",
            "city_label": CITY_LABELS["net_liquidity"],
            "current":    "—",
            "error":      "One or more components unavailable",
        }

    net_b = r_b + rrp_b - t_b  # all in billions

    # Thresholds in billions ($2.25T = 2250B, $2.5T = 2500B, etc.)
    # Current environment: reserves ~$3.08T, TGA ~$828B, RRP ~$0B → net ~$2.25T
    if net_b >= 3_500:
        alert_level, alert = "none", "Abundant — city fountains fully pressurized"
    elif net_b >= 2_750:
        alert_level, alert = "none", "Comfortable — above-average liquidity"
    elif net_b >= 2_000:
        alert_level, alert = "notable", "Moderate — mid-range, watch for tightening"
    elif net_b >= 1_500:
        alert_level, alert = "notable", "Tightening — approaching historical stress levels"
    else:
        alert_level, alert = "extreme", "Scarce — system under pressure"

    return {
        "name":        "Net Liquidity",
        "city_label":  CITY_LABELS["net_liquidity"],
        "current":     _fmt_billions(net_b),
        "current_raw": net_b,
        "formula":     "Reserves + RRP − TGA",
        "components": {
            "reserves": _fmt_billions(r_b),
            "rrp":      _fmt_billions(rrp_b),
            "tga":      _fmt_billions(t_b),
        },
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     f"Reserves ${r_b:.0f}B + RRP ${rrp_b:.1f}B − TGA ${t_b:.0f}B",
        "note":        "Rising net liquidity historically leads BTC price by 4–8 weeks.",
    }

# ── Main builder ──────────────────────────────────────────────────────────────

def _build_liquidity_metrics() -> dict:
    global _cache
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    # Fetch all series in parallel-ish (sequential is fine — FRED is fast)
    obs = {key: _fred_series(sid, n_obs=120) for key, sid in SERIES.items()}

    reserves = _build_reserves_card(obs["reserves"])
    tga      = _build_tga_card(obs["tga"])
    rrp      = _build_rrp_card(obs["rrp"])
    sofr     = _build_sofr_card(obs["sofr"])
    effr     = _build_effr_card(obs["effr"])
    m2       = _build_m2_card(obs["m2"])
    net_liq  = _build_net_liquidity(reserves, tga, rrp)

    # City assessment — quick tailwind/headwind read
    signals = []
    if reserves.get("alert_level") == "extreme" and "Scarce" in reserves.get("alert", ""):
        signals.append(("headwind", "Reserves scarce — main reservoir low"))
    elif reserves.get("alert_level") == "none":
        signals.append(("tailwind", "Reserves ample — reservoir well stocked"))

    if tga.get("alert_level") == "notable" and "High" in tga.get("alert", ""):
        signals.append(("headwind", "TGA elevated — Treasury hoarding, draining reserves"))
    elif tga.get("current_raw_b") is not None and tga["current_raw_b"] < 300:
        signals.append(("tailwind", "TGA low — Treasury injecting liquidity"))

    if rrp.get("alert_level") == "extreme":
        signals.append(("headwind", "RRP near zero — buffer depleted"))

    sofr_alert = sofr.get("alert_level", "none")
    if sofr_alert == "extreme":
        signals.append(("headwind", "SOFR above IORB — repo market stressed"))
    elif sofr_alert == "none" and sofr.get("alert", "").startswith("Below"):
        signals.append(("tailwind", "SOFR below IORB — abundant short-term liquidity"))

    m2_alert = m2.get("alert_level", "none")
    if m2_alert == "extreme" and "expanding" in m2.get("alert", ""):
        signals.append(("tailwind", "M2 rapidly expanding — total city supply growing"))
    elif m2_alert in ("extreme", "notable") and "contracting" in m2.get("alert", ""):
        signals.append(("headwind", "M2 contracting — total supply shrinking"))

    tailwinds = [s[1] for s in signals if s[0] == "tailwind"]
    headwinds = [s[1] for s in signals if s[0] == "headwind"]

    if len(tailwinds) > len(headwinds):
        city_read = "Reservoir filling · Water pressure stable · Conditions supportive"
        city_read_level = "bullish"
    elif len(headwinds) > len(tailwinds):
        city_read = "Reservoir draining · Pressure rising · Watch for tightening"
        city_read_level = "bearish"
    else:
        city_read = "Mixed signals · Monitor TGA and RRP for direction"
        city_read_level = "neutral"

    result = {
        "updated_at":     datetime.utcnow().isoformat() + "Z",
        "reserves":       reserves,
        "tga":            tga,
        "rrp":            rrp,
        "sofr":           sofr,
        "effr":           effr,
        "m2":             m2,
        "net_liquidity":  net_liq,
        "city_read":      city_read,
        "city_read_level": city_read_level,
        "tailwinds":      tailwinds,
        "headwinds":      headwinds,
    }

    # Persist snapshot
    _store_snapshot({
        "reserves":      reserves.get("current_raw"),
        "tga":           tga.get("current_raw"),
        "rrp":           rrp.get("current_raw"),
        "sofr":          sofr.get("current_raw"),
        "effr":          effr.get("current_raw"),
        "m2":            m2.get("current_raw"),
        "net_liquidity": net_liq.get("current_raw"),
    })

    _cache["data"] = result
    _cache["ts"]   = now
    return result

# ── Routes ────────────────────────────────────────────────────────────────────

@liquidity_router.get("/metrics")
def get_liquidity_metrics():
    """
    Returns all dollar liquidity indicators + net liquidity composite + city assessment.
    """
    return _build_liquidity_metrics()


@liquidity_router.get("/history")
def get_liquidity_history(days: int = 90):
    """Returns last N days of stored liquidity snapshots from SQLite."""
    rows = _fetch_history_rows(n_days=days)
    return {"rows": rows, "count": len(rows)}


@liquidity_router.get("/cache/flush")
def flush_liquidity_cache():
    global _cache
    _cache = {"data": None, "ts": 0.0}
    return {"flushed": True}

# ── Registration (add to main.py) ─────────────────────────────────────────────
#
#   from liquidity_routes import liquidity_router
#   app.include_router(liquidity_router)
#
# Endpoints:
#   GET /liquidity/metrics
#   GET /liquidity/history?days=90
#   GET /liquidity/cache/flush
