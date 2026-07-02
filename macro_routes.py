"""
macro_routes.py — migrated to shared cache layer

CHANGES FROM ORIGINAL:
  - Removed: _fetch_fred_series(), _fetch_all_fred_yields(), _fetch_fred_hy_oas()
  - Removed: _fetch_yfinance_bulk()
  - Removed: import requests, import yfinance as yf  (no longer needed here)
  - Added:   from shared.yf_cache import get_series as _yf
  - Added:   from shared.fred_cache import get_series as _fred, get_series_df as _fred_df
  - _build_macro_metrics() now reads from shared caches instead of fetching

Everything else — formatters, SQLite helpers, routes, cache logic — unchanged.
"""

import os
import time
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date, timezone
from fastapi import APIRouter

from shared.yf_cache   import get_series as _yf
from shared.fred_cache import get_series as _fred, get_series_df as _fred_df

# ── Router ────────────────────────────────────────────────────────────────────
macro_router = APIRouter(prefix="/macro")

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR      = os.getenv("DATA_DIR", "./data")
MACRO_DB_PATH = os.path.join(DATA_DIR, "macro_history.db")

# FRED series IDs for yields and HY OAS — now resolved via shared fred_cache
FRED_YIELD_SERIES = {
    "yield_1y":  "DGS1",
    "yield_2y":  "DGS2",
    "yield_3y":  "DGS3",
    "yield_5y":  "DGS5",
    "yield_10y": "DGS10",
}
FRED_HY_OAS_SERIES = "BAMLH0A0HYM2"

# ── SQLite helpers ────────────────────────────────────────────────────────────
# Unchanged from original

def _macro_db():
    """Open macro_history.db, create/migrate schema, return connection."""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(MACRO_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS macro_snapshots (
            date             TEXT PRIMARY KEY,
            yield_1y         REAL, yield_2y  REAL, yield_3y  REAL,
            yield_5y         REAL, yield_10y REAL,
            dxy              REAL, vix       REAL, hy_oas    REAL,
            nasdaq100        REAL, nasdaq100_sma20 REAL, nasdaq100_sma50 REAL, nasdaq100_sma200 REAL,
            vxn              REAL,
            sp500            REAL, sp500_sma20     REAL, sp500_sma50     REAL, sp500_sma200     REAL,
            brent            REAL, brent_sma20     REAL, brent_sma50     REAL, brent_sma200     REAL,
            gold             REAL, gold_sma20      REAL, gold_sma50      REAL, gold_sma200      REAL,
            silver           REAL, silver_sma20    REAL, silver_sma50    REAL, silver_sma200    REAL,
            platinum         REAL, platinum_sma20  REAL, platinum_sma50  REAL, platinum_sma200  REAL,
            copper           REAL, copper_sma20    REAL, copper_sma50    REAL, copper_sma200    REAL,
            stored_at        TEXT
        )
    """)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(macro_snapshots)").fetchall()}
    new_cols = [
        ("nasdaq100", "REAL"), ("nasdaq100_sma20", "REAL"), ("nasdaq100_sma50", "REAL"), ("nasdaq100_sma200", "REAL"),
        ("vxn", "REAL"),
        ("sp500", "REAL"), ("sp500_sma20", "REAL"), ("sp500_sma50", "REAL"), ("sp500_sma200", "REAL"),
        ("brent", "REAL"), ("brent_sma20", "REAL"), ("brent_sma50", "REAL"), ("brent_sma200", "REAL"),
        ("gold", "REAL"), ("gold_sma20", "REAL"), ("gold_sma50", "REAL"), ("gold_sma200", "REAL"),
        ("silver", "REAL"), ("silver_sma20", "REAL"), ("silver_sma50", "REAL"), ("silver_sma200", "REAL"),
        ("platinum", "REAL"), ("platinum_sma20", "REAL"), ("platinum_sma50", "REAL"), ("platinum_sma200", "REAL"),
        ("copper", "REAL"), ("copper_sma20", "REAL"), ("copper_sma50", "REAL"), ("copper_sma200", "REAL"),
    ]
    for col, typ in new_cols:
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE macro_snapshots ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass
    conn.commit()
    return conn


def _store_macro_snapshot(snap: dict):
    conn = _macro_db()
    today = date.today().isoformat()
    try:
        conn.execute("""
            INSERT INTO macro_snapshots
                (date, yield_1y, yield_2y, yield_3y, yield_5y, yield_10y,
                 dxy, vix, hy_oas,
                 nasdaq100, nasdaq100_sma20, nasdaq100_sma50, nasdaq100_sma200,
                 vxn,
                 sp500, sp500_sma20, sp500_sma50, sp500_sma200,
                 brent, brent_sma20, brent_sma50, brent_sma200,
                 gold, gold_sma20, gold_sma50, gold_sma200,
                 silver, silver_sma20, silver_sma50, silver_sma200,
                 platinum, platinum_sma20, platinum_sma50, platinum_sma200,
                 copper, copper_sma20, copper_sma50, copper_sma200,
                 stored_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date) DO UPDATE SET
                yield_1y=excluded.yield_1y, yield_2y=excluded.yield_2y,
                yield_3y=excluded.yield_3y, yield_5y=excluded.yield_5y,
                yield_10y=excluded.yield_10y,
                dxy=excluded.dxy, vix=excluded.vix, hy_oas=excluded.hy_oas,
                nasdaq100=excluded.nasdaq100, nasdaq100_sma20=excluded.nasdaq100_sma20,
                nasdaq100_sma50=excluded.nasdaq100_sma50, nasdaq100_sma200=excluded.nasdaq100_sma200,
                vxn=excluded.vxn,
                sp500=excluded.sp500, sp500_sma20=excluded.sp500_sma20,
                sp500_sma50=excluded.sp500_sma50, sp500_sma200=excluded.sp500_sma200,
                brent=excluded.brent, brent_sma20=excluded.brent_sma20,
                brent_sma50=excluded.brent_sma50, brent_sma200=excluded.brent_sma200,
                gold=excluded.gold, gold_sma20=excluded.gold_sma20,
                gold_sma50=excluded.gold_sma50, gold_sma200=excluded.gold_sma200,
                silver=excluded.silver, silver_sma20=excluded.silver_sma20,
                silver_sma50=excluded.silver_sma50, silver_sma200=excluded.silver_sma200,
                platinum=excluded.platinum, platinum_sma20=excluded.platinum_sma20,
                platinum_sma50=excluded.platinum_sma50, platinum_sma200=excluded.platinum_sma200,
                copper=excluded.copper, copper_sma20=excluded.copper_sma20,
                copper_sma50=excluded.copper_sma50, copper_sma200=excluded.copper_sma200,
                stored_at=excluded.stored_at
        """, (
            today,
            snap.get("yield_1y"), snap.get("yield_2y"),
            snap.get("yield_3y"), snap.get("yield_5y"), snap.get("yield_10y"),
            snap.get("dxy"), snap.get("vix"), snap.get("hy_oas"),
            snap.get("nasdaq100"), snap.get("nasdaq100_sma20"), snap.get("nasdaq100_sma50"), snap.get("nasdaq100_sma200"),
            snap.get("vxn"),
            snap.get("sp500"), snap.get("sp500_sma20"), snap.get("sp500_sma50"), snap.get("sp500_sma200"),
            snap.get("brent"), snap.get("brent_sma20"), snap.get("brent_sma50"), snap.get("brent_sma200"),
            snap.get("gold"), snap.get("gold_sma20"), snap.get("gold_sma50"), snap.get("gold_sma200"),
            snap.get("silver"), snap.get("silver_sma20"), snap.get("silver_sma50"), snap.get("silver_sma200"),
            snap.get("platinum"), snap.get("platinum_sma20"), snap.get("platinum_sma50"), snap.get("platinum_sma200"),
            snap.get("copper"), snap.get("copper_sma20"), snap.get("copper_sma50"), snap.get("copper_sma200"),
            datetime.utcnow().isoformat()
        ))
        conn.commit()
    except Exception as e:
        print(f"[macro] Error storing snapshot: {e}")
        conn.rollback()
    finally:
        conn.close()


def _fetch_macro_history_rows(n_days: int = 95) -> list[dict]:
    conn = _macro_db()
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(macro_snapshots)")
        available = {row[1] for row in cursor.fetchall()}
        base_cols = ["date", "yield_1y", "yield_2y", "yield_3y", "yield_5y", "yield_10y",
                     "dxy", "vix", "hy_oas"]
        extra_cols = ["nasdaq100", "nasdaq100_sma20", "nasdaq100_sma50", "nasdaq100_sma200",
                      "vxn", "sp500", "sp500_sma20", "sp500_sma50", "sp500_sma200",
                      "brent", "brent_sma20", "brent_sma50", "brent_sma200",
                      "gold", "gold_sma20", "gold_sma50", "gold_sma200",
                      "silver", "silver_sma20", "silver_sma50", "silver_sma200",
                      "platinum", "platinum_sma20", "platinum_sma50", "platinum_sma200",
                      "copper", "copper_sma20", "copper_sma50", "copper_sma200"]
        select_cols = base_cols + [c for c in extra_cols if c in available]
        rows = conn.execute(
            f"SELECT {', '.join(select_cols)} FROM macro_snapshots ORDER BY date DESC LIMIT ?",
            (n_days,)
        ).fetchall()
        return [dict(zip(select_cols, r)) for r in rows]
    except Exception as e:
        print(f"[macro] Error fetching history: {e}")
        return []
    finally:
        conn.close()


# ── Data fetchers — REPLACED WITH SHARED CACHE CALLS ─────────────────────────

def _fetch_all_fred_yields() -> dict:
    """
    Fetch all yield tenors from shared fred_cache.
    Returns {yield_1y: pd.Series, ...} — same shape as the original function.
    """
    result = {}
    for key, series_id in FRED_YIELD_SERIES.items():
        result[key] = _fred_df(series_id)   # pd.Series or None
    return result


def _fetch_fred_hy_oas() -> dict:
    """
    Fetch HY OAS from shared fred_cache.
    Returns {"values": [(date_str, float), ...]} — same shape as original.
    """
    pairs = _fred(FRED_HY_OAS_SERIES)
    if pairs:
        return {"values": pairs}
    return {"values": [], "error": "No data or FRED_API_KEY missing"}


def _fetch_yfinance_data() -> dict:
    """
    Pull all required series from the shared yf_cache.
    Returns {key: pd.Series | None} — same shape as the original _fetch_yfinance_bulk().
    """
    keys = ["dxy", "vix", "nasdaq", "vxn", "spx", "brent",
            "gold", "silver", "platinum", "copper"]
    # Map shared cache keys → local names expected by formatters
    return {
        "dxy":       _yf("dxy"),
        "vix":       _yf("vix"),
        "nasdaq100": _yf("nasdaq"),    # ^IXIC in shared cache
        "vxn":       _yf("vxn"),
        "sp500":     _yf("spx"),       # ^GSPC in shared cache
        "brent":     _yf("brent"),
        "gold":      _yf("gold"),
        "silver":    _yf("silver"),
        "platinum":  _yf("platinum"),
        "copper":    _yf("copper"),
    }


# ── Math helpers ──────────────────────────────────────────────────────────────
# Unchanged from original

def _calculate_sma(series, window: int) -> float | None:
    if series is None or len(series) < window:
        return None
    return float(series.tail(window).mean())


def _sma_pct_diff(current: float | None, sma: float | None) -> float | None:
    if current is None or sma is None or sma == 0:
        return None
    return ((current - sma) / sma) * 100


def _pct_rank(series, current_val) -> int | None:
    if series is None or len(series) < 5:
        return None
    arr = list(series)
    below = sum(1 for v in arr if v < current_val)
    return round(below / len(arr) * 100)


# ── Formatters ────────────────────────────────────────────────────────────────
# Unchanged from original

def _fmt_yield_card(key: str, series, label: str) -> dict:
    if series is None or len(series) == 0:
        return {"label": label, "current": None, "error": "No data — check FRED_API_KEY"}
    vals    = series.tolist()
    current = vals[-1]
    d1_chg  = round(current - vals[-2], 3) if len(vals) >= 2 else None
    d5_chg  = round(current - vals[-6], 3) if len(vals) >= 6 else None
    pctile  = _pct_rank(vals, current)

    def _alert(p):
        if p is None: return "–"
        if p >= 90:   return "Near 52w high"
        if p <= 10:   return "Near 52w low"
        return "Normal"

    return {
        "label":      label,
        "current":    round(current, 3),
        "d1_chg":     d1_chg,
        "d5_chg":     d5_chg,
        "percentile": pctile,
        "alert":      _alert(pctile),
        "source":     "FRED",
    }


def _fmt_dxy(series) -> dict:
    if series is None or len(series) == 0:
        return {"current": None, "error": "No data"}
    vals    = series.tolist()
    current = round(vals[-1], 2)
    d5      = round(current - vals[-6],  2) if len(vals) >= 6  else None
    d20     = round(current - vals[-21], 2) if len(vals) >= 21 else None
    pctile  = _pct_rank(vals, current)

    def _alert(p):
        if p is None: return "Normal"
        if p <= 15:   return "USD weakening"
        if p >= 85:   return "USD strengthening"
        return "Normal"

    pattern = ("Sustained dollar decline" if (d20 and d20 < -2) else
               "Dollar stabilizing"       if (d20 and abs(d20) <= 1) else
               "Dollar gaining"           if (d20 and d20 > 2) else "–")

    return {
        "current":    current,
        "d5_chg":     d5,
        "d20_chg":    d20,
        "percentile": pctile,
        "alert":      _alert(pctile),
        "pattern":    pattern,
    }


def _fmt_vix(series) -> dict:
    if series is None or len(series) == 0:
        return {"current": None, "error": "No data"}
    vals    = series.tolist()
    current = round(vals[-1], 2)
    d5      = round(current - vals[-6],  2) if len(vals) >= 6  else None
    d20     = round(current - vals[-21], 2) if len(vals) >= 21 else None
    pctile  = _pct_rank(vals, current)

    def _alert(cur):
        if cur is None: return "Normal"
        if cur >= 30:   return "Fear spike"
        if cur >= 20:   return "Elevated"
        if cur <= 13:   return "Extreme calm"
        return "Cooling" if (d5 and d5 < -2) else "Normal"

    pattern = ("VIX compressing — risk appetite returning" if (d5 and d5 < -3) else
               "VIX spiking — risk-off"                   if (d5 and d5 > 5)  else
               "Stable low volatility"                     if current < 16     else
               "Elevated volatility")

    return {
        "current":    current,
        "d5_chg":     d5,
        "d20_chg":    d20,
        "percentile": pctile,
        "alert":      _alert(current),
        "pattern":    pattern,
    }


def _fmt_hy_oas(fred_data: dict) -> dict:
    values = fred_data.get("values", [])
    if not values:
        return {"current": None, "error": fred_data.get("error", "No data")}

    _, vals = zip(*values)
    vals = [v * 100 for v in vals]  # FRED returns %, convert to bp

    current = round(vals[-1], 1)
    d5      = round(current - vals[-6],  1) if len(vals) >= 6  else None
    d20     = round(current - vals[-21], 1) if len(vals) >= 21 else None
    pctile  = _pct_rank(vals, current)

    def _alert(cur):
        if cur is None:  return "Normal"
        if cur >= 600:   return "Distress"
        if cur >= 450:   return "Stressed"
        if cur >= 350:   return "Moderately stressed"
        if cur <= 250:   return "Compressed / risk-on"
        return "Normal"

    return {
        "current":    current,
        "d5_chg":     d5,
        "d20_chg":    d20,
        "percentile": pctile,
        "alert":      _alert(current),
        "pattern":    ("Spreads tightening" if (d5 and d5 < -10) else
                       "Spreads widening"   if (d5 and d5 > 10)  else "Stable"),
    }


def _fmt_equity_sma_card(name: str, series) -> dict:
    if series is None or len(series) == 0:
        return {"current": None, "error": "No data"}
    vals    = series.tolist()
    current = round(vals[-1], 2)
    sma20   = _calculate_sma(series, 20)
    sma50   = _calculate_sma(series, 50)
    sma200  = _calculate_sma(series, 200)
    pct_20  = _sma_pct_diff(current, sma20)
    pct_50  = _sma_pct_diff(current, sma50)
    pct_200 = _sma_pct_diff(current, sma200)
    pctile  = _pct_rank(vals, current)

    alert = "–"
    if pct_200 is not None:
        if   pct_200 < -10: alert = "Far below 200d SMA"
        elif pct_200 < -5:  alert = "Below 200d SMA"
        elif pct_200 > 15:  alert = "Well above 200d SMA"
        elif pct_200 > 10:  alert = "Above 200d SMA"

    return {
        "current":         current,
        "sma20":           round(sma20,   2) if sma20   else None,
        "sma50":           round(sma50,   2) if sma50   else None,
        "sma200":          round(sma200,  2) if sma200  else None,
        "pct_from_sma20":  round(pct_20,  2) if pct_20  else None,
        "pct_from_sma50":  round(pct_50,  2) if pct_50  else None,
        "pct_from_sma200": round(pct_200, 2) if pct_200 else None,
        "percentile":      pctile,
        "alert":           alert,
    }


def _fmt_vxn(series) -> dict:
    if series is None or len(series) == 0:
        return {"current": None, "error": "No data"}
    vals    = series.tolist()
    current = round(vals[-1], 2)
    d5      = round(current - vals[-6],  2) if len(vals) >= 6  else None
    d20     = round(current - vals[-21], 2) if len(vals) >= 21 else None
    pctile  = _pct_rank(vals, current)

    def _alert(cur):
        if cur is None: return "Normal"
        if cur >= 30:   return "Tech volatility elevated"
        if cur >= 20:   return "Elevated"
        if cur <= 15:   return "Tech calm"
        return "Normal"

    return {
        "current":    current,
        "d5_chg":     d5,
        "d20_chg":    d20,
        "percentile": pctile,
        "alert":      _alert(current),
        "pattern":    ("Tech volatility rising"  if (d5 and d5 > 3)       else
                       "Tech volatility stable"  if (d5 and abs(d5) <= 1) else
                       "Tech volatility falling"),
    }


def _spread_label(y2, y10) -> str:
    if y2 is None or y10 is None: return "–"
    bp = round((y10 - y2) * 100)
    if bp < -25: return f"Inverted ({bp:+d}bp)"
    if bp < 0:   return f"Slightly inverted ({bp:+d}bp)"
    if bp < 50:  return f"Near flat ({bp:+d}bp)"
    if bp < 100: return f"Normal steepening ({bp:+d}bp)"
    return f"Steep ({bp:+d}bp)"


# ── Cache ─────────────────────────────────────────────────────────────────────
# Unchanged from original — macro refreshes once after 10AM EST

EST = timezone(timedelta(hours=-5))

def _last_10am_est() -> datetime:
    now_est    = datetime.now(EST)
    today_10am = now_est.replace(hour=10, minute=0, second=0, microsecond=0)
    if now_est < today_10am:
        return today_10am - timedelta(days=1)
    return today_10am

def _cache_is_stale(cache: dict) -> bool:
    if not cache["data"]:
        return True
    last_refresh = _last_10am_est()
    cache_time   = datetime.fromtimestamp(cache["ts"], tz=timezone.utc)
    return cache_time < last_refresh

_macro_cache: dict = {"data": None, "ts": 0.0}


# ── Main builder ──────────────────────────────────────────────────────────────

def _build_macro_metrics() -> dict:
    global _macro_cache
    if not _cache_is_stale(_macro_cache):
        return _macro_cache["data"]

    # ── Pull from shared caches — no network calls here ──────────────────
    fred_yields  = _fetch_all_fred_yields()
    fred_hy_data = _fetch_fred_hy_oas()
    yf_data      = _fetch_yfinance_data()

    # ── Build yield cards ─────────────────────────────────────────────────
    yields = {
        "1y":  _fmt_yield_card("yield_1y",  fred_yields.get("yield_1y"),  "1Y"),
        "2y":  _fmt_yield_card("yield_2y",  fred_yields.get("yield_2y"),  "2Y"),
        "3y":  _fmt_yield_card("yield_3y",  fred_yields.get("yield_3y"),  "3Y"),
        "5y":  _fmt_yield_card("yield_5y",  fred_yields.get("yield_5y"),  "5Y"),
        "10y": _fmt_yield_card("yield_10y", fred_yields.get("yield_10y"), "10Y"),
    }
    y2_val  = yields["2y"].get("current")
    y10_val = yields["10y"].get("current")

    result = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "yields": yields,
        "curve": {
            "spread_2y10y_bp": round((y10_val - y2_val) * 100) if (y2_val and y10_val) else None,
            "label": _spread_label(y2_val, y10_val),
        },
        "dxy":       _fmt_dxy(yf_data.get("dxy")),
        "vix":       _fmt_vix(yf_data.get("vix")),
        "hy_oas":    _fmt_hy_oas(fred_hy_data),
        "nasdaq100": _fmt_equity_sma_card("Nasdaq-100",  yf_data.get("nasdaq100")),
        "vxn":       _fmt_vxn(yf_data.get("vxn")),
        "sp500":     _fmt_equity_sma_card("S&P 500",     yf_data.get("sp500")),
        "brent":     _fmt_equity_sma_card("Brent Crude", yf_data.get("brent")),
        "gold":      _fmt_equity_sma_card("Gold",        yf_data.get("gold")),
        "silver":    _fmt_equity_sma_card("Silver",      yf_data.get("silver")),
        "platinum":  _fmt_equity_sma_card("Platinum",    yf_data.get("platinum")),
        "copper":    _fmt_equity_sma_card("Copper",      yf_data.get("copper")),
    }

    # ── Persist daily snapshot to SQLite ─────────────────────────────────
    _store_macro_snapshot({
        "yield_1y":  yields["1y"].get("current"),
        "yield_2y":  yields["2y"].get("current"),
        "yield_3y":  yields["3y"].get("current"),
        "yield_5y":  yields["5y"].get("current"),
        "yield_10y": yields["10y"].get("current"),
        "dxy":       result["dxy"].get("current"),
        "vix":       result["vix"].get("current"),
        "hy_oas":    result["hy_oas"].get("current"),
        "nasdaq100":        result["nasdaq100"].get("current"),
        "nasdaq100_sma20":  result["nasdaq100"].get("sma20"),
        "nasdaq100_sma50":  result["nasdaq100"].get("sma50"),
        "nasdaq100_sma200": result["nasdaq100"].get("sma200"),
        "vxn":              result["vxn"].get("current"),
        "sp500":            result["sp500"].get("current"),
        "sp500_sma20":      result["sp500"].get("sma20"),
        "sp500_sma50":      result["sp500"].get("sma50"),
        "sp500_sma200":     result["sp500"].get("sma200"),
        "brent":            result["brent"].get("current"),
        "brent_sma20":      result["brent"].get("sma20"),
        "brent_sma50":      result["brent"].get("sma50"),
        "brent_sma200":     result["brent"].get("sma200"),
        "gold":             result["gold"].get("current"),
        "gold_sma20":       result["gold"].get("sma20"),
        "gold_sma50":       result["gold"].get("sma50"),
        "gold_sma200":      result["gold"].get("sma200"),
        "silver":           result["silver"].get("current"),
        "silver_sma20":     result["silver"].get("sma20"),
        "silver_sma50":     result["silver"].get("sma50"),
        "silver_sma200":    result["silver"].get("sma200"),
        "platinum":         result["platinum"].get("current"),
        "platinum_sma20":   result["platinum"].get("sma20"),
        "platinum_sma50":   result["platinum"].get("sma50"),
        "platinum_sma200":  result["platinum"].get("sma200"),
        "copper":           result["copper"].get("current"),
        "copper_sma20":     result["copper"].get("sma20"),
        "copper_sma50":     result["copper"].get("sma50"),
        "copper_sma200":    result["copper"].get("sma200"),
    })

    _macro_cache["data"] = result
    _macro_cache["ts"]   = time.time()
    return result


# ── Routes ────────────────────────────────────────────────────────────────────
# Unchanged from original

@macro_router.get("/metrics")
def get_macro_metrics():
    return _build_macro_metrics()


@macro_router.get("/history")
def get_macro_history(days: int = 90):
    rows = _fetch_macro_history_rows(n_days=days)
    return {"rows": rows, "count": len(rows)}


@macro_router.get("/cache/flush")
def flush_macro_cache():
    global _macro_cache
    _macro_cache = {"data": None, "ts": 0.0}
    return {"flushed": True}
