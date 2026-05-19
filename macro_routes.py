"""
macro_routes.py — Add these routes to your existing main.py

Fetches macro data for the /macro/* endpoints:
  /macro/metrics — yields, DXY, VIX, HY OAS (all in one call)
  /macro/history — SQLite snapshots for historical view

Data sources:
  - yFinance (already installed): ^TNX, ^FVX, ^IRX, DX-Y.NYB, ^VIX
  - FRED API (free key): ICE BofA HY OAS series BAMLH0A0HYM2
  - Stablecoin / ETF / Funding: reuse existing _build_metrics_cached()

Setup:
  1. Get a free FRED API key at https://fred.stlouisfed.org/docs/api/api_key.html
  2. Add to backend .env:  FRED_API_KEY=your_key_here
  3. Paste this block into main.py (after existing imports)
  4. Run: uvicorn main:app --reload --port 8000

SQLite storage:
  macro_history.db is created automatically in DATA_DIR
"""

import os, time, sqlite3, requests
from datetime import datetime, timedelta, date
from fastapi import APIRouter
import yfinance as yf

# ── Router ─────────────────────────────────────────────────────────────────
# If you prefer to keep everything in main.py, remove the router and
# use @app.get("/macro/...") directly.
macro_router = APIRouter(prefix="/macro")

# ── Config ──────────────────────────────────────────────────────────────────
FRED_API_KEY  = os.getenv("FRED_API_KEY", "")
DATA_DIR      = os.getenv("DATA_DIR", "./data")
MACRO_DB_PATH = os.path.join(DATA_DIR, "macro_history.db")

# yFinance tickers
YF_TICKERS = {
    "yield_1y":  "^IRX",    # 13-week T-bill proxy for 1Y (closest freely available)
#    "yield_2y":  "^TWO",    # 2Y Treasury
#    "yield_3y":  "^THREE",  # 3Y Treasury (may fall back gracefully)
    "yield_5y":  "^FVX",    # 5Y Treasury
    "yield_10y": "^TNX",    # 10Y Treasury
    "dxy":       "DX-Y.NYB",
    "vix":       "^VIX",
}

# FRED series
FRED_HY_OAS_SERIES = "BAMLH0A0HYM2"   # ICE BofA US High Yield OAS (daily, free)

# ── SQLite helpers ───────────────────────────────────────────────────────────

def _macro_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(MACRO_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS macro_snapshots (
            date        TEXT PRIMARY KEY,
            yield_1y    REAL,
            yield_2y    REAL,
            yield_3y    REAL,
            yield_5y    REAL,
            yield_10y   REAL,
            dxy         REAL,
            vix         REAL,
            hy_oas      REAL,
            stored_at   TEXT
        )
    """)
    conn.commit()
    return conn


def _store_macro_snapshot(snap: dict):
    """Upsert today's macro snapshot into SQLite."""
    conn = _macro_db()
    today = date.today().isoformat()
    conn.execute("""
        INSERT INTO macro_snapshots
            (date, yield_1y, yield_2y, yield_3y, yield_5y, yield_10y,
             dxy, vix, hy_oas, stored_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(date) DO UPDATE SET
            yield_1y=excluded.yield_1y, yield_2y=excluded.yield_2y,
            yield_3y=excluded.yield_3y, yield_5y=excluded.yield_5y,
            yield_10y=excluded.yield_10y, dxy=excluded.dxy,
            vix=excluded.vix, hy_oas=excluded.hy_oas,
            stored_at=excluded.stored_at
    """, (
        today,
        snap.get("yield_1y"), snap.get("yield_2y"),
        snap.get("yield_3y"), snap.get("yield_5y"), snap.get("yield_10y"),
        snap.get("dxy"), snap.get("vix"), snap.get("hy_oas"),
        datetime.utcnow().isoformat()
    ))
    conn.commit()
    conn.close()


def _fetch_macro_history_rows(n_days: int = 95) -> list[dict]:
    conn = _macro_db()
    rows = conn.execute("""
        SELECT date, yield_1y, yield_2y, yield_3y, yield_5y, yield_10y,
               dxy, vix, hy_oas
        FROM macro_snapshots
        ORDER BY date DESC
        LIMIT ?
    """, (n_days,)).fetchall()
    conn.close()
    cols = ["date","yield_1y","yield_2y","yield_3y","yield_5y","yield_10y",
            "dxy","vix","hy_oas"]
    return [dict(zip(cols, r)) for r in rows]

# ── Data fetchers ────────────────────────────────────────────────────────────

def _fetch_yfinance_bulk(n_days: int = 95) -> dict:
    """
    Download n_days of daily data for all tickers in one yfinance call.
    Returns {ticker_key: pd.Series(close, index=date)}.
    """
    tickers = list(YF_TICKERS.values())
    period = f"{n_days}d"
    try:
        raw = yf.download(tickers, period=period, auto_adjust=True,
                          progress=False, threads=True)
        close = raw["Close"]
        result = {}
        for key, ticker in YF_TICKERS.items():
            if ticker in close.columns:
                result[key] = close[ticker].dropna()
            else:
                result[key] = None
        return result
    except Exception as e:
        print(f"yfinance bulk download error: {e}")
        return {k: None for k in YF_TICKERS}


def _fetch_fred_hy_oas(n_days: int = 95) -> dict:
    """
    Fetch ICE BofA HY OAS from FRED.
    Returns {"values": [(date_str, float), ...]} oldest-first.
    Falls back gracefully if no API key.
    """
    if not FRED_API_KEY:
        return {"values": [], "error": "No FRED_API_KEY set"}
    end   = date.today()
    start = end - timedelta(days=n_days + 30)  # extra buffer for weekends
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={FRED_HY_OAS_SERIES}"
        f"&observation_start={start.isoformat()}"
        f"&observation_end={end.isoformat()}"
        f"&api_key={FRED_API_KEY}"
        f"&file_type=json"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        obs = resp.json().get("observations", [])
        values = []
        for o in obs:
            try:
                values.append((o["date"], float(o["value"])))
            except (ValueError, KeyError):
                pass  # skip "." missing values
        return {"values": values}
    except Exception as e:
        return {"values": [], "error": str(e)}


# ── Calculators ─────────────────────────────────────────────────────────────

def _pct_rank(series, current_val) -> int | None:
    """Return 0–100 percentile of current_val within series."""
    if series is None or len(series) < 5:
        return None
    arr = sorted(series)
    below = sum(1 for v in arr if v < current_val)
    return round(below / len(arr) * 100)


def _fmt_yield_card(key: str, series, label: str) -> dict:
    """Format a single yield tenor card."""
    if series is None or len(series) == 0:
        return {"label": label, "current": None, "error": "No data"}
    vals = series.tolist()
    current = vals[-1]
    d1_chg  = round(current - vals[-2], 3)  if len(vals) >= 2  else None
    d5_chg  = round(current - vals[-6], 3)  if len(vals) >= 6  else None
    pctile  = _pct_rank(vals, current)

    def _alert(p):
        if p is None: return "–"
        if p >= 90: return "Near 52w high"
        if p <= 10: return "Near 52w low"
        return "Normal"

    return {
        "label":     label,
        "current":   round(current, 3),
        "d1_chg":    d1_chg,
        "d5_chg":    d5_chg,
        "percentile": pctile,
        "alert":     _alert(pctile),
    }


def _fmt_dxy(series) -> dict:
    if series is None or len(series) == 0:
        return {"current": None, "error": "No data"}
    vals = series.tolist()
    current = round(vals[-1], 2)
    d5  = round(current - vals[-6], 2)  if len(vals) >= 6  else None
    d20 = round(current - vals[-21], 2) if len(vals) >= 21 else None
    pctile = _pct_rank(vals, current)

    def _direction(chg):
        if chg is None: return "–"
        return f"{'+' if chg >= 0 else ''}{chg}"

    def _alert(p, d5v):
        if p is None: return "Normal"
        if p <= 15: return "USD weakening"
        if p >= 85: return "USD strengthening"
        return "Normal"

    pattern = "Sustained dollar decline" if (d20 and d20 < -2) else \
              "Dollar stabilizing" if (d20 and abs(d20) <= 1) else \
              "Dollar gaining" if (d20 and d20 > 2) else "–"

    return {
        "current":   current,
        "d5_chg":    d5,
        "d5_pct":    round(d5 / vals[-6] * 100, 1) if (d5 and len(vals) >= 6) else None,
        "d20_chg":   d20,
        "d20_pct":   round(d20 / vals[-21] * 100, 1) if (d20 and len(vals) >= 21) else None,
        "percentile": pctile,
        "alert":     _alert(pctile, d5),
        "pattern":   pattern,
    }


def _fmt_vix(series) -> dict:
    if series is None or len(series) == 0:
        return {"current": None, "error": "No data"}
    vals = series.tolist()
    current = round(vals[-1], 2)
    d5  = round(current - vals[-6], 2)  if len(vals) >= 6  else None
    d20 = round(current - vals[-21], 2) if len(vals) >= 21 else None
    pctile = _pct_rank(vals, current)

    def _alert(p, cur):
        if cur is None: return "Normal"
        if cur >= 30: return "Fear spike"
        if cur >= 20: return "Elevated"
        if cur <= 13: return "Extreme calm"
        return "Cooling" if (d5 and d5 < -2) else "Normal"

    pattern = "VIX compressing — risk appetite returning" if (d5 and d5 < -3) else \
              "VIX spiking — risk-off" if (d5 and d5 > 5) else \
              "Stable low volatility" if current < 16 else "Elevated volatility"

    return {
        "current":   current,
        "d5_chg":    d5,
        "d5_pct":    round(d5 / vals[-6] * 100, 1) if (d5 and len(vals) >= 6) else None,
        "d20_chg":   d20,
        "d20_pct":   round(d20 / vals[-21] * 100, 1) if (d20 and len(vals) >= 21) else None,
        "percentile": pctile,
        "alert":     _alert(pctile, current),
        "pattern":   pattern,
    }


def _fmt_hy_oas(fred_data: dict) -> dict:
    values = fred_data.get("values", [])
    if not values:
        err = fred_data.get("error", "No data")
        return {"current": None, "error": err}
    dates, vals = zip(*values)
    vals = list(vals)
    current = round(vals[-1], 1)
    d5  = round(current - vals[-6], 1)  if len(vals) >= 6  else None
    d20 = round(current - vals[-21], 1) if len(vals) >= 21 else None
    pctile = _pct_rank(vals, current)

    def _alert(p, cur):
        if cur is None: return "Normal"
        if cur >= 600: return "Distress"
        if cur >= 450: return "Stressed"
        if cur >= 350: return "Moderately stressed"
        if cur <= 250: return "Compressed / risk-on"
        return "Normal"

    return {
        "current":   current,
        "d5_chg":    d5,
        "d20_chg":   d20,
        "percentile": pctile,
        "alert":     _alert(pctile, current),
        "pattern":   "Spreads tightening" if (d5 and d5 < -10) else \
                     "Spreads widening" if (d5 and d5 > 10) else "Stable",
    }


def _spread_label(y2, y10) -> str:
    if y2 is None or y10 is None: return "–"
    spread_bp = round((y10 - y2) * 100)
    if spread_bp < -25: return f"Inverted ({spread_bp:+d}bp)"
    if spread_bp < 0:   return f"Slightly inverted ({spread_bp:+d}bp)"
    if spread_bp < 50:  return f"Near flat ({spread_bp:+d}bp)"
    if spread_bp < 100: return f"Normal steepening ({spread_bp:+d}bp)"
    return f"Steep ({spread_bp:+d}bp)"

# ── Cache ────────────────────────────────────────────────────────────────────
_macro_cache: dict = {"data": None, "ts": 0.0}
MACRO_CACHE_TTL = 300  # 5 minutes — yfinance + FRED are slow


def _build_macro_metrics() -> dict:
    global _macro_cache
    now = time.time()
    if _macro_cache["data"] and (now - _macro_cache["ts"]) < MACRO_CACHE_TTL:
        return _macro_cache["data"]

    yf_data  = _fetch_yfinance_bulk(n_days=95)
    fred_data = _fetch_fred_hy_oas(n_days=95)

    yields = {
        "1y":  _fmt_yield_card("yield_1y",  yf_data.get("yield_1y"),  "1Y"),
        "2y":  _fmt_yield_card("yield_2y",  yf_data.get("yield_2y"),  "2Y"),
        "3y":  _fmt_yield_card("yield_3y",  yf_data.get("yield_3y"),  "3Y"),
        "5y":  _fmt_yield_card("yield_5y",  yf_data.get("yield_5y"),  "5Y"),
        "10y": _fmt_yield_card("yield_10y", yf_data.get("yield_10y"), "10Y"),
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
        "dxy":    _fmt_dxy(yf_data.get("dxy")),
        "vix":    _fmt_vix(yf_data.get("vix")),
        "hy_oas": _fmt_hy_oas(fred_data),
    }

    # Persist snapshot for historical view
    _store_macro_snapshot({
        "yield_1y":  yields["1y"].get("current"),
        "yield_2y":  yields["2y"].get("current"),
        "yield_3y":  yields["3y"].get("current"),
        "yield_5y":  yields["5y"].get("current"),
        "yield_10y": yields["10y"].get("current"),
        "dxy":       result["dxy"].get("current"),
        "vix":       result["vix"].get("current"),
        "hy_oas":    result["hy_oas"].get("current"),
    })

    _macro_cache["data"] = result
    _macro_cache["ts"]   = now
    return result

# ── Routes ───────────────────────────────────────────────────────────────────

@macro_router.get("/metrics")
def get_macro_metrics():
    """
    Returns: {
      updated_at, yields {1y,2y,3y,5y,10y}, curve {spread_2y10y_bp, label},
      dxy, vix, hy_oas
    }
    """
    return _build_macro_metrics()


@macro_router.get("/history")
def get_macro_history(days: int = 90):
    """Returns last N days of stored macro snapshots from SQLite."""
    rows = _fetch_macro_history_rows(n_days=days)
    return {"rows": rows, "count": len(rows)}

# ── Registration (add this to the bottom of main.py) ──────────────────────
#
#   from macro_routes import macro_router
#   app.include_router(macro_router)
#
# Then your endpoints are:
#   GET /macro/metrics
#   GET /macro/history?days=90
