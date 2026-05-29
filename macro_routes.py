"""
macro_routes.py (FIXED) — Add these routes to your existing main.py

Includes automatic schema migration to add new equity/commodity columns.

Setup:
  1. Get a free FRED API key at https://fred.stlouisfed.org/docs/api/api_key.html
  2. Add to backend .env:  FRED_API_KEY=your_key_here
  3. Paste this block into main.py (after existing imports)
  4. Run: uvicorn main:app --reload --port 8000
"""

import os, time, sqlite3, requests
from datetime import datetime, timedelta, date
from fastapi import APIRouter
import yfinance as yf
import pandas as pd

# ── Router ─────────────────────────────────────────────────────────────────
macro_router = APIRouter(prefix="/macro")

# ── Config ──────────────────────────────────────────────────────────────────
FRED_API_KEY  = os.getenv("FRED_API_KEY", "")
DATA_DIR      = os.getenv("DATA_DIR", "./data")
MACRO_DB_PATH = os.path.join(DATA_DIR, "macro_history.db")

# yFinance tickers
YF_TICKERS = {
    "yield_1y":  "^IRX",
    "yield_5y":  "^FVX",
    "yield_10y": "^TNX",
    "dxy":       "DX-Y.NYB",
    "vix":       "^VIX",
    "nasdaq100": "^IXIC",
    "vxn":       "^VXN",
    "sp500":     "^GSPC",
    "brent":     "BZ=F",
    "gold":      "GC=F",
    "silver":    "SI=F",
    "platinum":  "PL=F",
    "copper":    "HG=F",
}

# FRED series
FRED_HY_OAS_SERIES = "BAMLH0A0HYM2"

# ── SQLite helpers ───────────────────────────────────────────────────────────

def _ensure_schema_upgraded():
    """Upgrade schema if needed. Creates new columns if they don't exist."""
    conn = sqlite3.connect(MACRO_DB_PATH)
    cursor = conn.cursor()
    
    # Check if the new columns exist
    cursor.execute("PRAGMA table_info(macro_snapshots)")
    columns = {row[1] for row in cursor.fetchall()}
    
    # List of new columns we need
    new_columns = [
        "nasdaq100", "nasdaq100_sma20", "nasdaq100_sma50", "nasdaq100_sma200",
        "vxn", "sp500", "sp500_sma20", "sp500_sma50", "sp500_sma200",
        "brent", "brent_sma20", "brent_sma50", "brent_sma200",
        "gold", "gold_sma20", "gold_sma50", "gold_sma200",
        "silver", "silver_sma20", "silver_sma50", "silver_sma200",
        "platinum", "platinum_sma20", "platinum_sma50", "platinum_sma200",
        "copper", "copper_sma20", "copper_sma50", "copper_sma200"
    ]
    
    # Add missing columns
    for col in new_columns:
        if col not in columns:
            try:
                cursor.execute(f"ALTER TABLE macro_snapshots ADD COLUMN {col} REAL")
                print(f"Added column: {col}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    print(f"Error adding column {col}: {e}")
    
    conn.commit()
    conn.close()


def _macro_db():
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
    # Add any missing columns for existing databases
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
            conn.execute(f"ALTER TABLE macro_snapshots ADD COLUMN {col} {typ}")
    conn.commit()
    return conn


def _store_macro_snapshot(snap: dict):
    """Upsert today's macro snapshot into SQLite."""
    conn = _macro_db()
    today = date.today().isoformat()
    
    try:
        conn.execute("""
            INSERT INTO macro_snapshots
                (date, yield_1y, yield_2y, yield_3y, yield_5y, yield_10y,
                 dxy, vix, hy_oas, nasdaq100, nasdaq100_sma20, nasdaq100_sma50, nasdaq100_sma200,
                 vxn, sp500, sp500_sma20, sp500_sma50, sp500_sma200,
                 brent, brent_sma20, brent_sma50, brent_sma200,
                 gold, gold_sma20, gold_sma50, gold_sma200,
                 silver, silver_sma20, silver_sma50, silver_sma200,
                 platinum, platinum_sma20, platinum_sma50, platinum_sma200,
                 copper, copper_sma20, copper_sma50, copper_sma200, stored_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date) DO UPDATE SET
                yield_1y=excluded.yield_1y, yield_2y=excluded.yield_2y,
                yield_3y=excluded.yield_3y, yield_5y=excluded.yield_5y,
                yield_10y=excluded.yield_10y, dxy=excluded.dxy,
                vix=excluded.vix, hy_oas=excluded.hy_oas,
                nasdaq100=excluded.nasdaq100, nasdaq100_sma20=excluded.nasdaq100_sma20,
                nasdaq100_sma50=excluded.nasdaq100_sma50, nasdaq100_sma200=excluded.nasdaq100_sma200,
                vxn=excluded.vxn, sp500=excluded.sp500,
                sp500_sma20=excluded.sp500_sma20, sp500_sma50=excluded.sp500_sma50,
                sp500_sma200=excluded.sp500_sma200, brent=excluded.brent,
                brent_sma20=excluded.brent_sma20, brent_sma50=excluded.brent_sma50,
                brent_sma200=excluded.brent_sma200,
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
        print(f"Error storing macro snapshot: {e}")
        conn.rollback()
    finally:
        conn.close()


def _fetch_macro_history_rows(n_days: int = 95) -> list[dict]:
    """Safely fetch history, returning only columns that exist."""
    conn = _macro_db()
    
    try:
        # Get available columns from table
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(macro_snapshots)")
        available_cols = {row[1] for row in cursor.fetchall()}
        
        # Build SELECT with only available columns
        base_cols = ["date", "yield_1y", "yield_2y", "yield_3y", "yield_5y", "yield_10y", "dxy", "vix", "hy_oas"]
        new_cols = ["nasdaq100", "nasdaq100_sma20", "nasdaq100_sma50", "nasdaq100_sma200",
                    "vxn", "sp500", "sp500_sma20", "sp500_sma50", "sp500_sma200",
                    "brent", "brent_sma20", "brent_sma50", "brent_sma200"]
        
        select_cols = base_cols + [c for c in new_cols if c in available_cols]
        select_clause = ", ".join(select_cols)
        
        query = f"SELECT {select_clause} FROM macro_snapshots ORDER BY date DESC LIMIT ?"
        rows = conn.execute(query, (n_days,)).fetchall()
        
        return [dict(zip(select_cols, r)) for r in rows]
    except Exception as e:
        print(f"Error fetching macro history: {e}")
        return []
    finally:
        conn.close()

# ── Data fetchers ────────────────────────────────────────────────────────────

def _fetch_yfinance_bulk(n_days: int = 300) -> dict:
    """Download n_days of daily data. 300d ensures Brent futures have enough history for 200d SMA."""
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
                # Single ticker case
                if len(YF_TICKERS) == 1:
                    result[key] = close.dropna()
                else:
                    result[key] = None
        return result
    except Exception as e:
        print(f"yfinance bulk download error: {e}")
        return {k: None for k in YF_TICKERS}


def _fetch_fred_hy_oas(n_days: int = 200) -> dict:
    """Fetch ICE BofA HY OAS from FRED."""
    if not FRED_API_KEY:
        return {"values": [], "error": "No FRED_API_KEY set"}
    end   = date.today()
    start = end - timedelta(days=n_days + 30)
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={FRED_HY_OAS_SERIES}"
        f"&observation_start={start.isoformat()}"
        f"&observation_end={end.isoformat()}"
        f"&api_key={FRED_API_KEY}"
        f"&file_type=json"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        obs = resp.json().get("observations", [])
        values = []
        for o in obs:
            try:
                values.append((o["date"], float(o["value"])))
            except (ValueError, KeyError):
                pass
        return {"values": values}
    except Exception as e:
        return {"values": [], "error": str(e)}

# ── SMA Calculator ──────────────────────────────────────────────────────────

def _calculate_sma(series, window: int) -> float | None:
    """Calculate SMA for a pandas Series, return last value."""
    if series is None or len(series) < window:
        return None
    return series.tail(window).mean()


def _sma_pct_diff(current: float | None, sma: float | None) -> float | None:
    """Calculate % difference of current vs SMA: (current - sma) / sma * 100"""
    if current is None or sma is None or sma == 0:
        return None
    return ((current - sma) / sma) * 100


def _pct_rank(series, current_val) -> int | None:
    """Return 0–100 percentile of current_val within series."""
    if series is None or len(series) < 5:
        return None
    arr = sorted(series)
    below = sum(1 for v in arr if v < current_val)
    return round(below / len(arr) * 100)

# ── Formatters ──────────────────────────────────────────────────────────────

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

    def _alert(p):
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
        "d20_chg":   d20,
        "percentile": pctile,
        "alert":     _alert(pctile),
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
        "d20_chg":   d20,
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
    current = round(vals[-1], 2)
    d5  = round(current - vals[-6], 2)  if len(vals) >= 6  else None
    d20 = round(current - vals[-21], 2) if len(vals) >= 21 else None
    pctile = _pct_rank(vals, current)

    def _alert(cur):
        if cur is None: return "Normal"
        if cur >= 6.0:  return "Distress"
        if cur >= 4.5:  return "Stressed"
        if cur >= 3.5:  return "Moderately stressed"
        if cur <= 2.5:  return "Compressed / risk-on"
        return "Normal"

    return {
        "current":    current,
        "d5_chg":     d5,
        "d20_chg":    d20,
        "percentile": pctile,
        "alert":      _alert(current),
        "pattern":    "Spreads tightening" if (d5 and d5 < -0.10) else \
                      "Spreads widening"   if (d5 and d5 > 0.10)  else "Stable",
    }


def _fmt_equity_sma_card(ticker_name: str, series) -> dict:
    """Format equity/commodity card with 20/50/200 SMAs."""
    if series is None or len(series) == 0:
        return {"current": None, "error": "No data"}

    vals = series.tolist()
    current = round(vals[-1], 2)

    sma20 = _calculate_sma(series, 20)
    sma50 = _calculate_sma(series, 50)
    sma200 = _calculate_sma(series, 200)

    pct_20 = _sma_pct_diff(current, sma20)
    pct_50 = _sma_pct_diff(current, sma50)
    pct_200 = _sma_pct_diff(current, sma200)

    pctile = _pct_rank(vals, current)

    # Alert logic: price relative to 200 SMA
    alert = "–"
    if pct_200 is not None:
        if pct_200 < -10: alert = "Far below 200d SMA"
        elif pct_200 < -5: alert = "Below 200d SMA"
        elif pct_200 > 15: alert = "Well above 200d SMA"
        elif pct_200 > 10: alert = "Above 200d SMA"

    return {
        "current": current,
        "sma20": round(sma20, 2) if sma20 else None,
        "sma50": round(sma50, 2) if sma50 else None,
        "sma200": round(sma200, 2) if sma200 else None,
        "pct_from_sma20": round(pct_20, 2) if pct_20 else None,
        "pct_from_sma50": round(pct_50, 2) if pct_50 else None,
        "pct_from_sma200": round(pct_200, 2) if pct_200 else None,
        "percentile": pctile,
        "alert": alert,
    }


def _fmt_vix_derivative(series) -> dict:
    """Format VXN as a volatility index (no SMAs, similar to VIX)."""
    if series is None or len(series) == 0:
        return {"current": None, "error": "No data"}
    vals = series.tolist()
    current = round(vals[-1], 2)
    d5  = round(current - vals[-6], 2)  if len(vals) >= 6  else None
    d20 = round(current - vals[-21], 2) if len(vals) >= 21 else None
    pctile = _pct_rank(vals, current)

    def _alert(cur):
        if cur is None: return "Normal"
        if cur >= 30: return "Tech volatility elevated"
        if cur >= 20: return "Elevated"
        if cur <= 15: return "Tech calm"
        return "Normal"

    return {
        "current":   current,
        "d5_chg":    d5,
        "d20_chg":   d20,
        "percentile": pctile,
        "alert":     _alert(current),
        "pattern":   "Tech volatility rising" if (d5 and d5 > 3) else "Tech volatility stable" if (d5 and abs(d5) <= 1) else "Tech volatility falling",
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
from datetime import datetime, timezone, timedelta

EST = timezone(timedelta(hours=-5))  # use -4 for EDT if you want to follow DST

def _last_10am_est() -> datetime:
    """Returns the most recent 10AM EST as a UTC-aware datetime."""
    now_est = datetime.now(EST)
    today_10am = now_est.replace(hour=10, minute=0, second=0, microsecond=0)
    if now_est < today_10am:
        # haven't hit 10am today yet — last refresh was yesterday at 10am
        return today_10am - timedelta(days=1)
    return today_10am

def _cache_is_stale(cache: dict) -> bool:
    if not cache["data"]:
        return True
    last_refresh = _last_10am_est()
    cache_time = datetime.fromtimestamp(cache["ts"], tz=timezone.utc)
    return cache_time < last_refresh


_macro_cache: dict = {"data": None, "ts": 0.0}



def _build_macro_metrics() -> dict:
    global _macro_cache
    now = time.time()
    if not _cache_is_stale(_macro_cache):
      return _macro_cache["data"]

    yf_data  = _fetch_yfinance_bulk(n_days=300)
    fred_data = _fetch_fred_hy_oas(n_days=300)

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
        "nasdaq100": _fmt_equity_sma_card("Nasdaq-100", yf_data.get("nasdaq100")),
        "vxn":       _fmt_vix_derivative(yf_data.get("vxn")),
        "sp500":     _fmt_equity_sma_card("S&P 500", yf_data.get("sp500")),
        "brent":     _fmt_equity_sma_card("Brent Crude", yf_data.get("brent")),
        "gold":      _fmt_equity_sma_card("Gold", yf_data.get("gold")),
        "silver":    _fmt_equity_sma_card("Silver", yf_data.get("silver")),
        "platinum":  _fmt_equity_sma_card("Platinum", yf_data.get("platinum")),
        "copper":    _fmt_equity_sma_card("Copper", yf_data.get("copper")),
    }

    # Persist snapshot
    snap = {
        "yield_1y":  yields["1y"].get("current"),
        "yield_2y":  yields["2y"].get("current"),
        "yield_3y":  yields["3y"].get("current"),
        "yield_5y":  yields["5y"].get("current"),
        "yield_10y": yields["10y"].get("current"),
        "dxy":       result["dxy"].get("current"),
        "vix":       result["vix"].get("current"),
        "hy_oas":    result["hy_oas"].get("current"),
        "nasdaq100": result["nasdaq100"].get("current"),
        "nasdaq100_sma20": result["nasdaq100"].get("sma20"),
        "nasdaq100_sma50": result["nasdaq100"].get("sma50"),
        "nasdaq100_sma200": result["nasdaq100"].get("sma200"),
        "vxn":       result["vxn"].get("current"),
        "sp500":     result["sp500"].get("current"),
        "sp500_sma20": result["sp500"].get("sma20"),
        "sp500_sma50": result["sp500"].get("sma50"),
        "sp500_sma200": result["sp500"].get("sma200"),
        "brent":     result["brent"].get("current"),
        "brent_sma20": result["brent"].get("sma20"),
        "brent_sma50": result["brent"].get("sma50"),
        "brent_sma200": result["brent"].get("sma200"),
        "gold":      result["gold"].get("current"),
        "gold_sma20": result["gold"].get("sma20"),
        "gold_sma50": result["gold"].get("sma50"),
        "gold_sma200": result["gold"].get("sma200"),
        "silver":    result["silver"].get("current"),
        "silver_sma20": result["silver"].get("sma20"),
        "silver_sma50": result["silver"].get("sma50"),
        "silver_sma200": result["silver"].get("sma200"),
        "platinum":  result["platinum"].get("current"),
        "platinum_sma20": result["platinum"].get("sma20"),
        "platinum_sma50": result["platinum"].get("sma50"),
        "platinum_sma200": result["platinum"].get("sma200"),
        "copper":    result["copper"].get("current"),
        "copper_sma20": result["copper"].get("sma20"),
        "copper_sma50": result["copper"].get("sma50"),
        "copper_sma200": result["copper"].get("sma200"),
    }
    _store_macro_snapshot(snap)

    _macro_cache["data"] = result
    _macro_cache["ts"]   = now
    return result

# ── Routes ───────────────────────────────────────────────────────────────────

@macro_router.get("/metrics")
def get_macro_metrics():
    """
    Returns: {
      updated_at, yields, curve, dxy, vix, hy_oas,
      nasdaq100, vxn, sp500, brent (all with SMA data)
    }
    """
    return _build_macro_metrics()


@macro_router.get("/history")
def get_macro_history(days: int = 90):
    """Returns last N days of stored macro snapshots from SQLite."""
    rows = _fetch_macro_history_rows(n_days=days)
    return {"rows": rows, "count": len(rows)}

@macro_router.get("/cache/flush")
def flush_macro_cache():
    global _macro_cache
    _macro_cache = {"data": None, "ts": 0.0}
    return {"flushed": True, "cache": "macro"}

# ── Registration (add this to the bottom of main.py) ──────────────────────
#
#   from macro_routes import macro_router
#   app.include_router(macro_router)
#
# Then your endpoints are:
#   GET /macro/metrics
#   GET /macro/history?days=90
