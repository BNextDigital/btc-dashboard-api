"""
etf_aum_routes.py — BTC ETF AUM (Market Cap) Tracker

Computes total AUM across the major US spot Bitcoin ETFs by fetching
shares_outstanding and daily close prices from yFinance, then storing
daily snapshots in SQLite for trend/sparkline history.

WHY NOT USE totalAssets FROM yf.Ticker.info?
  totalAssets from the yFinance info dict lags by 1–3 days and frequently
  returns 0 or stale values. Computing AUM directly as:
      AUM = shares_outstanding × close_price
  using the ETF's daily price history is more reliable and updates each
  trading day after market close.

ETF basket (US spot BTC ETFs as of 2026):
  IBIT   — iShares Bitcoin Trust (BlackRock)
  FBTC   — Fidelity Wise Origin Bitcoin Fund
  ARKB   — ARK 21Shares Bitcoin ETF
  BITB   — Bitwise Bitcoin ETF
  HODL   — VanEck Bitcoin ETF
  BTCO   — Invesco Galaxy Bitcoin ETF
  EZBC   — Franklin Bitcoin ETF
  BRRR   — Valkyrie Bitcoin Fund

Setup:
  1. Copy to btc-dashboard-api/etf_aum_routes.py
  2. In main.py:
       from etf_aum_routes import etf_aum_router
       app.include_router(etf_aum_router)

Endpoints:
  GET /etf-aum/metrics
  GET /etf-aum/cache/flush
"""

import os
import math
import time
import sqlite3
import yfinance as yf
from datetime import datetime, date, timedelta
from fastapi import APIRouter

# ── Router ────────────────────────────────────────────────────────────────────
etf_aum_router = APIRouter(prefix="/etf-aum")

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR     = os.getenv("DATA_DIR", "./data")
AUM_DB       = os.path.join(DATA_DIR, "etf_aum_history.db")
CACHE_TTL    = 3600   # 1 hour — updates once per trading day after close

_cache: dict = {"data": None, "ts": 0.0}

# ── ETF basket ────────────────────────────────────────────────────────────────
ETF_TICKERS = {
    "IBIT": "iShares Bitcoin Trust (BlackRock)",
    "FBTC": "Fidelity Wise Origin Bitcoin Fund",
    "ARKB": "ARK 21Shares Bitcoin ETF",
    "BITB": "Bitwise Bitcoin ETF",
    "HODL": "VanEck Bitcoin ETF",
    "BTCO": "Invesco Galaxy Bitcoin ETF",
    "EZBC": "Franklin Bitcoin ETF",
    "BRRR": "Valkyrie Bitcoin Fund",
}

# ── SQLite ────────────────────────────────────────────────────────────────────

def _db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(AUM_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS aum_snapshots (
            date        TEXT PRIMARY KEY,
            total_aum   REAL,
            ibit_aum    REAL,
            fbtc_aum    REAL,
            arkb_aum    REAL,
            bitb_aum    REAL,
            hodl_aum    REAL,
            btco_aum    REAL,
            ezbc_aum    REAL,
            brrr_aum    REAL,
            stored_at   TEXT
        )
    """)
    conn.commit()
    return conn


def _store_snapshot(snap: dict):
    conn = _db()
    conn.execute("""
        INSERT INTO aum_snapshots
            (date, total_aum, ibit_aum, fbtc_aum, arkb_aum, bitb_aum,
             hodl_aum, btco_aum, ezbc_aum, brrr_aum, stored_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(date) DO UPDATE SET
            total_aum=excluded.total_aum,
            ibit_aum=excluded.ibit_aum, fbtc_aum=excluded.fbtc_aum,
            arkb_aum=excluded.arkb_aum, bitb_aum=excluded.bitb_aum,
            hodl_aum=excluded.hodl_aum, btco_aum=excluded.btco_aum,
            ezbc_aum=excluded.ezbc_aum, brrr_aum=excluded.brrr_aum,
            stored_at=excluded.stored_at
    """, (
        snap["date"], snap.get("total_aum"),
        snap.get("IBIT"), snap.get("FBTC"), snap.get("ARKB"), snap.get("BITB"),
        snap.get("HODL"), snap.get("BTCO"), snap.get("EZBC"), snap.get("BRRR"),
        datetime.utcnow().isoformat(),
    ))
    conn.commit()
    conn.close()


def _fetch_history(n_days: int = 90) -> list[dict]:
    try:
        conn = _db()
        rows = conn.execute("""
            SELECT date, total_aum FROM aum_snapshots
            WHERE total_aum IS NOT NULL
            ORDER BY date DESC LIMIT ?
        """, (n_days,)).fetchall()
        conn.close()
        return [{"date": r[0], "total_aum": r[1]} for r in reversed(rows)]
    except Exception:
        return []

# ── AUM fetch ─────────────────────────────────────────────────────────────────

def _san(v) -> float | None:
    """Sanitize NaN/Inf to None."""
    try:
        return None if (v is None or math.isnan(v) or math.isinf(v)) else float(v)
    except (TypeError, ValueError):
        return None


def _fetch_aum() -> dict:
    """
    Fetch AUM for each ETF as shares_outstanding × close_price.
    Falls back to totalAssets from info if shares_outstanding is unavailable.
    Returns {ticker: aum_usd, ...} and {ticker: history_series, ...}.
    """
    tickers = list(ETF_TICKERS.keys())
    aum_today: dict[str, float | None] = {}
    history_by_ticker: dict[str, list[float]] = {}

    try:
        # Bulk download 90 days of price history
        raw = yf.download(
            tickers, period="90d",
            auto_adjust=True, progress=False, threads=True
        )
        close = raw["Close"] if "Close" in raw.columns else raw
    except Exception as e:
        print(f"[etf_aum] yFinance bulk download error: {e}")
        close = None

    for ticker in tickers:
        try:
            etf = yf.Ticker(ticker)

            # Shares outstanding — most reliable source
            shares = None
            info = etf.fast_info   # fast_info avoids slow network call
            try:
                shares = _san(getattr(info, "shares", None))
            except Exception:
                pass

            if shares is None:
                # Fallback to full info dict
                try:
                    full_info = etf.info
                    shares = _san(full_info.get("sharesOutstanding"))
                except Exception:
                    pass

            # Current close price
            price = None
            if close is not None and ticker in close.columns:
                series = close[ticker].dropna()
                if not series.empty:
                    price = _san(series.iloc[-1])
                    # Build historical AUM series (last 30 values for spark)
                    history_by_ticker[ticker] = [
                        _san(v) or 0 for v in series.tolist()
                    ]
            else:
                # Single-ticker fallback
                try:
                    hist = etf.history(period="1d")
                    if not hist.empty:
                        price = _san(hist["Close"].iloc[-1])
                except Exception:
                    pass

            # Compute AUM
            if shares and price:
                aum_today[ticker] = shares * price
            else:
                # Last resort: totalAssets from info
                try:
                    full_info = etf.info
                    total_assets = _san(full_info.get("totalAssets"))
                    if total_assets and total_assets > 0:
                        aum_today[ticker] = total_assets
                        print(f"[etf_aum] {ticker}: using totalAssets fallback")
                    else:
                        aum_today[ticker] = None
                        print(f"[etf_aum] {ticker}: no AUM data available")
                except Exception:
                    aum_today[ticker] = None

        except Exception as e:
            print(f"[etf_aum] {ticker} error: {e}")
            aum_today[ticker] = None

    return aum_today, history_by_ticker

# ── Card builder ──────────────────────────────────────────────────────────────

def _build_etf_aum() -> dict:
    global _cache
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    aum_by_ticker, history_by_ticker = _fetch_aum()

    # Total AUM
    valid_aums = [v for v in aum_by_ticker.values() if v is not None and v > 0]
    total_aum  = sum(valid_aums) if valid_aums else None

    # Historical snapshots from SQLite for sparkline + trend
    db_history = _fetch_history(n_days=90)

    # Store today's reading
    if total_aum:
        snap = {"date": date.today().isoformat(), "total_aum": total_aum}
        for ticker in ETF_TICKERS:
            snap[ticker] = aum_by_ticker.get(ticker)
        _store_snapshot(snap)

    # Build sparkline from SQLite history (last 30 trading days)
    spark = [round(r["total_aum"] / 1e9, 2) for r in db_history[-30:] if r["total_aum"]]

    # 7d and 30d changes
    d7_aum, d30_aum = None, None
    if len(db_history) >= 6:
        d7_aum = db_history[-6]["total_aum"]   # ~5 trading days back
    if len(db_history) >= 22:
        d30_aum = db_history[-22]["total_aum"]  # ~21 trading days back

    def chg_str(current, prev):
        if current is None or prev is None or prev == 0:
            return "—"
        delta = current - prev
        sign  = "+" if delta >= 0 else ""
        if abs(delta) >= 1e9:
            return f"{sign}${delta/1e9:.1f}B"
        return f"{sign}${delta/1e6:.0f}M"

    def pct_str(current, prev):
        if current is None or prev is None or prev == 0:
            return "—"
        return f"{(current - prev) / prev * 100:+.1f}%"

    # Percentile vs 90d history
    hist_totals = [r["total_aum"] for r in db_history if r["total_aum"]]
    pctile = 50
    if hist_totals and total_aum:
        pctile = round(sum(1 for v in hist_totals if v < total_aum) / len(hist_totals) * 100)

    # Alert
    if pctile >= 90:
        alert, alert_level = "AUM at cycle high — peak institutional positioning", "extreme"
    elif pctile >= 75:
        alert, alert_level = "AUM elevated — strong institutional presence", "notable"
    elif pctile <= 20:
        alert, alert_level = "AUM near lows — institutional positioning light", "notable"
    else:
        alert, alert_level = "AUM normal range", "none"

    # Per-ETF breakdown (sorted by AUM descending)
    breakdown = []
    for ticker, name in ETF_TICKERS.items():
        aum = aum_by_ticker.get(ticker)
        share_pct = round(aum / total_aum * 100, 1) if (aum and total_aum) else None
        breakdown.append({
            "ticker":    ticker,
            "name":      name,
            "aum":       f"${aum/1e9:.2f}B" if aum else "—",
            "aum_raw":   aum,
            "share_pct": share_pct,
        })
    breakdown.sort(key=lambda x: x["aum_raw"] or 0, reverse=True)

    # Clean breakdown for JSON
    for b in breakdown:
        del b["aum_raw"]

    result = {
        "updated_at":   datetime.utcnow().isoformat() + "Z",
        "total_aum":    f"${total_aum/1e9:.1f}B" if total_aum else "—",
        "total_aum_raw": total_aum,
        "d7_chg":       chg_str(total_aum, d7_aum),
        "d7_pct":       pct_str(total_aum, d7_aum),
        "d30_chg":      chg_str(total_aum, d30_aum),
        "d30_pct":      pct_str(total_aum, d30_aum),
        "percentile":   pctile,
        "alert":        alert,
        "alert_level":  alert_level,
        "spark":        spark,
        "breakdown":    breakdown,
        "etf_count":    len([b for b in breakdown if b["aum"] != "—"]),
        "note":         "AUM = shares_outstanding × close_price per ETF, summed across basket. Updates after US market close.",
    }

    _cache["data"] = result
    _cache["ts"]   = now
    return result

# ── Routes ────────────────────────────────────────────────────────────────────

@etf_aum_router.get("/metrics")
def get_etf_aum():
    """
    Returns total BTC ETF AUM across IBIT, FBTC, ARKB, BITB, HODL, BTCO, EZBC, BRRR.
    Computed as shares_outstanding × close_price per ETF. Cached 1 hour.
    """
    return _build_etf_aum()


@etf_aum_router.get("/cache/flush")
def flush_etf_aum_cache():
    global _cache
    _cache = {"data": None, "ts": 0.0}
    return {"flushed": True}

# ── Registration ──────────────────────────────────────────────────────────────
#
#   from etf_aum_routes import etf_aum_router
#   app.include_router(etf_aum_router)
#
# Endpoints:
#   GET /etf-aum/metrics
#   GET /etf-aum/cache/flush
