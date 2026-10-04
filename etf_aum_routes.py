"""Dated market capitalization estimates for the existing eight-fund BTC basket.

Shares × market close is market capitalization, not issuer-reported net assets.
Only complete, fresh, dated inputs enter v2 history. Legacy poll-dated rows are
retained in their original table and never used for comparisons.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import yfinance as yf
from fastapi import APIRouter

etf_aum_router = APIRouter(prefix="/etf-aum")
DATA_DIR = os.getenv("DATA_DIR", "./data")
AUM_DB = os.path.join(DATA_DIR, "etf_aum_history.db")
CACHE_TTL = 3600
MAX_SHARES_AGE_DAYS = 7
MAX_PRICE_AGE_DAYS = 4
MIN_PERCENTILE_SAMPLES = 60
NY = ZoneInfo("America/New_York")
_cache = {"data": None, "ts": 0.0}
_lock = threading.Lock()
ETF_TICKERS = {
    "IBIT": "iShares Bitcoin Trust (BlackRock)",
    "FBTC": "Fidelity Wise Origin Bitcoin Fund",
    "ARKB": "ARK 21Shares Bitcoin ETF",
    "BITB": "Bitwise Bitcoin ETF",
    "HODL": "VanEck Bitcoin ETF",
    "BTCO": "Invesco Galaxy Bitcoin ETF",
    "EZBC": "Franklin Bitcoin ETF",
    "BRRR": "CoinShares Bitcoin ETF",
}


def _now():
    return datetime.now(timezone.utc)


def _positive(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (ValueError, TypeError, OverflowError):
        return None


def _db():
    os.makedirs(os.path.dirname(os.path.abspath(AUM_DB)), exist_ok=True)
    conn = sqlite3.connect(AUM_DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS aum_snapshots_v2 (
        date TEXT PRIMARY KEY, total_aum REAL NOT NULL,
        components TEXT NOT NULL, stored_at TEXT NOT NULL
    )""")
    conn.commit()
    return conn


def _store_snapshot(snap):
    # Defensive validation keeps partial/undated values out of the trusted store.
    components = snap["components"]
    if set(components) != set(ETF_TICKERS):
        raise ValueError("Snapshot must cover the fixed ETF basket")
    as_of = date.fromisoformat(snap["date"])
    for row in components.values():
        shares_date = date.fromisoformat(row["shares_as_of"])
        if (row["price_as_of"] != snap["date"] or
                not 0 <= (as_of - shares_date).days <= MAX_SHARES_AGE_DAYS or
                not all(_positive(row[k]) for k in ("shares", "close", "market_cap")) or
                not math.isclose(row["market_cap"], row["shares"] * row["close"])):
            raise ValueError("Invalid dated component")
    total = sum(row["market_cap"] for row in components.values())
    if not _positive(snap["total_aum"]) or not math.isclose(total, snap["total_aum"]):
        raise ValueError("Snapshot total does not match components")
    conn = _db()
    try:
        conn.execute("""INSERT INTO aum_snapshots_v2 VALUES (?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET total_aum=excluded.total_aum,
            components=excluded.components, stored_at=excluded.stored_at""",
            (snap["date"], total, json.dumps(components, allow_nan=False), _now().isoformat()))
        conn.commit()
    finally:
        conn.close()


def _fetch_history(n_days=120):
    conn = _db()
    try:
        rows = conn.execute("""SELECT date, total_aum, components
            FROM aum_snapshots_v2 ORDER BY date DESC LIMIT ?""", (n_days,)).fetchall()
        return [{"date": row[0], "total_aum": row[1], "components": json.loads(row[2])}
                for row in reversed(rows)]
    finally:
        conn.close()


def _dated_values(series, cutoff):
    """Provider daily indices are exchange-session labels, not poll timestamps."""
    result = {}
    if series is None:
        return result
    for index, value in series.sort_index().items():
        day = index.date()
        value = _positive(value)
        if day <= cutoff and value is not None:
            result[day.isoformat()] = value
    return result


def _fetch_aum(now=None):
    now = now or _now()
    local = now.astimezone(NY)
    # Exclude the current session until after close plus a publication buffer.
    cutoff = local.date() if (local.hour, local.minute) >= (16, 15) else local.date() - timedelta(days=1)
    closes, shares, splits, errors = {}, {}, {}, []
    try:
        raw = yf.download(list(ETF_TICKERS), period="5d", auto_adjust=False,
                          progress=False, threads=2, timeout=15, actions=True)
        close = raw["Close"]
        split_data = raw["Stock Splits"] if "Stock Splits" in raw.columns else None
    except Exception as exc:
        close, split_data = None, None
        errors.append(f"Price request failed: {type(exc).__name__}")
    for ticker in ETF_TICKERS:
        splits[ticker] = _dated_values(split_data[ticker], local.date()) if split_data is not None and ticker in split_data else {}
        closes[ticker] = _dated_values(close[ticker], cutoff) if close is not None and ticker in close else {}
        # Yahoo can restate Friday's Close for a Monday split before Monday's
        # session completes. Restore the pre-split close used with Friday shares.
        for day, value in closes[ticker].items():
            later_splits = [ratio for split_day, ratio in splits[ticker].items() if split_day > day]
            closes[ticker][day] = value * math.prod(later_splits)
        try:
            series = yf.Ticker(ticker).get_shares_full(
                start=(cutoff - timedelta(days=14)).isoformat(),
                end=(cutoff + timedelta(days=1)).isoformat())
            shares[ticker] = _dated_values(series, cutoff)
        except Exception as exc:
            shares[ticker] = {}
            errors.append(f"{ticker} shares request failed: {type(exc).__name__}")
    return _align_components(closes, shares, local.date(), errors, splits)


def _align_components(closes, shares, today, errors=None, splits=None):
    # All funds must have their most recent completed close on the same date.
    latest = {t: max(closes.get(t, {}), default=None) for t in ETF_TICKERS}
    available_dates = {day for day in latest.values() if day}
    as_of = max(available_dates, default=None)
    components, missing = {}, []
    for ticker in ETF_TICKERS:
        price = _positive(closes.get(ticker, {}).get(as_of))
        eligible = [day for day in shares.get(ticker, {}) if as_of and day <= as_of]
        shares_date = max(eligible, default=None)
        count = _positive(shares.get(ticker, {}).get(shares_date))
        if (price is None or count is None or shares_date is None or
                (date.fromisoformat(as_of) - date.fromisoformat(shares_date)).days > MAX_SHARES_AGE_DAYS):
            missing.append(ticker)
            continue
        if (_positive(count * price) is None or
                any(shares_date < day <= as_of for day in (splits or {}).get(ticker, {}))):
            missing.append(ticker)
            continue
        components[ticker] = {"shares": count, "shares_as_of": shares_date,
                              "close": price, "price_as_of": as_of,
                              "market_cap": count * price}
    stale = as_of is not None and (today - date.fromisoformat(as_of)).days > MAX_PRICE_AGE_DAYS
    total = _positive(sum(r["market_cap"] for r in components.values()))
    complete = len(components) == len(ETF_TICKERS) and not stale and total is not None
    return {"date": as_of, "components": components,
            "total_aum": total if complete else None,
            "missing": missing, "stale": stale, "errors": errors or []}


def _baseline(history, as_of, days):
    target = date.fromisoformat(as_of) - timedelta(days=days)
    eligible = [r for r in history if date.fromisoformat(r["date"]) <= target]
    row = eligible[-1] if eligible else None
    # The last observation before a weekend/holiday is usable, a long gap is not.
    return row if row and (target - date.fromisoformat(row["date"])).days <= 4 else None


def _change(current, prior):
    if current is None or prior is None:
        return "—", "—"
    delta = current - prior["total_aum"]
    sign = "+" if delta >= 0 else "-"
    amount = abs(delta)
    money = f"{sign}${amount / 1e9:.1f}B" if amount >= 1e9 else f"{sign}${amount / 1e6:.0f}M"
    return money, f"{delta / prior['total_aum'] * 100:+.1f}%"


def _build_etf_aum():
    with _lock:
        stamp = time.time()
        if _cache["data"] is not None and stamp - _cache["ts"] < CACHE_TTL:
            return _cache["data"]
        now = _now()
        observation = _fetch_aum(now)
        current = observation["total_aum"]
        if current is not None:
            _store_snapshot(observation)
        history = _fetch_history()
        today = now.astimezone(NY).date()
        history = [r for r in history if date.fromisoformat(r["date"]) <= today]
        as_of = observation["date"]
        status = "ok" if current is not None else "unavailable"
        components = observation["components"]
        # A persisted last good value can be shown, but never as a fresh reading.
        if current is None and history:
            previous = history[-1]
            current, as_of, components = previous["total_aum"], previous["date"], previous["components"]
            status = "stale"
        history = [r for r in history if as_of and r["date"] <= as_of]
        d7 = _baseline(history, as_of, 7) if as_of and status == "ok" else None
        d30 = _baseline(history, as_of, 30) if as_of and status == "ok" else None
        d7_chg, d7_pct = _change(current, d7)
        d30_chg, d30_pct = _change(current, d30)
        window = [r for r in history if (date.fromisoformat(as_of) - date.fromisoformat(r["date"])).days <= 90] if as_of else []
        percentile = None
        if (status == "ok" and len(window) >= MIN_PERCENTILE_SAMPLES and
                (date.fromisoformat(as_of) - date.fromisoformat(window[0]["date"])).days >= 85):
            values = [r["total_aum"] for r in window]
            percentile = round(100 * (sum(v < current for v in values) + .5 * sum(v == current for v in values)) / len(values))
        alert, level = "—", "none"
        if percentile is not None:
            if percentile >= 90:
                alert, level = "Market cap near 90-day highs", "extreme"
            elif percentile >= 75:
                alert, level = "Market cap in upper 90-day range", "notable"
            elif percentile <= 20:
                alert, level = "Market cap in lower 90-day range", "notable"
        breakdown = []
        for ticker, name in ETF_TICKERS.items():
            row = components.get(ticker, {})
            value = row.get("market_cap")
            breakdown.append({"ticker": ticker, "name": name,
                "aum": f"${value / 1e9:.2f}B" if value else "—", "aum_raw": value,
                "share_pct": round(value / current * 100, 1) if value and current else None,
                **row})
        breakdown.sort(key=lambda row: row["aum_raw"] or 0, reverse=True)
        result = {
            "updated_at": now.isoformat(), "as_of": as_of, "methodology_version": 2,
            "total_aum": f"${current / 1e9:.1f}B" if current is not None else "—",
            "total_aum_raw": current, "d7_chg": d7_chg, "d7_pct": d7_pct,
            "d30_chg": d30_chg, "d30_pct": d30_pct,
            "comparison_dates": {"d7": d7["date"] if d7 else None, "d30": d30["date"] if d30 else None},
            "percentile": percentile, "alert": alert, "alert_level": level,
            "spark": [round(r["total_aum"] / 1e9, 2) for r in history[-30:]],
            "spark_dates": [r["date"] for r in history[-30:]],
            "breakdown": breakdown, "etf_count": len(components),
            "expected_etf_count": len(ETF_TICKERS), "basket": list(ETF_TICKERS),
            "history_samples": len(window),
            "data_quality": {"status": status, "missing_tickers": observation["missing"],
                             "source_stale": observation["stale"], "errors": observation["errors"]},
            "source": "Yahoo Finance via yfinance: dated shares outstanding and unadjusted daily close",
            "note": "Market capitalization estimate = latest dated shares × completed session close. Shares may lag close by up to 7 calendar days. Fixed eight-fund basket; excludes GBTC/BTC and other funds. Changes include price movement and share issuance/redemptions; they are not net flows or issuer-reported AUM. Legacy undated history is excluded.",
        }
        _cache.update(data=result, ts=stamp)
        return result


@etf_aum_router.get("/metrics")
def get_etf_aum():
    return _build_etf_aum()


@etf_aum_router.get("/cache/flush")
def flush_etf_aum_cache():
    with _lock:
        _cache.update(data=None, ts=0.0)
    return {"flushed": True}
