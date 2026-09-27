from __future__ import annotations

import argparse
import csv
import io
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import requests

from altcoin_history import db_summary, existing_history_index, store_universe, upsert_daily_closes
from altcoin_sources import fetch_binance_daily_klines, fetch_binance_universe

BASE = "https://data.binance.vision/data/spot"


def _months(start: date, end: date):
    out = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append((y, m))
        m += 1
        if m == 13:
            y += 1
            m = 1
    return out


def _parse_archive(content: bytes, symbol: str, asset: str):
    out = []
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names = z.namelist()
        if not names:
            return out
        for row in csv.reader(io.StringIO(z.read(names[0]).decode())):
            try:
                ts = int(row[0])
                # Binance archives have historically used milliseconds; tolerate microseconds.
                if ts > 10**14:
                    ts //= 1000
                day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date().isoformat()
                out.append(
                    {
                        "symbol": symbol,
                        "asset": asset,
                        "date": day,
                        "close": float(row[4]),
                        "source": "Binance static archive",
                    }
                )
            except (ValueError, IndexError, OSError):
                pass
    return out


def _get_archive(url: str):
    response = requests.get(url, timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.content


def _month_complete(month_key: str, last_date: str | None) -> bool:
    if not last_date:
        return False
    y, m = map(int, month_key.split("-"))
    if m == 12:
        month_end = date(y + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(y, m + 1, 1) - timedelta(days=1)
    return last_date >= month_end.isoformat()


def seed(days: int = 260, workers: int = 8, symbol_filter: set[str] | None = None):
    today = datetime.now(timezone.utc).date()
    end = today - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    current_month = date(today.year, today.month, 1)

    universe = fetch_binance_universe()
    store_universe(today.isoformat(), universe)

    rows = {r["symbol"]: r for r in universe if r.get("included")}
    rows["BTCUSDT"] = {"symbol": "BTCUSDT", "asset": "BTC"}
    rows["ETHUSDT"] = {"symbol": "ETHUSDT", "asset": "ETH"}

    if symbol_filter:
        rows = {k: v for k, v in rows.items() if k in symbol_filter}

    history = existing_history_index(rows.keys())
    archive_jobs = []

    # Use one monthly archive per completed month. If that month already exists
    # through its month-end close, skip it on reruns.
    for r in rows.values():
        symbol, asset = r["symbol"], r["asset"]
        state = history.get(symbol, {})
        last_date = state.get("last_date")
        for y, m in _months(start, end):
            month_start = date(y, m, 1)
            if month_start >= current_month:
                continue
            month_key = f"{y:04d}-{m:02d}"
            if month_key in set(state.get("months") or []) and _month_complete(month_key, last_date):
                continue
            archive_jobs.append(
                (
                    symbol,
                    asset,
                    f"{BASE}/monthly/klines/{symbol}/1d/{symbol}-1d-{month_key}.zip",
                )
            )

    written = 0
    errors = 0
    missing_archives = 0

    def run_archive(job):
        symbol, asset, url = job
        content = _get_archive(url)
        if not content:
            return symbol, [], True
        return symbol, _parse_archive(content, symbol, asset), False

    if archive_jobs:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {pool.submit(run_archive, job): job for job in archive_jobs}
            for i, future in enumerate(as_completed(futures), 1):
                symbol, _, _ = futures[future]
                try:
                    _, parsed, missing = future.result()
                    if missing:
                        missing_archives += 1
                    else:
                        written += upsert_daily_closes(parsed)
                except Exception as exc:
                    errors += 1
                    print(f"[altcoin_backfill] archive {symbol}: {exc}")

                if i % 100 == 0 or i == len(archive_jobs):
                    print(
                        f"[altcoin_backfill] archives {i}/{len(archive_jobs)} "
                        f"· {written} rows · {missing_archives} missing · {errors} errors"
                    )

    # One REST kline request per symbol fills the partial current month and any
    # post-archive tail. On reruns, start from the day after the persisted close.
    refreshed_history = existing_history_index(rows.keys())
    tail_jobs = []
    for r in rows.values():
        symbol, asset = r["symbol"], r["asset"]
        last_date = (refreshed_history.get(symbol) or {}).get("last_date")
        if last_date:
            tail_start = datetime.fromisoformat(last_date).date() + timedelta(days=1)
        else:
            tail_start = max(start, current_month)
        if tail_start <= end:
            tail_jobs.append((symbol, asset, tail_start.isoformat(), end.isoformat()))

    def run_tail(job):
        symbol, asset, start_date, end_date = job
        rows_out = fetch_binance_daily_klines(symbol, start_date, end_date)
        for row in rows_out:
            row["asset"] = asset
        return symbol, rows_out

    if tail_jobs:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {pool.submit(run_tail, job): job for job in tail_jobs}
            for i, future in enumerate(as_completed(futures), 1):
                symbol = futures[future][0]
                try:
                    _, parsed = future.result()
                    written += upsert_daily_closes(parsed)
                except Exception as exc:
                    errors += 1
                    print(f"[altcoin_backfill] tail {symbol}: {exc}")

                if i % 100 == 0 or i == len(tail_jobs):
                    print(
                        f"[altcoin_backfill] tails {i}/{len(tail_jobs)} "
                        f"· {written} rows · {errors} errors"
                    )

    return {
        "symbols": len(rows),
        "archive_jobs": len(archive_jobs),
        "tail_jobs": len(tail_jobs),
        "rows_written": written,
        "missing_archives": missing_archives,
        "errors": errors,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=260)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--symbols", default="")
    args = parser.parse_args()

    symbols = {x.strip().upper() for x in args.symbols.split(",") if x.strip()} or None
    print(seed(args.days, args.workers, symbols))
    print(db_summary())
