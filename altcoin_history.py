from __future__ import annotations
import json, os, sqlite3
from pathlib import Path
from typing import Iterable

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "altcoin_history.db"

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS daily_close(
            symbol TEXT NOT NULL, asset TEXT NOT NULL, date TEXT NOT NULL,
            close REAL NOT NULL, source TEXT NOT NULL DEFAULT 'Binance',
            PRIMARY KEY(symbol,date))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS universe_member(
            snapshot_date TEXT NOT NULL, symbol TEXT NOT NULL, asset TEXT NOT NULL,
            quote_asset TEXT NOT NULL, included INTEGER NOT NULL, exclusion_reason TEXT,
            PRIMARY KEY(snapshot_date,symbol))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS market_history(
            date TEXT PRIMARY KEY, altcoin_market_cap REAL, total_market_cap REAL,
            btc_dominance REAL, source TEXT NOT NULL, raw_timestamp TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS metric_snapshot(
            date TEXT PRIMARY KEY, calculation_version TEXT NOT NULL, payload_json TEXT NOT NULL)""")
        conn.commit()

def upsert_daily_closes(rows: Iterable[dict]) -> int:
    data=[]
    for r in rows:
        try:
            data.append((str(r["symbol"]).upper(),str(r["asset"]).upper(),str(r["date"]),float(r["close"]),str(r.get("source") or "Binance")))
        except (KeyError,TypeError,ValueError):
            pass
    if not data: return 0
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany("""INSERT INTO daily_close(symbol,asset,date,close,source) VALUES(?,?,?,?,?)
            ON CONFLICT(symbol,date) DO UPDATE SET asset=excluded.asset,close=excluded.close,source=excluded.source""",data)
        conn.commit()
    return len(data)

def store_universe(snapshot_date: str, rows: Iterable[dict]) -> int:
    data=[(snapshot_date,r["symbol"],r["asset"],r.get("quote_asset","USDT"),1 if r.get("included") else 0,r.get("exclusion_reason")) for r in rows]
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany("""INSERT INTO universe_member(snapshot_date,symbol,asset,quote_asset,included,exclusion_reason)
            VALUES(?,?,?,?,?,?) ON CONFLICT(snapshot_date,symbol) DO UPDATE SET
            asset=excluded.asset,quote_asset=excluded.quote_asset,included=excluded.included,exclusion_reason=excluded.exclusion_reason""",data)
        conn.commit()
    return len(data)

def load_histories(symbols: Iterable[str], limit_days: int = 420) -> dict[str,list[tuple[str,float]]]:
    out={}
    with sqlite3.connect(DB_PATH) as conn:
        for symbol in sorted(set(symbols)):
            rows=conn.execute("SELECT date,close FROM daily_close WHERE symbol=? ORDER BY date DESC LIMIT ?",(symbol,limit_days)).fetchall()
            out[symbol]=[(str(d),float(c)) for d,c in reversed(rows)]
    return out

def coverage(symbols: Iterable[str], minimum_days: int = 200) -> dict:
    symbols=sorted(set(symbols))
    any_count=qualified=0
    with sqlite3.connect(DB_PATH) as conn:
        for symbol in symbols:
            n=conn.execute("SELECT COUNT(*) FROM daily_close WHERE symbol=?",(symbol,)).fetchone()[0]
            any_count += 1 if n else 0
            qualified += 1 if n >= minimum_days else 0
    return {"universe_symbols":len(symbols),"with_any_history":any_count,"with_minimum_history":qualified,"minimum_days":minimum_days}

def store_market_history(row: dict):
    date=str(row.get("date") or "")[:10]
    if not date: return
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""INSERT INTO market_history(date,altcoin_market_cap,total_market_cap,btc_dominance,source,raw_timestamp)
            VALUES(?,?,?,?,?,?) ON CONFLICT(date) DO UPDATE SET
            altcoin_market_cap=excluded.altcoin_market_cap,total_market_cap=excluded.total_market_cap,
            btc_dominance=excluded.btc_dominance,source=excluded.source,raw_timestamp=excluded.raw_timestamp""",
            (date,row.get("altcoin_market_cap"),row.get("total_market_cap"),row.get("btc_dominance"),row.get("source","CoinMarketCap"),row.get("timestamp")))
        conn.commit()

def get_market_history(days: int = 400) -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        rows=conn.execute("""SELECT date,altcoin_market_cap,total_market_cap,btc_dominance,source,raw_timestamp
            FROM market_history ORDER BY date DESC LIMIT ?""",(days,)).fetchall()
    return [{"date":r[0],"altcoin_market_cap":r[1],"total_market_cap":r[2],"btc_dominance":r[3],"source":r[4],"timestamp":r[5]} for r in reversed(rows)]

def store_metric_snapshot(date: str, version: str, payload: dict):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""INSERT INTO metric_snapshot(date,calculation_version,payload_json) VALUES(?,?,?)
            ON CONFLICT(date) DO UPDATE SET calculation_version=excluded.calculation_version,payload_json=excluded.payload_json""",
            (date,version,json.dumps(payload,separators=(",",":"))))
        conn.commit()

def db_summary() -> dict:
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows=conn.execute("SELECT COUNT(*) FROM daily_close").fetchone()[0]
        symbols=conn.execute("SELECT COUNT(DISTINCT symbol) FROM daily_close").fetchone()[0]
        first,last=conn.execute("SELECT MIN(date),MAX(date) FROM daily_close").fetchone()
        market=conn.execute("SELECT COUNT(*) FROM market_history").fetchone()[0]
    return {"path":str(DB_PATH),"daily_close_rows":rows,"symbols":symbols,"first_date":first,"last_date":last,"market_history_rows":market}

init_db()
