import sqlite3
import time
from datetime import datetime, timezone

DB_FILE = "oi_history.db"


def init_db() -> None:
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS oi_snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                oi_usd    REAL    NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp
            ON oi_snapshots (timestamp)
        """)
        conn.commit()


def store_snapshot(oi_usd: float) -> None:
    ts = int(time.time())
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            "INSERT INTO oi_snapshots (timestamp, oi_usd) VALUES (?, ?)",
            (ts, oi_usd)
        )
        conn.commit()


def get_snapshots(days: int = 35) -> list[dict]:
    cutoff = int(time.time()) - (days * 86400)
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute(
            "SELECT timestamp, oi_usd FROM oi_snapshots WHERE timestamp >= ? ORDER BY timestamp ASC",
            (cutoff,)
        ).fetchall()
    return [{"timestamp": r[0], "oi_usd": r[1]} for r in rows]


def get_latest_snapshot() -> dict | None:
    with sqlite3.connect(DB_FILE) as conn:
        row = conn.execute(
            "SELECT timestamp, oi_usd FROM oi_snapshots ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
    return {"timestamp": row[0], "oi_usd": row[1]} if row else None


def get_snapshot_count() -> int:
    with sqlite3.connect(DB_FILE) as conn:
        return conn.execute("SELECT COUNT(*) FROM oi_snapshots").fetchone()[0]


def prune_old_snapshots(keep_days: int = 90) -> None:
    cutoff = int(time.time()) - (keep_days * 86400)
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM oi_snapshots WHERE timestamp < ?", (cutoff,))
        conn.commit()