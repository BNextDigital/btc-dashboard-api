import sqlite3
import time

import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DB_PATH = DATA_DIR / "oi_history.db"
NORMALIZED_INTERVAL_SECONDS = 15 * 60
MIN_ANALYTICS_HISTORY_SECONDS = 7 * 86400


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
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
    with sqlite3.connect(DB_PATH) as conn:
        # Skip if we already stored a snapshot in the last 10 minutes.
        recent = conn.execute(
            "SELECT timestamp FROM oi_snapshots ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        if recent and (ts - recent[0]) < 600:
            print(
                f"[oi_history] Skipping duplicate — "
                f"last snapshot was {ts - recent[0]}s ago"
            )
            return
        conn.execute(
            "INSERT INTO oi_snapshots (timestamp, oi_usd) VALUES (?, ?)",
            (ts, oi_usd),
        )
        conn.commit()


def get_raw_snapshots(days: int = 35) -> list[dict]:
    """Return the actual persisted OI observations, oldest to newest."""
    cutoff = int(time.time()) - (days * 86400)
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT timestamp, oi_usd FROM oi_snapshots "
            "WHERE timestamp >= ? ORDER BY timestamp ASC",
            (cutoff,),
        ).fetchall()
    return [{"timestamp": r[0], "oi_usd": r[1]} for r in rows]


def _normalize_snapshots(
    snapshots: list[dict],
    interval_seconds: int = NORMALIZED_INTERVAL_SECONDS,
) -> list[dict]:
    """
    Normalize irregular historical observations onto a fixed time grid.

    The original dashboard poller accidentally sampled every four hours while
    downstream OI analytics assumed 15-minute observations. A row-count gate
    therefore treated months of valid history as if 30-day history did not
    exist. Normalizing by elapsed time preserves the old observations while
    making mixed 4h/15m eras comparable.

    Values are forward-filled between real observations. This is deliberate:
    no intra-window OI move is invented, and each observed value represents the
    state known until the next observation arrived. The live/current OI value
    still comes directly from the derivatives feed, not from this series.
    """
    if len(snapshots) < 2:
        return snapshots

    interval = max(60, int(interval_seconds))
    first_ts = int(snapshots[0]["timestamp"])
    last_ts = int(snapshots[-1]["timestamp"])

    grid_start = ((first_ts + interval - 1) // interval) * interval
    grid_end = (last_ts // interval) * interval
    if grid_end < grid_start:
        return snapshots

    normalized: list[dict] = []
    idx = 0
    current = snapshots[0]

    for ts in range(grid_start, grid_end + 1, interval):
        while idx + 1 < len(snapshots) and snapshots[idx + 1]["timestamp"] <= ts:
            idx += 1
            current = snapshots[idx]

        normalized.append({
            "timestamp": ts,
            "oi_usd": current["oi_usd"],
        })

    return normalized


def get_snapshots(days: int = 35) -> list[dict]:
    """
    Return a 15-minute-normalized OI series for analytics compatibility.

    Use get_raw_snapshots() when actual persisted row count/cadence matters.
    """
    return _normalize_snapshots(get_raw_snapshots(days=days))


def get_latest_snapshot() -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT timestamp, oi_usd FROM oi_snapshots "
            "ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
    return {"timestamp": row[0], "oi_usd": row[1]} if row else None


def get_raw_snapshot_count() -> int:
    """Return the number of actual persisted observations."""
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("SELECT COUNT(*) FROM oi_snapshots").fetchone()[0]


def get_snapshot_count() -> int:
    """
    Return an analytics-compatible 15-minute-equivalent history count.

    data_sources.fetch_open_interest() historically uses row-count thresholds:
    48 to enable historical analytics and 2880 to enable the 30-day change.
    The legacy poller actually ran every four hours, so raw row count can never
    represent elapsed coverage correctly. Returning the normalized count makes
    those existing thresholds represent time again without throwing away the
    sparse historical database.

    Do not expose partial history as "real" before seven elapsed days. The old
    48-row gate represented only 12 hours at the intended cadence and could
    otherwise make a short history masquerade as a 7-day comparison.
    """
    raw = get_raw_snapshots(days=35)
    if len(raw) < 2:
        return len(raw)

    coverage_seconds = raw[-1]["timestamp"] - raw[0]["timestamp"]
    if coverage_seconds < MIN_ANALYTICS_HISTORY_SECONDS:
        return min(len(raw), 47)

    return len(_normalize_snapshots(raw))


def get_history_stats(days: int = 90) -> dict:
    """Expose real-row and elapsed-time coverage for diagnostics."""
    raw = get_raw_snapshots(days=days)
    if not raw:
        return {
            "raw_count": 0,
            "analytics_count": 0,
            "normalized_count": 0,
            "coverage_days": 0.0,
            "oldest_timestamp": None,
            "latest_timestamp": None,
        }

    normalized = _normalize_snapshots(raw)
    coverage_days = max(
        0.0,
        (raw[-1]["timestamp"] - raw[0]["timestamp"]) / 86400,
    )
    return {
        "raw_count": len(raw),
        "analytics_count": get_snapshot_count(),
        "normalized_count": len(normalized),
        "coverage_days": round(coverage_days, 2),
        "oldest_timestamp": raw[0]["timestamp"],
        "latest_timestamp": raw[-1]["timestamp"],
    }
