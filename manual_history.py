"""
manual_history.py — SQLite storage for manually overridden metric history.

Stores one row per metric per date. Used for:
- Percentile calculations from real historical data
- SEM analytics (correlate market state with trade outcomes)
- Backfilling past data from screenshots
"""

import sqlite3
import time
from datetime import datetime, timezone

DB_FILE = "manual_history.db"

METRICS = [
    "exchange_netflow",
    "lth_supply",
    "etf_flow",
    "realized_cap",
    "funding",
    "open_interest",
]


def init_db() -> None:
    """Create tables if they don't exist."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS metric_history (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                metric       TEXT    NOT NULL,
                date         TEXT    NOT NULL,  -- YYYY-MM-DD baseline date
                current      TEXT,              -- formatted display value
                d7           TEXT,
                vs30d        TEXT,
                percentile   INTEGER,
                alert        TEXT,
                pattern      TEXT,
                source       TEXT,              -- e.g. "CryptoQuant · tooltip exact"
                notes        TEXT,
                raw_value    REAL,              -- numeric value for analytics
                raw_unit     TEXT,              -- BTC / USD / % etc.
                created_at   INTEGER NOT NULL,  -- Unix timestamp
                updated_at   INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_metric_date
            ON metric_history (metric, date)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_metric
            ON metric_history (metric)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_date
            ON metric_history (date)
        """)
        conn.commit()


def upsert_metric(
    metric:     str,
    date:       str,
    current:    str,
    d7:         str,
    vs30d:      str,
    percentile: int,
    alert:      str,
    pattern:    str,
    source:     str  = "",
    notes:      str  = "",
    raw_value:  float | None = None,
    raw_unit:   str  = "",
) -> None:
    """
    Insert or update a metric entry for a given date.
    If a row for (metric, date) already exists, updates all fields.
    """
    now = int(time.time())
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            INSERT INTO metric_history
                (metric, date, current, d7, vs30d, percentile, alert, pattern,
                 source, notes, raw_value, raw_unit, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(metric, date) DO UPDATE SET
                current    = excluded.current,
                d7         = excluded.d7,
                vs30d      = excluded.vs30d,
                percentile = excluded.percentile,
                alert      = excluded.alert,
                pattern    = excluded.pattern,
                source     = excluded.source,
                notes      = excluded.notes,
                raw_value  = excluded.raw_value,
                raw_unit   = excluded.raw_unit,
                updated_at = excluded.updated_at
        """, (
            metric, date, current, d7, vs30d, percentile, alert, pattern,
            source, notes, raw_value, raw_unit, now, now
        ))
        conn.commit()


def get_history(metric: str, days: int = 90) -> list[dict]:
    """Returns history for a metric, most recent first."""
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("""
            SELECT date, current, d7, vs30d, percentile, alert, pattern,
                   source, notes, raw_value, raw_unit, updated_at
            FROM metric_history
            WHERE metric = ?
            ORDER BY date DESC
            LIMIT ?
        """, (metric, days)).fetchall()

    return [
        {
            "date":       r[0],
            "current":    r[1],
            "d7":         r[2],
            "vs30d":      r[3],
            "percentile": r[4],
            "alert":      r[5],
            "pattern":    r[6],
            "source":     r[7],
            "notes":      r[8],
            "raw_value":  r[9],
            "raw_unit":   r[10],
            "updated_at": r[11],
        }
        for r in rows
    ]


def get_entry(metric: str, date: str) -> dict | None:
    """Returns a single entry for a metric on a specific date."""
    with sqlite3.connect(DB_FILE) as conn:
        row = conn.execute("""
            SELECT date, current, d7, vs30d, percentile, alert, pattern,
                   source, notes, raw_value, raw_unit, updated_at
            FROM metric_history
            WHERE metric = ? AND date = ?
        """, (metric, date)).fetchone()

    if not row:
        return None

    return {
        "date":       row[0],
        "current":    row[1],
        "d7":         row[2],
        "vs30d":      row[3],
        "percentile": row[4],
        "alert":      row[5],
        "pattern":    row[6],
        "source":     row[7],
        "notes":      row[8],
        "raw_value":  row[9],
        "raw_unit":   row[10],
        "updated_at": row[11],
    }


def get_all_dates(metric: str) -> list[str]:
    """Returns all dates with data for a metric, most recent first."""
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("""
            SELECT date FROM metric_history
            WHERE metric = ?
            ORDER BY date DESC
        """, (metric,)).fetchall()
    return [r[0] for r in rows]


def get_percentile_from_history(metric: str, raw_value: float, window_days: int = 90) -> float | None:
    """
    Computes where raw_value ranks within the last window_days of history.
    Returns percentile 0-100, or None if not enough data.
    """
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("""
            SELECT raw_value FROM metric_history
            WHERE metric = ? AND raw_value IS NOT NULL
            ORDER BY date DESC
            LIMIT ?
        """, (metric, window_days)).fetchall()

    values = [r[0] for r in rows]
    if len(values) < 5:
        return None

    rank = sum(1 for v in values if v <= raw_value)
    return (rank / len(values)) * 100


def get_summary_stats(metric: str, window_days: int = 30) -> dict:
    """Returns summary statistics for a metric over a window."""
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("""
            SELECT date, percentile, alert, raw_value
            FROM metric_history
            WHERE metric = ?
            ORDER BY date DESC
            LIMIT ?
        """, (metric, window_days)).fetchall()

    if not rows:
        return {"count": 0}

    percentiles = [r[1] for r in rows if r[1] is not None]
    alerts      = [r[2] for r in rows if r[2] and r[2] != "—"]

    from collections import Counter
    alert_counts = Counter(alerts).most_common(3)

    return {
        "count":          len(rows),
        "date_range":     f"{rows[-1][0]} → {rows[0][0]}",
        "avg_percentile": round(sum(percentiles) / len(percentiles), 1) if percentiles else None,
        "top_alerts":     [{"alert": a, "count": c} for a, c in alert_counts],
    }


def get_row_count() -> dict:
    """Returns total row count per metric."""
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("""
            SELECT metric, COUNT(*) FROM metric_history
            GROUP BY metric ORDER BY metric
        """).fetchall()
    return {r[0]: r[1] for r in rows}