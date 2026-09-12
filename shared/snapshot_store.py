"""
shared/snapshot_store.py — atomic market snapshot persistence.

The collector writes one complete JSON document to a temporary file and then
os.replace() swaps it into place. Readers therefore see either the previous
complete snapshot or the new complete snapshot, never a partially-written file.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any


DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
SNAPSHOT_PATH = Path(
    os.getenv("SNAPSHOT_PATH", str(DATA_DIR / "latest_snapshot.json"))
)

_snapshot_cache_lock = threading.Lock()
_snapshot_cache: dict[str, Any] = {
    "mtime_ns": None,
    "size": None,
    "data": None,
}


def snapshot_path() -> Path:
    return SNAPSHOT_PATH


def snapshot_exists() -> bool:
    return SNAPSHOT_PATH.is_file()


def snapshot_age_seconds() -> float | None:
    try:
        return max(0.0, time.time() - SNAPSHOT_PATH.stat().st_mtime)
    except OSError:
        return None


def load_snapshot() -> dict[str, Any] | None:
    try:
        stat = SNAPSHOT_PATH.stat()
    except OSError:
        return None

    with _snapshot_cache_lock:
        if (
            _snapshot_cache["data"] is not None
            and _snapshot_cache["mtime_ns"] == stat.st_mtime_ns
            and _snapshot_cache["size"] == stat.st_size
        ):
            return _snapshot_cache["data"]

        try:
            with SNAPSHOT_PATH.open("r", encoding="utf-8") as snapshot_file:
                data = json.load(snapshot_file)
        except (OSError, json.JSONDecodeError):
            return None

        if not isinstance(data, dict):
            return None

        _snapshot_cache.update({
            "mtime_ns": stat.st_mtime_ns,
            "size": stat.st_size,
            "data": data,
        })
        return data


def get_snapshot_route(path: str) -> Any:
    data = load_snapshot()
    if not data:
        return None
    routes = data.get("routes")
    if not isinstance(routes, dict):
        return None
    return routes.get(path)


def write_snapshot_atomic(data: dict[str, Any]) -> Path:
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = SNAPSHOT_PATH.with_name(
        f".{SNAPSHOT_PATH.name}.{os.getpid()}.tmp"
    )

    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(
                data,
                f,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, SNAPSHOT_PATH)

        stat = SNAPSHOT_PATH.stat()
        with _snapshot_cache_lock:
            _snapshot_cache.update({
                "mtime_ns": stat.st_mtime_ns,
                "size": stat.st_size,
                "data": data,
            })

        # Best-effort directory fsync so the rename is durable on Linux.
        try:
            dir_fd = os.open(str(SNAPSHOT_PATH.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

        return SNAPSHOT_PATH
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
