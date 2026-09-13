"""Persistent shared cache for DeFiLlama's read-only JSON endpoints.

ETH and SOL routes run inside a disposable collector process every 15 minutes.
Their in-module caches therefore disappear before the next run, even though
TVL, protocol, DEX, and stablecoin data does not need that cadence. This layer
keeps one atomic cache file per endpoint and serves the last healthy response
when DeFiLlama is temporarily unavailable.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

import requests


DEFAULT_TTL = max(
    900,
    int(os.getenv("DEFILLAMA_CACHE_TTL_SECONDS", "3600")),
)
CACHE_DIR = Path(
    os.getenv(
        "DEFILLAMA_CACHE_DIR",
        str(Path(os.getenv("DATA_DIR", "/app/data")) / "defillama_cache"),
    )
)

_memory: dict[str, dict[str, Any]] = {}
_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"


def _lock_for(key: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(key, threading.Lock())


def _read_disk(key: str) -> dict[str, Any] | None:
    try:
        with _path(key).open("r", encoding="utf-8") as cache_file:
            value = json.load(cache_file)
        return value if isinstance(value, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _write_disk(key: str, value: dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _path(key)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as cache_file:
            json.dump(value, cache_file, ensure_ascii=False, separators=(",", ":"))
            cache_file.flush()
            os.fsync(cache_file.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def get_json(url: str, *, ttl: int = DEFAULT_TTL, timeout: int = 15) -> Any:
    """Return cached JSON for ``url``, refreshing at most once per TTL."""
    key = _key(url)
    now = time.time()

    with _lock_for(key):
        cached = _memory.get(key)
        if cached is None:
            cached = _read_disk(key)
            if cached is not None:
                _memory[key] = cached

        if (
            isinstance(cached, dict)
            and "data" in cached
            and now - float(cached.get("ts", 0)) < ttl
        ):
            return cached["data"]

        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers={"User-Agent": "btc-dashboard/1.0"},
            )
            response.raise_for_status()
            data = response.json()
            entry = {"url": url, "ts": now, "data": data}
            _memory[key] = entry
            _write_disk(key, entry)
            print(f"[defillama_cache] refreshed {url}")
            return data
        except Exception as exc:
            if isinstance(cached, dict) and "data" in cached:
                age = int(now - float(cached.get("ts", 0)))
                print(
                    f"[defillama_cache] refresh failed for {url}: {exc}; "
                    f"returning stale data ({age}s)"
                )
                _memory[key] = {**cached, "ts": now}
                return cached["data"]
            raise
