"""
shared/memory_utils.py — small process-memory release helper.

Why this exists:
Heavy pandas/yFinance workloads can free their Python objects while glibc keeps
the underlying heap pages resident. Railway bills resident memory (RSS), so
those already-free pages can continue to look like active RAM usage.

release_memory() runs Python GC, then asks glibc malloc_trim(0) to return free
heap pages to Linux. On non-Linux systems it safely falls back to GC only.

Use this only after known allocation-heavy batch operations have finished and
their temporary DataFrames/arrays are no longer referenced.
"""

from __future__ import annotations

import ctypes
import gc
import sys
from pathlib import Path


def _load_malloc_trim():
    """Resolve glibc malloc_trim once at import time; return None if unavailable."""
    if not sys.platform.startswith("linux"):
        return None

    try:
        libc = ctypes.CDLL("libc.so.6")
        trim = libc.malloc_trim
        trim.argtypes = [ctypes.c_size_t]
        trim.restype = ctypes.c_int
        return trim
    except Exception:
        return None


_MALLOC_TRIM = _load_malloc_trim()


def _rss_mb() -> float | None:
    """Return current resident memory from /proc/self/status on Linux."""
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                kb = int(line.split()[1])
                return round(kb / 1024, 2)
    except Exception:
        pass
    return None


def release_memory(reason: str = "") -> dict:
    """
    Collect unreachable Python objects and release free glibc heap pages.

    Returns lightweight diagnostics and never raises because memory cleanup
    must not be allowed to break a market-data refresh.
    """
    before = _rss_mb()

    try:
        collected = gc.collect()
    except Exception:
        collected = 0

    trimmed = False
    if _MALLOC_TRIM is not None:
        try:
            trimmed = bool(_MALLOC_TRIM(0))
        except Exception:
            trimmed = False

    after = _rss_mb()

    released_mb = None
    if before is not None and after is not None:
        released_mb = round(max(before - after, 0.0), 2)

    label = f" {reason}" if reason else ""
    if before is not None and after is not None:
        print(
            f"[memory]{label}: gc={collected}, trim={trimmed}, "
            f"rss={before:.2f}->{after:.2f} MB, released={released_mb:.2f} MB"
        )
    else:
        print(f"[memory]{label}: gc={collected}, trim={trimmed}")

    return {
        "gc_collected": collected,
        "trimmed": trimmed,
        "rss_before_mb": before,
        "rss_after_mb": after,
        "released_mb": released_mb,
    }
