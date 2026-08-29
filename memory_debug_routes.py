"""
memory_debug_routes.py — temporary process-memory diagnostics for Railway/Linux.

Add to main.py:

    from memory_debug_routes import memory_debug_router
    app.include_router(memory_debug_router)

Railway environment variable:

    MEMORY_DEBUG_TOKEN=<long-random-secret>

Endpoints:

    GET /debug/memory?token=<secret>
    POST /debug/memory/trim?token=<secret>

Remove this router (and the environment variable) after the memory investigation.
No third-party dependencies are required.
"""

from __future__ import annotations

import ctypes
import gc
import os
import platform
import time
import tracemalloc
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query


memory_debug_router = APIRouter(prefix="/debug/memory", tags=["memory-debug"])

# Tracemalloc sees Python-managed allocations. It does NOT see every allocation
# made by NumPy/pandas/yFinance/native libraries or glibc itself.
#
# A modest traceback depth keeps the diagnostic useful without making the
# profiler itself unnecessarily expensive.
if not tracemalloc.is_tracing():
    tracemalloc.start(10)


def _authorize(token: str) -> None:
    expected = os.getenv("MEMORY_DEBUG_TOKEN", "")
    if not expected or token != expected:
        # 404 makes the temporary debug surface less obvious to casual scans.
        raise HTTPException(status_code=404, detail="Not found")


def _kb_to_mb(value: int | None) -> float | None:
    if value is None:
        return None
    return round(value / 1024, 2)


def _proc_status() -> dict:
    """
    Read Linux /proc/self/status.

    Important fields:
      VmRSS   — current resident memory
      VmHWM   — peak resident memory since process start
      VmSize  — virtual address space
      Threads — current OS thread count
    """
    path = Path("/proc/self/status")
    if not path.exists():
        return {
            "available": False,
            "reason": "/proc/self/status unavailable on this platform",
        }

    wanted = {
        "VmRSS": None,
        "VmHWM": None,
        "VmSize": None,
        "VmData": None,
        "VmSwap": None,
        "Threads": None,
    }

    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if ":" not in line:
                continue
            key, raw = line.split(":", 1)
            if key not in wanted:
                continue

            value = raw.strip().split()[0]
            wanted[key] = int(value)

        return {
            "available": True,
            "rss_mb": _kb_to_mb(wanted["VmRSS"]),
            "high_water_mb": _kb_to_mb(wanted["VmHWM"]),
            "virtual_mb": _kb_to_mb(wanted["VmSize"]),
            "data_mb": _kb_to_mb(wanted["VmData"]),
            "swap_mb": _kb_to_mb(wanted["VmSwap"]),
            "threads": wanted["Threads"],
        }
    except Exception as exc:
        return {
            "available": False,
            "reason": str(exc),
        }


def _tracemalloc_stats(limit: int = 15) -> dict:
    current, peak = tracemalloc.get_traced_memory()
    snapshot = tracemalloc.take_snapshot()

    top = []
    for stat in snapshot.statistics("lineno")[:limit]:
        frame = stat.traceback[0]
        top.append(
            {
                "file": frame.filename,
                "line": frame.lineno,
                "size_mb": round(stat.size / (1024 * 1024), 3),
                "allocations": stat.count,
            }
        )

    return {
        "current_mb": round(current / (1024 * 1024), 2),
        "peak_mb": round(peak / (1024 * 1024), 2),
        "top_allocations": top,
    }


def _gc_stats() -> dict:
    return {
        "tracked_objects": len(gc.get_objects()),
        "generation_counts": list(gc.get_count()),
        "generation_stats": gc.get_stats(),
        "thresholds": list(gc.get_threshold()),
    }


def _malloc_trim() -> dict:
    """
    Ask glibc to release free heap pages back to the OS.

    This is diagnostic. If RSS drops materially after this call while
    tracemalloc remains roughly flat, retained glibc/native heap pages are
    likely a meaningful part of the Railway memory bill.
    """
    if platform.system() != "Linux":
        return {
            "supported": False,
            "reason": "malloc_trim diagnostic is Linux/glibc-specific",
        }

    try:
        libc = ctypes.CDLL("libc.so.6")
        trim = libc.malloc_trim
        trim.argtypes = [ctypes.c_size_t]
        trim.restype = ctypes.c_int
        rc = int(trim(0))

        return {
            "supported": True,
            "return_code": rc,
            "released_possible": bool(rc),
        }
    except Exception as exc:
        return {
            "supported": False,
            "reason": str(exc),
        }


def _snapshot() -> dict:
    return {
        "timestamp": time.time(),
        "pid": os.getpid(),
        "platform": platform.platform(),
        "process": _proc_status(),
        "python_allocations": _tracemalloc_stats(),
        "gc": _gc_stats(),
    }


@memory_debug_router.get("")
def memory_snapshot(
    token: str = Query(..., description="Temporary MEMORY_DEBUG_TOKEN"),
):
    """
    Inspect current process memory without modifying allocator state.
    """
    _authorize(token)
    return _snapshot()


@memory_debug_router.post("/trim")
def memory_trim(
    token: str = Query(..., description="Temporary MEMORY_DEBUG_TOKEN"),
):
    """
    Force Python GC, then ask glibc to release unused heap pages.

    Compare process.rss_mb before and after.
    """
    _authorize(token)

    before = _snapshot()

    collected = gc.collect()
    trim_result = _malloc_trim()

    after = _snapshot()

    before_rss = before.get("process", {}).get("rss_mb")
    after_rss = after.get("process", {}).get("rss_mb")

    rss_drop = None
    if isinstance(before_rss, (int, float)) and isinstance(after_rss, (int, float)):
        rss_drop = round(before_rss - after_rss, 2)

    return {
        "gc_collected": collected,
        "malloc_trim": trim_result,
        "rss_drop_mb": rss_drop,
        "before": before,
        "after": after,
    }
