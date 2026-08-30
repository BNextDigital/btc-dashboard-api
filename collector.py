"""
collector.py — short-lived BTC dashboard analytics collector.

Architecture:
  1. Import the existing heavy FastAPI application and its route modules.
  2. Invoke approved market-data GET endpoints directly in-process.
  3. Persist one complete atomic JSON snapshot.
  4. Store an OI history sample.
  5. Exit.

This process is intentionally disposable. pandas / NumPy / yFinance memory is
fully reclaimed by Linux when the process exits instead of remaining resident
inside the always-on API server.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute

from shared.snapshot_store import load_snapshot, write_snapshot_atomic


# Exact top-level market-data endpoints used by the primary dashboard.
EXACT_PATHS = {
    "/metrics",
    "/summary",
    "/causal",
    "/price",
    "/news",
    "/crypto-proxies",
    "/btc-premium",
}

# Market-data router families. These are snapshot-safe unless they match an
# excluded marker below.
PREFIXES = (
    "/macro",
    "/sector-flows",
    "/equity",
    "/forex",
    "/growth",
    "/commodity",
    "/commodities",
    "/etf-aum",
    "/leading",
    "/sol",
    "/eth",
    "/etf-flows",
    "/etf_flows",
    "/liquidity",
)

# Never execute maintenance, mutation, diagnostics, or parameterized-history
# routes merely because they happen to be GET endpoints.
EXCLUDED_MARKERS = (
    "/cache",
    "/flush",
    "/debug",
    "/backfill",
    "/history",
)

# Core routes first so shared caches are populated once and reused by later
# route builders during this collector process.
PRIORITY = {
    "/metrics": 0,
    "/price": 10,
    "/summary": 20,
    "/causal": 30,
    "/crypto-proxies": 40,
    "/etf-aum/metrics": 50,
}


def _approved_path(path: str) -> bool:
    if path in EXACT_PATHS:
        return True

    if not path.startswith(PREFIXES):
        return False

    return not any(marker in path for marker in EXCLUDED_MARKERS)


def _call_kwargs(route: APIRoute) -> dict[str, Any] | None:
    """
    Build concrete default kwargs for a route endpoint.

    Calling FastAPI endpoint functions directly is safe only when every
    argument can be resolved without request-specific input. FastAPI Query()
    defaults are taken from the route's parsed dependency metadata so the
    endpoint receives the actual Python default rather than a Query object.
    """
    if "{" in route.path:
        return None

    query_defaults = {
        field.name: field.default
        for field in route.dependant.query_params
        if not field.required
    }

    kwargs: dict[str, Any] = {}
    sig = inspect.signature(route.endpoint)

    for parameter in sig.parameters.values():
        if parameter.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        if parameter.name in query_defaults:
            kwargs[parameter.name] = query_defaults[parameter.name]
            continue

        if parameter.default is not inspect.Parameter.empty:
            default = parameter.default
            # FastAPI Param objects expose their underlying default.
            if hasattr(default, "default"):
                default = default.default
            kwargs[parameter.name] = default
            continue

        # Required request/body/dependency argument: not snapshot-safe.
        return None

    return kwargs


async def _invoke(route: APIRoute) -> Any:
    kwargs = _call_kwargs(route)
    if kwargs is None:
        raise RuntimeError("route requires request-specific arguments")

    value = route.endpoint(**kwargs)
    if inspect.isawaitable(value):
        value = await value
    return jsonable_encoder(value)


def _routes_to_collect(app) -> list[APIRoute]:
    candidates: list[APIRoute] = []

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        if "GET" not in (route.methods or set()):
            continue
        if not _approved_path(route.path):
            continue
        if _call_kwargs(route) is None:
            continue
        candidates.append(route)

    candidates.sort(key=lambda r: (PRIORITY.get(r.path, 1000), r.path))
    return candidates


def _store_oi_snapshot() -> dict[str, Any]:
    """
    Preserve the old always-on OI poller's history behavior.

    The collector itself is the scheduled process in the new architecture, so
    each collector run stores one OI observation before exiting.
    """
    try:
        from data_sources import _fetch_coingecko_derivatives
        from oi_history import get_snapshot_count, store_snapshot

        markets = _fetch_coingecko_derivatives()
        if not markets:
            return {"stored": False, "reason": "no derivative markets"}

        total_oi = sum(
            float(m.get("open_interest") or 0)
            for m in markets
        )
        if total_oi <= 0:
            return {"stored": False, "reason": "total OI unavailable"}

        store_snapshot(total_oi)
        return {
            "stored": True,
            "total_oi": total_oi,
            "snapshot_count": get_snapshot_count(),
        }
    except Exception as exc:
        return {
            "stored": False,
            "reason": f"{type(exc).__name__}: {exc}",
        }


async def collect() -> dict[str, Any]:
    # Importing main is intentionally delayed until collector execution so the
    # lightweight serving process never imports the analytics stack.
    import main

    started = time.time()
    generated_at = datetime.now(timezone.utc).isoformat()
    routes: dict[str, Any] = {}
    errors: dict[str, str] = {}

    selected = _routes_to_collect(main.app)
    print(f"[collector] collecting {len(selected)} market-data routes")

    for route in selected:
        route_started = time.time()
        try:
            routes[route.path] = await _invoke(route)
            elapsed = time.time() - route_started
            print(f"[collector] OK {route.path} ({elapsed:.2f}s)")
        except Exception as exc:
            errors[route.path] = f"{type(exc).__name__}: {exc}"
            print(f"[collector] ERROR {route.path}: {errors[route.path]}")

    # /metrics is the primary dashboard contract. If it failed, keep the
    # previously-good snapshot untouched rather than atomically replacing it
    # with a broken one.
    if "/metrics" not in routes:
        raise RuntimeError(
            "primary /metrics route failed; existing snapshot preserved"
        )

    # For non-critical route failures, preserve the last good value for that
    # route when available. The snapshot records which routes are stale.
    stale_routes: list[str] = []
    previous = load_snapshot()
    previous_routes = (
        previous.get("routes", {})
        if isinstance(previous, dict)
        else {}
    )
    if isinstance(previous_routes, dict):
        for failed_path in errors:
            if failed_path not in routes and failed_path in previous_routes:
                routes[failed_path] = previous_routes[failed_path]
                stale_routes.append(failed_path)

    oi_history = _store_oi_snapshot()

    finished = time.time()
    snapshot = {
        "schema_version": 1,
        "generated_at": generated_at,
        "generated_unix": finished,
        "duration_seconds": round(finished - started, 3),
        "route_count": len(routes),
        "error_count": len(errors),
        "errors": errors,
        "stale_routes": stale_routes,
        "oi_history": oi_history,
        "routes": routes,
    }

    path = write_snapshot_atomic(snapshot)
    print(
        f"[collector] snapshot written to {path} "
        f"({len(routes)} routes, {len(errors)} errors, "
        f"{snapshot['duration_seconds']:.2f}s)"
    )

    return snapshot


def main() -> int:
    try:
        snapshot = asyncio.run(collect())
    except Exception as exc:
        print(f"[collector] fatal: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
