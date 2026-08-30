"""
collector.py — short-lived BTC dashboard analytics collector.

The collector is intentionally disposable:
  1. Import the existing heavy analytics app.
  2. Run an explicit, audited manifest of frontend market-data GET routes.
  3. Write one atomic JSON snapshot.
  4. Store an OI history sample.
  5. Exit so Linux reclaims pandas / NumPy / yFinance memory.

Do not add background pollers here.
"""

from __future__ import annotations

import asyncio
import inspect
import sys
import time
from datetime import datetime, timezone
from typing import Any

from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute

from shared.snapshot_store import load_snapshot, write_snapshot_atomic


# Explicit frontend-facing market-data routes.
#
# Order matters: expensive aggregate endpoints are intentionally called before
# related detail endpoints so their in-process caches can be reused during this
# one collector run.
SNAPSHOT_ROUTES = (
    # ── BTC main dashboard ──────────────────────────────────────────────────
    "/metrics",
    "/price",
    "/summary",
    "/causal",
    "/crypto-proxies",
    "/btc-premium",
    "/news",
    "/etf-aum/metrics",
    "/liquidity/depth",

    # ── Macro / cross-asset dashboards ─────────────────────────────────────
    "/macro/metrics",
    "/liquidity/metrics",
    "/liquidity/yield-curve",
    "/forex/metrics",
    "/growth/metrics",
    "/equity/metrics",
    "/commodities/metrics",
    "/sector-flows/metrics",
    "/leading/all",

    # ── ETF & custody flows ─────────────────────────────────────────────────
    # summary -> breakdown -> custody, so call summary first and let the
    # module-level cache make the following two calls cheap.
    "/etf-flows/summary",
    "/etf-flows/breakdown",
    "/etf-flows/custody",

    # ── Solana dashboard ────────────────────────────────────────────────────
    "/sol/metrics",
    "/sol/price",
    "/sol/summary",
    "/sol/tvl",
    "/sol/ousd-status",

    # ── Ethereum dashboard ──────────────────────────────────────────────────
    "/eth/metrics",
    "/eth/price",
    "/eth/summary",
    "/eth/tvl",
    "/eth/structural",
)


def _route_map(app) -> dict[str, APIRoute]:
    """Return registered GET routes keyed by exact URL path."""
    result: dict[str, APIRoute] = {}

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        if "GET" not in (route.methods or set()):
            continue
        result[route.path] = route

    return result


def _call_kwargs(route: APIRoute) -> dict[str, Any]:
    """
    Resolve optional/default route arguments for direct endpoint invocation.

    The manifest intentionally contains only parameter-free frontend data
    routes, but this keeps the collector safe if one later gains an optional
    FastAPI Query() parameter.
    """
    if "{" in route.path:
        raise RuntimeError("parameterized path is not snapshot-safe")

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
            if hasattr(default, "default"):
                default = default.default
            kwargs[parameter.name] = default
            continue

        raise RuntimeError(
            f"route requires request-specific argument: {parameter.name}"
        )

    return kwargs


async def _invoke(route: APIRoute) -> Any:
    value = route.endpoint(**_call_kwargs(route))
    if inspect.isawaitable(value):
        value = await value
    return jsonable_encoder(value)


def _store_oi_snapshot() -> dict[str, Any]:
    """
    Store one OI observation per collector run.

    Duplicate protection remains inside oi_history, so an early/repeated
    collector invocation does not create duplicate samples.
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
    # Delayed import is intentional: only the disposable child process imports
    # main.py and the analytics dependency tree.
    import main

    started = time.time()
    generated_at = datetime.now(timezone.utc).isoformat()

    routes: dict[str, Any] = {}
    errors: dict[str, str] = {}

    registered = _route_map(main.app)

    missing = [
        path for path in SNAPSHOT_ROUTES
        if path not in registered
    ]

    if missing:
        print(
            "[collector] WARNING — manifest routes not registered: "
            + ", ".join(missing)
        )

    selected = [
        (path, registered[path])
        for path in SNAPSHOT_ROUTES
        if path in registered
    ]

    print(
        f"[collector] collecting {len(selected)}/{len(SNAPSHOT_ROUTES)} "
        "manifest routes"
    )

    # Record missing routes as errors. A previous good value can still be
    # carried forward below.
    for path in missing:
        errors[path] = "RouteNotRegistered: route missing from main.app"

    for path, route in selected:
        route_started = time.time()

        try:
            routes[path] = await _invoke(route)
            elapsed = time.time() - route_started
            print(f"[collector] OK {path} ({elapsed:.2f}s)")
        except Exception as exc:
            errors[path] = f"{type(exc).__name__}: {exc}"
            print(f"[collector] ERROR {path}: {errors[path]}")

    # /metrics is the primary BTC dashboard contract. Never replace a known
    # good snapshot if this route fails completely.
    if "/metrics" not in routes:
        raise RuntimeError(
            "primary /metrics route failed; existing snapshot preserved"
        )

    # Preserve previous good data for any secondary route that failed or was
    # temporarily unavailable. This is especially important on free API tiers
    # where transient 429s are expected.
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
        "schema_version": 2,
        "generated_at": generated_at,
        "generated_unix": finished,
        "duration_seconds": round(finished - started, 3),
        "manifest_route_count": len(SNAPSHOT_ROUTES),
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
        f"({len(routes)}/{len(SNAPSHOT_ROUTES)} routes, "
        f"{len(errors)} errors, "
        f"{len(stale_routes)} stale, "
        f"{snapshot['duration_seconds']:.2f}s)"
    )

    return snapshot


def main() -> int:
    try:
        asyncio.run(collect())
    except Exception as exc:
        print(
            f"[collector] fatal: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
