"""
collector.py — disposable analytics collector with cadence-aware modes.

Modes:
  fast    — 15-minute crypto/derivatives/depth state
  market  — 30-minute cross-asset/yFinance dashboards
  hourly  — 60-minute ETF custody/AUM + news
  slow    — 4-hour FRED/structural indicators
  all     — manual/bootstrap run of every mode

Each run imports the heavy analytics stack, updates only its assigned routes,
atomically merges them into the persisted snapshot, then exits so Linux
reclaims pandas / NumPy / yFinance memory.
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


FAST_ROUTES = (
    # BTC decision state — /metrics must be first because summary/causal reuse it.
    "/metrics",
    "/summary",
    "/causal",
    "/liquidity/depth",

    # Leading indicators whose own source TTL is 15 minutes.
    "/leading/options",
    "/leading/coinbase-premium",
    "/leading/basis-enhanced",

    # SOL — live crypto / DeFi state.
    "/sol/metrics",
    "/sol/price",
    "/sol/summary",
    "/sol/tvl",
    "/sol/ousd-status",

    # ETH — live crypto / DeFi state.
    "/eth/metrics",
    "/eth/price",
    "/eth/summary",
    "/eth/tvl",
    "/eth/structural",

    # Rate-limit-prone extras go last so they cannot block core dashboard state.
    "/btc-premium",
    "/price",
)


MARKET_ROUTES = (
    # Trigger the shared 90-ticker market cache once, then reuse it below.
    # 30 minutes keeps intraday FX/equity context useful while halving the
    # previous 15-minute full-universe Yahoo workload.
    "/macro/metrics",
    "/forex/metrics",
    "/equity/metrics",
    "/commodities/metrics",
    "/sector-flows/metrics",
    "/crypto-proxies",
)


HOURLY_ROUTES = (
    # These sources already describe an hourly cadence in their modules.
    "/etf-aum/metrics",
    "/etf-flows/summary",
    "/etf-flows/breakdown",
    "/etf-flows/custody",
    "/news",
)


SLOW_ROUTES = (
    # FRED series are daily/weekly/monthly; their own modules use 1h–4h TTLs.
    "/liquidity/metrics",
    "/liquidity/yield-curve",
    "/growth/metrics",

    # Slow leading components. Funding's source TTL is 8h; the other sources
    # are daily/weekly/monthly. Four hours is deliberately conservative.
    "/leading/funding-cumulative",
    "/leading/global-m2",
    "/leading/cot",
    "/leading/tether-mints",
    "/leading/breakevens",
)


ROUTE_GROUPS = {
    "fast": FAST_ROUTES,
    "market": MARKET_ROUTES,
    "hourly": HOURLY_ROUTES,
    "slow": SLOW_ROUTES,
}


def _dedupe(*groups: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for group in groups:
        for path in group:
            if path not in seen:
                seen.add(path)
                result.append(path)
    return tuple(result)


ALL_ROUTES = _dedupe(FAST_ROUTES, MARKET_ROUTES, HOURLY_ROUTES, SLOW_ROUTES)


LEADING_COMPONENTS = {
    "options": "/leading/options",
    "coinbase_premium": "/leading/coinbase-premium",
    "funding_cumulative": "/leading/funding-cumulative",
    "global_m2": "/leading/global-m2",
    "cot": "/leading/cot",
    "tether_mints": "/leading/tether-mints",
    "breakevens": "/leading/breakevens",
    "basis_enhanced": "/leading/basis-enhanced",
}


def _add_get_routes(
    result: dict[str, APIRoute],
    source,
    source_name: str,
) -> None:
    count = 0
    for route in getattr(source, "routes", []):
        if not isinstance(route, APIRoute):
            continue
        if "GET" not in (route.methods or set()):
            continue
        result[route.path] = route
        count += 1
    print(f"[collector] route source {source_name}: {count} GET routes")


def _route_map(main_module) -> dict[str, APIRoute]:
    result: dict[str, APIRoute] = {}
    sources = (
        ("main.app", main_module.app),
        ("macro_router", main_module.macro_router),
        ("sector_flows_router", main_module.sector_flows_router),
        ("equity_router", main_module.equity_router),
        ("forex_router", main_module.forex_router),
        ("growth_router", main_module.growth_router),
        ("commodity_router", main_module.commodity_router),
        ("etf_aum_router", main_module.etf_aum_router),
        ("leading_router", main_module.leading_router),
        ("sol_router", main_module.sol_router),
        ("eth_router", main_module.eth_router),
        ("etf_flows_router", main_module.etf_flows_router),
        ("dollar_liquidity_router", main_module.dollar_liquidity_router),
        ("depth_liquidity_router", main_module.depth_liquidity_router),
    )
    for source_name, source in sources:
        _add_get_routes(result, source, source_name)
    return result


def _call_kwargs(route: APIRoute) -> dict[str, Any]:
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
    try:
        from data_sources import _fetch_coingecko_derivatives
        from oi_history import get_snapshot_count, store_snapshot

        markets = _fetch_coingecko_derivatives()
        if not markets:
            return {"stored": False, "reason": "no derivative markets"}

        total_oi = sum(float(m.get("open_interest") or 0) for m in markets)
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


def _synthesize_leading_all(merged_routes: dict[str, Any]) -> None:
    """Build /leading/all from independently refreshed component snapshots."""
    previous_all = merged_routes.get("/leading/all")
    if not isinstance(previous_all, dict):
        previous_all = {}

    payload: dict[str, Any] = {
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    }
    available = 0

    for key, path in LEADING_COMPONENTS.items():
        if path in merged_routes:
            payload[key] = merged_routes[path]
            available += 1
        elif key in previous_all:
            payload[key] = previous_all[key]
            available += 1

    if available:
        merged_routes["/leading/all"] = payload


def _base_snapshot_state() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    previous = load_snapshot()
    if not isinstance(previous, dict):
        previous = {}

    previous_routes = previous.get("routes", {})
    if not isinstance(previous_routes, dict):
        previous_routes = {}

    collections = previous.get("collections", {})
    if not isinstance(collections, dict):
        collections = {}

    return previous, dict(previous_routes), dict(collections)


def _publish_checkpoint(
    *,
    mode: str,
    route_updates: dict[str, Any],
    errors: dict[str, str],
    generated_at: str,
    started: float,
    last_completed_route: str,
) -> None:
    previous, merged_routes, collections = _base_snapshot_state()
    merged_routes.update(route_updates)
    _synthesize_leading_all(merged_routes)

    now = time.time()
    collections[mode] = {
        "generated_at": generated_at,
        "generated_unix": now,
        "duration_seconds": round(now - started, 3),
        "collecting": True,
        "last_completed_route": last_completed_route,
        "error_count": len(errors),
    }

    checkpoint = {
        **previous,
        "schema_version": 4,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_unix": now,
        "route_count": len(merged_routes),
        "errors": errors,
        "collecting": True,
        "partial": True,
        "active_collection": mode,
        "last_completed_route": last_completed_route,
        "collections": collections,
        "routes": merged_routes,
    }

    write_snapshot_atomic(checkpoint)
    print(
        f"[collector:{mode}] checkpoint {last_completed_route} "
        f"({len(merged_routes)} routes available)"
    )


async def collect(mode: str) -> dict[str, Any]:
    import main

    if mode == "all":
        selected_paths = ALL_ROUTES
    else:
        selected_paths = ROUTE_GROUPS[mode]

    started = time.time()
    generated_at = datetime.now(timezone.utc).isoformat()
    route_updates: dict[str, Any] = {}
    errors: dict[str, str] = {}

    print(f"[collector:{mode}] imported main from {getattr(main, '__file__', 'unknown')}")
    registered = _route_map(main)

    missing = [path for path in selected_paths if path not in registered]
    if missing:
        print(
            f"[collector:{mode}] WARNING — routes not registered: "
            + ", ".join(missing)
        )
        for path in missing:
            errors[path] = "RouteNotRegistered: route missing from analytics routers"

    selected = [
        (path, registered[path])
        for path in selected_paths
        if path in registered
    ]

    print(
        f"[collector:{mode}] collecting {len(selected)}/{len(selected_paths)} routes"
    )

    for path, route in selected:
        route_started = time.time()
        try:
            route_updates[path] = await _invoke(route)
            elapsed = time.time() - route_started
            print(f"[collector:{mode}] OK {path} ({elapsed:.2f}s)")
            _publish_checkpoint(
                mode=mode,
                route_updates=route_updates,
                errors=errors,
                generated_at=generated_at,
                started=started,
                last_completed_route=path,
            )
        except Exception as exc:
            errors[path] = f"{type(exc).__name__}: {exc}"
            print(f"[collector:{mode}] ERROR {path}: {errors[path]}")

    if mode in ("fast", "all") and "/metrics" not in route_updates:
        raise RuntimeError("primary /metrics route failed; existing snapshot preserved")

    previous, merged_routes, collections = _base_snapshot_state()

    stale_routes: list[str] = []
    for failed_path in errors:
        if failed_path not in route_updates and failed_path in merged_routes:
            stale_routes.append(failed_path)

    merged_routes.update(route_updates)
    _synthesize_leading_all(merged_routes)

    oi_history = previous.get("oi_history")
    if mode in ("fast", "all"):
        oi_history = _store_oi_snapshot()

    finished = time.time()
    collections[mode] = {
        "generated_at": generated_at,
        "generated_unix": finished,
        "duration_seconds": round(finished - started, 3),
        "collecting": False,
        "last_completed_route": (
            selected[-1][0] if selected else None
        ),
        "route_count": len(route_updates),
        "error_count": len(errors),
        "stale_routes": stale_routes,
    }

    snapshot = {
        **previous,
        "schema_version": 4,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_unix": finished,
        "route_count": len(merged_routes),
        "error_count": len(errors),
        "errors": errors,
        "stale_routes": stale_routes,
        "collecting": False,
        "partial": False,
        "active_collection": None,
        "last_completed_route": (
            selected[-1][0] if selected else None
        ),
        "collections": collections,
        "oi_history": oi_history,
        "routes": merged_routes,
    }

    path = write_snapshot_atomic(snapshot)
    print(
        f"[collector:{mode}] snapshot written to {path} "
        f"({len(route_updates)} updated, {len(merged_routes)} available, "
        f"{len(errors)} errors, {len(stale_routes)} stale, "
        f"{snapshot['collections'][mode]['duration_seconds']:.2f}s)"
    )
    return snapshot


def main() -> int:
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "all"
    if mode not in (*ROUTE_GROUPS.keys(), "all"):
        print(
            f"Unknown collector mode '{mode}'. "
            f"Valid: {', '.join((*ROUTE_GROUPS.keys(), 'all'))}",
            file=sys.stderr,
        )
        return 2

    try:
        asyncio.run(collect(mode))
    except Exception as exc:
        print(
            f"[collector:{mode}] fatal: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
