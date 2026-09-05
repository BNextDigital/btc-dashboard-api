"""
sol_routes.py — Solana Dashboard Backend Routes

Designed for the Phase-3 disposable collector architecture.

Key rules:
- No direct yFinance calls from this module.
- CME SOL futures come from shared.yf_core_cache ("sol_futures"), warmed once
  alongside BTC and ETH futures.
- SOL spot / market data comes from CoinGecko.
- SOL funding + OI reuse shared.cg_cache /derivatives.
- SOL dominance reuses shared.cg_cache /global.
- CoinGecko source failures are isolated so one failed request does not blank
  unrelated SOL cards.
- Volume uses CoinGecko market_chart.total_volumes, not OHLC closes.
- Stablecoin data uses DefiLlama's dedicated stablecoins API host.
- SOL OI/funding history persists across disposable collector runs.
- Solana staking uses live RPC vote-account stake and live circulating supply.
- No synthetic fallback values are invented when a source is unavailable.

Collector routes:
    /sol/metrics
    /sol/price
    /sol/summary
    /sol/tvl
    /sol/ousd-status
"""

from __future__ import annotations

import calendar
import json
import os
import sqlite3
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from fastapi import APIRouter
from pydantic import BaseModel

from shared.cg_cache import (
    cg_request as _cg_shared,
    get_global as _cg_global,
    get_weighted_funding_oi as _cg_derivs,
)
from shared.yf_core_cache import get_series as _yf_core_series


# ── Config ────────────────────────────────────────────────────────────────────

sol_router = APIRouter(prefix="/sol", tags=["Solana"])

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

SOL_DB_PATH = DATA_DIR / "sol_history.db"
SOL_TVL_DB_PATH = DATA_DIR / "sol_tvl_history.db"
SOL_OVERRIDES = DATA_DIR / "sol_overrides.json"

DEFILLAMA_BASE = "https://api.llama.fi"
DEFILLAMA_STABLES_BASE = "https://stablecoins.llama.fi"

SOL_RPC_ENDPOINTS = [
    ("Solana Foundation", "https://api.mainnet-beta.solana.com"),
    ("PublicNode", "https://solana-rpc.publicnode.com"),
]

SOURCE_CACHE_TTL = 5 * 60
METRIC_CACHE_TTL = 5 * 60
DERIV_HISTORY_RETENTION_DAYS = 35
DERIV_HISTORY_MIN_WRITE_SECONDS = 10 * 60


# ── Database init ─────────────────────────────────────────────────────────────

def _init_dbs() -> None:
    with sqlite3.connect(SOL_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sol_basis_history (
                date TEXT PRIMARY KEY,
                basis_pct REAL,
                spot_price REAL,
                futures_price REAL,
                days_to_expiry INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sol_dominance_history (
                date TEXT PRIMARY KEY,
                dominance_pct REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sol_stablecoin_history (
                date TEXT PRIMARY KEY,
                total_usd REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sol_derivatives_history (
                timestamp INTEGER PRIMARY KEY,
                open_interest_usd REAL,
                funding_rate REAL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sol_derivatives_timestamp
            ON sol_derivatives_history (timestamp)
            """
        )
        conn.commit()

    with sqlite3.connect(SOL_TVL_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sol_tvl_history (
                date TEXT PRIMARY KEY,
                tvl_usd REAL,
                dex_volume_24h REAL
            )
            """
        )
        conn.commit()


_init_dbs()


# ── Generic helpers ───────────────────────────────────────────────────────────

def _safe_float(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _percentile(
    value: Optional[float],
    series: list[float],
) -> float:
    clean = [
        float(item)
        for item in series
        if item is not None
    ]

    if value is None or not clean:
        return 50.0

    return round(
        sum(1 for item in clean if item <= value)
        / len(clean)
        * 100,
        1,
    )


def _fmt_usd(
    value: Optional[float],
    signed: bool = False,
) -> str:
    if value is None:
        return "—"

    sign = ""
    if signed:
        sign = "+" if value >= 0 else "-"

    abs_value = abs(value)

    if abs_value >= 1e12:
        body = f"${abs_value / 1e12:.2f}T"
    elif abs_value >= 1e9:
        body = f"${abs_value / 1e9:.1f}B"
    elif abs_value >= 1e6:
        body = f"${abs_value / 1e6:.0f}M"
    elif abs_value >= 1e3:
        body = f"${abs_value / 1e3:.0f}k"
    else:
        body = f"${abs_value:,.0f}"

    if signed:
        return sign + body

    return ("-" if value < 0 else "") + body


def _unavailable(
    metric_id: str,
    source: str,
    error: str = "",
) -> dict:
    """
    Explicit no-data state.

    `_mock` remains for compatibility with the existing SOL frontend footer,
    but no synthetic value is supplied.
    """
    result = {
        "current": "—",
        "d7": "—",
        "vs30d": "—",
        "percentile": 50,
        "alert": "No data",
        "level": "none",
        "pattern": f"{metric_id}: source unavailable",
        "source": source,
        "_mock": True,
    }

    if error:
        result["source_error"] = error

    return result


def _last_friday(year: int, month: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    candidate = date(year, month, last_day)

    while candidate.weekday() != calendar.FRIDAY:
        candidate = candidate.replace(
            day=candidate.day - 1
        )

    return candidate


def _next_cme_sol_expiry(
    today: Optional[date] = None,
) -> date:
    """
    Regular CME SOL futures expire on the last Friday of each contract month.
    """
    today = today or datetime.now(timezone.utc).date()

    expiry = _last_friday(
        today.year,
        today.month,
    )

    if today <= expiry:
        return expiry

    if today.month == 12:
        year = today.year + 1
        month = 1
    else:
        year = today.year
        month = today.month + 1

    return _last_friday(year, month)


def _sum_nested_numeric(value) -> Optional[float]:
    """
    DefiLlama stablecoin values are commonly nested by peg type:
        {"peggedUSD": 123, "peggedEUR": 4, ...}

    Those fields are already expressed in USD-equivalent totals.
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, dict):
        values = [
            number
            for number in (
                _safe_float(item)
                for item in value.values()
            )
            if number is not None
        ]
        return sum(values) if values else None

    return None


# ── CoinGecko helpers ─────────────────────────────────────────────────────────

def _cg(
    path: str,
    params: Optional[dict] = None,
):
    return _cg_shared(path, params)


def fetch_sol_market() -> dict:
    data = _cg(
        "/coins/solana",
        {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        },
    )

    if not isinstance(data, dict):
        return {}

    market_data = data.get("market_data")
    if not isinstance(market_data, dict):
        return {}

    return {
        "price_usd": _safe_float(
            market_data.get(
                "current_price",
                {},
            ).get("usd")
        ),
        "change_24h": _safe_float(
            market_data.get(
                "price_change_percentage_24h"
            )
        ),
        "change_7d": _safe_float(
            market_data.get(
                "price_change_percentage_7d"
            )
        ),
        "change_30d": _safe_float(
            market_data.get(
                "price_change_percentage_30d"
            )
        ),
        "volume_24h": _safe_float(
            market_data.get(
                "total_volume",
                {},
            ).get("usd")
        ),
        "market_cap": _safe_float(
            market_data.get(
                "market_cap",
                {},
            ).get("usd")
        ),
        "circulating_supply": _safe_float(
            market_data.get(
                "circulating_supply"
            )
        ),
        "ath": _safe_float(
            market_data.get(
                "ath",
                {},
            ).get("usd")
        ),
        "ath_change_pct": _safe_float(
            market_data.get(
                "ath_change_percentage",
                {},
            ).get("usd")
        ),
        "source": "CoinGecko",
    }


def fetch_sol_market_chart(
    days: int = 30,
) -> dict:
    """
    Real historical price and volume series.

    The old route used OHLC closing prices as if they were historical volume.
    """
    data = _cg(
        "/coins/solana/market_chart",
        {
            "vs_currency": "usd",
            "days": str(days),
            "interval": "daily",
        },
    )

    if not isinstance(data, dict):
        return {
            "prices": [],
            "volumes": [],
            "market_caps": [],
        }

    prices: list[float] = []
    volumes: list[float] = []
    market_caps: list[float] = []

    for row in data.get("prices", []) or []:
        if (
            isinstance(row, (list, tuple))
            and len(row) >= 2
        ):
            value = _safe_float(row[1])
            if value is not None:
                prices.append(value)

    for row in data.get("total_volumes", []) or []:
        if (
            isinstance(row, (list, tuple))
            and len(row) >= 2
        ):
            value = _safe_float(row[1])
            if value is not None:
                volumes.append(value)

    for row in data.get("market_caps", []) or []:
        if (
            isinstance(row, (list, tuple))
            and len(row) >= 2
        ):
            value = _safe_float(row[1])
            if value is not None:
                market_caps.append(value)

    return {
        "prices": prices,
        "volumes": volumes,
        "market_caps": market_caps,
        "source": "CoinGecko market_chart",
    }


def fetch_sol_derivatives() -> dict:
    result = _cg_derivs("SOL")

    return (
        result
        if isinstance(result, dict)
        else {}
    )


def fetch_sol_dominance() -> dict:
    """
    Reuse shared CoinGecko /global cache instead of issuing another /global call.
    """
    try:
        global_data = _cg_global()

        if not isinstance(global_data, dict):
            return {
                "dominance_pct": None,
                "source": "CoinGecko global",
                "error": "unexpected response",
            }

        dominance = _safe_float(
            global_data.get(
                "market_cap_percentage",
                {},
            ).get("sol")
        )

        return {
            "dominance_pct": dominance,
            "source": "CoinGecko global",
        }

    except Exception as exc:
        return {
            "dominance_pct": None,
            "source": "CoinGecko global",
            "error": str(exc),
        }


# ── DeFiLlama helpers ─────────────────────────────────────────────────────────

def fetch_sol_tvl() -> dict:
    try:
        response = requests.get(
            f"{DEFILLAMA_BASE}/v2/historicalChainTvl/Solana",
            timeout=15,
        )
        response.raise_for_status()

        history = response.json()

        if not isinstance(history, list) or not history:
            return {
                "tvl_usd": None,
                "source": "DeFiLlama",
                "error": "empty history",
            }

        history = sorted(
            history,
            key=lambda row: row.get("date", 0),
        )

        current = _safe_float(
            history[-1].get("tvl")
        )
        d7_ago = (
            _safe_float(
                history[-8].get("tvl")
            )
            if len(history) >= 8
            else None
        )
        d30_ago = (
            _safe_float(
                history[-31].get("tvl")
            )
            if len(history) >= 31
            else None
        )

        last_90 = [
            value
            for value in (
                _safe_float(row.get("tvl"))
                for row in history[-90:]
            )
            if value is not None
        ]

        return {
            "tvl_usd": current,
            "tvl_7d_ago": d7_ago,
            "tvl_30d_ago": d30_ago,
            "percentile": _percentile(
                current,
                last_90,
            ),
            "history_30d": last_90[-30:],
            "source": "DeFiLlama",
        }

    except Exception as exc:
        return {
            "tvl_usd": None,
            "source": "DeFiLlama",
            "error": str(exc),
        }


def fetch_sol_protocol_breakdown() -> list[dict]:
    try:
        response = requests.get(
            f"{DEFILLAMA_BASE}/protocols",
            timeout=15,
        )
        response.raise_for_status()

        protocols = response.json()

        if not isinstance(protocols, list):
            return []

        solana = [
            protocol
            for protocol in protocols
            if "Solana"
            in (protocol.get("chains") or [])
        ]

        solana.sort(
            key=lambda protocol: (
                _safe_float(
                    protocol.get("tvl")
                )
                or 0.0
            ),
            reverse=True,
        )

        return [
            {
                "name": protocol.get(
                    "name",
                    "Unknown",
                ),
                "tvl": _safe_float(
                    protocol.get("tvl")
                ),
                "category": (
                    protocol.get("category")
                    or "—"
                ),
            }
            for protocol in solana[:10]
        ]

    except Exception as exc:
        print(
            f"[sol] protocol breakdown failed: {exc}"
        )
        return []


def fetch_sol_dex_volume() -> dict:
    try:
        response = requests.get(
            f"{DEFILLAMA_BASE}/overview/dexs/Solana",
            timeout=15,
        )
        response.raise_for_status()

        data = response.json()

        if not isinstance(data, dict):
            return {
                "dex_volume_24h": None,
                "dex_volume_7d": None,
                "source": "DeFiLlama",
            }

        return {
            "dex_volume_24h": _safe_float(
                data.get("total24h")
            ),
            "dex_volume_7d": _safe_float(
                data.get("total7d")
            ),
            "source": "DeFiLlama",
        }

    except Exception as exc:
        return {
            "dex_volume_24h": None,
            "dex_volume_7d": None,
            "source": "DeFiLlama",
            "error": str(exc),
        }


def fetch_sol_stablecoin_supply() -> dict:
    """
    Current and historical Solana stablecoin market cap.

    Stablecoins use their own DefiLlama API host:
        https://stablecoins.llama.fi
    """
    current = None
    history_values: list[float] = []
    error_parts = []

    try:
        response = requests.get(
            f"{DEFILLAMA_STABLES_BASE}/stablecoinchains",
            timeout=15,
        )
        response.raise_for_status()

        chains = response.json()

        if isinstance(chains, list):
            solana = next(
                (
                    chain
                    for chain in chains
                    if str(
                        chain.get("name", "")
                    ).lower()
                    == "solana"
                ),
                None,
            )

            if solana:
                current = _sum_nested_numeric(
                    solana.get(
                        "totalCirculatingUSD"
                    )
                )

    except Exception as exc:
        error_parts.append(
            f"current: {exc}"
        )

    try:
        response = requests.get(
            f"{DEFILLAMA_STABLES_BASE}/stablecoincharts/Solana",
            timeout=15,
        )
        response.raise_for_status()

        history = response.json()

        if isinstance(history, list):
            for row in history[-90:]:
                if not isinstance(row, dict):
                    continue

                value = _sum_nested_numeric(
                    row.get(
                        "totalCirculatingUSD"
                    )
                )

                if (
                    value is not None
                    and value > 0
                ):
                    history_values.append(
                        value
                    )

    except Exception as exc:
        error_parts.append(
            f"history: {exc}"
        )

    if (
        current is None
        and history_values
    ):
        current = history_values[-1]

    d7_ago = (
        history_values[-8]
        if len(history_values) >= 8
        else None
    )
    d30_ago = (
        history_values[-31]
        if len(history_values) >= 31
        else None
    )

    result = {
        "total_usd": current,
        "total_7d_ago": d7_ago,
        "total_30d_ago": d30_ago,
        "percentile": _percentile(
            current,
            history_values,
        ),
        "history_90d": history_values,
        "source": "DeFiLlama stablecoins",
    }

    if error_parts:
        result["error"] = "; ".join(
            error_parts
        )

    return result


# ── Solana staking ────────────────────────────────────────────────────────────

def _sol_rpc_call(
    method: str,
    params: Optional[list] = None,
) -> dict:
    errors = []

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or [],
    }

    for label, endpoint in SOL_RPC_ENDPOINTS:
        try:
            response = requests.post(
                endpoint,
                json=payload,
                headers={
                    "Content-Type": "application/json"
                },
                timeout=15,
            )
            response.raise_for_status()

            data = response.json()

            if data.get("error"):
                raise ValueError(
                    str(data["error"])
                )

            if "result" not in data:
                raise ValueError(
                    "missing RPC result"
                )

            return {
                "result": data["result"],
                "source": label,
            }

        except Exception as exc:
            errors.append(
                f"{label}: {exc}"
            )

    return {
        "result": None,
        "source": "Solana RPC",
        "error": "; ".join(errors),
    }


def fetch_sol_staking() -> dict:
    """
    Active/delinquent vote accounts and staking rate.

    The old route used a hard-coded ~580M SOL supply and returned fake
    64.8% / 1,463 values when RPC failed. This uses getSupply for live
    circulating supply and returns unavailable on failure.
    """
    votes = _sol_rpc_call(
        "getVoteAccounts",
        [{"commitment": "confirmed"}],
    )

    supply = _sol_rpc_call(
        "getSupply",
        [{"commitment": "confirmed"}],
    )

    if votes.get("result") is None:
        return {
            "active_validators": None,
            "delinquent_validators": None,
            "active_stake_sol": None,
            "staking_rate_pct": None,
            "top5_concentration_pct": None,
            "source": votes.get(
                "source",
                "Solana RPC",
            ),
            "error": votes.get(
                "error",
                "vote accounts unavailable",
            ),
        }

    vote_data = votes["result"]

    if not isinstance(vote_data, dict):
        return {
            "active_validators": None,
            "delinquent_validators": None,
            "active_stake_sol": None,
            "staking_rate_pct": None,
            "top5_concentration_pct": None,
            "source": votes.get(
                "source",
                "Solana RPC",
            ),
            "error": "unexpected vote-account response",
        }

    current = vote_data.get(
        "current",
        [],
    ) or []
    delinquent = vote_data.get(
        "delinquent",
        [],
    ) or []

    active_stake_lamports = sum(
        int(
            validator.get(
                "activatedStake",
                0,
            )
            or 0
        )
        for validator in current
    )

    total_stake_lamports = (
        active_stake_lamports
        + sum(
            int(
                validator.get(
                    "activatedStake",
                    0,
                )
                or 0
            )
            for validator in delinquent
        )
    )

    circulating_lamports = None

    supply_result = supply.get("result")
    if isinstance(supply_result, dict):
        value = supply_result.get("value")
        if isinstance(value, dict):
            circulating_lamports = _safe_float(
                value.get("circulating")
            )

    staking_rate = None

    if (
        circulating_lamports is not None
        and circulating_lamports > 0
    ):
        staking_rate = (
            active_stake_lamports
            / circulating_lamports
            * 100
        )

    top5 = sorted(
        current,
        key=lambda validator: int(
            validator.get(
                "activatedStake",
                0,
            )
            or 0
        ),
        reverse=True,
    )[:5]

    top5_stake = sum(
        int(
            validator.get(
                "activatedStake",
                0,
            )
            or 0
        )
        for validator in top5
    )

    top5_pct = (
        top5_stake
        / total_stake_lamports
        * 100
        if total_stake_lamports > 0
        else None
    )

    result = {
        "active_validators": len(current),
        "delinquent_validators": len(
            delinquent
        ),
        "active_stake_sol": round(
            active_stake_lamports / 1e9,
            0,
        ),
        "staking_rate_pct": (
            round(staking_rate, 2)
            if staking_rate is not None
            else None
        ),
        "top5_concentration_pct": (
            round(top5_pct, 1)
            if top5_pct is not None
            else None
        ),
        "circulating_supply_sol": (
            round(
                circulating_lamports / 1e9,
                0,
            )
            if circulating_lamports
            else None
        ),
        "source": votes.get(
            "source",
            "Solana RPC",
        ),
    }

    if (
        circulating_lamports is None
        and supply.get("error")
    ):
        result["supply_error"] = (
            supply["error"]
        )

    return result


# ── CME SOL basis — shared Yahoo core cache ───────────────────────────────────

def fetch_sol_cme_basis(
    spot_price: Optional[float],
) -> dict:
    """
    SOL spot comes from CoinGecko.
    SOL futures come from shared.yf_core_cache ("sol_futures").

    No yFinance network call is made here.
    """
    if (
        spot_price is None
        or spot_price <= 0
    ):
        return {
            "basis_pct": None,
            "source": (
                "Yahoo Finance SOL=F · shared core cache"
            ),
            "error": "SOL spot price unavailable",
        }

    try:
        futures = _yf_core_series(
            "sol_futures"
        )

        if (
            futures is None
            or len(futures) == 0
        ):
            raise ValueError(
                "SOL=F unavailable from core cache"
            )

        futures_price = float(
            futures.iloc[-1]
        )
        futures_date = str(
            futures.index[-1].date()
        )

        if futures_price <= 0:
            raise ValueError(
                "invalid SOL=F futures price"
            )

        expiry = _next_cme_sol_expiry()
        today = datetime.now(
            timezone.utc
        ).date()

        days_left = max(
            (expiry - today).days,
            1,
        )

        raw_basis_pct = (
            (futures_price / spot_price)
            - 1
        ) * 100

        annualized_basis_pct = (
            raw_basis_pct
            * 365
            / days_left
        )

        price_ratio = (
            futures_price / spot_price
        )

        if (
            price_ratio < 0.75
            or price_ratio > 1.25
            or annualized_basis_pct < -150
            or annualized_basis_pct > 200
        ):
            raise ValueError(
                "SOL=F/spot relationship failed sanity gate "
                f"(spot={spot_price:.2f}, "
                f"futures={futures_price:.2f}, "
                f"annualized={annualized_basis_pct:.1f}%)"
            )

        return {
            "basis_pct": annualized_basis_pct,
            "raw_basis_pct": raw_basis_pct,
            "spot_price": spot_price,
            "futures_price": futures_price,
            "futures_date": futures_date,
            "days_to_expiry": days_left,
            "expiry_date": expiry.isoformat(),
            "source": (
                "Yahoo Finance SOL=F · shared core cache"
            ),
        }

    except Exception as exc:
        return {
            "basis_pct": None,
            "source": (
                "Yahoo Finance SOL=F · shared core cache"
            ),
            "error": str(exc),
        }


# ── Per-process source cache ──────────────────────────────────────────────────

_source_cache: dict[str, dict] = {}
_metric_cache = {
    "data": None,
    "ts": 0.0,
}


def _cached_source(
    key: str,
    fetcher,
    ttl: int = SOURCE_CACHE_TTL,
):
    now = time.time()
    cached = _source_cache.get(key)

    if (
        cached
        and (now - cached["ts"]) < ttl
    ):
        return cached["data"]

    data = fetcher()

    _source_cache[key] = {
        "data": data,
        "ts": now,
    }

    return data


def _get_market_bundle() -> dict:
    """
    CoinGecko components are isolated from one another.

    A market-chart failure does not erase current price; a derivatives failure
    does not erase either.
    """
    result = {
        "market": {},
        "chart": {
            "prices": [],
            "volumes": [],
            "market_caps": [],
        },
        "derivatives": {},
        "dominance": {},
        "errors": {},
    }

    try:
        market = _cached_source(
            "cg_market",
            fetch_sol_market,
        )
        if isinstance(market, dict):
            result["market"] = market
    except Exception as exc:
        result["errors"]["market"] = str(exc)
        print(
            f"[sol] CoinGecko market failed: {exc}"
        )

    try:
        chart = _cached_source(
            "cg_chart",
            lambda: fetch_sol_market_chart(30),
        )
        if isinstance(chart, dict):
            result["chart"] = chart
    except Exception as exc:
        result["errors"]["chart"] = str(exc)
        print(
            f"[sol] CoinGecko market chart failed: {exc}"
        )

    try:
        derivatives = _cached_source(
            "cg_derivatives",
            fetch_sol_derivatives,
        )
        if isinstance(derivatives, dict):
            result["derivatives"] = derivatives
    except Exception as exc:
        result["errors"]["derivatives"] = str(exc)
        print(
            f"[sol] CoinGecko derivatives failed: {exc}"
        )

    try:
        dominance = _cached_source(
            "cg_dominance",
            fetch_sol_dominance,
        )
        if isinstance(dominance, dict):
            result["dominance"] = dominance
    except Exception as exc:
        result["errors"]["dominance"] = str(exc)
        print(
            f"[sol] CoinGecko dominance failed: {exc}"
        )

    return result


def _get_defi_bundle() -> dict:
    return {
        "tvl": _cached_source(
            "defi_tvl",
            fetch_sol_tvl,
        ),
        "protocols": _cached_source(
            "defi_protocols",
            fetch_sol_protocol_breakdown,
        ),
        "dex": _cached_source(
            "defi_dex",
            fetch_sol_dex_volume,
        ),
        "stablecoins": _cached_source(
            "defi_stablecoins",
            fetch_sol_stablecoin_supply,
        ),
    }


def _get_staking() -> dict:
    return _cached_source(
        "staking",
        fetch_sol_staking,
        ttl=15 * 60,
    )


# ── SOL history persistence ───────────────────────────────────────────────────

def _store_derivatives_observation(
    oi_usd: Optional[float],
    funding_rate: Optional[float],
) -> None:
    if (
        oi_usd is None
        and funding_rate is None
    ):
        return

    now_ts = int(time.time())

    try:
        with sqlite3.connect(
            SOL_DB_PATH
        ) as conn:
            recent = conn.execute(
                """
                SELECT timestamp
                FROM sol_derivatives_history
                ORDER BY timestamp DESC
                LIMIT 1
                """
            ).fetchone()

            if (
                recent
                and (
                    now_ts
                    - int(recent[0])
                )
                < DERIV_HISTORY_MIN_WRITE_SECONDS
            ):
                return

            conn.execute(
                """
                INSERT OR REPLACE INTO sol_derivatives_history
                    (
                        timestamp,
                        open_interest_usd,
                        funding_rate
                    )
                VALUES (?, ?, ?)
                """,
                (
                    now_ts,
                    oi_usd,
                    funding_rate,
                ),
            )

            cutoff = (
                now_ts
                - DERIV_HISTORY_RETENTION_DAYS
                * 86400
            )

            conn.execute(
                """
                DELETE FROM sol_derivatives_history
                WHERE timestamp < ?
                """,
                (cutoff,),
            )

            conn.commit()

    except Exception as exc:
        print(
            "[sol] derivatives history write failed: "
            f"{exc}"
        )


def _derivatives_history(
    field: str,
    days: int = 35,
) -> list[tuple[int, float]]:
    if field not in {
        "open_interest_usd",
        "funding_rate",
    }:
        return []

    cutoff = (
        int(time.time())
        - days * 86400
    )

    try:
        with sqlite3.connect(
            SOL_DB_PATH
        ) as conn:
            rows = conn.execute(
                f"""
                SELECT timestamp, {field}
                FROM sol_derivatives_history
                WHERE timestamp >= ?
                  AND {field} IS NOT NULL
                ORDER BY timestamp ASC
                """,
                (cutoff,),
            ).fetchall()

        return [
            (
                int(timestamp),
                float(value),
            )
            for timestamp, value in rows
        ]

    except Exception:
        return []


def _closest_history_value(
    rows: list[tuple[int, float]],
    target_ts: int,
) -> Optional[float]:
    if not rows:
        return None

    return min(
        rows,
        key=lambda row: abs(
            row[0] - target_ts
        ),
    )[1]


def _daily_history_values(
    table: str,
    field: str,
    days: int = 90,
) -> list[tuple[str, float]]:
    allowed = {
        ("sol_basis_history", "basis_pct"),
        ("sol_dominance_history", "dominance_pct"),
        ("sol_stablecoin_history", "total_usd"),
    }

    if (table, field) not in allowed:
        return []

    try:
        with sqlite3.connect(
            SOL_DB_PATH
        ) as conn:
            rows = conn.execute(
                f"""
                SELECT date, {field}
                FROM {table}
                WHERE {field} IS NOT NULL
                ORDER BY date DESC
                LIMIT ?
                """,
                (days,),
            ).fetchall()

        return [
            (str(row[0]), float(row[1]))
            for row in reversed(rows)
        ]

    except Exception:
        return []


def _store_daily_observations(
    *,
    basis: dict,
    dominance: dict,
    stablecoins: dict,
    tvl: dict,
    dex: dict,
) -> None:
    today = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    try:
        with sqlite3.connect(
            SOL_DB_PATH
        ) as conn:
            basis_pct = _safe_float(
                basis.get("basis_pct")
            )
            spot_price = _safe_float(
                basis.get("spot_price")
            )
            futures_price = _safe_float(
                basis.get("futures_price")
            )
            days_to_expiry = basis.get(
                "days_to_expiry"
            )

            if (
                basis_pct is not None
                and spot_price is not None
                and futures_price is not None
                and days_to_expiry is not None
            ):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO sol_basis_history
                        (
                            date,
                            basis_pct,
                            spot_price,
                            futures_price,
                            days_to_expiry
                        )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        today,
                        basis_pct,
                        spot_price,
                        futures_price,
                        int(days_to_expiry),
                    ),
                )

            dominance_pct = _safe_float(
                dominance.get(
                    "dominance_pct"
                )
            )

            if dominance_pct is not None:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO sol_dominance_history
                        (
                            date,
                            dominance_pct
                        )
                    VALUES (?, ?)
                    """,
                    (
                        today,
                        dominance_pct,
                    ),
                )

            stablecoin_total = _safe_float(
                stablecoins.get(
                    "total_usd"
                )
            )

            if stablecoin_total is not None:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO sol_stablecoin_history
                        (
                            date,
                            total_usd
                        )
                    VALUES (?, ?)
                    """,
                    (
                        today,
                        stablecoin_total,
                    ),
                )

            conn.commit()

    except Exception as exc:
        print(
            f"[sol] daily history write failed: {exc}"
        )

    try:
        tvl_usd = _safe_float(
            tvl.get("tvl_usd")
        )
        dex_24h = _safe_float(
            dex.get("dex_volume_24h")
        )

        if (
            tvl_usd is not None
            or dex_24h is not None
        ):
            with sqlite3.connect(
                SOL_TVL_DB_PATH
            ) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO sol_tvl_history
                        (
                            date,
                            tvl_usd,
                            dex_volume_24h
                        )
                    VALUES (?, ?, ?)
                    """,
                    (
                        today,
                        tvl_usd,
                        dex_24h,
                    ),
                )
                conn.commit()

    except Exception as exc:
        print(
            f"[sol] TVL history write failed: {exc}"
        )


# ── Formatters ────────────────────────────────────────────────────────────────

def format_price_move(
    change_24h: Optional[float],
    change_7d: Optional[float],
    change_30d: Optional[float],
    price_history: list[float],
) -> dict:
    if change_24h is None:
        return _unavailable(
            "price_move",
            "CoinGecko",
        )

    abs_24h = abs(change_24h)

    if abs_24h > 8:
        alert, level = (
            "Extreme move",
            "extreme",
        )
    elif abs_24h > 5:
        alert, level = (
            "Large move",
            "notable",
        )
    elif abs_24h > 3:
        alert, level = (
            "Notable move",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    daily_moves = []

    for index in range(
        1,
        len(price_history),
    ):
        previous = price_history[
            index - 1
        ]
        current = price_history[
            index
        ]

        if previous:
            daily_moves.append(
                abs(
                    (
                        current
                        - previous
                    )
                    / previous
                    * 100
                )
            )

    return {
        "current": f"{change_24h:+.1f}%",
        "d7": (
            f"{change_7d:+.1f}%"
            if change_7d is not None
            else "—"
        ),
        "vs30d": (
            f"{change_30d:+.1f}%"
            if change_30d is not None
            else "—"
        ),
        "percentile": _percentile(
            abs_24h,
            daily_moves,
        ),
        "alert": alert,
        "level": level,
        "pattern": (
            "Recovery"
            if (change_7d or 0) > 0
            else "Decline"
        ),
        "source": "CoinGecko",
    }


def format_volume(
    current_volume: Optional[float],
    volume_history: list[float],
) -> dict:
    if current_volume is None:
        return _unavailable(
            "volume",
            "CoinGecko",
        )

    history = [
        float(value)
        for value in volume_history[-30:]
        if (
            value is not None
            and value > 0
        )
    ]

    avg_30d = None
    avg_7d = None
    ratio = 1.0

    if history:
        avg_30d = (
            sum(history)
            / len(history)
        )

        recent_7 = history[-7:]
        avg_7d = (
            sum(recent_7)
            / len(recent_7)
        )

        if avg_30d:
            ratio = (
                current_volume
                / avg_30d
            )

    if ratio > 2.0:
        alert, level = (
            "Extreme activity",
            "extreme",
        )
    elif ratio > 1.5:
        alert, level = (
            "High activity",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    return {
        "current": _fmt_usd(
            current_volume
        ),
        "d7": (
            f"{_fmt_usd(avg_7d)} avg"
            if avg_7d is not None
            else "—"
        ),
        "vs30d": (
            f"{ratio:.2f}x 30d avg"
            if avg_30d is not None
            else "accumulating history"
        ),
        "percentile": _percentile(
            current_volume,
            history,
        ),
        "alert": alert,
        "level": level,
        "pattern": (
            "Volume surge"
            if ratio > 1.5
            else "Normal activity"
        ),
        "source": (
            "CoinGecko market_chart total_volumes"
        ),
    }


def format_funding(
    funding_rate: Optional[float],
) -> dict:
    if funding_rate is None:
        return _unavailable(
            "funding",
            "CoinGecko derivatives",
        )

    pct_8h = (
        funding_rate * 100
    )
    annualized = (
        pct_8h
        * 3
        * 365
    )

    if pct_8h > 0.07:
        alert, level = (
            "Extreme leverage",
            "extreme",
        )
    elif pct_8h > 0.04:
        alert, level = (
            "High leverage",
            "notable",
        )
    elif pct_8h < -0.03:
        alert, level = (
            "Extreme short bias",
            "extreme",
        )
    elif pct_8h < 0:
        alert, level = (
            "Short bias",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    history = _derivatives_history(
        "funding_rate",
        35,
    )

    historical_pct = [
        row[1] * 100
        for row in history
    ]

    return {
        "current": f"{pct_8h:.4f}%",
        "d7": "—",
        "vs30d": "—",
        "percentile": _percentile(
            pct_8h,
            historical_pct,
        ),
        "alert": alert,
        "level": level,
        "pattern": (
            f"Annualized equivalent "
            f"{annualized:.1f}%"
        ),
        "source": (
            "CoinGecko · Binance/Bybit/OKX "
            "OI-weighted"
        ),
    }


def format_open_interest(
    oi_usd: Optional[float],
) -> dict:
    if oi_usd is None:
        return _unavailable(
            "open_interest",
            "CoinGecko derivatives",
        )

    rows = _derivatives_history(
        "open_interest_usd",
        35,
    )

    now_ts = int(time.time())

    coverage_days = (
        (
            rows[-1][0]
            - rows[0][0]
        )
        / 86400
        if len(rows) >= 2
        else 0.0
    )

    values = [
        row[1]
        for row in rows
    ]

    d7_value = None
    d30_value = None

    if coverage_days >= 6.5:
        d7_value = (
            _closest_history_value(
                rows,
                now_ts - 7 * 86400,
            )
        )

    if coverage_days >= 29:
        d30_value = (
            _closest_history_value(
                rows,
                now_ts - 30 * 86400,
            )
        )

    d7_change = (
        (
            oi_usd
            - d7_value
        )
        / d7_value
        if d7_value
        else None
    )

    d30_change = (
        (
            oi_usd
            - d30_value
        )
        / d30_value
        if d30_value
        else None
    )

    percentile = (
        _percentile(
            oi_usd,
            values,
        )
        if coverage_days >= 1
        else 50.0
    )

    if (
        d7_change is not None
        and d7_change > 0.30
    ):
        alert, level = (
            "Extreme build-up",
            "extreme",
        )
    elif (
        d7_change is not None
        and d7_change > 0.18
    ):
        alert, level = (
            "Rapid build-up",
            "notable",
        )
    elif (
        coverage_days >= 7
        and percentile >= 90
    ):
        alert, level = (
            "OI near history high",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    return {
        "current": _fmt_usd(
            oi_usd
        ),
        "d7": (
            f"{d7_change * 100:+.1f}%"
            if d7_change is not None
            else "accumulating history"
        ),
        "vs30d": (
            f"{d30_change * 100:+.1f}%"
            if d30_change is not None
            else "accumulating history"
        ),
        "percentile": percentile,
        "alert": alert,
        "level": level,
        "pattern": (
            f"{coverage_days:.1f}d "
            "persisted OI history"
        ),
        "source": (
            "CoinGecko · Binance/Bybit/OKX "
            "aggregate"
        ),
    }


def format_cme_basis(
    raw: dict,
) -> dict:
    basis_pct = _safe_float(
        raw.get("basis_pct")
    )

    if basis_pct is None:
        return _unavailable(
            "cme_basis",
            raw.get(
                "source",
                (
                    "Yahoo Finance SOL=F · "
                    "shared core cache"
                ),
            ),
            raw.get("error", ""),
        )

    history = _daily_history_values(
        "sol_basis_history",
        "basis_pct",
        90,
    )
    values = [
        row[1]
        for row in history
    ]

    if basis_pct < 0:
        alert, level = (
            "Backwardation",
            "extreme",
        )
    elif basis_pct < 5:
        alert, level = (
            "Compressed",
            "notable",
        )
    elif basis_pct > 20:
        alert, level = (
            "Extreme carry",
            "extreme",
        )
    elif basis_pct > 15:
        alert, level = (
            "Elevated",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    raw_basis = _safe_float(
        raw.get("raw_basis_pct")
    )

    return {
        "current": f"{basis_pct:.1f}%",
        "d7": "—",
        "vs30d": "—",
        "percentile": (
            _percentile(
                basis_pct,
                values,
            )
            if len(values) >= 7
            else 50
        ),
        "alert": alert,
        "level": level,
        "pattern": (
            f"{raw.get('days_to_expiry', '—')}d "
            "to CME expiry · "
            f"{raw_basis:+.2f}% raw premium"
            if raw_basis is not None
            else (
                f"{raw.get('days_to_expiry', '—')}d "
                "to CME expiry"
            )
        ),
        "source": raw.get(
            "source",
            (
                "Yahoo Finance SOL=F · "
                "shared core cache"
            ),
        ),
    }


def format_defi_tvl(
    raw: dict,
) -> dict:
    tvl = _safe_float(
        raw.get("tvl_usd")
    )
    tvl_7d = _safe_float(
        raw.get("tvl_7d_ago")
    )
    tvl_30d = _safe_float(
        raw.get("tvl_30d_ago")
    )

    if tvl is None:
        return _unavailable(
            "defi_tvl",
            raw.get(
                "source",
                "DeFiLlama",
            ),
            raw.get("error", ""),
        )

    d7_change = (
        tvl - tvl_7d
        if tvl_7d is not None
        else None
    )

    d30_pct = (
        (
            tvl / tvl_30d
        )
        - 1
    ) * 100 if tvl_30d else None

    percentile = float(
        raw.get(
            "percentile",
            50,
        )
        or 50
    )

    if (
        d7_change is not None
        and d7_change > 0
        and percentile > 75
    ):
        alert, level = (
            "TVL acceleration",
            "notable",
        )
    elif (
        d7_change is not None
        and d7_change
        < -0.10 * tvl
    ):
        alert, level = (
            "TVL contraction",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    return {
        "current": _fmt_usd(tvl),
        "d7": (
            _fmt_usd(
                d7_change,
                signed=True,
            )
            if d7_change is not None
            else "—"
        ),
        "vs30d": (
            f"{d30_pct:+.1f}%"
            if d30_pct is not None
            else "—"
        ),
        "percentile": percentile,
        "alert": alert,
        "level": level,
        "pattern": (
            "Capital inflow"
            if (d7_change or 0) > 0
            else "Capital outflow"
        ),
        "source": raw.get(
            "source",
            "DeFiLlama",
        ),
    }


def format_dex_volume(
    raw: dict,
) -> dict:
    volume_24h = _safe_float(
        raw.get("dex_volume_24h")
    )
    volume_7d = _safe_float(
        raw.get("dex_volume_7d")
    )

    if volume_24h is None:
        return _unavailable(
            "dex_volume",
            raw.get(
                "source",
                "DeFiLlama",
            ),
            raw.get("error", ""),
        )

    avg_7d = (
        volume_7d / 7
        if volume_7d
        else None
    )

    ratio = (
        volume_24h / avg_7d
        if avg_7d
        else 1.0
    )

    if ratio > 2.0:
        alert, level = (
            "Extreme DEX activity",
            "extreme",
        )
    elif ratio > 1.5:
        alert, level = (
            "High DEX activity",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    return {
        "current": _fmt_usd(
            volume_24h
        ),
        "d7": (
            _fmt_usd(
                volume_7d
            )
            if volume_7d is not None
            else "—"
        ),
        "vs30d": (
            f"{ratio:.2f}x 7d daily avg"
            if avg_7d is not None
            else "—"
        ),
        "percentile": min(
            100,
            max(
                0,
                ratio / 2 * 100,
            ),
        ),
        "alert": alert,
        "level": level,
        "pattern": "Solana DEX activity",
        "source": raw.get(
            "source",
            "DeFiLlama",
        ),
    }


def format_stablecoin(
    raw: dict,
) -> dict:
    total = _safe_float(
        raw.get("total_usd")
    )

    if total is None:
        return _unavailable(
            "stablecoin_sol",
            raw.get(
                "source",
                "DeFiLlama stablecoins",
            ),
            raw.get("error", ""),
        )

    d7_ago = _safe_float(
        raw.get("total_7d_ago")
    )
    d30_ago = _safe_float(
        raw.get("total_30d_ago")
    )

    d7_change = (
        total - d7_ago
        if d7_ago is not None
        else None
    )

    d30_change_pct = (
        (
            total / d30_ago
        )
        - 1
    ) * 100 if d30_ago else None

    return {
        "current": _fmt_usd(total),
        "d7": (
            _fmt_usd(
                d7_change,
                signed=True,
            )
            if d7_change is not None
            else "—"
        ),
        "vs30d": (
            f"{d30_change_pct:+.1f}%"
            if d30_change_pct is not None
            else "—"
        ),
        "percentile": float(
            raw.get(
                "percentile",
                50,
            )
            or 50
        ),
        "alert": "—",
        "level": "none",
        "pattern": (
            "Stablecoin liquidity on Solana"
        ),
        "source": raw.get(
            "source",
            "DeFiLlama stablecoins",
        ),
    }


def format_staking(
    raw: dict,
) -> dict:
    staking_rate = _safe_float(
        raw.get("staking_rate_pct")
    )
    validators = raw.get(
        "active_validators"
    )

    if (
        staking_rate is None
        or validators is None
    ):
        return _unavailable(
            "staking_rate",
            raw.get(
                "source",
                "Solana RPC",
            ),
            raw.get("error", ""),
        )

    top5 = _safe_float(
        raw.get(
            "top5_concentration_pct"
        )
    )

    return {
        "current": (
            f"{staking_rate:.1f}%"
        ),
        "d7": "—",
        "vs30d": "—",
        "percentile": 50,
        "alert": "—",
        "level": "none",
        "pattern": (
            f"{int(validators):,} active validators"
            + (
                f" · top-5 stake {top5:.1f}%"
                if top5 is not None
                else ""
            )
        ),
        "source": raw.get(
            "source",
            "Solana RPC",
        ),
    }


def format_dominance(
    raw: dict,
) -> dict:
    dominance = _safe_float(
        raw.get("dominance_pct")
    )

    if dominance is None:
        return _unavailable(
            "dominance",
            raw.get(
                "source",
                "CoinGecko global",
            ),
            raw.get("error", ""),
        )

    history = _daily_history_values(
        "sol_dominance_history",
        "dominance_pct",
        90,
    )

    values = [
        row[1]
        for row in history
    ]

    d7_value = (
        history[-8][1]
        if len(history) >= 8
        else None
    )
    d30_value = (
        history[-31][1]
        if len(history) >= 31
        else None
    )

    d7_change = (
        dominance - d7_value
        if d7_value is not None
        else None
    )
    d30_change = (
        dominance - d30_value
        if d30_value is not None
        else None
    )

    if dominance > 3:
        alert, level = (
            "High dominance",
            "notable",
        )
    elif dominance < 1:
        alert, level = (
            "Low dominance",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    return {
        "current": f"{dominance:.2f}%",
        "d7": (
            f"{d7_change:+.2f}pp"
            if d7_change is not None
            else "accumulating history"
        ),
        "vs30d": (
            f"{d30_change:+.2f}pp"
            if d30_change is not None
            else "accumulating history"
        ),
        "percentile": (
            _percentile(
                dominance,
                values,
            )
            if len(values) >= 7
            else 50
        ),
        "alert": alert,
        "level": level,
        "pattern": (
            "SOL share of total crypto market cap"
        ),
        "source": raw.get(
            "source",
            "CoinGecko global",
        ),
    }


# ── Metric builder ────────────────────────────────────────────────────────────

def _build_metrics() -> dict:
    market_bundle = _get_market_bundle()

    market = market_bundle["market"]
    chart = market_bundle["chart"]
    derivatives = market_bundle[
        "derivatives"
    ]
    dominance = market_bundle[
        "dominance"
    ]

    oi_usd = _safe_float(
        derivatives.get(
            "open_interest_usd"
        )
    )
    funding_rate = _safe_float(
        derivatives.get(
            "funding"
        )
    )

    _store_derivatives_observation(
        oi_usd,
        funding_rate,
    )

    defi = _get_defi_bundle()
    staking = _get_staking()

    basis = _cached_source(
        "cme_basis",
        lambda: fetch_sol_cme_basis(
            _safe_float(
                market.get(
                    "price_usd"
                )
            )
        ),
        ttl=15 * 60,
    )

    _store_daily_observations(
        basis=basis,
        dominance=dominance,
        stablecoins=defi[
            "stablecoins"
        ],
        tvl=defi["tvl"],
        dex=defi["dex"],
    )

    return {
        "price_move": (
            format_price_move(
                _safe_float(
                    market.get(
                        "change_24h"
                    )
                ),
                _safe_float(
                    market.get(
                        "change_7d"
                    )
                ),
                _safe_float(
                    market.get(
                        "change_30d"
                    )
                ),
                chart.get(
                    "prices",
                    [],
                ),
            )
        ),
        "volume": (
            format_volume(
                _safe_float(
                    market.get(
                        "volume_24h"
                    )
                ),
                chart.get(
                    "volumes",
                    [],
                ),
            )
        ),
        "funding": (
            format_funding(
                funding_rate
            )
        ),
        "open_interest": (
            format_open_interest(
                oi_usd
            )
        ),
        "cme_basis": (
            format_cme_basis(
                basis
            )
        ),
        "defi_tvl": (
            format_defi_tvl(
                defi["tvl"]
            )
        ),
        "dex_volume": (
            format_dex_volume(
                defi["dex"]
            )
        ),
        "stablecoin_sol": (
            format_stablecoin(
                defi["stablecoins"]
            )
        ),
        "staking_rate": (
            format_staking(
                staking
            )
        ),
        "dominance": (
            format_dominance(
                dominance
            )
        ),
    }


def _build_metrics_cached() -> dict:
    now = time.time()

    if (
        _metric_cache["data"] is not None
        and (
            now
            - _metric_cache["ts"]
        )
        < METRIC_CACHE_TTL
    ):
        return _metric_cache["data"]

    result = _build_metrics()

    _metric_cache["data"] = result
    _metric_cache["ts"] = now

    return result


# ── Manual overrides ──────────────────────────────────────────────────────────

def _load_overrides() -> dict:
    if not SOL_OVERRIDES.exists():
        return {}

    try:
        data = json.loads(
            SOL_OVERRIDES.read_text(
                encoding="utf-8"
            )
        )

        return (
            data
            if isinstance(data, dict)
            else {}
        )

    except Exception:
        return {}


def _write_overrides(
    overrides: dict,
) -> None:
    tmp = SOL_OVERRIDES.with_name(
        f".{SOL_OVERRIDES.name}."
        f"{os.getpid()}.tmp"
    )

    tmp.write_text(
        json.dumps(
            overrides,
            indent=2,
        ),
        encoding="utf-8",
    )

    os.replace(
        tmp,
        SOL_OVERRIDES,
    )


class OverridePayload(BaseModel):
    metric: str
    current: str
    d7: str
    vs30d: str
    percentile: float
    alert: str
    level: str
    pattern: str
    source: Optional[str] = "manual"
    baseline_date: Optional[str] = None


# ── Routes ────────────────────────────────────────────────────────────────────

@sol_router.get("/")
def sol_root():
    return {
        "service": "sol-dashboard",
        "status": "ok",
        "version": "0.3.0",
    }


@sol_router.get("/metrics")
def sol_metrics():
    metrics = _build_metrics_cached()
    overrides = _load_overrides()

    result = {}

    for key, metric in metrics.items():
        override = overrides.get(key)

        if isinstance(
            override,
            dict,
        ):
            result[key] = {
                **metric,
                **override,
                "_is_override": True,
                "_mock": False,
            }
        else:
            result[key] = metric

    return result


@sol_router.get("/price")
def sol_price():
    market = _get_market_bundle()[
        "market"
    ]

    return {
        "price": _safe_float(
            market.get("price_usd")
        ),
        "change_24h": _safe_float(
            market.get("change_24h")
        ),
        "change_7d": _safe_float(
            market.get("change_7d")
        ),
        "change_30d": _safe_float(
            market.get("change_30d")
        ),
        "market_cap": _safe_float(
            market.get("market_cap")
        ),
        "ath": _safe_float(
            market.get("ath")
        ),
        "ath_pct": _safe_float(
            market.get(
                "ath_change_pct"
            )
        ),
    }


@sol_router.get("/tvl")
def sol_tvl():
    defi = _get_defi_bundle()

    return {
        "chain_tvl": defi["tvl"],
        "protocols": defi[
            "protocols"
        ][:8],
        "dex_volume": defi["dex"],
    }


@sol_router.get("/stablecoin")
def sol_stablecoin():
    return _get_defi_bundle()[
        "stablecoins"
    ]


@sol_router.get("/staking")
def sol_staking():
    return _get_staking()


@sol_router.get("/cme-basis")
def sol_cme_basis():
    market = _get_market_bundle()[
        "market"
    ]

    raw = fetch_sol_cme_basis(
        _safe_float(
            market.get("price_usd")
        )
    )

    metric = format_cme_basis(
        raw
    )

    return {
        **metric,
        **raw,
    }


@sol_router.get("/ousd-status")
def ousd_status():
    """
    Static thesis context.

    This route intentionally does not pretend to be live chain data. Replace
    with on-chain OUSD supply/integration telemetry once those sources exist.
    """
    return {
        "status": "pre_launch",
        "expected_live": "H2 2026",
        "announced": "2026-06-30",
        "partner_count": 140,
        "native_chains": [
            "Solana",
            "Stellar",
            "Base",
            "Polygon",
        ],
        "confirmed_partners": [
            "Visa",
            "Mastercard",
            "Stripe",
            "American Express",
            "Google",
            "Shopify",
            "BlackRock",
            "BNY",
            "Standard Chartered",
            "Coinbase",
            "Aave",
            "Solana Foundation",
        ],
        "open_questions": [
            "Reserve custodian — unpublished",
            "Attestation cadence — not confirmed",
            "Go-live date — H2 2026 expected, not fixed",
            "Management fee — not disclosed",
        ],
        "thesis_signals": {
            "confirms": [
                "Solana chosen as native chain — day-one deployment",
                "Stripe making OUSD default for business transactions",
                "140+ partner signatories including Tier 1 banks and payment networks",
                "Stablecoin supply on Solana accelerating pre-launch",
            ],
            "invalidates": [
                "Launch delayed beyond H2 2026",
                "Reserve composition or attestation below USDC standards",
                "Partner integration rate at go-live << 140 signatories",
                "Solana de-prioritized post-launch in favor of other chains",
            ],
        },
        "_data_mode": "static_thesis_context",
        "_last_updated": "2026-07-03",
    }


@sol_router.get("/summary")
def sol_summary():
    metrics = _build_metrics_cached()

    available = [
        metric
        for metric in metrics.values()
        if not metric.get("_mock")
    ]

    extreme = sum(
        1
        for metric in available
        if metric.get("level")
        == "extreme"
    )
    notable = sum(
        1
        for metric in available
        if metric.get("level")
        == "notable"
    )
    neutral = sum(
        1
        for metric in available
        if metric.get("level")
        == "none"
    )
    unavailable = (
        len(metrics)
        - len(available)
    )

    if extreme >= 2:
        structure = "EXTREME"
    elif (
        extreme >= 1
        or notable >= 4
    ):
        structure = "ELEVATED"
    elif notable >= 2:
        structure = "RECOVERY"
    else:
        structure = "NEUTRAL"

    return {
        "structure": structure,
        "extreme": extreme,
        "notable": notable,
        "neutral": neutral,
        "unavailable": unavailable,
    }


@sol_router.get("/manual-override")
def sol_get_overrides():
    return _load_overrides()


@sol_router.post("/manual-override")
def sol_set_override(
    payload: OverridePayload,
):
    overrides = _load_overrides()

    overrides[payload.metric] = (
        payload.dict()
    )

    _write_overrides(
        overrides
    )

    _metric_cache["data"] = None
    _metric_cache["ts"] = 0.0

    return {
        "status": "ok",
        "metric": payload.metric,
        "overrides_active": len(
            overrides
        ),
    }


@sol_router.delete(
    "/manual-override/{metric}"
)
def sol_clear_override(
    metric: str,
):
    overrides = _load_overrides()
    removed = metric in overrides

    if removed:
        del overrides[metric]

        _write_overrides(
            overrides
        )

        _metric_cache["data"] = None
        _metric_cache["ts"] = 0.0

    return {
        "status": "ok",
        "cleared": metric,
        "was_active": removed,
    }


@sol_router.get("/db/summary")
def sol_db_summary():
    results = {}

    for name, path in [
        (
            "sol_history",
            SOL_DB_PATH,
        ),
        (
            "sol_tvl_history",
            SOL_TVL_DB_PATH,
        ),
    ]:
        if not path.exists():
            results[name] = (
                "not initialized"
            )
            continue

        with sqlite3.connect(
            path
        ) as conn:
            tables = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type='table'
                    ORDER BY name
                    """
                ).fetchall()
            ]

            results[name] = {}

            for table in tables:
                count = conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table}
                    """
                ).fetchone()[0]

                results[name][table] = (
                    count
                )

    oi_rows = _derivatives_history(
        "open_interest_usd",
        35,
    )

    results["derivatives_history"] = {
        "samples": len(oi_rows),
        "coverage_days": (
            round(
                (
                    oi_rows[-1][0]
                    - oi_rows[0][0]
                )
                / 86400,
                2,
            )
            if len(oi_rows) >= 2
            else 0.0
        ),
        "latest_oi_usd": (
            oi_rows[-1][1]
            if oi_rows
            else None
        ),
    }

    return results


@sol_router.get("/debug/derivatives")
def sol_debug_derivatives():
    """
    Inspect the same strict SOL records used by shared.cg_cache.
    Useful when running the heavy analytics app directly.
    """
    from shared.cg_cache import (
        REFERENCE_EXCHANGES,
        get_derivatives,
    )

    all_tickers = get_derivatives()

    if not all_tickers:
        return {
            "error": (
                "get_derivatives() returned empty"
            )
        }

    candidates = [
        ticker
        for ticker in all_tickers
        if any(
            "SOL" in str(value).upper()
            for value in ticker.values()
            if isinstance(value, str)
        )
    ]

    strict = [
        ticker
        for ticker in all_tickers
        if ticker.get(
            "index_id",
            "",
        ).upper()
        == "SOL"
        and ticker.get(
            "contract_type"
        )
        == "perpetual"
        and ticker.get(
            "market"
        )
        in REFERENCE_EXCHANGES
        and ticker.get(
            "funding_rate"
        )
        is not None
        and (
            _safe_float(
                ticker.get(
                    "open_interest"
                )
            )
            or 0
        )
        > 0
    ]

    return {
        "total_tickers_in_cache": len(
            all_tickers
        ),
        "sol_candidates_broad": len(
            candidates
        ),
        "sol_strict_filter_match": len(
            strict
        ),
        "reference_exchanges": sorted(
            REFERENCE_EXCHANGES
        ),
        "field_keys_on_record": (
            list(
                candidates[0].keys()
            )
            if candidates
            else []
        ),
        "strict_matches": [
            {
                "market": ticker.get(
                    "market"
                ),
                "symbol": ticker.get(
                    "symbol"
                ),
                "index_id": ticker.get(
                    "index_id"
                ),
                "funding_rate": ticker.get(
                    "funding_rate"
                ),
                "open_interest": ticker.get(
                    "open_interest"
                ),
            }
            for ticker in strict
        ],
    }
