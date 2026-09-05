"""
eth_routes.py — Ethereum Dashboard Backend Routes

Designed for the Phase-3 disposable collector architecture.

Key rules:
- No direct yFinance calls from this module.
- CME ETH futures come from shared.yf_core_cache ("eth_futures"), which is
  warmed once by the fast collector alongside BTC and SOL futures.
- ETH spot / market data comes from CoinGecko.
- ETH funding + OI reuse shared.cg_cache /derivatives.
- CoinGecko source failures are isolated: a failed history request must not
  erase a successful current-price or derivatives fetch.
- ETH OI/funding history persists in SQLite across collector process exits.
- DeFiLlama, staking, and gas fetches are reused within one collector process.
- No synthetic fallback values are invented when a source is unavailable.

Collector routes:
    /eth/metrics
    /eth/price
    /eth/summary
    /eth/tvl
    /eth/structural
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
    get_weighted_funding_oi as _cg_derivs,
)
from shared.yf_core_cache import get_series as _yf_core_series


# ── Config ────────────────────────────────────────────────────────────────────

eth_router = APIRouter(prefix="/eth", tags=["Ethereum"])

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

ETH_DB_PATH = DATA_DIR / "eth_history.db"
ETH_TVL_DB_PATH = DATA_DIR / "eth_tvl_history.db"
ETH_OVERRIDES = DATA_DIR / "eth_overrides.json"

DEFILLAMA_BASE = "https://api.llama.fi"
BEACON_BASE = "https://beaconcha.in/api/v1"

ETH_RPC_ENDPOINTS = [
    ("Cloudflare", "https://cloudflare-eth.com/v1/mainnet"),
    ("PublicNode", "https://ethereum-rpc.publicnode.com"),
]

L2_CHAIN_ALIASES = {
    "Arbitrum": {"Arbitrum"},
    "Base": {"Base"},
    "Optimism": {"Optimism", "OP Mainnet"},
    "zkSync Era": {"zkSync Era", "ZKsync Era", "Zksync Era"},
    "Linea": {"Linea"},
    "Scroll": {"Scroll"},
    "Polygon zkEVM": {"Polygon zkEVM", "Polygon zkEvm"},
}

SOURCE_CACHE_TTL = 5 * 60
METRIC_CACHE_TTL = 5 * 60
DERIV_HISTORY_RETENTION_DAYS = 35
DERIV_HISTORY_MIN_WRITE_SECONDS = 10 * 60


# ── Database init ─────────────────────────────────────────────────────────────

def _init_dbs() -> None:
    with sqlite3.connect(ETH_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS eth_basis_history (
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
            CREATE TABLE IF NOT EXISTS eth_btc_ratio_history (
                date TEXT PRIMARY KEY,
                eth_btc_ratio REAL,
                eth_price REAL,
                btc_price REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS eth_gas_history (
                date TEXT PRIMARY KEY,
                gas_gwei REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS eth_derivatives_history (
                timestamp INTEGER PRIMARY KEY,
                open_interest_usd REAL,
                funding_rate REAL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_eth_derivatives_timestamp
            ON eth_derivatives_history (timestamp)
            """
        )
        conn.commit()

    with sqlite3.connect(ETH_TVL_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS eth_tvl_history (
                date TEXT PRIMARY KEY,
                mainnet_tvl REAL,
                l2_tvl REAL,
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

    `_mock` is retained only because the existing ETH frontend already uses it
    to distinguish unavailable data from a healthy live metric.
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


def _next_cme_eth_expiry(
    today: Optional[date] = None,
) -> date:
    """
    CME Ether futures terminate on the last Friday of the contract month.

    ETH=F is a continuous-contract proxy, so this uses the current front-month
    calendar date rather than querying Yahoo metadata.
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


# ── CoinGecko helpers ─────────────────────────────────────────────────────────

def _cg(
    path: str,
    params: Optional[dict] = None,
):
    """
    Shared CoinGecko request/auth helper.

    shared/cg_cache.py should use x-cg-demo-api-key for COINGECKO_API_KEY,
    unless a separate Pro key is configured.
    """
    return _cg_shared(path, params)


def fetch_eth_market() -> dict:
    """
    Current ETH market state.
    """
    data = _cg(
        "/coins/ethereum",
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
        "price_btc": _safe_float(
            market_data.get(
                "current_price",
                {},
            ).get("btc")
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


def fetch_eth_market_chart(
    days: int = 30,
) -> dict:
    """
    Historical price and volume.

    CoinGecko OHLC only contains price candles. Volume metrics must use
    market_chart.total_volumes instead.
    """
    data = _cg(
        "/coins/ethereum/market_chart",
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


def fetch_eth_derivatives() -> dict:
    """
    ETH funding + aggregate OI from shared /derivatives cache.
    """
    result = _cg_derivs("ETH")

    return (
        result
        if isinstance(result, dict)
        else {}
    )


# ── DeFiLlama helpers ─────────────────────────────────────────────────────────

def fetch_eth_mainnet_tvl() -> dict:
    try:
        response = requests.get(
            f"{DEFILLAMA_BASE}/v2/historicalChainTvl/Ethereum",
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


def fetch_eth_protocol_breakdown() -> list[dict]:
    try:
        response = requests.get(
            f"{DEFILLAMA_BASE}/protocols",
            timeout=15,
        )
        response.raise_for_status()

        protocols = response.json()

        if not isinstance(protocols, list):
            return []

        ethereum = [
            protocol
            for protocol in protocols
            if "Ethereum"
            in (protocol.get("chains") or [])
        ]

        ethereum.sort(
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
            for protocol in ethereum[:10]
        ]

    except Exception as exc:
        print(
            f"[eth] protocol breakdown failed: {exc}"
        )
        return []


def fetch_eth_l2_tvl() -> dict:
    try:
        response = requests.get(
            f"{DEFILLAMA_BASE}/v2/chains",
            timeout=15,
        )
        response.raise_for_status()

        chains = response.json()

        if not isinstance(chains, list):
            return {
                "total_l2_tvl": None,
                "chains": [],
                "source": "DeFiLlama",
                "error": "unexpected chains response",
            }

        alias_to_label = {
            alias: label
            for label, aliases
            in L2_CHAIN_ALIASES.items()
            for alias in aliases
        }

        selected = []
        seen = set()

        for chain in chains:
            name = chain.get("name")
            label = alias_to_label.get(name)

            if not label or label in seen:
                continue

            tvl = _safe_float(
                chain.get("tvl")
            )
            if tvl is None:
                continue

            seen.add(label)
            selected.append(
                {
                    "name": label,
                    "tvl": tvl,
                }
            )

        selected.sort(
            key=lambda chain: chain["tvl"],
            reverse=True,
        )

        if not selected:
            return {
                "total_l2_tvl": None,
                "chains": [],
                "source": "DeFiLlama",
                "error": (
                    "no configured Ethereum L2 chains found"
                ),
            }

        return {
            "total_l2_tvl": sum(
                chain["tvl"]
                for chain in selected
            ),
            "chains": selected,
            "source": "DeFiLlama",
        }

    except Exception as exc:
        return {
            "total_l2_tvl": None,
            "chains": [],
            "source": "DeFiLlama",
            "error": str(exc),
        }


def fetch_eth_dex_volume() -> dict:
    try:
        response = requests.get(
            f"{DEFILLAMA_BASE}/overview/dexs/Ethereum",
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


# ── ETH staking ───────────────────────────────────────────────────────────────

def fetch_eth_staking(
    circulating_supply: Optional[float],
) -> dict:
    """
    Latest active validator count from beaconcha.in.

    Staked ETH is approximated as active validators × 32 ETH. Staking rate uses
    current CoinGecko circulating supply rather than a fixed 120M assumption.
    """
    try:
        response = requests.get(
            f"{BEACON_BASE}/epoch/latest",
            timeout=10,
        )
        response.raise_for_status()

        payload = response.json()
        data = payload.get("data", {})

        if isinstance(data, list):
            data = data[0] if data else {}

        if not isinstance(data, dict):
            raise ValueError(
                "unexpected beaconcha.in response"
            )

        validators = (
            data.get("validatorscount")
            or data.get("validatorsCount")
            or data.get("validators_count")
        )

        if validators is None:
            raise ValueError(
                "validator count unavailable"
            )

        validators = int(validators)

        if validators <= 0:
            raise ValueError(
                "validator count unavailable"
            )

        staked_eth = validators * 32.0

        staking_rate = None
        if (
            circulating_supply is not None
            and circulating_supply > 0
        ):
            staking_rate = (
                staked_eth
                / circulating_supply
                * 100
            )

        return {
            "active_validators": validators,
            "staked_eth": round(
                staked_eth / 1e6,
                2,
            ),
            "staking_rate_pct": (
                round(staking_rate, 2)
                if staking_rate is not None
                else None
            ),
            "source": "beaconcha.in",
        }

    except Exception as exc:
        return {
            "active_validators": None,
            "staked_eth": None,
            "staking_rate_pct": None,
            "source": "beaconcha.in",
            "error": str(exc),
        }


# ── Gas price ─────────────────────────────────────────────────────────────────

def fetch_eth_gas() -> dict:
    request_body = {
        "jsonrpc": "2.0",
        "method": "eth_gasPrice",
        "params": [],
        "id": 1,
    }

    errors = []

    for label, endpoint in ETH_RPC_ENDPOINTS:
        try:
            response = requests.post(
                endpoint,
                json=request_body,
                headers={
                    "Content-Type": "application/json"
                },
                timeout=8,
            )
            response.raise_for_status()

            payload = response.json()

            if payload.get("error"):
                raise ValueError(
                    str(payload["error"])
                )

            result = payload.get("result")

            if (
                not isinstance(result, str)
                or not result.startswith("0x")
            ):
                raise ValueError(
                    "missing hex gas result"
                )

            gas_gwei = (
                int(result, 16)
                / 1e9
            )

            if (
                gas_gwei < 0
                or gas_gwei > 10_000
            ):
                raise ValueError(
                    f"implausible gas value: {gas_gwei}"
                )

            return {
                "gas_gwei": round(
                    gas_gwei,
                    2,
                ),
                "source": label,
            }

        except Exception as exc:
            errors.append(
                f"{label}: {exc}"
            )

    return {
        "gas_gwei": None,
        "source": "public Ethereum RPC",
        "error": "; ".join(errors),
    }


# ── CME ETH basis — shared Yahoo core cache ───────────────────────────────────

def fetch_eth_cme_basis(
    spot_price: Optional[float],
) -> dict:
    """
    ETH spot comes from CoinGecko.
    ETH futures come from shared.yf_core_cache ("eth_futures").

    This function performs no yFinance network call.
    """
    if (
        spot_price is None
        or spot_price <= 0
    ):
        return {
            "basis_pct": None,
            "source": (
                "Yahoo Finance ETH=F · shared core cache"
            ),
            "error": "ETH spot price unavailable",
        }

    try:
        futures = _yf_core_series(
            "eth_futures"
        )

        if (
            futures is None
            or len(futures) == 0
        ):
            raise ValueError(
                "ETH=F unavailable from core cache"
            )

        futures_price = float(
            futures.iloc[-1]
        )
        futures_date = str(
            futures.index[-1].date()
        )

        if futures_price <= 0:
            raise ValueError(
                "invalid ETH=F futures price"
            )

        expiry = _next_cme_eth_expiry()
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

        # Refuse obviously broken continuous-ticker relationships.
        price_ratio = (
            futures_price / spot_price
        )

        if (
            price_ratio < 0.80
            or price_ratio > 1.20
            or annualized_basis_pct < -100
            or annualized_basis_pct > 150
        ):
            raise ValueError(
                "ETH=F/spot relationship failed sanity gate "
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
                "Yahoo Finance ETH=F · shared core cache"
            ),
        }

    except Exception as exc:
        return {
            "basis_pct": None,
            "source": (
                "Yahoo Finance ETH=F · shared core cache"
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
    Fetch CoinGecko components independently.

    A failed market_chart request cannot erase current market data, and a
    derivatives failure cannot erase either.
    """
    result = {
        "market": {},
        "chart": {
            "prices": [],
            "volumes": [],
            "market_caps": [],
        },
        "derivatives": {},
        "errors": {},
    }

    try:
        market = _cached_source(
            "cg_market",
            fetch_eth_market,
        )
        if isinstance(market, dict):
            result["market"] = market
    except Exception as exc:
        result["errors"]["market"] = str(exc)
        print(
            f"[eth] CoinGecko market failed: {exc}"
        )

    try:
        chart = _cached_source(
            "cg_chart",
            lambda: fetch_eth_market_chart(30),
        )
        if isinstance(chart, dict):
            result["chart"] = chart
    except Exception as exc:
        result["errors"]["chart"] = str(exc)
        print(
            f"[eth] CoinGecko market chart failed: {exc}"
        )

    try:
        derivatives = _cached_source(
            "cg_derivatives",
            fetch_eth_derivatives,
        )
        if isinstance(derivatives, dict):
            result["derivatives"] = derivatives
    except Exception as exc:
        result["errors"]["derivatives"] = str(exc)
        print(
            f"[eth] CoinGecko derivatives failed: {exc}"
        )

    return result


def _get_defi_bundle() -> dict:
    return {
        "mainnet": _cached_source(
            "defi_mainnet",
            fetch_eth_mainnet_tvl,
        ),
        "l2": _cached_source(
            "defi_l2",
            fetch_eth_l2_tvl,
        ),
        "protocols": _cached_source(
            "defi_protocols",
            fetch_eth_protocol_breakdown,
        ),
        "dex": _cached_source(
            "defi_dex",
            fetch_eth_dex_volume,
        ),
    }


def _get_staking(
    circulating_supply: Optional[float],
) -> dict:
    return _cached_source(
        "staking",
        lambda: fetch_eth_staking(
            circulating_supply
        ),
    )


def _get_gas() -> dict:
    return _cached_source(
        "gas",
        fetch_eth_gas,
        ttl=60,
    )


# ── ETH history persistence ───────────────────────────────────────────────────

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
            ETH_DB_PATH
        ) as conn:
            recent = conn.execute(
                """
                SELECT timestamp
                FROM eth_derivatives_history
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
                INSERT OR REPLACE INTO eth_derivatives_history
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
                DELETE FROM eth_derivatives_history
                WHERE timestamp < ?
                """,
                (cutoff,),
            )

            conn.commit()

    except Exception as exc:
        print(
            "[eth] derivatives history write failed: "
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
            ETH_DB_PATH
        ) as conn:
            rows = conn.execute(
                f"""
                SELECT timestamp, {field}
                FROM eth_derivatives_history
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


def _store_daily_observations(
    *,
    basis: dict,
    market: dict,
    gas: dict,
    mainnet_tvl: dict,
    l2_tvl: dict,
    dex: dict,
) -> None:
    """
    Populate the existing ETH daily tables so historical cards can mature
    without requiring a separate backfill service.
    """
    today = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    try:
        with sqlite3.connect(
            ETH_DB_PATH
        ) as conn:
            basis_pct = _safe_float(
                basis.get("basis_pct")
            )
            basis_spot = _safe_float(
                basis.get("spot_price")
            )
            basis_futures = _safe_float(
                basis.get("futures_price")
            )
            days_to_expiry = basis.get(
                "days_to_expiry"
            )

            if (
                basis_pct is not None
                and basis_spot is not None
                and basis_futures is not None
                and days_to_expiry is not None
            ):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO eth_basis_history
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
                        basis_spot,
                        basis_futures,
                        int(days_to_expiry),
                    ),
                )

            eth_btc_ratio = _safe_float(
                market.get("price_btc")
            )
            eth_price = _safe_float(
                market.get("price_usd")
            )

            if (
                eth_btc_ratio is not None
                and eth_price is not None
            ):
                btc_price = (
                    eth_price
                    / eth_btc_ratio
                    if eth_btc_ratio > 0
                    else None
                )

                conn.execute(
                    """
                    INSERT OR REPLACE INTO eth_btc_ratio_history
                        (
                            date,
                            eth_btc_ratio,
                            eth_price,
                            btc_price
                        )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        today,
                        eth_btc_ratio,
                        eth_price,
                        btc_price,
                    ),
                )

            gas_gwei = _safe_float(
                gas.get("gas_gwei")
            )

            if gas_gwei is not None:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO eth_gas_history
                        (date, gas_gwei)
                    VALUES (?, ?)
                    """,
                    (
                        today,
                        gas_gwei,
                    ),
                )

            conn.commit()

    except Exception as exc:
        print(
            f"[eth] daily ETH history write failed: {exc}"
        )

    try:
        mainnet = _safe_float(
            mainnet_tvl.get("tvl_usd")
        )
        l2 = _safe_float(
            l2_tvl.get("total_l2_tvl")
        )
        dex_24h = _safe_float(
            dex.get("dex_volume_24h")
        )

        if (
            mainnet is not None
            or l2 is not None
            or dex_24h is not None
        ):
            with sqlite3.connect(
                ETH_TVL_DB_PATH
            ) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO eth_tvl_history
                        (
                            date,
                            mainnet_tvl,
                            l2_tvl,
                            dex_volume_24h
                        )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        today,
                        mainnet,
                        l2,
                        dex_24h,
                    ),
                )
                conn.commit()

    except Exception as exc:
        print(
            f"[eth] TVL history write failed: {exc}"
        )


# ── Formatters ────────────────────────────────────────────────────────────────

def format_eth_price_move(
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

    if abs_24h > 7:
        alert, level = (
            "Extreme move",
            "extreme",
        )
    elif abs_24h > 4:
        alert, level = (
            "Large move",
            "notable",
        )
    elif abs_24h > 2.5:
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


def format_eth_volume(
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


def format_eth_funding(
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

    if pct_8h > 0.06:
        alert, level = (
            "Extreme leverage",
            "extreme",
        )
    elif pct_8h > 0.035:
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


def format_eth_open_interest(
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
        and d7_change > 0.25
    ):
        alert, level = (
            "Extreme build-up",
            "extreme",
        )
    elif (
        d7_change is not None
        and d7_change > 0.15
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


def format_eth_cme_basis(
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
                    "Yahoo Finance ETH=F · "
                    "shared core cache"
                ),
            ),
            raw.get("error", ""),
        )

    if basis_pct < 0:
        alert, level = (
            "Backwardation",
            "extreme",
        )
    elif basis_pct < 3:
        alert, level = (
            "Compressed",
            "notable",
        )
    elif basis_pct > 18:
        alert, level = (
            "Extreme carry",
            "extreme",
        )
    elif basis_pct > 12:
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
        "percentile": min(
            100,
            max(
                0,
                basis_pct / 20 * 100,
            ),
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
                "Yahoo Finance ETH=F · "
                "shared core cache"
            ),
        ),
    }


def format_eth_defi_tvl(
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
        and percentile > 70
    ):
        alert, level = (
            "TVL acceleration",
            "notable",
        )
    elif (
        d7_change is not None
        and d7_change
        < -0.08 * tvl
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


def format_eth_l2_tvl(
    raw: dict,
) -> dict:
    total = _safe_float(
        raw.get("total_l2_tvl")
    )

    if total is None:
        return _unavailable(
            "l2_tvl",
            raw.get(
                "source",
                "DeFiLlama",
            ),
            raw.get("error", ""),
        )

    chains = raw.get(
        "chains",
        [],
    ) or []

    top_labels = [
        chain.get("name", "")
        for chain in chains[:3]
        if chain.get("name")
    ]

    return {
        "current": _fmt_usd(total),
        "d7": "—",
        "vs30d": "—",
        "percentile": 50,
        "alert": "—",
        "level": "none",
        "pattern": (
            "Tracked L2 DeFi TVL · "
            + " + ".join(top_labels)
            if top_labels
            else "Tracked Ethereum L2 DeFi TVL"
        ),
        "source": raw.get(
            "source",
            "DeFiLlama",
        ),
    }


def format_eth_staking(
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
        or not validators
    ):
        return _unavailable(
            "staking_rate",
            raw.get(
                "source",
                "beaconcha.in",
            ),
            raw.get("error", ""),
        )

    if staking_rate > 35:
        alert, level = (
            "Very high lock-up",
            "notable",
        )
    elif staking_rate < 20:
        alert, level = (
            "Low participation",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    return {
        "current": (
            f"{staking_rate:.1f}%"
        ),
        "d7": "—",
        "vs30d": "—",
        "percentile": min(
            100,
            max(
                0,
                staking_rate
                / 40
                * 100,
            ),
        ),
        "alert": alert,
        "level": level,
        "pattern": (
            f"{int(validators):,} active validators · "
            f"{raw.get('staked_eth', '—')}M ETH"
        ),
        "source": raw.get(
            "source",
            "beaconcha.in",
        ),
    }


def format_eth_btc_ratio(
    price_btc: Optional[float],
) -> dict:
    if price_btc is None:
        return _unavailable(
            "eth_btc_ratio",
            "CoinGecko",
        )

    reference = [
        0.02,
        0.025,
        0.03,
        0.035,
        0.04,
        0.045,
        0.05,
        0.055,
        0.06,
        0.07,
        0.08,
    ]

    percentile = _percentile(
        price_btc,
        reference,
    )

    if price_btc > 0.065:
        alert, level = (
            "Alt season signal",
            "notable",
        )
    elif price_btc < 0.025:
        alert, level = (
            "BTC dominance extreme",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    if price_btc > 0.055:
        regime = "Alt season territory"
    elif price_btc < 0.03:
        regime = "BTC dominance"
    else:
        regime = "Neutral zone"

    return {
        "current": f"{price_btc:.5f}",
        "d7": "—",
        "vs30d": "—",
        "percentile": percentile,
        "alert": alert,
        "level": level,
        "pattern": regime,
        "source": "CoinGecko",
    }


def format_eth_gas(
    raw: dict,
) -> dict:
    gas_gwei = _safe_float(
        raw.get("gas_gwei")
    )

    if gas_gwei is None:
        return _unavailable(
            "gas_price",
            raw.get(
                "source",
                "Ethereum RPC",
            ),
            raw.get("error", ""),
        )

    if gas_gwei > 80:
        alert, level = (
            "Congestion — high demand",
            "extreme",
        )
    elif gas_gwei > 30:
        alert, level = (
            "Elevated activity",
            "notable",
        )
    elif gas_gwei < 3:
        alert, level = (
            "Very low — network idle",
            "notable",
        )
    else:
        alert, level = (
            "—",
            "none",
        )

    return {
        "current": f"{gas_gwei:.1f} gwei",
        "d7": "—",
        "vs30d": "—",
        "percentile": min(
            100,
            max(
                0,
                gas_gwei,
            ),
        ),
        "alert": alert,
        "level": level,
        "pattern": "Live Ethereum gas price",
        "source": raw.get(
            "source",
            "Ethereum RPC",
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

    staking = _get_staking(
        _safe_float(
            market.get(
                "circulating_supply"
            )
        )
    )
    gas = _get_gas()

    basis = _cached_source(
        "cme_basis",
        lambda: fetch_eth_cme_basis(
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
        market=market,
        gas=gas,
        mainnet_tvl=defi["mainnet"],
        l2_tvl=defi["l2"],
        dex=defi["dex"],
    )

    return {
        "price_move": (
            format_eth_price_move(
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
            format_eth_volume(
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
            format_eth_funding(
                funding_rate
            )
        ),
        "open_interest": (
            format_eth_open_interest(
                oi_usd
            )
        ),
        "cme_basis": (
            format_eth_cme_basis(
                basis
            )
        ),
        "defi_tvl": (
            format_eth_defi_tvl(
                defi["mainnet"]
            )
        ),
        "l2_tvl": (
            format_eth_l2_tvl(
                defi["l2"]
            )
        ),
        "staking_rate": (
            format_eth_staking(
                staking
            )
        ),
        "eth_btc_ratio": (
            format_eth_btc_ratio(
                _safe_float(
                    market.get(
                        "price_btc"
                    )
                )
            )
        ),
        "gas_price": (
            format_eth_gas(
                gas
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
    if not ETH_OVERRIDES.exists():
        return {}

    try:
        data = json.loads(
            ETH_OVERRIDES.read_text(
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
    tmp = ETH_OVERRIDES.with_name(
        f".{ETH_OVERRIDES.name}."
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
        ETH_OVERRIDES,
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

@eth_router.get("/")
def eth_root():
    return {
        "service": "eth-dashboard",
        "status": "ok",
        "version": "0.3.0",
    }


@eth_router.get("/metrics")
def eth_metrics():
    """
    All ETH metric cards.

    Overrides are merged into the underlying metric values rather than merely
    being flagged as active.
    """
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


@eth_router.get("/price")
def eth_price():
    market = _get_market_bundle()[
        "market"
    ]

    return {
        "price": _safe_float(
            market.get("price_usd")
        ),
        "price_btc": _safe_float(
            market.get("price_btc")
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


@eth_router.get("/tvl")
def eth_tvl():
    defi = _get_defi_bundle()

    return {
        "mainnet": defi["mainnet"],
        "l2": defi["l2"],
        "protocols": defi[
            "protocols"
        ][:8],
        "dex": defi["dex"],
    }


@eth_router.get("/staking")
def eth_staking():
    market = _get_market_bundle()[
        "market"
    ]

    return _get_staking(
        _safe_float(
            market.get(
                "circulating_supply"
            )
        )
    )


@eth_router.get("/gas")
def eth_gas():
    return _get_gas()


@eth_router.get("/cme-basis")
def eth_cme_basis():
    market = _get_market_bundle()[
        "market"
    ]

    raw = fetch_eth_cme_basis(
        _safe_float(
            market.get("price_usd")
        )
    )

    metric = format_eth_cme_basis(
        raw
    )

    return {
        **metric,
        **raw,
    }


@eth_router.get("/structural")
def eth_structural():
    market = _get_market_bundle()[
        "market"
    ]
    defi = _get_defi_bundle()

    staking = _get_staking(
        _safe_float(
            market.get(
                "circulating_supply"
            )
        )
    )
    gas = _get_gas()

    return {
        "eth_btc_ratio": _safe_float(
            market.get("price_btc")
        ),
        "staking_rate_pct": _safe_float(
            staking.get(
                "staking_rate_pct"
            )
        ),
        "staked_eth_M": _safe_float(
            staking.get(
                "staked_eth"
            )
        ),
        "active_validators": (
            staking.get(
                "active_validators"
            )
        ),
        "l2_total_tvl": _safe_float(
            defi["l2"].get(
                "total_l2_tvl"
            )
        ),
        "l2_chains": defi["l2"].get(
            "chains",
            [],
        ),
        "gas_gwei": _safe_float(
            gas.get("gas_gwei")
        ),
        "gas_source": gas.get(
            "source"
        ),
        "staking_source": staking.get(
            "source"
        ),
        "burn_note": (
            "Live ETH burn is not wired yet; "
            "no synthetic burn value shown."
        ),
        "etf_note": (
            "ETH ETF flow is not wired into "
            "this route yet."
        ),
    }


@eth_router.get("/summary")
def eth_summary():
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


@eth_router.get("/manual-override")
def eth_get_overrides():
    return _load_overrides()


@eth_router.post("/manual-override")
def eth_set_override(
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


@eth_router.delete(
    "/manual-override/{metric}"
)
def eth_clear_override(
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


@eth_router.get("/db/summary")
def eth_db_summary():
    results = {}

    for name, path in [
        (
            "eth_history",
            ETH_DB_PATH,
        ),
        (
            "eth_tvl_history",
            ETH_TVL_DB_PATH,
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
