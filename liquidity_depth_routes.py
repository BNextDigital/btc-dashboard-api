"""
liquidity_depth_routes.py — Spot Depth & Liquidation Cascade Risk

Answers:
    "If nearby liquidation clusters trigger, does spot have the depth to absorb
    the forced flow?"

Phase 3 architecture:
- Live spot order books are fetched only when the disposable collector runs.
- Open interest is reused from the persisted oi_history.db populated by /metrics.
- OI/funding alert levels are reused from the latest /metrics snapshot.
- No duplicate CoinGecko derivatives request is made here.
- Depth history is persisted in SQLite so it survives collector process exits.

DATA SOURCES:
- Binance public REST, with OKX fallback
- Coinbase public REST
- Kraken public REST
- CoinGlass liquidation map when available
- oi_history.db for latest known BTC open interest
- latest_snapshot.json /metrics for leverage alert classifications
"""

from __future__ import annotations

import os
import sqlite3
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from fastapi import APIRouter

from oi_history import get_latest_snapshot as get_latest_oi_snapshot
from shared.snapshot_store import get_snapshot_route


liquidity_router = APIRouter(prefix="/liquidity", tags=["Liquidity Depth"])


# ── Config ────────────────────────────────────────────────────────────────────

COINGLASS_KEY = os.getenv("COINGLASS_API_KEY", "")
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEPTH_HISTORY_DB_PATH = DATA_DIR / "depth_history.db"

BINANCE_MIRRORS = [
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api.binance.com",
]

OKX_BASE = "https://www.okx.com"
COINBASE_BASE = "https://api.coinbase.com"
KRAKEN_BASE = "https://api.kraken.com"
COINGLASS_BASE = "https://open-api.coinglass.com/public/v2"

# Reliability haircut on visible order-book depth.
DEPTH_HAIRCUT_NORMAL = 0.60
DEPTH_HAIRCUT_STRESSED = 0.40

# These caches are process-local. Under the disposable collector architecture
# they only deduplicate work within one collector process.
DEPTH_CACHE_TTL = 30
ASSESS_CACHE_TTL = 60

# Persisted depth history.
DEPTH_HISTORY_RETENTION_DAYS = 35
DEPTH_MEDIAN_WINDOW_DAYS = 30
DEPTH_HISTORY_MIN_WRITE_SECONDS = 10 * 60


_depth_cache = {"data": None, "ts": 0.0}
_assess_cache = {"data": None, "ts": 0.0}


# ── Persistent depth history ──────────────────────────────────────────────────

def _init_depth_history_db() -> None:
    with sqlite3.connect(DEPTH_HISTORY_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS depth_snapshots (
                timestamp       INTEGER PRIMARY KEY,
                bid_2pct_usd    REAL NOT NULL,
                ask_2pct_usd    REAL NOT NULL,
                venue_count     INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_depth_timestamp
            ON depth_snapshots (timestamp)
            """
        )
        conn.commit()


def _record_depth_snapshot(
    bid_2pct_usd: float,
    ask_2pct_usd: float,
    venue_count: int,
) -> None:
    """
    Persist at most one depth sample every ten minutes.

    The fast collector currently runs every 15 minutes, so this preserves one
    useful observation per run while preventing accidental duplicate inserts.
    """
    now_ts = int(time.time())

    try:
        with sqlite3.connect(DEPTH_HISTORY_DB_PATH) as conn:
            recent = conn.execute(
                """
                SELECT timestamp
                FROM depth_snapshots
                ORDER BY timestamp DESC
                LIMIT 1
                """
            ).fetchone()

            if (
                recent
                and (now_ts - int(recent[0])) < DEPTH_HISTORY_MIN_WRITE_SECONDS
            ):
                return

            conn.execute(
                """
                INSERT OR REPLACE INTO depth_snapshots
                    (timestamp, bid_2pct_usd, ask_2pct_usd, venue_count)
                VALUES (?, ?, ?, ?)
                """,
                (
                    now_ts,
                    float(bid_2pct_usd),
                    float(ask_2pct_usd),
                    int(venue_count),
                ),
            )

            cutoff = now_ts - (DEPTH_HISTORY_RETENTION_DAYS * 86400)
            conn.execute(
                "DELETE FROM depth_snapshots WHERE timestamp < ?",
                (cutoff,),
            )
            conn.commit()
    except Exception as exc:
        print(f"[liquidity] depth history write failed: {exc}")


def _depth_history_stats(
    current_2pct_usd: float,
) -> tuple[Optional[float], float, int]:
    """
    Compare current 2% bid depth with the median of persisted observations from
    the last 30 days.

    Returns:
        (current_vs_median_pct, history_coverage_days, sample_count)

    The ratio is withheld until at least 24 hours of persisted observations
    exist so a handful of samples cannot masquerade as meaningful history.
    """
    now_ts = int(time.time())
    cutoff = now_ts - (DEPTH_MEDIAN_WINDOW_DAYS * 86400)

    try:
        with sqlite3.connect(DEPTH_HISTORY_DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT timestamp, bid_2pct_usd
                FROM depth_snapshots
                WHERE timestamp >= ?
                  AND bid_2pct_usd > 0
                ORDER BY timestamp ASC
                """,
                (cutoff,),
            ).fetchall()
    except Exception as exc:
        print(f"[liquidity] depth history read failed: {exc}")
        return None, 0.0, 0

    if not rows:
        return None, 0.0, 0

    coverage_days = max(
        0.0,
        (int(rows[-1][0]) - int(rows[0][0])) / 86400,
    )
    sample_count = len(rows)

    if coverage_days < 1.0 or sample_count < 4:
        return None, round(coverage_days, 2), sample_count

    values = [float(row[1]) for row in rows if float(row[1]) > 0]
    if not values:
        return None, round(coverage_days, 2), sample_count

    med = statistics.median(values)
    if med <= 0:
        return None, round(coverage_days, 2), sample_count

    pct = round((current_2pct_usd / med) * 100, 1)
    return pct, round(coverage_days, 2), sample_count


_init_depth_history_db()


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _safe_get(
    url: str,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    timeout: int = 8,
) -> Optional[dict]:
    try:
        response = requests.get(
            url,
            headers=headers or {},
            params=params or {},
            timeout=timeout,
        )

        if response.status_code == 429:
            print(f"[liquidity] Rate limited: {url}")
            return None

        response.raise_for_status()
        return response.json()

    except Exception as exc:
        print(f"[liquidity] GET {url} failed: {exc}")
        return None


# ── Order book fetchers ───────────────────────────────────────────────────────

def _fetch_binance_depth() -> Optional[dict]:
    """
    Binance L2 order book — top 500 levels each side.

    Railway IPs may be geo-blocked. Try all mirrors, then transparently fall
    back to OKX.
    """
    last_err = None

    for mirror in BINANCE_MIRRORS:
        try:
            response = requests.get(
                f"{mirror}/api/v3/depth",
                params={"symbol": "BTCUSDT", "limit": 500},
                timeout=8,
            )

            if response.status_code == 451:
                print(
                    f"[liquidity] Binance {mirror} → 451 geo-block, "
                    "trying next mirror"
                )
                last_err = "geo_block"
                continue

            if response.status_code == 429:
                print(
                    f"[liquidity] Binance {mirror} → 429 rate limit, "
                    "trying next mirror"
                )
                last_err = "rate_limited"
                continue

            if response.status_code == 403:
                print(
                    f"[liquidity] Binance {mirror} → 403 forbidden, "
                    "trying next mirror"
                )
                last_err = "forbidden"
                continue

            response.raise_for_status()
            data = response.json()

            if not data.get("bids"):
                continue

            print(f"[liquidity] Binance OK via {mirror}")
            return {
                "bids": [
                    (float(price), float(qty))
                    for price, qty in data.get("bids", [])
                ],
                "asks": [
                    (float(price), float(qty))
                    for price, qty in data.get("asks", [])
                ],
                "venue": "Binance",
            }

        except requests.exceptions.Timeout:
            print(f"[liquidity] Binance {mirror} → timeout")
            last_err = "timeout"

        except Exception as exc:
            print(f"[liquidity] Binance {mirror} → {exc}")
            last_err = str(exc)

    print(
        "[liquidity] All Binance mirrors failed "
        f"(last: {last_err}), trying OKX fallback"
    )
    return _fetch_okx_depth()


def _fetch_okx_depth() -> Optional[dict]:
    """
    OKX public order book — fallback when Binance is unavailable.
    """
    try:
        response = requests.get(
            f"{OKX_BASE}/api/v5/market/books",
            params={"instId": "BTC-USDT", "sz": "400"},
            timeout=8,
        )
        response.raise_for_status()

        data = response.json()
        if data.get("code") != "0":
            print(f"[liquidity] OKX depth error: {data.get('msg')}")
            return None

        books = data.get("data", [{}])[0]

        bids = [
            (float(row[0]), float(row[1]))
            for row in books.get("bids", [])
        ]
        asks = [
            (float(row[0]), float(row[1]))
            for row in books.get("asks", [])
        ]

        if not bids:
            return None

        print("[liquidity] OKX depth fallback: OK")
        return {
            "bids": bids,
            "asks": asks,
            "venue": "OKX",
        }

    except Exception as exc:
        print(f"[liquidity] OKX fallback failed: {exc}")
        return None


def _fetch_coinbase_depth() -> Optional[dict]:
    """
    Coinbase Advanced Trade public product book.
    """
    data = _safe_get(
        f"{COINBASE_BASE}/api/v3/brokerage/market/product_book",
        params={"product_id": "BTC-USD", "limit": 250},
    )

    if not data or "pricebook" not in data:
        return None

    book = data["pricebook"]

    return {
        "bids": [
            (float(row["price"]), float(row["size"]))
            for row in book.get("bids", [])
        ],
        "asks": [
            (float(row["price"]), float(row["size"]))
            for row in book.get("asks", [])
        ],
        "venue": "Coinbase",
    }


def _fetch_kraken_depth() -> Optional[dict]:
    """
    Kraken public order book — top 500 levels.
    """
    data = _safe_get(
        f"{KRAKEN_BASE}/0/public/Depth",
        params={"pair": "XBTUSD", "count": 500},
    )

    if not data or data.get("error"):
        return None

    result = data.get("result", {})
    pair_data = next(iter(result.values()), {}) if result else {}

    return {
        "bids": [
            (float(price), float(qty))
            for price, qty, _ in pair_data.get("bids", [])
        ],
        "asks": [
            (float(price), float(qty))
            for price, qty, _ in pair_data.get("asks", [])
        ],
        "venue": "Kraken",
    }


def _fetch_coinglass_liquidation_map() -> Optional[dict]:
    """
    CoinGlass liquidation heatmap.

    When unavailable, the assessment falls back to the latest persisted OI
    rather than making another derivatives request.
    """
    if not COINGLASS_KEY:
        return None

    data = _safe_get(
        f"{COINGLASS_BASE}/liquidation/map",
        headers={"coinglassSecret": COINGLASS_KEY},
        params={"symbol": "BTC", "interval": "4h"},
    )

    if not data or data.get("code") != "0":
        print(
            "[liquidity] CoinGlass liquidation map: "
            f"{data.get('msg') if data else 'no response'}"
        )
        return None

    return data.get("data")


# ── Reused leverage context ───────────────────────────────────────────────────

def _get_leverage_context() -> dict:
    """
    Reuse leverage data already collected elsewhere.

    OI:
        latest persisted raw OI from oi_history.db

    Alert classifications:
        latest /metrics snapshot, which the fast collector writes before
        /liquidity/depth runs.

    This intentionally makes no network request.
    """
    latest_oi = None
    metrics = {}

    try:
        latest_oi = get_latest_oi_snapshot()
    except Exception as exc:
        print(f"[liquidity] latest OI history read failed: {exc}")

    try:
        snapshot_metrics = get_snapshot_route("/metrics")
        if isinstance(snapshot_metrics, dict):
            metrics = snapshot_metrics
    except Exception as exc:
        print(f"[liquidity] metrics snapshot read failed: {exc}")

    oi_metric = metrics.get("open_interest", {})
    funding_metric = metrics.get("funding", {})

    if not isinstance(oi_metric, dict):
        oi_metric = {}
    if not isinstance(funding_metric, dict):
        funding_metric = {}

    oi_usd = 0.0
    oi_timestamp = None

    if isinstance(latest_oi, dict):
        try:
            raw_oi = latest_oi.get("oi_usd")
            if raw_oi is not None:
                oi_usd = max(0.0, float(raw_oi))
            oi_timestamp = latest_oi.get("timestamp")
        except (TypeError, ValueError):
            oi_usd = 0.0

    oi_age_seconds = None
    if isinstance(oi_timestamp, (int, float)):
        oi_age_seconds = max(0, int(time.time() - float(oi_timestamp)))

    return {
        "oi_usd": oi_usd,
        "oi_alert_level": oi_metric.get("alert_level", "none"),
        "funding_alert_level": funding_metric.get(
            "alert_level",
            "none",
        ),
        "oi_timestamp": oi_timestamp,
        "oi_age_seconds": oi_age_seconds,
        "source": "oi_history + metrics snapshot",
    }


# ── Depth aggregation ─────────────────────────────────────────────────────────

def _aggregate_depth(books: list[dict], spot_price: float) -> dict:
    """
    Aggregate bid and ask depth from multiple venues at 0.5%, 1%, and 2%
    around spot.
    """
    bands_pct = [0.005, 0.010, 0.020]

    result = {
        "spot_price": spot_price,
        "bid_depth": {
            f"{int(band * 1000) / 10}": {
                "usd": 0.0,
                "venues": {},
            }
            for band in bands_pct
        },
        "ask_depth": {
            f"{int(band * 1000) / 10}": {
                "usd": 0.0,
                "venues": {},
            }
            for band in bands_pct
        },
        "venue_totals": {},
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    for book in books:
        if not book:
            continue

        venue = book["venue"]
        venue_bid_2pct = 0.0
        venue_ask_2pct = 0.0

        for price, qty in book.get("bids", []):
            drop_pct = (spot_price - price) / spot_price
            if drop_pct < 0:
                continue

            usd_val = price * qty

            for band_pct in bands_pct:
                if drop_pct <= band_pct:
                    band_key = f"{int(band_pct * 1000) / 10}"
                    band = result["bid_depth"][band_key]
                    band["usd"] += usd_val
                    band["venues"][venue] = (
                        band["venues"].get(venue, 0.0) + usd_val
                    )

            if drop_pct <= 0.020:
                venue_bid_2pct += usd_val

        for price, qty in book.get("asks", []):
            rise_pct = (price - spot_price) / spot_price
            if rise_pct < 0:
                continue

            usd_val = price * qty

            for band_pct in bands_pct:
                if rise_pct <= band_pct:
                    band_key = f"{int(band_pct * 1000) / 10}"
                    band = result["ask_depth"][band_key]
                    band["usd"] += usd_val
                    band["venues"][venue] = (
                        band["venues"].get(venue, 0.0) + usd_val
                    )

            if rise_pct <= 0.020:
                venue_ask_2pct += usd_val

        result["venue_totals"][venue] = {
            "bid_2pct_usd": venue_bid_2pct,
            "ask_2pct_usd": venue_ask_2pct,
        }

    return result


def _venue_concentration(
    venue_totals: dict,
    side: str = "bid_2pct_usd",
) -> float:
    """
    Largest single venue's share of aggregate depth.
    """
    totals = [
        float(values.get(side, 0.0))
        for values in venue_totals.values()
        if float(values.get(side, 0.0)) > 0
    ]

    total = sum(totals)
    if not totals or total <= 0:
        return 1.0

    return max(totals) / total


# ── Liquidation estimate ──────────────────────────────────────────────────────

def _estimate_liquidation_exposure(
    liq_map_data,
    spot_price: float,
    oi_usd: float,
) -> dict:
    """
    Estimate forced liquidation flow within 2% of spot.

    Primary:
        CoinGlass liquidation levels when the response can be parsed.

    Fallback:
        persisted BTC OI:
          downside long-liquidation heuristic = 15% of OI
          upside short-liquidation heuristic = 8% of OI

    If neither source is available, the estimate is explicitly unavailable.
    """
    long_liq_usd = 0.0
    short_liq_usd = 0.0
    source = "unavailable"

    if liq_map_data and isinstance(liq_map_data, dict):
        try:
            price_data = liq_map_data.get(
                "liquidationLevels",
                liq_map_data.get("data", []),
            )

            if isinstance(price_data, list):
                for entry in price_data:
                    if not isinstance(entry, dict):
                        continue

                    price = float(entry.get("price", 0) or 0)
                    longs = float(
                        entry.get(
                            "longLiquidationUsd",
                            entry.get("l", 0),
                        )
                        or 0
                    )
                    shorts = float(
                        entry.get(
                            "shortLiquidationUsd",
                            entry.get("s", 0),
                        )
                        or 0
                    )

                    if price <= 0:
                        continue

                    distance = abs(spot_price - price) / spot_price

                    if distance <= 0.020:
                        long_liq_usd += max(0.0, longs)
                        short_liq_usd += max(0.0, shorts)

                if long_liq_usd > 0 or short_liq_usd > 0:
                    source = "CoinGlass heatmap"

        except Exception as exc:
            print(
                "[liquidity] Liquidation map parse error: "
                f"{exc}"
            )

    if (
        long_liq_usd <= 0
        and short_liq_usd <= 0
        and oi_usd > 0
    ):
        long_liq_usd = oi_usd * 0.15
        short_liq_usd = oi_usd * 0.08
        source = "OI estimate (heuristic)"

    return {
        "long_liq_usd": long_liq_usd,
        "short_liq_usd": short_liq_usd,
        "net_downside": long_liq_usd,
        "net_upside": short_liq_usd,
        "source": source,
        "available": long_liq_usd > 0 or short_liq_usd > 0,
    }


# ── Slippage estimate ─────────────────────────────────────────────────────────

def _estimate_slippage(
    forced_flow_usd: float,
    bid_depth: dict,
) -> str:
    """
    Estimate price-impact band by comparing forced sell flow with cumulative
    visible bid depth.
    """
    if forced_flow_usd <= 0:
        return "—"

    depth_05 = bid_depth.get("0.5", {}).get("usd", 0.0)
    depth_10 = bid_depth.get("1.0", {}).get("usd", 0.0)
    depth_20 = bid_depth.get("2.0", {}).get("usd", 0.0)

    if forced_flow_usd < depth_05:
        return "< 0.5%"

    if forced_flow_usd < depth_10:
        return "0.5–1.0%"

    if forced_flow_usd < depth_20:
        return "1.0–2.0%"

    overflow = forced_flow_usd - depth_20
    overflow_pct = (overflow / forced_flow_usd) * 3
    total_est = 2.0 + overflow_pct
    return f"~{total_est:.1f}%"


# ── Composite cascade risk ────────────────────────────────────────────────────

def _cascade_risk_label(
    adjusted_coverage: float,
    oi_alert_level: str,
    funding_alert_level: str,
    netflow_alert: str,
) -> tuple[str, str]:
    """
    Return (label, level) for cascade risk.
    """
    leverage_stressed = (
        oi_alert_level in ("notable", "extreme")
        or funding_alert_level in ("notable", "extreme")
    )
    supply_pressure = "inflow" in (netflow_alert or "").lower()

    if adjusted_coverage >= 1.5:
        base_label, base_level = (
            "Deep — strong absorption capacity",
            "none",
        )
    elif adjusted_coverage >= 1.0:
        base_label, base_level = (
            "Adequate depth",
            "none",
        )
    elif adjusted_coverage >= 0.75:
        base_label, base_level = (
            "Thin — elevated cascade risk",
            "notable",
        )
    elif adjusted_coverage >= 0.5:
        base_label, base_level = (
            "Fragile — high cascade risk",
            "extreme",
        )
    else:
        base_label, base_level = (
            "Critical — severe cascade potential",
            "extreme",
        )

    if base_level == "none" and leverage_stressed:
        return "Adequate depth · leverage elevated", "none"

    if (
        base_level == "notable"
        and leverage_stressed
        and supply_pressure
    ):
        return (
            "Thin depth + crowded leverage + exchange inflow "
            "— cascade risk HIGH",
            "extreme",
        )

    if base_level == "notable" and leverage_stressed:
        return (
            "Thin depth + crowded leverage — monitor closely",
            "extreme",
        )

    if base_level == "extreme" and leverage_stressed:
        return (
            "Fragile depth + crowded leverage + supply pressure "
            "— EXTREME",
            "extreme",
        )

    return base_label, base_level


# ── Format helpers ────────────────────────────────────────────────────────────

def _fmt_usd(value: Optional[float]) -> str:
    if value is None or value <= 0:
        return "—"

    if value >= 1e12:
        return f"${value / 1e12:.2f}T"

    if value >= 1e9:
        return f"${value / 1e9:.2f}B"

    if value >= 1e6:
        return f"${value / 1e6:.0f}M"

    if value >= 1e3:
        return f"${value / 1e3:.0f}k"

    return f"${value:,.0f}"


def _fmt_ratio(ratio: Optional[float]) -> str:
    if ratio is None:
        return "—"
    return f"{ratio:.2f}x"


# ── Core builder ──────────────────────────────────────────────────────────────

def _build_depth_assessment(
    context: Optional[dict] = None,
) -> dict:
    """
    Build the full spot-depth / cascade-risk assessment.

    If context is omitted, reuse the latest persisted leverage context.
    """
    ctx = context if isinstance(context, dict) else _get_leverage_context()

    # ── Step 1: Fetch order books
    primary_book = _fetch_binance_depth()
    coinbase_book = _fetch_coinbase_depth()
    kraken_book = _fetch_kraken_depth()

    books_fetched = [
        book
        for book in (
            primary_book,
            coinbase_book,
            kraken_book,
        )
        if book
    ]

    venues_up = [book["venue"] for book in books_fetched]

    if not books_fetched:
        return {
            "error": "All order book sources unavailable",
            "alert_level": "none",
        }

    # ── Step 2: Determine spot price
    spot_price = 0.0

    if (
        primary_book
        and primary_book.get("bids")
        and primary_book.get("asks")
    ):
        best_bid = primary_book["bids"][0][0]
        best_ask = primary_book["asks"][0][0]
        spot_price = (best_bid + best_ask) / 2

    elif coinbase_book and coinbase_book.get("bids"):
        spot_price = coinbase_book["bids"][0][0]

    elif kraken_book and kraken_book.get("bids"):
        spot_price = kraken_book["bids"][0][0]

    if spot_price <= 0:
        return {
            "error": "Could not determine spot price",
            "alert_level": "none",
        }

    # ── Step 3: Aggregate depth
    agg = _aggregate_depth(books_fetched, spot_price)

    bid_05 = agg["bid_depth"]["0.5"]["usd"]
    bid_10 = agg["bid_depth"]["1.0"]["usd"]
    bid_20 = agg["bid_depth"]["2.0"]["usd"]
    ask_20 = agg["ask_depth"]["2.0"]["usd"]

    # ── Step 4: Reused leverage context
    oi_usd = max(0.0, float(ctx.get("oi_usd", 0.0) or 0.0))
    oi_alert_level = ctx.get("oi_alert_level", "none")
    funding_alert_level = ctx.get(
        "funding_alert_level",
        "none",
    )
    netflow_alert = ""

    # ── Step 5: Liquidation exposure
    liq_map = _fetch_coinglass_liquidation_map()
    liq = _estimate_liquidation_exposure(
        liq_map,
        spot_price,
        oi_usd,
    )

    forced_flow_usd = float(liq["net_downside"] or 0.0)

    # ── Step 6: Reliability haircut
    stressed = (
        oi_alert_level in ("notable", "extreme")
        or funding_alert_level in ("notable", "extreme")
    )

    haircut = (
        DEPTH_HAIRCUT_STRESSED
        if stressed
        else DEPTH_HAIRCUT_NORMAL
    )

    visible_depth_usd = bid_20
    adjusted_depth_usd = visible_depth_usd * haircut

    # ── Step 7: Coverage ratios
    if forced_flow_usd > 0:
        depth_coverage_ratio: Optional[float] = (
            visible_depth_usd / forced_flow_usd
        )
        adjusted_coverage: Optional[float] = (
            adjusted_depth_usd / forced_flow_usd
        )
    else:
        depth_coverage_ratio = None
        adjusted_coverage = None

    # ── Step 8: Slippage estimate
    slippage_est = _estimate_slippage(
        forced_flow_usd,
        agg["bid_depth"],
    )

    # ── Step 9: Venue concentration
    concentration = _venue_concentration(
        agg["venue_totals"],
    )

    # ── Step 10: Persist depth history
    _record_depth_snapshot(
        bid_20,
        ask_20,
        len(venues_up),
    )

    (
        depth_vs_median,
        depth_history_days,
        depth_history_samples,
    ) = _depth_history_stats(bid_20)

    # ── Step 11: Cascade risk label
    if adjusted_coverage is None:
        cascade_label = (
            "Unavailable — no liquidation estimate"
        )
        cascade_level = "none"
    else:
        cascade_label, cascade_level = _cascade_risk_label(
            adjusted_coverage,
            oi_alert_level,
            funding_alert_level,
            netflow_alert,
        )

    # ── Step 12: Top-level alert
    if adjusted_coverage is None:
        alert = "No liquidation estimate"
        alert_level = "none"

    elif adjusted_coverage < 0.75 or cascade_level == "extreme":
        alert = "Extreme cascade risk"
        alert_level = "extreme"

    elif adjusted_coverage < 1.0 or cascade_level == "notable":
        alert = "Elevated cascade risk"
        alert_level = "notable"

    elif (
        depth_vs_median is not None
        and depth_vs_median < 60
    ):
        alert = "Depth thinning vs median"
        alert_level = "notable"

    else:
        alert = "—"
        alert_level = "none"

    rounded_visible_coverage = (
        round(depth_coverage_ratio, 2)
        if depth_coverage_ratio is not None
        else None
    )
    rounded_adjusted_coverage = (
        round(adjusted_coverage, 2)
        if adjusted_coverage is not None
        else None
    )

    return {
        # ── Standard metric-card schema
        "name": "Spot Depth",
        "category": "Liquidity",
        "current": _fmt_ratio(adjusted_coverage),
        "current_dir": (
            "flat"
            if adjusted_coverage is None
            else (
                "up"
                if adjusted_coverage >= 1.0
                else "down"
            )
        ),
        "d7": "—",
        "vs30d": "—",
        "percentile": None,
        "alert": alert,
        "alert_level": alert_level,
        "pattern": cascade_label,

        # ── Depth-specific fields
        "spot_price_usd": round(spot_price, 2),
        "bid_depth_0_5pct_usd": _fmt_usd(bid_05),
        "bid_depth_1_0pct_usd": _fmt_usd(bid_10),
        "bid_depth_2_0pct_usd": _fmt_usd(bid_20),
        "ask_depth_2_0pct_usd": _fmt_usd(ask_20),
        "visible_depth_usd": _fmt_usd(visible_depth_usd),
        "adjusted_depth_usd": _fmt_usd(adjusted_depth_usd),
        "depth_haircut_pct": f"{int(haircut * 100)}%",
        "haircut_reason": (
            "stressed"
            if stressed
            else "normal"
        ),

        "depth_coverage_ratio": rounded_visible_coverage,
        "adjusted_coverage": rounded_adjusted_coverage,

        "liquidation_estimate_usd": (
            _fmt_usd(forced_flow_usd)
            if forced_flow_usd > 0
            else "—"
        ),
        "liquidation_source": liq["source"],
        "oi_usd": (
            _fmt_usd(oi_usd)
            if oi_usd > 0
            else "—"
        ),
        "oi_age_seconds": ctx.get("oi_age_seconds"),
        "leverage_context_source": ctx.get(
            "source",
            "unknown",
        ),

        "slippage_estimate": slippage_est,
        "depth_vs_median_pct": depth_vs_median,
        "depth_history_days": depth_history_days,
        "depth_history_samples": depth_history_samples,
        "venue_concentration_pct": round(
            concentration * 100,
            1,
        ),
        "venues_online": venues_up,

        "cascade_risk_label": cascade_label,
        "cascade_risk_level": cascade_level,

        "oi_alert_level": oi_alert_level,
        "funding_alert_level": funding_alert_level,

        "venue_breakdown": {
            venue: {
                "bid_2pct_usd": _fmt_usd(
                    totals["bid_2pct_usd"]
                ),
                "ask_2pct_usd": _fmt_usd(
                    totals["ask_2pct_usd"]
                ),
                "share_pct": (
                    round(
                        totals["bid_2pct_usd"]
                        / visible_depth_usd
                        * 100,
                        1,
                    )
                    if visible_depth_usd > 0
                    else 0.0
                ),
            }
            for venue, totals in agg["venue_totals"].items()
        },

        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Cached builder ────────────────────────────────────────────────────────────

def _build_assessment_cached(
    context: Optional[dict] = None,
) -> dict:
    now = time.time()

    if (
        _assess_cache["data"]
        and (now - _assess_cache["ts"]) < ASSESS_CACHE_TTL
    ):
        return _assess_cache["data"]

    data = _build_depth_assessment(context)
    _assess_cache["data"] = data
    _assess_cache["ts"] = now
    return data


# ── Routes ────────────────────────────────────────────────────────────────────

@liquidity_router.get("/depth")
def get_liquidity_depth():
    """
    Full cascade-risk assessment.

    Reuses leverage data already collected by /metrics rather than issuing a
    second CoinGecko derivatives request.
    """
    context = _get_leverage_context()
    return _build_assessment_cached(context)


@liquidity_router.get("/orderbook")
def get_orderbook():
    """
    Raw aggregated order-book depth at 0.5%, 1%, and 2% bands.
    """
    now = time.time()

    if (
        _depth_cache["data"]
        and (now - _depth_cache["ts"]) < DEPTH_CACHE_TTL
    ):
        return _depth_cache["data"]

    primary_book = _fetch_binance_depth()
    coinbase_book = _fetch_coinbase_depth()
    kraken_book = _fetch_kraken_depth()

    books = [
        book
        for book in (
            primary_book,
            coinbase_book,
            kraken_book,
        )
        if book
    ]

    if not books:
        return {"error": "All venues unavailable"}

    spot = 0.0

    if (
        primary_book
        and primary_book.get("bids")
        and primary_book.get("asks")
    ):
        spot = (
            primary_book["bids"][0][0]
            + primary_book["asks"][0][0]
        ) / 2

    elif coinbase_book and coinbase_book.get("bids"):
        spot = coinbase_book["bids"][0][0]

    elif kraken_book and kraken_book.get("bids"):
        spot = kraken_book["bids"][0][0]

    if spot <= 0:
        return {"error": "Could not determine spot price"}

    agg = _aggregate_depth(books, spot)

    result = {
        "spot_price_usd": round(spot, 2),
        "venues": [book["venue"] for book in books],
        "bid_depth": {
            "0.5pct": _fmt_usd(
                agg["bid_depth"]["0.5"]["usd"]
            ),
            "1.0pct": _fmt_usd(
                agg["bid_depth"]["1.0"]["usd"]
            ),
            "2.0pct": _fmt_usd(
                agg["bid_depth"]["2.0"]["usd"]
            ),
            "raw": {
                "0.5pct_usd": agg["bid_depth"]["0.5"]["usd"],
                "1.0pct_usd": agg["bid_depth"]["1.0"]["usd"],
                "2.0pct_usd": agg["bid_depth"]["2.0"]["usd"],
            },
        },
        "ask_depth": {
            "0.5pct": _fmt_usd(
                agg["ask_depth"]["0.5"]["usd"]
            ),
            "1.0pct": _fmt_usd(
                agg["ask_depth"]["1.0"]["usd"]
            ),
            "2.0pct": _fmt_usd(
                agg["ask_depth"]["2.0"]["usd"]
            ),
        },
        "venue_breakdown": agg["venue_totals"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    _depth_cache["data"] = result
    _depth_cache["ts"] = now

    return result


@liquidity_router.get("/cascade-score")
def get_cascade_score():
    """
    Lightweight composite view.

    If the local assessment cache is empty, build from the same reused leverage
    context as /depth.
    """
    assessment = _build_assessment_cached(
        _get_leverage_context()
    )

    return {
        "cascade_risk_label": assessment.get(
            "cascade_risk_label",
            "—",
        ),
        "cascade_risk_level": assessment.get(
            "cascade_risk_level",
            "none",
        ),
        "adjusted_coverage": assessment.get(
            "adjusted_coverage",
        ),
        "alert": assessment.get("alert", "—"),
        "alert_level": assessment.get(
            "alert_level",
            "none",
        ),
        "slippage_estimate": assessment.get(
            "slippage_estimate",
            "—",
        ),
        "depth_vs_median_pct": assessment.get(
            "depth_vs_median_pct",
        ),
        "venues_online": assessment.get(
            "venues_online",
            [],
        ),
        "updated_at": assessment.get("updated_at"),
    }


@liquidity_router.get("/cache/flush")
def flush_liquidity_cache():
    """
    Clear process-local depth caches.

    Persisted depth history is intentionally not deleted.
    """
    global _depth_cache, _assess_cache

    _depth_cache = {"data": None, "ts": 0.0}
    _assess_cache = {"data": None, "ts": 0.0}

    return {
        "flushed": True,
        "caches": ["depth", "assessment"],
    }
