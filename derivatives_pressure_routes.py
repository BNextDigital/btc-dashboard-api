"""
derivatives_pressure_routes.py — BTC perpetual-futures pressure monitor.

Purpose:
    Answer "which side is carrying pain, building crowding, or being forced?"
    without pretending we know individual traders' entry prices, leverage,
    collateral, or P&L.

Architecture:
- Runs only inside the disposable FAST collector.
- Reuses the shared CoinGecko payload already fetched by /metrics.
- Reuses persisted OI history.
- Reuses /liquidity/depth for nearby liquidation-cluster context.
- Reads the existing leading_history.db for cumulative funding context.
- Persists tiny 15-minute pressure samples to SQLite for funding persistence.
- Makes NO new external network request.

Interpretation discipline:
- Funding payer/carry is OBSERVED from current perp funding.
- Position crowding is INFERRED from OI + funding + price.
- Forced-flow side is STRONGLY INFERRED from adverse price + falling OI.
- Liquidation heatmap data describes nearby vulnerability, NOT realized fills.
"""

from __future__ import annotations

import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter

from data_sources import get_shared_coingecko
from oi_history import get_raw_snapshots
from shared.snapshot_store import get_snapshot_route


derivatives_router = APIRouter(
    prefix="/derivatives",
    tags=["Derivatives"],
)

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

PRESSURE_DB_PATH = DATA_DIR / "derivatives_pressure_history.db"
LEADING_DB_PATH = DATA_DIR / "leading_history.db"

PRESSURE_RETENTION_DAYS = 35
PRESSURE_MIN_WRITE_SECONDS = 10 * 60

REFERENCE_EXCHANGES = {
    "Binance (Futures)",
    "Bybit (Futures)",
    "OKX (Futures)",
}


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(value)))


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _fmt_usd(value: Optional[float]) -> str:
    if value is None:
        return "—"
    sign = "-" if value < 0 else ""
    v = abs(value)
    if v >= 1e12:
        return f"{sign}${v / 1e12:.2f}T"
    if v >= 1e9:
        return f"{sign}${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"{sign}${v / 1e6:.0f}M"
    if v >= 1e3:
        return f"{sign}${v / 1e3:.0f}k"
    return f"{sign}${v:,.0f}"


def _level(score: Optional[float]) -> str:
    if score is None:
        return "unavailable"
    if score >= 75:
        return "high"
    if score >= 50:
        return "elevated"
    if score >= 25:
        return "moderate"
    return "low"


def _init_db() -> None:
    with sqlite3.connect(PRESSURE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pressure_samples (
                timestamp       INTEGER PRIMARY KEY,
                funding_pct_8h  REAL,
                oi_usd          REAL,
                price_usd       REAL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pressure_timestamp
            ON pressure_samples(timestamp)
            """
        )
        conn.commit()


_init_db()


def _record_sample(
    funding_pct_8h: Optional[float],
    oi_usd: Optional[float],
    price_usd: Optional[float],
) -> None:
    now_ts = int(time.time())

    try:
        with sqlite3.connect(PRESSURE_DB_PATH) as conn:
            recent = conn.execute(
                """
                SELECT timestamp
                FROM pressure_samples
                ORDER BY timestamp DESC
                LIMIT 1
                """
            ).fetchone()

            if (
                recent
                and now_ts - int(recent[0]) < PRESSURE_MIN_WRITE_SECONDS
            ):
                return

            conn.execute(
                """
                INSERT OR REPLACE INTO pressure_samples
                    (timestamp, funding_pct_8h, oi_usd, price_usd)
                VALUES (?, ?, ?, ?)
                """,
                (
                    now_ts,
                    funding_pct_8h,
                    oi_usd,
                    price_usd,
                ),
            )

            cutoff = now_ts - PRESSURE_RETENTION_DAYS * 86400
            conn.execute(
                "DELETE FROM pressure_samples WHERE timestamp < ?",
                (cutoff,),
            )
            conn.commit()

    except Exception as exc:
        print(f"[derivatives-pressure] sample write failed: {exc}")


def _pressure_samples(days: int = 8) -> list[dict]:
    cutoff = int(time.time()) - days * 86400
    try:
        with sqlite3.connect(PRESSURE_DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT timestamp, funding_pct_8h, oi_usd, price_usd
                FROM pressure_samples
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (cutoff,),
            ).fetchall()
        return [
            {
                "timestamp": int(row[0]),
                "funding_pct_8h": row[1],
                "oi_usd": row[2],
                "price_usd": row[3],
            }
            for row in rows
        ]
    except Exception as exc:
        print(f"[derivatives-pressure] sample read failed: {exc}")
        return []


def _latest_live_state() -> dict:
    shared = get_shared_coingecko()
    markets = shared.get("derivatives") or []
    chart = shared.get("chart") or {}

    all_oi = 0.0
    for market in markets:
        oi = _safe_float(market.get("open_interest"))
        if oi and oi > 0:
            all_oi += oi

    valid = [
        market
        for market in markets
        if market.get("market") in REFERENCE_EXCHANGES
        and market.get("index_id") == "BTC"
        and market.get("contract_type") == "perpetual"
        and _safe_float(market.get("funding_rate")) is not None
        and (_safe_float(market.get("open_interest")) or 0) > 0
    ]

    funding_pct_8h: Optional[float] = None
    exchange_rates: dict[str, float] = {}

    if valid:
        total_ref_oi = sum(
            float(market["open_interest"])
            for market in valid
        )
        if total_ref_oi > 0:
            # CoinGecko funding_rate is in percentage points per funding period.
            funding_pct_8h = (
                sum(
                    float(market["funding_rate"])
                    * float(market["open_interest"])
                    for market in valid
                )
                / total_ref_oi
            )

        for market in valid:
            exchange = (
                str(market.get("market", ""))
                .replace(" (Futures)", "")
                .replace(" Futures", "")
            )
            exchange_rates[exchange] = round(
                float(market["funding_rate"]),
                6,
            )

    price_usd: Optional[float] = None
    price_1d_pct: Optional[float] = None
    price_7d_pct: Optional[float] = None

    prices = chart.get("prices") if isinstance(chart, dict) else None
    if isinstance(prices, list) and prices:
        parsed: list[float] = []
        for row in prices:
            try:
                parsed.append(float(row[1]))
            except (TypeError, ValueError, IndexError):
                continue

        if parsed:
            price_usd = parsed[-1]
        if len(parsed) >= 2 and parsed[-2] > 0:
            price_1d_pct = (
                parsed[-1] / parsed[-2] - 1.0
            ) * 100.0
        if len(parsed) >= 8 and parsed[-8] > 0:
            price_7d_pct = (
                parsed[-1] / parsed[-8] - 1.0
            ) * 100.0

    return {
        "funding_pct_8h": funding_pct_8h,
        "exchange_rates": exchange_rates,
        "oi_usd": all_oi if all_oi > 0 else None,
        "price_usd": price_usd,
        "price_1d_pct": price_1d_pct,
        "price_7d_pct": price_7d_pct,
    }


def _nearest_oi_change(
    current_oi: Optional[float],
    seconds_ago: int,
    tolerance_seconds: int,
) -> Optional[float]:
    if current_oi is None or current_oi <= 0:
        return None

    rows = get_raw_snapshots(days=max(2, math.ceil(seconds_ago / 86400) + 2))
    if not rows:
        return None

    target = int(time.time()) - seconds_ago
    nearest = min(
        rows,
        key=lambda row: abs(int(row["timestamp"]) - target),
    )

    if abs(int(nearest["timestamp"]) - target) > tolerance_seconds:
        return None

    previous = _safe_float(nearest.get("oi_usd"))
    if previous is None or previous <= 0:
        return None

    return (current_oi / previous - 1.0) * 100.0


def _funding_history_context() -> dict:
    """
    Read the already-existing leading_history.db.

    Units in that table are funding percentage points:
      0.03 == 0.03% daily funding cost.
    """
    if not LEADING_DB_PATH.exists():
        return {
            "days_available": 0,
            "cumulative_7d_pct": None,
            "cumulative_30d_pct": None,
            "as_of": None,
        }

    try:
        with sqlite3.connect(LEADING_DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT date, daily_rate
                FROM funding_cumulative_history
                WHERE daily_rate IS NOT NULL
                ORDER BY date DESC
                LIMIT 30
                """
            ).fetchall()

        daily_rates = [
            float(row[1])
            for row in rows
            if row[1] is not None
        ]

        return {
            "days_available": len(daily_rates),
            "cumulative_7d_pct": (
                round(sum(daily_rates[:7]), 4)
                if len(daily_rates) >= 7
                else None
            ),
            "cumulative_30d_pct": (
                round(sum(daily_rates[:30]), 4)
                if len(daily_rates) >= 30
                else None
            ),
            "cumulative_available_pct": (
                round(sum(daily_rates), 4)
                if daily_rates
                else None
            ),
            "as_of": rows[0][0] if rows else None,
            "source": "leading_history.db",
        }

    except Exception as exc:
        print(f"[derivatives-pressure] funding history read failed: {exc}")
        return {
            "days_available": 0,
            "cumulative_7d_pct": None,
            "cumulative_30d_pct": None,
            "as_of": None,
            "error": str(exc),
        }


def _funding_persistence(samples: list[dict]) -> dict:
    now_ts = int(time.time())
    rows_24h = [
        row
        for row in samples
        if row["timestamp"] >= now_ts - 86400
        and row.get("funding_pct_8h") is not None
    ]

    if not rows_24h:
        return {
            "coverage_hours": 0.0,
            "sample_count": 0,
            "positive_share_pct": None,
            "negative_share_pct": None,
            "dominant_side": "unavailable",
        }

    coverage_hours = (
        rows_24h[-1]["timestamp"] - rows_24h[0]["timestamp"]
    ) / 3600.0 if len(rows_24h) >= 2 else 0.0

    positive = sum(
        1 for row in rows_24h
        if float(row["funding_pct_8h"]) > 0
    )
    negative = sum(
        1 for row in rows_24h
        if float(row["funding_pct_8h"]) < 0
    )
    total = len(rows_24h)

    positive_share = positive / total * 100.0
    negative_share = negative / total * 100.0

    if coverage_hours < 6 or total < 4:
        dominant = "building_history"
    elif positive_share >= 75:
        dominant = "longs_paying"
    elif negative_share >= 75:
        dominant = "shorts_paying"
    else:
        dominant = "mixed"

    return {
        "coverage_hours": round(coverage_hours, 1),
        "sample_count": total,
        "positive_share_pct": round(positive_share, 1),
        "negative_share_pct": round(negative_share, 1),
        "dominant_side": dominant,
    }


def _measured_liquidation_clusters() -> dict:
    depth = get_snapshot_route("/liquidity/depth")
    if not isinstance(depth, dict):
        return {
            "status": "unavailable",
            "source": None,
            "long_cluster_usd": None,
            "short_cluster_usd": None,
        }

    mode = depth.get("liquidation_data_mode")
    source = depth.get("liquidation_source")

    # Only use actual CoinGlass heatmap parsing here. Never turn the depth
    # route's OI heuristic scenario into measured liquidation exposure.
    if mode != "measured_heatmap" and source != "CoinGlass heatmap":
        return {
            "status": "unavailable",
            "source": source,
            "long_cluster_usd": None,
            "short_cluster_usd": None,
            "note": (
                "Nearby liquidation clusters unavailable; OI heuristics "
                "are intentionally excluded from Perps Pressure."
            ),
        }

    return {
        "status": "measured_heatmap",
        "source": source,
        "long_cluster_usd": _safe_float(
            depth.get("long_liquidation_cluster_usd_raw")
        ),
        "short_cluster_usd": _safe_float(
            depth.get("short_liquidation_cluster_usd_raw")
        ),
        "updated_at": depth.get("updated_at"),
        "note": (
            "Heatmap values describe nearby liquidation vulnerability, "
            "not liquidations already executed."
        ),
    }


def _score_carry(
    funding_pct_8h: Optional[float],
    side: str,
) -> Optional[int]:
    if funding_pct_8h is None:
        return None

    if side == "longs":
        directional = max(0.0, funding_pct_8h)
    else:
        directional = max(0.0, -funding_pct_8h)

    annualized_pct = directional * 3 * 365
    return round(_clamp(annualized_pct / 40.0 * 100.0))


def _score_crowding(
    oi_change_24h_pct: Optional[float],
    funding_pct_8h: Optional[float],
    side: str,
) -> Optional[int]:
    if oi_change_24h_pct is None or funding_pct_8h is None:
        return None

    if oi_change_24h_pct <= 0:
        return 0

    funding_supports = (
        funding_pct_8h > 0
        if side == "longs"
        else funding_pct_8h < 0
    )

    # 8% 24h OI expansion is treated as a very large build.
    base = _clamp(oi_change_24h_pct / 8.0 * 100.0)

    if not funding_supports:
        base *= 0.25

    return round(base)


def _score_forced(
    oi_change_24h_pct: Optional[float],
    price_1d_pct: Optional[float],
    side: str,
) -> Optional[int]:
    if oi_change_24h_pct is None or price_1d_pct is None:
        return None

    if oi_change_24h_pct >= 0:
        return 0

    adverse_price = (
        -price_1d_pct
        if side == "longs"
        else price_1d_pct
    )

    if adverse_price <= 0:
        return 0

    # Transparent coarse score:
    # -10% OI contraction -> 60 points
    #  -5% adverse price  -> 40 points
    oi_component = _clamp(
        abs(oi_change_24h_pct) / 10.0 * 60.0,
        0,
        60,
    )
    price_component = _clamp(
        adverse_price / 5.0 * 40.0,
        0,
        40,
    )
    return round(oi_component + price_component)


def _score_vulnerability(
    carry_score: Optional[int],
    crowding_score: Optional[int],
    cluster_usd: Optional[float],
    opposite_cluster_usd: Optional[float],
) -> Optional[int]:
    available = [
        score
        for score in (carry_score, crowding_score)
        if score is not None
    ]

    if not available and cluster_usd is None:
        return None

    carry = float(carry_score or 0)
    crowding = float(crowding_score or 0)

    cluster_component = 0.0
    if (
        cluster_usd is not None
        and opposite_cluster_usd is not None
        and cluster_usd + opposite_cluster_usd > 0
    ):
        share = cluster_usd / (
            cluster_usd + opposite_cluster_usd
        )
        cluster_component = share * 100.0

    # Visible ingredients stay exposed in the payload.
    score = (
        carry * 0.45
        + crowding * 0.40
        + cluster_component * 0.15
    )
    return round(_clamp(score))


def _forced_flow_state(
    long_forced: Optional[int],
    short_forced: Optional[int],
    oi_change_24h_pct: Optional[float],
) -> dict:
    lf = long_forced or 0
    sf = short_forced or 0

    if lf >= 35 and lf > sf:
        return {
            "side": "longs",
            "state": "long_deleveraging",
            "label": "Long deleveraging active",
            "confidence": "strongly_inferred",
            "explanation": (
                "Price is falling while open interest contracts. "
                "That is consistent with long positions being closed or forced out; "
                "the data does not identify individual liquidation fills."
            ),
        }

    if sf >= 35 and sf > lf:
        return {
            "side": "shorts",
            "state": "short_deleveraging",
            "label": "Short deleveraging active",
            "confidence": "strongly_inferred",
            "explanation": (
                "Price is rising while open interest contracts. "
                "That is consistent with short positions being closed or forced out; "
                "the data does not identify individual liquidation fills."
            ),
        }

    if oi_change_24h_pct is not None and oi_change_24h_pct <= -2:
        return {
            "side": "mixed",
            "state": "general_deleveraging",
            "label": "General deleveraging",
            "confidence": "strongly_inferred",
            "explanation": (
                "Open interest is contracting, but price direction does not "
                "cleanly identify which side is carrying the forced pressure."
            ),
        }

    return {
        "side": "none",
        "state": "not_confirmed",
        "label": "No clear forced deleveraging",
        "confidence": "possible",
        "explanation": (
            "Current price and OI behavior do not show a strong forced-unwind signature."
        ),
    }


def _primary_state(
    funding_pct_8h: Optional[float],
    annualized_pct: Optional[float],
    oi_change_24h_pct: Optional[float],
    price_1d_pct: Optional[float],
    forced: dict,
) -> dict:
    if forced.get("state") == "long_deleveraging":
        return {
            "code": "long_flush",
            "label": "Long Flush / Deleveraging",
            "primary_side": "longs",
            "severity": "high",
            "explanation": (
                "Long-side pressure has progressed beyond expensive carry into "
                "an OI contraction during falling price."
            ),
        }

    if forced.get("state") == "short_deleveraging":
        return {
            "code": "short_squeeze",
            "label": "Short Squeeze / Deleveraging",
            "primary_side": "shorts",
            "severity": "high",
            "explanation": (
                "Short-side pressure has progressed into an OI contraction during rising price."
            ),
        }

    if (
        funding_pct_8h is not None
        and funding_pct_8h > 0
        and annualized_pct is not None
        and annualized_pct >= 15
    ):
        if (
            oi_change_24h_pct is not None
            and oi_change_24h_pct > 0
            and price_1d_pct is not None
            and price_1d_pct <= 0
        ):
            return {
                "code": "long_carry_pressure",
                "label": "Longs Under Carry Pressure",
                "primary_side": "longs",
                "severity": "elevated",
                "explanation": (
                    "Longs are paying elevated funding while OI remains persistent "
                    "or builds despite weak price. Crowding remains vulnerable to a downside flush."
                ),
            }

        return {
            "code": "long_leverage_rich",
            "label": "Long Leverage Rich",
            "primary_side": "longs",
            "severity": "moderate",
            "explanation": (
                "Positive funding is expensive for longs, but forced deleveraging is not confirmed."
            ),
        }

    if (
        funding_pct_8h is not None
        and funding_pct_8h < 0
        and annualized_pct is not None
        and abs(annualized_pct) >= 15
    ):
        if (
            oi_change_24h_pct is not None
            and oi_change_24h_pct > 0
            and price_1d_pct is not None
            and price_1d_pct >= 0
        ):
            return {
                "code": "short_carry_pressure",
                "label": "Shorts Under Carry Pressure",
                "primary_side": "shorts",
                "severity": "elevated",
                "explanation": (
                    "Shorts are paying elevated funding while OI remains persistent "
                    "or builds despite firm price. Upside squeeze vulnerability is elevated."
                ),
            }

        return {
            "code": "short_leverage_rich",
            "label": "Short Leverage Rich",
            "primary_side": "shorts",
            "severity": "moderate",
            "explanation": (
                "Negative funding is expensive for shorts, but forced covering is not confirmed."
            ),
        }

    if oi_change_24h_pct is not None and oi_change_24h_pct >= 3:
        return {
            "code": "leverage_building",
            "label": "Leverage Building",
            "primary_side": "mixed",
            "severity": "moderate",
            "explanation": (
                "Open interest is expanding, but funding does not identify a strongly expensive side."
            ),
        }

    return {
        "code": "balanced",
        "label": "Perps Broadly Balanced",
        "primary_side": "mixed",
        "severity": "low",
        "explanation": (
            "Carry, OI and price do not currently form a strong one-sided pressure regime."
        ),
    }


def _parse_percent_value(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return _safe_float(value)

    try:
        cleaned = (
            str(value)
            .strip()
            .replace("%", "")
            .replace(",", "")
        )
        return _safe_float(cleaned)
    except Exception:
        return None


def _basis_context(
    perp_annualized_pct: Optional[float],
) -> dict:
    """
    Compare current perpetual funding carry with the existing CME basis snapshot.

    The comparison is deliberately qualitative. Perpetual funding and a dated
    CME futures basis are different instruments/tenors, so the spread is useful
    as a structure diagnostic rather than an arbitrage-equivalent P&L measure.
    """
    basis = get_snapshot_route("/leading/basis-enhanced")
    if not isinstance(basis, dict) or basis.get("error"):
        return {
            "status": "unavailable",
            "source": "CME futures basis snapshot",
        }

    cme_annualized_pct = _parse_percent_value(
        basis.get("annualized")
    )

    spread_vs_perp_pp = None
    if (
        perp_annualized_pct is not None
        and cme_annualized_pct is not None
    ):
        spread_vs_perp_pp = (
            perp_annualized_pct - cme_annualized_pct
        )

    regime_code = "comparison_unavailable"
    regime_label = "Carry comparison unavailable"
    interpretation = (
        "CME basis is available, but the current perp carry "
        "cannot be compared cleanly."
    )

    if cme_annualized_pct is not None:
        if cme_annualized_pct < 0:
            regime_code = "cme_backwardation"
            regime_label = "CME Backwardation"
            interpretation = (
                "CME futures are below spot on an annualized basis. "
                "Regulated futures carry is stressed rather than rich."
            )

        elif perp_annualized_pct is None:
            regime_code = "cme_only"
            regime_label = "CME Carry Available"
            interpretation = (
                "CME basis is available, but perp carry is missing."
            )

        elif (
            perp_annualized_pct < 0
            and cme_annualized_pct >= 5
        ):
            regime_code = "perp_short_cme_contango"
            regime_label = "Perp Shorts · CME Carry Intact"
            interpretation = (
                "Perpetual funding is negative while CME remains "
                "in positive contango. Crypto-native positioning is "
                "short-skewed while regulated futures carry remains positive."
            )

        elif (
            perp_annualized_pct >= 15
            and cme_annualized_pct >= 10
        ):
            regime_code = "broad_carry_rich"
            regime_label = "Broad Carry Rich"
            interpretation = (
                "Both perpetual funding and CME annualized basis are elevated. "
                "Leverage/carry pricing is rich across crypto-native and "
                "regulated futures venues."
            )

        elif (
            spread_vs_perp_pp is not None
            and spread_vs_perp_pp >= 5
        ):
            regime_code = "perps_richer"
            regime_label = "Perps Richer Than CME"
            interpretation = (
                "Perpetual funding is materially richer than CME basis. "
                "Current leverage pressure is more concentrated in perps "
                "than in regulated futures."
            )

        elif (
            spread_vs_perp_pp is not None
            and spread_vs_perp_pp <= -5
        ):
            regime_code = "cme_richer"
            regime_label = "CME Richer Than Perps"
            interpretation = (
                "CME basis is materially richer than perpetual funding. "
                "Regulated futures carry is stronger than current perp carry."
            )

        else:
            regime_code = "carry_aligned"
            regime_label = "Carry Broadly Aligned"
            interpretation = (
                "Perpetual funding and CME basis are broadly aligned. "
                "Neither venue is carrying a large relative premium."
            )

    return {
        "status": "available",
        "annualized": basis.get("annualized"),
        "cme_annualized_pct": (
            round(cme_annualized_pct, 2)
            if cme_annualized_pct is not None
            else None
        ),
        "perp_annualized_pct": (
            round(perp_annualized_pct, 2)
            if perp_annualized_pct is not None
            else None
        ),
        "spread_vs_perp_pp": (
            round(spread_vs_perp_pp, 2)
            if spread_vs_perp_pp is not None
            else None
        ),
        "raw_basis": basis.get("raw_basis"),
        "days_to_exp": basis.get("days_to_exp"),
        "futures_px": basis.get("futures_px"),
        "spot_px": basis.get("spot_px"),
        "trend_5d": basis.get("trend_5d"),
        "trend_note": basis.get("trend_note"),
        "pattern": basis.get("pattern"),
        "alert": basis.get("alert"),
        "alert_level": basis.get("alert_level"),
        "regime_code": regime_code,
        "regime_label": regime_label,
        "interpretation": interpretation,
        "comparison_note": (
            "Perp funding and dated CME basis are different carry structures; "
            "their spread is a positioning diagnostic, not a like-for-like "
            "arbitrage return."
        ),
        "source": "CME futures basis snapshot",
    }


def _build_pressure() -> dict:
    live = _latest_live_state()

    funding_pct_8h = _safe_float(live.get("funding_pct_8h"))
    oi_usd = _safe_float(live.get("oi_usd"))
    price_usd = _safe_float(live.get("price_usd"))
    price_1d_pct = _safe_float(live.get("price_1d_pct"))
    price_7d_pct = _safe_float(live.get("price_7d_pct"))

    annualized_pct = (
        funding_pct_8h * 3 * 365
        if funding_pct_8h is not None
        else None
    )

    _record_sample(
        funding_pct_8h,
        oi_usd,
        price_usd,
    )
    samples = _pressure_samples(days=8)

    oi_change_24h_pct = _nearest_oi_change(
        oi_usd,
        24 * 3600,
        6 * 3600,
    )
    oi_change_7d_pct = _nearest_oi_change(
        oi_usd,
        7 * 86400,
        18 * 3600,
    )

    cumulative = _funding_history_context()
    persistence = _funding_persistence(samples)
    clusters = _measured_liquidation_clusters()

    long_carry = _score_carry(funding_pct_8h, "longs")
    short_carry = _score_carry(funding_pct_8h, "shorts")

    long_crowding = _score_crowding(
        oi_change_24h_pct,
        funding_pct_8h,
        "longs",
    )
    short_crowding = _score_crowding(
        oi_change_24h_pct,
        funding_pct_8h,
        "shorts",
    )

    long_forced = _score_forced(
        oi_change_24h_pct,
        price_1d_pct,
        "longs",
    )
    short_forced = _score_forced(
        oi_change_24h_pct,
        price_1d_pct,
        "shorts",
    )

    long_vulnerability = _score_vulnerability(
        long_carry,
        long_crowding,
        clusters.get("long_cluster_usd"),
        clusters.get("short_cluster_usd"),
    )
    short_vulnerability = _score_vulnerability(
        short_carry,
        short_crowding,
        clusters.get("short_cluster_usd"),
        clusters.get("long_cluster_usd"),
    )

    forced = _forced_flow_state(
        long_forced,
        short_forced,
        oi_change_24h_pct,
    )
    primary = _primary_state(
        funding_pct_8h,
        annualized_pct,
        oi_change_24h_pct,
        price_1d_pct,
        forced,
    )

    payer = (
        "longs"
        if funding_pct_8h is not None and funding_pct_8h > 0
        else "shorts"
        if funding_pct_8h is not None and funding_pct_8h < 0
        else "neutral"
        if funding_pct_8h is not None
        else "unavailable"
    )

    notional = 100_000.0
    daily_cost = (
        notional * (abs(funding_pct_8h) / 100.0) * 3
        if funding_pct_8h is not None
        else None
    )
    weekly_cost = (
        daily_cost * 7
        if daily_cost is not None
        else None
    )

    matrix = {
        "longs": {
            "carry": {
                "score": long_carry,
                "level": _level(long_carry),
            },
            "crowding": {
                "score": long_crowding,
                "level": _level(long_crowding),
            },
            "forced": {
                "score": long_forced,
                "level": _level(long_forced),
            },
            "vulnerability": {
                "score": long_vulnerability,
                "level": _level(long_vulnerability),
            },
        },
        "shorts": {
            "carry": {
                "score": short_carry,
                "level": _level(short_carry),
            },
            "crowding": {
                "score": short_crowding,
                "level": _level(short_crowding),
            },
            "forced": {
                "score": short_forced,
                "level": _level(short_forced),
            },
            "vulnerability": {
                "score": short_vulnerability,
                "level": _level(short_vulnerability),
            },
        },
    }

    core_available = sum(
        value is not None
        for value in (
            funding_pct_8h,
            oi_change_24h_pct,
            price_1d_pct,
        )
    )
    quality = (
        "good"
        if core_available == 3
        else "partial"
        if core_available >= 1
        else "unavailable"
    )

    return {
        "name": "Perps Pressure",
        "category": "Derivatives · Perpetual Futures",
        "updated_at": datetime.now(timezone.utc).isoformat(),

        "state": primary,
        "matrix": matrix,

        "carry": {
            "payer": payer,
            "funding_pct_8h": (
                round(funding_pct_8h, 6)
                if funding_pct_8h is not None
                else None
            ),
            "annualized_pct_if_persisted": (
                round(annualized_pct, 2)
                if annualized_pct is not None
                else None
            ),
            "cumulative_7d_pct": cumulative.get(
                "cumulative_7d_pct"
            ),
            "cumulative_30d_pct": cumulative.get(
                "cumulative_30d_pct"
            ),
            "history_days_available": cumulative.get(
                "days_available",
                0,
            ),
            "hypothetical_notional_usd": notional,
            "cost_per_day_if_rate_persisted_usd": (
                round(daily_cost, 2)
                if daily_cost is not None
                else None
            ),
            "cost_per_week_if_rate_persisted_usd": (
                round(weekly_cost, 2)
                if weekly_cost is not None
                else None
            ),
            "exchange_rates_pct_8h": live.get(
                "exchange_rates",
                {},
            ),
            "persistence_24h": persistence,
            "confidence": "observed",
            "note": (
                "Annualized/day/week figures are simple carry translations. "
                "Funding varies; they are not forecasts."
            ),
        },

        "positioning": {
            "oi_usd": oi_usd,
            "oi_display": _fmt_usd(oi_usd),
            "oi_change_24h_pct": (
                round(oi_change_24h_pct, 2)
                if oi_change_24h_pct is not None
                else None
            ),
            "oi_change_7d_pct": (
                round(oi_change_7d_pct, 2)
                if oi_change_7d_pct is not None
                else None
            ),
            "price_usd": (
                round(price_usd, 2)
                if price_usd is not None
                else None
            ),
            "price_change_1d_pct": (
                round(price_1d_pct, 2)
                if price_1d_pct is not None
                else None
            ),
            "price_change_7d_pct": (
                round(price_7d_pct, 2)
                if price_7d_pct is not None
                else None
            ),
            "confidence": "observed_inputs",
        },

        "forced_flow": forced,

        "liquidation_vulnerability": {
            **clusters,
            "long_cluster_display": _fmt_usd(
                clusters.get("long_cluster_usd")
            ),
            "short_cluster_display": _fmt_usd(
                clusters.get("short_cluster_usd")
            ),
        },

        "basis_context": _basis_context(
            annualized_pct,
        ),

        "data_quality": {
            "status": quality,
            "core_inputs_available": core_available,
            "funding_sample_count_24h": persistence.get(
                "sample_count",
                0,
            ),
            "funding_history_coverage_hours": persistence.get(
                "coverage_hours",
                0.0,
            ),
            "liquidation_clusters": clusters.get("status"),
            "notes": [
                "Carry is directly observed from funding.",
                "Crowding is inferred from OI + funding.",
                "Forced side is strongly inferred from adverse price + falling OI.",
                "Heatmap clusters describe vulnerability, not realized liquidation fills.",
            ],
        },
    }


@derivatives_router.get("/pressure")
def get_derivatives_pressure():
    return _build_pressure()
