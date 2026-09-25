"""Powder Keg forced-selling state synthesizer.

Consumes only already-collected snapshot routes. No provider/network calls.
Missing inputs reduce coverage instead of counting as zero. Exhaustion is gated
behind an actual recent cascade stored in a tiny SQLite history.
"""

from __future__ import annotations

import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "powder_keg_history.db"

CASCADE_THRESHOLD = 60
EXHAUSTION_THRESHOLD = 65
LOOKBACK_HOURS = 72
MIN_WRITE_SECONDS = 10 * 60
RETENTION_DAYS = 45


def _num(value: Any) -> Optional[float]:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _get(value: Any, *keys: str) -> Any:
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _max_matrix(perps: dict, key: str) -> Optional[float]:
    values = [
        _num(_get(perps, "matrix", side, key, "score"))
        for side in ("longs", "shorts")
    ]
    clean = [value for value in values if value is not None]
    return max(clean) if clean else None


def _factor(
    key: str,
    label: str,
    score: Optional[float],
    weight: float,
    value: str,
    explanation: str,
    source: str,
) -> dict:
    return {
        "key": key,
        "label": label,
        "score": round(max(0, min(100, score)), 1) if score is not None else None,
        "weight": weight,
        "available": score is not None,
        "value": value,
        "explanation": explanation,
        "source": source,
    }


def _stage(name: str, factors: list[dict]) -> dict:
    total_weight = sum(item["weight"] for item in factors)
    usable = [item for item in factors if item["score"] is not None]
    usable_weight = sum(item["weight"] for item in usable)
    score = (
        round(sum(item["score"] * item["weight"] for item in usable) / usable_weight)
        if usable_weight
        else None
    )
    return {
        "name": name,
        "score": score,
        "coverage": round(usable_weight / total_weight * 100) if total_weight else 0,
        "factors": factors,
    }


def _fragility(perps: dict, depth: dict, equity: dict) -> dict:
    vulnerability = _max_matrix(perps, "vulnerability")
    primary = str(_get(perps, "state", "primary_side") or "mixed")
    perps_value = (
        f"{primary} · {vulnerability:.0f}/100"
        if vulnerability is not None
        else "—"
    )

    depth_pct = _num(depth.get("depth_vs_median_pct"))
    concentration = _num(depth.get("venue_concentration_pct"))
    depth_parts: list[tuple[float, float]] = []
    depth_values: list[str] = []
    if depth_pct is not None:
        score = 8 if depth_pct >= 120 else 18 if depth_pct >= 100 else 40 if depth_pct >= 80 else 65 if depth_pct >= 60 else 88
        depth_parts.append((score, 0.7))
        depth_values.append(f"depth {depth_pct:.0f}% of median")
    if concentration is not None:
        score = 85 if concentration >= 80 else 65 if concentration >= 65 else 45 if concentration >= 50 else 20
        depth_parts.append((score, 0.3))
        depth_values.append(f"largest venue {concentration:.0f}%")
    depth_score = (
        sum(score * weight for score, weight in depth_parts)
        / sum(weight for _, weight in depth_parts)
        if depth_parts else None
    )

    breadth = _get(equity, "breadth", "label")
    breadth_score = (
        {"very narrow": 90, "narrowing": 68, "neutral": 32, "expanding": 12}
        .get(str(breadth).lower(), 35)
        if breadth else None
    )

    return _stage("Fragility", [
        _factor(
            "perps_vulnerability", "Perps vulnerability", vulnerability, 0.55,
            perps_value,
            "Uses the existing Perps Pressure vulnerability matrix once; OI/funding are not rescored separately.",
            "Perps Pressure",
        ),
        _factor(
            "spot_fragility", "Spot absorption fragility", depth_score, 0.25,
            " · ".join(depth_values) if depth_values else "—",
            "Thin or concentrated visible bids leave less room for forced selling.",
            "Spot Depth",
        ),
        _factor(
            "equity_breadth", "Equity breadth", breadth_score, 0.20,
            str(breadth or "—"),
            str(_get(equity, "breadth", "description") or "RSP/SPY breadth measures participation."),
            "Equity",
        ),
    ])


def _trigger(perps: dict, macro: dict) -> dict:
    day2 = macro.get("day2") if isinstance(macro.get("day2"), dict) else {}
    regime = day2.get("regime") if isinstance(day2.get("regime"), dict) else {}
    confidence = _num(regime.get("confidence"))
    level = str(regime.get("level") or "").lower()
    macro_score = None
    if confidence is not None:
        macro_score = (
            confidence if level == "risk_off"
            else confidence * 0.82 if level == "tightening"
            else min(25, confidence * 0.25) if level == "neutral"
            else max(0, 15 - confidence * 0.15) if level == "risk_on"
            else confidence * 0.35
        )
        if isinstance(day2.get("trigger"), dict) and level in ("risk_off", "tightening"):
            macro_score = max(macro_score, 55)

    price = _num(_get(perps, "positioning", "price_change_1d_pct"))
    price_score = None
    if price is not None:
        price_score = (
            100 if price <= -6 else 88 if price <= -4 else 72 if price <= -2.5
            else 52 if price <= -1 else 32 if price <= -0.25 else 12 if price < 1 else 3
        )

    return _stage("Trigger", [
        _factor(
            "macro_day2", "Macro ignition", macro_score, 0.70,
            f"{regime.get('label', '—')} · {confidence:.0f}% confidence" if confidence is not None else "—",
            str(regime.get("explanation") or "Macro Day 2 transmission is unavailable."),
            "Macro Day 2",
        ),
        _factor(
            "btc_price_damage", "BTC price damage", price_score, 0.30,
            f"{price:+.2f}%" if price is not None else "—",
            "BTC downside is the asset-specific ignition check.",
            "Perps Pressure",
        ),
    ])


def _cross_market(macro: dict, equity: dict) -> tuple[Optional[float], str]:
    day2 = macro.get("day2") if isinstance(macro.get("day2"), dict) else {}
    vix = _num(_get(day2, "conditions", "vix", "d1_pct"))
    hy = _num(_get(day2, "conditions", "hy_oas", "d1_bp"))
    breadth = _get(equity, "breadth", "label")
    scores: list[float] = []
    values: list[str] = []

    if vix is not None:
        scores.append(95 if vix >= 12 else 78 if vix >= 7 else 58 if vix >= 3 else 38 if vix > 0 else 15)
        values.append(f"VIX {vix:+.1f}%")
    if hy is not None:
        scores.append(95 if hy >= 12 else 78 if hy >= 7 else 58 if hy >= 3 else 38 if hy > 0 else 15)
        values.append(f"HY {hy:+.1f}bp")
    if breadth:
        scores.append({"very narrow": 85, "narrowing": 65, "neutral": 30, "expanding": 12}.get(str(breadth).lower(), 35))
        values.append(f"breadth {breadth}")

    return (sum(scores) / len(scores) if scores else None, " · ".join(values) if values else "—")


def _cascade(perps: dict, depth: dict, macro: dict, equity: dict) -> dict:
    forced = _max_matrix(perps, "forced")
    forced_label = str(_get(perps, "forced_flow", "label") or "—")

    coverage = _num(depth.get("adjusted_coverage"))
    if coverage is not None:
        absorption = 98 if coverage < 0.5 else 88 if coverage < 1 else 74 if coverage < 1.5 else 58 if coverage < 2.5 else 38 if coverage < 4 else 15
        absorption_value = f"{coverage:.2f}× adjusted depth coverage"
    else:
        absorption = {"extreme": 90, "notable": 65, "neutral": 30, "none": 20}.get(
            str(depth.get("cascade_risk_level") or "").lower()
        )
        absorption_value = str(depth.get("cascade_risk_label") or "—")

    cross_score, cross_value = _cross_market(macro, equity)

    return _stage("Cascade", [
        _factor(
            "forced_flow", "Forced-flow pressure", forced, 0.55,
            forced_label,
            str(_get(perps, "forced_flow", "explanation") or "Price plus falling OI infer forced deleveraging."),
            "Perps Pressure",
        ),
        _factor(
            "spot_absorption", "Spot absorption stress", absorption, 0.30,
            absorption_value,
            f"Visible bids versus forced-flow stress; liquidation input: {depth.get('liquidation_source', 'unavailable')}.",
            "Spot Depth",
        ),
        _factor(
            "cross_market", "Cross-market confirmation", cross_score, 0.15,
            cross_value,
            "VIX, HY credit and breadth confirm whether selling is broadening.",
            "Macro Day 2 + Equity",
        ),
    ])


def _init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS powder_keg_samples (
                timestamp INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                fragility REAL,
                trigger_score REAL,
                cascade REAL,
                exhaustion REAL,
                carry REAL,
                forced REAL,
                depth_pct REAL,
                vix_d1 REAL,
                hy_d1 REAL
            )
            """
        )
        conn.commit()


def _history() -> list[dict]:
    cutoff = int(time.time()) - LOOKBACK_HOURS * 3600
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT timestamp, state, fragility, trigger_score, cascade,
                       exhaustion, carry, forced, depth_pct, vix_d1, hy_d1
                FROM powder_keg_samples
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (cutoff,),
            ).fetchall()
    except Exception:
        return []
    keys = ["timestamp", "state", "fragility", "trigger", "cascade", "exhaustion", "carry", "forced", "depth_pct", "vix_d1", "hy_d1"]
    return [dict(zip(keys, row)) for row in rows]


def _exhaustion(perps: dict, depth: dict, macro: dict, equity: dict, cascade: dict, history: list[dict]) -> dict:
    current_cascade = _num(cascade.get("score"))
    peaks = [_num(row.get("cascade")) for row in history]
    clean_peaks = [value for value in peaks if value is not None]
    if current_cascade is not None:
        clean_peaks.append(current_cascade)
    peak = max(clean_peaks, default=0)
    eligible = peak >= CASCADE_THRESHOLD

    retreat = None
    if eligible and current_cascade is not None:
        drop = peak - current_cascade
        retreat = 95 if drop >= 30 else 80 if drop >= 20 else 60 if drop >= 10 else 35 if drop > 0 else 5

    carry = _max_matrix(perps, "carry")
    carry_norm = None if carry is None else (95 if carry <= 15 else 75 if carry <= 30 else 45 if carry <= 50 else 12)
    forced = _max_matrix(perps, "forced")
    forced_cool = None if forced is None else (95 if forced <= 10 else 75 if forced <= 25 else 48 if forced <= 45 else 10)

    depth_pct = _num(depth.get("depth_vs_median_pct"))
    depth_recovery = None if depth_pct is None else (92 if depth_pct >= 110 else 75 if depth_pct >= 90 else 55 if depth_pct >= 75 else 35 if depth_pct >= 60 else 12)

    day2 = macro.get("day2") if isinstance(macro.get("day2"), dict) else {}
    vix = _num(_get(day2, "conditions", "vix", "d1_pct"))
    hy = _num(_get(day2, "conditions", "hy_oas", "d1_bp"))
    breadth = _get(equity, "breadth", "label")
    stabilization: list[float] = []
    if vix is not None:
        stabilization.append(90 if vix <= -5 else 75 if vix <= 0 else 35 if vix < 5 else 10)
    if hy is not None:
        stabilization.append(90 if hy <= -3 else 75 if hy <= 0 else 35 if hy < 5 else 10)
    if breadth:
        stabilization.append({"expanding": 92, "neutral": 68, "narrowing": 30, "very narrow": 10}.get(str(breadth).lower(), 50))
    stable_score = sum(stabilization) / len(stabilization) if stabilization else None

    gated = lambda value: value if eligible else None
    result = _stage("Exhaustion", [
        _factor("cascade_retreat", "Cascade retreat", gated(retreat), 0.30, f"peak {peak:.0f} → {current_cascade:.0f}" if eligible and current_cascade is not None else "—", "Pressure must retreat from a real recent cascade.", "Powder Keg history"),
        _factor("carry_normalization", "Carry normalization", gated(carry_norm), 0.20, f"{carry:.0f}/100" if carry is not None else "—", "Funding/carry pressure should normalize.", "Perps Pressure"),
        _factor("forced_flow_cooling", "Forced-flow cooling", gated(forced_cool), 0.20, f"{forced:.0f}/100" if forced is not None else "—", "Forced deleveraging should fade.", "Perps Pressure"),
        _factor("depth_recovery", "Spot-depth recovery", gated(depth_recovery), 0.15, f"{depth_pct:.0f}% of median" if depth_pct is not None else "—", "Visible bids should recover.", "Spot Depth"),
        _factor("cross_market_stabilization", "Cross-market stabilization", gated(stable_score), 0.15, f"VIX {vix:+.1f}% · HY {hy:+.1f}bp" if vix is not None and hy is not None else "partial", "Volatility, credit and breadth should stop worsening.", "Macro Day 2 + Equity"),
    ])
    result.update({
        "eligible": eligible,
        "eligible_reason": (
            f"Recent cascade peaked at {peak:.0f}/100 within {LOOKBACK_HOURS}h."
            if eligible
            else f"No {CASCADE_THRESHOLD}+/100 cascade in the last {LOOKBACK_HOURS}h."
        ),
        "recent_peak_cascade": round(peak) if eligible else None,
    })
    return result


def _state(stages: dict) -> dict:
    f = _num(_get(stages, "fragility", "score")) or 0
    t = _num(_get(stages, "trigger", "score")) or 0
    c = _num(_get(stages, "cascade", "score")) or 0
    e = _num(_get(stages, "exhaustion", "score")) or 0
    exhaustion = stages["exhaustion"]

    if exhaustion.get("eligible") and e >= EXHAUSTION_THRESHOLD and c < 55:
        return {"code": "EXHAUSTION", "label": "Selling Exhaustion", "color": "green", "confidence": exhaustion["coverage"], "summary": "A recent cascade is losing intensity while pressure and absorption conditions stabilize."}
    if c >= CASCADE_THRESHOLD:
        return {"code": "CASCADE", "label": "Forced-Selling Cascade", "color": "red", "confidence": stages["cascade"]["coverage"], "summary": "Mechanical deleveraging and/or weak spot absorption are active."}
    if t >= 55 and f >= 35:
        confidence = round((stages["trigger"]["coverage"] + stages["fragility"]["coverage"]) / 2)
        return {"code": "TRIGGERED", "label": "Triggered", "color": "orange", "confidence": confidence, "summary": "Ignition is active while structural fuel is present, but cascade intensity is not confirmed."}
    if f >= 55:
        return {"code": "FRAGILE", "label": "Fragile", "color": "yellow", "confidence": stages["fragility"]["coverage"], "summary": "Leverage, breadth or absorption conditions leave the market vulnerable without a strong active trigger."}
    confidence = round((stages["fragility"]["coverage"] + stages["trigger"]["coverage"] + stages["cascade"]["coverage"]) / 3)
    return {"code": "CALM", "label": "Calm / Unlit", "color": "slate", "confidence": confidence, "summary": "Available evidence does not show enough fuel plus ignition to classify the market as a Powder Keg."}


def _evidence(state_code: str, stages: dict) -> tuple[list[str], list[str], list[str]]:
    relevant = {
        "CALM": ("fragility", "trigger", "cascade"),
        "FRAGILE": ("fragility", "trigger"),
        "TRIGGERED": ("fragility", "trigger", "cascade"),
        "CASCADE": ("cascade", "fragility", "trigger"),
        "EXHAUSTION": ("exhaustion", "cascade"),
    }[state_code]
    factors = [factor for key in relevant for factor in stages[key]["factors"]]
    limitations = [f"{factor['label']}: unavailable" for factor in factors if factor["score"] is None]
    available = [factor for factor in factors if factor["score"] is not None]
    available.sort(key=lambda factor: factor["score"] * factor["weight"], reverse=True)
    supporting = [f"{factor['label']}: {factor['value']}" for factor in available if factor["score"] >= 55][:6]
    contradicting = [f"{factor['label']}: {factor['value']}" for factor in reversed(available) if factor["score"] <= 30][:6]
    return supporting, contradicting, list(dict.fromkeys(limitations))[:6]


def _diagnostics(routes: dict, exhaustion: dict) -> dict:
    perps = routes.get("/derivatives/pressure") if isinstance(routes.get("/derivatives/pressure"), dict) else {}
    depth = routes.get("/liquidity/depth") if isinstance(routes.get("/liquidity/depth"), dict) else {}
    macro = routes.get("/macro/metrics") if isinstance(routes.get("/macro/metrics"), dict) else {}
    equity = routes.get("/equity/metrics") if isinstance(routes.get("/equity/metrics"), dict) else {}
    metrics = routes.get("/metrics") if isinstance(routes.get("/metrics"), dict) else {}
    leading = routes.get("/leading/all") if isinstance(routes.get("/leading/all"), dict) else {}
    day2 = macro.get("day2") if isinstance(macro.get("day2"), dict) else {}
    options = leading.get("options") if isinstance(leading.get("options"), dict) else {}
    etf = metrics.get("etf_flow") if isinstance(metrics.get("etf_flow"), dict) else {}

    return {
        "leverage": {
            "state": _get(perps, "state", "label"),
            "primary_side": _get(perps, "state", "primary_side"),
            "funding_pct_8h": _num(_get(perps, "carry", "funding_pct_8h")),
            "oi_change_24h_pct": _num(_get(perps, "positioning", "oi_change_24h_pct")),
            "price_change_24h_pct": _num(_get(perps, "positioning", "price_change_1d_pct")),
            "carry_score": _max_matrix(perps, "carry"),
            "crowding_score": _max_matrix(perps, "crowding"),
            "forced_score": _max_matrix(perps, "forced"),
            "vulnerability_score": _max_matrix(perps, "vulnerability"),
            "liquidation_status": _get(perps, "liquidation_vulnerability", "status"),
            "long_liquidation_cluster": _get(perps, "liquidation_vulnerability", "long_cluster_display"),
            "short_liquidation_cluster": _get(perps, "liquidation_vulnerability", "short_cluster_display"),
            "basis_regime": _get(perps, "basis_context", "regime_label"),
        },
        "macro": {
            "regime": _get(day2, "regime", "label"),
            "regime_level": _get(day2, "regime", "level"),
            "confidence": _num(_get(day2, "regime", "confidence")),
            "policy_read": _get(day2, "policy_read", "label"),
            "real_10y_d1_bp": _num(_get(day2, "rates", "real_yield_10y", "d1_bp")),
            "yield_2y_d1_bp": _num(_get(day2, "rates", "yield_2y", "d1_bp")),
            "dxy_d1_pct": _num(_get(day2, "conditions", "dxy", "d1_pct")),
            "vix_d1_pct": _num(_get(day2, "conditions", "vix", "d1_pct")),
            "hy_oas_d1_bp": _num(_get(day2, "conditions", "hy_oas", "d1_bp")),
            "alignment": day2.get("data_alignment"),
            "transmission": day2.get("transmission", []),
        },
        "market_structure": {
            "cascade_risk_label": depth.get("cascade_risk_label"),
            "adjusted_coverage": _num(depth.get("adjusted_coverage")),
            "depth_vs_median_pct": _num(depth.get("depth_vs_median_pct")),
            "venue_concentration_pct": _num(depth.get("venue_concentration_pct")),
            "liquidation_source": depth.get("liquidation_source"),
            "breadth_label": _get(equity, "breadth", "label"),
            "breadth_20d": _get(equity, "breadth", "ratio_20d_chg"),
        },
        "options": {
            "term_structure": options.get("term_structure_label"),
            "term_spread": _num(options.get("term_spread")),
            "risk_reversal_25d": _num(options.get("risk_reversal_25d")),
            "alert": options.get("alert"),
        },
        "flows": {
            "etf_current": etf.get("current"),
            "etf_7d": etf.get("d7"),
            "etf_vs30d": etf.get("vs30d"),
            "note": "ETF flows are context-only until persistence/acceleration fields are explicit.",
        },
        "exhaustion": {
            "eligible": exhaustion.get("eligible"),
            "eligible_reason": exhaustion.get("eligible_reason"),
            "recent_peak_cascade": exhaustion.get("recent_peak_cascade"),
        },
    }


def _record(payload: dict) -> None:
    now = int(time.time())
    stages = payload["stages"]
    diagnostics = payload["diagnostics"]
    with sqlite3.connect(DB_PATH) as conn:
        latest = conn.execute("SELECT timestamp FROM powder_keg_samples ORDER BY timestamp DESC LIMIT 1").fetchone()
        if latest and now - int(latest[0]) < MIN_WRITE_SECONDS:
            return
        conn.execute(
            """
            INSERT OR REPLACE INTO powder_keg_samples
            (timestamp, state, fragility, trigger_score, cascade, exhaustion,
             carry, forced, depth_pct, vix_d1, hy_d1)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                payload["state"]["code"],
                _get(stages, "fragility", "score"),
                _get(stages, "trigger", "score"),
                _get(stages, "cascade", "score"),
                _get(stages, "exhaustion", "score"),
                _get(diagnostics, "leverage", "carry_score"),
                _get(diagnostics, "leverage", "forced_score"),
                _get(diagnostics, "market_structure", "depth_vs_median_pct"),
                _get(diagnostics, "macro", "vix_d1_pct"),
                _get(diagnostics, "macro", "hy_oas_d1_bp"),
            ),
        )
        conn.execute("DELETE FROM powder_keg_samples WHERE timestamp < ?", (now - RETENTION_DAYS * 86400,))
        conn.commit()


def build_powder_keg(routes: dict[str, Any], *, persist: bool = False) -> dict[str, Any]:
    _init_db()
    perps = routes.get("/derivatives/pressure") if isinstance(routes.get("/derivatives/pressure"), dict) else {}
    depth = routes.get("/liquidity/depth") if isinstance(routes.get("/liquidity/depth"), dict) else {}
    macro = routes.get("/macro/metrics") if isinstance(routes.get("/macro/metrics"), dict) else {}
    equity = routes.get("/equity/metrics") if isinstance(routes.get("/equity/metrics"), dict) else {}

    fragility = _fragility(perps, depth, equity)
    trigger = _trigger(perps, macro)
    cascade = _cascade(perps, depth, macro, equity)
    exhaustion = _exhaustion(perps, depth, macro, equity, cascade, _history())
    stages = {
        "fragility": fragility,
        "trigger": trigger,
        "cascade": cascade,
        "exhaustion": exhaustion,
    }
    state = _state(stages)
    supporting, contradicting, limitations = _evidence(state["code"], stages)

    payload = {
        "name": "Powder Keg",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "state": state,
        "stages": stages,
        "supporting": supporting,
        "contradicting": contradicting,
        "limitations": limitations,
        "diagnostics": _diagnostics(routes, exhaustion),
        "model": {
            "version": "2.0",
            "state_flow": ["CALM", "FRAGILE", "TRIGGERED", "CASCADE", "EXHAUSTION"],
            "thresholds": {
                "fragile": 55,
                "triggered": {"trigger": 55, "minimum_fragility": 35},
                "cascade": CASCADE_THRESHOLD,
                "exhaustion": {"score": EXHAUSTION_THRESHOLD, "lookback_hours": LOOKBACK_HOURS},
            },
            "principles": [
                "Subsystem outputs are scored once to reduce double-counting.",
                "Missing inputs reduce coverage instead of counting as zero.",
                "Exhaustion is eligible only after a recent cascade.",
                "State describes market structure; it is not an automatic trade instruction.",
            ],
        },
    }
    if persist:
        _record(payload)
    return payload
