"""
macro_day2.py — Day 2 macro interpretation layer.

Enriches the existing /macro/metrics payload without replacing legacy cards.
Core chain:
    macro trigger -> expectations repricing -> rates/real rates
    -> financial conditions -> cross-asset confirmation -> regime diagnosis

Reuses existing shared caches. No synthetic market values are invented.
Fed policy probabilities use CME methodology over Yahoo ZQ futures unless an official feed is supplied.
"""

from __future__ import annotations

import calendar
import json
import math
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from shared.fred_cache import get_series as _fred
from shared.yf_cache import get_series as _yf


DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
FEDWATCH_SNAPSHOT_PATH = DATA_DIR / "fedwatch_snapshot.json"
MACRO_TRIGGER_PATH = DATA_DIR / "macro_trigger.json"

# Official FOMC decision dates published by the Federal Reserve.
# Keep this calendar explicit/auditable instead of guessing meeting dates.
FOMC_DECISION_DATES = [
    date(2026, 1, 28),
    date(2026, 3, 18),
    date(2026, 4, 29),
    date(2026, 6, 17),
    date(2026, 7, 29),
    date(2026, 9, 16),
    date(2026, 10, 28),
    date(2026, 12, 9),
    date(2027, 1, 27),
    date(2027, 3, 17),
    date(2027, 4, 28),
    date(2027, 6, 9),
    date(2027, 7, 28),
    date(2027, 9, 15),
    date(2027, 10, 27),
    date(2027, 12, 8),
]



def _safe_float(value: Any) -> Optional[float]:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _pct_rank(values: list[float], current: Optional[float]) -> Optional[int]:
    if current is None or len(values) < 5:
        return None
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return None
    below = sum(1 for value in clean if value < current)
    return round(below / len(clean) * 100)


def _pct_change(current: float, previous: float) -> Optional[float]:
    if previous == 0:
        return None
    return (current / previous - 1.0) * 100


def _fmt_signed(value: Optional[float], decimals: int = 1, suffix: str = "") -> str:
    if value is None:
        return "—"
    return f"{value:+.{decimals}f}{suffix}"


def _load_json_file(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except Exception as exc:
        print(f"[macro_day2] Failed reading {path.name}: {exc}")
        return None


RATE_SERIES = {
    "yield_2y": ("DGS2", "2Y Treasury"),
    "yield_10y": ("DGS10", "10Y Treasury"),
    "real_yield_10y": ("DFII10", "10Y Real Yield"),
    "breakeven_5y": ("T5YIE", "5Y Breakeven"),
    "breakeven_10y": ("T10YIE", "10Y Breakeven"),
    "breakeven_5y5y": ("T5YIFR", "5Y5Y Inflation"),
}


def _rate_metric(series_id: str, label: str) -> dict:
    try:
        pairs = _fred(series_id)
    except Exception as exc:
        return {
            "label": label, "current": None, "d1_bp": None, "d5_bp": None,
            "percentile": None, "source": f"FRED · {series_id}", "error": str(exc),
        }

    if not pairs:
        return {
            "label": label, "current": None, "d1_bp": None, "d5_bp": None,
            "percentile": None, "source": f"FRED · {series_id}", "error": "No data",
        }

    values = [float(value) for _, value in pairs]
    current = values[-1]
    d1_bp = (current - values[-2]) * 100 if len(values) >= 2 else None
    d5_bp = (current - values[-6]) * 100 if len(values) >= 6 else None

    return {
        "label": label,
        "current": round(current, 3),
        "d1_bp": round(d1_bp, 1) if d1_bp is not None else None,
        "d5_bp": round(d5_bp, 1) if d5_bp is not None else None,
        "percentile": _pct_rank(values[-252:], current),
        "source": f"FRED · {series_id}",
        "as_of": pairs[-1][0],
    }


def _build_rates() -> dict:
    return {
        key: _rate_metric(series_id, label)
        for key, (series_id, label) in RATE_SERIES.items()
    }



def _policy_read(rates: dict, policy_sensor: dict, fedwatch: dict) -> dict:
    y2 = rates.get("yield_2y", {})
    y10 = rates.get("yield_10y", {})
    real10 = rates.get("real_yield_10y", {})
    be10 = rates.get("breakeven_10y", {})

    y2_bp = _safe_float(y2.get("d1_bp"))
    y10_bp = _safe_float(y10.get("d1_bp"))
    real_bp = _safe_float(real10.get("d1_bp"))
    be_bp = _safe_float(be10.get("d1_bp"))

    zt_pct = _safe_float(policy_sensor.get("d1_pct"))
    zt_signal = policy_sensor.get("signal")
    zt_date = _parse_iso_date(policy_sensor.get("as_of"))
    y2_date = _parse_iso_date(y2.get("as_of"))
    zt_is_fresher = bool(zt_date and (not y2_date or zt_date > y2_date))

    expected_delta = _safe_float(
        fedwatch.get("expected_change_bp_change_1d")
        if fedwatch.get("status") == "connected"
        else None
    )

    policy_direction = "neutral"
    policy_source = "2Y Treasury"

    # Repricing of the expected meeting move is the cleanest first choice.
    if expected_delta is not None and expected_delta >= 2.5:
        policy_direction = "tightening"
        policy_source = "Fed Funds futures"
    elif expected_delta is not None and expected_delta <= -2.5:
        policy_direction = "easing"
        policy_source = "Fed Funds futures"
    # If FRED's 2Y observation is a session behind, use ZT direction rather than
    # pretending yesterday's cash yield is today's policy sensor.
    elif zt_is_fresher and zt_signal in ("tightening", "easing"):
        policy_direction = str(zt_signal)
        policy_source = "2Y T-Note futures"
    elif y2_bp is not None and y2_bp >= 5:
        policy_direction = "tightening"
    elif y2_bp is not None and y2_bp <= -5:
        policy_direction = "easing"

    if policy_direction == "tightening":
        if (real_bp or 0) >= 2 and (be_bp or 0) >= 2:
            label = "Hawkish inflation repricing"
            explanation = (
                f"{policy_source} points tighter while real yields and inflation "
                "compensation are rising together."
            )
        elif (real_bp or 0) >= 2:
            label = "Hawkish real-rate repricing"
            explanation = (
                f"{policy_source} points tighter and real discount rates are rising."
            )
        else:
            label = "Hawkish policy repricing"
            explanation = f"{policy_source} points to a less-dovish expected policy path."
        level = "tightening"

    elif policy_direction == "easing":
        if (real_bp or 0) <= -2:
            label = "Dovish repricing"
            explanation = (
                f"{policy_source} points easier and real yields are falling. "
                "Cross-asset confirmation determines whether this is benign or a growth scare."
            )
        else:
            label = "Policy easing repricing"
            explanation = (
                f"{policy_source} points easier, but real yields are not confirming strongly."
            )
        level = "easing"

    elif (y10_bp or 0) >= 5 and (real_bp or 0) >= 3:
        label = "Long-end tightening"
        level = "tightening"
        explanation = "The long end and real discount rate are rising without a decisive policy repricing."

    elif (y10_bp or 0) <= -5 and (real_bp or 0) <= -3:
        label = "Long-end easing"
        level = "easing"
        explanation = "Long nominal and real yields are easing while policy pricing is steadier."

    else:
        label = "Rates broadly stable"
        level = "neutral"
        explanation = "No large policy, 2Y-futures or real-rate repricing is visible."

    return {
        "label": label,
        "level": level,
        "explanation": explanation,
        "policy_direction": policy_direction,
        "policy_source": policy_source,
        "components": {
            "yield_2y_d1_bp": y2_bp,
            "yield_10y_d1_bp": y10_bp,
            "real_10y_d1_bp": real_bp,
            "breakeven_10y_d1_bp": be_bp,
            "two_year_future_d1_pct": zt_pct,
            "two_year_future_is_fresher": zt_is_fresher,
            "expected_change_bp_change_1d": expected_delta,
        },
    }


def _market_metric(key: str, label: str) -> dict:
    try:
        series = _yf(key)
    except Exception as exc:
        return {
            "label": label, "current": None, "d1_pct": None, "d5_pct": None,
            "percentile": None, "source": f"Yahoo Finance · {key}", "error": str(exc),
        }

    if series is None or len(series) == 0:
        return {
            "label": label, "current": None, "d1_pct": None, "d5_pct": None,
            "percentile": None, "source": f"Yahoo Finance · {key}", "error": "No data",
        }

    values = [float(v) for v in series.tolist()]
    current = values[-1]
    d1_pct = _pct_change(current, values[-2]) if len(values) >= 2 else None
    d5_pct = _pct_change(current, values[-6]) if len(values) >= 6 else None

    return {
        "label": label,
        "current": round(current, 4),
        "d1_pct": round(d1_pct, 2) if d1_pct is not None else None,
        "d5_pct": round(d5_pct, 2) if d5_pct is not None else None,
        "percentile": _pct_rank(values[-252:], current),
        "source": f"Yahoo Finance · {key}",
        "as_of": str(series.index[-1].date()) if len(series.index) else None,
    }


def _build_policy_sensor() -> dict:
    """
    Same-session policy-direction sensor using 2Y T-Note futures (ZT=F).

    Futures prices move inversely to yields:
      price down -> 2Y yield pressure up -> tightening
      price up   -> 2Y yield pressure down -> easing

    We intentionally do NOT convert the futures price move into basis points;
    that would require contract/DV01 assumptions and create false precision.
    """
    try:
        series = _yf("two_year_note_future")
    except Exception as exc:
        series = None
        error = str(exc)
    else:
        error = None

    if series is None or len(series) == 0:
        return {
            "label": "2Y T-Note Futures",
            "current": None,
            "d1_pct": None,
            "d5_pct": None,
            "source": "Yahoo Finance · ZT=F",
            "signal": "unavailable",
            "yield_direction": None,
            "error": error or "No data",
            "note": "2Y T-Note futures price moves inversely to the underlying yield.",
        }

    values = [float(v) for v in series.tolist()]
    current = values[-1]
    d1_pct = _pct_change(current, values[-2]) if len(values) >= 2 else None
    d5_pct = _pct_change(current, values[-6]) if len(values) >= 6 else None

    # ZT's tick size is small, so keep more precision than generic market cards.
    # A 0.01% daily price move is enough to register direction, but we still
    # label the output as a direction sensor rather than a yield-bp estimate.
    if d1_pct is None:
        signal = "unavailable"
        yield_direction = None
    elif d1_pct <= -0.01:
        signal = "tightening"
        yield_direction = "up"
    elif d1_pct >= 0.01:
        signal = "easing"
        yield_direction = "down"
    else:
        signal = "neutral"
        yield_direction = "flat"

    return {
        "label": "2Y T-Note Futures",
        "current": round(current, 5),
        "d1_pct": round(d1_pct, 4) if d1_pct is not None else None,
        "d5_pct": round(d5_pct, 4) if d5_pct is not None else None,
        "percentile": _pct_rank(values[-252:], current),
        "source": "Yahoo Finance · ZT=F",
        "as_of": str(series.index[-1].date()) if len(series.index) else None,
        "signal": signal,
        "yield_direction": yield_direction,
        "note": "2Y T-Note futures price moves inversely to the underlying yield.",
    }


def _parse_iso_date(value: Any) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _latest_date(*values: Any) -> Optional[date]:
    dates = [d for d in (_parse_iso_date(v) for v in values) if d is not None]
    return max(dates) if dates else None



def _group_as_of(items: list[dict]) -> Optional[date]:
    """
    Return the oldest usable observation date in a confirmation group.
    The least-fresh input is the limiting date for a multi-signal read.
    """
    dates = [
        d
        for d in (_parse_iso_date(item.get("as_of")) for item in items if isinstance(item, dict))
        if d is not None
    ]
    return min(dates) if dates else None



def _build_data_alignment(
    rates: dict,
    conditions: dict,
    cross_asset: dict,
    policy_sensor: dict,
    fedwatch: dict,
) -> dict:
    """
    Expose temporal alignment instead of pretending daily FRED and same-session
    market data are synchronized. Lag is counted in weekday sessions so a
    Friday -> Monday handoff is one session, not three stale days.
    """
    def session_gap(older: Optional[date], newer: Optional[date]) -> int:
        if older is None or newer is None or older >= newer:
            return 0
        cursor = older
        sessions = 0
        while cursor < newer:
            cursor = date.fromordinal(cursor.toordinal() + 1)
            if cursor.weekday() < 5:
                sessions += 1
        return sessions

    market_as_of = _group_as_of([
        policy_sensor,
        conditions.get("dxy", {}),
        conditions.get("vix", {}),
        cross_asset.get("nasdaq", {}),
        cross_asset.get("sp500", {}),
        cross_asset.get("btc", {}),
        cross_asset.get("brent", {}),
        {"as_of": fedwatch.get("as_of")} if isinstance(fedwatch, dict) else {},
    ])
    treasury_as_of = _group_as_of([
        rates.get("yield_2y", {}),
        rates.get("yield_10y", {}),
        rates.get("real_yield_10y", {}),
    ])
    inflation_as_of = _group_as_of([
        rates.get("breakeven_5y", {}),
        rates.get("breakeven_10y", {}),
        rates.get("breakeven_5y5y", {}),
    ])
    credit_as_of = _group_as_of([conditions.get("hy_oas", {})])

    groups = {
        "Treasury / real rates": treasury_as_of,
        "Inflation expectations": inflation_as_of,
        "Credit": credit_as_of,
    }

    lagging_groups: list[str] = []
    lag_sessions: dict[str, int] = {}

    if market_as_of is not None:
        for label, group_date in groups.items():
            if group_date is None:
                continue
            gap = session_gap(group_date, market_as_of)
            lag_sessions[label] = gap
            if gap > 0:
                lagging_groups.append(label)

    max_lag_sessions = max(lag_sessions.values(), default=0)
    if market_as_of is None:
        status = "unknown"
        multiplier = 0.85
        note = "Unable to determine a common market-data reference date."
    elif max_lag_sessions == 0:
        status = "aligned"
        multiplier = 1.0
        note = "Rates, inflation, credit and market observations are session-aligned."
    elif max_lag_sessions == 1:
        status = "mixed_freshness"
        multiplier = 0.88
        note = (
            "Some confirmation series lag same-session market data by one session; "
            "the 2Y futures sensor is used for current policy direction when needed."
        )
    else:
        status = "stale_confirmation"
        multiplier = 0.72
        note = (
            "One or more confirmation series lag by multiple sessions; "
            "regime confidence is reduced."
        )

    return {
        "status": status,
        "market_as_of": market_as_of.isoformat() if market_as_of else None,
        "treasury_as_of": treasury_as_of.isoformat() if treasury_as_of else None,
        "inflation_as_of": inflation_as_of.isoformat() if inflation_as_of else None,
        "credit_as_of": credit_as_of.isoformat() if credit_as_of else None,
        "lagging_groups": lagging_groups,
        "lag_sessions": lag_sessions,
        "max_lag_sessions": max_lag_sessions,
        "confidence_multiplier": multiplier,
        "note": note,
    }


def _hy_oas_metric() -> dict:
    try:
        pairs = _fred("BAMLH0A0HYM2")
    except Exception as exc:
        return {
            "label": "HY OAS", "current": None, "d1_bp": None, "d5_bp": None,
            "source": "FRED · BAMLH0A0HYM2", "error": str(exc),
        }

    if not pairs:
        return {
            "label": "HY OAS", "current": None, "d1_bp": None, "d5_bp": None,
            "source": "FRED · BAMLH0A0HYM2", "error": "No data",
        }

    values = [float(value) * 100 for _, value in pairs]
    current = values[-1]
    d1 = current - values[-2] if len(values) >= 2 else None
    d5 = current - values[-6] if len(values) >= 6 else None

    return {
        "label": "HY OAS",
        "current": round(current, 1),
        "d1_bp": round(d1, 1) if d1 is not None else None,
        "d5_bp": round(d5, 1) if d5 is not None else None,
        "percentile": _pct_rank(values[-252:], current),
        "source": "FRED · BAMLH0A0HYM2",
        "as_of": pairs[-1][0],
    }


def _build_conditions() -> dict:
    return {
        "dxy": _market_metric("dxy", "DXY"),
        "vix": _market_metric("vix", "VIX"),
        "hy_oas": _hy_oas_metric(),
    }


def _build_cross_asset() -> dict:
    return {
        "nasdaq": _market_metric("nasdaq", "Nasdaq"),
        "sp500": _market_metric("spx", "S&P 500"),
        "btc": _market_metric("btc_usd", "Bitcoin"),
        "brent": _market_metric("brent", "Brent"),
        "gold": _market_metric("gold", "Gold"),
    }


def _load_macro_trigger() -> Optional[dict]:
    trigger = _load_json_file(MACRO_TRIGGER_PATH)
    if not trigger:
        return None
    result = dict(trigger)
    result.setdefault("source", "normalized macro trigger")
    return result


def _zq_key(year: int, month: int) -> str:
    return f"zq_{year:04d}_{month:02d}"


def _latest_effr() -> tuple[Optional[float], Optional[str]]:
    """Latest daily Effective Federal Funds Rate from FRED DFF."""
    try:
        pairs = _fred("DFF")
    except Exception as exc:
        print(f"[macro_day2] DFF fetch failed: {exc}")
        return None, None

    if not pairs:
        return None, None

    return float(pairs[-1][1]), pairs[-1][0]


def _next_fomc_decision(today: Optional[date] = None) -> Optional[date]:
    today = today or datetime.now(timezone.utc).date()
    for meeting in FOMC_DECISION_DATES:
        if meeting >= today:
            return meeting
    return None


def _probabilities_from_expected_change(change_bp: float) -> dict:
    """
    Convert the expected meeting move into the adjacent 25bp outcomes.

    This mirrors the binary-step assumption in CME's published methodology,
    but these values are derived locally from Yahoo-sourced ZQ prices and are
    NOT licensed CME FedWatch API output.
    """
    if not math.isfinite(change_bp):
        return {
            "cut_probability": None,
            "hold_probability": None,
            "hike_probability": None,
            "outcomes": [],
        }

    direction = -1 if change_bp < 0 else 1
    steps = abs(change_bp) / 25.0
    lower_steps = math.floor(steps)
    upper_steps = lower_steps + 1
    upper_weight = max(0.0, min(1.0, steps - lower_steps))
    lower_weight = 1.0 - upper_weight

    def label(step_count: int) -> str:
        signed_bp = direction * step_count * 25
        if signed_bp < 0:
            return f"Cut {abs(signed_bp)}bp"
        if signed_bp > 0:
            return f"Hike {signed_bp}bp"
        return "Hold"

    outcomes = []
    if lower_weight > 0.0001:
        outcomes.append({
            "target": label(lower_steps),
            "probability": round(lower_weight * 100, 1),
        })
    if upper_weight > 0.0001:
        outcomes.append({
            "target": label(upper_steps),
            "probability": round(upper_weight * 100, 1),
        })

    hold = 0.0
    cut = 0.0
    hike = 0.0
    for outcome in outcomes:
        target = str(outcome["target"])
        probability = float(outcome["probability"])
        if target == "Hold":
            hold += probability
        elif target.startswith("Cut"):
            cut += probability
        elif target.startswith("Hike"):
            hike += probability

    return {
        "cut_probability": round(cut, 1),
        "hold_probability": round(hold, 1),
        "hike_probability": round(hike, 1),
        "outcomes": outcomes,
    }


def _derive_probability_for_price(
    futures_price: float,
    meeting: date,
    pre_meeting_effr: float,
) -> Optional[dict]:
    """
    CME day-count method for one meeting month.

    ZQ price = 100 - expected average EFFR for the contract month.
    CME's published example counts the decision day in the pre-decision bucket.
    """
    days_in_month = calendar.monthrange(meeting.year, meeting.month)[1]
    days_before = meeting.day
    days_after = days_in_month - meeting.day

    if days_after <= 0:
        return None

    monthly_avg_effr = 100.0 - futures_price
    post_meeting_effr = (
        monthly_avg_effr * days_in_month
        - pre_meeting_effr * days_before
    ) / days_after

    expected_change_bp = (post_meeting_effr - pre_meeting_effr) * 100.0

    # Broken/stale quotes can produce absurd meeting moves. Surface unavailable
    # rather than manufacturing probabilities from them.
    if not math.isfinite(expected_change_bp) or abs(expected_change_bp) > 150:
        return None

    probabilities = _probabilities_from_expected_change(expected_change_bp)
    return {
        **probabilities,
        "monthly_implied_effr": round(monthly_avg_effr, 4),
        "pre_meeting_effr": round(pre_meeting_effr, 4),
        "post_meeting_expected_effr": round(post_meeting_effr, 4),
        "expected_change_bp": round(expected_change_bp, 1),
        "days_before": days_before,
        "days_after": days_after,
    }



def _derive_fed_funds_probabilities() -> dict:
    """
    Free FedWatch-style sensor from ZQ contracts in the shared Yahoo batch.

    The output tracks cut / hold / hike probabilities independently plus the
    change in the expected meeting move, so a hiking regime cannot appear
    "unchanged" merely because cut odds are pinned at zero.
    """
    meeting = _next_fomc_decision()
    if meeting is None:
        return {
            "status": "unavailable",
            "mode": "derived",
            "official_cme_fedwatch": False,
            "source": "CME methodology · Yahoo ZQ futures",
            "note": "No future FOMC meeting is present in the configured calendar.",
            "outcomes": [],
        }

    effr, effr_date = _latest_effr()
    if effr is None:
        return {
            "status": "unavailable",
            "mode": "derived",
            "official_cme_fedwatch": False,
            "source": "CME methodology · Yahoo ZQ futures",
            "next_meeting": meeting.isoformat(),
            "note": "Effective Fed Funds Rate (FRED DFF) is unavailable.",
            "outcomes": [],
        }

    key = _zq_key(meeting.year, meeting.month)
    try:
        contract = _yf(key)
    except Exception as exc:
        contract = None
        print(f"[macro_day2] {key} read failed: {exc}")

    if contract is None or len(contract) == 0:
        return {
            "status": "unavailable",
            "mode": "derived",
            "official_cme_fedwatch": False,
            "source": "CME methodology · Yahoo ZQ futures",
            "next_meeting": meeting.isoformat(),
            "note": f"Fed Funds futures contract {key} is unavailable from the shared Yahoo cache.",
            "outcomes": [],
        }

    current_price = float(contract.iloc[-1])
    current = _derive_probability_for_price(current_price, meeting, effr)
    if current is None:
        return {
            "status": "unavailable",
            "mode": "derived",
            "official_cme_fedwatch": False,
            "source": "CME methodology · Yahoo ZQ futures",
            "next_meeting": meeting.isoformat(),
            "note": "The current ZQ quote failed the probability sanity gate.",
            "outcomes": [],
        }

    # Use the SAME EFFR anchor for current and prior contract prices. This
    # isolates futures repricing from a small change in the published EFFR.
    previous = None
    if len(contract) >= 2:
        previous = _derive_probability_for_price(
            float(contract.iloc[-2]),
            meeting,
            effr,
        )

    cut_delta = hold_delta = hike_delta = expected_delta = None
    if previous is not None:
        cut_delta = round(
            float(current["cut_probability"]) - float(previous["cut_probability"]),
            1,
        )
        hold_delta = round(
            float(current["hold_probability"]) - float(previous["hold_probability"]),
            1,
        )
        hike_delta = round(
            float(current["hike_probability"]) - float(previous["hike_probability"]),
            1,
        )
        expected_delta = round(
            float(current["expected_change_bp"]) - float(previous["expected_change_bp"]),
            1,
        )

    probabilities = {
        "cut": float(current["cut_probability"]),
        "hold": float(current["hold_probability"]),
        "hike": float(current["hike_probability"]),
    }
    dominant_outcome = max(probabilities, key=probabilities.get)
    dominant_probability = probabilities[dominant_outcome]
    dominant_delta = {
        "cut": cut_delta,
        "hold": hold_delta,
        "hike": hike_delta,
    }[dominant_outcome]

    # Future ZQ monthly averages are useful as a rate path even when we do not
    # claim full multi-meeting FedWatch outcome trees.
    rate_path = []
    year, month = meeting.year, meeting.month
    for _ in range(9):
        series_key = _zq_key(year, month)
        try:
            series = _yf(series_key)
        except Exception:
            series = None
        if series is not None and len(series):
            px = float(series.iloc[-1])
            rate_path.append({
                "month": f"{year:04d}-{month:02d}",
                "contract_key": series_key,
                "price": round(px, 4),
                "implied_avg_effr": round(100.0 - px, 4),
                "as_of": str(series.index[-1].date()),
            })
        month += 1
        if month == 13:
            month = 1
            year += 1

    return {
        "status": "connected",
        "mode": "derived",
        "official_cme_fedwatch": False,
        "source": "CME methodology · Yahoo ZQ futures",
        "as_of": str(contract.index[-1].date()),
        "effr_as_of": effr_date,
        "next_meeting": meeting.isoformat(),
        "contract_key": key,
        "contract_price": round(current_price, 4),
        "current_effr": round(effr, 4),

        "cut_probability": current["cut_probability"],
        "hold_probability": current["hold_probability"],
        "hike_probability": current["hike_probability"],

        "cut_probability_change_1d_pp": cut_delta,
        "hold_probability_change_1d_pp": hold_delta,
        "hike_probability_change_1d_pp": hike_delta,

        "dominant_outcome": dominant_outcome,
        "dominant_probability": round(dominant_probability, 1),
        "dominant_probability_change_1d_pp": dominant_delta,

        "expected_change_bp": current["expected_change_bp"],
        "expected_change_bp_change_1d": expected_delta,
        "monthly_implied_effr": current["monthly_implied_effr"],
        "post_meeting_expected_effr": current["post_meeting_expected_effr"],
        "outcomes": current["outcomes"],
        "rate_path": rate_path,
        "note": (
            "Derived locally from 30-Day Fed Funds futures using CME's published "
            "day-count methodology. This is not official CME FedWatch API output."
        ),
    }


def _load_fedwatch() -> dict:
    """
    Prefer a future normalized official CME feed when present. Otherwise use
    the free, clearly-labelled ZQ derivation above.
    """
    snapshot = _load_json_file(FEDWATCH_SNAPSHOT_PATH)
    if snapshot:
        result = dict(snapshot)
        result.setdefault("status", "connected")
        result.setdefault("mode", "official")
        result.setdefault("official_cme_fedwatch", True)
        result.setdefault("source", "CME FedWatch")
        result.setdefault("outcomes", [])
        return result

    return _derive_fed_funds_probabilities()



def _build_regime(
    rates: dict,
    conditions: dict,
    cross_asset: dict,
    fedwatch: dict,
    policy_sensor: dict,
    alignment: dict,
) -> dict:
    y2 = rates.get("yield_2y", {})
    y2_bp = _safe_float(y2.get("d1_bp"))
    real_bp = _safe_float(rates.get("real_yield_10y", {}).get("d1_bp"))
    be5_bp = _safe_float(rates.get("breakeven_5y", {}).get("d1_bp"))
    be5_d5 = _safe_float(rates.get("breakeven_5y", {}).get("d5_bp"))
    be10_bp = _safe_float(rates.get("breakeven_10y", {}).get("d1_bp"))

    dxy_pct = _safe_float(conditions.get("dxy", {}).get("d1_pct"))
    vix_pct = _safe_float(conditions.get("vix", {}).get("d1_pct"))
    hy_bp = _safe_float(conditions.get("hy_oas", {}).get("d1_bp"))
    ndx_pct = _safe_float(cross_asset.get("nasdaq", {}).get("d1_pct"))
    btc_pct = _safe_float(cross_asset.get("btc", {}).get("d1_pct"))
    brent_pct = _safe_float(cross_asset.get("brent", {}).get("d1_pct"))
    brent_d5 = _safe_float(cross_asset.get("brent", {}).get("d5_pct"))

    zt_pct = _safe_float(policy_sensor.get("d1_pct"))
    zt_signal = policy_sensor.get("signal")
    zt_date = _parse_iso_date(policy_sensor.get("as_of"))
    y2_date = _parse_iso_date(y2.get("as_of"))
    zt_is_fresher = bool(zt_date and (not y2_date or zt_date > y2_date))

    expected_delta = _safe_float(
        fedwatch.get("expected_change_bp_change_1d")
        if fedwatch.get("status") == "connected"
        else None
    )
    expected_move = _safe_float(
        fedwatch.get("expected_change_bp")
        if fedwatch.get("status") == "connected"
        else None
    )

    evidence: list[str] = []
    hawkish_votes = dovish_votes = stress_votes = risk_on_votes = available_votes = 0

    # Count policy direction ONCE. Prefer expected-path repricing, then same-session
    # 2Y futures if cash FRED is stale, then the official FRED 2Y daily move.
    policy_vote = "neutral"
    if expected_delta is not None and abs(expected_delta) >= 2.5:
        available_votes += 1
        policy_vote = "hawkish" if expected_delta > 0 else "dovish"
        if policy_vote == "hawkish":
            hawkish_votes += 1
        else:
            dovish_votes += 1
        evidence.append(f"Expected FOMC move Δ {expected_delta:+.1f}bp")
    elif zt_is_fresher and zt_signal in ("tightening", "easing"):
        available_votes += 1
        policy_vote = "hawkish" if zt_signal == "tightening" else "dovish"
        if policy_vote == "hawkish":
            hawkish_votes += 1
        else:
            dovish_votes += 1
        evidence.append(
            f"2Y futures {zt_pct:+.2f}%"
            if zt_pct is not None
            else "2Y futures current-session signal"
        )
    elif y2_bp is not None:
        available_votes += 1
        if y2_bp >= 5:
            policy_vote = "hawkish"
            hawkish_votes += 1
            evidence.append(f"2Y {y2_bp:+.1f}bp")
        elif y2_bp <= -5:
            policy_vote = "dovish"
            dovish_votes += 1
            evidence.append(f"2Y {y2_bp:+.1f}bp")

    if real_bp is not None:
        available_votes += 1
        if real_bp >= 3:
            hawkish_votes += 1
            evidence.append(f"10Y real {real_bp:+.1f}bp")
        elif real_bp <= -3:
            dovish_votes += 1
            evidence.append(f"10Y real {real_bp:+.1f}bp")

    if dxy_pct is not None:
        available_votes += 1
        if dxy_pct >= 0.3:
            stress_votes += 1
            evidence.append(f"DXY {dxy_pct:+.2f}%")
        elif dxy_pct <= -0.3:
            risk_on_votes += 1
            evidence.append(f"DXY {dxy_pct:+.2f}%")

    if vix_pct is not None:
        available_votes += 1
        if vix_pct >= 5:
            stress_votes += 1
            evidence.append(f"VIX {vix_pct:+.1f}%")
        elif vix_pct <= -5:
            risk_on_votes += 1
            evidence.append(f"VIX {vix_pct:+.1f}%")

    if hy_bp is not None:
        available_votes += 1
        if hy_bp >= 5:
            stress_votes += 1
            evidence.append(f"HY OAS {hy_bp:+.1f}bp")
        elif hy_bp <= -5:
            risk_on_votes += 1
            evidence.append(f"HY OAS {hy_bp:+.1f}bp")

    if ndx_pct is not None:
        available_votes += 1
        if ndx_pct <= -0.75:
            stress_votes += 1
            evidence.append(f"Nasdaq {ndx_pct:+.2f}%")
        elif ndx_pct >= 0.75:
            risk_on_votes += 1
            evidence.append(f"Nasdaq {ndx_pct:+.2f}%")

    # Inflation impulse: energy is only promoted from "commodity move" to
    # macro inflation pressure when breakevens confirm it.
    energy_hot = (brent_pct is not None and brent_pct >= 3.0) or (
        brent_d5 is not None and brent_d5 >= 7.0
    )
    breakevens_hot = (be5_bp is not None and be5_bp >= 2.0) or (
        be10_bp is not None and be10_bp >= 2.0
    ) or (be5_d5 is not None and be5_d5 >= 5.0)
    energy_inflation_signal = bool(energy_hot and breakevens_hot)

    if energy_inflation_signal:
        if brent_pct is not None:
            evidence.append(f"Brent {brent_pct:+.2f}%")
        if be5_bp is not None:
            evidence.append(f"5Y BE {be5_bp:+.1f}bp")

    if energy_inflation_signal and (
        policy_vote == "hawkish"
        or (expected_move is not None and expected_move >= 10)
        or stress_votes >= 1
    ):
        regime_type, label, level = "energy_inflation_pressure", "Energy Inflation Pressure", "risk_off"
        explanation = (
            "Energy prices and breakeven inflation are rising together while policy or "
            "financial conditions lean tighter. This is an inflationary macro impulse, "
            "not a generic risk-off move."
        )
        aligned_votes = max(2, hawkish_votes + stress_votes)

    elif dovish_votes >= 1 and stress_votes >= 2:
        regime_type, label, level = "bad_dovish", "Bad Dovish", "risk_off"
        explanation = (
            "Rates are repricing easier policy while credit/volatility/growth assets show stress. "
            "The market appears to be pricing growth risk rather than benign disinflation."
        )
        aligned_votes = dovish_votes + stress_votes

    elif dovish_votes >= 1 and risk_on_votes >= 2 and stress_votes == 0:
        regime_type, label, level = "good_dovish", "Good Dovish", "risk_on"
        explanation = (
            "Policy/real-rate expectations are easing while risk assets and financial "
            "conditions remain constructive: a benign easing/disinflation configuration."
        )
        aligned_votes = dovish_votes + risk_on_votes

    elif hawkish_votes >= 2 and stress_votes >= 1:
        regime_type, label, level = "hawkish_inflation", "Hawkish Tightening", "risk_off"
        explanation = (
            "Policy-sensitive and real yields are rising with inflation compensation and tighter "
            "financial conditions."
            if (be10_bp or 0) >= 2
            else
            "Policy-sensitive and real yields are rising with tighter financial conditions."
        )
        aligned_votes = hawkish_votes + stress_votes

    elif hawkish_votes >= 1 and stress_votes >= 2:
        regime_type, label, level = "hawkish_repricing", "Hawkish Repricing", "risk_off"
        explanation = (
            "Policy pricing leans tighter and multiple cross-asset stress signals confirm the move."
        )
        aligned_votes = hawkish_votes + stress_votes

    elif hawkish_votes >= 2:
        regime_type, label, level = "hawkish_repricing", "Hawkish Repricing", "tightening"
        explanation = "Rates are repricing tighter, but cross-asset stress has not yet confirmed strongly."
        aligned_votes = hawkish_votes

    elif stress_votes >= 2:
        regime_type, label, level = "risk_off", "Risk-Off", "risk_off"
        explanation = "Financial conditions are deteriorating without a decisive policy-rate repricing."
        aligned_votes = stress_votes

    elif risk_on_votes >= 2:
        regime_type, label, level = "risk_on", "Risk-On", "risk_on"
        explanation = "Cross-asset conditions are constructive without a strong rates repricing signal."
        aligned_votes = risk_on_votes

    else:
        regime_type, label, level = "mixed", "Mixed / Neutral", "neutral"
        explanation = "The observable signals do not currently form a strong Day 2 transmission regime."
        aligned_votes = max(hawkish_votes, dovish_votes, stress_votes, risk_on_votes)

    raw_confidence = (
        round(min(100.0, 35.0 + (aligned_votes / max(available_votes, 1)) * 65.0))
        if available_votes
        else 0
    )
    freshness_multiplier = _safe_float(alignment.get("confidence_multiplier")) or 1.0
    confidence = round(raw_confidence * freshness_multiplier)

    return {
        "type": regime_type,
        "label": label,
        "level": level,
        "confidence": confidence,
        "confidence_raw": raw_confidence,
        "freshness_adjustment": freshness_multiplier,
        "explanation": explanation,
        "evidence": evidence[:8],
        "btc_d1_pct": btc_pct,
        "inputs_available": available_votes,
        "energy_inflation_signal": energy_inflation_signal,
    }



def _build_transmission(
    trigger: Optional[dict],
    fedwatch: dict,
    rates: dict,
    conditions: dict,
    cross_asset: dict,
    policy_sensor: dict,
    regime: dict,
) -> list[dict]:
    steps: list[dict] = []

    if trigger:
        surprise = _safe_float(trigger.get("surprise"))
        steps.append({
            "stage": "trigger",
            "label": trigger.get("title") or "Macro event",
            "move": (
                _fmt_signed(surprise, 2)
                if surprise is not None
                else str(trigger.get("surprise_label") or "event")
            ),
            "direction": str(trigger.get("surprise_label") or "neutral"),
            "confidence": "observed",
        })

    if fedwatch.get("status") == "connected":
        dominant = str(fedwatch.get("dominant_outcome") or "").lower()
        probability = _safe_float(fedwatch.get("dominant_probability"))
        dominant_delta = _safe_float(fedwatch.get("dominant_probability_change_1d_pp"))
        expected_move = _safe_float(fedwatch.get("expected_change_bp"))
        expected_delta = _safe_float(fedwatch.get("expected_change_bp_change_1d"))

        pieces = []
        if probability is not None and dominant:
            pieces.append(f"{probability:.1f}% {dominant}")
        if expected_move is not None:
            pieces.append(f"expected {_fmt_signed(expected_move, 1, 'bp')}")
        if dominant_delta is not None:
            pieces.append(f"{dominant} odds Δ {_fmt_signed(dominant_delta, 1, 'pp')}")
        if expected_delta is not None:
            pieces.append(f"path Δ {_fmt_signed(expected_delta, 1, 'bp')}")

        direction = "neutral"
        if expected_delta is not None and expected_delta >= 2.5:
            direction = "tightening"
        elif expected_delta is not None and expected_delta <= -2.5:
            direction = "easing"
        elif expected_move is not None and expected_move >= 10:
            direction = "tightening"
        elif expected_move is not None and expected_move <= -10:
            direction = "easing"

        steps.append({
            "stage": "expectations",
            "label": "Next FOMC pricing",
            "move": " · ".join(pieces) if pieces else "Probability path available",
            "direction": direction,
            "confidence": "observed",
        })

    # Use same-session 2Y futures when they are newer than FRED's cash 2Y.
    y2 = rates.get("yield_2y", {})
    zt_date = _parse_iso_date(policy_sensor.get("as_of"))
    y2_date = _parse_iso_date(y2.get("as_of"))
    zt_pct = _safe_float(policy_sensor.get("d1_pct"))
    zt_signal = policy_sensor.get("signal")
    zt_is_fresher = bool(zt_date and (not y2_date or zt_date > y2_date))

    if zt_is_fresher and zt_pct is not None:
        yield_arrow = "↑" if zt_signal == "tightening" else "↓" if zt_signal == "easing" else "→"
        steps.append({
            "stage": "rates",
            "label": "2Y T-Note futures",
            "move": f"price {_fmt_signed(zt_pct, 2, '%')} · yield {yield_arrow}",
            "direction": str(zt_signal if zt_signal in ("tightening", "easing") else "neutral"),
            "confidence": "observed",
        })
    else:
        y2_bp = _safe_float(y2.get("d1_bp"))
        if y2_bp is not None:
            steps.append({
                "stage": "rates",
                "label": "2Y Treasury",
                "move": _fmt_signed(y2_bp, 1, "bp"),
                "direction": "tightening" if y2_bp >= 3 else "easing" if y2_bp <= -3 else "neutral",
                "confidence": "observed",
            })

    real_bp = _safe_float(rates.get("real_yield_10y", {}).get("d1_bp"))
    if real_bp is not None:
        steps.append({
            "stage": "conditions",
            "label": "10Y real yield",
            "move": _fmt_signed(real_bp, 1, "bp"),
            "direction": "tightening" if real_bp >= 2 else "easing" if real_bp <= -2 else "neutral",
            "confidence": "observed",
        })

    # Promote energy into the chain only when inflation expectations confirm it.
    brent_pct = _safe_float(cross_asset.get("brent", {}).get("d1_pct"))
    brent_d5 = _safe_float(cross_asset.get("brent", {}).get("d5_pct"))
    be5_bp = _safe_float(rates.get("breakeven_5y", {}).get("d1_bp"))
    be10_bp = _safe_float(rates.get("breakeven_10y", {}).get("d1_bp"))
    energy_hot = (brent_pct is not None and brent_pct >= 3.0) or (
        brent_d5 is not None and brent_d5 >= 7.0
    )
    breakevens_hot = (be5_bp is not None and be5_bp >= 2.0) or (
        be10_bp is not None and be10_bp >= 2.0
    )

    if energy_hot and breakevens_hot:
        pieces = []
        if brent_pct is not None:
            pieces.append(f"Brent {_fmt_signed(brent_pct, 2, '%')}")
        if be5_bp is not None:
            pieces.append(f"5Y BE {_fmt_signed(be5_bp, 1, 'bp')}")
        elif be10_bp is not None:
            pieces.append(f"10Y BE {_fmt_signed(be10_bp, 1, 'bp')}")
        steps.append({
            "stage": "inflation",
            "label": "Energy / inflation impulse",
            "move": " · ".join(pieces),
            "direction": "tightening",
            "confidence": "strongly_inferred",
        })

    dxy_pct = _safe_float(conditions.get("dxy", {}).get("d1_pct"))
    if dxy_pct is not None:
        steps.append({
            "stage": "conditions",
            "label": "DXY",
            "move": _fmt_signed(dxy_pct, 2, "%"),
            "direction": "tightening" if dxy_pct >= 0.2 else "easing" if dxy_pct <= -0.2 else "neutral",
            "confidence": "observed",
        })

    hy_bp = _safe_float(conditions.get("hy_oas", {}).get("d1_bp"))
    if hy_bp is not None:
        steps.append({
            "stage": "conditions",
            "label": "HY credit spread",
            "move": _fmt_signed(hy_bp, 1, "bp"),
            "direction": "risk_off" if hy_bp >= 3 else "risk_on" if hy_bp <= -3 else "neutral",
            "confidence": "observed",
        })

    ndx_pct = _safe_float(cross_asset.get("nasdaq", {}).get("d1_pct"))
    btc_pct = _safe_float(cross_asset.get("btc", {}).get("d1_pct"))
    if ndx_pct is not None or btc_pct is not None:
        pieces = []
        vals = []
        if ndx_pct is not None:
            pieces.append(f"Nasdaq {_fmt_signed(ndx_pct, 2, '%')}")
            vals.append(ndx_pct)
        if btc_pct is not None:
            pieces.append(f"BTC {_fmt_signed(btc_pct, 2, '%')}")
            vals.append(btc_pct)
        avg = sum(vals) / len(vals)
        steps.append({
            "stage": "cross_asset",
            "label": "Risk assets",
            "move": " · ".join(pieces),
            "direction": "risk_on" if avg >= 0.5 else "risk_off" if avg <= -0.5 else "neutral",
            "confidence": "observed",
        })

    steps.append({
        "stage": "diagnosis",
        "label": regime.get("label", "Macro regime"),
        "move": regime.get("explanation", ""),
        "direction": regime.get("level", "neutral"),
        "confidence": "strongly_inferred",
    })
    return steps



def enrich_macro_metrics(base: dict) -> dict:
    """Return the existing payload plus a backwards-compatible `day2` namespace."""
    try:
        rates = _build_rates()
        conditions = _build_conditions()
        cross_asset = _build_cross_asset()
        policy_sensor = _build_policy_sensor()
        fedwatch = _load_fedwatch()
        trigger = _load_macro_trigger()

        alignment = _build_data_alignment(
            rates,
            conditions,
            cross_asset,
            policy_sensor,
            fedwatch,
        )
        policy_read = _policy_read(rates, policy_sensor, fedwatch)
        regime = _build_regime(
            rates,
            conditions,
            cross_asset,
            fedwatch,
            policy_sensor,
            alignment,
        )
        transmission = _build_transmission(
            trigger,
            fedwatch,
            rates,
            conditions,
            cross_asset,
            policy_sensor,
            regime,
        )

        enriched = dict(base)
        enriched["day2"] = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "trigger": trigger,
            "fedwatch": fedwatch,
            "rates": rates,
            "policy_sensor": policy_sensor,
            "policy_read": policy_read,
            "conditions": conditions,
            "cross_asset": cross_asset,
            "data_alignment": alignment,
            "regime": regime,
            "transmission": transmission,
            "data_mode": "event_relative" if trigger else "daily_repricing",
            "notes": [
                "Without a normalized macro trigger, changes are daily close-to-close rather than event-relative.",
                "Fed policy probabilities are derived from Yahoo ZQ futures unless an official normalized CME feed is supplied.",
                "2Y T-Note futures provide same-session policy direction when FRED cash-yield observations lag.",
            ],
        }
        return enriched

    except Exception as exc:
        print(f"[macro_day2] enrichment failed: {exc}")
        enriched = dict(base)
        enriched["day2"] = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
            "data_mode": "unavailable",
        }
        return enriched


