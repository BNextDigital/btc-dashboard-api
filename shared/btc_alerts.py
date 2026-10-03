"""BTC alert classification and synthesis, shared by collector and snapshot API."""
from datetime import datetime, timezone
from typing import Optional


# Match complete labels only: a substantive sentence containing these words
# is still a signal. User-authored alert text remains unchanged for display.
_NO_ALERT = {"", "—", "–", "-", "no alert", "no alerts", "none", "no data", "error"}
_LEVELS = {"none", "neutral", "notable", "extreme"}


def classify_alert(alert: Optional[str], level: Optional[str] = None) -> str:
    label = " ".join((alert or "").split()).casefold()
    if label in _NO_ALERT:
        return "none"
    # Numeric threshold rules can assign severity without saying "Extreme"
    # in the label (e.g. futures backwardation). Preserve those decisions.
    if level in _LEVELS:
        return level
    if "extreme" in label and "accumulation" not in label:
        return "extreme"
    if label in {"accumulation", "normal"}:
        return "neutral"
    return "notable"


def normalize_metrics(metrics: dict) -> dict:
    """Repair stale classifications without mutating cached or saved inputs."""
    result = {}
    for key, metric in metrics.items():
        if isinstance(metric, dict) and "alert" in metric:
            result[key] = {
                **metric,
                "alert_level": classify_alert(metric["alert"], metric.get("alert_level")),
            }
        else:
            result[key] = metric
    return result


def summarize_alerts(metrics: dict) -> dict:
    metrics = normalize_metrics(metrics)
    active_alerts = []
    for m in metrics.values():
        if m.get("alert_level") in ("extreme", "notable", "neutral"):
            active_alerts.append({
                "metric":  m["name"],
                "alert":   m["alert"],
                "level":   m["alert_level"],
                "current": m["current"],
            })

    level_order = {"extreme": 0, "notable": 1, "neutral": 2}
    active_alerts.sort(key=lambda a: level_order.get(a["level"], 3))

    extreme_count = sum(1 for a in active_alerts if a["level"] == "extreme")
    notable_count = sum(1 for a in active_alerts if a["level"] == "notable")

    if extreme_count >= 2:
        structure = "Multiple extreme signals active"
    elif extreme_count == 1 and notable_count >= 2:
        structure = "One extreme signal with elevated backdrop"
    elif extreme_count == 1:
        structure = f"Extreme {active_alerts[0]['metric'].lower()} signal"
    elif notable_count >= 3:
        structure = "Broad notable signals across metrics"
    elif notable_count >= 1:
        structure = "Notable signals — monitor closely"
    else:
        structure = "No significant alerts active"

    return {
        "structure":     structure,
        "extreme_count": extreme_count,
        "notable_count": notable_count,
        "active_alerts": active_alerts,
        "total_alerts":  len(active_alerts),
    }


def build_causal(metrics: dict, generated_at: Optional[str] = None) -> dict:
    metrics = normalize_metrics(metrics)

    def weight_from_level(level: str) -> str:
        return {"extreme": "extreme", "notable": "strong", "neutral": "moderate"}.get(level, "moderate")

    def derive_state(m: dict) -> str:
        alert   = m.get("alert") or "—"
        pattern = m.get("pattern", "—")
        current = m.get("current", "—")
        if alert != "—":
            base = alert.lower()
            return f"{base} · {pattern.lower()}" if pattern != "—" else base
        return pattern.lower() if pattern != "—" else f"at {current}"

    chain = [
        {
            "label":  "ETF & institutional flow",
            "state":  derive_state(metrics["etf_flow"]),
            "weight": weight_from_level(metrics["etf_flow"]["alert_level"]),
        },
        {
            "label":  "Price action",
            "state":  derive_state(metrics["price_move"]),
            "weight": weight_from_level(metrics["price_move"]["alert_level"]),
        },
        {
            "label":  "Volume",
            "state":  derive_state(metrics["volume"]),
            "weight": weight_from_level(metrics["volume"]["alert_level"]),
        },
        {
            "label":  "Funding",
            "state":  derive_state(metrics["funding"]),
            "weight": weight_from_level(metrics["funding"]["alert_level"]),
        },
        {
            "label":  "Capital (realized cap)",
            "state":  derive_state(metrics["realized_cap"]),
            "weight": weight_from_level(metrics["realized_cap"]["alert_level"]),
        },
        {
            "label":  "CME basis (cash & carry)",
            "state":  derive_state(metrics["cme_basis"]),
            "weight": weight_from_level(metrics["cme_basis"]["alert_level"]),
        },
        {
            "label":  "Stablecoin liquidity (USDT + USDC)",
            "state":  derive_state(metrics["stablecoin_supply"]),
            "weight": weight_from_level(metrics["stablecoin_supply"]["alert_level"]),
        },
        {
            "label":  "BTC dominance",
            "state":  derive_state(metrics["btc_dominance"]),
            "weight": weight_from_level(metrics["btc_dominance"]["alert_level"]),
        },
    ]

    return {
        "chain":         chain,
        "contradiction": _derive_contradiction(metrics),
        "generated_at":  generated_at or datetime.now(timezone.utc).isoformat(),
    }


def _derive_contradiction(metrics: dict) -> str:
    funding_level  = metrics["funding"]["alert_level"]
    cap_level      = metrics["realized_cap"]["alert_level"]
    etf_level      = metrics["etf_flow"]["alert_level"]
    oi_level       = metrics["open_interest"]["alert_level"]
    volume_pattern = metrics["volume"].get("pattern", "—")
    funding_alert  = (metrics["funding"].get("alert") or "—").lower()
    basis_level    = metrics["cme_basis"]["alert_level"]
    basis_alert    = (metrics["cme_basis"].get("alert") or "—").lower()

    if "shorting" in funding_alert and cap_level in ("notable", "extreme"):
        return "Extreme short positioning against strong capital inflow — leverage and spot diverging."

    if "leverage" in funding_alert and cap_level == "none":
        return "Elevated leverage with no corresponding capital inflow — positioning appears speculative."

    if oi_level in ("notable", "extreme") and volume_pattern == "Absorption":
        return "Large open position base with absorption volume — significant supply being absorbed."

    if etf_level in ("notable", "extreme") and "leverage" in funding_alert:
        return "Institutional inflow (ETF) alongside elevated retail leverage — capital quality diverging."

    if basis_level == "extreme" and "leverage" in funding_alert:
        return "Extreme CME basis alongside elevated funding — institutional carry demand meeting retail leverage."

    if "backwardation" in basis_alert and etf_level in ("notable", "extreme"):
        return "Futures backwardation despite ETF inflow — unusual structure, spot demand not translating to futures premium."

    if oi_level in ("notable", "extreme") and volume_pattern == "Distribution":
        return "Large open positions with distribution volume — crowded trade showing supply pressure."

    active_signals = [m for m in metrics.values() if m.get("alert_level") in ("notable", "extreme")]
    if len(active_signals) >= 3:
        return "Multiple signals elevated simultaneously — broad market activation across metrics."
    if len(active_signals) == 0:
        return "No significant contradictions — market structure is neutral across monitored metrics."

    return "Monitor for developing contradictions as signals evolve."
