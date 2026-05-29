"""
Benchmark calculation layer.

Each formatter takes raw numeric inputs and returns the standardized
metric schema:  current, d7, vs30d, percentile, alert, pattern, spark.

Formatters are intentionally dumb — they apply threshold rules to numbers.
They do NOT output bullish/bearish conclusions.
Interpretation is the user's job.

Thresholds come from the build guide (Step 8 — industry conventions):
- Percentile >= 90  → Extreme
- Percentile >= 75  → Elevated / Notable
- Percentile <= 10  → Extreme (low end)
"""

from typing import Optional


# ─── Shared helpers ────────────────────────────────────────────────────────

def _format_money(value: float) -> str:
    """Format a USD amount with sign + compact suffix. +450_000_000 → '+$450M'."""
    sign = "+" if value >= 0 else "-"
    abs_v = abs(value)
    if abs_v >= 1e9:
        return f"{sign}${abs_v / 1e9:.1f}B"
    if abs_v >= 1e6:
        return f"{sign}${abs_v / 1e6:.0f}M"
    if abs_v >= 1e3:
        return f"{sign}${abs_v / 1e3:.0f}k"
    return f"{sign}${abs_v:.0f}"


def _format_btc(value: float) -> str:
    """Format a BTC quantity. -12_000 → '-12,000 BTC'."""
    sign = "" if value >= 0 else "-"
    abs_v = abs(value)
    if abs_v >= 1e3:
        return f"{sign}{abs_v / 1e3:.0f}k BTC" if abs_v >= 1e5 else f"{sign}{abs_v:,.0f} BTC"
    return f"{sign}{abs_v:,.0f} BTC"


def _format_pct_change(ratio: float) -> str:
    """0.85 → '+85%', -0.17 → '-17%'."""
    return f"{ratio * 100:+.0f}%"


def _classify_alert(alert: str) -> str:
    """Map alert text to severity level for frontend styling."""
    if alert == "—":
        return "none"
    if "Extreme" in alert and "Accumulation" not in alert:
        return "extreme"
    if alert in ("Accumulation",):
        return "neutral"
    return "notable"


# ─── ETF FLOW ──────────────────────────────────────────────────────────────

def format_etf_flow(
    current_daily: float,
    last_7d_sum: float,
    avg_30d: float,
    percentile_90d: float,
    **kwargs,
) -> dict:
    ratio       = last_7d_sum / avg_30d if avg_30d else 0
    alert       = kwargs.get("_alert_override")
    alert_level = kwargs.get("_alert_level_override")
    aum_total   = kwargs.get("_aum_total")

    if not alert:
        if percentile_90d > 90:   alert = "Extreme inflow"
        elif ratio > 2.0:         alert = "Strong acceleration"
        elif ratio > 1.5:         alert = "Flow acceleration"
        else:                     alert = "—"
    if not alert_level:
        alert_level = _classify_alert(alert)

    # Flow card — directional signal only, no AUM
    flow_str = _format_money(current_daily) if current_daily else "—"

    return {
        "name":        "ETF Flow",
        "category":    "Flow",
        "current":     flow_str,
        "current_dir": "up" if current_daily >= 0 else "down",
        "d7":          _format_money(last_7d_sum) if last_7d_sum else "—",
        "vs30d":       _format_pct_change(ratio - 1) if ratio else "—",
        "percentile":  round(percentile_90d),
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     "Flow acceleration" if ratio > 1.5 else "—",
        "spark":       kwargs.get("_spark", []),
    }


def format_etf_aum(
    current_daily: float,
    last_7d_sum: float,
    avg_30d: float,
    percentile_90d: float,
    **kwargs,
) -> dict:
    aum_total = kwargs.get("_aum_total")
    if not aum_total:
        return None

    aum_str     = f"${aum_total / 1e9:.1f}B"
    percentile  = round(percentile_90d)

    if percentile >= 90:   alert, alert_level = "AUM at 90d high", "extreme"
    elif percentile >= 70: alert, alert_level = "Elevated AUM", "notable"
    elif percentile <= 20: alert, alert_level = "AUM near 90d low", "notable"
    else:                  alert, alert_level = "—", "none"

    return {
        "name":        "ETF AUM",
        "category":    "Flow",
        "current":     aum_str,
        "current_dir": "up" if (last_7d_sum or 0) >= 0 else "down",
        "d7":          _format_money(last_7d_sum) if last_7d_sum else "—",
        "vs30d":       f"${avg_30d / 1e9:.1f}B 30d avg" if avg_30d else "—",
        "percentile":  percentile,
        "alert":       alert,
        "alert_level": alert_level,
        "pattern":     f"Total AUM across IBIT, FBTC, ARKB, BITB + 4 others",
        "spark":       kwargs.get("_spark", []),
    }


# ─── FUNDING ───────────────────────────────────────────────────────────────

def format_funding(
    current_rate: float,
    avg_7d: float,
    avg_30d: float,
    percentile_90d: float,
    spread: float = 0.0,
    exchange_rates: dict = None,
    high_exchange: str = "",
    low_exchange: str = "",
    **kwargs,
) -> dict:
    ratio = avg_7d / avg_30d if avg_30d else 0
    if percentile_90d >= 90:
        alert = "Extreme leverage"
    elif percentile_90d >= 75:
        alert = "Elevated leverage"
    elif percentile_90d <= 10:
        alert = "Extreme shorting"
    else:
        alert = "—"

    pattern = "Leveraged move" if percentile_90d >= 75 else "—"

    # Spread label
    spread_pct = spread * 100
    if spread_pct > 0.005:
        spread_label = f"Wide · {spread_pct:.4f}% range"
    else:
        spread_label = f"Tight · {spread_pct:.4f}% range"

    return {
        "name":           "Funding",
        "category":       "Derivatives",
        "current":        f"{current_rate * 100:.4f}%",
        "current_dir":    "up" if current_rate >= 0 else "down",
        "d7":             f"{avg_7d * 100:.4f}% avg",
        "vs30d":          _format_pct_change(ratio - 1),
        "percentile":     round(percentile_90d),
        "alert":          alert,
        "alert_level":    _classify_alert(alert),
        "pattern":        pattern,
        "spark":          kwargs.get("_spark", []),
        "spread":         round(spread_pct, 4),
        "spread_label":   spread_label,
        "exchange_rates": exchange_rates or {},
        "high_exchange":  high_exchange,
        "low_exchange":   low_exchange,
    }
# ─── OPEN INTEREST ─────────────────────────────────────────────────────────

def format_open_interest(
    current_usd: float,
    growth_7d_pct: float,
    growth_30d_pct: float,
    percentile_90d: float,
    **kwargs,
) -> dict:
    if growth_7d_pct > 0.25:
        alert = "Extreme build-up"
    elif growth_7d_pct > 0.15:
        alert = "Rapid build-up"
    elif percentile_90d >= 90:
        alert = "Extreme OI"
    else:
        alert = "—"

    return {
        "name":        "Open Interest",
        "category":    "Derivatives",
        "current":     _format_money(current_usd),
        "current_dir": "up" if growth_7d_pct >= 0 else "down",
        "d7":          _format_pct_change(growth_7d_pct),
        "vs30d":       _format_pct_change(growth_30d_pct),
        "percentile":  round(percentile_90d),
        "alert":       alert,
        "alert_level": _classify_alert(alert),
        "pattern":     "—",
        "spark":       kwargs.get("_spark", []),
    }


# ─── EXCHANGE NETFLOW ──────────────────────────────────────────────────────

def format_exchange_netflow(
    current_btc: float,
    sum_7d_btc: float,
    avg_30d_btc: float,
    percentile_90d: float,
    **kwargs,
) -> dict:
    ratio_30d = sum_7d_btc / avg_30d_btc if avg_30d_btc else 0

    if sum_7d_btc < 0 and abs(sum_7d_btc) > abs(avg_30d_btc) * 1.5:
        alert   = "Strong outflow"
        pattern = "Supply leaving exchanges"
    elif sum_7d_btc > abs(avg_30d_btc) * 1.5:
        alert   = "Strong inflow"
        pattern = "Supply returning to exchanges"
    else:
        alert   = "—"
        pattern = "—"

    return {
        "name":        "Exchange Netflow",
        "category":    "On-chain",
        "current":     _format_btc(current_btc),
        "current_dir": "down" if current_btc < 0 else "up",
        "d7":          _format_btc(sum_7d_btc),
        "vs30d":       f"{ratio_30d:+.1f}x" if ratio_30d else "—",
        "percentile":  round(percentile_90d),
        "alert":       alert,
        "alert_level": _classify_alert(alert),
        "pattern":     pattern,
        "spark":       kwargs.get("_spark", []),
    }


# ─── VOLUME ────────────────────────────────────────────────────────────────

def format_volume(
    ratio_30d: float,
    ratio_7d: float,
    percentile_90d: float,
    price_change_pct: float,
    **kwargs,
) -> dict:
    if ratio_30d > 2.0:
        alert = "Extreme activity"
    elif ratio_30d > 1.5:
        alert = "High activity"
    else:
        alert = "—"

    if ratio_30d > 1.5 and abs(price_change_pct) < 0.01:
        pattern = "Absorption"
    elif ratio_30d > 1.5 and price_change_pct < -0.01:
        pattern = "Distribution"
    else:
        pattern = "—"

    return {
        "name":        "Volume",
        "category":    "Flow",
        "current":     f"{ratio_30d:.1f}x 30d avg",
        "current_dir": "up",
        "d7":          f"{ratio_7d:.1f}x",
        "vs30d":       _format_pct_change(ratio_30d - 1),
        "percentile":  round(percentile_90d),
        "alert":       alert,
        "alert_level": _classify_alert(alert),
        "pattern":     pattern,
        "spark":       kwargs.get("_spark", []),
    }


# ─── PRICE MOVE ────────────────────────────────────────────────────────────

def format_price_move(
    daily_change_pct: float,
    week_change_pct: float,
    avg_daily_30d: float,
    percentile_90d: float,
    **kwargs,
) -> dict:
    abs_daily = abs(daily_change_pct)

    if abs_daily > 0.08:
        alert = "Extreme move"
    elif abs_daily > 0.05:
        alert = "Large move"
    else:
        alert = "—"

    pattern = "Breakout test" if abs_daily > 0.05 else "—"

    return {
        "name":        "Price Move",
        "category":    "Price",
        "current":     f"{daily_change_pct * 100:+.1f}%",
        "current_dir": "up" if daily_change_pct >= 0 else "down",
        "d7":          f"{week_change_pct * 100:+.1f}%",
        "vs30d":       f"{avg_daily_30d * 100:+.1f}%",
        "percentile":  round(percentile_90d),
        "alert":       alert,
        "alert_level": _classify_alert(alert),
        "pattern":     pattern,
        "spark":       kwargs.get("_spark", []),
    }


# ─── REALIZED CAP GROWTH ───────────────────────────────────────────────────

def format_realized_cap(
    growth_pct: float,
    growth_7d_pct: float,
    avg_30d_pct: float,
    percentile_90d: float,
    **kwargs,
) -> dict:
    if growth_pct > 0.03:
        alert = "Strong capital inflow"
    elif growth_pct > 0.01:
        alert = "Capital inflow"
    elif growth_pct < 0:
        alert = "Capital outflow"
    else:
        alert = "—"

    return {
        "name":        "Realized Cap Growth",
        "category":    "On-chain",
        "current":     f"{growth_pct * 100:+.1f}%",
        "current_dir": "up" if growth_pct >= 0 else "down",
        "d7":          f"{growth_7d_pct * 100:+.1f}%",
        "vs30d":       f"{avg_30d_pct * 100:+.1f}% mo avg",
        "percentile":  round(percentile_90d),
        "alert":       alert,
        "alert_level": _classify_alert(alert),
        "pattern":     "—",
        "spark":       kwargs.get("_spark", []),
    }


# ─── LTH SUPPLY CHANGE ─────────────────────────────────────────────────────

def format_lth_supply(
    change_7d_btc: float,
    change_30d_btc: float,
    change_30d_pct: float,
    percentile_90d: float,
    **kwargs,
) -> dict:
    if change_7d_btc > 50_000:
        alert   = "Strong accumulation"
        pattern = "LTH absorbing"
    elif change_7d_btc > 0:
        alert   = "Accumulation"
        pattern = "LTH absorbing"
    elif change_7d_btc < -50_000:
        alert   = "Strong distribution"
        pattern = "LTH distributing"
    else:
        alert   = "—"
        pattern = "—"

    return {
        "name":        "LTH Supply Change",
        "category":    "On-chain",
        "current":     _format_btc(change_7d_btc),
        "current_dir": "up" if change_7d_btc >= 0 else "down",
        "d7":          _format_btc(change_30d_btc),
        "vs30d":       f"{change_30d_pct * 100:+.1f}%",
        "percentile":  round(percentile_90d),
        "alert":       alert,
        "alert_level": _classify_alert(alert),
        "pattern":     pattern,
        "spark":       kwargs.get("_spark", []),
    }
