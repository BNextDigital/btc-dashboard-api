"""
liquidity_depth_routes.py — Spot Depth & Liquidation Cascade Risk

Answers the core question your colleague framed:
  "If nearby liquidation clusters trigger, does spot have the depth to absorb the flow?"

DATA SOURCES (all free, no new API keys required):
  Binance public REST  — /api/v3/depth          (no key, 1200 req/min weight limit)
  Coinbase public REST — /api/v3/brokerage/product_book (no key)
  Kraken public REST   — /0/public/Depth         (no key)
  CoinGlass public API — /public/v2/liquidation/map (COINGLASS_API_KEY already in env)
  main.py OI cache     — current open interest USD pulled from shared metrics cache

INTEGRATION (2 lines in main.py):
    from liquidity_depth_routes import liquidity_router
    app.include_router(liquidity_router)

ENDPOINTS:
    GET /liquidity/depth          — full cascade risk assessment
    GET /liquidity/orderbook      — raw aggregated depth at each band
    GET /liquidity/cascade-score  — single composite score + label (lightweight poll)
    GET /liquidity/cache/flush    — force refresh

METRIC SCHEMA (matches existing dashboard pattern):
    {
      current, d7, vs30d, percentile,
      alert, alert_level, pattern,
      spark,
      # Plus depth-specific fields:
      depth_coverage_ratio, adjusted_coverage,
      cascade_risk_label, cascade_risk_level,
      bands: { pct_0_5, pct_1_0, pct_2_0 },
      venue_breakdown: { binance, coinbase, kraken },
      liquidation_estimate_usd,
      oi_usd,
      slippage_estimate,
      depth_vs_median_pct,
    }

THRESHOLDS (from colleague's framework):
    Depth Coverage (adjusted):
      > 1.5x  → Deep — strong absorption capacity
      1.0-1.5 → Adequate
      0.75-1.0 → Thin — elevated cascade risk
      < 0.75  → Extreme — high cascade risk

    Cascade Risk composite:
      LOW      — coverage > 1.5, funding neutral, OI stable
      MODERATE — coverage 0.75-1.5 OR OI elevated
      HIGH     — coverage < 0.75 AND OI/funding elevated
      EXTREME  — coverage < 0.5 OR all three factors red
"""

from __future__ import annotations

import os
import time
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from fastapi import APIRouter

liquidity_router = APIRouter(prefix="/liquidity", tags=["Liquidity Depth"])

# ── Config ────────────────────────────────────────────────────────────────────

COINGLASS_KEY  = os.getenv("COINGLASS_API_KEY", "")
DATA_DIR       = Path(os.getenv("DATA_DIR", "./data"))

BINANCE_BASE   = "https://api.binance.com"
COINBASE_BASE  = "https://api.coinbase.com"
KRAKEN_BASE    = "https://api.kraken.com"
COINGLASS_BASE = "https://open-api.coinglass.com/public/v2"

# Reliability haircut on visible order book depth.
# Accounts for spoofing, cancellations, maker withdrawal under stress.
# Uses conservative (stress) value — team can tune this.
DEPTH_HAIRCUT_NORMAL   = 0.60   # 60% of visible depth is "reliable" under normal conditions
DEPTH_HAIRCUT_STRESSED = 0.40   # 40% when OI/funding elevated (stressed conditions)

# Cache TTL: order books update continuously — 30s is aggressive but manageable
DEPTH_CACHE_TTL    = 30    # seconds — raw order book
ASSESS_CACHE_TTL   = 60    # seconds — full assessment (includes CoinGlass which is slower)
MEDIAN_HISTORY_LEN = 30    # days of depth snapshots for vs-median calculation

# ── In-memory caches ─────────────────────────────────────────────────────────

_depth_cache    = {"data": None, "ts": 0.0}
_assess_cache   = {"data": None, "ts": 0.0}
_depth_history: list[dict] = []   # rolling 30-entry list of depth snapshots for median calc


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _safe_get(url: str, headers: dict = None, params: dict = None, timeout: int = 8) -> Optional[dict]:
    try:
        r = requests.get(url, headers=headers or {}, params=params or {}, timeout=timeout)
        if r.status_code == 429:
            print(f"[liquidity] Rate limited: {url}")
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[liquidity] GET {url} failed: {e}")
        return None


# ── Order book fetchers ───────────────────────────────────────────────────────

def _fetch_binance_depth() -> Optional[dict]:
    """
    Binance L2 order book — top 500 levels each side.
    Weight cost: 5 (limit=500). Well within 1200/min limit.
    Returns {"bids": [(price, qty), ...], "asks": [...]}
    """
    data = _safe_get(
        f"{BINANCE_BASE}/api/v3/depth",
        params={"symbol": "BTCUSDT", "limit": 500},
    )
    if not data:
        return None
    return {
        "bids": [(float(p), float(q)) for p, q in data.get("bids", [])],
        "asks": [(float(p), float(q)) for p, q in data.get("asks", [])],
        "venue": "Binance",
    }


def _fetch_coinbase_depth() -> Optional[dict]:
    """
    Coinbase Advanced Trade public product book.
    No auth required for market data.
    """
    data = _safe_get(
        f"{COINBASE_BASE}/api/v3/brokerage/market/product_book",
        params={"product_id": "BTC-USD", "limit": 250},
    )
    if not data or "pricebook" not in data:
        return None
    book = data["pricebook"]
    return {
        "bids": [(float(b["price"]), float(b["size"])) for b in book.get("bids", [])],
        "asks": [(float(a["price"]), float(a["size"])) for a in book.get("asks", [])],
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
    # Kraken wraps in result key with dynamic pair name
    result = data.get("result", {})
    pair_data = next(iter(result.values()), {}) if result else {}
    return {
        "bids": [(float(p), float(q)) for p, q, _ in pair_data.get("bids", [])],
        "asks": [(float(p), float(q)) for p, q, _ in pair_data.get("asks", [])],
        "venue": "Kraken",
    }


def _fetch_coinglass_liquidation_map() -> Optional[dict]:
    """
    CoinGlass liquidation heatmap — estimated liquidation clusters by price band.
    Uses existing COINGLASS_API_KEY.
    Returns estimated USD of long/short liquidations near current price.
    """
    if not COINGLASS_KEY:
        return None
    data = _safe_get(
        f"{COINGLASS_BASE}/liquidation/map",
        headers={"coinglassSecret": COINGLASS_KEY},
        params={"symbol": "BTC", "interval": "4h"},
    )
    if not data or data.get("code") != "0":
        print(f"[liquidity] CoinGlass liquidation map: {data.get('msg') if data else 'no response'}")
        return None
    return data.get("data")


# ── Depth aggregation ─────────────────────────────────────────────────────────

def _aggregate_depth(books: list[dict], spot_price: float) -> dict:
    """
    Aggregate bid depth from multiple venues at 0.5%, 1.0%, 2.0% below spot.
    Also aggregates ask depth for short squeeze side.

    Returns USD depth at each band, per venue and combined.
    """
    bands_pct = [0.005, 0.010, 0.020]   # 0.5%, 1%, 2%
    result = {
        "spot_price": spot_price,
        "bid_depth": {f"{int(b*1000)/10}": {"usd": 0.0, "venues": {}} for b in bands_pct},
        "ask_depth": {f"{int(b*1000)/10}": {"usd": 0.0, "venues": {}} for b in bands_pct},
        "venue_totals": {},
        "fetched_at": datetime.utcnow().isoformat() + "Z",
    }

    for book in books:
        if not book:
            continue
        venue = book["venue"]
        venue_bid_2pct = 0.0
        venue_ask_2pct = 0.0

        # Bid side (downside depth — absorbs sell pressure from long liquidations)
        for price, qty in book.get("bids", []):
            drop_pct = (spot_price - price) / spot_price
            usd_val  = price * qty
            for band_pct in bands_pct:
                if drop_pct <= band_pct:
                    band_key = f"{int(band_pct*1000)/10}"
                    result["bid_depth"][band_key]["usd"] += usd_val
                    result["bid_depth"][band_key]["venues"][venue] = \
                        result["bid_depth"][band_key]["venues"].get(venue, 0) + usd_val
            if drop_pct <= 0.020:
                venue_bid_2pct += usd_val

        # Ask side (upside depth — absorbs buy pressure from short liquidations)
        for price, qty in book.get("asks", []):
            rise_pct = (price - spot_price) / spot_price
            usd_val  = price * qty
            for band_pct in bands_pct:
                if rise_pct <= band_pct:
                    band_key = f"{int(band_pct*1000)/10}"
                    result["ask_depth"][band_key]["usd"] += usd_val
                    result["ask_depth"][band_key]["venues"][venue] = \
                        result["ask_depth"][band_key]["venues"].get(venue, 0) + usd_val
            if rise_pct <= 0.020:
                venue_ask_2pct += usd_val

        result["venue_totals"][venue] = {
            "bid_2pct_usd": venue_bid_2pct,
            "ask_2pct_usd": venue_ask_2pct,
        }

    return result


def _venue_concentration(venue_totals: dict, side: str = "bid_2pct_usd") -> float:
    """
    Returns the largest single venue's share of total depth.
    High concentration = fragile aggregate depth.
    """
    totals = [v[side] for v in venue_totals.values() if v.get(side, 0) > 0]
    if not totals or sum(totals) == 0:
        return 1.0
    return max(totals) / sum(totals)


# ── Liquidation estimate ──────────────────────────────────────────────────────

def _estimate_liquidation_exposure(
    liq_map_data,
    spot_price: float,
    oi_usd: float,
) -> dict:
    """
    Estimate USD of liquidations likely within 2% of spot.
    Primary: CoinGlass heatmap data.
    Fallback: OI-based estimate (15% of OI within 2% band is a rough industry heuristic).
    """
    source = "estimated"
    long_liq_usd  = 0.0
    short_liq_usd = 0.0

    if liq_map_data and isinstance(liq_map_data, dict):
        try:
            # CoinGlass returns price-bucketed liquidation data
            # Structure varies by API version — handle gracefully
            price_data = liq_map_data.get("liquidationLevels", liq_map_data.get("data", []))
            if isinstance(price_data, list):
                for entry in price_data:
                    price  = float(entry.get("price", 0))
                    longs  = float(entry.get("longLiquidationUsd", entry.get("l", 0)))
                    shorts = float(entry.get("shortLiquidationUsd", entry.get("s", 0)))
                    drop   = abs(spot_price - price) / spot_price
                    if drop <= 0.020:
                        long_liq_usd  += longs
                        short_liq_usd += shorts
                source = "CoinGlass heatmap"
        except Exception as e:
            print(f"[liquidity] Liquidation map parse error: {e}")

    # Fallback: heuristic from OI
    if long_liq_usd == 0 and short_liq_usd == 0 and oi_usd > 0:
        # ~12-18% of OI typically sits within 2% of spot in a crowded market
        long_liq_usd  = oi_usd * 0.15
        short_liq_usd = oi_usd * 0.08
        source        = "OI estimate (heuristic)"

    return {
        "long_liq_usd":   long_liq_usd,
        "short_liq_usd":  short_liq_usd,
        "net_downside":   long_liq_usd,   # long liquidations = forced sells = downside pressure
        "net_upside":     short_liq_usd,
        "source":         source,
    }


# ── Slippage estimate ─────────────────────────────────────────────────────────

def _estimate_slippage(forced_flow_usd: float, bid_depth: dict) -> str:
    """
    Walk the order book to estimate price impact of forced_flow_usd of selling.
    Returns human-readable slippage estimate string.
    """
    depth_05 = bid_depth.get("0.5", {}).get("usd", 0)
    depth_10 = bid_depth.get("1.0", {}).get("usd", 0)
    depth_20 = bid_depth.get("2.0", {}).get("usd", 0)

    if forced_flow_usd <= 0:
        return "—"

    if forced_flow_usd < depth_05:
        return "< 0.5%"
    elif forced_flow_usd < depth_10:
        return "0.5–1.0%"
    elif forced_flow_usd < depth_20:
        return "1.0–2.0%"
    else:
        overflow = forced_flow_usd - depth_20
        overflow_pct = (overflow / forced_flow_usd) * 3   # rough extrapolation
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
    Returns (label, level) for cascade risk.
    Combines depth coverage with the leverage/flow signals already on the dashboard.
    """
    leverage_stressed = (
        oi_alert_level in ("notable", "extreme") or
        funding_alert_level in ("notable", "extreme")
    )
    supply_pressure = "inflow" in (netflow_alert or "").lower()

    # Pure depth signal
    if adjusted_coverage >= 1.5:
        base_label, base_level = "Deep — strong absorption capacity", "none"
    elif adjusted_coverage >= 1.0:
        base_label, base_level = "Adequate depth", "none"
    elif adjusted_coverage >= 0.75:
        base_label, base_level = "Thin — elevated cascade risk", "notable"
    elif adjusted_coverage >= 0.5:
        base_label, base_level = "Fragile — high cascade risk", "extreme"
    else:
        base_label, base_level = "Critical — severe cascade potential", "extreme"

    # Upgrade risk level when leverage signals compound depth fragility
    if base_level == "none" and leverage_stressed:
        return "Adequate depth · leverage elevated", "none"
    if base_level == "notable" and leverage_stressed and supply_pressure:
        return "Thin depth + crowded leverage + exchange inflow — cascade risk HIGH", "extreme"
    if base_level == "notable" and leverage_stressed:
        return "Thin depth + crowded leverage — monitor closely", "extreme"
    if base_level == "extreme" and leverage_stressed:
        return "Fragile depth + crowded leverage + supply pressure — EXTREME", "extreme"

    return base_label, base_level


# ── Depth vs median ───────────────────────────────────────────────────────────

def _depth_vs_median(current_2pct_usd: float) -> Optional[float]:
    """
    Compare current 2% bid depth against rolling 30-entry median.
    Returns percentage: 100 = at median, 50 = half of median (thinning).
    """
    global _depth_history
    if len(_depth_history) < 3:
        return None
    historical = [e["bid_2pct_usd"] for e in _depth_history if e.get("bid_2pct_usd")]
    if not historical:
        return None
    med = statistics.median(historical)
    if med == 0:
        return None
    return round((current_2pct_usd / med) * 100, 1)


def _record_depth_snapshot(bid_2pct_usd: float):
    """Append current depth to rolling history for median calculation."""
    global _depth_history
    _depth_history.append({
        "ts": time.time(),
        "bid_2pct_usd": bid_2pct_usd,
    })
    # Keep last 30 snapshots
    if len(_depth_history) > MEDIAN_HISTORY_LEN:
        _depth_history = _depth_history[-MEDIAN_HISTORY_LEN:]


# ── Format helpers ────────────────────────────────────────────────────────────

def _fmt_usd(value: float) -> str:
    if value >= 1e9:
        return f"${value/1e9:.2f}B"
    if value >= 1e6:
        return f"${value/1e6:.0f}M"
    return f"${value:,.0f}"


def _fmt_ratio(ratio: float) -> str:
    return f"{ratio:.2f}x"


# ── Core builder ──────────────────────────────────────────────────────────────

def _build_depth_assessment(metrics_cache: dict = None) -> dict:
    """
    Full spot depth + cascade risk assessment.
    metrics_cache: pass in the output of _build_metrics_cached() to avoid double-fetching OI/funding.
    """
    now = time.time()

    # ── Step 1: Fetch order books in parallel-ish (sequential for simplicity — each ~200ms)
    binance_book  = _fetch_binance_depth()
    coinbase_book = _fetch_coinbase_depth()
    kraken_book   = _fetch_kraken_depth()

    books_fetched = [b for b in [binance_book, coinbase_book, kraken_book] if b]
    venues_up     = [b["venue"] for b in books_fetched]

    if not books_fetched:
        return {"error": "All order book sources unavailable", "alert_level": "none"}

    # ── Step 2: Determine spot price (from Binance mid or fallback)
    spot_price = 0.0
    if binance_book and binance_book["bids"] and binance_book["asks"]:
        best_bid = binance_book["bids"][0][0]
        best_ask = binance_book["asks"][0][0]
        spot_price = (best_bid + best_ask) / 2
    elif coinbase_book and coinbase_book["bids"]:
        spot_price = coinbase_book["bids"][0][0]

    if spot_price == 0:
        return {"error": "Could not determine spot price", "alert_level": "none"}

    # ── Step 3: Aggregate depth across venues
    agg = _aggregate_depth(books_fetched, spot_price)

    bid_05  = agg["bid_depth"]["0.5"]["usd"]
    bid_10  = agg["bid_depth"]["1.0"]["usd"]
    bid_20  = agg["bid_depth"]["2.0"]["usd"]
    ask_20  = agg["ask_depth"]["2.0"]["usd"]

    # ── Step 4: Pull OI + funding from metrics cache (avoid re-fetching CoinGecko)
    oi_usd             = 0.0
    oi_alert_level     = "none"
    funding_alert_level = "none"
    netflow_alert      = ""

    if metrics_cache:
        oi_data      = metrics_cache.get("open_interest", {})
        funding_data = metrics_cache.get("funding", {})
        netflow_data = metrics_cache.get("exchange_netflow", {})

        # Parse OI USD from formatted string (e.g., "$28.4B" → 28_400_000_000)
        oi_str = oi_data.get("current", "")
        try:
            oi_str_clean = oi_str.replace("$", "").replace(",", "")
            if "B" in oi_str_clean:
                oi_usd = float(oi_str_clean.replace("B", "")) * 1e9
            elif "M" in oi_str_clean:
                oi_usd = float(oi_str_clean.replace("M", "")) * 1e6
        except Exception:
            pass

        oi_alert_level      = oi_data.get("alert_level", "none")
        funding_alert_level = funding_data.get("alert_level", "none")
        netflow_alert       = netflow_data.get("alert", "")

    # ── Step 5: Fetch liquidation map (CoinGlass)
    liq_map = _fetch_coinglass_liquidation_map()
    liq     = _estimate_liquidation_exposure(liq_map, spot_price, oi_usd)

    forced_flow_usd = liq["net_downside"]

    # ── Step 6: Apply depth haircut
    stressed = oi_alert_level in ("notable", "extreme") or funding_alert_level in ("notable", "extreme")
    haircut  = DEPTH_HAIRCUT_STRESSED if stressed else DEPTH_HAIRCUT_NORMAL

    visible_depth_usd  = bid_20
    adjusted_depth_usd = visible_depth_usd * haircut

    # ── Step 7: Coverage ratios
    depth_coverage_ratio = (visible_depth_usd / forced_flow_usd) if forced_flow_usd > 0 else 99.0
    adjusted_coverage    = (adjusted_depth_usd / forced_flow_usd) if forced_flow_usd > 0 else 99.0

    # ── Step 8: Slippage estimate
    slippage_est = _estimate_slippage(forced_flow_usd, agg["bid_depth"])

    # ── Step 9: Venue concentration
    concentration = _venue_concentration(agg["venue_totals"])

    # ── Step 10: Depth vs median
    _record_depth_snapshot(bid_20)
    depth_vs_median = _depth_vs_median(bid_20)

    # ── Step 11: Cascade risk label
    cascade_label, cascade_level = _cascade_risk_label(
        adjusted_coverage,
        oi_alert_level,
        funding_alert_level,
        netflow_alert,
    )

    # ── Step 12: Top-level alert (matches existing metric card schema)
    if adjusted_coverage < 0.75 or cascade_level == "extreme":
        alert       = "Extreme cascade risk"
        alert_level = "extreme"
    elif adjusted_coverage < 1.0 or cascade_level == "notable":
        alert       = "Elevated cascade risk"
        alert_level = "notable"
    elif depth_vs_median is not None and depth_vs_median < 60:
        alert       = "Depth thinning vs median"
        alert_level = "notable"
    else:
        alert       = "—"
        alert_level = "none"

    return {
        # ── Standard metric card schema ───────────────────────────────────
        "name":          "Spot Depth",
        "category":      "Liquidity",
        "current":       _fmt_ratio(adjusted_coverage),
        "current_dir":   "up" if adjusted_coverage >= 1.0 else "down",
        "d7":            "—",    # depth history builds over time
        "vs30d":         "—",
        "percentile":    None,   # no historical percentile yet — grows as _depth_history fills
        "alert":         alert,
        "alert_level":   alert_level,
        "pattern":       cascade_label,

        # ── Depth-specific fields ─────────────────────────────────────────
        "spot_price_usd":            round(spot_price, 2),
        "bid_depth_0_5pct_usd":      _fmt_usd(bid_05),
        "bid_depth_1_0pct_usd":      _fmt_usd(bid_10),
        "bid_depth_2_0pct_usd":      _fmt_usd(bid_20),
        "ask_depth_2_0pct_usd":      _fmt_usd(ask_20),
        "visible_depth_usd":         _fmt_usd(visible_depth_usd),
        "adjusted_depth_usd":        _fmt_usd(adjusted_depth_usd),
        "depth_haircut_pct":         f"{int(haircut*100)}%",
        "haircut_reason":            "stressed" if stressed else "normal",

        "depth_coverage_ratio":      round(depth_coverage_ratio, 2),
        "adjusted_coverage":         round(adjusted_coverage, 2),

        "liquidation_estimate_usd":  _fmt_usd(forced_flow_usd),
        "liquidation_source":        liq["source"],
        "oi_usd":                    _fmt_usd(oi_usd) if oi_usd > 0 else "—",

        "slippage_estimate":         slippage_est,
        "depth_vs_median_pct":       depth_vs_median,
        "venue_concentration_pct":   round(concentration * 100, 1),
        "venues_online":             venues_up,

        "cascade_risk_label":        cascade_label,
        "cascade_risk_level":        cascade_level,

        "oi_alert_level":            oi_alert_level,
        "funding_alert_level":       funding_alert_level,

        "venue_breakdown": {
            venue: {
                "bid_2pct_usd": _fmt_usd(totals["bid_2pct_usd"]),
                "ask_2pct_usd": _fmt_usd(totals["ask_2pct_usd"]),
                "share_pct":    round(totals["bid_2pct_usd"] / visible_depth_usd * 100, 1)
                                if visible_depth_usd > 0 else 0,
            }
            for venue, totals in agg["venue_totals"].items()
        },

        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


# ── Cached builder ────────────────────────────────────────────────────────────

def _build_assessment_cached(metrics_cache: dict = None) -> dict:
    now = time.time()
    if _assess_cache["data"] and (now - _assess_cache["ts"]) < ASSESS_CACHE_TTL:
        return _assess_cache["data"]
    data = _build_depth_assessment(metrics_cache)
    _assess_cache["data"] = data
    _assess_cache["ts"]   = now
    return data


# ── Routes ────────────────────────────────────────────────────────────────────

@liquidity_router.get("/depth")
def get_liquidity_depth():
    """
    Full cascade risk assessment — aggregated spot depth vs estimated liquidation exposure.
    Pulls OI/funding context from shared metrics cache.
    Cache TTL: 60s.
    """
    # Import here to avoid circular import — main.py defines _build_metrics_cached
    try:
        from main import _build_metrics_cached
        metrics = _build_metrics_cached()
    except ImportError:
        metrics = None

    return _build_assessment_cached(metrics)


@liquidity_router.get("/orderbook")
def get_orderbook():
    """
    Raw aggregated order book depth — 0.5%, 1%, 2% bands.
    Cache TTL: 30s (faster refresh than full assessment).
    """
    now = time.time()
    if _depth_cache["data"] and (now - _depth_cache["ts"]) < DEPTH_CACHE_TTL:
        return _depth_cache["data"]

    binance_book  = _fetch_binance_depth()
    coinbase_book = _fetch_coinbase_depth()
    kraken_book   = _fetch_kraken_depth()
    books         = [b for b in [binance_book, coinbase_book, kraken_book] if b]

    if not books:
        return {"error": "All venues unavailable"}

    spot = 0.0
    if binance_book and binance_book["bids"]:
        spot = binance_book["bids"][0][0]

    agg = _aggregate_depth(books, spot)

    result = {
        "spot_price_usd": round(spot, 2),
        "venues":         [b["venue"] for b in books],
        "bid_depth": {
            "0.5pct":  _fmt_usd(agg["bid_depth"]["0.5"]["usd"]),
            "1.0pct":  _fmt_usd(agg["bid_depth"]["1.0"]["usd"]),
            "2.0pct":  _fmt_usd(agg["bid_depth"]["2.0"]["usd"]),
            "raw": {
                "0.5pct_usd": agg["bid_depth"]["0.5"]["usd"],
                "1.0pct_usd": agg["bid_depth"]["1.0"]["usd"],
                "2.0pct_usd": agg["bid_depth"]["2.0"]["usd"],
            },
        },
        "ask_depth": {
            "0.5pct":  _fmt_usd(agg["ask_depth"]["0.5"]["usd"]),
            "1.0pct":  _fmt_usd(agg["ask_depth"]["1.0"]["usd"]),
            "2.0pct":  _fmt_usd(agg["ask_depth"]["2.0"]["usd"]),
        },
        "venue_breakdown": agg["venue_totals"],
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }

    _depth_cache["data"] = result
    _depth_cache["ts"]   = now
    return result


@liquidity_router.get("/cascade-score")
def get_cascade_score():
    """
    Lightweight poll endpoint — just the composite label + level.
    Frontend can use this for a top-bar indicator without fetching full /depth payload.
    """
    assessment = _build_assessment_cached()
    return {
        "cascade_risk_label":  assessment.get("cascade_risk_label", "—"),
        "cascade_risk_level":  assessment.get("cascade_risk_level", "none"),
        "adjusted_coverage":   assessment.get("adjusted_coverage"),
        "alert":               assessment.get("alert", "—"),
        "alert_level":         assessment.get("alert_level", "none"),
        "slippage_estimate":   assessment.get("slippage_estimate", "—"),
        "depth_vs_median_pct": assessment.get("depth_vs_median_pct"),
        "venues_online":       assessment.get("venues_online", []),
        "updated_at":          assessment.get("updated_at"),
    }


@liquidity_router.get("/cache/flush")
def flush_liquidity_cache():
    """Force refresh on next request."""
    global _depth_cache, _assess_cache
    _depth_cache  = {"data": None, "ts": 0.0}
    _assess_cache = {"data": None, "ts": 0.0}
    return {"flushed": True, "caches": ["depth", "assessment"]}
