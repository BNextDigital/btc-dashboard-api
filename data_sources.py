"""
data_sources.py — fetches raw data from external APIs.

Each function returns the raw numeric inputs that formatters.py expects.
This is the ONLY file that knows about external APIs.
If an API call fails, functions return None so the endpoint
can surface a clear error rather than crashing silently.
"""

from __future__ import annotations
import os
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, date, timedelta, timezone
from dotenv import load_dotenv
import pytz

load_dotenv()

CRYPTOQUANT_KEY  = os.getenv("CRYPTOQUANT_API_KEY")
COINGLASS_KEY    = os.getenv("COINGLASS_API_KEY")
COINGECKO_KEY    = os.getenv("COINGECKO_API_KEY")
FARSIDE_URL = "https://farside.co.uk/bitcoin-etf-flow-all-data/"
_farside_cache: dict = {"data": None, "fetch_date": None}
# fetch_date = the ET calendar date on which we last fetched after 2am

CRYPTOQUANT_BASE = "https://api.cryptoquant.com/v1/btc"
COINGLASS_BASE   = "https://open-api.coinglass.com/public/v2"
COINGECKO_BASE   = "https://api.coingecko.com/api/v3"

# ─── Simple in-memory cache ────────────────────────────────────────────────

_cache: dict = {}
CACHE_TTL = 90  # seconds

# Legacy ETF-flow fallback cache. Inputs are daily ETF data, so there is no
# benefit to re-running the Yahoo work on every metrics/health request.
_etf_flow_cache: dict = {"data": None, "ts": 0.0}
ETF_FLOW_CACHE_TTL = 3600  # 1 hour

def _cached_get(url: str, headers: dict, params: dict = None):
    cache_key = url + str(sorted((params or {}).items()))
    now = time.time()
    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if now - ts < CACHE_TTL:
            return data
    data = _safe_get(url, headers, params)
    if data is not None:
        _cache[cache_key] = (now, data)
    return data


# ─── Shared fetch state ────────────────────────────────────────────────────

_shared: dict = {
    "chart":       None,
    "ohlcv":       None,
    "derivatives": None,
    "fetched_at":  0,
}
SHARED_TTL = 90


def get_shared_coingecko() -> dict:
    global _shared
    now = time.time()
    if now - _shared["fetched_at"] < SHARED_TTL:
        return _shared
    chart, ohlcv = _fetch_coingecko_all()
    derivatives  = _fetch_coingecko_derivatives()
    _shared = {
        "chart":       chart,
        "ohlcv":       ohlcv,
        "derivatives": derivatives,
        "fetched_at":  now,
    }
    return _shared


# ─── Sparkline helper ──────────────────────────────────────────────────────

def _normalize_sparkline(values: list, points: int = 12) -> list:
    """
    Downsamples to `points` entries and normalizes to 0-100 scale.
    Returns empty list if not enough data.
    """
    if not values or len(values) < 2:
        return []
    if len(values) > points:
        indices = [int(i * (len(values) - 1) / (points - 1)) for i in range(points)]
        values  = [values[i] for i in indices]
    min_v = min(values)
    max_v = max(values)
    rng   = max_v - min_v
    if rng == 0:
        return [50] * len(values)
    return [round((v - min_v) / rng * 100) for v in values]


# ─── Shared helpers ────────────────────────────────────────────────────────

def _cq_headers() -> dict:
    return {"Authorization": f"Bearer {CRYPTOQUANT_KEY}"}

def _cg_headers() -> dict:
    return {"coinglassSecret": COINGLASS_KEY}

def _coingecko_headers() -> dict:
    return {"x-cg-demo-api-key": COINGECKO_KEY}

def _days_ago(n: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=n)
    return dt.strftime("%Y-%m-%d")

def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _safe_get(url: str, headers: dict, params: dict = None):
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code == 429:
            print(f"[data_sources] Rate limited on {url} — waiting 30s")
            time.sleep(30)
            r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[data_sources] GET {url} failed: {e}")
        return None


# ─── CryptoQuant: Exchange Netflow ────────────────────────────────────────

def fetch_exchange_netflow() -> dict | None:
    url = f"{CRYPTOQUANT_BASE}/exchange-flows/netflow"
    params = {
        "exchange": "all_exchange",
        "window":   "day",
        "from":     _days_ago(35),
        "to":       _today(),
        "limit":    35,
    }
    data = _safe_get(url, _cq_headers(), params)
    if not data:
        return None
    try:
        rows       = data["data"]["values"]
        values     = [r["value"] for r in rows]
        current_btc  = values[-1]
        sum_7d_btc   = sum(values[-7:])
        avg_30d_btc  = sum(abs(v) for v in values[-30:]) / 30
        abs_values   = sorted(abs(v) for v in values[-30:])
        abs_current  = abs(current_btc)
        rank         = sum(1 for v in abs_values if v <= abs_current)
        percentile   = (rank / len(abs_values)) * 100
        return {
            "current_btc":    current_btc,
            "sum_7d_btc":     sum_7d_btc,
            "avg_30d_btc":    avg_30d_btc,
            "sum_30d_btc":    sum(values[-30:]),
            "percentile_90d": percentile,
        }
    except (KeyError, IndexError, TypeError) as e:
        print(f"[data_sources] netflow parse error: {e}")
        return None


# ─── CoinGlass: LTH Supply ────────────────────────────────────────────────

def fetch_lth_supply() -> dict | None:
    url  = f"{COINGLASS_BASE}/indicator/lth-supply"
    data = _safe_get(url, _cg_headers())
    if not data:
        return None
    try:
        rows   = data["data"]
        values = [r["value"] for r in rows if r.get("value") is not None]
        change_7d_btc  = values[-1] - values[-8]  if len(values) >= 8  else 0
        change_30d_btc = values[-1] - values[-31] if len(values) >= 31 else 0
        change_30d_pct = change_30d_btc / values[-31] if len(values) >= 31 and values[-31] else 0
        sorted_changes = sorted(abs(values[i] - values[i-7]) for i in range(7, len(values)))
        rank           = sum(1 for c in sorted_changes if c <= abs(change_7d_btc))
        percentile     = (rank / len(sorted_changes)) * 100 if sorted_changes else 50
        return {
            "change_7d_btc":  change_7d_btc,
            "change_30d_btc": change_30d_btc,
            "change_30d_pct": change_30d_pct,
            "percentile_90d": percentile,
        }
    except (KeyError, IndexError, TypeError) as e:
        print(f"[data_sources] lth_supply parse error: {e}")
        return None


# ─── yfinance: ETF Flow (legacy fallback) ─────────────────────────────────

def fetch_etf_flow() -> dict | None:
    """
    Legacy ETF-flow proxy used when no manual ETF-flow history is available.

    Uses one bulk yFinance OHLCV download for all ETFs instead of eight
    individual history() calls. Successful results are cached for one hour
    because the underlying inputs are daily market data.
    """
    now = time.time()

    if (
        _etf_flow_cache["data"] is not None
        and (now - _etf_flow_cache["ts"]) < ETF_FLOW_CACHE_TTL
    ):
        return _etf_flow_cache["data"]

    try:
        import yfinance as yf

        tickers = [
            "IBIT", "FBTC", "ARKB", "BITB",
            "HODL", "BTCO", "EZBC", "BRRR",
        ]

        total_assets_now = 0
        total_vol_today = 0
        total_vol_5d_avg = 0
        assets_by_day = {}

        # One shared 10-day OHLCV download instead of 8 separate history() calls.
        raw = yf.download(
            tickers,
            period="10d",
            auto_adjust=True,
            progress=False,
            threads=4,
        )

        if raw is None or raw.empty:
            return None

        close = raw["Close"] if "Close" in raw.columns else None
        volume = raw["Volume"] if "Volume" in raw.columns else None

        if close is None or volume is None:
            return None

        for ticker in tickers:
            try:
                # Keep the existing totalAssets source unchanged for now.
                etf = yf.Ticker(ticker)
                info = etf.info

                assets = info.get("totalAssets", 0) or 0
                total_assets_now += assets

                if ticker not in close.columns or ticker not in volume.columns:
                    continue

                close_series = close[ticker].dropna()
                volume_series = volume[ticker].dropna()

                common_index = close_series.index.intersection(volume_series.index)
                if common_index.empty:
                    continue

                close_series = close_series.loc[common_index]
                volume_series = volume_series.loc[common_index]

                vol_today = int(volume_series.iloc[-1])
                vol_5d = int(volume_series.iloc[-5:].mean())

                total_vol_today += vol_today
                total_vol_5d_avg += vol_5d

                for dt in common_index:
                    d = str(dt.date())
                    if d not in assets_by_day:
                        assets_by_day[d] = 0
                    assets_by_day[d] += (
                        float(close_series.loc[dt])
                        * float(volume_series.loc[dt])
                    )

            except Exception as e:
                print(f"[data_sources] yfinance {ticker} error: {e}")
                continue

        if not total_assets_now:
            return None

        sorted_days = sorted(assets_by_day.keys())
        if len(sorted_days) >= 2:
            # Preserve the legacy proxy math exactly.
            recent = sum(assets_by_day[d] for d in sorted_days[-3:]) / 3
            previous = sum(assets_by_day[d] for d in sorted_days[-6:-3]) / 3
            flow_direction = recent - previous
        else:
            flow_direction = 0

        vol_ratio = (
            total_vol_today / total_vol_5d_avg
            if total_vol_5d_avg
            else 1
        )

        min_aum, max_aum = 30_000_000_000, 120_000_000_000
        percentile = max(
            0,
            min(
                100,
                (total_assets_now - min_aum)
                / (max_aum - min_aum)
                * 100,
            ),
        )

        if vol_ratio > 2.0 and flow_direction > 0:
            alert, alert_level = "Strong acceleration", "extreme"
        elif vol_ratio > 1.5 and flow_direction > 0:
            alert, alert_level = "Flow acceleration", "notable"
        elif vol_ratio > 1.5 and flow_direction < 0:
            alert, alert_level = "Outflow surge", "notable"
        elif flow_direction < 0:
            alert, alert_level = "Outflow", "neutral"
        else:
            alert, alert_level = "—", "none"

        flow_usd = flow_direction * 0.001  # directional proxy only — not displayed
        current_str = None  # formatter uses AUM instead
        spark_vals = [assets_by_day[d] for d in sorted_days]
        etf_spark = _normalize_sparkline(spark_vals)

        result = {
            "current_daily":         flow_usd,
            "last_7d_sum":           flow_usd * 7,
            "avg_30d":               flow_usd * 4.5,
            "percentile_90d":        percentile,
            "_alert_override":       alert,
            "_alert_level_override": alert_level,
            "_current_str":          current_str,
            "_aum_total":            total_assets_now,
            "_spark":                etf_spark,
        }

        _etf_flow_cache["data"] = result
        _etf_flow_cache["ts"] = now

        return result

    except Exception as e:
        print(f"[data_sources] etf_flow error: {e}")
        return None


# ─── Farside: ETF Flow ────────────────────────────────────────────────────

def _parse_farside_value(s: str) -> float | None:
    """Convert '(231.0)' → -231.0, '132.3' → 132.3, '-' → None"""
    s = s.strip()
    if not s or s == "-":
        return None
    negative = s.startswith("(") and s.endswith(")")
    s = s.replace("(", "").replace(")", "").replace(",", "")
    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None


def _farside_cache_valid() -> bool:
    """
    Cache is valid if we have data and we already fetched during today's
    2am ET window. Before 2am ET, 'today' is still yesterday's fetch.
    """
    if not _farside_cache["data"] or not _farside_cache["fetch_date"]:
        return False
    et = datetime.now(pytz.timezone("America/New_York"))
    # Before 2am, the current valid fetch_date is yesterday
    expected_date = et.date() if et.hour >= 2 else (et - timedelta(days=1)).date()
    return _farside_cache["fetch_date"] == expected_date


def fetch_etf_flow_farside() -> dict | None:
    global _farside_cache

    # Return cache if still valid
    if _farside_cache_valid():
        return _farside_cache["data"]

    et = datetime.now(pytz.timezone("America/New_York"))

    # Before 2am ET: don't fetch — return whatever stale data we have
    if et.hour < 2:
        return _farside_cache["data"]

    # Past 2am ET and cache is stale — fetch now
    try:
        resp = requests.get(
            FARSIDE_URL,
            timeout=15,
            headers={"User-Agent": "btc-dashboard/1.0 (internal research tool)"}
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            return None

        rows = table.find_all("tr")
        flow_by_date: dict[date, float] = {}

        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 13:
                continue
            date_str  = cells[0]    # "17 Jul 2026"
            total_str = cells[-1]   # "132.3" or "(231.0)"
            total = _parse_farside_value(total_str)
            if total is None:
                continue
            try:
                dt = datetime.strptime(date_str, "%d %b %Y").date()
                flow_by_date[dt] = total
            except ValueError:
                continue

        if not flow_by_date:
            return None

        sorted_dates = sorted(flow_by_date.keys())

        # Exclude ETF launch anomaly period (Jan–Mar 2024)
        cutoff = date(2024, 4, 1)

        today_flow = flow_by_date[sorted_dates[-1]]
        last_7d    = [flow_by_date[d] for d in sorted_dates[-7:]]
        last_30d   = [flow_by_date[d] for d in sorted_dates[-30:] if d >= cutoff]
        last_90d   = [flow_by_date[d] for d in sorted_dates[-90:] if d >= cutoff]

        sum_7d  = sum(last_7d)
        avg_30d = sum(last_30d) / len(last_30d) if last_30d else 0

        # 90d percentile
        sorted_90d = sorted(last_90d)
        rank       = sum(1 for v in sorted_90d if v <= today_flow)
        percentile = (rank / len(sorted_90d) * 100) if sorted_90d else 50

        result = {
            "current_daily":  today_flow,
            "last_date":      sorted_dates[-1].isoformat(),
            "last_7d_sum":    sum_7d,
            "avg_30d":        avg_30d,
            "percentile_90d": percentile,
            "source":         "Farside Investors",
            "_spark_raw":     [flow_by_date[d] for d in sorted_dates[-30:]],
            "_all_history":   {d.isoformat(): flow_by_date[d] for d in sorted_dates},
        }

        _farside_cache["data"]       = result
        _farside_cache["fetch_date"] = et.date()
        print(f"[farside] Fetched — {len(flow_by_date)} trading days, latest: {sorted_dates[-1]}")
        return result

    except Exception as e:
        print(f"[farside] fetch error: {e}")
        return _farside_cache["data"]  # return stale on error rather than None


# ─── CoinGecko: shared fetch ───────────────────────────────────────────────

def _fetch_coingecko_all() -> tuple:
    chart = _cached_get(
        f"{COINGECKO_BASE}/coins/bitcoin/market_chart",
        _coingecko_headers(),
        {"vs_currency": "usd", "days": "30", "interval": "daily"},
    )
    ohlcv = _cached_get(
        f"{COINGECKO_BASE}/coins/bitcoin/ohlc",
        _coingecko_headers(),
        {"vs_currency": "usd", "days": "30"},
    )
    return chart, ohlcv


# ─── CoinGecko: Price + Volume ─────────────────────────────────────────────

def fetch_price_and_volume(
    chart: dict | None = None,
    ohlcv: list | None = None,
) -> tuple:
    if chart is None or ohlcv is None:
        chart, ohlcv = _fetch_coingecko_all()
    if not chart or not ohlcv:
        return None, None

    try:
        # ── Price ─────────────────────────────────────────────────
        closes = [candle[4] for candle in ohlcv]

        daily_change  = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] else 0
        week_change   = (closes[-1] - closes[-8]) / closes[-8] if len(closes) >= 8 and closes[-8] else 0

        daily_moves   = [
            abs((closes[i] - closes[i-1]) / closes[i-1])
            for i in range(1, len(closes)) if closes[i-1]
        ]
        avg_daily_30d = sum(daily_moves) / len(daily_moves) if daily_moves else 0
        sorted_moves  = sorted(daily_moves)
        rank_price    = sum(1 for m in sorted_moves if m <= abs(daily_change))
        pctl_price    = (rank_price / len(sorted_moves)) * 100 if sorted_moves else 50

        price_spark = _normalize_sparkline([p[1] for p in chart["prices"]])

        price_inputs = {
            "daily_change_pct": daily_change,
            "week_change_pct":  week_change,
            "avg_daily_30d":    avg_daily_30d,
            "percentile_90d":   pctl_price,
            "_spark":           price_spark,
        }

        # ── Volume ────────────────────────────────────────────────
        volumes     = [v[1] for v in chart["total_volumes"]]
        avg_30d_vol = sum(volumes) / len(volumes) if volumes else 1
        ratio_30d   = volumes[-1] / avg_30d_vol if avg_30d_vol else 1
        avg_7d_vol  = sum(volumes[-7:]) / 7 if len(volumes) >= 7 else volumes[-1]
        ratio_7d    = avg_7d_vol / avg_30d_vol if avg_30d_vol else 1
        sorted_vols = sorted(volumes)
        rank_vol    = sum(1 for v in sorted_vols if v <= volumes[-1])
        pctl_vol    = (rank_vol / len(sorted_vols)) * 100 if sorted_vols else 50

        vol_spark = _normalize_sparkline(volumes)

        volume_inputs = {
            "ratio_30d":        ratio_30d,
            "ratio_7d":         ratio_7d,
            "percentile_90d":   pctl_vol,
            "price_change_pct": daily_change,
            "_spark":           vol_spark,
        }

        return price_inputs, volume_inputs

    except (IndexError, TypeError, ZeroDivisionError) as e:
        print(f"[data_sources] price/volume parse error: {e}")
        return None, None


# ─── CoinGecko: Realized Cap proxy ─────────────────────────────────────────

def fetch_realized_cap(chart: dict | None = None) -> dict | None:
    if chart is None:
        chart, _ = _fetch_coingecko_all()
    if not chart:
        return None

    try:
        mcaps = [m[1] for m in chart["market_caps"]]

        def growth(a, b):
            return (b - a) / a if a else 0

        growth_today  = growth(mcaps[-2], mcaps[-1])
        growth_7d     = growth(mcaps[-8], mcaps[-1]) if len(mcaps) >= 8 else 0
        daily_growths = [growth(mcaps[i-1], mcaps[i]) for i in range(1, len(mcaps)) if mcaps[i-1]]
        avg_30d_pct   = sum(daily_growths) / len(daily_growths) if daily_growths else 0
        sorted_g      = sorted(daily_growths)
        rank          = sum(1 for g in sorted_g if g <= growth_today)
        percentile    = (rank / len(sorted_g)) * 100 if sorted_g else 50

        mcap_spark = _normalize_sparkline(mcaps)

        return {
            "growth_pct":     growth_today,
            "growth_7d_pct":  growth_7d,
            "avg_30d_pct":    avg_30d_pct,
            "percentile_90d": percentile,
            "_spark":         mcap_spark,
        }

    except (IndexError, TypeError, ZeroDivisionError) as e:
        print(f"[data_sources] realized_cap parse error: {e}")
        return None


# ─── CoinGecko: Derivatives (OI + Funding) ────────────────────────────────

def _fetch_coingecko_derivatives() -> list | None:
    data = _cached_get(
        f"{COINGECKO_BASE}/derivatives",
        _coingecko_headers(),
    )
    if not data:
        return None
    try:
        btc_perps = [
            m for m in data
            if m.get("index_id") == "BTC"
            and m.get("contract_type") == "perpetual"
            and m.get("open_interest") is not None
            and m.get("open_interest", 0) > 0
        ]
        by_exchange: dict = {}
        for m in btc_perps:
            exchange = m["market"]
            if exchange not in by_exchange:
                by_exchange[exchange] = m
            elif m["open_interest"] > by_exchange[exchange]["open_interest"]:
                by_exchange[exchange] = m
        return list(by_exchange.values())
    except (KeyError, TypeError) as e:
        print(f"[data_sources] derivatives filter error: {e}")
        return None


def fetch_open_interest(markets: list | None = None) -> dict | None:
    if markets is None:
        markets = _fetch_coingecko_derivatives()
    if not markets:
        return None

    try:
        total_oi = sum(m["open_interest"] for m in markets)
        print(f"[debug] OI markets count: {len(markets)}")

        try:
            from oi_history import get_snapshots, get_snapshot_count
            snapshots  = get_snapshots(days=35)
            snap_count = get_snapshot_count()
            has_history = snap_count >= 48
        except Exception:
            snapshots   = []
            snap_count  = 0
            has_history = False

        if has_history and len(snapshots) >= 48:
            oi_values  = [s["oi_usd"] for s in snapshots]
            sorted_oi  = sorted(oi_values)
            rank       = sum(1 for v in sorted_oi if v <= total_oi)
            percentile = (rank / len(sorted_oi)) * 100

            now_ts     = int(time.time())
            target_7d  = now_ts - (7  * 86400)
            target_30d = now_ts - (30 * 86400)

            def closest_value(target_ts):
                closest = min(snapshots, key=lambda s: abs(s["timestamp"] - target_ts))
                return closest["oi_usd"]

            oi_7d_ago      = closest_value(target_7d)  if len(snapshots) >= 48   else total_oi
            oi_30d_ago     = closest_value(target_30d) if len(snapshots) >= 2880 else total_oi
            growth_7d_pct  = (total_oi - oi_7d_ago)  / oi_7d_ago  if oi_7d_ago  else 0
            growth_30d_pct = (total_oi - oi_30d_ago) / oi_30d_ago if oi_30d_ago else 0

            oi_spark = _normalize_sparkline(oi_values[-30:] if len(oi_values) >= 30 else oi_values)
            print(f"[oi] Using REAL history — {snap_count} snapshots, pctl={percentile:.0f}")
        else:
            min_oi, max_oi = 15_000_000_000, 70_000_000_000
            percentile     = max(0, min(100, (total_oi - min_oi) / (max_oi - min_oi) * 100))
            growth_7d_pct  = 0.0
            growth_30d_pct = 0.0
            oi_spark       = []
            print(f"[oi] Using RANGE estimate — only {snap_count} snapshots so far")

        return {
            "current_usd":    total_oi,
            "growth_7d_pct":  growth_7d_pct,
            "growth_30d_pct": growth_30d_pct,
            "percentile_90d": percentile,
            "_spark":         oi_spark,
        }

    except (KeyError, TypeError) as e:
        print(f"[data_sources] open_interest parse error: {e}")
        return None


def fetch_funding(markets: list | None = None) -> dict | None:
    if markets is None:
        markets = _fetch_coingecko_derivatives()
    if not markets:
        return None
    try:
        REFERENCE_EXCHANGES = {
            "Binance (Futures)",
            "Bybit (Futures)",
            "OKX (Futures)",
        }

        valid = [
            m for m in markets
            if m.get("market") in REFERENCE_EXCHANGES
            and m.get("index_id") == "BTC"
            and m.get("contract_type") == "perpetual"
            and m.get("funding_rate") is not None
            and m.get("open_interest", 0) > 0
            and m.get("funding_rate") != 0.01
            and m.get("funding_rate") != -0.01
        ]

        if not valid:
            return None

        total_oi     = sum(m["open_interest"] for m in valid)
        weighted_sum = sum(m["funding_rate"] * m["open_interest"] for m in valid)
        current_rate = (weighted_sum / total_oi) / 100 if total_oi else 0

        exchange_rates = {
            m["market"].replace(" (Futures)", "").replace(" Futures", ""):
            round(m["funding_rate"] / 100 * 100, 4)  # as % per 8h
            for m in valid
        }

        rates      = [m["funding_rate"] / 100 for m in valid]
        spread     = max(rates) - min(rates)
        high_ex    = max(valid, key=lambda m: m["funding_rate"])
        low_ex     = min(valid, key=lambda m: m["funding_rate"])

        min_r, max_r = -0.0001, 0.0006
        percentile   = max(0, min(100, (current_rate - min_r) / (max_r - min_r) * 100))

        return {
            "current_rate":   current_rate,
            "avg_7d":         current_rate * 0.95,
            "avg_30d":        current_rate * 0.70,
            "percentile_90d": percentile,
            "spread":         spread,
            "exchange_rates": exchange_rates,
            "high_exchange":  high_ex["market"].replace(" (Futures)", ""),
            "low_exchange":   low_ex["market"].replace(" (Futures)", ""),
        }
    except (KeyError, TypeError, ZeroDivisionError) as e:
        print(f"[data_sources] funding parse error: {e}")
        return None


# ─── News aggregation (CoinGecko + CoinDesk RSS + Cointelegraph RSS) ───────

def _fetch_rss(url: str) -> list[dict]:
    """
    Fetches and parses an RSS feed. Returns list of dicts with
    title, url, description, published_ts, source fields.
    Returns empty list on any failure.
    """
    import urllib.request
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime

    try:
        req  = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=8)
        xml_data = resp.read().decode("utf-8")
        root     = ET.fromstring(xml_data)
        channel  = root.find("channel")
        if channel is None:
            return []

        items = []
        source_name = channel.findtext("title", "").split(":")[0].strip()

        for item in channel.findall("item"):
            title       = item.findtext("title", "").strip()
            link        = item.findtext("link", "").strip()
            description = (item.findtext("description") or "").strip()
            pub_date    = item.findtext("pubDate", "")

            try:
                ts = int(parsedate_to_datetime(pub_date).timestamp())
            except Exception:
                ts = int(time.time())

            items.append({
                "title":        title,
                "url":          link,
                "description":  description[:200],
                "published_ts": ts,
                "source":       source_name,
            })

        return items

    except Exception as e:
        print(f"[data_sources] RSS {url} failed: {e}")
        return []


def fetch_btc_news() -> list | None:
    """
    Aggregates BTC-relevant news from three sources:
    1. CoinGecko API (primary)
    2. CoinDesk RSS
    3. Cointelegraph RSS

    Deduplicates by title similarity, filters by BTC relevance,
    sorts by recency, returns top 5.
    """
    BTC_KEYWORDS = [
        "bitcoin", "btc", "etf", "blackrock", "fidelity", "grayscale",
        "federal reserve", "fed ", "interest rate", "inflation", "cpi",
        "strategy", "microstrategy", "coinbase", "binance", "sec",
        "crypto", "institutional", "halving", "whale", "on-chain",
        "funding", "liquidat", "open interest", "macro", "defi",
        "stablecoin", "regulation", "treasury",
    ]

    CATEGORY_TAGS = {
        "etf": "ETF Flow", "blackrock": "Institutional", "fidelity": "Institutional",
        "grayscale": "Institutional", "strategy": "Corporate Flow",
        "microstrategy": "Corporate Flow", "federal reserve": "Macro",
        "fed ": "Macro", "interest rate": "Macro", "inflation": "Macro",
        "cpi": "Macro", "sec": "Regulatory", "regulation": "Regulatory",
        "coinbase": "Exchange", "binance": "Exchange",
        "liquidat": "Derivatives", "funding": "Derivatives",
        "open interest": "Derivatives", "whale": "On-chain",
        "on-chain": "On-chain", "halving": "On-chain",
        "defi": "DeFi", "stablecoin": "Stablecoin",
        "treasury": "Corporate Flow",
    }

    def get_tag(title: str, description: str) -> str:
        text = (title + " " + description).lower()
        for keyword, tag in CATEGORY_TAGS.items():
            if keyword in text:
                return tag
        return "Crypto"

    def is_relevant(title: str, description: str) -> bool:
        text = (title + " " + description).lower()
        return any(kw in text for kw in BTC_KEYWORDS)

    def format_time(ts: int) -> str:
        age = int(time.time()) - ts
        if age < 3600:
            return f"{age // 60}m ago"
        elif age < 86400:
            return f"{age // 3600}h ago"
        else:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%b %d")

    # ── Source 1: CoinGecko ───────────────────────────────────────
    all_items = []

    cg_data = _cached_get(
        f"{COINGECKO_BASE}/news",
        _coingecko_headers(),
        {"page": 1},
    )
    if cg_data and "data" in cg_data:
        for item in cg_data["data"]:
            all_items.append({
                "title":        item.get("title", ""),
                "url":          item.get("url", ""),
                "description":  item.get("description", "")[:200],
                "published_ts": item.get("crawled_at", int(time.time())),
                "source":       item.get("news_site", "CoinGecko"),
            })

    # ── Source 2: CoinDesk RSS ────────────────────────────────────
    coindesk_items = _fetch_rss("https://www.coindesk.com/arc/outboundfeeds/rss/")
    all_items.extend(coindesk_items)

    # ── Source 3: Cointelegraph RSS ───────────────────────────────
    ct_items = _fetch_rss("https://cointelegraph.com/rss")
    all_items.extend(ct_items)

    if not all_items:
        return None

    # ── Filter to relevant items ──────────────────────────────────
    relevant = [
        item for item in all_items
        if is_relevant(item["title"], item["description"])
    ]

    # ── Deduplicate by title similarity ───────────────────────────
    seen_words: list[set] = []
    deduped = []
    for item in relevant:
        title_words = set(item["title"].lower().split())
        is_dupe = any(
            len(title_words & seen) >= 5
            for seen in seen_words
        )
        if not is_dupe:
            deduped.append(item)
            seen_words.append(title_words)

    # ── Sort by recency and take top 5 ───────────────────────────
    deduped.sort(key=lambda x: x["published_ts"], reverse=True)
    top = deduped[:5]

    if not top:
        return None

    return [
        {
            "title":  item["title"],
            "source": item["source"],
            "time":   format_time(item["published_ts"]),
            "tag":    get_tag(item["title"], item["description"]),
            "url":    item["url"],
        }
        for item in top
    ]
