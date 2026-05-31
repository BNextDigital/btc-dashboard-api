"""
sector_flows_routes.py v2 — Sector Capital Flow Matrix

New additions:
  1. HYG/LQD credit spread — leading risk appetite indicator
  2. OBV z-score (20d rolling) — added alongside normalized OBV
  3. Relative strength vs SPY (5d + 20d continuous, replaces binary signal)
  4. CFTC COT data — futures positioning for Gold, Bonds, Crude

Endpoints:
  GET /sector-flows/metrics
  GET /sector-flows/cot           — COT positioning only
  GET /sector-flows/credit        — HYG/LQD spread only
"""

import os, time, math, io, csv
from datetime import datetime, date
from fastapi import APIRouter
import yfinance as yf
import pandas as pd
import requests

sector_flows_router = APIRouter(prefix="/sector-flows")

DATA_DIR      = os.getenv("DATA_DIR", "./data")
FLOW_CACHE_TTL = 300   # 5 min
COT_CACHE_TTL  = 3600  # 1 hour (COT is weekly)

# ── Sector definitions ────────────────────────────────────────────────────
SECTORS = {
    "equities":   ("^GSPC",   "S&P 500"),
    "tech":       ("XLK",     "Technology"),
    "financials": ("XLF",     "Financials"),
    "energy":     ("XLE",     "Energy"),
    "realestate": ("XLRE",    "Real Estate"),
    "metals":     ("GC=F",    "Gold (Metals)"),
    "crypto":     ("BTC-USD", "Bitcoin (Crypto)"),
    "bonds":      ("TLT",     "US Bonds (20y)"),
}

# Additional tickers needed for new features
CREDIT_TICKERS  = ["HYG", "LQD"]          # credit spread
RS_BENCHMARK    = "SPY"                    # relative strength benchmark

# CFTC COT report codes (futures & options combined)
# Source: https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm
COT_CODES = {
    "gold":   "088691",   # Gold futures (COMEX)
    "bonds":  "020601",   # 30-Year T-Bond (CBOT)
    "crude":  "067651",   # WTI Crude Oil (NYMEX)
}
COT_URL = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"

# ── Caches ────────────────────────────────────────────────────────────────
_flow_cache   = {"data": None, "ts": 0.0}
_cot_cache    = {"data": None, "ts": 0.0}
_credit_cache = {"data": None, "ts": 0.0}

# ── Sanitize ──────────────────────────────────────────────────────────────
def _san(val):
    if val is None:
        return None
    try:
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    except (TypeError, ValueError):
        return None

# ── Fetch helpers ─────────────────────────────────────────────────────────

def _bulk_download(tickers: list, n_days: int = 300) -> dict[str, pd.DataFrame | None]:
    """Download OHLCV for a list of tickers. Returns {ticker: df}."""
    result = {t: None for t in tickers}
    if not tickers:
        return result
    try:
        raw = yf.download(
            tickers, period=f"{n_days}d",
            auto_adjust=True, progress=False,
            threads=True, group_by="ticker"
        )
        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    df = raw.dropna(how="all")
                elif ticker in raw.columns.get_level_values(0):
                    df = raw[ticker].dropna(how="all")
                else:
                    df = None
                result[ticker] = df if (df is not None and len(df) > 20) else None
            except Exception as e:
                print(f"Parse error {ticker}: {e}")
    except Exception as e:
        print(f"Bulk download error: {e}")
    return result


def _fetch_sector_data(n_days: int = 300) -> dict:
    """Fetch all sector OHLCV data."""
    all_tickers = [v[0] for v in SECTORS.values()] + CREDIT_TICKERS + [RS_BENCHMARK]
    raw = _bulk_download(all_tickers, n_days)

    sector_data = {}
    for key, (ticker, _) in SECTORS.items():
        sector_data[key] = raw.get(ticker)

    return {
        "sectors": sector_data,
        "hyg":     raw.get("HYG"),
        "lqd":     raw.get("LQD"),
        "spy":     raw.get("SPY"),
    }

# ── Metric calculators ────────────────────────────────────────────────────

def _calc_mfi(df, period: int = 14) -> float | None:
    """Money Flow Index (0–100)."""
    if df is None or len(df) < period + 1:
        return None
    try:
        tp = (df["High"] + df["Low"] + df["Close"]) / 3
        mf = tp * df["Volume"]
        pos_mf = neg_mf = 0.0
        for i in range(1, period + 1):
            if tp.iloc[-i] > tp.iloc[-i - 1]:
                pos_mf += mf.iloc[-i]
            else:
                neg_mf += mf.iloc[-i]
        if neg_mf == 0:
            return 100.0
        return round(100 - (100 / (1 + pos_mf / neg_mf)), 2)
    except Exception as e:
        print(f"MFI error: {e}")
        return None


def _calc_obv_series(df) -> list[float] | None:
    """Build full OBV series."""
    if df is None or len(df) < 2:
        return None
    try:
        obv, vals = 0.0, []
        for i in range(len(df)):
            if i > 0:
                if df["Close"].iloc[i] > df["Close"].iloc[i - 1]:
                    obv += float(df["Volume"].iloc[i])
                elif df["Close"].iloc[i] < df["Close"].iloc[i - 1]:
                    obv -= float(df["Volume"].iloc[i])
            vals.append(obv)
        return vals
    except Exception as e:
        print(f"OBV series error: {e}")
        return None


def _calc_obv_normalized(df, window: int = 20) -> float | None:
    """OBV normalized to -100/+100 over trailing window."""
    vals = _calc_obv_series(df)
    if not vals or len(vals) < window:
        return None
    try:
        recent = vals[-window:]
        lo, hi = min(recent), max(recent)
        if hi == lo:
            return 0.0
        return round(((vals[-1] - lo) / (hi - lo)) * 200 - 100, 1)
    except Exception:
        return None


def _calc_obv_zscore(df, window: int = 20) -> float | None:
    """
    OBV z-score: how many std devs is current OBV from its rolling mean.
    Comparable across assets regardless of absolute volume scale.
    """
    vals = _calc_obv_series(df)
    if not vals or len(vals) < window + 1:
        return None
    try:
        recent = vals[-(window + 1):-1]   # exclude last point
        mean = sum(recent) / len(recent)
        variance = sum((x - mean) ** 2 for x in recent) / len(recent)
        std = variance ** 0.5
        if std == 0:
            return 0.0
        return round((vals[-1] - mean) / std, 2)
    except Exception as e:
        print(f"OBV z-score error: {e}")
        return None


def _calc_volume_momentum(df) -> float | None:
    """5d avg volume / 20d avg volume."""
    if df is None or len(df) < 20:
        return None
    try:
        v5  = df["Volume"].tail(5).mean()
        v20 = df["Volume"].tail(20).mean()
        return round(v5 / v20, 2) if v20 > 0 else None
    except Exception:
        return None


def _calc_relative_strength(df, spy_df, window: int) -> float | None:
    """
    Continuous relative strength: sector return minus SPY return over window days.
    Positive = outperforming, Negative = underperforming.
    """
    if df is None or spy_df is None or len(df) < window + 1 or len(spy_df) < window + 1:
        return None
    try:
        s_ret  = (df["Close"].iloc[-1]     - df["Close"].iloc[-window - 1])     / df["Close"].iloc[-window - 1]     * 100
        b_ret  = (spy_df["Close"].iloc[-1] - spy_df["Close"].iloc[-window - 1]) / spy_df["Close"].iloc[-window - 1] * 100
        return round(s_ret - b_ret, 2)
    except Exception as e:
        print(f"RS error: {e}")
        return None


def _flow_signal(mfi, vol_mom, rs_5d) -> str:
    """
    Composite flow signal using MFI + volume momentum + relative strength.
    More nuanced than the binary v1 version.
    """
    if mfi is None or vol_mom is None:
        return "Insufficient data"

    score = 0
    if mfi > 65:      score += 2
    elif mfi > 55:    score += 1
    elif mfi < 35:    score -= 2
    elif mfi < 45:    score -= 1

    if vol_mom > 1.3:  score += 2
    elif vol_mom > 1.1: score += 1
    elif vol_mom < 0.7: score -= 2
    elif vol_mom < 0.9: score -= 1

    if rs_5d is not None:
        if rs_5d > 2:    score += 1
        elif rs_5d < -2: score -= 1

    if score >= 4:   return "Heavy Inflow"
    if score >= 2:   return "Strong Inflow"
    if score >= 1:   return "Mild Inflow"
    if score <= -4:  return "Heavy Outflow"
    if score <= -2:  return "Strong Outflow"
    if score <= -1:  return "Mild Outflow"
    return "Stable"

# ── Credit spread ─────────────────────────────────────────────────────────

def _build_credit_spread(hyg_df, lqd_df) -> dict:
    """
    HYG / LQD spread as a risk appetite proxy.
    Rising ratio = risk-on (HY outperforming IG).
    Falling ratio = risk-off (HY underperforming IG).
    """
    if hyg_df is None or lqd_df is None:
        return {"error": "No data"}
    try:
        # Align on common dates
        hyg_close = hyg_df["Close"].dropna()
        lqd_close = lqd_df["Close"].dropna()
        common = hyg_close.index.intersection(lqd_close.index)
        if len(common) < 21:
            return {"error": "Insufficient overlapping data"}

        hyg_c = hyg_close.loc[common]
        lqd_c = lqd_close.loc[common]
        ratio = (hyg_c / lqd_c)

        current    = _san(round(float(ratio.iloc[-1]), 4))
        d5_chg     = _san(round(float(ratio.iloc[-1] - ratio.iloc[-6]), 4))  if len(ratio) >= 6  else None
        d20_chg    = _san(round(float(ratio.iloc[-1] - ratio.iloc[-21]), 4)) if len(ratio) >= 21 else None

        vals       = ratio.tolist()
        pctile     = round(sum(1 for v in vals if v < current) / len(vals) * 100)

        # Trend label
        if d5_chg is not None:
            if d5_chg > 0.005:   trend = "Risk-On — HY outperforming"
            elif d5_chg < -0.005: trend = "Risk-Off — HY underperforming"
            else:                 trend = "Neutral"
        else:
            trend = "–"

        alert = "–"
        if pctile >= 80: alert = "Elevated risk appetite"
        elif pctile <= 20: alert = "Risk aversion"

        return {
            "hyg_price":  _san(round(float(hyg_c.iloc[-1]), 2)),
            "lqd_price":  _san(round(float(lqd_c.iloc[-1]), 2)),
            "ratio":      current,
            "d5_chg":     d5_chg,
            "d20_chg":    d20_chg,
            "percentile": pctile,
            "trend":      trend,
            "alert":      alert,
        }
    except Exception as e:
        print(f"Credit spread error: {e}")
        return {"error": str(e)}

# ── CFTC COT ──────────────────────────────────────────────────────────────

def _fetch_cot_data() -> dict:
    """
    Fetch CFTC Commitments of Traders (Financial Futures, weekly).
    Returns net speculator positioning for Gold, Bonds, Crude.
    COT URL format: https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip

    Fields used:
      - 'Lev_Money_Positions_Long_All'  — leveraged money (hedge funds) longs
      - 'Lev_Money_Positions_Short_All' — leveraged money shorts
    Net = Long - Short. Positive = net long (bullish), Negative = net short (bearish).
    """
    global _cot_cache
    now = time.time()
    if _cot_cache["data"] and (now - _cot_cache["ts"]) < COT_CACHE_TTL:
        return _cot_cache["data"]

    result = {}
    year = date.today().year

    for attempt_year in [year, year - 1]:
        url = f"https://www.cftc.gov/files/dea/history/fut_fin_txt_{attempt_year}.zip"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()

            import zipfile
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            fname = [n for n in zf.namelist() if n.endswith(".txt")][0]
            content = zf.read(fname).decode("latin-1")

            reader = csv.DictReader(io.StringIO(content))
            rows_by_code: dict[str, list] = {}

            for row in reader:
                code = row.get("CFTC_Contract_Market_Code", "").strip()
                for asset, asset_code in COT_CODES.items():
                    if code == asset_code:
                        rows_by_code.setdefault(asset, []).append(row)

            for asset, rows in rows_by_code.items():
                # Sort by date, take last 20 weeks
                rows.sort(key=lambda r: r.get("Report_Date_as_YYYY-MM-DD", ""))
                recent = rows[-20:]
                if not recent:
                    continue

                latest = recent[-1]
                prev   = recent[-2] if len(recent) >= 2 else None

                def get_net(r):
                    try:
                        longs  = int(r.get("Lev_Money_Positions_Long_All",  "0").replace(",",""))
                        shorts = int(r.get("Lev_Money_Positions_Short_All", "0").replace(",",""))
                        return longs - shorts
                    except:
                        return None

                net_now  = get_net(latest)
                net_prev = get_net(prev) if prev else None

                nets = [get_net(r) for r in recent if get_net(r) is not None]
                pctile = round(sum(1 for v in nets if v < net_now) / len(nets) * 100) if nets and net_now is not None else None

                wk_chg = (net_now - net_prev) if (net_now is not None and net_prev is not None) else None

                def _alert(net, pct):
                    if net is None: return "–"
                    if pct is not None and pct >= 80: return "Extreme net long"
                    if pct is not None and pct <= 20: return "Extreme net short"
                    if net > 0: return "Net long"
                    return "Net short"

                result[asset] = {
                    "net_position":   net_now,
                    "wk_chg":         wk_chg,
                    "percentile":     pctile,
                    "report_date":    latest.get("Report_Date_as_YYYY-MM-DD", "–"),
                    "alert":          _alert(net_now, pctile),
                }

            if result:
                break   # got data, don't try prior year

        except Exception as e:
            print(f"COT fetch error ({attempt_year}): {e}")
            continue

    if not result:
        result = {"error": "COT data unavailable"}

    _cot_cache["data"] = result
    _cot_cache["ts"] = now
    return result

# ── Main build ────────────────────────────────────────────────────────────

def _build_sector_flows() -> dict:
    fetched    = _fetch_sector_data(n_days=300)
    sector_dfs = fetched["sectors"]
    hyg_df     = fetched["hyg"]
    lqd_df     = fetched["lqd"]
    spy_df     = fetched["spy"]

    sectors = {}
    for sector_key, (ticker, name) in SECTORS.items():
        df = sector_dfs.get(sector_key)

        mfi      = _calc_mfi(df)
        obv_norm = _calc_obv_normalized(df)
        obv_z    = _calc_obv_zscore(df)
        vol_mom  = _calc_volume_momentum(df)
        rs_5d    = _calc_relative_strength(df, spy_df, window=5)
        rs_20d   = _calc_relative_strength(df, spy_df, window=20)

        current   = _san(round(float(df["Close"].iloc[-1]), 2))  if df is not None and len(df) > 0 else None
        change_5d = _san(round((float(df["Close"].iloc[-1]) - float(df["Close"].iloc[-6])) / float(df["Close"].iloc[-6]) * 100, 2)) if df is not None and len(df) >= 6 else None

        signal = _flow_signal(mfi, vol_mom, rs_5d)

        sectors[sector_key] = {
            "name":                name,
            "ticker":              ticker,
            "current":             current,
            "change_5d":           change_5d,
            "mfi":                 _san(mfi),
            "obv":                 _san(obv_norm),       # normalized -100/+100
            "obv_zscore":          _san(obv_z),          # z-score (new)
            "volume_momentum":     _san(vol_mom),
            "relative_strength_5d":  _san(rs_5d),        # continuous RS vs SPY (new)
            "relative_strength_20d": _san(rs_20d),       # (new)
            "flow_signal":         signal,
        }

    return {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "sectors":    sectors,
        "credit":     _build_credit_spread(hyg_df, lqd_df),
        "cot":        _fetch_cot_data(),
    }

# ── Routes ────────────────────────────────────────────────────────────────

@sector_flows_router.get("/metrics")
def get_sector_flows_metrics():
    global _flow_cache
    now = time.time()
    if _flow_cache["data"] and (now - _flow_cache["ts"]) < FLOW_CACHE_TTL:
        return _flow_cache["data"]
    data = _build_sector_flows()
    _flow_cache["data"] = data
    _flow_cache["ts"] = now
    return data


@sector_flows_router.get("/credit")
def get_credit_spread():
    """HYG/LQD spread only — faster endpoint for polling."""
    fetched = _bulk_download(["HYG", "LQD"])
    return _build_credit_spread(fetched.get("HYG"), fetched.get("LQD"))


@sector_flows_router.get("/cot")
def get_cot():
    """CFTC COT positioning only."""
    return _fetch_cot_data()
