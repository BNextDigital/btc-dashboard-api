"""
sector_flows_routes.py — Sector Capital Flow Matrix

Tracks money flow, volume momentum, and relative outperformance across sectors.
Add to main.py and register with: app.include_router(sector_flows_router)

Endpoints:
  GET /sector-flows/metrics
"""

import os, time
from datetime import datetime, timedelta, date
from fastapi import APIRouter
import yfinance as yf
import pandas as pd

sector_flows_router = APIRouter(prefix="/sector-flows")

DATA_DIR = os.getenv("DATA_DIR", "./data")

# Sector definitions (ticker, display name, asset_class)
SECTORS = {
    "equities":   ("^GSPC", "S&P 500"),
    "tech":       ("XLK", "Technology"),
    "financials": ("XLF", "Financials"),
    "energy":     ("XLE", "Energy"),
    "realestate": ("XLRE", "Real Estate"),
    "metals":     ("GC=F", "Gold (Metals)"),
    "crypto":     ("BTC-USD", "Bitcoin (Crypto)"),
    "bonds":      ("TLT", "US Bonds (20y)"),
}

_flow_cache = {"data": None, "ts": 0.0}
FLOW_CACHE_TTL = 300  # 5 min


def _fetch_sector_data(n_days: int = 300) -> dict:
    """Fetch OHLCV data for all sectors."""
    result = {}
    for key, (ticker, name) in SECTORS.items():
        try:
            df = yf.download(ticker, period=f"{n_days}d", auto_adjust=True, progress=False)
            if df is not None and len(df) > 0:
                result[key] = df
            else:
                result[key] = None
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            result[key] = None
    return result


def _calculate_mfi(df, period: int = 14) -> float | None:
    """
    Money Flow Index (0–100).
    >70 = overbought (selling pressure), <30 = oversold (buying pressure)
    """
    if df is None or len(df) < period:
        return None
    
    try:
        tp = (df['High'] + df['Low'] + df['Close']) / 3  # typical price
        mf = tp * df['Volume']  # money flow
        
        positive_mf = 0
        negative_mf = 0
        
        for i in range(1, period + 1):
            if tp.iloc[-i] > tp.iloc[-i-1]:
                positive_mf += mf.iloc[-i]
            else:
                negative_mf += mf.iloc[-i]
        
        if negative_mf == 0:
            return 100.0
        mfi_ratio = positive_mf / negative_mf
        mfi = 100 - (100 / (1 + mfi_ratio))
        return round(mfi, 2)
    except Exception as e:
        print(f"MFI calc error: {e}")
        return None


def _calculate_obv(df) -> float | None:
    """
    On-Balance Volume trend.
    Positive = accumulation (buying), Negative = distribution (selling)
    Returns the last OBV value (cumulative).
    """
    if df is None or len(df) < 2:
        return None
    
    try:
        obv = 0
        obv_vals = []
        
        for i in range(len(df)):
            if i == 0:
                if df['Close'].iloc[i] > df['Close'].iloc[i] if i > 0 else 0:
                    obv += df['Volume'].iloc[i]
                else:
                    obv -= df['Volume'].iloc[i]
            else:
                if df['Close'].iloc[i] > df['Close'].iloc[i-1]:
                    obv += df['Volume'].iloc[i]
                elif df['Close'].iloc[i] < df['Close'].iloc[i-1]:
                    obv -= df['Volume'].iloc[i]
            obv_vals.append(obv)
        
        # Return normalized OBV trend (-100 to +100)
        min_obv = min(obv_vals[-20:]) if len(obv_vals) >= 20 else min(obv_vals)
        max_obv = max(obv_vals[-20:]) if len(obv_vals) >= 20 else max(obv_vals)
        
        if max_obv == min_obv:
            return 0.0
        
        normalized = ((obv_vals[-1] - min_obv) / (max_obv - min_obv)) * 200 - 100
        return round(normalized, 1)
    except Exception as e:
        print(f"OBV calc error: {e}")
        return None


def _calculate_volume_momentum(df) -> float | None:
    """
    Volume momentum: current 5d avg volume / 20d avg volume
    >1.2 = strong inflow, <0.8 = capital drying up
    """
    if df is None or len(df) < 20:
        return None
    
    try:
        vol_5d = df['Volume'].tail(5).mean()
        vol_20d = df['Volume'].tail(20).mean()
        
        if vol_20d == 0:
            return None
        
        ratio = vol_5d / vol_20d
        return round(ratio, 2)
    except Exception as e:
        print(f"Volume momentum calc error: {e}")
        return None


def _calculate_relative_performance(df, benchmark_df) -> float | None:
    """
    Sector return vs benchmark return (5d and 20d)
    Positive = outperforming, Negative = underperforming
    """
    if df is None or benchmark_df is None or len(df) < 20:
        return None
    
    try:
        sector_ret_5d = ((df['Close'].iloc[-1] - df['Close'].iloc[-6]) / df['Close'].iloc[-6] * 100) if len(df) >= 6 else 0
        bench_ret_5d = ((benchmark_df['Close'].iloc[-1] - benchmark_df['Close'].iloc[-6]) / benchmark_df['Close'].iloc[-6] * 100) if len(benchmark_df) >= 6 else 0
        
        outperformance = sector_ret_5d - bench_ret_5d
        return round(outperformance, 2)
    except Exception as e:
        print(f"Relative performance calc error: {e}")
        return None


def _build_sector_flows() -> dict:
    """Build complete sector flow matrix."""
    sector_data = _fetch_sector_data(n_days=300)
    benchmark_df = sector_data.get("equities")
    
    result = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "sectors": {}
    }
    
    for sector_key, (ticker, name) in SECTORS.items():
        df = sector_data.get(sector_key)
        
        mfi = _calculate_mfi(df)
        obv = _calculate_obv(df)
        vol_mom = _calculate_volume_momentum(df)
        rel_perf = _calculate_relative_performance(df, benchmark_df)
        
        # Current price + change
        current = None
        change_5d = None
        if df is not None and len(df) > 0:
            current = round(df['Close'].iloc[-1], 2)
            if len(df) >= 6:
                change_5d = round((df['Close'].iloc[-1] - df['Close'].iloc[-6]) / df['Close'].iloc[-6] * 100, 2)
        
        # Flow signal: synthesize MFI + volume momentum
        flow_signal = None
        if mfi is not None and vol_mom is not None:
            if mfi > 70 and vol_mom > 1.2:
                flow_signal = "Heavy Inflow"
            elif mfi > 60 and vol_mom > 1.0:
                flow_signal = "Strong Inflow"
            elif mfi < 30 and vol_mom < 0.8:
                flow_signal = "Heavy Outflow"
            elif mfi < 40 and vol_mom < 1.0:
                flow_signal = "Weak Outflow"
            else:
                flow_signal = "Stable"
        
        result["sectors"][sector_key] = {
            "name": name,
            "ticker": ticker,
            "current": current,
            "change_5d": change_5d,
            "mfi": mfi,              # Money Flow Index (0-100)
            "obv": obv,              # On-Balance Volume (-100 to +100)
            "volume_momentum": vol_mom,  # 5d vol / 20d vol
            "relative_performance": rel_perf,  # vs S&P 500
            "flow_signal": flow_signal,
        }
    
    return result


@sector_flows_router.get("/metrics")
def get_sector_flows_metrics():
    """
    Returns sector capital flow matrix with:
    - MFI (money flow intensity)
    - OBV (accumulation vs distribution)
    - Volume momentum (flow velocity)
    - Relative performance (sector vs S&P 500)
    - Flow signal (composite)
    """
    global _flow_cache
    now = time.time()
    
    if _flow_cache["data"] and (now - _flow_cache["ts"]) < FLOW_CACHE_TTL:
        return _flow_cache["data"]
    
    data = _build_sector_flows()
    _flow_cache["data"] = data
    _flow_cache["ts"] = now
    return data
