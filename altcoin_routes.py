from __future__ import annotations
import math, os
from datetime import datetime, timezone
from statistics import median
from typing import Any
from fastapi import APIRouter
from altcoin_history import coverage, load_histories, store_market_history, store_metric_snapshot, store_universe, db_summary
from altcoin_sources import fetch_binance_prices, fetch_binance_universe, fetch_cmc_altseason_latest, fetch_cmc_global_latest, fetch_cmc_index_latest
from shared.cg_cache import get_global as cg_get_global

altcoin_router=APIRouter(prefix="/altcoins",tags=["altcoins"])
VERSION="1.0.0"
MIN_HISTORY=200

def _f(v):
    try:
        x=float(v); return x if math.isfinite(x) else None
    except (TypeError,ValueError): return None

def _pct(a,b):
    return ((a/b)-1)*100 if a is not None and b not in (None,0) else None

def _ret(values,days):
    return _pct(values[-1][1],values[-days-1][1]) if len(values)>=days+1 else None

def _ma(values,n):
    return sum(v for _,v in values[-n:])/n if len(values)>=n else None

def _pctn(n,d):
    return round(n/d*100,1) if d else None

def _market():
    missing=[]; latest={}
    try: latest=fetch_cmc_global_latest()
    except Exception as exc: missing.append(f"CMC latest: {exc}")
    altcap=_f(latest.get("altcoin_market_cap")); total=_f(latest.get("total_market_cap")); dom=_f(latest.get("btc_dominance"))
    source="CoinMarketCap"
    if altcap is None:
        try:
            gd=cg_get_global(); total=_f((gd.get("total_market_cap") or {}).get("usd")); dom=_f((gd.get("market_cap_percentage") or {}).get("btc"))
            altcap=total*(1-dom/100) if total is not None and dom is not None else None
            source="CoinGecko fallback"; missing.append("CMC unavailable; using CoinGecko total-minus-BTC fallback")
        except Exception as exc: missing.append(f"CoinGecko fallback: {exc}")
    ts=latest.get("timestamp") or datetime.now(timezone.utc).isoformat()
    if altcap is not None:
        store_market_history({"date":ts[:10],"altcoin_market_cap":altcap,"total_market_cap":total,"btc_dominance":dom,"source":source,"timestamp":ts})
    return {"altcoin_market_cap":altcap,"total_market_cap":total,"btc_dominance":dom,"timestamp":ts,
        "provenance":{"source":source,"source_type":"AGGREGATOR","methodology":"CMC altcoin_market_cap; CoinGecko total-minus-BTC fallback","calculation_version":VERSION}},missing

def _breadth_rotation(universe):
    missing=[]
    included=[r for r in universe if r.get("included")]
    symbols=[r["symbol"] for r in included]
    asset={r["symbol"]:r["asset"] for r in included}
    histories=load_histories(symbols+["BTCUSDT","ETHUSDT"])
    eligible={s:v for s,v in histories.items() if s in asset and len(v)>=MIN_HISTORY}
    cov=coverage(symbols,MIN_HISTORY)
    if not eligible: missing.append("Binance history not seeded; run altcoin_backfill.py")
    prices={}
    try: prices=fetch_binance_prices()
    except Exception as exc: missing.append(f"Binance bulk ticker: {exc}")

    a20=a50=a200=ex200=exn=live200=liven=h30=h90=l30=hl_n=0
    d20=[]; d50=[]; d200=[]
    for s,v in eligible.items():
        cur=v[-1][1]; m20=_ma(v,20); m50=_ma(v,50); m200=_ma(v,200)
        if m20: a20+=cur>m20; d20.append((cur/m20-1)*100)
        if m50: a50+=cur>m50; d50.append((cur/m50-1)*100)
        if m200:
            a200+=cur>m200; d200.append((cur/m200-1)*100)
            if asset[s]!="ETH": exn+=1; ex200+=cur>m200
            live=_f(prices.get(s))
            if live is not None: liven+=1; live200+=live>m200
        if len(v)>=90:
            hl_n+=1; x30=[x for _,x in v[-30:]]; x90=[x for _,x in v[-90:]]
            h30+=cur>=max(x30); h90+=cur>=max(x90); l30+=cur<=min(x30)

    n=len(eligible)
    breadth={"universe_size":len(included),"eligible_200dma":n,"above_20dma_pct":_pctn(a20,n),"above_50dma_pct":_pctn(a50,n),
        "above_200dma_pct":_pctn(a200,n),"ex_eth_above_200dma_pct":_pctn(ex200,exn),"live_above_200dma_pct":_pctn(live200,liven),
        "median_distance_20dma":round(median(d20),2) if d20 else None,"median_distance_50dma":round(median(d50),2) if d50 else None,
        "median_distance_200dma":round(median(d200),2) if d200 else None,"highs_30d_pct":_pctn(h30,hl_n),"highs_90d_pct":_pctn(h90,hl_n),
        "lows_30d_pct":_pctn(l30,hl_n),"coverage":cov,
        "provenance":{"source":"Binance daily closes","source_type":"INTERNAL_DERIVED","universe":"binance_spot_usdt_v1","calculation_version":VERSION}}

    btc=histories.get("BTCUSDT",[]); eth=histories.get("ETHUSDT",[])
    rel={7:[],30:[],90:[]}; rel_eth=[]
    br={d:_ret(btc,d) for d in (7,30,90)}; er30=_ret(eth,30)
    for v in eligible.values():
        for d in (7,30,90):
            ar=_ret(v,d)
            if ar is not None and br[d] is not None: rel[d].append(ar-br[d])
        ar30=_ret(v,30)
        if ar30 is not None and er30 is not None: rel_eth.append(ar30-er30)
    outperform=lambda vals:_pctn(sum(x>0 for x in vals),len(vals))
    ethbtc=(eth[-1][1]/btc[-1][1]) if eth and btc and btc[-1][1] else None
    rotation={"eth_btc":ethbtc,"outperforming_btc_7d_pct":outperform(rel[7]),"outperforming_btc_30d_pct":outperform(rel[30]),
        "outperforming_btc_90d_pct":outperform(rel[90]),"median_relative_return_btc_30d":round(median(rel[30]),2) if rel[30] else None,
        "outperforming_eth_30d_pct":outperform(rel_eth),"median_relative_return_eth_30d":round(median(rel_eth),2) if rel_eth else None,
        "provenance":{"source":"Binance daily closes","source_type":"INTERNAL_DERIVED","universe":"binance_spot_usdt_v1","calculation_version":VERSION}}
    return breadth,rotation,missing

def _benchmarks():
    missing=[]; result={}
    for name in ("cmc20","cmc100"):
        try: result[name]=fetch_cmc_index_latest(name)
        except Exception as exc: result[name]=None; missing.append(f"{name.upper()}: {exc}")
    try: result["external_altseason_index"]=fetch_cmc_altseason_latest()
    except Exception as exc: result["external_altseason_index"]=None; missing.append(f"CMC Altcoin Season Index: {exc}")
    refs=load_histories(["BTCUSDT","ETHUSDT"],60)
    result["btc"]={"change_30d":_ret(refs.get("BTCUSDT",[]),30)}
    result["eth"]={"change_30d":_ret(refs.get("ETHUSDT",[]),30)}
    return result,missing

def _state(market,breadth,rotation):
    b=_f(breadth.get("above_200dma_pct")); ext=_f(breadth.get("median_distance_200dma")); rel=_f(rotation.get("outperforming_btc_30d_pct"))
    supporting=[]; contradicting=[]
    if b is None: label="RECOVERING"; confidence=0; summary="Breadth history is not yet sufficiently seeded to resolve the internal state."
    elif b<35: label="DEPRESSED"; confidence=85; summary="Long-term participation remains narrow."
    elif b<60: label="RECOVERING"; confidence=75; summary="Participation is improving but is not yet broadly established."
    elif ext is not None and ext>=30: label="EXTENDED"; confidence=80; summary="Participation is broad and median long-term extension is elevated."
    else: label="BROAD_ADVANCE"; confidence=80; summary="Participation is broad without a clear extension trigger."
    if b is not None: (supporting if b>=60 else contradicting).append(f"{b:.1f}% of eligible altcoins are above the 200DMA.")
    if rel is not None: (supporting if rel>=50 else contradicting).append(f"{rel:.1f}% of the eligible universe outperformed BTC over 30D.")
    covered=breadth.get("coverage",{}).get("with_minimum_history",0); size=breadth.get("coverage",{}).get("universe_symbols",0)
    coverage_pct=(covered/size*100) if size else 0
    return {"label":label,"confidence":round(min(confidence,coverage_pct)) if b is not None else 0,"coverage":round(coverage_pct,1),
        "timestamp":datetime.now(timezone.utc).isoformat(),"summary":summary},supporting,contradicting

@altcoin_router.get("/metrics")
def altcoin_metrics():
    now=datetime.now(timezone.utc); missing=[]
    try: universe=fetch_binance_universe(); store_universe(now.date().isoformat(),universe)
    except Exception as exc: universe=[]; missing.append(f"Binance universe: {exc}")
    market,m=_market(); missing+=m
    breadth,rotation,m=_breadth_rotation(universe); missing+=m
    benchmarks,m=_benchmarks(); missing+=m
    state,supporting,contradicting=_state(market,breadth,rotation)
    payload={"state":state,"market":market,"breadth":breadth,"rotation":rotation,"benchmarks":benchmarks,
        "exchange_activity":{"status":"not_enabled","note":"V1.1 enrichment pending CryptoQuant endpoint confirmation."},
        "diagnostics":{"supporting":supporting,"contradicting":contradicting,"missing":missing,
            "methodology":{"version":VERSION,"state_flow":["DEPRESSED","RECOVERING","BROAD_ADVANCE","EXTENDED","DETERIORATING"],
                "universe":"binance_spot_usdt_v1","minimum_history_days":MIN_HISTORY},
            "providers":{"coinmarketcap_api_key":bool(os.getenv("CMC_API_KEY")),"binance_public_market_data":True,"coinpaprika":"adapter_ready","cryptoquant_exchange_deposits":"v1.1"}}}
    try: store_metric_snapshot(now.date().isoformat(),VERSION,payload)
    except Exception as exc: payload["diagnostics"]["missing"].append(f"Persistence: {exc}")
    return payload

@altcoin_router.get("/health")
def altcoin_health():
    return {"status":"ok","calculation_version":VERSION,"db":db_summary(),"cmc_api_key":bool(os.getenv("CMC_API_KEY"))}
