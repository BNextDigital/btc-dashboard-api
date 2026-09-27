from __future__ import annotations
import math, os
from datetime import datetime, timezone
from statistics import median
from typing import Any
from fastapi import APIRouter
from altcoin_history import coverage, get_market_history, load_histories, store_market_history, store_metric_snapshot, store_universe, db_summary
from altcoin_sources import fetch_binance_prices, fetch_binance_universe, fetch_cmc_altseason_latest, fetch_cmc_altseason_history, fetch_cmc_global_latest, fetch_cmc_index_latest, fetch_cmc_index_history
from shared.cg_cache import get_global as cg_get_global

altcoin_router=APIRouter(prefix="/altcoins",tags=["altcoins"])
VERSION="1.0.0"
MIN_HISTORY=200
MIN_STATE_SAMPLE=100
MIN_STATE_COVERAGE_PCT=20.0

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

def _change_from_history(rows, days, value_key="value"):
    values=[_f(r.get(value_key)) for r in rows if isinstance(r,dict)]
    values=[v for v in values if v is not None]
    if len(values)<2:
        return None
    current=values[-1]
    index=max(0,len(values)-1-days)
    prior=values[index]
    return round(_pct(current,prior),2) if prior not in (None,0) else None

def _pp_change(current, prior):
    return round(current-prior,1) if current is not None and prior is not None else None

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

    hist=get_market_history(40)
    dom7=_f(hist[-8].get("btc_dominance")) if len(hist)>=8 else None
    dom30=_f(hist[-31].get("btc_dominance")) if len(hist)>=31 else None

    return {
        "altcoin_market_cap":altcap,
        "total_market_cap":total,
        "btc_dominance":dom,
        "btc_dominance_change_7d_pp":_pp_change(dom,dom7),
        "btc_dominance_change_30d_pp":_pp_change(dom,dom30),
        "timestamp":ts,
        "provenance":{
            "source":source,
            "source_type":"AGGREGATOR",
            "methodology":"CMC altcoin_market_cap; CoinGecko total-minus-BTC fallback",
            "calculation_version":VERSION,
        },
    },missing

def _breadth_rotation(universe):
    missing=[]
    included=[r for r in universe if r.get("included")]
    symbols=[r["symbol"] for r in included]
    asset={r["symbol"]:r["asset"] for r in included}
    histories=load_histories(symbols+["BTCUSDT","ETHUSDT"])

    asset_histories={s:v for s,v in histories.items() if s in asset}
    eligible20={s:v for s,v in asset_histories.items() if len(v)>=20}
    eligible50={s:v for s,v in asset_histories.items() if len(v)>=50}
    eligible200={s:v for s,v in asset_histories.items() if len(v)>=200}
    eligible_high30={s:v for s,v in asset_histories.items() if len(v)>=30}
    eligible_high90={s:v for s,v in asset_histories.items() if len(v)>=90}

    with_any=sum(1 for v in asset_histories.values() if v)
    cov={
        "universe_symbols":len(symbols),
        "with_any_history":with_any,
        "with_minimum_history":len(eligible200),
        "minimum_days":MIN_HISTORY,
        "eligible_20dma":len(eligible20),
        "eligible_50dma":len(eligible50),
        "eligible_200dma":len(eligible200),
        "eligible_highs_30d":len(eligible_high30),
        "eligible_highs_90d":len(eligible_high90),
    }
    if not eligible200:
        missing.append("Binance history not seeded sufficiently for 200DMA breadth")

    prices={}
    try:
        prices=fetch_binance_prices()
    except Exception as exc:
        missing.append(f"Binance bulk ticker: {exc}")

    def _ma_stats(rows,n):
        above=0
        distances=[]
        for _,v in rows.items():
            cur=v[-1][1]
            ma=_ma(v,n)
            if ma:
                above+=cur>ma
                distances.append((cur/ma-1)*100)
        return _pctn(above,len(rows)), (round(median(distances),2) if distances else None)

    above20,dist20=_ma_stats(eligible20,20)
    above50,dist50=_ma_stats(eligible50,50)
    above200,dist200=_ma_stats(eligible200,200)

    def _historical_ma_breadth(rows,n,days_ago):
        usable={}
        for s,v in rows.items():
            if len(v) >= n + days_ago:
                cutoff=len(v)-days_ago if days_ago else len(v)
                usable[s]=v[:cutoff]
        pct_value,_=_ma_stats(usable,n)
        return pct_value

    above20_7d=_historical_ma_breadth(eligible20,20,7)
    above20_30d=_historical_ma_breadth(eligible20,20,30)
    above50_7d=_historical_ma_breadth(eligible50,50,7)
    above50_30d=_historical_ma_breadth(eligible50,50,30)
    above200_7d=_historical_ma_breadth(eligible200,200,7)
    above200_30d=_historical_ma_breadth(eligible200,200,30)

    ex200={s:v for s,v in eligible200.items() if asset[s]!="ETH"}
    ex_above=0
    for _,v in ex200.items():
        cur=v[-1][1]; ma=_ma(v,200)
        if ma:
            ex_above+=cur>ma

    live_above=live_n=0
    for s,v in eligible200.items():
        ma=_ma(v,200)
        live=_f(prices.get(s))
        if ma and live is not None:
            live_n+=1
            live_above+=live>ma

    h30=l30=0
    for v in eligible_high30.values():
        cur=v[-1][1]
        window=[x for _,x in v[-30:]]
        h30+=cur>=max(window)
        l30+=cur<=min(window)

    h90=0
    for v in eligible_high90.values():
        cur=v[-1][1]
        window=[x for _,x in v[-90:]]
        h90+=cur>=max(window)

    breadth={
        "universe_size":len(included),
        "eligible_20dma":len(eligible20),
        "eligible_50dma":len(eligible50),
        "eligible_200dma":len(eligible200),
        "above_20dma_pct":above20,
        "above_20dma_change_7d_pp":_pp_change(above20,above20_7d),
        "above_20dma_change_30d_pp":_pp_change(above20,above20_30d),
        "above_50dma_pct":above50,
        "above_50dma_change_7d_pp":_pp_change(above50,above50_7d),
        "above_50dma_change_30d_pp":_pp_change(above50,above50_30d),
        "above_200dma_pct":above200,
        "above_200dma_change_7d_pp":_pp_change(above200,above200_7d),
        "above_200dma_change_30d_pp":_pp_change(above200,above200_30d),
        "ex_eth_above_200dma_pct":_pctn(ex_above,len(ex200)),
        "live_above_200dma_pct":_pctn(live_above,live_n),
        "median_distance_20dma":dist20,
        "median_distance_50dma":dist50,
        "median_distance_200dma":dist200,
        "highs_30d_pct":_pctn(h30,len(eligible_high30)),
        "highs_90d_pct":_pctn(h90,len(eligible_high90)),
        "lows_30d_pct":_pctn(l30,len(eligible_high30)),
        "coverage":cov,
        "provenance":{
            "source":"Binance daily closes",
            "source_type":"INTERNAL_DERIVED",
            "universe":"binance_spot_usdt_v1",
            "calculation_version":VERSION,
        },
    }

    btc=histories.get("BTCUSDT",[])
    eth=histories.get("ETHUSDT",[])
    br={d:_ret(btc,d) for d in (7,30,90)}
    er30=_ret(eth,30)

    rel={7:[],30:[],90:[]}
    rel_eth=[]
    relative_eligible={7:0,30:0,90:0}

    for v in asset_histories.values():
        for d in (7,30,90):
            ar=_ret(v,d)
            if ar is not None and br[d] is not None:
                rel[d].append(ar-br[d])
                relative_eligible[d]+=1
        ar30=_ret(v,30)
        if ar30 is not None and er30 is not None:
            rel_eth.append(ar30-er30)

    outperform=lambda vals:_pctn(sum(x>0 for x in vals),len(vals))
    ethbtc=(eth[-1][1]/btc[-1][1]) if eth and btc and btc[-1][1] else None
    ethbtc7=(eth[-8][1]/btc[-8][1]) if len(eth)>=8 and len(btc)>=8 and btc[-8][1] else None
    ethbtc30=(eth[-31][1]/btc[-31][1]) if len(eth)>=31 and len(btc)>=31 and btc[-31][1] else None
    rotation={
        "eth_btc":ethbtc,
        "eth_btc_change_7d_pct":round(_pct(ethbtc,ethbtc7),2) if ethbtc is not None and ethbtc7 else None,
        "eth_btc_change_30d_pct":round(_pct(ethbtc,ethbtc30),2) if ethbtc is not None and ethbtc30 else None,
        "eligible_relative_7d":relative_eligible[7],
        "eligible_relative_30d":relative_eligible[30],
        "eligible_relative_90d":relative_eligible[90],
        "outperforming_btc_7d_pct":outperform(rel[7]),
        "outperforming_btc_30d_pct":outperform(rel[30]),
        "outperforming_btc_90d_pct":outperform(rel[90]),
        "median_relative_return_btc_30d":round(median(rel[30]),2) if rel[30] else None,
        "eligible_relative_eth_30d":len(rel_eth),
        "outperforming_eth_30d_pct":outperform(rel_eth),
        "median_relative_return_eth_30d":round(median(rel_eth),2) if rel_eth else None,
        "provenance":{
            "source":"Binance daily closes",
            "source_type":"INTERNAL_DERIVED",
            "universe":"binance_spot_usdt_v1",
            "calculation_version":VERSION,
        },
    }
    return breadth,rotation,missing

def _benchmarks():
    missing=[]
    result={}
    for name in ("cmc20","cmc100"):
        try:
            latest=fetch_cmc_index_latest(name)
            try:
                hist=fetch_cmc_index_history(name,31)
                latest["change_7d_pct"]=_change_from_history(hist,7)
                latest["change_30d_pct"]=_change_from_history(hist,30)
            except Exception as exc:
                latest["change_7d_pct"]=None
                latest["change_30d_pct"]=None
                missing.append(f"{name.upper()} history: {exc}")
            result[name]=latest
        except Exception as exc:
            result[name]=None
            missing.append(f"{name.upper()}: {exc}")

    try:
        alt_latest=fetch_cmc_altseason_latest()
        try:
            hist=fetch_cmc_altseason_history("30d")
            alt_latest["change_7d"]=_pp_change(
                alt_latest.get("value"),
                _f(hist[-8].get("value")) if len(hist)>=8 else None,
            )
            alt_latest["change_30d"]=_pp_change(
                alt_latest.get("value"),
                _f(hist[0].get("value")) if hist else None,
            )
            cap_values=[r for r in hist if _f(r.get("altcoin_market_cap")) is not None]
            alt_latest["altcoin_market_cap_change_7d_pct"]=_change_from_history(
                cap_values,7,"altcoin_market_cap"
            )
            alt_latest["altcoin_market_cap_change_30d_pct"]=_change_from_history(
                cap_values,30,"altcoin_market_cap"
            )
        except Exception as exc:
            alt_latest["change_7d"]=None
            alt_latest["change_30d"]=None
            alt_latest["altcoin_market_cap_change_7d_pct"]=None
            alt_latest["altcoin_market_cap_change_30d_pct"]=None
            missing.append(f"CMC Altcoin Season history: {exc}")
        result["external_altseason_index"]=alt_latest
    except Exception as exc:
        result["external_altseason_index"]=None
        missing.append(f"CMC Altcoin Season Index: {exc}")

    refs=load_histories(["BTCUSDT","ETHUSDT"],60)
    result["btc"]={
        "change_7d":_ret(refs.get("BTCUSDT",[]),7),
        "change_30d":_ret(refs.get("BTCUSDT",[]),30),
    }
    result["eth"]={
        "change_7d":_ret(refs.get("ETHUSDT",[]),7),
        "change_30d":_ret(refs.get("ETHUSDT",[]),30),
    }
    return result,missing

def _state(market,breadth,rotation):
    b=_f(breadth.get("above_200dma_pct"))
    ext=_f(breadth.get("median_distance_200dma"))
    rel=_f(rotation.get("outperforming_btc_30d_pct"))
    supporting=[]
    contradicting=[]

    sample=int(breadth.get("eligible_200dma") or 0)
    size=int(breadth.get("universe_size") or 0)
    coverage_pct=(sample/size*100) if size else 0.0
    sample_ready=(
        sample>=MIN_STATE_SAMPLE
        and coverage_pct>=MIN_STATE_COVERAGE_PCT
        and b is not None
    )

    if not sample_ready:
        label="UNRESOLVED"
        confidence=0
        summary=(
            f"State withheld: 200DMA breadth sample is {sample}/{size} "
            f"({coverage_pct:.1f}% coverage), below the minimum "
            f"{MIN_STATE_SAMPLE} assets and {MIN_STATE_COVERAGE_PCT:.0f}% coverage."
        )
    elif b<35:
        label="DEPRESSED"
        confidence=85
        summary="Long-term participation remains narrow."
    elif b<60:
        label="RECOVERING"
        confidence=75
        summary="Participation is improving but is not yet broadly established."
    elif ext is not None and ext>=30:
        label="EXTENDED"
        confidence=80
        summary="Participation is broad and median long-term extension is elevated."
    else:
        label="BROAD_ADVANCE"
        confidence=80
        summary="Participation is broad without a clear extension trigger."

    if sample_ready:
        if b is not None:
            (supporting if b>=60 else contradicting).append(
                f"{b:.1f}% of {sample} 200DMA-eligible altcoins are above the 200DMA."
            )
        if rel is not None:
            rel_n=int(rotation.get("eligible_relative_30d") or 0)
            (supporting if rel>=50 else contradicting).append(
                f"{rel:.1f}% of {rel_n} 30D-eligible altcoins outperformed BTC."
            )

    resolved_confidence=round(min(confidence,coverage_pct)) if sample_ready else 0
    return {
        "label":label,
        "confidence":resolved_confidence,
        "coverage":round(coverage_pct,1),
        "sample_size":sample,
        "minimum_sample_size":MIN_STATE_SAMPLE,
        "minimum_coverage_pct":MIN_STATE_COVERAGE_PCT,
        "timestamp":datetime.now(timezone.utc).isoformat(),
        "summary":summary,
    },supporting,contradicting

@altcoin_router.get("/metrics")
def altcoin_metrics():
    now=datetime.now(timezone.utc); missing=[]
    try: universe=fetch_binance_universe(); store_universe(now.date().isoformat(),universe)
    except Exception as exc: universe=[]; missing.append(f"Binance universe: {exc}")
    market,m=_market(); missing+=m
    breadth,rotation,m=_breadth_rotation(universe); missing+=m
    benchmarks,m=_benchmarks(); missing+=m
    altseason=benchmarks.get("external_altseason_index") if isinstance(benchmarks,dict) else None
    if isinstance(altseason,dict):
        market["change_7d_pct"]=altseason.get("altcoin_market_cap_change_7d_pct")
        market["change_30d_pct"]=altseason.get("altcoin_market_cap_change_30d_pct")
    state,supporting,contradicting=_state(market,breadth,rotation)
    payload={"state":state,"market":market,"breadth":breadth,"rotation":rotation,"benchmarks":benchmarks,
        "exchange_activity":{"status":"not_enabled","note":"V1.1 enrichment pending CryptoQuant endpoint confirmation."},
        "diagnostics":{"supporting":supporting,"contradicting":contradicting,"missing":missing,
            "methodology":{"version":VERSION,"state_flow":["DEPRESSED","RECOVERING","BROAD_ADVANCE","EXTENDED","DETERIORATING"],
                "universe":"binance_spot_usdt_v1","minimum_history_days":MIN_HISTORY,"state_minimum_sample":MIN_STATE_SAMPLE,"state_minimum_coverage_pct":MIN_STATE_COVERAGE_PCT},
            "providers":{"coinmarketcap_api_key":bool(os.getenv("CMC_API_KEY")),"binance_public_market_data":True,"coinpaprika":"adapter_ready","cryptoquant_exchange_deposits":"v1.1"}}}
    try: store_metric_snapshot(now.date().isoformat(),VERSION,payload)
    except Exception as exc: payload["diagnostics"]["missing"].append(f"Persistence: {exc}")
    return payload

@altcoin_router.get("/health")
def altcoin_health():
    return {"status":"ok","calculation_version":VERSION,"db":db_summary(),"cmc_api_key":bool(os.getenv("CMC_API_KEY"))}
