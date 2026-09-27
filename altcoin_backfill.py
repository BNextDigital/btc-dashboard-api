from __future__ import annotations
import argparse,csv,io,zipfile
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import date,datetime,timedelta,timezone
import requests
from altcoin_history import db_summary,store_universe,upsert_daily_closes
from altcoin_sources import fetch_binance_universe

BASE="https://data.binance.vision/data/spot"

def _months(start,end):
    out=[]; y,m=start.year,start.month
    while (y,m)<=(end.year,end.month):
        out.append((y,m)); m+=1
        if m==13: y+=1; m=1
    return out

def _parse(content,symbol,asset):
    out=[]
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names=z.namelist()
        if not names:return out
        for r in csv.reader(io.StringIO(z.read(names[0]).decode())):
            try:
                ts=int(r[0]); ts=ts//1000 if ts>10**14 else ts
                day=datetime.fromtimestamp(ts/1000,tz=timezone.utc).date().isoformat()
                out.append({"symbol":symbol,"asset":asset,"date":day,"close":float(r[4]),"source":"Binance static archive"})
            except (ValueError,IndexError,OSError): pass
    return out

def _get(url):
    r=requests.get(url,timeout=30)
    if r.status_code==404:return []
    r.raise_for_status()
    return r.content

def seed(days=260,workers=8,symbol_filter=None):
    today=datetime.now(timezone.utc).date(); end=today-timedelta(days=1); start=end-timedelta(days=days-1)
    universe=fetch_binance_universe(); store_universe(today.isoformat(),universe)
    rows={r["symbol"]:r for r in universe if r.get("included")}
    rows["BTCUSDT"]={"symbol":"BTCUSDT","asset":"BTC"}; rows["ETHUSDT"]={"symbol":"ETHUSDT","asset":"ETH"}
    if symbol_filter: rows={k:v for k,v in rows.items() if k in symbol_filter}
    current=date(today.year,today.month,1); jobs=[]
    for r in rows.values():
        s,a=r["symbol"],r["asset"]
        for y,m in _months(start,end):
            if date(y,m,1)<current:
                jobs.append((s,a,f"{BASE}/monthly/klines/{s}/1d/{s}-1d-{y:04d}-{m:02d}.zip"))
        d=max(start,current)
        while d<=end:
            jobs.append((s,a,f"{BASE}/daily/klines/{s}/1d/{s}-1d-{d.isoformat()}.zip")); d+=timedelta(days=1)
    written=errors=0
    def run(job):
        s,a,url=job; content=_get(url); return _parse(content,s,a) if content else []
    with ThreadPoolExecutor(max_workers=max(1,workers)) as pool:
        futures={pool.submit(run,j):j for j in jobs}
        for i,f in enumerate(as_completed(futures),1):
            try: written+=upsert_daily_closes(f.result())
            except Exception as exc: errors+=1; print(f"[altcoin_backfill] {futures[f][0]}: {exc}")
            if i%100==0 or i==len(jobs): print(f"[altcoin_backfill] {i}/{len(jobs)} archives · {written} rows · {errors} errors")
    return {"symbols":len(rows),"jobs":len(jobs),"rows_written":written,"errors":errors}

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--days",type=int,default=260); p.add_argument("--workers",type=int,default=8); p.add_argument("--symbols",default="")
    a=p.parse_args(); filt={x.strip().upper() for x in a.symbols.split(",") if x.strip()} or None
    print(seed(a.days,a.workers,filt)); print(db_summary())
