"""
Standalone OI snapshot script.
Run via cron every 15 minutes:
  */15 * * * * cd /path/to/btc-dashboard-api && venv/bin/python poll_oi.py
"""
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from data_sources import _fetch_coingecko_derivatives
from oi_history import init_db, store_snapshot, get_snapshot_count, prune_old_snapshots

init_db()
markets = _fetch_coingecko_derivatives()
if markets:
    total_oi = sum(m["open_interest"] for m in markets)
    store_snapshot(total_oi)
    print(f"Stored: ${total_oi/1e9:.1f}B — {get_snapshot_count()} total")
    prune_old_snapshots()
else:
    print("No data returned — skipping snapshot")