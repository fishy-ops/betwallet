"""Moves market prices while a load test runs, so 'odds locked at placement' is exercised for real.

    odds_ticker.py [--interval 0.05] [--seconds 120]

Each tick atomically bumps odds_version, rewrites odds, and records the new price in
market_odds_history (one txn). audit.py later proves every bet locked a price that existed.
"""
import argparse
import asyncio
import json
import os
import random
import time

import asyncpg

DSN = os.environ.get("DATABASE_URL", "postgresql:///betwallet")


async def main(interval, seconds):
    c = await asyncpg.connect(DSN)
    ids = [r["id"] for r in await c.fetch("SELECT id FROM markets ORDER BY id")]
    end = time.time() + seconds
    ticks = 0
    while time.time() < end:
        mid = random.choice(ids)
        async with c.transaction():
            row = await c.fetchrow("SELECT odds_milli, odds_version FROM markets WHERE id=$1 FOR UPDATE", mid)
            odds = json.loads(row["odds_milli"]) if isinstance(row["odds_milli"], str) else row["odds_milli"]
            new = {k: max(1100, v + random.choice((-40, -20, 20, 40))) for k, v in odds.items()}
            ver = row["odds_version"] + 1
            await c.execute("UPDATE markets SET odds_milli=$2::jsonb, odds_version=$3 WHERE id=$1",
                            mid, json.dumps(new), ver)
            await c.execute("INSERT INTO market_odds_history VALUES ($1,$2,$3::jsonb)", mid, ver, json.dumps(new))
        ticks += 1
        await asyncio.sleep(interval)
    print(f"odds_ticker: {ticks} price changes")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=0.05)
    ap.add_argument("--seconds", type=float, default=120)
    a = ap.parse_args()
    asyncio.run(main(a.interval, a.seconds))
