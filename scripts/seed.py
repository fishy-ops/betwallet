"""Reset the schema and seed users/markets. Usage: seed.py [--users N] [--balance CENTS] [--markets M]

Opening balances are real ledger entries (funding -> wallet) in one balanced txn, so the global
sum-to-zero invariant holds from the first row.
"""
import argparse
import asyncio
import os
import pathlib

import asyncpg

DSN = os.environ.get("DATABASE_URL", "postgresql:///betwallet")
SCHEMA = pathlib.Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


async def seed(users=2000, balance=100_000, markets=20, shards=16, dsn=DSN):
    c = await asyncpg.connect(dsn)
    try:
        await c.execute(SCHEMA.read_text())
        funding = await c.fetchval("INSERT INTO accounts(kind, owner) VALUES ('funding','funding') RETURNING id")
        await c.fetchval("INSERT INTO accounts(kind, owner) VALUES ('house','house') RETURNING id")
        await c.executemany("INSERT INTO accounts(kind, owner) VALUES ('escrow',$1)",
                            [(f"escrow-{i}",) for i in range(shards)])
        await c.executemany("INSERT INTO accounts(kind, owner) VALUES ('wallet',$1)",
                            [(f"u{i}",) for i in range(users)])
        await c.executemany(
            "INSERT INTO markets(name, odds_milli) VALUES ($1, $2::jsonb)",
            [(f"market-{i}", '{"home": %d, "away": %d}' % (1500 + 37 * i, 2600 - 23 * i)) for i in range(markets)])
        await c.execute("INSERT INTO market_odds_history SELECT id, odds_version, odds_milli FROM markets")
        async with c.transaction():
            t = await c.fetchval("INSERT INTO ledger_txns(kind) VALUES ('deposit') RETURNING id")
            await c.execute("UPDATE accounts SET balance=$1 WHERE kind='wallet'", balance)
            await c.execute("UPDATE accounts SET balance=$1 WHERE id=$2", -balance * users, funding)
            await c.execute(
                "INSERT INTO ledger_entries(txn_id, account_id, amount) "
                "SELECT $1, id, $2 FROM accounts WHERE kind='wallet'", t, balance)
            await c.execute("INSERT INTO ledger_entries(txn_id, account_id, amount) VALUES ($1,$2,$3)",
                            t, funding, -balance * users)
    finally:
        await c.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=2000)
    ap.add_argument("--balance", type=int, default=100_000)
    ap.add_argument("--markets", type=int, default=20)
    a = ap.parse_args()
    asyncio.run(seed(a.users, a.balance, a.markets))
    print(f"seeded {a.users} users x {a.balance} cents, {a.markets} markets")
