import os
import sys
import pathlib

import asyncpg
import pytest_asyncio

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

TEST_DB = "betwallet_test"
TEST_DSN = f"postgresql:///{TEST_DB}"
os.environ["DATABASE_URL"] = TEST_DSN

from seed import seed  # noqa: E402


class Db:
    """asyncpg.Pool uses __slots__, so carry the account ids on a thin wrapper."""

    def __init__(self, pool, escrow_ids, house_id, funding_id):
        self._pool, self.escrow_ids, self.house_id, self.funding_id = pool, escrow_ids, house_id, funding_id

    def __getattr__(self, name):
        return getattr(self._pool, name)


@pytest_asyncio.fixture
async def db():
    admin = await asyncpg.connect("postgresql:///postgres")
    if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", TEST_DB):
        await admin.execute(f"CREATE DATABASE {TEST_DB}")
    await admin.close()
    await seed(users=5, balance=1000, markets=2, shards=4, dsn=TEST_DSN)
    pool = await asyncpg.create_pool(TEST_DSN, min_size=8, max_size=8)
    rows = await pool.fetch("SELECT id, kind FROM accounts WHERE kind IN ('escrow','house','funding') ORDER BY id")
    yield Db(pool,
             [r["id"] for r in rows if r["kind"] == "escrow"],
             next(r["id"] for r in rows if r["kind"] == "house"),
             next(r["id"] for r in rows if r["kind"] == "funding"))
    await pool.close()
