"""Concurrency tests for the money paths. These hit the service layer with real parallel
connections; the properties asserted are exactly the ones the spike test must preserve."""
import asyncio
import json
import random

import pytest

import audit
from app import service
from conftest import TEST_DSN


async def place(pool, user="u0", key=None, stake=100, market=1, sel="home", expected=None):
    async with pool.acquire() as c:
        return await service.place_bet(
            c, user_id=user, idempotency_key=key, market_id=market, selection=sel, stake=stake,
            expected_odds_milli=expected, escrow_ids=pool.escrow_ids)


async def settle(pool, bet_id, won):
    async with pool.acquire() as c:
        return await service.settle_bet(c, bet_id=bet_id, won=won,
                                        escrow_ids=pool.escrow_ids, house_id=pool.house_id)


async def balance(pool, user="u0"):
    return await pool.fetchval("SELECT balance FROM accounts WHERE kind='wallet' AND owner=$1", user)


async def assert_audit_clean():
    results, _ = await audit.run(TEST_DSN)
    bad = {k: v["violations"] for k, v in results.items() if v["violations"]}
    assert not bad, bad


async def test_double_spend_200_parallel_bets_on_10_bets_of_money(db):
    # u0 has 1000 cents; 200 distinct bets of 100 race. Exactly 10 may win.
    res = await asyncio.gather(*[place(db, key=f"key-{i:04d}") for i in range(200)], return_exceptions=True)
    ok = [r for r in res if isinstance(r, service.Bet)]
    rejected = [r for r in res if isinstance(r, service.InsufficientFunds)]
    assert len(ok) == 10 and len(rejected) == 190
    assert await balance(db) == 0
    await assert_audit_clean()


async def test_same_key_50_parallel_retries_charge_once(db):
    res = await asyncio.gather(*[place(db, key="retry-key-1") for _ in range(50)])
    assert len({r.bet_id for r in res}) == 1
    assert sum(not r.replayed for r in res) == 1
    assert await balance(db) == 900
    await assert_audit_clean()


async def test_key_reuse_with_different_payload_rejected(db):
    await place(db, key="reuse-key-1", stake=100)
    with pytest.raises(service.IdempotencyMismatch):
        await place(db, key="reuse-key-1", stake=500)
    assert await balance(db) == 900


async def test_odds_locked_at_placement_survive_price_change_and_replay(db):
    b1 = await place(db, key="odds-key-1")
    original = b1.locked_odds_milli
    new = json.dumps({"home": original + 500, "away": 2000})
    async with db.acquire() as c, c.transaction():
        await c.execute("UPDATE markets SET odds_milli=$1::jsonb, odds_version=2 WHERE id=1", new)
        await c.execute("INSERT INTO market_odds_history VALUES (1,2,$1::jsonb)", new)
    replay = await place(db, key="odds-key-1")
    assert replay.replayed and replay.locked_odds_milli == original     # still the old price
    b2 = await place(db, key="odds-key-2")
    assert b2.locked_odds_milli == original + 500                       # new bet gets new price
    with pytest.raises(service.OddsChanged):                            # stale client rejected
        await place(db, key="odds-key-3", expected=original)
    await assert_audit_clean()


async def test_insufficient_funds_does_not_burn_the_idempotency_key(db):
    with pytest.raises(service.InsufficientFunds):
        await place(db, key="funds-key-1", stake=5000)
    async with db.acquire() as c:
        await service.deposit(c, user_id="u0", amount=5000, funding_id=db.funding_id)
    b = await place(db, key="funds-key-1", stake=5000)
    assert not b.replayed
    await assert_audit_clean()


async def test_settle_pays_exactly_once_under_races(db):
    b = await place(db, key="settle-key-1", stake=1000)
    res = await asyncio.gather(*[settle(db, b.bet_id, True) for _ in range(20)])
    assert sum(not r.replayed for r in res) == 1
    assert await balance(db) == b.potential_payout          # stake already out, full payout in
    await assert_audit_clean()


async def test_random_mixed_load_keeps_every_invariant(db):
    rnd = random.Random(7)

    async def one(i):
        try:
            b = await place(db, user=f"u{rnd.randrange(5)}", key=f"mix-{i % 150:05d}",
                            stake=rnd.choice((50, 100, 100, 250)), market=1 + i % 2,
                            sel="home" if i % 2 else "away")
            if rnd.random() < 0.5:
                await settle(db, b.bet_id, rnd.random() < 0.45)
        except service.BetError:
            pass

    await asyncio.gather(*[one(i) for i in range(600)])
    await assert_audit_clean()
