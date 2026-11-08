"""Core money logic. Every function here runs in exactly one DB transaction."""
import hashlib
import json
import zlib
from dataclasses import dataclass

import asyncpg


class BetError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, detail: str = ""):
        super().__init__(detail)
        self.detail = detail


class InsufficientFunds(BetError):
    status, code = 402, "insufficient_funds"


class OddsChanged(BetError):
    status, code = 409, "odds_changed"


class MarketClosed(BetError):
    status, code = 409, "market_closed"


class NotFound(BetError):
    status, code = 404, "not_found"


class IdempotencyMismatch(BetError):
    status, code = 422, "idempotency_key_reused_with_different_request"


@dataclass
class Bet:
    bet_id: int
    user_id: str
    market_id: int
    selection: str
    stake: int
    locked_odds_milli: int
    potential_payout: int
    status: str
    replayed: bool = False

    def as_dict(self):
        return self.__dict__


def request_hash(user_id, market_id, selection, stake, expected_odds_milli) -> str:
    raw = f"{user_id}|{market_id}|{selection}|{stake}|{expected_odds_milli}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def payout_for(stake: int, odds_milli: int) -> int:
    return stake * odds_milli // 1000  # floor: rounding never favours the bettor


def _row_to_bet(r, replayed=False) -> Bet:
    return Bet(r["id"], r["user_id"], r["market_id"], r["selection"], r["stake"],
               r["locked_odds_milli"], r["potential_payout"], r["status"], replayed)


def escrow_for(user_id: str, escrow_ids: list[int]) -> int:
    return escrow_ids[zlib.crc32(user_id.encode()) % len(escrow_ids)]


async def place_bet(conn: asyncpg.Connection, *, user_id: str, idempotency_key: str,
                    market_id: int, selection: str, stake: int,
                    expected_odds_milli: int | None, escrow_ids: list[int]) -> Bet:
    """Idempotent bet placement with odds locked at this instant.

    Lock order is always: market row (shared) -> bet unique-key slot -> user wallet -> escrow
    shard. Every txn takes at most one wallet and one escrow row, in that order, so there is no
    lock cycle (no deadlocks).
    """
    rh = request_hash(user_id, market_id, selection, stake, expected_odds_milli)

    async with conn.transaction():
        # 1. Price. Read inside the txn so the odds we lock are the odds we validated against.
        #    FOR SHARE blocks a concurrent odds update from committing between our read and
        #    our insert, so "locked odds" are exactly the odds in force at placement.
        m = await conn.fetchrow(
            "SELECT status, odds_milli, odds_version FROM markets WHERE id=$1 FOR SHARE", market_id)
        if m is None:
            raise NotFound(f"market {market_id}")
        if m["status"] != "open":
            raise MarketClosed(m["status"])
        odds_map = m["odds_milli"]
        if isinstance(odds_map, str):
            odds_map = json.loads(odds_map)
        odds = odds_map.get(selection)
        if odds is None:
            raise NotFound(f"selection {selection}")
        if expected_odds_milli is not None and expected_odds_milli != odds:
            raise OddsChanged(f"current={odds} expected={expected_odds_milli}")

        # 2. Idempotency slot. The UNIQUE(user_id, idempotency_key) index serialises concurrent
        #    retries: the second inserter waits for the first to commit, then sees a conflict.
        payout = payout_for(stake, odds)
        row = await conn.fetchrow(
            """INSERT INTO bets (user_id, idempotency_key, request_hash, market_id, selection,
                                 stake, locked_odds_milli, odds_version, potential_payout)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
               ON CONFLICT (user_id, idempotency_key) DO NOTHING
               RETURNING id""",
            user_id, idempotency_key, rh, market_id, selection, stake, odds, m["odds_version"], payout)
        if row is None:
            prior = await conn.fetchrow(
                "SELECT * FROM bets WHERE user_id=$1 AND idempotency_key=$2", user_id, idempotency_key)
            if prior["request_hash"] != rh:
                raise IdempotencyMismatch()
            return _row_to_bet(prior, replayed=True)   # original locked odds, original result
        bet_id = row["id"]

        # 3. Debit. One atomic conditional UPDATE: the balance check and the decrement cannot be
        #    separated by another txn, so two concurrent bets can never both spend the same money.
        w = await conn.fetchrow(
            """UPDATE accounts SET balance = balance - $2
               WHERE kind='wallet' AND owner=$1 AND balance >= $2
               RETURNING id""", user_id, stake)
        if w is None:
            exists = await conn.fetchval("SELECT 1 FROM accounts WHERE kind='wallet' AND owner=$1", user_id)
            if not exists:
                raise NotFound(f"wallet {user_id}")
            raise InsufficientFunds()   # raising rolls back the bet row too -> key stays reusable

        escrow_id = escrow_for(user_id, escrow_ids)
        await conn.execute("UPDATE accounts SET balance = balance + $2 WHERE id=$1", escrow_id, stake)

        # 4. Double-entry: two entries that sum to zero.
        txn_id = await conn.fetchval(
            "INSERT INTO ledger_txns (kind, bet_id) VALUES ('bet_stake',$1) RETURNING id", bet_id)
        await conn.execute(
            "INSERT INTO ledger_entries (txn_id, account_id, amount) VALUES ($1,$2,$3),($1,$4,$5)",
            txn_id, w["id"], -stake, escrow_id, stake)
        await conn.execute("UPDATE bets SET stake_txn_id=$2 WHERE id=$1", bet_id, txn_id)

    return Bet(bet_id, user_id, market_id, selection, stake, odds, payout, "open")


async def settle_bet(conn: asyncpg.Connection, *, bet_id: int, won: bool,
                     escrow_ids: list[int], house_id: int) -> Bet:
    """Settle once. The conditional status UPDATE makes double-settlement impossible."""
    async with conn.transaction():
        b = await conn.fetchrow(
            "UPDATE bets SET status=$2 WHERE id=$1 AND status='open' RETURNING *",
            bet_id, "won" if won else "lost")
        if b is None:
            prior = await conn.fetchrow("SELECT * FROM bets WHERE id=$1", bet_id)
            if prior is None:
                raise NotFound(f"bet {bet_id}")
            return _row_to_bet(prior, replayed=True)
        escrow_id = escrow_for(b["user_id"], escrow_ids)
        legs = [(escrow_id, -b["stake"])]            # escrow releases the stake either way
        if won:
            wallet_id = await conn.fetchval(
                "SELECT id FROM accounts WHERE kind='wallet' AND owner=$1", b["user_id"])
            legs += [(wallet_id, b["potential_payout"]), (house_id, b["stake"] - b["potential_payout"])]
        else:
            legs += [(house_id, b["stake"])]
        legs = [l for l in legs if l[1] != 0]
        for acct, amt in sorted(legs):               # fixed order by account id: deadlock-free
            await conn.execute("UPDATE accounts SET balance = balance + $2 WHERE id=$1", acct, amt)
        txn_id = await conn.fetchval(
            "INSERT INTO ledger_txns (kind, bet_id) VALUES ($1,$2) RETURNING id",
            "bet_payout" if won else "bet_release", bet_id)
        await conn.executemany(
            "INSERT INTO ledger_entries (txn_id, account_id, amount) VALUES ($1,$2,$3)",
            [(txn_id, a, amt) for a, amt in legs])
        return _row_to_bet(b)


async def deposit(conn: asyncpg.Connection, *, user_id: str, amount: int, funding_id: int):
    async with conn.transaction():
        w = await conn.fetchrow(
            "UPDATE accounts SET balance = balance + $2 WHERE kind='wallet' AND owner=$1 RETURNING id",
            user_id, amount)
        if w is None:
            raise NotFound(f"wallet {user_id}")
        await conn.execute("UPDATE accounts SET balance = balance - $2 WHERE id=$1", funding_id, amount)
        t = await conn.fetchval("INSERT INTO ledger_txns (kind) VALUES ('deposit') RETURNING id")
        await conn.execute(
            "INSERT INTO ledger_entries (txn_id, account_id, amount) VALUES ($1,$2,$3),($1,$4,$5)",
            t, w["id"], amount, funding_id, -amount)
