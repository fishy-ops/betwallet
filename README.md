# betwallet

Bet placement + wallet service. Double-entry ledger, idempotent bets, odds locked at the moment
of placement, and a load test that proves no double-spends or balance drift under a 10x traffic spike.

Stack: Python, FastAPI, asyncpg, Postgres. Load testing with k6.

## How it stays correct

- **Double-entry ledger.** Every money movement is a transaction of append-only entries that sum to zero.
  Account balances are a cache, proven equal to the sum of entries by the auditor.
- **No double-spend.** The debit is one atomic `UPDATE ... WHERE balance >= stake`, with a
  `CHECK (balance >= 0)` on wallets as a backstop.
- **Idempotent bets.** `UNIQUE (user_id, idempotency_key)` serialises concurrent retries; a replay returns
  the original bet and original odds. Reusing a key with a different payload is a 422.
  A rejected bet (e.g. insufficient funds) rolls back and does not burn the key.
- **Odds locked at placement.** The market row is read `FOR SHARE` in the same transaction as the bet insert and
  copied onto the bet with its `odds_version`. Clients may send `expected_odds_milli`; a stale price gets a 409
  instead of a silent re-price.
- **Hot-row avoidance.** House escrow is sharded across 16 accounts.
- **Overload.** When the connection pool is saturated the API sheds with `503 + Retry-After` before touching any
  money state; clients retry with the same key.

## Run

```
python3 -m venv .venv && .venv/bin/pip install fastapi 'uvicorn[standard]' asyncpg orjson httpx pytest pytest-asyncio
createdb betwallet
.venv/bin/python -m pytest                 # concurrency tests (200 racing bets on 10 bets of money, etc.)
scripts/run_spike.sh                       # seed, serve, move odds, 10x spike with k6, audit, report
BASE_RATE=800 scripts/run_spike.sh         # harder: peaks at 8,000 req/s
```

Requires Postgres 16 and k6. `DATABASE_URL` defaults to `postgresql:///betwallet`.

## API

| Route | Purpose |
|---|---|
| `POST /bets` (header `Idempotency-Key`) | place a bet; 201 new, 200 replay |
| `POST /bets/{id}/settle` | settle won/lost, exactly once |
| `POST /deposit` | credit a wallet |
| `GET /wallet/{user_id}` | balance |

## Spike results (single laptop, k6 and server sharing the machine)

The spike is 1x for 15s, ramp to 10x, hold 20s, back to 1x. Traffic includes 12% verbatim retries, whale users
racing one wallet, and an odds ticker moving prices during the run.

| Baseline | Peak offered | Peak bets/sec (committed) | p99 (accepted bets) | Shed (503) | Audit |
|---|---|---|---|---|---|
| 100/s | 1,000/s | 682 | 6.1 ms | 0 | all pass |
| 300/s | 3,000/s | 1,608 | 6.7 ms | 0 | all pass |
| 800/s | 8,000/s | 4,913 | 88 ms | 14 of 227k | all pass |

Bets/sec is measured server-side from committed rows. Peak bets/sec is below the offered rate because part of the
traffic is retries (replayed, not new), stale-odds rejections (409) and out-of-funds rejections (402).

`scripts/audit.py` checks nine invariants from the database alone: balanced transactions, no balance drift,
money conserved, no negative wallets, exactly one debit per bet, escrow equals open stakes, locked odds match the
recorded market price, payout math, and settled at most once.

Not included: authentication, real payment rails, market suspension logic.
