"""HTTP layer. Thin: validate, take a pooled connection, call service, map errors.

Overload behaviour: if no DB connection frees up within ACQUIRE_TIMEOUT, we shed the request
with 503 + Retry-After instead of queueing unboundedly. Latency of *accepted* bets stays bounded
and no money state is ever touched by a shed request.
"""
import os

import asyncpg
import orjson
from fastapi import FastAPI, Header, Request
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field

from . import service

DSN = os.environ.get("DATABASE_URL", "postgresql:///betwallet")
POOL_MAX = int(os.environ.get("POOL_MAX", "12"))
ACQUIRE_TIMEOUT = float(os.environ.get("ACQUIRE_TIMEOUT", "0.25"))

app = FastAPI(default_response_class=ORJSONResponse)


class PlaceBet(BaseModel):
    user_id: str = Field(min_length=1, max_length=64)
    market_id: int
    selection: str = Field(min_length=1, max_length=32)
    stake: int = Field(gt=0, le=10_000_000)           # cents
    expected_odds_milli: int | None = None            # reject instead of silently re-pricing


class Settle(BaseModel):
    won: bool


class Deposit(BaseModel):
    user_id: str
    amount: int = Field(gt=0, le=100_000_000)


async def _init_conn(conn):
    await conn.set_type_codec("jsonb", encoder=lambda v: orjson.dumps(v).decode(),
                              decoder=orjson.loads, schema="pg_catalog")


@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(
        DSN, min_size=POOL_MAX, max_size=POOL_MAX, init=_init_conn, command_timeout=10)
    async with app.state.pool.acquire() as c:
        rows = await c.fetch("SELECT id, kind FROM accounts WHERE kind IN ('escrow','house','funding') ORDER BY id")
    app.state.escrow_ids = [r["id"] for r in rows if r["kind"] == "escrow"]
    app.state.house_id = next(r["id"] for r in rows if r["kind"] == "house")
    app.state.funding_id = next(r["id"] for r in rows if r["kind"] == "funding")


@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()


def _err(status, code, detail="", headers=None):
    return ORJSONResponse({"error": code, "detail": detail}, status_code=status, headers=headers)


async def _conn(request: Request):
    """Returns (conn, None) or (None, 503 response)."""
    try:
        return await request.app.state.pool.acquire(timeout=ACQUIRE_TIMEOUT), None
    except TimeoutError:
        return None, _err(503, "overloaded", "retry with the same Idempotency-Key", {"Retry-After": "1"})


@app.post("/bets")
async def place_bet(body: PlaceBet, request: Request, idempotency_key: str = Header(min_length=8, max_length=80)):
    conn, shed = await _conn(request)
    if shed:
        return shed
    try:
        bet = await service.place_bet(
            conn, user_id=body.user_id, idempotency_key=idempotency_key, market_id=body.market_id,
            selection=body.selection, stake=body.stake, expected_odds_milli=body.expected_odds_milli,
            escrow_ids=request.app.state.escrow_ids)
        # 201 for a newly created bet, 200 for an idempotent replay of an earlier one
        return ORJSONResponse(bet.as_dict(), status_code=200 if bet.replayed else 201)
    except service.BetError as e:
        return _err(e.status, e.code, e.detail)
    except (asyncpg.exceptions.DeadlockDetectedError, asyncpg.exceptions.SerializationError):
        return _err(503, "conflict_retry", "retry with the same Idempotency-Key", {"Retry-After": "0"})
    finally:
        await request.app.state.pool.release(conn)


@app.post("/bets/{bet_id}/settle")
async def settle(bet_id: int, body: Settle, request: Request):
    conn, shed = await _conn(request)
    if shed:
        return shed
    try:
        bet = await service.settle_bet(conn, bet_id=bet_id, won=body.won,
                                       escrow_ids=request.app.state.escrow_ids,
                                       house_id=request.app.state.house_id)
        return bet.as_dict()
    except service.BetError as e:
        return _err(e.status, e.code, e.detail)
    finally:
        await request.app.state.pool.release(conn)


@app.post("/deposit")
async def deposit(body: Deposit, request: Request):
    conn, shed = await _conn(request)
    if shed:
        return shed
    try:
        await service.deposit(conn, user_id=body.user_id, amount=body.amount,
                              funding_id=request.app.state.funding_id)
        return {"ok": True}
    except service.BetError as e:
        return _err(e.status, e.code, e.detail)
    finally:
        await request.app.state.pool.release(conn)


@app.get("/wallet/{user_id}")
async def wallet(user_id: str, request: Request):
    bal = await request.app.state.pool.fetchval(
        "SELECT balance FROM accounts WHERE kind='wallet' AND owner=$1", user_id)
    if bal is None:
        return _err(404, "not_found", user_id)
    return {"user_id": user_id, "balance": bal}


@app.get("/healthz")
async def healthz():
    return {"ok": True}
