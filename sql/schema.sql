-- Bet placement + wallet: double-entry ledger.
-- All money is integer cents. Odds are integer "milli-odds" (2500 = 2.500x decimal odds).

DROP TABLE IF EXISTS market_odds_history, ledger_entries, ledger_txns, bets, markets, accounts CASCADE;

-- kind: 'wallet' (a user), 'escrow' (house holding for open bets, sharded to avoid a hot row),
--       'house' (house bankroll that funds payouts), 'funding' (counter-account for deposits)
CREATE TABLE accounts (
    id          BIGSERIAL PRIMARY KEY,
    kind        TEXT   NOT NULL CHECK (kind IN ('wallet','escrow','house','funding')),
    owner       TEXT,                        -- user id for wallets, shard label otherwise
    -- Cached balance, maintained in the same txn as the entries. The ledger is the source of
    -- truth; audit.py proves cache == SUM(entries). Wallets can never go negative (CHECK below),
    -- which is the double-spend backstop even if application logic were wrong.
    balance     BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT wallet_non_negative CHECK (kind <> 'wallet' OR balance >= 0),
    CONSTRAINT escrow_non_negative CHECK (kind <> 'escrow' OR balance >= 0)
);
CREATE UNIQUE INDEX accounts_wallet_owner ON accounts(owner) WHERE kind = 'wallet';

CREATE TABLE markets (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT   NOT NULL,
    status      TEXT   NOT NULL DEFAULT 'open' CHECK (status IN ('open','suspended','settled')),
    -- current price per selection: {"home": 1900, "away": 2100}
    odds_milli  JSONB  NOT NULL,
    odds_version BIGINT NOT NULL DEFAULT 1
);

-- Every price a market has ever had. Lets audit.py prove each bet's locked odds were the real
-- price at the version it recorded.
CREATE TABLE market_odds_history (
    market_id   BIGINT NOT NULL REFERENCES markets(id),
    version     BIGINT NOT NULL,
    odds_milli  JSONB  NOT NULL,
    PRIMARY KEY (market_id, version)
);

CREATE TABLE ledger_txns (
    id          BIGSERIAL PRIMARY KEY,
    kind        TEXT   NOT NULL CHECK (kind IN ('deposit','bet_stake','bet_payout','bet_release')),
    bet_id      BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- amount > 0 credits the account, amount < 0 debits it. Append-only.
CREATE TABLE ledger_entries (
    id          BIGSERIAL PRIMARY KEY,
    txn_id      BIGINT NOT NULL REFERENCES ledger_txns(id),
    account_id  BIGINT NOT NULL REFERENCES accounts(id),
    amount      BIGINT NOT NULL CHECK (amount <> 0)
);
CREATE INDEX ledger_entries_account ON ledger_entries(account_id);
CREATE INDEX ledger_entries_txn ON ledger_entries(txn_id);

CREATE TABLE bets (
    id               BIGSERIAL PRIMARY KEY,
    user_id          TEXT   NOT NULL,
    idempotency_key  TEXT   NOT NULL,
    request_hash     TEXT   NOT NULL,           -- detects key reuse with a different payload
    market_id        BIGINT NOT NULL REFERENCES markets(id),
    selection        TEXT   NOT NULL,
    stake            BIGINT NOT NULL CHECK (stake > 0),
    locked_odds_milli INT   NOT NULL CHECK (locked_odds_milli > 1000),  -- price at placement
    odds_version     BIGINT NOT NULL,
    potential_payout BIGINT NOT NULL,
    status           TEXT   NOT NULL DEFAULT 'open' CHECK (status IN ('open','won','lost','void')),
    stake_txn_id     BIGINT REFERENCES ledger_txns(id),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, idempotency_key)
);
CREATE INDEX bets_market ON bets(market_id, status);
