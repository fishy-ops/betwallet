"""Ledger auditor. Proves, from the database alone, that no money was created, destroyed or
spent twice. Exit code 0 only if every invariant holds.

    audit.py [--json]
"""
import argparse
import asyncio
import json
import os
import sys

import asyncpg

DSN = os.environ.get("DATABASE_URL", "postgresql:///betwallet")

# name -> (description, SQL returning the number of VIOLATIONS)
CHECKS = {
    "txns_balanced": (
        "every ledger txn's entries sum to zero (double-entry)",
        "SELECT count(*) FROM (SELECT 1 FROM ledger_entries GROUP BY txn_id HAVING sum(amount) <> 0) x"),
    "no_balance_drift": (
        "every account's cached balance == SUM(its ledger entries)",
        """SELECT count(*) FROM accounts a
           LEFT JOIN (SELECT account_id, sum(amount) s FROM ledger_entries GROUP BY account_id) e
             ON e.account_id = a.id
           WHERE a.balance <> COALESCE(e.s, 0)"""),
    "money_conserved": (
        "sum of all account balances == 0 (nothing minted or burned)",
        "SELECT CASE WHEN COALESCE(sum(balance),0) = 0 THEN 0 ELSE 1 END FROM accounts"),
    "no_negative_wallets": (
        "no wallet below zero (no overdraft / double-spend)",
        "SELECT count(*) FROM accounts WHERE kind='wallet' AND balance < 0"),
    "one_stake_per_bet": (
        "every bet was debited exactly once, for exactly its stake",
        """SELECT count(*) FROM bets b
           LEFT JOIN (SELECT t.bet_id, count(DISTINCT t.id) n,
                             sum(CASE WHEN e.amount < 0 THEN -e.amount ELSE 0 END) debited
                      FROM ledger_txns t JOIN ledger_entries e ON e.txn_id = t.id
                      WHERE t.kind = 'bet_stake' GROUP BY t.bet_id) s ON s.bet_id = b.id
           WHERE s.n IS DISTINCT FROM 1 OR s.debited IS DISTINCT FROM b.stake"""),
    "escrow_equals_open_stakes": (
        "escrow holds exactly the stakes of open bets",
        """SELECT CASE WHEN (SELECT COALESCE(sum(balance),0) FROM accounts WHERE kind='escrow')
                         = (SELECT COALESCE(sum(stake),0) FROM bets WHERE status='open')
                    THEN 0 ELSE 1 END"""),
    "odds_were_real_and_locked": (
        "each bet's locked odds == the market price at the odds_version it recorded",
        """SELECT count(*) FROM bets b
           LEFT JOIN market_odds_history h ON h.market_id = b.market_id AND h.version = b.odds_version
           WHERE (h.odds_milli ->> b.selection)::int IS DISTINCT FROM b.locked_odds_milli"""),
    "payout_math": (
        "potential_payout == floor(stake * locked_odds)",
        "SELECT count(*) FROM bets WHERE potential_payout <> stake * locked_odds_milli / 1000"),
    "settled_once": (
        "no bet has more than one settlement txn",
        """SELECT count(*) FROM (SELECT bet_id FROM ledger_txns WHERE kind IN ('bet_payout','bet_release')
                               GROUP BY bet_id HAVING count(*) > 1) x"""),
}


async def run(dsn=DSN):
    c = await asyncpg.connect(dsn)
    try:
        results = {}
        for name, (desc, sql) in CHECKS.items():
            results[name] = {"desc": desc, "violations": await c.fetchval(sql)}
        stats = dict(await c.fetchrow(
            """SELECT (SELECT count(*) FROM bets) bets, (SELECT count(*) FROM ledger_entries) entries,
                      (SELECT count(*) FROM accounts WHERE kind='wallet') wallets,
                      (SELECT COALESCE(sum(stake),0) FROM bets) total_staked"""))
    finally:
        await c.close()
    return results, stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    results, stats = asyncio.run(run())
    ok = all(r["violations"] == 0 for r in results.values())
    if args.json:
        print(json.dumps({"ok": ok, "stats": stats, "checks": results}))
    else:
        for name, r in results.items():
            print(f"  {'PASS' if r['violations'] == 0 else 'FAIL'}  {name:28s} {r['desc']}"
                  + ("" if r["violations"] == 0 else f"   <-- {r['violations']} violations"))
        print(f"  [{stats['bets']} bets, {stats['entries']} ledger entries, {stats['wallets']} wallets, "
              f"{stats['total_staked']} cents staked]")
        print("AUDIT " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
