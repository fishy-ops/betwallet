"""Combine k6's client-side numbers with server-side truth from Postgres into one verdict.

    report.py K6_SUMMARY.json [--out results/last_run.json]

Bets/sec comes from committed rows (bets.created_at), not from what the client thinks happened.
"""
import argparse
import asyncio
import json
import os
import pathlib
import sys

import asyncpg

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import audit  # noqa: E402

DSN = os.environ.get("DATABASE_URL", "postgresql:///betwallet")


async def server_side():
    c = await asyncpg.connect(DSN)
    try:
        rows = await c.fetch(
            "SELECT date_trunc('second', created_at) s, count(*) n FROM bets GROUP BY 1 ORDER BY 1")
        per_sec = [r["n"] for r in rows]
        total = sum(per_sec)
        peak = max(per_sec) if per_sec else 0
        # sustained: best contiguous 10-second window (the 10x hold is 20s)
        best10 = max((sum(per_sec[i:i + 10]) / 10 for i in range(max(1, len(per_sec) - 9))), default=0)
        return {"committed_bets": total, "peak_bets_per_sec": peak,
                "best_10s_avg_bets_per_sec": round(best10, 1), "active_seconds": len(per_sec)}
    finally:
        await c.close()


def metric(m, name, field):
    return m.get(name, {}).get(field, 0)


async def main(summary_path, out):
    k6 = json.loads(pathlib.Path(summary_path).read_text())["metrics"]
    srv = await server_side()
    audit_results, audit_stats = await audit.run()
    violations = {k: v["violations"] for k, v in audit_results.items()}
    audit_ok = not any(violations.values())

    created = metric(k6, "bets_created", "count")
    lat = {f: metric(k6, "bet_latency_accepted", f) for f in ("avg", "med", "p(95)", "p(99)", "max")}
    http_reqs = metric(k6, "http_reqs", "count")
    shed = metric(k6, "shed_503", "count")
    bad = {n: metric(k6, n, "count") for n in ("LOCKED_ODDS_MISMATCH", "REPLAY_RESULT_DIFFERS", "unexpected_status")}
    # every 201 the client saw must be a committed row; extra rows are bets whose response was lost
    lost_responses = srv["committed_bets"] - created

    result = {
        "bets_per_sec": srv, "latency_ms_accepted_bets": lat,
        "requests": {"total": http_reqs, "created_201": created, "replayed_200": metric(k6, "bets_replayed", "count"),
                     "shed_503": shed, "odds_changed_409": metric(k6, "rejected_odds_changed", "count"),
                     "insufficient_402": metric(k6, "rejected_insufficient_funds", "count")},
        "client_side_violations": bad, "bets_committed_but_response_lost": lost_responses,
        "audit": {"ok": audit_ok, "violations": violations, "stats": audit_stats},
    }
    verdict = audit_ok and not any(bad.values()) and lost_responses == 0
    result["verdict"] = "PASS" if verdict else "FAIL"

    print("\n================ 10x SPIKE REPORT ================")
    print(f"  committed bets          {srv['committed_bets']}   (client saw {created} x 201)")
    print(f"  peak bets/sec           {srv['peak_bets_per_sec']}")
    print(f"  best 10s avg bets/sec   {srv['best_10s_avg_bets_per_sec']}")
    print(f"  latency (accepted)      p50 {lat['med']:.1f}  p95 {lat['p(95)']:.1f}  p99 {lat['p(99)']:.1f}  max {lat['max']:.1f} ms")
    print(f"  load shed (503)         {shed}  ({100 * shed / max(1, http_reqs):.1f}% of {http_reqs} requests)")
    print(f"  replayed (idempotent)   {result['requests']['replayed_200']}")
    print(f"  odds_changed 409        {result['requests']['odds_changed_409']}   insufficient 402  {result['requests']['insufficient_402']}")
    print(f"  locked-odds mismatches  {bad['LOCKED_ODDS_MISMATCH']}   replay differs  {bad['REPLAY_RESULT_DIFFERS']}   unexpected status  {bad['unexpected_status']}")
    print(f"  lost responses          {lost_responses}")
    print("  ---- ledger audit ----")
    for name, v in violations.items():
        print(f"    {'PASS' if v == 0 else 'FAIL'}  {name}" + ("" if v == 0 else f"  ({v} violations)"))
    print(f"  VERDICT: {result['verdict']}  (double-spends / balance drift: {'ZERO' if audit_ok else 'DETECTED'})")
    print("==================================================")

    pathlib.Path(out).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(out).write_text(json.dumps(result, indent=2, default=int))
    return 0 if verdict else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("summary")
    ap.add_argument("--out", default="results/last_run.json")
    a = ap.parse_args()
    sys.exit(asyncio.run(main(a.summary, a.out)))
