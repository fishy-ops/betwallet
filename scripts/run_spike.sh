#!/usr/bin/env bash
# End-to-end 10x spike: reset+seed -> start service -> move odds -> k6 spike -> audit ledger -> report.
#   BASE_RATE=100 WORKERS=4 scripts/run_spike.sh
set -euo pipefail
cd "$(dirname "$0")/.."

BASE_RATE="${BASE_RATE:-100}"
WORKERS="${WORKERS:-4}"
PORT="${PORT:-8000}"
PY=.venv/bin/python
mkdir -p results

echo "[1/5] reset + seed"
$PY scripts/seed.py --users 2000 --balance 100000 --markets 20

echo "[2/5] start service ($WORKERS workers)"
.venv/bin/uvicorn app.main:app --port "$PORT" --workers "$WORKERS" --log-level warning --no-access-log &
SERVER=$!
trap 'kill $SERVER ${TICKER:-} 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do curl -fs "http://127.0.0.1:$PORT/healthz" >/dev/null && break; sleep 0.2; done

echo "[3/5] odds ticker (prices move during the test)"
$PY scripts/odds_ticker.py --interval 0.05 --seconds 75 &
TICKER=$!

echo "[4/5] k6: ${BASE_RATE}/s -> $((BASE_RATE * 10))/s spike"
set +e
k6 run --quiet -e BASE_URL="http://127.0.0.1:$PORT" -e BASE_RATE="$BASE_RATE" \
   --summary-export results/k6_summary.json k6/spike.js
K6_RC=$?
set -e
kill "$TICKER" 2>/dev/null || true

echo "[5/5] audit + report"
set +e
$PY scripts/report.py results/k6_summary.json --out results/last_run.json
REPORT_RC=$?
set -e
echo "k6 thresholds exit=$K6_RC  report exit=$REPORT_RC"
exit $(( K6_RC != 0 || REPORT_RC != 0 ))
