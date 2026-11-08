// 10x traffic spike against POST /bets.
//   BASE_RATE  = baseline bets/sec (default 100)  -> spike peaks at 10 x BASE_RATE
//   Phases: warm 15s @1x -> ramp 5s to 10x -> hold 20s @10x -> drop 5s -> recover 15s @1x
//
// Traffic mix (what makes this a hard test, not a happy-path benchmark):
//   - 12% of requests are RETRIES of an earlier key (same Idempotency-Key + payload): must be
//     replayed, never double-charged.
//   - 8% of users are "whales" hammering one wallet with many parallel bets: the double-spend case.
//   - clients send expected_odds_milli from a possibly stale view; the odds ticker is moving prices,
//     so some get 409 odds_changed, and every 201 must carry exactly the odds that was expected.
import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:8000';
const RATE = parseInt(__ENV.BASE_RATE || '100');
const USERS = parseInt(__ENV.USERS || '2000');
const MARKETS = parseInt(__ENV.MARKETS || '20');

const created = new Counter('bets_created');          // 201
const replayed = new Counter('bets_replayed');        // 200 on an idempotent retry
const oddsChanged = new Counter('rejected_odds_changed');
const insufficient = new Counter('rejected_insufficient_funds');
const shed = new Counter('shed_503');
const unexpected = new Counter('unexpected_status');
const oddsMismatch = new Counter('LOCKED_ODDS_MISMATCH');   // must stay 0
const replayDiffers = new Counter('REPLAY_RESULT_DIFFERS'); // must stay 0
const latAccepted = new Trend('bet_latency_accepted', true);

export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-arrival-rate',
      startRate: RATE, timeUnit: '1s',
      preAllocatedVUs: 200, maxVUs: 2000,
      stages: [
        { target: RATE, duration: '15s' },
        { target: RATE * 10, duration: '5s' },
        { target: RATE * 10, duration: '20s' },
        { target: RATE, duration: '5s' },
        { target: RATE, duration: '15s' },
      ],
    },
  },
  thresholds: {
    'LOCKED_ODDS_MISMATCH': ['count==0'],
    'REPLAY_RESULT_DIFFERS': ['count==0'],
    'unexpected_status': ['count==0'],
    'bet_latency_accepted': ['p(99)<500'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const markets = {};   // market-selection -> last odds this VU saw (stale on purpose)
const sent = [];      // per-VU ring of {key, body, resp} for retries

function pick(n) { return Math.floor(Math.random() * n); }

export default function () {
  const iter = exec.scenario.iterationInTest;
  const headers = { 'Content-Type': 'application/json' };

  // 12%: retry an earlier request verbatim
  if (sent.length > 20 && Math.random() < 0.12) {
    const prev = sent[pick(sent.length)];
    const r = http.post(`${BASE}/bets`, prev.body,
      { headers: { ...headers, 'Idempotency-Key': prev.key }, tags: { kind: 'retry' } });
    if (r.status === 200 || r.status === 201) {
      replayed.add(1);
      const j = r.json();
      if (prev.resp && (j.bet_id !== prev.resp.bet_id || j.locked_odds_milli !== prev.resp.locked_odds_milli)) replayDiffers.add(1);
    } else if (r.status === 503) shed.add(1);
    else if (![402, 409].includes(r.status)) unexpected.add(1);
    return;
  }

  const whale = Math.random() < 0.08;
  const user = whale ? `u${pick(10)}` : `u${pick(USERS)}`;
  const mid = 1 + pick(MARKETS);
  const sel = Math.random() < 0.5 ? 'home' : 'away';
  const stake = whale ? 5000 + pick(5000) : 100 + pick(900);
  const key = `k6-${iter}-${Math.random().toString(36).slice(2, 10)}`;
  const mk = `${mid}-${sel}`;
  const body = { user_id: user, market_id: mid, selection: sel, stake };
  if (markets[mk] !== undefined) body.expected_odds_milli = markets[mk];
  const payload = JSON.stringify(body);

  const r = http.post(`${BASE}/bets`, payload,
    { headers: { ...headers, 'Idempotency-Key': key }, tags: { kind: 'new' } });

  if (r.status === 201) {
    created.add(1);
    latAccepted.add(r.timings.duration);
    const j = r.json();
    if (body.expected_odds_milli !== undefined && j.locked_odds_milli !== body.expected_odds_milli) oddsMismatch.add(1);
    markets[mk] = j.locked_odds_milli;
    if (sent.length < 200) sent.push({ key, body: payload, resp: j }); else sent[pick(200)] = { key, body: payload, resp: j };
  } else if (r.status === 409) {
    oddsChanged.add(1);
    markets[mk] = undefined;       // refresh: next request is priced by the server
  } else if (r.status === 402) {
    insufficient.add(1);
  } else if (r.status === 503) {
    shed.add(1);
  } else {
    unexpected.add(1);
  }
}
