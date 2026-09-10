import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('errors');
const enqueueTrend = new Trend('enqueue_duration');

export const options = {
  stages: [
    { duration: __ENV.RAMP_UP || '10s', target: Number(__ENV.VUS_LOW || 50) },
    { duration: __ENV.HOLD || '30s', target: Number(__ENV.VUS_HIGH || 100) },
    { duration: __ENV.RAMP_DOWN || '10s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<50'],
    errors: ['rate<0.01'],
  },
  // Default k6 output stops at p95; the README quotes p50 and p99.
  summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';

// TASK=k6-load-test (default) has no registered handler, so every job
// dead-letters immediately. That isolates intake and dispatch cost.
// TASK=webhook with WEBHOOK_URL set runs a real job through to COMPLETED.
const TASK = __ENV.TASK || 'k6-load-test';
const WEBHOOK_URL = __ENV.WEBHOOK_URL || '';

export default function () {
  const task = {
    name: TASK,
    queue: 'default',
    max_retries: 3,
  };
  if (TASK === 'webhook') {
    task.metadata = { url: WEBHOOK_URL };
  }

  const payload = JSON.stringify({ task });
  const params = { headers: { 'Content-Type': 'application/json' } };
  const res = http.post(`${BASE_URL}/api/jobs`, payload, params);

  const ok = check(res, {
    'status is 201': (r) => r.status === 201,
    'has job id': (r) => r.json('id') !== '',
  });

  errorRate.add(!ok);
  enqueueTrend.add(res.timings.duration);

  sleep(0.01);
}
