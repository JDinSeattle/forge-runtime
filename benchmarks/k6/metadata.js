import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// One request per iteration; iteration index makes the 90/10 mix deterministic.
// Only built-in modules are used. The Go fixture supplies ephemeral loopback
// credentials through the child environment, never through output or argv.
const baseURL = __ENV.FORGE_K6_URL;
if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(baseURL)) {
  throw new Error('The evidence fixture requires a loopback API');
}
const reads = new Counter('metadata_reads');
const writes = new Counter('metadata_admissions');
export const options = {
  scenarios: {
    metadata: { executor: 'constant-arrival-rate', rate: 50, timeUnit: '1s', duration: '20s', preAllocatedVUs: 16, maxVUs: 16, gracefulStop: '5s' },
  },
  thresholds: {
    http_req_failed: ['rate==0'],
    checks: ['rate==1'],
    dropped_iterations: ['count==0'],
    http_reqs: ['count==1000'],
    metadata_reads: ['count==900'],
    metadata_admissions: ['count==100'],
    http_req_duration: ['p(95)<200', 'p(99)<1000'],
  },
  systemTags: ['method', 'status', 'name', 'scenario', 'expected_response'],
  summaryTrendStats: ['min', 'med', 'max', 'p(95)', 'p(99)'],
};

export default function () {
  const index = exec.scenario.iterationInTest;
  // The arrival executor may schedule its boundary tick at exactly 20s.
  // Limit dispatch itself to the declared 1,000 requests; retain every actual
  // sample and the executor's full iteration count in the raw output.
  if (index >= 1000) return;
  const write = index % 10 === 0;
  const headers = { Authorization: `Bearer ${__ENV.FORGE_K6_TOKEN}`, 'X-Forge-Tenant': 'http-tenant' };
  let response;
  if (write) {
    headers['Content-Type'] = 'application/json';
    headers['Idempotency-Key'] = `k6-metadata-${index}`;
    const body = JSON.stringify({ task: 'k6 metadata admission fixture', base_commit: __ENV.FORGE_K6_BASE, config_id: 'fixture', budget: {} });
    response = http.post(`${baseURL}/v1/projects/${__ENV.FORGE_K6_PROJECT}/runs`, body, { headers, tags: { name: 'metadata_admission' }, redirects: 0, timeout: '3s' });
    writes.add(1);
  } else {
    response = http.get(`${baseURL}/v1/runs/${__ENV.FORGE_K6_RUN}`, { headers, tags: { name: 'metadata_read' }, redirects: 0, timeout: '3s' });
    reads.add(1);
  }
  check(response, { 'expected metadata status': r => r.status === (write ? 202 : 200) });
}
