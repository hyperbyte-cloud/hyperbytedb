import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const targetHost = __ENV.TARGET_HOST || '127.0.0.1';
const targetPort = __ENV.TARGET_PORT || '18080';
const db = __ENV.DB || 'bench';
const baseUrl = `http://${targetHost}:${targetPort}`;

const countLatency = new Trend('query_count_latency', true);
const meanByHostLatency = new Trend('query_mean_by_host_latency', true);
const tagValuesLatency = new Trend('query_tag_values_latency', true);
const pointLookupLatency = new Trend('query_point_lookup_latency', true);
const errorCount = new Counter('query_errors');

export const options = {
  vus: 1,
  iterations: __ENV.QUERY_ITERATIONS ? parseInt(__ENV.QUERY_ITERATIONS) : 50,
};

const queries = [
  { name: 'count(value)', q: 'SELECT count(value) FROM metrics', trend: countLatency },
  { name: 'mean by host', q: 'SELECT mean(value) FROM metrics GROUP BY host', trend: meanByHostLatency },
  { name: 'tag values host', q: 'SHOW TAG VALUES FROM metrics WITH KEY = host', trend: tagValuesLatency },
  { name: 'point lookup', q: "SELECT value FROM metrics WHERE host = 's50000'", trend: pointLookupLatency },
];

export default function () {
  for (const entry of queries) {
    const url = `${baseUrl}/query?db=${db}&q=${encodeURIComponent(entry.q)}`;
    const res = http.get(url, { tags: { query_name: entry.name } });
    const ok = check(res, { [`${entry.name} 200`]: (r) => r.status === 200 });
    if (!ok) {
      errorCount.add(1);
      console.warn(`FAIL [${res.status}] ${entry.name}`);
    }
    entry.trend.add(res.timings.duration);
  }
}
