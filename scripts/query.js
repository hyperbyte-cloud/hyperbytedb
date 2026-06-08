
import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const targetHost = __ENV.TARGET_HOST || '127.0.0.1';
const targetPort = __ENV.TARGET_PORT || 8086;
const db = 'telegraf';
const baseUrl = `http://${targetHost}:${targetPort}`;

// Per-query-type latency trends
const metadataLatency    = new Trend('query_metadata_latency', true);
const pointLatency       = new Trend('query_point_latency', true);
const aggregateLatency   = new Trend('query_aggregate_latency', true);
const filteredLatency    = new Trend('query_filtered_latency', true);
const errorCount         = new Counter('query_errors');

export const options = {
    vus: 1,
    iterations: __ENV.QUERY_ITERATIONS ? parseInt(__ENV.QUERY_ITERATIONS) : 30,
    thresholds: {
        query_metadata_latency:  ['p(95)<2000'],
        query_point_latency:     ['p(95)<5000'],
        query_aggregate_latency: ['p(95)<5000'],
        query_filtered_latency:  ['p(95)<5000'],
    },
};

const queries = [
    // ── Metadata (fast, hits RocksDB only) ──────────────────────────────
    { name: 'SHOW DATABASES',                    q: 'SHOW DATABASES',                                          bucket: 'metadata' },
    { name: 'SHOW MEASUREMENTS',                 q: 'SHOW MEASUREMENTS',                                      bucket: 'metadata' },
    { name: 'SHOW TAG KEYS (cpu)',               q: 'SHOW TAG KEYS FROM cpu',                                  bucket: 'metadata' },
    { name: 'SHOW TAG VALUES (hostname)',        q: 'SHOW TAG VALUES FROM cpu WITH KEY = "hostname"',          bucket: 'metadata' },
    { name: 'SHOW FIELD KEYS (cpu)',             q: 'SHOW FIELD KEYS FROM cpu',                                bucket: 'metadata' },
    { name: 'SHOW SERIES',                       q: 'SHOW SERIES',                                             bucket: 'metadata' },

    // ── Point reads (scan parquet via chDB) ─────────────────────────────
    { name: 'SELECT * FROM cpu LIMIT 10',        q: 'SELECT * FROM cpu LIMIT 10',                              bucket: 'point' },
    { name: 'SELECT * FROM memory LIMIT 10',     q: 'SELECT * FROM memory LIMIT 10',                           bucket: 'point' },
    { name: 'SELECT * FROM disk LIMIT 10',       q: 'SELECT * FROM disk LIMIT 10',                             bucket: 'point' },
    { name: 'SELECT * FROM cpu LIMIT 1000',      q: 'SELECT * FROM cpu LIMIT 1000',                            bucket: 'point' },

    // ── Aggregates ──────────────────────────────────────────────────────
    { name: 'COUNT(*) FROM cpu',                 q: 'SELECT COUNT(idle) FROM cpu',                              bucket: 'aggregate' },
    { name: 'MEAN(idle) FROM cpu',               q: 'SELECT MEAN(idle) FROM cpu',                               bucket: 'aggregate' },
    { name: 'MEAN(idle) GROUP BY hostname',      q: 'SELECT MEAN(idle) FROM cpu GROUP BY hostname',             bucket: 'aggregate' },
    { name: 'MEAN(idle) GROUP BY time(1h)',      q: 'SELECT MEAN(idle) FROM cpu GROUP BY time(1h)',             bucket: 'aggregate' },

    // ── Filtered / narrower scope ───────────────────────────────────────
    { name: 'cpu WHERE hostname=host1 LIMIT 10', q: "SELECT * FROM cpu WHERE hostname = 'host1' LIMIT 10",     bucket: 'filtered' },
    { name: 'cpu WHERE cpu=cpu0 LIMIT 10',       q: "SELECT * FROM cpu WHERE cpu = 'cpu0' LIMIT 10",           bucket: 'filtered' },
    { name: 'memory WHERE hostname=host2',       q: "SELECT * FROM memory WHERE hostname = 'host2' LIMIT 10",  bucket: 'filtered' },
];

export default function () {
    for (const entry of queries) {
        const url = `${baseUrl}/query?db=${db}&q=${encodeURIComponent(entry.q)}`;
        const res = http.get(url, { tags: { query_name: entry.name } });

        const ok = check(res, {
            [`${entry.name} status 200`]: (r) => r.status === 200,
        });

        if (!ok) {
            errorCount.add(1);
            console.warn(`FAIL [${res.status}] ${entry.name}: ${res.body}`);
        }

        const trend = {
            metadata:  metadataLatency,
            point:     pointLatency,
            aggregate: aggregateLatency,
            filtered:  filteredLatency,
        }[entry.bucket];
        trend.add(res.timings.duration);
    }
}
