
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// Custom metrics
export let pointsCounter = new Counter('points');
export let ErrorCount = new Counter('errors');

// Get Target Host from environment variable
const targetHost = __ENV.TARGET_HOST || '10.10.50.14';
const targetPort = __ENV.TARGET_PORT || 5234;

// Requests per second - script uses constant-arrival-rate to strictly enforce this
const rps = __ENV.RPS ? parseInt(__ENV.RPS) : 100;
const duration = __ENV.DURATION || '30s';

// Use constant-arrival-rate executor to maintain exact RPS regardless of request latency
export const options = {
    scenarios: {
        constant_rate: {
            executor: 'constant-arrival-rate',
            rate: rps,
            timeUnit: '1s',
            duration: duration,
            preAllocatedVUs: Math.min(rps * 2, 200),
            maxVUs: Math.max(rps * 3, 200),
        },
    },
};

// Base variables
const url = `http://${targetHost}:${targetPort}/write?precision=ms&db=`;
const hostnames = ['host1', 'host2', 'host3'];
const cpus = ['cpu0', 'cpu1', 'cpu2'];
const databases = ['server'];
const measurements = ['cpu', 'memory', 'disk'];
const fields = ['idle', 'user', 'system'];
const points = __ENV.POINTS_PER_REQUEST ? parseInt(__ENV.POINTS_PER_REQUEST) : 10000;
let counter = 0;

/** line (default) | msgpack — matches server Content-Type handling */
const writeFormat = (__ENV.WRITE_FORMAT || 'line').toLowerCase();

// ─── Minimal MessagePack writer (matches hyperbytedb msgpack wire: serde map + externally tagged FieldValue) ───

function concatParts(parts) {
    let total = 0;
    for (const p of parts) {
        total += p.length;
    }
    const out = new Uint8Array(total);
    let off = 0;
    for (const p of parts) {
        out.set(p, off);
        off += p.length;
    }
    return out;
}

/** UTF-8 encode (k6/goja has no TextEncoder). */
function utf8Bytes(s) {
    const bytes = [];
    for (let i = 0; i < s.length; i++) {
        let c = s.charCodeAt(i);
        if (c < 0x80) {
            bytes.push(c);
        } else if (c < 0x800) {
            bytes.push(0xc0 | (c >> 6), 0x80 | (c & 0x3f));
        } else if (c < 0xd800 || c >= 0xe000) {
            bytes.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 0x3f), 0x80 | (c & 0x3f));
        } else {
            i++;
            const c2 = s.charCodeAt(i);
            const cp = 0x10000 + ((c & 0x3ff) << 10) + (c2 & 0x3ff);
            bytes.push(
                0xf0 | (cp >> 18),
                0x80 | ((cp >> 12) & 0x3f),
                0x80 | ((cp >> 6) & 0x3f),
                0x80 | (cp & 0x3f),
            );
        }
    }
    return new Uint8Array(bytes);
}

function encStr(s) {
    const utf8 = utf8Bytes(s);
    const len = utf8.length;
    if (len < 32) {
        const out = new Uint8Array(1 + len);
        out[0] = 0xa0 | len;
        out.set(utf8, 1);
        return out;
    }
    if (len < 256) {
        const out = new Uint8Array(2 + len);
        out[0] = 0xd9;
        out[1] = len;
        out.set(utf8, 2);
        return out;
    }
    const out = new Uint8Array(3 + len);
    out[0] = 0xda;
    out[1] = (len >> 8) & 0xff;
    out[2] = len & 0xff;
    out.set(utf8, 3);
    return out;
}

function encF64(f) {
    const b = new ArrayBuffer(9);
    const u = new Uint8Array(b);
    u[0] = 0xcb;
    new DataView(b).setFloat64(1, f, false);
    return u;
}

function encI64(n) {
    const u = new Uint8Array(9);
    u[0] = 0xd3;
    let x = Math.floor(n);
    for (let i = 8; i >= 1; i--) {
        u[i] = x & 0xff;
        x = Math.floor(x / 256);
    }
    return u;
}

/** BTreeMap tag order: cpu, hostname */
function encTagMap(hostname, cpu) {
    return concatParts([
        new Uint8Array([0x82]),
        encStr('cpu'),
        encStr(cpu),
        encStr('hostname'),
        encStr(hostname),
    ]);
}

/** fields: { fieldName: { Float: f64 } } */
function encFieldsMap(fieldName, value) {
    return concatParts([
        new Uint8Array([0x81]),
        encStr(fieldName),
        concatParts([new Uint8Array([0x81]), encStr('Float'), encF64(value)]),
    ]);
}

/** Struct field order: measurement, tags, fields, timestamp */
function encodeOnePoint(measurement, hostname, cpu, fieldName, tsMs, value) {
    return concatParts([
        new Uint8Array([0x84]),
        encStr('measurement'),
        encStr(measurement),
        encStr('tags'),
        encTagMap(hostname, cpu),
        encStr('fields'),
        encFieldsMap(fieldName, value),
        encStr('timestamp'),
        encI64(tsMs),
    ]);
}

function encodeMsgpackBatch(measurement, hostname, cpu, fieldName, tsMs, count) {
    const parts = [];
    if (count < 16) {
        parts.push(new Uint8Array([0x90 | count]));
    } else if (count < 65536) {
        parts.push(new Uint8Array([0xdc, (count >> 8) & 0xff, count & 0xff]));
    } else {
        throw new Error('POINTS_PER_REQUEST must be < 65536 for msgpack batch');
    }
    for (let i = 0; i < count; i++) {
        parts.push(encodeOnePoint(measurement, hostname, cpu, fieldName, tsMs, Math.random()));
    }
    return concatParts(parts);
}

export default function () {
    const database = databases[counter % databases.length];
    const hostname = hostnames[counter % hostnames.length];
    const cpu = cpus[counter % cpus.length];
    const measurement = measurements[counter % measurements.length];
    const field = fields[counter % fields.length];

    counter++;

    const urlWithDatabase = `${url}${database}`;
    const tsMs = Date.now();

    let payload;
    const params = { headers: {} };

    if (writeFormat === 'msgpack') {
        payload = encodeMsgpackBatch(measurement, hostname, cpu, field, tsMs, points);
        params.headers['Content-Type'] = 'application/msgpack';
    } else {
        let linePayload = '';
        for (let i = 0; i < points; i++) {
            const ts = Date.now();
            const value = Math.random();
            linePayload += `${measurement},hostname=${hostname},cpu=${cpu} ${field}=${value} ${ts}\n`;
        }
        payload = linePayload;
        params.headers['Content-Type'] = 'text/plain';
    }

    const res = http.post(urlWithDatabase, payload, params);

    const ok = check(res, {
        'status is 200': (r) => r.status === 200 || r.status === 204,
    });

    if (!ok) {
        ErrorCount.add(1);
    } else {
        pointsCounter.add(points);
    }
}
