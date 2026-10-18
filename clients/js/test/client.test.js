import assert from "node:assert/strict";
import { createServer } from "node:http";
import { after, before, beforeEach, describe, test } from "node:test";
import { gunzipSync } from "node:zlib";
import { AuthError, HyperbyteDBClient, HyperbyteDBError, QueryError, ServerError, WriteError } from "../dist/index.js";

// In a suite: on Node 18 a top-level after() runs only once the event loop
// is empty, which the fake server never lets it be.
describe("client", () => {
  // Answers with the scripted replies in turn (then 204) and records requests.
  let script = [];
  let requests = [];
  let server;
  let url;

  before(async () => {
    server = createServer((req, res) => {
      const chunks = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        let body = Buffer.concat(chunks);
        if (req.headers["content-encoding"] === "gzip") body = gunzipSync(body);
        const u = new URL(req.url, "http://x");
        requests.push({ method: req.method, path: u.pathname, query: u.searchParams, headers: req.headers, body: body.toString() });
        const [status, headers, payload] = script.shift() ?? [204, {}, ""];
        res.writeHead(status, headers);
        res.end(payload);
      });
    });
    await new Promise((r) => server.listen(0, "127.0.0.1", r));
    url = `http://127.0.0.1:${server.address().port}`;
  });
  after(() => {
    server.closeAllConnections();
    server.close();
  });
  beforeEach(() => {
    script = [];
    requests = [];
  });

  const client = (opts = {}) => new HyperbyteDBClient(url, { backoff: 10, ...opts });

  test("writes are batched, gzipped and authenticated", async () => {
    const points = [0, 1, 2, 3, 4].map((i) => ({ measurement: "m", tags: { h: "a" }, fields: { v: i + 0.5 }, time: i }));
    assert.equal(await client({ username: "u", password: "p", batchPoints: 2 }).write(points, "mydb", { precision: "s" }), 5);
    assert.equal(requests.length, 3, "5 points in batches of 2");
    const [r] = requests;
    assert.equal(r.method, "POST");
    assert.equal(r.path, "/write");
    assert.deepEqual(Object.fromEntries(r.query), { db: "mydb", precision: "s" });
    assert.equal(r.headers.authorization, "Basic dTpw");
    assert.equal(r.headers["content-encoding"], "gzip");
    assert.match(r.headers["user-agent"], /^hyperbytedb-js\//);
    assert.equal(r.body, "m,h=a v=0.5 0\nm,h=a v=1.5 1");
  });

  test("503 is retried, honouring Retry-After", async () => {
    script = [[503, { "Retry-After": "0" }, "busy"], [503, {}, "busy"]];
    assert.equal(await client().write("m v=1", "db"), 1);
    assert.equal(requests.length, 3);
  });

  test("gives up after the last retry", async () => {
    script = Array(3).fill([503, {}, '{"error":"write capacity exhausted"}']);
    await assert.rejects(client({ retries: 2 }).write("m v=1", "db"), (e) => e instanceof ServerError && e.status === 503 && /capacity/.test(e.message));
  });

  test("413 splits the batch", async () => {
    script = [[413, {}, "too big"]];
    assert.equal(await client().write(["m v=1", "m v=2", "m v=3", "m v=4"], "db"), 4);
    assert.deepEqual(requests.map((r) => r.body), ["m v=1\nm v=2\nm v=3\nm v=4", "m v=1\nm v=2", "m v=3\nm v=4"]);
  });

  test("bad writes and auth failures are not retried", async () => {
    script = [[400, {}, '{"error":"field type conflict"}'], [401, {}, "authorization failed"]];
    await assert.rejects(client().write("m v=1", "db"), (e) => e instanceof WriteError && /field type conflict/.test(e.message));
    await assert.rejects(client().write("m v=1", "db"), AuthError);
    assert.equal(requests.length, 2);
  });

  test("query results and errors", async () => {
    const doc = { results: [{ statement_id: 0, series: [{ name: "cpu", tags: { host: "a" }, columns: ["time", "mean"], values: [[1, 2.5]] }] }] };
    script = [
      [200, { "Content-Type": "application/json" }, JSON.stringify(doc)],
      [200, {}, JSON.stringify({ results: [{ statement_id: 0, error: "measurement not found" }] })],
      [400, {}, '{"error":"error parsing query: bad"}'],
    ];
    const c = client();
    const res = await c.query("SELECT mean(v) FROM cpu GROUP BY host", { database: "db", params: { h: "a" } });
    assert.deepEqual(res[0].rows(), [{ host: "a", time: 1, mean: 2.5 }]);
    const form = new URLSearchParams(requests[0].body);
    assert.equal(requests[0].method, "POST");
    assert.deepEqual(Object.fromEntries(form), { q: "SELECT mean(v) FROM cpu GROUP BY host", db: "db", epoch: "ms", params: '{"h":"a"}' });
    await assert.rejects(c.query("SELECT * FROM nope", { database: "db" }), (e) => e instanceof QueryError && /not found/.test(e.message));
    await assert.rejects(c.query("SELEC", { database: "db" }), (e) => e instanceof QueryError && /parsing/.test(e.message));
  });

  test("keepErrors and rfc3339", async () => {
    script = [[200, {}, JSON.stringify({ results: [{ statement_id: 0, error: "boom" }] })]];
    const res = await client().query("SELECT 1", { epoch: "rfc3339", keepErrors: true });
    assert.equal(res[0].error, "boom");
    assert.equal(new URLSearchParams(requests[0].body).has("epoch"), false);
  });

  test("health answers 503 without retrying", async () => {
    script = [[503, { "Content-Type": "application/json" }, '{"status":"draining"}']];
    assert.deepEqual(await client({ backoff: 5000 }).health(), { status: "draining" });
    assert.equal(requests.length, 1);
  });

  test("ping prefers X-Hyperbytedb-Version", async () => {
    script = [
      [204, { "X-Hyperbytedb-Version": "0.9.3-beta", "X-Influxdb-Version": "HyperbyteDB-0.9.3-beta" }, ""],
      [204, { "X-Influxdb-Version": "HyperbyteDB-0.9.2-beta" }, ""],
    ];
    assert.equal(await client().ping(), "0.9.3-beta");
    assert.equal(await client().ping(), "0.9.2-beta", "a server before 0.9.3");
  });

  test("an unreachable server fails after retries", async () => {
    await assert.rejects(new HyperbyteDBClient("http://127.0.0.1:1", { retries: 1, backoff: 10 }).ping(), HyperbyteDBError);
  });

  test("an abort stops retrying", async () => {
    script = Array(6).fill([503, {}, "busy"]);
    const start = Date.now();
    await assert.rejects(client({ backoff: 1000 }).write("m v=1", "db", { signal: AbortSignal.timeout(100) }));
    assert.ok(Date.now() - start < 2000);
  });

  test("a finished request leaves no timer keeping the process alive", async () => {
    // The per-request timeout timer must be cleared: with a 1-hour timeout
    // left armed, this file would not exit.
    await new HyperbyteDBClient(url, { timeout: 3_600_000 }).ping();
  });
});
