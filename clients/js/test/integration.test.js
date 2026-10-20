// Against a real HyperbyteDB: HYPERBYTEDB_URL (and, with auth on,
// HYPERBYTEDB_USER / HYPERBYTEDB_PASSWORD for an admin).
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { after, before, describe, test } from "node:test";
import { AuthError, HyperbyteDBClient, QueryError, WriteError } from "../dist/index.js";

const URL_ = process.env.HYPERBYTEDB_URL;
const opts = { skip: !URL_ && "HYPERBYTEDB_URL not set" };
const db = new HyperbyteDBClient(URL_ ?? "http://localhost:8086", {
  username: process.env.HYPERBYTEDB_USER,
  password: process.env.HYPERBYTEDB_PASSWORD,
});
const name = `jsclient_${randomBytes(4).toString("hex")}`;

describe("integration", opts, () => {
  before(async () => URL_ && db.createDatabase(name));
  after(async () => URL_ && db.dropDatabase(name));

  // Rows reach queries at the next flush; wait for them.
  async function waitRows(q, n) {
    const deadline = Date.now() + 15_000;
    for (;;) {
      const rows = (await db.query(q, { database: name }))[0].rows();
      if (rows.length >= n || Date.now() > deadline) return rows;
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  test("ping and health", opts, async () => {
    assert.match(await db.ping(), /^\d/);
    assert.ok(await db.health());
  });

  test("round trip with awkward names and every type", opts, async () => {
    const base = 1_700_000_000_000;
    const points = [0, 1, 2].map((i) => ({
      measurement: "disk usage",
      tags: { host: "web 1,a=b", service: "Filesystem /" },
      fields: { used: 12.5 + i, inodes: BigInt(100 + i), mount: 'C:\\ "data"' },
      time: base + i * 1000,
    }));
    assert.equal(await db.write(points, name, { precision: "ms" }), 3);
    const rows = await waitRows('SELECT * FROM "disk usage" ORDER BY time', 3);
    assert.equal(rows.length, 3);
    assert.equal(rows[0].time, base);
    assert.equal(rows[0].host, "web 1,a=b");
    assert.equal(rows[0].service, "Filesystem /");
    assert.equal(rows[0].used, 12.5);
    assert.equal(rows[2].inodes, 102);
    assert.equal(rows[0].mount, 'C:\\ "data"');
  });

  test("queries by tag with epoch and bound params", opts, async () => {
    const lines = [];
    for (let h = 0; h < 3; h++) for (let i = 0; i < 4; i++) lines.push(`cpu,host=h${h} usage=${h * 10 + i}.0 ${1_700_000_000 + i}`);
    assert.equal(await db.write(lines, name, { precision: "s" }), 12);
    await waitRows("SELECT * FROM cpu", 12);
    const res = await db.query("SELECT max(usage) FROM cpu GROUP BY host", { database: name, epoch: "s" });
    assert.deepEqual(Object.fromEntries(res[0].series.map((s) => [s.tags.host, s.values[0][1]])), { h0: 3, h1: 13, h2: 23 });
    const counted = await db.query("SELECT count(usage) FROM cpu WHERE host = $h", { database: name, params: { h: "h1" } });
    assert.equal(counted[0].series[0].values[0][1], 4);
  });

  test("errors", opts, async () => {
    await assert.rejects(db.query("SELEKT nonsense", { database: name }), QueryError);
    await assert.rejects(db.write(["m v=1", 'm v="text"'], name), WriteError, "a field type conflict");
    if (process.env.HYPERBYTEDB_USER) {
      const bad = new HyperbyteDBClient(URL_, { username: process.env.HYPERBYTEDB_USER, password: "wrong", retries: 0 });
      await assert.rejects(bad.databases(), AuthError);
    }
  });
});
