# HyperbyteDB 0.9.3-beta

A correctness and security release. Upgrade from any 0.9.x as soon as you
can: 0.9.2-beta and earlier can **store values in the wrong column**, store
a **conflicting field type as null**, and **lose acknowledged writes** in a
rolling restart of a replicated cluster. The images also stop trusting the
compromised eMudhra CA (CVE-2026-111943). Pair it with operator v0.3.3 when
that is out (it fixes backups and restarts each pod once per update); v0.3.2
runs 0.9.3-beta.

## Security

- **CVE-2026-111943, the compromised eMudhra intermediate CAs.** Eight
  intermediates under *emSign ECC Root CA - G3* have keys anyone can
  recover, so certificates forged from them are accepted by clients that
  trust that root.
  - The server, proxy and CLI trust the root list built into them
    (`webpki-roots`), which held that root; the CLI, which connects over
    HTTPS to any endpoint, was the exposed one. Now `webpki-roots` 1.0.10
    and `rustls-webpki` 0.103.16.
  - The images ship Debian `ca-certificates 20250419~deb12u2`, which drops
    the root (0.9.2-beta shipped `~deb12u1`). An image build now fails if
    the bundle still holds it.
  - Clients: run `cargo update` for the Rust client. The Go, JavaScript and
    Python clients use the runtime's trust store: Go 1.27.3 or 1.26.10,
    Node.js 26.12.1, 24.22.1 or 22.23.4, Python `certifi` 2026.10.19 have
    the fix.
- `rustls` 0.23.46, `h2` 0.4.20 and `quinn-proto` 0.11.19 (RUSTSEC-2026-0258,
  -0285, -0338 to -0342, -0346, -0348, -0349). The build no longer pulls in
  OpenSSL.

## Fixed

- **Values stored in the wrong column (data corruption).** When a
  measurement's first flush held writes with different fields, values were
  stacked by column position: `m used=1` then `m free=2` stored the `2`
  under `used`. Tags of different series swapped the same way. Batches are
  now combined by column name. Data already written this way is not
  repaired; re-write it if you can.
- **Field type conflicts stored as null.** A point that wrote, say, a string
  into a float field was accepted with 204 and its value stored as null.
  Now it is a partial write, as in InfluxDB 1.x: conflicting points are
  dropped, the rest stored, and the reply is 400 `partial write: field type
  conflict: ... dropped=N`. Integers still widen into floats, and integer
  and unsigned into unsigned.
- **Acknowledged writes lost in a rolling restart** (replicated,
  `sync_quorum` with hinted handoff). Restarting peers got neither the write
  nor a hint; a restarted node could lose its readmission; and a write could
  be acknowledged with only the local copy. In a three-node restart test
  every node now holds every acknowledged write (0.9.2-beta: 643 to 1,825 of
  3,204 missing per node).
- **Sharded writes failed with 500 while a region's primary restarted.**
  They are now held in the receiving node's forward queue and acknowledged.
- **Proxy answered 503 at once** when a backend went down between health
  checks. It now takes that backend out of rotation and holds the request
  for up to `hold_timeout`.
- **Drains always took 90 s.** They waited for acknowledgements nothing
  records.
- **`SHOW TAG VALUES CARDINALITY` counted a value twice** after a restart.
- **Clusters formed with `cluster.peers`** left every node but the first a
  Raft learner, Syncing and unhealthy, for good. Learners are now promoted
  as they catch up.
- **A node's first writes after its `/health` turned 200 could get 503.**
- **Subqueries over an aggregate failed** (`SELECT max(m) FROM (SELECT
  mean(v) AS m ... GROUP BY time(10s))`), on one node and sharded alike.
- **A raw field with `GROUP BY time()`** gave a chDB error; it now gives
  InfluxDB's `GROUP BY requires at least one aggregate function`.

## Added

- **`top()`, `bottom()` and `sample()`**, with tag arguments, other fields
  selected beside them, `GROUP BY time()` and tags, and `LIMIT`/`OFFSET`.
  Sharded queries rank across regions. See
  [the reference](reference.md#selector-functions).
- **Go, JavaScript/TypeScript and Rust clients**, beside the Python one
  (`clients/`).
- **`X-Hyperbytedb-Version`** on every response, the bare version.
  `X-Influxdb-Version` and `X-Influxdb-Build` stay for InfluxDB tools.

## Upgrading

- Upgrade nodes one at a time, as for any rolling upgrade. A cluster mixing
  0.9.2 and 0.9.3 nodes has not been tested beyond that.
- Writes that conflict with a field's type now get a 400 partial write
  where they got 204. The batch's other points are stored; Telegraf does
  not retry a 400, so it does not write them twice.
- The Python client's `ping()` returns the bare version (`0.9.3-beta`, was
  `HyperbyteDB-0.9.2-beta`).
- Build images without the Docker layer cache, or the build stops at the
  CA bundle check.

## Known limits

Those of [0.9.2-beta](release-notes-0.9.2-beta.md#known-limits) still apply.
Also:

- `server.request_timeout_secs` (30 s) also caps queries, so a count over a
  few hundred million points can time out.
- Regions split by series count only: a measurement with few series and
  many points stays one region, and moving it during a scale-down is slow
  (277M points took about 50 minutes on n3).
- With `cluster.peers`, a write to a new node in the second or two before
  the leader adds it can still get a retryable 503.
