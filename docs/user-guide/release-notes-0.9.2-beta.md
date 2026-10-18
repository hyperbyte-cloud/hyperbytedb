# HyperbyteDB 0.9.2-beta

A small release on top of 0.9.1-beta: the InfluxDB 1.x cardinality
statements, and a fix to `SHOW SERIES` on sharded clusters. Pair it with
operator v0.3.2.

## Added

- **`SHOW ... CARDINALITY`.** `SHOW SERIES`, `MEASUREMENT`, `TAG KEY`,
  `TAG VALUES` and `FIELD KEY [EXACT] CARDINALITY`, with `ON`, `FROM`,
  `WITH`, `WHERE`, `LIMIT` and `OFFSET` as in InfluxDB 1.x.
  `SHOW SERIES CARDINALITY` failed to parse before.
  - Without a `WHERE`, counts come from the metadata caches, with no scan.
    The series cache now keeps a count per measurement. On a sharded cluster
    each region's peer counts its own range, so a replicated series counts
    once.
  - Counts are always exact. With no filter, series and measurement
    cardinality reply under `cardinality estimation`, as InfluxDB does.
    `EXACT` or any filter replies with one `count` row per measurement.
  - See [the reference](reference.md#cardinality).

## Fixed

- `SHOW SERIES` on a sharded cluster listed `<id>:{tags}` instead of series
  keys (`cpu,host=a`).

## Upgrading

- Upgrade nodes in any order. A 0.9.2 node asks a 0.9.1 peer for its series
  list and counts them itself, so cardinality is right during a rolling
  upgrade.
- Operator v0.3.2 knows 0.9.1-beta as its newest version. It renders
  everything for 0.9.2-beta, but its compatibility condition reads
  `NewerThanKnown`.

## Known limits

Those of [0.9.1-beta](release-notes-0.9.1-beta.md#known-limits) still apply.
Also:

- `SHOW FIELD KEY CARDINALITY` refuses a `WHERE`, because fields are not
  indexed by tag.
- `FROM /regex/` is not parsed for any `SHOW` statement yet.
