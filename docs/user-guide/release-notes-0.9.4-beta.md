# HyperbyteDB 0.9.4-beta

One fix, for nodes with little write traffic: they could need many times
their usual memory to restart, and fail to start at all. Upgrade any node
that takes few writes; operator v0.3.3 runs 0.9.4-beta.

## Fixed

- **A quiet node could not restart after a few days of uptime.** Each flush
  removed the flushed entries from the WAL with a range delete. On a busy
  node those are cleared within minutes; on a quiet one they piled up in
  memory for as long as the node ran, and the next start had to replay all
  of them, at a cost that grows with the square of their number. On n3 the
  shared tenant pool, after 5 days at about one point a second, needed
  11 GiB and 88 s to start, and was OOM-killed at its 4 GiB limit for
  19 hours. Truncation now removes only the new entries, with point deletes
  for a few, and the rest are flushed to disk before they pile up.
  Reopening a WAL after 30,000 flushes now takes 0.3 s and 1 MiB (it took
  10 s and 1.8 GB after 5,000).

## Upgrading

- Upgrade nodes one at a time, as usual.
- A node's first start on 0.9.4 still replays the range deletes it already
  holds, as before. The cost grows with the square of how long a quiet
  node has been up since its last start: n3's shared pool, flushing about
  once a minute, needed 11 GiB after 5 days. If a quiet node has been up
  for days, give it more memory for the upgrade, or restart it on its
  current version first: a start writes the replayed deletes to disk, so
  the next start, on 0.9.4, has nothing to replay.

## Known limits

Those of [0.9.3-beta](release-notes-0.9.3-beta.md#known-limits).
