# Deep Dive: Compaction

HyperbyteDB stores time-series data in embedded chDB `ReplacingMergeTree` tables. Background part merges and consolidation are handled by ClickHouse/chDB internally.

There is no application-level compaction service in the database binary.

For how WAL batches become queryable tables, see [Write path](deep-dive-write-path.md). For cluster alignment, see [Clustering](deep-dive-clustering.md).
