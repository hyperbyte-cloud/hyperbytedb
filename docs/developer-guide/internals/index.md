# Internals

Technical deep dives into the HyperbyteDB implementation for engineers working on the codebase.

## Contents

1. **[Core Modules](core-modules.md)** — Module-by-module walkthrough of every directory under `src/`. Covers domain types, port traits, adapter implementations, application services, the TimeseriesQL engine, and the clustering subsystem.

2. **[Key Design Decisions](key-design-decisions.md)** — Deep dives into the write path, read path, flush pipeline, WAL design, chDB storage layout, clustering model, Raft consensus, replication protocol, and peer sync.

3. **[Replication design](replication-design.md)** — Write-replication wire format (`/internal/replicate`), `sync_quorum`, hinted handoff (`CFh1`), flow control, and how this ties to self-repair.

4. **[Extension Points](extension-points.md)** — Step-by-step guides for adding new TimeseriesQL statements, storage backends, background services, and HTTP endpoints.
