# Changelog

All notable changes to HyperbyteDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `SECURITY.md` with vulnerability reporting policy and supported versions.
- GitHub issue and pull request templates for contributors.
- This changelog.

### Changed

- Expanded `config.toml.example` to cover all documented configuration sections.

## [0.8.3] - 2025-06-01

### Added

- chDB connection pool for parallel flush inserts and concurrent queries.
- Arrow WAL zero-copy flush path.
- InfluxQL parity improvements ([#52](https://github.com/hyperbyte-cloud/hyperbytedb/pull/52)).
- Advanced CLI completion and REPL enhancements ([#51](https://github.com/hyperbyte-cloud/hyperbytedb/pull/51)).

### Fixed

- Materialized view pipeline bugs ([#55](https://github.com/hyperbyte-cloud/hyperbytedb/pull/55), [#58](https://github.com/hyperbyte-cloud/hyperbytedb/pull/58)).
- Continuous query `GROUP BY` handling ([#10](https://github.com/hyperbyte-cloud/hyperbytedb/pull/10)).

## [0.8.0] - 2025-05-01

### Added

- Initial public release: InfluxDB v1 API, embedded chDB, RocksDB WAL, clustering with Raft schema consensus.
- `hyperbytedb-cli` interactive shell ([#8](https://github.com/hyperbyte-cloud/hyperbytedb/pull/8)).
- Materialized views ([#11](https://github.com/hyperbyte-cloud/hyperbytedb/pull/11)).

[Unreleased]: https://github.com/hyperbyte-cloud/hyperbytedb/compare/v0.8.3...HEAD
[0.8.3]: https://github.com/hyperbyte-cloud/hyperbytedb/compare/v0.8.0...v0.8.3
[0.8.0]: https://github.com/hyperbyte-cloud/hyperbytedb/releases/tag/v0.8.0
