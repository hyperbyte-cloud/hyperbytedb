# User Guide

This guide covers deploying, configuring, and operating HyperbyteDB. You do not need to read the source code.

## Reading order

| Step | Page | What you will do |
|------|------|------------------|
| 1 | [Installation](installation.md) | Run HyperbyteDB with Docker, Compose, kind, or the Kubernetes operator |
| 2 | [Configuration](configuration.md) | Set `config.toml` and environment variables |
| 3 | [Basic operations](basic-operations.md) | Create a database, write line protocol, run InfluxQL |
| 3b | [CLI (hyperbytedb-cli)](cli.md) | Interactive shell, batch queries, write/import from the terminal |
| 4 | [Authentication](authentication.md) | Optional: require credentials on `/write` and `/query` |
| 5 | [Advanced features](advanced-features.md) | Clustering, continuous queries, TLS, tracing |
| 6 | [Common workflows](common-workflows.md) | Migrate from InfluxDB 1.x, wire Telegraf and Grafana |
| 7 | [Administration](administration.md) | Metrics, logs, traces, backups, cluster operations |
| 8 | [Troubleshooting](troubleshooting.md) | Fix startup, query, and cluster issues |
| 9 | [API reference](reference.md) | HTTP endpoints and InfluxQL compatibility |

## Prerequisites

| Requirement | Details |
|-------------|---------|
| **Platforms** | Linux x86_64 or aarch64 for pre-built images and tarballs; macOS supported for source builds |
| **Runtime** | Docker, or Rust + libchdb for source builds |
| **Network** | Port `8086` by default for the HTTP API |
