# User Guide

This guide covers deploying, configuring, and operating HyperbyteDB. You do not need to read the source code.

## Reading order

| Step | Page | What you will do |
|------|------|------------------|
| 1 | [Installation](installation.md) | Run HyperbyteDB with Docker, Compose, or the Kubernetes operator |
| 2 | [Configuration](configuration.md) | Set `config.toml` and environment variables |
| 3 | [Basic operations](basic-operations.md) | Create a database, write line protocol, run TimeseriesQL |
| 3b | [CLI (hyperbytedb-cli)](cli.md) | Interactive shell, batch queries, write/import from the terminal |
| 4 | [Authentication](authentication.md) | Optional: require credentials on `/write` and `/query` |
| 4b | [Rate limiting](rate-limiting.md) | Optional: cap `/write` and `/query` request rates |
| 5 | [Advanced features](advanced-features.md) | Clustering, continuous queries, TLS, tracing |
| 6 | [Common workflows](common-workflows.md) | Migrate from InfluxDB 1.x, wire Telegraf and Grafana |
| 7 | [Administration](administration.md) | Metrics, logs, traces, backups, cluster operations |
| 8 | [Troubleshooting](troubleshooting.md) | Fix startup, query, and cluster issues |
| 9 | [API reference](reference.md) | HTTP endpoints and TimeseriesQL compatibility |
| 10 | [Resource sizing](resource-sizing.md) | CPU, memory, disk, and cluster sizing guidelines |
| 11 | [V1 stable scope](v1-stable-scope.md) | Supported topologies, guarantees, and breaking-change policy |

## Prerequisites

| Requirement | Details |
|-------------|---------|
| **Platforms** | Linux x86_64 or aarch64 for pre-built images and tarballs |
| **Runtime** | Docker, Docker Compose, or Kubernetes with the HyperbyteDB operator |
| **Network** | Port `8086` by default for the HTTP API |
