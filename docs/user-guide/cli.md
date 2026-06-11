# hyperbytedb-cli

`hyperbytedb-cli` is the interactive command-line client for HyperbyteDB. It mirrors the InfluxDB v1 `influx` client: an interactive TimeseriesQL shell, batch `-execute` mode, line-protocol write/import, and admin helpers over the HTTP API.

Backup and restore remain **server-local** operations on the `hyperbytedb` binary (`hyperbytedb backup` / `hyperbytedb restore`). A remote backup API may be added later.

---

## Installation

### Release tarball

Each `v*` GitHub Release ships `hyperbytedb-cli` alongside `hyperbytedb` and `libchdb.so`:

```bash
tar -xzf hyperbytedb-vx.x.x-linux-x86_64.tar.gz
./hyperbytedb-cli -host http://localhost:8086 ping
```

### Docker image

The official image includes the CLI at `/usr/local/bin/hyperbytedb-cli`:

```bash
docker exec -it <container> hyperbytedb-cli -host http://127.0.0.1:8086 ping
```

### Build from source

```bash
cargo build --release -p hyperbytedb-cli
./target/release/hyperbytedb-cli --help
```

---

## Quick start

### Interactive shell

```bash
hyperbytedb-cli -host http://localhost:8086
```

```
Connected to http://localhost:8086 (HyperbyteDB-x.x.x)
hyperbytedb> CREATE DATABASE telemetry
hyperbytedb> USE telemetry
telemetry> SHOW MEASUREMENTS
telemetry> SELECT * FROM cpu WHERE time > now() - 1h LIMIT 10
```

### Batch mode (scripting)

```bash
hyperbytedb-cli -host http://localhost:8086 -execute 'SHOW DATABASES' -format column
```

Exit code is non-zero on connection, auth, or query errors (stdout = data, stderr = errors).

### Write line protocol

```bash
echo 'cpu,host=a usage=1' | hyperbytedb-cli write -host http://localhost:8086 -database telemetry
```

### Import / export

```bash
hyperbytedb-cli import -host http://localhost:8086 -path backup.txt
hyperbytedb-cli export -host http://localhost:8086 -database telemetry -out dump.txt
```

---

## Connection and configuration

### Flags

| Flag | Description |
|------|-------------|
| `-host`, `-H` | Server URL or hostname (default `http://localhost:8086`) |
| `-port` | Port when host is not a full URL |
| `-database`, `-d` | Default database |
| `-username`, `-u` | Username |
| `-password`, `-p` | Password (empty prompts interactively) |
| `--ssl` | Use HTTPS |
| `--unsafeSsl` | Skip TLS certificate verification |
| `--profile` | Config profile from `~/.config/hyperbytedb/config.toml` |
| `-execute`, `-e` | Run TimeseriesQL and exit |
| `-format`, `-f` | `column`, `json`, or `csv` |
| `--precision` / `--epoch` | Timestamp format for query results |
| `--pretty` | Pretty-print JSON |
| `-v`, `--verbose` | Verbose output |

### Environment variables

| Variable | Description |
|----------|-------------|
| `HYPERBYTEDB_HOST` | Server URL |
| `HYPERBYTEDB_DATABASE` | Default database |
| `HYPERBYTEDB_USERNAME` | Username |
| `HYPERBYTEDB_PASSWORD` | Password |
| `HYPERBYTEDB_CLI_CONFIG` | Path to config file |
| `HYPERBYTEDB_CLI_HISTORY` | REPL history file (default `~/.hyperbytedb_history`) |

`INFLUX_HOST`, `INFLUX_DATABASE`, `INFLUX_USERNAME`, and `INFLUX_PASSWORD` are accepted as aliases for drop-in migration.

### Config profiles

`~/.config/hyperbytedb/config.toml`:

```toml
[default]
host = "http://localhost:8086"
database = "mydb"
username = "admin"

[prod]
host = "https://tsdb.example.com:8086"
username = "reader"
```

Use `--profile prod` to select a profile. Passwords should be supplied via environment variables or prompts, not stored in the config file.

---

## REPL meta-commands

These are handled locally and are **not** sent to `/query`:

| Command | Description |
|---------|-------------|
| `help` | List meta-commands |
| `connect <host[:port]>` | Reconnect to another server |
| `use <db>` or `use <db>.<rp>` | Set database / retention policy |
| `clear database\|db\|rp` | Clear session context |
| `auth` | Prompt for username/password |
| `insert <line_protocol>` | Write via line protocol |
| `insert into <rp> ...` | Write to a specific retention policy |
| `format json\|csv\|column` | Output format |
| `precision <unit>` | Timestamp display (`ns`, `us`, `ms`, `s`, `rfc3339`) |
| `pretty` | Toggle JSON pretty-print |
| `chunked` | Toggle chunked query responses |
| `chunk size <n>` | Chunk size (default 10000) |
| `settings` | Show session state |
| `timing` | Toggle per-query duration |
| `history` | History hint (use up-arrow) |
| `exit`, `quit` | Exit shell |

Any other input is TimeseriesQL. Semicolon-separated statements run in sequence.

---

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `create database <name>` | Create a database (InfluxDB v1-compatible shortcut) |
| `write` | Line protocol from stdin or `--file` |
| `import` | Influx-compatible DDL+DML import (`--path`, `--compressed`, `--pps`) |
| `export` | Logical export to DDL+DML + line protocol |
| `ping` | Liveness + server version |
| `health` | `/health` (add `--ready` for `/health/ready`) |
| `metrics` | Prometheus metrics |
| `statements` | Recent query digest (`/api/v1/statements`) |
| `cluster nodes` | List cluster nodes (admin) |
| `cluster leader` | Raft leader (admin) |
| `cluster metrics` | Cluster metrics (admin) |
| `cluster drain --yes` | Initiate graceful drain (admin) |

---

## TimeseriesQL via CLI

Schema shortcuts and TimeseriesQL in the REPL or `-execute`:

- `create database <name>` or `CREATE/DROP DATABASE`
- `CREATE/ALTER/DROP RETENTION POLICY`
- `CREATE/DROP USER`, `SET PASSWORD`, `SHOW USERS`, `GRANT`/`REVOKE`
- `SHOW MEASUREMENTS`, `SHOW TAG KEYS/VALUES`, `SHOW FIELD KEYS`, `SHOW SERIES`
- `SELECT`, `DELETE`, `DROP MEASUREMENT`
- `CREATE/DROP/SHOW CONTINUOUS QUERY`
- `CREATE/DROP/SHOW MATERIALIZED VIEW`

See [API & TimeseriesQL Reference](reference.md) for full syntax.

---

## Compatibility vs InfluxDB v1 `influx`

| Feature | Status |
|---------|--------|
| Interactive REPL | Supported |
| `-execute` batch mode | Supported |
| `use`, `connect`, `insert`, `format` | Supported |
| Line protocol write/import | Supported |
| Config profiles (`~/.config/hyperbytedb/config.toml`) | Supported (extension) |
| `SHOW STATS`, `SHOW DIAGNOSTICS`, `SHOW SHARDS` | Not supported — use `metrics` subcommand |
| `SHOW QUERIES`, `KILL QUERY` | Not supported |
| `EXPLAIN` / `EXPLAIN ANALYZE` | Not supported |
| Flux (`-type flux`) | Not supported |
| Subscriptions | Not supported |
| `influx_inspect export` (TSM) | N/A — HyperbyteDB uses chDB storage |
| `influxd backup` portable format | Server-local `hyperbytedb backup` only |

---

## Backup (server-side)

Backup and restore require filesystem access on the server host:

```bash
hyperbytedb --config config.toml backup --output /path/to/backup
# stop server
hyperbytedb --config config.toml restore --input /path/to/backup
hyperbytedb --config config.toml serve
```

See [Administration](administration.md) for details.

---

## Cluster admin examples

```bash
hyperbytedb-cli -host http://node1:8086 -username admin -password secret cluster nodes
hyperbytedb-cli -host http://node1:8086 -username admin -password secret cluster leader
hyperbytedb-cli -host http://node1:8086 -username admin -password secret cluster drain --yes
```

Cluster routes require **admin** credentials when `[auth] enabled = true`.
