#!/usr/bin/env bash
# Copyright 2026 Hyperbyte Cloud
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# HyperbyteDB Load Test Runner
#
# Two modes:
#   single  – builds and runs a local server, load-tests it, then tears it down.
#   cluster – targets an already-running cluster (e.g. via the K8s operator).
#
# Positional args: [host] [port] [rps] [duration] [points]
#   rps     – requests per second (default: 100); script strictly enforces this rate
#   duration – test duration, e.g. 15s, 1m (default: 10s)
#   points  – points per write request (default: 100)
#
# Usage:
#   ./load.sh                                     # Single mode, defaults
#   ./scripts/load_test.sh                        # Same (wrapper from repo root)
#   ./load.sh single                              # Explicit single mode
#   ./load.sh single 127.0.0.1 8086 100 15s 100  # Single mode (100 req/s, 15s, 100 pts/req)
#   ./load.sh cluster 10.0.0.50                   # Cluster mode (host required)
#   ./load.sh cluster 10.0.0.50 8086 100 15s 100  # Cluster mode, all args
#
#   CLUSTER_NODES=10.0.0.51,10.0.0.52,10.0.0.53 \
#     ./load.sh cluster 10.0.0.50                 # Health-check individual nodes
#
# Environment overrides:
#   MODE                single|cluster  (alternative to positional arg)
#   RPS                 requests per second (overrides positional arg)
#   CLUSTER_NODES       Comma-separated list of node addresses for health checks
#   WRITE_FORMAT        line (default) | msgpack — POST /write body encoding (msgpack matches
#                       Content-Type: application/msgpack on the server). Throughput in the
#                       summary is points/sec ≈ RPS × points per request when checks pass.
#   RUN_BENCHES         1 (default) | 0 — after k6, run cargo bench for ILP + columnar ingestion
#                       and include results in log/load/<ts>/summary.log
#   RUN_FLAMEGRAPH      0 (default) | 1 — single mode only. Captures perf samples on the
#                       server PID for the duration of the k6 soak, then writes:
#                         log/load/<ts>/perf.data            — raw perf record output
#                         log/load/<ts>/perf-report.txt      — perf report --stdio (full)
#                         log/load/<ts>/perf-top-symbols.txt — top 50 symbols by self-cost
#                         log/load/<ts>/flamegraph.svg       — interactive flamegraph
#                       Triggers a release rebuild with `-C force-frame-pointers=yes
#                       -C debuginfo=line-tables-only` so perf can walk + symbolize stacks.
#                       Requires: perf (linux-perf) and one of `flamegraph` / `inferno`
#                       (cargo install flamegraph  OR  cargo install inferno).
#                       Also requires `kernel.perf_event_paranoid <= 1` or root.
#   PERF_FREQ           99 (default) — perf sampling frequency in Hz. Higher = more
#                       resolution, larger perf.data, more recording overhead.
#
# Requirements: cargo (single mode only + benches), k6 (https://grafana.com/docs/k6/latest/set-up/install-k6/)
#

set -eu

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

readonly SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "$0")" && pwd)}"
readonly PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
readonly LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/log/load}"
readonly TIMESTAMP="$(date +%Y-%m-%d_%H-%M-%S)"
readonly SERVER_PID_FILE="$PROJECT_ROOT/.server.pid"

# Detect mode: first arg may be "single" or "cluster"; otherwise fall back to
# the MODE env var (default: single) and treat all args as host/port/etc.
if [[ "${1:-}" == "single" || "${1:-}" == "cluster" ]]; then
    MODE="$1"
    shift
else
    MODE="${MODE:-single}"
fi
readonly MODE

TARGET_HOST="${1:-}"
TARGET_PORT="${2:-5234}"
RPS="${RPS:-${3:-200}}"
DURATION="${4:-30s}"
POINTS_PER_REQUEST="${5:-1000}"
CLUSTER_NODES="${CLUSTER_NODES:-}"
WRITE_FORMAT="${WRITE_FORMAT:-line}"
RUN_BENCHES="${RUN_BENCHES:-1}"
RUN_FLAMEGRAPH="${RUN_FLAMEGRAPH:-0}"
PERF_FREQ="${PERF_FREQ:-99}"

K6_TARGET_HOST="${TARGET_HOST:-127.0.0.1}"

# perf record PID, populated in run_load_test when RUN_FLAMEGRAPH=1.
PERF_PID=""

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

log() {
    printf '\033[36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"
}

log_ok() {
    printf '\033[32m[%s] ✓\033[0m %s\n' "$(date +%H:%M:%S)" "$*"
}

log_err() {
    printf '\033[31m[%s] ✗\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2
}

cleanup() {
    # Best-effort: stop any in-flight perf record so it flushes perf.data.
    if [ -n "${PERF_PID:-}" ] && kill -0 "$PERF_PID" 2>/dev/null; then
        kill -INT "$PERF_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$PERF_PID" 2>/dev/null || true
    fi
    [[ "$MODE" == "cluster" ]] && return 0
    if [ -f "$SERVER_PID_FILE" ]; then
        pid="$(cat "$SERVER_PID_FILE")"
        if kill -0 "$pid" 2>/dev/null; then
            log "Stopping server (PID $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$SERVER_PID_FILE"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Server lifecycle
# ─────────────────────────────────────────────────────────────────────────────

pre_build_server() {
    if [[ "$RUN_FLAMEGRAPH" == "1" ]]; then
        # Force frame pointers so `perf record --call-graph fp` can walk the
        # stack reliably, and `line-tables-only` debuginfo so symbols + file/line
        # show up in perf report and the flamegraph (without bloating the binary
        # the way full DWARF does).
        log "Building HyperbyteDB server (release + frame pointers + line tables for perf)..."
        (
            cd "$PROJECT_ROOT"
            RUSTFLAGS="${RUSTFLAGS:-} -C force-frame-pointers=yes -C debuginfo=line-tables-only" \
                cargo build --release
        ) || {
            log_err "Build failed"
            exit 1
        }
    else
        log "Building HyperbyteDB server (release)..."
        (cd "$PROJECT_ROOT" && cargo build --release) || {
            log_err "Build failed"
            exit 1
        }
    fi
    log_ok "Build complete"
}

start_server() {
    mkdir -p "$LOG_DIR/$TIMESTAMP"

    local binary="$PROJECT_ROOT/target/release/hyperbytedb"
    if [ ! -x "$binary" ]; then
        log_err "Release binary not found at $binary (was pre_build_server skipped?)"
        exit 1
    fi

    log "Starting HyperbyteDB on 0.0.0.0:$TARGET_PORT..."
    cd "$PROJECT_ROOT"
    HYPERBYTEDB__SERVER__PORT="$TARGET_PORT" \
    HYPERBYTEDB__LOGGING__LEVEL=info \
        "$binary" serve \
        &> "$LOG_DIR/$TIMESTAMP/server.log" &
    SERVER_PID=$!
    echo "$SERVER_PID" > "$SERVER_PID_FILE"
    log_ok "Server PID: $SERVER_PID"
}

wait_for_server() {
    log "Waiting for server to accept connections..."
    base_url="http://127.0.0.1:$TARGET_PORT"
    max_attempts=60
    attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -sf -o /dev/null -m 2 "$base_url/health" 2>/dev/null; then
            log_ok "Server ready at $base_url"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    log_err "Server did not become ready within ${max_attempts}s"
    log_err "Last server output:"
    tail -20 "$LOG_DIR/$TIMESTAMP/server.log" >&2 || true
    return 1
}

stop_server() {
    if [ -f "$SERVER_PID_FILE" ]; then
        pid="$(cat "$SERVER_PID_FILE")"
        if kill -0 "$pid" 2>/dev/null; then
            log "Stopping server (PID $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$SERVER_PID_FILE"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Cluster lifecycle
# ─────────────────────────────────────────────────────────────────────────────

wait_for_cluster() {
    log "Verifying cluster connectivity..."
    local base_url="http://${K6_TARGET_HOST}:${TARGET_PORT}"

    local max_attempts=30
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -sf -o /dev/null -m 5 "$base_url/health" 2>/dev/null; then
            log_ok "Cluster endpoint reachable at $base_url"
            break
        fi
        attempt=$((attempt + 1))
        sleep 2
    done

    if [ $attempt -ge $max_attempts ]; then
        log_err "Cannot reach cluster at $base_url/health after ${max_attempts} attempts"
        return 1
    fi

    if [[ -n "$CLUSTER_NODES" ]]; then
        IFS=',' read -ra NODES <<< "$CLUSTER_NODES"
        local failed=0
        for node in "${NODES[@]}"; do
            node="$(echo "$node" | xargs)"
            local node_url="http://${node}:${TARGET_PORT}"
            if curl -sf -o /dev/null -m 5 "$node_url/health" 2>/dev/null; then
                log_ok "  Node $node healthy"
            else
                log_err "  Node $node unreachable at $node_url/health"
                failed=1
            fi
        done
        if [ $failed -ne 0 ]; then
            return 1
        fi
        log_ok "All ${#NODES[@]} cluster nodes healthy"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────────────

wait_for_flush() {
    local base_url="http://${K6_TARGET_HOST}:${TARGET_PORT}"
    local max_wait=120
    local waited=0
    local prev_count=""

    while [ $waited -lt $max_wait ]; do
        # Query SHOW MEASUREMENTS and check data exists via a SELECT
        resp=$(curl -sf "$base_url/query?db=server&q=SELECT+COUNT(idle)+FROM+cpu" 2>/dev/null || echo "")
        if echo "$resp" | grep -q '"values"'; then
            log_ok "Data flushed to disk (waited ${waited}s)"
            sleep 2
            return 0
        fi
        sleep 3
        waited=$((waited + 3))
        log "  still waiting for flush... (${waited}s / ${max_wait}s)"
    done

    log_err "Data not flushed within ${max_wait}s, proceeding anyway"
}

setup_databases() {
    local base_url="http://${K6_TARGET_HOST}:${TARGET_PORT}"
    log "Creating test databases..."
    for db in server; do
        curl -sf -X POST "$base_url/query" --data-urlencode "q=CREATE DATABASE $db" -o /dev/null || {
            log_err "Failed to create database '$db'"
            return 1
        }
    done
    log_ok "Databases ready"
}

# ─────────────────────────────────────────────────────────────────────────────
# perf / flamegraph capture
# ─────────────────────────────────────────────────────────────────────────────

# Pre-flight: confirm `perf` is installed and the kernel will let us sample a
# user-owned PID without root. Returns 0 on success, non-zero with a clear
# error message if anything is missing.
preflight_perf() {
    if ! command -v perf &>/dev/null; then
        log_err "perf not found. Install it:"
        log_err "  Debian/Ubuntu:  apt-get install -y linux-perf  # or linux-tools-\$(uname -r)"
        log_err "  RHEL/Fedora:    dnf install -y perf"
        return 1
    fi
    # On Debian-derived distros `perf` is a wrapper that exec's the
    # kernel-version-matched binary (e.g. perf_5.10). If the matching package
    # isn't installed, `command -v` succeeds but the wrapper fails at runtime.
    # Catch that here.
    if ! perf --version &>/dev/null; then
        log_err "perf is installed as a wrapper but the kernel-matched binary is missing."
        log_err "  perf --version output:"
        perf --version 2>&1 | sed 's/^/    /' >&2 || true
        log_err "  Install: apt-get install -y linux-tools-\$(uname -r)  # or linux-perf"
        return 1
    fi

    local paranoid
    paranoid="$(sysctl -n kernel.perf_event_paranoid 2>/dev/null || echo 3)"
    if [ "$paranoid" -gt 1 ] && [ "$(id -u)" -ne 0 ]; then
        log_err "kernel.perf_event_paranoid=$paranoid blocks userspace profiling."
        log_err "  Either run as root, or:  sudo sysctl -w kernel.perf_event_paranoid=1"
        log_err "  (persist with: echo 'kernel.perf_event_paranoid=1' | sudo tee /etc/sysctl.d/99-perf.conf)"
        return 1
    fi

    # We need either `flamegraph` (the cargo-flamegraph standalone binary)
    # or both `inferno-collapse-perf` + `inferno-flamegraph`.
    if ! command -v flamegraph &>/dev/null \
        && ! { command -v inferno-collapse-perf &>/dev/null && command -v inferno-flamegraph &>/dev/null; }; then
        log_err "No flamegraph generator found. Install one of:"
        log_err "  cargo install flamegraph   # provides 'flamegraph' binary"
        log_err "  cargo install inferno      # provides inferno-* binaries (much faster)"
        return 1
    fi

    return 0
}

# Start `perf record` against the running server PID. Records until killed
# (typically by stop_perf_record after k6 finishes). Writes to perf.data in
# the run's log dir.
start_perf_record() {
    local pid="$1"
    local out_dir="$2"

    if ! kill -0 "$pid" 2>/dev/null; then
        log_err "Cannot perf-record PID $pid (not running)"
        return 1
    fi

    log "Starting perf record on PID $pid at ${PERF_FREQ}Hz (call-graph=fp)..."
    # -F: sample frequency in Hz
    # -g --call-graph fp: walk frame pointers (cheap; matches the build flags)
    # -p: attach to existing pid (no fork)
    # --proc-map-timeout: give perf longer to read /proc/PID/maps under load
    perf record \
        -F "$PERF_FREQ" \
        -g --call-graph fp \
        -p "$pid" \
        --proc-map-timeout 5000 \
        -o "$out_dir/perf.data" \
        &> "$out_dir/perf-record.log" &
    PERF_PID=$!
    # Tiny grace period so perf is actually attached before k6 fires off.
    sleep 1
    if ! kill -0 "$PERF_PID" 2>/dev/null; then
        log_err "perf record died immediately. Last output:"
        tail -20 "$out_dir/perf-record.log" >&2 || true
        PERF_PID=""
        return 1
    fi
    log_ok "perf record PID: $PERF_PID → $out_dir/perf.data"
}

# Stop the in-flight `perf record`, waiting for it to flush perf.data cleanly.
# perf reacts to SIGINT by writing the trailing buffer and exiting 0.
stop_perf_record() {
    if [ -z "${PERF_PID:-}" ]; then
        return 0
    fi
    if ! kill -0 "$PERF_PID" 2>/dev/null; then
        PERF_PID=""
        return 0
    fi
    log "Stopping perf record (PID $PERF_PID, will flush perf.data)..."
    kill -INT "$PERF_PID" 2>/dev/null || true
    # perf can take a few seconds to flush a large buffer at high RPS.
    local waited=0
    while kill -0 "$PERF_PID" 2>/dev/null && [ "$waited" -lt 30 ]; do
        sleep 1
        waited=$((waited + 1))
    done
    if kill -0 "$PERF_PID" 2>/dev/null; then
        log_err "perf did not exit cleanly within 30s; sending SIGKILL"
        kill -9 "$PERF_PID" 2>/dev/null || true
    fi
    wait "$PERF_PID" 2>/dev/null || true
    PERF_PID=""
    log_ok "perf record stopped"
}

# Build human-readable perf report + flamegraph from perf.data.
# Idempotent: safe to call even if perf.data is missing (just skips).
generate_perf_report() {
    local out_dir="$1"
    local perf_data="$out_dir/perf.data"

    if [ ! -f "$perf_data" ] || [ ! -s "$perf_data" ]; then
        log_err "No perf.data captured (file missing or empty), skipping report"
        return 1
    fi

    log "Generating perf report → perf-report.txt"
    # --no-children: show self-cost per symbol (better for spotting hot leaves);
    #                the call-graph view is in flamegraph.svg which is much
    #                more useful for that than perf report's text tree.
    perf report --stdio --no-children --input="$perf_data" \
        > "$out_dir/perf-report.txt" 2> "$out_dir/perf-report.err" \
        || log_err "perf report failed (see perf-report.err)"

    log "Generating top-50 hot symbols → perf-top-symbols.txt"
    {
        echo "# Top 50 hot symbols (self-time) during HTTP soak"
        echo "# perf.data: $perf_data"
        echo "# generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo ""
        perf report --stdio --no-children --sort=symbol --input="$perf_data" 2>/dev/null \
            | awk '/^#/ { next } NF >= 3 { print }' \
            | head -50
    } > "$out_dir/perf-top-symbols.txt" 2>&1 || true

    log "Generating flamegraph → flamegraph.svg"
    local fg_svg="$out_dir/flamegraph.svg"
    if command -v inferno-collapse-perf &>/dev/null && command -v inferno-flamegraph &>/dev/null; then
        # inferno is the Rust port of Brendan Gregg's flamegraph tools; ~10x
        # faster than the perl scripts and produces an identical interactive SVG.
        perf script --input="$perf_data" 2>"$out_dir/perf-script.err" \
            | inferno-collapse-perf 2>>"$out_dir/perf-script.err" \
            | inferno-flamegraph \
                --title "HyperbyteDB server — HTTP soak (${RPS} req/s × ${DURATION}, ${POINTS_PER_REQUEST} pts/req, ${WRITE_FORMAT})" \
                --subtitle "perf record -F ${PERF_FREQ} --call-graph fp" \
                > "$fg_svg" 2>>"$out_dir/perf-script.err" \
            || { log_err "inferno flamegraph generation failed"; return 1; }
    elif command -v flamegraph &>/dev/null; then
        # cargo-flamegraph's standalone binary takes perf.data directly.
        flamegraph --perfdata "$perf_data" -o "$fg_svg" \
            &> "$out_dir/flamegraph.log" \
            || { log_err "flamegraph generation failed (see flamegraph.log)"; return 1; }
    else
        log_err "No flamegraph generator available (should have been caught by preflight)"
        return 1
    fi

    log_ok "perf artifacts: perf-report.txt, perf-top-symbols.txt, flamegraph.svg"
    return 0
}

# ─────────────────────────────────────────────────────────────────────────────
# Load test
# ─────────────────────────────────────────────────────────────────────────────

run_load_test() {
    local out_dir="$LOG_DIR/$TIMESTAMP"
    local capture_perf=0

    if [[ "$RUN_FLAMEGRAPH" == "1" ]]; then
        if [[ "$MODE" != "single" ]]; then
            log_err "RUN_FLAMEGRAPH=1 only supported in single mode (need local server PID); skipping perf capture"
        elif [ ! -f "$SERVER_PID_FILE" ]; then
            log_err "RUN_FLAMEGRAPH=1 but no server PID file; skipping perf capture"
        else
            local server_pid
            server_pid="$(cat "$SERVER_PID_FILE")"
            if start_perf_record "$server_pid" "$out_dir"; then
                capture_perf=1
            fi
        fi
    fi

    log "Running k6 load test ($RPS req/s, $DURATION, mode=$MODE) against $K6_TARGET_HOST:$TARGET_PORT..."
    log "  Points per request: $POINTS_PER_REQUEST"
    log "  Write format: $WRITE_FORMAT"
    [[ "$capture_perf" == "1" ]] && log "  perf capture: ON (server PID $(cat "$SERVER_PID_FILE"), ${PERF_FREQ}Hz)"

    TARGET_HOST="$K6_TARGET_HOST" \
    TARGET_PORT="$TARGET_PORT" \
    RPS="$RPS" \
    DURATION="$DURATION" \
    POINTS_PER_REQUEST="$POINTS_PER_REQUEST" \
    WRITE_FORMAT="$WRITE_FORMAT" \
    CLUSTER_NODES="$CLUSTER_NODES" \
    TEST_MODE="$MODE" \
        k6 run \
        "$SCRIPT_DIR/load.js" \
        | tee "$out_dir/load.log"

    if [[ "$capture_perf" == "1" ]]; then
        stop_perf_record
        generate_perf_report "$out_dir" || true
    fi
}

run_query_test() {
    local iterations="${QUERY_ITERATIONS:-30}"
    log "Running query latency test ($iterations iterations) against $K6_TARGET_HOST:$TARGET_PORT..."

    TARGET_HOST="$K6_TARGET_HOST" \
    TARGET_PORT="$TARGET_PORT" \
    QUERY_ITERATIONS="$iterations" \
    CLUSTER_NODES="$CLUSTER_NODES" \
    TEST_MODE="$MODE" \
        k6 run \
        "$SCRIPT_DIR/query.js" \
        | tee "$LOG_DIR/$TIMESTAMP/query.log"
}

summarise_load_results() {
    log_file="$1"
    if [ ! -f "$log_file" ]; then
        log_err "Log file not found: $log_file"
        return 1
    fi

    req_s=$(sed -n 's/.*http_reqs[^:]*:[[:space:]]*[0-9]*[[:space:]]*\([0-9.]*\)\/s.*/\1/p' "$log_file" | head -1)
    pts_s=$(sed -n 's/.*points[^:]*:[[:space:]]*[0-9]*[[:space:]]*\([0-9.]*\)\/s.*/\1/p' "$log_file" | head -1)
    avg_ms=$(sed -n 's/.*http_req_duration[^=]*avg=\([0-9.]*\)ms.*/\1/p' "$log_file" | head -1)
    success_pct=$(sed -n 's/.*checks_succeeded[^:]*:[[:space:]]*\([0-9.]*\)%.*/\1/p' "$log_file" | head -1)

    if [ -z "$req_s" ]; then
        log_err "Could not parse k6 results from $log_file"
        return 1
    fi

    printf '\n'
    printf '\033[1;36m%s\033[0m\n' '┌─ Load Test Summary ─────────────────────────────────'
    printf '  \033[33mRequests/sec\033[0m   %s req/s\n' "${req_s:-n/a}"
    printf '  \033[33mPoints/sec\033[0m     %s pts/s\n' "${pts_s:-n/a}"
    printf '  \033[33mAvg latency\033[0m    %s ms\n' "${avg_ms:-n/a}"
    printf '  \033[33mSuccess rate\033[0m   %s%%\n' "${success_pct:-n/a}"
    printf '\033[1;36m%s\033[0m\n' '└────────────────────────────────────────────────────'
    printf '\n'
}

summarise_query_results() {
    log_file="$1"
    if [ ! -f "$log_file" ]; then
        log_err "Log file not found: $log_file"
        return 1
    fi

    meta_avg=$(sed -n 's/.*query_metadata_latency[^=]*avg=\([0-9.]*\)ms.*/\1/p' "$log_file" | head -1)
    meta_p95=$(sed -n 's/.*query_metadata_latency[^=]*p(95)=\([0-9.]*\)ms.*/\1/p' "$log_file" | head -1)
    point_avg=$(sed -n 's/.*query_point_latency[^=]*avg=\([0-9.]*\)ms.*/\1/p' "$log_file" | head -1)
    point_p95=$(sed -n 's/.*query_point_latency[^=]*p(95)=\([0-9.]*\)ms.*/\1/p' "$log_file" | head -1)
    agg_avg=$(sed -n 's/.*query_aggregate_latency[^=]*avg=\([0-9.]*\)ms.*/\1/p' "$log_file" | head -1)
    agg_p95=$(sed -n 's/.*query_aggregate_latency[^=]*p(95)=\([0-9.]*\)ms.*/\1/p' "$log_file" | head -1)
    filt_avg=$(sed -n 's/.*query_filtered_latency[^=]*avg=\([0-9.]*\)ms.*/\1/p' "$log_file" | head -1)
    filt_p95=$(sed -n 's/.*query_filtered_latency[^=]*p(95)=\([0-9.]*\)ms.*/\1/p' "$log_file" | head -1)

    checks=$(sed -n 's/.*checks_succeeded[^:]*:[[:space:]]*\([0-9.]*\)%.*/\1/p' "$log_file" | head -1)

    printf '\n'
    printf '\033[1;35m%s\033[0m\n' '┌─ Query Latency Summary ─────────────────────────────'
    printf '  \033[33m%-22s\033[0m  avg %-8s  p95 %-8s\n' "Metadata (SHOW ...)"  "${meta_avg:-n/a}ms" "${meta_p95:-n/a}ms"
    printf '  \033[33m%-22s\033[0m  avg %-8s  p95 %-8s\n' "Point reads (SELECT)" "${point_avg:-n/a}ms" "${point_p95:-n/a}ms"
    printf '  \033[33m%-22s\033[0m  avg %-8s  p95 %-8s\n' "Aggregates"           "${agg_avg:-n/a}ms"  "${agg_p95:-n/a}ms"
    printf '  \033[33m%-22s\033[0m  avg %-8s  p95 %-8s\n' "Filtered queries"     "${filt_avg:-n/a}ms" "${filt_p95:-n/a}ms"
    printf '  \033[33m%-22s\033[0m  %s%%\n'               "Checks passed"        "${checks:-n/a}"
    printf '\033[1;35m%s\033[0m\n' '└────────────────────────────────────────────────────'
    printf '\n'
}

# ─────────────────────────────────────────────────────────────────────────────
# Criterion benches + consolidated summary report
# ─────────────────────────────────────────────────────────────────────────────

run_criterion_benches() {
	local ilp_log="$LOG_DIR/$TIMESTAMP/bench_ingestion_line_protocol.log"
	local col_log="$LOG_DIR/$TIMESTAMP/bench_ingestion_columnar.log"
	local query_log="$LOG_DIR/$TIMESTAMP/bench_query_fixed_dataset.log"

	log "Running Criterion bench: ingestion_line_protocol → $(basename "$ilp_log")"
	(
		cd "$PROJECT_ROOT"
		cargo bench --bench ingestion_line_protocol -- --noplot
	) 2>&1 | tee "$ilp_log" || {
		log_err "ingestion_line_protocol bench failed"
		return 1
	}

	log "Running Criterion bench: ingestion_columnar → $(basename "$col_log")"
	(
		cd "$PROJECT_ROOT"
		cargo bench --bench ingestion_columnar -- --noplot
	) 2>&1 | tee "$col_log" || {
		log_err "ingestion_columnar bench failed"
		return 1
	}

	log "Running Criterion bench: query_fixed_dataset → $(basename "$query_log")"
	(
		cd "$PROJECT_ROOT"
		cargo bench --bench query_fixed_dataset -- --noplot
	) 2>&1 | tee "$query_log" || {
		log_err "query_fixed_dataset bench failed"
		return 1
	}
	return 0
}

# Pairs Criterion "group/function" lines with the following "time:" line.
# Emits each Criterion bench id with the following time: and (if present) thrpt: lines.
extract_criterion_times() {
	local f="$1"
	[ -f "$f" ] || return 0
	awk '
		/^[a-zA-Z0-9_]+\/[a-zA-Z0-9_]+$/ { bench=$0; next }
		bench != "" && /^[[:space:]]+time:/ {
			sub(/^[[:space:]]+/, "")
			printf "%s  %s\n", bench, $0
			if (getline nl > 0) {
				if (nl ~ /^[[:space:]]+thrpt:/) {
					sub(/^[[:space:]]+/, "", nl)
					printf "      %s\n", nl
				} else if (nl ~ /^[a-zA-Z0-9_]+\/[a-zA-Z0-9_]+$/) {
					bench=nl
					next
				}
			}
			bench=""
			next
		}
	' "$f"
}

write_summary_report() {
	local out="$LOG_DIR/$TIMESTAMP/summary.log"
	local load_log="$LOG_DIR/$TIMESTAMP/load.log"
	local query_log="$LOG_DIR/$TIMESTAMP/query.log"
	local ilp_log="$LOG_DIR/$TIMESTAMP/bench_ingestion_line_protocol.log"
	local col_log="$LOG_DIR/$TIMESTAMP/bench_ingestion_columnar.log"
	local bench_query_log="$LOG_DIR/$TIMESTAMP/bench_query_fixed_dataset.log"

	load_req_s="$(sed -n 's/.*http_reqs[^:]*:[[:space:]]*[0-9]*[[:space:]]*\([0-9.]*\)\/s.*/\1/p' "$load_log" 2>/dev/null | head -1)"
	load_pts_s="$(sed -n 's/.*points[^:]*:[[:space:]]*[0-9]*[[:space:]]*\([0-9.]*\)\/s.*/\1/p' "$load_log" 2>/dev/null | head -1)"
	load_http="$(grep -F 'http_req_duration' "$load_log" 2>/dev/null | head -1 | sed 's/^[[:space:]]*//')"
	load_checks="$(grep -F 'checks_succeeded' "$load_log" 2>/dev/null | head -1 | sed 's/^[[:space:]]*//')"

	q_meta="$(grep -F 'query_metadata_latency' "$query_log" 2>/dev/null | grep '\.\.' | head -1 | sed 's/^[[:space:]]*//')"
	q_point="$(grep -F 'query_point_latency' "$query_log" 2>/dev/null | grep '\.\.' | head -1 | sed 's/^[[:space:]]*//')"
	q_agg="$(grep -F 'query_aggregate_latency' "$query_log" 2>/dev/null | grep '\.\.' | head -1 | sed 's/^[[:space:]]*//')"
	q_filt="$(grep -F 'query_filtered_latency' "$query_log" 2>/dev/null | grep '\.\.' | head -1 | sed 's/^[[:space:]]*//')"

	# Extract peak Criterion throughput from a bench log, optionally filtered
	# to groups whose name matches a "kind":
	#   - "single"     : groups NOT ending in `_concurrent` (sequential b.iter)
	#   - "concurrent" : groups ending in `_concurrent` (parallel fanout)
	# Emits one line: "<rate elem/s> <bench_id>" for the peak (or empty).
	extract_peak_thrpt() {
		local file="$1"
		local kind="$2"
		[ -f "$file" ] || return 0
		awk -v kind="$kind" '
			/^[a-zA-Z0-9_]+\/[a-zA-Z0-9_]+$/ {
				current_id = $0
				split(current_id, parts, "/")
				is_concurrent = (parts[1] ~ /_concurrent$/)
				if (kind == "concurrent")  match_active = is_concurrent
				else if (kind == "single") match_active = !is_concurrent
				else                       match_active = 1
				next
			}
			match_active && /^[[:space:]]+thrpt:/ {
				best = 0; rate = 0
				for (i = 1; i <= NF; ++i) {
					if ($i ~ /^[0-9.]+$/) rate = $i + 0
					else if ($i ~ /Kelem\/s/) { v = rate*1000;       if (v > best) best = v }
					else if ($i ~ /Melem\/s/) { v = rate*1000000;    if (v > best) best = v }
					else if ($i ~ /Gelem\/s/) { v = rate*1000000000; if (v > best) best = v }
				}
				if (best > 0) printf "%.0f %s\n", best, current_id
				match_active = 0
			}
		' "$file" | sort -nr | head -1
	}

	ilp_single_line=$(extract_peak_thrpt "$ilp_log" single)
	col_single_line=$(extract_peak_thrpt "$col_log" single)
	ilp_conc_line=$(extract_peak_thrpt "$ilp_log" concurrent)
	col_conc_line=$(extract_peak_thrpt "$col_log" concurrent)

	ilp_single_thrpt=$(echo "$ilp_single_line" | awk '{print $1}')
	ilp_single_id=$(echo "$ilp_single_line"    | awk '{print $2}')
	col_single_thrpt=$(echo "$col_single_line" | awk '{print $1}')
	col_single_id=$(echo "$col_single_line"    | awk '{print $2}')
	ilp_conc_thrpt=$(echo "$ilp_conc_line" | awk '{print $1}')
	ilp_conc_id=$(echo "$ilp_conc_line"    | awk '{print $2}')
	col_conc_thrpt=$(echo "$col_conc_line" | awk '{print $1}')
	col_conc_id=$(echo "$col_conc_line"    | awk '{print $2}')

	# Backwards-compat: keep the legacy "peak throughput" variables populated
	# with the higher of the two so existing dashboards / grep rules still
	# find a non-empty value.
	ilp_thrpt="${ilp_conc_thrpt:-$ilp_single_thrpt}"
	col_thrpt="${col_conc_thrpt:-$col_single_thrpt}"

	# Per-group concurrent scaling factors (cMAX-aggregate / c1-aggregate),
	# for **within the same Criterion group**. This is the only meaningful
	# scaling number — comparing the c12 aggregate of one group against
	# the single-core peak of an unrelated cache-hit fast-path bench
	# (the legacy `compute_scale` behaviour) is comparing apples to
	# oranges and routinely mislabels healthy 5x scaling as "0.3x".
	#
	# Output format: one line per `_concurrent` group, of the form
	#   <group> c1=<rate> cMAX=<rate> n=<MAX> ratio=<cMAX/c1>
	extract_concurrent_scaling() {
		local f="$1"
		[ -f "$f" ] || return 0
		awk '
			# Detect a Criterion bench id with concurrency suffix, e.g.
			# "columnar_wal_concurrent/c12_batch1000".
			/^[a-zA-Z0-9_]+\/c[0-9]+_batch[0-9]+$/ {
				split($0, parts, "/")
				cur_group = parts[1]
				if (cur_group !~ /_concurrent$/) { cur_group = ""; next }
				fn = parts[2]
				sub(/^c/, "", fn)
				sub(/_batch.*/, "", fn)
				cur_n = fn + 0
				next
			}
			cur_group != "" && /^[[:space:]]+thrpt:/ {
				# Parse "thrpt: [V1 U1 V2 U2 V3 U3]" and take the median
				# value (V2 with U2). Criterion always emits 3 values
				# (low / median / high). We strip the surrounding `[]`
				# from each field before testing numeric-ness so the
				# leading `[V1` token actually parses as a number.
				delete vals; delete units
				n_vals = 0
				for (i = 1; i <= NF; ++i) {
					tok = $i
					gsub(/[\[\]]/, "", tok)
					if (tok ~ /^[0-9.]+$/) {
						vals[++n_vals] = tok + 0
					} else if (tok ~ /Kelem\/s/) {
						units[n_vals] = 1000
					} else if (tok ~ /Melem\/s/) {
						units[n_vals] = 1000000
					} else if (tok ~ /Gelem\/s/) {
						units[n_vals] = 1000000000
					} else if (tok ~ /elem\/s/) {
						units[n_vals] = 1
					}
				}
				# Index 2 is the median for the standard 3-tuple, fall
				# back to the only value present if Criterion ever emits
				# a non-throughput line shape.
				idx = (n_vals >= 2 ? 2 : n_vals)
				if (idx >= 1 && units[idx] > 0) {
					rate = vals[idx] * units[idx]
					thrpt_at[cur_group "@" cur_n] = rate
					if (min_n[cur_group] == "" || cur_n < min_n[cur_group]) min_n[cur_group] = cur_n
					if (max_n[cur_group] == "" || cur_n > max_n[cur_group]) max_n[cur_group] = cur_n
					groups[cur_group] = 1
				}
				cur_group = ""
			}
			END {
				for (g in groups) {
					c1   = thrpt_at[g "@" min_n[g]]
					cmax = thrpt_at[g "@" max_n[g]]
					if (c1 > 0 && cmax > 0) {
						printf "%s c1=%.0f cMAX=%.0f n=%d ratio=%.2f\n", g, c1, cmax, max_n[g], cmax/c1
					}
				}
			}
		' "$f" | sort
	}

	ilp_scaling_lines="$(extract_concurrent_scaling "$ilp_log")"
	col_scaling_lines="$(extract_concurrent_scaling "$col_log")"

	# Pick the WAL group's ratio as the "headline" scaling factor — it's
	# the production-relevant story (RocksDB write path coordination).
	# Prefer the group-commit batched bench when present (that's the
	# code path actual ingestion takes); fall back to the direct
	# `*_wal_concurrent` bench otherwise.
	pick_ratio() {
		local lines="$1" group_substr="$2"
		echo "$lines" | awk -v needle="$group_substr" '
			$1 ~ needle {
				for (i = 1; i <= NF; ++i) {
					if ($i ~ /^ratio=/) { sub(/^ratio=/, "", $i); print $i; exit }
				}
			}
		'
	}
	ilp_wal_ratio=$(pick_ratio "$ilp_scaling_lines" "wal_batched_concurrent$")
	[ -z "$ilp_wal_ratio" ] && ilp_wal_ratio=$(pick_ratio "$ilp_scaling_lines" "wal_concurrent$")
	col_wal_ratio=$(pick_ratio "$col_scaling_lines" "wal_batched_concurrent$")
	[ -z "$col_wal_ratio" ] && col_wal_ratio=$(pick_ratio "$col_scaling_lines" "wal_concurrent$")

	# Compose summary out
	{
		echo "=================================================================================="
		echo "HyperbyteDB load & benchmark summary  (${TIMESTAMP})"
		echo "Target: ${K6_TARGET_HOST}:${TARGET_PORT}  Mode: ${MODE}  RPS: ${RPS}  Duration: ${DURATION}  Points/req: ${POINTS_PER_REQUEST}  Write format: ${WRITE_FORMAT}"
		echo "=================================================================================="
		echo ""
		echo "## HTTP write (k6 → POST /write)"
		echo "  http_reqs:          ${load_req_s:-n/a} req/s"
		echo "  points:             ${load_pts_s:-n/a} pts/s"
		echo "  ${load_http:-http_req_duration: n/a}"
		echo "  ${load_checks:-checks_succeeded: n/a}"
		echo ""
		echo "## Query response times (k6 → GET /query)"
		echo "  ${q_meta:-query_metadata_latency: n/a}"
		echo "  ${q_point:-query_point_latency: n/a}"
		echo "  ${q_agg:-query_aggregate_latency: n/a}"
		echo "  ${q_filt:-query_filtered_latency: n/a}"
		echo ""
		echo "## ILP benches (Criterion: ingestion_line_protocol, 1000 points / iter)"
		echo "  (per bench: time estimate, then throughput elements/s)"
		if [[ "${RUN_BENCHES:-0}" != "1" ]]; then
			echo "  (skipped — set RUN_BENCHES=1 to run)"
		elif [ -f "$ilp_log" ]; then
			extract_criterion_times "$ilp_log" | sed 's/^/  /' || echo "  (no timing lines parsed)"
		else
			echo "  (not run — log missing or bench failed before write)"
		fi
		echo ""
		echo "## Columnar benches (Criterion: ingestion_columnar)"
		echo "  (per bench: time estimate, then throughput elements/s)"
		if [[ "${RUN_BENCHES:-0}" != "1" ]]; then
			echo "  (skipped — set RUN_BENCHES=1 to run)"
		elif [ -f "$col_log" ]; then
			extract_criterion_times "$col_log" | sed 's/^/  /' || echo "  (no timing lines parsed)"
		else
			echo "  (not run — log missing or bench failed before write)"
		fi
		echo ""
		echo "## Query benches (Criterion: query_fixed_dataset, BENCH_DATASET=${BENCH_DATASET:-small})"
		echo "  (per bench: time estimate, then throughput elements/s)"
		if [[ "${RUN_BENCHES:-0}" != "1" ]]; then
			echo "  (skipped — set RUN_BENCHES=1 to run)"
		elif [ -f "$bench_query_log" ]; then
			extract_criterion_times "$bench_query_log" | sed 's/^/  /' || echo "  (no timing lines parsed)"
		else
			echo "  (not run — log missing or bench failed before write)"
		fi
		echo ""
		echo "## perf / flamegraph (HTTP soak)"
		local perf_data="$LOG_DIR/$TIMESTAMP/perf.data"
		local perf_top="$LOG_DIR/$TIMESTAMP/perf-top-symbols.txt"
		local fg_svg="$LOG_DIR/$TIMESTAMP/flamegraph.svg"
		if [[ "${RUN_FLAMEGRAPH:-0}" != "1" ]]; then
			echo "  (disabled — set RUN_FLAMEGRAPH=1 to capture, requires single mode + perf + flamegraph/inferno)"
		elif [ ! -f "$perf_data" ]; then
			echo "  (no perf.data captured — see perf-record.log if RUN_FLAMEGRAPH=1 was set)"
		else
			local perf_size; perf_size="$(du -h "$perf_data" 2>/dev/null | awk '{print $1}')"
			local fg_size;   fg_size="$(du -h "$fg_svg" 2>/dev/null | awk '{print $1}')"
			echo "  perf.data:           ${perf_size:-?} ($(basename "$perf_data"))"
			echo "  flamegraph.svg:      ${fg_size:-not generated} ($(basename "$fg_svg"))"
			echo "  Top hot symbols (perf-top-symbols.txt):"
			if [ -f "$perf_top" ]; then
				# Drop comment lines + blanks, indent first 10 entries.
				grep -vE '^#|^[[:space:]]*$' "$perf_top" 2>/dev/null \
					| head -10 \
					| sed 's/^/    /'
			else
				echo "    (perf-top-symbols.txt missing)"
			fi
			echo "  View flamegraph:     xdg-open '$fg_svg'  # or scp to a workstation"
		fi
		echo ""
		echo "=================================================================================="
		echo "Artifacts: load.log, query.log, server.log (single mode), summary.log (this file),"
		echo "  bench_ingestion_line_protocol.log, bench_ingestion_columnar.log, bench_query_fixed_dataset.log"
		[[ "${RUN_FLAMEGRAPH:-0}" == "1" ]] && \
			echo "  perf.data, perf-report.txt, perf-top-symbols.txt, flamegraph.svg, perf-record.log"
		echo "=================================================================================="
		echo ""
		# In-group scaling factor: cMAX-aggregate / c1-aggregate, computed
		# per `_concurrent` group via `extract_concurrent_scaling`. This
		# replaces the legacy `compute_scale` (concurrent_peak /
		# single_core_peak), which compared unrelated benches and routinely
		# mislabelled healthy 5x scaling as "0.3x" because the single-core
		# peak was a sub-µs cache-hit no-op.
		fmt_ratio() {
			local r="$1"
			if [ -n "$r" ]; then
				awk -v r="$r" 'BEGIN { printf "%.1fx", r }'
			else
				echo "n/a"
			fi
		}
		ilp_wal_scale=$(fmt_ratio "$ilp_wal_ratio")
		col_wal_scale=$(fmt_ratio "$col_wal_ratio")

		echo "########## Per-group concurrent scaling (cMAX / c1, same group) ##########"
		echo "## ILP benches"
		if [ -n "$ilp_scaling_lines" ]; then
			echo "$ilp_scaling_lines" | sed 's/^/  /'
		else
			echo "  (no concurrent groups parsed — bench skipped or malformed)"
		fi
		echo "## Columnar benches"
		if [ -n "$col_scaling_lines" ]; then
			echo "$col_scaling_lines" | sed 's/^/  /'
		else
			echo "  (no concurrent groups parsed — bench skipped or malformed)"
		fi
		echo "###########################################################################"
		echo ""

		echo "########## Summary (key metrics) ##########"
		echo "HTTP Throughput:                    ${load_req_s:-n/a} req/s"
		echo "Point Throughput:                   ${load_pts_s:-n/a} pts/s"
		echo "ILP single-core peak throughput:    ${ilp_single_thrpt:-n/a} elem/s  (${ilp_single_id:-n/a})"
		echo "ILP concurrent peak throughput:     ${ilp_conc_thrpt:-n/a} elem/s  (${ilp_conc_id:-n/a})"
		echo "ILP WAL scaling factor (cMAX/c1):   ${ilp_wal_scale}"
		echo "Columnar single-core peak:          ${col_single_thrpt:-n/a} elem/s  (${col_single_id:-n/a})"
		echo "Columnar concurrent peak:           ${col_conc_thrpt:-n/a} elem/s  (${col_conc_id:-n/a})"
		echo "Columnar WAL scaling factor:        ${col_wal_scale}"
		echo "###########################################"
	} > "$out"

	log_ok "Summary report: $out"
	cat "$out"
	{
		echo ""
		echo "────────── copied from summary.log ──────────"
		cat "$out"
	} >> "$load_log"
}

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

main() {
    trap cleanup EXIT INT TERM

    if ! command -v k6 &>/dev/null; then
        log_err "k6 is not installed. Install it: https://grafana.com/docs/k6/latest/set-up/install-k6/"
        exit 1
    fi

    # Fail fast on perf prerequisites before doing the (slow) frame-pointer rebuild.
    if [[ "$RUN_FLAMEGRAPH" == "1" ]]; then
        if [[ "$MODE" != "single" ]]; then
            log_err "RUN_FLAMEGRAPH=1 requires MODE=single (need a local server PID)"
            exit 1
        fi
        preflight_perf || exit 1
        log_ok "perf preflight passed; will record server with frame pointers"
    fi

    mkdir -p "$LOG_DIR/$TIMESTAMP"

    log "HyperbyteDB Load Test  [mode=$MODE]"
    log "  Log directory: $LOG_DIR/$TIMESTAMP"
    log "  Target: ${K6_TARGET_HOST}:$TARGET_PORT"
    [[ "$RUN_FLAMEGRAPH" == "1" ]] && log "  perf flamegraph: ENABLED (frequency=${PERF_FREQ}Hz)"

    case "$MODE" in
        single)
            # pre_build_server
            # start_server
            wait_for_server || exit 1
            ;;
        cluster)
            if [[ -z "$TARGET_HOST" ]]; then
                log_err "Cluster mode requires a target host."
                log_err "Usage: ./load.sh cluster <host> [port] [rps] [duration] [points]"
                exit 1
            fi
            [[ -n "$CLUSTER_NODES" ]] && log "  Cluster nodes: $CLUSTER_NODES"
            wait_for_cluster || exit 1
            ;;
        *)
            log_err "Unknown mode '$MODE'. Use 'single' or 'cluster'."
            exit 1
            ;;
    esac

    setup_databases
    run_load_test
    summarise_load_results "$LOG_DIR/$TIMESTAMP/load.log"

    log "Waiting for WAL flush before query test..."
    wait_for_flush

    # run_query_test
    # summarise_query_results "$LOG_DIR/$TIMESTAMP/query.log"

    # if [[ "${RUN_BENCHES}" == "1" ]]; then
    #     if command -v cargo &>/dev/null; then
    #         run_criterion_benches || log_err "One or more Criterion benches failed; see bench_*.log"
    #     else
    #         log "RUN_BENCHES=1 but cargo not found; skipping Criterion benches"
    #     fi
    # else
    #     log "RUN_BENCHES!=1 — skipping Criterion benches"
    # fi

    write_summary_report

    if [[ "$MODE" == "single" ]]; then
        stop_server
    fi

    log "Allowing flush to complete..."
    sleep 3
    log_ok "Done. Results in $LOG_DIR/$TIMESTAMP/ (see summary.log)"
}

main
