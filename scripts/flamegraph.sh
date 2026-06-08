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
# HyperbyteDB flamegraph capture
#
# Builds the server with frame pointers, drives a sustained HTTP write soak
# via k6, samples the server with `perf record`, then emits:
#
#   log/load/<ts>/perf.data            — raw perf samples
#   log/load/<ts>/perf-report.txt      — perf report --stdio (full)
#   log/load/<ts>/perf-top-symbols.txt — top-50 hot symbols by self-time
#   log/load/<ts>/flamegraph.svg       — interactive flamegraph (open in browser)
#   log/load/<ts>/load.log             — k6 load test output
#   log/load/<ts>/server.log           — server stdout/stderr
#   log/load/<ts>/summary.log          — consolidated summary
#
# This is a thin wrapper around `scripts/load.sh single` that:
#   - forces RUN_FLAMEGRAPH=1 (perf capture during the HTTP soak)
#   - skips the Criterion bench phase by default (RUN_BENCHES=0) so the run
#     finishes quickly and the flamegraph reflects the steady-state hot path,
#     not the noise of cargo bench
#   - skips the post-soak query phase (kept for symmetry with load.sh; queries
#     still run because they're cheap, but you can disable with QUERY=0)
#   - defaults to a longer, harder soak than load.sh's defaults so the perf
#     samples actually represent the hot path under sustained load
#
# Usage:
#   scripts/flamegraph.sh                       # 60s soak @ 500 RPS × 1000 pts/req
#   scripts/flamegraph.sh 1000                  # 60s soak @ 1000 RPS
#   scripts/flamegraph.sh 500 2m 1000 line      # 2-minute soak, 1000 pts/req, line protocol
#   scripts/flamegraph.sh 500 2m 1000 msgpack   # ditto, columnar msgpack format
#
# Positional args: [rps] [duration] [points_per_request] [write_format]
#
# Environment:
#   PERF_FREQ          99 (default) sampling Hz; raise for finer resolution.
#   RUN_BENCHES        0 (default here) — set to 1 to also run cargo bench.
#   POINTS_PER_REQUEST overrides positional arg.
#   WRITE_FORMAT       line (default) | msgpack
#
# Requirements:
#   - perf            (apt-get install -y linux-perf  OR  linux-tools-$(uname -r))
#   - kernel.perf_event_paranoid <= 1   (or run as root)
#   - flamegraph generator: one of
#         cargo install flamegraph     # 'flamegraph' standalone binary
#         cargo install inferno        # inferno-collapse-perf + inferno-flamegraph (faster)
#   - k6              (https://grafana.com/docs/k6/latest/set-up/install-k6/)
#

set -eu

readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Sensible "soak that produces a meaningful flamegraph" defaults. These are
# higher than load.sh's defaults because a 30s low-RPS run wastes most of its
# perf samples on k6 ramp-up and on idle epoll waits in tokio. Override with
# positional args or env vars.
RPS="${RPS:-${1:-500}}"
DURATION="${2:-60s}"
POINTS_PER_REQUEST="${POINTS_PER_REQUEST:-${3:-1000}}"
WRITE_FORMAT="${WRITE_FORMAT:-${4:-line}}"

export RPS
export DURATION
export POINTS_PER_REQUEST
export WRITE_FORMAT
export RUN_FLAMEGRAPH=1
export RUN_BENCHES="${RUN_BENCHES:-0}"
export PERF_FREQ="${PERF_FREQ:-99}"

cat <<EOF
[flamegraph.sh] Capturing HyperbyteDB hot paths under sustained HTTP write load.
  RPS:                $RPS
  Duration:           $DURATION
  Points per request: $POINTS_PER_REQUEST
  Write format:       $WRITE_FORMAT
  perf frequency:     ${PERF_FREQ}Hz
  Cargo benches:      $([[ "$RUN_BENCHES" == "1" ]] && echo "yes (RUN_BENCHES=1)" || echo "skipped (RUN_BENCHES=0)")

Artifacts will be written under log/load/<timestamp>/.
EOF

# Hand off to load.sh in single mode. It does the heavy lifting:
# build → start server → start perf → run k6 → stop perf → generate report → flamegraph.
# Positional args after the mode are: host port (we let load.sh fill defaults).
exec "$SCRIPT_DIR/load.sh" single
