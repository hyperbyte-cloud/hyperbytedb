#!/usr/bin/env bash
# Run a command inside rust:bookworm via Docker.
#
# ARC runner pods do not honor job-level `container:` directives unless the
# scale set is configured for container jobs. With containerMode.type=dind, the
# runner pod exposes Docker so we can invoke the same images explicitly.
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: run-in-rust-container.sh <command>" >&2
  exit 1
fi

command=$1
RUST_IMAGE="${RUST_IMAGE:-rust:bookworm}"
WORKSPACE="${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is required}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found on PATH." >&2
  echo "Configure the ARC runner scale set with containerMode.type=dind." >&2
  exit 1
fi

docker_args=(
  --rm
  -v "${WORKSPACE}:${WORKSPACE}"
  -w "${WORKSPACE}"
  -e "CARGO_TERM_COLOR=${CARGO_TERM_COLOR:-always}"
  -e "RUSTFLAGS=${RUSTFLAGS:-}"
  -e "CARGO_INCREMENTAL=${CARGO_INCREMENTAL:-0}"
)

if [ -n "${CARGO_HOME:-}" ]; then
  docker_args+=(-e "CARGO_HOME=${CARGO_HOME}" -v "${CARGO_HOME}:${CARGO_HOME}")
fi

if [ -n "${RUNNER_TOOL_CACHE:-}" ]; then
  docker_args+=(-v "${RUNNER_TOOL_CACHE}:${RUNNER_TOOL_CACHE}" -e "RUNNER_TOOL_CACHE=${RUNNER_TOOL_CACHE}")
fi

docker run "${docker_args[@]}" "$RUST_IMAGE" bash -ec "$command"
