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
# chdb-rust is checked out as a sibling of GITHUB_WORKSPACE (see checkout-chdb-rust.sh).
# Mount the parent directory so path = "../../chdb-rust" resolves inside the container.
WORKSPACE_PARENT="$(dirname "${WORKSPACE}")"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found on PATH." >&2
  echo "Configure the ARC runner scale set with containerMode.type=dind." >&2
  exit 1
fi

docker_args=(
  --rm
  -v "${WORKSPACE_PARENT}:${WORKSPACE_PARENT}"
  -w "${WORKSPACE}"
  -e "CARGO_TERM_COLOR=${CARGO_TERM_COLOR:-always}"
  -e "RUSTFLAGS=${RUSTFLAGS:-}"
  -e "CARGO_INCREMENTAL=${CARGO_INCREMENTAL:-0}"
)

# Optional emulated platform (e.g. linux/arm64). Used by the release pipeline to
# build aarch64 binaries on the amd64 runner via QEMU/binfmt. Requires
# docker/setup-qemu-action to have registered binfmt handlers first. sccache keys
# objects by target triple, so emulated arm64 builds share the same node-local
# SCCACHE_DIR as the native amd64 builds without collisions.
if [ -n "${RUST_PLATFORM:-}" ]; then
  docker_args+=(--platform "${RUST_PLATFORM}")
fi

if [ -n "${CARGO_HOME:-}" ]; then
  docker_args+=(-e "CARGO_HOME=${CARGO_HOME}" -v "${CARGO_HOME}:${CARGO_HOME}")
fi

# sccache compilation cache. SCCACHE_DIR is a node-local hostPath that persists
# across ephemeral runner pods; bind-mount it into the container and forward the
# sccache settings so cargo's RUSTC_WRAPPER finds a warm cache.
#
# NOTE: `docker run -v` resolves the source path on the *dind daemon's*
# filesystem, so the dind sidecar (not just the runner pod) must expose
# /opt/sccache as a hostPath for the cache to actually persist on the node.
if [ -n "${SCCACHE_DIR:-}" ]; then
  mkdir -p "${SCCACHE_DIR}" 2>/dev/null || true
  docker_args+=(
    -v "${SCCACHE_DIR}:${SCCACHE_DIR}"
    -e "SCCACHE_DIR=${SCCACHE_DIR}"
    -e "RUSTC_WRAPPER=${RUSTC_WRAPPER:-sccache}"
    -e "SCCACHE_CACHE_SIZE=${SCCACHE_CACHE_SIZE:-20G}"
  )
fi

if [ -n "${RUNNER_TOOL_CACHE:-}" ]; then
  docker_args+=(-v "${RUNNER_TOOL_CACHE}:${RUNNER_TOOL_CACHE}" -e "RUNNER_TOOL_CACHE=${RUNNER_TOOL_CACHE}")
fi

docker run "${docker_args[@]}" "$RUST_IMAGE" bash -ec "$command"
