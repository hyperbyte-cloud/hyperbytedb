#!/usr/bin/env bash
# System packages and libchdb for Rust CI inside rust:bookworm.
# Runs as root inside the container, so apt-get and lib.chdb.io work as-is.
set -euo pipefail

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  ca-certificates \
  clang \
  curl \
  git \
  libclang-dev \
  libssl-dev \
  llvm-dev \
  pkg-config

curl -sSL https://lib.chdb.io | bash
