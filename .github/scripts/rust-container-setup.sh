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

curl -fsSL https://lib.chdb.io -o /tmp/install-chdb.sh
bash /tmp/install-chdb.sh

# Install sccache so cargo's RUSTC_WRAPPER works inside the container. The
# compiled-object cache lives in the bind-mounted SCCACHE_DIR (see
# run-in-rust-container.sh), so only the static binary is fetched here.
install_sccache() {
  if command -v sccache >/dev/null 2>&1; then
    sccache --version
    return 0
  fi

  local version="v0.8.2"
  local target
  case "$(uname -m)" in
    x86_64) target="x86_64-unknown-linux-musl" ;;
    aarch64 | arm64) target="aarch64-unknown-linux-musl" ;;
    *)
      echo "Unsupported architecture for sccache: $(uname -m)" >&2
      exit 1
      ;;
  esac

  local pkg="sccache-${version}-${target}"
  curl -fsSL "https://github.com/mozilla/sccache/releases/download/${version}/${pkg}.tar.gz" \
    | tar -xz -C /tmp
  install -m 0755 "/tmp/${pkg}/sccache" /usr/local/bin/sccache
  rm -rf "/tmp/${pkg}"
  sccache --version
}

install_sccache
mkdir -p "${SCCACHE_DIR:-/opt/sccache}"
