#!/usr/bin/env bash
# Clone hyperbyte-cloud/chdb-rust next to the hyperbytedb checkout.
#
# hyperbytedb/hyperbytedb/Cargo.toml uses path = "../../chdb-rust", which resolves
# to a sibling of GITHUB_WORKSPACE (the hyperbytedb repo root):
#   <runner-work>/hyperbytedb/hyperbytedb   <- GITHUB_WORKSPACE
#   <runner-work>/chdb-rust                 <- this script
set -euo pipefail

: "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is required}"

CHDB_RUST_REPO="${CHDB_RUST_REPO:-https://github.com/hyperbyte-cloud/chdb-rust.git}"
CHDB_RUST_REF="${CHDB_RUST_REF:-feat_arrow_insert}"
DEST="$(dirname "${GITHUB_WORKSPACE}")/chdb-rust"

echo "Checking out ${CHDB_RUST_REPO} @ ${CHDB_RUST_REF} -> ${DEST}"

if [[ -d "${DEST}/.git" ]]; then
  git -C "${DEST}" fetch --depth 1 origin "${CHDB_RUST_REF}"
  git -C "${DEST}" checkout FETCH_HEAD
else
  git clone --depth 1 --branch "${CHDB_RUST_REF}" "${CHDB_RUST_REPO}" "${DEST}"
fi

echo "chdb-rust HEAD: $(git -C "${DEST}" rev-parse --short HEAD)"
