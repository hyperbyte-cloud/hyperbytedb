#!/usr/bin/env bash
# Clone hyperbyte-cloud/chdb-rust next to the hyperbytedb repo root.
#
# hyperbytedb/hyperbytedb/Cargo.toml uses path = "../../chdb-rust", which resolves
# to a sibling of the hyperbytedb checkout:
#   <parent>/hyperbytedb/hyperbytedb
#   <parent>/chdb-rust
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE="${GITHUB_WORKSPACE:-${REPO_ROOT}}"
CHDB_RUST_REPO="${CHDB_RUST_REPO:-https://github.com/hyperbyte-cloud/chdb-rust.git}"
CHDB_RUST_REF="${CHDB_RUST_REF:-feat_arrow_insert}"
DEST="$(dirname "${WORKSPACE}")/chdb-rust"

echo "Checking out ${CHDB_RUST_REPO} @ ${CHDB_RUST_REF} -> ${DEST}"

if [[ -d "${DEST}/.git" ]]; then
  git -C "${DEST}" fetch --depth 1 origin "${CHDB_RUST_REF}"
  git -C "${DEST}" checkout FETCH_HEAD
else
  git clone --depth 1 --branch "${CHDB_RUST_REF}" "${CHDB_RUST_REPO}" "${DEST}"
fi

echo "chdb-rust HEAD: $(git -C "${DEST}" rev-parse --short HEAD)"

if [[ ! -f "${DEST}/Cargo.toml" ]]; then
  echo "error: ${DEST}/Cargo.toml not found after checkout" >&2
  exit 1
fi

CRATE_MANIFEST="${WORKSPACE}/hyperbytedb/Cargo.toml"
if [[ -f "${CRATE_MANIFEST}" ]]; then
  RESOLVED="$(python3 -c "import os; print(os.path.normpath(os.path.join(os.path.dirname('${CRATE_MANIFEST}'), '../../chdb-rust')))")"
  if [[ ! -f "${RESOLVED}/Cargo.toml" ]]; then
    echo "error: path dependency does not resolve (${RESOLVED})" >&2
    exit 1
  fi
  echo "path dependency resolves to: ${RESOLVED}"
fi
