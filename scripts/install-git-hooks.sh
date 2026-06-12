#!/usr/bin/env bash
# Point this repository at the version-controlled hooks in .githooks/.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

chmod +x .githooks/pre-commit
git config core.hooksPath .githooks

echo "Installed git hooks (core.hooksPath=.githooks)"
echo "Pre-commit runs: cargo fmt --check, cargo clippy --all-targets -- -D warnings"
