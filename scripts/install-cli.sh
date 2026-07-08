#!/usr/bin/env bash
# Install hyperbytedb-cli from GitHub Releases.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/hyperbyte-cloud/hyperbytedb/main/scripts/install-cli.sh | sudo bash
#   curl -fsSL ... | bash -s -- --version v0.8.4-beta --install-dir "$HOME/.local/bin"
#
# Environment:
#   HYPERBYTEDB_CLI_VERSION   Release tag (default: newest GitHub release)
#   INSTALL_DIR               Install directory (default: /usr/local/bin)

set -euo pipefail

REPO="hyperbyte-cloud/hyperbytedb"
DEFAULT_INSTALL_DIR="/usr/local/bin"

usage() {
	cat <<EOF
Install hyperbytedb-cli from https://github.com/${REPO}/releases

Usage: install-cli.sh [options]

Options:
  --version TAG       Release tag (e.g. v0.8.4-beta)
  --install-dir DIR   Install directory (default: ${DEFAULT_INSTALL_DIR})
  -h, --help          Show this help

Environment:
  HYPERBYTEDB_CLI_VERSION   Same as --version
  INSTALL_DIR               Same as --install-dir
EOF
}

VERSION="${HYPERBYTEDB_CLI_VERSION:-}"
INSTALL_DIR="${INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"

while [ $# -gt 0 ]; do
	case "$1" in
	--version)
		VERSION="$2"
		shift 2
		;;
	--install-dir)
		INSTALL_DIR="$2"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "Unknown option: $1" >&2
		usage >&2
		exit 1
		;;
	esac
done

case "$(uname -s)" in
Linux) ;;
Darwin)
	echo "Pre-built hyperbytedb-cli binaries are Linux-only." >&2
	echo "On macOS, build from source: cargo build --release -p hyperbytedb-cli" >&2
	exit 1
	;;
*)
	echo "Unsupported OS: $(uname -s)" >&2
	exit 1
	;;
esac

case "$(uname -m)" in
x86_64) ARCH="x86_64" ;;
aarch64 | arm64) ARCH="aarch64" ;;
*)
	echo "Unsupported architecture: $(uname -m)" >&2
	exit 1
	;;
esac

resolve_version() {
	if [ -n "$VERSION" ]; then
		echo "$VERSION"
		return
	fi
	curl -fsSL "https://api.github.com/repos/${REPO}/releases?per_page=1" |
		sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' |
		head -1
}

VERSION="$(resolve_version)"
if [ -z "$VERSION" ]; then
	echo "Could not determine release version. Set HYPERBYTEDB_CLI_VERSION or pass --version." >&2
	exit 1
fi

ARTIFACT="hyperbytedb-cli-${VERSION}-linux-${ARCH}.gz"
CHECKSUM="${ARTIFACT}.sha256"
BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"

tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap cleanup EXIT

echo "Downloading ${ARTIFACT}..."
curl -fsSL "${BASE_URL}/${ARTIFACT}" -o "${tmpdir}/${ARTIFACT}"
curl -fsSL "${BASE_URL}/${CHECKSUM}" -o "${tmpdir}/${CHECKSUM}"

(
	cd "$tmpdir"
	sha256sum -c "${CHECKSUM}"
)

gunzip -c "${tmpdir}/${ARTIFACT}" >"${tmpdir}/hyperbytedb-cli"
chmod 0755 "${tmpdir}/hyperbytedb-cli"

if [ ! -d "$INSTALL_DIR" ]; then
	if [ ! -w "$(dirname "$INSTALL_DIR")" ] && [ "$(id -u)" -ne 0 ]; then
		echo "Cannot create ${INSTALL_DIR}. Re-run with sudo or set INSTALL_DIR (e.g. \$HOME/.local/bin)." >&2
		exit 1
	fi
	mkdir -p "$INSTALL_DIR"
fi

if [ ! -w "$INSTALL_DIR" ] && [ "$(id -u)" -ne 0 ]; then
	echo "Cannot write to ${INSTALL_DIR}. Re-run with sudo or set INSTALL_DIR (e.g. \$HOME/.local/bin)." >&2
	exit 1
fi

install -m 0755 "${tmpdir}/hyperbytedb-cli" "${INSTALL_DIR}/hyperbytedb-cli"

echo "Installed hyperbytedb-cli ${VERSION} to ${INSTALL_DIR}/hyperbytedb-cli"
if ! command -v hyperbytedb-cli >/dev/null 2>&1; then
	echo "Add ${INSTALL_DIR} to your PATH if needed."
fi
