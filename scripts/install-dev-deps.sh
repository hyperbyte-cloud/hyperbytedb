#!/bin/sh
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

# install-dev-deps.sh — install developer dependencies for HyperbyteDB on Linux.
#
# POSIX-compliant (sh, dash, ash, bash, zsh in sh-mode). No bashisms.
# Verified with `shellcheck --shell=sh`.
#
# Installs, in this order (each step is idempotent and individually skippable):
#
#   1. System build packages   clang llvm-dev libclang-dev pkg-config libssl-dev
#                              build-essential / gcc-c++ / base-devel  (distro-aware)
#   2. Rust toolchain          via rustup; channel pinned by rust-toolchain.toml
#   3. libchdb                 the embedded ClickHouse engine (curl https://lib.chdb.io | sh)
#   4. k6                      Grafana's load-testing tool (used by scripts/load.sh)
#   5. Documentation deps      mkdocs-material, mkdocs-minify-plugin (pip, optional)
#
# Supported distros: Debian/Ubuntu, Fedora/RHEL/CentOS/Rocky/Alma, Arch/Manjaro,
#                    openSUSE/SLES, Alpine.
#
# Usage:
#   sh scripts/install-dev-deps.sh                     # install required deps
#   sh scripts/install-dev-deps.sh --all               # also install docs
#   sh scripts/install-dev-deps.sh --check             # dry-run; show what would happen
#   sh scripts/install-dev-deps.sh --skip-rust --skip-chdb
#
# Flags:
#   --all          Install everything, including optional docs.
#   --check        Dry-run; print actions without executing them.
#   --no-sudo      Do not invoke sudo even when root is required.
#   --yes          Pass non-interactive confirmation flags to package managers.
#   --skip-system  Skip installing system packages.
#   --skip-rust    Skip installing Rust / rustup.
#   --skip-chdb    Skip installing libchdb.
#   --skip-k6      Skip installing k6.
#   --with-docs    Install MkDocs documentation dependencies.
#   -h, --help     Show this help and exit.
#
# Environment overrides:
#   CHDB_INSTALL_URL   URL piped into sh to install libchdb (default: https://lib.chdb.io)
#   RUSTUP_INSTALL_URL URL piped into sh to install rustup  (default: https://sh.rustup.rs)
#   K6_VERSION         k6 release tag to install (default: resolve "latest" from GitHub)
#   K6_FALLBACK_VERSION k6 version to use if "latest" cannot be resolved (default: v0.55.2)
#   PIP                Python pip command (default: pip3, falls back to pip)
#
# Exit codes: 0 success, 1 user error, 2 unsupported environment, 3 install failure.

set -eu

# ---------------------------------------------------------------------------
# Output helpers — colour only when stdout is a TTY and `tput` agrees.
# ---------------------------------------------------------------------------
if [ -t 1 ] && command -v tput >/dev/null 2>&1 && [ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]; then
    C_RESET=$(tput sgr0)
    C_BOLD=$(tput bold)
    C_RED=$(tput setaf 1)
    C_GREEN=$(tput setaf 2)
    C_YELLOW=$(tput setaf 3)
    C_BLUE=$(tput setaf 4)
else
    C_RESET=''; C_BOLD=''; C_RED=''; C_GREEN=''; C_YELLOW=''; C_BLUE=''
fi

log_info()  { printf '%s==>%s %s\n'   "$C_BLUE"   "$C_RESET" "$*"; }
log_ok()    { printf '%s ok%s  %s\n'  "$C_GREEN"  "$C_RESET" "$*"; }
log_warn()  { printf '%swarn%s %s\n'  "$C_YELLOW" "$C_RESET" "$*" >&2; }
log_error() { printf '%serr%s  %s\n'  "$C_RED"    "$C_RESET" "$*" >&2; }
log_step()  { printf '\n%s%s%s\n'     "$C_BOLD"   "$*"       "$C_RESET"; }

die() { log_error "$*"; exit 3; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
DO_SYSTEM=1
DO_RUST=1
DO_CHDB=1
DO_K6=1
DO_DOCS=0
DRY_RUN=0
USE_SUDO=1
ASSUME_YES=1   # Default to non-interactive; matches what most installers expect in dev envs.

usage() {
    sed -n '2,42p' "$0" | sed 's/^# \{0,1\}//'
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --all)         DO_DOCS=1 ;;
        --check)       DRY_RUN=1 ;;
        --no-sudo)     USE_SUDO=0 ;;
        --yes|-y)      ASSUME_YES=1 ;;
        --no-yes)      ASSUME_YES=0 ;;
        --skip-system) DO_SYSTEM=0 ;;
        --skip-rust)   DO_RUST=0 ;;
        --skip-chdb)   DO_CHDB=0 ;;
        --skip-k6)     DO_K6=0 ;;
        --with-docs)   DO_DOCS=1 ;;
        -h|--help)     usage; exit 0 ;;
        *)
            log_error "unknown argument: $1"
            usage
            exit 1
            ;;
    esac
    shift
done

CHDB_INSTALL_URL=${CHDB_INSTALL_URL:-https://lib.chdb.io}
RUSTUP_INSTALL_URL=${RUSTUP_INSTALL_URL:-https://sh.rustup.rs}
K6_FALLBACK_VERSION=${K6_FALLBACK_VERSION:-v0.55.2}

# ---------------------------------------------------------------------------
# Environment checks
# ---------------------------------------------------------------------------
case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux) : ;;
    *)
        log_error "this script supports Linux only (detected: $(uname -s))"
        exit 2
        ;;
esac

ARCH=$(uname -m 2>/dev/null || echo unknown)
case "$ARCH" in
    x86_64|amd64|aarch64|arm64) : ;;
    *) log_warn "unrecognised architecture '$ARCH'; libchdb may not have a prebuilt for this CPU" ;;
esac

# ---------------------------------------------------------------------------
# sudo / privilege escalation helper.
# Sets SUDO to either "sudo", "" (already root), or aborts if root is required
# but unavailable.
# ---------------------------------------------------------------------------
SUDO=''
require_root() {
    if [ "$(id -u)" -eq 0 ]; then
        SUDO=''
        return 0
    fi
    if [ "$USE_SUDO" -eq 0 ]; then
        log_error "root required and --no-sudo was set; rerun as root or drop --no-sudo"
        exit 1
    fi
    if command -v sudo >/dev/null 2>&1; then
        SUDO='sudo'
        return 0
    fi
    log_error "root required but neither running as root nor 'sudo' is available"
    exit 1
}

# Run a command, honouring --check (dry-run). $* is printed verbatim before
# execution; quoting is preserved by the surrounding `run` invocation pattern.
run() {
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '   %s[dry-run]%s %s\n' "$C_YELLOW" "$C_RESET" "$*"
        return 0
    fi
    # shellcheck disable=SC2086 # intentional word-splitting; callers control quoting
    "$@"
}

# ---------------------------------------------------------------------------
# Distro detection — read /etc/os-release per systemd-style convention.
# ---------------------------------------------------------------------------
DISTRO_ID=''
DISTRO_LIKE=''
if [ -r /etc/os-release ]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    DISTRO_ID=${ID:-}
    DISTRO_LIKE=${ID_LIKE:-}
fi

# Map any distro to one of: debian | rhel | arch | suse | alpine | unknown
detect_family() {
    case "$DISTRO_ID" in
        debian|ubuntu|linuxmint|pop|kali|raspbian) echo debian; return ;;
        fedora|rhel|centos|rocky|almalinux|ol)     echo rhel;   return ;;
        arch|manjaro|endeavouros|cachyos)          echo arch;   return ;;
        opensuse*|sles|sled)                       echo suse;   return ;;
        alpine)                                    echo alpine; return ;;
    esac
    # Fall back to ID_LIKE.
    for like in $DISTRO_LIKE; do
        case "$like" in
            debian|ubuntu)         echo debian; return ;;
            rhel|fedora|centos)    echo rhel;   return ;;
            arch)                  echo arch;   return ;;
            suse|opensuse)         echo suse;   return ;;
        esac
    done
    echo unknown
}

DISTRO_FAMILY=$(detect_family)

# ---------------------------------------------------------------------------
# Step 1 — system build dependencies
# ---------------------------------------------------------------------------
install_system_packages() {
    log_step "[1/5] System packages"

    case "$DISTRO_FAMILY" in
        debian)
            require_root
            APT_FLAGS=''
            [ "$ASSUME_YES" -eq 1 ] && APT_FLAGS='-y'
            # `sudo env VAR=…` (not `env VAR=… sudo`) — sudo resets the child
            # environment by default, so the var must be set on its far side.
            # shellcheck disable=SC2086
            run $SUDO env DEBIAN_FRONTEND=noninteractive apt-get update
            # shellcheck disable=SC2086
            run $SUDO env DEBIAN_FRONTEND=noninteractive apt-get install $APT_FLAGS --no-install-recommends \
                build-essential \
                ca-certificates \
                clang \
                curl \
                git \
                libclang-dev \
                libssl-dev \
                llvm-dev \
                pkg-config
            ;;
        rhel)
            require_root
            DNF_FLAGS=''
            [ "$ASSUME_YES" -eq 1 ] && DNF_FLAGS='-y'
            PKG_MGR='dnf'
            command -v dnf >/dev/null 2>&1 || PKG_MGR='yum'
            # shellcheck disable=SC2086
            run $SUDO $PKG_MGR install $DNF_FLAGS \
                ca-certificates \
                clang \
                clang-devel \
                curl \
                gcc \
                gcc-c++ \
                git \
                llvm-devel \
                make \
                openssl-devel \
                pkgconfig
            ;;
        arch)
            require_root
            PAC_FLAGS='--needed'
            [ "$ASSUME_YES" -eq 1 ] && PAC_FLAGS="$PAC_FLAGS --noconfirm"
            # shellcheck disable=SC2086
            run $SUDO pacman -Sy $PAC_FLAGS \
                base-devel \
                ca-certificates \
                clang \
                curl \
                git \
                llvm \
                openssl \
                pkgconf
            ;;
        suse)
            require_root
            ZYP_FLAGS=''
            [ "$ASSUME_YES" -eq 1 ] && ZYP_FLAGS='-n'
            # shellcheck disable=SC2086
            run $SUDO zypper $ZYP_FLAGS install \
                ca-certificates \
                clang \
                curl \
                gcc \
                gcc-c++ \
                git \
                libopenssl-devel \
                llvm-devel \
                make \
                pkg-config
            ;;
        alpine)
            require_root
            # shellcheck disable=SC2086
            run $SUDO apk add --no-cache \
                build-base \
                ca-certificates \
                clang-dev \
                curl \
                git \
                llvm-dev \
                openssl-dev \
                pkgconf
            ;;
        unknown|*)
            log_error "unsupported distro: ID='$DISTRO_ID' ID_LIKE='$DISTRO_LIKE'"
            log_error "install these packages manually, then rerun with --skip-system:"
            log_error "  clang llvm-dev libclang-dev pkg-config libssl-dev build-essential curl git"
            exit 2
            ;;
    esac

    log_ok "system packages installed (family: $DISTRO_FAMILY)"
}

# ---------------------------------------------------------------------------
# Step 2 — Rust toolchain via rustup.
# rust-toolchain.toml in the repo root pins the channel; rustup will auto-
# install the matching version on first cargo invocation.
# ---------------------------------------------------------------------------
install_rust() {
    log_step "[2/5] Rust toolchain (rustup)"

    if command -v rustup >/dev/null 2>&1; then
        log_ok "rustup already installed: $(rustup --version 2>/dev/null | head -n1)"
    else
        if ! command -v curl >/dev/null 2>&1; then
            die "curl is required to install rustup"
        fi
        log_info "downloading rustup from $RUSTUP_INSTALL_URL"
        # Use a temp file rather than `curl | sh` so a partial download cannot
        # execute. Verify the signature via TLS only — this matches upstream's
        # documented install instructions.
        TMP_RUSTUP=$(mktemp)
        # shellcheck disable=SC2064
        trap "rm -f '$TMP_RUSTUP'" EXIT INT TERM HUP
        run curl --proto '=https' --tlsv1.2 -sSf "$RUSTUP_INSTALL_URL" -o "$TMP_RUSTUP"
        # `--default-toolchain none --profile minimal` matches the Dockerfile;
        # the actual toolchain comes from rust-toolchain.toml.
        run sh "$TMP_RUSTUP" -y --default-toolchain none --profile minimal
        rm -f "$TMP_RUSTUP"
        trap - EXIT INT TERM HUP
    fi

    # Make cargo/rustc visible in this shell for the chDB step and subsequent
    # invocations, without requiring the user to source anything.
    if [ -f "$HOME/.cargo/env" ] && [ "$DRY_RUN" -eq 0 ]; then
        # shellcheck disable=SC1091
        . "$HOME/.cargo/env"
    fi

    # Materialise the pinned toolchain so the first `cargo build` does not
    # surprise the user with a multi-minute rustc download.
    if [ -f "$(repo_root)/rust-toolchain.toml" ]; then
        log_info "materialising pinned toolchain from rust-toolchain.toml"
        ( run cd "$(repo_root)" && run rustup show active-toolchain ) || \
            log_warn "could not preinstall pinned toolchain; will be installed on first cargo invocation"
    fi

    log_ok "Rust toolchain ready"
}

# Locate the repository root (the directory containing this script's parent).
repo_root() {
    ( cd "$(dirname "$0")/.." 2>/dev/null && pwd )
}

# ---------------------------------------------------------------------------
# Step 3 — libchdb (embedded ClickHouse).
# The upstream installer drops libchdb.so + chdb.h into /usr/local; we then
# refresh the dynamic linker cache.
# ---------------------------------------------------------------------------
install_chdb() {
    log_step "[3/5] libchdb"

    if [ -f /usr/local/lib/libchdb.so ] && [ -f /usr/local/include/chdb.h ]; then
        log_ok "libchdb already installed at /usr/local"
        return 0
    fi

    if ! command -v curl >/dev/null 2>&1; then
        die "curl is required to install libchdb"
    fi

    require_root
    log_info "downloading libchdb installer from $CHDB_INSTALL_URL"

    # The upstream installer writes to /usr/local; therefore it must run as
    # root. We pipe via a tempfile so a partial download cannot execute.
    TMP_CHDB=$(mktemp)
    # shellcheck disable=SC2064
    trap "rm -f '$TMP_CHDB'" EXIT INT TERM HUP
    run curl --proto '=https' --tlsv1.2 -sSfL "$CHDB_INSTALL_URL" -o "$TMP_CHDB"
    run $SUDO sh "$TMP_CHDB"
    rm -f "$TMP_CHDB"
    trap - EXIT INT TERM HUP

    if command -v ldconfig >/dev/null 2>&1; then
        run $SUDO ldconfig
    fi

    if [ "$DRY_RUN" -eq 0 ] && [ ! -f /usr/local/lib/libchdb.so ]; then
        die "libchdb installation reported success but /usr/local/lib/libchdb.so is missing"
    fi

    log_ok "libchdb installed"
}

# ---------------------------------------------------------------------------
# Step 4 — k6 (Grafana load testing tool).
#
# Installed via Grafana's official static binary release on GitHub. We
# deliberately do not use distro repos (k6 isn't packaged in Debian/Ubuntu
# main, EPEL, or openSUSE; Grafana's apt/dnf repos add another supply-chain
# surface). Instead we fetch the signed tarball, extract it, and `install` the
# binary into /usr/local/bin — same pattern as libchdb.
# ---------------------------------------------------------------------------
install_k6() {
    log_step "[4/5] k6 (Grafana load-testing tool)"

    if command -v k6 >/dev/null 2>&1; then
        log_ok "k6 already installed: $(k6 version 2>/dev/null | head -n1)"
        return 0
    fi

    if ! command -v curl >/dev/null 2>&1; then
        die "curl is required to install k6"
    fi
    if ! command -v tar >/dev/null 2>&1; then
        die "tar is required to install k6"
    fi

    K6_ARCH=''
    case "$ARCH" in
        x86_64|amd64)  K6_ARCH=amd64 ;;
        aarch64|arm64) K6_ARCH=arm64 ;;
        *)
            log_warn "no prebuilt k6 binary for architecture '$ARCH'; install manually"
            return 0
            ;;
    esac

    # Resolve the version. In dry-run we skip the network call and use the
    # pinned fallback so --check is fully offline.
    K6_VER=${K6_VERSION:-}
    if [ -z "$K6_VER" ] && [ "$DRY_RUN" -eq 0 ]; then
        log_info "resolving latest k6 release from github.com/grafana/k6"
        # `curl -sI` on /releases/latest returns a 302 whose Location header
        # points at /releases/tag/<version>. We extract <version> with sed —
        # no jq, no Python.
        K6_VER=$(curl -sI --max-time 10 \
                      -H 'Accept: text/html' \
                      https://github.com/grafana/k6/releases/latest 2>/dev/null \
                  | sed -n 's@^[Ll]ocation:.*tag/\(v[0-9.]*\).*@\1@p' \
                  | tr -d '\r' \
                  | head -n1)
    fi
    if [ -z "$K6_VER" ]; then
        K6_VER=$K6_FALLBACK_VERSION
        [ "$DRY_RUN" -eq 0 ] && log_warn "could not resolve latest k6 version; falling back to $K6_VER"
    fi

    K6_DIR_NAME="k6-${K6_VER}-linux-${K6_ARCH}"
    K6_TARBALL="${K6_DIR_NAME}.tar.gz"
    K6_URL="https://github.com/grafana/k6/releases/download/${K6_VER}/${K6_TARBALL}"

    log_info "installing k6 ${K6_VER} (linux-${K6_ARCH}) from $K6_URL"

    K6_TMPDIR=$(mktemp -d)
    # shellcheck disable=SC2064
    trap "rm -rf '$K6_TMPDIR'" EXIT INT TERM HUP
    run curl --proto '=https' --tlsv1.2 -fSL --max-time 120 "$K6_URL" -o "$K6_TMPDIR/$K6_TARBALL"
    run tar -xzf "$K6_TMPDIR/$K6_TARBALL" -C "$K6_TMPDIR"

    require_root
    # `install -m 0755` is POSIX-portable across coreutils/busybox and atomic
    # via rename, so a partially-written binary cannot be observed.
    # shellcheck disable=SC2086
    run $SUDO install -m 0755 "$K6_TMPDIR/$K6_DIR_NAME/k6" /usr/local/bin/k6

    rm -rf "$K6_TMPDIR"
    trap - EXIT INT TERM HUP

    if [ "$DRY_RUN" -eq 0 ] && ! command -v k6 >/dev/null 2>&1; then
        die "k6 install reported success but 'k6' is not on PATH"
    fi

    log_ok "k6 installed to /usr/local/bin/k6"
}

# ---------------------------------------------------------------------------
# Step 5 — documentation tooling (optional).
# ---------------------------------------------------------------------------
install_docs() {
    log_step "[5/5] Documentation dependencies (MkDocs)"

    PIP_BIN=${PIP:-}
    if [ -z "$PIP_BIN" ]; then
        if command -v pip3 >/dev/null 2>&1; then
            PIP_BIN=pip3
        elif command -v pip >/dev/null 2>&1; then
            PIP_BIN=pip
        else
            log_warn "no pip found; install python3-pip first or rerun with --skip-docs"
            return 0
        fi
    fi

    REQS="$(repo_root)/docs/requirements.txt"
    if [ ! -f "$REQS" ]; then
        log_warn "docs/requirements.txt not found; skipping"
        return 0
    fi

    # `--user` keeps things out of system site-packages and avoids needing
    # sudo. PEP 668 systems require --break-system-packages or a venv; we
    # detect that and fall back gracefully.
    if "$PIP_BIN" install --help 2>/dev/null | grep -q -- '--break-system-packages'; then
        run "$PIP_BIN" install --user --break-system-packages -r "$REQS"
    else
        run "$PIP_BIN" install --user -r "$REQS"
    fi

    log_ok "docs dependencies installed"
}

# ---------------------------------------------------------------------------
# Verification — print a one-line status for each tool we expect.
# ---------------------------------------------------------------------------
verify() {
    log_step "Verification"
    # cargo may not be on PATH yet in the current shell; source if available.
    if [ -f "$HOME/.cargo/env" ]; then
        # shellcheck disable=SC1091
        . "$HOME/.cargo/env"
    fi

    check_one() {
        # $1 = label, $2 = command to run for version
        # Using sh -c so we can pass pipelines.
        out=$(sh -c "$2" 2>/dev/null | head -n1 || true)
        if [ -n "$out" ]; then
            log_ok "$1: $out"
        else
            log_warn "$1: not found on PATH"
        fi
    }

    check_one "clang     " "clang --version"
    check_one "pkg-config" "pkg-config --version"
    check_one "rustc     " "rustc --version"
    check_one "cargo     " "cargo --version"
    if [ -f /usr/local/lib/libchdb.so ]; then
        log_ok "libchdb   : /usr/local/lib/libchdb.so"
    else
        log_warn "libchdb   : /usr/local/lib/libchdb.so not found"
    fi
    if [ "$DO_K6"   -eq 1 ]; then check_one "k6        " "k6 version"; fi
    if [ "$DO_DOCS" -eq 1 ]; then check_one "mkdocs    " "mkdocs --version"; fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
log_info "HyperbyteDB developer dependency installer"
log_info "distro family : $DISTRO_FAMILY ($DISTRO_ID)"
log_info "architecture  : $ARCH"
if [ "$DRY_RUN" -eq 1 ]; then
    log_info "dry-run       : yes"
else
    log_info "dry-run       : no"
fi

# Each step runs through a plain `if`. Crucially we do *not* use the
# `cond && step || msg` idiom here: in POSIX sh, the middle command of an
# AND-OR list runs with `set -e` suppressed, which would silently swallow
# install failures. With an `if`, set -e is preserved inside the called
# function and a real failure aborts the script.
if [ "$DO_SYSTEM" -eq 1 ]; then install_system_packages; else log_info "[1/5] system packages — skipped"; fi
if [ "$DO_RUST"   -eq 1 ]; then install_rust;            else log_info "[2/5] Rust — skipped"; fi
if [ "$DO_CHDB"   -eq 1 ]; then install_chdb;            else log_info "[3/5] libchdb — skipped"; fi
if [ "$DO_K6"     -eq 1 ]; then install_k6;              else log_info "[4/5] k6 — skipped"; fi
if [ "$DO_DOCS"   -eq 1 ]; then install_docs;            else log_info "[5/5] docs — skipped (use --with-docs to enable)"; fi

verify

log_step "Done."
log_info "Open a new shell, or run:  . \"\$HOME/.cargo/env\""
log_info "Then build with:           cargo build --release"
