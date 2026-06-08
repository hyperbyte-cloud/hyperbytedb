#!/usr/bin/env bash
# Install CI dependencies for GitHub Actions on ARC/Kubernetes runners.
#
# ARC runner pods use the actions-runner image, which is minimal and typically
# runs as a non-root user without sudo. Job-level `container:` directives are
# not supported unless the runner scale set is configured for container jobs, so
# workflows install what they need directly into the runner pod.
set -euo pipefail

PROFILE="${1:-k8s-runner}"
LOCAL_ROOT="${HOME}/.local"
LOCAL_BIN="${LOCAL_ROOT}/bin"
mkdir -p "$LOCAL_BIN"

add_to_path() {
  local dir="$1"
  if [ -d "$dir" ] && [[ ":${PATH}:" != *":${dir}:"* ]]; then
    export PATH="${dir}:${PATH}"
    if [ -n "${GITHUB_PATH:-}" ]; then
      echo "$dir" >> "$GITHUB_PATH"
    fi
  fi
}

add_to_path "$LOCAL_BIN"
add_to_path "${LOCAL_ROOT}/usr/bin"

install_apt_packages() {
  local packages=("$@")
  if [ "${#packages[@]}" -eq 0 ]; then
    return 0
  fi

  if [ "$(id -u)" -eq 0 ] && command -v apt-get >/dev/null 2>&1; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${packages[@]}"
    return 0
  fi

  if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null && command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${packages[@]}"
    return 0
  fi

  if ! command -v dpkg-deb >/dev/null 2>&1; then
    echo "dpkg-deb is required to install packages without root." >&2
    exit 1
  fi

  local workdir
  workdir="$(mktemp -d)"
  trap 'rm -rf "$workdir"' RETURN

  cd "$workdir"
  for pkg in "${packages[@]}"; do
    if apt-get download "$pkg" 2>/dev/null; then
      dpkg-deb -x "${pkg}"_*.deb "$LOCAL_ROOT"
      rm -f "${pkg}"_*.deb
      continue
    fi

    case "$pkg" in
      make)
        curl -fsSL "http://archive.ubuntu.com/ubuntu/pool/main/m/make-dfsg/make_4.3-4.1build1_amd64.deb" -o make.deb
        dpkg-deb -x make.deb "$LOCAL_ROOT"
        ;;
      *)
        echo "Failed to install ${pkg} without root." >&2
        exit 1
        ;;
    esac
  done
}

ensure_command() {
  command -v "$1" >/dev/null 2>&1
}

install_kind() {
  ensure_command kind && return 0
  local arch="amd64"
  case "$(uname -m)" in
    x86_64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *)
      echo "Unsupported architecture for kind: $(uname -m)" >&2
      exit 1
      ;;
  esac
  curl -fsSL "https://kind.sigs.k8s.io/dl/latest/kind-linux-${arch}" -o "${LOCAL_BIN}/kind"
  chmod +x "${LOCAL_BIN}/kind"
}

install_kubectl() {
  ensure_command kubectl && return 0
  local arch="amd64"
  case "$(uname -m)" in
    x86_64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *)
      echo "Unsupported architecture for kubectl: $(uname -m)" >&2
      exit 1
      ;;
  esac
  local version
  version="$(curl -fsSL https://dl.k8s.io/release/stable.txt)"
  curl -fsSL "https://dl.k8s.io/release/${version}/bin/linux/${arch}/kubectl" -o "${LOCAL_BIN}/kubectl"
  chmod +x "${LOCAL_BIN}/kubectl"
}

install_helm() {
  ensure_command helm && return 0
  local arch="amd64"
  case "$(uname -m)" in
    x86_64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *)
      echo "Unsupported architecture for helm: $(uname -m)" >&2
      exit 1
      ;;
  esac
  local version
  version="$(curl -fsSL https://get.helm.sh/helm-latest-version)"
  curl -fsSL "https://get.helm.sh/helm-${version}-linux-${arch}.tar.gz" | tar xz -C "${RUNNER_TEMP:-/tmp}"
  mv "${RUNNER_TEMP:-/tmp}/linux-${arch}/helm" "${LOCAL_BIN}/helm"
  chmod +x "${LOCAL_BIN}/helm"
}

install_yq() {
  ensure_command yq && return 0
  local arch="amd64"
  case "$(uname -m)" in
    x86_64) arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *)
      echo "Unsupported architecture for yq: $(uname -m)" >&2
      exit 1
      ;;
  esac
  curl -fsSL "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_${arch}" -o "${LOCAL_BIN}/yq"
  chmod +x "${LOCAL_BIN}/yq"
}

case "$PROFILE" in
  k8s-runner)
    missing=()
    ensure_command make || missing+=(make)
    ensure_command curl || missing+=(curl)
    ensure_command git || missing+=(git)
    install_apt_packages "${missing[@]}"
    add_to_path "${LOCAL_ROOT}/usr/bin"
    ;;
  golang-container)
    install_apt_packages make git curl ca-certificates
    add_to_path "${LOCAL_ROOT}/usr/bin"
    ;;
  *)
    echo "Unknown profile: ${PROFILE}" >&2
    exit 1
    ;;
esac

if [ "${INSTALL_KIND:-false}" = "true" ]; then
  install_kind
fi
if [ "${INSTALL_KUBECTL:-false}" = "true" ]; then
  install_kubectl
fi
if [ "${INSTALL_HELM:-false}" = "true" ]; then
  install_helm
fi
if [ "${INSTALL_YQ:-false}" = "true" ]; then
  install_yq
fi

echo "Installed CI dependencies (profile=${PROFILE})"
command -v make >/dev/null && make --version | head -1 || true
command -v go >/dev/null && go version || true
command -v kind >/dev/null && kind version || true
command -v kubectl >/dev/null && kubectl version --client=true || true
command -v helm >/dev/null && helm version --short || true
command -v yq >/dev/null && yq --version || true
command -v docker >/dev/null && docker version --format '{{.Client.Version}}' 2>/dev/null || true
