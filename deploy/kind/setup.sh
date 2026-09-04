#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MANIFESTS_DIR="$SCRIPT_DIR/manifests"
MONITORING_MANIFESTS_DIR="$MANIFESTS_DIR/monitoring"
# Prefer workspace sibling (../hyperbytedb-operator) — includes feat/crd-sharding-spec.
if [[ -n "${HYPERBYTEDB_OPERATOR_DIR:-}" ]]; then
    OPERATOR_DIR="$HYPERBYTEDB_OPERATOR_DIR"
elif [[ -d "$PROJECT_ROOT/../hyperbytedb-operator" ]]; then
    OPERATOR_DIR="$(cd "$PROJECT_ROOT/../hyperbytedb-operator" && pwd)"
else
    OPERATOR_DIR="$PROJECT_ROOT/hyperbytedb-operator"
fi
CLUSTER_NAME="hyperbytedb"
NAMESPACE="hyperbytedb"
# kind always names the kubeconfig context "kind-<cluster>". Pin every kubectl
# / helm call to this context so the script can't accidentally clobber whatever
# cluster the user happens to have selected as their current context.
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
HYPERBYTEDB_IMAGE="hyperbytedb:local"
PROXY_IMAGE="hyperbytedb-proxy:local"
OPERATOR_IMAGE="hyperbytedb-operator:local"

# Operator source: where to pull the hyperbytedb-operator from.
#   local — build from $OPERATOR_DIR and load into kind (default; uses
#           ../hyperbytedb-operator when present, else ./hyperbytedb-operator)
#   helm  — install the published chart from GHCR; no local checkout needed
# Override with --operator=<local|helm> or OPERATOR_SOURCE=<local|helm>.
OPERATOR_SOURCE="${OPERATOR_SOURCE:-local}"
# Perf mode: 4-worker kind cluster, 4-replica CR, no proxy/monitoring.
PERF_MODE="${PERF_MODE:-false}"
SKIP_MONITORING="${SKIP_MONITORING:-false}"
SKIP_PROXY="${SKIP_PROXY:-false}"
# Helm-mode chart reference + version. Empty version => latest.
OPERATOR_HELM_CHART="${OPERATOR_HELM_CHART:-oci://ghcr.io/hyperbyte-cloud/charts/hyperbytedb-operator}"
OPERATOR_HELM_VERSION="${OPERATOR_HELM_VERSION:-}"
OPERATOR_RELEASE="hyperbytedb-operator"
HYPERBYTEDB_REPLICAS=2
HYPERBYTEDB_CR_NAME="hyperbytedb"
HYPERBYTEDB_CR_MANIFEST=""
SHARDED_MODE="${SHARDED_MODE:-false}"
SHARDED_SETTLE_SECS="${SHARDED_SETTLE_SECS:-60}"
RAFT_JOIN_TIMEOUT_SECS="${RAFT_JOIN_TIMEOUT_SECS:-600}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()   { echo -e "${GREEN}[+]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
err()   { echo -e "${RED}[✗]${NC} $*" >&2; }
info()  { echo -e "${CYAN}[i]${NC} $*"; }
header(){ echo -e "\n${BOLD}═══ $* ═══${NC}\n"; }

# Shadow `kubectl` and `helm` so every invocation in this script is pinned to
# the kind cluster's kubeconfig context. Without this, the script silently
# uses whatever the user's current-context happens to be, which leads to
# fun failures like "operator deployed to the wrong cluster, image is on a
# different cluster's nodes, ErrImageNeverPull".
kubectl() {
    command kubectl --context "$KUBE_CONTEXT" "$@"
}
helm() {
    command helm --kube-context "$KUBE_CONTEXT" "$@"
}

usage() {
    cat <<EOF
Usage: $(basename "$0") [command] [options]

Commands:
  up        Create kind cluster, build images, deploy operator + HyperbyteDB CR
  down      Tear down the Kind cluster
  hdb-up    (Re)create HyperbyteDB via operator — CR apply + wait for Raft healthy
  hdb-down  Delete HyperbytedbCluster CR; wait for operator GC + wipe PVCs
  hdb-reset hdb-down then hdb-up (clean storage, operator stays running)
  rebuild   Rebuild images and restart pods
  status    Show cluster status
  logs      Tail hyperbytedb pod logs

Options:
  --no-build              Skip Docker image build (use existing images)
  --sharded               6-worker kind + 6-node sharded CR (spec.sharding via operator)
  --perf                  4-worker kind + perf sharded CR; skip proxy & monitoring
  --skip-monitoring       Skip prometheus/grafana/loki/tempo/alloy deploy
  --skip-proxy            Skip hyperbytedb-proxy image build
  --operator=<src>        Where to get the operator: 'local' (build from
                          \$OPERATOR_DIR, default) or 'helm' (install the
                          published chart from GHCR — no local checkout needed)
  --operator-version=<v>  When --operator=helm, pin the chart to a specific
                          version (e.g. 0.1.0). Defaults to latest.
  --help                  Show this help

Environment variables (alternatives to flags):
  OPERATOR_SOURCE          local | helm
  OPERATOR_HELM_CHART      OCI chart ref (default: $OPERATOR_HELM_CHART)
  OPERATOR_HELM_VERSION    Chart version (default: latest)

Exposed services (after 'up'):
  HyperbyteDB API   http://localhost:8086
  Prometheus     http://localhost:9090
  Grafana        http://localhost:3000  (admin/admin)
                 Logs: Explore → Loki, e.g. {namespace="hyperbytedb"} | json | system_trace="true"
                 Traces: Explore → Tempo, search service.name=hyperbytedb
EOF
}

# ── Preflight checks ─────────────────────────────────────────────────────────

check_prerequisites() {
    local missing=()
    local required=(docker kind kubectl)
    if [[ "$OPERATOR_SOURCE" == "helm" ]]; then
        required+=(helm)
    fi
    for cmd in "${required[@]}"; do
        # type -P only looks at PATH, so it isn't fooled by the kubectl/helm
        # shell-function wrappers defined above.
        if ! type -P "$cmd" &>/dev/null; then
            missing+=("$cmd")
        fi
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        err "Missing required tools: ${missing[*]}"
        err "Install them before running this script."
        exit 1
    fi
    if ! docker info &>/dev/null; then
        err "Docker daemon is not running."
        exit 1
    fi
    case "$OPERATOR_SOURCE" in
        local)
            if [[ ! -d "$OPERATOR_DIR" ]]; then
                err "OPERATOR_SOURCE=local but operator checkout not found at: $OPERATOR_DIR"
                err "Clone hyperbyte-cloud/hyperbytedb-operator next to this repo (../hyperbytedb-operator)"
                err "or under $PROJECT_ROOT/hyperbytedb-operator, or pass --operator=helm."
                exit 1
            fi
            ;;
        helm) ;;
        *)
            err "Invalid OPERATOR_SOURCE: '$OPERATOR_SOURCE' (expected 'local' or 'helm')"
            exit 1
            ;;
    esac
}

cluster_exists() {
    kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"
}

# ── Build ─────────────────────────────────────────────────────────────────────
# BuildKit enables RUN --mount=type=cache in Dockerfiles (much faster repeat Rust/Go builds).
export DOCKER_BUILDKIT=1

build_hyperbytedb_image() {
    header "Building hyperbytedb Docker image"
    # Build context is the PARENT of hyperbytedb/ so the Dockerfile can COPY the
    # sibling chdb-rust/ path dependency (Arrow insert path). See Dockerfile.
    local build_ctx
    build_ctx="$(cd "$PROJECT_ROOT/.." && pwd)"
    if [[ ! -f "$build_ctx/chdb-rust/Cargo.toml" ]]; then
        err "chdb-rust checkout not found at: $build_ctx/chdb-rust"
        err "hyperbytedb depends on it via a path dependency; clone it next to hyperbytedb/."
        exit 1
    fi
    docker build -t "$HYPERBYTEDB_IMAGE" -f "$PROJECT_ROOT/Dockerfile" "$build_ctx"
    log "Image built: $HYPERBYTEDB_IMAGE"
}

build_proxy_image() {
    header "Building hyperbytedb-proxy Docker image"
    docker build -t "$PROXY_IMAGE" \
        -f "$PROJECT_ROOT/hyperbytedb-proxy/Dockerfile" \
        "$PROJECT_ROOT"
    log "Image built: $PROXY_IMAGE"
}

build_operator_image() {
    header "Building hyperbytedb-operator Docker image"
    docker build -t "$OPERATOR_IMAGE" "$OPERATOR_DIR"
    log "Image built: $OPERATOR_IMAGE"
}

load_images() {
    header "Loading images into Kind cluster"
    kind load docker-image "$HYPERBYTEDB_IMAGE" --name "$CLUSTER_NAME"
    if [[ "$SKIP_PROXY" != true ]]; then
        kind load docker-image "$PROXY_IMAGE" --name "$CLUSTER_NAME"
    fi
    if [[ "$OPERATOR_SOURCE" == "local" ]]; then
        kind load docker-image "$OPERATOR_IMAGE" --name "$CLUSTER_NAME"
    fi
    log "Images loaded into cluster"
}

# ── Cluster lifecycle ─────────────────────────────────────────────────────────

create_host_data_dirs() {
    log "Creating host data directories for direct disk I/O"
    mkdir -p /tmp/hyperbytedb-data/worker-0
    mkdir -p /tmp/hyperbytedb-data/worker-1
    if [[ "$PERF_MODE" == true ]]; then
        mkdir -p /tmp/hyperbytedb-data/worker-2
        mkdir -p /tmp/hyperbytedb-data/worker-3
    fi
    if [[ "$SHARDED_MODE" == true ]]; then
        for i in 2 3 4 5; do
            mkdir -p "/tmp/hyperbytedb-data/worker-$i"
        done
    fi
}

configure_local_path_provisioner() {
    log "Reconfiguring local-path-provisioner to use host-mounted path /data/hyperbytedb"
    kubectl get configmap local-path-config -n local-path-storage -o yaml \
        | sed 's|/var/local-path-provisioner|/data/hyperbytedb|g' \
        | kubectl apply -f -
    kubectl rollout restart deployment/local-path-provisioner -n local-path-storage
    kubectl rollout status deployment/local-path-provisioner -n local-path-storage --timeout=60s
}

create_cluster() {
    if cluster_exists; then
        warn "Kind cluster '$CLUSTER_NAME' already exists — skipping creation"
        return 0
    fi
    header "Creating Kind cluster"
    create_host_data_dirs
    local kind_cfg="$SCRIPT_DIR/kind-config.yaml"
    if [[ "$SHARDED_MODE" == true ]]; then
        kind_cfg="$SCRIPT_DIR/kind-config-6worker.yaml"
    elif [[ "$PERF_MODE" == true ]]; then
        kind_cfg="$SCRIPT_DIR/kind-config-4worker.yaml"
    fi
    kind create cluster --config "$kind_cfg"
    configure_local_path_provisioner
    log "Cluster '$CLUSTER_NAME' created (using host path for storage)"
}

delete_cluster() {
    if ! cluster_exists; then
        warn "Kind cluster '$CLUSTER_NAME' does not exist"
        return 0
    fi
    header "Deleting Kind cluster"
    kind delete cluster --name "$CLUSTER_NAME"
    if [[ -d /tmp/hyperbytedb-data ]]; then
        log "Cleaning up host data directories"
        rm -rf /tmp/hyperbytedb-data
    fi
    log "Cluster deleted"
}

# ── Deploy ────────────────────────────────────────────────────────────────────

deploy_namespace() {
    log "Creating namespace"
    kubectl apply -f "$MANIFESTS_DIR/namespace.yaml"
}

deploy_crds() {
    # In helm mode the chart installs CRDs (crd.enable=true by default).
    if [[ "$OPERATOR_SOURCE" == "helm" ]]; then
        info "Skipping CRD apply — Helm chart installs CRDs"
        return 0
    fi
    log "Installing HyperbytedbCluster CRDs"
    kubectl apply -f "$OPERATOR_DIR/config/crd/bases/"
}

deploy_operator() {
    case "$OPERATOR_SOURCE" in
        local)
            log "Deploying hyperbytedb-operator (local image: $OPERATOR_IMAGE)"
            kubectl apply -f "$MANIFESTS_DIR/operator.yaml"
            ;;
        helm)
            deploy_operator_helm
            ;;
    esac
}

deploy_operator_helm() {
    local version_args=()
    if [[ -n "$OPERATOR_HELM_VERSION" ]]; then
        version_args+=(--version "$OPERATOR_HELM_VERSION")
        log "Installing hyperbytedb-operator via Helm ($OPERATOR_HELM_CHART @ $OPERATOR_HELM_VERSION)"
    else
        log "Installing hyperbytedb-operator via Helm ($OPERATOR_HELM_CHART, latest)"
    fi
    # Install/upgrade into the same namespace as the rest of the workloads so
    # the existing manifests (CR, NodePort, dashboards, etc.) keep working.
    helm upgrade --install "$OPERATOR_RELEASE" "$OPERATOR_HELM_CHART" \
        "${version_args[@]}" \
        --namespace "$NAMESPACE" \
        --create-namespace \
        --set crd.enable=true \
        --set crd.keep=true \
        --set rbacHelpers.enable=false \
        --wait --timeout 5m
}

deploy_hyperbytedb_cr() {
    resolve_hyperbytedb_cr
    if [[ "$SHARDED_MODE" == true ]]; then
        log "Creating HyperbytedbCluster CR (6-node sharded, spec.sharding via operator)"
    elif [[ "$PERF_MODE" == true ]]; then
        log "Creating HyperbytedbCluster CR (4-node perf sharded cluster)"
    else
        log "Creating HyperbytedbCluster CR (2-node cluster)"
    fi
    kubectl apply -f "$HYPERBYTEDB_CR_MANIFEST"
}

deploy_hyperbytedb_nodeport() {
    # When the proxy is enabled in the CR, the operator-managed
    # `hyperbytedb-proxy` Service owns NodePort 30086 (host port 8086).
    # Drop the legacy direct-to-StatefulSet NodePort so it doesn't fight
    # for the same port. To still hit a single backend pod for diagnostics,
    # use `kubectl port-forward pod/hyperbytedb-0 8086:8086 -n hyperbytedb`.
    if kubectl get svc hyperbytedb-nodeport -n "$NAMESPACE" &>/dev/null; then
        log "Removing legacy hyperbytedb-nodeport (proxy now owns NodePort 30086)"
        kubectl delete svc hyperbytedb-nodeport -n "$NAMESPACE" --ignore-not-found
    else
        info "Skipping legacy hyperbytedb-nodeport (proxy owns NodePort 30086)"
    fi
}

deploy_grafana_dashboard_configmap() {
    local dashboard_dir="$SCRIPT_DIR/grafana/dashboards"
    local args=()
    for file in "$dashboard_dir"/*.json; do
        if [[ -f "$file" ]]; then
            args+=(--from-file="$(basename "$file")"="$file")
        fi
    done

    if [[ ${#args[@]} -eq 0 ]]; then
        warn "No Grafana dashboard JSON files found — skipping"
        return 0
    fi

    log "Creating grafana-dashboards ConfigMap from dashboard JSON files"
    kubectl create configmap grafana-dashboards \
        --namespace "$NAMESPACE" \
        "${args[@]}" \
        --dry-run=client -o yaml | kubectl apply --server-side --force-conflicts -f -
}

deploy_kube_state_metrics() {
    log "Deploying kube-state-metrics"
    kubectl apply -f "$MONITORING_MANIFESTS_DIR/kube-state-metrics.yaml"
}

deploy_telegraf() {
    log "Deploying telegraf"
    kubectl apply -f "$MONITORING_MANIFESTS_DIR/telegraf.yaml"
}

deploy_prometheus() {
    log "Deploying prometheus"
    kubectl apply -f "$MONITORING_MANIFESTS_DIR/prometheus.yaml"
}

deploy_loki() {
    log "Deploying Loki"
    kubectl apply -f "$MONITORING_MANIFESTS_DIR/loki.yaml"
}

deploy_tempo() {
    log "Deploying Tempo"
    kubectl apply -f "$MONITORING_MANIFESTS_DIR/tempo.yaml"
}

cleanup_legacy_promtail() {
    if kubectl get daemonset promtail -n "$NAMESPACE" &>/dev/null; then
        log "Removing legacy Promtail (replaced by Grafana Alloy)"
        kubectl delete daemonset promtail -n "$NAMESPACE" --wait=false
    fi
    kubectl delete configmap promtail-config -n "$NAMESPACE" --ignore-not-found
    kubectl delete serviceaccount promtail -n "$NAMESPACE" --ignore-not-found
    kubectl delete clusterrole promtail-hyperbytedb --ignore-not-found
    kubectl delete clusterrolebinding promtail-hyperbytedb --ignore-not-found
}

deploy_alloy_logs() {
    cleanup_legacy_promtail
    log "Deploying Grafana Alloy (pod logs → Loki, OTLP → Tempo)"
    kubectl apply -f "$MONITORING_MANIFESTS_DIR/alloy.yaml"
}

verify_tempo_traces() {
    local count
    count=$(kubectl exec -n "$NAMESPACE" deploy/tempo -- \
        wget -qO- 'http://localhost:3200/api/search?limit=5' 2>/dev/null \
        | sed -n 's/.*"traces":\[\(.*\)\].*/\1/p' || echo "")
    if [[ -n "$count" && "$count" != "" && "$count" != "null" ]]; then
        log "Tempo search API returned trace data"
    else
        warn "Tempo still has no traces — ensure hyperbytedb image includes OTLP support and CR/config was updated"
    fi
}

deploy_grafana() {
    log "Deploying grafana"
    kubectl apply -f "$MONITORING_MANIFESTS_DIR/grafana.yaml"
}

# ── Wait helpers ──────────────────────────────────────────────────────────────

wait_for_job() {
    local name="$1" timeout="${2:-120s}"
    info "Waiting for Job/$name to complete (timeout ${timeout})..."
    kubectl wait --for=condition=complete "job/$name" \
        --namespace "$NAMESPACE" \
        --timeout="$timeout" 2>/dev/null || {
        warn "Job/$name not complete within $timeout — check 'kubectl logs job/$name -n $NAMESPACE'"
    }
}

wait_for_rollout() {
    local kind="$1" name="$2" timeout="${3:-180s}"
    info "Waiting for $kind/$name to be ready (timeout ${timeout})..."
    kubectl rollout status "$kind/$name" \
        --namespace "$NAMESPACE" \
        --timeout="$timeout" 2>/dev/null || {
        warn "$kind/$name not fully ready within $timeout — check 'kubectl get pods -n $NAMESPACE'"
    }
}

wait_for_statefulset() {
    local name="$1" replicas="$2" timeout="${3:-300}"
    info "Waiting for StatefulSet $name ($replicas replicas, timeout ${timeout}s)..."
    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        local ready
        ready=$(kubectl get statefulset "$name" -n "$NAMESPACE" \
            -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
        ready=${ready:-0}
        if [[ "$ready" -ge "$replicas" ]]; then
            log "StatefulSet $name: $ready/$replicas replicas ready"
            return 0
        fi
        printf "\r  %d/%d replicas ready (%ds elapsed)..." "$ready" "$replicas" "$elapsed"
        sleep 5
        elapsed=$((elapsed + 5))
    done
    echo
    warn "StatefulSet $name: only $ready/$replicas ready after ${timeout}s"
}

operator_deployment_name() {
    # In helm mode the chart names the deployment <release>-controller-manager;
    # the local manifest uses a flat 'hyperbytedb-operator'. Resolve both via
    # the app.kubernetes.io/name label so callers don't have to care.
    kubectl get deployment -n "$NAMESPACE" \
        -l app.kubernetes.io/name=hyperbytedb-operator \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

wait_for_operator() {
    # Helm install already runs with --wait; skip the redundant poll.
    if [[ "$OPERATOR_SOURCE" == "helm" ]]; then
        return 0
    fi
    info "Waiting for hyperbytedb-operator to be ready..."
    kubectl rollout status deployment/hyperbytedb-operator \
        --namespace "$NAMESPACE" \
        --timeout=120s 2>/dev/null || {
        warn "Operator not ready within 120s"
    }
    local op_replicas
    op_replicas=$(kubectl get deployment hyperbytedb-operator -n "$NAMESPACE" \
        -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
    if [[ "$op_replicas" == "0" ]]; then
        warn "Operator scaled to 0 — scaling back to 1 (required for Raft membership + sharding config)"
        kubectl scale deployment/hyperbytedb-operator -n "$NAMESPACE" --replicas=1
        kubectl rollout status deployment/hyperbytedb-operator \
            --namespace "$NAMESPACE" \
            --timeout=120s 2>/dev/null || true
    fi
}

resolve_hyperbytedb_cr() {
    HYPERBYTEDB_CR_MANIFEST="$MANIFESTS_DIR/hyperbytedb-cr.yaml"
    HYPERBYTEDB_REPLICAS=2
    if [[ "$SHARDED_MODE" == true ]]; then
        HYPERBYTEDB_CR_MANIFEST="$MANIFESTS_DIR/hyperbytedb-cr-6node-sharded.yaml"
        HYPERBYTEDB_REPLICAS=6
    elif [[ "$PERF_MODE" == true ]]; then
        HYPERBYTEDB_CR_MANIFEST="$MANIFESTS_DIR/hyperbytedb-cr-perf-sharded.yaml"
        HYPERBYTEDB_REPLICAS=4
    fi
}

hyperbytedb_sts_name() {
    kubectl get statefulset -n "$NAMESPACE" -l app.kubernetes.io/name=hyperbytedb \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "hyperbytedb"
}

cluster_nodes_json() {
    local pod="${1:-hyperbytedb-0}"
    kubectl exec -n "$NAMESPACE" "$pod" -c hyperbytedb -- \
        curl -sf "http://127.0.0.1:8086/cluster/nodes" 2>/dev/null || true
}

wait_for_raft_membership() {
    local replicas="$1"
    local timeout="${2:-$RAFT_JOIN_TIMEOUT_SECS}"
    info "Waiting for $replicas/$replicas active Raft members (timeout ${timeout}s)..."
    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        local nodes_json active total
        nodes_json=$(cluster_nodes_json hyperbytedb-0)
        active=$(echo "$nodes_json" | python3 -c \
            "import sys,json; d=json.load(sys.stdin); print(sum(1 for n in d.get('nodes',[]) if str(n.get('state','')).lower()=='active'))" \
            2>/dev/null || echo "0")
        total=$(echo "$nodes_json" | python3 -c \
            "import sys,json; d=json.load(sys.stdin); print(len(d.get('nodes',[])))" \
            2>/dev/null || echo "0")
        if [[ "$active" -ge "$replicas" && "$total" -ge "$replicas" ]]; then
            log "Raft membership: $active/$replicas active"
            return 0
        fi
        printf "\r  Raft: %s/%s active nodes (%ds)..." "$active" "$replicas" "$elapsed"
        sleep 5
        elapsed=$((elapsed + 5))
    done
    echo
    warn "Raft membership incomplete after ${timeout}s (last: ${active:-0}/${replicas} active)"
    return 1
}

wait_for_cr_healthy() {
    local timeout="${1:-600}"
    info "Waiting for HyperbytedbCluster/$HYPERBYTEDB_CR_NAME Healthy (timeout ${timeout}s)..."
    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        local phase ready replicas
        phase=$(kubectl get hyperbytedbcluster "$HYPERBYTEDB_CR_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        ready=$(kubectl get hyperbytedbcluster "$HYPERBYTEDB_CR_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
        replicas=$(kubectl get hyperbytedbcluster "$HYPERBYTEDB_CR_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "$HYPERBYTEDB_REPLICAS")
        ready=${ready:-0}
        replicas=${replicas:-$HYPERBYTEDB_REPLICAS}
        if [[ "$ready" -ge "$replicas" && ( "$phase" == "Running" || "$phase" == "Healthy" ) ]]; then
            log "HyperbytedbCluster ready: $ready/$replicas phase=$phase"
            return 0
        fi
        printf "\r  CR: ready=%s/%s phase=%s (%ds)..." "$ready" "$replicas" "${phase:-unknown}" "$elapsed"
        sleep 5
        elapsed=$((elapsed + 5))
    done
    echo
    warn "HyperbytedbCluster not healthy within ${timeout}s"
    return 1
}

wait_for_hdb_teardown() {
    local timeout="${1:-300}"
    info "Waiting for HyperbyteDB workloads to disappear (timeout ${timeout}s)..."
    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        local sts_pods pvcs
        sts_pods=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=hyperbytedb \
            --no-headers 2>/dev/null | wc -l | tr -d ' ')
        sts_pods=${sts_pods:-0}
        if [[ "$sts_pods" -eq 0 ]] && ! kubectl get hyperbytedbcluster "$HYPERBYTEDB_CR_NAME" \
            -n "$NAMESPACE" &>/dev/null; then
            log "HyperbyteDB CR and pods removed"
            return 0
        fi
        printf "\r  pods=%s CR=%s (%ds)..." "$sts_pods" \
            "$(kubectl get hyperbytedbcluster "$HYPERBYTEDB_CR_NAME" -n "$NAMESPACE" \
                -o name 2>/dev/null || echo gone)" "$elapsed"
        sleep 3
        elapsed=$((elapsed + 3))
    done
    echo
    warn "Teardown still in progress after ${timeout}s"
    return 1
}

delete_hdb_pvcs() {
    local pvcs
    pvcs=$(kubectl get pvc -n "$NAMESPACE" -l app.kubernetes.io/name=hyperbytedb \
        --no-headers 2>/dev/null | wc -l | tr -d ' ')
    pvcs=${pvcs:-0}
    if [[ "$pvcs" -eq 0 ]]; then
        info "No HyperbyteDB PVCs to delete"
        return 0
    fi
    log "Deleting $pvcs HyperbyteDB PVC(s) (StatefulSet PVCs survive CR delete)"
    kubectl delete pvc -n "$NAMESPACE" -l app.kubernetes.io/name=hyperbytedb --wait=true
    wipe_hdb_host_data
}

# kind extraMounts bind /tmp/hyperbytedb-data/worker-N → node /data/hyperbytedb.
# local-path may recreate a PVC with the same name and reuse leftover raft/meta
# files, so hdb-reset must wipe the hostPath contents — not just the PVC objects.
wipe_hdb_host_data() {
    local d
    if [[ ! -d /tmp/hyperbytedb-data ]]; then
        return 0
    fi
    log "Wiping kind hostPath data under /tmp/hyperbytedb-data"
    for d in /tmp/hyperbytedb-data/worker-*; do
        [[ -d "$d" ]] || continue
        find "$d" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    done
}

ensure_operator_stack() {
    if ! cluster_exists; then
        err "Kind cluster '$CLUSTER_NAME' does not exist. Run './deploy/kind/setup.sh up' first."
        exit 1
    fi
    deploy_namespace
    deploy_crds
    if ! kubectl get deployment hyperbytedb-operator -n "$NAMESPACE" &>/dev/null; then
        deploy_operator
    fi
    wait_for_operator
}

maybe_build_and_load() {
    local skip_build=false
    for arg in "$@"; do
        [[ "$arg" == "--no-build" ]] && skip_build=true
    done
    if [[ "$skip_build" == true ]]; then
        info "Skipping Docker build (--no-build)"
        return 0
    fi
    build_hyperbytedb_image
    if [[ "$SKIP_PROXY" != true ]]; then
        build_proxy_image
    fi
    if [[ "$OPERATOR_SOURCE" == "local" ]]; then
        build_operator_image
    fi
    load_images
    local op_dep
    op_dep=$(operator_deployment_name)
    if [[ -n "$op_dep" ]]; then
        kubectl rollout restart "deployment/$op_dep" -n "$NAMESPACE"
        wait_for_operator
    fi
}

# ── Status ────────────────────────────────────────────────────────────────────

show_status() {
    header "Cluster Status"
    echo -e "${BOLD}Operator source:${NC} $OPERATOR_SOURCE"
    if [[ "$SHARDED_MODE" == true ]]; then
        echo -e "  profile: 6-node sharded (spec.sharding via operator)"
    elif [[ "$PERF_MODE" == true ]]; then
        echo -e "  profile: 4-node perf sharded"
    fi
    if [[ "$OPERATOR_SOURCE" == "helm" ]]; then
        echo -e "  chart: $OPERATOR_HELM_CHART"
        echo -e "  version: ${OPERATOR_HELM_VERSION:-latest}"
    fi
    echo
    echo -e "${BOLD}Nodes:${NC}"
    kubectl get nodes -o wide 2>/dev/null || true
    echo
    echo -e "${BOLD}HyperbytedbCluster CR:${NC}"
    kubectl get hyperbytedbclusters -n "$NAMESPACE" 2>/dev/null || true
    echo
    echo -e "${BOLD}Pods (${NAMESPACE}):${NC}"
    kubectl get pods -n "$NAMESPACE" -o wide 2>/dev/null || true
    echo
    echo -e "${BOLD}Services (${NAMESPACE}):${NC}"
    kubectl get svc -n "$NAMESPACE" 2>/dev/null || true
    echo
    echo -e "${BOLD}StatefulSets (${NAMESPACE}):${NC}"
    kubectl get statefulsets -n "$NAMESPACE" 2>/dev/null || true
    echo
    echo -e "${BOLD}PVCs (${NAMESPACE}):${NC}"
    kubectl get pvc -n "$NAMESPACE" 2>/dev/null || true
}

show_access_info() {
    echo
    echo -e "${BOLD}════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}  Access your services:${NC}"
    echo -e "${BOLD}════════════════════════════════════════════════${NC}"
    echo -e "  HyperbyteDB API   ${CYAN}http://localhost:8086${NC}  ${CYAN}(via hyperbytedb-proxy)${NC}"
    echo -e "  Prometheus     ${CYAN}http://localhost:9090${NC}"
    echo -e "  Grafana        ${CYAN}http://localhost:3000${NC}  (admin/admin)"
    echo -e "  Loki (in-cluster)  ${CYAN}http://loki:3100${NC} — Grafana Explore → Loki"
    echo -e "  Tempo (in-cluster) ${CYAN}http://tempo:3200${NC} — Grafana Explore → Tempo"
    echo -e "  OTLP (in-cluster)  ${CYAN}http://alloy-logs:4318${NC} — HyperbyteDB → Alloy → Tempo"
    echo -e "${BOLD}════════════════════════════════════════════════${NC}"
    echo
    echo -e "  ${BOLD}Quick test:${NC}"
    echo -e "    curl -s 'http://localhost:8086/query?q=SHOW+DATABASES' ${CYAN}# public listener${NC}"
    echo -e "    curl -s http://localhost:8087/readyz          ${CYAN}# proxy admin listener${NC}"
    echo -e "    curl -s http://localhost:8087/admin/backends  ${CYAN}# proxy view of backends${NC}"
    echo
    echo -e "  ${BOLD}Inspect cluster (direct to DB pods, not via proxy):${NC}"
    echo -e "    kubectl port-forward -n hyperbytedb pod/hyperbytedb-0 8086:8086"
    echo -e "    curl -s http://localhost:8086/cluster/metrics | jq ."
    echo
    echo -e "  ${BOLD}Run load test:${NC}"
    echo -e "    bash scripts/load.sh cluster 127.0.0.1 8086"
    echo
}

# ── Commands ──────────────────────────────────────────────────────────────────

cmd_up() {
    local skip_build=false
    for arg in "$@"; do
        [[ "$arg" == "--no-build" ]] && skip_build=true
    done

    check_prerequisites

    local cluster_was_new=false
    if ! cluster_exists; then
        cluster_was_new=true
    fi

    create_cluster

    if [[ "$skip_build" == false ]]; then
        build_hyperbytedb_image
        if [[ "$SKIP_PROXY" != true ]]; then
            build_proxy_image
        else
            info "Skipping hyperbytedb-proxy image build (--skip-proxy / --perf)"
        fi
        if [[ "$OPERATOR_SOURCE" == "local" ]]; then
            build_operator_image
        else
            info "Skipping operator image build (OPERATOR_SOURCE=helm)"
        fi
    else
        info "Skipping Docker build (--no-build)"
    fi

    load_images

    header "Deploying workloads"
    deploy_namespace
    deploy_crds
    deploy_operator

    header "Waiting for operator"
    wait_for_operator

    deploy_hyperbytedb_cr
    deploy_hyperbytedb_nodeport
    if [[ "$SKIP_MONITORING" != true ]]; then
        deploy_grafana_dashboard_configmap
        deploy_kube_state_metrics
        deploy_telegraf
        deploy_prometheus
        deploy_loki
        deploy_tempo
        deploy_alloy_logs
        deploy_grafana
    else
        info "Skipping monitoring stack (--skip-monitoring / --perf)"
    fi

    # When the cluster already existed, pods won't pick up the new image
    # automatically since the tag hasn't changed. Force a rolling restart.
    # In helm mode the operator image came from a registry tag we didn't
    # rebuild, so only restart its pod when running locally.
    if [[ "$cluster_was_new" == false && "$skip_build" == false ]]; then
        header "Restarting pods to pick up new images"
        if [[ "$OPERATOR_SOURCE" == "local" ]]; then
            local op_dep
            op_dep=$(operator_deployment_name)
            if [[ -n "$op_dep" ]]; then
                kubectl rollout restart "deployment/$op_dep" -n "$NAMESPACE"
            fi
        fi
        # The operator will recreate the StatefulSet with the new image
        # but we need to restart existing pods to pick up the new hyperbytedb image
        local sts_name
        sts_name=$(kubectl get statefulset -n "$NAMESPACE" -l app.kubernetes.io/name=hyperbytedb -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
        if [[ -n "$sts_name" ]]; then
            kubectl rollout restart statefulset/"$sts_name" -n "$NAMESPACE"
        fi
    fi

    header "Waiting for workloads"
    wait_for_statefulset hyperbytedb "$HYPERBYTEDB_REPLICAS" 600
    if [[ "$HYPERBYTEDB_REPLICAS" -gt 1 ]]; then
        wait_for_cr_healthy 600 || true
        wait_for_raft_membership "$HYPERBYTEDB_REPLICAS" || true
        if [[ "$SHARDED_MODE" == true ]]; then
            info "Settling ${SHARDED_SETTLE_SECS}s for shard heartbeats..."
            sleep "$SHARDED_SETTLE_SECS"
        fi
    fi
    if [[ "$SKIP_MONITORING" != true ]]; then
        verify_tempo_traces
        wait_for_rollout deployment kube-state-metrics 120s
        wait_for_rollout deployment telegraf 120s
        wait_for_rollout deployment prometheus 120s
        wait_for_rollout deployment loki 120s
        wait_for_rollout deployment tempo 180s
        wait_for_rollout deployment alloy-logs 180s
        wait_for_rollout deployment grafana 120s
    fi

    show_status
    show_access_info

    log "Done! All services deployed."
}

cmd_hdb_down() {
    check_prerequisites
    if ! cluster_exists; then
        err "Kind cluster '$CLUSTER_NAME' does not exist."
        exit 1
    fi

    header "Deleting HyperbytedbCluster (operator-managed teardown)"
    if kubectl get hyperbytedbcluster "$HYPERBYTEDB_CR_NAME" -n "$NAMESPACE" &>/dev/null; then
        kubectl delete hyperbytedbcluster "$HYPERBYTEDB_CR_NAME" -n "$NAMESPACE" --wait=true
    else
        info "HyperbytedbCluster/$HYPERBYTEDB_CR_NAME not found — skipping CR delete"
    fi

    wait_for_hdb_teardown 300 || true
    delete_hdb_pvcs
    wipe_hdb_host_data
    log "HyperbyteDB cluster removed (operator still running)."
}

cmd_hdb_up() {
    local skip_build=false
    for arg in "$@"; do
        [[ "$arg" == "--no-build" ]] && skip_build=true
    done

    check_prerequisites
    ensure_operator_stack
    maybe_build_and_load "$@"

    resolve_hyperbytedb_cr
    header "Applying HyperbytedbCluster ($HYPERBYTEDB_REPLICAS replicas)"
    kubectl apply -f "$HYPERBYTEDB_CR_MANIFEST"
    deploy_hyperbytedb_nodeport

    wait_for_statefulset hyperbytedb "$HYPERBYTEDB_REPLICAS" 600
    wait_for_cr_healthy 600 || true
    if [[ "$HYPERBYTEDB_REPLICAS" -gt 1 ]]; then
        wait_for_raft_membership "$HYPERBYTEDB_REPLICAS" || true
    fi
    if [[ "$SHARDED_MODE" == true ]]; then
        info "Settling ${SHARDED_SETTLE_SECS}s for shard heartbeats..."
        sleep "$SHARDED_SETTLE_SECS"
    fi

    show_status
    log "HyperbyteDB cluster is up."
}

cmd_hdb_reset() {
    cmd_hdb_down
    cmd_hdb_up "$@"
}

cmd_down() {
    check_prerequisites
    delete_cluster
}

cmd_rebuild() {
    check_prerequisites
    if ! cluster_exists; then
        err "Cluster '$CLUSTER_NAME' does not exist. Run 'up' first."
        exit 1
    fi
    build_hyperbytedb_image
    build_proxy_image
    if [[ "$OPERATOR_SOURCE" == "local" ]]; then
        build_operator_image
    else
        info "Skipping operator image build (OPERATOR_SOURCE=helm)"
    fi
    load_images

    resolve_hyperbytedb_cr
    if kubectl get hyperbytedbcluster "$HYPERBYTEDB_CR_NAME" -n "$NAMESPACE" &>/dev/null; then
        local cr_replicas
        cr_replicas=$(kubectl get hyperbytedbcluster "$HYPERBYTEDB_CR_NAME" -n "$NAMESPACE" \
            -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "")
        if [[ -n "$cr_replicas" ]]; then
            HYPERBYTEDB_REPLICAS="$cr_replicas"
        fi
    fi

    local op_dep
    op_dep=$(operator_deployment_name)
    if [[ -n "$op_dep" ]]; then
        log "Restarting operator ($op_dep)"
        kubectl rollout restart "deployment/$op_dep" -n "$NAMESPACE"
    fi
    wait_for_operator
    log "Restarting hyperbytedb pods"
    sts_name=$(kubectl get statefulset -n "$NAMESPACE" -l app.kubernetes.io/name=hyperbytedb -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [[ -n "$sts_name" ]]; then
        kubectl rollout restart statefulset/"$sts_name" -n "$NAMESPACE"
    fi
    wait_for_statefulset hyperbytedb "$HYPERBYTEDB_REPLICAS" 600
    show_status
    log "Rebuild complete."
}

cmd_logs() {
    local pod="${1:-}"
    if [[ -n "$pod" ]]; then
        kubectl logs -n "$NAMESPACE" "$pod" -f
    else
        kubectl logs -n "$NAMESPACE" -l app.kubernetes.io/name=hyperbytedb -f --max-log-requests=5
    fi
}

# ── Main ──────────────────────────────────────────────────────────────────────

parse_global_flags() {
    # Strip --operator/--operator-version (consumed here) from $@; pass the
    # rest through to subcommands. Sets OPERATOR_SOURCE / OPERATOR_HELM_VERSION
    # as side effects.
    REMAINING_ARGS=()
    for arg in "$@"; do
        case "$arg" in
            --perf)
                PERF_MODE=true
                SKIP_MONITORING=true
                SKIP_PROXY=true
                ;;
            --sharded)
                SHARDED_MODE=true
                SKIP_MONITORING=true
                SKIP_PROXY=true
                ;;
            --skip-monitoring)
                SKIP_MONITORING=true
                ;;
            --skip-proxy)
                SKIP_PROXY=true
                ;;
            --operator=*)
                OPERATOR_SOURCE="${arg#--operator=}"
                ;;
            --operator-version=*)
                OPERATOR_HELM_VERSION="${arg#--operator-version=}"
                ;;
            *)
                REMAINING_ARGS+=("$arg")
                ;;
        esac
    done
}

main() {
    local cmd="${1:-up}"
    shift 2>/dev/null || true

    parse_global_flags "$@"
    # Re-bind $@ to the args minus consumed global flags. Use the safe-with-
    # set-u expansion for an empty array.
    set -- ${REMAINING_ARGS[@]+"${REMAINING_ARGS[@]}"}

    case "$cmd" in
        up)         cmd_up "$@" ;;
        down)       cmd_down ;;
        hdb-up)     cmd_hdb_up "$@" ;;
        hdb-down)   cmd_hdb_down ;;
        hdb-reset)  cmd_hdb_reset "$@" ;;
        rebuild)    cmd_rebuild ;;
        status)     show_status ; show_access_info ;;
        logs)       cmd_logs "$@" ;;
        --help|-h)  usage ;;
        *)
            err "Unknown command: $cmd"
            usage
            exit 1
            ;;
    esac
}

main "$@"
