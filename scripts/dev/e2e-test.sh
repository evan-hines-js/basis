#!/bin/bash
# E2E test script for Lattice operator
# This script tests the full bootstrap flow with a real kind cluster

set -euo pipefail

# Configuration
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-lattice-e2e}"
CAPI_VERSION="${CAPI_VERSION:-v1.9.2}"
CAPD_VERSION="${CAPD_VERSION:-v1.9.2}"# Docker provider
TIMEOUT="${TIMEOUT:-300}"
NAMESPACE="${NAMESPACE:-lattice-system}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    if kind get clusters 2>/dev/null | grep -q "^${KIND_CLUSTER_NAME}$"; then
        kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
    fi
    # Kill any running lattice processes
    pkill -f "target/debug/lattice" || true
    pkill -f "target/release/lattice" || true
}

# Trap for cleanup on exit
trap cleanup EXIT

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing=()

    command -v kind >/dev/null 2>&1 || missing+=("kind")
    command -v kubectl >/dev/null 2>&1 || missing+=("kubectl")
    command -v docker >/dev/null 2>&1 || missing+=("docker")
    command -v cargo >/dev/null 2>&1 || missing+=("cargo")

    if [ ${#missing[@]} -gt 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        exit 1
    fi

    log_info "All prerequisites satisfied"
}

# Build the operator
build_operator() {
    log_info "Building Lattice operator..."
    cargo build --release
    log_info "Build complete"
}

# Create kind cluster for management
create_kind_cluster() {
    log_info "Creating kind cluster: ${KIND_CLUSTER_NAME}"

    if kind get clusters 2>/dev/null | grep -q "^${KIND_CLUSTER_NAME}$"; then
        log_warn "Cluster ${KIND_CLUSTER_NAME} already exists, deleting..."
        kind delete cluster --name "${KIND_CLUSTER_NAME}"
    fi

    # Create kind cluster with extra port mappings for bootstrap and gRPC,
    # and FIPS-compliant TLS cipher suites on the API server.
    cat <<EOF | kind create cluster --name "${KIND_CLUSTER_NAME}" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 30080
    hostPort: 8080
    protocol: TCP
  - containerPort: 30443
    hostPort: 8443
    protocol: TCP
  - containerPort: 30051
    hostPort: 50051
    protocol: TCP
  kubeadmConfigPatches:
  - |
    kind: ClusterConfiguration
    apiServer:
      extraArgs:
        tls-cipher-suites: "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
        tls-min-version: "VersionTLS12"
EOF

    log_info "Kind cluster created"

    # Wait for cluster to be ready
    kubectl wait --for=condition=Ready nodes --all --timeout=60s
}

# Install CAPI and providers
install_capi() {
    log_info "Installing Cluster API..."

    # CAPI providers are installed by the Lattice operator natively
    # (no clusterctl needed - the operator reads YAML manifests from /providers/)
    log_info "CAPI will be installed by the Lattice operator"

    # Wait for CAPI pods to be ready
    log_info "Waiting for CAPI pods to be ready..."
    kubectl wait --for=condition=Ready pods --all -n capi-system --timeout=120s || true
    kubectl wait --for=condition=Ready pods --all -n capd-system --timeout=120s || true
}

# Deploy the CRD
deploy_crd() {
    log_info "Deploying LatticeCluster CRD..."
    ./target/release/lattice --crd | kubectl apply -f -
    log_info "CRD deployed"
}

# Start the operator in controller mode
start_operator() {
    log_info "Starting Lattice operator in controller mode..."

    # Get the host IP that kind containers can reach
    local host_ip
    host_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}' "${KIND_CLUSTER_NAME}-control-plane" | head -1)

    # Start operator in background
    RUST_LOG=info ./target/release/lattice controller \
        --bootstrap-addr "0.0.0.0:8080" \
        --grpc-addr "0.0.0.0:50051" \
        --cell-endpoint "${host_ip}:8080" \
        &

    OPERATOR_PID=$!
    log_info "Operator started with PID ${OPERATOR_PID}"

    # Give it time to start
    sleep 5

    # Check if still running
    if ! kill -0 ${OPERATOR_PID} 2>/dev/null; then
        log_error "Operator failed to start"
        exit 1
    fi
}

# Create a test LatticeCluster
create_test_cluster() {
    log_info "Creating test LatticeCluster..."

    cat <<EOF | kubectl apply -f -
apiVersion: lattice.dev/v1alpha1
kind: LatticeCluster
metadata:
  name: test-workload
spec:
  provider:
    type: docker
    kubernetes:
      version: "1.32.0"
  nodes:
    controlPlane: 1
    workers: 0
  networking:
    default:
      cidr: "172.19.0.0/16"
EOF

    log_info "LatticeCluster created"
}

# Wait for cluster to be provisioned
wait_for_cluster() {
    log_info "Waiting for cluster to be provisioned (timeout: ${TIMEOUT}s)..."

    local start_time
    start_time=$(date +%s)

    while true; do
        local current_time
        current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ ${elapsed} -ge ${TIMEOUT} ]; then
            log_error "Timeout waiting for cluster"
            kubectl get latticecluster test-workload -o yaml
            exit 1
        fi

        local phase
        phase=$(kubectl get latticecluster test-workload -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")

        log_info "Cluster phase: ${phase} (${elapsed}s elapsed)"

        case "${phase}" in
            "Ready")
                log_info "Cluster is ready!"
                return 0
                ;;
            "Failed")
                log_error "Cluster provisioning failed"
                kubectl get latticecluster test-workload -o yaml
                exit 1
                ;;
            *)
                sleep 10
                ;;
        esac
    done
}

# Verify CAPI resources were created
verify_capi_resources() {
    log_info "Verifying CAPI resources..."

    # Check for Cluster resource
    if ! kubectl get cluster test-workload >/dev/null 2>&1; then
        log_error "CAPI Cluster not found"
        exit 1
    fi

    # Check for DockerCluster
    if ! kubectl get dockercluster test-workload >/dev/null 2>&1; then
        log_error "DockerCluster not found"
        exit 1
    fi

    # Check for KubeadmControlPlane
    if ! kubectl get kubeadmcontrolplane test-workload-control-plane >/dev/null 2>&1; then
        log_error "KubeadmControlPlane not found"
        exit 1
    fi

    log_info "All CAPI resources created successfully"
}

# Run the tests
run_unit_tests() {
    log_info "Running unit tests..."
    cargo test --lib
    log_info "Unit tests passed"
}

run_integration_tests() {
    log_info "Running integration tests..."
    cargo test --test kind
    log_info "Integration tests passed"
}

# Main
main() {
    log_info "Starting E2E test suite"

    check_prerequisites

    # Build first
    build_operator

    # Run unit tests
    run_unit_tests

    # Run integration tests (non-kind)
    run_integration_tests

    # Full E2E with kind cluster
    log_info "=== Starting full E2E test with kind cluster ==="

    create_kind_cluster
    install_capi
    deploy_crd
    start_operator
    create_test_cluster
    wait_for_cluster
    verify_capi_resources

    log_info "=== E2E tests completed successfully! ==="
}

# Allow running specific steps
case "${1:-all}" in
    all)
        main
        ;;
    build)
        build_operator
        ;;
    unit)
        run_unit_tests
        ;;
    integration)
        run_integration_tests
        ;;
    kind)
        check_prerequisites
        create_kind_cluster
        install_capi
        deploy_crd
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo "Usage: $0 [all|build|unit|integration|kind|cleanup]"
        exit 1
        ;;
esac
