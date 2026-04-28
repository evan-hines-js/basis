#!/bin/bash
# E2E test with Docker provider (CAPD)
# No external dependencies - runs entirely in kind clusters
#
# Usage:
#   ./test-docker.sh              # Run unified E2E (default)
#   ./test-docker.sh unified      # Run unified E2E (2-cluster)
#   ./test-docker.sh workload2    # Run workload2 E2E (3-cluster)
#   ./test-docker.sh mesh         # Run mesh E2E
#   ./test-docker.sh proxy        # Run proxy E2E
#   ./test-docker.sh cedar        # Run cedar E2E
#   ./test-docker.sh secrets      # Run secrets E2E
#   ./test-docker.sh <name>       # Run any *_e2e test by name
#
# FIPS is disabled for macOS test builds (--no-default-features) because
# aws-lc-fips-sys fails to build on macOS ARM64. Production container builds
# on Linux use FIPS (default features).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Determine which E2E test to run
TEST_NAME="${1:-unified}"

# Map short names to test module names
case "$TEST_NAME" in
    unified)         TEST_MODULE="unified_e2e" ;;
    workload2)       TEST_MODULE="workload2_e2e" ;;
    mesh)            TEST_MODULE="mesh_e2e" ;;
    proxy)           TEST_MODULE="proxy_e2e" ;;
    cedar)           TEST_MODULE="cedar_e2e" ;;
    cedar-secrets)   TEST_MODULE="cedar_secrets_e2e" ;;
    cedar-security)  TEST_MODULE="cedar_security_e2e" ;;
    secrets)         TEST_MODULE="secrets_e2e" ;;
    scaling)         TEST_MODULE="scaling_e2e" ;;
    capi)            TEST_MODULE="capi_e2e" ;;
    kubeconfig)      TEST_MODULE="kubeconfig_e2e" ;;
    autoscaling)     TEST_MODULE="autoscaling_e2e" ;;
    oidc)            TEST_MODULE="oidc_e2e" ;;
    pivot)           TEST_MODULE="pivot_standalone_e2e" ;;
    upgrade)         TEST_MODULE="upgrade_e2e" ;;
    independence)    TEST_MODULE="docker_independence_e2e" ;;
    media-server)    TEST_MODULE="media_server_e2e" ;;
    *)               TEST_MODULE="${TEST_NAME}_e2e" ;;
esac

# Disable extras (monitoring, etc.) for resource-heavy multi-cluster tests
case "$TEST_NAME" in
    workload2) export LATTICE_DISABLE_EXTRAS=true ;;
esac

echo "Selected E2E test: $TEST_MODULE"
echo

# Clean up previous test runs
echo "Cleaning up previous test runs..."
kind delete cluster --name lattice-bootstrap 2>/dev/null || true
kind delete cluster --name e2e-mgmt 2>/dev/null || true
kind delete cluster --name e2e-workload 2>/dev/null || true
kind delete cluster --name e2e-workload2 2>/dev/null || true

# Clean up any orphaned CAPD containers
docker rm -f $(docker ps -aq --filter "name=e2e-mgmt" --filter "name=e2e-workload") 2>/dev/null || true

echo "Cleanup complete."
echo

export LATTICE_MGMT_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/docker-mgmt.yaml"
export LATTICE_WORKLOAD_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/docker-workload.yaml"
export LATTICE_WORKLOAD2_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/docker-workload2.yaml"
export LATTICE_ENABLE_INDEPENDENCE_TEST=true
export LATTICE_ENABLE_HIERARCHY_TEST=true
export LATTICE_ENABLE_MESH_TEST=true

# Docker-compose uses the external kind network
export LATTICE_NETWORK=kind
export LATTICE_NETWORK_EXTERNAL=true

# Build the lattice CLI without FIPS so kubectl can use it as an exec credential plugin.
# The FIPS build produces a dynamic library (libaws_lc_fips_*_crypto.dylib) that macOS
# can't locate at runtime due to missing @rpath, causing credential exec failures.
echo "Building lattice CLI (non-FIPS)..."
cargo build -p lattice-cli --no-default-features
export PATH="$REPO_ROOT/target/debug:$PATH"

echo "Management cluster config: $LATTICE_MGMT_CLUSTER_CONFIG"
echo "Workload cluster config: $LATTICE_WORKLOAD_CLUSTER_CONFIG"
echo "Workload2 cluster config: $LATTICE_WORKLOAD2_CLUSTER_CONFIG"
echo

RUST_LOG=info cargo test -p lattice-cli --features provider-e2e --test e2e "$TEST_MODULE" -- --nocapture
