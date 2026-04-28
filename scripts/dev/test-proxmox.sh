#!/bin/bash
# E2E test with Proxmox provider (CAPMOX)
#
# Required environment variables for credentials:
#   PROXMOX_URL - Proxmox API URL (e.g., https://10.0.0.97:8006)
#   PROXMOX_TOKEN - API token ID (e.g., root@pam!lattice)
#   PROXMOX_SECRET - API token secret
#
# The installer will create the credentials secret automatically.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Verify required credentials
if [[ -z "$PROXMOX_URL" ]]; then
    echo "Error: PROXMOX_URL environment variable required"
    exit 1
fi
if [[ -z "$PROXMOX_TOKEN" ]]; then
    echo "Error: PROXMOX_TOKEN environment variable required"
    exit 1
fi
if [[ -z "$PROXMOX_SECRET" ]]; then
    echo "Error: PROXMOX_SECRET environment variable required"
    exit 1
fi

export LATTICE_MGMT_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/proxmox-mgmt.yaml"
export LATTICE_WORKLOAD_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/proxmox-workload.yaml"
export LATTICE_WORKLOAD2_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/proxmox-workload2.yaml"
export LATTICE_ENABLE_INDEPENDENCE_TEST=true
export LATTICE_ENABLE_HIERARCHY_TEST=true
export LATTICE_ENABLE_MESH_TEST=true

# Dev services host (Vault, Keycloak, registry mirrors via nginx proxy on bastion)
DEV_HOST="${LATTICE_DEV_HOST:-10.0.0.131}"
export LATTICE_VAULT_HOST_URL="http://${DEV_HOST}:8200"
export LATTICE_VAULT_INTERNAL_URL="http://${DEV_HOST}:8200"
export LATTICE_KEYCLOAK_HOST_URL="http://${DEV_HOST}:8080"
export LATTICE_KEYCLOAK_INTERNAL_URL="http://${DEV_HOST}:8080"

# Build the lattice CLI without FIPS so kubectl can use it as an exec credential plugin.
# The FIPS build produces a dynamic library (libaws_lc_fips_*_crypto.dylib) that macOS
# can't locate at runtime due to missing @rpath, causing credential exec failures.
echo "Building lattice CLI (non-FIPS)..."
cargo build -p lattice-cli --no-default-features
export PATH="$REPO_ROOT/target/debug:$PATH"

echo "Proxmox URL: $PROXMOX_URL"
echo "Management cluster config: $LATTICE_MGMT_CLUSTER_CONFIG"
echo "Workload cluster config: $LATTICE_WORKLOAD_CLUSTER_CONFIG"
echo "Workload2 cluster config: $LATTICE_WORKLOAD2_CLUSTER_CONFIG"
echo

RUST_LOG=info cargo test -p lattice-cli --features provider-e2e --test e2e unified_e2e -- --nocapture
