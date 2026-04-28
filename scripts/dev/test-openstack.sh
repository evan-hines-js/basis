#!/bin/bash
# E2E test with OpenStack provider (CAPO)
#
# Required environment variables:
#   OS_CLOUD - Cloud name from clouds.yaml (e.g., "ovh")
#   OPENSTACK_CLOUD_CONFIG - Path to clouds.yaml file
#
# Or set these directly:
#   OS_AUTH_URL - OpenStack auth URL
#   OS_USERNAME - OpenStack username
#   OS_PASSWORD - OpenStack password
#   OS_PROJECT_NAME - OpenStack project name
#   OS_USER_DOMAIN_NAME - OpenStack user domain
#   OS_PROJECT_DOMAIN_NAME - OpenStack project domain
#
# The installer will create the credentials secret automatically.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Verify required credentials
if [[ -z "$OS_CLOUD" && -z "$OS_AUTH_URL" ]]; then
    echo "Error: Either OS_CLOUD or OS_AUTH_URL environment variable required"
    echo ""
    echo "Option 1: Use clouds.yaml"
    echo "export OS_CLOUD=ovh"
    echo "export OPENSTACK_CLOUD_CONFIG=/path/to/clouds.yaml"
    echo ""
    echo "Option 2: Use environment variables"
    echo "export OS_AUTH_URL=https://auth.cloud.ovh.net/v3"
    echo "export OS_USERNAME=user@example.com"
    echo "export OS_PASSWORD=secret"
    echo "export OS_PROJECT_NAME=1234567890"
    echo "export OS_USER_DOMAIN_NAME=Default"
    echo "export OS_PROJECT_DOMAIN_NAME=Default"
    exit 1
fi

export LATTICE_MGMT_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/openstack-mgmt.yaml"
export LATTICE_WORKLOAD_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/openstack-workload.yaml"
export LATTICE_WORKLOAD2_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/openstack-workload2.yaml"
export LATTICE_ENABLE_INDEPENDENCE_TEST=true
export LATTICE_ENABLE_HIERARCHY_TEST=true
export LATTICE_ENABLE_MESH_TEST=true

# Build the lattice CLI without FIPS so kubectl can use it as an exec credential plugin.
# The FIPS build produces a dynamic library (libaws_lc_fips_*_crypto.dylib) that macOS
# can't locate at runtime due to missing @rpath, causing credential exec failures.
echo "Building lattice CLI (non-FIPS)..."
cargo build -p lattice-cli --no-default-features
export PATH="$REPO_ROOT/target/debug:$PATH"

echo "OpenStack Cloud: ${OS_CLOUD:-"(using env vars)"}"
echo "Management cluster config: $LATTICE_MGMT_CLUSTER_CONFIG"
echo "Workload cluster config: $LATTICE_WORKLOAD_CLUSTER_CONFIG"
echo "Workload2 cluster config: $LATTICE_WORKLOAD2_CLUSTER_CONFIG"
echo

RUST_LOG=info cargo test -p lattice-cli --features provider-e2e --test e2e unified_e2e -- --nocapture
