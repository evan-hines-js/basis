#!/bin/bash
# E2E test with Basis provider
#
# Credentials:
#   Put a `.basis.credentials` file in the lattice repo root (gitignored).
#   See scripts/dev/test-basis.credentials.example for the expected shape.
#
# What that file provides:
#   BASIS_CONTROLLER_URL - gRPC URL of the Basis controller, e.g. https://10.0.0.97:7443
#   BASIS_PKI_DIR        - Directory holding capi-provider.crt, capi-provider.key
#                          and ca.crt. Defaults to ../basis/deploy/ansible/pki/
#                          (the layout Ansible emits when provisioning Basis hosts).
#   GHCR_USER / GHCR_TOKEN - required to pull the private basis-capi-provider
#                          image from ghcr.io. The installer seeds these as an
#                          `image-registry-credentials` Secret and `basis-mgmt.yaml`
#                          declares a `default` ImageProvider wrapping them.
#
# The installer seeds the `basis-credentials` Secret in lattice-secrets (ESO
# source) from BASIS_CLIENT_CERT / BASIS_CLIENT_KEY / BASIS_CA_CERT. The user's
# InfraProvider in basis-mgmt.yaml references it via `credentials.id`.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CRED_FILE="$REPO_ROOT/.basis.credentials"
if [[ -f "$CRED_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$CRED_FILE"
fi

if [[ -z "$BASIS_CONTROLLER_URL" ]]; then
    echo "Error: BASIS_CONTROLLER_URL required (set in $CRED_FILE or export it)"
    exit 1
fi

BASIS_PKI_DIR="${BASIS_PKI_DIR:-$REPO_ROOT/../basis/deploy/ansible/pki}"
for f in capi-provider.crt capi-provider.key ca.crt; do
    if [[ ! -f "$BASIS_PKI_DIR/$f" ]]; then
        echo "Error: missing $BASIS_PKI_DIR/$f"
        echo "       Set BASIS_PKI_DIR in $CRED_FILE to the directory with"
        echo "       capi-provider.crt, capi-provider.key, ca.crt"
        exit 1
    fi
done

export BASIS_CONTROLLER_URL
export BASIS_CLIENT_CERT="$(cat "$BASIS_PKI_DIR/capi-provider.crt")"
export BASIS_CLIENT_KEY="$(cat "$BASIS_PKI_DIR/capi-provider.key")"
export BASIS_CA_CERT="$(cat "$BASIS_PKI_DIR/ca.crt")"

export LATTICE_MGMT_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/basis-mgmt.yaml"
export LATTICE_WORKLOAD_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/basis-workload.yaml"
export LATTICE_WORKLOAD2_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/basis-workload2.yaml"
export LATTICE_ENABLE_INDEPENDENCE_TEST=true
export LATTICE_ENABLE_HIERARCHY_TEST=true
export LATTICE_ENABLE_MESH_TEST=true
# Basis VMs come up with `dataDiskGibs` raw block devices Rook can claim,
# so the storage e2e is meaningful here. Defer to caller's value if set
# (so a developer iterating can opt out without editing the script).
export LATTICE_E2E_STORAGE="${LATTICE_E2E_STORAGE:-true}"

# Dev services host (Vault, Keycloak, registry mirrors via nginx proxy on bastion)
DEV_HOST="${LATTICE_DEV_HOST:-10.0.0.131}"
export LATTICE_VAULT_HOST_URL="http://${DEV_HOST}:8200"
export LATTICE_VAULT_INTERNAL_URL="http://${DEV_HOST}:8200"
export LATTICE_KEYCLOAK_HOST_URL="http://${DEV_HOST}:8080"
export LATTICE_KEYCLOAK_INTERNAL_URL="http://${DEV_HOST}:8080"

BASIS_REPO="${BASIS_REPO:-$REPO_ROOT/../basis}"

# Always refresh the basis provider bundle (CRDs + RBAC + Deployment)
# from the basis checkout so a stale `ipPool`-style schema in this
# repo can never reach the management cluster. Skip if the basis
# checkout isn't accessible — the committed snapshot is the fallback.
if [[ -x "$BASIS_REPO/scripts/generate-capi-components.sh" ]]; then
    echo "Regenerating basis provider bundle from $BASIS_REPO"
    LATTICE_REPO="$REPO_ROOT" \
        "$BASIS_REPO/scripts/generate-capi-components.sh" --sync-lattice
else
    echo "Warning: $BASIS_REPO/scripts/generate-capi-components.sh not found;" \
         "using the committed snapshot in test-providers/. Set BASIS_REPO to refresh."
fi

# Optionally (re)build and push the basis-capi-provider image before running.
# Useful when iterating on the basis CAPI provider — the Deployment in
# test-providers/infrastructure-basis/v0.1.0 pulls the image by tag.
if [[ "${BASIS_PUSH_IMAGE:-false}" == "true" ]]; then
    if [[ ! -x "$BASIS_REPO/scripts/build-capi-provider.sh" ]]; then
        echo "Error: $BASIS_REPO/scripts/build-capi-provider.sh not found or not executable"
        echo "       Set BASIS_REPO to the basis checkout, or disable BASIS_PUSH_IMAGE."
        exit 1
    fi
    "$BASIS_REPO/scripts/build-capi-provider.sh" --push
fi

echo "Building lattice CLI (non-FIPS)..."
cargo build -p lattice-cli --no-default-features
export PATH="$REPO_ROOT/target/debug:$PATH"

echo "Basis controller: $BASIS_CONTROLLER_URL"
echo "Basis PKI dir:    $BASIS_PKI_DIR"
echo "Management cluster config: $LATTICE_MGMT_CLUSTER_CONFIG"
echo "Workload cluster config:   $LATTICE_WORKLOAD_CLUSTER_CONFIG"
echo "Workload2 cluster config:  $LATTICE_WORKLOAD2_CLUSTER_CONFIG"
echo

RUST_LOG=info cargo test -p lattice-cli --features provider-e2e --test e2e unified_e2e -- --nocapture
