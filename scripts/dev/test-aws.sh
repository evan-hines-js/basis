#!/bin/bash
# E2E test with AWS provider (CAPA)
#
# Required environment variables for credentials:
#   AWS_ACCESS_KEY_ID - AWS access key
#   AWS_SECRET_ACCESS_KEY - AWS secret access key
#   AWS_REGION - AWS region (e.g., us-west-2)
#
# Optional:
#   AWS_SESSION_TOKEN - Session token for temporary credentials
#
# The installer will create the credentials secret automatically.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Verify required credentials
if [[ -z "$AWS_ACCESS_KEY_ID" ]]; then
    echo "Error: AWS_ACCESS_KEY_ID environment variable required"
    exit 1
fi
if [[ -z "$AWS_SECRET_ACCESS_KEY" ]]; then
    echo "Error: AWS_SECRET_ACCESS_KEY environment variable required"
    exit 1
fi
if [[ -z "$AWS_REGION" ]]; then
    echo "Error: AWS_REGION environment variable required"
    exit 1
fi

# Generate AWS_B64ENCODED_CREDENTIALS for CAPA
# This is the format the CAPI AWS provider expects
AWS_CREDENTIALS="[default]
aws_access_key_id = $AWS_ACCESS_KEY_ID
aws_secret_access_key = $AWS_SECRET_ACCESS_KEY
region = $AWS_REGION"

if [[ -n "$AWS_SESSION_TOKEN" ]]; then
    AWS_CREDENTIALS="$AWS_CREDENTIALS
aws_session_token = $AWS_SESSION_TOKEN"
fi

export AWS_B64ENCODED_CREDENTIALS=$(echo "$AWS_CREDENTIALS" | base64 | tr -d '\n')

export LATTICE_MGMT_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/aws-mgmt.yaml"
export LATTICE_WORKLOAD_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/aws-workload.yaml"
export LATTICE_WORKLOAD2_CLUSTER_CONFIG="$REPO_ROOT/crates/lattice-cli/tests/e2e/fixtures/clusters/aws-workload2.yaml"
export LATTICE_ENABLE_INDEPENDENCE_TEST=true
export LATTICE_ENABLE_HIERARCHY_TEST=true
export LATTICE_ENABLE_MESH_TEST=true

# Build the lattice CLI without FIPS so kubectl can use it as an exec credential plugin.
# The FIPS build produces a dynamic library (libaws_lc_fips_*_crypto.dylib) that macOS
# can't locate at runtime due to missing @rpath, causing credential exec failures.
echo "Building lattice CLI (non-FIPS)..."
cargo build -p lattice-cli --no-default-features
export PATH="$REPO_ROOT/target/debug:$PATH"

echo "AWS Region: $AWS_REGION"
echo "Management cluster config: $LATTICE_MGMT_CLUSTER_CONFIG"
echo "Workload cluster config: $LATTICE_WORKLOAD_CLUSTER_CONFIG"
echo "Workload2 cluster config: $LATTICE_WORKLOAD2_CLUSTER_CONFIG"
echo

RUST_LOG=info cargo test -p lattice-cli --features provider-e2e --test e2e unified_e2e -- --nocapture
