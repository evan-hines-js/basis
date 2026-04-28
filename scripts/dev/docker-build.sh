#!/usr/bin/env bash
# Build Docker image using versions from versions.toml
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERSIONS_FILE="$PROJECT_ROOT/versions.toml"

# Parse versions.toml - handles both [section.subsection] and key = "value" formats
get_version() {
    local section=$1
    local subsection=$2
    local key=${3:-version}

    awk -v section="$section" -v subsection="$subsection" -v key="$key" '
        /^\[/ {
            # Extract section name, handling dots
            gsub(/[\[\]]/, "", $0)
            current_section = $0
        }
        current_section == section"."subsection && $1 == key {
            gsub(/[" ]/, "", $3)
            print $3
        }
    ' "$VERSIONS_FILE"
}

# Get tool versions
HELM_VERSION=$(get_version "tools" "helm")

# Get provider versions
CAPI_VERSION=$(get_version "providers" "cluster-api")
RKE2_VERSION=$(get_version "providers" "bootstrap-rke2")
CAPMOX_VERSION=$(get_version "providers" "infrastructure-proxmox")
CAPA_VERSION=$(get_version "providers" "infrastructure-aws")
CAPO_VERSION=$(get_version "providers" "infrastructure-openstack")
IPAM_VERSION=$(get_version "providers" "ipam-in-cluster")
CERTMANAGER_VERSION=$(get_version "providers" "cert-manager")

echo "Building with versions from versions.toml:"
echo "helm: $HELM_VERSION"
echo "capi: $CAPI_VERSION"
echo "rke2: $RKE2_VERSION"
echo "capmox: $CAPMOX_VERSION"
echo "capa: $CAPA_VERSION"
echo "capo: $CAPO_VERSION"
echo "ipam-in-cluster: $IPAM_VERSION"
echo "cert-manager: $CERTMANAGER_VERSION"

DOCKER_BUILDKIT=1 docker build \
    --build-arg HELM_VERSION="$HELM_VERSION" \
    --build-arg CAPI_VERSION="$CAPI_VERSION" \
    --build-arg RKE2_VERSION="$RKE2_VERSION" \
    --build-arg CAPMOX_VERSION="$CAPMOX_VERSION" \
    --build-arg CAPA_VERSION="$CAPA_VERSION" \
    --build-arg CAPO_VERSION="$CAPO_VERSION" \
    --build-arg IPAM_VERSION="$IPAM_VERSION" \
    --build-arg CERTMANAGER_VERSION="$CERTMANAGER_VERSION" \
    --platform linux/amd64 \
    --build-arg FIPS=true \
    "$@" \
    "$PROJECT_ROOT"
