#!/bin/bash
# Build and push the basis-capi-provider container image.
#
# Tag comes from basis/Cargo.toml [workspace.package].version. Pushed to
# ghcr.io/evan-hines-js/basis-capi-provider:<tag>. Pass --push to actually push;
# default is build-only.
#
# The Deployment in lattice test-providers/infrastructure-basis/v0.1.0/
# references this image by the same tag, so bumping the basis workspace
# version also requires bumping versions.toml's [providers.infrastructure-basis]
# in lattice.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VERSION=$(awk -F '"' '/^version = / { print $2; exit }' "$REPO_ROOT/Cargo.toml")
if [[ -z "$VERSION" ]]; then
    echo "Error: could not extract version from Cargo.toml"
    exit 1
fi

IMAGE="ghcr.io/evan-hines-js/basis-capi-provider:v${VERSION}"

echo "Building $IMAGE"
docker build \
    --platform linux/amd64 \
    -t "$IMAGE" \
    "$REPO_ROOT"

if [[ "${1:-}" == "--push" ]]; then
    echo "Pushing $IMAGE"
    docker push "$IMAGE"
fi

echo "Built $IMAGE"
