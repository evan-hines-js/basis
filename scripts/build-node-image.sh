#!/bin/bash
# Build the lattice-node VM image.
#
# Produces a qcow2 disk image containing an Ubuntu 24.04 base with
# kubelet / kubeadm / containerd pre-installed for a specific Kubernetes
# version, then pushes it as an OCI artifact so basis-agent can pull it
# via skopeo.
#
# Output tag: ghcr.io/evan-hines-js/lattice-node:v<K8S_VERSION>
#
# This is the image referenced by the BasisProvider in lattice-capi
# (crates/lattice-capi/src/provider/basis.rs#node_image_for).
#
# Dependencies (on the build host):
#   - libguestfs-tools (virt-customize)
#   - curl
#   - oras (or skopeo + a qcow2-as-blob workflow)
#
# Usage:
#   ./scripts/build-node-image.sh 1.32.0            # build only
#   ./scripts/build-node-image.sh 1.32.0 --push     # build + push to ghcr

set -euo pipefail

K8S_VERSION="${1:-}"
PUSH="${2:-}"
if [[ -z "$K8S_VERSION" ]]; then
    echo "Usage: $0 <k8s-version> [--push]"
    echo "Example: $0 1.32.0 --push"
    exit 1
fi

# Drop any leading "v"; kubeadm apt metadata uses bare numbers.
K8S_VERSION="${K8S_VERSION#v}"
K8S_MINOR="$(echo "$K8S_VERSION" | cut -d. -f1-2)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="${WORK_DIR:-$REPO_ROOT/build/images}"
mkdir -p "$WORK_DIR"

BASE_IMAGE_URL="https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
BASE_IMAGE="$WORK_DIR/noble-server-cloudimg-amd64.img"
OUTPUT_IMAGE="$WORK_DIR/lattice-node-v${K8S_VERSION}.qcow2"
IMAGE_TAG="ghcr.io/evan-hines-js/lattice-node:v${K8S_VERSION}"

if [[ ! -f "$BASE_IMAGE" ]]; then
    echo "Downloading Ubuntu 24.04 cloud image..."
    curl -fSL "$BASE_IMAGE_URL" -o "$BASE_IMAGE"
fi

echo "Building $OUTPUT_IMAGE from $BASE_IMAGE..."
cp "$BASE_IMAGE" "$OUTPUT_IMAGE"
# Grow the image so apt install has headroom. basis-agent creates a qcow2
# overlay on top at VM-creation time sized to the machine's diskGib, but
# the base has to fit kubeadm + containerd itself.
qemu-img resize "$OUTPUT_IMAGE" 10G

# Pre-install the Kubernetes stack so the CAPI bootstrap userdata only
# has to do kubeadm init/join, not package setup. Mirrors the pattern
# kubernetes-sigs/image-builder uses for CAPMOX templates.
virt-customize -a "$OUTPUT_IMAGE" \
    --update \
    --install curl,ca-certificates,apt-transport-https,gnupg,socat,conntrack,ebtables,ethtool,containerd \
    --run-command "mkdir -p /etc/apt/keyrings" \
    --run-command "curl -fsSL https://pkgs.k8s.io/core:/stable:/v${K8S_MINOR}/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg" \
    --run-command "echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v${K8S_MINOR}/deb/ /' > /etc/apt/sources.list.d/kubernetes.list" \
    --run-command "apt-get update && apt-get install -y kubelet=${K8S_VERSION}-1.1 kubeadm=${K8S_VERSION}-1.1 kubectl=${K8S_VERSION}-1.1" \
    --run-command "apt-mark hold kubelet kubeadm kubectl" \
    --run-command "mkdir -p /etc/containerd && containerd config default > /etc/containerd/config.toml" \
    --run-command "sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml" \
    --run-command "systemctl enable containerd kubelet" \
    --run-command "modprobe br_netfilter overlay || true" \
    --run-command "echo 'br_netfilter\noverlay' > /etc/modules-load.d/k8s.conf" \
    --run-command "echo 'net.bridge.bridge-nf-call-iptables = 1\nnet.bridge.bridge-nf-call-ip6tables = 1\nnet.ipv4.ip_forward = 1' > /etc/sysctl.d/99-k8s.conf" \
    --run-command "swapoff -a && sed -i '/ swap / s/^/#/' /etc/fstab" \
    --truncate /etc/machine-id

echo "Built $OUTPUT_IMAGE"

if [[ "$PUSH" == "--push" ]]; then
    echo "Pushing $IMAGE_TAG..."
    if ! command -v oras >/dev/null; then
        echo "Error: oras CLI not installed. See https://oras.land/docs/installation/"
        exit 1
    fi
    (cd "$WORK_DIR" && oras push "$IMAGE_TAG" \
        "lattice-node-v${K8S_VERSION}.qcow2:application/vnd.lattice.node.v1+qcow2")
    echo "Pushed $IMAGE_TAG"
fi
