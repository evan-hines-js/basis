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
# Customization approach: mount the qcow2 via qemu-nbd and chroot into
# it. This uses the host's network directly — no nested appliance VM,
# no libguestfs DNS issues. Must run on Linux as root.
#
# Dependencies (on the build host):
#   - qemu-utils (qemu-nbd, qemu-img)
#   - cloud-guest-utils (growpart)
#   - e2fsprogs (resize2fs)
#   - parted (partprobe)
#   - curl
#   - oras (for --push)
#
# Usage:
#   sudo ./scripts/build-node-image.sh 1.32.0            # build only
#   sudo ./scripts/build-node-image.sh 1.32.0 --push     # build + push to ghcr

set -euo pipefail

K8S_VERSION="${1:-}"
PUSH="${2:-}"
if [[ -z "$K8S_VERSION" ]]; then
    echo "Usage: $0 <k8s-version> [--push]"
    echo "Example: $0 1.32.0 --push"
    exit 1
fi

if [[ $EUID -ne 0 ]]; then
    echo "Error: must run as root (needs qemu-nbd, mount, chroot)"
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

NBD_DEV="${NBD_DEV:-/dev/nbd0}"
MOUNT_DIR="$(mktemp -d -t lattice-node-build.XXXXXX)"

cleanup() {
    set +e
    umount "$MOUNT_DIR/dev/pts" 2>/dev/null
    umount "$MOUNT_DIR/dev" 2>/dev/null
    umount "$MOUNT_DIR/proc" 2>/dev/null
    umount "$MOUNT_DIR/sys" 2>/dev/null
    umount "$MOUNT_DIR/run" 2>/dev/null
    umount "$MOUNT_DIR" 2>/dev/null
    qemu-nbd --disconnect "$NBD_DEV" 2>/dev/null
    rmdir "$MOUNT_DIR" 2>/dev/null
}
trap cleanup EXIT

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

modprobe nbd max_part=16
qemu-nbd --connect="$NBD_DEV" "$OUTPUT_IMAGE"
partprobe "$NBD_DEV" || true
# Give the kernel a moment to populate partition device nodes.
for _ in 1 2 3 4 5; do
    [[ -b "${NBD_DEV}p1" ]] && break
    sleep 1
done

# Grow partition 1 + its filesystem to fill the resized image.
growpart "$NBD_DEV" 1 || true
e2fsck -fy "${NBD_DEV}p1" || true
resize2fs "${NBD_DEV}p1"

mount "${NBD_DEV}p1" "$MOUNT_DIR"

# Bind-mounts needed for the chroot's apt, gpg, etc.
mount --bind /dev "$MOUNT_DIR/dev"
mount --bind /dev/pts "$MOUNT_DIR/dev/pts"
mount -t proc proc "$MOUNT_DIR/proc"
mount -t sysfs sys "$MOUNT_DIR/sys"
mount -t tmpfs tmpfs "$MOUNT_DIR/run"

# Use host DNS inside the chroot. Ubuntu cloud images ship resolv.conf
# as a symlink to systemd-resolved's stub; replace it with a real file
# for the customize phase. systemd-resolved rewrites this on first boot.
rm -f "$MOUNT_DIR/etc/resolv.conf"
cp /etc/resolv.conf "$MOUNT_DIR/etc/resolv.conf"

chroot "$MOUNT_DIR" /bin/bash -euo pipefail <<CHROOT_EOF
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get upgrade -y -o Dpkg::Options::=--force-confnew
apt-get install -y curl ca-certificates apt-transport-https gnupg socat conntrack ebtables ethtool containerd

mkdir -p /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v${K8S_MINOR}/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v${K8S_MINOR}/deb/ /' > /etc/apt/sources.list.d/kubernetes.list

apt-get update
apt-get install -y kubelet=${K8S_VERSION}-1.1 kubeadm=${K8S_VERSION}-1.1 kubectl=${K8S_VERSION}-1.1
apt-mark hold kubelet kubeadm kubectl

mkdir -p /etc/containerd
containerd config default > /etc/containerd/config.toml
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
systemctl enable containerd kubelet

printf 'br_netfilter\noverlay\n' > /etc/modules-load.d/k8s.conf
printf 'net.bridge.bridge-nf-call-iptables = 1\nnet.bridge.bridge-nf-call-ip6tables = 1\nnet.ipv4.ip_forward = 1\n' > /etc/sysctl.d/99-k8s.conf

sed -i '/ swap / s/^/#/' /etc/fstab
truncate -s 0 /etc/machine-id
CHROOT_EOF

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
