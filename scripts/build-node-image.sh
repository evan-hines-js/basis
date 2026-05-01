#!/bin/bash
# Build the lattice-node VM image.
#
# Produces an uncompressed qcow2 containing an Ubuntu 24.04 base with
# kubelet / kubeadm / containerd pre-installed for a specific Kubernetes
# version, then pushes it as an OCI artifact so basis-agent can pull it.
#
# We push qcow2 (sparse, ~2 GiB) rather than raw (~10 GiB logical) so
# uploads stay tolerable. basis-agent converts the qcow2 to raw locally
# on first pull — cloud-hypervisor 45 needs a raw backing file for
# overlay disks (qcow2-backed-by-qcow2 trips EINVAL on io_uring reads).
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

# If a prior run was SIGKILL'd, its trap never fired and we've inherited
# mounts, a running qemu-nbd daemon, and/or stuck kernel state on the nbd
# device (manifests as "Input/output error" when reconnecting). Force a
# full reset before touching anything.
reclaim_nbd() {
    set +e
    # 1. Unmount leftover mounts backed by our nbd device. `tac` so we
    #    unmount children (/dev, /dev/pts, ...) before their parents.
    awk -v dev="$NBD_DEV" '$1 ~ "^"dev {print $2}' /proc/mounts \
        | tac | xargs -r -n1 umount -l 2>/dev/null

    # 2. Graceful disconnect. A stale qemu-nbd daemon can block rmmod; if
    #    disconnect doesn't reap it, kill explicitly.
    qemu-nbd --disconnect "$NBD_DEV" 2>/dev/null
    pgrep -f "qemu-nbd.*$NBD_DEV" | xargs -r kill 2>/dev/null
    sleep 1
    pgrep -f "qemu-nbd.*$NBD_DEV" | xargs -r kill -9 2>/dev/null

    # 3. Force-reload the nbd module. A prior unclean disconnect leaves
    #    per-device state stuck inside the kernel — rmmod+modprobe is the
    #    only userspace way to clear it short of a reboot. The main script
    #    re-modprobes below before qemu-nbd --connect.
    if lsmod | grep -q '^nbd '; then
        rmmod nbd 2>/dev/null
    fi
    set -e
}
reclaim_nbd

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
# Explicit kernel install. Ubuntu's minimal cloud image ships
# `linux-image-virtual` (kernel-stub only); we replace it with the
# generic meta-package so `/boot/vmlinuz-*` + `/boot/initrd.img-*`
# exist on-disk for direct-kernel boot (see build-node-image.sh top).
apt-get install -y linux-image-generic
apt-get install -y curl ca-certificates apt-transport-https gnupg socat conntrack ebtables ethtool containerd chrony
# chrony replaces systemd-timesyncd: timesyncd's default poll stretches to
# ~34min and lets VM clocks drift 20–30ms between corrections, which trips
# Ceph's 50ms MON_CLOCK_SKEW threshold when two mons drift in opposite
# directions. chrony polls aggressively and disciplines the kernel clock
# continuously, holding VMs inside ±1ms.
systemctl disable --now systemd-timesyncd 2>/dev/null || true
systemctl enable chrony

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

# Extract the guest kernel and initrd *before* cleanup unmounts the
# rootfs — cloud-hypervisor's minimal firmware (rust-hypervisor-firmware)
# doesn't implement the UEFI variable / TPM / Secure Boot surface that
# Ubuntu Noble's shim+grub depend on, so the EFI-chained boot on Noble
# gets the kernel running but never loads an initrd and panics mounting
# rootfs. We skip the entire chain by booting the kernel directly with
# `cloud-hypervisor --kernel --initramfs`.
#
# Ubuntu cloud images don't ship `/boot/vmlinuz` / `/boot/initrd.img`
# symlinks — only the versioned files from the installed kernel package.
# Glob to find them; there's exactly one each after a fresh install.
KERNEL_OUT="$WORK_DIR/lattice-node-v${K8S_VERSION}.vmlinuz"
INITRD_OUT="$WORK_DIR/lattice-node-v${K8S_VERSION}.initrd"
echo ""
echo "=== $MOUNT_DIR/boot contents (root of mounted rootfs) ==="
ls -la "$MOUNT_DIR/boot" 2>&1 || true
echo ""
echo "=== device partitions on $NBD_DEV ==="
lsblk -f "$NBD_DEV" 2>&1 || true
echo ""
echo "=== current mounts from our nbd device ==="
grep "$NBD_DEV" /proc/mounts || true
echo ""

shopt -s nullglob
KERNEL_SRCS=("$MOUNT_DIR"/boot/vmlinuz-*)
INITRD_SRCS=("$MOUNT_DIR"/boot/initrd.img-*)
shopt -u nullglob

if [[ ${#KERNEL_SRCS[@]} -ne 1 ]]; then
    echo "ERROR: expected exactly one vmlinuz-*, found ${#KERNEL_SRCS[@]}"
    exit 1
fi
if [[ ${#INITRD_SRCS[@]} -ne 1 ]]; then
    echo "ERROR: expected exactly one initrd.img-*, found ${#INITRD_SRCS[@]}"
    exit 1
fi
cp "${KERNEL_SRCS[0]}" "$KERNEL_OUT"
cp "${INITRD_SRCS[0]}" "$INITRD_OUT"

cleanup

# We deliberately leave the qcow2 clusters as-is (Ubuntu ships them
# compressed). That keeps the OCI upload small; basis-agent strips the
# compression locally after pull since cloud-hypervisor can't read
# compressed clusters at runtime.

echo "Built $OUTPUT_IMAGE"
echo "Kernel  $KERNEL_OUT"
echo "Initrd  $INITRD_OUT"

if [[ "$PUSH" == "--push" ]]; then
    echo "Pushing $IMAGE_TAG..."
    if ! command -v oras >/dev/null; then
        echo "Error: oras CLI not installed. See https://oras.land/docs/installation/"
        exit 1
    fi
    # Capture the digest of the manifest we're about to push. `oras push`
    # can report "Pushed" even when the manifest PUT was denied (HEAD
    # errors are printed but don't fail the process), so we verify the
    # registry actually serves the digest we just uploaded.
    PUSH_OUT=$(cd "$WORK_DIR" && oras push "$IMAGE_TAG" \
        "lattice-node-v${K8S_VERSION}.qcow2:application/vnd.lattice.node.v1+qcow2" \
        "lattice-node-v${K8S_VERSION}.vmlinuz:application/vnd.lattice.node.v1+kernel" \
        "lattice-node-v${K8S_VERSION}.initrd:application/vnd.lattice.node.v1+initrd" 2>&1)
    echo "$PUSH_OUT"
    LOCAL_DIGEST=$(echo "$PUSH_OUT" | awk '/^Digest:/ {print $2}')
    if [[ -z "$LOCAL_DIGEST" ]]; then
        echo "Error: oras push did not report a Digest — push likely failed"
        exit 1
    fi
    REMOTE_DIGEST=$(oras manifest fetch --descriptor "$IMAGE_TAG" 2>/dev/null | jq -r .digest)
    if [[ "$LOCAL_DIGEST" != "$REMOTE_DIGEST" ]]; then
        echo "Error: registry is serving a stale manifest for $IMAGE_TAG"
        echo "  pushed: $LOCAL_DIGEST"
        echo "  remote: $REMOTE_DIGEST"
        echo "  → check your oras login / token scopes (write:packages required)"
        exit 1
    fi
    echo "Pushed $IMAGE_TAG (verified digest $REMOTE_DIGEST)"
fi
