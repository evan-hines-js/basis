#!/bin/bash
# Investigate a basis VM's disk state. Usage:
#   ./investigate-vm.sh <vm-id>
# Run on the hypervisor host as root. Works even with the VM running
# (mounts the guest rootfs read-only).

set -u
VM_ID="${1:-}"
if [[ -z "$VM_ID" ]]; then
  echo "usage: $0 <vm-id>" >&2
  exit 1
fi

LV="basis/vm-${VM_ID}"
LV_DEV="/dev/basis/vm-${VM_ID}"
VM_DIR="/var/lib/basis/vms/${VM_ID}"
MNT="/tmp/basis-investigate-${VM_ID}"

hr() { printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }

hr "LV attributes and sizes"
lvs --units g --noheadings -o lv_name,lv_attr,lv_size,data_percent,origin "$LV" 2>&1
echo "lv_attr chars: 1=type (V=thin) 2=perm (w=rw r=ro) 6=state (a=active)"

hr "block-layer read-only check"
blockdev --getro "$LV_DEV" 2>&1
echo "0=rw, 1=ro"

hr "partition table (forces kernel to re-scan)"
# partx is in util-linux — always available. -a adds partitions; -s lists.
partx -a "$LV_DEV" 2>&1 || true
partx -s "$LV_DEV" 2>&1
lsblk "$LV_DEV"

hr "attempt read-only mount of rootfs"
mkdir -p "$MNT"
# partx creates /dev/basis/vm-<id>1 style nodes on modern kernels.
# Try both variants.
ROOT_PART=""
for c in "${LV_DEV}1" "${LV_DEV}p1" "/dev/mapper/basis-vm--${VM_ID//-/--}1"; do
  [[ -b "$c" ]] && ROOT_PART="$c" && break
done

if [[ -z "$ROOT_PART" ]]; then
  echo "no p1 device created by partx; listing devices under /dev/mapper:"
  ls -la /dev/mapper/ | grep "${VM_ID:0:12}" || true
  echo "and raw /dev entries:"
  ls -la /dev/basis/ | grep "${VM_ID:0:12}" || true
fi

if [[ -n "$ROOT_PART" ]]; then
  echo "mounting $ROOT_PART -> $MNT (ro, nouuid to allow shared mount)"
  # `-o ro,noload,norecovery` is safest when the FS is live-mounted in the guest.
  if mount -o ro,noload "$ROOT_PART" "$MNT" 2>&1; then

    hr "guest filesystem size (inside the VM right now)"
    df -h "$MNT"

    hr "cloud-init log: growpart, resize, error, warn"
    for log in "$MNT/var/log/cloud-init.log" "$MNT/var/log/cloud-init-output.log"; do
      if [[ -f "$log" ]]; then
        echo "--- $log ---"
        grep -niE "growpart|resize_rootfs|resize2fs|read.only|EROFS|sector.0|partition.*grew|failed to" "$log" | head -40
        echo ""
      fi
    done

    hr "cloud-init module run order (was growpart even scheduled?)"
    for cfg in "$MNT/etc/cloud/cloud.cfg" "$MNT/etc/cloud/cloud.cfg.d/"*.cfg; do
      [[ -f "$cfg" ]] || continue
      echo "--- $cfg ---"
      grep -A 20 "cloud_init_modules\|cloud_config_modules\|cloud_final_modules" "$cfg" 2>/dev/null | head -40
      echo ""
    done

    hr "evidence of growpart actually running"
    [[ -d "$MNT/var/lib/cloud/instance/sem" ]] && ls "$MNT/var/lib/cloud/instance/sem/" 2>&1
    echo ""
    echo "Expected presence of: config_growpart, config_resizefs"

    umount "$MNT" 2>&1
  else
    echo "mount failed — FS state may be inconsistent from RO writes"
    # Retry with -o norecovery to suppress journal replay on a live FS
    mount -o ro,noload,norecovery "$ROOT_PART" "$MNT" 2>&1 && {
      hr "second-attempt: cloud-init log"
      grep -niE "growpart|resize" "$MNT/var/log/cloud-init.log" 2>&1 | head -30
      umount "$MNT"
    }
  fi
fi

rmdir "$MNT" 2>/dev/null

hr "cidata userdata format (first line tells cloud-init how to process)"
if [[ -f "$VM_DIR/cidata.iso" ]]; then
  mkdir -p "$MNT-ci"
  mount -o loop,ro "$VM_DIR/cidata.iso" "$MNT-ci" 2>&1
  echo "First line: $(head -1 "$MNT-ci/user-data")"
  echo "File size: $(stat -c%s "$MNT-ci/user-data") bytes"
  umount "$MNT-ci"
  rmdir "$MNT-ci"
fi

hr "done — key things to look for above"
cat <<'EOF'
  - lv_attr char 2 should be 'w' (was already confirmed rw)
  - "df -h /" inside the guest: if 10G, growpart didn't run. if ~40G, it did.
  - "growpart" entries in cloud-init.log: success, skip, or error?
  - cloud_init_modules in cloud.cfg: is `growpart` listed? `resizefs`?
  - /var/lib/cloud/instance/sem: presence of config_growpart / config_resizefs
    proves those modules ran at least once.
EOF
