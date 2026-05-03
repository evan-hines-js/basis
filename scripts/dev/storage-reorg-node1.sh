#!/usr/bin/env bash
# Storage reorg for node-1 (poweredge-lg, 10.0.0.97).
#
# End state:
#   /dev/sdf1   500 GiB    PVE thin pool `pve-data/data`, registered as
#                          PVE storage `data`. Holds PVE VM disks
#                          (proxmox-backup-server, ubuntu24-build,
#                          template 9000).
#   /dev/sdf2   ~430 GiB   basis rootfs VG `basis` (BX500 consumer
#                          SATA). Acceptable here only because the
#                          homelab runs 1 CP replica pinned to node-2
#                          (PLP rootfs); workers landing here keep
#                          etcd off the consumer disk. basis-prereqs
#                          provisions the thin pool inside it on next
#                          ansible run.
#   /dev/sd{a,c,d,e}       4× Intel PLP enterprise SSDs, one VG per
#                          device (`basis-fast-{sda,sdc,sdd,sde}`).
#                          Form the basis `tier=fast` data pool.
#                          ~744 GiB raw across four devices.
#   /dev/sdb               consumer SATA, untouched.
#
# Idempotent. Every phase checks state and skips work that's already
# done; safe to re-run after an interrupt.
#
# Run on the host as root.

set -euo pipefail

PVE_DATA_VG=pve-data
PVE_DATA_LV=data
SDF_PVE_PART_GIB=500

BASIS_ROOTFS_VG=basis

PLP_DRIVES=(sda sdc sdd sde)
PVE_VM_IDS=(100 101 9000)

# ----- helpers ------------------------------------------------------

cyan='\033[1;36m'; yellow='\033[1;33m'; red='\033[1;31m'; gray='\033[0;90m'; nc='\033[0m'
log()  { printf "${cyan}== %s ==${nc}\n" "$*"; }
step() { printf "   ${gray}•${nc} %s\n" "$*"; }
warn() { printf "${yellow}[!] %s${nc}\n" "$*" >&2; }
die()  { printf "${red}[FAIL] %s${nc}\n" "$*" >&2; exit 1; }

vg_exists()      { vgs --noheadings -o vg_name "$1" >/dev/null 2>&1; }
lv_exists()      { lvs --noheadings -o lv_name "$1/$2" >/dev/null 2>&1; }
pv_exists()      { pvs --noheadings -o pv_name "$1" >/dev/null 2>&1; }
pv_in_vg()       { [[ "$(pvs --noheadings -o vg_name "$1" 2>/dev/null | awk '{$1=$1};1')" == "$2" ]]; }
pvesm_defined()  { grep -qE "^[[:alpha:]]+: $1\$" /etc/pve/storage.cfg; }
zpool_exists()   { zpool list -H -o name | grep -qx "$1"; }
vm_stopped()     { [[ "$(qm status "$1" 2>/dev/null | awk '{print $2}')" == "stopped" ]]; }

require_cmds() {
  local missing=()
  for c in "$@"; do command -v "$c" >/dev/null 2>&1 || missing+=("$c"); done
  [[ ${#missing[@]} -eq 0 ]] || die "missing required commands: ${missing[*]}"
}

wait_for_block() {
  local dev=$1 n=${2:-10}
  for _ in $(seq 1 "$n"); do [[ -b "$dev" ]] && return 0; sleep 1; done
  return 1
}

reread_pt() {
  local dev=$1
  if   command -v partprobe >/dev/null;     then partprobe "$dev"
  elif command -v partx     >/dev/null;     then partx -u "$dev" || true
  elif command -v blockdev  >/dev/null;     then blockdev --rereadpt "$dev" || true
  else die "no way to re-read partition table (need partprobe/partx/blockdev)"
  fi
}

shutdown_vm() {
  local vmid=$1
  vm_stopped "$vmid" && return 0
  step "shutting down VM $vmid"
  qm shutdown "$vmid" || true
  for _ in $(seq 1 60); do vm_stopped "$vmid" && return 0; sleep 2; done
  warn "VM $vmid did not stop in 120s; force-killing"
  qm stop "$vmid"
}

# ----- phases -------------------------------------------------------

phase_prereqs() {
  log "Phase 0: prereqs"
  require_cmds sgdisk pvcreate vgcreate lvcreate vgremove pvremove wipefs \
               lsblk pvesm qm zpool zfs systemctl
  [[ $EUID -eq 0 ]] || die "must run as root"
}

phase_stop_agent() {
  log "Phase 1: stop basis-agent"
  if systemctl is-active --quiet basis-agent; then
    step "stopping basis-agent"; systemctl stop basis-agent
  else
    step "already stopped"
  fi
}

phase_partition_sdf() {
  log "Phase 2: partition /dev/sdf into sdf1 (${SDF_PVE_PART_GIB}GiB) + sdf2 (rest)"
  if [[ -b /dev/sdf1 && -b /dev/sdf2 ]]; then
    step "sdf1 + sdf2 already present"
    return 0
  fi
  if pv_exists /dev/sdf; then
    die "/dev/sdf is a single-PV — refusing to repartition with live VG on it"
  fi
  step "sgdisk: zero + create sdf1 + sdf2"
  sgdisk -Z /dev/sdf >/dev/null
  sgdisk -n "1:1MiB:+${SDF_PVE_PART_GIB}GiB" /dev/sdf >/dev/null
  sgdisk -n "2:0:0"                          /dev/sdf >/dev/null
  reread_pt /dev/sdf
  wait_for_block /dev/sdf1 10 || die "/dev/sdf1 didn't appear"
  wait_for_block /dev/sdf2 10 || die "/dev/sdf2 didn't appear"
  step "sdf1 + sdf2 ready"
}

phase_pve_data() {
  log "Phase 3: PVE thin pool ${PVE_DATA_VG}/${PVE_DATA_LV} on /dev/sdf1"
  pv_exists /dev/sdf1                    || pvcreate /dev/sdf1
  vg_exists "$PVE_DATA_VG"               || vgcreate "$PVE_DATA_VG" /dev/sdf1
  if ! lv_exists "$PVE_DATA_VG" "$PVE_DATA_LV"; then
    step "lvcreate thin pool"
    lvcreate --thinpool "$PVE_DATA_LV" --extents 95%FREE \
             --chunksize 256K --poolmetadatasize 1G "$PVE_DATA_VG"
  fi
  if ! pvesm_defined data; then
    step "pvesm add lvmthin data"
    pvesm add lvmthin data \
      --vgname "$PVE_DATA_VG" --thinpool "$PVE_DATA_LV" \
      --content images,rootdir
  else
    step "PVE storage 'data' already defined"
  fi
}

# If basis VG exists but its PV isn't /dev/sdf2, peel it off so we
# can rebuild on sdf2. Refuses if the VG holds anything beyond the
# rootfs thin pool — that means live VMs.
phase_relocate_rootfs_if_needed() {
  vg_exists "$BASIS_ROOTFS_VG" || return 0
  pv_in_vg /dev/sdf2 "$BASIS_ROOTFS_VG" && return 0

  log "Phase 4a: relocate $BASIS_ROOTFS_VG off prior PV(s) onto /dev/sdf2"
  local foreign
  foreign=$(lvs --noheadings -o lv_name "$BASIS_ROOTFS_VG" | awk '{$1=$1};1' | grep -vx data || true)
  if [[ -n "$foreign" ]]; then
    warn "$BASIS_ROOTFS_VG contains LVs other than the thin pool:"
    echo "$foreign" | sed 's/^/      /'
    die "Stop running VMs and remove these LVs first."
  fi

  if lv_exists "$BASIS_ROOTFS_VG" data; then
    step "lvremove $BASIS_ROOTFS_VG/data"
    lvremove -f "$BASIS_ROOTFS_VG/data"
  fi

  # Capture every PV in the VG before tearing it down.
  mapfile -t old_pvs < <(pvs --noheadings -o pv_name -S "vg_name=$BASIS_ROOTFS_VG" | awk '{$1=$1};1')
  step "vgremove $BASIS_ROOTFS_VG"
  vgremove -f "$BASIS_ROOTFS_VG"
  for p in "${old_pvs[@]}"; do
    [[ -z "$p" ]] && continue
    step "pvremove $p"
    pvremove -f "$p" || true
  done
}

phase_basis_rootfs_vg() {
  log "Phase 4: basis rootfs VG '$BASIS_ROOTFS_VG' on /dev/sdf2"
  pv_exists /dev/sdf2          || pvcreate /dev/sdf2
  vg_exists "$BASIS_ROOTFS_VG" || vgcreate "$BASIS_ROOTFS_VG" /dev/sdf2
  step "VG ready (basis-prereqs role creates the thin pool LV)"
}

phase_migrate_vms() {
  log "Phase 5: migrate PVE VM disks ent-zfspool → data"
  if ! zpool_exists ent-zfspool; then
    step "ent-zfspool already gone — checking for orphaned references"
    local orphans=()
    for vmid in "${PVE_VM_IDS[@]}"; do
      qm config "$vmid" 2>/dev/null | grep -q "ent-zfspool:" && orphans+=("$vmid")
    done
    if (( ${#orphans[@]} > 0 )); then
      warn "VMs still reference the destroyed pool: ${orphans[*]}"
      warn "Their disks are gone. Edit /etc/pve/qemu-server/<vmid>.conf to drop"
      warn "the ent-zfspool: lines, or recreate the VMs from backup."
    fi
    return 0
  fi

  for vmid in "${PVE_VM_IDS[@]}"; do
    qm config "$vmid" >/dev/null 2>&1 || { step "VM $vmid not present"; continue; }
    if ! qm config "$vmid" | grep -q "ent-zfspool:"; then
      step "VM $vmid has no disk on ent-zfspool"
      continue
    fi
    shutdown_vm "$vmid"
    mapfile -t slots < <(qm config "$vmid" | awk -F: '/ent-zfspool:/ {print $1}')
    for slot in "${slots[@]}"; do
      step "qm move-disk $vmid $slot → data"
      qm move-disk "$vmid" "$slot" data --delete 1
    done
  done

  step "destroying any leftover datasets"
  zfs list -H -o name -r ent-zfspool 2>/dev/null | tac | while read -r ds; do
    [[ "$ds" == "ent-zfspool" ]] && continue
    zfs destroy -r "$ds" 2>/dev/null || true
  done

  step "zpool destroy ent-zfspool"
  zpool destroy ent-zfspool
}

phase_basis_fast_pool() {
  log "Phase 6: basis fast pool — one VG per PLP drive (${PLP_DRIVES[*]})"
  for d in "${PLP_DRIVES[@]}"; do
    local dev="/dev/$d" vg="basis-fast-$d"
    if vg_exists "$vg" && pv_in_vg "$dev" "$vg"; then
      step "$dev → $vg (already configured)"; continue
    fi
    if pv_exists "$dev"; then
      step "$dev has stale PV header; pvremove first"
      pvremove --force "$dev"
    fi
    step "$dev: wipefs + pvcreate + vgcreate $vg"
    wipefs -a "$dev" >/dev/null
    pvcreate "$dev"
    vgcreate "$vg" "$dev"
  done
}

phase_report() {
  log "Phase 7: post-state"
  echo
  echo "--- VGs ---"
  vgs
  echo
  echo "--- PVE storage ---"
  pvesm status
  echo
  echo "--- PVE VM disk locations ---"
  for vmid in "${PVE_VM_IDS[@]}"; do
    qm config "$vmid" 2>/dev/null | awk -v vmid="$vmid" \
      '/^scsi[0-9]+:|^virtio[0-9]+:|^sata[0-9]+:|^ide[0-9]+:/ {print vmid": "$0}' || true
  done
  echo
  log "Reorg complete."
  echo "Next:"
  echo "  1. host_vars/node-1.yml: basis_lvm_devices=[/dev/sdf2],"
  echo "     fast pool with sda/sdc/sdd/sde, tier: bulk."
  echo "  2. Re-run basis-prereqs ansible role."
  echo "  3. systemctl start basis-agent."
}

main() {
  phase_prereqs
  phase_stop_agent
  phase_partition_sdf
  phase_pve_data
  phase_relocate_rootfs_if_needed
  phase_basis_rootfs_vg
  phase_migrate_vms
  phase_basis_fast_pool
  phase_report
}

main "$@"
