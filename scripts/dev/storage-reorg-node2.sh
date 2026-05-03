#!/usr/bin/env bash
# Storage reorg for node-2 (poweredge-md, 10.0.0.206).
#
# Current state:
#   /dev/sd{b,c,d,e}   4× Intel PLP enterprise SSDs, all four PVs in
#                      one rootfs VG `basis` (host_vars sets
#                      basis_lvm_devices: sdb..sde).
#   /dev/pve/basis-data  320 GiB thick LV in the OS-disk `pve` VG,
#                        registered as fast pool (basis-fast-pve VG).
#
# End state:
#   /dev/sd{b,c}       rootfs thin pool VG `basis` (2× PLP, ~360 GiB).
#                      basis-prereqs (re)creates the thin pool LV.
#   /dev/sd{d,e}       2× PLP, one VG per device
#                      (`basis-fast-{sdd,sde}`). Form the fast data
#                      pool. ~360 GiB raw across two devices — enough
#                      for size=2 OSDs at failure-domain=osd.
#   /dev/pve/basis-data  reclaimed back into pve VG (lvremove).
#
# Refuses to run if there are any LVs in the `basis` VG that aren't
# the rootfs thin pool / image LVs we put there — i.e. if any VMs are
# live on this host. Run scripts/dev/drain-node.sh first or stop
# basis-agent and remove VM LVs manually.
#
# Idempotent. Run on the host as root.

set -euo pipefail

ROOTFS_VG=basis
ROOTFS_THINPOOL=data

OLD_FAST_VG=basis-fast-pve
OLD_FAST_LV=/dev/pve/basis-data

ROOTFS_DRIVES=(sdb sdc)
FAST_DRIVES=(sdd sde)

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

require_cmds() {
  local missing=()
  for c in "$@"; do command -v "$c" >/dev/null 2>&1 || missing+=("$c"); done
  [[ ${#missing[@]} -eq 0 ]] || die "missing required commands: ${missing[*]}"
}

# Names of every LV in the basis VG that is NOT the rootfs thin pool.
foreign_basis_lvs() {
  vg_exists "$ROOTFS_VG" || return 0
  lvs --noheadings -o lv_name "$ROOTFS_VG" \
    | awk '{$1=$1};1' \
    | grep -vx "$ROOTFS_THINPOOL" || true
}

# ----- phases -------------------------------------------------------

phase_prereqs() {
  log "Phase 0: prereqs"
  require_cmds pvcreate vgcreate vgreduce vgextend lvremove pvremove wipefs \
               vgs lvs pvs systemctl
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

phase_drain_check() {
  log "Phase 2: refuse if basis VG has live VM LVs"
  local foreign; foreign=$(foreign_basis_lvs)
  if [[ -n "$foreign" ]]; then
    warn "Found non-rootfs LVs in $ROOTFS_VG VG:"
    echo "$foreign" | sed 's/^/      /'
    die "These look like VM disks/snapshots. Remove them first (or drain the host) and re-run."
  fi
  step "no foreign LVs in $ROOTFS_VG"
}

phase_remove_old_fast_pool() {
  log "Phase 3: remove old fast pool ($OLD_FAST_VG → reclaim $OLD_FAST_LV)"
  if vg_exists "$OLD_FAST_VG"; then
    step "vgremove $OLD_FAST_VG"
    vgremove -f "$OLD_FAST_VG"
  else
    step "$OLD_FAST_VG already gone"
  fi
  if pv_exists "$OLD_FAST_LV"; then
    step "pvremove $OLD_FAST_LV"
    pvremove -f "$OLD_FAST_LV"
  fi
  if [[ -b "$OLD_FAST_LV" ]] && lv_exists pve basis-data; then
    step "lvremove $OLD_FAST_LV (reclaim into pve VG)"
    lvremove -f "$OLD_FAST_LV"
  else
    step "$OLD_FAST_LV already removed"
  fi
}

phase_shrink_rootfs_vg() {
  log "Phase 4: shrink $ROOTFS_VG to ${ROOTFS_DRIVES[*]} only"
  for d in "${FAST_DRIVES[@]}"; do
    local dev="/dev/$d"
    if pv_in_vg "$dev" "$ROOTFS_VG"; then
      step "vgreduce $ROOTFS_VG $dev (pvmove first if extents are in use)"
      # If extents are allocated on this PV, move them onto the remaining
      # PVs before reducing. pvmove is a no-op if nothing's on the PV.
      pvmove "$dev" || true
      vgreduce "$ROOTFS_VG" "$dev"
    else
      step "$dev not in $ROOTFS_VG"
    fi
    if pv_exists "$dev"; then
      step "pvremove $dev"
      pvremove -f "$dev"
    fi
  done
}

phase_basis_fast_pool() {
  log "Phase 5: build fast pool — one VG per drive on ${FAST_DRIVES[*]}"
  for d in "${FAST_DRIVES[@]}"; do
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
  log "Phase 6: post-state"
  echo
  echo "--- VGs ---"
  vgs
  echo
  echo "--- $ROOTFS_VG PVs ---"
  pvs -S "vg_name=$ROOTFS_VG"
  echo
  log "Reorg complete."
  echo "Next:"
  echo "  1. Update deploy/ansible/host_vars/node-2.yml:"
  echo "     - basis_lvm_devices: [/dev/sdb, /dev/sdc]"
  echo "     - basis_storage_pools: 'fast' pool with sdd/sde devices"
  echo "  2. Re-run basis-prereqs ansible role."
  echo "  3. systemctl start basis-agent."
}

main() {
  phase_prereqs
  phase_stop_agent
  phase_drain_check
  phase_remove_old_fast_pool
  phase_shrink_rootfs_vg
  phase_basis_fast_pool
  phase_report
}

main "$@"
