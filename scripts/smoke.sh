#!/usr/bin/env bash
# Smoke test: end-to-end cluster + machine lifecycle against a running
# Basis controller. Exits 0 iff the full path works, fails loud on the
# first broken step.
#
# "Works" now means the VM is actually reachable over the network — not
# just that the controller thinks it's RUNNING. A VM that cloud-
# hypervisor started but whose guest OS hung at boot would pass the
# previous version of this test; it won't pass this one.
#
# Flags:
#   --keep     Don't tear down on success. Useful for poking at the VM.
#              On failure the VM is always kept so you can inspect the
#              serial journal on the hypervisor.
#
# Prereqs:
#   - A Basis controller reachable at $BASIS_ENDPOINT
#   - A Basis agent connected to that controller on some host
#   - The four env vars below set to valid cert paths
#   - The machine fixture's IP must be reachable from this host (bridge
#     layer-2 or routable), otherwise the reachability probe can't work

set -euo pipefail

: "${BASIS_ENDPOINT:?BASIS_ENDPOINT not set (e.g. https://10.0.0.206:7443)}"
: "${BASIS_TLS_CA:?BASIS_TLS_CA not set (path to ca.crt)}"
: "${BASIS_TLS_CERT:?BASIS_TLS_CERT not set (path to capi-provider.crt)}"
: "${BASIS_TLS_KEY:?BASIS_TLS_KEY not set (path to capi-provider.key)}"

KEEP=0
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=1;;
        *) echo "unknown arg: $arg" >&2; exit 2;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES="$REPO_ROOT/crates/basis-ctl/fixtures"
BIN="$REPO_ROOT/target/release/basis-ctl"

# Time to wait for the guest to come up on the network after CreateMachine
# returns. cloud-init + getty + sshd is typically 20-30s on Ubuntu noble
# on this hardware; 90s is generous.
BOOT_DEADLINE_SECONDS=90

cd "$REPO_ROOT"

step() { echo; echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# Build once up front. Any later invocation of `$BIN` is just the binary,
# no cargo noise interleaved with test output.
step "Build basis-ctl"
cargo build --release --quiet -p basis-ctl
[[ -x "$BIN" ]] || fail "basis-ctl did not build at $BIN"

# Idempotent cleanup — delete is a no-op (prints "not found, skipping")
# when the resource is already absent, so the first run of this script
# from a fresh controller still exits 0 through this block.
step "Clean slate"
"$BIN" delete -f "$FIXTURES/machine-debug.yaml"
"$BIN" delete -f "$FIXTURES/cluster.yaml"

step "Apply cluster"
"$BIN" apply -f "$FIXTURES/cluster.yaml"

# `apply machine` blocks (server-side) until the agent reports the VM
# started or the 60s timeout fires. "Started" here means cloud-hypervisor
# returned from boot, NOT that the guest OS is up. We still need the
# reachability probe below.
step "Apply machine (blocks until agent reports CreateVm completed)"
APPLY_OUT=$("$BIN" apply -f "$FIXTURES/machine-debug.yaml")
echo "$APPLY_OUT"
VM_ID=$(echo "$APPLY_OUT" | awk '/^machine/ {for (i=1; i<=NF; i++) if ($i ~ /^id=/) {sub("id=","",$i); print $i; exit}}')
VM_IP=$(echo "$APPLY_OUT" | awk '/^machine/ {for (i=1; i<=NF; i++) if ($i ~ /^ip=/) {sub("ip=","",$i); print $i; exit}}')
[[ -n "$VM_ID" ]] || fail "could not parse vm id from apply output"
[[ -n "$VM_IP" ]] || fail "could not parse vm ip from apply output"

step "Verify controller lists debug-0 as RUNNING"
LIST=$("$BIN" get-machines)
echo "$LIST"
echo "$LIST" | grep -q '^[a-f0-9-]\+  *debug-0 *RUNNING' \
    || fail "debug-0 not in RUNNING state (see listing above)"

# This is the important check. Cloud-hypervisor "started the VM" is
# not the same as "the VM is actually functional." We probe the VM's
# static IP until it responds. If it never does, boot hung inside the
# guest (grub, kernel panic, cloud-init, network config, etc.) and the
# controller's RUNNING state is misleading.
step "Wait for VM to answer on the network ($VM_IP, up to ${BOOT_DEADLINE_SECONDS}s)"
deadline=$((SECONDS + BOOT_DEADLINE_SECONDS))
until ping -c1 -W1 "$VM_IP" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
        echo
        echo "VM $VM_ID ($VM_IP) never answered ICMP in ${BOOT_DEADLINE_SECONDS}s."
        echo "Not tearing down — inspect on the hypervisor:"
        echo "  journalctl -u basis-vm-$VM_ID.service --no-pager"
        fail "VM unreachable after boot"
    fi
    sleep 2
done
echo "reachable after ${SECONDS}s"

if [[ "$KEEP" == 1 ]]; then
    step "VM is up — skipping teardown (--keep)"
    echo "  vm_id:  $VM_ID"
    echo "  ip:     $VM_IP"
    echo "  ssh:    ssh ubuntu@$VM_IP   # password: basis"
    echo "  serial: journalctl -u basis-vm-$VM_ID.service --no-pager"
    echo "  clean:  $BIN delete -f $FIXTURES/machine-debug.yaml"
    exit 0
fi

step "Delete machine"
"$BIN" delete -f "$FIXTURES/machine-debug.yaml"

step "Verify machine gone"
if "$BIN" get-machines | grep -q '  debug-0 '; then
    fail "debug-0 still listed after delete"
fi

step "Delete cluster"
"$BIN" delete -f "$FIXTURES/cluster.yaml"

step "ALL SMOKE TESTS PASSED"
