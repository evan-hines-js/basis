#!/usr/bin/env bash
# Smoke test: end-to-end cluster + machine lifecycle against a running
# Basis controller. Exits 0 iff every step works, fails loud on the
# first broken one.
#
# Section 1 is the real-VM happy path. It boots a VM, pings it, and
# tears it down. "RUNNING" in the controller isn't trusted — we require
# the guest OS to answer on the network.
#
# Sections 2+ exercise the controller-level invariants that Lattice's
# e2e suite doesn't cover because the e2e suite only drives the happy
# path. They're fast (no VM boots) and cover the idempotency, IP
# lifecycle, cascade-delete, and rejection paths that were touched by
# the recent resource-leak fix pass.
#
# Flags:
#   --keep           Don't tear down on success (Section 1 only).
#   --quick          Skip Section 1 (real VM boot) and run only the
#                    controller-level sections. Useful during iteration.
#   --parallel-safe  Skip sections that assert sequential-allocator
#                    ordering (3, 6, the IP-reuse half of 8). Required
#                    when multiple copies of this script share one
#                    controller — see scripts/load-testing.sh.
#   --suffix=<name>  Namespace every resource name with "-<name>" and
#                    generate per-suffix copies of cluster.yaml and
#                    machine-debug.yaml in the tmpdir. Lets many copies
#                    of this script run against the same controller
#                    without colliding on names.
#
# Env:
#   SMOKE_SKIP_BUILD=1  Skip `cargo build`. For callers that already
#                       built basis-ctl and don't want 100 workers
#                       racing on the target dir.
#
# Prereqs:
#   - A Basis controller reachable at $BASIS_ENDPOINT
#   - A Basis agent connected to that controller on some host
#   - The four env vars below set to valid cert paths
#   - Section 1: machine fixture's IP reachable from this host
#     (bridge layer-2 or routable)

set -euo pipefail

: "${BASIS_ENDPOINT:?BASIS_ENDPOINT not set (e.g. https://10.0.0.206:7443)}"
: "${BASIS_TLS_CA:?BASIS_TLS_CA not set (path to ca.crt)}"
: "${BASIS_TLS_CERT:?BASIS_TLS_CERT not set (path to capi-provider.crt)}"
: "${BASIS_TLS_KEY:?BASIS_TLS_KEY not set (path to capi-provider.key)}"

KEEP=0
QUICK=0
PARALLEL_SAFE=0
SUFFIX=""
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=1;;
        --quick) QUICK=1;;
        --parallel-safe) PARALLEL_SAFE=1;;
        --suffix=*) SUFFIX="${arg#--suffix=}";;
        *) echo "unknown arg: $arg" >&2; exit 2;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES="$REPO_ROOT/crates/basis-ctl/fixtures"
BIN="$REPO_ROOT/target/release/basis-ctl"

# How long we'll wait after CreateMachine for the guest to answer ICMP.
# cloud-init + getty + sshd is typically 20-30s on Ubuntu noble on this
# hardware; 90s is generous.
BOOT_DEADLINE_SECONDS=90

cd "$REPO_ROOT"

step() { echo; echo "==> $*"; }
pass() { echo "  ok: $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# `mktemp` varies across BSD/GNU; use a portable template.
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/basis-smoke.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

# Suffix convention: empty suffix means "use the shipped fixtures and
# original names" so existing callers are unaffected. When set, every
# named resource gets "-$SUFFIX" appended and we generate per-suffix
# copies of the two shipped fixtures into TMP_DIR.
S=""
[[ -n "$SUFFIX" ]] && S="-$SUFFIX"

CLUSTER_NAME="debug$S"
MACHINE_NAME="debug-0$S"
BOGUS_NAME="bogus$S"
OVERSIZE_NAME="oversize$S"
BADIMG_NAME="badimg$S"

if [[ -n "$SUFFIX" ]]; then
    CLUSTER_FIXTURE="$TMP_DIR/cluster.yaml"
    MACHINE_FIXTURE="$TMP_DIR/machine-debug.yaml"
    cat >"$CLUSTER_FIXTURE" <<YAML
apiVersion: basis.dev/v1
kind: Cluster
metadata:
  name: $CLUSTER_NAME
spec:
  ipPool: default
YAML
    cat >"$MACHINE_FIXTURE" <<YAML
apiVersion: basis.dev/v1
kind: Machine
metadata:
  name: $MACHINE_NAME
spec:
  cluster: $CLUSTER_NAME
  cpu: 2
  memoryMib: 2048
  diskGib: 10
  image: ghcr.io/evan-hines-js/lattice-node:v1.32.0
  bootstrapDataFile: $FIXTURES/bootstrap-debug.yaml
  gpus: 0
YAML
else
    CLUSTER_FIXTURE="$FIXTURES/cluster.yaml"
    MACHINE_FIXTURE="$FIXTURES/machine-debug.yaml"
fi

# Extract a field from the `basis-ctl apply` output line for a resource.
# Format: "machine <name>  id=<id>  ip=<ip>  provider=<...>"
parse_field() {
    local line="$1" key="$2"
    echo "$line" | awk -v k="$key" '{
        for (i=1; i<=NF; i++) if (index($i, k"=")==1) { sub(k"=","",$i); print $i; exit }
    }'
}

# Apply a fixture, print the output, return it on stdout. Lets the
# caller `parse_field` id / ip from the captured text without re-running
# the RPC.
apply_capture() {
    local file="$1"
    local out
    out=$("$BIN" apply -f "$file")
    echo "$out"
}

# Ensure the controller state is empty for our fixtures before we start.
# Delete is idempotent, so this is always safe even on a fresh controller.
reset_state() {
    "$BIN" delete -f "$MACHINE_FIXTURE" >/dev/null 2>&1 || true
    "$BIN" delete -f "$CLUSTER_FIXTURE" >/dev/null 2>&1 || true
}

if [[ "${SMOKE_SKIP_BUILD:-0}" != 1 ]]; then
    step "Build basis-ctl"
    cargo build --release --quiet -p basis-ctl
fi
[[ -x "$BIN" ]] || fail "basis-ctl did not build at $BIN"

reset_state

###############################################################################
# Section 1 — Real VM happy path.
# Boots a VM, verifies the guest OS comes up on the network, tears it down.
# Catches problems the controller can't see: guest boot hangs, cloud-init
# failures, host network misconfig.
###############################################################################
if [[ "$QUICK" == 0 ]]; then
    step "[1/8] Apply cluster"
    "$BIN" apply -f "$CLUSTER_FIXTURE"

    step "[1/8] Apply machine (blocks until agent reports CreateVm completed)"
    APPLY_OUT=$(apply_capture "$MACHINE_FIXTURE")
    echo "$APPLY_OUT"
    VM_LINE=$(echo "$APPLY_OUT" | grep '^machine' | head -1)
    VM_ID=$(parse_field "$VM_LINE" id)
    VM_IP=$(parse_field "$VM_LINE" ip)
    [[ -n "$VM_ID" ]] || fail "could not parse vm id from apply output"
    [[ -n "$VM_IP" ]] || fail "could not parse vm ip from apply output"

    step "[1/8] Verify controller lists $MACHINE_NAME as RUNNING"
    LIST=$("$BIN" get-machines)
    echo "$LIST"
    echo "$LIST" | grep -q "^[a-f0-9-]\+  *$MACHINE_NAME *RUNNING" \
        || fail "$MACHINE_NAME not in RUNNING state (see listing above)"

    # The critical check. Cloud-hypervisor "started the VM" is not the
    # same as "the guest is functional." Probe the static IP until it
    # answers. If it never does, boot hung inside the guest (grub,
    # kernel panic, cloud-init, network config) and RUNNING lies.
    step "[1/8] Wait for VM to answer on the network ($VM_IP, up to ${BOOT_DEADLINE_SECONDS}s)"
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
    pass "reachable after ${SECONDS}s"

    if [[ "$KEEP" == 1 ]]; then
        step "VM is up — skipping teardown (--keep)"
        echo "  vm_id:  $VM_ID"
        echo "  ip:     $VM_IP"
        echo "  ssh:    ssh ubuntu@$VM_IP   # password: basis"
        echo "  serial: journalctl -u basis-vm-$VM_ID.service --no-pager"
        echo "  clean:  $BIN delete -f $MACHINE_FIXTURE"
        exit 0
    fi

    step "[1/8] Delete machine + verify gone"
    "$BIN" delete -f "$MACHINE_FIXTURE"
    if "$BIN" get-machines | grep -q "  $MACHINE_NAME "; then
        fail "$MACHINE_NAME still listed after delete"
    fi
    pass "$MACHINE_NAME removed from controller"

    step "[1/8] Delete cluster"
    "$BIN" delete -f "$CLUSTER_FIXTURE"
    pass "cluster deleted"
fi

###############################################################################
# Section 2 — Idempotent re-apply.
# Proves: CreateCluster and CreateMachine return the existing record on
# a second apply instead of erroring with AlreadyExists. This is the
# property CAPI reconcilers rely on to recover from partial failures.
###############################################################################
step "[2/8] Idempotent re-apply (cluster + machine)"
reset_state
"$BIN" apply -f "$CLUSTER_FIXTURE"          >/dev/null
"$BIN" apply -f "$CLUSTER_FIXTURE"          >/dev/null || fail "re-apply cluster returned error"
pass "cluster re-apply returned success"

FIRST=$(apply_capture "$MACHINE_FIXTURE")
FIRST_LINE=$(echo "$FIRST" | grep '^machine' | head -1)
FIRST_ID=$(parse_field "$FIRST_LINE" id)
FIRST_IP=$(parse_field "$FIRST_LINE" ip)
[[ -n "$FIRST_ID" && -n "$FIRST_IP" ]] || fail "could not parse first-apply output: $FIRST_LINE"

SECOND=$(apply_capture "$MACHINE_FIXTURE")
SECOND_LINE=$(echo "$SECOND" | grep '^machine' | head -1)
SECOND_ID=$(parse_field "$SECOND_LINE" id)
SECOND_IP=$(parse_field "$SECOND_LINE" ip)

if [[ "$FIRST_ID" != "$SECOND_ID" ]]; then
    fail "re-apply produced a new VM id ($FIRST_ID != $SECOND_ID) — CreateMachine is not idempotent"
fi
if [[ "$FIRST_IP" != "$SECOND_IP" ]]; then
    fail "re-apply produced a new IP ($FIRST_IP != $SECOND_IP)"
fi
pass "machine re-apply returned the same id and IP"

###############################################################################
# Section 3 — IP release + reuse on delete.
# Proves: teardown_vm releases the allocation, and the next allocator
# picks up the freed address (confirming `release_ips` actually fires
# and the allocator walks from the lowest free address). Covers the
# class of bug where a failed cleanup leaves an IP permanently leased.
#
# Skipped under --parallel-safe: "lowest free address" is the position
# in a shared, global queue, so concurrent workers can't assert which
# IP they'll get back.
###############################################################################
if [[ "$PARALLEL_SAFE" == 0 ]]; then
    step "[3/8] IP release on delete"
    # `$MACHINE_NAME` is still alive from Section 2. Record its IP,
    # delete, re-apply, and expect the same IP back (it's the lowest
    # free after release).
    ORIG_IP="$FIRST_IP"
    "$BIN" delete -f "$MACHINE_FIXTURE" >/dev/null
    if "$BIN" get-machines | grep -q "  $MACHINE_NAME "; then
        fail "$MACHINE_NAME still listed after delete"
    fi

    REUSE=$(apply_capture "$MACHINE_FIXTURE")
    REUSE_LINE=$(echo "$REUSE" | grep '^machine' | head -1)
    REUSE_IP=$(parse_field "$REUSE_LINE" ip)
    [[ -n "$REUSE_IP" ]] || fail "could not parse reuse IP: $REUSE_LINE"

    if [[ "$REUSE_IP" != "$ORIG_IP" ]]; then
        fail "IP not reused after delete (original=$ORIG_IP, after-delete=$REUSE_IP) — release_ips may be leaking"
    fi
    pass "IP $ORIG_IP released on delete and reassigned to the next machine"
else
    step "[3/8] IP release on delete — SKIPPED (--parallel-safe)"
fi

###############################################################################
# Section 4 — Cluster cascade delete.
# Proves: DeleteCluster tears down every VM in the cluster and removes
# the cluster row, even when the agent's DeleteVm is best-effort. The
# recent fix that changed `let _ = release_ips(...)` to an warn-on-error
# is only useful if the cascade itself still works.
#
# We grep the full machine list by name rather than
# `get-machines --cluster <name>` — the latter expects a cluster *id*
# and silently returns an empty list for a name.
###############################################################################
step "[4/8] Cluster cascade delete"
# Section 2's re-apply already asserted the machine exists (same id+ip
# on both applies). A pre-check here duplicates that assertion, and
# under load the extra `get-machines` call becomes a source of spurious
# failures when the RPC is slow or partial. Go straight to the real
# invariant: delete the cluster, assert the machine is gone.
"$BIN" delete -f "$CLUSTER_FIXTURE"
if "$BIN" get-machines 2>/dev/null | grep -q "  $MACHINE_NAME "; then
    fail "$MACHINE_NAME still listed after cluster cascade delete"
fi
pass "cluster delete cascaded to $MACHINE_NAME"

###############################################################################
# Section 5 — Machine referencing an unknown cluster is rejected cleanly.
# Proves: the controller returns NotFound (not Internal or a hang) when
# a machine's `spec.cluster` points at a cluster that doesn't exist.
# Regression check for the `map_err(db_status)` path that used to
# blanket-map sqlx errors to 404.
###############################################################################
step "[5/8] Apply machine into nonexistent cluster is rejected"
BOGUS="$TMP_DIR/machine-bogus-cluster.yaml"
cat >"$BOGUS" <<YAML
apiVersion: basis.dev/v1
kind: Machine
metadata:
  name: $BOGUS_NAME
spec:
  cluster: does-not-exist$S
  cpu: 1
  memoryMib: 256
  diskGib: 10
  image: ghcr.io/evan-hines-js/lattice-node:v1.32.0
  bootstrapDataFile: $FIXTURES/bootstrap-debug.yaml
  gpus: 0
YAML
# basis-ctl resolves `spec.cluster` name → id via list_clusters(), so a
# bogus name surfaces client-side as "cluster 'does-not-exist' not
# found" rather than reaching CreateMachine. Either shape is acceptable;
# what we assert is (a) it fails, (b) nothing gets created.
if "$BIN" apply -f "$BOGUS" 2>/dev/null; then
    fail "apply of machine with bogus cluster ref unexpectedly succeeded"
fi
if "$BIN" get-machines 2>/dev/null | grep -q "  $BOGUS_NAME "; then
    fail "bogus machine was persisted even though apply failed"
fi
pass "bogus cluster ref rejected with no partial state"

###############################################################################
# Section 6 — Multi-cluster VIP allocation + release.
# Proves: CreateCluster allocates distinct VIPs from the pool's VIP
# sub-range, DeleteCluster releases them, and a subsequent CreateCluster
# picks up the freed VIP as the lowest-free address. Exercises
# `allocate_vip` / `release_ips(ClusterVip)` — the cluster analogue of
# Section 3, which only covered VM IPs.
#
# Skipped under --parallel-safe: the reuse half asserts a specific
# lowest-free address, which doesn't hold when other workers are
# churning VIPs in the same pool.
###############################################################################
if [[ "$PARALLEL_SAFE" == 0 ]]; then
    step "[6/8] Multi-cluster VIP allocation + release"
    write_cluster_fixture() {
        local name="$1" path="$2"
        cat >"$path" <<YAML
apiVersion: basis.dev/v1
kind: Cluster
metadata:
  name: $name
spec:
  ipPool: default
YAML
    }
    CLUSTER_A="$TMP_DIR/cluster-vip-a.yaml"
    CLUSTER_B="$TMP_DIR/cluster-vip-b.yaml"
    CLUSTER_C="$TMP_DIR/cluster-vip-c.yaml"
    write_cluster_fixture "smoke-vip-a$S" "$CLUSTER_A"
    write_cluster_fixture "smoke-vip-b$S" "$CLUSTER_B"
    write_cluster_fixture "smoke-vip-c$S" "$CLUSTER_C"

    parse_endpoint() { parse_field "$(echo "$1" | grep '^cluster')" endpoint; }

    OUT_A=$("$BIN" apply -f "$CLUSTER_A"); VIP_A=$(parse_endpoint "$OUT_A")
    OUT_B=$("$BIN" apply -f "$CLUSTER_B"); VIP_B=$(parse_endpoint "$OUT_B")
    [[ -n "$VIP_A" && -n "$VIP_B" ]] || fail "could not parse VIPs"
    if [[ "$VIP_A" == "$VIP_B" ]]; then
        fail "two clusters received the same VIP ($VIP_A) — allocator is broken"
    fi
    pass "two clusters got distinct VIPs ($VIP_A, $VIP_B)"

    "$BIN" delete -f "$CLUSTER_A" >/dev/null
    OUT_C=$("$BIN" apply -f "$CLUSTER_C"); VIP_C=$(parse_endpoint "$OUT_C")
    if [[ "$VIP_C" != "$VIP_A" ]]; then
        fail "VIP $VIP_A not reused after cluster delete (got $VIP_C instead) — release_ips(ClusterVip) may be leaking"
    fi
    pass "VIP $VIP_A released on cluster delete and reassigned"

    "$BIN" delete -f "$CLUSTER_B" >/dev/null
    "$BIN" delete -f "$CLUSTER_C" >/dev/null
else
    step "[6/8] Multi-cluster VIP allocation + release — SKIPPED (--parallel-safe)"
fi

###############################################################################
# Section 7 — Scheduler rejection on impossible resource request.
# Proves: requesting more CPU than any host has triggers
# SchedulerError::NoCapacity → Status::resource_exhausted, and no VM
# row, IP allocation, or partial state is left behind. IP allocation
# happens *after* pick_host, so if this test passes we know
# cleanup on schedule-failure is structurally unreachable rather than
# relying on a cleanup path firing.
###############################################################################
step "[7/8] Scheduler rejects impossible request with no partial state"
reset_state
"$BIN" apply -f "$CLUSTER_FIXTURE" >/dev/null

OVERSIZE="$TMP_DIR/machine-oversize.yaml"
cat >"$OVERSIZE" <<YAML
apiVersion: basis.dev/v1
kind: Machine
metadata:
  name: $OVERSIZE_NAME
spec:
  cluster: $CLUSTER_NAME
  cpu: 9999
  memoryMib: 256
  diskGib: 10
  image: ghcr.io/evan-hines-js/lattice-node:v1.32.0
  bootstrapDataFile: $FIXTURES/bootstrap-debug.yaml
  gpus: 0
YAML
if "$BIN" apply -f "$OVERSIZE" 2>/dev/null; then
    fail "oversize machine request unexpectedly succeeded"
fi
if "$BIN" get-machines 2>/dev/null | grep -q "  $OVERSIZE_NAME "; then
    fail "oversize machine row persisted after scheduler rejection"
fi
pass "oversize request rejected, no row persisted"

###############################################################################
# Section 8 — Bad image ref → clean failure + agent-side rollback.
# Proves: when the agent fails partway through CreateVm (here: image
# pull against a nonexistent repo), the controller surfaces a clear
# error, the VM row is removed, and the IP is released. This is the
# closest we can get to verifying the recent resource-leak fix
# (handlers::create_vm rolls back on any inner-step error) without
# direct host access.
#
# Under --parallel-safe we skip the "IP came back" half — with other
# workers churning IPs, the reclaimed address may be handed to them
# before we re-probe — and assert only that the failed row was removed.
###############################################################################
step "[8/8] Bad image ref → failure with full rollback"
if [[ "$PARALLEL_SAFE" == 0 ]]; then
    # Record the IP that would be the next allocation so we can assert
    # it was released (re-applying a valid machine should get the same
    # IP).
    VALID_PROBE=$(apply_capture "$MACHINE_FIXTURE")
    VALID_IP=$(parse_field "$(echo "$VALID_PROBE" | grep '^machine' | head -1)" ip)
    [[ -n "$VALID_IP" ]] || fail "could not probe next-free IP"
    "$BIN" delete -f "$MACHINE_FIXTURE" >/dev/null
fi

BADIMG="$TMP_DIR/machine-badimg.yaml"
cat >"$BADIMG" <<YAML
apiVersion: basis.dev/v1
kind: Machine
metadata:
  name: $BADIMG_NAME
spec:
  cluster: $CLUSTER_NAME
  cpu: 1
  memoryMib: 256
  diskGib: 10
  image: ghcr.io/evan-hines-js/does-not-exist-$(date +%s)$S:v9999
  bootstrapDataFile: $FIXTURES/bootstrap-debug.yaml
  gpus: 0
YAML
# `apply` blocks until the agent reports FAILED or times out. Image
# pull against a nonexistent GHCR repo 404s fast; expect a few seconds.
if "$BIN" apply -f "$BADIMG" 2>/dev/null; then
    fail "apply with bogus image unexpectedly succeeded"
fi
if "$BIN" get-machines 2>/dev/null | grep -q "  $BADIMG_NAME "; then
    fail "$BADIMG_NAME row persisted after agent-reported failure — cleanup_failed_vm regressed"
fi

if [[ "$PARALLEL_SAFE" == 0 ]]; then
    # Re-apply the valid machine. If the IP isn't reclaimed, the IP from
    # the failed attempt is still held and this one gets a different IP.
    AFTER=$(apply_capture "$MACHINE_FIXTURE")
    AFTER_IP=$(parse_field "$(echo "$AFTER" | grep '^machine' | head -1)" ip)
    if [[ "$AFTER_IP" != "$VALID_IP" ]]; then
        fail "IP not released after failed create ($VALID_IP expected, got $AFTER_IP) — IP leaked"
    fi
    pass "failed create left no row and no leased IP; $MACHINE_NAME re-allocated to $AFTER_IP"
    "$BIN" delete -f "$MACHINE_FIXTURE" >/dev/null
else
    pass "failed create left no row (IP-reuse assertion skipped under --parallel-safe)"
fi

"$BIN" delete -f "$CLUSTER_FIXTURE" >/dev/null

step "ALL SMOKE TESTS PASSED"
