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
#   --suffix=<name>  Namespace every resource name with "-<name>" and
#                    generate per-suffix copies of cluster.yaml and
#                    machine-debug.yaml in the tmpdir. Lets many copies
#                    of this script run against the same controller
#                    without colliding on names.
#
# Every assertion below holds under concurrent runs against a shared
# controller — invariants that depend on a globally-known free address
# (e.g. "this specific IP is the lowest free") belong in the controller's
# integration tests, not here.
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
#   - Section 1: edge IP reachable from this host (LAN-routable)
#   - Section 1's tree-only egress check runs only when this host also
#     has a route to tree CIDRs (i.e. when you're on the hypervisor).
#   - `sshpass` installed (Section 1 SSHes into the guest to verify it
#     can curl ghcr.io — the whole point of basis is hosting k8s
#     workers that pull node images, so "VM comes up" isn't enough)

set -euo pipefail

: "${BASIS_ENDPOINT:?BASIS_ENDPOINT not set (e.g. https://10.0.0.206:7443)}"
: "${BASIS_TLS_CA:?BASIS_TLS_CA not set (path to ca.crt)}"
: "${BASIS_TLS_CERT:?BASIS_TLS_CERT not set (path to capi-provider.crt)}"
: "${BASIS_TLS_KEY:?BASIS_TLS_KEY not set (path to capi-provider.key)}"
command -v sshpass >/dev/null \
    || { echo "smoke needs sshpass for guest egress checks: apt install sshpass" >&2; exit 2; }

KEEP=0
QUICK=0
SUFFIX=""
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=1;;
        --quick) QUICK=1;;
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

# Run one command inside the guest. Password auth via the `basis`
# password cloud-init sets in bootstrap-debug.yaml. StrictHostKeyChecking
# off because the host key is fresh per VM.
ssh_guest() {
    local ip="$1"; shift
    sshpass -p basis ssh -q \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=5 \
        "ubuntu@$ip" "$@"
}

# Poll `ssh` until the guest's sshd accepts commands, up to
# BOOT_DEADLINE_SECONDS. Returns 0 on success, 1 on timeout.
wait_for_ssh() {
    local ip="$1"
    local deadline=$((SECONDS + BOOT_DEADLINE_SECONDS))
    until ssh_guest "$ip" 'true' 2>/dev/null; do
        (( SECONDS < deadline )) || return 1
        sleep 2
    done
}

# Inside the guest, fetch ghcr.io/v2/ and return the HTTP status.
# Prints "000" on network failure. A 2xx/3xx/4xx/5xx return means the
# guest can actually reach the internet — ghcr.io/v2/ answers 401
# without auth, which is still proof-of-connectivity.
guest_curl_ghcr() {
    local ip="$1"
    ssh_guest "$ip" 'curl -sSL -m 15 -o /dev/null -w "%{http_code}" https://ghcr.io/v2/ 2>/dev/null || echo 000' \
        2>/dev/null | tr -d '[:space:]'
}

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
spec: {}
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
  edge: true
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
    # Prefer the edge IP for the reachability probe — it's on the
    # physical LAN so it's routable from the operator's laptop, not
    # just the hypervisor. Tree IPs are overlay-only.
    VM_IP=$(parse_field "$VM_LINE" edge_ip)
    [[ -n "$VM_IP" ]] || VM_IP=$(parse_field "$VM_LINE" ip)
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

    # "Pingable" is necessary but not sufficient — the real bar is
    # "can pull its k8s node image," which means DNS, a default
    # route, and reachability to ghcr.io from inside the guest.
    # Testing that end-to-end path is the whole point of basis:
    # anything less and Lattice's workers won't join their cluster.
    step "[1/8] Wait for guest sshd + verify external egress from edge NIC"
    wait_for_ssh "$VM_IP" || fail "guest sshd on $VM_IP not reachable within ${BOOT_DEADLINE_SECONDS}s"
    status=$(guest_curl_ghcr "$VM_IP")
    [[ "$status" =~ ^[1-5][0-9]{2}$ ]] \
        || fail "guest at $VM_IP cannot reach https://ghcr.io/v2/ (status=$status) — edge NIC egress broken"
    pass "edge VM reached ghcr.io (HTTP $status)"

    # Tree-only VMs — the case k8s workers will actually use — only
    # have an IP on the overlay. Reaching the internet from there
    # requires the hypervisor to NAT the tree CIDR out the uplink.
    # This sub-section runs only when we're sitting ON the hypervisor
    # (tree IPs are in the local routing table); from a laptop we
    # skip it, since tree IPs aren't routable from outside the fabric.
    if ip route get 10.100.0.1 2>/dev/null | grep -q 'dev brt'; then
        step "[1/8] Tree-only egress (hypervisor only)"
        TREE_MACHINE_FIXTURE="$TMP_DIR/machine-tree.yaml"
        TREE_MACHINE_NAME="tree$S"
        cat >"$TREE_MACHINE_FIXTURE" <<YAML
apiVersion: basis.dev/v1
kind: Machine
metadata:
  name: $TREE_MACHINE_NAME
spec:
  cluster: $CLUSTER_NAME
  cpu: 2
  memoryMib: 2048
  diskGib: 10
  image: ghcr.io/evan-hines-js/lattice-node:v1.32.0
  bootstrapDataFile: $FIXTURES/bootstrap-debug.yaml
  gpus: 0
YAML
        TREE_OUT=$(apply_capture "$TREE_MACHINE_FIXTURE")
        echo "$TREE_OUT"
        TREE_LINE=$(echo "$TREE_OUT" | grep '^machine' | head -1)
        TREE_IP=$(parse_field "$TREE_LINE" ip)
        [[ -n "$TREE_IP" ]] || fail "could not parse tree VM ip"

        deadline=$((SECONDS + BOOT_DEADLINE_SECONDS))
        until ping -c1 -W1 "$TREE_IP" >/dev/null 2>&1; do
            (( SECONDS < deadline )) || fail "tree VM never pinged at $TREE_IP"
            sleep 2
        done

        wait_for_ssh "$TREE_IP" || fail "tree VM sshd never ready"
        status=$(guest_curl_ghcr "$TREE_IP")
        [[ "$status" =~ ^[1-5][0-9]{2}$ ]] \
            || fail "tree-only VM cannot reach https://ghcr.io/v2/ (status=$status) — hypervisor NAT for tree CIDR broken"
        pass "tree-only VM reached ghcr.io (HTTP $status) via hypervisor NAT"

        # Per-host bridge IP check. Each hypervisor owns a unique
        # address in the tree's bridge_range and every VM it hosts
        # uses that same address as default gateway. The genuine
        # regression — a cross-host reply hijacked by the responding
        # host — can only be caught with two hypervisors, but we can
        # still assert the single-host invariants the fix guarantees:
        #   (1) the guest's default gateway matches the host's bridge IP
        #   (2) that IP comes from the tree's bridge_range, so the
        #       shared-gateway layout is definitively gone.
        HOST_BRIDGE_IP=$(ip -o route get "$TREE_IP" \
            | awk '{for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}')
        [[ -n "$HOST_BRIDGE_IP" ]] \
            || fail "could not read host bridge IP via 'ip route get $TREE_IP'"
        VM_GATEWAY=$(ssh_guest "$TREE_IP" 'ip -4 route show default | awk "{print \$3}"' | tr -d '[:space:]')
        [[ -n "$VM_GATEWAY" ]] \
            || fail "could not read default gateway from guest"
        if [[ "$HOST_BRIDGE_IP" != "$VM_GATEWAY" ]]; then
            fail "guest default gateway $VM_GATEWAY != host bridge IP $HOST_BRIDGE_IP — per-host gateway wiring broken"
        fi
        # bridge_range lives at the bottom of the tree CIDR: with the
        # default /20 tree and bridge_reserve=32, the first 32 usable
        # addresses are bridge IPs and VM IPs start above them. The
        # VM's own address must sit numerically above the gateway.
        gw_last=${HOST_BRIDGE_IP##*.}
        vm_last=${TREE_IP##*.}
        if (( vm_last <= gw_last )); then
            fail "VM IP $TREE_IP is not above bridge IP $HOST_BRIDGE_IP — bridge_range carve regressed"
        fi
        pass "host bridge IP $HOST_BRIDGE_IP matches VM default gateway (per-host gateway wiring OK)"

        "$BIN" delete -f "$TREE_MACHINE_FIXTURE" >/dev/null
    else
        echo "  (skipping tree-only egress: no local route to tree CIDR — not on the hypervisor)"
    fi

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
# Section 3 — Delete clears the row.
# Proves: teardown_vm removes the VM from the controller's view. The
# stronger property — that the IP comes back as the lowest-free
# address — is asserted in basis-controller's integration tests where
# we own the allocator's state.
###############################################################################
step "[3/8] Delete clears the row"
"$BIN" delete -f "$MACHINE_FIXTURE" >/dev/null
if "$BIN" get-machines | grep -q "  $MACHINE_NAME "; then
    fail "$MACHINE_NAME still listed after delete"
fi
# Re-apply must succeed; if `release_vm_ips` were broken the new
# allocation would either fail or hand out a duplicate IP, both of
# which trip downstream sections.
REUSE=$(apply_capture "$MACHINE_FIXTURE")
echo "$REUSE" | grep -q '^machine' || fail "machine re-apply after delete did not produce a row"
pass "machine deleted and re-applied cleanly"

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
# Section 6 — Multi-cluster VIP uniqueness.
# Proves: CreateCluster never hands the same VIP to two clusters
# (would mean the tree vip_range allocator is double-issuing). The
# stronger "freed VIP is reclaimed as lowest-free" property is
# asserted in basis-controller's integration tests.
###############################################################################
step "[6/8] Multi-cluster VIP uniqueness"
write_cluster_fixture() {
    local name="$1" path="$2"
    cat >"$path" <<YAML
apiVersion: basis.dev/v1
kind: Cluster
metadata:
  name: $name
spec: {}
YAML
}
CLUSTER_A="$TMP_DIR/cluster-vip-a.yaml"
CLUSTER_B="$TMP_DIR/cluster-vip-b.yaml"
write_cluster_fixture "smoke-vip-a$S" "$CLUSTER_A"
write_cluster_fixture "smoke-vip-b$S" "$CLUSTER_B"

parse_endpoint() { parse_field "$(echo "$1" | grep '^cluster')" endpoint; }

OUT_A=$("$BIN" apply -f "$CLUSTER_A"); VIP_A=$(parse_endpoint "$OUT_A")
OUT_B=$("$BIN" apply -f "$CLUSTER_B"); VIP_B=$(parse_endpoint "$OUT_B")
[[ -n "$VIP_A" && -n "$VIP_B" ]] || fail "could not parse VIPs"
if [[ "$VIP_A" == "$VIP_B" ]]; then
    fail "two clusters received the same VIP ($VIP_A) — allocator is double-issuing"
fi
pass "two clusters got distinct VIPs ($VIP_A, $VIP_B)"

"$BIN" delete -f "$CLUSTER_A" >/dev/null
"$BIN" delete -f "$CLUSTER_B" >/dev/null

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
# error and the VM row is removed. The IP-reclamation half is
# asserted in basis-controller's integration tests where we own the
# allocator's snapshot.
###############################################################################
step "[8/8] Bad image ref → failure with full rollback"

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

pass "failed create left no row"

"$BIN" delete -f "$CLUSTER_FIXTURE" >/dev/null

step "ALL SMOKE TESTS PASSED"
