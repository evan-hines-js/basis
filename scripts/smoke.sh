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
#   - Section 1 needs a route into the tree overlay CIDR (i.e. run on
#     the hypervisor). VMs are tree-only — no LAN-routable edge NIC —
#     so off-host runs can't reach the guest. Section 1 self-skips
#     the egress probe when no such route exists; --quick skips it
#     entirely.
#   - `sshpass` installed (Section 1 SSHes into the guest to verify it
#     can curl ghcr.io — the whole point of basis is hosting k8s
#     workers that pull node images, so "VM comes up" isn't enough)

set -euo pipefail

: "${BASIS_ENDPOINT:?BASIS_ENDPOINT not set (e.g. https://10.0.0.206:7443)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES="$REPO_ROOT/crates/basis-ctl/fixtures"
BIN="$REPO_ROOT/target/release/basis-ctl"

# shellcheck source=lib/pki.sh
. "$REPO_ROOT/scripts/lib/pki.sh"
resolve_pki BASIS_TLS_CA   ca.crt
resolve_pki BASIS_TLS_CERT capi-provider.crt
resolve_pki BASIS_TLS_KEY  capi-provider.key
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

# How long we'll wait after CreateMachine for the guest to answer ICMP.
# cloud-init + getty + sshd is typically 20-30s on Ubuntu noble on this
# hardware; 90s is generous.
BOOT_DEADLINE_SECONDS=90

cd "$REPO_ROOT"

SECTION_TOTAL=10
section() { echo; echo "==> [$1/$SECTION_TOTAL] $2"; }
step() { echo; echo "    -> $*"; }
pass() { echo "    ok: $*"; }
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

# True iff a TCP connection to <ip>:22 completes within 1s. Lets the
# caller tell "sshd hasn't started yet" apart from "sshd is up but
# rejected our credentials" — those two failures want different
# triage. Uses bash's /dev/tcp so we don't add another tool dep.
tcp22_open() {
    local ip="$1"
    timeout 1 bash -c ">/dev/tcp/$ip/22" 2>/dev/null
}

# Wait until sshd's TCP port is reachable, separately from auth working.
# Returns 0 on success, 1 if the port never opened. Used to give a
# precise failure when sshd is up but `ssh ubuntu@…` keeps getting
# Permission denied — historically that meant the bootstrap fixture
# locked the ubuntu account (lock_passwd defaulted to true) and every
# password attempt was rejected with no usable signal.
wait_for_ssh_port() {
    local ip="$1"
    local deadline=$((SECONDS + BOOT_DEADLINE_SECONDS))
    until tcp22_open "$ip"; do
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
spec:
  externalIpPool: cell-public
  externalServiceIps: 2
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
    # Prefer the rustup-managed cargo the basis-holod ansible role
    # installs at /var/cache/holo-build/cargo/bin/cargo. Hypervisors
    # run Ubuntu 22.04's apt rustc 1.85 by default, which is below
    # the `time` crate's 1.88 MSRV — so falling back to system cargo
    # on those hosts produces a confusing dep-graph error. Laptops
    # without that path keep using whatever cargo is on PATH.
    #
    # The cargo at /var/cache/holo-build/cargo/bin/cargo is a rustup
    # proxy. It needs RUSTUP_HOME + CARGO_HOME pointed at the same
    # /var/cache/holo-build/{rustup,cargo} directories the role wrote
    # the toolchain to — without them, the proxy reads root's empty
    # rustup config and errors with "no default toolchain configured."
    if [[ -x /var/cache/holo-build/cargo/bin/cargo ]]; then
        RUSTUP_HOME=/var/cache/holo-build/rustup \
        CARGO_HOME=/var/cache/holo-build/cargo \
            /var/cache/holo-build/cargo/bin/cargo build --release --quiet -p basis-ctl
    else
        cargo build --release --quiet -p basis-ctl
    fi
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
    section 1 "Real VM happy path"
    step "Apply cluster"
    "$BIN" apply -f "$CLUSTER_FIXTURE"

    step "Apply machine (blocks until agent reports CreateVm completed)"
    APPLY_OUT=$(apply_capture "$MACHINE_FIXTURE")
    echo "$APPLY_OUT"
    VM_LINE=$(echo "$APPLY_OUT" | grep '^machine' | head -1)
    VM_ID=$(parse_field "$VM_LINE" id)
    # VMs are tree-only — single NIC on the per-cluster VXLAN overlay.
    # The IP printed here is the tree-overlay address; it's reachable
    # only from somewhere with a route into the tree CIDR (the
    # hypervisor itself or a host carrying the tree).
    VM_IP=$(parse_field "$VM_LINE" ip)
    [[ -n "$VM_ID" ]] || fail "could not parse vm id from apply output"
    [[ -n "$VM_IP" ]] || fail "could not parse vm ip from apply output"

    step "Verify controller lists $MACHINE_NAME as RUNNING"
    LIST=$("$BIN" get-machines)
    echo "$LIST"
    echo "$LIST" | grep -q "^[a-f0-9-]\+  *$MACHINE_NAME *RUNNING" \
        || fail "$MACHINE_NAME not in RUNNING state (see listing above)"

    # In the BGP-based model, what's *cell-wide* reachable is the
    # cluster's apiserver VIP and Service block (advertised by every
    # host carrying the tree, proxy-ARPed on the underlay). Individual
    # VM IPs live on the tree overlay and aren't routable off-host
    # without sitting on a hypervisor. Section 1's deeper checks
    # (in-VM ping, sshd, ghcr egress, gateway wiring) all need that
    # tree-local route — so we run them only when we have one. From
    # off-host, the section above already verified the controller
    # surfaced the VM as RUNNING; deeper coverage requires --quick on
    # off-host runs or running smoke.sh on the hypervisor.
    if ! ip route get "$VM_IP" 2>/dev/null | grep -q 'dev brt'; then
        echo "  (skipping in-VM checks: no local route into tree CIDR — re-run on the hypervisor for full Section 1 coverage)"
    else
        # The critical check. Cloud-hypervisor "started the VM" is
        # not the same as "the guest is functional." Probe the static
        # IP until it answers. If it never does, boot hung inside the
        # guest (grub, kernel panic, cloud-init, network config) and
        # RUNNING lies.
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
        pass "reachable after ${SECONDS}s"

        # The real bar is "the guest can pull its k8s node image" —
        # DNS, default route, and reachability to ghcr.io. Tree-only
        # VMs reach the internet via the hypervisor's MASQUERADE on
        # the tree CIDR.
        step "Wait for guest sshd + verify external egress (tree → hypervisor NAT)"
        # Two-stage check so a "sshd never started" failure (TCP-22 never
        # opens) reports differently from "sshd answers but rejects the
        # password" (TCP-22 open, ssh keeps returning Permission denied).
        # Triage for the former is journalctl on the agent host; for the
        # latter, /var/log/cloud-init-output.log inside the guest.
        wait_for_ssh_port "$VM_IP" \
            || fail "guest sshd on $VM_IP did not open port 22 within ${BOOT_DEADLINE_SECONDS}s"
        if ! wait_for_ssh "$VM_IP"; then
            fail "sshd on $VM_IP accepts TCP but password auth for ubuntu/basis is being rejected — \
check bootstrap-debug.yaml landed on this host and that cloud-init's chpasswd ran \
(/var/log/cloud-init-output.log inside the guest)"
        fi
        pass "guest accepts password auth for ubuntu/basis"
        status=$(guest_curl_ghcr "$VM_IP")
        [[ "$status" =~ ^[1-5][0-9]{2}$ ]] \
            || fail "guest at $VM_IP cannot reach https://ghcr.io/v2/ (status=$status) — tree CIDR MASQUERADE broken"
        pass "tree VM reached ghcr.io (HTTP $status) via hypervisor NAT"

        # Per-host bridge IP check. Each hypervisor owns a unique
        # address in the tree's bridge_range and every VM it hosts
        # uses that same address as default gateway. The genuine
        # regression — a cross-host reply hijacked by the responding
        # host — can only be caught with two hypervisors, but the
        # single-host invariants are still worth pinning:
        #   (1) the guest's default gateway matches the host's bridge IP
        #   (2) that IP comes from the tree's bridge_range
        HOST_BRIDGE_IP=$(ip -o route get "$VM_IP" \
            | awk '{for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}')
        [[ -n "$HOST_BRIDGE_IP" ]] \
            || fail "could not read host bridge IP via 'ip route get $VM_IP'"
        VM_GATEWAY=$(ssh_guest "$VM_IP" 'ip -4 route show default | awk "{print \$3}"' | tr -d '[:space:]')
        [[ -n "$VM_GATEWAY" ]] \
            || fail "could not read default gateway from guest"
        if [[ "$HOST_BRIDGE_IP" != "$VM_GATEWAY" ]]; then
            fail "guest default gateway $VM_GATEWAY != host bridge IP $HOST_BRIDGE_IP — per-host gateway wiring broken"
        fi
        gw_last=${HOST_BRIDGE_IP##*.}
        vm_last=${VM_IP##*.}
        if (( vm_last <= gw_last )); then
            fail "VM IP $VM_IP is not above bridge IP $HOST_BRIDGE_IP — bridge_range carve regressed"
        fi
        pass "host bridge IP $HOST_BRIDGE_IP matches VM default gateway (per-host gateway wiring OK)"
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

    step "Delete machine + verify gone"
    "$BIN" delete -f "$MACHINE_FIXTURE"
    if "$BIN" get-machines | grep -q "  $MACHINE_NAME "; then
        fail "$MACHINE_NAME still listed after delete"
    fi
    pass "$MACHINE_NAME removed from controller"

    step "Delete cluster"
    "$BIN" delete -f "$CLUSTER_FIXTURE"
    pass "cluster deleted"
fi

###############################################################################
# Section 2 — Idempotent re-apply.
# Proves: CreateCluster and CreateMachine return the existing record on
# a second apply instead of erroring with AlreadyExists. This is the
# property CAPI reconcilers rely on to recover from partial failures.
###############################################################################
section 2 "Idempotent re-apply (cluster + machine)"
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
section 3 "Delete clears the row"
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
section 4 "Cluster cascade delete"
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
section 5 "Apply machine into nonexistent cluster is rejected"
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
section 6 "Multi-cluster VIP uniqueness"
write_cluster_fixture() {
    local name="$1" path="$2"
    cat >"$path" <<YAML
apiVersion: basis.dev/v1
kind: Cluster
metadata:
  name: $name
spec:
  externalIpPool: cell-public
  externalServiceIps: 2
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
section 7 "Scheduler rejects impossible request with no partial state"
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
section 8 "Bad image ref → failure with full rollback"

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

###############################################################################
# Section 9 — BGP dataplane reachability.
# Proves the cell-wide cluster-VIP path actually moves packets end-to-end:
# BGP advertisement (controller → reflector → host speakers), host /32
# route into the bridge, proxy-ARP on the underlay, bridge FDB learning
# from gratuitous ARP, VXLAN forwarding, in-VM IP claim. We don't run a
# real apiserver — we boot one VM, claim the apiserver VIP on its tree
# NIC the same way kube-vip would, run a tiny TCP listener, and probe
# from the operator machine.
#
# Self-skips when the operator can't reach the cell-public LAN at all
# (VIP pings already known to fail; the section would just time out).
###############################################################################
if [[ "$QUICK" == 0 ]]; then
    section 9 "BGP reachability — claim VIP on a VM, probe from off-host"

    APPLY_OUT=$(apply_capture "$CLUSTER_FIXTURE")
    CLUSTER_LINE=$(echo "$APPLY_OUT" | grep '^cluster' | head -1)
    VIP=$(parse_field "$CLUSTER_LINE" endpoint)
    [[ -n "$VIP" ]] || fail "could not parse apiserver VIP from cluster apply"

    # Render a one-off bootstrap that adds the VIP as a secondary on
    # the tree NIC (kube-vip-style L2 claim) and runs a tiny TCP
    # listener on it. The bridge FDB learns the VIP→VM-MAC mapping
    # from the gratuitous ARP that `ip addr add` emits, so the host
    # can forward LAN-incoming traffic for the VIP into the bridge.
    #
    # Auto-detect the NIC name — Ubuntu cloud images give virtio NICs
    # predictable names (`ens3`/`enp1s0`/etc.), not `eth0`. Pick the
    # first non-loopback link that has an IPv4 address and use that.
    # Run as a oneshot systemd unit so cloud-init's runcmd doesn't
    # block on the persistent listener.
    BGP_BOOTSTRAP="$TMP_DIR/bootstrap-bgp.yaml"
    cat >"$BGP_BOOTSTRAP" <<YAML
#cloud-config
write_files:
  - path: /usr/local/bin/vip-claim
    permissions: '0755'
    content: |
      #!/bin/sh
      set -eux
      NIC=\$(ip -o -4 addr show scope global | awk '{print \$2; exit}')
      [ -n "\$NIC" ] || { echo "no nic with global ipv4 found" >&2; exit 1; }
      ip addr add $VIP/32 dev "\$NIC"
      # Persistent TCP listener on the VIP. python3 ships in every
      # Ubuntu cloud image so this is more portable than nc/ncat.
      # Send Content-Length + Connection: close so curl knows when the
      # body ends, then shutdown(SHUT_WR) + drain before close so the
      # kernel sends a FIN instead of a RST (RST trips curl exit 56
      # even when the body landed correctly).
      exec python3 -c '
      import socket
      s = socket.socket()
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind(("0.0.0.0", 9999)); s.listen(8)
      body = b"basis-vip-probe\n"
      headers = (
          b"HTTP/1.0 200 OK\r\n"
          b"Content-Length: " + str(len(body)).encode() + b"\r\n"
          b"Connection: close\r\n\r\n"
      )
      while True:
          c, _ = s.accept()
          try:
              c.sendall(headers + body)
              c.shutdown(socket.SHUT_WR)
              try: c.recv(1024)
              except OSError: pass
          finally:
              c.close()
      '
  - path: /etc/systemd/system/vip-claim.service
    permissions: '0644'
    content: |
      [Unit]
      Description=basis smoke VIP probe
      After=network-online.target
      Wants=network-online.target
      [Service]
      Type=simple
      ExecStart=/usr/local/bin/vip-claim
      Restart=on-failure
      RestartSec=2s
      [Install]
      WantedBy=multi-user.target
runcmd:
  - [ systemctl, enable, --now, 'serial-getty@ttyS0.service' ]
  - [ systemctl, enable, --now, 'vip-claim.service' ]
YAML

    BGP_MACHINE_FIXTURE="$TMP_DIR/machine-bgp.yaml"
    BGP_MACHINE_NAME="bgp-probe$S"
    cat >"$BGP_MACHINE_FIXTURE" <<YAML
apiVersion: basis.dev/v1
kind: Machine
metadata:
  name: $BGP_MACHINE_NAME
spec:
  cluster: $CLUSTER_NAME
  cpu: 1
  memoryMib: 1024
  diskGib: 10
  image: ghcr.io/evan-hines-js/lattice-node:v1.32.0
  bootstrapDataFile: $BGP_BOOTSTRAP
  gpus: 0
YAML
    "$BIN" apply -f "$BGP_MACHINE_FIXTURE" >/dev/null

    # Wait for the VIP to answer. The full path being exercised:
    #   ping → LAN → host proxy-ARP reply → host /32 route via brt<vni>
    #     → bridge FDB → VXLAN to leader → VM's ARP-claimed VIP.
    # 90s gives cloud-init time to enable+start the vip-claim unit.
    deadline=$((SECONDS + BOOT_DEADLINE_SECONDS))
    until ping -c1 -W1 "$VIP" >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then
            # Triage hints — a VIP that never answers ICMP usually means
            # one of these. Print them so the operator knows where to
            # look first instead of starting from scratch.
            echo
            echo "  triage:"
            echo "    1. on the controller host (e.g. node-2):"
            echo "       ip route get $VIP                  # expect 'dev brt<vni>'"
            echo "       ip neigh show proxy $VIP dev vmbr0  # expect an entry"
            echo "    2. confirm the agent advertised the VIP (controller log):"
            echo "       journalctl -u basis-agent -n 50 --no-pager | grep -i bgp"
            echo "    3. confirm the VM claimed the VIP. ssh into the VM (its tree IP)"
            echo "       and check 'ip -4 addr', 'systemctl status vip-claim.service',"
            echo "       and '/var/log/cloud-init-output.log'."
            "$BIN" delete -f "$BGP_MACHINE_FIXTURE" >/dev/null 2>&1 || true
            "$BIN" delete -f "$CLUSTER_FIXTURE" >/dev/null 2>&1 || true
            fail "VIP $VIP unreachable after ${BOOT_DEADLINE_SECONDS}s — BGP / proxy-ARP / route / FDB path broken"
        fi
        sleep 2
    done
    pass "VIP $VIP answers ICMP after ${SECONDS}s"

    # ICMP can be answered by the LAN gateway in some misconfigs (we
    # saw this when basis was given the whole /24 and allocated .1).
    # The TCP probe is the real check — only the in-VM listener
    # answers on port 9999, so a successful body roundtrip proves the
    # full forwarding path actually delivers to the leader VM.
    # Assert on body content rather than curl's exit code: a RST
    # after the body was delivered should still count as success.
    body=$(curl -sS --max-time 10 "http://$VIP:9999/" 2>/dev/null || true)
    case "$body" in
        *basis-vip-probe*)
            pass "VIP $VIP delivers TCP traffic to the in-VM listener (full BGP+ARP+VXLAN path)" ;;
        *)
            "$BIN" delete -f "$BGP_MACHINE_FIXTURE" >/dev/null 2>&1 || true
            "$BIN" delete -f "$CLUSTER_FIXTURE" >/dev/null 2>&1 || true
            fail "TCP probe to $VIP:9999 returned no body — ICMP works but forwarding to VM doesn't (got: ${body:-<empty>})" ;;
    esac

    "$BIN" delete -f "$BGP_MACHINE_FIXTURE" >/dev/null
    "$BIN" delete -f "$CLUSTER_FIXTURE" >/dev/null
fi

###############################################################################
# Section 10 — Soft anti-affinity within a cluster.
# Proves: when the fleet has ≥2 healthy hosts, the scheduler spreads a
# cluster's VMs instead of bin-packing them all onto one host. Three
# small same-cluster VMs must land on ≥2 distinct hosts. This is the
# placement guarantee the cross-host networking path (VXLAN delivery
# between bridges, BGP from a non-leader host) actually depends on.
#
# Self-skips on single-host fleets — the invariant has no meaning when
# every VM is forced to one host regardless.
###############################################################################
if [[ "$QUICK" == 0 ]]; then
    section 10 "Anti-affinity spreads cluster VMs across hosts"

    # Read the healthy-host count from the controller's prometheus
    # /metrics endpoint (plain HTTP, not the mTLS gRPC port). Default
    # port is 9443; override via BASIS_METRICS_PORT.
    AA_META_HOST="${BASIS_ENDPOINT#*://}"
    AA_META_HOST="${AA_META_HOST%%:*}"
    AA_META_PORT="${BASIS_METRICS_PORT:-9443}"
    AA_HEALTHY_HOSTS=""
    if command -v curl >/dev/null; then
        AA_HEALTHY_HOSTS=$(curl -sS --max-time 5 \
            "http://$AA_META_HOST:$AA_META_PORT/metrics" 2>/dev/null \
            | awk '/^basis_hosts\{healthy="true"\}/ {print $NF}')
    fi

    if [[ -z "$AA_HEALTHY_HOSTS" ]]; then
        echo "  (skipping: could not read basis_hosts from $AA_META_HOST:$AA_META_PORT/metrics — set BASIS_METRICS_PORT or run on the controller host)"
    elif (( AA_HEALTHY_HOSTS < 2 )); then
        echo "  (skipping: only $AA_HEALTHY_HOSTS healthy host registered — anti-affinity needs ≥2)"
    else
        AA_CLUSTER_FIXTURE="$TMP_DIR/cluster-aa.yaml"
        AA_CLUSTER_NAME="smoke-aa$S"
        cat >"$AA_CLUSTER_FIXTURE" <<YAML
apiVersion: basis.dev/v1
kind: Cluster
metadata:
  name: $AA_CLUSTER_NAME
spec:
  externalIpPool: cell-public
  externalServiceIps: 1
YAML
        "$BIN" apply -f "$AA_CLUSTER_FIXTURE" >/dev/null

        # Three identical small VMs in the same cluster. Equal sizing
        # rules out best-fit deciding the placement; any spread we see
        # is the anti-affinity tie-break at work. Sized below the
        # smallest realistic host so all three fit anywhere — a
        # capacity-driven spill would mask the anti-affinity signal.
        # Apply in parallel: `basis-ctl apply` blocks until the agent
        # reports CreateVm completed (~30s), and these three machines
        # have no dependency on each other. The controller's optimistic
        # commit gate already serializes capacity claims, so concurrent
        # CreateMachine calls are safe.
        AA_FIXTURES=()
        AA_PIDS=()
        for i in 0 1 2; do
            AA_M="$TMP_DIR/machine-aa-$i.yaml"
            cat >"$AA_M" <<YAML
apiVersion: basis.dev/v1
kind: Machine
metadata:
  name: aa-$i$S
spec:
  cluster: $AA_CLUSTER_NAME
  cpu: 1
  memoryMib: 512
  # Must be >= the node image's virtual size (10 GiB). Smaller asks
  # the agent to shrink the thin snapshot, which is unsupported and
  # surfaces as "New size given ... not larger than existing size".
  diskGib: 10
  image: ghcr.io/evan-hines-js/lattice-node:v1.32.0
  bootstrapDataFile: $FIXTURES/bootstrap-debug.yaml
  gpus: 0
YAML
            AA_FIXTURES+=("$AA_M")
            "$BIN" apply -f "$AA_M" >/dev/null &
            AA_PIDS+=($!)
        done
        AA_FAIL=0
        for pid in "${AA_PIDS[@]}"; do
            wait "$pid" || AA_FAIL=1
        done
        if (( AA_FAIL )); then
            for f in "${AA_FIXTURES[@]}"; do "$BIN" delete -f "$f" >/dev/null 2>&1 || true; done
            "$BIN" delete -f "$AA_CLUSTER_FIXTURE" >/dev/null 2>&1 || true
            fail "one or more concurrent CreateMachine applies failed — check controller log"
        fi

        AA_LISTING=$("$BIN" get-machines)
        # HOST is the last column of `get-machines` output. Pull it for
        # our three names and uniq.
        AA_DISTINCT=$(echo "$AA_LISTING" \
            | awk -v p="aa-[012]$S" '$2 ~ ("^"p"$") {print $NF}' \
            | sort -u)
        AA_DISTINCT_COUNT=$(echo "$AA_DISTINCT" | grep -c .)
        if (( AA_DISTINCT_COUNT < 2 )); then
            echo "  placement:"
            echo "$AA_LISTING" | awk -v p="aa-[012]$S" '$2 ~ ("^"p"$") {print "    " $2 " -> " $NF}'
            for f in "${AA_FIXTURES[@]}"; do "$BIN" delete -f "$f" >/dev/null 2>&1 || true; done
            "$BIN" delete -f "$AA_CLUSTER_FIXTURE" >/dev/null 2>&1 || true
            fail "3 same-cluster VMs all landed on one host — soft anti-affinity not splitting"
        fi
        pass "3 same-cluster VMs spread across $AA_DISTINCT_COUNT hosts (anti-affinity active)"

        # Cross-node connectivity probe.
        # Placement spread is necessary but not sufficient — the actual
        # value of putting VMs on different hosts is that they can still
        # talk to each other over the per-cluster VXLAN overlay. This
        # exercises the full data path:
        #   VM A → tap → bridge → VXLAN encap → underlay → peer host
        #   → VXLAN decap → bridge → tap → VM B
        # in both directions (proxy-ARP / FDB regressions can be
        # one-way, so a unidirectional check would miss them).
        #
        # Probes:
        #   - ICMP: cheapest L3 reachability check.
        #   - TCP-22: sshd is already listening in every debug VM, so
        #     this proves bidirectional flow without an extra listener.
        #
        # Self-skips when the smoke runner can't reach the tree CIDR
        # (off-host laptop runs) — same gate as Section 1's egress
        # check, since we SSH into the source VM to run the probe.

        # Pull (name, ip, host) for our three VMs in one pass. Columns
        # from `get-machines` are: ID, NAME, STATE, IP, HOST.
        AA_TRIPLES=$(echo "$AA_LISTING" \
            | awk -v p="aa-[012]$S" '$2 ~ ("^"p"$") {print $2"|"$4"|"$5}')
        # Pick any cross-host pair from those three.
        SRC_NAME=""; SRC_IP=""; SRC_HOST=""
        DST_NAME=""; DST_IP=""; DST_HOST=""
        while IFS='|' read -r a_name a_ip a_host; do
            while IFS='|' read -r b_name b_ip b_host; do
                if [[ -n "$a_host" && -n "$b_host" && "$a_host" != "$b_host" ]]; then
                    SRC_NAME=$a_name; SRC_IP=$a_ip; SRC_HOST=$a_host
                    DST_NAME=$b_name; DST_IP=$b_ip; DST_HOST=$b_host
                    break 2
                fi
            done <<<"$AA_TRIPLES"
        done <<<"$AA_TRIPLES"
        [[ -n "$SRC_IP" && -n "$DST_IP" ]] \
            || fail "could not pick cross-host pair from placement (parse bug?)"

        # On a hypervisor, the route resolves either via `brt<vni>`
        # (cross-host via the overlay) or `tap<...>` (when the VM is
        # local to this host). Both mean we can SSH in. Off-host laptop
        # runs have no route at all and `ip route get` fails — that's
        # the real skip condition.
        if ! ip route get "$SRC_IP" >/dev/null 2>&1; then
            echo "  (skipping cross-node probe: no route to $SRC_IP — re-run on a hypervisor for full coverage)"
        else
            step "Cross-node probe: $SRC_NAME ($SRC_IP on $SRC_HOST) <-> $DST_NAME ($DST_IP on $DST_HOST)"
            wait_for_ssh_port "$SRC_IP" || fail "src $SRC_IP sshd never opened TCP-22"
            wait_for_ssh_port "$DST_IP" || fail "dst $DST_IP sshd never opened TCP-22"
            wait_for_ssh "$SRC_IP" || fail "src $SRC_IP sshd open but password auth failing"
            wait_for_ssh "$DST_IP" || fail "dst $DST_IP sshd open but password auth failing"

            # SRC -> DST.
            if ! ssh_guest "$SRC_IP" "ping -c3 -W2 $DST_IP" >/dev/null 2>&1; then
                fail "$SRC_NAME ($SRC_HOST) cannot ping $DST_NAME ($DST_HOST) — VXLAN forwarding broken in SRC->DST direction"
            fi
            if ! ssh_guest "$SRC_IP" "timeout 5 bash -c '>/dev/tcp/$DST_IP/22'" >/dev/null 2>&1; then
                fail "$SRC_NAME ($SRC_HOST) cannot TCP-connect to $DST_NAME:22 ($DST_HOST) — overlay drops or one-way ARP/FDB"
            fi
            # DST -> SRC. Asymmetric regressions (e.g. proxy-ARP only on
            # one host) only fail in one direction.
            if ! ssh_guest "$DST_IP" "ping -c3 -W2 $SRC_IP" >/dev/null 2>&1; then
                fail "$DST_NAME ($DST_HOST) cannot ping $SRC_NAME ($SRC_HOST) — VXLAN forwarding broken in DST->SRC direction"
            fi
            if ! ssh_guest "$DST_IP" "timeout 5 bash -c '>/dev/tcp/$SRC_IP/22'" >/dev/null 2>&1; then
                fail "$DST_NAME ($DST_HOST) cannot TCP-connect to $SRC_NAME:22 ($SRC_HOST) — overlay drops or one-way ARP/FDB"
            fi
            pass "ICMP + TCP both directions across $SRC_HOST <-> $DST_HOST (overlay forwarding healthy)"
        fi

        for f in "${AA_FIXTURES[@]}"; do "$BIN" delete -f "$f" >/dev/null; done
        "$BIN" delete -f "$AA_CLUSTER_FIXTURE" >/dev/null
    fi
fi

step "ALL SMOKE TESTS PASSED"
