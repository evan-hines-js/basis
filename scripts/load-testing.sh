#!/usr/bin/env bash
# Load test: hammer one cluster with create/delete churn so the
# scheduler runs the placement chain (capacity + anti-affinity + rank
# + labels) thousands of times across whatever hosts are connected.
# A single shared cluster is the realistic shape — anti-affinity is
# per-cluster, so spreading the same cluster's VMs across hosts is
# what we actually want to stress.
#
# Each "slot" worker runs forever:
#   create VM → hold for random 10..60s → delete → repeat
# `WORKERS` is the cap on concurrent VMs. With WORKERS=10 the cluster
# has 0..10 VMs alive at any moment, depending on timing.
#
# Usage:
#   scripts/load-testing.sh                    # 10 workers, 10..60s VM lifetimes
#   WORKERS=25 scripts/load-testing.sh
#   MIN_HOLD_SECS=5 MAX_HOLD_SECS=15 scripts/load-testing.sh
#   LOG_DIR=/var/log/basis scripts/load-testing.sh
#
# Failures surface in three places (in increasing verbosity):
#   - This terminal, as a single line per failure (streamed live)
#   - $LOG_DIR/failures.log, the aggregate of every slot's failures
#   - $LOG_DIR/ltNNN.log, full stdout+stderr of each failing op
#
# Progress:
#   - $LOG_DIR/ltNNN.summary, one line per slot with counters + last
#     failure reason inline. `tail *.summary` tells you which slots
#     are healthy and why unhealthy ones fell over.
#   - $LOG_DIR/distribution.log, per-host VM counts every 10s. Shows
#     the scheduler's anti-affinity spread in real time.

set -euo pipefail

: "${BASIS_ENDPOINT:?BASIS_ENDPOINT not set}"

WORKERS="${WORKERS:-10}"
MIN_HOLD_SECS="${MIN_HOLD_SECS:-10}"
MAX_HOLD_SECS="${MAX_HOLD_SECS:-60}"
LOG_DIR="${LOG_DIR:-/tmp/basis-load}"
CLUSTER_NAME="${CLUSTER_NAME:-loadtest}"
EXTERNAL_POOL="${EXTERNAL_POOL:-cell-public}"
EXTERNAL_SERVICE_IPS="${EXTERNAL_SERVICE_IPS:-2}"
DISTRIBUTION_INTERVAL_SECS="${DISTRIBUTION_INTERVAL_SECS:-10}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES="$REPO_ROOT/crates/basis-ctl/fixtures"
BIN="$REPO_ROOT/target/release/basis-ctl"

# PKI fallback (mirrors smoke.sh): rsync.sh syncs deploy/ansible/pki/
# to every host, so unset/stale BASIS_TLS_* paths fall back there
# rather than blowing up with a bare "No such file" from tonic.
PKI_DEFAULT_DIR="$REPO_ROOT/deploy/ansible/pki"
resolve_pki() {
    local var="$1" filename="$2" current
    current="${!var:-}"
    if [[ -n "$current" && -f "$current" ]]; then
        return 0
    fi
    local fallback="$PKI_DEFAULT_DIR/$filename"
    if [[ -f "$fallback" ]]; then
        if [[ -n "$current" ]]; then
            echo "  warn: $var=$current does not exist; falling back to $fallback" >&2
        fi
        printf -v "$var" '%s' "$fallback"
        export "${var?}"
        return 0
    fi
    echo "FAIL: $var unset (or stale) and no fallback at $fallback" >&2
    exit 2
}
resolve_pki BASIS_TLS_CA   ca.crt
resolve_pki BASIS_TLS_CERT capi-provider.crt
resolve_pki BASIS_TLS_KEY  capi-provider.key

# Start clean so stale failure captures from a prior run don't
# confuse `tail failures.log`. Per-slot logs will be recreated.
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"
: >"$LOG_DIR/failures.log"
: >"$LOG_DIR/distribution.log"

echo "building basis-ctl..."
# Same cargo resolution smoke.sh uses — prefer the rustup toolchain
# the basis-holod ansible role installs. Hypervisors run Ubuntu's
# apt rustc 1.85, below the `time` crate's 1.88 MSRV; falling back
# to system cargo on those hosts produces an inscrutable dep-graph
# error.
if [[ -x /var/cache/holo-build/cargo/bin/cargo ]]; then
    (cd "$REPO_ROOT" && \
     RUSTUP_HOME=/var/cache/holo-build/rustup \
     CARGO_HOME=/var/cache/holo-build/cargo \
        /var/cache/holo-build/cargo/bin/cargo build --release --quiet -p basis-ctl)
else
    (cd "$REPO_ROOT" && cargo build --release --quiet -p basis-ctl)
fi
[[ -x "$BIN" ]] || { echo "FAIL: basis-ctl missing at $BIN"; exit 2; }

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/basis-load.XXXXXX")"

# One shared cluster. Generated rather than using the shipped fixture
# so CLUSTER_NAME / EXTERNAL_POOL are operator-tunable.
CLUSTER_FIXTURE="$TMP_DIR/cluster.yaml"
cat >"$CLUSTER_FIXTURE" <<YAML
apiVersion: basis.dev/v1
kind: Cluster
metadata:
  name: $CLUSTER_NAME
spec:
  externalIpPool: $EXTERNAL_POOL
  externalServiceIps: $EXTERNAL_SERVICE_IPS
YAML

PIDS=()
DIST_PID=""

stop() {
    echo
    echo "stopping $WORKERS workers..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    [[ -n "$DIST_PID" ]] && kill "$DIST_PID" 2>/dev/null || true
    wait 2>/dev/null || true

    echo
    echo "tearing down cluster $CLUSTER_NAME (cascades all VMs)..."
    "$BIN" delete -f "$CLUSTER_FIXTURE" >/dev/null 2>&1 \
        || echo "  warn: cluster delete returned non-zero (may already be gone)"

    rm -rf "$TMP_DIR"

    echo
    echo "final summary:"
    shopt -s nullglob
    for f in "$LOG_DIR"/*.summary; do
        cat "$f"
    done
    shopt -u nullglob
    echo
    local total_fails
    total_fails="$(wc -l <"$LOG_DIR/failures.log" | tr -d ' ')"
    echo "total failures: $total_fails  (see $LOG_DIR/failures.log)"
    echo "host distribution log: $LOG_DIR/distribution.log"
}
trap stop EXIT INT TERM

extract_reason() {
    local f="$1"
    local line
    line="$(grep -m1 -E '^(FAIL:|Error:)' "$f" 2>/dev/null || true)"
    if [[ -z "$line" ]]; then
        line="$(grep -v '^[[:space:]]*$' "$f" 2>/dev/null | tail -1 || true)"
    fi
    printf '%s' "${line:0:240}"
}

# Per-slot worker. Each slot owns one VM at a time: create, hold,
# delete, repeat. Random hold time means slots desynchronize after
# the first cycle so create/delete operations spread out across the
# wall clock (instead of stampeding the controller every N seconds).
worker() {
    local slot="$1"
    local log="$LOG_DIR/$slot.log"
    local current="$LOG_DIR/$slot.current"
    local summary="$LOG_DIR/$slot.summary"
    local creates=0 deletes=0 fails=0 last_fail_at="-" last_fail_reason="-"

    printf 'slot=%s creates=0 deletes=0 fails=0 last_fail=-\n' "$slot" >"$summary"

    # Per-slot machine fixture, regenerated each cycle so the VM name
    # carries the slot suffix (idempotent: re-applying same spec hits
    # `create_machine`'s name-based idempotency rather than a fresh
    # slot each cycle, which is what we want — an apply-then-delete
    # cadence that exercises the scheduler on each create).
    local machine_fixture="$TMP_DIR/$slot-machine.yaml"
    local machine_name="loadtest-$slot"
    cat >"$machine_fixture" <<YAML
apiVersion: basis.dev/v1
kind: Machine
metadata:
  name: $machine_name
spec:
  cluster: $CLUSTER_NAME
  cpu: 2
  memoryMib: 2048
  diskGib: 10
  image: ghcr.io/evan-hines-js/lattice-node:v1.32.0
  bootstrapDataFile: $FIXTURES/bootstrap-debug.yaml
  gpus: 0
YAML

    record_failure() {
        local op="$1"
        fails=$((fails + 1))
        last_fail_at="$(date -u +%FT%TZ)"
        last_fail_reason="$op: $(extract_reason "$current")"
        [[ -z "$last_fail_reason" ]] && last_fail_reason="$op: (empty output; see $log)"
        {
            echo "===== $op FAILED at $last_fail_at (creates=$creates deletes=$deletes) ====="
            cat "$current"
            echo
        } >>"$log"
        printf '[%s] %s %s: %s\n' \
               "$last_fail_at" "$slot" "$op" "$last_fail_reason" \
               >>"$LOG_DIR/failures.log"
        printf '%s %s %s FAIL: %s\n' \
               "$last_fail_at" "$slot" "$op" "$last_fail_reason" >&2
    }

    while :; do
        if "$BIN" apply -f "$machine_fixture" >"$current" 2>&1; then
            creates=$((creates + 1))
        else
            record_failure "create"
            sleep 1
            continue
        fi

        local hold=$((MIN_HOLD_SECS + RANDOM % (MAX_HOLD_SECS - MIN_HOLD_SECS + 1)))
        sleep "$hold"

        if "$BIN" delete -f "$machine_fixture" >"$current" 2>&1; then
            deletes=$((deletes + 1))
        else
            record_failure "delete"
            sleep 1
        fi

        printf 'slot=%s creates=%d deletes=%d fails=%d last_fail_at=%s last_fail=%s\n' \
               "$slot" "$creates" "$deletes" "$fails" "$last_fail_at" "$last_fail_reason" \
               >"$summary"
    done
}

# Periodic snapshot of how many VMs each host is carrying. Reads from
# the controller via `get-machines`; the third column is the host id.
# Anti-affinity is per-cluster, so a healthy spread looks like
# (count_h1 - count_h2) bouncing around 0; a stuck-on-one-host pattern
# is a regression in either anti-affinity scoring or label filtering.
distribution_logger() {
    while :; do
        local ts
        ts="$(date -u +%FT%TZ)"
        # `get-machines` columns: id name state ip host. We only care
        # about state=RUNNING so failed/pending don't skew the picture.
        local snapshot
        snapshot="$("$BIN" get-machines 2>/dev/null \
            | awk -v cluster="$CLUSTER_NAME" '
                NR == 1 { next }                  # header
                $2 ~ ("^loadtest-") && $3 == "RUNNING" { print $5 }
              ' \
            | sort | uniq -c | awk '{ printf "%s=%d ", $2, $1 }')"
        [[ -z "$snapshot" ]] && snapshot="(no running VMs)"
        printf '%s %s\n' "$ts" "$snapshot" >>"$LOG_DIR/distribution.log"
        sleep "$DISTRIBUTION_INTERVAL_SECS"
    done
}

echo "creating shared cluster $CLUSTER_NAME..."
"$BIN" apply -f "$CLUSTER_FIXTURE" \
    || { echo "FAIL: cluster apply failed; see controller log"; exit 1; }

cat <<EOF

starting $WORKERS slot workers, hold time ${MIN_HOLD_SECS}..${MAX_HOLD_SECS}s
logs in $LOG_DIR/

live failures stream to this terminal below.

from another shell:
  tail -f $LOG_DIR/failures.log      # aggregate failure stream
  tail -f $LOG_DIR/distribution.log  # per-host VM counts (anti-affinity check)
  tail    $LOG_DIR/*.summary         # per-slot counters + last fail reason
  less    $LOG_DIR/ltNNN.log         # full output of each failing op

ctrl-c to stop (cluster + all VMs cleaned up on exit).
----------
EOF

distribution_logger &
DIST_PID=$!

for i in $(seq 1 "$WORKERS"); do
    slot="$(printf 'lt%03d' "$i")"
    worker "$slot" &
    PIDS+=($!)
done

echo "$WORKERS slots running, distribution snapshots every ${DISTRIBUTION_INTERVAL_SECS}s."
wait || true
