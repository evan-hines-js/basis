#!/usr/bin/env bash
# Load test: spawn N parallel smoke.sh workers forever, each with a
# unique cluster/machine name so they don't collide on controller
# state. Every worker loops `smoke.sh --quick --parallel-safe
# --suffix=ltNNN`. Ctrl-C stops the workers.
#
# Usage:
#   scripts/load-testing.sh                    # 100 workers
#   WORKERS=25 scripts/load-testing.sh
#   LOG_DIR=/var/log/basis scripts/load-testing.sh
#
# Failures surface in three places (in increasing verbosity):
#   - This terminal, as a single line per failure (streamed live)
#   - $LOG_DIR/failures.log, the aggregate of every worker's failures
#   - $LOG_DIR/ltNNN.log, full stdout+stderr of each failing run
#
# And for progress:
#   - $LOG_DIR/ltNNN.summary, one line per worker with counters and
#     the most recent failure reason inline. `tail *.summary` tells
#     you at a glance which workers are healthy and why unhealthy
#     ones fell over.

set -euo pipefail

: "${BASIS_ENDPOINT:?BASIS_ENDPOINT not set}"
: "${BASIS_TLS_CA:?BASIS_TLS_CA not set}"
: "${BASIS_TLS_CERT:?BASIS_TLS_CERT not set}"
: "${BASIS_TLS_KEY:?BASIS_TLS_KEY not set}"

WORKERS="${WORKERS:-100}"
LOG_DIR="${LOG_DIR:-/tmp/basis-load}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SMOKE="$SCRIPT_DIR/smoke.sh"

# Start clean so stale failure captures from a prior run don't
# confuse `tail failures.log`. Per-worker logs will be recreated.
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"
: >"$LOG_DIR/failures.log"

echo "building basis-ctl..."
(cd "$REPO_ROOT" && cargo build --release --quiet -p basis-ctl)
export SMOKE_SKIP_BUILD=1

PIDS=()

stop() {
    echo
    echo "stopping $WORKERS workers..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
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
}
trap stop EXIT INT TERM

# Pull the first interesting error line out of a captured run. Prefers
# the smoke.sh-emitted "FAIL:" assertion, falls back to "Error:"
# (basis-ctl RPC surface), falls back to the last non-blank line.
extract_reason() {
    local f="$1"
    local line
    line="$(grep -m1 -E '^(FAIL:|Error:)' "$f" 2>/dev/null || true)"
    if [[ -z "$line" ]]; then
        line="$(grep -v '^[[:space:]]*$' "$f" 2>/dev/null | tail -1 || true)"
    fi
    # Trim to one line and cap length so summary lines stay readable.
    printf '%s' "${line:0:240}"
}

worker() {
    local suffix="$1"
    local log="$LOG_DIR/$suffix.log"
    local current="$LOG_DIR/$suffix.current"
    local summary="$LOG_DIR/$suffix.summary"
    local runs=0 fails=0 last_fail_at="-" last_fail_reason="-"

    # Write an initial summary so `tail *.summary` works immediately.
    printf 'worker=%s runs=0 fails=0 last_fail=-\n' "$suffix" >"$summary"

    while :; do
        runs=$((runs + 1))
        if "$SMOKE" --quick --parallel-safe "--suffix=$suffix" \
                >"$current" 2>&1; then
            :
        else
            fails=$((fails + 1))
            last_fail_at="$(date -u +%FT%TZ)"
            last_fail_reason="$(extract_reason "$current")"
            [[ -z "$last_fail_reason" ]] && last_fail_reason="(empty output; see $log)"

            # Append the full failing run to per-worker log.
            {
                echo "===== run $runs FAILED at $last_fail_at ====="
                cat "$current"
                echo
            } >>"$log"

            # Short form → shared failures.log AND the main terminal.
            # POSIX guarantees atomic writes below PIPE_BUF (~4 KiB),
            # so two workers appending short lines won't interleave.
            printf '[%s] %s run=%d: %s\n' \
                   "$last_fail_at" "$suffix" "$runs" "$last_fail_reason" \
                   >>"$LOG_DIR/failures.log"
            printf '%s %s run=%d FAIL: %s\n' \
                   "$last_fail_at" "$suffix" "$runs" "$last_fail_reason" >&2
        fi
        printf 'worker=%s runs=%d fails=%d last_fail_at=%s last_fail=%s\n' \
               "$suffix" "$runs" "$fails" "$last_fail_at" "$last_fail_reason" \
               >"$summary"
    done
}

cat <<EOF
starting $WORKERS workers, logs in $LOG_DIR/

live failures stream to this terminal below.

from another shell:
  tail -f $LOG_DIR/failures.log   # aggregate failure stream
  tail    $LOG_DIR/*.summary      # per-worker counters + last fail reason
  less    $LOG_DIR/ltNNN.log      # full captured output of each failing run

ctrl-c to stop.
----------
EOF

for i in $(seq 1 "$WORKERS"); do
    suffix="$(printf 'lt%03d' "$i")"
    worker "$suffix" &
    PIDS+=($!)
done

echo "$WORKERS workers running."
wait || true
