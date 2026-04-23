#!/usr/bin/env bash
# Chaos monkey for the basis fleet. Periodically attacks the controller,
# agents, and running VMs over SSH + systemd while a load test
# (scripts/load-testing.sh) exercises the control plane. The load test's
# invariants — cascade delete cleans up, idempotent re-apply returns the
# same row, orphan sweep reclaims leaked resources — must still hold
# over a long enough window despite these attacks. A rising failure
# rate that doesn't recover once attacks pause is a real availability
# or consistency bug worth investigating.
#
# Runs on the operator workstation; every attack is an ssh + systemctl
# / iptables command against a remote host. Use alongside the load
# test:
#
#   term 1: scripts/load-testing.sh
#   term 2: CHAOS_HOSTS="root@10.0.0.206" scripts/chaos.sh
#
# Watch /tmp/basis-load/failures.log next to /tmp/basis-chaos.log to
# correlate smoke.sh failures with attack timestamps.
#
# Env:
#   CHAOS_HOSTS          required. Space-separated user@host SSH targets
#                        covering the agent fleet.
#   CHAOS_CONTROLLER     user@host of the controller. Defaults to the
#                        first entry in CHAOS_HOSTS (dev deploys are
#                        typically colocated).
#   CHAOS_SEED           integer; deterministic seed. Default: epoch
#                        seconds (non-deterministic). Same seed replays
#                        the same attack sequence — rerun a failing
#                        run to reproduce.
#   CHAOS_INTERVAL       seconds between attacks. Default 90.
#   CHAOS_JITTER         ± jitter on interval. Default 30.
#   CHAOS_ATTACKS        space-separated subset of the attack list.
#                        Default: "agent_restart agent_network vm_kill
#                                  controller_restart".
#   CHAOS_BLACKOUT_SECS  agent_network blackout duration. Default 10.
#   CHAOS_DRY_RUN=1      log planned attacks without executing; useful
#                        for previewing the sequence a seed produces.
#   LOG_FILE             chaos event log. Default /tmp/basis-chaos.log.
#
# Attacks:
#   agent_restart       `systemctl restart basis-agent` on a random
#                       host. Exercises the agent's startup reconcile,
#                       tracked-unit rediscovery, and the post-handshake
#                       state-report path.
#   agent_network       `iptables -j DROP` on tcp/7443 outbound for
#                       CHAOS_BLACKOUT_SECS seconds on a random host.
#                       Exercises the agent's reconnect loop and the
#                       controller's stale-heartbeat detection.
#   vm_kill             `systemctl stop basis-vm-<uuid>.service` on a
#                       random running VM. Simulates a cloud-hypervisor
#                       crash; the agent's periodic reconcile should
#                       detect it and report FAILED, and the controller
#                       should transition state without orphaning the
#                       LV / tap / IP.
#   controller_restart  `systemctl restart basis-controller`. In-flight
#                       RPCs will fail hard; smoke.sh iterations during
#                       the window will log failures. After restart,
#                       agents reconnect, ReconcileHost resyncs, and
#                       subsequent iterations should succeed.

set -euo pipefail

: "${CHAOS_HOSTS:?CHAOS_HOSTS must be set (e.g. \"root@10.0.0.206\")}"
CHAOS_INTERVAL="${CHAOS_INTERVAL:-90}"
CHAOS_JITTER="${CHAOS_JITTER:-30}"
CHAOS_SEED="${CHAOS_SEED:-$(date +%s)}"
CHAOS_ATTACKS="${CHAOS_ATTACKS:-agent_restart agent_network vm_kill controller_restart}"
CHAOS_BLACKOUT_SECS="${CHAOS_BLACKOUT_SECS:-10}"
CHAOS_DRY_RUN="${CHAOS_DRY_RUN:-0}"
LOG_FILE="${LOG_FILE:-/tmp/basis-chaos.log}"

read -r -a HOSTS <<<"$CHAOS_HOSTS"
CHAOS_CONTROLLER="${CHAOS_CONTROLLER:-${HOSTS[0]}}"
read -r -a ATTACKS <<<"$CHAOS_ATTACKS"

: >"$LOG_FILE"

ssh_cmd() {
    ssh -o ConnectTimeout=5 \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        "$@"
}

log() {
    printf '[%s] %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG_FILE"
}

# Seeded PRNG. awk's srand + a per-call counter gives a deterministic
# sequence from (CHAOS_SEED, N). Not cryptographic; doesn't need to be.
COUNTER=0
seeded_rand() {
    local min="$1" max="$2"
    COUNTER=$((COUNTER + 1))
    awk -v seed="$CHAOS_SEED" -v n="$COUNTER" -v min="$min" -v max="$max" 'BEGIN {
        srand(seed + n)
        printf "%d", min + int(rand() * (max - min + 1))
    }'
}

pick_host()   { echo "${HOSTS[$(seeded_rand 0 $((${#HOSTS[@]}    - 1)))]}"; }
pick_attack() { echo "${ATTACKS[$(seeded_rand 0 $((${#ATTACKS[@]} - 1)))]}"; }

# ---- Attacks ----

attack_agent_restart() {
    local host="$1"
    log "attack=agent_restart host=$host"
    [[ "$CHAOS_DRY_RUN" == 1 ]] && return
    ssh_cmd "$host" systemctl restart basis-agent 2>&1 \
        | sed 's/^/  /' | tee -a "$LOG_FILE" >/dev/null
}

attack_controller_restart() {
    local host="$CHAOS_CONTROLLER"
    log "attack=controller_restart host=$host"
    [[ "$CHAOS_DRY_RUN" == 1 ]] && return
    ssh_cmd "$host" systemctl restart basis-controller 2>&1 \
        | sed 's/^/  /' | tee -a "$LOG_FILE" >/dev/null
}

attack_agent_network() {
    local host="$1"
    local target="${CHAOS_CONTROLLER##*@}"   # strip user@ prefix
    log "attack=agent_network host=$host target=$target:7443 blackout=${CHAOS_BLACKOUT_SECS}s"
    [[ "$CHAOS_DRY_RUN" == 1 ]] && return
    # Two-rule dance to keep SSH safe:
    #   (1) Insert ACCEPT for outbound tcp sport=22 at OUTPUT chain
    #       position 1. iptables evaluates top-down, so SSH reply
    #       traffic bypasses any DROP rule appended later. Defense-
    #       in-depth: if the DROP rule below ever over-fires (misread
    #       of --dport, conntrack quirk, whatever), our SSH session
    #       stays alive and we keep a cleanup path.
    #   (2) DROP outbound traffic to the controller's gRPC endpoint
    #       specifically, NOT all outbound tcp/7443 anywhere on the
    #       host. Narrow targeting avoids surprising other services.
    # Trap cleans up both rules even if this ssh session dies during
    # the blackout.
    ssh_cmd "$host" "bash -c '
        cleanup() {
            iptables -D OUTPUT -p tcp -d $target --dport 7443 -j DROP 2>/dev/null || true
            iptables -D OUTPUT -p tcp --sport 22 -j ACCEPT 2>/dev/null || true
        }
        trap cleanup EXIT
        iptables -I OUTPUT 1 -p tcp --sport 22 -j ACCEPT
        iptables -A OUTPUT -p tcp -d $target --dport 7443 -j DROP
        sleep $CHAOS_BLACKOUT_SECS
    '" 2>&1 | sed 's/^/  /' | tee -a "$LOG_FILE" >/dev/null
}

attack_vm_kill() {
    local host="$1"
    local unit
    unit="$(ssh_cmd "$host" \
        "systemctl list-units --type=service --state=running --no-legend 'basis-vm-*' | awk 'NR==1 {print \$1}'" \
        2>/dev/null || true)"
    if [[ -z "$unit" ]]; then
        log "attack=vm_kill host=$host (no running basis-vm-* units, skipped)"
        return
    fi
    log "attack=vm_kill host=$host unit=$unit"
    [[ "$CHAOS_DRY_RUN" == 1 ]] && return
    ssh_cmd "$host" "systemctl stop $unit" 2>&1 \
        | sed 's/^/  /' | tee -a "$LOG_FILE" >/dev/null
}

# ---- Loop ----

cat <<EOF
basis chaos monkey
==================
hosts:       ${HOSTS[*]}
controller:  $CHAOS_CONTROLLER
attacks:     ${ATTACKS[*]}
interval:    ${CHAOS_INTERVAL}s ± ${CHAOS_JITTER}s
seed:        $CHAOS_SEED
dry-run:     $([[ "$CHAOS_DRY_RUN" == 1 ]] && echo "yes (no commands will execute)" || echo "no")
log:         $LOG_FILE

ctrl-c to stop.

EOF

while :; do
    delay="$(seeded_rand $((CHAOS_INTERVAL - CHAOS_JITTER)) $((CHAOS_INTERVAL + CHAOS_JITTER)))"
    sleep "$delay"

    attack="$(pick_attack)"
    case "$attack" in
        agent_restart)      attack_agent_restart      "$(pick_host)" ;;
        agent_network)      attack_agent_network      "$(pick_host)" ;;
        vm_kill)            attack_vm_kill            "$(pick_host)" ;;
        controller_restart) attack_controller_restart ;;
        *)                  log "unknown attack: $attack" ;;
    esac
done
