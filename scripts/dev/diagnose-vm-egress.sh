#!/usr/bin/env bash
# Diagnose a tree-cluster VM that can't reach the LAN/Internet.
#
# Walks the egress path (VM → bridge → VRF → uplink → LAN router) and
# reports which hop drops the packet. Run as root on the hypervisor
# carrying the VM. The VM's TAP must already be enslaved to brc<vni>.
#
# Usage:
#   sudo ./diagnose-vm-egress.sh <vni> <vm_ip> [uplink_iface] [target_ip]
#
# Defaults: uplink_iface=vmbr0, target_ip=8.8.8.8
#
# Example:
#   sudo ./diagnose-vm-egress.sh 10000 10.100.0.33

set -euo pipefail

VNI="${1:-}"
VM_IP="${2:-}"
UPLINK="${3:-vmbr0}"
TARGET="${4:-8.8.8.8}"

if [[ -z "$VNI" || -z "$VM_IP" ]]; then
    echo "usage: $0 <vni> <vm_ip> [uplink] [target_ip]" >&2
    exit 2
fi

if [[ "$EUID" -ne 0 ]]; then
    echo "must run as root (needs sysctl + tcpdump + ip route)" >&2
    exit 2
fi

BRIDGE="brc${VNI}"
RESET='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'

section() { echo -e "\n${BLUE}=== $1 ===${RESET}"; }
ok()      { echo -e "${GREEN}✓ $1${RESET}"; }
warn()    { echo -e "${YELLOW}⚠ $1${RESET}"; }
fail()    { echo -e "${RED}✗ $1${RESET}"; }

# ----- Discovery -----
section "Discovery"

if ! ip link show "$BRIDGE" &>/dev/null; then
    fail "bridge $BRIDGE does not exist on this host"
    exit 1
fi
ok "bridge $BRIDGE exists"

VRF="$(ip -o link show "$BRIDGE" | awk '{
    for (i=1; i<=NF; i++) if ($i == "master") { print $(i+1); exit }
}')"
if [[ -z "$VRF" ]]; then
    warn "$BRIDGE has no master VRF (LAN-pool cluster?). Continuing without VRF table check."
    TABLE=""
else
    ok "bridge enslaved to VRF: $VRF"
    TABLE="$(ip -d link show "$VRF" | awk '/vrf table/ { for (i=1; i<=NF; i++) if ($i == "table") { print $(i+1); exit } }')"
    if [[ -z "$TABLE" ]]; then
        fail "could not parse VRF table id from 'ip -d link show $VRF'"
        exit 1
    fi
    ok "VRF table id: $TABLE"
fi

if ! ip link show "$UPLINK" &>/dev/null; then
    fail "uplink $UPLINK does not exist"
    exit 1
fi
ok "uplink $UPLINK exists"

UPLINK_GW="$(ip route show default | awk -v dev="$UPLINK" '$0 ~ dev { for (i=1; i<=NF; i++) if ($i == "via") { print $(i+1); exit } }')"
if [[ -z "$UPLINK_GW" ]]; then
    fail "no default route via $UPLINK in main table"
    exit 1
fi
ok "uplink gateway: $UPLINK_GW"

# ----- Sysctls -----
section "Sysctls"

IP_FORWARD="$(sysctl -n net.ipv4.ip_forward)"
[[ "$IP_FORWARD" -eq 1 ]] && ok "net.ipv4.ip_forward = 1" || fail "net.ipv4.ip_forward = $IP_FORWARD (expected 1)"

for k in all "$UPLINK" "$BRIDGE"; do
    rp="$(sysctl -n "net.ipv4.conf.$k.rp_filter" 2>/dev/null || echo "?")"
    if [[ "$rp" == "1" ]]; then
        warn "net.ipv4.conf.$k.rp_filter = 1 (strict — can drop replies on asymmetric routing)"
    else
        ok "net.ipv4.conf.$k.rp_filter = $rp"
    fi
done

# ----- Routing tables -----
section "Routing"

if [[ -n "$TABLE" ]]; then
    echo "VRF table $TABLE:"
    ip route show table "$TABLE" | sed 's/^/    /'
    if ip route show table "$TABLE" | grep -q '^default'; then
        ok "VRF has default route"
    else
        fail "VRF has NO default route — packets to non-tree destinations will drop in the VRF"
        echo "    fix: ip route add default via $UPLINK_GW dev $UPLINK table $TABLE"
    fi
fi

echo "Main table default:"
ip route show default | sed 's/^/    /'

# ----- nat / FORWARD -----
section "iptables — MASQUERADE + FORWARD"

# Look for masquerade for the cluster CIDR. We don't know the CIDR
# without controller introspection; instead, look for any rule whose
# -s overlaps with the bridge's local /N.
BRIDGE_NET="$(ip -4 -o addr show "$BRIDGE" | awk '{print $4; exit}')"
if [[ -z "$BRIDGE_NET" ]]; then
    fail "bridge $BRIDGE has no IPv4 address — cluster never bootstrapped"
    exit 1
fi
ok "cluster CIDR (from bridge addr): $BRIDGE_NET"

CLUSTER_NET="$(ipcalc -n "$BRIDGE_NET" 2>/dev/null | awk -F= '/^Network/ {print $2}' || echo "")"
if [[ -z "$CLUSTER_NET" ]]; then
    # ipcalc not installed; derive with python
    CLUSTER_NET="$(python3 -c "import ipaddress,sys; n=ipaddress.ip_interface(sys.argv[1]); print(n.network)" "$BRIDGE_NET" 2>/dev/null || echo "")"
fi
[[ -n "$CLUSTER_NET" ]] && ok "cluster network: $CLUSTER_NET" || warn "could not compute cluster network, MASQUERADE check approximated"

if iptables -t nat -S POSTROUTING | grep -E "MASQUERADE" | grep -q "$BRIDGE\|$CLUSTER_NET\|$UPLINK"; then
    ok "MASQUERADE rule exists in nat/POSTROUTING"
    iptables -t nat -S POSTROUTING | grep MASQUERADE | sed 's/^/    /'
else
    fail "no MASQUERADE rule for this cluster — egress will leave with src=$VM_IP and replies will not return"
fi

echo "FORWARD chain (filter):"
iptables -L FORWARD -v -n --line-numbers | head -20 | sed 's/^/    /'

# ----- Packet trace -----
section "Live packet trace"

if ! ip neigh show "$VM_IP" dev "$BRIDGE" 2>/dev/null | grep -qE 'REACHABLE|STALE|DELAY|PROBE'; then
    warn "$VM_IP not in $BRIDGE neighbour table; sending arping to populate"
    ip vrf exec "$VRF" arping -I "$BRIDGE" -c 2 -w 2 "$VM_IP" 2>&1 | sed 's/^/    /' || true
fi

NAT_BEFORE="$(iptables -t nat -L POSTROUTING -v -n -x | awk '/MASQUERADE/ {print $1}' | head -1)"
[[ -z "$NAT_BEFORE" ]] && NAT_BEFORE=0

echo
echo "Capturing 12s on $BRIDGE, $VRF, $UPLINK simultaneously."
echo "From the VM ($VM_IP), run: ping -c 5 $TARGET"
echo

mkdir -p /tmp/basis-diag
: >/tmp/basis-diag/brc.txt
: >/tmp/basis-diag/vrf.txt
: >/tmp/basis-diag/uplink.txt

timeout 12 tcpdump -ni "$BRIDGE" -nn -l "icmp and host $TARGET" >/tmp/basis-diag/brc.txt 2>/dev/null &
PID_BRC=$!
if [[ -n "$VRF" ]]; then
    timeout 12 tcpdump -ni "$VRF" -nn -l "icmp and host $TARGET" >/tmp/basis-diag/vrf.txt 2>/dev/null &
    PID_VRF=$!
fi
timeout 12 tcpdump -ni "$UPLINK" -nn -l "icmp and host $TARGET" >/tmp/basis-diag/uplink.txt 2>/dev/null &
PID_UP=$!

# Trigger ping from the host via the VRF — proves the path even if
# the user can't easily ping from the VM. If they DO ping from the VM
# during the capture window, even better.
ip vrf exec "$VRF" ping -c 3 -W 2 "$TARGET" >/dev/null 2>&1 || true

wait $PID_BRC $PID_UP ${PID_VRF:-} 2>/dev/null || true

NAT_AFTER="$(iptables -t nat -L POSTROUTING -v -n -x | awk '/MASQUERADE/ {print $1}' | head -1)"
[[ -z "$NAT_AFTER" ]] && NAT_AFTER=0

echo
echo "Captures:"
for f in brc vrf uplink; do
    n=$(wc -l </tmp/basis-diag/$f.txt 2>/dev/null || echo 0)
    label=$f
    [[ "$f" == "brc" ]] && label="brc=$BRIDGE"
    [[ "$f" == "vrf" ]] && label="vrf=$VRF"
    [[ "$f" == "uplink" ]] && label="uplink=$UPLINK"
    echo "  $label: $n packets"
    head -3 /tmp/basis-diag/$f.txt 2>/dev/null | sed 's/^/    /'
done

# Distinguish "the VM transmitted" from "the host's own probe transmitted":
# count brc10000 packets whose source matches the VM_IP. If zero, the
# VM never put a packet on the wire and any vmbr0 traffic we see is
# the script's own host-side probe inside the VRF.
VM_TX_COUNT="$(grep -c "IP $VM_IP >" /tmp/basis-diag/brc.txt 2>/dev/null || echo 0)"
echo "  VM-sourced packets on $BRIDGE: $VM_TX_COUNT"

NAT_DELTA=$((NAT_AFTER - NAT_BEFORE))
echo
echo "MASQUERADE rule packet count: $NAT_BEFORE → $NAT_AFTER (delta: $NAT_DELTA)"

# ----- Verdict -----
section "Verdict"

if [[ -z "$VRF" ]]; then
    echo "LAN-pool cluster (no VRF). Diagnose using main table only."
elif ! ip route show table "$TABLE" | grep -q '^default'; then
    fail "ROOT CAUSE: VRF table $TABLE has no default route. VMs in this tree can't reach anything outside the cluster's own CIDR."
    echo
    echo "  Workaround: ip route add default via $UPLINK_GW dev $UPLINK table $TABLE"
    echo
    echo "  Real fix: agent's ensure_vrf must install a default route into the"
    echo "  VRF's table at create time (and keep it updated if the uplink gateway"
    echo "  changes). See crates/basis-agent/src/network/cluster.rs::ensure_vrf."
elif [[ "$VM_TX_COUNT" -eq 0 ]]; then
    fail "ROOT CAUSE: VM ($VM_IP) put zero packets on $BRIDGE during the capture window."
    echo
    echo "  Host-side ping THROUGH the VRF works (see uplink+MASQUERADE above) — basis"
    echo "  dataplane is correct. The VM itself isn't transmitting."
    echo
    echo "  Check inside the VM:"
    echo "    ip addr                              # NIC up, IP assigned?"
    echo "    ip route                             # default via $(echo "$BRIDGE_NET" | cut -d/ -f1)?"
    echo "    journalctl -u cloud-final --no-pager | tail -50"
    echo "    journalctl -u systemd-networkd --no-pager | tail -30"
    echo "    cat /etc/netplan/*.yaml"
    echo
    echo "  Common cause: cloud-init user-data failed mid-run (missing user, bad"
    echo "  chpasswd format, etc.) and unwound before applying network-config."
elif [[ "$(wc -l </tmp/basis-diag/uplink.txt)" -eq 0 ]]; then
    fail "Packets reach the VRF but are not making it to the uplink — FORWARD chain or routing issue."
elif [[ "$NAT_DELTA" -eq 0 ]]; then
    fail "Packets reach the uplink but MASQUERADE rule did not fire — packet leaves with VM source IP, replies never return."
elif [[ "$(grep -c "$TARGET >" /tmp/basis-diag/uplink.txt 2>/dev/null || echo 0)" -gt 0 && "$(grep -c "> $TARGET" /tmp/basis-diag/uplink.txt 2>/dev/null || echo 0)" -gt 0 ]]; then
    ok "Egress + reply both seen on $UPLINK. If the VM still can't ping, the failure is between vmbr0 and the VM's stack — likely conntrack/VRF reverse-path."
else
    warn "Egress packets seen on $UPLINK but no replies. Upstream LAN/Internet issue."
fi
