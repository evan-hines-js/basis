#!/usr/bin/env bash
# Probe whether the LAN gateway will accept an eBGP session from basis-controller.
#
# This is a binary feasibility check for "can we run BGP-upstream in this
# homelab?" Comcast XB-series gateways and the vast majority of consumer
# routers have BGP daemons absent or firewalled off, so the test is
# decisive: if tcp/179 isn't accepting connections, eBGP-upstream is not
# possible without replacing the gateway.
#
# Usage:
#   ./probe-router-bgp.sh [gateway-ip]
# If the gateway IP is omitted the script auto-detects via `route get`.

set -u

GATEWAY="${1:-}"

if [[ -z "$GATEWAY" ]]; then
  case "$(uname)" in
    Darwin) GATEWAY=$(route -n get default 2>/dev/null | awk '/gateway:/ {print $2}') ;;
    Linux)  GATEWAY=$(ip -4 route show default 2>/dev/null | awk '/default/ {print $3; exit}') ;;
    *)      echo "unknown OS — pass the gateway IP explicitly"; exit 2 ;;
  esac
fi

if [[ -z "$GATEWAY" ]]; then
  echo "could not auto-detect default gateway — pass an IP as \$1"
  exit 2
fi

echo "== Probing $GATEWAY =="

# 1. ICMP reachability (sanity — confirms gateway is up).
if ping -c 1 -W 2 "$GATEWAY" >/dev/null 2>&1; then
  echo "  ping            : OK"
else
  echo "  ping            : FAIL — gateway unreachable, abort."
  exit 1
fi

# 2. tcp/179 reachability. nc returns 0 on a successful three-way handshake
# (port open + listener accepted), non-zero on RST or timeout. Comcast
# XB-series gateways close 179 inbound at the WAN edge AND don't run BGP
# locally, so this is the decisive check — if 179 doesn't open, BGP is
# physically blocked.
echo -n "  tcp/179         : "
if nc -z -v -w 3 "$GATEWAY" 179 >/dev/null 2>&1; then
  echo "ACCEPTING (port 179 open — BGP daemon may be present)"
  bgp_port_open=1
else
  echo "REFUSED/TIMEOUT (no BGP daemon listening)"
  bgp_port_open=0
fi

# 3. Gateway identification — router web UI returns the firmware string
# in headers / HTML. Strictly diagnostic; doesn't change the verdict.
echo -n "  http banner     : "
banner=$(curl -s -m 3 -I "http://$GATEWAY/" 2>/dev/null | tr -d '\r' | grep -iE '^(server|x-cmts|x-router):' | head -1)
if [[ -n "$banner" ]]; then
  echo "$banner"
else
  echo "(no banner)"
fi

echo
echo "== Verdict =="
if [[ "$bgp_port_open" == "1" ]]; then
  cat <<EOF
tcp/179 is open. The gateway either runs a BGP daemon directly or
forwards 179 to one. Next step: try a real eBGP OPEN with a known peer
ASN to see if the daemon will negotiate. To do that with no extra tools:

  # Install gobgp (or any BGP CLI), then:
  gobgp -u $GATEWAY -p 50051 global rib summary

If that fails, the daemon is listening but not configured for our peer.
Either way, BGP-upstream is *physically* possible here.
EOF
  exit 0
else
  cat <<EOF
tcp/179 is closed. The gateway is not running BGP and is not forwarding
the port — typical for Comcast XB-series, eero, and any consumer ISP-
issued gateway. eBGP-upstream is *not possible* with this gateway in
the path. Options:

  1. Replace the gateway with a BGP-capable device (MikroTik / pfSense /
     OPNsense / VyOS / etc.) and re-run this probe.
  2. Run a BGP-capable router *behind* the Comcast gateway and put the
     basis cell's uplink on its LAN side. The Comcast gateway becomes a
     dumb pipe; the new router peers eBGP with basis-controller.
  3. Stay on the L2-stub path (proxy-ARP / GARP / elect_lan_vip_owner).
     Functional but doesn't scale beyond a single L2 segment.

For development / testing of the BGP-upstream code path: stand up a
second holod (or an FRR / GoBGP container) on a basis host and peer
basis-controller against it. Validates the controller-side rendering
without touching the LAN gateway.
EOF
  exit 1
fi
