# Basis Networking Domains

Basis stitches together five distinct network domains. Each has its own
address space, its own controller, and a different trust boundary.
Understanding them in isolation — and at the seams between them — is
the prerequisite for reasoning about any reachability, isolation, or
allocation question.

```
                         ┌──────────────────────────────────────────┐
                         │  D1  Underlay (management LAN)           │
                         │      operator-owned IPs, MTU, routing    │
                         └──────────────────────────────────────────┘
                                        ▲           ▲
                          BGP tcp/179   │           │  VXLAN udp/4789
                          gRPC tcp/7443 │           │  (outer header)
                                        ▼           ▼
   ┌────────────────────────────┐   ┌──────────────────────────────┐
   │  D2  Cell BGP fabric       │   │  D3  Per-tree overlay        │
   │  iBGP, holod, controller=  │   │  one VNI + bridge per tree   │
   │  RR, agents=speakers       │   │  per host that carries it    │
   │  carries D4+D5 prefixes    │   │  carries D4+D5 packets       │
   └────────────────────────────┘   └──────────────────────────────┘
                  ▲                                ▲
                  │  advertises                    │  encapsulates
                  │                                │
   ┌──────────────┴──────────────┐   ┌─────────────┴────────────────┐
   │  D4  In-tree address space  │   │  D5  External pool space     │
   │  bridge / VM / nested-VIP   │   │  LAN-routable VIPs + LB      │
   │  carved from tree CIDR      │   │  service blocks              │
   └─────────────────────────────┘   └──────────────────────────────┘
```

D1 carries D2 and D3. D2 advertises prefixes from D4 (tree CIDRs) and
D5 (external VIPs/blocks). D3 forwards packets for both. D4 and D5 are
disjoint allocation pools that a cluster picks between via
`externalIpPool` on the `BasisCluster` spec.

---

## D1 — Underlay (management LAN)

**What it is.** The physical or operator-managed L3 network that every
host sits on. Each host has one primary IPv4 on its uplink bridge.

**What it carries.**
- Outer headers of all VXLAN traffic (UDP/4789).
- All BGP sessions (TCP/179) between agent speakers and the controller
  reflector.
- Controller↔agent gRPC (TCP/7443), metrics, SSH, Ansible, image pulls.

**Allocation.** Operator-owned. Basis does not allocate underlay
addresses; it reads each host's primary IPv4 off the uplink bridge at
agent startup and reports it as the host's `vtep_address`. See
`crates/basis-agent/src/network/mod.rs` (probing) and
`hosts.vtep_address` in the controller DB.

**Trust boundary.** The LAN itself. Anything that can reach an agent's
LAN IP can attempt a TCP/179 connection; the controller's BGP ACL
(D2) is what restricts who can actually peer.

**MTU.** Every other domain inherits from the underlay MTU. Tree
overlay MTU is `uplink_mtu - 50` (VXLAN overhead). The agent applies
this to bridges, VXLAN devices, and bakes it into VM cloud-init so
guest NICs match.

---

## D2 — Cell BGP fabric (iBGP via holod)

**What it is.** A single-AS iBGP mesh with the controller acting as a
route reflector and every agent as a leaf speaker. The BGP engine is
`holod`, run as a separate systemd unit on every basis node;
basis-controller and basis-agent each push YANG-modeled config to
their local holod over gRPC and never speak BGP themselves. The
BGP daemon and the basis daemon have independent lifecycles — a
controller restart does not flap sessions.

**What it carries.** UPDATE messages only. Two route classes:

| Route                   | Source            | Originated by                          | Next-hop      |
|-------------------------|-------------------|----------------------------------------|---------------|
| Tree CIDR (`/20`-ish)   | D4 supernet carve | every host carrying any VM in the tree | host VTEP     |
| External VIP (`/32`)    | D5 named pool     | every host carrying any cluster in tree| host VTEP     |
| External LB block (`/N`)| D5 named pool     | every host carrying any cluster in tree| host VTEP     |

In-tree allocations (D4) are NOT advertised individually — the tree
CIDR aggregate covers them. Only external (D5) prefixes get per-cluster
advertisements, because they live outside the tree CIDR and the LAN
needs explicit routes to them. Filtering happens in
`server.rs::build_reconcile_command` via `tree_net.contains(...)`.

Address family is `ipv4-unicast` only. No EVPN — VXLAN BUM destinations
are managed out-of-band via D3's FDB programming, not via type-2/type-3
EVPN routes.

**Configuration.** `BasisControllerSpec.bgp` (config.rs):
- `asn` — cell-wide ASN, every speaker uses it (iBGP).
- `routerId` — controller's underlay IP.
- `holodEndpoint` — local holod gRPC.
- `instanceName` — logical name in holod's running config.

**Reconciliation.** Controller has two background loops
(`crates/basis-controller/src/bgp.rs`):
- A peer reconciler that diffs the set of healthy hosts (non-empty
  `vtep_address`) against the neighbor list pushed to holod.
- An ACL reconciler that owns the nftables table `basis_bgp` and
  rewrites the allowlist of source IPs permitted to open TCP/179.

Agents register exactly one neighbor (the reflector) and push the
union of their tree CIDRs and per-cluster external prefixes as
`networks[]`. Both sides dedupe before pushing — no UPDATE traffic
when nothing changed.

**Trust boundary.** The nftables ACL on the controller side restricts
TCP/179 to known host VTEP addresses. There is no MD5/TCP-AO; the
trust model is "the LAN is the perimeter" plus "only basis-registered
hosts have valid VTEP addresses on file."

**Known limit — thin trust model.** A compromised host on the LAN
that can spoof a registered VTEP address can open a session, hijack
an existing one, or inject prefixes (subject to whatever holod
validates). This is acceptable for the current target environment
(operator-controlled LAN, every host running basis) but is the first
thing that needs hardening for any deployment with a less-trusted
underlay. The cheapest meaningful step is GTSM (TTL=255 / accept-only
ttl 255), followed by MD5 or TCP-AO if holod's northbound exposes
them.

---

## D3 — Per-tree overlay (VXLAN)

**What it is.** A separate L2 broadcast domain per tree, implemented
as one Linux bridge + one VXLAN device per (host, tree) pair.

| Device           | Name             | Role                                      |
|------------------|------------------|-------------------------------------------|
| Linux bridge     | `brt<vni>`       | tree-scoped L2 segment + L3 gateway iface |
| VXLAN netdev     | `vxt<vni>`       | encap/decap, slaved to the bridge         |
| VM TAP           | `bas<hash(vmid)>`| guest's NIC, slaved to the bridge         |

Names derive deterministically from the VNI to fit `IFNAMSIZ`=15.

**Forwarding.**
- Intra-host VM↔VM in the same tree: bridge-local, no encap.
- Cross-host VM↔VM: bridge → VXLAN → outer UDP/4789 with `local =
  this host's VTEP`, dest learned from FDB, decapped on the peer host.
- VM → host services (DNS etc.): bridge gateway IP (D4 bridge range)
  is the L3 next-hop on the host kernel, normal forwarding from there.

**FDB programming.** VXLAN learning is **enabled** on the device, but
the controller's peer list is layered on top:
- Controller pushes the full `vtep_addresses` set in every
  `ReconcileHostCommand`. Agent rewrites the BUM FDB (dst-mac
  `00:00:00:00:00:00`) to exactly that set —
  `tree.rs::reconcile_fdb`. This is what makes BUM flooding work
  without multicast on the underlay.
- Learning fills in unicast entries from in-tree gARP (Cilium / kube-vip
  leader announcements), so a VIP failover propagates to peer FDBs
  without basis having to track per-cluster leader state.

**Spoof guard.** Learning would let a tenant VM forge BUM frames for
another tree's VNI and poison peer FDBs. The agent installs one global
`iptables -t filter -A FORWARD -p udp --dport 4789 -j DROP` rule
(`network/mod.rs::ensure_vxlan_spoof_guard`). Host-originated encap
goes through OUTPUT and is unaffected; only frames coming off a TAP
hit FORWARD and get dropped.

**Design constraint.** This is deliberately coarse: it assumes all
legitimate VXLAN traffic originates in the host network namespace
(OUTPUT). Anything that legitimately emits VXLAN from inside a netns
— host-networked workloads, sidecar overlays, nested CNIs that use
VXLAN — would be false-positive-dropped. Today nothing in basis does
that, so the trade is fine; revisit before allowing host-network pods
or any second overlay on the underlay.

**Per-tree NAT.** The agent installs a source-scoped
`iptables -t nat -A POSTROUTING -s <tree_cidr> -o <uplink> -j MASQUERADE`
rule per tree (`ensure_tree_masquerade`). Without it, VM traffic
egressing the uplink would source from a tree address the upstream
router can't reverse-route. Paired with a TCP MSS clamp on the FORWARD
chain so PMTUD-broken peers don't silently drop oversize segments
once VXLAN's 50-byte tax kicks in.

**MTU assumption.** Overlay MTU is computed once at agent startup
from the local uplink (`uplink_mtu - 50`). The system assumes the
underlay MTU is uniform across every host and that no intermediate
link is smaller. If a host comes up with a different uplink MTU, or
an underlay path silently clamps lower, large frames blackhole. The
MSS clamp catches TCP; UDP (including some control-plane traffic) is
on its own. Open work: validate MTU at agent registration and/or
actively probe between VTEPs before declaring a tree healthy.

**Trust boundary.** The VNI is the isolation primitive. Different
trees are different L2 segments; the spoof guard prevents a guest from
crossing them via crafted VXLAN.

---

## D4 — In-tree address space

**What it is.** RFC1918 address space carved hierarchically:

```
network.treeSupernet   (e.g. 10.0.0.0/8)
    └── per-tree CIDR  (network.treePrefix, default /20)
            ├── bridgeReserve   (default 32 IPs, bottom of CIDR)
            │       per-host bridge IPs / VM gateways
            ├── VM range        (the bulk)
            │       one IP per VM
            └── vipReserve      (default 32 IPs, top of CIDR)
                    nested cluster VIPs + LB blocks
```

Every layer is allocated by the controller inside SQL transactions
against `controller.db` so concurrent CreateCluster / CreateMachine
calls cannot collide.

**Tree allocation** (`db.rs::allocate_tree`).
- Picks the lowest free `vni` from `network.vniRange`
  (default 10000..=16_000_000, well below the 24-bit VXLAN ceiling).
- Picks the lowest free `/treePrefix` slice from `treeSupernet` that
  doesn't overlap any existing tree CIDR.
- Both are stable for the tree's lifetime; on tree teardown (last
  cluster gone) the VNI and CIDR are reclaimed.

**Bridge IP carving** (`db.rs::ensure_host_bridge_ip`).
- Every host carrying a tree gets exactly one IP from
  `bridgeReserve`, distinct from every other host's. This is the
  L3 gateway address VMs see; cross-host return traffic must hit
  the right host, so per-host uniqueness is load-bearing.
- Stored in `tree_host_bridges (tree_id, host_id, ip_address)`
  with a `UNIQUE(tree_id, ip_address)` constraint.

**VM IP carving** (`db.rs::allocate_vm_ip`).
- One IP per VM from the VM range, allocated inside the
  CreateMachine transaction alongside the `vms` row.
- Released on VM delete via `release_vm_ips`.

**Nested VIPs** (`vipReserve`). When a `BasisCluster.spec.externalIpPool`
is empty, the cluster's apiserver VIP and (optional) LoadBalancer
service block come from the top of the tree CIDR. These are reachable
only over D3 — they're inside the tree CIDR, so D2 only advertises the
aggregate, not the individual VIP.

**Trust boundary.** The tree itself. Allocations within a tree are
visible to anything in that tree (VMs, in-cluster Cilium, sibling
clusters under the same tree); they are invisible to other trees and
to the LAN.

---

## D5 — External pool space

**What it is.** LAN-routable address ranges that basis advertises into
the underlay via D2 so that LAN clients can reach individual cluster
endpoints without joining the overlay.

**Configuration.** `network.pools[]` (config.rs `Pool`). Each pool has
a unique name and a CIDR carved from the LAN's own address space.
Pools cannot overlap each other or the tree supernet. A cluster
references a pool by name in its `externalIpPool` field; empty name
falls back to D4's `vipReserve`.

**Allocation.** Two distinct allocators against the pool (`db.rs`):
- `allocate_pool_vip` — one `/32` per cluster apiserver. Claimed
  inside the cluster by kube-vip.
- `allocate_service_block` — power-of-two-aligned block (size from
  `CreateClusterRequest.external_service_ips` or the cell default).
  Cilium hands out individual IPs from the block to LoadBalancer
  Services. Alignment is required so the allocation can be
  represented as a single `/N` prefix in BGP.

**Per-host data-plane plumbing** (agent-side, `tree.rs`). For every
external prefix the controller tells this host to carry, the agent
installs:
- An `ip route replace <prefix> dev brt<vni>` override so LAN-arriving
  packets for the prefix forward onto the tree bridge instead of
  being treated as connected on the underlay.
- A proxy-ARP entry per individual host address in the prefix
  (`ip neigh replace proxy <addr> dev <uplink>`) so LAN ARP for any
  IP in the block is answered with this host's MAC. Expanded via
  `ipnet::Ipv4Net::hosts()` — for `/32` prefixes that's just the
  one address; for a `/28` it's the 14 usable hosts.

**Multi-host advertisement.** Every host carrying any cluster in the
tree advertises the same set of external prefixes and installs the
same proxy-ARP entries. There is no per-VIP owner tracking; LAN
clients ECMP across the hosts and the bridge's FDB (populated by
kube-vip's gARP) decides which TAP the packet lands on. This is also
why D3 keeps VXLAN learning on — a kube-vip failover updates peer
FDBs without basis having to react.

**Edge NICs are not used.** A previous iteration gave each VM a
second NIC on the LAN pool. That's gone (commit 4719938); the ingress
path is now strictly D5 advertise → D3 forward.

**Known limit — proxy-ARP scaling.** Per-host-address expansion is
fine for `/32` apiserver VIPs and small (`/28`) service blocks, but
a `/24` block × many clusters × many trees per host puts thousands
of proxy-ARP entries on a single uplink. Linux can carry it, but
`gc_thresh` tuning, reconcile cost, and ARP table churn become real
concerns. Above a few hundred entries per host the right move is
either eBPF-driven ARP suppression or a "summary proxy" that
answers for a whole prefix instead of expanding it.

**Known limit — ECMP × FDB race.** LAN ECMP can land a packet on
any host advertising the prefix, on the assumption that the host's
bridge FDB will steer it correctly (locally to a TAP, or via VXLAN
to whichever peer holds the leader). That assumption is violated
during three windows: (1) cold start before any gARP has been seen,
(2) the gap between a kube-vip leader change and the gARP propagating
to peer FDBs, (3) asymmetric flows where conntrack lives on a
different host than the destination MAC. Today the failure mode in
those windows is BUM flood (the bridge floods to every TAP and the
VXLAN device on miss), which usually delivers but wastes capacity
and can confuse conntrack. Open work: define explicit convergence
bounds for VIP failover and decide whether transient flood is the
acceptable failure or whether we want a controller-driven
unicast-FDB pin during the gap.

**Known limit — no ownership model.** "Every host advertises every
prefix; FDB decides where the packet lands" trades determinism for
simplicity. The honest answer to "which host owns this VIP right
now?" is "look at the FDB on whichever host the packet hit." That's
correct but operationally thin. Planned mitigation is observability,
not enforcement: surface current FDB state for tree VIPs through the
controller (probably as a `kubectl get` column or a metrics label)
so operators have a single place to ask the question.

**Trust boundary.** The LAN. Pool CIDRs are real LAN addresses, so
anything reachable on the LAN can hit them. Authorization for what
gets advertised is upstream — the controller decides which prefix
ends up in `cluster_vips` for each tree.

---

## How a packet traverses the domains

**LAN client → cluster apiserver (D5 path):**
1. LAN client ARPs for `<apiserver_vip>`. One of the hosts carrying
   the tree answers with its MAC (D5 proxy-ARP).
2. Packet arrives on that host's uplink, hits the
   `<apiserver_vip>/32 dev brt<vni>` route override (D5).
3. Bridge forwards onto the right TAP per FDB entry learned from
   kube-vip's gARP (D3).
4. If the kube-vip leader is on a different host, the bridge instead
   forwards out the VXLAN device, encapsulates with the leader host's
   VTEP as destination (D3 over D1), and the peer host decaps onto
   its bridge.

**VM → external internet (D4 → D1):**
1. VM sends with default gateway = host's bridge IP for its tree (D4).
2. Host kernel routes per its own routing table; if egress is the
   uplink, the per-tree MASQUERADE rule rewrites the source to the
   host's LAN IP (D3 NAT, D1 egress).
3. Reply lands on the host's LAN IP, conntrack reverses the NAT, the
   bridge delivers to the TAP.

**VM A on host 1 → VM B on host 2, same tree (D4 over D3 over D1):**
1. VM A → bridge `brt<vni>` on host 1 (D4).
2. Bridge → VXLAN device, FDB lookup yields host 2's VTEP (D3).
3. Outer UDP/4789 packet to host 2 over the underlay (D1).
4. Host 2 decaps, bridge delivers to VM B's TAP (D3 → D4).

---

## Domain ownership summary

| Domain | Address space    | Allocator         | Programmer (kernel state)        | Advertised in D2? |
|--------|------------------|-------------------|----------------------------------|-------------------|
| D1     | operator's LAN   | operator          | operator + agent (probes only)   | n/a               |
| D2     | n/a (control)    | controller        | controller + agent → holod       | n/a               |
| D3     | VNI + MAC space  | controller (VNI)  | agent (links, FDB, NAT, guard)   | n/a               |
| D4     | tree supernet    | controller        | agent (bridge IP only)           | tree CIDR only    |
| D5     | named LAN pools  | controller        | agent (route + proxy-ARP)        | per-prefix        |

Allocation always happens on the controller, inside SQL transactions
against `controller.db`. Kernel state always happens on the agent,
driven by `ReconcileHostCommand`. The agent never invents addresses;
the controller never touches netlink.

The load-bearing property of this split is that **allocation is
centralized, forwarding is fully distributed**. The controller is
authoritative for what should exist; agents are independently
responsible for how packets move. A controller restart doesn't flap
BGP sessions (holod owns those), doesn't break VXLAN forwarding
(agents already programmed it), and doesn't lose allocations (SQL
is durable). New CreateCluster / CreateMachine calls block during
the restart, but the data plane is untouched.

---

## Open work

The design is sound at the current scale (single-digit hosts,
single-digit trees per host, small service blocks). The three areas
that need work before pushing materially past that:

**Failure-mode catalog.** Document the behavior under: host dies
mid-flow, FDB stale after leader change, partial BGP convergence
(reflector reachable from some speakers but not others), ARP race
on first packet to a fresh VIP, controller down for longer than a
host's BGP keepalive (does the host re-advertise correctly when
the reflector comes back?). Most of these are "should be fine" —
the value is in turning that into "is fine, and here's the test."

**Scaling envelope.** Set explicit numbers for: max trees per host
(bridge + VXLAN device count, FDB size), max prefixes per pool
(BGP UPDATE size, proxy-ARP entry count per uplink), max
`vtep_addresses` per tree (FDB BUM entry count), max VMs per host.
Today these are implicit and bounded by "we haven't tried more."

**Convergence guarantees.** Put numbers on VIP failover ("X ms under
Y conditions"), tree creation latency end-to-end (CreateCluster
returns → first VM has working egress), BGP withdraw on host loss.
Without these, "did this regress?" is unanswerable.
