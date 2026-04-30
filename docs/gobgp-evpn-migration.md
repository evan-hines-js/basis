# GoBGP daemon + L3-only EVPN substrate

Status: design
Trigger: scheduled after the current Holo-based BGP work has e2e coverage. Not before.

## Goal

Replace basis's bespoke controller-pushed FDB / ARP / route plane with a routed-only BGP-EVPN dataplane, where every VM is a /32 host route and every cluster is a VRF.

Concretely: every per-VM IP and every cluster CIDR becomes a BGP Type-5 (IP prefix) route advertised by basis-controller and consumed by basis-agent. The `ReconcileHostCommand`'s FDB / cluster_vips fields, `elect_lan_vip_owner`, the per-cluster Linux bridge, proxy-ARP, GARP, and the LAN-VIP owner reconcile path all retire. There is no Type-2 (MAC+IP) advertisement; no bridge MAC learning; no ARP between VMs.

This is both a control-plane and a dataplane change. VXLAN encap stays for HW offload and tenant isolation via VNI; what changes is that VXLAN now carries L3 (routed) traffic between hosts rather than L2 frames, and there is no Linux bridge in front of it on the host.

## Why L3-only is correct for basis

basis is the hypervisor substrate for Lattice. Lattice runs Kubernetes via CAPI, exclusively. Nothing else runs on basis VMs, ever. That tightens the design space significantly:

- K8s control-plane traffic is all unicast TCP. kubelet → apiserver, etcd peer-to-peer, Cilium BGP, container-image pulls, scheduler / controller-manager → apiserver — none use broadcast or multicast.
- K8s pod traffic is handled by Cilium native routing (`routingMode=native`, `autoDirectNodeRoutes=true`) — already pure-L3 inside the cluster, with each node advertising its pod CIDR via BGP. The cluster overlay basis provides is the *underlay* from K8s's perspective.
- VIPs (apiserver VIP, LB pool IPs) work over BGP-anycast at least as well as over ARP, and substantially better at scale. kube-vip has a BGP mode; Cilium has BGP-mode LB advertisement.
- There is no K8s feature that requires VMs of the same cluster to be on a shared L2 segment. ARP between sibling VMs is never used by any K8s component.

Stretching L2 between VMs costs control-plane churn (Type-2 advertise/withdraw on every VM lifecycle event), bridge MAC-table pressure at scale, BUM replication for any broadcast traffic the workload happens to emit, and substantial code (FDB push, leader-elected proxy-ARP, GARP). Lattice never benefits from any of it.

L3-only deletes all of that and gains:

- Sub-second VIP failover via BGP route withdrawal vs. multi-second GARP-driven re-election.
- One mental model: routes. `ip route show vrf <cluster>` answers any "where does this packet go" question. No FDB lookup, no ARP cache, no proxy-ARP table.
- Linear control-plane growth at scale. Type-5 is one route per VM regardless of how many sibling VMs exist; Type-2 is one route per VM-VM pair on the same VNI in the worst case (mitigated by EVPN ARP suppression but not eliminated).
- Native composition with Cilium and kube-vip BGP modes — the K8s side already speaks BGP; basis stops being the L2 outlier.

## Decision

**Daemon: GoBGP.** Replaces Holo on every basis host. Apache-2.0, EVPN-mature, used in production at carrier scale, gRPC northbound that maps cleanly onto basis's reconciler pattern.

**Encap: VXLAN.** Same wire format, same NIC HW offload, no MTU shift.

**Control plane: BGP-EVPN, RFC 7432 + 8365, Type-5 only.** Per-cluster route-target carries tenant identity. Per-host route-distinguisher disambiguates routes to the same /32 from sibling hosts (multi-homing, ECMP). No Type-2, no Type-3, no Type-4, no Type-6.

**Dataplane shape:** each cluster is a Linux VRF on every host that carries the cluster. The VRF holds /32 host routes for every VM and /N routes for every cluster VIP. Inter-VM traffic on the same host routes through the host kernel via the VRF; cross-host traffic VXLAN-encaps to the destination host's VTEP, decaps, and routes locally. There is no Linux bridge per cluster.

**LAN-side reachability:** eBGP-upstream peering with the customer's edge router is the production target. The L2 stub (proxy-ARP, GARP, owner election) survives as a per-pool *opt-in fallback* for environments without a BGP-speaking router (homelab, dev, edge sites). Same binary works in both topologies; per-pool config selects which path applies. See Stage 4.

**kube-vip mode for apiserver VIPs:** BGP, not ARP. Lattice's CP-node bootstrap config selects kube-vip BGP mode for basis-provider clusters. The apiserver VIP becomes a /32 advertised by whichever CP node holds the lease, peering with the host's local gobgpd. Equivalently, Cilium's apiserver-handling can take over.

## Alternatives rejected

- **L2-over-L3 (EVPN Type-2 + Type-5).** The migration shape implied for general-purpose multi-tenant fabrics. basis doesn't need it: nothing inside a Lattice cluster wants L2 between VMs. Carrying Type-2 buys generality basis will never use, at the cost of ~half the route-update rate, MAC-table pressure on every host, and the per-cluster bridge dataplane.
- **Stay on Holo, add EVPN upstream.** Holo's BGP today doesn't implement any EVPN RFCs. Landing them upstream is a multi-year project under a small-team OSS roadmap.
- **FRR.** Same EVPN coverage as GoBGP. GPL-2 vs Apache-2.0 (commercial-distribution risk). Driver model is `vtysh` + config-file reloads — controller integration is shellouts or socket-frame parsing, fragile compared to typed gRPC.
- **Pure L3 with no overlay (Calico-style host routing).** Drops VXLAN. Requires every host's kernel routing table to scale linearly with cluster count. Tenant isolation needs per-tenant VRFs *and* per-tenant route-leak boundaries enforced at every hop. AWS does this with custom silicon (Nitro); software implementations top out around 100 hosts before kernel routing-table churn becomes the bottleneck. VXLAN dataplane stays for HW offload; the VNI gives tenant isolation cheaply.

## What changes, what stays

**Changes (substantial):**

- `crates/basis-common/src/holo.rs` → `crates/basis-common/src/gobgp.rs`. gRPC northbound with typed `AddPeer` / `AddPath` / `ListPeer` / `ListPath` calls; reconcilers diff against current state and issue Add/Delete RPCs.
- `crates/basis-controller/src/bgp.rs` — RR config, peer reconciler, ACL reconciler stay (with GoBGP). New module `crates/basis-controller/src/evpn.rs` originates Type-5 per VM placement and per cluster CIDR via GoBGP's local-RIB API. Per-cluster RT + per-host RD applied at origination.
- `crates/basis-agent/src/bgp.rs` — host speaker swaps to GoBGP. New module `crates/basis-agent/src/evpn.rs` subscribes to the agent's local GoBGP RIB and programs `ip route add <vm-ip>/32 dev <cluster-vrf>` for every Type-5 the agent imports. No bridge, no FDB, no neigh table programming.
- `crates/basis-agent/src/network/cluster.rs` — `brc<vni>` Linux bridge replaced by a routing-only construct. The VXLAN device (`vxlan<vni>`) is enslaved directly to the per-tree VRF; there is no `bridge fdb` table because there is no bridge. ARP suppression is moot because no ARP happens.
- `crates/basis-agent/src/network/cluster.rs` — `add_proxy_arp` / `del_proxy_arp` / `send_garp` / `expand_lan_vips` / proxy-ARP entry tracking *gated* on pool's `upstream_advertised` flag. Code stays for fallback; runs only when the pool isn't BGP-announced upstream.
- `crates/basis-controller/src/server.rs`'s `build_reconcile_command` collapses substantially.
- `crates/basis-controller/src/db.rs` — `cluster_lan_vip_owner` table stays for L2-stub-fallback pools; election runs only when the cluster's pool is `upstream_advertised=false`.
- `crates/basis-proto/proto/basis.proto`:
  - `ReconcileHostCommand.clusters[].fdb` deleted.
  - `ReconcileHostCommand.clusters[].cluster_vips[]` deleted.
  - `ReconcileHostCommand.clusters[].internal_cluster_vips[]` deleted.
  - `ReconcileHostCommand.clusters[].gateway_ip` deleted (no bridge → no gateway IP in the L2 sense; the cluster's first usable address is just another VM IP).
  - `ClusterState` reduces to: cluster_id, vni, cidr, vrf-name, RT, RD-prefix.

**Lattice-side change (small, but required):**

- CP-node bootstrap kube-vip config selects BGP mode when provider=basis. kube-vip peers with the host's local gobgpd at `127.0.0.1:50051`. The apiserver VIP becomes a /32 originated by the lease holder.

**Stays:**

- VXLAN UDP/4789 dataplane.
- Per-tree VRF isolation. `bvrf-*` enslaves the VXLAN device directly instead of going through a bridge.
- The Cilium-on-nodes peering against the cell RR (target switches from Holo to GoBGP, peering shape unchanged).
- `tcp_l3mdev_accept` / VRF-bound socket plumbing.
- Pool allocation, cluster CRUD lifecycle, machine placement, image distribution.
- nftables source-IP ACL on tcp/179.

## Migration stages

Each stage independently shippable and revertable.

### Stage 1 — Holo → GoBGP daemon swap, no behavior change

Swap Holo for GoBGP everywhere. Same iBGP cell topology, same IPv4 unicast AFI, same controller-pushed FDB / cluster_vips path. The `bgp_running_config` YANG-JSON render is replaced by typed Peer / Path RPCs to GoBGP.

Validation: same homelab e2e tests pass, Cilium-on-nodes peering works, cluster_vip /32s reflect through the cell exactly as today.

### Stage 2 — EVPN Type-5 dual-write

Enable l2vpn-evpn AFI/SAFI on every speaker. Controller starts originating Type-5 per VM (host /32) and per cluster CIDR alongside the existing controller-pushed FDB and cluster_vips reconcile fields. Agent consumes Type-5 routes via local-RIB subscription and installs `ip route` entries additively. Bridge FDB still present and authoritative for VM-to-VM L2 traffic on the cluster bridge.

Validation: agent's effective routing table matches across both inputs (existing push + Type-5). Diff logs at zero in steady state.

### Stage 3 — Type-5 authoritative; bridge dataplane removed

Cut over: `ReconcileHostCommand` stops carrying FDB / cluster_vips / internal_cluster_vips. The agent stops creating the per-cluster Linux bridge — the VXLAN device is enslaved to the cluster's VRF directly. VM tap interfaces are addressed (each VM gets a /32 on the host's VRF interface, not on a bridge port). Inter-VM traffic on the same host routes via the VRF's routing table.

Validation: rolling restart a VM, confirm Type-5 advertise-then-withdraw cycle produces correct host routing on every other host. Pull a host's network cable, observe other hosts' Type-5 import for that host's VMs withdrawn within BGP hold-time.

### Stage 4 — Make the L2 stub opt-in (per-pool fallback)

The cell-internal L3-only design is independent of how LAN clients reach the cell. proxy-ARP, GARP, `elect_lan_vip_owner`, the `cluster_lan_vip_owner` table all stay in the codebase but become **conditional** — gated on whether the pool a cluster_vip belongs to is BGP-announced upstream.

Per-pool selector:

- **`upstream_advertised: true`** (the production target — eBGP peering with the customer router exists, RT is exported, customer router has the route): basis-agent skips the L2 stub entirely for VIPs in this pool. The customer router routes LAN→VIP via the BGP-learned next-hop.
- **`upstream_advertised: false`** (homelab on Comcast / dev environment with a non-BGP-capable router): basis-agent runs the existing L2 stub — proxy-ARP entries on vmbr0, GARP burst on owner change, single-elected `cluster_lan_vip_owner`. Identical to today's behavior for these VIPs.

Same binary supports both. The decision is config-driven on the pool, not a compile-time toggle.

Validation:
- Cell with `BgpConfig.upstream` configured + a pool where every VIP advertises upstream: zero proxy-ARP entries on any basis host, LAN clients reach cluster VIPs via the customer router's BGP-learned routes.
- Cell with no `BgpConfig.upstream` + a pool with `upstream_advertised=false`: proxy-ARP entries present, GARP-driven owner election picks a single host per VIP, LAN clients reach VIPs via that host's vmbr0.
- Mixed cell with both pool types: proxy-ARP only for the L2-stub pool's VIPs.

### Stage 5 — Lattice-side kube-vip BGP mode + apiserver-via-BGP

Lattice's CP-node bootstrap config switches kube-vip to BGP mode for basis-provider clusters. Apiserver VIPs become /32s advertised by whichever CP node holds the kube-vip lease, peering with the host's local gobgpd. Failover becomes BGP route withdrawal (sub-second).

Validation: kill the kube-vip leader CP node, observe the next leader's gobgpd start advertising the apiserver /32 within one BGP keepalive interval. Existing apiserver clients (kubelet, controller-manager) re-establish connections within 5–10 seconds.

## What this enables

After all stages:

- **Sub-second VIP failover** via BGP withdrawal instead of GARP-driven re-election.
- **Multi-homed VIPs** for free — multiple CP nodes can advertise the same apiserver /32 with ECMP across them; no leader election required for read-only paths.
- **Linear control-plane growth at scale.** At 1000-host cells, the route-update rate is bounded by VM lifecycle events; no Type-2 churn, no MAC-table thrash.
- **One mental model.** `ip route show vrf <cluster>` answers everything. No FDB tables, no ARP caches, no proxy-ARP entries on any host.
- **Smaller code surface in basis.** Substantial deletion across `network/cluster.rs`, `controller/db.rs`, `proto/basis.proto`, the controller's reconcile-build path.

## Risks

- **GoBGP scale on a single cell controller.** GoBGP holds the cell's full RIB in memory. At 10k VMs × ~2 routes/VM ≈ 20k Type-5 routes. Well within GoBGP's tested envelope (NTT runs millions of routes), but worth a load test before stage 3 cutover.
- **kube-vip BGP mode operational lift on the Lattice side.** kube-vip BGP config has knobs (peer ASN, peer address, hold-time) that today's lattice bootstrap doesn't surface. Stage 5 needs lattice-rust changes to plumb the local gobgpd address into kube-vip's static-pod config.
- **Inter-VM same-host traffic now hits the host kernel routing path** instead of bridge L2 forwarding. Per-packet CPU is slightly higher than bridge forwarding. At K8s node densities (~10–50 nodes per host) this is well under noise floor; quantify before declaring it a non-issue.
- **No ARP fallback for misconfigured VMs.** If a VM somehow ARPs for a sibling VM's IP, the host won't answer (proxy-ARP gone, no bridge to do MAC learning). The VM's traffic goes to the default gateway, which routes correctly. Misconfig is loud, not silent — fine.
- **License audit.** Apache-2.0 GoBGP plus its test-vector files. One-pass check before stage 1 starts.

## Out of scope

- IPv6.
- Multi-cell topology.
- BGP-LU / SR-MPLS / SRv6.
- Workloads on basis other than CAPI-provisioned K8s. If a future basis use case wants L2 between VMs, that's a new design — not this one.

## When to start

After:
1. The Holo-based BGP-on-basis work (current session) has e2e coverage and runs the full e2e suite green.
2. There's a basis-provider e2e test specifically exercising the cluster_vip advertisement → Cilium DSR / SNAT path so we have a baseline to compare Type-5-driven behavior against.
3. kube-vip's BGP mode has been validated standalone (a lattice cluster brought up with kube-vip BGP-mode against a test gobgpd) so stage 5 isn't gated on simultaneously debugging kube-vip and basis.

Not before. The current Holo path is the known-good baseline; the migration converts it into the L3-only target without losing the ability to roll back.
