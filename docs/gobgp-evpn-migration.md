# BGP daemon swap + EVPN control plane

Status: design
Trigger: scheduled after the current Holo-based BGP work has e2e coverage. Not before.

## Goal

Replace basis's bespoke controller-pushed FDB / ARP / route plane with industry-standard BGP-EVPN over the existing VXLAN dataplane.

Concretely: every per-VM MAC+IP and every cluster CIDR becomes a BGP route advertised by basis-controller and consumed by basis-agent. The `ReconcileHostCommand`'s `clusters[].fdb` lists, the `cluster_vips` push, and `elect_lan_vip_owner` all retire. Type-2 (MAC+IP), Type-5 (IP prefix), and ES/ESI route advertisements replace them.

This is a control-plane change. The dataplane (VXLAN UDP/4789, per-tree VRF, brc<vni> bridges, bvrf-* VRFs) is unchanged.

## Decision

**Daemon: GoBGP.** Replaces Holo on every basis host (controller and agents) and as the BGP peer Cilium-on-nodes targets.

**Encap: VXLAN.** Keep what's already deployed. EVPN's NLRI carries the encap selection per route; VXLAN is one of the standard options.

**Control plane: BGP-EVPN, RFC 7432 + 8365.** Type-2 for MAC+IP, Type-5 for IP-prefix routes, route-targets per cluster for tenant separation, route-distinguishers per host.

## Alternatives rejected

- **Stay on Holo, add EVPN upstream.** Holo's BGP today is RFC 4271 + multiprotocol + communities. EVPN is ~8 RFCs of additional complexity sitting at the dataplane / control plane boundary. Holo is a young, small-team project; landing and stabilising EVPN upstream would put basis on the bleeding edge of a feature the maintainers haven't prioritised. Possible long-term but not credible inside 12 months.
- **FRR.** Same EVPN feature set as GoBGP. GPL-2 vs GoBGP's Apache-2.0 (matters for any future proprietary distribution). Driver model is `vtysh` + config-file reloads, not a typed gRPC API — driving it from a controller means shellouts or socket-framing parsing, fragile compared to GoBGP's gRPC northbound. The wins of FRR over GoBGP (broader IGP coverage, longer track record on enterprise gear) don't apply here.
- **Native L3 / Calico-style routed dataplane.** Drops the overlay entirely; every pod CIDR is a BGP /N advertised cell-wide. Loses per-cluster L2 isolation, requires globally-routable IPs for VMs, breaks the per-tree VRF model basis already commits to for tenant separation.
- **Stay on the controller-pushed FDB system.** Works at current scale. Doesn't compose: every new feature (multi-homing, sub-second failover, anycast VIP, multi-rack L3 fabric) requires bespoke RPC additions instead of falling out of standard EVPN behaviour.

## What changes, what stays

**Changes:**
- `crates/basis-common/src/holo.rs` → replaced by `crates/basis-common/src/gobgp.rs`. gRPC northbound shape is similar (Replace/Modify/Delete operations); the YANG-JSON payload becomes GoBGP's typed Path / Peer messages.
- `crates/basis-controller/src/bgp.rs` — RR config, peer reconciler, ACL reconciler stay, but talk to GoBGP. New module: `crates/basis-controller/src/evpn.rs` originating Type-2 (one per VM placement) and Type-5 (one per cluster CIDR) routes via GoBGP's local-RIB API.
- `crates/basis-agent/src/bgp.rs` — host speaker swaps daemons. New module: `crates/basis-agent/src/evpn.rs` subscribes to the agent's local GoBGP RIB and programs:
  - VXLAN FDB entries from Type-2 (`bridge fdb add <mac> dev vxlan<vni> dst <vtep-ip>`)
  - Bridge ARP entries from Type-2 (suppresses cluster-overlay ARP flooding via EVPN ARP-suppression)
  - Routes for cluster CIDRs from Type-5
- `crates/basis-proto/proto/basis.proto`:
  - `ReconcileHostCommand.clusters[].fdb` deleted (EVPN owns it).
  - `ReconcileHostCommand.clusters[].cluster_vips[]` deleted (Type-2 owns it).
  - `ReconcileHostCommand.clusters[].internal_cluster_vips[]` deleted.
  - `ClusterState` keeps cidr / gateway_ip / vni / VRF metadata.
- `crates/basis-controller/src/server.rs`'s `build_reconcile_command` collapses substantially.
- `elect_lan_vip_owner`, `cluster_lan_vip_owner` table, sticky-owner reconcile path → all deleted. EVPN multi-homing replaces them.
- `add_proxy_arp` / `del_proxy_arp` / `send_garp` paths become conditional on whether the deployment runs an eBGP-upstream peering. With one configured, they're unused; without one, they remain as the L2 stub for legacy LAN topologies (homelab being one).

**Stays:**
- VXLAN UDP/4789 dataplane.
- Per-tree VRF isolation, `bvrf-*` enslavement of `brc<vni>` bridges.
- The Cilium-on-nodes peering against the cell RR (target switches from Holo to GoBGP, peering shape unchanged).
- `tcp_l3mdev_accept` / VRF-bound socket plumbing.
- Pool allocation, cluster CRUD lifecycle, machine placement, image distribution — all unchanged.
- nftables source-IP ACL on tcp/179 — same shape, points at GoBGP's listener.

## Migration stages

Each stage is independently shippable and revertable.

### Stage 1 — daemon swap, no behavior change

Swap Holo for GoBGP everywhere. Configure the same iBGP cell topology: controller is RR, agents and k8s nodes are clients, single cell ASN, IPv4 unicast only. Cluster_vips still flow via the existing controller-pushed reconcile RPC (`ReconcileHostCommand` keeps its FDB / cluster_vips fields). The `bgp_running_config` JSON-YANG render swaps for GoBGP-typed Peer / Path messages.

Validation: same homelab e2e tests pass. Cilium-on-nodes peering works. cluster_vip /32s reflect through the cell exactly as today.

### Stage 2 — EVPN AFI/SAFI on the RR; dual-write

Enable l2vpn-evpn AFI/SAFI on every speaker. Controller starts originating Type-2 per VM and Type-5 per cluster CIDR alongside the existing controller-pushed FDB / cluster_vips push. Agent consumes EVPN routes via local-RIB subscription but treats them as additive to the existing FDB; conflicts log a warning rather than diverge.

Validation: agent's effective FDB matches across both inputs (existing push + EVPN). Diff logs at zero in steady state.

### Stage 3 — EVPN authoritative

Cut over: the `ReconcileHostCommand` stops carrying FDB / cluster_vips / internal_cluster_vips. The agent's existing FDB-from-push code is removed. EVPN is the only source.

Validation: rolling restart a VM, confirm Type-2 advertise-then-withdraw cycle produces correct host FDB on every other host. Run a network partition between controller and one host, confirm that host's GoBGP holds the routes through the partition (BGP graceful restart) where the existing system would have re-pushed on reconnect.

### Stage 4 — EVPN multi-homing replaces VIP owner election

`elect_lan_vip_owner` deleted. Multiple agents advertise the same Type-2 (VIP MAC+IP) with EVPN ES/ESI metadata; the LAN-side router (or the L2-stub) sees ECMP. Sub-second failover via BGP session loss instead of multi-second GARP-driven re-election.

Validation: pull a host's network cable, observe VIP traffic shifting to a sibling within 1–2 BGP hold-time intervals (sub-second with hold-down tuned, default ~90s).

## Risks

- **GoBGP scale on a single cell controller.** GoBGP holds the cell's full RIB in memory. At 10k VMs × ~3 routes/VM (Type-2 MAC, Type-2 MAC+IP, Type-5 prefix membership) ≈ 30k routes. Well within GoBGP's tested envelope (NTT runs millions of routes), but worth a load test before stage 3 cutover.
- **Holo → GoBGP gRPC API shape.** Both expose typed gRPC, but Holo speaks YANG-JSON (Replace operation against a path tree) and GoBGP speaks typed Path messages directly. The driver code in `crates/basis-common` is small (under 200 lines today) but it's the only abstraction; rewriting it correctly is real work.
- **EVPN ARP-suppression interaction with basis's existing proxy-ARP.** During stage 2 dual-write, both systems may try to populate the bridge's permanent neigh entries. Need explicit ownership: EVPN owns brc<vni> arp-suppression; proxy-ARP owns vmbr0 LAN-stub. Conflict only arises if a cluster_vip matches an underlay address, which the allocator already prevents.
- **License / commercial-distribution audit.** Apache-2.0 is permissive but the GoBGP repo includes some test-vector files under different licenses. Worth a one-pass check before stage 1 starts.

## Out of scope

- IPv6.
- Multi-cell topology (cell-of-cells).
- BGP-LU / SR-MPLS / SRv6 — different overlay technologies, not on the path.
- Replacing the Cilium-on-nodes BGP integration with a different model. The cell's daemon swap is transparent to Cilium; both Holo and GoBGP speak standard BGP and Cilium peers as a normal RR client either way.

## When to start

After:
1. The Holo-based BGP-on-basis work (this session) has e2e coverage and has run a full e2e suite green.
2. There's a basis-provider e2e test specifically exercising the cluster_vip advertisement → Cilium DSR / SNAT path so we have a baseline to compare EVPN-driven behaviour against.

Not before. Pre-stabilising the existing path so the migration has a known-good baseline is the highest-leverage thing right now.
