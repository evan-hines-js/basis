# K8s-only simplifications

basis is a CAPI-only substrate: every VM it provisions runs nothing but
a kubelet, containerd, and what kubeadm puts on the box. Several
parts of the codebase still carry generic-VM ergonomics that don't
apply to that workload. This doc enumerates them and proposes a
concrete cut for each.

The throughline is that K8s already owns the abstractions a
generic-VM substrate has to provide (identity, draining, scheduling,
storage, networking). Anywhere basis duplicates one of those
responsibilities, we're paying for surface we don't use.

---

## 1. Graceful shutdown timeout

**Current.** `delete_vm()` in `crates/basis-agent/src/vm.rs:268`
runs `systemctl stop` against the transient unit. Cloud-hypervisor
catches SIGTERM and ACPI-powers-off the guest; systemd's default
`TimeoutStopSec=90s` gives the guest up to 90 seconds to drain
before SIGKILL. Then `udevadm settle --timeout=5` waits for device
events.

**Why it's generic-VM.** Generic guests need that 90s to flush
buffers, close client connections, write pid files. A kubelet has
none of that — `kubectl drain` already moved pods off the node
before basis is asked to delete it. Anything left running has no
state worth preserving.

**Target.** Kill the cloud-hypervisor process directly (SIGKILL)
and skip the ACPI path. Drop `TimeoutStopSec` to a small value
(2–5s) for the case where systemd is in the loop.

**Implementation.**
- Override `TimeoutStopSec=2s` in the transient unit properties
  passed to `systemd-run` in `start_vm()`.
- Document in `vm.rs` that fast-kill is correct because drain is
  upstream's responsibility.
- Keep `udevadm settle` — it's about host device cleanup, not
  guest semantics.

**Risk.** Low. CAPI's machine-deletion contract already assumes
the node has been drained.

---

## 2. Cloud-init passthrough → typed kubeadm bootstrap

**Current.** `bytes bootstrap_data` flows opaquely from CAPI
through `BasisMachine` → `basis-capi-provider` →
`basis-client::MachineRequest.bootstrap_data` →
`CreateMachineRequest.bootstrap_data` (`crates/basis-proto/proto/basis.proto:153`)
→ `image::create_cloud_init_iso(userdata=&[u8])`
(`crates/basis-agent/src/image.rs:465`). The bytes are written
verbatim to cloud-init's `user-data` file.

**Why it's generic-VM.** Cloud-init is a kitchen-sink config
format — users, packages, write_files, runcmd, ssh keys, …
For a kubeadm join, the only inputs that matter are: the join
command (control-plane endpoint + token + CA hash) and the
kubelet config. Threading those through cloud-init means we ship
the full cloud-init package in the image, parse YAML at boot, and
pay first-boot latency for a single `kubeadm join` invocation.

**Target.** A typed `KubeadmBootstrap` proto message replacing
`bytes bootstrap_data`:

```proto
message KubeadmBootstrap {
  string join_command = 1;       // "kubeadm join 10.0.0.5:6443 --token …"
  bytes  kubelet_config = 2;     // /var/lib/kubelet/config.yaml
  bytes  ca_cert_pem = 3;        // /etc/kubernetes/pki/ca.crt
  optional bytes containerd_config = 4;
}
```

Agent writes those files directly into the cloud-init ISO (or
a small `basis-firstboot.service` that runs once and self-disables),
then execs `kubeadm join`. No cloud-init package required.

**Implementation.**
- Add `KubeadmBootstrap` to `basis.proto`. Replace the
  `bootstrap_data` field on `CreateMachineRequest` (pre-release,
  no compat shim).
- Capi-provider builds the message from the CAPI `Machine`'s
  bootstrap secret — parse the `cloud-config` once on the
  controller side, extract the join command and configs.
- Agent renders an ISO containing only those typed files plus
  the netplan `network-config` (still useful as a stable network
  bootstrap format).
- Drop cloud-init from the image; add a one-shot
  `basis-firstboot.service` that runs `kubeadm join` once
  on first boot.

**Risk.** Medium. Parsing CAPI's bootstrap secret on the provider
side is the only fragile bit — it's currently opaque, but the
KubeadmConfig CRD is the only producer we care about, so we can
key off its known shape.

---

## 3. Cloud-init network-config richness

**Current.** `image::network_config()`
(`crates/basis-agent/src/image.rs:514`) renders YAML netplan v2
threading `mac`, `ip_address`, `gateway`, `prefix_len`,
`dns_servers`, `mtu`. `meta-data` carries `instance_id` and
`local-hostname`.

**Why it's generic-VM.** Hostname/FQDN/search-domains matter
for users SSHing in, AD joins, DNS resolution. None of those
apply to a kubelet — Kubernetes uses node names from the
kubeadm join, not the OS hostname. DNS is overlay-internal:
CoreDNS handles in-cluster names, the host needs only the
controller VIP and gateway.

**Target.** Drop `local-hostname` from meta-data (or set it to
the vm_id, not user-facing). Drop search domains from
network-config. Keep MAC, IP/prefix, gateway, MTU — those are
load-bearing for L2 reachability before kubeadm runs.

**Implementation.** One-line cuts in
`image.rs:create_cloud_init_iso()` and `network_config()`. Verify
kubelet picks up its node name from kubeadm-config, not
`hostname -f`.

**Risk.** Low. Trivial to revert if a kubeadm path turns out to
key off hostname.

---

## 4. Per-VM MAC pinning

**Current.** `primary_mac()` in `crates/basis-agent/src/vm.rs:567`
generates a deterministic MAC `52:54:00:<hash(vm_id)>`. Netplan
binds the IP config to the MAC (quoted to avoid YAML 1.1
sexagesimal reinterpretation, `image.rs:529`).

**Why it's generic-VM.** Stable MACs matter for DHCP reservations,
licensing dongles, NAS exports keyed on MAC. None apply here:
the VM has a static IP from the controller's allocator, no DHCP,
no licensing.

**Target.** Random MAC per VM creation, no determinism needed.
The vm_id is already the stable identity at the basis layer; the
MAC is just a wire-format artifact.

**Implementation.**
- `primary_mac()` becomes `random_mac()`: `52:54:00:<random 3 bytes>`.
- Persist the chosen MAC in the VM row at create time so reboots
  and migrations reuse the same MAC for the lifetime of the VM.
- Netplan template still binds by MAC — just no longer derived
  from vm_id.

**Risk.** Low. Persisting per-VM MAC is a tiny schema add; the
deterministic-from-vm_id property had no consumer.

---

## 5. `elect_lan_vip_owner` / proxy-ARP / GARP fallback

**Current.** `db::elect_lan_vip_owner()`
(`crates/basis-controller/src/db.rs:952`) picks one host to be
sticky LAN VIP owner; that host proxy-ARPs / GARPs the VIP onto
the LAN. Used in `classify_cluster_vips()`
(`server.rs:120`) when the cluster's external pool is `Lan`-scoped.

**Why it's generic-VM (and why it's still live for us).** This
is a fallback for LANs that don't run BGP — a small homelab
switch can't peer with basis-controller, so we need an L2
trick to make the VIP reachable. With a CAPI-only commitment
plus the new GoBGP design, the supported topology is "the LAN
has a BGP-speaking router" — basis-controller (or basis-agent
with `bgp_router_neighbors`) peers with the ToR / OPNsense /
whatever, and the VIP is announced over BGP like any other
service IP.

**Target.** Remove the L2 fallback and require BGP for LAN-scoped
external pools. Operators on non-BGP LANs use a `Tree`-scoped
pool instead (which already works without LAN-side BGP because
the VIP lives inside the tree-VRF overlay).

**Implementation.**
- Drop `elect_lan_vip_owner`, `cluster_lan_vip_owner` table, and
  the proxy-ARP/GARP plumbing on the agent side.
- `Lan`-scoped pools without configured BGP neighbors become a
  validation error at controller startup ("Lan-scoped external
  pool requires bgp.lan_neighbors").
- Update homelab ansible to configure OPNsense as a BGP neighbor
  of basis-controller, removing the L2-trick deployment shape.

**Risk.** Medium — this is a deployment-shape change, not just
code. Easy in our homelab; document the requirement clearly for
future operators.

---

## 6. Data-disk surface

**Current.** `MachineRequest.extra_disk_gibs: Vec<u32>`
(`crates/basis-client/src/lib.rs:142`,
`basis.proto:197 ExtraDisk`) lets a CAPI machine request an
arbitrary list of additional disks by size.

**Why it's generic-VM.** The premise is "users want N disks of
varying sizes, mounted wherever." Kubelet workloads don't —
they get storage via PVCs, which are backed by Rook/Ceph PVs
on storage hosts. The only disk shapes basis actually needs:
- root disk (OS image)
- containerd scratch (large, ephemeral, on every node)
- raw block devices for Rook (storage-host nodes only,
  consumed whole, not mounted)

**Target.** Replace the `Vec<u32>` with explicit named roles:

```proto
message MachineRequest {
  // …
  uint32 root_disk_gib = 8;
  uint32 containerd_scratch_gib = 9;
  // For storage-host nodes only. Each entry is a raw block device
  // exposed to the guest; Rook claims them whole.
  repeated uint32 rook_raw_disk_gib = 10;
}
```

CAPI provider sets `rook_raw_disk_gib` only for machines whose
KubeadmConfig role-labels them as storage hosts. Non-storage
machines never get extra disks.

**Implementation.**
- Schema change on the proto + agent disk-creation code.
- BasisMachine spec gets a `role: Worker | Storage` enum;
  provider keys disk shape off it.
- Drop `ExtraDisk` and `extra_disk_gibs`.

**Risk.** Low. Mechanical refactor; the surface narrows.

---

## 7. CPU overcommit ratio

**Current.** `BasisControllerSpec.cpu_overcommit_ratio: f32`
(`crates/basis-controller/src/config.rs:67`) defaults to 4.0.
Scheduler treats `host.total_cpu * 4.0` as available
(`scheduler.rs:163`); DB enforces capacity at insert
(`db.rs:1269`); exported as Prometheus gauge.

**Why it's generic-VM.** Overcommit is correct for guest workloads
that are bursty or idle (interactive desktops, dev VMs, batch
jobs with low duty cycle). Kubelets pack pods to CPU *requests*;
the scheduler assumes those requests are honored. With basis
overcommit at 4.0, four kubelets share one physical core's worth
of capacity but each tells K8s it has 100% of its allocation —
under load the pods see CPU starvation that K8s' own scheduler
can't reason about.

**Target.** Hard-pin overcommit to 1.0 and remove the knob. The
CPU shape advertised to K8s matches reality.

**Implementation.**
- Drop `cpu_overcommit_ratio` from `BasisControllerSpec` and
  `Db::open` signature.
- `Available::from()` uses `host.total_cpu` directly.
- Drop the Prometheus gauge.
- One-line ansible group_vars cleanup.

**Risk.** Low. In a homelab the ratio was buying us "more VMs
fit on one host"; the cost was K8s scheduling lying to itself.
At our cluster sizes the right answer is fewer, larger VMs, not
overcommitted small ones.

---

## 8. `apiserver_visibility` enum

**Current.** `APISERVER_PUBLIC = 0` allocates the apiserver VIP
from the external pool; `APISERVER_PRIVATE = 1` uses the cluster
CIDR's last address. Branched in `classify_cluster_vips()`
(`server.rs:120`) and the BGP advertising gate (`bgp.rs:291`).

**Why it's generic-VM-ish.** "Private" only makes sense if there's
a meaningful "outside" the cluster CIDR that should be denied
reach. For a CAPI substrate where the operator is the cluster's
sole consumer and external clients reach LB Services via VIPs
anyway, the apiserver always wants to be reachable from the
operator's network — i.e. external.

**Target.** Drop the enum. Apiserver always allocates from the
external pool; if the operator wants apiserver reach restricted,
they put it on a `Tree`-scoped pool that's only reachable from
inside the basis fabric (same mechanism that already exists for
internal Services).

**Implementation.**
- Drop `APISERVER_PUBLIC`/`APISERVER_PRIVATE` from `basis.proto`.
- `BasisCluster.spec.apiserverVisibility` removed; controller
  always allocates from `external_ip_pool`.
- BGP advertising gate becomes "advertise iff pool scope is
  external", which is already the rule for LB Services.

**Risk.** Low. Behavior collapses to one path; reachability is
controlled by the pool's scope, which is the right axis.

---

## 9. NodePort (never built — keep it that way)

There is no NodePort code path in basis or its CAPI provider.
This entry exists to be explicit: external Service reachability
is always via VIP allocated from an external pool and advertised
over BGP. NodePort would mean clients hit `<node-ip>:<random-high-port>`
which gives us:
- ephemeral node IPs (nodes are cattle),
- non-standard ports clients can't use directly,
- forced SNAT or `externalTrafficPolicy: Local` correctness
  footguns.

The fabric is the load balancer. There's no "but what if there's
no LB" fallback to plan for.

---

## Summary of cuts

| # | Area | Change shape | Blast radius |
|---|------|--------------|--------------|
| 1 | Shutdown | `TimeoutStopSec=2s`, fast-kill | Agent only |
| 2 | Bootstrap | Typed `KubeadmBootstrap`, drop cloud-init | Proto + provider + image |
| 3 | Network-config | Drop hostname/search-domains | Agent template |
| 4 | MAC | Random + persisted (drop determinism) | Agent + DB |
| 5 | LAN VIP | Drop L2 fallback, require BGP | Controller + ansible |
| 6 | Disks | Named roles, drop free-form list | Proto + provider |
| 7 | CPU overcommit | Pin to 1.0, drop knob | Controller |
| 8 | Apiserver visibility | Drop enum, always external | Proto + controller |
| 9 | NodePort | (already absent — document non-goal) | Doc only |

Recommended order: 1, 3, 4, 7, 8 first (small, mostly-mechanical,
low risk); then 6; then 5 (deployment-shape change); then 2
(largest surface change but biggest payoff in image size +
boot time).
