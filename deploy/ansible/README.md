# Basis — Ansible deployment

One-shot install of `basis-controller` and `basis-agent` across a fleet of
bare-metal hosts. The playbook is designed for iterative use: run it,
let it fail, fix the task, run it again. Everything is idempotent.

## Topology

One dedicated host under `[basis_controller]` runs the control-plane
process — no VMs on it. Every host under `[basis_agents]` runs
`basis-agent` and hosts VMs: KVM, LVM thin pool, cloud-hypervisor, qemu
utilities are only installed there. The controller host only needs the
binary, TLS material, and a config file.

## Prerequisites

On the Ansible control node (your laptop):

```
pip install ansible-core cryptography
ansible-galaxy collection install -r requirements.yml
```

### Per-host preparation (fresh Ubuntu/Debian)

Three things aren't automated — the playbook can't safely do them without
a human in the loop:

1. **Root SSH** (or non-root + passwordless sudo). Ubuntu Server doesn't
   enable root SSH by default. Easiest path: during install, seed the
   root user's `~/.ssh/authorized_keys` via cloud-init autoinstall. Or
   change `ansible_user` in inventory and give that user `NOPASSWD` in
   `/etc/sudoers.d/`.

2. **A dedicated partition for the LVM thin pool.** On agents with a
   single NVMe (the OS and the VM disk pool share the drive), leave free
   space during install and carve out a partition:

   ```
   parted /dev/nvme0n1 mkpart primary 60GB 100%   # OS in the first 60G
   wipefs -a /dev/nvme0n1p3                       # required — basis refuses to pvcreate over an existing FS signature
   ```

   Then put the partition in `host_vars/<host>.yml`:

   ```yaml
   basis_lvm_devices:
     - /dev/nvme0n1p3
   ```

   Hosts with dedicated SSDs (the PowerEdges) list whole devices instead.
   The playbook fails fast with a clear message if `basis_lvm_devices` is
   empty for any agent.

3. **BIOS virtualization on.** The `Verify /dev/kvm exists` task fails
   fast if SVM/VT-x is off.

### What the playbook does handle

- **Bridge networking.** On first apply, `basis-prereqs` renders
  `/etc/netplan/60-basis-bridge.yaml` with the host's current IP moved
  onto `vmbr0` (physical NIC as a bridge member), then **reboots** to
  apply it cleanly. Subsequent runs are no-ops. This step is what lets
  `basis-agent` later enslave the NIC to the bridge without stripping
  the host's IP and killing SSH. On ex-Proxmox hosts where the bridge
  already owns the IP, detection catches that and skips the reboot.

  Set `basis_manage_netplan: false` in group/host vars if you manage
  host networking yourself.

- **Everything else:** apt packages, LVM thin pool, cloud-hypervisor,
  kernel modules, PKI, systemd units, configs.

## First-time setup

```
cd deploy/ansible
cp inventory.ini.example inventory.ini
$EDITOR inventory.ini                          # list your host IPs
$EDITOR group_vars/all.yml                     # adjust IP pool range

# Per-agent LVM devices for the VM-disk thin pool. One file per agent —
# these are the block devices basis wipes on first apply. Controllers
# don't need this.
$EDITOR host_vars/dell1.yml                    # basis_lvm_devices: [/dev/sdb, ...]

# Build release binaries — the playbook expects them at target/release.
(cd ../.. && cargo build --release -p basis-controller -p basis-agent)

ansible-playbook site.yml -vv
```

Heterogeneous hardware is supported out of the box. Each host's NIC is
autodetected at apply time: picks the bridge's non-tap slave if the
bridge already exists, otherwise the default-route interface. So a Dell
with `eno1`, a Beelink with `enp2s0`, and an ex-Proxmox node with `nic0`
can all share the same one-line-per-host inventory. Override with
`basis_physical_nic=<name>` only for hosts with multiple NICs or no
default route.

## Iteration loop

Bring up one host first:

```
ansible-playbook site.yml --limit dell1 -vv
```

If a task fails, fix it in the role, then re-run. Tasks are idempotent
so there's no harm in restarting from the top. To skip ahead:

```
ansible-playbook site.yml --limit dell1 --start-at-task "Copy basis-agent binary"
```

Scoped reruns by role:

```
ansible-playbook site.yml --tags pki         # just (re)issue certs
ansible-playbook site.yml --tags controller
ansible-playbook site.yml --tags agent
```

Once `dell1` is healthy, add more hosts to `--limit` or drop it and roll
the whole fleet.

## Checking it worked

On the controller host:

```
journalctl -u basis-controller -f
sqlite3 /var/lib/basis/controller.db 'SELECT hostname, healthy, available_cpu FROM hosts;'
```

You should see a row per agent that has connected, `healthy = 1`, and
`available_cpu` updated from the real heartbeats (not the initial total).

On an agent:

```
journalctl -u basis-agent -f
```

## What lives where

| Artifact | On the host |
|---|---|
| Binaries | `/usr/local/bin/basis-{controller,agent}` |
| TLS | `/etc/basis/tls/` (CA + the host's own cert + key) |
| Configs | `/etc/basis/basiscontroller.yaml` or `/etc/basis/host.yaml` |
| State | `/var/lib/basis/` (controller.db, agent.db, images/, vms/) |

| Artifact | On the control node |
|---|---|
| CA + all leaf certs | `deploy/ansible/pki/` (gitignored — **do not commit**) |

## Cert rotation

Delete the target cert file in `pki/` and re-run the `pki` tag — the
playbook regenerates the missing cert and the `reload basis-agent` /
`restart basis-controller` handlers pick it up. The CA key stays put;
only leaves rotate.

## Removing a host

Remove the host from `inventory.ini`. On the host itself:
`systemctl stop basis-agent && systemctl disable basis-agent`. The
controller will mark it unhealthy after 90s of missed heartbeats, and
on reconnect (or via a future explicit-delete API) the VMs registered
to that host get cleaned up per the design doc.

## Known pitfalls

- **Cloud Hypervisor binary URL.** The playbook downloads a static build
  from the upstream GitHub release. If the architecture isn't x86_64,
  override `cloud_hypervisor_url` in `group_vars/all.yml`.
- **BIOS virtualization.** The `verify /dev/kvm exists` task fails fast
  if you forgot to enable SVM/VT-x. Go flip it and re-run.
- **Bridge conflict.** If a host already uses `basis0` for something
  else, override `basis_bridge` in a `host_vars/<host>.yml`.
- **IP pool overlap.** `basis_ip_pool.range_*` MUST NOT overlap your
  router's DHCP range. Basis hands out static IPs; DHCP collisions mean
  a broken VM and no good diagnostic.
