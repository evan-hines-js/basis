# Basis — Ansible deployment

One-shot install of `basis-controller` and `basis-agent` across a fleet of
bare-metal hosts. The playbook is designed for iterative use: run it,
let it fail, fix the task, run it again. Everything is idempotent.

## Prerequisites

On the Ansible control node (your laptop):

```
pip install ansible-core cryptography
ansible-galaxy collection install -r requirements.yml
```

On every managed host: SSH access as a user with passwordless `sudo` (or
`ansible_user=root`, as in the example inventory).

## First-time setup

```
cd deploy/ansible
cp inventory.ini.example inventory.ini
$EDITOR inventory.ini                          # set your hosts + NIC names
$EDITOR group_vars/all.yml                     # adjust IP pool range

# Build release binaries — the playbook expects them at target/release.
(cd ../.. && cargo build --release -p basis-controller -p basis-agent)

ansible-playbook site.yml -vv
```

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
