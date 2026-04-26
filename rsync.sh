#!/bin/bash
# Sync basis source + ansible-issued PKI to the relevant hosts:
#   - 10.0.0.131 — install host (lattice-cli install sources
#     ~/basis/.basis.credentials, which `cat`s the synced PKI).
#   - 10.0.0.206 — hypervisor used to run smoke.sh's Section 1
#     end-to-end (the in-VM SSH + egress checks need a route into
#     the tree CIDR, which only a hypervisor has).
# Re-rsync after every ansible run to keep both hosts current.
set -euo pipefail
# user@host pairs — 10.0.0.206 is provisioned with root login (no
# unprivileged ubuntu user), the install host runs as ubuntu.
for target in ubuntu@10.0.0.131 root@10.0.0.206 root@10.0.0.97; do
  rsync -az --delete \
    --exclude='build' \
    --exclude='target' \
    --exclude='node_modules' \
    ../basis/ "$target:~/basis/"
done
