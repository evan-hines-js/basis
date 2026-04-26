#!/bin/bash
# Sync basis source + ansible-issued PKI to the install host. The
# pki/ dir under deploy/ansible/ rides along — lattice-cli install
# on the remote sources `.basis.credentials` which `cat`s the certs
# straight out of that synced directory. Re-rsync after every
# ansible run to keep the install host's view of PKI current.
set -euo pipefail
rsync -az --delete \
  --exclude='build' \
  --exclude='target' \
  --exclude='node_modules' \
  ../basis/ ubuntu@10.0.0.131:~/basis/
