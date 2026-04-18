#!/usr/bin/env bash
# End-to-end deploy: cross-compile basis binaries via Docker, then run
# the Ansible playbook against whatever `inventory.ini` says.
#
# Usage:
#   ./deploy/bootstrap.sh                           # full run
#   ./deploy/bootstrap.sh --limit node-1 -vv        # scoped run, verbose
#   ./deploy/bootstrap.sh --tags agent              # specific stage
#
# Any extra args are forwarded to ansible-playbook.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ANSIBLE_DIR="$REPO_ROOT/deploy/ansible"
BUILD_TARGET="x86_64-unknown-linux-gnu"
TARGET_DIR="$REPO_ROOT/target/$BUILD_TARGET/release"

say() { printf '\n==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

say "Checking prerequisites"
for cmd in docker ansible-playbook ansible-galaxy; do
  command -v "$cmd" >/dev/null || die "missing: $cmd"
done
docker info >/dev/null 2>&1 || die "docker daemon not running"

[[ -f "$ANSIBLE_DIR/inventory.ini" ]] || die \
  "missing $ANSIBLE_DIR/inventory.ini — copy inventory.ini.example and edit first"

say "Installing Ansible collections"
ansible-galaxy collection install -r "$ANSIBLE_DIR/requirements.yml" >/dev/null

say "Cross-compiling basis-controller and basis-agent for $BUILD_TARGET"
# `--platform linux/amd64` forces the container to run as x86_64 even on
# Apple Silicon (via Rosetta). Makes the cargo target match the image's
# native target so we don't need `rustup target add`.
#
# The named volume caches the crate registry across runs; the repo's
# own `target/` is bind-mounted so incremental builds stay fast.
docker run --rm \
  --platform linux/amd64 \
  -v "$REPO_ROOT":/work \
  -v basis-cargo-cache:/usr/local/cargo/registry \
  -w /work \
  rust:1-bookworm \
  bash -euxc "
    apt-get update -qq
    apt-get install -y -qq cmake clang protobuf-compiler pkg-config
    cargo build --release --target $BUILD_TARGET \
      -p basis-controller -p basis-agent
  "

for bin in basis-controller basis-agent; do
  [[ -x "$TARGET_DIR/$bin" ]] || die "build did not produce $TARGET_DIR/$bin"
done
say "Binaries built: $TARGET_DIR"

say "Running Ansible playbook"
cd "$ANSIBLE_DIR"
exec ansible-playbook site.yml \
  -e "basis_binary_dir=$TARGET_DIR" \
  "$@"
