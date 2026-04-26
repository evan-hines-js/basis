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
# Local image baked from deploy/build.Dockerfile. We `docker build` it
# every run; if the Dockerfile is unchanged, all layers hit the cache and
# the build is essentially free. The bake gives us a stable rustc + apt
# deps without re-running apt-get on every cargo invocation.
BUILD_IMAGE="basis-build:local"
BUILD_DOCKERFILE="$SCRIPT_DIR/build.Dockerfile"
# Build artifacts live in a docker-managed volume, NOT in the bind-mounted
# repo. Bind mounts on macOS (virtiofs/osxfs) don't preserve mtime byte-for-byte,
# which silently invalidates cargo's `CheckDepInfo` and triggers a full rebuild
# every run. With the volume, the target dir lives on the same Linux fs cargo
# wrote it to last time. We copy the two binaries we ship out at the end.
BUILD_VOLUME="basis-cargo-target"
STAGING_DIR="$REPO_ROOT/target/$BUILD_TARGET/release"

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

say "Building cross-compile image (cached unless build.Dockerfile changed)"
docker build \
  --platform linux/amd64 \
  -t "$BUILD_IMAGE" \
  -f "$BUILD_DOCKERFILE" \
  "$SCRIPT_DIR"

say "Cross-compiling basis-controller and basis-agent for $BUILD_TARGET"
# `--platform linux/amd64` forces the container to run as x86_64 even on
# Apple Silicon (via Rosetta). Makes the cargo target match the image's
# native target so we don't need `rustup target add`.
#
# Two named volumes:
#   * basis-cargo-cache  — crate registry (~/.cargo/registry)
#   * basis-cargo-target — CARGO_TARGET_DIR; lives on a Linux fs so cargo's
#                          mtime-based incremental check actually works.
mkdir -p "$STAGING_DIR"
docker run --rm \
  --platform linux/amd64 \
  -v "$REPO_ROOT":/work \
  -v basis-cargo-cache:/usr/local/cargo/registry \
  -v "$BUILD_VOLUME":/build \
  -v "$STAGING_DIR":/out \
  -w /work \
  -e CARGO_TARGET_DIR=/build \
  "$BUILD_IMAGE" \
  bash -euxc "
    cargo build --release --target $BUILD_TARGET \
      -p basis-controller -p basis-agent
    install -m 0755 \
      /build/$BUILD_TARGET/release/basis-controller \
      /build/$BUILD_TARGET/release/basis-agent \
      /out/
  "

for bin in basis-controller basis-agent; do
  [[ -x "$STAGING_DIR/$bin" ]] || die "build did not produce $STAGING_DIR/$bin"
done
say "Binaries staged: $STAGING_DIR"
TARGET_DIR="$STAGING_DIR"

say "Running Ansible playbook"
cd "$ANSIBLE_DIR"
# Pick up the repo-local secrets.yml automatically if it exists so callers
# don't have to remember `-e @secrets.yml` every run. The vault password
# itself lives at ~/.basis-vault-pass (configured in ansible.cfg).
EXTRA_VARS=()
if [[ -f "$ANSIBLE_DIR/secrets.yml" ]]; then
  EXTRA_VARS+=("-e" "@secrets.yml")
fi
exec ansible-playbook site.yml \
  -e "basis_binary_dir=$TARGET_DIR" \
  "${EXTRA_VARS[@]}" \
  "$@"
