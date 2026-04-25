#!/usr/bin/env bash
# Regenerate `deploy/capi/infrastructure-components.yaml` — the complete
# clusterctl-style provider bundle (Namespace + CRDs + RBAC + Deployment).
#
# This file is the single source of truth for the basis-capi-provider
# package. Lattice keeps a committed copy at
# `test-providers/infrastructure-basis/v0.1.0/infrastructure-components.yaml`;
# pass --sync-lattice (or set LATTICE_REPO) to update it in the same
# invocation.
#
# Called automatically from lattice's `scripts/dev/test-basis.sh` before
# every e2e run so stale CRD schemas never reach the apiserver.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT="$REPO_ROOT/deploy/capi/infrastructure-components.yaml"

SYNC_LATTICE=0
LATTICE_REPO="${LATTICE_REPO:-}"
for arg in "$@"; do
    case "$arg" in
        --sync-lattice) SYNC_LATTICE=1 ;;
        --lattice-repo=*) LATTICE_REPO="${arg#--lattice-repo=}"; SYNC_LATTICE=1 ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

mkdir -p "$(dirname "$OUT")"
echo "Regenerating $OUT"
# `--manifest-path` pins cargo to the basis workspace. Without it the
# script would resolve against the caller's cwd — e.g. test-basis.sh
# invoking us from inside the lattice checkout would fail with
# "package `basis-capi-provider` not found in workspace".
cargo run --quiet --manifest-path "$REPO_ROOT/Cargo.toml" \
    -p basis-capi-provider -- --print-components > "$OUT"
echo "  wrote $(wc -l < "$OUT") lines"

if (( SYNC_LATTICE )); then
    if [[ -z "$LATTICE_REPO" ]]; then
        LATTICE_REPO="$(cd "$REPO_ROOT/../lattice" 2>/dev/null && pwd)" || true
    fi
    if [[ -z "$LATTICE_REPO" || ! -d "$LATTICE_REPO" ]]; then
        echo "Error: --sync-lattice requires LATTICE_REPO to point at a lattice checkout" >&2
        exit 1
    fi
    LATTICE_OUT="$LATTICE_REPO/test-providers/infrastructure-basis/v0.1.0/infrastructure-components.yaml"
    if [[ ! -d "$(dirname "$LATTICE_OUT")" ]]; then
        echo "Error: $LATTICE_OUT parent dir does not exist — is this the right lattice checkout?" >&2
        exit 1
    fi
    cp "$OUT" "$LATTICE_OUT"
    echo "  synced to $LATTICE_OUT"
fi
