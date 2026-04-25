//! Enforces that `deploy/capi/infrastructure-components.yaml` stays in
//! sync with the live CRD types + components template. If this test
//! fails, regenerate with `scripts/generate-capi-components.sh`.
//!
//! The same logic keeps Lattice's committed snapshot
//! (`test-providers/infrastructure-basis/v0.1.0/infrastructure-components.yaml`)
//! fresh — the script writes to both paths so CI in basis and e2e in
//! lattice can't silently disagree about the CRD schema.

use std::path::PathBuf;

#[test]
fn components_snapshot_matches_committed_file() {
    let rendered = basis_capi_provider::components::render()
        .expect("render() failed — a refactor probably broke the splice marker");

    let snapshot_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/capi/infrastructure-components.yaml");
    let committed = std::fs::read_to_string(&snapshot_path).unwrap_or_else(|e| {
        panic!(
            "could not read committed snapshot at {}: {e} — run \
             `scripts/generate-capi-components.sh` to produce it",
            snapshot_path.display()
        )
    });

    if rendered != committed {
        panic!(
            "committed infrastructure-components.yaml is stale. \
             Regenerate with `scripts/generate-capi-components.sh`.\n\
             snapshot path: {}",
            snapshot_path.display()
        );
    }
}
