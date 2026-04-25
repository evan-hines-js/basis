//! Clusterctl-style provider bundle generator.
//!
//! Emits a single `infrastructure-components.yaml` containing the
//! Namespace, all three `basis.infrastructure.cluster.x-k8s.io` CRDs,
//! and the ServiceAccount / RBAC / Deployment that run the provider
//! in the management cluster. Lattice's
//! `test-providers/infrastructure-basis/v0.1.0/infrastructure-components.yaml`
//! is a committed snapshot of this output, refreshed by the helper
//! script `basis/scripts/generate-capi-components.sh`.
//!
//! Source of truth:
//!   * CRDs: `crds.rs` via `CustomResourceExt::crd()`.
//!   * Static bits (Namespace, RBAC, Deployment): `components-template.yaml`.
//!   * Provider image tag + `app.kubernetes.io/version` label: the
//!     workspace version (`CARGO_PKG_VERSION` of this crate).
//!
//! A snapshot test in `tests/components_snapshot.rs` re-renders this
//! and diffs against the committed snapshot so basis CI catches drift.

use kube::CustomResourceExt;

use crate::crds::{BasisCluster, BasisMachine, BasisMachineTemplate};

const COMPONENTS_TEMPLATE: &str = include_str!("components-template.yaml");
const CRD_SENTINEL: &str = "# __BASIS_CRDS_HERE__";
const VERSION_SENTINEL: &str = "__BASIS_VERSION__";

/// The version string substituted into the Deployment image tag and
/// `app.kubernetes.io/version` label. Taken from
/// `[workspace.package].version` via `CARGO_PKG_VERSION`; bumping the
/// workspace version + regenerating keeps the YAML image tag in lockstep
/// with the tag `scripts/build-capi-provider.sh` pushes to ghcr.
pub const PROVIDER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Render the full `infrastructure-components.yaml`. Idempotent — each
/// call recomputes from the template and the CRD types.
pub fn render() -> anyhow::Result<String> {
    let (head, tail) = COMPONENTS_TEMPLATE
        .split_once(CRD_SENTINEL)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "components template is missing the {CRD_SENTINEL} splice marker — a refactor \
             broke the sync between components.rs and components-template.yaml"
            )
        })?;
    let head = head.replace(VERSION_SENTINEL, PROVIDER_VERSION);
    let tail = tail.replace(VERSION_SENTINEL, PROVIDER_VERSION);

    let mut out = String::with_capacity(COMPONENTS_TEMPLATE.len() + 16 * 1024);
    out.push_str(&head);
    append_crd::<BasisCluster>(&mut out)?;
    append_crd::<BasisMachineTemplate>(&mut out)?;
    append_crd::<BasisMachine>(&mut out)?;
    out.push_str(&tail);
    Ok(out)
}

/// Attach the labels CAPI core uses to discover our provider. Without
/// these, `cluster.x-k8s.io/v1beta2` core sees our BasisCluster as
/// "unknown contract" and `Cluster.status.InfrastructureReady` stays
/// `Unknown (InternalError: Please check controller logs for errors)`
/// no matter what we write to `BasisCluster.status.ready`. The pair of
/// contract labels maps CAPI v1beta{1,2} → our CRD version, so the
/// same bundle keeps working across a CAPI major upgrade.
fn append_crd<T: CustomResourceExt>(out: &mut String) -> anyhow::Result<()> {
    let mut crd = T::crd();
    let labels = crd.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(
        "cluster.x-k8s.io/provider".to_string(),
        "infrastructure-basis".to_string(),
    );
    labels.insert(
        "cluster.x-k8s.io/v1beta1".to_string(),
        "v1alpha1".to_string(),
    );
    labels.insert(
        "cluster.x-k8s.io/v1beta2".to_string(),
        "v1alpha1".to_string(),
    );
    out.push_str("---\n");
    out.push_str(&serde_yaml_ng::to_string(&crd)?);
    Ok(())
}
