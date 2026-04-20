//! Helpers shared by the `BasisCluster` and `BasisMachine` reconcilers.
//!
//! Anything duplicated across both files belongs here — there should be
//! one spelling of "namespace fallback," "merge-patch," etc.

use kube::api::{Api, Patch, PatchParams};
use kube::Resource;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

/// Namespace of a CAPI-provider-managed CR, with the same fallback CAPI
/// core uses when a cluster-scoped reconcile fires against something
/// shaped like a namespaced object.
pub fn namespace_of<T: Resource>(obj: &T) -> String {
    obj.meta()
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string())
}

/// Apply a merge patch to an object's `spec`. Thin wrapper to keep the
/// reconcilers free of `PatchParams::default()` / `Patch::Merge`
/// boilerplate.
pub async fn merge_spec<T>(
    api: &Api<T>,
    name: &str,
    patch: &serde_json::Value,
) -> Result<(), kube::Error>
where
    T: Resource + Clone + DeserializeOwned + Debug + Serialize,
    <T as Resource>::DynamicType: Default,
{
    api.patch(name, &PatchParams::default(), &Patch::Merge(patch))
        .await?;
    Ok(())
}

/// Apply a merge patch to an object's `status` subresource.
pub async fn merge_status<T>(
    api: &Api<T>,
    name: &str,
    patch: &serde_json::Value,
) -> Result<(), kube::Error>
where
    T: Resource + Clone + DeserializeOwned + Debug + Serialize,
    <T as Resource>::DynamicType: Default,
{
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
        .await?;
    Ok(())
}
