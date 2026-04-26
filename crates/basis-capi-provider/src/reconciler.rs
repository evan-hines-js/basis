//! Cross-resource scaffolding shared by the BasisCluster and BasisMachine
//! reconcilers: the (otherwise identical) controller context, the small
//! trait that classifies a reconcile error, the helper that mirrors a
//! failure back onto the resource as `Ready=False` + `failureMessage`,
//! and the thin kube-API patch wrappers both reconcilers use.

use std::fmt::{Debug, Display};
use std::sync::Arc;

use kube::api::{Api, Patch, PatchParams, Resource};
use kube::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use tracing::warn;

use crate::client_cache::BasisClientCache;
use crate::conditions;
use crate::crds::Condition;

/// Namespace of a CAPI-provider-managed CR, with the same fallback CAPI
/// core uses when a cluster-scoped reconcile fires against something
/// shaped like a namespaced object.
pub fn namespace_of<T: Resource>(obj: &T) -> String {
    obj.meta()
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string())
}

/// Apply a merge patch to an object's `spec`.
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

/// Context handed to every reconciler.
pub struct ProviderContext {
    pub client: Client,
    pub clients: Arc<BasisClientCache>,
}

/// Properties every reconcile-error type carries. Keeps the failure
/// recording helper below from caring whether it's been handed a
/// `ClusterError` or a `MachineError` — both implement this with a
/// stable, machine-readable `reason()` and a one-bit "is this a
/// credentials problem?" classifier the cache uses to decide whether
/// to invalidate.
pub trait ReconcileError: Display {
    /// Stable label that goes into the `Ready=False` condition's
    /// `reason` field. Dashboards/alerts match on this.
    fn condition_reason(&self) -> &'static str;

    /// True when the failure suggests our cached BasisClient is using
    /// stale credentials and the cache entry should be dropped.
    fn is_credentials_problem(&self) -> bool;

    /// True when the failure is controller-side load shedding
    /// (`Status::Unavailable` / `DeadlineExceeded`). basis-client
    /// already retries these internally with an ~30s budget; anything
    /// that surfaces here survived that budget, so the right response
    /// is a *short* requeue (5s, not 15s) and an `info!` log instead
    /// of `error!`. Alerting should ignore the resulting
    /// `BasisBackpressure` condition — it's expected during overload.
    fn is_transient(&self) -> bool;
}

/// Patch the resource's `status` with a `Ready=False` condition and
/// `failureMessage`. Best-effort: a patch failure is logged but never
/// returned, because the original `err` is what callers care about.
///
/// `existing_conditions` is whatever the resource currently has on
/// `status.conditions` so we preserve other controllers' entries
/// through the upsert.
pub async fn record_failure_status<R, E>(
    api: &Api<R>,
    name: &str,
    generation: Option<i64>,
    existing_conditions: Vec<Condition>,
    err: &E,
) where
    R: Resource<DynamicType = ()> + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
    E: ReconcileError,
{
    let message = err.to_string();
    let mut conditions = existing_conditions;
    conditions::upsert(
        &mut conditions,
        conditions::ready_false(err.condition_reason(), message.clone(), generation),
    );

    let payload = json!({
        "status": {
            "ready": false,
            "failureMessage": message,
            "conditions": conditions,
        }
    });
    if let Err(patch_err) = merge_status(api, name, &payload).await {
        warn!(
            resource = %name,
            error = %patch_err,
            "could not patch failure status"
        );
    }
}
