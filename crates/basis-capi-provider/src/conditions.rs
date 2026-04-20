//! Condition list maintenance shared by the cluster and machine reconcilers.
//!
//! CAPI v1beta2 conditions follow the Kubernetes API convention:
//! `lastTransitionTime` only changes when `status` changes, and existing
//! conditions authored by other controllers must be preserved on write.

use basis_common::time::now_rfc3339;

use crate::crds::Condition;

/// Build a `Ready=True` condition stamped `now` with the given reason.
/// Both reconcilers emit the same shape (`Ready=True, reason=<phase>`);
/// this keeps the call site readable and avoids drift in field names.
pub fn ready_true(reason: &'static str, generation: Option<i64>) -> Condition {
    Condition {
        kind: "Ready".to_string(),
        status: "True".to_string(),
        reason: Some(reason.to_string()),
        message: None,
        last_transition_time: now_rfc3339(),
        observed_generation: generation,
    }
}

/// Merge `new` into `existing` by `type`. Preserves `lastTransitionTime`
/// when the condition's `status` is unchanged (per K8s convention).
/// Other controllers' conditions on other `type`s pass through untouched.
pub fn upsert(existing: &mut Vec<Condition>, new: Condition) {
    match existing.iter_mut().find(|c| c.kind == new.kind) {
        Some(current) => {
            if current.status != new.status {
                current.last_transition_time = new.last_transition_time;
            }
            current.status = new.status;
            current.reason = new.reason;
            current.message = new.message;
            current.observed_generation = new.observed_generation;
        }
        None => existing.push(new),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cond(kind: &str, status: &str, time: &str) -> Condition {
        Condition {
            kind: kind.to_string(),
            status: status.to_string(),
            reason: None,
            message: None,
            last_transition_time: time.to_string(),
            observed_generation: Some(1),
        }
    }

    #[test]
    fn inserts_new_condition() {
        let mut conds = vec![];
        upsert(&mut conds, cond("Ready", "True", "t1"));
        assert_eq!(conds.len(), 1);
        assert_eq!(conds[0].last_transition_time, "t1");
    }

    #[test]
    fn preserves_transition_time_when_status_unchanged() {
        let mut conds = vec![cond("Ready", "True", "t1")];
        upsert(&mut conds, cond("Ready", "True", "t2"));
        assert_eq!(conds[0].last_transition_time, "t1");
    }

    #[test]
    fn bumps_transition_time_when_status_changes() {
        let mut conds = vec![cond("Ready", "True", "t1")];
        upsert(&mut conds, cond("Ready", "False", "t2"));
        assert_eq!(conds[0].last_transition_time, "t2");
        assert_eq!(conds[0].status, "False");
    }

    #[test]
    fn preserves_other_controllers_conditions() {
        let mut conds = vec![cond("Paused", "True", "t0")];
        upsert(&mut conds, cond("Ready", "True", "t1"));
        assert_eq!(conds.len(), 2);
        assert!(conds.iter().any(|c| c.kind == "Paused"));
        assert!(conds.iter().any(|c| c.kind == "Ready"));
    }
}
