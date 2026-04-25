use std::time::{Duration, SystemTime};

/// RFC 3339 timestamp for the current time, second-precision.
///
/// String-sortable, which matters because the controller DB compares
/// `last_heartbeat` against a cutoff timestamp using `<`.
pub fn now_rfc3339() -> String {
    humantime::format_rfc3339_seconds(SystemTime::now()).to_string()
}

/// RFC 3339 timestamp for `Duration` ago. Used by the health checker to
/// compute a heartbeat staleness cutoff.
pub fn rfc3339_ago(duration: Duration) -> String {
    let cutoff = SystemTime::now()
        .checked_sub(duration)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    humantime::format_rfc3339_seconds(cutoff).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_is_rfc3339_like() {
        let ts = now_rfc3339();
        assert!(ts.ends_with('Z'));
        assert!(ts.contains('T'));
        assert_eq!(ts.len(), 20); // "YYYY-MM-DDTHH:MM:SSZ"
    }

    #[test]
    fn test_ago_is_in_past() {
        let now = now_rfc3339();
        let past = rfc3339_ago(Duration::from_secs(60));
        assert!(past < now, "expected {past} < {now}");
    }

    #[test]
    fn test_ago_handles_pre_epoch() {
        // Should not panic if duration is absurdly large.
        let _ = rfc3339_ago(Duration::from_secs(u64::MAX));
    }
}
