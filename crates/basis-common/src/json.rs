//! Helpers for reading JSON blobs that basis itself has written.
//!
//! The controller and agent both store structured data (GPU inventory, GPU
//! assignments, PCI address lists) as JSON text inside SQLite columns.
//! Because basis is the only writer of those columns, a parse failure at
//! read time is not a recoverable data-shape bug — it means our own schema
//! has drifted and we must crash loudly.
//!
//! [`parse_owned_json`] centralises that invariant. Every call site names
//! the logical field it is reading so the panic message identifies the
//! exact column when the invariant is ever broken, instead of surfacing a
//! bare `serde_json::Error`.
//!
//! Do NOT use this for data that came from a remote peer, a config file,
//! or any other input we don't control.

use serde::de::DeserializeOwned;

/// Parse a JSON blob that was written by basis and must round-trip.
///
/// `field` should be a static label like `"vm.gpu_assignments"` that
/// identifies the column. Panics on failure with a message that names the
/// field, the target type, and the underlying `serde_json` error.
pub fn parse_owned_json<T: DeserializeOwned>(json: &str, field: &'static str) -> T {
    serde_json::from_str(json).unwrap_or_else(|e| {
        panic!(
            "basis-owned JSON in `{field}` failed to parse as {}: {e}",
            std::any::type_name::<T>()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_json() {
        let v: Vec<u32> = parse_owned_json("[1,2,3]", "test");
        assert_eq!(v, vec![1, 2, 3]);
    }

    #[test]
    #[should_panic(expected = "basis-owned JSON in `test.field`")]
    fn panics_on_bad_json_with_field_name() {
        let _: Vec<u32> = parse_owned_json("not json", "test.field");
    }
}
