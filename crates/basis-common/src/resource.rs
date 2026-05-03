//! Kubernetes-style resource envelope used for local config files.
//!
//! Agent and controller configs both live in YAML files shaped like CRDs:
//!
//! ```yaml
//! apiVersion: basis.dev/v1alpha1
//! kind: Host                # or BasisController
//! metadata:
//!   name: node-1
//! spec:
//!   ...
//! ```
//!
//! The shape matches the `BasisCluster` / `BasisMachine` CRDs served by
//! the capi-provider, so every YAML in the Basis ecosystem reads the same
//! way.

use serde::{Deserialize, Serialize};

/// The only API version Basis ships today.
pub const API_VERSION: &str = "basis.dev/v1alpha1";

/// A typed resource envelope. `Spec` is the resource-kind-specific payload.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource<Spec> {
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: Spec,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Metadata {
    pub name: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    #[error("reading {path}: {source}")]
    Read {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parsing {path}: {source}")]
    Parse {
        path: std::path::PathBuf,
        #[source]
        source: serde_yaml_ng::Error,
    },

    #[error("{path}: unsupported apiVersion '{got}' (expected '{}')", API_VERSION)]
    ApiVersion {
        path: std::path::PathBuf,
        got: String,
    },

    #[error("{path}: wrong kind '{got}' (expected '{expected}')")]
    Kind {
        path: std::path::PathBuf,
        got: String,
        expected: &'static str,
    },

    /// Resource-specific validation failure surfaced after parse. The
    /// `kind` lets callers identify which resource type rejected the
    /// document; `source` carries the validator's message.
    #[error("validating {kind}: {source}")]
    Other {
        kind: String,
        #[source]
        source: anyhow::Error,
    },
}

/// Read a YAML file, deserialize to `Resource<Spec>`, verify the envelope.
///
/// Envelope fields (`apiVersion`, `kind`) are validated **before** the
/// `spec` is deserialized — a mismatched-kind file is reported as a Kind
/// error even if its spec wouldn't parse against `Spec`, which is a far
/// more useful error than "unknown field" spam.
pub fn load_resource<Spec>(
    path: &std::path::Path,
    expected_kind: &'static str,
) -> Result<Resource<Spec>, ResourceError>
where
    Spec: for<'de> Deserialize<'de>,
{
    let contents = std::fs::read_to_string(path).map_err(|source| ResourceError::Read {
        path: path.to_path_buf(),
        source,
    })?;

    let envelope: Resource<serde_yaml_ng::Value> =
        serde_yaml_ng::from_str(&contents).map_err(|source| ResourceError::Parse {
            path: path.to_path_buf(),
            source,
        })?;

    if envelope.api_version != API_VERSION {
        return Err(ResourceError::ApiVersion {
            path: path.to_path_buf(),
            got: envelope.api_version,
        });
    }
    if envelope.kind != expected_kind {
        return Err(ResourceError::Kind {
            path: path.to_path_buf(),
            got: envelope.kind,
            expected: expected_kind,
        });
    }

    let spec: Spec =
        serde_yaml_ng::from_value(envelope.spec).map_err(|source| ResourceError::Parse {
            path: path.to_path_buf(),
            source,
        })?;

    Ok(Resource {
        api_version: envelope.api_version,
        kind: envelope.kind,
        metadata: envelope.metadata,
        spec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct TestSpec {
        value: u32,
    }

    fn write_temp(yaml: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        f
    }

    #[test]
    fn loads_valid_resource() {
        let f = write_temp(
            r#"apiVersion: basis.dev/v1alpha1
kind: Test
metadata:
  name: x
spec:
  value: 42
"#,
        );
        let r: Resource<TestSpec> = load_resource(f.path(), "Test").unwrap();
        assert_eq!(r.metadata.name, "x");
        assert_eq!(r.spec.value, 42);
    }

    #[test]
    fn rejects_wrong_kind_before_parsing_spec() {
        // spec content is invalid for TestSpec, but we should see the Kind
        // error first — kind check runs before spec deserialization.
        let f = write_temp(
            r#"apiVersion: basis.dev/v1alpha1
kind: Other
metadata: { name: x }
spec: { nothing: valid }
"#,
        );
        assert!(matches!(
            load_resource::<TestSpec>(f.path(), "Test"),
            Err(ResourceError::Kind { .. })
        ));
    }

    #[test]
    fn rejects_wrong_api_version() {
        let f = write_temp(
            r#"apiVersion: basis.dev/v2
kind: Test
metadata: { name: x }
spec: { value: 1 }
"#,
        );
        assert!(matches!(
            load_resource::<TestSpec>(f.path(), "Test"),
            Err(ResourceError::ApiVersion { .. })
        ));
    }
}
