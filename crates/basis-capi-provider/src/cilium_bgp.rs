//! Per-cluster Cilium BGP CRD renderers.
//!
//! Lattice's `crates/lattice-cilium` ships the Cilium chart with
//! `bgpControlPlane.enabled=true` for basis-provider clusters, but
//! the chart alone has nobody to peer with — the peer config lives
//! in the `CiliumBGPClusterConfig` / `CiliumBGPPeerConfig` /
//! `CiliumBGPAdvertisement` CRDs, which need the cell route reflector
//! address + ASN. Those values are basis-internal (the reflector is
//! embedded in basis-controller) so this renderer lives here, not in
//! Lattice.
//!
//! The CRDs are rendered from the values stamped onto
//! `BasisClusterStatus` after `Basis.CreateCluster` returns. The
//! workload-cluster apply path is a TODO — basis-capi-provider does
//! not currently hold a kube client to the workload cluster, so the
//! rendered manifests are emitted by [`render_bgp_crds`] for a
//! follow-up to wire up (either via a basis-side post-bootstrap
//! reconciler, a CAPI-style "additional manifests" hook, or by
//! plumbing the values through to lattice-cell's bootstrap bundle).
//!
//! Output is stable, deterministic JSON (one document per Vec entry)
//! so callers can cat them into a manifest stream without
//! re-marshalling.

use serde::Serialize;
use serde_json::json;

/// Inputs the per-cluster BGP CRDs depend on. Keep this struct
/// tight: every field is a value Lattice must NOT learn (only basis
/// knows the cell RR + ASN; the cluster-id is already on the
/// BasisCluster CR).
pub struct BgpRenderInputs<'a> {
    /// Stable cluster identifier returned by `Basis.CreateCluster`.
    /// Used as the `nodeSelector` value so the per-cluster
    /// `CiliumBGPClusterConfig` only matches its own nodes if
    /// multiple BasisClusters ever share a single workload cluster
    /// (currently 1:1, but the labelling is forward-compatible).
    pub cluster_id: &'a str,
    /// Cell BGP route reflector — the iBGP peer every node in the
    /// cluster speaks to. Same value `Basis.CreateCluster` returned.
    pub reflector_address: &'a str,
    /// Cell ASN. iBGP, single ASN cell-wide.
    pub asn: u32,
}

/// Render the four CRDs Cilium needs for BGP-LB on a basis-provider
/// cluster, in apply order: peer-config (TCP/keepalive timers etc.),
/// cluster-config (selects nodes + peers), advertisement (selects the
/// `default` LB pool to announce). Returns one JSON-serialised
/// document per Vec entry.
pub fn render_bgp_crds(inputs: &BgpRenderInputs<'_>) -> Result<Vec<String>, serde_json::Error> {
    let peer_config = serde_json::to_string(&CiliumBGPPeerConfig {
        api_version: API_VERSION,
        kind: "CiliumBGPPeerConfig",
        metadata: Metadata {
            name: PEER_CONFIG_NAME,
            labels: managed_by_labels(),
        },
        spec: json!({
            // Timers carried over from Cilium defaults; pinned here
            // so a chart upgrade can't quietly change failure
            // detection latency.
            "timers": {
                "holdTimeSeconds": 90,
                "keepAliveTimeSeconds": 30,
                "connectRetryTimeSeconds": 120,
            },
            // Graceful restart so a basis-controller bounce doesn't
            // flap every cluster's BGP-advertised LB IPs while the
            // RR's TCP sessions re-establish.
            "gracefulRestart": {
                "enabled": true,
                "restartTimeSeconds": 120,
            },
            // Advertise IPv4 unicast only. Match the export
            // advertisement below; mismatched AFI/SAFI on the
            // session means the prefix never moves.
            "families": [
                { "afi": "ipv4", "safi": "unicast" }
            ],
        }),
    })?;

    let cluster_config = serde_json::to_string(&CiliumBGPClusterConfig {
        api_version: API_VERSION,
        kind: "CiliumBGPClusterConfig",
        metadata: Metadata {
            name: CLUSTER_CONFIG_NAME,
            labels: managed_by_labels(),
        },
        spec: json!({
            // No nodeSelector — every k8s node in the cluster runs
            // the BGP daemon and peers with the RR. ECMP across all
            // announcers is exactly the design.
            "bgpInstances": [{
                "name": "basis-cell",
                "localASN": inputs.asn,
                "peers": [{
                    "name": "basis-rr",
                    "peerASN": inputs.asn,
                    "peerAddress": inputs.reflector_address,
                    "peerConfigRef": {
                        "kind": "CiliumBGPPeerConfig",
                        "name": PEER_CONFIG_NAME,
                    },
                }],
            }],
        }),
    })?;

    let advertisement = serde_json::to_string(&CiliumBGPAdvertisement {
        api_version: API_VERSION,
        kind: "CiliumBGPAdvertisement",
        metadata: Metadata {
            name: ADVERT_NAME,
            // Selected by the BGPClusterConfig's bgpInstances.
            // (Cilium's docs use `peerSelector`; `advertise.matchLabels`
            // is the modern v2alpha1 shape.)
            labels: bgp_advertise_labels(inputs.cluster_id),
        },
        spec: json!({
            "advertisements": [{
                "advertisementType": "Service",
                "service": {
                    // Announce LoadBalancer IPs only. ClusterIPs and
                    // ExternalIPs aren't routed beyond the cluster
                    // by basis's RR.
                    "addresses": ["LoadBalancerIP"],
                },
                // Match every Service in the cluster's `default` LB
                // pool. The pool itself comes from lattice-cell's
                // bootstrap bundle (`generate_bgp_lb_pool`).
                "selector": {
                    "matchExpressions": [{
                        "key": "io.kubernetes.service.namespace",
                        "operator": "Exists",
                    }],
                },
            }],
        }),
    })?;

    Ok(vec![peer_config, cluster_config, advertisement])
}

const API_VERSION: &str = "cilium.io/v2alpha1";
const PEER_CONFIG_NAME: &str = "basis-rr";
const CLUSTER_CONFIG_NAME: &str = "basis-cell";
const ADVERT_NAME: &str = "basis-lb-services";

fn managed_by_labels() -> serde_json::Value {
    json!({
        "app.kubernetes.io/managed-by": "basis-capi-provider",
    })
}

/// Selector labels the `CiliumBGPClusterConfig` matches its
/// advertisements by. Tagging the advertisement with the cluster_id
/// keeps future per-cluster advertisement variants from cross-
/// matching if multiple BasisClusters ever land on the same workload
/// cluster.
fn bgp_advertise_labels(cluster_id: &str) -> serde_json::Value {
    json!({
        "app.kubernetes.io/managed-by": "basis-capi-provider",
        "basis.lattice.dev/cluster-id": cluster_id,
        "advertise": "default-lb",
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    name: &'static str,
    labels: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CiliumBGPPeerConfig {
    api_version: &'static str,
    kind: &'static str,
    metadata: Metadata,
    spec: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CiliumBGPClusterConfig {
    api_version: &'static str,
    kind: &'static str,
    metadata: Metadata,
    spec: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CiliumBGPAdvertisement {
    api_version: &'static str,
    kind: &'static str,
    metadata: Metadata,
    spec: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_bgp_crds_emits_three_docs() {
        let docs = render_bgp_crds(&BgpRenderInputs {
            cluster_id: "abc-123",
            reflector_address: "10.0.0.206",
            asn: 65000,
        })
        .unwrap();
        assert_eq!(docs.len(), 3);
        assert!(docs[0].contains("CiliumBGPPeerConfig"));
        assert!(docs[1].contains("CiliumBGPClusterConfig"));
        assert!(docs[2].contains("CiliumBGPAdvertisement"));
    }

    /// The reflector address + ASN must round-trip into the
    /// CiliumBGPClusterConfig spec — the RR address is the value
    /// every k8s node's Cilium will dial.
    #[test]
    fn cluster_config_includes_reflector_and_asn() {
        let docs = render_bgp_crds(&BgpRenderInputs {
            cluster_id: "abc-123",
            reflector_address: "10.0.0.206",
            asn: 65000,
        })
        .unwrap();
        let cluster_config = &docs[1];
        assert!(cluster_config.contains("\"peerAddress\":\"10.0.0.206\""));
        assert!(cluster_config.contains("\"peerASN\":65000"));
        assert!(cluster_config.contains("\"localASN\":65000"));
    }

    /// The advertisement must select LB-IPs (not ClusterIPs / pod IPs)
    /// — basis's RR only reflects LB pool /32s out of the cell.
    #[test]
    fn advertisement_announces_loadbalancer_ips_only() {
        let docs = render_bgp_crds(&BgpRenderInputs {
            cluster_id: "abc-123",
            reflector_address: "10.0.0.206",
            asn: 65000,
        })
        .unwrap();
        let advert = &docs[2];
        assert!(advert.contains("\"advertisementType\":\"Service\""));
        assert!(advert.contains("\"addresses\":[\"LoadBalancerIP\"]"));
    }
}
