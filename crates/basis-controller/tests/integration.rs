//! End-to-end gRPC flows exercised over real mTLS.
//!
//! Every test here generates its own CA + leaf certs with `rcgen`, writes
//! them to a tempdir, and runs the real `BasisServer::serve` on a random
//! loopback port. The same code path used by `basis-controller` in
//! production handles these requests — no insecure/test-only server
//! variants exist.

use std::sync::Arc;
use std::time::Duration;

use basis_proto::*;
use rustls::crypto::CryptoProvider;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

mod certs;
use certs::TestPki;

use basis_common::tls::{CAPI_PROVIDER_IDENTITY, CONTROLLER_IDENTITY};

async fn install_crypto_provider_once() {
    static INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    INIT.get_or_init(|| async {
        let _ = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider());
    })
    .await;
}

/// Small test-scale network: /24 per cluster gives 256 addresses —
/// bridge_reserve(8) + 2 sentinels = 10 reserved, leaving 246 VM IPs
/// starting at 10.0.0.9. The single named pool stands in for the
/// LAN-routable cluster VIP / LB block source.
fn test_network_config() -> basis_controller::config::NetworkConfig {
    basis_controller::config::NetworkConfig {
        cluster_supernet: "10.0.0.0/8".to_string(),
        cluster_prefix: 24,
        bridge_reserve: 8,
        default_external_service_ips: 4,
        vni_range: basis_controller::config::VniRange {
            start: 10_000,
            end: 11_000,
        },
        pools: vec![
            basis_controller::config::Pool {
                name: "cell-internal".to_string(),
                // /27 = 32 addrs total → 30 allocatable (.1..=.30).
                cidr: "192.168.100.0/27".to_string(),
                scope: basis_controller::config::PoolScope::Lan,
            },
            // Second pool, Tree-scoped, for trust_domain enforcement
            // tests. Disjoint CIDR from `cell-internal` so the
            // overlap check in NetworkConfig::validate is happy.
            basis_controller::config::Pool {
                name: "cell-tree".to_string(),
                cidr: "172.20.0.0/26".to_string(),
                scope: basis_controller::config::PoolScope::Tree,
            },
        ],
    }
}

struct RunningController {
    endpoint: String,
    pki: Arc<TestPki>,
    shutdown: CancellationToken,
    _handle: tokio::task::JoinHandle<()>,
}

impl RunningController {
    async fn start(
        reconcile_interval: Duration,
        safety: basis_controller::config::SafetyConfig,
    ) -> (Self, basis_controller::db::Db) {
        install_crypto_provider_once().await;

        let db = basis_controller::db::Db::open(":memory:".as_ref(), 1.0)
            .await
            .unwrap();

        let pki = Arc::new(TestPki::new(CONTROLLER_IDENTITY));
        let server_tls = pki.server_tls_config();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = CancellationToken::new();
        let metrics = basis_controller::metrics::Metrics::new(1.0).unwrap();
        let server = basis_controller::server::BasisServer::new(
            db.clone(),
            metrics,
            vec!["1.1.1.1".to_string()],
            test_network_config(),
            basis_controller::config::BgpConfig {
                asn: 64500,
                router_id: "10.0.0.1".to_string(),
                holod_endpoint: "http://127.0.0.1:50051".to_string(),
                instance_name: "basis-test".to_string(),
            },
            safety,
        )
        .with_reconcile_interval(reconcile_interval);
        let server_shutdown = shutdown.clone();

        let handle = tokio::spawn(async move {
            let _ = server.serve(listener, server_tls, server_shutdown).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        (
            Self {
                endpoint: format!("https://{addr}"),
                pki,
                shutdown,
                _handle: handle,
            },
            db,
        )
    }

    fn client_tls(&self, client_cn: &str) -> ClientTlsConfig {
        let (cert_pem, key_pem) = self.pki.leaf(client_cn);
        ClientTlsConfig::new()
            .identity(Identity::from_pem(cert_pem, key_pem))
            .ca_certificate(Certificate::from_pem(self.pki.ca_pem()))
            .domain_name(CONTROLLER_IDENTITY)
    }

    async fn capi_client(&self) -> basis_client::BasisClient<tonic::transport::Channel> {
        let channel = Endpoint::from_shared(self.endpoint.clone())
            .unwrap()
            .tls_config(self.client_tls(CAPI_PROVIDER_IDENTITY))
            .unwrap()
            .connect()
            .await
            .unwrap();
        basis_client::BasisClient::new(channel)
    }

    async fn agent_client(
        &self,
        hostname: &str,
    ) -> basis_agent_client::BasisAgentClient<tonic::transport::Channel> {
        let channel = Endpoint::from_shared(self.endpoint.clone())
            .unwrap()
            .tls_config(self.client_tls(hostname))
            .unwrap()
            .connect()
            .await
            .unwrap();
        basis_agent_client::BasisAgentClient::new(channel)
    }
}

impl Drop for RunningController {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Create a cluster via the CAPI API and return its id + the VIP.
/// `APISERVER_PUBLIC` against the named pool, with the requested
/// trust_domain (`""` for the untagged group). Tests that just want a
/// cluster on the default Lan pool pass `("cell-internal", "")`.
async fn create_cluster(
    running: &RunningController,
    name: &str,
    pool: &str,
    trust_domain: &str,
) -> (String, String) {
    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: name.to_string(),
            external_ip_pool: pool.to_string(),
            external_service_ips: 0,
            apiserver_visibility: ApiserverVisibility::ApiserverPublic as i32,
            trust_domain: trust_domain.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    (resp.cluster_id, resp.control_plane_endpoint)
}

/// Register an agent and consume its RegisterAck, returning the
/// outbound channel, inbound command stream, host_id, and the initial
/// reconcile state the controller sent inline with the ack. Pass
/// `inventory = None` for the common case (fresh agent, no
/// pre-existing kernel state to report).
async fn register_agent(
    running: &RunningController,
    hostname: &str,
    inventory: Option<HostInventory>,
) -> (
    mpsc::Sender<AgentMessage>,
    tonic::Streaming<ControllerCommand>,
    String,
    ReconcileHostCommand,
) {
    let mut client = running.agent_client(hostname).await;
    let (tx, rx) = mpsc::channel::<AgentMessage>(32);

    tx.send(AgentMessage {
        payload: Some(agent_message::Payload::Register(RegisterHostRequest {
            hostname: hostname.to_string(),
            total_cpu: 16,
            total_memory_mib: 65536,
            total_disk_gib: 1000,
            gpus: Vec::new(),
            vtep_address: "10.100.0.1".to_string(),
            rank: 0,
            labels: std::collections::HashMap::new(),
            current_inventory: inventory,
        })),
    })
    .await
    .unwrap();

    let response = client
        .stream_messages(ReceiverStream::new(rx))
        .await
        .unwrap();
    let mut inbound = response.into_inner();

    let ack = match inbound.next().await.unwrap().unwrap().command {
        Some(controller_command::Command::RegisterAck(a)) => a,
        other => panic!("expected RegisterAck, got {:?}", other),
    };
    let initial = ack
        .initial_state
        .expect("RegisterAck must carry initial_state");

    (tx, inbound, ack.host_id, initial)
}

fn basic_machine_req(name: &str, cluster_id: &str) -> CreateMachineRequest {
    CreateMachineRequest {
        cluster_id: cluster_id.to_string(),
        name: name.to_string(),
        cpu: 4,
        memory_mib: 8192,
        disk_gib: 100,
        image: "test-image:latest".to_string(),
        bootstrap_data: b"#!/bin/bash\necho hello".to_vec(),
        gpus: 0,
        gpu_constraints: None,
        extra_disks: Vec::new(),
        placement: None,
    }
}

/// Drive the CreateMachine dance: agent receives CreateVm, reports RUNNING,
/// CreateMachine returns. Returns the CreateMachine response.
async fn drive_create_to_running(
    agent_tx: &mpsc::Sender<AgentMessage>,
    inbound: &mut tonic::Streaming<ControllerCommand>,
    capi: &mut basis_client::BasisClient<tonic::transport::Channel>,
    req: CreateMachineRequest,
) -> CreateMachineResponse {
    let create_handle = {
        let mut capi = capi.clone();
        tokio::spawn(async move { capi.create_machine(req).await })
    };

    let vm_id = expect_create_vm(inbound).await;
    report_vm_state(agent_tx, &vm_id, MachineState::Running, "", false).await;
    create_handle.await.unwrap().unwrap().into_inner()
}

/// Drive a DeleteMachine RPC end-to-end under the tombstone model:
/// the RPC returns immediately, and the controller follows up with a
/// reconcile push carrying `vm_tombstones[]`. Ack the tombstones,
/// then wait until the vm row is fully cleared from the DB.
async fn drive_delete_via_tombstone(
    db: &basis_controller::db::Db,
    agent_tx: &mpsc::Sender<AgentMessage>,
    inbound: &mut tonic::Streaming<ControllerCommand>,
    vm_id: &str,
    delete: impl std::future::Future<Output = Result<tonic::Response<DeleteMachineResponse>, tonic::Status>>
        + Send
        + 'static,
) -> tonic::Response<DeleteMachineResponse> {
    let handle = tokio::spawn(delete);
    let resp = handle.await.unwrap().unwrap();
    consume_tombstones_until_gone(db, agent_tx, inbound, std::iter::once(vm_id.to_string())).await;
    resp
}

/// Consume reconcile pushes from `inbound`, ack any tombstones they
/// carry, until every vm_id in `expected_vms` has been removed from
/// the DB (i.e. `get_vm` returns NotFound). The controller re-emits
/// tombstones on every reconcile until acked, so this loop is
/// bounded by the number of reconcile pushes rather than time.
async fn consume_tombstones_until_gone(
    db: &basis_controller::db::Db,
    agent_tx: &mpsc::Sender<AgentMessage>,
    inbound: &mut tonic::Streaming<ControllerCommand>,
    expected_vms: impl IntoIterator<Item = String>,
) {
    let mut remaining: std::collections::HashSet<String> = expected_vms.into_iter().collect();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let mut still_present: std::collections::HashSet<String> = std::collections::HashSet::new();
        for id in &remaining {
            if db.get_vm(id).await.is_ok() {
                still_present.insert(id.clone());
            }
        }
        remaining = still_present;
        if remaining.is_empty() {
            return;
        }
        let cmd = tokio::time::timeout(std::time::Duration::from_secs(2), inbound.next()).await;
        let cmd = match cmd {
            Ok(Some(Ok(c))) => c,
            _ => continue,
        };
        if let Some(controller_command::Command::ReconcileHost(rc)) = cmd.command {
            if !rc.cluster_tombstones.is_empty() || !rc.vm_tombstones.is_empty() {
                let ack = TombstoneAck {
                    cluster_vnis: rc.cluster_tombstones.iter().map(|t| t.vni).collect(),
                    vm_ids: rc.vm_tombstones.clone(),
                };
                agent_tx
                    .send(AgentMessage {
                        payload: Some(agent_message::Payload::TombstoneAck(ack)),
                    })
                    .await
                    .unwrap();
                // Give the controller a moment to process the ack
                // before the next round-trip.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
    panic!("vms not torn down within deadline: {:?}", remaining);
}

/// Consume inbound commands until we see a CreateVm; return its vm_id.
/// Non-CreateVm commands (e.g. a ReconcileHost push that happens to
/// overlap) are ignored — the controller interleaves the two freely.
async fn expect_create_vm(inbound: &mut tonic::Streaming<ControllerCommand>) -> String {
    loop {
        let cmd = inbound.next().await.unwrap().unwrap();
        match &cmd.command {
            Some(controller_command::Command::CreateVm(c)) => return c.vm_id.clone(),
            Some(controller_command::Command::ReconcileHost(_)) => continue,
            other => panic!("expected CreateVm, got {:?}", other),
        }
    }
}

async fn report_vm_state(
    agent_tx: &mpsc::Sender<AgentMessage>,
    vm_id: &str,
    state: MachineState,
    error_message: &str,
    transient: bool,
) {
    agent_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::VmState(ReportVmStateRequest {
                vm_id: vm_id.to_string(),
                state: state as i32,
                error_message: error_message.to_string(),
                transient,
            })),
        })
        .await
        .unwrap();
}

/// Polls `check` for the full `window`, panicking with `msg` the
/// instant it returns true. Used for negative assertions: "this
/// shouldn't happen", where there's no observable signal you can
/// wait *for* — only a bound on how long you'd plausibly need to
/// wait to be confident it won't happen.
async fn assert_stays_false<F, Fut>(mut check: F, window: Duration, msg: &str)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        if check().await {
            panic!("{msg}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn test_create_cluster_public_apiserver_lands_in_pool() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, vip) = create_cluster(&running, "my-cluster", "cell-internal", "").await;

    assert!(!cluster_id.is_empty());
    // `create_cluster` defaults to APISERVER_PUBLIC against the
    // test pool. Pool is 192.168.100.0/27 → allocatable [.1, .30].
    // First apiserver VIP gets .1, and with default_external_service_ips=4
    // (a /30) the Service block lands at the next aligned /30 (.4/30).
    assert_eq!(vip, "192.168.100.1");

    let mut capi = running.capi_client().await;
    let got = capi
        .get_cluster(GetClusterRequest {
            cluster_id: cluster_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.cluster_id, cluster_id);
    assert_eq!(got.control_plane_endpoint, vip);
    assert_eq!(got.name, "my-cluster");
    assert_eq!(got.cidr, "10.0.0.0/24", "first cluster carves the low /24");
    assert_eq!(
        got.vni, 10_000,
        "first cluster gets the low end of the VNI range"
    );
    assert_eq!(
        got.service_block_cidr, "192.168.100.4/30",
        "service block is the next aligned /30 above the apiserver VIP"
    );
}

#[tokio::test]
async fn test_create_cluster_private_apiserver_lands_at_top_of_cidr() {
    // APISERVER_PRIVATE puts the apiserver VIP at the last usable
    // address of the cluster CIDR, never advertised cell-wide. The
    // LB block still comes from the named pool.
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: "private-cp".to_string(),
            external_ip_pool: "cell-internal".to_string(),
            external_service_ips: 0,
            apiserver_visibility: ApiserverVisibility::ApiserverPrivate as i32,
            trust_domain: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.cidr, "10.0.0.0/24");
    // Last usable in 10.0.0.0/24 is .254 (broadcast - 1).
    assert_eq!(resp.control_plane_endpoint, "10.0.0.254");
    // LB block lands at the first aligned /30 above .1 (network).
    // Pool is .0/27, allocatable [.1, .30]; aligning .1 up to a /30
    // boundary gives .4 — same as the public test even though no
    // apiserver VIP took a pool slot.
    assert_eq!(resp.service_block_cidr, "192.168.100.4/30");
}

#[tokio::test]
async fn test_create_cluster_is_idempotent_by_name() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (first_id, first_vip) = create_cluster(&running, "dup", "cell-internal", "").await;

    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: "dup".to_string(),
            external_ip_pool: "cell-internal".to_string(),
            external_service_ips: 0,
            apiserver_visibility: ApiserverVisibility::ApiserverPublic as i32,
            trust_domain: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.cluster_id, first_id);
    assert_eq!(resp.control_plane_endpoint, first_vip);
}

#[tokio::test]
async fn test_full_create_delete_flow() {
    let (running, db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, _vip) = create_cluster(&running, "test-cluster", "cell-internal", "").await;

    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "test-host-1", None).await;
    let mut capi = running.capi_client().await;

    let resp = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("test-vm", &cluster_id),
    )
    .await;
    // First VM IP in a /24 tree sits just above the bridge range:
    // bridge_reserve=8 reserves .1–.8 for host bridges, so VMs start
    // at .9.
    assert_eq!(resp.ip_address, "10.0.0.9");
    assert!(resp.provider_id.contains(&resp.id));

    let machine = capi
        .get_machine(GetMachineRequest {
            id: resp.id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(machine.name, "test-vm");
    assert_eq!(machine.cluster_id, cluster_id);
    assert_eq!(machine.state, MachineState::Running as i32);

    let list = capi
        .list_machines(ListMachinesRequest {
            cluster_id: cluster_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.machines.len(), 1);

    let vm_id = resp.id.clone();
    let delete_fut = {
        let mut capi = capi.clone();
        let vm_id = vm_id.clone();
        async move {
            capi.delete_machine(DeleteMachineRequest { id: vm_id })
                .await
        }
    };
    drive_delete_via_tombstone(&db, &agent_tx, &mut inbound, &vm_id, delete_fut).await;

    assert!(db.get_vm(&resp.id).await.is_err());
}

#[tokio::test]
async fn test_delete_cluster_cascades_machine_deletes() {
    let (running, db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, doomed_vip) = create_cluster(&running, "doomed", "cell-internal", "").await;
    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "host-a", None).await;
    let mut capi = running.capi_client().await;

    let vm = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("victim", &cluster_id),
    )
    .await;

    let delete_fut = {
        let mut capi = capi.clone();
        let cluster_id = cluster_id.clone();
        async move {
            capi.delete_cluster(DeleteClusterRequest { cluster_id })
                .await
        }
    };
    let _ = tokio::spawn(delete_fut).await.unwrap().unwrap();
    consume_tombstones_until_gone(&db, &agent_tx, &mut inbound, std::iter::once(vm.id.clone()))
        .await;

    assert!(db.get_vm(&vm.id).await.is_err());
    assert_eq!(
        capi.get_cluster(GetClusterRequest {
            cluster_id: cluster_id.clone()
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::NotFound
    );

    // After the last cluster in a tree is deleted, the tree itself
    // is deleted — its VNI and CIDR come back to the free pool
    // immediately. A fresh tree root reclaims them.
    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: "after-delete".to_string(),
            external_ip_pool: "cell-internal".to_string(),
            external_service_ips: 0,
            apiserver_visibility: ApiserverVisibility::ApiserverPublic as i32,
            trust_domain: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.control_plane_endpoint, doomed_vip);
}

#[tokio::test]
async fn test_create_machine_no_agent() {
    let (running, db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, _vip) =
        create_cluster(&running, "no-agent-cluster", "cell-internal", "").await;

    // Pre-seed a host record without a live stream.
    db.upsert_host(&basis_controller::db::HostRow {
        id: "ghost-host".to_string(),
        hostname: "ghost".to_string(),
        total_cpu: 16,
        total_memory_mib: 65536,
        total_disk_gib: 1000,
        gpu_inventory: Vec::new(),
        vtep_address: "10.100.0.99".to_string(),
        last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
        healthy: true,
        rank: 0,
        labels: std::collections::BTreeMap::new(),
    })
    .await
    .unwrap();

    let mut capi = running.capi_client().await;
    let result = capi
        .create_machine(basic_machine_req("orphan-vm", &cluster_id))
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);

    // Tombstone model: rows are kept in PENDING_TEARDOWN until the
    // agent acks. Here no agent ever connected, so the row sits in
    // PENDING_TEARDOWN — representing the controller's intent to
    // release the resources. The next agent reconnect (or operator
    // gc) is what finalises the delete.
    let row = db
        .get_vm_by_name(&cluster_id, "orphan-vm")
        .await
        .unwrap()
        .expect("vm row remains in PENDING_TEARDOWN until acked");
    assert_eq!(
        row.state,
        basis_proto::MachineState::PendingTeardown as i64,
        "failed CreateMachine must mark the VM PENDING_TEARDOWN, not RUNNING/FAILED",
    );
}

#[tokio::test]
async fn test_create_machine_unknown_cluster_fails() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let _agent = register_agent(&running, "any-host", None).await;

    let mut capi = running.capi_client().await;
    let err = capi
        .create_machine(basic_machine_req("orphan", "nonexistent-cluster"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_create_machine_agent_reports_failure() {
    let (running, db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, _vip) = create_cluster(&running, "fail-cluster", "cell-internal", "").await;
    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "fail-host", None).await;

    let capi = running.capi_client().await;
    let create_handle = {
        let mut capi = capi.clone();
        let req = basic_machine_req("fail-vm", &cluster_id);
        tokio::spawn(async move { capi.create_machine(req).await })
    };

    let vm_id = expect_create_vm(&mut inbound).await;

    agent_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::VmState(ReportVmStateRequest {
                vm_id: vm_id.clone(),
                state: MachineState::Failed as i32,
                error_message: "disk image pull failed".to_string(),
                transient: false,
            })),
        })
        .await
        .unwrap();

    let err = create_handle.await.unwrap().unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(err.message().contains("disk image pull failed"));

    // Same model as the no-agent case: the row stays in
    // PENDING_TEARDOWN until the agent acks the resulting tombstone.
    // The mock agent in this test never processes tombstones, so the
    // row remains visible — verify the tombstone-state, not absence.
    let row = db.get_vm(&vm_id).await.expect("vm row remains pending");
    assert_eq!(row.state, basis_proto::MachineState::PendingTeardown as i64,);
}

#[tokio::test]
async fn test_wrong_cn_rejected_from_capi_rpc() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;

    let channel = Endpoint::from_shared(running.endpoint.clone())
        .unwrap()
        .tls_config(running.client_tls("not-the-capi-provider"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = basis_client::BasisClient::new(channel);

    let err = client
        .list_machines(ListMachinesRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn test_capi_cn_cannot_open_agent_stream() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let mut client = running.agent_client(CAPI_PROVIDER_IDENTITY).await;
    let (_tx, rx) = mpsc::channel::<AgentMessage>(1);
    let err = client
        .stream_messages(ReceiverStream::new(rx))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn test_agent_cn_must_match_registered_hostname() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let mut client = running.agent_client("my-hostname").await;
    let (tx, rx) = mpsc::channel::<AgentMessage>(32);

    tx.send(AgentMessage {
        payload: Some(agent_message::Payload::Register(RegisterHostRequest {
            hostname: "some-other-host".to_string(),
            total_cpu: 16,
            total_memory_mib: 65536,
            total_disk_gib: 1000,
            gpus: Vec::new(),
            vtep_address: "10.100.0.1".to_string(),
            rank: 0,
            labels: std::collections::HashMap::new(),
            current_inventory: None,
        })),
    })
    .await
    .unwrap();

    let err = client
        .stream_messages(ReceiverStream::new(rx))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn test_heartbeat_flips_unhealthy_back_to_healthy() {
    let (running, db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (agent_tx, inbound, host_id, _initial) =
        register_agent(&running, "capacity-host", None).await;

    db.mark_host_unhealthy(&host_id).await.unwrap();
    assert!(!db.get_host(&host_id).await.unwrap().healthy);

    agent_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Heartbeat(HeartbeatRequest {})),
        })
        .await
        .unwrap();

    let mut attempts = 0;
    loop {
        if db.get_host(&host_id).await.unwrap().healthy {
            break;
        }
        attempts += 1;
        if attempts > 50 {
            panic!("heartbeat never marked host healthy");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    drop(agent_tx);
    drop(inbound);
}

#[tokio::test]
async fn test_agent_stream_cannot_report_for_other_host() {
    let (running, db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, _vip) =
        create_cluster(&running, "host-scope-cluster", "cell-internal", "").await;
    let (agent_a_tx, mut agent_a_inbound, host_a_id, _initial_a) =
        register_agent(&running, "host-a", None).await;

    let capi = running.capi_client().await;
    let mut create_handle = {
        let mut capi = capi.clone();
        let req = basic_machine_req("scoped-vm", &cluster_id);
        tokio::spawn(async move { capi.create_machine(req).await })
    };

    let vm_id = expect_create_vm(&mut agent_a_inbound).await;
    let (agent_b_tx, agent_b_inbound, _host_b_id, _initial_b) =
        register_agent(&running, "host-b", None).await;

    report_vm_state(&agent_b_tx, &vm_id, MachineState::Running, "", false).await;
    // The timeout doubles as a deterministic processing window: if the
    // wrong-host report were honoured it would resolve `create_handle`
    // (and we'd see Ready inside the 200ms). Failing to resolve in
    // that window is the strong signal that the report was dropped.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut create_handle)
            .await
            .is_err(),
        "CreateMachine should still be waiting for the assigned host"
    );
    assert_eq!(
        db.get_vm(&vm_id).await.unwrap().state,
        MachineState::Creating as i64,
        "VM state report from a different host must be ignored"
    );

    db.mark_host_unhealthy(&host_a_id).await.unwrap();
    agent_b_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Heartbeat(HeartbeatRequest {})),
        })
        .await
        .unwrap();
    // No "rejected" signal observable from the API — assert the wrong-
    // stream heartbeat *cannot* flip health by waiting longer than the
    // controller could plausibly take to process it, then probing.
    // 200ms is generous for an in-process test; CI runners under load
    // occasionally need more.
    assert_stays_false(
        || async { db.get_host(&host_a_id).await.unwrap().healthy },
        Duration::from_millis(200),
        "heartbeat from a different stream must not update host health",
    )
    .await;

    report_vm_state(&agent_a_tx, &vm_id, MachineState::Running, "", false).await;
    create_handle.await.unwrap().unwrap();

    drop(agent_a_tx);
    drop(agent_a_inbound);
    drop(agent_b_tx);
    drop(agent_b_inbound);
}

#[tokio::test]
async fn test_controller_pushes_periodic_reconcile() {
    let (running, _db) = RunningController::start(
        Duration::from_millis(150),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (_agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "reconcile-host", None).await;

    let cmd = tokio::time::timeout(Duration::from_secs(2), inbound.next())
        .await
        .expect("ReconcileHost command never arrived")
        .unwrap()
        .unwrap();
    match cmd.command {
        Some(controller_command::Command::ReconcileHost(r)) => {
            assert!(r.clusters.is_empty(), "no VMs → no cluster membership");
            assert!(r.cluster_tombstones.is_empty());
            assert!(r.vm_tombstones.is_empty());
        }
        other => panic!("expected ReconcileHost, got {other:?}"),
    }
}

#[tokio::test]
async fn test_reconnect_reports_expected_vm_ids_and_clusters() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, _vip) =
        create_cluster(&running, "reconnect-cluster", "cell-internal", "").await;
    let (agent_tx, mut inbound, host_id, initial1) =
        register_agent(&running, "reconnect-host", None).await;
    assert!(initial1.clusters.is_empty());
    assert!(initial1.cluster_tombstones.is_empty());
    assert!(initial1.vm_tombstones.is_empty());

    let mut capi = running.capi_client().await;
    let resp = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("persistent-vm", &cluster_id),
    )
    .await;

    drop(agent_tx);
    drop(inbound);

    let _ = resp;
    let (_tx2, _inbound2, host_id2, initial2) =
        register_agent(&running, "reconnect-host", None).await;
    assert_eq!(host_id2, host_id);
    assert!(initial2.vm_tombstones.is_empty(), "no pending VM teardowns",);
    assert!(
        initial2.cluster_tombstones.is_empty(),
        "no pending cluster teardowns",
    );
    assert_eq!(
        initial2.clusters.len(),
        1,
        "host's active cluster membership rehydrates on reconnect",
    );
    let cluster_state = &initial2.clusters[0];
    assert_eq!(cluster_state.vni, 10_000);
    // VTEP peer list contains this host's own address. (The agent
    // filters itself out client-side before building FDB entries.)
    assert_eq!(cluster_state.vtep_addresses, vec!["10.100.0.1".to_string()]);
}

/// End-to-end verification of the optimistic-concurrency contract:
/// two concurrent `CreateMachine` calls can't silently oversubscribe
/// a host even when they share the same pre-commit snapshot. The
/// winner commits; the loser's retry re-runs `pick_host` against the
/// updated state and — since there's no fallback host in this test —
/// surfaces `ResourceExhausted`. Proves the DB capacity gate catches
/// the race and the server's retry loop classifies the outcome.
#[tokio::test]
async fn test_concurrent_create_cannot_oversubscribe_host() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, _vip) = create_cluster(&running, "race-cluster", "cell-internal", "").await;
    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "race-host", None).await;
    let capi = running.capi_client().await;

    // Host reports 16 cpu in `register_agent`. Two 10-cpu requests
    // can't both fit; strictly one must win.
    let mut req_a = basic_machine_req("vm-a", &cluster_id);
    req_a.cpu = 10;
    let mut req_b = basic_machine_req("vm-b", &cluster_id);
    req_b.cpu = 10;

    let handle_a = {
        let mut capi = capi.clone();
        tokio::spawn(async move { capi.create_machine(req_a).await })
    };
    let handle_b = {
        let mut capi = capi.clone();
        tokio::spawn(async move { capi.create_machine(req_b).await })
    };

    // Only the winner dispatches `CreateVm` to the agent; drive that
    // one to RUNNING. The loser returns without ever touching the
    // agent stream.
    let winning_vm_id = expect_create_vm(&mut inbound).await;
    report_vm_state(&agent_tx, &winning_vm_id, MachineState::Running, "", false).await;

    let result_a = handle_a.await.unwrap();
    let result_b = handle_b.await.unwrap();

    let (winner, loser) = match (&result_a, &result_b) {
        (Ok(_), Err(_)) => (result_a, result_b),
        (Err(_), Ok(_)) => (result_b, result_a),
        other => panic!("exactly one CreateMachine should succeed; got {other:?}"),
    };

    let winner_resp = winner.unwrap().into_inner();
    assert_eq!(winner_resp.id, winning_vm_id);

    let loser_status = loser.unwrap_err();
    assert_eq!(
        loser_status.code(),
        tonic::Code::ResourceExhausted,
        "loser must surface scheduler exhaustion, got: {loser_status:?}"
    );
}

/// Disaster recovery — auto-cleanup mode. With
/// `safety.auto_reconcile_orphan_inventory = true` (operator has
/// deliberately wiped the DB and wants the cell to self-clean on
/// reconnect), the controller diffs the agent's inventory against
/// its empty DB and emits one-shot tombstones for every orphan.
#[tokio::test]
async fn test_register_synthesises_tombstones_when_safety_flag_on() {
    let safety = basis_controller::config::SafetyConfig {
        auto_reconcile_orphan_inventory: true,
    };
    let (running, db) = RunningController::start(Duration::from_secs(60), safety).await;

    let inventory = HostInventory {
        vm_ids: vec!["orphan-vm".to_string()],
        clusters: vec![InventoryCluster {
            vni: 99_999,
            cidr: "10.250.0.0/24".to_string(),
        }],
    };
    let (_tx, _inbound, _host_id, initial) =
        register_agent(&running, "wipe-recovery-host", Some(inventory)).await;

    // Both the orphan VM and the orphan bridge must come back as
    // tombstones in the inline `initial_state` so the agent tears
    // them down before doing anything else.
    assert_eq!(initial.vm_tombstones, vec!["orphan-vm".to_string()]);
    assert_eq!(initial.cluster_tombstones.len(), 1);
    assert_eq!(initial.cluster_tombstones[0].vni, 99_999);
    assert_eq!(initial.cluster_tombstones[0].cidr, "10.250.0.0/24");

    // Synthetic tombstones don't create persistent rows: nothing was
    // pending before, nothing is pending after.
    let pending_clusters = db
        .list_pending_cluster_tombstones("wipe-recovery-host")
        .await
        .unwrap_or_default();
    assert!(
        pending_clusters.is_empty(),
        "synthesised tombstones must not leak DB state",
    );
    assert!(db
        .list_pending_vm_tombstones("wipe-recovery-host")
        .await
        .unwrap_or_default()
        .is_empty());
}

/// Production-safe default: `safety.auto_reconcile_orphan_inventory`
/// is OFF. An agent reporting inventory the controller doesn't
/// recognise — exactly what happens when controller.db has been
/// lost or restored from an old backup — gets an EMPTY tombstone
/// list, NOT auto-cleanup. The agent's bridges + VMs stay alive
/// while the operator inspects, restores the DB, or deliberately
/// flips the flag. This is the safety mechanism that prevents
/// "DB lost ⇒ every VM auto-deleted on reconnect."
#[tokio::test]
async fn test_register_freezes_orphan_inventory_by_default() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;

    let inventory = HostInventory {
        vm_ids: vec!["alive-vm-the-controller-forgot".to_string()],
        clusters: vec![InventoryCluster {
            vni: 99_999,
            cidr: "10.250.0.0/24".to_string(),
        }],
    };
    let (_tx, _inbound, _host_id, initial) =
        register_agent(&running, "frozen-host", Some(inventory)).await;

    assert!(
        initial.vm_tombstones.is_empty(),
        "default safety must NOT tombstone VMs the controller doesn't recognise",
    );
    assert!(
        initial.cluster_tombstones.is_empty(),
        "default safety must NOT tombstone bridges the controller doesn't recognise",
    );
}

/// Inventory entries that DO match the controller's DB are not
/// duplicated as tombstones: the agent's bridge for an active
/// cluster stays in the ACTIVE list, not the cluster_tombstones
/// list. Same for a VM whose vms row is on this host.
#[tokio::test]
async fn test_register_inventory_match_is_not_tombstoned() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, _vip) = create_cluster(&running, "live-cluster", "cell-internal", "").await;
    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "live-host", None).await;
    let mut capi = running.capi_client().await;

    let vm = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("live-vm", &cluster_id),
    )
    .await;
    drop(agent_tx);
    drop(inbound);

    // Agent reconnects reporting accurate inventory matching the DB.
    // Nothing should be tombstoned.
    let inventory = HostInventory {
        vm_ids: vec![vm.id.clone()],
        clusters: vec![InventoryCluster {
            vni: 10_000,
            cidr: "10.0.0.0/24".to_string(),
        }],
    };
    let (_tx2, _inbound2, _, initial2) =
        register_agent(&running, "live-host", Some(inventory)).await;
    assert!(
        initial2.vm_tombstones.is_empty(),
        "matching inventory must not be tombstoned",
    );
    assert!(
        initial2.cluster_tombstones.is_empty(),
        "matching cluster bridge must not be tombstoned",
    );
    assert_eq!(initial2.clusters.len(), 1);
}

/// Reverse-direction inventory reconcile: when the agent's reported
/// `current_inventory` is missing a VM the controller's DB has on
/// this host (host reboot lost the VM, agent crash + restart, etc.),
/// the controller rolls the live VM into the standard teardown
/// pipeline so CAPI sees the death. Gated by the safety flag —
/// freeze when off, reap when on.
#[tokio::test]
async fn test_register_reaps_db_running_orphan_when_safety_on() {
    let safety = basis_controller::config::SafetyConfig {
        auto_reconcile_orphan_inventory: true,
    };
    let (running, db) = RunningController::start(Duration::from_secs(60), safety).await;
    let (cluster_id, _vip) = create_cluster(&running, "rcl", "cell-internal", "").await;
    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "reap-host", None).await;
    let mut capi = running.capi_client().await;

    let vm = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("doomed-vm", &cluster_id),
    )
    .await;
    drop(agent_tx);
    drop(inbound);

    // Reconnect with inventory that LACKS the VM: agent says it
    // doesn't have this VM anymore (host reboot, VM died on the host,
    // etc.). With the safety flag ON, the controller rolls the VM
    // into the teardown pipeline.
    let inventory = HostInventory {
        vm_ids: Vec::new(),
        clusters: Vec::new(),
    };
    let (_tx2, _inbound2, _, initial2) =
        register_agent(&running, "reap-host", Some(inventory)).await;
    assert!(
        initial2.vm_tombstones.iter().any(|id| id == &vm.id),
        "DB-orphan VM must ride a tombstone in the rebuilt initial_state; got: {:?}",
        initial2.vm_tombstones,
    );

    let row = db.get_vm(&vm.id).await.expect("vm row still present");
    assert_eq!(
        row.state,
        basis_proto::MachineState::PendingTeardown as i64,
        "DB-orphan VM must transition to PENDING_TEARDOWN",
    );
}

/// Production-safe default: when the safety flag is OFF, a VM the DB
/// has but the agent doesn't report stays in its current state. Lets
/// the operator investigate before any state change.
#[tokio::test]
async fn test_register_freezes_db_orphans_by_default() {
    let (running, db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;
    let (cluster_id, _vip) = create_cluster(&running, "freeze-cl", "cell-internal", "").await;
    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "freeze-host", None).await;
    let mut capi = running.capi_client().await;

    let vm = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("hopeful-vm", &cluster_id),
    )
    .await;
    drop(agent_tx);
    drop(inbound);

    let inventory = HostInventory {
        vm_ids: Vec::new(),
        clusters: Vec::new(),
    };
    let (_tx2, _inbound2, _, initial2) =
        register_agent(&running, "freeze-host", Some(inventory)).await;
    assert!(
        initial2.vm_tombstones.is_empty(),
        "freeze: no tombstone must be emitted for DB orphans the agent doesn't have",
    );

    let row = db.get_vm(&vm.id).await.expect("vm row still present");
    assert_eq!(
        row.state,
        basis_proto::MachineState::Running as i64,
        "freeze: DB-orphan VM must stay in its prior state for operator review",
    );
}

/// Tree-cluster fan-out + per-tree VRF metadata. Every tree-scoped
/// cluster appears in every host's `clusters[]` (so every host
/// eagerly materialises a bridge for it), but each ClusterState
/// carries its own `trust_domain`. The agent maps `trust_domain` to a
/// per-tree Linux VRF and enslaves the bridge to it; cross-tree
/// traffic dies in the kernel because each VRF table only holds its
/// own tree's routes.
///
/// The host below carries a VM in `alpha` (tenant-a), so `alpha`
/// shows up with a populated `gateway_ip`/`cidr`. `beta` (tenant-b)
/// is fanned out as a ghost (empty `gateway_ip`/`cidr`) and carries
/// `trust_domain = "tenant-b"`. LAN-pool clusters carry an empty
/// `trust_domain` — the agent leaves their bridges in the main
/// routing table.
#[tokio::test]
async fn test_eager_bootstrap_carries_trust_domain_per_cluster() {
    let (running, _db) = RunningController::start(
        Duration::from_secs(60),
        basis_controller::config::SafetyConfig::default(),
    )
    .await;

    let (alpha_id, _) = create_cluster(&running, "alpha", "cell-tree", "tenant-a").await;
    let (beta_id, _) = create_cluster(&running, "beta", "cell-tree", "tenant-b").await;

    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "tenant-a-host", None).await;
    let mut capi = running.capi_client().await;
    let _vm = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("alpha-vm", &alpha_id),
    )
    .await;
    drop(agent_tx);
    drop(inbound);

    let (_tx2, _inbound2, _, initial) = register_agent(&running, "tenant-a-host", None).await;

    let by_id: std::collections::HashMap<String, &basis_proto::ClusterState> = initial
        .clusters
        .iter()
        .map(|c| (c.cluster_id.clone(), c))
        .collect();

    let alpha = by_id.get(&alpha_id).expect("alpha must be in clusters[]");
    assert_eq!(
        alpha.trust_domain, "tenant-a",
        "alpha carries its trust_domain"
    );
    assert!(
        !alpha.gateway_ip.is_empty(),
        "alpha carried by host, has gateway_ip"
    );
    assert!(!alpha.cidr.is_empty(), "alpha carried by host, has cidr");

    let beta = by_id
        .get(&beta_id)
        .expect("beta must be in clusters[] (ghost-bootstrap fans out unconditionally)");
    assert_eq!(
        beta.trust_domain, "tenant-b",
        "beta carries its trust_domain"
    );
    assert!(
        beta.gateway_ip.is_empty(),
        "beta is a ghost — no per-host gateway_ip"
    );
    assert!(
        beta.cidr.is_empty(),
        "beta is a ghost — no cidr (no masquerade)"
    );
}
