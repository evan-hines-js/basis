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

/// Small test-scale network: /24 per tree keeps VM IPs readable
/// (10.0.0.2, .3, …) while leaving room for a handful of VIPs up top.
fn test_network_config() -> basis_controller::config::NetworkConfig {
    basis_controller::config::NetworkConfig {
        tree_supernet: "10.0.0.0/8".to_string(),
        tree_prefix: 24,
        vip_reserve: 16,
        vni_range: basis_controller::config::VniRange {
            start: 10_000,
            end: 11_000,
        },
        vni_cooldown_secs: 60,
        edge_pool: basis_controller::config::EdgePool {
            cidr: "192.168.100.0/24".to_string(),
            gateway: "192.168.100.1".to_string(),
            range_start: "192.168.100.20".to_string(),
            range_end: "192.168.100.100".to_string(),
        },
    }
}

struct RunningController {
    endpoint: String,
    pki: Arc<TestPki>,
    shutdown: CancellationToken,
    _handle: tokio::task::JoinHandle<()>,
}

impl RunningController {
    async fn start() -> (Self, basis_controller::db::Db) {
        Self::start_with_reconcile(Duration::from_secs(60)).await
    }

    async fn start_with_reconcile(
        reconcile_interval: Duration,
    ) -> (Self, basis_controller::db::Db) {
        install_crypto_provider_once().await;

        let db = basis_controller::db::Db::open(":memory:".as_ref())
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
            1.0,
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
async fn create_cluster(running: &RunningController, name: &str) -> (String, String) {
    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: name.to_string(),
            parent_cluster_id: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    (resp.cluster_id, resp.control_plane_endpoint)
}

/// Register an agent and consume its RegisterAck, returning the
/// outbound channel, inbound command stream, host_id, and the initial
/// reconcile state the controller sent inline with the ack.
async fn register_agent(
    running: &RunningController,
    hostname: &str,
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
        edge: false,
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

/// Drive DeleteMachine: agent receives DeleteVm, reports STOPPED,
/// DeleteMachine returns.
async fn drive_delete_to_stopped(
    agent_tx: &mpsc::Sender<AgentMessage>,
    inbound: &mut tonic::Streaming<ControllerCommand>,
    delete: impl std::future::Future<Output = Result<tonic::Response<DeleteMachineResponse>, tonic::Status>>
        + Send
        + 'static,
) -> tonic::Response<DeleteMachineResponse> {
    let handle = tokio::spawn(delete);
    let vm_id = expect_delete_vm(inbound).await;
    report_vm_state(agent_tx, &vm_id, MachineState::Stopped, "", false).await;
    handle.await.unwrap().unwrap()
}

/// Drive DeleteCluster: consume cascading DeleteVm + any interleaved
/// ReconcileHost pushes, sending STOPPED for each delete.
async fn drive_delete_cluster_to_stopped(
    agent_tx: &mpsc::Sender<AgentMessage>,
    inbound: &mut tonic::Streaming<ControllerCommand>,
    delete: impl std::future::Future<Output = Result<tonic::Response<DeleteClusterResponse>, tonic::Status>>
        + Send
        + 'static,
) -> tonic::Response<DeleteClusterResponse> {
    let handle = tokio::spawn(delete);
    tokio::pin!(handle);
    loop {
        tokio::select! {
            biased;
            result = &mut handle => {
                return result.unwrap().unwrap();
            }
            cmd = inbound.next() => {
                let cmd = cmd.unwrap().unwrap();
                match &cmd.command {
                    Some(controller_command::Command::DeleteVm(c)) => {
                        let vm_id = c.vm_id.clone();
                        report_vm_state(agent_tx, &vm_id, MachineState::Stopped, "", false).await;
                    }
                    Some(controller_command::Command::ReconcileHost(_)) => {
                        // Membership update; nothing for the test to do.
                    }
                    other => panic!("expected DeleteVm during cluster cascade, got {:?}", other),
                }
            }
        }
    }
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

async fn expect_delete_vm(inbound: &mut tonic::Streaming<ControllerCommand>) -> String {
    loop {
        let cmd = inbound.next().await.unwrap().unwrap();
        match &cmd.command {
            Some(controller_command::Command::DeleteVm(c)) => return c.vm_id.clone(),
            Some(controller_command::Command::ReconcileHost(_)) => continue,
            other => panic!("expected DeleteVm, got {:?}", other),
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

#[tokio::test]
async fn test_create_cluster_reserves_vip() {
    let (running, _db) = RunningController::start().await;
    let (cluster_id, vip) = create_cluster(&running, "my-cluster").await;

    assert!(!cluster_id.is_empty());
    // First tree carves 10.0.0.0/24; VIPs sit at the top. First VIP =
    // broadcast(10.0.0.255) - 1 - (vip_reserve - 1) = 10.0.0.239.
    assert_eq!(vip, "10.0.0.239");

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
    assert!(got.parent_cluster_id.is_empty(), "tree root has no parent");
    assert_ne!(got.tree_id, "");
    assert_eq!(got.vni, 10_000, "first tree gets the low end of the VNI range");
}

#[tokio::test]
async fn test_child_cluster_inherits_parent_tree() {
    let (running, _db) = RunningController::start().await;
    let mut capi = running.capi_client().await;
    let root = capi
        .create_cluster(CreateClusterRequest {
            name: "root".to_string(),
            parent_cluster_id: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let child = capi
        .create_cluster(CreateClusterRequest {
            name: "child".to_string(),
            parent_cluster_id: root.cluster_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(child.tree_id, root.tree_id, "child shares parent's tree");
    assert_eq!(child.vni, root.vni, "child shares parent's VNI");
    assert_ne!(
        child.control_plane_endpoint, root.control_plane_endpoint,
        "each cluster gets its own VIP"
    );
}

#[tokio::test]
async fn test_create_cluster_is_idempotent_by_name() {
    let (running, _db) = RunningController::start().await;
    let (first_id, first_vip) = create_cluster(&running, "dup").await;

    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: "dup".to_string(),
            parent_cluster_id: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.cluster_id, first_id);
    assert_eq!(resp.control_plane_endpoint, first_vip);
}

#[tokio::test]
async fn test_full_create_delete_flow() {
    let (running, db) = RunningController::start().await;
    let (cluster_id, _vip) = create_cluster(&running, "test-cluster").await;

    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "test-host-1").await;
    let mut capi = running.capi_client().await;

    let resp = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("test-vm", &cluster_id),
    )
    .await;
    // First VM IP in a /24 tree = gateway + 1 = 10.0.0.2.
    assert_eq!(resp.ip_address, "10.0.0.2");
    assert!(resp.provider_id.contains(&resp.id));
    assert!(resp.edge_ip.is_empty(), "non-edge machine gets no second IP");

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
    assert!(!machine.edge);

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
        async move {
            capi.delete_machine(DeleteMachineRequest { id: vm_id })
                .await
        }
    };
    drive_delete_to_stopped(&agent_tx, &mut inbound, delete_fut).await;

    assert!(db.get_vm(&resp.id).await.is_err());
}

#[tokio::test]
async fn test_edge_machine_gets_second_nic_ip() {
    let (running, _db) = RunningController::start().await;
    let (cluster_id, _vip) = create_cluster(&running, "edge-cluster").await;
    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "edge-host").await;
    let mut capi = running.capi_client().await;

    let mut req = basic_machine_req("edge-vm", &cluster_id);
    req.edge = true;
    let resp = drive_create_to_running(&agent_tx, &mut inbound, &mut capi, req).await;

    // Tree-side IP is the first in the VM range; edge IP is the first
    // in the edge pool's range.
    assert_eq!(resp.ip_address, "10.0.0.2");
    assert_eq!(resp.edge_ip, "192.168.100.20");

    let machine = capi
        .get_machine(GetMachineRequest { id: resp.id })
        .await
        .unwrap()
        .into_inner();
    assert!(machine.edge);
    assert_eq!(machine.edge_ip, "192.168.100.20");
}

#[tokio::test]
async fn test_delete_cluster_cascades_machine_deletes() {
    let (running, db) = RunningController::start().await;
    let (cluster_id, doomed_vip) = create_cluster(&running, "doomed").await;
    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "host-a").await;
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
    drive_delete_cluster_to_stopped(&agent_tx, &mut inbound, delete_fut).await;

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

    // The tree's VIP is still in VNI-cooldown (no room for reuse) —
    // but the sub-range itself is a per-tree allocation and the tree
    // row persists until cooldown expires. The next cluster is a
    // fresh tree on the next VNI and a different VIP entirely. This
    // is a behaviour change from the single-pool model.
    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: "after-delete".to_string(),
            parent_cluster_id: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_ne!(
        resp.control_plane_endpoint, doomed_vip,
        "new tree gets its own CIDR and VIP space"
    );
}

#[tokio::test]
async fn test_create_machine_no_agent() {
    let (running, db) = RunningController::start().await;
    let (cluster_id, _vip) = create_cluster(&running, "no-agent-cluster").await;

    // Pre-seed a host record without a live stream.
    db.upsert_host(&basis_controller::db::HostRow {
        id: "ghost-host".to_string(),
        hostname: "ghost".to_string(),
        total_cpu: 16,
        total_memory_mib: 65536,
        total_disk_gib: 1000,
        gpu_inventory: "[]".to_string(),
        vtep_address: "10.100.0.99".to_string(),
        last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
        healthy: true,
    })
    .await
    .unwrap();

    let mut capi = running.capi_client().await;
    let result = capi
        .create_machine(basic_machine_req("orphan-vm", &cluster_id))
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
}

#[tokio::test]
async fn test_create_machine_unknown_cluster_fails() {
    let (running, _db) = RunningController::start().await;
    let _agent = register_agent(&running, "any-host").await;

    let mut capi = running.capi_client().await;
    let err = capi
        .create_machine(basic_machine_req("orphan", "nonexistent-cluster"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_create_machine_agent_reports_failure() {
    let (running, db) = RunningController::start().await;
    let (cluster_id, _vip) = create_cluster(&running, "fail-cluster").await;
    let (agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "fail-host").await;

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

    assert!(
        db.get_vm(&vm_id).await.is_err(),
        "VM record should be deleted after failure"
    );
}

#[tokio::test]
async fn test_wrong_cn_rejected_from_capi_rpc() {
    let (running, _db) = RunningController::start().await;

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
    let (running, _db) = RunningController::start().await;
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
    let (running, _db) = RunningController::start().await;
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
    let (running, db) = RunningController::start().await;
    let (agent_tx, inbound, host_id, _initial) =
        register_agent(&running, "capacity-host").await;

    db.mark_host_unhealthy(&host_id).await.unwrap();
    assert!(!db.get_host(&host_id).await.unwrap().healthy);

    agent_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Heartbeat(HeartbeatRequest {
                host_id: host_id.clone(),
            })),
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
async fn test_controller_pushes_periodic_reconcile() {
    let (running, _db) = RunningController::start_with_reconcile(Duration::from_millis(150)).await;
    let (_agent_tx, mut inbound, _host_id, _initial) =
        register_agent(&running, "reconcile-host").await;

    let cmd = tokio::time::timeout(Duration::from_secs(2), inbound.next())
        .await
        .expect("ReconcileHost command never arrived")
        .unwrap()
        .unwrap();
    match cmd.command {
        Some(controller_command::Command::ReconcileHost(r)) => {
            assert!(r.expected_vm_ids.is_empty());
            assert!(r.trees.is_empty(), "no VMs → no tree membership");
        }
        other => panic!("expected ReconcileHost, got {other:?}"),
    }
}

#[tokio::test]
async fn test_reconnect_reports_expected_vm_ids_and_trees() {
    let (running, _db) = RunningController::start().await;
    let (cluster_id, _vip) = create_cluster(&running, "reconnect-cluster").await;
    let (agent_tx, mut inbound, host_id, initial1) =
        register_agent(&running, "reconnect-host").await;
    assert!(initial1.expected_vm_ids.is_empty());
    assert!(initial1.trees.is_empty());

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

    let (_tx2, _inbound2, host_id2, initial2) = register_agent(&running, "reconnect-host").await;
    assert_eq!(host_id2, host_id);
    assert_eq!(initial2.expected_vm_ids, vec![resp.id]);
    assert_eq!(initial2.trees.len(), 1, "host has one tree's worth of VMs");
    let tree_state = &initial2.trees[0];
    assert_eq!(tree_state.vni, 10_000);
    // VTEP peer list contains this host's own address. (The agent
    // filters itself out client-side before building FDB entries.)
    assert_eq!(tree_state.vtep_addresses, vec!["10.100.0.1".to_string()]);
}
