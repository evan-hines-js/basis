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
        db.seed_ip_pools(&[basis_controller::config::IpPool {
            name: "default".to_string(),
            cidr: "10.0.10.0/24".to_string(),
            gateway: "10.0.10.1".to_string(),
            vm_range: basis_controller::config::IpRange {
                start: "10.0.10.20".to_string(),
                end: "10.0.10.250".to_string(),
            },
            vip_range: basis_controller::config::IpRange {
                start: "10.0.10.10".to_string(),
                end: "10.0.10.19".to_string(),
            },
        }])
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
            1.0,
        )
        .with_reconcile_interval(reconcile_interval);
        let server_shutdown = shutdown.clone();

        let handle = tokio::spawn(async move {
            let _ = server.serve(listener, server_tls, server_shutdown).await;
        });

        // Give the runtime a moment to start accepting connections.
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

/// Create a cluster via the CAPI API and return its id + the VIP the
/// controller allocated from the pool's vip sub-range.
async fn create_cluster(running: &RunningController, name: &str) -> (String, String) {
    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: name.to_string(),
            ip_pool: "default".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    (resp.cluster_id, resp.control_plane_endpoint)
}

/// Register an agent and consume its RegisterAck. Returns the outbound
/// channel, the inbound command stream, and the ack.
async fn register_agent(
    running: &RunningController,
    hostname: &str,
) -> (
    mpsc::Sender<AgentMessage>,
    tonic::Streaming<ControllerCommand>,
    RegisterHostResponse,
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

    (tx, inbound, ack)
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
/// DeleteMachine returns. Now that teardown is synchronous, every
/// delete-in-a-test needs this to avoid hitting the server-side
/// `DELETE_MACHINE_TIMEOUT`.
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

/// Drive DeleteCluster: consume any number of cascading DeleteVm
/// commands, sending STOPPED for each. Returns once DeleteCluster
/// resolves — which only happens after every cascaded delete is
/// confirmed by the agent.
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
                    other => panic!("expected DeleteVm during cluster cascade, got {:?}", other),
                }
            }
        }
    }
}

async fn expect_create_vm(inbound: &mut tonic::Streaming<ControllerCommand>) -> String {
    let cmd = inbound.next().await.unwrap().unwrap();
    match &cmd.command {
        Some(controller_command::Command::CreateVm(c)) => c.vm_id.clone(),
        other => panic!("expected CreateVm, got {:?}", other),
    }
}

async fn expect_delete_vm(inbound: &mut tonic::Streaming<ControllerCommand>) -> String {
    let cmd = inbound.next().await.unwrap().unwrap();
    match &cmd.command {
        Some(controller_command::Command::DeleteVm(c)) => c.vm_id.clone(),
        other => panic!("expected DeleteVm, got {:?}", other),
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
    // VIP was auto-allocated from the pool's vip sub-range (see the
    // `IpPool` seeded in `start_with_reconcile`: vip_range is
    // 10.0.10.10 – 10.0.10.19).
    assert!(vip.starts_with("10.0.10.1"), "vip={vip}");

    // GetCluster returns the same values.
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
    assert_eq!(got.ip_pool, "default");
    assert_eq!(got.name, "my-cluster");
}

#[tokio::test]
async fn test_create_cluster_is_idempotent_by_name() {
    // CAPI reconcilers retry after partial failures (cluster created in
    // basis but status patch never landed). A second CreateCluster with
    // the same name must return the existing record, not error — and
    // crucially must return the SAME VIP, otherwise the reconciler would
    // see drift between basis and the BasisCluster CR.
    let (running, _db) = RunningController::start().await;
    let (first_id, first_vip) = create_cluster(&running, "dup").await;

    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: "dup".to_string(),
            ip_pool: "default".to_string(),
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
    let (cluster_id, vip) = create_cluster(&running, "test-cluster").await;
    assert!(vip.starts_with("10.0.10."));

    let (agent_tx, mut inbound, _ack) = register_agent(&running, "test-host-1").await;
    let mut capi = running.capi_client().await;

    let resp = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("test-vm", &cluster_id),
    )
    .await;
    // VM allocations come from the pool's vm sub-range (starts at
    // 10.0.10.20 — see the `IpPool` seeded in
    // `start_with_reconcile`).
    assert_eq!(resp.ip_address, "10.0.10.20");
    assert!(resp.provider_id.contains(&resp.id));

    // GetMachine / ListMachines
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

    // Delete. Synchronous on the server side: DeleteMachine blocks
    // until the agent reports STOPPED, so we have to drive the agent
    // to completion.
    let vm_id = resp.id.clone();
    let delete_fut = {
        let mut capi = capi.clone();
        async move {
            capi.delete_machine(DeleteMachineRequest { id: vm_id })
                .await
        }
    };
    drive_delete_to_stopped(&agent_tx, &mut inbound, delete_fut).await;

    // VM row is gone after the agent confirmed.
    assert!(db.get_vm(&resp.id).await.is_err());
}

#[tokio::test]
async fn test_delete_cluster_cascades_machine_deletes() {
    let (running, db) = RunningController::start().await;
    let (cluster_id, doomed_vip) = create_cluster(&running, "doomed").await;
    let (agent_tx, mut inbound, _ack) = register_agent(&running, "host-a").await;
    let mut capi = running.capi_client().await;

    let vm = drive_create_to_running(
        &agent_tx,
        &mut inbound,
        &mut capi,
        basic_machine_req("victim", &cluster_id),
    )
    .await;

    // DeleteCluster is synchronous: it won't return until every
    // cascaded VM's agent has reported STOPPED, so we have to feed
    // the confirmation while the RPC is in flight.
    let delete_fut = {
        let mut capi = capi.clone();
        let cluster_id = cluster_id.clone();
        async move {
            capi.delete_cluster(DeleteClusterRequest { cluster_id })
                .await
        }
    };
    drive_delete_cluster_to_stopped(&agent_tx, &mut inbound, delete_fut).await;

    // VM row is gone once the cascade completed.
    assert!(db.get_vm(&vm.id).await.is_err());

    // Cluster row is gone.
    assert_eq!(
        capi.get_cluster(GetClusterRequest {
            cluster_id: cluster_id.clone()
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::NotFound
    );

    // VIP is back in the pool — the next cluster we create reclaims the
    // same address, proving `release_ips` ran during cluster teardown.
    // (Both clusters were the only VIP holders and the sub-range
    // allocator picks the lowest free address.)
    let mut capi = running.capi_client().await;
    let resp = capi
        .create_cluster(CreateClusterRequest {
            name: "reclaim".to_string(),
            ip_pool: "default".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.control_plane_endpoint, doomed_vip);
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
    let (agent_tx, mut inbound, _ack) = register_agent(&running, "fail-host").await;

    let capi = running.capi_client().await;
    let create_handle = {
        let mut capi = capi.clone();
        let req = basic_machine_req("fail-vm", &cluster_id);
        tokio::spawn(async move { capi.create_machine(req).await })
    };

    let cmd = inbound.next().await.unwrap().unwrap();
    let vm_id = match &cmd.command {
        Some(controller_command::Command::CreateVm(c)) => c.vm_id.clone(),
        _ => panic!("expected CreateVm"),
    };

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
    // The CAPI provider's CN is authorised for CAPI RPCs only. Opening the
    // agent stream with that CN must be rejected before any register
    // message is inspected.
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
    let (agent_tx, inbound, ack) = register_agent(&running, "capacity-host").await;

    // Simulate a blip: the health checker has marked this host unhealthy.
    // A fresh heartbeat should flip it back.
    db.mark_host_unhealthy(&ack.host_id).await.unwrap();
    assert!(!db.get_host(&ack.host_id).await.unwrap().healthy);

    agent_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Heartbeat(HeartbeatRequest {
                host_id: ack.host_id.clone(),
            })),
        })
        .await
        .unwrap();

    let mut attempts = 0;
    loop {
        if db.get_host(&ack.host_id).await.unwrap().healthy {
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
    // Closes the staleness gap: while the agent is already connected, the
    // controller periodically pushes the authoritative VM list so a VM
    // that was deleted but whose DeleteVm never landed still gets cleaned
    // up without waiting for the agent to reconnect.
    let (running, _db) = RunningController::start_with_reconcile(Duration::from_millis(150)).await;
    let (_agent_tx, mut inbound, _ack) = register_agent(&running, "reconcile-host").await;

    // First ReconcileHostCommand must arrive within a few intervals. With
    // no VMs on this host, it should carry an empty list.
    let cmd = tokio::time::timeout(Duration::from_secs(2), inbound.next())
        .await
        .expect("ReconcileHost command never arrived")
        .unwrap()
        .unwrap();
    match cmd.command {
        Some(controller_command::Command::ReconcileHost(r)) => {
            assert!(r.expected_vm_ids.is_empty());
        }
        other => panic!("expected ReconcileHost, got {other:?}"),
    }
}

#[tokio::test]
async fn test_reconnect_reports_expected_vm_ids() {
    let (running, _db) = RunningController::start().await;
    let (cluster_id, _vip) = create_cluster(&running, "reconnect-cluster").await;
    let (agent_tx, mut inbound, ack1) = register_agent(&running, "reconnect-host").await;
    assert!(ack1.expected_vm_ids.is_empty());

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

    let (_tx2, _inbound2, ack2) = register_agent(&running, "reconnect-host").await;
    assert_eq!(ack2.host_id, ack1.host_id);
    assert_eq!(ack2.expected_vm_ids, vec![resp.id]);
}
