use std::time::Duration;

use basis_proto::*;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

/// Spin up a controller with an in-memory DB, simulate an agent stream,
/// then exercise CreateMachine → agent reports RUNNING → response returned,
/// followed by DeleteMachine.
#[tokio::test]
async fn test_full_create_delete_flow() {
    // --- Set up controller ---
    let db = basis_controller::db::Db::open(":memory:".as_ref())
        .await
        .unwrap();

    let pool_config = basis_controller::config::IpPoolConfig {
        name: "default".to_string(),
        cidr: "10.0.10.0/24".to_string(),
        gateway: "10.0.10.1".to_string(),
        range_start: "10.0.10.10".to_string(),
        range_end: "10.0.10.250".to_string(),
    };
    basis_controller::ip::seed_ip_pools(&db, &[pool_config])
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let server = basis_controller::server::BasisServer::new(db.clone());
    let addr = server.serve_insecure(shutdown.clone()).await.unwrap();

    // Give the server a moment to start accepting connections
    tokio::time::sleep(Duration::from_millis(50)).await;

    let endpoint = format!("http://{addr}");

    // --- Simulate an agent connecting ---
    let agent_channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut agent_client = basis_agent_client::BasisAgentClient::new(agent_channel);

    let (agent_tx, agent_rx) = mpsc::channel::<AgentMessage>(32);

    // Send RegisterHost as first message
    agent_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Register(RegisterHostRequest {
                hostname: "test-host-1".to_string(),
                total_cpu: 16,
                total_memory_mib: 65536,
                total_disk_gib: 1000,
                gpus: Vec::new(),
                iommu_groups: Vec::new(),
            })),
        })
        .await
        .unwrap();

    let outbound = ReceiverStream::new(agent_rx);
    let response = agent_client.stream_messages(outbound).await.unwrap();
    let mut inbound = response.into_inner();

    // Wait for registration ack
    let ack = inbound.next().await.unwrap().unwrap();
    let host_id = match ack.command {
        Some(controller_command::Command::RegisterAck(r)) => r.host_id,
        other => panic!("expected RegisterHostResponse, got {:?}", other),
    };
    assert!(!host_id.is_empty());

    // --- Create a machine via the CAPI API ---
    let capi_channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut capi_client = basis_client::BasisClient::new(capi_channel);

    // Spawn the CreateMachine call in a task (it blocks until agent reports RUNNING)
    let create_handle = tokio::spawn(async move {
        capi_client
            .create_machine(CreateMachineRequest {
                name: "test-vm".to_string(),
                cluster: "test-cluster".to_string(),
                cpu: 4,
                memory_mib: 8192,
                disk_gib: 100,
                image: "test-image:latest".to_string(),
                bootstrap_data: b"#!/bin/bash\necho hello".to_vec(),
                gpus: 0,
                gpu_constraints: None,
                ip_pool: "default".to_string(),
            })
            .await
    });

    // Agent receives CreateVM command
    let create_cmd = inbound.next().await.unwrap().unwrap();
    let vm_id = match &create_cmd.command {
        Some(controller_command::Command::CreateVm(cmd)) => {
            assert_eq!(cmd.name, "test-vm");
            assert_eq!(cmd.cpu, 4);
            assert_eq!(cmd.memory_mib, 8192);
            assert_eq!(cmd.ip_address, "10.0.10.10"); // First IP in pool
            assert_eq!(cmd.gateway, "10.0.10.1");
            assert_eq!(cmd.prefix_len, 24);
            cmd.vm_id.clone()
        }
        other => panic!("expected CreateVmCommand, got {:?}", other),
    };

    // Agent reports VM as RUNNING
    agent_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::VmState(ReportVmStateRequest {
                vm_id: vm_id.clone(),
                state: MachineState::Running as i32,
                error_message: String::new(),
            })),
        })
        .await
        .unwrap();

    // CreateMachine should now return
    let create_response = create_handle.await.unwrap().unwrap().into_inner();
    assert_eq!(create_response.id, vm_id);
    assert_eq!(create_response.ip_address, "10.0.10.10");
    assert!(create_response.provider_id.contains(&vm_id));
    assert!(!create_response.host.is_empty());

    // --- Verify GetMachine ---
    let capi_channel2 = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut capi_client2 = basis_client::BasisClient::new(capi_channel2);

    let machine = capi_client2
        .get_machine(GetMachineRequest {
            id: vm_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(machine.name, "test-vm");
    assert_eq!(machine.cluster, "test-cluster");
    assert_eq!(machine.state, MachineState::Running as i32);
    assert_eq!(machine.ip_address, "10.0.10.10");

    // --- Verify ListMachines ---
    let list = capi_client2
        .list_machines(ListMachinesRequest {
            cluster: "test-cluster".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.machines.len(), 1);

    // --- Delete the machine ---
    let delete_response = capi_client2
        .delete_machine(DeleteMachineRequest {
            id: vm_id.clone(),
        })
        .await;
    assert!(delete_response.is_ok());

    // Agent receives DeleteVM command
    let delete_cmd = inbound.next().await.unwrap().unwrap();
    match &delete_cmd.command {
        Some(controller_command::Command::DeleteVm(cmd)) => {
            assert_eq!(cmd.vm_id, vm_id);
        }
        other => panic!("expected DeleteVmCommand, got {:?}", other),
    };

    // Verify VM is gone
    let get_result = capi_client2
        .get_machine(GetMachineRequest { id: vm_id })
        .await;
    assert!(get_result.is_err());
    assert_eq!(
        get_result.unwrap_err().code(),
        tonic::Code::NotFound
    );

    // Verify IP was released — creating another VM should get the same IP
    let create2_handle = {
        let endpoint = endpoint.clone();
        let _agent_tx = agent_tx.clone();
        tokio::spawn(async move {
            let ch = tonic::transport::Endpoint::from_shared(endpoint)
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut client = basis_client::BasisClient::new(ch);
            client
                .create_machine(CreateMachineRequest {
                    name: "test-vm-2".to_string(),
                    cluster: "test-cluster".to_string(),
                    cpu: 2,
                    memory_mib: 4096,
                    disk_gib: 50,
                    image: "test-image:latest".to_string(),
                    bootstrap_data: Vec::new(),
                    gpus: 0,
                    gpu_constraints: None,
                    ip_pool: "default".to_string(),
                })
                .await
        })
    };

    // Agent receives second CreateVM
    let create_cmd2 = inbound.next().await.unwrap().unwrap();
    let vm_id2 = match &create_cmd2.command {
        Some(controller_command::Command::CreateVm(cmd)) => {
            // Should get 10.0.10.10 again (released after delete)
            assert_eq!(cmd.ip_address, "10.0.10.10");
            cmd.vm_id.clone()
        }
        other => panic!("expected CreateVmCommand, got {:?}", other),
    };

    agent_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::VmState(ReportVmStateRequest {
                vm_id: vm_id2,
                state: MachineState::Running as i32,
                error_message: String::new(),
            })),
        })
        .await
        .unwrap();

    let resp2 = create2_handle.await.unwrap().unwrap().into_inner();
    assert_eq!(resp2.ip_address, "10.0.10.10");

    // Clean up
    shutdown.cancel();
}

/// Test that CreateMachine fails gracefully when no agent is connected.
#[tokio::test]
async fn test_create_machine_no_agent() {
    let db = basis_controller::db::Db::open(":memory:".as_ref())
        .await
        .unwrap();

    let pool_config = basis_controller::config::IpPoolConfig {
        name: "default".to_string(),
        cidr: "10.0.10.0/24".to_string(),
        gateway: "10.0.10.1".to_string(),
        range_start: "10.0.10.10".to_string(),
        range_end: "10.0.10.15".to_string(),
    };
    basis_controller::ip::seed_ip_pools(&db, &[pool_config])
        .await
        .unwrap();

    // Insert a host record directly (simulating a previously-registered agent that's now gone)
    db.upsert_host(&basis_controller::db::HostRow {
        id: "ghost-host".to_string(),
        hostname: "ghost".to_string(),
        address: String::new(),
        total_cpu: 16,
        total_memory_mib: 65536,
        total_disk_gib: 1000,
        available_cpu: 16,
        available_memory_mib: 65536,
        available_disk_gib: 1000,
        gpu_inventory: "[]".to_string(),
        last_heartbeat: "2025-01-01T00:00:00Z".to_string(),
        healthy: true,
    })
    .await
    .unwrap();

    let shutdown = CancellationToken::new();
    let server = basis_controller::server::BasisServer::new(db);
    let addr = server.serve_insecure(shutdown.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = basis_client::BasisClient::new(channel);

    let result = client
        .create_machine(CreateMachineRequest {
            name: "orphan-vm".to_string(),
            cluster: "test".to_string(),
            cpu: 2,
            memory_mib: 4096,
            disk_gib: 50,
            image: "test:latest".to_string(),
            bootstrap_data: Vec::new(),
            gpus: 0,
            gpu_constraints: None,
            ip_pool: "default".to_string(),
        })
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);

    shutdown.cancel();
}

/// Test that CreateMachine returns FAILED when the agent reports failure.
#[tokio::test]
async fn test_create_machine_agent_reports_failure() {
    let db = basis_controller::db::Db::open(":memory:".as_ref())
        .await
        .unwrap();

    let pool_config = basis_controller::config::IpPoolConfig {
        name: "default".to_string(),
        cidr: "10.0.10.0/24".to_string(),
        gateway: "10.0.10.1".to_string(),
        range_start: "10.0.10.10".to_string(),
        range_end: "10.0.10.15".to_string(),
    };
    basis_controller::ip::seed_ip_pools(&db, &[pool_config])
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let server = basis_controller::server::BasisServer::new(db.clone());
    let addr = server.serve_insecure(shutdown.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let endpoint = format!("http://{addr}");

    // Connect agent
    let agent_ch = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut agent_client = basis_agent_client::BasisAgentClient::new(agent_ch);

    let (agent_tx, agent_rx) = mpsc::channel::<AgentMessage>(32);
    agent_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Register(RegisterHostRequest {
                hostname: "fail-host".to_string(),
                total_cpu: 8,
                total_memory_mib: 32768,
                total_disk_gib: 500,
                gpus: Vec::new(),
                iommu_groups: Vec::new(),
            })),
        })
        .await
        .unwrap();

    let response = agent_client
        .stream_messages(ReceiverStream::new(agent_rx))
        .await
        .unwrap();
    let mut inbound = response.into_inner();

    // Consume registration ack
    let _ = inbound.next().await.unwrap().unwrap();

    // Create machine
    let capi_ch = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut capi_client = basis_client::BasisClient::new(capi_ch);

    let create_handle = tokio::spawn(async move {
        capi_client
            .create_machine(CreateMachineRequest {
                name: "fail-vm".to_string(),
                cluster: "test".to_string(),
                cpu: 2,
                memory_mib: 4096,
                disk_gib: 50,
                image: "test:latest".to_string(),
                bootstrap_data: Vec::new(),
                gpus: 0,
                gpu_constraints: None,
                ip_pool: "default".to_string(),
            })
            .await
    });

    // Agent receives command and reports FAILED
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
            })),
        })
        .await
        .unwrap();

    // CreateMachine should return an error
    let result = create_handle.await.unwrap();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(err.message().contains("disk image pull failed"));

    // VM record and IP should be cleaned up
    let vm_result = db.get_vm(&vm_id).await;
    assert!(vm_result.is_err(), "VM record should be deleted after failure");

    shutdown.cancel();
}
