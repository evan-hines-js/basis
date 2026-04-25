fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // Box the large variants of the agent command oneof so each
        // queued `ControllerCommand` is pointer-sized instead of
        // dragging a ~300-byte `CreateVmCommand` along every mpsc slot
        // — even when the actual message is a ~24-byte `DeleteVm`.
        // Wire format is unchanged; only the generated Rust type differs.
        .boxed(".basis.v1.ControllerCommand.command.create_vm")
        .boxed(".basis.v1.ControllerCommand.command.reconcile_host")
        .boxed(".basis.v1.ControllerCommand.command.register_ack")
        .compile_protos(&["proto/basis.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/basis.proto");

    Ok(())
}
