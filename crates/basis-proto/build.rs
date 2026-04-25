fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(
            ".basis.v1",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        // Box the large variants of the agent command oneof so each
        // queued `ControllerCommand` is pointer-sized instead of
        // dragging a ~300-byte `CreateVmCommand` along every mpsc slot
        // — even when the actual message is a ~24-byte `DeleteVm`.
        // Wire format is unchanged; only the generated Rust type differs.
        .boxed(".basis.v1.ControllerCommand.command.create_vm")
        .boxed(".basis.v1.ControllerCommand.command.reconcile_host")
        .boxed(".basis.v1.ControllerCommand.command.register_ack")
        .compile_protos(&["proto/basis.proto"], &["proto"])?;

    // holo.proto — the upstream holo daemon's gRPC northbound. Vendored
    // verbatim from holo-routing/holo at the same tag pinned by the
    // ansible installer. We compile a client only — basis is a
    // consumer of holod, never a server.
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&["proto/holo.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/basis.proto");
    println!("cargo:rerun-if-changed=proto/holo.proto");

    Ok(())
}
