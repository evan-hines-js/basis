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

    // gobgp.proto and friends — vendored verbatim from osrg/gobgp v4.4.0.
    // Imported as `package api` (GoBGP's choice). Client-only.
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                "proto/gobgp/api/attribute.proto",
                "proto/gobgp/api/capability.proto",
                "proto/gobgp/api/common.proto",
                "proto/gobgp/api/extcom.proto",
                "proto/gobgp/api/gobgp.proto",
                "proto/gobgp/api/nlri.proto",
            ],
            &["proto/gobgp"],
        )?;

    println!("cargo:rerun-if-changed=proto/basis.proto");
    println!("cargo:rerun-if-changed=proto/gobgp");

    Ok(())
}
