fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["proto/basis.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/basis.proto");

    Ok(())
}
