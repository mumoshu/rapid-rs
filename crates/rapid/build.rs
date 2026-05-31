use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto/rapid.proto");
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/rapid.proto"], &["proto"])?;
    Ok(())
}
