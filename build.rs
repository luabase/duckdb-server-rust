use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS")?;
    if target_os == "macos" {
        println!("cargo:rustc-link-lib=dylib=duckdb");
        println!("cargo:rustc-link-search=native=/opt/homebrew/lib"); // Apple Silicon default
        println!("cargo:rustc-link-search=native=/usr/local/lib"); // Intel Mac default
    }

    let out_dir = std::env::var("OUT_DIR")?;
    let proto_root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("proto");
    let proto_file = proto_root.join("health.proto");

    tonic_build::configure()
        .build_server(true)
        .file_descriptor_set_path(format!("{}/health_descriptor.bin", out_dir))
        .compile_protos(&[proto_file], &[proto_root])?;

    Ok(())
}
