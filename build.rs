fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS")?;
    if target_os == "macos" {
        println!("cargo:rustc-link-lib=dylib=duckdb");
        println!("cargo:rustc-link-search=native=/opt/homebrew/lib"); // Apple Silicon default
        println!("cargo:rustc-link-search=native=/usr/local/lib"); // Intel Mac default
    }
    Ok(())
}
