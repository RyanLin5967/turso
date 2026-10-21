fn main() {
    napi_build::setup();
    // The napi_* symbols are node's, resolved when it loads this library, so on linux-gnu this
    // package's own targets (the cdylib and its test binaries) link with them unresolved. Scoped
    // here, never workspace-wide (engine-gettid-rev MED-1: workspace-wide it hid every undefined
    // symbol, libc::gettid on glibc 2.28 among them).
    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    if os == "linux" && env == "gnu" {
        println!("cargo:rustc-link-arg=-Wl,--warn-unresolved-symbols");
    }
}
