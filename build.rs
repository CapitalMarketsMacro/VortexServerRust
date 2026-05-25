//! Emits the `solace_enabled` cfg.
//!
//! The Solace ingress depends on `solace-rs` / `solace-rs-sys`, whose build
//! script panics on Windows (`panic!("Windows currently not supported")`).
//! Those deps are therefore target-gated out of Cargo.toml on Windows, so the
//! `solace` feature being on is not sufficient to know the deps are present.
//!
//! We expose a single derived cfg, `solace_enabled`, true only when the
//! `solace` feature is enabled AND the target OS is not Windows. All Solace
//! ingress code keys off this cfg, so a Windows build cleanly compiles Solace
//! out instead of failing to link a crate that cannot exist there.
fn main() {
    println!("cargo::rustc-check-cfg=cfg(solace_enabled)");

    let feature_on = std::env::var_os("CARGO_FEATURE_SOLACE").is_some();
    let is_windows = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows");

    if feature_on && !is_windows {
        println!("cargo::rustc-cfg=solace_enabled");
    }
}
