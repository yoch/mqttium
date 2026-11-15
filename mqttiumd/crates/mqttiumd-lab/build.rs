//! Captures the parameters this binary is compiled with, so that a run
//! manifest describes the binary that runs rather than the tools that
//! happen to be installed when it runs (DEM-003).

use std::env;
use std::process::Command;

fn main() {
    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let version = Command::new(&rustc)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=MQTTIUMD_LAB_BUILD_RUSTC={version}");
    for name in ["TARGET", "PROFILE", "OPT_LEVEL"] {
        let value = env::var(name).unwrap_or_else(|_| "unknown".to_owned());
        println!("cargo:rustc-env=MQTTIUMD_LAB_BUILD_{name}={value}");
    }
    // Cargo separates the flags with the unit separator; a space reads better
    // and the manifest only records them.
    let flags = env::var("CARGO_ENCODED_RUSTFLAGS")
        .unwrap_or_default()
        .replace('\u{1f}', " ");
    println!("cargo:rustc-env=MQTTIUMD_LAB_BUILD_RUSTFLAGS={flags}");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
}
