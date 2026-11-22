//! Captures the parameters this binary is compiled with, so that a run
//! manifest describes the binary that runs rather than the tools that
//! happen to be installed when it runs (DEM-003).

use std::env;
use std::process::Command;

/// What a remapped prefix is recorded as coming from.
const REMAPPED_FROM: &str = "<source>";

/// The flags as the manifest records them: every flag as it stands, but
/// the directory a `--remap-path-prefix` maps from. That directory is
/// where the source happened to be unpacked; recorded, it would enter the
/// binary, and two builds of one source in two directories would be two
/// binaries. What the prefix is mapped to stays.
fn recorded<'a>(flags: impl Iterator<Item = &'a str>) -> String {
    let mut out = Vec::new();
    let mut remap_follows = false;
    for flag in flags {
        let mapping = if remap_follows {
            Some(("", flag))
        } else {
            flag.strip_prefix("--remap-path-prefix=")
                .map(|mapping| ("--remap-path-prefix=", mapping))
        };
        remap_follows = flag == "--remap-path-prefix";
        match mapping.and_then(|(option, mapping)| Some((option, mapping.rsplit_once('=')?.1))) {
            Some((option, to)) => out.push(format!("{option}{REMAPPED_FROM}={to}")),
            None => out.push(flag.to_owned()),
        }
    }
    out.join(" ")
}

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
    let encoded = env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    let flags = recorded(encoded.split('\u{1f}').filter(|flag| !flag.is_empty()));
    println!("cargo:rustc-env=MQTTIUMD_LAB_BUILD_RUSTFLAGS={flags}");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
}
