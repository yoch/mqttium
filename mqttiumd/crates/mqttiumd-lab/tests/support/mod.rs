//! Helpers shared by the integration tests that build their own documents.
//!
//! Documents are written below the root's ignored target directory so that
//! they can be referenced like any other document. Cargo's compilation
//! directory may be elsewhere and is independent of these fixture paths.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

pub fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Creates an empty directory for one test and returns it with its path
/// relative to the mqttiumd root.
pub fn scratch(name: &str) -> (PathBuf, String) {
    let directory = root()
        .join("target/contract-test-documents")
        .join(std::process::id().to_string())
        .join(name);
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).expect("scratch directory");
    let canonical_root = root().canonicalize().expect("root");
    let relative = directory
        .canonicalize()
        .expect("scratch directory")
        .strip_prefix(&canonical_root)
        .expect("test documents are below the mqttiumd root")
        .to_string_lossy()
        .into_owned();
    (directory, relative)
}

pub fn write(directory: &Path, name: &str, document: &Value) {
    let text = serde_json::to_string_pretty(document).expect("JSON");
    fs::write(directory.join(name), text).expect("document written");
}
