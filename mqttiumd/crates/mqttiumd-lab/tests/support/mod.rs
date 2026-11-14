//! Helpers shared by the integration tests that build their own documents.
//!
//! Documents are written below the target directory, which is below the
//! mqttiumd root, so that they can be referenced like any other document.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

pub fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Creates an empty directory for one test and returns it with its path
/// relative to the mqttiumd root.
pub fn scratch(name: &str) -> (PathBuf, String) {
    let directory = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).expect("scratch directory");
    let canonical_root = root().canonicalize().expect("root");
    let relative = directory
        .canonicalize()
        .expect("scratch directory")
        .strip_prefix(&canonical_root)
        .expect("the target directory is below the mqttiumd root")
        .to_string_lossy()
        .into_owned();
    (directory, relative)
}

pub fn write(directory: &Path, name: &str, document: &Value) {
    let text = serde_json::to_string_pretty(document).expect("JSON");
    fs::write(directory.join(name), text).expect("document written");
}
