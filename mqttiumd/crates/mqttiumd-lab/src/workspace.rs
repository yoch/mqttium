//! The files a validation run reads: schemas, catalogues and documents.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::diagnostic::{Diagnostic, Report, code};
use crate::schemas::Schemas;
use crate::{semantic, strict};

/// The closed catalogues that documents refer to.
#[derive(Default)]
pub struct Catalogs {
    /// Injection point to the actions it offers.
    pub fault_points: BTreeMap<String, BTreeSet<String>>,
    /// Command kinds a fault may match.
    pub command_kinds: BTreeSet<String>,
    /// Profile identifiers.
    pub profiles: BTreeSet<String>,
    /// Assertion names.
    pub assertions: BTreeSet<String>,
    /// Requirement, scenario, mutation, model, gate and lot identifiers.
    pub requirements: BTreeSet<String>,
}

/// Catalogue files every contract directory must hold.
const CATALOG_FILES: [(&str, &str); 4] = [
    ("fault-points.json", "mqttiumd.fault-catalog"),
    ("profiles.json", "mqttiumd.profile-catalog"),
    ("assertions.json", "mqttiumd.assertion-catalog"),
    ("requirements.json", "mqttiumd.requirement-register"),
];

/// Whether `path` is relative and stays below the root it is joined to.
pub fn is_contained(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

/// One contract directory, its schemas and its catalogues.
pub struct Workspace {
    root: PathBuf,
    contracts: PathBuf,
    schemas: Schemas,
    catalogs: Catalogs,
}

fn strings(value: &Value, list: &str, key: &str) -> BTreeSet<String> {
    value
        .get(list)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| match key {
            "" => entry.as_str(),
            _ => entry.get(key).and_then(Value::as_str),
        })
        .map(str::to_owned)
        .collect()
}

impl Workspace {
    /// Opens the contract directory `contracts`, given relative to `root`.
    /// Schemas are read from the sibling `schema/<version>` directory.
    pub fn open(root: &Path, contracts: &Path) -> Result<Self, Diagnostic> {
        let contracts = root.join(contracts);
        let version = contracts.file_name().ok_or_else(|| {
            Diagnostic::new(
                code::FILE_UNREADABLE,
                "",
                "the contract directory has no name",
            )
        })?;
        let schema_dir = contracts
            .parent()
            .unwrap_or(Path::new("."))
            .join("schema")
            .join(version);
        let mut workspace = Self {
            root: root.to_path_buf(),
            contracts,
            schemas: Schemas::load(&schema_dir)?,
            catalogs: Catalogs::default(),
        };

        let mut loaded = Vec::new();
        for (file, format) in CATALOG_FILES {
            let path = workspace.contracts.join(file);
            let bytes = fs::read(&path).map_err(|error| {
                Diagnostic::new(
                    code::FILE_UNREADABLE,
                    "",
                    format!("{}: {error}", path.display()),
                )
            })?;
            let document = strict::parse(&bytes)?;
            if let Some(first) = workspace.schemas.validate(&document).into_iter().next() {
                return Err(Diagnostic::new(
                    code::REFERENCE_INVALID,
                    "",
                    format!("{file}: {first}"),
                ));
            }
            if document.get("format").and_then(Value::as_str) != Some(format) {
                return Err(Diagnostic::new(
                    code::REFERENCE_INVALID,
                    "",
                    format!("{file} must have the format {format}"),
                ));
            }
            loaded.push(document);
        }
        let [faults, profiles, assertions, requirements] = loaded.as_slice() else {
            unreachable!("one document per catalogue file");
        };
        workspace.catalogs = Catalogs {
            fault_points: faults
                .get("points")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|point| {
                    (
                        point["point"].as_str().unwrap_or_default().to_owned(),
                        strings(point, "actions", ""),
                    )
                })
                .collect(),
            command_kinds: strings(faults, "command_kinds", ""),
            profiles: strings(profiles, "profiles", "id"),
            assertions: strings(assertions, "assertions", "name"),
            requirements: strings(requirements, "entries", "id"),
        };
        Ok(workspace)
    }

    /// The catalogues of this contract directory.
    pub fn catalogs(&self) -> &Catalogs {
        &self.catalogs
    }

    /// The root that relative paths of documents are joined to.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Checks a parsed document: schema first, then semantics.
    pub fn check_value(&self, document: &Value) -> Report {
        let mut report = Report {
            diagnostics: self.schemas.validate(document),
            notes: Vec::new(),
        };
        if report.is_accepted() {
            semantic::check(self, document, &mut report);
        }
        report
    }

    /// Checks the bytes of one document.
    pub fn check_bytes(&self, bytes: &[u8]) -> Report {
        match strict::parse(bytes) {
            Ok(document) => self.check_value(&document),
            Err(diagnostic) => Report::refused(diagnostic),
        }
    }

    /// Checks one file, given relative to the root.
    pub fn check_file(&self, path: &Path) -> Report {
        self.check_path(&self.root.join(path))
    }

    fn check_path(&self, path: &Path) -> Report {
        match fs::read(path) {
            Ok(bytes) => self.check_bytes(&bytes),
            Err(error) => Report::refused(Diagnostic::new(
                code::FILE_UNREADABLE,
                "",
                format!("{}: {error}", path.display()),
            )),
        }
    }

    /// Loads a document referenced by another one at `pointer`. The path
    /// must stay below the root and the document must be valid and of the
    /// expected format.
    pub fn load_reference(
        &self,
        path: &str,
        pointer: &str,
        format: &str,
    ) -> Result<Value, Diagnostic> {
        if !is_contained(path) {
            return Err(Diagnostic::new(
                code::PATH_ESCAPE,
                pointer,
                format!("{path:?} is not a path below the mqttiumd root"),
            ));
        }
        let bytes = fs::read(self.root.join(path)).map_err(|error| {
            Diagnostic::new(code::FILE_UNREADABLE, pointer, format!("{path}: {error}"))
        })?;
        let invalid = |reason: String| {
            Diagnostic::new(
                code::REFERENCE_INVALID,
                pointer,
                format!("{path}: {reason}"),
            )
        };
        let document = strict::parse(&bytes).map_err(|error| invalid(error.to_string()))?;
        if let Some(first) = self.check_value(&document).diagnostics.into_iter().next() {
            return Err(invalid(first.to_string()));
        }
        if document.get("format").and_then(Value::as_str) != Some(format) {
            return Err(invalid(format!("expected the format {format}")));
        }
        Ok(document)
    }

    /// Checks every JSON file of the contract directory, in name order.
    pub fn check_contracts(&self) -> Vec<(PathBuf, Report)> {
        let mut files: Vec<PathBuf> = fs::read_dir(&self.contracts)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .collect();
        files.sort();
        files
            .into_iter()
            .map(|path| {
                let report = self.check_path(&path);
                (path, report)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::is_contained;

    #[test]
    fn only_paths_below_the_root_are_contained() {
        assert!(is_contained(
            "tests/fixtures/p1/three-persistent-sessions.json"
        ));
        assert!(is_contained("a"));
        for path in [
            "",
            "/etc/passwd",
            "../x",
            "a/../../x",
            "a//b",
            "./a",
            "a/./b",
            "a\\b",
        ] {
            assert!(!is_contained(path), "{path:?}");
        }
    }
}
