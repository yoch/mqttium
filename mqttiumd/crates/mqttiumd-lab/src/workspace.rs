//! The files a validation run reads: schemas, catalogues and documents.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
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

/// The largest document this reader loads.
pub const MAX_DOCUMENT_BYTES: u64 = 16 * 1024 * 1024;

/// The longest chain of documents referring to one another.
pub const MAX_REFERENCE_DEPTH: usize = 4;

/// One way a document of a format may refer to a document of another.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Reference {
    /// Format of the referring document.
    pub from: &'static str,
    /// JSON Pointer of the member that holds the path.
    pub pointer: &'static str,
    /// Format the referenced document must have.
    pub to: &'static str,
}

/// A scenario names the fixture it starts from.
pub const SCENARIO_FIXTURE: Reference = Reference {
    from: "mqttiumd.scenario",
    pointer: "/initial_fixture",
    to: "mqttiumd.fixture",
};

/// A transition registry names the catalogue of its vocabulary.
pub const REGISTRY_CATALOG: Reference = Reference {
    from: "mqttiumd.transition-registry",
    pointer: "/catalog",
    to: "mqttiumd.transition-catalog",
};

/// A qualifying result names the scenario it ran.
pub const RESULT_SCENARIO: Reference = Reference {
    from: "mqttiumd.run-result",
    pointer: "/scenario/path",
    to: "mqttiumd.scenario",
};

/// A qualifying result names the manifest of its run.
pub const RESULT_MANIFEST: Reference = Reference {
    from: "mqttiumd.run-result",
    pointer: "/manifest/path",
    to: "mqttiumd.run-manifest",
};

/// Every reference between formats. The format of a referenced document is
/// checked against this table before the document is validated, and the
/// table is acyclic, so following references always terminates. The stack
/// of [`Workspace`] guards the same property at run time.
pub const REFERENCES: [Reference; 4] = [
    SCENARIO_FIXTURE,
    REGISTRY_CATALOG,
    RESULT_SCENARIO,
    RESULT_MANIFEST,
];

/// Why a document cannot enter the stack of documents being checked.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StackRefusal {
    /// The document is already being checked.
    Cycle,
    /// The chain of references is longer than [`MAX_REFERENCE_DEPTH`].
    TooDeep,
}

/// The documents being checked, outermost first.
#[derive(Default)]
struct ReferenceStack(RefCell<Vec<PathBuf>>);

impl ReferenceStack {
    fn enter(&self, path: &Path) -> Result<(), StackRefusal> {
        let mut active = self.0.borrow_mut();
        if active.iter().any(|entry| entry == path) {
            return Err(StackRefusal::Cycle);
        }
        if active.len() >= MAX_REFERENCE_DEPTH {
            return Err(StackRefusal::TooDeep);
        }
        active.push(path.to_path_buf());
        Ok(())
    }

    fn leave(&self) {
        self.0.borrow_mut().pop();
    }
}

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
    canonical_root: PathBuf,
    contracts: PathBuf,
    schemas: Schemas,
    catalogs: Catalogs,
    active: ReferenceStack,
}

/// Bytes of a document and the resolved path they were read from.
struct Loaded {
    path: PathBuf,
    bytes: Vec<u8>,
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
        let canonical_root = root.canonicalize().map_err(|error| {
            Diagnostic::new(
                code::FILE_UNREADABLE,
                "",
                format!("{}: {error}", root.display()),
            )
        })?;
        let mut workspace = Self {
            root: root.to_path_buf(),
            canonical_root,
            contracts,
            schemas: Schemas::load(&schema_dir)?,
            catalogs: Catalogs::default(),
            active: ReferenceStack::default(),
        };

        let mut loaded = Vec::new();
        for (file, format) in CATALOG_FILES {
            let path = workspace.contracts.join(file);
            let file_bytes = workspace
                .read(&path)
                .map_err(|(code, message)| Diagnostic::new(code, "", message))?
                .bytes;
            let document = strict::parse(&file_bytes)?;
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

    /// Reads one document.
    ///
    /// Symbolic links are followed. The resolved file must be a regular
    /// file below the resolved root, and no larger than
    /// [`MAX_DOCUMENT_BYTES`]; a link that leaves the root is refused like
    /// any other path that does.
    fn read(&self, path: &Path) -> Result<Loaded, (&'static str, String)> {
        let shown = path.display();
        let resolved = path
            .canonicalize()
            .map_err(|error| (code::FILE_UNREADABLE, format!("{shown}: {error}")))?;
        if !resolved.starts_with(&self.canonical_root) {
            return Err((
                code::PATH_ESCAPE,
                format!("{shown} resolves outside the mqttiumd root"),
            ));
        }
        let metadata = fs::metadata(&resolved)
            .map_err(|error| (code::FILE_UNREADABLE, format!("{shown}: {error}")))?;
        if !metadata.is_file() {
            return Err((
                code::FILE_UNREADABLE,
                format!("{shown} is not a regular file"),
            ));
        }
        let too_large = || {
            (
                code::FILE_TOO_LARGE,
                format!("{shown} is larger than {MAX_DOCUMENT_BYTES} bytes"),
            )
        };
        if metadata.len() > MAX_DOCUMENT_BYTES {
            return Err(too_large());
        }
        // The size is checked again while reading: the file may have grown.
        let mut bytes = Vec::new();
        fs::File::open(&resolved)
            .and_then(|file| file.take(MAX_DOCUMENT_BYTES + 1).read_to_end(&mut bytes))
            .map_err(|error| (code::FILE_UNREADABLE, format!("{shown}: {error}")))?;
        if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
            return Err(too_large());
        }
        Ok(Loaded {
            path: resolved,
            bytes,
        })
    }

    /// Checks a parsed document: schema first, then semantics. At most
    /// [`MAX_DIAGNOSTICS`] diagnostics are kept; the rest are counted.
    pub fn check_value(&self, document: &Value) -> Report {
        let mut report = Report {
            diagnostics: self.schemas.validate(document),
            ..Report::default()
        };
        if report.is_accepted() {
            semantic::check(self, document, &mut report);
        }
        report.truncate();
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
        let loaded = match self.read(path) {
            Ok(loaded) => loaded,
            Err((code, message)) => return Report::refused(Diagnostic::new(code, "", message)),
        };
        if let Err(refusal) = self.active.enter(&loaded.path) {
            return Report::refused(Diagnostic::new(
                code::REFERENCE_CYCLE,
                "",
                format!("{}: {refusal:?}", path.display()),
            ));
        }
        let report = self.check_bytes(&loaded.bytes);
        self.active.leave();
        report
    }

    /// Loads the document that another one refers to through `reference`.
    ///
    /// The path must stay below the root. The format of the document is
    /// checked against `reference.to` before the document is validated, so
    /// a document of the wrong format is refused without being followed.
    pub fn load_reference(&self, reference: &Reference, path: &str) -> Result<Value, Diagnostic> {
        let pointer = reference.pointer;
        if !is_contained(path) {
            return Err(Diagnostic::new(
                code::PATH_ESCAPE,
                pointer,
                format!("{path:?} is not a path below the mqttiumd root"),
            ));
        }
        let loaded = self
            .read(&self.root.join(path))
            .map_err(|(code, message)| Diagnostic::new(code, pointer, message))?;
        let invalid = |reason: String| {
            Diagnostic::new(
                code::REFERENCE_INVALID,
                pointer,
                format!("{path}: {reason}"),
            )
        };
        let document = strict::parse(&loaded.bytes).map_err(|error| invalid(error.to_string()))?;
        let format = document.get("format").and_then(Value::as_str);
        if format != Some(reference.to) {
            return Err(invalid(format!(
                "expected the format {}, found {}",
                reference.to,
                format.unwrap_or("none")
            )));
        }
        self.active.enter(&loaded.path).map_err(|refusal| {
            Diagnostic::new(
                code::REFERENCE_CYCLE,
                pointer,
                format!("{path}: {refusal:?}"),
            )
        })?;
        let report = self.check_value(&document);
        self.active.leave();
        match report.diagnostics.into_iter().next() {
            Some(first) => Err(invalid(first.to_string())),
            None => Ok(document),
        }
    }

    /// Reads a file that a document cites as evidence and returns its
    /// bytes. The same containment rules apply as for any document.
    pub fn read_evidence(&self, path: &str) -> Result<Vec<u8>, (&'static str, String)> {
        if !is_contained(path) {
            return Err((
                code::PATH_ESCAPE,
                format!("{path:?} is not a path below the mqttiumd root"),
            ));
        }
        self.read(&self.root.join(path)).map(|loaded| loaded.bytes)
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
    use std::path::Path;

    use super::{MAX_REFERENCE_DEPTH, REFERENCES, ReferenceStack, StackRefusal, is_contained};

    #[test]
    fn the_reference_graph_between_formats_is_acyclic() {
        // Follow every chain of references; none may come back to a format
        // it already visited, and none may exceed the run-time depth.
        fn follow(chain: &mut Vec<&'static str>) {
            let last = *chain.last().expect("non-empty chain");
            for reference in REFERENCES.iter().filter(|reference| reference.from == last) {
                assert!(
                    !chain.contains(&reference.to),
                    "cycle: {chain:?} -> {}",
                    reference.to
                );
                chain.push(reference.to);
                assert!(chain.len() <= MAX_REFERENCE_DEPTH, "too deep: {chain:?}");
                follow(chain);
                chain.pop();
            }
        }
        for reference in REFERENCES {
            follow(&mut vec![reference.from]);
        }
    }

    #[test]
    fn the_stack_refuses_a_document_already_being_checked() {
        let stack = ReferenceStack::default();
        assert_eq!(stack.enter(Path::new("/root/a.json")), Ok(()));
        assert_eq!(stack.enter(Path::new("/root/b.json")), Ok(()));
        assert_eq!(
            stack.enter(Path::new("/root/a.json")),
            Err(StackRefusal::Cycle)
        );
        stack.leave();
        stack.leave();
        assert_eq!(stack.enter(Path::new("/root/a.json")), Ok(()));
    }

    #[test]
    fn the_stack_refuses_a_chain_that_is_too_deep() {
        let stack = ReferenceStack::default();
        for depth in 0..MAX_REFERENCE_DEPTH {
            assert_eq!(
                stack.enter(Path::new(&format!("/root/{depth}.json"))),
                Ok(())
            );
        }
        assert_eq!(
            stack.enter(Path::new("/root/one-more.json")),
            Err(StackRefusal::TooDeep)
        );
    }

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
