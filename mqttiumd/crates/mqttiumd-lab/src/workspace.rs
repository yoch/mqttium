//! The files a validation run reads: schemas, catalogues and documents.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::diagnostic::{Diagnostic, Report, code};
pub use crate::files::MAX_DOCUMENT_BYTES;
use crate::files::{self, Loaded, Refusal};
use crate::schemas::Schemas;
use crate::{manifest, semantic, strict};

/// One contractual injection point.
#[derive(Default)]
pub struct FaultPoint {
    /// What passes through the point: `command` or `result`.
    pub carries: String,
    /// The actions the point offers.
    pub actions: BTreeSet<String>,
}

/// The closed catalogues that documents refer to.
#[derive(Default)]
pub struct Catalogs {
    /// Injection point to the actions it offers.
    pub fault_points: BTreeMap<String, FaultPoint>,
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
    contracts_digest: String,
    /// The contract and schema files as they were read when the workspace
    /// was opened, by resolved path. The digest is the digest of these
    /// bytes, and these bytes are what the workspace interprets.
    snapshot: BTreeMap<PathBuf, Vec<u8>>,
    /// Names of the JSON files of the contract directory, in order.
    contract_names: Vec<String>,
    active: ReferenceStack,
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
        let unreadable = |message: String| Diagnostic::new(code::FILE_UNREADABLE, "", message);
        let canonical_root = root
            .canonicalize()
            .map_err(|error| unreadable(format!("{}: {error}", root.display())))?;
        let (contract_dir, schema_dir) =
            files::contract_directories(contracts).map_err(unreadable)?;
        // One read of every contract and schema file: these bytes are
        // hashed, and these bytes are interpreted.
        let mut snapshot = BTreeMap::new();
        let mut listing = Vec::new();
        let mut schema_files = Vec::new();
        let mut contract_names = Vec::new();
        for (directory, is_schema) in [(&contract_dir, false), (&schema_dir, true)] {
            let entries = fs::read_dir(root.join(directory))
                .map_err(|error| unreadable(format!("{directory}: {error}")))?;
            for entry in entries {
                let entry = entry.map_err(|error| unreadable(format!("{directory}: {error}")))?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.ends_with(".json") {
                    continue;
                }
                let loaded = files::read_document(&canonical_root, &entry.path())
                    .map_err(|(code, message)| Diagnostic::new(code, "", message))?;
                listing.push((
                    format!("{directory}/{name}"),
                    manifest::sha256_hex(&loaded.bytes),
                ));
                if is_schema {
                    if name.ends_with(".schema.json") {
                        schema_files.push((name, loaded.bytes.clone()));
                    }
                } else {
                    contract_names.push(name);
                }
                snapshot.insert(loaded.path, loaded.bytes);
            }
        }
        listing.sort();
        schema_files.sort();
        contract_names.sort();
        let contracts_digest = manifest::contracts_digest(
            listing
                .iter()
                .map(|(path, sha256)| (sha256.as_str(), path.as_str())),
        );
        let contracts = root.join(contract_dir);
        let schemas = Schemas::from_bytes(&schema_files)?;
        let mut workspace = Self {
            root: root.to_path_buf(),
            canonical_root,
            contracts,
            schemas,
            catalogs: Catalogs::default(),
            contracts_digest,
            snapshot,
            contract_names,
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
                        FaultPoint {
                            carries: point["carries"].as_str().unwrap_or_default().to_owned(),
                            actions: strings(point, "actions", ""),
                        },
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

    /// The identity of this contract revision by exact content.
    pub fn contracts_digest(&self) -> &str {
        &self.contracts_digest
    }

    /// The root that relative paths of documents are joined to.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Reads one document through the bounded, confined reader.
    fn read(&self, path: &Path) -> Result<Loaded, Refusal> {
        // A contract file is the content the digest names, whatever the
        // file holds now, even if it is gone.
        let resolved = path
            .canonicalize()
            .ok()
            .or_else(|| {
                let relative = path.strip_prefix(&self.root).ok()?;
                Some(self.canonical_root.join(relative))
            })
            .unwrap_or_else(|| path.to_path_buf());
        if let Some(bytes) = self.snapshot.get(&resolved) {
            return Ok(Loaded {
                path: resolved,
                bytes: bytes.clone(),
            });
        }
        files::read_document(&self.canonical_root, path)
    }

    /// Checks a parsed document: schema first, then semantics. At most
    /// [`MAX_DIAGNOSTICS`] diagnostics are kept; the rest are counted.
    pub fn check_value(&self, document: &Value) -> Report {
        let mut report = Report::default();
        report.diagnostics = self
            .schemas
            .validate_counted(document, &mut report.omitted_diagnostics);
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
        self.load_cited(reference, path, None)
    }

    /// Loads a referenced document that is also cited as evidence with a
    /// digest and a size. The bytes that are hashed are the bytes that are
    /// validated: the file is read once.
    pub fn load_cited(
        &self,
        reference: &Reference,
        path: &str,
        cited: Option<(&str, Option<u64>)>,
    ) -> Result<Value, Diagnostic> {
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
        if let Some((sha256, bytes)) = cited {
            let digest = manifest::sha256_hex(&loaded.bytes);
            if digest != sha256 || bytes != Some(loaded.bytes.len() as u64) {
                return Err(Diagnostic::new(
                    code::EVIDENCE_MISMATCH,
                    pointer,
                    format!(
                        "{path} has {} bytes and the digest {digest}",
                        loaded.bytes.len()
                    ),
                ));
            }
        }
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

    /// Digest and size of a file that a document cites as evidence. The
    /// file is hashed as a stream: evidence has no size bound.
    pub fn hash_evidence(&self, path: &str) -> Result<(String, u64), Refusal> {
        if !is_contained(path) {
            return Err((
                code::PATH_ESCAPE,
                format!("{path:?} is not a path below the mqttiumd root"),
            ));
        }
        files::hash_file(&self.canonical_root, &self.root.join(path))
    }

    /// Checks every JSON file of the contract directory, in name order.
    pub fn check_contracts(&self) -> Vec<(PathBuf, Report)> {
        self.contract_names
            .iter()
            .map(|name| {
                let path = self.contracts.join(name);
                let report = self.check_path(&path);
                (path, report)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::{
        MAX_REFERENCE_DEPTH, REFERENCES, ReferenceStack, StackRefusal, Workspace, is_contained,
    };

    /// A copy of the contract directory and its schemas under a root of
    /// its own, so that a test may change the files.
    fn copy_of_the_contracts(name: &str) -> PathBuf {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let root = source.join("target/workspace-tests").join(name);
        let _ = fs::remove_dir_all(&root);
        for directory in ["contracts/v1", "contracts/schema/v1"] {
            fs::create_dir_all(root.join(directory)).expect("directory");
            for entry in fs::read_dir(source.join(directory)).expect("contracts") {
                let entry = entry.expect("entry");
                fs::copy(entry.path(), root.join(directory).join(entry.file_name())).expect("copy");
            }
        }
        root
    }

    /// The digest names the rules that are applied: a contract file that
    /// changes after the workspace was opened changes neither the digest
    /// nor what is validated, and a workspace opened afterwards names
    /// other contracts.
    #[test]
    fn the_bytes_that_are_hashed_are_the_bytes_that_are_interpreted() {
        let root = copy_of_the_contracts("one-snapshot");
        let contracts = Path::new("contracts/v1");
        let workspace = Workspace::open(&root, contracts).expect("opens");
        let digest = workspace.contracts_digest().to_owned();
        let registry = Path::new("contracts/v1/p1.transitions.json");
        assert!(workspace.check_file(registry).is_accepted());

        // The catalogue of the registry is replaced by one without machines.
        let catalog = root.join("contracts/v1/p1.catalog.json");
        let changed = fs::read_to_string(&catalog)
            .expect("catalogue")
            .replace("\"inbound_qos2\"", "\"inbound_qos3\"");
        fs::write(&catalog, changed).expect("write");
        fs::write(root.join("contracts/v1/profiles.json"), "{}").expect("write");
        fs::remove_file(root.join("contracts/v1/assertions.json")).expect("remove");

        assert_eq!(workspace.contracts_digest(), digest);
        let report = workspace.check_file(registry);
        assert!(report.is_accepted(), "{:?}", report.diagnostics);
        for (path, report) in workspace.check_contracts() {
            assert!(
                report.is_accepted(),
                "{}: {:?}",
                path.display(),
                report.diagnostics
            );
        }
        assert_eq!(workspace.check_contracts().len(), 6);

        // Opened again, the directory is other contracts, and invalid ones.
        assert!(Workspace::open(&root, contracts).is_err());
        let original = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contracts/v1");
        fs::copy(
            original.join("assertions.json"),
            root.join("contracts/v1/assertions.json"),
        )
        .expect("copy");
        fs::write(
            root.join("contracts/v1/profiles.json"),
            fs::read(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contracts/v1/profiles.json"),
            )
            .expect("profiles"),
        )
        .expect("write");
        let reopened = Workspace::open(&root, contracts).expect("opens");
        assert_ne!(reopened.contracts_digest(), digest);
        assert!(!reopened.check_file(registry).is_accepted());
        let _ = fs::remove_dir_all(&root);
    }

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
