//! Diagnostics of contract validation.
//!
//! The codes are a closed vocabulary shared with the example corpus: an
//! invalid example must be rejected for its declared code, not for any
//! reason at all.

use std::fmt;

/// The closed set of rejection codes.
pub mod code {
    /// The bytes are not one JSON value.
    pub const JSON_SYNTAX: &str = "json-syntax";
    /// An object repeats a key; no library may silently keep one of them.
    pub const DUPLICATE_KEY: &str = "duplicate-key";
    /// A number has a fraction, an exponent or exceeds 64 bits. Version 1
    /// formats hold integers only; large quantities are decimal strings.
    pub const NUMBER_NOT_INTEGER: &str = "number-not-integer";
    /// The `format` member is missing or names no known format.
    pub const FORMAT_UNKNOWN: &str = "format-unknown";
    /// The `schema_version` member is missing or not a known version.
    pub const SCHEMA_VERSION_UNKNOWN: &str = "schema-version-unknown";
    /// The document violates its JSON Schema.
    pub const SCHEMA_VIOLATION: &str = "schema-violation";
    /// A schema file is not a local, self-contained 2020-12 schema.
    pub const SCHEMA_NOT_LOCAL: &str = "schema-not-local";
    /// A decimal string denotes a value above `u64::MAX`.
    pub const U64_OUT_OF_RANGE: &str = "u64-out-of-range";
    /// A decimal string is not the canonical spelling of its value.
    pub const U64_NOT_CANONICAL: &str = "u64-not-canonical";
    /// A path is absolute or leaves the mqttiumd root.
    pub const PATH_ESCAPE: &str = "path-escape";
    /// A referenced file cannot be read.
    pub const FILE_UNREADABLE: &str = "file-unreadable";
    /// A referenced file is itself invalid.
    pub const REFERENCE_INVALID: &str = "reference-invalid";
    /// A document refers back to a document being checked, or the chain of
    /// references is too long.
    pub const REFERENCE_CYCLE: &str = "reference-cycle";
    /// A document exceeds the size this reader loads.
    pub const FILE_TOO_LARGE: &str = "file-too-large";
    /// A binding is used but never defined, or used before its definition.
    pub const BINDING_UNDEFINED: &str = "binding-undefined";
    /// Two definitions share one binding or checkpoint name.
    pub const BINDING_DUPLICATE: &str = "binding-duplicate";
    /// The injection point is not in the catalogue.
    pub const FAULT_POINT_UNKNOWN: &str = "fault-point-unknown";
    /// The action is not offered by the injection point.
    pub const FAULT_ACTION_UNKNOWN: &str = "fault-action-unknown";
    /// The command kind is not in the catalogue.
    pub const COMMAND_KIND_UNKNOWN: &str = "command-kind-unknown";
    /// The profile is not in the catalogue.
    pub const PROFILE_UNKNOWN: &str = "profile-unknown";
    /// The assertion is not in the catalogue.
    pub const ASSERTION_UNKNOWN: &str = "assertion-unknown";
    /// The client is not a session of the fixture.
    pub const CLIENT_UNKNOWN: &str = "client-unknown";
    /// A step may run longer than the scenario budget.
    pub const BUDGET_EXCEEDED: &str = "budget-exceeded";
    /// A step cannot run in the broker state reached before it.
    pub const STEP_ORDER_INVALID: &str = "step-order-invalid";
    /// Two entries of one catalogue share a name.
    pub const DUPLICATE_NAME: &str = "duplicate-name";
    /// A name refers to no entry of its catalogue.
    pub const CATALOG_REFERENCE_UNKNOWN: &str = "catalog-reference-unknown";
    /// The requirement identifier is not in the register.
    pub const REQUIREMENT_UNKNOWN: &str = "requirement-unknown";
    /// Two rules of a registry cover the same guard point.
    pub const REGISTRY_OVERLAP: &str = "registry-overlap";
    /// A registry declared complete leaves a guard point uncovered.
    pub const REGISTRY_HOLE: &str = "registry-hole";
    /// The guard domain of a registry is larger than the coverage budget.
    pub const COVERAGE_BUDGET_EXCEEDED: &str = "coverage-budget-exceeded";
    /// A rule both requires and forbids one effect.
    pub const REGISTRY_EFFECT_CONFLICT: &str = "registry-effect-conflict";
    /// The verdict is not the one its assertions and injections determine.
    pub const VERDICT_INCONSISTENT: &str = "verdict-inconsistent";
    /// A qualifying result does not cover what its scenario requires.
    pub const RESULT_INCOMPLETE: &str = "result-incomplete";
    /// A file cited as evidence, or a contract digest, is not the declared
    /// one.
    pub const EVIDENCE_MISMATCH: &str = "evidence-mismatch";

    /// Every code, so that the example corpus can prove it exercises them.
    pub const ALL: [&str; 34] = [
        JSON_SYNTAX,
        DUPLICATE_KEY,
        NUMBER_NOT_INTEGER,
        FORMAT_UNKNOWN,
        SCHEMA_VERSION_UNKNOWN,
        SCHEMA_VIOLATION,
        SCHEMA_NOT_LOCAL,
        U64_OUT_OF_RANGE,
        U64_NOT_CANONICAL,
        PATH_ESCAPE,
        FILE_UNREADABLE,
        REFERENCE_INVALID,
        REFERENCE_CYCLE,
        FILE_TOO_LARGE,
        BINDING_UNDEFINED,
        BINDING_DUPLICATE,
        FAULT_POINT_UNKNOWN,
        FAULT_ACTION_UNKNOWN,
        COMMAND_KIND_UNKNOWN,
        PROFILE_UNKNOWN,
        ASSERTION_UNKNOWN,
        CLIENT_UNKNOWN,
        BUDGET_EXCEEDED,
        STEP_ORDER_INVALID,
        DUPLICATE_NAME,
        CATALOG_REFERENCE_UNKNOWN,
        REQUIREMENT_UNKNOWN,
        REGISTRY_OVERLAP,
        REGISTRY_HOLE,
        COVERAGE_BUDGET_EXCEEDED,
        REGISTRY_EFFECT_CONFLICT,
        VERDICT_INCONSISTENT,
        RESULT_INCOMPLETE,
        EVIDENCE_MISMATCH,
    ];
}

/// One reason a document is refused.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Diagnostic {
    /// A value of [`code`].
    pub code: &'static str,
    /// JSON Pointer into the document; empty for the document itself.
    pub pointer: String,
    /// Human-readable detail. Not part of the contract.
    pub message: String,
}

impl Diagnostic {
    /// Builds a diagnostic.
    pub fn new(code: &'static str, pointer: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            pointer: pointer.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pointer = if self.pointer.is_empty() {
            "/"
        } else {
            &self.pointer
        };
        write!(formatter, "{} at {}: {}", self.code, pointer, self.message)
    }
}

/// The most diagnostics and the most notes one report keeps.
pub const MAX_DIAGNOSTICS: usize = 64;

/// The outcome of checking one document.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct Report {
    /// Reasons the document is refused. Empty means accepted. At most
    /// [`MAX_DIAGNOSTICS`] are kept.
    pub diagnostics: Vec<Diagnostic>,
    /// Diagnostics found beyond the ones kept. A truncated report still
    /// refuses the document.
    pub omitted_diagnostics: usize,
    /// Facts worth showing that do not refuse the document, such as the
    /// holes of a seed registry.
    pub notes: Vec<String>,
}

impl Report {
    /// A report refusing the document for one reason.
    pub fn refused(diagnostic: Diagnostic) -> Self {
        Self {
            diagnostics: vec![diagnostic],
            ..Self::default()
        }
    }

    /// Keeps the first [`MAX_DIAGNOSTICS`] diagnostics and notes and counts
    /// what it drops.
    pub fn truncate(&mut self) {
        if self.diagnostics.len() > MAX_DIAGNOSTICS {
            self.omitted_diagnostics += self.diagnostics.len() - MAX_DIAGNOSTICS;
            self.diagnostics.truncate(MAX_DIAGNOSTICS);
        }
        if self.notes.len() > MAX_DIAGNOSTICS {
            let omitted = self.notes.len() - MAX_DIAGNOSTICS;
            self.notes.truncate(MAX_DIAGNOSTICS);
            self.notes.push(format!("{omitted} more notes omitted"));
        }
    }

    /// Whether the document is accepted.
    pub fn is_accepted(&self) -> bool {
        self.diagnostics.is_empty()
    }
}
