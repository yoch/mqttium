//! Offline JSON Schema 2020-12 validation (DEM-014).
//!
//! Schemas are read from one local directory. A reference may point inside
//! its own file or to another file of that directory; anything else is
//! refused when the directory is loaded. No retriever is compiled in.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use jsonschema::{Registry, Validator};
use serde_json::Value;

use crate::diagnostic::{Diagnostic, MAX_DIAGNOSTICS, code};
use crate::strict;

/// The dialect every schema file declares.
pub const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// Base under which this reader registers the schema files, so that their
/// relative references resolve. It is private to this reader and never
/// fetched. The files carry no `$id`: a reader that cannot join references
/// under this scheme registers them under a base of its own.
const REGISTRY_BASE: &str = "mqttiumd://contracts/schema/v1/";

/// The only schema version this revision reads.
pub const SCHEMA_VERSION: u64 = 1;

/// The closed map from document format to the schema file that defines it.
pub const FORMATS: &[(&str, &str)] = &[
    ("mqttiumd.scenario", "scenario.schema.json"),
    ("mqttiumd.fixture", "fixture.schema.json"),
    ("mqttiumd.trace-record", "trace-record.schema.json"),
    (
        "mqttiumd.transition-registry",
        "transition-registry.schema.json",
    ),
    ("mqttiumd.run-manifest", "run-manifest.schema.json"),
    ("mqttiumd.run-result", "run-result.schema.json"),
    ("mqttiumd.fault-catalog", "catalogs.schema.json"),
    ("mqttiumd.profile-catalog", "catalogs.schema.json"),
    ("mqttiumd.assertion-catalog", "catalogs.schema.json"),
    ("mqttiumd.requirement-register", "catalogs.schema.json"),
    ("mqttiumd.transition-catalog", "catalogs.schema.json"),
    ("mqttiumd.example-index", "catalogs.schema.json"),
];

/// Compiled validators, one per schema file.
pub struct Schemas {
    validators: BTreeMap<String, Validator>,
}

fn not_local(file: &str, message: impl Into<String>) -> Diagnostic {
    Diagnostic::new(
        code::SCHEMA_NOT_LOCAL,
        "",
        format!("{file}: {}", message.into()),
    )
}

/// Checks that every `$ref` of `schema` stays inside `files` and that no
/// `$id` moves the base of a reference.
fn check_references(file: &str, schema: &Value, files: &[String]) -> Result<(), Diagnostic> {
    match schema {
        Value::Object(members) => {
            if members.contains_key("$id") {
                return Err(not_local(
                    file,
                    "$id is not allowed; references are file-relative",
                ));
            }
            if let Some(Value::String(reference)) = members.get("$ref") {
                let target = reference.split('#').next().unwrap_or_default();
                let local = target.is_empty() || files.iter().any(|name| name == target);
                if !local {
                    return Err(not_local(file, format!("$ref {reference:?} is not local")));
                }
            }
            members
                .values()
                .try_for_each(|value| check_references(file, value, files))
        }
        Value::Array(items) => items
            .iter()
            .try_for_each(|value| check_references(file, value, files)),
        _ => Ok(()),
    }
}

/// Checks one schema document before it is compiled.
pub fn check_schema_document(
    file: &str,
    schema: &Value,
    files: &[String],
) -> Result<(), Diagnostic> {
    if schema.get("$schema").and_then(Value::as_str) != Some(DIALECT) {
        return Err(not_local(file, format!("$schema must be {DIALECT}")));
    }
    check_references(file, schema, files)
}

impl Schemas {
    /// Loads and compiles every `*.schema.json` of `directory`.
    pub fn load(directory: &Path) -> Result<Self, Diagnostic> {
        let unreadable = |error: std::io::Error| {
            Diagnostic::new(
                code::FILE_UNREADABLE,
                "",
                format!("{}: {error}", directory.display()),
            )
        };
        let mut files = Vec::new();
        for entry in fs::read_dir(directory).map_err(unreadable)? {
            let name = entry.map_err(unreadable)?.file_name();
            let name = name.to_string_lossy().into_owned();
            if name.ends_with(".schema.json") {
                files.push(name);
            }
        }
        files.sort();

        let mut documents = Vec::new();
        for file in &files {
            let bytes = fs::read(directory.join(file)).map_err(unreadable)?;
            let schema = strict::parse(&bytes)
                .map_err(|error| not_local(file, format!("{}: {}", error.code, error.message)))?;
            check_schema_document(file, &schema, &files)?;
            documents.push((file.clone(), schema));
        }
        for (_, file) in FORMATS {
            if !files.iter().any(|name| name == file) {
                return Err(not_local(file, "schema file is missing"));
            }
        }

        let mut builder = Registry::new();
        for (file, schema) in &documents {
            builder = builder
                .add(format!("{REGISTRY_BASE}{file}"), schema)
                .map_err(|error| not_local(file, error.to_string()))?;
        }
        let registry = builder
            .prepare()
            .map_err(|error| not_local("registry", error.to_string()))?;

        let mut validators = BTreeMap::new();
        for (file, schema) in &documents {
            let validator = jsonschema::options()
                .with_registry(&registry)
                .with_base_uri(format!("{REGISTRY_BASE}{file}"))
                .build(schema)
                .map_err(|error| not_local(file, error.to_string()))?;
            validators.insert(file.clone(), validator);
        }
        Ok(Self { validators })
    }

    /// Validates a parsed document: known format, known version, schema.
    pub fn validate(&self, document: &Value) -> Vec<Diagnostic> {
        let Some(format) = document.get("format").and_then(Value::as_str) else {
            return vec![Diagnostic::new(
                code::FORMAT_UNKNOWN,
                "/format",
                "the document does not declare a format",
            )];
        };
        let Some((_, file)) = FORMATS.iter().find(|(name, _)| *name == format) else {
            return vec![Diagnostic::new(
                code::FORMAT_UNKNOWN,
                "/format",
                format!("{format:?} is not a known format"),
            )];
        };
        if document.get("schema_version").and_then(Value::as_u64) != Some(SCHEMA_VERSION) {
            return vec![Diagnostic::new(
                code::SCHEMA_VERSION_UNKNOWN,
                "/schema_version",
                format!("only schema_version {SCHEMA_VERSION} is known"),
            )];
        }
        let Some(validator) = self.validators.get(*file) else {
            return vec![not_local(file, "schema file is not loaded")];
        };
        // One more than a report keeps, so that truncation is still counted.
        validator
            .iter_errors(document)
            .take(MAX_DIAGNOSTICS + 1)
            .map(|error| {
                Diagnostic::new(
                    code::SCHEMA_VIOLATION,
                    error.instance_path().as_str(),
                    format!("{error} (schema {})", error.schema_path().as_str()),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn files() -> Vec<String> {
        vec![
            "a.schema.json".to_owned(),
            "primitives.schema.json".to_owned(),
        ]
    }

    fn schema(body: Value) -> Value {
        let mut schema = json!({"$schema": DIALECT});
        schema
            .as_object_mut()
            .expect("object")
            .extend(body.as_object().expect("object").clone());
        schema
    }

    #[test]
    fn local_references_are_accepted() {
        let document = schema(json!({
            "properties": {
                "x": {"$ref": "#/$defs/y"},
                "z": {"$ref": "primitives.schema.json#/$defs/u64"}
            },
            "$defs": {"y": {"type": "string"}}
        }));
        assert_eq!(
            check_schema_document("a.schema.json", &document, &files()),
            Ok(())
        );
    }

    #[test]
    fn remote_and_unknown_references_are_refused() {
        for reference in [
            "https://example.com/x.json",
            "https://json-schema.org/draft/2020-12/schema",
            "mqttiumd://contracts/schema/v1/primitives.schema.json#/$defs/u64",
            "../v2/primitives.schema.json",
            "missing.schema.json#/$defs/x",
            "file:///etc/passwd",
        ] {
            let document = schema(json!({"items": [{"$ref": reference}]}));
            let error = check_schema_document("a.schema.json", &document, &files())
                .expect_err("must be refused");
            assert_eq!(error.code, code::SCHEMA_NOT_LOCAL, "{reference}");
        }
    }

    #[test]
    fn the_dialect_is_fixed_and_no_identifier_rebases_references() {
        let mut other_dialect = schema(json!({}));
        other_dialect["$schema"] = json!("http://json-schema.org/draft-07/schema#");
        assert!(check_schema_document("a.schema.json", &other_dialect, &files()).is_err());
        let with_id = schema(json!({"$id": "https://example.com/a.schema.json"}));
        assert!(check_schema_document("a.schema.json", &with_id, &files()).is_err());
        let nested_id = schema(json!({"$defs": {"x": {"$id": "elsewhere/", "type": "string"}}}));
        assert!(check_schema_document("a.schema.json", &nested_id, &files()).is_err());
    }
}
