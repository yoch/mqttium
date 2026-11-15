//! Contract documents remain confined to the workspace even when Cargo
//! builds the tests in an independently selected directory.

mod support;

#[test]
fn document_fixtures_are_root_relative_with_an_external_cargo_target() {
    let (directory, relative) = support::scratch("external-target-fixture");
    let document = serde_json::json!({"test": "external-target"});
    support::write(&directory, "fixture.json", &document);
    let from_root = support::root().join(relative).join("fixture.json");
    assert_eq!(
        std::fs::canonicalize(from_root).expect("root-relative fixture"),
        std::fs::canonicalize(directory.join("fixture.json")).expect("fixture"),
    );
    std::fs::remove_dir_all(directory).expect("remove test documents");
}
