//! Canonical descriptive Will examples have the same meaning in both readers.
//! This is format compatibility, not qualification of product Will behavior.

use std::path::Path;

use mqttiumd_contract::command::Recipe;
use mqttiumd_contract::session::{SessionAction, SessionExpiry};
use serde_json::Value;

fn last_recipe(name: &str) -> Recipe {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let bytes = std::fs::read(root.join(format!("contracts/examples/v1/valid/{name}.json")))
        .expect("descriptive Will example");
    let value: Value = serde_json::from_slice(&bytes).expect("JSON example");
    let encoded = value["operations"].as_array().unwrap().last().unwrap()["recipe_hex"]
        .as_str()
        .unwrap();
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let recipe = Recipe::decode_canonical(&bytes, 16, 1024).expect("canonical Will recipe");
    let mut again = Vec::new();
    recipe.encode_canonical(&mut again).expect("encode");
    assert_eq!(again, bytes);
    recipe
}

#[test]
fn will_examples_preserve_open_occurrence_end_guard_and_independent_admission() {
    let Recipe::SessionLifecycle(open) = last_recipe("store-snapshot-will-open") else {
        panic!("Will-aware lifecycle Open");
    };
    let SessionAction::OpenWillAt {
        client_id,
        expiry,
        now,
        will: Some(spec),
        ..
    } = open.action
    else {
        panic!("Will-aware Open action");
    };
    assert_eq!(client_id.as_slice(), b"capteur-1");
    assert_eq!(expiry, SessionExpiry::Never);
    assert_eq!(now, 100);
    assert_eq!(spec.topic.as_slice(), b"atelier/capteur-1/status");
    assert_eq!(spec.payload.as_slice(), b"offline");

    let Recipe::SessionLifecycle(end) = last_recipe("store-snapshot-will-pending") else {
        panic!("Will-aware lifecycle End");
    };
    let SessionAction::EndWill {
        guard,
        expiry,
        observed_at,
        publish,
    } = end.action
    else {
        panic!("Will-aware End action");
    };
    assert_eq!(guard.session.get(), 4);
    assert_eq!(guard.opened_by, Some(open.operation));
    assert_eq!(expiry, SessionExpiry::Never);
    assert_eq!(observed_at, 200);
    assert!(publish);

    let Recipe::WillPublication(admission) = last_recipe("store-snapshot-will-published") else {
        panic!("independent Will publication kind17");
    };
    assert_eq!(admission.will, open.operation);
    assert_ne!(admission.operation, open.operation);
    assert_ne!(admission.operation, end.operation);
    assert_eq!(admission.publication.sequence().get(), 43);
    assert_eq!(admission.payload.id.sequence().get(), 44);
    assert_eq!(admission.payload.length.get(), 7);
    assert!(admission.targets.is_empty());
}
