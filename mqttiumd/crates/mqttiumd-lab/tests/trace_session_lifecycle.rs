//! Shared descriptive lifecycle examples are canonical and reader-compatible.
//! These checks do not qualify network lifecycle behavior.

use std::path::Path;

use mqttiumd_contract::command::Recipe;
use mqttiumd_contract::session::{SessionAction, SessionEndReason, SessionExpiry};
use serde_json::Value;

fn example(name: &str) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let bytes = std::fs::read(root.join(format!("contracts/examples/v1/valid/{name}.json")))
        .expect("descriptive example");
    serde_json::from_slice(&bytes).expect("JSON example")
}

fn last_recipe(snapshot: &Value) -> Recipe {
    let encoded = snapshot["operations"]
        .as_array()
        .expect("receipts")
        .last()
        .expect("lifecycle receipt")["recipe_hex"]
        .as_str()
        .expect("canonical hex");
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&encoded[at..at + 2], 16).expect("hex byte"))
        .collect::<Vec<_>>();
    let recipe = Recipe::decode_canonical(&bytes, 16, 1024).expect("canonical lifecycle");
    let mut encoded_again = Vec::new();
    recipe.encode_canonical(&mut encoded_again).expect("encode");
    assert_eq!(encoded_again, bytes);
    recipe
}

#[test]
fn kind_eight_examples_preserve_distinct_open_and_close_identities() {
    let Recipe::SessionLifecycle(open) =
        last_recipe(&example("store-snapshot-session-open-receipt"))
    else {
        panic!("open example must use kind8");
    };
    let SessionAction::Open {
        client_id,
        expected,
        clean_start,
        expiry,
    } = open.action
    else {
        panic!("open action");
    };
    assert_eq!(client_id.as_slice(), b"new-client");
    assert!(expected.is_none());
    assert!(clean_start);
    assert_eq!(expiry, SessionExpiry::OnDisconnect);

    let Recipe::SessionLifecycle(close) =
        last_recipe(&example("store-snapshot-session-close-receipt"))
    else {
        panic!("close example must use kind8");
    };
    let SessionAction::Close { guard, reason } = close.action else {
        panic!("close action");
    };
    assert_eq!(guard.session.get(), 1);
    assert!(guard.opened_by.is_none());
    assert_eq!(reason, SessionEndReason::Disconnect);
    assert_ne!(open.operation, close.operation);
}

#[test]
fn finite_lifecycle_examples_preserve_the_original_detachment_across_readers() {
    let Recipe::SessionLifecycle(open) =
        last_recipe(&example("store-snapshot-session-finite-open"))
    else {
        panic!("finite open example");
    };
    let SessionAction::OpenAt { expiry, now, .. } = open.action else {
        panic!("time-aware open action");
    };
    assert_eq!(expiry, SessionExpiry::After(7));
    assert_eq!(now, 100);
    let Recipe::SessionLifecycle(detach) =
        last_recipe(&example("store-snapshot-session-finite-detached"))
    else {
        panic!("detach example");
    };
    let SessionAction::Detach {
        guard,
        expiry,
        observed_at,
    } = detach.action
    else {
        panic!("detach action");
    };
    assert_eq!(guard.opened_by, Some(open.operation));
    assert_eq!(expiry, SessionExpiry::After(7));
    assert_eq!(observed_at, now);
    let Recipe::SessionLifecycle(expire) =
        last_recipe(&example("store-snapshot-session-finite-expired"))
    else {
        panic!("expiry example");
    };
    let SessionAction::Expire {
        guard: expired_guard,
        detached,
        now,
    } = expire.action
    else {
        panic!("expiry action");
    };
    assert_eq!(expired_guard, guard);
    assert_eq!(detached.operation, detach.operation);
    assert_eq!(detached.deadline, Some(observed_at + 7_000_000_000));
    assert_eq!(Some(now), detached.deadline);
}
