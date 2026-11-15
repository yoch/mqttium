//! A command parked inside SQLite stays in flight until SIGKILL.
#[path = "bench_support.rs"]
mod bench_support;

use mqttiumd_lab::bench::scenario::Scenario;
use serde_json::{Value, json};

#[test]
fn the_scenario_reader_preserves_the_transaction_points() {
    for point in ["store.commit.prepared", "store.commit.logged"] {
        let mut document: Value = serde_json::from_slice(
            &std::fs::read(bench_support::root().join("tests/scenarios/p1/P1-02.json")).unwrap(),
        ).unwrap();
        document["faults"][0]["point"] = json!(point);
        document["faults"][0]["action"] = json!("barrier");
        let scenario = Scenario::read(&document).expect("a declared transaction point");
        assert_eq!(scenario.faults[0].point.name(), point);
    }
}
