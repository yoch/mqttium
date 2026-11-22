//! The broker acknowledges in the order of reception (record 0050), on the
//! bench: the real core, a store and their adapters, judged by the oracle.
//!
//! Scenario `ORDER-A` holds two admissions of one client in the transit of
//! the store adapter and releases the second first. The result of the
//! second admission is applied while the first is still to be committed:
//! the core keeps its PUBREC and makes it eligible after that of the
//! first. No scenario written before had the results of two exchanges of
//! one connection reach the core in the reverse order of their PUBLISH
//! packets.
//!
//! The oracle has no assertion on the order of two acknowledgements: it
//! judges the run as any other, and this test reads the order on the
//! records of the trace.

#[path = "bench_support.rs"]
mod bench_support;

use bench_support::{judge, not_passing, records, run_scenario_on};

const SCENARIO: &str = "tests/scenarios/order/ORDER-A.json";

#[test]
fn a_pubrec_waits_for_that_of_an_earlier_publication_whose_result_is_late() {
    for backend in ["sim-store", "sqlite"] {
        for seed in 1..=4 {
            let case = format!("{backend} seed {seed}");
            let campaign = run_scenario_on(SCENARIO, seed, "order", backend);
            assert_eq!(campaign.report.stopped, None, "{case}");
            assert!(campaign.report.not_reached.is_empty(), "{case}");

            // What the core learned and what it made eligible, in the order
            // of the trace: the operation of each admission result, by its
            // sequence, and the Packet Identifier of each PUBREC.
            let mut story = Vec::new();
            for record in records(&campaign.trace()) {
                let payload = &record.payload;
                if record.kind == "StoreResultDelivered" {
                    let sequence = payload["operation_id"]["sequence"]
                        .as_str()
                        .expect("a sequence")
                        .to_owned();
                    story.push(format!("result {sequence}"));
                } else if record.kind == "FrameEligible" && payload["packet_kind"] == "PUBREC" {
                    story.push(format!("PUBREC {}", payload["packet_id"]));
                }
            }
            // Operations 1 and 2 admit Packet Identifiers 7 and 8; the two
            // releases follow. The result of the second admission comes
            // first, and no PUBREC leaves between the two results.
            assert_eq!(
                story[..4],
                ["result 2", "result 1", "PUBREC 7", "PUBREC 8"],
                "{case}: {story:?}"
            );

            let verdict = judge(&campaign, SCENARIO);
            assert_eq!(
                verdict["verdict"],
                "PASS",
                "{case}: {:?}",
                not_passing(&verdict)
            );
        }
    }
}
