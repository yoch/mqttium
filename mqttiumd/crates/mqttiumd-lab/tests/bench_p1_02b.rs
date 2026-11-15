//! P1-02b through the components (ADR 0001, step 3 as record 0015
//! corrects it): no crash, a held command, a fence refused, a held
//! unknown answer, a lost commit result and two deadlines.

#[path = "bench_support.rs"]
mod bench_support;

use bench_support::{records, run_scenario};

const SCENARIO: &str = "tests/scenarios/p1/P1-02b.json";

#[test]
fn p1_02b_is_played_to_its_last_step_without_a_crash() {
    let campaign = run_scenario(SCENARIO, 1, "p102b-played");
    assert_eq!(campaign.report.stopped, None);
    assert!(campaign.report.not_reached.is_empty());
    let trace = records(&campaign.trace());
    assert!(trace.iter().all(|record| record.kind != "BrokerCrashed"));
    let kinds: Vec<&str> = trace.iter().map(|record| record.kind.as_str()).collect();
    let at = |kind: &str, nth: usize| {
        kinds
            .iter()
            .enumerate()
            .filter(|(_, found)| **found == kind)
            .nth(nth)
            .map(|(index, _)| index)
            .unwrap_or_else(|| panic!("{kind} #{nth}"))
    };
    // The first deadline, its resolution, its fence refused and its
    // unknown answer held; then the command commits and its result is
    // lost; the second deadline, a commit reported, PUBREC.
    assert!(at("DeadlineDelivered", 0) < at("ResolveSubmitted", 0));
    assert!(at("ResolveSubmitted", 0) < at("StoreFenceRefused", 0));
    assert!(at("StoreFenceRefused", 0) < at("StoreCommitStable", 0));
    assert!(at("StoreCommitStable", 0) < at("DeadlineDelivered", 1));
    let pubrec = trace
        .iter()
        .position(|record| {
            record.kind == "PeerPacketObserved" && record.payload["packet_kind"] == "PUBREC"
        })
        .expect("PUBREC");
    assert!(at("DeadlineDelivered", 1) < pubrec);
    assert_eq!(trace[pubrec].payload["reason_code"], 0);
    // The held unknown arrives after PUBREC. Nothing of the core follows
    // it until the client's PUBREL.
    let late = trace
        .iter()
        .position(|record| {
            record.kind == "StoreResultDelivered" && record.payload["outcome"] == "PendingOrUnknown"
        })
        .expect("the late unknown answer");
    assert!(late > pubrec);
    let next = trace[late + 1..]
        .iter()
        .find(|record| record.origin == "broker-core")
        .expect("a later record of the core");
    assert_eq!(next.kind, "PacketDelivered");
    assert_eq!(next.payload["packet_kind"], "PUBREL");
    // One admission, one release, one PUBREC, one PUBCOMP.
    let count = |kind: &str, member: &str, value: &str| {
        trace
            .iter()
            .filter(|record| record.kind == kind && record.payload[member] == value)
            .count()
    };
    assert_eq!(
        count("CommandSubmitted", "command_kind", "AdmitPublication"),
        1
    );
    assert_eq!(
        count("CommandSubmitted", "command_kind", "ReleaseInbound"),
        1
    );
    assert_eq!(count("PeerPacketObserved", "packet_kind", "PUBREC"), 1);
    assert_eq!(count("PeerPacketObserved", "packet_kind", "PUBCOMP"), 1);
    for checkpoint in ["after_commit", "final"] {
        campaign.cites_snapshot(checkpoint);
    }
}

#[test]
fn p1_02b_gives_the_same_trace_for_the_same_seed() {
    let first = run_scenario(SCENARIO, 5, "p102b-same");
    let second = run_scenario(SCENARIO, 5, "p102b-same");
    assert_eq!(first.trace(), second.trace());
}
