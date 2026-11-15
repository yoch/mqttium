//! QABS-A, B and C through the components (DEM-018, record 0015): a
//! commit against a fence, a fence against a held command, and a fence
//! that cannot be written while the command stays held.

#[path = "bench_support.rs"]
mod bench_support;

use bench_support::{Record, records, run_scenario};

fn played(letter: &str, checkpoint: &str) -> Vec<Record> {
    let campaign = run_scenario(
        &format!("tests/scenarios/qabs/QABS-{letter}.json"),
        1,
        &format!("qabs-{letter}"),
    );
    assert_eq!(campaign.report.stopped, None, "QABS-{letter}");
    assert!(campaign.report.not_reached.is_empty(), "QABS-{letter}");
    campaign.cites_snapshot(checkpoint);
    records(&campaign.trace())
}

fn count(trace: &[Record], kind: &str) -> usize {
    trace.iter().filter(|record| record.kind == kind).count()
}

fn outcomes(trace: &[Record], kind: &str) -> Vec<String> {
    trace
        .iter()
        .filter(|record| record.kind == kind)
        .map(|record| record.payload["outcome"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn qabs_a_the_command_commits_before_the_held_fence_enters_the_writer() {
    let trace = played("A", "final");
    assert_eq!(count(&trace, "StoreFenceStable"), 0);
    assert_eq!(count(&trace, "StoreCommitStable"), 2, "admission, release");
    // The held fence reads again in the writer and finds the commit.
    assert_eq!(
        outcomes(&trace, "StoreResultDelivered"),
        ["Committed", "Committed", "Committed"]
    );
    assert!(trace.iter().any(|record| {
        record.kind == "PeerPacketObserved"
            && record.payload["packet_kind"] == "PUBREC"
            && record.payload["reason_code"] == 0
    }));
}

#[test]
fn qabs_b_the_fence_commits_while_the_command_is_held_and_the_command_is_refused() {
    let trace = played("B", "final");
    assert_eq!(count(&trace, "StoreFenceStable"), 1);
    assert_eq!(
        count(&trace, "StoreCommitStable"),
        0,
        "the admission never executes"
    );
    let pubrec = trace
        .iter()
        .find(|record| {
            record.kind == "PeerPacketObserved" && record.payload["packet_kind"] == "PUBREC"
        })
        .expect("a refusing PUBREC");
    assert_eq!(pubrec.payload["reason_code"], 0x83);
    // The released command answers a certain rejection, held at the
    // barrier: the core has not consumed it when the run ends.
    assert_eq!(
        outcomes(&trace, "StoreResultReady"),
        ["RejectedCertain", "RejectedCertain"]
    );
    assert_eq!(
        outcomes(&trace, "StoreResultDelivered"),
        ["RejectedCertain"]
    );
}

#[test]
fn qabs_c_the_fence_is_refused_and_the_command_stays_held() {
    let trace = played("C", "unknown");
    assert_eq!(count(&trace, "StoreFenceRefused"), 1);
    assert_eq!(count(&trace, "StoreFenceStable"), 0);
    assert_eq!(count(&trace, "StoreCommitStable"), 0);
    assert_eq!(
        count(&trace, "HeldItemReleased"),
        0,
        "the command stays held"
    );
    assert_eq!(outcomes(&trace, "StoreResultReady"), ["PendingOrUnknown"]);
    assert!(
        trace
            .iter()
            .all(|record| record.payload["packet_kind"] != "PUBREC"),
        "no acknowledgement while the outcome is unknown"
    );
}
