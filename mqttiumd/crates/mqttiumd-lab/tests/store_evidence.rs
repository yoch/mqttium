//! The evidence of the store in the formats of lot L3: every snapshot
//! and every trace record written here is checked against the schemas
//! and the semantic pass of `contracts/v1`, and against the examples of
//! the corpus where one describes the same state.

use std::path::Path;

use serde_json::Value;

use mqttiumd_contract::capacity::References;
use mqttiumd_contract::effects::{Effect, EffectSink, OwnedEffect, SubmitResult};
use mqttiumd_contract::ids::{
    EffectId, EffectSeq, LogicalOwnerId, OperationId, Revision, StoreIncarnation,
};
use mqttiumd_lab::store::adapter::{
    AdapterLimits, FaultAction, FaultPoint, FaultSpec, Performed, SimStore, StoreAction,
    StoreEvent, WorkId, WorkKind,
};
use mqttiumd_lab::store::conformance::{World, altered, fixture_limits, operation_id};
use mqttiumd_lab::store::durable::{AdminCommand, DurableStore, Limits, MemoryStore};
use mqttiumd_lab::store::evidence::{SnapshotContext, TraceWriter, snapshot};
use mqttiumd_lab::workspace::Workspace;

fn workspace() -> Workspace {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    Workspace::open(&root, Path::new("contracts/v1")).expect("the contracts open")
}

fn accepted(workspace: &Workspace, document: &Value) {
    let report = workspace.check_value(document);
    assert!(
        report.is_accepted(),
        "{:#?}\n{}",
        report.diagnostics,
        serde_json::to_string_pretty(document).expect("JSON")
    );
}

fn example(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/examples/v1/valid")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).expect("the example")).expect("JSON")
}

fn memory(incarnation: StoreIncarnation, limits: Limits) -> MemoryStore {
    MemoryStore::create(incarnation, limits)
}

const CONTEXT: SnapshotContext<'static> = SnapshotContext {
    run_id: "p1-02-example",
    checkpoint: "after_commit_before_crash",
    backend: "sim-store",
};

/// The state the example `store-snapshot-after-commit` describes: the
/// fixture, then one admission towards `b` and `c`. Everything but the
/// metadata of the data set is compared member by member; the example
/// was written for an opening and a number of commits of its own.
#[test]
fn a_snapshot_after_a_commit_is_the_example_of_the_corpus() {
    let mut limits = fixture_limits();
    limits.max_route_targets = 8;
    let mut world = World::with_limits(&memory, limits);
    let command = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&command).committed_now);
    let written = snapshot(&world.store.snapshot(), CONTEXT);
    let workspace = workspace();
    accepted(&workspace, &written);

    let expected = example("store-snapshot-after-commit.json");
    for member in [
        "format",
        "schema_version",
        "run_id",
        "checkpoint",
        "backend",
        "sessions",
        "fences",
        "payloads",
        "publications",
        "obligations",
        "inbound_exchanges",
        "account",
    ] {
        assert_eq!(written[member], expected[member], "{member}");
    }
    // One difference, stated: the example gives the fixture's route the
    // table revision 1, and this store advances the revision once per
    // target added, so the route table and the recipe that carries it
    // say 2. Everything else of them is the example's.
    assert_eq!(
        written["route_table"]["routes"],
        expected["route_table"]["routes"]
    );
    assert_eq!(written["route_table"]["revision"], "2");
    assert_eq!(expected["route_table"]["revision"], "1");
    let (ours, theirs) = (&written["operations"][0], &expected["operations"][0]);
    assert_eq!(ours["operation_id"], theirs["operation_id"]);
    assert_eq!(ours["decision"], theirs["decision"]);
    let (ours, theirs) = (
        ours["recipe_hex"].as_str().expect("hex"),
        theirs["recipe_hex"].as_str().expect("hex"),
    );
    let differing: Vec<usize> = ours
        .bytes()
        .zip(theirs.bytes())
        .enumerate()
        .filter(|(_, (left, right))| left != right)
        .map(|(index, _)| index)
        .collect();
    assert_eq!(ours.len(), theirs.len());
    assert_eq!(differing.len(), 1, "{differing:?}");
    assert_eq!(&ours[differing[0]..=differing[0]], "2");
    assert_eq!(written["operations"].as_array().map(Vec::len), Some(1));
    for member in [
        "incarnation",
        "last_opened_epoch",
        "collected_through",
        "next_session",
    ] {
        assert_eq!(
            written["meta"][member], expected["meta"][member],
            "{member}"
        );
    }
    assert!(written["meta"]["opened_by"]["startup_hex"].is_string());
}

/// Every kind of row: a commit, a capacity rejection with its resource,
/// a fence, a terminal session, a release; and the empty store.
#[test]
fn every_snapshot_of_the_store_is_accepted() {
    let workspace = workspace();
    let empty = memory(StoreIncarnation::new(1), fixture_limits());
    accepted(&workspace, &snapshot(&empty.snapshot(), CONTEXT));

    let mut limits = fixture_limits();
    limits.references = References::new(2);
    let mut world = World::with_limits(&memory, limits);
    let first = world.admission(1, 7, b"payload", &[world.b, world.c]);
    assert!(world.store.apply(&first).committed_now);
    let release = world.release(2, 7, 1);
    assert!(world.store.apply(&release).committed_now);
    let refused = world.admission(3, 8, b"payload", &[world.b]);
    let _ = world.store.apply(&refused);
    let precondition = altered(&world.admission(4, 9, b"p", &[world.b]), |admit| {
        admit.source.session_revision = Revision::new(9);
    });
    let _ = world.store.apply(&precondition);
    let _ = world.store.resolve_and_fence(operation_id(world.epoch, 5));
    assert!(
        world
            .store
            .admin(AdminCommand::DeleteSession { session: world.c })
            .is_ok()
    );
    let state = world.store.snapshot();
    assert!(!state.fences.is_empty());
    let written = snapshot(&state, CONTEXT);
    accepted(&workspace, &written);
    let decisions: Vec<&Value> = written["operations"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| &row["decision"])
        .collect();
    assert!(
        decisions
            .iter()
            .any(|decision| decision["resource"] == "references")
    );
    assert!(
        written["sessions"]
            .as_array()
            .expect("rows")
            .iter()
            .any(|row| row["state"] == "Terminal")
    );
}

type Bench = World<SimStore<MemoryStore>>;

fn effect(bench: &Bench, sequence: u64) -> EffectId {
    EffectId {
        epoch: bench.epoch,
        owner: LogicalOwnerId::new(0),
        sequence: EffectSeq::new(sequence),
    }
}

fn submit(bench: &mut Bench, sequence: u64, body: Effect) -> WorkId {
    let id = effect(bench, sequence);
    assert_eq!(
        bench.store.try_submit(OwnedEffect::new(id, body)),
        SubmitResult::Accepted
    );
    WorkId::Effect(id)
}

fn resolve(bench: &mut Bench, sequence: u64, operation: OperationId) -> WorkId {
    submit(bench, sequence, Effect::ResolveAndFence { operation })
}

fn perform(bench: &mut Bench, action: StoreAction) -> Performed {
    bench.store.perform(action).expect("eligible")
}

/// A run that leaves every kind of evidence the adapter records, written
/// as trace records: each one is accepted by the schema and the semantic
/// pass, in one sequence, and nothing it records is left unwritten.
#[test]
fn the_evidence_of_a_run_is_written_as_accepted_trace_records() {
    let mut bench = World::new(&memory).wrap(|store| {
        SimStore::new(
            store,
            AdapterLimits {
                operations_in_flight: 4,
                result_slots: 8,
                faults: 4,
                events: 256,
            },
        )
    });
    let hold = FaultSpec {
        point: FaultPoint::CommandTransit,
        action: FaultAction::HoldCommand,
        kind: Some(WorkKind::AdmitPublication),
        source: None,
        occurrence: 1,
    };
    let refuse = FaultSpec {
        point: FaultPoint::FenceTransit,
        action: FaultAction::RefuseFence,
        kind: Some(WorkKind::ResolveAndFence),
        source: None,
        occurrence: 2,
    };
    bench.store.arm(hold).expect("allowed");
    bench.store.arm(refuse).expect("allowed");
    let names = ["held_admission", "refused_fence"];

    // A held admission, a fence that is written, one that is refused.
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let held = submit(
        &mut bench,
        1,
        Effect::Persist {
            command: command.clone(),
        },
    );
    let absent = operation_id(bench.epoch, 9);
    let fenced = resolve(&mut bench, 2, absent);
    perform(&mut bench, StoreAction::EnterWriter(fenced));
    perform(&mut bench, StoreAction::DeliverResult(fenced));
    let refused = resolve(&mut bench, 3, command.recipe.operation());
    perform(&mut bench, StoreAction::EnterWriter(refused));
    perform(&mut bench, StoreAction::DeliverResult(refused));
    // The admission is released and commits; its release follows.
    perform(&mut bench, StoreAction::ReleaseCommand(held));
    perform(&mut bench, StoreAction::EnterWriter(held));
    perform(&mut bench, StoreAction::DeliverResult(held));
    let released = bench.release(2, 7, 1);
    let release = submit(&mut bench, 4, Effect::Persist { command: released });
    perform(&mut bench, StoreAction::EnterWriter(release));
    perform(&mut bench, StoreAction::DeliverResult(release));
    // A result that answers nothing, and a recovery in pages.
    let WorkId::Effect(stray) = fenced else {
        panic!("an effect");
    };
    let _ = bench.store.present_result(
        stray,
        operation_id(bench.epoch, 9),
        mqttiumd_contract::outcome::StoreReply::Outcome(
            mqttiumd_contract::outcome::StoreOutcome::PendingOrUnknown,
        ),
    );
    let mut cursor = None;
    loop {
        let page = bench.store.recover(cursor, 4).expect("a page");
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    let events = bench.store.take_events();
    let mut writer = TraceWriter::new("p1-02-example", 1, 1, &names);
    let written = writer.write(&events);
    // The delivery of a result is the core's record, not the adapter's.
    assert!(
        written
            .unwritten
            .iter()
            .all(|event| matches!(event, StoreEvent::ResultDelivered { .. })),
        "{:#?}",
        written.unwritten
    );
    let workspace = workspace();
    let mut kinds = Vec::new();
    for (index, record) in written.records.iter().enumerate() {
        accepted(&workspace, record);
        assert_eq!(record["record_sequence"], (index + 1).to_string());
        let kind = record["kind"].as_str().expect("a kind").to_owned();
        if !kinds.contains(&kind) {
            kinds.push(kind);
        }
    }
    kinds.sort();
    assert_eq!(
        kinds,
        [
            "CommandSubmitted",
            "FaultHit",
            "HeldItemReleased",
            "InputRefused",
            "RecoveryPageRead",
            "ResolveSubmitted",
            "StoreCommitStable",
            "StoreFenceRefused",
            "StoreFenceStable",
            "StoreResultReady",
        ]
    );
    // The payload of the commit is the example's.
    let commit = written
        .records
        .iter()
        .find(|record| record["kind"] == "StoreCommitStable")
        .expect("a commit");
    assert_eq!(
        commit["payload"],
        example("trace-record-store-commit-stable.json")["payload"]
    );
    let hit = written
        .records
        .iter()
        .find(|record| {
            record["kind"] == "FaultHit" && record["payload"]["fault_id"] == "refused_fence"
        })
        .expect("the refusal was reached");
    assert_eq!(hit["payload"]["occurrence"], "2");
    assert_eq!(hit["payload"]["point"], "store.fence.transit");
    let find = |kind: &str| {
        written
            .records
            .iter()
            .find(|record| record["kind"] == kind)
            .unwrap_or_else(|| panic!("no {kind}"))
    };
    assert_eq!(
        find("InputRefused")["payload"]["reason"],
        "no-effect-in-flight"
    );
    assert_eq!(
        find("RecoveryPageRead")["payload"]["run_epoch"],
        bench.epoch.get().to_string()
    );
    let released = find("HeldItemReleased");
    assert_eq!(released["payload"]["fault_id"], "held_admission");
    let held = written
        .records
        .iter()
        .find(|record| {
            record["kind"] == "FaultHit" && record["payload"]["fault_id"] == "held_admission"
        })
        .expect("the hold was reached");
    assert_eq!(
        released["causal_parents"][0]["origin_sequence"],
        held["origin_sequence"]
    );
    assert_eq!(
        released["causal_parents"][0]["origin_component"],
        "fault-injector"
    );
    // Each component numbers its own records from zero, without a gap.
    let mut next = std::collections::BTreeMap::new();
    for record in &written.records {
        let component = record["origin_component"].as_str().expect("a component");
        let expected: &mut u64 = next.entry(component).or_insert(0);
        assert_eq!(
            record["origin_sequence"],
            expected.to_string(),
            "{component}"
        );
        *expected += 1;
    }
    assert!(next.len() >= 4, "{next:?}");
}
