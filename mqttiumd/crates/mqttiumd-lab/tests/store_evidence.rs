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
    AdapterLimits, Answer, FaultAction, FaultPoint, FaultSpec, Performed, SimStore, StoreAction,
    StoreEvent, WorkId, WorkKind, startup_nonce,
};
use mqttiumd_lab::store::conformance::{World, altered, fixture_limits, operation_id};
use mqttiumd_lab::store::durable::{AdminCommand, DurableStore, Limits, MemoryStore};
use mqttiumd_lab::store::evidence::{
    SnapshotContext, TraceWriter, recovered_fact, snapshot, store_outcome,
};
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
    // The example gives the fixture's table the revision this store writes,
    // one per target added: 2. The route table and the operation, recipe
    // included, are the example's.
    assert_eq!(written["route_table"], expected["route_table"]);
    assert_eq!(written["operations"], expected["operations"]);
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
    // Every event is written: a delivery as the adapter's handover; the
    // core writes what it consumed.
    assert!(written.unwritten.is_empty(), "{:#?}", written.unwritten);
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
            "StoreResultHandedOver",
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

/// The public forms of a recovered fact and of a result's outcome are
/// exactly what `TraceWriter` writes, so that the records of the core
/// (lot L4) and of the store have one writing of each.
#[test]
fn the_public_forms_are_what_the_trace_writer_writes() {
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
    // A commit, a fence of an absence, a rejection after it.
    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let admitted = submit(
        &mut bench,
        1,
        Effect::Persist {
            command: command.clone(),
        },
    );
    perform(&mut bench, StoreAction::EnterWriter(admitted));
    perform(&mut bench, StoreAction::DeliverResult(admitted));
    let absent = operation_id(bench.epoch, 9);
    let fenced = resolve(&mut bench, 2, absent);
    perform(&mut bench, StoreAction::EnterWriter(fenced));
    perform(&mut bench, StoreAction::DeliverResult(fenced));
    let late = altered(&bench.admission(9, 8, b"p", &[bench.b]), |_| {});
    let rejected = submit(&mut bench, 3, Effect::Persist { command: late });
    perform(&mut bench, StoreAction::EnterWriter(rejected));
    let mut cursor = None;
    loop {
        let page = bench.store.recover(cursor, 3).expect("a page");
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let events = bench.store.take_events();
    let names: [&str; 0] = [];
    let written = TraceWriter::new("public-forms", 1, 1, &names).write(&events);

    let mut facts = Vec::new();
    let mut outcomes = Vec::new();
    for event in &events {
        match event {
            StoreEvent::RecoveryPageRead { facts: page, .. } => {
                facts.extend(page.iter().map(recovered_fact));
            }
            StoreEvent::ResultReady {
                answer: Answer::Operation { reply, .. },
                ..
            } => {
                let (outcome, reason) = store_outcome(*reply).expect("an outcome");
                outcomes.push((outcome.to_owned(), reason.map(str::to_owned)));
            }
            _ => {}
        }
    }
    let of_kind = |kind: &str| {
        written
            .records
            .iter()
            .filter(|record| record["kind"] == kind)
            .collect::<Vec<_>>()
    };
    let written_facts: Vec<Value> = of_kind("RecoveryPageRead")
        .iter()
        .flat_map(|record| {
            record["payload"]["facts"]
                .as_array()
                .expect("facts")
                .clone()
        })
        .collect();
    assert!(!facts.is_empty());
    assert_eq!(written_facts, facts);
    let written_outcomes: Vec<(String, Option<String>)> = of_kind("StoreResultReady")
        .iter()
        .map(|record| {
            (
                record["payload"]["outcome"]
                    .as_str()
                    .expect("an outcome")
                    .to_owned(),
                record["payload"]["reject_reason"]
                    .as_str()
                    .map(str::to_owned),
            )
        })
        .collect();
    assert_eq!(written_outcomes, outcomes);
    let kinds: Vec<_> = outcomes
        .iter()
        .map(|(outcome, _)| outcome.as_str())
        .collect();
    assert_eq!(kinds, ["Committed", "RejectedCertain", "RejectedCertain"]);
    assert!(
        outcomes
            .iter()
            .any(|(_, reason)| reason.as_deref() == Some("Fenced"))
    );
}

fn adapter_bench() -> Bench {
    World::new(&memory).wrap(|store| {
        SimStore::new(
            store,
            AdapterLimits {
                operations_in_flight: 4,
                result_slots: 8,
                faults: 4,
                events: 256,
            },
        )
    })
}

fn of_kind<'a>(records: &'a [Value], kind: &str) -> Vec<&'a Value> {
    records
        .iter()
        .filter(|record| record["kind"] == kind)
        .collect()
}

/// The reference a record makes to `parent` in its `causal_parents`.
fn reference(parent: &Value) -> Value {
    serde_json::json!({
        "origin_component": parent["origin_component"],
        "origin_incarnation": parent["origin_incarnation"],
        "origin_sequence": parent["origin_sequence"],
    })
}

/// K1: a result the adapter hands over is written as
/// `StoreResultHandedOver`, with the content of the `StoreResultReady`
/// the store produced and that record as its parent: committed,
/// rejected, delayed then released. A duplicate's second copy is not
/// handed over. A result presented from outside was produced by no
/// store: its handover has no such parent.
#[test]
fn a_result_handed_over_follows_the_result_the_store_produced() {
    let mut bench = adapter_bench();
    let delay = FaultSpec {
        point: FaultPoint::ResultReady,
        action: FaultAction::DelayResult,
        kind: Some(WorkKind::ResolveAndFence),
        source: None,
        occurrence: 1,
    };
    let duplicate = FaultSpec {
        point: FaultPoint::ResultReady,
        action: FaultAction::DuplicateResult,
        kind: Some(WorkKind::AdmitPublication),
        source: None,
        occurrence: 1,
    };
    bench.store.arm(delay).expect("allowed");
    bench.store.arm(duplicate).expect("allowed");
    let names = ["delayed_fence", "duplicated_admission"];

    let command = bench.admission(1, 7, b"payload", &[bench.b, bench.c]);
    let admitted = submit(&mut bench, 1, Effect::Persist { command });
    perform(&mut bench, StoreAction::EnterWriter(admitted));
    perform(&mut bench, StoreAction::DeliverResult(admitted));
    assert_eq!(
        perform(&mut bench, StoreAction::DeliverResult(admitted)),
        Performed::Uncorrelated
    );
    let absent = operation_id(bench.epoch, 9);
    let fenced = resolve(&mut bench, 2, absent);
    perform(&mut bench, StoreAction::EnterWriter(fenced));
    perform(&mut bench, StoreAction::ReleaseResult(fenced));
    perform(&mut bench, StoreAction::DeliverResult(fenced));
    // A result from outside, for work in flight whose own result the
    // store already produced: the outside one answers it first.
    let other = operation_id(bench.epoch, 10);
    let outside = resolve(&mut bench, 3, other);
    perform(&mut bench, StoreAction::EnterWriter(outside));
    let WorkId::Effect(outside_effect) = outside else {
        panic!("an effect");
    };
    let presented = bench.store.present_result(
        outside_effect,
        other,
        mqttiumd_contract::outcome::StoreReply::Outcome(
            mqttiumd_contract::outcome::StoreOutcome::PendingOrUnknown,
        ),
    );
    assert!(matches!(presented, Ok(Performed::Delivered { .. })));
    assert_eq!(
        perform(&mut bench, StoreAction::DeliverResult(outside)),
        Performed::Uncorrelated
    );

    let events = bench.store.take_events();
    let written = TraceWriter::new("k1-handover", 1, 1, &names).write(&events);
    let workspace = workspace();
    for record in &written.records {
        accepted(&workspace, record);
    }
    let ready = of_kind(&written.records, "StoreResultReady");
    let handed = of_kind(&written.records, "StoreResultHandedOver");
    assert_eq!(ready.len(), 3);
    assert_eq!(handed.len(), 3, "{:#?}", written.unwritten);
    for (handover, produced) in handed.iter().zip(&ready).take(2) {
        assert_eq!(handover["origin_component"], "store-adapter");
        assert_eq!(handover["payload"], produced["payload"]);
        assert_eq!(
            handover["causal_parents"],
            serde_json::json!([reference(produced)])
        );
    }
    assert_eq!(handed[1]["payload"]["reject_reason"], "Fenced");
    let foreign = handed[2];
    assert_eq!(foreign["payload"]["outcome"], "PendingOrUnknown");
    assert!(
        foreign["causal_parents"]
            .as_array()
            .expect("parents")
            .iter()
            .all(|parent| parent["origin_component"] != "store-evidence")
    );
    assert!(
        !written
            .unwritten
            .iter()
            .any(|event| matches!(event, StoreEvent::ResultDelivered { .. }))
    );
}

/// The opening of an epoch is written as `OpeningSubmitted`, the whole
/// command, then `StoreEpochOpened` once, from the ledger, caused by it.
/// The same command answered `Committed` again opens nothing and writes
/// no second `StoreEpochOpened`. The reply itself is the core's record.
#[test]
fn an_opening_is_written_as_submitted_then_opened_once() {
    let mut bench = adapter_bench();
    // One more epoch, so that `after` differs from the incarnation.
    mqttiumd_lab::store::conformance::open(bench.store.store_mut());
    let mut opening = bench.store.begin_opening(startup_nonce(7, 1));
    let command = opening
        .submit(bench.store.store().ledger().last_opened())
        .expect("an attempt");
    let id = WorkId::Opening(command.attempt);
    for _ in 0..2 {
        bench.store.submit_opening(command).expect("room");
        perform(&mut bench, StoreAction::EnterWriter(id));
        perform(&mut bench, StoreAction::DeliverResult(id));
    }
    let events = bench.store.take_events();
    let names: [&str; 0] = [];
    let written = TraceWriter::new("opening", 1, 1, &names).write(&events);
    let workspace = workspace();
    for record in &written.records {
        accepted(&workspace, record);
    }
    let attempt = serde_json::json!({
        "startup_hex": "00000000000000070000000000000001",
        "ticket": command.attempt.ticket.get().to_string(),
    });
    assert_ne!(command.after.get(), command.incarnation.get());
    let submitted = of_kind(&written.records, "OpeningSubmitted");
    assert_eq!(submitted.len(), 2);
    for record in &submitted {
        assert_eq!(record["origin_component"], "store-adapter");
        assert_eq!(
            record["payload"],
            serde_json::json!({
                "attempt": attempt,
                "incarnation": command.incarnation.get().to_string(),
                "after": command.after.get().to_string(),
            })
        );
    }
    let opened = of_kind(&written.records, "StoreEpochOpened");
    assert_eq!(opened.len(), 1);
    assert_eq!(opened[0]["origin_component"], "store-evidence");
    assert_eq!(
        opened[0]["payload"],
        serde_json::json!({
            "incarnation": command.incarnation.get().to_string(),
            "epoch": (command.after.get() + 1).to_string(),
            "attempt": attempt,
        })
    );
    assert_eq!(
        opened[0]["causal_parents"],
        serde_json::json!([reference(submitted[0])])
    );
    // The replies are handed to the core, which writes them.
    assert!(written.unwritten.iter().all(|event| matches!(
        event,
        StoreEvent::ResultReady { .. } | StoreEvent::ResultDelivered { .. }
    )));
}
