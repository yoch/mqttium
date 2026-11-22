//! A violation of the store contract through the components: one
//! operation presented with two recipes.
//!
//! The deterministic store is wrapped so that, before the first admission
//! of the core is applied, the same operation is applied once with other
//! payload bytes under the same identity. The real admission then finds
//! the operation decided with another payload: the store answers
//! `Violation(RecipeMismatch)`. The bench must write the answer at the
//! three ends of its path (ready in the writer, handed over by the
//! adapter, delivered to the core) with the same content, and no positive
//! acknowledgement may follow.

#[path = "bench_support.rs"]
mod bench_support;

use std::fs;

use mqttiumd_contract::command::{Command, EpochLedger, OpenEpoch, Recipe};
use mqttiumd_contract::ids::{ConnectionGeneration, OperationId};
use mqttiumd_contract::outcome::{OpeningOutcome, StoreOutcome};
use mqttiumd_contract::packet::SharedBytes;
use mqttiumd_lab::bench::backend::Memory;
use mqttiumd_lab::bench::campaign::Setup;
use mqttiumd_lab::bench::collector::{Collector, KindTable, Origin};
use mqttiumd_lab::bench::mqtt::peer;
use mqttiumd_lab::bench::process::{Books, Choice, Process};
use mqttiumd_lab::store::adapter::StoreAction;
use mqttiumd_lab::store::durable::{
    AdminCommand, AdminDone, AdminRefused, Applied, DurableState, DurableStore, MemoryStore,
    ReadBudget, ReadError, RecoveryCursor, RecoveryPage, RecoveryRefused, Resolved,
};
use serde_json::{Value, json};

use bench_support::root;

/// The deterministic store, with one admission presented twice under one
/// identity: first with other bytes, then as the core sent it.
struct TwoRecipes {
    inner: MemoryStore,
    done: bool,
}

impl DurableStore for TwoRecipes {
    fn ledger(&self) -> Result<EpochLedger, ReadError> {
        self.inner.ledger()
    }

    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError> {
        self.inner.open_epoch(command)
    }

    fn apply(&mut self, command: &Command) -> Applied {
        if !self.done
            && matches!(command.recipe.get(), Recipe::AdmitPublication(_))
            && let Some(payload) = &command.payload
        {
            self.done = true;
            let other: Vec<u8> = payload
                .as_slice()
                .iter()
                .map(u8::to_ascii_uppercase)
                .collect();
            let first = Command {
                recipe: command.recipe.clone(),
                payload: Some(SharedBytes::copy_from(&other, other.len()).unwrap()),
            };
            let applied = self.inner.apply(&first);
            assert!(
                matches!(
                    applied.reply,
                    mqttiumd_contract::outcome::StoreReply::Outcome(StoreOutcome::Committed(_))
                ),
                "the first recipe commits: {applied:?}"
            );
        }
        self.inner.apply(command)
    }

    fn established(&self, operation: OperationId) -> Result<Option<StoreOutcome>, ReadError> {
        self.inner.established(operation)
    }

    fn resolve_and_fence(&mut self, operation: OperationId) -> Resolved {
        self.inner.resolve_and_fence(operation)
    }

    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused> {
        self.inner.admin(command)
    }

    fn recover_with_budget(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        self.inner.recover_with_budget(cursor, limit, budget)
    }

    fn snapshot_with_budget(&self, budget: ReadBudget) -> Result<DurableState, ReadError> {
        self.inner.snapshot_with_budget(budget)
    }
}

fn records(collector: Collector) -> Vec<Value> {
    let (trace, _) = collector.finish(false);
    trace
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

#[test]
fn a_violation_is_written_at_the_three_ends_and_nothing_positive_follows() {
    let read = |path: &str| {
        mqttiumd_lab::strict::parse(&fs::read(root().join(path)).expect(path)).expect(path)
    };
    let fixture = read("tests/fixtures/p1/three-persistent-sessions.json");
    let schema = read("contracts/schema/v1/trace-record.schema.json");
    let setup =
        Setup::from_fixture(&mut Memory, &fixture, "bench-violation", 1, 1).expect("the fixture");
    let mut collector = Collector::new(
        "bench-violation",
        KindTable::from_schema(&schema).expect("the schema"),
        1 << 24,
    );
    let mut books = Books::new(0);
    let mut choices = Vec::new();
    let store = TwoRecipes {
        inner: setup.store,
        done: false,
    };
    let mut process = Process::start(
        store,
        0,
        setup.settings,
        &[],
        &mut collector,
        &mut books,
        &mut choices,
    )
    .expect("the real start-up");
    process
        .recover(&mut collector, &mut books)
        .expect("recovery");

    let connection = ConnectionGeneration::new(1);
    process.connect("producer", connection);
    let send = |process: &mut Process<TwoRecipes>, collector: &mut Collector, bytes: &[u8]| {
        let sent = collector
            .emit(
                Origin::Peer,
                "PeerPacketSent",
                &[],
                json!({"client": "producer", "packet_kind": "CONNECT", "bytes_hex": ""}),
            )
            .expect("recorded");
        process
            .receive(connection, bytes, sent, collector)
            .expect("received");
    };
    send(
        &mut process,
        &mut collector,
        &peer::connect(b"p1-producer").unwrap(),
    );
    let publish = peer::publish(b"p1/example", 2, false, false, Some(7), b"payload").unwrap();
    // Every choice offered, the first one each time, until nothing is
    // eligible or the core stops on the violation.
    let mut published = false;
    let mut stop = None;
    for _ in 0..64 {
        let offered = process.choices();
        let Some(choice) = offered.first().cloned() else {
            if published {
                break;
            }
            published = true;
            send(&mut process, &mut collector, &publish);
            continue;
        };
        if let Err(stopped) = process
            .perform(&choice, &mut collector, &mut books)
            .and_then(|_| process.settle(&mut collector, &mut books))
        {
            stop = Some(stopped);
            break;
        }
        assert!(
            !matches!(choice, Choice::Store(StoreAction::EnterWriter(_))) || published,
            "nothing enters the writer before the PUBLISH"
        );
    }
    assert!(published, "the PUBLISH was sent");
    let trace = records(collector);
    let violation = |kind: &str| -> Vec<Value> {
        trace
            .iter()
            .filter(|record| record["kind"] == kind && record["payload"]["outcome"] == "Violation")
            .map(|record| record["payload"].clone())
            .collect()
    };
    let ready = violation("StoreResultReady");
    let handed = violation("StoreResultHandedOver");
    let delivered = violation("StoreResultDelivered");
    assert_eq!(ready.len(), 1, "{stop:?}");
    assert_eq!(ready[0]["violation"], "RecipeMismatch");
    assert!(ready[0].get("reject_reason").is_none(), "{}", ready[0]);
    // The same content at each end, but for what only the writer states.
    let fields = |payload: &Value| {
        json!({
            "effect_id": payload["effect_id"],
            "operation_id": payload["operation_id"],
            "outcome": payload["outcome"],
            "violation": payload["violation"],
        })
    };
    assert_eq!(handed.len(), 1);
    assert_eq!(delivered.len(), 1, "the core took the violation: {stop:?}");
    assert_eq!(fields(&handed[0]), fields(&ready[0]));
    assert_eq!(fields(&delivered[0]), fields(&ready[0]));
    assert_eq!(delivered[0], handed[0]);
    // After the violation, no acknowledgement that claims a commit.
    let at = trace
        .iter()
        .position(|record| {
            record["kind"] == "StoreResultDelivered" && record["payload"]["outcome"] == "Violation"
        })
        .unwrap();
    assert!(
        trace[at..].iter().all(|record| {
            !(record["kind"] == "FrameEligible"
                && record["payload"]["packet_kind"] == "PUBREC"
                && record["payload"]["reason"] == "Success")
        }),
        "a positive PUBREC after a violation"
    );
}
