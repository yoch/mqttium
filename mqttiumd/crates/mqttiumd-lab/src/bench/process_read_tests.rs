use super::*;
use crate::bench::backend::Memory;
use crate::bench::collector::KindTable;
use crate::bench::failure::{Failure, ReadCause};
use crate::bench::setup::Setup;
use crate::store::durable::MemoryStore;
use mqttiumd_contract::command::{Command, EpochLedger, OpenEpoch};
use mqttiumd_contract::outcome::{OpeningOutcome, StoreOutcome};
use mqttiumd_store_api::*;
use std::cell::Cell;

struct MovingFrontier {
    inner: MemoryStore,
    calls: Cell<u32>,
}
impl DurableStore for MovingFrontier {
    fn ledger(&self) -> Result<EpochLedger, ReadError> {
        self.inner.ledger()
    }
    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError> {
        self.inner.open_epoch(command)
    }
    fn apply(&mut self, command: &Command) -> Applied {
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
        let call = self.calls.get();
        self.calls.set(call + 1);
        match call {
            0 => self.inner.recover_with_budget(cursor, limit, budget),
            1 => Err(RecoveryRefused::FrontierMoved),
            // An implementation that silently resets/retries would recover
            // to readiness on call 3. Keep that old defect finite in this test.
            _ => self.inner.recover_with_budget(None, 64, budget),
        }
    }
    fn snapshot_with_budget(&self, budget: ReadBudget) -> Result<DurableState, ReadError> {
        self.inner.snapshot_with_budget(budget)
    }
}

#[test]
fn a_moved_frontier_after_one_page_stays_partial_without_reset_or_retry() {
    let fixture: Value = crate::strict::parse(include_bytes!(
        "../../../../tests/fixtures/p1/three-persistent-sessions.json"
    ))
    .unwrap();
    let Setup {
        mut settings,
        store,
        ..
    } = Setup::from_fixture(&mut Memory, &fixture, "frontier-refusal", 1, 0).unwrap();
    settings.recovery_page = 3;
    let schema = crate::strict::parse(include_bytes!(
        "../../../../contracts/schema/v1/trace-record.schema.json"
    ))
    .unwrap();
    let mut collector = Collector::new(
        settings.run_id,
        KindTable::from_schema(&schema).unwrap(),
        1 << 20,
    );
    let mut books = Books::new(0);
    let store = MovingFrontier {
        inner: store,
        calls: Cell::new(0),
    };
    let mut process = Process::start(
        store,
        0,
        settings,
        &[],
        &mut collector,
        &mut books,
        &mut Vec::new(),
    )
    .unwrap();
    let expected_durable = process.store().snapshot().unwrap();
    assert_eq!(process.owner.revision().get(), 0);
    let failure = process
        .recover(&mut collector, &mut books)
        .expect_err("a later complete inventory cannot replace this partial attempt");
    assert_eq!(
        failure.failure,
        Some(Failure::Read {
            cause: ReadCause::FrontierMoved,
            phase: ReadPhase::Recovery,
            process_incarnation: 1,
            page: Some(1)
        })
    );
    assert_eq!(process.store().calls.get(), 2, "no implicit retry");
    assert!(
        process.owner.is_recovering(),
        "a partial owner is not ready"
    );
    assert_eq!(
        process.owner.revision().get(),
        1,
        "the valid prefix was preserved, not reset"
    );
    assert_eq!(
        process.store().snapshot().unwrap(),
        expected_durable,
        "only the already completed startup opening may have changed durable state"
    );
    let (trace, refused) = collector.finish(false);
    assert!(refused.is_empty());
    let records: Vec<Value> = trace
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    let pages: Vec<&Value> = records
        .iter()
        .filter(|record| record["kind"] == "RecoveryPageDelivered")
        .collect();
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0]["payload"]["last"], false);
    assert!(records.iter().all(|record| !matches!(
        record["kind"].as_str(),
        Some("StoreCommitStable" | "FrameWritten" | "PeerPacketObserved")
    )));
}
