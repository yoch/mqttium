//! A typed failure during fixture/create setup must still leave a readable run.
use super::*;
use crate::store::durable::MemoryStore;
use mqttiumd_contract::command::{Command as StoreCommand, EpochLedger, OpenEpoch};
use mqttiumd_contract::ids::{OperationId, StoreIncarnation};
use mqttiumd_contract::outcome::{OpeningOutcome, StoreOutcome};
use mqttiumd_store_api::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum At {
    Open,
    Snapshot,
}
struct FailingStore {
    inner: MemoryStore,
    error: ReadError,
}
impl DurableStore for FailingStore {
    fn ledger(&self) -> Result<EpochLedger, ReadError> {
        self.inner.ledger()
    }
    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError> {
        self.inner.open_epoch(command)
    }
    fn apply(&mut self, command: &StoreCommand) -> Applied {
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
    fn snapshot_with_budget(&self, _: ReadBudget) -> Result<DurableState, ReadError> {
        Err(self.error)
    }
}
struct FailingBackend {
    at: At,
    error: ReadError,
}
impl Backend for FailingBackend {
    type Store = FailingStore;
    fn name(&self) -> &'static str {
        "sim-store"
    }
    fn create(
        &mut self,
        incarnation: StoreIncarnation,
        limits: Limits,
    ) -> Result<FailingStore, ReadError> {
        if self.at == At::Open {
            Err(self.error)
        } else {
            Ok(FailingStore {
                inner: MemoryStore::create(incarnation, limits),
                error: self.error,
            })
        }
    }
    fn reopen_after_crash(&mut self, store: FailingStore) -> Result<FailingStore, ReadError> {
        Ok(store)
    }
    fn inspect(&self, store: Option<&FailingStore>) -> Result<DurableState, ReadError> {
        store.ok_or(ReadError::Unavailable)?.snapshot()
    }
    fn describe(&self) -> Value {
        json!({"engine":"deterministic test reader"})
    }
    fn crash(&self) -> &'static str {
        "no process started"
    }
}

#[test]
fn typed_setup_failures_retain_trace_and_result_before_any_process_starts() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture = fs::read(root.join("tests/fixtures/p1/three-persistent-sessions.json")).unwrap();
    let fixture: Value = serde_json::from_slice(&fixture).unwrap();
    for at in [At::Open, At::Snapshot] {
        for error in [
            ReadError::InvalidData,
            ReadError::LimitExceeded,
            ReadError::Unavailable,
            ReadError::Io,
            ReadError::AllocationFailed,
        ] {
            let phase = if at == At::Open { "Open" } else { "Snapshot" };
            let out = std::env::temp_dir().join(format!(
                "mqttiumd-setup-failure-{}-{phase}-{}",
                std::process::id(),
                error.code()
            ));
            let _ = fs::remove_dir_all(&out);
            let request = Request {
                root: root.clone(),
                contracts: PathBuf::from("contracts/v1"),
                scenario: PathBuf::from("tests/scenarios/p1/P1-02.json"),
                out: out.clone(),
                seed: 1,
                run_id: Some("failed-setup".to_owned()),
                backend: "sim-store".to_owned(),
                built_from: None,
                process: "in-process".to_owned(),
            };
            let mut backend = FailingBackend { at, error };
            let failed = Setup::from_fixture(&mut backend, &fixture, "failed-setup", 1, 1);
            let error_before_transport = match failed {
                Err(error) => error,
                Ok(_) => panic!("the faulty setup succeeded"),
            };
            assert_eq!(
                error_before_transport.failure.unwrap().payload()["code"],
                error.code()
            );
            let report = execute_with(&request, backend)
                .expect("a typed failed setup still leaves campaign artifacts");
            assert!(report.stopped.is_some());
            let records: Vec<Value> = fs::read_to_string(out.join("trace.jsonl"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(records.len(), 3, "header, typed failure, incomplete footer");
            assert_eq!(records[1]["kind"], "DurableReadFailed");
            assert_eq!(records[1]["payload"]["code"], error.code());
            assert_eq!(records[1]["payload"]["phase"], phase);
            assert_eq!(records[1]["payload"]["process_incarnation"], "1");
            assert_eq!(records[2]["payload"]["complete"], false);
            let result: Value =
                serde_json::from_slice(&fs::read(out.join("run-result.json")).unwrap()).unwrap();
            assert_eq!(result["verdict"], "INCONCLUSIVE");
            assert_eq!(result["qualification"], "provisional");
            assert_eq!(result["failure_count"], "1");
            assert_eq!(result["failures"][0]["code"], error.code());
            assert_eq!(result["failures"][0]["phase"], phase);
            let workspace = Workspace::open(&out, Path::new("contracts/v1")).unwrap();
            let validation = workspace.check_file(Path::new("run-result.json"));
            assert!(validation.is_accepted(), "{:?}", validation.diagnostics);
            fs::remove_dir_all(out).unwrap();
        }
    }
}
