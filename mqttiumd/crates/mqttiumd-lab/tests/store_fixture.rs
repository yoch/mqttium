//! A fixture is set up through valid commands, never by writing state.

use std::path::Path;

use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::ids::{Revision, RunEpoch, SessionId, StoreIncarnation};
use mqttiumd_contract::packet::Qos;
use mqttiumd_lab::store::durable::{
    Accounts, AdminCommand, AdminDone, AdminRefused, Applied, DurableState, DurableStore, Limits,
    MemoryStore, RecoveryCursor, RecoveryPage, RecoveryRefused, Resolved, RouteTarget,
    SessionState,
};
use mqttiumd_lab::store::fixture::{FixtureError, FixturePlan, install, plan};
use serde_json::{Value, json};

fn repository_fixture() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/p1/three-persistent-sessions.json");
    let bytes = std::fs::read(path).expect("the fixture of the repository");
    serde_json::from_slice(&bytes).expect("JSON")
}

fn planned() -> FixturePlan {
    plan(&repository_fixture()).expect("a valid fixture")
}

#[test]
fn the_plan_holds_what_the_fixture_declares() {
    let plan = planned();
    assert_eq!(plan.incarnation, StoreIncarnation::new(1));
    assert_eq!(
        plan.limits,
        Limits {
            payload_bytes: PayloadBytes::new(4096),
            references: References::new(16),
            exchange_slots: ExchangeSlots::new(4),
            receipt_slots: ReceiptSlots::new(8),
            max_targets_per_admission: 4,
            max_topic_bytes: 256,
            max_payload_bytes: 1024,
            // Not declared by a fixture: exactly what it sets up.
            max_sessions: 3,
            max_route_targets: 4,
        }
    );
    assert_eq!(plan.operations_in_flight, 2);
    assert_eq!(plan.result_slots, 8);
    let sessions: Vec<_> = plan
        .sessions
        .iter()
        .map(|session| (session.client.as_str(), session.client_id.as_slice()))
        .collect();
    assert_eq!(
        sessions,
        [
            ("producer", b"p1-producer".as_slice()),
            ("b", b"p1-b".as_slice()),
            ("c", b"p1-c".as_slice()),
        ]
    );
    assert_eq!(plan.routes.len(), 1);
    assert_eq!(plan.routes[0].topic, b"p1/example");
    assert_eq!(
        plan.routes[0].targets,
        [
            ("b".to_owned(), Qos::AtLeastOnce),
            ("c".to_owned(), Qos::AtLeastOnce)
        ]
    );
}

#[test]
fn the_fixture_is_installed_and_opens_no_epoch() {
    let plan = planned();
    let mut store = MemoryStore::create(plan.incarnation, plan.limits);
    let installed = install(&mut store, &plan).expect("installed");

    let state = store.snapshot();
    let session = |client: &str| installed.sessions[client];
    assert_eq!(installed.sessions.len(), 3);
    for (client, client_id) in [
        ("producer", b"p1-producer".as_slice()),
        ("b", b"p1-b"),
        ("c", b"p1-c"),
    ] {
        let record = &state.sessions[&session(client)];
        assert_eq!(record.client_id, client_id);
        assert_eq!(record.revision, Revision::new(1));
        assert_eq!(record.state, SessionState::Active);
    }
    assert_eq!(state.sessions.len(), 3);
    assert_eq!(
        state.routes[b"p1/example".as_slice()],
        [
            RouteTarget {
                session: session("b"),
                qos: Qos::AtLeastOnce
            },
            RouteTarget {
                session: session("c"),
                qos: Qos::AtLeastOnce
            },
        ]
    );
    assert_eq!(state.routes.len(), 1);
    assert_eq!(installed.route_revision, state.route_revision);
    assert_eq!(state.accounts, Accounts::default());
    // Opening an epoch is start-up's, not the fixture's.
    assert_eq!(state.ledger.open_epoch(), None);
    assert_eq!(state.ledger.last_opened(), RunEpoch::new(0));
    assert!(state.operations.is_empty());
}

/// A store that only counts what reaches it. A fixture must go through
/// the commands of the writer and through nothing else.
struct CommandsOnly {
    inner: MemoryStore,
    commands: Vec<AdminCommand>,
}

impl DurableStore for CommandsOnly {
    fn ledger(&self) -> mqttiumd_contract::command::EpochLedger {
        self.inner.ledger()
    }
    fn open_epoch(
        &mut self,
        _: &mqttiumd_contract::command::OpenEpoch,
    ) -> mqttiumd_contract::outcome::OpeningOutcome {
        panic!("a fixture opens no epoch");
    }
    fn apply(&mut self, _: &mqttiumd_contract::command::Command) -> Applied {
        panic!("a fixture applies no recipe");
    }
    fn established(
        &self,
        _: mqttiumd_contract::ids::OperationId,
    ) -> Option<mqttiumd_contract::outcome::StoreOutcome> {
        panic!("a fixture reads no operation");
    }
    fn resolve_and_fence(&mut self, _: mqttiumd_contract::ids::OperationId) -> Resolved {
        panic!("a fixture resolves nothing");
    }
    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused> {
        self.commands.push(command.clone());
        self.inner.admin(command)
    }
    fn recover(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        self.inner.recover(cursor, limit)
    }
    fn snapshot(&self) -> DurableState {
        self.inner.snapshot()
    }
}

#[test]
fn the_fixture_reaches_the_store_as_commands_in_its_own_order() {
    let plan = planned();
    let mut store = CommandsOnly {
        inner: MemoryStore::create(plan.incarnation, plan.limits),
        commands: Vec::new(),
    };
    let installed = install(&mut store, &plan).expect("installed");
    let session = |client: &str| installed.sessions[client];
    assert_eq!(
        store.commands,
        [
            AdminCommand::CreateSession {
                client_id: b"p1-producer".to_vec()
            },
            AdminCommand::CreateSession {
                client_id: b"p1-b".to_vec()
            },
            AdminCommand::CreateSession {
                client_id: b"p1-c".to_vec()
            },
            AdminCommand::AddRoute {
                topic: b"p1/example".to_vec(),
                session: session("b"),
                qos: Qos::AtLeastOnce
            },
            AdminCommand::AddRoute {
                topic: b"p1/example".to_vec(),
                session: session("c"),
                qos: Qos::AtLeastOnce
            },
        ]
    );
    // Session identities come from the store, not from the fixture.
    assert_eq!(session("producer"), SessionId::new(1));
}

fn edited(edit: impl FnOnce(&mut Value)) -> Value {
    let mut fixture = repository_fixture();
    edit(&mut fixture);
    fixture
}

#[test]
fn a_fixture_the_reader_cannot_use_is_refused() {
    let malformed = [
        edited(|fixture| fixture["store"]["incarnation"] = json!(1)),
        edited(|fixture| fixture["store"]["incarnation"] = json!("01")),
        edited(|fixture| fixture["capacities"]["references"] = json!("-1")),
        edited(|fixture| fixture["capacities"]["payload_bytes"] = json!("18446744073709551616")),
        edited(|fixture| {
            fixture["capacities"]
                .as_object_mut()
                .expect("an object")
                .remove("receipt_slots");
        }),
        edited(|fixture| fixture["sessions"] = json!({})),
        edited(|fixture| fixture["sessions"][0]["client_id"] = json!(7)),
        edited(|fixture| fixture["routes"][0]["targets"][0]["qos"] = json!(3)),
        edited(|fixture| fixture["routes"][0]["topic"] = json!(null)),
        // One more session than the schema allows: refused before any
        // of them is read.
        edited(|fixture| {
            let one = fixture["sessions"][0].clone();
            fixture["sessions"] = Value::Array(vec![one; 65]);
        }),
    ];
    for fixture in &malformed {
        assert!(
            matches!(plan(fixture), Err(FixtureError::Malformed(_))),
            "{fixture}"
        );
    }
}

#[test]
fn a_command_the_store_refuses_stops_the_installation() {
    let unknown = plan(&edited(|fixture| {
        fixture["routes"][0]["targets"][1]["client"] = json!("nobody");
    }))
    .expect("the shape is valid");
    let mut store = MemoryStore::create(unknown.incarnation, unknown.limits);
    assert_eq!(
        install(&mut store, &unknown),
        Err(FixtureError::UnknownClient("nobody".to_owned()))
    );

    let duplicate = plan(&edited(|fixture| {
        fixture["sessions"][2]["client_id"] = json!("p1-b");
    }))
    .expect("the shape is valid");
    let mut store = MemoryStore::create(duplicate.incarnation, duplicate.limits);
    assert_eq!(
        install(&mut store, &duplicate),
        Err(FixtureError::Refused(AdminRefused::DuplicateClientId))
    );
    // The commands before the refused one were committed, each as a
    // whole: two sessions, and no half of the third.
    assert_eq!(store.snapshot().sessions.len(), 2);
}
