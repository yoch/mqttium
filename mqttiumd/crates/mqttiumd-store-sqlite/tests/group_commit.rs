//! Group commit (record 0045): `SqliteStore::apply_group` runs several
//! commands in one transaction and answers each as `apply` would have,
//! one command after the other, on the same starting state.
//!
//! The reference of every comparison is a second SQLite store that receives
//! the same commands through `apply`, one transaction each: the replies, the
//! bounded snapshot and the raw rows of every table must be equal. What a
//! group changes is the crash granularity only, shown at the end with a
//! process killed at the two kill points of the store (feature
//! `kill-points`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References, Resource,
};
use mqttiumd_contract::command::{
    ChangeSubscriptions, ClaimDelivery, Command, FinishDelivery, FrozenRecipe, Recipe,
    UpdateSubscriptions,
};
use mqttiumd_contract::ids::{DeliveryId, OperationId, OutboundPacketId, Revision, RunEpoch};
use mqttiumd_contract::outcome::{RejectReason, StoreOutcome, StoreReply, StoreViolation};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::pubsub::{
    GrantedSubscriptionChange, SubscriptionChange, SubscriptionOptions,
};
use mqttiumd_contract::retained::{ReplaySubscriptions, RetainedPublication};
use mqttiumd_contract::session::{
    SessionAction, SessionEndReason, SessionExpiry, SessionGuard, SessionLifecycle,
};
use mqttiumd_lab::store::conformance::{
    TOPIC, World, accounts_match_what_is_held, admission_in, altered, fixture_limits, operation_id,
    publication_id,
};
use mqttiumd_store_api::{Applied, Decision, Durability, DurableStore, Limits, Resolved};
use mqttiumd_store_sqlite::{GroupRefused, MAX_GROUP, SqliteStore};

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A file no other test uses, in a directory of its own.
fn fresh_path(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "mqttiumd-group-commit-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&directory).expect("a temporary directory");
    directory.join(format!("{name}.sqlite"))
}

/// The fixture limits with room for many exchanges and receipts.
fn roomy() -> Limits {
    Limits {
        payload_bytes: PayloadBytes::new(1 << 20),
        references: References::new(100_000),
        exchange_slots: ExchangeSlots::new(4),
        receipt_slots: ReceiptSlots::new(100_000),
        ..fixture_limits()
    }
}

/// What `apply` answers when nothing certain can be said.
const UNKNOWN: Applied = Applied {
    reply: StoreReply::Outcome(StoreOutcome::PendingOrUnknown),
    committed_now: false,
    wrote: false,
};

fn rejected(reason: RejectReason) -> StoreReply {
    StoreReply::Outcome(StoreOutcome::RejectedCertain(reason))
}

fn command(recipe: Recipe) -> Command {
    Command {
        recipe: FrozenRecipe::freeze(recipe).expect("well formed"),
        payload: None,
    }
}

fn operation(command: &Command) -> OperationId {
    command.recipe.get().operation()
}

/// Every row of every table, as the engine returns it, read on a
/// connection of its own while the writer lives. Two stores that received
/// the same writes hold the same rows in the same order.
fn tables(path: &Path) -> BTreeMap<String, Vec<String>> {
    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("a reader");
    let names: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
        .expect("the schema")
        .query_map([], |row| row.get(0))
        .expect("the tables")
        .collect::<Result<_, _>>()
        .expect("their names");
    assert!(names.iter().any(|name| name == "operations"));
    names
        .into_iter()
        .map(|name| {
            let mut statement = conn
                .prepare(&format!("SELECT * FROM \"{name}\""))
                .expect("the table");
            let columns = statement.column_count();
            let rows = statement
                .query_map([], |row| {
                    let mut cells = Vec::with_capacity(columns);
                    for index in 0..columns {
                        cells.push(row.get::<_, rusqlite::types::Value>(index)?);
                    }
                    Ok(format!("{cells:?}"))
                })
                .expect("its rows")
                .collect::<Result<Vec<String>, _>>()
                .expect("decoded");
            (name, rows)
        })
        .collect()
}

/// Two SQLite stores prepared alike. One receives groups, the other the
/// same commands one transaction each.
struct Twin {
    grouped: World<SqliteStore>,
    sequential: World<SqliteStore>,
    grouped_path: PathBuf,
    sequential_path: PathBuf,
}

impl Twin {
    fn new(name: &str, limits: Limits) -> Self {
        let grouped_path = fresh_path(&format!("{name}-grouped"));
        let sequential_path = fresh_path(&format!("{name}-sequential"));
        let world = |path: &Path| {
            World::with_limits(
                &|incarnation, limits| {
                    SqliteStore::create(path, incarnation, limits).expect("a new store")
                },
                limits,
            )
        };
        let twin = Self {
            grouped: world(&grouped_path),
            sequential: world(&sequential_path),
            grouped_path,
            sequential_path,
        };
        twin.same_state();
        twin
    }

    /// The same commands on both stores, one transaction each.
    fn both(&mut self, commands: &[Command]) -> Vec<Applied> {
        commands
            .iter()
            .map(|command| {
                let applied = self.sequential.store.apply(command);
                assert_eq!(self.grouped.store.apply(command), applied);
                applied
            })
            .collect()
    }

    /// One group on one store, the same commands in order on the other:
    /// equal replies, equal states. Returns the replies.
    fn compare(&mut self, commands: &[Command]) -> Vec<Applied> {
        let members: Vec<&Command> = commands.iter().collect();
        let grouped = self
            .grouped
            .store
            .apply_group(&members)
            .expect("within the bound");
        let sequential: Vec<Applied> = commands
            .iter()
            .map(|command| self.sequential.store.apply(command))
            .collect();
        assert_eq!(grouped, sequential, "the replies of the group");
        self.same_state();
        grouped
    }

    fn same_state(&self) {
        let grouped = self.grouped.store.snapshot().expect("healthy snapshot");
        let sequential = self.sequential.store.snapshot().expect("healthy snapshot");
        assert_eq!(accounts_match_what_is_held(&sequential), Ok(()));
        assert_eq!(grouped, sequential, "the durable state");
        assert_eq!(
            tables(&self.grouped_path),
            tables(&self.sequential_path),
            "the rows of every table"
        );
    }
}

/// One admission towards `b` and `c` and its release, numbered `n`.
fn pair<S>(world: &World<S>, n: u64) -> [Command; 2] {
    let packet = u16::try_from(n).expect("fits");
    [
        world.admission(2 * n, packet, b"payload", &[world.b, world.c]),
        world.release(2 * n + 1, packet, 2 * n),
    ]
}

fn pairs<S>(world: &World<S>, numbers: std::ops::RangeInclusive<u64>) -> Vec<Command> {
    numbers.flat_map(|n| pair(world, n)).collect()
}

fn committed_now(replies: &[Applied]) -> Vec<bool> {
    replies
        .iter()
        .map(|applied| applied.committed_now)
        .collect()
}

fn wrote(replies: &[Applied]) -> Vec<bool> {
    replies.iter().map(|applied| applied.wrote).collect()
}

#[test]
fn admissions_with_fan_out_and_their_releases_commit_as_one_group() {
    let mut twin = Twin::new("fan-out", roomy());
    let before = twin.sequential.store.snapshot().expect("healthy").revision;
    let commands = pairs(&twin.sequential, 1..=4);
    let replies = twin.compare(&commands);
    assert_eq!(committed_now(&replies), [true; 8]);
    assert_eq!(wrote(&replies), [true; 8]);
    let state = twin.grouped.store.snapshot().expect("healthy");
    assert_eq!(state.revision, before + 8, "one revision for each member");
    assert_eq!(state.obligations.len(), 8, "two destinations each");
    assert!(state.inbound.is_empty(), "each release ended its exchange");
    // A second group continues from the state the first one left.
    let replies = twin.compare(&pairs(&twin.sequential, 5..=7));
    assert_eq!(committed_now(&replies), [true; 6]);
}

#[test]
fn a_group_survives_reopening_like_its_sequential_twin() {
    let mut twin = Twin::new("reopen", roomy());
    let commands = pairs(&twin.sequential, 1..=3);
    twin.compare(&commands);
    let expected = twin.sequential.store.snapshot().expect("healthy");
    let Twin {
        grouped,
        grouped_path,
        ..
    } = twin;
    drop(grouped);
    let mut reopened = SqliteStore::open(&grouped_path).expect("reopened");
    assert_eq!(reopened.snapshot().expect("healthy"), expected);
    // Presented again, every member is answered from its durable record.
    let members: Vec<&Command> = commands.iter().collect();
    let again = reopened.apply_group(&members).expect("within the bound");
    assert_eq!(again.len(), commands.len());
    assert!(again.iter().all(|applied| matches!(
        applied.reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ) && !applied.committed_now
        && !applied.wrote));
    assert_eq!(reopened.snapshot().expect("healthy"), expected);
}

#[test]
fn claims_finishes_and_subscription_changes_see_the_earlier_members() {
    let mut twin = Twin::new("claims", roomy());
    let world = &twin.sequential;
    let epoch = world.epoch;
    let delivery = |index| DeliveryId {
        publication: publication_id(epoch, 1),
        index,
    };
    let claim = |sequence, target, index| {
        command(Recipe::ClaimDelivery(ClaimDelivery {
            operation: operation_id(epoch, sequence),
            delivery: delivery(index),
            target,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).expect("non-zero"),
        }))
    };
    let finish = |sequence, target, index, claimed_as| {
        command(Recipe::FinishDelivery(FinishDelivery {
            operation: operation_id(epoch, sequence),
            delivery: delivery(index),
            target,
            target_revision: Revision::new(1),
            packet_id: OutboundPacketId::new(7).expect("non-zero"),
            claim: operation_id(epoch, claimed_as),
        }))
    };
    let remove = command(Recipe::ChangeSubscriptions(ChangeSubscriptions {
        operation: operation_id(epoch, 6),
        session: world.b,
        session_revision: Revision::new(1),
        route_revision: world.route_revision,
        changes: BoundedVec::from_slice(
            &[SubscriptionChange {
                topic: SharedBytes::copy_from(TOPIC, 256).expect("fits"),
                subscribe: false,
            }],
            8,
        )
        .expect("fits"),
    }));
    // Each member needs what the one before it wrote: the claim its
    // obligation, the finish its claim.
    let commands = [
        world.admission(1, 7, b"payload", &[world.b, world.c]),
        world.release(2, 7, 1),
        claim(3, world.b, 0),
        claim(4, world.c, 1),
        finish(5, world.b, 0, 3),
        remove,
    ];
    let replies = twin.compare(&commands);
    assert_eq!(committed_now(&replies), [true; 6]);
    let state = twin.grouped.store.snapshot().expect("healthy");
    assert_eq!(state.obligations.len(), 1);
    assert_eq!(state.outbound.len(), 1);
    // The next group finishes what the first one claimed.
    let replies = twin.compare(&[finish(7, twin.sequential.c, 1, 4)]);
    assert_eq!(committed_now(&replies), [true]);
    let state = twin.grouped.store.snapshot().expect("healthy");
    assert!(state.obligations.is_empty() && state.outbound.is_empty());
}

/// The admission `sequence` as a retained publication on its own topic.
fn retained<S>(world: &World<S>, sequence: u64, payload: &[u8]) -> Command {
    let plain = world.admission(
        sequence,
        u16::try_from(sequence).expect("fits"),
        payload,
        &[],
    );
    let mut canonical = Vec::new();
    plain
        .recipe
        .get()
        .encode_canonical(&mut canonical)
        .expect("well formed");
    let Ok(Recipe::AdmitPublication(mut admission)) = Recipe::decode_canonical(&canonical, 16, 256)
    else {
        panic!("not an admission");
    };
    admission.retain = true;
    admission.costs.references = References::new(1);
    Command {
        recipe: FrozenRecipe::freeze(Recipe::RetainedPublication(RetainedPublication {
            admission,
            retain_targets: 0,
        }))
        .expect("well formed"),
        payload: plain.payload,
    }
}

#[test]
fn retained_publications_and_their_replay_commit_as_one_group() {
    let limits = Limits {
        references: References::new(128),
        receipt_slots: ReceiptSlots::new(8192),
        payload_bytes: PayloadBytes::new(4 * 1024 * 1024),
        ..fixture_limits()
    };
    let mut twin = Twin::new("retained", limits);
    let world = &twin.sequential;
    let replay = command(Recipe::ReplaySubscriptions(ReplaySubscriptions {
        update: UpdateSubscriptions {
            operation: operation_id(world.epoch, 2),
            session: world.b,
            session_revision: Revision::new(1),
            route_revision: world.route_revision,
            changes: BoundedVec::from_slice(
                &[GrantedSubscriptionChange {
                    topic: SharedBytes::copy_from(TOPIC, 256).expect("fits"),
                    granted: Some(Qos::AtMostOnce),
                    options: SubscriptionOptions::DEFAULT,
                }],
                8,
            )
            .expect("fits"),
        },
        first_publication: publication_id(world.epoch, 1000),
    }));
    // The second member replaces the value the first one retained, and the
    // replay freezes what the two members before it left.
    let commands = [
        retained(world, 1, b"frozen"),
        retained(world, 3, b"newer"),
        replay,
    ];
    let replies = twin.compare(&commands);
    assert_eq!(committed_now(&replies), [true; 3]);
    let state = twin.grouped.store.snapshot().expect("healthy");
    assert_eq!(state.retained.len(), 1);
    let kept = state.retained.values().next().expect("one retained value");
    assert_eq!(kept.publication, publication_id(twin.sequential.epoch, 3));
}

fn lifecycle<S>(world: &World<S>, sequence: u64, replace: bool) -> Command {
    let guard = SessionGuard {
        session: world.b,
        opened_by: None,
    };
    let action = if replace {
        SessionAction::Open {
            client_id: SharedBytes::copy_from(b"p1-b", 1024).expect("fits"),
            expected: Some(guard),
            clean_start: true,
            expiry: SessionExpiry::Never,
        }
    } else {
        SessionAction::Close {
            guard,
            reason: SessionEndReason::Disconnect,
        }
    };
    command(Recipe::SessionLifecycle(SessionLifecycle {
        operation: operation_id(world.epoch, sequence),
        action,
    }))
}

fn session_lifecycle_in_a_group(name: &str, replace: bool) {
    let mut twin = Twin::new(name, roomy());
    let prefix = pairs(&twin.sequential, 1..=3);
    assert_eq!(committed_now(&twin.both(&prefix)), [true; 6]);
    let world = &twin.sequential;
    // The target is also a producer, with a claimed delivery of its own.
    let own = altered(
        &world.admission(8, 19, b"B-produced-for-C", &[world.c]),
        |admit| {
            admit.source.session = world.b;
            admit.source.exchange.session = world.b;
        },
    );
    let claim = command(Recipe::ClaimDelivery(ClaimDelivery {
        operation: operation_id(world.epoch, 9),
        delivery: DeliveryId {
            publication: publication_id(world.epoch, 2),
            index: 0,
        },
        target: world.b,
        target_revision: Revision::new(1),
        packet_id: OutboundPacketId::new(7).expect("non-zero"),
    }));
    // After the lifecycle command the old incarnation of `b` is gone: an
    // admission that still names it is refused inside the same group.
    let late = world.admission(11, 21, b"too late", &[world.b]);
    let commands = [own, claim, lifecycle(world, 10, replace), late];
    let replies = twin.compare(&commands);
    assert_eq!(committed_now(&replies), [true, true, true, false]);
    assert_eq!(
        replies[3].reply,
        rejected(RejectReason::PreconditionFailed),
        "the last member saw the lifecycle command before it"
    );
    assert!(replies[3].wrote, "its refusal is recorded");
    let state = twin.grouped.store.snapshot().expect("healthy");
    assert!(!state.sessions.contains_key(&twin.sequential.b));
}

#[test]
fn a_session_close_in_a_group_cancels_what_the_earlier_members_wrote() {
    session_lifecycle_in_a_group("lifecycle-close", false);
}

#[test]
fn a_session_replacement_in_a_group_cancels_what_the_earlier_members_wrote() {
    session_lifecycle_in_a_group("lifecycle-replace", true);
}

#[test]
fn an_operation_presented_twice_in_one_group_is_decided_once() {
    let mut twin = Twin::new("duplicate", roomy());
    let world = &twin.sequential;
    let admission = world.admission(1, 7, b"payload", &[world.b, world.c]);
    // The same identity under another recipe.
    let other = altered(&admission, |admit| admit.retain = true);
    let release = world.release(2, 7, 1);
    let commands = [
        admission.clone(),
        admission.clone(),
        other,
        release.clone(),
        release,
        admission,
    ];
    let replies = twin.compare(&commands);
    assert_eq!(
        committed_now(&replies),
        [true, false, false, true, false, false]
    );
    assert_eq!(wrote(&replies), [true, false, false, true, false, false]);
    assert_eq!(replies[1].reply, replies[0].reply, "the repetition");
    assert_eq!(
        replies[2].reply,
        StoreReply::Violation(StoreViolation::RecipeMismatch)
    );
    assert_eq!(replies[4].reply, replies[3].reply);
    assert_eq!(replies[5].reply, replies[0].reply);
    let state = twin.grouped.store.snapshot().expect("healthy");
    assert_eq!(state.operations.len(), 2);
    assert_eq!(state.publications.len(), 1);
}

/// A group with refusals of every kind in its middle: a guard that fails
/// after rows were prepared, an identity of an epoch that is not open, a
/// full account.
fn refusals<S>(world: &World<S>) -> Vec<Command> {
    let stale = altered(
        &world.admission(2, 8, b"payload", &[world.b, world.c]),
        |admit| admit.targets.as_mut_slice()[1].session_revision = Revision::new(9),
    );
    let early = admission_in(
        RunEpoch::new(world.epoch.get() + 1),
        world.route_revision,
        world.producer,
        1,
        30,
        b"payload",
        &[world.b],
    );
    vec![
        world.admission(1, 7, b"payload", &[world.b, world.c]),
        stale.clone(),
        // The inbound slot of packet 7 is taken by the first member.
        world.admission(3, 7, b"payload", &[world.b, world.c]),
        world.admission(4, 9, b"payload", &[world.c]),
        early,
        world.release(5, 7, 1),
        // A refusal presented again is answered from its receipt.
        stale,
        world.admission(6, 10, b"payload", &[world.b]),
        world.admission(7, 11, b"payload", &[world.b]),
        world.admission(8, 12, b"payload", &[world.b]),
        // The fifth open exchange: the account of four is full.
        world.admission(9, 13, b"payload", &[world.b]),
        world.release(10, 9, 4),
        world.admission(11, 14, b"payload", &[world.b]),
    ]
}

#[test]
fn a_refused_member_is_recorded_and_its_neighbours_still_apply() {
    let mut twin = Twin::new("refusals", roomy());
    let commands = refusals(&twin.sequential);
    let replies = twin.compare(&commands);
    let precondition = rejected(RejectReason::PreconditionFailed);
    let full = rejected(RejectReason::CapacityExceeded(Resource::ExchangeSlots));
    assert_eq!(
        committed_now(&replies),
        [
            true, false, false, true, false, true, false, true, true, true, false, true, true
        ]
    );
    for refused in [1, 2, 4] {
        assert_eq!(replies[refused].reply, precondition, "member {refused}");
        assert!(replies[refused].wrote, "member {refused} left a receipt");
    }
    assert_eq!(replies[6].reply, precondition);
    assert!(!replies[6].wrote, "answered from the receipt of member 1");
    assert_eq!(replies[10].reply, full);
    assert!(replies[10].wrote);

    // The receipts are durable with the group: they are there after the
    // store is closed and opened again, beside what the members committed.
    let expected = twin.sequential.store.snapshot().expect("healthy");
    let epoch = twin.sequential.epoch;
    let Twin {
        grouped,
        grouped_path,
        ..
    } = twin;
    drop(grouped);
    let reopened = SqliteStore::open(&grouped_path).expect("reopened");
    let state = reopened.snapshot().expect("healthy");
    assert_eq!(state, expected);
    let decision = |command: &Command| state.operations[&operation(command)].decision;
    assert_eq!(
        decision(&commands[1]),
        Decision::Rejected(RejectReason::PreconditionFailed)
    );
    assert_eq!(
        decision(&commands[2]),
        Decision::Rejected(RejectReason::PreconditionFailed)
    );
    assert_eq!(
        decision(&commands[4]),
        Decision::Rejected(RejectReason::PreconditionFailed)
    );
    assert_eq!(
        decision(&commands[10]),
        Decision::Rejected(RejectReason::CapacityExceeded(Resource::ExchangeSlots))
    );
    assert!(matches!(decision(&commands[0]), Decision::Committed(_)));
    assert!(matches!(decision(&commands[12]), Decision::Committed(_)));
    // Nothing of a refused member but its receipt: no publication of it.
    for refused in [2, 3, 9] {
        assert!(
            !state
                .publications
                .contains_key(&publication_id(epoch, refused))
        );
    }
    assert_eq!(state.operations.len(), 12, "one receipt for each identity");
    assert_eq!(
        reopened.established(operation(&commands[1])),
        Ok(Some(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed
        )))
    );
}

#[test]
fn a_member_without_a_receipt_slot_is_unknown_inside_a_group_too() {
    // Three receipts fit. The fourth member cannot be recorded, neither
    // its effect nor a rejection of it: nothing certain is said about it.
    let limits = Limits {
        receipt_slots: ReceiptSlots::new(3),
        ..fixture_limits()
    };
    let mut twin = Twin::new("no-receipt-slot", limits);
    let world = &twin.sequential;
    let stale = altered(
        &world.admission(6, 12, b"payload", &[world.b, world.c]),
        |admit| admit.targets.as_mut_slice()[1].session_revision = Revision::new(9),
    );
    let commands = [
        world.admission(1, 7, b"payload", &[world.b]),
        world.release(2, 7, 1),
        world.admission(3, 8, b"payload", &[world.b]),
        world.release(4, 8, 3),
        world.admission(5, 9, b"payload", &[world.b]),
        stale,
        world.admission(1, 7, b"payload", &[world.b]),
    ];
    let replies = twin.compare(&commands);
    assert_eq!(
        committed_now(&replies),
        [true, true, true, false, false, false, false]
    );
    for unknown in [3, 4, 5] {
        assert_eq!(replies[unknown], UNKNOWN, "member {unknown}");
    }
    assert!(matches!(
        replies[6].reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    for absent in [3, 4, 5] {
        assert_eq!(
            twin.grouped.store.established(operation(&commands[absent])),
            Ok(None),
            "nothing is established about member {absent}"
        );
    }
}

/// A small deterministic generator: the sequences do not need to be
/// random, only varied and reproducible.
struct Sequence(u64);

impl Sequence {
    fn next(&mut self, below: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % below
    }
}

#[test]
fn varied_groups_equal_their_sequential_application() {
    for seed in 1..=6u64 {
        let limits = Limits {
            receipt_slots: ReceiptSlots::new(40),
            ..roomy()
        };
        let mut twin = Twin::new(&format!("varied-{seed}"), limits);
        let mut sequence = Sequence(seed);
        let mut history: Vec<Command> = Vec::new();
        let mut next = 1u64;
        let mut open: Vec<(u64, u16)> = Vec::new();
        for _ in 0..8 {
            let size = 2 + sequence.next(9) as usize;
            let mut group = Vec::with_capacity(size);
            for _ in 0..size {
                let world = &twin.sequential;
                let packet = u16::try_from(1 + next % 60).expect("fits");
                let command = match sequence.next(8) {
                    // An earlier command again, whatever it was.
                    0 | 1 if !history.is_empty() => {
                        history[sequence.next(history.len() as u64) as usize].clone()
                    }
                    // The release of an exchange that may be open.
                    2 | 3 if !open.is_empty() => {
                        let (admitted, packet) =
                            open.remove(sequence.next(open.len() as u64) as usize);
                        next += 1;
                        world.release(next, packet, admitted)
                    }
                    // A guard that fails after rows were prepared.
                    4 => {
                        next += 1;
                        altered(
                            &world.admission(next, packet, b"stale", &[world.b, world.c]),
                            |admit| {
                                admit.targets.as_mut_slice()[1].session_revision = Revision::new(9);
                            },
                        )
                    }
                    // An admission, towards one destination or two.
                    kind => {
                        next += 1;
                        open.push((next, packet));
                        let targets: &[_] = if kind % 2 == 0 {
                            &[world.b, world.c]
                        } else {
                            &[world.c]
                        };
                        world.admission(next, packet, b"payload", targets)
                    }
                };
                history.push(command.clone());
                group.push(command);
            }
            twin.compare(&group);
        }
        let state = twin.grouped.store.snapshot().expect("healthy");
        assert!(
            state
                .operations
                .values()
                .any(|record| matches!(record.decision, Decision::Rejected(_))),
            "seed {seed} exercised a refusal"
        );
        assert!(
            state.operations.len() > 8,
            "seed {seed} committed something"
        );
    }
}

#[test]
fn an_empty_group_commits_nothing() {
    let mut twin = Twin::new("empty", roomy());
    let before = twin.grouped.store.snapshot().expect("healthy");
    assert_eq!(twin.grouped.store.apply_group(&[]), Ok(Vec::new()));
    assert_eq!(twin.grouped.store.snapshot().expect("healthy"), before);
    twin.same_state();
}

#[test]
fn a_group_of_one_equals_apply() {
    let mut twin = Twin::new("one", roomy());
    let world = &twin.sequential;
    let admission = world.admission(1, 7, b"payload", &[world.b, world.c]);
    let stale = altered(
        &world.admission(2, 8, b"payload", &[world.b, world.c]),
        |admit| admit.targets.as_mut_slice()[1].session_revision = Revision::new(9),
    );
    let release = world.release(3, 7, 1);
    let mut replies = Vec::new();
    for command in [&admission, &admission, &stale, &stale, &release] {
        replies.extend(twin.compare(std::slice::from_ref(command)));
    }
    assert_eq!(committed_now(&replies), [true, false, false, false, true]);
    assert_eq!(wrote(&replies), [true, false, true, false, true]);
    // The medium fails: a committed identity is still recognised, a new
    // one is unknown, exactly as `apply` answers.
    twin.grouped.store.set_durability(Durability::Failing);
    twin.sequential.store.set_durability(Durability::Failing);
    let fresh = twin
        .sequential
        .admission(4, 9, b"payload", &[twin.sequential.b]);
    let replies = [
        twin.compare(std::slice::from_ref(&admission)),
        twin.compare(std::slice::from_ref(&fresh)),
    ]
    .concat();
    assert!(matches!(
        replies[0].reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ));
    assert_eq!(replies[1], UNKNOWN);
}

#[test]
fn a_group_over_the_bound_is_refused_without_any_durable_change() {
    assert_eq!(MAX_GROUP, 64);
    let mut twin = Twin::new("bound", roomy());
    let commands = pairs(&twin.sequential, 1..=33);
    assert_eq!(commands.len(), MAX_GROUP + 2);
    let before = twin.grouped.store.snapshot().expect("healthy");
    for over in [MAX_GROUP + 1, MAX_GROUP + 2] {
        let members: Vec<&Command> = commands[..over].iter().collect();
        assert_eq!(
            twin.grouped.store.apply_group(&members),
            Err(GroupRefused::TooLarge {
                commands: over,
                max: MAX_GROUP
            })
        );
        assert_eq!(twin.grouped.store.snapshot().expect("healthy"), before);
        twin.same_state();
        for command in &commands[..over] {
            assert_eq!(twin.grouped.store.established(operation(command)), Ok(None));
        }
    }
    // The largest group is accepted, and equals its sequential twin.
    let replies = twin.compare(&commands[..MAX_GROUP]);
    assert_eq!(committed_now(&replies), [true; MAX_GROUP]);
}

#[test]
fn a_group_on_a_failing_medium_leaves_nothing_and_says_nothing_certain() {
    let mut twin = Twin::new("failing", roomy());
    let prefix = pairs(&twin.sequential, 1..=1);
    twin.both(&prefix);
    let before = twin.grouped.store.snapshot().expect("healthy");
    let world = &twin.sequential;
    let stale = altered(
        &world.admission(8, 20, b"payload", &[world.b, world.c]),
        |admit| admit.targets.as_mut_slice()[1].session_revision = Revision::new(9),
    );
    // New members, one that a healthy medium would refuse and record, and
    // one already committed before the group.
    let mut commands = pairs(world, 2..=3);
    commands.push(stale);
    commands.push(prefix[0].clone());
    let members: Vec<&Command> = commands.iter().collect();

    twin.grouped.store.set_durability(Durability::Failing);
    twin.sequential.store.set_durability(Durability::Failing);
    let replies = twin
        .grouped
        .store
        .apply_group(&members)
        .expect("within the bound");
    assert_eq!(
        replies, [UNKNOWN; 6],
        "no member of a group that was not committed is answered"
    );
    let sequential: Vec<Applied> = commands
        .iter()
        .map(|command| twin.sequential.store.apply(command))
        .collect();
    // A failed `apply` says the same of every member that had to write.
    assert_eq!(sequential[..5], [UNKNOWN; 5]);
    assert_eq!(twin.grouped.store.snapshot().expect("healthy"), before);
    twin.same_state();
    // A group that has nothing to write reads what is durable, as `apply`
    // does while the medium fails.
    let durable = [prefix[0].clone(), prefix[1].clone()];
    let recognised = twin.compare(&durable);
    assert!(recognised.iter().all(|applied| matches!(
        applied.reply,
        StoreReply::Outcome(StoreOutcome::Committed(_))
    ) && !applied.wrote));

    twin.grouped.store.set_durability(Durability::Available);
    twin.sequential.store.set_durability(Durability::Available);
    // Nothing is established about a member of the failed group, the one
    // that would have been refused included: absence is not rejection.
    for command in &commands[..5] {
        assert_eq!(twin.grouped.store.established(operation(command)), Ok(None));
        assert_eq!(
            twin.sequential.store.established(operation(command)),
            Ok(None)
        );
    }
    // Resolving one of them fences it, as after a failed `apply`.
    let fenced = Resolved {
        outcome: StoreOutcome::RejectedCertain(RejectReason::Fenced),
        fenced_now: true,
    };
    let first = operation(&commands[0]);
    assert_eq!(twin.grouped.store.resolve_and_fence(first), fenced);
    assert_eq!(twin.sequential.store.resolve_and_fence(first), fenced);
    twin.same_state();
    // Presented again on a healthy medium, the group is decided as its
    // sequential twin decides it: the fenced member stays refused.
    let replies = twin.compare(&commands);
    assert_eq!(replies[0].reply, rejected(RejectReason::Fenced));
    assert_eq!(
        committed_now(&replies),
        [false, false, true, true, false, false]
    );
    assert_eq!(replies[4].reply, rejected(RejectReason::PreconditionFailed));
}

/// A process killed at the two kill points of the store while it commits a
/// group (plan `docs/missions/L7-kill-inside-a-transaction.md`, record
/// 0045): before the commit no member is durable, once the commit is
/// logged every member is.
#[cfg(feature = "kill-points")]
mod killed {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Child, Command as Process, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use mqttiumd_lab::store::durable::MemoryStore;
    use mqttiumd_store_sqlite::kill_points::KillPoint;

    use super::*;

    /// Set in the environment of the child: the path of its store.
    const CHILD: &str = "MQTTIUMD_GROUP_KILL_CHILD";
    /// Set in the environment of the child: the point it stops at.
    const POINT: &str = "MQTTIUMD_GROUP_KILL_POINT";
    const AT: &str = "group-kill-child: at ";
    const NO_POINT: &str = "group-kill-child: the group did not reach the point";
    /// Exchanges committed one transaction each before the group.
    const BEFORE: u64 = 3;

    fn point_name(point: KillPoint) -> &'static str {
        match point {
            KillPoint::CommitPrepared => "store.commit.prepared",
            KillPoint::CommitLogged => "store.commit.logged",
        }
    }

    /// The group the child commits: exchanges, with a refused member whose
    /// receipt is part of the group, in its middle.
    fn group<S>(world: &World<S>) -> Vec<Command> {
        let stale = altered(
            &world.admission(100, 50, b"payload", &[world.b, world.c]),
            |admit| admit.targets.as_mut_slice()[1].session_revision = Revision::new(9),
        );
        let mut commands = pairs(world, BEFORE + 1..=BEFORE + 2);
        commands.push(stale);
        commands.extend(pairs(world, BEFORE + 3..=BEFORE + 3));
        commands
    }

    fn prefix<S: DurableStore>(world: &mut World<S>) {
        for command in pairs(world, 1..=BEFORE) {
            assert!(world.store.apply(&command).committed_now);
        }
    }

    /// The deterministic store after the same prefix: the reference.
    fn reference() -> World<MemoryStore> {
        let mut world = World::with_limits(&MemoryStore::create, roomy());
        prefix(&mut world);
        world
    }

    fn counting(store: &mut SqliteStore) -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        let prepared = Arc::new(AtomicU64::new(0));
        let logged = Arc::new(AtomicU64::new(0));
        let (p, l) = (Arc::clone(&prepared), Arc::clone(&logged));
        store
            .set_kill_hook(Some(Box::new(move |point| {
                match point {
                    KillPoint::CommitPrepared => &p,
                    KillPoint::CommitLogged => &l,
                }
                .fetch_add(1, Ordering::SeqCst);
            })))
            .expect("armed");
        (prepared, logged)
    }

    #[test]
    fn a_group_passes_each_kill_point_once() {
        let path = fresh_path("kill-count");
        let mut world = World::with_limits(
            &|incarnation, limits| {
                SqliteStore::create(&path, incarnation, limits).expect("created")
            },
            roomy(),
        );
        prefix(&mut world);
        let (prepared, logged) = counting(&mut world.store);
        let passed = || {
            (
                prepared.load(Ordering::SeqCst),
                logged.load(Ordering::SeqCst),
            )
        };
        let commands = group(&world);
        let members: Vec<&Command> = commands.iter().collect();
        let replies = world.store.apply_group(&members).expect("within the bound");
        assert_eq!(wrote(&replies), [true; 7], "six commits and one receipt");
        assert_eq!(passed(), (1, 1), "one commit for the seven members");
        // Everything is recognised: nothing to commit, no point passed.
        let replies = world.store.apply_group(&members).expect("within the bound");
        assert_eq!(wrote(&replies), [false; 7]);
        assert_eq!(passed(), (1, 1));
        assert_eq!(world.store.apply_group(&[]), Ok(Vec::new()));
        assert_eq!(passed(), (1, 1));
        // `apply` keeps one commit for each command.
        for command in pairs(&world, 10..=11) {
            assert!(world.store.apply(&command).committed_now);
        }
        assert_eq!(passed(), (5, 5));
    }

    /// Not a test of its own: the body of the child process, run only when
    /// the parent sets `CHILD`. It commits the prefix, then one group with
    /// a hook that stops at the first passage of its point.
    #[test]
    fn group_kill_child() {
        let Some(path) = std::env::var_os(CHILD) else {
            return;
        };
        let point = std::env::var(POINT).expect("a point");
        let mut world = World::with_limits(
            &|incarnation, limits| {
                SqliteStore::create(Path::new(&path), incarnation, limits).expect("created")
            },
            roomy(),
        );
        assert!(world.store.effective_settings().kill_points);
        prefix(&mut world);
        let commands = group(&world);
        let members: Vec<&Command> = commands.iter().collect();
        world
            .store
            .set_kill_hook(Some(Box::new(move |passed| {
                if point_name(passed) == point {
                    let mut out = std::io::stdout().lock();
                    writeln!(out, "{AT}{point}").expect("said");
                    out.flush().expect("flushed");
                    loop {
                        std::thread::sleep(std::time::Duration::from_secs(60));
                    }
                }
            })))
            .expect("armed");
        let _ = world.store.apply_group(&members);
        println!("{NO_POINT}");
    }

    struct Holder {
        child: Child,
    }

    impl Holder {
        /// The test binary itself, run again on `group_kill_child` only;
        /// it returns once the child stands at `point`.
        fn spawn(path: &Path, point: &str) -> Self {
            let mut child = Process::new(std::env::current_exe().expect("the test binary"))
                .args([
                    "--exact",
                    "killed::group_kill_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, path)
                .env(POINT, point)
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("the child process");
            let stdout = child.stdout.take().expect("its output");
            // Own the child before any fallible wait: a timeout or a closed
            // pipe must still kill and reap it.
            let holder = Self { child };
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let reader = std::thread::spawn(move || {
                let mut lines = BufReader::new(stdout)
                    .take(4096)
                    .lines()
                    .map_while(Result::ok);
                let said = lines.find(|line| line.contains(AT) || line.ends_with(NO_POINT));
                let _ = sender.send(said);
            });
            let said = receiver
                .recv_timeout(std::time::Duration::from_secs(30))
                .expect("the kill point reported within 30 seconds");
            reader.join().expect("the bounded reader ended");
            assert!(
                said.as_deref()
                    .is_some_and(|line| line.ends_with(&format!("{AT}{point}"))),
                "the child did not stop at {point}: {said:?}"
            );
            holder
        }

        /// `SIGKILL`, then the status the kernel reports.
        fn kill(mut self) {
            self.child.kill().expect("killed");
            let status = self.child.wait().expect("reaped");
            assert_eq!(status.signal(), Some(9), "killed by SIGKILL: {status:?}");
        }
    }

    impl Drop for Holder {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    /// Kills the child at `point` of its group and reopens its file: what
    /// is durable is the prefix, and the whole group when `durable`. No
    /// state in between exists.
    fn killed_at(point: KillPoint, durable: bool) {
        let path = fresh_path("killed");
        let mut reference = reference();
        let commands = group(&reference);
        let before = reference.store.snapshot().expect("healthy");
        let mut whole = self::reference();
        let sequential: Vec<Applied> = commands
            .iter()
            .map(|command| whole.store.apply(command))
            .collect();
        assert_eq!(wrote(&sequential), [true; 7]);
        let after = whole.store.snapshot().expect("healthy");
        assert_ne!(before, after);

        let holder = Holder::spawn(&path, point_name(point));
        holder.kill();
        // No intervention between the kill and the restart.
        let mut store = SqliteStore::open(&path).expect("opened after the kill");
        let recovered = store.try_snapshot().expect("decodes");
        assert_eq!(accounts_match_what_is_held(&recovered), Ok(()));
        if durable {
            assert_eq!(recovered, after, "every member of the group is there");
        } else {
            assert_eq!(recovered, before, "no member of the group is there");
            for command in &commands {
                assert_eq!(store.established(operation(command)), Ok(None));
            }
        }
        // Presented again: decided anew, or answered from the durable
        // records without a second effect, as the reference answers.
        if durable {
            reference = whole;
        }
        let expected: Vec<Applied> = commands
            .iter()
            .map(|command| reference.store.apply(command))
            .collect();
        assert_eq!(wrote(&expected), [!durable; 7]);
        let members: Vec<&Command> = commands.iter().collect();
        assert_eq!(store.apply_group(&members), Ok(expected));
        assert_eq!(store.try_snapshot().expect("decodes"), after);
    }

    #[test]
    fn a_kill_before_the_commit_of_a_group_leaves_no_member() {
        killed_at(KillPoint::CommitPrepared, false);
    }

    #[test]
    fn a_kill_once_the_commit_of_a_group_is_logged_keeps_every_member() {
        killed_at(KillPoint::CommitLogged, true);
    }
}
