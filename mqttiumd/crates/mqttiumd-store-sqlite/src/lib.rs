//! The SQLite store of mqttiumd (lot L7).
//!
//! One file, one connection that is the only writer, WAL and
//! `synchronous=FULL`, the values read back after they are set and the
//! opening refused when they differ (DEM-011). The engine is the SQLite
//! amalgamation compiled into the binding; its version and compile
//! options are read from the engine itself and kept with the settings.
//!
//! Every command is one `BEGIN IMMEDIATE` transaction on the real tables:
//! each guard is a read in it, each row a write in it, and a guard that
//! fails rolls everything back, so no subset of an admission is ever
//! admitted. The decisions are those of the contract and of the
//! deterministic store: `EpochLedger::open` decides an opening and
//! `OpeningOutcome::read` answers it from the ledger that is durable
//! after the write; a rejection is recorded on a receipt that holds a
//! receipt slot, and without one nothing certain is announced.
//!
//! A commit that fails is reported as not written. SQLite rolls back a
//! transaction whose commit fails; the answer is then unknown, never a
//! certain rejection, and the next command reads the durable state again.
//!
//! # The writer's lock
//!
//! `BEGIN IMMEDIATE` with no busy timeout serialises transactions; it does
//! not make one process the owner of the store for its whole life. That is
//! the writer's lock: an exclusive, non-blocking advisory lock
//! (`File::try_lock`, `flock` on Linux) on a file beside the database,
//! `<database>.writer-lock`. `SqliteStore::create` and `SqliteStore::open`
//! take it before they touch the database and hold it for the whole life
//! of the `SqliteStore`; it is released when the store is dropped, after
//! its connection is closed, and by the kernel when the process dies, a
//! `SIGKILL` included. A second writer while the first lives is refused at
//! once with `OpenError::WriterAlive`, from this process or another.
//! `SqliteStore::inspect` does not take it, and the database itself is not
//! locked beyond what SQLite does: `locking_mode` stays `NORMAL`, so an
//! inspector reads while the writer lives.
//!
//! The lock file is never removed by the store: a removal would let two
//! writers lock two different files of the same name. Its existence alone
//! means nothing; only the lock held on it does. What the lock does not
//! guarantee: an advisory lock does not stop a program that ignores it
//! (another SQLite client opening the database directly), and file
//! systems over a network, where `flock` may not be shared between hosts,
//! are outside its perimeter.

#![forbid(unsafe_code)]

mod codec;
mod schema;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};

use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References, Resource,
};
use mqttiumd_contract::command::{
    AdmitPublication, Command, EpochLedger, EpochPhase, LedgerOpening, OpenEpoch, Recipe,
    ReleaseInbound,
};
use mqttiumd_contract::effects::{InboundPhase, RecoveredFact};
use mqttiumd_contract::ids::{
    DeliveryId, InboundPacketId, OperationId, PayloadId, PublicationId, Revision, RunEpoch,
    SessionId, StoreIncarnation,
};
use mqttiumd_contract::outcome::{
    CommitFacts, Knowledge, OpeningOutcome, RejectReason, StoreOutcome, StoreReply, StoreViolation,
};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_store_api::{
    Accounts, AdminCommand, AdminDone, AdminRefused, Applied, Decision, Durability, DurableState,
    DurableStore, InboundRecord, Limits, MAX_CLIENT_ID_BYTES, ObligationRecord, OperationRecord,
    PayloadRecord, PublicationRecord, RecoveryCursor, RecoveryPage, RecoveryRefused, Resolved,
    RouteTarget, SessionRecord, SessionState,
};

use codec::{
    Corrupt, attempt_blob, blob_attempt, blob_exchange, blob_u64, blob_usize, cause_code,
    code_cause, code_inbound_phase, code_obligation_phase, code_qos, code_session_state,
    codes_reason, exchange_blob, inbound_phase_code, obligation_phase_code, qos_code, reason_codes,
    session_state_code, u64_blob, usize_blob,
};

pub use schema::{FORMAT, SCHEMA_VERSION};

/// Whether this compiled store carries test-only transaction kill points.
/// The bench checks this before writing a campaign, including when Cargo
/// unified a transitive request for the store feature into its normal build.
pub const KILL_POINTS_ENABLED: bool = cfg!(feature = "kill-points");

/// The points of the store at which the bench may kill its process, in
/// a build with the feature `kill-points` only (record 0002, plan
/// `docs/missions/L7-kill-inside-a-transaction.md`). The product is never
/// built with it: `effective_settings().kill_points` says which build
/// runs, and `tools/check_kill_points.sh` refuses a product build that
/// carries it.
#[cfg(feature = "kill-points")]
pub mod kill_points {
    /// A point inside the write path of a command.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum KillPoint {
        /// `store.commit.prepared`: every guard read and every row written,
        /// the meta row one revision later, `COMMIT` not called yet. The
        /// transaction is open and holds the engine's write lock.
        CommitPrepared,
        /// `store.commit.logged`: the engine's WAL hook after the commit
        /// and after release of SQLite's write lock. With WAL and FULL,
        /// the commit is durable; `COMMIT` has not returned to the adapter.
        CommitLogged,
    }

    /// Called at each point the store passes. It may block, for the bench
    /// to kill the process there; it must not call the store.
    pub type Hook = Box<dyn FnMut(KillPoint) + Send>;

    /// The hook, shared with the engine's WAL hook.
    pub(crate) type Shared = std::sync::Arc<std::sync::Mutex<Option<Hook>>>;

    /// Calls the hook, if one is set, at `point`.
    pub(crate) fn pass(shared: &Shared, point: KillPoint) {
        if let Ok(mut hook) = shared.lock()
            && let Some(hook) = hook.as_mut()
        {
            hook(point);
        }
    }
}

/// What the engine says about itself and about the settings in force,
/// read back after they were set (DEM-011).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Settings {
    /// `sqlite_version()`.
    pub sqlite_version: String,
    /// `PRAGMA compile_options`, in the engine's order.
    pub compile_options: Vec<String>,
    /// `PRAGMA journal_mode`, read back: `wal`.
    pub journal_mode: String,
    /// `PRAGMA synchronous`, read back: 2, which is FULL.
    pub synchronous: i64,
    /// `PRAGMA user_version`: the schema version of the file.
    pub user_version: i64,
    /// `PRAGMA page_size`, in bytes.
    pub page_size: i64,
    /// `PRAGMA wal_autocheckpoint`, in pages: the engine's checkpoint of
    /// the WAL into the database. Commits are not grouped otherwise.
    pub wal_autocheckpoint: i64,
    /// Who, if anyone, checkpoints the WAL on this connection. Normal
    /// builds use `sqlite-autocheckpoint`; an armed kill hook uses
    /// `disabled-while-kill-hook-armed`, with no checkpoint in its callback.
    pub checkpoint_policy: &'static str,
    /// Whether this build carries the kill points of the bench (feature
    /// `kill-points`). A build of the product must say false.
    pub kill_points: bool,
}

/// Why a file was not opened as a store.
#[derive(Debug)]
pub enum OpenError {
    /// `create` was given a file that already holds a database.
    NotEmpty,
    /// The schema version of the file is not this store's: an older or a
    /// future format, refused before anything is written.
    SchemaVersion(i64),
    /// The file names another format, or a durable value does not decode.
    Corrupt,
    /// `PRAGMA quick_check` found the file damaged; its first lines.
    Integrity(String),
    /// A setting read back is not the one that was set.
    Setting {
        name: &'static str,
        read_back: String,
    },
    Sql(rusqlite::Error),
    /// Another `SqliteStore` holds the writer's lock of this file, in this
    /// process or another: refused at once, without waiting.
    WriterAlive,
    /// The lock file could not be opened, or the lock could not be asked
    /// for (not that it is held: that is `WriterAlive`).
    WriterLock(std::io::Error),
}

impl From<rusqlite::Error> for OpenError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

impl From<Corrupt> for OpenError {
    fn from(_: Corrupt) -> Self {
        Self::Corrupt
    }
}

/// A transaction could not go on: an SQL error or a value that does not
/// decode. Nothing it prepared is written.
#[derive(Debug)]
enum Fault {
    Sql(rusqlite::Error),
    Corrupt,
}

impl From<rusqlite::Error> for Fault {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

impl From<Corrupt> for Fault {
    fn from(_: Corrupt) -> Self {
        Self::Corrupt
    }
}

/// Why a transaction stopped before its commit: a guard refused it, or it
/// could not go on.
enum Abort<E> {
    Refused(E),
    Fault,
}

impl<E> From<rusqlite::Error> for Abort<E> {
    fn from(_: rusqlite::Error) -> Self {
        Self::Fault
    }
}

impl<E> From<Corrupt> for Abort<E> {
    fn from(_: Corrupt) -> Self {
        Self::Fault
    }
}

impl<E> From<Fault> for Abort<E> {
    fn from(_: Fault) -> Self {
        Self::Fault
    }
}

/// What a transaction did.
enum Txn<T, E> {
    Committed(T),
    /// A guard refused it; it was rolled back.
    Refused(E),
    /// It was not written: rolled back, or its commit failed.
    NotWritten,
}

/// The single row of `meta`, decoded.
#[derive(Clone, Copy, Debug)]
struct Meta {
    ledger: EpochLedger,
    collected_through: RunEpoch,
    revision: u64,
    limits: Limits,
    accounts: Accounts,
    next_session: SessionId,
    route_revision: Revision,
}

type Guard<T> = Result<T, Abort<RejectReason>>;

/// Builds one recovered fact from a row; an inner error is a fact that
/// cannot be built within its budget, in its place.
type BuildFact<'a> =
    dyn Fn(&rusqlite::Row<'_>) -> Result<Result<RecoveredFact, RecoveryRefused>, Fault> + 'a;

fn refused<T>(reason: RejectReason) -> Guard<T> {
    Err(Abort::Refused(reason))
}

fn blob(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Vec<u8>> {
    row.get(index)
}

fn read_meta(conn: &Connection) -> Result<Meta, Fault> {
    let mut statement = conn.prepare_cached(
        "SELECT format, incarnation, last_opened, opened_by, collected_through, revision,
                next_session, route_revision,
                limit_payload_bytes, limit_references, limit_exchange_slots,
                limit_receipt_slots, max_targets_per_admission, max_topic_bytes,
                max_payload_bytes, max_sessions, max_route_targets,
                used_payload_bytes, used_references, used_exchange_slots, used_receipt_slots
           FROM meta WHERE one = 1",
    )?;
    let row = statement.query_row([], |row| {
        let mut blobs = Vec::with_capacity(20);
        for index in 1..21 {
            if index == 3 {
                blobs.push(row.get::<_, Option<Vec<u8>>>(index)?.unwrap_or_default());
            } else {
                blobs.push(blob(row, index)?);
            }
        }
        Ok((row.get::<_, String>(0)?, blobs))
    })?;
    let (format, b) = row;
    if format != FORMAT {
        return Err(Fault::Corrupt);
    }
    let opened_by = if b[2].is_empty() {
        None
    } else {
        Some(blob_attempt(&b[2])?)
    };
    let ledger = EpochLedger::from_durable(
        StoreIncarnation::new(blob_u64(&b[0])?),
        RunEpoch::new(blob_u64(&b[1])?),
        opened_by,
    )
    .map_err(|_| Fault::Corrupt)?;
    Ok(Meta {
        ledger,
        collected_through: RunEpoch::new(blob_u64(&b[3])?),
        revision: blob_u64(&b[4])?,
        next_session: SessionId::new(blob_u64(&b[5])?),
        route_revision: Revision::new(blob_u64(&b[6])?),
        limits: Limits {
            payload_bytes: PayloadBytes::new(blob_u64(&b[7])?),
            references: References::new(blob_u64(&b[8])?),
            exchange_slots: ExchangeSlots::new(blob_u64(&b[9])?),
            receipt_slots: ReceiptSlots::new(blob_u64(&b[10])?),
            max_targets_per_admission: blob_usize(&b[11])?,
            max_topic_bytes: blob_usize(&b[12])?,
            max_payload_bytes: blob_usize(&b[13])?,
            max_sessions: blob_usize(&b[14])?,
            max_route_targets: blob_usize(&b[15])?,
        },
        accounts: Accounts {
            payload_bytes: PayloadBytes::new(blob_u64(&b[16])?),
            references: References::new(blob_u64(&b[17])?),
            exchange_slots: ExchangeSlots::new(blob_u64(&b[18])?),
            receipt_slots: ReceiptSlots::new(blob_u64(&b[19])?),
        },
    })
}

fn write_meta(conn: &Connection, meta: &Meta) -> Result<(), Fault> {
    let limits = meta.limits;
    let accounts = meta.accounts;
    let opened_by = meta.ledger.opened_by().map(attempt_blob);
    let changed = conn
        .prepare_cached(
            "UPDATE meta SET incarnation = ?1, last_opened = ?2, opened_by = ?3,
                collected_through = ?4, revision = ?5, next_session = ?6, route_revision = ?7,
                limit_payload_bytes = ?8, limit_references = ?9, limit_exchange_slots = ?10,
                limit_receipt_slots = ?11, max_targets_per_admission = ?12,
                max_topic_bytes = ?13, max_payload_bytes = ?14, max_sessions = ?15,
                max_route_targets = ?16, used_payload_bytes = ?17, used_references = ?18,
                used_exchange_slots = ?19, used_receipt_slots = ?20
          WHERE one = 1",
        )?
        .execute(params![
            u64_blob(meta.ledger.incarnation().get()),
            u64_blob(meta.ledger.last_opened().get()),
            opened_by.as_ref().map(<[u8; 24]>::as_slice),
            u64_blob(meta.collected_through.get()),
            u64_blob(meta.revision),
            u64_blob(meta.next_session.get()),
            u64_blob(meta.route_revision.get()),
            u64_blob(limits.payload_bytes.get()),
            u64_blob(limits.references.get()),
            u64_blob(limits.exchange_slots.get()),
            u64_blob(limits.receipt_slots.get()),
            usize_blob(limits.max_targets_per_admission)?,
            usize_blob(limits.max_topic_bytes)?,
            usize_blob(limits.max_payload_bytes)?,
            usize_blob(limits.max_sessions)?,
            usize_blob(limits.max_route_targets)?,
            u64_blob(accounts.payload_bytes.get()),
            u64_blob(accounts.references.get()),
            u64_blob(accounts.exchange_slots.get()),
            u64_blob(accounts.receipt_slots.get()),
        ])?;
    if changed == 1 {
        Ok(())
    } else {
        Err(Fault::Corrupt)
    }
}

/// Charges one account against its limit, or names the resource that is
/// short. The same order as the deterministic store.
fn charge(meta: &mut Meta, costs: Accounts) -> Guard<()> {
    let limits = meta.limits;
    let used = meta.accounts;
    let short = |resource| Abort::Refused(RejectReason::CapacityExceeded(resource));
    let payload_bytes = used
        .payload_bytes
        .checked_add(costs.payload_bytes)
        .filter(|total| *total <= limits.payload_bytes)
        .ok_or_else(|| short(Resource::PayloadBytes))?;
    let references = used
        .references
        .checked_add(costs.references)
        .filter(|total| *total <= limits.references)
        .ok_or_else(|| short(Resource::References))?;
    let exchange_slots = used
        .exchange_slots
        .checked_add(costs.exchange_slots)
        .filter(|total| *total <= limits.exchange_slots)
        .ok_or_else(|| short(Resource::ExchangeSlots))?;
    let receipt_slots = used
        .receipt_slots
        .checked_add(costs.receipt_slots)
        .filter(|total| *total <= limits.receipt_slots)
        .ok_or_else(|| short(Resource::ReceiptSlots))?;
    meta.accounts = Accounts {
        payload_bytes,
        references,
        exchange_slots,
        receipt_slots,
    };
    Ok(())
}

/// What is already established about an operation, before any guard.
enum Established {
    Outcome(StoreOutcome),
    Absent(EpochPhase),
}

fn decision_of(row: &rusqlite::Row<'_>, first: usize) -> rusqlite::Result<DecisionColumns> {
    Ok(DecisionColumns {
        decision: row.get(first)?,
        publication: row.get(first + 1)?,
        exchange: row.get(first + 2)?,
        reason: row.get(first + 3)?,
        resource: row.get(first + 4)?,
    })
}

struct DecisionColumns {
    decision: i64,
    publication: Option<Vec<u8>>,
    exchange: Option<Vec<u8>>,
    reason: Option<i64>,
    resource: Option<i64>,
}

const DECISION_ADMITTED: i64 = 1;
const DECISION_RELEASED: i64 = 2;
const DECISION_REJECTED: i64 = 3;

impl DecisionColumns {
    fn decode(&self) -> Result<Decision, Corrupt> {
        let exchange = || blob_exchange(self.exchange.as_deref().ok_or(Corrupt)?);
        match self.decision {
            DECISION_ADMITTED => Ok(Decision::Committed(CommitFacts::PublicationAdmitted {
                publication: PublicationId::from_be_slice(
                    self.publication.as_deref().ok_or(Corrupt)?,
                )
                .map_err(|_| Corrupt)?,
                exchange: exchange()?,
            })),
            DECISION_RELEASED => Ok(Decision::Committed(CommitFacts::InboundReleased {
                exchange: exchange()?,
            })),
            DECISION_REJECTED => Ok(Decision::Rejected(codes_reason(
                self.reason.ok_or(Corrupt)?,
                self.resource,
            )?)),
            _ => Err(Corrupt),
        }
    }
}

fn insert_operation(
    conn: &Connection,
    operation: OperationId,
    recipe: &[u8],
    decision: Decision,
) -> Result<(), Fault> {
    let (code, publication, exchange, reason, resource) = match decision {
        Decision::Committed(CommitFacts::PublicationAdmitted {
            publication,
            exchange,
        }) => (
            DECISION_ADMITTED,
            Some(publication.to_be_bytes().to_vec()),
            Some(exchange_blob(exchange).to_vec()),
            None,
            None,
        ),
        Decision::Committed(CommitFacts::InboundReleased { exchange }) => (
            DECISION_RELEASED,
            None,
            Some(exchange_blob(exchange).to_vec()),
            None,
            None,
        ),
        Decision::Rejected(rejected) => {
            let (reason, resource) = reason_codes(rejected);
            (DECISION_REJECTED, None, None, Some(reason), resource)
        }
    };
    conn.prepare_cached(
        "INSERT INTO operations (id, decision, publication, exchange, reason, resource, recipe)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?
    .execute(params![
        operation.to_be_bytes().as_slice(),
        code,
        publication,
        exchange,
        reason,
        resource,
        recipe
    ])?;
    Ok(())
}

fn read_operation(
    conn: &Connection,
    operation: OperationId,
) -> Result<Option<OperationRecord>, Fault> {
    let row = conn
        .prepare_cached(
            "SELECT recipe, decision, publication, exchange, reason, resource
               FROM operations WHERE id = ?1",
        )?
        .query_row([operation.to_be_bytes().as_slice()], |row| {
            Ok((blob(row, 0)?, decision_of(row, 1)?))
        })
        .optional()?;
    match row {
        None => Ok(None),
        Some((recipe, columns)) => Ok(Some(OperationRecord {
            recipe,
            decision: columns.decode()?,
        })),
    }
}

fn fenced(conn: &Connection, operation: OperationId) -> Result<bool, Fault> {
    Ok(conn
        .prepare_cached("SELECT 1 FROM fences WHERE id = ?1")?
        .query_row([operation.to_be_bytes().as_slice()], |_| Ok(()))
        .optional()?
        .is_some())
}

fn established(
    conn: &Connection,
    meta: &Meta,
    operation: OperationId,
) -> Result<Established, Fault> {
    if operation.incarnation() != meta.ledger.incarnation() {
        return Ok(Established::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::PreconditionFailed,
        )));
    }
    if operation.epoch() <= meta.collected_through && operation.epoch().get() != 0 {
        return Ok(Established::Outcome(
            StoreOutcome::RetiredOutcomeUnavailable,
        ));
    }
    if let Some(record) = read_operation(conn, operation)? {
        return Ok(Established::Outcome(match record.decision {
            Decision::Committed(facts) => StoreOutcome::Committed(facts),
            Decision::Rejected(reason) => StoreOutcome::RejectedCertain(reason),
        }));
    }
    if fenced(conn, operation)? {
        return Ok(Established::Outcome(StoreOutcome::RejectedCertain(
            RejectReason::Fenced,
        )));
    }
    Ok(match meta.ledger.phase_of(operation.epoch()) {
        EpochPhase::Closed => {
            Established::Outcome(StoreOutcome::RejectedCertain(RejectReason::EpochClosed))
        }
        phase => Established::Absent(phase),
    })
}

/// The revision and state of a session, when it exists.
fn session_row(
    conn: &Connection,
    session: SessionId,
) -> Result<Option<(Revision, SessionState)>, Fault> {
    let row = conn
        .prepare_cached("SELECT revision, state FROM sessions WHERE id = ?1")?
        .query_row([session.to_be_bytes().as_slice()], |row| {
            Ok((blob(row, 0)?, row.get::<_, i64>(1)?))
        })
        .optional()?;
    match row {
        None => Ok(None),
        Some((revision, state)) => Ok(Some((
            Revision::new(blob_u64(&revision)?),
            code_session_state(state)?,
        ))),
    }
}

fn active_session(conn: &Connection, session: SessionId, revision: Revision) -> Guard<()> {
    match session_row(conn, session)? {
        Some((current, SessionState::Active)) if current == revision => Ok(()),
        _ => refused(RejectReason::PreconditionFailed),
    }
}

fn exists(conn: &Connection, sql: &str, key: &[u8]) -> Result<bool, Fault> {
    Ok(conn
        .prepare_cached(sql)?
        .query_row([key], |_| Ok(()))
        .optional()?
        .is_some())
}

fn stage_admission(
    conn: &Connection,
    meta: &mut Meta,
    admit: &AdmitPublication,
    payload: Option<&SharedBytes>,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    let operation = admit.operation;
    let same_scope = |incarnation, epoch, owner| {
        incarnation == operation.incarnation()
            && epoch == operation.epoch()
            && owner == operation.owner()
    };
    if !same_scope(
        admit.publication.incarnation(),
        admit.publication.epoch(),
        admit.publication.owner(),
    ) || !same_scope(
        admit.payload.id.incarnation(),
        admit.payload.id.epoch(),
        admit.payload.id.owner(),
    ) || !same_scope(
        admit.request.incarnation(),
        admit.request.epoch(),
        admit.request.owner(),
    ) {
        return refused(failed);
    }
    let targets = admit.targets.as_slice();
    if targets.len() > meta.limits.max_targets_per_admission
        || admit.topic.len() > meta.limits.max_topic_bytes
        || admit.route_revision != meta.route_revision
    {
        return refused(failed);
    }
    let Some(bytes) = payload else {
        return refused(failed);
    };
    let length = u64::try_from(bytes.len()).map_err(|_| Abort::Refused(failed))?;
    if length != admit.payload.length.get() || bytes.len() > meta.limits.max_payload_bytes {
        return refused(failed);
    }
    // The costs are part of the frozen recipe and must be the costs of
    // what it admits: the store releases exactly what it charged.
    let exactly_once = admit.qos == Qos::ExactlyOnce;
    let reference_count = u64::try_from(targets.len()).map_err(|_| Abort::Refused(failed))?;
    if admit.costs.payload_bytes != admit.payload.length
        || admit.costs.references.get() != reference_count
        || admit.costs.exchange_slots.get() != u64::from(exactly_once)
        || admit.costs.receipt_slots.get() != 1
    {
        return refused(failed);
    }
    active_session(conn, admit.source.session, admit.source.session_revision)?;

    // A payload identity names one payload, kept or not.
    let publication = admit.publication.to_be_bytes();
    let payload_id = admit.payload.id.to_be_bytes();
    if exists(
        conn,
        "SELECT 1 FROM publications WHERE id = ?1",
        &publication,
    )? || exists(conn, "SELECT 1 FROM payloads WHERE id = ?1", &payload_id)?
        || exists(
            conn,
            "SELECT 1 FROM publications WHERE payload = ?1",
            &payload_id,
        )?
    {
        return refused(failed);
    }
    conn.prepare_cached(
        "INSERT INTO publications (id, operation, source, qos, retain, payload, topic)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?
    .execute(params![
        publication.as_slice(),
        operation.to_be_bytes().as_slice(),
        admit.source.session.to_be_bytes().as_slice(),
        qos_code(admit.qos),
        i64::from(admit.retain),
        payload_id.as_slice(),
        admit.topic.as_slice(),
    ])?;
    // Its bytes are kept while an obligation refers to them, and only then
    // charged. Without a target they are charged and released in this same
    // transaction.
    if reference_count > 0 {
        conn.prepare_cached(
            "INSERT INTO payloads (id, refs, length, bytes) VALUES (?1, ?2, ?3, ?4)",
        )?
        .execute(params![
            payload_id.as_slice(),
            u64_blob(reference_count).as_slice(),
            u64_blob(length).as_slice(),
            bytes.as_slice(),
        ])?;
    }
    for target in targets {
        active_session(conn, target.session, target.session_revision)?;
        let delivery = target.delivery;
        let key = (
            delivery.publication.to_be_bytes(),
            i64::from(delivery.index),
        );
        let taken = conn
            .prepare_cached("SELECT 1 FROM obligations WHERE publication = ?1 AND idx = ?2")?
            .query_row(params![key.0.as_slice(), key.1], |_| Ok(()))
            .optional()?
            .is_some();
        if taken {
            return refused(failed);
        }
        conn.prepare_cached(
            "INSERT INTO obligations (publication, idx, target, target_revision, qos, cause, phase)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?
        .execute(params![
            key.0.as_slice(),
            key.1,
            target.session.to_be_bytes().as_slice(),
            target.session_revision.to_be_bytes().as_slice(),
            qos_code(target.effective_qos),
            cause_code(target.cause),
            obligation_phase_code(mqttiumd_store_api::ObligationPhase::Pending),
        ])?;
    }
    if exactly_once {
        // One admission per inbound exchange: an occupied slot is never
        // taken over, whatever generation the newcomer claims.
        let session = admit.source.session.to_be_bytes();
        let packet = i64::from(admit.source.packet_id.get());
        let taken = conn
            .prepare_cached("SELECT 1 FROM inbound WHERE session = ?1 AND packet_id = ?2")?
            .query_row(params![session.as_slice(), packet], |_| Ok(()))
            .optional()?
            .is_some();
        if taken {
            return refused(failed);
        }
        conn.prepare_cached(
            "INSERT INTO inbound (session, packet_id, exchange, publication, phase)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?
        .execute(params![
            session.as_slice(),
            packet,
            exchange_blob(admit.source.exchange).as_slice(),
            publication.as_slice(),
            inbound_phase_code(InboundPhase::AwaitPubrel),
        ])?;
    }
    charge(
        meta,
        Accounts {
            payload_bytes: admit.costs.payload_bytes,
            references: admit.costs.references,
            exchange_slots: admit.costs.exchange_slots,
            receipt_slots: admit.costs.receipt_slots,
        },
    )?;
    if reference_count == 0 {
        meta.accounts.payload_bytes = meta
            .accounts
            .payload_bytes
            .checked_sub(admit.costs.payload_bytes)
            .ok_or(Abort::Refused(failed))?;
    }
    Ok(CommitFacts::PublicationAdmitted {
        publication: admit.publication,
        exchange: admit.source.exchange,
    })
}

fn stage_release(
    conn: &Connection,
    meta: &mut Meta,
    release: &ReleaseInbound,
) -> Guard<CommitFacts> {
    let failed = RejectReason::PreconditionFailed;
    let session = release.source_session.to_be_bytes();
    let packet = i64::from(release.packet_id.get());
    let marker = conn
        .prepare_cached(
            "SELECT exchange, phase FROM inbound WHERE session = ?1 AND packet_id = ?2",
        )?
        .query_row(params![session.as_slice(), packet], |row| {
            Ok((blob(row, 0)?, row.get::<_, i64>(1)?))
        })
        .optional()?;
    match marker {
        Some((exchange, phase))
            if blob_exchange(&exchange)? == release.exchange
                && code_inbound_phase(phase)? == InboundPhase::AwaitPubrel => {}
        _ => return refused(failed),
    }
    conn.prepare_cached("DELETE FROM inbound WHERE session = ?1 AND packet_id = ?2")?
        .execute(params![session.as_slice(), packet])?;
    meta.accounts.exchange_slots = meta
        .accounts
        .exchange_slots
        .checked_sub(ExchangeSlots::new(1))
        .ok_or(Abort::Refused(failed))?;
    // The receipt of the release holds a slot like every receipt.
    charge(meta, Accounts::RECEIPT)?;
    Ok(CommitFacts::InboundReleased {
        exchange: release.exchange,
    })
}

/// Ends one obligation and releases exactly what it held.
fn end_obligation(
    conn: &Connection,
    meta: &mut Meta,
    publication: &[u8],
    index: i64,
) -> Result<(), Abort<AdminRefused>> {
    let broken = Abort::Refused(AdminRefused::Inconsistent);
    conn.prepare_cached("DELETE FROM obligations WHERE publication = ?1 AND idx = ?2")?
        .execute(params![publication, index])?;
    meta.accounts.references = meta
        .accounts
        .references
        .checked_sub(References::new(1))
        .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
    let Some(payload) = conn
        .prepare_cached("SELECT payload FROM publications WHERE id = ?1")?
        .query_row([publication], |row| blob(row, 0))
        .optional()?
    else {
        return Err(broken);
    };
    let Some((refs, length)) = conn
        .prepare_cached("SELECT refs, length FROM payloads WHERE id = ?1")?
        .query_row([payload.as_slice()], |row| {
            Ok((blob(row, 0)?, blob(row, 1)?))
        })
        .optional()?
    else {
        return Err(broken);
    };
    let refs = blob_u64(&refs)?
        .checked_sub(1)
        .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
    if refs == 0 {
        conn.prepare_cached("DELETE FROM payloads WHERE id = ?1")?
            .execute([payload.as_slice()])?;
        meta.accounts.payload_bytes = meta
            .accounts
            .payload_bytes
            .checked_sub(PayloadBytes::new(blob_u64(&length)?))
            .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
    } else {
        conn.prepare_cached("UPDATE payloads SET refs = ?2 WHERE id = ?1")?
            .execute(params![payload.as_slice(), u64_blob(refs).as_slice()])?;
    }
    Ok(())
}

fn admin_refused<T>(refusal: AdminRefused) -> Result<T, Abort<AdminRefused>> {
    Err(Abort::Refused(refusal))
}

fn stage_admin(
    conn: &Connection,
    meta: &mut Meta,
    command: AdminCommand,
) -> Result<AdminDone, Abort<AdminRefused>> {
    let active = session_state_code(SessionState::Active);
    match command {
        AdminCommand::CreateSession { client_id } => {
            if client_id.len() > MAX_CLIENT_ID_BYTES {
                return admin_refused(AdminRefused::ClientIdTooLong);
            }
            let duplicate = conn
                .prepare_cached("SELECT 1 FROM sessions WHERE state = ?1 AND client_id = ?2")?
                .query_row(params![active, client_id.as_slice()], |_| Ok(()))
                .optional()?
                .is_some();
            if duplicate {
                return admin_refused(AdminRefused::DuplicateClientId);
            }
            let count: i64 = conn
                .prepare_cached("SELECT count(*) FROM sessions WHERE state = ?1")?
                .query_row([active], |row| row.get(0))?;
            let count = usize::try_from(count).map_err(|_| Abort::Fault)?;
            if count >= meta.limits.max_sessions {
                return admin_refused(AdminRefused::TableFull);
            }
            let session = meta.next_session;
            meta.next_session = session
                .checked_next()
                .map_err(|_| Abort::Refused(AdminRefused::Exhausted))?;
            let revision = Revision::new(1);
            conn.prepare_cached(
                "INSERT INTO sessions (id, revision, state, client_id) VALUES (?1, ?2, ?3, ?4)",
            )?
            .execute(params![
                session.to_be_bytes().as_slice(),
                revision.to_be_bytes().as_slice(),
                active,
                client_id.as_slice()
            ])?;
            Ok(AdminDone::SessionCreated { session, revision })
        }
        AdminCommand::AddRoute {
            topic,
            session,
            qos,
        } => {
            if topic.len() > meta.limits.max_topic_bytes {
                return admin_refused(AdminRefused::TopicTooLong);
            }
            match session_row(conn, session)? {
                Some((_, SessionState::Active)) => {}
                _ => return admin_refused(AdminRefused::UnknownSession),
            }
            let mut targets = Vec::new();
            {
                let mut statement = conn.prepare_cached(
                    "SELECT session, position FROM routes WHERE topic = ?1 ORDER BY position",
                )?;
                let mut rows = statement.query([topic.as_slice()])?;
                while let Some(row) = rows.next()? {
                    if targets.len() > meta.limits.max_route_targets {
                        return Err(Abort::Fault);
                    }
                    targets.push((blob(row, 0)?, blob_u64(&blob(row, 1)?)?));
                }
            }
            let target = session.to_be_bytes();
            if targets
                .iter()
                .any(|(existing, _)| existing.as_slice() == target)
            {
                return admin_refused(AdminRefused::DuplicateTarget);
            }
            if targets.len() >= meta.limits.max_route_targets {
                return admin_refused(AdminRefused::TableFull);
            }
            let position = match targets.last() {
                Some((_, last)) => last
                    .checked_add(1)
                    .ok_or(Abort::Refused(AdminRefused::Exhausted))?,
                None => 0,
            };
            conn.prepare_cached(
                "INSERT INTO routes (topic, position, session, qos) VALUES (?1, ?2, ?3, ?4)",
            )?
            .execute(params![
                topic.as_slice(),
                u64_blob(position).as_slice(),
                target.as_slice(),
                qos_code(qos)
            ])?;
            meta.route_revision = meta
                .route_revision
                .checked_next()
                .map_err(|_| Abort::Refused(AdminRefused::Exhausted))?;
            Ok(AdminDone::RouteAdded {
                route_revision: meta.route_revision,
            })
        }
        AdminCommand::DeleteSession { session } => {
            let revision = match session_row(conn, session)? {
                Some((revision, SessionState::Active)) => revision,
                _ => return admin_refused(AdminRefused::UnknownSession),
            };
            let revision = revision
                .checked_next()
                .map_err(|_| Abort::Refused(AdminRefused::Exhausted))?;
            let key = session.to_be_bytes();
            conn.prepare_cached("UPDATE sessions SET revision = ?2, state = ?3 WHERE id = ?1")?
                .execute(params![
                    key.as_slice(),
                    revision.to_be_bytes().as_slice(),
                    session_state_code(SessionState::Terminal)
                ])?;
            // The session's own inbound markers, with their slots.
            let markers = conn
                .prepare_cached("DELETE FROM inbound WHERE session = ?1")?
                .execute([key.as_slice()])?;
            for _ in 0..markers {
                meta.accounts.exchange_slots = meta
                    .accounts
                    .exchange_slots
                    .checked_sub(ExchangeSlots::new(1))
                    .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
            }
            // Obligations end by their destination, never by the session
            // that produced the publication.
            let mut ended = Vec::new();
            {
                let mut statement = conn.prepare_cached(
                    "SELECT publication, idx FROM obligations WHERE target = ?1
                      ORDER BY publication, idx",
                )?;
                let mut rows = statement.query([key.as_slice()])?;
                while let Some(row) = rows.next()? {
                    ended.push((blob(row, 0)?, row.get::<_, i64>(1)?));
                }
            }
            for (publication, index) in ended {
                end_obligation(conn, meta, &publication, index)?;
            }
            let removed = conn
                .prepare_cached("DELETE FROM routes WHERE session = ?1")?
                .execute([key.as_slice()])?;
            if removed > 0 {
                meta.route_revision = meta
                    .route_revision
                    .checked_next()
                    .map_err(|_| Abort::Refused(AdminRefused::Exhausted))?;
            }
            Ok(AdminDone::SessionDeleted)
        }
        AdminCommand::CollectClosedEpochs { through } => {
            if meta.ledger.phase_of(through) != EpochPhase::Closed {
                return admin_refused(AdminRefused::EpochNotClosed);
            }
            if through > meta.collected_through {
                meta.collected_through = through;
            }
            let collected = meta.collected_through;
            // Epoch 0 is never opened, so never closed: its receipts and
            // fences are never retired.
            let retired = |operation: OperationId| {
                operation.epoch() <= collected && operation.epoch().get() != 0
            };
            let mut freed = 0u64;
            for table in ["operations", "fences"] {
                let mut doomed = Vec::new();
                {
                    let mut statement = conn.prepare_cached(&format!("SELECT id FROM {table}"))?;
                    let mut rows = statement.query([])?;
                    while let Some(row) = rows.next()? {
                        let id = blob(row, 0)?;
                        let operation =
                            OperationId::from_be_slice(&id).map_err(|_| Abort::Fault)?;
                        if retired(operation) {
                            doomed.push(id);
                        }
                    }
                }
                let mut delete =
                    conn.prepare_cached(&format!("DELETE FROM {table} WHERE id = ?1"))?;
                for id in doomed {
                    delete.execute([id.as_slice()])?;
                    freed = freed
                        .checked_add(1)
                        .ok_or(Abort::Refused(AdminRefused::Exhausted))?;
                }
            }
            // Every receipt and every fence holds one receipt slot.
            meta.accounts.receipt_slots = meta
                .accounts
                .receipt_slots
                .checked_sub(ReceiptSlots::new(freed))
                .ok_or(Abort::Refused(AdminRefused::Inconsistent))?;
            Ok(AdminDone::Collected)
        }
    }
}

/// Opens a connection to `path` and checks, before anything is written,
/// that its schema version is `expected` (0 for a file to create).
/// The path of the writer's lock of the database at `path`.
#[must_use]
pub fn writer_lock_path(path: &Path) -> PathBuf {
    let mut file = path.as_os_str().to_os_string();
    file.push(".writer-lock");
    PathBuf::from(file)
}

/// Takes the writer's lock of the database at `path`, without waiting.
/// The lock file is created if it is missing and never truncated or
/// removed.
fn writer_lock(path: &Path) -> Result<File, OpenError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(writer_lock_path(path))
        .map_err(OpenError::WriterLock)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(OpenError::WriterAlive),
        Err(TryLockError::Error(error)) => Err(OpenError::WriterLock(error)),
    }
}

fn connect(path: &Path) -> Result<Connection, OpenError> {
    let conn = Connection::open(path)?;
    // One writer: a busy database is an error, never a wait.
    conn.busy_timeout(std::time::Duration::ZERO)?;
    Ok(conn)
}

/// Sets WAL and `synchronous=FULL`, then reads back what is in force, the
/// engine version and its compile options.
fn settle(conn: &Connection) -> Result<Settings, OpenError> {
    let journal_mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(OpenError::Setting {
            name: "journal_mode",
            read_back: journal_mode,
        });
    }
    conn.execute_batch("PRAGMA synchronous = FULL")?;
    let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    let synchronous: i64 = conn.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(OpenError::Setting {
            name: "journal_mode",
            read_back: journal_mode,
        });
    }
    if synchronous != 2 {
        return Err(OpenError::Setting {
            name: "synchronous",
            read_back: synchronous.to_string(),
        });
    }
    let sqlite_version: String = conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
    let mut compile_options = Vec::new();
    {
        let mut statement = conn.prepare("PRAGMA compile_options")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            compile_options.push(row.get::<_, String>(0)?);
        }
    }
    let user_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let wal_autocheckpoint: i64 =
        conn.query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?;
    Ok(Settings {
        sqlite_version,
        compile_options,
        journal_mode,
        synchronous,
        user_version,
        page_size,
        wal_autocheckpoint,
        checkpoint_policy: "sqlite-autocheckpoint",
        kill_points: KILL_POINTS_ENABLED,
    })
}

/// `PRAGMA quick_check`: its first lines, `["ok"]` for a sound file. A
/// damage the check itself trips over is a report too, not an SQL error.
fn quick_check(conn: &Connection) -> Result<Vec<String>, OpenError> {
    let run = || -> rusqlite::Result<Vec<String>> {
        let mut report = Vec::new();
        let mut statement = conn.prepare("PRAGMA quick_check")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            if report.len() < 8 {
                report.push(row.get::<_, String>(0)?);
            }
        }
        Ok(report)
    };
    match run() {
        Ok(report) => Ok(report),
        Err(error @ rusqlite::Error::SqliteFailure(failure, _))
            if matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
            ) =>
        {
            Ok(vec![error.to_string()])
        }
        Err(error) => Err(OpenError::Sql(error)),
    }
}

/// The SQLite store. The connection is the only writer of its file.
pub struct SqliteStore {
    conn: Connection,
    settings: Settings,
    durability: Durability,
    /// The ledger as of the last transaction; refreshed after each one.
    ledger: EpochLedger,
    /// The writer's lock, held while the store lives. Declared last: the
    /// fields drop in order, so the connection is closed, and its last
    /// checkpoint done, before another writer can take the lock.
    #[cfg(feature = "kill-points")]
    kill_hook: kill_points::Shared,
    #[cfg(feature = "kill-points")]
    autocheckpoint_before_hook: Option<i64>,
    _writer_lock: File,
}

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStore")
            .field("settings", &self.settings)
            .field("durability", &self.durability)
            .field("ledger", &self.ledger)
            .finish_non_exhaustive()
    }
}

impl SqliteStore {
    /// Creates an empty data set in a file that holds no database yet. No
    /// epoch is open in it.
    pub fn create(
        path: &Path,
        incarnation: StoreIncarnation,
        limits: Limits,
    ) -> Result<Self, OpenError> {
        let lock = writer_lock(path)?;
        let mut conn = connect(path)?;
        let tables: i64 =
            conn.query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if tables != 0 || version != 0 {
            return Err(OpenError::NotEmpty);
        }
        settle(&conn)?;
        let meta = Meta {
            ledger: EpochLedger::fresh(incarnation),
            collected_through: RunEpoch::new(0),
            revision: 0,
            limits,
            accounts: Accounts::default(),
            next_session: SessionId::new(1),
            route_revision: Revision::new(0),
        };
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(schema::CREATE)?;
        tx.execute(
            "INSERT INTO meta (one, format, incarnation, last_opened, collected_through, revision,
                 next_session, route_revision, limit_payload_bytes, limit_references,
                 limit_exchange_slots, limit_receipt_slots, max_targets_per_admission,
                 max_topic_bytes, max_payload_bytes, max_sessions, max_route_targets,
                 used_payload_bytes, used_references, used_exchange_slots, used_receipt_slots)
             VALUES (1, ?1, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2, ?2)",
            params![FORMAT, u64_blob(0).as_slice()],
        )?;
        write_meta(&tx, &meta).map_err(|_| OpenError::Corrupt)?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
        Self::verified(conn, lock)
    }

    /// Opens an existing data set: its schema version is checked before
    /// anything is written, then the settings are set and read back, and
    /// its durable values are decoded.
    pub fn open(path: &Path) -> Result<Self, OpenError> {
        if !path.exists() {
            return Err(OpenError::Corrupt);
        }
        let lock = writer_lock(path)?;
        let conn = connect(path)?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != SCHEMA_VERSION {
            return Err(OpenError::SchemaVersion(version));
        }
        Self::verified(conn, lock)
    }

    /// Reads the durable state of the file at `path` without the writer,
    /// for an inspector outside the process that writes it.
    ///
    /// The file is opened read-only (`SQLITE_OPEN_READ_ONLY`, the flag of
    /// `mode=ro`; never `immutable`, which would read without locks while
    /// the WAL changes). No pragma that writes is set and no checkpoint is
    /// run. The whole state is read in one read transaction, which ends,
    /// and the connection is closed, before this returns: the inspector
    /// holds nothing that would keep the writer's checkpoint back. It does
    /// not take the writer's lock, and reads while a writer lives.
    pub fn inspect(path: &Path) -> Result<DurableState, OpenError> {
        if !path.exists() {
            return Err(OpenError::Corrupt);
        }
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        // A reader may wait briefly for the writer's index of the WAL.
        conn.busy_timeout(std::time::Duration::from_secs(1))?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != SCHEMA_VERSION {
            return Err(OpenError::SchemaVersion(version));
        }
        let read = conn.unchecked_transaction()?;
        let state = read_snapshot(&read).map_err(|_| OpenError::Corrupt)?;
        read.commit()?;
        drop(conn);
        Ok(state)
    }

    fn verified(conn: Connection, lock: File) -> Result<Self, OpenError> {
        let settings = settle(&conn)?;
        // Before an epoch is opened: every page of the file is checked,
        // not only those opening reads.
        let report = quick_check(&conn)?;
        if report != ["ok"] {
            return Err(OpenError::Integrity(report.join("\n")));
        }
        if settings.user_version != SCHEMA_VERSION {
            return Err(OpenError::SchemaVersion(settings.user_version));
        }
        let meta = read_meta(&conn).map_err(|_| OpenError::Corrupt)?;
        Ok(Self {
            conn,
            settings,
            durability: Durability::Available,
            ledger: meta.ledger,
            #[cfg(feature = "kill-points")]
            kill_hook: kill_points::Shared::default(),
            #[cfg(feature = "kill-points")]
            autocheckpoint_before_hook: None,
            _writer_lock: lock,
        })
    }

    /// The durable state, or why it could not be read: a value that does
    /// not decode is `OpenError::Corrupt`, as for `inspect`.
    pub fn try_snapshot(&self) -> Result<DurableState, OpenError> {
        self.read_snapshot().map_err(|fault| match fault {
            Fault::Corrupt => OpenError::Corrupt,
            Fault::Sql(error) => OpenError::Sql(error),
        })
    }

    /// Sets the function the store calls at each kill point, or removes it.
    ///
    /// Installation order is explicit: disable SQLite's autocheckpoint,
    /// read back zero, then install the WAL hook. The hook reports the
    /// committed transaction and performs no checkpoint. Removing it first
    /// removes the WAL callback, then restores the saved autocheckpoint
    /// threshold. A PRAGMA alone cannot prove the callback is installed;
    /// the kill-point tests observe actual callback invocations.
    #[cfg(feature = "kill-points")]
    pub fn set_kill_hook(&mut self, hook: Option<kill_points::Hook>) -> Result<(), OpenError> {
        use kill_points::{KillPoint, pass};
        let armed = hook.is_some();
        if armed {
            let threshold = self
                .autocheckpoint_before_hook
                .unwrap_or(self.settings.wal_autocheckpoint);
            self.conn.pragma_update(None, "wal_autocheckpoint", 0)?;
            let read_back: i64 = self
                .conn
                .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?;
            if read_back != 0 {
                return Err(OpenError::Setting {
                    name: "wal_autocheckpoint",
                    read_back: read_back.to_string(),
                });
            }
            *self
                .kill_hook
                .lock()
                .expect("kill callback is not poisoned") = hook;
            let shared = std::sync::Arc::clone(&self.kill_hook);
            self.conn.wal_hook(Some(
                move |_wal: &mut rusqlite::hooks::Wal, _frames: std::ffi::c_int| {
                    pass(&shared, KillPoint::CommitLogged);
                    Ok(())
                },
            ))?;
            self.autocheckpoint_before_hook = Some(threshold);
            self.settings.wal_autocheckpoint = read_back;
            self.settings.checkpoint_policy = "disabled-while-kill-hook-armed";
        } else {
            self.conn.wal_hook(
                None::<fn(&mut rusqlite::hooks::Wal, std::ffi::c_int) -> rusqlite::Result<()>>,
            )?;
            *self
                .kill_hook
                .lock()
                .expect("kill callback is not poisoned") = None;
            let threshold = self
                .autocheckpoint_before_hook
                .unwrap_or(self.settings.wal_autocheckpoint);
            self.conn
                .pragma_update(None, "wal_autocheckpoint", threshold)?;
            self.settings.wal_autocheckpoint =
                self.conn
                    .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?;
            self.autocheckpoint_before_hook = None;
            self.settings.checkpoint_policy = "sqlite-autocheckpoint";
        }
        Ok(())
    }

    /// The engine and the settings in force, read back at opening.
    #[must_use]
    pub fn effective_settings(&self) -> &Settings {
        &self.settings
    }

    /// For a measurement only: sets `PRAGMA wal_autocheckpoint` on the
    /// writer's connection and reads it back into the settings. A store
    /// keeps the engine's default otherwise; nothing in the product calls
    /// this. Refused while a kill hook is armed: setting the PRAGMA would
    /// replace that hook in SQLite, even if its value stayed zero.
    pub fn set_wal_autocheckpoint_for_measurement(&mut self, pages: i64) -> Result<i64, OpenError> {
        #[cfg(feature = "kill-points")]
        if self.autocheckpoint_before_hook.is_some() {
            return Err(OpenError::Setting {
                name: "wal_autocheckpoint_while_kill_hook_armed",
                read_back: self.settings.wal_autocheckpoint.to_string(),
            });
        }
        self.conn.pragma_update(None, "wal_autocheckpoint", pages)?;
        let read_back: i64 = self
            .conn
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?;
        self.settings.wal_autocheckpoint = read_back;
        Ok(read_back)
    }

    /// Sets whether the medium accepts writes. While it fails, every
    /// transaction is rolled back instead of committed.
    pub fn set_durability(&mut self, durability: Durability) {
        self.durability = durability;
    }

    /// Runs one write transaction. `prepare` reads and writes rows and the
    /// decoded `meta`; on success `meta` is written one revision later and
    /// the transaction committed, unless the medium fails.
    fn transact<T, E>(
        &mut self,
        prepare: impl FnOnce(&Connection, &mut Meta) -> Result<T, Abort<E>>,
    ) -> Txn<T, E> {
        let failing = self.durability == Durability::Failing;
        let result = (|| {
            let tx: Transaction<'_> = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|_| None)?;
            let mut meta = read_meta(&tx).map_err(|_| None)?;
            let value = match prepare(&tx, &mut meta) {
                Ok(value) => value,
                // Dropping the transaction rolls it back.
                Err(Abort::Refused(refusal)) => return Ok(Err(refusal)),
                Err(Abort::Fault) => return Err(None),
            };
            meta.revision = meta.revision.checked_add(1).ok_or(None)?;
            write_meta(&tx, &meta).map_err(|_| None)?;
            // K1 `store.commit.prepared`: every row written, `COMMIT` not
            // called; the transaction is open.
            #[cfg(feature = "kill-points")]
            kill_points::pass(&self.kill_hook, kill_points::KillPoint::CommitPrepared);
            if failing {
                return Err(None);
            }
            tx.commit().map_err(|_| None)?;
            Ok(Ok(value))
        })();
        // What is durable now, whatever happened.
        if let Ok(meta) = read_meta(&self.conn) {
            self.ledger = meta.ledger;
        }
        match result {
            Ok(Ok(value)) => Txn::Committed(value),
            Ok(Err(refusal)) => Txn::Refused(refusal),
            Err(None::<()>) | Err(Some(())) => Txn::NotWritten,
        }
    }

    /// Records a certain rejection on a receipt that holds a receipt slot.
    /// Without a slot nothing is written and nothing certain announced.
    fn commit_rejection(
        &mut self,
        operation: OperationId,
        recipe: &[u8],
        reason: RejectReason,
    ) -> (StoreOutcome, bool) {
        let written = self.transact(|conn, meta| {
            charge(meta, Accounts::RECEIPT).map_err(|_| Abort::Refused(()))?;
            insert_operation(conn, operation, recipe, Decision::Rejected(reason))?;
            Ok::<(), Abort<()>>(())
        });
        match written {
            Txn::Committed(()) => (StoreOutcome::RejectedCertain(reason), true),
            Txn::Refused(()) | Txn::NotWritten => (StoreOutcome::PendingOrUnknown, false),
        }
    }

    fn read_ledger(&self) -> EpochLedger {
        read_meta(&self.conn).map_or(self.ledger, |meta| meta.ledger)
    }

    /// The recovery inventory from `start`, at most `count` facts, in the
    /// order of record 0014, each section read with its own bounded query.
    fn inventory(
        &self,
        meta: &Meta,
        start: usize,
        count: usize,
    ) -> Result<Vec<Result<RecoveredFact, RecoveryRefused>>, Fault> {
        let mut out = Vec::new();
        let mut position = 0usize;
        let wanted = |out: &mut Vec<_>, position: &mut usize| {
            let take = *position >= start && out.len() < count;
            *position = position.saturating_add(1);
            take
        };
        let refused = |_| RecoveryRefused::AllocationFailed;
        let incarnation = meta.ledger.incarnation();
        let open = meta.ledger.open_epoch();
        if let Some(epoch) = open
            && wanted(&mut out, &mut position)
        {
            out.push(Ok(RecoveredFact::Epoch { incarnation, epoch }));
        }
        if wanted(&mut out, &mut position) {
            let bound = |value: usize| u32::try_from(value).map_err(refused);
            out.push((|| {
                Ok(RecoveredFact::Limits {
                    max_targets_per_admission: bound(meta.limits.max_targets_per_admission)?,
                    max_topic_bytes: bound(meta.limits.max_topic_bytes)?,
                    max_payload_bytes: PayloadBytes::new(
                        u64::try_from(meta.limits.max_payload_bytes).map_err(refused)?,
                    ),
                })
            })());
        }
        if wanted(&mut out, &mut position) {
            out.push(Ok(RecoveredFact::RouteTable {
                revision: meta.route_revision,
            }));
        }
        let max_topic_bytes = meta.limits.max_topic_bytes;
        let active = session_state_code(SessionState::Active);
        // Each section: its rows from `skip`, at most what is still wanted.
        let section = |sql: &str,
                       out: &mut Vec<_>,
                       position: &mut usize,
                       build: &BuildFact<'_>|
         -> Result<(), Fault> {
            let skip = start.saturating_sub(*position);
            let room = count.saturating_sub(out.len());
            let total: i64 = self
                .conn
                .prepare_cached(&format!("SELECT count(*) FROM ({sql})"))?
                .query_row(
                    params![
                        active,
                        open.map(|epoch| epoch.to_be_bytes().to_vec())
                            .unwrap_or_default()
                    ],
                    |row| row.get(0),
                )?;
            let total = usize::try_from(total).map_err(|_| Fault::Corrupt)?;
            if room > 0 && skip < total {
                let mut statement = self
                    .conn
                    .prepare_cached(&format!("{sql} LIMIT ?3 OFFSET ?4"))?;
                let limit = i64::try_from(room).map_err(|_| Fault::Corrupt)?;
                let offset = i64::try_from(skip).map_err(|_| Fault::Corrupt)?;
                let mut rows = statement.query(params![
                    active,
                    open.map(|epoch| epoch.to_be_bytes().to_vec())
                        .unwrap_or_default(),
                    limit,
                    offset
                ])?;
                while let Some(row) = rows.next()? {
                    out.push(build(row)?);
                }
            }
            *position = position.saturating_add(total);
            Ok(())
        };
        // ?1 is the code of an active session and ?2 the open epoch, in
        // every query, used or not.
        section(
            "SELECT r.topic, r.session, r.qos, s.revision, s.state FROM routes r
               LEFT JOIN sessions s ON s.id = r.session
              WHERE ?1 = ?1 AND ?2 = ?2 ORDER BY r.topic, r.position",
            &mut out,
            &mut position,
            &|row| {
                let topic = blob(row, 0)?;
                let target =
                    SessionId::from_be_slice(&blob(row, 1)?).map_err(|_| Fault::Corrupt)?;
                let max_qos = code_qos(row.get(2)?)?;
                let revision: Option<Vec<u8>> = row.get(3)?;
                let state: Option<i64> = row.get(4)?;
                let Some(revision) = revision.filter(|_| state == Some(active)) else {
                    return Ok(Err(RecoveryRefused::AllocationFailed));
                };
                let Ok(topic) = SharedBytes::copy_from(&topic, max_topic_bytes) else {
                    return Ok(Err(RecoveryRefused::AllocationFailed));
                };
                Ok(Ok(RecoveredFact::Route {
                    topic,
                    target,
                    target_revision: Revision::new(blob_u64(&revision)?),
                    max_qos,
                }))
            },
        )?;
        section(
            "SELECT id, client_id, revision FROM sessions WHERE state = ?1 AND ?2 = ?2 ORDER BY id",
            &mut out,
            &mut position,
            &|row| {
                let session =
                    SessionId::from_be_slice(&blob(row, 0)?).map_err(|_| Fault::Corrupt)?;
                let Ok(client_id) = SharedBytes::copy_from(&blob(row, 1)?, MAX_CLIENT_ID_BYTES)
                else {
                    return Ok(Err(RecoveryRefused::AllocationFailed));
                };
                Ok(Ok(RecoveredFact::Session {
                    session,
                    client_id,
                    revision: Revision::new(blob_u64(&blob(row, 2)?)?),
                }))
            },
        )?;
        section(
            "SELECT packet_id, exchange, publication, phase FROM inbound
              WHERE ?1 = ?1 AND ?2 = ?2 ORDER BY session, packet_id",
            &mut out,
            &mut position,
            &|row| {
                let packet: i64 = row.get(0)?;
                let packet_id = u16::try_from(packet)
                    .ok()
                    .and_then(InboundPacketId::new)
                    .ok_or(Fault::Corrupt)?;
                Ok(Ok(RecoveredFact::InboundExchange {
                    exchange: blob_exchange(&blob(row, 1)?)?,
                    packet_id,
                    publication: PublicationId::from_be_slice(&blob(row, 2)?)
                        .map_err(|_| Fault::Corrupt)?,
                    phase: code_inbound_phase(row.get(3)?)?,
                }))
            },
        )?;
        // What is established about the operations of older epochs: the
        // epoch is bytes 9 to 16 of the identity, compared as bytes.
        section(
            "SELECT id, decision, publication, exchange, reason, resource FROM operations
              WHERE ?1 = ?1 AND substr(id, 9, 8) != ?2 ORDER BY id",
            &mut out,
            &mut position,
            &|row| {
                let operation =
                    OperationId::from_be_slice(&blob(row, 0)?).map_err(|_| Fault::Corrupt)?;
                let knowledge = match decision_of(row, 1)?.decode()? {
                    Decision::Committed(facts) => Knowledge::Committed(facts),
                    Decision::Rejected(reason) => Knowledge::RejectedCertain(reason),
                };
                Ok(Ok(RecoveredFact::Operation {
                    operation,
                    knowledge,
                }))
            },
        )?;
        section(
            "SELECT id FROM fences WHERE ?1 = ?1 AND substr(id, 9, 8) != ?2 ORDER BY id",
            &mut out,
            &mut position,
            &|row| {
                let operation =
                    OperationId::from_be_slice(&blob(row, 0)?).map_err(|_| Fault::Corrupt)?;
                Ok(Ok(RecoveredFact::Operation {
                    operation,
                    knowledge: Knowledge::RejectedCertain(RejectReason::Fenced),
                }))
            },
        )?;
        Ok(out)
    }

    fn read_snapshot(&self) -> Result<DurableState, Fault> {
        // One read transaction for the whole state, even on the writer's
        // own connection: a state read in several snapshots is a trap.
        let read = self.conn.unchecked_transaction()?;
        let state = read_snapshot(&read)?;
        read.commit()?;
        Ok(state)
    }
}

/// Every table of the file, decoded, as one `DurableState`. The caller
/// holds the read transaction.
fn read_snapshot(conn: &Connection) -> Result<DurableState, Fault> {
    {
        let meta = read_meta(conn)?;
        let mut sessions = BTreeMap::new();
        {
            let mut statement =
                conn.prepare("SELECT id, client_id, revision, state FROM sessions")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                sessions.insert(
                    SessionId::from_be_slice(&blob(row, 0)?).map_err(|_| Fault::Corrupt)?,
                    SessionRecord {
                        client_id: blob(row, 1)?,
                        revision: Revision::new(blob_u64(&blob(row, 2)?)?),
                        state: code_session_state(row.get(3)?)?,
                    },
                );
            }
        }
        let mut routes: BTreeMap<Vec<u8>, Vec<RouteTarget>> = BTreeMap::new();
        {
            let mut statement =
                conn.prepare("SELECT topic, session, qos FROM routes ORDER BY topic, position")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                routes.entry(blob(row, 0)?).or_default().push(RouteTarget {
                    session: SessionId::from_be_slice(&blob(row, 1)?)
                        .map_err(|_| Fault::Corrupt)?,
                    qos: code_qos(row.get(2)?)?,
                });
            }
        }
        let mut operations = BTreeMap::new();
        {
            let mut statement = conn.prepare(
                "SELECT id, recipe, decision, publication, exchange, reason, resource FROM operations",
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                operations.insert(
                    OperationId::from_be_slice(&blob(row, 0)?).map_err(|_| Fault::Corrupt)?,
                    OperationRecord {
                        recipe: blob(row, 1)?,
                        decision: decision_of(row, 2)?.decode()?,
                    },
                );
            }
        }
        let mut fences = BTreeSet::new();
        {
            let mut statement = conn.prepare("SELECT id FROM fences")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                fences.insert(
                    OperationId::from_be_slice(&blob(row, 0)?).map_err(|_| Fault::Corrupt)?,
                );
            }
        }
        let mut publications = BTreeMap::new();
        {
            let mut statement = conn.prepare(
                "SELECT id, operation, source, topic, qos, retain, payload FROM publications",
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                publications.insert(
                    PublicationId::from_be_slice(&blob(row, 0)?).map_err(|_| Fault::Corrupt)?,
                    PublicationRecord {
                        operation: OperationId::from_be_slice(&blob(row, 1)?)
                            .map_err(|_| Fault::Corrupt)?,
                        source: SessionId::from_be_slice(&blob(row, 2)?)
                            .map_err(|_| Fault::Corrupt)?,
                        topic: blob(row, 3)?,
                        qos: code_qos(row.get(4)?)?,
                        retain: row.get::<_, i64>(5)? == 1,
                        payload: PayloadId::from_be_slice(&blob(row, 6)?)
                            .map_err(|_| Fault::Corrupt)?,
                    },
                );
            }
        }
        let mut payloads = BTreeMap::new();
        {
            let mut statement = conn.prepare("SELECT id, bytes, refs FROM payloads")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                payloads.insert(
                    PayloadId::from_be_slice(&blob(row, 0)?).map_err(|_| Fault::Corrupt)?,
                    PayloadRecord {
                        bytes: blob(row, 1)?,
                        references: blob_u64(&blob(row, 2)?)?,
                    },
                );
            }
        }
        let mut obligations = BTreeMap::new();
        {
            let mut statement = conn.prepare(
                "SELECT publication, idx, target, target_revision, qos, cause, phase FROM obligations",
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let index: i64 = row.get(1)?;
                obligations.insert(
                    DeliveryId {
                        publication: PublicationId::from_be_slice(&blob(row, 0)?)
                            .map_err(|_| Fault::Corrupt)?,
                        index: u32::try_from(index).map_err(|_| Fault::Corrupt)?,
                    },
                    ObligationRecord {
                        target: SessionId::from_be_slice(&blob(row, 2)?)
                            .map_err(|_| Fault::Corrupt)?,
                        target_revision: Revision::new(blob_u64(&blob(row, 3)?)?),
                        qos: code_qos(row.get(4)?)?,
                        cause: code_cause(row.get(5)?)?,
                        phase: code_obligation_phase(row.get(6)?)?,
                    },
                );
            }
        }
        let mut inbound = BTreeMap::new();
        {
            let mut statement = conn
                .prepare("SELECT session, packet_id, exchange, publication, phase FROM inbound")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let packet: i64 = row.get(1)?;
                inbound.insert(
                    (
                        SessionId::from_be_slice(&blob(row, 0)?).map_err(|_| Fault::Corrupt)?,
                        u16::try_from(packet)
                            .ok()
                            .and_then(InboundPacketId::new)
                            .ok_or(Fault::Corrupt)?,
                    ),
                    InboundRecord {
                        exchange: blob_exchange(&blob(row, 2)?)?,
                        publication: PublicationId::from_be_slice(&blob(row, 3)?)
                            .map_err(|_| Fault::Corrupt)?,
                        phase: code_inbound_phase(row.get(4)?)?,
                    },
                );
            }
        }
        Ok(DurableState {
            ledger: meta.ledger,
            collected_through: meta.collected_through,
            revision: meta.revision,
            limits: meta.limits,
            accounts: meta.accounts,
            next_session: meta.next_session,
            route_revision: meta.route_revision,
            sessions,
            routes,
            operations,
            fences,
            publications,
            payloads,
            obligations,
            inbound,
        })
    }
}

impl DurableStore for SqliteStore {
    fn ledger(&self) -> EpochLedger {
        self.read_ledger()
    }

    fn open_epoch(&mut self, command: &OpenEpoch) -> OpeningOutcome {
        // The decision is the contract's; only an opening writes.
        if let LedgerOpening::Opens { ledger } = self.read_ledger().open(command) {
            let _written = self.transact(|_, meta| {
                meta.ledger = ledger;
                Ok::<(), Abort<()>>(())
            });
        }
        // The answer comes from what is durable now.
        OpeningOutcome::read(&self.read_ledger(), command)
    }

    fn apply(&mut self, command: &Command) -> Applied {
        let settled = |outcome| Applied {
            reply: StoreReply::Outcome(outcome),
            committed_now: false,
            wrote: false,
        };
        let rejected = |(outcome, wrote)| Applied {
            reply: StoreReply::Outcome(outcome),
            committed_now: false,
            wrote,
        };
        let unknown = settled(StoreOutcome::PendingOrUnknown);
        let recipe = command.recipe.get();
        let operation = recipe.operation();
        let mut canonical = Vec::new();
        if recipe.encode_canonical(&mut canonical).is_err() {
            return settled(StoreOutcome::RejectedCertain(
                RejectReason::PreconditionFailed,
            ));
        }
        let Ok(meta) = read_meta(&self.conn) else {
            return unknown;
        };

        // 1. Recognise an operation that was already decided.
        let Ok(record) = read_operation(&self.conn, operation) else {
            return unknown;
        };
        if let Some(record) = record {
            let same_payload = match (recipe, &command.payload, record.decision) {
                (Recipe::AdmitPublication(admit), Some(bytes), Decision::Committed(_)) => {
                    let stored: Result<Option<Vec<u8>>, _> = self
                        .conn
                        .prepare_cached("SELECT bytes FROM payloads WHERE id = ?1")
                        .and_then(|mut statement| {
                            statement
                                .query_row([admit.payload.id.to_be_bytes().as_slice()], |row| {
                                    row.get(0)
                                })
                                .optional()
                        });
                    match stored {
                        Ok(stored) => stored.is_none_or(|stored| stored == bytes.as_slice()),
                        Err(_) => return unknown,
                    }
                }
                _ => true,
            };
            if record.recipe != canonical || !same_payload {
                return Applied {
                    reply: StoreReply::Violation(StoreViolation::RecipeMismatch),
                    committed_now: false,
                    wrote: false,
                };
            }
        }
        let phase = match established(&self.conn, &meta, operation) {
            Ok(Established::Outcome(outcome)) => return settled(outcome),
            Ok(Established::Absent(phase)) => phase,
            Err(_) => return unknown,
        };
        if phase != EpochPhase::Open {
            return rejected(self.commit_rejection(
                operation,
                &canonical,
                RejectReason::PreconditionFailed,
            ));
        }

        // 2 to 5. Guards, capacities and every row, in one transaction.
        let payload = command.payload.as_ref();
        let written = self.transact(|conn, meta| {
            let facts = match recipe {
                Recipe::AdmitPublication(admit) => stage_admission(conn, meta, admit, payload)?,
                Recipe::ReleaseInbound(release) => stage_release(conn, meta, release)?,
            };
            insert_operation(conn, operation, &canonical, Decision::Committed(facts))?;
            Ok(facts)
        });
        match written {
            Txn::Committed(facts) => Applied {
                reply: StoreReply::Outcome(StoreOutcome::Committed(facts)),
                committed_now: true,
                wrote: true,
            },
            Txn::Refused(reason) => rejected(self.commit_rejection(operation, &canonical, reason)),
            Txn::NotWritten => unknown,
        }
    }

    fn established(&self, operation: OperationId) -> Option<StoreOutcome> {
        let Ok(meta) = read_meta(&self.conn) else {
            // Nothing could be read: absence is not established, and no
            // fence may be written on it.
            return Some(StoreOutcome::PendingOrUnknown);
        };
        match established(&self.conn, &meta, operation) {
            Ok(Established::Outcome(outcome)) => Some(outcome),
            Ok(Established::Absent(_)) => None,
            Err(_) => Some(StoreOutcome::PendingOrUnknown),
        }
    }

    fn resolve_and_fence(&mut self, operation: OperationId) -> Resolved {
        let settled = |outcome| Resolved {
            outcome,
            fenced_now: false,
        };
        let written = self.transact(|conn, meta| {
            // Read again inside the transaction: the fence is written only
            // on an absence established in it.
            match established(conn, meta, operation)? {
                Established::Outcome(outcome) => return Err(Abort::Refused(Some(outcome))),
                Established::Absent(_) => {}
            }
            // A fence holds a receipt slot. Without one no fence is
            // written, and absence proves nothing: unknown.
            charge(meta, Accounts::RECEIPT).map_err(|_| Abort::Refused(None))?;
            conn.prepare_cached("INSERT INTO fences (id) VALUES (?1)")?
                .execute([operation.to_be_bytes().as_slice()])?;
            Ok(())
        });
        match written {
            Txn::Committed(()) => Resolved {
                outcome: StoreOutcome::RejectedCertain(RejectReason::Fenced),
                fenced_now: true,
            },
            Txn::Refused(Some(outcome)) => settled(outcome),
            Txn::Refused(None) | Txn::NotWritten => settled(StoreOutcome::PendingOrUnknown),
        }
    }

    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused> {
        match self.transact(|conn, meta| stage_admin(conn, meta, command)) {
            Txn::Committed(done) => Ok(done),
            Txn::Refused(refusal) => Err(refusal),
            Txn::NotWritten => Err(AdminRefused::WriteFailed),
        }
    }

    fn recover(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        if limit == 0 {
            return Err(RecoveryRefused::EmptyPage);
        }
        let meta = read_meta(&self.conn).map_err(|_| RecoveryRefused::AllocationFailed)?;
        let revision = meta.revision;
        let start = match cursor {
            Some(cursor) if cursor.revision() != revision => {
                return Err(RecoveryRefused::FrontierMoved);
            }
            Some(cursor) => cursor.position(),
            None => 0,
        };
        let mut facts =
            BoundedVec::with_capacity(limit).map_err(|_| RecoveryRefused::AllocationFailed)?;
        // One more than the page, to know whether a next page exists.
        let wanted = limit
            .checked_add(1)
            .ok_or(RecoveryRefused::AllocationFailed)?;
        let read = self
            .inventory(&meta, start, wanted)
            .map_err(|_| RecoveryRefused::AllocationFailed)?;
        let mut read = read.into_iter();
        let mut position = start;
        while !facts.is_full() {
            let Some(fact) = read.next() else {
                return Ok(RecoveryPage { facts, next: None });
            };
            if facts.try_push(fact?).is_err() {
                return Err(RecoveryRefused::AllocationFailed);
            }
            position = position
                .checked_add(1)
                .ok_or(RecoveryRefused::AllocationFailed)?;
        }
        let next = read
            .next()
            .is_some()
            .then(|| RecoveryCursor::new(revision, position));
        Ok(RecoveryPage { facts, next })
    }

    fn snapshot(&self) -> DurableState {
        // The interface has no error here. A caller that must not stop on
        // a value that does not decode calls `try_snapshot` or `inspect`.
        self.try_snapshot()
            .expect("the durable state of the SQLite store decodes")
    }
}
