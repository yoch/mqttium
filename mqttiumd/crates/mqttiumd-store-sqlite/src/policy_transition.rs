//! Offline lease, complete state binding, and a transactional authority change.

use super::{
    Connection, OpenError, OpenFlags, ReadBudget, ReadError, SECURE_SCHEMA_VERSION, SqliteStore,
    StoragePolicy, TransactionBehavior, params, quick_check, read_snapshot, reader, writer_lock,
    writer_lock_path,
};
use mqttiumd_store_api::{DurableState, SessionState};
use rusqlite::backup::{Backup, StepResult};
use rusqlite::types::ValueRef;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

const DATABASE_BYTES: i64 = 128 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub struct PolicyInventory {
    pub sha256: [u8; 32],
    pub incarnation: u64,
    pub revision: u64,
    pub counts: BTreeMap<String, usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyMaintenanceError {
    WrongSchema(i64),
    OldPolicyMismatch,
    UnknownAuthority,
    OwnerChange,
    Read(ReadError),
    UnsupportedDatabasePath,
    WriterAlive,
    WriterLock,
    StalePlan,
    Backup,
    BusinessStateChanged,
    CommitUnknown,
    ReadbackFailed,
}

impl PolicyMaintenanceError {
    pub fn code(self) -> &'static str {
        match self {
            Self::WrongSchema(_) => "wrong_schema",
            Self::OldPolicyMismatch => "old_policy_mismatch",
            Self::UnknownAuthority => "unknown_durable_authority",
            Self::OwnerChange => "non_monotone_policy_change",
            Self::Read(ReadError::InvalidData) => "invalid_durable_state",
            Self::Read(ReadError::LimitExceeded) => "capacity_exceeded",
            Self::Read(_) => "durable_read_failed",
            Self::UnsupportedDatabasePath => "unsupported_database_path",
            Self::WriterAlive => "writer_alive",
            Self::WriterLock => "writer_lock_unavailable",
            Self::StalePlan => "stale_plan",
            Self::Backup => "backup_failed",
            Self::BusinessStateChanged => "business_state_changed",
            Self::CommitUnknown => "commit_unknown",
            Self::ReadbackFailed => "readback_failed",
        }
    }
}

impl From<OpenError> for PolicyMaintenanceError {
    fn from(error: OpenError) -> Self {
        match error {
            OpenError::SchemaVersion(version) => Self::WrongSchema(version),
            OpenError::SecurityPolicyMismatch => Self::OldPolicyMismatch,
            OpenError::WriterAlive => Self::WriterAlive,
            OpenError::WriterLock(_) => Self::WriterLock,
            other => Self::Read(other.read_error()),
        }
    }
}

impl From<rusqlite::Error> for PolicyMaintenanceError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Read(reader::sql_error(&error))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyTransition {
    Applied { backup_sha256: [u8; 32] },
    AlreadyApplied,
    Unchanged,
}

/// In-process observation points. The normal CLI supplies an empty observer.
#[derive(Clone, Copy)]
pub enum PolicyTransitionPoint {
    FingerprintStaged,
    Committed,
}

fn database_path(path: &Path) -> Result<PathBuf, PolicyMaintenanceError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| PolicyMaintenanceError::Read(reader::io_error(&error)))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(PolicyMaintenanceError::UnsupportedDatabasePath);
    }
    // Ancestor aliases still denote the same adjacent lock file. A final
    // symlink or a hard link can give the database a second lock name.
    path.canonicalize()
        .map_err(|error| PolicyMaintenanceError::Read(reader::io_error(&error)))
}

fn connect(path: &Path, mode: OpenFlags) -> Result<Connection, PolicyMaintenanceError> {
    let conn = Connection::open_with_flags(path, mode | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
    conn.busy_timeout(Duration::ZERO)?;
    Ok(conn)
}

fn check_schema(conn: &Connection) -> Result<(), PolicyMaintenanceError> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version != SECURE_SCHEMA_VERSION {
        return Err(PolicyMaintenanceError::WrongSchema(version));
    }
    let pages: i64 = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    if pages < 1 || size < 1 {
        return Err(PolicyMaintenanceError::Read(ReadError::InvalidData));
    }
    if pages
        .checked_mul(size)
        .is_none_or(|bytes| bytes > DATABASE_BYTES)
    {
        return Err(PolicyMaintenanceError::Read(ReadError::LimitExceeded));
    }
    let mut table = conn.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
    )?;
    for name in BUSINESS_ROWS[1..]
        .iter()
        .map(|(name, _)| *name)
        .chain(["security_policy", "security_client_owners"])
    {
        if !table.query_row([name], |row| row.get::<_, bool>(0))? {
            return Err(PolicyMaintenanceError::Read(ReadError::InvalidData));
        }
    }
    Ok(())
}

// Fixed queries for the secure schema. Include the semantic schema and owner rows that
// must stay fixed. Exclude only the two authority tables that can change. Do
// not depend on page layout, checkpoints, query planner or row insertion order.
const BUSINESS_ROWS: [(&str, &str); 16] = [
    (
        "sqlite_schema",
        "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name",
    ),
    ("meta", "SELECT * FROM meta ORDER BY one"),
    ("sessions", "SELECT * FROM sessions ORDER BY id"),
    ("wills", "SELECT * FROM wills ORDER BY id"),
    ("session_clock", "SELECT * FROM session_clock ORDER BY one"),
    ("routes", "SELECT * FROM routes ORDER BY topic,position"),
    ("operations", "SELECT * FROM operations ORDER BY id"),
    ("fences", "SELECT * FROM fences ORDER BY id"),
    (
        "receipt_origins",
        "SELECT * FROM receipt_origins ORDER BY epoch,owner",
    ),
    ("publications", "SELECT * FROM publications ORDER BY id"),
    ("retained", "SELECT * FROM retained ORDER BY topic"),
    ("payloads", "SELECT * FROM payloads ORDER BY id"),
    (
        "obligations",
        "SELECT * FROM obligations ORDER BY publication,idx",
    ),
    (
        "outbound",
        "SELECT * FROM outbound ORDER BY publication,idx",
    ),
    (
        "inbound",
        "SELECT * FROM inbound ORDER BY session,packet_id",
    ),
    (
        "security_session_owners",
        "SELECT * FROM security_session_owners ORDER BY session",
    ),
];

fn business_sha256(conn: &Connection) -> Result<[u8; 32], PolicyMaintenanceError> {
    let mut meter = reader::Meter::new(ReadBudget::SNAPSHOT);
    let mut hash = Sha256::new();
    hash.update(b"mqttiumd/policy-business-state/schema13/v1\0");
    for (name, sql) in BUSINESS_ROWS {
        hash.update(name.as_bytes());
        hash.update([0]);
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            meter
                .row(row, &[])
                .map_err(|fault| PolicyMaintenanceError::Read(fault.read_error()))?;
            hash.update([1]);
            for index in 0..row.as_ref().column_count() {
                let value = row.get_ref(index)?;
                match value {
                    ValueRef::Null => hash.update([0]),
                    ValueRef::Integer(n) => {
                        hash.update([1]);
                        hash.update(n.to_be_bytes());
                    }
                    ValueRef::Text(bytes) | ValueRef::Blob(bytes) => {
                        hash.update([if matches!(value, ValueRef::Text(_)) {
                            2
                        } else {
                            3
                        }]);
                        hash.update((bytes.len() as u64).to_be_bytes());
                        hash.update(bytes);
                    }
                    ValueRef::Real(_) => {
                        return Err(PolicyMaintenanceError::Read(ReadError::InvalidData));
                    }
                }
            }
        }
        hash.update([0]);
    }
    Ok(hash.finalize().into())
}

fn check_references(conn: &Connection, state: &DurableState) -> Result<(), PolicyMaintenanceError> {
    let invalid = PolicyMaintenanceError::Read(ReadError::InvalidData);
    let mut clients = std::collections::BTreeSet::new();
    if state.sessions.len() > state.limits.max_sessions
        || state
            .sessions
            .iter()
            .any(|(id, session)| *id >= state.next_session || !clients.insert(&session.client_id))
        || state.routes.values().flatten().any(|target| {
            !state
                .sessions
                .get(&target.session)
                .is_some_and(|session| session.state == SessionState::Active)
        })
        || state.inbound.iter().any(|((source, _), exchange)| {
            !state
                .sessions
                .get(source)
                .is_some_and(|session| session.state == SessionState::Active)
                || !state
                    .publications
                    .get(&exchange.publication)
                    .is_some_and(|publication| publication.source == *source)
        })
    {
        return Err(invalid);
    }
    let mut refs = BTreeMap::<_, u64>::new();
    for publication in state
        .retained
        .values()
        .map(|row| row.publication)
        .chain(state.obligations.keys().map(|id| id.publication))
    {
        let payload = state.publications.get(&publication).ok_or(invalid)?.payload;
        *refs.entry(payload).or_default() += 1;
    }
    if state.obligations.values().any(|target| {
        !state.sessions.get(&target.target).is_some_and(|session| {
            session.state == SessionState::Active && session.revision == target.target_revision
        })
    }) || refs.len() != state.payloads.len()
        || state
            .payloads
            .iter()
            .any(|(id, row)| refs.get(id) != Some(&row.references))
    {
        return Err(invalid);
    }
    let payload_bytes: u64 = state
        .payloads
        .values()
        .map(|row| row.bytes.len() as u64)
        .sum();
    let references: u64 = refs.values().sum();
    let exchanges = (state.inbound.len() + state.outbound.len()) as u64;
    let receipts = (state.operations.len() + state.fences.len()) as u64;
    for (observed, accounted, limit) in [
        (
            payload_bytes,
            state.accounts.payload_bytes.get(),
            state.limits.payload_bytes.get(),
        ),
        (
            references,
            state.accounts.references.get(),
            state.limits.references.get(),
        ),
        (
            exchanges,
            state.accounts.exchange_slots.get(),
            state.limits.exchange_slots.get(),
        ),
        (
            receipts,
            state.accounts.receipt_slots.get(),
            state.limits.receipt_slots.get(),
        ),
    ] {
        if observed != accounted || accounted > limit {
            return Err(invalid);
        }
    }
    let mut sizes = conn.prepare("SELECT length,length(bytes) FROM payloads")?;
    let mut rows = sizes.query([])?;
    while let Some(row) = rows.next()? {
        let len = super::blob_u64(reader::borrow_blob(row, 0, 8).map_err(|_| invalid)?)
            .map_err(|_| invalid)?;
        if Some(len) != u64::try_from(row.get::<_, i64>(1)?).ok() {
            return Err(invalid);
        }
    }
    Ok(())
}

fn inventory(conn: &Connection) -> Result<PolicyInventory, PolicyMaintenanceError> {
    if quick_check(conn)? != ["ok"] {
        return Err(PolicyMaintenanceError::Read(ReadError::InvalidData));
    }
    let state = read_snapshot(conn, ReadBudget::SNAPSHOT)
        .map_err(|fault| PolicyMaintenanceError::Read(fault.read_error()))?;
    check_references(conn, &state)?;
    let (exact, wildcard) =
        state
            .routes
            .iter()
            .fold((0, 0), |(exact, wildcard), (filter, targets)| {
                if filter.iter().any(|byte| matches!(*byte, b'+' | b'#')) {
                    (exact, wildcard + targets.len())
                } else {
                    (exact + targets.len(), wildcard)
                }
            });
    let armed = state
        .wills
        .values()
        .filter(|will| will.state == mqttiumd_contract::will::WillState::Armed)
        .count();
    Ok(PolicyInventory {
        sha256: business_sha256(conn)?,
        incarnation: state.ledger.incarnation().get(),
        revision: state.revision,
        counts: [
            ("sessions", state.sessions.len()),
            ("exact_routes", exact),
            ("wildcard_routes", wildcard),
            ("obligations", state.obligations.len()),
            ("claims", state.outbound.len()),
            ("inbound", state.inbound.len()),
            ("retained", state.retained.len()),
            ("payloads", state.payloads.len()),
            ("publications", state.publications.len()),
            ("will_armed", armed),
            ("will_pending", state.wills.len() - armed),
            ("receipts", state.operations.len()),
            ("fences", state.fences.len()),
            ("receipt_frontiers", state.receipt_frontiers.len()),
        ]
        .into_iter()
        .map(|(name, count)| (name.to_owned(), count))
        .collect(),
    })
}

/// Sidecars are never valid outputs, even when checkpointing has removed them.
pub(super) fn distinct_destination(db: &Path, output: &Path) -> std::io::Result<PathBuf> {
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = output.file_name().ok_or(std::io::ErrorKind::InvalidInput)?;
    let absolute = parent.canonicalize()?.join(name);
    let db = db.canonicalize()?;
    let collides = ["", "-wal", "-shm", "-journal"].into_iter().any(|suffix| {
        let mut path = db.as_os_str().to_owned();
        path.push(suffix);
        absolute.as_os_str() == path
    });
    if collides || absolute == writer_lock_path(&db) {
        return Err(std::io::ErrorKind::InvalidInput.into());
    }
    Ok(absolute)
}

fn backup(
    db: &Path,
    from: &Connection,
    old: &StoragePolicy,
    expected: [u8; 32],
    path: &Path,
) -> Result<[u8; 32], PolicyMaintenanceError> {
    let failed = PolicyMaintenanceError::Backup;
    let path = distinct_destination(db, path).map_err(|_| failed)?;
    let parent = path.parent().ok_or(failed)?;
    let meta = fs::metadata(parent).map_err(|_| failed)?;
    if !meta.is_dir() || meta.mode() & 0o077 != 0 || path.symlink_metadata().is_ok() {
        return Err(failed);
    }
    let mut pending_name = path.as_os_str().to_owned();
    pending_name.push(".pending");
    let pending = PathBuf::from(pending_name);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&pending)
        .map_err(|_| failed)?;
    let mut copy = || -> Result<[u8; 32], PolicyMaintenanceError> {
        {
            let mut dest =
                connect(&pending, OpenFlags::SQLITE_OPEN_READ_WRITE).map_err(|_| failed)?;
            let size: i64 = from
                .query_row("PRAGMA page_size", [], |row| row.get(0))
                .map_err(|_| failed)?;
            dest.pragma_update(None, "page_size", size)
                .map_err(|_| failed)?;
            // Read from the already frozen source snapshot. A busy or locked
            // copy is a refusal, since the operator owns the exclusive lease.
            let result = Backup::new(from, &mut dest)
                .and_then(|copy| copy.step(-1))
                .map_err(|_| failed)?;
            if result != StepResult::Done {
                return Err(failed);
            }
            let mode: String = dest
                .query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))
                .map_err(|_| failed)?;
            if !mode.eq_ignore_ascii_case("delete") {
                return Err(failed);
            }
            check_schema(&dest).map_err(|_| failed)?;
            old.verify_in(&dest).map_err(|_| failed)?;
            if inventory(&dest).map_err(|_| failed)?.sha256 != expected {
                return Err(failed);
            }
            dest.close().map_err(|_| failed)?;
        }
        file.sync_all().map_err(|_| failed)?;
        let mut hash = Sha256::new();
        let mut bytes = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut bytes).map_err(|_| failed)?;
            if read == 0 {
                break;
            }
            hash.update(&bytes[..read]);
        }
        // Publish a complete, synced standalone image without replacement.
        fs::hard_link(&pending, &path).map_err(|_| failed)?;
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|_| failed)?;
        Ok(hash.finalize().into())
    };
    let result = copy();
    // Only our exclusive temporary path is removed. An interrupted copy
    // leaves a .pending file that is never offered as a verified backup.
    let _ = fs::remove_file(&pending);
    result
}

impl SqliteStore {
    /// Inspects one committed WAL snapshot without settling the writer or
    /// advancing an epoch. A live writer can make the plan stale.
    pub fn inspect_policy(
        path: &Path,
        old: &StoragePolicy,
    ) -> Result<PolicyInventory, PolicyMaintenanceError> {
        let db = database_path(path)?;
        let conn = connect(&db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let read = conn.unchecked_transaction()?;
        check_schema(&read)?;
        old.verify_in(&read)?;
        let result = inventory(&read)?;
        read.commit()?;
        Ok(result)
    }

    /// Validates output paths before creation, including plan outputs.
    pub fn policy_output_path(db: &Path, output: &Path) -> std::io::Result<PathBuf> {
        distinct_destination(db, output)
    }

    /// The product checks the grants. The store independently enforces
    /// immutable existing owners and exact old and new authority rows.
    pub fn apply_policy(
        path: &Path,
        old: &StoragePolicy,
        new: &StoragePolicy,
        expected: &PolicyInventory,
        destination: &Path,
        mut observe: impl FnMut(PolicyTransitionPoint),
    ) -> Result<PolicyTransition, PolicyMaintenanceError> {
        old.validate()?;
        new.validate()?;
        if old
            .owners
            .iter()
            .any(|(id, owner)| new.owners.get(id) != Some(owner))
        {
            return Err(PolicyMaintenanceError::OwnerChange);
        }
        let db = database_path(path)?;
        let _lease = writer_lock(&db)?;
        let backup_sha256 = {
            let conn = connect(&db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            let read = conn.unchecked_transaction()?;
            check_schema(&read)?;
            let fingerprint = StoragePolicy::recorded_fingerprint(&read)?;
            let old_authority = fingerprint == old.fingerprint;
            let policy = if old_authority {
                old
            } else if fingerprint == new.fingerprint {
                new
            } else {
                return Err(PolicyMaintenanceError::UnknownAuthority);
            };
            policy.verify_in(&read)?;
            if inventory(&read)? != *expected {
                return Err(PolicyMaintenanceError::StalePlan);
            }
            if !old_authority {
                return Ok(PolicyTransition::AlreadyApplied);
            }
            if old.fingerprint == new.fingerprint {
                new.verify_in(&read)?;
                return Ok(PolicyTransition::Unchanged);
            }
            let copied = backup(&db, &read, old, expected.sha256, destination)?;
            read.commit()?;
            copied
        };

        let mut conn = connect(&db, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        check_schema(&tx)?;
        old.verify_in(&tx)?;
        if business_sha256(&tx)? != expected.sha256 {
            return Err(PolicyMaintenanceError::StalePlan);
        }
        if tx.execute(
            "UPDATE security_policy SET fingerprint=?1 WHERE one=1 AND fingerprint=?2",
            params![new.fingerprint.as_slice(), old.fingerprint.as_slice()],
        )? != 1
        {
            return Err(PolicyMaintenanceError::StalePlan);
        }
        observe(PolicyTransitionPoint::FingerprintStaged);
        {
            let mut add = tx.prepare("INSERT INTO security_client_owners VALUES (?1, ?2)")?;
            for (id, owner) in &new.owners {
                if !old.owners.contains_key(id) {
                    add.execute(params![id, owner])?;
                }
            }
        }
        new.verify_in(&tx)?;
        if business_sha256(&tx)? != expected.sha256 {
            return Err(PolicyMaintenanceError::BusinessStateChanged);
        }
        tx.commit()
            .map_err(|_| PolicyMaintenanceError::CommitUnknown)?;
        observe(PolicyTransitionPoint::Committed);
        let readback = || -> Result<(), PolicyMaintenanceError> {
            let read = conn.unchecked_transaction()?;
            check_schema(&read)?;
            new.verify_in(&read)?;
            if business_sha256(&read)? != expected.sha256 {
                return Err(PolicyMaintenanceError::BusinessStateChanged);
            }
            read.commit()?;
            Ok(())
        };
        readback().map_err(|_| PolicyMaintenanceError::ReadbackFailed)?;
        Ok(PolicyTransition::Applied { backup_sha256 })
    }
}
