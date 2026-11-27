//! Atomic Schema-14 / Schema-15 snapshot writer for `DurableState`.
//!
//! Writes a complete, self-contained SQLite database image into a sibling
//! temporary file in `journal_mode = DELETE`, syncs the file descriptor,
//! removes any stale WAL/SHM sidecars under the caller's exclusive
//! `WriterLease`, atomically renames over the target path, and syncs the
//! parent directory.

use std::fs::{File, OpenOptions, Permissions, remove_file};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};

use mqttiumd_contract::will::WillState;
use mqttiumd_store_api::DurableState;

use crate::{
    FORMAT, Fault, Meta, OpenError, StoragePolicy, WriterLease, cause_code, exchange_blob,
    inbound_phase_code, insert_operation, obligation_phase_code, qos_code, schema, security,
    session_state_code, sessions, u64_blob, write_meta, writer_lock_path,
};

static SNAPSHOT_SEQ: AtomicU64 = AtomicU64::new(0);

fn sibling_temp_path(path: &Path) -> PathBuf {
    let seq = SNAPSHOT_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".snapshot.{}.{seq}.tmp", std::process::id()));
    PathBuf::from(name)
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn remove_if_exists(path: &Path) -> Result<(), OpenError> {
    match remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(OpenError::WriterLock(error)),
    }
}

fn create_temp_with_permissions(
    tmp: &Path,
    existing: Option<&Permissions>,
    secured: bool,
) -> Result<(), OpenError> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        let mode = existing
            .map(Permissions::mode)
            .unwrap_or(if secured { 0o600 } else { 0o666 });
        opts.mode(mode);
    }
    let file = opts.open(tmp).map_err(OpenError::WriterLock)?;
    if let Some(perms) = existing {
        file.set_permissions(perms.clone())
            .map_err(OpenError::WriterLock)?;
    } else if secured {
        #[cfg(unix)]
        file.set_permissions(Permissions::from_mode(0o600))
            .map_err(OpenError::WriterLock)?;
    }
    Ok(())
}

/// Makes durable the directory entry of `path`: opens its parent directory
/// (the current one for a bare name) and syncs it. Used after a rename
/// that replaces a file of the store.
pub fn sync_parent_dir(path: &Path) -> Result<(), OpenError> {
    let dir = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(OpenError::WriterLock)
}

/// Atomically writes `state` as a complete Schema-14 (or Schema-15 when
/// `policy` is `Some`) SQLite database at `path`.
///
/// The caller must hold the exclusive `WriterLease` for `path`.
/// Preserves existing file permissions on `path` and verifies `fsync` of both
/// the snapshot image and its parent directory.
pub fn save_state_atomic(
    path: &Path,
    state: &DurableState,
    policy: Option<&StoragePolicy>,
    lease: &WriterLease,
) -> Result<(), OpenError> {
    if lease.lock_path() != writer_lock_path(path) {
        return Err(OpenError::WriterAlive);
    }
    let expected = security::version(policy);
    if let Some(policy) = policy {
        policy.validate()?;
    }
    let existing_perms = match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                return Err(OpenError::Corrupt);
            }
            Some(meta.permissions())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(OpenError::WriterLock(error)),
    };
    let tmp = sibling_temp_path(path);
    create_temp_with_permissions(&tmp, existing_perms.as_ref(), policy.is_some())?;
    let write_result = write_snapshot_file(&tmp, state, policy, expected);
    if let Err(error) = write_result {
        let _ = remove_file(&tmp);
        return Err(error);
    }
    if let Err(error) = File::open(&tmp).and_then(|file| file.sync_all()) {
        let _ = remove_file(&tmp);
        return Err(OpenError::WriterLock(error));
    }
    // Clean any stale WAL/SHM sidecars before replacing the main image so no
    // reader or next opener sees a mismatched WAL header.
    if let Err(error) = remove_if_exists(&sidecar_path(path, "-wal"))
        .and_then(|()| remove_if_exists(&sidecar_path(path, "-shm")))
    {
        let _ = remove_file(&tmp);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&tmp, path) {
        let _ = remove_file(&tmp);
        return Err(OpenError::WriterLock(error));
    }
    sync_parent_dir(path)?;
    Ok(())
}

fn write_snapshot_file(
    tmp: &Path,
    state: &DurableState,
    policy: Option<&StoragePolicy>,
    expected_version: i64,
) -> Result<(), OpenError> {
    let mut conn = Connection::open_with_flags(
        tmp,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(std::time::Duration::ZERO)?;
    // Standalone self-contained image (no -wal/-shm sidecars). Durability is
    // enforced explicitly via File::sync_all on the closed temp file before
    // atomic rename.
    let _: String = conn.query_row("PRAGMA journal_mode = DELETE", [], |row| row.get(0))?;
    conn.execute_batch("PRAGMA synchronous = OFF;")?;

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
    let meta = Meta {
        ledger: state.ledger,
        collected_through: state.collected_through,
        revision: state.revision,
        limits: state.limits,
        accounts: state.accounts,
        next_session: state.next_session,
        route_revision: state.route_revision,
    };
    write_meta(&tx, &meta).map_err(fault_to_open)?;

    if let Some(policy) = policy {
        policy.install(&tx)?;
    }

    if let Some(anchor) = state.session_clock {
        tx.execute(
            "INSERT INTO session_clock (one, domain, tick, logical) VALUES (1, ?1, ?2, ?3)",
            params![
                anchor.reading.domain.as_slice(),
                u64_blob(anchor.reading.tick).as_slice(),
                u64_blob(anchor.logical).as_slice(),
            ],
        )?;
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO sessions (id, revision, state, expiry, opened_by, detached_by, deadline, client_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for (id, row) in &state.sessions {
            let opened = row.opened_by.map(|op| op.to_be_bytes());
            let detached_op = row.detached.map(|d| d.operation.to_be_bytes());
            let deadline = row.detached.and_then(|d| d.deadline).map(u64_blob);
            stmt.execute(params![
                id.to_be_bytes().as_slice(),
                u64_blob(row.revision.get()).as_slice(),
                session_state_code(row.state),
                sessions::expiry_code(row.expiry),
                opened.as_ref().map(|b| b.as_slice()),
                detached_op.as_ref().map(|b| b.as_slice()),
                deadline.as_ref().map(|b| b.as_slice()),
                row.client_id.as_slice(),
            ])?;
        }
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO wills (id, session, state, client_id, topic, payload, retain)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for (id, intent) in &state.wills {
            let state_code = match intent.state {
                WillState::Armed => 1i64,
                WillState::Pending => 2i64,
            };
            stmt.execute(params![
                id.to_be_bytes().as_slice(),
                intent.session.to_be_bytes().as_slice(),
                state_code,
                intent.client_id.as_slice(),
                intent.spec.topic.as_slice(),
                intent.spec.payload.as_slice(),
                i64::from(intent.spec.retain),
            ])?;
        }
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO routes (topic, position, session, qos, options)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for (topic, targets) in &state.routes {
            for (pos, target) in targets.iter().enumerate() {
                stmt.execute(params![
                    topic.as_slice(),
                    u64_blob(pos as u64).as_slice(),
                    target.session.to_be_bytes().as_slice(),
                    qos_code(target.qos),
                    i64::from(target.options.bits()),
                ])?;
            }
        }
    }

    for (operation, record) in &state.operations {
        insert_operation(&tx, *operation, &record.recipe, record.decision)
            .map_err(fault_to_open)?;
    }

    {
        let mut stmt = tx.prepare("INSERT INTO fences (id) VALUES (?1)")?;
        for fence in &state.fences {
            stmt.execute([fence.to_be_bytes().as_slice()])?;
        }
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO receipt_origins (epoch, owner, consumed_through, publication_through, payload_through)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for closure in state.receipt_frontiers.values() {
            stmt.execute(params![
                closure.origin.epoch.to_be_bytes().as_slice(),
                closure.origin.owner.to_be_bytes().as_slice(),
                u64_blob(closure.through.get()).as_slice(),
                u64_blob(closure.publication_through.get()).as_slice(),
                u64_blob(closure.payload_through.get()).as_slice(),
            ])?;
        }
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO publications
                 (id, operation, source, qos, retain, payload, retained_version,
                  expiry_domain, expiry_tick, expiry_interval, topic, properties)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        )?;
        for (id, pub_row) in &state.publications {
            let rv = pub_row.retained_version.map(|v| v.to_be_bytes());
            let exp_domain = pub_row.expiry.map(|e| e.observed.domain);
            let exp_tick = pub_row.expiry.map(|e| u64_blob(e.observed.tick));
            let exp_interval = pub_row.expiry.map(|e| i64::from(e.interval));
            let props_blob = if let Some(props) = &pub_row.properties {
                if props.is_empty() {
                    return Err(OpenError::Corrupt);
                }
                let mut encoded = Vec::new();
                props
                    .encode_canonical(&mut encoded)
                    .map_err(|_| OpenError::Corrupt)?;
                Some(encoded)
            } else {
                None
            };
            stmt.execute(params![
                id.to_be_bytes().as_slice(),
                pub_row.operation.to_be_bytes().as_slice(),
                pub_row.source.to_be_bytes().as_slice(),
                qos_code(pub_row.qos),
                i64::from(pub_row.retain),
                pub_row.payload.to_be_bytes().as_slice(),
                rv.as_ref().map(|b| b.as_slice()),
                exp_domain.as_ref().map(|b| b.as_slice()),
                exp_tick.as_ref().map(|b| b.as_slice()),
                exp_interval,
                pub_row.topic.as_slice(),
                props_blob.as_deref(),
            ])?;
        }
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO retained (topic, publication, producer_client_id)
             VALUES (?1, ?2, ?3)",
        )?;
        for (topic, row) in &state.retained {
            stmt.execute(params![
                topic.as_slice(),
                row.publication.to_be_bytes().as_slice(),
                row.producer_client_id.as_slice(),
            ])?;
        }
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO payloads (id, refs, length, bytes)
             VALUES (?1, ?2, ?3, ?4)",
        )?;
        for (id, payload) in &state.payloads {
            stmt.execute(params![
                id.to_be_bytes().as_slice(),
                u64_blob(payload.references).as_slice(),
                u64_blob(payload.bytes.len() as u64).as_slice(),
                payload.bytes.as_slice(),
            ])?;
        }
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO obligations (publication, idx, target, target_revision, qos, cause, retain, phase)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for (delivery, row) in &state.obligations {
            stmt.execute(params![
                delivery.publication.to_be_bytes().as_slice(),
                i64::from(delivery.index),
                row.target.to_be_bytes().as_slice(),
                u64_blob(row.target_revision.get()).as_slice(),
                qos_code(row.qos),
                cause_code(row.cause),
                i64::from(row.retain),
                obligation_phase_code(row.phase),
            ])?;
        }
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO outbound (publication, idx, target, target_revision, packet_id, claim, started)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for (delivery, row) in &state.outbound {
            stmt.execute(params![
                delivery.publication.to_be_bytes().as_slice(),
                i64::from(delivery.index),
                row.target.to_be_bytes().as_slice(),
                u64_blob(row.target_revision.get()).as_slice(),
                i64::from(row.packet_id.get()),
                row.claim.to_be_bytes().as_slice(),
                i64::from(row.started),
            ])?;
        }
    }

    {
        let mut stmt = tx.prepare(
            "INSERT INTO inbound (session, packet_id, exchange, publication, phase)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for ((session, packet_id), row) in &state.inbound {
            stmt.execute(params![
                session.to_be_bytes().as_slice(),
                i64::from(packet_id.get()),
                exchange_blob(row.exchange).as_slice(),
                row.publication.to_be_bytes().as_slice(),
                inbound_phase_code(row.phase),
            ])?;
        }
    }

    tx.pragma_update(None, "user_version", expected_version)?;
    tx.commit()?;
    conn.close().map_err(|(_, error)| OpenError::Sql(error))?;
    Ok(())
}

fn fault_to_open(fault: Fault) -> OpenError {
    match fault {
        Fault::Sql(error) => OpenError::Sql(error),
        Fault::Corrupt => OpenError::Corrupt,
        Fault::Read(error) => OpenError::Read(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SqliteStore, acquire_writer_lock};
    use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
    use mqttiumd_contract::ids::StoreIncarnation;
    use mqttiumd_store_api::Limits;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

    fn test_limits() -> Limits {
        Limits {
            payload_bytes: PayloadBytes::new(1024 * 1024),
            references: References::new(256),
            exchange_slots: ExchangeSlots::new(64),
            receipt_slots: ReceiptSlots::new(256),
            max_targets_per_admission: 16,
            max_topic_bytes: 128,
            max_payload_bytes: 32_768,
            max_sessions: 32,
            max_route_targets: 16,
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mqttiumd-snap-unit-{name}-{}-{}",
            std::process::id(),
            TEST_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[cfg(unix)]
    #[test]
    fn save_state_atomic_preserves_existing_permissions_and_rejects_symlinks() {
        let dir = temp_dir("perms");
        let db = dir.join("state.sqlite");
        let store = SqliteStore::create(&db, StoreIncarnation::new(1), test_limits()).unwrap();
        let (state, _settings, lease) = store.into_state_and_lease().unwrap();

        for mode in [0o600u32, 0o640u32] {
            std::fs::set_permissions(&db, Permissions::from_mode(mode)).unwrap();
            save_state_atomic(&db, &state, None, &lease).unwrap();
            let got = std::fs::metadata(&db).unwrap().permissions().mode() & 0o777;
            assert_eq!(got, mode, "mode {mode:o} must be preserved across snapshot");
        }

        // Symlink target must be refused without modifying its target.
        let link = dir.join("link.sqlite");
        std::os::unix::fs::symlink(&db, &link).unwrap();
        let link_lease = acquire_writer_lock(&link).unwrap();
        assert!(matches!(
            save_state_atomic(&link, &state, None, &link_lease),
            Err(OpenError::Corrupt)
        ));
        drop(link_lease);
        drop(lease);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn save_state_atomic_fails_cleanly_on_unwritable_dir_and_verifies_parent_fsync() {
        let dir = temp_dir("fsync");
        let db = dir.join("state.sqlite");
        let store = SqliteStore::create(&db, StoreIncarnation::new(2), test_limits()).unwrap();
        let (state, _settings, lease) = store.into_state_and_lease().unwrap();
        save_state_atomic(&db, &state, None, &lease).unwrap();
        let before = std::fs::read(&db).unwrap();

        // Non-existent parent dir fails parent fsync verification explicitly.
        assert!(sync_parent_dir(&dir.join("missing/file.sqlite")).is_err());

        // Read-only parent directory refuses temp snapshot creation while preserving prior DB.
        // Skip only if the effective user ignores 0o555 directory permissions (e.g. root).
        std::fs::set_permissions(&dir, Permissions::from_mode(0o555)).unwrap();
        let probe = dir.join("perm-probe");
        let dir_is_enforced = File::create(&probe).is_err();
        let _ = remove_file(&probe);
        if dir_is_enforced {
            let err = save_state_atomic(&db, &state, None, &lease);
            std::fs::set_permissions(&dir, Permissions::from_mode(0o755)).unwrap();
            assert!(err.is_err(), "read-only parent dir must fail snapshot");
            assert_eq!(std::fs::read(&db).unwrap(), before);
        } else {
            std::fs::set_permissions(&dir, Permissions::from_mode(0o755)).unwrap();
        }

        drop(lease);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The state is handed over to an engine that replaces the database
    /// file and deletes its write-ahead log. Nothing committed may then be
    /// in that log only, also when SQLite did not empty it at the close
    /// because another connection, a reader such as `inspect`, was open.
    #[test]
    fn handing_the_state_over_leaves_no_commit_in_the_write_ahead_log() {
        use mqttiumd_store_api::{AdminCommand, DurableStore};

        let dir = temp_dir("handover");
        let db = dir.join("state.sqlite");
        let mut store = SqliteStore::create(&db, StoreIncarnation::new(3), test_limits()).unwrap();
        store
            .admin(AdminCommand::CreateSession {
                client_id: b"client".to_vec(),
            })
            .expect("a session");
        let reader = Connection::open_with_flags(&db, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let _: i64 = reader
            .query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0))
            .unwrap();

        let (state, _settings, lease) = store.into_state_and_lease().unwrap();
        let log = sidecar_path(&db, "-wal");
        let left = std::fs::metadata(&log).map_or(0, |meta| meta.len());
        assert_eq!(left, 0, "bytes left in the write-ahead log");

        // The main file alone holds the state.
        drop(reader);
        drop(lease);
        remove_if_exists(&log).unwrap();
        remove_if_exists(&sidecar_path(&db, "-shm")).unwrap();
        let reopened = SqliteStore::open(&db).expect("the main file opens alone");
        assert_eq!(reopened.try_snapshot().unwrap(), state);
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
