//! Log-first durable store (`LogStore`): CRC-32C-framed append-only decision
//! journal (`.mlog`) paired with an indexed in-memory [`VolatileStore`] and
//! atomic Schema-14 / Schema-15 SQLite checkpoints.
//!
//! # Architecture
//! Every durable mutation in `mqttiumd` is already a deterministic, canonical
//! command (`Command`, `OpenEpoch`, `ResolveAndFence`, receipt collection,
//! session clock checkpoint, message expiry sweep, or `AdminCommand`).
//! Instead of translating each command into multiple SQL statements across
//! 12 B-tree tables and 4 KiB WAL pages, `LogStore`:
//! 1. Encodes one compact CRC-32C-framed group record (reusing
//!    `FrozenRecipe::canonical_bytes()` with zero re-serialization).
//! 2. Appends to `<path>.mlog` and calls `File::sync_data()` (`fdatasync`)
//!    **before** mutating in-memory state (`DUR-001`, `DUR-002`).
//! 3. Applies the deterministic transitions to `VolatileStore` in O(log N) RAM
//!    only after `sync_data()` succeeds. On any write/sync failure (or
//!    `Durability::Failing`), `.mlog` is truncated back to its prior offset
//!    and RAM remains untouched.
//! 4. Periodically compacts `.mlog` into an atomic Schema-14 / Schema-15 SQLite
//!    checkpoint at `<path>`, rotating `<path>.mlog` with `base_revision = R`
//!    and `fsync`ing the parent directory.
//!
//! Recovery loads the SQLite checkpoint `<path>` (preserving all schema and
//! `StoragePolicy` validation) and replays every valid CRC-verified `.mlog`
//! frame in commit order. A truncated or torn tail frame at EOF (power loss
//! prior to `fdatasync`) is cleanly truncated; any mid-file corruption,
//! header mismatch, or invalid record fails closed with `OpenError::Corrupt`.

use std::fs::{File, OpenOptions, Permissions, remove_file};
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mqttiumd_contract::command::{
    Command, EpochLedger, FrozenRecipe, LedgerOpening, OpenEpoch, OpeningAttempt, Recipe,
};
use mqttiumd_contract::ids::{
    DeliveryId, LogicalOwnerId, OperationId, OperationSeq, PayloadSeq, PublicationId,
    PublicationSeq, RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::message_expiry::MessageTime;
use mqttiumd_contract::outcome::{OpeningOutcome, StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_contract::receipts::{
    ClosedReceiptCollection, ClosedReceiptReply, ReceiptClosure, ReceiptCollection,
    ReceiptCollectionReply, ReceiptOrigin,
};
use mqttiumd_store_api::{
    AdminCommand, AdminDone, AdminRefused, Applied, Durability, DurableState, DurableStore, Limits,
    MessageExpirySweep, ReadBudget, ReadError, RecoveryCursor, RecoveryPage, RecoveryRefused,
    Resolved, SessionClockAnchor, SessionClockReading, WillPage, WorkPage,
};
use mqttiumd_store_sqlite::{
    GroupRefused, OpenError, Settings, SqliteStore, StoragePolicy, WriterLease,
    acquire_writer_lock, save_state_atomic,
};

use crate::{UNKNOWN, VolatileStore};

/// Exact I/O boundaries of one prepared journal frame in instrumented builds.
#[cfg(feature = "log-write-points")]
#[derive(Clone, Copy, Debug)]
pub enum LogWritePoint {
    /// Before writing any byte of this frame.
    BeforeAppend,
    /// The complete frame is in the kernel page cache; sync_data has not run.
    BeforeSync,
    /// sync_data returned successfully, before publishing the frame in memory.
    AfterSync,
}

/// Maximum commands committed in one `LogStore::apply_group` frame.
pub const LOG_MAX_GROUP: usize = 256;

/// Default `.mlog` size threshold before automatic SQLite checkpoint compaction.
pub const DEFAULT_CHECKPOINT_BYTES: u64 = 128 * 1024 * 1024;

const WAL_MAGIC: &[u8; 4] = b"MQLG";
const WAL_VERSION: u16 = 1;
const WAL_HEADER_LEN: u64 = 24;
const MIN_PREALLOC_CHUNK: u64 = 256 * 1024;
const MAX_PREALLOC_CHUNK: u64 = 4 * 1024 * 1024;
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
/// The smallest unit a medium writes whole. A frame is written over
/// several of them and a power loss keeps any subset.
const SECTOR_BYTES: u64 = 512;
const ENCODE_BUF_SHRINK_THRESHOLD: usize = 256 * 1024;

const TAG_COMMAND: u8 = 1;
const TAG_OPEN_EPOCH: u8 = 2;
const TAG_RESOLVE_AND_FENCE: u8 = 3;
const TAG_COLLECT_RECEIPTS: u8 = 4;
const TAG_COLLECT_CLOSED_RECEIPTS: u8 = 5;
const TAG_CHECKPOINT_CLOCK: u8 = 6;
const TAG_EXPIRE_MESSAGES: u8 = 7;
const TAG_ADMIN: u8 = 8;

const ADMIN_CREATE_SESSION: u8 = 1;
const ADMIN_ADD_ROUTE: u8 = 2;
const ADMIN_DELETE_SESSION: u8 = 3;
const ADMIN_COLLECT_CLOSED_EPOCHS: u8 = 4;

static ROTATE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Pure-Rust Slice-by-8 Castagnoli CRC-32C (`0x82F63B78` reflected polynomial).
const CRC32C_TABLES: [[u32; 256]; 8] = {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        tables[0][i] = crc;
        i += 1;
    }
    let mut t = 1usize;
    while t < 8 {
        let mut i = 0usize;
        while i < 256 {
            let prev = tables[t - 1][i];
            tables[t][i] = tables[0][(prev & 0xff) as usize] ^ (prev >> 8);
            i += 1;
        }
        t += 1;
    }
    tables
};

#[must_use]
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let lo = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) ^ crc;
        let hi = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        crc = CRC32C_TABLES[7][(lo & 0xff) as usize]
            ^ CRC32C_TABLES[6][((lo >> 8) & 0xff) as usize]
            ^ CRC32C_TABLES[5][((lo >> 16) & 0xff) as usize]
            ^ CRC32C_TABLES[4][(lo >> 24) as usize]
            ^ CRC32C_TABLES[3][(hi & 0xff) as usize]
            ^ CRC32C_TABLES[2][((hi >> 8) & 0xff) as usize]
            ^ CRC32C_TABLES[1][((hi >> 16) & 0xff) as usize]
            ^ CRC32C_TABLES[0][(hi >> 24) as usize];
    }
    for &b in chunks.remainder() {
        crc = CRC32C_TABLES[0][((crc ^ u32::from(b)) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

fn wal_path_for(db_path: &Path) -> PathBuf {
    let mut name = db_path.as_os_str().to_os_string();
    name.push(".mlog");
    PathBuf::from(name)
}

fn wal_temp_path(wal_path: &Path) -> PathBuf {
    let seq = ROTATE_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut name = wal_path.as_os_str().to_os_string();
    name.push(format!(".rotate.{}.{seq}.tmp", std::process::id()));
    PathBuf::from(name)
}

fn sync_parent_dir(path: &Path) -> Result<(), OpenError> {
    let dir = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(OpenError::WriterLock)
}

fn encode_wal_header(incarnation: StoreIncarnation, base_revision: u64) -> [u8; 24] {
    let mut header = [0u8; 24];
    header[0..4].copy_from_slice(WAL_MAGIC);
    header[4..6].copy_from_slice(&WAL_VERSION.to_be_bytes());
    header[6..8].copy_from_slice(&0u16.to_be_bytes());
    header[8..16].copy_from_slice(&incarnation.get().to_be_bytes());
    header[16..24].copy_from_slice(&base_revision.to_be_bytes());
    header
}

fn decode_wal_header(header: &[u8; 24]) -> Result<(StoreIncarnation, u64), OpenError> {
    if &header[0..4] != WAL_MAGIC {
        return Err(OpenError::Corrupt);
    }
    let version = u16::from_be_bytes([header[4], header[5]]);
    let flags = u16::from_be_bytes([header[6], header[7]]);
    if version != WAL_VERSION || flags != 0 {
        return Err(OpenError::Corrupt);
    }
    let incarnation = StoreIncarnation::new(u64::from_be_bytes([
        header[8], header[9], header[10], header[11], header[12], header[13], header[14],
        header[15],
    ]));
    let base_revision = u64::from_be_bytes([
        header[16], header[17], header[18], header[19], header[20], header[21], header[22],
        header[23],
    ]);
    Ok((incarnation, base_revision))
}

fn write_fresh_wal_atomic(
    wal_path: &Path,
    incarnation: StoreIncarnation,
    base_revision: u64,
    secured: bool,
) -> Result<File, OpenError> {
    let tmp = wal_temp_path(wal_path);
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create_new(true);
    #[cfg(unix)]
    opts.mode(if secured { 0o600 } else { 0o666 });
    let mut file = opts.open(&tmp).map_err(OpenError::WriterLock)?;
    if secured {
        #[cfg(unix)]
        if let Err(err) = file.set_permissions(Permissions::from_mode(0o600)) {
            drop(file);
            let _ = remove_file(&tmp);
            return Err(OpenError::WriterLock(err));
        }
    }
    let header = encode_wal_header(incarnation, base_revision);
    if let Err(err) = file.write_all(&header).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = remove_file(&tmp);
        return Err(OpenError::WriterLock(err));
    }
    drop(file);
    if let Err(err) = std::fs::rename(&tmp, wal_path) {
        let _ = remove_file(&tmp);
        return Err(OpenError::WriterLock(err));
    }
    sync_parent_dir(wal_path)?;
    let mut reopened = OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal_path)
        .map_err(OpenError::WriterLock)?;
    reopened
        .seek(SeekFrom::End(0))
        .map_err(OpenError::WriterLock)?;
    Ok(reopened)
}

/// Durable log-first realisation of [`DurableStore`].
///
/// Every mutating call appends a CRC-32C-verified frame to `<path>.mlog` and
/// calls `fdatasync` before mutating the indexed in-memory [`VolatileStore`].
pub struct LogStore {
    store: VolatileStore,
    path: PathBuf,
    wal_path: PathBuf,
    wal: File,
    wal_len: u64,
    wal_alloc_len: u64,
    checkpoint_bytes: u64,
    policy: Option<StoragePolicy>,
    sqlite_settings: Option<Settings>,
    lease: WriterLease,
    durability: Durability,
    /// Log length at which the next checkpoint is attempted.
    compact_at: u64,
    /// The incarnation and revision of a checkpoint that replaced `<path>`
    /// while its `.mlog` is still the log of the checkpoint before. Nothing
    /// is appended to that log: the next write replaces it first.
    rotation_due: Option<(StoreIncarnation, u64)>,
    #[cfg(feature = "log-write-points")]
    write_hook: Option<Box<dyn FnMut(LogWritePoint) + Send>>,
    encode_buf: Vec<u8>,
}

impl std::fmt::Debug for LogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogStore")
            .field("path", &self.path)
            .field("wal_len", &self.wal_len)
            .field("secured", &self.policy.is_some())
            .finish_non_exhaustive()
    }
}

impl LogStore {
    /// Opens an existing `<path>` (plus any `<path>.mlog` journal) under the
    /// exclusive writer lease, or creates a fresh SQLite checkpoint + `.mlog`
    /// journal if `<path>` does not yet exist.
    pub fn open_or_create(
        path: &Path,
        incarnation: StoreIncarnation,
        limits: Limits,
        policy: Option<StoragePolicy>,
        require_existing: bool,
    ) -> Result<Self, OpenError> {
        let wal_path = wal_path_for(path);
        let exists = path.try_exists().map_err(OpenError::WriterLock)?;
        if !exists {
            if require_existing {
                return Err(OpenError::Read(ReadError::Unavailable));
            }
            if let Some(p) = policy.as_ref()
                && (p.owners.is_empty() || p.owners.len() > 4_096)
            {
                return Err(OpenError::SecurityPolicyMismatch);
            }
            let lease = acquire_writer_lock(path)?;
            let store = VolatileStore::create(incarnation, limits);
            let initial_state = store.snapshot().map_err(OpenError::Read)?;
            save_state_atomic(path, &initial_state, policy.as_ref(), &lease)?;
            let wal = write_fresh_wal_atomic(&wal_path, incarnation, 0, policy.is_some())?;
            return Ok(Self {
                store,
                path: path.to_path_buf(),
                wal_path,
                wal,
                wal_len: WAL_HEADER_LEN,
                wal_alloc_len: WAL_HEADER_LEN,
                checkpoint_bytes: DEFAULT_CHECKPOINT_BYTES,
                policy,
                sqlite_settings: None,
                lease,
                durability: Durability::Available,
                compact_at: DEFAULT_CHECKPOINT_BYTES,
                rotation_due: None,
                #[cfg(feature = "log-write-points")]
                write_hook: None,
                encode_buf: Vec::new(),
            });
        }

        let sqlite = match policy.as_ref() {
            Some(p) => SqliteStore::open_secured(path, p)?,
            None => SqliteStore::open(path)?,
        };
        let (state, settings, lease) = sqlite.into_state_and_lease()?;
        let checkpoint_rev = state.revision;
        let checkpoint_incarnation = state.ledger.incarnation();
        let mut store = VolatileStore::reopen(state);

        let wal_exists = match std::fs::symlink_metadata(&wal_path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                    return Err(OpenError::Corrupt);
                }
                true
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
            Err(err) => return Err(OpenError::WriterLock(err)),
        };

        let (wal, wal_len, wal_alloc_len) = if !wal_exists {
            let wal = write_fresh_wal_atomic(
                &wal_path,
                checkpoint_incarnation,
                checkpoint_rev,
                policy.is_some(),
            )?;
            (wal, WAL_HEADER_LEN, WAL_HEADER_LEN)
        } else {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal_path)
                .map_err(OpenError::WriterLock)?;
            let file_len = file.metadata().map_err(OpenError::WriterLock)?.len();
            if file_len < WAL_HEADER_LEN {
                return Err(OpenError::Corrupt);
            }
            let mut raw_header = [0u8; 24];
            file.read_exact(&mut raw_header)
                .map_err(OpenError::WriterLock)?;
            let (wal_incarnation, base_rev) = decode_wal_header(&raw_header)?;
            if wal_incarnation != checkpoint_incarnation {
                return Err(OpenError::Corrupt);
            }
            if base_rev > checkpoint_rev {
                return Err(OpenError::Corrupt);
            }
            if base_rev < checkpoint_rev {
                // Crash occurred after atomic checkpoint rename and before WAL rotation.
                drop(file);
                let wal = write_fresh_wal_atomic(
                    &wal_path,
                    checkpoint_incarnation,
                    checkpoint_rev,
                    policy.is_some(),
                )?;
                (wal, WAL_HEADER_LEN, WAL_HEADER_LEN)
            } else {
                let (valid_len, clean_tail) = replay_wal(&mut file, file_len, &mut store)?;
                let alloc_len = if !clean_tail && valid_len < file_len {
                    file.set_len(valid_len).map_err(OpenError::WriterLock)?;
                    file.sync_all().map_err(OpenError::WriterLock)?;
                    valid_len
                } else {
                    file_len
                };
                (file, valid_len, alloc_len)
            }
        };

        Ok(Self {
            store,
            path: path.to_path_buf(),
            wal_path,
            wal,
            wal_len,
            wal_alloc_len,
            checkpoint_bytes: DEFAULT_CHECKPOINT_BYTES,
            policy,
            sqlite_settings: Some(settings),
            lease,
            durability: Durability::Available,
            compact_at: DEFAULT_CHECKPOINT_BYTES,
            rotation_due: None,
            #[cfg(feature = "log-write-points")]
            write_hook: None,
            encode_buf: Vec::new(),
        })
    }

    /// Observes the write stages of frames appended while the hook is installed.
    #[cfg(feature = "log-write-points")]
    pub fn set_write_hook(&mut self, hook: Option<Box<dyn FnMut(LogWritePoint) + Send>>) {
        self.write_hook = hook;
    }

    /// Configures the automatic checkpoint compaction threshold in bytes.
    pub fn set_checkpoint_bytes(&mut self, bytes: u64) {
        self.checkpoint_bytes = bytes.max(WAL_HEADER_LEN);
        self.compact_at = self.checkpoint_bytes;
    }

    /// Current `.mlog` byte length on disk.
    #[must_use]
    pub fn wal_len(&self) -> u64 {
        self.wal_len
    }

    /// SQLite settings from initial checkpoint load, if loaded from an existing file.
    #[must_use]
    pub fn sqlite_settings(&self) -> Option<&Settings> {
        self.sqlite_settings.as_ref()
    }

    /// Sets whether the medium accepts durable writes (used by conformance and
    /// fault-injection tests).
    pub fn set_durability(&mut self, durability: Durability) {
        self.durability = durability;
    }

    /// Bounded attachment barrier, O(log N) via `VolatileStore::has_will_for_client`.
    pub fn has_will_for_client(&self, client: &[u8]) -> Result<bool, ReadError> {
        self.store.has_will_for_client(client)
    }

    /// Borrow the underlying in-memory [`VolatileStore`].
    #[must_use]
    #[inline]
    pub fn store(&self) -> &VolatileStore {
        &self.store
    }

    /// Atomically checkpoints the full state into the Schema-14 / Schema-15
    /// SQLite database at `<path>` and rotates `<path>.mlog` to an empty log
    /// rooted at the new revision.
    pub fn compact(&mut self) -> Result<(), OpenError> {
        let state = self.store.snapshot().map_err(OpenError::Read)?;
        save_state_atomic(&self.path, &state, self.policy.as_ref(), &self.lease)?;
        self.rotation_due = Some((state.ledger.incarnation(), state.revision));
        self.rotate()
    }

    /// Replaces the log by an empty one rooted at the checkpoint that made
    /// it stale, if there is one. The state has not moved since that
    /// checkpoint: no write is applied before its frame is in the log.
    fn rotate(&mut self) -> Result<(), OpenError> {
        let Some((incarnation, revision)) = self.rotation_due else {
            return Ok(());
        };
        debug_assert_eq!(self.store.revision, revision);
        self.wal =
            write_fresh_wal_atomic(&self.wal_path, incarnation, revision, self.policy.is_some())?;
        self.wal_len = WAL_HEADER_LEN;
        self.wal_alloc_len = WAL_HEADER_LEN;
        self.compact_at = self.checkpoint_bytes;
        self.rotation_due = None;
        Ok(())
    }

    fn ensure_preallocated(&mut self, needed_end: u64) -> std::io::Result<()> {
        let required = needed_end.saturating_add(4);
        if required <= self.wal_alloc_len {
            return Ok(());
        }
        let chunk = self
            .wal_alloc_len
            .clamp(MIN_PREALLOC_CHUNK, MAX_PREALLOC_CHUNK);
        let cap = self.checkpoint_bytes.max(required);
        // The file is longer than its allocated length only when a cut
        // failed: the bytes it left are zero-filled with the rest.
        let target = required
            .max(self.wal_alloc_len.saturating_add(chunk).min(cap))
            .max(self.wal.metadata()?.len());
        self.wal.seek(SeekFrom::Start(self.wal_alloc_len))?;
        let zeros = [0u8; 65_536];
        let mut written = self.wal_alloc_len;
        while written < target {
            let n = usize::try_from((target - written).min(zeros.len() as u64)).unwrap_or(65_536);
            self.wal.write_all(&zeros[..n])?;
            written += n as u64;
        }
        self.wal.sync_all()?;
        self.wal_alloc_len = target;
        Ok(())
    }

    fn maybe_compact(&mut self) {
        if self.wal_len >= self.compact_at && self.compact().is_err() {
            // The commit that brought the log here is synchronised and stays
            // valid. An attempt writes the whole state: the next one waits
            // for as much log again.
            self.compact_at = self.wal_len.saturating_add(self.checkpoint_bytes);
        }
    }

    fn prepare_frame(&mut self, record_count: u16, write_body: impl FnOnce(&mut Vec<u8>)) {
        self.encode_buf.clear();
        self.encode_buf.extend_from_slice(&0u32.to_be_bytes());
        self.encode_buf
            .extend_from_slice(&record_count.to_be_bytes());
        write_body(&mut self.encode_buf);
        let body_len = self.encode_buf.len() - 4;
        let len_u32 = u32::try_from(body_len).expect("frame fits in u32");
        self.encode_buf[0..4].copy_from_slice(&len_u32.to_be_bytes());
        let crc = crc32c(&self.encode_buf);
        self.encode_buf.extend_from_slice(&crc.to_be_bytes());
    }

    /// Whether recovery accepts the prepared frame: `replay_wal` reads no
    /// body longer than `MAX_FRAME_BYTES`.
    fn prepared_frame_is_readable(&self) -> bool {
        self.encode_buf.len() - 8 <= MAX_FRAME_BYTES
    }

    /// Appends the prepared frame to `.mlog` and calls `sync_data()`.
    /// On any I/O failure (or `Durability::Failing`), cuts `.mlog` back to
    /// `prev_len` and returns `Err(())`. A frame recovery would refuse is
    /// never written, and nothing is written to a log a checkpoint made
    /// stale until that log is replaced.
    fn flush_prepared_frame(&mut self) -> Result<u64, ()> {
        if self.durability == Durability::Failing
            || !self.prepared_frame_is_readable()
            || self.rotate().is_err()
        {
            return Err(());
        }
        let prev_len = self.wal_len;
        let frame_len = self.encode_buf.len() as u64;
        let needed_end = prev_len.saturating_add(frame_len);
        #[cfg(feature = "log-write-points")]
        if let Some(hook) = self.write_hook.as_mut() {
            hook(LogWritePoint::BeforeAppend);
        }
        let write_ok = self
            .ensure_preallocated(needed_end)
            .and_then(|()| self.wal.seek(SeekFrom::Start(prev_len)))
            .and_then(|_| self.wal.write_all(&self.encode_buf))
            .and_then(|()| {
                #[cfg(feature = "log-write-points")]
                if let Some(hook) = self.write_hook.as_mut() {
                    hook(LogWritePoint::BeforeSync);
                }
                self.wal.sync_data()
            });
        #[cfg(feature = "log-write-points")]
        if write_ok.is_ok()
            && let Some(hook) = self.write_hook.as_mut()
        {
            hook(LogWritePoint::AfterSync);
        }
        if self.encode_buf.capacity() > ENCODE_BUF_SHRINK_THRESHOLD {
            self.encode_buf = Vec::new();
        } else {
            self.encode_buf.clear();
        }
        match write_ok {
            Ok(()) => {
                self.wal_len = needed_end;
                Ok(prev_len)
            }
            Err(_) => {
                self.rollback_wal_to(prev_len);
                Err(())
            }
        }
    }

    /// Ends the log at `prev_len`. The cut of the file is an attempt: what
    /// it leaves is beyond the allocated length, and `ensure_preallocated`
    /// zero-fills it before another frame is written.
    fn rollback_wal_to(&mut self, prev_len: u64) {
        let _ = self
            .wal
            .set_len(prev_len)
            .and_then(|()| self.wal.sync_all());
        self.wal_len = prev_len;
        self.wal_alloc_len = prev_len;
    }

    /// Applies up to [`LOG_MAX_GROUP`] commands in one `fdatasync`ed WAL frame.
    ///
    /// Exact parity with `SqliteStore::apply_group`: read-only groups execute
    /// without disk I/O (even under `Durability::Failing`); groups that need a
    /// write either commit durably as one atomic frame or leave state untouched
    /// and answer `PendingOrUnknown` for every member.
    ///
    /// One exception: a group whose frame would exceed `MAX_FRAME_BYTES` is
    /// committed as two groups, the first half before the second.
    pub fn apply_group(&mut self, commands: &[&Command]) -> Result<Vec<Applied>, GroupRefused> {
        if commands.len() > LOG_MAX_GROUP {
            return Err(GroupRefused::TooLarge {
                commands: commands.len(),
                max: LOG_MAX_GROUP,
            });
        }
        match commands {
            [] => return Ok(Vec::new()),
            [only] => return Ok(vec![self.apply(only)]),
            _ => {}
        }
        let mut to_log: Vec<&Command> = Vec::new();
        for &cmd in commands {
            if !self.store.is_command_already_settled(cmd) {
                to_log.push(cmd);
            }
        }
        if to_log.is_empty() {
            return Ok(commands.iter().map(|c| self.store.apply(c)).collect());
        }
        if self.durability == Durability::Failing {
            return Ok(vec![UNKNOWN; commands.len()]);
        }
        let count = u16::try_from(to_log.len()).expect("LOG_MAX_GROUP fits in u16");
        self.prepare_frame(count, |buf| {
            for cmd in &to_log {
                encode_command_record(buf, cmd);
            }
        });
        if !self.prepared_frame_is_readable() {
            // The group is committed as two, in order, each half a frame
            // of its own: what the caller would have seen had it presented
            // two groups. A half reduced to one command that still does
            // not fit is answered unknown by `apply`.
            self.encode_buf.clear();
            drop(to_log);
            let (first, second) = commands.split_at(commands.len() / 2);
            let mut replies = self.apply_group(first)?;
            replies.extend(self.apply_group(second)?);
            return Ok(replies);
        }
        let Ok(prev_len) = self.flush_prepared_frame() else {
            return Ok(vec![UNKNOWN; commands.len()]);
        };
        let mut any_wrote = false;
        let replies: Vec<Applied> = commands
            .iter()
            .map(|c| {
                let res = self.store.apply(c);
                any_wrote |= res.wrote;
                res
            })
            .collect();
        if !any_wrote {
            self.rollback_wal_to(prev_len);
        } else {
            self.maybe_compact();
        }
        Ok(replies)
    }
}

impl DurableStore for LogStore {
    fn read_wills(
        &self,
        after: Option<OperationId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WillPage, ReadError> {
        self.store.read_wills(after, limit, budget)
    }

    fn expire_messages(
        &mut self,
        now: MessageTime,
        after: Option<PublicationId>,
        limit: u32,
    ) -> Result<MessageExpirySweep, ReadError> {
        // Dry-run with Failing durability executes all validation and returns
        // Ok immediately if nothing needs expiration.
        self.store.durability = Durability::Failing;
        let probe = self.store.expire_messages(now, after, limit);
        self.store.durability = Durability::Available;
        match probe {
            Ok(sweep) => return Ok(sweep),
            Err(ReadError::Unavailable) => {}
            Err(err) => return Err(err),
        }
        if self.durability == Durability::Failing {
            return Err(ReadError::Unavailable);
        }
        self.prepare_frame(1, |buf| {
            encode_expire_messages_record(buf, now, after, limit);
        });
        let Ok(prev_len) = self.flush_prepared_frame() else {
            return Err(ReadError::Io);
        };
        let rev_before = self.store.revision;
        let res = self.store.expire_messages(now, after, limit);
        if self.store.revision == rev_before {
            self.rollback_wal_to(prev_len);
        } else {
            self.maybe_compact();
        }
        res
    }

    fn checkpoint_session_clock(
        &mut self,
        expected: Option<SessionClockAnchor>,
        reading: SessionClockReading,
    ) -> Result<SessionClockAnchor, ReadError> {
        if self.store.session_clock != expected {
            return self.store.session_clock.ok_or(ReadError::InvalidData);
        }
        let _ = SessionClockAnchor::advance(self.store.session_clock, reading)?;
        if self.durability == Durability::Failing {
            return Err(ReadError::Unavailable);
        }
        self.prepare_frame(1, |buf| {
            encode_checkpoint_clock_record(buf, expected, reading);
        });
        let Ok(prev_len) = self.flush_prepared_frame() else {
            return Err(ReadError::Io);
        };
        let res = self.store.checkpoint_session_clock(expected, reading);
        if res.is_err() {
            self.rollback_wal_to(prev_len);
        } else {
            self.maybe_compact();
        }
        res
    }

    fn collect_receipts(
        &mut self,
        collection: ReceiptCollection,
    ) -> Result<ReceiptCollectionReply, ReadError> {
        self.store.durability = Durability::Failing;
        let probe = self.store.collect_receipts(collection);
        self.store.durability = Durability::Available;
        match probe {
            Ok(ReceiptCollectionReply::PendingOrUnknown) => {}
            other => return other,
        }
        if self.durability == Durability::Failing {
            return Ok(ReceiptCollectionReply::PendingOrUnknown);
        }
        self.prepare_frame(1, |buf| {
            encode_collect_receipts_record(buf, collection);
        });
        let Ok(prev_len) = self.flush_prepared_frame() else {
            return Ok(ReceiptCollectionReply::PendingOrUnknown);
        };
        let rev_before = self.store.revision;
        let res = self.store.collect_receipts(collection);
        if self.store.revision == rev_before {
            self.rollback_wal_to(prev_len);
        } else {
            self.maybe_compact();
        }
        res
    }

    fn receipt_frontier(&self, origin: ReceiptOrigin) -> Result<Option<ReceiptClosure>, ReadError> {
        self.store.receipt_frontier(origin)
    }

    fn collect_closed_receipts(
        &mut self,
        collection: ClosedReceiptCollection,
    ) -> Result<ClosedReceiptReply, ReadError> {
        self.store.durability = Durability::Failing;
        let probe = self.store.collect_closed_receipts(collection);
        self.store.durability = Durability::Available;
        match probe {
            Ok(ClosedReceiptReply::PendingOrUnknown) => {}
            other => return other,
        }
        if self.durability == Durability::Failing {
            return Ok(ClosedReceiptReply::PendingOrUnknown);
        }
        self.prepare_frame(1, |buf| {
            encode_collect_closed_receipts_record(buf, collection);
        });
        let Ok(prev_len) = self.flush_prepared_frame() else {
            return Ok(ClosedReceiptReply::PendingOrUnknown);
        };
        let rev_before = self.store.revision;
        let res = self.store.collect_closed_receipts(collection);
        if self.store.revision == rev_before {
            self.rollback_wal_to(prev_len);
        } else {
            self.maybe_compact();
        }
        res
    }

    fn ledger(&self) -> Result<EpochLedger, ReadError> {
        self.store.ledger()
    }

    fn open_epoch(&mut self, command: &OpenEpoch) -> Result<OpeningOutcome, ReadError> {
        if !matches!(self.store.ledger.open(command), LedgerOpening::Opens { .. }) {
            return self.store.open_epoch(command);
        }
        self.store.durability = Durability::Failing;
        let probe = self.store.open_epoch(command);
        self.store.durability = Durability::Available;
        let probe_outcome = probe?;
        if self.durability == Durability::Failing {
            return Ok(probe_outcome);
        }
        self.prepare_frame(1, |buf| {
            encode_open_epoch_record(buf, command);
        });
        let Ok(prev_len) = self.flush_prepared_frame() else {
            return Ok(OpeningOutcome::read(&self.store.ledger, command));
        };
        let rev_before = self.store.revision;
        let res = self.store.open_epoch(command);
        if self.store.revision == rev_before {
            self.rollback_wal_to(prev_len);
        } else {
            self.maybe_compact();
        }
        res
    }

    fn apply(&mut self, command: &Command) -> Applied {
        if self.store.is_command_already_settled(command) {
            return self.store.apply(command);
        }
        if self.durability == Durability::Failing {
            return UNKNOWN;
        }
        self.prepare_frame(1, |buf| {
            encode_command_record(buf, command);
        });
        let Ok(prev_len) = self.flush_prepared_frame() else {
            return UNKNOWN;
        };
        let res = self.store.apply(command);
        if !res.wrote {
            self.rollback_wal_to(prev_len);
        } else {
            self.maybe_compact();
        }
        res
    }

    fn established(&self, operation: OperationId) -> Result<Option<StoreOutcome>, ReadError> {
        self.store.established(operation)
    }

    fn resolve_and_fence(&mut self, operation: OperationId) -> Resolved {
        self.store.durability = Durability::Failing;
        let probe = self.store.resolve_and_fence(operation);
        self.store.durability = Durability::Available;
        if !matches!(probe.outcome, StoreOutcome::PendingOrUnknown) {
            return probe;
        }
        // Distinguish "already lacking receipt capacity/budget" from Failing durability.
        if !self.store.can_fence(operation) {
            return probe;
        }
        if self.durability == Durability::Failing {
            return probe;
        }
        self.prepare_frame(1, |buf| {
            encode_resolve_and_fence_record(buf, operation);
        });
        let Ok(prev_len) = self.flush_prepared_frame() else {
            return Resolved {
                outcome: StoreOutcome::PendingOrUnknown,
                fenced_now: false,
            };
        };
        let res = self.store.resolve_and_fence(operation);
        if !res.fenced_now {
            self.rollback_wal_to(prev_len);
        } else {
            self.maybe_compact();
        }
        res
    }

    fn admin(&mut self, command: AdminCommand) -> Result<AdminDone, AdminRefused> {
        self.store.durability = Durability::Failing;
        let probe = self.store.admin(command.clone());
        self.store.durability = Durability::Available;
        match probe {
            Err(AdminRefused::WriteFailed) => {}
            other => return other,
        }
        if self.durability == Durability::Failing {
            return Err(AdminRefused::WriteFailed);
        }
        self.prepare_frame(1, |buf| {
            encode_admin_record(buf, &command);
        });
        let Ok(prev_len) = self.flush_prepared_frame() else {
            return Err(AdminRefused::WriteFailed);
        };
        let res = self.store.admin(command);
        if res.is_err() {
            self.rollback_wal_to(prev_len);
        } else {
            self.maybe_compact();
        }
        res
    }

    fn recover_with_budget(
        &self,
        cursor: Option<RecoveryCursor>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<RecoveryPage, RecoveryRefused> {
        self.store.recover_with_budget(cursor, limit, budget)
    }

    fn read_work_with_budget(
        &self,
        session: SessionId,
        after: Option<DeliveryId>,
        limit: usize,
        budget: ReadBudget,
    ) -> Result<WorkPage, ReadError> {
        self.store
            .read_work_with_budget(session, after, limit, budget)
    }

    fn snapshot_with_budget(&self, budget: ReadBudget) -> Result<DurableState, ReadError> {
        self.store.snapshot_with_budget(budget)
    }
}

fn encode_command_record(buf: &mut Vec<u8>, cmd: &Command) {
    buf.push(TAG_COMMAND);
    let len_pos = buf.len();
    buf.extend_from_slice(&0u32.to_be_bytes());
    let recipe_start = buf.len();
    cmd.recipe.encode_canonical_into(buf);
    let rlen = u32::try_from(buf.len() - recipe_start).expect("recipe fits in u32");
    buf[len_pos..len_pos + 4].copy_from_slice(&rlen.to_be_bytes());
    match &cmd.payload {
        None => buf.push(0),
        Some(payload) => {
            buf.push(1);
            let plen = u32::try_from(payload.len()).expect("payload fits in u32");
            buf.extend_from_slice(&plen.to_be_bytes());
            buf.extend_from_slice(payload.as_slice());
        }
    }
}

fn encode_open_epoch_record(buf: &mut Vec<u8>, cmd: &OpenEpoch) {
    buf.push(TAG_OPEN_EPOCH);
    buf.extend_from_slice(&cmd.attempt.to_be_bytes());
    buf.extend_from_slice(&cmd.incarnation.get().to_be_bytes());
    buf.extend_from_slice(&cmd.after.get().to_be_bytes());
}

fn encode_resolve_and_fence_record(buf: &mut Vec<u8>, op: OperationId) {
    buf.push(TAG_RESOLVE_AND_FENCE);
    buf.extend_from_slice(&op.to_be_bytes());
}

fn encode_collect_receipts_record(buf: &mut Vec<u8>, collection: ReceiptCollection) {
    buf.push(TAG_COLLECT_RECEIPTS);
    let c = collection.closure;
    buf.extend_from_slice(&c.origin.incarnation.get().to_be_bytes());
    buf.extend_from_slice(&c.origin.epoch.get().to_be_bytes());
    buf.extend_from_slice(&c.origin.owner.get().to_be_bytes());
    buf.extend_from_slice(&c.through.get().to_be_bytes());
    buf.extend_from_slice(&c.publication_through.get().to_be_bytes());
    buf.extend_from_slice(&c.payload_through.get().to_be_bytes());
    buf.extend_from_slice(&collection.limit.to_be_bytes());
}

fn encode_collect_closed_receipts_record(buf: &mut Vec<u8>, collection: ClosedReceiptCollection) {
    buf.push(TAG_COLLECT_CLOSED_RECEIPTS);
    buf.extend_from_slice(&collection.through.get().to_be_bytes());
    buf.extend_from_slice(&collection.limit.to_be_bytes());
}

fn encode_checkpoint_clock_record(
    buf: &mut Vec<u8>,
    expected: Option<SessionClockAnchor>,
    reading: SessionClockReading,
) {
    buf.push(TAG_CHECKPOINT_CLOCK);
    match expected {
        None => buf.push(0),
        Some(anchor) => {
            buf.push(1);
            buf.extend_from_slice(&anchor.reading.domain);
            buf.extend_from_slice(&anchor.reading.tick.to_be_bytes());
            buf.extend_from_slice(&anchor.logical.to_be_bytes());
        }
    }
    buf.extend_from_slice(&reading.domain);
    buf.extend_from_slice(&reading.tick.to_be_bytes());
}

fn encode_expire_messages_record(
    buf: &mut Vec<u8>,
    now: MessageTime,
    after: Option<PublicationId>,
    limit: u32,
) {
    buf.push(TAG_EXPIRE_MESSAGES);
    buf.extend_from_slice(&now.domain);
    buf.extend_from_slice(&now.tick.to_be_bytes());
    match after {
        None => buf.push(0),
        Some(pub_id) => {
            buf.push(1);
            buf.extend_from_slice(&pub_id.to_be_bytes());
        }
    }
    buf.extend_from_slice(&limit.to_be_bytes());
}

fn encode_admin_record(buf: &mut Vec<u8>, command: &AdminCommand) {
    buf.push(TAG_ADMIN);
    match command {
        AdminCommand::CreateSession { client_id } => {
            buf.push(ADMIN_CREATE_SESSION);
            let len = u32::try_from(client_id.len()).expect("client_id fits in u32");
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(client_id);
        }
        AdminCommand::AddRoute {
            topic,
            session,
            qos,
        } => {
            buf.push(ADMIN_ADD_ROUTE);
            let len = u32::try_from(topic.len()).expect("topic fits in u32");
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(topic);
            buf.extend_from_slice(&session.get().to_be_bytes());
            buf.push(qos.to_wire());
        }
        AdminCommand::DeleteSession { session } => {
            buf.push(ADMIN_DELETE_SESSION);
            buf.extend_from_slice(&session.get().to_be_bytes());
        }
        AdminCommand::CollectClosedEpochs { through } => {
            buf.push(ADMIN_COLLECT_CLOSED_EPOCHS);
            buf.extend_from_slice(&through.get().to_be_bytes());
        }
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], OpenError> {
        let end = self.pos.checked_add(n).ok_or(OpenError::Corrupt)?;
        if end > self.bytes.len() {
            return Err(OpenError::Corrupt);
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn take_u8(&mut self) -> Result<u8, OpenError> {
        Ok(self.take(1)?[0])
    }

    fn take_u16(&mut self) -> Result<u16, OpenError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn take_u32(&mut self) -> Result<u32, OpenError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn take_u64(&mut self) -> Result<u64, OpenError> {
        let b = self.take(8)?;
        Ok(u64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn take_32(&mut self) -> Result<[u8; 32], OpenError> {
        let mut out = [0u8; 32];
        out.copy_from_slice(self.take(32)?);
        Ok(out)
    }
}

/// Whether the file holds only zeros from `from` to `file_len`.
fn remaining_is_all_zero(file: &mut File, from: u64, file_len: u64) -> Result<bool, OpenError> {
    if from >= file_len {
        return Ok(true);
    }
    file.seek(SeekFrom::Start(from))
        .map_err(OpenError::WriterLock)?;
    let mut buf = [0u8; 16_384];
    let mut left = file_len - from;
    while left > 0 {
        let want = usize::try_from(left.min(buf.len() as u64)).unwrap_or(16_384);
        file.read_exact(&mut buf[..want])
            .map_err(OpenError::WriterLock)?;
        if buf[..want].iter().any(|&b| b != 0) {
            return Ok(false);
        }
        left -= want as u64;
    }
    Ok(true)
}

fn replay_wal(
    file: &mut File,
    file_len: u64,
    store: &mut VolatileStore,
) -> Result<(u64, bool), OpenError> {
    let mut pos = WAL_HEADER_LEN;
    let mut frame_buf = Vec::new();
    while pos < file_len {
        let remaining = file_len - pos;
        if remaining < 4 {
            let clean = remaining_is_all_zero(file, pos, file_len)?;
            return Ok((pos, clean));
        }
        let mut len_bytes = [0u8; 4];
        file.read_exact(&mut len_bytes)
            .map_err(OpenError::WriterLock)?;
        let body_len = u32::from_be_bytes(len_bytes) as usize;
        if body_len == 0 {
            // The zero-filled tail of the allocation.
            if remaining_is_all_zero(file, pos + 4, file_len)? {
                return Ok((pos, true));
            }
            // Or a frame cut by a power loss that kept later sectors and
            // lost its first. That sector then holds what it held before,
            // zeros from here to its end, and what was kept lies within
            // the reach of one frame. Anything else is damage.
            let sector_end = (pos / SECTOR_BYTES + 1) * SECTOR_BYTES;
            let frame_end = pos + 8 + MAX_FRAME_BYTES as u64;
            if remaining_is_all_zero(file, pos + 4, sector_end.min(file_len))?
                && remaining_is_all_zero(file, frame_end, file_len)?
            {
                return Ok((pos, false));
            }
            return Err(OpenError::Corrupt);
        }
        if remaining < 10 {
            return Ok((pos, false));
        }
        if !(2..=MAX_FRAME_BYTES).contains(&body_len) {
            if remaining <= 4096 {
                return Ok((pos, false));
            }
            return Err(OpenError::Corrupt);
        }
        let total_frame = 4u64
            .checked_add(body_len as u64)
            .and_then(|n| n.checked_add(4))
            .ok_or(OpenError::Corrupt)?;
        if total_frame > remaining {
            // Torn tail frame at EOF before fdatasync completed: truncate here.
            return Ok((pos, false));
        }
        frame_buf.clear();
        frame_buf
            .try_reserve_exact(4 + body_len)
            .map_err(|_| OpenError::Read(ReadError::AllocationFailed))?;
        frame_buf.extend_from_slice(&len_bytes);
        frame_buf.resize(4 + body_len, 0);
        file.read_exact(&mut frame_buf[4..])
            .map_err(OpenError::WriterLock)?;
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes)
            .map_err(OpenError::WriterLock)?;
        let expected_crc = u32::from_be_bytes(crc_bytes);
        let actual_crc = crc32c(&frame_buf);
        if actual_crc != expected_crc {
            if pos + total_frame == file_len
                || remaining_is_all_zero(file, pos + total_frame, file_len)?
            {
                // Corrupt final frame at EOF / pre-allocated tail (torn write before fdatasync).
                return Ok((pos, false));
            }
            // Corruption before non-zero frames means an earlier committed frame was damaged.
            return Err(OpenError::Corrupt);
        }
        replay_frame_body(&frame_buf[4..], store)?;
        pos += total_frame;
    }
    Ok((pos, true))
}

fn replay_frame_body(body: &[u8], store: &mut VolatileStore) -> Result<(), OpenError> {
    let mut cur = Cursor::new(body);
    let count = cur.take_u16()?;
    let limits = store.limits;
    for _ in 0..count {
        let tag = cur.take_u8()?;
        match tag {
            TAG_COMMAND => {
                let rlen = cur.take_u32()? as usize;
                let rbytes = cur.take(rlen)?;
                let recipe = Recipe::decode_canonical(
                    rbytes,
                    limits.max_targets_per_admission,
                    limits.max_topic_bytes,
                )
                .map_err(|_| OpenError::Corrupt)?;
                let frozen = FrozenRecipe::freeze(recipe).map_err(|_| OpenError::Corrupt)?;
                let has_payload = cur.take_u8()?;
                let payload = match has_payload {
                    0 => None,
                    1 => {
                        let plen = cur.take_u32()? as usize;
                        let pbytes = cur.take(plen)?;
                        Some(
                            SharedBytes::copy_from(pbytes, limits.max_payload_bytes)
                                .map_err(|_| OpenError::Corrupt)?,
                        )
                    }
                    _ => return Err(OpenError::Corrupt),
                };
                let cmd = Command {
                    recipe: frozen,
                    payload,
                };
                let applied = store.apply(&cmd);
                if matches!(applied.reply, StoreReply::Violation(_)) {
                    return Err(OpenError::Corrupt);
                }
            }
            TAG_OPEN_EPOCH => {
                let attempt =
                    OpeningAttempt::from_be_slice(cur.take(24)?).map_err(|_| OpenError::Corrupt)?;
                let incarnation = StoreIncarnation::new(cur.take_u64()?);
                let after = RunEpoch::new(cur.take_u64()?);
                let cmd = OpenEpoch {
                    attempt,
                    incarnation,
                    after,
                };
                store.open_epoch(&cmd).map_err(OpenError::Read)?;
            }
            TAG_RESOLVE_AND_FENCE => {
                let op =
                    OperationId::from_be_slice(cur.take(32)?).map_err(|_| OpenError::Corrupt)?;
                let _ = store.resolve_and_fence(op);
            }
            TAG_COLLECT_RECEIPTS => {
                let origin = ReceiptOrigin {
                    incarnation: StoreIncarnation::new(cur.take_u64()?),
                    epoch: RunEpoch::new(cur.take_u64()?),
                    owner: LogicalOwnerId::new(cur.take_u64()?),
                };
                let closure = ReceiptClosure {
                    origin,
                    through: OperationSeq::new(cur.take_u64()?),
                    publication_through: PublicationSeq::new(cur.take_u64()?),
                    payload_through: PayloadSeq::new(cur.take_u64()?),
                };
                let limit = cur.take_u16()?;
                store
                    .collect_receipts(ReceiptCollection { closure, limit })
                    .map_err(OpenError::Read)?;
            }
            TAG_COLLECT_CLOSED_RECEIPTS => {
                let through = RunEpoch::new(cur.take_u64()?);
                let limit = cur.take_u16()?;
                store
                    .collect_closed_receipts(ClosedReceiptCollection { through, limit })
                    .map_err(OpenError::Read)?;
            }
            TAG_CHECKPOINT_CLOCK => {
                let has_expected = cur.take_u8()?;
                let expected = match has_expected {
                    0 => None,
                    1 => {
                        let domain = cur.take_32()?;
                        let tick = cur.take_u64()?;
                        let logical = cur.take_u64()?;
                        Some(SessionClockAnchor {
                            reading: SessionClockReading { domain, tick },
                            logical,
                        })
                    }
                    _ => return Err(OpenError::Corrupt),
                };
                let domain = cur.take_32()?;
                let tick = cur.take_u64()?;
                store
                    .checkpoint_session_clock(expected, SessionClockReading { domain, tick })
                    .map_err(OpenError::Read)?;
            }
            TAG_EXPIRE_MESSAGES => {
                let domain = cur.take_32()?;
                let tick = cur.take_u64()?;
                let has_after = cur.take_u8()?;
                let after = match has_after {
                    0 => None,
                    1 => Some(
                        PublicationId::from_be_slice(cur.take(32)?)
                            .map_err(|_| OpenError::Corrupt)?,
                    ),
                    _ => return Err(OpenError::Corrupt),
                };
                let limit = cur.take_u32()?;
                store
                    .expire_messages(MessageTime { domain, tick }, after, limit)
                    .map_err(OpenError::Read)?;
            }
            TAG_ADMIN => {
                let subtag = cur.take_u8()?;
                let cmd = match subtag {
                    ADMIN_CREATE_SESSION => {
                        let len = cur.take_u32()? as usize;
                        let client_id = cur.take(len)?.to_vec();
                        AdminCommand::CreateSession { client_id }
                    }
                    ADMIN_ADD_ROUTE => {
                        let len = cur.take_u32()? as usize;
                        let topic = cur.take(len)?.to_vec();
                        let session = SessionId::new(cur.take_u64()?);
                        let qos = match cur.take_u8()? {
                            0 => Qos::AtMostOnce,
                            1 => Qos::AtLeastOnce,
                            2 => Qos::ExactlyOnce,
                            _ => return Err(OpenError::Corrupt),
                        };
                        AdminCommand::AddRoute {
                            topic,
                            session,
                            qos,
                        }
                    }
                    ADMIN_DELETE_SESSION => {
                        let session = SessionId::new(cur.take_u64()?);
                        AdminCommand::DeleteSession { session }
                    }
                    ADMIN_COLLECT_CLOSED_EPOCHS => {
                        let through = RunEpoch::new(cur.take_u64()?);
                        AdminCommand::CollectClosedEpochs { through }
                    }
                    _ => return Err(OpenError::Corrupt),
                };
                let _ = store.admin(cmd);
            }
            _ => return Err(OpenError::Corrupt),
        }
    }
    if cur.pos != body.len() {
        return Err(OpenError::Corrupt);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn crc32c_matches_iscsi_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0x0000_0000);
    }
}
