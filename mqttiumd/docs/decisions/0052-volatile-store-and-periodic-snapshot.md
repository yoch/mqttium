# 0052 — Indexed volatile store with optional periodic atomic SQLite snapshots

Status: **Accepted** (27 November 2026, by the repository owner after the validation of `9dfd568`, report 0043; proposed on 22 November 2026). Applied to `mqttiumd-store-volatile`, `mqttiumd-store-sqlite`, and `mqttiumd`.

Amended by 0055: a third storage mode, `log`, with the guarantee `per_commit_fdatasync_append_log`; `--storage-mode` takes `durable|log|volatile`.

## Context

In durable mode, every QoS 1/2 commit group pays a full SQLite WAL `synchronous=FULL` fsync before acknowledging the publisher. On single-disk and embedded ARM64 storage (microSD / eMMC / NVMe), this caps durable QoS 1 throughput at disk sync latency regardless of CPU headroom, whereas competing brokers such as Mosquitto default to in-memory state with optional periodic persistence (`autosave_interval`).

`ARC-020` provides for an explicit volatile store profile while `DUR-001` and `DUR-002` require that any durability trade-off be explicitly selected by the operator, explicitly reported at readiness, and never used as a silent fallback for durable mode.

## Decision

1. **New workspace crate `mqttiumd-store-volatile` (`#![forbid(unsafe_code)]`)**:
   - `VolatileStore` implements the complete `DurableStore` trait plus `apply_group` (same `MAX_GROUP = 64` bound and `GroupRefused` contract) and `has_will_for_client`.
   - Every mutating command executes **check-then-mutate** directly in memory with zero full-state cloning: all guards, capacity arithmetic, and preflight checks finish before mutating any table, preserving atomic all-or-nothing admission (`ADM-002`) and exact parity with `MemoryStore` and `SqliteStore`.
   - Secondary `O(1)` / `O(log N)` indices (`active_by_client`, `routes_by_session`, `obligations_by_target`, `obligations_by_pub`, `inbound_by_pub`, `inbound_by_session`, `outbound_by_target_pi`, `publications_by_payload`, `retained_by_pub`, `wills_by_client`, `IntervalSet`-backed per-origin `OriginWitness`, and `open_will_receipt_bytes`) eliminate linear scans on hot paths, and `SharedBytes` blobs make `read_work` zero-copy.
2. **Atomic Schema-14 / Schema-15 SQLite snapshots (`VolatileHandle`)**:
   - `mqttiumd-store-sqlite` exposes `WriterLease`, `acquire_writer_lock`, `SqliteStore::into_state_and_lease` (closing SQLite while retaining the exclusive `.writer-lock` with zero lock gap), and `save_state_atomic`.
   - `save_state_atomic` writes a standalone `journal_mode=DELETE` Schema-14 (or Schema-15 when secured by a `StoragePolicy`) database to a sibling temporary file (preserving existing target file permissions and refusing symlinks), fsyncs the file descriptor, removes any stale `-wal`/`-shm` sidecars, atomically renames over the target path, and verifies `fsync` of the parent directory.
   - Pure in-memory mode (`--storage-mode volatile` without `--snapshot-interval` or `--existing-db`, or `persistence false`) uses `VolatileHandle::in_memory` and never opens, reads, locks, or overwrites any database file on disk.
   - Any failure of the final shutdown snapshot emits `volatile_snapshot_failed` and propagates as a non-zero broker exit code while leaving the previous snapshot intact.
   - Any snapshot produced by `VolatileHandle` can be inspected live via `SqliteStore::inspect` and reopened seamlessly by either `SqliteStore::open` / `open_secured` (durable mode) or `VolatileHandle::open_or_create` (volatile mode).
3. **Explicit CLI & readiness reporting (`DUR-001`, `DUR-002`)**:
   - `--storage-mode durable|volatile` defaults to `durable`; the broker never falls back implicitly to `volatile`.
   - `--snapshot-interval SECS` (0..=86400) is accepted only when `--storage-mode volatile` (`0` = pure RAM, `> 0` = periodic atomic snapshot every `SECS` seconds plus final atomic snapshot on clean shutdown).
   - The `ready` event and startup readiness line explicitly declare `storage_mode`, `durability_guarantee` (`per_commit_fsync_wal_full` | `periodic_atomic_snapshot` | `none_in_memory_only`), and `snapshot_interval_secs`.

## Verification

- `crates/mqttiumd-store-volatile/tests/conformance.rs` runs the full `mqttiumd_lab::store::conformance`, `retained_conformance`, and `retained_conformance::expiry` suites on `VolatileStore`, verifies differential parity of `apply_group` against both `MemoryStore` and `SqliteStore`, and verifies live inspection, writer lock exclusivity, and Schema-14 / Schema-15 snapshot round-trips through `SqliteStore::open` / `open_secured`.
- `crates/mqttiumd/tests/volatile_mode.rs` tests live broker startup in volatile mode, periodic/shutdown snapshot creation, and clean restart into both durable and volatile modes.
