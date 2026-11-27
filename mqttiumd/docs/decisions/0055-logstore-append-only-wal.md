# 0055 — Append-only durable WAL store (`LogStore`, `--storage-mode log`)

Status: **Accepted with amendments** (27 November 2026, by the repository owner after the validation of `9dfd568`, which corrected the text where the code contradicted it, report 0043; proposed on 23 November 2026). Applied to `mqttiumd-store-volatile`, `mqttiumd-store-sqlite`, and `mqttiumd`.

## Context

In `--storage-mode durable`, every group commit executes SQLite B-tree insertions, secondary index updates, WAL page framing, and `synchronous=FULL` fsyncs on SQLite pages. On real single-disk ARM64 storage (`ext4`), SQLite's per-command write amplification caps durable QoS 1 throughput near 2,400–3,400 msg/s regardless of CPU headroom, whereas `--storage-mode volatile` trades away per-commit crash durability for RAM throughput.

Because `mqttiumd-core` already freezes every irreversible state transition into a self-contained canonical command (`FrozenRecipe` + optional payload bytes) and every store operation is deterministic (`DEM-009`), persisting the minimal append-only stream of committed/decided operations directly to a single sequential log file with `fdatasync` per group commit preserves full per-commit crash durability while avoiding SQLite's per-command B-tree write amplification.

## Decision

1. **Append-only sequential log backend `LogStore` (`crates/mqttiumd-store-volatile/src/log.rs`)**:
   - `LogStore` pairs the indexed in-memory state machine `VolatileStore` (record 0052) with an append-only binary WAL file `<db>.mlog` alongside the Schema-14 / Schema-15 SQLite checkpoint `<db>`.
   - Exclusive writer ownership is enforced through the same `WriterLease` (`.writer-lock`) used by `SqliteStore` and `VolatileHandle`.
   - Binary `.mlog` format (`v1`):
     - 24-byte file header: magic `b"MQLG"`, version `u16` (`1`), reserved `u16` (`0`), store incarnation `u64`, checkpoint `base_revision: u64` (big-endian).
     - Each committed transaction/group appends a single framed record: `[body_len: u32][record_count: u16][records...][crc32c: u32]` written via sequential `pwrite`/`write_all` followed by a single `fdatasync` before returning outcomes to callers.
     - A frame body holds at most 64 MiB (`MAX_FRAME_BYTES`), the bound recovery applies. A group whose frame would exceed it is committed as two groups, the first half before the second, each all-or-nothing; one command that would exceed it is not written and is answered `PendingOrUnknown`. The product cannot form such a command (payloads are limited to one mebibyte).
     - Integrity checksums use Castagnoli CRC32C implemented in `#![forbid(unsafe_code)]` Rust with a compile-time Slice-by-8 lookup table (`CRC32C_TABLES`).
     - Supported record tags cover the complete `DurableStore` mutation surface: `Command` (canonical recipe + payload bytes), `ResolveAndFence`, `OpenEpoch`, `AdminCommand`, `CollectReceipts`, `CollectClosedReceipts`, and `CheckpointClock`.
2. **Crash recovery, torn-tail handling, and automatic compaction**:
   - On startup, `LogStore::open_or_create` loads the latest atomic SQLite checkpoint (Schema-14, or Schema-15 when secured by a `StoragePolicy`) at revision `R_ckpt`, then validates `<db>.mlog`.
   - If the `.mlog` header's `base_revision < R_ckpt` (crash after checkpoint rename before WAL rotation), the stale WAL is discarded and a fresh header at `R_ckpt` is created and synced.
   - If `base_revision == R_ckpt`, every complete frame with a valid CRC32C is replayed deterministically into `VolatileStore`.
   - A truncated frame or CRC mismatch at the very tail of the log (power loss mid-write, including inside pre-allocated zero-filled space) is truncated cleanly back to the last valid frame boundary (`sync_all` + parent directory `fsync`).
   - A frame is written over several sectors and a power loss may keep the later ones and lose the first. The log then shows, after its last committed frame, a zero length, zeros to the end of that 512-byte sector, and what was kept of the frame. That tail is cut like any torn frame when what was kept lies within the reach of one frame (8 bytes and `MAX_FRAME_BYTES` from the zero length): no answer was given for it (added on 26 November 2026; such a log was refused as corrupt).
   - Any CRC mismatch or framing corruption **before** the tail fails closed with `OpenError::Corrupt` (never silently skipped). One image is not told apart from a torn tail and is cut without an error: a committed frame whose first sector reads as zeros, followed only by frames within the reach of one frame. Telling it apart needs a frame format that can be read backwards; it is left open.
   - Once the `.mlog` reaches the configured compaction threshold (`checkpoint_bytes`, default 128 MiB), `LogStore` atomically writes a fresh Schema-14 / Schema-15 SQLite checkpoint via `save_state_atomic` (preserving permissions, refusing symlinks, `fsync` on file and parent directory) and resets `<db>.mlog` to a new 24-byte header at the new checkpoint revision.
   - A checkpoint that fails changes nothing: the commit that reached the threshold is synchronised and stays valid, and the next attempt waits until the log has grown by the threshold again, since each attempt writes the whole state. When the checkpoint was saved and the log could not be replaced, nothing is appended to the stale log and every write answers `PendingOrUnknown`; each write first tries the replacement again, so the store writes again as soon as the cause is gone (added on 26 November 2026; the store stayed unable to write until an explicit checkpoint).
   - A frame whose write or synchronisation fails is cut from the file. When the cut fails too, the bytes it left are zero-filled, and the fill synchronised, before another frame is written: a frame is never followed by bytes that are not its own.
   - Read-only queries (`snapshot`, `work_page`, `recovery`, `inspect`) never wait on disk writes; under `Durability::Failing`, mutating groups leave both RAM state and `.mlog` length unchanged and answer `StoreOutcome::PendingOrUnknown`, while read-only groups still answer established receipts.
3. **Explicit CLI & readiness reporting (`DUR-001`, `DUR-002`)**:
   - `--storage-mode durable|log|volatile` keeps `durable` (SQLite) as the default; `log` is strictly opt-in and never used as a silent fallback.
   - The `ready` event and startup line explicitly declare `storage_mode: "log"` and `durability_guarantee: "per_commit_fdatasync_append_log"` (corrected on 23 November 2026: this line said `per_commit_fdatasync_wal`, a string the executable never printed; record 0059 noted it).
   - Any SQLite checkpoint produced by `LogStore` can be inspected live via `SqliteStore::inspect` and reopened seamlessly by `SqliteStore::open` / `open_secured`.

## Consequences

- **Durability**: Every acknowledged QoS 1/2 publish, claim, finish, subscription change, session lifecycle transition, and receipt retirement is `fdatasync`-durable on real storage before the broker replies on the wire.
- **Performance**: On ARM64 real `ext4`, `LogStore` sustains 17,500–20,500 QoS 1 msg/s (6×–8.4× SQLite WAL FULL) with p50 latency 8–10 ms at 256 concurrent connections, and 33,000 msg/s on `tmpfs`.
- **Verification**: `crates/mqttiumd-store-volatile/tests/log_conformance.rs` runs the full `mqttiumd_lab::store` conformance suites on `LogStore`, verifies byte-for-byte snapshot parity against `SqliteStore` across `apply_group`, `Durability::Failing`, uncompacted `.mlog` crash recovery, torn-tail truncation, fail-closed mid-file corruption, stale-WAL recovery, and Schema-15 secured policy persistence. Both `durable` and `log` modes pass 126/126 differential compatibility scenarios against Mosquitto 2.0.21 (`tests/compat/run.py`) with 0 defects.
