# 0068 — LogStore `.mlog` v2 backward-readable frame footer

Status: **Proposed** (27 November 2026). Amends [decision 0055](0055-logstore-append-only-wal.md) for the `.mlog` binary frame format. Applied to `mqttiumd-store-volatile`.

## Context

[Decision 0055](0055-logstore-append-only-wal.md) defines the append-only `.mlog` format (`v1`) with a 4-byte leading `body_len` and a 4-byte trailing `crc32c`. When recovery encounters `body_len == 0` followed by non-zero bytes within one frame's reach (`8 + MAX_FRAME_BYTES`) after a zeroed first 512-byte sector, `v1` cannot distinguish two different disk images:

1. **Torn tail (uncommitted)**: power loss dropped the first 512-byte sector of the single tail frame being written while later sectors of that same frame reached the medium. No answer was sent for it; recovery must truncate at `pos`.
2. **Damaged earlier committed frame**: a committed frame at `pos` lost its first sector (now reading as zeros), and one or more subsequent committed frames lie past that sector within 64 MiB. Silently truncating at `pos` would drop committed transactions.

Decision 0055 explicitly left that ambiguity open pending a frame format that can be read backwards.

## Decision

1. **Bump `.mlog` header version to `WAL_VERSION = 2`**:
   - The 24-byte `.mlog` file header layout (`b"MQLG"`, `version: u16`, `flags: u16 = 0`, `incarnation: u64`, `base_revision: u64`) is unchanged except `version = 2`.
   - Legacy `v1` `.mlog` files are refused fail-closed with `OpenError::Corrupt` before any mutation (consistent with SQLite schema versioning). A cleanly checkpointed database (`.mlog` rotated/absent or `base_revision < R_ckpt`) reopens seamlessly by creating a fresh `v2` `.mlog`.
2. **Self-authenticating 16-byte frame footer (`FRAME_OVERHEAD = 20`)**:
   - Each `v2` frame is laid out as:
     `[body_len: u32 BE][body: body_len bytes][body_crc: u32 BE][body_len: u32 BE][footer_crc: u32 BE][b"MLGE"]`
   - `body_crc` is Castagnoli CRC-32C over `[body_len || body]` (unchanged from `v1`).
   - `footer_crc` is Castagnoli CRC-32C over `[frame_start_offset: u64 BE || body_len: u32 BE || b"MLGE"]`, finalized once the exact write offset `prev_len` is settled after any WAL rotation.
   - Forward replay verifies all four footer fields (`body_crc`, matching `body_len`, `footer_crc` at `pos`, and `b"MLGE"`).
3. **Backward footer scan on zeroed first sector**:
   - When forward replay encounters `body_len == 0` at `pos` with non-zero bytes after `[pos + 4 .. sector_end]` and within `[sector_end .. pos + FRAME_OVERHEAD + MAX_FRAME_BYTES]`, recovery scans the non-zero suffix `[sector_end .. last_nonzero_end]` for any self-authenticating `v2` footer (`b"MLGE"`, valid `body_len` in `2..=MAX_FRAME_BYTES`, `computed_start = frame_end - (FRAME_OVERHEAD + body_len)` with `computed_start > pos`, and matching `footer_crc(computed_start, body_len)`).
   - If any such footer is found — even if the subsequent frame's own header was wiped inside the same zeroed 512-byte sector — recovery fails closed with `OpenError::Corrupt`.
   - If no such footer exists (including a genuine torn single `v2` tail frame whose valid footer reached disk with `computed_start == pos`), recovery truncates cleanly back to `pos`.

## Consequences

- **Safety**: Closes the open condition of ADR 0055 for any damaged committed frame followed by one or more committed frames within 64 MiB. A single last frame whose first sector reads as zeros remains physically indistinguishable from an uncommitted torn tail and truncates cleanly.
- **Cost**: +12 bytes per group-commit frame (+16-byte CRC-32C calculation, ~5 ns); zero cost on normal clean recovery (the backward scan never runs when trailing pre-allocated space is all zeros).
- **Compatibility**: Existing uncompacted `v1` `.mlog` files must be checkpointed (clean shutdown / `compact`) before upgrading; uncompacted `v1` `.mlog` files fail closed rather than replaying silently.
- **Verification**: `crates/mqttiumd-store-volatile/tests/log_conformance.rs` (`log_refuses_a_zeroed_first_sector_when_a_committed_frame_follows_within_one_frame`) tests all four cases: (A) zeroed first sector followed by a complete committed frame, (B) shared zeroed sector wiping Frame 2 and Frame 3's header while leaving Frame 3's footer, (C) genuine torn single `v2` tail frame with intact footer at `computed_start == pos`, and (D) fail-closed refusal of legacy `v1` `.mlog`.
