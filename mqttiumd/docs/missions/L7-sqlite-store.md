# Mission L7 — the SQLite store, a first vertical slice

Read `README.md` of this directory first, then `mqttiumd/AGENTS.md`,
`docs/reception/open-conditions.md` (the rows closed by L7), chapter 4
of the specification for DEM-011, and decision records 0002, 0003, 0008,
0009, 0011 and 0014.

## Goal

A store on one SQLite file realises `DurableStore` behind the existing
contract, passes the same contract tests as the deterministic store,
is played by the bench in one judged scenario, and gives an early
measurement of its writer.

## What the store must guarantee

- **One file, one writer, WAL, `synchronous=FULL` (DEM-011).** The
  values are set, then read back and recorded; a requested configuration
  is not a proof that it applies. The store refuses to open when a value
  read back differs. The exact engine version and its compile options
  are read from the engine and recorded.
- **One transaction per command.** Every guard is a read inside the
  transaction and every row a write in it; a failed guard rolls back and
  leaves no admitted subset. The command runs on the real tables.
- **The contract, not a second one.** The decision of an opening is
  `EpochLedger::open`; its answer is `OpeningOutcome::read` of the ledger
  that is durable after the write. Recipes are compared by their exact
  canonical bytes.
- **Durable encodings (AGENTS rule 7).** Identities and 64-bit values are
  BLOBs of a checked type and length, big-endian. A counter is decoded in
  Rust, advanced with `checked_add`, and written back conditionally;
  there is no SQL arithmetic on a BLOB. The schema version is
  `PRAGMA user_version`; an unknown or future version is refused before
  any write.
- **Bounded reads.** Recovery reads bounded pages of the inventory in the
  order of record 0014, at one state revision.
- **No `unsafe` in our code.** The binding carries it; the pure crates
  take no dependency.

## Order of delivery

1. The crate `crates/mqttiumd-store-sqlite/`: schema, opening and
   verification of the file, `DurableStore`.
2. The generic contract tests of `mqttiumd-lab::store::conformance`
   applied to it, F1 (`accounts_match_what_is_held`), `established`, the
   opening of an epoch and the collection included. A case that does not
   pass is a finding, never a case removed.
3. The early measurement of the writer (record 0008), as soon as 1 and 2
   hold: the real `AdmitPublication` transaction, durability as in
   DEM-011. It reports the hardware and storage, the effective SQLite
   parameters read back, payload sizes and fan-out, the rate of durable
   store operations, latencies with percentiles, CPU and memory, and any
   commit grouping. A store transaction rate is not an MQTT rate of the
   broker. Exploratory and non-qualifying, and said so. The script and
   the report are committed; generated data never is.
4. The topology of processes and of the collector for a real crash
   (record 0002): the evidence a verdict needs must survive the killed
   process. Written and read by the integrator before any real kill.
5. With lot L4, through the integrator: one scenario played by the bench
   on this store and judged by the oracle.

## Later

- Transaction crash validation is implemented in the next increment:
  `L7-kill-inside-a-transaction.md`. K1/K2b/K3 are deterministic process
  boundaries; K2a samples timings, and WAL prefixes are separate artificial
  images. Their finite evidence supports M1's atomic `Commit` assumption
  without proving it for every instant, kernel failure or storage device.

## Paths

Owned by this lot: `crates/mqttiumd-store-sqlite/`, and
`tools/measure_writer.*` with its report under `docs/reports/` if the
measurement needs them. The workspace member and the new dependencies in
`Cargo.toml` and `Cargo.lock` are shared files: proposed to the
integrator with their reason.

Not owned: the deterministic store and its contract tests
(`crates/mqttiumd-lab/src/store`, lot L2), the bench (L4), the formats
(L3). When the store needs a change there, ask the integrator for the
smallest change that would do.

## Reception

- `cargo test --workspace --locked` runs the contract tests on both
  stores.
- The effective settings, the engine version and its compile options are
  read back and recorded.
- The measurement report states its perimeter, its units and that it
  qualifies nothing.
- The closing report lists the exact revision, the commands run and their
  results, what was not done, and the limits of the store.
