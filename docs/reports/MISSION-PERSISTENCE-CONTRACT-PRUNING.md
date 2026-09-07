# Mission — persistence contract pruning

Base: `main@7d917547b491c8e521d1d82960b1e10810e4bd80`

Measured candidate: `5aadffe90e06cff7e91b75d665a2e81764222f53`

## Goal

Reduce the persistence contract to the smallest modern interface actually needed by MQTTium, then delete capability detection and legacy whole-object fallbacks that exist only for hypothetical weaker third-party stores.

## Outcome

The persistence runtime now has one complete Provisional `InflightStore` contract. Bounded replay, payload-free metadata paging, and conditional transition/completion operations are mandatory rather than optional capabilities discovered at runtime.

The separate `PagedInflightStore`, `BoundedInboundReplayStore`, and `TransitionInflightStore` protocols and their eager/read-mutate-write fallback paths are removed. The shipped stores also no longer expose the retired whole-object helpers `update_out`, `out_items`, `out_pages`, `pop_in`, `update_in`, `in_items`, `in_pages`, or `contains_in`.

Across the six runtime/persistence files changed by the mission (`persistence/__init__.py`, both shipped stores, `engine.py`, `inbound.py`, and `outbound.py`), the final implementation delta against the mission base is **+137 / -620 lines**. The simplification therefore removes substantially more runtime code than it adds.

## Success criteria

- One coherent persistence state-machine path for built-in and supported third-party stores: achieved.
- Transition/metadata operations required by the runtime are no longer optional: achieved.
- Replay remains bounded in memory; no supported path intentionally falls back to eager whole-store hydration: achieved.
- Remove `InboundMessage | InboundRecordMeta` dual-path logic wherever the stronger contract makes it unnecessary: achieved.
- Preserve MQTT correctness, restart recovery, durable QoS semantics and store atomicity: covered by unit/project, stateful/fuzz, integration, cross-platform, resilience and soak validation.
- Measure ACK/replay call counts, timing and memory against the exact base SHA: achieved below.
- Update Provisional persistence docs, migration guidance and changelog for the contract break: achieved.

## Exact A/A → A/B benchmark

GitHub Actions run: `34101244233` (`PR434 exact persistence benchmark`).

The harness checked out exact detached worktrees for the base and candidate and ran every sample in a fresh subprocess with `PYTHONPATH` pinned to that worktree. Seven rotated repeats used A1 = base, A2 = base, B = candidate, on Ubuntu 24.04 / Python 3.12.14. Replay payloads were 4096 bytes; ACK cases used 20,000 Memory-store records and 3,000 SQLite records; replay used 4,000 Memory-store records and 2,000 SQLite records.

| Metric | A/A control median | Candidate vs base | Base median | Candidate median |
| --- | ---: | ---: | ---: | ---: |
| Memory ACK throughput | -0.15% | **+0.42%** | 106,184 ops/s | 107,160 ops/s |
| SQLite ACK throughput | +0.85% | **-0.16%** | 43,361 ops/s | 43,165 ops/s |
| Memory outbound replay | +0.04% | **-9.91%** | 83.27 ms | 75.01 ms |
| SQLite outbound replay | -1.40% | **-38.59%** | 182.24 ms | 112.47 ms |
| Memory inbound startup | -0.18% | **-14.35%** | 20.73 ms | 17.68 ms |
| SQLite inbound startup | -0.31% | **-6.37%** | 25.57 ms | 23.92 ms |
| Memory inbound replay | -0.11% | **-4.59%** | 40.20 ms | 38.15 ms |
| SQLite inbound replay | +0.89% | **-2.71%** | 53.40 ms | 52.82 ms |

ACK performance is neutral within the A/A noise floor. Replay improves, especially SQLite outbound replay.

The call-count oracle explains the largest gain without changing observable replay work:

- ACK: base and candidate both call `complete_out()` exactly once per acknowledged record and never load the outbound payload.
- Memory outbound replay: base = `out_summary_pages: 1`, `get_out: 4000`, `update_out: 4000`; candidate = `out_summary_pages: 1`, `get_out: 4000`, no `update_out`.
- SQLite outbound replay: base = `out_summary_pages: 1`, `get_out: 2000`, `update_out: 2000`; candidate = `out_summary_pages: 1`, `get_out: 2000`, no `update_out`.
- Inbound replay uses the same single bounded `in_replay_pages()` call in base and candidate and delivers the same number of messages.

The retired per-record `update_out()` write during replay was therefore measurable overhead rather than required durable state.

### Memory

Replay peak allocations are effectively unchanged:

| Replay peak | Candidate vs base |
| --- | ---: |
| Memory outbound | -0.00% |
| SQLite outbound | +0.01% |
| Memory inbound | -0.12% |
| SQLite inbound | -0.01% |

Startup peak differences are likewise below 0.12%. The simplification keeps the bounded-memory behaviour of the shipped stores while removing the eager fallback contract.

## Validation evidence before the final exact-head gate

- Second-pass transformation: Ruff clean, `git diff --check` clean, **1597 unit/project tests passed**.
- Documentation migration: `mkdocs build --strict` passed.
- Exact benchmark candidate: run `34101244233` passed.
- Soak on the benchmark-triggering head: run `34101244260` passed.
- CI run `34101244199`: Python 3.11–3.14 unit/integration jobs, Windows/macOS, fuzz/stateful, resilience, package, Ruff, mypy, Bandit and strict docs all passed. Its only failure was `zizmor` rejecting the temporary benchmark workflow's unpinned Actions/permissions; that workflow and harness were removed after the benchmark evidence was captured.

The branch must still pass CI and soak on the final clean HEAD after this report commit. No merge is part of this mission without an explicit separate decision.

## Documentation / migration

The Provisional contract change is documented in:

- `CHANGELOG.md`;
- `docs/api-stability.md`;
- `docs/migration.md`;
- `docs/sessions-and-persistence.md`.

Third-party stores remain supported, but they must implement the complete modern `InflightStore` contract; MQTTium no longer maintains a weaker compatibility path that can silently materialise a whole durable session or emulate conditional transitions with read/mutate/write sequences.

## Non-goals

- No unrelated persistence feature additions.
- No change to SQLite durability guarantees.
- No merge until exact-head CI, fuzz/stateful tests and targeted persistence benchmarks are green.