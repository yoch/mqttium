# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

MQTTium is a dependency-free, async-native MQTT 3.1.1 / 5.0 client (`src/mqttium`, Python 3.11–3.14,
Apache-2.0). Runtime dependencies must stay empty — tooling only lives in the
`dev`/`fuzz`/`security`/`release` extras.

Since `0.2.0b1` the public surface is tiered in `docs/API-STABILITY.md`: **Stable** (`mqttium`
errors/enums, `mqttium.api`, `mqttium.helpers`) follows SemVer, **Provisional**
(`compat`/`persistence`/`transport`/`protocol.ProtocolEngine`/`packets`/`codec`, `ClientStats`)
may gain fields with a changelog entry, **Internal** (`InboundSession`, `OutboundSession`,
`EffectPump`, `WritePump`, `EngineEffect`, `PacketIdPool`, underscore modules) has no guarantee
even when importable. Check the tier before changing or re-exporting a name.

## Commands

```bash
python -m pip install -e ".[dev,fuzz,security,release]"

ruff format --check src tests benchmarks
ruff check src tests benchmarks
mypy src/mqttium
bandit -q -ll -r src

python -m pytest -q tests/unit
python -m pytest -q tests/unit --cov=mqttium --cov-report=term-missing --cov-fail-under=80
python -m pytest -q tests/unit/test_qos2_matrix.py::test_name       # single test
python -m pytest -q tests/integration                                # needs a broker, see below
```

`pyproject.toml` sets `pythonpath = ["src"]` and `asyncio_mode = "auto"` — no `PYTHONPATH` and no
`@pytest.mark.asyncio` are needed under pytest.

### Integration tests

They require a Mosquitto broker on `127.0.0.1:11883` and **silently skip** when it is absent, so a
green run does not mean they executed. Start one the way CI does:

```bash
printf 'listener 11883 127.0.0.1\nallow_anonymous true\npersistence false\n' > /tmp/mosq.conf
mosquitto -c /tmp/mosq.conf -d
```

CI installs Mosquitto and waits for the port before running them, so they do execute there — a
local skip hides breakage until the `test` job (unit + integration on 3.11–3.14). The other jobs
narrow the matrix deliberately: `quality` runs lint, mypy, bandit and the coverage gate on 3.12
only, and `cross-platform` runs unit tests on macOS and Windows for 3.11/3.14 (it requires
`openssl` on PATH for the TLS tests), so a Windows- or macOS-only break surfaces there.

Unit tests that need a transport use `tests/support.py` — `QueueTransport` (a fake `AsyncTransport`
collecting `WriteItem`s), `feed_engine()` to push raw wire bytes into a `ProtocolEngine`, and
`write_item_bytes()` to flatten the `(header, payload)` form. Prefer them over ad-hoc doubles.

### Fuzzing

```bash
PYTHONPATH=src python tests/fuzz/fuzz.py --seed 1 --iterations 20000   # deterministic, seeded
python -m pytest -q tests/fuzz/test_hypothesis_fuzz.py                 # Hypothesis
HYPOTHESIS_PROFILE=aggressive python -m pytest -q tests/fuzz/test_hypothesis_fuzz.py
```

### Benchmarks

`benchmarks/*.py` are standalone scripts (`perf_sprint.py` for regressions, `paired_regression.py`
and `paired_network.py` for candidate-vs-base runs, `publish_many_ab.py`, `compare_libs.py`,
`realworld.py`, `application_stress.py`, `memory_profile.py`, `soak.py`, plus targeted A/Bs such as
`persistence_index_ab.py` and `packet_id_pool_ab.py`). Results are build artefacts: never commit
generated numbers, and follow the validity contract in `docs/BENCHMARKING.md` (paired A/B, medians,
rotated order, `N/A` rather than a manufactured comparison). The one committed JSON,
`benchmarks/memory_thresholds.json`, is a hand-set limit file checked by
`check_memory_thresholds.py`, not a measurement.

### CI hygiene gates

`.github/workflows/ci.yml` fails if any cache/build artefact is tracked by git, or if the strings
`mqttnext`/`MQTTNext`/`MQTTNEXT` appear anywhere under `src tests examples benchmarks` (pre-spin-out
name). The `package` job reads the expected version out of `src/mqttium/__init__.py` by AST
(`__version__` is the hatch version source, so a bump needs no workflow edit), then checks wheel
metadata, `py.typed`, bundled licences, sdist contents, and imports every module of the installed
wheel in an isolated interpreter — a module that only imports under `pythonpath = ["src"]` fails
here.

The release path has its own workflows: `prepublish-audit.yml` and `publish.yml` both require the
git tag to be exactly `v{__version__}` and cross-check the GitHub pre-release flag against the
`a`/`b`/`rc`/`.dev` suffix. `benchmarks.yml` and `paired-regression.yml` run the measurement suites
against a broker and upload JSON artefacts; they never commit numbers.

## Architecture

Two layers, strictly separated:

- **`protocol/engine.py` — `ProtocolEngine`**: the correctness core. Pure synchronous state machine:
  no `asyncio`, no sockets, no user callbacks, no wall clock, no `await`. Packets and commands go
  in (`handle_raw`, `queue_publish`, `begin_connect`, …), `EngineEffect` objects come out via
  `take_effects()`. It owns connection state, negotiation, packet dispatch and the one ordered
  effect stream, while directional MQTT publication state has one owner on each side:
  - **`protocol/outbound.py` — `OutboundSession`** (`engine.outbound`): admission budget, packet
    ids, flow window, the `QUEUED` deque, outbound store records, replay and hydration. A QoS 1/2
    publish acquires four resources (budget, mid, store row, flow slot); this is the one component
    that acquires and releases them, which is what makes rollback auditable. It does *not* own
    connection state — it reads `state`/`negotiated` back from the engine and emits through
    `ProtocolEngine._emit`/`_send` so effect ordering stays observable in one place.
    `engine.flow`, `engine.packet_ids` and `engine._queued` remain compatibility views for tests,
    benchmarks and instrumentation. Both publish entry points unwind through the single
    `_rollback()` primitive; callers only snapshot, so the success path pays nothing and the two
    cannot drift. Admission is all-or-nothing and fault-injected in
    `tests/unit/test_outbound_transaction.py` — extend it when you add an acquisition step.
  - **`protocol/inbound.py` — `InboundSession`** (`engine.inbound`): inbound topic aliases, the
    local Receive Maximum counter, persisted QoS 1/2 records, manual acknowledgement and restart
    redelivery. `PUBLISH` and `PUBREL` dispatch directly to its bound handlers, avoiding an extra
    wrapper frame on the ingress hot path. It emits through the engine's shared effect stream;
    connection state and negotiated settings remain on `ProtocolEngine`.
- **`api/async_client.py` — `AsyncClient`**: the asyncio adapter. Owns the transport lifecycle, the
  reader task, keepalive/reconnect timers, callbacks, delivery queues, and turns effects into
  futures, receipts and messages. Two runtime mechanisms are factored out into their own owners,
  and the client keeps only read-only views of their state:
  - `api/_writer.py` — `WritePump` owns the write queue, byte/count backpressure, batching, the
    single writer task and the last-write timestamp. `AsyncClient` decides what a writer failure
    does to protocol state; it does not touch the queue.
  - `api/_effects.py` — `EffectPump` owns the connection-scoped effect deque, the inline/slow-path
    accounting and the `mqttium-effect-flush` task. `AsyncClient._collect_effects_locked` is bound
    to `EffectPump.collect_from_engine` at construction, and `_pending_effects`,
    `_effect_enqueued`, `_effect_applied` are properties over the pump kept for tests and
    instrumentation. Interpretation of individual effects stays on the client
    (`_apply_effect_inline`), because that is what owns transports, futures and queues.

Anything protocol-shaped belongs in the engine or one of its directional sessions; anything with a
timer, socket or callback belongs in the client. Do not leak `asyncio` into `protocol/`.

### Invariants that tests actively enforce

These come from `docs/IMPLEMENTATION-GUIDE.md` §1 and have dedicated regression tests — breaking one
surfaces as a confusing failure elsewhere:

1. **Single writer.** Exactly one task writes to the transport, fed by a FIFO queue. Wire order ==
   the order of engine `SEND` effects. Nothing else calls `transport.write`.
2. **Receipts before wire.** A future/receipt keyed by a packet id is registered *before* the packet
   can leave; the ACK may land during the very next `await`.
3. **Owned bytes.** The engine only ever receives owned `bytes`; `IncrementalDecoder` copies at the
   packet boundary. Never hand a `memoryview` of the reusable buffer to the engine or the API.
4. **Packet ids ≠ flow window.** `PacketIdPool` always spans `1..65535`; `FlowControl` bounds only
   unfinished QoS 1/2 PUBLISHes and is what CONNACK `Receive Maximum` adjusts. Inbound QoS 2 ids are
   never freed into the outbound pool.
5. **One source of truth.** QoS state lives in the `InflightStore` plus the `OutboundQoSState` /
   `InboundQoSState` values; the outbound `_queued` deque is only an ordered index of `QUEUED`
   messages and must stay in sync both ways. Inbound aliases, receive-window count and recovery
   markers live only in `InboundSession`; engine underscore attributes exposing them are views.
6. **Callbacks outside critical sections.** No engine lock is held during a callback; a callback may
   call `publish()` without deadlocking.
7. **No in-session retransmission.** PUBLISH/PUBREL are replayed only on reconnect with a present
   session (DUP=1 on PUBLISH). There is deliberately no retransmit timer.

### Effect pipeline (the subtle part of `AsyncClient`)

`_collect_effects_locked()` (i.e. `EffectPump.collect_from_engine` in `api/_effects.py`) drains the
engine and applies the common single-effect case inline (`_apply_effect_inline`) with no accounting
at all — the counters and the `_pending_effects` deque exist only for genuinely async slow paths
(backpressure waits, callback dispatch). When several
effects are produced, `SEND` effects are reordered first so wire order is preserved before any
application-visible event fires; leftovers are drained by the `mqttium-effect-flush` task.

Every effect carries a **connection epoch**. On disconnect the epoch is bumped and stale effects are
dropped, which is how in-flight work from a dead connection is prevented from touching the new one.
When touching this area, keep `_effect_enqueued`/`_effect_applied` balanced — waiters rely on them.

`CONTINUE_INBOUND_REPLAY` is the one effect the client answers by re-entering the engine.
`InboundSession.replay_session()` emits a bounded batch of redeliveries and then this marker;
applying it takes `_engine_lock` and asks for the next batch. That is what puts delivery
backpressure *between* batches instead of after all of them, and keeps replay memory proportional
to one batch. It carries the epoch like everything else, so a disconnect mid-replay drops it and
the cursor is abandoned at the next `begin_connect()`. A direct `ProtocolEngine` consumer that is
not `AsyncClient` must pump `continue_inbound_replay()` itself while `inbound.replay_pending`.

### Other structure

- `codec/` — VBI, bounded incremental decoder (`buffer.py`), MQTT-UTF-8 primitives, MQTT 5 property
  table (`properties.py`, with encode/decode validation per packet type), and fixed-header/flags
  validation (`packet_validation.py`).
- `packets/` — one module per packet family (`connect.py`, `publish.py`, `subscription.py`,
  `acks.py`, `control.py`) over shared framing helpers in `_common.py`. `__init__.py` only
  re-exports; keep `__all__` in sync when adding a packet type.
- `persistence/` — `InflightStore` is a `Protocol`; `MemoryInflightStore` and `SqliteInflightStore`
  implement it. `OutboundSession` hydrates packet ids and the offline queue from the store at
  construction, while `InboundSession` recovers inbound delivery markers. Two opt-in extensions,
  each resolved once with `isinstance` and each with a working fallback:
  - `PagedInflightStore` (`out_pages`/`out_summary_pages`/`in_pages`) keeps replay memory
    proportional to page size; without it the sessions fall back to the eager path.
  - `TransitionInflightStore` adds atomic, conditional, **payload-free** transitions
    (`complete_out`, `transition_out`, `contains_in`, `in_meta`, `mark_in_delivered`,
    `transition_in`, `complete_in`, `in_index_pages`, `set_out_logical_size`). The store never owns
    the state machine: `expected_state`/`new_state` come from the session, and the store only
    guarantees the mutation is atomic, returning `None` when the record is absent or has moved on.
    This is what lets a PUBACK settle a multi-megabyte publication without reading the BLOB.
    Budget accounting then relies on the persisted `logical_size`; legacy rows carrying 0 are
    recomputed once at hydration and written back.

  `SqliteInflightStore` versions its schema in `PRAGMA user_version` (`SQLITE_SCHEMA_VERSION`,
  currently 4). Migrations run in one transaction, so an interrupted upgrade reopens at the
  starting version; a newer schema is refused. `batch()` is lazy — `BEGIN IMMEDIATE` is deferred to
  the first mutation, so a read-only ingress lot takes no write lock. Two storage decisions are
  load-bearing and measured, not cosmetic: **`payload` is declared last** (SQLite walks the columns
  before the one you want, and a BLOB spills to overflow pages, so a metadata column placed after
  it costs an overflow traversal on reads that never wanted the payload), and there is
  **deliberately no `seq` index** — ordered pagination is one sorted metadata pass
  (`_ordered_mids`) followed by primary-key page reads, which beat the indexed page-per-query form
  while costing nothing on every publish. `benchmarks/persistence_index_ab.py` keeps that
  falsifiable. Pages snapshot their identifiers up front, so a page whose records were acknowledged
  meanwhile comes back *shorter* — same contract as `MemoryInflightStore`.
- `transport/` — `AsyncTransport` protocol over TCP/TLS, Unix and WebSocket. A `WriteItem`
  (`writes.py`) is either `bytes` or a `(header, payload)` tuple: payloads past
  `SEGMENT_THRESHOLD` (1 MiB) are written as two consecutive writes instead of being concatenated.
- `dispatch/matcher.py` — `TopicMatcher`, the filter→callback index used for callback dispatch. It
  deliberately duplicates a small amount of filter logic rather than sharing engine state, and
  values may legitimately be `None`.
- `helpers/` — one-shot `publish`/`subscribe` convenience APIs (the Paho `publish.single` /
  `subscribe.simple` analogues). Stable API tier, but strictly a consumer of `AsyncClient`.
- Statistics are snapshots, and each layer owns its own: `protocol/stats.py`
  (`OutboundStats`/`InboundStats`, computed by the sessions), `transport/stats.py`
  (`TransportStats`, with an `unavailable()` fallback so implementing `stats()` stays optional for
  third-party transports), and `api/stats.py` (`ClientStats` and friends, which compose the rest).
  The dependency always points from the runtime adapter towards the core, never back.
- `compat/paho.py` — additive Paho `CallbackAPIVersion.VERSION2` façade running `AsyncClient` on a
  dedicated thread + loop. It is a strict consumer of the native API; the core must never import or
  accommodate it. Intentional divergences are documented in `docs/COMPAT.md` and enforced by
  `tests/unit/test_compat_confinement.py`.
- `protocol/` — `effects.py` (the `EngineEffect` vocabulary), `config.py` (`EngineConfig` and the
  allowlist of runtime-mutable fields), `engine.py` (connection/dispatch/effect orchestration),
  `outbound.py` and `inbound.py` (directional publication state), plus the small owned pieces they
  build on: `packet_ids.py`, `flow_control.py`, `negotiated.py` (CONNACK results, guide §3) and
  `reconnect.py` (backoff policy and the terminal-reason-code sets, guide §5). `__init__.py`
  re-exports lazily through `__getattr__` so `import mqttium.protocol` does not drag in the engine,
  the codec and the persistence layer; the directional sessions stay internal. `packets` must never
  import `protocol` — that dependency is what the `codec/packet_validation.py` split removed.

## Conventions

- **No logging.** `logging` is deliberately absent from `src/` — even a disabled level guard costs
  ~1.6% per publish. Observability goes through receipts, callbacks and counters. See
  `docs/LOGGING.md` before adding any instrumentation.
- **Fail before mutating state.** Topic/filter validation and negotiated-limit checks
  (`maximum_packet_size`, `maximum_qos`, `retain_available`, …) raise before anything is queued.
- **No silent degradation.** `publish(qos=2)` against a broker advertising `maximum_qos=1` raises
  `ProtocolError` rather than downgrading.
- Errors derive from `MQTTError` and never shadow builtins (hence `MQTTTimeoutError`).
- Ruff line length 100; `select = ["E4", "E7", "E9", "F", "B", "RUF006", "UP"]`. `RUF006` matters:
  keep a reference to every `asyncio.create_task`.
- Performance changes need a comparable benchmark and may not weaken correctness checks; API,
  protocol-state or persistence invariant changes must be documented. Anything user-visible goes
  under `[Unreleased]` in `CHANGELOG.md`; an incompatible change to a Stable name additionally
  needs a note in `docs/MIGRATION.md`.

## Documentation authority

`docs/API-STABILITY.md` is authoritative for what may change and how; `docs/RELEASING.md` for the
tag/publication procedure. On protocol behaviour, conflicts resolve as: the MQTT 3.1.1/5.0 spec >
`docs/IMPLEMENTATION-GUIDE.md` (precise contracts: property table, CONNACK negotiation, keepalive,
reconnect, timeouts, QoS decisions, backpressure budgets) > `docs/DESIGN.md` (architecture).
`docs/DESIGN.md`, `docs/IMPLEMENTATION-GUIDE.md`, `docs/FUZZING.md` and `docs/LOGGING.md` are
written in French; the rest of the repository is English.

The directory is split by kind, and `docs/README.md` indexes both halves. Files directly under
`docs/` are **contracts**: maintained descriptions of current behaviour, updated in the same change
that contradicts them. Files under `docs/reports/` are **reports**: one-off records of a
measurement, audit or decision, each naming the commit it describes. Read a report for rationale,
never cite it as current behaviour, and never edit it to match new code — supersede it with a new
report instead. A report never outranks a contract, whatever its date.

When you add a document, put it on the right side of that line and add it to the matching index. A
new hot-path A/B or campaign record is a report; only a change to what MQTTium guarantees touches a
contract.
