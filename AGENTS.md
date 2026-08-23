# AGENTS.md

MQTTium is a dependency-free, async-native MQTT 3.1.1 and MQTT 5.0 client for
Python 3.11–3.14, licensed under Apache-2.0. This is the canonical repository
guide for coding agents. Detailed user and maintainer contracts live in `docs/`.

## Development loop

This project uses pip, not uv. Runtime dependencies must remain empty.

```bash
python -m pip install -e ".[dev,fuzz,security,release,docs]"
ruff format --check src tests benchmarks
ruff check src tests benchmarks
mypy src/mqttium
bandit -q -ll -r src
python -m pytest -q tests/unit tests/project
python -m pytest -q tests/unit tests/project --cov=mqttium
PYTHONPATH=src python tests/fuzz/fuzz.py --seed 1 --iterations 20000
python -m pytest -q tests/fuzz/test_hypothesis_fuzz.py tests/fuzz/test_stateful_invariants.py
mkdocs build --strict
```

`pyproject.toml` supplies `pythonpath = ["src"]` and asyncio auto mode. Do not
add `@pytest.mark.asyncio`. TLS tests require `openssl` on `PATH`.

Live integration tests require Mosquitto on `127.0.0.1:11883`. CI sets
`MQTTIUM_REQUIRE_BROKER=1`, so a missing broker or skipped integration test is a
failure. Start a local broker with:

```bash
printf 'listener 11883 127.0.0.1\nallow_anonymous true\npersistence false\n' > /tmp/mosq.conf
mosquitto -c /tmp/mosq.conf -d
```

Use the packet-aware transports and wait helpers in `tests/support.py` before
creating a new test double. Keep fault-specific transports local to their test.

## Architecture and ownership

MQTTium has two strictly separated layers. Never leak asyncio, sockets, wall
clock access, or user callbacks into `protocol/`.

- `protocol/engine.py` contains `ProtocolEngine`, a synchronous state machine.
  Commands and packets go in; ordered `EngineEffect` values come out through
  `take_effects()`.
- `protocol/outbound.py` owns outbound admission, packet identifiers, flow
  slots, persisted outbound records, replay, and rollback.
- `protocol/inbound.py` owns inbound aliases, Receive Maximum accounting,
  persisted QoS state, manual acknowledgements, and restart redelivery.
- `api/async_client.py` adapts the engine to asyncio and owns connections,
  timers, callbacks, receipts, delivery queues, and reconnect policy.
- `api/_writer.py` owns every transport write, the writer queue, batching, and
  writer-side byte/count backpressure.
- `api/_effects.py` owns the connection-scoped effect deque and deferred effect
  processing. The client interprets effects because it owns runtime objects.

The native API must not accommodate the Paho façade. `compat/paho.py` is a
Provisional consumer that runs `AsyncClient` on a dedicated thread and loop;
the core never imports it.

## Load-bearing invariants

These invariants have regression coverage and must remain explicit in reviews:

1. **Single writer.** Only `WritePump` writes to a transport. A segmented
   `(header, payload)` item is written consecutively and never takes the eager
   path.
2. **Receipts before wire.** Register packet-id futures and receipts before the
   corresponding packet can leave; an ACK may arrive at the next await.
3. **Owned bytes.** The decoder copies at packet boundaries. Never pass a view
   of its reusable buffer into the engine or public API.
4. **Packet identifiers are not flow slots.** Packet ids span `1..65535`;
   `FlowControl` limits only unfinished outbound QoS 1/2 publishes.
5. **One source of truth.** QoS state lives in the inflight store and directional
   session states. Compatibility attributes and ordered queues are views or
   indexes and must stay synchronized with their owner.
6. **Callbacks outside critical sections.** No engine lock is held while user
   code runs. Callback-initiated publication must not deadlock.
7. **No in-session retransmission.** PUBLISH and PUBREL replay only after a
   reconnect with a present session; there is no retransmission timer.

QoS 1/2 outbound admission is a transaction over budget, packet id, store row,
and flow slot. All failures unwind through the outbound session's rollback
path. Extend `tests/unit/test_outbound_transaction.py` whenever an acquisition
step is added.

## Effect and replay pipeline

The common single-effect case is applied inline. Deferred effects live in the
`EffectPump`; SEND effects retain wire order before application-visible events.
Every connection-scoped effect carries an epoch, and stale effects from a dead
connection must not affect a new one.

`CONTINUE_INBOUND_REPLAY` is answered by re-entering the engine for the next
bounded replay batch. This places delivery backpressure between batches and
keeps memory proportional to one batch. Direct `ProtocolEngine` consumers must
pump `continue_inbound_replay()` while replay remains pending.

## Persistence

`InflightStore` is the base protocol. Memory and SQLite stores also expose
optional paged and conditional-transition capabilities detected once by the
directional sessions. The store guarantees atomic mutation; it never owns the
MQTT state machine.

SQLite schema changes are transactional and versioned with
`PRAGMA user_version`. Preserve these measured design choices unless new A/B
evidence justifies a change:

- payload columns stay last so metadata reads do not traverse large BLOBs;
- ordered pagination uses a metadata snapshot followed by primary-key reads;
- `batch()` does not acquire a write lock until the first mutation;
- logical sizes are persisted so acknowledgements do not reread payloads.

## Project conventions

- **No logging in `src/`.** Observability uses receipts, callbacks, counters,
  and immutable statistics snapshots. See the logging decision document.
- **No silent degradation.** Unsupported QoS or negotiated-limit violations
  raise before state changes; they are never silently downgraded.
- Validate topics, properties, sizes, and negotiated limits before mutation.
- MQTTium-defined Stable client errors derive from `MQTTError` and avoid
  builtin names. Documented system and cancellation exceptions may cross a
  Stable boundary (`OSError` from transport setup, for example). Provisional
  `SqliteInflightStore` follows Python's DB-API boundary and may raise
  `sqlite3.Error` or `RuntimeError` as documented.
- Keep a reference to every created asyncio task; Ruff's `RUF006` enforces it.
- Generated benchmark data, coverage files, caches, and build artifacts are
  never committed.
- Performance changes require comparable evidence and may not weaken protocol,
  persistence, or API contracts.

## API and documentation contracts

API tiers are defined by the API stability document, not by importability or
`__all__`:

- **Stable** follows SemVer and the documented deprecation process.
- **Provisional** is supported and tested but may evolve with changelog and
  migration guidance.
- **Internal** includes underscore modules, directional sessions, pumps, and
  engine plumbing and has no compatibility guarantee.

Update `[Unreleased]` in `CHANGELOG.md` for user-visible changes. An incompatible
Stable change also requires a migration note.

Maintained documentation is a current contract and must change with the code it
describes. `docs/reports/` is immutable historical evidence: never rewrite a
report to describe current behavior; index it as superseded or add a new report.
All maintained content is English.

Protocol-behavior conflicts resolve in this order: MQTT specification statements
under `docs/spec/`, the implementation guide, then the architecture/design
document.

## Release and repository hygiene

- `src/mqttium/__init__.py::__version__` is the Hatch version source. Release
  tags must be exactly `v{__version__}`.
- Publication builds once, validates the exact artifact, and uses PyPI trusted
  publishing. Do not introduce a second release build path.
- CI rejects tracked caches/build output and the legacy project-name strings
  under `src`, `tests`, `examples`, and `benchmarks`.
- Self-hosted workflows execute only trusted code, serialize access to the
  persistent runner, use run-specific temporary state, and clean up on failure.
- Benchmark scripts emit artifacts; public cross-client evidence belongs in the
  independent benchmark repository and never includes the Paho façade as the
  MQTTium product.
