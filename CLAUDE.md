# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

MQTTium is a dependency-free, async-native MQTT 3.1.1 / 5.0 client (`src/mqttium`, Python 3.11–3.14,
Apache-2.0, alpha `0.1.0a2`). Runtime dependencies must stay empty — tooling only lives in the
`dev`/`fuzz`/`security`/`release` extras.

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

### Fuzzing

```bash
PYTHONPATH=src python tests/fuzz/fuzz.py --seed 1 --iterations 20000   # deterministic, seeded
python -m pytest -q tests/fuzz/test_hypothesis_fuzz.py                 # Hypothesis
HYPOTHESIS_PROFILE=aggressive python -m pytest -q tests/fuzz/test_hypothesis_fuzz.py
```

### Benchmarks

`benchmarks/*.py` are standalone scripts (`perf_sprint.py` for regressions, `publish_many_ab.py`,
`compare_libs.py`, `realworld.py`, `application_stress.py`, `memory_profile.py`). Results are build
artefacts: never commit generated numbers, and follow the validity contract in
`docs/BENCHMARKING.md` (paired A/B, medians, rotated order, `N/A` rather than a manufactured
comparison).

### CI hygiene gates

`.github/workflows/ci.yml` fails if any cache/build artefact is tracked by git, or if the strings
`mqttnext`/`MQTTNext`/`MQTTNEXT` appear anywhere under `src tests examples benchmarks` (pre-spin-out
name). The `package` job pins `0.1.0a2` in assertions — bumping the version in
`src/mqttium/__init__.py` (the hatch version source) requires updating that workflow too.

## Architecture

Two layers, strictly separated:

- **`protocol/engine.py` — `ProtocolEngine`**: the correctness core. Pure synchronous state machine:
  no `asyncio`, no sockets, no user callbacks, no wall clock, no `await`. Packets and commands go
  in (`handle_raw`, `queue_publish`, `begin_connect`, …), `EngineEffect` objects come out via
  `take_effects()`. All QoS 1/2 state, session replay, packet-id allocation, flow control and
  negotiation validation live here — which is why it is unit-testable without a network.
- **`api/async_client.py` — `AsyncClient`**: the asyncio adapter. Owns the transport, the reader
  task, the single writer task, keepalive/reconnect timers, callbacks, delivery queues, and turns
  effects into futures, receipts and messages.

Anything protocol-shaped belongs in the engine; anything with a timer, socket or callback belongs in
the client. Do not leak `asyncio` into `protocol/`.

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
   `InboundQoSState` values; the engine's `_queued` deque is only an ordered index of `QUEUED`
   messages and must stay in sync both ways.
6. **Callbacks outside critical sections.** No engine lock is held during a callback; a callback may
   call `publish()` without deadlocking.
7. **No in-session retransmission.** PUBLISH/PUBREL are replayed only on reconnect with a present
   session (DUP=1 on PUBLISH). There is deliberately no retransmit timer.

### Effect pipeline (the subtle part of `AsyncClient`)

`_collect_effects_locked()` drains the engine and applies the common single-effect case inline
(`_apply_effect_inline`) with no accounting at all — the counters and the `_pending_effects` deque
exist only for genuinely async slow paths (backpressure waits, callback dispatch). When several
effects are produced, `SEND` effects are reordered first so wire order is preserved before any
application-visible event fires; leftovers are drained by the `mqttium-effect-flush` task.

Every effect carries a **connection epoch**. On disconnect the epoch is bumped and stale effects are
dropped, which is how in-flight work from a dead connection is prevented from touching the new one.
When touching this area, keep `_effect_enqueued`/`_effect_applied` balanced — waiters rely on them.

### Other structure

- `codec/` — VBI, bounded incremental decoder (`buffer.py`), MQTT-UTF-8 primitives, MQTT 5 property
  table (`properties.py`, with encode/decode validation per packet type).
- `packets/__init__.py` — one module holding every packet dataclass and its encode/decode.
- `persistence/` — `InflightStore` is a `Protocol`; `MemoryInflightStore` and `SqliteInflightStore`
  implement it. The engine hydrates packet ids and the offline queue from the store at construction,
  so durable restart works without extra API. `PagedInflightStore` is an opt-in extension
  (`out_pages`/`out_summary_pages`/`in_pages`) that keeps replay memory proportional to page size;
  the engine resolves it once with `isinstance` and otherwise falls back to the eager path.
- `transport/` — `AsyncTransport` protocol over TCP/TLS, Unix and WebSocket. A `WriteItem` is either
  `bytes` or a `(header, payload)` tuple: payloads past `SEGMENT_THRESHOLD` (1 MiB) are written as
  two consecutive writes instead of being concatenated.
- `compat/paho.py` — additive Paho `CallbackAPIVersion.VERSION2` façade running `AsyncClient` on a
  dedicated thread + loop. It is a strict consumer of the native API; the core must never import or
  accommodate it. Intentional divergences are documented in `docs/COMPAT.md` and enforced by
  `tests/unit/test_compat_confinement.py`.
- `protocol/__init__.py` re-exports lazily through `__getattr__` to break the
  `packets → protocol.validate → protocol → engine → packets` cycle. Do not add eager imports there.

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
  protocol-state or persistence invariant changes must be documented.

## Documentation authority

On conflict: the MQTT 3.1.1/5.0 spec > `docs/IMPLEMENTATION-GUIDE.md` (precise contracts: property
table, CONNACK negotiation, keepalive, reconnect, timeouts, QoS decisions, backpressure budgets) >
`docs/DESIGN.md` (architecture). `docs/DESIGN.md`, `docs/IMPLEMENTATION-GUIDE.md`, `docs/FUZZING.md`
and `docs/LOGGING.md` are written in French; the rest of the repository is English.
