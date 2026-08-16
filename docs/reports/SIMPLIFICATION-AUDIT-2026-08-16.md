# Perf-aware simplification audit

| | |
| --- | --- |
| Date | 2026-08-16 |
| Commit audited | `32e99a4` (merge of `release/1.0.0rc6`, tag `v1.0.0rc6`) |
| Scope | `src/mqttium` except `compat/paho.py`, and with it `dispatch/matcher.py`, whose only runtime consumer is the facade |
| Method | Full read of `api/`, `protocol/`, `codec/`, `packets/`, `persistence/`, `transport/`, `helpers/`; every finding reproduced by execution, by profiling, or by tracing every call site |

This is a dated report. It records what was true of `32e99a4` and what was done
about it. It is not a contract; do not edit it to match later code — write a new
report instead (see [`README.md`](README.md)).

## Question

The tree has been through several successive waves of fixes and optimisations.
Each wave left code running in parallel with the code it was meant to replace:
fast paths that duplicate the general path, compatibility views, parameters that
became constants, tri-state flags kept for a "legacy" case that no longer
occurs. Which of that can be removed **without paying for it in performance**,
and which of it only *looks* redundant?

The bar for every change: it must remove code *and* remove work, or remove code
and provably change nothing that runs.

## Gate, and two traps in it

`benchmarks/hotpath_profile.py` over the 31 scenarios of
`benchmarks/_paired_scenarios.py::REGISTRY` is the primary gate. It counts exact
Python calls and allocations per operation, so it is deterministic and
independent of host conditions — unlike a timing run, it can be used during
development without an idle machine.

Two properties of that harness had to be established before any number from it
could be trusted. Both were found by A/A control, as
[`../BENCHMARKING.md`](../BENCHMARKING.md) requires.

**The first profile after a source change is not comparable.**
`mqttium.protocol` re-exports lazily through `__getattr__`, so a cold run
attributes the import to whichever scenario touches the name first — worth about
1 call/op on the engine cycle scenarios. The first baseline taken in this work
was invalid for exactly this reason: re-measuring the *unmodified* base tree
reproduced the same deltas that had been read as a regression. Base and
candidate must both be measured warm, and a candidate must never be compared
against a baseline captured immediately after a checkout.

**`websocket_mask_4k` was bimodal on identical code**, reporting either 8.5 or
11.8 calls/op. `transport/websocket._xor_tables()` builds a 256×256 XOR table on
first use: one time per process, ~65 500 profiler-visible calls (256 `bytes()`
each consuming a 256-item generator). Whichever measurement touched it first
absorbed that cost. Reproducible by running the scenario alone several times, or
by touching any source file and running it again. The registry now primes the
table at import, which is outside every profiled window rather than an arbitrary
one; four consecutive runs, one immediately after a `touch`, then report
8.2004 calls/op.

The two `compat_*` scenarios run the facade on its own thread and drift by up to
a few percent run to run. They are out of scope here and are reported but not
gated. `tracemalloc_peak_bytes` is advisory for the same reason: it is stable to
a few hundred bytes on the synchronous scenarios and swings by tens of percent
on the threaded ones.

Functional gate throughout: `pytest tests/unit tests/integration` against a live
Mosquitto on `127.0.0.1:11883`, plus `ruff format --check`, `ruff check`,
`mypy`, and `bandit`.

> A trap worth recording for anyone reproducing this: the system interpreter on
> the audit host has no `pytest-asyncio`, so `pytest` reports
> `939 passed, 229 skipped` and looks green while silently dropping every
> `async def` test. Through the project virtualenv the same command reports
> `1168 passed`. A skip count above zero means the wrong interpreter or a
> missing broker.

## Result

`32e99a4` against the finished audit, both trees measured warm on the same
harness, base loaded through `PYTHONPATH` from `git archive` so nothing else
differs. Calls per operation:

| scenario | calls/op | | scenario | calls/op |
| --- | --- | --- | --- | --- |
| `publish_complete_receipt` | −16.7% | | `ingress_engine_qos0_v5` | −7.4% |
| `mqtt5_puback_reason_cycle` | −13.4% | | `publish_complete_callback` | −7.1% |
| `effect_send_inline` | −11.8% | | `effect_batch_inline` | −5.9% |
| `qos1_cycle_memory` | −10.3% | | `effect_batch_ordered` | −5.7% |
| `native_publish_nowait_qos0` | −9.5% | | `effect_batch_reordered` | −5.7% |
| `qos1_cycle_sqlite` | −9.3% | | `native_publish_nowait_qos0_callback` | −5.4% |
| `ingress_engine_qos0` | −9.0% | | `effect_single_message_callback` | −4.2% |
| `async_publish_nowait_qos0` | −8.9% | | `ingress_publish_qos1` | −2.9% |
| `qos2_cycle_sqlite` | −8.6% | | the four `encode_*` | ±0.00% |

No scenario regresses. The four `encode_*` scenarios, `websocket_mask_4k`,
`receipt_settle_unawaited`, the two `writer_*` and the three `delivery_*` are
exactly neutral, which is the expected shape: nothing in this audit touched the
PUBLISH encoder or the delivery queues' inner loops.

Source: **562 insertions, 669 deletions** across 20 files;
`pytest tests/unit tests/integration` goes from 1168 to 1172 passing, 0 skipped.

Traced peak moves two ways. It rises ~10% on the four ingress/cycle scenarios —
a flat ~600 bytes per engine for the two per-state handler dicts, constant
rather than per-operation, on a ~6 KB base — and ~1.7% elsewhere. Every
committed limit in `memory_thresholds.json` still passes with headroom.

## What was changed

Five waves, each independently revertable, each gated as above.

### 1 — Dead code

Symbols with no reference anywhere in `src`, `tests`, `benchmarks`, `examples`
or `docs`, plus guards that cannot fire. The one worth naming:
`packets/_control.py` defined `_PINGREQ`, `_PINGRESP` and `_DISCONNECT` twice,
the second block silently rebinding the first — a merge residue that had
survived every previous audit. Also `packets/_common._props_or_empty`,
`codec/primitives.append_u16`, `IncrementalDecoder.process_packets`, four
`AsyncClient` views and bound aliases, two `ProtocolEngine` setters, and the
`nowait` parameter of `_flush_effects`, whose only caller never passes it.

Exactly neutral on all 29 non-threaded scenarios.

### 2 — Less code *and* fewer operations

The changes that pay twice.

- `ProtocolEngine._check_outbound_size` sized the frame *before* testing whether
  a limit existed, and `maximum_packet_size` is `None` unless the broker
  negotiated one. Its sibling `_check_publish_wire_size` already returned early:
  this was the copy that had diverged. It runs on every QoS 0 publish, QoS 1/2
  launch, retransmit, PINGREQ, SUBSCRIBE and manual acknowledgement.
- `take_effects()` called into the inbound session unconditionally to return
  immediately for every publish-only or QoS 0 batch.
- `handle_raw` resolved the per-state allowlist and the handler separately — a
  dict lookup, a `frozenset()` construction and a set membership test before a
  second dict lookup, per inbound packet. One per-state handler table, derived
  at construction from the same declarative allowlist, answers both at once.
- `decode_properties` maintained a `seen` set whose contents were always the
  keys of `result.values`.
- `Properties._encoded` is the *encode* cache, and every decoded inbound packet
  allocated one without ever using it.
- `sqlite3.Binary` **is** `memoryview`, and `IntEnum`/`bool` bind as integers, so
  the wrapper and six `int()` casts per stored publish were no-ops; every reader
  already coerces on the way out.

Measured, warm on both sides:

| scenario | calls/op |
| --- | --- |
| `publish_complete_receipt` | −16.7% |
| `effect_send_inline` | −11.8% |
| `native_publish_nowait_qos0` | −9.5% |
| `ingress_engine_qos0` | −9.0% |
| `mqtt5_puback_reason_cycle` | −8.5% |
| `qos1_cycle_memory` | −7.2% |
| `qos1_cycle_sqlite` | −6.5% |
| `qos2_cycle_sqlite` | −5.9% |

Traced peak rises by a flat ~600 bytes per engine for the two per-state handler
dicts. It is constant, not per-operation — identical on an 80 000-operation and
a 50 000-operation scenario — against a ~6 KB base. `memory_profile.py` stays
within every committed threshold.

### 3 — Python frames off the publish and acknowledgement paths

`outbound.py:441-443` records that each additional Python frame on the publish
path measured ~1.5%, so this is where frame removal is worth the risk.

- `OutboundSession._emit`/`_send` were a forwarding layer `InboundSession` never
  had, so an outbound SEND cost three frames instead of one.
- `_try_launch` was a single-caller helper whose `except: flow.release()`
  duplicated `_rollback`, which already restores the window from its
  `inflight_start` snapshot.
- The same call site read `engine.state` a fourth time and consulted the flow
  window twice. For QoS > 0, `topic_bytes is not None` **is** the conjunction
  `state is CONNECTED and flow.available > 0` already recorded during
  validation, and nothing between the two observations can change either — the
  engine is synchronous and no intervening step touches connection state or the
  window. `queue_publish` now reads neither, and never attempts an acquire that
  cannot succeed. The publish path reads `engine.state` zero times.
- `_on_qos2` probed the store twice (`contains_in` then `in_meta`) where every
  other handler probes once. Both stores return `None` for an absent record
  without allocating, and the metadata columns precede `payload` in the SQLite
  row, so a miss costs what the bare existence query cost while a duplicate no
  longer costs two queries.
- `_acquire_slot` replaced a per-message `max()` builtin call with the plain
  compare `OutboundSession._reserve` already used.
- Both sessions cache `_is_v5` from the codec bindings instead of comparing
  `config.protocol` per publish. The protocol is fixed for the engine's lifetime
  — it is not in `_RUNTIME_MUTABLE_ENGINE_CONFIG_FIELDS`.
- `WritePump` resolved `write_many` by `getattr` on every batch; it is now
  resolved once per writer task, as a local, which unlike `_write_nowait` cannot
  outlive the transport it came from and so needs no explicit clearing.

| scenario | calls/op |
| --- | --- |
| `mqtt5_puback_reason_cycle` | −5.3% |
| `qos1_cycle_memory` | −4.4% |
| `qos2_cycle_sqlite` | −4.2% |
| `qos1_cycle_sqlite` | −4.0% |
| `ingress_publish_qos1` | −2.9% |

Every other scenario exactly neutral.

`tests/unit/test_outbound_transaction.py` gains
`test_launch_decision_matches_the_validation_snapshot`, pinning both sides of
the substitution: a free window launches and takes exactly one slot, a full
window queues and takes none. The existing `launch_encode` fault injection
already covers a raise after the slot is acquired, which is what the removed
local compensation handled.

### 4 — Structural duplication on cold paths

Where the duplication had begun to drift and a future fix would have had to be
applied twice.

- `ApplicationDelivery.accept` and `accept_decoded` were 66 near-identical lines
  differing in two expressions; everything else, including the
  `_release_unqueued` rollback, was copied character for character.
- Three ~90% identical CONNECT encoders — MQTT 3.1.1, MQTT 5, and a third
  inlined in `ConnectPacket.encode` for MQTT 3.1. Nothing binds a
  version-specific connect encoder; the dispatch happens in the dataclass either
  way. This is the one place where the `X.py`/`_X.py` split earned nothing.
- Six of the eight `_ack.py` encoders are unreachable from the engine (see
  below).
- `MemoryInflightStore.out_pages`/`in_pages` were the same loop twice, both
  doing two dict lookups per record where `in_index_pages` already used one;
  `in_replay_pages` encoded every topic to UTF-8 purely to size it, allocating a
  copy of each topic during replay, when `logical_size` is already maintained.
- Three copies of "enqueue a terminal packet, then bounded-wait the writer" in
  `AsyncClient`, and the reconnect reason-code block written twice.

Net over the audit: **562 insertions, 669 deletions** across 20 source files.

One measured caveat, stated precisely because it looks like a regression:
`qos1_cycle_*` gains 1 call/op and `qos2_cycle_sqlite` 2, and this is the
*scenario's own* `PubAckPacket` / `PubRecPacket` / `PubCompPacket` construction
now going through the shared acknowledgement encoder — fixture cost inside the
measured loop, not engine work. Profiling confirms the engine calls only the
decoders, and `ingress_publish_qos1`, the real inbound auto-acknowledgement
path, is unchanged at +0.00%.

### 5 — One conformance divergence

`OutboundSession.on_pubrec` tested for a Reason Code ≥ 0x80 only inside the
conditional-transition branch. A third-party `InflightStore` without the
`TransitionInflightStore` extension took the fallback path, where the
`msg is None` test came first, so an unknown packet identifier was answered with
an **orphan PUBREL 0x92** — a packet MQTT 5 §4.3.3 does not allow, since a
negative PUBREC ends the QoS 2 exchange. The shipped memory and SQLite stores
both implement the extension and were never affected.

Hoisting the test above the store branch fixes the divergence *and* removes the
duplicate check the fallback carried. Covered by
`test_negative_pubrec_never_answers_with_pubrel`, parametrised over
`MemoryInflightStore` and the extension-less `PlainInflightStore`; it fails on
the previous code with exactly the orphan PUBREL 0x92.

## Looks redundant, is load-bearing

Recorded so the next reader does not "clean up" a measured decision.

- **`packets/_ack.py`'s eight decoders.** Structurally identical, deliberately
  so: they are what the engine binds and calls per acknowledgement, and their
  helper-free shape is what
  [`ACK-SPECIALIZED-PRIMITIVES.md`](ACK-SPECIALIZED-PRIMITIVES.md) measured at
  1.1654x on a full engine cycle. The *encoders* in the same file were collapsed
  precisely because the engine cannot reach six of them — the directional
  sessions write success acknowledgements from four-byte literals, and the only
  encoder the engine reaches is PUBREL, for an orphan PUBREC. The module
  docstring now records that asymmetry.
- **`_publish.py`'s two PUBLISH encoders**, ~40 duplicated lines. That
  duplication is what removes the per-publish protocol branch from the hottest
  encoder in the library.
- **`_rollback`'s optional `effect_start` / `queued_start` /
  `packet_ids_empty_start`.** Always passing them would add two attribute reads
  to the per-message path; a single publish cannot leave an effect or a `_queued`
  entry behind, so only a chunk needs them.
- **The epoch checks in `_apply_effect_inline`** are unreachable from their
  production callers, but pinned by `test_batched_message_effects.py` and
  load-bearing for epoch discipline.
- **`buffer.py`'s view-copy threshold**, `memory.py:206`'s dict rebuild,
  `payload` declared last in the SQLite schema, and the deliberate absence of a
  `seq` index — all four carry a measurement in their comment.
- **The "compatibility views".** Contrary to what the name suggests, almost all
  of them are used by the tests, the fuzzer or the benchmarks. Only two setters
  were genuinely dead. Do not remove the rest on the strength of the wording.
- **`EngineEffect.requires_delivery_mark`'s tri-state.** `None` is unreachable
  through `engine._emit`, whose default is `False`, but roughly fifteen tests
  construct `EngineEffect` directly and rely on the conservative behaviour.

## Not applied

Real findings, each deserving its own change rather than a place in a
simplification pass.

- **`transport/websocket.py:497`** — `del buf[:total]` per frame is an O(n)
  memmove on the read path, while `codec/buffer.py` solved exactly this with a
  read offset and bounded compaction. `_parse_frame` is imported by four
  test/fuzz modules, so the signature change needs its own budget. The
  per-byte generator unmask two lines below could reuse `_mask_payload`'s
  translate tables; client-side it only runs in tests and fuzzing.
- **Generating the eight ACK decoders from closure factories** would preserve
  the measured property (one direct call, no generic helper) and take ~160 lines
  to ~40, at the cost of greppable `def decode_pubrec_v5` and readable
  tracebacks. A judgement call, not a perf question.
- **`DELETE … RETURNING logical_size`** would fuse the `SELECT` + `UPDATE`/
  `DELETE` of each acknowledgement transition and make the `seq` re-check
  unnecessary, but needs SQLite ≥ 3.35 with a fallback: *more* code, not less.
  A perf item, not a simplification.
- **Inlining the queue-state reads in `WritePump.try_enqueue`**, and skipping
  the `Condition` when `waiters == 0`. Both are real, and both depend on the
  simultaneity of the five eager-path conditions that invariant §1 rests on.
  They want their concurrency argument written down first.
- **The twin size validations per publish** (`_check_publish_wire_size` then
  `_check_outbound_size`) are only removable under a negotiated limit.
- **`WritePump`'s `item_size` recomputation in the batch epilogue** was
  considered and rejected: moving it into the collection loop changes which loop
  calls it, not how many times. Only threading the size through the queue would
  save anything, and
  [`PERFORMANCE-AUDIT-0.2.0b4.md`](PERFORMANCE-AUDIT-0.2.0b4.md) already
  declined that.
- **`AsyncClient._apply_effect`'s `nowait` parameter** is never `True` in
  production — `EffectPump.drain(nowait=True)` does not forward it — which also
  makes `WritePump.enqueue`'s `FlowControlError` branch and one
  `except FlowControlError` unreachable. Removing it touches ~20 test call sites
  for no runtime gain, so the coherent parameter chain was left alone.

## Open questions

Neither was decided here.

**MQTT 3.1.1 silently drops MQTT 5 properties.**
`encode_publish_item_v311` accepts `properties` and ignores them, as do
`encode_subscribe_v311` and `encode_unsubscribe_v311` — while
`encode_disconnect_v311` and `encode_puback_v311` *raise* in the same situation,
and `CLAUDE.md` states "No silent degradation". Either the publish/subscribe
paths should raise, or the exception should be documented. If it must stay
silent for hot-path reasons, the check belongs at admission in `outbound`, not
in the encoder.

**Three Stable-tier enum members are never referenced**:
`OutboundQoSState.DONE`, `InboundQoSState.DONE`, `ConnectionState.RECONNECTING`.
They were deliberately **kept**. The two `DONE` values are hydration-reachable:
`_row_to_out`/`_row_to_in` do `OutboundQoSState(int(row["state"]))`, so a
third-party store that persisted a terminal state would start raising
`ValueError` at hydration. And `RECONNECTING` reads as an unimplemented state
rather than dead code — `AsyncClient` never enters it, which is arguably the
gap. Both deserve a decision in `docs/API-STABILITY.md`, not a removal in a
simplification pass.

## Unrelated defect found while measuring

`benchmarks/check_memory_thresholds.py` **fails on `32e99a4` itself**, before
any change in this audit:

```
- paho_saturation_4k: logical accepted_messages is 5000, expected 512
- paho_saturation_4k: logical rejected_messages is 0, expected 4488
```

The scenario now accepts every message instead of rejecting 4 488 of them, which
is consistent with the compat publish-handoff work that stopped blocking the
producer thread on QoS 1/2 admission
([`COMPAT-PUBLISH-HANDOFF-2026-08-16.md`](COMPAT-PUBLISH-HANDOFF-2026-08-16.md)).
The expectations in `memory_profile.py` were not updated with it, so the gate in
`.github/workflows/benchmarks.yml` is red for a reason unrelated to memory. It
is in the Paho facade, out of this audit's scope, and left untouched.

Separately, `paho_saturation_4k`'s traced peak varies between 2.45 and 3.60 MiB
run to run against a 3.50 MiB limit, on both trees — so that threshold is
occasionally breached by noise alone.
