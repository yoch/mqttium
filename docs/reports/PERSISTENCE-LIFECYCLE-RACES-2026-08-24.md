# Persistence / lifecycle races — 2026-08-24

| | |
| --- | --- |
| Date | 2026-08-24 |
| Tree audited | `a336f83` (`Prepare 1.0.0rc9`, `origin/main`) |
| Package | mqttium 1.0.0rc9 |
| Environment | CPython 3.12.3, Linux |
| Scope | Durable outbound/inbound records, packet-id ownership, logical pending accounting, `session_present` true/false, cancellation/teardown, injected store failures, shutdown, inbound replay vs ACK |
| Out of scope | Sequential stateful fuzz (`tests/fuzz/test_stateful_invariants.py`), security fuzzing, arbitrary threads |
| Tests | `tests/unit/test_persistence_lifecycle_races.py` (41 passed) |

This is dated evidence, not a product contract. It records what was true of
`a336f83` when the schedules below were executed. Do not rewrite it to match
later code; add a new report.

## Method

mqttium's engine is loop-confined. The investigation did not introduce
threads. The interleavings that exist in the real runtime are:

1. **Effect-pump yield.** Inbound restart redelivery emits one bounded MESSAGE
   batch and an `CONTINUE_INBOUND_REPLAY` effect. The adapter re-enters
   `ProtocolEngine.continue_inbound_replay()` after applying that batch. Any
   `handle_raw` (PUBREL, PUBACK, `ack()`, transport close) that runs between
   those entries is a legitimate schedule.
2. **Delivery await.** `AsyncClient` drops `_engine_lock` while waiting on
   MESSAGE delivery, then re-acquires it to `mark_inbound_delivered`. A reader
   can complete the same mid under the lock in that window.
3. **Store paging.** Both stores snapshot identifiers, then look rows up. A
   delete between those two steps is a hole, not a resurrection.
4. **Fault injection** around store methods (`complete_out`, `transition_out`,
   `get_out`) on otherwise sequential engine calls.

The sequential fuzzer already checks Memory vs SQLite observational equality
and engine invariants after every *completed* operation. It never yields
between inbound replay batches, never wraps store methods, and never races a
cursor-held copy against a later ACK.

## Answers to the charter questions

| Question | Result at `a336f83` |
| --- | --- |
| Can cancellation or teardown land between two ownership transitions and leave store, packet-id pool and counters disagreeing? | **Not with the built-in stores.** Admission is transactional; `complete_out` / `transition_out` that *raise before mutating* leave ownership unchanged. A wrapper that mutates then raises is a store-contract violation the engine does not unwind. Late CONNACK after `notify_transport_closed` is ignored. |
| Can replay race an ACK/completion and resurrect a record, send twice incorrectly, double-release a packet id, leak capacity, or leave a receipt unresolved? | **Store rows are not resurrected.** Packet ids are not double-released (release is idempotent; budget is not). Receive Maximum / pending bytes follow the store. **The inbound replay cursor can still MESSAGE a protocol-completed mid** (application-visible resurrection). **Terminal `_fail_pending` settles receipts without purging durable outbound**; a later `session_present=true` SEND has no live receipt. |
| Can SQLite and Memory differ under the same lifecycle schedule even when sequential API behaviour agrees? | **Yes, at the cursor/page boundary.** Memory `in_replay_pages` yields live record objects; SQLite yields snapshots. `mark_inbound_delivered` on a mid the cursor already hydrated skips on Memory and still emits on SQLite. PUBREL-between-batches ghosts on **both**, because the cursor copy is not refreshed. |

## Classification

### Store implementations (both Memory and SQLite)

These paths agree. Defects here would be store bugs; none were found in the
schedules below.

- Identifier snapshot then lookup omits rows deleted in between
  (`in_replay_pages`, `out_summary_pages`).
- `complete_out` with a mismatched expected state is a no-op; the row stays.
- Hydration + `session_present` true/false + PUBACK + PUBREL + close +
  `session_present` false produce identical ownership snapshots.

### Runtime integration (engine cursor, AsyncClient)

These are not store bugs. Sequential store APIs and the sequential fuzzer do
not see them.

## Findings

### R1 — inbound replay cursor emits a protocol-completed record (runtime)

**Severity.** High for application-visible QoS 2 correctness. Low for durable
state: the store row stays deleted, Receive Maximum and pending bytes follow
the store.

**Schedule.** 100 inbound WAIT_PUBREL rows. CONNACK `session_present=true`
emits mids 1..64 (`REPLAY_BATCH_MESSAGES`) and `CONTINUE_INBOUND_REPLAY`.
The first store page is larger than one effect batch, so mid 70 is already an
`InboundMessage` in `InboundReplayCursor` (`chain.from_iterable` over pages).
PUBREL 70 deletes the durable row and releases the slot. The next
`continue_inbound_replay()` still MESSAGE-emits mid 70.

**Cause.** `_should_redeliver` trusts the cursor copy (`delivered=False`) and
does not re-read the store:

```958:968:src/mqttium/protocol/inbound.py
    def _should_redeliver(self, inbound: InboundMessage) -> bool:
        if not inbound.delivered:
            return True
        ...
```

`transport_closed` does not clear `_replay`; only `start_connection` /
`discard_session` do. `AsyncClient` applies CONTINUE under `_engine_lock`, so
PUBREL cannot run *inside* `drain_replay`, but it **can** run between batches.

**Not a send-twice on the wire for outbound.** Outbound `replay_session()` is
synchronous on CONNACK; there is no yield a PUBACK can sneak into without a
store wrapper re-entering the engine.

**Pin.**
`test_inbound_replay_cursor_emits_a_pubrel_completed_record_from_its_stale_copy`
(characterization: 70 is in later MESSAGE effects; `get_in(70) is None`;
inflight/bytes still match the store).

### R2 — CONTINUE after transport close still drains the engine cursor (runtime)

**Severity.** Medium for direct `ProtocolEngine` consumers. `AsyncClient`
drops CONTINUE when the effect epoch does not match `_connection_epoch`.

`notify_transport_closed` → `inbound.transport_closed()` does not set
`_replay = None`. `continue_inbound_replay()` after close still emits the old
cursor. The next `begin_connect()` / `start_connection()` abandons it.

**Pin.**
`test_continue_after_transport_close_without_new_connect_still_drains_the_cursor`,
`test_stale_continue_inbound_replay_effect_does_not_reenter_the_engine`.

### R3 — Memory vs SQLite diverge on `mark_inbound_delivered` of a cursor-held mid (runtime × store shape)

**Severity.** Medium as a portability hazard for engine-level interleaving.
`AsyncClient` only marks after MESSAGE apply, so mid 70 is not marked before
it is emitted on the normal adapter path.

Memory `in_replay_pages` appends the live dict value. SQLite hydrates a new
object from the row. `mark_inbound_delivered(70)` between batches sets
`delivered=True` on the Memory object the cursor still holds (skip) and
leaves the SQLite copy at `delivered=False` (emit).

**Pin.**
`test_mark_delivered_of_a_cursor_held_mid_diverges_memory_vs_sqlite`.

### R4 — terminal receipt failure does not purge durable outbound (runtime, intentional split)

**Severity.** Low if reconnect is expected to replay. Application-visible:
`wait()` on the receipt raises, then a later `session_present=true` SEND has
`_pop_publish_receipt` return `None` — no second settlement.

`_fail_pending` settles every receipt and clears the map. It does not delete
store rows or release packet ids. That split is what lets a persistent
session resume; it also means a caller that treated the failed receipt as
terminal can see a retransmission with no handle.

**Pin.**
`test_fail_pending_settles_receipts_without_purging_durable_outbound`.

### R5 — `complete_out` that deletes then raises is not compensated (store contract)

**Severity.** Low for built-in stores (they do not do this). High for a
third-party store that violates atomicity.

The engine treats a raising `complete_out` as failure
(`PROTOCOL_ERROR` / internal handler error) and does **not** unwind packet id
or pending bytes. If the wrapper already deleted the row, store and engine
ledgers disagree.

**Pin.**
`test_complete_out_after_mutation_raise_is_a_store_contract_violation`,
`test_puback_store_failure_does_not_release_ownership` (raise-before-mutate
keeps ownership).

## Invariants that held

| Invariant | Evidence |
| --- | --- |
| Every durable outbound row has the matching packet id | `assert_outbound_ownership` after hydrate, resume true/false, PUBACK, duplicate PUBACK, close, injected raise-before-mutate |
| Packet ids released exactly once for engine-visible completions | Duplicate PUBACK is a no-op; `PacketIdPool.release` is idempotent |
| Pending logical accounting matches durable ownership | Outbound messages/bytes; inbound `_pending_bytes` vs `in_items()` after PUBREL, prefix manual ack, close, `session_present` false |
| No completed transaction is resurrected **in the store** | PUBREL/ack/complete delete stays deleted across later CONTINUE |
| No unowned row survives `session_present=false` purge | WAIT_* outbound failed + deleted; inbound cleared; QUEUED outbound kept with matching id/bytes |
| Replay across session resume is semantically idempotent **from the store** | Second `session_present=true` retransmits the same mids without duplicating ownership; reconnect during partial inbound replay starts a fresh store-backed cursor (`start_connection` clears `_replay`) |
| Built-in stores agree on a shared lifecycle schedule | hydrate → resume true → PUBACK → PUBREL → close → resume false |

MQTT-4.6.0-2 is preserved: `ack(70)` while 1 is still the QoS 1 prefix does
not pop 70. Acking the prefix 1..70 between batches releases capacity once
even if the cursor later MESSAGE-emits 65..70.

Outbound vanish-on-`get_out` during CONNACK replay fails that mid once,
releases the id and budget, and leaves ownership empty.

## What the sequential fuzzer cannot see

- CONTINUE yield + `handle_raw(PUBREL)` / `ack()` on a mid already in the
  cursor but not yet emitted.
- `notify_transport_closed` without a following `begin_connect`.
- Store wrappers that raise after mutation.
- Memory live-object aliasing vs SQLite snapshots under `mark_inbound_delivered`.
- AsyncClient epoch vs engine cursor after teardown.
- Receipt map vs durable outbound after `_fail_pending`.

## Recommendations (not applied)

Investigation only; this tree does not change `src/`.

1. **R1/R3.** Make `_should_redeliver` consult the store (or a live mid set)
   before emitting, or materialise copies and drop mids whose `get_in` is
   `None`. That closes PUBREL-between-batches and aligns Memory with SQLite.
2. **R2.** Clear `_replay` in `transport_closed` as defence in depth for
   direct engine consumers. `AsyncClient` already has the epoch guard.
3. **R5.** Document that `complete_out` / `transition_out` / `complete_in`
   must be atomic: raise-without-mutation or commit. The engine will not
   compensate a split brain.
4. Keep the new tests. They are deterministic and cheaper than extending the
   sequential fuzzer with cursor-level yields.

## Limitations

- No live broker. Schedules are engine/store/AsyncClient-internal.
- No multi-threaded store access; sqlite's lock is not the race under study.
- Outbound ACK-during-replay was only forced via `get_out` wrappers.
  Legitimate asyncio cannot interleave PUBACK with synchronous
  `replay_session()`.
- Receipt/reconnect coverage is `_fail_pending` plus a later engine resume,
  not a full reconnect state machine with a transport.
- `docs/reports/` is historical evidence; current behaviour is the code and
  these tests.

## Commands

```bash
python -m pytest -q tests/unit/test_persistence_lifecycle_races.py
```

41 passed at the date of this report against `a336f83` plus the tests in this
change.
