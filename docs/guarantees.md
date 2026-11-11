# Behavioural guarantees

This page states what an application can rely on from the running client:
which waits are bounded, by what, and what is observed when a bound is
missed. It complements [Protocol conformance](conformance.md), which is about
the wire.

Each guarantee has a precondition, a status and its evidence. A guarantee
without executable evidence is not claimed; it is listed as **Open** so the
gap stays visible.

| Status | Meaning |
| --- | --- |
| **Checked** | Regression tests assert it, and a failure of the guarantee fails CI |
| **Partial** | Holds under the stated precondition only, or for part of the surface |
| **Open** | Believed or intended, but not yet backed by executable evidence |

`tests/project/test_guarantee_register.py` checks that every guarantee below
names evidence that exists, so an entry cannot outlive its tests.

Models under `formal/models/` check safety invariants and the absence of
stuck states in an abstraction of the code. They are cited as supporting
evidence, never as proof of the Python implementation; see
[Formal models](formal-models.md).

## Liveness

### G1 — No wait cycle inside the library

**Status:** Partial.

No task of the client waits, directly or through a lock, on a task that waits
on it. The engine lock is never held across a suspension, so it is never
observed held; the writer, keepalive, effect and authentication tasks cancel
the reader and never join it.

*Precondition:* none. *Limit:* the argument is by construction and by the
checks below. Temporal properties are model-checked for three waits only (the
consumer's receipt, the reader's reconnect decision, the retry after a lost
replacement); the others are not modelled.

- `tests/project/test_engine_lock_discipline.py`
- `tests/project/test_cancellation_discipline.py`
- `formal/models/ConnectionRetirement.tla`
- `formal/models/CancellationOwnership.tla`

### G2 — A lost connection ends every pending wait

**Status:** Partial.

Once the connection is lost and will not be re-established, parked
publishers, pending `subscribe`/`unsubscribe` calls, receipts and `messages()`
iterators end with the connection's cause. With a reconnect policy that will
retry, QoS 1/2 receipts and `messages()` stay pending for the replacement
connection by design.

*Precondition:* the loss is detected. A peer whose host still answers TCP
while its application has stopped is detected only by MQTT keepalive, so this
bound needs `keepalive > 0`: detection takes at most `keepalive` plus
`ping_timeout`. With `keepalive=0` only TCP keepalive remains (about 90
seconds, for a vanished host or a cut path, TCP transports only).

- `tests/unit/test_keepalive.py`
- `tests/unit/test_keepalive_real_world.py`
- `tests/unit/test_tcp_socket_keepalive.py`
- `tests/unit/test_nonreplayable_failures.py`
- `tests/fuzz/test_runtime_fuzzer.py`

### G3 — Application backpressure cannot stall the client silently

**Status:** Checked.

While the `messages()` queue is full the reader reads nothing, so no
acknowledgement or PINGRESP is processed. That state lasts at most
`iterator_admission_timeout` (60 seconds by default): the connection then
fails with `MessageDeliveryError`, which a reconnect policy retries. A
consumer that awaits a receipt inside its own loop is therefore bounded too.

*Precondition:* `iterator_admission_timeout` is not `None`. Callback delivery
has no queue: a blocking callback blocks the event loop and is outside this
guarantee.

- `tests/unit/test_iterator_consumer_deadlock.py`
- `tests/unit/test_delivery_backpressure.py`
- `tests/unit/test_observed_facts_under_backpressure.py`
- `formal/models/ConsumerBackpressure.tla`

### G4 — A timeout argument bounds the whole call

**Status:** Checked.

`connect*(timeout=)`, `subscribe(timeout=)`, `unsubscribe(timeout=)`,
`publish(timeout=)` and a receipt's `wait(timeout=)` end at their deadline
with `MQTTTimeoutError`. A `publish()` that times out before admission admits
nothing; a publication admitted before the deadline is returned, never undone.

*Precondition:* the argument is passed. Without it, `publish()` and receipt
waits last as long as the connection or its reconnect policy, and `connect*()`
with a reconnect policy retries for as long as the policy allows.

- `tests/unit/test_call_deadlines.py`
- `tests/unit/test_connect_call_deadline.py`
- `tests/unit/test_connection_deadline.py`

### G5 — Every receipt settles

**Status:** Partial.

A QoS 1/2 receipt ends acknowledged, failed with the connection's cause, or
replayed on a resumed session. It is never left pending by a connection that
is gone for good, including when `disconnect()` races the reader's teardown.

*Limit:* there is no retransmission within a session. A broker that stays
connected, answers pings and never acknowledges holds the receipt until the
connection ends; bound that case with `wait(timeout=)`.

- `tests/unit/test_disconnect_reader_teardown_race.py`
- `tests/fuzz/test_runtime_teardown_fuzzer.py`
- `tests/unit/test_failed_receipt_seal.py`
- `tests/unit/test_reconnect_receipts.py`
- `tests/unit/test_publish_receipt_waiters.py`
- `formal/models/SealedFailedReceipt.tla`
- `formal/models/ReaderTeardownCancel.tla`

### G6 — The client is never reconnecting with nothing reconnecting it

**Status:** Checked.

`RECONNECTING` implies a reconnect task or a pending retry. A second failure
reported while the reader releases a lost connection cannot end the reader
before it starts the reconnect.

- `tests/unit/test_reader_teardown_second_cancel.py`
- `tests/fuzz/test_runtime_teardown_fuzzer.py`
- `tests/unit/test_disconnect_hook_reconnect_progress.py`
- `formal/models/ReaderTeardownCancel.tla`
- `formal/models/ReconnectStability.tla`
- `formal/models/LifecycleReconnectHook.tla`

### G7 — A background failure surfaces as a disconnect cause

**Status:** Partial.

A failure of the writer, of the keepalive loop or of the effect flush ends
the connection with that failure as its cause, reported through
`on_disconnect`, receipts and `stats().last_disconnect_error`.

*Limit:* an exception raised inside the reader's own teardown is not covered
by a test.

- `tests/unit/test_keepalive_unexpected_failure.py`
- `tests/unit/test_dependency_cancellation.py`
- `formal/models/WritePumpEagerFailure.tla`

## Cancellation

### G8 — Cancelling a public awaitable is honoured and leaves consistent state

**Status:** Checked.

A task cancelled inside `connect*()`, `disconnect()`, `publish()`,
`publish_many()`, `subscribe()`, `unsubscribe()` or a receipt wait ends
cancelled. Before a publication is committed, cancellation admits nothing;
after, the publication stays active and its packet identifier, flow slot and
store row stay owned by the client. A committed `disconnect()` finishes its
shutdown before the cancellation is delivered.

- `tests/unit/test_disconnect_join_cancellation.py`
- `tests/unit/test_disconnect_cancellation.py`
- `tests/unit/test_request_cancellation.py`
- `tests/unit/test_dependency_cancellation.py`
- `tests/unit/test_outbound_transaction.py`
- `formal/models/AdmissionRollback.tla`

## Confinement and blocking

### G9 — Misuse from another event loop is refused

**Status:** Checked.

Every awaitable entry point and `publish_nowait()` raise `RuntimeError` when
called from another event loop than the one that connected the client,
instead of running concurrently with it.

*Limit:* `stats()` and plain attribute reads from another thread are not
checked.

- `tests/unit/test_owner_loop_confinement.py`

### G10 — The library does not block the event loop beyond a stated bound

**Status:** Open.

Intended bound: one reader lot (256 packets or 1 MiB) of synchronous work.
Known exceptions, not yet bounded:

- every `SqliteInflightStore` call is synchronous; a write lock held by
  another connection stalls the loop for up to 0.25 seconds while receiving
  and up to ten seconds in `publish()` (see G13 for what survives it);
- replay of persisted outbound records at reconnect, and store hydration in
  the constructor, are linear in the number of records and do not yield.

## Durability and bounds

### G11 — Persisted QoS state survives a restart

**Status:** Partial.

With a store and `clean_start=False`, outbound QoS 1/2 publications and
inbound exchanges that were persisted are replayed or redelivered after the
process restarts, and an inbound exchange completes only after its delivery
was marked.

*Limit:* covered by restart tests at chosen points; there is no campaign that
kills the process at every statement boundary.

- `tests/unit/test_inbound_recovery.py`
- `tests/unit/test_manual_ack_resume.py`
- `tests/unit/test_resumed_session_replay_conflict.py`
- `tests/unit/test_sqlite_commit_failure.py`
- `formal/models/InboundDeliveryCommit.tla`
- `formal/models/ResumedSessionReplay.tla`

### G12 — Memory is bounded by configuration

**Status:** Partial.

The writer queue, the iterator queue, unacknowledged outbound publications
and inbound in-flight state are bounded by their constructor arguments, and
exceeding an inbound bound ends the connection.

*Limit:* with MQTT 3.1.1 and no explicit `maximum_packet_size`, a single
inbound packet may be as large as the protocol allows (256 MiB), because the
broker cannot be told a limit.

- `tests/unit/test_delivery_byte_budget.py`
- `tests/unit/test_inbound_byte_budget.py`
- `tests/resilience/test_decoder_header_budget.py`
- `tests/resilience/test_hostile_broker.py`
- `formal/models/SendQuotaOwnership.tla`

### G13 — A store lock held by another connection does not end the client

**Status:** Partial.

While receiving, the write lock is reserved before the engine applies a
packet that writes. A refusal drops that packet unapplied: the connection is
lost and retried, the broker resends what was never acknowledged, and nothing
in memory is ahead of the store. A refused `publish()` fails that call only.

*Precondition:* `SqliteInflightStore`; the memory store has no lock.
*Limit:* marking a delivery and `ack()` write outside a reserved lot and
still fail-stop the client when the lock is refused, as does any store failure
that is not a lock refusal.

- `tests/unit/test_sqlite_lock_contention.py`
- `formal/models/IngressLockRefusal.tla`
