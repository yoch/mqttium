# Formal WritePump cancellation ownership audit — 2026-09-24

## Exact baseline

`main@5774db38b94615417f2c3c6106254429eb52b6f8`

The full-client reproduction was run against the exact `1.0.0rc15` source
distribution emitted by CI for that SHA.

This report addresses issue #509 only. The synchronous eager `write_nowait()`
failure path is tracked separately by #504.

## Ownership distinction

`asyncio.CancelledError` does not by itself identify the owner of
cancellation.

Two cases are materially different:

1. **lifecycle-owned cancellation** — the writer task has an actual cancellation
   request (`writer_task.cancelling() > 0`);
2. **dependency-originated cancellation exception** — the transport's
   `await write(...)` raises `CancelledError` while the writer task has no
   cancellation request.

Only case 1 may terminate the writer as ordinary lifecycle cancellation.
Case 2 is a transport failure and must retire the writer generation exactly like
another transport exception.

## rc15 defect

Current rc15 has:

```python
if isinstance(exc, asyncio.CancelledError) and (
    writer_task.cancelling() or self._latency_failure is None
):
    raise
failure = exc
```

For an ordinary transport-originated `CancelledError`:

```text
writer_task.cancelling() == 0
_latency_failure is None == True
```

so the exception is re-raised as if lifecycle owned it.

The writer task dies without:

- dropping/invalidation of the failed writer generation;
- `on_failure`;
- reader/transport teardown;
- receipt settlement.

## Full-client reproduction

A complete `AsyncClient` connects normally through a test transport. Its first
PUBLISH write raises one pre-created:

```python
asyncio.CancelledError("transport self-cancel")
```

Nobody calls `Task.cancel()` on the writer.

Observed rc15:

```text
publish_attempts  1
writer_done       True
writer_cancelled  True
connected         True
engine_state      CONNECTED
disconnect_exc    None
receipt_done      False
receipt_registry  [1]
receipt_wait      TimeoutError
```

The client remains publicly connected while its writer task is gone.

Candidate:

```text
publish_attempts  1
connected         False
engine_state      DISCONNECTED
disconnect_exc    same CancelledError object
receipt_done      True
receipt_registry  []
receipt_wait      same CancelledError object
```

## Candidate

The classification becomes:

```python
if isinstance(exc, asyncio.CancelledError) and writer_task.cancelling():
    raise
failure = exc
```

The normal existing writer-failure path then:

1. drops eager transport ownership;
2. invalidates the writer epoch;
3. wakes admission waiters;
4. calls `on_failure`;
5. lets `AsyncClient` close/cancel the reader-owned connection;
6. settles outstanding receipts through normal teardown.

No new failure mechanism is introduced.

## Negative controls

The focused branch test verifies that an actual `WritePump.stop()` cancellation
still produces no `on_failure`.

The pre-existing
`tests/unit/test_write_pump_latency_failure.py` already parameterizes
`asyncio.CancelledError` for the latency-failure latch. That path remains a
transport failure and therefore exercises the opposite branch.

## Formal model

`formal/mqtt/WritePumpCancellation.tla` distinguishes:

- writer task alive/dead;
- active write ownership;
- task cancellation request;
- writer generation validity;
- failure notification;
- pending QoS receipt;
- connection liveness;
- already-latched latency failure.

Safety invariant:

```text
a writer that dies from a dependency-originated failure
must not leave its connection generation live/admissible
```

An independent bounded executable version of the same state machine was explored
to depth 6:

```text
rc15:
  states       = 17
  transitions  = 34
  violations   = 1
  shortest     = commit -> take -> transport_cancel

candidate:
  states       = 18
  transitions  = 36
  violations   = 0
```

The TLA+ specification and two configs are committed for auditability. TLC was
not executed in the original environment because the TLA+ tools jar was not
available there; this report does not label the TLA+ artifact as a TLC proof.

## Acceptance / merge gates

Before merge, require:

- full AsyncClient reproduction in CI;
- true writer-task cancellation negative control;
- existing latency-failure `CancelledError` coverage;
- writer failure liveness / stale epoch tests;
- full unit/project, cross-platform, fuzz, resilience and soak gates;
- no behavior coupling with #504's synchronous eager producer-stack failure.

The runtime change is intentionally one ownership predicate.
