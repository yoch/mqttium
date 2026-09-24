# Formal eager WritePump failure ownership audit — 2026-09-24

## Stack / exact baseline

This change addresses issue #504 and is intentionally stacked on the narrow
#509 cancellation-owner fix.

Repository baseline beneath the stack:

`main@5774db38b94615417f2c3c6106254429eb52b6f8`

The concrete rc15 and candidate reproductions were run from the exact
`1.0.0rc15` CI source distribution for that main SHA.

## Failure boundary

The optional transport `write_nowait()` may fail synchronously on the
producer/EffectPump stack.

A thrown exception is wire-ambiguous: the transport may have exposed no bytes,
a prefix, or the full frame before raising. Therefore the frame must never be
retried merely because local accounting can be restored.

At the same time, returning the exception while keeping the writer generation
live is unsafe: another producer can continue to use the same failed transport.

The ownership rule is therefore:

```text
eager write exception
  -> failed generation fenced before returning to producer
  -> ambiguous frame retained only as writer ownership/accounting
  -> existing writer observes the latched failure
  -> frame is retired without wire retry
  -> existing on_failure/lifecycle teardown runs
```

## rc15 behavior

For a transport whose `write_nowait(PUBLISH)` raises a controlled `OSError`:

QoS0/QoS1/QoS2, both `publish()` and `publish_nowait()`:

```text
caller outcome       = original OSError
client.is_connected  = True
engine.state          = CONNECTED
writer epoch          = unchanged
transport             = still installed
disconnect_exc        = None
```

For QoS1/2 the caller has not received its newly-created receipt but rc15 still
retains its internal protocol ownership.

No awaited PUBLISH write occurred; the one eager attempt is the only possible
wire exposure.

## Candidate

The writer gains one internal helper that reuses the already-established
latency-failure retirement policy:

1. store the original exception in the existing failure latch;
2. drop the eager binding;
3. increment writer epoch synchronously;
4. enqueue the ambiguous bytes as an ownership-only marker;
5. charge normal writer accounting for that marker;
6. re-raise the original exception to the producer.

The existing writer task wakes on the marker. Before any transport write it
sees the failure latch, releases the marker accounting in its normal `finally`,
invalidates/wakes as required, and calls the existing `on_failure` path.

No new failure task or fire-and-forget lifecycle owner is introduced.

## Full-client candidate evidence

For QoS 0/1/2 × async/nowait:

```text
caller outcome       = same original OSError
client.is_connected  = False
engine.state          = DISCONNECTED
disconnect_exc        = same OSError object
writer/connection epoch aligned after teardown
writer queue/resident/bytes = 0
receipt registry      = empty
eager PUBLISH attempts = 1
awaited PUBLISH writes = 0
```

The protocol engine can retain replayable session ownership after unexpected
connection loss according to its existing session policy; this PR's guarantee
is that no unreachable writer/receipt owner or retry of ambiguous bytes remains.

## Success-ACK control

A PUBACK `write_nowait()` failure was also reproduced.

That exception already originates on the reader-owned ingress stack, so rc15
eventually closes the connection through reader failure handling. The candidate
preserves that outcome while ensuring the WritePump's own generation/binding is
also fenced by the same mechanism.

Regression requires:

```text
one eager PUBACK attempt
zero awaited PUBACK retries
same original failure as disconnect cause
```

## Immediate fence test

A direct WritePump regression asserts the strongest synchronous boundary:

immediately after `try_enqueue()` raises from `write_nowait()`, before any
event-loop turn:

- writer epoch has advanced;
- eager binding is gone;
- failure latch is the same exception object;
- ambiguous item is queued/resident exactly once;
- another enqueue is rejected as stale.

After the writer runs, the marker is retired without an awaited transport write
and `on_failure` sees the same exception.

## Formal model

`formal/mqtt/WritePumpEagerFailure.tla` tracks:

- protocol commitment;
- producer-visible failure;
- generation validity;
- eager transport binding;
- ownership-only marker;
- writer liveness;
- failure reporting;
- connection liveness;
- receipt registration;
- forbidden wire retry.

Safety invariants:

```text
producer-visible eager failure
=> generation invalid AND eager binding removed

ownership-only marker
=> never retried on wire
```

Independent bounded exploration of the same state machine, depth 5:

```text
rc15:
  states       = 6
  transitions  = 9
  violations   = 4
  shortest     = eager_fail

candidate:
  states       = 8
  transitions  = 11
  violations   = 0
```

TLA+ configs for rc15 and the candidate are committed. TLC was not executed in
the original analysis environment; the report does not claim otherwise.

## Scope

This stack depends on the separate #509 writer cancellation classification.
The eager failure mechanism itself handles arbitrary `BaseException`, including
a dependency-originated `CancelledError`; #509 determines how the writer task
later classifies cancellation when it consumes the latch.

Before merge/rebase, qualify #509 first, then retarget this PR onto its merged
result (or rebase both onto current main).

## Required gates

- all 6 DATA cases (QoS0/1/2 × async/nowait);
- success-ACK eager failure;
- immediate generation-fence unit test;
- existing eager ordering/rearm/burst tests;
- existing latency failure tests, including CancelledError;
- writer failure-liveness tests;
- full unit/project, cross-platform, fuzz, resilience, Linux soaks;
- performance gate: no success-path change beyond the exception wrapper around
  the existing `write_nowait` call.
