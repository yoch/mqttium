# Formal reconnect cancellation ownership audit — 2026-09-24

## Exact baseline

`main@5774db38b94615417f2c3c6106254429eb52b6f8`

This report addresses issue #510: a dependency-originated
`asyncio.CancelledError` can currently terminate the automatic reconnect
supervisor as if the reconnect task itself had been cancelled.

Concrete baseline/candidate reproductions were run from the exact
`1.0.0rc15` CI source distribution for that SHA.

## Ownership distinction

A reconnect attempt may observe `CancelledError` from two owners:

1. **reconnect-task cancellation** — e.g. public `disconnect()` takes over and
   calls `reconnect_task.cancel()`;
2. **dependency-originated cancellation exception** — transport factory/connect
   code raises `CancelledError` while `asyncio.current_task().cancelling()==0`.

Only case 1 may leave the retry loop through its outer
`except asyncio.CancelledError`.

Case 2 is an attempt failure and must go through the same retry/permanent/
exhaustion policy as another dependency failure.

## rc15 defect

The inner reconnect-attempt handler catches only `Exception`:

```python
try:
    ...
    connack = await self._connect_once_locked(...)
except Exception as exc:
    ...
```

`CancelledError` therefore bypasses that policy and reaches:

```python
except asyncio.CancelledError:
    raise
```

The supervisor exits and its `finally` clears `_reconnect_task`, but the
application stream was deliberately kept open for automatic reconnect.

Observed with `max_retries=2`:

```text
factory calls       = 2   # initial + only one retry
reconnect_task      = None
connected           = False
delivery.closed     = False
reconnect attempt   = 1
disconnect cause    != dependency CancelledError
stream              = still open
```

There is now no supervisor capable of reconnecting or terminalizing that
preserved stream.

## Candidate

Catch `CancelledError` at the attempt-owner boundary:

```python
except (Exception, asyncio.CancelledError) as exc:
    if isinstance(exc, asyncio.CancelledError):
        task = asyncio.current_task()
        if task is None or task.cancelling():
            raise
    self._disconnect_exc = exc
    ... existing retry/terminal policy ...
```

A dependency cancellation is therefore a normal retryable setup failure unless
another existing policy classifies it terminal.

With the same reproduction:

```text
factory calls       = 3   # initial + two allowed retries
reconnect_task      = None
connected           = False
delivery.closed     = True
reconnect attempt   = 2
disconnect cause    = exact dependency CancelledError object
stream              = terminal
```

## Owner-cancellation negative control

A second full-client case blocks the replacement transport factory and then
calls public `disconnect()`.

That call really cancels the reconnect task. The candidate observes
`task.cancelling() > 0` and propagates cancellation to the lifecycle owner.

Observed:

```text
factory calls       = 2
extra retry         = none
reconnect_task      = None
delivery.closed     = True
connected           = False
```

## Formal model

`formal/mqtt/ReconnectCancellation.tla` separates:

- reconnect supervisor alive/dead;
- explicit cancellation request;
- connection state;
- application stream state;
- retry attempt count;
- one active attempt;
- last dependency failure;
- terminalization.

Safety invariant:

```text
a reconnect supervisor must not disappear while
the client is disconnected and the reconnect-preserved stream remains open
unless another owner has terminalized/replaced it
```

An independent bounded executable model of the same ownership rule was explored
with two retries:

```text
rc15:
  states       = 9
  transitions  = 9
  violation    = yes
  shortest     = attempt -> dependency_cancel

candidate:
  states       = 19
  transitions  = 22
  violation    = none
```

The TLA+ specification and rc15/fixed configs are committed as audit artifacts.
TLC itself was not executed in the original environment; this report does not
claim a TLC proof.

## Scope / neighboring owners

This finding is the reconnect-supervisor analogue of #509, but it is not the
same owner or teardown path:

- #509: writer task dies and leaves a live writer/connection generation;
- #510: reconnect task dies and leaves a preserved application stream without a
  supervisor.

The fixes therefore remain independent.

## Required gates

Before merge:

- full-client dependency-cancellation reproduction;
- real public-disconnect cancellation negative control;
- existing transient reconnect/backoff tests;
- permanent connection failure tests;
- reconnect message-stream continuity tests;
- full Python/cross-platform/fuzz/resilience/Linux-soak matrix;
- no change to retry counts/backoff for ordinary Exception failures.

No public API change.
