# Formal lifecycle-hook / reconnect ownership audit — 2026-09-24

## Exact baseline

`main@5774db38b94615417f2c3c6106254429eb52b6f8`

This report addresses issue #508.

The concrete reproductions were run against the exact `1.0.0rc15` source
distribution produced by CI for that SHA.

## Wait-for cycle

Automatic reconnect currently waits for `LifecycleHooks._reconnect_ready`.

A disconnect notification clears that event, and the lifecycle worker restores
it only after the user `on_disconnect` hook has returned.

That is safe only if the hook never waits for work whose completion requires
reconnect.

But durable offline QoS publication is a supported composition:

```python
async def on_disconnect(exc):
    receipt = await client.publish("offline", b"x", qos=1)
    await receipt.wait()
```

The resulting wait-for graph is:

```text
on_disconnect
  -> QoS receipt completion
  -> replacement broker ACK
  -> automatic reconnect
  -> _reconnect_ready
  -> completion of on_disconnect
```

So rc15 creates a cycle.

## Exact rc15 behavior

Successful-reconnect scenario:

```text
on_disconnect entered
offline receipt obtained
replacement transport factory calls = 0
hook waits forever
client remains disconnected
```

The reconnect loop has not even attempted the second transport.

Exhaustion scenario:

```text
on_disconnect entered
offline receipt obtained
retry factory calls = 0
receipt never terminalized
stream remains reconnect-preserved/open
hook remains pending
```

## Candidate ownership rule

Transport recovery and lifecycle notification serialization are distinct
owners.

The candidate therefore opens reconnect admission only when the disconnect
notification has:

1. been selected by the lifecycle worker;
2. passed its token check;
3. reached the point immediately before the user callback is invoked.

At that point the notification is no longer merely pending state that can be
overwritten. The running hook is retained as the lifecycle owner.

Automatic reconnect calls:

```python
begin_operation(preserve_hook=True)
```

which advances connection ownership without cancelling the running
`on_disconnect`.

A successful reconnect can then enqueue an `on_connect` notification, but the
same lifecycle worker remains blocked on the disconnect hook task. Therefore
`on_connect` stays serialized behind `on_disconnect`.

Explicit application takeover is unchanged: ordinary
`begin_operation(... preserve_hook=False)` still cancels obsolete external hook
work, while a connect/disconnect called directly from the running hook retains
its existing origin semantics.

## Successful reconnect reproduction

Candidate:

```text
factory calls = 2
events:
  disconnect-enter
  receipt-obtained
  disconnect-exit
  connect

replacement broker PUBLISH count = 1
client connected = True
```

The second broker ACK settles the offline receipt. Only then does the disconnect
hook return; the queued on_connect callback runs afterwards.

## Exhaustion reproduction

With two permitted retry attempts and a replacement factory that always raises
one `ConnectionRefusedError`:

```text
factory calls        = 3   # initial + 2 retries
receipt wait outcome = same ConnectionRefusedError
disconnect hook      = finished
delivery.closed      = True
reconnect_task       = None
```

So terminal retry policy can now settle the very receipt that the hook is
awaiting.

## Formal model

`formal/mqtt/LifecycleReconnectHook.tla` separates:

- disconnect hook state;
- reconnect admission gate;
- reconnect supervisor state;
- durable receipt state;
- pending/running on_connect notification.

Safety invariants:

```text
no hook -> receipt -> reconnect -> hook ownership cycle

on_connect cannot run before the active on_disconnect hook completes
```

Independent bounded executable exploration of the same abstraction:

```text
rc15:
  states       = 3
  transitions  = 2
  violation    = yes
  shortest     = loss -> hook_publish_wait

candidate:
  states       = 9
  transitions  = 8
  violation    = none
```

The TLA+ specification and rc15/fixed configs are committed for auditability.
TLC itself was not executed in the original analysis environment; no TLC-proof
claim is made.

## Neighboring lifecycle contracts

This change must preserve the extensive lifecycle work already present in the
repository:

- external connect/disconnect supersedes obsolete hook work;
- connect/disconnect directly invoked by the active hook preserves that hook;
- automatic reconnect stream generation remains continuous;
- on_connect/on_disconnect user callbacks remain serialized;
- lifecycle callbacks still run outside protocol/engine/effect locks.

The candidate does not move user callbacks onto the transport path and does not
make transport operations await hook completion.

## Required gates

Before merge:

- successful durable receipt wait from on_disconnect;
- reconnect exhaustion unblocks/terminalizes the same receipt;
- existing callback lifecycle regression suite;
- cancelled explicit takeover tests;
- automatic reconnect stream continuity;
- Python/cross-platform/fuzz/resilience/Linux soaks;
- no throughput claim needed: lifecycle-only path.

No public API change.
