# Runtime concurrency fuzzer: generative qualification

- Date: 2026-08-24
- Starting commit: `f27211f`
- Target: real `AsyncClient` runtime schedule interleavings

## Architecture

The target remains one MQTTium-specific harness. A packet-aware transport feeds
the real `AsyncClient`, `WritePump`, `EffectPump`, and `ApplicationDelivery`.
Test-only gates exist at transport write/close, reconnect factory, callback, and
wrapped effect-application boundaries. No production scheduling hook or generic
asyncio scheduler was added.

Generation has two layers:

1. a state-aware grammar chooses only legal connected-state QoS 0/1 work,
   writer/callback gates, releases, and bounded event-loop yields until the exact
   `--steps` budget reserved for the anchor is consumed;
2. one of six short ownership motifs runs: the original writer failure,
   callback cancellation, or explicit reconnect motifs, or an automatic
   reconnect, callback/lifecycle, or EffectPump failing-close motif.

Every application task has an explicit accepted outcome. Unexpected task
exceptions and loop exception-handler contexts fail the seed. A whole-schedule
watchdog converts unbounded liveness loss into the normal JSON failure artifact
and then runs terminal cleanup. Transport artifacts distinguish attempted from
completed writes; epoch isolation uses completed writes only.

## Keepalive teardown cycle

The requested cycle reproduced with a live MQTT 5 reader, negotiated Maximum
Packet Size of one byte, and a due keepalive:

```text
keepalive -> local PINGREQ PacketTooLargeError -> _force_close -> await reader
reader.finally -> cancel/await keepalive
```

The bounded test did not settle. Recursive cancellation eventually raised
`RecursionError`, with each task awaiting the other. The regression test is
`test_impossible_pingreq_teardown_does_not_cycle_reader_and_keepalive`.

The minimal correction leaves the reader as the connection-visible teardown
owner: the keepalive records the terminal packet-size error and closes the
transport, rather than entering `_force_close()`. Reader EOF then performs the
normal epoch invalidation, task settlement, and terminal cleanup. No ordinary
keepalive or writer hot path changed.

## Second production finding: explicit takeover lost after a blocked factory

The generative automatic-reconnect family found another real ownership race.
An explicit `connect()` could wait behind the reconnect factory's lifecycle
lock. If the automatic attempt established a connection before releasing that
lock, `_prepare_explicit_connect()` saw `CONNECTED`, returned early, and the
explicit caller failed with `ProtocolError("Already connected or connecting")`.
The automatic generation therefore defeated an already-declared user takeover.

`test_explicit_connect_waiting_on_reconnect_factory_still_takes_over` reduces
the race to three packet-aware broker generations. The fix treats a live
automatic reconnect task as replaceable even after it reaches CONNECTED,
cancels it, joins the automatically established reader, then cancels any
successor reconnect task that reader teardown created before constructing the
explicit generation. Existing callback takeover and reconnect-sleep takeover
tests remain green.

## Oracles and artifacts

The target now fails on:

- inconsistent writer resident count/bytes or a waiter owned by a dead writer;
- impossible EffectPump progress, non-deterministic failure ownership, or a
  late effect left pending during failing-close;
- outbound ledger/packet-id/flow/receipt disagreement;
- inbound Receive Maximum violations;
- a completed transport write whose completion epoch differs from its transport
  owner epoch;
- unexpected application task completion, exception, or cancellation;
- unexpected event-loop exception contexts;
- terminal connection tasks, waiters, effects, writer capacity, or receipts;
- checkpoint or whole-schedule liveness failure.

`mqttium-runtime-fuzz-v1` artifacts retain the seed, ordered external and
scheduling operations, reached checkpoints, mutation, owner statistics,
attempted/completed frames and completion epochs per transport generation, and
the exact exception/invariant. Shrinking remains deliberately deferred.

## Mutation qualification

The original four mechanical mutations remain. Two behavioral mutations require
their interleaving windows:

- a late effect is not settled while failing-close owns the EffectPump;
- the pre-fix explicit-connect behavior lets an automatic generation defeat a
  caller already waiting behind its blocked factory.

With seeds `0..599` and 32 steps, detection was:

| Mutation | Detected | Rate |
| --- | ---: | ---: |
| writer failure does not wake waiters | 100/600 | 16.7% |
| connection epoch is not invalidated | 600/600 | 100% |
| effect completion is not settled | 600/600 | 100% |
| callback cancellation kills worker | 100/600 | 16.7% |
| failing-close abandons late effect | 100/600 | 16.7% |
| user takeover loses to reconnect | 25/600 | 4.2% |

## Reference campaign and CI disposition

The unmutated local reference campaign used seeds `0..1999`, 32 exact steps per
seed, and completed with zero failures. It produced 1,994 unique full operation
traces and 1,993 unique scheduling/release traces. Important boundary counts
included 333 EffectPump failure windows, 254 blocked reconnect factories, 1,717
blocked callbacks, 2,198 writer-active checkpoints, and 334 active writer
failures.

PR smoke remains bounded: 12 healthy seeds at 24 steps plus the pytest
qualification campaign. Nightly is not enabled. The proposed nightly budget is
50,000 seeds at 32 steps in ten disjoint shards; long/manual release campaigns
remain external to required CI, starting at 1,000,000 seeds and 48 steps.

No additional production defect was observed in the 2,000-seed reference
campaign after the two ownership fixes above.
