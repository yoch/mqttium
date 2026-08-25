# Runtime fuzzer V3: pressure and fine-grained interleaving qualification

| | |
| --- | --- |
| Date | 2026-08-25 |
| Base commit | `b0b9018` (main) |
| Scope | Issues #388 and #389 (part B): the V3 pressure profile of the runtime schedule fuzzer |
| Prior work | [`RUNTIME-FUZZER-GENERATIVE-QUALIFICATION-2026-08-24.md`](RUNTIME-FUZZER-GENERATIVE-QUALIFICATION-2026-08-24.md) (V1), [`RUNTIME-FUZZER-COMPOSITION-QUALIFICATION-2026-08-24.md`](RUNTIME-FUZZER-COMPOSITION-QUALIFICATION-2026-08-24.md) (V2), #397 (terminal `publish_waiters == 0` oracle, #389 part A) |

This is a dated report. It records what was true of the named commits and what
was done about it. Do not edit it to match later code — write a new report.

## Problem

V1/V2 find ownership bugs but leave much of the runtime pressure surface
structurally cold: the harness transport has no `write_nowait`/`write_many`,
the writer admits one frame, payloads are tiny, schedules are serialized, and
every operation settles four event-loop turns. Eager writes, eager refusal
and fallback, the latency-batch flush, segmented writes, `write_many`
coalescing, and parked producers were unreachable by construction. #389
additionally required demonstrating a real application publisher in the
parked state *during execution* and qualifying every exit from it.

## Shape

`tests/fuzz/runtime_pressure_fuzzer.py` builds on the V1 harness exactly as
V2 does. It is not a generic asyncio scheduler; the model is bounded and
seed-reproducible. The V1 harness gained two profile seams — `_client_options()`
and a `settle_turns` attribute defaulting to the historical four-turn
convergence — plus one oracle refinement: the receipts-mirror invariant
(`stats.receipts.publish == outbound.pending_messages`) is now guarded by
terminal teardown, because durable session records legitimately outlive
receipts failed by `_fail_pending`. V1 grammar never reaches that state; the
pressure grammar does, deliberately. V1/V2 schedules and regressions are
otherwise byte-identical in behavior.

Varied axes (all from the seed):

- **Transport capabilities**: `write_nowait` present/absent with a
  schedule-controlled deterministic refusal budget; `write_many`
  present/absent. Both axes are composed present *and* absent across a
  campaign.
- **Writer sizing**: a 32-frame / 256 KiB profile, a four-frame batching
  burst, and a 16-frame callback-overlap saturation window. The harness samples
  resident high water during execution and requires both the 4- and 16-frame
  surfaces. Production defaults are never changed.
- **Producer bursts**: several `publish()` tasks spawned before any of them
  runs, so their admissions land in one event-loop turn.
- **Payload classes**: 16 B, 13 000 B (four cross the 48 KiB latency-batch
  target), 140 000 B (past `SEGMENT_THRESHOLD`).
- **Settlement budget**: `schedule.settle` operations choose 0/1/2/4 turns.
- **Bounded lifecycle overlaps**: pressure is composed separately with reader
  teardown, reconnect-factory, callback-worker, and EffectPump ownership. Each
  schedule opens at most one lifecycle window.

Eleven families, one per `seed % 11`: `eager_paced`, `burst_latency_batch`,
`burst_write_many`, `segmented`, `parked_release`, `parked_cancel`,
`parked_teardown`, `pressure_reader_teardown`, `pressure_reconnect`,
`pressure_callback`, and `pressure_effect`.

Two ingredients the exact wire-multiplicity obligation (#390) required:
`checkpoint.wire_bulk PUBLISH:n` raises the multiplicity target by a whole
batch, because one batched transport write completes several frames at once
and the per-frame checkpoint cannot straddle that; and `broker.puback_pending`
acknowledges completed *occurrences*, not identifiers, because a settled
identifier is legally reused within one schedule.

## Coverage gate

A campaign records per-run pressure counters and — by default from the CLI —
**fails a green run** that left any required surface cold: `eager_accepted`,
`eager_refused`, `latency_batches`, `write_many_calls`, `segmented_writes`,
`parked_publisher_observed`, `writer_waiters_observed`, 4- and 16-frame resident
high water, and a distinct pressure-overlap counter for reader teardown,
reconnect, callback, and effect ownership. A `write_many` call qualifies only
when it carries at least four PUBLISH frames; a mono-frame CONNECT or unrelated
control batch cannot satisfy the coalescing gate. Likewise, an overlap is
recorded only while both the pressure state and lifecycle owner are
simultaneously observable. This implements the
#388 rule that a green campaign with zero hits on an intended counter is a
coverage failure, not evidence of correctness.

Reference campaign at the audited commit (x86-64 dev host):

```
[DONE] target=runtime-pressure seeds=4096 failures=0 operation_traces=4091
       scheduling_traces=4087 seed_start=0 steps=36 wall_seconds=26.63
[PRESSURE] eager_accepted=16325 eager_refused=746 latency_batches=373
           parked_publisher_observed=1488 pressure_lifecycle_overlaps=1488
           pressure_reader_teardown_overlaps=372
           pressure_reconnect_overlaps=372 pressure_callback_overlaps=372
           pressure_effect_overlaps=372 segmented_writes=373
           write_many_calls=553 writer_waiters_observed=372
           writer_4_resident_observed=372 writer_16_resident_observed=372
```

The campaign produced 4,091 operation traces and 4,087 scheduling traces; every
family ran 372 or 373 times and every required surface stayed hot.

## Parked-publisher contract (#389 part B)

The three `parked_*` families and the reconnect-overlap family configure
`max_pending_outbound_messages=2` on the profile's own client and run three
concurrent QoS 1 `publish()` tasks, so the third parks on admission capacity.
The harness samples
`stats.receipts.publish_waiters` after every operation into a high-water
counter, and the `publisher_parked` checkpoint blocks until the parked state
is *observed during execution* — the gap #389 identified in PR #382's
after-the-fact probe. Exits qualified:

- **Release** — broker acknowledgements free capacity; the parked publisher
  retries, admits, and its PUBLISH reaches the wire exactly once.
- **Cancellation** — the parked task is cancelled, hands its wakeup on, and
  settles exactly once as cancelled.
- **Terminal teardown** — `disconnect()` wakes the parked publisher, whose
  retry fails terminally with `MQTTError` instead of parking forever.
- **Reconnect ownership transition** — the publisher remains parked while the
  reconnect factory owns lifecycle progress, then retries against the new
  session and reaches the wire exactly once.

In every family the Part A oracle proves `publish_waiters == 0` at terminal
quiescence and final task settlement proves exactly-once completion.

## Mutation qualification

Eight mechanical breakages, each requiring a new pressure surface to become
visible, over seeds 0–21 (two schedules per family):

| mutation | detected | failing oracle |
| --- | --- | --- |
| `eager_accept_drops_frame` | 8/22 | wire multiplicity / liveness |
| `eager_refusal_lies` | 4/22 | wire multiplicity, latency-flush liveness |
| `segmented_payload_dropped` | 2/22 | wire multiplicity liveness |
| `parked_publisher_not_woken` | 6/22 | wire liveness; terminal waiter oracle |
| `publish_waiter_decrement_lost` | 8/22 | `publish waiter survived terminal teardown` |
| `write_many_decoalesced` | 2/22 | coalescing checkpoint |
| `writer_pressure_bypassed` | 2/22 | writer-admission parking checkpoint |
| `pressure_lifecycle_separated` | 8/22 | four simultaneous-owner overlap checkpoints |

Detections equal the number of schedules whose family exercises the mutated
surface — the mutants need their windows, as V1/V2 qualification established
for theirs.

## CI integration

The PR gate runs a 32-seed coverage-gated V3 smoke beside the V1/V2 smokes
and the full pytest qualification. The PR-gate stateful fuzz budget also
rises from 2 × 50 to 6 × 200 (~4 s): the previous budget provably could not
reach the replay-parked settlement defect fixed in #398, which first
manifests at seed 1, step 188 of the default profile.

## Not done, deliberately

- No generic asyncio scheduler and no third-window composition; each of the
  four lifecycle kinds is covered in its own one-window schedule, per the #388
  handoff.
- The 50k-seed ARM64 nightly (`tools/ci/runtime_campaign.py`) still runs V1
  only. Wiring V3 into the nightly rotation is a separate infrastructure
  decision once a reference campaign size for it is chosen.
