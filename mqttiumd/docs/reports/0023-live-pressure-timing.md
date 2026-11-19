# LIVE timing under HISTORY pressure

The prepilot gate and the MQTT interoperability preview exercise different
binaries. The prepilot site uses the frozen normal broker from
`51be4079f7ee42f46500e993315eddc971f0a8d4` with binary SHA-256
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119`.

## Diagnose the original short split reception

The sealed reception runs `pilot-split/short` before the site rehearsal.
A failed short split stops the operator step before the site result exists.
The CI diagnostic reads `pilot-split/short/reception.json` and the LIVE
dashboard trace from that same run. It publishes `timing.json` and a row per
event in `events.jsonl`. Each row keeps creation and receipt wall times, the
receiver observation's monotonic tick and membership in the original
inclusive ten-second window. Missing inputs appear in `missing_evidence`.
The gate, broker and load stay fixed.

```sh
python3 mqttiumd/tools/diagnose_split_live.py evidence-14574226002.zip \
  --artifact-sha256 0b8b312e61ada402e1ce034976ad19e04cb7e5f1d51282d51a0fa47e2b588b62 \
  --out ./short-live-timing
```

Replaying that ZIP at source `63f7524a` yields **487 arrivals** in the window
and p99 **374.751591 ms** against **250 ms**. All 500 events eventually
arrived. The trace contains creation and subscriber observation times. It
lacks per-event SQLite COMMIT, queue-to-publish, LIVE publish-call and
callback-entry timestamps, so the delay cannot be apportioned from it.
[Original short split evidence](https://github.com/yoch/mqttium/actions/runs/44612808082/artifacts/14574226002)

## Historical CI observations

These site rehearsals used eight producers at 50 events/s, a 16 s schedule and a 10 s
HISTORY outage. The LIVE gate measures ordinals `[300, 800)`. The p99 is
the 495th ordered latency bucket for a complete window of 500 events.
Buckets round milliseconds up and cap at 4,001. The gate limits p99 to 250 ms.

| Run evidence | LIVE in the window | p99 | Maximum bucket | Largest reception gap |
| --- | ---: | ---: | ---: | ---: |
| [44496645469, artifact 14515003001](https://github.com/yoch/mqttium/actions/runs/44496645469/artifacts/14515003001) | 500 | 5 ms | 6 ms | 22.37 ms |
| [44548159766, artifact 14543515778](https://github.com/yoch/mqttium/actions/runs/44548159766/artifacts/14543515778) | 500 | 287 ms | 388 ms | 399.31 ms |
| [44548159766, artifact 14542952603](https://github.com/yoch/mqttium/actions/runs/44548159766/artifacts/14542952603) | 500 | 389 ms | 430 ms | 440.44 ms |

Read the complete window from `pilot-site/site/result.json`
(`history_caught_up.live_window`) or, for the two failed gate attempts, from
`pilot-site/site/samples.jsonl` (`metrics.live.window` with 500 received).
The archived records contain aggregate percentiles. They lack the latency
buckets and event timestamps needed to recalculate the historical p99
independently or attribute each slow event. Both failures used four logical CPUs
on Azure with ext4; the green attempt used four logical CPUs on AWS with ext4.
These records establish the environment difference without identifying its effect.

## Reproduce stage timing

Use a verified prepilot TAR and its independently recorded digest. Install
the pilot's pinned Python requirements and use a filesystem representative
of the host under test.

```sh
python mqttiumd/tools/measure_pilot_live.py PREPILOT.tar.gz \
  --sha256 VERIFIED_TAR_SHA256 --out /path/to/new-live-timing
```

This uses a disposable copy of the package with the same broker, outbox
durability, schedule and original gate. It records monotonic timestamps around
the SQLite COMMIT, the call to publish on LIVE and entry to the subscriber's
callback. The original end-to-end latency uses the application's wall
timestamps. The tool joins both streams by event identity and checks the
original window count and p99 against the new per-event calculation. The
disposable copy ends at the measured gate. `timing.json` records the exact
binary, package source, workload, platform and stage summaries. `events.jsonl`
contains the event-level measurements.

The first instrumented site rehearsal on Linux x86_64 with four CPUs of cgroup time and tmpfs
received all 500 events. Its p99 was 0.11 ms for SQLite COMMIT, 1.61 ms from
commit to the LIVE publish call, 6.35 ms from publish to callback and 8 ms
end to end in the gate's buckets. This command runs a separate instrumented
campaign with its own receipt. The sealed reception still governs CI.
