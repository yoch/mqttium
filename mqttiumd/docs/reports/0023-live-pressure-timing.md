# LIVE timing under HISTORY pressure

The prepilot gate and the MQTT interoperability preview exercise different
binaries. The prepilot site uses the frozen normal broker from
`51be4079f7ee42f46500e993315eddc971f0a8d4` with binary SHA-256
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119`.

## Historical CI observations

Each run used eight producers at 50 events/s, a 16 s schedule and a 10 s
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

The first diagnostic on Linux x86_64 with four CPUs of cgroup time and tmpfs
received all 500 events. Its p99 was 0.11 ms for SQLite COMMIT, 1.61 ms from
commit to the LIVE publish call, 6.35 ms from publish to callback and 8 ms
end to end in the gate's buckets. A diagnostic step in the existing prepilot
job repeats the measurement on the same worker after the sealed reception.
The sealed gate continues to govern CI. A pass there or in a separate
diagnostic leaves the historical timing attribution open.
