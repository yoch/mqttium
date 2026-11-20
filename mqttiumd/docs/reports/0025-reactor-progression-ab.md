# Retained output and reactor progression

## Transport receipt

The [audited reference](https://github.com/yoch/mqttium/pull/648#issuecomment-7033369025)
retained a QoS 1 PUBLISH when the output queue was full. If the connection
closed before the PUBLISH was queued, the core received the failure and
preserved the claim. The product journal omitted the failure. The regression
requires the original generation and effect to have `effect_retained`
followed by `frame_failed`, with no queue observation. On reconnect the
same claim and Packet Identifier produce the expected PUBLISH. The valid
PUBACK drains the durable obligation.

The regular Rust suite also exercises the 92 occupied ordinary input
positions, the committed writer result and the independent subscriber in
both generation orders. The G receipt matches retained effects against
the complete journal by generation and EffectId. The historical journal
still has 51 writes and one failure. Run the checker with
`python3 mqttiumd/tests/system/retained_frames.py historical.events.jsonl`.

## Reproduce the comparison

Run this from the repository on Linux. Use a writable persistent volume
outside the repository and a new output directory. The command builds the
pinned release twice and verifies the Mosquitto 2.0.21 source archive for
the client driver.

```sh
python3 mqttiumd/tools/run_progression_ab.py /path/to/volume /path/to/new-receipts \
  --base 4793277e3579bcd0716c714b677bc7020a465f91
```

Both builds use the same source directory, compiler, release profile and
lockfile. For B, the command applies `mqttiumd/tools/bounded-batch.mutant`
after building A and saves both executable hashes. The normal build uses
the fixed polling loop.

The trial starts from the published bounded-batch study patch with
SHA-256 bcdf787abc3c35c66a9f5b2afc35c82ea890b7b9bffef2e5dda46e60377ae1ee.
The original patch allocated eight slots on each drain. A close can invoke
a nested drain before returning.

B can apply at most eight extra ready continuation inputs per outer
reactor turn. The budget renews before accepting new sockets. Nested drains
share it and take a slot before they apply the input or dispatch its effects.
The extra part of a drain yields to pending priority lanes. The stop receipt
records how many extra inputs the owner applied and the maximum number of
slots taken in one turn.

## Measurement and verdict

One hosted Ubuntu job runs both releases on the same volume. Each trial
records the actual device and mount of its SQLite file and WAL, the
writer's WAL/FULL readback, mTLS and quota configuration, and observed CPU
affinities. A separate witness runs during a QoS 0 flow. It closes two
subscribers with outstanding deliveries and a QoS 2 producer awaiting
PUBREL. The producer resumes the original exchange. Another subscriber
receives QoS 1 and the retained deadline elapses inside the send window.

Four publisher and subscriber pairs generate the load. Five repetitions
alternate A/B and B/A at 3,500/s QoS 0 and 800/s QoS 1 for 30 seconds.
The higher QoS 0 trial offers 5,500/s. The QoS 1 recovery trial offers
1,500/s for 30 seconds and 800/s for 30 seconds on one PID, client flow
and SQLite inode. Recovery requires three consecutive one-second buckets
of 800 positives and unique deliveries, with one second of observation
grace, after all acknowledged earlier work has delivered and all submitted
earlier work has a reply.

The per-trial files preserve scheduled IDs, submissions and reason codes,
arrival and ACK times, duplicates, CPU samples and the database's pending
deliveries after drainage. Latencies use nearest-rank percentiles from
both the scheduled timestamp and the submit timestamp. Rotated segments
are hard-linked during measurement and checked for continuous sequences
from start to stop. The run fails qualification on missing observations
and preserves the raw inputs and adverse repetitions.

B is eligible for a target SSD trial only if it improves the window
delivery count in all five QoS 0 pairs, all five QoS 1 B trials deliver
their full offers without refusal or loss after positive PUBACK, and the
progression witnesses pass. The result also records the overload recovery
observation and names the first divergent slots in each failing QoS 1 trial.
A successful workflow
can contain an adverse performance verdict. The verdict is in
`verdict.json`, alongside `manifest.json`, per-trial JSON and the raw
receipts. Hosted-volume results describe that runner and its mount.
