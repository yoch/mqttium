# Runtime fuzzer shared-runner timeout incidents — 2026-08-26

## Decision

The V2 and V3 long-campaign attempts below exposed a test-harness
classification problem, not a reproducible MQTTium protocol or runtime defect.
Their wall-clock deadlines used the system monotonic clock, which continued to
advance while `nice -n 19` workers were descheduled on an actively shared host.
Consequently, lack of CPU time could be reported as a schedule liveness or
connection failure.

The strict defaults remain unchanged for CI and dedicated runners. Commit
`b5a8a0105e28b2de80d4b3e8792fe229555d1217` makes the V1, V2, and V3 schedule
watchdog and connection timeout explicit CLI options, records both values in
failure artifacts, and documents a 30-second/10-second starting profile for
shared long campaigns. This changes timeout classification only; it does not
weaken any MQTT ownership, wire, epoch, accounting, task-settlement, or terminal
oracle.

This is a dated incident record. Do not rewrite it to describe later campaigns.

## Observations

### V3 invalid calibration attempt

An overloaded V3 calibration attempt on code
`0bae517e413fd5986f3060c609c92599bb2779c9` reported seed 3,161,661 after the
0.5-second reconnect deadline expired. The attempt had accidentally reached
three orchestrators and fifteen workers and was excluded from the V3 campaign
evidence. The seed passed once through the CLI and 500/500 times in-process on
the same code. The subsequent valid one-million-seed V3 campaign used exactly
three `nice -n 19` workers and completed with zero failures. Its immutable
report contains the full result.

### V2 interrupted long-campaign attempt

The first V2 long attempt ran on merged code
`6968be144157ba2c0c3b8a5b820013c4ebec6a94`, over the intended range
`[5,000,000, 6,000,000)`, in 10,000-seed shards with three `nice -n 19`
workers. It produced eight timeout artifacts while the host was actively busy:

| Seed | Pair | Observed timeout symptom |
| ---: | --- | --- |
| 5,883,894 | callback × reconnect | 2-second whole-schedule watchdog |
| 5,889,519 | callback × reader | 2-second whole-schedule watchdog |
| 5,897,403 | callback × reader | 0.5-second callback connection deadline |
| 5,902,369 | writer × reconnect | 2-second whole-schedule watchdog |
| 5,916,763 | writer × reconnect | 0.5-second connection deadline |
| 5,917,848 | callback × reconnect | retry after connection deadline |
| 5,917,928 | effect × reconnect | 2-second whole-schedule watchdog |
| 5,919,316 | callback × writer | 2-second whole-schedule watchdog |

The first four seeds passed direct replay and 250/250 strict isolated
repetitions each, for 1,000/1,000 successful replays. All eight seeds passed
through the corrected CLI with `--watchdog-seconds 30` and
`--connect-timeout-seconds 10`; the slowest replay completed in 0.404 seconds.
No independent invariant violation appeared in any artifact.

The attempt was stopped after 910,000 valid schedules. It is incomplete and
must not be cited as the V2 million-seed qualification. Its external evidence
was retained under
`/tmp/mqttium-v2-long-6968be1-p3-nice19-20260826/` on the local host.

## Mechanism

`asyncio.wait_for()` and the client connection deadlines measure elapsed
monotonic wall time. If the OS does not schedule a low-priority worker for most
of a two-second or half-second interval, the deadline can expire even though
the coroutine would make immediate progress when it next receives CPU time.
That is appropriate for a production wall-clock timeout, but it is not enough
evidence by itself to call a deterministic fuzz schedule deadlocked.

The correction does not attempt to pause clocks or infer scheduler CPU time.
Those approaches would change asyncio semantics and complicate the harness.
Instead, it keeps strict defaults and selects an explicit, recorded shared-host
profile with enough wall-clock headroom to tolerate local contention.

## Classification procedure

A timeout under contention remains a finding until reviewed:

1. stop the affected campaign and preserve its exact artifact;
2. replay the exact seed and generated schedule with the recorded deadlines;
3. repeat in isolation with the strict defaults;
4. inspect the independent invariants in the artifact;
5. classify it as environmental only when replays pass and no other oracle
   fails.

The corrected long campaign must use a fresh seed range and exact code
identity. The 910,000 interrupted schedules cannot be combined with a later
harness version into one qualification result.
