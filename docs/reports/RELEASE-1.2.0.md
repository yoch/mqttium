# Release 1.2.0 evidence

Date: 2026-10-05. Candidate runtime: `main` at
`cfab7cc0a4e468e24d8a2c350b3228f40882aa50`. The release commit adds only
documentation, the version string, the frozen changelog section, release-facing
installation and support text, and this report. Baseline for comparisons:
`1.1.0` (`b611d492e418594fb8fbbeeb16cdc04088751fbe`).

## Why 1.2.0

An analysis of the independent cross-client benchmark campaign
`yoch/mqtt-python-client-bench` `results/v2/20260930T220715Z` (report dated
2026-10-04) named three weak points of `mqttium==1.1.0` against gmqtt, paho-mqtt
and awscrt, and proved their causes with micro-benchmarks on a Raspberry Pi 5:

| | Weak point | Cause |
| --- | --- | --- |
| W1 | Fixed-rate QoS 1 publish: CPU and latency | (a) one eager write per loop turn, the rest of a burst waits for the writer task; (b) a completion future per publication, imposed by the receipt API; (c) a deeper admission path |
| W2 | Fixed-rate QoS 1 receive: CPU | (a) one extra event-loop iteration per read, for the acknowledgement eager-permit re-arm callback; (b) a deeper ingress path |
| W3 | Receive with many wildcard callback filters | every wildcard filter scanned linearly for each message |

The TLS receive gap the campaign showed was reclassified by that report as W2
exposed under TLS plus an allocator regime of the benchmark itself.

The maintainer's rule for this release: no regression of any kind, and code
simplicity first, because it keeps the client robust, correct and modellable.
A change that adds structure is kept only with a clear measured gain.

## Content and dispositions

| Item | Disposition | Pull request |
| --- | --- | --- |
| W3 | Fixed. Wildcard filters live in a prefix tree derived from Paho's `MQTTMatcher` (supplied by its author), walked iteratively, with exact filters still a dictionary lookup. Matching is checked against the previous linear matcher by seeded and Hypothesis differential tests. | #611 |
| W2a | Fixed. When the reader is the only success-ACK producer (push transport, callback delivery, automatic acknowledgement), it restores the ACK eager permit itself just before it really suspends, instead of a callback costing a loop iteration. The `AckPermitTurn` TLA+ model checks that the rule stays one eager ACK per loop turn. | #612 |
| W1b | Addressed by API. `PublishReceipt.add_done_callback()` and `PublishReceipt.exception()` observe each publication without a coroutine or task per message. | #613 |
| W1a | Kept as is (maintainer decision). Writing every frame of a burst eagerly is the "unbounded budget" #254 measured: budgets of 2, 4 and 8 already cost 5–13 % of closed-loop QoS 0 capacity. A cheaper end-of-turn flush would still send frames 2–5 of a burst at the end of the turn, for little latency gain and one more mechanism. | — |
| W1c, W2b | Not pursued (maintainer decision). The remaining receive gap to gmqtt on the Pi, about 13 µs per message, is spread over many small steps of the effect pipeline (about 22 `len()` calls, two effect collections and two decoder passes per message) that carry Receive Maximum accounting, persistence and ordering. Shaving them would save under 1 µs per change and cost readability. | — |

### Rejected while implementing

- **Matcher:** the first tree walk executed more bytecode than the linear scan at
  one wildcard filter (239 against 197 instructions). Slots for the `+` and `#`
  children, a per-node wildcard flag and entries shared between the index and
  the tree brought it to 189 (exact path 32 against 32). A per-topic result
  cache was not added: the tree already costs under 1 µs at 100 filters.
- **ACK re-arm:** re-arming before every reader `await`, including one that does
  not suspend, allows two eager ACKs in one turn (`AckPermitTurn-naive.cfg`).
  Ownership is therefore limited to the configuration where every reader
  suspension is known.
- **Done callbacks:** a Python object in place of the waiter future was slower
  than CPython's C `Future` (0.78–0.82 of the benchmark adapter's private
  future). A scheduled `partial` stored in the waiter list was lighter for
  callback users but added an `isinstance` check per waiter to every settlement,
  a regression for `wait()` users. The released form adds one waiter future and
  leaves settlement unchanged.

## Evidence

### Raspberry Pi 5, the benchmark's own micro-benchmarks

Run [37308238871](https://github.com/yoch/mqttium/actions/runs/37308238871):
`microbench/mqttium/sub_fixed.py` and `pub_fixed.py` at the benchmark commit
`cd9f475e`, three source trees in two ABC rounds (1.1.0; `main` with #611; #612),
CPython 3.12, Mosquitto on core 0, client on core 1. Values per message, both
rounds.

| QoS 1 receive | 1.1.0 | with #611 | with #611 and #612 | gmqtt |
| --- | --- | --- | --- | --- |
| 2,000/s CPU µs | 62.9 / 61.9 | 63.0 / 62.6 | 58.2 / 58.3 | 45.3 |
| 5,000/s CPU µs | 58.3 / 57.9 | 58.6 / 59.0 | 55.3 / 56.3 | 41.1 |
| 2,000/s, 100 wildcard filters, CPU µs | 150.9 / 150.7 | 67.2 / 67.3 | 63.8 / 63.5 | — |
| 2,000/s `select()` / non-blocking / `call_soon` | 3.00 / 2.00 / 2.01 | 3.00 / 2.00 / 2.01 | 2.01 / 1.01 / 1.01 | 2.00 / 1.00 / 1.00 |
| Python calls under the profiler, 2,000 / 5,000 | 213.1 / 196.7 | 210.9 / 196.7 | 190.2 / 180.6 | — |

Fixed-rate QoS 1 publish shares no changed path: 80.3–80.5 µs per message at
2,000/s in all three trees; at 5,000/s 56.5–56.8 (1.1.0), 55.9–57.4 (#611) and
57.6–57.7 (#612), with p99 latency 176 / 168–184 / 143–152 µs.

### Local measurements

- Matcher, interleaved minimum of 15 runs on a desktop core, 1.1.0 → #611:
  exact filters 0.97–1.015, one wildcard filter 0.835–0.850, 10 filters
  0.25–0.27, 100 filters 0.035–0.037.
- Receipt observation, per receipt: a task awaiting `wait()` 4.2 µs,
  `add_done_callback` 2.1 µs, the benchmark adapter's private future 1.6 µs.

## Qualification of `cfab7cc0`

QUALIFICATION_PENDING
