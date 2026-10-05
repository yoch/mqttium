# Release 1.2.0 evidence

Date: 2026-10-05. Candidate runtime: `main` at
`b2ae1018c05117b79554da216430253be75733b4`. The release commit adds only
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
| Qualification | A send-path regression introduced by #612, found by the qualification and fixed before release (below). | #614 |
| W1a | Kept as is (maintainer decision). Writing every frame of a burst eagerly is the "unbounded budget" #254 measured: budgets of 2, 4 and 8 already cost 5–13 % of closed-loop QoS 0 capacity. A cheaper end-of-turn flush would still send frames 2–5 of a burst at the end of the turn, for little latency gain and one more mechanism. | — |
| W1c, W2b | Not pursued (maintainer decision). The remaining receive gap to gmqtt on the Pi, about 13 µs per message, is spread over many small steps of the effect pipeline (about 22 `len()` calls, two effect collections and two decoder passes per message) that carry Receive Maximum accounting, persistence and ordering. Shaving them would save under 1 µs per change and cost readability. | — |

### Regression found during qualification

The first qualification of `main` at `cfab7cc0` (ARM64 paired regression
[37313227344](https://github.com/yoch/mqttium/actions/runs/37313227344)) passed
its thresholds but showed the send path slower than 1.1.0: `writer_try_enqueue`
0.809, `writer_enqueue_async` 0.825, `effect_send_inline` 0.920,
`publish_nowait` QoS 0 0.951–0.954, strict writer capacity QoS 0 0.960 against
an A/A of 0.999, and the advisory network sweep 0.979–0.99 at windows of 8 and
more.

A `try_enqueue` micro run on each merge of the campaign, with Python 3.11 and
3.13, placed it at #612, which changed no line of the send path. #612 took
`WritePump` from 29 to 31 instance attributes, past the point where CPython
keeps its inline attribute layout, so every attribute access on the pump got
slower. Without those two assignments the micro returned to the 1.1.0 baseline
(3.13: 415–428 ns against 500–520 with them and 417–466 for 1.1.0).

#614 declares `WritePump.__slots__`, so attribute access no longer depends on
the number of fields: 304–319 ns against 330–342 for 1.1.0 on Python 3.11, and
422–454 against 418–439 on 3.13. A guard test pins the slots.

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

## Qualification of `b2ae1018`

| Evidence | Result |
| --- | --- |
| [CI 37320131177](https://github.com/yoch/mqttium/actions/runs/37320131177) and [ARM64 CI 37320131343](https://github.com/yoch/mqttium/actions/runs/37320131343) | Passed |
| [Soak and broker interoperability 37320177753](https://github.com/yoch/mqttium/actions/runs/37320177753) | Passed: Linux and macOS soaks for MQTT 3.1.1 and 5, EMQX 5.8.9 and HiveMQ CE 2026.5 |
| Strict ARM64 network gate vs 1.1.0, [37325168769](https://github.com/yoch/mqttium/actions/runs/37325168769) | Passed: QoS 1 receipt ACK throughput at windows 1, 20 and 64 within the gate per ABBA cycle, mostly 0.989–1.009 of 1.1.0 |
| ARM64 paired regression vs 1.1.0, [37320172164](https://github.com/yoch/mqttium/actions/runs/37320172164) | Passed: strict writer-capacity A/B 1.004 (QoS 0) and 1.003 (QoS 1), A/A 1.000 and 1.005; strict paced writer-latency A/B lag 1.000 at 2,500 and 0.999 at 10,000 msgs/s; advisory network sweep 0.993–1.008 |

### ARM64 paired microbenchmarks against 1.1.0

Median candidate/base throughput over 11 pairs (run 37320172164): the send
path that regressed in the first qualification is back at or above 1.1.0
(`writer_try_enqueue` 1.024, `writer_enqueue_async` 1.019, `effect_send_inline`
1.003, `publish_nowait` QoS 0 0.998–1.003); every other existing scenario
0.985–1.03. The new matcher scenarios measure 1.034 (exact filter), 1.133 (one
wildcard filter) and 28.4 (100 wildcard filters). `receipt_done_callback`
(0.838) compares the new public `add_done_callback()` with the benchmark
adapter's private future on 1.1.0, not an existing path of 1.1.0.
