# MQTTium weak points in benchmark campaign 20260930T220715Z: causes and evidence

Date: 2026-10-04. Scope: identify the patent weak points of mqttium in the
latest v2 campaign of the independent benchmark
(`yoch/mqtt-python-client-bench`, `results/v2/20260930T220715Z`), state the
cause of each from the source code, and prove it with a micro-benchmark that
compares mqttium with the two best clients of the category.

This report deliberately **proposes no fix**. It establishes causes and the
evidence behind them, and lists what is still unproven.

## TL;DR

| # | Weak point | Campaign symptom | Cause (proven unless noted) |
|---|---|---|---|
| W1 | Fixed-rate QoS 1 publish | 148 µs/msg vs gmqtt 122.5, awscrt 103.8 (2,000/s); p50 latency 233 µs vs 72 at 5,000/s | (a) one eager data write per loop turn, the rest of a burst waits for the writer task; (b) a completion future per publish, imposed by the receipt API; (c) deeper admission path (~60 more Python calls per message) |
| W2 | Fixed-rate QoS 1 receive | 83.3 µs/msg vs gmqtt 63.4, awscrt 40.0 (5,000/s) | One extra event-loop iteration per received message (ACK eager-permit re-arm via `call_soon`) plus a deeper ingress path (212 vs 133 Python calls per message) |
| W3 | Receive with many wildcard filters | +64 µs/msg vs paho +3 | `TopicMatcher.iter_match` scans every wildcard filter linearly |
| — | TLS receive (reclassified) | +32.5 µs/msg for TLS vs gmqtt +0.7 | Not a TLS-specific defect. TLS costs both clients about the same. The campaign gap is W2 exposed under TLS, plus a glibc malloc regime that inflated gmqtt's plain-TCP baseline |

## 1. Inputs

- **Campaign:** `results/v2/20260930T220715Z` (standard profile, i7-3770
  development desktop, many runs flagged `host_quiet`). It was used only to
  pick the weak points, not to quantify causes.
- **Measured library version:** mqttium 1.1.0. For every file cited below, the
  content is byte-identical between the installed wheel, tag `v1.1.0` and
  `origin/main` (`b611d492`). **Line numbers refer to `v1.1.0`.** The local
  checkout of this repository was 83 commits behind `origin/main` when this
  report was written.
- **Other clients:** gmqtt 0.7.0, paho-mqtt 2.1.0, awscrt 0.37.0.
- **Reference host for all numbers below:** Raspberry Pi 5 (self-hosted runner
  `rpi5`), kernel 6.18.50+rpt-rpi-2712, `performance` governor, load average
  around 0.1, CPython 3.12.13 (uv-managed), native Mosquitto 2.1.2.
  - Core 0 runs the broker, core 1 the orchestrator and client, core 3 the C
    peer.
  - Two complete runs, each value the median of 3 interleaved rounds:
    [run 1](https://github.com/yoch/mqttium/actions/runs/37152027668) and
    [run 2](https://github.com/yoch/mqttium/actions/runs/37153397118). Run 2
    adds user/system time and minor faults per message.
  - Both runs agree within about 2 µs/msg, except two receive cases explained
    in section 6.
- **Micro-benchmark code:** branch `microbench/mqttium-weaknesses` of
  `yoch/mqtt-python-client-bench` (directory `microbench/mqttium/`).
- **Pi workflow:** branch `microbench/bench-weaknesses-rpi5` of this
  repository (`.github/workflows/rpi5-bench-microbench.yml`). It is push
  triggered, pins the bench commit, and is not meant to be merged.

## 2. Method

The micro-benchmarks reuse the campaign's own adapters and drive shape
(`nowait` for mqttium and gmqtt, `sync` for awscrt and paho). The other side is
always the bench's C peer (`peer/mqtt_peer.c`): a sink for publish tests, a
source for receive tests. Peer and client receive the same absolute
`CLOCK_MONOTONIC` schedule.

Fixed-rate publishing paces with 1 ms ticks, so a 5,000/s offer is a burst of 5
messages per tick. A 2,000/s offer alternates bursts of 2 messages, which is
where the "burst position" below comes from.

Instrumentation (`microbench/mqttium/common.py`):

- **CPU per message:** sum of `/proc/self/task/*/schedstat` over the measured
  window.
- **User time, system time and minor faults:** `getrusage(RUSAGE_SELF)`
  deltas.
- **Event-loop counters**, by patching the instance only:
  - `loop_it`: `selector.select` calls per message;
  - `nb_it`: selects with `timeout == 0`, that is iterations that found ready
    callbacks;
  - `call_soon`: `loop.call_soon` calls per message.
- **Publish→wire delay by burst position:** a hook on `transport.write` records
  when each publish reaches the socket, as a p50 per position in the 1 ms
  burst.
- **Deterministic Python call counts:** `cProfile` over the window, divided by
  the message count. Absolute times under the profiler are inflated; only call
  counts and ratios are used.
- **A/B variants of mqttium**, each changing one thing:
  - `noobserve`: does not register a completion future per publish;
  - `eager`: re-arms the eager data permit after every write;
  - `pull`: forces the `StreamReader` receive path on plain TCP.

  These variants exist only to attribute cost. They are not proposals.

## 3. W1: fixed-rate QoS 1 publish

### Results (Pi, µs of client CPU per message; latency publish→peer, µs)

| Rate | Client | Run 1 CPU | Run 2 CPU | Run 2 user / sys | e2e p50 / p99 | Python calls/msg |
|---|---|---|---|---|---|---|
| 2,000 | awscrt | 51.3 | 47.8 | 45.2 / 2.5 | 52 / 68 | – |
| 2,000 | gmqtt | 54.9 | 64.6 | 49.2 / 15.3 (0.88 minflt/msg) | 26 / 38 | 207.7 |
| 2,000 | **mqttium** | **80.6** | **80.6** | 73.1 / 7.4 | **58 / 104** | **269.2** |
| 2,000 | mqttium `noobserve` | – | 76.0 | 65.8 / 9.3 | 54 / 96 | 225.9 |
| 2,000 | mqttium `eager` | – | 69.6 | 61.3 / 8.3 | 36 / 50 | 228.7 |
| 5,000 | awscrt | 35.7 | 32.6 | 30.9 / 1.3 | 76 / 88 | – |
| 5,000 | gmqtt | 38.0 | 37.9 | 30.8 / 6.8 | 23 / 36 | 153.6 |
| 5,000 | **mqttium** | **57.0** | **57.6** | 51.5 / 6.2 | **96 / 160** | **211.1** |
| 5,000 | mqttium `noobserve` | – | 45.8 | 41.6 / 4.8 | 96 / 176 | 177.8 |
| 5,000 | mqttium `eager` | – | 46.2 | 40.4 / 5.7 | 31 / 46 | 187.0 |

gmqtt's 2,000/s value moved between runs (54.9 → 64.6) together with 0.88
minor faults per message. This is the allocation regime of section 6 on its
PUBACK reads; mqttium is stable.

Publish→`transport.write` p50 by burst position (run 2, µs):

| Case | writes/msg | pos 0 | pos 1 | pos 2 | pos 3 | pos 4 |
|---|---|---|---|---|---|---|
| gmqtt 5,000/s | 1.00 | 11 | 9 | 8 | 8 | 8 |
| mqttium 5,000/s | 0.40 | 26 | 97 | 72 | 54 | 38 |
| mqttium `noobserve` 5,000/s | 0.35 | 24 | 87 | 65 | 50 | 36 |
| mqttium `eager` 5,000/s | 1.00 | 23 | 19 | 17 | 16 | 16 |
| gmqtt 2,000/s | 1.00 | 11 | 9 | | | |
| mqttium 2,000/s | 0.87 | 24 | 43 | | | |
| mqttium `eager` 2,000/s | 1.00 | 23 | 19 | | | |

### Cause W1a: one eager data write per loop turn

`WritePump._try_write_data_eager` (`api/_writer.py:290-320`) writes straight
to the transport only when `_eager_armed` is set. After one successful write it
clears the permit (`self._eager_armed = False`) and schedules
`_rearm_eager_if_idle` through `call_soon` (`_schedule_eager_rearm`,
`api/_writer.py:154-158`). The re-arm only happens on the next loop iteration
and only if the queue is empty (`api/_writer.py:146-152`).

Within one burst, the first publish goes out eagerly. Every following publish
falls back to `self.queue.put_nowait(item)` (`try_enqueue`,
`api/_writer.py:348-361`) and waits for the writer task (`_run`,
`api/_writer.py:550`) to be scheduled. That task coalesces the rest of the
burst into one write.

Evidence:

- mqttium performs 0.40 socket writes per message at 5,000/s, against 1.00 for
  gmqtt.
- Positions 1 to 4 reach the wire 38 to 97 µs after `publish_nowait()`, against
  8 to 9 µs for gmqtt. Position 1 is the worst because it waits for the
  writer-task wakeup; later positions were published later in the same burst
  but leave in the same coalesced write.
- In the profile, `_writer._run` runs 0.42 times per message at 2,000/s.
- **A/B (`eager`):** re-arming after every write gives 1.00 write per message
  and flat 16-23 µs positions. e2e p50 drops from 96 to 31 µs at 5,000/s and
  from 58 to 36 µs at 2,000/s. CPU drops by 11.0 µs/msg (2,000/s) and
  11.4 µs/msg (5,000/s).
- The coalescing therefore does not save CPU here, and it is the dominant
  cause of the latency gap.

### Cause W1b: a completion future per publish

The adapter must observe each publish's completion to count confirmed
publishes. `on_publish` was removed in rc15, so the remaining public way is
`PublishReceipt.wait()` (`api/models.py:224-252`). It allocates an
`asyncio.Future` per receipt (`self._waiters = [waiter]`). Completing it costs
a `call_soon` and a task or callback step per message.

Evidence (A/B `noobserve`):

- CPU drops by 4.6 µs/msg (2,000/s) and 11.8 µs/msg (5,000/s).
- Python calls drop by 43.3 per message (2,000/s) and 33.3 (5,000/s).
- `call_soon` per message drops from 2.90 to 2.06 (2,000/s), and loop
  iterations from 2.24 to 2.06.

This cost is a property of the public API surface (no synchronous completion
hook), not of the benchmark: any application that needs per-message completion
pays it.

### Cause W1c: a deeper admission path

Even with W1a removed (`eager`), the first publish of a burst needs 23 µs to
reach `transport.write`, against 11 µs for gmqtt. The `eager` variant still
executes 228.7 Python calls per message against 207.7 for gmqtt, and the
default path 269.2.

The profile's top own-time entries per publish (2,000/s) are:

- `packets/_publish.py:74 encode_publish_item_v311`;
- `protocol/outbound.py:486 queue_publish`;
- `protocol/outbound.py:310 _prepare_publish_request`;
- `protocol/outbound.py:572 _finish` and `724 _launch`;
- `api/_effects.py:149 collect_from_engine`;
- 26.1 `len()` calls per message.

The public entry is `AsyncClient.publish_nowait` (`api/async_client.py:1249`),
which goes through `_check_nowait_publish_capacity` (`:2212`),
`_commit_publish` (`:692`) and `_finalize_loop_commands`.

This is shown by counts, not by an A/B: no variant isolates a single layer.
How much of the gap each layer costs is not measured.

## 4. W2: fixed-rate QoS 1 receive

### Results (Pi, µs of client CPU per received message)

| Case | Run 1 | Run 2 | user / sys | minflt/msg | loop_it | nb_it | call_soon | Python calls/msg |
|---|---|---|---|---|---|---|---|---|
| plain 2,000/s awscrt | 26.6 | 26.8 | 23.3 / 2.5 | 1.00 | – | – | – | – |
| plain 2,000/s gmqtt | 63.1 | 43.3 | 39.6 / 3.3 | 0.00 | 2.00 | 1.00 | 1.00 | 133.0 |
| plain 2,000/s **mqttium** | 63.1 | **62.4** | 59.0 / 3.1 | 0.00 | **3.00** | **2.00** | **2.01** | **212.1** |
| plain 5,000/s awscrt | 27.4 | 25.7 | 7.8 / 13.5 | 1.00 | – | – | – | – |
| plain 5,000/s gmqtt | 40.3 | 40.2 | 35.4 / 4.6 | 0.00 | 1.80 | 1.00 | 1.00 | 118.8 |
| plain 5,000/s **mqttium** | 59.4 | **58.5** | 55.6 / 2.2 | 0.00 | **2.60** | **1.80** | **2.01** | **195.7** |

gmqtt's 2,000/s run 1 value (63.1) is the allocation regime of section 6. Run
2 shows mqttium 19 µs/msg behind gmqtt at 2,000/s, the same as at 5,000/s
(18 µs/msg). The campaign showed the gap only at 5,000/s for the same reason.

### Cause W2a: an extra loop iteration per received message

Every inbound QoS 1 PUBLISH produces a PUBACK. It is submitted through
`WritePump.try_enqueue_ack` (`api/_writer.py:372-386`), which writes it
eagerly via `_try_write_ack_eager` (`:322-346`), consumes the ACK permit and
schedules the re-arm callback with `call_soon` (`:154-158`).

The loop sequence per message is therefore three iterations:

1. A blocking `epoll` returns data; the push protocol wakes the reader task's
   future (`call_soon` #1).
2. The reader task runs `_read_loop` (`api/async_client.py:1827`), processes
   the message, writes the PUBACK and schedules the re-arm (`call_soon` #2).
   It then calls `receive()` again, finds nothing and suspends.
3. A non-blocking iteration runs `_rearm_eager_if_idle`.

gmqtt needs two iterations: protocol wakeup, then reader task.

Evidence: exact counters, identical in both runs. mqttium makes 3.00
selects/msg with 2.00 non-blocking and 2.01 `call_soon`/msg; gmqtt makes 2.00,
1.00 and 1.00. `transport/_push.py:232 receive` and `codec/buffer.py:412
next_packet` both run 2.00 times per message, consistent with the empty
second pass in step 2.

Not proven by A/B: no variant removes only the re-arm on the receive side, so
the share of the 19 µs due to the extra iteration is not measured. At the
publish side, one such iteration costs a few µs on this host (`_run_once` +
`select` + `epoll`).

### Cause W2b: a deeper ingress path

The profile counts 212.1 Python calls per received message against 133.0 for
gmqtt (2,000/s), and 195.7 against 118.8 (5,000/s).

The top own-time entries per message are:

- `api/async_client.py:1827 _read_loop`;
- `api/_effects.py:149 collect_from_engine` and `:245 drain_inline` (2.00/msg);
- `api/_delivery_lane.py:72 drain`;
- `protocol/inbound.py:583 _on_qos1_auto`;
- `api/async_client.py:1780 _process_ingress_batch`;
- `packets/_publish.py:63 decode_qos12_fields_v311`;
- 28 `len()` and 6 `max()` calls per message.

gmqtt's path is `_read_loop` → `_read_packet` → `_handle_packet` →
`_handle_publish_packet` → callback.

Again, this is shown by counts; the per-layer share is not measured.

## 5. W3: dispatch with many wildcard filters

### Results (Pi)

Matcher alone, ns per matched topic (`dispatch.py`):

| Filters | paho | mqttium | ratio | mqttium comparisons/msg |
|---|---|---|---|---|
| 1 | 1,891 | 1,574 | 0.8 | 1 |
| 10 | 1,959 | 9,046 | 4.6 | 10 |
| 100 | 2,646 | 82,642 | 31.2 | 100 |
| 1,000 | 2,640 | 824,489 | 312.3 | 1,000 |

End to end, 2,000/s, one topic per message vs 100 filters
(`sub_fixed.py`, µs/msg):

| Client | single filter (fan-in) | 100 filters | delta |
|---|---|---|---|
| paho | 51.0 | 53.0 | +2.0 |
| mqttium | 63.1 | 153.0 | +89.9 |

Run 1 gave +2.1 and +88.6.

### Cause

`TopicMatcher.iter_match` (`dispatch/matcher.py:73-90`) checks exact filters
first, then iterates over every registered wildcard filter (`for topic_filter
in self._wildcards`) and tests each one. Cost is linear in the number of
wildcard filters: comparisons per message equal the filter count exactly. paho
matches with a topic trie, so its cost does not grow with the filter count.

Callbacks are registered with `AsyncClient.message_callback_add`
(`api/async_client.py:1559`) and resolved by `_dispatch_topic_message`
(`:1587`).

## 6. TLS receive: reclassified, with a side finding on the measurement

### Results (Pi, 2,000/s, µs/msg)

| Case | Run 1 | Run 2 | Python calls/msg |
|---|---|---|---|
| plain gmqtt | 63.1 | 43.3 | 133.0 |
| plain mqttium (push transport) | 63.1 | 62.4 | 212.1 |
| plain mqttium, forced `pull` | 87.8 | 64.4 | 209.1 |
| TLS gmqtt | 75.2 | 73.9 | 169.0 |
| TLS mqttium (pull transport) | 99.5 | 99.0 | 245.1 |
| TLS awscrt | 32.8 | 31.0 | – |

### Conclusion

TLS adds about the same cost to both clients: +36.6 µs/msg for mqttium and
+30.6 µs/msg for gmqtt (run 2). The TLS gap between them (25 µs/msg, stable
across runs) is the plain-TCP gap of W2 (19 µs/msg) plus about 6 µs.

The forced `pull` path costs only about 2 µs/msg more than the push path on
plain TCP when the allocation regime below is not involved.
`transport/tcp.py:32-39` only uses the push transport for plain TCP on the
stdlib selector loop; TLS always uses `asyncio.open_connection`.

### Why the campaign showed a TLS-specific gap: allocation regime

asyncio's selector transport reads plain TCP with `sock.recv(max_size)`, where
`max_size` is 256 KiB. CPython allocates the full 256 KiB `bytes` object, then
shrinks it to the received length. In glibc, a 256 KiB request is above the
initial mmap threshold (128 KiB). The allocation is therefore an `mmap`, and
the shrink path unmaps it without raising the threshold. Only `free()` of a
larger mmapped block raises the dynamic threshold.

A process thus pays an `mmap` + `munmap` + about two page faults on every read
until something in its history frees a large block. After that, it stays in
the cheap regime for good. Which regime a process lands in depends on its
history, so the cost differs between processes and runs.

Evidence (`microbench/mqttium/recv_regime.py`, development desktop, socketpair,
300-byte messages):

| Read | minflt/msg | sys µs/msg |
|---|---|---|
| `recv_into` preallocated buffer (what mqttium's push transport does) | 0.00 | 1.7 |
| `recv(256 KiB)`, fresh process | 2.00 | 15-20 |
| `recv(256 KiB)` after one full 256 KiB read | 0.00 | 1.9 |

`strace -c` on the fresh-process case shows exactly one `mmap` and one
`munmap` per read (40,039 and 40,004 calls for 2 × 20,000 reads). Pinning
`GLIBC_TUNABLES=glibc.malloc.trim_threshold=…:glibc.malloc.mmap_threshold=…`
removes the faults, which identifies glibc's mmap threshold as the switch.

Consequences:

- **mqttium's push transport is immune**, because it reads into a preallocated
  decoder window. It loses that immunity under TLS, but on the Pi this costs
  little (about 2 µs/msg in the cheap regime).
- **gmqtt on plain TCP (and mqttium forced to `pull`) is exposed.** In the
  campaign, gmqtt's plain receive baseline was most likely inflated by this
  regime, which made its TLS delta look like +0.7 µs/msg while mqttium's looked
  like +32.5 µs/msg.
  - The campaign records user/system time in 10 ms ticks, too coarse to
    classify single runs.
  - Still, gmqtt's system time at `sub_qos1_fixed` (2,000/s, plain) is visibly
    bimodal: 14-25 µs/msg in some runs, 57-64 µs/msg in others.
- **On the Pi**, run 1's two outliers (gmqtt plain 63.1 and mqttium `pull` 87.8,
  both +20 to +25 µs/msg) have the signature of the costly regime. Run 2 shows
  0.00 faults per message in every receive case and no outlier.

### Not proven

- The costly regime was **not observed directly on the Pi**: run 1 did not
  record faults, and run 2 never entered the regime. It is inferred on the Pi
  from the signature, and proven only on the development desktop.
- Locally, the TLS paths of both clients showed 0 faults per message. Why TLS
  reads escape the regime was not established. One possibility is a large free
  during the TLS handshake or certificate loading.

This side finding concerns the benchmark's stability more than mqttium. It can
move any client that reads plain TCP through asyncio's default transport by
about 20 µs/msg between processes.

## 7. Not weak points

- **`invalid` runs in the campaign:** caused by host noise (`host_quiet` on the
  development desktop), not by mqttium.
- **Peak RSS:** about 4 MB above gmqtt.
- **TLS connection time:** a minor difference.

## 8. Open questions and measurements still missing

These are measurements, not fixes.

1. **W1c and W2b:** attribute the extra Python calls per message to layers
   (public API → admission → engine → effects → writer; reader → decoder →
   engine → effects → delivery lane). No variant isolates a layer yet.
2. **W2a:** an A/B that removes only the receive-side re-arm iteration, to
   measure its share of the 19 µs/msg gap.
3. **W1b:** quantify how much of the per-publish future cost is the `Future`
   object itself versus the extra loop step.
4. **Section 6:** confirm the allocation regime on the Pi by running
   `recv_regime.py` there, and explain why TLS reads were fault-free.

## 9. Reproducing

Every command runs from the benchmark repository, on branch
`microbench/mqttium-weaknesses`. It needs an interpreter with mqttium, gmqtt,
paho and awscrt installed (`uv sync --extra dev --extra paho --extra gmqtt
--extra awscrt --extra mqttium` in a single environment) and the bench on
`PYTHONPATH`.

It also needs Mosquitto on `127.0.0.1:11883` (plain) and `11884` (TLS) with
the bench's configuration and certificates. The C peer is compiled on first
use into `build/`.

```bash
PYTHONPATH=src python microbench/mqttium/dispatch.py
PYTHONPATH=src python microbench/mqttium/pub_fixed.py --trace-writes --repeat 3
PYTHONPATH=src python microbench/mqttium/pub_fixed.py --profile
PYTHONPATH=src python microbench/mqttium/sub_fixed.py --repeat 3
PYTHONPATH=src python microbench/mqttium/sub_fixed.py --repeat 3 \
  --cases plain:2000:paho:fanin,plain:2000:paho:f100,plain:2000:mqttium:fanin,plain:2000:mqttium:f100
PYTHONPATH=src python microbench/mqttium/sub_fixed.py --profile \
  --cases plain:2000:gmqtt,plain:2000:mqttium,plain:2000:mqttium:pull,tls:2000:gmqtt,tls:2000:mqttium
python microbench/mqttium/recv_regime.py
```

On the Pi, push branch `microbench/bench-weaknesses-rpi5` of this repository.
The workflow pins the bench commit in `BENCH_SHA`, runs everything with the
core layout of section 1, and uploads the `bench-microbench-rpi5` artifact.
