# Receive-path architecture: measurements and dead ends

Date: 2026-09-08.
Branch: `perf/decoder-recv-into`, on top of `c509bcb` (post-1.0.0rc13 `main`).

This records an independent replication of the RC13 / #445 / #446 comparison, the
fourth architecture that came out of it, and — just as importantly — the
hypotheses that were tested and **falsified**, so they are not proposed again.

## The four arms

| Arm | Receive path |
|---|---|
| RC13 (`main`) | `StreamReader` + `sock.recv(256 KiB)` |
| #445 (`e0db213`) | `BufferedProtocol` + `recv_into` → `bytes(chunk)` → deque → `read()` |
| #446 (`1b52cbd`) | `BufferedProtocol` + `recv_into` → `StreamReader.feed_data()` |
| #447 (this branch) | `BufferedProtocol` + `recv_into` **into the decoder's own slab** |

All four end at the same place: an owned, immutable `bytes` payload on
`Message`. That copy is not negotiable and none of the arms removes it.

## Measurement conditions

x86_64, 8 logical cores, CPython **3.12.13** — not the RPi5/3.14.7 reference
machine. Real sockets, sender on a dedicated thread, receiver pinned with
`taskset -c 2,3`, which are **distinct physical cores** (`thread_siblings_list`
is `0,4 / 1,5 / 2,6 / 3,7`). Medians of 7 interleaved repetitions; delivered
byte counts asserted identical across arms.

**These numbers are directional, not gate-grade.** This host does not satisfy
the enforced preflight (`max_load_per_cpu` 0.25, `max_cpu_percent` 20); observed
load during the runs was 0.7–2.8. They discriminate between architectures; they
do not replace a fresh-process RPi5 run.

## Throughput and CPU

GiB/s of application payload delivered as owned `bytes`:

| Arm | 64 KiB | vs RC13 | 1 KiB | vs RC13 | 256 B | vs RC13 |
|---|---:|---:|---:|---:|---:|---:|
| RC13 `StreamReader` 256 K | 1.362 | 1.000x | 0.231 | 1.000x | 0.070 | 1.000x |
| #446 buffered SR 80 K | 1.083 | 0.796x | 0.228 | 0.990x | 0.072 | 1.028x |
| #446 buffered SR 128 K | 1.299 | 0.954x | 0.236 | 1.024x | 0.072 | 1.020x |
| #445 chunks 80 K | 1.217 | 0.894x | 0.228 | 0.987x | 0.070 | 1.000x |
| #445 chunks 128 K | 1.446 | 1.062x | 0.238 | 1.030x | 0.069 | 0.990x |
| #447 128 K | 1.376 | 1.010x | 0.545 | 2.363x | 0.179 | 2.536x |
| **#447 256 K** | **2.502** | **1.837x** | **0.563** | **2.439x** | 0.178 | **2.523x** |

CPU seconds for the same volume — insensitive to scheduling:

| | 64 KiB | 1 KiB | 256 B |
|---|---:|---:|---:|
| RC13 | 0.24 s | 0.57 s | 0.67 s |
| #447 256 K | 0.14 s | 0.25 s | 0.27 s |
| gain | 1.69x | 2.25x | 2.46x |

## Latency

Request/response, one PUBLISH out and one echoed back, timed until the receive
path has produced an owned payload. TCP loopback, blocking echo thread, 5
interleaved repetitions. Reproduce with `tools/recv_arch_rtt_probe.py`.

| payload | RC13 p50 | #445 | #446 | #447 | #447 vs RC13 |
|---|---:|---:|---:|---:|---:|
| 256 B | 99.3 µs | 62.7 | 65.6 | **57.8** | 1.72x |
| 4 KiB | 110.9 µs | 71.0 | 74.0 | **62.9** | 1.76x |
| 64 KiB | 224.5 µs | 137.6 | 141.2 | **114.2** | 1.97x |

This closes the question the throughput work left open. #447 does not trade
latency for throughput; it is the best arm on both.

## End to end, in the real client

The numbers above measure the **receive path in isolation**. In the full client
that path is a minority of the per-message cost, so they must not be read as
end-to-end expectations. Subscriber saturated by a raw-socket blaster through
Mosquitto (broker pinned to core 1, client to cores 2,3), 20 000 messages per
run, 3 runs per arm, ~17-18 us of CPU per message in total:

| payload | arm | msg/s (median) | CPU us/msg | `ru_minflt` |
|---|---|---:|---:|---:|
| 256 B | `main` | 60 109 | 16.6 | 1703-1916 |
| 256 B | #447 | 61 134 | 16.3 | **332-432** |
| 1 KiB | `main` | 56 815 | 17.8 | 4014-4717 |
| 1 KiB | #447 | 57 578 | 17.7 | **341-431** |

**Throughput end to end is within noise here — on the order of 1-2 %, not the
2.4x of the isolated path.** The receive path simply is not this workload's
bottleneck once engine, effects, delivery and callback dispatch are included.

**Minor faults are not within noise: 5x fewer at 256 B and 10x fewer at 1 KiB,
consistently across every run.** That is the whole point. The regime this work
exists to remove is the ASLR-dependent allocator/page-fault mode documented in
`docs/network-release-gate.md`, which costs ~25 % throughput and ~2 extra faults
per message when a process lands in it. #447 removes the allocation that feeds
it rather than making the fast regime faster.

So the case for #447 is allocation stability first and CPU headroom second, and
the RPi5 gate — where the bimodality actually bites — is the measurement that
decides it.

## Three results that changed the framing

**1. The win is largest on small messages, not large ones.** The investigation
was framed around 64 KiB payloads, where #447 gains 1.84x. At 1 KiB and 256 B it
gains ~2.5x. Small messages are the RTT and message-rate regime, so the
architecture matters most exactly where the study was not looking.

**2. RC13's allocator bimodality reproduces on x86.** In the series above RC13
ran at 98 minor faults; an earlier series of the same code on an SMT pair
produced **26 430**. The layout-dependent regime documented in
`docs/network-release-gate.md` is not ARM-specific. All ratios above are
measured against RC13 in its *fast* regime, so they are conservative.

**3. 80 KiB was suboptimal for both existing prototypes.** Raising the receive
size to 128 KiB moves #446 from 0.796x to 0.954x and #445 from 0.894x to 1.062x
on the 64 KiB payload, and is neutral on small messages. For #446 the binding
constraint is mechanical rather than empirical: `StreamReader` pauses the
transport at `len(buffer) > 2 * limit` (`asyncio/streams.py`, `_DEFAULT_LIMIT =
2 ** 16`), so 128 KiB is the largest receive size whose full callback cannot
trip it on its own. At 144 KiB every full callback pauses and the next read
resumes. 128 KiB also stays below the 144–160 KiB band where the earlier ARM
full-copy allocator probes began to bifurcate.

## The design trap: level- vs edge-triggered wakeup

The first "realistic" #447 prototype — protocol signals, reader task drains —
livelocked. The wait condition was level-triggered (`_end > _start`, "the
decoder holds bytes"). Holding bytes is not holding a *complete frame*: on a
partial frame the reader wakes, decodes nothing, never awaits, and so the event
loop can never run `buffer_updated()` to deliver the rest.

| Wait condition | Delivered | `recv` callbacks | Reader wakeups |
|---|---:|---:|---:|
| level (`_end > _start`) | 0.2 MiB in 20 s | 5 | **11 034 427** |
| edge (a new `buffer_updated`) | 63.9 MiB in 0.03 s | 376 | 378 |

#445 and #446 cannot hit this, because `read()` returns a chunk and awaits
naturally. In a push architecture the wait primitive must be an edge, and the
wakeup-to-callback ratio (~1:1) is worth asserting in a test —
`test_one_reader_wakeup_per_receive_callback` does.

The same shift applies to backpressure. It used to be implicit: a reader that
stopped calling `read()` stopped the transport. In push mode it must be stated,
so the protocol calls `pause_reading()` above high water. `get_buffer()` cannot
express it by returning a short buffer — asyncio raises `RuntimeError` on an
empty one.

## Falsified hypotheses

**Raising `_COMPACT_THRESHOLD` (64 KiB → 256 KiB).** A synthetic sweep showed
+7–8 % on 64 KiB payloads and a clean 4x reduction in compactions. It did not
survive. A second series on the same host reversed the sign (0.961x), and an
independent socket-level run elsewhere also reversed it (1.52 → 1.47 GiB/s). It
was microbenchmark noise. The constant is gone in this branch for an unrelated
reason — the slab compacts in place — but it should not be reintroduced as a
tuning knob.

**Preallocating the decoder without removing the copy.** A fixed-capacity slab
fed by `feed()` measured 0.90–0.96x of the growing `bytearray`, and an
independent run found the same on small messages (763 k vs 583 k msg/s). The
gain is not preallocation; it is deleting the receive-buffer → decoder-buffer
copy. Preallocation is only the enabler.

**"`bytearray.clear()` reuses the buffer."** Half true, and worth recording
because the old comment claimed otherwise: `clear()` does release the backing
store (`getsizeof` 200057 → 57), and `del buf[:start]` shrinks capacity too. So
the old decoder reallocated constantly. Fixing that alone still buys nothing —
see the previous point.

**Micro-optimising #445's transient `memoryview`.** Caching it produced no
reproducible gain. `bytes(memoryview(buf)[:n])` is already the right choice;
`bytes(buf[:n])` is markedly worse because it slices the `bytearray` first.

## What #447 costs

The decoder rewrite touches the `feed()` path used by TLS, WebSocket, Proactor
and third-party loops. Naively it regressed that path ~3 %; inlining the
common case (room already at the tail, fully-drained rewind, high-water update)
brought it back to parity. Paired against `main`, 9 repetitions:

| scenario | candidate/base |
|---|---:|
| `ingress_engine_qos0` | 0.998 |
| `ingress_engine_qos0_v5` | 1.010 |
| `ingress_publish_qos1` | 1.003 |

## Memory cost, measured

`benchmarks/memory_profile.py` was broken on `main` before this could be run:
it still called `store.out_items()` / `store.in_items()`, removed by 42479f5
("remove retired persistence primitives"). Repaired here against the paged
`out_summary_pages()` / `in_index_pages()` API, so the guardrail works again.

Paired against `main`, full scale, isolated child process per scenario:

| scenario | main peak | #447 peak | delta |
|---|---:|---:|---:|
| `inbound_bounded_persistence_4k` | 8.62 MiB | 8.87 MiB | **+0.250 MiB** |
| `reconnect_epoch_cleanup_4k` | 8.12 MiB | 8.37 MiB | **+0.246 MiB** |
| all 13 others | — | — | +0.000 MiB |

Exactly one 256 KiB slab, and only in the scenarios that actually decode
inbound traffic — the slab is allocated lazily, so a client that never receives
pays nothing. `reconnect_epoch_cleanup_4k` gains one slab, not one per
connection, which confirms `clear()` releases oversized growth on reconnect.
Both scenarios stay inside their thresholds (+3.13 and +1.83 MiB of headroom);
`check_memory_thresholds.py` passes on all 15.

## Shrink hysteresis

The first version gave the slab back on every fully drained oversized frame.
For a stream of frames just over capacity that is grow -> consume -> shrink ->
grow: **100 reallocations for 50 frames**, reintroducing exactly the churn this
design removes. An oversized slab is now retired only after
`_OVERSIZE_RETENTION` (64) consecutive drains that did not need the extra room —
1 reallocation for the same 50 frames — while `clear()` still drops it at once
so a new connection never inherits it.

## Open risks

- CPython 3.12.13 here; 3.13.5 and 3.14.7 elsewhere. The three converge, but
  final validation is fresh-process on the RPi5 under the enforced preflight.
- `DEFAULT_CAPACITY` is a throughput/memory trade, not an optimum. 128 KiB would
  halve the per-connection cost; measured 1.38 vs 2.50 GiB/s on 64 KiB payloads
  and equal on small ones. 256 KiB is the throughput choice; the argument for
  128 KiB is real if per-connection footprint matters more.
- `_process_direct_qos0_batch` hoists `decoder._buf` once per batch. The grow
  path replaces that object, so it must never run mid-batch. It cannot today —
  the batch is synchronous and `get_buffer()` only runs in a loop callback — but
  this is now a load-bearing invariant.
