# Receive-path architecture: measurements and dead ends

Date: 2026-09-08.
Branch: `perf/decoder-recv-into`, on top of `c509bcb` (post-1.0.0rc13 `main`).

**Provenance.** This branch is **PR #448** (`perf/decoder-recv-into`, based on
`main@c509bcb`). A different PR #447 exists, by another author, prototyping the
same architecture on top of #446. Earlier revisions of this file called this
branch's arm "#447" before that PR existed; every such label has been corrected
to #448. Measurements below are this branch's unless explicitly attributed.

This records an independent replication of the RC13 / #445 / #446 comparison, the
fourth architecture that came out of it, and — just as importantly — the
hypotheses that were tested and **falsified**, so they are not proposed again.

## The four arms

| Arm | Receive path |
|---|---|
| RC13 (`main`) | `StreamReader` + `sock.recv(256 KiB)` |
| #445 (`e0db213`) | `BufferedProtocol` + `recv_into` → `bytes(chunk)` → deque → `read()` |
| #446 (`1b52cbd`) | `BufferedProtocol` + `recv_into` → `StreamReader.feed_data()` |
| #448 (this branch) | `BufferedProtocol` + `recv_into` **into the decoder's own slab** |

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
| #448 128 K | 1.376 | 1.010x | 0.545 | 2.363x | 0.179 | 2.536x |
| **#448 256 K** | **2.502** | **1.837x** | **0.563** | **2.439x** | 0.178 | **2.523x** |

CPU seconds for the same volume — insensitive to scheduling:

| | 64 KiB | 1 KiB | 256 B |
|---|---:|---:|---:|
| RC13 | 0.24 s | 0.57 s | 0.67 s |
| #448 256 K | 0.14 s | 0.25 s | 0.27 s |
| gain | 1.69x | 2.25x | 2.46x |

## Latency

Request/response, one PUBLISH out and one echoed back, timed until the receive
path has produced an owned payload. TCP loopback, blocking echo thread, 5
interleaved repetitions. Reproduce with `tools/recv_arch_rtt_probe.py`.

| payload | streamreader p50 | chunk-queue | buffered-sr | push | push vs base |
|---|---:|---:|---:|---:|---:|
| 256 B | 99.3 µs | 62.7 | 65.6 | **57.8** | 1.72x |
| 4 KiB | 110.9 µs | 71.0 | 74.0 | **62.9** | 1.76x |
| 64 KiB | 224.5 µs | 137.6 | 141.2 | **114.2** | 1.97x |

**Read this narrowly, on two axes.**

*Regime*: it is a saturated echo loop with no engine, no effects and no callback
dispatch, so it measures the receive path's own latency, not application RTT.

*Provenance*: it is an ablation of four receive **mechanisms** over one shared
decoder -- this branch's. Only the `push` arm runs shipped code
(`DecoderPushProtocol` + `PushStreamTransport` + `IncrementalDecoder`); the
other three are minimal reimplementations of the mechanism each PR uses, not
those branches at their commits. It supports "this mechanism costs less per
round trip". It does not support "PR X is faster than PR Y".

The parallel prototype in PR #447 measured application RTT on
the RPi5 at a realistic fixed rate (3942 msg/s, external pacer) and found
**+0.0047 %, i.e. no measurable p50 change**. That is the number to trust for
application latency, and it is consistent with the end-to-end throughput result
below: at realistic rates the receive path is not what sets the cost.

What the table does establish is the absence of a trade: this architecture does
not buy throughput by spending latency.

## End to end, in the real client

The numbers above measure the **receive path in isolation**. In the full client
that path is a minority of the per-message cost, so they must not be read as
end-to-end expectations. Subscriber saturated by a raw-socket blaster through
Mosquitto (broker pinned to core 1, client to cores 2,3), 20 000 messages per
run, 3 runs per arm, ~17-18 us of CPU per message in total:

| payload | arm | msg/s (median) | CPU us/msg | `ru_minflt` |
|---|---|---:|---:|---:|
| 256 B | `main` | 60 109 | 16.6 | 1703-1916 |
| 256 B | #448 | 61 134 | 16.3 | **332-432** |
| 1 KiB | `main` | 56 815 | 17.8 | 4014-4717 |
| 1 KiB | #448 | 57 578 | 17.7 | **341-431** |

**Throughput end to end is within noise here — on the order of 1-2 %, not the
2.4x of the isolated path.** The receive path simply is not this workload's
bottleneck once engine, effects, delivery and callback dispatch are included.

**Minor faults are not within noise: 5x fewer at 256 B and 10x fewer at 1 KiB,
consistently across every run.** That is the whole point. The regime this work
exists to remove is the ASLR-dependent allocator/page-fault mode documented in
`docs/network-release-gate.md`, which costs ~25 % throughput and ~2 extra faults
per message when a process lands in it. #448 removes the allocation that feeds
it rather than making the fast regime faster.

So the case for #448 is allocation stability first and CPU headroom second, and
the RPi5 gate — where the bimodality actually bites — is the measurement that
decides it.

## Three results that changed the framing

**1. The win is largest on small messages, not large ones.** The investigation
was framed around 64 KiB payloads, where #448 gains 1.84x. At 1 KiB and 256 B it
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

The first "realistic" #448 prototype — protocol signals, reader task drains —
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

## What #448 costs

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

| scenario | main peak | #448 peak | delta |
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
design removes. An oversized slab is retired only after `_OVERSIZE_RETENTION` (64) consecutive
drains whose consumed extent stayed inside `DEFAULT_CAPACITY` — 1 reallocation
for the same 50 frames — while `clear()` drops it at once so a new connection
never inherits it. So the slab does **not** shrink back as soon as a large frame
is consumed: it is retained across a window of small frames first, and a
sustained stream of large frames keeps it indefinitely. That retained memory is
the deliberate cost of not thrashing.

Keying retention off *reallocation* rather than consumed extent was itself a bug,
caught in adversarial review: after the first oversized frame the slab is already
large enough, so later oversized frames fit without reallocating and looked idle.
Measured on 200 identical oversized frames, the slab was retired at frame 64 and
re-grown at 65, again at 129/130 and 194/195 — periodic churn exactly contrary to
the mechanism's purpose.

## Errors found by cross-review

Two defects in this branch were found only by comparing against the parallel
prototype in PR #447, both of which it had guarded from the start:

- **Pausing on an incomplete head frame deadlocks the connection.** A frame may
  legally be as large as `max_packet_size`. Pausing because buffered bytes
  crossed the high water stops the only source that can complete such a frame.
  Reproduced with one ~256 KiB frame against a 192 KiB high water: reading
  paused, `receive()` blocked forever. The pause now also requires the head
  frame to be consumable, and the resume fires as soon as it stops being.
- **A connection error was reported as a clean EOF.** `StreamReader.read()`
  raises, and the client's error taxonomy and reconnect policy depend on that
  distinction. The push path swallowed it. `receive()` now raises, with the
  exception winning over bytes buffered before it, while a clean EOF still
  delivers its last generation.

Both are the kind of defect a saturated benchmark never shows: neither affects
throughput, and both break real connections.

## Stale slab bytes must never reach framing

The slab is reused, not zeroed, so bytes past `_end` are previous traffic.
`decode_vbi()` bounded on `len(buffer)` — the backing capacity — so a partially
received Remaining Length continued into them. Reproduced: consume a frame whose
body is `FF FF 10`, commit only `30 80` of the next PUBLISH, and framing yields
`PacketTooLargeError: Packet size 35651461 exceeds maximum 16777216` on
legitimate traffic. `head_frame_ready()` returned True, so the reader ran and
surfaced it as fatal.

`decode_vbi()` now takes an `end` bound and both framing entry points pass
`_end`. The same oracle covers `peek_packet_bounds()`, `head_frame_ready()` and
`next_packet()`, over four stale-byte patterns, three partial headers and 20
randomised split streams. The borrowed MQTT 5 property path was already bounded
via `_decode_bounded_vbi`.

## First touch and adaptive receive window

The slab no longer starts at the steady-state receive size. Allocation floors at
`_MIN_CAPACITY` (16 KiB); the receive path asks for `_INITIAL_WINDOW` (64 KiB)
and is promoted a step at a time to `RECEIVE_QUANTUM` (256 KiB) after four
consecutive completely filled windows, which is the signal that the peer really
has that much waiting.

| | before | after |
|---|---:|---:|
| first touch, 4-byte ACK via `feed()` | 256 KiB | **16 KiB** |
| first receive window | 256 KiB | 64 KiB |
| window under sustained full fills | 256 KiB | 256 KiB |
| window under partial fills | 256 KiB | 64 KiB |
| memory peak vs `main` | +0.250 MiB | **+0.016 MiB** |

This matters beyond the receive path: every decoder pays first touch, including
TLS, WebSocket and Proactor users who get no zero-copy benefit, and every extra
connection multiplies it. It also explains the `qos1_cycle_*` / `qos2_cycle_*`
microbenchmark regressions on run 34284019205: that harness builds a fresh
`IncrementalDecoder` per ACK, so a 256 KiB slab was allocated to decode four
bytes.

## Backpressure envelope

The window handed to one `recv_into()` is capped at `RECEIVE_QUANTUM` (256 KiB)
rather than spanning the free tail. Without that cap a slab left large by an
earlier oversized frame hands its whole tail to one receive, and the high-water
check — which can only run afterwards — is overshot in proportion to retained
capacity. Measured on a real socket: 4 MiB frame drained, slab retained at
8 MiB, slow reader, `SO_RCVBUF` 14 MiB, small-frame flood:

| | window offered | one callback | peak buffered | overshoot vs 192 KiB |
|---|---:|---:|---:|---:|
| free tail | 8192 KiB | 448 KiB | 635 KiB | **3.3x** |
| capped | 256 KiB | 256 KiB | 256 KiB | **1.3x** |

The bound is now structural: `HIGH_WATER + RECEIVE_QUANTUM` = 448 KiB, whatever
the slab size. Cost: a frame larger than the quantum takes more callbacks to
receive (a 4 MiB frame is 16 receives rather than 1). Steady state is unchanged,
since `RECEIVE_QUANTUM == DEFAULT_CAPACITY`.

## Growth envelope

Capacity is bounded by `max_packet_size + _MIN_WINDOW`, not by the frame size
alone. Doubling on its own reached **2x `max_packet_size`**: a frame just over a
doubling step leaves a tail smaller than the 16 KiB window, so the next
`writable_window()` doubles again even though the frame never exceeds the limit.
Measured with a 4 MiB limit and 4 KiB receive chunks: capacity 8 MiB, transient
12 MiB while both slabs are alive. Framing rejects anything larger than
`max_packet_size`, so that headroom can never be used; capacity is now capped
there and the same case measures 1.01x. An explicit `feed()` larger than the
ceiling is still honoured, since WebSocket and TLS may hand over more in one
call.

Growth is by doubling, so capacity can reach ~2x the largest frame actually
seen when that frame is well below the limit — measured 16 MiB of capacity for
repeated 8 MiB frames. Doubling is kept because growing by the exact need would
make receiving one large frame in 256 KiB windows quadratic in bytes copied.
Transient peak during a reallocation remains old + new capacity, inherent to a
copying grow.

### Retained capacity by traffic profile

Measured, one decoder per connection, `max_packet_size` 16 MiB:

| profile | capacity/conn | reallocations | note |
|---|---:|---:|---|
| one huge (4 MiB) then idle | 8.00 MiB | 1 | released after 64 inbound frames |
| one huge then sustained tiny | 0.25 MiB | 2 | released |
| bursty huge/tiny x20 | 8.00 MiB | 1 | no thrash |
| just-over-256 KiB x300 | 0.50 MiB | 1 | no thrash |
| near-max 8 MiB x20 | 16.00 MiB | 1 | at the ceiling |
| 100 conns, 1 in 10 saw a huge | 8.00 MiB (10 conns) | — | 102.5 MiB total, +76 MiB RSS |

The idle case is the one to weigh: a connection that receives one frame above
`DEFAULT_CAPACITY` and then goes quiet keeps that slab. It is not pinned
indefinitely — retirement counts inbound *frames*, and keepalive supplies them,
so a 60 s keepalive releases it in ~64 minutes; a reconnect releases it at once
via `clear()`. Retiring sooner was rejected: making retention size-aware would
release an 8 MiB slab after 2 small drains, which reintroduces grow/shrink
thrashing on the bursty profile above, trading a bounded delay for a repeated
cost.

## Open risks

- CPython 3.12.13 here; 3.13.5 and 3.14.7 elsewhere. The three converge, but
  final validation is fresh-process on the RPi5 under the enforced preflight.
- **`DEFAULT_CAPACITY` = 256 KiB is justified only by the isolated large-payload
  benchmark, and the gate should decide it.** At 1 KiB and 256 B, 128 KiB and
  256 KiB are equal (0.545 vs 0.563 and 0.179 vs 0.178 GiB/s); only the 64 KiB
  payload separates them (1.38 vs 2.50). The headline product claim here is
  allocator stability, and that comes from receiving into stable storage rather
  than from the slab's size, so it does **not** require 256 KiB. Note also that
  the slab is paid by every decoder, including the `feed()` path: TLS, WebSocket,
  Proactor and third-party loops carry the storage without getting the zero-copy
  receive. For a process holding N connections the difference between the two is
  a flat N x 128 KiB — 12.5 MiB at 100 connections, 125 MiB at 1000 — against a
  gain confined to large-payload throughput. A client library holding one or a
  few connections will not notice either way; a fan-out application will.
- `_process_direct_qos0_batch` hoists `decoder._buf` once per batch. The grow
  path replaces that object, so it must never run mid-batch. It cannot today —
  the batch is synchronous and `get_buffer()` only runs in a loop callback — but
  this is now a load-bearing invariant.
