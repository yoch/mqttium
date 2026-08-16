# Native PUBACK latency — attribution, not evidence — 2026-08-16

Answers step B0 of [`FLOORS-NOT-CEILINGS-2026-08-16.md`](FLOORS-NOT-CEILINGS-2026-08-16.md)
Gap B. **No code changed.**

| | |
| --- | --- |
| Date | 2026-08-16 |
| Commit described | `2e2ac6d` (branch `perf/compat-floors-2026-08-16`) |
| Host | i7-3770, 8 logical CPUs, `performance` governor, Mosquitto 2.0.18 |
| Preflight | **INELIGIBLE** — `load_1m_per_cpu` 0.284 exceeds the 0.250 limit; the workstation was in interactive use |

> **This report contains no evidence.** The host failed `runner_probe.py
> --enforce`, and every latency cell but one also failed the baseline p50-CV
> gate. Per [`BENCHMARKING.md`](../BENCHMARKING.md) an ineligible host produces
> no release evidence "even if its ratio looks good". Nothing here may be quoted
> as a MQTTium latency figure. It was run, at the maintainer's explicit request,
> to decide **which experiment to build**, not to measure its result.

## What the diagnostic changes about the plan

Gap B proposed two experiments: **B1** eager write (remove the writer-task hop)
and **B2** an opt-in inline `on_publish` (remove the callback-worker hop).

**B2's premise does not survive.** The report assumed the isolated callback
worker is a cost to remove. Measured at the same fixed 10 000 msgs/s, minutes
apart, on one host:

| completion discipline | median p50 | capacity | A/A ratio | base p50 CV | loop-lag ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| `callback` | **0.671 ms** | 13 676/s | 1.0003 | 3.30 % | 1.038 |
| `receipt` | **0.950 ms** | 11 551/s | 0.9988 | 6.65 % ✗ | 1.308 ✗ |

The callback path is the **faster** of the two, by ~0.28 ms, and it is the only
cell whose A/A control passed every gate it owns. The receipt discipline is
slower because `paired_open_loop.py` spawns one `asyncio.create_task` per
publish to await each receipt (`paired_open_loop.py:123-124,158`) — 15 000 tasks
per sample — which costs more than the shared, bounded callback worker.

That is a property of the harness, not of the library, and
[`BENCHMARKING.md`](../BENCHMARKING.md) already warns that receipt completion
"includes awaiting-task scheduling delay and must not be used as a neutral
latency control unless its own A/A cell passes". Here it did not pass.

Consequence: **the isolated callback worker is not the dominant hop**, so an
opt-in `on_publish_inline` would add permanent Stable-tier constructor surface,
default-off, to remove the cheaper of the two hops. B2 should not be built on
this basis.

## The writer hop, measured deterministically

Loop turns are host-independent in a way that microseconds on a loaded
workstation are not. Counting event-loop iterations between admission and the
transport call, over 50 publishes with the loop otherwise quiet:

```
loop turns between publish_nowait() and transport.write():
  1 turn(s): 50 of 50 publishes
```

Every publish waits exactly one event-loop turn for the writer task to be
scheduled out of `await queue.get()` (`api/_writer.py:308`). This is the hop B1
targets, and it is unconditional: the same turn is paid by an auto-PUBACK on the
ingress path.

That quantifies the *structure* of the cost. It does **not** quantify its price:
one loop turn costs microseconds on an idle loop and much more when publisher,
reader, writer, callback and keepalive tasks are all runnable. Establishing the
price needs an eligible host.

## Rate sweep — recorded so it is not re-run

Callback completion, MQTT 3.1.1, 256 B, window 64, four pairs per point. Every
point failed the 5 % baseline p50-CV gate except 1 000 msgs/s, so these are
shapes, not values:

| offered | median p50 | median completed | utilisation | base p50 CV |
| ---: | ---: | ---: | ---: | ---: |
| 1 000 | 1.567 ms | 974/s | 6 % | 2.5 % |
| 2 500 | 0.561 ms | 2 397/s | 14 % | 14.3 % ✗ |
| 5 000 | 0.472 ms | 4 842/s | 28 % | 9.8 % ✗ |
| 10 000 | 0.665 ms | 9 188/s | 53 % | 12.6 % ✗ |

Two shapes worth keeping:

1. **Latency is not monotonic in offered rate.** The 1 000 msgs/s point is the
   *worst*, not the best. A nearly idle loop pays a full wake per message, which
   swamps the queueing it avoids. Any future fixed-rate comparison must not
   assume "lower offer ⇒ lower latency".
2. **Between 2 500 and 10 000 msgs/s, p50 is roughly flat at 0.47–0.67 ms** at
   14–53 % utilisation. On this host, at this offer, the cost looks dominated by
   fixed per-message hops rather than by queueing. That is the opposite of the
   load-bias that explained the retracted 2026-08-09 `2.95×` claim
   ([`README.md`](README.md)), and it is the reason B1 remains worth building —
   but it is a hypothesis, at 10–14 % CV, not a finding.

Capacity here (11.5–17.4 k/s) is well below the ~21 k the external campaign
measured, because the host was loaded. Absolute latencies are therefore **not**
comparable with the campaign's 0.40 ms, nor with the 0.10–0.14 ms floor.

## Recommendation

1. **Build B1** (eager write behind a `write_nowait` on `StreamTransport`, gated
   on an explicit in-flight-write flag, WebSocket excluded). The hop it removes
   is confirmed unconditional and exactly one loop turn per outbound packet.
   It relaxes documented invariant 1 from "one writer task" to "at most one
   write in flight, FIFO order preserved", so `CLAUDE.md`, `AGENTS.md` and
   `IMPLEMENTATION-GUIDE.md` §1 must change with it.
2. **Do not build B2** on this data. Revisit only if a measurement on an
   eligible host shows the callback hop dominating, which this one does not.
3. **Validate B1 on an eligible host.** Requires `runner_probe.py --enforce` to
   pass, i.e. an idle machine. The `callback` cell at a fixed 10 000 msgs/s held
   3.30 % p50 CV and a 1.0003 A/A ratio even on a loaded host, so it is the cell
   to use; the `receipt` cell is not a valid control for this change.

## Reproduction

```bash
python benchmarks/runner_probe.py --output /tmp/probe.json --enforce   # must pass
python benchmarks/paired_open_loop.py --base-root . --candidate-root . \
    --protocols 311 --payloads 256 --windows 64 \
    --target-rates 10000 --completions callback \
    --repeat 4 --count 4000 --policy strict --preflight-report /tmp/probe.json
```

`--repeat` must stay even: an odd count gives one arm the leading position more
often, and the first process of each pair measures faster.
