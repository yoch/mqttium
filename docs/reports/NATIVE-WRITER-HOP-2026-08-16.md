# Native writer hop — eager write validated — 2026-08-16

Closes step B1 of [`FLOORS-NOT-CEILINGS-2026-08-16.md`](FLOORS-NOT-CEILINGS-2026-08-16.md)
Gap B. Attribution that selected this experiment is in
[`NATIVE-PUBACK-ATTRIBUTION-2026-08-16.md`](NATIVE-PUBACK-ATTRIBUTION-2026-08-16.md).

| | |
| --- | --- |
| Date | 2026-08-16 |
| Commit described | `4ba8946` against its parent `3962f32` |
| Host | i7-3770, 8 logical CPUs, `performance` governor, Mosquitto 2.0.20 on `127.0.0.1:11883` |
| Preflight | **eligible** (`runner_probe.py --enforce` passed immediately before each run) |
| Harness | `benchmarks/paired_open_loop.py`, MQTT 3.1.1, 256 B, window 64, `--completions callback`, `--policy strict`, fixed absolute rates |

## Verdict

**Accepted.** The eager write meets the network-optimisation bar in
[`BENCHMARKING.md`](../BENCHMARKING.md): a reproducible gain well above 5 % at
more than two load points, baseline CV ≤ 5 %, an A/A control that passes,
throughput not reduced, and loop lag not increased.

It also produced a harness finding that outlives it: the loop-lag ratio is
**not meaningful when the two arms sit in different pacing regimes**, and it
penalises the *faster* build when they do. See "The 5 000 msgs/s artifact".

## What changed

Every outbound frame cost one event-loop turn: `WritePump._run` parks on
`await queue.get()`, so no byte moved until the producing callback yielded. The
same turn was paid by every automatic PUBACK. When the queue is empty, no write
is in flight and no producer is waiting for space, the frame is now buffered
straight through `StreamTransport.write_nowait`.

Confirmed by counting event-loop iterations, which does not depend on host load:

```
loop turns between publish_nowait() and the transport write
  before: 1 turn  — 50 of 50 publishes
  after:  0 turns — 50 of 50 publishes
```

## Every cell measured

Four independent runs. `gain` is the reduction in median callback p50 latency;
`pairs` counts pairs favouring the candidate; `compl` is the
candidate/base completed-rate ratio. Gates: baseline CV ≤ 5 %, loop-lag ratio
≤ 1.05, completed ratio ≥ 0.97, and the same cell passing in the run's own A/A.

| run | rate | gain | pairs | baseCV | lagR | compl | verdict |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| R1 (6 pairs) | 5 000 | +5.0 % | 6/6 | 3.66 % | 1.106 | 0.9989 | excluded: lagR |
| R1 (6 pairs) | 10 000 | +28.6 % | 6/6 | 6.65 % | 0.860 | 1.0016 | excluded: baseCV, A/A |
| R2 (8 pairs) | 2 500 | **+27.8 %** | 8/8 | 3.36 % | 0.963 | 1.0146 | **clean** |
| R2 (8 pairs) | 5 000 | +8.6 % | 8/8 | 4.65 % | 1.115 | 1.0013 | excluded: lagR |
| R2 (8 pairs) | 7 500 | **+25.1 %** | 8/8 | 2.27 % | 0.716 | 1.0108 | **clean** |
| R3 fine (6) | 4 000 | **+22.8 %** | 6/6 | 2.01 % | 0.992 | 1.0229 | **clean** |
| R3 fine (6) | 4 500 | **+16.7 %** | 6/6 | 2.82 % | 0.999 | 1.0251 | **clean** |
| R3 fine (6) | 5 000 | +11.1 % | 5/6 | 18.37 % | 1.286 | 1.0012 | excluded: baseCV, lagR |
| R3 fine (6) | 5 500 | +14.8 % | 6/6 | 2.67 % | 5.572 | 1.0016 | excluded: lagR |
| R3 fine (6) | 6 000 | +24.6 % | 6/6 | 9.38 % | 2.564 | 1.0160 | excluded: baseCV, lagR |
| R4 final (8) | 4 000 | **+23.9 %** | 8/8 | 2.29 % | 0.983 | 1.0230 | **clean** |
| R4 final (8) | 4 500 | +18.4 % | 8/8 | 5.94 % | 1.002 | 1.0254 | excluded: baseCV |
| R4 final (8) | 7 500 | +26.7 % | 8/8 | 4.78 % | 0.734 | 1.0226 | excluded: A/A |
| R4 final (8) | 10 000 | +11.1 % | 6/8 | 35.36 % | 1.429 | 1.0252 | excluded: baseCV, lagR, A/A |

**Five clean cells at four distinct rates**, from three different runs:
2 500 (+27.8 %), 4 000 (+22.8 %, +23.9 %), 4 500 (+16.7 %), 7 500 (+25.1 %).
The requirement is two load points; four were obtained.

**The exclusions do not flatter the result.** Every excluded cell also shows a
gain, from +5.0 % to +28.6 %, i.e. comparable to or larger than the cells that
were kept. Excluding them removes noise, not evidence against the change. All
fourteen cells show a gain, and thirteen of fourteen have *every* pair
favouring the candidate.

Throughput moved with latency rather than against it: the completed-rate ratio
is between 0.9989 and 1.0254, so the gain is not bought by dropping work.

The primary artifact is **R2**, whose A/A control reported `status=passed` at
all three of its rates (baseline p50 CV 3.67 % / 3.05 % / 1.51 %, lag ratios
0.989 / 1.000 / 0.952, completed ratios ≈ 0.9998). R3's A/A also passed. R1's
and R4's A/A each failed a cell, which is why their cells are marked.

## The 5 000 msgs/s artifact

R1 and R2 both failed on one gate only: loop-lag ratio 1.106 and 1.115 at
5 000 msgs/s, while the A/A at that rate returned exactly 1.000 with identical
code on both arms. It was reproducible and it was not harness noise.

The fine sweep explains it. `loop_lag_p95` measures how late the harness's
**paced publisher** wakes relative to its own deadline
(`paired_open_loop.py:139-153`). Absolute values per arm:

| rate | base lag | candidate lag | ratio |
| ---: | ---: | ---: | ---: |
| 4 000 | 1.088 ms | 1.081 ms | 0.992 |
| 4 500 | 1.087 ms | 1.084 ms | 0.999 |
| 5 000 | 0.803 ms | 1.050 ms | 1.286 |
| 5 500 | **0.173 ms** | 0.982 ms | 5.572 |
| 6 000 | 0.204 ms | 0.538 ms | 2.564 |
| 7 500 | 0.171 ms | 0.122 ms | 0.734 |

The metric is **bimodal**. While the publisher still has slack it genuinely
`await asyncio.sleep()`s between messages and the measurement sits on a ~1 ms
plateau set by timer wake-up granularity. Once its per-iteration work exceeds
the pacing interval it stops sleeping, and the metric collapses by roughly 5×
to something that does measure loop congestion.

The two arms leave that plateau at **different rates, and the faster one leaves
later**. The base is slower per publish, so it drops off at ~5 000–5 500; the
candidate still has slack there and stays on the plateau until ~6 000–7 500. In
that band the ratio compares 0.982 ms against 0.173 ms and reports a "5.57×
regression" that is nothing but the candidate being faster. The inflated
baseline CVs at 5 000 and 6 000 (18.37 %, 9.38 %) are the same effect: the base
flips between modes from sample to sample.

Where both arms are in the *same* regime the ratio behaves: ≈ 1.00 on the
plateau (0.992, 0.999) and clearly better off it (0.716, 0.734).

**There is no loop-lag regression in this change.** An earlier reading of this
report's data attributed the 5 000 msgs/s bump to timer quantisation noise with
a random sign; that was wrong. The mechanism is systematic and the sign is not
random — it always penalises the faster arm.

## Consequence for the harness

`paired_open_loop.py`'s loop-lag ratio should not be read for any change that
shifts publisher per-iteration cost, unless both arms are confirmed to be in
the same pacing regime. Comparing the two arms' *absolute* `loop_lag_p95` is
what reveals this: values near ~1 ms mean the publisher is still sleeping and
the number is a timer artifact. A caveat has been added to
[`BENCHMARKING.md`](../BENCHMARKING.md).

This affects any future candidate that makes the publisher faster or slower —
which is most of them — so it is worth checking before trusting a lag verdict.

## Not measured

- MQTT 5, payloads other than 256 B, and windows other than 64. The hop removed
  is per-frame and version-independent, but only this cell was measured.
- WebSocket, which has no `write_nowait` and keeps the queued path unchanged.
- The `receipt` completion discipline, which the attribution report established
  is not a valid neutral control for this change.
- Rates above 10 000 msgs/s, where the base arm's CV made every cell unusable.

## Reproduction

```bash
python benchmarks/runner_probe.py --output /tmp/probe.json --enforce
python benchmarks/paired_open_loop.py --base-root <pre-B1> --candidate-root . \
    --protocols 311 --payloads 256 --windows 64 \
    --target-rates 2500,7500 --completions callback \
    --repeat 8 --count 4000 --policy strict --preflight-report /tmp/probe.json
```

Keep `--repeat` even: an odd count gives one arm the leading position more
often, and the first process of each pair measures faster. Avoid the
5 000–6 000 msgs/s band for this comparison, for the reason above.
