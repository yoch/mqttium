# QoS 1 RTT tradeoffs — 2026-09-01

Decision brief. Nothing in this report is a merge recommendation.
No product code from the alternatives landed on `qos1-shared-publish-preparation`.

## Scope

Measure the RTT cost of two August decisions, and compare disposable
alternatives that try to keep the #254 batcher while removing the
application ping-pong hop.

| Decision | What it protected | Suspected RTT cost |
| --- | --- | --- |
| [#254](https://github.com/yoch/mqttium/pull/254) one eager write per loop turn | QoS 0 writer coalescing after #253 | PUBACK consumes the permit; a same-turn `on_message` → `publish_nowait` reply queues (+1 writer hop) |
| [#281](https://github.com/yoch/mqttium/pull/281) latency microbatch only on awaited `publish()` QoS 1/2 | rc7 `nowait` / QoS 0 batching | A tight `publish_nowait` QoS 1 burst cannot flush 4–16 frames |
| [#402](https://github.com/yoch/mqttium/pull/402) idle sync callbacks inline | PUBACK p50 (ARM64 10 k −14.6 %) | Makes the #254 hop visible; not a RTT sacrifice |

[#402](https://github.com/yoch/mqttium/pull/402) is recalled only. It is not reverted here.

## Control and alternatives

Control commit: `bec84f3a34a653fa77c3f28429e4cd0e26f3b212`
(`qos1-shared-publish-preparation`: shared `_PreparedPublish` + reentrant
deque fix; no topic cache; `_PreparedPublish` remains a `NamedTuple`).

Alternatives were patched in detached worktrees. None were merged.

| Label | Rule |
| --- | --- |
| Control | #254 + #281 as on that commit |
| A | After `EffectPump.drain_inline` empties, flush exactly one idle queued frame via `write_nowait`. The data eager permit is unchanged. The first A sketch (flush from `_finalize_loop_commands` while `_callback_active`) is a **no-op** on this path: the reply SEND is applied *after* `on_message` returns. The measured A is the drain-end flush. |
| B | A 4-byte success ACK (PUBACK/PUBREC/PUBCOMP) that takes eager does **not** disarm `_eager_armed` |
| D | `publish_nowait` / `publish(..., nowait=True)` QoS 1/2 may call `_try_flush_latency_batch` (min 4 / cap 16 / 48 KiB) |
| E (balanced) | Two permits: data eager once per turn (#254); 4-byte success ACKs neither require nor consume the data permit |

## Validity

- Host: Linux x86_64, Python 3.12.3. `runner_probe.py` **unsuitable** (CPU use 25.2% > 20%). `--enforce` was not used.
- Broker: Mosquitto, `127.0.0.1:11883`, anonymous, no persistence.
- All `paired_writer_capacity` and `paired_open_loop` runs are **advisory / invalid** (A/A QoS 0 CV 5.3–5.5%; several A/B CVs above 5%). They are not release evidence.
- The in-process hop and coalesce cells do not use the broker. They are deterministic counters (`n=200` hop, `n=40` coalesce/burst).

Raw JSON is under `/tmp/mqttium-rtt-campaign/results/` (not committed).

## Attribution (outbound publish → PUBACK)

Control only, Mosquitto, 64 B, `n=800`.

| Rate | admit→write p50 | write→decode p50 | decode→settle p50 | write→settle p50 | write→decode share |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 500 /s | 0.044 ms | 0.414 ms | 0.023 ms | 0.438 ms | **94.5%** |
| 10 000 /s | 0.028 ms | 0.468 ms | 0.032 ms | 0.500 ms | **93.7%** |

#254 does **not** explain outbound PUBACK p50. Almost all of it is
write→decode (broker + kernel + `transport.read`). Client settle after
decode is ~20–30 µs.

## #254 — hop cost (in-process, `n=200`)

Inbound auto-ack QoS 1 PUBLISH, sync `on_message` → `publish_nowait` QoS 1
reply. Transport offers `write_nowait`. Hop = reply reached the fake broker
via the writer-task `write()` path (or was still queued), not via `write_nowait`.

| Variant | Reply hop rate | ACK path | Reply enqueue path |
| --- | ---: | --- | --- |
| Control | **1.00** (200/200) | eager 200 | queued 200 |
| A | **0.00** | eager 200 | queued 200, then drain flush |
| B | **0.00** | eager 200 | eager 200 |
| D | **1.00** (200/200) | eager 200 | queued 200 |
| E | **0.00** | eager 200 | eager 200 |

#254's suspected cost is real and binary on this path: the reply always
takes a writer hop under the status quo. A/B/E all remove that hop. D does
not (one frame is below the microbatch minimum).

## #254 — what it protected (QoS 0 coalesce, `n=40`, burst 16)

Tight `publish_nowait` QoS 0, no yield.

| Variant | Eager after loop p50 | Queued after loop p50 |
| --- | ---: | ---: |
| Control, A, B, D, E | 1 | 15 |

Every alternative keeps the #253 regime: first frame eager, the rest queued
for the batcher. None of A/B/D/E is “N eager data frames”.

## #281 — nowait microbatch

The latency batch flushes only when `qsize==16` or `qsize>=4` **and**
queued bytes ≥ 48 KiB. After a 16-publish tight loop the first frame is
eager, so **15** remain queued: **no variant flushes**, including D
(batches p50 = 0, settle ~1.4 ms on the fake broker).

A 17-publish loop leaves 16 queued:

| Variant | Queued after loop | Latency batches | Batched items | Settle p50 | Settle p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Control | 16 | 0 | 0 | 1.551 ms | 2.252 ms |
| D | 0 | 1 | 16 | 1.223 ms | 2.025 ms |

D's RTT cost-avoidance appears only at the 16-frame (or 48 KiB) threshold:
settle p50 **−21%** on the in-process broker. It does not move ping-pong
(1 frame).

## Live application ping-pong (Mosquitto, `n=400`, 1 inflight)

p50 40.96–41.02 ms for every variant (spread 0.06 ms). That is the broker
+ two QoS 1 deliveries on this host, not the writer hop. The in-callback
queue snapshot used for “live hop rate” is taken too early (reply SEND is
applied after `on_message` returns) and is **not** usable. Use the
in-process hop table above.

## Writer capacity (advisory)

`paired_writer_capacity.py`, MQTT 3.1.1, 256 B, inflight 20, outstanding 64,
repeat 4, 20 k QoS 0 / 8 k QoS 1. Policy advisory. A/A already invalid.

| Pair | QoS 0 cand/base | QoS 0 CVs | QoS 1 cand/base | QoS 1 CVs |
| --- | ---: | --- | ---: | --- |
| A/A control | 0.995 | 5.3% / 5.5% | 0.986 | 3.2% / 4.5% |
| Control vs A | **0.942** | 6.0% / 11.2% | 0.995 | 1.0% / 4.9% |
| Control vs B | 1.023 | 9.0% / 4.7% | 0.995 | 1.3% / 1.2% |
| Control vs D | 0.970 | 3.2% / 7.0% | 0.986 | 2.6% / 3.3% |
| Control vs E | **0.950** | 5.2% / 2.4% | 0.989 | 6.9% / 2.2% |

A and E sit on or under the 95% QoS 0 floor in the median ratio. The A/A
control is itself invalid, so this is **not** enough to disqualify them, and
not enough to call them capacity-neutral. Eligible-host repeat is required
before a capacity claim.

## Open-loop 2 500 / 10 000 (advisory)

`paired_open_loop.py`, MQTT 3.1.1, 64 B, window 64, callback, repeat 4,
count 2 000. Completed-rate ratios stay in ~0.99–1.01 vs control for A and
E. Loop-lag ratios are mixed (A 10 k lag ratio 0.92). Latency CVs fail
validity. No outbound p50 claim.

## Pro / cons

### #254 status quo (control)

- **Pro:** QoS 0 tight burst is exactly 1 eager + 15 queued (`n=40/40`). Matches the #253 fix.
- **Cons:** Application reply hop rate **1.00** (`n=200/200`). Outbound PUBACK p50 is elsewhere (broker).
- **Win-win?** No. It wins the batcher debate and loses the ping-pong hop.

### A — drain-end singleton flush

- **Pro:** Hop rate 0.00; QoS 0 coalesce unchanged; does not change the data permit; open-loop completed rate ~neutral on this host.
- **Cons:** Reply still *enqueues* then flushes (extra queue accounting). Writer QoS 0 median 0.942 with CV 11% — inconclusive, looks like the riskiest capacity cell. The naive “flush while `_callback_active`” implementation does **not** work.
- **Win-win?** Inconclusive on capacity; hop yes, batcher detector yes.

### B — ACK does not disarm data permit

- **Pro:** Hop rate 0.00 at enqueue (no extra flush). QoS 0 coalesce unchanged. Writer QoS 0 median 1.023 (noisy).
- **Cons:** Any same-turn write after an eager ACK can take eager, not only a callback reply. Closer to the rc6 “keep eagering while the queue is empty” shape if ACKs are frequent.
- **Win-win?** Inconclusive (host). Hop yes, coalesce detector yes.

### D — nowait may request the #281 microbatch

- **Pro:** At 16 queued QoS 1 frames, one `write_nowait` of the joined batch; settle p50 1.55→1.22 ms on the fake broker. QoS 0 coalesce unchanged (QoS 0 does not call the flush).
- **Cons:** No effect on ping-pong. No effect on a 16-publish burst (15 queued). Writer QoS 0 0.970, candidate CV 7%.
- **Win-win?** Different debate. Not a #254 fix.

### E — data permit ≠ ACK permit (balanced candidate)

- **Pro:** Hop rate 0.00 at enqueue. QoS 0 data burst still 1+15. Does not give a second *data* eager. Open-loop completed rate ~neutral.
- **Cons:** Many 4-byte ACKs in one turn may each `write_nowait` (not measured as a capacity cell). Writer QoS 0 median 0.950 — on the floor, A/A invalid.
- **Win-win?** The only design that matches both intended invariants in the counter cells. Capacity vs #254 still needs an eligible runner.

## Reading for discussion (not a merge vote)

A/B/E all remove the measured #254 hop without breaking the 1+15 QoS 0
coalesce detector. E is the one that does it by splitting permits (ACK vs
data) rather than by a later flush. Outbound PUBACK p50 on this machine is
~94% write→decode; changing eager policy will not move that number.
Writer-capacity QoS 0 is too noisy here to accept or reject A/E.

#281 is a separate, thresholded effect: D helps only when 16 frames or
48 KiB are already queued.

Next measurement, if any: same cells on an eligible runner (`runner_probe.py
--enforce`) with `paired_writer_capacity` repeat 8. No further micro-opts
until that A/A is valid.
