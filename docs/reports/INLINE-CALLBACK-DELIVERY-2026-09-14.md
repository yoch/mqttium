# MQTTium inline callback delivery and hot-path trim — 2026-09-14

This report records the simplification and performance follow-up to the data-plane/lifecycle candidate `21684c9` on the lean-native branch. Its runtime tree is **`05ab4657cdd622511eaafcbf5f84378fd902f692`**, introduced by commits `6a6570f` (hot-path trim) and `12d1b79` (inline callbacks) and unchanged by the later test, harness and documentation commits of the same series. The reference is current main **RC14 `c194597bcf5af4951fbec2b560600eef3cb84b3c`**.

The candidate removes the bounded message-callback worker task and its queue: synchronous message callbacks now run on the reader that decoded the lot, outside every protocol lock. Immediate iterator admission and QoS 1/2 delivery marks no longer create a coroutine per message. Two constructor parameters and three statistics fields are removed, one statistics field and one task flag are added. The runtime is **42 physical lines smaller** than the previous candidate and **3,145 lines smaller than RC14**; the `ApplicationDelivery` module shrinks from 335 to 262 lines and `AsyncClient` loses two constructor parameters.

Against RC14 on this host, the in-memory diagnostics move from the previous candidate's **−60.22%** callback reception to **−33.41%**, from **−17.17%** iterator reception to **+0.99%**, and from **−13.91%** QoS 0 batch publication to **−6.49%**; all routing scenarios are 65–600% faster because dispatch no longer crosses a queue. The 16-cell network comparison records a delivered-rate aggregate of **−8.75%** and **−11.87%** in two consecutive runs, with the deficit concentrated in `publish_many()` long lots (**−13.89% / −19.64%**) and short bursts within **−3.3%**. Whole-process RSS is unchanged once a harness bias described below is removed; separate-phase traced Python peak is **about 41% lower**. These results do not establish parity with RC14; they establish that the remaining deficit is the deliberate one-pipeline receive design and the progressive `publish_many()` admission, not the callback contract.

## Source identities and environment

| Role | Commit | `src` tree |
| --- | --- | --- |
| Current main RC14 (reference `A`) | `c194597bcf5af4951fbec2b560600eef3cb84b3c` | `80e1b875adc37530ca3320774f92181494abc30d` |
| Previous candidate head | `58db9e0e068891ea20bf3613b028b8586aeabcc4` | `35794a15cf563a3532fd19fcb3469d5e5db69a17` |
| Local snapshot: inline callbacks only | `290ff74e1b29eab98540dc8207aac6c2d9eab7d3` | `490920c2760ed2227bf92d0bb0fa6e0ef0380d3c` |
| Local snapshot: plus decode/publish trim | `bc32e5c5a07ff9c6960fb66e5c25e1a5c3967c4e` | `e0561685a29b1a2182a85b478c2a393b26e0e92b` |
| Final candidate (measured `B`) | `6d2e047753604606042a7031accc6ce2108527dd` (snapshot) = branch `12d1b79` onward | `05ab4657cdd622511eaafcbf5f84378fd902f692` |

The two intermediate snapshots are local measurement commits whose `src` trees are stated so each step can be reproduced; they are not part of the branch history. The final snapshot's `src` tree is byte-identical to the branch runtime from `12d1b79` onward, verified with `diff -r` before and after committing.

Environment: this is a **cloud VM, not the maintainer's calibrated host**. CPython 3.12.3 (GCC 13.3.0), Linux 6.12.94+, x86-64/glibc 2.39, four logical CPUs on a shared Intel Xeon, no governor control, 16 GiB RAM. Mosquitto 2.0.18 on `127.0.0.1:11883`, `allow_anonymous`, no persistence. Workers were pinned to logical CPU 2 for the network harness and CPU 1 for diagnostics; the broker and controller shared the remaining CPUs. Load averages rose from about 0.3 to about 1.1 during each campaign, which is the campaign's own work. Ratios from this host are comparable within a run and indicative across runs; they are **not** comparable in absolute terms with the previous report's `i7-3770` numbers, and no cross-host ratio is multiplied or bridged below.

Harnesses: `benchmarks/lean_native_diagnostics.py` at SHA256 `72a1bb01f76d0aa56c677c9277fce8fe0390e8f2fad665352524661063c31a2f` (the committed file), and `benchmarks/lean_native_compare.py` at `b4d8b91560e4d68241e7bc32a7d212620f616fc73fa3d8e001d072390aa06778` for run 1 and `c7066147b265e5cd9ea3fc416b44db4a57a2c871af48f6140dea18f78bb4a85b` (the committed file, with the bytecode fix) for run 2. Both harnesses were made constructible on both arms through signature inspection because the candidate removes `max_pending_callbacks`; the workload code is identical on both arms.

## 1. Simplification review

The review compared the previous candidate with RC14 and the open PRs, looking for ownership that exists only to serve another piece of ownership. Each decision below states what was removed or kept and why.

### Removed: the message-callback worker

The previous candidate already restricted message callbacks to synchronous functions and gave lifecycle hooks their own supervisor. What remained of the callback worker was a bounded `asyncio.Queue` of `(target, message, size)` jobs, a byte reservation mirroring the iterator queue, a worker task with its own fairness counter, a drain/cancel shutdown protocol with `callback_shutdown_timeout`, and a `try_accept`/`accept`/`_reserve` admission trio that had to behave identically for two queues.

With synchronous callbacks, the worker no longer provides anything the reader cannot do itself:

- **Backpressure.** The reader already decodes no further lot until the current one is handed to the application. Queueing callbacks only moved the same work one task later and added a second bound (`max_pending_callbacks`) that had to be sized without any information the reader lacks. Inline invocation makes callback cost the backpressure, which is the documented model for synchronous callbacks.
- **Isolation.** Callback exceptions were already isolated per invocation by `invoke_sync_isolated`; the worker only added a place where a cancelled worker had to discard queued charges.
- **Fairness.** The worker yielded every `_CALLBACK_QUANTUM` invocations. The reader now does exactly the same: `accept()` returns `asyncio.sleep(0)` once the quantum is exhausted, counting route fan-out, and the lane awaits it. A synchronous callback still cannot be pre-empted; that limit is unchanged.
- **Lock discipline.** Invariant 6 is preserved: the lane drains outside the engine and effect-pump locks, and the new responder regression asserts both are free inside the callback.

Removed from the public surface: `max_pending_callbacks`, `callback_shutdown_timeout`, `DeliveryStats.callback_queued`, `DeliveryStats.callback_limit`, `TaskStats.callback_worker`. Added: `DeliveryStats.callback_invocations` (lifetime count including fan-out) and `TaskStats.lifecycle` (the hook supervisor, previously unobservable). The CHANGELOG and migration guide carry the change; the lifecycle supervisor is by contract allowed to outlive `disconnect()`, so the distribution smoke waits for it instead of asserting it synchronously.

### Removed: one coroutine per delivered message

`ApplicationDelivery.accept()` and `AsyncClient._apply_delivery_effect()` were `async def`, so every message paid coroutine creation and a `send()` even when nothing waited. Both now return `None` after an immediate handoff and an awaitable only for waiting work: iterator capacity, the fairness yield, an awaited durable delivery mark, or replay continuation. `DeliveryLane.drain()` awaits only what it receives. The waiting iterator path is one coroutine, `_accept_waiting`, that checks byte and queue capacity under one shared `delivery_timeout`; the previous split between `_reserve`, `messages_queue.put()` and a rollback flag is gone, and `asyncio.Queue.join()`/`task_done()` bookkeeping that no consumer used is removed.

A QoS 1/2 delivery mark that follows an immediate handoff runs synchronously when `_engine_lock` is free. No `await` separates the handoff from the mark, so a free lock cannot become contended in between, and the lane checked the connection epoch immediately before the call. When the handoff waited, or the lock is held, the awaited path re-checks the epoch under the lock as before. The regression in `test_read_loop_batching.py` now expects one fewer lock acquisition for this case.

### Kept, with reasons

- **The fairness quantum (128).** Without it a long lot with a cheap callback would hold the loop for the entire lot. The value was selected by the previous report's A/B between 64/128/256 and is unchanged.
- **The single receive pipeline.** RC14 reaches its callback-reception number through `_process_direct_qos0_batch`, a borrowed-buffer decoder and `deliver_callback_messages_inline`, bypassing the engine, the owned-bytes copy, the effect deque and the delivery lane for a recognised QoS 0 burst. The candidate refuses this: it is a second delivery ownership with its own eligibility rules, and it is exactly the class of specialisation this branch exists to remove. The cost is quantified below and accepted as an explicit trade.
- **Progressive `publish_many()` admission.** RC14 materialises `list(islice(source, 256))` chunks before waiting for receipts; the candidate admits each item as capacity allows, bounded by the QoS 0 prefix. The traced-peak reductions of 22–88% in long lots come from this choice, and its throughput cost is retained as the main open item.
- **`MessageRoute` dispatch and the remaining 30 constructor parameters.** Route selection is a pure function and now costs one call per message; trimming tuning parameters is a separate contract decision and was not started here.

## 2. Open pull requests reviewed

| PR | Nature | Disposition |
| --- | --- | --- |
| #458 `experiment/uniform-message-worker` | Keeps one serial callback worker and lets a **sole eligible** sync callback-only `MESSAGE` run inline after the lock is released; every other case (bursts, routes, replay, direct QoS 0) stays on the worker. −71 runtime lines vs RC14; Pi fixed-rate QoS 1 p50/CPU neutral, p95/p99 +4.9%/+6.6%. | Its inline-after-lock idea is subsumed: the candidate runs **every** synchronous callback inline and has no worker to be eligible for. #458 retains dual ownership and an eligibility table with eight rows, which is the branching this branch removes. Not adopted as an alternative. |
| #456 `first-inline` scheduler | Self-rejected. Mixing an inline first callback with worker-owned followers in one burst lost 5.33% throughput at burst 2 and raised the second callback's p50 by 43%. | Confirms the design conclusion used here: a hybrid inline/worker split inside a burst is worse than either uniform choice. The candidate is uniform-inline for sync callbacks and uniform-queue for the iterator. |
| #443 responder reentrancy test | Test-only. Two adjacent `MESSAGE` effects, a sync `on_message` calling `publish_nowait(qos=1)`, asserting SEND order and receipt retention. | The scenario matters **more** under inline delivery, because the callback now runs while the lane drains. Ported as `tests/unit/test_inline_callback_reentrancy.py` at the API level: both protocols, inbound QoS 0/1, QoS 1 and QoS 0 replies, engine and pump locks asserted free, wire order, receipts, delivery marks and lane high-water of one five-message lot. |
| #430 README performance | Docs-only, describes rc13 main regressions (+9.4% publish sweep etc.). | Not applicable to this branch and would contradict its measured status; nothing merged. It should not be applied to the lean branch without a fresh campaign. |

## 3. Hot-path trim

Profiling the diagnostics after the worker removal showed the residual reception cost in decode and construction rather than delivery. Five changes, none altering a contract:

| Change | Reason |
| --- | --- |
| `Message.__post_init__` skips `_owned_payload` when the payload is already `bytes` | Decoded messages always carry owned bytes; only foreign payloads pay the coercion |
| `unpack_utf8` reads the length prefix inline and tests `memoryview` first | One fewer call per topic; the `bytes` slice already produces the owned copy |
| QoS 0 decoders construct `Message` positionally | Keyword construction of a frozen slotted dataclass is measurably slower on 3.12 |
| Inbound QoS check compares `qos_raw == 0` | Avoids an enum conversion per PUBLISH |
| `RawPacket` constructed positionally; ready QoS 0 `publish_many` prefix tests `message.qos != 0` and skips coercion for `bytes` payloads | Same construction and coercion reasoning on the outbound side; invalid QoS levels still reach ordinary admission and raise |

Diagnostics for this step alone (A = inline-callback snapshot `290ff74`, B = trim snapshot `bc32e5c`, 2 AB / 1 AA cycles): callback reception **+16.96%**, iterator reception **+14.16%**, QoS 0 batch publication **+4.59%**, overlapping routes −0.19% (AA 0.991–1.012). The positional `RawPacket` change was added afterwards and is part of the final tree.

## 4. Structural result

Method counts use the `AsyncClient` AST; private excludes dunder methods; constructor parameters exclude `self`.

| Source | Runtime files | Physical runtime lines | `AsyncClient` methods / private | Constructor parameters | `_delivery.py` lines |
| --- | ---: | ---: | ---: | ---: | ---: |
| Current main `c194597` | 64 | 17,248 | 119 / 92 | 33 | 1,091 |
| Previous candidate `58db9e0` | 60 | 14,145 | 78 / 55 | 32 | 335 |
| This candidate | 60 | 14,103 | 81 / 58 | 30 | 262 |

The three added private methods are `_mark_delivered_locked`, `_mark_delivered` and `_continue_inbound_replay`, extracted from the former monolithic `_apply_delivery_effect` so that the synchronous and awaited paths are separate, readable units. Unit/project test collection moves from 1,885 to 1,861: 32 tests that asserted worker internals (`callback_task`, `callback_queue`, `max_pending_callbacks`, drain timeouts) are replaced by contract tests on invocation counts, inline ordering and the fairness yield, and eight responder-reentrancy variants are new. The diff against `58db9e0`, excluding this report and its index entry, is 57 files, +905/−928.

## 5. Qualification

Local, on the final tree, with `-W error --strict-config --strict-markers` where CI uses them:

| Check | Result |
| --- | --- |
| `ruff format --check`, `ruff check` (src, tests, benchmarks) | clean |
| `mypy src/mqttium` | 60 files, no issues |
| `bandit -q -ll -r src` | clean |
| `pytest tests/unit tests/project` | 1,853 passed; branch coverage 92.06% against the 89% gate |
| `pytest tests/integration tests/resilience` with `MQTTIUM_REQUIRE_BROKER=1` | 40 passed against Mosquitto 2.0.18 |
| `tests/fuzz/fuzz.py --seed 1 --iterations 20000` | codec, engine, websocket: 0 crashes, 0 invariant violations |
| Runtime schedule, composition and pressure fuzzers (CI arguments, `--require-coverage`) | 0 failures; all coverage keys and pressure families present |
| `pytest tests/fuzz` | 123 passed |
| `installed_distribution_smoke.py`, `installed_distribution_extended_smoke.py shutdown` | pass against the local broker |
| `mkdocs build --strict` | clean |

Hosted CI on the pushed branch is the authoritative record for the Python 3.11–3.14 and OS matrix; it was not available inside this environment.

## 6. Measurement method and two bias findings

Method as in the previous report: each cell runs two same-source A/A ABBA cycles and three A/B ABBA cycles in fresh worker processes; each cycle divides the geometric mean of its two B observations by that of its two A observations; cell ratios weight cycles equally and grouped ratios weight cells equally. AA ranges show observed same-source variation and are not subtracted. No confidence interval or significance claim is made.

The user's suspicion that the previous deficit could contain measurement bias was checked. Two findings:

**RSS bytecode bias (fixed).** `lean_native_compare.py` runs workers with `PYTHONDONTWRITEBYTECODE=1` and reports `VmHWM`. A source tree that had never been imported without that variable has no `__pycache__`, so each worker compiles every module at import. Run 1 measured **+8.24% RSS** for the candidate across all 16 cells with AA ranges of 1.000–1.000, which is the signature of a systematic offset, not a workload effect. A direct check on one cell gave 29,092 vs 28,980 KiB with caches on both trees and 33,204 vs 32,224 KiB without: about **3 MiB** attributable to compilation, present on whichever arm lacked a cache. The profile of the run-1 candidate worker shows `builtins.compile` at 58 ms across 60 calls, absent from the reference. The committed harness now runs `compileall` on both roots before the first worker and records this in its metadata; run 2 shows RSS **−1.23%**, consistent with the previous report's −3.12% on its own host. The timed phases exclude import, so this bias never affected rate, CPU or latency columns; it would have affected any RSS conclusion drawn from a fresh checkout.

**Scenario semantics.** `callback_only` and the four `route_*` diagnostics measure `ApplicationDelivery.accept()` in isolation. On RC14 that is a queue put plus a worker round; on the candidate it is the invocation itself. Their +65% to +603% ratios are real for what they measure and are **not** end-to-end gains; the honest end-to-end callback figure is `receive_callback`. Conversely, RC14's `receive_callback` and QoS 0 long-lot network cells exercise a borrowed-decode fast path that the candidate deliberately lacks, so those comparisons measure a design difference, not the callback contract. Both readings are kept side by side below.

A third caveat is noise: this VM has four shared logical CPUs. QoS 0 long-lot cells complete in about 50–70 ms and show AA ranges as wide as 0.84–1.12; those cells are reported but their exact percentages should not be quoted alone. Burst-1 and QoS 1 cells have AA ranges within about ±2% and are reproducible between the two runs to within about two points.

## 7. Diagnostics against RC14

`benchmarks/lean_native_diagnostics.py`, 10 scenarios, 3 AB / 2 AA cycles, worker pinned to CPU 1, 07:22–07:24 UTC; raw `diag_full_1.json` SHA256 `f0ab6b7a18810dc949c106e517cc9f7a8fcaba6048268a4c304bacea5c19c883`. The previous-candidate column is the previous report's final diagnostics on the maintainer's host and is shown for direction only.

| Scenario | Count | Rate change | AA range | CPU/operation change | Previous candidate rate (other host) |
| --- | ---: | ---: | --- | ---: | ---: |
| QoS 0 batch publication (`publish`) | 43,938 | −6.49% | 1.008–1.025 | +6.94% | −13.91% |
| QoS 1 individual publication | 19,160 | −1.85% | 1.005–1.026 | +1.89% | −2.11% |
| QoS 1 batch publication | 20,177 | −6.37% | 1.000–1.005 | +6.80% | −2.69% |
| Iterator reception | 102,174 | +0.99% | 0.951–1.019 | −0.98% | −17.17% |
| Callback reception | 197,898 | −33.41% | 1.002–1.008 | +50.18% | −60.22% |
| Callback delivery only | 200,000 | +602.69% | 1.004–1.022 | −85.77% | +2.77% |
| Exact route | 200,000 | +277.53% | 1.009–1.011 | −73.51% | +25.69% |
| Overlapping routes | 142,774 | +65.32% | 1.013–1.018 | −39.51% | +9.02% |
| Fallback route | 200,000 | +302.52% | 0.997–1.004 | −75.16% | +12.22% |
| Route error handling | 138,372 | +64.83% | 1.003–1.011 | −39.33% | +11.78% |

Iterator reception is now within the AA range of the reference. Callback reception retains a one-third deficit whose composition is given in section 9. The QoS 1 batch result is the one diagnostic that is worse than the previous report's figure; it is within a different host's measurement and its 1.000–1.005 AA range says the −6.37% is real on this host. The QoS 1 batch path was not changed by this series beyond the shared decode/construction trim; the previous report already attributed its cost to progressive admission.

The first step alone (A = RC14, B = inline snapshot `290ff74`, 2 AB / 1 AA) measured callback reception −44.62%, iterator reception −13.56%, publication −12.39%. The trim step then recovered the amounts listed in section 3.

## 8. Network comparison against RC14

`benchmarks/lean_native_compare.py`, self-subscribed combined publish/receive workload, 256-byte payloads, flow limit 20, both protocols, QoS 0/1, memory store, iterator and synchronous-callback delivery, bursts 1 and long (`publish_many()`), 16 cells, 3 AB / 2 AA cycles, target 0.5 s, worker on CPU 2. Run 1 (07:25–07:29 UTC, raw `net_1.json`) used the harness before the bytecode fix; run 2 (07:44–07:48 UTC, raw `net_3.json`, SHA256 `10016e36d36529a78700a7a8608bdf362c4c5c5a61242d765b54671321d93b01`) used the committed harness. Both are reported; RSS is only meaningful in run 2.

Grouped ratios, cells weighted equally:

| Group | Cells | Rate run 1 | Rate run 2 | CPU/message run 2 | App p50 run 2 | Traced peak run 2 | RSS run 2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All | 16 | −8.75% | −11.87% | +11.41% | −52.87% | −40.77% | −1.23% |
| Burst 1 | 8 | −3.31% | −3.35% | +2.99% | +4.50% | +11.92% | −0.35% |
| Long lot | 8 | −13.89% | −19.64% | +20.52% | −78.74% | −68.65% | −2.10% |
| QoS 0 | 8 | −10.71% | −15.55% | +14.08% | −8.46% | −7.31% | −1.77% |
| QoS 1 | 8 | −6.75% | −8.02% | +8.80% | −75.73% | −62.15% | −0.69% |
| Iterator | 8 | −9.59% | −12.67% | +8.66% | −54.16% | −42.60% | −1.35% |
| Sync callback | 8 | −7.90% | −11.06% | +14.23% | −51.54% | −38.88% | −1.11% |
| MQTT 3.1.1 | 8 | −8.43% | −12.18% | +11.29% | −53.28% | −39.54% | −1.19% |
| MQTT 5 | 8 | −9.07% | −11.55% | +11.53% | −52.45% | −41.97% | −1.27% |

Per cell, run 2, with the run-1 rate beside it:

| Protocol / QoS / delivery / burst | n | Rate run 2 | AA range run 2 | Rate run 1 | CPU/msg | App p50 | Traced peak |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 3.1.1 / 0 / iterator / 1 | 7,048 | −2.35% | 1.003–1.015 | −3.10% | +1.68% | +1.90% | +7.65% |
| 3.1.1 / 0 / iterator / long | 8,192 | −20.92% | 0.940–1.121 | −14.52% | +6.27% | −42.08% | −22.08% |
| 3.1.1 / 0 / callback / 1 | 7,456 | −6.66% | 1.006–1.012 | −6.20% | +6.59% | +8.41% | +10.98% |
| 3.1.1 / 0 / callback / long | 8,192 | −31.80% | 0.839–1.027 | −16.34% | +46.62% | +3.13% | +3.86% |
| 3.1.1 / 1 / iterator / 1 | 4,744 | −2.47% | 0.983–0.995 | −0.43% | +2.44% | +4.29% | +22.43% |
| 3.1.1 / 1 / iterator / long | 8,192 | −16.28% | 0.942–1.018 | −14.41% | +19.58% | −93.32% | −87.98% |
| 3.1.1 / 1 / callback / 1 | 4,800 | −0.10% | 0.986–0.991 | +0.89% | +0.19% | +1.58% | +14.88% |
| 3.1.1 / 1 / callback / long | 8,192 | −11.78% | 1.005–1.008 | −11.54% | +13.52% | −95.14% | −89.07% |
| 5 / 0 / iterator / 1 | 8,192 | −4.33% | 0.991–1.004 | −5.88% | +3.04% | +4.59% | +7.69% |
| 5 / 0 / iterator / long | 8,192 | −30.46% | 1.002–1.075 | −20.13% | +14.71% | −35.19% | −44.17% |
| 5 / 0 / callback / 1 | 8,192 | −5.98% | 0.993–1.006 | −7.91% | +5.13% | +7.80% | +6.61% |
| 5 / 0 / callback / long | 8,192 | −15.89% | 0.807–1.000 | −10.17% | +36.65% | +2.31% | −12.07% |
| 5 / 1 / iterator / 1 | 4,256 | −3.64% | 0.999–1.002 | −3.11% | +3.82% | +5.03% | +16.20% |
| 5 / 1 / iterator / long | 8,192 | −16.32% | 0.977–0.995 | −13.17% | +19.62% | −93.33% | −86.34% |
| 5 / 1 / callback / 1 | 4,808 | −1.06% | 0.996–1.003 | −0.37% | +1.16% | +2.63% | +9.85% |
| 5 / 1 / callback / long | 8,192 | −10.77% | 0.994–1.014 | −10.33% | +12.21% | −95.13% | −86.93% |

Reading these against the previous report's corresponding 96-cell groups (which also included SQLite, so the comparison is directional): QoS 0 callback long lots were −42.76% and are now −16% to −32% depending on the run; QoS 0 callback burst 1 was −20.00% and is now −6% to −8%; QoS 1 callback burst 1 was −7.33% and is now within ±1%; QoS 1 iterator burst 1 was −6.53% and is now −2.5% to −3.6%; QoS 1 long lots were −21% to −23% and are now −11% to −16%. The QoS 1 long-lot p50 and traced-peak reductions of about 87–95% are the progressive-admission clock effect the previous report explained: items are timestamped as the generator yields them and fewer already-timestamped items wait ahead of admission. They do not indicate faster ACK RTT and the whole-lot rate is lower.

The short-burst traced-peak increases of 7–22% are small absolute numbers (about 6–14 KB per phase) and come from the delivery lane retaining one lot and the lifecycle supervisor's state; they are retained rather than explained away.

## 9. Where the remaining deficit is

Per-message CPU profiles of the `receive_callback` diagnostic at 2,048 messages, one worker each, sorted by cumulative time:

| Path | RC14 `c194597` | Candidate `05ab4657` |
| --- | --- | --- |
| Reader entry | `_read_loop` → `_process_direct_qos0_batch` (8 calls) | `_read_loop` → `_process_ingress_batch` (17 calls) |
| Per message | `peek_packet_bounds` + `decode_qos0_message_v311_borrowed` (no copy, no engine) → `deliver_callback_messages_inline` → `invoke` | `next_packet` (owned copy) → `engine.handle_raw` → `_on_publish_v311` → `decode_qos0_message_v311` → `EngineEffect` → `DeliveryLane.drain` → `_apply_delivery_effect` → `accept` → callback |
| Worker CPU per message | 2.39 µs | 3.61 µs |

The 1.2 µs difference is the engine dispatch, the owned-bytes copy at the packet boundary, one effect object and the lane step. Those are exactly the four things RC14's specialised path skips and this branch keeps as its single pipeline (invariants 3 and 5). Closing the gap fully therefore requires one of three choices, none taken here: reintroduce a recognised-burst bypass (rejected in section 1), move QoS 0 decode into the engine's `handle_raw` so `RawPacket` is never materialised for PUBLISH (a protocol-layer change worth a separate measured PR), or accept the cost.

For long lots the profile of the network worker (MQTT 3.1.1, QoS 0, callback, one long lot of 8,224 publications including warmup) shows the writer coroutine resumed 35 times on the candidate against 6 on RC14, with one `try_enqueue`/`put_nowait` pair per item. RC14 hands the writer 256-item chunks; the candidate admits progressively and pays a writer wake per bounded prefix. That is the source of the −12% to −32% long-lot rates and of the traced-peak savings; the two are the same design choice.

## 10. Decisions and open items

- The inline synchronous callback model is adopted on this branch: strictly fewer owners, fewer parameters, fewer statistics, one fewer task, and a measured recovery of roughly half the previous receive deficit on this host. It is a **Provisional**-tier change with CHANGELOG and migration notes.
- The hot-path trim is adopted; each change is contract-neutral and covered by the existing decode, publish and fuzz suites.
- The bytecode RSS bias in `lean_native_compare.py` is fixed and recorded in harness metadata. Earlier RSS figures from any campaign that mixed cached and uncached checkouts under this harness should be treated as suspect; rate, CPU and latency figures are unaffected.
- Not established: parity with RC14. Long-lot `publish_many()` throughput remains 12–32% below RC14 on this host with wide AA ranges in the QoS 0 cells, and callback reception remains about one third slower in the in-memory diagnostic. The maintainer's calibrated host and the 96-cell grid should be rerun on the pushed tree before any merge decision; the previous report's paced QoS 0 overload failure was not re-executed here and stays open.
- Candidate next step with the best evidence-to-risk ratio: decode QoS 0 PUBLISH bodies inside the engine's raw-packet path so no intermediate `RawPacket` copy is made for the most common inbound packet, measured with the same two harnesses.

## 11. Addendum, same day: receive-path stage decomposition and one prototype

An external review of sections 1–10 asked for three things: freeze the architecture listed in section 1 (synchronous message callbacks and routes, `messages()` for asynchronous consumption, receipts instead of `on_publish`, the separate asynchronous lifecycle supervisor, `DeliveryLane`, frozen routing, immutable properties, progressive prefixes); explain the callback-reception deficit stage by stage, `decoder → packet → inbound engine → effect creation and separation → DeliveryLane → ApplicationDelivery`, without a second scheduler, without borrowed payloads crossing layers and without a second reception model, keeping at most two or three local prototypes that each show a gain clearly outside noise behind a simple invariant; and stop work on QoS 1 prefix batching and on a 256 quantum. The first and third are adopted as the branch's working rule. This addendum records the second.

The review also listed "reader-inline callbacks" among the prohibitions. Section 1's design is kept: it removes the callback scheduler rather than adding one beside it, which is what the hybrid proposals in section 2 did. The property given up is stated once more so it is not lost: with a worker, decoding could run up to 1,024 queued invocations ahead of application callbacks; without it, a lot's callbacks return before the next lot is decoded. For synchronous callbacks that buffer carried no throughput (a blocked event loop is blocked whichever task runs the callback), and the already-decoded protocol effects of a lot are still applied before its messages (section 1, `DeliveryLane` fence).

### Method

Stage times were taken without a profiler: one 256-message lot of MQTT 3.1.1 QoS 0 PUBLISH packets (topic `a/b`, 256-byte payload, 261-byte remaining length, 264 bytes on the wire), repeated 300 times, median per message, worker pinned to logical CPU 1. Stages are measured on their real objects: a fresh `IncrementalDecoder`, a connected `AsyncClient` in callback mode with a counting `on_message`, the client's own engine, pump and lane. The full-path row pushes the lot through the scripted transport and waits for the 256th callback, which is the same measurement as the `receive_callback` diagnostic. Single-object costs were taken with `timeit` (minimum of five rounds of 200,000). Trees: the section 7 candidate `05ab4657` and RC14 `c194597`.

### Decomposition

| Stage (ns per message) | Candidate `05ab4657` | RC14 `c194597` |
| --- | ---: | ---: |
| A. `decoder.feed` + `next_packet`: VBI, bounds, owned body copy, `RawPacket` | 1,060 | 1,058 |
| B. `decode_qos0_message_v311(RawPacket)`: topic, payload copy, `Message` | 1,157 | 1,357 |
| C. `engine.handle_raw` + `take_effects` (includes B; dispatch, `EngineEffect`) | 1,620 | 1,848 |
| A′. RC14 direct path: `peek_packet_bounds` + borrowed decode + consume | — | 1,546 |
| D. `collect_from_engine` + `DeliveryLane.collect` (partition), as C+D − C | 113 | — |
| E. `DeliveryLane.drain`: no-op pump drain, `_apply_delivery_effect`, `accept`, callback | 388 | — |
| F. Full reader path, transport read to callback | 3,414 | 2,208 |

A + C + D + E = 3,181 ns; the 230 ns to F is the reader loop itself (engine lock, store batch context, pump drain, event wake). RC14's F − A′ = 662 ns is its batch enqueue, worker round and loop.

Against RC14 the 1,206 ns difference decomposes as: A + B versus A′, **+671 ns** (materialising an owned `RawPacket` and decoding it in a second pass, against one borrowed pass with a single payload copy); dispatch, effect object and partition, C − B + D, **+576 ns** (RC14's direct path bypasses the engine entirely); delivery and loop, E + 230 versus 662, **−44 ns** (the inline handoff is cheaper than RC14's batch enqueue and worker). No stage is anomalous; each is a few Python calls. Section 9's attribution stands, now with numbers that carry no profiler weighting.

Single-object costs behind the stages: `Message(...)` **651 ns**; `RawPacket(...)` **270 ns** frozen against **88 ns** as a plain slotted dataclass; `unpack_utf8` 243 ns; `bytes()` of a 265-byte `bytearray` slice 81 ns; `PacketType.from_byte` 62 ns; `validate_received_publish_topic` 45 ns; payload slice 28 ns. A frozen dataclass assigns each field through `object.__setattr__`, so its construction costs about three times a plain one.

Two findings follow. The largest single cost, `Message` construction, is common to both arms (19% of the candidate path, 29% of RC14's) and is the public immutability contract; it is not part of the deficit and is not changed under the freeze. The second, `RawPacket`, is an Internal container built once per inbound packet of any type that paid the same premium with no contract behind it: nothing mutates or hashes it, and the owned-bytes guarantee (invariant 3) is a property of `remaining`, not of the container.

### Prototype: plain `RawPacket`

Commit `0b3c467` drops `frozen=True` from `RawPacket` and keeps `slots=True`; the branch `src` tree becomes `0e80e4a1f9570adc46cfb9ac55e4ef1cbfffd1e3` (measurement snapshot `8408c0d`, byte-identical excluding `__pycache__`). Stage A in three alternated pairs against `05ab4657`: 940–958 → 705–725 ns; full path 3,300–3,331 → 3,075–3,141 ns, **−6% to −7%** with pair spreads of about 30 ns. Diagnostics (`diag_rawpacket.json`, SHA256 `730fd3ee637736051d4aaaf7740e38dec84aa1d7430ee2c5f4082c06ae66ef1a`, 3 AB / 1 AA, 08:29 UTC), A = `05ab4657`, B = `0e80e4a1`:

| Scenario | Count | Rate change | Cycle ratios | CPU/operation change |
| --- | ---: | ---: | --- | ---: |
| Callback reception | 136,226 | **+9.83%** | 1.075, 1.149, 1.073 | −8.92% |
| Iterator reception | 113,065 | **+6.37%** | 1.067, 1.064, 1.061 | −5.98% |
| QoS 0 batch publication | 80,075 | +6.21% | 1.046, 1.084, 1.057 | −5.84% |
| QoS 1 batch publication | 19,463 | +2.19% | 1.035, 1.012, 1.019 | −2.14% |

Every cycle ratio is above 1. Publication improves because the scripted transport's broker emulation decodes with the same decoder (section 6, scenario semantics); on a real broker only the ACK decode benefits, which is the QoS 1 row. Unit and project suites (1,861), the three fuzzers at 20,000 iterations, the Hypothesis and stateful suites, `ruff`, `mypy` and `bandit` pass on the tree.

Against RC14 (`diag_rc14_vs_cand3.json`, SHA256 `eb19b8b5061734122ae20b1773d34200f7bb18f945543369ad55ef5153404f22`, 08:34 UTC), same method as section 7:

| Scenario | Count | Rate change | AA | CPU/operation change | Section 7 |
| --- | ---: | ---: | --- | ---: | ---: |
| Callback reception | 196,291 | **−28.78%** | 0.998 | +40.42% | −33.41% |
| Iterator reception | 112,342 | **+7.95%** | 0.994 | −7.37% | +0.99% |
| QoS 0 batch publication | 85,654 | −1.12% | 1.005 | +1.13% | −6.49% |
| QoS 1 batch publication | 20,199 | −5.24% | 0.998 | +5.52% | −6.37% |
| Callback delivery only | 200,000 | +610.09% | 1.009 | −85.92% | +602.69% |

### Prototypes considered and not taken

- Inlining `validate_raw_packet`'s PUBLISH branch into `handle_raw`, or removing the `_emit` indirection: about 90 ns each, bought with a duplicated validation rule or a second effect-construction site. Below the bar.
- Section 10's suggested next step, decoding QoS 0 PUBLISH bodies inside the engine's raw-packet path so no `RawPacket` is built for that type: it is a second decode entry into the engine for one packet type, which is a second reception model by another name. Withdrawn.
- A borrowed `RawPacket.remaining` view into the decoder buffer: violates invariant 3. Not attempted.

### Position

The receive hot-path question is closed on this branch. The remaining callback-reception deficit against RC14, about 29% on this host, is accounted for to within 50 ns by the two invariant-bearing steps RC14's direct path skips: the owned packet boundary (+671 ns) and the engine dispatch with its effect object (+576 ns). The delivery side is already cheaper than RC14's. No further local prototype shows a gain outside noise behind an invariant simpler than the one it replaces. The `Message` construction cost is recorded as the one item that would pay on both arms if its contract were ever revisited; that is a separate decision, not part of this branch.

## 12. Addendum, 2026-09-15: `publish_nowait(qos=0)` ran the generic preflight before the direct path

An external review traced the QoS 0 `publish_nowait()` deficit against RC14 to an ordering residue of the lean rewrite rather than to the new architecture. RC14 tried the direct QoS 0 handoff first and fell back to `_check_nowait_publish_capacity()` only when it declined. The rewrite removed the direct path and kept the preflight; when the direct path was rebuilt on the writer-ownership rules (`c14ea0c`, then `41738a6` for `publish_many()`), `publish_nowait()` placed it after the preflight it was meant to short-circuit. `publish()` and `_publish_ready_prefix()` were already direct-first. The result, whenever the writer held a resident frame — the steady state of a saturated producer — was one `publish_wire_size()` preview followed by the real encode of the same PUBLISH in `prepare_qos0()`, and a writer admission on the preview before the writer's own admission on the exact frame.

### Change

Commit `22c56d5` moves the direct QoS 0 attempt ahead of `_check_nowait_publish_capacity()` in `publish_nowait()` and changes nothing else. Every guard of the direct path (connected, no local terminal failure, writer epoch, engine lock, effect-pump lock, no inline drain, no pending effects) still runs before any encoding; `prepare_qos0()` still validates topic, properties, aliases, `retain_available` and the negotiated packet size; `writer.try_enqueue()` still admits on the exact frame size, epoch and bounds. The preflight remains for QoS 1/2 and for QoS 0 the direct path declines. `tests/unit/test_native_publish_nowait.py` now asserts that a ready QoS 0 `publish_nowait()` never enters the preflight or the size preview, with an empty and with a resident writer, and that writer refusal follows frame validation. The one behavioural difference is on the refusal path: with the write queue full, an invalid QoS 0 request now raises `ProtocolError`/`PacketTooLargeError` instead of `FlowControlError`, and a refused valid request pays the encode before the refusal, as it did on RC14.

### Evidence

Microbenchmark (`publish_nowait(qos=0)`, 256-byte payload, one resident writer frame, no transport, pinned CPU, median of 7 × 20,000 calls, two alternated passes):

| Tree | MQTT 3.1.1 | MQTT 5 |
| --- | ---: | ---: |
| RC14 `c194597` | 2.31 µs | 2.34–2.58 µs |
| Branch before (`8e29cfa`) | 3.26–3.47 µs | 3.32–3.48 µs |
| Branch after (`22c56d5`) | 2.31–2.35 µs | 2.35–2.37 µs |

`benchmarks/paired_writer_capacity.py` (closed-loop `publish_nowait`, QoS 0, 256 bytes, outstanding 64, 60,000 messages, six alternated fresh-process pairs, publisher on CPU 2, Mosquitto 127.0.0.1:11883, advisory policy without runner preflight; JSON under `/tmp/bench/paired_*.json`, SHA256 prefixes `71a8c5fa`, `5a690a08`, `ebd512fc`, `680c4d45`, `85db6f96`, `da857267`):

| Pair | Protocol | Candidate / base completed rate | Base CV | Candidate CV |
| --- | --- | ---: | ---: | ---: |
| RC14 → before | 3.1.1 | 0.762 | 15.2% | 11.2% |
| RC14 → before | 5 | 0.760 | 5.2% | 3.9% |
| before → after | 3.1.1 | **1.309** | 3.5% | 1.3% |
| before → after | 5 | **1.285** | 4.1% | 2.0% |
| RC14 → after | 3.1.1 | 0.975 | 1.5% | 2.7% |
| RC14 → after | 5 | 0.992 | 0.9% | 0.7% |

No synchronous rejection occurred in any run. The first RC14 → before pair has a high baseline CV and is reported for direction only; the other five are within the harness's advisory thresholds.

### Position

The `publish_nowait(qos=0)` deficit against RC14 in the closed-loop writer regime, about −24% on this host, is recovered to within −2.5% / −0.8%, inside the noise band of the harness. The `_owned_payload(bytes)` shortcut suggested as a second ablation is not taken: the stated stop rule was parity within ±3–5%, and it is met. The in-repository `lean_native_compare.py` and `lean_native_diagnostics.py` publisher cells use `publish_many()`, whose ready path was already direct-first, so they do not move with this change; the external adapter's `publish_nowait` shape is what `paired_writer_capacity.py` protects. Qualification on `22c56d5`: `ruff`, `mypy`, `bandit`; unit, project, integration (Mosquitto, `MQTTIUM_REQUIRE_BROKER=1`) and resilience suites 1,909 passed; Hypothesis and stateful fuzz suites passed.

## 13. Addendum, 2026-09-15: empty MQTT 5 property tables were instantiated per packet

An external subscriber-only campaign (`sub_exact_telemetry`, Mosquitto translating an MQTT 3.1.1 publisher's QoS 0 PUBLISH to an MQTT 5 subscriber) measured the branch at about 6.2 µs CPU per message on MQTT 5 against about 5.0 µs on MQTT 3.1.1 and about 5.0 µs for RC14 on MQTT 5, with the delivered rate sitting at exactly the CPU ceiling. The review traced the difference to `decode_properties()`: every MQTT 5 PUBLISH and ACK carries a property table, in that workload always the single byte `0x00`, and the fast path built `Properties()` for it on every packet. RC14's `Properties` was a mutable dataclass around a dict; the lean rewrite made it frozen with a `MappingProxyType` over an owned copy (commit `6fd09d8`), a contract improvement kept deliberately, but the empty fast path was never adapted to the new construction cost: dataclass, default dict, `__post_init__`, second dict, proxy and `object.__setattr__`, to represent nothing.

### Change

Commit `bef3457` binds one module-level `_EMPTY_PROPERTIES = Properties()` in `codec/properties.py` and returns it for the empty table. Immutability is what makes the value shareable: `Properties` is frozen, `values` is a read-only proxy, and `encode_properties()` returns `b"\x00"` for an empty table before touching the per-instance encode cache, so no state ever attaches to the shared value. No identity-dependent use of decoded properties exists in `src/` or the tests. The non-empty path, `Properties` mutability, the decoder and the receive pipeline are unchanged; the borrowed direct decoder of RC14 is not restored. `tests/unit/test_properties.py` pins identity across two decodes, immutability of the shared value, the absence of cached state after encoding, and independence of non-empty tables.

### Evidence

Microbenchmark on the cloud host (pinned CPU, medians; RC14 `c194597`, before `a4b79a8`, after `bef3457`; QoS 0 PUBLISH, 256-byte payload; the client row uses the scripted in-process transport with a synchronous `on_message`):

| Stage | RC14 | Before | After |
| --- | ---: | ---: | ---: |
| `decode_properties(b"\x00")` | 193–222 ns | 570 ns | **67 ns** |
| `engine.handle_raw` + `take_effects`, MQTT 3.1.1 | 2,251–2,293 ns | 1,766–1,816 ns | 1,784–1,807 ns |
| `engine.handle_raw` + `take_effects`, MQTT 5 | 2,407–2,662 ns | 2,573–2,649 ns | **2,014–2,017 ns** |
| reader → callback, MQTT 3.1.1 | 2,457–2,492 ns | 3,091–3,208 ns | 3,146–3,167 ns |
| reader → callback, MQTT 5 | 2,845–3,003 ns | 4,327–4,518 ns | **3,345–3,364 ns** |

The MQTT 5 excess over MQTT 3.1.1 on the full client path falls from about 1.3 µs to about 0.2 µs per message (RC14: 0.4–0.5 µs); MQTT 3.1.1 does not move. The engine is now faster than RC14 on both protocols; the remaining client-path gap against RC14 is the callback-reception residual of sections 9 and 11, which is protocol-independent.

`lean_native_compare.py` (self-subscribed client against Mosquitto, QoS 0, memory store, `long` bursts, 3 AB cycles and 1 AA cycle, CPU 2; `props_net_before_after.json` SHA256 prefix `95a4297d`, `props_net_rc14_after.json` `2b6d4748`), CPU per message, candidate over base:

| Cell | Before → after | RC14 → after |
| --- | ---: | ---: |
| MQTT 3.1.1 callback | 1.003 | 1.278 |
| MQTT 3.1.1 iterator | 0.990 | 1.017 |
| MQTT 5 callback | **0.889** | 1.240 |
| MQTT 5 iterator | 0.967 | 1.053 |

On this host the delivered-rate metric of that harness is not usable for the cells: its A/A control cycles ranged from 0.92 to 1.30, so only the CPU-per-message column, whose base CVs are 0.6–3% except for one cell, is reported. After the change the MQTT 5 cells cost the same relative to RC14 as their MQTT 3.1.1 counterparts, which is the expected shape once the protocol-specific allocation is gone.

### Position

The MQTT 5 reception deficit relative to MQTT 3.1.1 on this branch is explained by one avoidable allocation per packet and is closed by a shared immutable value that the frozen `Properties` contract makes safe. A non-empty table still decodes about 5–7% slower than on RC14 because of the owned copy and the proxy; that is the price of the immutability contract, on a far less frequent case, and is not revisited here. The receive-pipeline position of section 11 is unchanged.

## 14. Addendum, 2026-09-15: external post-fix validation of the two fixes

Sections 12 and 13 close with internal microbenchmarks and one in-repository `lean_native_compare.py` run. The external matrix that first reported both deficits re-ran afterwards, and its results are recorded here so a later reader does not find only the pre-fix runner output and conclude that the RTT question stayed open.

This section transcribes an **external campaign (matrix #39)**. Its raw data is not in this repository and was not reproduced on the hosts used for sections 12 and 13; it is corroborating evidence, not in-repo measurement.

### Results reported

| Workload | Reported outcome |
| --- | --- |
| `publish_nowait` QoS 0 | The hole seen on the old candidate is recovered at `c4f477d`; back to approximately RC14. |
| Exact subscriber, MQTT 5 | About 161k msg/s before, about 190–195k msg/s after; the protocol-specific CPU excess of section 13 is gone. |
| RTT QoS 1 | Initially looked regressed in the matrix. |

### RTT QoS 1: the matrix drop was environmental

The matrix result was qualified with direct paired runs rather than accepted as reported:

| Pair | MQTT 3.1.1 | MQTT 5 |
| --- | ---: | ---: |
| `c4f477d` / `8e29cfa` | 0.999 | 1.039 |
| `c4f477d` / RC14 | 1.125 | 1.154 |

The candidate is flat against its immediate predecessor, and its standing against RC14 matches the deficit already described in sections 9 and 11. No code regression is attributable to the two fixes; the matrix drop is environmental.

### Position

Both fixes hold outside the hosts that produced them. The open item remains the one stated in section 9 — the protocol-independent callback-reception residual against RC14 — and no new deficit is introduced. The earlier dated reports are left as written; this addendum supersedes their runner-level RTT reading.
