# Performance audit for 0.2.0b4

This audit re-reads the 2026-08-09 cross-client record
([`CROSS-CLIENT-BENCHMARK-2026-08-09.md`](CROSS-CLIENT-BENCHMARK-2026-08-09.md), written against
`0.2.0b2`) against the current tree, folds in the still-open performance work, and records the
quick wins taken before the `0.2.0b4` tag. Like
[`QUALITY-AUDIT-0.2.0b4.md`](QUALITY-AUDIT-0.2.0b4.md), it is an inventory rather than a claim that
every item must be redesigned. The goal for this cycle was explicitly *not* expensive optimisation:
it was to remove avoidable per-message work and design errors while the beta stabilises.

## Reference and reproduction

- Base commit: `4bdcdb3` (`main`, after `v0.2.0b3`).
- Date: 2026-08-10.
- Verified on CPython 3.12.13, Linux 6.8.0-136-lowlatency, glibc 2.39.
- Unit suite 656 passed; `tests/integration` 8 passed against Mosquitto on `127.0.0.1:11883`;
  `tests/fuzz/fuzz.py --seed 1 --iterations 20000` clean on codec, engine and websocket targets;
  Hypothesis fuzz 4 passed; Ruff, mypy and Bandit clean.
- `benchmarks/memory_profile.py` followed by `check_memory_thresholds.py`: every scenario within
  its limit with `benchmarks/memory_thresholds.json` **unchanged**.

No generated number is committed. Figures quoted from earlier work are cited to the run that
produced them.

## Verdict on the 0.2.0b2 record

| Claim in the 2026-08-09 record | Verdict |
| --- | --- |
| Fastest client measured at QoS 0, in both identities | **Still true of the code.** The direct writer path is unchanged at `api/async_client.py:548-585`. |
| The QoS 0 lead is silently conditional on `on_publish is None` | **Still true, now pinned.** PR #64 covered `publish_nowait` only; the async `publish()` entry, `publish_many`, both pending-effect gates and re-enabling after the callback is cleared are pinned here. |
| QoS 1 costs ÷2.74 against QoS 0, and 2.95× gmqtt's PUBACK p50 | **Never verified in this repository.** Issue #39's same-runner RTT figures put MQTTium at roughly 90% of gmqtt, which does not support a 3× framing, and #39 already withdrew cross-machine absolutes as evidence. Treat the in-repo number as authoritative until the external native-async harness is available. |
| The benchmark adapter is not the explanation | **Still true.** |
| The `on_publish is None` fast path is a pessimisation under load | **Mis-stated.** That A/B compared two *adapter* disciplines — awaiting each receipt versus correlating acks in a callback — not the internal branch. `_apply_effect_inline` and `_apply_effect` both call `_settle_publish` unconditionally; the callback only adds work on top. The result is a statement about how an application observes completion, and it argues for a cheaper receipt (finding 6), not against the branch. |
| `_engine_lock` contention serialises the completion path | **Refuted.** #39 measured zero contention across the instrumented scenarios, and the reader's critical section contains no `await`, so the lock is never held across a suspension on the outbound QoS 1 path. |
| The Paho façade plateaus at QoS ≥ 1 and barely benefits from pipelining | **Still true, and now root-caused.** See findings 7 and the escalation below. |
| `max_outbound_bytes` default is surprising next to the message bound | **Still true.** Addressed by naming the bound rather than changing it (finding 8). |
| Issue #57, `ProtocolError` while disconnecting | **Closed** by PR #58. |

## Findings and what was done

Each finding names the mechanism, not an estimate. Magnitudes are deliberately absent where this
cycle could not measure them — see "What this cycle could not measure".

1. **The reader confirmed an empty buffer by decoding it again.**
   `api/async_client.py:1215-1233`. `process_packets_bounded` stops early only when
   `next_packet()` returns `None`, so a batch that reached neither the count nor the byte bound had
   already drained the buffer. The loop re-entered anyway to observe `handled == 0`, costing a
   second engine-lock acquisition, a second store batch context and a second bounded decode on
   every read that did not saturate a bound — at a window-limited QoS 1 workload, once per
   acknowledged message. *Fixed;* `_read_loop` complexity fell from 21 to 20. Pinned in
   `tests/unit/test_read_loop_batching.py`, including the two bound-hitting cases that must still
   re-enter.

2. **Every acknowledgement probed the batch-receipt table.** `api/async_client.py:1821-1832`.
   `_settle_publish` looked up `_batch_receipts` for every settled identifier, including for the
   clients that never call `publish_many`; and `_pop_batch_receipt` still did the `get`-then-`pop`
   pair that PR #72 removed from `_pop_publish_receipt`. *Both fixed.*

3. **Every QoS 1/2 publish constructed a `QoS` enum to be rejected.** `api/async_client.py:567`,
   `:597`. Both direct QoS 0 entry points converted before comparing. `QoS` is an `IntEnum`, so
   comparing first is equivalent; an invalid level still raises `ValueError`, now only from
   `_validate_publish_request`. *Fixed and pinned, including the invalid-level cases.*

4. **Ordered effect batches built two partition lists and discarded them.**
   `api/_effects.py:93-122`. The pump partitions SEND-first so wire order survives, but it
   allocated both lists on every multi-effect batch even when nothing needed reordering — the shape
   of inbound delivery, which is a batch of `MESSAGE` effects with no `SEND` among them. *Fixed:*
   detect first, which needs no allocation, and partition only when the scan finds a `SEND` after a
   non-`SEND`. A batch that does need reordering now pays a few extra comparisons before the same
   partition. `benchmarks/paired_regression.py` gains `effect_batch_ordered` and
   `effect_batch_reordered`; the existing `effect_batch_inline` is all-`SEND` and reaches neither
   arm.

5. **Every QoS 1/2 publication allocated an `asyncio.Event`.** `api/models.py:171-205`. Awaiting one
   costs two coroutine frames plus the future and waiter deque the event builds internally, and a
   publication that is never awaited — the entire point of `publish_nowait` — paid for the event and
   used none of it. *Fixed:* completion is a flag and the future behind `wait()` is created on first
   use. The future only ever carries `None`; failures stay in `_error` and are raised after it
   resolves, so a failed, never-awaited receipt cannot leave an unretrieved exception for asyncio's
   default handler to print — which matters because `src/` installs no logging. Pinned, along with a
   repeated settle that must not hit `InvalidStateError`.

6. **`publish_nowait` sized packets the writer could not use.** `api/async_client.py:1328-1345`.
   Admission sized the packet to ask whether it fit, and `queue_publish` sized it again — on MQTT 5
   with properties, two `encode_properties` calls before the third that builds the real frame. This
   is the defect PR #40 fixed *inside* the engine, left standing in the admission check outside it.
   *Fixed* by observing that an empty writer queue admits a single item of any size, so the size
   cannot change the answer. The batch path still sizes every request because it accumulates those
   sizes. **Deliberately not fixed by threading the sized parts into `queue_publish`**, which would
   widen the hottest signature in the library for a case the emptiness test already covers.

7. **The Paho façade charged every user for a callback they never installed.**
   `compat/paho.py:186-208`. `_async.on_publish` was assigned unconditionally at construction, and
   the dispatcher returns immediately when the user's `on_publish` is `None`. So every façade user
   paid a deque entry, a callback-queue hop, a worker wakeup and a coroutine per acknowledged
   publication to reach a no-op — and could never satisfy `_direct_qos0_ready()`. *Fixed:*
   `Client.on_publish` is a property that installs and clears the inner callback through
   `_run_loop_mutation`. **Scope, stated precisely:** the façade reaches the engine through
   `_queue_publish_on_loop`, not `AsyncClient.publish`, so this does not by itself route façade
   traffic onto the direct QoS 0 writer path. What it removes is the per-completion callback hop.

8. **The two writer bounds are of deliberately different magnitudes and neither error said so.**
   `api/async_client.py:137-138`. `max_outbound_bytes` (1 MiB) against `max_outbound_messages`
   (10 000) implies about 105 bytes per queued message, so the byte bound binds first as payloads
   grow: 1 MiB admits roughly 16 outstanding 64 KiB publications, not 10 000. The external harness
   saw 76% refusals at 64 KiB and 98% at 1 MiB until it sized the byte bound by hand. *`FlowControlError`
   now names the bound and its configured value at every refusal site, built only on the error path.
   Defaults are unchanged*: widening one before a release would loosen a memory guarantee the memory
   campaign established. Documented in `README.md` and `docs/IMPLEMENTATION-GUIDE.md` §10.

9. **The inflight tables were reallocated to release nothing.** `persistence/memory.py:189-195`,
   `:281-286`. Both tables were replaced whenever the last record left, to drop a peak-sized hash
   table. At an inflight window of 1 to 8 the table never grows but does drain to empty on every
   acknowledgement, so the store allocated a fresh dict per message for no benefit. *Fixed* by
   reading the emptied table's own footprint — a dict does not shrink on delete, so it still reports
   what its peak demanded — rather than tracking a high-water mark on the insert path. The two
   capacity tests asserted object identity after a single record, which pins the mechanism rather
   than the guarantee; they now grow the table to 512 records and assert the released footprint.

## Measured and rejected

- **`OutboundRecordMeta` allocated per acknowledgement** (`persistence/memory.py:231-240` →
  `protocol/outbound.py:504`), solely so the session can read `logical_size`. The type is already
  `slots=True, frozen=True`, and removing the allocation means changing the `TransitionInflightStore`
  protocol across both stores and CLAUDE.md's persistence contract. Complexity out of proportion to
  the gain; left alone deliberately.
- **Reordering `on_puback`'s emissions** so a single-ack batch never reaches the pump's reorder
  branch. It helps only single-ack batches, and it works *only* because the pump partitions
  SEND-first — creating exactly the `protocol/`↔`api/` coupling the layer separation exists to
  prevent. Rejected; finding 4 covers the same cost without the coupling.
- Issue #39's earlier rejections stand and should not be re-litigated: callback-worker batch drain
  (≤1%, rejected twice), the queue-inline effect fast path (−7.7%/−8.6% with event-loop lag p95
  rising from 2.1 ms to 9.7 ms), and a synchronous inline callback mode (a contract change to
  blocking, exception, reentrancy and fairness semantics).

## Open work folded in

- **Issue #39, "Native hot-path performance program"** — stays open. It holds the deepest
  measurement corpus in the project and its acceptance criteria gate every change above. This audit
  belongs there as a comment, not as a replacement.
- **PR #74, long fuzz campaigns** — draft. Reliability evidence for the beta rather than a
  performance item; it should land before the tag regardless of this program.

## Escalations: documented, not fixed

- **The façade's per-message handoff.** `compat/paho.py:682-698`. A QoS ≥ 1 producer blocks on a
  `concurrent.futures.Future` per message, so a single producer thread has an empty queue when the
  drain runs: instrumentation counted 4 000 drains for 4 000 publications, against ~4.1–4.3 messages
  per drain with eight producers. This is inherent to Paho's contract — `publish()` must return an
  allocated MID, and MIDs are allocated on the loop. Escaping it means pre-allocating MIDs off-loop,
  which breaks the packet-id and single-source-of-truth invariants, or adding an entry point outside
  the Paho surface. Accepted as an architectural cost.
- **`EngineConfig.local_receive_maximum = 65535`** (`protocol/config.py:34`) against `AsyncClient`'s
  `100` (`api/async_client.py:124`). Direct engine consumers and client consumers get different
  inbound windows and nothing explains the split. Documenting it is right; aligning a default before
  a beta tag is not.

## What this cycle could not measure

The paired harnesses were run on the audit host and **produced no usable evidence**, so no throughput
or latency claim is made for any change above.

- `benchmarks/paired_regression.py --repeat 9` on `compat_publish_qos1` returned a base-arm
  coefficient of variation of **18.3%**, with the *same* code measuring between 6 249 and 13 171
  ops/s across pairs. `docs/BENCHMARKING.md` invalidates a cell above 5%. An earlier run on the
  effect scenarios was equally unstable (base 105k–186k ops/s). The host was not idle: the audit
  itself was running on it. Per the validity contract this is reported as `N/A` rather than as a
  manufactured comparison.
- Consequently the changes above are justified by **removed work that is visible in the code and
  pinned by tests** — a decode call, a lock acquisition, a dict lookup, an enum construction, two
  list allocations, an event, a duplicate property encode, a callback hop, a dict reallocation —
  and not by a measured percentage. Confirming their size needs the dedicated runner with
  `runner_probe.py --enforce`, or CI's paired jobs.
- **Absolute latency remains unconfirmable in CI.** Hosted runners present no `performance`
  governor, so the record's 1.17 ms and 2.99 ms p50 figures can be neither reproduced nor refuted
  there; only same-runner *ratios* survive.
- **Event-loop-lag fairness is not reported by `paired_network.py`.** Findings 1 and 4 both touch
  scheduling boundaries, and a fairness regression would be invisible in CI. The EffectPump counters
  (`inline_effects`, `reordered_batches`, `apply_suspensions`, `pending_high_water`) are the
  available proxy and are asserted in unit tests; direct loop-lag evidence for those two changes is
  absent.
- **The cross-client matrix is not reproducible here.** It needs `mqtt-python-client-bench` with
  per-role pinning across disjoint physical-core groups. The 0.2.0b2 rankings are therefore not
  restated as current, and the comparison stays parked on #39 pending the external native-async
  harness.

## Before the tag

1. Run the paired harnesses on the dedicated runner, or read CI's jobs, and attach the ratios to
   issue #39 — every change above is currently unquantified.
2. Sweep `paired_network.py --windows 1,8,32,64,128 --completions receipt,callback`, and settle the
   completion-discipline question in-repo against the record's 5.13/4.19 ms, stating the
   closed-loop-at-window versus open-loop-at-load-fraction difference.
3. Land PR #74 and run a soak plus `application_stress.py`: finding 1 changes the ingress
   backpressure loop and finding 9 changes store behaviour.
