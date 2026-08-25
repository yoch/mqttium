# Engine structural simplification

| | |
| --- | --- |
| Date | 2026-08-25 |
| Base commit | `ee2f1eb` (main) |
| Scope | The engine in the broad sense: `protocol/` and `api/` (`async_client.py`, `_writer.py`, `_effects.py`, `_delivery.py`) |
| Method | Full read of both layers, call-site tracing for every candidate removal, then the same gates as [`SIMPLIFICATION-AUDIT-2026-08-16.md`](SIMPLIFICATION-AUDIT-2026-08-16.md) |

This is a dated report. It records what was true of the named commits and what
was done about it. Do not edit it to match later code — write a new report.

## Question

Successive incremental fixes since the 2026-08-16 audit left new duplication
running in parallel inside the runtime: two effect interpreters repeating the
same acknowledgement resolution, two per-kind copies of batch delivery, four
copies of persisted inbound record completion, and repeated lifecycle
boilerplate. Which of it can be removed without paying for it in performance —
and does any of it hide a defect?

## Gate

`benchmarks/hotpath_profile.py` over the full scenario registry, both trees
measured warm (second run after a cold pass), is the primary gate — exact
Python call counts, deterministic on a shared host.
`benchmarks/paired_regression.py --repeat 7` provides advisory paired timing.
Functional gates: `ruff format --check`, `ruff check`, `mypy`, `bandit`,
`pytest tests/unit tests/project --cov`, `pytest tests/integration` against a
live Mosquitto on `127.0.0.1:11883`, `tests/fuzz/fuzz.py --seed 1
--iterations 20000`, the Hypothesis and stateful invariant suites, and
`mkdocs build --strict`.

## Result

Calls per operation: **all 28 non-threaded scenarios are exactly +0.00%**
against the base tree. The two `compat_*` scenarios moved +0.39% and +0.98%;
re-running them on identical code swings several percent (92.8–103.3 calls/op
for `compat_publish_qos0_batch` across three runs of the same tree), so both
remain reported-not-gated, as the 2026-08-16 audit established.

Advisory paired timing (median candidate/base over 7 pairs, shared cloud
host): every synchronous scenario within 0.987–1.049 with base CVs of the same
magnitude — no systematic movement in either direction.

Tests go from 1450 to 1451 passing — four regression tests added for the
defect below, three tests of a removed dead Provisional helper deleted —
with integration (16) and both fuzz suites green; coverage rises from 89.80%
to 90.27%. Source across `src/` is a net **−122 lines**: `inbound.py`
1230→1163, `_delivery.py` 963→927, `_effects.py` 396→383, `buffer.py`
248→216, `async_client.py` 2602→2593, `engine.py` 962→964, `outbound.py`
1272→1303 (+31 for the defect fix below, −14 of pre-existing duplication).

## What was changed

Each wave is an independently revertable commit, gated as above.

### 1 — Cold-path dedup in `AsyncClient`

- One FIFO receipt registry (`_fifo_register`/`_fifo_pop`) replaces the two
  register/pop method pairs for publish and batch receipts. Runtime call
  sites call the module helpers directly, so the hot paths keep one frame;
  the historical method seams delegate for tests and benchmarks.
- One SUBACK/UNSUBACK resolution shared by `_apply_effect_inline` and
  `_apply_effect`; one `_terminal_publish_result` shared by `_apply_effect`
  and teardown settlement.
- One `_extend_message_effects` materialisation for borrowed-decode QoS 0
  messages, previously copied between `_process_direct_qos0_batch` and the
  read loop.
- One `_connect_explicit` body under the three explicit connect entry points.
  The TCP path keeps the load-bearing conditional factory reset: an injected
  `_transport_factory` must survive a plain TCP reconnect (test seam and
  custom-transport seam; unconditional reset broke 73 tests immediately).
- One `_terminal_shutdown` for the three terminal application-stream
  teardowns; the reconnect loop no longer repeats the teardown-final
  bookkeeping `_fail_pending` already owns.
- `subscribe()`/`unsubscribe()` share one flush-and-await-ACK tail;
  `_notify_publish_space` delegates to `_wake_publish_waiters`.

### 2 — One batch delivery for MESSAGE and DECODED_MESSAGE

The 2026-08-16 audit deferred merging `deliver_batch_inline` /
`deliver_decoded_batch_inline` because a shared body reads
`effect.decoded_property_wire_size` on the plain MESSAGE path. Applied now
under this campaign's brief (structure over single attribute loads):
producers pair the size with DECODED_MESSAGE and leave it `None` on MESSAGE,
so one `deliver_message_batch_inline` selects the smallness test per effect
and no longer splits a mixed prefix at the kind boundary. `EffectPump` and
`AsyncClient` dispatch one batch acceptor. Measured cost: +0.00 calls/op
everywhere; paired timing on the three `delivery_*` scenarios 0.989–1.007
(base CVs 0.5–4.1%).

### 3 — Inbound and engine structure

- The three PUBLISH decoders select the QoS 1/2 handler once instead of
  duplicating the keyword tail; `_on_qos1_auto` shares one PUBACK+MESSAGE
  emission between its fresh and retransmitted paths (branches only — no new
  frame on the ingress path).
- One `_complete_stored_inbound` replaces the four transition-or-pop copies
  that settled persisted inbound records (recovered QoS 1, PUBREL completion,
  manual acknowledgement, ordered manual QoS 1 drain).
- `_on_connack` now reads as decode → refusal → validation → negotiation →
  replay, with the MQTT 5 property obligations and Session Present
  contradictions in named validators on the same once-per-connection path.
- `OutboundSession.complete_record` is an explicit alias of `discard_record`;
  the two identical bodies had already drifted into separate copies once.

## Defect found and fixed

`tests/fuzz/test_stateful_invariants.py` fails on the **unmodified base tree**
at seed 1, step 188 (`pending_messages=10 but the store holds 11 records`).
CI never sees it: the PR gate runs `MQTTIUM_FUZZ_SEEDS=2 MQTTIUM_FUZZ_STEPS=50`
and the failure needs 188 steps.

`replay_session()` parks a WAIT_* record in `_queued` when the send quota
cannot admit its retransmission. A broker acknowledgement can settle the
exchange while it is parked; the stale queue entry then made the next
`drain()` re-materialise the deleted record, resurrect it through
`update_out`, and retransmit a settled publication — the same hazard
`purge_after_clean_session()` already documents and guards, but on the
ordinary acknowledgement path. A freed identifier could also be reallocated
while the stale entry sat in the queue, breaking the duplicate-mid invariant.

`_settle()` now removes the parked entry with the record. A parked-entry
counter keeps the common case at one integer truth test per settlement and
per drained message — `qos1_cycle_*`, `mqtt5_puback_reason_cycle` and
`ingress_publish_qos1` stay at exactly +0.00 calls/op.
`tests/unit/test_replay_parked_settlement.py` pins the settlement, the
resurrection ban and identifier reuse over Memory and SQLite stores; the full
stateful suite (12 seeds × 200 steps) passes on the fixed tree.

## Second pass: secondary engine layers

The persistence, codec, packet, transport and dispatch layers were fully
audited on 2026-08-16 and received only reviewed fixes since, so this pass
looked specifically for accretion those fixes left behind.

- **`IncrementalDecoder.process_packets_bounded` removed** (Provisional, with
  changelog and migration note). The direct-QoS 0 ingress work gave the
  client's read loop its own count/byte bounds plus the auto-acknowledgement
  handoff boundary, orphaning the method; its unbounded sibling
  `process_packets` was removed for the same reason in 1.0.0rc7.
- **`in_replay_pages` flush dedup** in both built-in stores: each carried
  three copies of "hydrate the accumulated identifiers, yield if non-empty,
  reset"; one local helper per implementation now owns it. Replay-only, cold.
- **Examined and declined: the conditional-transition shells.**
  `complete_out`/`transition_out` and `complete_in`/`transition_in` share a
  read-validate-mutate-commit shell in `SqliteInflightStore` (~60 dedupable
  lines). Every zero-cost factoring shape was ruled out: any shared shell adds
  at least one Python call per conditional mutation, and these run once per
  acknowledgement on the SQLite hot path (`qos1_cycle_sqlite`,
  `qos2_cycle_sqlite`). Declined on the same rule the 2026-08-16 audit applied
  to the ACK decoders.

## Looks redundant, is load-bearing (additions to the 2026-08-16 list)

- **The conditional transport-factory reset in `_connect_explicit`.** `connect()`
  reclaims `TcpTransport.connect` only when leaving a Unix/WebSocket endpoint;
  an injected factory must survive a plain TCP connect.
- **The four receipt-registry method seams.** Tests and benchmark scenarios
  construct against them on both sides of an A/B run; keep them delegating.
