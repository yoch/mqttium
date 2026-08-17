# Engine quality audit closure — 2026-08-17

| | |
| --- | --- |
| Date | 2026-08-17 |
| Closure base | `4404d4d4` |
| Original audit | `ENGINE-QUALITY-AUDIT-2026-08-17.md` |
| Independent review | `ENGINE-QUALITY-AUDIT-INDEPENDENT-REVIEW-2026-08-17.md` |
| Purpose | Record the final disposition after E1–E4 and O1 were resolved, and prevent unproven simplification churn from being reintroduced later. |

This is a dated closure record. It does not rewrite the original audit or its
independent review. Those reports remain useful evidence of what was found and
why two of the original recommendations had to be corrected.

## Final defect status

- **E1 — closed in #258.** Clean-session purge now removes stale blocked replay
  entries before failing their durable records.
- **E2 — closed in #258.** Buffered packets arriving after a terminal engine
  state no longer replace the real disconnect/refusal reason; a runtime boundary
  regression covers refused CONNACK plus trailing PINGRESP in one read.
- **E3 — closed in #258.** `begin_connect()` rejects `DISCONNECTING` while still
  allowing a new connection after transport-close teardown completes.
- **O1 / #260 — closed in #261.** Engine invariant `AssertionError`s escape
  peer-protocol containment, remain the runtime failure cause and suppress
  automatic reconnect; generic persistence/store exceptions retain their
  previous containment contract.
- **E4 / #259 — closed in #262.** MQTT 5 Maximum Packet Size values 1, 2 and 3
  remain legal negotiation values. If a required automatic PUBACK/PUBREC/PUBCOMP
  cannot fit, mqttium now fails locally before mutating/delivering the affected
  QoS exchange, preserves durable QoS 2 state where relevant and does not blame
  the peer or reconnect automatically.

At #262's final HEAD (`d822862d`) the exact synthetic merge ref passed 1199 unit
tests, 90.30% total coverage, 92% coverage of the tiny-limit rare path, ruff,
mypy, bandit, Python 3.11–3.14 plus Mosquitto integration, Windows/macOS jobs,
package validation, seeded/Hypothesis fuzz, long fuzz smoke and Linux MQTT 5 /
3.1.1 reconnect/backpressure soak. The merged `main` commit is `4404d4d4`.

## Simplification candidates — final disposition

The original audit listed three non-blocking simplification candidates. A
repository-wide follow-up was performed from the CI-built source distribution,
so this pass included `tests/`, `benchmarks/`, fuzzing and compatibility code
that the original engine-only audit deliberately excluded.

### S1 — remove ProtocolEngine compatibility facades: rejected

The candidate described roughly 80 lines of views such as `packet_ids`, `flow`,
`_queued`, `_topic_aliases`, `_inbound_inflight`, `_pending_sub_mids` and the
pending-outbound counters.

The repository-wide evidence does not support removal:

- `ProtocolEngine` and `FlowControl` are explicitly **Provisional** APIs in
  `docs/API-STABILITY.md`. Provisional means revisions require a changelog entry
  and migration guidance; it is not permission for silent breakage.
- `engine.flow` has production consumers in `AsyncClient`.
- The remaining views are heavily used by the unit tests, fuzz/stateful
  invariants and retained benchmarks. They are diagnostic seams that keep those
  callers from reaching further into `InboundSession` / `OutboundSession`.
- The dedicated 2026-08-16 simplification audit already removed the genuinely
  dead setters and explicitly classified the remaining compatibility views as
  **load-bearing**.

Moving every diagnostic/fuzz caller to directional-session internals would
reduce a few facade lines while increasing coupling to implementation owners and
weakening the stable shape of invariant checks. No correctness or measured
performance benefit offsets that cost.

**Decision: keep.** Revisit only as an intentional Provisional API migration,
not as cleanup.

### S2 — factor the three QoS 1 MESSAGE-emission blocks: rejected

Two nearly identical MESSAGE constructions live in `_on_qos1_auto`; a related
fresh-message block exists in `_on_qos1_manual`.

A helper on the normal automatic branch would add a Python frame to one of the
measured publish/acknowledgement paths. The 2026-08-16 simplification campaign
records roughly a 1.5% cost per added Python frame on the publish path and
therefore deliberately removes such forwarding layers where justified.

Restricting a helper to the cold duplicate-in-current-batch branch avoids that
performance risk, but then it does not materially simplify the file: the helper
still needs topic, payload, MID, retain, DUP, properties and decoded-property
metadata, while the normal block must stay inline. The existing `_emit_message`
helper is for persisted `InboundMessage` replay and intentionally has different
semantics (`requires_delivery_mark=True`, plain MESSAGE effect, stored fields).
Stretching it to cover fresh decoded messages would mix those contracts rather
than simplify them.

**Decision: keep the explicit blocks.** Their duplication is smaller and clearer
than the abstraction required to remove it.

### S3 — inline `_check_publish_size` into `validate_against_negotiated`: rejected

`_check_publish_size` currently has one internal caller and delegates to
`size_parts()` plus `_check_publish_wire_size()`.

Inlining it would delete only a handful of lines and one cold-path method call.
It would not remove the underlying responsibilities:

- `size_parts()` is shared with normal outbound admission;
- `publish_wire_size()` is the exact-size path used by `publish_nowait()` and is
  backed by a dedicated performance/correctness report (`NOWAIT-WIRE-SIZE.md`);
- `_check_publish_wire_size()` is the negotiated-limit check that deliberately
  returns before exact wire-size arithmetic when the broker advertised no
  limit.

The wrapper therefore names a coherent operation used during negotiation/replay
validation. Removing it gives no observable performance, memory, API or
correctness benefit and makes a measured sizing area noisier for negligible LOC
reduction.

**Decision: keep.** No refactor without a concrete maintenance or performance
problem.

## Closure verdict

The 2026-08-17 engine-quality audit is **closed** at `main=4404d4d4`:

- every accepted correctness/hardening finding is resolved;
- #259 and #260 are closed as completed;
- the corrected MQTT 5 interpretation is implemented and regression-covered;
- no simplification candidate has evidence strong enough to justify production
  churn;
- no further code change is recommended from this audit.

Future work should treat the three rejected simplifications as decided unless a
new requirement or repository-level measurement changes the trade-off.