# Independent review of the 2026-08-17 engine audit

| | |
| --- | --- |
| Date | 2026-08-17 |
| Audit reviewed | `ENGINE-QUALITY-AUDIT-2026-08-17.md` at `a8432023` |
| Base independently checked | `4677e550` |
| Purpose | Preserve the useful findings while correcting conclusions that do not survive independent review. |

The original audit is a dated record and is intentionally left unchanged. This
report is the follow-up review required by `docs/reports/README.md`: it records
which findings remain accepted, which claims are narrowed, and which follow-up
issues need a different solution.

## Verdict

- **E1: accepted.** The stale flow-blocked replay queue is a real reconnect bug
  and the fix in `a8432023` is correct.
- **E2: accepted.** Ignoring buffered packets after the engine reaches a terminal
  state prevents transport noise from replacing the real terminal reason. A
  runtime-level regression now covers a refused CONNACK followed by a PINGRESP
  in the same transport read, not only the engine state transition.
- **E3: accepted.** `begin_connect()` must reject `DISCONNECTING`; reconnecting
  after the transport-close transition remains legal.
- **E4: finding retained, original diagnosis/recommendation rejected.** A broker
  Maximum Packet Size of 1, 2 or 3 is legal MQTT 5; only zero is forbidden.
  MQTT 5 §3.2.2.3.6 requires the Client not to send a packet larger than the
  advertised value. MQTTium therefore must handle an automatic ACK that cannot
  fit as a local capability/connection failure, not classify the CONNACK itself
  as a peer Protocol Error. Issue #259 is rewritten around that distinction.
- **O1: accepted, proposed one-line fix incomplete.** Internal invariant
  `AssertionError`s should not be converted into peer-attributed
  `PROTOCOL_ERROR`s. However, simply letting the assertion escape is unsafe:
  the current reconnect policy retries failures without an MQTT reason code,
  which could reuse an engine after an invariant violation. Issue #260 is
  rewritten to require both preservation of the original invariant failure and
  terminal reconnect behaviour.

## Corrections to E1 consequences

The core E1 failure is reproduced and the fix is retained. One consequence in
the original report was too broad: it named the shipped `MemoryInflightStore` as
an example of an object-backed replay path that could retransmit a packet id
after the pool was cleared. The current memory store implements
`PagedInflightStore` and `out_summary_pages()`, so shipped replay uses
`OutboundMessageSummary` entries on this path. A third-party non-paged store can
have different materialisation behaviour, but that variant must not be stated
as behaviour of the shipped memory store.

## Corrections to E2 wording

The observable bug is real, but `Client.disconnect_reason` is not a public
mqttium API surface. The failure path is the runtime's stored disconnect
exception / connection-refusal result, while compatibility adapters retain
structured `DisconnectInfo`. The new boundary test exercises the strongest
case: a refused CONNACK and a trailing packet are decoded from one read, and the
caller must still receive `Connection refused: reason_code=5`.

The terminal-state check added by E2 does execute on ingress. An isolated CPython
expression probe found the two-element tuple membership form measurably slower
than chained identity checks, but that probe is not a repository benchmark and
is insufficient evidence for another production change. This review follow-up
therefore changes **no production code**. The code under performance-sensitive
paths remains byte-for-byte the `a8432023` implementation; non-regression is
validated by the ordinary quality/fuzz/integration/soak gates rather than by a
micro-optimisation based on an unrelated host.

## E4 / issue #259

MQTT 5 §3.2.2.3.6 says Maximum Packet Size in CONNACK may be any non-zero Four
Byte Integer value and requires the Client not to exceed it. Therefore:

1. `maximum_packet_size=1..3` must not be rejected merely for being below a
   PUBACK/PUBREC/PUBCOMP frame size.
2. `_validate_new_outbound_effects()` currently removes a produced handler batch
   and raises `PacketTooLargeError`; `handle_raw()` then exposes that as a
   `PROTOCOL_ERROR`. The original report's "silent loss with no error" wording
   is inaccurate.
3. The useful hardening target is to detect a mandatory automatic response that
   cannot fit **before irreversible inbound state/application-delivery effects
   are committed**, and to terminate locally without blaming the peer.
4. Rejecting the connection at CONNACK can be considered only as an explicit
   mqttium policy choice, not as MQTT conformance enforcement; a tiny negotiated
   limit can still be useful for receive-only / QoS 0 scenarios.

## O1 / issue #260

`handle_raw()` intentionally contains generic handler/store exceptions, but an
engine assertion is qualitatively different: it demonstrates a violated local
invariant. The desired behaviour is:

- preserve the original `AssertionError` rather than stringify it into a
  `PROTOCOL_ERROR`;
- make that failure terminal for automatic reconnect;
- keep the existing containment contract for ordinary persistence/store
  failures unless a separate policy decision changes it;
- test both engine propagation and runtime reconnect suppression.

## Simplification recommendations

No simplification from the original report is approved for immediate work.
`ProtocolEngine` is publicly re-exported and several compatibility facades are
explicitly retained as historical/public diagnostic seams, so removing them is
not justified by an audit that excluded tests and `compat/`. The MESSAGE helper
and sizing-helper proposals save too little complexity to justify churn in
measured hot paths without separate evidence.

## Merge criterion for #258

The bugfix portion is acceptable once the follow-up runtime regression passes on
the current PR HEAD and the required CI matrix is green for that exact revision.
The original report should be read together with this review; #259 and #260 are
follow-up work and are not prerequisites to merge E1-E3.
