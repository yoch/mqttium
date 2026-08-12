# Hardening audit for 1.0.0rc2

Dated record of an independent hardening pass over MQTTium `1.0.0rc2`
(commit `c34a949`, tip of `main` after #183). This report is not a contract:
it describes what was examined on 2026-08-12 and which defects were confirmed.
Do not cite it as current behaviour after subsequent fixes land — supersede it
with a newer report instead.

## Method

1. Start from the RC1 / RC2 hardening history (`#84–#91`, `#110–#115`,
   `#126–#134`, `#154–#155`, PR `#181`, PR `#183`) and from
   `docs/CONFORMANCE.md` (which already states that the discovery rate is not
   yet zero).
2. For every candidate: cite a rule (MQTT statement, `IMPLEMENTATION-GUIDE`
   invariant, or an existing intentional regression test), check documentation /
   comments / RC tests for intent, reproduce with a minimal harness, then
   classify as **BUG**, **intentional**, **already fixed**, or **inconclusive**.
3. Open one public `[Bug]` GitHub issue per confirmed defect, with an executable
   reproduction. True security advisories would follow `SECURITY.md` (private
   reporting); every finding below is a protocol / lifecycle correctness defect
   in the same category as prior RC audits.
4. No runtime fixes in this pass — issues only.

Harnesses used: `ProtocolEngine` + `IncrementalDecoder`, and `AsyncClient` with
in-process fake transports. Python 3.12.3 on Linux x86_64; `PYTHONPATH=src`.

## Confirmed findings

| ID | Issue | Summary |
| --- | --- | --- |
| F1 | [#186](https://github.com/yoch/mqttium/issues/186) | `_protocol_disconnect` emits a DISCONNECT larger than negotiated Maximum Packet Size |
| F2 | [#187](https://github.com/yoch/mqttium/issues/187) | Local `_protocol_disconnect` leaves `AsyncClient` transport / reader alive (zombie) |
| F3 | [#188](https://github.com/yoch/mqttium/issues/188) | CONNACK protocol violations during `connect()` surface as `MQTTTimeoutError` |

### F1 — oversized DISCONNECT from `_protocol_disconnect` (#186)

`ProtocolEngine._protocol_disconnect` and `InboundSession._protocol_disconnect`
`_send` an MQTT 5 DISCONNECT then `raise ProtocolError`. `handle_raw` catches
that exception **before** `_validate_new_outbound_effects`, so a 4-byte
`SEND` (`e0028200`) survives when `negotiated.maximum_packet_size ∈ {1,2,3}`.

Contrast: `_reject_auth_method` / `accept_auth=False` return without raising, so
the same size gate correctly converts the batch into `PacketTooLargeError`.
PR `#181` / `#183` covered intentional `disconnect()`, fatal disconnect, and
client-side AUTH rejection — not these engine paths.

Triggers observed: broker AUTH `0x19`, invalid inbound Topic Alias, Receive
Maximum exceeded, Clean Start with Session Present, empty ClientID without
Assigned Client Identifier.

### F2 — zombie connection after local protocol disconnect (#187)

After `_protocol_disconnect`, effects are `SEND`, `DISCONNECTED(from_broker=False)`,
`PROTOCOL_ERROR`. `AsyncClient` closes the transport only for
`from_broker=True`. The terminal `PROTOCOL_ERROR` raises inside `EffectPump`;
`_done` clears `error` when `pending` is empty (intentional anti-poison policy
for *unowned* failures — see
`test_protocol_error_is_not_reported_against_a_later_unrelated_publish`, where
the engine **stays CONNECTED**).

With broker AUTH `0x19` after a live session: `engine=DISCONNECTED`,
`is_connected=False`, transport still open, reader still running,
`on_disconnect` never called, `_disconnect_exc is None`.

Do **not** merge this with the intentional “rude second CONNACK must not take
the session down” contract.

### F3 — `connect()` times out on CONNACK violations (#188)

When `_protocol_disconnect` rejects a CONNACK **before** emitting `CONNACK`
(Clean Start + Session Present, empty ClientID without Assigned Client
Identifier), `EffectPump` only forwards apply failures to `_connack_fut` while
`engine.state is CONNECTING`. The disconnect already moved state to
`DISCONNECTED`, so `connect()` never sees the `ProtocolError` and waits until
the CONNACK timeout (`MQTTTimeoutError`).

Related to #187 (same `_protocol_disconnect` + effect-pump interaction) but a
distinct user-visible failure mode on the connect path.

## Candidates examined and rejected

| Candidate | Verdict |
| --- | --- |
| Orphan PUBREL → success PUBCOMP (not `0x92`) | **Intentional** — `inbound.py` documents MQTT 5 interop |
| `$share/g/+` accepted | **Legal** shared-subscription wildcard filter |
| Invalid Will topic / Will `response_topic` | **Already fixed** in RC2 |
| Outbound `topic_alias=0`, subscription identifier on PUBLISH, shared + No Local, negotiation caps (`maximum_qos`, retain, wildcards, shared) | **Already refused** |
| Second CONNACK / malformed PINGRESP without tearing the session down | **Intentional** — `test_rc_regressions` non-fatal `PROTOCOL_ERROR` when engine stays `CONNECTED` |
| Empty `content_type` / `correlation_data` | **Admissible** zero-length UTF-8 / binary |
| `Properties(...)` accepting out-of-range RRI/RPI before encode | **Rejected at encode / `begin_connect`** |
| In-place `bytearray` mutation after `queue_publish` | **Already hardened** — wire kept the snapshot (`hello`, not `EVIL!`) |
| Huge remaining-length VBI | **Refused** — `PacketTooLargeError` against decoder max |
| Wildcard / empty topic / DUP on QoS 0 (MQTT 5 fast path and 3.1.1) | **PROTOCOL_ERROR** on both paths |
| Empty SUBSCRIBE / UNSUBSCRIBE | **Refused** |
| `ack()` after `notify_transport_closed` | **NotConnectedError** |
| PUBREL reserved flags ≠ `0x02` | **MalformedPacketError** |
| QoS-crossing inbound MID reuse | **DISCONNECT `0x82` + PROTOCOL_ERROR** (still subject to F1 size gap) |
| WebSocket coalesce bounds / masking | **No defect found** in read + contract comments (`_MAX_COALESCED_PAYLOAD`, frame/batch caps) |
| Compat import confinement from protocol | **Clean** |
| Unsolicited AUTH Success (`0x00`) while CONNECTED | **Inconclusive / likely intentional** — delivered as `AUTH` for the handler; not filed |

## Surfaces surveyed without a confirmed new defect

- Persistence: SQLite `complete_out` / orphan PUBACK after external completion (no crash; empty effect batch).
- Packet-id accounting across `notify_transport_closed` (publish MID retained for resume; sub MID released) — matches session-resume design.
- `DISCONNECTING` ignores further ingress (no effects).
- Server Keep Alive negotiation, Clean Start Session Present rejection at engine level (correct detection; runtime issues are F1–F3).
- Incremental decoder default max packet size enforcement.
- `SEGMENT_THRESHOLD` (128 KiB) present as documented for RC2.

## Phase-2 surface matrix

Status per planned sweep area (`clean` = no new defect beyond F1–F3;
`bug→#N` = filed; `intentional` = documented non-bug behaviour).

| # | Surface | Status |
| --- | --- | --- |
| 2.1 | `_protocol_disconnect` / MPS / size validation | **bug→#186**, **bug→#187**, **bug→#188** |
| 2.2 | Fast-path MQTT 5 decode vs 3.1.1 fallback | **clean** — wildcard / empty / DUP QoS 0 refused on both paths |
| 2.3 | Property encoding cache / mutation | **clean** — in-place `bytearray` snapshot held |
| 2.4 | Persistence transitions / SQLite | **clean** — external `complete_out` + orphan PUBACK stable |
| 2.5 | WebSocket coalesce | **clean** — bounds/masking contract held on read |
| 2.6 | Compat Paho confinement | **clean** — protocol layer does not import compat |
| 2.7 | Decoder / delivery budgets | **clean** — oversized VBI → `PacketTooLargeError`; delivery controller present |
| 2.8 | Reconnect / connection epoch | **clean** — epoch bump works; covered by `test_reconnect_stale_effects` / `test_reconnect_receipts` (no new counter-example) |
| 2.9 | CONFORMANCE statement gaps | **clean for sampled high-impact rules**; residual uncited statements remain a known coverage gap (see Limits) — no new executable counter-example filed |
| 2.10 | Hostile-broker liveness | **intentional / existing suite** — `test_hostile_broker.py` not extended; focused harnesses used instead; no new hang found beyond F2 |

Manual ACK + MPS was re-checked as part of 2.1 (already covered by RC2
`test_inbound_ack_packet_size` / `test_control_packet_size_lifecycle` for the
paths `#181` fixed; residual gap is only `_protocol_disconnect`).

## Limits of this audit

- Not a full walk of all ~290 client-reachable numbered statements in
  `docs/spec/`; high-impact statements and RC2-touched code were prioritised.
- No multi-hour soak, no external broker fuzz campaign, no TLS handshake matrix
  against mis-issued certificates (`ssl=` remains a passthrough to
  `asyncio.open_connection`).
- Hostile-broker liveness suite was not extended with new scripts in-tree;
  findings above used focused unit harnesses instead.
- Probe issues `#184` / `#185` were created earlier while checking write access
  and could not be closed with this token — they should be closed manually.

## Follow-up

Fix order suggested by coupling:

1. Teach `_protocol_disconnect` (engine + inbound) to honour Maximum Packet Size
   the same way intentional disconnect / AUTH fallback already do (#186).
2. Finalize `AsyncClient` whenever the engine emits local `DISCONNECTED` (or
   when `PROTOCOL_ERROR` accompanies an already-disconnected engine) (#187).
3. Fail `_connack_fut` on connect-path protocol disconnects even after state is
   `DISCONNECTED` (#188) — likely falls out of (2) if the error is preserved for
   the waiter.
