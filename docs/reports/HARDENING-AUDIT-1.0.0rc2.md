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
| F4 | [#190](https://github.com/yoch/mqttium/issues/190) | CONNACK auth-property validation leaves `ProtocolEngine` stuck in `CONNECTING` |
| F5 | [#191](https://github.com/yoch/mqttium/issues/191) | Client DISCONNECT accepts Server-only reason codes and Server Reference |
| F6 | [#192](https://github.com/yoch/mqttium/issues/192) | Request Problem Information 0 is not enforced on inbound packets |
| F7 | [#193](https://github.com/yoch/mqttium/issues/193) | ReconnectPolicy retries CONNACK Banned (`0x8A`) |

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

### F4 — CONNACK auth validation leaves engine `CONNECTING` (#190)

When `_on_connack` rejects a successful-looking CONNACK for auth-property
rules (`authentication_method` mismatch, or `authentication_data` without a
CONNECT method), it raises `ProtocolError` **without** leaving `CONNECTING`.
`_pending_connect` is already cleared, so `begin_connect()` then fails with
“Already connected or connecting”. Recovery needs an explicit
`notify_transport_closed()`.

Unsuccessful CONNACK reason codes correctly emit `DISCONNECTED`. Empty ClientID
without ACI and Clean Start + Session Present use `_protocol_disconnect`. These
auth-property checks should follow the same teardown pattern.

`AsyncClient.connect()` still fails promptly (state is still `CONNECTING`, so
EffectPump can fail `_connack_fut`). The defect is the engine state machine /
direct `ProtocolEngine` consumers. Distinct from #188.

### F5 — Client DISCONNECT accepts Server-only reasons / Server Reference (#191)

`encode_disconnect` / `begin_disconnect` share one `_DISCONNECT_V5_REASONS`
allowlist for inbound and outbound. Server-only Table 3-28 codes (`0x89`,
`0x8B`, `0x8D`, `0x8E`, `0x9C`, `0x9D`, `0x9F`, `0xA0`) are therefore
Client-encodable — e.g. `begin_disconnect(0x8E)` yields `e0028e00`.
`server_reference` is likewise attachable on a Client DISCONNECT because the
property table is packet-typed, not direction-typed (same structural gap as
outbound `subscription_identifier` / Client AUTH Success before those fixes).

## Round 4 expert sweep

Confirmed **#192** (RPI=0) and **#193** (Banned reconnect). Additional probes without new filings:

- `$` / `#` TopicMatcher rules (`#` does not match `$SYS/…`)
- SUBSCRIBE duplicate `subscription_identifier` refused
- Callback re-entrancy into `publish()` succeeds (invariant 6)
- Packet-ID pool exhausts 1..65535 cleanly
- Direct `EngineConfig` field assignment bypasses `update()` allowlist — API footgun, not filed (use `config.update()`)
- Session taken over (`0x8E`) remains retryable — treated as intentional vs Banned

## Round 3 expert sweep (additional)

Surfaces probed beyond F1–F5 without a new confirmed bug (sample):

- QoS 2 redelivery before PUBREL (PUBREC only, no second MESSAGE)
- Manual-ack PUBREL→WAIT_USER_ACK→ack ordering
- SQLite `logical_size` recompute on hydration; CAS `transition_out`; newer schema refuse
- Session resume PUBREL replay; `session_present=0` purge
- Fragmented / coalesced decoder feeds; UTF-8 overlong / surrogates rejected
- Alias remap; alias not stored after wildcard Topic Name rejection
- Auto-ack SEND-before-MESSAGE order; flow queue under `receive_maximum`
- Publish-while-disconnecting refused; receipt cancel isolation; masked WS server frames API
- Compat confinement / ingress-bound tests present

### Round 3 rejected / inconclusive

| Candidate | Verdict |
| --- | --- |
| Publish Topic Name `$share/...` | **Inconclusive** — MQTT SHOULD refuse `$` PUBLISH Topic Names; not a MUST |
| Inbound DISCONNECT `0x04` (Client-only) from broker | **Low severity / broker misbehavior** — noted under #191 outbound focus |
| Invalid CONNACK reason leaving `pending_connect=True` | **Recoverable** — a later valid CONNACK still connects; distinct from #190 |
| Keepalive + MPS=1 without keepalive task started | **Inconclusive harness** — `queue_ping` raises `PacketTooLargeError`; production `_keepalive_loop` already force-closes |

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
| Outbound publish with `receive_maximum=1` admitting a second QoS1 | **Intentional** — second message enters `QUEUED`, no extra SEND until flow frees |
| MQTT 5 SUBACK with reason byte but no property length | **False positive** — malformed framing; correct `mid + 0x00 + reason` works and releases the MID |
| CONNACK `Assigned Client Identifier` while local ClientID non-empty | **Inconclusive** — numbered statements only MUST ACI for zero-length ClientID; no clear MUST NOT filed |
| PUBACK / PUBREC failure reason codes (`0x80`/`0x87`/`0x97`) | **Clean** — `PUBLISH_FAILED`, store cleared, MID released |
| `0x10` No matching subscribers on PUBACK | **Clean** — treated as `PUBLISH_COMPLETE` |

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
| 2.1 | `_protocol_disconnect` / MPS / CONNACK / DISCONNECT direction | **bug→#186–#188**, **bug→#190–#193** |
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

Fix order suggested by coupling (not implemented in this audit pass):

1. Teach `_protocol_disconnect` (engine + inbound) to honour Maximum Packet Size
   the same way intentional disconnect / AUTH fallback already do (#186).
2. Finalize `AsyncClient` whenever the engine emits local `DISCONNECTED` (or
   when `PROTOCOL_ERROR` accompanies an already-disconnected engine) (#187).
3. Fail `_connack_fut` on connect-path protocol disconnects even after state is
   `DISCONNECTED` (#188) — likely falls out of (2) if the error is preserved for
   the waiter.
4. On CONNACK auth-property validation failures, leave `CONNECTING` via
   `_protocol_disconnect` / `DISCONNECTED` rather than a bare `PROTOCOL_ERROR`
   (#190).
5. Split Client vs Server DISCONNECT reason allowlists and reject Client
   `server_reference` on outbound DISCONNECT (#191).

### Suggested fix approaches (for issue assignees)

Issue comments could not be posted with the audit token (403). The approaches
below are also mirrored on PR #189.

**#186** — Before `_send` in `_protocol_disconnect`, `encode_disconnect` +
`_check_outbound_size`. On `PacketTooLargeError`, omit the SEND but still
transition to `DISCONNECTED`. Optionally align `_reject_auth_method`.

**#187** — Close the transport on any `DISCONNECTED` while it is still open.
Optionally set `_disconnect_exc` from `PROTOCOL_ERROR` only when
`engine.state is DISCONNECTED`, preserving the intentional non-fatal rude
CONNACK policy when the engine stays `CONNECTED`.

**#188** — In `EffectPump`, if `_connack_fut` is pending and not done,
`set_exception(exc)` without requiring `state is CONNECTING`.

**#190** — In `_on_connack`, on auth-method / auth-data validation failure, use
the same teardown pattern as empty ClientID without ACI (`_protocol_disconnect`
then raise), instead of raising alone while left in `CONNECTING`.
**#191** — Split outbound Client vs inbound DISCONNECT reason allowlists; reject
`server_reference` on Client-originated DISCONNECT (directional check outside
the packet-type property table).

**#192** — Snapshot effective Request Problem Information at `begin_connect`
(absent ⇒ 1). When RPI is 0, reject inbound Reason String / User Properties on
packets other than PUBLISH/CONNACK/DISCONNECT with Protocol Error.

**#193** — Add `0x8A` to `_V5_TERMINAL`; audit other permanent CONNACK refusals.
Keep `0x88`/`0x89` retryable.
