# Public API surface review before 1.0

Date: 2026-09-24. Source: `main` after 1.0.0rc16. Status: **proposals awaiting the
maintainer's decision** (§6 decided). No code changes follow from this report until each line
is decided.

Every Stable item becomes a SemVer obligation at 1.0. This review lists every
supported item. Where an item has no demonstrated use, duplicates another,
exposes internals, or promises behaviour the code does not have, it proposes
removing it, making it private, or reshaping it.

Evidence columns count occurrences in maintained guides (`README.md`, `docs/*.md`,
`docs/reference/*.md`; reports excluded) and in `examples/`. Supported names and
signatures are pinned by `tests/project/test_public_api_surface.py`, which each
accepted change updates together with `docs/api-stability.md`,
`docs/migration.md` and `CHANGELOG.md`.

Legend: **K** keep · **R** remove · **P** make private or Internal ·
**S** reshape.

## Decisions recorded (maintainer, 2026-09-24)

| Scope | Decision |
| --- | --- |
| §1 dead items D1–D6 | Remove or make Internal as proposed. |
| §2 duplicates U2–U5 | Remove as proposed. U7 kept. |
| §3 leaks L1–L6, L8 | Fix as proposed. |
| E2 `MQTTTimeoutError` | Also derive from `TimeoutError`. |
| §6 stores | Classes public (both), protocol methods Internal. |
| Pending | D7 (`will`), §5 statistics, U1, U6, L7, E5. |

## 1. Dead or misleading items

| # | Item | Tier | Evidence | Proposal | Reason | Migration |
| --- | --- | --- | --- | --- | --- | --- |
| D1 | `ConnectionState.RECONNECTING` | Stable | never assigned in `src/` | R | A state the client never reports invites wrong application logic. | None; the value is never observed. |
| D2 | `MQTTProtocolVersion.MQTTv31` | Stable | always refused at construction | R | Kept "for imports" only; a Stable member that can never be used. | Remove the reference. |
| D3 | `NegotiatedSettings.from_connack()` | Stable | used only by the engine | P | Internal factory; `SubscribeResult.from_packet` was already retired for the same reason. | None for applications. |
| D4 | `NegotiatedSettings.effective_keepalive` | Stable | alias of `server_keep_alive`; unit tests only | R | Duplicate field. | Use `server_keep_alive`. |
| D5 | `NegotiatedSettings.effective_client_id()` | Stable | never called in `src/` | R | Duplicates `AsyncClient.effective_client_id`. | Use the client property. |
| D6 | `SubscribeOptions.encode_byte()`, `ConnAckPacket.decode()`, `AuthPacket.encode()` / `.decode()` | Stable (via their classes) | codec only | P | Codec methods on Stable value types freeze the codec. | None for applications. |
| D7 | `will=Message(...)` constructor argument | Stable | 0 guide examples | S | `Message.properties`, `.dup` and `.mid` are silently ignored (`engine.py:307-320`), against the no-silent-degradation rule; `will_properties` duplicates `Message.properties`. Proposal: `will=PublishMessage(...)`, whose `properties` become the Will Properties; remove `will_properties`. | `will=PublishMessage(topic, payload, qos, retain, properties)`. |

## 2. Duplicates

| # | Item | Tier | Evidence | Proposal | Reason | Migration |
| --- | --- | --- | --- | --- | --- | --- |
| U1 | `AsyncClient.is_connected` | Stable | guides use `state` | K or R | Equal to `state is ConnectionState.CONNECTED`. Keeping it is cheap and common in clients; removing it shrinks the surface. **Maintainer's choice.** | `client.state is ConnectionState.CONNECTED`. |
| U2 | Client id: `AsyncClient.effective_client_id`, `negotiated.assigned_client_identifier`, `NegotiatedSettings.effective_client_id()` | Stable | – | K the property; see D5 | One way to get the effective id. | – |
| U3 | `PublishBatchError.failures`, `.failure_count`, `.failure_counts` | Stable | guides use `.receipt` only | R | Copies of `PublishBatchError.receipt` fields. | `exc.receipt.failures`, etc. |
| U4 | `PublishBatchError.cause` | Stable | 0 | R | Duplicates `__cause__` (always raised `from`). | `exc.__cause__`. |
| U5 | `PublishBatchReceipt.completed` | Stable | 0 | R | Equals `submitted - pending_count`. | Compute it. |
| U6 | `PublishBatchReceipt.failure_count` | Stable | 0 | K | Equals `sum(failure_counts)`, but it is the natural summary field; keep one of the two. **Maintainer's choice.** | – |
| U7 | `SubscribeResult` and `UnsubscribeResult` | Stable | identical shape (`mid`, `reason_codes`) | K | The names document intent; merging them buys little. Make both frozen (see L3). | – |

## 3. Leaks of internal state

| # | Item | Tier | Evidence | Proposal | Reason | Migration |
| --- | --- | --- | --- | --- | --- | --- |
| L1 | `PublishReceipt(mid, qos, _waiters, _error, _settled)`: public constructor with private fields; mutable `mid`/`qos`; `__eq__` compares waiter lists | Stable | applications never construct it | S | Freeze `mid` and `qos` as read-only, hide the constructor fields, use identity equality. | None unless an application constructed receipts. |
| L2 | `PublishBatchReceipt(max_failure_details=...)` public constructor | Stable | applications never construct it | S | Returned by `publish_many`; construction is internal. | None. |
| L3 | `SubscribeResult` / `UnsubscribeResult` mutable | Stable | – | S | Every other result model is frozen. | None. |
| L4 | `store=` annotated with the Internal `InflightStore` Protocol | Stable parameter | – | S | Annotate with the supported classes only (see §6). | None. |
| L5 | `ClientStats` nested types (`OutboundStats`, `InboundStats`, `WriterStats`, `DecoderStats`, `DeliveryStats`, `ReceiptStats`, `TransportStats`) importable only from Internal modules | Provisional | – | S | Export them from `mqttium.api` if kept (see §5). | None. |
| L6 | `__all__` in Internal packages (`packets` 19 names, `protocol` 11, `transport` 10, `codec` 7, `dispatch` 1, `api.models`, `api.stats`) | Internal | – | P | An `__all__` reads as a public list; drop it where the package is Internal. | None. |
| L7 | Public-named callback aliases `OnMessage`, `OnConnect`, `OnDisconnect`, `OnAuth` in `async_client.py` | none | 0 | K (document) or P | Useful for typing hooks; either export them from `mqttium.api` or prefix them. **Maintainer's choice.** | – |
| L8 | `MessageDelivery` (a `Literal` alias from a private module) | Stable | 3 | K | Harmless typing aid; move its definition to a public module. | – |

## 4. Constructor parameters (30)

Keep everything not listed here. Items to decide:

| # | Parameter | Evidence | Proposal | Reason |
| --- | --- | --- | --- | --- |
| C1 | `will_properties` | 5 guide mentions | R (with D7) | Folded into `will=PublishMessage`. |
| C2 | `ping_timeout` | 3 | K | Derived from `keepalive` by default; needed for slow links. |
| C3 | `subscribe_timeout` | 4 | K, clarify | Also governs UNSUBACK; name it in the docs, no rename. |
| C4 | `max_outbound_inflight` | 6 | K | Local cap below the broker's Receive Maximum; distinct from the pending budget. |
| C5 | `auth_timeout`, `auth_handler` | 6, 1 | K | Enhanced authentication is a supported MQTT 5 feature. |

## 5. Statistics (Provisional, 51 fields)

Statistics are Provisional, so they may still evolve after 1.0. Proposals
therefore aim only at fields that expose implementation details a later change
would have to keep emulating.

| # | Field | Proposal | Reason |
| --- | --- | --- | --- |
| T1 | `ClientStats.connection_epoch` | R | Internal counter; `reconnect_attempt` and `state` cover operations. |
| T2 | `WriterStats.last_outbound` | R | Keepalive timestamp, internal. |
| T3 | `DecoderStats` (all 3 fields) | R | Internal decoder buffer; `max_packet_size` repeats configuration. |
| T4 | `TransportStats.kind` | R | Internal class name. |
| T5 | `ReceiptStats.publish_batches`, `publish_waiters`; `WriterStats.waiters`; `OutboundStats.awaiting_slot`; `DeliveryStats.waiters` | K, rename consistently | Useful backpressure signals; use one word (`waiters`) for parked callers. |
| T6 | Limit field names (`inflight_limit`, `inflight_byte_limit`, `iterator_limit`, `iterator_byte_limit`, `max_messages`, `max_bytes`) | S | Use one naming scheme (`*_limit`). |

## 6. Stores — decided

Today `MemoryInflightStore` and `SqliteInflightStore` are Provisional classes.
Their 19 methods (`put_out`, `get_out`, `complete_out`, `in_replay_pages`,
`batch`, ...) are public by name, but they take and return Internal record types,
and the guides use only the constructors, `close()` and `with`.

**Decision (maintainer, 2026-09-24): both classes stay public; their protocol
methods become Internal.**

| # | Item | Status |
| --- | --- | --- |
| S1 | `MemoryInflightStore()` constructor | Public (Provisional). `store=None` keeps using it by default. |
| S2 | `SqliteInflightStore(path)` constructor, `close()`, context manager | Public (Provisional). These are the lifecycle operations an application needs. |
| S3 | The 19 store-protocol methods and `batch()` | Internal. They take and return Internal records. The engine owns their semantics, so later changes to the protocol and records (sealing, slot ownership, ACK settlement) stay internal. |
| S4 | `store=` annotation | `MemoryInflightStore | SqliteInflightStore | None` instead of the Internal protocol (see L4). |

No other method is proposed as public. The only plausible application need is
inspecting a database offline, for example to know whether session state is
pending before connecting. Nothing in the guides relies on it, and
`client.stats()` covers a running client. A dedicated read-only query can be
added later if a real use appears.

Follow-up: the persistence guide (`docs/sessions-and-persistence.md`, which
calls `batch()` Internal while it is a public method), `docs/api-stability.md`
and the reference pages must then say exactly this.

## 7. Enumerations and errors

| # | Item | Proposal | Reason |
| --- | --- | --- | --- |
| E1 | `QoS` not re-exported from `mqttium.api`; guides pass integers | K | Integers remain accepted; `QoS` stays at the root. |
| E2 | `MQTTTimeoutError` is not a `TimeoutError` | S | Derive from both `MQTTError` and `TimeoutError` so `except TimeoutError` works. Additive. |
| E3 | `MandatoryResponseTooLargeError` under `PacketTooLargeError` | K | Distinct remedy (the broker's limit prevents a mandatory response). |
| E4 | `MalformedPacketError` vs `ProtocolError` | K | Distinguishes malformed wire data from protocol violations. |
| E5 | `ProtocolError` also raised for local misuse (bad `Properties` types, MQTT 5 options on 3.1.1, stale `ack()` handle) | S | Raise `ValueError`/`TypeError` for local argument errors, keep `ProtocolError` for peer or protocol state. Changes the type users catch; requires a migration note. **Maintainer's choice.** |

## 8. Out of scope

Documentation redirect stubs (`docs/API-STABILITY.md` and similar) and retired
entry-point tests are repository hygiene, not API. They are handled separately.
