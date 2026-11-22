# First stable release audit — 2026-09-22

## Decision and audited baseline

**Recommendation: do not release 1.0 from this candidate.** Two independently
reproduced P1 defects break request/result correlation and terminal packet
ordering. A failed SQLite commit can subsequently become durable during normal
cleanup; this is an additional conservative release blocker. The candidate also
fails the existing strict open-loop performance gate. Passing ordinary CI does
not cover the new deterministic scenarios in this report.

The overall architecture and intended public API are coherent. This audit does
not recommend a rewrite, a generic command framework, or a larger public API.
The useful corrections concern exchange ownership, terminal transitions,
transaction failure isolation, consistent limits, and a small number of public
contract details that should be settled before the first stable release.

| Baseline | Value |
| --- | --- |
| Repository | `yoch/mqttium` |
| Audited main | [`6f057bc23179f2dc651521d98751e082c4ca389d`](https://github.com/yoch/mqttium/commit/6f057bc23179f2dc651521d98751e082c4ca389d) |
| Local/remote agreement | Local HEAD and GitHub `main` independently checked; both equal the audited commit |
| Last remote status observation | 2026-09-22 11:51 UTC |
| Source isolation | Source archive of that exact commit; executable probes imported this archive |
| Local validation interpreter | Python 3.12.13, existing project development environment; initial deadline/NaN probes also ran on system Python 3.12.3 |
| Scope of changes | This new historical report only; no production, test, configuration, or maintained-documentation changes |

This is a fresh, consolidated audit of the updated main, not a claim that an
earlier local-main audit also covered subsequent changes. Earlier reports are
historical evidence and remain unchanged. Existing unrelated untracked files
were left alone. No commit, pull request, release, or publication was created.

P1 means urgent correctness work that should block release. P2 means a concrete
correctness or contract problem to address in the release preparation; whether
it independently blocks publication depends on its impact. P3 means a narrower
typing or observability defect. Severity and release acceptance are separate:
F03 is P2 for what was demonstrated, but the recommendation is to block the
first stable release until its transaction semantics are repaired.

## Findings at a glance

| ID | Priority | Finding | Evidence and disposition |
| --- | --- | --- | --- |
| F01 | P1 | A delayed SUBACK/UNSUBACK completes a later request after packet identifier reuse | Reproduced for both operations in MQTT 3.1.1 and 5; release blocker |
| F02 | P1 | Public publication remains admissible while a fatal DISCONNECT drains | Actual wire order `DISCONNECT, PUBLISH`; release blocker |
| F03 | P2 | A failed SQLite COMMIT can be committed later by `close()` | Native SQLite fault, direct and batch mutations; conservative release blocker |
| F04 | P2 | Cancelling `disconnect()` can abandon connection cleanup | Open transport and reader/writer/keepalive tasks remain |
| F05 | P2 | Cancelling a subscription during effect transfer leaves an unowned future | Later teardown reports `Future exception was never retrieved` |
| F06 | P2 | Connection stages reuse the timeout, while WebSocket imposes a hidden 30-second cap | Public 0.20-second deadline succeeds after 0.302 seconds; public 60-second WebSocket timeout fails at 30.03 seconds |
| F07 | P2 | Known EOF or malformed CONNACK is reported only after the connect timeout | Original terminal cause is retained internally but not delivered to caller |
| F08 | P2 | Non-finite timing values pass validation and can disable dead-peer detection | NaN PINGRESP deadline leaves an unresponsive connection connected |
| F09 | P2 | WebSocket receive ceiling conflicts with the advertised MQTT packet limit | 32 MiB MQTT limit, fixed 16 MiB WebSocket limit, rejection at 20 MiB |
| F10 | P2 | SQLite replay byte limits omit MQTT 5 properties | 2,160,345 logical bytes in a 1,048,576-byte batch; memory store behaves differently |
| F11 | P3 | Batch failure receipt is publicly typed as `object` | Documented recovery code fails consumer type checking |
| A01 | P2 | Generated public reference exposes factories requiring Internal packet types | Actual generated documentation inspected; settle before API freeze |
| Q01 | P2 | Local release runner uses an obsolete quality gate | Both `quick` and `rc` omit project tests and use 87.36% instead of 89% |
| D01 | P3 | Pull transport backlog metric reports zero for buffered input | 100 queued bytes reported as zero; Provisional observability debt |

The strict performance failure and unfinished long campaigns are recorded
separately under release evidence. They are not counted as additional source
defects.

## Confirmed correctness findings

### F01 — P1: acknowledgement can settle the wrong subscription operation

**Source:** [subscription future registration](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L1359-L1369),
[unsubscribe registration and waiting](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L1392-L1416),
[engine acknowledgement handling](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/protocol/engine.py#L722-L769).

Decoding SUBACK/UNSUBACK releases the protocol packet identifier before the
corresponding API completion effect necessarily runs. Writer pressure can defer
that effect: an automatic PUBACK from the same ingress collection must be
transferred first. A second subscription operation can then allocate the freed
identifier and replace the first future in `_sub_futs` or `_unsub_futs`. The old
completion effect looks up only the identifier and resolves the new future.

The deterministic reproduction uses a one-item writer queue and a transport
whose write can be paused. After the first request is on the wire, a QoS 0
publication fills the writer. The peer sends the first acknowledgement together
with a QoS 1 incoming PUBLISH. While the automatic PUBACK blocks effect transfer,
a second request is admitted; it reuses identifier 1. Releasing the writer yields:

```text
MQTTv311 SUBSCRIBE   MIDs [1, 1]: MQTTTimeoutError, SubscribeResult
MQTTv311 UNSUBSCRIBE MIDs [1, 1]: MQTTTimeoutError, UnsubscribeResult
MQTTv5   SUBSCRIBE   MIDs [1, 1]: MQTTTimeoutError, SubscribeResult
MQTTv5   UNSUBSCRIBE MIDs [1, 1]: MQTTTimeoutError, UnsubscribeResult
```

The second acknowledgement is never emitted by the test peer. The first caller
loses its result, and the second sees success before the broker has accepted its
operation. No identifier or engine-state mutation is injected by the probe;
internal counters are read only for synchronization. This was independently
reproduced by the coordinating reviewer.

**Correction and acceptance:** retain the originating exchange identity through
API completion, or establish a consistent settlement-before-reuse boundary that
also holds under the engine lock. Merely keying futures by a reusable packet
identifier is insufficient. Cleanup must check the identity of the future being
removed. Cover both operations, both protocol versions, deferred effects,
immediate acknowledgements, and cancellation. Do not release a still-live
protocol identifier simply because its caller was cancelled. Appendix B1 is the
complete reproduction.

### F02 — P1: a PUBLISH can follow a fatal DISCONNECT on the same connection

**Source:** [reader error path](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L1608-L1642),
[fatal disconnect](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L2306-L2326),
[terminal writer drain](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L901-L917).

For an ingress validation failure that has not already retired the engine,
`_send_fatal_disconnect()` awaits writer drainage while the client remains
CONNECTED. A concurrent public `publish()` is admitted behind the terminal
packet. The reader's final cleanup happens later.

The peer sends an invalid MQTT 5 DISCONNECT fixed header (`e1 00`). The test
transport pauses the client's fatal DISCONNECT write. At that point the client
reports CONNECTED and accepts a normal public publication. Releasing the writer
produces the actual decoded sequence:

```text
CONNECT -> DISCONNECT -> PUBLISH
```

This violates MQTT 5 statement `MQTT-3.14.4-1`, preserved in the repository's
[specification statements](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/docs/spec/mqtt-v5.0-statements.json):
after sending DISCONNECT, its sender must send no further MQTT Control Packets.
The scenario was independently executed by two additional reviewers. It does not
depend on manually changing client state.

**Correction and acceptance:** close admission synchronously before the first
await of terminal shutdown. Preserve the original error and the single-writer
ordering while draining the terminal packet with a bound. Exercise publish,
subscribe, unsubscribe, authentication, and already queued SEND effects during
the terminal window; nothing may follow DISCONNECT on that connection. Appendix
B2 contains the reproduction.

### F03 — P2: failed SQLite mutations may become durable during close

**Source:** [batch completion, commit and close](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/persistence/sqlite.py#L493-L523).

A failure from `Connection.commit()` is not followed by rollback, either on
standalone mutation or on successful-body batch exit. The batch transaction
flags are nevertheless reset. `close()` unconditionally commits again before
closing the connection. A mutation reported to its caller as failed can therefore
become durable during ordinary cleanup. A persistent commit error also prevents
the close call from reaching connection closure.

The reproduction seeds a durable outbound record, then uses SQLite's native
authorizer to deny COMMIT. `complete_out()` is exercised directly and inside
`batch()`; no store or connection method is replaced. Both raise
`sqlite3.DatabaseError: not authorized`. Afterwards:

```text
SQLite in_transaction = True
Store _transaction_started = False
Independent connection: 1 outbound row
Remove COMMIT fault, call store.close()
Independent connection: 0 outbound rows
```

The engine's fail-stop latch does not roll back the application-owned store.
The maintained persistence example ends with `disconnect()` followed by
`store.close()`, so the late commit is reachable through the recommended cleanup
pattern. Existing `_BatchExitFailingStore` coverage raises before the real COMMIT
and takes the already-working body-error rollback branch; it misses this window.

The injected failure demonstrates transaction isolation and cleanup behavior,
not a physical disk failure or a particular lost MQTT message. SQLite explicitly
documents that failed COMMIT can leave a transaction active, and several resource
or I/O errors do not guarantee rollback of the whole transaction.
[SQLite transaction documentation](https://www.sqlite.org/lang_transaction.html).

**Correction and acceptance:** rollback an unsuccessful commit where possible,
preserve the primary exception, and prevent normal close from committing a
transaction whose operation already failed. Verify direct and batch mutation,
independent observer-visible state, and cleanup when the error persists. This is
a conservative first-release blocker despite P2 severity: the durable mutation
contract should be dependable before users entrust sessions to the store.
Appendix B3 contains the reproduction, independently repeated by the coordinator.

### F04 — P2: disconnect cancellation abandons cleanup

**Source:** [disconnect transition and flush](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L980-L1001).

After `begin_disconnect()` changes state to DISCONNECTING, the public method
awaits terminal writer drainage. `_force_close()` follows that await without a
cancellation-safe ownership boundary. Cancelling the calling task during the
drain propagates cancellation and skips the remaining teardown.

With the terminal write paused, the reproduction cancels and awaits the public
disconnect task. The transport remains open; state stays DISCONNECTING; reader,
writer, and keepalive tasks remain active. With `keepalive=0` and no further peer
event, no remaining owner finishes the abandoned shutdown. A second explicit
disconnect is used to clean up the probe. This is a shutdown cancellation gap,
not a claim that completed disconnect calls leak resources.

**Correction and acceptance:** once the terminal transition commits, retain an
owner for bounded resource retirement even if the initiating caller is cancelled;
then propagate cancellation. Avoid unowned shield tasks. Cover cancellation
before transition, during drain, and during retirement. The `cancelled()` case in
Appendix B2 reproduces the issue.

### F05 — P2: cancellation during request transfer leaves an unowned future

**Source:** [request acknowledgement helper](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L1400-L1416).

The effect pump is drained before entering the block that owns waiting for the
request future. Cancellation during that first await leaves the future registered
without a consumer. Disconnect later assigns it an exception, producing
`Future exception was never retrieved` even though the application properly
awaited and handled the cancelled subscription task.

The reproduction pauses a full writer queue, starts `subscribe()`, waits for
blocked effect transfer, cancels the call, and disconnects. It observes one
retained subscription future and the unhandled-future diagnostic. The API
reviewer independently repeated it. Unsubscribe shares the affected helper; that
extension is established by inspection, not an additional executed cancellation
matrix. No protocol corruption is inferred from this diagnostic alone.

**Correction and acceptance:** cover both effect transfer and acknowledgement
waiting with the same operation-owned cleanup. Remove/cancel only that operation's
future, with identity checking to avoid aggravating F01. Keep committed protocol
state until ACK or teardown; cancellation cannot withdraw a request already on
the wire. Test both request types and both cancellation windows. See Appendix B4.

### F06 — P2: connection deadline ownership is inconsistent

**Source:** [transport factory](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L701-L705),
[transport wait and CONNACK wait](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L813-L851),
[WebSocket timeout defaults](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/transport/websocket.py#L67-L81).

The public contract describes one deadline covering transport setup and CONNACK.
Implementation gives transport setup the full timeout and then gives CONNACK a
new full timeout. A transport taking 0.15 seconds followed by CONNACK after another
0.15 seconds successfully connects in approximately 0.302 seconds with
`timeout=0.20`. A successful attempt can approach twice the stated budget, before
additional cleanup time.

WebSocket also has an independent inner default of 30 seconds, which the public
factory does not override. An actual elapsed-time probe with public
`connect_timeout=60` and `connect_ws(timeout=60)` fails during an unanswered HTTP
upgrade after 30.03 seconds. The stream is closed correctly. The probe uses a real
event loop and StreamReader; only network opening is replaced with an in-memory
stream. These are two symptoms of one deadline ownership problem, not two
independent findings.

**Correction and acceptance:** compute one absolute attempt deadline, pass the
remaining budget through every stage, and define bounded cleanup separately.
Apply it to TCP, Unix, WebSocket, defaults, overrides, and reconnect attempts.
Test slow setup plus slow CONNACK, and a public WebSocket budget above 30 seconds
without making the regression suite sleep for 30 seconds. Appendices B5 and B6
contain the executed reproductions.

### F07 — P2: terminal connection failures are converted into delayed timeouts

**Source:** [CONNACK wait](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L884-L899),
[reader terminal cleanup](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L1608-L1706).

EOF before CONNACK and certain inline malformed-packet failures close the
connection without settling `_connack_fut`. The caller continues waiting until
its configured timeout, then receives `MQTTTimeoutError('CONNACK timed out')`
instead of the already-known terminal cause.

With a 0.1-second timeout, both immediate EOF and malformed CONNACK flags
(`21 03 00 00 00`) produce that timeout after approximately 0.101 seconds. The
retained teardown cause is respectively `MQTTError: Connection closed` and
`MalformedPacketError: CONNACK flags 0x1 != 0x0`. Under the default budget this
needlessly waits 30 seconds and obscures diagnostics.

**Correction and acceptance:** settle a pending connection attempt promptly with
the first authoritative terminal cause. Preserve deliberate-disconnect,
cancellation, and genuine silence/timeout semantics. Cover EOF, malformed
CONNACK, transport exceptions, cancellation, and no response separately. Appendix
B7 contains the reproduction, independently repeated by the coordinator.

### F08 — P2: NaN timing values can silently disable liveness

**Source:** [client validation](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L144-L171),
[keepalive deadline checks](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L1828-L1860),
[reconnect validation](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/protocol/reconnect.py#L58-L68).

Checks based on `<= 0` and ordering do not reject NaN. Timing configuration and
reconnect progression can therefore accept values that invalidate their own
comparisons. In particular, `now >= ping_deadline` is always false when the
deadline is NaN.

Two packet-aware peers withhold PINGRESP. With keepalive 1, after 1.7 seconds both
have received PINGREQ: the client using `ping_timeout=0.05` is disconnected, while
the client using NaN remains connected. A NaN reconnect initial delay also
propagates to `next_delay()`.

**Correction and acceptance:** require finite values for timing/backoff fields
whose contract is finite positive or nonnegative numbers. Preserve documented
`None` and zero semantics; validate per-call overrides before mutation. Cover NaN
and both infinities plus the documented sentinels. The confirmed behavioral case
is NaN; this report does not infer cross-version behavior of `asyncio.sleep(NaN)`.
Invalid application configuration is required, so this is not a P1 on its own.
Appendix B8 contains the liveness reproduction.

### F09 — P2: WebSocket advertises more receive capability than it supports

**Source:** [public WebSocket factory](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/async_client.py#L701-L705),
[frame limit default](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/transport/websocket.py#L67-L69),
[fragment/message guard](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/transport/websocket.py#L254-L255),
[frame guard](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/transport/websocket.py#L471-L472).

The public client accepts and advertises `maximum_packet_size=32*1024*1024` in
MQTT 5, but does not pass a corresponding limit to its WebSocket transport.
The internal 16 MiB default remains active, with no supported public way to
raise it. Fragmentation does not bypass the same reassembled-message bound.
The inspected maintained contract does not disclose a WebSocket-specific cap.

A correct in-memory HTTP upgrade and MQTT 5 CONNACK establishes the connection.
Feeding only the valid binary frame header declaring 20 MiB immediately produces
`ConnectionError: WebSocket frame 20971520 exceeds max 16777216`. No 20 MiB
allocation or malformed MQTT payload is needed: the rejection happens before
MQTT decoding despite the larger advertised packet capability.

**Correction and acceptance:** align transport receive capability with the
configured MQTT limit, and explicitly define the separate policy for a WebSocket
message containing multiple MQTT packets. Alternatively reject unsupported
combinations before connection and document the restriction. Do not add a public
knob merely to expose an accidental internal default. Cover frame and fragmented
message boundaries through the public factory. See Appendix B9.

### F10 — P2: SQLite replay excludes properties from the byte bound

**Source:** [SQLite replay size expression](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/persistence/sqlite.py#L63-L65),
[page construction](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/persistence/sqlite.py#L776-L793),
[engine replay emission accounting](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/protocol/inbound.py#L735-L741).

SQLite's replay page size counts topic and payload, but hydrates complete MQTT 5
properties as well. The memory store instead uses the persisted logical size,
which includes those properties. The engine's extra emission guard repeats the
topic-plus-payload omission.

Three legal persisted QoS 2 records each contain a one-byte topic, one-byte
payload, and twelve user properties with 60,000-byte values. Each publication has
a validated logical size of 720,115 bytes. With the same 1,048,576-byte bound:

| Store | Page record counts | Logical bytes per first page | First engine replay emission |
| --- | --- | ---: | --- |
| Memory | `1, 1, 1` | 720,115 | One message |
| SQLite | `3` | 2,160,345 | Three messages |

The API reviewer independently repeated both page and engine results. This
violates the intended pre-hydration batch bound and creates store-dependent
behavior. Other bounds, including the 64-record page and configured session
limits, still exist; this is not a claim of unlimited memory consumption.

**Correction and acceptance:** use persisted, validated `logical_size` in both
SQLite page selection and engine emission accounting. Preserve the intentional
single-oversized-record rule needed for progress. Test store parity with large
legal properties and tiny payloads; current tests focused on topic/payload size
do not exercise it. Appendix B10 contains the reproduction.

### F11 — P3: the batch recovery receipt loses its public static type

**Source:** [PublishBatchError](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/errors.py#L77-L83).

The documented failure recovery path exposes the committed prefix through
`PublishBatchError.receipt`, but that attribute is inferred as `object | None`.
A normal consumer importing only public API cannot use `receipt.submitted` or
`await receipt.wait()` under mypy without a cast. Both accesses report
`attr-defined` errors in mypy 2.3. Package-internal type checking passes because
it does not exercise this consumer contract.

**Correction and acceptance:** annotate the attribute and constructor argument
as `PublishBatchReceipt | None`, using a type-checking import/postponed annotation
to avoid a runtime import cycle. Type-check a small public consumer of successful
batch submission and failed-prefix recovery. This is a typing defect, not a
runtime release blocker on its own. See Appendix B11.

## Public API correctness and minimality

### A01 — P2: documented model factories cross the Internal boundary

**Source:** [result factories](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/api/models.py#L271-L291),
[reference generation](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/docs/reference/models.md#L40-L46),
[support tiers](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/docs/api-stability.md#L10-L37).

The strict documentation build succeeds, but the generated canonical public
reference includes both of these entries:

```text
SubscribeResult.from_packet(packet: SubAckPacket) -> SubscribeResult
UnsubscribeResult.from_packet(packet: UnsubAckPacket) -> UnsubscribeResult
```

The result models are Stable; these packet arguments are Internal. This is a
concrete documentation boundary leak, not an assertion that every importable
method or every `__all__` entry is supported.

**Correction before 1.0:** keep packet conversion private or in the adapter,
list supported members of Stable model classes explicitly, and restrict generated
reference members accordingly. Promoting Internal packet types would unnecessarily
expand the compatibility burden. Check the actual generated reference and public
member annotations, not only top-level exports and client parameter names.

### Surface assessment and decisions to settle

| Area | Assessment | Recommended release decision |
| --- | --- | --- |
| Native client operations, errors and enums | Coherent scope for an async MQTT client; removed Paho and one-shot surfaces do not need replacement wrappers | Preserve the native surface and the documented pre-v1 break |
| Receipts and batch completion | Useful explicit completion contract; F01/F05 concern ownership and F11 typing | Repair these contracts without adding parallel callback APIs |
| Protocol, writer, retained-publication and iterator budgets | Different lifetimes justify separate limits | Preserve independent budgets; avoid merging them into an ambiguous global limit |
| Large client constructor | Argument count alone is not unnecessary complexity; configuration is validated centrally | No new configuration-object hierarchy justified by this audit |
| Memory/SQLite stores and immutable stats | Correctly designated Provisional | Keep supported and tested without silently promoting them to Stable |
| Engine, codecs, extension protocols and records | Correctly designated Internal | Do not expand compatibility promises to simplify internal annotations |
| Result conversion factories | Actual generated documentation leaks Internal packet parameters | Resolve A01 before freezing model members |
| Receipt construction | Dataclass signature includes `_waiters`, `_error`, `_settled`; these are not displayed in current reference | Make fields non-init/internal or explicitly state supported construction/member rules |
| Packet/settings helper methods on Stable classes | `ConnAckPacket.decode`, `AuthPacket.encode/decode`, `SubscribeOptions.encode_byte`, `NegotiatedSettings.from_connack` exist but are not rendered by current documentation | Explicitly classify supported members; do not infer stability from importability |
| Missing ACK properties in subscription results; outgoing disconnect/unsubscribe properties | Intentional minimality rather than a demonstrated correctness defect | Optional future additions can be additive; do not freeze more packet plumbing now |

The root API, immutable properties, callback route ownership, topic dispatch,
receipt waiter isolation and nested immutable snapshots were inspected. No
additional confirmed algorithmic defect was found in those assigned modules.
This does not certify every possible interaction.

## Code quality and technical debt

The source does not warrant a broad "slop cleanup". Separation of synchronous
protocol state from asynchronous runtime, directional session ownership, the
single writer, and explicit bounded delivery have real purposes and regression
coverage. Specialized hot paths and measured SQLite layout decisions should not
be replaced by abstraction for its own sake.

The highest-cost debt is inconsistent ownership at boundaries, demonstrated by
the findings rather than inferred from file size:

| Debt | Concrete evidence | Bounded direction |
| --- | --- | --- |
| Exchange settlement differs across operations | Publish settles preceding effects before reuse; subscriptions expose F01/F05 | Unify operation identity and cleanup rules at the existing owner boundaries |
| Terminal state and future settlement are spread over several paths | Fatal drain, reader failure, public disconnect and force-close expose F02/F04/F07 | Establish a small terminal-transition contract before awaits; preserve first-cause error handling |
| Transaction flags can disagree with SQLite | F03 leaves SQLite in a transaction after store flags reset | Give commit failure and close explicit transaction ownership |
| Limit calculations diverge across implementations | F06/F09/F10 | Use the intended shared deadline and logical-size definitions, without a new framework |

### D01 — P3: zero backlog is reported when the value is not zero

[Stream transport statistics](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/src/mqttium/transport/_stream.py#L116-L122)
hardcode `buffered_read_bytes=0`. A real StreamReader containing 100 bytes still
reports zero. This affects pull-based TLS, Unix and fallback stream paths.
WebSocket counts its own buffers but omits pending StreamReader input; the push
transport has separate accounting.

The field is in the supported Provisional immutable statistics contract and is
useful for backpressure diagnosis. Define consistent measured/unavailable
semantics rather than silently treating unknown as zero. Do not fix the API by
adding brittle reliance on unpromised StreamReader internals without considering
ownership. Appendix B9 also contains this observation. This does not independently
block release.

### Lower-priority cleanup and coverage debt

- Comments in `protocol/outbound.py:106–114` and
  `protocol/inbound.py:129–130,492–495` still describe removed `update()` and
  `_RUNTIME_MUTABLE_ENGINE_CONFIG_FIELDS` mechanisms. They teach an obsolete
  configuration model. A per-file-ignore comment in `pyproject.toml` also retains
  historical Paho terminology. Remove obsolete explanations when the relevant
  code is next touched; they do not justify structural refactoring.
- The private `_prepare_publish_request()` contains QoS 0 branches while both
  production call sites route QoS 0 through `prepare_qos0()` first. This is a
  bounded simplification opportunity, subject to checking internal test and
  benchmark use and preserving measured hot-path behavior.
- `NegotiatedSettings.from_connack()` retains unused `requested_keepalive` and
  `local_client_id` arguments; `effective_keepalive` aliases `server_keep_alive`.
  Clarify the member boundary before internal cleanup; no new public setting is
  needed.
- Runtime fuzz V1 fixes MQTT 5, callback delivery, memory store and keepalive 0,
  with QoS 0/1 publication grammar; V2/V3 reuse the harness. Their campaign length
  does not independently qualify QoS 2, SQLite, iterator/manual ACK, keepalive,
  AUTH, subscriptions, or real transports in combination. Deterministic tests
  cover many of these individually. Add the newly demonstrated operation and
  terminal interleavings before increasing campaign duration indiscriminately.

## Release tooling and qualification evidence

### Q01 — P2: local release quality gate has drifted from the maintained contract

**Source:** [local quality command](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/benchmarks/local_release.py#L413-L431),
[mode dispatch](https://github.com/yoch/mqttium/blob/6f057bc23179f2dc651521d98751e082c4ca389d/benchmarks/local_release.py#L766-L767).

Both the `quick` and `rc` local release modes call a quality command that runs
only `tests/unit` and explicitly lowers coverage to `--cov-fail-under=87.36`.
The maintained gate is unit plus project tests with branch-inclusive coverage
of at least 89%. The local runner also omits strict documentation building.
Consequently a successful local release run is weaker evidence than its current
release-preparation role suggests.

Current CI applies the proper 89% gate and project tests, so this finding is not
evidence that the audited main failed those checks. Synchronize the local runner
with the required release commands and add a small drift check around gate
construction. Do not lower the declared threshold to match the stale runner.

### Existing remote evidence inspected

Heavy campaigns were not launched locally. Existing remote runs, job steps,
logs and available artifacts were inspected in read-only mode. The table states
the actual evidence level rather than treating every green workflow as a complete
source/artifact qualification.

| Run | Candidate and inspected result | Qualification meaning |
| --- | --- | --- |
| [Main CI 35718219778](https://github.com/yoch/mqttium/actions/runs/35718219778) | Exact audited SHA; successful jobs and logs inspected | Quality, cross-platform tests, targeted fuzz/resilience, docs, workflow checks and packaging pass |
| [ARM64 CI 35718219854](https://github.com/yoch/mqttium/actions/runs/35718219854) | Workflow metadata at audited SHA; unit, resilience and integration steps successful | Job/step status verified; per-test log counts were not retrieved |
| [Artifact validation 35717936038](https://github.com/yoch/mqttium/actions/runs/35717936038) | `89352c0d23df14b8143442853b9b2cbe9576f95c`; build, resilience smoke, wheels on 3.11–3.14 and sdist on 3.12 successful | Relevant source and exercised test paths are byte-identical to audited main; publication and PyPI verification were intentionally skipped |
| [ARM64 paired 35716373609](https://github.com/yoch/mqttium/actions/runs/35716373609) | `852d572`; successful job status inspected | Useful supporting evidence; this audit did not revalidate every result cell/artifact |
| [ARM64 memory health 35716636859](https://github.com/yoch/mqttium/actions/runs/35716636859) | `852d572`; successful job status inspected | Supporting evidence, not a fresh measurement from this audit |
| [Strict open-loop 35718634040](https://github.com/yoch/mqttium/actions/runs/35718634040) | Exact audited SHA; failed, logs and result artifact inspected | An active release gate has a confirmed failure; details below |
| [Long fuzz 35714320518](https://github.com/yoch/mqttium/actions/runs/35714320518) | `c4d8306e529fdc5ee0260276b83437c39e09a493`; still in progress at observation | No completed long-campaign qualification claimed |
| [Long soak 35715555414](https://github.com/yoch/mqttium/actions/runs/35715555414) | `3ba3880c8f81c27d567d33a99abbecfdd26e70fa`; still in progress at observation | Linux two-hour MQTT 3.1.1/5 jobs unfinished; other successful jobs do not substitute for them |
| [ARM64 nightly runtime 35716645072](https://github.com/yoch/mqttium/actions/runs/35716645072) | `852d572`; cancelled | Not qualifying evidence |

The ARM64 workflow checks out `main`; the run metadata alone is not an immutable
checkout attestation. Main was independently verified unchanged during this
audit. For `852d572`, `3ba3880` and `c4d8306`, the source differences against the
audited main are module docstrings in three files; an AST comparison ignoring
docstrings found no executable differences. Relevant fuzz, installed-smoke and
soak paths were also compared. This supports behavioral relevance, but is not
byte-identical source evidence and does not replace qualifying the eventual
corrected release commit. In particular it is not a claim of identical timing
or binary artifacts.

The main CI log provides these concrete results:

- Quality suite: **1,901 passed**, 35.24 seconds, **92.19% branch-inclusive
  coverage**, required minimum 89%.
- Linux unit jobs: Python 3.11 had 1,805 passed/75 skipped; Python 3.12, 3.13 and
  3.14 each had 1,880 passed. Live integration: 13 passed on each version.
- macOS: Python 3.11 had 1,832 passed/75 skipped; Python 3.14 had 1,907 passed.
  Windows: Python 3.11 had 1,829 passed/78 skipped; Python 3.14 had
  1,904 passed/3 skipped. These are actual matrix results, not zero-skip claims.
- Resilience: 27 passed. Hypothesis/stateful group: 28 passed. Runtime
  qualification group: 71 passed. Deterministic fuzz seeds 1, 2 and 3 each
  completed 20,000 steps.
- Formatting, Ruff, mypy, Bandit, strict MkDocs, actionlint/zizmor, packaging and
  coverage upload steps were successful. A successful security linter is not
  an absence-of-vulnerabilities certificate.

The long fuzz plan names `rc-24-cpu-hours`, with five 288-minute workers. This is
24 aggregate worker-hours of planned wall time, not measured CPU consumption.
The unfinished soak's completed macOS and broker-compatibility jobs include
EMQX 5.8.9 and HiveMQ CE 2026.5. A skipped scheduled resilience step is not counted
as passed.

### Strict performance gate failure on the exact candidate

The result artifact names baseline
`c194597bcf5af4951fbec2b560600eef3cb84b3c` (RC14) and candidate
`6f057bc23179f2dc651521d98751e082c4ca389d`. Policy is strict, the invalidation list
is empty, and the failing cell's confirmation completed:

```text
MQTT 3.1.1; payload 64 bytes; receipt completion; window 100;
offered load 0.50 of baseline capacity
Failure diagnostic loop-lag ratio: 1.2630
Additive lower 95% bound: 0.008916 ms
Same-code noise floor:   0.001972 ms
Throughput ratio:       0.9994
```

The consolidated scenario summary reports loop ratio 1.2561, while the failure
diagnostic reports 1.2630; these are different summaries in the artifact and are
not silently conflated here. The result is a statistically qualified failure
under the repository's current gate. It is **not** evidence of a 26% throughput
regression: measured throughput in that cell is nearly unchanged.

Artifact `arm64-network-release-35718634040`, ID `10690368847`, was downloaded
and inspected. Its ZIP SHA-256 matches the GitHub artifact digest:

```text
e10ca37f31706b0090daf87e917ddac1ab29f2c53f9c13071dbf5069a70de85b
```

The `open-loop.json` SHA-256 is:

```text
094286e32904a1388cd6d9363ff15b93b8f9ed4121651845693834b4f61de340
```

Investigate this cell with comparable evidence after correctness repairs. Do not
describe it as a successful release gate, discard it merely because CI is green,
or weaken thresholds to obtain a pass. The small absolute delta should inform
engineering priority, while the declared acceptance policy remains explicit.

### Packaging and publication assessment

The version source, project metadata, build configuration, distribution
validation tools and publication workflow were inspected. Runtime dependencies
remain empty. The release workflow builds once, validates the produced artifact,
and uses trusted publishing for that artifact; no competing release build path
was identified. Artifact validation evidence above is distinct from publication:
the run intentionally did not upload or verify a PyPI release.

The source version is still `1.0.0rc14`; preparing a new stable version and exact
matching `v{__version__}` tag is a normal release prerequisite, not a newly found
runtime defect. A first stable release must qualify its final commit and exact
distribution artifacts after any corrections and metadata changes. No release
action was attempted in this audit.

## Audit coverage, checks and limits

### Production-source coverage

All 60 Python modules under `src/mqttium`, totaling 14,196 lines at this commit,
were read across four reviewers, with overlaps for cross-checks. This is reading
coverage, not a claim that all possible behaviors were tested.

| Production area | Modules | Lines | Review focus |
| --- | ---: | ---: | --- |
| Root models, properties, topics, errors, enums | 5 | 505 | Ownership, input contracts, typing, supported exports |
| API and runtime | 9 | 4,401 | Public operations, futures, receipts, cancellation, callbacks, writer/effect/delivery/lifecycle ownership |
| Codec | 6 | 1,214 | UTF-8, variable integers, property validation, buffering, size boundaries and owned bytes |
| Packet implementations | 15 | 1,821 | Fixed flags, reason codes, properties, version-specific lengths and encoding/decoding symmetry |
| Persistence | 3 | 1,265 | Schema refusal, atomic mutation, transaction failure, metadata layout, page hydration and replay |
| Protocol | 12 | 3,790 | Directional state, admission rollback, packet identifiers, flow control, replay, negotiation and reconnect settings |
| Transport | 8 | 1,079 | TCP/push/pull, Unix, TLS, WebSocket framing/upgrade/limits/cleanup and statistics |
| Dispatch | 2 | 121 | Ordered route matching, exact/wildcard indexes and frozen callback configuration |

Supporting maintained contracts were inspected: API stability and migration,
configuration/sizing, architecture and implementation ownership, MQTT 5 behavior,
persistence, transport/TLS, operations, testing/fuzzing, and release preparation.
The applicable MQTT statement for F02 was checked against the repository's
specification corpus. Tests, fuzz harnesses and workflows were read selectively
where relevant; this report does not claim a full line-by-line review of every
test, benchmark, workflow, or documentation page.

### Load-bearing invariant assessment

| Invariant | Assessment from review and focused checks |
| --- | --- |
| Single writer and consecutive segmented writes | Ownership remains localized in WritePump; F02 concerns terminal admission/order, not a second writer |
| Receipts/futures before wire | Publication paths and receipt registration have explicit coverage; registration alone does not prevent the F01 exchange identity error |
| Owned bytes at decoder boundaries | Copy/ownership paths inspected; no new reusable-buffer alias escape found |
| Packet identifiers distinct from flow slots | Separation is explicit; F01 is early API-result reuse, not conflation with QoS flow slots |
| One source of truth and rollback | Directional stores remain authoritative; F03 exposes failed-COMMIT ownership and F10 divergent byte accounting |
| User callbacks outside the engine lock | Structure and reentry tests support this precise contract; AUTH can execute while EffectPump owns its own lock, and lifecycle hooks have separate bounded ownership |
| Replay only after reconnect with session present | No in-session retransmission timer was found; replay continuation remains bounded by count, with the F10 byte-accounting defect |
| Connection epochs and delivery fences | Existing targeted separation/atomicity/interleaving tests pass; subscription settlement and terminal windows still need the new cases |

### Focused validation executed during this audit

These checks ran against the source archive using the existing development
environment. They are targeted baseline checks, not a replacement for remote
full qualification. Some selections overlap; their counts must not be summed as
a unique total.

| Slice | Executed result |
| --- | --- |
| API, configuration, properties, receipts and negotiation | 92 passed |
| Topics, callback API/order and reconnect loop | 37 passed |
| Runtime ownership, outbound transaction, delivery/protocol separation, effect atomicity, request cleanup and callback reentry | 56 passed in 1.40 seconds |
| Fatal disconnect, connecting disconnect, lifecycle and effect-failure routing | 26 passed in 2.00 seconds |
| SQLite/schema/corruption/maintenance, transition atomicity, replay and alternate transports | 130 passed: 124 in sandbox, six socket-dependent cases passed with authorized loopback access |
| Codec, packet strictness, conformance statements, ingress hardening, property encoding cache and UTF-8 symmetry | 154 passed in 1.73 seconds |
| Strict documentation generation | Passed in 7.05 seconds; actual generated model reference inspected for A01 |
| Standalone new reproductions | F01–F10 reproduced; F11 confirmed by public-consumer mypy; D01 reproduced with F09 |

The runtime groups used `test_outbound_transaction`,
`test_delivery_protocol_separation`, `test_effect_atomicity`,
`test_async_runtime_interleavings`, `test_subscription_mid_terminal_cleanup`,
`test_inline_callback_reentrancy`, `test_fatal_disconnect`,
`test_connecting_disconnect`, `test_async_lifecycle`, and
`test_effect_failure_routing`. The codec group used `test_codec`,
`test_packet_strictness`, `test_conformance_statements`,
`test_decoder_ingress_hardening`, `test_property_encoding_cache`, and
`test_utf8_symmetry` under `tests/unit`.

An initial system-interpreter attempt could not collect tests because it lacked
the configured pytest timeout plugin. It was replaced by the existing development
environment. Six initial loopback failures were sandbox permission failures, and
the exact six passed when rerun with authorized socket access. Neither is counted
as a product failure. The standalone probes were kept outside the repository and
did not alter production code or existing tests.

### Deliberate choices and non-findings

- Synchronous SQLite and its busy timeout are documented tradeoffs; this audit
  does not demand an asynchronous database abstraction. WAL with NORMAL
  synchronization is not a power-loss guarantee. The maintained recovery scope
  is process restart, and no power-loss experiment was conducted.
- Schema 5 deliberately rejects historical/future formats and unexpected schema
  objects. Historical migration is not promised. Payload-last columns,
  metadata-first pagination, lazy transaction acquisition and persisted logical
  sizes have concrete design rationale; preserve them while fixing F03/F10.
- No application-controlled SQL interpolation was identified. Dynamic
  placeholder lists and fixed table selectors are constrained. TLS relies on
  standard SSLContext validation; focused trust and hostname-rejection tests
  pass, and secure WebSocket refuses `ssl=False`.
- Broker-assigned ClientID persistence, subscription-intent persistence,
  application-job durability and exactly-once business processing are not promised
  by the session store. Their absence is not a new release blocker.
- Iterator mode ignoring callbacks, callback routes freezing on the first
  connection attempt, literal shared-filter matching, QoS 0 completion at writer
  admission, zero iterator-byte accounting when disabled, and documented builtin
  exceptions for certain invalid inputs are existing explicit contracts.
- The presence of a legacy MQTT 3.1 enum does not promise protocol support.
  Importability does not imply stability. No case for restoring removed adapters
  or one-shot helpers was established.

This audit found no additional confirmed codec memory-ownership, topic-matching,
SQL injection or TLS-validation defect in the reviewed paths. That statement is
limited to this review and its tests. There was no exhaustive penetration test,
physical crash/disk-error matrix, new local full-suite run, production-scale
benchmark, or independent rerun of every platform/broker combination. Long remote
campaigns remained unfinished at the observation time. Green tests and high
coverage cannot establish absence of races outside their scenarios.

## Recommended closure order

1. Repair **F01 and F02** with the exact deterministic regressions first. Their
   symptoms directly invalidate request success and terminal protocol behavior.
2. Repair **F03**, with real commit-failure isolation and cleanup assertions.
   Preserve the original exception and documented application ownership.
3. Finish the related runtime boundary work **F04–F07** together where sensible,
   without broad architectural refactoring. Recheck lifecycle self-reentry,
   receipt ownership, cancellation and epoch isolation.
4. Repair **F08–F10**, preserving documented sentinels, limits and the
   single-oversized-replay-record progress rule. Resolve **A01** before freezing
   model members; fix the small public typing defect **F11** at the same time.
5. Synchronize **Q01** and record an explicit disposition for remaining Provisional
   observability and low-priority cleanup debt. D01 does not itself require
   postponing release; misleading documentation should not accompany it.
6. Run the maintained quality, project, protocol, integration and security checks
   on the corrected candidate. Expand targeted fuzz grammar for the newly proven
   gaps, then qualify the final commit with required remote long campaigns.
7. Investigate and close the **strict open-loop failure** with comparable
   evidence. Record completed long-fuzz/soak results; an unfinished or cancelled
   run is not a pass. Existing campaign results on this candidate remain useful
   history but cannot fully qualify later runtime fixes.
8. Prepare the final version/tag and validate the exact release artifacts via the
   existing single-build trusted-publishing path. Publication is a separate
   action, not part of this audit.

All correction directions above are recommendations, not implemented changes.
The audit is complete as an assessment and reproducible report; the candidate is
not certified ready for release.

## Appendix A — Reproduction environment

The programs below are the actual audit probes, included inline so this report
does not depend on temporary files surviving. Save each Python block under the
indicated name outside the repository, then run from an isolated checkout or
archive of the audited SHA with its development dependencies installed using
pip. For runtime probes:

```bash
PYTHONPATH=src:. python /tmp/audit_probe.py
```

Use the chosen block's filename in place of `audit_probe.py`. For the typing
probe, run:

```bash
PYTHONPATH=src:. python -m mypy /tmp/audit-api-batch-typing.py
```

The runtime scripts import Internal helpers intentionally to instrument the
implementation under audit. That does not propose a supported extension API.
Most probes use existing packet-aware test transports, gate real async stages,
or replace only network opening. No production source is patched. SQLite's native
COMMIT authorizer is the explicit failure injection in B3. The WebSocket timeout
probe B6 intentionally takes about 30 seconds; the others are short. B8 uses a
small real-time liveness interval. The scripts expose the candidate's defects;
their assertions/expected outputs must change after fixes.

## Appendix B — Complete executable reproductions

### B1 — F01 — request acknowledgement reuse

Filename: `audit_subscription_ack_reuse_matrix.py`.

```python
import asyncio
from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from tests.support import QueueTransport, wait_until

class Transport(QueueTransport):
    def __init__(self, protocol):
        super().__init__(); self.protocol=protocol; self.decoder=IncrementalDecoder(); self.requests=[]; self.gate=asyncio.Event(); self.gate.set(); self.write_held=asyncio.Event()
    async def write(self, data):
        if not self.gate.is_set(): self.write_held.set()
        await self.gate.wait()
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT: self.push_rx(encode_frame(PacketType.CONNACK,0,b'\x00\x00'+(b'\x00' if self.protocol==MQTTProtocolVersion.MQTTv5 else b'')))
            elif raw.packet_type in (PacketType.SUBSCRIBE,PacketType.UNSUBSCRIBE): self.requests.append(int.from_bytes(raw.remaining[:2],'big'))
    async def close(self):
        self.gate.set(); await super().close()

async def case(protocol, unsub):
    t=Transport(protocol); c=AsyncClient('audit',protocol=protocol,keepalive=0,max_write_queue_messages=1)
    async def factory(*args,**kwargs): return t
    c._transport_factory=factory
    await c.connect('unused')
    call=c.unsubscribe if unsub else c.subscribe
    a=asyncio.create_task(call('first',timeout=.1))
    await wait_until(lambda: len(t.requests)==1)
    mid=t.requests[0]
    t.gate.clear(); await c.publish('block','x'); await t.write_held.wait()
    incoming=PublishPacket(topic='in',payload=b'x',qos=QoS.AT_LEAST_ONCE,mid=7,retain=False,dup=False).encode(protocol)
    reasons=b'\x00' if not unsub or protocol==MQTTProtocolVersion.MQTTv5 else b''
    properties=b'\x00' if protocol==MQTTProtocolVersion.MQTTv5 else b''
    t.push_rx(encode_frame(PacketType.UNSUBACK if unsub else PacketType.SUBACK,0,mid.to_bytes(2,'big')+properties+reasons)+incoming)
    await wait_until(lambda:c._write_pump.waiters==1)
    b=asyncio.create_task(call('second',timeout=.1))
    await wait_until(lambda:c._effect_pump.waiters>=2)
    t.gate.set()
    r=await asyncio.gather(a,b,return_exceptions=True)
    print(protocol.name, 'UNSUBSCRIBE' if unsub else 'SUBSCRIBE', 'MIDs',t.requests,'results',[type(x).__name__ for x in r], 'second ACK never emitted')
    assert type(r[0]).__name__=='MQTTTimeoutError'
    assert type(r[1]).__name__==('UnsubscribeResult' if unsub else 'SubscribeResult')
    await c.disconnect()
async def main():
    for protocol in (MQTTProtocolVersion.MQTTv311,MQTTProtocolVersion.MQTTv5):
        for unsub in (False,True): await case(protocol,unsub)
asyncio.run(main())
```

### B2 — F02/F04 — terminal admission and cancelled shutdown

Filename: `audit_terminal_window.py`.

```python
import asyncio
from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType
from tests.support import ScriptedBrokerTransport, wait_until

class Broker(ScriptedBrokerTransport):
    def __init__(self):
        super().__init__(protocol=MQTTProtocolVersion.MQTTv5); self.gate=asyncio.Event(); self.terminal_started=asyncio.Event(); self.packet_types=[]
    async def write(self,data):
        if data[0] & 0xf0 == int(PacketType.DISCONNECT):
            self.terminal_started.set(); await self.gate.wait()
        await super().write(data)
    def handle_packet(self, raw):
        self.packet_types.append(raw.packet_type.name); super().handle_packet(raw)
    async def close(self):
        self.gate.set(); await super().close()

async def setup():
    t=Broker(); c=AsyncClient('audit',protocol=MQTTProtocolVersion.MQTTv5,keepalive=0)
    async def factory(*args,**kwargs): return t
    c._transport_factory=factory; await c.connect('unused'); return c,t

async def fatal():
    c,t=await setup()
    t.push_rx(b'\xe1\x00')
    await asyncio.wait_for(t.terminal_started.wait(),1)
    print('fatal while draining state:',c.state.name)
    await c.publish('after/fatal',b'x')
    t.gate.set()
    await wait_until(lambda: not c.is_connected)
    print('fatal wire sequence:',t.packet_types)
    assert t.packet_types[-2:]==['DISCONNECT','PUBLISH']
    await c.disconnect()

async def cancelled():
    c,t=await setup()
    task=asyncio.create_task(c.disconnect())
    await asyncio.wait_for(t.terminal_started.wait(),1)
    task.cancel()
    try: await task
    except asyncio.CancelledError: pass
    await asyncio.sleep(.02)
    print('cancelled disconnect state:',c.state.name,'closed:',t.is_closing(),'live_tasks:',c._running_tasks())
    assert c.state.name=='DISCONNECTING' and not t.is_closing()
    await c.disconnect()
async def main():
    await fatal(); await cancelled()
asyncio.run(main())
```

### B3 — F03 — failed COMMIT committed by close

Filename: `audit_sqlite_commit_failure.py`.

```python
import sqlite3
import tempfile
from pathlib import Path
from mqttium.persistence import SqliteInflightStore
from mqttium.types import OutboundMessage
from mqttium.enums import QoS, OutboundQoSState

for batched in (False, True):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'store.sqlite'
        store = SqliteInflightStore(path)
        store.put_out(OutboundMessage(mid=1, topic='q', payload=b'old', qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.WAIT_PUBACK, logical_size=4))
        def authorizer(action, arg1, arg2, database, trigger):
            if action == sqlite3.SQLITE_TRANSACTION and arg1 == 'COMMIT':
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        store._conn.set_authorizer(authorizer)
        try:
            if batched:
                with store.batch():
                    store.complete_out(1, OutboundQoSState.WAIT_PUBACK)
            else:
                store.complete_out(1, OutboundQoSState.WAIT_PUBACK)
        except sqlite3.DatabaseError as error:
            print('batch=', batched, 'reported=', type(error).__name__, str(error), 'active_transaction=', store._conn.in_transaction, 'store_transaction_flag=', store._transaction_started)
        store._conn.set_authorizer(None)
        with sqlite3.connect(path) as observer:
            print('durable rows before close=', observer.execute('SELECT COUNT(*) FROM outbound').fetchone()[0])
        store.close()
        with sqlite3.connect(path) as observer:
            print('durable rows after close=', observer.execute('SELECT COUNT(*) FROM outbound').fetchone()[0])
```

### B4 — F05 — cancelled request future ownership

Filename: `audit_cancel_subscribe.py`.

```python
import asyncio, gc
from mqttium.api import AsyncClient
from tests.support import ScriptedBrokerTransport, wait_until
class Broker(ScriptedBrokerTransport):
    def __init__(self):
        super().__init__(); self.gate=asyncio.Event(); self.gate.set(); self.held=asyncio.Event()
    async def write(self,data):
        if not self.gate.is_set(): self.held.set()
        await self.gate.wait(); await super().write(data)
    async def close(self):
        self.gate.set(); await super().close()
async def main():
    t=Broker(); c=AsyncClient('audit',keepalive=0,max_write_queue_messages=1)
    async def factory(*args,**kwargs): return t
    c._transport_factory=factory; await c.connect('unused')
    observed=[]; asyncio.get_running_loop().set_exception_handler(lambda loop,context:observed.append(context))
    t.gate.clear(); await c.publish('hold',b'x'); await t.held.wait()
    a=asyncio.create_task(c.subscribe('never/observed'))
    await wait_until(lambda:c._write_pump.waiters>0)
    a.cancel()
    try: await a
    except asyncio.CancelledError: pass
    print('future retained after cancelled request:',len(c._sub_futs))
    await c.disconnect()
    del a; gc.collect(); await asyncio.sleep(0)
    print('loop exception messages:',[x['message'] for x in observed])
asyncio.run(main())
```

### B5 — F06 — a deadline spent twice

Filename: `audit-api-connect-deadline.py`.

```python
import asyncio
import time

from mqttium.api import AsyncClient
from mqttium.enums import PacketType
from tests.support import ScriptedBrokerTransport

class DelayedConnackTransport(ScriptedBrokerTransport):
    def handle_packet(self, raw):
        if raw.packet_type is PacketType.CONNECT:
            self.ack_timer = asyncio.get_running_loop().call_later(
                0.15, super().handle_packet, raw
            )
        else:
            super().handle_packet(raw)

async def main():
    transport = DelayedConnackTransport()
    async def factory(host, port, *, ssl=None):
        await asyncio.sleep(0.15)
        return transport
    client = AsyncClient('audit-connect-deadline', keepalive=0)
    client._transport_factory = factory
    started = time.monotonic()
    try:
        connack = await client.connect('fake-broker', timeout=0.20)
        elapsed = time.monotonic() - started
        print(f'connect(timeout=0.20) returned success in {elapsed:.3f}s; reason={connack.reason_code}')
        assert elapsed > 0.28
    finally:
        await client.disconnect()

asyncio.run(main())
```

### B6 — F06 — hidden WebSocket timeout

Filename: `audit_websocket_timeout.py`.

```python
import asyncio
import time
from unittest.mock import patch
from mqttium.api import AsyncClient

class Writer:
    def __init__(self):
        self.closed = False
        self.request = b''
    def write(self, data): self.request += data
    async def drain(self): pass
    def close(self): self.closed = True
    async def wait_closed(self): pass

async def main():
    reader = asyncio.StreamReader()
    writer = Writer()
    async def open_connection(host, port, *, ssl):
        return reader, writer
    client = AsyncClient('timeout-audit', connect_timeout=60, keepalive=0)
    started = time.monotonic()
    with patch('asyncio.open_connection', open_connection):
        try:
            await client.connect_ws('ws://broker.example/mqtt', timeout=60)
        except Exception as error:
            print('public timeout=60; elapsed=', round(time.monotonic()-started, 2), 'exception=', type(error).__name__, str(error), 'stream closed=', writer.closed)
    await client.disconnect()

asyncio.run(main())
```

### B7 — F07 — terminal connection cause masked by timeout

Filename: `audit_connect_terminal_cause.py`.

```python
import asyncio,time
from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion,PacketType
from tests.support import QueueTransport
class Peer(QueueTransport):
    def __init__(self,malformed): super().__init__(); self.malformed=malformed
    async def write(self,data):
        if data[0] & 0xf0 == int(PacketType.CONNECT):
            self.push_rx(b'\x21\x03\x00\x00\x00' if self.malformed else b'')
async def case(malformed):
    t=Peer(malformed); c=AsyncClient('audit',protocol=MQTTProtocolVersion.MQTTv5,keepalive=0)
    async def factory(*args,**kwargs): return t
    c._transport_factory=factory; errors=[]; c.on_disconnect=errors.append
    start=time.monotonic()
    try: await c.connect('unused',timeout=.1)
    except Exception as exc: print('malformed' if malformed else 'EOF','connect raised:',type(exc).__name__,str(exc),'elapsed',round(time.monotonic()-start,3),'actual teardown:',type(c._disconnect_exc).__name__+': '+str(c._disconnect_exc))
    await c.disconnect()
async def main():
    await case(False); await case(True)
asyncio.run(main())
```

### B8 — F08 — non-finite liveness configuration

Filename: `audit-api-nan-ping.py`.

```python
import asyncio
from mqttium.api import AsyncClient, ReconnectPolicy
from mqttium.protocol.reconnect import _ReconnectState
from tests.support import ScriptedBrokerTransport, transport_factory

async def main():
    clients=[]
    for timeout in (0.05, float('nan')):
        broker=ScriptedBrokerTransport(auto_pingresp=False)
        client=AsyncClient('audit-nan-ping', keepalive=1, ping_timeout=timeout)
        client._transport_factory=transport_factory(broker)
        await client.connect('fake-broker')
        clients.append((timeout, client, broker))
    try:
        await asyncio.sleep(1.7)
        for timeout, client, broker in clients:
            print(f'ping_timeout={timeout}, PINGREQ={broker.pingreqs}, connected={client.is_connected}')
        assert clients[0][1].is_connected is False
        assert clients[1][1].is_connected is True
        policy=ReconnectPolicy(initial_delay=float('nan'))
        print(f'ReconnectPolicy NaN delay: {_ReconnectState(policy).next_delay()}')
    finally:
        for _, client, _ in clients:
            await client.disconnect()
asyncio.run(main())
```

### B9 — F09/D01 — WebSocket receive limit and backlog statistics

Filename: `audit_websocket_size_and_stats.py`.

```python
import asyncio
import base64
import hashlib
import struct
from unittest.mock import patch
from mqttium.api import AsyncClient
from mqttium import MQTTProtocolVersion
from mqttium.transport.websocket import _parse_frame
from mqttium.transport._stream import StreamTransport

class Writer:
    def __init__(self, reader): self.reader, self.closed, self.transport = reader, False, self
    def get_write_buffer_size(self): return 0
    def is_closing(self): return self.closed
    async def drain(self): pass
    def close(self): self.closed = True; self.reader.feed_eof()
    async def wait_closed(self): pass
    def write(self, data):
        if data.startswith(b'GET '):
            key = next(line.split(b': ',1)[1] for line in data.split(b'\r\n') if line.startswith(b'Sec-WebSocket-Key:'))
            accept = base64.b64encode(hashlib.sha1(key+b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11', usedforsecurity=False).digest())
            self.reader.feed_data(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+b'\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n')
        else:
            parsed = _parse_frame(bytearray(data), 1<<20, expect_masked=True)
            if parsed and parsed[1] == 2 and parsed[2][:1] == b'\x10':
                self.reader.feed_data(b'\x82\x05\x20\x03\x00\x00\x00')
    def writelines(self, parts): self.write(b''.join(parts))

async def main():
    reader = asyncio.StreamReader(); writer = Writer(reader)
    async def open_connection(host, port, *, ssl): return reader, writer
    client = AsyncClient('size-audit', protocol=MQTTProtocolVersion.MQTTv5, maximum_packet_size=32*1024*1024, keepalive=0)
    with patch('asyncio.open_connection', open_connection):
        await client.connect_ws('ws://example/mqtt', timeout=1)
    print('connected=', client.is_connected, 'configured MQTT bound=', client.stats().decoder.max_packet_size, 'advertised MQTT5 bound=', client._engine._sent_maximum_packet_size, 'effective WS bound=', client._transport._max_frame_size)
    reader_task = client._reader_task
    reader.feed_data(b'\x82\x7f'+struct.pack('!Q',20*1024*1024))
    await asyncio.wait_for(reader_task, 1)
    print('after legal 20MiB binary frame header: connected=',client.is_connected,'cause=',type(client._disconnect_exc).__name__,str(client._disconnect_exc))
    await client.disconnect()
    buffered = asyncio.StreamReader(); stream_writer = Writer(buffered)
    buffered.feed_data(b'x'*100)
    stream = StreamTransport(buffered, stream_writer)
    print('pull transport stats with 100 bytes pending=',stream.stats().buffered_read_bytes,'actual=',len(buffered._buffer))

asyncio.run(main())
```

### B10 — F10 — property-heavy replay pages

Filename: `audit_replay_property_bounds.py`.

```python
import tempfile
from pathlib import Path
from mqttium.persistence import MemoryInflightStore, SqliteInflightStore
from mqttium.types import Properties, InboundMessage
from mqttium.enums import InboundQoSState, QoS, MQTTProtocolVersion, PacketType
from mqttium.protocol._sizing import publish_logical_size
from mqttium.protocol.engine import ProtocolEngine, EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.codec.buffer import RawPacket
from mqttium.protocol.inbound import REPLAY_BATCH_BYTES

properties = Properties({'user_property': tuple(('key'+str(i), 'x'*60000) for i in range(12))})
logical_size = publish_logical_size(True, 't', 1, properties)
with tempfile.TemporaryDirectory() as directory:
    stores = [MemoryInflightStore(), SqliteInflightStore(Path(directory)/'replay.sqlite')]
    for store in stores:
        with store.batch():
            for mid in (1,2,3):
                store.put_in(InboundMessage(mid=mid, topic='t', payload=b'x', qos=QoS.EXACTLY_ONCE, retain=False, state=InboundQoSState.WAIT_PUBREL, properties=properties, logical_size=logical_size))
        pages=list(store.in_replay_pages(64,REPLAY_BATCH_BYTES))
        print(type(store).__name__, 'limit=',REPLAY_BATCH_BYTES, 'page counts=',[len(page) for page in pages], 'page logical bytes=',[sum(m.logical_size for m in page) for page in pages])
        engine=ProtocolEngine(EngineConfig(client_id='replay-audit',protocol=MQTTProtocolVersion.MQTTv5,clean_start=False),store=store)
        engine.begin_connect(); engine.take_effects()
        engine.handle_raw(RawPacket(PacketType.CONNACK,0,b'\x01\x00\x00'))
        messages=[e for e in engine.take_effects() if e.kind is EffectKind.MESSAGE]
        print('first engine replay batch messages=',len(messages),'logical bytes=',len(messages)*logical_size)
    stores[1].close()
```

### B11 — F11 — public consumer typing

Filename: `audit-api-batch-typing.py`.

```python
from mqttium import PublishBatchError
from mqttium.api import AsyncClient, PublishMessage

async def publish_work(client: AsyncClient) -> None:
    try:
        await client.publish_many([PublishMessage('t', b'body', qos=1)])
    except PublishBatchError as exc:
        if exc.receipt is not None:
            print(exc.receipt.submitted)
            await exc.receipt.wait()
```
