# Changelog

All notable changes to MQTTium are documented here.

The format follows Keep a Changelog and versions follow Semantic Versioning.

## [Unreleased]

## [1.0.0rc14] - 2026-09-11

### Fixed

- Keep replacement-connection callbacks alive when a callback disconnects and
  reconnects before its own worker job returns (#455). Reopening retires the old
  queued work and its reservations before admitting the new connection, without
  replacing the active worker or changing steady-state callback dispatch.

- Preserve delivery across live sync-to-async topic-callback reconfiguration
  without disabling eligible synchronous inline dispatch (#453). A captured sync
  router checks its current execution mode before invoking user callbacks. Only
  unstarted work whose route became asynchronous transfers to the bounded worker,
  retaining FIFO ahead of reentrant admissions and the callback queue bound.
  Queued work redirects within its existing worker job. User synchronous callbacks
  returning awaitables remain contract violations.

### Changed

- Experimental message callback scheduling keeps one ordinary bounded worker
  for async callbacks, bursts, reentrant/queued work, direct-decode QoS 0 and the
  callback leg of `both`. One eligible idle synchronous callback-only MESSAGE
  effect may execute inline after the engine lock is released; a second eligible
  MESSAGE keeps the whole message run worker-owned. One queue entry represents
  each worker-owned notification; physical callback batches, queue-capacity
  mutation and synchronous pair-inline scheduling remain removed. Worker turns
  stay bounded by their starting queue occupancy. The separate `on_publish` fast
  path, strict `def`/`async def` callable contract (including rejection of a sync
  callback that dynamically returns an awaitable), and shared iterator byte
  accounting are unchanged.
- Use a stable topic dispatcher with a per-message live route snapshot. Private
  worker cancellation retires its active notification without losing queued work;
  explicit shutdown/reopen governs queue retirement and stale admission rejection.
  This is an experimental scheduling-contract change; see migration guidance.

- Receive cleartext TCP straight into the decoder's own storage on CPython selector event loops. `asyncio.BufferedProtocol.get_buffer()` returns a window carved out of `IncrementalDecoder`'s own slab, so received bytes are no longer copied through an intermediate receive buffer before reaching the parser. Storage is adaptive: it starts at 16 KiB, the receive window starts at 64 KiB and is promoted toward 256 KiB only under sustained full windows, a known large frame caps progressive growth at its exact extent without reserving the entire announced body, and an enlarged slab is retired once large frames stop arriving. TLS, WebSocket, Proactor, non-CPython runtimes and third-party event loops keep the `read()` + `feed()` path.

- **Provisional API change.** Receiving is now a transport capability rather than part of the common contract. `AsyncTransport` no longer declares `read()`; a transport offers exactly one of `PullTransport` (`read()`) or `DecoderPushTransport` (`attach_decoder()` + `receive()`), both exported from `mqttium.transport` and both runtime-checkable. On CPython selector event loops with cleartext TCP, `TcpTransport.connect()` returns a push-capable transport rather than an instance of the calling class, and that transport has no `read()` at all, so it cannot be misclassified as pull-capable. Custom transports should declare the capability they implement; the supported extension point remains the transport factory seam, not subclassing `TcpTransport`. TLS, WebSocket, Unix sockets, Proactor, non-CPython runtimes and third-party loops keep the pull capability unchanged.

- Fail MQTT 5 connection negotiation locally when the Server advertises `Maximum Packet Size` below 4 bytes. Such a limit is legal, but MQTTium cannot both respect it and guarantee the mandatory 4-byte PUBACK/PUBREC/PUBCOMP QoS responses. The client now raises `PacketTooLargeError` before entering `CONNECTED` instead of carrying connection-scoped tiny-peer branches until an acknowledgement is required. A limit of 4 remains supported.

- Stop executing MQTT 3.1 as a client protocol. `MQTTProtocolVersion.MQTTv31` remains a Stable enum member with value `3`, but selecting it now fails during configuration before runtime state is created. The dedicated MQTT 3.1 CONNECT encoder and inbound PUBLISH path are removed; MQTT 3.1.1 and MQTT 5 remain the supported protocols.

- Unify the Provisional persistence API around one complete `InflightStore`
  contract. Bounded replay, payload-free metadata paging, and conditional state
  transitions/completion are now required instead of runtime-detected optional
  capabilities. Remove `PagedInflightStore`, `BoundedInboundReplayStore`, and
  `TransitionInflightStore`, together with the eager/whole-object fallback paths.
  `MemoryInflightStore` and `SqliteInflightStore` also drop the retired helpers
  `update_out`, `out_items`, `out_pages`, `pop_in`, `update_in`, `in_items`,
  `in_pages`, and `contains_in`. Third-party stores must implement the modern
  `InflightStore` contract; see the migration and persistence guides.

- `PublishReceipt.wait()` no longer routes waiters through `asyncio.shield()`
  over one shared future. Each active waiter now parks on its own future held
  in a lazily created list, which isolates cancellation by construction instead
  of by wrapping. Observable semantics are unchanged: a publication nobody
  awaits still allocates no completion primitive, cancelling one `wait()`
  cancels only that waiter, a waiter created after another was cancelled still
  completes, and every waiter still receives the same terminal error instance.
  Settlement now releases the waiter collection, so no future is retained past
  completion or cancellation. The private `_future` field is replaced by
  `_waiters`.

## [1.0.0rc13] - 2026-09-04

### Fixed

- Avoid payload-sized temporary allocations when compacting or copying decoder storage. `feed()` now handles typed, multidimensional and strided memoryviews as wire bytes, and fragmented pull ingress shares the direct path's bounded large-frame sizing. A length announcement alone no longer reserves the complete packet.
- Reject custom transports offering neither or both receive capabilities before sending CONNECT, preserving the local error rather than reporting a misleading CONNACK timeout.

- Local store/persistence failures during ingress processing now propagate
  with their original exception instead of being converted to a
  peer-attributed `PROTOCOL_ERROR`. An already-observed terminal broker
  outcome (PUBACK / PUBCOMP success, MQTT 5 reason codes at or above `0x80`,
  negative MQTT 5 PUBREC) still settles its receipt; a failed ingress lot
  keeps only terminal publish outcomes and drops anything else it produced;
  a local-terminal failure fail-stops the client (no automatic reconnect or
  replay, explicit `connect()` refused, no new publish admission — create a
  new `AsyncClient`). A replacement client built on the same ambiguous or
  known-stale store is not automatically repaired: reconcile persistence and
  broker-session state explicitly. Direct `ProtocolEngine` consumers: `handle_raw()` may
  now raise local store exceptions that previously surfaced as
  `PROTOCOL_ERROR` effects; handle them around `handle_raw()` instead of
  matching on `"Internal handler error"`.

### Changed

- Clarify that only MQTT 3.1.1 and MQTT 5 are supported and tested. MQTT 3.1
  (`MQTTProtocolVersion.MQTTv31`) is explicitly out of the support matrix; the
  Stable enum member is retained for backwards compatibility.
- Restore the ordinary QoS 0 publication hot path, removing the generic
  prepared-publish carrier overhead while preserving validation and
  backpressure semantics.
- Reduce QoS 1/2 publication preparation overhead and remove success-ACK byte
  reclassification from ordinary writer admission by carrying ACK provenance
  internally; no Stable API or ordering/backpressure semantics changed.
- Document `InflightStore.batch()` atomicity and the `take_effects()`
  ownership-transfer boundary; third-party stores must not report backend
  failures with `MQTTError`.

## [1.0.0rc12] - 2026-09-02

### Changed

- Reuse one mutation-free QoS 1/2 publication preparation between synchronous
  writer-capacity preflight and protocol admission. Resident-writer
  `publish_nowait()` and `publish(..., nowait=True)` calls no longer size the
  topic or encode MQTT 5 properties twice; packet identifiers, receipts,
  rollback, wire ordering, and queue bounds are unchanged.
- Give fixed success PUBACK, PUBREC, and PUBCOMP frames one bounded eager-write
  permit independent from application data. A synchronous `on_message` reply
  can therefore reach the transport in the callback turn, while ACK and data
  bursts remain limited to one eager write of each kind per event-loop turn.

### Fixed

- Keep inline message delivery stable when a synchronous `on_message` callback
  publishes reentrantly. The nested publication may append SEND effects to the
  active effect deque; those effects now remain ordered behind the message
  prefix instead of invalidating its iterator and failing the connection.

## [1.0.0rc11] - 2026-08-30

### Added

- Add Stable `AsyncClient.message_callback_add` / `message_callback_remove` for
  topic-filtered inbound callbacks. Matching filters run instead of `on_message`,
  in registration order, using the existing `TopicMatcher`. The matcher exists
  only while filters are registered; otherwise inbound delivery reads the direct
  installed `on_message` pointer with no topic-routing branch. Protocol engine
  and the Paho façade remain independent: the façade keeps its own matcher and
  VERSION2 callback wrapping.

### Changed

- Stabilize the manual ARM64 network release gate by fixing the measured process
  address layout, using six-cycle same-code controls, and checking historical
  host load only before the first block. Later blocks retain instantaneous CPU,
  temperature, and governor checks without waiting for their own one-minute load
  average to decay.
- Correct the Paho compatibility design notes to state the normative MQTT 5
  Receive Maximum behavior for QoS 2: successful PUBREC keeps the sender's
  quota slot until PUBCOMP, failed PUBREC releases it, and PUBREL does not
  acquire a slot.
- Keep the Paho compatibility façade callback-only for inbound delivery and
  install its message dispatcher and topic matcher only while `on_message` or a
  filtered callback is registered. Idle façade clients therefore allocate no
  inbound wrapper and do not accumulate messages in an unused iterator queue.

### Fixed

- Enforce the MQTT 5 Receive Maximum, Maximum Packet Size, and Topic Alias
  Maximum values actually advertised in CONNECT when explicit
  `connect_properties` override their dedicated constructor settings.
- Invoke idle synchronous `on_publish` and eligible `on_message` callbacks in
  the reader/effect-drain turn after receipt or delivery settlement. Async
  callbacks, callback bursts, reentrant delivery, and occupied/full callback
  queues retain the bounded callback-worker path and its existing backpressure.
- Reuse an MQTT 5 broker-assigned Client Identifier when reconnecting the same
  durable Session, and fail before CONNECT when persisted resumable QoS state
  has no stable ClientID after a process restart.
- Preserve malformed-packet and packet-too-large classifications across the
  protocol engine/runtime boundary so MQTT 5 fatal DISCONNECT responses use the
  correct reason code while local failures remain local.

## [1.0.0rc10] - 2026-08-26

### Added

- Add a native `stats()` snapshot with Provisional nested delivery, ingress,
  receipt, task, engine, and writer counters. It is derived from component-owned
  state and does not start a sampler or logging subsystem.

### Changed

- Replace the internal deque writer queue with one bounded list/deque hybrid
  that supports O(1) FIFO admission and contiguous write batches while preserving
  backpressure, byte accounting, and connection-epoch ownership.

## [1.0.0rc9] - 2026-08-24

### Changed

- Replace repeated `inspect.iscoroutinefunction()` checks on callback hot paths
  with a callable-form classifier that recognizes plain functions, bound methods,
  partials and callable objects without dynamic invocation.

## [1.0.0rc8] - 2026-08-21

### Added

- Add callback delivery modes `auto`, `iterator`, `callback`, and `both`, with
  bounded iterator and callback queues and explicit delivery byte accounting.

### Changed

- Keep callback execution outside the protocol-engine critical section and isolate
  callback failures from protocol state. Callback admission remains bounded;
  queue saturation applies backpressure rather than growing an unbounded task set.

## [1.0.0rc7] - 2026-08-18

### Changed

- Improve reconnect ownership and receipt settlement under transport failure.

## [1.0.0rc6] - 2026-08-14

### Changed

- Add bounded writer batching while keeping one write owner and preserving effect
  order.

## [1.0.0rc5] - 2026-08-10

### Changed

- Improve native async publication throughput without changing receipt semantics.

## [1.0.0rc4] - 2026-08-07

### Changed

- Introduce explicit bounded application-delivery queues and callback isolation.

## [1.0.0rc3] - 2026-08-04

### Changed

- Improve incremental decoder and effect-drain batching.

## [1.0.0rc2] - 2026-08-01

### Changed

- Add MQTT 5 flow-control and packet-size negotiation.

## [1.0.0rc1] - 2026-07-29

### Added

- First release-candidate line for the native async client.
