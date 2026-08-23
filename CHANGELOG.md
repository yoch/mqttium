# Changelog

All notable changes to MQTTium are documented here.

The format follows Keep a Changelog and versions follow Semantic Versioning.

## [Unreleased]

### Fixed

- Preserve an immediate writer failure while sending CONNECT, even if closing
  the transport also fails, instead of reporting a later CONNACK timeout.
- Preserve an active `messages()` iterator across automatic reconnects, while
  binding iterators to explicit connection generations so an old iterator
  cannot consume messages from a later explicit connection.
- Route writer-close and deferred-effect failures through one reader-owned
  connection teardown path, preserving the original error for active callers
  and disconnect/reconnect observability without poisoning later operations.
- Treat an empty WebSocket binary message as valid data rather than transport
  EOF.
- Validate required SQLite tables and columns even when `user_version` already
  names the current schema.
- Release connection-scoped SUBSCRIBE and UNSUBSCRIBE packet identifiers on
  broker and protocol terminal transitions.
- Snapshot `MemoryInflightStore` iterator membership so deleting records during
  replay cannot raise `RuntimeError: dictionary changed size during iteration`.

### Changed

- Reduce high-rate QoS 0 subscriber overhead by decoding eligible MQTT 3.1.1 and MQTT 5 callback deliveries directly from the bounded ingress buffer into owned `Message` values. Stateful MQTT 5 Topic Alias traffic and all mixed, QoS 1/2, control, error, and backpressure paths retain the historical protocol-engine path.
- Reframe the project documentation around the native `AsyncClient` for
  production asyncio services, gateways, and IoT systems. The Paho VERSION2
  compatibility façade remains tested and Provisional as a migration aid, but
  is no longer presented as a co-equal product surface.
- Rationalize the test and GitHub Actions architecture, including
  branch-inclusive coverage, mandatory broker integration, deterministic fuzz
  coverage, consolidated release validation, and explicit self-hosted runner
  trust boundaries.
- Publish the existing branch-inclusive coverage report to Codecov using OIDC.
  Codecov is an informational, base-relative review aid because its treatment
  of partially covered branch lines differs from coverage.py; the repository's
  local branch-inclusive 89% coverage gate remains authoritative.

### Documentation

- Correct the Provisional Paho compatibility matrix: its synchronous façade
  does not expose TLS configuration; use the native `AsyncClient` TLS surface.
- Define the persistence and transport exception boundaries without wrapping
  Python system exceptions in an artificial MQTT-specific hierarchy.
- Display the MQTTium logo prominently on the documentation home page.
- Document the test taxonomy, local release-equivalent commands, reliability
  rules, and the distinction between coverage.py and Codecov percentages.
- Add a versioned MkDocs Material / Read the Docs site, exhaustive Stable API
  reference, user-oriented guides, curated historical-evidence index, project
  support policy, and a single canonical `AGENTS.md` repository guide.
- Reconcile the active scheduler experiment statuses with their accepted,
  eligible-runner evidence and merge decisions while preserving dated report
  bodies unchanged.
- Generate canonical and AI-facing documentation links from the active Read the
  Docs build URL so `latest`, stable releases, and pull-request previews remain
  self-contained, and render the Material navigation cues on the documentation
  home page. Keep the installable documentation extra on the same bounded tool
  major versions used by hosted builds.
- Refresh the README, package metadata, community templates, and repository
  discovery metadata for the stable launch. Public comparative benchmark
  claims are deferred to the independent cross-client benchmark repository.

## [1.0.0rc8] - 2026-08-20

### Fixed

- `max_outbound_messages` now counts writer-resident admitted frames, including
  the writer's active batch, rather than only `queue.qsize()`. Extracting a
  batch no longer opens a window that could admit more resident frames than
  configured. `WriterStats.queued_messages` is still the live queue size.
- Fix the rc7 burst-latency regression for ordinary awaited QoS 1/2 publishing
  with a bounded latency microbatch: flush the entire already-admitted,
  non-segmented writer queue only when it contains 4 to 16 frames and reaches
  48 KiB or the 16-frame cap. QoS 0, `publish_nowait()`, segmented writes, FIFO
  ordering and the normal writer fallback are unchanged.

### Changed

- Writer-capacity release wakes only as many blocked enqueue waiters as a
  successfully written batch can plausibly service instead of waking every
  waiter. Lifecycle and terminal paths still wake all, and cancellation
  forwards a consumed targeted wake so another waiter can make progress.
- Publish-admission capacity now wakes blocked publishers proportionally to
  released slots instead of using one shared wake-all event. `publish_many()`
  remains one waiter for its chunk and terminal teardown still wakes all.

### Documentation

- Add writer backpressure guidance explaining `max_outbound_bytes` as a
  burst-buffer, batching and FIFO head-of-line latency trade-off, including how
  to distinguish backlogged from clear-path latency measurements.

## [1.0.0rc7] - 2026-08-18

### Fixed

- Reject non-empty MQTT 5 `Properties` on MQTT 3.1/3.1.1 CONNECT, Will,
  PUBLISH, SUBSCRIBE and UNSUBSCRIBE encodings instead of silently dropping
  fields that cannot exist on the selected wire protocol. Empty `Properties()`
  remains wire-equivalent to no properties.
- Purge flow-blocked retransmissions from the outbound queue when a CONNACK
  reports Session Present 0. `replay_session()` can leave WAIT_\* records in
  the queue when the broker's Receive Maximum window cannot admit them; after
  the server-side session expired, a stale entry made `drain()` re-materialise
  an already-failed record, double-release its byte reservation (an internal
  `AssertionError` surfaced as a protocol error) and abort the fresh
  connection. Every publication is now failed exactly once with
  `SessionDiscardedError` and the reconnect proceeds.
- Ignore packets still buffered by the transport after the engine reached a
  terminal state. A trailing packet after a broker DISCONNECT or refused
  CONNACK used to surface as a `PROTOCOL_ERROR` that replaced the real
  disconnect reason in `Client.disconnect_reason`.
- Reject `begin_connect()` while the engine is DISCONNECTING (the final
  DISCONNECT is still draining to the transport) instead of silently flipping
  the state machine to CONNECTING under a closing connection.
- Answer a PUBREC carrying a Reason Code of 0x80 or greater by ending the QoS 2
  exchange on every store, as MQTT 5 §4.3.3 requires. The check previously sat
  inside the conditional-transition branch, so a third-party `InflightStore`
  without the `TransitionInflightStore` extension reached the "no such record"
  test first and answered an unknown packet identifier with an orphan
  PUBREL 0x92. The shipped memory and SQLite stores were never affected.

### Removed

- `ClientStats.protocol` and the `ProtocolStats` type (Provisional). The
  aggregate was documented as retained for pre-stable compatibility and every
  one of its nine fields duplicated a value already present in
  `ClientStats.outbound` / `ClientStats.inbound`. Read those instead — the
  mapping is one-to-one, e.g. `stats().protocol.flow_inflight` becomes
  `stats().outbound.flow_inflight`, and `stats().protocol.inbound_inflight`
  becomes `stats().inbound.inflight`.
- `IncrementalDecoder.process_packets` (Provisional). It had no caller;
  `process_packets_bounded` is the count- and byte-bounded form the client uses.

### Changed

- `AsyncClient.messages()` returns the delivery iterator directly instead of
  being an async generator that re-yields from it, removing one generator
  resume/suspend per delivered message. `async for client.messages()` and
  `anext(client.messages())` are unaffected; only
  `inspect.isasyncgenfunction(AsyncClient.messages)` changes from `True` to
  `False`.

## [1.0.0rc6] - 2026-08-16

### Fixed

- Enforce MQTT 5 enhanced-authentication phase sequencing: require Authentication
  Method where mandated, reject AUTH Success during initial CONNECT authentication,
  and accept Server AUTH Success/Continue while CONNECTED only during an active
  client re-authentication exchange.
- Reject unsolicited CONNACK Response Information unless the CONNECT wire snapshot
  requested it.
- Allow graceful `disconnect()` while CONNECT is in flight without turning an
  intentional shutdown into a Will-triggering transport abort.
- Reject MQTT 5 `Session Present=1` when the client retains no MQTT Client Session
  State, using hydrated O(1) state rather than a second persistence scan.
- Reject the Client-only `Disconnect with Will Message` reason when it is received
  from a Server.
- Preserve manual QoS 1 PUBACK wire order when application acknowledgements arrive
  out of order, including recovered durable state across reconnect.

### Changed

- Outbound writes no longer always cost an event-loop turn. When the writer
  queue is empty, no write is in flight and no producer is waiting for queue
  space, `WritePump` buffers a non-segmented frame straight through the
  transport, instead of waking the writer task to do it. Measured by counting
  loop iterations (not timings): 1 turn between `publish_nowait()` and the
  transport write, in 50 of 50 publishes, becomes 0. The same turn was paid by
  every automatic PUBACK. Wire order and the one-write-in-flight rule are
  unchanged; segmented `(header, payload)` frames are never written this way.
  Validated on a preflight-eligible host against a live broker: median callback
  p50 latency improves by 16.7% to 27.8% across four load points (2 500, 4 000,
  4 500 and 7 500 msgs/s) with every pair favouring the change, while the
  completed rate is unchanged or slightly higher. Certified on MQTT 3.1.1 with
  an outbound window of 64, and independently on MQTT 5 with a window of 20
  (+26.6 % and +25.6 %). See
  `docs/reports/NATIVE-WRITER-HOP-2026-08-16.md`.
- `mqttium.compat.paho`: QoS 1/2 `publish()` no longer blocks the calling thread
  until the network loop has allocated a packet identifier. All QoS levels now
  return as soon as the request is accepted, matching Paho's shape. Measured with
  `benchmarks/compat_qosn_submit_ab.py` on a preflight-eligible host, the
  single-producer mean drain batch rose from 1.00 to 224.88 and the coalesced
  path went from 0.69x to 2.67x the one-callback-per-message handoff it is meant
  to beat. This is a submit-rate benchmark with no broker I/O.
- `mqttium.compat.paho`: `MQTTMessageInfo.mid` for QoS 1/2 is now a façade
  correlation identifier wrapping over `1..65535`, not the wire packet
  identifier. `on_publish` reports the same value `publish()` returned. Packet
  identifiers remain owned by the network loop (`docs/paho-compatibility.md` §8).
- `mqttium.compat.paho`: an admission refusal that happens after `publish()`
  returned is reported as `rc = MQTT_ERR_QUEUE_SIZE` on the returned handle
  rather than synchronously. `wait_for_publish()` and `is_published()` re-check
  `rc` once admission has settled, so a refused publication cannot report
  success. Cross-thread ingress saturation is still refused synchronously with
  `mid = None`. Because a producer can now outrun the loop, `MQTT_ERR_QUEUE_SIZE`
  is reachable for QoS 1/2 under sustained overload; producers must shed.
- `mqttium.compat.paho`: QoS 0 is committed through the same writer-direct path
  the native client uses, removing 2 engine effects and 2 effect-pump round
  trips per message (exact counts), with the effect path kept as the fallback.
  Ordering across QoS levels is unchanged.

### Added

- `ClientStats.writer` gains `eager_writes` and `eager_bytes`: how many frames
  bypassed the writer-task wakeup, and how many bytes they carried.
- `StreamTransport.write_nowait(data) -> bool` (TCP and Unix): buffer one frame
  without awaiting, declining when a drain is due. It is deliberately **not** on
  the `AsyncTransport` protocol — a transport may offer it, and one whose write
  is more than a buffer append must not, so `WebSocketTransport` does not.
- `mqttium.compat.paho.Client(max_outbound_inflight=...)`: cap unfinished QoS 1/2
  publications below the broker's Receive Maximum. Attach-time only, so it can no
  longer be reached only by rebuilding the inner `AsyncClient`.

### Removed

- `mqttium.compat.paho._PUBLISH_HANDOFF_TIMEOUT` (internal): `publish()` no
  longer waits for loop-side admission, so there is nothing to time out.

## [1.0.0rc5] - 2026-08-16

### Changed

- Transfer one eligible small inbound MESSAGE directly to the bounded
  callback/iterator delivery queues, avoiding an EffectPump task wake-up while
  retaining the existing capacity, persistence-mark and callback-isolation
  guards.
- Admit native QoS 0 `on_publish` callbacks directly to the bounded callback
  worker after atomic writer admission, avoiding the intermediate effect-task
  wake-up when callback capacity is immediately available. Full callback queues
  retain the existing ordered backpressure path, including atomic batch
  fallback.
- Admit QoS 1/2 `on_publish` callbacks directly to the bounded callback worker
  when capacity is immediately available, settling the receipt without an
  intermediate effect-task wake-up. Callback execution remains isolated, and
  a full callback queue retains the existing ordered backpressure path.
- Clarify that native QoS 0 receipts and `on_publish` callbacks mark writer
  admission rather than socket drain or broker receipt, including how to size
  the writer byte budget and handle immediate backpressure.
- Extend the independent paced open-loop benchmark with fixed absolute rates,
  outbound-window sweeps and automatic same-tree A/A control validation while
  preserving the existing calibrated-fraction release mode; correlate callback
  completions in FIFO order so reused MQTT packet identifiers remain measurable.

- Skip empty durable inbound-store metadata probes on the automatic QoS 1 steady-state path while retaining collision checks as soon as persisted inbound state exists.
- Reuse MQTT 5 property-table bytes already encoded during immediate QoS 1/2 admission; queued publications continue to encode their current mutable properties at launch.
- Reuse validated Topic Name bytes on immediate QoS 1/2 launch only when flow capacity is available, avoiding eager bytes that would be discarded by a saturated connected queue.
- Carry the exact wire size of fresh decoded MQTT 5 properties as ephemeral effect metadata so small and accounted delivery avoid re-encoding without storing stale size state on mutable `Properties` or in persistence.
- Decode inbound MQTT 5 QoS 0 directly into the delivered `Message`, preserve the decoded-property-size handoff, and skip Topic Alias resolution only for non-empty alias-free Topic Names; QoS 1/2 retain the generic field decoder.

## [1.0.0rc4] - 2026-08-13

### Changed

- Rationalize the protocol engine internals without behaviour change: in-flight
  SUBSCRIBE/UNSUBSCRIBE requests are tracked in one structure instead of a set
  kept in sync with a dict, the two `queue_subscribe`/`queue_unsubscribe`
  allocation/rollback paths share one helper, `InboundSession.on_pubrel` runs
  one state machine over both store shapes instead of two parallel copies, the
  outbound flow window is restarted from CONNACK in one place, and the
  unreachable broker-PINGREQ handler (already refused by the per-state packet
  gate) is removed.
- Validate offline-queued publications once per CONNACK instead of twice on the
  resumed-session branch: `replay_session` already checks every record against
  the new negotiation before retransmitting or re-queueing it, so the second
  full-queue pass — which re-encoded each message's MQTT 5 property table — is
  gone. Resuming a session with 10,000 queued QoS 1 publications handles the
  CONNACK about one third faster; failure effects and their ordering are
  unchanged.

## [1.0.0rc3] - 2026-08-13

- Replace generic packet encode/decode dispatch with direct MQTT 3.1.1 and
  MQTT 5 primitives across acknowledgements, PUBLISH, CONNECT/CONNACK,
  subscriptions and control packets. Protocol sessions bind their hot PUBLISH
  and acknowledgement paths once, avoiding per-packet version branches,
  generic helper frames and transient packet dataclasses while the Provisional
  packet views remain available as thin factories.

- Batch consecutive small non-persisted MESSAGE effects (QoS 0 and fresh automatic QoS 1) directly during the inline effect drain, and skip the absent-row delivery mark for automatic QoS 1 while retaining marks for persisted replay/manual/QoS 2 records.

- Hold the inbound Receive Maximum slot for an automatic QoS 1 PUBACK until
  the effect batch is handed off, so pipelined PUBLISH packets are admitted by
  the ordinary acquire path instead of a second decode that reconstructed the
  count from SEND bytes.

- Fast-path common success/no-properties PUBREC, PUBREL and PUBCOMP frames without transient packet objects while retaining full MQTT 5 property/RPI validation for longer acknowledgement forms.

- Avoid transient `PublishPacket` allocation when launching or re-encoding stored QoS 1/2 outbound messages; call the shared functional PUBLISH encoder directly while preserving byte output and replay semantics.

- Optimize MQTT 3.1.1 inbound QoS 2 PUBLISH decoding by reusing the specialized QoS 1/2 field parser, avoiding a transient `PublishPacket` without changing QoS 2 state semantics.

### Fixed

- Preserve the broker's terminal CONNACK refusal error when reconnect gives up,
  so pending publish receipts and admission waiters no longer receive a generic
  `Connection closed` failure during reader teardown.
- Keep Paho `loop_stop()` scoped to the network-loop generation it observed,
  so a concurrent restart cannot have its replacement loop stopped or cleared
  by an older stop operation.
- Harden MQTT-over-WebSocket teardown and handshake handling: close the TCP
  stream before any cancellable close wait, bracket IPv6 literals in the HTTP
  `Host` header, and enforce the configured handshake timeout as one total
  deadline instead of resetting it for each received chunk.
- Correct MQTT string and PUBLISH conformance: preserve legal U+FEFF text,
  reject empty MQTT 3.1.1 inbound Topic Names at the packet boundary, surface
  PUBLISH wire-limit overflow as `PacketTooLargeError`, reject semantic Will
  fields without a Will Topic, and normalize reason-only MQTT 5 ACK properties.
- Harden the Paho VERSION2 façade: serialize `loop_start()` ownership, reject
  blocking publish waits and disconnects from the network thread, propagate
  QoS 0 admission failures through `MQTTMessageInfo`, restore Paho's
  `is_connected()` method shape and `ConnectFlags`, accept `payload=None` as a
  zero-length publish, and validate per-topic callback filters.
- Keep automatic reconnect eligible after failed transport/CONNACK attempts, so
  the configured retry policy can reach later attempts and terminal exhaustion
  settles pending receipts; carry refused CONNACK reason codes structurally
  instead of recovering them from exception text.
- Tighten low-risk lifecycle and persistence edges: remove the unused opposite-
  direction `FlowControl` argument, make delivery stream reset synchronous, pin
  SQLite's existing five-second busy timeout explicitly, close SQLite handles
  on constructor failures, reject version-current databases missing required
  tables, validate `helpers.subscribe.simple(msg_count)` as positive, and refresh
  RC2 API-stability/roadmap status.
- Local MQTT protocol failures now complete teardown even when the negotiated
  Maximum Packet Size prevents sending the normative DISCONNECT, close the
  runtime transport, and surface CONNACK validation failures to `connect()`
  instead of timing out.
- MQTT 5 client-side DISCONNECT encoding enforces direction-specific reason
  codes and rejects Server Reference; CONNECT rejects Authentication Data
  without Authentication Method.
- Request Problem Information is enforced on inbound acknowledgement and AUTH
  packets before QoS/subscription state mutates.
- Automatic reconnect stops on permanent MQTT 5 connection refusals such as
  Banned and protocol/configuration errors, while transient capacity failures
  such as Connection rate exceeded remain eligible for backoff and retry.

- Refresh the retained memory benchmark contract for the current bounded
  WebSocket coalescing path and its RC2-equivalent property-heavy allocation
  baseline.
- End asynchronous ingress batches exactly when automatic QoS 1 acknowledgements
  fill the remaining local Receive Maximum window, preventing another PUBLISH
  from being admitted before those acknowledgements reach the effect pump while
  preserving full-size batches for QoS 0 and control traffic.

## [1.0.0rc2] - 2026-08-12

### Fixed

- Inbound QoS state now rejects packet-identifier reuse across unfinished QoS 1
  and QoS 2 exchanges without overwriting durable records or leaking Receive
  Maximum/byte reservations. Automatic acknowledgements remain counted until
  their runtime handoff, including mixed QoS 1/QoS 2 batches.
- MQTT 5 request/response correlation is stricter: SUBACK and UNSUBACK must match
  the outstanding request type and result count before the MID is released;
  malformed PINGRESP packets, server DISCONNECT packets carrying forbidden
  Session Expiry changes, and successful empty-client-id CONNACK packets without
  an Assigned Client Identifier are rejected.
- MQTT 5 topic/property validation is applied before connection-scoped Topic
  Alias state can mutate. Response Topic rejects empty values and wildcards for
  both PUBLISH and Will properties, invalid Will Topics are refused, and an
  outbound empty Topic Name is not accepted without established alias state.
- Directional enhanced-authentication rules are enforced: a broker cannot send
  Re-authenticate (`0x19`), a client cannot send Success (`0x00`), unsupported
  AUTH is rejected without leaving the engine connected, and handler failures
  surface against the connection attempt that caused them.
- The broker's Maximum Packet Size is enforced on publication and control-packet
  paths, including graceful/fatal DISCONNECT, PINGREQ, manual PUBACK/PUBCOMP and
  authentication fallbacks. Manual ACK validates before durable mutation; if a
  mandatory control packet cannot fit, the transport is closed rather than
  emitting an illegal packet. An intentional `disconnect()` fallback still reports
  a clean `on_disconnect(None)`, while an AUTH rejection that cannot fit closes the
  active connection instead of leaving it open.
- Graceful and fatal shutdown preserve the single-writer invariant, bound
  terminal enqueue/drain, reject new publication while disconnecting, and leave
  transport and protocol state consistently closed even when no DISCONNECT can
  legally be encoded.
- MQTT 3.1/3.1.1 connection and PUBLISH edge cases are validated explicitly,
  including empty ClientId session rules, password-without-username in 3.1.1,
  and the MQTT 3.1 regression where payload bytes were briefly interpreted as
  MQTT 5 property length.
- `EffectPump` no longer hands a stale background exception to an unrelated
  later operation, while enhanced-auth handler failures during connection are
  still delivered to the waiting `connect()`.
- Cached MQTT 5 property encoding snapshots mutable binary values, so in-place
  `bytearray` changes cannot reuse stale encoded property bytes.
- Additional conformance checks cover Clean Start/Session Present, client-side
  Subscription Identifier direction, DISCONNECT Session Expiry, Response Topic,
  Will Topic, shared-subscription No Local, and other MQTT 5 packet-direction
  constraints found during the RC1 audit.

### Changed

- MQTT 5 inbound QoS 0/1/2 PUBLISH handling decodes directly into the existing
  delivery/QoS state machines instead of constructing a short-lived
  `PublishPacket`; protocol-specific MQTT 3.1/3.1.1 fallbacks remain covered.
- Common acknowledgement and publication hot paths avoid redundant codec work:
  success PUBACK encode/decode uses the fixed form, validated QoS 0 Topic Name
  bytes are reused, and MQTT 5 property bodies are reused across sizing and wire
  encoding with mutation-safe invalidation.
- Exact callback filters retain O(1) lookup while wildcard filters use the small
  ordered scan. Filters are matched literally, including `$share/{group}/...`,
  preserving Paho callback semantics rather than rewriting shared-subscription
  filters inside the callback matcher.
- Automatic ACK effects are produced SEND-first, queued SQLite launches use
  state-only transitions instead of rewriting payload BLOBs, segmented writes
  use the measured 128 KiB threshold, and WebSocket writes coalesce bounded MQTT
  items into fewer masked frames without changing ordering or writer limits.

### Added

- `docs/spec/` vendors numbered MQTT 3.1.1 and MQTT 5.0 conformance statements
  from reproducible OASIS archives, with provenance/regeneration tooling and
  executable checks linked from `docs/conformance.md`.

### Documentation

- The cross-client QoS 1 latency claim from the 2026-08-09 external benchmark is
  retracted in the reports index. The apparent 2.95x gap came from pacing each
  client at a fraction of its own calibrated capacity rather than comparing
  matched absolute load; the dated source report remains unchanged as a
  historical record.
- `InflightStore.update_out()` / `update_in()` document their actual mutable-state
  guarantees and the built-in stores' payload-free transition/compaction
  behavior.

## [1.0.0rc1] - 2026-08-11

### Added

- A single local release runner with `quick`, `performance`, and `rc` profiles,
  durable manifests under `/tmp`, mandatory broker integration, local quality,
  performance and resource gates, short reconnect soaks, and isolated-artifact
  transport smokes. Cross-version and independent-broker validation remains a
  final GitHub matrix; multi-hour campaigns are available separately after the
  first RC.
- An open-loop A/B harness covering calibrated 50/75/90/100% load, MQTT 3.1.1
  and 5, 64-byte and 4 KiB payloads, receipt and callback completion, ABBA
  ordering, latency, CPU, loop-lag, completeness, and EffectPump counters.
- Exact call-count/allocation profiling for the retained micro-scenario
  registry and resource-aware soak snapshots.

### Changed

- Application delivery is owned by the internal `ApplicationDelivery`
  controller. `AsyncClient` delegates iterator/callback queues, byte
  reservations, callback workers, reset/shutdown, and delivery statistics while
  preserving public signatures, defaults, ordering, backpressure, and callback
  exception isolation. Mode-specialised admission removes repeated hot-path
  branches.
- Paired network evaluation now always persists eligibility, policy, thresholds,
  status, failures, and Markdown before exiting. Advisory runs remain visible
  and non-blocking; strict runs return 1 for a measured regression and 2 for an
  invalid runner or worker sample. Subscriber timestamps are collected in a
  separate process, the observer no longer adds a second PUBACK stream, and
  every cell records calibrated and actual sample duration. A final A/A control
  exceeded the noise budget, so closed-loop network results are advisory rather
  than release gates.
- Paired micro workers use a scenario registry instead of one complex dispatch
  function. Benchmark-only dependencies are available through the `benchmark`
  extra, and accidental tracked `.patch`/`.diff` files are rejected.

### Removed

- The obsolete tracked `network-hotpaths-remaining.patch` review artefact.

## [0.2.0b4] - 2026-08-11

### Changed

- `PublishReceipt` no longer builds an `asyncio.Event` per QoS 1/2
  publication. Completion is a flag, and the shared future behind `wait()` is
  created only if something actually waits, so a publication that is never
  awaited allocates no completion primitive at all. Waiters attach through
  `asyncio.shield`, so cancelling one `wait()` cancels only that waiter and
  leaves the receipt and any other waiter intact — the isolation a per-waiter
  `Event` gave implicitly. `wait()` and `is_done()` are unchanged; the private
  `_event` field is replaced by `_future`/`_settled`.
- The Paho façade now installs its inner `on_publish` dispatcher only while
  the user has set `Client.on_publish`, instead of unconditionally at
  construction. Every façade user previously paid a callback-queue hop per
  acknowledged publication to reach a dispatcher that returned immediately,
  and could never satisfy the native client's direct QoS 0 precondition.
  `Client.on_publish` is now a property; reading and assigning it are
  unchanged, including from a non-loop thread.
- Acknowledgement frames without a reason code or properties are emitted
  directly instead of being assembled through the generic encoder, the publish
  encoder no longer re-converts a `QoS` its caller has already validated, topic
  validation no longer builds an encoded form it discards, and the MQTT UTF-8
  rules are checked with string scans rather than a loop over code points. No
  behaviour changes: the same inputs are accepted and rejected, and the only
  visible difference is that a string breaking several UTF-8 rules at once may
  now cite a different one of them.
- `FlowControlError` from the bounded writer now names the bound that refused
  and its configured value, instead of reporting only that a limit was
  reached. `max_outbound_bytes` (1 MiB) and `max_outbound_messages` (10 000)
  imply about 105 bytes per queued message, so the byte bound is the one that
  binds as payloads grow; the defaults are unchanged.
- Documentation is now split by kind and indexed by `docs/README.md`: maintained
  contracts stay directly under `docs/`, while dated measurements, audits and
  campaign records moved to `docs/reports/`. Entries published before this
  reorganisation refer to those reports by their former top-level `docs/` path.

## [0.2.0b3] - 2026-08-09

### Added

- The release workflow now builds wheel and sdist once, smoke-tests those exact
  artifacts across Python 3.11–3.14 plus TCP, TLS, WebSocket, Unix, SQLite,
  Paho migration and clean shutdown, then publishes the same files.
- Installed-distribution smoke coverage now exercises MQTT 3.1.1 and MQTT 5
  over WebSocket and Unix transports, the documented Paho VERSION2 migration
  subset, cancellation, and clean process shutdown. Stable exports and the
  `AsyncClient` constructor/method signatures are locked by regression tests.
- `max_pending_inbound_bytes` now bounds persisted inbound QoS 2 and
  manually-acknowledged QoS 1 application data at 64 MiB by default. Runtime
  statistics expose current, high-water and configured byte values, and SQLite
  schema 4 preserves exact accounting across restarts.

### Fixed

- Packets already in flight while a graceful disconnect is underway no longer
  produce a spurious `ProtocolError`; terminal disconnect handling remains
  authoritative.
- MQTT 5 property encoding now rejects out-of-range Variable Byte Integers and
  oversized binary values with the public `ProtocolError` contract instead of
  leaking low-level `ValueError` exceptions.

## [0.2.0b2] - 2026-08-07

### Changed

- Native QoS 0 publishing now prepares MQTT 3.1.1 and MQTT 5 PUBLISH frames
  once and admits safe single or batched writes directly into the bounded writer.
  Callback and effect-ordering cases keep the established protocol-engine path.
- WebSocket client masking now uses lazy byte-translation tables instead of a
  Python loop per payload byte, while retaining a fresh RFC 6455 mask per frame.

## [0.2.0b1] - 2026-08-06

### Added

- The supported native entry points now expose every type needed by public
  `AsyncClient` signatures: messages, MQTT 5 properties, connection packets,
  subscribe options, negotiated settings, reconnect policy and configuration
  literals. The root package also exposes the operational exception hierarchy
  and `ConnectionState`. `docs/api-stability.md` classifies Stable, Provisional
  and Internal surfaces independently of Python importability or `__all__`.

### Changed

- Inbound delivery byte accounting is now kept outside public `Message`
  instances. One consumer carries the byte count directly; simultaneous
  callback and iterator delivery share a compact two-reference reservation.
  This removes private mutable state from the frozen model, makes each
  `Message` 16 bytes smaller and preserves exact backpressure and queue bounds.

### Fixed

- The Paho compatibility façade now hard-bounds its cross-thread publish
  handoff by request count and logical bytes. Saturation returns
  `MQTT_ERR_QUEUE_SIZE`; reservations are released on admission, cancellation,
  scheduling failure and shutdown, so publisher threads cannot bypass the
  native client's memory guarantees with an unbounded ingress queue.

## [0.1.0a4] - 2026-08-06

### Added

- `TransitionInflightStore`, an optional store extension for atomic, conditional
  and payload-free record transitions (`complete_out`, `transition_out`,
  `contains_in`, `in_meta`, `mark_in_delivered`, `transition_in`, `complete_in`,
  `in_index_pages`, `set_out_logical_size`). `MemoryInflightStore` and
  `SqliteInflightStore` implement it; a store that does not keeps working
  through the existing whole-object path. Acknowledgement handling, the inbound
  duplicate check and delivery marking no longer read a payload back, so a
  PUBACK for a multi-megabyte publication settles on metadata alone.
- Durable schema versioning through `PRAGMA user_version`
  (`SQLITE_SCHEMA_VERSION`). Schema 2 adds a persisted `outbound.logical_size`
  and declares `payload` last, so metadata reads never traverse BLOB overflow
  pages. Databases written by schema 1 are rebuilt in a single transaction on
  open — an interrupted migration reopens as schema 1 rather than as half of
  two — and a database written by a newer MQTTium is refused instead of
  reinterpreted.
- `benchmarks/persistence_index_ab.py`, a rotated A/B of the durable `seq`
  indices over a realistic mixed profile (batched acknowledgement lots,
  reconnect replay, event-loop lag percentiles).
- Decision counters on the two runtime pumps, so their strategies can be
  changed against evidence rather than intuition. `WriterStats` gains `batches`,
  `batched_items`, `batched_bytes`, `segmented_writes` and
  `enqueue_suspensions`; `EffectStats` gains `batches`, `multi_effect_batches`,
  `reordered_batches`, `inline_effects` and `apply_suspensions`.
- `AsyncTransport.stats()`, an optional method a transport may implement to
  report its own buffer occupancy. A transport that does not is reported
  through `TransportStats.unavailable()` instead of being probed attribute by
  attribute from the client.
- `benchmarks/qos1_frame_policy.py` and `docs/QOS1-FRAME-POLICY.md`, retaining
  the allocation/replay A/B that selected the outbound PUBLISH frame policy.
- Seven isolated memory scenarios for the audit's remaining risk paths, with
  exact workload assertions and versioned `tracemalloc` peak thresholds.

### Changed

- Consecutive small QoS 0 `MESSAGE` effects can now be transferred to the bounded iterator/callback queues in one `EffectPump` pass. Single messages, acknowledged QoS, exact byte accounting and full destinations retain the established path; callback execution remains isolated.
- Inbound MQTT 3.1.1 QoS 1 PUBLISH packets now decode their delivery fields directly before entering the shared acknowledgement state machine, avoiding a short-lived intermediate `PublishPacket`; MQTT 5 and QoS 2 retain the generic decoder.
- Inbound MQTT 3.1.1 QoS 0 PUBLISH packets now decode directly into the delivered `Message`, avoiding a short-lived intermediate `PublishPacket`; MQTT 5 and acknowledged QoS paths keep the generic decoder.
- `publish_nowait()` and `publish_many_nowait()` now compute the exact MQTT wire size for bounded-writer admission instead of encoding a disposable preview frame. QoS 1/2 now encode only the real publication after packet-ID allocation.
- QoS 1 and pre-PUBREC QoS 2 records no longer retain contiguous encoded
  PUBLISH frames after the initial SEND; those frames duplicated the payload
  and replay already re-encoded them. Segmented `(header, payload)` items remain
  cached because they share the payload, and replay sets DUP by replacing only
  the small header before reusing the tuple on later reconnects.
- Outbound QoS 2 records are fully compacted after PUBREC: topic, payload and
  PUBLISH properties are removed atomically while the original logical size is
  retained until PUBCOMP releases admission accounting. SQLite schema 3 also
  compacts pre-existing WAIT_PUBCOMP rows during migration.

- Inbound restart redelivery is now incremental and backpressured. Replay
  restores the Receive Maximum window from a payload-free index, then emits
  bounded batches driven by an internal `CONTINUE_INBOUND_REPLAY` effect, so
  delivery backpressure applies *between* batches. Peak allocation during a
  4,000 x 1 KiB session replay dropped from 5.9 MiB to 0.76 MiB. Stores without
  the paging and metadata extensions keep the previous eager behaviour.
- `SqliteInflightStore.batch()` is lazy: `BEGIN IMMEDIATE` is deferred to the
  first mutation, so a read-only ingress lot takes no write lock and pays no
  commit.
- `ClientStats` gains directional `outbound` and `inbound` snapshots,
  each produced by the session that owns the state. The pre-stable `protocol`
  field remains as a deprecated aggregate built from those snapshots so existing
  diagnostic consumers keep working. `AsyncClient.stats()` now only assembles
  owner-produced sections instead of reaching through private session or
  transport attributes.
- `EffectPump` partitions a multi-effect batch SEND-first in a single pass, and
  leaves the list alone when it was already ordered. The two-generator form it
  replaces walked the batch twice and always rebuilt the list. This is what pays
  for the new counters: a batch of eight effects is ~11% faster than before, and
  a QoS 0 publish — which emits SEND plus PUBLISH_COMPLETE, so it takes this
  path — is ~4-5% faster end to end.
- Ordered pagination no longer re-issues `WHERE seq>? ORDER BY seq LIMIT ?` per
  page, which re-scanned and re-sorted the table each time and made a full
  replay quadratic. One sorted metadata pass produces the ordered identifiers,
  then each page is read back by primary key. On 10,000 x 4 KiB records this is
  faster than the indexed page-per-query form while adding nothing to every
  publish: no `seq` index is created, and any left by an earlier build is
  dropped on open. `SqliteInflightStore` pages now match `MemoryInflightStore`
  exactly when records are deleted mid-iteration — the page comes back shorter,
  with insertion order preserved and no duplicate or resurrected record.

### Removed

- `InflightStore.pop_out()`. The library never called it — `get_out()` plus
  `delete_out()` cover every path — so it was surface a third-party store had
  to implement for nothing. `pop_in()` stays; the inbound acknowledgement
  fallback still uses it.
- The `outbound.extra` column, written on every insert and never read.
- The base64-text payload reader. Payloads are always stored as BLOBs, so the
  fallback only existed for a storage format no writer produces.

### Fixed

- MQTT 5 PUBLISH packets without properties decode to an empty `Properties`
  rather than `None`, so the small-message delivery fast path tested identity
  and pushed every property-less v5 message into exact byte accounting.

## [0.1.0a3] - 2026-08-05

### Added

- `AsyncClient.stats()` and immutable `ClientStats` snapshots covering protocol
  admission, effect and writer queues, decoder and transport buffers, delivery
  budgets, receipts, task state and lifetime high-water marks. Statistics are
  maintained without logging or background sampling.
- `max_ingress_batch_bytes` on `AsyncClient`. The reader now drains packets in
  batches bounded by both 256 packets and a byte target, while still allowing
  one individually oversized packet to make progress.
- A reconnect/backpressure soak harness plus pull-request Linux checks and
  manually triggered Linux, macOS, EMQX and HiveMQ campaigns with retained JSON
  measurements.
- A documented public API candidate and stable-release acceptance policy.
- `AsyncClient.publish_nowait()`, a synchronous, non-suspending publication
  method for producers already executing on the client's event loop. It performs
  immediate engine and writer-capacity checks, returns the normal
  `PublishReceipt`, and coalesces asynchronous effect completion without
  creating a publication coroutine.
- `ProtocolEngine.reconfigure()`, the validated configuration boundary used by
  runtime adapters instead of mutating the attached configuration directly.

### Fixed

- `benchmarks/paired_regression.py --scenario` is now honoured in parent mode
  instead of silently running the full scenario matrix.

### Changed

- `OutboundSession` now handles `PUBACK`, `PUBREC` and `PUBCOMP` directly, so
  the component that acquires publication budget, packet identifiers, store
  records and flow slots also releases them through terminal acknowledgement.
- The bounded transport writer has been extracted into `WritePump`, which now
  solely owns queue ordering, byte/count backpressure, batching, the writer task
  and `last_outbound`. `AsyncClient` retains transport lifecycle and writer
  failure policy, while direct-bound enqueue methods preserve the SEND hot path.
- The Paho façade now uses a narrow loop-bound `AsyncClient` adapter boundary
  rather than accessing the protocol engine, receipt registries and effect pump
  directly. Its cross-thread batching is preserved: QoS 0 uses receiptless
  batched admission, while QoS 1/2 receives the authoritative MID and registered
  receipt before releasing the calling thread.
- The native `await publish()` hot path remains inline rather than routing
  through adapter wrappers; paired measurements found the wrapper version
  2.36% slower, while the retained path is performance-neutral against `main`.

## [0.1.0a2] - 2026-08-04

### Added

- Configurable outbound admission limits on `AsyncClient`:
  `max_pending_outbound_messages`, `max_pending_outbound_bytes` and
  `publish_backpressure` (`"wait"` or `"error"`). Admission is checked before a
  packet identifier is allocated or a store record is written, so a refusal
  leaves no state behind.
- A shared inbound delivery byte budget (`max_pending_delivery_bytes`) charged
  once per message and released only when the last consumer drops it, so
  iterator and callback delivery cannot double-count the same payload.
- Bounded failure retention for `publish_many()`: `max_failure_details`
  (default 128) and `failure_sink`, plus `PublishBatchError.failure_count` and
  `PublishBatchError.failure_counts`.
- `PagedInflightStore`, an opt-in protocol extending `InflightStore` with
  `out_pages()`, `out_summary_pages()` and `in_pages()`. The engine uses it to
  hydrate a persistent session without materialising every payload at once, and
  falls back to the eager path for a store that does not implement it. Both
  shipped stores implement it.
- `EngineConfig.update()`, which validates a candidate atomically before
  changing fields and restricts derived-state changes after engine attachment.
- Removed the legacy public `EngineConfig.max_queued` field. Use
  `max_pending_outbound_messages` and `max_pending_outbound_bytes`; `None`
  disables either limit while zero rejects new QoS 1/2 publications.
- Paho façade: `max_queued_messages_set()`, `max_queued_bytes_set()` (no Paho
  equivalent) and `MQTT_ERR_QUEUE_SIZE` (15) for admission refusals.

### Changed

- **Behaviour change.** Outbound admission is bounded by default
  (`max_pending_outbound_messages=10_000`, `max_pending_outbound_bytes=64 MiB`,
  `max_pending_delivery_bytes=64 MiB`). Previously a QoS 1/2 producer could
  queue until the 65 535 packet-identifier space was exhausted. Pass `None` for
  either limit to restore unbounded queueing.
- Connection epochs are attached to every engine effect, so work in flight from
  a dead connection can no longer touch its successor.
- SQLite session hydration reads keyset-paginated pages and a payload-free
  summary projection instead of loading every row eagerly.
- WebSocket `write_many()` flushes in batches bounded by
  `max_write_batch_bytes` (1 MiB); an oversized item is written alone.
- Coalesced Paho-compatible QoS 0/1/2 cross-thread publishing onto bounded
  network-loop batches, with atomic queue-size refusal and cancellation-safe
  MID handoff semantics.
  Drains are capped at 256 requests or 1 MiB of logical topic-plus-payload bytes.
- Paho façade: `wait_for_publish()` and `is_published()` now raise on a
  non-zero return code instead of reporting a publication that never happened.

### Fixed

- `publish_many()` no longer leaks the outbound byte budget when a chunk is
  rolled back on a transactional store. `SqliteInflightStore.batch()` rolls back
  before the engine's recovery path runs, so the per-record sizes were already
  gone and the reserved bytes were never returned. Repeated failed chunks —
  from a rejected admission *or* from any mid-batch validation error — would
  eventually exhaust the 64 MiB default and refuse every QoS 1/2 publish.
- A `publish()` parked on outbound admission capacity is now failed when the
  connection is lost for good, instead of waiting forever. Capacity is only
  released by an acknowledgement, and a parked producer holds no receipt, so
  neither the receipt failure path nor the writer-capacity wake-up could reach
  it. A producer parked while a reconnect is pending still waits, as before.
- Publish/QoS completion effects are emitted before the packet identifier is
  released, and receipts are tracked FIFO per identifier, so an acknowledgement
  for a reused MID can no longer settle a stale receipt.
- Restored Paho-compatible publish throughput, which had regressed to roughly a
  third of Paho's by serialising every submission behind the network loop.

## [0.1.0a1] - 2026-08-03

### Added

- Initial async-native MQTT 3.1.1 and MQTT 5 client spin-out.
- QoS 0/1/2, reconnect, session replay and multiple transports.
- In-memory and SQLite inflight persistence.
- Aggregate `publish_many()` pipeline with bounded memory.
- Additive Paho VERSION2 compatibility façade.
- Standalone Python 3.11–3.14 CI, fuzzing and Mosquitto integration tests.
- Wheel, source-distribution and isolated-install release validation.
- PEP 561 inline typing marker.
- PyPI Trusted Publishing workflow using GitHub OIDC.

### Changed

- Replaced the original Paho-shaped topic matcher with an independent
  flat-filter implementation before publication.
- Adopted PEP 639 license metadata and explicit Apache-2.0 package files.

### Removed

- Pre-spin-out comparative analysis and generated coverage data from the
  published source tree.

[Unreleased]: https://github.com/yoch/mqttium/compare/v1.0.0rc8...HEAD
[1.0.0rc8]: https://github.com/yoch/mqttium/compare/v1.0.0rc7...v1.0.0rc8
[1.0.0rc7]: https://github.com/yoch/mqttium/compare/v1.0.0rc6...v1.0.0rc7
[1.0.0rc6]: https://github.com/yoch/mqttium/compare/v1.0.0rc5...v1.0.0rc6
[1.0.0rc5]: https://github.com/yoch/mqttium/compare/v1.0.0rc4...v1.0.0rc5
[1.0.0rc4]: https://github.com/yoch/mqttium/compare/v1.0.0rc3...v1.0.0rc4
[1.0.0rc3]: https://github.com/yoch/mqttium/compare/v1.0.0rc2...v1.0.0rc3
[1.0.0rc2]: https://github.com/yoch/mqttium/compare/v1.0.0rc1...v1.0.0rc2
[1.0.0rc1]: https://github.com/yoch/mqttium/compare/v0.2.0b4...v1.0.0rc1
[0.2.0b4]: https://github.com/yoch/mqttium/compare/v0.2.0b3...v0.2.0b4
[0.2.0b3]: https://github.com/yoch/mqttium/compare/v0.2.0b2...v0.2.0b3
[0.2.0b2]: https://github.com/yoch/mqttium/compare/v0.2.0b1...v0.2.0b2
[0.2.0b1]: https://github.com/yoch/mqttium/compare/v0.1.0a4...v0.2.0b1
[0.1.0a4]: https://github.com/yoch/mqttium/compare/v0.1.0a3...v0.1.0a4
[0.1.0a3]: https://github.com/yoch/mqttium/compare/v0.1.0a2...v0.1.0a3
[0.1.0a2]: https://github.com/yoch/mqttium/compare/v0.1.0a1...v0.1.0a2
[0.1.0a1]: https://github.com/yoch/mqttium/releases/tag/v0.1.0a1
