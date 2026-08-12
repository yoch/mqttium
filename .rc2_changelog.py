from pathlib import Path

path = Path("CHANGELOG.md")
text = path.read_text()
start = text.index("## [Unreleased]")
end = text.index("## [1.0.0rc1]")

release = """## [Unreleased]

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
  emitting an illegal packet.
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
  executable checks linked from `docs/CONFORMANCE.md`.

### Documentation

- The cross-client QoS 1 latency claim from the 2026-08-09 external benchmark is
  retracted in the reports index. The apparent 2.95x gap came from pacing each
  client at a fraction of its own calibrated capacity rather than comparing
  matched absolute load; the dated source report remains unchanged as a
  historical record.
- `InflightStore.update_out()` / `update_in()` document their actual mutable-state
  guarantees and the built-in stores' payload-free transition/compaction
  behavior.

"""

text = text[:start] + release + text[end:]
old = "[Unreleased]: https://github.com/yoch/mqttium/compare/v1.0.0rc1...HEAD\n"
new = (
    "[Unreleased]: https://github.com/yoch/mqttium/compare/v1.0.0rc2...HEAD\n"
    "[1.0.0rc2]: https://github.com/yoch/mqttium/compare/v1.0.0rc1...v1.0.0rc2\n"
)
if text.count(old) != 1:
    raise SystemExit(f"comparison link count={text.count(old)}")
text = text.replace(old, new, 1)
path.write_text(text)
