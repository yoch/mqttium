# Configuration and sizing

`AsyncClient` starts with bounded defaults suitable for evaluation and many
moderate workloads. Production services should choose limits from payload size,
burst rate, acknowledgement latency, reconnect duration, consumer latency, and
the process memory budget.

## Constructor reference by responsibility

### Identity, protocol, and session

| Setting | Default | Purpose |
| --- | ---: | --- |
| `client_id` | `""` | Requested client identifier; an empty value may be assigned by an MQTT 5 broker |
| `protocol` | MQTT 3.1.1 | Select MQTT 3.1.1 or MQTT 5 |
| `clean_start` | `True` | Request a new broker session rather than resuming one |
| `keepalive` | `60` | Requested keepalive interval in seconds |
| `username`, `password` | `None` | CONNECT credentials |
| `connect_properties` | `None` | MQTT 5 CONNECT properties |
| `will`, `will_properties` | `None` | Last Will message and MQTT 5 properties |
| `store` | `None` | Optional supplied memory or SQLite store |

### Broker-facing protocol limits

| Setting | Default | Purpose |
| --- | ---: | --- |
| `local_receive_maximum` | `100` | Maximum unfinished inbound QoS 1/2 publications advertised to the broker |
| `max_outbound_inflight` | `None` | Optional local cap below the broker's Receive Maximum |
| `maximum_packet_size` | `None` | MQTT 5 inbound maximum packet size advertised to the broker |
| `topic_alias_maximum` | `0` | Inbound topic aliases accepted from an MQTT 5 broker |

`local_receive_maximum` controls inbound work. `max_outbound_inflight` controls
outbound work. Neither changes the MQTT packet-identifier range.

Use dedicated constructor arguments for Receive Maximum, Maximum Packet Size
and Topic Alias Maximum. Supplying any of those names in `connect_properties`
raises `ProtocolError` before creating protocol state. Configuration is fixed
for the client instance.

### Outbound protocol admission

| Setting | Default | Purpose |
| --- | ---: | --- |
| `max_pending_outbound_messages` | `10_000` | Unfinished outbound publications retained by protocol state |
| `max_pending_outbound_bytes` | `64 MiB` | Logical topic, payload, and property bytes retained by outbound state |

### Writer and ingress

| Setting | Default | Purpose |
| --- | ---: | --- |
| `max_outbound_messages` | `10_000` | Encoded frames resident in the writer |
| `max_outbound_bytes` | `1 MiB` | Encoded bytes resident in the writer |
| `max_ingress_batch_bytes` | `1 MiB` | Maximum decoded input work in one bounded batch |
| `max_pending_inbound_bytes` | `64 MiB` | Retained inbound protocol-state bytes |

The writer admits one oversized item when otherwise empty so a configured byte
limit cannot permanently block a valid large packet. No second item is admitted
until capacity returns.

### Application delivery

| Setting | Default | Purpose |
| --- | ---: | --- |
| `message_delivery` | `"iterator"` | Choose iterator or callback delivery |
| `manual_ack` | `False` | Let the application control inbound QoS acknowledgement timing |
| `max_pending_messages` | `65_536` | Iterator queue count bound |
| `max_pending_callbacks` | `1_024` | Callback queue count bound |
| `max_pending_delivery_bytes` | `64 MiB` | Topic, payload and property bytes retained for application delivery |
| `delivery_timeout` | `None` | Optional positive deadline across byte and queue waits |
| `callback_shutdown_timeout` | `5.0` | Callback drain allowance during shutdown |

### Connection and authentication

| Setting | Default | Purpose |
| --- | ---: | --- |
| `reconnect` | disabled | Opt-in `ReconnectPolicy` |
| `ping_timeout` | derived | PINGRESP deadline; derived from keepalive when omitted |
| `ack_timeout` | `30.0` | Default SUBACK and UNSUBACK deadline |
| `auth_handler` | `None` | MQTT 5 enhanced-authentication callback, fixed at construction |
| `auth_timeout` | `10.0` | Deadline for each enhanced-authentication callback invocation |

## A sizing method

1. Record the largest accepted topic, payload, and property set.
2. Estimate the publications that can accumulate during the longest expected
   acknowledgement or reconnect interval.
3. Set both message and byte limits; do not infer bytes from an average payload.
4. Bound delivery from the slowest callback or iterator service time.
5. Load test with the actual broker limits and inspect high-water marks.
6. Leave operational headroom without making overload invisible.

For an outbound burst of `rate × duration`, start with a message bound near that
count and a byte bound based on the high-percentile logical message size. Then
validate with `client.stats().outbound` and `client.stats().writer`; they cover
different queues.

## Wait, refuse, or batch

- Use the default `await client.publish(...)` to propagate pressure naturally.
- Use `publish_nowait()` only
  when the application has an explicit shed, retry, or spill policy.
- Use `publish_many()` to consume large iterables progressively and retain
  aggregate completion without one task per message.
- Avoid immediate retry loops after `FlowControlError`; they can busy-spin while
  no capacity is released.

## Reconnect policy

`ReconnectPolicy` defaults to full-jitter exponential backoff starting at one
second and capped at 60 seconds. Set `max_retries=None` for an unbounded retry
count only when the surrounding service is expected to remain alive. Terminal
authentication, authorization, and protocol errors are not retried.

MQTT 5 `Use another server` and `Server moved` are terminal. The application
chooses any replacement endpoint explicitly. Broker DISCONNECT details arrive
through `on_disconnect` as `BrokerDisconnectError` when there is no more specific
failure; its properties retain the server reference.

## Validate the result

Capture snapshots during normal traffic, a burst, slow-consumer pressure, and a
forced reconnect. After traffic drains, pending counters should return to the
expected idle state. See [Operations and Observability](operations.md) for the
fields and [Writer Backpressure](backpressure.md) for encoded burst
sizing.
