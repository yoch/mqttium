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

A supplied `store` only resumes the session it holds when `clean_start=False`.
With the default `clean_start=True` the broker discards its side of the session
and the client discards the store's unfinished publications at CONNACK; the
store then only provides durability within one process lifetime. Choose the
pair deliberately: `store` + `clean_start=False` for restart recovery,
`clean_start=True` when a fresh session is intended.

### MQTT 5 options on an MQTT 3.1.1 client

`connect_properties`, `will_properties`, `topic_alias_maximum` and
`auth_handler` describe MQTT 5 features. Passing any of them with the default
MQTT 3.1.1 protocol raises `ProtocolError` from the constructor: there is no
wire representation to degrade to, so the client refuses rather than ignores.
`maximum_packet_size` is protocol-agnostic; see below.

### Broker-facing protocol limits

| Setting | Default | Purpose |
| --- | ---: | --- |
| `max_inbound_inflight` | `100` | Concurrent inbound QoS 1/2 exchanges accepted; advertised as Receive Maximum on MQTT 5 |
| `max_outbound_inflight` | `None` | Optional local cap below the broker's Receive Maximum |
| `maximum_packet_size` | `None` | Largest inbound packet accepted by the decoder; advertised to an MQTT 5 broker |
| `topic_alias_maximum` | `0` | Inbound topic aliases accepted from an MQTT 5 broker |

`max_inbound_inflight` controls inbound work. `max_outbound_inflight` controls
outbound work. Neither changes the MQTT packet-identifier range.

Use dedicated constructor arguments for Receive Maximum, Maximum Packet Size
and Topic Alias Maximum. Supplying any of those names in `connect_properties`
raises `ProtocolError` before creating protocol state. Configuration is fixed
for the client instance.

### Outbound protocol admission

| Setting | Default | Purpose |
| --- | ---: | --- |
| `max_unacknowledged_messages` | `10_000` | Outbound QoS 1/2 publications admitted and not yet completed, including those waiting for an inflight slot |
| `max_unacknowledged_bytes` | `64 MiB` | Logical topic, payload, and property bytes of those publications |

Both bounds refuse new admissions with `FlowControlError` (or park an awaiting
`publish()` until capacity returns); they never disconnect.

### Writer and inbound protocol state

| Setting | Default | Purpose |
| --- | ---: | --- |
| `max_write_queue_messages` | `10_000` | Encoded frames resident in the writer |
| `max_write_queue_bytes` | `1 MiB` | Encoded bytes resident in the writer |
| `max_inbound_inflight_bytes` | `64 MiB` | Logical bytes retained for inbound QoS 1/2 exchanges |

The writer admits one oversized item when otherwise empty so a configured byte
limit cannot permanently block a valid large packet. No second item is admitted
until capacity returns.

The inbound bounds have a different failure mode from every outbound bound:
the client cannot refuse a PUBLISH the broker has already sent. When the
broker exceeds `max_inbound_inflight` the client sends DISCONNECT with reason
`0x93` (Receive Maximum exceeded); when a retained QoS 1/2 exchange would
exceed `max_inbound_inflight_bytes` it sends DISCONNECT with reason `0x97`
(Quota exceeded). Both end the connection and surface through
`on_disconnect`; a reconnect policy may retry. On MQTT 3.1.1 the broker is not
told either limit, so size `max_inbound_inflight` at or above the broker's
own inflight window when using `manual_ack` with slow acknowledgement.

The reader decodes input in fixed lots of at most 256 packets or 1 MiB before
handing effects to the application; that quantum is a fairness constant, not
a memory bound, and is not configurable.

### Application delivery

| Setting | Default | Purpose |
| --- | ---: | --- |
| `message_delivery` | `"iterator"` | Choose iterator or callback delivery |
| `manual_ack` | `False` | Let the application control inbound QoS acknowledgement timing; iterator delivery only |
| `max_iterator_messages` | `65_536` | Iterator queue count bound |
| `max_iterator_bytes` | `64 MiB` | Topic, payload and property bytes retained in the iterator queue |
| `iterator_admission_timeout` | `None` | Optional positive deadline for admitting one message into the iterator queue |

The iterator queue is the only place where the client retains messages on the
application's behalf, so its three bounds only exist in iterator mode. In
callback mode the reader hands each message to the synchronous callback and
retains nothing; backpressure is the callback's own duration. Passing a
non-default iterator bound with `message_delivery="callback"` raises
`ValueError` at construction, and so does `manual_ack=True`: callbacks are
synchronous and acknowledge automatically, while `ack()` is awaited from the
asynchronous `messages()` consumer.

### Connection and authentication

| Setting | Default | Purpose |
| --- | ---: | --- |
| `reconnect` | `None` | Opt-in `ReconnectPolicy`; `None` disables reconnection |
| `connect_timeout` | `30.0` | Transport and CONNACK deadline for `connect*()` when the call omits `timeout`, and for every automatic reconnect attempt |
| `ping_timeout` | derived | PINGRESP deadline; derived from keepalive when omitted |
| `subscribe_timeout` | `30.0` | Default SUBACK and UNSUBACK deadline |
| `auth_handler` | `None` | MQTT 5 enhanced-authentication callback, fixed at construction |
| `auth_timeout` | `10.0` | Deadline for each enhanced-authentication callback invocation |

Every default deadline lives on the constructor; `connect*()`, `subscribe()`
and `unsubscribe()` accept a per-call `timeout` override. `ReconnectPolicy`
only describes the retry progression.

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
second and capped at 60 seconds. Passing a policy enables reconnection;
`reconnect=None` disables it. Set `max_retries=None` for an unbounded retry
count only when the surrounding service is expected to remain alive. Terminal
authentication, authorization, and protocol errors are not retried. Each
attempt uses the client's `connect_timeout`.

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
