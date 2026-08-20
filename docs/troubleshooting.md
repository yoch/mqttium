# Troubleshooting

Start with the exception, connection state, broker response, and two runtime
snapshots taken at different times. A single queue high-water mark proves only
that a burst occurred.

## The integration test passed but no broker was running

Repository integration tests skip when Mosquitto is absent. Start the listener
described in [Contributing](https://github.com/yoch/mqttium/blob/main/CONTRIBUTING.md)
and confirm that tests were collected and executed rather than skipped.

## `publish()` waits

The default policy waits for protocol or writer capacity. Inspect:

- `stats().outbound` for unfinished QoS state and broker flow limits;
- `stats().writer` for encoded write pressure;
- `stats().delivery` for a slow callback or iterator;
- `stats().tasks` and `reconnect_attempt` for active recovery.

If waiting is unacceptable, choose `publish_backpressure="error"` and implement
a shed, retry, or spill policy. Do not simply remove the bounds.

## `publish_nowait()` raises `FlowControlError`

Immediate refusal is its contract. Large payloads usually exhaust the writer
byte budget before the message count. Wait for progress, reduce the offered
rate, spill work elsewhere, or use `await publish()`.

An immediate retry loop can busy-spin on the same full queue.

## A receipt does not complete

Check the QoS and the expected completion packet. For QoS 1/2, a receipt may
remain pending across an active reconnect while MQTTium determines whether the
broker session can resume. Apply a business deadline with `asyncio.timeout()`;
cancelling only the waiter does not cancel the protocol exchange.

Capture the negotiated Receive Maximum, pending outbound state, packet IDs,
writer state, reconnect state, and broker logs.

## TLS fails

Verify the broker hostname, listener port, trust root, certificate validity,
system time, and client certificate requirements. Test with an explicit
`SSLContext`. Do not disable verification as a permanent workaround.

## WebSocket fails during handshake

Confirm the `ws://` or `wss://` URL, path, proxy routing, MQTT subprotocol, TLS
identity, and broker WebSocket listener. A normal MQTT TCP port is not
automatically a WebSocket endpoint.

## Messages are not delivered

With `message_delivery="auto"`, assigning `on_message` selects callback
delivery; otherwise the iterator is used. With `"both"`, both consumers retain
capacity. Check callback exceptions through the event loop exception handler
and inspect delivery queue counts and bytes.

With `manual_ack=True`, call `await client.ack(message)` after durable
application processing. Duplicate delivery remains possible after failure and
must be handled by the application.

## Reconnect stops

Automatic reconnect is opt-in. It stops after `max_retries` and for terminal
authentication, authorization, and protocol errors. Inspect the final CONNACK
or DISCONNECT reason before increasing retry counts.

## A durable session does not resume

Verify all of the following:

1. the same client identifier is used;
2. clean start is disabled;
3. the broker session has not expired or been deleted;
4. the broker reports `session_present`;
5. the application reopened the intended inflight store;
6. the stored schema is supported.

Client persistence cannot recreate broker-owned subscriptions or queued
messages after the broker discards its session.

## Shutdown hangs or loses work

Keep disconnect in `finally`, stop new producers first, allow receipts to settle
according to the service deadline, then disconnect and close the application-
owned store. Include callback shutdown time and queued writer data in that
deadline.

## Requesting help

Follow [Reporting Issues](reporting-issues.md). Include an exact version or
commit, minimal reproducer, broker and Python versions, protocol, transport,
configuration, exception chain, and redacted snapshots. Never post credentials,
keys, private addresses, or sensitive payloads.
