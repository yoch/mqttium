# Transports and security

MQTTium supports TCP, TLS, WebSocket, and Unix-domain sockets. Transport choice
does not change publish receipts or QoS semantics.

## TCP

```python
await client.connect("broker.example", 1883, timeout=10)
```

Plain TCP provides no confidentiality or peer authentication. Use it only on a
trusted network or inside another authenticated tunnel.

## TLS

Use Python's normal `SSLContext` so certificate authorities, client
certificates, hostname checking, and minimum TLS versions remain explicit:

```python
import ssl

context = ssl.create_default_context(cafile="broker-ca.pem")
context.minimum_version = ssl.TLSVersion.TLSv1_2
context.load_cert_chain("client-cert.pem", "client-key.pem")

await client.connect(
    "broker.example",
    8883,
    ssl=context,
    timeout=10,
)
```

Passing `ssl=True` creates Python's default client context. Prefer an explicit
context when the deployment has a private CA, mutual TLS, or a defined TLS
policy.

Do not disable hostname or certificate verification to make a failing setup
connect. Confirm the hostname, trust roots, system time, certificate validity,
and broker listener first.

## MQTT over WebSocket

```python
await client.connect_ws(
    "wss://broker.example/mqtt",
    ssl=context,
    extra_headers={"X-Deployment": "gateway-a"},
    timeout=10,
)
```

The transport uses RFC 6455 binary frames and requests the MQTT subprotocol.
Use `wss://` outside a trusted local environment. Extra headers are visible to
the WebSocket endpoint; do not place long-lived secrets in source code or logs.

A WebSocket URL must include a hostname; there is no implicit `localhost`
fallback. The `ssl` option accepts only `None`, a `bool`, or an `SSLContext`.
For `wss://`, `None` and `True` enable Python's default TLS context, an explicit
context is preserved, and `False` is refused. Other values, including `0` and
an empty string from dynamic configuration, raise `ValueError`. Invalid URLs
and TLS options are rejected before opening a socket or sending extra headers.
Plain `ws://` retains its explicit `ssl=False` behavior.

## Unix-domain sockets

```python
await client.connect_unix("/run/mosquitto/mosquitto.sock", timeout=10)
```

Unix sockets are local to a compatible operating system. Protect the socket
path with filesystem ownership and permissions; there is no TLS layer between
local processes.

## Credentials

Pass `username` and `password` to `AsyncClient` for MQTT CONNECT credentials.
Keep secrets in the application's secret provider and avoid serialising the
client configuration or raw CONNECT packet.

MQTT credentials do not encrypt traffic. Combine them with TLS whenever the
network is not already confidential and authenticated.

## MQTT 5 enhanced authentication

Enhanced authentication uses `auth_handler` and `await client.auth(...)`. The
application owns the authentication method, challenge processing, credential
storage, and redaction policy. See [MQTT 5](mqtt-5.md).

## Timeouts and failure handling

A connect timeout covers transport setup and CONNACK; `connect_timeout` on
the client applies to explicit calls that omit `timeout` and to every
automatic reconnect attempt. Treat certificate failures, broker
authorization failures, and malformed protocol traffic as terminal until the
configuration changes; repeatedly retrying them adds load without improving
availability.

Automatic reconnect stops on `ssl.SSLCertVerificationError`, malformed MQTT
packets, and peer protocol violations, including failures before CONNACK or
during the reconnect stability window. Pending work fails with the terminal
cause and the application message stream ends. Certificate setup failures
are reported through `on_disconnect` even when no new reader was started.
A later explicit connection can begin a new stream after the endpoint or
trust configuration is repaired; these peer/security failures alone do not
make the client permanently unusable.

Ordinary connection resets, connection refusal, timeouts, and TLS EOF remain
eligible for the existing retry policy and backoff. A valid negative CONNACK
still follows its protocol-specific reason-code policy: transient server
busy/unavailable responses are not confused with malformed peer traffic just
because the refused connection is exposed as `ProtocolError`.

MQTTium intentionally does not log credentials, topics, properties, or payloads.
See [Logging and Observability](observability.md) for application-owned diagnostics.
