# Threat model

This page states what MQTTium defends against, what it leaves to the
application, and where a defence is not yet backed by a test. It complements
[Behavioural guarantees](guarantees.md), which is about liveness and bounds
under honest peers, and the security policy in `SECURITY.md`.

It uses the same discipline as the guarantee register: each threat has a
status and its evidence, and `tests/project/test_guarantee_register.py` checks
that the evidence exists.

| Status | Meaning |
| --- | --- |
| **Checked** | A test asserts the defence, and its failure fails CI |
| **Partial** | The defence holds under the stated condition only, or part of it is untested |
| **Open** | No defence, or none backed by a test; the residual risk is the application's |

## Scope

MQTTium is a client library running inside the application's process. It has
no privileges of its own and no network listener.

| Boundary | Trusted? |
| --- | --- |
| The application, its callbacks and its configuration | Yes. Code in the process can do anything the library can. |
| The broker | No. A broker may be faulty, misconfigured or hostile. |
| The network path | No, unless TLS with verification is used. |
| The SQLite store file and its directory | Only as far as file permissions hold. |
| The published distributions | Verifiable; see `SECURITY.md`. |

Out of scope: broker-side authorization, payload contents, credential storage
by the application, and side channels of the Python runtime.

## Hostile or faulty broker

### T1 — Malformed input crashes the client or escapes as an untyped error

**Status:** Checked.

Every packet is validated before the engine acts on it. Malformed, oversized
or out-of-state input ends the connection with a typed `MQTTError`, and under
MQTT 5 a DISCONNECT with the matching reason code. Fuzzers assert that only
that error family escapes.

- `tests/resilience/test_hostile_broker.py`
- `tests/unit/test_packet_strictness.py`
- `tests/unit/test_fuzz_codec.py`
- `tests/fuzz/test_hypothesis_fuzz.py`
- `tests/fuzz/test_stateful_invariants.py`

### T2 — A length header makes the client reserve memory

**Status:** Checked.

A packet larger than the configured maximum is refused when its length is
decoded, before its body arrives. The receive buffer grows with the bytes
received, never from the announced length.

- `tests/resilience/test_decoder_header_budget.py`
- `tests/unit/test_decoder_ingress_hardening.py`
- `tests/unit/test_decoder_storage_pressure.py`
- `tests/unit/test_websocket_client_receive_limit.py`

### T3 — A broker sends more data than the application can hold

**Status:** Partial.

With MQTT 5 the client announces a 16 MiB Maximum Packet Size and a Receive
Maximum of 100, and ends the connection when either is exceeded. The
`messages()` queue is bounded and a full queue is bounded in time (G3).

*Residual risk, by default configuration:*

- **MQTT 3.1.1 has no way to announce a packet limit.** Without an explicit
  `maximum_packet_size`, one inbound packet may be as large as the protocol
  allows (256 MiB), and it is buffered as it arrives. Set
  `maximum_packet_size` when the broker is not trusted.
- `max_inbound_inflight_bytes` is unset by default: the bytes held by
  unfinished inbound QoS exchanges are bounded only by the packet and Receive
  Maximum limits multiplied together.
- With MQTT 3.1.1 the default inbound window is 65 535 exchanges.

- `tests/unit/test_connect_effective_inbound_limits.py`
- `tests/unit/test_real_world_limits.py`
- `tests/unit/test_inbound_byte_budget.py`
- `tests/unit/test_property_decode_budget.py`
- `tests/unit/test_inbound_alias_validation.py`
- `tests/unit/test_iterator_consumer_deadlock.py`

### T4 — A broker stalls the client instead of failing it

**Status:** Partial.

A broker that stops answering is detected by keep-alive and ends pending
waits; see G2 to G5 for the bounds and their preconditions. A broker that
stays connected, answers pings and never acknowledges holds a QoS receipt
until the application's own timeout.

- `tests/resilience/test_hostile_broker.py`
- `tests/unit/test_keepalive_real_world.py`
- `tests/unit/test_call_deadlines.py`

### T5 — A broker redirects the client or drives a reconnect storm

**Status:** Checked.

A Server Reference is exposed to the application and never followed.
Refusals the broker marks as permanent are not retried. Other failures are
retried with exponential backoff and jitter, capped at 60 seconds by default.

*Residual risk:* a `ReconnectPolicy` configured with zero delays retries as
fast as connections fail.

- `tests/unit/test_broker_disconnect_diagnostics.py`
- `tests/unit/test_permanent_reconnect_failures.py`
- `tests/unit/test_reconnect_retry_loop.py`
- `tests/unit/test_negotiation.py`

### T6 — Broker-supplied text is trusted by the application

**Status:** Open.

Reason strings, user properties, topics and a WebSocket handshake status line
reach the application unmodified, in messages, exception attributes and
`stats().last_disconnect_error`. Only invalid UTF-8 and U+0000 are refused;
other control characters, including line breaks, pass through. An application
that writes them to a log or a terminal must escape them itself.

## Network attacker

### T7 — Interception or impersonation of the broker

**Status:** Partial.

Plain TCP and `ws://` give no confidentiality and no authentication, and they
are what `connect()` uses unless `ssl` is passed. With `ssl=True` or `wss://`
the standard library's default context applies: certificate and hostname
verification, TLS 1.2 or later. MQTTium builds no context of its own and does
not weaken one.

*Residual risk:* a caller-supplied `SSLContext` with verification disabled is
used as given. The rejection tests pass an explicit default context; the
`ssl=True` shorthand has no test of its own.

- `tests/unit/test_tls.py`
- `tests/unit/test_websocket_endpoint_validation.py`

### T8 — A peer that vanishes without closing the connection

**Status:** Partial.

MQTT keep-alive detects it within `keepalive` plus `ping_timeout`. With
`keepalive=0`, TCP keep-alive remains, on TCP transports only (about 90
seconds).

- `tests/unit/test_keepalive.py`
- `tests/unit/test_tcp_socket_keepalive.py`

### T9 — A WebSocket endpoint that is not an MQTT broker

**Status:** Partial.

The handshake response is bounded to 64 KiB under a timeout; the accept key,
the upgrade headers and the subprotocol are validated; frames are bounded
before they are buffered; masking keys come from the operating system's
random source. Credentials in a URL and line breaks in extra headers are
refused.

*Residual risk:* an accept-key mismatch, missing upgrade headers and the
handshake size limit are enforced in the code but have no test.

- `tests/unit/test_websocket.py`
- `tests/unit/test_websocket_lifecycle.py`
- `tests/unit/test_websocket_client_receive_limit.py`

## Local store

### T10 — Another local user reads or alters persisted messages

**Status:** Partial.

A database created by `SqliteInflightStore` is created with mode `0600`, in a
directory created with mode `0700` when it did not exist; its WAL files
inherit that mode. SQL statements are parameterised.

*Residual risk:* topics, properties and payloads are stored in clear. An
existing file keeps the permissions it had. Rows are type-checked when read,
not authenticated: a process able to write the file can make the client
replay a publication it never accepted. Credentials are never written to the
store.

- `tests/unit/test_sqlite_private_files.py`
- `tests/unit/test_sqlite_schema_validation.py`
- `tests/unit/test_sqlite_corruption_detection.py`

### T11 — Another connection holds the database lock

**Status:** Partial.

See G13: a refused lock while receiving drops the packet unapplied and the
connection is retried; a refused `publish()` fails that call.

- `tests/unit/test_sqlite_lock_contention.py`

## Secrets in the process

### T12 — Credentials appear in logs, errors or representations

**Status:** Open.

The library writes no log and prints nothing. No exception message includes
a username, a password or authentication data, and `stats()` carries no
credential. The engine configuration keeps the password and the will out of
its representation.

*Residual risk, untested:* two Internal objects still show secrets in their
`repr()`, which a debugger or a traceback that prints local variables would
expose: the transient `ConnectPacket` shows the password, and the engine
configuration shows `connect_properties`, including `authentication_data`.
A `Message` representation includes its payload by design.

## Supply chain

### T13 — A dependency is compromised

**Status:** Checked.

MQTTium has no runtime dependency, and a test fails if the package metadata
declares one.

- `tests/project/test_release_metadata.py`

### T14 — A published artifact is not the one that was built and tested

**Status:** Partial.

One build produces the distributions; the same files are validated, smoke
tested and published through PyPI Trusted Publishing. PyPI serves a signed
publish attestation naming this repository and `publish.yml`; `SECURITY.md`
gives the command that verifies it. Actions are pinned by commit and
workflows are audited in CI.

*Residual risk:* the build is not bit-for-bit reproducible, so the evidence
is the attestation, not an independent rebuild. The attestation's presence is
checked by hand after a release, not by a test. There is no SBOM, no static
analysis beyond `bandit`, and no OpenSSF Scorecard; none was judged worth its
upkeep for a package without dependencies at this stage.

- `tests/project/test_release_metadata.py`
