# Reporting issues

A useful report lets another person reproduce, classify, and regression-test a
problem without reconstructing the environment through multiple follow-ups.
Use the structured GitHub bug form for incorrect behaviour. Report security
vulnerabilities privately through the process in
[SECURITY.md](https://github.com/yoch/mqttium/blob/main/SECURITY.md).

## Before reporting

Confirm which installed distribution is running:

```bash
python - <<'PY'
import importlib.metadata
import mqttium

print("package:", importlib.metadata.version("mqttium"))
print("runtime:", mqttium.__version__)
print("loaded from:", mqttium.__file__)
PY
```

Reproduce with the latest applicable release in a clean virtual environment.
For unreleased development, include the exact commit SHA and state that the
checkout is editable.

Never post usernames, passwords, tokens, private keys, private broker addresses,
or sensitive topics and payloads.

## Minimal reproducer

Include a complete executable program with:

- explicit `AsyncClient` options;
- the exact connect, subscribe, publish, acknowledgement, and shutdown order;
- bounded timeouts so a stall is visible;
- the smallest topic and payload that still fail;
- a `finally` block that disconnects or records why shutdown failed.

For Paho compatibility, include `CallbackAPIVersion.VERSION2`, loop ownership,
callback signatures, and whether publication originates inside or outside a
callback.

## Environment

Record:

- MQTTium and Python versions;
- operating system and architecture;
- broker product, version, and relevant listener settings;
- MQTT 3.1.1 or MQTT 5;
- TCP, TLS, WebSocket, or Unix transport;
- exact exception chain and broker-side error;
- whether the failure is deterministic or intermittent.

## Runtime snapshot

Call `client.stats()` on the owning event loop when it remains responsive:

```python
from dataclasses import asdict
import json

print(json.dumps(asdict(client.stats()), indent=2, default=str))
```

Redact identifiers and application data. For a reconnect or drain problem,
snapshots before and after the transition are more useful than one final value.

## Failure-specific evidence

### Connection or authentication

Include TLS settings excluding secrets, CONNACK/AUTH information, server
reference handling, and whether plain TCP reaches the same broker.

### Flow control or stalled publication

Include outbound and writer limits, backpressure mode, QoS, payload size, and
the outbound, writer, effect, receipt, and reconnect snapshot sections.

### Delivery or acknowledgement

Include delivery mode, manual acknowledgement, callback sync/async shape, queue
limits, last observed event, and shutdown outcome.

### Persistence or session replay

Include clean-start/session-expiry settings, client identifier, broker
`session_present`, store implementation, restart boundary, and the last known
protocol transition.

## Maintainer-ready report

An issue is ready for implementation when it has:

1. a published version or exact commit;
2. a minimal reproducer or retained failing artifact;
3. a classified surface and failure phase;
4. the expected invariant;
5. enough evidence to place a regression test.

Correctness, data loss, unbounded growth, deadlock, security, and clean shutdown
take priority over convenience requests. Performance reports must follow the
same-machine evidence requirements in the [Benchmarking Contract](benchmarking.md).
