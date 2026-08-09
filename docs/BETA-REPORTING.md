# Beta issue reporting and triage

MQTTium beta reports should make it possible to reproduce, classify and test a
fix without first reconstructing the reporter's environment. Use the structured
GitHub bug form for incorrect behaviour. Security vulnerabilities must use the
private process in [`SECURITY.md`](../SECURITY.md), not a public issue.

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

Try the latest published beta in a clean virtual environment. A report from an
editable checkout is still useful when it concerns unreleased development, but
state that explicitly and include the commit SHA.

Reduce the failure to one client operation and one broker where practical.
Include the exact Python, operating-system, broker, MQTT protocol and transport
versions. Never post usernames, passwords, tokens, private keys, private broker
addresses or sensitive payloads.

## Minimal reproducer

A useful reproducer is a complete executable program with:

- explicit `AsyncClient` construction options;
- the exact `connect`, subscribe, publish, acknowledgement and shutdown order;
- bounded timeouts so a stall becomes visible;
- the smallest payload and topic shape that still fails;
- a `finally` block that disconnects or records why shutdown could not finish.

For Paho compatibility reports, include the callback API version, loop method,
callback signatures and whether publication originates inside or outside a
callback.

## Runtime snapshot

`client.stats()` is intended to be called from the client's owning event loop.
When the loop is still responsive, capture a snapshot close to the failure:

```python
from dataclasses import asdict
import json

print(json.dumps(asdict(client.stats()), indent=2, default=str))
```

Review the output before posting and redact client identifiers, topics or other
application-specific values. For a reconnect or drain problem, snapshots from
both before and after the transition are more useful than one final snapshot.

## What to include by failure class

### Connection and authentication

Include the protocol version, transport, TLS settings excluding secrets, broker
listener configuration, the returned CONNACK or AUTH information when
available, and the complete exception chain. State whether a plain TCP
connection to the same broker succeeds.

### Flow control and stalled publication

Include the configured outbound message/byte limits, writer limits,
`publish_backpressure`, QoS, payload size and the outbound, writer, effect and
receipt sections of `ClientStats`. State whether the call waited, raised
`FlowControlError`, timed out or was cancelled.

### Delivery, callbacks and acknowledgement

Include `message_delivery`, `manual_ack`, callback sync/async shape, queue limits
and whether the same message is also consumed through `messages()`. State which
callback or iterator event was last observed and whether shutdown completed.

### Reconnect and session replay

Include `ReconnectPolicy`, clean-start/session settings, the broker-side reason
for the disconnect, QoS, persistence store, pending receipt state and snapshots
before disconnect and after reconnect. Do not increase timeouts merely to hide
a stalled queue; preserve the first reproducible failure.

### Persistence

Include the store implementation, database lifetime, whether the failure occurs
before or after a process restart, the last successful protocol transition and
a minimal database created by the reproducer when it contains no sensitive
application data.

## Maintainer triage

A beta issue is ready for implementation when it has:

1. a published version or exact commit;
2. a minimal reproducer or a retained failing test artifact;
3. a classified surface and failure phase;
4. an expected invariant;
5. a proposed regression test location.

Correctness, data-loss, unbounded-growth, deadlock and clean-shutdown failures
take priority over convenience requests. Performance-only reports belong to the
separate performance program and must include same-machine comparable evidence.
