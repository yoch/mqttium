# Logging and observability

## Decision: no logging inside the library

MQTTium does not emit logs, and the standard `logging` module is absent from
`src/`. This is deliberate.

Useful MQTT logs tend to sit on the publish, message, and acknowledgement paths.
Even a disabled `isEnabledFor(DEBUG)` guard measured about 1.65% per publication
in the retained paired microbenchmark, and every additional log point compounds
that cost. Enabled per-message logging is much more expensive and can expose
topics, payloads, credentials, or other application data.

Keeping logging outside the library also avoids global handler configuration and
leaves each application in control of its own privacy and sampling policy.

## Observability without background work

MQTTium exposes state when the application asks for it. Unused observability has
no sampler, logging, or formatting cost.

| Need | API |
| --- | --- |
| Publish completion or failure | `await receipt.wait()` and `receipt.is_done()` |
| Connection lifecycle | `on_connect` and `on_disconnect` |
| Incoming messages | `on_message` or `async for message in client.messages()` |
| Current state | `client.is_connected`, `client.state`, `client.negotiated` |
| Protocol failures | typed exceptions such as `ProtocolError` and `MQTTTimeoutError` |
| Broker disconnect details | `DisconnectInfo` |
| Queue and resource pressure | `client.stats()` |

## Add application-level instrumentation

A small wrapper can add the metrics and logs an application actually needs:

```python
import logging
import time

from mqttium.api import AsyncClient


log = logging.getLogger("myapp.mqtt")


class ObservedClient:
    def __init__(self, client: AsyncClient) -> None:
        self._client = client
        self.published = 0
        self.errors = 0
        self.last_publish_latency = 0.0
        client.on_disconnect = self._on_disconnect

    async def publish(self, topic, payload, **kwargs):
        started = time.monotonic()
        try:
            receipt = await self._client.publish(topic, payload, **kwargs)
            self.published += 1
            return receipt
        except Exception:
            self.errors += 1
            log.warning("MQTT publish failed for topic=%s", topic)
            raise
        finally:
            self.last_publish_latency = time.monotonic() - started

    def _on_disconnect(self, error) -> None:
        log.info("MQTT disconnected: %r", error)

    def __getattr__(self, name):
        return getattr(self._client, name)
```

Prefer counters and sampled timings over one formatted string per message. Log
state transitions and failures that are meaningful to the application, and
redact topic or payload data where required.

## Future instrumentation

If the library ever needs an event hook, it must remain optional, avoid global
logging, and add no formatting on the inactive path. Any proposal must include
a paired benchmark demonstrating less than 0.5% disabled overhead.
