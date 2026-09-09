# Custom transport receive capabilities

`mqttium.transport` is a **Provisional** integration surface. The receive contract is now split into a common transport contract plus one explicit receive capability.

Use these canonical imports:

```python
from mqttium.transport import AsyncTransport, DecoderPushTransport, PullTransport
```

`AsyncTransport` describes the common write/lifecycle surface. A custom transport used by `AsyncClient` must additionally implement exactly one receive style:

- **`PullTransport`** — implement `async read(n: int = 65536) -> bytes`. MQTTium reads bytes and feeds them into its incremental decoder. Existing custom transports written for the previous `AsyncTransport.read()` contract continue to use this mode without a runtime migration.
- **`DecoderPushTransport`** — implement `attach_decoder(decoder)` and `async receive() -> bool`. The transport receives directly into the decoder-owned writable storage; `receive()` reports a new receive generation and returns `False` on clean EOF. It must not invent a byte-stream `read()` notification.

The protocols are `runtime_checkable`; `AsyncClient` detects the receive mode structurally. MQTTium's built-in direct selector TCP transport satisfies `DecoderPushTransport`; TLS, Unix, WebSocket and ordinary stream fallbacks satisfy `PullTransport`.

A custom transport should implement **one receive capability, not both**. Implementing neither is rejected when the reader starts. The current client gives the push capability precedence if an object accidentally satisfies both, but dual-mode transports are outside the supported Provisional contract and should not rely on that precedence.

## Migration from the previous Provisional contract

Before this revision, the public `AsyncTransport` protocol included `read()`. The method has moved to the public `PullTransport` capability so that decoder-push transports are not forced to expose a false byte-stream API.

For an existing custom transport that already implements `read()`, no behavioral change is required: keep the method and treat the object as `AsyncTransport` + `PullTransport`. Code that uses the protocols for static typing should import `PullTransport` explicitly when it needs the receive method.

New direct-ingress integrations should implement `AsyncTransport` + `DecoderPushTransport` and use the decoder passed to `attach_decoder()` as the owner of receive storage. The decoder/framing API is itself Provisional; do not retain writable views across callbacks or expose them to application code.

Because this surface is Provisional, future compatible refinements may add capability methods, but incompatible changes remain subject to a changelog entry and migration guidance under the project API-stability policy.
