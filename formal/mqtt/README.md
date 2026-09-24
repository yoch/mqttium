# Formal MQTT/runtime models

Small TLA+ models used to audit concrete MQTTium ownership boundaries.

Current model on this branch:

- `ReconnectCancellation.tla` — distinguishes cancellation requested on the
  automatic reconnect task from dependency-originated `CancelledError` (#510).

These models supplement executable regressions and bounded state exploration.
A committed TLA+ file is not, by itself, a claim that TLC was executed.
