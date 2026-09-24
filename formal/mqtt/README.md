# Formal MQTT/runtime models

Small TLA+ models used to audit concrete MQTTium ownership boundaries.

Current model on this branch:

- `LifecycleReconnectHook.tla` — separates lifecycle-hook serialization from
  automatic transport-reconnect progress so a disconnect hook may await durable
  MQTT work without blocking the connection needed to complete it (#508).

The models supplement executable regressions and bounded state exploration.
A committed TLA+ file is not, by itself, a claim that TLC was executed.
