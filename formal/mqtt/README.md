# Formal MQTT/runtime models

Small TLA+ models used to audit concrete MQTTium ownership boundaries.

Current model on this branch:

- `WritePumpCancellation.tla` — distinguishes lifecycle task cancellation from
  dependency-originated `CancelledError` in the writer (#509).

The models supplement executable regressions and state exploration. A committed
TLA+ file is not, by itself, a claim that TLC was executed.
