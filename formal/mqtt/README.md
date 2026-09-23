# Formal MQTT/runtime models

This directory contains small, auditable TLA+ models used to review specific
MQTTium ownership boundaries. They supplement executable regression, fuzz and
integration tests; the presence of a model does not by itself imply that TLC
was run.

Current model:

- `ActiveDeliveryGeneration.tla` — iterator slow-admission ownership across
  connection epochs and application stream generations (#500).
