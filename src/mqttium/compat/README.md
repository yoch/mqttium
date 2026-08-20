# Paho compatibility notes

The VERSION2 compatibility implementation lives in `mqttium.compat.paho`. It
is intended as an adoption path for existing synchronous Paho applications;
new async code should use `mqttium.api.AsyncClient`.

- Start with the runnable
  [`examples/paho_compat.py`](../../../examples/paho_compat.py).
- Follow the staged [`migration guide`](../../../docs/migration.md).
- Check the exact [`compatibility matrix`](../../../docs/paho-compatibility.md), including
  intentional differences and unsupported behaviour.
