# Paho compatibility notes

The VERSION2 compatibility implementation lives in `mqttium.compat.paho`. It
is intended as an adoption path for existing synchronous Paho applications;
new async code should use `mqttium.api.AsyncClient`.

- Start with the runnable
  [`examples/paho_compat.py`](../../../examples/paho_compat.py).
- Follow the staged [`migration guide`](../../../docs/MIGRATION.md).
- Check the exact [`compatibility matrix`](../../../docs/COMPAT.md), including
  intentional differences and unsupported behaviour.
