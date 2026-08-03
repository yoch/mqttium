# Contributing

Contributions are welcome while MQTTium is being prepared for its first public
package release.

## Development

```bash
python -m pip install -e ".[dev]"
ruff format --check src tests benchmarks
ruff check src tests benchmarks
mypy src/mqttium
python -m pytest -q tests/unit
```

Integration tests require a local Mosquitto broker on the port configured by
the test suite.

## Pull requests

Keep changes focused, add tests for observable behavior, and document any API,
protocol-state or persistence invariant that changes. Performance changes must
include a comparable benchmark and may not weaken correctness checks.
