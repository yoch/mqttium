# Contributing

Contributions and reproducible feedback are welcome during the MQTTium beta.
Incorrect behaviour in a published beta should use the structured bug form and
the evidence checklist in [`docs/BETA-REPORTING.md`](docs/BETA-REPORTING.md).
Security vulnerabilities must follow the private process in [`SECURITY.md`](SECURITY.md).

## Development

```bash
python -m pip install -e ".[dev,fuzz,security,release]"

ruff format --check src tests benchmarks
ruff check src tests benchmarks
mypy src/mqttium
bandit -q -ll -r src
python -m pytest -q tests/unit --cov=mqttium --cov-report=term-missing --cov-fail-under=80
python -m pytest -q tests/integration

PYTHONPATH=src python tests/fuzz/fuzz.py --seed 1 --iterations 20000
python -m pytest -q tests/fuzz/test_hypothesis_fuzz.py
```

Integration tests require a local Mosquitto broker on `127.0.0.1:11883` and
**skip silently** when it is absent, so a green run does not prove they ran:

```bash
printf 'listener 11883 127.0.0.1\nallow_anonymous true\npersistence false\n' > /tmp/mosq.conf
mosquitto -c /tmp/mosq.conf -d
```

CI additionally builds the distributions and asserts their contents
(`validate-pyproject`, `python -m build`, `twine check --strict`,
`check-wheel-contents`, wheel metadata and isolated-install checks), and fails
if any cache or build artefact is tracked by git.

## Bug reports

Prefer a minimal complete reproducer against a published beta installed in a
clean virtual environment. Include the MQTTium, Python, broker and operating-
system versions, protocol, transport, relevant client options, complete
traceback and a `ClientStats` snapshot when the owning event loop is responsive.
Redact credentials, private keys, tokens, private addresses and sensitive
payloads. See [`docs/BETA-REPORTING.md`](docs/BETA-REPORTING.md) for failure-
specific evidence and triage criteria.

## Pull requests

Keep changes focused, add tests for observable behavior, and document any API,
protocol-state or persistence invariant that changes. Update `CHANGELOG.md`
under `[Unreleased]` for anything user-visible. Performance changes must include
a comparable benchmark and may not weaken correctness checks; follow the
validity contract in [`docs/BENCHMARKING.md`](docs/BENCHMARKING.md) and never
commit generated numbers.

Documentation is indexed by [`docs/README.md`](docs/README.md) and split in two:
contracts directly under `docs/` describe current behaviour and are updated with
the code, while [`docs/reports/`](docs/reports/README.md) holds dated
measurements and audits that are never revised after the fact. A benchmark
write-up or campaign record belongs in `reports/`.
