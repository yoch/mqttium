# Contributing

Focused contributions and reproducible feedback are welcome. Security
vulnerabilities must follow the private process in [SECURITY.md](SECURITY.md),
not a public issue.

## Development environment

MQTTium uses `pip` and has no runtime dependencies:

```bash
python -m pip install -e ".[dev,fuzz,security,release,docs]"

ruff format --check src tests benchmarks
ruff check src tests benchmarks
mypy src/mqttium
bandit -q -ll -r src
python -m pytest -q tests/unit tests/project
python -m pytest -q tests/unit tests/project --cov=mqttium --cov-branch
mkdocs build --strict
```

Run integration tests against Mosquitto on `127.0.0.1:11883`:

```bash
printf 'listener 11883 127.0.0.1\nallow_anonymous true\npersistence false\n' > /tmp/mosq.conf
mosquitto -c /tmp/mosq.conf -d
MQTTIUM_REQUIRE_BROKER=1 python -m pytest -q tests/integration
```

Without `MQTTIUM_REQUIRE_BROKER=1`, integration tests skip when the broker is
absent. Use the guard above for release evidence. TLS tests also require
OpenSSL on `PATH`.

Fuzzing commands:

```bash
PYTHONPATH=src python tests/fuzz/fuzz.py --seed 1 --iterations 20000
python -m pytest -q tests/fuzz/test_hypothesis_fuzz.py
python -m pytest -q tests/fuzz/test_stateful_invariants.py
PYTHONPATH=src python -m tests.fuzz.runtime_pressure_fuzzer \
  --seed 0 --seeds 32 --steps 36 --require-coverage \
  --artifacts-dir /tmp/mqttium-runtime-pressure-fuzz
```

See the [testing and coverage guide](docs/testing.md) for the suite taxonomy,
resilience commands, coverage semantics, and reliability rules. CI additionally
enforces its configured coverage threshold, validates wheel
and source distributions, installs artifacts in isolation, imports packaged
modules, checks license/type metadata, and rejects tracked cache/build output.

## Before opening a pull request

- Keep the change focused and explain observable behaviour.
- Add a regression test for defects and public contract changes.
- Update current documentation in the same change as behaviour.
- Add user-visible changes under `[Unreleased]` in `CHANGELOG.md`.
- For Stable API changes, update the stability policy and migration guidance.
- For protocol changes, cite the relevant MQTT statement and preserve ownership
  and rollback invariants.
- For performance changes, follow [the benchmarking contract](docs/benchmarking.md)
  and retain valid same-machine controls outside Git.
- Never commit generated benchmark results, coverage databases, caches, build
  output, or local secrets.

## Documentation policy

Active guides and contracts describe current behaviour and change with the
code. Dated files in `docs/reports/` are immutable historical evidence; never
rewrite them to match a newer implementation. Write a new report and update the
curated archive status instead.

Documentation must be English, build with `mkdocs build --strict`, and remain
readable as source Markdown. Public examples should parse and, where practical,
run against an installed artifact.

## Reporting bugs

Use the structured issue form and the complete
[reporting checklist](docs/reporting-issues.md). Include the exact MQTTium,
Python, broker, operating-system, protocol, and transport versions; a minimal
reproducer; the complete exception chain; and redacted runtime snapshots when
available.

Correctness, data loss, unbounded growth, deadlock, security, and clean shutdown
issues take priority over convenience requests.
