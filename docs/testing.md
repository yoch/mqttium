# Testing and coverage

MQTTium separates fast correctness checks from tests that need sockets, a live
broker, longer schedules, or generated inputs. Run the smallest relevant layer
while developing, then run every affected layer before requesting review.

## Test layers

| Directory | Purpose |
| --- | --- |
| `tests/unit` | Deterministic behavior without real network sockets |
| `tests/project` | Public API, documentation, packaging, release, and repository contracts |
| `tests/resilience` | Loopback hostile-peer, race, timing, shutdown, and leak scenarios |
| `tests/integration` | Behavior against a live Mosquitto broker |
| `tests/fuzz` | Seeded fuzzing, Hypothesis properties, and state-machine invariants |

`benchmarks/runtime_soak.py` is a longer lifecycle search, not a unit-test
replacement. See [Runtime soak and quiescence](runtime-soak.md).

Installed-distribution smoke programs are release assets. They validate the
built wheel and source distribution rather than the source checkout.

## Local commands

Install all contributor tooling with:

```bash
python -m pip install -e ".[dev,fuzz,security,release,docs]"
```

Run the deterministic suite and its branch-inclusive coverage gate:

```bash
python -m pytest -q tests/unit tests/project
python -m pytest -q tests/unit tests/project --cov=mqttium --cov-branch
```

Run resilience and fuzz checks separately:

```bash
python -m pytest -q tests/resilience
PYTHONPATH=src python tests/fuzz/fuzz.py --seed 1 --iterations 20000
python -m pytest -q \
  tests/fuzz/test_hypothesis_fuzz.py \
  tests/fuzz/test_stateful_invariants.py
```

Integration tests expect Mosquitto on `127.0.0.1:11883`. Set the same guard as
CI so an unavailable broker or any skipped integration case fails the run:

```bash
printf 'listener 11883 127.0.0.1\nallow_anonymous true\npersistence false\n' > /tmp/mosq.conf
mosquitto -c /tmp/mosq.conf -d
MQTTIUM_REQUIRE_BROKER=1 python -m pytest -q tests/integration
```

TLS tests also require OpenSSL on `PATH`.

## Coverage semantics

The authoritative repository gate is coverage.py's branch-inclusive total for
`tests/unit` and `tests/project`. It combines executed statements and branch
destinations and must remain at or above **89.00%**. The threshold is defined in
`pyproject.toml` and enforced directly by pytest-cov in CI.

Codecov receives the same branch-inclusive XML report for review and historical
visibility. Its headline percentage uses different semantics: a line with only
some branch destinations executed is reported as partial rather than fully
covered. The Codecov badge can therefore be numerically lower than the
coverage.py total even though both consume the same run.

To avoid pretending these percentages are interchangeable, Codecov's project
status compares each change with its base commit (`target: auto`) and remains
informational. Patch coverage has an informational 90% target. Failure to
produce or upload the report still fails CI; only coverage.py owns the absolute
89.00% release gate.

## Reliability rules

- CI treats warnings, unknown markers, invalid pytest configuration, and tests
  exceeding 30 seconds as failures.
- Integration runs require a broker and permit no silent skips.
- Failed tests are never rerun automatically.
- Race campaigns use explicit seeds so failures can be reproduced.
- Throughput thresholds belong in benchmark or scheduled diagnostic campaigns,
  not correctness tests.
- A discovered engine defect needs a regression test and direct fix, or a
  tracked issue when it cannot be resolved in the current change.
