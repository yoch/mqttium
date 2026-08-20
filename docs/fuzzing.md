# Fuzzing

MQTTium uses two complementary fuzz harnesses. Both are reproducible and check
protocol invariants rather than treating every parser exception as a failure.

| Target | Inputs | Main oracle |
| --- | --- | --- |
| `codec` | properties, PUBLISH packets, and mutated typed frames | only documented parse errors escape |
| `engine` | stateful connection, acknowledgement, alias, reconnect, and manual-ack sequences | bounded flow, consistent packet identifiers, non-negative counters |
| `websocket` | lengths, control frames, and fragmentation | bounded buffering and deterministic rejection |

`tests/fuzz/fuzz.py` is dependency-free and driven by an explicit seed.
`tests/fuzz/test_hypothesis_fuzz.py` adds property-based generation, shrinking,
and a persistent example database.

## Run locally

```bash
# Fast smoke included in the unit suite
python -m pytest tests/unit/test_fuzz_smoke.py -q

# Deterministic campaign
python tests/fuzz/fuzz.py --seed 1 --iterations 20000

# Hypothesis with the default profile
python -m pytest tests/fuzz/test_hypothesis_fuzz.py -q

# More aggressive local search
HYPOTHESIS_PROFILE=aggressive \
  python -m pytest tests/fuzz/test_hypothesis_fuzz.py -q
```

The local release runner can orchestrate multiple deterministic shards:

```bash
python benchmarks/fuzz_campaign.py \
  --shards 5 \
  --duration-minutes 288 \
  --output-dir /tmp/mqttium-fuzz
```

The seed for every shard and batch is written to `metadata.json` and
`campaign.log`. Failure inputs are stored as binary artefacts, so a failure can
be replayed without relying on the original machine.

## Failure output

The deterministic fuzzer writes parseable progress to standard error:

```text
[START] target=engine seed=1 iterations=20000
[PROGRESS] target=engine iter=2001/20000 rate=87590/s elapsed=0.0s
[FAIL] target=engine iter=50 kind=crash seed=7 elapsed=0.00s
[ARTIFACT] tests/fuzz/artifacts/engine-seed7-iter50.bin
[DONE] target=engine status=FAIL iters=20000 crashes=1 elapsed=0.1s
```

Use `--progress-every` to change reporting frequency, `--artifacts-dir` to
choose where failing inputs are written, and `--quiet` to suppress live progress.
An invariant violation or crash returns exit code 1.

Long campaigns are release evidence, not a requirement for every development
iteration. Their outputs belong under `/tmp` or another external artefact
directory and must not be committed to the source tree.
