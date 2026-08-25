# Fuzzing

MQTTium uses complementary deterministic and property-based fuzz harnesses. They are
reproducible and check
protocol invariants rather than treating every parser exception as a failure.

| Target | Inputs | Main oracle |
| --- | --- | --- |
| `codec` | properties, PUBLISH packets, and mutated typed frames | only documented parse errors escape |
| `engine` | stateful connection, acknowledgement, inbound/outbound alias, enhanced AUTH, reconnect, replay, and manual-ack sequences | bounded flow, canonical durable topics, consistent packet identifiers and byte/message ledgers |
| `websocket` | lengths, control frames, and fragmentation | bounded buffering and deterministic rejection |
| replay interleavings | page hydration followed by PUBREL, delivery handoff, close, continuation, and reconnect | Memory/SQLite equivalence; no completed, delivered, or stale-epoch emission |
| `runtime` | external MQTT events plus explicit releases at writer, lifecycle, and callback boundaries | owner accounting, effect settlement, epoch isolation, and terminal liveness |
| `runtime-composition` | legal history plus two simultaneously open MQTTium ownership windows | V1 oracles plus release-order, takeover, and cross-generation ownership |
| `runtime-pressure` | transport capability choices, producer bursts, payload classes, admission saturation, and one lifecycle owner at a time | exact wire obligations, pressure high water, parked-producer settlement, and observed pressure/lifecycle overlap |

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

# Real AsyncClient runtime schedules (the PR smoke uses 12 seeds)
PYTHONPATH=src python tests/fuzz/runtime_fuzzer.py \
  --seed 0 --seeds 12 --steps 24 \
  --artifacts-dir /tmp/mqttium-runtime-fuzz

# Two-window lifecycle composition (V2)
PYTHONPATH=src python -m tests.fuzz.runtime_composition_fuzzer \
  --seed 0 --seeds 24 --steps 48 \
  --artifacts-dir /tmp/mqttium-runtime-composition-fuzz

# Pressure/interleaving profile with mandatory surface coverage (V3)
PYTHONPATH=src python -m tests.fuzz.runtime_pressure_fuzzer \
  --seed 0 --seeds 32 --steps 36 --require-coverage \
  --artifacts-dir /tmp/mqttium-runtime-pressure-fuzz
```

## Runtime schedule target

`tests/fuzz/runtime_fuzzer.py` is a small MQTTium-specific schedule fuzzer, not
an asyncio simulator. Each seed builds a short operation history and runs it
through the real `AsyncClient`, `WritePump`, `EffectPump`, and
`ApplicationDelivery`. Its only fake is a packet-aware transport/broker with an
ingress queue and explicit gates for transport writes/close, reconnect factory,
callbacks, and test-wrapped effect application. There are no production
scheduling hooks and no changes to runtime hot paths.

The original three ownership motifs remain fixed regression anchors:

- writer admission while an active write fails and the reader is admitting a
  mandatory PUBACK;
- callback self-cancellation while the callback worker owns later delivery;
- terminal EOF followed by an explicit replacement connection.

A state-aware grammar now spends every `--steps` entry. From the current model
state it chooses only legal outbound/inbound actions or release decisions, then
returns to a connected boundary before entering one of three additional motif
families:

- automatic reconnect with successful, failed, or blocked factories, including
  disconnect and explicit takeover while the factory owns the lifecycle lock;
- blocked callback delivery, release ordering, callback disconnect/connect,
  and cancellation of an application lifecycle operation;
- blocked EffectPump application, concurrent drains, injected failure, and an
  effect collected while the failing close is deliberately held.

Seeds also choose QoS 0/1 inbound and outbound work, writer/callback gate
episodes, and bounded event-loop yields before those boundaries. Invalid API
operations are not generated and `--steps` is an exact exploration budget.
After each operation the target checks exact writer resident/byte accounting,
effect progress, outbound flow/packet-id/receipt agreement, inbound Receive
Maximum, callback-worker ownership, completed-write epochs, application task
outcomes, and event-loop exception contexts. A two-second whole-schedule
watchdog catches liveness failures outside checkpoint polling. Terminal
checkpoints additionally require every connection-scoped waiter, queued byte,
effect, and receipt to settle. A checkpoint that cannot be reached is reported
as a liveness failure; it is never treated as a successful timeout.

### Failure artifacts and replay

Every failing seed can write `runtime-seed<seed>.json` with schema
`mqttium-runtime-fuzz-v1`. It contains the seed, ordered operations, reached
checkpoints, mutation name when qualification is active, owner snapshots,
attempted and completed packet history per transport generation, and the
exception or invariant. The same `--seed` and `--steps` values reconstruct the
schedule exactly. Histories
are intentionally short; deletion-based shrinking is deferred until the
target produces enough real findings to justify it.

### Qualification by mutation

`tests/fuzz/test_runtime_fuzzer.py` mechanically breaks six runtime contracts
inside the test harness only:

- writer failure advances its epoch without waking admission waiters;
- connection setup/teardown does not invalidate the epoch;
- an applied effect does not advance its settlement target;
- callback self-cancellation terminates the callback worker;
- an effect collected during the failing-close window is not settled;
- an automatically established connection defeats an already-waiting explicit
  user takeover.

A 24-seed qualification campaign must detect each class in at least the seeds
that exercise its ownership boundary. The unmodified target is also run twice
for the same seed and must produce identical operations and final owner state.
Campaign output reports seeds, failures, unique full operation traces, unique
scheduling/release traces, and coverage counts for every operation/checkpoint.

## Runtime composition target

`tests/fuzz/runtime_composition_fuzzer.py` is a bounded V2 extension over the
unchanged V1 harness. It still runs the real `AsyncClient` and event loop. A
state-valid connected history is followed by one of six ownership pairs:

- callback × reconnect factory;
- old writer × replacement generation;
- deferred effect × reconnect;
- callback × reader/transport teardown;
- callback × writer;
- effect × writer.

Only the existing test transport, callback, EffectPump, reconnect-factory, and
transport-close seams are gated. Generated state records the identity and count
of open windows, rejects a release without a corresponding owner, never permits
more than two simultaneous windows, and ends with every gate settled. Release
permutations and their bounded intervening yields are first-class operations.
The V2 artifact schema adds the pair, release trace, per-step window depth, and
composition owner snapshot to every V1 failure field.

Four behavioral mutations qualify the composed space: a cancelled old write
completes after replacement, a retired callback loses explicit takeover to an
automatic reconnect, a deferred effect is replayed into a replacement
generation, and a closing transport is mistaken for a healthy generation when
a callback connects. Each mutation is inert outside its relevant pair. V1
generation, nightly rotation, and oracles are unchanged.

## Runtime pressure target

`tests/fuzz/runtime_pressure_fuzzer.py` is the bounded V3 profile over the V1
harness. Eleven seed-selected families vary `write_nowait` and `write_many`
presence, deterministic eager refusal, 4- and 16-frame writer residency,
one-turn producer bursts, tiny/batching/segmented payloads, and 0/1/2/4-turn
settlement. Three dedicated families plus the reconnect-overlap family place a
real application publisher behind saturated outbound admission and qualify ACK
release, cancellation, terminal teardown, and reconnect ownership transition.

Four more families compose pressure separately with reader teardown, reconnect
factory, callback worker, and EffectPump ownership. An overlap counts only
while both owners are observable at the same checkpoint. Campaign coverage is
mandatory by default and requires a `write_many` call carrying at least four
PUBLISH frames, rather than mono-frame capability use or an unrelated control
batch. Eight test-only mutations or negative controls
qualify eager accept/refusal, latency batching, vectored coalescing, segmented
writes, parked-publisher wake/accounting, writer parking, and all four lifecycle
overlaps. Failure artifacts use schema `mqttium-runtime-fuzz-v3` and retain the
profile, family, settlement plan, pressure high water, and owner snapshot.

### Campaign levels

1. **PR smoke:** 12 fixed healthy V1 seeds at 24 steps, 12 fixed healthy V2
   seeds at 48 steps, 32 coverage-gated V3 seeds at 36 steps, bounded pytest
   qualification of all six V1, four V2, and eight V3 mutations, plus the
   stateful invariant campaign at 6 seeds × 200 steps.
2. **ARM64 nightly:** 50,000 seeds at 32 steps, split into ten disjoint
   5,000-seed shards. The workflow run number advances a monotonic seed range
   from 2,000,000; rerunning the same workflow run deliberately replays the same
   range.
   This permanent safety net runs V1 only; V2/V3 remain PR smoke and explicit
   qualification targets until their campaign rotation is chosen.
3. **Long/manual release proposal:** at least 1,000,000 seeds at 48 steps across
   disjoint recorded ranges, with artifacts retained outside the repository.
   Do not add schedule shrinking or a second broker model merely to consume the
   budget.

The permanent nightly runs only from trusted `main` on the serialized ARM64
self-hosted runner; it is not a pull-request check. Its 30-day artifact contains
the exact SHA and environment, ten recorded ranges, per-family and per-operation
coverage, trace diversity, CPU/wall/RSS summaries, and every JSON failure with
campaign context. Healthy seeds emit only shard and campaign summaries. The V1
long campaign completed 1,000,000 schedules at 48 steps with zero failures and
99.98% unique scheduling traces; V1 is now a stable safety net rather than an
area for further grammar expansion.

The V3 long campaign completed 1,000,000 schedules at 48 steps on x86-64 with
zero failures, all mandatory pressure counters nonzero, and 99.9736% unique
scheduling traces. V2 retains a clean 50,000-seed calibration; its recommended
million-seed two-window campaign remains pending. See the dated reports for the
exact commits, environments, ranges, and limitations.

The local release runner can orchestrate multiple deterministic shards:

```bash
python benchmarks/fuzz_campaign.py \
  --shards 5 \
  --duration-minutes 288 \
  --output /tmp/mqttium-fuzz
```

The seed for every shard and batch is written to `metadata.json` and
`campaign.log`. Failure inputs are stored as binary artefacts, so a failure can
be replayed without relying on the original machine.

The engine seed also reproduces API-side operations that have no wire corpus,
including outbound Topic Alias replacement/reuse and enhanced AUTH exchanges.
The stateful pytest campaign additionally compares Memory and SQLite after each
replay interleaving:

```bash
MQTTIUM_FUZZ_SEEDS=24 MQTTIUM_FUZZ_STEPS=500 \
  python -m pytest tests/fuzz/test_stateful_invariants.py -q
```

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

Runtime failures use JSON rather than binary input because the generated value
is an ordered schedule and owner snapshot, not a packet corpus.

Long campaigns are release evidence, not a requirement for every development
iteration. Their outputs belong under `/tmp` or another external artefact
directory and must not be committed to the source tree.
