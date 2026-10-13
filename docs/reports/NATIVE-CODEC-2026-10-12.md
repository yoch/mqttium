# Complete compiled codec — 2026-10-12

## Decision and scope

Implement one optional `mqttium.codec._native` extension through the existing
Hatch build. The complete wire codec is generated from maintained Python
sources, with 89 lines of bounded Cython kernels for the hottest operations.
This replaces the first study's isolated, mechanically compiled module variants
with an integrated backend. The Python implementation remains the reference and
can be selected even when the extension is installed.

On this host, the native backend reduces PUBLISH encoding CPU per operation by
53–56%, complete/fragmented framing by 33–44%, and QoS 0 `publish_nowait` admission
by 25%. The network matrix observes 5–13% lower client CPU per delivered message;
its smaller differences need qualification on a quieter machine. These are
same-client implementation comparisons, not public cross-client or release
qualification results.

## Implementation

- Nineteen codec/topic/packet modules bind their definitions to one compiled
  extension. All MQTT 3.1.1/5 packet families, properties, topic validation,
  primitives, VBI and incremental framing are included. The connection-level
  binding table, protocol engine, clients, persistence and matcher remain Python.
- Build-time AST generation retains the existing protocol logic. Symbol
  collisions and changed decoder copy boundaries fail the build. There is no
  second handwritten MQTT implementation.
- Typed decoder state and bounded integer/string readers remove interpreter
  work. Frame bodies are copied directly into owned bytes in one allocation;
  no reusable-buffer view reaches the client.
- PUBLISH headers and small payloads are assembled into one exact-sized bytes
  allocation after the existing validation. Payloads at least 128 KiB retain
  the existing segmented-write path and reuse the payload bytes.
- Primitive and assembly fast paths require the expected exact input types;
  uncommon inputs retain the compiled generic reference path. Bounds checks
  precede pointer reads and copies. No blanket disabling of bounds checks or
  GIL release is used.
- `MQTTIUM_BUILD_CODEC=1` requests the native wheel. Cython/setuptools are
  build-only dependencies; pure builds need neither. `MQTTIUM_CODEC_BACKEND`
  selects `auto`, `python` or `native` before import. Only an absent extension
  permits automatic fallback; broken imports remain visible.

## Artifacts and method

Source is the working-tree implementation based on
`af7004bec68259bc48b187fb1858b859f3173ecf`, not a new release. Both timing arms
unpack exactly the same native wheel and select their backend explicitly.
Every worker checks imported paths; native workers additionally verify that
all exported definitions are bound to the extension.

- CPython 3.12.14, Linux x86-64, AMD EPYC 7763 virtual CPU, GCC 14.2.0.
- Development Cython 3.2.4; isolated native build requires `Cython>=3.2.4,<4`.
- CPUs 0–4 available with a cgroup budget of four CPU equivalents. Client
  workers are pinned to CPU 0; dedicated Mosquitto is pinned to CPU 1 with
  `set_tcp_nodelay true`, bound to `127.0.0.1:11883`.
- Governor unavailable on this managed host, so release eligibility is not
  established. No concurrent builds or test suites ran during measurements.
- Fresh processes, `PYTHONHASHSEED=0`, warmup, six paired samples per case,
  alternating A/B and B/A order. Microbenchmarks use `--scale 2`; calibrated
  framing/property/VBI samples target about 0.5 seconds or more.
- CPU cost is process CPU time divided by completed operations. This measures
  work per message, not a fixed percentage of a saturated CPU. Savings are
  `100 × (1 − 1 / median(paired Python/native CPU ratios))`; throughput changes
  use the median paired wall-time ratio. The range is the observed paired
  range, not a confidence interval. Arm medians are shown separately.
- Network cases use 24,000 messages, 256-byte payloads, callback delivery,
  bursts of 32 and an inflight limit of 20. One client publishes and receives
  its own subscription. Sequence, payload, receipt completion and empty final
  protocol backlog are verified. Timing excludes connection setup and broker
  CPU, and includes the inbound QoS 2 acknowledgement tail.
- Microbenchmarks reuse the existing paired scenarios. Encode cases use a
  small JSON payload. The in-memory QoS 1 cycle uses 4 KiB and a synthetic ACK;
  ingress cases start from an already-framed packet. `publish_nowait` uses a
  discard writer, so it measures admission rather than network delivery.

Wheel: `mqttium-1.2.1-cp312-cp312-linux_x86_64.whl`.

Wheel SHA-256: `d61f3d29ac92368bc0aec35e425fa99fa110d7440dc324a10742fa89e1a6b2f4`.

Generated codec source digest: `34f30e4a85744b56f8f3b914ff23983f6c61b4bdf6855adf49bb49bf388b26f1`.

The native wheel is about 2.72 MiB; its unstripped extension is about 7.57 MiB.
The pure wheel is about 197 KiB. Binary artifacts are kept outside the repository.

## Local CPU measurements

| Scenario | Python CPU µs/op | Native CPU µs/op | CPU reduction | CPU speedup range | Throughput change |
| --- | ---: | ---: | ---: | ---: | ---: |
| `encode_qos0` | 1.264 | 0.593 | 53.3% | 2.018–2.160× | +114.3% |
| `encode_qos1_v5` | 1.495 | 0.655 | 55.9% | 2.204–2.284× | +126.9% |
| `ingress_engine_qos0` | 2.123 | 1.766 | 16.9% | 1.173–1.546× | +20.4% |
| `ingress_engine_qos0_v5` | 2.456 | 2.119 | 14.0% | 1.128–1.341× | +16.2% |
| `ingress_publish_qos1` | 3.576 | 3.165 | 11.1% | 1.112–1.197× | +12.5% |
| `qos1_cycle_memory` | 18.260 | 12.128 | 32.3% | 1.454–1.548× | +47.8% |
| `native_publish_nowait_qos0` | 3.102 | 2.345 | 25.1% | 1.285–1.364× | +33.5% |
| `dispatch_match_exact` | 0.288 | 0.281 | 1.9% | 0.993–1.037× | +1.9% |
| `dispatch_match_100` | 1.021 | 1.014 | 0.6% | 0.988–1.014× | +0.6% |
| `frame_64` | 0.887 | 0.515 | 42.2% | 1.698–1.889× | +73.1% |
| `frame_4096` | 1.450 | 0.957 | 33.1% | 1.451–1.602× | +49.4% |
| `frame_fragmented` | 4.797 | 2.720 | 43.5% | 1.710–1.790× | +77.0% |
| `properties_v5` | 7.504 | 6.338 | 15.9% | 1.176–1.203× | +18.9% |
| `vbi_four` | 0.652 | 0.426 | 35.0% | 1.464–1.550× | +53.8% |

The matcher is an unchanged control: its sub-2% apparent differences are noise.
The largest gains come from typed framing and allocation reduction in PUBLISH
assembly. Compared with the initial mechanically compiled experiment, complete
4 KiB framing now improves instead of regressing.

## Loopback measurements and controls

| Scenario | Python CPU µs/op | Native CPU µs/op | CPU reduction | CPU speedup range | Throughput change |
| --- | ---: | ---: | ---: | ---: | ---: |
| `network_311_qos0` | 20.239 | 17.546 | 13.2% | 1.044–1.297× | +15.0% |
| `network_311_qos1` | 50.557 | 46.936 | 6.9% | 0.971–1.128× | +6.6% |
| `network_311_qos2` | 80.734 | 76.334 | 5.4% | 1.023–1.095× | +5.7% |
| `network_5_qos0` | 21.256 | 18.452 | 12.0% | 1.104–1.206× | +14.0% |
| `network_5_qos1` | 52.454 | 45.757 | 12.8% | 1.077–1.258× | +14.8% |
| `network_5_qos2` | 83.941 | 76.745 | 8.4% | 1.004–1.126× | +9.0% |

A/A controls compare Python with itself using the same wheel. The first micro
controls (four pairs) reported CPU savings of +1.0% for encoding, −0.1% for
engine ingress and −1.2% for 4 KiB framing. An initial 12,000-message MQTT 5/QoS 1
control showed −6.2% apparent savings, exposing network variability. The control
was therefore extended to all six cells using the same 24,000-message count as
A/B, with four pairs per cell:

| Python/Python control | Apparent CPU reduction | CPU speedup range |
| --- | ---: | ---: |
| `network_311_qos0` | -0.6% | 0.967–1.000× |
| `network_311_qos1` | -0.5% | 0.941–1.002× |
| `network_311_qos2` | +2.6% | 1.011–1.064× |
| `network_5_qos0` | +5.0% | 0.992–1.106× |
| `network_5_qos1` | -1.1% | 0.985–1.010× |
| `network_5_qos2` | -0.0% | 0.994–1.009× |

The A/A matrix reaches +5.0% median apparent savings for MQTT 5/QoS 0; one pair
reaches 1.106×. MQTT 3.1.1/QoS 1 has an A/B pair below 1.0, and QoS 2's smaller
median gain is close to the control variation. Do not treat every network cell
as a proven gain. MQTT 5/QoS 1 has the clearest network separation in this run:
A/B ratios 1.077–1.258 versus A/A 0.985–1.010. Recheck representative fixed-rate
traffic, TLS/WebSocket, SQLite, ARM64 and other Python versions before making
platform-wide performance claims. No CPU baseline correction or subtraction
was applied to the reported A/B numbers.

## Correctness and distribution validation

- Native unit/project suite: 2,601 passed.
- Python unit/project suite: 2,599 passed, two native-specific tests skipped;
  branch-inclusive coverage 93.82%, above the 89% requirement.
- The differential oracle compares 31,364 normalized results: bytes, decoded
  values, exception types/messages, invalid offsets and types, all VBI size
  boundaries, MQTT 3.1.1/5 control packets, and fragmented PUBLISH frames around
  the segmented-write threshold. The complete existing suite runs against the
  native wheel as well; this corpus supplements it.
- Native deterministic fuzz: 20,000 iterations each for codec, engine and
  WebSocket, with zero crashes or invariant failures. Hypothesis/stateful:
  52 passed. Resilience: 27 passed. Installed native Mosquitto integration:
  22 passed, including subprocess examples.
- Instrumentation tests now inspect the backend owning implementation globals.
  Topic handoff counts real string encodes. Property retention checks weak
  references while retaining the exception, preserving the memory guarantee
  without requiring Cython frames to expose Python locals.
- Pure wheel/sdist built in isolation; native wheel rebuilt from that sdist in
  isolation. Metadata/content checks and Twine checks pass, with native platform
  tags and unchanged empty runtime dependencies. A clean virtual environment
  imports and exercises the native wheel without Cython installed.
- Ruff, mypy, Bandit and strict MkDocs checks pass. A blocking Linux native CI
  matrix for Python 3.11–3.15 is added; only CPython 3.12 was executed locally.
  CI execution on the other versions remains pending.

## Reproduction and retained evidence

See [Compiled codec](../native-codec.md) for installation and backend selection.
The benchmark harness is [compiled_modules.py](../../benchmarks/compiled_modules.py).
The earlier [feasibility report](COMPILED-SUBMODULES-2026-10-12.md) remains unchanged.

```bash
# On this host the interpreter's configured Clang was absent, so use GCC.
MQTTIUM_BUILD_CODEC=0 python -m build --outdir /tmp/codec-pure-dist
CC=gcc LDSHARED='gcc -shared' MQTTIUM_BUILD_CODEC=1 python -m pip wheel --no-deps --wheel-dir /tmp/codec-native-dist /tmp/codec-pure-dist/mqttium-1.2.1.tar.gz
python benchmarks/compiled_modules.py prepare-wheel --root /tmp/codec-study --wheel /tmp/codec-native-dist/mqttium-1.2.1-cp312-cp312-linux_x86_64.whl
python benchmarks/compiled_modules.py compare --root /tmp/codec-study --variant native --repeat 6 --scale 2 --output /tmp/codec-micro.json
python benchmarks/compiled_modules.py compare --root /tmp/codec-study --variant native --repeat 6 --count 24000 --port 11883 --cases network_311_qos0,network_311_qos1,network_311_qos2,network_5_qos0,network_5_qos1,network_5_qos2 --output /tmp/codec-network.json
# Repeat the network command with --variant python --repeat 4 for A/A.
```

Local raw files (not committed):

- `/workspace/cloud-setup/codec-aa.json`: initial controls.
- `/workspace/cloud-setup/codec-micro.json`: all local operation pairs.
- `/workspace/cloud-setup/codec-network.json`: six-cell network A/B matrix.
- `/workspace/cloud-setup/codec-network-aa.json`: matching-count network controls.
- `/workspace/cloud-setup/native-codec-study/manifest.json`: exact wheel identity.
- `/workspace/cloud-setup/codec-sdist-native-dist/`: built native wheel.
- `/workspace/cloud-setup/codec-sdist-final-tests.log`: exact sdist-built wheel suite.
- `/workspace/cloud-setup/codec-python-final-tests.log`: Python suite and coverage.
- `/workspace/cloud-setup/codec-native-integration-final.log`: installed integration.
- `/workspace/cloud-setup/codec-native-fuzz.log` and `codec-native-hypothesis.log`:
  fuzz results.

Raw benchmark JSON includes all paired timings, imported paths, native source
digest, wheel hash and benchmark-harness hash. The report preserves compact
results; retain those external raw artifacts alongside it for future audits.
