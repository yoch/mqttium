# Compiled submodules feasibility study — 2026-10-12

Compiling packet codecs with Cython saves about 19–21% CPU on isolated encoding,
but the full client's benefit is much smaller: the clearest network result is
about 5% less CPU on MQTT 5 QoS 1. Whole-module compilation regresses complete
4-KiB framing and exact-filter matching. Retain a narrow optional-codec prototype;
the evidence does not justify generalizing compilation or enabling it by default.

## Scope and source

This is a local feasibility experiment against MQTTium commit
`af7004bec68259bc48b187fb1858b859f3173ecf`. It does not change runtime source,
dependencies, imports, or release packaging. The reproducible experiment is
[`benchmarks/compiled_modules.py`](../../benchmarks/compiled_modules.py).

The question is how much CPU can be saved by compiling a small, synchronous
part of the existing implementation, before considering a separately maintained
C/Rust implementation or changing protocol/session ownership.

## Candidate inventory

| Candidate | Reason to consider it | Scope / priority |
| --- | --- | --- |
| `codec.primitives`, `codec.vbi` | Per-packet Python calls, offset arithmetic, variable-length integer loops | Measure first, but one-byte VBI is already cached and `struct`, UTF-8 conversion and substring searches already execute in C |
| `packets._publish`, `packets._ack`, `packets._common` | Encoding headers and decoding topic/payload fields are on every active connection's data path | High priority; keep version-specific paths and owned-byte boundaries |
| `codec.buffer` | Framing, fragmented input and many small packets repeatedly traverse Python methods | High priority; preserve slab growth limits, ingress window rules and copies at packet boundaries |
| `codec.properties`, `codec.packet_validation`, `topics` | MQTT 5 parsing, validation and repeated property dispatch | Measure property-rich packets separately from the already optimized empty-property case |
| `dispatch.matcher` | Python trie traversal and generator overhead for wildcard filters | Independent optional candidate; exact filters already use a dictionary |
| `types._decoded_message` | Object construction is prominent in the receive profile | Follow-up candidate; changing immutable/public value objects has more semantic and introspection consequences than compiling codec helpers |
| `transport.websocket._mask_payload` | Masking scales with payload size | Lower priority for this study: four strided slices and `bytes.translate` already do most byte processing in C; a fused typed XOR kernel could reduce copies, but needs its own WebSocket workload |
| Packet IDs, flow control, memory store, engine/effect/writer orchestration | Some Python bookkeeping remains after codec acceleration | Defer until re-profiling: no large numeric loops, substantial mutable state and ordering contracts; do not move asyncio into the protocol layer |
| SQLite, TLS, socket operations | Can dominate selected deployments | These already call native implementations; compiling their Python wrappers cannot remove database, crypto, syscall or network costs |

A separate cProfile pass of the unchanged source supports the codec selection.
For QoS 0 engine ingress, `unpack_utf8` and `decode_qos0_message_v311` are among
the largest self-time contributors alongside `_decoded_message`, `handle_raw`
and effect construction. For `publish_nowait`, `encode_publish_item_v311` leads
self time; direct admission and writer operations remain substantial. Profiled
times are used for attribution only, never for the speedup comparisons.

## Mechanism evaluated

Cython 3.2.4 compiles the existing `.py` files into CPython extension modules
under the same module names. Sources are copied into isolated experiment trees;
the reference tree has only Python modules. Extensions take precedence over
their companion `.py` files in the selected candidate tree. Each worker checks
the imported source root and the expected `.so`/`.py` backend for all ten
candidate modules. A manifest records source and extension SHA-256 hashes.

The compilation uses `language_level=3`, `annotation_typing=False` and
`binding=True`. Type annotations therefore do not become new Cython argument
restrictions. No bounds checking is disabled, no raw pointers are introduced,
and no protocol validation or owned-byte copy is removed. The GIL remains held:
this experiment measures reduced interpreter overhead, not parallel execution.

The groups are:

| Group | Compiled modules |
| --- | --- |
| `python` | None; also used for the A/A control |
| `leaves` | `codec.vbi`, `codec.primitives`, `codec.properties`, `codec.packet_validation`, `topics` |
| `wire` | `leaves` plus `codec.buffer`, `packets._publish`, `packets._ack`, `packets._common` |
| `packets` | `wire` without `codec.buffer`; an ablation added after observing the complete-frame regression |
| `matcher` | `dispatch.matcher` only |
| `combined` | `wire` plus `dispatch.matcher` |

The `packets` follow-up reuses the exact same built extensions, leaving only the
buffer module in Python. It introduces no source optimization or recompile
between comparisons.

This approach keeps one source of protocol logic. Explicitly typed Cython
kernels are a possible second stage if these measurements justify additional
maintenance. A C or Rust extension would offer tighter control over allocation
and typed data, but would require a second implementation and the same binary
distribution work. Neither alternative was benchmarked here.

## Measurement design and limitations

- CPython 3.12.14, Linux x86-64, AMD EPYC 7763 virtual CPU; GCC 14.2.0.
- Affinity exposes CPUs 0–4 with a cgroup budget of four CPU equivalents. Workers
  use CPU 0 and the dedicated Mosquitto process uses CPU 1.
- The runner preflight rejects release eligibility because the CPU governor is
  unavailable. These are exploratory same-host measurements, not release gates
  or public cross-client benchmark evidence.
- Fresh worker processes, fixed `PYTHONHASHSEED=0`, warmup before timing, serial
  measurements, alternating AB/BA order. Four A/A pairs and six A/B pairs per
  scenario. Compilation, imports, warmup and profiling are excluded from timing.
  The noisy initial matcher screen was followed by six pairs with five times
  the timed operations (`--scale 5`); use that longer result for routing.
- CPU uses `process_time`; wall time uses `perf_counter`. Reported CPU speedup
  is the median paired `base CPU/op / candidate CPU/op`. CPU reduction is
  `100 * (1 - 1/speedup)`. This is CPU cost at equal work, not a claim about
  instantaneous CPU utilization at saturation.
- Existing `_paired_scenarios` workloads measure packet encode, engine ingress,
  a complete memory-store QoS 1 exchange, API admission and routing. The API
  admission workload uses a discard writer; it does not measure the socket.
- Framing workloads retain one decoder and feed 32 frames at a time, with 64 B
  or 4096 B payloads. The fragmented case feeds the 64 B batch in 13-byte chunks.
  Results count frames, including feed and extraction costs. They do not include
  packet-field decoding or application delivery.
- The populated MQTT 5 property case decodes content type, two user-property
  pairs and message expiry. The VBI case is the canonical four-byte maximum;
  its gain must not be extrapolated to common one-byte lengths.
- Network workloads use the existing `lean_native_compare._phase`: one client
  publishes and subscribes through a dedicated local Mosquitto, memory store,
  callback delivery, 256 B payload, bursts of 32, inflight limit 20, 12,000
  completed round trips. A separate 1,000-message phase warms the worker. Both
  protocol versions and QoS 0/1/2 are measured. Sequence, payload, delivery count
  and cleared inflight state are checked. CPU excludes the broker but includes
  both publishing and receiving in the client. This is a closed-loop workload,
  not a fixed-rate latency or saturation qualification.
  The broker uses `set_tcp_nodelay true`, as required by the current benchmark
  contract. An initial Nagle-enabled pilot/control was stopped after observing
  the documented roughly 40 ms stalls; none of its timing enters the results.

## CPU results: isolated paths

Positive numbers below mean **less CPU per operation**; negative numbers mean
more CPU. Each number is derived from the median paired CPU ratio, not a
subtraction of independently averaged rates.

| Scenario | `leaves` CPU saved | `wire` CPU saved | `wire` paired speedup range |
| --- | ---: | ---: | ---: |
| QoS 0 packet encoding, MQTT 3.1.1 | 3.8% | **18.9%** | 1.202–1.324× |
| QoS 1 packet encoding, MQTT 5 | 4.8% | **21.1%** | 1.214–1.288× |
| QoS 0 engine ingress, MQTT 3.1.1 | 7.1% | 6.0% | 1.020–1.158× |
| QoS 0 engine ingress, MQTT 5 | 7.8% | 6.3% | 1.001–1.080× |
| QoS 1 engine ingress | 2.4% | 2.1% | 0.888–1.199× |
| Complete QoS 1 memory-store cycle | 4.2% | **17.1%** | 1.145–1.273× |
| `publish_nowait` QoS 0 admission | 0.7% | 7.8% | 1.034–1.123× |
| Framing, 64 B complete payloads | 3.3% | −1.2% | 0.971–1.011× |
| Framing, 4096 B complete payloads | 3.8% | **−5.0%** | 0.947–0.964× |
| Framing, 13-byte input fragments | 2.0% | 8.7% | 0.959–1.130× |
| Populated MQTT 5 properties | 2.2% | 2.6% | 0.997–1.156× |
| Four-byte VBI decode | 30.2% | 29.4% | 1.229–1.443× |

The A/A median CPU ratios span 0.953–1.008 across these workloads and routing;
the QoS 1 ingress control ranges from 0.885 to 1.135. Accordingly the 2.1%
ingress result and the small property/framing deltas are not convincing
improvements. The encode and memory-cycle results are much clearer. The VBI
result is real for this loop-heavy primitive, but the `leaves` rows demonstrate
why it cannot be promoted to a whole-client claim.

Removing `codec.buffer` from the compiled group (`packets`) gives:

| Scenario | CPU saved |
| --- | ---: |
| QoS 0 packet encoding | 19.1% |
| Complete QoS 1 memory-store cycle | 13.3% |
| `publish_nowait` QoS 0 admission | 9.5% |
| Framing, 64 B complete payloads | 5.2% |
| Framing, 4096 B complete payloads | 0.7% |
| Framing, 13-byte input fragments | 3.5% |

These are new comparisons against Python, not a direct paired `packets` versus
`wire` race. They preserve the useful encode gain and do not reproduce the
complete-4-KiB-frame regression. Small remaining framing differences should
still be treated as noise or secondary effects of compiled VBI calls.

The longer matcher confirmation shows **5.0% more CPU** for exact filters
(paired speedups 0.931–0.966×). The 100-wildcard-filter workload shows only 2.2%
less CPU, with speedups 0.994–1.143×. Compiling the matcher as-is is not justified.

## CPU and throughput results: full client over loopback

The narrower `packets` variant is compared against its own fresh Python arm.
The broad `combined` result is shown for context; these two campaigns are not
a direct paired race between the variants.

| Protocol / QoS | A/A CPU saved (should be 0%) | `combined` CPU saved | `packets` CPU saved | `packets` throughput change | `packets` CPU speedup range |
| --- | ---: | ---: | ---: | ---: | ---: |
| MQTT 3.1.1 / 0 | −5.5% | 8.7% | 2.5% | +2.8% | 0.899–1.092× |
| MQTT 3.1.1 / 1 | +1.2% | 2.2% | 4.8% | +4.8% | 0.972–1.107× |
| MQTT 3.1.1 / 2 | −0.7% | 3.2% | −0.7% | −0.6% | 0.912–1.125× |
| MQTT 5 / 0 | +0.5% | 6.2% | 5.8% | +5.0% | 0.980–1.099× |
| MQTT 5 / 1 | +0.5% | 6.8% | 5.0% | +5.4% | 1.042–1.102× |
| MQTT 5 / 2 | −0.3% | 3.3% | 6.0% | +6.6% | 0.909–1.144× |

The codec improvement is diluted by the rest of the client, asyncio and socket
work. The narrow variant's MQTT 5 QoS 1 result is the clearest positive network
signal: all six paired CPU ratios exceed 1.04. Most other packet-variant ranges
cross 1.0, so their medians do **not** establish a reliable improvement. The
especially noisy MQTT 3.1.1 QoS 0 A/A control (0.854–1.017×) precludes a confident
conclusion for that cell. Do not advertise a uniform 5–20% client CPU reduction
from this study, or choose the broader compilation based on its larger noisy
QoS 0 median.

The combined extensions occupy about 7.9 MiB unstripped, including compiler
debug information. In its network campaign the candidate's median peak RSS is
roughly 1.4 MiB above Python. These are diagnostic build/process costs, not a
wheel-size or retained-memory guarantee; a release build needs its own stripped
binary, wheel and memory evaluation. No latency-tail or open-loop fairness
improvement is claimed.

## Decision and integration path

**Cython is feasible, but compiling everything is not justified.** The most
useful measured boundary is packet encoding and related codec helpers. The
`packets` variant is a reasonable next prototype: it keeps approximately 19%
less CPU on isolated encoding and avoids the complete-frame regression.
It does not establish a large or universal end-to-end gain.

1. Keep the Python implementation as the reference. Prioritize `_publish`,
   `_ack` and the helpers they call; further ablation can narrow this eight-module
   group. Compiling VBI or properties alone is not a sufficient product case.
2. Keep `codec.buffer` and `dispatch.matcher` in Python with this mechanism.
   A typed extension class or fused kernel would be a different experiment,
   requiring renewed ownership, malformed-input and allocation validation.
3. Before accepting an optional backend, close the traceback/retention testing
   gap below and run both backends across supported CPython versions 3.11–3.15,
   Linux/macOS/Windows and x86-64/ARM64. Only CPython 3.12/Linux/x86-64 was built
   here. Include fragmented ingress, 4 KiB and larger payloads, TLS/WebSocket,
   SQLite and fixed-rate latency/fairness on an eligible target host.
4. Integrate through the existing Hatch build/release path if that evidence
   justifies it. Build Cython is a build-time dependency, not a runtime Python
   dependency. Preserve a pure-Python distribution for unsupported platforms;
   binary wheels must be correctly tagged and tested as the actual released
   artifacts. The experimental same-name `.so` overlay is not a production
   fallback mechanism: a broken extension import must not be mistaken for a
   valid pure-Python run. CI and benchmarks should explicitly identify the backend.
5. Evaluate explicitly typed Cython kernels only after re-profiling the remaining
   client CPU. A second implementation in C/Rust, or compiling the session engine
   wholesale, is not supported by the measured benefit/maintenance tradeoff here.

No production accelerator, release-build change or runtime dependency was added.

## Functional evidence

The unchanged reference passes **2,591 unit and project tests**. Both the combined
and final packets prototypes pass **2,547 of 2,548 unit tests**. The combined
prototype also passes the seeded fuzzer's **20,000
iterations for each of codec, engine and WebSocket**, and **53 Hypothesis,
stateful-invariant and matcher differential tests**. Ruff, formatting, mypy
and Bandit checks pass. The final 43 project tests and strict documentation build
also pass.

One unit test remains incompatible with compiled execution:
`test_property_decode_budget.py::test_over_budget_value_is_not_decoded_or_retained`.
Its assertions that the decode budget raises and that exactly `2 * LIMIT`
UTF-8 fields were decoded pass. Its search for a Python traceback frame named
`decode_properties` fails under Cython, so its subsequent local-variable
retention assertion cannot run. This is **not full validation of the compiled
backend**: add an equivalent observable retention check before shipping, while
preserving the Python-backend check. The test is unchanged, not skipped.

An initial sandboxed test run also failed 21 release-tool tests because the
sandbox changes ancestor ownership as observed by the private-output-directory
checks. Re-running with the real filesystem ownership, as documented in the
local environment guide, removes all 21 failures without changing safeguards.

## Reproduction

From the repository root, using pip as required by the project:

```bash
.venv/bin/python -m pip install Cython==3.2.4 setuptools==84.0.0
CC=gcc LDSHARED='gcc -shared' .venv/bin/python benchmarks/compiled_modules.py \
  build --root /tmp/mqttium-compiled-study
.venv/bin/python benchmarks/compiled_modules.py compare \
  --root /tmp/mqttium-compiled-study --variant python --repeat 4 \
  --output /tmp/mqttium-compiled-aa.json
.venv/bin/python benchmarks/compiled_modules.py compare \
  --root /tmp/mqttium-compiled-study --variant wire --repeat 6 \
  --output /tmp/mqttium-compiled-wire.json
```

Repeat with `--variant leaves` for codec attribution; for routing use
`--variant matcher --cases dispatch_match_exact,dispatch_match_100`. Run only
one measurement campaign at a time. The build destination must not exist.
The explicit compiler override was needed because this CPython was built with
Clang, but this environment provides GCC rather than a `clang` executable.

For network measurements, start a local Mosquitto on `127.0.0.1:18885` on CPU 1
with anonymous access, persistence disabled and `set_tcp_nodelay true`. Then run the same comparison
with `--variant python` (A/A) and `--variant combined` (A/B), specifying:

```text
--cases network_311_qos0,network_311_qos1,network_311_qos2,network_5_qos0,network_5_qos1,network_5_qos2
```

The harness defaults to CPU 0, port 18885 and count 12000 on this host; set
`--cpu`, `--port` and `--count` explicitly on other machines. Reproduce candidate
tests with both `PYTHONPATH=/tmp/mqttium-compiled-study/combined/src` and pytest's
`-o pythonpath=/tmp/mqttium-compiled-study/combined/src` so the project's default
`pythonpath=src` cannot silently select the reference. The deterministic fuzzer
uses `PYTHONPATH` directly. Profiles can be reproduced with the harness's
`worker --root <tree> --variant <group> --case <scenario> --profile` command.

Raw JSON, build/test logs and binary experiment trees for this run are local
artifacts under `/workspace/cloud-setup/compiled-*`. They are not tracked in
Git. The build manifest is
`/workspace/cloud-setup/compiled-study-v1/manifest.json`.
The measured tables come from `compiled-aa.json`, `compiled-leaves.json`,
`compiled-wire.json`, `compiled-packets.json`, `compiled-matcher-long.json`,
`compiled-network-aa.json`, `compiled-network-combined.json` and
`compiled-network-packets.json`. The final campaign log is `compiled-followup.log`;
the earlier `compiled-campaign.log` contains the microbenchmark screen and the
abandoned first network control. The dedicated broker was stopped after the study.
