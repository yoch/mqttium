# Compiled acceleration of hot paths

Date: 2026-08-13.

Base commit: `1925145` (`main`, after `v1.0.0rc2` plus the QoS 1/2 launch-encode
and success-ack fast paths).

## Question

Would shipping compiled fragments — Cython, mypyc, C, or Rust — buy a
*significant* throughput or latency gain without giving up a Pythonic,
dependency-free, fuzzable client?

This is an evaluation, not a prototype. No runtime code changes. Numbers below
are from one in-process pass on this host (CPython 3.12.3, Linux x86_64) and
from issue #39's earlier same-runner decompositions. They are diagnostic, not
release evidence: the host is not `runner_probe.py --enforce` eligible, and
hosted-runner wall-clock is not claimed.

## Verdict

Do not introduce compiled code before 1.0.

After 1.0, the only experiment worth running is an **optional compiled overlay
of the codec kernel**, with the current Python remaining the source of truth,
the fuzz oracle, and the sdist fallback. Compiling `ProtocolEngine`, the
directional sessions, `EffectPump`, `WritePump` or `AsyncClient` would freeze
the project's main maintainability assets for a ceiling the existing
decomposition already bounds.

The remaining gap versus thinner clients is mostly object construction,
isolated delivery and ordered effects — not interpreted byte loops. Compiled
kernels that still return Python `Message` / `bytes` objects still pay the
dominant cost. A per-primitive FFI wrapper around VBI or UTF-8 would likely
lose.

## Constraints any compiled path must keep

These are load-bearing, not preferences:

- **Zero runtime dependencies.** A compiler, Cython, or a Rust toolchain may
  exist in the *build* extra. It must not appear in the installed wheel's
  `Requires-Dist`.
- **Pure-Python fallback.** Today the artefact is a `py3-none-any` wheel that
  `pip install`s without a compiler on 3.11–3.14, Linux, macOS and Windows.
  Losing that for a 5% micro gain is a product change, not an optimisation.
- **Owned immutable bytes.** The decoder must never hand a `memoryview` of its
  reusable buffer to the engine or the application
  ([`PUBLISH-DECODE-PROFILE.md`](PUBLISH-DECODE-PROFILE.md)).
- **Engine isolation.** `protocol/` stays synchronous, free of `asyncio`,
  sockets and callbacks. A compiled kernel may *feed* the engine; it must not
  become the engine.
- **Fuzz equivalence.** Deterministic and Hypothesis campaigns must pass
  against both implementations, with byte-identical frames and the same
  exception types on malformed input. `mqttium.codec` is Provisional: silent
  divergence is still a bug.
- **Benchmarking contract.** Retain only if
  [`BENCHMARKING.md`](../BENCHMARKING.md) is met: ≥2% micro with 8/11 pairs, or
  ≥5% network at two load points; baseline CV ≤5%; control within 2%; no
  fairness, memory or semantic regression.
- **Pythonic public surface.** `AsyncClient`, `Message`, `PublishReceipt` and
  the helpers stay ordinary Python types. Compiled code is an implementation
  detail behind Provisional codec names.

"Native" in this repository already means the async client versus the Paho
façade. This report uses **compiled** / **extension module** for C/Rust/mypyc
artefacts.

## Where time actually goes

Issue #39 ranked causes on a dedicated runner at MQTT 3.1.1 QoS 0, 256-byte
payload, after the specialised inbound decoders had landed:

| Stage | Time / message |
| --- | ---: |
| Framing / `IncrementalDecoder` | 3.02 µs |
| Minimal topic + payload parse | +0.96 µs |
| Topic validation | +0.08 µs |
| `Message` construction / retention | +2.34 µs |
| `EngineEffect` wrapper | +0.23 µs |
| asyncio queue hops | +0.8–0.93 µs |
| Remaining inbound-session work | +0.62 µs |
| Common engine dispatch | +0.45 µs |
| Full `ProtocolEngine` | 7.89 µs |

The principal avoidable ingress cost after #41/#42/#174/#198 is therefore
**Python object construction**, not Remaining Length or UTF-8. Callback
isolation versus gmqtt's inline `on_message` explains another large slice of
the headline ingress gap; that is a contract, not a missing `xor`.

This host, same tree, in-process (not paired, not CPU-pinned), from
`benchmarks/native_kernel_probe.py`:

| Operation | ns/op |
| --- | ---: |
| Empty Python call | 21 |
| `bytes()` copy, 18 B | 72 |
| `encode_vbi` / `decode_vbi` (one byte) | 52 / 80 |
| `RawPacket(...)` | 327 |
| `unpack_utf8` + payload slice | 373 |
| `IncrementalDecoder.next_packet` (pre-fed small PUBLISH) | 1 046 |
| `Message(...)` with fields already decoded | 912 |
| `encode_publish_item` QoS 0 small | 940 |
| `encode_publish_item` QoS 1 small | 1 033 |
| Specialised `_decode_v311_qos0_message` | 1 582 |
| `PublishPacket.decode` QoS 0 | 1 861 |
| `encode_properties` empty / two fields | 91 / 1 429 |
| `PubAckPacket.encode` success | 760 |
| `ProtocolEngine.handle_raw` QoS 0 (existing `RawPacket`) | 2 805 |
| Engine QoS 1 publish + PUBACK cycle | 9 842 |
| WebSocket `_mask_payload` 64 B / 4 KiB / 1 MiB | 1 214 / 7 222 / 1 864 472 |

Reproduce with `PYTHONPATH=src python benchmarks/native_kernel_probe.py`.
Absolute nanoseconds on this host are noise-sensitive; the *ratios* (Message
construction ≈ `next_packet`, VBI ≪ FFI, 1 MiB mask ≫ small-frame mask) are
the claim.

Two arithmetic consequences:

1. **FFI is not free.** A no-op CPython extension call is typically 40–80 ns.
   Wrapping `encode_vbi` (62 ns) or `decode_vbi` (77 ns) as its own exported
   function would likely regress. Compiled work has to absorb a whole packet
   (or a batch) per crossing.
2. **A compiled decoder that still returns a Python `Message` still pays
   ~1 µs.** On this host that is as large as `next_packet` itself. The
   specialised QoS 0 path already skipped `PublishPacket`; the leftover is the
   frozen slotted dataclass the Stable API exposes.

CPython's own C is already on the path: `str.encode` / `bytes.decode`,
`str.find` for the MQTT UTF-8 NUL/BOM rules, `bytes.translate` for WebSocket
masking, `struct.unpack_from`, `bytearray.extend`. Interpreted control flow
over a handful of header bytes is what remains, and it is small next to
allocation.

## What compiled code can and cannot buy

### Can help, with a bounded ceiling

| Kernel | Why it is a candidate | Realistic ceiling |
| --- | --- | --- |
| Combined frame + PUBLISH field decode, preferably batched | One crossing: buffer → `(topic, payload, qos, mid, flags)`. Avoids `RawPacket` + a second parse. This is the *CPU* form of the in-buffer decoder rejected for peak memory. | Maybe 0.5–1.5 µs/msg on the engine ingress path (~15–40% of `handle_raw`+framing here; a much smaller fraction of isolated-callback end-to-end). Must still copy the payload to owned `bytes`. |
| `encode_publish_item` | Hot outbound QoS 0/1 launch; ~0.9 µs of bytearray assembly. | Perhaps 2× on the encode micro; a few percent of `publish_nowait` once admission, receipts and the writer remain Python. |
| MQTT 5 property table when properties are actually present | Empty table is 82 ns; two fields are 1.4–1.7 µs of Python dispatch. | Pays only on property-heavy PUBLISH. The common telemetry path is empty. |
| WebSocket mask on large frames | 64 B is ~1 µs (four `translate` passes). 1 MiB is ~1.7 ms ≈ 620 MiB/s, memory-bandwidth bound in Python. A C `xor` is typically several GiB/s. | Irrelevant for small MQTT frames. Material for 64 KiB–1 MiB WebSocket payloads, which are not the current QoS 0/1 ranking driver. Unmeasured end-to-end; [`MEMORY-PROFILE-FOLLOW-UP.md`](MEMORY-PROFILE-FOLLOW-UP.md) explicitly parked native masking. |
| Compiling `Message` together with the decoder (mypyc) | Construction is ~1 µs. mypyc can turn a frozen dataclass into a C-level type that still looks like Python. | The one way compiled code attacks the *dominant* leftover. Public identity, hashing, and `slots`/`frozen` behaviour must stay identical. |

### Will not pay, or would cost more than it saves

- **VBI, `pack_u16`, UTF-8 validation as standalone exports.** Already C or
  already cheaper than an FFI round trip.
- **`PacketIdPool`.** Sequential allocate is ~150 ns of Python; the frontier
  representation is a memory win, not a CPU crisis
  ([`PACKET-ID-POOL-PERFORMANCE.md`](PACKET-ID-POOL-PERFORMANCE.md)).
- **Success PUBACK/PUBREC/PUBREL/PUBCOMP.** Already a four-byte literal.
- **`ProtocolEngine`, `InboundSession`, `OutboundSession`.** Correctness core,
  fuzz target, rollback/admission transactions. Compiling them does not remove
  `Message` / effect / store objects and makes every protocol change a
  rebuild. Issue #39 already showed engine dispatch after decode is ~0.45 µs.
- **`EffectPump`, `WritePump`, `AsyncClient`.** The cost is scheduling,
  isolation and fairness. #39 rejected inline callbacks and suspension removal
  for contract reasons, not because a compiler was missing.
- **`SqliteInflightStore`.** SQLite is already compiled. The remaining cost is
  Python marshalling of metadata, which the transition extension exists to
  avoid on the ACK path.
- **`TopicMatcher`.** Exact filters are a dict; wildcards are a short linear
  scan of user-installed filters, not a per-byte loop.

### Architectural gap compiled code cannot close

gmqtt's ingress advantage is a thinner path: parse, call `on_message`,
sometimes `transport.write` from that callback. MQTTium's path is: owned
frame → engine → ordered `EngineEffect` → `EffectPump` → bounded delivery
queue → isolated worker. Compiling the first step does not delete the others.
An opt-in inline callback is a *semantic* change and is out of scope here
(issue #39, `PERFORMANCE-1.0.0rc1.md`).

On QoS 1 RTT the remaining difference versus gmqtt is load-dependent queueing
and completion discipline, not a missing inner loop. A compiled PUBLISH
encoder that saves 0.4 µs inside a 200 µs–1 ms round trip is invisible.

## Tooling

Ranked for *this* repository, not for extension modules in general.

### 1. mypyc — first prototype, if any

Compile selected typed modules (`codec/`, `packets/publish.py`,
`types.Message`) to C extensions while keeping the `.py` files as the edited
source.

- **Fit.** Dev extra already has mypy. The codec is small (~900 lines),
  typed, and free of `asyncio`.
- **Maintainability.** Contributors still write Python. No second language,
  no `.pyx` dialect.
- **Risks.** `dataclass(slots=True, frozen=True)` support is historically
  uneven; exceptions, `IntEnum`, and `Protocol` store hooks at the engine
  boundary are easy to get wrong. mypyc tracks CPython releases with a lag —
  3.14 support is a release-blocker if the compiled wheel is the default.
- **Packaging.** Per-CPython-version wheels (not ABI3). `hatch-mypyc` or a
  custom hatch hook. sdist still ships the `.py` files so `pip install` on an
  unknown tag falls back to interpreted code.

Use mypyc to answer "does compiling `Message` + the decoder move
`ingress_engine_qos0` by ≥2%?". If it does not, stop. Do not rewrite in C to
chase the same objects.

### 2. Cython — second, only for a fused kernel

One extension module, e.g. `mqttium._codec`, implementing
`IncrementalDecoder.next_packet` fused with PUBLISH field extraction and
`encode_publish_item`. Python wrappers keep the Provisional names.

- **Fit.** Mature wheels, Windows, `language_level=3`, can compile annotated
  `.py` in pure-Python mode.
- **Maintainability.** Acceptable if the Cython is a *leaf* and the Python
  reference stays authoritative. Dual `.py` / `.pyx` implementations of the
  engine would not be.
- **Risks.** Typed memoryviews make it easy to violate the owned-bytes
  invariant. Exception translation must produce `MalformedPacketError` /
  `ProtocolError`, not `ValueError`.

### 3. Hand-written C (CPython C API or HPy)

Maximum control, worst fit. The codec is 900 lines of already-specialised
Python; a C port would be a second implementation to fuzz, and the
contributor base is Python. Limited API / ABI3 would shrink the wheel matrix
if this were ever chosen, but it should not be chosen first.

### 4. Rust / PyO3 / maturin

Memory-safe and fashionable. It adds a second ecosystem, a second formatter,
and a second CI image for a kernel that is not doing cryptography or SIMD
parsing of untrusted multi-megabyte documents. MQTT headers are tiny.
`pydantic-core` is the wrong analogy: that project *is* the compiled engine.
MQTTium's identity is a readable protocol state machine.

Consider Rust only if a fused kernel has already won in Cython/mypyc *and*
the C dialect becomes the maintenance problem. Not as the opening move.

### 5. Do not use

- **cffi / ctypes wrapping a tiny C file per primitive.** FFI tax, two
  artefacts, no batching.
- **Numba, PyPy wheels as the supported runtime.** Extra deps or a second
  interpreter. The library's contract is CPython 3.11–3.14.
- **An optional extra `mqttium[fast]` that pulls a dependency.** That is a
  runtime dependency by another name. The split, if any, is *wheel tag*
  (compiled vs `py3-none-any`), not an extra.

### 6. Measure CPython's own JIT first

3.13+ copy-and-patch can speed exactly the interpreted header loops without
any packaging change. This tree's CI already runs 3.13 and 3.14. A
`PYTHON_JIT=1` pass of `native_kernel_probe.py` and `paired_regression.py`
on an eligible host is cheaper than cibuildwheel and might close the case.

Free-threaded CPython is a deliberate non-goal (`DESIGN.md`). A compiled
extension that is not free-thread-safe would make that non-goal harder to
revisit.

## Packaging and CI cost

Today: hatchling, no build hook, one `py3-none-any` wheel, the `package` job
imports every module of that wheel in an isolated interpreter.

A default compiled wheel implies:

- `cibuildwheel` for manylinux, musllinux, macOS universal2, Windows;
- four Python minor versions, at least x86_64 and arm64;
- a compiler in the sdist path, or a documented "interpreted fallback";
- longer `package` / `publish` jobs, and a new class of "the 3.14rc wheel
  lagged mypyc" failures;
- `check-wheel-contents` and the isolated-import smoke must still pass, now
  for many tags.

That cost is justifiable for a ≥5% end-to-end gain on the scenarios issue #39
cares about. It is not justifiable for a 2% `encode_qos0` micro that does not
move `native_publish_nowait_qos0` or broker-fed ingress.

Recommended shape *if* a prototype clears the gate:

1. sdist contains only Python (current tree).
2. Build produces *either* `py3-none-any` (default, what PyPI users get) *or*
   platform wheels from a separate optional workflow.
3. At import, `codec` tries `_codec` and falls back. No environment variable
   required for correctness.
4. Tests and fuzz run twice in CI when a compiled artefact is present:
   fallback-only, then extension-loaded, with a differential oracle.

Until that prototype exists, do not add a build backend, a `src/mqttium/*.c`,
or a cibuildwheel job.

## How to measure a prototype

Follow the existing order. Do not start with a network sweep.

1. **Exact work.** `benchmarks/hotpath_profile.py` on `encode_qos0`,
   `ingress_engine_qos0`, `native_publish_nowait_qos0`. The compiled path must
   reduce *calls* or *allocations*, not only hope for wall-clock.
2. **Kernel probe.** `benchmarks/native_kernel_probe.py` against base and
   candidate. If `Message(...)` is unchanged and `next_packet` has not moved
   by more than the FFI noise, stop.
3. **Paired micro.** `paired_regression.py`, 11 ABBA pairs, CPU-pinned eligible
   host. Required cells: `encode_qos0`, `encode_qos1`, `ingress_engine_qos0`,
   `ingress_engine_qos1`, `native_publish_nowait_qos0`. Control:
   `qos1_cycle_memory` (must stay within 2% if the kernel does not touch the
   store).
4. **Equivalence before speed.** Byte-identical encode for the publish matrix
   already used by unit tests; Hypothesis + `tests/fuzz/fuzz.py` on both
   implementations; malformed VBI, non-canonical VBI, oversize packets, QoS 3,
   DUP on QoS 0.
5. **Network last.** `paired_open_loop.py` at 50% and 90% calibrated load,
   MQTT 3.1.1, 64-byte and 256-byte, receipt and callback completion. Closed
   loop `paired_network.py` stays advisory. Cross-client rankings stay on the
   external harness.
6. **Reject** if event-loop lag p95 rises, if memory thresholds move, or if
   the gain exists only with inline callbacks.

A throwaway mypyc or Cython branch is the right vehicle. It is not a
candidate for merge until step 3 is green.

## Maintainability rules if it ever ships

- One Python reference implementation. The compiled module is allowed to be
  faster, not different.
- No `#ifdef`, no feature-detect in the engine, no "fast Message" vs "slow
  Message" in the Stable API.
- Public types remain importable when the extension is absent (`py.typed`
  still applies to the Python sources).
- The engine continues to receive owned `bytes` and to emit `EngineEffect`.
  Compiled code stops at the codec / `encode_publish_item` boundary.
- Comments that record rejected Python-level ideas (effect-pump split,
  inflight-table `getsizeof` gate, in-buffer PUBLISH parser) stay; a compiled
  kernel is not permission to revive them without a new measurement.
- Complexity proportional to the measured gain. A 200-line Cython fused
  decoder that clears 5% ingress is in proportion. A Rust crate plus
  maturin plus a second property table is not.

## Decision

- **Before 1.0:** no compiled code, no packaging-matrix change. Keep picking
  Python-level leftovers that remove a *call* (the programme that produced
  the specialised decoders, exact wire-size admission, success-ack literals,
  and the QoS 1/2 launch encode).
- **After 1.0, only if a production profile shows decoder CPU as the
  bottleneck:** a throwaway mypyc (then Cython) overlay of the codec kernel
  plus `Message`, measured as above. If it misses the gate, close the
  question rather than escalating to C or Rust.
- **Never:** compiling the protocol state machine, the asyncio adapter, or
  wrapping VBI/UTF-8 as standalone extensions.

The in-buffer PUBLISH parser remains rejected for peak memory. Revisit it
only as part of a fused compiled kernel whose *CPU* case has already been
won, and only with the same owned-bytes and malformed-packet isolation
constraints.
