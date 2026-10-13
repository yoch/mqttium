# Complete native codec audit — 2026-10-13

## Disposition

[PR #623](https://github.com/yoch/mqttium/pull/623) was opened at `aba4f5f` and
reviewed for memory safety, wire compatibility, resource ownership, generator
maintenance, packaging and missed optimizations. Corrections are in `66335f0`
and `e1e4ced`. The code at `e1e4ced7cd97a34e43ee42a959d4aa178b2646ed` passes
[the complete required CI](https://github.com/yoch/mqttium/actions/runs/38365567733),
including native builds on Linux, Windows and macOS.

No unresolved blocking defect was identified in the audited paths. This is a
bounded review and test result, not a proof that all possible bugs are absent.
Native editable installation remains deliberately unsupported and now fails
explicitly, with working installation alternatives in the error and guide.

## Confirmed findings and corrections

| Finding | Priority | Evidence and correction |
| --- | --- | --- |
| Native editable install silently loses acceleration | P2 | `MQTTIUM_BUILD_CODEC=1 pip install -e .` succeeded but installed the extension outside the source package selected by the editable path. `auto` then selected Python and `native` raised `ModuleNotFoundError`. Native editable builds now fail before C compilation; regular wheel/source installs and pure-Python editable installs remain supported. Both the initial failure and corrected rejection were reproduced with pip. |
| Source transformations could discard new validation | P2 | The decoder transformation checked only the `if` condition before replacing both branches; adding validation inside either branch would be silently discarded. PUBLISH assembly also bypassed an unchecked generic tail. The generator now verifies the complete copy block and fingerprints the reviewed assembly tail. Mutation tests add validation and require generation to fail. |
| Imported globals escaped collision detection | P2 | Only definitions were tracked when fusing module namespaces. An import such as `from math import pi as MAX_VBI` in a later module could overwrite the earlier codec's wire maximum. The generator now tracks definition/import origins and rejects conflicts, wildcard imports and relative imports. The alias collision is covered by a mutation test. |
| Failed/interrupted builds leave generated staging files | P3 | Staging cleanup ran only after a successful wheel build. The hook now cleans its own unique directory on exceptions, including interruption, while preserving concurrent staging directories. Tests cover both ordinary failure and `KeyboardInterrupt`. |
| Native fingerprint omitted generator transformations | P3 | Two different generators could advertise the same source digest. The generator source is now included. New decoder fields also require explicit review instead of being assigned a C integer type implicitly. |

A validation regression introduced while adding the assembly guard was caught
by the expanded CI: Python 3.13 changes `ast.dump()` formatting for empty
fields, causing false source-drift failures. `e1e4ced` normalizes semantic AST
structure instead of hashing the interpreter's diagnostic representation. The
Linux 3.11–3.15 and Windows/macOS 3.11/3.15 jobs then all passed. The intermediate
failure is not attributed to the original runtime codec.

The reference Python wire algorithms and shipped Cython kernels were not
changed by this audit. The hardened generator produces the same executable
Cython code as the sanitized build, apart from its diagnostic source digest;
that equivalence was checked directly.

## Review scope and memory argument

Reviewed the build hook, backend selector, generated namespace/export mapping,
all handwritten Cython kernels, decoder transformations, PUBLISH fast-path
preconditions, source/reference fallback and affected tests. The review followed
calls through properties, packet bindings and client admission/delivery.

- Primitive pointer readers are reached only for exact bytes and exact integer
  offsets in `0..len(buffer)`. Their remaining-length checks precede fixed-width
  or UTF-8 reads; malformed/truncated input retains protocol errors.
- PUBLISH assembly requires exact bytes inputs after maintained validation and
  MQTT Remaining Length checks. Header size, MID width, VBI length and payload
  placement were checked together. The segmented branch keeps owned payload
  bytes and the existing writer ordering contract.
- Decoder copies validate both ends against the actual bytearray capacity and
  return immutable owned bytes. Integer frame extents are bounded by MQTT wire
  limits; control counters reset at bounded thresholds. No borrowed pointer
  escapes to the client, and no kernel releases the GIL while using a pointer.
- VBI accumulators remain bounded to four digits. The generic path continues to
  handle negative/unusual offsets and buffer representations; differential
  checks include their exception types and messages.
- Property validation and decode budgets remain in the reference implementation.
  The retained-exception memory test checks live decoded strings, without
  depending on native traceback locals.
- Explicit backend selection is checked before client codec binding. Missing
  extension fallback, broken ABI/dependency imports, and forced Python selection
  are tested. The extension has no Cython runtime dependency.

## Executed validation

| Check | Result |
| --- | --- |
| Local CPython 3.12 native unit/project suite | 2,610 passed |
| Local Python reference unit/project suite | 2,608 passed, two native-only tests skipped; 93.85% branch-inclusive coverage |
| Expanded differential oracle | 36,740 identical normalized outputs/errors between Python and native |
| Hypothesis/stateful tests | 52 passed |
| Native resilience suite | 27 passed |
| AddressSanitizer + UndefinedBehaviorSanitizer oracle | All 36,740 outputs match Python; no diagnostic |
| Sanitized deterministic codec fuzz | 100,000 iterations, zero crashes/invariant violations |
| Sanitized deterministic engine fuzz | 20,000 iterations, zero crashes/invariant violations |
| Sanitized deterministic WebSocket fuzz | 20,000 iterations, zero crashes/invariant violations |
| Native Linux CI | Python 3.11, 3.12, 3.13, 3.14 and 3.15: sdist build, installed wheel, unit/project, differential, fuzz and Mosquitto integration passed |
| Native Windows/macOS CI | Python 3.11 and 3.15: wheel build, installed-wheel unit and socket-resilience tests passed |
| Packaging | Native/pure distributions pass CI validation; native wheel imports in a clean environment without Cython; native editable rejection reproduced |
| Static/documentation checks | Ruff, mypy, Bandit, Actionlint and strict MkDocs pass; Zizmor offline locally and online in CI pass |

The expanded oracle adds all property specifications across allowed/disallowed
packet contexts, truncated property bodies, UTF-8 limits, direct writable-window
receives interleaved with feed, reusable-decoder ownership, and malformed frame
bounds. It supplements the protocol suite rather than replacing it.

Sanitizers used GCC 14.2, CPython 3.12 and Cython 3.2.4 on Linux x86-64, compiling
the extension with `-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer`.
`PYTHONMALLOC=malloc`, ASan preloading and fail-on-error UBSan were enabled.
LeakSanitizer was disabled: this run checks invalid access and undefined
behaviour, not process-exit interpreter leak accounting. Existing ownership and
retention tests still run separately. An initial all-target 100,000-iteration
run completed the codec stage; its increasingly expensive engine stage was
interrupted and rerun to a bounded, completed 20,000 iterations. Interrupted
engine progress is not included in the pass totals above.

The initial PR's native Linux CI also passed before audit changes. The extended
matrix adds compiled coverage on Windows/macOS, where previous jobs exercised
only Python. CI builds with isolated dependencies; observed current CI uses
Cython 3.3.0, while local sanitizer/probe builds exercise the declared minimum
3.2.4.

## Missed-optimization experiment

A separate, unshipped copy added a bounded C pointer VBI loop behind exact-type
and offset/end checks. Its complete differential corpus passed. Six alternating
fresh-process native/native pairs per case were measured with process CPU time,
CPU 0 affinity, warmup and approximately 0.4-second samples. No builds or tests
ran concurrently with this probe. Baseline is the audited compiled codec, not
Python. Raw paired ranges are not confidence intervals.

| Case | Candidate CPU cost change | Observed baseline/candidate speedup range |
| --- | ---: | ---: |
| One-byte VBI | +22.2% | 0.780–0.924× |
| Four-byte VBI | −68.7% | 2.825–3.322× |
| Complete 64-byte frames | +9.4% | 0.821–0.976× |
| Complete 4 KiB frames | −23.8% | 1.140–1.355× |
| Fragmented frames | +14.1% | 0.808–0.902× |

**Rejected for this PR.** Checking and dispatching into the new kernel costs
more than the existing one-byte path. Faster large VBIs do not justify regressions
on small and fragmented frames. A future candidate should enter the C loop only
after the existing one-byte return, or use already-validated decoder state to
avoid repeating checks. That narrower design still needs differential tests,
sanitizers and paired framing/network evidence; it is not claimed as a measured
gain here.

Other remaining candidates are allocations of packet/result tuples, repeated
property decoder calls and client/engine work outside the codec. Replacing those
objects or compiling state-machine code would increase scope and alter ownership
or test seams. The earlier admission/network results already show that codec
speedup does not translate one-for-one to client speedup. No additional speedup
is claimed for these unimplemented candidates. Releasing the GIL around these
short Python-object-heavy paths is not recommended without a separate design.

## Limits and retained evidence

This audit does not qualify ARM64, 32-bit builds, free-threaded CPython, PyPy,
all custom object subclasses, allocator exhaustion, or every possible transport
and broker behaviour. The sanitizers instrument the extension, not a rebuilt
sanitized interpreter. Long production soaks, fixed-rate performance, TLS/
WebSocket performance and SQLite performance remain distinct qualification work.
The CI workflow named “Soak and broker interoperability” on a pull request is
not presented as evidence that a long scheduled soak ran.

The [implementation guide](../native-codec.md) states supported build modes.
The [October 12 measurements](NATIVE-CODEC-2026-10-12.md) remain immutable; audit
changes do not replace their historical environment or claim new whole-client
gains. The VBI probe and all binary/raw outputs remain outside the repository.

Local evidence under `/workspace/cloud-setup/codec-audit/`:

- `editable.log`, `editable-rejected.log`: packaging reproduction and correction.
- `python-tests.log`, `native-tests.log`, `hypothesis.log`, `resilience.log`.
- `sanitized-build.log`, `sanitized-corpus.json`, `sanitized-corpus.log`,
  `sanitized-fuzz.log`, `sanitized-engine.log`, `sanitized-websocket.log`.
- `expanded-python.json`, `expanded-native.json`: reference/candidate oracle.
- `sanitized-generation.pyx`, `current-generation.pyx`: runtime-source comparison.
- `vbi-probe/`, `vbi-probe-dist/`, `vbi_probe_bench.py`,
  `vbi-probe-results.json`, `vbi-probe-results.log`, `vbi-probe-corpus.json`:
  exact rejected prototype and raw timings.

The referenced code CI is retained on GitHub at run `38365567733`.
