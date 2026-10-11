# Python 3.15 profiling

Python 3.15 adds Tachyon (`profiling.sampling`). Use it to investigate where
MQTTium executes and waits, alongside the existing exact-call and allocation
tools. The runtime remains dependency-free and supports Python 3.11–3.15.

## Reproducible local qualification

Install MQTTium with pip in a Python 3.15 environment. The controller and its
fresh workers use that same interpreter and inherit `PYTHON_JIT`/`PYTHON_GIL`.
Write artifacts to a new directory outside the repository:

```bash
PYTHONPATH=src python benchmarks/sampling_profile.py \
  --suite micro --scenario encode_qos0 --mode cpu \
  --seconds 3 --cycles 2 --output /tmp/mqttium-encode-profile

PYTHONPATH=src python benchmarks/sampling_profile.py \
  --suite native --scenario receive_iterator --mode async-all \
  --rate 1000 --seconds 3 --cycles 2 --output /tmp/mqttium-receive-profile
```

The micro suite uses `_paired_scenarios`; the native suite uses the existing
packet-aware diagnostic transport from `lean_native_diagnostics`. Native phases
validate deliveries, receipts and cleanup. This transport includes broker
emulation and is not a network capacity measurement. Repeated phases include
their own setup, warm-up and teardown, so their aggregate operations/second
must not be substituted for the release gates' measurements.

The script primes one phase before READY, runs two uninstrumented controls,
then baseline/sample/sample/baseline cycles in fresh processes. It records
wall time, process CPU, operations, runtime/build, JIT and GIL state, and
same-code variability. It reports the sampled/baseline ratio without a pass
threshold. A noisy control invalidates an overhead claim; increase duration
and qualify the host before interpreting small differences.

On Linux, `--worker-cpu` and `--profiler-cpu` can reserve separate logical CPUs.
Choose different physical cores, avoiding SMT siblings and other workloads.
The scheduled performance-health job captures both 1 kHz and 10 kHz on 3.15;
existing 3.12 timing and memory references remain separate.

## Choose the question before the mode

| Mode | Intended investigation |
| --- | --- |
| `cpu` | Encoding, decoding, dispatch, admission and persistence CPU costs |
| `wall` | Time including sleeps, sockets and synchronization |
| `gil` | Which threads hold the GIL in a standard build |
| `exception` | Error/exception paths; choose a workload that exercises them |
| `async-running` | Logical await chains of active tasks |
| `async-all` | Active and suspended tasks, including flow/queue waits |

Async-aware mode is not compatible with CPU/GIL/exception mode, all-thread
selection, native frames, or binary output in Python 3.15. Its self-contained
HTML contains the complete profile tree; the controller also saves that tree
as `profile.json`. Other modes save `profile.bin` and render HTML afterwards.
The JSON extraction detects changes in CPython's HTML format and fails
explicitly. Empty captures are rejected. Logs survive failures and timeouts.

Suspended-task weights are not a sum of end-to-end latency: several tasks can
wait during the same sample. Correlate those stacks with receipt completion,
writer/iterator bounds and existing loop-lag measurements. Read sampling-error
statistics in the HTML before trusting short-lived hotspots.

Keep `hotpath_profile.py` for exact calls/operation and `memory_profile.py` for
allocation evidence. Sampling cannot count every call or prove a small speedup.
Confirm an optimization with the existing uninstrumented A/A and ABBA gates.
Do not relax protocol or persistence invariants in response to a profile.

## Live processes and hangs

```bash
python -m profiling.sampling attach --mode cpu -a -d 20 --flamegraph -o cpu.html PID
python -m profiling.sampling attach --async-aware --async-mode all -d 20 --flamegraph -o waits.html PID
python -m profiling.sampling dump --async-aware --async-mode all PID
python -m profiling.sampling replay profile.bin --diff-flamegraph baseline.bin -o diff.html
```

Profiler and target must use the same Python minor version and standard versus
free-threaded build; prereleases must also match exactly. Linux may require a
per-process ptrace authorization. The qualification worker authorizes only its
parent and descendants when Yama is enabled; it never changes global policy.
Other platforms retain their normal process-inspection requirements.

Python 3.15's default frame pointers make `perf` useful when Python stacks
point into TLS, SQLite or other native code. Native components still need
compatible unwind information. A Tachyon native marker alone does not identify
the expensive C function.

## JIT and free-threading experiments

Use separate environments and outputs for interpreter, JIT and free-threaded
builds. Record actual `sys._jit` and `sys._is_gil_enabled()` state rather than
inferring it from a requested flag. Warm-up and steady-state behavior both
matter. Free-threading does not make one asyncio loop execute on multiple cores
and does not grant thread safety to `AsyncClient`.

The independent benchmark repository selects builds with `--python`, freezes
their identity and dependency sets, and refuses incompatible campaign resumes.
Use it for public cross-client evidence and new 3.14/3.15 network campaigns.
Keep new syntax or builtins out of shared runtime paths that must parse on 3.11.
Audit explicit text encodings and deprecated event-loop policies during upgrades;
MQTT wire topics and payload handling retain their explicit encodings.

References: [Tachyon documentation](https://docs.python.org/3.15/library/profiling.sampling.html)
and [Python 3.15 migration notes](https://docs.python.org/3.15/whatsnew/3.15.html).
