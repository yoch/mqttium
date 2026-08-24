# Stability and interoperability campaign

The normal CI suite proves fast correctness. The finalisation campaign adds
longer-lived evidence for lifecycle behaviour that is difficult to validate in
small unit tests.

## Harness

`benchmarks/soak.py` runs a publisher and subscriber against a real broker. Each
cycle:

1. publishes a bounded QoS 1 batch;
2. waits for every receipt;
3. confirms subscriber delivery;
4. waits for protocol admission and packet identifiers, inbound replay and byte
   accounting, delivery/writer/effect waiters, queues, flow slots, and every
   receipt class to return to zero;
5. closes the publisher transport and confirms automatic reconnection before
   the next cycle.

The harness writes a JSON result containing workload totals, elapsed time,
forced reconnect count, RSS/USS/PSS and tracemalloc samples, tasks, threads,
descriptors, delivery queues, high-water statistics and the final drained
state. Discrete resource growth, loss/duplication, missed reconnect or a
non-drained counter fails the run; RSS trends remain diagnostic while the
versioned tracemalloc thresholds are the memory gate. A single command can be
used against any externally managed broker:

```bash
PYTHONPATH=src python benchmarks/soak.py \
  --host 127.0.0.1 \
  --port 1883 \
  --protocol 5 \
  --cycles 20 \
  --messages-per-cycle 500 \
  --output /tmp/mqttium-soak.json
```

## Release coverage

`python benchmarks/local_release.py rc --base-ref <approved-baseline>` provides:

- memory, application stress and exact hot-path call/allocation profiles;
- local unit, type, lint, security and mandatory broker integration gates;
- short resource-aware reconnect soaks for MQTT 3.1.1 and MQTT 5;
- strict local micro/open-loop performance gates on an eligible machine, plus
  an advisory closed-loop network diagnostic;
- isolated wheel TCP, TLS, WebSocket, Unix, SQLite, Paho VERSION2 and shutdown
  smokes;
- manifests, JSON artefacts and broker logs retained outside the repository.

After the source tree and local manifest are final, the GitHub matrix validates
Python 3.11–3.14 and interoperability with EMQX and HiveMQ. Those environment-
specific checks are release evidence for portability, but GitHub performance
numbers remain advisory.

Installed-artifact workflows separately install the exact PyPI artifact rather
than the checkout. Their retained matrix covers wheel/sdist metadata and Stable
imports on Python 3.11–3.14, TCP and TLS broker round trips, SQLite restart,
WebSocket and Unix transports, the Paho VERSION2 migration subset, cancellation
and clean shutdown. They are manually dispatchable for every candidate.

Multi-hour deterministic fuzz and soak campaigns are required release evidence.
Record their exact source commit, configuration, retained artifacts, and outcome.

## Retained evidence

The campaign run against `main` at
`0006198de800228c1d1b92790f56e074d791608d` is recorded in
the [2026-08-05 historical campaign record](https://github.com/yoch/mqttium/blob/main/docs/reports/STABLE-RELEASE-EVIDENCE-2026-08-05.md).
It retains the run URLs, workload totals and artifact digests for Linux, macOS,
Mosquitto, EMQX and HiveMQ under MQTT 3.1.1 and MQTT 5, together with the full
benchmark and paired-regression runs for the same final source tree.

## Acceptance criteria

The release campaign requires retained successful runs showing:

- no lost subscriber-confirmed message in the configured workload;
- automatic reconnect after every forced transport closure;
- zero pending protocol messages/bytes, flow slots, writer entries, effects and
  receipts after each cycle drains;
- no task, descriptor or queue growth across cycles;
- success for MQTT 3.1.1 and MQTT 5 locally, followed by the GitHub version and
  broker interoperability matrix;
- no material regression in the paired micro and network benchmarks.

A runner definition is not itself evidence. The successful local manifest and
artifact digests must be recorded in the release evidence report.

## Failure triage

When a soak fails, retain:

- the JSON result when one was produced;
- broker logs;
- the failing commit SHA and workflow run ID;
- protocol version and broker image/tag;
- the final `ClientStats` snapshot or idle violations.

Do not increase timeouts until the retained counters establish whether the
failure is slow progress, a stalled queue, a reconnect-policy decision or a
broker interoperability difference.
