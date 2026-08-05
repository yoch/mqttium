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
4. waits for protocol admission, flow slots, writer bytes/messages, pending
   effects and receipts to return to zero;
5. closes the publisher transport and confirms automatic reconnection before
   the next cycle.

The harness writes a JSON result containing workload totals, elapsed time,
forced reconnect count and high-water statistics. A single command can be used
against any externally managed broker:

```bash
PYTHONPATH=src python benchmarks/soak.py \
  --host 127.0.0.1 \
  --port 1883 \
  --protocol 5 \
  --cycles 20 \
  --messages-per-cycle 500 \
  --output /tmp/mqttium-soak.json
```

## Workflow coverage

`.github/workflows/finalization.yml` provides:

- a short Mosquitto run on pull requests for MQTT 3.1.1 and MQTT 5;
- manually configurable Mosquitto runs on Ubuntu and macOS;
- manually triggered interoperability runs against pinned EMQX and HiveMQ
  Community Edition images;
- retained JSON artefacts and broker logs.

The workflow is intentionally separate from normal CI so the permanent unit,
integration, packaging and fuzzing gates remain fast. Extended macOS and
multi-broker campaigns run only through `workflow_dispatch`.

## Retained evidence

The campaign run against `main` at
`0006198de800228c1d1b92790f56e074d791608d` is recorded in
[`STABLE-RELEASE-EVIDENCE-2026-08-05.md`](STABLE-RELEASE-EVIDENCE-2026-08-05.md).
It retains the run URLs, workload totals and artifact digests for Linux, macOS,
Mosquitto, EMQX and HiveMQ under MQTT 3.1.1 and MQTT 5, together with the full
benchmark and paired-regression runs for the same final source tree.

## Acceptance criteria

A stable-release candidate requires retained successful runs showing:

- no lost subscriber-confirmed message in the configured workload;
- automatic reconnect after every forced transport closure;
- zero pending protocol messages/bytes, flow slots, writer entries, effects and
  receipts after each cycle drains;
- no task, descriptor or queue growth across cycles;
- success for MQTT 3.1.1 and MQTT 5 on Linux and macOS;
- success against Mosquitto and at least two independent broker
  implementations;
- no material regression in the paired micro and network benchmarks.

A workflow definition is not itself evidence. The successful run URLs and
artefact digests should be recorded in the release issue or release notes.

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
