from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one occurrence, found {count}: {old!r}")
    return text.replace(old, new, 1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--harness", type=Path, required=True)
    args = parser.parse_args()
    candidate = args.candidate
    harness = args.harness

    for name in (
        "native_rtt_ingress.py",
        "direct_ingress_hotpath.py",
        "paired_native_hotpaths.py",
    ):
        shutil.copy2(harness / "benchmarks" / name, candidate / "benchmarks" / name)

    changelog = candidate / "CHANGELOG.md"
    text = changelog.read_text()
    text = replace_once(
        text,
        "### Changed\n\n",
        "### Changed\n\n"
        "- Consecutive small QoS 0 `MESSAGE` effects are now transferred to the\n"
        "  iterator/callback delivery queues in one `EffectPump` pass. The normal\n"
        "  scheduled-flush boundary, callback worker, queue limits and delivery\n"
        "  ordering remain unchanged; QoS 1, QoS 2, manual acknowledgement, large\n"
        "  messages and exact byte accounting keep the existing path. A controlled\n"
        "  decode-to-callback A/B improved median telemetry ingress by 5.5%, while\n"
        "  paired QoS 1 application RTT remained neutral.\n",
    )
    changelog.write_text(text)

    benchmark_workflow = candidate / ".github/workflows/benchmarks.yml"
    text = benchmark_workflow.read_text()
    marker = (
        "      - name: Application delivery and persistence stress\n"
        "        run: python benchmarks/application_stress.py --output /tmp/mqttium-bench/application-stress.json\n"
    )
    addition = marker + (
        "      - name: Native QoS 1 RTT profile\n"
        "        run: |\n"
        "          python benchmarks/native_rtt_ingress.py \\\n"
        "            --scenario rtt --runs 3 --rtt-pairs 8000 \\\n"
        "            --label \"${GITHUB_SHA}\" \\\n"
        "            --output /tmp/mqttium-bench/native-rtt.json\n"
        "      - name: Direct QoS 0 ingress profile\n"
        "        run: |\n"
        "          python benchmarks/direct_ingress_hotpath.py \\\n"
        "            --runs 3 --messages 160000 \\\n"
        "            --label \"${GITHUB_SHA}\" \\\n"
        "            --output /tmp/mqttium-bench/direct-ingress.json\n"
    )
    benchmark_workflow.write_text(replace_once(text, marker, addition))

    paired_workflow = candidate / ".github/workflows/paired-regression.yml"
    text = paired_workflow.read_text()
    marker = "      - name: Upload paired measurements\n"
    addition = (
        "      - name: Paired native RTT and ingress hotpaths\n"
        "        run: |\n"
        "          python candidate/benchmarks/paired_native_hotpaths.py \\\n"
        "            --base-root base \\\n"
        "            --candidate-root candidate \\\n"
        "            --repeat 3 \\\n"
        "            --rtt-pairs 8000 \\\n"
        "            --ingress-messages 120000 \\\n"
        "            --minimum-ratio 0.97 \\\n"
        "            --output /tmp/mqttium-paired/native-hotpaths.json\n"
        + marker
    )
    paired_workflow.write_text(replace_once(text, marker, addition))

    docs = candidate / "docs/NATIVE-HOTPATH-PROFILE.md"
    docs.write_text(
        '''# Native RTT and ingress hot-path profile

## Scope

An external multi-library benchmark reported two possible MQTTium 0.1.0a3 gaps
under MQTT 3.1.1:

- application QoS 1 round-trip capacity around 5,747 request/response pairs per
  second, below Paho and gmqtt in that harness;
- exact-topic QoS 0 telemetry ingress around 27,963 messages per second, about
  6% below gmqtt and awscrt.

The investigation reproduced the two shapes with native `AsyncClient` clients,
not through the compatibility facade or a sync-to-loop bridge. Measurements use
rotated base/candidate order and fresh interpreters. Absolute hosted-runner
throughput varies, so patch decisions use paired ratios and logical counters.

## Findings, ranked

### 1. The reported RTT result is stale relative to current `main`

The first control run measured current `main` at about 7,786 QoS 1 pairs/s on the
same 32-outstanding native scenario. That is roughly 35% above the reported a3
value before any new patch. Existing post-a3 work on owner-local protocol paths,
effect partitioning and outbound frame retention already closed most or all of
the historical gap on this runner.

### 2. QoS 0 ingress paid per-message async dispatch inside an existing batch

The reader already decodes up to 256 packets, collects their effects and waits for
one scheduled `EffectPump` flush before reading the next batch. Within that flush,
however, every small QoS 0 `MESSAGE` was still dispatched through the individual
async `_apply_effect()` path even when iterator/callback queues had immediate
capacity and no exact byte accounting was required.

The retained change consumes consecutive eligible QoS 0 messages in one pump
pass. It does not remove the flush boundary or execute user callbacks inline.

Final five-cycle ABBA result, 160,000 telemetry256 messages per sample:

| Metric | Base | Candidate | Change |
| --- | ---: | ---: | ---: |
| Median direct ingress | 73,640 msg/s | 77,664 msg/s | +5.53% |
| Paired cycle range | — | — | +3.75% to +6.99% |
| Effect-pump suspensions | 693 | 693 | unchanged |

Because suspensions are unchanged, the gain is attributable to less per-message
coroutine/dispatch work inside the batch, not to weaker backpressure or a larger
reader batch.

### 3. The scheduled flush boundary is useful for QoS 1 RTT

An experiment that applied small messages before the scheduled flush removed
thousands of suspensions, but reduced direct ingress and increased writer batch
count under RTT. The yield was also a coalescing boundary for PUBACK and responder
PUBLISH work. That experiment was rejected.

The retained QoS 0-only batch hook is invoked only when the next pending effect is
a `MESSAGE`, and returns immediately for non-QoS0 messages. Final QoS 1 ABBA
results were neutral:

| Metric | Base | Candidate | Change |
| --- | ---: | ---: | ---: |
| Median RTT capacity | 6,266 pairs/s | 6,277 pairs/s | -0.23% paired median |
| p50 latency | 4.637 ms | 4.663 ms | +0.56% |
| p95 latency | 5.757 ms | 5.718 ms | -0.67% |
| Combined writer batches | 6,131 | 6,101 | no increase |

One noisy cycle was slower while the other four were within or above baseline;
the paired median is inside the established runner noise. The permanent gate
allows at most a 3% median regression.

### 4. Broker-fed local ingress can be source-limited

A Mosquitto scenario with 32 preconnected raw publishers offered roughly
57-60k msg/s and the subscriber drained nearly at that rate. It was useful for
end-to-end correctness, but not for measuring the remaining subscriber ceiling.
The retained direct transport benchmark therefore performs the real
CONNECT/SUBSCRIBE/read/decode/effect/callback path while removing producer and
broker throughput as confounders.

### 5. Other hypotheses were not supported strongly enough to patch

- TCP_NODELAY was already enabled by `TcpTransport`.
- The default outbound inflight window was not limiting the 32-outstanding RTT
  scenario.
- Skipping the redundant auto-acknowledged QoS 1 delivery persistence mark alone
  measured -0.28% RTT and -0.22% ingress, entirely within noise.
- Eager `PublishReceipt` event allocation remains a plausible QoS 1 cost, but no
  patch was retained without isolated evidence and a clear lifecycle contract.

## Retained implementation

Only consecutive messages satisfying all of these conditions use the batch path:

- effect kind is `MESSAGE` and QoS is 0;
- the message qualifies for the existing zero-accounting small-message path;
- every selected destination queue has immediate capacity;
- the connection epoch still matches.

The pump stops at the first ineligible effect and resumes the established async
path. Iterator and callback destinations are checked before either is mutated,
so `message_delivery="both"` cannot be half-delivered. Callback jobs are queued
in order and still execute on the callback worker.

## Risks and mitigations

- **Ordering:** effects are consumed from the existing deque in original order;
  the batch stops rather than skipping an ineligible message.
- **Backpressure:** full queues fall back to the existing awaited path. Reader
  batching and its suspension boundary are unchanged.
- **Memory accounting:** property-bearing, large and exact-accounted messages do
  not use the new path.
- **QoS semantics:** QoS 1, QoS 2 and manual acknowledgement are deliberately
  outside the optimization.
- **Callback behavior:** user callbacks are not called inline and retain the
  same exception/reporting worker.
- **Regression risk:** the paired workflow checks both direct ingress and native
  QoS 1 RTT with rotated base/candidate samples and rejects a median ratio below
  0.97.

## Reproduction

Candidate-only profiles:

```bash
python benchmarks/native_rtt_ingress.py \\
  --scenario rtt --runs 3 --rtt-pairs 8000 \\
  --label local --output /tmp/native-rtt.json

python benchmarks/direct_ingress_hotpath.py \\
  --runs 3 --messages 160000 \\
  --label local --output /tmp/direct-ingress.json
```

Paired source checkouts:

```bash
python benchmarks/paired_native_hotpaths.py \\
  --base-root ../base --candidate-root . \\
  --output /tmp/native-hotpaths.json
```

The final experimental A/B is retained in GitHub Actions run `31049968366`.
'''
    )


if __name__ == "__main__":
    main()
