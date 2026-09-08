# ASLR / asyncio receive-allocation ablation

Experimental notes only. This branch is not a release decision.

Observed external trigger: under normal ASLR on the Raspberry Pi benchmark, MQTTium can enter a persistent higher-latency regime; the same workload with worker ASLR disabled (`setarch -R`) did not reproduce that regime in the causal-control campaign. Historical diagnosis also associated the slow layout with extra minor faults around asyncio receive allocation.

Python 3.14's selector transport uses a 256 KiB `max_size` for `socket.recv()`. MQTTium then receives through `StreamReader` and copies into its own incremental decoder buffer. This branch tests one minimal ablation: cap the selector transport's per-read allocation to 64 KiB when that private CPython knob is present, while preserving normal ASLR and all MQTTium batching/decoder semantics.

The cap is deliberately an experiment, not yet a claimed fix. Acceptance requires an interleaved ARM campaign under normal ASLR showing both:

- materially lower `ru_minflt` per message and disappearance or strong reduction of the slow-layout probability; and
- no material regression in representative throughput/RTT distributions.

If the ablation confirms causality but costs throughput, the structural follow-up is a `BufferedProtocol` receive path with a reusable MQTTium-owned buffer, avoiding the repeated selector `recv()` allocation rather than merely shrinking it.
