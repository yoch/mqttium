# RC17 long-campaign closure — 2026-09-27

## Decision

The long-duration evidence condition G01 from the
[initial stable-promotion audit](RC17-STABLE-PROMOTION-AUDIT-2026-09-27.md)
is now satisfied for `c9bab1ad93dd2e875c07706aecdeaa69eb8dce89`
(`v1.0.0rc17`). Both campaigns completed successfully; their retained
archives were downloaded and their SHA-256 values verified against GitHub.
This closes the pending campaign condition, not the whole stable-promotion
review. The initial report remains an unchanged historical checkpoint.

Still required before final 1.0: the disposition of
[issue #493](https://github.com/yoch/mqttium/issues/493) (confirmed open at this
checkpoint), real-application migration evidence, correction of the four
reported documentation findings, and qualification of the exact final-release
commit and artifacts. No new runtime failure was reported by these campaigns.

## Long fuzz

[Run 36293924785](https://github.com/yoch/mqttium/actions/runs/36293924785)
completed successfully at 09:15:07 UTC. All five shards ran the
`rc-24-cpu-hours` profile with base seed 17 and a 288-minute target each.
All metadata files name the exact RC17 commit.

The logs contain **370 successful deterministic batches of 100,000
iterations**, or **37,000,000 iterations** across codec, engine and WebSocket
targets, in addition to the Hypothesis runs and shard-zero stateful tests.
There are no retained failure files. Recorded campaign elapsed time totals
87,455 worker-seconds (24.29 aggregate worker-hours); this is
not a measurement of process CPU hours. Finishing the final batch can exceed
an individual shard's nominal duration.

| Shard | Starting seed | Completed batches | Recorded elapsed seconds |
| --- | --- | --- | --- |
| 0 | 17000000 | 83 | 17779 |
| 1 | 17200000 | 101 | 17613 |
| 2 | 17400000 | 62 | 17380 |
| 3 | 17600000 | 68 | 17320 |
| 4 | 17800000 | 56 | 17363 |

## Soak and interoperability

[Run 36293926073](https://github.com/yoch/mqttium/actions/runs/36293926073)
completed successfully at 06:19:03 UTC. Both Linux Mosquitto workloads ran
for the required two hours on the exact RC17 source.

| Protocol | Elapsed seconds | Published = received | Forced reconnects |
| --- | --- | --- | --- |
| MQTTv311 | 7200.47 | 8,703,000 | 17,405 |
| MQTTv5 | 7200.73 | 5,968,500 | 11,936 |

Both results report stable resources, no publisher idle violations and no
discrete resource violations. Published and received totals match in each
workload. RSS values remain diagnostic under the harness contract.

Both macOS protocol jobs and EMQX 5.8.9 / HiveMQ CE 2026.5 interoperability
jobs also passed. Each of their six protocol results reports 11,000 published
and received messages and no idle/discrete-resource violations. These are
short checks, not multi-hour runs. The `scheduled-resilience` job was skipped
as designed because this was a manual dispatch, not a scheduled run.

## Evidence retention

Archives and parsed results are retained under
`/tmp/mqttium-rc17-audit-20260927/`; the parsed inventory is
`completed-campaign-evidence.json`. Archive digests verified during this
follow-up are recorded below.

| Archive | SHA-256 |
| --- | --- |
| `fuzz-rc-24-cpu-hours-shard-3-36293924785` | `a1064e511e28f4bec0475588f8656852b484b4885596fe33870d2ce9a04e7834` |
| `fuzz-rc-24-cpu-hours-shard-0-36293924785` | `320b94436fdf3a9a55830ae869e0a574e5a9c85c7d62861ea95f7e83013b3058` |
| `fuzz-rc-24-cpu-hours-shard-1-36293924785` | `dcf2bb22a0389064bedf56e68268e39ac49f33510bf41e5b72e87016e06607ba` |
| `fuzz-rc-24-cpu-hours-shard-4-36293924785` | `5e1364b297c7af885cc7cd1acd7f014f30b1c178c33c791d659b140205a10cd2` |
| `fuzz-rc-24-cpu-hours-shard-2-36293924785` | `ea2193c91e972085ae1725582b2fe1108b4e2e447959b6afbab33df4ec56498b` |
| `soak-mosquitto-311-36293926073` | `940f8c8264549eacb0db57b79a97d60309e48ac4fe3fb9a4ee55f0c41ab3720c` |
| `soak-mosquitto-5-36293926073` | `0dfadc879162536ae5368f0e01629f7cf101230334f0d1f5e9a1517ebfc770d1` |
| `soak-macos-5-36293926073` | `b6d52bdee95dc296b36c4e64ae4cad17e8e1a1e6209c4528e686b722778666ee` |
| `soak-macos-311-36293926073` | `e7edc0e3d8464b4f61d1556624ff2a7c913162d5a79fe010f3b6cbfb32b86722` |
| `soak-emqx-5.8.9-36293926073` | `03a37354a46de933da315131c64dfd6ff725e8398a495d26073469812cdf62bf` |
| `soak-hivemq-2026.5-36293926073` | `5dcbb4ff348ae7ec776156825eab4240b122f6867af6ee85fd4effeeee1b85b1` |

## Local performance context

The maintainer clarified after the initial checkpoint that the local machine
was not idle. Local timings and throughput are diagnostic only and must not be
used to establish performance parity or regression. No additional local
performance measurements were run for this follow-up. Previously retained
controlled ARM64 comparisons keep their original scope and the #493 caveats;
they do not close that issue.
