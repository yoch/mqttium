# Stable-release evidence — 2026-08-05

Evidence commit: `0006198de800228c1d1b92790f56e074d791608d` (`main` after PR #31).

## Retained runs

- CI: https://github.com/yoch/mqttium/actions/runs/31032604795
- Full finalization campaign: https://github.com/yoch/mqttium/actions/runs/31032603265
- Full benchmark workflow: https://github.com/yoch/mqttium/actions/runs/31029903048
- Paired micro and network regression: https://github.com/yoch/mqttium/actions/runs/31029903776

The finalization matrix completed successfully with 20 cycles, 500 QoS 1 messages per
cycle and 19 forced reconnects per protocol scenario.

| Platform / broker | Protocol | Published | Received | Idle violations | Artifact digest |
|---|---:|---:|---:|---:|---|
| Ubuntu 24.04 / Mosquitto | 3.1.1 | 10,000 | 10,000 | 0 | `sha256:a1c77760fb1c40fb1c320baacc12880464a942d389a5252e7ce6dc33d716c214` |
| Ubuntu 24.04 / Mosquitto | 5 | 10,000 | 10,000 | 0 | `sha256:ae69dfbd4ffd379327c2e76886123109c47fae7ec98e12f8e7508926dd356f32` |
| macOS 15 / Mosquitto | 3.1.1 | 10,000 | 10,000 | 0 | `sha256:58a4ae864b19fea9c0586db169aa1eb0e77abaea710279ba3ae291c110d98345` |
| macOS 15 / Mosquitto | 5 | 10,000 | 10,000 | 0 | `sha256:e311b18302ae3fc98df0120d32c667ca8ff390ce42ff3f122521dd57ed5d8e97` |
| EMQX 5.8.9 | 3.1.1 and 5 | 20,000 | 20,000 | 0 | `sha256:85f20de29c55a1cd224ed521d30222f987d3ebb486036878b7359c26775e28e2` |
| HiveMQ CE 2026.5 | 3.1.1 and 5 | 20,000 | 20,000 | 0 | `sha256:91a4d930246b2cd39e124b10e2e5ae896afaf290aa29c43c83f9aa1d3e3b155f` |

Every retained JSON result reports:

- exact subscriber-confirmed delivery equality;
- 19 successful forced reconnects;
- an empty `publisher_idle_violations` list;
- non-zero protocol, writer and decoder high-water values, proving that the bounded
  paths were exercised rather than bypassed.

The benchmark and paired-regression runs cover the final PR #31 tree that was squash-merged
into the evidence commit. Their throughput, latency, persistence, memory and packaging gates
all passed.

The soak artifacts expire on 2026-09-04. Their digests and run identifiers are retained here
so the release decision remains auditable after artifact expiry.
