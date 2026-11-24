# The performance gate, its tools, and two decisions of lot 5

Reports 0044 and 0045 measured lot 5 of `refactor/deslop-layers` with
scratch scripts and a gate that differs from the one the plan named. This
report records what the owner of the repository decided about both, on
24 November 2026, and the tools now committed under `tools/`. It changes no
code of the broker.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`, on top of `047c68f`.
- **Host**: Linux x86-64 container, 4 cores.

## The decisions

- **The gate.** The plan's gate was the median throughput, 5 % at most
  lower. With the same code on both sides that throughput moved by up to
  7 % (report 0044). The owner accepted the statistic reports 0044 and 0045
  used instead, for the lots that follow: the broker CPU time per delivered
  message, cores pinned, as the median over passes of B/A, threshold 1.05,
  on `dist` builds. A deciding shape above the threshold over six passes is
  played again alone over twelve, which decide. The gate resolves about
  4 % on QoS 0 bursts with one subscriber (report 0045). A median between
  1.03 and the threshold is followed by an instruction count, which does
  not decide.
- **The delivery table lends its rows.** `Deliveries` (lot 5) is closed to
  writes, but lends its rows read only (`rows`, 26 places in
  `owner::pubsub`) and the rows of a session (`of_session`). Written as a
  plain getter, `rows` is reported as a field leak (report 0045). The owner
  chose to keep read-only lending as the interface of the table. The header
  of `.deslop-please.allow` says so.

## The tools

All in `tools/`, standard library only.

| Tool | What it does |
| - | - |
| `measure_qos0_delivery.py` | As before (record 0063, report 0038), with one more field: `broker_cpu_us_per_delivery`, user plus system CPU time of the broker from `/proc/<pid>/stat`, read before it stops, over the deliveries received. |
| `measure_qos1_pairs.py` | Pairs of publisher and subscriber, one topic per pair, QoS 1, eight publications in flight per publisher (the Receive Maximum the broker announces), on the volatile store by default. Prints the acknowledged publications per second and the same CPU field. |
| `run_perf_gate.py` | The gate: two executables, A and B, alternated within each pass, broker on cores 0–1 and clients on 2–3 through `taskset`; four deciding shapes (QoS 0 bursts with 1 and 8 subscribers, QoS 1 with 1 and 8 pairs) and two reported (`blast`); the rule above; exit status 1 when a deciding shape fails. |
| `count_broker_instructions.sh` | The broker under `valgrind --tool=callgrind` on QoS 0 bursts with one subscriber; prints the instructions retired per play, start and end included. |
| `test_perf_gate.py` | Unit tests: the median of ratios, the rule, the bytes of the QoS 1 measure's PUBLISH and PUBACK, the variable byte integer, the packet reader, and the CPU reading in `/proc`. No broker. |

The `contracts` job of the CI runs `test_perf_gate.py`. The gate itself is
played by hand on a quiet host: it takes about fifty minutes for six
passes.

## Commands run

From `mqttiumd/`:

| Command | Result |
| - | - |
| `python3 tools/test_perf_gate.py` | 12 tests, OK |
| `python3 -I tools/measure_qos0_delivery.py --binary <dist 10a87fc> --messages 20000` | 20 000 of 20 000 received, `broker_cpu_us_per_delivery` 6.5 |
| `python3 -I tools/measure_qos1_pairs.py --binary <dist 10a87fc> --pairs 2 --messages 5000` | 10 000 received, `broker_cpu_us_per_delivery` 55.0 |
| `tools/count_broker_instructions.sh <dist 10a87fc> 1 5000` | one total, 139.7 M instructions |
| `python3 -I tools/run_perf_gate.py <dist 9b7c05d> <dist 10a87fc> --passes 1 --only "qos0 bursts x1" --only "qos1 pairs x1"` | exit 0; 1.011 and 0.959 |

One pass decides nothing; these runs check that the tools work.

## Not run, not shown

- A full gate with the committed tools: the figures of reports 0044 and
  0045 come from the scratch scripts these tools replace, with the same
  method and the same shapes.
