# Broker throughput on one Linux host

## Reproduce the comparison

Use an optimized mqttiumd build and the pinned Mosquitto 2.0.21 release
source. The same libmosquitto 2.0.21 driver opens both sets of MQTT 5
connections with TLS 1.3, certificates from the same local CA and
verified server identity. It subscribes first, then paces a fixed
256-byte payload with a unique sequence number per publication.

```sh
cd mqttiumd
cargo build --release --locked -p mqttiumd --no-default-features
sh preview/build-mosquitto-reference.sh /tmp/load-reference
python3 tools/run_broker_load.py \
  --mqttiumd target/release/mqttiumd --source "$(git rev-parse HEAD)" \
  --reference /tmp/load-reference/mosquitto-2.0.21 \
  --scratch /dev/shm --out /tmp/broker-load --duration-ms 1800 --repeats 3
python3 tools/plot_broker_load.py /tmp/broker-load/matrix.json --out /tmp/broker-load.png
```

Run as a regular Linux user with cgroup v2, procfs, `taskset`, OpenSSL,
a C compiler and Python 3.11+. The optional plot takes Matplotlib. Each
run uses fresh temporary broker stores and fresh test credentials. A
different `--scratch` path tests another filesystem. A 1.8-second
publication window is followed by a fixed 500 ms receive drain. Rates
are exploratory measurements.

With four disjoint pairs the broker affinity varies across 1, 2 and 4
logical CPUs within the effective quota. The client count varies across 1,
4 and 8 pairs at two broker CPUs. A pair has one subscriber and one
publisher and owns a topic. The cap is 16 simultaneous connections. The
generator stays on the same logical CPU across the matrix. Broker and
generator share the ancestor cgroup quota. The run records actual broker
affinity, generator affinity and cgroup throttling alongside the results.

Each case retains its message IDs, monotonic schedule, submission, PUBACK
and subscriber observation times, reasons, and copy counts in
`messages.csv.gz`. `process-samples.jsonl.gz` retains procfs CPU time,
run queue time, the wait channel and RSS. Throughput counts unique arrivals
within the publication window. The second count includes the drain. Tail
observations stay identifiable. A generator window limit or a missed
scheduled slot is recorded separately from a negative PUBACK. A client
submission timestamp includes any later queueing inside libmosquitto.

## Persistence and accounting

QoS 0 uses ordinary, non-retained traffic and live subscribers. QoS 1
uses persistent sessions, subscriptions at QoS 1 and non-retained
messages. On mqttiumd, a successful PUBACK follows the durable admission
transaction, including the matching recipient obligations. The readiness
record confirms SQLite WAL with `synchronous=2` (FULL). A later
subscriber observation is a separate timestamp. The QoS 1 result counts
omitted, negative and positive PUBACKs separately.

Mosquitto persists by writing a snapshot. The tested configuration enables
persistence with a 3,600-second autosave interval and saves on clean
shutdown. A positive PUBACK acknowledges broker processing and queueing
under that configuration. It gives no per-publication proof that the
snapshot contains the message. The comparison uses the actual guarantees
of each binary. Temporary stores in this reception were on tmpfs, so
SQLite's commit mode provides no measurement of disk flush latency or
survival through host power loss. mqttiumd's mandatory product event log
and Mosquitto's information, warning and error logs were enabled.

The repeated points are the highest scheduled rate with at least 99%
eventual delivery and the first rate above it in that sweep, when
available. Raw rates and dispersion are preserved. The 99% rule selects
repeats and never gates CI.

## Baseline measurement

The loopback tests ran on a KVM x86_64 VM with five logical CPUs on an AMD
EPYC 9V74, a four-CPU cgroup budget (`cpu.max=400000 100000`) and tmpfs
stores. The broker used CPU 0, CPUs 0–1 or CPUs 0–3. The generator stayed
on CPU 4. No cgroup throttling was observed in the 236 measured cases.

| Executable | Source | SHA-256 |
|---|---|---|
| mqttiumd, release, no default features | `942aa5bd739bec38032875af44bdd80beaf42629` | `1e23218ed5ab46f0f91220cf63687245f9530af8be6921ffe6ce16c9fde5509a` |
| Mosquitto, optimized `-O2` | v2.0.21, source TAR `7ad5e84caeb8d2bb6ed0c04614b2a7042def961af82d87f688ba33db857b899d` | `d567c7e9d8b000649f50f83d6815f98dfed2dcd74cfce547d2d94701bf764c33` |
| libmosquitto shared by both runs | v2.0.21 | `e70a61094303e000084bb63641512fdb8d730e74a09891079c292d9247341fff` |

With four pairs, these counts are arrival rates inside the 1.8-second
send window. Each cell gives the median and min–max of three trials.

| Broker | QoS | Offered/s | 1 CPU | 2 CPUs | 4 CPUs | Gain at 2 / 4 CPUs over 1 |
|---|---:|---:|---:|---:|---:|---:|
| mqttiumd | 0 | 1,500 | 848 [837–873] | 869 [848–879] | 854 [848–865] | 1.03× / 1.01× |
| mqttiumd | 1 | 800 | 794 [792–795] | 796 [795–796] | 796 [796–797] | 1.00× / 1.00× |
| mqttiumd | 1 | 1,500 | 513 [494–571] | 936 [851–946] | 828 [774–843] | 1.83× / 1.61× |
| Mosquitto | 0 | 150,000 | 121,079 [94,687–134,231] | 119,166 [112,157–122,146] | 132,871 [89,733–141,507] | 0.98× / 1.10× |
| Mosquitto | 1 | 48,000 | 46,595 [44,204–47,491] | 47,321 [44,247–47,893] | 47,427 [47,297–47,859] | 1.02× / 1.02× |

At two CPUs, increasing the mqttiumd client count from 1 to 4 to 8 pairs
yielded 847, 869 and 870/s at 1,500 QoS 0/s, with three trials per cell.
At 800 QoS 1/s it yielded 377/s (one trial), 796/s and 796/s. Four pairs
received all 1,440 messages within the drain in each of the three QoS 1
repetitions at this load. At 1,500 QoS 1/s, even eight pairs were
lossy at the admission boundary (429–1,331 receipts/s within the window).
Increasing connection count therefore helps keep durable work in flight,
then hits a shared limit. A peak at a load with persistent rejections is
unsuitable as a steady-state target.

The following latencies are milliseconds from submission. Each percentile
is the median of its per-trial percentile at two broker CPUs and four
pairs. A received message or a positive PUBACK contributes only to its
own distribution.

| Broker | QoS | Offered/s | Subscriber p50 / p95 / p99 | Positive PUBACK p50 / p95 / p99 | Generator lag p99 |
|---|---:|---:|---:|---:|---:|
| mqttiumd | 0 | 1,500 | 146.78 / 149.90 / 150.73 | — | 0.95 |
| mqttiumd | 1 | 800 | 9.71 / 14.67 / 18.15 | 3.45 / 5.21 / 6.18 | 0.30 |
| mqttiumd | 1 | 1,500 | 117.06 / 377.91 / 409.13 | 4.51 / 7.53 / 11.59 | 0.94 |
| Mosquitto | 0 | 96,000 | 0.07 / 25.07 / 59.26 | — | 12.95 |
| Mosquitto | 0 | 150,000 | 197.24 / 490.59 / 542.61 | — | 3.57 |
| Mosquitto | 1 | 48,000 | 0.34 / 35.46 / 42.26 | 0.12 / 0.29 / 1.99 | 3.82 |

At 1,500 QoS 0/s (`mqttiumd-q0-c2-p4-r1500-00`), the broker reported
1,044 drops. The subscriber counts found the same 1,044 absent IDs after
the drain and no duplicate. The main thread used 7.0% of one CPU, slept
in `hrtimer_nanosleep` at 32 of 35 samples and had traffic pending without
saturating a CPU. Peak RSS in the entire baseline was 10,472 KiB for
mqttiumd and 12,044 KiB for Mosquitto. In a separate traced trial at
3,500/s, 926 `clock_nanosleep(1 ms)` calls wholly inside the window used
1.039 s of the 1.8 s window (58%, median 1.087 ms per call). `strace`
perturbed throughput. Its sample diagnoses the waiting site.

In the first four-pair MQTT 5 QoS 1 trial at 1,500/s and two CPUs,
1,812 positive and 888 quota PUBACKs (`0x97`) accounted for all 2,700
submissions. The 1,812 positive acknowledgments all had a corresponding
delivery by the end of the drain. The SQLite writer used 87–92% of a CPU
across the three trials. Local submissions, terminal PUBACKs, unavailable
connections and full client send windows each have their own counters.
The raw matrix records 15 QoS 0 cases at extreme rates where the process
refused with `core_input_budget` and exited 2. Treat those cases as
overload failures.

At Mosquitto's 150,000/s QoS 0 point, its broker thread was at 99–100%
of one CPU and the single client driver used 92–95% (two broker CPUs).
At its 48,000/s QoS 1 point the driver was at 93–96%, with the broker
at 65–68%. The highest points therefore also exercise the generator's
limit. The observed rates are useful lower bounds for the test platform,
with no extrapolation of the Mosquitto QoS 1 ceiling under per-message
sync or a faster generator.

## Waking on mTLS work

The experiment replaces the reactor's post-dispatch sleep with a wait of
up to 1 ms. The TLS task signals after enqueuing an input or completing a
write. The reactor still polls the listener, store results and timers on
timeout, and all queue limits and turn order are preserved. The same
comparison can run with `--candidate PATH --candidate-source SHA` using
the baseline broker as `--mqttiumd`. The trial used two broker CPUs, four
pairs, five alternating repetitions and these settings.

```sh
python3 tools/run_broker_load.py \
  --mqttiumd "$baseline" --source 942aa5bd739bec38032875af44bdd80beaf42629 \
  --candidate "$wakeup" --candidate-source "$wakeup_source" \
  --reference /tmp/load-reference/mosquitto-2.0.21 --scratch /dev/shm \
  --out /tmp/load-wakeup --cores 2 --pairs 4 --repeats 5 --duration-ms 1800 \
  --rates-q0 0,900,1500,3500 --rates-q1 300,800,1500
```

The measured experiment executable was built from `942aa5bd` and the source
patch `5872c6d7287cc0c2abc270948731b3cbaf1a579de1fe66407056f607b4da94f8`.
Its SHA-256 is `06700d496f98245674dcb075d411566d3430a1b4ea249dd0eaaa853e8f45db99`.
The evidence bundle includes that patch. The release packages and their
post-extraction tests have their own binary and source identities.

| QoS | Offered/s | Fixed sleep, received/s | Wake on work, received/s | Throughput ratio | Subscriber p99 before / after |
|---|---:|---:|---:|---:|---:|
| 0 | 900 | 884 [821–891] | 899 [899–899] | 1.02× | 33.22 / 6.32 ms |
| 0 | 1,500 | 884 [817–886] | 1,499 [1,497–1,499] | 1.70× | 159.84 / 5.59 ms |
| 0 | 3,500 | 862 [843–882] | 3,438 [3,251–3,454] | 3.99× | 163.72 / 54.98 ms |
| 1 | 800 | 796 [792–797] | 798 [787–799] | 1.00× | 28.71 / 25.92 ms |
| 1 | 1,500 | 842 [749–903] | 1,001 [848–1,226] | 1.19× | 304.38 / 223.49 ms |

With eight idle connections the median total broker CPU was 8.1% of one
CPU with sleep and 7.8% with wakeups (ranges 7.5–9.7% and 7.3–12.5%).
Peak sampled RSS was 9,448 KiB and 9,384 KiB. At 3,500 QoS 0/s,
the minimum count in a single subscriber window across all trials rose
from 41 to 1,381 of 1,575 scheduled per pair. At 800 QoS 1/s both variants
acknowledged and delivered all 7,200 events across five trials. The
wakeup variant used more CPU here (median 108% of one CPU versus 91%),
with subscriber p99 as high as 83.87 ms (52.10 ms with sleep). At the
overloaded 1,500/s point, the broker writer reached 93.5–97.2% of one CPU.
Quota acknowledgments remained visible (`0x97`, 2,979 in the five trials).
The wakeup addresses the QoS 0 wait. Durable work remains bound to a
single writer and requires its own measurements and optimization.

The separate `--expiry-witness` run used three alternating 4.2-second
trials per binary at 3,500 QoS 0/s. An independent Mosquitto client first
confirmed PUBACK for a retained message with a two-second expiry. A
read-only SQLite observer saw the row after PUBACK and its physical
removal during the traffic window in all six cases. The measured time
from PUBACK observation to the first absent sample was 2.07–2.11 s for
the wakeup trial and 2.08–2.14 s for the baseline. The observer sampled
at roughly 50 ms intervals. This result exercises expiry under load.

A separate affinity sweep of the wakeup trial gave these four-pair
results. Each cell gives median and min–max over three repetitions.

| QoS | Offered/s | 1 CPU | 2 CPUs | 4 CPUs | Gain at 2 / 4 CPUs over 1 |
|---|---:|---:|---:|---:|---:|
| 0 | 3,500 | 3,491 [3,481–3,494] | 3,485 [3,473–3,486] | 3,497 [3,494–3,498] | 1.00× / 1.00× |
| 0 | 5,500 | 4,515 [4,415–4,558] | 3,662 [3,644–3,978] | 4,546 [4,439–4,773] | 0.81× / 1.01× |
| 1 | 800 | 796 [796–798] | 797 [757–798] | 798 [43–799] | 1.00× / 1.00× |
| 1 | 1,500 | 774 [773–788] | 1,004 [780–1,219] | 1,109 [482–1,276] | 1.30× / 1.43× |

At 800 QoS 1/s, one two-CPU wakeup trial closed three connections and
one four-CPU trial closed all four publisher connections. The latter
stopped at 98 local submissions out of 1,440 scheduled, 45 positive
PUBACKs and 77 deliveries in the receive window. All 45 positively
acknowledged IDs were received. The comparison below investigates the
connection loss at this point.

In five additional alternating trials per binary at four CPUs and
800 QoS 1/s, both executables delivered all 7,200 publications after the
drain with no connection loss. At 1,500/s, the same campaign observed six
baseline and two trial connections closed with `outgoing_queue_full`.
The four-slot write queue filled while submitting a control frame. This
is an existing failure mode under pressure. The scaling sweep recorded
`effect_retained` in the 800/s failures. This event locates a full output
queue. The logs available from that sweep do not attribute the transient
rate of filling to a specific caller or system scheduling event.

## Decision

The candidate keeps the fixed wait. The wakeup delivered a large QoS 0
gain and passed the expiry witness. Its two severe QoS 1 outliers at 800/s
leave progress at a previously clean load unqualified. All positively
acknowledged IDs in those outliers arrived. The short trial offers no
durability conclusion beyond that observation.

The first demonstrated QoS 0 bottleneck is polling between reactor
turns. Adding cores left that ceiling at about 850/s in the baseline.
Before enabling the wakeup, make control-frame pressure defer work within
the existing bounded output queues and check progress of each connection
through a burst. At sustained QoS 1 load the next investment is in the
single SQLite writer. Measure transaction and receipt work independently
and evaluate bounded batching while keeping ACK after durable commit and
`synchronous=FULL`. An additional two CPUs alone did not produce a
reliable gain once the writer had its own CPU budget. None of these
measurements set CI thresholds.
