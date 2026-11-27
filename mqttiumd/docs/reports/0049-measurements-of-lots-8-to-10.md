# Measurements of lots 8 to 10 and of the sharded mode

Branch `perf/lot10-measurements`, after report 0048. Reports 0039 to 0041
claim the performance of lots 8 to 10 (records 0064 to 0066). Report 0043
and the open conditions hold those figures as not established: single
runs, code changed since, a matrix generator that wrote past its buffer
(fixed by `675128d`), and Mosquitto 2.1.2 kept in memory against an
mqttiumd that synchronised a log. The owner chose to measure them again
before the lot that would take the sharded mode out of "experimental".
This report answers three questions: what each lot gained, what the
sharded mode gains and costs, and what baseline later optimisations
start from.

## Method

**Machine.** The 4-core x86-64 VM of reports 0043 to 0048: Intel Xeon at
2.10 GHz under KVM, 16 GB, kernel 6.18.44, ext4 on `/dev/vda`, tmpfs on
`/dev/shm`, rustc 1.97.0, gcc 13.3. The Raspberry Pi 5 of reports 0039 to
0041 cannot be reached from here: these figures check the direction and
the size of each claim, not its ARM64 value.

**Revisions.** One `dist` build each (`cargo build -p mqttiumd --profile
dist --locked`):

| Label | Commit | sha256 of the executable |
| - | - | - |
| lot 7 | `290db2be` | `a15cf595…27f9` |
| lot 8 | `0252fdc5` | `790b4458…4def` |
| lot 9 | `5e306904` | `789dbca8…fd25` |
| lot 10 | `9dfd568` | `1f1219d7…b596` |
| head | `40c0e8e` | `52c38b24…7d0e` |

**Reference.** Mosquitto 2.1.4 built from the archive that
`tests/compat/build-reference.sh` verifies (`ca30b26c…ea30`; executable
`6711a9c5…8b71`), with `persistence false`. It is compared with the
volatile mode only, where neither broker makes anything durable. The log
and durable modes are compared with themselves.

**Drivers.** The C drivers of `tools/arm64_bench/`, compiled on this
machine: `arm64_bench.c` for pairs (MQTT 5, 256-byte payloads, a 2 s
window then a drain) and `arm64_matrix_bench.c` for the matrix (the eight
scenarios of `run_matrix_bench.py`). The same drivers, from the head,
play every revision. `tools/measure_qos0_delivery.py` plays the QoS 0
shapes of report 0039; `tools/arm64_bench/idle_rss_bench.py` measures
idle memory.

**Campaigns.** `tools/arm64_bench/run_ab_campaign.py` plays every case on
every target in each pass, in reverse order on odd passes, six passes
per series, as the gate of report 0043 alternates its builds. Each cell
below is the median over the six passes, with the range when it matters.
The broker runs on CPUs 0 to 2 with `--io-threads 2`, the driver on CPU
3, as in reports 0039 to 0041. A load average under 0.6 and no other
process were checked before the first series; during a series the load
is the broker's and the driver's own (2 to 4.5). The fan-in sweep started
while the load average still counted the gate run before it (14.7);
without its first pass its medians say the same.

**Figures.** Deliveries per second and the publish-to-delivery p50 and
p99 come from the driver. The broker's CPU per delivery is the user and
system time it spent while the driver ran (`/proc/<pid>/stat`), divided
by the deliveries. Refusals are PUBACKs with a reason code of 0x80 or
more. An offered rate is the total publish rate a case asks for; a case
whose median meets its offered rate does not show a broker's limit.

## Lot 8 against lot 7 (record 0064, report 0039)

QoS 1 pairs, offered beyond saturation (4 pairs at 300,000/s, 32 at
200,000/s, 128 at 150,000/s), deliveries per second:

| Case | Lot 7 | Lot 8 | Change | Report 0039 | Verdict |
| - | - | - | - | - | - |
| 4 pairs, volatile | 87.3k (p50 0.31 ms) | 145.7k (0.18 ms) | +67 % | +107.5 % at 150,000/s offered (L95) | gain confirmed, smaller |
| 32 pairs, volatile | 81.7k (2.54 ms) | 140.5k (1.38 ms) | +72 % | +80.7 % (L97, compares two offered rates) | confirmed |
| 128 pairs, volatile | 68.2k (11.3 ms) | 100.9k (1.92 ms) | +48 % | 62,632/s, tied with Mosquitto (L100) | gain confirmed |
| 4 pairs, log, tmpfs | 64.7k | 102.0k | +58 % | 92,406/s (L102) | confirmed |
| 32 pairs, log, tmpfs | 76.6k | 118.3k | +54 % | 75,653/s at 150,000/s (L104) | confirmed |
| 128 pairs, log, tmpfs | 67.2k | 95.6k | +42 % | — | — |
| 32 pairs at 60,000/s, log, ext4 | 51.3k (p99 103 ms) | 55.8k (p99 13.7 ms) | +9 %, p99 ÷7.5 | 22,950 → 26,483/s with p50 397 ms (L79) | lot 8 better on both; the ARM64 latency regression is not seen here |
| 32 pairs at 100,000/s, volatile, mTLS | 78.0k (2.40 ms) | 99.0k (0.68 ms) | +27 % | 81,566/s, 1.89× Mosquitto (L107) | confirmed |

QoS 0 delivery, 100,000 messages of 64 bytes, broker on CPUs 0 to 2 and
the Python client on CPU 3, deliveries per second:

| Shape | Lot 7 | Lot 8 | Change | Report 0039 (L111–115) |
| - | - | - | - | - |
| Bursts of 100, 1 subscriber | 242k | 265k | +9.5 % | 334,726/s |
| Bursts of 1,000, 1 subscriber | 280k | 321k | +15 % | 410,558/s |
| Blast, 8 subscribers | 632k | 718k | +14 % | 593,873/s |

QoS 0 may be shed: three runs of the whole series lost some messages
(lot 7 28 and lot 8 424 of 100,000 in one run of bursts of 1,000, lot 9
482 in one blast); every other run received every message. Idle private
memory per connection (`Private_Dirty`, two runs each):

| Connections | Lot 7 | Lot 8 | Change | Report 0039 |
| - | - | - | - | - |
| 768, plain TCP | 9,768 B | 4,736 B | −52 % | 5,099 B (L119-135) |
| 400, mTLS | 21,100 B | 14,198 B | −33 % | 14,090 B |

Every claim of lot 8 holds in direction. Its gain on 4 pairs is smaller
on this machine than on the Pi (+67 % against +107.5 %); on 32 pairs it
is close (+72 % against +80.7 %).

## Lot 9 against lot 8 (record 0065, report 0040)

The matrix at the rates of `run_matrix_bench.py`, deliveries per second
(p50/p99 in ms):

| Scenario | Mode | Lot 8 | Lot 9 | Report 0040 | Verdict |
| - | - | - | - | - | - |
| fanout-1x4-q1 (160k offered) | volatile | 148.5k (0.85/1.68) | 160.0k (0.30/1.17) | +27.6 % (L78) | confirmed up to the offered rate |
| fanout-1x4-q1 | log | 94.9k (2.92/5.90) | 151.3k (0.70/1.83) | +53.4 % (L79) | confirmed, +59 % |
| fanout-1x16-q1 (240k offered) | volatile | 172.6k (1.61/3.06) | 236.7k (0.58/1.70) | +39.1 % (L80) | confirmed, +37 % |
| fanout-1x16-q1 | log | 99.8k (5.43/10.0) | 159.0k (1.38/3.73) | +75.3 % (L81) | confirmed, +59 % |
| fanin-32x1-q1 (80k offered) | log | 2.0k, 94 % lost, p99 4.3 s | 79.9k (529/804), 0 to 15,001 refusals | 23.1× (L85) | confirmed |
| slow-sub-q1 (70k offered) | log | 37.0k (323/1,947) | 70.0k (0.22/1.03) | 6,400× lower p50 (L89) | confirmed |
| churn-q1 | log | 2,253 refusals, 3.8 % lost | 0 refusals | 551 refusals → 0 (L91) | confirmed |

The other scenarios meet their offered rate on both lots. In pairs, lot
9 equals lot 8 within the noise (128 volatile pairs: 100.9k and 103.9k);
report 0040 says lot 9 beats lot 8 there (L116), which this machine does
not separate. Idle memory per connection falls from 4,736 to 2,699 B in
plain TCP (−43 %, 0040 says 1.77×) and from 14,198 to 11,807 B in mTLS
(−17 %). The log ext4 gain of 0040 (L125, +54.3 % at 60,000/s) is not
seen: on this machine lot 8 had no loss to recover there (55.8k against
56.2k).

## Lot 10 at K = 0 against lot 9 (record 0066, report 0041)

Record 0066 says that K = 0 is "the exact lot 9 path". In pairs, in QoS 0
and in every matrix scenario but one, lot 10 equals lot 9 within the
noise, and so does the head. The exception is the fan-in, 32 publishers
to one subscriber:

| Fan-in, offered | Mode | Lot 9 | Lot 10 | Head |
| - | - | - | - | - |
| 50,000/s | log, tmpfs | 0.17 ms p50, 0 refused | 0.17 ms, 0 | 0.16 ms, 0 |
| 60,000/s | log, tmpfs | 0.24 ms | 0.25 ms | 288 ms (0.18 to 685) |
| 70,000/s | log, tmpfs | 0.34 ms, 0 refused, 31.2 µs | 843 ms, 5,832 refused, 39.9 µs | 819 ms, 7,482 refused, 39.3 µs |
| 80,000/s | log, tmpfs | 472 ms, 0 refused | 943 ms, 29,036 refused (18 % lost) | 927 ms, 26,371 refused (17 % lost) |
| 100,000/s | volatile | 105 ms | 290 ms | 323 ms |

The last column of each cell is the broker's CPU per delivery where it
differs. Lot 9 keeps up with the fan-in in log mode to between 70,000
and 80,000 publications per second; lot 10 and the head stop between
60,000 and 70,000, refuse with 0x97 beyond, and spend 28 % more CPU per
delivery at 70,000/s. In volatile mode the three keep up at 80,000/s; at
100,000/s the p50 of lot 9 grows to 105 ms and that of the others to
about 300 ms; at 120,000/s all three refuse alike. This is a regression
of lot 10 on the default path, which the head inherits; record 0066
states the opposite. Its cause is below.

**Cause.** `git bisect` over the 48 commits of lot 10 that change the
crates names `76af3a5`, "Honor rescan requests arriving during a work
read". Each step built a dist executable and played it against lot 9 at
70,000/s in log mode, six alternated passes; a step was bad when its
median CPU per delivery exceeded lot 9's by more than 12 %. Twelve
alternated passes against its parent `285a4f8` confirm it:

| Fan-in, log, tmpfs | `285a4f8` | `76af3a5` |
| - | - | - |
| 60,000/s | p99 69 ms, 34.6 µs | p99 427 ms, 37.4 µs (+8 %) |
| 70,000/s | 70.0k delivered, 0 refused, 33.3 µs | 67.6k, 4,777 refused, 39.3 µs (+18 %) |

`76af3a5` fixes a lost wake-up: a rescan requested while a work read was
in flight was overwritten by that read's result, so an obligation made
visible during the read could stay unseen. The fix keeps the request,
restarts the scan from the start of the session's obligations (the read's
cursor and the scan frontiers are dropped), and skips the inline claim
while a rescan is pending (`owner/connections.rs`, `owner/pubsub.rs`). In
a fan-in, obligations become visible during nearly every read of the
subscriber's work, so the cost is that of a correctness fix, not of a
defect; which part of it comes from the restarts and which from the
skipped inline claims was not instrumented. Paying less for it, by
resuming the scan at the first obligation made visible during the read
for instance, is work for a later lot, with a model of the scan frontier.

Idle memory per connection rises from 2,699 to 2,880 B in plain TCP
(+7 %) and from 11,807 to 11,960 B in mTLS (+1 %); 0066 says it is
byte-identical to lot 9 (L156). The head is at 2,760 B. In QoS 0
bursts of 1,000 lot 10 reads 305k against 342k for lot 9, with ranges
that overlap (247k–323k, 285k–381k).

## The sharded mode, on the head

The same drivers, K = 0 to 3, on three broker cores. Where the head at
K = 0 meets the offered rate, a larger K cannot show a gain, only its
cost; where it does not, the table shows what K buys.

**Where K = 0 is saturated.** Deliveries per second:

| Case | Mode | K = 0 | K = 1 | K = 2 | K = 3 |
| - | - | - | - | - | - |
| fanout-1x16-q1 at 480k offered | volatile | 280k (0.81/1.80) | — | 319k, unstable: up to 39 % lost, p99 up to 1.8 s (below) | 339k (0.82/6.25) |
| fanout-1x16-q1 at 480k offered | log | 211k (1.09/2.88) | — | 309k, p99 up to 1.5 s | 335k (0.83/4.25) |
| fanout-1x16-q1 at 240k offered | log | 220k (1.05/2.44) | 163k, 30 % lost | 240k (0.54/5.39) | 240k (0.58/2.73) |
| fanin-32x1-q1 at 80k offered | log | 66.1k, 17 % lost | 70.8k, 11 % lost | 79.8k, 0.3 % lost | 77.4k, 3.2 % lost |
| 128 pairs QoS 1 | volatile | 102k (1.81 ms) | 123k (12.1 ms) | 121k (6.49 ms) | 117k (6.91 ms) |
| 128 pairs QoS 1 | log, tmpfs | 106k (7.33 ms) | 122k (9.03 ms) | 121k (6.61 ms) | 116k (6.88 ms) |
| 32 pairs QoS 1 | volatile | 156k (1.24 ms) | 137k | 130k (1.75 ms) | 125k |
| 32 pairs QoS 1 | log, tmpfs | 110k (1.81 ms) | 127k | 122k (1.79 ms) | 112k |
| 4 pairs QoS 1 | volatile | 152k (0.17 ms) | 100k | 90k (0.31 ms) | 93k |

**Where K = 0 is not saturated.** Every other matrix scenario (fanout-1x4,
fanout-1x16-q0, slow subscribers, churn, mixed), QoS 0 pairs and the ext4
log pairs deliver the same at every K. A larger K raises the p50 from
0.1–0.2 to 0.2–0.45 ms and the broker's CPU per delivery by up to 35 %
(churn 55.5 → 73.8 µs at K = 2; slow-sub-q1 24.8 → 33.5 µs; ext4 log 128
pairs 70.5 → 82.0 µs; fanout-1x16-q0 unchanged). In volatile mode the
fan-in at 80,000/s, which K = 0 keeps up with (p50 0.11 ms), falls behind
in some passes at every K ≥ 1 (p50 up to 1 s, up to 32,135 refusals), as
report 0041 measured (L76) and record 0066 does not say.

**K = 1** is worse than K = 0 nearly everywhere: one shard saturates and
fanout-1x16-q1 loses 30 to 39 % of its publications, as 0041 noted (L97).

**CPU under the gate.** `run_perf_gate.py` with A = the head and B = the
head at K = 2, broker on CPUs 0 to 2 and clients on CPU 3, median of B/A:
QoS 0 bursts ×1 1.331, ×8 1.578; QoS 1 pairs ×1 1.539, ×8 1.174 (each
replayed over 12 passes, as the gate does above 1.05); blast ×1 1.251, ×8
1.646 (6 passes, reported only). K = 2 costs 17 to 65 % more CPU per
delivery on every shape; on eight Python QoS 1 pairs it also acknowledges
about 20 % more per second (83k against 69k).

**Idle memory.** K = 2 adds 7.6 MB at rest (16.1 MB against 8.5 MB, 8
threads against 5) and 1.45 KB per connection (4,210 against 2,760 B at
768; mTLS 13,400 against 11,880 B at 400). Report 0041 says 7.9 MiB and
1.2 KiB (L119-126): confirmed.

**What the sharded mode buys here.** On three broker cores of this VM, K = 3
raises the ceiling of a wide QoS 1 fan-out by 21 % in volatile mode and
59 % in log mode with a stable p99; K = 2 rescues the fan-in in log mode
from the lot 10 regression; K = 1 or 2 adds 15 to 20 % to 128 QoS 1
pairs and 10 to 16 % to 32 pairs in log mode. It loses 16 to 41 % on 4
and 32 volatile pairs, raises latency at every load, costs up to 65 %
more CPU per delivery and 7.6 MB of memory, and K = 2 is unstable on the
widest fan-out: 11 of its 18 volatile runs and 9 of its 18 log runs beyond
240,000/s lost publications or had a p99 above 100 ms, against none at
K = 0 and three at K = 3. Report 0041's headline (+39.6 % on
fanout-1x16-q1 volatile at K = 2) is not reproduced: K = 0 reaches that
offered rate here, and beyond it K = 2 is unstable. Its log figures
(+49 % at K = 3) are.

## Against Mosquitto 2.1.4, volatile mode

| Case | mqttiumd (head) | Mosquitto 2.1.4 |
| - | - | - |
| 4 QoS 1 pairs, saturated | 137k, p50 0.19 ms, 14.7 µs | 175k, 1.27 ms, 5.7 µs |
| 32 QoS 1 pairs, saturated | 148k, 1.30 ms, 13.5 µs | 169k, 9.30 ms, 5.7 µs |
| 128 QoS 1 pairs, saturated | 89k, 3.83 ms, 24.0 µs | 102k, 20.7 ms, 7.6 µs |
| fanout-1x16-q1 at 480k offered | 280k (K = 0), 339k (K = 3) | 415k, 0.47 ms, 2.3 µs |
| fanout-1x16-q0 at 480k | 480k, p50 0.17 ms | 480k, p50 631 ms |
| 32 QoS 0 pairs at 300k | p50 0.44 ms, p99 10 ms | p50 5.4 ms, p99 26 ms |
| 32 QoS 1 pairs at 100k, mTLS | 99.2k, 0.77 ms | 60.8k, 32.3 ms |
| 32 QoS 0 pairs at 120k, mTLS | p50 0.27 ms | p50 40.4 ms |
| Idle, 768 plain connections | 10.7 MB, 2,760 B each | 9.3 MB, 5,019 B each |
| Idle, 400 mTLS connections | 14.7 MB, 11,880 B each | 20.5 MB, 28,805 B each |

Mosquitto, on one thread, reaches a higher QoS 1 ceiling and spends 2.4
to 3.5 times less CPU per delivery; mqttiumd keeps a far lower latency
under load and wins under mTLS. Report 0039's "25.7 % less VmRSS than
Mosquitto at 768 connections" (L134) does not hold against 2.1.4 on this
machine: mqttiumd's per-connection cost is lower, its base is 3 MB
higher.

## Instructions

`tools/count_broker_instructions.sh`, which now takes the shape, on the
four shapes of the gate with the sizes of reports 0047 and 0048, the head
and lot 10 alternated, two plays each:

| Shape | Lot 10 | Head | Change |
| - | - | - | - |
| QoS 0 bursts, 1 subscriber, 20,000 | 286.1 M | 280.9 M | −1.8 % |
| QoS 0 bursts, 8 subscribers, 10,000 | 718.2 M | 703.9 M | −2.0 % |
| QoS 1 pairs, 1 pair, 20,000 | 804.1 M | 800.2 M | −0.5 % |
| QoS 1 pairs, 8 pairs, 4,000 each | 1,259.4 M | 1,254.8 M | −0.4 % |

They agree with reports 0047 and 0048 and are the baseline of later work.
The script called without a shape gives 280.9 M, as before.

## What this means

- The gains of lots 8 and 9 are real on this machine, some of them
  smaller than on the Pi.
- Lot 10 lowered the fan-in limit of the default path in log mode from
  70,000–80,000 to 60,000–70,000 publications per second, through
  `76af3a5`, the fix of a lost wake-up; it also added 7 % to the idle
  memory of a connection, which the validation mostly recovered.
- The sharded mode is not a general gain on four cores: it pays for wide
  fan-outs and saturated fan-ins in log mode, and costs CPU, latency and
  memory everywhere else.
- mqttiumd's CPU per delivery is 2.4 to 3.5 times Mosquitto's; that, not
  the number of owners, is where the next optimisations have room.

## Commands

From `mqttiumd/tools/arm64_bench/`, with `H`, `L7` … the executables and
`MQ` Mosquitto 2.1.4:

```bash
python3 run_ab_campaign.py --driver pairs --mode volatile --passes 6 --target lot7=$L7 --target lot8=$L8 --target lot9=$L9 --target lot10=$L10 --target head=$H --target mosquitto=$MQ --cases 4:1:300000,32:1:200000,128:1:150000,32:1:60000,32:0:300000 --out A1.jsonl
python3 run_ab_campaign.py --driver pairs --mode log --work-dir /dev/shm --passes 6 --target lot7=$L7 --target lot8=$L8 --target lot9=$L9 --target lot10=$L10 --target head=$H --cases 4:1:300000,32:1:200000,128:1:150000 --out A2.jsonl
python3 run_ab_campaign.py --driver pairs --mode log --work-dir /var/tmp --passes 6 --target lot7=$L7 --target lot8=$L8 --target lot9=$L9 --target lot10=$L10 --target head=$H --cases 32:1:60000,32:1:20000,128:1:20000 --out A3.jsonl
python3 run_ab_campaign.py --driver pairs --mode volatile --tls --passes 6 --target lot7=$L7 --target lot8=$L8 --target lot9=$L9 --target lot10=$L10 --target head=$H --target mosquitto=$MQ --cases 32:1:60000,32:1:100000,32:0:120000 --out A4.jsonl
python3 run_ab_campaign.py --driver matrix --mode volatile --passes 6 --target lot8=$L8 --target lot9=$L9 --target lot10=$L10 --target head=$H --target mosquitto=$MQ --out A5.jsonl
python3 run_ab_campaign.py --driver matrix --mode log --work-dir /dev/shm --passes 6 --target lot8=$L8 --target lot9=$L9 --target lot10=$L10 --target head=$H --out A6.jsonl
python3 run_ab_campaign.py --driver matrix --mode log --work-dir /dev/shm --passes 6 --target lot9=$L9 --target lot10=$L10 --target head=$H --cases fanin-32x1-q1@50000,fanin-32x1-q1@60000,fanin-32x1-q1@70000,fanin-32x1-q1@80000 --out A9.jsonl
python3 run_ab_campaign.py --driver matrix --mode volatile --passes 6 --target lot9=$L9 --target lot10=$L10 --target head=$H --cases fanin-32x1-q1@80000,fanin-32x1-q1@100000,fanin-32x1-q1@120000 --out A9v.jsonl
python3 run_ab_campaign.py --driver matrix --mode volatile --passes 6 --target k0=$H --target k1=$H,--delivery-shards,1 --target k2=$H,--delivery-shards,2 --target k3=$H,--delivery-shards,3 --target mosquitto=$MQ --out B1.jsonl
python3 run_ab_campaign.py --driver matrix --mode log --work-dir /dev/shm --passes 6 --target k0=$H --target k1=$H,--delivery-shards,1 --target k2=$H,--delivery-shards,2 --target k3=$H,--delivery-shards,3 --out B2.jsonl
python3 run_ab_campaign.py --driver pairs --mode volatile --passes 6 --target k0=$H --target k1=$H,--delivery-shards,1 --target k2=$H,--delivery-shards,2 --target k3=$H,--delivery-shards,3 --cases 4:1:300000,32:1:200000,128:1:150000,32:0:300000 --out B3.jsonl
python3 run_ab_campaign.py --driver pairs --mode log --work-dir /dev/shm --passes 6 --target k0=$H --target k1=$H,--delivery-shards,1 --target k2=$H,--delivery-shards,2 --target k3=$H,--delivery-shards,3 --cases 32:1:200000,128:1:150000,32:0:300000 --out B4.jsonl
python3 run_ab_campaign.py --driver pairs --mode log --work-dir /var/tmp --passes 6 --target k0=$H --target k2=$H,--delivery-shards,2 --cases 32:1:60000,128:1:20000 --out B5.jsonl
python3 run_ab_campaign.py --driver matrix --mode volatile --passes 6 --target k0=$H --target k2=$H,--delivery-shards,2 --target k3=$H,--delivery-shards,3 --target mosquitto=$MQ --cases fanout-1x16-q1@20000,fanout-1x16-q1@25000,fanout-1x16-q1@30000 --out B8.jsonl
python3 run_ab_campaign.py --driver matrix --mode log --work-dir /dev/shm --passes 6 --target k0=$H --target k2=$H,--delivery-shards,2 --target k3=$H,--delivery-shards,3 --cases fanout-1x16-q1@20000,fanout-1x16-q1@25000,fanout-1x16-q1@30000 --out B8l.jsonl
python3 run_ab_campaign.py --summary A1.jsonl
python3 idle_rss_bench.py --binary $H --mosquitto $MQ --only-mqttiumd [--delivery-shards 2] --out idle.json
```

From `mqttiumd/`: `python3 tools/run_perf_gate.py $H k2.sh --passes 6
--broker-cpus 0,1,2 --client-cpus 3`, where `k2.sh` runs the head with
`--delivery-shards 2` added; `tools/measure_qos0_delivery.py --shape
bursts --burst 100|1000 --subscribers 1 --messages 100000` and `--shape
blast --subscribers 8`, the broker under `taskset -c 0,1,2` and the
client under `taskset -c 3`, six alternated passes;
`tools/count_broker_instructions.sh --shape qos0-bursts|qos1-pairs
--clients 1|8 $H 1 20000|10000|4000`. The bisection ran `git bisect run`
from `9dfd568` (bad) and `5e306904` (good), limited to `mqttiumd/crates`,
with a step that builds the commit and plays `run_ab_campaign.py --driver
matrix --mode log --work-dir /dev/shm --passes 6 --target lot9=$L9
--target c=<the build> --cases fanin-32x1-q1@70000`; the confirmation
played `--passes 12` with the targets `285a4f8` and `76af3a5` and the
cases `fanin-32x1-q1@70000,fanin-32x1-q1@60000`. The raw results stay
outside the repository.

## Not measured

The ARM64 figures, which need the Pi; QoS 2, which these drivers do not
play; power loss; the durable SQLite mode under the drivers (report 0043
measures it under the gate); rates beyond what one generator core can
drive (Mosquitto shows it reaches at least 415,000 QoS 1 deliveries per
second); and K above 3 or more than three broker cores, which this VM
cannot give.
