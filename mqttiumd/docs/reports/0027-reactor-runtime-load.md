# Load on the reactor runtime (records 0044 to 0046)

The matrix of report 0024, played again on the executable built from
`967e95d2faca` with the runtime of records 0044 to 0046, next to Mosquitto
2.0.21 on the same harness. One Linux virtual machine, four logical CPUs
shared with the load generator, two of them given to the broker, stores
and scratch files on tmpfs. These are exploratory measurements of one
host and one run; they qualify nothing and say nothing about a disk.

## Reproduce

```sh
cd mqttiumd
cargo build -p mqttiumd --profile dist --locked
sh preview/build-mosquitto-reference.sh /tmp/load-reference
python3 tools/run_broker_load.py \
  --mqttiumd target/dist/mqttiumd --source "$(git rev-parse HEAD)" \
  --reference /tmp/load-reference/mosquitto-2.0.21 \
  --scratch /dev/shm --out /tmp/broker-load-0027 \
  --cores 2 --pairs 4,32 --repeats 2 --duration-ms 3000 \
  --rates-q0 10000,30000,60000,100000,150000 --rates-q1 800,1500,3000,6000
```

The driver, the certificates, the 256-byte payload and the pacing are
those of report 0024: MQTT 5 over TLS 1.3 with client certificates, one
publisher and one subscriber per pair, one topic per pair. Each point is a
3-second publication window followed by a 500 ms drain; the tool repeats
a point only at the knee of a curve. Two things differ from report 0024.
The tool accepts up to 64 pairs, since the connection limit is an option;
32 pairs are 64 connections. The host has no cgroup v2 hierarchy and only
a root user: the tool records quota and throttling as unavailable and
keeps the reference broker as root. Kernel 6.18.44, x86_64.

## Before

On this host the executable of report 0024 (`bf9f702`) delivered 868
QoS 0 and 796 QoS 1 publications per second with four pairs, whatever the
offered rate: every loop of that runtime slept one millisecond.

## Results

Received publications per second, with the share of the offered
publications that did not arrive in brackets when it is not zero. Two
figures are the two runs of a repeated point.

### QoS 0

| Pairs | Offered/s | mqttiumd | Mosquitto 2.0.21 |
| -: | -: | - | - |
| 4 | 10,000 | 9,996 | 10,000 |
| 4 | 30,000 | 29,950 (0.2 %) | 29,997 |
| 4 | 60,000 | 59,599 (0.7 %); 59,395 (1.0 %) | 59,999 |
| 4 | 100,000 | 83,850 (16 %); 87,287 (13 %) | 99,041; 99,998 |
| 4 | 150,000 | 72,780 (52 %) | 134,491 (2.4 %); 135,226 (2.1 %) |
| 32 | 10,000 | 10,000; 9,945 (0.5 %) | 10,000 |
| 32 | 30,000 | 27,211 (9.3 %); 27,803 (7.3 %) | 30,000 |
| 32 | 60,000 | 29,995 (50 %) | 59,999 |
| 32 | 100,000 | 19,191 (81 %) | 97,489 |
| 32 | 150,000 | 5,134 (96 %) | 139,763; 117,464 (11 %) |

### QoS 1, durable (WAL, `synchronous=FULL`, on tmpfs)

| Pairs | Offered/s | mqttiumd | Mosquitto 2.0.21 |
| -: | -: | - | - |
| 4 | 800 | 800 | 800 |
| 4 | 1,500 | 1,500 | 1,500 |
| 4 | 3,000 | 2,998; 2,970 | 3,000 |
| 4 | 6,000 | 2,464 (58 %); 2,993 (49 %) | 6,000; 6,000 |
| 32 | 800 | 798 | 800 |
| 32 | 1,500 | 1,496 (0.2 %); 1,499 | 1,500 |
| 32 | 3,000 | 2,135 (28 %); 2,179 (27 %) | 3,000 |
| 32 | 6,000 | 2,619 (56 %) | 6,000; 6,000 |

A QoS 1 publication that mqttiumd does not deliver is refused with
PUBACK 0x97 (Quota exceeded), not lost: between 8,887 and 10,479 refusals
at 6,000 offered with four pairs. With 32 pairs, beyond the store's rate,
connections are also closed with `input_capacity`: 8 in one of the two
runs at 3,000 offered, 18 at 6,000 (record 0046 says why). No connection
was closed at any rate mqttiumd sustained. Mosquitto sustained every
QoS 1 rate offered here; its own limit was not reached.

Median delay from submission to the subscriber at sustained rates:
0.08 to 0.10 ms for mqttiumd QoS 0 with four pairs (Mosquitto 0.05 ms),
0.8 to 2.9 ms for mqttiumd QoS 1 (Mosquitto 0.1 ms).

## Reading

- **QoS 0, eight connections.** From 868 to about 59,500 publications
  per second with at most 1 % not delivered, and 84,000 to 87,000 at the
  point where 13 to 16 % are dropped. Mosquitto delivers 100,000 without
  loss and about 135,000 at its own knee. The gap went from a factor of
  about 140 to a factor of about 1.6.
- **QoS 0, 64 connections.** mqttiumd sustains about 27,500 and then
  degrades as the offered rate grows, down to 5,000 at 150,000 offered:
  the core thread uses 90 % of a CPU from 30,000 offered, with work per
  turn that grows with the number of connections and dropped publications
  that each cost a report. Mosquitto is unaffected by the
  number of connections at these rates. This is the weakest point of the
  current runtime.
- **QoS 1.** From 796 to 3,000 publications per second with eight
  connections and about 2,150 with 64. The store worker is the bound: its
  thread used 83 to 86 % of a CPU at 1,500 offered and 98 to 99 % from
  3,000. Mosquitto is at least twice
  as fast and was not pushed to its limit; report 0024 measured about
  47,000 for it on another host.
- **Overload.** mqttiumd loses throughput when more is offered than it
  can serve (2,464 at 6,000 offered against 2,998 at 3,000): refusing
  costs it work. Mosquitto keeps its plateau.

Against the thresholds the session plan had estimated for this runtime:
QoS 0 at 20,000 and at 50,000 with 99 % delivered are met with eight
connections; 100,000 is not (87,000 with losses); 1,000 connections are
not offered (128); durable QoS 1 at 5,000 is not met (3,000).

## What moved the figures

In order, each with the commit that carries its own measurement:

| Change | QoS 0, 4 pairs | QoS 1, 4 pairs |
| - | -: | -: |
| Executable of report 0024 | 868 | 796 |
| Loops woken by a signal instead of a 1 ms sleep (`c382612`) | 13,900 | 1,300 |
| Connections as non-blocking tasks on `mio` reactors (`b21369d`) | 59,500 at 60,000; 92,700 at 100,000 | unchanged |
| Statements of the store kept compiled, commands committed in groups (`4a07d7b`, `0456435`) | unchanged | 2,830 at 3,000 |

Those intermediate figures were taken with one 1.8-second run each, some
while other jobs used the machine; the tables above are the reference.
The grouped commit matters less here than it will on a disk: on tmpfs a
commit costs no synchronisation, and the gain came mostly from no longer
compiling SQL for every command (11 of 18 samples of the worker thread
before, 4 of 24 after).

## What this report does not show

- Nothing on a real disk, on ARM, with payloads other than 256 bytes,
  with retained messages, wildcards or fan-out beyond one subscriber.
- No endurance: every point lasts three seconds.
- No isolation of the broker from the generator: both share four CPUs.
- The Volatile profile of ARC-020 does not exist yet; every QoS 1 figure
  is Durable.
- The two changes of `ed9f357` (one reactor wake-up per turn, session
  bindings in one pass) are included in the executable measured here but
  were not measured on their own on a quiet machine.
