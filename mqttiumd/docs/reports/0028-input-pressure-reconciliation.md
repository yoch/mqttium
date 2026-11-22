# Publications reconciled under input pressure, and the return to nominal

The counter-audit of `bbe1453` asked for the account of every publication
offered to the broker (submitted, refused, acknowledged, delivered), for
the latency percentiles at 64 connections and for the return to a nominal
load after an overload. This report gives them for the executable built
from `038ce239d248` with the `dist` profile.

One Linux virtual machine, four logical CPUs shared with the load
generator, two of them given to the broker, stores and scratch files on
tmpfs, nothing else running in the session. These are exploratory
measurements of one host; they qualify nothing and say nothing about a
disk. The harness, the certificates and the 256-byte payload are those of
report 0027: MQTT 5 over TLS 1.3 with client certificates, one publisher
and one subscriber per pair, one topic per pair, 32 pairs for 64
connections. Each point is a 5-second publication window followed by a
500 ms drain.

## Reproduce

```sh
cd mqttiumd
cargo build -p mqttiumd --profile dist --locked
sh preview/build-mosquitto-reference.sh /tmp/load-reference
common="--mqttiumd target/dist/mqttiumd --source $(git rev-parse HEAD) \
  --reference /tmp/load-reference/mosquitto-2.0.21 --scratch /dev/shm \
  --only-broker mqttiumd --cores 2 --duration-ms 5000 \
  --broker-option=--max-connections --broker-option=128 \
  --broker-option=--event-level --broker-option=ops"
python3 tools/run_broker_load.py $common --out /tmp/load-q1 \
  --pairs 4,32 --qos 1 --rates-q1 500,1500,3000 --repeats 2
python3 tools/run_broker_load.py $common --out /tmp/load-then-3000 \
  --pairs 32 --qos 1 --rates-q1 3000 --then 500,6000 --repeats 2
python3 tools/run_broker_load.py $common --out /tmp/load-then-6000 \
  --pairs 32 --qos 1 --rates-q1 6000 --then 500,6000 --repeats 2
python3 tools/run_broker_load.py $common --out /tmp/load-q0 \
  --pairs 32 --qos 0 --rates-q0 10000,30000,60000 --repeats 1
```

`--then RATE,DURATION_MS` offers a second rate to the same connections
after the first; the result then accounts for each phase and each of its
seconds (`phases` in `result.json`).

## QoS 1: every publication is answered

In every run below the generator lost no connection, the broker closed
none (its only closures are the 8 or 64 DISCONNECT packets that end a
run), no publication stayed without a PUBACK, none was acknowledged
without being delivered and none was delivered twice. Submitted is
therefore acknowledged plus refused, and delivered equals acknowledged. A
refusal is a PUBACK with Reason Code 0x97 (Quota exceeded): no exchange
credit came back within the wait of record 0046.

Latencies are from the submission of a publication, in milliseconds,
p50 / p95 / p99. Two lines are the two runs of a repeated point.

| Connections | Offered/s | Submitted | Acknowledged | Refused (0x97) | Delivered | To the subscriber | To the PUBACK |
| -: | -: | -: | -: | -: | -: | - | - |
| 8 | 500 | 2,500 | 2,500 | 0 | 2,500 | 1.3 / 2.1 / 4.1 | 1.0 / 1.5 / 3.2 |
| 8 | 1,500 | 7,500 | 7,500 | 0 | 7,500 | 1.2 / 6.3 / 10.0 | 0.5 / 2.1 / 3.9 |
| 8 | 1,500 | 7,500 | 7,500 | 0 | 7,500 | 0.9 / 5.0 / 9.8 | 0.4 / 1.7 / 3.4 |
| 8 | 3,000 | 15,000 | 14,478 | 522 | 14,478 | 4.5 / 52.3 / 72.4 | 1.0 / 2.8 / 4.1 |
| 8 | 3,000 | 15,000 | 14,361 | 639 | 14,361 | 5.0 / 52.9 / 96.8 | 1.0 / 2.8 / 22.3 |
| 64 | 500 | 2,500 | 2,500 | 0 | 2,500 | 1.5 / 16.0 / 24.9 | 0.8 / 11.8 / 18.1 |
| 64 | 1,500 | 7,500 | 7,491 | 9 | 7,491 | 7.8 / 45.5 / 68.8 | 3.1 / 17.3 / 29.7 |
| 64 | 1,500 | 7,500 | 7,472 | 28 | 7,472 | 9.8 / 56.1 / 92.1 | 3.9 / 20.0 / 33.6 |
| 64 | 3,000 | 15,000 | 9,283 | 5,717 | 9,283 | 66.6 / 100.4 / 120.9 | 14.2 / 29.8 / 37.4 |
| 64 | 3,000 | 15,000 | 10,003 | 4,997 | 10,003 | 59.5 / 97.0 / 115.5 | 13.6 / 28.1 / 36.1 |

At 3,000 publications per second the broker acknowledges about 2,900 a
second for 8 connections and 1,850 to 2,000 for 64: with more
connections more of the offer is refused, and the refusals themselves
cost the single core that applies the inputs.

## Return to nominal at 64 connections

An overload of five seconds is followed, on the same 64 connections, by
six seconds at 500 publications per second. The point at 500 alone, in
the table above, is the nominal reference: p99 of 24.9 ms to the
subscriber and 18.1 ms to the PUBACK, no refusal.

| Overload | Run | Overload: submitted / acknowledged / refused | First nominal second: acknowledged / refused, p99 to the subscriber | Following five seconds: refused, p99 to the subscriber |
| -: | -: | - | - | - |
| 3,000/s | 1 | 15,000 / 8,963 / 6,037 | 500 / 0, 65.6 ms | 0, 21.1 to 25.4 ms |
| 3,000/s | 2 | 15,000 / 10,020 / 4,980 | 499 / 1, 46.8 ms | 0, 16.8 to 29.7 ms |
| 6,000/s | 1 | 29,085 / 5,418 / 23,667 | 173 / 327, 837.6 ms | 0, 17.7 to 28.1 ms |
| 6,000/s | 2 | 28,414 / 5,577 / 22,837 | 158 / 342, 855.0 ms | 0, 16.9 to 27.8 ms |

After 3,000 a second the first nominal second still carries the tail of
the overload in its p99 and refuses at most one publication; from the
next second the broker is at its nominal figures. After 6,000 a second
the backlog of the overload is served during the first nominal second,
which refuses two thirds of its publications; from the next second the
broker is nominal again. In the four runs every publication of both
phases is answered, every acknowledged one is delivered, and no
connection is closed.

Two things these runs show that are not good. At 6,000 a second the
broker acknowledges about 1,100 publications a second, less than at
3,000, and a publication it does acknowledge has waited most of a second
(p50 of 672 and 749 ms): the excess is refused late, after it has
queued, where an earlier refusal would cost less. The generator also
failed to submit 3 to 5 % of that offer in its own schedule.

## QoS 0 at 64 connections

One run of each point. QoS 0 has no acknowledgement: a publication is
delivered or not.

| Offered/s | Submitted | Delivered | Not delivered | To the subscriber |
| -: | -: | -: | -: | - |
| 10,000 | 50,000 | 49,887 | 113 (0.2 %) | 0.1 / 8.0 / 11.9 |
| 30,000 | 150,000 | 128,507 | 21,493 (14 %) | 0.7 / 9.3 / 17.9 |
| 60,000 | 299,997 | 107,268 | 192,729 (64 %) | 4.7 / 19.7 / 31.2 |

No connection is closed. The figures at 30,000 and 60,000 are below the
single runs of report 0027 on the same host (9 % and 50 % not arrived
with a 3-second window); the two reports were not alternated and the
difference is not attributed.

## What this does not show

- Nothing about a disk: the store is on tmpfs, and the cost of `fsync`
  is absent.
- The generator shares the machine and its CPUs with the broker.
- Five seconds of overload and one host. A longer overload, a slow
  subscriber and clients that fill their Receive Maximum at QoS 2 under
  this load are not measured here; the last has an executable test
  (`clients_that_fill_their_announced_window_together_all_complete`).
- No comparison with Mosquitto: its QoS 1 answer in this bench is not a
  synchronous commit (open conditions, "Load figures are those of one
  host").
