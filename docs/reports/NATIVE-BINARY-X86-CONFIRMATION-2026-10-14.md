# Native codec x86-64 confirmation — 2026-10-14

## Disposition

This report repeats the Raspberry Pi 5 qualification of
[NATIVE-BINARY-QUALIFICATION-2026-10-13](NATIVE-BINARY-QUALIFICATION-2026-10-13.md)
on an x86-64 workstation. That report deferred this confirmation.

The x86-64 results agree with the Pi 5, with smaller gains. The hook's
profile-guided build (`MQTTIUM_BUILD_CODEC=1 MQTTIUM_CODEC_PGO=1`) reduces
client CPU per message by:

- 5.6–8.7 % in the closed loop;
- 5 % for publications at 10,000 QoS 1 messages per second;
- nothing for publications at 2,000 and 5,000/s;
- 0.9–2.0 % for fixed-rate QoS 1 reception, far below the 5 % threshold.

The default native build gains 2.5–6.3 % in the closed loop. It is neutral at
fixed rate, except reception at 5,000/s, where it costs 2.4 % more.

Neither platform reaches the 5 % threshold for fixed-rate reception, so the
2026-10-13 decision stands: the native codec stays a build-from-source option
and no `mqttium[binary]` distribution is published.

## Method

The host was an Intel Core i7-3770: Ivy Bridge, 3.4 GHz, four cores with two
threads each. It ran Linux 6.8.0-142-lowlatency with the `performance` governor,
GCC 12.4, uv-managed CPython 3.12.13 and Cython 3.3.0. Both wheels were built
from commit `484a81d5` with the Hatch hook: one default build and one
profile-guided build.

Mosquitto 2.1.2 ran in the `eclipse-mosquitto:2.1.2-alpine` container, limited
to CPU 1. Its configuration was the Pi 5 configuration without the TLS
listener and without `max_keepalive`. The desktop session was confined to
CPU 0, and orchestration ran on its sibling thread, CPU 4. The measured client
ran on CPU 2 and the bench's C peer on CPU 3. The sibling threads of those
three cores, CPUs 5–7, were left idle. System daemons and other containers
were not confined. The 1-minute load average was 0.57 when measurement started.

The three methods are the ones used on the Pi 5, with the same case lists,
repeats and message counts:

- `benchmarks/compiled_modules.py compare`, with six fresh-process ABBA pairs
  per case;
- the bench's `pub_fixed.py` and `sub_fixed.py` (`yoch/mqtt-python-client-bench`
  at `cd9f475`), two rounds of three runs each, rotating arms;
- `perf stat` over the whole fixed-rate receive process tree.

The only change to the bench was reading the broker port from the environment,
so the dedicated broker could listen on a free port. The raw output of this
local run is not kept in the repository.

A same-code closed-loop A/A control gave medians within ±1.2 %, with pair
ratios from 0.974 to 1.061. No micro-benchmark A/A control was run on this host.

## Results

### Micro, CPU per operation

| Case | Python | Native | Hook PGO | PGO reduction |
| --- | ---: | ---: | ---: | ---: |
| `encode_qos0` | 2,189 ns | −57.8 % | 770 ns | 64.6 % |
| `encode_qos1_v5` | 2,587 ns | −58.9 % | 851 ns | 66.7 % |
| `frame_64` | 1,274 ns | −48.2 % | 546 ns | 57.1 % |
| `frame_4096` | 2,313 ns | −34.6 % | 1,427 ns | 39.0 % |
| `properties_v5` | 11,560 ns | −8.3 % | 10,163 ns | 12.2 % |
| `ingress_engine_qos0` | 3,014 ns | −9.5 % | 2,665 ns | 13.2 % |
| `ingress_publish_qos1` | 5,866 ns | −6.1 % | 5,107 ns | 13.2 % |
| `qos1_cycle_memory` | 24,690 ns | −1.0 % | 22,955 ns | 8.2 % |
| `native_publish_nowait_qos0` | 5,361 ns | −25.2 % | 3,730 ns | 30.1 % |

The Python column comes from the PGO comparison. The Native column is the
reduction measured by its own comparison against Python.

### Closed loop, client CPU per message

| Cell | Python | Native | Hook PGO | Hook PGO ratio range |
| --- | ---: | ---: | ---: | --- |
| 3.1.1 QoS 0 | 30.06 µs | 28.28 µs (−6.3 %) | 27.34 µs (−8.7 %) | 1.046–1.126 |
| 3.1.1 QoS 1 | 64.11 µs | 62.45 µs (−3.1 %) | 60.62 µs (−5.6 %) | 1.033–1.065 |
| 5 QoS 0 | 30.89 µs | 29.48 µs (−4.1 %) | 28.36 µs (−8.7 %) | 1.059–1.109 |
| 5 QoS 1 | 67.63 µs | 65.62 µs (−2.5 %) | 62.87 µs (−7.0 %) | 1.045–1.088 |

Every PGO median lies outside the A/A medians. Per cell, every PGO pair ratio
is above the A/A control's range, except for 3.1.1 QoS 0, whose A/A control
reached 1.061.

### Fixed rate, QoS 1, client CPU per message

| Direction and rate | Python | Native | Hook PGO |
| --- | ---: | ---: | ---: |
| Publish 2,000/s | 154.8 µs | 154.8 µs | 153.8 µs (−0.6 %) |
| Publish 5,000/s | 95.0 µs | 94.9 µs | 94.9 µs (−0.2 %) |
| Publish 10,000/s | 66.5 µs | 65.2 µs (−1.8 %) | 63.1 µs (−5.0 %) |
| Receive 2,000/s | 93.6 µs | 94.0 µs (+0.4 %) | 92.8 µs (−0.9 %) |
| Receive 5,000/s | 83.6 µs | 85.5 µs (+2.4 %) | 82.8 µs (−0.9 %) |
| Receive 10,000/s | 72.6 µs | 72.8 µs (+0.3 %) | 71.1 µs (−2.0 %) |

The two rounds agreed within 1.1 µs for every cell. Across arms, median
receive latency was 154–156 µs at 2,000/s, 133–148 µs at 5,000/s and
158–164 µs at 10,000/s. The PGO build's medians were equal to or lower than
Python's.

### Hardware counters, reception at 5,000/s

Each value is the mean of two `perf stat` runs covering the whole process
tree: the orchestrating parent, the client and the C peer.

| Arm | Cycles | Instructions | IPC | L1 i-cache misses | iTLB misses | Branch misses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Python | 16.45 G | 10.66 G | 0.65 | 417 M | 21.2 M | 124 M |
| Native | 16.62 G | 10.34 G | 0.62 | 458 M | 21.9 M | 115 M |
| PGO | 16.45 G | 10.26 G | 0.62 | 441 M | 22.0 M | 113 M |

As on the Pi 5, the native builds execute fewer instructions but miss the
instruction cache more often: 10 % more for the default build, 6 % for PGO.
Unlike the Pi 5, branch misses fall. Cycles end up within 1 % of Python.

## Comparison with the Raspberry Pi 5

| Measure | Pi 5, hook PGO | x86-64, hook PGO |
| --- | ---: | ---: |
| Closed loop, QoS 0 | −12.5 to −12.7 % | −8.7 % |
| Closed loop, QoS 1 | −6.9 to −7.7 % | −5.6 to −7.0 % |
| Publish 5,000/s | −10.4 % | −0.2 % |
| Publish 10,000/s | −6.0 % | −5.0 % |
| Receive 2,000–10,000/s | within ±2 % | −0.9 to −2.0 % |

The micro gains are of the same order on both hosts. End to end, the codec's
share of per-message CPU is smaller on this host. At 2,000/s, a publication
costs 155 µs of CPU here against 80 µs on the Pi 5, mostly outside the codec.

## Limits

- One x86-64 microarchitecture from 2012 was measured, on a desktop
  workstation rather than an isolated runner. Newer cores, macOS and Windows
  are still unmeasured.
- The broker and the client ran on one host, as on the Pi 5. TLS, WebSocket,
  SQLite persistence and other CPython versions were not measured.
