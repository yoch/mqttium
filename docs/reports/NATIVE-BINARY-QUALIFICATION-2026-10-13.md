# Native codec qualification on the Raspberry Pi 5 — 2026-10-13

## Disposition

This report measures the optional native codec of
[PR #623](https://github.com/yoch/mqttium/pull/623) end to end and records the
attempts to widen its gain. It answers one question: is a native build
efficient enough to offer as an installable option?

On the Raspberry Pi 5, the profile-guided build now produced by the Hatch hook
(`MQTTIUM_BUILD_CODEC=1 MQTTIUM_CODEC_PGO=1`) reduces client CPU per message by:

- 12.5–12.7 % for closed-loop QoS 0 and 6.9–7.7 % for closed-loop QoS 1;
- 10 % at 5,000 and 6 % at 10,000 QoS 1 publications per second;
- nothing for publications at 2,000/s;
- nothing for fixed-rate QoS 1 reception at 2,000–10,000 messages/s
  (within ±2 %).

The default native build without PGO made fixed-rate reception 4–9 % more
expensive. PGO removes that regression.

Compiling more of the client was measured and rejected. Generic Cython
compilation of the protocol layer, and typed Cython state for its classes, both
made the client slower.

The x86-64 confirmation, macOS and Windows have not been measured here.

## Method

All numbers come from the self-hosted Raspberry Pi 5 runner with the
`performance` governor and Mosquitto 2.1.2. Mosquitto ran on core 0,
orchestration on core 1, the measured client on core 2 and the bench's C peer
on core 3. The interpreter was uv-managed CPython 3.12.13 unless stated.

Each comparison uses one installed wheel twice, selecting the backend with
`MQTTIUM_CODEC_BACKEND`. Every arm imports from its own root, and the harness
asserts which modules are compiled. Three methods were used:

- **Micro and closed-loop:** `benchmarks/compiled_modules.py compare` runs
  fresh-process pairs in alternating AB/BA order, six per case. It reports the
  median ratio and range. The closed loop is a self-subscribed client sending
  24,000 messages of 256 B in bursts of 32, with at most 20 in flight.
- **Fixed rate:** the independent bench's `pub_fixed.py` and `sub_fixed.py`
  (`yoch/mqtt-python-client-bench` at `cd9f475`) report CPU per message from
  `getrusage` and median latency. Each value comes from two rounds, each the
  median of three, rotating between arms.
- **Hardware counters:** `perf stat` measured the whole fixed-rate receive
  process tree.

Same-code A/A controls stayed within ±0.5 % for micro cases and ±0.4 % for
closed-loop medians; their ranges reached about ±4 %.

The workflow lives on the unmerged diagnostic branch
`diag/native-binary-rpi5`. Its runs are listed below; each run page records
the exact commit and keeps the raw JSON and text artifacts.

| Run | Content |
| --- | --- |
| [38407694332](https://github.com/yoch/mqttium/actions/runs/38407694332) | Micro: native codec, generic compiled protocol |
| [38409507406](https://github.com/yoch/mqttium/actions/runs/38409507406) | Closed loop, fixed rate, native-mode profiles, C flags (invalid, see below) |
| [38412222328](https://github.com/yoch/mqttium/actions/runs/38412222328) | Fixed-rate receive bisection, explicit `-O3` C flags |
| [38416916057](https://github.com/yoch/mqttium/actions/runs/38416916057) | Typed protocol classes, three rates, hardware counters |
| [38420891599](https://github.com/yoch/mqttium/actions/runs/38420891599) | Profile-guided build (experimental training) |
| [38426359471](https://github.com/yoch/mqttium/actions/runs/38426359471) | PGO training scope, CPython 3.14 |
| [38432560964](https://github.com/yoch/mqttium/actions/runs/38432560964) | The Hatch hook's PGO build, the product path |

## Results

### Micro, CPU per operation (run 38432560964)

| Case | Python | Hook PGO | Reduction |
| --- | ---: | ---: | ---: |
| `encode_qos0` | 1,867 ns | 782 ns | 58.2 % |
| `encode_qos1_v5` | 2,173 ns | 853 ns | 60.7 % |
| `frame_64` | 1,283 ns | 688 ns | 46.3 % |
| `frame_4096` | 2,032 ns | 1,352 ns | 33.1 % |
| `properties_v5` | 11,328 ns | 9,789 ns | 13.6 % |
| `ingress_engine_qos0` | 2,800 ns | 2,415 ns | 13.8 % |
| `ingress_publish_qos1` | 4,779 ns | 4,197 ns | 12.2 % |
| `qos1_cycle_memory` | 18,052 ns | 15,521 ns | 14.0 % |
| `native_publish_nowait_qos0` | 4,147 ns | 2,988 ns | 28.1 % |

### Closed loop, client CPU per message (runs 38409507406 and 38432560964)

| Cell | Python | Native | Hook PGO | Hook PGO ratio range |
| --- | ---: | ---: | ---: | --- |
| 3.1.1 QoS 0 | 19.68 µs | 17.91 µs (−8.3 %) | 17.18 µs (−12.5 %) | 1.126–1.160 |
| 3.1.1 QoS 1 | 46.83 µs | 44.45 µs (−5.1 %) | 43.46 µs (−6.9 %) | 1.060–1.115 |
| 5 QoS 0 | 20.46 µs | 18.60 µs (−9.0 %) | 17.89 µs (−12.7 %) | 1.127–1.181 |
| 5 QoS 1 | 49.14 µs | 46.20 µs (−5.4 %) | 45.37 µs (−7.7 %) | 1.075–1.117 |

Native values are from run 38409507406; the Python column and the hook PGO
values are from run 38432560964.

CPython 3.14.3 gave the same picture in run 38426359471. The native codec
saved 3.7–7.9 % in the closed loop, and the PGO build saved 7.5–10.9 %.

### Fixed rate, QoS 1, client CPU per message (run 38432560964)

| Direction and rate | Python | Native* | Hook PGO |
| --- | ---: | ---: | ---: |
| Publish 2,000/s | 80.3 µs | 80.3 µs | 80.3 µs |
| Publish 5,000/s | 57.5 µs | 54.4–57.0 µs | 51.5 µs (−10.4 %) |
| Publish 10,000/s | 42.0 µs | 40.5 µs | 39.5 µs (−6.0 %) |
| Receive 2,000/s | 58.2 µs | 62.0–62.8 µs | 58.8 µs |
| Receive 5,000/s | 54.5 µs | 58.9–59.7 µs | 55.5 µs |
| Receive 10,000/s | 48.5 µs | 51.2–51.5 µs | 49.5 µs |

\* The Native column comes from runs 38412222328 to 38420891599, against
Python values within 1.5 µs of this table. Median receive latency was 61 µs
for Python and hook PGO, against 64–67 µs for the default native build at
2,000 and 5,000/s.

### Why the default native build lost at fixed-rate reception

With one message per wakeup, the native process executed fewer instructions
but more cycles. `perf stat` for reception at 5,000/s (run 38420891599):

| Arm | Cycles | Instructions | IPC | L1 i-cache misses | iTLB misses | Branch misses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Python | 5.24 G | 7.45 G | 1.42 | 163 M | 16.5 M | 53.3 M |
| Native | 5.67 G | 6.94 G | 1.22 | 203 M | 20.6 M | 61.2 M |
| PGO | 5.21 G | 6.97 G | 1.34 | 183 M | 19.4 M | 54.8 M |

The generated C for a message's path spreads over more distinct code than the
interpreter loop. In the closed loop, where the code stays cached, it is a net
gain; when messages arrive one at a time, it is a net loss.

Bisection ruled out a single culprit. Leaving the frame decoder in Python saved
only 1–1.8 µs; leaving the PUBLISH/PUBACK codecs in Python saved nothing.

A profile-guided build separates hot from cold code and orders branches by
observed frequency. That cut i-cache and branch misses by about 10 % and
brought reception back to parity.

## Rejected candidates

| Candidate | Evidence | Decision |
| --- | --- | --- |
| Generic Cython compilation of `protocol/` and the memory store as same-name extensions | All unit tests passed unchanged, but the QoS 1 cycle cost 17.6 % more CPU over the native codec and 19.9 % more over Python; engine ingress cost 2–6 % more. Attribute access that CPython 3.12 specializes became generic lookups. | Rejected |
| Typed `cclass` state bases for the protocol classes; methods kept on patchable Python classes | Engine ingress micro improved 3.6 %, but the QoS 1 cycle cost 11.7 % more. The closed loop cost 5–10 % more than the native codec alone, and fixed-rate reception 7.9 µs more per message. | Rejected |
| `-O3 -g0 -fomit-frame-pointer -fno-semantic-interposition -fno-plt` | Encoding 4–7 % faster, engine ingress 2–5 % slower. | Rejected |
| Same flags without an explicit `-O3` | `CFLAGS` replaced the interpreter's optimisation flags; the build was 2–3 times slower. | Invalid measurement |
| PGO trained only on engine-level micro cases | Equal in micro and closed loop, but 2.4 µs per message more than loopback training at fixed-rate reception. | Training extended |

## Adopted

The hook's profile-guided build (`MQTTIUM_CODEC_PGO=1`) currently requires
GCC; other compilers are refused. It works in four steps:

1. Generate C once, so every compilation sees identical source and object
   paths.
2. Compile an instrumented extension.
3. Run `mqttium.codec._train`: an `AsyncClient` against a minimal in-process
   loopback broker, one message at a time and in bursts, then engine-level
   QoS 2, property and fragmented-read work.
4. Recompile with `-fprofile-use -fprofile-partial-training`.

It needs no external broker, so it can run inside wheel-building containers.
Against the experimental PGO trained with Mosquitto, it was within ±1 % in the
closed loop and at fixed rate, and up to 4 % slower on isolated encoding micro
cases.

## Limits and open questions

- x86-64, macOS and Windows were not measured for this report; only Linux
  aarch64 with GCC 16 was. PGO is not available yet with Clang or MSVC.
- At fixed rate, the receive path is dominated by the Python runtime and
  asyncio, not by the codec. No codec-only change can reduce it by 5 %. Larger
  receive gains need algorithmic changes that would apply to both backends,
  such as acknowledgement coalescing per read or fewer effect drains.
- TLS, WebSocket, SQLite persistence, CPython 3.13 and 3.15 were not measured.
- Cython was 3.3.0 for every build in these runs.
