# Product TCP system reception

These probes launch the real `mqttiumd` product executable and inspect the same
SQLite file. They do not import its codec, the lab, a scenario fixture or the
oracle. Their results are **system-test-only**, not qualifying oracle campaigns.
The external client is the independently implemented `paho-mqtt` 2.1.0 client.

From `mqttiumd/`, with the repository Rust toolchain and Python available:

```sh
python3 -m pip install -r tests/system/requirements.txt
tools/run_tcp_reception.sh /tmp/mqttiumd-tcp-reception
```

The source tree must be clean. The script builds the normal product and a
separate `test-hooks` product, then runs the complete reception on that one
instrumented binary. Build directories stay outside the source and evidence
directories; `MQTTIUMD_TCP_TARGET_ROOT` can select their parent. The script also
builds two isolated mutant worktrees. A compiler error, timeout or unrelated
failure never counts as detecting either mutation. The expected failures are
`premature_pubrec` and `stale_generation_forwarded`.

When `strace` is installed, it wraps each actual reception after compilation.
Its witness belongs to that receipt and its recorded broker PIDs and binary
hash, including the product SIGKILL in case D. Without it, the manifest records
the narrower process-wait-status witness. Each case retains product events,
external observer records, client observations and relevant durable snapshots.
Each executable is copied into `binaries/` before running that exact copy. The
source archive is `git archive` of the recorded clean revision; retained command
arrays, dependency features and toolchain versions describe the builds. The
top-level index, `SHA256SUMS`, archive and archive digest describe this run.

For one development probe against an already built product:

```sh
python3 tests/system/tcp_reception.py --binary /path/to/mqttiumd \
  --out /tmp/mqttiumd-tcp-C --case C
python3 tests/system/tcp_reception.py --binary /path/to/mqttiumd \
  --out /tmp/mqttiumd-tcp-all --mode complete
```

The basic mode runs case A on a normal build. Cases C, D, G-generation and H
require the product feature `test-hooks`; these options are rejected by a normal
build. The driver always kills and reaps a broker left behind by a failed case.

| Case | Finite reception |
| --- | --- |
| A | Fresh database, actual paho CONNECT/QoS 2 cycle, durable session creation and reconnect with Session Present. |
| B | Byte fragmentation, concatenation with independent PING responses, every split of one small PUBLISH (at most 64), 32 malformed flag trials, declared oversize, truncation, unsupported feature distinction and Keep Alive. |
| C | Reached and externally retained precommit barriers for admission and release, no positive PUBREC/PUBCOMP while held, operation-correlated durable facts after release. |
| D | Admission committed before PUBREC, actual broker SIGKILL and wait status -9, same-file restart, duplicate PUBLISH without a second admission, completed PUBREL/PUBCOMP. |
| E | Completed exchange followed by a new PUBLISH using the same Packet Identifier, with a second publication identity and admission. |
| F | Copies of a database created through TCP; `quick_check=ok` with invalid session state or an excessive ClientID; typed refusal, no readiness, writer available, no business reset. |
| G-concurrency | Two simultaneous clients; another progresses while one leaves a packet incomplete; real four-slot egress saturation by a nonreader with a durable admission and unread PUBREC, bounded closure, retained effect identity accounted for, then retransmission and completion without a second admission. |
| G-generation | Eighteen takeovers, exceeding the core's remembered window, then replay of an actual captured old CONNECT; runtime rejects its generation while the current connection progresses. |
| H | Graceful shutdown with an accepted admission held before commit, released writer, uncertain submitted work preserved and reconciled after restart without a second admission. |

The nonreader configures its receive buffer and, on Linux, window clamp before
the TCP handshake. Requested and effective values are retained. Its pressure
phase sends at most 128 KiB and observes for at most twelve seconds until the
actual `output_queue_full` effect-retention event, including after the byte
budget is exhausted. Buffered bytes alone are not proof that MQTT packets have
reached the core; input-queue saturation is a distinct failure. Another client
progresses concurrently. The partial-packet wait is bounded by seven seconds,
and replay by three. Test barriers have the
product's 30-second cancellation budget. Product limits and client socket buffer
values are retained in their respective observations. Large probe byte records
keep their exact digest and size rather than exceeding the observer log budget.

This reception covers incoming durable QoS 2 only. It establishes no subscriber
delivery, QoS 1, complete MQTT conformance, unbounded service lifetime, power-loss
or universal crash guarantee. Existing qualifying K1/K2b/K3 campaigns retain
their separate scope and provenance.
