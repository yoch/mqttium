# Runtime load measurements

These probes start a single local broker on loopback. Use a Linux host with
Python 3.11 or later, OpenSSL, and an unused output directory for each run.
The manifest records the executable and Python sources by SHA-256, exact
commands, kernel and affinities. The raw broker journal and per-probe results
are separate files. Commands below run from `mqttiumd/`.

```sh
cargo build -p mqttiumd --profile dist --locked
binary="$PWD/target/dist/mqttiumd"
python tools/runtime_load/backlog.py --binary "$binary" --out /tmp/backlog-hour
python tools/runtime_load/audit_log.py /tmp/backlog-hour/events.jsonl --out /tmp/backlog-hour/journal-check.json
python tools/runtime_load/memory.py --binary "$binary" --out /tmp/six-sessions
python tools/runtime_load/profiles.py close --binary "$binary" --tls --out /tmp/tls-close
python tools/runtime_load/profiles.py waiting --binary "$binary" --depth 12 --out /tmp/reader
python tools/runtime_load/profiles.py bounds --binary "$binary" --sessions 1024 --out /tmp/holds-1024
taskset -c 3 python tools/runtime_load/profiles.py qos0 --binary "$binary" \
    --tls --payload 65536 --fanout 64 --rate 16 --duration 4 \
    --broker-cpus 0-2 --competitor-cpu 0 --out /tmp/qos0-tls-loaded
```

* **Backlog** paces a single MQTT 3.1.1 publisher to 100 QoS 2 handshakes/s for
  one hour. A persistent QoS 2 subscriber disconnects at 60 s, resumes at
  about 95 s, and commits each application identity on PUBREL. The broker
  runs in volatile mode. No restart takes place. The monitor records progress,
  scheduling lag and Linux process memory every minute.
* **Journal** checks every retained line including its newline, the product
  event format, a contiguous sequence and clean startup/shutdown markers for
  a single run. A journal whose start has already rotated out cannot meet
  the whole-run criterion. Report duration and whether rotation actually
  occurred with every result.
* **Memory** fills six disconnected persistent QoS 1 sessions, reconnects and
  drains each sequence, fences each client's ACKs with PINGRESP, then samples
  VmSize, RSS, PSS, anonymous pages and private dirty pages. Compare repeated
  post-drain samples including idle time. This exercises the configurable
  workload in volatile storage.
* **Close** measures the TLS handshake, MQTT CONNECT/CONNACK and the time from
  MQTT DISCONNECT to clean peer EOF separately. TLS uses certificate and peer
  verification and reports a truncated TLS stream as a failed run. The broker
  journal supplies its terminal-observation timestamp. The distribution spans
  both endpoints' scheduling. The samples before and after a batch include
  process CPU ticks and per-thread context switch counts.
* **Waiting** sends normal pipelined MQTT 3.1.1 QoS 2 traffic. The driver
  releases each identifier on PUBREC, verifies every ordered delivery at a
  QoS 0 subscriber, and times a PINGRESP on an independent connection.
  Compare depths 1, 8, 12 and 20. Wire timing includes the peer and scheduler;
  the driver has no internal timing hook for when a publication actually waits.
* **Bounds** holds exactly eight acknowledged QoS 2 exchanges per connection
  by delaying PUBREL; reports the jointly held count and separately measures
  fill, full-table PINGRESP, release, a new exchange and process memory. Use
  separate runs of 1, 1024 and 4096 sessions on a host with sufficient RAM
  and at least 4128 file descriptors for each process.
* **QoS 0** reports offered publications, unique copies received per subscriber
  within the timed window and after up to 5 s of drain, duplicates and
  publish-to-copy latency for the copies observed. Use `--tls`,
  `--payload 256|65536`, `--fanout 1|16|64`, `--shards 0|2|3` and an
  optional bounded SHA-256 process on one broker CPU. The producer caps total
  offered copies at 64 MiB/s. Compare at the same offered rate, payload,
  affinity and transport; keep the generator off the broker CPUs and record
  host contention. Interleave paired repetitions before making comparisons.

These are bounded measurements on one Linux host. Record socket placement,
CPU quotas, measured rate, raw distributions, event logs, storage, run
duration and contention with each claim. Retained topics, real network
latency, host power loss and native non-Linux platforms require separate
measurements. For pinned Mosquitto load references see
[the load harness](../arm64_bench/README.md).
