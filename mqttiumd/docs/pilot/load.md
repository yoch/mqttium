# Load, resources and exploratory comparison

All corrected mqttiumd runs used the downloaded normal release executable
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119` from
source `51be4079`. Build platform was Linux x86_64/glibc 2.39. Execution platform
was Linux 6.18.57/glibc 2.41. The ELF symbol floor is GLIBC 2.34. This execution
establishes compatibility on the named machine. `manifest.json` binds the raw
campaign, ledger and one-second samples to their hashes.

The host exposed AMD EPYC 9V74, affinity CPUs 0–4, cgroup CPU quota 400000/100000
and memory limit 17,179,869,184 bytes. The filesystem was overlayfs. Its backing
medium was unidentified. The Python inspector used SQLite 3.46.1, while the
broker reported bundled SQLite 3.54.1. The normal broker reported WAL,
`synchronous=2` (FULL),4096-byte pages and autocheckpoint 1000, with hooks and
kill points disabled.

Eight public Paho 2.1.0 clients published numbered 256-byte payloads at 50/s in
aggregate. Two Paho dashboards used a mix of exact and wildcard filters and
public manual acknowledgements. Receive Maximum was 8, publisher inflight 8 and
client queue 16. Reconnect-on-failure was disabled. Ten TCP openings, ten TLS
authentications and ten session establishments were observed. The same broker
PID and SQLite pathname served all 90,000 messages. Both dashboards received
90,000 distinct verified sequences. The event stream recorded 90,000 admissions,
180,000 Claims, 180,000 Finishes and 6,980 receipt-collection passes. There were
zero stream gaps, duplicate committed operations, MQTT DUP deliveries or client
errors in this healthy run. Ordinary disconnection cancelled all eight Wills.

| Measured latency |p50 ms|p95 ms|p99 ms|Maximum ms|
| --- | --- | --- | --- | --- |
|Publication → PUBACK|3.295|5.262|7.293|308.472|
|Publication → dashboard 1|6.837|9.304|12.495|327.723|
|Publication → dashboard 2|7.053|10.049|13.523|328.777|

The maximum is retained as an outlier. The user-specified 250 ms criterion was
p99. The compressed ledger contains every publication and receipt timestamp.
There were 1,782 periodic coherent SQLite/resource samples across 1,800 seconds.
All sampled accounts equalled independently counted rows and references and
remained within their limits. Maximum sampled claims were 2 and references 10.
These periodic maxima do not establish subsecond queue peaks. Full-window
internal ready/input/output queue cardinalities were not retained by this
observer. Its event counter still detected every rotation and sequence gap.

RSS began at 9,452 KiB and ended at 8,652 KiB, with sampled range 8,648–9,724 KiB.
CPU was 341.86 user and 165.08 system seconds, approximately 0.282 CPU cores over
the window. Kernel-accounted process writes were 16,302,505,984 bytes, including
SQLite and required event output. That quantity is distinct from final file size
and physical SSD wear. Peak main image was 126,976 bytes, peak WAL 4,169,472 bytes
and SHM 32,768 bytes. At rest there were 10 sessions, 11 routes, 8 retained values,
8 payloads/2,048 bytes, 8 references, 36 receipts and zero obligations, claims,
inbound exchanges, fences or Wills. The image had 31 pages and 4 freelist pages.
No VACUUM, SQL cleanup, quota increase or restart freed capacity during the run.

A second sample completed 6,000 publications and 12,000 Finishes over 120 seconds
with 1 KiB payloads. PUBACK p99 was 7.394 ms and maximum 23.772 ms. Its normal shutdown
also created zero Wills. The presence measurements used 100 sequential
connections and 24 additional connections through four workers per transport.
TLS handshake+CONNACK p99 was 58.70 ms. Detected EOF→retained-Will commit p99 was
11.31 ms and commit→receipt 10.23 ms. Matching cleartext values were 9.38 ms and 9.15 ms.
Percentiles use nearest rank. Keep Alive was measured separately at 1,494 ms for
a negotiated one-second interval under TLS.

The separate eight-process perturbation killed two actual sensor PIDs and the
broker with observed wait status-9. Five more armed Wills were recovered at
startup. A normal sensor departure published its own retained offline and
cancelled its Will. The offline dashboard recovered 16 due messages. Seven Will
admissions were observed across the two epochs. Final state contained eight
retained values, no obligation/claim/Will and exact reference counts. An initial
receiver stopped while two intentions were still pending. Its receipt remains
in the evidence. The corrected receiver waits for intentions, obligations and
claims to drain before assessing message completeness.

The quota receiver offered 500 publications at 50/s with one dashboard offline.
It received 127 successes and 373 definite 0x97 refusals, starting at 2.54 s.
The control publication progressed. Both eligible dashboards ultimately received
exactly the 127 admitted sequences. Drainage restored all resources and allowed
a further publication. This receiver passes its safety assertions and supplies
the capacity evidence behind the overall NO-GO.

Mosquitto 2.0.21 was run separately on the same machine with identical Paho
clients, TLS, topics, 256-byte payloads, rate and destinations for 120 seconds.
Both brokers completed 6,000 publishes and 12,000 deliveries. mqttiumd PUBACK
p99 was 6.755 ms and Mosquitto 3.747 ms. Their persistence policies differ.
Mosquitto used persistence with a 30-second autosave and no demonstrated per-PUBACK
fsync guarantee. mqttiumd used SQLite FULL admission and recipient commits.
The comparison is exploratory. The configurations and distributions are saved.

A separate matched SIGKILL experiment first allowed 31 seconds for a quiet
snapshot, then received 20 positive PUBACKs in a 50/s burst while both dashboards
were offline. mqttiumd resumed both sessions and delivered 20 messages to each,
with the last retained burst value. Mosquitto resumed both earlier sessions but
delivered zero burst messages and replayed the checkpointed retained seed.
That observation belongs to its stated periodic-persistence policy. It is not
a comparison at equal durability guarantees.
