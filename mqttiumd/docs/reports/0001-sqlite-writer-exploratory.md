# 0001 — The SQLite writer, a first exploratory measurement

Record 0008, lot L7. **Exploratory: it qualifies nothing.** There is no
threshold, no comparison with another broker, and no claim about a profile.
The rates below count durable store transactions on one machine. They are
not MQTT messages of a broker, which would also pay for the core, the
adapters, the network and the evidence.

## What was measured

`crates/mqttiumd-store-sqlite/examples/measure_writer.rs` drives
`SqliteStore` directly. Each step is two transactions on the real tables:

- the real `AdmitPublication`: QoS 2 inbound, one publication, its payload,
  one obligation per target, the inbound marker, the accounts and the
  receipt;
- the `ReleaseInbound` that ends that inbound exchange and frees its slot.

Each command is its own `BEGIN IMMEDIATE` transaction, committed with WAL
and `synchronous=FULL`. There is no adapter, no core, no evidence and no
process control around it. The deterministic store runs the same loop as a
reference without a medium (`--memory`).

Command, run on revision `1e4492a184f73dc636f3404b88b4c7cb4e53b0d4` with a
clean tree:

```text
tools/measure_writer.sh <output directory>     # OPS=2000 WARMUP=200
```

Each point is 200 steps of warm-up, then 2000 measured steps, so 4000
measured transactions. The store is created empty for each point and grows
during it: obligations stay pending in S0, so nothing is deleted.

## Machine and storage

| | |
| - | - |
| CPU | Intel Xeon Processor @ 2.10 GHz, 4 CPUs online |
| Memory | 16 GB |
| Kernel | Linux 6.18.44, x86_64, a virtual machine |
| Storage | `/dev/vda`, ext4 (`rw,relatime,discard`), a virtio disk |
| Toolchain | rustc 1.97.0, release profile of the workspace |

The disk is virtual. `synchronous=FULL` makes SQLite call `fsync` at each
commit, but what the host does with that `fsync` is not known here. These
latencies are those of this VM. They say nothing about durability on power
loss, which stays the declared failure model. They are not Raspberry Pi
numbers either: that storage, the dominant risk of R1, is not measured yet.

## Engine and settings, read back

| Setting | Value |
| - | - |
| `sqlite_version()` | 3.54.1, the amalgamation of `libsqlite3-sys 0.40.0` (`bundled`) |
| `journal_mode` | `wal` |
| `synchronous` | 2 (FULL) |
| `page_size` | 4096 |
| `wal_autocheckpoint` | 1000 pages |
| Selected compile options | `THREADSAFE=1`, `DEFAULT_SYNCHRONOUS=2`, `DEFAULT_WAL_SYNCHRONOUS=2`, `DEFAULT_WAL_AUTOCHECKPOINT=1000`, `DEFAULT_PAGE_SIZE=4096`, `TEMP_STORE=1`, `SYSTEM_MALLOC`, `MUTEX_PTHREADS`, `COMPILER=gcc-13.3.0` |

The full list of compile options is in each JSON report. Commits are not
grouped: one transaction is one command. The engine's checkpoint of the WAL
into the database is the only grouping of writes.

## Results

Latencies are in microseconds, as p50 / p90 / p99 / p99.9 / max. The rate is
the number of transactions of the window (both kinds) divided by its length.
CPU is user plus system time of the process over the window.

| Backend | Payload, fan-out | Transactions/s | Window s | CPU s | Peak RSS | `AdmitPublication` | `ReleaseInbound` |
| - | - | - | - | - | - | - | - |
| SQLite | 64 B, 1 | 2900 | 1.38 | 0.62 | 7.1 MB | 373 / 561 / 1633 / 2513 / 2721 | 229 / 354 / 1075 / 2116 / 3913 |
| SQLite | 1 KiB, 1 | 2424 | 1.65 | 0.81 | 7.1 MB | 498 / 645 / 2583 / 3035 / 3283 | 261 / 365 / 624 / 2662 / 3502 |
| SQLite | 1 KiB, 8 | 2262 | 1.77 | 0.83 | 7.2 MB | 569 / 801 / 2599 / 3003 / 5660 | 211 / 325 / 675 / 2485 / 2627 |
| SQLite | 16 KiB, 1 | 2127 | 1.88 | 0.89 | 7.1 MB | 555 / 759 / 4443 / 5613 / 7247 | 261 / 384 / 738 / 4474 / 4815 |
| Memory | 64 B, 1 | 1901 | 2.10 | 2.09 | 9.3 MB | 462 / 1010 / 1451 / 1732 / 2229 | 440 / 926 / 1309 / 1625 / 1647 |
| Memory | 1 KiB, 1 | 1356 | 2.95 | 2.93 | 13.4 MB | 612 / 1293 / 1604 / 1851 / 4850 | 655 / 1413 / 1778 / 1978 / 2672 |
| Memory | 1 KiB, 8 | 1076 | 3.72 | 3.69 | 17.9 MB | 878 / 1409 / 1582 / 1693 / 1713 | 959 / 1541 / 1730 / 1932 / 3842 |
| Memory | 16 KiB, 1 | 449 | 8.90 | 8.84 | 79.3 MB | 2108 / 3826 / 4664 / 5111 / 18491 | 2104 / 3815 / 4657 / 5059 / 21247 |

At the end of each SQLite point the WAL held about 4.2 MB, that is about
1000 pages. The database file held 2.3 MB at 64 B and 38.6 MB at 16 KiB.

Three more runs of the SQLite point at 1 KiB and fan-out 1, on an earlier
revision with the same code path, gave 2373 to 2705 transactions per second,
an admission p50 of 424 to 512 µs and a p99 of 2348 to 2518 µs. One earlier
run had a single admission at 105 ms. It did not happen again in four runs,
and it is not explained.

## What it says, and what it does not

- **Order of magnitude.** On this VM one durable admission takes about half a
  millisecond at the median, and the writer serves about 2000 to 3000 store
  transactions per second, whatever the payload size or fan-out measured.
  An MQTT QoS 2 publication costs at least two of them here, its admission
  and its release. The rate of a broker would be lower still, for the
  reasons given at the top.
- **Where the time goes.** The process uses about 45 % of one CPU over the
  window; the rest is waiting, mostly on `fsync`. This is an inference from
  CPU against wall time, not a profile.
- **The tail.** The p99 of an admission is four to eight times its median,
  and grows with the payload size. The release does not show the same
  growth. The likely cause is the engine's checkpoint, once the WAL reaches
  1000 pages, inside the commit that crosses that threshold. That is a
  hypothesis, not a measurement.
- **The deterministic store is not a reference for cost.** It is slower than
  SQLite here, and slows down as its state grows: 449 transactions per second
  with 16 KiB payloads. That is its stated limit, a copy of the whole durable
  state per command. It matters for the length of bench runs, not for the
  writer.

## One proposed experiment

To test the hypothesis about the tail, run the same point (1 KiB, fan-out 1)
on the same machine and revision, five times each, with only
`wal_autocheckpoint` changed: 1000 pages (the default, as here) and
10 000 pages. Compare the p99 and p99.9 of the admission and the number of
slow admissions. This needs a measurement-only option of the example; the
store keeps its default. Nothing is changed in the store before that
comparison has been made and read.

## Not measured

The Raspberry Pi and its storage. Several writers, or a reader alongside
the writer. Recovery and reopening. The bench around the store, through the
supervised child. Long runs, beyond 4400 transactions per point, and growth
of the database file over hours. Power loss.
