# 0002 — The tail of the writer against the checkpoint of the WAL

Lot L7. This is the narrow experiment proposed by report 0001 and agreed
by the integration owner. **Exploratory: it qualifies nothing.** It counts
durable store transactions on one machine, not MQTT messages. Report 0001
is not rewritten; this one completes it.

## Question

Report 0001 found that the p99 of an admission is four to eight times its
median, and supposed that the cause is the engine's checkpoint of the WAL
into the database, run inside the commit that crosses `wal_autocheckpoint`.
Does the tail follow the checkpoint? If it does, what does a rarer
checkpoint cost when it does happen?

## Method

Command, on revision `e1cbb7cddbbfc6e3d61ba2e3e8e6b30cf8be04b2` with a
clean tree:

```text
tools/measure_wal_checkpoint.sh <output directory>     # OPS=2000 WARMUP=200 RUNS=5
```

- One point: a 1 KiB payload, fan-out 1. Each step is the real
  `AdmitPublication` then its `ReleaseInbound`, one transaction each, WAL
  and `synchronous=FULL` read back. 200 steps of warm-up, 2000 measured.
- Ten runs, alternated: five with `wal_autocheckpoint` at 1000 pages (the
  default) and five at 10 000 pages. The value is set on the writer's
  connection and read back. It is set by the measurement only, through
  `SqliteStore::set_wal_autocheckpoint_for_measurement`; the store keeps
  the engine's default.
- The same machine as report 0001: a VM with 4 Xeon CPUs at 2.1 GHz and
  ext4 on a virtio disk, where `fsync` is worth what the virtual disk makes
  of it.
- For each run: the latency percentiles, the number of admissions and of
  releases above 2 ms and above 10 ms, the ten slowest with their step, and
  the largest size of the WAL file, sampled after every step.

## Results

Latencies are in microseconds.

| Pages | Run | Transactions/s | Admission p50 | p99 | p99.9 | max | Admissions > 2 ms | > 10 ms | Release p99 | max | Releases > 2 ms | > 10 ms | WAL max MB |
| - | - | - | - | - | - | - | - | - | - | - | - | - | - |
| 1000 | 1 | 2513 | 470 | 2505 | 3495 | 3971 | 33 | 0 | 698 | 3687 | 9 | 0 | 4.2 |
| 1000 | 2 | 2271 | 549 | 2562 | 3398 | 4569 | 31 | 0 | 761 | 2969 | 8 | 0 | 4.2 |
| 1000 | 3 | 2276 | 535 | 2781 | 3579 | 4919 | 28 | 0 | 700 | 3362 | 9 | 0 | 4.2 |
| 1000 | 4 | 2084 | 582 | 2780 | 3244 | 7093 | 39 | 0 | 1371 | 4381 | 13 | 0 | 4.2 |
| 1000 | 5 | 2210 | 559 | 2812 | 3313 | 3932 | 31 | 0 | 806 | 3812 | 11 | 0 | 4.2 |
| 10 000 | 1 | 2281 | 513 | 982 | 2994 | 13075 | 4 | 2 | 708 | 64082 | 4 | 2 | 41.2 |
| 10 000 | 2 | 2069 | 575 | 1208 | 3730 | 13829 | 8 | 2 | 894 | 11979 | 6 | 1 | 41.2 |
| 10 000 | 3 | 2102 | 592 | 1029 | 11591 | 23284 | 11 | 3 | 613 | 11080 | 5 | 1 | 41.2 |
| 10 000 | 4 | 2096 | 554 | 1408 | 9613 | 14079 | 14 | 2 | 816 | 11613 | 1 | 1 | 41.2 |
| 10 000 | 5 | 1735 | 555 | 1193 | 11033 | 223558 | 6 | 3 | 798 | 242840 | 5 | 2 | 41.2 |

Two further facts come from the JSON reports.

- At 10 000 pages, the two slowest admissions fall on steps 978 and 1562
  in every run, at 11 to 14 ms each. A step writes about 17 WAL frames:
  the WAL reaches 41.2 MB, about 10 000 pages of 4 KiB, after some 590
  steps.
- At 1000 pages the WAL never exceeds 4.2 MB. 2200 steps at about
  17 frames each give some 37 checkpoints per run. The number of
  transactions above 2 ms, admissions and releases together, is 37 to 52
  per run.

## What it says

- **The tail follows the checkpoint.** At the default, the number of slow
  transactions per run is close to the number of checkpoints, and a slow
  one costs about 2 to 4 ms against about 0.5 ms. At 10 000 pages, the
  admission p99 falls from 2.5–2.8 ms to 1.0–1.4 ms, and the slowest
  admissions fall on the same two steps in every run, the steps at which
  the WAL reaches the threshold. This supports the hypothesis of report
  0001. It is an inference from counts and places, not a trace of the
  engine.
- **The cost moves; it does not go away.** A checkpoint of about 10 000
  pages takes 11 to 14 ms in the commit that triggers it, against 2 to
  4 ms for 1000 pages. The WAL grows to 41.2 MB instead of 4.2 MB. The
  rate of transactions does not improve: 2084–2513 per second at 1000
  pages and 1735–2281 at 10 000, within the spread of the runs.
- **Unexplained stalls.** Three runs at 10 000 pages had a transaction far
  above the checkpoint itself: 64 ms in run 1, 23 ms in run 3, 224 ms and
  243 ms in run 5. None was seen at 1000 pages in these runs. Report 0001
  saw one 105 ms admission at 1000 pages. They are not explained. On a
  virtual disk they may come from the host; this experiment cannot tell.

## Conclusion

The store keeps the engine's default of 1000 pages. A larger threshold
trades a shorter p99 for rarer, longer stalls and a WAL ten times larger,
with no gain in rate on this machine. That is not a correction, and nothing
is changed in the store.

If the tail matters later, the question to measure is a different one:
whether a checkpoint outside the commit path, at a point the writer
chooses, keeps the p99 without the stalls. That would be a design change
and needs a decision of its own. It is not proposed here.

## Not measured

The same experiment on the Raspberry Pi and its storage, where the cost
of a checkpoint is the one that matters. Other payload sizes and fan-outs.
The time spent inside the checkpoint itself, apart from the commit that
runs it.
