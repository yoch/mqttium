# Decision records 0010 to 0043: a proposal for their acceptance

Thirty-four records are applied by the code and still read "Proposed".
Only the repository owner accepts a record. This page proposes a decision
for each, with the reason, so that they can be settled in one pass. It
decides nothing itself.

Accepting a record changes the derived requirement register, hence the
contract digest that `validate` prints (seen when record 0044 was
accepted: `b08617f1…` became `fe14cd49…`). Accept the chosen records in
one commit, then run `python tools/build_corpus.py` and state the new
digest.

## What the proposals rest on

- The records were not re-audited line by line for this page. The
  proposal "accept" means: the record is implemented, the checks that
  exercise it pass at the revision this page is committed with (jobs
  `contracts`, `models`, `pilot proof` and `runtime`), and records 0044 to
  0046 do not contradict it.
- A text search of every record for the runtime details that records
  0044 to 0046 changed (connection limit, polling, per-connection threads,
  the 100 ms input wait) found two records, 0022 and 0043. They are
  proposed with an amendment note, not rewritten.

## Proposals

| Records | Subject | Proposal | Reason |
| - | - | - | - |
| 0010 | Credits interface of `release_if_current` | Accept | Proved by the harnesses of `proofs/credits` (job `pilot proof`), pinned by `BODY.sha256` |
| 0011, 0012, 0014 | Epoch opening, reservation refusals, recovered facts | Accept | Contract types and core behaviour of the first wave; exercised by the oracle campaigns and the conformance suites of both stores |
| 0013 | Twelve behaviours the chapters leave open | Accept, and read again when records on backpressure follow 0046 | Its closure rules are the ones 0046 narrows for input pressure; 0046 names what remains |
| 0015, 0016, 0017, 0018, 0019, 0020, 0021 | Scenario vocabulary, evidence formats, binary tied to source, permanent delay, real kill, kill inside a transaction, bounded durable reads | Accept | The formats and injection points of the lab and the oracle; every qualifying campaign depends on them. Record 0045 keeps the two points of 0020 and says how a group passes them |
| 0022 | First product TCP composition | Accept with a note | Its "initial product budgets" (16 connections, queues of 4, timers 512) are now the bounded profile of `runtime::Limits`; the connection limit is an option since 0046 |
| 0023, 0024, 0025, 0026, 0027, 0028 | Incoming QoS 1, receive credits, exact subscriptions and outgoing QoS 1, volatile QoS 0, receipt retirement, identifier reuse | Accept | Product increments covered by the white-box runtime tests in both configurations; the per-frame receipts of 0024 are kept by the reactor transport |
| 0029, 0030 | Session lifecycle, finite session expiry | Accept | Covered by the runtime tests and the store suites |
| 0031, 0032, 0033, 0034, 0037 | Hierarchical subscriptions, retained, message expiry, Will, retained Will | Accept | Covered by the runtime tests, the store suites and the wildcard mutants |
| 0035, 0036, 0039 | mTLS profile, offline policy transitions, per-filter authorisation | Accept | Covered by the security and runtime tests |
| 0038, 0040, 0041 | MQTT 3.1.1 wire profile, incoming and outgoing QoS 2 | Accept | Covered by the codec and runtime tests |
| 0042 | Checked single-listener Mosquitto cutover | Leave proposed | Its kit is frozen evidence of the executable built from `51be4079` (record 0044, report 0026). Decide it with the reader of `mosquitto.conf` planned for version 0.1 |
| 0043 | Bounded reactor progress under transport pressure | Accept with a note | Its rules hold; the ordinary input FIFO it sizes at 128 slots is multiplied with the connection limit, and 0046 defers a packet refused for effect slots where 0043 closed it |

## What acceptance would not say

- That the broker conforms to MQTT: the statement register under
  `conformance/` is the map of that evidence.
- That a frozen reception still passes on the current executable: they
  are not repaired (record 0044).
- Anything about records 0045 and 0046, which are proposed with the work
  of this branch and should be decided after their models are reviewed.
