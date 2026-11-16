# Live receipt collection continuation

- Confirmed start: PR648 HEAD3ad41e95c9153df051a54a86162683606d9d62fe,
  main874f298b4de7c3a13006cb1ff767dfb09e4031ac, draft/unmerged. Connector
  metadata, comments, green checks and native fetch agree. Review attachment
  is evidence, not additional authority. No general QoS 0 re-audit performed.
- Historical red regression: original normal binary644f391f..., source3ad41e95,
  8192 positive SUBACK replacements, one route, zero publications/obligations,
  next result unknown, PING progresses, restart remains full. Distinct from
  review's normal binary265d5ed9.... Tests30dbe8a3/f6ef8cfd retain own identities.
- Contract freezeeaaae456: Proposed0027; per-origin operation/publication/payload
  closure; no maintenance OperationId/receipt; bounded controls, holes and
  physical sweeps. Contract unit suite and store API check passed. Previous
  digest6d1a0045...a6c3042c7; derived formats/new digest follow the consumer lot.
- Separate core/store/runtime/format/model/reception lots are integrated.
  Code checkpoint e4edacf0 includes exact collector timer classification,
  reserved input backpressure/fairness, startup cancellation, the full-window
  fenced Release retry, partial-sweep recovery and all namespace guards.
- Complete diagnostic at1e785746: normal A+B/D/G plus six hook runs and five
  compiled mutants PASS as expected; archive3858f77f...75e8c071b. Same-run
  strace, actual barriers, source/binary/client identities and SQL snapshots.
  B:32769 replacements+42 publications=32811 business completions, same process/
  file/main connections, receipts4..64/8192, RSS9.21..9.26MB, DB20pages, WAL4.14MB.
  Historical red archive580317a0...776cccae remains separate and unchanged.
- Independent review confirmed/fixed full-window Release deadlock, absolute
  guard observation and ordinary deadlines occupying the maintenance reserve.
  Model-only b16c0eb9 has ten exact TLC outcomes, archivecca014ee...8dd401.
- Final descriptive registry includes deferred Release; current digest is
  92d4192f1f701224395818686f52caaff59ff6859569c62f8570650bddec5cf9.
  Canonical recipe kinds1..7 remain unchanged. SQLite schema4 refuses1/2/3.
- Next: receive the clean final command tools/run_receipt_reception.sh, normal/
  instrumented suites sequentially, old product regressions, fresh K1/K2b/K3
  and exact-head CI; publish with expected remote HEAD3ad41e95 protection.
  Final SHA, archive hashes and check identities belong in PR648 and indexes,
  without retrospectively relabeling diagnostics. No merge/ADR acceptance.
  Outgoing Packet Identifier recycling remains the next operational increment.
