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
  162a9cd3b9e1f5a623ebcba47f75ac852e1c5c02674867dd74496b03a5e6126d.
  Canonical recipe kinds1..7 remain unchanged. SQLite schema4 refuses1/2/3.
- Published checkpoint f17a9f36:864 normal/873 instrumented Rust tests,71
  product-hook tests,174 oracle tests and all four old product wrappers pass.
  K1/K2b/K3 qualify/recompute/rebuild on that exact checkpoint; keep their
  source labels. Its main normal run completed32811 business commands.
- Final reception under build contention exposed two probe-only bugs: treating
  0.3s silence as pressure and selecting an interim Unknown resolution. Two
  committed red/fix pairs now require causal fresh pressure plus same-origin
  SQL floor/span, and a post-release same-operation terminal result.42 pilots
  and traced corrected C pass; runtime/contracts/models equal f17a byte-for-byte.
- Next: receive the fresh clean final tools/run_receipt_reception.sh and fresh
  K1/K2b/K3/source reconstructions, then exact-head CI. Expected remote f17a9f36
  supersedes the original3ad41e95 check. Old local wrappers remain f17a evidence;
  final CI reruns those on the actual publication. Do not relabel prior runs.
  Final SHA, archive hashes and check identities belong in PR648 and indexes,
  without retrospectively relabeling diagnostics. No merge/ADR acceptance.
  Outgoing Packet Identifier recycling remains the next operational increment.

- Final profile edge: red8cf2cb18 exposed128 legal eight-filter recipes
  exceeding a1MiB page (1,076,224bytes); API6a61c42e and store8791ff2b use
  a separate2MiB collection proof budget, preserving ordinary pages/row/quota
  bounds. Actual both-store normal21/instrumented22 cases and Clippy pass.
