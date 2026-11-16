# Explicit session lifecycle continuation

- Remote starting authority: PR648 draft/unmerged, HEAD
  a6db4e50650415e1db832bc8e055ff4962c3a86a, main
  874f298b4de7c3a13006cb1ff767dfb09e4031ac. Local tracked content matched.
  Existing PI/receipt/pubsub evidence remains attached to those revisions.
- Proposed0029 freezes the public zero/infinite retention matrix, receipted
  Open/Close authority, durable attachment token and target-only atomic cleanup.
  Root owns common types. Before-contract digest:
  8884608f0e66e905307a546b6296934099db688ae5b2829d187dc816dda0bb05.
- Store RED9b04a9ad89b1706ffedeabf0dc150a54d82c98ba compiles and fails both
  existing-interface512-cycle cases exactly session_terminal_history_grew.
  Functional bd27eb05 physically erases ended sessions; schema5 persists
  retention/attachment and refuses older formats without modifying them.
  Followup04c7c40d98e4f963c91a569556c8d85677290420 passes9 differential cases,
  including full128 replacement, shared references, receipt collection/replay,
  initial-policy validation and typed atomic startup expiry.
- Core RED7ccd9df2/GREENdbdad52d introduces one correlated lifecycle flight.
  RED656c8746/GREENaeb14117 additionally receives refused zero-policy takeover,
  planning-time capacity validation and stable other-destination route order.
- Writer8f46d248/80e491a1 adds a reserved correlated lifecycle lane and actual
  SQLite prepared/committed barriers. The root reserved-lane test passes with65
  authentic ordinary replies unread: collection, Open, same-operation Resolve
  and Close all commit/reply independently. Physical quota8192/controlreserve512
  remain; business threshold640 leaves384 per-origin continuation positions.
- Runtime266c332a passes the first actual socket/writer ephemeral cycles and
  persistent-to-zero resume/SP1 then termination. A complete product suite
  exposes an initial work-rearm gap at accepted CONNACK; runtime owner is
  correcting it against the unchanged autonomous12-message regression.
- Intermediate normal-client diagnostics at agent integration88a667c1 pass
  A/public matrix, B512 complete distinct ClientIDs and D/target-only loaded
  cancellation. B records no terminal session rows, all zero postdrain runtime
  cardinalities and restart; ledger72719fa5c9417c59f5d4e47c90a15971836b71fa80f3d9a02bacbb56795d97d1.
  These are diagnostics, not final published-SHA evidence.
- Next: integrate runtime/adversarial fixes, execute full-capacity C and E/F/G,
  exact compiled mutants, freeze source and run tools/run_session_reception.sh.
  Run normal/instrumented gates sequentially and existing system receptions;
  publish with expected a6db4e50650415e1db832bc8e055ff4962c3a86a ancestry/lease.
  Reread remote content and exact-SHA CI. Never merge main or accept Proposed ADRs.

- Final integrated runtime includes real RED/GREEN tests for early DISCONNECT0
  during unknown Open, close facts under ordinary pressure, future close
  reservation at accept and shared frame/terminal reservation; related guards
  are247171aa. Final runtime7e3929e0 (rootff825434) passes normal83lib/hooks87lib
  plus its binary tests and strict product Clippy. Normal shutdown gives exact
  zero-session ends2s, reports unknown outcome if incomplete and preserves
  typed startup/read cause. An actual held WorkPage for a retired target is
  ignored even in the instrumentation release path.
- Targeted independent review found exactly the two reservation overcommit
  schedules; their real-socket RED/GREEN tests now pass. Review stopped after
  those local guards. Model has8expected outcomes/79762safety states, unchanged
  old74; all eight integrated outcomes pass. No protected/French files changed.
- Formats/schema515/corpus496/traces405 are consistent. Contract digest is
  c249cd468b460b8304b043a5ced515fee14ac13d3f5bb676d5dd0f0180b5c897.
  Original diagnostic full-suite source changed during its schema corpus scan;
  its expected-overflow schema mismatch is kept as nonfinal diagnostic. Fresh
  compiled readers then pass contracts18/store_evidence13/kind8example1.
  All final suites must use a private source pin, with normal/instrumented
  workspace runs sequential in that checkout.
- Last finite addition before freeze: actual held committed source admission
  across source termination, same-operation reconciliation and surviving other
  destination. Then pin one clean source for full A-G, full Rust gates, old
  receipts/PI/pubsub/transaction CI and protected publication. The final run
  index/source-head, PR receipt and exact-head checks supply publication binding;
  no diagnostic archive is relabeled.
