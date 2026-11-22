# Protected outgoing identifier continuation

- Start: connector/native confirmed PR648 HEAD66d2f616256e7f69f0d0483ddb7fabb513b625d8,
  main874f298b4de7c3a13006cb1ff767dfb09e4031ac, draft/unmerged, green checks,
  no human reviews/threads. No receipt-collection general re-audit.
- Red allocator1611a4e7 reproduces CapacityExceeded after known Finish near
  historical65535; fix38fdbb58 removes the cursor and uses bounded smallest-free
  live claims. Shared Rust/Proposed0028 freezeb5400463 passed152 contract tests.
- Real socket red76049091 preserves a dequeued no-target packet until later
  claimPI1, exposing no_target_puback_rebound; immutable pipeline fixd01fd230.
  Target definition is runtime reader-channel dequeue, before owner input;
  upstream raw bytes are expressly outside the association guarantee.
- Initial integrated diagnostic128: Rust7ac2433775d84ac142a66212dd52ef651c92f1ce,
  harness9e9ec6e2df22f1aa68582d7896cb62130e30027e, binarye28c6d56...478c89.
  Public mqttium→Paho receives128/known Finish128, one connection each, PI1
  reuses127, collection applications7, all1962 actual event records valid.
  This is smoke, explicitly not the final65537 reception.
- Store tests6b10fe70 preserve PI reuse across retired receipts, old Finish
  guards, unknown results and recovery before payload. No SQL schema change.
- Core guards/tests end e361b5f5; mutation follow-ups e554bd56/b8e90fb6/7e561e49,
  launcher41d81e8b;270 core+two doctests/152 contract/17 boundary checks pass.
  Three compiled pure mutants fail for their own exact invariant markers.
- Formats01d1a19b red/de893e70 fix:183 oracle,44 Rust readers,452 schema docs;
  corpus434/traces405 current. Digest162a9cd3...e6126d→8884608f...0bb05.
- Focused model040df332 adds12 expected outcomes, old62 unchanged. Archive
  c87e563121e677126dd84c3872bfd320301cb89fc3ea48bef1c13cc418b301b7.
- Integrate remaining actual-copy/Written/unknown-Finish hooks and targeted
  system cases, freeze clean source, run tools/run_pi_reception.sh (strict
  normal65537, reversed clients, short cases and four mutation classes), then
  final required gates and existing receipt/non-regression receptions.
  PR/indexes record exact final SHA, artifacts, hashes and CI; never relabel
  diagnostic runs. Expected remote remains66d2 until protected publication.
  No main merge, implicit migration/session eviction or ADR acceptance.
- Targeted takeover review reproduced global puback_connection_unbound when
  an older Persist blocks the old peer's Close after Owner rebind. Red
  b6f5120f/4eef57d1 and fixf100d8ae preserve a durable-confirmed peer SessionId;
  stale facts retain their old generation. Actual Some-copy/early-ACK/Written
  and old-Finish replay test4452186d passes. Root integrated through056a2d8f.
- Full normal/instrumented Rust suites ran sequentially at4611dad2:882/891
  PASS, both strict Clippy passes, CLI validate accepted11, digest8884608f...0bb05.
  Runtime tests also cover ACK received before actual WireProgress/Written.
- Harness review fixed two evidence gaps test-first: only empty registration
  is not active collection, and quotas are checked at every/final SQL window;
  builder/harness/client imports now share an exclusively owned private pinned
  checkout even if the caller advances. Original red/fix pairs are retained.
- Nine short real C/D/E diagnostic cases pass. The runtime rebinding mutant
  compiles and fails exactly observed_puback_rebound; three pure transition
  mutants retain their separate scope. The final clean wrapper reruns these
  before strict65537 plus reverse clients; no prior source is relabeled.
- First publication9e653055 remains draft/unmerged. Local exact9e pub/sub,
  QoS0 and receipt-GC receptions pass; their own archives retain that source.
  Its PI normal run stops at7746 wire receptions after the collector misses
  an inode-reused log generation; actual Finish seq117302 exists in that run.
  Retain the original FAIL and incomplete-run analysis, never count it as a
  mutation detection. Collector red9e19f6b7/fix83188305 (root796d8319/dfe5cbcb)
  adds two real-file generation regressions; all57 system pilots pass.
- Takeover CI's generic aggregate wait has no recoverable original phase.
  Real-response relay red2ce97b95/fixaea65c4e (rootb65cb718/59b0d12a) separately
  proves controls progress with16 correlated replies held, then drains each
  exact effect within the unchanged one-second bound. Only test code changes;
  normal/instrumented target, fmt, strict workspace Clippy and validate11 pass.
  Freeze the integrated source and rerun the entire strict65537 wrapper;
  publish with expected9e653055 ancestry/lease and verify its own CI/artifacts.
- Correct the remaining obsolete monotonic-PI paragraph in the product profile
  (6661ef3d, documentation only). A fresh strict run receives/finishes11264,
  then stops on collector FileNotFoundError during a real log rotation. Keep
  its original FAIL; add a scan/open rotation regression and preserve cursor
  state across interrupted scans. Restart the full normal case from zero.
  Normal Rust log at546a completes53 result groups/882 tests with zero failures;
  the environment interrupted its orchestration status recording, not cargo's
  complete log. A filesystem-backed runner records remaining gates/statuses.
- Scan/open red27d15887/fixb947a378 covers disappearance before open and an
  interrupted scan after another real file's Finish is already buffered.
  Offsets are committed only for a complete scan; retry uses the existing
  bounded caller, with no recursive retry or acceptance of a genuine gap.
  PI17/system59 tests pass. Freeze this integrated source for a fresh strict
  normal65537 and its own CI; all production Rust is unchanged by these fixes.
- Received f37fe364a28c9e594787120444fd3b2b252ad56e, tree
  0880167c2f3e1bcb204a946c06a7fdf40694f7ca: 70 connector blobs and1993 archived
  Git blobs match; PR draft/unmerged. Normal CI and local runs EACH receive and
  durably finish65537, PI1 reuse65536, one publisher/subscriber/proxy connection,
  same process/file,3073collections/64sampledflooradvances,8192quota unchanged.
  CI765129.064ms, PID6723/inode4752897; local1430923.946ms,
  PID1216151/inode2918080. All9shortcases,4ownSIGKILLs,4exactmutants received;
  independent raw-wire/identity/SQL/hash audit PASS. Each final business state
  is empty with4receipt rows and exit0. Reverse public-client case passes.
- All8mqttiumd CI jobs43938665922 green, including qualifying K1/K2b/K3,
  recompute and K2a/WAL; client43938665778/model43938665739/docs green after
  retry of cancelled-before-start macOS jobs. Normal882/instrumented891,
  oracle183, system59, schema452, models74 expected. Receipt-GC sourcef37
  artifact separately receives32811businesscommands. No new global qualification.
- Local archive ecced6d4a427aa68b72e7c53451431ffe6898dfda63f54d3dcf4dd3b194ed7aa;
  CI inner29a43062b4e224184a0283583de6a890bd9995d94dba292fefdfcb1550e02766,
  outerZIPe4a95280cfbe1ec550b68034eed350cd5b913ac792f87f74ff24e7ae65bcf8b5.
  Report0012 records measured sourcef37 and all limits. Publish this final
  documentation-only successor with expectedf37 ancestry/lease; verify its
  exact-head content/CI and fresh PI index without relabeling historical runs.
  Next increment: explicit safe session lifecycle beyond128 historical sessions.
