# Message Expiry continuation journal

## Remote authority and design

The GitHub connector confirmed draft/unmerged PR648 at
`795d200929db643618be22cd01c5538f78ed1d96`, base
`874f298b4de7c3a13006cb1ff767dfb09e4031ac`, branch
`claude/optimistic-feynman-fqh947`. Reviews/threads were empty and all published
workflows/docs succeeded. The local/fetched tree matched
`47ab54933a9fb93fc82df8c50b7b5994c1caaf9d`; a targeted connector file patch and
the canonical command blob were read before edits. The full-PR patch transport
closed; this was tooling, not a broker observation.

Proposed0033 freezes raw domain/tick/interval before ingress deferral. Unknown
continuity quarantines old expiring data rather than renewing its lifetime.
The socket writer gates each zero-progress attempt. Only actual positive-write
observations can establish a separate durable Started marker. Missing proof
after crash remains uncertain; known-started QoS1 keeps its reference/PI through
Finish. Source, protocol and publication identities are not reused.

The shared contract lot was integrated before consumers. New tags13..16 leave
1..12 unchanged. Schema9 refuses historical formats before mutation. Contract
digest was `948a42f7161f97e3cd0cfe4d5b37bee5f9b56325b1a4b69f1c7124b97aac5412`;
current format/schema digest is
`36a15f70c77acaf2611ccd5a955d9898fc65de2bcac93b861d607ac3b2d95f1e`, to be
confirmed again by the final validator. Business quotas remain unchanged.

## Executed increments, each with its own scope

- Codec RED `9a4ac487` rejects property0x02 before the fix; wire implementation
  and58 isolated module tests pass. Default/instrumented focused Clippy pass.
- Core340 tests plus2doctests pass in its private worktree. Twenty-five expiry
  cases cover queued/started/uncertain work, held ACKs, original lifetime,
  stale facts, receipt-window holes and reservation rollback. Additional RED
  commits exposed and fixed missing time, excessive certainty and cache stalls.
- Both stores implement immutable metadata, atomic timed admission/replay,
  guarded Started/Expire, exact shared references and four-publication scans.
  SQLite142 normal/157 instrumented tests passed sequentially before the last
  frozen-sweep race fix; its17 expiry tests and deterministic cases pass after
  that fix. Future admissions are skipped by a retried older as-of sweep, not
  called clock corruption; a fresh pass still ages them.
- Real public RED at `ecc9a35b` is retained separately under
  `/workspace/msgexp-ecc9a35b-red-exit`: the old normal broker refuses0x02.
- Clean integrated candidate `86662b64d687f7e32ba999df1271529387e96286` ran the
  first real retained-TTL2/no-subscriber path: durable admission, physical
  reclamation, no stale wildcard replay and a succeeding timeless value.
  `/workspace/msgexp-86662b64-minimal/receipt.json` is a system PASS only.
  Its120 live matrix cells,32 valid replays, options, late-PUBACK and malformed
  cases also pass diagnostically. Offline harness assumptions were corrected
  explicitly: infinite sessions need not have a detachment row, QoS0-derived
  durable copies count too, and replay metadata can legitimately be transient.
- Candidate `b447c3e6` normal copy is
  `/workspace/msgexp-b447c3e6-normal`, SHA256
  `a31d186178e0026c520ae1940aafbff0b7a8f428b9f338eff5acc42c80fe9577`.
  Its actual normal restart passed with the observed domain policy.
- Six compiled controls pass diagnostically at `842a23b9` (b447 plus a
  helper-only test-target correction). Positives0, executed mutants101 with
  exact assertions, distinct binary hashes and restored sources. Receipt
  `/workspace/msgexp-mutant-diagnostic-b447-lib/result.json`, SHA256
  `de153e5c86935d6f7e1ebdc7b76776a5d5dca2eab3e912826e09b8eb4894a279`.
  An earlier zero-test target mistake is preserved as FAIL, not a detection.
- Finite model:15 expected TLC outcomes match; all16 tooling tests pass.
  Maximum interval uses an explicitly finite abstraction, not a wire-integer
  proof. Existing model/qualifying-campaign scopes are unchanged.
- Product integration117 default/125 hook unit tests and both Clippy variants
  pass in the control worktree. Authentic shared sweep-result RED20680885 is
  fixed by c6efc5b9: bounded copy, stale result ignore, frozen scan observation,
  three retries and explicit retry/applied facts. Root receives this code;
  final workspace/system gates remain required on the final content.

## Next actions

Execute the new normal package and expiry demo, then the source-pinned
`tools/run_message_expiry_reception.sh` (normal cases, genuine hook cuts,
six compiled controls, strict evidence and hashes). Run required workspace
gates sequentially for normal/instrumented suites. Publish with an expected
remote SHA lease, receive its CI artifacts, download/extract/execute the actual
normal archive without Git/Cargo, and independently recompute freshK1/K2b/K3.
Do not relabel the diagnostic candidates above as that final execution.

The PR remains draft/unmerged; ADRs remain Proposed. Unknown-origin quarantine,
ambiguous claimed-transmission availability and physical-departure/preemption
limits must stay visible. No migration, Will, TLS/authentication, shared
subscriptions, outgoingQoS2 or general performance/conformance claim is added.


## First full-gate findings and targeted fixes

Source-pinned fc8ae1e9 passes the extracted normal public retained+expiry demo:
its observed restart outcome is temporal quarantine, not proven expiry. Its
full system run correctly refuses the capacity harness, which counted historical
PUBACKs sharing the legitimately reused PI1; d1d45b1f scopes the reader to a
pre-publication frame cursor. The actual negative000197 remains in the untouched
failed capture; it is not a broker defect or a mutation detection.

The full Rust gate exposes an old child-directory collision, unchanged since795:
two factories with the same parent/rank/count removed each other's live sockets.
RED482dbe05/GREEN9a39cb5c uses checked per-spawn nonces, exclusive creation and
owned cleanup. All17 actual child/control tests and both concurrent typed-read
receptions pass; signal/wait/drain behavior is unchanged.

Independent flow review confirms a new TTL-origin defect before this delivery:
framed packets could wait unstamped in the reader or pending-CONNECT queue.
REDcc9774bf/3d9235cc, GREEN34296671 freezes the same clock's result at full-frame
completion and carries errors/observations unchanged. One reader watermark
refuses regression/domain changes without comparing an old queued observation
to a newer global watermark. Its122 normal/130hook product tests and both
Clippy configurations pass in its private worktree. Final source-pinned whole
reception and workspace gates must now be regenerated on the corrected content.

## Integrated gate and network reception checkpoint

The root default workspace at aa8efdcf passes strict Clippy and1102 Rust tests.
The subsequent d635dffb instrumented workspace passes strict Clippy and1117tests,
run after the normal suite. Validator confirms the digest above and16documents.
Corpus577, second-reader600documents, historical405traces and255oracle tests
pass without rewriting historical evidence. Independent read-only review of
d635dffb finds no concrete safety defect in the frozen ingress, early PUBACK,
Started/uncertain-claim and sweep chain; it explicitly retains the documented
pre-write sample/physical-departure limitation.

Source-pinned d635dffb receives all9 normal groups (120live matrix cells,
24offline cells,32valid replay, capacity recovery), its normal restart and all11
separate own-run SIGKILL cases. Three wildcard and9retained boundary receptions
also pass at that exact pin, independently under
`/workspace/message-expiry-old-boundaries-d635-169`. These are system tests.
The full expiry receipt remains FAIL: its held-admission harness waits8seconds
while the unchanged product resolution delay is10seconds; its original client
waiter also expires at5seconds. RED47501ec8/GREEN09a380b3 forwards a bounded15s
completion budget to the same receipt and waits12s for the same operation's
real resolution. This is a harness correction, not a broker defect or timeout
mutation detection. The failed output is preserved at
`/workspace/message-expiry-final-candidate`.

Strict legacy recipe readers exposed incomplete test fixtures (missing retained
bitmap/replay identity suffix). Fixture-only9dc8cd49 keeps those readers strict
and the targeted identity rejection; the full pilot discovery passes169tests
in that private worktree. Root must run the corrected full expiry reception,
including held outcome, certain-unsent control and six compiled mutations,
before recording its reception. All119finite model configurations are being
checked separately at d635dffb; no final whole-model verdict is claimed yet.
