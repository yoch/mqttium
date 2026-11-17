# Finite session expiry continuation

- Starting remote authority: PR648 draft/unmerged, a44136e1e91c7d25c54e7a28520bea5334b814b9,
  main874f298b4de7c3a13006cb1ff767dfb09e4031ac. No intervening changes/reviews;
  all three Actions workflows green. Native checkout and remote runtime blob match.
- Actual-writer DISCONNECT0 regression RED5bf0b006 fails for retained durable
  session, not timeout. GREEN6b583782 handles the result queued before the
  packet's own drain and the existing post-Peer-loss permutation. Published
 6b583782feda10ae9dd2f1d7a0dd42d47c8e4692, remote SHA/runtime blob confirmed.
  Required formatting/Clippy/unit suites pass; doctests rerun in isolated
  publication checkout after a shared-target artifact collision, plus validate12.
- Contract RED e2a7aa1c precedes GREEN3bcf3d9e. Proposed0030 freezes logical nanos,
  exact detach occurrence, conditional Expire/OpenAt and a singleton clock CAS.
  Root owns integration; contracts/core, stores, writer/time, models and system
  reception have distinct file owners. No proof-protected/French files change.
- Intermediate runtime667aabe2 passes96 product tests. Normal diagnostic
 07ec44d6966d770dd072744bc5c441d4142a5c0846e90d2c779e71d53a05d405 and hooks
 13b78fb17387868d373effb7b51f643482b873ebebc600df266c91a6c38677f2 execute
  public matrix, shared responsibilities, full128/no eviction,64 rearms,
  two genuine held outcomes and five separate SIGKILL/same-file cuts. Their
  captured tracked modifications make these diagnostics, not final-SHA receipts.
- Focused review found one integration-only lane bug: blocked Expire moved
  into ordinary inputs. REDfc6768df fails blocked_expiry_escaped_its_reserved_lane
  with128 ordinary facts and a real committed but withheld Open result.
  Keep expiry in its reserved128 lane and report effective business window512.
- Next: finish schema/readers/corpus, clean-source reception, bounded compiled
  controls, final sequential normal/instrumented gates and current CI. Publish
  with lease against confirmed6b583782; reread remote content and exact-SHA jobs.

- Clean78d2aeb5 launcher PASS294.338s, archivea7fea47894cbc4575a6b93ca55ffd97b344b58896a0be7b9f8f407f3b3db61ff.
  Normal354ccb2a…74fd8/hooks1eb094a3…f1723;2131sourceblobs/368hashes/369packedfiles
  audited. PublicB/E/F/corruptimage, genuineunknown3/7,5ownSIGKILLs and4compiled
  exact controls pass. Independent auditarchive9e55421c5515daae85057407b49cc27bc1c1c71011b06877dc844a876598f6a4.
- Clean938dcd1e gates PASS:968normal/987instrumented sequential,fmt/Clippy/validate13,
 218oracle/91pilots/537schemas;newexpiry8+oldGC10 expectedmodels. Furtheroldmodels
  stopped by explicit localbudget, not a detection; full90remains CI. Gatearchive
 abaa2eb2168ab6d106bd04beda48be6cec97c5366552ffbe25f20bf09077d734.
- Actual7666-event stream check exposed preexisting SessionLifecycle command_kind
  missing from schema; REDa1ed87b3 fails actual_lifecycle_event_not_described.
  Describe generic adapter kind explicitly; writer retains specific action kinds.
  Add bounded real-stream reader to final reception, preserve old78d artifacts.
  Contract digest5fada9d9…0ad0 becomes9269cee70ac3eaa90d78458e2227f2854bae0045d4c28c7842d264dc375493c7.
- Final source also contains checkpoint-CAS age regression765c48ef and receipt
  pressure-window RED570bd417/GREENaa20896d (profile512/1024, quota8192 unchanged).
  Next action: commit stream-reader/report, run219oracle/currentformat readers,
  publish with lease against6b583782 then receive exact-SHA CI10jobs/artifacts.

- Published31d9731a4bb7123b095a20ae476eeb2a03c95c42, remoteHEAD/tree/eightcritical
  blobs match; PRdraft/unmerged. Current219oracle/102pilots/538schema docs,
  strictClippy/99producttests/validate13 pass. CI44034680874 reveals a setup
  omission: oldproductjobs install only Paho, while newdiscovered stream tests
  import jsonschema. This ModuleNotFoundError is not a broker/control detection.
  Add shared pinned schema-reader requirements transitively to system tests;
  verify a fresh isolated environment, publish the corrected source and receive
  its own CI. Preserve31d failure artifact/source identities and all earlier runs.
- Exactpublished31d localreception PASS184.927s, including newstrictstreamgate:
 16streams/7658events/0skipped. Archive14653d7c45a33742d785817e764acbfc355f8aff5a712a2c297df50273181881;
 source0f9f603c6a42a5a8e016a03c9a02eb0e7cd2424600787e53b11dd8570207326d.
 Normal8cfec8e9df08f29d4556d7225c45a8dc79effb82c35a218a25f5041e59fdb6f2;
 hooks1d0740aaab597a03806e21761df977cf846c1fe28455549110dcef1285473a89.
 Audit2135sourceblobs/371hashes/372packedfiles and five ownSIGKILL witnesses pass.
- The corrected transitive system requirements install into a fresh Python3.13
  environment; all102pilots pass there. No broker Rust, contract, model or quota
  changes follow31d. Publish this setup/journal correction, then wait for its
  exact-head CI and fresh qualifying campaign artifacts; earlier failures remain
  tooling failures, never negative-control detections.
