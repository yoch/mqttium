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
