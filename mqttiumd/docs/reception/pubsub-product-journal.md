# Durable pub/sub continuation

- Repository/PR: yoch/mqttium, #648, draft and unmerged. Confirmed starting
  head bd579da64fbc6904efbba95608142fdcf3fbf191, main
  874f298b4de7c3a13006cb1ff767dfb09e4031ac.
- First published increment fe90df3ac6c58820d06a48ecd3a196bd4ac187bc fixes
  Receive Maximum using actual wire progress, one bounded gap deferral and
  existing occurrence guards. Four remote changed blobs exactly read back.
- Shared types frozen in 4e6abf75; durable operations, both stores, core,
  codec, writer and network integrated on codex/pubsub-product. ADRs 0024/25
  remain Proposed. Schema 2 refuses schema 1 without changing main/WAL.
- Preliminary normal9/hooks15 pub/sub cases and three exact negative controls
  passed. These diagnostics remain under /workspace/mqttium-ps-reception-evidence
  and are not relabeled as a final published-SHA receipt.
- Additional red/fix increments preserve canonical subscription equality,
  PUBACK-before-DISCONNECT order, deferred SUBSCRIBE/UNSUBSCRIBE order,
  unknown-claim outgoing order and active control PI guards.
- Final receipt commands: tools/run_pubsub_reception.sh, run_tcp_reception.sh,
  run_qos1_reception.sh and run_transactional_reception.sh, each with a fresh
  directory outside the checkout. Report 0009 describes scope and invariants.
  Each index/source-head.txt records its exact clean revision; CI artifacts
  and PR receipt record publication, checks and archive hashes.
- Received source 7411400c27d404949a6d3799813f41cd9efb501f: 802 normal,
  810 instrumented and 55 product-hook Rust tests; 152 oracle tests,
  397 schema documents; pubsub normal9/hooks15 and all three semantic mutants;
  previous TCP/QoS1 receipts; qualifying K1/K2b/K3, recompute and second builds;
  eight typed-read cases. Same-head archives under /tmp/mqttium-ps-final-*.
  Pubsub archive SHA-256 d727ee2c68b2b033a2b87f367b340969173cade1a14b35bb148e0babe0c953ec;
  transactions 697ee6556d93c73a066306a8447fbc689d29857923edd58a28e0eb95fd78ee45.
- Connector read-back: 71 changed source/schema/guide blobs match. CI product,
  models (52 expected outcomes), proof and client jobs passed; contracts
  stopped because M3's injected recipe match lacked the three new variants.
  This compilation failure is not an invariant detection. The following
  patch-only correction preserves M3's premature PUBREC and expected reason.
- Next action: execute both repaired mutation loops, publish without
  overwriting a changed remote HEAD, and receive the fresh exact-head CI
  (including its own source/binary/campaign evidence). The 7411400c archives
  retain their original identity; they are not relabeled. Then QoS 0 is the
  next functional increment; no main merge/global ADR acceptance is authorized.

Contract digest d789be0eb72021c4c77b60132c097c558b81cc72bd5899fdea74a5fb26e42193.
