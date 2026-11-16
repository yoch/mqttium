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
- Next action: run normal/instrumented complete suites sequentially, execute
  clean system receipts, publish without overwriting a changed remote HEAD,
  read back content and receive CI of that exact SHA. Then QoS 0 is the next
  functional increment; no main merge or global ADR acceptance is authorized.

Contract digest d789be0eb72021c4c77b60132c097c558b81cc72bd5899fdea74a5fb26e42193.
