# Product TCP continuation journal

- Remote reference confirmed through GitHub: draft PR #648, branch
  `claude/optimistic-feynman-fqh947`, head
  `d1cd81b46d9554a0b5ff7f73c487db02360be82f`, base
  `874f298b4de7c3a13006cb1ff767dfb09e4031ac`; no reviews or threads;
  the three prior workflows were green. Native fetch and checkout matched.
- Historical contract digest:
  `sha256:880b0f581567526780da13b4ea66b29e43d32bb781fbeb64c300f4644e8f8c67`.
  Proposed record 0022 adds confirmed session/transport inputs and a separate
  bounded product profile/event format. New digest:
  `sha256:775794a5e47f5e1d881f0572cdc3e64fa250d46e796e7474d9c5854c102fa5cc`.
  Historical proposed decisions are still proposed.
- Work is isolated on `codex/tcp-qos2-product`; core, codec, socket, store
  and reception lots use separate worktrees and Cargo targets. Normal and
  instrumented full suites in the integration checkout run sequentially.
- First executable: a new SQLite file, Paho MQTT 2.1.0 (MQTT 5,
  Clean Start false, infinite session expiry), one completed QoS 2 exchange,
  followed by Session Present true on reconnection. Admission/release receipts
  remain; inbound/publication/payload/obligation rows are reclaimed. Initial
  system receipt `/tmp/tcp-A-first/receipt.json` is development evidence,
  not the final SHA's receipt and not a qualifying lab campaign.
- Executed initially: all normal workspace tests; normal and instrumented
  strict Clippy; 142 oracle tests; the two schema readers agree on 377
  documents; derived corpus/traces regenerated; P1-02 accepted (8 documents).
- Integration review found capacity head-of-line blocking, fresh-session
  CONNECT pipelining and stale PUBCOMP receive-window accounting. Executable
  regressions precede fixes; final reception must rerun the product and
  preserve its own binary, client, database, barriers and process receipts.
- Full development A–H system receipt passed in `/tmp/tcp-first-complete-3`;
  the independently repeated receipt is `/tmp/tcp-complete-reception-3`.
  These are not final-source receipts. Root reactor regressions now pass,
  including actual delayed PUBCOMP receipt/reuse and bounded refusals.
- The final wrapper retains four actual executed binaries, a source archive,
  effective feature trees, build/client commands, client/durable/process
  observations, per-run strace, precise mutant failures and SHA256SUMS.
- Next action: finish the final sequential checks, run the clean-source wrapper
  and fresh transactional qualification, publish without overwriting concurrent
  work, then verify the exact remote head/content and every SHA-specific check.
