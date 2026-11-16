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
- Published implementation: `f0cc192f31b440ee64e5132e6b95a83761ff4a2f`.
  All 53 modified blobs were read back through GitHub and matched. PR #648
  remains draft. Final local suites: 740 normal / 748 instrumented Rust,
  142 oracle and 9 independent pilot tests; strict Clippy/formatting,
  362 corpus and 405 trace files current, 377 schema documents agreeing.
- Clean-source product receipt of that exact SHA:
  `/workspace/mqttium-tcp-evidence/f0cc192f/index.json`; archive
  `f0cc192f.tar.gz`, SHA-256
  `80bcdaf3928b38d2a51fe5d3e402371480e4e9ce2e89186b79270ddfc0196cf8`.
  A–H pass; exact `premature_pubrec` and `stale_generation_forwarded`
  controls fail as required. Actual wait -9 and each run's own strace remain.
- Its product CI job also passed. Artifact 14191085292 was downloaded;
  ZIP SHA-256 `18803d2079db81ac3ad5fabcd83fa401c425513ce21cfdead3437a6cd0569ac5`.
  The source head, all four retained binaries, event schemas/bounds and own-run
  kill/wait were checked. CI, Formal models and Read the Docs passed; the
  remaining mqttiumd gates were still running at this checkpoint.
- A preflight then caught two stale M1 payload-loss mutation contexts after
  the store cleanup. Only the contexts are refreshed; `expected.json` remains
  byte-identical (SHA-256
  `0c270a3626109a31a3e78704d2d9957c54e33452991274fabe899f05647f9bd7`).
  A patch refusal is not semantic detection. Replay both complete mutant loops
  and the clean-source product receipt before the final publication/checks.
- Final SHA-specific CI and fresh K1/K2b/K3 qualification are retained in the
  updated PR and its workflow artifacts. The final source identity is always
  `index.json.source_head`; development archives are never relabelled.
- Next product increment after reception: QoS 1/PUBACK after commit, finite
  Packet Identifier reuse and MQTT-4.3.2-5 reconnect tests with an independent
  client and a premature-PUBACK control. No permanent QoS 1 deduplication claim.
