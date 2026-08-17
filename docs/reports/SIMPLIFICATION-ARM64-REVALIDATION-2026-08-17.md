# Simplification audit — ARM64 revalidation after issue #253

Date: 2026-08-17

This report supersedes the performance-acceptance conclusion of
`SIMPLIFICATION-AUDIT-2026-08-16.md` for PR #252 after rebasing it onto
the issue-#253 writer correction and ARM64 runner integration. The older
dated report is intentionally left unchanged.

## Revalidation finding

The rebased tree at `3ee2a3c5a78b0ea487ddbdf7927665b519fddd45`
passed functional CI (`32014996281`), long fuzz (`32014996232`) and
finalization (`32014996294`). Strict Pi-5 current-main-vs-candidate
validation (`32015071488`) nevertheless found a scheduling regression:
capacity A/A and A/B passed and latency A/A passed, but at 2,500 msg/s
callback p50 moved from about 0.334 ms on current main to about 1.361 ms
on #252 (~4.07x, 0/8 favourable pairs). At 10,000 msg/s p50 remained
slightly favourable while loop-lag rose to about 6.35x.

A linear scan (`32015752809`) located the transition exactly at rebased
commit `832f808e413dd47fd922469110316751ba205906`, `perf: remove
Python frames from the publish and acknowledgement paths`. Source and
interaction isolation (`32016075623`, `32016317775`, `32017494910`,
`32018097989`, `32018272639`) showed that this is an interaction rather
than one bad line:

- writer-only, inbound-only and outbound-only substitutions were neutral
  in isolation;
- either engine micro-optimization alone still left the slow 2,500/s
  plateau, even with `_try_launch` restored;
- restoring outbound forwarding wrappers was worse;
- reverting the writer's once-per-task `write_many` lookup was worse.

The smallest measured correction that recovered the useful scheduling
regime is therefore:

1. restore the engine effect-emission boundary: `_send` delegates to
   `_emit` and `EngineEffect` uses the explicit keyword construction;
2. restore the outbound `_try_launch` admission/compensation boundary;
3. retain the independent writer `write_many` cache, inbound
   specialization and direct outbound effect forwarding.

In screening run `32017494910`, that combination moved 2,500/s p50 from
the ~1.36 ms plateau to about 0.476 ms, kept 10,000/s favourable (~0.262
ms versus ~0.285 ms), and returned loop lag to a comparable regime.
These screening numbers are not final evidence; the committed corrected
HEAD requires fresh A/A and A/B validation.

## Corrected-HEAD acceptance contract

The comparison baseline is current post-#254/#255 main
`0999d8abfe44a568209209403d7f215b18cc4eb7`. Acceptance requires:

- full functional CI, fuzz and finalization green;
- strict closed-loop capacity A/A valid and A/B >=95% of current main
  for QoS 0 and QoS 1;
- paced callback-latency A/A valid at 2,500 and 10,000 msg/s;
- paced A/B passing completion and loop-lag gates;
- median candidate/main callback p50 <=1.50x at each acceptance rate.

The explicit p50 ceiling exists because the original ~4x regression can
otherwise hide behind neutral throughput and a host-dependent loop-lag
regime. It is a regression guard, not a performance target.
