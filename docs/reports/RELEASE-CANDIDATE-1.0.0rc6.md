# Release candidate report — 1.0.0rc6

Candidate source before release metadata: `e07f0148` (`main`, 2026-08-16).
Release metadata is normalized across the package version, README, changelog,
API stability policy, roadmap and report index on the final candidate branch.

## Scope

RC6 collects the reviewed work merged after `1.0.0rc5`. It is intentionally a
consolidation candidate: correctness hardening and measured hot-path work were
reviewed and merged independently before the release cut.

The protocol/lifecycle hardening includes:

- #232 enforces MQTT 5 enhanced-authentication sequencing, including required
  Authentication Method handling, initial-auth reason-code constraints and
  explicit re-authentication phase tracking (closing #231, #233, #241 and #244);
- #236 rejects unsolicited CONNACK Response Information unless the CONNECT wire
  snapshot requested it;
- #238 makes graceful disconnect while CONNECTING deterministic without turning
  an intentional shutdown into an abrupt transport abort;
- #240 rejects MQTT 5 Session Present when the client retains no Client Session
  State while keeping the decision O(1) after hydration;
- #245 rejects Client-only DISCONNECT reason `0x04` when received from a Server;
- #248 preserves manual QoS 1 PUBACK wire order even when application `ack()`
  calls arrive out of order, integrated with the same hydration metadata scan
  used for recovered session state.

The performance work includes:

- #249 makes Paho-compatible QoS 1/2 `publish()` return without a producer-thread
  round trip to the network loop. The façade owns a correlation MID namespace,
  preserves wire-MID ownership on the loop, reserves live façade MIDs safely
  across wrap-around, and keeps callback correlation valid through callback
  toggles, bulk failure and completion delivery. QoS 0 also uses the native
  writer-direct path when safe, and `max_outbound_inflight` is exposed at façade
  construction;
- #250 adds an ordered eager writer path for contiguous TCP/Unix frames when the
  writer is idle. The optimization removes one writer-task event-loop turn,
  preserves FIFO and one-write-in-flight semantics, declines under backpressure
  or segmentation, and bounds eager admission by projected socket-buffer usage.
  WebSocket and transports without the optional eager primitive remain on the
  historical queued writer path.

No Stable API default is changed. The Paho compatibility surface and client
statistics remain Provisional under the documented stability policy; their RC6
changes are recorded in the changelog.

## Repository state at cut

`main` was exactly `e07f0148caf3ffe50d11089af838c39cfc32f2d5`, 37 commits ahead
of the RC5 merge commit `71fa15d6db96d6188ab11353665c66ecc4c736f9`.
There were no open pull requests before the RC6 release branch was created.

The dated performance reports are retained as historical evidence for the
commits they name and are not rewritten to pretend they measured later audit
fixes. In particular, `COMPAT-PUBLISH-HANDOFF-2026-08-16.md` and
`NATIVE-WRITER-HOP-2026-08-16.md` describe the benchmarked implementations and
bound their claims to the measured paths.

## Validation inherited from merged heads

Each correctness PR above reached `main` only after its exact rebased head had
passed repository CI; the combined session-state/manual-ACK integration also
passed focused recovery coverage, the broad unit suite, long fuzz and
Mosquitto finalization soaks before merge.

#249 passed its final exact-head CI and finalization after the MID-lifetime and
callback-correlation audit fixes. #250 likewise passed its final exact-head CI,
finalization/soak and package/publish build after the projected high-water
admission fix and its loop-free regression-test correction.

The release branch must still pass its own exact-head repository CI and
`Publish to PyPI` pull-request artifact/smoke gates. Earlier green runs are
inherited evidence for merged runtime changes, not a substitute for validating
the RC6 artifact itself.

## Performance evidence

The release keeps the repository's strict distinction between structural
measurements, eligible-host benchmark evidence and end-to-end claims.

For the Paho compatibility path, the retained submit benchmark demonstrates the
mechanism directly: removing the cross-thread synchronous MID handoff allows
multiple publications to coalesce instead of serializing every producer call.
Those figures are submission/handoff measurements with no broker I/O, not a
universal broker-throughput multiplier.

For the native writer path, the retained eligible-host campaign reports
callback-p50 improvements of 16.7% to 27.8% at certified MQTT 3.1.1/window-64
load points, plus independently certified MQTT 5/window-20 improvements of
26.6% and 25.6%, with completed rate unchanged or slightly higher. The exact
structural effect is one event-loop turn between `publish_nowait()` and the
transport write becoming zero when the eager path is eligible.

Later correctness auditing intentionally narrowed eager eligibility near the
64 KiB socket-buffer high-water boundary and strengthened façade MID lifetime
bookkeeping. The historical reports remain scoped to the commits they measured;
RC6 does not silently extrapolate their exact percentages onto the final audit
heads.

## Release decision

RC6 is ready to enter release-PR validation from the current clean `main`: the
post-RC5 protocol fixes and performance changes are already independently
reviewed and merged, no open PR competes with the cut, and release metadata is
the only intended delta. Promotion depends on the RC6 PR's exact-head CI and
artifact/publish-smoke gates remaining green.
