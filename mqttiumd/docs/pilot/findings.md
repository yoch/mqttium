# Pre-pilot findings register

The reviewed reference is `49bbaaa4d1d080f458980e3498e90638e317668d`.
The four source reports under `reviews/` preserve their original assessments.
This register records independent verification and subsequent dispositions.
The full PR has 1,571 changed files and 1,324 commits at that reference. These
focused reviews cover the named paths and predicates.

| ID | Severity in the proposed pilot | Evidence and consequence | Disposition |
| --- | --- | --- | --- |
| Q1 | P1 | A terminated TLS writer can preempt a decoded normal DISCONNECT. The ordinary 30-minute run also admitted six Wills during requested normal client closure. This can replace a retained presence value. | Reproduced by a deterministic real TLS-task test. Test committed red at `a7eeb6accda45b6d9f4dd40bbfd2eadb8f11cb45`. Correction retains bounded inbound progress until its ordered terminal, including outgoing PINGRESP failures. Requalified on downloaded source 51be4079, including the full 90,000/180,000 run. |
| Q2 | P1 | Closing a recipient can lose a queued genuine Started result after a received expiring QoS 1 PUBLISH and its valid PUBACK. Its original claim remains Pending after same-session resume. | Two independent red runs, exact Finish absent and exact claim present. Test committed red at `319bbd5c`. Correction preserves the generation through producer quiescence and final frame-fact consumption. Requalified on downloaded source 51be4079, including the full 90,000/180,000 run. |
| Q3 | P1 | Two same-ClientID Opens can pass negative Will barriers before either Open commits. The second Open then blocks the old Will termination and returns 0x80 after closing the first. | Independent deterministic red using two real writer replies. Test committed at `319bbd5c`. Correction orders earlier Opens and rechecks the attachment before the next barrier. Requalified on downloaded source 51be4079, including the full 90,000/180,000 run. |
| Q4 | Retracted | The trust reviewer initially rated Clean Start with a small Maximum Packet Size as P1. | The independent verifier checked MQTT-3.1.2-4 and 3.1.2-24. Discarding old state after receiving Clean Start 1 is required. Failure to fit CONNACK adds no requirement to roll back Open. No defect established. |
| Q5 | P2 within canonical private directories | Broker leases derive from the supplied path. Final symlinks and hardlink aliases can bypass that pathname convention. Direct custom event paths can collide with SQLite files. | Pilot launchers require private canonical regular database files and separate per-run log paths. Offline tooling already rejects final aliases. Arbitrary direct CLI paths are outside this operating profile. |
| Q6 | P2 within measured image and disk bounds | Logical MQTT quotas do not impose a physical disk ceiling. Logs rotate within each process, while per-start directories accumulate. Disk or required log failure stops service. | Require host disk monitoring, private backups and log rotation. Publish measured WAL, image, write volume and rest cardinalities. Storage exhaustion remains an operational incident. |
| Q7 | P2 below offline inventory ceiling | A policy update close to its 128 MiB image ceiling may commit an expansion that its readback cannot inventory. `AlreadyApplied` verifies authority after a lost output but does not reissue the original backup attestation. | Use the measured small pilot image, run plan on the served stopped file, retain backup and plan, alert well below the ceiling. No post-commit expansion beyond the cap was reproduced here. |
| Q9 | Pilot-blocking operating limit | At 50/s with one dashboard offline, 127/500 publications were admitted and 373 refused 0x97 after 2.54 s. All 127 recovered. | NO-GO for the proposed outage profile. The hard quota is correct and unchanged. A producer back-pressure/spooling contract has not been received. |
| Q8 | P2 | TLS shutdown omits server close_notify. Keep Alive uses reactor processing time. A still-fresh sealed frame can lose its exact zero-byte classification after its remaining interval changes. | Preserve documented temporal uncertainty and process-level recovery scope. Retain these limitations for the private pilot assessment. |

Every confirmed correction is local to runtime ordering. MQTT contracts,
canonical recipes, SQLite schema 12/13 and configured business quotas keep their
existing representations. Red regressions are committed separately from the
correction. The source reports and red evidence keep their original SHA.
