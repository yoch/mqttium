# Message Expiry — CI follow-up and received output-pressure correction

Report0017 preserves its received local candidate66361b74. The first subsequent
publication9df5979b8d9a6814b928231eee4a8d34e5091e5f ran all existing CI gates and
exposed two separate failures. Neither failed job is counted as a completed
reception or as a negative-control detection.

## Durable QoS1 output pressure

The retained job in run44158367529 passes its first five normal groups, then
fails its32-value capacity replay with BrokenPipe. Each value is32768bytes;
the receiver is active. A known Claim is followed by a temporary four-slot
socket-output Full and immediate `outgoing_queue_full` closure. The outgoing
window is eight. This close-on-Full policy was already present in795d2009;
queue occupancy alone did not establish a blocked socket or its write deadline.

The original failed ZIP is artifact14362621803,32333178bytes, SHA256
`0d70890275e506206d291afb217f33fcb86371e9309ef9f844b5394a13d839bc`.
Its actual normal binary is
`41b281865f6038d6394a30d13f5fb1fca0c948741f02b6b6ebe07ed9b6836079`.
The immutable capture is retained outside Git at
`/workspace/msgexp-9df5979b-retained-ci-failed`.

RED01eb2d5e reaches a deterministically full real adapter queue using an actual
admission and claim, and fails `temporary_publish_queue_full_closed_generation`.
GREENe67ea110 returns the same owned QoS1 effect for retry and removes its
provisional ACK binding. It preserves effect/delivery/claim/PI and connection
generation, then exercises actual socket acceptance and ACK/Finish. The existing
Disconnected path, control-frame policy, four physical output slots, Receive
Maximum and250ms socket-write deadline are unchanged. No contract or quota
changes are needed; the existing `effect_retained` event reports retry pressure.

Clean e67ea11044e570858779f796ce828803a40b2102 receives both complete launchers:

| Reception | Actual result |
| --- | --- |
| Retained | All six normal groups, nine boundary/unknown cases and five compiled controls PASS |
| Large retained replay |32messages ×32768bytes, all completed; eight actual PUBLISH queue-Full retries; no connection closure except shutdown |
| Message Expiry | Nine normal groups,14boundary cases including11own-run SIGKILLs, six compiled controls and packaged demo PASS |
| Product regressions |123default/131instrumented unit tests, both strict Clippy variants, concurrency pressure, generation and shutdown cases PASS in the separate ee0ae904 owner worktree |
| Root workspace | Formatting, strict normal workspace Clippy,1107normal Rust tests, validator16documents/digest36a15f70 PASS |

The two root receptions execute normal bytes with SHA256
`813eec3a29f83565aa65d03f69656a8b9d3be0d0e53f9a6bb4168a7c0fd3fd3d`.
Their source/bytes match; each process and syscall witness belongs only to its
own execution. All551retained and982expiry kept hashes were independently
checked. Retained system tar SHA256
`d58516aedfaad48825dfafbf82d4f841a3edcf4a9ef43fc375bf149335e3a519`;
expiry system tar
`30e6e63233dc56f2e4fa7f59cef80cd424110ca6c01fc9b9a39fee9bc75e2583`.

## Split syscall witness

The first Message Expiry CI job passes every business case, all six compiled
controls, packaged demo and strict schema check (14897events). Its final index
check nevertheless rejects SIGKILL4730. The same-run strace records caller4615's
`kill(4730, SIGKILL <unfinished ...>` followed by that caller's successful
`<... kill resumed>) = 0`. The single-line producer regex misses this valid
split representation. The failed job therefore has no completed root index,
root SHA256SUMS or reception tar, and none is manufactured afterward.

Artifact14361174676 is42474130bytes, API ZIP SHA256
`7300f453bdc9087de74a7cfa8d214ae89170f62e55ce1c8a8381b9de52e52f2c`.
RED636e85d8 reproduces rejection of the actual split lines. GREEN7e89db5d
matches unfinished/resumed records by exact caller and syscall, preserves
successful-return and target-PID checks, and rejects unmatched, cross-caller,
failed or truncated evidence. All35 Message Expiry pilot tests pass, and the
corrected helper recognizes all11 actual9df kill witnesses without modifying
that failed receipt or manufacturing its missing index. The historical
K1/K2b/K3 checker is unchanged. The final published SHA, fresh CI artifacts and independent
download/extraction execution are recorded in PR648 after reception; neither
9df5979b nor these local candidate receipts are relabeled as that final run.
