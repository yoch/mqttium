# 0013 — Behaviours of the broker that the chapters leave open

Status: **Proposed** (15 November 2026, lots L1 and L3, first wave).
Recorded by the integration owner from what the lots decided while writing
the transition registry and the contract of acknowledgements. Each line is
a behaviour a client or an operator can observe; none is settled by the
chapters or by an accepted record. It becomes a requirement only when the
owner of the repository accepts it, line by line if need be. Until then
the rules stay in the registry of lot L3 as written, marked as depending
on this record.

## Context

Record 0001 leaves to the oracle role the registry row for a PUBLISH
replayed on a live connection. Writing the whole registry of P1 (92 rules)
met seven more cells where the chapters name a state or forbid an outcome
without saying what the broker does. Lot L1, asked which reason a refusing
PUBREC carries, found that the chapters fix none and that a first choice
comes before the reason.

## Decisions proposed

| # | Cell or question | Rule proposed | Source | Alternative set aside |
| - | - | - | - | - |
| 1 | PUBLISH with the Packet Identifier of an exchange whose admission is unresolved, on the connection that already carried it | Absorbed like a replay after a reconnect: no command, no acknowledgement; the pending resolution continues and its PUBREC answers both | Not settled. Record 0001 leaves the row to the oracle; MQTT 5 section 4.4 forbids the resend and does not say what the receiver does | Close the connection for a protocol error (MQTT 5, 4.4 and 4.13): the admission would still resolve and the client would get its PUBREC only after reconnecting and replaying |
| 2 | `AdmissionPending` x PUBREL | Close the connection; the admission continues | Not settled. ARC-011 says an unknown PUBREL follows an explicit protocol rule, but here the identifier is known and in progress | Answer PUBCOMP "not found", which would end an exchange whose admission may still commit |
| 3 | `ReleasePending` x PUBLISH QoS 2 | Close the connection; the release continues | ADM-006 forbids the second admission; the answer to the client is not settled | Absorb, or answer PUBREC |
| 4 | A release rejected for certain | For `Fenced`: a new command under a new `OperationId`, at most four preparations per exchange; the state stays `ReleasePending`. For `PreconditionFailed`, `EpochClosed` and a lack of capacity, and beyond the fourth preparation: the owner stops (`CoreFault::ReleaseRefused`). The core of lot L1 does this, and the registry says it since lot L3 added the dimension `refusal` (`fenced`, `fenced_at_bound`, `definitive`) and the two rules `inbound_qos2.release_pending.rejected.fenced_at_bound.stop` and `.definitive.stop`; before, the registry prepared again whatever the reason, which the review of the core showed to loop without end | DEM-009 for the new identity; the moment of the new preparation is not settled | Return to `AwaitPubrel` and wait for a PUBREL the client will not send on a live connection |
| 5 | Meaning of `Consumed` and `ReceiptClosed` | `Consumed`: a committed result whose dependent effects the owner published. `ReceiptClosed`: an identity that cannot execute and whose result detail no longer exists | V3-005 names the states, V3-006 describes `ReceiptClosed`; the exact meaning of `Consumed` is not settled | `Consumed` also covering a consumed rejection, which would need one more guard dimension |
| 6 | A commit reported again in `AwaitPubrel` | No second PUBREC | DEM-010 for the absence of a regression; whether the frame may be sent again is not settled | Send PUBREC again, harmless for the protocol |
| 7 | Effects `EmitPubrecRefusal` and `EmitPubcompNotFound` | Two effects of the catalogue, so that a rule can forbid the positive acknowledgement (ACK-001) and require the negative one | Consequence of ARC-011 | One effect per frame, which cannot express ACK-001 in a refusing rule |
| 8 | A `retired` result for an operation still awaited | Unreachable in S0 | S0 collects nothing (DEM-020 excludes collection from M1a); not settled beyond S0 | — |
| 9 | An admission the store rejected for certain, on a live process that still holds the recipe and the payload | Refuse to the client. This is the rule of the registry today (`inbound_qos2.admission_pending.rejected.refuse`) and the recommendation of lot L1 for S0 | DEM-009 and chapter 2 provide for the other outcome: "after a certain rejection, a new preparation gets another OperationId" | Prepare the admission again under a new `OperationId` with the same `RequestId`: it needs a bound on the attempts and a registry rule, and refuses to the client only when the bound is reached |
| 10 | Reason of the PUBREC that refuses an admission | `0x97` Quota exceeded for a rejection for lack of capacity only; `0x83` Implementation specific error for `Fenced`, `PreconditionFailed` and `EpochClosed` | MQTT 5 section 3.5.2.1: `0x83` is "the PUBLISH is valid but the receiver does not accept it". The chapters fix no reason code | `0x97` for every refusal, which states that a quota is the cause when none is; `0x80` Unspecified error, which says less |
| 11 | An admission, or a resolution, when no receipt slot is left | The outcome is unknown and nothing is written. Every receipt, of a commit or of a rejection, and every fence holds a receipt slot, which collection gives back; a certain rejection that could not be recorded would not be certain. The deterministic store of lot L2 does this | DEM-010: an absence is not a rejection. The chapters do not say what bounds rejection receipts and fences | Rejection receipts and fences written without any bound, which grow exactly when capacity is exhausted (the store before its review); or a separate account for them, which can be exhausted the same way |
| 12 | A PUBLISH replayed after a certain refusal that the client did not observe (the connection was reset before the refusing PUBREC left) | A new admission under a new identity: the refusal took the exchange to `Absent`, so the replay is a first PUBLISH. This is the rule of the registry (`inbound_qos2.admission_pending.rejected.refuse`) and what the core of lot L1 does | Not settled. ARC-011 says that a certain refusal ends the exchange without distribution; neither it nor MQTT 5 section 4.3.3 says what a receiver remembers of a refusal it could not deliver. Either way the message is distributed at most once | Keep a volatile `Refused` slot that answers the replay with the same refusal, as the models M1a and M1b of lot L5 do (`mqttiumd/models/m1/README.md`, "Disagreements with the registry"): the answer to one exchange is then stable, at the price of one more state and of a bound on what is remembered |

Line 10 is in the contract already: `AckReason::ImplementationSpecificError`
and `AckReason::refusing` (lot L1, on the integration branch). It is needed
whichever way line 9 is decided.

## Consequences

- Lines 1 to 8 are rules of `contracts/v1/p1.transitions.json`, on the
  integration branch since the contract stack of lot L3 was merged.
- If line 9 is decided the other way, the registry gains a rule and a
  bound, and scenario QABS-B changes its expected frames.
- Line 12 is a disagreement between the registry and the models of lot L5,
  found by lot L5 by reading the actions; no counterexample was run. Until
  it is decided, neither side changes (DEM-025), and the correspondence of
  M1 names the difference.
- A line that is refused becomes a contract change of lot L3, with the
  counterexample that the alternative closes.
