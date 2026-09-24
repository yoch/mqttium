# Formal inbound session ownership audit — 2026-09-24

## Scope and exact baseline

This report records a bounded formal/refinement investigation of inbound MQTT QoS
state at exact repository baseline:

`5774db38b94615417f2c3c6106254429eb52b6f8`

The concrete reproductions were run against the exact `1.0.0rc15` wheel emitted
by the successful CI run for that SHA. Both shipped persistence backends were
used where the finding depends on durable state.

This report is historical evidence. It does not claim that TLC or the complete
repository release matrix was run in the investigation environment.

## Model boundary

The reference model deliberately separates state that the implementation had
partially conflated:

1. durable inbound MQTT Session State;
2. Receive Maximum ownership scoped to the current Network Connection;
3. manual QoS1 acknowledgement ordering and pending acknowledgement intent;
4. QoS2 phase progression;
5. connection liveness.

The corresponding TLA+ specification is `formal/mqtt/InboundSession.tla`.

The investigation also used an independent executable state model and a
concrete differential-refinement checker. Generated traces were replayed against
the real `ProtocolEngine`; after each transition the abstract durable phase,
current connection quota, acknowledgement ordering and terminal state were
compared.

## Finding 1 — durable inbound state precharges replacement-connection quota

Tracked as #498.

`InboundSession.replay_session()` restored `_inflight` from `store.in_count()`.
That made every persisted inbound row consume Receive Maximum immediately on a
replacement connection, even when no QoS>0 PUBLISH for that exchange had been
observed on that connection.

The strongest reproduction uses a persisted QoS2 `WAIT_USER_ACK` record. That
state proves PUBREL was already received on the previous connection; the
exchange can progress through application acknowledgement/PUBCOMP without
another QoS>0 PUBLISH. With Receive Maximum 1, rc15 nevertheless restored
`_inflight = 1`, then rejected the first new PUBLISH on the replacement
connection with `0x93 Receive Maximum exceeded`.

The same reproduction succeeded against both `MemoryInflightStore` and
`SqliteInflightStore`.

A correct ownership model does not simply reset the counter. A persisted
`WAIT_PUBREL` exchange may legitimately be retransmitted as PUBLISH after
reconnect if the Server did not receive the earlier PUBREC. That retransmitted
PUBLISH must acquire one current-connection slot. The candidate therefore tracks
which persisted MIDs have actually had a QoS>0 PUBLISH observed on the current
connection and releases quota only for those MIDs.

## Finding 2 — QoS2 phase regression after PUBREL

Tracked as #499.

Minimal trace:

1. receive QoS2 PUBLISH(mid=7) -> `WAIT_PUBREL`, send PUBREC;
2. receive PUBREL(mid=7) -> `WAIT_USER_ACK` in manual-ack mode;
3. receive another QoS2 PUBLISH(mid=7, DUP=1).

rc15 treated step 3 as an ordinary duplicate and sent PUBREC again. The local
`WAIT_USER_ACK` state proves PUBREL has already been received, so accepting a
new PUBLISH rewinds the exchange from phase 2 back toward phase 1.

The candidate rejects this as a protocol error while preserving the durable
`WAIT_USER_ACK` row. MQTT 5 uses the existing protocol-error disconnect path;
MQTT 3.1.1 also terminates the invalid exchange/connection. A duplicate PUBLISH
while still in `WAIT_PUBREL` remains valid and still repeats PUBREC.

## Bounded formal/refinement evidence

Representative abstract exploration after adding manual acknowledgement actions:

| MIDs | Receive Maximum | depth | distinct states | candidate invariant failures |
| ---: | ---: | ---: | ---: | ---: |
| 4 | 1 | 8 | 4,527 | 0 |
| 5 | 1 | 8 | 12,161 | 0 |
| 5 | 2 | 8 | 42,741 | 0 |
| 5 | 3 | 8 | 92,031 | 0 |

Representative concrete differential-refinement run:

- 3 packet identifiers;
- Receive Maximum 1;
- depth 5;
- 54,899 generated traces;
- 266,328 concrete `ProtocolEngine` transitions compared;
- candidate divergences: 0.

The baseline implementation diverged in the reconnect/quota and post-PUBREL
trace families.

## Oracle qualification

The checker was mutation-tested rather than accepted merely because the
candidate produced zero divergences.

Two independent mutations were introduced:

1. restore reconnect precharge (`_inflight = persisted`);
2. restore acceptance of QoS2 PUBLISH in `WAIT_USER_ACK`.

Both were detected. The second mutation rediscovered the expected shortest
counterexample:

`q2_publish(1) -> pubrel(1) -> q2_publish(1)`.

## What is and is not proved

The bounded state exploration and differential refinement are executable and
were run. They establish the listed invariants for the explored domains and
compare the model against concrete implementation transitions.

`formal/mqtt/InboundSession.tla` is an auditable specification of the same
ownership split, but TLC was not executed in the original investigation
environment because `tla2tools.jar` was unavailable there. This report therefore
does not call the TLA+ artifact a TLC proof.

The candidate still requires the repository's complete CI, broker integration,
fuzz, soak and performance qualification before merge.
