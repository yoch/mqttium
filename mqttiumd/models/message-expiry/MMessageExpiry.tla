------------------------- MODULE MMessageExpiry -------------------------
EXTENDS Naturals, Integers, FiniteSets, TLC

CONSTANTS ExpiryInterval, MaxCrashes, AllowDomainChange, Mutation

VARIABLES origin, stamp, state, retained, referenced, references, payload,
          claims, packets, socket, acked, ambiguous, attempts, sourceAlive,
          replaySelected, running, domain, tick, crashes, changedDomain,
          hAdmitted, hExpired, hFinished, hPositive, hRetainedExpired,
          hBadFirstSend, hRetriedPastTTL, hRecoveredAmbiguous

vars == <<origin, stamp, state, retained, referenced, references, payload,
          claims, packets, socket, acked, ambiguous, attempts, sourceAlive,
          replaySelected, running, domain, tick, crashes, changedDomain,
          hAdmitted, hExpired, hFinished, hPositive, hRetainedExpired,
          hBadFirstSend, hRetriedPastTTL, hRecoveredAmbiguous>>

Deliveries == 1..2
\* TLC's machine integers cannot represent the wire maximum 4294967295.
\* Four is its finite ordered abstraction, not a claim about conversion.
MaximumInterval == 4
RawTicks == {0, 1, 2, 3, MaximumInterval}
DeliveryStates == {"absent", "pending", "claim", "known-started",
                   "temporal-uncertain", "expired", "finished"}
RetainedStates == {"retained-current", "temporal-uncertain", "expired"}
SocketStates == {"idle", "socket-start-uncertain", "positive"}
ExpiryRecords == [domain : 1..2, tick : RawTicks,
                  interval : {0, 2, MaximumInterval}]
OwnedStates == {"pending", "claim", "known-started", "temporal-uncertain"}
Bit(b) == IF b THEN 1 ELSE 0
RetainedOwns == retained \in {"retained-current", "temporal-uncertain"}
KnownDomain(st) == domain = st.domain /\ tick >= st.tick
Comparable(st) == (domain = st.domain \/ Mutation = "domain-assumption")
                  /\ tick >= st.tick
Fresh(st) == Comparable(st) /\ tick - st.tick < st.interval
Due(st) == st.interval = 0
           \/ (Comparable(st) /\ tick - st.tick >= st.interval)

Init ==
    /\ origin = [domain |-> 1, tick |-> 0, interval |-> ExpiryInterval]
    /\ stamp = [d \in Deliveries |-> origin]
    /\ state = [d \in Deliveries |-> IF d = 1 THEN "pending" ELSE "absent"]
    /\ retained = "retained-current" /\ referenced = {1}
    /\ references = 2 /\ payload = TRUE /\ claims = {} /\ packets = {}
    /\ socket = [d \in Deliveries |-> "idle"]
    /\ acked = {} /\ ambiguous = {} /\ attempts = [d \in Deliveries |-> 0]
    /\ sourceAlive = TRUE /\ replaySelected = FALSE /\ running = TRUE
    /\ domain = 1 /\ tick = 0 /\ crashes = 0 /\ changedDomain = FALSE
    /\ hAdmitted = {1} /\ hExpired = {} /\ hFinished = {} /\ hPositive = {}
    /\ hRetainedExpired = FALSE /\ hBadFirstSend = FALSE
    /\ hRetriedPastTTL = {} /\ hRecoveredAmbiguous = FALSE

\* A finite sample of raw instants includes both sides of the ordinary
\* boundary and the abstract maximum finite interval. Ticks advance stopped.
Tick ==
    /\ \E later \in RawTicks : later > tick /\ tick' = later
    /\ UNCHANGED <<origin, stamp, state, retained, referenced, references,
                   payload, claims, packets, socket, acked, ambiguous, attempts,
                   sourceAlive, replaySelected, running, domain, crashes,
                   changedDomain, hAdmitted, hExpired, hFinished, hPositive,
                   hRetainedExpired, hBadFirstSend, hRetriedPastTTL,
                   hRecoveredAmbiguous>>

\* Recipient 2 gets a distinct obligation to the original publication.
\* Selection rechecks freshness even before retained maintenance has run.
SelectReplay ==
    /\ running /\ ~replaySelected /\ retained = "retained-current"
    /\ Fresh(origin)
    /\ state' = [state EXCEPT ![2] = "pending"]
    /\ stamp' = [stamp EXCEPT ![2] = IF Mutation = "ttl-reset"
                 THEN [domain |-> domain, tick |-> tick,
                       interval |-> origin.interval] ELSE origin]
    /\ referenced' = referenced \cup {2} /\ references' = references + 1
    /\ replaySelected' = TRUE /\ hAdmitted' = hAdmitted \cup {2}
    /\ UNCHANGED <<origin, retained, payload, claims, packets, socket, acked,
                   ambiguous, attempts, sourceAlive, running, domain, tick,
                   crashes, changedDomain, hExpired, hFinished, hPositive,
                   hRetainedExpired, hBadFirstSend, hRetriedPastTTL,
                   hRecoveredAmbiguous>>

Claim(d) ==
    /\ running /\ state[d] = "pending" /\ Fresh(stamp[d])
    /\ state' = [state EXCEPT ![d] = "claim"]
    /\ claims' = claims \cup {d} /\ packets' = packets \cup {d}
    /\ UNCHANGED <<origin, stamp, retained, referenced, references, payload,
                   socket, acked, ambiguous, attempts, sourceAlive,
                   replaySelected, running, domain, tick, crashes, changedDomain,
                   hAdmitted, hExpired, hFinished, hPositive, hRetainedExpired,
                   hBadFirstSend, hRetriedPastTTL, hRecoveredAmbiguous>>

\* This is the last authorization before the first possible socket byte.
\* A durable claim alone is not permission to first-send stale data.
SocketStart(d) ==
    /\ running /\ socket[d] = "idle" /\ attempts[d] < 2
    /\ (state[d] = "known-started"
        \/ (state[d] = "claim"
            /\ (Fresh(stamp[d]) \/ Mutation = "stale-send")))
    /\ socket' = [socket EXCEPT ![d] = "socket-start-uncertain"]
    /\ ambiguous' = ambiguous \cup {d}
    /\ attempts' = [attempts EXCEPT ![d] = @ + 1]
    /\ hBadFirstSend' = (hBadFirstSend
           \/ (state[d] = "claim" /\ ~Fresh(origin)))
    /\ hRetriedPastTTL' = IF state[d] = "known-started" /\ Due(origin)
                          THEN hRetriedPastTTL \cup {d} ELSE hRetriedPastTTL
    /\ UNCHANGED <<origin, stamp, state, retained, referenced, references,
                   payload, claims, packets, acked, sourceAlive, replaySelected,
                   running, domain, tick, crashes, changedDomain, hAdmitted,
                   hExpired, hFinished, hPositive, hRetainedExpired,
                   hRecoveredAmbiguous>>

PositiveSend(d) ==
    /\ running /\ socket[d] = "socket-start-uncertain"
    /\ socket' = [socket EXCEPT ![d] = "positive"]
    /\ hPositive' = hPositive \cup {d}
    /\ UNCHANGED <<origin, stamp, state, retained, referenced, references,
                   payload, claims, packets, acked, ambiguous, attempts,
                   sourceAlive, replaySelected, running, domain, tick, crashes,
                   changedDomain, hAdmitted, hExpired, hFinished,
                   hRetainedExpired, hBadFirstSend, hRetriedPastTTL,
                   hRecoveredAmbiguous>>

DurableStarted(d) ==
    /\ running /\ state[d] \in {"claim", "temporal-uncertain"}
    /\ d \in claims /\ socket[d] = "positive"
    /\ state' = [state EXCEPT ![d] = "known-started"]
    /\ UNCHANGED <<origin, stamp, retained, referenced, references, payload,
                   claims, packets, socket, acked, ambiguous, attempts,
                   sourceAlive, replaySelected, running, domain, tick, crashes,
                   changedDomain, hAdmitted, hExpired, hFinished, hPositive,
                   hRetainedExpired, hBadFirstSend, hRetriedPastTTL,
                   hRecoveredAmbiguous>>

\* A retry may be after TTL, but cannot refresh publication metadata.
Retry(d) ==
    /\ running /\ state[d] = "known-started" /\ socket[d] = "positive"
    /\ d \notin acked /\ attempts[d] < 2
    /\ socket' = [socket EXCEPT ![d] = "idle"]
    /\ stamp' = [stamp EXCEPT ![d] = IF Mutation \in {"ttl-reset", "retry-ttl-reset"}
                 THEN [domain |-> domain, tick |-> tick,
                       interval |-> origin.interval] ELSE @]
    /\ UNCHANGED <<origin, state, retained, referenced, references, payload,
                   claims, packets, acked, ambiguous, attempts, sourceAlive,
                   replaySelected, running, domain, tick, crashes, changedDomain,
                   hAdmitted, hExpired, hFinished, hPositive, hRetainedExpired,
                   hBadFirstSend, hRetriedPastTTL, hRecoveredAmbiguous>>

\* ACK can be observed before the positive-start fact is durably engaged.
Ack(d) ==
    /\ running /\ socket[d] = "positive" /\ d \notin acked
    /\ acked' = acked \cup {d}
    /\ UNCHANGED <<origin, stamp, state, retained, referenced, references,
                   payload, claims, packets, socket, ambiguous, attempts,
                   sourceAlive, replaySelected, running, domain, tick, crashes,
                   changedDomain, hAdmitted, hExpired, hFinished, hPositive,
                   hRetainedExpired, hBadFirstSend, hRetriedPastTTL,
                   hRecoveredAmbiguous>>

DurableFinish(d) ==
    /\ running /\ state[d] = "known-started" /\ d \in acked
    /\ state' = [state EXCEPT ![d] = "finished"]
    /\ referenced' = referenced \ {d} /\ references' = references - 1
    /\ payload' = (references' > 0)
    /\ claims' = claims \ {d} /\ packets' = packets \ {d}
    /\ socket' = [socket EXCEPT ![d] = "idle"]
    /\ acked' = acked \ {d} /\ ambiguous' = ambiguous \ {d}
    /\ hFinished' = hFinished \cup {d}
    /\ UNCHANGED <<origin, stamp, retained, attempts, sourceAlive,
                   replaySelected, running, domain, tick, crashes, changedDomain,
                   hAdmitted, hExpired, hPositive, hRetainedExpired,
                   hBadFirstSend, hRetriedPastTTL, hRecoveredAmbiguous>>

ExpireDelivery(d) ==
    /\ running /\ Due(stamp[d])
    /\ (state[d] \in {"pending", "claim", "temporal-uncertain"}
        \/ (Mutation = "free-started-ref" /\ state[d] = "known-started"))
    /\ LET drops == d \notin ambiguous \/ state[d] = "known-started"
       IN /\ state' = [state EXCEPT ![d] = IF drops THEN "expired"
                                          ELSE "temporal-uncertain"]
          /\ referenced' = IF drops THEN referenced \ {d} ELSE referenced
          /\ references' = references - Bit(drops)
          /\ payload' = (references' > 0)
          /\ claims' = IF drops THEN claims \ {d} ELSE claims
          /\ packets' = IF drops THEN packets \ {d} ELSE packets
          /\ socket' = IF drops THEN [socket EXCEPT ![d] = "idle"] ELSE socket
          /\ acked' = IF drops THEN acked \ {d} ELSE acked
          /\ hExpired' = IF drops THEN hExpired \cup {d} ELSE hExpired
    /\ UNCHANGED <<origin, stamp, retained, ambiguous, attempts, sourceAlive,
                   replaySelected, running, domain, tick, crashes, changedDomain,
                   hAdmitted, hFinished, hPositive, hRetainedExpired,
                   hBadFirstSend, hRetriedPastTTL, hRecoveredAmbiguous>>

ExpireRetained ==
    /\ running /\ RetainedOwns /\ Due(origin)
    /\ retained' = "expired" /\ references' = references - 1
    /\ payload' = (references' > 0) /\ hRetainedExpired' = TRUE
    /\ UNCHANGED <<origin, stamp, state, referenced, claims, packets, socket,
                   acked, ambiguous, attempts, sourceAlive, replaySelected,
                   running, domain, tick, crashes, changedDomain, hAdmitted,
                   hExpired, hFinished, hPositive, hBadFirstSend,
                   hRetriedPastTTL, hRecoveredAmbiguous>>

\* The source session owns neither the retained slot nor recipient work.
SourceDeath ==
    /\ running /\ sourceAlive /\ sourceAlive' = FALSE
    /\ UNCHANGED <<origin, stamp, state, retained, referenced, references,
                   payload, claims, packets, socket, acked, ambiguous, attempts,
                   replaySelected, running, domain, tick, crashes, changedDomain,
                   hAdmitted, hExpired, hFinished, hPositive, hRetainedExpired,
                   hBadFirstSend, hRetriedPastTTL, hRecoveredAmbiguous>>

Crash ==
    /\ running /\ crashes < MaxCrashes
    /\ running' = FALSE /\ crashes' = crashes + 1
    /\ socket' = [d \in Deliveries |-> "idle"]
    /\ acked' = {} /\ ambiguous' = {}
    /\ UNCHANGED <<origin, stamp, state, retained, referenced, references,
                   payload, claims, packets, attempts, sourceAlive,
                   replaySelected, domain, tick, changedDomain, hAdmitted,
                   hExpired, hFinished, hPositive, hRetainedExpired,
                   hBadFirstSend, hRetriedPastTTL, hRecoveredAmbiguous>>

LoseContinuity ==
    /\ ~running /\ AllowDomainChange /\ ~changedDomain
    /\ domain' = 2 /\ tick' = 0 /\ changedDomain' = TRUE
    /\ UNCHANGED <<origin, stamp, state, retained, referenced, references,
                   payload, claims, packets, socket, acked, ambiguous, attempts,
                   sourceAlive, replaySelected, running, crashes, hAdmitted,
                   hExpired, hFinished, hPositive, hRetainedExpired,
                   hBadFirstSend, hRetriedPastTTL, hRecoveredAmbiguous>>

\* Recovery cannot distinguish a claimed-but-never-sent delivery from an
\* unrecorded positive send. Expired/incomparable claims remain charged.
Recover ==
    /\ ~running /\ running' = TRUE /\ ambiguous' = claims
    /\ state' = [d \in Deliveries |->
          IF state[d] = "claim" /\ ~Fresh(stamp[d])
          THEN "temporal-uncertain"
          ELSE IF state[d] = "pending" /\ ~Comparable(stamp[d])
               THEN "temporal-uncertain" ELSE state[d]]
    /\ retained' = IF retained = "retained-current" /\ ~Comparable(origin)
                    THEN "temporal-uncertain" ELSE retained
    /\ hRecoveredAmbiguous' = (hRecoveredAmbiguous
          \/ \E d \in Deliveries : state[d] = "claim" /\ ~Fresh(stamp[d]))
    /\ UNCHANGED <<origin, stamp, referenced, references, payload, claims,
                   packets, socket, acked, attempts, sourceAlive, replaySelected,
                   domain, tick, crashes, changedDomain, hAdmitted, hExpired,
                   hFinished, hPositive, hRetainedExpired, hBadFirstSend,
                   hRetriedPastTTL>>

Idle == UNCHANGED vars
Next == Tick \/ SelectReplay \/ (\E d \in Deliveries : Claim(d))
        \/ (\E d \in Deliveries : SocketStart(d))
        \/ (\E d \in Deliveries : PositiveSend(d))
        \/ (\E d \in Deliveries : DurableStarted(d))
        \/ (\E d \in Deliveries : Retry(d))
        \/ (\E d \in Deliveries : Ack(d))
        \/ (\E d \in Deliveries : DurableFinish(d))
        \/ (\E d \in Deliveries : ExpireDelivery(d))
        \/ ExpireRetained \/ SourceDeath \/ Crash \/ LoseContinuity \/ Recover \/ Idle
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ origin \in ExpiryRecords /\ origin.interval = ExpiryInterval
    /\ stamp \in [Deliveries -> ExpiryRecords]
    /\ state \in [Deliveries -> DeliveryStates] /\ retained \in RetainedStates
    /\ referenced \subseteq Deliveries /\ references \in 0..3 /\ payload \in BOOLEAN
    /\ claims \subseteq Deliveries /\ packets \subseteq Deliveries
    /\ socket \in [Deliveries -> SocketStates]
    /\ acked \subseteq Deliveries /\ ambiguous \subseteq Deliveries
    /\ attempts \in [Deliveries -> 0..2]
    /\ sourceAlive \in BOOLEAN /\ replaySelected \in BOOLEAN /\ running \in BOOLEAN
    /\ domain \in 1..2 /\ tick \in RawTicks /\ crashes \in 0..MaxCrashes
    /\ changedDomain \in BOOLEAN /\ hAdmitted \subseteq Deliveries
    /\ hExpired \subseteq Deliveries /\ hFinished \subseteq Deliveries
    /\ hPositive \subseteq Deliveries /\ hRetainedExpired \in BOOLEAN
    /\ hBadFirstSend \in BOOLEAN /\ hRetriedPastTTL \subseteq Deliveries
    /\ hRecoveredAmbiguous \in BOOLEAN

PublicationExpiryFixed ==
    /\ origin = [domain |-> 1, tick |-> 0, interval |-> ExpiryInterval]
    /\ \A d \in Deliveries : state[d] # "absent" => stamp[d] = origin
ReferencesExact ==
    /\ referenced = {d \in Deliveries : state[d] \in OwnedStates}
    /\ references = Cardinality(referenced) + Bit(RetainedOwns)
PayloadLifetimeExact == payload = (references > 0)
ClaimOwnershipExact ==
    /\ packets = claims /\ claims \subseteq referenced
    /\ \A d \in Deliveries : state[d] \in {"claim", "known-started"} => d \in claims
NoStaleFirstSend == ~hBadFirstSend
StartedReferenceHeld ==
    \A d \in hPositive \ hFinished : d \in referenced /\ d \in claims /\ d \in packets
NoAssumedContinuity ==
    running /\ ~KnownDomain(origin) =>
        /\ retained # "retained-current"
        /\ \A d \in Deliveries : state[d] \notin {"pending", "claim"}
NoLostObligations ==
    /\ hAdmitted = {d \in Deliveries : state[d] # "absent"}
    /\ hExpired = {d \in Deliveries : state[d] = "expired"}
    /\ hFinished = {d \in Deliveries : state[d] = "finished"}
    /\ hRetainedExpired = (retained = "expired")

NoPastTTLFinishWitness == ~(hRetriedPastTTL \cap hFinished # {})
NoAmbiguousExpiryWitness ==
    ~(hRecoveredAmbiguous /\ running /\ KnownDomain(origin) /\ Due(origin)
      /\ \E d \in hPositive : state[d] = "temporal-uncertain"
                                /\ d \in claims /\ d \in referenced /\ d \in packets)
NoDomainQuarantineWitness ==
    ~(changedDomain /\ running /\ retained = "temporal-uncertain"
      /\ state[1] = "temporal-uncertain" /\ 1 \in referenced)
NoFiniteBoundaryWitness ==
    ~(tick = ExpiryInterval /\ retained = "expired" /\ state[1] = "expired"
      /\ references = 0 /\ ~payload)
=============================================================================
