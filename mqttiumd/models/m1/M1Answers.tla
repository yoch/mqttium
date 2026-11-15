----------------------------- MODULE M1Answers -----------------------------
(***************************************************************************)
(* M1Answers: which answers of its resolutions an operation still applies  *)
(* (audit F3, the retention rule of `Answers` in                           *)
(* crates/mqttiumd-core/src/owner.rs, invariants I1 to I5), and the        *)
(* hypothesis I3 under which an unresolved exchange leaves                 *)
(* AdmissionPending.                                                       *)
(*                                                                         *)
(* One operation in flight. Its command was accepted and its result was    *)
(* lost: the Persist is retained, its answer never comes. Each deadline    *)
(* of the unresolved operation emits one resolution, of rank n + 1. The    *)
(* operation retains the effect of its Persist, of its R latest            *)
(* resolutions and of its L latest landmarks, the resolutions whose rank   *)
(* is a power of two. The two rings are the implementation's: rank n goes  *)
(* to slot n mod R of `recent` and, when n is a power of two, to slot      *)
(* ctz(n) mod L of `landmarks`. The window depends on ranks only, never    *)
(* on time.                                                                *)
(*                                                                         *)
(* An answer is applied when its rank is retained when it arrives, and     *)
(* absorbed otherwise. The store answers every resolution with a terminal  *)
(* result (record 0015), except a refused fence, which answers unknown and *)
(* is counted by MaxUnknown. Answers arrive in any order, may be repeated  *)
(* (MaxDup) and may be lost (MaxLoss).                                     *)
(*                                                                         *)
(* Lag = "uniform" is I3: every answer arrives at most K newer resolutions *)
(* after its own, K the same for every resolution. It is a guard on the    *)
(* environment: no resolution is emitted while an answer that would then   *)
(* be more than K resolutions old is still in transit. Lag = "unbounded"   *)
(* drops the guard: each delay is finite, with no common bound. That is    *)
(* outside I3, and its configuration keeps a counterexample.               *)
(*                                                                         *)
(* The history of the verifier (prefix h) is read by no guard.             *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    R,          \* RECENT_RESOLUTIONS
    L,          \* LANDMARK_RESOLUTIONS
    K,          \* the uniform lag of I3, in newer resolutions
    Lag,        \* "uniform" (I3) or "unbounded"
    MaxLoss,    \* answers lost
    MaxUnknown, \* answers that are unknown: a refused fence
    MaxDup,     \* answers delivered twice
    MaxRank,    \* last rank the model emits
    Mutation    \* "none" or the name of a model mutation

Mutations == {"none", "NoLandmarks", "LandmarkSlotByRank", "ApplyAny",
              "KeepEvicted"}

\* Exponents TLC may raise 2 to without leaving its 32-bit integers.
Exps == 0..20

ASSUME /\ R \in Nat \ {0} /\ L \in Nat \ {0} /\ K \in Nat \ {0}
       /\ Lag \in {"uniform", "unbounded"}
       /\ MaxLoss \in Nat /\ MaxUnknown \in Nat /\ MaxDup \in Nat
       /\ Mutation \in Mutations
       /\ MaxRank \in 1..(2^20)

IsPow2(x) == \E j \in Exps : 2^j = x
Ctz(x) == CHOOSE j \in Exps : x % 2^j = 0 /\ x % 2^(j + 1) # 0
Log2(x) == CHOOSE j \in Exps : 2^j <= x /\ x < 2^(j + 1)

\* The smallest power of two that is at least K.
P0 == CHOOSE p \in {2^j : j \in Exps} : p >= K /\ (p = 1 \/ p \div 2 < K)

\* Under I3 the first landmark p >= P0 whose answer is useful arrives by
\* rank p + K; each useless answer (lost or unknown) can cost one landmark.
Bound == P0 * 2^(MaxLoss + MaxUnknown) + K

\* The configuration must emit past the bound, or progress proves nothing.
ASSUME MaxRank > Bound

Ranks == 1..MaxRank
None == 0

VARIABLES
    n,          \* resolutions emitted (`emitted`)
    recent,     \* slot -> rank, or None
    landmarks,  \* slot -> rank, or None
    kept,       \* evicted ranks kept anyway: empty unless KeepEvicted
    inflight,   \* ranks whose answer is in transit
    resolved,   \* a terminal answer was applied: AdmissionPending is left
    eLoss, eUnknown, eDup,
    hApplied,   \* ranks whose answer was applied
    hForged,    \* ranks applied while outside the specified window
    hDelivered, \* ranks whose answer arrived at least once
    hAbsorbed,  \* ranks whose answer arrived and was absorbed
    hLost,      \* ranks whose answer was lost
    hOrder      \* an answer arrived after the answer of a newer rank

vars == <<n, recent, landmarks, kept, inflight, resolved, eLoss, eUnknown,
          eDup, hApplied, hForged, hDelivered, hAbsorbed, hLost, hOrder>>

Range(f) == {f[i] : i \in DOMAIN f}
Held == (Range(recent) \cup Range(landmarks) \cup kept) \ {None}

\* What I1 and I3 say is retained after n emissions, independently of the
\* rings: the R latest ranks, and the L latest powers of two.
SpecWindow(m) ==
    IF m = 0 THEN {}
    ELSE {x \in 1..m : x > m - R}
         \cup {2^j : j \in {i \in Exps : 2^i <= m /\ i > Log2(m) - L}}

Retained(r) == r \in Held \/ Mutation = "ApplyAny"

TypeOK ==
    /\ n \in 0..MaxRank
    /\ recent \in [0..(R - 1) -> {None} \cup Ranks]
    /\ landmarks \in [0..(L - 1) -> {None} \cup Ranks]
    /\ kept \subseteq Ranks
    /\ inflight \subseteq Ranks
    /\ resolved \in BOOLEAN
    /\ eLoss \in 0..MaxLoss /\ eUnknown \in 0..MaxUnknown /\ eDup \in 0..MaxDup
    /\ hApplied \subseteq Ranks /\ hForged \subseteq Ranks
    /\ hDelivered \subseteq Ranks /\ hAbsorbed \subseteq Ranks
    /\ hLost \subseteq Ranks /\ hOrder \in BOOLEAN

Init ==
    /\ n = 0
    /\ recent = [i \in 0..(R - 1) |-> None]
    /\ landmarks = [i \in 0..(L - 1) |-> None]
    /\ kept = {}
    /\ inflight = {}
    /\ resolved = FALSE
    /\ eLoss = 0 /\ eUnknown = 0 /\ eDup = 0
    /\ hApplied = {} /\ hForged = {} /\ hDelivered = {} /\ hAbsorbed = {}
    /\ hLost = {} /\ hOrder = FALSE

LandmarkSlot(x) == IF Mutation = "LandmarkSlotByRank" THEN x % L ELSE Ctz(x) % L

\* A deadline of the unresolved operation: one resolution of rank n + 1 is
\* emitted and retained, and the next deadline is armed (ResolveAndFence,
\* then ArmDeadline). Under I3 the timer cannot outrun an answer by more
\* than K resolutions.
EmitResolution ==
    LET m == n + 1
        old == IF IsPow2(m) /\ Mutation # "NoLandmarks"
               THEN landmarks[LandmarkSlot(m)] ELSE None
    IN
    /\ ~resolved /\ n < MaxRank
    /\ Lag = "uniform" => \A r \in inflight : m <= r + K
    /\ n' = m
    /\ recent' = [recent EXCEPT ![m % R] = m]
    /\ landmarks' = IF IsPow2(m) /\ Mutation # "NoLandmarks"
                    THEN [landmarks EXCEPT ![LandmarkSlot(m)] = m]
                    ELSE landmarks
    /\ kept' = IF Mutation = "KeepEvicted"
               THEN (kept \cup {recent[m % R], old}) \ {None}
               ELSE kept
    /\ inflight' = inflight \cup {m}
    /\ UNCHANGED <<resolved, eLoss, eUnknown, eDup, hApplied, hForged,
                   hDelivered, hAbsorbed, hLost, hOrder>>

\* The answer of rank r arrives, in any order. It is applied if r is
\* retained, absorbed otherwise; after the operation ended, a late answer
\* changes nothing (I4, I5). It may be delivered again later (a duplicate),
\* and it may be unknown (a refused fence).
AnswerArrives(r) ==
    /\ r \in inflight
    /\ \E unknown \in BOOLEAN, again \in BOOLEAN :
        /\ unknown => eUnknown < MaxUnknown
        /\ again => eDup < MaxDup
        /\ eUnknown' = IF unknown THEN eUnknown + 1 ELSE eUnknown
        /\ eDup' = IF again THEN eDup + 1 ELSE eDup
        /\ inflight' = IF again THEN inflight ELSE inflight \ {r}
        /\ IF ~resolved /\ Retained(r)
           THEN /\ resolved' = ~unknown
                /\ hApplied' = hApplied \cup {r}
                /\ hForged' = IF r \in SpecWindow(n) THEN hForged
                              ELSE hForged \cup {r}
                /\ UNCHANGED hAbsorbed
           ELSE /\ UNCHANGED <<resolved, hApplied, hForged>>
                /\ hAbsorbed' = IF resolved THEN hAbsorbed
                                ELSE hAbsorbed \cup {r}
    /\ hDelivered' = hDelivered \cup {r}
    /\ hOrder' = (hOrder \/ \E s \in hDelivered : s > r)
    /\ UNCHANGED <<n, recent, landmarks, kept, eLoss, hLost>>

AnswerLost(r) ==
    /\ r \in inflight /\ eLoss < MaxLoss
    /\ inflight' = inflight \ {r}
    /\ eLoss' = eLoss + 1
    /\ hLost' = hLost \cup {r}
    /\ UNCHANGED <<n, recent, landmarks, kept, resolved, eUnknown, eDup,
                   hApplied, hForged, hDelivered, hAbsorbed, hOrder>>

\* The end of a run: the operation resolved, or the bound of the model is
\* reached, and no answer is in transit.
Terminal == inflight = {} /\ (resolved \/ n = MaxRank)

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ EmitResolution
    \/ \E r \in Ranks : AnswerArrives(r) \/ AnswerLost(r)
    \/ Quiescent

Spec == Init /\ [][Next]_vars

\* The timer fires, and every answer in transit eventually arrives or is
\* lost. Nothing else is assumed.
FairSpec ==
    /\ Spec
    /\ WF_vars(EmitResolution)
    /\ \A r \in Ranks : WF_vars(AnswerArrives(r) \/ AnswerLost(r))

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

\* I1: at most 1 + R + L effects are retained: the Persist, R resolutions,
\* L landmarks.
BoundedRetention == Cardinality(Held) + 1 <= 1 + R + L

\* The rings hold exactly what the specification says is retained.
RetentionMatchesSpec == Held = SpecWindow(n)

\* I2: no result is applied for an effect the operation does not retain.
NothingForged == hForged = {}

(***************************************************************************)
(* Progress. Under I3 the exchange leaves AdmissionPending by rank Bound,  *)
(* whatever the order, the duplicates and the bounded losses.              *)
(***************************************************************************)
ProgressWithinBound == n > Bound => resolved

\* The shape of a run without a uniform bound: past the bound of I3, no
\* answer lost or unknown, every answer whose rank is no longer retained
\* has arrived and was absorbed, and the operation is still unresolved.
\* The answers still in transit are only retained ones, each to arrive
\* after its own eviction: the pattern repeats at every rank.
Starved ==
    /\ n > Bound /\ ~resolved
    /\ eLoss = 0 /\ eUnknown = 0
    /\ \A r \in 1..n : r \notin Held => r \in hAbsorbed
    /\ inflight \subseteq Held

NoStarvationWithEveryEvictedAnswerDelivered == ~Starved

LeavesAdmissionPending == <>resolved

(***************************************************************************)
(* Non-vacuity, each the negation of an invariant TLC must report.         *)
(***************************************************************************)

\* An answer that arrived more than R resolutions late was still applied,
\* through its landmark, after another answer was absorbed.
ReachLandmarkCommit ==
    /\ resolved /\ hAbsorbed # {}
    /\ \E r \in hApplied : IsPow2(r) /\ n - r >= R

\* The answer of a landmark was lost, and a later landmark resolved.
ReachNextLandmarkAfterLoss ==
    /\ resolved
    /\ \E lost \in hLost, r \in hApplied : IsPow2(lost) /\ IsPow2(r) /\ r > lost

\* Answers arrived out of order and the operation resolved.
ReachOutOfOrder == resolved /\ hOrder

NoLandmarkCommit         == ~ReachLandmarkCommit
NoNextLandmarkAfterLoss  == ~ReachNextLandmarkAfterLoss
NoOutOfOrder             == ~ReachOutOfOrder

=============================================================================
