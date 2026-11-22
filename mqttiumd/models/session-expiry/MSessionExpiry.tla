------------------------- MODULE MSessionExpiry -------------------------
EXTENDS Naturals, TLC

CONSTANTS MaxTick, MaxOps, MaxCrashes, AllowDomainChange, Mutation

VARIABLES session, anchor, domain, tick, running, active, request, timer,
          issued, crashes, changedDomain, hFixedDeadline, hStaleTimer,
          hExpiredResume, hProgressFloor, hDelayedDetach, hDetachedRestart

vars == <<session, anchor, domain, tick, running, active, request, timer,
          issued, crashes, changedDomain, hFixedDeadline, hStaleTimer,
          hExpiredResume, hProgressFloor, hDelayedDetach, hDetachedRestart>>

TTL == 2
NoRequest == [operation |-> 0, session |-> 0, token |-> 0, observed |-> 0]
NoTimer == [session |-> 0, token |-> 0, detachedBy |-> 0, deadline |-> 0]
Now == anchor.logical + (tick - anchor.tick)
Applicable == running /\ domain = anchor.domain /\ tick >= anchor.tick
Due == session.present /\ ~session.attached /\ Now >= session.deadline
TimerMatches == session.present /\ ~session.attached
             /\ timer.session = session.incarnation
             /\ timer.token = session.token
             /\ timer.detachedBy = session.detachedBy
             /\ timer.deadline = session.deadline
             /\ Now >= session.deadline

Init == /\ session = [present |-> TRUE, incarnation |-> 1, token |-> 1,
                       attached |-> TRUE, detachedBy |-> 0, deadline |-> 0]
        /\ anchor = [domain |-> 1, tick |-> 0, logical |-> 0]
        /\ domain = 1 /\ tick = 0 /\ running = TRUE /\ active = TRUE
        /\ request = NoRequest /\ timer = NoTimer /\ issued = 1
        /\ crashes = 0 /\ changedDomain = FALSE /\ hFixedDeadline = 0
        /\ hStaleTimer = FALSE /\ hExpiredResume = FALSE
        /\ hProgressFloor = 0 /\ hDelayedDetach = FALSE
        /\ hDetachedRestart = FALSE

Tick == /\ tick < MaxTick /\ tick' = tick + 1
        /\ UNCHANGED <<session, anchor, domain, running, active, request,
                       timer, issued, crashes, changedDomain, hFixedDeadline,
                       hStaleTimer, hExpiredResume, hProgressFloor,
                       hDelayedDetach, hDetachedRestart>>

ObserveClose == /\ Applicable /\ active /\ request = NoRequest
                /\ issued < MaxOps
                /\ request' = [operation |-> issued + 1,
                                session |-> session.incarnation,
                                token |-> session.token, observed |-> Now]
                /\ issued' = issued + 1 /\ active' = FALSE
                /\ UNCHANGED <<session, anchor, domain, tick, running, timer,
                               crashes, changedDomain, hFixedDeadline,
                               hStaleTimer, hExpiredResume, hProgressFloor,
                               hDelayedDetach, hDetachedRestart>>

CommitDetach ==
    /\ Applicable /\ request # NoRequest /\ session.present
    /\ session.attached /\ request.session = session.incarnation
    /\ request.token = session.token
    /\ LET captured == request.observed + TTL
           committed == IF Mutation = "deadline-at-commit" THEN Now + TTL
                        ELSE captured
       IN /\ session' = [session EXCEPT !.attached = FALSE,
                             !.detachedBy = request.operation,
                             !.deadline = committed]
          /\ hFixedDeadline' = captured
    /\ hDelayedDetach' = (hDelayedDetach \/ Now > request.observed)
    /\ request' = NoRequest
    /\ UNCHANGED <<anchor, domain, tick, running, active, timer, issued,
                   crashes, changedDomain, hStaleTimer, hExpiredResume,
                   hProgressFloor, hDetachedRestart>>

QueueTimer == /\ Applicable /\ Due /\ timer = NoTimer
              /\ timer' = [session |-> session.incarnation,
                            token |-> session.token,
                            detachedBy |-> session.detachedBy,
                            deadline |-> session.deadline]
              /\ UNCHANGED <<session, anchor, domain, tick, running, active,
                             request, issued, crashes, changedDomain,
                             hFixedDeadline, hStaleTimer, hExpiredResume,
                             hProgressFloor, hDelayedDetach, hDetachedRestart>>

ApplyTimer ==
    /\ Applicable /\ timer # NoTimer
    /\ LET applies == IF Mutation = "stale-timer"
                      THEN session.present /\ Now >= timer.deadline
                      ELSE TimerMatches
       IN /\ session' = IF applies
                        THEN [session EXCEPT !.present = FALSE,
                                                 !.attached = FALSE]
                        ELSE session
          /\ active' = IF applies THEN FALSE ELSE active
          /\ hStaleTimer' = (hStaleTimer \/ (applies /\ ~TimerMatches))
    \* The retained copy models one duplicated already-associated timer fact.
    \* It is not an unbounded product timer history.
    /\ UNCHANGED <<anchor, domain, tick, running, request, timer, issued,
                   crashes, changedDomain, hFixedDeadline, hExpiredResume,
                   hProgressFloor, hDelayedDetach, hDetachedRestart>>

Open ==
    /\ Applicable /\ request = NoRequest /\ issued < MaxOps
    /\ LET resumes == session.present /\
               (Mutation = "wait-for-timer" \/ ~Due)
           fresh == IF resumes THEN session.incarnation
                    ELSE session.incarnation + 1
       IN /\ session' = [present |-> TRUE, incarnation |-> fresh,
                          token |-> issued + 1, attached |-> TRUE,
                          detachedBy |-> 0, deadline |-> 0]
          /\ hExpiredResume' = (hExpiredResume \/ (resumes /\ Due))
    /\ issued' = issued + 1 /\ active' = TRUE /\ hFixedDeadline' = 0
    /\ UNCHANGED <<anchor, domain, tick, running, request, timer, crashes,
                   changedDomain, hStaleTimer, hProgressFloor,
                   hDelayedDetach, hDetachedRestart>>

Checkpoint ==
    /\ Applicable /\ session.present /\ ~session.attached
    /\ tick > anchor.tick
    /\ anchor' = [domain |-> domain, tick |-> tick, logical |-> Now]
    /\ hProgressFloor' = Now
    /\ UNCHANGED <<session, domain, tick, running, active, request, timer,
                   issued, crashes, changedDomain, hFixedDeadline,
                   hStaleTimer, hExpiredResume, hDelayedDetach,
                   hDetachedRestart>>

Crash == /\ Applicable /\ crashes < MaxCrashes
         /\ running' = FALSE /\ active' = FALSE /\ request' = NoRequest
         /\ crashes' = crashes + 1 /\ hProgressFloor' = Now
         /\ UNCHANGED <<session, anchor, domain, tick, timer, issued,
                        changedDomain, hFixedDeadline, hStaleTimer,
                        hExpiredResume, hDelayedDetach, hDetachedRestart>>

RestartSameDomain ==
    /\ ~running /\ domain = anchor.domain
    /\ LET recoveredNow == IF Mutation = "lose-progress"
                           THEN anchor.logical ELSE Now
       IN /\ session' = IF session.present /\ session.attached
                        THEN [session EXCEPT !.attached = FALSE,
                                !.detachedBy = session.token,
                                !.deadline = recoveredNow + TTL]
                        ELSE session
          /\ hFixedDeadline' = IF session.present /\ session.attached
                               THEN recoveredNow + TTL ELSE hFixedDeadline
          /\ anchor' = [domain |-> domain, tick |-> tick,
                         logical |-> recoveredNow]
    /\ running' = TRUE
    /\ hDetachedRestart' = (hDetachedRestart \/
                               (session.present /\ ~session.attached))
    /\ UNCHANGED <<domain, tick, active, request, timer, issued, crashes,
                   changedDomain, hStaleTimer, hExpiredResume,
                   hProgressFloor, hDelayedDetach>>

ChangeDomain == /\ ~running /\ AllowDomainChange /\ ~changedDomain
                /\ domain' = 2 /\ tick' = 0 /\ changedDomain' = TRUE
                \* Unprovable downtime is not subtracted. Only committed
                \* logical progress is the conservative lower bound.
                /\ hProgressFloor' = anchor.logical
                /\ UNCHANGED <<session, anchor, running, active, request,
                               timer, issued, crashes, hFixedDeadline,
                               hStaleTimer, hExpiredResume, hDelayedDetach,
                               hDetachedRestart>>

ReanchorAndRestart ==
    /\ ~running /\ domain # anchor.domain
    /\ anchor' = [domain |-> domain, tick |-> tick,
                   logical |-> anchor.logical]
    /\ session' = IF session.present /\ session.attached
                   THEN [session EXCEPT !.attached = FALSE,
                           !.detachedBy = session.token,
                           !.deadline = anchor.logical + TTL]
                   ELSE session
    /\ hFixedDeadline' = IF session.present /\ session.attached
                         THEN anchor.logical + TTL ELSE hFixedDeadline
    /\ running' = TRUE
    /\ hDetachedRestart' = (hDetachedRestart \/
                               (session.present /\ ~session.attached))
    /\ UNCHANGED <<domain, tick, active, request, timer, issued, crashes,
                   changedDomain, hStaleTimer, hExpiredResume,
                   hProgressFloor, hDelayedDetach>>

Idle == UNCHANGED vars

Next == Tick \/ ObserveClose \/ CommitDetach \/ QueueTimer \/ ApplyTimer
        \/ Open \/ Checkpoint \/ Crash \/ RestartSameDomain \/ ChangeDomain
        \/ ReanchorAndRestart \/ Idle
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ session \in [present : BOOLEAN, incarnation : 1..MaxOps,
                      token : 1..MaxOps, attached : BOOLEAN,
                      detachedBy : 0..MaxOps, deadline : Nat]
    /\ anchor \in [domain : 1..2, tick : 0..MaxTick, logical : Nat]
    /\ domain \in 1..2 /\ tick \in 0..MaxTick
    /\ running \in BOOLEAN /\ active \in BOOLEAN
    /\ request \in [operation : 0..MaxOps, session : 0..MaxOps,
                      token : 0..MaxOps, observed : Nat]
    /\ timer \in [session : 0..MaxOps, token : 0..MaxOps,
                    detachedBy : 0..MaxOps, deadline : Nat]
    /\ issued \in 1..MaxOps /\ crashes \in 0..MaxCrashes
    /\ changedDomain \in BOOLEAN /\ hFixedDeadline \in Nat
    /\ hStaleTimer \in BOOLEAN /\ hExpiredResume \in BOOLEAN
    /\ hProgressFloor \in Nat /\ hDelayedDetach \in BOOLEAN
    /\ hDetachedRestart \in BOOLEAN

FixedDetachDeadline == session.present /\ ~session.attached
                       => session.deadline = hFixedDeadline
NoStaleTimer == ~hStaleTimer
NoExpiredResume == ~hExpiredResume
ContinuousDomainProgress == Applicable => Now >= hProgressFloor
ActiveSessionPreserved == active => session.present /\ session.attached
NoBoundaryWitness == ~(hDelayedDetach /\ hDetachedRestart /\ timer # NoTimer)
=============================================================================
