--------------------------- MODULE MScanFrontier ---------------------------
EXTENDS Integers, FiniteSets

(*
 * Scan-frontier pagination and DirtySet invalidation for one subscriber
 * session (`Connections::mark_dirty_from`, `Connections::begin_scan`,
 * `Connections::complete_page`, `Owner::apply_draft`, `Owner::apply_pubsub`).
 *
 * Publications 1..N are admitted monotonically; their store commits and
 * owner StoreResult notifications may arrive out of order. A low obligation
 * (id 0) models retained subscription replay / Will / shard dirty with
 * unbounded invalidation (`dirty_any`). A bounded LoadWork snapshot takes at
 * most Page rows strictly above cursor and returns (items, next).
 *
 * Modes:
 * - "dirty_floor"              : repaired DirtySet frontier (Lot 11B)
 * - "76af3a5"                  : unconditional rescan rewind (R1 regression)
 * - "lot9"                     : complete_page overwrites rescan_needed with discard
 * - "mut_no_floor_check"       : ignores missed dirty publications <= page_next
 * - "mut_no_rescan_on_empty"   : clears rescan_needed when page_next = -1 even if dirty > high
 * - "mut_advance_under_pending": advances known_through past a pending lower publication
 *)

CONSTANTS
    N,
    Page,
    AllowOutOfOrder,
    AllowLowReplay,
    AllowInvalidate,
    Mode

VARIABLES
    next_pub,
    pending,
    committed,
    notified,
    low_done,
    loaded,
    finished,
    rescan_needed,
    after,
    known_through,
    dirty_any,
    dirty_pubs,
    loading,
    read_after,
    read_invalidated,
    page_ready,
    page_items,
    page_next,
    rewound_on_next

vars == <<
    next_pub, pending, committed, notified, low_done, loaded, finished,
    rescan_needed, after, known_through, dirty_any, dirty_pubs,
    loading, read_after, read_invalidated, page_ready, page_items, page_next,
    rewound_on_next
>>

None == -1
PubIds == 1..N
AllIds == 0..N
CursorDomain == -1..N

Max2(a, b) == IF a >= b THEN a ELSE b
MaxSet(S) == CHOOSE x \in S : \A y \in S : x >= y

\* Up to Page smallest elements of S.
SmallestPage(S) ==
    IF Cardinality(S) <= Page THEN S
    ELSE CHOOSE T \in SUBSET S :
        /\ Cardinality(T) = Page
        /\ \A x \in T, y \in (S \ T) : x < y

Init ==
    /\ next_pub = 1
    /\ pending = {}
    /\ committed = {}
    /\ notified = {}
    /\ low_done = FALSE
    /\ loaded = {}
    /\ finished = {}
    /\ rescan_needed = FALSE
    /\ after = None
    /\ known_through = None
    /\ dirty_any = FALSE
    /\ dirty_pubs = {}
    /\ loading = FALSE
    /\ read_after = None
    /\ read_invalidated = FALSE
    /\ page_ready = FALSE
    /\ page_items = {}
    /\ page_next = None
    /\ rewound_on_next = FALSE

TypeOK ==
    /\ next_pub \in 1..(N + 1)
    /\ pending \subseteq PubIds
    /\ committed \subseteq AllIds
    /\ notified \subseteq PubIds
    /\ low_done \in BOOLEAN
    /\ loaded \subseteq AllIds
    /\ finished \subseteq AllIds
    /\ rescan_needed \in BOOLEAN
    /\ after \in CursorDomain
    /\ known_through \in CursorDomain
    /\ dirty_any \in BOOLEAN
    /\ dirty_pubs \subseteq PubIds
    /\ loading \in BOOLEAN
    /\ read_after \in CursorDomain
    /\ read_invalidated \in BOOLEAN
    /\ page_ready \in BOOLEAN
    /\ page_items \subseteq AllIds
    /\ page_next \in CursorDomain
    /\ rewound_on_next \in BOOLEAN

Admit ==
    /\ next_pub <= N
    /\ next_pub' = next_pub + 1
    /\ pending' = pending \cup {next_pub}
    /\ UNCHANGED <<
        committed, notified, low_done, loaded, finished,
        rescan_needed, after, known_through, dirty_any, dirty_pubs,
        loading, read_after, read_invalidated, page_ready, page_items, page_next,
        rewound_on_next
       >>

StoreCommit ==
    \E p \in (pending \ committed) :
        /\ (AllowOutOfOrder \/ (\A q \in (pending \ committed) : p <= q))
        /\ committed' = committed \cup {p}
        /\ UNCHANGED <<
            next_pub, pending, notified, low_done, loaded, finished,
            rescan_needed, after, known_through, dirty_any, dirty_pubs,
            loading, read_after, read_invalidated, page_ready, page_items, page_next,
            rewound_on_next
           >>

ApplyCommit ==
    \E p \in (pending \cap committed) :
        /\ (AllowOutOfOrder \/ (\A q \in (pending \cap committed) : p <= q))
        /\ pending' = pending \ {p}
        /\ notified' = notified \cup {p}
        /\ rescan_needed' = TRUE
        /\ dirty_pubs' = IF loading THEN dirty_pubs \cup {p} ELSE dirty_pubs
        /\ after' = IF Mode \in {"dirty_floor", "mut_no_floor_check",
                                 "mut_no_rescan_on_empty", "mut_advance_under_pending"}
                    THEN IF after # None /\ after >= p THEN None ELSE after
                    ELSE after
        /\ known_through' = IF known_through # None /\ known_through >= p THEN None ELSE known_through
        /\ UNCHANGED <<
            next_pub, committed, low_done, loaded, finished,
            dirty_any, loading, read_after, read_invalidated, page_ready, page_items, page_next,
            rewound_on_next
           >>

LowReplayCommit ==
    /\ AllowLowReplay
    /\ ~low_done
    /\ low_done' = TRUE
    /\ committed' = committed \cup {0}
    /\ rescan_needed' = TRUE
    /\ dirty_any' = IF loading THEN TRUE ELSE dirty_any
    /\ after' = None
    /\ known_through' = None
    /\ UNCHANGED <<
        next_pub, pending, notified, loaded, finished, dirty_pubs,
        loading, read_after, read_invalidated, page_ready, page_items, page_next,
        rewound_on_next
       >>

BeginScan ==
    /\ ~loading
    /\ (rescan_needed \/ after # None)
    /\ LET cursor == IF after # None THEN after ELSE known_through IN
        /\ loading' = TRUE
        /\ read_after' = cursor
        /\ read_invalidated' = FALSE
        /\ page_ready' = FALSE
        /\ page_items' = {}
        /\ page_next' = None
        /\ rescan_needed' = FALSE
        /\ dirty_any' = FALSE
        /\ dirty_pubs' = {}
        /\ UNCHANGED <<
            next_pub, pending, committed, notified, low_done, loaded, finished,
            after, known_through, rewound_on_next
           >>

StoreSnapshot ==
    /\ loading
    /\ ~page_ready
    /\ LET avail == { d \in (committed \ finished) : d > read_after }
           items == SmallestPage(avail)
           has_more == Cardinality(avail) > Page
           nxt == IF has_more THEN MaxSet(items) ELSE None
       IN
        /\ page_ready' = TRUE
        /\ page_items' = items
        /\ page_next' = nxt
        /\ UNCHANGED <<
            next_pub, pending, committed, notified, low_done, loaded, finished,
            rescan_needed, after, known_through, dirty_any, dirty_pubs,
            loading, read_after, read_invalidated, rewound_on_next
           >>

CompletePage ==
    /\ loading
    /\ page_ready
    /\ LET discard == read_invalidated
           added == IF discard THEN {} ELSE page_items
           high == IF page_next # None THEN page_next
                   ELSE IF page_items # {} THEN MaxSet(page_items)
                   ELSE None
           pending_ok == IF Mode = "mut_advance_under_pending" THEN high # None
                         ELSE (high # None /\ \A q \in pending : q > high)
           missed_next ==
               IF Mode = "mut_no_floor_check" THEN FALSE
               ELSE (dirty_any \/ (\E p \in dirty_pubs : p <= page_next /\ p \notin page_items))
           missed_high ==
               IF Mode = "mut_no_floor_check" THEN FALSE
               ELSE (dirty_any \/ (high # None /\ \E p \in dirty_pubs : p <= high /\ p \notin page_items))
           has_above_high ==
               dirty_any
               \/ (high = None /\ dirty_pubs # {})
               \/ (high # None /\ \E p \in dirty_pubs : p > high)
       IN
        /\ loaded' = loaded \cup added
        /\ loading' = FALSE
        /\ read_after' = None
        /\ read_invalidated' = FALSE
        /\ page_ready' = FALSE
        /\ page_items' = {}
        /\ page_next' = None
        /\ dirty_any' = FALSE
        /\ dirty_pubs' = {}
        /\ IF Mode = "76af3a5" THEN
                LET r == rescan_needed \/ discard IN
                /\ rescan_needed' = r
                /\ after' = IF r THEN None ELSE page_next
                /\ rewound_on_next' = (rewound_on_next \/ (~discard /\ page_next # None /\ r))
                /\ known_through' =
                    IF ~discard /\ ~r /\ pending_ok
                    THEN Max2(known_through, high)
                    ELSE known_through
           ELSE IF Mode = "lot9" THEN
                /\ rescan_needed' = discard
                /\ after' = IF discard THEN None ELSE page_next
                /\ rewound_on_next' = rewound_on_next
                /\ known_through' =
                    IF ~discard /\ pending_ok
                    THEN Max2(known_through, high)
                    ELSE known_through
           ELSE \* dirty_floor (DirtySet) and its targeted mutants
                IF discard THEN
                    /\ rescan_needed' = TRUE
                    /\ after' = None
                    /\ rewound_on_next' = rewound_on_next
                    /\ known_through' = known_through
                ELSE IF page_next # None THEN
                    IF missed_next THEN
                        /\ rescan_needed' = TRUE
                        /\ after' = None
                        /\ rewound_on_next' = TRUE
                        /\ known_through' = known_through
                    ELSE
                        /\ rescan_needed' = FALSE
                        /\ after' = page_next
                        /\ rewound_on_next' = rewound_on_next
                        /\ known_through' =
                            IF pending_ok THEN Max2(known_through, page_next)
                            ELSE known_through
                ELSE \* page_next = None
                    IF missed_high THEN
                        /\ rescan_needed' = TRUE
                        /\ after' = None
                        /\ rewound_on_next' = rewound_on_next
                        /\ known_through' = known_through
                    ELSE IF has_above_high THEN
                        /\ rescan_needed' =
                            IF Mode = "mut_no_rescan_on_empty" THEN FALSE ELSE TRUE
                        /\ after' = None
                        /\ rewound_on_next' = rewound_on_next
                        /\ known_through' =
                            IF pending_ok THEN Max2(known_through, high)
                            ELSE known_through
                    ELSE
                        /\ rescan_needed' = FALSE
                        /\ after' = None
                        /\ rewound_on_next' = rewound_on_next
                        /\ known_through' =
                            IF pending_ok THEN Max2(known_through, high)
                            ELSE known_through
        /\ UNCHANGED << next_pub, pending, committed, notified, low_done, finished >>

FinishDelivery ==
    /\ AllowInvalidate
    /\ \E d \in loaded :
        /\ loaded' = loaded \ {d}
        /\ finished' = finished \cup {d}
        /\ IF loading /\ (read_after = None \/ d >= read_after) THEN
                /\ read_invalidated' = TRUE
                /\ after' = None
           ELSE
                /\ UNCHANGED << read_invalidated, after >>
        /\ UNCHANGED <<
            next_pub, pending, committed, notified, low_done,
            rescan_needed, known_through, dirty_any, dirty_pubs,
            loading, read_after, page_ready, page_items, page_next,
            rewound_on_next
           >>

Terminal ==
    /\ next_pub = N + 1
    /\ pending = {}
    /\ (~AllowLowReplay \/ low_done)
    /\ ~loading
    /\ ~rescan_needed
    /\ after = None

Done ==
    /\ Terminal
    /\ UNCHANGED vars

Next ==
    \/ Admit
    \/ StoreCommit
    \/ ApplyCommit
    \/ LowReplayCommit
    \/ BeginScan
    \/ StoreSnapshot
    \/ CompletePage
    \/ FinishDelivery
    \/ Done

Spec == Init /\ [][Next]_vars

\* Every committed and notified obligation that has not been loaded/finished
\* lies strictly above known_through, and known_through never overtakes a
\* publication still pending commit.
KnownThroughSound ==
    /\ \A d \in (committed \ (loaded \cup finished)) :
        (d \in pending) \/ (known_through < d)
    /\ \A q \in pending : known_through < q

\* Once quiescent, no committed obligation can remain unloaded/invisible.
NoInvisibleObligation ==
    Terminal => (committed \subseteq (loaded \cup finished))

\* Under strictly monotonic commits with no invalidation or low replay,
\* a non-discarded page with page_next # None never rewinds its cursor.
MonotoneFanInProgress ==
    ~rewound_on_next

=============================================================================
