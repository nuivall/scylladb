\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

------------------------------ MODULE row_cache ------------------------------
(***************************************************************************)
(* The partition-level row cache of one table, against its underlying     *)
(* mutation source (the sstables), with memtable flushes merged into the   *)
(* cache by row_cache::update(), populating readers, eviction and          *)
(* invalidation.  See docs/dev/row_cache.md and the comment on _prev_      *)
(* snapshot_pos in db/row_cache.hh.                                        *)
(*                                                                         *)
(* The cache holds entries for some keys, and a continuity flag on each    *)
(* entry: the range between the previous entry and this one has no other   *)
(* partitions.  A key is known to the cache if it has an entry, or lies in *)
(* a continuous range.  Updates are not atomic: keys before _prev_snapshot_*)
(* pos reflect the new snapshot and phase, the rest the previous ones.     *)
(*                                                                         *)
(* Code (db/row_cache.cc, db/read_context.hh):                             *)
(*   FlushStart/UpdateStep   do_update(), update()                         *)
(*   InvalidateStart/Step    invalidate()                                  *)
(*   PointMiss/PointPopulate single_partition_populating_reader            *)
(*   Scan*                   autoupdating_underlying_reader,               *)
(*       range_populating_reader, do_find_or_create_entry()                *)
(*   Evict                   cache_entry eviction                          *)
(*                                                                         *)
(* Abstractions: a partition is one value, the newest write's timestamp    *)
(* (0: absent); an entry is complete (row-level population, which uses the *)
(* same phase, is not modelled); a scan covers the whole ring and always   *)
(* reads the underlying source (the case of an empty or discontinuous      *)
(* cache); writes only insert.  Positions: key k is 2k, "after key k" is   *)
(* 2k+1, the start of the ring is 0.                                       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    N,                    \* number of keys
    PopulateChecksPhase,  \* readers populate only if the phase still matches
    ContinuityChecksPrev, \* continuity is set only if the previous entry is the previous key read
    EvictClearsContinuity,\* evicting an entry clears the next entry's continuity
    UpdateChecksAbsence,  \* update() inserts a missing entry only if the key is known absent
    InvalidateClearsNext, \* invalidate() clears the next entry's continuity before it yields
    MaxWrites, MaxFlushes, MaxInvalidations, MaxEvictions, MaxPointReads, MaxScans

Keys == 1..N
End == N + 1             \* the dummy entry after all partitions, never evicted
NONE == -1
INF == 2 * N + 10

VARIABLES
    under,      \* under[k]: value of k in the underlying source (all sstables)
    prevUnder,  \* the previous snapshot, while an update runs
    mem,        \* the active memtable
    fm,         \* the memtable being merged into cache
    fmRem,      \* its keys update() hasn't processed yet
    updating,   \* "flush", "invalidate", or "none"
    invRem,     \* keys invalidate() hasn't processed yet
    phase,      \* _underlying_phase
    prevPos,    \* _prev_snapshot_pos; INF when no update runs
    ent,        \* ent[k]: the cache entry's value, or NONE
    cont,       \* cont[k] for k in 1..End: the entry's continuity flag
    pr,         \* point readers: [st, k, ph, val]
    sc,         \* the scanning reader
    nextTs, nW, nF, nI, nE, nP, nS

vars == <<under, prevUnder, mem, fm, fmRem, updating, invRem, phase, prevPos, ent, cont,
          pr, sc, nextTs, nW, nF, nI, nE, nP, nS>>

Max(a, b) == IF a > b THEN a ELSE b

PhaseOf(p) == IF p < prevPos THEN phase ELSE phase - 1
SnapOf(p) == IF p < prevPos THEN under ELSE prevUnder

\* The first entry at or after key position k (lower_bound), the dummy at the end.
NextEntry(k) == CHOOSE j \in (k..End) : (j = End \/ ent[j] # NONE) /\ \A i \in k..(j - 1) : ent[i] = NONE
\* The entry before k, or 0 if k is the first.
PrevEntry(k) == IF \E i \in 1..(k - 1) : ent[i] # NONE
                THEN CHOOSE i \in 1..(k - 1) : ent[i] # NONE /\ \A j \in (i + 1)..(k - 1) : ent[j] = NONE
                ELSE 0
Known(k) == ent[k] # NONE \/ cont[NextEntry(k)]
CacheVal(k) == IF ent[k] # NONE THEN ent[k] ELSE 0

\* Inserts an entry for k, with the continuity of the range it splits.
Insert(e, c, k, v) == [e EXCEPT ![k] = v]
InsertCont(e, c, k) == [c EXCEPT ![k] = c[NextEntry(k)]]

Init ==
    /\ under = [k \in Keys |-> 0] /\ prevUnder = [k \in Keys |-> 0]
    /\ mem = [k \in Keys |-> 0] /\ fm = [k \in Keys |-> 0] /\ fmRem = {}
    /\ updating = "none" /\ invRem = {}
    /\ phase = 1 /\ prevPos = INF
    /\ ent = [k \in Keys |-> NONE]
    /\ cont = [k \in 1..End |-> TRUE]       \* an empty cache of an empty table
    /\ pr = [i \in 1..MaxPointReads |-> [st |-> "unused", k |-> 1, ph |-> 0, val |-> 0]]
    /\ sc = [st |-> "unused", crPhase |-> 0, snap |-> [k \in Keys |-> 0], auLast |-> 0,
             newLast |-> 0, rpLast |-> NONE, pos |-> 0]
    /\ nextTs = 1 /\ nW = 0 /\ nF = 0 /\ nI = 0 /\ nE = 0 /\ nP = 0 /\ nS = 0

-----------------------------------------------------------------------------
Write(k) ==
    /\ nW < MaxWrites
    /\ mem' = [mem EXCEPT ![k] = nextTs]
    /\ nextTs' = nextTs + 1 /\ nW' = nW + 1
    /\ UNCHANGED <<under, prevUnder, fm, fmRem, updating, invRem, phase, prevPos, ent, cont,
                   pr, sc, nF, nI, nE, nP, nS>>

\* A flush: the memtable is written to an sstable, which the external
\* updater adds to the underlying source; the phase advances, and update()
\* starts merging the memtable into the cache.
FlushStart ==
    /\ updating = "none" /\ nF < MaxFlushes
    /\ \E k \in Keys : mem[k] # 0
    /\ prevUnder' = under
    /\ under' = [k \in Keys |-> Max(under[k], mem[k])]
    /\ fm' = mem /\ fmRem' = {k \in Keys : mem[k] # 0}
    /\ mem' = [k \in Keys |-> 0]
    /\ phase' = phase + 1 /\ prevPos' = 0
    /\ updating' = "flush" /\ nF' = nF + 1
    /\ UNCHANGED <<invRem, ent, cont, pr, sc, nextTs, nW, nI, nE, nP, nS>>

\* update() processes the memtable's next partition.
UpdateStep ==
    /\ updating = "flush" /\ fmRem # {}
    /\ LET k == CHOOSE x \in fmRem : \A y \in fmRem : x <= y
           rem == fmRem \ {k}
       IN /\ \/ /\ ent[k] # NONE
                /\ ent' = [ent EXCEPT ![k] = Max(@, fm[k])] /\ UNCHANGED cont
             \/ /\ ent[k] = NONE
                \* the range is continuous, or the presence checker says the key
                \* is definitely absent from the previous snapshot (it may say
                \* "maybe" for an absent key)
                /\ \/ ~UpdateChecksAbsence
                   \/ cont[NextEntry(k)]
                   \/ prevUnder[k] = 0
                /\ ent' = [ent EXCEPT ![k] = fm[k]]
                /\ cont' = InsertCont(ent, cont, k)
             \/ /\ ent[k] = NONE
                /\ UpdateChecksAbsence
                /\ ~cont[NextEntry(k)]
                /\ UNCHANGED <<ent, cont>>
          /\ fmRem' = rem
          /\ prevPos' = IF rem = {} THEN INF ELSE 2 * (CHOOSE x \in rem : \A y \in rem : x <= y)
          /\ updating' = IF rem = {} THEN "none" ELSE "flush"
    /\ UNCHANGED <<under, prevUnder, mem, fm, invRem, phase, pr, sc, nextTs, nW, nF, nI, nE, nP, nS>>

\* Something adds data to the underlying source behind the memtables (e.g.
\* an sstable from streaming), and invalidate() drops what the cache knows
\* about the whole ring.
InvalidateStart(k) ==
    /\ updating = "none" /\ nI < MaxInvalidations
    /\ prevUnder' = under
    /\ under' = [under EXCEPT ![k] = nextTs]
    /\ nextTs' = nextTs + 1
    /\ phase' = phase + 1 /\ prevPos' = 0
    /\ updating' = "invalidate" /\ nI' = nI + 1
    /\ UNCHANGED <<mem, fm, fmRem, invRem, ent, cont, pr, sc, nW, nF, nE, nP, nS>>

\* The loop of invalidate(): erases the first entry at or after
\* _prev_snapshot_pos (evict() leaves the next entry's continuity as it is),
\* and may yield with _prev_snapshot_pos at the next entry, whose
\* continuity it clears first (the clear_continuity(*it) after the loop).
\* At the end of the range it clears the continuity of the entry after it.
InvalidateStep ==
    /\ updating = "invalidate"
    /\ LET todo == {k \in Keys : ent[k] # NONE /\ 2 * k >= prevPos} IN
       IF todo = {}
       THEN /\ cont' = [cont EXCEPT ![End] = FALSE]
            /\ prevPos' = INF /\ updating' = "none"
            /\ UNCHANGED ent
       ELSE LET k == CHOOSE x \in todo : \A y \in todo : x <= y IN
            /\ ent' = [ent EXCEPT ![k] = NONE]
            /\ prevPos' = 2 * NextEntry(k + 1)
            /\ cont' = IF InvalidateClearsNext THEN [cont EXCEPT ![NextEntry(k + 1)] = FALSE] ELSE cont
            /\ UNCHANGED updating
    /\ UNCHANGED <<under, prevUnder, mem, fm, fmRem, invRem, phase, pr, sc, nextTs, nW, nF, nI, nE, nP, nS>>

Evict(k) ==
    /\ nE < MaxEvictions /\ ent[k] # NONE
    /\ ent' = [ent EXCEPT ![k] = NONE]
    /\ cont' = IF EvictClearsContinuity
               THEN [cont EXCEPT ![NextEntry(k + 1)] = FALSE]
               ELSE cont
    /\ nE' = nE + 1
    /\ UNCHANGED <<under, prevUnder, mem, fm, fmRem, updating, invRem, phase, prevPos, pr, sc,
                   nextTs, nW, nF, nI, nP, nS>>

-----------------------------------------------------------------------------
\* single_partition_populating_reader: a miss reads the snapshot of the
\* key's phase...
PointMiss(i, k) ==
    /\ pr[i].st = "unused" /\ nP < MaxPointReads
    /\ ~Known(k)
    /\ pr' = [pr EXCEPT ![i] = [st |-> "read", k |-> k, ph |-> PhaseOf(2 * k), val |-> SnapOf(2 * k)[k]]]
    /\ nP' = nP + 1
    /\ UNCHANGED <<under, prevUnder, mem, fm, fmRem, updating, invRem, phase, prevPos, ent, cont,
                   sc, nextTs, nW, nF, nI, nE, nS>>

\* ... and populates the cache if the key's phase is still the same.
PointPopulate(i) ==
    /\ pr[i].st = "read"
    /\ LET k == pr[i].k IN
       IF (pr[i].ph = PhaseOf(2 * k) \/ ~PopulateChecksPhase) /\ ent[k] = NONE
       THEN /\ ent' = [ent EXCEPT ![k] = pr[i].val]
            \* find_or_create_missing() copies the range's continuity;
            \* find_or_create_incomplete() creates a discontinuous entry.
            /\ cont' = IF pr[i].val = 0 THEN InsertCont(ent, cont, k) ELSE [cont EXCEPT ![k] = FALSE]
       ELSE UNCHANGED <<ent, cont>>
    /\ pr' = [pr EXCEPT ![i].st = "done"]
    /\ UNCHANGED <<under, prevUnder, mem, fm, fmRem, updating, invRem, phase, prevPos, sc,
                   nextTs, nW, nF, nI, nE, nP, nS>>

-----------------------------------------------------------------------------
\* scanning_and_populating_reader over the whole ring, reading the
\* underlying source through range_populating_reader.

ScanStart ==
    /\ sc.st \in {"unused", "done"} /\ nS < MaxScans
    /\ sc' = [st |-> "scan", crPhase |-> PhaseOf(0), snap |-> SnapOf(0), auLast |-> 0,
              newLast |-> 0, rpLast |-> 0, pos |-> 0]
    /\ nS' = nS + 1
    /\ UNCHANGED <<under, prevUnder, mem, fm, fmRem, updating, invRem, phase, prevPos, ent, cont,
                   pr, nextTs, nW, nF, nI, nE, nP>>

\* Sets k's continuity if the entry before it is the one the reader
\* populated last (do_find_or_create_entry()).
LinkCont(c, e, k, prevKey) ==
    IF prevKey # NONE /\ (~ContinuityChecksPrev \/ PrevEntry(k) = prevKey)
    THEN [c EXCEPT ![k] = TRUE] ELSE c

\* move_to_next_partition(): recreates the underlying reader from the
\* snapshot of the current phase if the phase changed, then reads the next
\* partition and populates it if its phase matches.
ScanStep ==
    /\ sc.st = "scan"
    /\ LET auLast == sc.newLast
           start == IF auLast > 0 THEN 2 * auLast + 1 ELSE 0
           ph == PhaseOf(start)
           recreate == ph # sc.crPhase
           crPhase == IF recreate THEN ph ELSE sc.crPhase
           snap == IF recreate THEN SnapOf(start) ELSE sc.snap
           \* can_set_continuity(): the phase of population_range_start()
           canSet == sc.rpLast # NONE /\ (crPhase = PhaseOf(start) \/ ~PopulateChecksPhase)
           nexts == {k \in Keys : k > sc.pos /\ snap[k] # 0}
       IN
       IF nexts = {}
       THEN \* end of stream: the dummy's continuity
            /\ cont' = IF canSet THEN LinkCont(cont, ent, End, sc.rpLast) ELSE cont
            /\ sc' = [sc EXCEPT !.st = "done"]
            /\ UNCHANGED ent
       ELSE LET k == CHOOSE x \in nexts : \A y \in nexts : x <= y IN
            /\ IF crPhase = PhaseOf(2 * k) \/ ~PopulateChecksPhase
               THEN /\ ent' = IF ent[k] = NONE THEN [ent EXCEPT ![k] = snap[k]] ELSE ent
                    /\ LET c1 == IF ent[k] = NONE THEN [cont EXCEPT ![k] = FALSE] ELSE cont
                       IN cont' = IF canSet THEN LinkCont(c1, ent', k, sc.rpLast) ELSE c1
               ELSE UNCHANGED <<ent, cont>>
            /\ sc' = [sc EXCEPT !.crPhase = crPhase, !.snap = snap, !.auLast = auLast,
                                !.newLast = k, !.rpLast = k, !.pos = k]
    /\ UNCHANGED <<under, prevUnder, mem, fm, fmRem, updating, invRem, phase, prevPos, pr,
                   nextTs, nW, nF, nI, nE, nP, nS>>

-----------------------------------------------------------------------------
Next ==
    \/ FlushStart \/ UpdateStep \/ InvalidateStep \/ ScanStart \/ ScanStep
    \/ \E k \in Keys : Write(k) \/ InvalidateStart(k) \/ Evict(k)
    \/ \E i \in 1..MaxPointReads : PointPopulate(i) \/ \E k \in Keys : PointMiss(i, k)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* What a read sees for a key the cache knows: the cache's value merged with
\* the memtable update() is still moving into the cache.
Effective(k) == Max(CacheVal(k), IF updating = "flush" /\ k \in fmRem THEN fm[k] ELSE 0)

\* Reads are correct: whatever the cache knows about a key, with the rest of
\* the memtable being merged, is the underlying source.  While invalidate()
\* runs, the part it hasn't reached yet (at or after _prev_snapshot_pos)
\* is exempt: it reflects the previous snapshot and is about to be erased.
Coherent ==
    \A k \in Keys :
        (Known(k) /\ ~(updating = "invalidate" /\ 2 * k >= prevPos)) => Effective(k) = under[k]

\* State abstraction for TLC (VIEW): only the order of timestamps matters,
\* so they are replaced by their ranks, and phases by their distance from
\* the current one (a phase two or more behind matches nothing).  This
\* changes no behavior.
Vals == {under[k] : k \in Keys} \cup {prevUnder[k] : k \in Keys} \cup {mem[k] : k \in Keys}
        \cup {fm[k] : k \in Keys} \cup {ent[k] : k \in Keys} \cup {pr[i].val : i \in DOMAIN pr}
        \cup {sc.snap[k] : k \in Keys}
Rk(t) == IF t <= 0 THEN t ELSE Cardinality({u \in Vals : 0 < u /\ u <= t})
RkF(f) == [k \in Keys |-> Rk(f[k])]
RelPh(x) == IF x < phase - 1 THEN -2 ELSE x - phase
\* (Comparing each variable with itself makes TLC materialize lazily built
\* functions in the state, which it otherwise fails to write to disk when
\* fingerprints come from a view.)
Materialized == under = under /\ prevUnder = prevUnder /\ mem = mem /\ fm = fm /\ ent = ent /\ cont = cont /\ pr = pr /\ sc = sc
StateView ==
    <<Materialized, RkF(under), IF updating = "none" THEN 0 ELSE RkF(prevUnder), RkF(mem),
      IF updating = "flush" THEN RkF(fm) ELSE 0, fmRem, updating, prevPos, RkF(ent), cont,
      [i \in DOMAIN pr |-> IF pr[i].st = "read" THEN [pr[i] EXCEPT !.ph = RelPh(@), !.val = Rk(@)] ELSE [st |-> pr[i].st]],
      IF sc.st = "scan" THEN [sc EXCEPT !.crPhase = RelPh(@), !.snap = RkF(@)] ELSE [st |-> sc.st],
      nW, nF, nI, nE, nP, nS>>

\* Witnesses.
W_ScanPopulated == ~(sc.st = "done" /\ cont[End] /\ \E k \in Keys : ent[k] > 0)
W_FlushIntoCache == ~(nF > 0 /\ updating = "none" /\ \E k \in Keys : ent[k] > 0 /\ ent[k] = under[k] /\ mem[k] = 0
                      /\ under[k] # prevUnder[k])
=============================================================================
