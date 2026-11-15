\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

----------------------------- MODULE cache_mvcc -----------------------------
(***************************************************************************)
(* The MVCC versions of one partition in the row cache, at row level: the  *)
(* rules of "Evictable snapshots" in docs/dev/mvcc.md.                     *)
(*                                                                         *)
(* Each version has its own entries and continuity (independent           *)
(* continuity): an entry's flag says the interval before it, back to the   *)
(* version's previous entry, is complete.  Every version has a last dummy  *)
(* entry.  The snapshot's logical value of a row is taken from the newest  *)
(* version which has the row or a continuous interval over it.             *)
(*                                                                         *)
(* Code: partition_entry, partition_version (mutation/partition_version.cc) *)
(*   Write        row_cache::update() -> apply_to_incomplete()             *)
(*   Populate     cache_mutation_reader populating the latest version      *)
(*   Evict        cache_tracker LRU eviction of rows_entry                 *)
(*   Touch        moving a row to the front of the LRU on read             *)
(*   Merge        merging versions no snapshot needs (apply_monotonically) *)
(*   Upgrade      a schema upgrade appending a version                     *)
(*   The last dummy of a new version goes to the front of the LRU.         *)
(*                                                                         *)
(* Abstractions: a row is one value, the newest write's timestamp; no      *)
(* range tombstones; population is phase-correct (see row_cache); a merge  *)
(* is atomic.                                                              *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    M,                      \* clustering positions 1..M; M+1 is the last dummy
    TouchOnlyLatest,        \* only entries of the latest version move to the LRU front
    EvictClearsNext,        \* eviction marks the next entry of its version discontinuous
    PopulateOnlyLatest,     \* population inserts only into the latest version
    MergedDummyTakesOlderLru,\* a merged last dummy takes the older version's LRU place (9ccd4ea416a)
    MaxWrites, MaxEvictions, MaxSnapshots, MaxPopulations, MaxUpgrades

Pos == 1..M
\* Every version id the bounds allow: the first, one per write, one per upgrade.
Ids == 1..(1 + MaxWrites + MaxUpgrades)
D == M + 1
NOV == [has |-> FALSE, val |-> 0, cont |-> FALSE]

VARIABLES
    under,    \* under[p]: the row in the underlying source (0: absent)
    vers,     \* versions, oldest first: [id, e] with e[p] = [has, val, cont] for p in 1..D
    snaps,    \* snaps[id]: open snapshots holding version id
    lru,      \* LRU of entries, least recently used first: <<[v, p]>>
    nextId, nextTs, nW, nE, nS, nP, nU

vars == <<under, vers, snaps, lru, nextId, nextTs, nW, nE, nS, nP, nU>>

Latest == vers[Len(vers)]
VIdx(id) == CHOOSE i \in 1..Len(vers) : vers[i].id = id
Has(v, p) == vers[v].e[p].has

\* In version v, the first entry at or after p (the last dummy at worst).
NextIn(v, p) == CHOOSE q \in p..D : (q = D \/ Has(v, q)) /\ \A r \in p..(q - 1) : ~Has(v, r)
\* Version v knows row p: it has the entry, or p is in a continuous interval.
Knows(v, p) == Has(v, p) \/ vers[v].e[NextIn(v, p)].cont
ValIn(v, p) == IF Has(v, p) THEN vers[v].e[p].val ELSE 0

\* The logical row of the snapshot of versions 1..top: from the newest one
\* that knows it; NONE if none does.
NONE == -1
Logical(top, p) ==
    IF \E v \in 1..top : Knows(v, p)
    THEN LET v == CHOOSE x \in 1..top : Knows(x, p) /\ \A y \in (x + 1)..top : ~Knows(y, p)
         IN ValIn(v, p)
    ELSE NONE

InLru(v, p) == \E i \in 1..Len(lru) : lru[i] = [v |-> v, p |-> p]
RemoveLru(s, x) == SelectSeq(s, LAMBDA y : y # x)
ToFront(s, x) == Append(RemoveLru(s, x), x)

Init ==
    /\ under = [p \in Pos |-> 0]
    \* the cache knows the partition is empty
    /\ vers = << [id |-> 1, e |-> [p \in 1..D |-> IF p = D THEN [has |-> TRUE, val |-> 0, cont |-> TRUE] ELSE NOV]] >>
    /\ snaps = [i \in Ids |-> 0]
    /\ lru = << [v |-> 1, p |-> D] >>
    /\ nextId = 2 /\ nextTs = 1
    /\ nW = 0 /\ nE = 0 /\ nS = 0 /\ nP = 0 /\ nU = 0

Held(id) == snaps[id] > 0

\* A new version on top, with only a last dummy, continuous nowhere.
NewVersion ==
    [id |-> nextId, e |-> [p \in 1..D |-> IF p = D THEN [has |-> TRUE, val |-> 0, cont |-> FALSE] ELSE NOV]]

-----------------------------------------------------------------------------
OpenSnapshot ==
    /\ nS < MaxSnapshots
    /\ snaps' = [snaps EXCEPT ![Latest.id] = @ + 1]
    /\ nS' = nS + 1
    /\ UNCHANGED <<under, vers, lru, nextId, nextTs, nW, nE, nP, nU>>

CloseSnapshot(id) ==
    /\ Held(id)
    /\ snaps' = [snaps EXCEPT ![id] = @ - 1]
    /\ UNCHANGED <<under, vers, lru, nextId, nextTs, nW, nE, nS, nP, nU>>

\* A write reaches the underlying source, and update() applies it to the
\* cache (apply_to_incomplete): into the latest version, or a new one if a
\* snapshot holds the latest.  The row is cached only if the snapshot knows
\* it, and then with everything older versions have (information
\* monotonicity); otherwise the cache keeps not knowing it.
Write(p) ==
    /\ nW < MaxWrites
    /\ under' = [under EXCEPT ![p] = nextTs]
    /\ LET needNew == Held(Latest.id)
           vs == IF needNew THEN Append(vers, NewVersion) ELSE vers
           top == Len(vs)
           known == Logical(Len(vers), p) # NONE
           e1 == [vs[top].e EXCEPT ![p] = [has |-> TRUE, val |-> nextTs, cont |-> @.cont]]
       IN /\ vers' = IF known THEN [vs EXCEPT ![top].e = e1] ELSE vs
          /\ lru' = LET l1 == IF needNew THEN Append(lru, [v |-> nextId, p |-> D]) ELSE lru
                    IN IF known THEN ToFront(l1, [v |-> vs[top].id, p |-> p]) ELSE l1
          /\ nextId' = IF needNew THEN nextId + 1 ELSE nextId
    /\ nextTs' = nextTs + 1 /\ nW' = nW + 1
    /\ UNCHANGED <<snaps, nE, nS, nP, nU>>

\* A reader that missed reads row p from the underlying source and inserts
\* it into the latest version; if the underlying source has no row between
\* the version's previous entry and p, the interval is marked continuous.
Populate(p, v) ==
    /\ nP < MaxPopulations
    /\ IF PopulateOnlyLatest THEN v = Len(vers) ELSE v \in 1..Len(vers)
    /\ ~Has(v, p)
    /\ LET prevs == {q \in 1..(p - 1) : Has(v, q)}
           lo == IF prevs = {} THEN 0 ELSE CHOOSE x \in prevs : \A y \in prevs : y <= x
           gapEmpty == \A q \in (lo + 1)..(p - 1) : under[q] = 0
       IN vers' = [vers EXCEPT ![v].e[p] = [has |-> TRUE, val |-> under[p], cont |-> gapEmpty]]
    /\ lru' = Append(lru, [v |-> vers[v].id, p |-> p])
    /\ nP' = nP + 1
    /\ UNCHANGED <<under, snaps, nextId, nextTs, nW, nE, nS, nU>>

\* A read touches row p of the version it found it in.
Touch(v, p) ==
    /\ Has(v, p)
    /\ TouchOnlyLatest => v = Len(vers)
    /\ InLru(vers[v].id, p)
    /\ lru' = ToFront(lru, [v |-> vers[v].id, p |-> p])
    /\ UNCHANGED <<under, vers, snaps, nextId, nextTs, nW, nE, nS, nP, nU>>

\* The least recently used entry is evicted; the interval it closed becomes
\* incomplete.  A last dummy isn't removed, only made discontinuous.
Evict ==
    /\ nE < MaxEvictions /\ Len(lru) > 0
    /\ LET x == Head(lru)
           v == VIdx(x.v)
           nx == IF x.p = D THEN D ELSE NextIn(v, x.p + 1)
           e1 == IF x.p = D THEN [vers[v].e EXCEPT ![D].cont = FALSE]
                 ELSE [vers[v].e EXCEPT ![x.p] = NOV]
           e2 == IF EvictClearsNext \/ x.p = D THEN [e1 EXCEPT ![nx].cont = FALSE] ELSE e1
       IN /\ vers' = [vers EXCEPT ![v].e = e2]
          /\ lru' = Tail(lru)
    /\ nE' = nE + 1
    /\ UNCHANGED <<under, snaps, nextId, nextTs, nW, nS, nP, nU>>

\* The oldest version, if no snapshot holds it, is merged into the next one.
\* Each row takes the value of the newer version if it knows the row, else
\* the older one's; entries and continuity are rebuilt from that (losing
\* knowledge where the result can't be expressed, which is safe).  Entries
\* keep their LRU places; the merged last dummy keeps the newer version's
\* place, or (9ccd4ea416a) takes the older one's.
MKnows(p) == Knows(1, p) \/ Knows(2, p)
MVal(p) == IF Knows(2, p) THEN ValIn(2, p) ELSE ValIn(1, p)
FromOld(p) == p # D /\ Has(1, p) /\ ~Has(2, p) /\ ~Knows(2, p)
MHas(p) == p = D \/ Has(2, p) \/ FromOld(p)
MPrev(q) == IF \E r \in 1..(q - 1) : MHas(r)
            THEN CHOOSE r \in 1..(q - 1) : MHas(r) /\ \A x \in (r + 1)..(q - 1) : ~MHas(x)
            ELSE 0
MCont(q) == \A r \in (MPrev(q) + 1)..(q - 1) : MKnows(r) /\ MVal(r) = 0

Merge ==
    /\ Len(vers) >= 2 /\ ~Held(vers[1].id)
    /\ LET o == vers[1]
           n == vers[2]
           e == [p \in 1..D |-> IF MHas(p)
                                THEN [has |-> TRUE, val |-> IF p = D THEN 0 ELSE MVal(p), cont |-> MCont(p)]
                                ELSE NOV]
           keep(y) == \/ y.v \notin {o.id, n.id}
                      \/ y.v = n.id /\ (y.p # D \/ ~MergedDummyTakesOlderLru) /\ MHas(y.p)
                      \/ y.v = o.id /\ FromOld(y.p)
                      \/ y.v = o.id /\ y.p = D /\ MergedDummyTakesOlderLru
           l1 == SelectSeq(lru, keep)
       IN /\ vers' = <<[id |-> n.id, e |-> e]>> \o SubSeq(vers, 3, Len(vers))
          /\ lru' = [i \in 1..Len(l1) |-> IF l1[i].v = o.id THEN [l1[i] EXCEPT !.v = n.id] ELSE l1[i]]
    /\ UNCHANGED <<under, snaps, nextId, nextTs, nW, nE, nS, nP, nU>>

\* A schema upgrade appends a version with only a last dummy, at the front
\* of the LRU.
Upgrade ==
    /\ nU < MaxUpgrades
    /\ vers' = Append(vers, NewVersion)
    /\ lru' = Append(lru, [v |-> nextId, p |-> D])
    /\ nextId' = nextId + 1 /\ nU' = nU + 1
    /\ UNCHANGED <<under, snaps, nextTs, nW, nE, nS, nP>>

-----------------------------------------------------------------------------
Next ==
    \/ OpenSnapshot \/ Evict \/ Merge \/ Upgrade
    \/ \E id \in Ids : CloseSnapshot(id)
    \/ \E p \in Pos : Write(p) \/ \E v \in 1..Len(vers) : Populate(p, v)
    \/ \E v \in 1..Len(vers), p \in 1..D : Touch(v, p)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Reads through the cache are correct: whatever the latest snapshot knows
\* about a row is the underlying source's row.
Correct == \A p \in Pos : Logical(Len(vers), p) \in {NONE, under[p]}

\* State abstraction for TLC (VIEW): only the order of timestamps matters,
\* so they are replaced by their ranks, and version ids by the versions'
\* positions.  This changes no behavior.
Vals == {under[p] : p \in Pos} \cup {vers[i].e[p].val : i \in 1..Len(vers), p \in 1..D}
Rk(t) == IF t <= 0 THEN t ELSE Cardinality({u \in Vals : 0 < u /\ u <= t})
\* (Comparing each variable with itself makes TLC materialize lazily built
\* functions in the state, which it otherwise fails to write to disk when
\* fingerprints come from a view.)
Materialized == vers = vers /\ lru = lru /\ snaps = snaps /\ under = under
StateView ==
    <<Materialized,
      [p \in Pos |-> Rk(under[p])],
      [i \in 1..Len(vers) |-> [p \in 1..D |-> [vers[i].e[p] EXCEPT !.val = Rk(@)]]],
      [i \in 1..Len(vers) |-> IF Held(vers[i].id) THEN snaps[vers[i].id] ELSE 0],
      [i \in 1..Len(lru) |-> [v |-> VIdx(lru[i].v), p |-> lru[i].p]],
      nW, nE, nS, nP, nU>>

\* Witnesses.
W_Merged == ~(nW > 0 /\ nS > 0 /\ Len(vers) = 1 /\ \E p \in Pos : Has(1, p) /\ vers[1].e[p].val > 0)
W_EvictedOld == ~(Len(vers) >= 2 /\ nE > 0 /\ \E p \in Pos : Logical(Len(vers), p) = under[p] /\ under[p] > 0)
=============================================================================
