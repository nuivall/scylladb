\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

----------------------- MODULE commitlog_cleanup_proof -----------------------
(***************************************************************************)
(* A proof, for any set of writes, that a commitlog cleanup record kept    *)
(* below every replay position not yet in an sstable loses no write on    *)
(* replay: the fix of the bug found by ../commitlog_cleanup (catalog #10). *)
(* The write's commitlog add and its memtable apply stay separate steps,  *)
(* in any order, as in database::do_apply().                              *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLAPS

CONSTANTS Writes

VARIABLES rp, nextRp, active, sstables, record, acked, lost

vars == <<rp, nextRp, active, sstables, record, acked, lost>>

Init ==
    /\ rp = [w \in Writes |-> 0] /\ nextRp = 1
    /\ active = {} /\ sstables = {} /\ record = 0 /\ acked = {} /\ lost = {}

Add(w) ==
    /\ rp[w] = 0
    /\ rp' = [rp EXCEPT ![w] = nextRp]
    /\ nextRp' = nextRp + 1
    /\ UNCHANGED <<active, sstables, record, acked, lost>>

Apply(w) ==
    /\ rp[w] # 0 /\ w \notin acked
    /\ active' = active \cup {w}
    /\ acked' = acked \cup {w}
    /\ UNCHANGED <<rp, nextRp, sstables, record, lost>>

Flush ==
    /\ sstables' = sstables \cup active
    /\ active' = {}
    /\ UNCHANGED <<rp, nextRp, record, acked, lost>>

\* The fixed save: below every position of a write not in an sstable.
SaveRecord ==
    \E v \in Nat :
        /\ record < v /\ v < nextRp
        /\ \A w \in Writes : rp[w] # 0 /\ w \notin sstables => v < rp[w]
        /\ record' = v
        /\ UNCHANGED <<rp, nextRp, active, sstables, acked, lost>>

\* Replay skips a write at or below the record; one not in an sstable is lost.
Crash ==
    /\ lost' = {w \in acked : w \notin sstables /\ rp[w] <= record}
    /\ UNCHANGED <<rp, nextRp, active, sstables, record, acked>>

Next == (\E w \in Writes : Add(w) \/ Apply(w)) \/ Flush \/ SaveRecord \/ Crash

Spec == Init /\ [][Next]_vars

NoLostWrite == lost = {}

TypeOK ==
    /\ rp \in [Writes -> Nat] /\ nextRp \in Nat /\ nextRp >= 1
    /\ active \subseteq Writes /\ sstables \subseteq Writes /\ acked \subseteq Writes
    /\ lost \subseteq Writes /\ record \in Nat

IndInv ==
    /\ TypeOK
    /\ record < nextRp
    /\ \A w \in Writes : rp[w] < nextRp
    /\ \A w \in acked : rp[w] # 0
    /\ \A w \in Writes : rp[w] # 0 /\ w \notin sstables => record < rp[w]
    /\ lost = {}

LEMMA InitInv == Init => IndInv
  BY DEF Init, IndInv, TypeOK

LEMMA NextInv == IndInv /\ [Next]_vars => IndInv'
<1> SUFFICES ASSUME IndInv, [Next]_vars PROVE IndInv'
  OBVIOUS
<1>1. ASSUME NEW w \in Writes, Add(w) PROVE IndInv'
  BY <1>1 DEF IndInv, TypeOK, Add
<1>2. ASSUME NEW w \in Writes, Apply(w) PROVE IndInv'
  BY <1>2 DEF IndInv, TypeOK, Apply
<1>3. CASE Flush
  BY <1>3 DEF IndInv, TypeOK, Flush
<1>4. CASE SaveRecord
  BY <1>4 DEF IndInv, TypeOK, SaveRecord
<1>5. CASE Crash
  BY <1>5 DEF IndInv, TypeOK, Crash
<1>6. CASE UNCHANGED vars
  BY <1>6 DEF IndInv, TypeOK, vars
<1> QED
  BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6 DEF Next

THEOREM Safety == Spec => []NoLostWrite
<1>1. IndInv => NoLostWrite
  BY DEF IndInv, NoLostWrite
<1> QED
  BY InitInv, NextInv, <1>1, PTL DEF Spec
=============================================================================
