\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

------------------------- MODULE tombstone_gc_repair -------------------------
(***************************************************************************)
(* Tombstone garbage collection in tombstone_gc = {'mode': 'repair'}, with  *)
(* tablet repair and tablet merge.                                         *)
(*                                                                         *)
(* A tombstone may be purged once a repair that started after it was       *)
(* written has completed for its range: all replicas have it then.  The    *)
(* repair time of a tablet goes into the tombstone GC state for the        *)
(* tablet's range (replica/tablets.cc, read_tablet_info), which keeps the  *)
(* maximum per token (repair_history_map, inplace_max).  A merge combines  *)
(* the repair times of the two siblings (merge_tablet_info() in            *)
(* locator/tablets.cc).                                                    *)
(*                                                                         *)
(* Keys: one per tablet.  Replicas: two, both replicas of both tablets.    *)
(* Time is logical: every write and every repair start takes the next     *)
(* tick.  A write reaches some of the replicas (CL=ONE, no hints).         *)
(* propagation_delay is 0.  Repair syncs all replicas and marks the data   *)
(* repaired; compaction runs per class (repaired, unrepaired), drops what  *)
(* a newer cell of the same class shadows, and purges a GC-eligible        *)
(* tombstone if nothing older for its key remains in the sstables it      *)
(* consults (all of them, or only the repaired ones when compacting the    *)
(* repaired class).                                                        *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    MergeRepairTime,  \* "max" (the code) or "min"
    MaxMerges,        \* 0 or 1
    MaxWrites, MaxRepairs, MaxCompactions

Replicas == {"A", "B"}
Keys == {"kL", "kR"}
TabletOf(k) == IF k = "kL" THEN "L" ELSE "R"
Tablets == {"L", "R"}

VARIABLES
    now,       \* logical clock
    store,     \* store[r]: cells [k, ts, del, rep] replica r has
    rt,        \* rt[t]: the tablet's repair time (0: never repaired)
    merged,    \* the two tablets were merged
    gc,        \* gc[k]: the GC state's repair time for k's token
    writes,    \* every write: [k, ts, del]
    nW, nR, nC

vars == <<now, store, rt, merged, gc, writes, nW, nR, nC>>

Init ==
    /\ now = 1
    /\ store = [r \in Replicas |-> {}]
    /\ rt = [t \in Tablets |-> 0]
    /\ merged = FALSE
    /\ gc = [k \in Keys |-> 0]
    /\ writes = {}
    /\ nW = 0 /\ nR = 0 /\ nC = 0

-----------------------------------------------------------------------------
Max2(a, b) == IF a > b THEN a ELSE b
Min2(a, b) == IF a < b THEN a ELSE b

\* A write at CL=ONE: it reaches a non-empty subset of the replicas.
Write(k, del) ==
    /\ nW < MaxWrites
    /\ \E S \in SUBSET Replicas \ {{}} :
        store' = [r \in Replicas |-> IF r \in S
                                     THEN store[r] \cup {[k |-> k, ts |-> now, del |-> del, rep |-> FALSE]}
                                     ELSE store[r]]
    /\ writes' = writes \cup {[k |-> k, ts |-> now, del |-> del]}
    /\ now' = now + 1 /\ nW' = nW + 1
    /\ UNCHANGED <<rt, merged, gc, nR, nC>>

\* Repair of a tablet (before the merge) or of the merged tablet: it starts
\* at `now`, syncs the keys of its range to all replicas, marks them
\* repaired, and records its start as the repair time of the range.
KeysOf(t) == IF merged THEN Keys ELSE {k \in Keys : TabletOf(k) = t}
Repair(t) ==
    /\ nR < MaxRepairs
    /\ merged => t = "L"
    /\ LET ks == KeysOf(t)
           synced == {[c EXCEPT !.rep = TRUE] : c \in UNION {{x \in store[r] : x.k \in ks} : r \in Replicas}}
       IN store' = [r \in Replicas |-> {x \in store[r] : x.k \notin ks} \cup synced]
    /\ rt' = IF merged THEN [x \in Tablets |-> now] ELSE [rt EXCEPT ![t] = now]
    /\ gc' = [k \in Keys |-> IF k \in KeysOf(t) THEN Max2(gc[k], now) ELSE gc[k]]
    /\ now' = now + 1 /\ nR' = nR + 1
    /\ UNCHANGED <<merged, writes, nW, nC>>

\* Tablet merge: the merged tablet's repair time goes into the GC state for
\* the whole merged range (taking the maximum per token).
Merge ==
    /\ ~merged /\ MaxMerges > 0
    /\ LET mrt == IF MergeRepairTime = "max" THEN Max2(rt["L"], rt["R"]) ELSE Min2(rt["L"], rt["R"])
       IN /\ rt' = [t \in Tablets |-> mrt]
          /\ gc' = [k \in Keys |-> Max2(gc[k], mrt)]
    /\ merged' = TRUE
    /\ UNCHANGED <<now, store, writes, nW, nR, nC>>

\* A compaction on replica r of the cells of key k in one class.
Compact(r, k, rep) ==
    /\ nC < MaxCompactions
    /\ LET in == {c \in store[r] : c.k = k /\ c.rep = rep}
           top == CHOOSE c \in in : \A d \in in : d.ts <= c.ts
           consulted == {c \in store[r] \ in : c.k = k /\ (rep => c.rep)}
           purge == top.del /\ top.ts < gc[k] /\ \A c \in consulted : c.ts > top.ts
       IN /\ in # {}
          /\ store' = [store EXCEPT ![r] = (@ \ in) \cup (IF purge THEN {} ELSE {top})]
    /\ nC' = nC + 1
    /\ UNCHANGED <<now, rt, merged, gc, writes, nW, nR>>

Next ==
    \/ Merge
    \/ \E k \in Keys, d \in BOOLEAN : Write(k, d)
    \/ \E t \in Tablets : Repair(t)
    \/ \E r \in Replicas, k \in Keys, rep \in BOOLEAN : Compact(r, k, rep)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* A read at CL=ALL returns the newest write: nothing deleted comes back.
Newest(S) == CHOOSE c \in S : \A d \in S : d.ts <= c.ts
Visible(S) == IF S = {} THEN 0 ELSE IF Newest(S).del THEN 0 ELSE Newest(S).ts
NoResurrection ==
    \A k \in Keys :
        Visible({[ts |-> c.ts, del |-> c.del] : c \in {x \in UNION {store[r] : r \in Replicas} : x.k = k}})
        = Visible({[ts |-> w.ts, del |-> w.del] : w \in {x \in writes : x.k = k}})

W_MergedAndPurged == ~(merged /\ \E k \in Keys : \E w \in writes : w.k = k /\ w.del
                        /\ \A r \in Replicas : \A c \in store[r] : c.k = k => FALSE)
=============================================================================
