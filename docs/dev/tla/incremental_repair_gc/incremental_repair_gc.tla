\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

------------------------- MODULE incremental_repair_gc -------------------------
(***************************************************************************)
(* Incremental tablet repair, tablet migration and tombstone GC in         *)
(* tombstone_gc = {'mode': 'repair'}, for one key of one tablet.           *)
(*                                                                         *)
(* An sstable is repaired iff 0 < repaired_at <= sstables_repaired_at      *)
(* (repair::is_repaired()).  An incremental repair takes the sstables that *)
(* aren't repaired on every replica (prepare_sstables_for_incremental_     *)
(* repair()), syncs the rows they hold, writing what a replica lacks into  *)
(* a new sstable, marks them all with sstables_repaired_at + 1             *)
(* (mark_sstable_as_repaired()), and the coordinator then commits          *)
(* sstables_repaired_at + 1 and the repair time.  It can fail between the  *)
(* marking and the commit.                                                 *)
(*                                                                         *)
(* A tablet migration streams the leaving replica's sstables, files and    *)
(* all, so repaired_at is kept (file-based streaming).                    *)
(*                                                                         *)
(* Compaction runs per class: on the repaired sstables, a tombstone is     *)
(* purged if it is GC-eligible and no repaired sstable outside the         *)
(* compaction holds older data for the key                                 *)
(* (sstable_set_for_tombstone_gc(): repaired only); on the others, if no   *)
(* sstable at all does.  GC eligibility: written before the repair time    *)
(* (propagation_delay 0).                                                  *)
(*                                                                         *)
(* Time is logical: writes and repair starts take the next tick.  A write  *)
(* at QUORUM reaches two of the three replicas, or all.                    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Nodes,             \* e.g. {"A", "B", "C", "P"}
    InitReplicas,      \* e.g. {"A", "B", "C"}
    RepairSkipsDown,   \* a repair may run with one replica down, skipping it
    MaxWrites, MaxRepairs, MaxMigrations, MaxCompactions, MaxFails

VARIABLES
    now,
    replicas,  \* the tablet's replicas
    sst,       \* sst[n]: sstables on node n: [ts, del, ra]
    sra,       \* sstables_repaired_at
    rtime,     \* repair time (0: never)
    writes,    \* every write: [ts, del]
    nW, nR, nM, nC, nF

vars == <<now, replicas, sst, sra, rtime, writes, nW, nR, nM, nC, nF>>

Repaired(s) == s.ra > 0 /\ s.ra <= sra

Init ==
    /\ now = 1
    /\ replicas = InitReplicas
    /\ sst = [n \in Nodes |-> {}]
    /\ sra = 0 /\ rtime = 0
    /\ writes = {}
    /\ nW = 0 /\ nR = 0 /\ nM = 0 /\ nC = 0 /\ nF = 0

-----------------------------------------------------------------------------
\* A write at QUORUM: a majority of the replicas gets it (in a new sstable).
Write(del) ==
    /\ nW < MaxWrites
    /\ \E S \in SUBSET replicas :
        /\ 2 * Cardinality(S) > Cardinality(replicas)
        /\ sst' = [n \in Nodes |-> IF n \in S THEN sst[n] \cup {[ts |-> now, del |-> del, ra |-> 0]} ELSE sst[n]]
    /\ writes' = writes \cup {[ts |-> now, del |-> del]}
    /\ now' = now + 1 /\ nW' = nW + 1
    /\ UNCHANGED <<replicas, sra, rtime, nR, nM, nC, nF>>

\* An incremental repair of the replicas in R (all of them, or all but one
\* that is down): sync the rows of the sstables that aren't repaired, mark
\* them; then commit, or fail before the commit.
Repair(R, commit) ==
    /\ nR < MaxRepairs
    /\ R \subseteq replicas
    /\ R = replicas \/ (RepairSkipsDown /\ Cardinality(replicas \ R) = 1)
    /\ commit \/ nF < MaxFails
    /\ LET sets == [n \in R |-> {s \in sst[n] : ~Repaired(s)}]
           rows == {[ts |-> s.ts, del |-> s.del] : s \in UNION {sets[n] : n \in R}}
           mark(s) == [s EXCEPT !.ra = sra + 1]
       IN sst' = [n \in Nodes |->
                    IF n \in R
                    THEN {s \in sst[n] : Repaired(s)} \cup {mark(s) : s \in sets[n]}
                         \cup {[ts |-> r.ts, del |-> r.del, ra |-> sra + 1] :
                                 r \in {x \in rows : ~\E s \in sets[n] : s.ts = x.ts}}
                    ELSE sst[n]]
    /\ IF commit
       THEN /\ sra' = sra + 1 /\ rtime' = now /\ UNCHANGED nF
       ELSE /\ UNCHANGED <<sra, rtime>> /\ nF' = nF + 1
    /\ now' = now + 1 /\ nR' = nR + 1
    /\ UNCHANGED <<replicas, writes, nW, nM, nC>>

\* Migration of a replica to a new node: the new node gets copies of the
\* leaving replica's sstables, repaired_at included; the leaving replica's
\* data is cleaned up.
Migrate(l, p) ==
    /\ nM < MaxMigrations
    /\ l \in replicas /\ p \notin replicas /\ sst[p] = {}
    /\ replicas' = (replicas \ {l}) \cup {p}
    /\ sst' = [sst EXCEPT ![p] = sst[l], ![l] = {}]
    /\ nM' = nM + 1
    /\ UNCHANGED <<now, sra, rtime, writes, nW, nR, nC, nF>>

\* A compaction on node n of the sstables of one class.
Compact(n, rep) ==
    /\ nC < MaxCompactions /\ n \in replicas
    /\ LET in == {s \in sst[n] : Repaired(s) = rep}
           top == CHOOSE s \in in : \A t \in in : t.ts <= s.ts
           consulted == {s \in sst[n] \ in : rep => Repaired(s)}
           purge == top.del /\ top.ts < rtime /\ \A s \in consulted : s.ts > top.ts
           outra == CHOOSE r \in {s.ra : s \in in} : \A t \in in : t.ra <= r
       IN /\ Cardinality(in) >= 2
          /\ sst' = [sst EXCEPT ![n] = (@ \ in) \cup (IF purge THEN {} ELSE {[top EXCEPT !.ra = outra]})]
    /\ nC' = nC + 1
    /\ UNCHANGED <<now, replicas, sra, rtime, writes, nW, nR, nM, nF>>

Next ==
    \/ \E d \in BOOLEAN : Write(d)
    \/ \E R \in SUBSET Nodes, c \in BOOLEAN : Repair(R, c)
    \/ \E l, p \in Nodes : Migrate(l, p)
    \/ \E n \in Nodes, rep \in BOOLEAN : Compact(n, rep)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* A read at QUORUM, of any majority of the replicas, returns the newest
\* write: nothing deleted comes back, nothing written is lost.
Newest(S) == CHOOSE c \in S : \A d \in S : d.ts <= c.ts
Visible(S) == IF S = {} THEN 0 ELSE IF Newest(S).del THEN 0 ELSE Newest(S).ts
Cells(Q) == {[ts |-> s.ts, del |-> s.del] : s \in UNION {sst[n] : n \in Q}}
ReadsCorrect ==
    \A Q \in SUBSET replicas :
        2 * Cardinality(Q) > Cardinality(replicas) => Visible(Cells(Q)) = Visible(writes)
=============================================================================
