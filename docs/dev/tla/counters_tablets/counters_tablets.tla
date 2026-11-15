\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

---------------------------- MODULE counters_tablets ----------------------------
(***************************************************************************)
(* A counter of a tablet table (docs/dev/counters.md), and the migration   *)
(* of the tablet from node A to node B of the same rack.  Node C, in       *)
(* another rack, stays a replica.                                          *)
(*                                                                         *)
(* A counter cell is a set of shards, one per counter id: <<value,        *)
(* clock>>.  The id of a tablet replica is its rack's (R for A and B, Q    *)
(* for C); a pending replica uses the negated one (NR).  A leader reads    *)
(* its own shard from its local copy, adds the delta, increments the      *)
(* clock, and writes the shard locally and to the other write replicas;   *)
(* the write to a replica may fail (CL=ONE: the leader's write is enough). *)
(* Reconciliation keeps, per id, the shard with the highest clock.        *)
(*                                                                         *)
(* Migration: write_both (B gets writes, as pending replica), streaming    *)
(* (B gets A's cells, a snapshot taken at one point), use_new (A gets no  *)
(* more writes), done (B is a replica and uses R; A's data cleaned up).   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS MaxIncs, Migrate

Nodes == {"A", "B", "C"}
Ids == {"R", "NR", "Q"}
Zero == <<0, 0>>

VARIABLES
    cell,     \* cell[n][id]: n's local copy of the shard
    stage,    \* "none", "write_both", "streamed", "use_new", "done"
    sent,     \* sum of acknowledged increments
    ninc

vars == <<cell, stage, sent, ninc>>

Init ==
    /\ cell = [n \in Nodes |-> [i \in Ids |-> Zero]]
    /\ stage = "none" /\ sent = 0 /\ ninc = 0

WriteReplicas ==
    CASE stage \in {"none"} -> {"A", "C"}
      [] stage \in {"write_both", "streamed"} -> {"A", "B", "C"}
      [] stage \in {"use_new", "done"} -> {"B", "C"}

IdOf(n) == CASE n = "C" -> "Q"
             [] n = "A" -> "R"
             [] n = "B" -> IF stage = "done" THEN "R" ELSE "NR"

Newer(a, b) == a[2] > b[2] \/ (a[2] = b[2] /\ a[1] > b[1])
Merge(a, b) == IF Newer(b, a) THEN b ELSE a

\* An increment led by n, replicated to the replicas in `to`; the others miss it.
Inc(n, to) ==
    LET id == IdOf(n)
        s == cell[n][id]
        new == <<s[1] + 1, s[2] + 1>>
    IN
    /\ ninc < MaxIncs
    /\ n \in WriteReplicas
    /\ to \subseteq WriteReplicas \ {n}
    \* the increment reaches C (or is led by it): it isn't lost with the
    \* leaving replica alone, so only the new replica can miss it
    /\ n = "C" \/ "C" \in to
    /\ cell' = [m \in Nodes |-> IF m = n \/ m \in to
                                THEN [cell[m] EXCEPT ![id] = Merge(@, new)] ELSE cell[m]]
    /\ sent' = sent + 1
    /\ ninc' = ninc + 1
    /\ UNCHANGED stage

Advance ==
    /\ Migrate
    /\ \/ stage = "none" /\ stage' = "write_both" /\ UNCHANGED cell
       \* streaming copies A's cells into B
       \/ stage = "write_both" /\ stage' = "streamed"
          /\ cell' = [cell EXCEPT !["B"] = [i \in Ids |-> Merge(cell["B"][i], cell["A"][i])]]
       \/ stage = "streamed" /\ stage' = "use_new" /\ UNCHANGED cell
       \* cleanup on A
       \/ stage = "use_new" /\ stage' = "done" /\ cell' = [cell EXCEPT !["A"] = [i \in Ids |-> Zero]]
    /\ UNCHANGED <<sent, ninc>>

Next ==
    \/ \E n \in Nodes, to \in SUBSET Nodes : Inc(n, to)
    \/ Advance

Spec == Init /\ [][Next]_vars

\* What a read at CL=ALL returns after everything is reconciled: per id
\* the newest shard over all replicas, summed.
Reconciled == LET best(i) == LET c == {cell[n][i] : n \in Nodes} IN
                              CHOOSE x \in c : \A y \in c : x = y \/ Newer(x, y)
              IN best("R")[1] + best("NR")[1] + best("Q")[1]

\* No increment is lost.
NoLostIncrement == Reconciled = sent

W_Done == ~(stage = "done" /\ sent > 1)
=============================================================================
