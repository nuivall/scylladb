\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

----------------------------- MODULE hints_repair_gc -----------------------------
(***************************************************************************)
(* One key on two replicas X and Y, tombstone_gc in repair mode, hinted   *)
(* handoff, and repair's flush of hints before it records the repair time *)
(* (repair_flush_hints_batchlog, 63a126daf2a).                            *)
(*                                                                         *)
(* An operation (a write or a delete, with a timestamp from one clock)    *)
(* reaches X; its delivery to Y may fail, and the coordinator then keeps  *)
(* a hint created at the current time.  A hint is delivered at any later  *)
(* time.  A repair takes a sync point (now), waits for every hint created *)
(* at or before it; the wait may time out, and then with the discard     *)
(* option the hints created at or before the sync point are dropped and  *)
(* no repair time is recorded.  A repair that finished syncs X and Y and *)
(* records repair_time = the sync point.  GC purges, on a replica, a      *)
(* tombstone written before repair_time with the data it shadows.        *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS MaxOps, Discard, MaxTime

Reps == {"X", "Y"}

VARIABLES
    now,
    cell,      \* cell[r]: [ts, del, wtime] newest version on r, or NoCell
    hints,     \* set of [op, created]
    repairTime,
    acked,     \* the operations acknowledged: [ts, del]
    nops

vars == <<now, cell, hints, repairTime, acked, nops>>

NoCell == [ts |-> 0, del |-> FALSE, wtime |-> 0]

Init ==
    /\ now = 1 /\ cell = [r \in Reps |-> NoCell] /\ hints = {}
    /\ repairTime = 0 /\ acked = {} /\ nops = 0

Newer(a, b) == a.ts > b.ts \/ (a.ts = b.ts /\ a.del /\ ~b.del)
Merge(a, b) == IF Newer(b, a) THEN b ELSE a

Tick == now < MaxTime /\ now' = now + 1

\* A write or delete at timestamp now, reaching X; Y gets it or a hint is kept.
Op(del, toY) ==
    LET v == [ts |-> now, del |-> del, wtime |-> now] IN
    /\ nops < MaxOps
    /\ nops' = nops + 1
    /\ acked' = acked \cup {[ts |-> now, del |-> del]}
    /\ cell' = [r \in Reps |-> IF r = "X" \/ toY THEN Merge(cell[r], v) ELSE cell[r]]
    /\ hints' = IF toY THEN hints ELSE hints \cup {[op |-> v, created |-> now]}
    /\ Tick
    /\ UNCHANGED repairTime

Deliver(h) ==
    /\ h \in hints
    /\ cell' = [cell EXCEPT !["Y"] = Merge(@, h.op)]
    /\ hints' = hints \ {h}
    /\ UNCHANGED <<now, repairTime, acked, nops>>

\* A repair whose hints flush finished: every hint created at or before the
\* sync point was delivered; X and Y are synced; the repair time is recorded.
RepairOK ==
    /\ ~\E h \in hints : h.created <= now
    /\ cell' = [r \in Reps |-> Merge(cell["X"], cell["Y"])]
    /\ repairTime' = now
    /\ Tick
    /\ UNCHANGED <<hints, acked, nops>>

\* A repair whose hints flush timed out: no repair time; with Discard the
\* hints created at or before the sync point are dropped.
RepairTimeout ==
    /\ \E h \in hints : h.created <= now
    /\ hints' = IF Discard THEN {h \in hints : h.created > now} ELSE hints
    /\ Tick
    /\ UNCHANGED <<cell, repairTime, acked, nops>>

\* GC on a replica: a tombstone written before the repair time is purged
\* with what it shadows.
GC(r) ==
    /\ cell[r].del /\ cell[r].wtime < repairTime
    /\ cell' = [cell EXCEPT ![r] = NoCell]
    /\ UNCHANGED <<now, hints, repairTime, acked, nops>>

Next ==
    \/ \E del \in BOOLEAN, toY \in BOOLEAN : Op(del, toY)
    \/ \E h \in hints : Deliver(h)
    \/ RepairOK \/ RepairTimeout
    \/ \E r \in Reps : GC(r)

Spec == Init /\ [][Next]_vars

\* The newest acknowledged operation.
Last == CHOOSE a \in acked : \A b \in acked : a = b \/ Newer([ts |-> a.ts, del |-> a.del, wtime |-> 0], [ts |-> b.ts, del |-> b.del, wtime |-> 0])

\* A read at ALL never returns data whose newest acknowledged operation is a delete.
NoResurrection == acked # {} /\ Last.del =>
    LET m == Merge(cell["X"], cell["Y"]) IN m.ts = 0 \/ m.del

W_Purged == ~(\E r \in Reps : cell[r].ts = 0 /\ acked # {} /\ repairTime > 0 /\ hints = {})
=============================================================================
