\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

------------------------------- MODULE view_building -------------------------------
(***************************************************************************)
(* The view building coordinator (docs/dev/view-building-coordinator.md)   *)
(* building one view of a base table with one tablet, RF=1, while the     *)
(* tablet migrates between nodes A and B (or the migration fails).        *)
(*                                                                         *)
(* Tasks live in group0 (`tasks`): [id, replica, aborted].  The           *)
(* coordinator sends a task to the worker of its replica; the worker      *)
(* builds (reads the base data it holds) and records the id as finished   *)
(* locally (lost when the node restarts); the coordinator learns the      *)
(* result and keeps it in memory (lost on a leader change), then removes  *)
(* the finished tasks from group0 in a later batch.  A task aborted in    *)
(* the meantime is not reported as finished.  A tablet migration aborts   *)
(* the tasks of the tablet at its start; at its end every task on the     *)
(* leaving replica gets a copy on the pending one and is deleted          *)
(* (generate_tablet_migration_updates()); a failed migration recreates    *)
(* the aborted tasks with new ids.  The view is marked built once no task *)
(* of it is left (no_tasks_left_for_view()).                              *)
(*                                                                         *)
(* covered: the view rows exist, i.e. a build ran to completion on the    *)
(* replica that held the tablet's data at that time.                      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS MaxIds, MaxRestarts

Nodes == {"A", "B"}

VARIABLES
    viewCreated,
    tasks,       \* set of [id, replica, aborted]
    nextId,
    holder,      \* the node holding the tablet's data (after cleanup)
    mig,         \* "none", "running" (A -> B, both hold data), "ended", "failed"
    running,     \* task ids the worker is executing
    wfinished,   \* wfinished[n]: ids the worker of n finished
    cfinished,   \* ids the coordinator learned finished, not yet removed
    covered, built, nr

vars == <<viewCreated, tasks, nextId, holder, mig, running, wfinished, cfinished, covered, built, nr>>

Init ==
    /\ viewCreated = FALSE
    /\ tasks = {}
    /\ nextId = 1
    /\ holder = "A"
    /\ mig = "none"
    /\ running = {}
    /\ wfinished = [n \in Nodes |-> {}]
    /\ cfinished = {}
    /\ covered = FALSE /\ built = FALSE /\ nr = 0

NewTask(id, r) == [id |-> id, replica |-> r, aborted |-> FALSE]
TaskOf(id) == CHOOSE t \in tasks : t.id = id
HasData(n) == n = holder \/ (mig = "running" /\ n \in {"A", "B"})

\* CREATE MATERIALIZED VIEW: a task per tablet replica of the tablet map,
\* which lists A until the migration ends.
CreateView ==
    /\ ~viewCreated /\ nextId <= MaxIds
    /\ viewCreated' = TRUE
    /\ tasks' = {NewTask(nextId, IF mig = "ended" THEN "B" ELSE "A")}
    /\ nextId' = nextId + 1
    /\ UNCHANGED <<holder, mig, running, wfinished, cfinished, covered, built, nr>>

\* The coordinator sends a live task to its worker (not while its tablet migrates).
Send(id) ==
    /\ \E t \in tasks : t.id = id /\ ~t.aborted
    /\ mig # "running"
    /\ id \notin running
    /\ running' = running \cup {id}
    /\ UNCHANGED <<viewCreated, tasks, nextId, holder, mig, wfinished, cfinished, covered, built, nr>>

\* The worker finishes a task it was executing: it read the data its node holds.
WorkerFinish(id) ==
    /\ id \in running
    /\ \E t \in tasks : t.id = id
    /\ LET r == TaskOf(id).replica IN
       /\ wfinished' = [wfinished EXCEPT ![r] = @ \cup {id}]
       /\ covered' = (covered \/ HasData(r))
    /\ running' = running \ {id}
    /\ UNCHANGED <<viewCreated, tasks, nextId, holder, mig, cfinished, built, nr>>

\* The coordinator learns the result: a task the worker finished and which isn't aborted.
Learn(id) ==
    /\ \E t \in tasks : t.id = id /\ ~t.aborted /\ id \in wfinished[t.replica]
    /\ cfinished' = cfinished \cup {id}
    /\ UNCHANGED <<viewCreated, tasks, nextId, holder, mig, running, wfinished, covered, built, nr>>

\* The batched removal of finished tasks.  An aborted task is kept, to make
\* the adjusted copy from it.
RemoveFinished ==
    /\ cfinished # {}
    /\ tasks' = {t \in tasks : t.id \notin cfinished \/ t.aborted}
    /\ cfinished' = {}
    /\ UNCHANGED <<viewCreated, nextId, holder, mig, running, wfinished, covered, built, nr>>

LeaderChange ==
    /\ nr < MaxRestarts
    /\ nr' = nr + 1
    /\ cfinished' = {}
    /\ UNCHANGED <<viewCreated, tasks, nextId, holder, mig, running, wfinished, covered, built>>

RestartWorker(n) ==
    /\ nr < MaxRestarts
    /\ nr' = nr + 1
    /\ wfinished' = [wfinished EXCEPT ![n] = {}]
    /\ running' = {i \in running : \A t \in tasks : t.id = i => t.replica # n}
    /\ UNCHANGED <<viewCreated, tasks, nextId, holder, mig, cfinished, covered, built>>

MigrationStart ==
    /\ mig = "none"
    /\ mig' = "running"
    /\ tasks' = {[t EXCEPT !.aborted = TRUE] : t \in tasks}
    /\ UNCHANGED <<viewCreated, nextId, holder, running, wfinished, cfinished, covered, built, nr>>

\* End: every task on the leaving replica is copied to the pending one; cleanup on A.
MigrationEnd ==
    /\ mig = "running"
    /\ mig' = "ended"
    /\ holder' = "B"
    /\ LET moved == {t \in tasks : t.replica = "A"}
           ids == {nextId + k : k \in 0..(Cardinality(moved) - 1)}
       IN /\ nextId + Cardinality(moved) - 1 <= MaxIds
          /\ tasks' = (tasks \ moved) \cup
                {NewTask(nextId + k, "B") : k \in 0..(Cardinality(moved) - 1)}
          /\ nextId' = nextId + Cardinality(moved)
    /\ UNCHANGED <<viewCreated, running, wfinished, cfinished, covered, built, nr>>

\* Failure: the aborted tasks are recreated with new ids; the data stays on A.
MigrationFail ==
    /\ mig = "running"
    /\ mig' = "failed"
    /\ LET ab == {t \in tasks : t.aborted} IN
       /\ nextId + Cardinality(ab) - 1 <= MaxIds
       /\ tasks' = (tasks \ ab) \cup {NewTask(nextId + k, "A") : k \in 0..(Cardinality(ab) - 1)}
       /\ nextId' = nextId + Cardinality(ab)
    /\ UNCHANGED <<viewCreated, holder, running, wfinished, cfinished, covered, built, nr>>

MarkBuilt ==
    /\ viewCreated /\ ~built
    /\ tasks = {}
    /\ built' = TRUE
    /\ UNCHANGED <<viewCreated, tasks, nextId, holder, mig, running, wfinished, cfinished, covered, nr>>

Next ==
    \/ CreateView \/ RemoveFinished \/ LeaderChange \/ MarkBuilt
    \/ MigrationStart \/ MigrationEnd \/ MigrationFail
    \/ \E id \in 1..MaxIds : Send(id) \/ WorkerFinish(id) \/ Learn(id)
    \/ \E n \in Nodes : RestartWorker(n)

Spec == Init /\ [][Next]_vars

\* A view marked built has its rows.
BuiltIsCovered == built => covered

W_BuiltAfterMigration == ~(built /\ mig = "ended")
W_BuiltAfterFailure == ~(built /\ mig = "failed")
=============================================================================
