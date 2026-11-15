\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

------------------------- MODULE group0_state_machine -------------------------
(***************************************************************************)
(* The group 0 state machine of one node, as seen by schema changes.       *)
(*                                                                         *)
(* Group 0 changes are optimistic read-modify-write operations: an         *)
(* operation reads the local state together with the last state ID in      *)
(* system.group0_history, computes a change, and appends a command that    *)
(* carries the observed ID as prev_state_id.  apply() turns a command      *)
(* whose prev_state_id is not the last ID into a no-op.  The protocol is   *)
(* correct only if the state an operation reads is the state the history   *)
(* ID names, which on a node means: the in-memory schema equals the        *)
(* persisted schema whenever an operation can read it.                     *)
(*                                                                         *)
(* Modelled (code in service/raft/group0_state_machine.cc and              *)
(* raft_group0_client.cc):                                                 *)
(*   ApplyNext      group0_state_machine::apply() / merge_and_apply()      *)
(*   SnapPull       transfer_snapshot(): pulling the remote state, outside *)
(*                  the read_apply mutex                                   *)
(*   SnapMerge      transfer_snapshot(): merge_schema_from() under the     *)
(*                  mutex, then the history mutation                       *)
(*   Enable         enable_in_memory_state_machine()                       *)
(*   LocalOp        start_operation() (read barrier, read_apply mutex,     *)
(*                  last state ID) + prepare_command() + add_entry()       *)
(*   RemoteOp       the same, on another node; always correct              *)
(*   Abort/Restart  raft_group0::abort_server_on_error(), node restart     *)
(*                                                                         *)
(* Abstractions:                                                           *)
(*   - Raft is a single committed log, `log`.  Every command is            *)
(*     committed when appended.                                            *)
(*   - A command's change adds one schema item, named by its new state     *)
(*     ID; schema state is a set of items, merged by union, like           *)
(*     timestamped mutations.                                              *)
(*   - schema_applier: a merge diffs the persisted schema before and       *)
(*     after writing the mutations and applies only the difference to      *)
(*     memory.  This is why a stale in-memory schema never heals by        *)
(*     itself.                                                             *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    \* Fix of 2f372ec9707: a failed change breaks the read_apply mutex
    \* (group0_state_machine::fail()) instead of releasing it.
    FixBreakMutexOnFailure,
    \* Fix of 02efd050634: on the join path the in-memory state machine is
    \* enabled when the server is created, so no snapshot transfer runs
    \* with it disabled.
    EnableImmediately,
    \* Hypothesis: in transfer_snapshot(), merge_schema_from() fails with an
    \* error other than schema_persisted_but_not_applied after the mutations
    \* became visible (database::do_apply_many() writes the commitlog
    \* atomically, then applies the memtables one by one).  Such an error
    \* does not call fail().
    ModelGenericFailureAfterPersist,
    MaxRemoteOps, MaxLocalOps, MaxFailures, MaxRestarts

NONE == [none |-> TRUE]

VARIABLES
    log,            \* committed group 0 log: sequence of commands
    nextId,         \* source of fresh, increasing state IDs
    disk,           \* schema items persisted on the node (system_schema.*)
    hist,           \* state IDs persisted in system.group0_history
    mem,            \* schema items in the node's in-memory schema
    inMem,          \* _in_memory_state_machine_enabled
    applied,        \* raft applied index of the node
    broken,         \* the read_apply semaphore is broken
    running,        \* the node's group 0 server runs
    abortPending,   \* abort_server_on_error() was called, abort not done
    pulled,         \* state pulled by a snapshot transfer, not yet merged
    nRemote, nLocal, nFail, nRestart

vars == <<log, nextId, disk, hist, mem, inMem, applied, broken, running,
          abortPending, pulled, nRemote, nLocal, nFail, nRestart>>

-----------------------------------------------------------------------------
\* The cluster-wide meaning of the log.  A command takes effect if its
\* prev_state_id is the last effective command's ID.  `ok` records whether
\* every effective command was computed from the state it was applied to.
RECURSIVE Eff(_)
Eff(k) ==
    IF k = 0 THEN [st |-> {}, last |-> 0, ok |-> TRUE]
    ELSE LET p == Eff(k - 1)
             e == log[k]
         IN IF e.prev = p.last
            THEN [st |-> p.st \cup {e.new}, last |-> e.new,
                  ok |-> p.ok /\ e.basis = p.st]
            ELSE p

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
LastId == Max(hist)

\* The read_apply mutex.  Actions that take it are atomic, so it is free
\* between actions unless broken.
MutexAvailable == ~broken

TypeOK ==
    /\ disk \subseteq 1..nextId /\ mem \subseteq 1..nextId
    /\ applied \in 0..Len(log)
    /\ inMem \in BOOLEAN /\ broken \in BOOLEAN /\ running \in BOOLEAN

Init ==
    /\ log = <<>>
    /\ nextId = 1
    /\ disk = {} /\ mem = {} /\ hist = {0}
    /\ inMem = EnableImmediately
    /\ applied = 0
    /\ broken = FALSE
    /\ running = TRUE
    /\ abortPending = FALSE
    /\ pulled = NONE
    /\ nRemote = 0 /\ nLocal = 0 /\ nFail = 0 /\ nRestart = 0

-----------------------------------------------------------------------------
\* A change that failed after persisting: the in-memory state is behind.
\* apply() catches every error; transfer_snapshot() only
\* schema_persisted_but_not_applied.  Either way raft stops the server in
\* the background.
FailChange ==
    /\ broken' = (broken \/ FixBreakMutexOnFailure)
    /\ abortPending' = TRUE

\* Another node runs a group 0 operation.  It reads the latest state.
RemoteOp ==
    /\ nRemote < MaxRemoteOps
    /\ LET e == Eff(Len(log)) IN
       log' = Append(log, [prev |-> e.last, new |-> nextId, basis |-> e.st,
                           origin |-> "remote"])
    /\ nextId' = nextId + 1
    /\ nRemote' = nRemote + 1
    /\ UNCHANGED <<disk, hist, mem, inMem, applied, broken, running,
                   abortPending, pulled, nLocal, nFail, nRestart>>

\* A DDL statement on the node: start_operation() does a read barrier
\* (waits until everything committed is applied), takes the read_apply
\* mutex and reads the last state ID; the statement is computed from the
\* in-memory schema; add_entry() appends the command.
LocalOp ==
    /\ nLocal < MaxLocalOps
    /\ running /\ inMem
    /\ applied = Len(log)
    /\ MutexAvailable
    /\ log' = Append(log, [prev |-> LastId, new |-> nextId, basis |-> mem,
                           origin |-> "local"])
    /\ nextId' = nextId + 1
    /\ nLocal' = nLocal + 1
    /\ UNCHANGED <<disk, hist, mem, inMem, applied, broken, running,
                   abortPending, pulled, nRemote, nFail, nRestart>>

\* The applier applies the next committed command.
ApplyNext ==
    /\ running /\ ~abortPending
    /\ MutexAvailable
    /\ applied < Len(log)
    /\ LET e == log[applied + 1]
           new == {e.new} \ disk
       IN IF e.prev # LastId
          THEN \* obsolete command: a no-op
               /\ applied' = applied + 1
               /\ UNCHANGED <<disk, hist, mem, broken, abortPending, nFail>>
          ELSE \/ \* success: the change, then the history entry
                  /\ disk' = disk \cup {e.new}
                  /\ mem' = IF inMem THEN mem \cup new ELSE mem
                  /\ hist' = hist \cup {e.new}
                  /\ applied' = applied + 1
                  /\ UNCHANGED <<broken, abortPending, nFail>>
               \/ \* the schema is persisted, updating memory fails
                  /\ inMem
                  /\ nFail < MaxFailures
                  /\ nFail' = nFail + 1
                  /\ disk' = disk \cup {e.new}
                  /\ FailChange
                  /\ UNCHANGED <<mem, hist, applied>>
    /\ UNCHANGED <<log, nextId, inMem, running, pulled, nRemote, nLocal,
                   nRestart>>

\* The leader sends a snapshot to the lagging node.  transfer_snapshot()
\* pulls the leader's current state, which may be newer than the snapshot
\* index, without holding the read_apply mutex.  raft_rpc::apply_snapshot()
\* calls it before raft checks for a snapshot in progress, so retries and
\* concurrent transfers are not excluded.
SnapPull ==
    /\ running
    /\ pulled = NONE
    /\ applied < Len(log)
    /\ \E idx \in (applied + 1)..Len(log) :
        LET e == Eff(Len(log)) IN
        pulled' = [st |-> e.st, hist |-> {0} \cup e.st, idx |-> idx]
    /\ UNCHANGED <<log, nextId, disk, hist, mem, inMem, applied, broken,
                   running, abortPending, nRemote, nLocal, nFail, nRestart>>

\* transfer_snapshot() takes the read_apply mutex and merges the schema.
SnapMerge ==
    /\ running
    /\ pulled # NONE
    /\ MutexAvailable
    /\ LET diff == pulled.st \ disk IN
       IF ~inMem
       THEN \* write_mutations_to_database(): disk only
            /\ disk' = disk \cup pulled.st
            /\ hist' = hist \cup pulled.hist
            /\ applied' = Max({applied, pulled.idx})
            /\ UNCHANGED <<mem, broken, abortPending, nFail>>
       ELSE \/ \* merge_schema_from() succeeds
               /\ disk' = disk \cup pulled.st
               /\ mem' = mem \cup diff
               /\ hist' = hist \cup pulled.hist
               /\ applied' = Max({applied, pulled.idx})
               /\ UNCHANGED <<broken, abortPending, nFail>>
            \/ \* schema_persisted_but_not_applied: fail() + _on_fatal_error
               /\ nFail < MaxFailures
               /\ diff # {}
               /\ nFail' = nFail + 1
               /\ disk' = disk \cup pulled.st
               /\ FailChange
               /\ UNCHANGED <<mem, hist, applied>>
            \/ \* another error after the mutations became visible: the
               \* exception propagates, raft retries the transfer later
               /\ ModelGenericFailureAfterPersist
               /\ nFail < MaxFailures
               /\ diff # {}
               /\ nFail' = nFail + 1
               /\ disk' = disk \cup pulled.st
               /\ UNCHANGED <<mem, hist, applied, broken, abortPending>>
    /\ pulled' = NONE
    /\ UNCHANGED <<log, nextId, inMem, running, nRemote, nLocal, nRestart>>

\* enable_in_memory_state_machine(): sets the flag and calls reload_state(),
\* which does not reload the schema.
Enable ==
    /\ running /\ ~inMem
    /\ MutexAvailable
    /\ inMem' = TRUE
    /\ UNCHANGED <<log, nextId, disk, hist, mem, applied, broken, running,
                   abortPending, pulled, nRemote, nLocal, nFail, nRestart>>

\* The background abort of the group 0 server completes.
Abort ==
    /\ abortPending
    /\ running' = FALSE
    /\ abortPending' = FALSE
    /\ pulled' = NONE
    /\ UNCHANGED <<log, nextId, disk, hist, mem, inMem, applied, broken,
                   nRemote, nLocal, nFail, nRestart>>

\* The node restarts: the schema is loaded from disk, raft replays the log
\* (the prev_state_id check makes replayed commands no-ops).
Restart ==
    /\ ~running /\ ~abortPending
    /\ nRestart < MaxRestarts
    /\ nRestart' = nRestart + 1
    /\ running' = TRUE
    /\ broken' = FALSE
    /\ mem' = disk
    /\ inMem' = TRUE
    /\ applied' = 0
    /\ UNCHANGED <<log, nextId, disk, hist, abortPending, pulled, nRemote,
                   nLocal, nFail>>

Next ==
    \/ RemoteOp \/ LocalOp \/ ApplyNext \/ SnapPull \/ SnapMerge
    \/ Enable \/ Abort \/ Restart

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Safety, cluster-wide: every effective group 0 command was computed from
\* the state it was applied on, i.e. group 0 read-modify-write operations
\* are linearizable.
Linearizable == Eff(Len(log)).ok

\* Safety, local: whoever can take the read_apply mutex sees an in-memory
\* schema that matches the persisted one.
MemMatchesDisk == (running /\ inMem /\ MutexAvailable) => mem = disk

=============================================================================
