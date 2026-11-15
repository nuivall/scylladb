\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

---------------------------- MODULE commitlog_cleanup ----------------------------
(***************************************************************************)
(* Writes to one compaction group on one shard, its memtables and         *)
(* flushes, and a commitlog cleanup record saved for its range (after a   *)
(* tablet repair: flush_pending_repair_time_update(), at                  *)
(* table::highest_flushed_replay_position(range)), across a crash.        *)
(*                                                                         *)
(* A write takes its replay position when the commitlog adds it, and      *)
(* reaches the active memtable later (database::apply(): the commitlog    *)
(* add, then apply_in_memory()); nothing orders the second step by the    *)
(* first.  Sealing a memtable notes its highest position; once it and the *)
(* memtables sealed before it are in sstables, the group's flushed        *)
(* position rises to it (seal_active_memtable(), 2da478abe9b).  Replay    *)
(* skips a mutation of the range at or below the record's position.       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS Writes, MaxCrashes,
    Fixed       \* the fix: the record stays below every write not yet in an sstable

VARIABLES
    rp,         \* rp[w]: the replay position of w, 0 before the commitlog add
    nextRp,
    active,     \* writes in the active memtable
    sealed,     \* sealed memtables, not yet in sstables: a set of sets
    sstables,   \* writes in sstables
    flushedRp,  \* the group's highest flushed position
    record,     \* the cleanup record's position, 0 for none
    acked,      \* writes acknowledged (applied to a memtable)
    lost, nc

vars == <<rp, nextRp, active, sealed, sstables, flushedRp, record, acked, lost, nc>>

Init ==
    /\ rp = [w \in Writes |-> 0] /\ nextRp = 1
    /\ active = {} /\ sealed = {} /\ sstables = {}
    /\ flushedRp = 0 /\ record = 0 /\ acked = {} /\ lost = {} /\ nc = 0

Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A x \in S : x <= m

AddToCommitlog(w) ==
    /\ rp[w] = 0
    /\ rp' = [rp EXCEPT ![w] = nextRp]
    /\ nextRp' = nextRp + 1
    /\ UNCHANGED <<active, sealed, sstables, flushedRp, record, acked, lost, nc>>

ApplyInMemory(w) ==
    /\ rp[w] # 0 /\ w \notin acked
    /\ active' = active \cup {w}
    /\ acked' = acked \cup {w}
    /\ UNCHANGED <<rp, nextRp, sealed, sstables, flushedRp, record, lost, nc>>

\* Seal and flush the active memtable in one step (one flush at a time).
Flush ==
    /\ active # {}
    /\ sstables' = sstables \cup active
    /\ flushedRp' = Max({flushedRp} \cup {rp[w] : w \in active})
    /\ active' = {}
    /\ UNCHANGED <<rp, nextRp, sealed, record, acked, lost, nc>>

Min(S) == CHOOSE m \in S : \A x \in S : m <= x
Safe == Min({flushedRp} \cup {rp[w] - 1 : w \in {x \in Writes : rp[x] # 0 /\ x \notin sstables}})
Target == IF Fixed THEN Safe ELSE flushedRp

SaveRecord ==
    /\ Target > record
    /\ record' = Target
    /\ UNCHANGED <<rp, nextRp, active, sealed, sstables, flushedRp, acked, lost, nc>>

\* A crash: the memtable is gone; replay brings back the writes above the
\* record's position.
Crash ==
    /\ nc < MaxCrashes
    /\ nc' = nc + 1
    /\ lost' = {w \in acked : w \notin sstables /\ rp[w] <= record}
    /\ UNCHANGED <<rp, nextRp, active, sealed, sstables, flushedRp, record, acked>>

Next ==
    \/ \E w \in Writes : AddToCommitlog(w) \/ ApplyInMemory(w)
    \/ Flush \/ SaveRecord \/ Crash

Spec == Init /\ [][Next]_vars

\* No acknowledged write is skipped by the replay without being in an sstable.
NoLostWrite == lost = {}
=============================================================================
