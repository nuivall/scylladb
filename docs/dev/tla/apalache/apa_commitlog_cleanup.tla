\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

------------------------- MODULE apa_commitlog_cleanup -------------------------
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
EXTENDS Integers, FiniteSets, Apalache

CONSTANTS
    \* @type: Set(Str);
    Writes,
    \* @type: Int;
    MaxCrashes

VARIABLES
    \* @type: Str -> Int;
    rp,
    \* @type: Int;
    nextRp,
    \* @type: Set(Str);
    active,
    \* @type: Set(Set(Str));
    sealed,
    \* @type: Set(Str);
    sstables,
    \* @type: Int;
    flushedRp,
    \* @type: Int;
    record,
    \* @type: Set(Str);
    acked,
    \* @type: Set(Str);
    lost,
    \* @type: Int;
    nc

vars == <<rp, nextRp, active, sealed, sstables, flushedRp, record, acked, lost, nc>>

Init ==
    /\ rp = [w \in Writes |-> 0] /\ nextRp = 1
    /\ active = {} /\ sealed = {} /\ sstables = {}
    /\ flushedRp = 0 /\ record = 0 /\ acked = {} /\ lost = {} /\ nc = 0

\* @type: Set(Int) => Int;
Max(S) == ApaFoldSet(LAMBDA a, b : IF a > b THEN a ELSE b, 0, S)

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

SaveRecord ==
    /\ flushedRp > record
    /\ record' = flushedRp
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

\* For Apalache: the constants.
CInit == Writes = {"x", "y"} /\ MaxCrashes = 1
CInit5 == Writes = {"w1", "w2", "w3", "w4", "w5"} /\ MaxCrashes = 1
CInit6 == Writes = {"w1", "w2", "w3", "w4", "w5", "w6"} /\ MaxCrashes = 1
CInit7 == Writes = {"w1", "w2", "w3", "w4", "w5", "w6", "w7"} /\ MaxCrashes = 1

\* No acknowledged write is skipped by the replay without being in an sstable.
NoLostWrite == lost = {}
=============================================================================
