\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

----------------------------- MODULE logstor_split -----------------------------
(***************************************************************************)
(* A tablet split of a logstor table on one shard: the write pipeline,    *)
(* the separator, split mode, the split of the main group's segments, the *)
(* readiness check and the completion.                                    *)
(*                                                                         *)
(* A write picks its compaction group when it is made: the main group, or *)
(* once the storage group is in split mode the split-ready group of its   *)
(* side (storage_group::select_compaction_group()).  It then goes through *)
(* the write buffer to a mixed segment, and a separator task queued after *)
(* the chunk write copies it into the separator buffer of the group it   *)
(* picked (write_to_separator(), record.target.cg).  A separator flush    *)
(* writes the buffer into a full segment of that group.                   *)
(*                                                                         *)
(* storage_group::split(): set split mode; if the main group is empty     *)
(* (no segments, empty separator buffers: logstor_group::empty()) the     *)
(* storage group is ready; otherwise flush the main group's separator      *)
(* buffer and split its segments into the split-ready groups.  The         *)
(* completion requires the main group to be empty                         *)
(* (handle_tablet_split_completion()).  After it the main group is gone.  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS Writes, DrainPipelineBeforeReady

Groups == {"main", "l", "r"}

VARIABLES
    splitMode, ready, completed,
    target,      \* target[w]: the group w picked, "" before it is made
    stage,       \* stage[w]: "none", "buffered", "queued" (chunk on disk, task queued), "sepbuf", "segment"
    segs,        \* segs[g]: records in g's full segments
    sepbuf,      \* sepbuf[g]: records in g's separator buffer
    orphan       \* records that reached a group which no longer exists

vars == <<splitMode, ready, completed, target, stage, segs, sepbuf, orphan>>

Init ==
    /\ splitMode = FALSE /\ ready = FALSE /\ completed = FALSE
    /\ target = [w \in Writes |-> ""]
    /\ stage = [w \in Writes |-> "none"]
    /\ segs = [g \in Groups |-> {}]
    /\ sepbuf = [g \in Groups |-> {}]
    /\ orphan = {}

Live(g) == ~(g = "main" /\ completed)

\* A write is made: it picks its group.
Make(w) ==
    /\ stage[w] = "none"
    /\ target' = [target EXCEPT ![w] = IF splitMode THEN "l" ELSE "main"]
    /\ stage' = [stage EXCEPT ![w] = "buffered"]
    /\ UNCHANGED <<splitMode, ready, completed, segs, sepbuf, orphan>>

\* The write buffer is written as a chunk; the separator task is queued.
WriteChunk(w) ==
    /\ stage[w] = "buffered"
    /\ stage' = [stage EXCEPT ![w] = "queued"]
    /\ UNCHANGED <<splitMode, ready, completed, target, segs, sepbuf, orphan>>

\* The separator task copies the record into its group's separator buffer.
Separate(w) ==
    /\ stage[w] = "queued"
    /\ stage' = [stage EXCEPT ![w] = "sepbuf"]
    /\ IF Live(target[w])
       THEN /\ sepbuf' = [sepbuf EXCEPT ![target[w]] = @ \cup {w}]
            /\ UNCHANGED orphan
       ELSE /\ orphan' = orphan \cup {w}
            /\ UNCHANGED sepbuf
    /\ UNCHANGED <<splitMode, ready, completed, target, segs>>

FlushSeparator(g) ==
    /\ sepbuf[g] # {}
    /\ Live(g)
    /\ segs' = [segs EXCEPT ![g] = @ \cup sepbuf[g]]
    /\ sepbuf' = [sepbuf EXCEPT ![g] = {}]
    /\ stage' = [w \in Writes |-> IF w \in sepbuf[g] THEN "segment" ELSE stage[w]]
    /\ UNCHANGED <<splitMode, ready, completed, target, orphan>>

MainEmpty == segs["main"] = {} /\ sepbuf["main"] = {}
PipelineHasMain == \E w \in Writes : target[w] = "main" /\ stage[w] \in {"buffered", "queued"}

\* storage_group::split(), one call: set split mode, check readiness, or
\* flush the main group's separator buffer and move its segments.
Split ==
    /\ ~completed
    /\ splitMode' = TRUE
    /\ IF MainEmpty /\ (DrainPipelineBeforeReady => ~PipelineHasMain)
       THEN /\ ready' = TRUE
            /\ UNCHANGED <<segs, sepbuf, stage>>
       ELSE /\ ready' = FALSE
            /\ segs' = [segs EXCEPT !["main"] = {}, !["l"] = @ \cup segs["main"] \cup sepbuf["main"]]
            /\ sepbuf' = [sepbuf EXCEPT !["main"] = {}]
            /\ stage' = [w \in Writes |-> IF w \in sepbuf["main"] THEN "segment" ELSE stage[w]]
    /\ UNCHANGED <<completed, target, orphan>>

\* A write to the main group after readiness makes it not ready again.
Complete ==
    /\ ready /\ ~completed
    /\ completed' = TRUE
    /\ UNCHANGED <<splitMode, ready, target, stage, segs, sepbuf, orphan>>

Next ==
    \/ \E w \in Writes : Make(w) \/ WriteChunk(w) \/ Separate(w)
    \/ \E g \in Groups : FlushSeparator(g)
    \/ Split \/ Complete

Spec == Init /\ [][Next]_vars

\* The completion finds the main group empty.
CompleteOnEmpty == completed => MainEmpty

\* No record reaches a group that no longer exists.
NoOrphan == orphan = {}

W_Completed == ~(completed /\ \E w \in Writes : stage[w] = "segment")
=============================================================================
