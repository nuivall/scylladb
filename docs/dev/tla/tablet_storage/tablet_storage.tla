\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

---------------------------- MODULE tablet_storage ----------------------------
(***************************************************************************)
(* The sstables of one tablet on one node, across its shards: tablet split *)
(* (split compaction, the bypass, the ACK, finalization), intra-node       *)
(* migration (clone, load, the coordinator's stage change, cleanup), and   *)
(* restarts, which reload what is on disk.                                 *)
(*                                                                         *)
(* The tablet T splits into the tablets l and r; the keys are "l" and "r". *)
(* An sstable holds a set of keys.  disk holds the files, sealed or not;   *)
(* mem[s] the sstables shard s has in its compaction groups: "main", or    *)
(* "l" / "r", the split-ready groups that become the tablets l and r at    *)
(* finalization (handle_tablet_split_completion()).  A write is durable    *)
(* (commitlog) and lands in a new sstable on each shard it is routed to.   *)
(*                                                                         *)
(* The reads of a tablet go to its owner (the read selector); at boot an  *)
(* sstable is loaded by the shard that serves reads of its range          *)
(* (compute_shards_for_this_sstable() with the tablet sharder), and       *)
(* unsealed files are removed.                                             *)
(*                                                                         *)
(* One compaction runs at a time.  It writes its outputs (sealed), then    *)
(* completes: update_sstable_sets_on_compaction_completion() swaps them    *)
(* in, or throws; on a throw it deletes the outputs it didn't attach       *)
(* (9cbab949b4d) -- for the bypass, the input itself.                      *)
(*                                                                         *)
(* An intra-node migration of a tablet from shard `from` to `to`:          *)
(* streaming (clone_locally_tablet_storage(): clone the sstables of the    *)
(* tablet on `from`, then load them into `to`, splitting the ones that     *)
(* need it, then respond to the coordinator); the coordinator, having the  *)
(* response, sets write_both_read_new (owner := to) in a later iteration   *)
(* (advance_in_background()); cleanup removes the tablet's sstables on     *)
(* `from`.  Writes go to both shards until cleanup.                        *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Shards,                 \* e.g. {1, 2}
    MaxWrites, MaxOps, MaxRestarts, MaxFails, MaxMigrations,
    BypassFailDeletesInput, \* code: TRUE -- a failed completion deletes all its outputs
    CloneUnsealed,          \* code: TRUE (48d243f32fc) -- clones stay unsealed until attached
    AttachAtomic,           \* code: TRUE (27d460758f3) -- split check and attach under the add gate
    RestartKeepsResponse    \* code: TRUE -- the coordinator advances on a response from before a restart

Keys == {"l", "r"}
Tablets == {"T", "l", "r"}

VARIABLES
    disk,        \* files: [id, keys, sealed]
    mem,         \* mem[s]: sstables of shard s: [id, keys, grp]
    nid,         \* next file id
    count,       \* tablet count: 1 (T) or 2 (l, r)
    needsSplit,  \* the split decision is set
    owner,       \* owner[t]: the shard serving reads of tablet t
    ack,         \* the node set its split ready sequence number (in memory)
    coordAck,    \* the coordinator has seen the node's ACK
    op,          \* the running compaction
    trans,       \* the intra-node migration
    written,     \* keys written (acknowledged)
    aborted,     \* finalization found an unsplit group (on_internal_error)
    bootFailed,  \* a sealed sstable spans two tablets at boot
    nW, nO, nR, nF, nM

vars == <<disk, mem, nid, count, needsSplit, owner, ack, coordAck, op, trans,
          written, aborted, bootFailed, nW, nO, nR, nF, nM>>

NoOp == [kind |-> "none"]
NoTrans == [st |-> "none"]

Init ==
    /\ disk = {} /\ mem = [s \in Shards |-> {}] /\ nid = 1
    /\ count = 1 /\ needsSplit = FALSE
    /\ owner = [t \in Tablets |-> CHOOSE s \in Shards : \A x \in Shards : s <= x]
    /\ ack = FALSE /\ coordAck = FALSE
    /\ op = NoOp /\ trans = NoTrans
    /\ written = {} /\ aborted = FALSE /\ bootFailed = FALSE
    /\ nW = 0 /\ nO = 0 /\ nR = 0 /\ nF = 0 /\ nM = 0

-----------------------------------------------------------------------------
TabletOf(k) == IF count = 1 THEN "T" ELSE k
Side(keys) == CHOOSE k \in keys : TRUE

\* The compaction group an sstable with these keys goes to
\* (select_compaction_group()).
Place(keys) ==
    IF count = 2 THEN Side(keys)
    ELSE IF needsSplit /\ Cardinality(keys) = 1 THEN Side(keys)
    ELSE "main"

InTablet(x, t) == count = 1 \/ x.grp = t

Migrating(t) == trans.st # "none" /\ trans.t = t
WriteShards(t) == {owner[t]} \cup (IF Migrating(t) THEN {trans.from, trans.to} ELSE {})

Ids(S) == {x.id : x \in S}
Without(D, ids) == {f \in D : f.id \notin ids}

-----------------------------------------------------------------------------
Write(k) ==
    /\ nW < MaxWrites
    /\ LET W == WriteShards(TabletOf(k)) IN
       /\ disk' = disk \cup {[id |-> nid + s, keys |-> {k}, sealed |-> TRUE] : s \in W}
       /\ mem' = [s \in Shards |-> IF s \in W
                    THEN mem[s] \cup {[id |-> nid + s, keys |-> {k}, grp |-> Place({k})]}
                    ELSE mem[s]]
    /\ nid' = nid + Cardinality(Shards) + 1
    /\ written' = written \cup {k}
    /\ nW' = nW + 1
    /\ UNCHANGED <<count, needsSplit, owner, ack, coordAck, op, trans, aborted, bootFailed, nO, nR, nF, nM>>

\* A regular compaction of a group with two or more sstables.
StartCompact(s, g) ==
    /\ op = NoOp /\ nO < MaxOps
    /\ LET ins == {x \in mem[s] : x.grp = g}
           out == [id |-> nid, keys |-> UNION {x.keys : x \in ins}, grp |-> g]
       IN /\ Cardinality(ins) >= 2
          /\ disk' = disk \cup {[id |-> nid, keys |-> out.keys, sealed |-> TRUE]}
          /\ op' = [kind |-> "compact", s |-> s, ins |-> ins, outs |-> {out}]
    /\ nid' = nid + 1 /\ nO' = nO + 1
    /\ UNCHANGED <<mem, count, needsSplit, owner, ack, coordAck, trans, written, aborted, bootFailed, nW, nR, nF, nM>>

\* The split of a main-group sstable: a split compaction into one sstable
\* per side, or, if it is on one side already, the bypass.
StartSplit(s, x) ==
    /\ op = NoOp /\ nO < MaxOps
    /\ count = 1 /\ needsSplit /\ x \in mem[s] /\ x.grp = "main"
    /\ IF Cardinality(x.keys) = 2
       THEN /\ disk' = disk \cup {[id |-> nid, keys |-> {"l"}, sealed |-> TRUE],
                                  [id |-> nid + 1, keys |-> {"r"}, sealed |-> TRUE]}
            /\ op' = [kind |-> "split", s |-> s, ins |-> {x},
                      outs |-> {[id |-> nid, keys |-> {"l"}, grp |-> "l"],
                                [id |-> nid + 1, keys |-> {"r"}, grp |-> "r"]}]
       ELSE /\ op' = [kind |-> "bypass", s |-> s, ins |-> {x},
                      outs |-> {[x EXCEPT !.grp = Side(x.keys)]}]
            /\ UNCHANGED disk
    /\ nid' = nid + 2 /\ nO' = nO + 1
    /\ UNCHANGED <<mem, count, needsSplit, owner, ack, coordAck, trans, written, aborted, bootFailed, nW, nR, nF, nM>>

\* What a failed completion deletes.
FailDeletes(o) == IF BypassFailDeletesInput THEN Ids(o.outs) ELSE Ids(o.outs) \ Ids(o.ins)
\* What an aborted compaction (stopped before completion) deletes.
AbortDeletes(o) == IF o = NoOp THEN {} ELSE Ids(o.outs) \ Ids(o.ins)

\* The completion swaps the outputs in and deletes the inputs; if an input
\* is gone ("Unable to remove input SSTable") it throws.
Complete ==
    /\ op # NoOp
    /\ IF op.ins \subseteq mem[op.s]
       THEN /\ mem' = [mem EXCEPT ![op.s] = (@ \ op.ins) \cup op.outs]
            /\ disk' = Without(disk, Ids(op.ins) \ Ids(op.outs))
       ELSE /\ disk' = Without(disk, FailDeletes(op))
            /\ UNCHANGED mem
    /\ op' = NoOp
    /\ UNCHANGED <<nid, count, needsSplit, owner, ack, coordAck, trans, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

\* The completion throws before attaching (bad_alloc, an internal error).
FailCompletion ==
    /\ op # NoOp /\ nF < MaxFails
    /\ disk' = Without(disk, FailDeletes(op))
    /\ op' = NoOp /\ nF' = nF + 1
    /\ UNCHANGED <<mem, nid, count, needsSplit, owner, ack, coordAck, trans, written, aborted, bootFailed, nW, nO, nR, nM>>

-----------------------------------------------------------------------------
SplitDecide ==
    /\ count = 1 /\ ~needsSplit
    /\ needsSplit' = TRUE
    /\ UNCHANGED <<disk, mem, nid, count, owner, ack, coordAck, op, trans, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

\* all_storage_groups_split() on every shard (the split monitor reduces
\* over the shards): the main groups are empty.  The coordinator then sees
\* the node's ACK, and may keep seeing it across a restart.
Ack ==
    /\ count = 1 /\ needsSplit /\ ~ack
    /\ \A s \in Shards : ~\E x \in mem[s] : x.grp = "main"
    /\ ack' = TRUE
    /\ coordAck' = TRUE
    /\ UNCHANGED <<disk, mem, nid, count, needsSplit, owner, op, trans, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

\* The coordinator finalizes once the node ACKed and no migration runs.
Finalize ==
    /\ count = 1 /\ needsSplit /\ coordAck /\ trans = NoTrans
    /\ count' = 2 /\ needsSplit' = FALSE
    /\ owner' = [owner EXCEPT !["l"] = owner["T"], !["r"] = owner["T"]]
    /\ ack' = FALSE /\ coordAck' = FALSE
    /\ aborted' = (aborted \/ \E s \in Shards : \E x \in mem[s] : x.grp = "main")
    /\ UNCHANGED <<disk, mem, nid, op, trans, written, bootFailed, nW, nO, nR, nF, nM>>

-----------------------------------------------------------------------------
MigStart(t, to) ==
    /\ trans = NoTrans /\ nM < MaxMigrations
    /\ t \in (IF count = 1 THEN {"T"} ELSE Keys) /\ to # owner[t]
    /\ trans' = [st |-> "stream", t |-> t, from |-> owner[t], to |-> to,
                 ph |-> "clone", clones |-> {}, resp |-> FALSE]
    /\ nM' = nM + 1
    /\ UNCHANGED <<disk, mem, nid, count, needsSplit, owner, ack, coordAck, op, written, aborted, bootFailed, nW, nO, nR, nF>>

\* clone_tablet_storage() on `from`: links of the tablet's sstables.
Clone ==
    /\ trans.st = "stream" /\ trans.ph = "clone"
    /\ LET src == {x \in mem[trans.from] : InTablet(x, trans.t)}
           rank(x) == Cardinality({y \in src : y.id < x.id})
           cl == {[id |-> nid + rank(x), keys |-> x.keys] : x \in src}
       IN /\ disk' = disk \cup {[id |-> c.id, keys |-> c.keys, sealed |-> ~CloneUnsealed] : c \in cl}
          /\ trans' = [trans EXCEPT !.ph = "load", !.clones = cl]
          /\ nid' = nid + Cardinality(src)
    /\ UNCHANGED <<mem, count, needsSplit, owner, ack, coordAck, op, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

\* The load on `to`, for each clone: split it if it needs it, seal, attach.
\* With AttachAtomic the split check and the attach are one step (under
\* the add gate); otherwise the check happens first and the attach later.
Split1(c) == IF count = 1 /\ needsSplit /\ Cardinality(c.keys) = 2
             THEN {[id |-> c.id * 100 + 1, keys |-> {"l"}], [id |-> c.id * 100 + 2, keys |-> {"r"}]}
             ELSE {c}
Checked == UNION {Split1(c) : c \in trans.clones}

LoadCheck ==
    /\ trans.st = "stream" /\ trans.ph = "load"
    /\ disk' = Without(disk, Ids(trans.clones)) \cup {[id |-> c.id, keys |-> c.keys, sealed |-> ~CloneUnsealed \/ c \notin trans.clones] : c \in Checked}
    /\ trans' = [trans EXCEPT !.ph = "attach", !.clones = Checked]
    /\ UNCHANGED <<mem, nid, count, needsSplit, owner, ack, coordAck, op, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

Attach ==
    /\ trans.st = "stream" /\ trans.ph = "attach"
    /\ disk' = {IF f.id \in Ids(trans.clones) THEN [f EXCEPT !.sealed = TRUE] ELSE f : f \in disk}
    /\ mem' = [mem EXCEPT ![trans.to] = @ \cup {[id |-> c.id, keys |-> c.keys, grp |-> Place(c.keys)] : c \in trans.clones}]
    /\ trans' = [trans EXCEPT !.ph = "respond", !.clones = {}]
    /\ UNCHANGED <<nid, count, needsSplit, owner, ack, coordAck, op, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

Load ==
    \/ /\ ~AttachAtomic /\ (LoadCheck \/ Attach)
    \/ /\ AttachAtomic /\ trans.st = "stream" /\ trans.ph = "load"
       /\ LET cs == Checked IN
          /\ disk' = Without(disk, Ids(trans.clones)) \cup {[id |-> c.id, keys |-> c.keys, sealed |-> TRUE] : c \in cs}
          /\ mem' = [mem EXCEPT ![trans.to] = @ \cup {[id |-> c.id, keys |-> c.keys, grp |-> Place(c.keys)] : c \in cs}]
          /\ trans' = [trans EXCEPT !.ph = "respond", !.clones = {}]
       /\ UNCHANGED <<nid, count, needsSplit, owner, ack, coordAck, op, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

Respond ==
    /\ trans.st = "stream" /\ trans.ph = "respond"
    /\ trans' = [trans EXCEPT !.ph = "done", !.resp = TRUE]
    /\ UNCHANGED <<disk, mem, nid, count, needsSplit, owner, ack, coordAck, op, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

\* The coordinator, holding the response: write_both_read_new.
ReadNew ==
    /\ trans.st = "stream" /\ trans.resp
    /\ trans' = [trans EXCEPT !.st = "read_new"]
    /\ owner' = [owner EXCEPT ![trans.t] = trans.to]
    /\ UNCHANGED <<disk, mem, nid, count, needsSplit, ack, coordAck, op, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

\* Cleanup on `from`: stop its compactions, drop the tablet's sstables.
Cleanup ==
    /\ trans.st = "read_new"
    /\ LET gone == {x \in mem[trans.from] : InTablet(x, trans.t)}
           stop == op # NoOp /\ op.s = trans.from
       IN /\ mem' = [mem EXCEPT ![trans.from] = @ \ gone]
          /\ disk' = Without(disk, Ids(gone) \cup (IF stop THEN AbortDeletes(op) ELSE {}))
          /\ op' = IF stop THEN NoOp ELSE op
    /\ trans' = NoTrans
    /\ UNCHANGED <<nid, count, needsSplit, owner, ack, coordAck, written, aborted, bootFailed, nW, nO, nR, nF, nM>>

-----------------------------------------------------------------------------
\* A crash and a boot.  The running compaction is lost (its sealed outputs
\* stay), unsealed files are removed, every sealed file is loaded by the
\* shard serving reads of its tablet; an unanswered streaming is retried.
Restart ==
    /\ nR < MaxRestarts
    /\ LET d == {f \in disk : f.sealed}
           spans(f) == count = 2 /\ Cardinality(f.keys) = 2
           tab(f) == IF count = 1 THEN "T" ELSE Side(f.keys)
       IN /\ disk' = d
          /\ mem' = [s \in Shards |-> {[id |-> f.id, keys |-> f.keys, grp |-> Place(f.keys)] :
                                         f \in {g \in d : ~spans(g) /\ owner[tab(g)] = s}}]
          /\ bootFailed' = (bootFailed \/ \E f \in d : spans(f))
    /\ trans' = IF trans.st = "stream" /\ (~trans.resp \/ ~RestartKeepsResponse)
                THEN [trans EXCEPT !.ph = "clone", !.clones = {}, !.resp = FALSE]
                ELSE trans
    /\ op' = NoOp
    /\ ack' = FALSE
    /\ nR' = nR + 1
    /\ UNCHANGED <<nid, count, needsSplit, owner, coordAck, written, aborted, nW, nO, nF, nM>>

-----------------------------------------------------------------------------
Next ==
    \/ \E k \in Keys : Write(k)
    \/ \E s \in Shards, g \in {"main", "l", "r"} : StartCompact(s, g)
    \/ \E s \in Shards : \E x \in mem[s] : StartSplit(s, x)
    \/ Complete \/ FailCompletion
    \/ SplitDecide \/ Ack \/ Finalize
    \/ \E t \in Tablets, s \in Shards : MigStart(t, s)
    \/ Clone \/ Load \/ Respond \/ ReadNew \/ Cleanup
    \/ Restart

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Every written key is in a sealed file (it survives a restart) ...
Durable == \A k \in written : \E f \in disk : f.sealed /\ k \in f.keys
\* ... and in an sstable of the shard serving its reads.
Readable == \A k \in written : \E x \in mem[owner[TabletOf(k)]] : k \in x.keys
\* After the node ACKed the split, no sealed sstable is unsplit
\* (48d243f32fc, 27d460758f3).
SplitAckHolds == (count = 1 /\ needsSplit /\ coordAck) =>
                    \A f \in disk : f.sealed => Cardinality(f.keys) = 1
NoAbort == ~aborted
BootOK == ~bootFailed

\* Witnesses: violated iff reachable.
W_Finalized == count = 1
W_MigratedAfterSplit == ~(count = 2 /\ trans.st = "read_new")
W_RestartWithResponse == ~(trans.st = "stream" /\ trans.resp /\ nR > 0)
=============================================================================
