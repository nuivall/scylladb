\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

-------------------------------- MODULE logstor --------------------------------
(***************************************************************************)
(* Logstor (docs/dev/logstor.md) on one shard: user writes appended to a   *)
(* mixed segment, the separator moving them into full segments of their    *)
(* compaction group, compaction, TRUNCATE, a tablet leaving the shard and  *)
(* coming back, and a crash followed by recovery.                          *)
(*                                                                         *)
(* The index is in memory.  Recovery rebuilds it by scanning every segment *)
(* whose first header is valid and keeping, for each key whose token has a *)
(* compaction group on the shard, the record with the highest timestamp    *)
(* (then segment sequence).  Freeing a segment doesn't touch the disk;     *)
(* only discard_segments() (TRUNCATE, tablet cleanup) zeroes the first     *)
(* block of the full segments the group owns.  A reused segment gets a new *)
(* sequence and its old records are gone.                                  *)
(*                                                                         *)
(* A record is <<key, id>>; wts[id] is the write's <<timestamp, value>>. *)
(* A delete is a record too.  Two writes may share a timestamp (client     *)
(* timestamps).  The runtime index keeps the later of two records of a     *)
(* key when their timestamps are equal (default_entry_cmp, insert());      *)
(* recovery keeps the one in the segment with the higher sequence, then at *)
(* the higher offset.  expected[k] is the write the runtime index holds:   *)
(* the last acknowledged one with the highest timestamp since the last     *)
(* TRUNCATE, or 0.  acked[k] are the writes since the last TRUNCATE.       *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Segs,              \* e.g. 1..4
    MaxRecs,           \* records per segment
    MaxWrites, MaxCrashes, MaxTruncates, MaxMoves, MaxCompactions,
    TruncateFlushes    \* TRUNCATE flushes the separator buffers before discarding

Keys == {"a", "b"}
Vals == {1, 2}
CG(k) == k             \* each key in its own compaction group (tablet)
CGs == Keys
Nil == 0

VARIABLES
    disk,      \* disk[s]: [valid, seq, kind ("mixed"/"full"), cg, recs]
    index,     \* index[k]: <<seg, ts>> or <<Nil, 0>>
    free,      \* free segments
    active,    \* the active mixed segment, or Nil
    sepbuf,    \* sepbuf[g]: records <<k, ts, src>> waiting for the separator
    owned,     \* compaction groups on this shard
    expected,  \* expected[k]
    wts, acked, clock,
    nextSeq, nextTs, nc, nt, nm, nk, crashed

vars == <<disk, index, free, active, sepbuf, owned, expected, wts, acked, clock, nextSeq, nextTs, nc, nt, nm, nk, crashed>>

NoLoc == <<Nil, 0>>
Empty == [valid |-> FALSE, seq |-> 0, kind |-> "mixed", cg |-> "", recs |-> <<>>]

Init ==
    /\ disk = [s \in Segs |-> Empty]
    /\ index = [k \in Keys |-> NoLoc]
    /\ free = Segs
    /\ active = Nil
    /\ sepbuf = [g \in CGs |-> <<>>]
    /\ owned = CGs
    /\ expected = [k \in Keys |-> 0]
    /\ wts = [i \in 1..MaxWrites |-> <<0, 0>>]
    /\ acked = [k \in Keys |-> {}]
    /\ clock = 1
    /\ nextSeq = 1 /\ nextTs = 1
    /\ nc = 0 /\ nt = 0 /\ nm = 0 /\ nk = 0 /\ crashed = FALSE

Live(s) == {k \in Keys : index[k][1] = s}
Refs(s) == {g \in CGs : \E i \in 1..Len(sepbuf[g]) : sepbuf[g][i][3] = s}

\* A segment is in use while it is active, referenced by a separator
\* buffer, or holds a live record.
InUse(s) == s = active \/ Refs(s) # {} \/ Live(s) # {}

AllocActive ==
    /\ active = Nil
    /\ \E s \in free :
        /\ active' = s
        /\ free' = free \ {s}
        /\ disk' = [disk EXCEPT ![s] = [valid |-> TRUE, seq |-> nextSeq, kind |-> "mixed", cg |-> "", recs |-> <<>>]]
        /\ nextSeq' = nextSeq + 1
    /\ UNCHANGED <<index, sepbuf, owned, expected, wts, acked, clock, nextTs, nc, nt, nm, nk, crashed>>

\* A full active segment is retired; it is freed once nothing uses it.
RetireActive ==
    /\ active # Nil
    /\ Len(disk[active].recs) = MaxRecs
    /\ active' = Nil
    /\ UNCHANGED <<disk, index, free, sepbuf, owned, expected, wts, acked, clock, nextSeq, nextTs, nc, nt, nm, nk, crashed>>

TS(id) == wts[id][1]

Write(k, tie, v) ==
    LET ts == IF tie THEN TS(expected[k]) ELSE clock IN
    /\ nextTs <= MaxWrites
    /\ tie => expected[k] # 0
    /\ wts' = [wts EXCEPT ![nextTs] = <<ts, v>>]
    /\ acked' = [acked EXCEPT ![k] = @ \cup {nextTs}]
    /\ clock' = IF tie THEN clock ELSE clock + 1
    /\ CG(k) \in owned
    /\ active # Nil
    /\ Len(disk[active].recs) < MaxRecs
    /\ disk' = [disk EXCEPT ![active].recs = Append(@, <<k, nextTs>>)]
    /\ index' = [index EXCEPT ![k] = <<active, nextTs>>]
    /\ sepbuf' = [sepbuf EXCEPT ![CG(k)] = Append(@, <<k, nextTs, active>>)]
    /\ expected' = [expected EXCEPT ![k] = nextTs]
    /\ nextTs' = nextTs + 1
    /\ UNCHANGED <<free, active, owned, nextSeq, nc, nt, nm, nk, crashed>>

\* The separator writes a group's buffer into a new full segment and moves
\* the index entries that still point at the records it copied.
WriteFull(g, recs, s) ==
    disk' = [disk EXCEPT ![s] = [valid |-> TRUE, seq |-> nextSeq, kind |-> "full", cg |-> g, recs |-> recs]]

SepFlush(g) ==
    /\ sepbuf[g] # <<>>
    /\ \E s \in free :
        LET b == sepbuf[g]
            recs == [i \in 1..Len(b) |-> <<b[i][1], b[i][2]>>]
        IN
        /\ WriteFull(g, recs, s)
        /\ free' = free \ {s}
        /\ nextSeq' = nextSeq + 1
        /\ index' = [k \in Keys |->
                IF \E i \in 1..Len(b) : b[i][1] = k /\ index[k] = <<b[i][3], b[i][2]>>
                THEN <<s, index[k][2]>> ELSE index[k]]
        /\ sepbuf' = [sepbuf EXCEPT ![g] = <<>>]
    /\ UNCHANGED <<active, owned, expected, wts, acked, clock, nextTs, nc, nt, nm, nk, crashed>>

\* Compaction rewrites the live records of a full segment of a group.
Compact(s) ==
    /\ disk[s].valid /\ disk[s].kind = "full"
    /\ disk[s].cg \in owned
    /\ Live(s) # {}
    /\ Refs(s) = {}
    /\ nk < MaxCompactions
    /\ nk' = nk + 1
    /\ \E t \in free :
        LET ks == Live(s)
            k1 == CHOOSE k \in ks : TRUE
            recs == IF Cardinality(ks) = 1 THEN <<<<k1, index[k1][2]>>>>
                    ELSE <<<<"a", index["a"][2]>>, <<"b", index["b"][2]>>>>
        IN
        /\ WriteFull(disk[s].cg, recs, t)
        /\ free' = free \ {t}
        /\ nextSeq' = nextSeq + 1
        /\ index' = [k \in Keys |-> IF k \in ks THEN <<t, index[k][2]>> ELSE index[k]]
    /\ UNCHANGED <<active, sepbuf, owned, expected, wts, acked, clock, nextTs, nc, nt, nm, crashed>>

\* free_segment(): back on the free list, the disk untouched.
Free(s) ==
    /\ s \notin free
    /\ disk[s].valid
    /\ ~InUse(s)
    /\ free' = free \cup {s}
    /\ UNCHANGED <<disk, index, active, sepbuf, owned, expected, wts, acked, clock, nextSeq, nextTs, nc, nt, nm, nk, crashed>>

\* discard_segments() of the groups gs: zero the first block of each full
\* segment they own and free it.
Discard(gs) ==
    LET ds == {s \in Segs : disk[s].valid /\ disk[s].kind = "full" /\ disk[s].cg \in gs}
    IN /\ disk' = [s \in Segs |-> IF s \in ds THEN Empty ELSE disk[s]]
       /\ free' = free \cup ds

Truncate ==
    /\ nt < MaxTruncates
    /\ nt' = nt + 1
    \* With TruncateFlushes the separator buffers are flushed first; their full
    \* segments are then discarded with the rest, so the net effect is that the
    \* buffers are empty.
    /\ index' = [k \in Keys |-> NoLoc]
    /\ Discard(CGs)
    /\ sepbuf' = IF TruncateFlushes THEN [g \in CGs |-> <<>>] ELSE sepbuf
    /\ expected' = [k \in Keys |-> 0]
    /\ acked' = [k \in Keys |-> {}]
    /\ UNCHANGED <<active, owned, wts, clock, nextSeq, nextTs, nc, nm, nk, crashed>>

\* Tablet cleanup: the group leaves the shard; its index range is erased,
\* its full segments discarded, its separator buffer dropped.
Leave(g) ==
    /\ g \in owned
    /\ nm < MaxMoves
    /\ nm' = nm + 1
    /\ owned' = owned \ {g}
    /\ index' = [k \in Keys |-> IF CG(k) = g THEN NoLoc ELSE index[k]]
    /\ Discard({g})
    /\ sepbuf' = [sepbuf EXCEPT ![g] = <<>>]
    /\ UNCHANGED <<active, expected, wts, acked, clock, nextSeq, nextTs, nc, nt, nk, crashed>>

\* The tablet comes back: streaming writes its current data (expected, as
\* the other replica has it) into a new full segment.
Return(g) ==
    /\ g \notin owned
    /\ owned' = owned \cup {g}
    /\ LET ks == {k \in Keys : CG(k) = g /\ expected[k] # 0} IN
       IF ks = {}
       THEN UNCHANGED <<disk, free, index, nextSeq>>
       ELSE \E t \in free :
            LET k1 == CHOOSE k \in ks : TRUE IN
            /\ WriteFull(g, <<<<k1, expected[k1]>>>>, t)
            /\ free' = free \ {t}
            /\ nextSeq' = nextSeq + 1
            /\ index' = [index EXCEPT ![k1] = <<t, expected[k1]>>]
    /\ UNCHANGED <<active, sepbuf, expected, wts, acked, clock, nextTs, nc, nt, nm, nk, crashed>>

\* Records the recovery scan finds for key k.
Found(k) == UNION {{<<s, disk[s].recs[i][2], i>> :
                       i \in {j \in 1..Len(disk[s].recs) : disk[s].recs[j][1] = k}} :
                   s \in {x \in Segs : disk[x].valid}}

Better(a, b) ==
    \/ TS(a[2]) > TS(b[2])
    \/ TS(a[2]) = TS(b[2]) /\ disk[a[1]].seq > disk[b[1]].seq
    \/ TS(a[2]) = TS(b[2]) /\ a[1] = b[1] /\ a[3] > b[3]

Recovered(k) ==
    IF CG(k) \notin owned \/ Found(k) = {} THEN NoLoc
    ELSE LET a == CHOOSE a \in Found(k) : \A b \in Found(k) : a = b \/ Better(a, b)
         IN <<a[1], a[2]>>

Crash ==
    /\ nc < MaxCrashes
    /\ nc' = nc + 1
    /\ crashed' = TRUE
    /\ LET idx == [k \in Keys |-> Recovered(k)]
           mixedLive(g) == {k \in Keys : CG(k) = g /\ idx[k] # NoLoc /\ disk[idx[k][1]].kind = "mixed"}
       IN
       /\ index' = idx
       \* Recovered mixed segments are separated again.
       /\ sepbuf' = [g \in CGs |->
              IF mixedLive(g) = {} THEN <<>>
              ELSE LET k1 == CHOOSE k \in mixedLive(g) : TRUE
                   IN <<<<k1, idx[k1][2], idx[k1][1]>>>>]
       /\ free' = {s \in Segs : ~disk[s].valid \/ ~\E k \in Keys : idx[k][1] = s}
    /\ active' = Nil
    /\ UNCHANGED <<disk, owned, expected, wts, acked, clock, nextSeq, nextTs, nt, nm, nk>>

Next ==
    \/ AllocActive \/ RetireActive
    \/ \E k \in Keys, tie \in BOOLEAN, v \in Vals : Write(k, tie, v)
    \/ \E g \in CGs : SepFlush(g) \/ Leave(g) \/ Return(g)
    \/ \E s \in Segs : Compact(s) \/ Free(s)
    \/ Truncate
    \/ Crash

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Reads of an owned key return the newest acknowledged write.
W(id) == IF id = 0 THEN <<0, 0>> ELSE wts[id]
Correct == \A k \in Keys : CG(k) \in owned => W(index[k][2]) = W(expected[k])

\* What Scylla's reconciliation keeps of the acknowledged writes of k: the
\* highest timestamp, then the larger value (compare_atomic_cell_for_merge()).
Reconciled(k) ==
    LET top == {w \in acked[k] : \A x \in acked[k] : TS(x) <= TS(w)}
    IN IF top = {} THEN 0 ELSE CHOOSE v \in {wts[w][2] : w \in top} : \A x \in top : wts[x][2] <= v

\* Reads of an owned key return what reconciliation would.
AgreesWithReconciliation == \A k \in Keys : CG(k) \in owned =>
    (IF index[k][1] = Nil THEN 0 ELSE wts[index[k][2]][2]) = Reconciled(k)

\* A segment on the free list holds no live record and no buffered one.
FreeIsFree == \A s \in free : Live(s) = {} /\ Refs(s) = {} /\ s # active

W_CrashAfterTruncate == ~(crashed /\ nt > 0 /\ nextTs > 2)
W_CrashAfterReturn == ~(crashed /\ nm > 0 /\ CGs \subseteq owned /\ nextTs > 2)
=============================================================================
