\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

--------------------------- MODULE sc_raft_commitlog ---------------------------
(***************************************************************************)
(* The raft log of one strongly consistent tablet group on one replica,   *)
(* kept in the commitlog ("Commit log based persistence for Raft log      *)
(* entries" in docs/dev/strong_consistency.md), across crashes.           *)
(*                                                                         *)
(* The rest of the cluster is an adversary that follows raft: it sends    *)
(* entries, truncating this replica's uncommitted tail where their term   *)
(* differs, and commits a prefix of this replica's log.  An entry is      *)
(* <<index, term>>.                                                        *)
(*                                                                         *)
(* Disk: commitlog segments, each a sequence of batches [commit, from,    *)
(* terms]; the system.raft_groups row [floor (snapshot_idx), truncs];     *)
(* the tablet's flushed data.  Memory: the raft log, commit and apply     *)
(* indexes, the tablet memtable, the raft_groups memtable, and the        *)
(* truncation records (raft_commitlog::_truncations).                     *)
(*                                                                         *)
(* truncate_log() adds a record [seg, from, to] per segment record it     *)
(* trims, in memory.  A release (front record: committed, applied, a      *)
(* later segment exists) writes the descriptor -- floor = max_index, and  *)
(* the in-memory truncation records less those of segments older than    *)
(* the oldest one left -- into the raft_groups memtable.  A segment is    *)
(* deleted once nothing of it is left in a memtable.                      *)
(*                                                                         *)
(* Replay (raft_commitlog_replay_buffer), batch by batch in segment       *)
(* order: floor := max(floor, commit), apply the buffer at or below it;   *)
(* each record of the batch's segment claims, as a cursor, the next copy  *)
(* of its range; the first surviving copy, if its term differs from the   *)
(* buffered one, drops the buffer from it, recording per segment what it  *)
(* dropped; copies at or below the floor are applied, the rest buffered.  *)
(* finish_replay() writes the row, then rewrites the tail into a new      *)
(* segment; a crash can fall between the two.                             *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS MaxIdx, MaxTerm, MaxSegs, MaxCrashes, MaxSteps

VARIABLES
    log,        \* the raft log in memory: log[i] = term (0: below the snapshot, not held)
    commit, applied,
    segs,       \* segs[s]: batches [commit, from, terms]
    active, nextSeg,
    qrecs,      \* the group's segment records: segments holding its entries, oldest first
    mtruncs,    \* truncation records in memory: [seg, from, to, id]
    row,        \* durable row: [floor, truncs]
    rgMem,      \* raft_groups memtable: a row not yet flushed, or [floor |-> -1]
    tmem,       \* tablet memtable: <<i, t, seg>>
    tdisk,      \* tablet data on disk: <<i, t>>
    down,       \* crashed between finish_replay()'s row write and its tail rewrite
    committed,  \* ghost: entries the cluster committed
    nextId, nc, steps

vars == <<log, commit, applied, segs, active, nextSeg, qrecs, mtruncs, row, rgMem, tmem, tdisk, down,
          committed, nextId, nc, steps>>

Terms == 1..MaxTerm
NoRow == [floor |-> -1, truncs |-> {}]

Init ==
    /\ log = <<>> /\ commit = 0 /\ applied = 0
    /\ segs = (1 :> <<>>) /\ active = 1 /\ nextSeg = 2
    /\ qrecs = <<>> /\ mtruncs = {}
    /\ row = [floor |-> 0, truncs |-> {}]
    /\ rgMem = NoRow
    /\ tmem = {} /\ tdisk = {}
    /\ down = FALSE
    /\ committed = {}
    /\ nextId = 1 /\ nc = 0 /\ steps = 0

Up == ~down /\ steps < MaxSteps /\ steps' = steps + 1

\* Indexes of this group in segment s.
Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A x \in S : x <= m
Min(S) == CHOOSE m \in S : \A x \in S : m <= x

\* qrecs[k] = [seg, first, max]
Receive(i, n, t) ==
    LET newTail == [j \in 1..n |-> t]
        last == i + n - 1
        \* raft truncates at the first index whose term differs, if any
        cs == {j \in i..last : j <= Len(log) /\ log[j] # t}
        conflict == cs # {}
        ci == IF conflict THEN Min(cs) ELSE 0
        nl == IF conflict THEN SubSeq(log, 1, i - 1) \o newTail
              ELSE [j \in 1..Max({Len(log), last}) |-> IF j >= i /\ j <= last THEN t ELSE log[j]]
        \* truncate_log(ci): records fully at or above ci go, the last one is trimmed
        cut == {k \in 1..Len(qrecs) : qrecs[k].max >= ci}
        newRecs == {[seg |-> qrecs[k].seg, from |-> IF qrecs[k].first >= ci THEN qrecs[k].first ELSE ci,
                     to |-> qrecs[k].max] : k \in cut}
        kept == SelectSeq(qrecs, LAMBDA r : r.first < ci)
        trimmed == [k \in 1..Len(kept) |-> IF kept[k].max >= ci THEN [kept[k] EXCEPT !.max = ci - 1] ELSE kept[k]]
        q1 == IF conflict THEN trimmed ELSE qrecs
        w0 == IF conflict THEN ci ELSE Len(log) + 1
        q2 == IF Len(q1) > 0 /\ q1[Len(q1)].seg = active
              THEN [q1 EXCEPT ![Len(q1)].max = IF last > @ THEN last ELSE @]
              ELSE Append(q1, [seg |-> active, first |-> w0, max |-> last])
    IN
    /\ Up
    /\ n >= 1 /\ i >= 1 /\ i <= Len(log) + 1 /\ i > commit /\ last <= MaxIdx
    /\ i > 1 => log[i - 1] <= t
    /\ \A j \in 1..Len(log) : log[j] <= t
    /\ \A j \in i..last : \A e \in committed : e[1] = j => e[2] = t
    /\ log' = nl
    /\ mtruncs' = IF conflict
                  THEN mtruncs \cup {[seg |-> y.seg, from |-> y.from, to |-> y.to, id |-> nextId * 10 + y.seg] : y \in newRecs}
                  ELSE mtruncs
    /\ nextId' = nextId + 1
    /\ qrecs' = q2
    \* log::maybe_append() skips the entries it already holds in the same term:
    \* only the conflicting or new ones are stored.
    /\ LET w == IF conflict THEN ci ELSE Len(log) + 1 IN
       /\ w <= last
       /\ segs' = [segs EXCEPT ![active] = Append(@, [commit |-> commit, from |-> w, terms |-> [j \in 1..(last - w + 1) |-> t]])]
    /\ UNCHANGED <<commit, applied, active, nextSeg, row, rgMem, tmem, tdisk, down, committed, nc>>

Commit(c) ==
    /\ Up
    /\ c > commit /\ c <= Len(log)
    /\ \A i \in 1..c : log[i] # 0
    /\ commit' = c
    /\ committed' = committed \cup {<<i, log[i]>> : i \in (commit + 1)..c}
    /\ UNCHANGED <<log, applied, segs, active, nextSeg, qrecs, mtruncs, row, rgMem, tmem, tdisk, down, nextId, nc>>

SegOf(i) == LET ks == {k \in 1..Len(qrecs) : qrecs[k].first <= i /\ i <= qrecs[k].max}
            IN IF ks = {} THEN 0 ELSE qrecs[Min(ks)].seg

Apply ==
    /\ Up
    /\ applied < commit
    /\ applied' = applied + 1
    /\ tmem' = tmem \cup {<<applied + 1, log[applied + 1], SegOf(applied + 1)>>}
    /\ UNCHANGED <<log, commit, segs, active, nextSeg, qrecs, mtruncs, row, rgMem, tdisk, down, committed, nextId, nc>>

FlushTablet ==
    /\ Up
    /\ tmem # {}
    /\ tdisk' = tdisk \cup {<<e[1], e[2]>> : e \in tmem}
    /\ tmem' = {}
    /\ UNCHANGED <<log, commit, applied, segs, active, nextSeg, qrecs, mtruncs, row, rgMem, down, committed, nextId, nc>>

NewSegment ==
    /\ Up
    /\ nextSeg <= MaxSegs
    /\ segs' = segs @@ (nextSeg :> <<>>)
    /\ active' = nextSeg
    /\ nextSeg' = nextSeg + 1
    /\ UNCHANGED <<log, commit, applied, qrecs, mtruncs, row, rgMem, tmem, tdisk, down, committed, nextId, nc>>

Release ==
    LET r == qrecs[1]
        oldest == Min(DOMAIN segs)
        live == {x \in mtruncs : x.seg >= oldest}
    IN
    /\ Up
    /\ Len(qrecs) > 0
    /\ r.max <= commit /\ r.max <= applied
    /\ Len(qrecs) > 1 \/ r.seg # active
    /\ mtruncs' = live
    /\ rgMem' = [floor |-> r.max, truncs |-> live]
    /\ qrecs' = Tail(qrecs)
    /\ UNCHANGED <<log, commit, applied, segs, active, nextSeg, row, tmem, tdisk, down, committed, nextId, nc>>

FlushRaftGroups ==
    /\ Up
    /\ rgMem # NoRow
    /\ row' = [floor |-> IF rgMem.floor > row.floor THEN rgMem.floor ELSE row.floor, truncs |-> rgMem.truncs]
    /\ rgMem' = NoRow
    /\ UNCHANGED <<log, commit, applied, segs, active, nextSeg, qrecs, mtruncs, tmem, tdisk, down, committed, nextId, nc>>

\* The commitlog deletes a segment nothing references: no record of the
\* group, no unflushed descriptor, no unflushed applied entry from it.
Delete(s) ==
    /\ Up
    /\ s \in DOMAIN segs /\ s # active
    /\ ~\E k \in 1..Len(qrecs) : qrecs[k].seg = s
    /\ rgMem = NoRow
    /\ ~\E e \in tmem : e[3] = s
    /\ segs' = [x \in DOMAIN segs \ {s} |-> segs[x]]
    /\ UNCHANGED <<log, commit, applied, active, nextSeg, qrecs, mtruncs, row, rgMem, tmem, tdisk, down, committed, nextId, nc>>

-----------------------------------------------------------------------------
(* Replay *)

Order == LET D == DOMAIN segs IN
         [k \in 1..Cardinality(D) |-> CHOOSE s \in D : Cardinality({x \in D : x < s}) = k - 1]

Batches == LET so == Order IN
    LET F[k \in 0..Len(so)] == IF k = 0 THEN <<>> ELSE
            F[k - 1] \o [b \in 1..Len(segs[so[k]]) |-> [seg |-> so[k], commit |-> segs[so[k]][b].commit,
                                                       from |-> segs[so[k]][b].from, terms |-> segs[so[k]][b].terms]]
    IN F[Len(so)]

\* st: [floor, buf (seq of <<i, t, seg>>, ascending index), apply, truncs,
\* cursor (record id -> next index), nid]
\* One batch, as add_batch() takes it.
Batch(st, b) ==
    LET fl == IF b.commit > st.floor THEN b.commit ELSE st.floor
        \* drain what the new floor covers
        dr == {x \in 1..Len(st.buf) : st.buf[x][1] <= fl}
        buf0 == SelectSeq(st.buf, LAMBDA e : e[1] > fl)
        app0 == st.apply \cup {<<st.buf[x][1], st.buf[x][2]>> : x \in dr}
        \* drop_truncated_copies(): each copy is claimed by the oldest cursor of
        \* this segment waiting for its index
        RECURSIVE Claim(_, _, _)
        Claim(cur, j, keep) ==
            IF j > Len(b.terms) THEN <<cur, keep>> ELSE
            LET i == b.from + j - 1
                w == {r \in st.truncs : r.seg = b.seg /\ cur[r.id] = i /\ i <= r.to}
            IN IF w # {}
               THEN LET r == CHOOSE x \in w : \A y \in w : x.id <= y.id
                    IN Claim([cur EXCEPT ![r.id] = i + 1], j + 1, keep)
               ELSE Claim(cur, j + 1, Append(keep, <<i, b.terms[j]>>))
        cl == Claim(st.cursor, 1, <<>>)
        rem == cl[2]
        \* superseded_by() on the head of what is left
        first == rem[1]
        cnt == IF rem = <<>> \/ buf0 = <<>> THEN 0
               ELSE IF first[1] < buf0[1][1] THEN (IF first[1] > b.commit THEN Len(buf0) ELSE 0)
               ELSE LET c == Cardinality({x \in 1..Len(buf0) : buf0[x][1] >= first[1]}) IN
                    IF c = 0 THEN 0
                    ELSE IF buf0[Len(buf0) - c + 1][2] = first[2] THEN 0 ELSE c
        popped == SubSeq(buf0, Len(buf0) - cnt + 1, Len(buf0))
        buf1 == SubSeq(buf0, 1, Len(buf0) - cnt)
        psegs == {popped[x][3] : x \in 1..Len(popped)}
        idxOf(d) == {popped[x][1] : x \in {y \in 1..Len(popped) : popped[y][3] = d}}
        recs == {[seg |-> d, from |-> Min(idxOf(d)), to |-> Max(idxOf(d)), id |-> 100 + st.nid * 10 + d] : d \in psegs}
        \* place what is left: at or below the floor apply, buffered at the same
        \* index keep the buffered copy, else append
        RECURSIVE Place(_, _, _)
        Place(bf, ap, j) ==
            IF j > Len(rem) THEN <<bf, ap>> ELSE
            LET e == rem[j] IN
            IF e[1] <= fl THEN Place(bf, ap \cup {e}, j + 1)
            ELSE IF \E x \in 1..Len(bf) : bf[x][1] = e[1] THEN Place(bf, ap, j + 1)
            ELSE Place(Append(bf, <<e[1], e[2], b.seg>>), ap, j + 1)
        pl == Place(buf1, app0, 1)
    IN [st EXCEPT !.floor = fl, !.buf = pl[1], !.apply = pl[2],
                  !.truncs = @ \cup recs,
                  !.cursor = [x \in DOMAIN cl[1] \cup {r.id : r \in recs} |->
                                IF x \in DOMAIN cl[1] THEN cl[1][x]
                                ELSE (CHOOSE r \in recs : r.id = x).to + 1],
                  !.nid = @ + 1]

RECURSIVE Run(_, _)
Run(k, st) == IF k > Len(Batches) THEN st ELSE Run(k + 1, Batch(st, Batches[k]))

Replayed == Run(1, [floor |-> row.floor, buf |-> <<>>, apply |-> {},
                    truncs |-> row.truncs,
                    cursor |-> [x \in {r.id : r \in row.truncs} |-> (CHOOSE r \in row.truncs : r.id = x).from],
                    nid |-> nextId])

\* A crash, then startup: the replay, finish_replay() (the row, then the
\* tail rewritten into a new segment), the memtable flush, the deletion of
\* the replayed segments.  `at` is where the next crash falls: "row" after
\* the row, "tail" after the rewrite, "done" not before the startup ends.
Crash(at) ==
    LET r == Replayed
        \* the rewrite writes the buffered tail; replay refuses a gap under it
        top == IF r.buf = <<>> THEN r.floor ELSE r.buf[Len(r.buf)][1]
        tailSeg == IF Len(r.buf) = 0 THEN <<>>
                   ELSE <<[commit |-> r.floor, from |-> r.buf[1][1], terms |-> [x \in 1..Len(r.buf) |-> r.buf[x][2]]]>>
    IN
    /\ nc < MaxCrashes
    /\ nc' = nc + 1
    /\ row' = [floor |-> r.floor, truncs |-> r.truncs]
    /\ nextId' = r.nid + 1
    /\ tmem' = {}
    /\ rgMem' = NoRow
    /\ CASE at = "row" ->
            /\ down' = TRUE
            /\ tdisk' = tdisk
            /\ UNCHANGED <<log, commit, applied, segs, active, nextSeg, qrecs, mtruncs>>
         [] at = "tail" ->
            /\ down' = TRUE
            /\ tdisk' = tdisk
            /\ segs' = segs @@ (nextSeg :> tailSeg)
            /\ nextSeg' = nextSeg + 1
            /\ UNCHANGED <<log, commit, applied, active, qrecs, mtruncs>>
         [] at = "done" ->
            /\ down' = FALSE
            /\ tdisk' = tdisk \cup r.apply
            /\ log' = [i \in 1..top |-> IF \E x \in 1..Len(r.buf) : r.buf[x][1] = i
                                          THEN r.buf[CHOOSE x \in 1..Len(r.buf) : r.buf[x][1] = i][2] ELSE 0]
            /\ commit' = r.floor
            /\ applied' = r.floor
            /\ mtruncs' = r.truncs
            /\ segs' = (nextSeg :> tailSeg)
            /\ qrecs' = IF Len(r.buf) = 0 THEN <<>> ELSE <<[seg |-> nextSeg, first |-> r.buf[1][1], max |-> top]>>
            /\ active' = nextSeg
            /\ nextSeg' = nextSeg + 1
    /\ UNCHANGED <<committed, steps>>

Next ==
    \/ \E i \in 1..MaxIdx, n \in 1..MaxIdx, t \in Terms : Receive(i, n, t)
    \/ \E c \in 1..MaxIdx : Commit(c)
    \/ Apply \/ FlushTablet \/ NewSegment \/ Release \/ FlushRaftGroups
    \/ \E s \in 1..(MaxSegs + MaxCrashes) : Delete(s)
    \/ \E at \in {"row", "tail", "done"} : Crash(at)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Nothing applied that the cluster didn't commit.
AppliedCommitted == \A e \in tdisk \cup {<<x[1], x[2]>> : x \in tmem} : e \in committed

\* Every entry up to the apply index is in the tablet's data, across replays.
NoLostCommit == ~down => \A i \in 1..applied : \E e \in tdisk \cup {<<x[1], x[2]>> : x \in tmem} : e[1] = i

W_ReplayedTail == ~(nc > 0 /\ ~down /\ Len(log) > commit /\ commit > 0)
W_Superseded == ~(nc > 0 /\ \E r \in row.truncs : r.id >= 100)

\* The replay's tail has no gap (finish_replay() refuses otherwise).
TailContiguous == LET r == Replayed IN \A x \in 1..Len(r.buf) : r.buf[x][1] = r.floor + x
=============================================================================
