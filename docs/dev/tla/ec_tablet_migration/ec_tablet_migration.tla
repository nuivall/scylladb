\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

-------------------------- MODULE ec_tablet_migration --------------------------
(***************************************************************************)
(* Migration of one eventually-consistent tablet, with the data of one     *)
(* key: the stages, coordinators writing and reading with the replica      *)
(* sets of the stage they pinned, streaming under topology guards,         *)
(* cleanup, cancellation, and tombstone garbage collection.  See "Tablet   *)
(* transitions", "Cancelling tablet transitions" and "Topology guards" in  *)
(* docs/dev/topology-over-raft.md.                                         *)
(*                                                                         *)
(* Code:                                                                   *)
(*   Coordinator actions   the eventually-consistent stages of             *)
(*       handle_tablet_migration() (service/topology_coordinator.cc),      *)
(*       rollback_cancelled(), tablet_mutation_builder::cancel_transition()*)
(*   WritePin/Deliver/End  storage_proxy::mutate() with the stage's write  *)
(*       selector; acks needed: block_for(CL) + pending (storage_proxy.cc) *)
(*   ReadPin/ReadExec      a read with the stage's read selector           *)
(*   Rpc*                  send_tablet_stream_data, the stream consumer's  *)
(*       topology_guard (streaming/consumer.cc)                            *)
(*   Barrier               global_tablet_token_metadata_barrier: every     *)
(*       host sees the stage, older requests and guards of closed sessions *)
(*       are drained                                                       *)
(*                                                                         *)
(* Abstractions: one key, last-write-wins by timestamp, a delete is a      *)
(* tombstone; RF=2 with WriteCL + ReadCL > RF; hosts don't fail; a tombstone is purged on  *)
(* a replica once every replica has it (repair-based tombstone GC) and no  *)
(* transition runs.  An RPC whose response the coordinator stops waiting   *)
(* for (RpcLost) keeps running on the destination.                         *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Hosts, InitReplicas, NoHost,
    SkipBarrier,        \* ablation: stages whose barrier is dropped
    GuardChecksSession, \* ablation: FALSE = a stream applies whatever the session
    WriteCL, ReadCL,    \* replicas a write and a read need, e.g. ONE and ALL
    MaxMigrations, MaxFailures, MaxWrites, MaxReads, MaxRpcs

Stages == {"allow_write_both_read_old", "write_both_read_old", "streaming",
           "write_both_read_new", "use_new", "cleanup", "end_migration",
           "cleanup_target", "revert_migration"}
Cancellable == {"allow_write_both_read_old", "write_both_read_old", "streaming"}

VARIABLES
    pub,      \* published tablet states, in group 0 order
    vw,       \* vw[h]: index in pub of the state host h applied
    step,     \* coordinator: index of the next step of the current stage
    store,    \* store[h]: cells [ts, del] host h has for the key
    wr,       \* writes: [st, v, ts, del, got]
    rd,       \* reads: [st, v, minTs]
    acked,    \* timestamps of acknowledged writes
    rpcs,     \* stream RPCs: [id, sess, dst, src, st, cells, lost]
    bad,      \* a read returned something older than an acknowledged write
    purged,   \* ghost: a tombstone was purged
    nextTs, nextSess, nMig, nFail

vars == <<pub, vw, step, store, wr, rd, acked, rpcs, bad, purged, nextTs, nextSess,
          nMig, nFail>>

-----------------------------------------------------------------------------
\* The replicas a request with a given view writes to and reads from
\* (tablet_transition_info::writes and ::reads).
WriteOld(r) == r.stage \in {"none", "allow_write_both_read_old", "cleanup_target", "revert_migration"}
WriteBoth(r) == r.stage \in {"write_both_read_old", "streaming", "write_both_read_new"}
WriteSet(r) ==
    IF WriteOld(r) THEN r.C ELSE IF WriteBoth(r) THEN r.C \cup r.next ELSE r.next
ReadSet(r) ==
    IF r.stage \in {"none", "allow_write_both_read_old", "write_both_read_old",
                    "streaming", "cleanup_target", "revert_migration"}
    THEN r.C ELSE r.next
\* Acks a write needs: block_for(CL) of the current replicas, plus every
\* pending replica it is sent to.
AcksNeeded(r) == IF WriteBoth(r) THEN WriteCL + Cardinality(r.next \ r.C) ELSE WriteCL

\* Whether a host's view makes it a replica that takes writes for the tablet:
\* the pending replica allocates its storage on seeing the transition.
Accepts(h, r) ==
    IF r.stage \in {"none", "cleanup_target", "revert_migration"} THEN h \in r.C
    ELSE IF r.stage \in {"cleanup", "end_migration"} THEN h \in r.next
    ELSE h \in r.C \cup r.next

StageSteps(s) ==
    CASE s = "allow_write_both_read_old" -> <<"barrier">>
      [] s = "write_both_read_old"       -> <<"barrier">>
      [] s = "streaming"                 -> <<"stream">>
      [] s = "write_both_read_new"       -> <<"barrier">>
      [] s = "use_new"                   -> <<"barrier">>
      [] s = "cleanup"                   -> <<"cleanup">>
      [] s = "end_migration"             -> <<"barrier">>
      [] s = "cleanup_target"            -> <<"barrier", "cleanup">>
      [] s = "revert_migration"          -> <<"barrier">>
      [] s = "none"                      -> <<>>
Steps(s) == SelectSeq(StageSteps(s), LAMBDA x : ~(x = "barrier" /\ s \in SkipBarrier))

Cur == Len(pub)
CurState == pub[Cur]
View(h) == pub[vw[h]]
StepIs(x) == step <= Len(Steps(CurState.stage)) /\ Steps(CurState.stage)[step] = x

MaxTs(S) == IF S = {} THEN 0 ELSE CHOOSE t \in {c.ts : c \in S} : \A c \in S : c.ts <= t

ActiveWrites == {i \in DOMAIN wr : wr[i].st = "pinned"}
ActiveReads == {i \in DOMAIN rd : rd[i].st = "pinned"}
\* An RPC that holds a topology guard on its destination.
Receiving == {r \in rpcs : r.st = "recv"}

Init ==
    /\ pub = << [mig |-> 0, stage |-> "none", C |-> InitReplicas, next |-> InitReplicas,
                 L |-> NoHost, P |-> NoHost, sess |-> 0, cancelled |-> FALSE] >>
    /\ vw = [h \in Hosts |-> 1]
    /\ step = 1
    /\ store = [h \in Hosts |-> {}]
    /\ wr = [i \in 1..MaxWrites |-> [st |-> "unused", v |-> 0, ts |-> 0, del |-> FALSE, got |-> {}]]
    /\ rd = [i \in 1..MaxReads |-> [st |-> "unused", v |-> 0, minTs |-> 0]]
    /\ acked = {}
    /\ rpcs = {}
    /\ bad = FALSE /\ purged = FALSE
    /\ nextTs = 1 /\ nextSess = 1 /\ nMig = 0 /\ nFail = 0

Publish(r) == pub' = Append(pub, r) /\ step' = 1
DoneStep == step' = step + 1
DataVars == <<store, wr, rd, acked, rpcs, bad, purged, nextTs>>

-----------------------------------------------------------------------------
\* Topology coordinator

StartMigration ==
    /\ CurState.stage = "none"
    /\ nMig < MaxMigrations
    /\ \E l \in CurState.C, p \in Hosts \ CurState.C :
        Publish([CurState EXCEPT !.mig = nMig + 1, !.stage = "allow_write_both_read_old",
                 !.next = (CurState.C \ {l}) \cup {p}, !.L = l, !.P = p])
    /\ nMig' = nMig + 1
    /\ UNCHANGED <<vw, DataVars, nextSess, nFail>>

Advance ==
    /\ CurState.stage \notin {"none", "streaming"}
    \* The flag stops only the cancellable stages; it goes with the transition.
    /\ ~(CurState.cancelled /\ CurState.stage \in Cancellable)
    /\ step > Len(Steps(CurState.stage))
    /\ LET s == CurState.stage IN
       CASE s = "allow_write_both_read_old" ->
                Publish([CurState EXCEPT !.stage = "write_both_read_old", !.sess = nextSess])
         [] s = "write_both_read_old" -> Publish([CurState EXCEPT !.stage = "streaming"])
         [] s = "write_both_read_new" -> Publish([CurState EXCEPT !.stage = "use_new"])
         [] s = "use_new"             -> Publish([CurState EXCEPT !.stage = "cleanup"])
         [] s = "cleanup"             -> Publish([CurState EXCEPT !.stage = "end_migration"])
         [] s = "end_migration"       ->
                Publish([CurState EXCEPT !.stage = "none", !.C = CurState.next,
                         !.L = NoHost, !.P = NoHost])
         [] s = "cleanup_target"      -> Publish([CurState EXCEPT !.stage = "revert_migration"])
         [] s = "revert_migration"    ->
                Publish([CurState EXCEPT !.stage = "none", !.next = CurState.C,
                         !.L = NoHost, !.P = NoHost, !.cancelled = FALSE])
    /\ nextSess' = IF CurState.stage = "allow_write_both_read_old" THEN nextSess + 1 ELSE nextSess
    /\ UNCHANGED <<vw, DataVars, nMig, nFail>>

\* Global token metadata barrier: every host sees the stage, no request of an
\* older view runs, and the guards of sessions that are no longer open are
\* released.
Barrier ==
    /\ StepIs("barrier")
    /\ \A h \in Hosts : vw[h] = Cur
    /\ \A i \in ActiveWrites : wr[i].v = Cur
    /\ \A i \in ActiveReads : rd[i].v = Cur
    /\ \A r \in Receiving : r.sess = CurState.sess
    /\ DoneStep
    /\ UNCHANGED <<pub, vw, DataVars, nextSess, nMig, nFail>>

\* The coordinator waits for the RPCs it sent and hasn't given up on.
Outstanding == {r \in rpcs : r.sess = CurState.sess /\ ~r.lost /\ r.st \in {"sent", "recv"}}

StreamSend ==
    /\ StepIs("stream") /\ ~CurState.cancelled
    /\ Outstanding = {}
    /\ Cardinality(rpcs) < MaxRpcs
    /\ rpcs' = rpcs \cup {[id |-> Cardinality(rpcs) + 1, sess |-> CurState.sess,
                           dst |-> CurState.P, src |-> CurState.L,
                           st |-> "sent", cells |-> {}, lost |-> FALSE]}
    /\ UNCHANGED <<pub, vw, step, store, wr, rd, acked, bad, purged, nextTs, nextSess, nMig, nFail>>

\* Streaming succeeded: the session is deleted with no barrier.
StreamComplete ==
    /\ StepIs("stream") /\ ~CurState.cancelled
    /\ \E r \in rpcs : r.sess = CurState.sess /\ r.st = "done" /\ ~r.lost
    /\ Publish([CurState EXCEPT !.stage = "write_both_read_new", !.sess = 0])
    /\ UNCHANGED <<vw, DataVars, nextSess, nMig, nFail>>

\* Streaming failed (the RPC failed, or the coordinator gave up on it), and
\* the coordinator rolls back.  Otherwise it retries with StreamSend.
StreamFail ==
    /\ StepIs("stream") /\ ~CurState.cancelled
    /\ nFail < MaxFailures
    /\ Outstanding = {}
    /\ \E r \in rpcs : r.sess = CurState.sess /\ (r.lost \/ r.st = "failed")
    /\ Publish([CurState EXCEPT !.stage = "cleanup_target", !.sess = 0])
    /\ nFail' = nFail + 1
    /\ UNCHANGED <<vw, DataVars, nextSess, nMig>>

\* cancel_transition(): clears the session and marks the transition.
Cancel ==
    /\ nFail < MaxFailures
    /\ CurState.stage \in Cancellable /\ ~CurState.cancelled
    /\ pub' = Append(pub, [CurState EXCEPT !.cancelled = TRUE, !.sess = 0])
    /\ nFail' = nFail + 1
    /\ UNCHANGED <<vw, step, DataVars, nextSess, nMig>>

\* rollback_cancelled(): once the stage's in-flight action is done.
RollbackCancelled ==
    /\ CurState.cancelled /\ CurState.stage \in Cancellable
    /\ \A r \in rpcs : r.st \in {"sent", "recv"} => r.lost
    /\ Publish([CurState EXCEPT !.stage = "cleanup_target", !.sess = 0])
    /\ UNCHANGED <<vw, DataVars, nextSess, nMig, nFail>>

CleanupTarget(r) == IF r.stage = "cleanup" THEN r.L ELSE r.P

\* The cleanup RPC: checks the stage on the target, then removes its data.
Cleanup ==
    /\ StepIs("cleanup")
    /\ LET h == CleanupTarget(CurState) IN
        /\ vw[h] = Cur
        /\ store' = [store EXCEPT ![h] = {}]
    /\ DoneStep
    /\ UNCHANGED <<pub, vw, wr, rd, acked, rpcs, bad, purged, nextTs, nextSess, nMig, nFail>>

-----------------------------------------------------------------------------
\* Replicas

ApplyGroup0(h) ==
    /\ vw[h] < Cur
    /\ \E j \in (vw[h] + 1)..Cur : vw' = [vw EXCEPT ![h] = j]
    /\ UNCHANGED <<pub, step, DataVars, nextSess, nMig, nFail>>

\* The stream consumer on the pending replica materializes the topology
\* guard, which fails unless the RPC's session is open in the state it sees,
\* and reads the leaving replica's data.
RpcRecv(r) ==
    /\ r.st = "sent"
    /\ LET open == View(r.dst).sess = r.sess
       IN rpcs' = (rpcs \ {r}) \cup
            {IF open \/ ~GuardChecksSession
             THEN [r EXCEPT !.st = "recv", !.cells = store[r.src]]
             ELSE [r EXCEPT !.st = "failed"]}
    /\ UNCHANGED <<pub, vw, step, store, wr, rd, acked, bad, purged, nextTs, nextSess, nMig, nFail>>

\* The consumer writes the streamed data, still under the guard, and the
\* guard is released.
RpcApply(r) ==
    /\ r.st = "recv"
    /\ store' = [store EXCEPT ![r.dst] = @ \cup r.cells]
    /\ rpcs' = (rpcs \ {r}) \cup {[r EXCEPT !.st = "done"]}
    /\ UNCHANGED <<pub, vw, step, wr, rd, acked, bad, purged, nextTs, nextSess, nMig, nFail>>

\* The coordinator stops waiting for the RPC (e.g. the connection broke);
\* the destination may still run it.
RpcLost(r) ==
    /\ r.st \in {"sent", "recv"} /\ ~r.lost
    /\ rpcs' = (rpcs \ {r}) \cup {[r EXCEPT !.lost = TRUE]}
    /\ UNCHANGED <<pub, vw, step, store, wr, rd, acked, bad, purged, nextTs, nextSess, nMig, nFail>>

\* Repair-based tombstone GC on host h, outside transitions: a tombstone every
\* replica has is purged, with what it shadows.
PurgeTombstone(h) ==
    /\ CurState.stage = "none" /\ vw[h] = Cur /\ h \in CurState.C
    /\ \E t \in store[h] :
        /\ t.del
        /\ \A h2 \in CurState.C : \E c \in store[h2] : c.ts >= t.ts
        /\ store' = [store EXCEPT ![h] = {c \in @ : c.ts > t.ts}]
    /\ purged' = TRUE
    /\ UNCHANGED <<pub, vw, step, wr, rd, acked, rpcs, bad, nextTs, nextSess, nMig, nFail>>

-----------------------------------------------------------------------------
\* Coordinators of client requests

WritePin(i, h, del) ==
    /\ wr[i].st = "unused"
    /\ wr' = [wr EXCEPT ![i] = [st |-> "pinned", v |-> vw[h], ts |-> nextTs, del |-> del, got |-> {}]]
    /\ nextTs' = nextTs + 1
    /\ UNCHANGED <<pub, vw, step, store, rd, acked, rpcs, bad, purged, nextSess, nMig, nFail>>

\* A replica of the write's view receives it, and stores it if its own view
\* makes it take writes for the tablet.
Deliver(i, h) ==
    /\ wr[i].st = "pinned"
    /\ h \in WriteSet(pub[wr[i].v]) \ wr[i].got
    /\ Accepts(h, View(h))
    /\ store' = [store EXCEPT ![h] = @ \cup {[ts |-> wr[i].ts, del |-> wr[i].del]}]
    /\ wr' = [wr EXCEPT ![i].got = @ \cup {h}]
    /\ acked' = IF Cardinality(wr'[i].got) >= AcksNeeded(pub[wr[i].v])
                THEN acked \cup {wr[i].ts} ELSE acked
    /\ UNCHANGED <<pub, vw, step, rd, rpcs, bad, purged, nextTs, nextSess, nMig, nFail>>

\* The write handler is done (all replied, or it timed out).
WriteEnd(i) ==
    /\ wr[i].st = "pinned"
    /\ wr' = [wr EXCEPT ![i].st = "done"]
    /\ UNCHANGED <<pub, vw, step, store, rd, acked, rpcs, bad, purged, nextTs, nextSess, nMig, nFail>>

ReadPin(i, h) ==
    /\ rd[i].st = "unused"
    /\ rd' = [rd EXCEPT ![i] = [st |-> "pinned", v |-> vw[h],
                                minTs |-> IF acked = {} THEN 0 ELSE MaxTs({[ts |-> t] : t \in acked})]]
    /\ UNCHANGED <<pub, vw, step, store, wr, acked, rpcs, bad, purged, nextTs, nextSess, nMig, nFail>>

\* A read of ReadCL of the view's read replicas returns the newest cell.
ReadExec(i) ==
    /\ rd[i].st = "pinned"
    /\ \E S \in SUBSET ReadSet(pub[rd[i].v]) :
        /\ Cardinality(S) = ReadCL
        /\ LET res == MaxTs(UNION {store[h] : h \in S}) IN
           bad' = (bad \/ res < rd[i].minTs)
    /\ rd' = [rd EXCEPT ![i].st = "done"]
    /\ UNCHANGED <<pub, vw, step, store, wr, acked, rpcs, purged, nextTs, nextSess, nMig, nFail>>

-----------------------------------------------------------------------------
Next ==
    \/ StartMigration \/ Advance \/ Barrier \/ StreamSend \/ StreamComplete
    \/ StreamFail \/ Cancel \/ RollbackCancelled \/ Cleanup
    \/ \E h \in Hosts : ApplyGroup0(h) \/ PurgeTombstone(h)
    \/ \E r \in rpcs : RpcRecv(r) \/ RpcApply(r) \/ RpcLost(r)
    \/ \E i \in 1..MaxWrites, h \in Hosts, d \in BOOLEAN : WritePin(i, h, d)
    \/ \E i \in 1..MaxWrites : WriteEnd(i) \/ \E h \in Hosts : Deliver(i, h)
    \/ \E i \in 1..MaxReads : ReadExec(i) \/ \E h \in Hosts : ReadPin(i, h)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* No acknowledged write is lost, and no deleted data comes back: a read
\* returns a cell at least as new as every write acknowledged before it
\* started.
ReadsSeeAckedWrites == ~bad

\* INV-TABL-1: outside a transition, only the tablet's replicas hold its data.
StorageOnReplicasOnly ==
    CurState.stage = "none" => \A h \in Hosts \ CurState.C : store[h] = {}

\* Witnesses (expected to be violated, to show the paths are reachable).
W_MigrationEnded == ~(\E k \in 1..Len(pub) : pub[k].stage = "end_migration") \/ CurState.stage # "none"
W_CancelRolledBack == ~(\E k \in 1..Len(pub) : pub[k].cancelled) \/ CurState.stage # "none"
W_PurgedThenMigrated == ~(purged /\ nMig = MaxMigrations /\ CurState.stage = "none")

-----------------------------------------------------------------------------
\* State abstraction for TLC (VIEW), to keep fewer and smaller states.  Only
\* the order of timestamps and of session ids matters, so they are replaced
\* by their ranks; published states no host or request can see any more are
\* dropped and the rest renumbered; RPCs are kept as a bag without their
\* ids, and the finished ones of other sessions, which nothing reads, only
\* count towards MaxRpcs.  This changes no behavior.  Configurations whose
\* invariants read the history (the witnesses) don't use it.
TsVals == {c.ts : c \in UNION {store[h] : h \in Hosts} \cup UNION {r.cells : r \in rpcs}}
          \cup {wr[i].ts : i \in DOMAIN wr} \cup acked \cup {rd[i].minTs : i \in DOMAIN rd}
TsRk(t) == IF t = 0 THEN 0 ELSE Cardinality({u \in TsVals : 0 < u /\ u <= t})
CellsRk(S) == {[c EXCEPT !.ts = TsRk(@)] : c \in S}
LiveIdx == {vw[h] : h \in Hosts} \cup {wr[i].v : i \in ActiveWrites} \cup {rd[i].v : i \in ActiveReads} \cup {Cur}
Lo == CHOOSE i \in LiveIdx : \A j \in LiveIdx : i <= j
Sh(i) == i - Lo + 1
Kept == SubSeq(pub, Lo, Len(pub))
SessVals == {Kept[k].sess : k \in DOMAIN Kept} \cup {r.sess : r \in rpcs}
SessRk(x) == IF x = 0 THEN 0 ELSE Cardinality({u \in SessVals : 0 < u /\ u <= x})
LiveRpc(r) == r.st \in {"sent", "recv"} \/ r.sess = CurState.sess
RpcView(r) == [sess |-> SessRk(r.sess), dst |-> r.dst, src |-> r.src, st |-> r.st,
               cells |-> CellsRk(r.cells), lost |-> r.lost]
\* (Comparing each variable with itself makes TLC materialize lazily built
\* functions in the state, which it otherwise fails to write to disk when
\* fingerprints come from a view.)
Materialized == pub = pub /\ vw = vw /\ store = store /\ wr = wr /\ rd = rd /\ rpcs = rpcs
StateView ==
    <<Materialized, [k \in DOMAIN Kept |-> [Kept[k] EXCEPT !.sess = SessRk(@)]],
      [h \in Hosts |-> Sh(vw[h])], step,
      [h \in Hosts |-> CellsRk(store[h])],
      [i \in DOMAIN wr |-> IF wr[i].st = "pinned" THEN [wr[i] EXCEPT !.v = Sh(@), !.ts = TsRk(@)]
                          ELSE [st |-> wr[i].st]],
      [i \in DOMAIN rd |-> IF rd[i].st = "pinned" THEN [rd[i] EXCEPT !.v = Sh(@), !.minTs = TsRk(@)]
                          ELSE [st |-> rd[i].st]],
      {TsRk(t) : t \in acked},
      {<<RpcView(r), Cardinality({q \in rpcs : LiveRpc(q) /\ RpcView(q) = RpcView(r)})>> : r \in {q \in rpcs : LiveRpc(q)}},
      Cardinality(rpcs), bad, nMig, nFail>>

Symm == Permutations(InitReplicas)
=============================================================================
