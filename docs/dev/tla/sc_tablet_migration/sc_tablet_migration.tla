\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

------------------------- MODULE sc_tablet_migration -------------------------
(***************************************************************************)
(* Migration of one strongly-consistent tablet: the stages, the raft       *)
(* group configuration each stage's sync drives the group to, and the      *)
(* requests, servers and storage on every replica.  The protocol and its   *)
(* invariants are described in "Strongly-consistent tablets" in            *)
(* docs/dev/topology-over-raft.md; this spec checks that the barriers and  *)
(* syncs listed there establish the invariants.                            *)
(*                                                                         *)
(* Code:                                                                   *)
(*   Serving(), Hosts(), Expected()  serving_replicas()                    *)
(*       (strong_consistency/coordinator.cc), hosts_raft_group() and       *)
(*       expected_raft_config() (strong_consistency/groups_manager.cc)     *)
(*   Coordinator actions   the strongly-consistent stages of               *)
(*       handle_tablet_migration() in service/topology_coordinator.cc      *)
(*   Sync*                 groups_manager::handle_migration_rpc() and      *)
(*       sync_raft_group_config()                                          *)
(*   Cleanup*              the tablet cleanup RPC                          *)
(*   Pin/Enter/Leave       coordinator::create_operation_ctx(),            *)
(*       acquire_server(), mutate()/query()                                *)
(*                                                                         *)
(* Abstractions:                                                           *)
(*   - group 0 is a sequence of published tablet states, `pub`; every      *)
(*     host applies a prefix of it, and may skip states.                   *)
(*   - Raft of the tablet's group is one committed configuration, `cfg`,   *)
(*     changed atomically by its leader (joint configurations are not      *)
(*     modelled).  Hosts learn the leader late (`known`).  A demoted       *)
(*     replica never wins an election (see "A demoted replica can lead     *)
(*     for one commit" for the exception the code tolerates).              *)
(*   - A global barrier is a guard: every host applied the stage, and no   *)
(*     request or stage-scoped RPC with an older view runs.                *)
(*   - No intra-node migration, node restarts or dead nodes.               *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Hosts,              \* e.g. {h1, h2, h3}
    InitReplicas,       \* replica set before the first migration
    NoHost,
    \* Ablations, all empty or TRUE in the code:
    SkipBarrier,        \* stages whose barrier is dropped
    ExtraBarrier,       \* stages given a barrier before their other steps
    GuardAbortsSync,    \* a stage change on the host aborts its sync handler
    NewerSyncSupersedes,\* a newer sync handler aborts the older one on start
    \* The coordinator rolls back from sc_become_voter only when the pending
    \* replica is excluded.  Is it then dead, or alive (an excluded node that
    \* still runs, e.g. one partitioned from the coordinator only)?
    ExcludedPendingAlive,
    MaxMigrations, MaxFailures, MaxElections, MaxRequests, MaxCleanupRpcs

FailableToRemove == {"start_migration", "sc_add_nonvoter", "sc_snapshot_transfer"}

VARIABLES
    pub,        \* published tablet states, in group 0 order
    vw,         \* vw[h]: index in pub of the state host h applied
    cfg,        \* raft group configuration: [v |-> voters, nv |-> non-voters]
    leader,     \* raft leader, or NoHost
    known,      \* known[h]: the leader host h names, or NoHost
    storage,    \* storage[h]: h has the tablet's storage
    caughtUp,   \* hosts whose raft log has the group's state
    step,       \* coordinator: index of the next step of the current stage
    syncs,      \* sync handlers: [h, idx, st]
    rpcs,       \* cleanup RPCs: [h, idx, k (attempt), st]
    reqs,       \* requests: [h, v, st]
    excluded,   \* hosts the coordinator treats as dead (excluded_tablet_nodes)
    dead,       \* hosts that are down
    promoted,   \* ghost: the pending replica of the migration became a voter
    nMig, nFail, nElect

vars == <<pub, vw, cfg, leader, known, storage, caughtUp, step, syncs, rpcs,
          reqs, excluded, dead, promoted, nMig, nFail, nElect>>

-----------------------------------------------------------------------------
\* The per-stage tables from the design doc.

Serving(r) ==
    CASE r.stage = "none"                                          -> r.C
      [] r.stage \in {"start_migration", "sc_add_nonvoter",
                      "sc_snapshot_transfer"}                      -> r.C
      [] r.stage \in {"sc_become_voter", "sc_rollback"}            -> r.C \cup {r.P}
      [] r.stage \in {"use_new", "cleanup", "end_migration"}       -> r.next
      [] r.stage \in {"sc_remove_pending", "cleanup_target",
                      "revert_migration"}                          -> r.C

HostsGroup(h, r) ==
    CASE r.stage = "none"                                          -> h \in r.C
      [] r.stage \in {"start_migration", "sc_add_nonvoter",
                      "sc_snapshot_transfer", "sc_become_voter", "use_new",
                      "sc_rollback", "sc_remove_pending"}          -> h \in r.C \/ h = r.P
      [] r.stage \in {"cleanup", "end_migration"}                  -> h \in r.next
      [] r.stage \in {"cleanup_target", "revert_migration"}        -> h \in r.C

Expected(r) ==
    CASE r.stage \in {"none", "start_migration", "sc_remove_pending",
                      "cleanup_target", "revert_migration"}        -> [v |-> r.C, nv |-> {}]
      [] r.stage \in {"sc_add_nonvoter", "sc_snapshot_transfer",
                      "sc_rollback"}                               -> [v |-> r.C, nv |-> {r.P}]
      [] r.stage = "sc_become_voter"                               -> [v |-> r.next, nv |-> {r.L}]
      [] r.stage \in {"use_new", "cleanup", "end_migration"}       -> [v |-> r.next, nv |-> {}]

StageSteps(s) ==
    CASE s = "start_migration"      -> <<>>
      [] s = "sc_add_nonvoter"      -> <<"sync">>
      [] s = "sc_snapshot_transfer" -> <<"transfer">>
      [] s = "sc_become_voter"      -> <<"barrier", "sync">>
      [] s = "use_new"              -> <<"barrier", "sync">>
      [] s = "cleanup"              -> <<"cleanup">>
      [] s = "end_migration"        -> <<"barrier">>
      [] s = "sc_rollback"          -> <<"sync">>
      [] s = "sc_remove_pending"    -> <<"barrier", "sync">>
      [] s = "cleanup_target"       -> <<"cleanup">>
      [] s = "revert_migration"     -> <<"barrier">>
      [] s = "none"                 -> <<>>

Steps(s) ==
    LET base == IF s \in ExtraBarrier THEN <<"barrier">> \o StageSteps(s) ELSE StageSteps(s)
    IN SelectSeq(base, LAMBDA x : ~(x = "barrier" /\ s \in SkipBarrier))

NextStage(s) ==
    CASE s = "start_migration"      -> "sc_add_nonvoter"
      [] s = "sc_add_nonvoter"      -> "sc_snapshot_transfer"
      [] s = "sc_snapshot_transfer" -> "sc_become_voter"
      [] s = "sc_become_voter"      -> "use_new"
      [] s = "use_new"              -> "cleanup"
      [] s = "cleanup"              -> "end_migration"
      [] s = "sc_rollback"          -> "sc_remove_pending"
      [] s = "sc_remove_pending"    -> "cleanup_target"
      [] s = "cleanup_target"       -> "revert_migration"

Members(c) == c.v \cup c.nv

-----------------------------------------------------------------------------
Cur == Len(pub)
CurState == pub[Cur]
View(h) == pub[vw[h]]
Running(h) == h \notin dead /\ HostsGroup(h, View(h))
Alive(h) == h \notin dead
\* A majority of the configuration's voters is up.
Quorum(c) == 2 * Cardinality(c.v \ dead) > Cardinality(c.v)

ActiveReqs == {i \in DOMAIN reqs : reqs[i].st \in {"pinned", "inside"}}
LiveViews == {vw[h] : h \in Hosts \ dead} \cup {reqs[i].v : i \in ActiveReqs}

Init ==
    /\ pub = << [mig |-> 0, stage |-> "none", C |-> InitReplicas,
                 next |-> InitReplicas, L |-> NoHost, P |-> NoHost] >>
    /\ vw = [h \in Hosts |-> 1]
    /\ cfg = [v |-> InitReplicas, nv |-> {}]
    /\ \E l \in InitReplicas :
        /\ leader = l
        /\ known = [h \in Hosts |-> IF h \in InitReplicas THEN l ELSE NoHost]
    /\ storage = [h \in Hosts |-> h \in InitReplicas]
    /\ caughtUp = InitReplicas
    /\ step = 1
    /\ syncs = {} /\ rpcs = {}
    /\ excluded = {} /\ dead = {} /\ promoted = FALSE
    /\ reqs = [i \in 1..MaxRequests |-> [h |-> NoHost, v |-> 0, st |-> "unused"]]
    /\ nMig = 0 /\ nFail = 0 /\ nElect = 0

Publish(r) ==
    /\ pub' = Append(pub, r)
    /\ step' = 1

-----------------------------------------------------------------------------
\* Topology coordinator

StartMigration ==
    /\ CurState.stage = "none"
    /\ nMig < MaxMigrations
    /\ \E l \in CurState.C, p \in Hosts \ (CurState.C \cup excluded) :
        Publish([mig |-> nMig + 1, stage |-> "start_migration", C |-> CurState.C,
                 next |-> (CurState.C \ {l}) \cup {p}, L |-> l, P |-> p])
    /\ nMig' = nMig + 1
    /\ promoted' = FALSE
    /\ UNCHANGED <<excluded, dead, vw, cfg, leader, known, storage, caughtUp, syncs, rpcs, reqs,
                   nFail, nElect>>

\* All steps of the stage are done: publish the next stage, or, at the end
\* of a migration, delete the transition.
Advance ==
    /\ CurState.stage # "none"
    /\ step > Len(Steps(CurState.stage))
    /\ IF CurState.stage = "end_migration"
       THEN Publish([CurState EXCEPT !.stage = "none", !.C = CurState.next,
                     !.L = NoHost, !.P = NoHost])
       ELSE IF CurState.stage = "revert_migration"
       THEN Publish([CurState EXCEPT !.stage = "none", !.next = CurState.C,
                     !.L = NoHost, !.P = NoHost])
       ELSE Publish([CurState EXCEPT !.stage = NextStage(CurState.stage)])
    /\ UNCHANGED <<excluded, dead, promoted, vw, cfg, leader, known, storage, caughtUp, syncs, rpcs, reqs,
                   nMig, nFail, nElect>>

\* An error, or a cancellation, at any point of a stage before the pending
\* replica can be promoted.
FailToRemove ==
    /\ nFail < MaxFailures
    /\ CurState.stage \in FailableToRemove
    /\ Publish([CurState EXCEPT !.stage = "sc_remove_pending"])
    /\ nFail' = nFail + 1
    /\ UNCHANGED <<excluded, dead, promoted, vw, cfg, leader, known, storage, caughtUp, syncs, rpcs, reqs,
                   nMig, nElect>>

\* sc_become_voter rolls back when more new replicas than old ones are
\* excluded, i.e. the pending replica is.
FailToRollback ==
    /\ nFail < MaxFailures
    /\ CurState.stage = "sc_become_voter"
    /\ Publish([CurState EXCEPT !.stage = "sc_rollback"])
    /\ excluded' = excluded \cup {CurState.P}
    /\ IF ExcludedPendingAlive
       THEN UNCHANGED <<dead, leader, reqs>>
       ELSE /\ dead' = dead \cup {CurState.P}
            /\ leader' = IF leader = CurState.P THEN NoHost ELSE leader
            /\ reqs' = [i \in DOMAIN reqs |->
                          IF reqs[i].h = CurState.P THEN [reqs[i] EXCEPT !.st = "done"] ELSE reqs[i]]
    /\ nFail' = nFail + 1
    /\ UNCHANGED <<promoted, vw, cfg, known, storage, caughtUp, syncs, rpcs, nMig, nElect>>

StepIs(x) == step <= Len(Steps(CurState.stage)) /\ Steps(CurState.stage)[step] = x

DoneStep == step' = step + 1

\* Global barrier and drain at the current stage.  Excluded nodes are
\* skipped.
Barrier ==
    /\ StepIs("barrier")
    /\ \A h \in Hosts \ excluded : vw[h] = Cur
    /\ \A i \in ActiveReqs : reqs[i].h \notin excluded => reqs[i].v = Cur
    /\ \A r \in rpcs : r.st = "running" /\ r.h \notin excluded => r.idx = Cur
    \* A sync handler's tablet_metadata_guard holds the old map.
    /\ \A t \in syncs : t.st = "running" /\ t.h \notin excluded => t.idx = Cur
    /\ DoneStep
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, syncs, rpcs,
                   reqs, nMig, nFail, nElect>>

\* sync_raft_group_config to a host of the stage's serving set, sent again if
\* refused.
SyncSend ==
    /\ StepIs("sync")
    /\ \E h \in Serving(CurState) \ excluded :
        /\ ~\E s \in syncs : s.h = h /\ s.idx = Cur /\ s.st \in {"sent", "running", "done"}
        /\ syncs' = syncs \cup {[h |-> h, idx |-> Cur, st |-> "sent"]}
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, step, rpcs,
                   reqs, nMig, nFail, nElect>>

SyncComplete ==
    /\ StepIs("sync")
    /\ \A h \in Serving(CurState) \ excluded : [h |-> h, idx |-> Cur, st |-> "done"] \in syncs
    /\ DoneStep
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, syncs, rpcs,
                   reqs, nMig, nFail, nElect>>

\* The pending replica receives a raft snapshot.
Transfer ==
    /\ StepIs("transfer")
    /\ Running(CurState.P)
    /\ caughtUp' = caughtUp \cup {CurState.P}
    /\ DoneStep
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, syncs, rpcs, reqs,
                   nMig, nFail, nElect>>

CleanupTarget(r) == IF r.stage = "cleanup" THEN r.L ELSE r.P

\* A cleanup RPC is sent; a timed out one is sent again, while the first may
\* still run.
CleanupSend ==
    /\ StepIs("cleanup")
    /\ ~\E r \in rpcs : r.idx = Cur /\ r.st = "done"
    /\ \E k \in 1..MaxCleanupRpcs :
        /\ ~\E r \in rpcs : r.idx = Cur /\ r.k = k
        /\ rpcs' = rpcs \cup {[h |-> CleanupTarget(CurState), idx |-> Cur, k |-> k, st |-> "sent"]}
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, step, syncs,
                   reqs, nMig, nFail, nElect>>

\* A cleanup of an excluded node is skipped.
CleanupComplete ==
    /\ StepIs("cleanup")
    /\ \/ \E r \in rpcs : r.idx = Cur /\ r.st = "done"
       \/ CleanupTarget(CurState) \in excluded
    /\ DoneStep
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, syncs, rpcs,
                   reqs, nMig, nFail, nElect>>

-----------------------------------------------------------------------------
\* Replicas

\* A host applies group 0 up to some later state.  Its raft server and its
\* storage follow from the state it sees (I5).
ApplyGroup0(h) ==
    /\ Alive(h)
    /\ vw[h] < Cur
    /\ \E j \in (vw[h] + 1)..Cur :
        /\ vw' = [vw EXCEPT ![h] = j]
        /\ storage' = [storage EXCEPT ![h] = @ \/ HostsGroup(h, pub[j])]
        /\ leader' = IF leader = h /\ ~HostsGroup(h, pub[j]) THEN NoHost ELSE leader
    /\ UNCHANGED <<excluded, dead, promoted, pub, cfg, known, caughtUp, step, syncs, rpcs, reqs, nMig,
                   nFail, nElect>>

SyncStart(s) ==
    /\ s.st = "sent"
    /\ Alive(s.h)
    /\ vw[s.h] >= s.idx
    /\ IF vw[s.h] = s.idx /\ Running(s.h)
       THEN syncs' = {IF t.h = s.h /\ t.st = "running" /\ NewerSyncSupersedes
                      THEN [t EXCEPT !.st = "aborted"] ELSE t : t \in syncs \ {s}}
                     \cup {[s EXCEPT !.st = "running"]}
       ELSE syncs' = (syncs \ {s}) \cup {[s EXCEPT !.st = "refused"]}
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, step, rpcs,
                   reqs, nMig, nFail, nElect>>

\* The handler on the leader proposes the stage's configuration.  The abort
\* source is checked right before modify_config() appends the entry.
SyncPropose(s) ==
    /\ s.st = "running"
    /\ Alive(s.h)
    /\ GuardAbortsSync => vw[s.h] = s.idx
    /\ leader = s.h
    /\ cfg # Expected(pub[s.idx])
    /\ cfg' = Expected(pub[s.idx])
    /\ Quorum(cfg) /\ Quorum(cfg')
    /\ leader' = IF s.h \in cfg'.v THEN leader ELSE NoHost
    /\ promoted' = (promoted \/ CurState.P \in cfg'.v)
    /\ UNCHANGED <<excluded, dead, pub, vw, known, storage, caughtUp, step, syncs, rpcs, reqs,
                   nMig, nFail, nElect>>

\* synced(S): the configuration is the stage's, and a voter names a voter.
SyncReport(s) ==
    /\ s.st = "running"
    /\ Alive(s.h)
    /\ GuardAbortsSync => vw[s.h] = s.idx
    /\ cfg = Expected(pub[s.idx])
    /\ s.h \in cfg.v => known[s.h] \in cfg.v
    /\ syncs' = (syncs \ {s}) \cup {[s EXCEPT !.st = "done"]}
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, step, rpcs,
                   reqs, nMig, nFail, nElect>>

\* The handler ends without converging: at the coordinator's deadline, or,
\* once the stage moved on on its host, by its guard.
SyncEnd(s) ==
    /\ s.st = "running"
    /\ syncs' = (syncs \ {s}) \cup {[s EXCEPT !.st = "aborted"]}
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, step, rpcs,
                   reqs, nMig, nFail, nElect>>

\* storage_service::cleanup_tablet(): after a read barrier, checks that the
\* tablet is at cleanup or cleanup_target with this host as its target (not
\* which migration it is in), waits for the server's teardown, and later
\* removes the storage.
CleanupStart(r) ==
    /\ r.st = "sent"
    /\ Alive(r.h)
    /\ vw[r.h] >= r.idx
    /\ IF View(r.h).stage \in {"cleanup", "cleanup_target"}
          /\ CleanupTarget(View(r.h)) = r.h /\ ~Running(r.h)
       THEN rpcs' = (rpcs \ {r}) \cup {[r EXCEPT !.st = "running"]}
       ELSE rpcs' = (rpcs \ {r}) \cup {[r EXCEPT !.st = "refused"]}
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, step, syncs,
                   reqs, nMig, nFail, nElect>>

CleanupEffect(r) ==
    /\ r.st = "running"
    /\ storage' = [storage EXCEPT ![r.h] = FALSE]
    /\ caughtUp' = caughtUp \ {r.h}
    /\ rpcs' = (rpcs \ {r}) \cup {[r EXCEPT !.st = "done"]}
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, step, syncs, reqs, nMig, nFail,
                   nElect>>

-----------------------------------------------------------------------------
\* Raft of the tablet's group

Elect(v) ==
    /\ nElect < MaxElections
    /\ v \in cfg.v /\ Running(v) /\ v # leader
    /\ Quorum(cfg)
    /\ leader' = v
    /\ known' = [known EXCEPT ![v] = v]
    /\ nElect' = nElect + 1
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, storage, caughtUp, step, syncs, rpcs, reqs,
                   nMig, nFail>>

\* The leader contacts a member, which learns who leads.
LearnLeader(h) ==
    /\ leader # NoHost /\ leader # h
    /\ h \in Members(cfg) /\ Running(h)
    /\ known[h] # leader
    /\ known' = [known EXCEPT ![h] = leader]
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, storage, caughtUp, step, syncs, rpcs,
                   reqs, nMig, nFail, nElect>>

-----------------------------------------------------------------------------
\* Requests

\* create_operation_ctx(): the request pins the host's tablet map.
Pin(i, h) ==
    /\ reqs[i].st = "unused"
    /\ Alive(h)
    /\ h \in Serving(View(h))
    /\ reqs' = [reqs EXCEPT ![i] = [h |-> h, v |-> vw[h], st |-> "pinned"]]
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, step, syncs,
                   rpcs, nMig, nFail, nElect>>

\* acquire_server(): finds the server the host runs now, if any.
Enter(i) ==
    /\ reqs[i].st = "pinned"
    /\ reqs' = [reqs EXCEPT ![i].st = IF Running(reqs[i].h) THEN "inside" ELSE "done"]
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, step, syncs,
                   rpcs, nMig, nFail, nElect>>

Leave(i) ==
    /\ reqs[i].st \in {"pinned", "inside"}
    /\ reqs' = [reqs EXCEPT ![i].st = "done"]
    /\ UNCHANGED <<excluded, dead, promoted, pub, vw, cfg, leader, known, storage, caughtUp, step, syncs,
                   rpcs, nMig, nFail, nElect>>

-----------------------------------------------------------------------------
Next ==
    \/ StartMigration \/ Advance \/ FailToRemove \/ FailToRollback \/ Barrier \/ SyncSend \/ SyncComplete
    \/ Transfer \/ CleanupSend \/ CleanupComplete
    \/ \E h \in Hosts : ApplyGroup0(h) \/ Elect(h) \/ LearnLeader(h)
    \/ \E s \in syncs : SyncStart(s) \/ SyncPropose(s) \/ SyncReport(s) \/ SyncEnd(s)
    \/ \E r \in rpcs : CleanupStart(r) \/ CleanupEffect(r)
    \/ \E i \in 1..MaxRequests : Enter(i) \/ Leave(i) \/ \E h \in Hosts : Pin(i, h)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Invariants, named as in the design doc.

\* I1: a leader a node of a live view's serving set names is in that set.
I1_LeaderCoverage ==
    \A v \in LiveViews : \A h \in Serving(pub[v]) \ dead :
        known[h] \in Serving(pub[v]) \cup {NoHost}

\* I2: a request inside m's server keeps m a member, with a running server.
I2_Membership ==
    \A i \in ActiveReqs : reqs[i].st = "inside" =>
        reqs[i].h \in Members(cfg) /\ Running(reqs[i].h)

\* I3: nothing reads or applies to m's storage after its cleanup.
I3_Storage ==
    \A i \in ActiveReqs : reqs[i].st = "inside" => storage[reqs[i].h]

\* I4: the serving sets of any two live views are nested.
I4_Nesting ==
    \A v1, v2 \in LiveViews :
        Serving(pub[v1]) \subseteq Serving(pub[v2]) \/ Serving(pub[v2]) \subseteq Serving(pub[v1])

\* I5: a replica runs the group's server only while it has the storage.
I5_StorageUnderServer == \A h \in Hosts : Running(h) => storage[h]

\* Raft safety needs every voter to hold the group's state.
VotersCaughtUp == cfg.v \subseteq caughtUp

LeaderIsVoter == leader # NoHost => leader \in cfg.v

\* synced(S) holds until the next sync: once the current stage's sync is
\* done, or if it has none, the configuration is the stage's.
SyncPending ==
    \E k \in 1..Len(Steps(CurState.stage)) :
        Steps(CurState.stage)[k] = "sync" /\ step <= k
SyncedHolds == ~SyncPending => cfg = Expected(CurState)

\* Witnesses: "invariants" a configuration expects to be violated, to show
\* that a path is reachable within its bounds, so that a pass covers it.
Reached(st) == \E k \in 1..Len(pub) : pub[k].stage = st
W_MigrationEnded == ~(Reached("end_migration") /\ CurState.stage = "none")
W_RemovedPending == ~(Reached("sc_remove_pending") /\ CurState.stage = "none")
W_RolledBack == ~(Reached("sc_rollback") /\ CurState.stage = "none")
W_RolledBackAfterPromotion == ~(promoted /\ Reached("sc_rollback") /\ CurState.stage = "none")
W_SecondMigrationEnded == ~(nMig = 2 /\ CurState.stage = "none")

-----------------------------------------------------------------------------
\* State abstraction for TLC (VIEW), to keep fewer and smaller states: the
\* published states before the oldest one any host or pending work can still
\* see are dropped and the rest renumbered, and finished handlers of earlier
\* stages, which nothing reads, are forgotten.  Nothing in the spec compares
\* an index with anything but another index, so this changes no behavior.
\* Configurations whose invariants read the history (the witnesses) don't
\* use it.
LiveIdx ==
    {vw[h] : h \in Hosts} \cup {reqs[i].v : i \in ActiveReqs} \cup {Cur}
    \cup {t.idx : t \in {x \in syncs : x.st \in {"sent", "running"}}}
    \cup {r.idx : r \in {x \in rpcs : x.st \in {"sent", "running"}}}
Lo == CHOOSE i \in LiveIdx : \A j \in LiveIdx : i <= j
Sh(i) == i - Lo + 1
Keep(x) == x.st \in {"sent", "running"} \/ x.idx = Cur
\* (Comparing each variable with itself makes TLC materialize lazily built
\* functions in the state, which it otherwise fails to write to disk when
\* fingerprints come from a view.)
Materialized == pub = pub /\ vw = vw /\ known = known /\ storage = storage /\ reqs = reqs /\ syncs = syncs /\ rpcs = rpcs
StateView ==
    <<Materialized, SubSeq(pub, Lo, Len(pub)),
      [h \in Hosts |-> Sh(vw[h])],
      [i \in DOMAIN reqs |-> IF i \in ActiveReqs THEN [reqs[i] EXCEPT !.v = Sh(@)]
                             ELSE [h |-> NoHost, v |-> 0, st |-> reqs[i].st]],
      {[t EXCEPT !.idx = Sh(@)] : t \in {x \in syncs : Keep(x)}},
      {[r EXCEPT !.idx = Sh(@)] : r \in {x \in rpcs : Keep(x)}},
      cfg, leader, known, storage, caughtUp, step, excluded, dead, nMig, nFail, nElect>>

\* The two initial replicas are interchangeable.
Symm == Permutations(InitReplicas)

=============================================================================
