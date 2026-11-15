\* Copyright (C) 2026-present ScyllaDB
\*
\* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

--------------------------- MODULE compaction_groups ---------------------------
(***************************************************************************)
(* Ownership of sstables by compaction groups of one table's tablets, while*)
(* compaction, tablet split, tablet merge, streaming and repair run        *)
(* concurrently on one shard.                                              *)
(*                                                                         *)
(* Layouts, in order: one tablet (main group M); split mode (M plus the    *)
(* split-ready groups SL and SR, which new sstables of one side go to);    *)
(* split (SL and SR are the mains of two tablets); merged (SL and SR are   *)
(* merging groups of a new tablet whose main group is N); merged and the   *)
(* merge fiber done.  The tablet map swap is synchronous and takes no      *)
(* lock: it happens between any two steps.                                 *)
(*                                                                         *)
(* Code (replica/table.cc, compaction/compaction_manager.cc):              *)
(*   Route                compaction_group_for_sstable(), the live map     *)
(*   RegSnapshot/Register regular compaction selection under the view's    *)
(*                        sstable_set_lock, registering in _compacting     *)
(*   RegComplete, Gc*     the replacer: removes inputs from the group the  *)
(*                        compaction runs on, adds outputs to the routed   *)
(*                        group; incremental compaction's GC sstables      *)
(*   Split*               split_compaction_task_executor: rewrite, bypass  *)
(*   Add*                 add_sstable_and_update_cache(): route, hold the  *)
(*                        group's async_gate, add                          *)
(*   MergeSwap/FiberStop  handle_tablet_merge_completion(),                *)
(*                        merge_completion_fiber(): stop, then move        *)
(*   SplitComplete        handle_tablet_split_completion()                 *)
(*   Cr*                  perform_component_rewrite() (repair marking)     *)
(*                                                                         *)
(* Abstractions: one view per group (no repaired/unrepaired split); the    *)
(* maintenance set and off-strategy are left out; an sstable is its id and *)
(* the side of the split point it covers ("L", "R" or both, "LR").         *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    SelectHoldsLock,  \* selection holds sstable_set_lock from snapshot to registration (3f9d1c16c70)
    BypassTakesLock,  \* the split bypass takes sstable_set_lock (7420f936bbc)
    GcPinned,         \* GC sstables go to the compaction's own group (99d302fb483)
    MaxRegular, MaxGc, MaxAdds, MaxRewrites, MaxIds

CGs == {"M", "SL", "SR", "N"}
NONE == "none"

VARIABLES
    phase,      \* "normal", "split_mode", "split", "merged", "merged_done"
    cg,         \* cg[c]: [st, set, dis]; st: "absent", "active", "merging", "stopped"
    side,       \* side[s]: the side of the split point sstable s covers
    lock,       \* lock[c]: the task holding c's sstable_set_lock, or 0
    compacting, \* the compaction manager's _compacting_sstables
    tasks,      \* tasks[i]: [kind, c, in, snap, st, gc, todo]
    adds,       \* pending adds: [s, c]
    err,        \* a replacer didn't find an input in its group ("Unable to remove input SSTable")
    lost,       \* an sstable was added to a group that is gone
    nextId, nR, nG, nA, nC

vars == <<phase, cg, side, lock, compacting, tasks, adds, err, lost, nextId, nR, nG, nA, nC>>

TaskIds == 1..(MaxRegular + MaxRewrites + 1)

Route(sd) ==
    CASE phase = "normal"                 -> "M"
      [] phase = "split_mode"             -> IF sd = "LR" THEN "M" ELSE IF sd = "L" THEN "SL" ELSE "SR"
      [] phase = "split"                  -> IF sd = "L" THEN "SL" ELSE IF sd = "R" THEN "SR" ELSE NONE
      [] phase \in {"merged", "merged_done"} -> "N"

SideOf(S) == IF \A s \in S : side[s] = "L" THEN "L"
             ELSE IF \A s \in S : side[s] = "R" THEN "R" ELSE "LR"

Live(c) == cg[c].st \in {"active", "merging"}
Running(i) == tasks[i].st \in {"snap", "run", "gc"}
FreeTask == CHOOSE i \in TaskIds : tasks[i].st = "unused"
HasFree == \E i \in TaskIds : tasks[i].st = "unused"

\* Adds sstable s to group d; a group that is gone loses it.
AddTo(c0, d, s) ==
    IF d # NONE /\ Live(d) THEN [c0 EXCEPT ![d].set = @ \cup {s}] ELSE c0
Lost(d) == d = NONE \/ ~Live(d)

NoTask == [kind |-> "none", c |-> "M", in |-> {}, snap |-> {}, st |-> "unused", gc |-> 0, todo |-> {}]

Init ==
    /\ phase = "normal"
    /\ cg = [c \in CGs |-> IF c = "M" THEN [st |-> "active", set |-> {1, 2, 3}, dis |-> FALSE]
                           ELSE [st |-> "absent", set |-> {}, dis |-> FALSE]]
    /\ side = [s \in 1..MaxIds |-> CASE s = 1 -> "LR" [] s = 2 -> "L" [] s = 3 -> "R" [] OTHER -> "LR"]
    /\ lock = [c \in CGs |-> 0]
    /\ compacting = {}
    /\ tasks = [i \in TaskIds |-> NoTask]
    /\ adds = {}
    /\ err = FALSE /\ lost = FALSE
    /\ nextId = 4 /\ nR = 0 /\ nG = 0 /\ nA = 0 /\ nC = 0

-----------------------------------------------------------------------------
\* Regular compaction.  Selection snapshots the main set and later filters
\* out what is registered as compacting and registers its inputs; between
\* the two there is a yield, under sstable_set_lock.
RegSnapshot(c) ==
    /\ nR < MaxRegular /\ HasFree
    /\ cg[c].st = "active" /\ ~cg[c].dis /\ lock[c] = 0
    /\ LET i == FreeTask IN
       /\ tasks' = [tasks EXCEPT ![i] = [NoTask EXCEPT !.kind = "regular", !.c = c, !.snap = cg[c].set, !.st = "snap"]]
       /\ lock' = IF SelectHoldsLock THEN [lock EXCEPT ![c] = i] ELSE lock
    /\ nR' = nR + 1
    /\ UNCHANGED <<phase, cg, side, compacting, adds, err, lost, nextId, nG, nA, nC>>

RegRegister(i) ==
    /\ tasks[i].st = "snap"
    /\ LET c == tasks[i].c IN
       \E in \in SUBSET (tasks[i].snap \ compacting) :
          /\ in # {}
          /\ compacting' = compacting \cup in
          /\ tasks' = [tasks EXCEPT ![i].in = in, ![i].st = "run"]
          /\ lock' = IF lock[c] = i THEN [lock EXCEPT ![c] = 0] ELSE lock
    /\ UNCHANGED <<phase, cg, side, adds, err, lost, nextId, nR, nG, nA, nC>>

\* Incremental compaction: an exhausted run produces a garbage-collection
\* sstable, added now and removed when the compaction completes.
GcAdd(i) ==
    /\ tasks[i].st = "run" /\ tasks[i].kind = "regular" /\ nG < MaxGc /\ nextId <= MaxIds
    /\ lock[tasks[i].c] = 0
    /\ LET g == nextId
           d == IF GcPinned THEN tasks[i].c ELSE Route(SideOf(tasks[i].in))
       IN /\ side' = [side EXCEPT ![g] = SideOf(tasks[i].in)]
          /\ cg' = AddTo(cg, d, g)
          /\ lost' = (lost \/ Lost(d))
          /\ compacting' = compacting \cup {g}
          /\ tasks' = [tasks EXCEPT ![i].gc = g, ![i].st = "gc"]
    /\ nextId' = nextId + 1 /\ nG' = nG + 1
    /\ UNCHANGED <<phase, lock, adds, err, nR, nA, nC>>

\* The replacer, under the source group's sstable_set_lock: removes the
\* inputs (and the GC sstable) from the group the compaction runs on, adds
\* the output to the group the live map routes it to.
RegComplete(i) ==
    /\ tasks[i].st \in {"run", "gc"} /\ tasks[i].kind = "regular" /\ nextId <= MaxIds
    /\ LET c == tasks[i].c
           rm == tasks[i].in \cup (IF tasks[i].gc > 0 THEN {tasks[i].gc} ELSE {})
           o == nextId
           d == Route(SideOf(tasks[i].in))
       IN /\ lock[c] = 0
          /\ err' = (err \/ ~(rm \subseteq cg[c].set))
          /\ side' = [side EXCEPT ![o] = SideOf(tasks[i].in)]
          /\ cg' = AddTo([cg EXCEPT ![c].set = @ \ rm], d, o)
          /\ lost' = (lost \/ Lost(d))
          /\ compacting' = compacting \ rm
    /\ tasks' = [tasks EXCEPT ![i].st = "done"]
    /\ nextId' = nextId + 1
    /\ UNCHANGED <<phase, lock, adds, nR, nG, nA, nC>>

\* Stopping or disabling compaction on a group aborts its regular tasks.
AbortOn(c) ==
    LET ab == {i \in TaskIds : tasks[i].kind = "regular" /\ tasks[i].c = c /\ Running(i)} IN
    /\ tasks' = [i \in TaskIds |-> IF i \in ab THEN [tasks[i] EXCEPT !.st = "aborted"] ELSE tasks[i]]
    /\ compacting' = compacting \ UNION {tasks[i].in : i \in ab}
    /\ lock' = IF lock[c] \in ab THEN [lock EXCEPT ![c] = 0] ELSE lock

-----------------------------------------------------------------------------
\* Tablet split.
SplitMode ==
    /\ phase = "normal"
    /\ phase' = "split_mode"
    /\ cg' = [cg EXCEPT !["SL"] = [st |-> "active", set |-> {}, dis |-> FALSE],
                        !["SR"] = [st |-> "active", set |-> {}, dis |-> FALSE]]
    /\ UNCHANGED <<side, lock, compacting, tasks, adds, err, lost, nextId, nR, nG, nA, nC>>

\* The split task captures M's sstables under run_with_compaction_disabled():
\* M's regular compactions are stopped first.
AbortedOn(c) == {i \in TaskIds : tasks[i].kind = "regular" /\ tasks[i].c = c /\ Running(i)}
SplitStart ==
    /\ phase = "split_mode" /\ HasFree
    /\ ~\E i \in TaskIds : tasks[i].kind = "split"
    /\ lock["M"] = 0
    /\ LET ab == AbortedOn("M")
           i == FreeTask
           comp == compacting \ UNION {tasks[t].in : t \in ab}
           in == cg["M"].set \ comp
       IN /\ tasks' = [[t \in TaskIds |-> IF t \in ab THEN [tasks[t] EXCEPT !.st = "aborted"] ELSE tasks[t]]
                        EXCEPT ![i] = [NoTask EXCEPT !.kind = "split", !.c = "M", !.in = in, !.todo = in, !.st = "run"]]
          /\ compacting' = comp \cup in
    /\ UNCHANGED <<phase, cg, side, lock, adds, err, lost, nextId, nR, nG, nA, nC>>

\* An sstable spanning both sides is rewritten into one per side; one of a
\* single side is moved as is (bypass), staying registered until the task
\* ends.  Both remove it from M and add by the live map.
SplitStep(i) ==
    /\ tasks[i].kind = "split" /\ tasks[i].st = "run" /\ tasks[i].todo # {}
    /\ \E x \in tasks[i].todo :
        /\ err' = (err \/ x \notin cg["M"].set)
        /\ IF side[x] = "LR"
           THEN /\ lock["M"] = 0 /\ nextId + 1 <= MaxIds
                /\ LET l == nextId r == nextId + 1 IN
                   /\ side' = [side EXCEPT ![l] = "L", ![r] = "R"]
                   /\ cg' = AddTo(AddTo([cg EXCEPT !["M"].set = @ \ {x}], Route("L"), l), Route("R"), r)
                   /\ lost' = (lost \/ Lost(Route("L")) \/ Lost(Route("R")))
                   /\ nextId' = nextId + 2
           ELSE /\ BypassTakesLock => lock["M"] = 0
                /\ cg' = AddTo([cg EXCEPT !["M"].set = @ \ {x}], Route(side[x]), x)
                /\ lost' = (lost \/ Lost(Route(side[x])))
                /\ UNCHANGED <<side, nextId>>
        /\ tasks' = [tasks EXCEPT ![i].todo = @ \ {x}]
    /\ UNCHANGED <<phase, lock, compacting, adds, nR, nG, nA, nC>>

SplitDone(i) ==
    /\ tasks[i].kind = "split" /\ tasks[i].st = "run" /\ tasks[i].todo = {}
    /\ tasks' = [tasks EXCEPT ![i].st = "done"]
    /\ compacting' = compacting \ tasks[i].in
    /\ UNCHANGED <<phase, cg, side, lock, adds, err, lost, nextId, nR, nG, nA, nC>>

\* The new tablet count is committed once M is empty and nothing is being
\* added to it.
SplitComplete ==
    /\ phase = "split_mode"
    /\ cg["M"].set = {} /\ ~\E a \in adds : a.c = "M"
    /\ phase' = "split"
    /\ cg' = [cg EXCEPT !["M"].st = "stopped"]
    /\ AbortOn("M")
    /\ UNCHANGED <<side, adds, err, lost, nextId, nR, nG, nA, nC>>

-----------------------------------------------------------------------------
\* Tablet merge: the two groups become merging groups of the new tablet,
\* whose main group N starts with compaction disabled.
MergeSwap ==
    /\ phase = "split"
    /\ phase' = "merged"
    /\ cg' = [cg EXCEPT !["SL"].st = "merging", !["SR"].st = "merging",
                        !["N"] = [st |-> "active", set |-> {}, dis |-> TRUE]]
    /\ UNCHANGED <<side, lock, compacting, tasks, adds, err, lost, nextId, nR, nG, nA, nC>>

\* The merge fiber stops a merging group (aborting its compactions and
\* waiting for adds holding its gate) and moves its sstables into N.
FiberStop(g) ==
    /\ phase = "merged" /\ cg[g].st = "merging"
    /\ ~\E a \in adds : a.c = g
    /\ ~\E i \in TaskIds : tasks[i].c = g /\ tasks[i].kind # "regular" /\ Running(i)
    /\ AbortOn(g)
    /\ LET c1 == [cg EXCEPT !["N"].set = @ \cup cg[g].set, ![g] = [@ EXCEPT !.st = "stopped", !.set = {}]]
       IN /\ cg' = IF \A h \in {"SL", "SR"} \ {g} : c1[h].st = "stopped"
                   THEN [c1 EXCEPT !["N"].dis = FALSE] ELSE c1
          /\ phase' = IF \A h \in {"SL", "SR"} \ {g} : c1[h].st = "stopped" THEN "merged_done" ELSE phase
    /\ UNCHANGED <<side, adds, err, lost, nextId, nR, nG, nA, nC>>

-----------------------------------------------------------------------------
\* Streaming adds a new sstable: routes it, holds the group's gate, adds it.
AddStart ==
    /\ nA < MaxAdds /\ nextId <= MaxIds
    /\ \E sd \in {"L", "R", "LR"} :
        /\ phase = "split" => sd # "LR"
        /\ LET d == Route(sd) IN
           /\ d # NONE /\ cg[d].st = "active"
           /\ side' = [side EXCEPT ![nextId] = sd]
           /\ adds' = adds \cup {[s |-> nextId, c |-> d]}
    /\ nextId' = nextId + 1 /\ nA' = nA + 1
    /\ UNCHANGED <<phase, cg, lock, compacting, tasks, err, lost, nR, nG, nC>>

AddDo(a) ==
    /\ cg' = AddTo(cg, a.c, a.s)
    /\ lost' = (lost \/ Lost(a.c))
    /\ adds' = adds \ {a}
    /\ UNCHANGED <<phase, side, lock, compacting, tasks, err, nextId, nR, nG, nA, nC>>

-----------------------------------------------------------------------------
\* Repair marks an sstable repaired by rewriting its statistics: captured
\* with compaction disabled on its group, without filtering what is
\* registered as compacting; rewritten later through the replacer.
CrCapture(c) ==
    /\ nC < MaxRewrites /\ HasFree
    /\ cg[c].st = "active" /\ lock[c] = 0
    /\ ~\E i \in TaskIds : tasks[i].c = c /\ tasks[i].kind # "regular" /\ Running(i)
    /\ \E x \in cg[c].set :
        LET ab == AbortedOn(c)
            i == FreeTask
        IN /\ tasks' = [[t \in TaskIds |-> IF t \in ab THEN [tasks[t] EXCEPT !.st = "aborted"] ELSE tasks[t]]
                         EXCEPT ![i] = [NoTask EXCEPT !.kind = "rewrite", !.c = c, !.in = {x}, !.st = "run"]]
           /\ compacting' = (compacting \ UNION {tasks[t].in : t \in ab}) \cup {x}
    /\ nC' = nC + 1
    /\ UNCHANGED <<phase, cg, side, lock, adds, err, lost, nextId, nR, nG, nA>>

CrDo(i) ==
    /\ tasks[i].kind = "rewrite" /\ tasks[i].st = "run" /\ nextId <= MaxIds
    /\ LET c == tasks[i].c
           x == CHOOSE y \in tasks[i].in : TRUE
           o == nextId
           d == Route(side[x])
       IN /\ lock[c] = 0
          /\ err' = (err \/ x \notin cg[c].set)
          /\ side' = [side EXCEPT ![o] = side[x]]
          /\ cg' = AddTo([cg EXCEPT ![c].set = @ \ {x}], d, o)
          /\ lost' = (lost \/ Lost(d))
          /\ compacting' = compacting \ {x}
    /\ tasks' = [tasks EXCEPT ![i].st = "done"]
    /\ nextId' = nextId + 1
    /\ UNCHANGED <<phase, lock, adds, nR, nG, nA, nC>>

-----------------------------------------------------------------------------
Next ==
    \/ SplitMode \/ SplitStart \/ SplitComplete \/ MergeSwap \/ AddStart
    \/ \E c \in CGs : RegSnapshot(c) \/ CrCapture(c) \/ FiberStop(c)
    \/ \E i \in TaskIds : RegRegister(i) \/ GcAdd(i) \/ RegComplete(i) \/ SplitStep(i) \/ SplitDone(i) \/ CrDo(i)
    \/ \E a \in adds : AddDo(a)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* No "Unable to remove input SSTable": a replacer finds what it removes.
InputsFound == ~err
\* Nothing is added to a group that is gone, no sstable is in two groups,
\* and a stopped group holds nothing.
NoLoss ==
    /\ ~lost
    /\ \A c1, c2 \in CGs : c1 # c2 => cg[c1].set \cap cg[c2].set = {}
    /\ \A c \in CGs : cg[c].st = "stopped" => cg[c].set = {}

W_Merged == phase # "merged_done"
=============================================================================
