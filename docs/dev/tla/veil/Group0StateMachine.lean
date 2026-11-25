/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

-- models: service/raft/group0_state_machine.cc, service/raft/raft_group0_client.cc, db/schema_applier.cc, replica/database.cc
-- checked-against: 61d55c44f6a

/-! # The group 0 state machine of one node (catalog #1)

../group0_state_machine/group0_state_machine.tla action for action.
Encoding changes, none of which changes the reachable states:
- a command is named by its new state ID, a value of the ordered sort `sid`
  (the smallest one not used yet; TLC: `nextId`); log order is ID order, so
  the log is the set `inLog` with each command's `prev` (none for 0),
  `basis` and origin, and `applied` is the set of the first commands;
- `Eff(Len(log))` is kept as it grows: `effS` is its `st` and `ok` its
  `ok`, both functions of the log;
- `hist` leaves out the 0 every history holds; `pulled.hist` is
  `{0} \cup pulled.st`, so only `pst` and the index `pidx` are kept;
- an action with alternatives (ApplyNext, SnapMerge) is one action per
  alternative.
-/

veil module Group0StateMachine

set_option maxRecDepth 100000
set_option synthInstance.maxHeartbeats 2000000

type sid
instantiate tot : TotalOrder sid
open TotalOrder

param fixBreakMutexOnFailure : Bool
param enableImmediately : Bool
param modelGenericFailureAfterPersist : Bool
param maxRemoteOps : Nat
param maxLocalOps : Nat
param maxFailures : Nat
param maxRestarts : Nat
param checkMem : Bool

relation inLog : sid → Bool
relation pv : sid → sid → Bool
relation basis : sid → sid → Bool
relation remote : sid → Bool
relation effS : sid → Bool
individual ok : Bool
relation disk : sid → Bool
relation hist : sid → Bool
relation mem : sid → Bool
individual inMem : Bool
relation app : sid → Bool
individual broken : Bool
individual running : Bool
individual abortPending : Bool
individual pulled : Bool
relation pst : sid → Bool
relation pidx : sid → Bool
individual nRemote : Nat
individual nLocal : Nat
individual nFail : Nat
individual nRestart : Nat

#gen_state

after_init {
  inLog X := false
  pv X Y := false
  basis X Y := false
  remote X := false
  effS X := false
  ok := true
  disk X := false
  hist X := false
  mem X := false
  inMem := enableImmediately
  app X := false
  broken := false
  running := true
  abortPending := false
  pulled := false
  pst X := false
  pidx X := false
  nRemote := 0
  nLocal := 0
  nFail := 0
  nRestart := 0
}

-- RemoteOp: another node reads the latest state and appends a command.
action remote_op {
  let c :| ¬ inLog c ∧ ∀ u, ¬ inLog u → le c u
  let l :| (effS l ∧ ∀ x, effS x → le x l) ∨ (∀ x, ¬ effS x)
  require nRemote < maxRemoteOps
  inLog c := true
  remote c := true
  pv c X := decide (effS l ∧ X = l)
  basis c X := effS X
  effS c := true
  nRemote := nRemote + 1
}

-- LocalOp: a DDL statement on the node, computed from the in-memory schema,
-- with the last state ID of the node's history as prev_state_id.
action local_op {
  let c :| ¬ inLog c ∧ ∀ u, ¬ inLog u → le c u
  let h :| (hist h ∧ ∀ x, hist x → le x h) ∨ (∀ x, ¬ hist x)
  let l :| (effS l ∧ ∀ x, effS x → le x l) ∨ (∀ x, ¬ effS x)
  require nLocal < maxLocalOps
  require running ∧ inMem
  require ∀ x, inLog x → app x
  require ¬ broken
  -- The command takes effect if its prev is Eff's last.
  let isEff := (hist h ∧ effS l ∧ h = l) ∨ (¬ hist h ∧ ¬ effS l)
  ok := decide (ok ∧ (isEff → ∀ x, mem x ↔ effS x))
  inLog c := true
  pv c X := decide (hist h ∧ X = h)
  basis c X := mem X
  effS c := decide isEff
  nLocal := nLocal + 1
}

-- ApplyNext, obsolete command: a no-op.
action apply_noop {
  let e :| inLog e ∧ ¬ app e ∧ ∀ u, inLog u ∧ ¬ app u → le e u
  let h :| (hist h ∧ ∀ x, hist x → le x h) ∨ (∀ x, ¬ hist x)
  require running ∧ ¬ abortPending ∧ ¬ broken
  require ¬ ((hist h ∧ pv e h) ∨ (¬ hist h ∧ ∀ p, ¬ pv e p))
  app e := true
}

-- ApplyNext, success: the change, then the history entry.
action apply_ok {
  let e :| inLog e ∧ ¬ app e ∧ ∀ u, inLog u ∧ ¬ app u → le e u
  let h :| (hist h ∧ ∀ x, hist x → le x h) ∨ (∀ x, ¬ hist x)
  require running ∧ ¬ abortPending ∧ ¬ broken
  require (hist h ∧ pv e h) ∨ (¬ hist h ∧ ∀ p, ¬ pv e p)
  mem e := decide (mem e ∨ (inMem ∧ ¬ disk e))
  disk e := true
  hist e := true
  app e := true
}

-- ApplyNext, the schema is persisted and updating memory fails.
action apply_fail {
  let e :| inLog e ∧ ¬ app e ∧ ∀ u, inLog u ∧ ¬ app u → le e u
  let h :| (hist h ∧ ∀ x, hist x → le x h) ∨ (∀ x, ¬ hist x)
  require running ∧ ¬ abortPending ∧ ¬ broken
  require (hist h ∧ pv e h) ∨ (¬ hist h ∧ ∀ p, ¬ pv e p)
  require inMem
  require nFail < maxFailures
  nFail := nFail + 1
  disk e := true
  broken := broken || fixBreakMutexOnFailure
  abortPending := true
}

-- SnapPull: transfer_snapshot() pulls the current state, for a snapshot of
-- an index `c` past the applied one.
action snap_pull (c : sid) {
  require running
  require ¬ pulled
  require inLog c ∧ ¬ app c
  pulled := true
  pst X := effS X
  pidx X := decide (X = c)
}

-- SnapMerge with the in-memory state machine disabled: disk only.
action snap_merge_disk {
  require running ∧ pulled ∧ ¬ broken
  require ¬ inMem
  disk X := disk X || pst X
  hist X := hist X || pst X
  app X := decide (app X ∨ (inLog X ∧ ∃ p, pidx p ∧ le X p))
  pulled := false
  pst X := false
  pidx X := false
}

-- SnapMerge, merge_schema_from() succeeds.
action snap_merge_ok {
  require running ∧ pulled ∧ ¬ broken
  require inMem
  mem X := decide (mem X ∨ (pst X ∧ ¬ disk X))
  disk X := disk X || pst X
  hist X := hist X || pst X
  app X := decide (app X ∨ (inLog X ∧ ∃ p, pidx p ∧ le X p))
  pulled := false
  pst X := false
  pidx X := false
}

-- SnapMerge, schema_persisted_but_not_applied: fail() and abort.
action snap_merge_fail {
  require running ∧ pulled ∧ ¬ broken
  require inMem
  require nFail < maxFailures
  require ∃ x, pst x ∧ ¬ disk x
  nFail := nFail + 1
  disk X := disk X || pst X
  broken := broken || fixBreakMutexOnFailure
  abortPending := true
  pulled := false
  pst X := false
  pidx X := false
}

-- SnapMerge, another error after the mutations became visible: raft retries.
action snap_merge_generic {
  require running ∧ pulled ∧ ¬ broken
  require inMem
  require modelGenericFailureAfterPersist
  require nFail < maxFailures
  require ∃ x, pst x ∧ ¬ disk x
  nFail := nFail + 1
  disk X := disk X || pst X
  pulled := false
  pst X := false
  pidx X := false
}

-- enable_in_memory_state_machine().
action enable {
  require running ∧ ¬ inMem ∧ ¬ broken
  inMem := true
}

-- The background abort of the group 0 server completes.
action abort_server {
  require abortPending
  running := false
  abortPending := false
  pulled := false
  pst X := false
  pidx X := false
}

-- The node restarts: the schema is loaded from disk, raft replays the log.
action restart {
  require ¬ running ∧ ¬ abortPending
  require nRestart < maxRestarts
  nRestart := nRestart + 1
  running := true
  broken := false
  mem X := disk X
  inMem := true
  app X := false
}

-- Linearizable: every effective command was computed from the state it was applied on.
safety [linearizable] ok
-- MemMatchesDisk: whoever can take the read_apply mutex sees the persisted schema.
safety [mem_matches_disk] checkMem → (running ∧ inMem ∧ ¬ broken) → (mem X ↔ disk X)

#gen_spec

-- MC_before_02efd050634.cfg: TLC finds Linearizable violated (5 states).
-- expect: violation linearizable
#model_check compiled { sid := Fin 3, fixBreakMutexOnFailure := true, enableImmediately := false, modelGenericFailureAfterPersist := false, maxRemoteOps := 2, maxLocalOps := 1, maxFailures := 1, maxRestarts := 1, checkMem := false }
-- MC_before_2f372ec9707.cfg: TLC finds Linearizable violated (6 states).
-- expect: violation linearizable
#model_check compiled { sid := Fin 3, fixBreakMutexOnFailure := false, enableImmediately := true, modelGenericFailureAfterPersist := false, maxRemoteOps := 2, maxLocalOps := 1, maxFailures := 1, maxRestarts := 1, checkMem := false }
-- MC_current.cfg: TLC passes, 452 states.
-- expect: pass
#model_check compiled { sid := Fin 3, fixBreakMutexOnFailure := true, enableImmediately := true, modelGenericFailureAfterPersist := false, maxRemoteOps := 2, maxLocalOps := 1, maxFailures := 1, maxRestarts := 1, checkMem := true }
-- MC_current_large.cfg: TLC passes, 11,068 states.
-- expect: pass
#model_check compiled { sid := Fin 5, fixBreakMutexOnFailure := true, enableImmediately := true, modelGenericFailureAfterPersist := false, maxRemoteOps := 3, maxLocalOps := 2, maxFailures := 2, maxRestarts := 2, checkMem := true }
-- MC_generic_failure_after_persist.cfg: TLC finds Linearizable violated (6 states).
-- expect: violation linearizable
#model_check compiled { sid := Fin 3, fixBreakMutexOnFailure := true, enableImmediately := true, modelGenericFailureAfterPersist := true, maxRemoteOps := 2, maxLocalOps := 1, maxFailures := 1, maxRestarts := 1, checkMem := false }

end Group0StateMachine
