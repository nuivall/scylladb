/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

-- models: replica/table.cc, replica/compaction_group.hh, compaction/compaction_manager.cc, compaction/compaction.cc, repair/row_level.cc
-- checked-against: 61d55c44f6a

/-! # Compaction groups of a table's tablets on one shard (catalog #3)

../compaction_groups/compaction_groups.tla action for action. Encoding
changes:
- an sstable is a value of the ordered sort `sid` (two more values than
  MaxIds, so a pick of fresh ids never fails where TLC needs none; the
  `nextId` guards keep TLC's bound), the smallest unused one
  for a new sstable (TLC: `nextId`); `side` of an unused id is LR, as in
  TLC; the three initial sstables are created by a `setup` step before
  anything else runs;
- a task slot is a value of the ordered sort `tid`, the smallest unused
  (TLC: `CHOOSE`); a task's sets are relations;
- RegRegister's choice of a subset of the snapshot is a sequence of steps,
  one sstable at a time, while `busy` blocks every other action;
- `Route(sd)` is `NONE` only in the split phase for an LR sstable
  (`routeNone`), else the group `route`; `SideOf` of a task's inputs is
  computed in each action that needs it.
The state count differs from TLC's (the setup step, the registration
steps); the check is the same verdict on every config.
-/

veil module CompactionGroups

set_option maxRecDepth 100000
set_option synthInstance.maxHeartbeats 2000000

enum cgid = {cM, cSL, cSR, cN}
enum cgst = {absent, active, merging, stopped}
enum phs = {normal, splitMode, split, merged, mergedDone}
enum sd = {sL, sR, sLR}
enum tkind = {knone, regular, ksplit, rewrite}
enum tstat = {unused, tsnap, trun, tgc, tdone, taborted}

type sid
instantiate tot : TotalOrder sid
type tid
instantiate ttot : TotalOrder tid
open TotalOrder

param selectHoldsLock : Bool
param bypassTakesLock : Bool
param gcPinned : Bool
param maxRegular : Nat
param maxGc : Nat
param maxAdds : Nat
param maxRewrites : Nat
param maxIds : Nat
param witness : Bool

individual started : Bool
individual busy : Bool
individual phase : phs
function cst : cgid → cgst
relation cset : cgid → sid → Bool
relation cdis : cgid → Bool
function side : sid → sd
relation used : sid → Bool
relation lockBy : cgid → tid → Bool
relation compacting : sid → Bool
function kind : tid → tkind
function tc : tid → cgid
relation tin : tid → sid → Bool
relation snapS : tid → sid → Bool
function st : tid → tstat
relation gcOf : tid → sid → Bool
relation todo : tid → sid → Bool
relation addp : sid → cgid → Bool
individual err : Bool
individual lost : Bool
individual nextId : Nat
individual nR : Nat
individual nG : Nat
individual nA : Nat
individual nC : Nat

#gen_state

after_init {
  started := false
  busy := false
  phase := normal
  cst C := absent
  cset C S := false
  cdis C := false
  side S := sLR
  used S := false
  lockBy C T := false
  compacting S := false
  kind T := knone
  tc T := cM
  tin T S := false
  snapS T S := false
  st T := unused
  gcOf T S := false
  todo T S := false
  addp S C := false
  err := false
  lost := false
  nextId := 4
  nR := 0
  nG := 0
  nA := 0
  nC := 0
}

-- TLC's initial sstables 1 (LR), 2 (L), 3 (R) in M.
action setup {
  let a1 :| ∀ u, le a1 u
  let a2 :| a2 ≠ a1 ∧ ∀ u, u ≠ a1 → le a2 u
  let a3 :| a3 ≠ a1 ∧ a3 ≠ a2 ∧ ∀ u, u ≠ a1 ∧ u ≠ a2 → le a3 u
  require ¬ started
  started := true
  cst cM := active
  cset cM X := decide (X = a1 ∨ X = a2 ∨ X = a3)
  used X := decide (X = a1 ∨ X = a2 ∨ X = a3)
  side a2 := sL
  side a3 := sR
}

ghost relation live (c : cgid) := cst c = active ∨ cst c = merging
ghost relation running (t : tid) := st t = tsnap ∨ st t = trun ∨ st t = tgc

-- RegSnapshot(c): selection snapshots the main set.
action reg_snapshot (c : cgid) {
  let i :| st i = unused ∧ ∀ u, st u = unused → le i u
  require started ∧ ¬ busy
  require nR < maxRegular
  require cst c = active ∧ ¬ cdis c ∧ ∀ t, ¬ lockBy c t
  kind i := regular
  tc i := c
  snapS i X := cset c X
  st i := tsnap
  if selectHoldsLock then
    lockBy c i := true
  nR := nR + 1
}

-- RegRegister(i): registers a nonempty subset of the snapshot that isn't
-- compacting; chosen one sstable at a time.
action reg_pick (i : tid) (x : sid) {
  require started
  require st i = tsnap
  require ¬ busy ∨ (∃ y, tin i y)
  require snapS i x ∧ ¬ compacting x ∧ ¬ tin i x
  tin i x := true
  busy := true
}

action reg_register (i : tid) {
  require started ∧ busy
  require st i = tsnap ∧ ∃ y, tin i y
  compacting X := compacting X || tin i X
  st i := trun
  lockBy C i := false
  busy := false
}

-- GcAdd(i): an incremental compaction's GC sstable.
action gc_add (i : tid) {
  let g :| ¬ used g ∧ ∀ u, ¬ used u → le g u
  require started ∧ ¬ busy
  require st i = trun ∧ kind i = regular ∧ nG < maxGc ∧ nextId ≤ maxIds
  require ∀ t, ¬ lockBy (tc i) t
  let v := if (∀ s, tin i s → side s = sL) then sL else if (∀ s, tin i s → side s = sR) then sR else sLR
  let rNone := phase = split ∧ v = sLR
  let r := if phase = normal then cM else if phase = splitMode then (if v = sLR then cM else if v = sL then cSL else cSR)
    else if phase = split then (if v = sL then cSL else cSR) else cN
  let toNone := ¬ gcPinned ∧ rNone
  let d := if gcPinned then tc i else r
  used g := true
  side g := v
  if ¬ toNone ∧ (cst d = active ∨ cst d = merging) then
    cset d g := true
  lost := lost || decide (toNone ∨ ¬ (cst d = active ∨ cst d = merging))
  compacting g := true
  gcOf i g := true
  st i := tgc
  nextId := nextId + 1
  nG := nG + 1
}

-- RegComplete(i): the replacer.
action reg_complete (i : tid) {
  let o :| ¬ used o ∧ ∀ u, ¬ used u → le o u
  require started ∧ ¬ busy
  require (st i = trun ∨ st i = tgc) ∧ kind i = regular ∧ nextId ≤ maxIds
  require ∀ t, ¬ lockBy (tc i) t
  let c := tc i
  let v := if (∀ s, tin i s → side s = sL) then sL else if (∀ s, tin i s → side s = sR) then sR else sLR
  let rNone := phase = split ∧ v = sLR
  let d := if phase = normal then cM else if phase = splitMode then (if v = sLR then cM else if v = sL then cSL else cSR)
    else if phase = split then (if v = sL then cSL else cSR) else cN
  err := err || decide (∃ s, (tin i s ∨ gcOf i s) ∧ ¬ cset c s)
  used o := true
  side o := v
  cset c X := decide (cset c X ∧ ¬ tin i X ∧ ¬ gcOf i X)
  if ¬ rNone ∧ (cst d = active ∨ cst d = merging) then
    cset d o := true
  lost := lost || decide (rNone ∨ ¬ (cst d = active ∨ cst d = merging))
  compacting X := decide (compacting X ∧ ¬ tin i X ∧ ¬ gcOf i X)
  st i := tdone
  nextId := nextId + 1
}

action split_mode {
  require started ∧ ¬ busy
  require phase = normal
  phase := splitMode
  cst cSL := active
  cst cSR := active
}

-- SplitStart: M's regular compactions are aborted, then the split task
-- captures M's sstables that aren't compacting.
action split_start {
  let i :| st i = unused ∧ ∀ u, st u = unused → le i u
  require started ∧ ¬ busy
  require phase = splitMode
  require ∀ t, kind t ≠ ksplit
  require ∀ t, ¬ lockBy cM t
  compacting X := decide (compacting X ∧
    ¬ ∃ t, kind t = regular ∧ tc t = cM ∧ (st t = tsnap ∨ st t = trun ∨ st t = tgc) ∧ tin t X)
  st T := if kind T = regular ∧ tc T = cM ∧ (st T = tsnap ∨ st T = trun ∨ st T = tgc) then taborted else st T
  kind i := ksplit
  tc i := cM
  tin i X := decide (cset cM X ∧ ¬ compacting X)
  todo i X := decide (cset cM X ∧ ¬ compacting X)
  st i := trun
  compacting X := compacting X || tin i X
}

-- SplitStep(i, x): rewrite an LR sstable into one per side, or move a
-- one-sided one (the bypass); both add by the live map.
action split_step (i : tid) (x : sid) {
  let l :| ¬ used l ∧ ∀ u, ¬ used u → le l u
  let r :| ¬ used r ∧ r ≠ l ∧ ∀ u, ¬ used u ∧ u ≠ l → le r u
  require started ∧ ¬ busy
  require kind i = ksplit ∧ st i = trun ∧ todo i x
  err := err || decide (¬ cset cM x)
  let dl := if phase = splitMode ∨ phase = split then cSL else if phase = normal then cM else cN
  let dr := if phase = splitMode ∨ phase = split then cSR else if phase = normal then cM else cN
  if side x = sLR then
    require ∀ t, ¬ lockBy cM t
    require nextId + 1 ≤ maxIds
    used l := true
    used r := true
    side l := sL
    side r := sR
    cset cM x := false
    if cst dl = active ∨ cst dl = merging then
      cset dl l := true
    if cst dr = active ∨ cst dr = merging then
      cset dr r := true
    lost := lost || decide (¬ (cst dl = active ∨ cst dl = merging) ∨ ¬ (cst dr = active ∨ cst dr = merging))
    nextId := nextId + 2
  else
    require bypassTakesLock → ∀ t, ¬ lockBy cM t
    let d := if side x = sL then dl else dr
    cset cM x := false
    if cst d = active ∨ cst d = merging then
      cset d x := true
    lost := lost || decide (¬ (cst d = active ∨ cst d = merging))
  todo i x := false
}

action split_done (i : tid) {
  require started ∧ ¬ busy
  require kind i = ksplit ∧ st i = trun ∧ ∀ s, ¬ todo i s
  st i := tdone
  compacting X := decide (compacting X ∧ ¬ tin i X)
}

-- SplitComplete: the new tablet count, once M is empty and nothing is being added to it.
action split_complete {
  require started ∧ ¬ busy
  require phase = splitMode
  require ∀ s, ¬ cset cM s
  require ∀ s, ¬ addp s cM
  phase := split
  cst cM := stopped
  -- AbortOn(M)
  compacting X := decide (compacting X ∧
    ¬ ∃ t, kind t = regular ∧ tc t = cM ∧ (st t = tsnap ∨ st t = trun ∨ st t = tgc) ∧ tin t X)
  lockBy cM T := decide (lockBy cM T ∧ ¬ (kind T = regular ∧ (st T = tsnap ∨ st T = trun ∨ st T = tgc)))
  st T := if kind T = regular ∧ tc T = cM ∧ (st T = tsnap ∨ st T = trun ∨ st T = tgc) then taborted else st T
}

action merge_swap {
  require started ∧ ¬ busy
  require phase = split
  phase := merged
  cst cSL := merging
  cst cSR := merging
  cst cN := active
  cdis cN := true
}

-- FiberStop(g): stop a merging group and move its sstables into N.
action fiber_stop (g : cgid) {
  require started ∧ ¬ busy
  require phase = merged ∧ cst g = merging
  require ∀ s, ¬ addp s g
  require ∀ t, ¬ (tc t = g ∧ kind t ≠ regular ∧ (st t = tsnap ∨ st t = trun ∨ st t = tgc))
  -- AbortOn(g)
  compacting X := decide (compacting X ∧
    ¬ ∃ t, kind t = regular ∧ tc t = g ∧ (st t = tsnap ∨ st t = trun ∨ st t = tgc) ∧ tin t X)
  lockBy g T := decide (lockBy g T ∧ ¬ (kind T = regular ∧ (st T = tsnap ∨ st T = trun ∨ st T = tgc)))
  st T := if kind T = regular ∧ tc T = g ∧ (st T = tsnap ∨ st T = trun ∨ st T = tgc) then taborted else st T
  let other := if g = cSL then cSR else cSL
  let last := cst other = stopped
  cset cN X := cset cN X || cset g X
  cst g := stopped
  cset g X := false
  if last then
    cdis cN := false
    phase := mergedDone
}

-- AddStart: streaming routes a new sstable of side `v` and holds the group's gate.
action add_start (v : sd) {
  let s :| ¬ used s ∧ ∀ u, ¬ used u → le s u
  require started ∧ ¬ busy
  require nA < maxAdds ∧ nextId ≤ maxIds
  require phase = split → v ≠ sLR
  let d := if phase = normal then cM else if phase = splitMode then (if v = sLR then cM else if v = sL then cSL else cSR)
    else if phase = split then (if v = sL then cSL else cSR) else cN
  require cst d = active
  used s := true
  side s := v
  addp s d := true
  nextId := nextId + 1
  nA := nA + 1
}

action add_do (s : sid) (c : cgid) {
  require started ∧ ¬ busy
  require addp s c
  if cst c = active ∨ cst c = merging then
    cset c s := true
  lost := lost || decide (¬ (cst c = active ∨ cst c = merging))
  addp s c := false
}

-- CrCapture(c, x): repair's component rewrite captures x without filtering
-- what is registered as compacting.
action cr_capture (c : cgid) (x : sid) {
  let i :| st i = unused ∧ ∀ u, st u = unused → le i u
  require started ∧ ¬ busy
  require nC < maxRewrites
  require cst c = active ∧ ∀ t, ¬ lockBy c t
  require ∀ t, ¬ (tc t = c ∧ kind t ≠ regular ∧ (st t = tsnap ∨ st t = trun ∨ st t = tgc))
  require cset c x
  compacting X := decide (compacting X ∧
    ¬ ∃ t, kind t = regular ∧ tc t = c ∧ (st t = tsnap ∨ st t = trun ∨ st t = tgc) ∧ tin t X)
  st T := if kind T = regular ∧ tc T = c ∧ (st T = tsnap ∨ st T = trun ∨ st T = tgc) then taborted else st T
  kind i := rewrite
  tc i := c
  tin i X := decide (X = x)
  st i := trun
  compacting x := true
  nC := nC + 1
}

action cr_do (i : tid) (x : sid) {
  let o :| ¬ used o ∧ ∀ u, ¬ used u → le o u
  require started ∧ ¬ busy
  require kind i = rewrite ∧ st i = trun ∧ nextId ≤ maxIds
  require tin i x
  require ∀ t, ¬ lockBy (tc i) t
  let c := tc i
  let v := side x
  let rNone := phase = split ∧ v = sLR
  let d := if phase = normal then cM else if phase = splitMode then (if v = sLR then cM else if v = sL then cSL else cSR)
    else if phase = split then (if v = sL then cSL else cSR) else cN
  err := err || decide (¬ cset c x)
  used o := true
  side o := v
  cset c x := false
  if ¬ rNone ∧ (cst d = active ∨ cst d = merging) then
    cset d o := true
  lost := lost || decide (rNone ∨ ¬ (cst d = active ∨ cst d = merging))
  compacting x := false
  st i := tdone
  nextId := nextId + 1
}

-- InputsFound.
safety [inputs_found] ¬ witness → ¬ err
-- NoLoss.
safety [no_loss] ¬ witness → ¬ lost ∧ (C ≠ D → ¬ (cset C S ∧ cset D S)) ∧ (cst C = stopped → ¬ cset C S)
safety [w_merged] witness → phase ≠ mergedDone

#gen_spec

-- MC_before_3f9d1c16c70.cfg: TLC finds InputsFound violated (7 states).
-- expect: violation inputs_found
#model_check compiled { sid := Fin 11, tid := Fin 4, selectHoldsLock := false, bypassTakesLock := true, gcPinned := true, maxRegular := 2, maxGc := 1, maxAdds := 1, maxRewrites := 1, maxIds := 9, witness := false }
-- MC_before_7420f936bbc.cfg: TLC finds InputsFound violated (9 states).
-- expect: violation inputs_found
#model_check compiled { sid := Fin 11, tid := Fin 4, selectHoldsLock := true, bypassTakesLock := false, gcPinned := true, maxRegular := 2, maxGc := 1, maxAdds := 1, maxRewrites := 1, maxIds := 9, witness := false }
-- MC_before_99d302fb483.cfg: TLC finds InputsFound violated (6 states).
-- expect: violation inputs_found
#model_check compiled { sid := Fin 11, tid := Fin 4, selectHoldsLock := true, bypassTakesLock := true, gcPinned := false, maxRegular := 2, maxGc := 1, maxAdds := 1, maxRewrites := 1, maxIds := 9, witness := false }
-- MC_current.cfg: TLC finds InputsFound violated (12 states; catalog #3).
-- expect: violation inputs_found
#model_check compiled { sid := Fin 11, tid := Fin 4, selectHoldsLock := true, bypassTakesLock := true, gcPinned := true, maxRegular := 2, maxGc := 1, maxAdds := 1, maxRewrites := 1, maxIds := 9, witness := false }
-- MC_witness_merged.cfg: TLC finds the merge completing (10 states).
-- expect: violation w_merged
#model_check compiled { sid := Fin 11, tid := Fin 3, selectHoldsLock := true, bypassTakesLock := true, gcPinned := true, maxRegular := 1, maxGc := 1, maxAdds := 1, maxRewrites := 1, maxIds := 9, witness := true }

end CompactionGroups
