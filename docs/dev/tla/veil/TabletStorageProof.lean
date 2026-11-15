/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

/-! # tablet_storage with both fixes: an inductive invariant (catalog #4, #6)

TabletStorage.lean with the fixed configuration assumed (a failed
completion deletes only the outputs it created; a restart voids a streaming
response; clones unsealed until attached; the split check and the attach are
atomic) and an inductive strengthening of its properties. The witnesses are
off (`witness = 0`).
-/

veil module TabletStorageProof

set_option maxRecDepth 100000
-- Instance synthesis for the conjunction of ~40 invariants needs more than the
-- default; without it every goal fails with "cannot translate Type".
set_option synthInstance.maxHeartbeats 2000000

enum shard = {s1, s2}
enum key = {kl, kr}
enum tab = {tT, tl, tr}
enum group = {main, gl, gr}
enum okind = {onone, ocompact, osplit, obypass}
enum tstage = {tnone, stream, readNew}
enum tphase = {pclone, pload, pattach, prespond, pdone}

type fid
instantiate tot : TotalOrder fid
open TotalOrder

param maxWrites : Nat
param maxOps : Nat
param maxRestarts : Nat
param maxFails : Nat
param maxMigrations : Nat
param bypassFailDeletesInput : Bool
param cloneUnsealed : Bool
param attachAtomic : Bool
param restartKeepsResponse : Bool
param witness : Nat

relation alloc : fid → Bool
relation fL : fid → Bool
relation fR : fid → Bool
relation disk : fid → Bool
relation sealed : fid → Bool
relation inMem : shard → fid → Bool
function grp : fid → group
individual count2 : Bool
individual needsSplit : Bool
function owner : tab → shard
individual ack : Bool
individual coordAck : Bool
individual opKind : okind
individual opShard : shard
relation opIn : fid → Bool
relation opOut : fid → Bool
function opOutGrp : fid → group
individual tst : tstage
individual tt : tab
individual tfrom : shard
individual tto : shard
individual tph : tphase
relation tclones : fid → Bool
individual tresp : Bool
relation mark : fid → Bool
individual busy : Bool
relation written : key → Bool
individual aborted : Bool
individual bootFailed : Bool
individual nW : Nat
individual nO : Nat
individual nR : Nat
individual nF : Nat
individual nM : Nat

#gen_state

after_init {
  alloc F := false
  fL F := false
  fR F := false
  disk F := false
  sealed F := false
  inMem S F := false
  grp F := main
  count2 := false
  needsSplit := false
  owner T := s1
  ack := false
  coordAck := false
  opKind := onone
  opShard := s1
  opIn F := false
  opOut F := false
  opOutGrp F := main
  tst := tnone
  tt := tT
  tfrom := s1
  tto := s1
  tph := pclone
  tclones F := false
  tresp := false
  mark F := false
  busy := false
  written K := false
  aborted := false
  bootFailed := false
  nW := 0
  nO := 0
  nR := 0
  nF := 0
  nM := 0
}

-- Place(keys): the compaction group a file with these keys goes to.
ghost relation placeL (f : fid) := (count2 ∨ (needsSplit ∧ ¬ (fL f ∧ fR f))) ∧ fL f
ghost relation placeR (f : fid) := (count2 ∨ (needsSplit ∧ ¬ (fL f ∧ fR f))) ∧ ¬ fL f ∧ fR f
-- InTablet(x, t).
ghost relation inTab (f : fid) (t : tab) := ¬ count2 ∨ (t = tl ∧ grp f = gl) ∨ (t = tr ∧ grp f = gr)
-- What a failed completion deletes.
ghost relation failDel (f : fid) := opOut f ∧ (bypassFailDeletesInput ∨ ¬ opIn f)
-- The clones still to split in Load.
ghost relation needs (f : fid) := tclones f ∧ ¬ count2 ∧ needsSplit ∧ fL f ∧ fR f

action write (k : key) {
  let f1 :| ¬ alloc f1 ∧ ∀ u, ¬ alloc u → le f1 u
  let f2 :| ¬ alloc f2 ∧ f2 ≠ f1 ∧ ∀ u, ¬ alloc u ∧ u ≠ f1 → le f2 u
  require ¬ busy
  require nW < maxWrites
  let t := if count2 then (if k = kl then tl else tr) else tT
  let mig := tst ≠ tnone ∧ tt = t
  let on1 := owner t = s1 ∨ (mig ∧ (tfrom = s1 ∨ tto = s1))
  let on2 := owner t = s2 ∨ (mig ∧ (tfrom = s2 ∨ tto = s2))
  let g := if count2 ∨ needsSplit then (if k = kl then gl else gr) else main
  if on1 then
    alloc f1 := true
    fL f1 := decide (k = kl)
    fR f1 := decide (k = kr)
    disk f1 := true
    sealed f1 := true
    inMem s1 f1 := true
    grp f1 := g
  if on2 then
    alloc f2 := true
    fL f2 := decide (k = kl)
    fR f2 := decide (k = kr)
    disk f2 := true
    sealed f2 := true
    inMem s2 f2 := true
    grp f2 := g
  written k := true
  nW := nW + 1
}

-- StartCompact(s, g): a compaction of a group with two or more sstables.
action start_compact (s : shard) (g : group) {
  let f :| ¬ alloc f ∧ ∀ u, ¬ alloc u → le f u
  require ¬ busy
  require opKind = onone ∧ nO < maxOps
  require ∃ x y, x ≠ y ∧ inMem s x ∧ grp x = g ∧ inMem s y ∧ grp y = g
  alloc f := true
  fL f := decide (∃ x, inMem s x ∧ grp x = g ∧ fL x)
  fR f := decide (∃ x, inMem s x ∧ grp x = g ∧ fR x)
  disk f := true
  sealed f := true
  opKind := ocompact
  opShard := s
  opIn X := decide (inMem s X ∧ grp X = g)
  opOut f := true
  opOutGrp f := g
  nO := nO + 1
}

-- StartSplit(s, x): a split compaction, or the bypass for a one-sided sstable.
action start_split (s : shard) (x : fid) {
  let f1 :| ¬ alloc f1 ∧ ∀ u, ¬ alloc u → le f1 u
  let f2 :| ¬ alloc f2 ∧ f2 ≠ f1 ∧ ∀ u, ¬ alloc u ∧ u ≠ f1 → le f2 u
  require ¬ busy
  require opKind = onone ∧ nO < maxOps
  require ¬ count2 ∧ needsSplit ∧ inMem s x ∧ grp x = main
  if fL x ∧ fR x then
    alloc f1 := true
    fL f1 := true
    disk f1 := true
    sealed f1 := true
    alloc f2 := true
    fR f2 := true
    disk f2 := true
    sealed f2 := true
    opKind := osplit
    opOut f1 := true
    opOutGrp f1 := gl
    opOut f2 := true
    opOutGrp f2 := gr
  else
    opKind := obypass
    opOut x := true
    opOutGrp x := if fL x then gl else gr
  opShard := s
  opIn x := true
  nO := nO + 1
}

-- Complete: swap the outputs in; if an input is gone, throw and delete.
action complete_op {
  require ¬ busy
  require opKind ≠ onone
  if ∀ x, opIn x → inMem opShard x then
    inMem opShard X := decide ((inMem opShard X ∧ ¬ opIn X) ∨ opOut X)
    grp X := if opOut X then opOutGrp X else if opIn X then main else grp X
    disk X := decide (disk X ∧ ¬ (opIn X ∧ ¬ opOut X))
    sealed X := decide (sealed X ∧ ¬ (opIn X ∧ ¬ opOut X))
  else
    disk X := decide (disk X ∧ ¬ failDel X)
    sealed X := decide (sealed X ∧ ¬ failDel X)
  opKind := onone
  opShard := s1
  opIn X := false
  opOut X := false
  opOutGrp X := main
}

-- FailCompletion: the completion throws before attaching.
action fail_completion {
  require ¬ busy
  require opKind ≠ onone ∧ nF < maxFails
  disk X := decide (disk X ∧ ¬ failDel X)
  sealed X := decide (sealed X ∧ ¬ failDel X)
  opKind := onone
  opShard := s1
  opIn X := false
  opOut X := false
  opOutGrp X := main
  nF := nF + 1
}

action split_decide {
  require ¬ busy
  require ¬ count2 ∧ ¬ needsSplit
  needsSplit := true
}

action do_ack {
  require ¬ busy
  require ¬ count2 ∧ needsSplit ∧ ¬ ack
  require ∀ s x, inMem s x → grp x ≠ main
  ack := true
  coordAck := true
}

action finalize {
  require ¬ busy
  require ¬ count2 ∧ needsSplit ∧ coordAck ∧ tst = tnone
  aborted := decide (aborted ∨ decide (∃ s x, inMem s x ∧ grp x = main))
  owner tl := owner tT
  owner tr := owner tT
  count2 := true
  needsSplit := false
  ack := false
  coordAck := false
}

action mig_start (t : tab) (dst : shard) {
  require ¬ busy
  require tst = tnone ∧ nM < maxMigrations
  require if count2 then t ≠ tT else t = tT
  require dst ≠ owner t
  tst := stream
  tt := t
  tfrom := owner t
  tto := dst
  tph := pclone
  tresp := false
  nM := nM + 1
}

-- Clone: mark the sources, then clone them one at a time, smallest first.
-- (A pick can't quantify over a formula that reads an individual such as
-- `tfrom`, hence the marks.)
action clone_begin {
  require ¬ busy
  require tst = stream ∧ tph = pclone
  mark X := decide (inMem tfrom X ∧ inTab X tt)
  busy := true
}

action clone_step {
  let x :| mark x ∧ ∀ y, mark y → le x y
  let c :| ¬ alloc c ∧ ∀ u, ¬ alloc u → le c u
  require busy ∧ tst = stream ∧ tph = pclone
  alloc c := true
  fL c := fL x
  fR c := fR x
  disk c := true
  sealed c := decide (¬ cloneUnsealed)
  tclones c := true
  mark x := false
}

action clone_finish {
  require busy ∧ tst = stream ∧ tph = pclone
  require ∀ y, ¬ mark y
  tph := pload
  busy := false
}

-- Load: the split of the clones that need it, one at a time.
action load_split_step {
  let c :| tclones c ∧ fL c ∧ fR c ∧ ∀ y, tclones y ∧ fL y ∧ fR y → le c y
  let f1 :| ¬ alloc f1 ∧ ∀ u, ¬ alloc u → le f1 u
  let f2 :| ¬ alloc f2 ∧ f2 ≠ f1 ∧ ∀ u, ¬ alloc u ∧ u ≠ f1 → le f2 u
  require tst = stream ∧ tph = pload
  require ¬ count2 ∧ needsSplit
  disk c := false
  sealed c := false
  tclones c := false
  alloc f1 := true
  fL f1 := true
  disk f1 := true
  sealed f1 := true
  tclones f1 := true
  alloc f2 := true
  fR f2 := true
  disk f2 := true
  sealed f2 := true
  tclones f2 := true
  busy := true
}

-- The end of Load: with AttachAtomic seal and attach at once, else go on to Attach.
action load_finish {
  require tst = stream ∧ tph = pload
  require ∀ y, ¬ needs y
  if attachAtomic then
    sealed X := decide (sealed X ∨ (tclones X ∧ disk X))
    inMem tto X := inMem tto X || tclones X
    grp X := if tclones X then (if placeL X then gl else if placeR X then gr else main) else grp X
    tclones X := false
    tph := prespond
  else
    tph := pattach
  busy := false
}

action attach {
  require ¬ busy
  require ¬ attachAtomic
  require tst = stream ∧ tph = pattach
  sealed X := decide (sealed X ∨ (tclones X ∧ disk X))
  inMem tto X := inMem tto X || tclones X
  grp X := if tclones X then (if placeL X then gl else if placeR X then gr else main) else grp X
  tclones X := false
  tph := prespond
}

action respond {
  require ¬ busy
  require tst = stream ∧ tph = prespond
  tph := pdone
  tresp := true
}

action read_new {
  require ¬ busy
  require tst = stream ∧ tresp
  tst := readNew
  owner tt := tto
}

-- Cleanup on `from`: stop its compaction, drop the tablet's sstables.
action cleanup {
  require ¬ busy
  require tst = readNew
  let stop := opKind ≠ onone ∧ opShard = tfrom
  mark X := decide (inMem tfrom X ∧ inTab X tt)
  disk X := decide (disk X ∧ ¬ mark X ∧ ¬ (stop ∧ opOut X ∧ ¬ opIn X))
  sealed X := decide (sealed X ∧ ¬ mark X ∧ ¬ (stop ∧ opOut X ∧ ¬ opIn X))
  inMem tfrom X := decide (inMem tfrom X ∧ ¬ mark X)
  grp X := if mark X then main else grp X
  mark X := false
  if stop then
    opKind := onone
    opShard := s1
    opIn X := false
    opOut X := false
    opOutGrp X := main
  tst := tnone
  tt := tT
  tfrom := s1
  tto := s1
  tph := pclone
  tresp := false
}

-- Restart: unsealed files go, every sealed one is loaded by the shard
-- serving reads of its tablet; an unanswered streaming is retried.
action restart {
  require ¬ busy
  require nR < maxRestarts
  bootFailed := decide (bootFailed ∨ decide (∃ f, sealed f ∧ count2 ∧ fL f ∧ fR f))
  inMem S X := decide (sealed X ∧ ¬ (count2 ∧ fL X ∧ fR X) ∧
    owner (if count2 then (if fL X then tl else tr) else tT) = S)
  grp X := if sealed X ∧ ¬ (count2 ∧ fL X ∧ fR X) then
      (if placeL X then gl else if placeR X then gr else main) else main
  disk X := sealed X
  if tst = stream ∧ (¬ tresp ∨ ¬ restartKeepsResponse) then
    tph := pclone
    tclones X := false
    tresp := false
  opKind := onone
  opShard := s1
  opIn X := false
  opOut X := false
  opOutGrp X := main
  ack := false
  nR := nR + 1
}

ghost relation hasKey (f : fid) (k : key) := (k = kl ∧ fL f) ∨ (k = kr ∧ fR f)

safety [durable] witness = 0 → written K → ∃ f, sealed f ∧ hasKey f K
safety [readable] witness = 0 → written K →
  ∃ f, inMem (owner (if count2 then (if K = kl then tl else tr) else tT)) f ∧ hasKey f K
safety [split_ack_holds] witness = 0 → (¬ count2 ∧ needsSplit ∧ coordAck) → sealed F → ¬ (fL F ∧ fR F)
safety [no_abort] witness = 0 → ¬ aborted
safety [boot_ok] witness = 0 → ¬ bootFailed
safety [w_finalized] witness = 1 → ¬ count2
safety [w_migrated_after_split] witness = 2 → ¬ (count2 ∧ tst = readNew)
safety [w_restart_with_response] witness = 3 → ¬ (tst = stream ∧ tresp ∧ nR > 0)

assumption [fixed] bypassFailDeletesInput = false ∧ cloneUnsealed = true ∧ attachAtomic = true ∧
  restartKeepsResponse = false ∧ witness = 0
-- (`fids_left`, a check that the instance has enough file ids for TLC's
-- bounds, is left out: it doesn't hold for every instance.)

-- Files: where a file can be, and that a fresh id is unused.
invariant [sealed_disk] sealed F → disk F
invariant [disk_alloc] disk F → alloc F
invariant [mem_sealed] inMem S F → sealed F
invariant [mem_one_shard] inMem s1 F ∧ inMem s2 F → False
invariant [alloc_keys] alloc F → fL F ∨ fR F
invariant [fresh] ¬ alloc F → ¬ fL F ∧ ¬ fR F ∧ ¬ opIn F ∧ ¬ opOut F ∧ ¬ tclones F ∧ ¬ mark F ∧ ¬ inMem S F
invariant [clone_disk] tclones F → disk F ∧ ¬ inMem S F
-- A sealed file is in a shard's memory, the output of the running compaction, or a clone.
invariant [sealed_accounted] sealed F → (∃ s, inMem s F) ∨ (opKind ≠ onone ∧ opOut F) ∨ tclones F

-- Compaction groups follow the keys.
invariant [grp_l] inMem S F ∧ grp F = gl → fL F ∧ ¬ fR F
invariant [grp_r] inMem S F ∧ grp F = gr → fR F ∧ ¬ fL F
invariant [two_keys_main] inMem S F ∧ fL F ∧ fR F → grp F = main
invariant [split_no_main] count2 → inMem S F → grp F ≠ main
invariant [split_one_key] count2 → sealed F → ¬ (fL F ∧ fR F)
invariant [split_clone_one_key] count2 → tclones F → ¬ (fL F ∧ fR F)
-- The ACK exists only between the split decision and finalization; after it,
-- no main group, so no two-key sstable.
invariant [ack_coord] ack → coordAck
invariant [coord_split] coordAck → ¬ count2 ∧ needsSplit
invariant [ack_no_main] ¬ count2 ∧ needsSplit ∧ coordAck → inMem S F → grp F ≠ main
invariant [two_key_clone_unsealed] tclones F ∧ fL F ∧ fR F → ¬ sealed F

-- The running compaction.
invariant [op_none] opKind = onone → ¬ opIn F ∧ ¬ opOut F
invariant [op_in_mem] opIn F → inMem opShard F
invariant [op_out_sealed] opOut F → sealed F
invariant [op_keeps_keys] opIn F ∧ hasKey F K → ∃ g, opOut g ∧ hasKey g K
invariant [op_new_out] opOut F ∧ ¬ opIn F → ¬ inMem S F ∧ ¬ tclones F
invariant [op_out_l] opOut F ∧ opOutGrp F = gl → fL F ∧ ¬ fR F
invariant [op_out_r] opOut F ∧ opOutGrp F = gr → fR F ∧ ¬ fL F
invariant [op_out_main] opOut F ∧ opOutGrp F = main → ∃ x, opIn x ∧ grp x = main
invariant [op_two_keys_main] opOut F ∧ fL F ∧ fR F → opOutGrp F = main

-- The intra-node migration.
invariant [mig_idle] tst = tnone → tph = pclone ∧ ¬ tresp ∧ ¬ busy ∧ ¬ tclones F ∧ ¬ mark F
invariant [mig_shards] tst ≠ tnone → tfrom ≠ tto
invariant [mig_tablet] tst ≠ tnone → (if count2 then tt ≠ tT else tt = tT)
invariant [mig_owner_stream] tst = stream → owner tt = tfrom
invariant [mig_owner_read_new] tst = readNew → owner tt = tto
invariant [mig_busy] busy → tst = stream ∧ (tph = pclone ∨ tph = pload)
invariant [mig_mark] mark F → busy ∧ tph = pclone ∧ inMem tfrom F ∧ inTab F tt
invariant [mig_clones] tclones F → tst = stream ∧ (tph = pload ∨ (tph = pclone ∧ busy))
invariant [mig_resp] tresp → tph = pdone
invariant [mig_read_new] tst = readNew → tph = pdone
-- Every written key of the migrating tablet is on `to`, or on its way there.
invariant [mig_loading] tst = stream ∧ (tph = pload ∨ (tph = pclone ∧ busy)) ∧ written K ∧ (if count2 then (if K = kl then tl else tr) else tT) = tt →
  (∃ f, inMem tto f ∧ hasKey f K) ∨ (∃ c, tclones c ∧ hasKey c K) ∨ (∃ x, mark x ∧ hasKey x K)
invariant [mig_loaded] tst = stream ∧ (tph = prespond ∨ tph = pdone) ∧ written K ∧ (if count2 then (if K = kl then tl else tr) else tT) = tt →
  ∃ f, inMem tto f ∧ hasKey f K

#gen_spec

-- The strengthening holds on MC_fixed.cfg's bounds.
#model_check compiled { fid := Fin 24, maxWrites := 2, maxOps := 2, maxRestarts := 1, maxFails := 1, maxMigrations := 1, bypassFailDeletesInput := false, cloneUnsealed := true, attachAtomic := true, restartKeepsResponse := false, witness := 0 }

-- For every instance and every execution.
#check_invariants

end TabletStorageProof
