/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

-- models: locator/tablets.cc, replica/tablets.cc, tombstone_gc.cc, repair/repair.cc
-- checked-against: 61d55c44f6a

/-! # Tombstone GC in repair mode across a tablet merge (catalog #2): proof of the fix

TombstoneGcRepair.lean with the fix assumed (a merge takes the earlier repair
time of the two tablets) and an inductive strengthening of NoResurrection.

../tombstone_gc_repair/tombstone_gc_repair.tla action for action.
Encoding changes, none of which changes the reachable states:
- logical time is an ordered type `tick`; `now` is the smallest tick not
  spent yet (TLC: a counter from 1);
- a time that may be 0 (`rt`, `gc`) is a relation holding at most one tick,
  none for 0;
- a cell `[k, ts, del, rep]` of replica `r` is `cell r k ts del rep`;
- a write's replica set `S` is two flags, at least one set.
-/

veil module TombstoneGcRepairProof

enum replica = {ra, rb}
enum key = {kL, kR}
enum tablet = {tL, tR}

type tick
instantiate tot : TotalOrder tick
open TotalOrder

param mergeMax : Bool
param maxMerges : Nat
param maxWrites : Nat
param maxRepairs : Nat
param maxCompactions : Nat

relation spent : tick → Bool
relation cell : replica → key → tick → Bool → Bool → Bool
relation rtAt : tablet → tick → Bool
individual merged : Bool
relation gcAt : key → tick → Bool
relation wrote : key → tick → Bool → Bool
individual nW : Nat
individual nR : Nat
individual nC : Nat

#gen_state

after_init {
  spent T := false
  cell R K T D P := false
  rtAt B T := false
  merged := false
  gcAt K T := false
  wrote K T D := false
  nW := 0
  nR := 0
  nC := 0
}

ghost relation tabletOf (k : key) (t : tablet) := (k = kL ∧ t = tL) ∨ (k = kR ∧ t = tR)

-- Write(k, del): at CL=ONE, to the replicas flagged.
action write (k : key) (del : Bool) (toA toB : Bool) {
  let now :| ¬ spent now ∧ ∀ u, ¬ spent u → le now u
  require nW < maxWrites
  require toA ∨ toB
  if toA then
    cell ra k now del false := true
  if toB then
    cell rb k now del false := true
  wrote k now del := true
  spent now := true
  nW := nW + 1
}

-- Repair(t): syncs the keys of its range to both replicas, marked repaired.
action repair (t : tablet) {
  let now :| ¬ spent now ∧ ∀ u, ¬ spent u → le now u
  require nR < maxRepairs
  require merged → t = tL
  cell R K T D P := if merged ∨ tabletOf K t then decide (P ∧ ∃ r q, cell r K T D q) else cell R K T D P
  rtAt B T := if merged ∨ B = t then decide (T = now) else rtAt B T
  gcAt K T := if merged ∨ tabletOf K t then decide (T = now) else gcAt K T
  spent now := true
  nR := nR + 1
}

-- Merge: the merged tablet's repair time, the later (the code) or the earlier
-- of the two, goes into the GC state, which keeps the later per token.
action merge {
  require ¬ merged ∧ maxMerges > 0
  -- mrt: the merged repair time; none stands for 0.
  rtAt B T := decide (if mergeMax then
      (rtAt tL T ∧ ∀ y, rtAt tR y → le y T) ∨ (rtAt tR T ∧ ∀ y, rtAt tL y → le y T)
    else
      (∃ y, rtAt tL y) ∧ (∃ y, rtAt tR y) ∧
      ((rtAt tL T ∧ ∀ y, rtAt tR y → le T y) ∨ (rtAt tR T ∧ ∀ y, rtAt tL y → le T y)))
  -- rtAt tL is the merged time here: assignments take effect in order.
  gcAt K T := decide ((gcAt K T ∧ ∀ y, rtAt tL y → le y T) ∨ (rtAt tL T ∧ ∀ y, gcAt K y → le y T))
  merged := true
}

-- Compact(r, k, rep), split for the proof into the step that keeps the newest
-- cell of the class and the one that purges it; together they are the same
-- transitions as TombstoneGcRepair.compact (the purge split by class, and the repaired one by replica).
action compact_keep (r : replica) (k : key) (rep : Bool) {
  let top :| (∃ d, cell r k top d rep) ∧ ∀ y d, cell r k y d rep → le y top
  let topDel :| cell r k top topDel rep
  require nC < maxCompactions
  require ∃ y d, cell r k y d rep
  let purge := topDel ∧ (∃ g, gcAt k g ∧ le top g ∧ top ≠ g) ∧
    ∀ y d, cell r k y d (¬ rep) ∧ (rep → ¬ rep) → (le top y ∧ top ≠ y)
  require ¬ purge
  cell r k T D rep := decide (T = top ∧ D = topDel)
  nC := nC + 1
}

action compact_purge_repaired_ra (k : key) {
  let r := ra
  let rep := true
  let top :| (∃ d, cell r k top d rep) ∧ ∀ y d, cell r k y d rep → le y top
  let topDel :| cell r k top topDel rep
  require nC < maxCompactions
  require ∃ y d, cell r k y d rep
  let purge := topDel ∧ (∃ g, gcAt k g ∧ le top g ∧ top ≠ g) ∧
    ∀ y d, cell r k y d (¬ rep) ∧ (rep → ¬ rep) → (le top y ∧ top ≠ y)
  require purge
  cell r k T D rep := false
  nC := nC + 1
}

action compact_purge_repaired_rb (k : key) {
  let r := rb
  let rep := true
  let top :| (∃ d, cell r k top d rep) ∧ ∀ y d, cell r k y d rep → le y top
  let topDel :| cell r k top topDel rep
  require nC < maxCompactions
  require ∃ y d, cell r k y d rep
  let purge := topDel ∧ (∃ g, gcAt k g ∧ le top g ∧ top ≠ g) ∧
    ∀ y d, cell r k y d (¬ rep) ∧ (rep → ¬ rep) → (le top y ∧ top ≠ y)
  require purge
  cell r k T D rep := false
  nC := nC + 1
}

action compact_purge_unrepaired (r : replica) (k : key) {
  let rep := false
  let top :| (∃ d, cell r k top d rep) ∧ ∀ y d, cell r k y d rep → le y top
  let topDel :| cell r k top topDel rep
  require nC < maxCompactions
  require ∃ y d, cell r k y d rep
  let purge := topDel ∧ (∃ g, gcAt k g ∧ le top g ∧ top ≠ g) ∧
    ∀ y d, cell r k y d (¬ rep) ∧ (rep → ¬ rep) → (le top y ∧ top ≠ y)
  require purge
  cell r k T D rep := false
  nC := nC + 1
}

-- NoResurrection, its two directions: what reads return was written last
-- (split by the class and replica of the cell read), and what was written last is returned.
safety [stored_was_written_repaired_ra]
  (cell ra K T false true ∧ ∀ y d p', (cell ra K y d p' ∨ cell rb K y d p') → le y T) →
  (wrote K T false ∧ ∀ y d, wrote K y d → le y T)
safety [stored_was_written_repaired_rb]
  (cell rb K T false true ∧ ∀ y d p', (cell ra K y d p' ∨ cell rb K y d p') → le y T) →
  (wrote K T false ∧ ∀ y d, wrote K y d → le y T)
safety [stored_was_written_unrepaired]
  (∃ r, cell r K T false false ∧ ∀ y d p', (cell ra K y d p' ∨ cell rb K y d p') → le y T) →
  (wrote K T false ∧ ∀ y d, wrote K y d → le y T)
safety [written_is_stored]
  (wrote K T false ∧ ∀ y d, wrote K y d → le y T) →
  (∃ r p, cell r K T false p ∧ ∀ r' y d p', cell r' K y d p' → le y T)

set_option veil.smt.timeout 600

assumption [fixed] mergeMax = false

-- Ticks: `spent` is a prefix of the order, and everything refers to spent ticks.
invariant [used_prefix] spent X ∧ ¬ spent Y → le X Y ∧ X ≠ Y
invariant [wrote_used] wrote K T D → spent T
invariant [gc_used] gcAt K T → spent T
invariant [rt_used] rtAt B T → spent T
invariant [gc_unique] gcAt K X ∧ gcAt K Y → X = Y
invariant [rt_unique] rtAt B X ∧ rtAt B Y → X = Y
invariant [wrote_unique] wrote K T D ∧ wrote Q T E → K = Q ∧ D = E
invariant [cell_wrote] cell R K T D P → wrote K T D
-- Writes and repairs take different ticks.
invariant [gc_not_write] wrote K T D ∧ gcAt Q T → False
invariant [rt_not_write] wrote K T D ∧ rtAt B T → False
-- With the fix a merge leaves the GC state alone: no repair time is above it.
invariant [rt_below_gc] rtAt B X ∧ (merged ∨ tabletOf K B) → ∃ g, gcAt K g ∧ le X g
-- The last repair of a key splits its cells: repaired ones are older, unrepaired newer.
invariant [unrepaired_after] cell R K T D false ∧ gcAt K G → le G T ∧ G ≠ T
invariant [repaired_before] cell R K T D true → ∃ g, gcAt K g ∧ le T g ∧ T ≠ g

-- `Y` is the newest write of `K` before its last repair `G`: every replica
-- holding repaired cells of `K` still has it, and if a replica has none, it
-- purged it, so it is a tombstone.
invariant [holds_newest] cell R K T D true ∧ gcAt K G ∧ wrote K Y E ∧ le Y G ∧ Y ≠ G ∧
  (∀ y e, wrote K y e ∧ le y G ∧ y ≠ G → le y Y) → cell R K Y E true
invariant [purged_tombstone] (∀ y e, ¬ cell S K y e true) ∧ gcAt K G ∧ wrote K Y E ∧ le Y G ∧ Y ≠ G ∧
  (∀ y e, wrote K y e ∧ le y G ∧ y ≠ G → le y Y) → E = true
-- No repaired cell is newer than the newest write before the repair.
invariant [repaired_not_newer] cell R K T D true ∧ gcAt K G ∧ wrote K Y E ∧ le Y G ∧ Y ≠ G ∧
  (∀ y e, wrote K y e ∧ le y G ∧ y ≠ G → le y Y) → le T Y
-- So a replica's newest repaired cell is that write.
invariant [newest_repaired_is_it] cell R K T D true ∧ (∀ y e, cell R K y e true → le y T) ∧
  gcAt K G ∧ wrote K Y E ∧ le Y G ∧ Y ≠ G ∧ (∀ y e, wrote K y e ∧ le y G ∧ y ≠ G → le y Y) →
  T = Y ∧ D = E
-- Hence no repaired cell anywhere is newer than a replica's newest repaired cell.
invariant [repaired_le_newest] cell R K Y E true ∧ cell S K T D true ∧ (∀ y e, cell S K y e true → le y T) →
  le Y T
-- The newest write after the repair is unrepaired on some replica.
invariant [newest_kept] wrote K T D ∧ (∀ y e, wrote K y e → le y T) ∧ (∀ g, gcAt K g → le g T ∧ g ≠ T) →
  ∃ r, cell r K T D false

#gen_spec

-- The strengthening holds on MC_merge_min.cfg's bounds.
-- expect: pass
#model_check interpreted { tick := Fin 3, mergeMax := false, maxMerges := 1, maxWrites := 2, maxRepairs := 1, maxCompactions := 2 }

-- For every instance and every execution.
-- expect: pass
#check_invariants

end TombstoneGcRepairProof
