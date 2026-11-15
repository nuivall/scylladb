/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

/-! # Tombstone GC in repair mode across a tablet merge (catalog #2)

../tombstone_gc_repair/tombstone_gc_repair.tla action for action.
Encoding changes, none of which changes the reachable states:
- logical time is an ordered type `tick`; `now` is the smallest tick not
  spent yet (TLC: a counter from 1);
- a time that may be 0 (`rt`, `gc`) is a relation holding at most one tick,
  none for 0;
- a cell `[k, ts, del, rep]` of replica `r` is `cell r k ts del rep`;
- a write's replica set `S` is two flags, at least one set.
-/

veil module TombstoneGcRepair

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

-- Compact(r, k, rep): keeps the newest cell of the class, or purges it.
action compact (r : replica) (k : key) (rep : Bool) {
  let top :| (∃ d, cell r k top d rep) ∧ ∀ y d, cell r k y d rep → le y top
  let topDel :| cell r k top topDel rep
  require nC < maxCompactions
  require ∃ y d, cell r k y d rep
  let purge := topDel ∧ (∃ g, gcAt k g ∧ le top g ∧ top ≠ g) ∧
    ∀ y d, cell r k y d (¬ rep) ∧ (rep → ¬ rep) → (le top y ∧ top ≠ y)
  cell r k T D rep := decide (T = top ∧ D = topDel ∧ ¬ purge)
  nC := nC + 1
}

-- NoResurrection: a read at CL=ALL returns what the newest write says.
safety [no_resurrection]
  (∃ r p, cell r K T false p ∧ ∀ r' y d p', cell r' K y d p' → le y T) ↔
  (wrote K T false ∧ ∀ y d, wrote K y d → le y T)

#gen_spec

-- MC_current.cfg: TLC finds NoResurrection violated.
#model_check compiled { tick := Fin 3, mergeMax := true, maxMerges := 1, maxWrites := 2, maxRepairs := 1, maxCompactions := 2 }

-- MC_merge_min.cfg: TLC passes, 6,889 states.
#model_check compiled { tick := Fin 3, mergeMax := false, maxMerges := 1, maxWrites := 2, maxRepairs := 1, maxCompactions := 2 }

-- MC_no_merge.cfg: TLC passes, 2,965 states.
#model_check compiled { tick := Fin 3, mergeMax := true, maxMerges := 0, maxWrites := 2, maxRepairs := 1, maxCompactions := 2 }

end TombstoneGcRepair
