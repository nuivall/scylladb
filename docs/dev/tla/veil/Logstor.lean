/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

-- models: replica/logstor/
-- checked-against: 61d55c44f6a

/-! # Logstor on one shard (catalog #8, #9)

../logstor/logstor.tla action for action. Encoding changes, none of which
changes the reachable states:
- a segment's records and a separator buffer are sets, not sequences: the
  records of one key in a segment are always in write order (the offset
  order recovery compares), a compaction writes `a` before `b`, so each
  sequence is a function of its set; a mixed segment's length is `cnt`;
- a write id is a value of the ordered sort `wid`, the smallest one not used
  yet (TLC: `nextTs`), and an id is used iff its timestamp is set;
- the index, `expected` and the active segment are relations holding at most
  one value, none for Nil or 0; an empty or mixed segment's group is `ka`
  (TLC: "");
- `check` selects the properties of each config (0: Correct and FreeIsFree,
  1: AgreesWithReconciliation, 2 and 3: the witnesses).
-/

veil module Logstor

set_option maxRecDepth 100000
set_option synthInstance.maxHeartbeats 2000000

enum key = {ka, kb}
enum kind = {mixed, full}

type seg
type wid
instantiate tot : TotalOrder wid
open TotalOrder

param maxRecs : Nat
param maxWrites : Nat
param maxCrashes : Nat
param maxTruncates : Nat
param maxMoves : Nat
param maxCompactions : Nat
param truncateFlushes : Bool
param check : Nat

relation valid : seg → Bool
function sseq : seg → Nat
function skind : seg → kind
function scg : seg → key
relation holds : seg → key → wid → Bool
function cnt : seg → Nat
relation idx : key → seg → wid → Bool
relation isFree : seg → Bool
relation actv : seg → Bool
-- The segments a discard zeroes (always empty between actions).
relation dz : seg → Bool
relation sb : key → wid → seg → Bool
relation owned : key → Bool
relation expd : key → wid → Bool
function wts : wid → Nat
function wval : wid → Nat
relation acked : key → wid → Bool
individual clock : Nat
individual nextSeq : Nat
individual nextTs : Nat
individual nc : Nat
individual nt : Nat
individual nm : Nat
individual nk : Nat
individual crashed : Bool

#gen_state

after_init {
  valid S := false
  sseq S := 0
  skind S := mixed
  scg S := ka
  holds S K I := false
  cnt S := 0
  idx K S I := false
  isFree S := true
  actv S := false
  dz S := false
  sb K I S := false
  owned K := true
  expd K I := false
  wts I := 0
  wval I := 0
  acked K I := false
  clock := 1
  nextSeq := 1
  nextTs := 1
  nc := 0
  nt := 0
  nm := 0
  nk := 0
  crashed := false
}

action alloc_active (s : seg) {
  require ∀ x, ¬ actv x
  require isFree s
  actv s := true
  isFree s := false
  valid s := true
  sseq s := nextSeq
  skind s := mixed
  scg s := ka
  holds s K I := false
  cnt s := 0
  nextSeq := nextSeq + 1
}

-- A full active segment is retired.
action retire_active (s : seg) {
  require actv s ∧ cnt s = maxRecs
  actv s := false
}

-- Write(k, tie, v): `tie` reuses the timestamp of the expected write; `v2`
-- picks the value 2 (else 1).
action write (k : key) (tie : Bool) (v2 : Bool) (a : seg) {
  let id :| wts id = 0 ∧ ∀ u, wts u = 0 → le id u
  let e :| expd k e ∨ ∀ x, ¬ expd k x
  require nextTs ≤ maxWrites
  require tie → expd k e
  require owned k
  require actv a
  require cnt a < maxRecs
  wts id := if tie then wts e else clock
  wval id := if v2 then 2 else 1
  acked k id := true
  clock := if tie then clock else clock + 1
  holds a k id := true
  cnt a := cnt a + 1
  idx k S I := decide (S = a ∧ I = id)
  sb k id a := true
  expd k I := decide (I = id)
  nextTs := nextTs + 1
}

-- SepFlush(g): the group's buffer goes to a new full segment `s`; the index
-- entries that still point at the copied records move with them.
action sep_flush (g : key) (s : seg) {
  require ∃ i x, sb g i x
  require isFree s
  valid s := true
  sseq s := nextSeq
  skind s := full
  scg s := g
  holds s K I := decide (K = g ∧ ∃ x, sb g I x)
  cnt s := 0
  isFree s := false
  nextSeq := nextSeq + 1
  idx K S I := decide ((idx K S I ∧ ¬ (K = g ∧ sb g I S)) ∨
    (K = g ∧ S = s ∧ ∃ x, idx K x I ∧ sb g I x))
  sb g I X := false
}

-- Compact(s): the live records of a full segment go to a new full segment `t`.
action compact (s : seg) (t : seg) {
  require valid s ∧ skind s = full
  require owned (scg s)
  require ∃ k i, idx k s i
  require ∀ g i, ¬ sb g i s
  require nk < maxCompactions
  require isFree t
  nk := nk + 1
  valid t := true
  sseq t := nextSeq
  skind t := full
  scg t := scg s
  holds t K I := idx K s I
  cnt t := 0
  isFree t := false
  nextSeq := nextSeq + 1
  idx K S I := decide ((idx K S I ∧ S ≠ s) ∨ (S = t ∧ idx K s I))
}

-- free_segment(): back on the free list, the disk untouched.
action free_seg (s : seg) {
  require ¬ isFree s
  require valid s
  require ¬ actv s ∧ (∀ g i, ¬ sb g i s) ∧ (∀ k i, ¬ idx k s i)
  isFree s := true
}

-- TRUNCATE: discard_segments() of every group: zero and free their full segments.
action truncate {
  require nt < maxTruncates
  nt := nt + 1
  idx K S I := false
  dz S := decide (valid S ∧ skind S = full)
  isFree S := isFree S || dz S
  holds S K I := holds S K I && !dz S
  valid S := valid S && !dz S
  sseq S := if dz S then 0 else sseq S
  skind S := if dz S then mixed else skind S
  scg S := if dz S then ka else scg S
  dz S := false
  if truncateFlushes then
    sb K I S := false
  expd K I := false
  acked K I := false
}

-- Leave(g): tablet cleanup on this shard.
action leave (g : key) {
  require owned g
  require nm < maxMoves
  nm := nm + 1
  owned g := false
  idx g S I := false
  dz S := decide (valid S ∧ skind S = full ∧ scg S = g)
  isFree S := isFree S || dz S
  holds S K I := holds S K I && !dz S
  valid S := valid S && !dz S
  sseq S := if dz S then 0 else sseq S
  skind S := if dz S then mixed else skind S
  scg S := if dz S then ka else scg S
  dz S := false
  sb g I S := false
}

-- Return(g): streaming writes the expected record into a new full segment `t`.
action return_tablet (g : key) (t : seg) {
  let e :| expd g e ∨ ∀ x, ¬ expd g x
  require ¬ owned g
  owned g := true
  if expd g e then
    require isFree t
    valid t := true
    sseq t := nextSeq
    skind t := full
    scg t := g
    holds t K I := decide (K = g ∧ I = e)
    cnt t := 0
    isFree t := false
    nextSeq := nextSeq + 1
    idx g S I := decide (S = t ∧ I = e)
}

-- Better(a, b) of recovery, as "a is at least as good as b" for records
-- (s, i) and (s2, i2) of one key.
ghost relation atLeast (s : seg) (i : wid) (s2 : seg) (i2 : wid) :=
  (s = s2 ∧ i = i2) ∨ wts i > wts i2 ∨ (wts i = wts i2 ∧ sseq s > sseq s2) ∨
  (wts i = wts i2 ∧ s = s2 ∧ le i2 i ∧ i ≠ i2)

-- Crash and recovery: rebuild the index from every valid segment, separate
-- the live records of mixed segments again, free what nothing uses.
action crash {
  require nc < maxCrashes
  nc := nc + 1
  crashed := true
  idx K S I := decide (owned K ∧ valid S ∧ holds S K I ∧
    ∀ s2 i2, valid s2 ∧ holds s2 K i2 → atLeast S I s2 i2)
  sb K I S := decide (idx K S I ∧ skind S = mixed)
  isFree S := decide (¬ valid S ∨ ∀ k i, ¬ idx k S i)
  actv S := false
}

-- Correct: reads of an owned key return the newest acknowledged write.
safety [correct] check = 0 → owned K →
  ((∀ s i, ¬ idx K s i) ∧ (∀ e, ¬ expd K e)) ∨
  (∃ s i e, idx K s i ∧ expd K e ∧ wts i = wts e ∧ wval i = wval e)
-- FreeIsFree.
safety [free_is_free] check = 0 → isFree S → (∀ k i, ¬ idx k S i) ∧ (∀ g i, ¬ sb g i S) ∧ ¬ actv S
-- AgreesWithReconciliation: the highest timestamp, then the larger value.
safety [agrees_with_reconciliation] check = 1 → owned K →
  ((∀ s i, ¬ idx K s i) ∧ (∀ w, ¬ acked K w)) ∨
  (∃ s i w, idx K s i ∧ acked K w ∧ wval i = wval w ∧ (∀ x, acked K x → wts x ≤ wts w) ∧
    (∀ x, acked K x ∧ wts x = wts w → wval x ≤ wval w))
safety [w_crash_after_truncate] check = 2 → ¬ (crashed ∧ nt > 0 ∧ nextTs > 2)
safety [w_crash_after_return] check = 3 → ¬ (crashed ∧ nm > 0 ∧ owned ka ∧ owned kb ∧ nextTs > 2)

#gen_spec

-- MC_move.cfg: TLC finds Correct violated (6 states).
-- expect: violation correct
#model_check compiled { seg := Fin 3, wid := Fin 3, maxRecs := 2, maxWrites := 3, maxCrashes := 1, maxTruncates := 0, maxMoves := 1, maxCompactions := 1, truncateFlushes := true, check := 0 }
-- MC_ties.cfg: TLC finds Correct violated (6 states).
-- expect: violation correct
#model_check compiled { seg := Fin 3, wid := Fin 2, maxRecs := 2, maxWrites := 2, maxCrashes := 1, maxTruncates := 0, maxMoves := 0, maxCompactions := 1, truncateFlushes := true, check := 0 }
-- MC_ties_reconcile.cfg: TLC finds AgreesWithReconciliation violated (4 states).
-- expect: violation agrees_with_reconciliation
#model_check compiled { seg := Fin 3, wid := Fin 2, maxRecs := 2, maxWrites := 2, maxCrashes := 1, maxTruncates := 0, maxMoves := 0, maxCompactions := 1, truncateFlushes := true, check := 1 }
-- MC_truncate.cfg: TLC finds Correct violated (5 states with one worker; 6
-- with 8, whose search order doesn't guarantee the shortest trace).
-- expect: violation correct
#model_check compiled { seg := Fin 3, wid := Fin 2, maxRecs := 2, maxWrites := 2, maxCrashes := 1, maxTruncates := 1, maxMoves := 0, maxCompactions := 1, truncateFlushes := true, check := 0 }
-- The witnesses: TLC finds each reachable (6 and 7 states).
-- expect: violation w_crash_after_truncate
#model_check compiled { seg := Fin 3, wid := Fin 2, maxRecs := 2, maxWrites := 2, maxCrashes := 1, maxTruncates := 1, maxMoves := 0, maxCompactions := 1, truncateFlushes := true, check := 2 }
-- expect: violation w_crash_after_return
#model_check compiled { seg := Fin 3, wid := Fin 3, maxRecs := 2, maxWrites := 3, maxCrashes := 1, maxTruncates := 0, maxMoves := 1, maxCompactions := 1, truncateFlushes := true, check := 3 }

-- Every feature, no property (check := 9): TLC explores 21,659,782 distinct
-- states at these bounds (Segs = {1, 2, 3}, MaxRecs = 2, MaxWrites = 3, one
-- crash, TRUNCATE, move and compaction, TruncateFlushes = TRUE).
-- expect: pass
-- cost: heavy
#model_check compiled { seg := Fin 3, wid := Fin 3, maxRecs := 2, maxWrites := 3, maxCrashes := 1, maxTruncates := 1, maxMoves := 1, maxCompactions := 1, truncateFlushes := true, check := 9 }

end Logstor
