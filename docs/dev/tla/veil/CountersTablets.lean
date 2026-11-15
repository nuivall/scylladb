/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

/-! # Counters across a tablet migration (catalog #11)

A translation of ../counters_tablets/counters_tablets.tla (configs
MC_no_migration and MC_migration: MaxIncs = 3) following Veil's tla-to-veil
skill. The TLA+ actions map one to one: `Inc(n, to)` is `inc`, `Advance` is
`advance`. One encoding change: `to \in SUBSET Nodes` becomes three `Bool`
parameters, since an action parameter can't be a set here. A cell
`<<value, clock>>` is the pair of functions `val`, `clk`.
-/

/-- `Merge` of the TLA+ spec: the newer of two shards, by clock, then by value. -/
public def lexMax (a b : Nat × Nat) : Nat × Nat :=
  if b.2 > a.2 ∨ (b.2 = a.2 ∧ b.1 > a.1) then b else a

veil module CountersTablets

enum node = {a, b, c}
enum cid = {r, nr, q}
enum stage_t = {idle, write_both, streamed, use_new, done}

param maxIncs : Nat
param migrate : Bool

function val : node → cid → Nat
function clk : node → cid → Nat
individual stage : stage_t
individual sent : Nat
individual ninc : Nat

#gen_state

after_init {
  val N I := 0
  clk N I := 0
  stage := idle
  sent := 0
  ninc := 0
}

-- WriteReplicas of the TLA+ spec.
ghost relation writeReplica (n : node) :=
  (stage = idle ∧ (n = a ∨ n = c)) ∨
  ((stage = write_both ∨ stage = streamed) ∧ (n = a ∨ n = b ∨ n = c)) ∨
  ((stage = use_new ∨ stage = done) ∧ (n = b ∨ n = c))

-- Inc(n, to): n leads an increment of its own shard, replicated to the nodes
-- of `to` (toA, toB, toC); the increment reaches c (or is led by it).
action inc (n : node) (toA toB toC : Bool) {
  require ninc < maxIncs
  require writeReplica n
  require toA → (writeReplica a ∧ n ≠ a)
  require toB → (writeReplica b ∧ n ≠ b)
  require toC → (writeReplica c ∧ n ≠ c)
  require n = c ∨ toC
  let i := if n = c then q else if n = a then r else if stage = done then r else nr
  let newv := val n i + 1
  let newc := clk n i + 1
  if n = a ∨ toA then
    let m := lexMax (val a i, clk a i) (newv, newc)
    val a i := m.1
    clk a i := m.2
  if n = b ∨ toB then
    let m := lexMax (val b i, clk b i) (newv, newc)
    val b i := m.1
    clk b i := m.2
  if n = c ∨ toC then
    let m := lexMax (val c i, clk c i) (newv, newc)
    val c i := m.1
    clk c i := m.2
  sent := sent + 1
  ninc := ninc + 1
}

-- Advance: the migration stages; streaming merges a's cells into b, cleanup zeroes a.
action advance {
  require migrate
  require stage ≠ done
  if stage = idle then
    stage := write_both
  else if stage = write_both then
    let mR := lexMax (val b r, clk b r) (val a r, clk a r)
    let mN := lexMax (val b nr, clk b nr) (val a nr, clk a nr)
    let mQ := lexMax (val b q, clk b q) (val a q, clk a q)
    val b r := mR.1
    clk b r := mR.2
    val b nr := mN.1
    clk b nr := mN.2
    val b q := mQ.1
    clk b q := mQ.2
    stage := streamed
  else if stage = streamed then
    stage := use_new
  else
    val a I := 0
    clk a I := 0
    stage := done
}

-- NoLostIncrement: per id the newest shard over all replicas, summed.
safety [no_lost_increment]
  (lexMax (lexMax (val a r, clk a r) (val b r, clk b r)) (val c r, clk c r)).1 +
  (lexMax (lexMax (val a nr, clk a nr) (val b nr, clk b nr)) (val c nr, clk c nr)).1 +
  (lexMax (lexMax (val a q, clk a q) (val b q, clk b q)) (val c q, clk c q)).1 = sent

#gen_spec

-- MC_no_migration.cfg: MaxIncs = 3, Migrate = FALSE; TLC: pass, 20 distinct states.
#model_check interpreted { maxIncs := 3, migrate := false }

-- MC_migration.cfg: MaxIncs = 3, Migrate = TRUE; TLC: NoLostIncrement violated.
#model_check interpreted { maxIncs := 3, migrate := true }

end CountersTablets
