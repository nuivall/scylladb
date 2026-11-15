/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

/-! # Commitlog cleanup record, as TLC sees it (benchmark)

../commitlog_cleanup/commitlog_cleanup.tla action for action, with the
`Fixed` constant, for benchmarking against TLC: replay positions are
natural numbers handed out by a counter, as in the TLA+ spec, so the
explored states match TLC's (CommitlogCleanup.lean abstracts positions
instead, for proofs). `sealed` is always empty in the TLA+ spec and is left
out.
-/

veil module CommitlogCleanupBench

type write

param maxCrashes : Nat
param fixed : Bool

function rp : write → Nat
individual nextRp : Nat
relation active : write → Bool
relation sstables : write → Bool
individual flushedRp : Nat
individual record : Nat
relation acked : write → Bool
relation lost : write → Bool
individual nc : Nat

#gen_state

after_init {
  rp X := 0
  nextRp := 1
  active X := false
  sstables X := false
  flushedRp := 0
  record := 0
  acked X := false
  lost X := false
  nc := 0
}

action add_to_commitlog (w : write) {
  require rp w = 0
  rp w := nextRp
  nextRp := nextRp + 1
}

action apply_in_memory (w : write) {
  require rp w ≠ 0 ∧ ¬ acked w
  active w := true
  acked w := true
}

-- Seal and flush the active memtable in one step.
action flush {
  require ∃ w, active w
  let top :| active top ∧ ∀ w, active w → rp w ≤ rp top
  flushedRp := if rp top > flushedRp then rp top else flushedRp
  sstables X := sstables X || active X
  active X := false
}

-- The record: the highest flushed position, or with the fix the highest
-- one below every write not yet in an sstable.
action save_record {
  -- `low`: the unflushed write with the lowest position, any write if none.
  let low :| (rp low ≠ 0 ∧ ¬ sstables low ∧ ∀ w, rp w ≠ 0 ∧ ¬ sstables w → rp low ≤ rp w) ∨
             (∀ w, ¬ (rp w ≠ 0 ∧ ¬ sstables w))
  let target := if fixed ∧ rp low ≠ 0 ∧ ¬ sstables low ∧ rp low - 1 < flushedRp then rp low - 1 else flushedRp
  require target > record
  record := target
}

action crash {
  require nc < maxCrashes
  nc := nc + 1
  lost X := decide (acked X ∧ ¬ sstables X ∧ rp X ≤ record)
}

safety [no_lost_write] ¬ lost X

#gen_spec

-- MC_current.cfg and MC_fixed.cfg: TLC finds NoLostWrite violated, and passes with 70 states.
#model_check compiled { write := Fin 2, maxCrashes := 1, fixed := false }
#model_check compiled { write := Fin 2, maxCrashes := 1, fixed := true }

-- MC_bench_{bug,fix}_{5,6,7}.cfg.
#model_check compiled { write := Fin 5, maxCrashes := 1, fixed := false }
#model_check compiled { write := Fin 5, maxCrashes := 1, fixed := true }
#model_check compiled { write := Fin 6, maxCrashes := 1, fixed := false }
#model_check compiled { write := Fin 6, maxCrashes := 1, fixed := true }
#model_check compiled { write := Fin 7, maxCrashes := 1, fixed := false }
#model_check compiled { write := Fin 7, maxCrashes := 1, fixed := true }

end CommitlogCleanupBench
