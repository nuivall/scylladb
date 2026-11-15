/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

/-! # Invariant synthesis for the fixed commitlog cleanup record

A Veil model of ../commitlog_cleanup/commitlog_cleanup.tla. A write takes its
replay position when the commitlog adds it (`add`) and reaches the memtable in
a later step (`apply`); nothing orders the two steps across writes. A flush
moves the memtable to sstables. A tablet repair saves a commitlog cleanup
record (`save_record`); after a crash, replay skips the writes at or below it,
so an acknowledged write that is neither in an sstable nor above the record is
lost.

Replay positions are an uninterpreted, totally ordered sort `pos`; a new
position is above every position handed out before (the commitlog's
monotonic allocation). The second module is the fix: the record stays below
the position of every write not yet in an sstable.
-/

veil module CommitlogCleanupUpdr

type write
type pos
instantiate tot : TotalOrder pos
open TotalOrder

relation added : write → Bool
function rp : write → pos
relation acked : write → Bool
relation active : write → Bool
relation flushed : write → Bool
individual hasRecord : Bool
individual record : pos
relation lost : write → Bool

#gen_state

after_init {
  added X := false
  acked X := false
  active X := false
  flushed X := false
  hasRecord := false
  lost X := false
}

-- The commitlog add: a position above every position handed out.
action add (w : write) (p : pos) {
  require ¬ added w
  require ∀ v, added v → (le (rp v) p ∧ rp v ≠ p)
  require hasRecord → (le record p ∧ record ≠ p)
  added w := true
  rp w := p
}

-- The memtable apply, in any order relative to other writes' adds.
action apply (w : write) {
  require added w ∧ ¬ acked w
  acked w := true
  active w := true
}

-- Seal and flush the memtable.
action flush {
  flushed X := flushed X || active X
  active X := false
}

-- A tablet repair saves the record at the highest flushed position, below every write not in an sstable (the fix).
action save_record (w : write) {
  require flushed w
  require ∀ v, flushed v → le (rp v) (rp w)
  require ∀ v, added v ∧ ¬ flushed v → (le (rp w) (rp v) ∧ rp w ≠ rp v)
  hasRecord := true
  record := rp w
}

-- Crash and replay: skipped writes not in an sstable are lost.
action crash {
  lost X := decide (acked X ∧ ¬ flushed X ∧ hasRecord ∧ le (rp X) record)
}

safety [no_lost_write] ¬ lost X

#gen_spec

#updr

end CommitlogCleanupUpdr
