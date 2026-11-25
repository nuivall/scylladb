/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

-- models: service/storage_service.cc, service/topology_coordinator.cc, service/tablet_allocator.cc, db/system_keyspace.cc, main.cc
-- checked-against: 61d55c44f6a

/-! # The vnodes-to-tablets migration (catalog #7)

A translation of ../vnodes_tablets_migration/vnodes_tablets_migration.tla,
action for action, to benchmark Veil against TLC on the same model.
Encoding changes, none of which changes the reachable states:
- `towner` is all `None` or a copy of `vowner`, so it is a flag `mapped`
  and a function that is reset to `a` while the flag is off;
- a write id is a value of the ordered type `wid`, the smallest one not
  used yet (TLC: `nw + 1`); `wid` has `MaxWrites` values;
- the joiner `c` always exists; with `joiner` off it is never a member, so
  nothing of it changes (TLC: `Nodes = {"a", "b"}`).
-/

veil module VnodesTabletsMigration

enum node = {a, b, c}
enum range = {r1, r2}
enum mode = {unset, vn, tb}

type wid
instantiate tot : TotalOrder wid
open TotalOrder

param joiner : Bool
param maxWrites : Nat
param maxRestarts : Nat
param maxStops : Nat
param maxLeaderChanges : Nat
param currentColumn : Bool
-- MC_topology_ops and the bench configs leave BootOK out.
param checkBootOk : Bool

relation isMember : node → Bool
relation up : node → Bool
function vowner : range → node
individual mapped : Bool
function towner : range → node
individual ksTablets : Bool
function layout : node → mode
function intent : node → mode
function linten : node → mode
function cur : node → mode
function stats : node → mode
relation data : node → range → wid → Bool
relation written : range → wid → Bool
relation stuck : node → Bool
individual usedTablets : Bool
individual rolledBack : Bool
individual nw : Nat
individual nr : Nat
individual ns : Nat
individual nl : Nat

#gen_state

after_init {
  isMember N := decide (N = a ∨ N = b)
  up N := decide (N = a ∨ N = b)
  vowner R := if R = r1 then a else b
  mapped := false
  towner R := a
  ksTablets := false
  layout N := vn
  intent N := unset
  linten N := unset
  cur N := unset
  stats N := unset
  data N R X := false
  written R X := false
  stuck N := false
  usedTablets := false
  rolledBack := false
  nw := 0
  nr := 0
  ns := 0
  nl := 0
}

ghost relation migrating := mapped ∧ ¬ ksTablets

-- Write(r): to the replica the coordinator routes it to.
action write (rg : range) (w : wid) {
  require nw < maxWrites
  require ∀ v, ¬ (written r1 v ∨ written r2 v) → le w v
  require ¬ (written r1 w ∨ written r2 w)
  let o := if ksTablets then towner rg else vowner rg
  require up o
  require layout o = tb → (mapped ∧ towner rg = o)
  nw := nw + 1
  data o rg w := true
  written rg w := true
}

action prepare {
  require ¬ ksTablets
  require ¬ mapped
  mapped := true
  towner R := vowner R
  if currentColumn then
    cur N := if isMember N then vn else cur N
}

-- Mark(n, m, applied).
action mark (n : node) (m : mode) (applied : Bool) {
  require up n ∧ isMember n
  require m ≠ unset
  require migrating
  require intent n ≠ m
  intent n := m
  if applied then
    linten n := m
  else
    require ns < maxStops
    up n := false
    ns := ns + 1
}

action stop (n : node) {
  require up n
  require ns < maxStops
  up n := false
  ns := ns + 1
}

action boot (n : node) {
  let md := if ksTablets then tb else if migrating ∧ linten n = tb then tb else vn
  let forward := ¬ ksTablets ∧ md = tb
  let ok := if forward then (∀ rg w, data n rg w → (mapped ∧ towner rg = n))
            else if ksTablets then layout n = tb else True
  require isMember n ∧ ¬ up n
  require ¬ stuck n
  require nr < maxRestarts
  nr := nr + 1
  if ok then
    up n := true
    layout n := md
    linten n := intent n
    if currentColumn ∧ intent n ≠ unset then
      cur n := md
    usedTablets := usedTablets || decide (md = tb)
  else
    stuck n := true
}

action refresh_stats {
  require ∃ n, stats n ≠ (if up n then layout n else stats n)
  stats N := if up N then layout N else stats N
}

action leader_change {
  require nl < maxLeaderChanges
  nl := nl + 1
  stats N := unset
}

action finalize_forward {
  require migrating
  require ∀ n, isMember n →
    (intent n = tb ∧ (if up n then layout n else stats n) = tb ∧ (cur n = unset ∨ cur n = tb))
  stats N := if up N then layout N else stats N
  ksTablets := true
  intent N := unset
  cur N := unset
}

action finalize_rollback {
  require migrating
  require ∀ n, isMember n →
    ((intent n = unset ∨ intent n = vn) ∧ (if up n then layout n else stats n) ≠ tb ∧ (cur n = unset ∨ cur n = vn))
  stats N := if up N then layout N else stats N
  rolledBack := usedTablets
  mapped := false
  towner R := a
  intent N := unset
  cur N := unset
}

-- Bootstrap(c): c takes r2 over from its vnode replica, streaming its data.
action bootstrap {
  require joiner
  require ¬ isMember c
  require ¬ ksTablets
  let o := vowner r2
  require up o
  isMember c := true
  up c := true
  vowner r2 := c
  data c r2 X := data o r2 X
}

-- Cleanup(n): drops the ranges n no longer replicates.
action cleanup (n : node) {
  require up n
  require ¬ ksTablets
  require layout n = vn
  require ∃ rg w, vowner rg ≠ n ∧ data n rg w
  data n R X := data n R X ∧ vowner R = n
}

safety [durable] written R X → ∃ n, data n R X
safety [readable] written R X →
  up (if ksTablets then towner R else vowner R) → data (if ksTablets then towner R else vowner R) R X
safety [layout_matches] ksTablets → ∀ n, isMember n → layout n = tb
safety [boot_ok] checkBootOk → ¬ stuck N

#gen_spec

-- MC_before_current_column.cfg: TLC finds LayoutMatches violated.
-- expect: violation layout_matches
#model_check compiled { wid := Fin 2, joiner := false, maxWrites := 2, maxRestarts := 4, maxStops := 4, maxLeaderChanges := 1, currentColumn := false, checkBootOk := true }

-- MC_current.cfg: TLC passes, 288,498 states.
-- expect: pass
#model_check compiled { wid := Fin 2, joiner := false, maxWrites := 2, maxRestarts := 4, maxStops := 4, maxLeaderChanges := 1, currentColumn := true, checkBootOk := true }

-- MC_topology_ops.cfg: TLC passes, 5,428,990 states.
-- expect: pass
-- cost: heavy
#model_check compiled { wid := Fin 2, joiner := true, maxWrites := 2, maxRestarts := 3, maxStops := 3, maxLeaderChanges := 1, currentColumn := true, checkBootOk := false }

-- MC_bench_bug_large.cfg: TLC finds LayoutMatches violated.
-- expect: violation layout_matches
-- cost: heavy
#model_check compiled { wid := Fin 2, joiner := true, maxWrites := 2, maxRestarts := 4, maxStops := 4, maxLeaderChanges := 1, currentColumn := false, checkBootOk := false }

-- MC_bench_fix_large.cfg: TLC passes, 17,430,982 states.
-- expect: pass
-- cost: heavy
#model_check compiled { wid := Fin 2, joiner := true, maxWrites := 2, maxRestarts := 4, maxStops := 4, maxLeaderChanges := 1, currentColumn := true, checkBootOk := false }

end VnodesTabletsMigration
