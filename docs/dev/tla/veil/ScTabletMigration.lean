/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

/-! # Migration of one strongly-consistent tablet (catalog #5)

../sc_tablet_migration/sc_tablet_migration.tla action for action, with
Hosts = {h1, h2, h3}, InitReplicas = {h1, h2}. Encoding changes:
- the published states are values of the ordered sort `pid` (`pubd`, with
  per-state fields); a new state takes the smallest unpublished one; the
  current one, each host's view and each request's view are relations
  holding one pid (set by a `setup` step that publishes TLC's initial state
  and picks its leader);
- NoHost is a cleared flag (`hasLeader`, `hasKnown`, `pLs`, `pPs`);
- the sync handlers and cleanup RPCs are relations over their record
  fields, as TLC's sets of records are;
- `Steps(s)` is at most a barrier then one step: `hasBar` and `restIs`;
  SkipBarrier is at most one stage (`skipOn`, `skipStage`), ExtraBarrier at
  most {sc_rollback} (`extraRollback`), as in every config;
- the request slots are `rslot`; with MaxRequests = 0 there is one slot
  that `pin` never fills;
- `check` selects the properties: 0 all eight invariants, 1 I1 only, 2
  SyncedHolds only, 10 to 14 the witnesses.
TLC uses a VIEW and symmetry, this copy doesn't, so state counts differ; the
check is the same verdict on every config.
-/

veil module ScTabletMigration

set_option maxRecDepth 100000
set_option synthInstance.maxHeartbeats 2000000

enum host = {h1, h2, h3}
enum stage = {gnone, gstart, gaddnv, gsnap, gbecome, gusenew, gcleanup, gend, grollback, gremove, gctarget, grevert}
enum stepk = {kbar, ksync, ktransfer, kcleanup, knil}
enum sst = {ysent, yrunning, ydone, yrefused, yaborted}
enum kslot = {k1, k2}
enum rqst = {runused, rpinned, rinside, rdone}

type pid
instantiate tot : TotalOrder pid
type rslot
open TotalOrder

param skipOn : Bool
param skipStage : stage
param extraRollback : Bool
param guardAbortsSync : Bool
param newerSyncSupersedes : Bool
param excludedPendingAlive : Bool
param maxMigrations : Nat
param maxFailures : Nat
param maxElections : Nat
param maxRequests : Nat
param check : Nat

individual started : Bool
relation pubd : pid → Bool
relation curR : pid → Bool
function pmig : pid → Nat
function pstage : pid → stage
relation pC : pid → host → Bool
relation pnext : pid → host → Bool
function pL : pid → host
relation pLs : pid → Bool
function pP : pid → host
relation pPs : pid → Bool
relation vw : host → pid → Bool
relation cv : host → Bool
relation cnv : host → Bool
individual leader : host
individual hasLeader : Bool
function known : host → host
relation hasKnown : host → Bool
relation storage : host → Bool
relation caughtUp : host → Bool
individual step : Nat
relation syn : host → pid → sst → Bool
relation rpc : host → pid → kslot → sst → Bool
function reqH : rslot → host
relation reqV : rslot → pid → Bool
function reqSt : rslot → rqst
relation excluded : host → Bool
relation dead : host → Bool
individual promoted : Bool
individual nMig : Nat
individual nFail : Nat
individual nElect : Nat

#gen_state

after_init {
  started := false
  pubd P := false
  curR P := false
  pmig P := 0
  pstage P := gnone
  pC P H := false
  pnext P H := false
  pL P := h1
  pLs P := false
  pP P := h1
  pPs P := false
  vw H P := false
  cv H := false
  cnv H := false
  leader := h1
  hasLeader := false
  known H := h1
  hasKnown H := false
  storage H := false
  caughtUp H := false
  step := 1
  syn H P S := false
  rpc H P K S := false
  reqH R := h1
  reqV R P := false
  reqSt R := runused
  excluded H := false
  dead H := false
  promoted := false
  nMig := 0
  nFail := 0
  nElect := 0
}

-- The per-stage tables of the design doc.
ghost relation serving (p : pid) (h : host) :=
  ((pstage p = gnone ∨ pstage p = gstart ∨ pstage p = gaddnv ∨ pstage p = gsnap ∨
    pstage p = gremove ∨ pstage p = gctarget ∨ pstage p = grevert) ∧ pC p h) ∨
  ((pstage p = gbecome ∨ pstage p = grollback) ∧ (pC p h ∨ (pPs p ∧ pP p = h))) ∨
  ((pstage p = gusenew ∨ pstage p = gcleanup ∨ pstage p = gend) ∧ pnext p h)
ghost relation hostsGroup (h : host) (p : pid) :=
  (pstage p = gnone ∧ pC p h) ∨
  ((pstage p = gstart ∨ pstage p = gaddnv ∨ pstage p = gsnap ∨ pstage p = gbecome ∨
    pstage p = gusenew ∨ pstage p = grollback ∨ pstage p = gremove) ∧ (pC p h ∨ (pPs p ∧ pP p = h))) ∨
  ((pstage p = gcleanup ∨ pstage p = gend) ∧ pnext p h) ∨
  ((pstage p = gctarget ∨ pstage p = grevert) ∧ pC p h)
ghost relation expV (p : pid) (h : host) :=
  ((pstage p = gnone ∨ pstage p = gstart ∨ pstage p = gremove ∨ pstage p = gctarget ∨
    pstage p = grevert ∨ pstage p = gaddnv ∨ pstage p = gsnap ∨ pstage p = grollback) ∧ pC p h) ∨
  ((pstage p = gbecome ∨ pstage p = gusenew ∨ pstage p = gcleanup ∨ pstage p = gend) ∧ pnext p h)
ghost relation expNV (p : pid) (h : host) :=
  ((pstage p = gaddnv ∨ pstage p = gsnap ∨ pstage p = grollback) ∧ pPs p ∧ pP p = h) ∨
  (pstage p = gbecome ∧ pLs p ∧ pL p = h)
ghost relation cfgIsExp (p : pid) := ∀ h, (cv h ↔ expV p h) ∧ (cnv h ↔ expNV p h)
ghost relation runningH (h : host) := ¬ dead h ∧ ∃ p, vw h p ∧ hostsGroup h p
-- Quorum(cfg) and Quorum(Expected(p)).
ghost relation quorumNow :=
  2 * ((if cv h1 ∧ ¬ dead h1 then 1 else 0) + (if cv h2 ∧ ¬ dead h2 then 1 else 0) +
       (if cv h3 ∧ ¬ dead h3 then 1 else 0)) >
  (if cv h1 then 1 else 0) + (if cv h2 then 1 else 0) + (if cv h3 then 1 else 0)
ghost relation quorumExp (p : pid) :=
  2 * ((if expV p h1 ∧ ¬ dead h1 then 1 else 0) + (if expV p h2 ∧ ¬ dead h2 then 1 else 0) +
       (if expV p h3 ∧ ¬ dead h3 then 1 else 0)) >
  (if expV p h1 then 1 else 0) + (if expV p h2 then 1 else 0) + (if expV p h3 then 1 else 0)
-- Steps(s): an optional barrier, then at most one other step.
ghost relation hasBar (s : stage) :=
  (s = gbecome ∨ s = gusenew ∨ s = gend ∨ s = gremove ∨ s = grevert ∨ (extraRollback ∧ s = grollback)) ∧
  ¬ (skipOn ∧ s = skipStage)
ghost relation restIs (s : stage) (k : stepk) :=
  (k = ksync ∧ (s = gaddnv ∨ s = gbecome ∨ s = gusenew ∨ s = grollback ∨ s = gremove)) ∨
  (k = ktransfer ∧ s = gsnap) ∨
  (k = kcleanup ∧ (s = gcleanup ∨ s = gctarget)) ∨
  (k = knil ∧ (s = gnone ∨ s = gstart ∨ s = gend ∨ s = grevert))
ghost relation stepIs (p : pid) (k : stepk) :=
  (k = kbar ∧ hasBar (pstage p) ∧ step = 1) ∨
  (k ≠ kbar ∧ k ≠ knil ∧ restIs (pstage p) k ∧ step = (if hasBar (pstage p) then 2 else 1))
ghost relation stepsDone (p : pid) :=
  step > (if hasBar (pstage p) then 1 else 0) + (if restIs (pstage p) knil then 0 else 1)
ghost relation activeReq (i : rslot) := reqSt i = rpinned ∨ reqSt i = rinside

-- TLC's initial state, with its leader `l`.
action setup (l : host) {
  let p0 :| ∀ u, le p0 u
  require ¬ started
  require l = h1 ∨ l = h2
  started := true
  pubd p0 := true
  curR p0 := true
  pC p0 H := decide (H = h1 ∨ H = h2)
  pnext p0 H := decide (H = h1 ∨ H = h2)
  vw H P := decide (P = p0)
  cv H := decide (H = h1 ∨ H = h2)
  leader := l
  hasLeader := true
  known H := if H = h1 ∨ H = h2 then l else h1
  hasKnown H := decide (H = h1 ∨ H = h2)
  storage H := decide (H = h1 ∨ H = h2)
  caughtUp H := decide (H = h1 ∨ H = h2)
  reqV R P := decide (P = p0)
  step := 1
}

-- Topology coordinator.

action start_migration (l : host) (p : host) {
  let c :| curR c
  let n :| ¬ pubd n ∧ ∀ u, ¬ pubd u → le n u
  require started
  require pstage c = gnone
  require nMig < maxMigrations
  require pC c l ∧ ¬ pC c p ∧ ¬ excluded p
  pubd n := true
  curR c := false
  curR n := true
  pmig n := nMig + 1
  pstage n := gstart
  pC n H := pC c H
  pnext n H := decide ((pC c H ∧ H ≠ l) ∨ H = p)
  pL n := l
  pLs n := true
  pP n := p
  pPs n := true
  step := 1
  nMig := nMig + 1
  promoted := false
}

action advance {
  let c :| curR c
  let n :| ¬ pubd n ∧ ∀ u, ¬ pubd u → le n u
  require started
  require pstage c ≠ gnone
  require stepsDone c
  let s := pstage c
  pubd n := true
  curR c := false
  curR n := true
  pmig n := pmig c
  if s = gend then
    pstage n := gnone
    pC n H := pnext c H
    pnext n H := pnext c H
  else
    if s = grevert then
      pstage n := gnone
      pC n H := pC c H
      pnext n H := pC c H
    else
      pstage n := if s = gstart then gaddnv else if s = gaddnv then gsnap else if s = gsnap then gbecome
        else if s = gbecome then gusenew else if s = gusenew then gcleanup else if s = gcleanup then gend
        else if s = grollback then gremove else if s = gremove then gctarget else grevert
      pC n H := pC c H
      pnext n H := pnext c H
      pL n := pL c
      pLs n := pLs c
      pP n := pP c
      pPs n := pPs c
  step := 1
}

-- FailToRemove: an error or a cancellation before the pending replica can be promoted.
action fail_to_remove {
  let c :| curR c
  let n :| ¬ pubd n ∧ ∀ u, ¬ pubd u → le n u
  require started
  require nFail < maxFailures
  require pstage c = gstart ∨ pstage c = gaddnv ∨ pstage c = gsnap
  pubd n := true
  curR c := false
  curR n := true
  pmig n := pmig c
  pstage n := gremove
  pC n H := pC c H
  pnext n H := pnext c H
  pL n := pL c
  pLs n := pLs c
  pP n := pP c
  pPs n := pPs c
  step := 1
  nFail := nFail + 1
}

-- FailToRollback: sc_become_voter rolls back when the pending replica is excluded.
action fail_to_rollback {
  let c :| curR c
  let n :| ¬ pubd n ∧ ∀ u, ¬ pubd u → le n u
  require started
  require nFail < maxFailures
  require pstage c = gbecome
  let p := pP c
  pubd n := true
  curR c := false
  curR n := true
  pmig n := pmig c
  pstage n := grollback
  pC n H := pC c H
  pnext n H := pnext c H
  pL n := pL c
  pLs n := pLs c
  pP n := pP c
  pPs n := pPs c
  step := 1
  excluded p := true
  if ¬ excludedPendingAlive then
    dead p := true
    if hasLeader ∧ leader = p then
      hasLeader := false
      leader := h1
    reqSt R := if reqH R = p ∧ reqSt R ≠ runused then rdone else reqSt R
  nFail := nFail + 1
}

-- Barrier: a global barrier and drain at the current stage; excluded nodes are skipped.
action barrier {
  let c :| curR c
  require started
  require stepIs c kbar
  require ∀ h, ¬ excluded h → vw h c
  require ∀ i, activeReq i ∧ ¬ excluded (reqH i) → reqV i c
  require ∀ h p k, rpc h p k yrunning ∧ ¬ excluded h → p = c
  require ∀ h p, syn h p yrunning ∧ ¬ excluded h → p = c
  step := step + 1
}

action sync_send (h : host) {
  let c :| curR c
  require started
  require stepIs c ksync
  require serving c h ∧ ¬ excluded h
  require ¬ (syn h c ysent ∨ syn h c yrunning ∨ syn h c ydone)
  syn h c ysent := true
}

action sync_complete {
  let c :| curR c
  require started
  require stepIs c ksync
  require ∀ h, serving c h ∧ ¬ excluded h → syn h c ydone
  step := step + 1
}

-- Transfer: the pending replica receives a raft snapshot.
action transfer {
  let c :| curR c
  require started
  require stepIs c ktransfer
  require runningH (pP c)
  caughtUp (pP c) := true
  step := step + 1
}

-- CleanupSend: a cleanup RPC; a timed out one is sent again.
action cleanup_send (k : kslot) {
  let c :| curR c
  require started
  require stepIs c kcleanup
  require ∀ h k2, ¬ rpc h c k2 ydone
  require ∀ h s, ¬ rpc h c k s
  let t := if pstage c = gcleanup then pL c else pP c
  rpc t c k ysent := true
}

-- CleanupComplete: a cleanup of an excluded node is skipped.
action cleanup_complete {
  let c :| curR c
  require started
  require stepIs c kcleanup
  require (∃ h k, rpc h c k ydone) ∨ excluded (if pstage c = gcleanup then pL c else pP c)
  step := step + 1
}

-- Replicas.

-- ApplyGroup0(h, j): h applies group 0 up to a later state j.
action apply_group0 (h : host) (j : pid) {
  let c :| curR c
  let v :| vw h v
  require started
  require ¬ dead h
  require le v c ∧ v ≠ c
  require pubd j ∧ le v j ∧ v ≠ j ∧ le j c
  vw h P := decide (P = j)
  storage h := decide (storage h ∨ hostsGroup h j)
  if hasLeader ∧ leader = h ∧ ¬ hostsGroup h j then
    hasLeader := false
    leader := h1
}

action sync_start (h : host) (p : pid) {
  let v :| vw h v
  require started
  require syn h p ysent
  require ¬ dead h
  require le p v
  if v = p ∧ hostsGroup h v then
    if newerSyncSupersedes then
      syn h P yaborted := decide (syn h P yaborted ∨ syn h P yrunning)
      syn h P yrunning := false
    syn h p ysent := false
    syn h p yrunning := true
  else
    syn h p ysent := false
    syn h p yrefused := true
}

-- SyncPropose: the handler on the leader proposes the stage's configuration.
action sync_propose (h : host) (p : pid) {
  let c :| curR c
  require started
  require syn h p yrunning
  require ¬ dead h
  require guardAbortsSync → vw h p
  require hasLeader ∧ leader = h
  require ¬ cfgIsExp p
  require quorumNow ∧ quorumExp p
  cv H := decide (expV p H)
  cnv H := decide (expNV p H)
  if ¬ expV p h then
    hasLeader := false
    leader := h1
  promoted := decide (promoted ∨ (pPs c ∧ expV p (pP c)))
}

-- SyncReport: synced(S): the configuration is the stage's, and a voter names a voter.
action sync_report (h : host) (p : pid) {
  require started
  require syn h p yrunning
  require ¬ dead h
  require guardAbortsSync → vw h p
  require cfgIsExp p
  require cv h → (hasKnown h ∧ cv (known h))
  syn h p yrunning := false
  syn h p ydone := true
}

action sync_end (h : host) (p : pid) {
  require started
  require syn h p yrunning
  syn h p yrunning := false
  syn h p yaborted := true
}

-- CleanupStart: the stage must be cleanup or cleanup_target with this host as target.
action cleanup_start (h : host) (p : pid) (k : kslot) {
  let v :| vw h v
  require started
  require rpc h p k ysent
  require ¬ dead h
  require le p v
  rpc h p k ysent := false
  if ((pstage v = gcleanup ∧ pL v = h) ∨ (pstage v = gctarget ∧ pP v = h)) ∧ ¬ hostsGroup h v then
    rpc h p k yrunning := true
  else
    rpc h p k yrefused := true
}

action cleanup_effect (h : host) (p : pid) (k : kslot) {
  require started
  require rpc h p k yrunning
  storage h := false
  caughtUp h := false
  rpc h p k yrunning := false
  rpc h p k ydone := true
}

-- Raft of the tablet's group.

action elect (v : host) {
  require started
  require nElect < maxElections
  require cv v ∧ runningH v ∧ ¬ (hasLeader ∧ leader = v)
  require quorumNow
  leader := v
  hasLeader := true
  known v := v
  hasKnown v := true
  nElect := nElect + 1
}

action learn_leader (h : host) {
  require started
  require hasLeader ∧ leader ≠ h
  require (cv h ∨ cnv h) ∧ runningH h
  require ¬ (hasKnown h ∧ known h = leader)
  known h := leader
  hasKnown h := true
}

-- Requests.

action pin (i : rslot) (h : host) {
  let v :| vw h v
  require started
  require maxRequests > 0
  require reqSt i = runused
  require ¬ dead h
  require serving v h
  reqH i := h
  reqV i P := decide (P = v)
  reqSt i := rpinned
}

action enter (i : rslot) {
  require started
  require reqSt i = rpinned
  reqSt i := if runningH (reqH i) then rinside else rdone
}

action leave (i : rslot) {
  require started
  require reqSt i = rpinned ∨ reqSt i = rinside
  reqSt i := rdone
}

-- The invariants, named as in the design doc.
ghost relation liveView (p : pid) := (∃ g, ¬ dead g ∧ vw g p) ∨ (∃ i, activeReq i ∧ reqV i p)
safety [i1_leader_coverage] (check = 0 ∨ check = 1) →
  liveView P → serving P H → ¬ dead H → (¬ hasKnown H ∨ serving P (known H))
safety [i2_membership] check = 0 → reqSt I = rinside → (cv (reqH I) ∨ cnv (reqH I)) ∧ runningH (reqH I)
safety [i3_storage] check = 0 → reqSt I = rinside → storage (reqH I)
safety [i4_nesting] check = 0 → liveView P → liveView Q →
  (∀ h, serving P h → serving Q h) ∨ (∀ h, serving Q h → serving P h)
safety [i5_storage_under_server] check = 0 → runningH H → storage H
safety [voters_caught_up] check = 0 → cv H → caughtUp H
safety [leader_is_voter] check = 0 → hasLeader → cv leader
safety [synced_holds] (check = 0 ∨ check = 2) → curR C →
  ¬ (restIs (pstage C) ksync ∧ step ≤ (if hasBar (pstage C) then 2 else 1)) → cfgIsExp C
ghost relation reached (s : stage) := ∃ p, pubd p ∧ pstage p = s
ghost relation curNone := ∃ c, curR c ∧ pstage c = gnone
safety [w_migration_ended] check = 10 → ¬ (reached gend ∧ curNone)
safety [w_removed_pending] check = 11 → ¬ (reached gremove ∧ curNone)
safety [w_rolled_back] check = 12 → ¬ (reached grollback ∧ curNone)
safety [w_rolled_back_after_promotion] check = 13 → ¬ (promoted ∧ reached grollback ∧ curNone)
safety [w_second_migration_ended] check = 14 → ¬ (nMig = 2 ∧ curNone)
-- The instance has a free state index left (else a publish would be cut off).
safety [pids_left] ∃ p, ¬ pubd p

#gen_spec

-- Configs: Fin 12 indices for one migration, Fin 22 for two; one request slot.
-- MC_current.cfg: pass.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 1, maxElections := 2, maxRequests := 0, check := 0 }
-- MC_excluded_alive_i1.cfg: I1_LeaderCoverage violated (catalog #5).
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := true, maxMigrations := 1, maxFailures := 1, maxElections := 2, maxRequests := 0, check := 1 }
-- MC_excluded_alive_i1_rollback_barrier.cfg: pass.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := true, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := true, maxMigrations := 1, maxFailures := 1, maxElections := 2, maxRequests := 0, check := 1 }
-- MC_excluded_alive.cfg: I2_Membership violated.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := true, maxMigrations := 1, maxFailures := 1, maxElections := 2, maxRequests := 1, check := 0 }
-- MC_current_requests.cfg: pass.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 1, maxElections := 0, maxRequests := 1, check := 0 }
-- MC_current_two_migrations.cfg: pass.
#model_check compiled { pid := Fin 22, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 2, maxFailures := 1, maxElections := 1, maxRequests := 0, check := 0 }
-- MC_ablate_become_voter_barrier.cfg: I1_LeaderCoverage violated.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := true, skipStage := gbecome, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 0, maxElections := 1, maxRequests := 1, check := 0 }
-- MC_ablate_end_migration_barrier.cfg: I5_StorageUnderServer violated.
#model_check compiled { pid := Fin 22, rslot := Fin 1, skipOn := true, skipStage := gend, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 2, maxFailures := 0, maxElections := 0, maxRequests := 0, check := 0 }
-- MC_ablate_remove_pending_barrier.cfg: pass.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := true, skipStage := gremove, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 1, maxElections := 0, maxRequests := 1, check := 0 }
-- MC_ablate_revert_migration_barrier.cfg: I5_StorageUnderServer violated.
#model_check compiled { pid := Fin 22, rslot := Fin 1, skipOn := true, skipStage := grevert, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 2, maxFailures := 1, maxElections := 0, maxRequests := 0, check := 0 }
-- MC_ablate_use_new_barrier.cfg: I2_Membership violated.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := true, skipStage := gusenew, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 0, maxElections := 1, maxRequests := 1, check := 0 }
-- MC_ablate_sync_guard_and_supersede.cfg: pass.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := false, newerSyncSupersedes := false, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 1, maxElections := 2, maxRequests := 0, check := 0 }
-- MC_ablate_sync_guard_and_supersede_alive.cfg: SyncedHolds violated.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := false, newerSyncSupersedes := false, excludedPendingAlive := true, maxMigrations := 1, maxFailures := 1, maxElections := 2, maxRequests := 0, check := 2 }
-- Witnesses: MC_witness_end, _remove_pending, _rollback, _rollback_after_promotion_alive
-- (reachable), _rollback_after_promotion (not reachable: pass), _second_migration.
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 0, maxElections := 0, maxRequests := 0, check := 10 }
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 1, maxElections := 0, maxRequests := 0, check := 11 }
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 1, maxElections := 0, maxRequests := 0, check := 12 }
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := true, maxMigrations := 1, maxFailures := 1, maxElections := 1, maxRequests := 0, check := 13 }
#model_check compiled { pid := Fin 12, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 1, maxFailures := 1, maxElections := 1, maxRequests := 0, check := 13 }
#model_check compiled { pid := Fin 22, rslot := Fin 1, skipOn := false, skipStage := gnone, extraRollback := false, guardAbortsSync := true, newerSyncSupersedes := true, excludedPendingAlive := false, maxMigrations := 2, maxFailures := 0, maxElections := 0, maxRequests := 0, check := 14 }

end ScTabletMigration
