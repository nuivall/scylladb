/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

/-! # The auth cache's permission sets, with the candidate fix: an inductive invariant

AuthCache.lean with `fix` and `fixSnap` assumed (a clear voids the lookups
in flight on its record; load_all() clears every role's permissions at the
end; LDAP reloads are left as they are), and an inductive
strengthening of `fresh`: for any number of roles and shards and every
interleaving, once a reload is done every cached entry is what the tables
give.
-/

veil module AuthCacheProof

enum lstage = {lidle, lstarted, lread}

type role
type shard

param fix : Bool
param fixLdap : Bool
param ldap : Bool
param maxCmds : Nat
param snap : Bool
param fixSnap : Bool
param pruneOn : Bool
param checkRefused : Bool

-- The tables.
relation present : role → Bool
relation grant : role → Bool
relation su : role → Bool
relation mem : role → role → Bool
-- The cache of each shard: a record per role, its cached entry and value.
relation recd : shard → role → Bool
relation cperm : shard → role → Bool
relation cval : shard → role → Bool
-- What a shard's record of a role says: superuser, and the roles it is a member of.
relation recSu : shard → role → Bool
relation recMem : shard → role → role → Bool
-- A lookup in flight per shard and role: its stage, the value it read, and
-- whether its record was replaced (or, with the fix, cleared) since it started.
function lst : shard → role → lstage
relation lval : shard → role → Bool
relation lvoid : shard → role → Bool
-- An LDAP reload in flight per shard and role.
function dst : shard → role → lstage
relation dval : shard → role → Bool
relation dvoid : shard → role → Bool
-- The reload of the last command, still to do on each shard.
relation toReplace : shard → role → Bool
relation toClear : shard → role → Bool
individual nCmd : Nat

#gen_state

after_init {
  present R := false
  grant R := false
  su R := false
  mem R P := false
  recd S R := false
  cperm S R := false
  cval S R := false
  recSu S R := false
  recMem S R P := false
  lst S R := lidle
  lval S R := false
  lvoid S R := false
  dst S R := lidle
  dval S R := false
  dvoid S R := false
  toReplace S R := false
  toClear S R := false
  nCmd := 0
}

-- The permission the tables give a role: its own, or a parent's, or superuser.
ghost relation eff (r : role) :=
  present r ∧ (grant r ∨ su r ∨ ∃ p, mem r p ∧ present p ∧ (grant p ∨ su p))
-- What the loader computes on shard s: the closure and the superuser flags
-- from the shard's records (`find_record()` reads `_cache.get()`), the grants
-- from the table (`default_authorizer::authorize()`).
ghost relation effC (s : shard) (r : role) :=
  recd s r ∧ (grant r ∨ recSu s r ∨ ∃ p, recMem s r p ∧ recd s p ∧ (grant p ∨ recSu s p))
ghost relation reloading := ∃ s x, toReplace s x ∨ toClear s x

-- Lookups.

action get_miss (s : shard) (r : role) {
  require recd s r ∧ ¬ cperm s r ∧ lst s r = lidle
  lst s r := lstarted
  lvoid s r := false
}

action load_read (s : shard) (r : role) {
  require lst s r = lstarted
  lval s r := decide (effC s r)
  lst s r := lread
}

action load_store (s : shard) (r : role) {
  require lst s r = lread
  if ¬ lvoid s r ∧ ¬ cperm s r then
    cperm s r := true
    cval s r := lval s r
  lst s r := lidle
  lval s r := false
  lvoid s r := false
}

-- LDAP's reload_all_permissions(): reloads an entry that is cached.
action ldap_read (s : shard) (r : role) {
  require ldap
  require recd s r ∧ cperm s r ∧ dst s r = lidle
  dval s r := decide (effC s r)
  dvoid s r := false
  dst s r := lread
}

action ldap_write (s : shard) (r : role) {
  require dst s r = lread
  if ¬ dvoid s r then
    cperm s r := true
    cval s r := dval s r
  dst s r := lidle
  dval s r := false
  dvoid s r := false
}

-- Group 0 commands. Each one changes the tables, then names the roles whose
-- records every shard replaces and the roles whose permissions it clears.

action cmd_create (r : role) (v : Bool) {
  require ¬ reloading ∧ nCmd < maxCmds ∧ (snap ∨ ¬ v)
  require ¬ present r
  present r := true
  toReplace S X := decide (v ∨ X = r)
  toClear S X := decide (v ∧ fixSnap)
  nCmd := nCmd + 1
}

-- DROP ROLE: its grants, superuser flag and memberships go; members and
-- parents are changed too (their memberships).
action cmd_drop (r : role) (v : Bool) {
  require ¬ reloading ∧ nCmd < maxCmds ∧ (snap ∨ ¬ v)
  require present r
  toReplace S X := decide (v ∨ X = r ∨ mem X r ∨ mem r X)
  -- load_roles() clears the inheritors of every changed role: r's members (by
  -- its old record) and the other members of r's parents (by their new ones).
  toClear S X := decide ((v ∧ fixSnap) ∨ (¬ v ∧ (mem X r ∨ ∃ g, mem r g ∧ mem X g ∧ X ≠ r)))
  present r := false
  grant r := false
  su r := false
  mem r X := false
  mem X r := false
  nCmd := nCmd + 1
}

-- GRANT / REVOKE the permission on the resource TO / FROM p.
action cmd_grant_perm (p : role) (b : Bool) (v : Bool) {
  require ¬ reloading ∧ nCmd < maxCmds ∧ (snap ∨ ¬ v)
  require present p
  grant p := b
  toReplace S X := decide (v ∨ X = p)
  toClear S X := decide ((v ∧ fixSnap) ∨ (¬ v ∧ (mem X p)))
  nCmd := nCmd + 1
}

-- ALTER ROLE p WITH SUPERUSER = b.
action cmd_superuser (p : role) (b : Bool) (v : Bool) {
  require ¬ reloading ∧ nCmd < maxCmds ∧ (snap ∨ ¬ v)
  require present p
  su p := b
  toReplace S X := decide (v ∨ X = p)
  toClear S X := decide ((v ∧ fixSnap) ∨ (¬ v ∧ (mem X p)))
  nCmd := nCmd + 1
}

-- GRANT p TO r (a one-level hierarchy: p has no parents, r no members).
action cmd_grant_role (p : role) (r : role) (v : Bool) {
  require ¬ reloading ∧ nCmd < maxCmds ∧ (snap ∨ ¬ v)
  require present p ∧ present r ∧ p ≠ r ∧ ¬ mem r p
  require ∀ x, ¬ mem p x
  require ∀ x, ¬ mem x r
  mem r p := true
  toReplace S X := decide (v ∨ X = p ∨ X = r)
  toClear S X := decide ((v ∧ fixSnap) ∨ (¬ v ∧ (mem X p)))
  nCmd := nCmd + 1
}

-- REVOKE p FROM r.
action cmd_revoke_role (p : role) (r : role) (v : Bool) {
  require ¬ reloading ∧ nCmd < maxCmds ∧ (snap ∨ ¬ v)
  require mem r p
  mem r p := false
  toReplace S X := decide (v ∨ X = p ∨ X = r)
  toClear S X := decide ((v ∧ fixSnap) ∨ (¬ v ∧ (mem X p)))
  nCmd := nCmd + 1
}

-- DROP TABLE (KEYSPACE, FUNCTION...): service::revoke_all() deletes the
-- resource's rows of every role that has one (role_permissions is keyed by
-- role, so those roles are reloaded) ...
action cmd_drop_resource (v : Bool) {
  require ¬ reloading ∧ nCmd < maxCmds ∧ (snap ∨ ¬ v)
  require pruneOn
  toReplace S X := decide (v ∨ grant X)
  toClear S X := decide ((v ∧ fixSnap) ∨ (¬ v ∧ ∃ p, mem X p ∧ grant p))
  grant X := false
  nCmd := nCmd + 1
}

-- ... and cache::prune() drops the resource's entries on the coordinator's
-- shard, under its _loading_sem, before the batch commits (here: any time).
action prune (s : shard) {
  require pruneOn
  require ∀ r, dst s r = lidle
  cperm s X := false
  cval s X := false
}

-- load_roles(): a changed role's record, replaced on shard `s`.
action reload_replace (s : shard) (x : role) {
  require toReplace s x
  -- distribute_role() takes the shard's _loading_sem, which an LDAP reload holds.
  require ∀ r, dst s r = lidle
  recd s x := present x
  recSu s x := su x
  recMem s x P := mem x P
  cperm s x := false
  cval s x := false
  lvoid s x := true
  dvoid s x := true
  toReplace s x := false
}

-- Then the inheritors' permissions, cleared in place.
action reload_clear (s : shard) (y : role) {
  require ∀ t x, ¬ toReplace t x
  require toClear s y
  cperm s y := false
  cval s y := false
  if fix then
    lvoid s y := true
  if fixLdap then
    dvoid s y := true
  toClear s y := false
}

-- Once a reload is done, a cached entry is what the tables give: it grants
-- nothing the tables don't, and (with `checkRefused`) refuses nothing they grant.
safety [fresh] ¬ reloading → cperm S R → (cval S R → eff R) ∧ (checkRefused → eff R → cval S R)

assumption [fixed] fix = true ∧ fixLdap = false ∧ fixSnap = true

-- The one-level hierarchy.
invariant [mem_irrefl] ¬ mem R R
invariant [mem_parent_top] mem R P → ¬ mem P X
invariant [mem_member_leaf] mem R P → ¬ mem X R
invariant [mem_present] mem R P → present R ∧ present P
-- A shard's records say what the tables say, unless their replace is pending;
-- a pending replace of a parent leaves its members to be cleared.
invariant [rec_match] ¬ toReplace S X → (recd S X ↔ present X) ∧ (recSu S X ↔ su X) ∧ (∀ p, recMem S X p ↔ mem X p)
invariant [parent_pending_mem] toReplace S P ∧ mem R P → toClear S R
invariant [parent_pending_rec] toReplace S P ∧ recMem S R P → toReplace S R ∨ toClear S R
invariant [cperm_recd] cperm S R → recd S R
-- A lookup or LDAP reload in flight that wasn't voided has its record.
invariant [lookup_recd] lst S R ≠ lidle ∧ ¬ lvoid S R → recd S R
invariant [ldap_recd] dst S R ≠ lidle ∧ ¬ dvoid S R → recd S R
-- A cached entry, the value a lookup read and the value an LDAP reload read
-- are what the tables give, unless the role's reload is pending on that shard.
invariant [cache_fresh] cperm S R → (cval S R ↔ eff R) ∨ toReplace S R ∨ toClear S R
invariant [lookup_fresh] lst S R = lread ∧ ¬ lvoid S R → (lval S R ↔ eff R) ∨ toReplace S R ∨ toClear S R
invariant [ldap_fresh] dst S R = lread ∧ ¬ dvoid S R → (dval S R ↔ eff R) ∨ toReplace S R ∨ toClear S R
-- A stale LDAP reload keeps a replace pending on its shard (it holds the
-- semaphore the replace needs), so the clear, which waits for every
-- replace, comes after its write.
invariant [ldap_stale_blocks] dst S R = lread ∧ ¬ dvoid S R ∧ ¬ (dval S R ↔ eff R) → ∃ x, toReplace S x

#gen_spec

-- The strengthening holds on the model-checked bounds.
#model_check compiled { shard := Fin 1, role := Fin 2, fix := true, fixLdap := false, ldap := true, maxCmds := 5, snap := true, fixSnap := true, pruneOn := true, checkRefused := true }

-- For every instance and every execution.
#check_invariants

end AuthCacheProof
