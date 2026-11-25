/-
Copyright (C) 2026-present ScyllaDB

SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-/
module
public import Veil

-- models: auth/cache.cc, auth/cache.hh, service/raft/group0_state_machine.cc
-- checked-against: 61d55c44f6a

/-! # The auth cache's permission sets (auth/cache.cc)

The permissions of one resource, cached per role on each shard, while
group 0 applies role and permission changes.

Code:
- `get_miss`, `load_read`, `load_store`: `cache::get_permissions()` on a miss,
  `load_permissions()`: the loader (`service::get_uncached_permissions()`)
  reads the tables, then `add_permissions()` stores the result into the
  role record the lookup started with (kept alive by `role_ptr`), unless an
  entry is there already (`emplace`). It holds `_permission_loader_sem`
  only, nothing a reload takes.
- `cmd_*`: a group 0 command changes the tables; `group0_state_machine::
  reload_modules()` then calls `cache::load_roles()` with the roles whose
  partitions changed.
- `reload_replace`: `load_roles()` fetches each changed role and replaces its
  record (`add_role()`), on shard 0 and then on the others
  (`distribute_role()`, under each shard's `_loading_sem`, so after an LDAP
  reload in flight there); a replaced record starts with no permissions,
  and a lookup in flight on the old record stores into a dead object.
- The loader takes a role's memberships and superuser flags from the
  shard's records (`standard_role_manager::find_record()`), its grants from
  the table; a record missing on the shard counts as no permission here
  (the code throws, and nothing is stored).
- `reload_clear`: after that, `invoke_on_all` clears, in place, the cached
  permissions of the roles that inherit from a changed one
  (`gather_inheriting_roles()`, `clear_role_permissions()`), without
  `_loading_sem`.
- `ldap_read`, `ldap_write`: `reload_all_permissions()` (the LDAP role
  manager's pruner): for every cached entry, `perms_cache[res] = co_await
  loader(...)`, under the shard's `_loading_sem`.
- A command with `v` set reaches the node in a raft snapshot instead
  (`group0_state_machine::load_snapshot()` -> `reload_state()` ->
  `cache::load_all()`): shard 0 fetches every role and replaces its record,
  one at a time with yields between them, then `distribute_role()` copies
  each record to the other shards, each copy under that shard's
  `_loading_sem`, and `prune_all()` removes the dropped roles. Nothing
  clears the permissions cached in records it already replaced.
- `cmd_drop_resource`, `prune`: dropping a resource revokes it from every
  role (`service::revoke_all()`) and prunes its entries on the local shard
  (`cache::prune()`), before the batch commits.
- Anonymous permissions (`_anonymous_permissions`) are not modelled: the
  authorizers return a constant for the anonymous user
  (`default_authorizer::authorize()` returns NONE), so they can't go stale.

Abstractions: shards are interchangeable (a sort); one resource and one
permission bit; a one-level role
hierarchy (a role with members has no parents), so a role's inheritors are
its members; at most one lookup and one LDAP reload in flight per shard and
role; a command's reload finishes before the next command applies (group 0
applies one at a time).

`fix` is a candidate fix for SCYLLADB-4206: a clear also voids the lookups
in flight on the record (e.g. a generation the store checks); `fixLdap`
does the same for LDAP reloads in flight. `fixSnap` is a candidate fix for
`load_all()`: once every shard has the new records, it clears every role's
permissions (as `load_roles()` clears the inheritors).
-/

veil module AuthCache

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

#gen_spec

-- The code: SCYLLADB-4206 (five commands: create P and R, GRANT P TO R, GRANT
-- the permission to P, REVOKE it).
-- expect: violation fresh
#model_check compiled { shard := Fin 2, role := Fin 3, fix := false, fixLdap := false, ldap := false, maxCmds := 5, snap := false, fixSnap := false, pruneOn := false, checkRefused := true }
-- The candidate fix, without LDAP.
-- expect: pass
#model_check compiled { shard := Fin 2, role := Fin 3, fix := true, fixLdap := false, ldap := false, maxCmds := 5, snap := false, fixSnap := false, pruneOn := false, checkRefused := true }
-- With LDAP's reload_all_permissions, on one shard with two roles (the race
-- is within a shard and needs only the parent and its member; larger
-- instances take more than the bench's 62 GB): the code, then the lookup
-- fix alone.
-- expect: violation fresh
#model_check compiled { shard := Fin 1, role := Fin 2, fix := false, fixLdap := false, ldap := true, maxCmds := 5, snap := false, fixSnap := false, pruneOn := false, checkRefused := true }
-- expect: pass
#model_check compiled { shard := Fin 1, role := Fin 2, fix := true, fixLdap := false, ldap := true, maxCmds := 5, snap := false, fixSnap := false, pruneOn := false, checkRefused := true }
-- Commands that may arrive in a snapshot (load_all()), one shard and two
-- roles: the lookup fix alone, then with the load_all() fix too.
-- expect: violation fresh
#model_check compiled { shard := Fin 1, role := Fin 2, fix := true, fixLdap := false, ldap := false, maxCmds := 5, snap := true, fixSnap := false, pruneOn := false, checkRefused := true }
-- expect: pass
#model_check compiled { shard := Fin 1, role := Fin 2, fix := true, fixLdap := false, ldap := false, maxCmds := 5, snap := true, fixSnap := true, pruneOn := false, checkRefused := true }
-- The snapshot race granting what the tables don't (a revoked superuser's
-- member keeps its permissions).
-- expect: violation fresh
#model_check compiled { shard := Fin 1, role := Fin 2, fix := true, fixLdap := false, ldap := false, maxCmds := 5, snap := true, fixSnap := false, pruneOn := false, checkRefused := false }
-- Dropped resources and prune(), with the lookup fix.
-- expect: pass
#model_check compiled { shard := Fin 1, role := Fin 2, fix := true, fixLdap := false, ldap := false, maxCmds := 5, snap := false, fixSnap := false, pruneOn := true, checkRefused := true }

end AuthCache
