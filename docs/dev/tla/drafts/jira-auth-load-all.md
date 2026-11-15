DRAFT, not posted.

Summary: auth: a permission lookup during load_all() can cache a set computed from a mix of new and old role records, per shard

When a node applies a group 0 snapshot (`group0_state_machine::load_snapshot()`
-> `reload_state()` -> `auth::cache::load_all()`), the auth cache replaces
every role record one at a time: shard 0 fetches each role and calls
`add_role()`, yielding between roles (each `fetch_role()` runs queries), and
`distribute_role()` then copies each record to the other shards, one
cross-shard call per role. Nothing clears cached permissions afterwards:
`load_roles()` clears the inheriting roles at the end, `load_all()` doesn't.

A permission lookup of role R that misses between the replacement of R's
record and that of R's parent P reads R's memberships from R's new record
but P's superuser flag from P's old one (`get_uncached_permissions()` ->
`standard_role_manager::find_record()` reads `_cache`), and stores the
result into R's new record. When P's record is replaced afterwards, R's
entry is left as it is. Lookups take `_permission_loader_sem` only, so the
`_loading_sem` that `load_all()` holds (PR #27573) doesn't order them.

Effect, on one shard, until R changes again or the node restarts:
- `ALTER ROLE P WITH SUPERUSER = false` received in a snapshot: R keeps the
  permissions it had through P.
- `ALTER ROLE P WITH SUPERUSER = true` received in a snapshot: R is refused.
The durable state is correct.

A node applies a snapshot when it is behind the group 0 leader's log (raft
snapshot transfer), e.g. after a partition or a long pause.

Found with a Veil (Lean) model of the auth cache,
docs/dev/tla/veil/AuthCache.lean (configs "Commands that may arrive in a
snapshot"); the model checker's trace, one shard, two roles: create P, create
R, GRANT P TO R, then ALTER ROLE P WITH SUPERUSER = true applied by a
snapshot; R's record replaced; lookup of R reads (P's old record: not
superuser); P's record replaced; the lookup stores NONE into R's record.

Candidate fix: once every shard has the new records, clear every role's
cached permissions (`invoke_on_all` over `_roles`), voiding the lookups in
flight as for SCYLLADB-4206. With it, and the SCYLLADB-4206 candidate fix,
AuthCacheProof.lean proves that once a reload is done every cached entry is
what the tables give, for any number of roles and shards; without it the
proof fails at the commands applied by a snapshot.

Related: SCYLLADB-4206 (same mechanism for load_roles()'s in-place clear).
Reproducer: test/cluster/auth_cluster/test_auth_cache_load_all_race.py
(dev mode; a test-only pause `auth_cache_load_all_pause` after `add_role()`
in `load_all()`). Three single-shard nodes; P superuser, GRANT P TO R; the
lagging node drops group 0 appends while the leader runs ALTER ROLE P WITH
SUPERUSER = false and takes a snapshot, so it learns of the change from
the snapshot; during the pause after R's record, R runs a SELECT on it.
Afterwards R is refused on the leader and allowed on the lagging node.
Fails 3 of 3 (only the lagging node allows R); passes 3 of 3 with the
candidate fix (load_all() clearing every role's permissions at the end).
