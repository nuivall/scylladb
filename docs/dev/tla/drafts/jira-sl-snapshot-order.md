DRAFT, not posted.

Summary: a node applying a group 0 snapshot computes effective service levels from the auth cache before reloading it

`group0_state_machine::reload_state()` (run by `load_snapshot()`, i.e. when a
node catches up from a raft snapshot) calls
`storage_service::update_service_levels_cache()` before
`auth::cache::load_all()`. The effective service levels are computed in
`service_level_controller::auth_integration::reload_cache()` from the role
manager's `query_all()`, `query_all_directly_granted()` and
`query_attribute_for_all()`, which all read the auth cache's role records.
So on that node the effective service levels reflect the roles as they
were before the snapshot. `reload_modules()` (commands applied one at a
time) has the right order: `load_roles()` first. The order changed in
19da1cb6569, which added `load_all()` to `reload_state()`.

Effect: a role whose service level attachment (or role grants) arrived in
the snapshot keeps its old effective service level (timeout, workload
type, shares) on that node until the next service level or role change.
Boot is not affected (main.cc recomputes the service levels after the auth
service starts).

Reproducer: test/cluster/auth_cluster/test_service_level_snapshot_order.py
(dev mode). Three single-shard nodes; CREATE SERVICE LEVEL, CREATE ROLE;
a non-leader drops group 0 appends
(`raft_drop_incoming_append_entries_for_specified_group`) while the leader
runs ATTACH SERVICE LEVEL and forces a snapshot; then LIST EFFECTIVE
SERVICE LEVEL OF the role says "doesn't have assigned any service level"
on that node only. Fails 3 of 3; passes 3 of 3 with
`update_service_levels_cache()` moved after `load_all()`.

Fix: call `update_service_levels_cache()` after `auth_cache().load_all()`
in `reload_state()`.
