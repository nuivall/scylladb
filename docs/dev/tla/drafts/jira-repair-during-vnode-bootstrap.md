DRAFT, not posted.

Summary: vnodes: repair during bootstrap/decommission records repair history without the pending replica; deleted rows come back after tombstone GC

A vnode repair picks its participants from the natural replicas only
(repair/repair.cc `get_neighbors()`: `erm.get_natural_replicas()`), and
`update_system_repair_table()` (repair/row_level.cc) records the range in
system.repair_history when the participants equal the natural replicas.
During a streaming bootstrap (`allowed_repair_based_node_ops` doesn't
include bootstrap by default) or decommission, the range also has a pending
replica, which streamed it from the node losing it. Rows the repair writes
to that node after the stream read them go to its sstables, not through
storage_proxy, so the pending replica never gets them. Nothing prevents a
user repair while a topology change is in progress.

If the node losing the range missed a delete (hints disabled or expired),
the joiner gets the deleted row without its tombstone. The repair brings
the tombstone to the natural replicas and records a repair time; with
tombstone_gc mode repair -- the default for vnode tables too -- they then
purge the tombstone, and once the joiner owns the range the row is back
(read repair or the next repair spreads it).

Reproducer: test/cluster/test_repair_during_vnode_bootstrap_gc.py (dev mode;
adds a test-only pause `bootstrap_pause_after_streaming` next to
`stop_after_streaming` in storage_service.cc). 3 nodes, RF=3, vnodes, hints
off; one node misses the deletes of 200 rows; a 4th node bootstraps and is
held after streaming; repair; release; flush, major compaction: 45 to 70
of 200 deleted rows are back at CL=ALL. Fails 3 of 3; the same with the
repair after the bootstrap passes 3 of 3. The test runs without a commitlog
and with `repair_hints_batchlog_flush_cache_time_in_ms: 0`, as
test_data_resurrection_in_memtable.py does, so GC follows the repair time
at once; in production the commitlog cap lifts as segments roll and the
flush cache only shifts the repair time by up to 60 s.

Candidate fix: don't record repair history while a topology change is in
progress (`get_topology_change_info()` of the repair's erm) -- passes 3 of
3. Checking only the range's end token for pending replicas is not enough
(4 to 30 rows still came back): a repaired range can contain a joining
token. A complete fix also has to cover a topology change that starts
during the repair (re-check with a fresh erm before recording), or add the
pending replicas to the repair.

Related: SCYLLADB-6092 (tablets: migration off a replica that missed the
delete, then incremental repair; different mechanism and fix).

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #23).
