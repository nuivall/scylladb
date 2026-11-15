DRAFT, not posted.

Summary: vnodes: repair-based rebuild with the small table optimization resurrects deleted rows from unowned copies on not-yet-cleaned-up nodes

Since cb52178d5d3 (SCYLLADB-3248), repair-based node operations apply the
small table optimization to user tables under
`small_table_optimization_for_rbno_max_table_size` (1 GiB by default);
rebuild is repair-based by default. The optimization repairs one range
covering the whole ring with every node of the source DC(s) as a peer
(repair/repair.cc). Each peer reads the whole ring from its own sstables,
including ranges it no longer owns; the only ownership filters are on the
receiving side (row_level.cc `flush_rows()` for the master,
`put_row_diff_with_rpc_stream()` for the followers), so a node's unowned
copies are sent to the current owners.

After a bootstrap, the old owners keep the ranges they gave up: the
topology coordinator runs vnode cleanup only before removenode and
decommission, and compaction drops the unowned data of an sstable only when
it compacts that sstable. Rows deleted after the bootstrap, whose
tombstones the owners have purged (gc_grace passed, or repair-mode GC after
a repair), come back from such a copy on every owner when a rebuild runs.

Reproducer: test/cluster/test_rebuild_small_table_stale_data.py (dev mode,
no injection). 3 nodes RF=2, vnodes, tombstone_gc immediate (no commitlog,
as test_data_resurrection_in_memtable.py does); insert 200 rows; bootstrap
a 4th node; delete all at CL=ALL; flush; major compaction on all nodes but
the first (whose stale sstables thus stay uncompacted); rebuild another
node: 49 to 97 of 200 deleted rows are back at CL=ALL. Fails 3 of 3; with
`small_table_optimization_for_rbno_max_table_size: 0` it passes 3 of 3.

Fix options: filter what a peer reads/sends by its own ownership in the
small-table path (the token metadata is already computed for the receive
side, `get_tm_for_small_table_optimization_check()`); or don't use the
optimization for tables that require cleanup on any peer.

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #25).
