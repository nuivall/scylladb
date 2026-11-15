# Catalog of bugs and reproducers

Found during the TLA+ experiment (2026-11). Nothing here has been posted to
Jira or GitHub. Details of each are in README.md, under the spec named.

| # | bug | kind | status | reproducer | spec | found by |
|---|---|---|---|---|---|---|
| 1 | `transfer_snapshot()` calls `fail()` only for `schema_persisted_but_not_applied`; another exception after `do_apply_many()` persisted the schema lets raft retry, and the joiner re-creates `system_distributed` with SimpleStrategy RF=3 | corruption (schema) | new; no Jira; PR #32876 doesn't cover it | `test_joining_node_stops_group0_on_schema_persist_failure` in `test/cluster/test_schema_merge_failure_stops_group0.py`, injection `database_apply_many_fail` | group0_state_machine | TLC (group0_state_machine: exception path after persist), then code check, then reproducer |
| 2 | `merge_tablet_info()` takes the max `repair_time` of the merged tablets, raising the tombstone GC horizon of the half repaired less recently | resurrection | known: SCYLLADB-6091, fixed on master by PR #32897 (ad818f5f8fa) | `test/cluster/test_tombstone_gc_tablet_merge.py` (merge test fails, no-merge control passes) | tombstone_gc_repair | TLC (tombstone_gc_repair: merge takes max repair_time), then reproducer; Jira found it known |
| 3 | GC sstables of a compaction of repaired sstables get `repaired_at=0`; incremental repair marking and scrub capture them while the owning compaction runs | crash ("Unable to remove input SSTable") | root cause of open SCYLLADB-5896; SCYLLADB-6121 looks the same (scrub), not reproduced | `test_incremental_repair_during_repaired_compaction_with_gc_sstables` in `test/cluster/test_incremental_repair.py`, injections `compaction_pause_after_gc_sstables_attached`, `component_rewrite_pause_after_view` | compaction_groups | code reading prompted by TLC (compaction_groups: unfiltered capture), then reproducer |
| 4 | a completion that fails before attaching deletes its outputs (9cbab949b4d); the split bypass and off-strategy pass the input as the output, so the live sstable is deleted | data loss (after restart) | new; master and 2026.4 only; maybe SCYLLADB-6108 (needs node-2's log of SCT run fa2dc458, which needs AWS access); off-strategy variant not reproduced | `test/cluster/test_split_bypass_failure.py`, injection `update_sstable_sets_on_compaction_completion_fail_once` | none (code reading) | code reading while writing a spec step (compaction_groups bypass), then reproducer |
| 5 | SC tablets: I1 violated on rollback when an excluded pending replica comes back (c59a1208f7e removed the `sc_rollback` barrier; a comment in `do_config_sync` still relies on it) | invariant only in the model: on a cluster the excluded node is banned at the messaging layer (`ban_hosts`) and exits when it returns, so I1 itself can't break; what remains is the ordering the barrier existed for: the leader's held sc_become_voter sync lands its config change after the other replicas reported sc_rollback synced | new; not reproduced | `test_sc_rollback_sync_fences_sync_of_sc_become_voter` in test/cluster/test_strong_consistency.py (test-only pause `sc_config_sync_pause_on_leader`): fails 3 of 3 (early sc_rollback reports from 2 replicas); with `with_barrier::yes` restored at sc_rollback (commit "TEST-ONLY candidate fix: SC tablet rollback barrier") passes 3 of 3 | sc_tablet_migration | TLC (sc_tablet_migration: I1 on rollback), then git log of the removed barrier |
| 6 | intra-node migration: a node that restarts after the streaming response but before the coordinator sets write_both_read_new boots with the tablet's sstables on the leaving shard; reads go to the empty pending shard and cleanup deletes the leaving shard's sstables | data loss (replica) | known: SCYLLADB-6226, fixed on master by PR #33027 (5c701acb75f); found independently by TLC | `test_restart_after_intranode_streaming_keeps_data[True]` in `test/cluster/test_tablets_intranode.py` (`[False]` is the control), injections `intranode_migration_streaming_done_wait` (new) and `stream_tablet_wait` | tablet_storage | TLC (tablet_storage: crash at every step), then code check, then reproducer; Jira found it known |
| 7 | vnodes-to-tablets finalization accepts a down node on its cached load stats: a node that upgraded, went back to vnodes and went down within a refresh interval counts as tablet-based; the keyspace switches to tablets and, from reading, that node's next boot hits `on_internal_error` on vnode-split sstables; no rollback possible | data loss: the finalization succeeds with the node down, and on its boots hundreds of its keys are missing (RF=1: data gone); not the refused boot the reading predicted | known mechanism: open PR #31829 describes it and refuses finalization while a node is down; found independently; reachable today on 2026.3, and on master for migrations without published modes (278e523e5a0 closes the rest) | test/cluster/test_vnodes_to_tablets_finalize_down_node.py (TOPOLOGY_CURRENT_STORAGE_MODE suppressed, 3600 s refresh interval, X re-marked for tablets while dropping group 0 appends, then killed before finalize): `[downgraded_down]` fails 3 of 3 ("Data mismatch: missing keys ..."); controls `[tablets_down]` and `[downgraded_up]` pass; fix: upstream PR #31829 (refuse finalization while a node is down), not tried | vnodes_tablets_migration | TLC (vnodes_tablets_migration: cached load stats encoded as the code keeps them); PR search found it known |
| 8 | logstor: TRUNCATE discards only the full segments of the table's groups (zeroing their first block); the truncated records stay in mixed segments, which are freed without zeroing, and recovery indexes every record of a valid segment with no truncation time to filter by, so a restart brings the truncated rows back | resurrection | new mechanism; logstor is experimental; open task SCYLLADB-5484 ("implement TRUNCATE for logstor that works correctly with concurrent writes and recovery") names no mechanism | `test_truncated_logstor_rows_stay_gone[True]` in `test/cluster/test_logstor_truncate_recovery.py` (100 of 100 rows back after a graceful restart; `[False]` passes) | logstor | TLC (logstor spec from the design doc: first counterexample), then code check, then reproducer |
| 9 | logstor: writes of a key with the same timestamp aren't reconciled like LSM: the runtime index keeps the later arrival (a DELETE then an INSERT at one timestamp leaves the row visible; value 2 then 1 leaves 1), and recovery keeps the record in the segment with the higher sequence, so after the separator copied the earlier write into a newer full segment, a crash changes the value reads return | resurrection at equal timestamps; reads change across a restart | runtime half known: SCYLLADB-6194 (open); the recovery half isn't in it | `test/cluster/test_logstor_equal_timestamps.py`: `test_logstor_reconciles_equal_timestamps_like_sstables[delete_first, larger_value_first]` and `test_logstor_equal_timestamp_survives_restart` ("(1, 2) before the restart and (1, 1) after it") all fail | logstor | TLC (logstor: values and timestamp ties), then code check, then reproducers; Jira found the runtime half |
| 10 | a write's replay position is taken at the commitlog add, before it reaches the memtable (`database::do_apply()`), and nothing orders the two; a later write applied first, then a flush, raise the group's flushed position (`compaction_group::_highest_flushed_rp`, 2da478abe9b) above the earlier write, which lands in the next memtable; the cleanup record a tablet repair saves there makes replay skip it | data loss (acked, all replicas) | new (no Jira or PR, 2026-11-14); same family as SCYLLADB-6071 (fixed), not covered by its fix; window in production: any yield between the add and the apply (`unfreeze_gently()` of mutations over 128 KB, dirty memory waits) during a repair | `test_cleanup_record_keeps_a_write_applied_after_a_later_one` in `test/cluster/test_commitlog_cleanup_record_reordered_write.py` ("lost on 2 of 2 replicas" at CL=ALL), injection `database_apply_after_commitlog_add` (new) | commitlog_cleanup | TLC (commitlog_cleanup: add and apply as two steps), then code check, then reproducer with a new injection. Second consumer, by reading (2026-11-15): TRUNCATE saves max(low mark, `highest_flushed_replay_position()`) as its record (`truncate_table_on_all_shards()`), so a write that takes its position after the low mark and reaches the memtable after the truncate's flush survives the truncate and is then skipped by replay; only writes concurrent with TRUNCATE, not reproduced (needs a pause between the mark and the flush) |
| 11 | counters with tablets: a migration within a rack hands the rack's counter id from the leaving to the new replica, which continues the shard from its own copy; an increment the leaving replica led after streaming and whose write to the new replica failed (CL below ALL) leaves that copy behind, and the new replica's next increment reuses a logical clock already written, so reconciliation keeps one of the two | lost increment (permanent, all replicas) | new (no Jira or PR, 2026-11-14; PR #32962 only fixes the SCYLLADB-6164 test); SCYLLADB-5413 (counter migration) states the one-writer-per-id invariant but not that the new writer starts from the latest shard; a late hint doesn't help (lower clock) | `test_counter_increment_survives_tablet_migration` in `test/cluster/test_counter_tablet_migration_stale_shard.py` ("the counter is 6 after 7 increments"), injections `pause_after_streaming_tablet_before_fetch_column_mappings`, `database_apply` (throw) | counters_tablets | TLC (counters_tablets from counters.md), then code check, then reproducer |
| 12 | tablet-aware restore skips the sstables marked `downloaded` in `system_distributed.snapshot_sstables`, so that a retried restore doesn't fetch them again; nothing resets the flag and the manifest's INSERT doesn't write it, so for the 3 days the entries live a second restore of the same snapshot (e.g. after a TRUNCATE) restores nothing and reports success | data not restored (silent) | new (no Jira or PR, 2026-11-14) | `test_restore_tablets_twice_from_one_snapshot` in `test/cluster/object_store/test_backup.py` ("restored 0 of 10 rows") | none | code reading while preparing a restore spec (the retry-skip comment in `download_tablet_sstables()`), then reproducer |
| 13 | the auth cache's permission sets: a lookup in flight stores, into the live record, a set it read before an inherited role's change; the clear that `load_roles()` does for the inheriting roles ran meanwhile, so the stale set stays on that shard (the Veil trace: a lookup of R reads before GRANT of the permission to R's parent, stores after the clear, and R stays refused) | stale permission (a revoked one honored, or a granted one refused, on one shard, until the role changes again or a restart) | known: SCYLLADB-4206 (New); LDAP's `reload_all_permissions()` is not affected (it holds the shard's `_loading_sem`, which the reload's `distribute_role()` takes before the clear) | the ticket's injection test | auth cache (veil/AuthCache.lean) | code reading while writing the spec, then the Jira search found it known; the Veil model finds it and proves a candidate fix |
| 14 | the auth cache's `load_all()` (a node applying a group 0 snapshot) replaces the role records one at a time and clears no cached permissions afterwards; a lookup of a member between the replacement of its record and of its parent's reads the parent's old superuser flag from the cache records (`find_record()`) and stores the result into the member's new record (Veil trace: `ALTER ROLE P WITH SUPERUSER = true` in a snapshot, R refused; with the property restricted to granting, a revoked superuser's member keeps everything) | stale permission (a revoked superuser's member keeps its permissions, or a member is refused, on one shard, until the role changes again or a restart) | new (Jira and GitHub searched: nothing on load_all(); PR #27573 put `_loading_sem` on `load_all()`, which lookups don't take) | test/cluster/auth_cluster/test_auth_cache_load_all_race.py, fails 3 of 3 on master: after `ALTER ROLE P WITH SUPERUSER = false`, R is refused on the leader and the third node and allowed only on the node that applied the snapshot; passes 3 of 3 with the candidate fix (commit "TEST-ONLY candidate fix") (test-only pause `auth_cache_load_all_pause` in `load_all()`; the node drops group 0 appends so it learns of the change only from the snapshot) | auth cache (veil/AuthCache.lean) | extending the Veil auth cache model to `load_all()`, `prune()` and anonymous permissions; the model checker's first trace |
| 15 | `group0_state_machine::reload_state()` (a node applying a group 0 snapshot) recomputes the effective service levels (`update_service_levels_cache()`) before it reloads the auth cache (`load_all()`), and the effective service levels are computed from the auth cache's role records (`query_all()`, `query_all_directly_granted()`, `query_attribute_for_all()`); `reload_modules()` has the right order (`load_roles()` first). Since 19da1cb6569 | wrong service level (timeout, workload type, shares) of the roles a snapshot changed, on that node, until the next service level or role change; boot is fine (main.cc recomputes after auth starts) | new (Jira searched) | test/cluster/auth_cluster/test_service_level_snapshot_order.py: after ATTACH SERVICE LEVEL received in a snapshot, `LIST EFFECTIVE SERVICE LEVEL` says the role has none on the lagging node only; fails 3 of 3, passes 3 of 3 with the two calls swapped (commit "TEST-ONLY candidate fix: reload_state() updates service levels") | none (code reading) | after #14, comparing `reload_state()` with `reload_modules()` module by module: a snapshot must leave the same derived state as the commands it replaces |
| 16 | `view_update_generator`'s fiber (db/view/view_update_generator.cc) takes the whole queue of tables with staging sstables into a local map; when processing one table throws, it re-queues that table only and `break`s, and the local map with the other tables of the pass is destroyed: their staging sstables are never processed until a restart, and their `_registration_sem` units are never returned | missing view rows (base rows without view rows) on that node until a restart; a persistent error on one table (corrupt staging sstable, I/O error, a view update error such as SCYLLADB-6142) drops the others on every pass; leaked registration units can later block staging registration | new (Jira and GitHub searched) | test/cluster/mv/test_mv_staging_failure_drops_others.py: two tables with views, repair brings their rows back as staging sstables while `view_update_generator_consume_staging_sstable` fails every pass; after it is disabled, one view stays at 0 of 1000 rows; fails 3 of 3, passes 3 of 3 with the rest of the pass re-queued (commit "TEST-ONLY candidate fix: view_update_generator re-queues") | none (code reading) | a subagent hunting view-building data bugs, briefed with the patterns of #13-#15 (state taken in bulk, a failure path that restores only part of it); checked by reading, then reproduced |
| 17 | counters on tablets: a replica's counter id is its rack's (`table::update_effective_replication_map()`, `topology::get_rack_uuid()`, b71762d5daa), which assumes one replica per rack; nothing enforces it (`rf_rack_valid_keyspaces` defaults to false and isn't required for counters; the vnodes-to-tablets migration accepts an RF-rack-invalid keyspace with a warning), so RF=3 in one rack gives three replicas one counter id: two leaders read the same shard and write the same id with the same next clock, and the merge keeps one | lost counter increments (acknowledged), also permanent divergence between replicas (repair can't break a clock tie) | new (Jira and GitHub searched; SCYLLADB-6164 is a different counter id issue) | test/cluster/test_counters_single_rack_rf3.py: +10, then +1 and +2 led concurrently by two replicas (`apply_counter_update_delay_100ms` between the read and the transform); one rack: 12 or 11 on every replica, fails 3 of 3; three racks (control): passes 3 of 3. Without injection (`test_concurrent_counter_increments_without_injection`: 8 clients on two replicas, 2,400 increments at QUORUM): one rack loses 641-695 (about 28%), 3 of 3; three racks lose none | none (code reading) | a subagent hunting counter bugs beyond #11, checking where two nodes can write one counter shard; checked by reading, then reproduced |
| 18 | batchlog replay (db/batchlog_manager.cc `process_batch()`) checks each table of a batch (dropped? truncated after the batch?) when it decodes it, and sends after the replay rate limiter, without checking again; a DROP TABLE of one of its tables in between makes the send throw `no_such_column_family`, which the replay takes as "drop the batch", and a TRUNCATE in between lets that table's pre-truncate mutations through | DROP: the batch's mutations to the other tables are never applied, although the client got WriteTimeout(BATCH), which promises they will be; TRUNCATE: truncated rows come back | new (Jira and GitHub searched) | test/cluster/test_batchlog_replay_table_race.py (test-only pause `batchlog_replay_before_send` before the limiter; the batch is logged and not sent with `storage_proxy_fail_send_batch`): DROP: a has 0 rows; TRUNCATE: b's row comes back; 6 of 6 fail; with the tables checked again right before the send (commit "TEST-ONLY candidate fix: batchlog replay re-checks tables") 6 of 6 pass | none (code reading) | a subagent hunting batchlog bugs with the "value captured at enqueue time, used later without re-validation" lens; checked by reading, then reproduced |
| 19 | tablet-aware restore's `downloaded` flag in system_distributed.snapshot_sstables is keyed by (snapshot, dc, rack, first token, sstable id), with no replica identity (sstables_loader.cc, `update_sstable_download_status()`); two replicas of a tablet in one rack (RF above the rack count, allowed with `rf_rack_valid_keyspaces` off, the default) share it, so once one marks an sstable the other skips it | a restore that reports success leaves a replica without the data (fewer copies than RF; CL=ONE reads served by it miss the rows); deterministic on the retry after a failed restore, which the flag was built for | new (Jira and GitHub searched); distinct from #12 (same flag, nothing resets it after TRUNCATE) | test/cluster/object_store/test_restore_two_replicas_one_rack.py: RF=2 on 2 nodes; one replica's download fails once (`fail_download_sstable`), the restore fails, the retry succeeds; one rack: keys on 1 of 2 replicas, 10 of 10 fail (S3 and GCS); two racks (control): 4 of 4 pass; without the failure: pass | none (code reading) | a subagent hunting restore bugs with the "identity assumed unique but shared" lens from #17; checked by reading, then reproduced |
| 20 | `table::update_tombstone_gc_rf_one()` (4f5310bc725) marks a table RF=1 from `_erm->get_replication_factor()`, the keyspace strategy's RF, and repair-mode tombstone GC then collects its tombstones immediately (tombstone_gc.cc); a multi-step RF change (every DC 0<->N) keeps the old options until it completes, so while it extends an RF=1 keyspace to a new DC the tablets already have replicas there and every replica still purges immediately | resurrection: a replica that missed a delete keeps the row while the others purge the tombstone, and repair or read repair spreads it back; for the whole (possibly long) change | new; adjacent to SCYLLADB-4629 (In Review: steady RF=1 tables purge without flushing hints or batchlog), which doesn't cover a change | test/cluster/test_rf_one_gc_during_multi_rf_change.py: RF change `{dc1: [rack1a]}` -> `+ dc2: [rack2a, rack2b]` held after the tables are extended (`determine_rf_change_actions_per_rack_throw`); delete at CL=ALL, flush, major compaction: the dc2 replicas keep nothing of the deleted partition (tombstone purged); after the change completes the same sequence keeps the tombstone; fails 3 of 3 | none (code reading) | reading the boot-vs-runtime derived state (the RF=1 registry) after the RF change review; confirmed by reading, then reproduced |
| 21 | TRUNCATE without a flush (`auto_snapshot: false`, or any table not on local storage, where it never flushes) clears the memtables, whose writes stay in the active commitlog segment; the truncation record is the highest replay position of the discarded sstables (`database::truncate()`, `table::discard_sstables()`), and the low mark taken before the clear is used only when there are none (PR #7005 replaced the `low_mark <= rp` assert that caught this, #6995, with that fallback), so with any sstable older than the memtable's writes the record is below them | resurrection: after a crash (any unclean stop) before the segment is recycled, commitlog replay brings back every truncated write that was in a memtable, and the boot flush makes it permanent; on every replica that crashes | new (Jira and GitHub searched; SCYLLADB-5394 is the views-only variant and says the base table is covered by the low mark, which holds only without sstables) | test/cluster/test_truncate_without_flush_record.py: one node, `commitlog_sync: batch`, `auto_snapshot: false`; insert, flush, insert 100 rows, TRUNCATE, kill, start: 100 of 100 rows back, fails 3 of 3; without the first flush (control) passes 3 of 3; with the record taken as max(sstables' position, low mark) (commit "TEST-ONLY candidate fix: truncate records at least the low mark") both pass 3 of 3; no injection | none (code reading) | a subagent's commitlog review (the commitlog added to the focus), looking for truncation records that don't cover writes made before the truncate; confirmed by reading `database::truncate()`, then reproduced |
| 22 | commitlog segment ids start at the machine's uptime in ms (`runtime::get_boot_time()`, steady_clock; commitlog.cc `from_db_config()`, `segment_manager::init()`) or above the segments on disk, so after a machine reboot and a clean shutdown they restart far below the previous run's; TRUNCATE records the highest position of the discarded sstables (`table::discard_sstables()`), including sstables flushed before the reboot, in the old, higher id space, and replay skips every entry of the table at or below it; the records dropped at boot (#15354, PR #15532) are only the ones written before the reboot; restoring this node's own snapshot (refresh, upload dir) after a reboot brings old positions back the same way | lost acknowledged writes: every write to the table after the TRUNCATE is skipped by commitlog replay after a crash, until the table's next flush or TRUNCATE; on every replica that crashes, any `auto_snapshot` | new (Jira and GitHub searched; SCYLLADB-5134 (In Review) has the same root cause for strong consistency's raft truncation records only) | test/cluster/test_truncate_after_reboot_record.py: one node, `commitlog_sync: batch`; insert, flush, clean restart with `decrease_commitlog_base_segment_id` (imitates the reboot, as `test_commitlog.py::test_reboot` does), TRUNCATE, 100 inserts, kill, start: 0 of 100 rows, fails 3 of 3; without the injection (control) passes 3 of 3; with the record taken as the table's low mark only (commit "TEST-ONLY candidate fix: truncate records the low mark only", which also fixes #21) both reproducers and both controls pass 3 of 3, and test_commitlog.py, the cluster truncate tests and database_test.cc (67 cases) pass | none (code reading) | a second subagent commitlog review (the parts the first didn't read), looking for replay positions compared across id spaces; confirmed by reading `segment_manager::init()` and `database::truncate()`, then reproduced |
| 23 | a vnode repair picks its participants from the natural replicas (repair.cc `get_neighbors()`) and records the range in system.repair_history when all of them took part (row_level.cc `update_system_repair_table()`); during a streaming bootstrap (or decommission) the joining node is a pending replica that streamed the range from the node losing it, and rows the repair writes to that node afterwards go to its sstables, not to the joiner; nothing stops a user repair during a topology change | resurrection: the natural replicas purge the tombstones the joiner lacks (repair-mode tombstone GC, the default for vnode tables too), and once the joiner owns the range the deleted rows are back; needs a replica that missed the delete (hints off or expired) and a repair overlapping a scale-out or scale-in | new (Jira and GitHub searched; SCYLLADB-6092 is the tablet variant with a different mechanism; SCYLLADB-93 is decommission waiting on a repair's erm) | test/cluster/test_repair_during_vnode_bootstrap_gc.py: 3 nodes RF=3 (vnodes, hints off), one node misses the deletes of 200 rows; a 4th node bootstraps, held after streaming (test-only pause `bootstrap_pause_after_streaming`); repair, release, compaction: 45 to 70 of 200 deleted rows back at CL=ALL, fails 3 of 3; repair after the bootstrap (control) passes 3 of 3; the test runs without the commitlog and with no hints-flush cache (as test_data_resurrection_in_memtable.py does) so that GC follows the repair time at once; skipping ranges whose end token has pending replicas still lost 4 to 30 rows (the repaired range can contain a joining token); recording nothing during a topology change (commit "TEST-ONLY candidate fix: vnode repair records nothing during a topology change") passes 3 of 3 | none (code reading) | a subagent review of repair-time correctness in tombstone GC (when is a repair time recorded although some replica lacks data?); confirmed by reading `get_neighbors()` and `update_system_repair_table()`, then reproduced; the first runs needed the commitlog cap and the 60 s hints-flush cache out of the way, found from the comment in test_data_resurrection_in_memtable.py |
| 24 | tablet load-and-stream (`nodetool refresh --load-and-stream`) streams an sstable spanning several tablets once per tablet and marks it for deletion after each successful batch, to unlink the marked ones after the last tablet (sstables_loader.cc `tablet_sstable_streamer::stream()`, `defer_unlinking::yes`, PR #29599); when a later tablet's stream fails, `stream()` throws before that, but `sstable::close_files()` unlinks every sstable marked for deletion when its last reference goes | data loss of the loaded data: the upload directory loses the sstable, so a retried refresh can't load the rows of the tablets that weren't streamed (they exist nowhere else on the node); any stream failure after the first tablet (receiver error, node restart, abort) | new (Jira and GitHub searched; PR #29599 / SCYLLADB-1647 added the deferral for the success path only) | test/cluster/test_refresh_failure_keeps_upload_sstables.py: one node, an sstable from a 1-tablet table staged in the upload dir of a 4-tablet table; a test-only injection fails every batch after the first (`load_and_stream_fail_after_a_batch`); the refresh fails, the upload dir is empty, and the retry loads 46 of 200 rows, fails 3 of 3; without the failure (control) passes 3 of 3; unmarking the sstables when `stream()` fails (commit "TEST-ONLY candidate fix: tablet load-and-stream unmarks sstables on failure") passes 3 of 3 | none (code reading) | a subagent review of the sstable import paths (what an imported sstable carries and what the loader does on failure); confirmed by reading `stream()` and `sstable::close_files()`, then reproduced; the first test (a receiver-side failure released through `load_and_stream_before_streaming_batch`) never failed: with shared messages, one message releases every later pause, so the failure came after the streaming |
| 25 | repair-based rebuild (default: `allowed_repair_based_node_ops` includes rebuild) applies the small table optimization to user tables under `small_table_optimization_for_rbno_max_table_size` (1 GiB default, cb52178d5d3, SCYLLADB-3248): one repair over the whole ring with every node as a peer (repair.cc); each peer reads the whole ring from its own sstables, including ranges it no longer owns, and only the receivers filter by ownership (row_level.cc `flush_rows()`, `put_row_diff_with_rpc_stream()`), so a node's unowned copies are sent to the owners; the topology coordinator cleans up vnode tables only before removenode and decommission, so after a bootstrap the old owners keep the ranges they gave up until a compaction touches those sstables | resurrection: rows deleted after the bootstrap whose tombstones the owners purged come back on every owner from the stale copy; vnodes only; also through the system keyspaces always optimized and user repair with `small_table_optimization=true` | new (Jira and GitHub searched; the reviews of PRs #30741, #21207, #21952 don't mention stale data) | test/cluster/test_rebuild_small_table_stale_data.py: 3 nodes RF=2 (vnodes, no commitlog, tombstone_gc immediate), insert 200 rows, bootstrap a 4th node (no cleanup), delete all at CL=ALL, flush, major compaction on all but the first node (compaction would drop its sstables' unowned data), rebuild another node via REST: 49 to 97 of 200 deleted rows back at CL=ALL, fails 3 of 3; with `small_table_optimization_for_rbno_max_table_size: 0` (control) passes 3 of 3; no fix tried (filter what a peer sends by its own ownership in the small-table reader) | none (code reading) | a subagent review of row-level repair data movement; confirmed by reading the small-table path and the receive-side filters, then reproduced; the first runs showed no stale copies (a major compaction also cleans sstables that require cleanup) and lost the node (`manager.rebuild_node()` stops it afterwards) |

Veil models (2026-11-14, from when Veil became the single checker; files in
veil/). Each translation was validated against TLC: the same verdict on
every config, the same counterexample length (with one TLC worker, whose
search order is breadth-first; with 8 TLC can report a longer trace), and,
where the encoding allows it, the same state count. "Fix proved" means
`#check_invariants` proved the properties for every instance size from an
inductive strengthening (each goal trusted to the SMT solver, cvc5 or z3,
unless the row says the Lean kernel re-checked it), that the strengthening
holds on the model-checked bounds, and that the proof fails with the fix
removed.

| # | Veil model | matches TLC | fix proved |
|---|---|---|---|
| 1 | Group0StateMachine.lean | 5 configs; state counts equal (452; 11,068) | the current code, Group0StateMachineProof.lean (321 goals); fails with bug #1's generic failure on (`snap_merge_generic` breaks MemMatchesDisk), and without 2f372ec9707 |
| 2 | TombstoneGcRepair.lean | 3 configs; state counts equal (6,889; 2,965) | the earlier of the repair times, TombstoneGcRepairProof.lean (184 goals); fails with the later one (`merge` breaks the strengthening) |
| 3 | CompactionGroups.lean | 5 configs; counts differ (setup and registration steps) | no fix modelled yet |
| 4, 6 | TabletStorage.lean | 8 configs; counts differ (abstract file ids: 535,351 vs 555,966) | both fixes, TabletStorageProof.lean (940 goals); fails without either (`fail_completion` breaks Durable; `restart` breaks the migration invariant) |
| 5 | ScTabletMigration.lean | 15 of 19 configs, same verdicts and trace lengths (bug #5: I1, 40 states); the 4 passing configs with requests or a second migration (among them the rollback-barrier fix of #5) didn't fit in 62 GB without TLC's VIEW and symmetry (MC_current: 110,542,183 states, 15 GB) | not yet |
| 7 | VnodesTabletsMigration.lean | 5 configs; state counts equal | the published modes, VnodesTabletsMigrationProof.lean (204 goals; all but `boot`'s re-checked by the Lean kernel, `boot`'s trusted to the SMT solver); fails with the column off |
| 8, 9 | Logstor.lean | 6 configs; state counts equal (21,659,782 with every feature) | no fix in the code yet |
| 10 | CommitlogCleanup.lean, CommitlogCleanupBench.lean | state counts equal | CommitlogCleanupFixed (kernel-checked) |
| 11 | CountersTablets.lean | 2 configs; state counts equal | no fix modelled yet |
| 12 | none (code reading) | | |
| 13 | AuthCache.lean (new spec) | no TLA+ spec | the candidate fix (a clear voids the lookups in flight on its record), AuthCacheProof.lean (210 goals), with LDAP reloads as the code has them; fails at `reload_clear` without the fix |
| 14 | AuthCache.lean | no TLA+ spec | the candidate fix (`load_all()` clears every role's permissions once every shard has the new records, voiding the lookups in flight) with #13's, AuthCacheProof.lean (240 goals, also covering `prune()` and dropped resources); fails at the commands applied by a snapshot without it |
| 15 | none (code reading) | | |
| 16 | none (code reading) | | |
| 17 | none (code reading) | | |
| 18 | none (code reading) | | |
| 19 | none (code reading) | | |
| 20 | none (code reading) | | |
| 21 | none (code reading) | | |
| 22 | none (code reading) | | |
| 23 | none (code reading) | | |
| 24 | none (code reading) | | |
| 25 | none (code reading) | | |

Reproducer stability (2026-11-15, bench, dev mode, `--repeat 5`, this branch
with the four TEST-ONLY candidate fixes reverted, b318628230a): every
reproducer failed 5 of 5 with its own assertion, every control passed 5 of 5
(110 failed, 55 passed). Reproducers: #1, #2, #3, #4, #6, #8 (`[True]`), #9
(3 tests), #10, #11, #12, #14, #15, #16, #17 (both, one rack), #18 (drop,
truncate), #19 (S3 and GCS, one rack, failure first), #20. Controls: #2
(no merge), #6 (`[False]`), #8 (`[False]`), #17 (three racks, both tests),
#19 (the other six variants). No reproducer: #5, #7; #13 has the ticket's
test.
Same check for #21, #22, #23 (fc3f003cae1, their TEST-ONLY fixes reverted):
every reproducer failed 5 of 5 (#23: 38 to 88 of 200 rows back), every
control passed 5 of 5 (15 failed, 15 passed).
Same for #24, #25 (0a530a30c08, #24's TEST-ONLY fix reverted): every
reproducer failed 5 of 5 (#25: 38 to 78 of 200 rows back), every control
passed 5 of 5 (10 failed, 10 passed).

Commitlog positions across split, merge, migration and drain (third subagent review, 2026-11-15), nothing new:
- Cleanup records are per tablet range (`token_min_pos()`), taken after the leaving shard flushed; a tablet that comes back gets positions above the record (positions grow within a run).
- Split-ready and merged groups start with an invalid flushed position and get one only from their own flushes; merge takes the minimum over the merging groups (too low only, safe).
- `stats_metadata().position` is read only by `discard_sstables()` (#21, #22) and compaction, both filtered by origin host and shard; `_highest_flushed_rp` comes only from flushes of the current run.
- Cleanup and truncation records are dropped after replay at boot (main.cc), so none crosses a restart.
- `drain()` flushes, then shuts down the commitlog, which deletes only clean segments; decommission's tablet cleanup flushes first.

Schema changes vs data (subagent review, 2026-11-15), nothing solid:
- Drop time of a column comes from the DDL node's clock (`schema_builder::remove_column()`, `api::new_timestamp()`), not the group 0 timestamp, and a re-added column inherits it; base tables have no leeway (CDC log tables do, cdc/log.cc). With node or client clock skew, a write acknowledged after the re-add with a timestamp at or below the drop time is filtered only by sstable readers and the converting applier: readable from memtable/cache, gone after a flush and a cache-bypassing read or restart. Near documented behaviour (docs/cql/ddl.rst on timestamps) and SCYLLADB-2660; lead only.
- DROP TABLE, CREATE ... WITH ID while a node lags in group 0: streaming/repair from the lagging node writes the old table's rows into the re-created one by id (a third channel next to SCYLLADB-5245, 5251); not verified whether tablet migration sessions close on the drop.
- Ruled out: dropped_time round trip, hints' and commitlog's column mappings, writes with unknown schema versions (barrier, then fetch, loud failure), pre-drop cells in hints/replay (converting applier filters), counter column re-add (refused); known: PR #29544/SCYLLADB-1563, PR #32365/GH #9930, GH #24290/#24305/PR #32366.

Materialized views (subagent review, 2026-11-15):
- Lead, not reproduced: `view_updating_consumer::do_flush_buffer()` (db/view/view.cc) catches every error of a staging pass's view update push, logs a warning and goes on, and the sstable leaves staging; `generate_and_propagate_view_updates()` throws `overloaded_exception` ("Too many view updates started concurrently", 59d5bfa742b) when the view update semaphore is exhausted, which for a staging pass (repair, load-and-stream) drops the partition's view updates while its base rows land; permanent at RF=1 or with load-and-stream. The swallow is known for the parse_assert trigger (SCYLLADB-6142); GH #21535 covers overload during staging but assumes a retry. Tried test/cluster/mv/test_mv_staging_overload_drops_updates.py (2 nodes RF=1, vnodes, `view_update_limit` and `delay_before_remote_view_update` at startup, 1000 rows of 1 KB and 8 KB through load-and-stream): no push failure was logged, the view got every row; the overload wasn't reached, cause not found.
- Correction to #16: an error like SCYLLADB-6142's is swallowed by `do_flush_buffer()` and never reaches #16's catch; #16 holds for the errors that escape (the reader, permits), as its reproducer's `view_update_generator_consume_staging_sstable` injection does.
- Lead, weak: a staging pass reads through `as_mutation_source_excluding_staging()`, which hides every staging sstable, so two concurrent passes over one tablet (one via the view building coordinator, one directly to the generator) don't see each other's sstables (ghost view row when the deleting sstable and the older inserting one are in different passes).
- Ruled out: write vs write on one shard (`local_base_lock` held through commitlog add and memtable apply); pending base replica pairing (only duplicates).

Boot-time sstable handling (subagent review, 2026-11-15), nothing reproducible:
- Lead, power loss only: `create_pending_deletion_log()` (sstables/sstable_directory.cc) neither fsyncs the log nor syncs `pending_delete/` after the rename, only its parent (`base_dir.sync()`, since f47b5e60bcd); the inputs' unlinks follow with `sync_dir::no`. On a file system that may persist a later unlink before the rename, a power cut can leave partly deleted inputs with no log (refused boot or resurrection). XFS/ext4 journal metadata in order, and a kill keeps the page cache, so a cluster test can't show it; a one-line hardening.
- Ruled out: boot reshard/reshape (no tombstone purge, duplicates only), compaction completion order, tablet cleanup/truncate/quarantine commits, TemporaryTOC moves and links, staging moves vs deletion, unowned tablet sstables at boot (refuses to boot), pending-delete replay order, scan before process, file-streaming generations, object-storage registry GC, upload dir scans, leftover logs after a swallowed `execute()` failure.

Reads during tablet transitions (subagent review, 2026-11-15), nothing new:
- Known: read repair writes only to the replicas that answered, not the pending one (SCYLLADB-1969, GH #20458); CL sized from the strategy RF while tablets have more replicas during a multi-step RF change (GH #20625, SCYLLADB-3632, 5983, draft PR #31903).
- Ruled out: stage-to-read-set mapping, replica-side fencing before and after the query, barriers before cleanup and in rollback stages, storage removal only at end/revert, intra-node read shard until write_both_read_new, multishard reader refusing tablets, range splitter, querier cache position checks, pager re-split from the last position with one erm per scan, split/merge group remapping, short-read merging.

Final validation (2026-11-15, bench, dev mode, every reproducer of #1-#25):
- Without the fixes (d893ab32175: this branch with every TEST-ONLY candidate fix reverted), `--repeat 5`: every reproducer variant failed 5 of 5 (149 failures), every control passed (96). #18 `[drop]` ran 4 times (test.py didn't schedule the fifth); all 4 failed.
- With every fix (8d125e0f12d), `--repeat 3`: every reproducer passed 3 of 3 (138), except #2, #6 and #7, whose fixes are upstream PRs not on this branch (#32897, #33027, #31829), as expected. The logstor unit tests (120 cases) pass with the #8 and #9 fixes.
- Regression runs with the fixes: test_tombstone_gc.py, test_repair.py, object_store/test_backup.py, test_schema_merge_failure_stops_group0.py, test_counter_tablet_migration_stale_shard.py (213 passed with the first batch); test_commitlog*.py, test_incremental_repair.py, auth_cluster/, test_counters_with_tablets.py, test_data_resurrection_in_memtable.py, test_logstor*.py (107 and 134 passed). The only failure: `test_counter_ids_reuse_in_single_rack`, which pins the rack-shared counter ids that the #11/#17 fix replaces with per-host ids (a deliberate trade-off: more counter shards over migrations).

Retest on master be3f06f4c38 (2026-11-25, local dev build, branch tla-new-bug-reproducers: master plus the reproducers of the 20 new bugs and their injections, no fixes, each failing case marked xfail(strict=True)): `--repeat 3` gave 51 passed and 69 xfailed, so every bug still reproduces. A single run with `--runxfail` showed each failing with its own assertion. #19's GCS variant passed once there: the failed restore could stop the other replica's downloads before it marked its sstables. The test now holds the failing replica (`pause_tablet_restore`) until the other one has marked every sstable; afterwards it failed 10 of 10 (S3 and GCS) and its controls passed 30 of 30. #9's test of equal timestamps at runtime is left out of that branch (SCYLLADB-6194).
This branch rebased on the same master, with every fix, one run of every reproducer: all pass except #7 (xfail, open PR #31829) and `test_counter_ids_reuse_in_single_rack` (xfail with the #11/#17 fix). #2 and #6 now pass without a fix of this branch: master has their upstream fixes.

Candidate fixes (all TEST-ONLY commits on this branch, each validated by its reproducer):
- #1 a schema apply that fails after writing the mutations to the commitlog is reported as persisted-but-not-applied (stops group 0), a failure before it is left to raft's retry; #3 GC sstables inherit the compaction's repaired_at; #4 a failed completion doesn't unlink outputs that are also inputs; #5 `with_barrier::yes` restored at sc_rollback; #8 logstor recovery skips records at or below the table's truncation time (trusts write timestamps, ms resolution); #9 a 32-bit value-aware tie-break in the logstor index entry (deletion wins, then the value's first 4 bytes; prefix ties remain); #10 repair's cleanup record capped below the oldest commitlog segment with unflushed writes of the table (segment granularity: more replay, never less); #11 and #17 a counter id of the replica's own (host id mixed with the shard, so intra-node migrations don't hand one over either); #12 and #19 tablet restore doesn't skip on the shared `downloaded` flag (a retry re-downloads, duplicates only); #13 an epoch bumped by every permission clear, a lookup stores only if unchanged; #14 clear cached permissions after `load_all()`; #15 service levels updated after the auth cache; #16 the rest of a failed pass re-queued; #18 tables re-checked right before the send; #20 RF=1 only when every tablet has one replica and none is in transition; #21 and #22 the truncation record is the table's low mark; #23 no repair history recorded for vnode tables during a token ring change, with or without the small table optimization (not covering a change that starts mid-repair); #24 tablet load-and-stream unmarks sstables on failure; #25 the vnode repair reader skips the partitions the node neither owns nor is about to own while filling its buffer.
- #2, #6: upstream PRs #32897, #33027, merged on master. #7: open upstream PR #31829.
- #7's reproducer is marked xfail (strict) until that lands; `test_counter_ids_reuse_in_single_rack`, which pins the rack-shared counter ids the #11/#17 fix replaces, is marked xfail with the fix.

Code review of the series (2026-11-15) and what was done:
- Changed after the review (validated locally, dev build, `--repeat 3`: the affected reproducers 51 passed and 9 xfailed; test_repair.py, test_counters_with_tablets.py, test_tombstone_gc.py pass): #25 filtered after the buffer was filled (an unowned stretch read as the end of the node's data) -> filtered while reading; #23 skipped the small-table branch and applied to tablet tables -> both repairs, vnode tables only; #11/#17 still handed an id over in intra-node migrations and kept a dead rack-count block -> per-shard ids; #1 stopped group 0 on any apply failure, also one that persisted nothing -> only after the commitlog write.
- Known limits, left as they are: #21/#22 fix covers the base table's truncation record, not its views' and indexes' (each still records its discarded sstables' position); #10 caps the record by the table-wide oldest dirty segment on the shard, so another tablet's memtable can pull a range's record low (more replay of data whose tombstone may be purged); #8 trusts write timestamps (also when segments are streamed in a migration); #9 ties remain for values sharing a 4-byte prefix (most small bigints); #12/#19 a retried restore downloads everything again, and the per-sstable `downloaded` write is now dead.


Open leads, not bugs yet:
- Restored sstables keep their `repaired_at` (download/clone copy Statistics; `attach_sstable()` resets only the level), and the receiver judges them against its own tablet's `sstables_repaired_at`, which only grows and survives TRUNCATE: per-node snapshots taken at different repair rounds can make unrepaired data count as repaired (incremental repair skips it; repaired-only tombstone GC purges against it). Subagent review 2026-11-15, medium-low confidence; needs a filtered incremental repair, snapshot, repair, TRUNCATE, `refresh --scope node` per node; a full repair after restore (Manager, draft PR #32351) mitigates.
- SCYLLADB-6108: confirm bug 4 from node-2's log (look for `Failed to complete splitting ... retrying`, `Unable to remove input SSTable`, `bad_alloc` before 22:23 on 2026-11-06).
- Exception safety of every path that deletes or moves sstables (cleanup, scrub, tablet and intra-node migration cleanup, off-strategy).
- SCYLLADB-6092: incremental_repair_gc finds nothing under the code's semantics.
- Bug 7: a reproducer would suppress TOPOLOGY_CURRENT_STORAGE_MODE (`suppress_features`, as `test_vnodes_to_tablets_migration.py` does) and raise the load stats refresh interval after the first refresh; worth it only if PR #31829 stalls.
- vnodes-to-tablets replace (supported mid-migration, moves data) isn't modelled yet.
- logstor split with a write still in the pipeline (spec logstor_split): TLC showed readiness declared while a write that picked the main group is still queued for the separator; a test holding the separator task (test-only injection, since removed) saw readiness wait for it, so it is not reachable. While the task was held the split monitor looped about 12k times a second; a separator stuck for good (SCYLLADB-5944) would keep a split spinning. Performance only.
- Ruled out by one code check each (2026-11-14): logstor compaction's index update is conditional (`update_record_location()`); a counter or Paxos write reaches every write shard of an intra-node migration (`apply_on_shards()`); LWT participants during a migration (quorum of natural plus pending); a hint whose target is no longer a replica is re-sent to the current replicas (`hint_sender::send_one_mutation()`).

## How each bug was found

1. **group0 snapshot transfer.** Writing group0_state_machine, the model gave
   `transfer_snapshot()` the code's error handling: only
   `schema_persisted_but_not_applied` stops the state machine. TLC showed a
   generic exception after the schema was persisted lets raft retry the
   snapshot on a node whose schema already changed. Reading the code confirmed
   it; the reproducer needed a new injection (`database_apply_many_fail`)
   after `do_apply_many()` persisted the mutations.
2. **tablet merge raises the GC horizon (SCYLLADB-6091).** tombstone_gc_repair
   modelled per-tablet `repair_time` and a merge taking the max of the two
   halves, as `merge_tablet_info()` does. TLC found a tombstone purged before
   the half repaired less recently was repaired, resurrecting the data. The
   cluster test showed it; a Jira search then found SCYLLADB-6091.
3. **GC sstables marked by incremental repair (SCYLLADB-5896).**
   compaction_groups found a capture that doesn't filter what other tasks have
   registered (scrub, component rewrite) taking the same sstable as another
   compaction. Asking where else the code captures without filtering led to
   incremental repair's marking. Reading why the sstables were unrepaired
   found that the GC writer never sets `repaired_at`. Getting the reproducer
   to fail took GC sstables (random, uncompressed values), an abortable pause
   after they were attached, and a pause between the rewrite's views.
4. **failed split bypass deletes the live sstable.** While writing the bypass
   step of compaction_groups, the question was what happens when the bypass's
   completion fails. The model's bypass was atomic, so TLC couldn't say; the
   code (9cbab949b4d) deletes the outputs, which for a move are the input. A
   survey of SCYLLADB-6108's timeline pointed at the same path. The
   reproducer needed the commitlog off (replay hid the loss) and a restart
   right after the failure (the split isn't retried).
5. **SC rollback I1.** sc_tablet_migration's invariant I1 failed on rollback
   when an excluded pending replica returns; `git log` showed c59a1208f7e
   removed the barrier that a comment still relies on.
6. **intra-node migration across a restart (SCYLLADB-6226).** Modelling
   SCYLLADB-6108's timeline in tablet_storage, with a crash allowed at every
   step and boot reloading by the read shard, TLC found a restart between the
   streaming response and the coordinator's write_both_read_new. The code
   confirmed both halves: `advance_in_background()` keeps the response across
   the restart, and boot loads by the "previous" read selector. The reproducer
   holds the replica after the load (new injection) so `stream_tablet_wait`
   can hold the coordinator. A search afterwards found SCYLLADB-6226 and its
   fix PR #33027, opened the day before.
7. **vnodes-to-tablets finalization on cached load stats.** Reading the
   migration code for data-loss paths showed two asymmetries in
   finalization: a down node is checked against its cached load stats
   (`require_live_nodes::no`), and the published-mode check is skipped when
   the node published none. vnodes_tablets_migration modelled the cache as
   it is (refreshed for live nodes only, emptied by a leader change) and a
   node's boot direction as `populate_keyspace()` picks it. With modes
   published TLC found nothing; without them it found finalization
   accepting a node that had gone back to vnodes. `git log -S` on the check
   showed 278e523e5a0 added it after 2026.3, for a different reason (the
   status API). Jira had no report; a search of PRs touching the load
   stats collection found open PR #31829, which describes the same window
   (for its new boot override) and makes finalization refuse while a node
   is down.

8. **logstor TRUNCATE resurrected by recovery.** Written from
   docs/dev/logstor.md with little code reading: which segments recovery
   scans (every one with a valid first header), and which frees zero the
   disk (only discard_segments()). TLC's first counterexample was two
   writes, TRUNCATE, crash. Each step was then checked against the code
   (`table::discard_logstor_segments()`, the recovery resolver). The
   reproducer needed nothing but a graceful restart.
9. **logstor equal timestamps.** Adding values and timestamp ties to the
   logstor spec: TLC found the runtime rule (later arrival wins) and the
   recovery rule (higher segment sequence wins) disagreeing after the
   separator copied the earlier write. Reading `default_entry_cmp` and
   `cmp_with_seq` confirmed both, and a second invariant against Scylla's
   reconciliation failed at once. The restart reproducer took three tries:
   a table flush doesn't flush the separator (open PR #32766), and a
   graceful stop does, so it fills the separator buffer and kills the node.
10. **cleanup record over a reordered write.** Reading the 6071 fix's
   claim ("a mutation of this group at or below it is in an sstable"), the
   spec split a write into its commitlog add and its memtable apply; TLC
   found the reordering at once (76 states). The only reordering guard in
   the code is for truncate (`check_valid_rp()`). The first reproducer
   passed because the repair copied the row from the other replica; it
   needed the write held on both.
11. **counters across a tablet migration.** counters_tablets was written from
   docs/dev/counters.md (rack ids, the negated id of a pending replica,
   highest clock wins) with replication to each replica allowed to fail.
   TLC: the new replica continues the rack's shard from a copy that missed
   one increment. Two reproducer attempts passed for reasons the code
   explained: within a node the leader writes every write shard itself
   (`apply_on_shards()`), and right after the migration the new replica
   may not yet consider itself one (the race of SCYLLADB-6164), so the test
   moves between nodes and takes a read barrier before B leads.
12. **a second restore of a snapshot.** Preparing a spec for tablet-aware
   restore (a replica restarting mid-restore, the 6226 pattern), reading
   `download_tablet_sstables()` turned up the skip of sstables already
   marked downloaded "so a re-entered or retried restore doesn't
   re-download". The flag is keyed by snapshot, not by restore request, and
   nothing resets it. No spec was needed; the reproducer restores, truncates
   and restores again.

13. **the auth cache's stale permission sets (SCYLLADB-4206).** Asked for an
   auth cache spec, reading auth/cache.cc for what a model needs showed that
   `load_permissions()` holds only `_permission_loader_sem` while the
   reload's in-place `clear_role_permissions()` takes nothing, so a lookup
   that read before a change can store after the clear. The Jira search
   done before modelling found SCYLLADB-4206, open and with an injection
   test. The Veil model (veil/AuthCache.lean: group 0 commands, the reload's
   replace and clear steps per shard, lookups that read the shard's role
   records and the grants table, LDAP reloads under the shard's
   `_loading_sem`) finds it with five commands. A first version without the
   semaphore also blamed `reload_all_permissions()`; with it, that path is
   safe, and the proof of the candidate fix covers it unchanged.

14. **the auth cache's `load_all()` race.** Asked to cover what the auth
   cache model left out (`prune()`, `load_all()`, anonymous permissions).
   Reading the callers showed `load_all()` runs not only at boot but on
   every group 0 snapshot a running node applies (`load_snapshot()` ->
   `reload_state()`), and that, unlike `load_roles()`, it clears nothing.
   In the model a command may now arrive in a snapshot (every record
   replaced, in any order, nothing cleared); the model checker's first
   trace (one shard, two roles, five commands) is the bug. Restricting the
   property to "grants nothing the tables don't" gives the privilege
   retention trace. `prune()` and anonymous permissions came out safe
   (prune only removes entries; the authorizers answer the anonymous user
   with a constant).

15. **service levels after a group 0 snapshot.** #14 came from the snapshot
   path doing less than the incremental one, so `reload_state()` was
   compared with `reload_modules()` module by module. Auth and service
   levels are in opposite orders: the effective service levels, computed
   from the auth cache, are recomputed before the auth cache is reloaded.
   Deterministic, so no model was needed; the reproducer reuses #14's
   snapshot-only catch-up (the lagging node drops group 0 appends).
   Checked on the way and ruled out: the authorized prepared statements
   cache (time-based by design, 2 s refresh; the batch path inserts only
   after `check_access()`), and the lost update in
   `flush_pending_repair_time_update()` (it only loses repair times, which
   makes tombstone GC more conservative).
   The rest of the comparison came out equal: topology (both paths end in
   `topology_state_load()`; the incremental tablet update re-reads every
   changed row and clears its transition before, as a full read starts
   empty), the views built everywhere that gate tombstone GC on views
   (`update_built_views()`: from the topology reload, or from the view
   building reload that every `system.tablets` change triggers, on both
   paths), CDC streams and compression dictionaries (all of them on a
   snapshot, the changed ones otherwise), and the audit's known roles
   (read after `load_all()`).

DDL vs topology (2026-11-15), read, not modelled:
- Enabling CDC on a table of a keyspace migrating from vnodes to tablets
  creates its log table without a tablet map (7535fabccfa left that guard
  for later; draft PR #32090 lists it as the one unsupported schema change;
  SCYLLADB-1166). Known, and loud: finalization checks every table's map
  when submitted and again in the topology coordinator, whose update is
  conditional on its group 0 guard, so it refuses rather than switching a
  keyspace with an unmapped table.
- Multi-step RF change (`next_strategy_options`): the planner extends base
  tables, then views, and shrinks views, then base tables, each step only
  once the previous one has nothing left on committed replica sets (a tablet
  in transition still counts as missing); `abort_rf_change()` accepts only
  while some tablet lacks a replica, from the same committed sets, under a
  group 0 guard. So no replica is removed while an abort can still be
  accepted. Removenode: a replica on the node being removed stays in the
  committed set until its same-rack replacement is streamed, so it never
  looks missing; RF-change moves skip tablets in transition; a down or
  empty target rack makes the planner abort (next back to the current
  options, added replicas shrunk away). Resize: the RF planner moves merge
  siblings separately, and merge finalization waits for co-location, so
  that only delays a merge. A second RF change on the keyspace is refused
  under the guard (`ongoing_rf_change()`), and an ALTER without
  replication options keeps `next_strategy_options`. Not modelled: reading
  left no interleaving to check.

Also read on 2026-11-15, nothing found:
- TRUNCATE vs tablet migration: the truncate_table transition starts only
  once no tablet transition is in flight, and none starts during it.
  Hints replayed after a TRUNCATE resurrect rows: known, GitHub #6096.
- Replace during the vnodes-to-tablets migration (PR #30727): each phase is
  idempotent per tablet and committed in guarded batches; finalize skips
  tablets already on the replacing node, so a coordinator change between
  batches resumes cleanly; the replacing node starts on vnodes and a
  forward finalization refuses it loudly.
- Group 0 state machine enabled before non-system keyspaces load (PR
  #32522): messaging listens only after reload_schema_dependent_state(),
  so no command from the leader is applied in between; the replay before
  it runs with the state machine disabled.
- Logstor: delete records are never dropped (no tombstone GC to race with
  recovery); the partition cache populates only if the index entry still
  points at the location read, under the read guard that blocks segment
  reuse.

- Compaction's purge check against data flushed mid-compaction: the
  sstable set is a snapshot updated only with other compactions'
  replacements, so a flush moves data out of the memtable check and into an
  sstable the snapshot lacks. Known: SCYLLADB-4747 (and SCYLLADB-4755 for
  fully expired sstables).
- Commitlog replay filters: truncation and cleanup records are compared
  within the writing shard; replay skips tokens the node doesn't own now
  (`shard_for_writes()` empty); tablet cleanup records the table's highest
  position for the range (`cleanup_compaction_groups()`); startup flushes,
  deletes the replayed segments, and only then drops the records.
- Sstable deletion across crashes: on the filesystem the pending-delete log
  is renamed into place before any input goes; on object storage all
  inputs turn "removing" in one registry batch. Object storage shares an
  sstable between nodes through refs/nodes/<host> objects and deletes the
  components when a node drops the last one (count, then delete); a tablet
  stream's destination adds its reference while the source still holds the
  sstable (wipe runs only when the last in-memory pointer drops). Not
  checked further: snapshot references vs compaction, two nodes dropping
  references at once.

Open leads from the view-building review (2026-11-15), confirmed by reading, not reproduced:
- A tablet split finalizing while a staging registration is queued: the
  registration's last token is taken from the tablet map when queued
  (view_building_worker.cc `register_staging_sstable_tasks()`) and committed
  later unchanged, also on a group0_concurrent_modification retry;
  `do_process_staging()` maps it to the tablet holding it now, the right
  child, so the left child's staging sstable gets no task until another
  staging registration of that tablet or a restart, and blocks its split.
- Tablet migration by mutation streaming (file streaming off, or during an
  upgrade before it is enabled): staging data streamed to the pending
  replica lands as normal sstables, and the leaving replica no longer
  generates view updates from use_new on. Not checked yet.

Fencing, row cache, object-storage references (subagent review, 2026-11-15), nothing found:
- Stale coordinators vs tablet cleanup: tablet writes pin the replica-side
  erm right after `check_fence` with no yield, and the barrier before
  cleanup drains all nodes; hints, batchlog, read repair and view updates go
  through write handlers that capture an erm; every rollback stage
  transitions through a barrier.
- Row cache vs external sstables: every external add (streaming, file
  streaming, load-and-stream, refresh) goes through `row_cache::invalidate`;
  the phase and `_prev_snapshot_pos` reject stale populates; merge, split
  and erm refresh move sstables without changing contents.
- Object-storage references: `clone_from()` re-checks the snapshot reference
  after creating its own; two nodes dropping references both delete
  (idempotent); a shared object is never rewritten in place (new sstable id).
  Lead, benign: a reference-sharing clone pins the source only in memory
  (FIXME at stream_blob.cc:854); if the source drops it first, the clone's
  load fails on missing components and the stream retries from the
  compaction output. Question for backup owners: `backup_sstables` skips
  components that exist while an external dropper may be deleting them.

(The RF=1 tombstone-GC lead above became #20.)

Counters and schema changes (subagent review, 2026-11-15), ruled out:
- Counter commitlog replay is idempotent (the transformed shard mutation is
  logged); a failed memtable apply bumps the next clock
  (`_failed_counter_applies_to_memtable`); every storage group gets a counter
  id at each erm update, split and merge included; rebuild_v2 repairs the
  survivors before streaming, so a replaced counter replica starts from the
  highest surviving clock.
- Dropped and re-added columns: every schema upgrade goes through
  `converting_mutation_partition_applier`, which drops cells at or below the
  highest drop timestamp, and the sstable readers filter the same way;
  re-adding a dropped counter column is rejected.

Check-then-await-then-act audit (subagent, 2026-11-15, the #18 lens):
- Lead, weak, vnodes only: TRUNCATE doesn't wait for streams or repair
  writers already writing sstables (`_pending_streams_phaser` is closed only
  on DROP); an sstable created before `truncated_at` and added after
  `discard_sstables()` survives (streaming/consumer.cc: lookup, create,
  write, add), so pre-truncate rows being repaired or streamed in come back.
  Tablets are fenced (truncate waits for no transitions, behind a barrier).
- Ruled out: hint sending (a DROP of another table costs one segment retry,
  not a loss); repair followers answering a missing table only after a
  group0 barrier applied the DROP; stream and file-stream handlers (table
  lookup and `stream_in_progress()` in one continuation); load-and-stream
  (holds the erm and the stream guard); every other `catch
  (no_such_column_family&)` covers one table, one tablet or one entry.

Node operations on tablets (subagent, 2026-11-15), nothing new:
- Known: a queued filtered user repair narrows the rebuild_v2 repair of a
  removenode/replace/RF extend (open PR #32064, SCYLLADB-4970).
- Lead, availability only: new-table allocation can place a replica on an
  excluded node (`add_tablets_in_racks()` checks `is_normal()` only,
  `add_tablets_in_dc()` misses excluded); the drain loop re-plans, so no
  data risk, one live replica fewer meanwhile.
- Ruled out: a down-but-not-excluded peer fails the rebuild repair and it
  retries; rebuild_v2 repairs from every non-excluded replica, the leaving
  one included; drain waits for transitions and checks `has_replica_on`
  after the barrier; excluded or left nodes are banned; RF=1 removenode
  loses the data by design; the skiplist and incomplete load stats lead to
  stalls, not loss; two replicas in one rack are shrunk one at a time
  (multiset diff); merge's `sstables_repaired_at` race is SCYLLADB-6197.

Ruled out by reading, not modelled: a tablet merge across a restart
(merging only regroups in memory), and merging groups left behind by a
failed merge fiber (clone, cleanup and split enumerate them). Topology
operations during a migration aren't rejected by the code, but TLC found
they fail loudly (the joiner's boot, SCYLLADB-5816) rather than lose data.

What worked: steps the code can fail, crash or yield in, written as such in
the model (bugs 1, 4, 6), and checking what survives a restart, not only
what is in memory (4, 6), and modelling caches and stale views as the
code keeps them rather than as fresh state (7). What didn't: deepening models that showed nothing
(incremental_repair_gc, the row cache at larger bounds).
