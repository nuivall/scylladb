DRAFT, not posted.

Summary: TRUNCATE without auto_snapshot: truncated rows come back after a crash (truncation record below the cleared memtable's commitlog entries)

Without a snapshot (`auto_snapshot: false`, or a table not on local
storage, which never snapshots), TRUNCATE doesn't flush: `cf.clear()` drops
the memtables, and their writes stay in the active commitlog segment.
`database::truncate()` then saves as the truncation record the replay
position returned by `discard_sstables()`, the highest position among the
discarded sstables written on this node and shard. The low mark taken
before the clear (`set_low_replay_position_mark()`) is used only when that
position is empty. With any sstable flushed before the memtable's writes,
the record is below them, and commitlog replay after a crash re-applies
every truncated write that was in a memtable; the boot flush makes them
permanent.

History: #6995 hit `SCYLLA_ASSERT(low_mark <= rp)` on exactly this path;
PR #7005 dropped the assert and added the empty-rp fallback instead of
saving the low mark. `test_safety_after_truncate` (database_test.cc)
doesn't flush before truncating, so it takes the fallback and passes.

Reproducer: test/cluster/test_truncate_without_flush_record.py (dev mode,
no injection). One node, `commitlog_sync: batch`, `auto_snapshot: false`:
insert a row, flush, insert 100 rows, TRUNCATE (0 rows), kill the node,
start it: 100 of 100 rows are back. Fails 3 of 3; without the first flush
(control) passes 3 of 3.

Candidate fix: save max(discarded sstables' position, low mark) -- passes
the reproducer and the control 3 of 3.

Related: SCYLLADB-5394 (views-only variant; its description says the base
table is covered by the low mark, which holds only when there are no
sstables).

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #21).
