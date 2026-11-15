DRAFT, not posted.

Summary: TRUNCATE after a machine reboot records a pre-reboot replay position; writes after it are lost on a crash

Commitlog segment ids start at `runtime::get_boot_time()` in ms, a
steady_clock value, i.e. the machine's uptime (commitlog.cc
`config::from_db_config()`), or above the highest segment id on disk
(`segment_manager::init()`). After a clean shutdown no segments are left,
so after a machine reboot the ids restart far below the previous run's.

`database::truncate()` saves as the truncation record the highest replay
position of the discarded sstables that originated on this node and shard
(`table::discard_sstables()`). Sstables flushed before the reboot carry
positions in the old, higher id space, so the record is above every
position of the current run. On the next replay, every entry of the table
at or below the record is skipped (commitlog_replayer.cc), so writes
acknowledged after the TRUNCATE are lost if the node crashes before the
table's next flush. #15354 (PR #15532) drops truncation records at boot,
which covers records written before the reboot, not one written after it
from pre-reboot sstables.

Reproducer: test/cluster/test_truncate_after_reboot_record.py (dev mode).
One node, `commitlog_sync: batch`: insert, flush, clean restart with
`decrease_commitlog_base_segment_id` (imitates the reboot, as
test_commitlog.py::test_reboot does), TRUNCATE, insert 100 rows (all
read back), kill, start: 0 of 100 rows. Fails 3 of 3; without the
injection (control) passes 3 of 3.

Candidate fix: record the table's low mark (`set_low_replay_position_mark()`,
the highest position the table applied in this run, raised to the flushed
position when flushing), or a zero position of this shard when the table
applied nothing in this run; never the sstables' positions. This also
fixes the no-flush case (separate draft: truncated rows back after a
crash). Passes both reproducers and controls 3 of 3, test_commitlog.py and
the cluster truncate tests and database_test.cc (67 cases).

Related: SCYLLADB-5134 (In Review), the same root cause for strong
consistency's raft truncation records.

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #22).
