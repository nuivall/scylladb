DRAFT, not posted.

Summary: batchlog replay racing DROP TABLE drops the whole batch; racing TRUNCATE resurrects truncated rows

`batchlog_manager::process_batch()` decodes a batch and, per mutation, skips
tables that no longer exist (`try_find_table()`) or were truncated after the
batch (`written_at <= get_truncation_time()`). It then builds the mutations
(yielding), waits on the replay rate limiter (`limiter.reserve()`, which can
take seconds at the default 1 MB/s split across nodes), and only then sends
(`send_batchlog_replay_to_all_replicas()`). Nothing checks the tables again.

- DROP TABLE b in that window: `mutate_prepare()` throws
  `no_such_column_family` for b's mutation, the replay's catch treats it as
  "drop the batch", and the batchlog entry is deleted. The batch's mutations
  to other tables are never applied, although the coordinator answered
  WriteTimeout with write type BATCH, which promises eventual application.
- TRUNCATE b in that window: b's mutations, written before the truncate, are
  sent after it, and the truncated rows come back -- what the decode-time
  truncation check exists to prevent.

Reproducer: test/cluster/test_batchlog_replay_table_race.py (dev mode; a
test-only pause `batchlog_replay_before_send` before `limiter.reserve()`).
A batch over tables a and b is logged and not sent
(`storage_proxy_fail_send_batch`); the replay is held after the table
checks; DROP or TRUNCATE b; release. DROP: a has no row; TRUNCATE: b's row is
back. 6 of 6 fail; with the tables checked again right before the send, 6
of 6 pass.

Fix: re-check per mutation right before the send (skip dropped tables
instead of failing the batch; drop mutations truncated after `written_at`);
the send itself can still yield, so a complete fix for TRUNCATE also needs
the truncation to be ordered with in-flight replays (or the replica to
reject mutations older than the truncation, as for commitlog replay).

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #18).
