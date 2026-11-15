DRAFT, not posted.

Summary: tablet-aware restore: replicas of a tablet in one rack share the `downloaded` flag, so a retried restore leaves one of them empty

Tablet-aware restore skips the sstables marked `downloaded` in
system_distributed.snapshot_sstables, and marks each one after attaching it
(`sstables_loader`, `update_sstable_download_status()`). The row key is
(snapshot, dc, rack, first token, sstable id): it has no replica identity.
With RF above the rack count (allowed: `rf_rack_valid_keyspaces` defaults to
false, and `restore_tablets` checks only DC coverage), two replicas of a
tablet in one rack share those rows, and whichever reads after the other
has marked them skips them.

Deterministic path: one replica's download fails, the restore fails, the
user retries (what the flag is for); the retry finds every sstable the other
replica restored already marked, skips them on the failed replica, and
reports success. That replica stays without the data: fewer copies than RF,
and CL=ONE reads served by it miss the rows. After a TRUNCATE + restore the
restored sstables keep their repaired_at, so incremental repair doesn't copy
them over either.

Reproducer: test/cluster/object_store/test_restore_two_replicas_one_rack.py
(dev mode). RF=2 on 2 nodes; `fail_download_sstable` once on one node;
restore fails, retry succeeds. One rack: keys are on 1 of 2 replicas (10 of
10 runs, S3 and GCS); two racks: 4 of 4 pass.

Fix options: key the flag by replica (host id) too, or require
RF-rack-valid layouts for tablet-aware restore (test_backup.py already says
such a rack "is not a configuration a backup can be restored from", but
nothing enforces it).

Related: FINDINGS #12 (the same flag is never reset, so restoring the same
snapshot again after a TRUNCATE skips everything), #17 (counter ids per rack
with the same layout).

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #19).
