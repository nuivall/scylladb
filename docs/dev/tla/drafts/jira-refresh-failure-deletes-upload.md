DRAFT, not posted.

Summary: tablets: a failed nodetool refresh --load-and-stream deletes upload-dir sstables whose other tablets were never streamed

`tablet_sstable_streamer::stream()` (sstables_loader.cc) streams an sstable
that spans several tablets once per tablet, with `defer_unlinking::yes`:
after each successful batch, `stream_sstable_mutations()` marks the sstable
for deletion, and `stream()` unlinks the marked sstables after the last
tablet (`unlink_marked_sstables()`). When a later tablet's stream fails,
`stream()` throws before that, but the mark stays: `sstable::close_files()`
unlinks every sstable marked for deletion when its last reference drops. So
the failed refresh deletes the sstable from upload/, and the rows of the
tablets after the failure exist nowhere: a retried refresh finds nothing.
Restore passes unlink=false and isn't affected.

Reproducer: test/cluster/test_refresh_failure_keeps_upload_sstables.py
(dev mode; adds a test-only injection `load_and_stream_fail_after_a_batch`
that fails every batch after the first). One node; an sstable of a
1-tablet table staged in the upload dir of a 4-tablet table; the refresh
fails, upload/ is empty, and the retry loads 46 of 200 rows. Fails 3 of 3;
without the failure, passes 3 of 3.

Candidate fix: when `stream()` fails, unmark the sstables
(`unmark_for_deletion()`), leaving them in upload/ for the retry; passes
3 of 3.

History: the lazy deletion comes from tablet load-and-stream (ab498489fed);
PR #29599 (SCYLLADB-1647) added the deferral, considering the success path.

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #24).
