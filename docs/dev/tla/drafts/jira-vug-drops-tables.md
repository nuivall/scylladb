DRAFT, not posted.

Summary: view_update_generator: a staging failure of one table drops the queued staging sstables of the other tables

`view_update_generator::start()`'s fiber moves `_sstables_with_tables` into a
local map and processes it table by table. When
`generate_updates_from_staging_sstables()` throws for a table, the catch
puts that table's sstables back into `_sstables_with_tables` and `break`s.
The tables after it in the local map are destroyed with it at the end of
the pass: their staging sstables are never processed (until the node
restarts and rediscovers them), and the `_registration_sem` units they took
in `register_staging_sstable()` are never signalled.

Effect: base rows without view rows on that node until a restart. A
persistent failure of one table (a corrupt staging sstable, an I/O error,
a view update error such as SCYLLADB-6142) drops whatever else was queued
on every retry pass. The leaked registration units can eventually block
`register_staging_sstable()` callers (repair, streaming).

Reproducer: test/cluster/mv/test_mv_staging_failure_drops_others.py (dev
mode). Two vnode tables with views; node 0 loses their sstables; repair
brings the base rows back as staging sstables while
`view_update_generator_consume_staging_sstable` makes every pass fail; after
disabling it, one of the views stays at 0 of 1000 rows on node 0. Fails 3
of 3; passes 3 of 3 with the fix below.

Fix: in the catch, re-queue the remaining tables of the pass too, e.g.
    for (auto rest = std::next(table_it); rest != sstables_with_tables.end(); ++rest) {
        std::move(rest->second.begin(), rest->second.end(),
                  std::back_inserter(_sstables_with_tables[rest->first]));
    }
(the `stop_iteration::yes` break is on shutdown, where dropping is fine).

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #16).
