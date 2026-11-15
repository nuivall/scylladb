DRAFT, not posted.

Summary: tombstone_gc=repair purges immediately during a multi-step RF change that extends an RF=1 keyspace to a new DC

`table::update_tombstone_gc_rf_one()` marks a table RF=1 in the shared
tombstone GC state from `_erm->get_replication_factor()`, which for tablets
is the keyspace replication strategy's RF. In repair mode an RF=1 table's
tombstones are collected immediately. A multi-step RF change (every DC
changes 0<->N, e.g. `{dc1: [r1]}` -> `{dc1: [r1], dc2: [r1, r2]}`) keeps the
keyspace's current options until it completes (`next_strategy_options`
holds the target), so while the change extends the tablets to dc2 -- which
can take long for big keyspaces -- each tablet already has replicas in both
DCs, and every replica still purges tombstones immediately. A replica that
missed a delete keeps the row while the others drop the tombstone; repair
or read repair then spreads the row back.

Reproducer: test/cluster/test_rf_one_gc_during_multi_rf_change.py (dev
mode). dc1: one node, dc2: two; keyspace `{dc1: [rack1a]}`, table with
tombstone_gc repair; ALTER to add dc2 [rack2a, rack2b], held after the
tables are extended (`determine_rf_change_actions_per_rack_throw`); the
tablet has 3 replicas. DELETE at CL=ALL, flush, major compaction: the dc2
replicas keep nothing of the deleted partition. After the change completes,
the same sequence keeps the tombstone. Fails 3 of 3.

Fix options: derive "RF=1" from the tablet replica counts (or the larger of
the current and next options during an RF change) instead of the strategy's
RF.

Related: SCYLLADB-4629 (In Review) -- repair mode on steady RF=1 tables
purges without flushing hints/batchlog.

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #20).
