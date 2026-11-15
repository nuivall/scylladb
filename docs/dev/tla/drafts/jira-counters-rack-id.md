DRAFT, not posted.

Summary: counters with tablets: replicas in one rack share a counter id, so concurrent increments are lost when RF exceeds the rack count

Since b71762d5daa ("counters: reuse counter IDs by rack"), a tablet replica's
counter id is its rack's name UUID (`table::update_effective_replication_map()`,
`topology::get_rack_uuid()`), on the assumption that "there is a single
replica in a rack". Nothing enforces that for counter tables:
`rf_rack_valid_keyspaces` and `enforce_rack_list` default to false,
`create_table_statement` only checks the `counters_with_tablets` feature, and
the vnodes-to-tablets migration accepts an RF-rack-invalid keyspace with a
warning (tablet_allocator.cc). With RF=3 in a single rack all three replicas
use one counter id.

Two increments of a counter led by two replicas (each coordinator that is a
replica leads its own update) both read the shard at clock c and both write
clock c+1 under the same id; merging keeps one (mutation/counters.cc), so an
acknowledged increment is lost. The same happens sequentially when the
first leader's replication to the second hasn't arrived (CL=ONE/QUORUM).
Replicas can also diverge for good: repair merges by highest clock too.

Reproducer: test/cluster/test_counters_single_rack_rf3.py (dev mode). Three
nodes, RF=3, tablets; +10 at CL=ALL, then +1 and +2 concurrently through two
nodes with `apply_counter_update_delay_100ms` (between the read and the
transform). One rack: 12 or 11 on every replica (expected 13), fails 3 of 3.
Three racks: passes 3 of 3.
Without any injection, `test_concurrent_counter_increments_without_injection`
(8 clients pinned to two replicas, 2,400 increments at CL=QUORUM) loses
641-695 acknowledged increments (about 28%) in one rack, 3 of 3 runs; none
with three racks.

Fix options: require RF-rack validity for counter tables on tablets (and in
the migration), or give each replica a distinct id (e.g. host-based, with
the rack id only for the one-replica-per-rack case).

Found by code reading (TLA+/Veil experiment, docs/dev/tla/FINDINGS.md #17).
