# TLA+ specifications

This directory holds TLA+ specifications of ScyllaDB protocols, and model
configurations that the TLC model checker explores exhaustively. A spec
states a protocol's invariants and checks them against every interleaving
of its steps, within small bounds. That is where tests are weakest: a race
that needs five events in a particular order across three nodes is one
state among millions to TLC, and one schedule that a test is unlikely to
hit.

## Running

```
scripts/tla/check.sh                 # the configurations marked "ci: yes"
scripts/tla/check.sh --all           # all of them
scripts/tla/check.sh docs/dev/tla/sc_tablet_migration/MC_current.cfg
```

The script needs Java, which the toolchain image already has
(`install-dependencies.sh`). It downloads a pinned `tla2tools.jar` into
`build/tla/` and checks its checksum. TLC output goes to `build/tla/out/`;
`scripts/tla/trace_diff.py <file>` prints a counterexample as the sequence
of actions and the variables each one changed.

Each model configuration states the outcome it expects:

```
\* expect: pass                 no invariant is violated
\* expect: Linearizable         TLC finds a violation of this invariant
\* ci: yes                      cheap enough to run on every change
```

`check.sh` fails if any outcome differs. A configuration that expects a
violation is as much a test as one that expects none: it keeps a known bug,
or an ablation, reproducible, and catches a spec change that makes the model
blind to it.

The Veil models in `veil/` run the same way:

```
scripts/tla/check_veil.py                # every check not marked "cost: heavy"
scripts/tla/check_veil.py --all          # all of them
scripts/tla/check_veil.py docs/dev/tla/veil/CountersTablets.lean
scripts/tla/check_veil.py --stale        # models whose code changed
```

It needs elan (`lake`), and uses a Veil checkout at the pinned tag from
`$VEIL_DIR`, or clones and builds one in `build/veil`. Each check command in a
model is preceded by its expected outcome:

```
-- expect: pass                          no violation, or every proof goal holds
-- expect: violation no_lost_write       a violation of this safety property
-- expect: not inductive crash:no_lost_write
                                         #check_invariants fails these goals only
-- cost: heavy                           run only with --all
```

### Keeping a model in step with the code

Each Veil model names the code it describes and the commit it was last
checked against by reading:

```
-- models: db/commitlog/, repair/row_level.cc, replica/database.cc
-- checked-against: 61d55c44f6a
```

`--stale` lists the models whose files changed after that commit, with the
commits. For each one, read the changes against the model's actions. If the
model no longer matches the code, change it, and update the expected
outcomes when the change is meant to alter them. A changed outcome is the
point: a fix should turn a `violation` into `pass`, and a new bug the other
way. Then rerun the model and move `checked-against` to the commit you read.
The cluster reproducers of the bugs a model found are the other half of the
check: they run in test.py, xfail until the bug is fixed.

## Layout and conventions

- `docs/dev/tla/<name>/<name>.tla`: the spec. Its header maps every action
  to the code it models (`file`, function) and lists the abstractions.
- `docs/dev/tla/<name>/MC*.cfg`: model configurations. Name them after what
  they show: `MC_current` (the code as it is), `MC_before_<sha>` (the code
  before a fix), `MC_ablate_<what>` (a protection removed), or after a
  hypothesis.
- Invariants are named after the design doc's when it has names (`I1`..`I5`
  in `sc_tablet_migration`).
- Model Raft and group 0 as a linearizable log, with every node applying a
  prefix of it at its own pace. Don't re-specify Raft; it has its own spec and
  its own randomized tests (`test/raft`). Model what is built on top of it.
- A failure, a crash, a retried or stale RPC, or a delay is a nondeterministic
  action, bounded by a counter (`MaxFailures`, ...).
- Constants switch a fix or a protection on and off, so one spec covers the
  code before and after a change, and every ablation.
- Keep the state space small enough for `ci: yes` to finish in seconds. Large
  configurations are run on demand, e.g. on a big machine.

## How a spec finds bugs

1. **Write the model from the design doc and the code.** The stage tables,
   preconditions and invariants in `docs/dev/topology-over-raft.md` are
   already close to a spec; most of the work is deciding what to abstract.
2. **Calibrate it on known bugs.** Model the code before a past fix, and check
   that TLC finds the bug; then that the fix makes it pass. A model that can't
   find a bug we know about won't find one we don't.
3. **Ablate.** Remove one protection at a time (a barrier, a guard, a check).
   Each one should be needed by some invariant, and the counterexample shows
   what for. A protection whose removal changes nothing is either redundant
   or protects something the model is missing; both are worth asking about.
4. **Hunt.** Widen the nondeterminism to what the code really allows (more
   failures, retries, lagging nodes, a second migration) and let TLC search.
5. **Triage a counterexample.** It is either a model artifact (the model
   allows a step the code doesn't, so fix the model) or a candidate bug. Map
   each step of the trace to code. The trace names the interleaving, which
   is the hard part of writing a reproducer.
6. **Reproduce.** Turn the trace into a `test/cluster` test: error injections
   (`utils::error_injection`, `wait_for_message`) hold each node at the trace's
   interleaving points. Keep the configuration that found it, with
   `expect:` set to the violation, until the fix lands, then flip it to
   `pass`.

### Worked example 1: a past bug, found again

`group0_state_machine/MC_before_2f372ec9707` models the code before
2f372ec9707 ("keep the read_apply mutex unavailable after a failed change").
TLC finds, in five steps:

1. a remote node commits a schema change `1`;
2. the node's `apply()` persists `1`, then fails to update the in-memory
   schema; the read_apply mutex is released, the server's abort is pending;
3. a snapshot transfer pulls the leader's state `{1}`;
4. `merge_schema_from()` diffs `{1}` against the persisted `{1}`: nothing to
   apply in memory; the history and the applied index move forward;
5. a local DDL reads the in-memory schema `{}` and the last state ID `1`,
   and its command passes the `prev_state_id` check: a change computed from
   a state that isn't the one it applies to (`Linearizable` is violated).

This is the failure the commit describes. `MC_current` passes.
`MC_before_02efd050634` finds the root cause of 1efcdffaf99 the same way:
`enable_in_memory_state_machine()` doesn't reload the schema, so a change
persisted while the flag was off never reaches memory. The commit describes
it with a snapshot transfer; TLC's shortest trace persists the change with
`apply()`, which also only writes to disk while the flag is off.

### Worked example 2: a new bug, found by the model and reproduced

The fix of 2f372ec9707 breaks the mutex in `transfer_snapshot()` only for
`schema_persisted_but_not_applied`. But `database::do_apply_many()`, called
by `merge_schema_from()`, writes the commitlog atomically and then applies
the memtables one by one (see its FIXME). An exception there, e.g. a
`bad_alloc` in `unfreeze_gently()` or `table::apply()`, is not that exception
type, so the mutex stays usable and raft retries the transfer.
`MC_generic_failure_after_persist` models this, and TLC finds the same
violation in five steps: the transfer fails after persisting `{1}`; the
applier (or a retried transfer) merges `1` again, but diffs persisted state
against persisted state and leaves memory as it was; a local DDL is then
computed from the stale in-memory schema. `apply()` is not affected: it
calls `fail()` for any exception.

Nothing between `transfer_snapshot()` and `do_apply_many()` catches the
exception. `test_joining_node_stops_group0_on_schema_persist_failure` in
`test/cluster/test_schema_merge_failure_stops_group0.py` reproduces it, with
the injection `database_apply_many_fail`, which throws once from
`do_apply_many()` after the memtables took the mutations. On a dev build:

1. the leader logs "Transferring snapshot to ... failed with: ...injected
   failure after database::do_apply_many()": the transfer fails, and group 0
   on the joining node keeps running;
2. the retried transfer "completed": its merge found nothing to do;
3. the joining node creates `system_distributed` with `SimpleStrategy` and
   RF 3, replacing the cluster's `NetworkTopologyStrategy` replication: the
   corruption of SCYLLADB-5770, through a path its fix doesn't cover.

The other tests in that file pass on the same build. The model makes the
whole change visible before the failure; the code can also fail with only
some mutations in memtables, which leaves the in-memory schema stale the
same way. The likely fix is to call `fail()` for any exception once
`merge_schema_from()` may have written. The open rewrite of this code
(scylladb#32876) doesn't cover it either: it marks the snapshot persisted
only after `database::apply()` returns.

Not every lead survives triage. `merge_schema_from()` returns without
merging when it can't get a storage service permit, and `transfer_snapshot()`
would then record the snapshot as applied without its schema. But the
permit is unavailable only once `storage_service::stop()` runs, and `main.cc`
aborts group 0 before that, so no transfer can see it.

## Specifications

### group0_state_machine

One node's group 0 state machine as seen by schema changes: `apply()`,
snapshot transfer, the in-memory and the persisted schema, the history table,
the read_apply mutex, local DDL, failures and restarts. `Linearizable` is
end to end (every effective command was computed from the state it applies
to); `MemMatchesDisk` is the local invariant behind it.

| configuration | expects | |
|---|---|---|
| MC_current, MC_current_large | pass | |
| MC_before_2f372ec9707 | Linearizable | the bug of 2f372ec9707, found again |
| MC_before_02efd050634 | Linearizable | the race of 1efcdffaf99, found again |
| MC_generic_failure_after_persist | Linearizable | new bug, reproduced, see above |

### sc_tablet_migration

The migration of a strongly-consistent tablet, from the stage table in
"Strongly-consistent tablets" in `topology-over-raft.md`: stages published
in group 0 and applied by each host at its own pace, the barriers, the
per-stage config syncs and their handlers (guarded and superseded), raft
leadership and every host's view of it, requests pinning a view and entering
a server, the cleanup RPCs, and storage. Invariants I1..I5 are the doc's;
`SyncedHolds` is synced(S); `VotersCaughtUp` and `LeaderIsVoter` are
sanity checks. Three hosts, RF=2, no intra-node migration, no restarts.

`MC_current_large`, with leader changes and requests together, passes
(3M distinct states in under 3 minutes, with the state view; without it,
TLC ran out of disk at depth 59 and 334M states). The other configurations
split it, to run faster. Rollback from `sc_become_voter` happens only when the pending
replica is excluded; `ExcludedPendingAlive` says whether it is then down,
as exclusion assumes, or has come back up.

| configuration | result | states | |
|---|---|---|---|
| MC_current | pass | 55M | leader changes, no requests |
| MC_current_requests | pass | 171M | a request, no leader changes |
| MC_current_two_migrations | pass | 1.5M | two migrations, one leader change |
| MC_witness_* | reachable | | end, revert, rollback, second migration |
| MC_witness_rollback_after_promotion | unreachable | 31M | with RF=2 and the pending replica down, a rollback after its promotion loses quorum: not covered |
| MC_ablate_become_voter_barrier | I1 | | needed |
| MC_ablate_use_new_barrier | I2 | | needed |
| MC_ablate_end_migration_barrier | I5 | | needed: a stale cleanup RPC deletes a later copy |
| MC_ablate_revert_migration_barrier | I5 | | needed, likewise |
| MC_ablate_remove_pending_barrier | pass | 233M | protects nothing in the model: the pending replica serves only at `sc_become_voter` and `sc_rollback`, rollback needs it excluded, and barriers skip excluded nodes. A question for the code's authors, not a claim |
| MC_ablate_sync_guard_and_supersede | pass | 58M | with the excluded replica down, nothing needs them |
| MC_ablate_sync_guard_and_supersede_alive | SyncedHolds | | needed once it can come back |
| MC_excluded_alive | I2 | 33M | barriers skip the excluded replica, so its requests aren't drained |
| MC_excluded_alive_i1 | I1 | 9M | see below |
| MC_excluded_alive_i1_rollback_barrier | pass | 653K | the barrier c59a1208f7e removed, restored: it closes the I1 gap |

#### Finding: I1 at sc_rollback

`mark_excluded()` refuses a node gossip sees alive, but nothing fences an
excluded node, so one that was down can come back before it is removed.
`MC_excluded_alive_i1` then breaks I1 in 40 steps. Migration L=h2, P=h3; h3
is excluded at `sc_become_voter`, which rolls back:

1. the sync of `sc_rollback` goes to h1 and h2; h2 has applied the stage,
   finds the configuration it expects and reports synced;
2. h1, the leader, hasn't applied `sc_rollback` yet, so its sync handler of
   `sc_become_voter` isn't cancelled, and it proposes that stage's
   configuration: h3 is a voter again;
3. h3 wins an election and h2 learns of it; h1 wins again, its handler of
   `sc_rollback` supersedes the old one and reverts the configuration, and
   the sync completes;
4. `sc_remove_pending` is published while h2, which serves at that stage,
   still names h3, outside its serving set.

The barrier at `sc_rollback` that c59a1208f7e removed waited for the old
handler: with it restored, the same model passes (`MC_excluded_alive_i1_rollback_barrier`). The comment of `do_config_sync` in `topology_coordinator.cc` still
says `sc_rollback` runs it; the comment at `sc_rollback` argues it isn't
needed, replica by replica, while the trace needs two replicas. The impact
is mild: a request that finds the stale leader forgets it and waits for the
next one. It needs an excluded node to come back. Not reproduced.

### ec_tablet_migration

The migration of an eventually-consistent tablet, with the data of one key:
coordinators write and read with the replica sets of the stage they pinned
(acks: `block_for(CL)` plus the pending replica), streaming under topology
guards, including an RPC the coordinator gave up on that keeps running on
the destination, cleanup, cancellation, and tombstone GC once every replica
has the tombstone. `ReadsSeeAckedWrites` (no acknowledged write is lost and
nothing deleted comes back) and `StorageOnReplicasOnly` (INV-TABL-1). RF=2
with writes at ONE and reads at ALL: with QUORUM on both, every acknowledged
write is on the replica that stays, and no missing barrier could lose data.

| configuration | result | states | |
|---|---|---|---|
| MC_current | pass | 11M | one migration, a write, a read, a failure or a cancellation |
| MC_current_two_migrations | no violation to depth 27 | 59M | too large to finish, even with the state view |
| MC_witness_* | reachable | | end, cancel and roll back, purge a tombstone and migrate |
| MC_ablate_write_both_read_old_barrier | lost write | | needed: a write of the old view misses the stream |
| MC_ablate_write_both_read_new_barrier | lost write | | needed: a read of the old replicas misses a write the new ones acknowledged |
| MC_ablate_cleanup_target_barrier | INV-TABL-1 | | needed: an abandoned stream writes to the pending replica after its cleanup |
| MC_ablate_use_new_barrier | pass | 3M | the model's cleanup is atomic; the barrier protects a cleanup racing writes |
| MC_ablate_end_migration_barrier, MC_ablate_revert_migration_barrier | pass | | they drain stale cleanup RPCs, which this model doesn't run concurrently (sc_tablet_migration does) |
| MC_ablate_session_check | INV-TABL-1 | | needed: without it, a stream the coordinator gave up on writes to the pending replica after the rollback cleaned it |

### row_cache

The partition-level row cache against its sstables: memtable flushes merged
by `update()` partition by partition, with the ring split at
`_prev_snapshot_pos` into the part that reflects the new snapshot and the
part that reflects the previous one; populating point reads and range scans
(`autoupdating_underlying_reader` recreating its reader on a phase change,
continuity set through the previous-entry pointer); eviction; and
`invalidate()` yielding mid-range. `Coherent` is what reads see: whatever
the cache knows about a partition, with what the flushed memtable still
holds for it, is the underlying source; during `invalidate()` only the part
it hasn't reached may lag.

| configuration | result | |
|---|---|---|
| MC_current, MC_current_two_scans | pass | |
| MC_current_large | no violation to depth 18 (167M states) | too large to finish, even with the state view |
| MC_witness_* | reachable | a scan populates the whole ring; a flush merges into a cached partition |
| MC_ablate_populate_phase | Coherent | needed: a miss reads the old snapshot, the flush skips the uncached key, the reader caches the stale value |
| MC_ablate_continuity_prev | Coherent | needed |
| MC_ablate_evict_continuity | Coherent | needed |
| MC_ablate_update_absence | Coherent | needed: `update()` may cache a memtable partition only if the cache knows it is complete |
| MC_ablate_invalidate_clear_next | Coherent | needed: without the `clear_continuity(*it)` after the loop, a read during a yield finds an erased partition absent, though both snapshots have it |

Two leads were triaged away. `invalidate()` with several ranges sets
`_prev_snapshot_pos` to each range's start in turn, which is only correct
for sorted ranges, an unstated precondition; its only multi-range caller
(cleanup compaction) passes `subtract_ranges()` output, which is sorted.
And the yield in `invalidate()` looked like it left a stale continuity flag,
until the model was made to follow the code exactly: the flag is cleared
after the loop on the way out, as the last ablation shows.

### cache_mvcc

The MVCC versions of one cached partition, at row level, with the rules of
"Evictable snapshots" in `mvcc.md`: independent continuity per version,
information monotonicity, the last dummy, older versions evicted first
(only latest-version entries move to the LRU front), insertion only in the
latest version, and version merging. `Correct`: the latest snapshot's
logical view of each row is the underlying row or unknown.

| configuration | result | |
|---|---|---|
| MC_current | pass | |
| MC_current_large | no violation to depth 16 (122M states) | too large to finish, even with the state view |
| MC_witness_merge | reachable | |
| MC_ablate_touch_any_version | Correct | needed: a read moves an old version's dummy to the front, the newer version's row is evicted first, and the partition reads as empty |
| MC_ablate_evict_next | Correct | needed |
| MC_ablate_populate_older | Correct | needed |
| MC_before_9ccd4ea416a | pass | not reproduced: that bug came from `apply_monotonically()` relying on the eviction order of entries at the same position, and this model's merge is atomic |

Reversed-read cursor bugs (b32ac334b3a, 21d9f54a9a6) are iterator bugs in
`partition_snapshot_row_cursor`, out of reach of either model.

### compaction_groups

Ownership of sstables by the compaction groups of a table's tablets on one
shard, with tablet split (rewrite and bypass) and merge (the merging groups,
the merge fiber), regular and incremental compaction (garbage collected
sstables), streaming, and repair's component rewrite. The compacting
registry is the plain set it is in the code. `InputsFound`: no replacer
misses an input ("Unable to remove input SSTable"); `NoLoss`: no sstable is
added to a group that is gone, or held by two groups.

| configuration | result | |
|---|---|---|
| MC_before_3f9d1c16c70, MC_before_7420f936bbc, MC_before_99d302fb483 | InputsFound | the three fixed races, found again |
| MC_current | InputsFound | the split bypass leaves an sstable registered by the split task; a capture that doesn't filter what is registered (scrub, component rewrite) takes it on the split-ready group; the split task deregisters it; a regular compaction takes it too |
| MC_witness_merged | reachable | |

#### Finding: incremental repair marks a running compaction's GC sstables (SCYLLADB-5896)

The model has one view per group; the code splits a group into an
unrepaired, a repairing and a repaired view, and the same pattern shows up
across views. The GC writer of a compaction doesn't set `repaired_at`, so
the GC sstables of a compaction of repaired sstables have `repaired_at=0`:
they are classified unrepaired while the compaction that owns them runs on
the repaired view. Incremental repair disables compaction on the unrepaired
view only, takes every sstable that isn't repaired, GC sstables included,
and marks them by rewriting them, view by view: unrepaired, repairing,
repaired. It rewrites the GC sstables on the repairing view before it stops
the compaction on the repaired view; if that compaction releases its GC
sstables in between, they aren't in the set any more.

`test_incremental_repair_during_repaired_compaction_with_gc_sstables` in
`test/cluster/test_incremental_repair.py` reproduces it on three nodes, with
two test-only injections that hold the compaction after it attached GC
sstables and the rewrite between views: every node aborts with "Unable to
remove input SSTable [...origin=garbage_collection:repaired_at=0...] ...
for compaction of type unknown", the signature of SCYLLADB-5896 (open when
this was written). SCYLLADB-6121's abort ("... origin=garbage_collection
... for compaction of type scrub") looks like the same pattern with scrub,
which also captures without filtering what is registered; not reproduced.
Fixes to consider: give GC sstables the compaction's output `repaired_at`,
and make captures skip what other tasks have registered.

#### Finding: a failed split bypass deletes the live sstable

Found while reading the code for the model's bypass step, not by TLC. The
model's bypass moves the sstable atomically and can't fail, so it can't
show this bug. 9cbab949b4d ("don't leak compaction output when attaching
it fails") made `compaction_group::update_sstable_sets_on_compaction_completion()`
delete `desc.all_new_sstables()` if it throws before the new sstable sets
are installed. Most completions have new outputs, so that cleanup is right
for them. Two completions don't: the split bypass (`do_rewrite_sstable()`,
which moves an sstable that lies on one side of the split point) and
off-strategy integration both pass the input itself as the output
(`old_sstables == new_sstables`). If such a completion fails, the files of
a live sstable are deleted. Reads keep working while the open sstable holds
the files, so the loss shows up only after a restart. The split is not
retried after the failure either.

`test/cluster/test_split_bypass_failure.py` reproduces it on one node with
the commitlog disabled. It fails the split's first completion through the
test-only injection `update_sstable_sets_on_compaction_completion_fail_once`,
restarts the node and reads the rows back: "200 of 200 rows lost after the
failed split bypass and a restart". After the failure the table directory
holds no sstables, while the node still reports the sstable's size in memory.

In production, any exception before the sets are installed triggers it: the
"Unable to remove input SSTable" internal error (it throws, since
`abort_on_internal_error` is false by default), `bad_alloc` while building
the new set, and so on. 9cbab949b4d is on master only, and so in 2026.4;
it isn't on 2026.1-2026.3. SCYLLADB-6108 (open, 2026.4.0~rc1: a replica
missing a tablet's rows after a split aborted by a restart) has the same
shape: "Failed to complete splitting", then a tablet that is split-ready
with no split compaction after the reboot. This bug may be its cause; that
is not proven. Fix to consider: never delete what is also an input, i.e.
delete `new_sstables` minus `old_sstables`.

### tombstone_gc_repair

Tombstone GC with `tombstone_gc = {'mode': 'repair'}`, tablet repair and
tablet merge. A tombstone may be purged once a repair that started after it
was written has completed for its range. The tablet's repair time goes into
the tombstone GC state for its whole range, which only ever takes the
maximum per token. `NoResurrection`: a read at CL=ALL returns the newest
write.

| configuration | result | |
|---|---|---|
| MC_current | NoResurrection | known bug (SCYLLADB-6091), found independently and reproduced: see below |
| MC_merge_min | pass | the merged tablet takes the earlier repair time |
| MC_no_merge | pass | |

#### Finding: tablet merge resurrects deleted data (SCYLLADB-6091)

`merge_tablet_info()` (`locator/tablets.cc`) gives the merged tablet the
later `repair_time` of its two siblings, and `read_tablet_info()` writes it
into the tombstone GC state for the whole merged range. The half that was
repaired earlier gets the other half's repair time. TLC, in 6 steps: a row
reaches only replica A, its deletion only replica B; the other tablet is
repaired; the merge; B purges the tombstone, which is GC-eligible now
although no repair synced it, and has nothing older to keep it; the row,
still on A, is visible again (and the next repair copies it back to B).

`test/cluster/test_tombstone_gc_tablet_merge.py` replays the trace on a
2-node cluster: `test_tombstone_gc_after_tablet_merge` fails ("deleted row
came back"), `test_tombstone_gc_without_tablet_merge`, the same steps
without the merge, passes. The rule dates from the introduction of
`merge_tablet_info()` (014e1c9a0fe). This was found independently of
SCYLLADB-6091, reported a week earlier, whose fix (scylladb#32897, open
when this was written) takes the minimum of the repair times, as
`MC_merge_min` does. Its description also explains why the maximum of
`sstables_repaired_at` is safe: each sibling's sstables are reset against
its own watermark before the merged map is committed.

### tablet_storage

The sstables of one tablet on one node, across two shards: tablet split
(split compaction, the bypass, the node's ACK, finalization), intra-node
migration (clone, load, the response, the coordinator's stage change,
cleanup), and restarts, which reload what is on disk into the shard serving
reads. A compaction's completion can throw. Unlike compaction_groups, this
spec checks what survives a restart, not only what is in memory.
`Durable`: every write is in a sealed file; `Readable`: in an sstable of
the shard serving its reads; `SplitAckHolds`: after the ACK, no sealed
sstable is unsplit; `NoAbort`, `BootOK`: finalization and boot find no
unsplit sstable.

| configuration | result | |
|---|---|---|
| MC_before_48d243f32fc, MC_before_27d460758f3 | SplitAckHolds | the two fixed bugs, found again |
| MC_current | Durable | a failed bypass deletes its input (the split bypass finding) |
| MC_migration_restart | Readable | the bypass fixed: a restart between the streaming response and write_both_read_new |
| MC_fixed | pass | both fixed (556K states) |
| MC_witness_* | reachable | finalization, a migration after the split, a restart after the response |

#### Finding: an intra-node migration across a restart loses the tablet (SCYLLADB-6226)

Found by TLC; already known: SCYLLADB-6226, fix PR #33027 (open when this
was written, which re-clones such tablets at startup). An independent
rediscovery, like SCYLLADB-6091. Streaming of an intra-node migration loads the tablet's
sstables into the pending shard and returns. The coordinator records the
result (`advance_in_background()`) and moves the tablet to
write_both_read_new in a later iteration, without checking that the node
didn't restart in between. In the streaming stage the read selector is
"previous", so a node that boots there loads every sstable of the tablet,
clones included, into the leaving shard (`compute_shards_for_this_sstable()`
with the tablet sharder), and the pending shard comes up empty. Nothing
reloads it when reads switch. Reads of the tablet then return nothing, and
cleanup deletes the leaving shard's sstables, so the data is gone from the
replica.

`test_restart_after_intranode_streaming_keeps_data` in
`test/cluster/test_tablets_intranode.py` reproduces it on two nodes. A
test-only injection, `intranode_migration_streaming_done_wait`, holds the
replica after the load, so the test can turn on `stream_tablet_wait` on the
coordinator before the response arrives. The test restarts the replica,
lets the migration finish and reads back: "100 of 100 rows lost by the
intra-node migration across a restart"; the control without the restart
passes. The window in production is between the response and the
coordinator's next iteration. The window is long when the coordinator is
held up, for example by a barrier waiting for the restarting node.

A merge across a restart, the next case planned for this spec, was ruled
out by reading: `merge_sstables_from()` only moves sstables between
in-memory sets, so a crash mid-merge leaves nothing on disk for boot to
misplace, and clone, cleanup and split all enumerate merging groups
(`storage_group::for_each_compaction_group()`).

### vnodes_tablets_migration

The vnodes-to-tablets migration of one keyspace, node by node: prepare,
each node's intended mode and the restart that reshards into it (reading
the intent from its local, possibly stale, system.topology), the mode it
publishes, the coordinator's load stats (refreshed for live nodes, cached
for down ones, empty after a leader change), and finalization forward or
back with the code's checks. Optionally the topology operations the code
doesn't reject during a migration (bootstrap, vnodes cleanup). One rack,
RF=1, two token ranges. `Durable`, `Readable`: acknowledged writes;
`LayoutMatches`: after finalization every node's tables are tablet-based;
`BootOK`: no boot fails.

| configuration | result | |
|---|---|---|
| MC_current | pass | modes published (288,498 states) |
| MC_before_current_column | LayoutMatches | no published modes: release 2026.3, or a migration that predates the column |
| MC_topology_ops | pass | bootstrap and cleanup during the migration; the joiner's forward boot fails (SCYLLADB-5816), which blocks finalization; BootOK left out (9M states) |
| MC_witness_* | reachable | forward, rollback, forward with a joiner |

#### Finding: finalization trusts the cached load stats of a down node

Found by TLC; the mechanism is known: open PR #31829 describes it and makes
finalization refuse while any normal node is down. Not merged when this was
written. Finalization forward requires every node's load stats to show the
tables as tablet-based; they are "the physical evidence"
(`finalize_migration` in `topology_coordinator.cc`). But
`collect_tablet_load_stats(require_live_nodes::no)` keeps the cached entry
of a node that is down, so what finalization checks for a down node is how
it looked when it was last up. TLC: a node upgrades (tablets, its stats
cached), is marked back and restarts into vnodes, is marked for tablets
again and goes down before the next refresh; the other nodes upgrade;
finalization accepts the down node and switches the keyspace to tablets.
The refresh runs every 60 s by default and on some schema and topology
events; neither a mark nor a node coming up triggers one. From reading (not
observed): the node's next boot loads its vnode-split sstables with no
resharding by tablet (`populate_keyspace()` picks no direction once the
keyspace uses tablets), and `compaction_group_for_token_range()` hits
`on_internal_error` on the first sstable spanning two tablets. The
migration can't be rolled back by then.

278e523e5a0 (master and 2026.4) closes it when the node published its
mode: its restart into vnodes published `vnodes`, and finalization refuses
the mismatch. It stays open on 2026.3, which has no such check, and on
master for a migration started before TOPOLOGY_CURRENT_STORAGE_MODE was
enabled, where no node publishes. PR #31829 gives as its example its own
new boot override, which reshards a down node back to vnodes; the
downgrade and re-upgrade above reach the same window without it. No data
is deleted; the node can't start until its sstables are resharded by hand
or it is replaced, so with RF=1 its ranges are unavailable. Not reproduced.

`MC_current` speaks to `LayoutMatches` only: without topology operations
nothing removes data or changes routing, so `Durable` and `Readable` can't
fail there. Replace, the one supported operation that moves data during a
migration (RF>=2, vnode streaming from tablet-layout peers, the map phases
and their rollback), isn't modelled.

### logstor

Logstor (`docs/dev/logstor.md`) on one shard, written from the design doc:
user writes appended to mixed segments, the separator moving them into
full segments of their compaction group, compaction, TRUNCATE, a tablet
leaving and coming back, and a crash followed by recovery. Recovery keeps,
for each key of a group on the shard, the newest record of every segment
whose first header is valid; freeing a segment leaves the disk as it is,
and only `discard_segments()` zeroes the first block of a group's full
segments. `Correct`: the index returns the newest acknowledged write since
the last TRUNCATE; `FreeIsFree`: no free segment is in use.

| configuration | result | |
|---|---|---|
| MC_truncate | Correct | TRUNCATE, then a crash: the truncated rows come back |
| MC_ties | Correct | two writes with one timestamp: a crash changes the value |
| MC_ties_reconcile | AgreesWithReconciliation | the later write wins a timestamp tie, not the tombstone or the larger value |
| MC_move | Correct | the same as MC_ties, through the separator |
| MC_witness_* | reachable | a crash after a TRUNCATE, after a tablet came back |

#### Finding: truncated logstor rows come back after a restart

TRUNCATE (`database::truncate()` -> `table::discard_logstor_segments()`)
clears the table's index and discards the full segments its groups own.
The records are also in the mixed segments the writes went to: the
separator copies them out and the mixed segment is freed, but nothing
zeroes it, and a mixed segment that still holds unseparated records isn't
even freed. Recovery indexes every record of a segment with a valid
header, and nothing filters by the table's truncation time. So the next
restart, graceful or not, brings back every truncated row whose mixed
segment hasn't been reused. `test_truncated_logstor_rows_stay_gone` in
`test/cluster/test_logstor_truncate_recovery.py`: "100 of 100 truncated rows
came back" after a graceful restart; without the restart the rows stay
gone. Logstor is experimental; SCYLLADB-5484 is an open task to make
TRUNCATE "work correctly with concurrent writes and recovery".

#### Finding: equal timestamps are resolved by arrival, and differently by recovery

The runtime index keeps the later of two records of a key when their
timestamps are equal (`primary_index::default_entry_cmp`, `insert()`), so
a DELETE followed by an INSERT with the same timestamp leaves the row
visible, where an sstable table keeps it deleted, and two values with one
timestamp keep the later, not the larger; replicas that saw them in
different orders disagree. SCYLLADB-6194 (open) describes this. Recovery
breaks the tie differently: the record in the segment with the higher
sequence, then at the higher offset (`rebuild_index()`). The separator
copies a group's buffer, dead records included, into a new full segment,
so once it copied the first write while the mixed segment it came from is
still active, the second write lands in an older segment, and after a
crash the first one wins: reads change across a restart.
`test/cluster/test_logstor_equal_timestamps.py` reproduces all three: the
two reconciliation cases against an sstable table, and the restart ("(1, 2)
before the restart and (1, 1) after it"; it fills the separator buffer
with large rows of the same tablet and kills the node, since a graceful
stop flushes the separator again).

### commitlog_cleanup

Writes to one compaction group, its memtables and flushes, and the
commitlog cleanup record a tablet repair saves for its range at
`table::highest_flushed_replay_position(range)`, across a crash. A write
takes its replay position at the commitlog add and reaches the memtable in
a later step. `NoLostWrite`: replay skips no acknowledged write that isn't
in an sstable. MC_current: NoLostWrite (76 states).

#### Finding: a cleanup record skips a write that reached the memtable late

`database::do_apply()` awaits the commitlog add, then `apply_in_memory()`;
nothing makes the second step follow the replay position order of the
first, and the only check (`table::check_valid_rp()`) is against a
truncate. A later write applied first raises the memtable's
`replay_position()`; flushing it raises the group's flushed position
(2da478abe9b) above the earlier write, which then lands in the next
memtable. A tablet repair saves the cleanup record at that position, and
replay after an unclean restart skips the earlier write, acknowledged or
not. `test_cleanup_record_keeps_a_write_applied_after_a_later_one` in
`test/cluster/test_commitlog_cleanup_record_reordered_write.py` holds a
write on both replicas between the two steps (new injection
`database_apply_after_commitlog_add`), writes another, repairs, releases the
first (acknowledged at CL=ALL), kills both nodes: lost on 2 of 2 replicas.
In production the window is any yield between the steps, e.g.
`unfreeze_gently()` for mutations over 128 KB or a wait for dirty memory,
during a repair's flush. A fix: reject the apply of a position at or below
the group's flushed position and re-add the write to the commitlog, as the
truncate check does, or bound the record by the lowest position still in
flight.

### counters_tablets

A counter of a tablet table (`docs/dev/counters.md`) while its tablet
migrates from A to B in one rack, with C in another rack: per-rack counter
ids, the negated id of a pending replica, a leader that continues its own
shard from its local copy, replication that may fail per replica (but
reaches C), and highest-clock reconciliation. `NoLostIncrement`: the
reconciled value is the number of increments acknowledged.

| configuration | result | |
|---|---|---|
| MC_no_migration | pass | one writer per id |
| MC_migration | NoLostIncrement | the new replica continues the rack's shard from a stale copy |
| MC_witness_done | reachable | |

#### Finding: a tablet migration loses a counter increment

With tablets, a replica's counter id is its rack's. When a tablet migrates
within a rack, the new replica uses the negated id while pending and the
rack's id once the migration ends, continuing that shard from its own
copy. That copy has the leaving replica's increments only if every one of
them reached it: one led after streaming whose write to the pending
replica failed (CL=ONE or QUORUM succeeds without it) leaves it a clock
behind, and the new replica's next increment writes a clock that the
leaving replica already used, with another value. Reconciliation keeps the
higher clock, and on a tie the existing shard (`counters.cc`), so one
increment is gone everywhere. A hint delivered later loses to the newer
clock. `test_counter_increment_survives_tablet_migration` in
`test/cluster/test_counter_tablet_migration_stale_shard.py`: racks r1 (A,
B) and r2 (C), RF=2; B paused after streaming, an increment led by A that
fails on B (`database_apply` throw), the migration finishes, B leads:
"the counter is 6 after 7 increments". Within a node it can't happen: the
leader applies the update on every write shard itself. A fix has to give
the new replica the leaving replica's shard before it leads under the
rack's id, e.g. by reading it from the other replicas (highest clock) on
its first update, or by keeping the pending id for good.

### sc_raft_commitlog

The raft log of one strongly consistent tablet group on one replica, kept
in the commitlog ("Commit log based persistence for Raft log entries" in
`docs/dev/strong_consistency.md`), across crashes. The cluster is an
adversary that follows raft (entries already held in the same term are not
stored again, `log::maybe_append()`; a conflicting tail is truncated).
Truncation records are kept in memory and persisted with the next
release's descriptor, which goes to the `raft_groups` memtable. Replay
follows `raft_commitlog_replay_buffer::add_batch()`: the floor from batch
headers, cursors claiming truncated copies, `superseded_by()` on the head
of each batch, then `finish_replay()`; a crash can fall after its row write
or after its tail rewrite, before the replayed segments are deleted.
`AppliedCommitted`: no uncommitted entry is applied; `NoLostCommit`: every
entry up to the apply index is in the tablet's data.

| configuration | result | |
|---|---|---|
| MC_small | pass | two indexes, eight steps (150K states) |
| MC_current | pass | three indexes, nine steps (5.0M states, ten minutes on the bench) |
| MC_witness_* | reachable | a tail that survives a replay, a supersede that records a truncation |

The first counterexamples were modelling errors, each fixed against the
code: a Receive that stored entries raft already holds, a replay buffer that
dropped a copy above a gap, and an invariant that compared the apply index
with the commit index before a crash.

### logstor_split

A tablet split of a logstor table on one shard: a write picks its group
when it is made and reaches that group's separator buffer later, through a
separator task; the split's readiness check (sticky once set) and the
completion's emptiness check. TLC found readiness declared while a write
for the main group is still queued (MC_current: CompleteOnEmpty), but a
test holding the separator task saw readiness wait for it, so the model
misses what makes the group count as non-empty; MC_drained, with readiness
waiting for the pipeline, passes. Not a bug; while the task was held, the
split monitor looped about 12k times a second.

### view_building

The view building coordinator (`docs/dev/view-building-coordinator.md`)
building one view of a one-tablet base table while the tablet migrates or
the migration fails, with the worker's local record of finished tasks lost
on restart, the coordinator's in-memory list lost on a leader change, and
the batched removal of finished tasks. `BuiltIsCovered`: a view marked
built had a build run to completion on the replica holding the data. Passes
(679 states); witnesses reach a build after a finished and after a failed
migration. A view created during a migration is safe because the end of the
migration moves every task of the leaving replica, not only the aborted
ones (`generate_tablet_migration_updates()`).

### hints_repair_gc

One key on two replicas with tombstone_gc in repair mode, hinted handoff,
and repair's hints flush before it records the repair time, including the
new option that discards the hints a timed-out flush waited for
(63a126daf2a). `NoResurrection`: a read at ALL never returns data whose
newest acknowledged operation is a delete. Passes with and without the
discard (2.2K and 4.5K states); a successful repair syncs the tombstone
before GC can purge it, and a discarded hint only loses data, which the
commit message accepts for CL=ANY.

SC truncate was reviewed without a spec: `apply_truncate_command()`
compares only the last request id, but a stale retry of an earlier
truncate can't land after a later one, since the RPC's topology guard
aborts it once the session closes and raft never commits an older-term
entry after a newer committed one.

### Apalache and TLAPS experiments

Tools: Apalache 0.62.4 (`apalache-mc`, symbolic: SMT over Z3) and the TLA+
proof manager tlapm 1.6.0-pre (backends Zenon, Z3, LS4), both installed
outside the tree (`~/tools`).

On commitlog_cleanup (catalog #10), `apalache/apa_commitlog_cleanup.tla` is
the spec with type annotations (`@type`) and `Max` as `ApaFoldSet`, since
Apalache handles `CHOOSE` poorly. Steps, with the times on this laptop:

| step | command | result |
|---|---|---|
| find the bug | `check --cinit=CInit --inv=NoLostWrite --length=8` | violation at step 7, 1.8 s, ITF trace |
| the fix, bounded | `apa_commitlog_cleanup_fixed.tla`, `--no-deadlock --length=10` | holds, 12.6 s |
| `IndInv` holds initially | `--inv=IndInv --length=0` | holds, 1.2 s |
| `IndInv` is inductive | `--init=IndInv --inv=IndInv --length=1` | holds for all integer positions, 1.5 s |
| `IndInv` implies the property | `--init=IndInv --inv=NoLostWrite --length=0` | holds, 1.2 s |

The checks run for two writes; for those, they amount to a proof for every
length of execution, which TLC can't give. The first attempt at the inductive
invariant failed on its own type bounds (`nextRp \in 1..10`), which the
counterexample showed at once; unbounded `Int` fixed it. Without
`--no-deadlock`, Apalache reports a deadlock (no write left) as an error.

`tlaps/commitlog_cleanup_proof.tla` proves the fix for any set of writes:
`Spec => []NoLostWrite`, by the same inductive invariant, with the save of
the record stated declaratively (below `nextRp` and below every position not
in an sstable). tlapm proves all 27 obligations in 0.3 s. Without the fix's
guard, 1 obligation fails, the `SaveRecord` step, which is what the bug is.

### Veil 2.0 experiments

Veil (verse-lab/veil, tag v4.35.0, Lean 4.35.0) built on the bench in
`~/veil`; files are checked with `lake lean <file>` from that checkout
(`lake env lean` doesn't load cvc5's native library, and `#model_check`
compiles a binary unless told `interpreted`). Veil is Ivy-style first-order
logic, so replay positions are an uninterpreted sort with `TotalOrder`, not
integers; a new position is above every position handed out.

`veil/CommitlogCleanup.lean` (bug #10, modules `CommitlogCleanup` and
`CommitlogCleanupFixed`), `veil/CommitlogCleanupUpdr.lean` and
`veil/CommitlogCleanupTemporal.lean`:

| subtool | what it gave | time |
|---|---|---|
| `sat trace` (symbolic, any instance) | the bug's 7-step trace, the same interleaving TLC found; the solver picked 2 writes, 2 positions | part of 19 s for the file |
| `#model_check interpreted` | buggy: no_lost_write violated; fixed: no violation, 189 states | same |
| `#simulate` (2000 random walks of 20 steps) | buggy: the violation | same |
| `#check_invariants` (SMT) | buggy: a counterexample to induction for `crash` (an acked, unflushed write at the record); fixed: every VC proved, for all instances | same |
| `unsat trace` (8 steps, fixed) | no output, which seems to mean no execution exists (not confirmed) | same |
| `#updr` | from the safety property alone, an inductive invariant with 5 clauses at frame 5, two of which are the hand-written `unflushed_above_record` | 57 s, 269 solver calls |
| `#temporal_check` | without fairness of flush: a lasso that never flushes violates "every acked write is eventually flushed"; with weak fairness: holds (291 product states) | 21 s |
| `veil.smt.trust false` | the fixed proof's 30 SMT goals reconstructed and checked by the Lean kernel | 21 s with the temporal check |

`veil/CountersTablets.lean` translates counters_tablets (bug #11) with Veil's
`tla-to-veil` skill: the same actions (`inc`, `advance`), one encoding change
(`to \in SUBSET Nodes` as three `Bool` parameters), and the skill's
validation against TLC:

| check | TLC | Veil |
|---|---|---|
| MC_no_migration (MaxIncs = 3) | pass, 20 distinct states | no violation, 20 states |
| MC_migration | NoLostIncrement violated | violated, the same trace: stream, an increment led by a that skips b, cleanup, b leads |

6 s for both checks. Veil warned that it could not generate its
`wp_local_eq` lemma for `inc` (nested `if`s with `let`s), which only matters
for SMT verification.

Not tried: the Distributed DSL (protocols as per-node programs over a
network), `prove_temporal_by` (liveness proofs), and the TLC-vs-Veil
benchmark script (a benchmark of our own is below).

Pitfalls met: doc comments (`/-- -/`) are not allowed before an `action`;
a capitalized name in an assignment (`val A i := ...`) is a universal index
over all values, not the enum constructor `A`, so constructors used as point
indices have to be lowercase; `none` as an enum constructor clashes with
`Option.none`; a `def` used in a module needs `public`; a `Bool`
relation takes a `decide (...)` of a proposition; `set_option ... in` before
`#check_invariants` has no effect, the option has to be set for the file;
the temporal checker stops at the first violated property, so properties
that should hold need a run of their own.

### Benchmark: TLC, Apalache and Veil on two bug/fix pairs

Question: which one tool should find a bug and check its fix? Each tool ran
the same model of a bug and of its fix, at the same bounds, on the bench
(32 cores, 62 GB), one run at a time. TLC 1.7.4 ran through `check.sh`
(32 workers, `-Xmx40g`; the time includes JVM startup). Apalache 0.62.4
ran with `--length` at least the counterexample's length for a bug, and at
the diameter TLC reports for a fix, so that a pass covers every reachable
state; 30 minutes per run. Veil's compiled checker was built with
`veil.modelChecker.buildOnly`, and the binary then ran alone, on 1 thread
and on 32.

The models: `vnodes_tablets_migration` (bug #7: `CurrentColumn = FALSE`; the
fix: `TRUE`) and `commitlog_cleanup` (bug #10; the fix: the new `Fixed`
constant) with 5 to 7 writes. The Veil copies, `veil/VnodesTabletsMigration.lean`
and `veil/CommitlogCleanupBench.lean`, are action-for-action translations.
Validation: every Veil run explored exactly TLC's distinct states, and all
three tools return counterexamples of the same length (12 steps for #7, 7
for #10). The Apalache copies are `apalache/apa_vnodes_tablets_migration.tla`
(the spec with `@type` annotations and one `CHOOSE` replaced) and the
existing `apa_commitlog_cleanup*.tla`.

| model | states | TLC | Veil, 1 thread | Veil, 32 threads | Apalache |
|---|---|---|---|---|---|
| #7 bug, MC_before_current_column | (12 steps) | 1.2 s | 0.3 s | 0.2 s | 17 s |
| #7 bug, MC_bench_bug_large | (12 steps) | 5.4 s | 9.2 s | 4.8 s | 497 s |
| #7 fix, MC_current | 288,498 | 1.5 s | 0.6 s | 0.4 s | pass to the diameter (21 steps), 22 min, 1.1 GB |
| #7 fix, MC_topology_ops | 5,428,990 | 9.6 s | 16.0 s | 8.9 s | not run |
| #7 fix, MC_bench_fix_large | 17,430,982 | 27 s, 16 GB | 52 s, 2.4 GB | 25 s, 3.7 GB | not run |
| #10 bug, 5 / 6 / 7 writes | (7 steps) | 0.7 / 0.8 / 0.8 s | 0.1 s | 0.1 s | 2.9 / 5.7 / 5.6 s |
| #10 fix, 5 writes | 121,762 | 1.0 s | 0.2 s | 0.1 s | timeout: depth 11 of 15 after 30 min |
| #10 fix, 6 writes | 2,195,630 | 3.1 s | 4.0 s | 1.9 s | not run |
| #10 fix, 7 writes | 46,135,630 | 40 s, 18 GB | 97 s, 6.6 GB | 35 s, 9.8 GB | not run |

TLC's memory is mostly heap it was allowed to take (`-Xmx40g`), not what
the search needs. Veil's build comes on top: 15 s for the commitlog file
(8 configurations) and 47 s for the vnodes file (5), about 5 s of C
compilation per configuration, so a single check that takes TLC seconds is
faster in TLC. At 32 threads the two explicit-state checkers are within 10%
of each other on the large runs (Veil 9% and 11% faster); Veil needs a quarter to half of TLC's
memory.

Apalache finds the bugs, by a symbolic search for any execution of the
given length, but is 10 to 100 times slower than both, and its time grows
with the length, not the state count: a pass at the diameter is out of
reach for these models. Its strength is elsewhere: wide integer domains,
where explicit-state search can't enumerate, and inductiveness checks (see
above).

Veil is the one tool that does both jobs and more: it finds the bug as fast
as TLC, checks the fix exhaustively as fast as TLC, and can prove the fix
for every instance (`#check_invariants`, with an invariant `#updr` can
synthesize; done for #10 above).
For #7, `veil/VnodesTabletsMigrationProof.lean` first asked `#check_invariants`
to prove the fix from the four properties alone: in 14 s it proved 49 of 55
goals and named the 6 that fail (`finalize_forward` breaks `readable` and
`layout_matches`, `boot` breaks `readable` and `boot_ok`, `cleanup` breaks
`durable`, `bootstrap` breaks `readable`). Fourteen hand-written invariants
(the owner of a range holds every write of it; a published mode of tablets
means the node booted to tablets; a joiner of the current migration that
holds a write can't boot to tablets) then proved all 204 goals in 16 s; with
the published-mode column off the proof fails.

Translation cost: the Apalache copy of the vnodes spec typechecked on the
first try (mechanical annotations). The Veil copy took about 45 minutes
and two rounds of fixes: name clashes with Lean (`mt`, `W`), a set of
write ids encoded as an ordered sort with the smallest unused value picked,
and a `towner` that is all `None` or a copy of `vowner` encoded as a flag.

### Veil copies of the catalog specs

After the benchmark Veil became the single checker (2026-11-14). Every spec
with a catalog bug has a Veil copy in `veil/`, checked against TLC config by
config; the fixes of #1, #2, #4, #6 and #7 are proved for every instance
size, each proof failing with its fix removed. FINDINGS.md has the table per
bug; TOOL_COMPARISON.md has the tool comparison and what writing the proofs
took. `veil/AuthCache.lean` is a new spec, of the auth cache's permission
sets (auth/cache.cc): it finds SCYLLADB-4206 (a lookup in flight stores a
set read before an inherited role's change) and proves a candidate fix
(the clear voids the lookups in flight on its record).

### advanced_rpc_compression_negotiation

The dictionary negotiation of `advanced_rpc_compression.md`, written with
that feature. It had no configuration and its module name didn't match its
file, so it couldn't be run. `MC.cfg` checks its invariants and its
liveness property: both hold.

## Where to look next

Ranked by: invariants already written down, recent protocol bugs in the
area, a state space that stays small once abstracted, and properties that
don't need Raft's internals.

| area | why | size |
|---|---|---|
| group 0 state machine (done) | 2f372ec9707, 1efcdffaf99; crisp property | small |
| SC tablet migration (done) | most churn in the codebase; invariants written as I1..I5 | medium |
| eventually-consistent tablet migration and topology guards (done) | INV-TABL-1/2; the resurrection example in "Topology guards"; cancellation | medium |
| row cache: partitions and MVCC (done) | the rules in `row_cache.hh` and `mvcc.md`; 9ccd4ea416a | small |
| row cache: preemptible version merging | `apply_monotonically()` with sentinels, readers refreshing their cursor; where 9ccd4ea416a came from | medium |
| tombstone GC, repair and tablet merge (done) | resurrection; found SCYLLADB-6091 independently | small |
| compaction vs tablet split and merge (done) | three historic races; reproduced SCYLLADB-5896 | medium |
| raft configuration changes carried by snapshots | a recent fix; Scylla-specific non-voters | medium |
| DDL vs topology operations | 7535fabccfa (schema changes during vnodes-to-tablets migration), 580dfd63e57 (RF change pause/abort vs schema), resize finalization | medium |
| node operations (bootstrap, decommission, replace, rollback_to_normal, CDC generations) | mature code | medium |
| LWT on tablets across migrations, the view building coordinator | later | |

Further out: trace validation, i.e. logging the modelled events in
`test/cluster` runs and checking each log against the spec. That checks
that the code implements the model, which model checking alone can't.

## Limits

- A model is only as good as its abstraction. A pass means no violation
  within the bounds and the modelled behavior; a counterexample can be an
  artifact. Each spec header says what is left out.
- Implementation bugs (a double gate close, an uninitialized member, a wrong
  version computation) are out of reach; protocol and interleaving bugs are
  in.
- A spec drifts as the code changes, and nothing ties it to the code yet.
  Update it together with its design doc section. A cheap check to add:
  compare the stage names in the spec with `tablet_transition_stage`.
