#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import time

import pytest
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

PAUSE = "bootstrap_pause_after_streaming"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("overlap", [True, False])
async def test_repair_during_vnode_bootstrap_keeps_deleted_rows_deleted(manager: ScyllaClusterManager, overlap: bool) -> None:
    """A vnode repair picks its participants from the natural replicas only
    (repair.cc get_neighbors()) and records the range as repaired when all of them
    took part (update_system_repair_table()). During a bootstrap the joining node is a
    pending replica: it streamed the range from the node that loses it, and rows a
    repair writes to that node afterwards go to its sstables, not to the joiner. If
    that node missed a delete, the joiner keeps the deleted rows; the repair records a
    repair time on the natural replicas, which then purge the tombstones (repair-mode
    tombstone GC) and the rows come back once the joiner owns the range.

    The joiner is held after streaming; the repair runs meanwhile. With the repair
    after the bootstrap (the control), the joiner takes part and gets the tombstones.
    """
    config = {'tablets_mode_for_new_keyspaces': 'disabled', 'hinted_handoff_enabled': False}
    # As in test_data_resurrection_in_memtable.py: no commitlog cap on gc_before, and a
    # repair time from a fresh hints flush rather than one cached from before the deletes.
    cmdline = ['--enable-commitlog', '0', '--repair-hints-batchlog-flush-cache-time-in-ms', '0']
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, property_file={'dc': 'dc1', 'rack': 'r1'})
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int) WITH tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': 1}")
    keys = range(200)
    insert = cql.prepare("INSERT INTO ks.t (pk, v) VALUES (?, ?)")
    insert.consistency_level = ConsistencyLevel.ALL
    await asyncio.gather(*[cql.run_async(insert, (k, k)) for k in keys])
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "ks", "t") for s in servers])

    # The third node misses the deletes (no hints).
    await manager.server_stop_gracefully(servers[2].server_id)
    delete = cql.prepare("DELETE FROM ks.t WHERE pk = ?")
    delete.consistency_level = ConsistencyLevel.QUORUM
    await asyncio.gather(*[cql.run_async(delete, (k,)) for k in keys])
    await manager.server_start(servers[2].server_id)
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    joiner = await manager.server_add(start=False, cmdline=cmdline, property_file={'dc': 'dc1', 'rack': 'r1'},
                                      config={**config, 'error_injections_at_startup': [PAUSE]})
    bootstrap = asyncio.create_task(manager.server_start(joiner.server_id))
    log = await manager.server_open_log(joiner.server_id)

    async def paused():
        return await log.grep(f"{PAUSE}: waiting") or None
    await wait_for(paused, time.time() + 180)
    logger.info("the joiner streamed and is held")

    if overlap:
        await manager.api.repair(servers[0].ip_addr, "ks", "t")
        logger.info("repaired while the joiner is pending")
    await manager.api.message_injection(joiner.ip_addr, PAUSE)
    await bootstrap
    servers.append(joiner)
    if not overlap:
        await manager.api.repair(servers[0].ip_addr, "ks", "t")
        logger.info("repaired after the bootstrap")

    history = await cql.run_async("SELECT * FROM system.repair_history")
    logger.info("repair_history rows on the driver's node: %d", len(history))

    await asyncio.sleep(2)  # past the propagation delay
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "ks", "t") for s in servers])
    await asyncio.gather(*[manager.api.keyspace_compaction(s.ip_addr, "ks", "t") for s in servers])

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    for s in servers:
        h = next(h for h in hosts if h.address == s.ip_addr)
        frags = await cql.run_async("SELECT pk, mutation_fragment_kind, metadata FROM MUTATION_FRAGMENTS(ks.t)", host=h)
        kinds = {}
        for f in frags:
            kinds[f.mutation_fragment_kind] = kinds.get(f.mutation_fragment_kind, 0) + 1
        tombstoned = sum(1 for f in frags if f.mutation_fragment_kind == 'partition start' and '"tombstone":{}' not in f.metadata)
        logger.info("node %s: %s, partitions with a tombstone: %d", s.server_id, kinds, tombstoned)
    rows = await cql.run_async(SimpleStatement("SELECT pk FROM ks.t", consistency_level=ConsistencyLevel.ALL))
    logger.info("overlap=%s: %d of %d deleted rows back", overlap, len(rows), len(keys))
    assert not rows, f"{len(rows)} of {len(keys)} deleted rows came back"
