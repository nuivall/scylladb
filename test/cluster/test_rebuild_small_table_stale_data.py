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
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("small_table_optimization", [pytest.param(True, marks=pytest.mark.xfail(strict=True, reason="each peer sends the rows of ranges it no longer owns")), False])
async def test_rebuild_does_not_resurrect_unowned_data(manager: ScyllaClusterManager, small_table_optimization: bool) -> None:
    """Repair-based rebuild applies the small table optimization to user tables under
    small_table_optimization_for_rbno_max_table_size (1 GiB by default, cb52178d5d3):
    one repair over the whole ring with every node as a peer. Each peer reads the
    whole ring from its own sstables, including ranges it no longer owns, and nothing
    filters what a peer sends by whether it owns it; the master and the receivers keep
    the rows they own. After a bootstrap, the old owners keep the ranges they gave up
    until a cleanup (the topology coordinator cleans up only before removenode and
    decommission). Rows deleted afterwards, whose tombstones the owners purged, come
    back from those copies. With the optimization off (the control), each range is
    repaired among its owners only.
    """
    config = {'tablets_mode_for_new_keyspaces': 'disabled',
              'small_table_optimization_for_rbno_max_table_size': 1073741824 if small_table_optimization else 0}
    # No commitlog cap on gc_before (as in test_data_resurrection_in_memtable.py).
    cmdline = ['--enable-commitlog', '0', '--smp', '1', '--logger-log-level', 'repair=info']
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, property_file={'dc': 'dc1', 'rack': 'r1'})
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int) WITH tombstone_gc = {'mode': 'immediate'}")
    keys = range(200)
    insert = cql.prepare("INSERT INTO ks.t (pk, v) VALUES (?, ?)")
    insert.consistency_level = ConsistencyLevel.ALL
    await asyncio.gather(*[cql.run_async(insert, (k, k)) for k in keys])
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "ks", "t") for s in servers])

    # A bootstrap moves ranges away from the first three nodes; no cleanup follows.
    servers.append(await manager.server_add(config=config, cmdline=cmdline, property_file={'dc': 'dc1', 'rack': 'r1'}))
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    delete = cql.prepare("DELETE FROM ks.t WHERE pk = ?")
    delete.consistency_level = ConsistencyLevel.ALL
    await asyncio.gather(*[cql.run_async(delete, (k,)) for k in keys])
    await asyncio.sleep(1)
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "ks", "t") for s in servers])
    # Compaction drops the data of sstables that require cleanup, so the first node,
    # whose leftover copies we keep, isn't compacted (as when its old sstables simply
    # haven't been compacted since the bootstrap); the others purge their tombstones.
    await asyncio.gather(*[manager.api.keyspace_compaction(s.ip_addr, "ks", "t") for s in servers[1:]])

    async def local_rows() -> dict:
        seen = {}
        for s in servers:
            h = next(h for h in hosts if h.address == s.ip_addr)
            frags = await cql.run_async("SELECT pk, mutation_fragment_kind FROM MUTATION_FRAGMENTS(ks.t)", host=h)
            seen[s.server_id] = sum(1 for f in frags if f.mutation_fragment_kind == 'clustering row')
        return seen
    before = await local_rows()
    logger.info("rows held locally after the delete and compaction (stale copies): %s", before)
    assert not await cql.run_async(SimpleStatement("SELECT pk FROM ks.t", consistency_level=ConsistencyLevel.ALL))

    # The REST call directly: manager.rebuild_node() stops the node afterwards.
    await manager.api.rebuild_node(servers[1].ip_addr, timeout=300)
    after = await local_rows()
    logger.info("rows held locally after the rebuild: %s", after)

    rows = await cql.run_async(SimpleStatement("SELECT pk FROM ks.t", consistency_level=ConsistencyLevel.ALL))
    logger.info("small_table_optimization=%s: %d of %d deleted rows back", small_table_optimization, len(rows), len(keys))
    assert before[servers[0].server_id] > 0, "precondition: no stale copies left after the bootstrap"
    assert not rows, f"{len(rows)} of {len(keys)} deleted rows came back after a rebuild"
