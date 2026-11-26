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

from test.cluster.util import get_topology_coordinator
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

INJECTION = "determine_rf_change_actions_per_rack_throw"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.xfail(strict=True, reason="tombstones of an RF=1 table are purged at once during a multi-step RF change")
async def test_tombstones_of_extended_rf_one_table_are_not_purged_immediately(manager: ScyllaClusterManager) -> None:
    """A table counts as RF=1 for tombstone GC by its keyspace's strategy RF
    (table::update_tombstone_gc_rf_one()), and repair mode then purges its tombstones
    immediately. A multi-step RF change (each DC 0<->N) keeps the old options until it
    completes, so while it extends an RF=1 keyspace to a new DC, the tablets already
    have replicas there and every replica still purges tombstones immediately: a
    replica that missed the delete keeps the row, and repair brings it back.

    The RF change is held after the base tables are extended (the injection throws in
    the planner between extending tables and views). A row deleted at CL=ALL then has
    its tombstone purged by a major compaction on every replica, although the tablet
    has three replicas and was never repaired. Once the change completes, the same
    sequence keeps the tombstone (the control).
    """
    config = {"tablets_mode_for_new_keyspaces": "enabled", "rf_rack_valid_keyspaces": "false",
              "tablet_load_stats_refresh_interval_in_seconds": 1}
    cmdline = ["--enforce-rack-list", "true", "--smp", "1", "--logger-log-level", "load_balancer=debug"]
    servers = [await manager.server_add(config=config, cmdline=cmdline, property_file={'dc': 'dc1', 'rack': 'rack1a'}),
               await manager.server_add(config=config, cmdline=cmdline, property_file={'dc': 'dc2', 'rack': 'rack2a'}),
               await manager.server_add(config=config, cmdline=cmdline, property_file={'dc': 'dc2', 'rack': 'rack2b'})]
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['rack1a']} AND tablets = {'initial': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int) WITH tombstone_gc = {'mode': 'repair'}")
    for k in range(4):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({k}, {k})", consistency_level=ConsistencyLevel.ALL))

    coord = await manager.find_server_by_host_id(servers, await get_topology_coordinator(manager))
    for s in servers:
        await manager.api.enable_injection(s.ip_addr, INJECTION, one_shot=False)
    async def alter_keyspace():
        await cql.run_async(
            "ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ['rack1a'], 'dc2': ['rack2a', 'rack2b']}",
            timeout=400)
    alter = asyncio.create_task(alter_keyspace())
    await manager.api.wait_for_injection_enter(coord.ip_addr, INJECTION, threshold=1, deadline=time.time() + 120)

    replicas = await get_all_tablet_replicas(manager, servers[0], "ks", "t")
    logger.info("tablet replicas while the RF change is held: %s", replicas)
    assert all(len(t.replicas) == 3 for t in replicas)

    async def delete_compact_and_look(pk: int) -> dict:
        await cql.run_async(SimpleStatement(f"DELETE FROM ks.t WHERE pk = {pk}", consistency_level=ConsistencyLevel.ALL))
        await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "ks", "t") for s in servers])
        await asyncio.sleep(2)  # gc_before has a 1 s resolution
        await asyncio.gather(*[manager.api.keyspace_compaction(s.ip_addr, "ks", "t") for s in servers])
        seen = {}
        for s, h in zip(servers, hosts):
            rows = await cql.run_async(f"SELECT pk, mutation_fragment_kind FROM MUTATION_FRAGMENTS(ks.t) WHERE pk = {pk}", host=h)
            seen[s.server_id] = sorted({r.mutation_fragment_kind for r in rows})
        return seen

    during = await delete_compact_and_look(0)
    logger.info("pk 0, deleted during the RF change, after a major compaction: %s", during)

    for s in servers:
        await manager.api.message_injection(s.ip_addr, INJECTION)
        await manager.api.disable_injection(s.ip_addr, INJECTION)
    await alter

    after = await delete_compact_and_look(1)
    logger.info("pk 1, deleted after the RF change, after a major compaction: %s", after)
    assert all(after.values()), f"control: the tombstone of a 3-replica table was purged without a repair: {after}"
    assert all(during.values()), \
        f"the tombstone was purged immediately on the replicas with no data left: {during} (3 replicas, no repair)"
