#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import time

import pytest
from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.rest_client import read_barrier

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.xfail(strict=True, reason="the new replica continues the rack counter shard from a stale copy")
async def test_counter_increment_survives_tablet_migration(manager: ScyllaClusterManager) -> None:
    """Verify that a tablet migration doesn't lose a counter increment the new replica missed.

    With tablets, the counter id of a replica is its rack's, and a pending replica uses the negated
    one; once the migration ends, the new replica takes over the rack's id and continues that shard
    from its own copy. If it missed an increment the leaving replica led after streaming (a write
    to a pending replica may fail without failing the client write), its copy is behind, and its
    next increment reuses a logical clock already written: reconciliation keeps one of the two.

    Racks r1 (nodes A and B) and r2 (node C), RF=2; the tablet migrates from A to B. While B is
    held after streaming, an increment led by A fails on B and reaches C. After the migration B
    leads another increment. (Within a node the leader applies the update on every write shard
    itself, so an intra-node migration can't miss one.)
    """
    cmdline = ['--logger-log-level', 'raft_topology=debug']
    property_files = [{"dc": "dc1", "rack": r} for r in ("r1", "r1", "r2")]
    servers = await manager.servers_add(3, cmdline=cmdline, property_file=property_files,
                                        config={'hinted_handoff_enabled': False})
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
    cql_host = {s.server_id: next(h for h in hosts if h.address == s.ip_addr) for s in servers}

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          "AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.c (pk int PRIMARY KEY, n counter)")
        token = 0
        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'c')
        r1_replica = next(r for r in replicas[0].replicas if r[0] in (host_ids[servers[0].server_id], host_ids[servers[1].server_id]))
        a = next(s for s in servers if host_ids[s.server_id] == r1_replica[0])
        b = next(s for s in servers[:2] if s.server_id != a.server_id)

        async def increment(leader, cl: ConsistencyLevel) -> None:
            # Coordinated by a replica, which then leads the update.
            await cql.run_async(SimpleStatement(f"UPDATE {ks}.c SET n = n + 1 WHERE pk = 1", consistency_level=cl),
                                host=cql_host[leader.server_id])

        sent = 0
        for _ in range(3):
            await increment(a, ConsistencyLevel.ALL)
            sent += 1

        b_log = await manager.server_open_log(b.server_id)
        mark = await b_log.mark()
        await manager.api.enable_injection(b.ip_addr, "pause_after_streaming_tablet_before_fetch_column_mappings", one_shot=True,
                                           parameters={"keyspace": ks, "table": "c"})
        migration = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, 'c',
                                                                r1_replica[0], r1_replica[1], host_ids[b.server_id], 0, token))
        await b_log.wait_for("pause_after_streaming_tablet_before_fetch_column_mappings: waiting", from_mark=mark, timeout=60)

        # Streaming is done: an increment led by A which the pending replica B misses.
        await manager.api.enable_injection(b.ip_addr, "database_apply", one_shot=False,
                                           parameters={"ks_name": ks, "cf_name": "c", "what": "throw"})
        await increment(a, ConsistencyLevel.ONE)
        sent += 1
        await manager.api.disable_injection(b.ip_addr, "database_apply")

        await manager.api.message_injection(b.ip_addr, "pause_after_streaming_tablet_before_fetch_column_mappings")
        await migration
        # Every node has applied the end of the migration, so B leads under the rack's id.
        for srv in servers:
            await read_barrier(manager.api, srv.ip_addr)

        for _ in range(3):
            await increment(b, ConsistencyLevel.ALL)
            sent += 1

        rows = await cql.run_async(SimpleStatement(f"SELECT n FROM {ks}.c WHERE pk = 1", consistency_level=ConsistencyLevel.ALL))
        assert rows[0].n == sent, f"the counter is {rows[0].n} after {sent} increments"
