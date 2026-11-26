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

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

INJECTION = "apply_counter_update_delay_100ms"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("racks", [pytest.param(1, marks=pytest.mark.xfail(strict=True, reason="replicas of one rack share the rack counter id")), 3])
async def test_counter_increments_led_by_two_replicas_of_one_rack(manager: ScyllaClusterManager, racks: int):
    """Counter increments led by two replicas of a tablet in the same rack.

    A tablet replica's counter id is its rack's (table::update_effective_replication_map(),
    topology::get_rack_uuid()), which assumes one replica per rack. With
    rf_rack_valid_keyspaces off (the default), RF=3 in a single rack places three
    replicas in one rack. Two leaders then read the same shard, and each writes its
    increment with the same counter id and the same next clock; merging keeps one.
    With three racks (the control), each replica has its own counter id.
    """
    servers = await manager.servers_add(3, config={'rf_rack_valid_keyspaces': False}, cmdline=['--smp', '1'],
                                        property_file=[{'dc': 'dc1', 'rack': f'rack{i % racks + 1}'} for i in range(3)])
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, c counter)")
        await cql.run_async(SimpleStatement(f"UPDATE {ks}.t SET c = c + 10 WHERE pk = 0", consistency_level=ConsistencyLevel.ALL))

        # Two coordinators, each a replica and so the leader of its own update; each
        # reads the counter and waits 100 ms before transforming its update into a shard.
        sessions = [await manager.get_cql_exclusive(s) for s in servers[:2]]
        for s in servers[:2]:
            await manager.api.enable_injection(s.ip_addr, INJECTION, one_shot=False)
        await asyncio.gather(
            sessions[0].run_async(SimpleStatement(f"UPDATE {ks}.t SET c = c + 1 WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)),
            sessions[1].run_async(SimpleStatement(f"UPDATE {ks}.t SET c = c + 2 WHERE pk = 0", consistency_level=ConsistencyLevel.ONE)),
        )
        for s in servers[:2]:
            await manager.api.disable_injection(s.ip_addr, INJECTION)

        # Every update was acknowledged; let replication settle, then read each replica.
        await asyncio.sleep(2)
        values = {}
        for s, h in zip(servers, hosts):
            exclusive = await manager.get_cql_exclusive(s)
            await wait_for_cql_and_get_hosts(exclusive, [s], time.time() + 60)
            rows = await exclusive.run_async(SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 0", consistency_level=ConsistencyLevel.ONE))
            values[s.server_id] = rows[0].c
        total = (await cql.run_async(SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 0", consistency_level=ConsistencyLevel.ALL)))[0].c
        logger.info("per replica (CL=ONE, own coordinator): %s, CL=ALL: %s", values, total)
        assert total == 13, f"counter is {total} after +10, +1, +2 acknowledged (per replica: {values})"


@pytest.mark.parametrize("racks", [pytest.param(1, marks=pytest.mark.xfail(strict=True, reason="replicas of one rack share the rack counter id")), 3])
async def test_concurrent_counter_increments_without_injection(manager: ScyllaClusterManager, racks: int):
    """The same without an injection: two clients, each pinned to its own replica (so
    each replica leads its clients' updates), increment one counter concurrently at
    CL=QUORUM; every acknowledged increment must count."""
    servers = await manager.servers_add(3, config={'rf_rack_valid_keyspaces': False}, cmdline=['--smp', '1'],
                                        property_file=[{'dc': 'dc1', 'rack': f'rack{i % racks + 1}'} for i in range(3)])
    cql, hosts = await manager.get_ready_cql(servers)
    per_client = 300

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, c counter)")
        sessions = [await manager.get_cql_exclusive(s) for s in servers[:2]]
        stmt = SimpleStatement(f"UPDATE {ks}.t SET c = c + 1 WHERE pk = 0", consistency_level=ConsistencyLevel.QUORUM)

        async def client(session) -> int:
            acked = 0
            for _ in range(per_client):
                await session.run_async(stmt)
                acked += 1
            return acked

        acked = sum(await asyncio.gather(*(client(s) for s in sessions for _ in range(4))))
        await asyncio.sleep(2)
        total = (await cql.run_async(SimpleStatement(f"SELECT c FROM {ks}.t WHERE pk = 0", consistency_level=ConsistencyLevel.ALL)))[0].c
        logger.info("racks=%d: %d increments acknowledged, counter at CL=ALL: %d", racks, acked, total)
        assert total == acked, f"{acked - total} of {acked} acknowledged increments lost"
