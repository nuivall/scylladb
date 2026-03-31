# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Multi-node cluster tests for DELETE ... ALLOW FILTERING.
#
# Basic predicate/syntax tests live in test/cqlpy/test_delete_allow_filtering.py.

import asyncio
import logging
import pytest
import time

from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace, new_test_table, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_filtering_delete_with_paging(manager: ManagerClient):
    """Filtering delete across many partitions to exercise paging.
    Uses enough data to force multiple pages (page size is 1000)."""
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        async with new_test_table(manager, ks, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
            stmts = []
            for p in range(400):
                for c in range(10):
                    stmts.append(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c % 3})")
            await asyncio.gather(*[cql.run_async(s) for s in stmts])

            await cql.run_async(f"DELETE FROM {table} WHERE v = 0 ALLOW FILTERING")

            rows = await cql.run_async(
                SimpleStatement(f"SELECT count(*) FROM {table}",
                                consistency_level=ConsistencyLevel.ALL))
            total_original = 400 * 10
            deleted = sum(1 for p in range(400) for c in range(10) if c % 3 == 0)
            assert rows[0].count == total_original - deleted


@pytest.mark.asyncio
async def test_filtering_delete_propagated(manager: ManagerClient):
    """Writes with ALL and reads from individual nodes to confirm
    all replicas received the tombstones."""
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        async with new_test_table(manager, ks, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
            stmts = []
            for p in range(20):
                for c in range(5):
                    stmts.append(SimpleStatement(
                        f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c % 3})",
                        consistency_level=ConsistencyLevel.ALL))
            await asyncio.gather(*[cql.run_async(s) for s in stmts])

            await cql.run_async(SimpleStatement(
                f"DELETE FROM {table} WHERE v = 0 ALLOW FILTERING",
                consistency_level=ConsistencyLevel.ALL))

            expected = sorted([(p, c, c % 3) for p in range(20) for c in range(5) if c % 3 != 0])
            for host in hosts:
                rows = await cql.run_async(
                    SimpleStatement(f"SELECT p, c, v FROM {table}",
                                    consistency_level=ConsistencyLevel.ONE),
                    host=host)
                assert sorted(rows) == expected


@pytest.mark.asyncio
async def test_filtering_delete_concurrent_from_different_nodes(manager: ManagerClient):
    """Two filtering deletes with different predicates executed concurrently
    from different coordinator nodes. Both should complete without conflict
    and all nodes should agree on the final state."""
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        async with new_test_table(manager, ks, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
            stmts = []
            for p in range(20):
                for c in range(4):
                    stmts.append(SimpleStatement(
                        f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c})",
                        consistency_level=ConsistencyLevel.ALL))
            await asyncio.gather(*[cql.run_async(s) for s in stmts])

            del1 = cql.run_async(
                SimpleStatement(f"DELETE FROM {table} WHERE v = 0 ALLOW FILTERING",
                                consistency_level=ConsistencyLevel.QUORUM),
                host=hosts[0])
            del2 = cql.run_async(
                SimpleStatement(f"DELETE FROM {table} WHERE v = 3 ALLOW FILTERING",
                                consistency_level=ConsistencyLevel.QUORUM),
                host=hosts[1])
            await asyncio.gather(del1, del2)

            expected = sorted([(p, c, c) for p in range(20) for c in range(4)
                               if c not in (0, 3)])
            for host in hosts:
                rows = await cql.run_async(
                    SimpleStatement(f"SELECT p, c, v FROM {table}",
                                    consistency_level=ConsistencyLevel.ALL),
                    host=host)
                assert sorted(rows) == expected
