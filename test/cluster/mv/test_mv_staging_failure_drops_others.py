#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import time

import pytest

from test.cluster.mv.test_mv_staging import assert_row_count_on_host, delete_table_sstables
from test.cluster.util import new_test_keyspace, wait_for_cql_and_get_hosts
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for, wait_for_view
from cassandra.query import SimpleStatement, ConsistencyLevel

logger = logging.getLogger(__name__)

TABLES = ["tab1", "tab2"]
ROWS = 1000


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_staging_failure_of_one_table_keeps_others(manager: ScyllaClusterManager):
    """When processing the staging sstables of one table fails,
    view_update_generator's fiber re-queues that table only and leaves the
    pass; the other tables of the pass, taken from the queue with it, are
    dropped, and their view updates are never generated (until a restart).
    """
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'enabled': false}") as ks:
        for t in TABLES:
            await cql.run_async(f"CREATE TABLE {ks}.{t} (key int, c int, v int, PRIMARY KEY (key))")
            await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{t} (key, c, v) VALUES ({i}, {i}, 1)") for i in range(ROWS)])
            await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv_{t} AS SELECT * FROM {ks}.{t} "
                                "WHERE key IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, key)")
            await wait_for_view(cql, f"mv_{t}", 2)

        # Node 0 loses its sstables of both tables and views, so repair brings
        # the base rows back as staging sstables.
        for t in TABLES:
            await manager.api.keyspace_flush(servers[0].ip_addr, ks, t)
            await manager.api.keyspace_flush(servers[0].ip_addr, ks, f"mv_{t}")
            await delete_table_sstables(manager, servers[0], ks, t)
            await delete_table_sstables(manager, servers[0], ks, f"mv_{t}")
        await manager.server_stop_gracefully(servers[0].server_id)
        await manager.server_start(servers[0].server_id)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        # Every pass fails on the first table it processes.
        s0_log = await manager.server_open_log(servers[0].server_id)
        mark = await s0_log.mark()
        await manager.api.enable_injection(servers[0].ip_addr, "view_update_generator_consume_staging_sstable", one_shot=False)
        for t in TABLES:
            await manager.api.repair(servers[0].ip_addr, ks, t)
        # Both tables are queued now; let a few passes run.
        async def failed_passes() -> bool | None:
            return len(await s0_log.grep(f"Processing {ks} failed for table", from_mark=mark)) >= 4 or None
        await wait_for(failed_passes, time.time() + 60)
        await manager.api.disable_injection(servers[0].ip_addr, "view_update_generator_consume_staging_sstable")

        # Without failures, every queued staging sstable gets processed.
        processed = []
        async def all_processed() -> bool | None:
            nonlocal processed
            processed = [t for t in TABLES if await s0_log.grep(f"Processed {ks}.{t}:", from_mark=mark)]
            return len(processed) == len(TABLES) or None
        try:
            await wait_for(all_processed, time.time() + 30)
        except Exception:
            pass

        await manager.server_stop_gracefully(servers[1].server_id)
        host0 = (await wait_for_cql_and_get_hosts(cql, [servers[0]], time.time() + 60))[0]
        counts = {}
        for t in TABLES:
            await assert_row_count_on_host(cql, host0, ks, t, ROWS)
            stmt = SimpleStatement(f"SELECT * FROM {ks}.mv_{t}", consistency_level=ConsistencyLevel.LOCAL_ONE)
            counts[t] = len(await cql.run_async(stmt, host=host0))
        logger.info("processed: %s, view rows on node 0: %s", processed, counts)
        await manager.server_start(servers[1].server_id)
        assert counts == {t: ROWS for t in TABLES}, \
            f"view rows on node 0: {counts}; staging processed for {processed} only"
