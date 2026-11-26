#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("flush_first", [pytest.param(True, marks=pytest.mark.xfail(strict=True, reason="the truncation record is below the writes TRUNCATE dropped from the memtable")), False])
async def test_truncated_rows_stay_gone_after_a_crash(manager: ScyllaClusterManager, flush_first: bool) -> None:
    """Without auto_snapshot, TRUNCATE doesn't flush: it drops the memtables and leaves
    their writes in the active commitlog segment. The truncation record that keeps them
    from being replayed is the highest replay position of the discarded sstables
    (database::truncate(), table::discard_sstables()); the low mark taken before the
    truncate is used only when there are no sstables. With an sstable flushed before the
    later writes, the record is below those writes, and a crash replays them: the
    truncated rows come back. Without the earlier flush (the control), the record is the
    low mark and covers every write.
    """
    config = {'commitlog_sync': 'batch', 'auto_snapshot': False}
    servers = await manager.servers_add(1, config=config, cmdline=['--smp', '1'])
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES (-1, -1)")
        if flush_first:
            await manager.api.keyspace_flush(servers[0].ip_addr, ks, "t")
        keys = range(100)
        for pk in keys:
            await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({pk}, {pk})")

        await cql.run_async(f"TRUNCATE {ks}.t")
        assert not await cql.run_async(f"SELECT pk FROM {ks}.t")

        await manager.server_stop(servers[0].server_id, convict=False)
        await manager.server_start(servers[0].server_id)
        cql, _ = await manager.get_ready_cql(servers)

        rows = await cql.run_async(f"SELECT pk FROM {ks}.t")
        logger.info("flush_first=%s: %d rows after the crash", flush_first, len(rows))
        assert not rows, f"{len(rows)} truncated rows came back after a crash"
