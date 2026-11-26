#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("restart", [False, pytest.param(True, marks=pytest.mark.xfail(strict=True, reason="logstor recovery brings back rows truncated before the restart"))])
async def test_truncated_logstor_rows_stay_gone(manager: ScyllaClusterManager, restart: bool) -> None:
    """Verify that rows of a truncated logstor table don't come back.

    TRUNCATE clears the index of the table and discards the full segments its compaction
    groups own, zeroing their first block (discard_segments()). User writes land in mixed
    segments first, and those keep their records on disk: the separator copies the records
    out and frees the mixed segment without zeroing it. Recovery indexes every record of a
    segment whose header is valid, with no truncation time to filter by, so a restart after
    the TRUNCATE brings the truncated rows back.
    """
    cfg = {'experimental_features': ['logstor']}
    servers = await manager.servers_add(1, config=cfg)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH storage_engine = 'logstor'")
        keys = range(100)
        for pk in keys:
            await cql.run_async(f"INSERT INTO {ks}.test (pk, v) VALUES ({pk}, {pk})")

        await cql.run_async(f"TRUNCATE {ks}.test")
        assert not await cql.run_async(f"SELECT pk FROM {ks}.test")

        if restart:
            await manager.server_stop_gracefully(servers[0].server_id)
            await manager.server_start(servers[0].server_id)
            cql, _ = await manager.get_ready_cql(servers)

        rows = await cql.run_async(f"SELECT pk FROM {ks}.test")
        assert not rows, f"{len(rows)} of {len(keys)} truncated rows came back"
