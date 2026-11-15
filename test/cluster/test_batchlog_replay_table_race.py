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
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

PAUSE = "batchlog_replay_before_send"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("op", ["drop", "truncate"])
async def test_batchlog_replay_racing_a_table_change(manager: ScyllaClusterManager, op: str) -> None:
    """A logged batch over tables a and b whose send failed is replayed from the batchlog.
    The replay checks each table (dropped? truncated after the batch?) when it decodes
    the batch, and sends later, after the replay rate limiter. A DROP TABLE b in between
    makes the send throw no_such_column_family, which the replay takes as "drop the
    batch": a's row is never written. A TRUNCATE b in between lets b's row, written
    before the truncate, come back after it.
    """
    config = {"error_injections_at_startup": ["short_batchlog_manager_replay_interval"],
              "write_request_timeout_in_ms": 2000}
    servers = await manager.servers_add(2, config=config, cmdline=["--logger-log-level", "batchlog_manager=trace"],
                                        auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async def on_all(f, injection, **kw):
        await asyncio.gather(*[f(s.ip_addr, injection, **kw) for s in servers])

    async def batchlog_rows() -> int:
        return sum([(await cql.run_async("SELECT COUNT(*) FROM system.batchlog_v2", host=h))[0].count for h in hosts])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        for t in ["a", "b"]:
            await cql.run_async(f"CREATE TABLE {ks}.{t} (pk int PRIMARY KEY, v int)")

        # The batch reaches the batchlog, but none of its mutations is sent.
        await on_all(manager.api.enable_injection, "skip_batch_replay", one_shot=False)
        await on_all(manager.api.enable_injection, "storage_proxy_fail_send_batch", one_shot=False)
        try:
            await cql.run_async(f"BEGIN BATCH INSERT INTO {ks}.a (pk, v) VALUES (0, 0); INSERT INTO {ks}.b (pk, v) VALUES (0, 0); APPLY BATCH")
        except Exception as e:
            logger.info("batch failed as injected: %s", e)
        await on_all(manager.api.disable_injection, "storage_proxy_fail_send_batch")
        assert await batchlog_rows() > 0

        # Replay, held after the table checks.
        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await log.mark() for log in logs]
        await on_all(manager.api.enable_injection, PAUSE, one_shot=True)
        await on_all(manager.api.disable_injection, "skip_batch_replay")

        async def paused():
            for s, log, mark in zip(servers, logs, marks):
                if await log.grep(f"{PAUSE}: waiting", from_mark=mark):
                    return s
            return None
        replayer = await wait_for(paused, time.time() + 60)
        logger.info("replay paused on %s", replayer)

        if op == "drop":
            await cql.run_async(f"DROP TABLE {ks}.b")
        else:
            await cql.run_async(f"TRUNCATE {ks}.b")
        await manager.api.message_injection(replayer.ip_addr, PAUSE)
        await on_all(manager.api.disable_injection, PAUSE)

        async def batchlog_empty():
            return await batchlog_rows() == 0 or None
        await wait_for(batchlog_empty, time.time() + 60)

        a_rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.a", consistency_level=ConsistencyLevel.ALL))
        b_rows = [] if op == "drop" else await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.b", consistency_level=ConsistencyLevel.ALL))
        logger.info("%s: a=%s b=%s", op, list(a_rows), list(b_rows))
        assert len(a_rows) == 1, f"after {op} of b during the replay, the batch's row of a is missing"
        assert len(b_rows) == 0, "b's row, written before the TRUNCATE, came back after it"
