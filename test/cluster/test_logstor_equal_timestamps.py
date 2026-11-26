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

TS = 1000000

@pytest.mark.xfail(strict=True, reason="logstor recovery resolves equal timestamps differently from the runtime index")
async def test_logstor_equal_timestamp_survives_restart(manager: ScyllaClusterManager) -> None:
    """Verify that a restart doesn't change the value logstor returns for writes of the same timestamp.

    The runtime index keeps the later write; recovery keeps the record in the segment with the
    higher sequence number. When the separator flushes the first write into a new full segment
    while the mixed segment it came from is still active, the second write lands in that older
    mixed segment, and recovery picks the first write. The separator flushes a group's buffer
    when it is full, so the test fills it with large rows of the same tablet. The node is killed:
    a graceful stop would flush the separator again, moving the second write into a newer full
    segment too.
    """
    cfg = {'experimental_features': ['logstor']}
    servers = await manager.servers_add(1, config=cfg, cmdline=["--logger-log-level", "logstor=trace", "--smp", "1"])
    cql = manager.get_cql()
    log = await manager.server_open_log(servers[0].server_id)

    async with new_test_keyspace(manager, "WITH tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.ls (pk int PRIMARY KEY, v int, pad text) WITH storage_engine = 'logstor'")
        mark = await log.mark()
        await cql.run_async(f"INSERT INTO {ks}.ls (pk, v) VALUES (1, 1) USING TIMESTAMP {TS}")
        pad = 'x' * (30 * 1024)
        for pk in range(100, 120):
            await cql.run_async(f"INSERT INTO {ks}.ls (pk, v, pad) VALUES ({pk}, 0, '{pad}')")
            if await log.grep("Write full segment .* from separator", from_mark=mark):
                break
        else:
            pytest.fail("the separator didn't flush")
        await cql.run_async(f"INSERT INTO {ks}.ls (pk, v) VALUES (1, 2) USING TIMESTAMP {TS}")
        before = [(r.pk, r.v) for r in await cql.run_async(f"SELECT pk, v FROM {ks}.ls WHERE pk = 1")]

        await manager.server_stop(servers[0].server_id, convict=False)
        await manager.server_start(servers[0].server_id)
        cql, _ = await manager.get_ready_cql(servers)

        after = [(r.pk, r.v) for r in await cql.run_async(f"SELECT pk, v FROM {ks}.ls WHERE pk = 1")]
        assert after == before, f"logstor returned {before} before the restart and {after} after it"
