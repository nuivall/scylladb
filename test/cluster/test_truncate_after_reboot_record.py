#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import logging
import time

import pytest

from test.cluster.util import new_test_keyspace, reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("reboot", [True, False])
async def test_writes_after_truncate_survive_a_crash_after_reboot(manager: ScyllaClusterManager, reboot: bool) -> None:
    """Commitlog segment ids start at the machine's uptime in ms (steady_clock) or above
    the segments on disk, so after a machine reboot and a clean shutdown they restart far
    below the previous run's. TRUNCATE saves as its truncation record the highest replay
    position of the discarded sstables (table::discard_sstables()), including sstables
    flushed before the reboot, whose positions are in the old, higher id space; replay
    skips every entry of the table at or below the record, so the writes acknowledged
    after the TRUNCATE are lost on a crash. The truncation records dropped at boot
    (#15354) don't cover a record written after the reboot.

    The reboot is imitated with decrease_commitlog_base_segment_id, as test_commitlog.py
    test_reboot does; without it (the control), the ids keep growing.
    """
    server = await manager.server_add(config={'commitlog_sync': 'batch'}, cmdline=['--smp', '1'])
    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES (-1, -1)")
        await manager.api.keyspace_flush(server.ip_addr, ks, "t")

        await manager.server_stop_gracefully(server.server_id)
        if reboot:
            await manager.server_update_config(server.server_id, 'error_injections_at_startup',
                                               ['decrease_commitlog_base_segment_id'])
        await manager.server_start(server.server_id)
        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

        await cql.run_async(f"TRUNCATE {ks}.t")
        keys = range(100)
        for pk in keys:
            await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({pk}, {pk})")
        assert len(await cql.run_async(f"SELECT pk FROM {ks}.t")) == len(keys)

        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

        rows = await cql.run_async(f"SELECT pk FROM {ks}.t")
        logger.info("reboot=%s: %d of %d rows after the crash", reboot, len(rows), len(keys))
        assert len(rows) == len(keys), f"{len(keys) - len(rows)} of {len(keys)} rows written after the TRUNCATE lost in a crash"
