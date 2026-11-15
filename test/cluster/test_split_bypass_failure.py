#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import struct
import time

import pytest
from cassandra.murmur3 import murmur3

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace, reconnect_driver

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_failed_split_bypass_keeps_the_sstable(manager: ScyllaClusterManager) -> None:
    """Verify that a tablet split which fails to move an sstable doesn't delete it.

    The split moves an sstable that lies on one side of the split point as is (the bypass): its
    completion has the same sstable as input and output. If attaching the outputs of a completion
    fails, update_sstable_sets_on_compaction_completion() deletes them, so as not to leak them on
    disk -- which, for a move, is the live input. Reads keep using it until the node restarts, and
    then its rows are gone. The same holds for off-strategy, which moves sstables from the
    maintenance set to the main one the same way.

    The test fails the split's first completion with an error injection, restarts the node and reads
    every row back.
    """
    # Without a commitlog, so that the restart sees only what is on disk: data written long enough
    # ago isn't in the commitlog any more, which would otherwise replay it over the loss.
    server = await manager.server_add(cmdline=['--enable-commitlog', '0'])
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                          "AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        await manager.api.disable_autocompaction(server.ip_addr, ks, "t")

        # Keys of the left half of the token ring only, so that the split moves their sstable as is.
        keys = [pk for pk in range(20000) if murmur3(struct.pack('>i', pk)) < -(1 << 62)][:200]
        for pk in keys:
            await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({pk}, {pk})")
        await manager.api.keyspace_flush(server.ip_addr, ks, "t")

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        await manager.api.enable_injection(server.ip_addr, "update_sstable_sets_on_compaction_completion_fail_once",
                                           one_shot=False, parameters={"ks_name": ks})
        await manager.api.enable_injection(server.ip_addr, "tablet_force_tablet_count_increase", one_shot=True)
        # The split moves the sstable to the left tablet's group; that completion fails.
        await log.wait_for("update_sstable_sets_on_compaction_completion_fail_once error injection", from_mark=mark, timeout=120)
        await manager.api.disable_injection(server.ip_addr, "tablet_force_tablet_count_increase")

        await manager.server_stop_gracefully(server.server_id)
        await manager.server_start(server.server_id)
        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

        rows = await cql.run_async(f"SELECT pk FROM {ks}.t")
        missing = set(keys) - {r.pk for r in rows}
        assert not missing, f"{len(missing)} of {len(keys)} rows lost after the failed split bypass and a restart"
