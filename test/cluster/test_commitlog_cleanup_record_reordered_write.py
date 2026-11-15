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

from test.cluster.util import new_test_keyspace, reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cleanup_record_keeps_a_write_applied_after_a_later_one(manager: ScyllaClusterManager) -> None:
    """Verify that a commitlog cleanup record doesn't cover a write still only in a memtable.

    A write takes its replay position when the commitlog adds it, and reaches the memtable after
    that (database::do_apply()); a later write can get there first. A flush of the memtable then
    raises the group's flushed position (compaction_group::_highest_flushed_rp) to the later
    write's position, above the earlier write which lands in the next memtable. The cleanup record
    a tablet repair saves at that position makes the commitlog replay skip the earlier write, and
    an unclean restart loses it. Found by the TLA+ spec in docs/dev/tla/commitlog_cleanup.
    """
    config = {'commitlog_sync': 'batch'}
    property_files = [{"dc": "dc1", "rack": f"r{i}"} for i in range(2)]
    servers = await manager.servers_add(2, config=config, cmdline=['--smp', '1'], property_file=property_files)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
        table_id = await manager.get_table_id(ks, "test")
        token = (await get_all_tablet_replicas(manager, servers[0], ks, "test"))[0].last_token

        async def insert(pk: int) -> None:
            await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, c) VALUES ({pk}, {pk})",
                                                consistency_level=ConsistencyLevel.ALL))

        for pk in range(10):
            await insert(pk)

        # Hold the write on both replicas, so that the repair finds it in neither memtable and
        # doesn't copy it into an sstable.
        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await log.mark() for log in logs]
        for s in servers:
            await manager.api.enable_injection(s.ip_addr, "database_apply_after_commitlog_add", one_shot=True,
                                               parameters={"ks_name": ks, "cf_name": "test"})
        victim = asyncio.create_task(insert(100))
        for log, mark in zip(logs, marks):
            await log.wait_for("database_apply_after_commitlog_add: waiting", from_mark=mark, timeout=60)

        # A later write, which reaches the memtable first, then a repair, which flushes it and
        # saves the cleanup record.
        await insert(101)
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

        async def record_saved(h):
            rows = await cql.run_async("SELECT table_uuid, end_token_inclusive FROM system.commitlog_cleanups", host=h)
            return True if any(str(r.table_uuid) == str(table_id) and r.end_token_inclusive == token for r in rows) else None
        # With the record kept below the unflushed writes, it may cover nothing and not be
        # saved at all (records below the commitlog's minimum position are pruned); the held
        # write must be released before its CL=ALL timeout, so wait for the records briefly.
        deadline = time.time() + 10
        for h in hosts:
            try:
                await wait_for(lambda: record_saved(h), deadline)
            except Exception as e:
                logger.info("no cleanup record on %s: %s", h, e)

        for s in servers:
            await manager.api.message_injection(s.ip_addr, "database_apply_after_commitlog_add")
        await victim  # acknowledged at CL=ALL

        async def keys(h) -> set[int]:
            rows = await cql.run_async(f"SELECT pk, mutation_fragment_kind FROM MUTATION_FRAGMENTS({ks}.test)", host=h)
            return {r.pk for r in rows if r.mutation_fragment_kind == "clustering row"}
        for h in hosts:
            assert {100, 101} <= await keys(h)

        for s in servers:
            await manager.server_stop(s.server_id, convict=False)
        for s in servers:
            await manager.server_start(s.server_id)
        cql = await reconnect_driver(manager)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 120)

        lost_on = [h for h in hosts if 100 not in await keys(h)]
        for h in hosts:
            assert 101 in await keys(h)
        assert not lost_on, f"the write acknowledged at CL=ALL was lost on commitlog replay on {len(lost_on)} of {len(hosts)} replicas"
