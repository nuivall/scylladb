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

from test.cluster.object_store.test_backup import topo, create_cluster, take_snapshot, do_backup, check_mutation_replicas
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("fail_first", [False, True])
@pytest.mark.parametrize("racks", [1, 2])
async def test_restore_tablets_two_replicas_in_one_rack(build_mode: str, manager: ScyllaClusterManager, object_storage, fail_first: bool, racks: int):
    '''Tablet-aware restore marks each restored sstable `downloaded` in
    system_distributed.snapshot_sstables, keyed by (snapshot, dc, rack, first token,
    sstable id), and skips the marked ones. Two replicas of a tablet in one rack (RF
    above the rack count, allowed with rf_rack_valid_keyspaces off) share those rows:
    once one replica marks an sstable, the other skips it. With fail_first, one
    replica's download fails, the restore fails, and the retry, which the flag is
    there for, finds everything the other replica restored already marked. With two
    racks (the control), each replica has rows of its own.

    The failing replica is held before its downloads until the other one has marked
    every sstable. Without that, the failure can stop the other replica's downloads
    first, and the retry then downloads everything on both.'''
    topology = topo(rf=2, nodes=2, racks=racks, dcs=1)
    servers, host_ids = await create_cluster(topology, manager, logger, object_storage)
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()

    num_keys = 12
    tablet_count = 4
    ks_opts = f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {topology.rf}}}"
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test ( pk text primary key, value int ) WITH tablets = {{'min_tablet_count': {tablet_count}}};")
        insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, value) VALUES (?, ?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL
        await asyncio.gather(*(cql.run_async(insert_stmt, (str(i), i)) for i in range(num_keys)))
        snap_name, _ = await take_snapshot(ks, servers, manager, logger)
        await asyncio.gather(*(do_backup(s, snap_name, f'{s.server_id}/{snap_name}', ks, 'test', object_storage, manager, logger) for s in servers))

    if fail_first:
        await manager.api.enable_injection(servers[1].ip_addr, "pause_tablet_restore", one_shot=False)
        await manager.api.enable_injection(servers[1].ip_addr, "fail_download_sstable", one_shot=True)
    async with new_test_keyspace(manager, ks_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test ( pk text primary key, value int ) WITH tablets = {{'min_tablet_count': {tablet_count}, 'max_tablet_count': {tablet_count}}};")
        manifests = [f'{s.server_id}/{snap_name}/manifest.json' for s in servers]

        async def start_restore():
            return await manager.api.restore_tablets(servers[0].ip_addr, ks, 'test', snap_name, servers[0].datacenter,
                                                     object_storage.address, object_storage.bucket_name, manifests)

        async def restore():
            return await manager.api.wait_task(servers[0].ip_addr, await start_restore())

        async def first_replica_marked_all():
            rows = await cql.run_async(f"SELECT downloaded FROM system_distributed.snapshot_sstables WHERE "
                                       f"snapshot_name = '{snap_name}' AND \"keyspace\" = '{ks}' AND \"table\" = 'test' AND "
                                       f"datacenter = '{servers[0].datacenter}' AND rack = '{servers[0].rack}'")
            return True if rows and all(r.downloaded for r in rows) else None

        if fail_first:
            tid = await start_restore()
            await wait_for(first_replica_marked_all, time.time() + 50)
            await manager.api.message_injection(servers[1].ip_addr, "pause_tablet_restore")
            await manager.api.disable_injection(servers[1].ip_addr, "pause_tablet_restore")
            status = await manager.api.wait_task(servers[0].ip_addr, tid)
        else:
            status = await restore()
        if fail_first:
            assert status['state'] == 'failed', f"the injected download failure didn't fail the restore: {status}"
            status = await restore()
        assert status['state'] == 'done', f"restore failed: {status}"

        await check_mutation_replicas(cql, manager, servers, range(num_keys), topology, logger, ks, 'test')
