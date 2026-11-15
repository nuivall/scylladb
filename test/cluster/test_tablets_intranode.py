#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from cassandra.cluster import Session, ConsistencyLevel

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts, start_writes
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas
from test.cluster.util import new_test_keyspace, FeatureConfig, get_topology_coordinator, reconnect_driver


import pytest
import asyncio
import logging
import time
import os
import threading


logger = logging.getLogger(__name__)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_intranode_migration(manager: ScyllaClusterManager, storage_config: FeatureConfig):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'stream_session=trace',
        '--logger-log-level', 'tablets=trace',
        '--logger-log-level', 'database=trace',
    ]
    cfg = storage_config.get_cluster_cfg()
    servers = [await manager.server_add(config=cfg, cmdline=cmdline)]

    await manager.disable_tablet_balancing()

    cql = manager.get_cql()
    keyspace_opts = storage_config.get_keyspace_opts(
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}")
    async with new_test_keyspace(manager, keyspace_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        finish_writes = await start_writes(cql, ks, "test")

        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        src_shard = replica[1]
        dst_shard = src_shard ^ 1

        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], src_shard, replica[0], dst_shard, tablet_token)

        key_count = await finish_writes()

        rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
        assert len(rows) == key_count
        for r in rows:
            assert r.c == r.pk


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_crash_during_intranode_migration(manager: ScyllaClusterManager, storage_config: FeatureConfig):
    cmdline = [
        '--logger-log-level', 'tablets=trace',
        '--logger-log-level', 'database=trace',
        '--commitlog-sync', 'batch', # So that ACKed writes are not lost on crash
    ]
    cfg = storage_config.get_cluster_cfg()
    servers = [await manager.server_add(config=cfg, cmdline=cmdline)]

    await manager.disable_tablet_balancing()

    cql = manager.get_cql()

    keyspace_opts = storage_config.get_keyspace_opts(
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 4}")
    async with new_test_keyspace(manager, keyspace_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        finish_writes = await start_writes(cql, ks, "test", ignore_errors=True)

        tablet_token = 0 # Choose one tablet, any of them
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)

        src_shard = replica[1]
        dst_shard = src_shard ^ 1

        await manager.api.enable_injection(servers[0].ip_addr, 'crash-in-tablet-write-both-read-new', one_shot=True)

        migration_task = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, "test",
                                                    replica[0], src_shard, replica[0], dst_shard, tablet_token))

        s0_logs = await manager.server_open_log(servers[0].server_id)
        await s0_logs.wait_for('crash-in-tablet-write-both-read-new hit')
        await manager.server_stop(servers[0].server_id, convict=False)
        await manager.server_start(servers[0].server_id)
        await wait_for_cql_and_get_hosts(manager.cql, servers, time.time() + 60)

        # Wait for the tablet migration to finish
        await manager.api.quiesce_topology(servers[0].ip_addr)

        try:
            await migration_task
        except:
            pass

        key_count = await finish_writes()

        rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
        assert len(rows) == key_count
        for r in rows:
            assert r.c == r.pk


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cross_shard_migration(manager: ScyllaClusterManager, storage_config: FeatureConfig):
    """
    Test scenario where writes are concurrently made with migration, where
    some of them are coordinated by the owning host and some by the non-owning host.

    This reproduces the following problem with sharder logic:
      1) node A: tablet is in stage write_both_read_new (replicas: B:1 -> A:1)
      2) node B: tablet is in stage write_both_read_new
      3) node A: tablet is in stage use_new
      4) node B: coordinate write to node A (since stage is still "write both" here)
      5) node A: receive write request, sharder thinks no shard owns the tablet, fails the write

    In this scenario, sharder on node A should still return shard 1.
    """

    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'tablets=trace',
        '--logger-log-level', 'database=trace',
    ]

    cfg = storage_config.get_cluster_cfg()
    servers = await manager.servers_add(2, config=cfg, cmdline=cmdline)

    await manager.disable_tablet_balancing()

    cql = manager.get_cql()
    keyspace_opts = storage_config.get_keyspace_opts(
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 2}")
    async with new_test_keyspace(manager, keyspace_opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        finish_writes = await start_writes(cql, ks, "test")

        tablet0_token = -1
        tablet1_token = 1
        replica0 = await get_tablet_replica(manager, servers[0], ks, 'test', tablet0_token)
        replica1 = await get_tablet_replica(manager, servers[0], ks, 'test', tablet1_token)

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        # Place tablets on non-zero shards so that defaulted shard (0) is never the right shard.
        # This is to catch the problem when sharder (incorrectly) thinks that tablet does not have
        # any replica on the current host and assigns shard 0 to it in shard_for_read().
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica0[0], replica0[1], s0_host_id, 1, tablet0_token)
        await manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica1[0], replica1[1], s1_host_id, 1, tablet1_token)

        # Put whole token ring into migration so that all requests hit the migration path. Half of them
        # will be coordinated by the owning host, half will be coordinated by the non-owning host.
        migration0 = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, "test",
                                                                s0_host_id, 1, s1_host_id, 1, tablet0_token))
        migration1 = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, "test",
                                                                s1_host_id, 1, s0_host_id, 1, tablet1_token))

        await migration0
        await migration1

        key_count = await finish_writes()

        rows = await cql.run_async(f"SELECT * FROM {ks}.test;")
        assert len(rows) == key_count
        for r in rows:
            assert r.c == r.pk


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("restart", [False, True])
async def test_restart_after_intranode_streaming_keeps_data(manager: ScyllaClusterManager, restart: bool):
    """Verify that an intra-node migration doesn't lose the tablet's data if the node restarts after
    streaming, before the coordinator moves the tablet to write_both_read_new.

    The streaming loads the sstables of the tablet into the pending shard and returns; the coordinator
    moves the tablet to write_both_read_new in a later iteration. A node that boots in the streaming
    stage loads every sstable of the tablet into the shard serving reads, i.e. the leaving one, so the
    pending shard comes up empty. If the coordinator then moves on with the response it got before the
    restart, reads go to the empty pending shard and cleanup deletes the leaving shard's sstables.

    The test holds the coordinator between the response and the stage change, restarts the replica
    there (or, as a control, doesn't), lets the migration finish and reads every row back.
    """
    cmdline = ['--logger-log-level', 'raft_topology=debug']
    servers = await manager.servers_add(2, cmdline=cmdline)
    await manager.disable_tablet_balancing()
    cql = manager.get_cql()

    coordinator = await manager.find_server_by_host_id(servers, await get_topology_coordinator(manager))
    replica_server = next(s for s in servers if s.server_id != coordinator.server_id)
    replica_host = await manager.get_host_id(replica_server.server_id)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                          "AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        token = 0  # Any: there is one tablet.
        host, shard = await get_tablet_replica(manager, coordinator, ks, 't', token)
        if host != replica_host:
            await manager.api.move_tablet(coordinator.ip_addr, ks, 't', host, shard, replica_host, 0, token)
            shard = 0

        keys = range(100)
        for pk in keys:
            await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({pk}, {pk})")
        # In sstables, so that commitlog replay at the restart doesn't bring the rows back.
        await manager.api.keyspace_flush(replica_server.ip_addr, ks, 't')

        replica_log = await manager.server_open_log(replica_server.server_id)
        coordinator_log = await manager.server_open_log(coordinator.server_id)
        mark = await replica_log.mark()
        coordinator_mark = await coordinator_log.mark()
        await manager.api.enable_injection(replica_server.ip_addr, "intranode_migration_streaming_done_wait", one_shot=True)
        migration = asyncio.create_task(manager.api.move_tablet(coordinator.ip_addr, ks, 't',
                                                                replica_host, shard, replica_host, shard ^ 1, token))

        # The pending shard has the sstables; hold the coordinator, then let the streaming respond.
        await replica_log.wait_for("intranode_migration_streaming_done_wait: waiting", from_mark=mark, timeout=60)
        await manager.api.enable_injection(coordinator.ip_addr, "stream_tablet_wait", one_shot=False)
        await manager.api.message_injection(replica_server.ip_addr, "intranode_migration_streaming_done_wait")
        await coordinator_log.wait_for("streaming for tablet .* resolved", from_mark=coordinator_mark, timeout=60)

        if restart:
            await manager.server_stop_gracefully(replica_server.server_id)
            await manager.server_start(replica_server.server_id)
        await manager.api.disable_injection(coordinator.ip_addr, "stream_tablet_wait")
        await migration

        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        rows = await cql.run_async(f"SELECT pk FROM {ks}.t")
        missing = set(keys) - {r.pk for r in rows}
        assert not missing, f"{len(missing)} of {len(keys)} rows lost by the intra-node migration across a restart"
