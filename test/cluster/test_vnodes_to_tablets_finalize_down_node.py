#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
from contextlib import suppress

import pytest
from cassandra.query import ConsistencyLevel

from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import create_new_test_keyspace, reconnect_driver, ensure_group0_leader_on
from test.cluster.test_vnodes_to_tablets_migration import verify_data_integrity, new_migrating_test_keyspace

logger = logging.getLogger(__name__)

REFRESHED = "Refreshed table load stats for all DC"
DROP_APPENDS = "raft_drop_incoming_append_entries_for_specified_group"
# Refusals that mean the scenario was not set up, not that finalization is fixed.
WINDOW_MISSED = ["No load stats available", "has intended storage mode", "has not yet migrated"]


@pytest.mark.parametrize("scenario", [
    # The bug: X went back to vnodes, its re-mark for tablets was committed but never
    # applied on X, and X died; the coordinator still holds X's stats from its tablet phase.
    pytest.param("downgraded_down", marks=pytest.mark.xfail(reason="finalization trusts a down node's cached stats; fixed by PR #31829", strict=True)),
    # Control: X stays tablet-based and is down at finalization; its cached stats are right.
    "tablets_down",
    # Control: as the bug, but X stays up, so finalization refreshes its stats.
    "downgraded_up",
])
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_finalize_with_down_node_on_stale_load_stats(manager: ScyllaClusterManager, scenario: str):
    """Reproducer for FINDINGS.md bug #7.

    Forward finalization checks a down node against the coordinator's cached load stats
    (collect_tablet_load_stats(require_live_nodes::no)), and skips the published-mode check
    for a node that published none (TOPOLOGY_CURRENT_STORAGE_MODE is suppressed here, as in
    a cluster mid-upgrade). A node X that went back to vnodes after its stats were cached as
    tablets, and is down at finalization, is taken as tablet-based: the keyspace switches to
    tablets while X's sstables are split by vnodes, and X can't load them.

    The load stats refresh interval is an hour, so the only refreshes are the ones the
    coordinator starts with and the ones this test forces (CREATE TABLE) - and finalization's.
    Every node must intend tablets for forward finalization, so X is marked for tablets again
    after its downgrade; that mark must not reach X's local system.topology, or X boots
    forward and reshards by tablet (populate_keyspace() reads local state). So X drops group0
    appends while it marks itself and is killed once the leader has committed the mark.

    Correct behaviour: finalization is refused, or X boots (twice: the first boot reads the
    pre-finalization schema) with all its data.
    """
    num_keys = 1000
    written = num_keys // 2  # keys 0..written-1 are in the table
    cfg = {
        'tablet_load_stats_refresh_interval_in_seconds': 3600,
        'num_tokens': 16,
        'error_injections_at_startup': [{'name': 'suppress_features', 'value': 'TOPOLOGY_CURRENT_STORAGE_MODE'}],
    }
    cmdline = ['--smp', '2', '--logger-log-level', 'raft_topology=debug']
    servers = await manager.servers_add(3, cmdline=cmdline, config=cfg)
    a, b, x = servers
    host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
    x_host = host_ids[x.server_id]
    cql, _ = await manager.get_ready_cql(servers)

    async def node_modes() -> dict:
        status = await manager.api.get_vnode_tablet_migration_status(a.ip_addr, ks)
        return {n['host_id']: (n['current_mode'], n['intended_mode']) for n in status['nodes']}

    async def wait_for_x_intended_tablets():
        for _ in range(120):
            if (await node_modes())[x_host][1] == 'tablets':
                return
            await asyncio.sleep(0.5)
        assert False, f"X's mark for tablets never committed: {await node_modes()}"

    async def insert(start: int, end: int):
        stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
        stmt.consistency_level = ConsistencyLevel.ONE
        await asyncio.gather(*(cql.run_async(stmt, [k, k]) for k in range(start, end)))
        for s in await manager.running_servers():
            await manager.api.keyspace_flush(s.ip_addr, ks, "test")

    async def start_x(what: str):
        nonlocal cql
        try:
            await manager.server_start(x.server_id)
        except Exception as e:
            log = await manager.server_open_log(x.server_id)
            hits = await log.grep(r"Unable to load .* that belongs to tablets|on_internal_error|Exception while populating keyspace")
            pytest.fail(f"bug #7: {what} boot of X failed after finalization ({e}); log: {[h[0] for h in hits[:3]]}")
        await reconnect_driver(manager)
        cql, _ = await manager.get_ready_cql(await manager.running_servers())
        await read_barrier(manager.api, x.ip_addr)
        await verify_data_integrity(cql, ks, "test", written, cl=ConsistencyLevel.ONE)

    opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}"
    async with new_migrating_test_keyspace(manager, opts) as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH compaction = {{'class': 'IncrementalCompactionStrategy', 'enabled': false}}")
        await insert(0, num_keys // 2)

        await manager.api.create_vnode_tablet_migration(a.ip_addr, ks)
        for s in [b, a, x]:
            await manager.api.upgrade_node_to_tablets(s.ip_addr)
            await manager.server_restart(s.server_id)
            await reconnect_driver(manager)
            cql, _ = await manager.get_ready_cql(servers)

        # The cache lives in the coordinator; keep it on A, which never restarts again.
        await ensure_group0_leader_on(manager, a)
        log_a = await manager.server_open_log(a.server_id)
        await manager.server_sees_other_server(a.ip_addr, x.ip_addr)

        logger.info("Forcing a load stats refresh with every node tablet-based")
        mark = await log_a.mark()
        aux_ks = await create_new_test_keyspace(cql, opts)
        await cql.run_async(f"CREATE TABLE {aux_ks}.t (pk int PRIMARY KEY)")
        await log_a.wait_for(REFRESHED, from_mark=mark, timeout=60)
        modes = await node_modes()
        assert modes == {h: ('tablets', 'tablets') for h in host_ids.values()}, f"refresh did not see all nodes on tablets: {modes}"

        window = await log_a.mark()
        if scenario == "tablets_down":
            await manager.server_stop(x.server_id, convict=True)
            await manager.others_not_see_server(x.ip_addr)
        else:
            logger.info("Downgrading X to vnodes and restarting it")
            await manager.api.downgrade_node_to_vnodes(x.ip_addr)
            await manager.server_restart(x.server_id)
            await reconnect_driver(manager)
            cql, _ = await manager.get_ready_cql(servers)
            # New vnode-split sstables on X, sure to span several of its tablets.
            await insert(num_keys // 2, num_keys)
            written = num_keys

            logger.info("Marking X for tablets again, without letting X apply the mark")
            group0_id = (await cql.run_async(
                "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'",
                host=cql.cluster.metadata.get_host(a.ip_addr)))[0].value
            await manager.api.enable_injection(x.ip_addr, DROP_APPENDS, one_shot=False, parameters={"value": group0_id})
            mark_task = asyncio.create_task(manager.api.upgrade_node_to_tablets(x.ip_addr))
            await wait_for_x_intended_tablets()
            # The mark is committed, but X's own system.topology must still say vnodes:
            # boot replays only X's local group0 log, so that is what X boots with.
            rows = await cql.run_async("SELECT host_id, intended_storage_mode FROM system.topology WHERE key = 'topology'",
                                       host=cql.cluster.metadata.get_host(x.ip_addr))
            local = {str(r.host_id): r.intended_storage_mode for r in rows}
            assert str(local.get(str(x_host))) == 'vnodes', f"X applied its own mark: {local}"
            assert not mark_task.done(), "X's mark returned, so X applied it"
            if scenario == "downgraded_down":
                await manager.server_stop(x.server_id, convict=True)
                with suppress(Exception):
                    await mark_task
                await manager.others_not_see_server(x.ip_addr)
            else:
                await manager.api.disable_injection(x.ip_addr, DROP_APPENDS)
                await mark_task

        logger.info("Checking the preconditions of finalization")
        await read_barrier(manager.api, a.ip_addr)
        leader = await manager.api.get_raft_leader(a.ip_addr)
        assert str(leader) == str(host_ids[a.server_id]), f"coordinator moved off A ({leader}), its load stats cache is gone"
        modes = await node_modes()
        assert all(m[1] == 'tablets' for m in modes.values()), f"not every node intends tablets: {modes}"
        if scenario != "downgraded_up":
            # X's entry in system.tablet_sizes is what the coordinator last saw of it.
            assert modes[x_host][0] == 'tablets', f"X's cached stats aren't tablet-based: {modes}"
            refreshes = await log_a.grep(REFRESHED, from_mark=window)
            assert not refreshes, f"load stats were refreshed while X was in transition: {refreshes}"

        logger.info("Finalizing")
        refused = None
        try:
            await manager.api.finalize_vnode_tablet_migration(a.ip_addr, ks)
        except HTTPError as e:
            refused = str(e)
        logger.info(f"Finalization {'refused: ' + refused if refused else 'succeeded'}")

        if scenario == "downgraded_up":
            assert refused and "has not yet migrated" in refused, f"expected a refusal citing X, got {refused}"
            await verify_data_integrity(cql, ks, "test", written, cl=ConsistencyLevel.ONE)
            return

        if refused:
            assert not any(m in refused for m in WINDOW_MISSED), f"refused for a precondition, not the down node: {refused}"
        await start_x("first")
        if not refused:
            # The first boot read the vnode keyspace from X's local schema; this one reads tablets.
            await manager.server_stop_gracefully(x.server_id)
            await start_x("second")
