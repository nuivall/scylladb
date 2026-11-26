#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import time

import pytest
from cassandra import InvalidRequest

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.cluster.util import get_topology_coordinator, trigger_snapshot
from test.pylib.rest_client import inject_error_one_shot, read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

DROP_APPENDS = "raft_drop_incoming_append_entries_for_specified_group"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.xfail(strict=True, reason="a group 0 snapshot updates service levels before the auth cache")
async def test_snapshot_updates_effective_service_levels(manager: ScyllaClusterManager) -> None:
    """A node that learns from a group 0 snapshot that a service level was
    attached to a role must give the role that service level.

    group0_state_machine::reload_state() recomputes the effective service
    levels before it reloads the auth cache (load_all()), and the effective
    service levels are computed from the auth cache's role records.
    """
    servers = await manager.servers_add(3, config=auth_config,
                                        cmdline=['--smp', '1', '--logger-log-level', 'raft_group_registry=debug'],
                                        auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    await cql.run_async("CREATE SERVICE LEVEL sl_snap WITH timeout = 7s")
    await cql.run_async("CREATE ROLE r_snap WITH PASSWORD = 'x' AND LOGIN = true")

    leader_id = await get_topology_coordinator(manager)
    host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
    leader = next(s for s in servers if host_ids[s.server_id] == leader_id)
    lagging = next(s for s in servers if s.server_id != leader.server_id)
    others = [s for s in servers if s.server_id != lagging.server_id]
    hosts = {s.server_id: h for s, h in zip(servers, await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60))}
    leader_host = hosts[leader.server_id]
    logger.info("leader %s, lagging %s", leader, lagging)

    await asyncio.gather(*(inject_error_one_shot(manager.api, s.ip_addr, "raft_server_set_snapshot_thresholds",
                                                 parameters={'snapshot_threshold': '0', 'snapshot_threshold_log_size': '0',
                                                             'snapshot_trailing': '0', 'snapshot_trailing_size': '0'})
                           for s in others))
    group0_id = (await cql.run_async(
        "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'", host=leader_host))[0].value
    log = await manager.server_open_log(lagging.server_id)
    mark = await log.mark()

    # The lagging node learns of the ATTACH only from a snapshot.
    await manager.api.enable_injection(lagging.ip_addr, DROP_APPENDS, one_shot=False, parameters={"value": group0_id})
    await cql.run_async("ATTACH SERVICE LEVEL sl_snap TO r_snap", host=leader_host)
    await log.wait_for(rf"Dropping append request .* for group {group0_id}", from_mark=mark, timeout=60)
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}",
                        host=leader_host)
    for i in range(3):
        await cql.run_async(f"CREATE TABLE ks.filler_{i} (pk int PRIMARY KEY)", host=leader_host)
    await trigger_snapshot(manager, leader)
    await manager.api.disable_injection(lagging.ip_addr, DROP_APPENDS)

    await log.wait_for(r"transfer snapshot from .* completed", from_mark=mark, timeout=120)
    await read_barrier(manager.api, lagging.ip_addr)
    assert not await log.grep("Loading role r_snap", from_mark=mark), "the ATTACH reached the node outside the snapshot"

    wrong = {}
    for s in servers:
        try:
            rows = await cql.run_async("LIST EFFECTIVE SERVICE LEVEL OF r_snap", host=hosts[s.server_id])
            values = {r.service_level_option: r.effective_service_level for r in rows}
        except InvalidRequest as e:
            values = {"error": str(e)}
        logger.info("%s: %s", s, values)
        if values.get("timeout") != "sl_snap":
            wrong[s.server_id] = values
    assert not wrong, f"r_snap lacks sl_snap on {wrong} (lagging: {lagging.server_id})"
