#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import time

import pytest
from cassandra import Unauthorized
from cassandra.auth import PlainTextAuthProvider
from cassandra.metadata import Murmur3Token

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.cluster.util import get_topology_coordinator, trigger_snapshot
from test.pylib.rest_client import inject_error_one_shot, read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

INJECTION = "auth_cache_load_all_pause"
DROP_APPENDS = "raft_drop_incoming_append_entries_for_specified_group"


def _token(name: str) -> int:
    return Murmur3Token.from_key(name.encode()).value


def _role_names() -> tuple[str, str]:
    """A parent and a member role whose system.roles partitions load_all()
    reads member first (it pages through the table in token order)."""
    for i in range(100):
        parent, member = f"parent_{i}", f"member_{i}"
        if _token(member) < _token(parent):
            return parent, member
    raise RuntimeError("no suitable role names")


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.xfail(strict=True, reason="load_all() keeps permissions computed from the old role records")
async def test_load_all_keeps_permissions_of_revoked_superuser(manager: ScyllaClusterManager) -> None:
    """A node that learns from a group 0 snapshot that a role lost superuser
    can keep granting that role's members everything.

    cache::load_all() replaces the role records one at a time and clears no
    cached permissions afterwards. A lookup for the member, after its record
    was replaced and before the parent's was, reads the parent's old
    superuser flag from the cache records and stores ALL into the member's
    new record, where it stays.
    """
    servers = await manager.servers_add(3, config=auth_config, cmdline=['--smp', '1', '--logger-log-level', 'raft_group_registry=debug'], auto_rack_dc="dc1")
    cql, _ = await manager.get_ready_cql(servers)

    parent, member = _role_names()
    await cql.run_async(f"CREATE ROLE {parent} WITH SUPERUSER = true")
    await cql.run_async(f"CREATE ROLE {member} WITH PASSWORD = 'x' AND LOGIN = true")
    await cql.run_async(f"GRANT {parent} TO {member}")
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    # The node that will catch up by a snapshot: not the group 0 leader.
    leader_id = await get_topology_coordinator(manager)
    host_ids = {s.server_id: await manager.get_host_id(s.server_id) for s in servers}
    leader = next(s for s in servers if host_ids[s.server_id] == leader_id)
    lagging = next(s for s in servers if s.server_id != leader.server_id)
    others = [s for s in servers if s.server_id != lagging.server_id]
    logger.info("leader %s, lagging %s, roles %s %s", leader, lagging, parent, member)

    await asyncio.gather(*(inject_error_one_shot(manager.api, s.ip_addr, "raft_server_set_snapshot_thresholds",
                                                 parameters={'snapshot_threshold': '0', 'snapshot_threshold_log_size': '0',
                                                             'snapshot_trailing': '0', 'snapshot_trailing_size': '0'})
                           for s in others))
    await manager.api.enable_injection(lagging.ip_addr, INJECTION, one_shot=False, parameters={"role": member})
    log = await manager.server_open_log(lagging.server_id)
    mark = await log.mark()

    leader_host = (await wait_for_cql_and_get_hosts(cql, [leader], time.time() + 60))[0]
    group0_id = (await cql.run_async(
        "SELECT value FROM system.scylla_local WHERE key = 'raft_group0_id'", host=leader_host))[0].value
    # The lagging node drops group 0's appends: it learns of the commands
    # below only from the snapshot the leader sends once its log is truncated.
    # (Pausing the process isn't enough: appends sent meanwhile wait in its
    # socket buffers and get applied before the snapshot.)
    await manager.api.enable_injection(lagging.ip_addr, DROP_APPENDS, one_shot=False,
                                       parameters={"value": group0_id})
    await cql.run_async(f"ALTER ROLE {parent} WITH SUPERUSER = false", host=leader_host)
    await log.wait_for(rf"Dropping append request .* for group {group0_id}", from_mark=mark, timeout=60)
    for i in range(3):
        await cql.run_async(f"CREATE TABLE ks.filler_{i} (pk int PRIMARY KEY)", host=leader_host)
    await trigger_snapshot(manager, leader)
    await manager.api.disable_injection(lagging.ip_addr, DROP_APPENDS)

    # load_all() on the lagging node has replaced the member's record, not yet the parent's.
    await log.wait_for(f"load_all: replaced role {member}, pausing", from_mark=mark, timeout=120)
    # The parent's change reached the node only through the snapshot.
    assert not await log.grep(f"Loading role {parent}", from_mark=mark)
    member_auth = PlainTextAuthProvider(username=member, password='x')
    during = await manager.get_cql_exclusive(lagging, auth_provider=member_auth)
    await during.run_async("SELECT * FROM ks.t")
    await manager.api.message_injection(lagging.ip_addr, INJECTION)
    await manager.api.disable_injection(lagging.ip_addr, INJECTION)
    await read_barrier(manager.api, lagging.ip_addr)

    # The tables say the member has no permission; every node must refuse it.
    granting = []
    for s in servers:
        session = await manager.get_cql_exclusive(s, auth_provider=member_auth)
        await wait_for_cql_and_get_hosts(session, [s], time.time() + 60)
        try:
            await session.run_async("SELECT * FROM ks.t")
            granting.append(s)
        except Unauthorized:
            pass
    assert not granting, f"{member} still has its revoked superuser parent's permissions on {granting} (lagging: {lagging})"
