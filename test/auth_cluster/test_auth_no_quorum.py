#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import time
from test.pylib.manager_client import ManagerClient
import pytest
from cassandra.auth import PlainTextAuthProvider
from test.pylib.util import read_barrier, unique_name, wait_for_cql_and_get_hosts
from cassandra.query import SimpleStatement

"""
Tests how cluster behaves when lost quorum. Ideally for operations with CL=1 live part of the
cluster should still work but that's guaranteed only if auth data is replicated everywhere.
"""
@pytest.mark.asyncio
async def test_auth_no_quorum(manager: ManagerClient) -> None:
    config = {
        # disable auth cache
        'permissions_validity_in_ms': 0,
        'permissions_update_interval_in_ms': 0,
    }
    servers = await manager.servers_add(3, config=config)

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    await manager.servers_see_each_other(servers)

    # create users, this is done so that previous auth implementation (with RF=1) fails this test
    # otherwise it could happen that all users are luckily placed on a single node
    roles = ["r" + unique_name() for _ in range(10)]
    for role in roles:
        # if not exists due to https://github.com/scylladb/python-driver/issues/296
        cql.execute(f"CREATE ROLE IF NOT EXISTS {role} WITH PASSWORD = '{role}' AND LOGIN = true")

    # auth reads are eventually consistent so we need to for a sync on all nodes
    for server in servers:
        host = cql.cluster.metadata.get_host(server.ip_addr)
        await read_barrier(cql, host)
    # check if users are replicated everywhere
    for role in roles:
        for server in servers:
            await manager.driver_connect(server=server,
                auth_provider=PlainTextAuthProvider(username=role, password=role))
    # lost quorum
    for i in range(2):
        await manager.server_stop_gracefully(servers[i].server_id)
    alive_server = servers[2]
    # can still login on remaining node - whole auth data is local
    for role in roles:
        await manager.driver_connect(server=alive_server,
            auth_provider=PlainTextAuthProvider(username=role, password=role))
    names = [row.role for row in cql.execute(f"LIST ROLES", execution_profile='whitelist')]
    assert(set(names) == set(['cassandra'] + roles))
