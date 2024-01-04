#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
import pytest
import logging
from cassandra.auth import PlainTextAuthProvider
from test.pylib.util import read_barrier, unique_name

logger = logging.getLogger(__name__)

"""
Tests how cluster behaves when lost quorum. Ideally for operations with CL=1 live part of the
cluster should still work but that's guaranteed only if auth data is replicated everywhere.
"""
@pytest.mark.asyncio
async def test_auth_no_quorum(manager: ManagerClient) -> None:
    pytest.skip("flaky: Failed to create connection pool for new host")
    config = {
        # disable auth cache
        'permissions_validity_in_ms': 0,
        'permissions_update_interval_in_ms': 0,
    }
    servers = [await manager.server_add(config=config) for _ in range(3)]
    manager.auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cql = manager.get_cql()
    # create users, this is done so that previous auth implementation (with RF=1) fails this test
    # otherwise it could happen that all users are luckily placed on a single node
    users = ["r" + unique_name() for _ in range(10)]
    # for simplicity user and password are the same random string
    for user in users:
        cql.execute(f"CREATE ROLE {user} WITH PASSWORD = '{user}' AND LOGIN = true")
    try:
        # auth reads are eventually consistent so we need to for a sync on all nodes
        for i in range(3):
            host = cql.cluster.metadata.get_host(servers[i].ip_addr)
            await read_barrier(cql, host)
        # check if users are replicated everywhere
        for user in users:
            manager.auth_provider = PlainTextAuthProvider(username=user, password=user)
            for i in range(3):
                await manager.driver_connect(server=servers[i])
        # lost quorum
        for i in range(2):
            await manager.server_stop_gracefully(servers[i].server_id)
        for user in users:
            print("USR", user)
            manager.auth_provider = PlainTextAuthProvider(username=user, password=user)
            await manager.driver_connect(server=servers[2])
    finally:
        manager.auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        for i in range(1):
            await manager.server_start(servers[i].server_id)
        # quorum is back
        for user in users:
            cql.execute(f"DROP ROLE {user}")
