#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
import pytest
import logging
from cassandra.auth import PlainTextAuthProvider
from test.alternator.util import random_string
import concurrent

logger = logging.getLogger(__name__)

"""
Tests how cluster behaves when lost quorum. Ideally for operations with CL=1 live part of the
cluster should still work but that's guaranteed only if auth data is replicated everywhere.
"""
@pytest.mark.asyncio
@pytest.mark.xfail(reason="Auth implementation not resilient to avaliablity loss")
async def test_auth_no_quorum(manager: ManagerClient) -> None:
    config = {
        # disable auth cache
        'permissions_validity_in_ms': 0,
        'permissions_update_interval_in_ms': 0,
    }

    servers = [await manager.server_add(config=config) for _ in range(3)]

    cql = manager.get_cql()
    users = [random_string() for _ in range(100)]
    # for simplicity user and password are the same random string
    futures = [cql.execute_async(f"CREATE ROLE {user} WITH PASSWORD = '{user}' AND LOGIN = true;") for user in users]
    concurrent.futures.wait(futures)

    for i in range(2):
        await manager.server_stop_gracefully(servers[i].server_id)
   
    # this may still succeed if all users get stored on this node but it's unlikely
    for user in users:
        manager.auth_provider = PlainTextAuthProvider(username=user, password=user)
        await manager.driver_connect(server=servers[2])
