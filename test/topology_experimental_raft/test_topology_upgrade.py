#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import pytest
import time

from test.pylib.rest_client import HTTPError
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import log_run_time, wait_until_topology_upgrade_finishes, \
        check_system_topology_and_cdc_generations_v3_consistency

def mock_auth_data():
    return {
        "roles": [("user1", True, False, frozenset('users1'), "salt1")],
    }

def populate_auth_data(manager: ManagerClient):
    data = mock_auth_data()
    cql = manager.get_cql()
    stmt = cql.prepare("INSERT INTO system_auth.roles (role, can_login, is_superuser, member_of, salted_hash) VALUES (?, ?, ?, ?, ?)")
    for role in data["roles"]:
        cql.execute(stmt.bind(role))

def check_auth_v2_data_consistency(manager: ManagerClient):
    data = mock_auth_data()
    # default auto-added user
    data["roles"] += ("cassandra", True, True, None, None)
    cql = manager.get_cql()
    roles = cql.execute("SELECT * FROM system_auth_v2.roles").all()
    assert set(roles) == set(data["roles"])

@pytest.mark.asyncio
@log_run_time
async def test_topology_upgrade_basic(request, manager: ManagerClient):
    # First, force the first node to start in legacy mode due to the error injection
    cfg = {'error_injections_at_startup': ['force_gossip_based_join']}

    servers = [await manager.server_add(config=cfg)]
    # Disable injections for the subsequent nodes - they should fall back to
    # using gossiper-based node operations
    del cfg['error_injections_at_startup']

    servers += [await manager.server_add(config=cfg) for _ in range(2)]
    cql = manager.cql
    assert(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Checking the upgrade state on all nodes")
    for host in hosts:
        status = await manager.api.raft_topology_upgrade_status(host.address)
        assert status == "not_upgraded"

    populate_auth_data(manager)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Check that triggering upgrade is idempotent")
    await manager.api.upgrade_to_raft_topology(hosts[0].address)

    logging.info("Waiting until upgrade finishes")
    await asyncio.gather(*(wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60) for h in hosts))

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)
    logging.info("Checking consistency of data in system_auth_v2")
    check_auth_v2_data_consistency(manager)

    logging.info("Booting new node")
    await manager.server_add(config=cfg)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)
    logging.info("Checking consistency of data in system_auth_v2")
    check_auth_v2_data_consistency(manager)
