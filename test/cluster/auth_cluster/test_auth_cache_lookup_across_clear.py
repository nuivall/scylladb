#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging

import pytest
from cassandra import Unauthorized
from cassandra.auth import PlainTextAuthProvider

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

PAUSE = "auth_cache_load_permissions_pause"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("grant_during_lookup", [True, False])
async def test_permission_lookup_across_a_grant(manager: ScyllaClusterManager, grant_during_lookup: bool) -> None:
    """A permission lookup of role R stores the set it loaded into R's cache record
    (cache::load_permissions()). When a GRANT to R's parent role clears the cached
    permissions of R (load_roles() clears the inheriting roles) while the lookup is
    loading, the lookup stores the set it loaded before the GRANT, and R stays refused
    until something clears R's permissions again (SCYLLADB-4206). The lookup is held
    after loading by a test-only pause. Without the GRANT during the lookup (the
    control), R is allowed after the GRANT.
    """
    server = await manager.server_add(config=auth_config, cmdline=['--smp', '1'])
    cql, _ = await manager.get_ready_cql([server])

    await cql.run_async("CREATE ROLE parent")
    await cql.run_async("CREATE ROLE member WITH PASSWORD = 'x' AND LOGIN = true")
    await cql.run_async("GRANT parent TO member")
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

    member = await manager.get_cql_exclusive(server, auth_provider=PlainTextAuthProvider(username="member", password="x"))

    async def select():
        try:
            await member.run_async("SELECT * FROM ks.t")
            return True
        except Unauthorized:
            return False

    if grant_during_lookup:
        await manager.api.enable_injection(server.ip_addr, PAUSE, one_shot=True, parameters={"role": "member"})
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()
        lookup = asyncio.create_task(select())
        await log.wait_for(f"{PAUSE}: waiting", from_mark=mark, timeout=60)
        await cql.run_async("GRANT SELECT ON ks.t TO parent")
        await manager.api.message_injection(server.ip_addr, PAUSE)
        logger.info("the held lookup returned allowed=%s", await lookup)
    else:
        assert not await select()
        await cql.run_async("GRANT SELECT ON ks.t TO parent")

    await asyncio.sleep(1)
    allowed = await select()
    logger.info("grant_during_lookup=%s: member allowed after the GRANT: %s", grant_during_lookup, allowed)
    assert allowed, "member is still refused SELECT after its parent was granted it"
