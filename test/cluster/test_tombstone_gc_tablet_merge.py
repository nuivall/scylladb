#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
import struct
import time

import pytest
from cassandra.murmur3 import murmur3
from cassandra.query import ConsistencyLevel, SimpleStatement

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.tablets import get_tablet_count
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)


def token_of(pk: int) -> int:
    """The Murmur3 token of an int partition key."""
    return murmur3(struct.pack('>i', pk))


async def keys_in_two_tablets(cql, ks: str) -> tuple[int, int]:
    """Returns a key of the first tablet and a key of the second one, of a table with two tablets."""
    last_tokens = sorted(r.last_token for r in await cql.run_async(
        f"SELECT last_token FROM system.tablets WHERE keyspace_name = '{ks}' AND table_name = 't' ALLOW FILTERING"))
    assert len(last_tokens) == 2
    key_l = next(pk for pk in range(1000) if token_of(pk) <= last_tokens[0])
    key_r = next(pk for pk in range(1000) if token_of(pk) > last_tokens[0])
    return key_l, key_r


async def do_test_tombstone_gc_after_tablet_merge(manager: ScyllaClusterManager, merge: bool) -> None:
    cfg = {'hinted_handoff_enabled': False}
    servers = await manager.servers_add(2, config=cfg, auto_rack_dc='dc1')
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} "
                                          "AND tablets = {'initial': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int) WITH "
                            "tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '0'} "
                            "AND compaction = {'class': 'NullCompactionStrategy'}")
        key_l, key_r = await keys_in_two_tablets(cql, ks)
        logger.info("key of the first tablet: %s, of the second: %s", key_l, key_r)
        a, b = servers

        # The row reaches only a.
        await manager.server_stop_gracefully(b.server_id)
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.t (pk, v) VALUES ({key_l}, 1)",
                                            consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(b.server_id)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        # Its deletion reaches only b.
        await manager.server_stop_gracefully(a.server_id)
        await cql.run_async(SimpleStatement(f"DELETE FROM {ks}.t WHERE pk = {key_l}",
                                            consistency_level=ConsistencyLevel.ONE))
        await manager.server_start(a.server_id)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        # The first tablet is never repaired; the second one is, after the deletion.
        await asyncio.sleep(2)
        await manager.api.tablet_repair(a.ip_addr, ks, "t", token_of(key_r))

        if merge:
            for s in servers:
                await manager.api.enable_injection(s.ip_addr, "tablet_force_tablet_count_decrease", one_shot=False)
            async def merged():
                return True if await get_tablet_count(manager, a, ks, "t") == 1 else None
            await wait_for(merged, time.time() + 120)
            for s in servers:
                await manager.api.disable_injection(s.ip_addr, "tablet_force_tablet_count_decrease")

        # b compacts its tombstone away if it is GC-eligible.
        await manager.api.keyspace_flush(b.ip_addr, ks, "t")
        await manager.api.keyspace_compaction(b.ip_addr, ks, "t")

        rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.t WHERE pk = {key_l}",
                                                   consistency_level=ConsistencyLevel.ALL))
        assert rows == [], f"deleted row came back: {rows}"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tombstone_gc_after_tablet_merge(manager: ScyllaClusterManager) -> None:
    """Verify that a tablet merge doesn't make a tombstone GC-eligible before a repair synced it.

    With tombstone_gc = {'mode': 'repair'}, a tombstone may be purged once a repair which started
    after it was written has completed for its range. merge_tablet_info() gives the merged tablet
    the later repair time of the two siblings, and the tombstone GC state takes it for the whole
    merged range. A tombstone written to the sibling which was repaired earlier, after its last
    repair, then becomes GC-eligible although no repair synced it. A replica which has only the
    tombstone purges it, and the row the tombstone deleted, which another replica still has, comes
    back.

    Reproduces SCYLLADB-6091, found independently by the TLA+ model in
    docs/dev/tla/tombstone_gc_repair.
    """
    await do_test_tombstone_gc_after_tablet_merge(manager, merge=True)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tombstone_gc_without_tablet_merge(manager: ScyllaClusterManager) -> None:
    """The same steps without the merge: the tombstone isn't GC-eligible, and the row stays deleted."""
    await do_test_tombstone_gc_after_tablet_merge(manager, merge=False)
