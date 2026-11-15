#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import os
import shutil

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name

logger = logging.getLogger(__name__)

FAIL = "load_and_stream_fail_after_a_batch"


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("fail", [True, False])
async def test_failed_refresh_keeps_the_unstreamed_rows(manager: ScyllaClusterManager, fail: bool) -> None:
    """Tablet load-and-stream (nodetool refresh --load-and-stream) streams an sstable
    spanning several tablets once per tablet and, after each successful batch, marks it
    for deletion, to unlink the marked sstables after the last tablet
    (tablet_sstable_streamer::stream(), defer_unlinking::yes). When a later tablet's
    stream fails, stream() throws before that, but an sstable marked for deletion is
    unlinked anyway when its last reference goes (sstable::close_files()): the upload
    directory loses it, and a retried refresh can't load the rows of the tablets that
    weren't streamed. Without the failure (the control) every row is loaded.
    """
    server = await manager.server_add(cmdline=['--smp', '1'])
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        # One tablet in the source table, so its sstable spans every tablet of the target.
        await cql.run_async(f"CREATE TABLE {ks}.src (pk int PRIMARY KEY, c int) WITH tablets = {{'min_tablet_count': 1, 'max_tablet_count': 1}}")
        await cql.run_async(f"CREATE TABLE {ks}.dst (pk int PRIMARY KEY, c int) WITH tablets = {{'min_tablet_count': 4, 'max_tablet_count': 4}}")
        keys = range(200)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.src (pk, c) VALUES ({k}, {k})") for k in keys])
        await manager.api.keyspace_flush(server.ip_addr, ks, "src")
        snap = unique_name("snap_")
        await manager.api.take_snapshot(server.ip_addr, ks, snap)

        workdir = await manager.server_get_workdir(server.server_id)
        ks_dir = f"{workdir}/data/{ks}"
        src_dir = next(os.path.join(ks_dir, d) for d in os.listdir(ks_dir) if d.startswith("src-"))
        dst_dir = next(os.path.join(ks_dir, d) for d in os.listdir(ks_dir) if d.startswith("dst-"))
        upload = os.path.join(dst_dir, "upload")
        os.makedirs(upload, exist_ok=True)
        snap_dir = os.path.join(src_dir, "snapshots", snap)
        for item in os.listdir(snap_dir):
            if item not in ("manifest.json", "schema.cql"):
                shutil.copy2(os.path.join(snap_dir, item), os.path.join(upload, item))
        logger.info("upload dir before: %s", sorted(os.listdir(upload)))

        if fail:
            # The first tablet's batch streams; the next one fails (test-only injection).
            await manager.api.enable_injection(server.ip_addr, FAIL, one_shot=True)
        refresh = asyncio.create_task(manager.api.load_new_sstables(server.ip_addr, ks, "dst", load_and_stream=True))
        try:
            await refresh
            logger.info("refresh succeeded")
        except Exception as e:
            logger.info("refresh failed: %s", e)
            assert fail, f"refresh failed without an injected failure: {e}"
        await manager.api.disable_injection(server.ip_addr, FAIL)

        left = sorted(os.listdir(upload))
        logger.info("upload dir after: %s", left)
        if fail:
            # The operator retries the failed refresh.
            await manager.api.load_new_sstables(server.ip_addr, ks, "dst", load_and_stream=True)

        rows = await cql.run_async(f"SELECT pk FROM {ks}.dst")
        logger.info("fail=%s: %d of %d rows loaded", fail, len(rows), len(keys))
        assert len(rows) == len(keys), \
            f"{len(keys) - len(rows)} of {len(keys)} rows lost: the failed refresh deleted the upload sstables (left: {left})"
