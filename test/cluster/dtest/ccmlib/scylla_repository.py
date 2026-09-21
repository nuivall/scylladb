#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Stand-in for ccm's ccmlib/scylla_repository.py.

The real module downloads/builds relocatable Scylla packages, which the in-tree
dtest port has no use for (Scylla is provided by test.pylib.scylla_cluster_manager
instead), so `setup()` stays unimplemented.

What is implemented here is the Scylla Manager side of that module. The manager
server, the sctool CLI and the per-node manager agent are not built from this
tree; they ship as one relocatable tarball on downloads.scylladb.com. The dtest
manager tests need all three, so `setup_scylla_manager()` downloads the tarball
once, caches it next to the other test.py downloads, and returns the unpacked
directory for ccmlib.scylla_manager.ScyllaManager to install from.
"""

from __future__ import annotations

import logging
import os
import platform
import re
import shutil
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from test.pylib.version_fetch_utils import (
    download_scylla_version,
    extract_tar_no_same_owner,
    with_file_lock,
)

logger = logging.getLogger(__name__)

DOWNLOADS_BUCKET = "downloads.scylladb.com"
BASE_DOWNLOADS_URL = f"https://s3.amazonaws.com/{DOWNLOADS_BUCKET}"
MANAGER_RELEASE_PREFIX = "downloads/scylla-manager/relocatable/"
MANAGER_UNSTABLE_PREFIX = "manager/relocatable/unstable/"

# A URL, or a path to a local .tar.gz, to use instead of the default package.
# This is the in-tree equivalent of ccm's `--scylla-manager=` option.
SCYLLA_MANAGER_PACKAGE_ENV = "SCYLLA_MANAGER_PACKAGE"

_MANAGER_TARBALL_RE = re.compile(r"scylla-manager_(?P<version>[^_]+)_linux_(?P<arch>[^.]+)\.tar\.gz$")


def setup(version, verbose=True, skip_downloads=False):
    raise NotImplementedError(
        "ccmlib.scylla_repository.setup is not available in the in-tree dtest port; "
        "Scylla installs are managed via test.pylib.scylla_cluster_manager instead."
    )


def _list_manager_relocatables(prefix: str, architecture: str) -> list[str]:
    """Return the keys of every manager relocatable under `prefix`, sorted.

    Both layouts sort oldest-first by key: releases are grouped by version
    directory, and unstable builds by an ISO-8601 timestamp directory.
    """

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=DOWNLOADS_BUCKET, Prefix=prefix):
        for entry in page.get("Contents", ()):
            key = entry["Key"]
            match = _MANAGER_TARBALL_RE.search(key)
            if match and match.group("arch") == architecture:
                keys.append(key)
    return sorted(keys)


def _architecture(architecture: str | None) -> str:
    return architecture or os.environ.get("SCYLLA_ARCH") or platform.machine()


def get_manager_release_url(version: str = "", architecture: str | None = None) -> str:
    """URL of the newest published manager release, optionally pinned to a version.

    `version` is matched against the version part of the file name, so "3.5" picks
    the newest 3.5.x and "3.5.1" picks that exact release.
    """

    architecture = _architecture(architecture)
    keys = _list_manager_relocatables(MANAGER_RELEASE_PREFIX, architecture=architecture)
    if version:
        keys = [key for key in keys if _MANAGER_TARBALL_RE.search(key).group("version").startswith(version)]
    if not keys:
        raise RuntimeError(f"no Scylla Manager release for version={version!r} architecture={architecture!r}")
    return f"{BASE_DOWNLOADS_URL}/{keys[-1]}"


def get_manager_latest_reloc_url(branch: str = "master", architecture: str | None = None) -> str:
    """URL of the newest manager build of `branch`.

    This, not the newest release, is the default: a manager only understands
    the SSTable format versions it was built to know about, and master Scylla
    writes newer ones than the last release understands.  A published 3.9.1
    server, for instance, fails a restore of this tree's SSTables with
    "unknown SSTable format version: mt-...".
    """

    architecture = _architecture(architecture)
    keys = _list_manager_relocatables(f"{MANAGER_UNSTABLE_PREFIX}{branch}/", architecture=architecture)
    if not keys:
        raise RuntimeError(f"no Scylla Manager build for branch={branch!r} architecture={architecture!r}")
    return f"{BASE_DOWNLOADS_URL}/{keys[-1]}"


def setup_scylla_manager(scylla_manager_package: str | None = None) -> Path:
    """Download (once) and unpack a Scylla Manager relocatable, return its directory.

    The directory holds `sctool`, `scylla-manager` and `scylla-manager-agent`
    next to the `etc/` sample configuration, which is what
    ccmlib.scylla_manager.ScyllaManager installs from.
    """

    scylla_manager_package = (
        scylla_manager_package
        or os.environ.get(SCYLLA_MANAGER_PACKAGE_ENV)
        or get_manager_latest_reloc_url()
    )

    local_package = Path(scylla_manager_package)
    archive_name = local_package.name if local_package.is_file() else scylla_manager_package.rsplit("/", 1)[-1]

    cache_root = Path(os.getenv("XDG_CACHE_HOME", str(Path.home() / ".cache")))
    cache_dir = cache_root / "scylladb" / "test.py" / "scylla-manager" / archive_name
    unpack_dir = cache_dir / "unpacked"
    unpacked_marker = cache_dir / "unpacked.success"

    with with_file_lock(cache_dir / "lock"):
        if not unpacked_marker.exists():
            if local_package.is_file():
                archive_path = local_package
            else:
                logger.info("Downloading Scylla Manager from %s", scylla_manager_package)
                archive_path = download_scylla_version(url=scylla_manager_package, output_dir=cache_dir, retry=5)
                if archive_path is None:
                    raise RuntimeError(f"failed to download Scylla Manager package {scylla_manager_package}")
            shutil.rmtree(unpack_dir, ignore_errors=True)
            unpack_dir.mkdir(parents=True, exist_ok=True)
            extract_tar_no_same_owner(archive_path, unpack_dir)
            for name in ("sctool", "scylla-manager", "scylla-manager-agent"):
                binary = unpack_dir / name
                if not binary.is_file():
                    raise RuntimeError(f"{scylla_manager_package} is not a Scylla Manager relocatable: {name} is missing")
                binary.chmod(0o755)
            unpacked_marker.touch()

    return unpack_dir
