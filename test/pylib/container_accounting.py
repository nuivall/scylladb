#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Charge the containers a test starts to the xdist worker that started it.

A test's memory is measured as its worker's cgroup: the worker, and every Scylla node it
starts, live under it.  A container does not.  The Docker daemon puts its containers in its
own cgroups (system.slice/docker-<id>.scope), so a Cassandra JVM a migration test starts
is in no learned peak and no live reading, and the budget scheduler admitted nineteen such
tests at once on 20 GB it could not see.

Containers a test starts through the Docker SDK are registered automatically: an xdist
worker wraps ``ContainerCollection.run`` (see install_docker_hook), and every detached
container it starts is recorded with the container's cgroup under the worker.  The
scheduler and the resource gatherer add what those cgroups hold to the worker's own.  A
container that has gone takes its cgroup with it and drops out.
"""

from __future__ import annotations

import importlib.abc
import importlib.util
import os
import sys
from collections.abc import Iterable
from pathlib import Path

REGISTRY_ENV = "SCYLLA_TEST_CONTAINER_REGISTRY"
CGROUP_ROOT = Path("/sys/fs/cgroup")


def registry_dir() -> Path | None:
    value = os.environ.get(REGISTRY_ENV)
    return Path(value) if value else None


def cgroup_of_pid(pid: int, proc: Path = Path("/proc")) -> Path | None:
    """The cgroup v2 directory of a process."""
    try:
        for line in (proc / str(pid) / "cgroup").read_text().splitlines():
            if line.startswith("0::"):
                return CGROUP_ROOT / line[3:].lstrip("/")
    except OSError:
        pass
    return None


def register_container_pid(pid: int | None, worker: str | None = None) -> None:
    """Record that the container whose main process is `pid` was started by this worker.

    Does nothing outside an xdist worker of a run that keeps a registry, or when the
    container's cgroup cannot be read.
    """
    registry = registry_dir()
    worker = worker or os.environ.get("PYTEST_XDIST_WORKER")
    if registry is None or not worker or not pid:
        return
    cgroup = cgroup_of_pid(pid)
    if cgroup is None:
        return
    try:
        registry.mkdir(parents=True, exist_ok=True)
        with open(registry / f"{worker}.txt", "a") as f:
            f.write(f"{cgroup}\n")
    except OSError:
        pass            # accounting is best effort; a test must not fail over it


def container_cgroups(worker: str) -> list[Path]:
    """Cgroups of the containers this worker started that still exist."""
    registry = registry_dir()
    if registry is None:
        return []
    try:
        lines = (registry / f"{worker}.txt").read_text().splitlines()
    except OSError:
        return []
    return [p for p in dict.fromkeys(Path(line) for line in lines if line) if p.is_dir()]


def all_container_cgroups() -> dict[str, list[Path]]:
    """worker -> cgroups of the containers it started that still exist."""
    registry = registry_dir()
    if registry is None or not registry.is_dir():
        return {}
    return {f.stem: cgroups for f in registry.glob("*.txt") if (cgroups := container_cgroups(f.stem))}


def memory_stat(path: Path, field: str) -> float | None:
    """One field of a cgroup's memory.stat."""
    try:
        with open(path / "memory.stat") as f:
            for line in f:
                if line.startswith(field + " "):
                    return float(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return None


def worker_anon(cgroup: Path, containers: Iterable[Path] | None = None, worker: str | None = None) -> float | None:
    """Anonymous memory of a worker's cgroup plus that of the containers it started.

    The one definition of a worker's memory, for the peaks the profile learns and for the
    live readings the forecast subtracts them from: the two must be the same measure.
    `containers` defaults to what the registry holds for `worker`.
    """
    own = memory_stat(cgroup, "anon")
    if own is None:
        return None
    if containers is None:
        containers = container_cgroups(worker) if worker else []
    return own + sum(memory_stat(c, "anon") or 0.0 for c in containers)


DOCKER_CONTAINERS_MODULE = "docker.models.containers"


def _wrap_container_run(ContainerCollection) -> None:
    if getattr(ContainerCollection.run, "_charged_to_worker", False):
        return
    original = ContainerCollection.run

    def run(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        if kwargs.get("detach"):
            try:
                result.reload()
                register_container_pid(result.attrs.get("State", {}).get("Pid"))
            except Exception:
                pass            # accounting is best effort; a test must not fail over it
        return result

    run._charged_to_worker = True
    ContainerCollection.run = run


class _DockerHook(importlib.abc.MetaPathFinder):
    """Wraps ContainerCollection.run when the Docker SDK's containers module is first imported."""

    def find_spec(self, fullname, path, target=None):
        if fullname != DOCKER_CONTAINERS_MODULE:
            return None
        sys.meta_path.remove(self)
        spec = importlib.util.find_spec(fullname)
        if spec is None or spec.loader is None:
            return spec
        exec_module = spec.loader.exec_module

        def exec_and_wrap(module):
            exec_module(module)
            _wrap_container_run(module.ContainerCollection)

        spec.loader.exec_module = exec_and_wrap
        return spec


def install_docker_hook() -> None:
    """Register every detached container this process starts through the Docker SDK.

    Wraps ``docker.models.containers.ContainerCollection.run``, which is how the dtest
    harnesses start Cassandra, cassandra-stress and LDAP.  A container run to completion
    (``detach=False``) returns its output and is over before it could matter.  The SDK is
    not imported for this: a worker that never starts a container never pays for it (it is
    about 500 modules), and one that does gets the wrapper when the harness imports it.
    """
    module = sys.modules.get(DOCKER_CONTAINERS_MODULE)
    if module is not None:
        _wrap_container_run(module.ContainerCollection)
    elif not any(isinstance(f, _DockerHook) for f in sys.meta_path):
        sys.meta_path.insert(0, _DockerHook())
