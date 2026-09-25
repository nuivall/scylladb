#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Unit tests for charging containers to the worker that started them.

Run with: python3 -m pytest --noconftest -p no:cacheprovider test/pylib_test/test_container_accounting.py
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import test.pylib.container_accounting as ca
from test.pylib.budget_scheduler import CgroupReader
from test.pylib.resource_gather import ResourceGatherOn


def _cgroup(path: Path, anon: int, mapped: int = 0) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / "memory.stat").write_text(f"anon {anon}\nfile 123\nfile_mapped {mapped}\n")
    return path


@pytest.fixture
def world(tmp_path, monkeypatch):
    """A cgroup root with the tests' tree and a docker container, a /proc, and a registry."""
    root = tmp_path / "cgroup"
    monkeypatch.setattr(ca, "CGROUP_ROOT", root)
    monkeypatch.setenv(ca.REGISTRY_ENV, str(tmp_path / "registry"))
    tests = root / "user.slice" / "tests"
    _cgroup(tests, 5_000)
    _cgroup(tests / "gw0", 1_000)
    _cgroup(tests / "gw1", 2_000)
    jvm = _cgroup(root / "system.slice" / "docker-abc.scope", 800_000, mapped=50_000)
    proc = tmp_path / "proc"
    (proc / "4242").mkdir(parents=True)
    (proc / "4242" / "cgroup").write_text("0::/system.slice/docker-abc.scope\n")
    return SimpleNamespace(root=root, tests=tests, jvm=jvm, proc=proc)


def test_a_registered_container_is_charged_to_its_worker(world, monkeypatch):
    real = ca.cgroup_of_pid
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: real(pid, world.proc))
    ca.register_container_pid(4242, worker="gw1")
    assert ca.container_cgroups("gw1") == [world.jvm]
    assert ca.container_cgroups("gw0") == []
    assert ca.all_container_cgroups() == {"gw1": [world.jvm]}


def test_the_cgroup_of_a_process_is_read_from_proc(world):
    assert ca.cgroup_of_pid(4242, world.proc) == world.jvm
    assert ca.cgroup_of_pid(9999, world.proc) is None


def test_a_container_that_is_gone_drops_out(world, monkeypatch):
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: world.jvm)
    ca.register_container_pid(4242, worker="gw0")
    for f in world.jvm.iterdir():
        f.unlink()
    world.jvm.rmdir()
    assert ca.container_cgroups("gw0") == []


def test_nothing_is_recorded_outside_a_worker_or_without_a_registry(world, monkeypatch):
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: world.jvm)
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    ca.register_container_pid(4242)
    assert ca.all_container_cgroups() == {}
    monkeypatch.delenv(ca.REGISTRY_ENV)
    ca.register_container_pid(4242, worker="gw0")
    assert ca.container_cgroups("gw0") == []


def test_the_scheduler_sees_a_workers_containers(world, monkeypatch):
    """What the forecast and the tests' total are measured from includes the Cassandra JVM."""
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: world.jvm)
    ca.register_container_pid(4242, worker="gw1")
    reader = CgroupReader(world.tests)
    reader.refresh(["gw0", "gw1"])
    assert reader.memory("gw0") == pytest.approx(1_000)
    assert reader.memory("gw1") == pytest.approx(2_000 + 800_000), "anonymous, like the peaks the profile learns"


def test_a_tests_anonymous_peak_includes_its_workers_containers(world, monkeypatch):
    """A learned peak must include the JVM, or the profile prices a migration test at its worker alone."""
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: world.jvm)
    ca.register_container_pid(4242, worker="gw1")
    gatherer = SimpleNamespace(cgroup_path=world.tests / "gw1", worker_id="gw1")
    assert ResourceGatherOn._read_anon(gatherer) == 2_000 + 800_000
    other = SimpleNamespace(cgroup_path=world.tests / "gw0", worker_id="gw0")
    assert ResourceGatherOn._read_anon(other) == 1_000


def test_a_detached_container_started_through_the_docker_sdk_is_registered(world, monkeypatch):
    """No harness has to cooperate: the worker's hook registers every detached container it starts."""
    from docker.models.containers import ContainerCollection
    started = []

    class FakeContainer:
        attrs = {"State": {"Pid": 4242}}

        def reload(self):
            pass

    def fake_run(self, image, **kwargs):
        started.append(image)
        return FakeContainer() if kwargs.get("detach") else b"output"

    monkeypatch.setattr(ContainerCollection, "run", fake_run)
    real = ca.cgroup_of_pid
    monkeypatch.setattr(ca, "cgroup_of_pid", lambda pid: real(pid, world.proc))
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    ca.install_docker_hook()
    ca.install_docker_hook()                           # installing twice does not wrap twice
    assert ContainerCollection.run._charged_to_worker
    ContainerCollection.run(None, "cassandra:3.11", detach=True)
    assert ca.container_cgroups("gw3") == [world.jvm]
    ContainerCollection.run(None, "cassandra:3.11", command="cat /etc/cassandra/cassandra.yaml")
    assert ca.container_cgroups("gw3") == [world.jvm], "a container run to completion is not registered"
    assert started == ["cassandra:3.11", "cassandra:3.11"]


def test_the_docker_hook_does_not_import_the_sdk():
    """A worker that never starts a container must not pay for importing the Docker SDK."""
    import subprocess, sys
    code = ("import sys, test.pylib.container_accounting as ca; ca.install_docker_hook(); "
            "print('docker' in sys.modules); import docker.models.containers as c; "
            "print(getattr(c.ContainerCollection.run, '_charged_to_worker', False))")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout.split()
    assert out == ["False", "True"], "not imported by the hook, and wrapped once the harness imports it"
