#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Unit tests for the CPU/RAM budget scheduler (no xdist workers involved).

Run with: python3 -m pytest --noconftest -p no:cacheprovider test/pylib_test/test_budget_scheduler.py

--noconftest matters: the repository's test/conftest.py registers the test.py runner
plugin, whose session start wipes <tmpdir>/report and (re)starts the shared services;
running these tests without it while a ./test.py run is active kills that run.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from test.pylib.budget_scheduler import (
    STATIC_MEM_CPP,
    STATIC_MEM_GDB,
    GB,
    BudgetScheduling,
    CostModel,
    parse_seastar_args,
    profile_key,
)

NO_CGROUP = Path("/nonexistent/cgroup")


class FakeNode:
    def __init__(self, wid: str):
        self.gateway = SimpleNamespace(id=wid)
        self.sent: list[int] = []
        self._shutdown_sent = False
        self._down = False

    @property
    def shutting_down(self) -> bool:
        return self._down or self._shutdown_sent

    def send_runtest_some(self, indices):
        assert not self._shutdown_sent, "sent tests after shutdown"
        self.sent.extend(indices)

    def shutdown(self):
        self._shutdown_sent = True


class FakeConfig:
    def __init__(self, tmp: Path, nodes: int, **overrides):
        self.opts = {
            "--budget-depth": 1,
            "--budget-cpu-target": 0.9,
            "--mode": ["release"],
            "--budget-cpu-overcommit": 1.5,

                    "--budget-default-cost": "2,2G",
            "--budget-k-sigma": 0.0,
            "--budget-profile": str(tmp / "profile.json"),
            "--tmpdir": str(tmp),
            "tx": ["popen"] * nodes,
        }
        self.opts.update(overrides)
        self.pluginmanager = SimpleNamespace(getplugin=lambda name: None)
        self.hook = SimpleNamespace(pytest_collectreport=lambda report: None)

    def getoption(self, name):
        return self.opts[name]

    def getvalue(self, name):
        return self.opts[name]


def make_model(tmp: Path, ncpus: int, entries: dict[str, tuple[float, float, float]]) -> CostModel:
    """entries: key -> (cores, mem_bytes, wall)."""
    model = CostModel(tmp / "profile.json", ncpus, k_sigma=0.0)
    for key, (cores, mem, wall) in entries.items():
        model.tests[key] = {"cores": cores, "mem": mem, "wall": wall, "var_cores": 0.0, "n": 3, "n_unc": 3}
    return model


def make_sched(tmp: Path, collection: list[str], costs: dict[str, tuple[float, float, float]],
               nodes: int = 2, ncpus: int = 4, mem_total: float = 20 * GB, **opts) -> tuple[BudgetScheduling, list[FakeNode]]:
    model = make_model(tmp, ncpus, {profile_key(nid): c for nid, c in costs.items()})
    sched = BudgetScheduling(FakeConfig(tmp, nodes, **opts), model=model, ncpus=ncpus, mem_total=mem_total,
                             cgroup_tests=NO_CGROUP, available_fn=lambda: mem_total)
    fake_nodes = [FakeNode(f"gw{i}") for i in range(nodes)]
    for n in fake_nodes:
        sched.add_node(n)
        sched.add_node_collection(n, collection)
    sched.schedule()
    return sched, fake_nodes


def committed(sched: BudgetScheduling) -> set[int]:
    return set(sched.committed_at)


# ---------------------------------------------------------------- helpers ----

def test_profile_key_strips_mode_and_run_id():
    assert profile_key("cqlpy/test_x.py::test_y[1.5].dev.1") == "dev|cqlpy/test_x.py::test_y[1.5]"
    assert profile_key("boost/x_test.cc::case.2.release.7") == "release|boost/x_test.cc::case.2"


def test_parse_seastar_args():
    assert parse_seastar_args("-c2 -m2G") == (2.0, 2e9)
    assert parse_seastar_args("-c1 -m256M --logger-log-level x=trace") == (1.0, 256e6)
    assert parse_seastar_args("--smp 3 --memory 1G") == (3.0, 1e9)
    assert parse_seastar_args("-c1") == (1.0, None)


def test_static_cost_reads_boost_custom_args(tmp_path):
    """Shards come from -c.  Memory does not come from -m: see the test below."""
    model = CostModel(tmp_path / "p.json", ncpus=16)
    c = model.cost("boost/sstable_test.cc::some_case.dev.1")            # custom_args: -c1 -m2G
    assert c.cores == 1.0 and c.source == "static-cpp"
    c = model.cost("boost/multishard_query_test.cc::some_case.dev.1")   # custom_args: -c2 -m3G
    assert c.cores == 2.0
    c = model.cost("boost/unknown_test.cc::case.dev.1")
    assert c.cores == 2.0
    c = model.cost("cqlpy/test_x.py::test_y.dev.1")
    assert c.source == "static-python" and c.cores == 1.0


def test_static_memory_comes_from_measurement_not_from_the_seastar_flag(tmp_path):
    """-m is what a test may reserve, not what it takes; a -m2G boost case peaks at 0.19 GB.

    Reading -m as the estimate booked 2.3 GB apiece, so a 36 GB budget was exhausted by
    seventeen tests and the rest of the first run starved behind them.
    """
    model = CostModel(tmp_path / "p.json", ncpus=16, mode="dev")        # no per-mode guesses: the base ones
    big = model.cost("boost/multishard_query_test.cc::some_case.dev.1")   # -c2 -m3G
    assert big.mem == pytest.approx(STATIC_MEM_CPP), "a roomy -m must not inflate the guess"
    plain = model.cost("boost/unknown_test.cc::case.dev.1")               # no custom args
    assert plain.mem == pytest.approx(STATIC_MEM_CPP)
    # ... but a small -m is a real ceiling, and does lower it
    small = model.cost("boost/reader_concurrency_semaphore_test.cc::case.dev.1")   # -c1 -m256M
    assert small.mem == pytest.approx(0.256e9 + 0.1e9)
    assert small.mem < STATIC_MEM_CPP


def test_the_one_suite_that_really_is_hungry_is_not_guessed_at_a_gigabyte(tmp_path):
    """scylla_gdb loads the whole binary's debug info: 2.4 GB median, 16 GB at the top.

    Guessing 1 GB for it, like every other Python suite, is what drove the first run into
    swap: a handful admitted together promise 4 GB and take forty.
    """
    model = CostModel(tmp_path / "p.json", ncpus=16, mode="dev")        # no per-mode guesses: the base ones
    gdb = model.cost("scylla_gdb/test_basic_commands.py::test_scylla_commands[task-queues].dev.1")
    assert gdb.source == "static-gdb"
    assert gdb.mem == pytest.approx(STATIC_MEM_GDB)
    assert gdb.mem > model.cost("cqlpy/test_x.py::test_y.dev.1").mem * 5
    # and it is ordered as a long test, not a one-second one
    assert gdb.wall >= 15.0
    c = model.cost("cluster/test_x.py::test_y.dev.1")
    assert c.source == "static-cluster"


def test_learn_ema_and_family_fallback(tmp_path):
    model = CostModel(tmp_path / "p.json", ncpus=8)
    model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 4.0, "memory_peak": 1e9})
    assert model.tests["dev|f.py::a"]["cores"] == 2.0
    model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 0.5e9})
    e = model.tests["dev|f.py::a"]
    assert e["cores"] == pytest.approx(0.7 * 2.0 + 0.3 * 1.0)
    assert e["mem"] == pytest.approx(1e9)            # a lower peak does not lower the estimate at once
    assert e["n"] == 2
    # asymmetric memory: three runs far below the estimate bring it down slowly
    for _ in range(3):
        model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 0.2e9})
    assert e["mem"] == pytest.approx(0.8e9)
    model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 3e9})
    assert e["mem"] == pytest.approx(3e9)            # a higher peak is taken immediately
    fam = model.cost("f.py::b.dev.1")                # unseen sibling -> family estimate
    assert fam.source == "family" and fam.cores == pytest.approx(e["cores"])
    # first-in-file samples of a known test feed the per-file setup cost, not the test
    model.learn({"key": "dev|f.py::a", "wall": 5.0, "usage_sec": 10.0, "memory_peak": 1e9, "first_in_file": True})
    assert model.tests["dev|f.py::a"]["n"] == 6
    model.save()
    reloaded = CostModel(tmp_path / "p.json", ncpus=8)
    assert reloaded.tests["dev|f.py::a"]["n"] == 6 and "dev|f.py" in reloaded.files


# --------------------------------------------------------------- scheduler ----

def test_sending_a_successor_is_what_starts_a_test(tmp_path):
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    # 3 cores each on a 4-cpu box with target 3.6 -> strictly one at a time
    sched, nodes = make_sched(tmp_path, col, {n: (3.0, 1e9, 1.0) for n in col})
    # every worker got primed with one held test, exactly one of them was committed
    assert all(len(n.sent) >= 1 for n in nodes)
    assert len(committed(sched)) == 1
    runner = next(n for n in nodes if len(n.sent) == 2)
    waiter = next(n for n in nodes if len(n.sent) == 1)
    running = runner.sent[0]
    assert committed(sched) == {running}
    # finishing the running test frees the budget: exactly one more starts
    sched.mark_test_complete(runner, running)
    assert len(committed(sched)) == 1
    assert (sched.stats["rejected_cpu_band"] + sched.stats["rejected_cpu_ceiling"]
            + sched.stats["rejected_no_headroom"]) >= 1
    assert waiter.sent or runner.sent  # somebody got the successor


def test_never_more_than_budget_and_all_tests_run(tmp_path):
    col = [f"a.py::t{i}.dev.1" for i in range(12)]
    costs = {n: (1.5, 1e9, 1.0) for n in col}
    sched, nodes = make_sched(tmp_path, col, costs, nodes=4, ncpus=4)   # target 3.6 -> 2 at a time
    done: set[int] = set()
    guard = 0
    while len(done) < len(col):
        guard += 1
        assert guard < 200
        assert len(committed(sched)) <= 2
        # complete one committed test (the oldest)
        idx = min(committed(sched), key=lambda i: sched.committed_at[i])
        node = next(n for n in nodes if idx in sched.node2pending[n])
        sched.mark_test_complete(node, idx)
        done.add(idx)
    assert done == set(range(12))
    assert sched.tests_finished
    assert all(n._shutdown_sent for n in nodes)


def test_progress_rule_runs_oversized_test(tmp_path):
    """A test asking for more RAM than the budget can never fit, but must still run (alone)."""
    col = ["a.py::big.dev.1", "a.py::big2.dev.1"]
    sched, nodes = make_sched(tmp_path, col, {n: (1.0, 100 * GB, 1.0) for n in col}, nodes=2, ncpus=4)
    assert len(committed(sched)) == 1
    assert sched.stats["forced"] == 1 and sched.stats["rejected_mem"] >= 1


def test_memory_gate(tmp_path):
    col = [f"a.py::t{i}.dev.1" for i in range(3)]
    # tiny cpu, 12 GB each on a 20 GB box (target 15 GB) -> one at a time by memory
    sched, nodes = make_sched(tmp_path, col, {n: (0.1, 12 * GB, 1.0) for n in col}, nodes=3, ncpus=16)
    assert len(committed(sched)) == 1
    assert sched.stats["rejected_mem"] >= 1


def make_mem_sched(tmp: Path, n: int, mem: float, avail: dict, clock: dict, live: dict | None = None):
    """n workers on 16 cores, n tests of `mem` each and almost no CPU; available and time are the caller's."""
    col = [f"a.py::t{i}.dev.1" for i in range(n)]
    model = make_model(tmp, 16, {profile_key(c): (0.1, mem, 60.0) for c in col})
    sched = BudgetScheduling(FakeConfig(tmp, n), model=model, ncpus=16, mem_total=20 * GB, cgroup_tests=NO_CGROUP,
                             now=lambda: clock["t"], available_fn=lambda: avail["v"])
    if live is not None:
        sched.live.memory = lambda wid: live.get(wid, 0.0)
    nodes = [FakeNode(f"gw{i}") for i in range(n)]
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, col)
    sched.schedule()
    return sched, nodes


def test_headroom_counts_the_tests_admitted_earlier_in_the_same_pass(tmp_path):
    """Every test in a pass sees the memory the previous ones were just given.

    Checking each test against the same MemAvailable let 35 tests in within one pass on the
    full release suite, each seeing 4.7 GB free for its 0.6 GB.  Here the budget (18 GB free
    at start) would hold six 3 GB tests on paper; the reserve (1 GB on a 20 GB machine) leaves
    room for five.
    """
    sched, _ = make_mem_sched(tmp_path, 8, 3 * GB, {"v": 18 * GB}, {"t": 0.0})
    assert sched.mem_reserve == pytest.approx(1 * GB)
    assert len(committed(sched)) == 5
    assert sched.stats["rejected_mem"] >= 1


def test_memory_nobody_reserved_stops_admission(tmp_path):
    """Memory outside every reservation -- grown workers, fixtures, containers -- still counts.

    Reservations say 12 of 18 GB; the machine says 8 GB is left, of which the four tests that
    just started have not taken their 12 yet.  Admission must believe the machine.
    """
    avail = {"v": 16 * GB}
    sched, nodes = make_mem_sched(tmp_path, 8, 3 * GB, avail, {"t": 0.0})
    assert len(committed(sched)) == 5
    first = min(committed(sched))
    avail["v"] = 8 * GB
    sched.mark_test_complete(next(n for n in nodes if first in sched.node2pending[n]), first)
    assert len(committed(sched)) == 4, "reservations had room, the machine did not"


def test_a_started_test_stops_counting_what_it_holds(tmp_path):
    """What a test has allocated is in MemAvailable already; its forecast counts only the rest."""
    avail, clock, live = {"v": 10 * GB}, {"t": 0.0}, {}
    sched, nodes = make_mem_sched(tmp_path, 4, 3 * GB, avail, clock, live)
    assert len(committed(sched)) == 3                   # 10 - 1 reserve = 9 GB: three 3 GB tests
    assert sched._mem_headroom() == pytest.approx(0.0)
    running = [n for n in nodes if any(i in sched.committed_at for i in sched.node2pending[n])]
    # each running test's worker has grown by its whole forecast, and the machine shows it
    for node in running:
        live[node.gateway.id] = 3 * GB
    avail["v"] = 1 * GB
    sched._refresh_forecasts()
    assert sched._mem_headroom() == pytest.approx(0.0), "the grown part must not count twice"
    # at 1 GB instead: 2 GB each still to come
    for node in running:
        live[node.gateway.id] = 1 * GB
    avail["v"] = 7 * GB
    sched._refresh_forecasts()
    assert sched._mem_headroom() == pytest.approx(0.0)


def test_release_guesses_price_dtests_as_the_heavy_tail_of_the_cluster_suite(tmp_path):
    model = CostModel(tmp_path / "p.json", ncpus=16, mode="release")
    dtest = model.cost("cluster/dtest/manager_backup_tests.py::TestX::test_y.release.1")
    cluster = model.cost("cluster/test_x.py::test_y.release.1")
    assert dtest.source == "static-dtest" and cluster.source == "static-cluster"
    assert dtest.mem == pytest.approx(0.9e9) and cluster.mem == pytest.approx(0.62e9)
    assert model.cost("scylla_gdb/test_basic_commands.py::test_x.release.1").mem == pytest.approx(6.1e9)
    # debug has no dtest guess of its own and keeps its cluster one
    debug = CostModel(tmp_path / "d.json", ncpus=16, mode="debug")
    assert debug.cost("cluster/dtest/x_test.py::test_y.debug.1").mem == pytest.approx(8.5e9)


def test_a_worker_holding_its_modules_cluster_adds_only_what_the_test_takes_beyond_it(tmp_path):
    """A peak is learned as the whole worker cgroup; what the worker holds already is not new memory."""
    avail, clock, live = {"v": 10 * GB}, {"t": 0.0}, {}
    col = [f"a.py::t{i}.dev.1" for i in range(8)]
    model = make_model(tmp_path, 16, {profile_key(c): (0.1, 2.2 * GB, 60.0) for c in col})
    sched = BudgetScheduling(FakeConfig(tmp_path, 8), model=model, ncpus=16, mem_total=20 * GB, cgroup_tests=NO_CGROUP,
                             now=lambda: clock["t"], available_fn=lambda: avail["v"])
    nodes = [FakeNode(f"gw{i}") for i in range(8)]
    for node in nodes:
        live[node.gateway.id] = 2 * GB                   # each worker keeps its module's 2 GB node
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    for node in nodes:
        sched.add_node(node)
        sched.add_node_collection(node, col)
    sched.schedule()
    # 10 GB available less a 1 GB reserve: at 2.2 GB each, four would fit; at 0.2 GB each, all eight do
    assert len(committed(sched)) == 8


def test_a_merged_samples_file_is_not_merged_again(tmp_path):
    """With a fixed SCYLLA_TEST_HOST_ID a left-behind samples file would be learned by every later run."""
    from test.pylib.budget_scheduler import append_sample, merge_run_into_profile, samples_path
    append_sample(tmp_path, {"key": "dev|f.py::t", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 1e9})
    assert merge_run_into_profile(tmp_path, tmp_path / "profile.json", 8) == 1
    assert not samples_path(tmp_path).exists()
    assert merge_run_into_profile(tmp_path, tmp_path / "profile.json", 8) == 0
    assert CostModel(tmp_path / "profile.json", 8).tests["dev|f.py::t"]["n"] == 1


def test_learning_drops_only_the_learned_files_cached_costs(tmp_path):
    col = ["a.py::t1.dev.1", "a.py::t2.dev.1", "b.py::t1.dev.1"]
    sched, nodes = make_sched(tmp_path, col, {n: (0.5, 1e9, 1.0) for n in col}, nodes=1)
    for idx in range(3):
        sched._costs_for(idx)
    pending = [i for i in range(3) if i not in sched.committed_at]
    sched.learn({"key": "dev|a.py::t1", "wall": 1.0, "usage_sec": 0.5, "memory_peak": 1e9})
    for idx in pending:
        assert (idx in sched._costs) == (sched._file_of(idx) != "dev|a.py"), idx


def test_a_first_in_file_sample_still_teaches_the_peak(tmp_path):
    """A small file is first-in-file on every worker; its tests' peaks must still be learned."""
    model = CostModel(tmp_path / "p.json", ncpus=8)
    model.learn({"key": "dev|f.py::a", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 1e9})
    model.learn({"key": "dev|f.py::a", "wall": 9.0, "usage_sec": 18.0, "memory_peak": 4e9, "first_in_file": True})
    e = model.tests["dev|f.py::a"]
    assert e["mem"] == pytest.approx(4e9), "a higher peak is taken at once, first-in-file or not"
    assert e["wall"] == pytest.approx(2.0) and e["n"] == 1, "its setup time stays out of the test's entry"
    assert model.files["dev|f.py"]["n"] == 1


def test_each_test_is_priced_for_its_own_build_mode(tmp_path):
    """--mode dev --mode debug: a debug cluster test gets debug's first-run price, not dev's."""
    model = CostModel(tmp_path / "p.json", ncpus=8, mode="dev")
    assert model.cost("cluster/test_x.py::test_y.debug.1").mem == pytest.approx(8.5e9)
    assert model.cost("cluster/test_x.py::test_y.dev.1").mem == pytest.approx(0.6e9)


def test_a_test_committed_behind_a_running_one_starts_when_it_ends(tmp_path):
    """Its clocks start when it really starts, not when it is committed."""
    clock, avail, live = {"t": 0.0}, {"v": 40 * GB}, {}
    col = [f"a.py::t{i}.dev.1" for i in range(3)]
    model = make_model(tmp_path, 16, {profile_key(c): (0.1, 5 * GB, 300.0) for c in col})
    sched = BudgetScheduling(FakeConfig(tmp_path, 1), model=model, ncpus=16, mem_total=64 * GB, cgroup_tests=NO_CGROUP,
                             now=lambda: clock["t"], available_fn=lambda: avail["v"])
    sched.live.memory = lambda wid: live.get(wid, 0.0)
    node = FakeNode("gw0")
    sched.add_node(node); sched.add_node_collection(node, col)
    sched.schedule()
    running, behind = sched.node2pending[node][0], sched.node2pending[node][1]
    sched._commit(behind, node)                        # as a retirement or the end of a run does
    assert running in sched._started_at and behind not in sched._started_at
    clock["t"] = 600.0
    sched.mark_test_complete(node, running)
    assert sched._started_at[behind] == 600.0


def test_remove_node_distinguishes_held_from_running(tmp_path):
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    sched, nodes = make_sched(tmp_path, col, {n: (3.0, 1e9, 1.0) for n in col})
    runner = next(n for n in nodes if len(n.sent) == 2)
    waiter = next(n for n in nodes if len(n.sent) == 1)
    held_only = waiter.sent[0]
    waiter._down = True
    assert sched.remove_node(waiter) is None          # never started -> not a crash
    assert held_only in sched.pending_set or any(held_only in sched.node2pending[n] for n in sched.node2pending)
    running = runner.sent[0]
    runner._down = True
    assert sched.remove_node(runner) == col[running]  # was running -> crash item
    assert running not in sched.committed_at


def test_file_affinity_and_longest_first(tmp_path):
    col = ["a.py::a1.dev.1", "a.py::a2.dev.1", "a.py::a3.dev.1", "b.py::b1.dev.1", "b.py::b2.dev.1"]
    costs = {"a.py::a1.dev.1": (0.1, 1e8, 5.0), "a.py::a2.dev.1": (0.1, 1e8, 1.0), "a.py::a3.dev.1": (0.1, 1e8, 1.0),
             "b.py::b1.dev.1": (0.1, 1e8, 10.0), "b.py::b2.dev.1": (0.1, 1e8, 1.0)}
    sched, nodes = make_sched(tmp_path, col, costs, nodes=2, ncpus=16)
    first = [n.sent[0] for n in nodes]
    # b has the most remaining wall (11 s) so its longest test goes out first, then a's longest
    assert col[first[0]] == "b.py::b1.dev.1" and col[first[1]] == "a.py::a1.dev.1"
    # successors stay in the same file
    assert col[nodes[0].sent[1]].startswith("b.py")
    assert col[nodes[1].sent[1]].startswith("a.py")


def test_a_test_is_predicted_at_its_average_parallelism(tmp_path):
    """One number per test: CPU-seconds over wall time, and nothing once it is well past its wall."""
    model = CostModel(tmp_path / "p.json", ncpus=8)
    model.learn({"key": "dev|f.py::t", "wall": 6.0, "usage_sec": 12.0, "memory_peak": 1.2e9})
    assert model.predict_at("f.py::t.dev.1", 0.5) == pytest.approx(2.0)
    assert model.predict_at("f.py::t.dev.1", 5.0) == pytest.approx(2.0)
    assert model.predict_at("f.py::t.dev.1", 30.0, future=True) == 0.0           # long past its wall


def test_a_just_started_test_counts_at_its_forecast_not_what_it_holds(tmp_path):
    """Two 11 GB tests on a 20 GB machine: the second waits even though nothing is using RAM yet."""
    col = [f"a.py::t{i}.dev.1" for i in range(3)]
    sched, nodes = make_sched(tmp_path, col, {n: (0.1, 11 * GB, 5.0) for n in col}, nodes=3, ncpus=16)
    assert len(committed(sched)) == 1
    assert sum(sched.res_mem.values()) == pytest.approx(11 * GB)   # request = the peak, no margin
    assert sched.stats["rejected_mem"] >= 1
    # completion releases the reservation and the next one is admitted
    running = next(iter(committed(sched)))
    node = next(n for n in nodes if running in sched.node2pending[n])
    sched.mark_test_complete(node, running)
    assert running not in sched.res_mem and len(committed(sched)) == 1


def test_backfill_past_unfittable_head(tmp_path):
    """Head of the queue needs 32 GB on a 20 GB box; the small tests behind it must run."""
    col = ["a.py::big.dev.1", "b.py::small1.dev.1", "b.py::small2.dev.1"]
    costs = {"a.py::big.dev.1": (4.0, 32 * GB, 100.0), "b.py::small1.dev.1": (2.0, 4 * GB, 1.0),
             "b.py::small2.dev.1": (4.0, 8 * GB, 1.0)}
    sched, nodes = make_sched(tmp_path, col, costs, nodes=3, ncpus=8, mem_total=25 * GB)   # budget 20 GB
    sched.check_schedule()      # the second backfilled test is held until the next tick commits it
    names = sorted(col[i] for i in committed(sched))
    assert "a.py::big.dev.1" not in names and len(names) == 2      # both small ones run
    assert sched.stats["backfilled"] >= 1


def test_longest_test_starts_first(tmp_path):
    """The long pole goes in when the machine is still empty, which is when it fits.

    Ordering by a file's total remaining work buried a 120 s test that lives in a small
    file behind whole directories of short ones, so it started once everything else had
    drained and then ran alone. Ordering by the longest single test puts it first.
    """
    col = ["b.py::s0.dev.1", "b.py::s1.dev.1", "b.py::s2.dev.1", "b.py::s3.dev.1", "a.py::hog.dev.1"]
    costs = {"a.py::hog.dev.1": (1.0, 8 * GB, 120.0)}
    costs.update({f"b.py::s{i}.dev.1": (1.0, 2 * GB, 30.0) for i in range(4)})
    sched, nodes = make_sched(tmp_path, col, costs, nodes=3, ncpus=8, mem_total=40 * GB)
    hog = col.index("a.py::hog.dev.1")
    assert hog in committed(sched), "the longest test must be among the first admitted"


def test_selection_counts_what_other_workers_already_hold(tmp_path):
    """Two workers must not both choose a test that only one of them can run.

    A test sent to a worker cannot be recalled, so if selection ignored the tests its
    colleagues are holding, every worker would pick something heavy at the same moment
    and the losers would sit blocked until the memory freed up.
    """
    col = [f"a.py::big{i}.dev.1" for i in range(2)] + [f"b.py::s{i}.dev.1" for i in range(6)]
    costs = {f"a.py::big{i}.dev.1": (1.0, 12 * GB, 60.0) for i in range(2)}
    costs.update({f"b.py::s{i}.dev.1": (1.0, 1 * GB, 5.0) for i in range(6)})
    sched, nodes = make_sched(tmp_path, col, costs, nodes=3, ncpus=8, mem_total=25 * GB)   # budget 20 GB
    chosen = [q[-1] for q in sched.node2pending.values() if q]
    big = {i for i, n in enumerate(col) if "big" in n}
    assert len(set(chosen) & big) <= 1, "only one worker may hold a 12 GB test on a 20 GB budget"
    assert len(chosen) == 3, "the other workers still get work, just lighter work"


def test_passed_over_test_still_runs(tmp_path):
    """Backfill may pass over a big test for as long as the machine is full.

    No test has a deadline, so nothing is reserved for it: the workers stay busy
    with whatever fits, and the big one runs once the small ones stop refilling
    the machine.  What must never happen is that it is dropped or dead-locks.
    """
    clock = {"t": 1000.0}
    col = ["a.py::big.dev.1"] + [f"b.py::s{i}.dev.1" for i in range(12)]
    costs = {"a.py::big.dev.1": (3.5, 1e8, 100.0)}                # 3.5 of 4 cores, head of the order
    costs.update({f"b.py::s{i}.dev.1": (1.0, 1e8, 1.0) for i in range(12)})
    model = make_model(tmp_path, 4, {profile_key(n): c for n, c in costs.items()})
    sched = BudgetScheduling(FakeConfig(tmp_path, 3), model=model, ncpus=4, mem_total=20 * GB,
                             cgroup_tests=NO_CGROUP, now=lambda: clock["t"], available_fn=lambda: 20 * GB)
    nodes = [FakeNode(f"gw{i}") for i in range(3)]
    for n in nodes:
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.schedule()
    big = 0
    assert committed(sched) == {big}                 # at a fresh start the head runs first, as it should
    # its worker dies: big is requeued at the front while the other two workers fill up with smalls
    victim = next(n for n in nodes if big in sched.node2pending[n])
    victim._down = True
    assert sched.remove_node(victim) == col[big]      # reported as crashed by xdist ...
    sched.mark_test_pending(col[big])                   # ... and re-queued, as a rerun plugin would
    spare = FakeNode("gw3")
    sched.add_node(spare); sched.add_node_collection(spare, col); sched.schedule()
    assert big not in committed(sched) and len(committed(sched)) >= 2

    def complete_one():
        idx = min(committed(sched), key=lambda i: sched.committed_at[i])
        node = next(n for n in sched.node2pending if idx in sched.node2pending[n])
        sched.mark_test_complete(node, idx)

    busy = []
    for _ in range(20):
        clock["t"] += 1.0
        complete_one()
        busy.append(len(committed(sched)))
        if big in committed(sched):
            break
    assert big in committed(sched)                    # it runs, without anything being reserved for it
    assert max(busy) >= 2                             # and the machine kept working while it waited


def test_contention_gates_parallelism_learning(tmp_path):
    model = CostModel(tmp_path / "p.json", ncpus=8)
    model.learn({"key": "dev|f.py::t", "wall": 25.0, "usage_sec": 100.0, "memory_peak": 1e8, "cpu_stall_frac": 0.01})
    e = model.tests["dev|f.py::t"]
    assert e["cores"] == pytest.approx(4.0) and e["n_unc"] == 1
    # a contended run (same work, twice the wall) must not lower the parallelism estimate
    model.learn({"key": "dev|f.py::t", "wall": 50.0, "usage_sec": 100.0, "memory_peak": 1e8, "cpu_stall_frac": 0.4})
    assert e["cores"] == pytest.approx(4.0) and e["wall"] == pytest.approx(25.0)
    assert e["n"] == 2 and e["n_unc"] == 1
    # an uncontended one does
    model.learn({"key": "dev|f.py::t", "wall": 20.0, "usage_sec": 100.0, "memory_peak": 1e8, "cpu_stall_frac": 0.0})
    assert e["cores"] == pytest.approx(0.7 * 4.0 + 0.3 * 5.0) and e["n_unc"] == 2


def test_a_target_above_the_core_count_really_over_subscribes(tmp_path):
    """Asking for more than the machine has is a request to keep work runnable, not idle.

    Below 100 % the machine itself is the limit and the target only bites inside the
    over-commit band; above it, the target has to move the hard gate too or the setting
    does nothing at all.
    """
    col = [f"a.py::t{i}.dev.1" for i in range(12)]
    costs = {n: (1.0, GB, 30.0) for n in col}
    at100, _ = make_sched(tmp_path, col, costs, nodes=12, ncpus=4, mem_total=60 * GB,
                          **{"--budget-cpu-target": 1.0})
    sub = tmp_path / "over"; sub.mkdir()
    at150, _ = make_sched(sub, col, costs, nodes=12, ncpus=4, mem_total=60 * GB,
                          **{"--budget-cpu-target": 1.5})
    assert len(committed(at150)) > len(committed(at100)), (
        f"1.5 admitted {len(committed(at150))}, 1.0 admitted {len(committed(at100))}")


def test_the_budget_is_measured_once_the_workers_are_up(tmp_path):
    """Whatever the workers cost is already spent by then, so measure it rather than guess.

    The guess it replaces was a per-worker constant, and at two workers per CPU a constant
    wrong by 50 MB is wrong by gigabytes.
    """
    col = [f"a.py::t{i}.dev.1" for i in range(4)]
    model = make_model(tmp_path, 8, {profile_key(n): (0.5, GB, 5.0) for n in col})
    free = {"now": 60 * GB}
    sched = BudgetScheduling(FakeConfig(tmp_path, 8), model=model, ncpus=8, mem_total=64 * GB,
                             cgroup_tests=NO_CGROUP, available_fn=lambda: free["now"])
    for i in range(8):
        n = FakeNode(f"gw{i}")
        sched.add_node(n); sched.add_node_collection(n, col)
    free["now"] = 42 * GB          # the workers have started and taken their share
    sched.schedule()
    assert sched.mem_target == pytest.approx(42 * GB), (
        "the budget must follow what is really left, not a per-worker estimate")


def test_no_memory_is_held_back(tmp_path):
    """The budget is what is free, all of it: no share of the machine is kept aside.

    12% of RAM held back and a 70% ceiling left 17 GB of a 22 GB free machine to debug
    cluster tests that take 7-11 GB each.  Admission's live RAM check guards the machine.
    """
    col = ["a.py::t0.dev.1"]
    model = make_model(tmp_path, 8, {profile_key(col[0]): (0.5, GB, 5.0)})
    for free in (30 * GB, 63 * GB):
        sched = BudgetScheduling(FakeConfig(tmp_path, 2), model=model, ncpus=8, mem_total=64 * GB,
                                 cgroup_tests=NO_CGROUP, available_fn=lambda: free)
        for i in range(2):
            n = FakeNode(f"gw{i}")
            sched.add_node(n); sched.add_node_collection(n, col)
        sched.schedule()
        assert sched.mem_target == pytest.approx(free)


def test_the_budget_follows_ram_not_swap(tmp_path):
    """Pages the kernel has already moved out of the tests are not RAM, and not a signal.

    Only what the machine has available, and what the tests hold in RAM, decide.
    """
    col = ["a.py::t0.dev.1", "a.py::t1.dev.1"]
    model = make_model(tmp_path, 8, {profile_key(n): (0.5, GB, 5.0) for n in col})
    sched = BudgetScheduling(FakeConfig(tmp_path, 2), model=model, ncpus=8, mem_total=64 * GB,
                             cgroup_tests=NO_CGROUP, available_fn=lambda: 30 * GB)
    assert not hasattr(sched, "measured_swap")
    before = sched.mem_target
    for n in (FakeNode("gw0"), FakeNode("gw1")):
        sched.add_node(n); sched.add_node_collection(n, col)
    sched.schedule()
    assert sched.mem_target == before == 30 * GB
    assert committed(sched) == {0, 1}


def test_debug_mode_guesses_more_memory_than_release(tmp_path):
    """A first run in debug must not price tests as if they were release builds.

    The same test carries the sanitizers and no optimisation, and admission counts a
    just-started test at its predicted peak, so a release-sized guess lets the scheduler
    admit several times more work than the machine can hold.
    """
    release = CostModel(tmp_path / "r.json", ncpus=16, mode="release")
    debug = CostModel(tmp_path / "d.json", ncpus=16, mode="debug")
    for nodeid in ("alternator/test_x.py::test_y", "cluster/test_z.py::test_w",
                   "boost/some_test.cc::case"):
        assert debug.cost(nodeid).mem > 2 * release.cost(nodeid).mem, nodeid
    # sanitize and coverage are debug builds by another name
    assert CostModel(tmp_path / "s.json", ncpus=16, mode="sanitize").cost(
        "cluster/test_z.py::test_w").mem == debug.cost("cluster/test_z.py::test_w").mem
