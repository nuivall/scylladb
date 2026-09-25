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


import pytest

from test.pylib.budget_scheduler import (
    STATIC_MEM_CPP,
    STATIC_MEM_GDB,
    CostModel,
    parse_seastar_args,
    profile_key,
)


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


def test_a_merged_samples_file_is_not_merged_again(tmp_path):
    """With a fixed SCYLLA_TEST_HOST_ID a left-behind samples file would be learned by every later run."""
    from test.pylib.budget_scheduler import append_sample, merge_run_into_profile, samples_path
    append_sample(tmp_path, {"key": "dev|f.py::t", "wall": 2.0, "usage_sec": 2.0, "memory_peak": 1e9})
    assert merge_run_into_profile(tmp_path, tmp_path / "profile.json", 8) == 1
    assert not samples_path(tmp_path).exists()
    assert merge_run_into_profile(tmp_path, tmp_path / "profile.json", 8) == 0
    assert CostModel(tmp_path / "profile.json", 8).tests["dev|f.py::t"]["n"] == 1


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
