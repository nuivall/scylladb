#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""CPU- and RAM-aware ("budget") scheduler for pytest-xdist.

How it works
------------
Every test run records, per test, its wall time, the CPU-seconds its whole
cgroup consumed, its peak anonymous memory, and whether it was contended (see
``pytest_runtest_protocol`` in ``test/pylib/runner.py``).
The samples land in ``<tmpdir>/budget_samples_<HOST_ID>.jsonl`` and are merged
into ``<tmpdir>/budget_profile.json`` at the end of the run.  The profile keeps
only what admission reads: the reservation (natural parallelism and its
variance, conservative peak memory), the expected wall time used for ordering,
and the per-file setup cores.

On the next run the controller (the xdist master) uses that profile to admit
tests: a worker that holds a test is only allowed to start it when the
predicted total CPU and RAM of everything already running plus this test fits
into the budget.  The xdist worker protocol makes that possible without any
change to the workers: a worker pops a test and then blocks until it knows the
*next* index (or a shutdown), so sending one more index is what starts the held
test.

One test is always allowed to start when nothing is running, so a run can never
dead-lock.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import shlex
import statistics
import threading
import time
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psutil
import pytest
import yaml

from test import HOST_ID, TOP_SRC_DIR
from test.pylib.container_accounting import all_container_cgroups, worker_anon

if TYPE_CHECKING:
    from xdist.workermanage import WorkerController

logger = logging.getLogger(__name__)

GB = 1e9
PROFILE_FILENAME = "budget_profile.json"
SAMPLES_GLOB = "budget_samples_*.jsonl"

EMA_ALPHA = 0.3
MEM_DECAY = 0.7
# A run whose cgroup waited for a CPU more than this share of its wall time was
# contended: its wall time and parallelism are not learned (only its CPU work).
CONTENDED_STALL_FRAC = 0.10
PROFILE_VERSION = 4       # bumped when the recorded format changes; older files are ignored
# Static memory guesses, for the run that has no profile yet.  Measured over the 11,433
# tests of the release suite (median / p90 / max, GB):
#   boost and the other C++ suites    0.19 / 0.36 / 1.84
#   cqlpy, alternator, cql, rest_api  0.20 / 0.24 / 0.48
#   cluster                           0.19 / 0.44 / 3.72
#   scylla_gdb                        2.44 / 3.46 / 16.01
# The seastar -m flag is what a test is *allowed* to reserve, not what it takes: a -m2G
# boost case peaks at a fifth of a gigabyte.  Reading it as the estimate booked 2.3 GB
# apiece, so seventeen tests could exhaust a 36 GB budget and the rest of the run starved;
# meanwhile the one suite that is genuinely hungry was booked at 1 GB and swapped the
# machine.  These are set near each family's p90, which also keeps a full set of workers
# inside the budget: 64 x 0.5 GB is 32 GB.
STATIC_MEM_CPP = 0.5 * 10**9
STATIC_MEM_PY = 0.3 * 10**9
STATIC_MEM_CLUSTER = 0.6 * 10**9
STATIC_MEM_GDB = 3.5 * 10**9

# First-run guesses, before the profile knows anything.  A debug build has no
# optimisation and carries the sanitizers, so the same test costs several times what it
# costs in release: measured on this tree, a C++ case peaks at 1.0 G rather than 0.04 G,
# an alternator test at 1.4 G rather than 0.25 G, a cluster test at 4.2 G.  Guessing
# release numbers for a debug run is what drove the machine into swap in under a minute,
# because admission counts a just-started test at its predicted peak -- and the
# prediction was five times too small.  Over-guessing only costs some parallelism on the
# very first run; under-guessing costs the machine.
# These are p90s, not medians, and deliberately so.  A run with no profile is pricing risk,
# not describing a typical test: measured here, cluster tests in debug range from 4.3 GB to
# 8.4 GB, and a single one of them taking 8.4 against a 4.5 guess is enough to put the box
# into swap in four seconds, which no feedback loop can react to.  Over-pricing costs some
# parallelism on the first run only, because the profile replaces these with real numbers.
STATIC_MEM_BY_MODE = {
    # Anonymous peaks measured over a full debug run (10176 tests): cpp p90 1.3G (max 7.0G)
    # over 4372 cases, py p90 1.6G (p99 1.7G) over 5200, cluster p90 5.5G (p99 9.3G, max
    # 11.4G).  Priced above the p90 because a run with no profile is pricing risk; the
    # profile takes over after one run.
    "debug": {"cpp": 2.0 * 10**9, "py": 1.8 * 10**9, "cluster": 8.5 * 10**9, "gdb": 8.0 * 10**9},
    # Firm peaks (anonymous + mapped) learned over a full release run (15,147 tests), p90 each:
    # cpp 0.30G (max 2.4G) over 4553, py 0.30G (max 0.5G) over 5283, cluster outside dtest
    # 0.62G (max 1.7G) over 1539, cluster/dtest 0.90G (p99 3.3G, max 9.4G) over 3726, gdb 6.1G
    # (max 13G) over 46.  The dtests are the heavy tail of the cluster suite, so they get their
    # own guess rather than dragging the rest of it up.
    "release": {"cpp": 0.3 * 10**9, "py": 0.3 * 10**9, "cluster": 0.62 * 10**9, "dtest": 0.9 * 10**9,
                "gdb": 6.1 * 10**9},
}
STATIC_MEM_MODE_ALIAS = {"sanitize": "debug", "coverage": "debug"}
# No memory is held back.  The budget is what is free once the workers are up, and a
# fixed share of RAM kept aside on top of that (12% of the machine, and a 70% ceiling)
# only cost parallelism: on a 33 GB machine with 22 GB free it left 17 GB for debug
# cluster tests that take 7-11 GB each.  Admission watches RAM itself: a test starts only
# while the machine has its predicted peak available.
# Admission gates on measured headroom: what the machine has available, minus what the
# tests admitted recently have not taken yet, minus this reserve.  Reservations alone were
# blind to memory no test owns -- workers growing from 0.2 to 0.5 GB each over a run, module
# fixtures, containers -- and the full release suite reached 25 GB of the tests' own swap
# with 20 GB reserved against 42 GB held.  The reserve keeps admission from spending the
# last of MemAvailable, which is where the kernel starts reclaiming the tests themselves.
MEM_RESERVE_FRACTION = 0.05
MEM_RESERVE_MIN = 1 * 10**9
MEM_RESERVE_MAX = 4 * 10**9


def profile_key(nodeid: str) -> str:
    """``cqlpy/test_x.py::test_y[p].dev.1`` -> ``dev|cqlpy/test_x.py::test_y[p]``.

    ``modify_pytest_item`` appends ``.<mode>.<run_id>`` to every item; strip it so
    repeats and runs share one entry, and put the mode in front because costs
    differ per build mode.
    """
    parts = nodeid.rsplit(".", 2)
    if len(parts) == 3:
        base, mode, _run_id = parts
    else:
        base, mode = nodeid, "unknown"
    return f"{mode}|{base}"


def file_of_key(key: str) -> str:
    """``dev|cqlpy/test_x.py::test_y`` -> ``dev|cqlpy/test_x.py``."""
    return key.split("::", 1)[0]


def parse_size(text: str) -> float:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?)i?B?\s*", text, re.IGNORECASE)
    if not m:
        raise ValueError(f"bad size: {text!r}")
    mult = {"": 1, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}[m.group(2).upper()]
    return float(m.group(1)) * mult


def parse_seastar_args(args: str) -> tuple[float | None, float | None]:
    """Return (cores, memory_bytes) from a seastar arg string like ``-c2 -m2G``."""
    cores = mem = None
    tokens = shlex.split(args)
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        for flag, kind in (("-c", "cores"), ("--smp", "cores"), ("-m", "mem"), ("--memory", "mem")):
            value = None
            if tok == flag and i + 1 < len(tokens):
                value = tokens[i + 1]
                i += 1
            elif tok.startswith(flag) and len(tok) > len(flag) and (flag.startswith("--") and tok[len(flag)] == "=" or not flag.startswith("--")):
                value = tok[len(flag):].lstrip("=")
            if value is not None:
                try:
                    if kind == "cores":
                        cores = float(value)
                    else:
                        mem = parse_size(value)
                except ValueError:
                    pass
                break
        i += 1
    return cores, mem


@dataclass
class Cost:
    cores: float
    mem: float
    wall: float
    source: str


class CostModel:
    """Loads/saves the profile and predicts the cost of a test from its nodeid."""

    def __init__(self, profile_path: Path, ncpus: int, k_sigma: float = 1.0,
                 default_cost: tuple[float, float] = (2.0, 2 * GB), mode: str = "release"):
        self.profile_path = profile_path
        self.ncpus = ncpus
        self.k_sigma = k_sigma
        self.mode = STATIC_MEM_MODE_ALIAS.get(mode, mode)   # for a key that names no known mode
        self.default_cores, self.default_mem = default_cost
        self.tests: dict[str, dict[str, Any]] = {}
        self.files: dict[str, dict[str, Any]] = {}
        self._file_index: dict[str, list[str]] | None = None
        self._static_cache: dict[str, dict[str, Any]] = {}
        self.load()

    # -- persistence -------------------------------------------------------

    def load(self) -> None:
        try:
            data = json.loads(self.profile_path.read_text())
        except FileNotFoundError:
            return
        except (OSError, ValueError) as e:
            logger.warning("budget: cannot read profile %s: %s", self.profile_path, e)
            return
        if data.get("version") != PROFILE_VERSION:
            logger.info("budget: ignoring profile %s written in format v%s", self.profile_path, data.get("version"))
            return
        self.tests = data.get("tests", {})
        self.files = data.get("files", {})
        self._file_index = None

    def save(self) -> None:
        data = {"version": PROFILE_VERSION, "tests": self.tests, "files": self.files}
        tmp = self.profile_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=1, sort_keys=True))
        os.replace(tmp, self.profile_path)

    # -- learning ----------------------------------------------------------

    def learn(self, sample: dict[str, Any]) -> None:
        """Merge one execution record.

        Record: wall, usage_sec (cgroup cpu_time = user + system), memory_peak
        (anon peak of the cgroup), cpu_stall_frac (contention), first_in_file.

        CPU work (CPU-seconds) is learned from every run.  Natural parallelism
        (cpu_time / wall) and uncontended runtime are learned only from runs that
        were not contended: a contended run has a longer wall time and would
        teach a lower parallelism, which would admit more tests, which would
        contend more.  Memory adapts asymmetrically: a higher peak is taken at
        once, a lower one only after repeated evidence.
        """
        key = sample["key"]
        wall = sample.get("wall")
        if wall is None or wall <= 0:
            return
        usage = sample.get("usage_sec")
        cores = usage / wall if usage is not None else None
        mem = sample.get("memory_peak")
        stall = sample.get("cpu_stall_frac")
        contended = bool(sample.get("cpu_contended")) or (stall is not None and stall > CONTENDED_STALL_FRAC)
        entry = self.tests.get(key)
        first = bool(sample.get("first_in_file"))
        fkey = file_of_key(key)

        if first and entry is not None and entry.get("n", 0) >= 1:
            # The first test of a file on a worker pays for the module fixture
            # (cluster start).  Its time and cores go to the file's setup cost, from the
            # excess over the test's own estimate, and stay out of the test's entry.  Its
            # memory does not: what a worker holds at the peak of such a test is what the
            # test costs on a fresh worker, and a small file is first on every worker, so
            # skipping these samples froze its tests' peaks at the one that created them.
            setup = self.files.setdefault(fkey, {"setup_cores": 0.0, "n": 0})
            extra_cores = max(0.0, cores - (entry.get("cores") or 0.0)) if cores is not None else 0.0
            setup["setup_cores"] = _ema(setup["setup_cores"], extra_cores, setup["n"])
            setup["n"] += 1
            self._learn_mem(entry, mem)
            return

        if entry is None:
            entry = self.tests[key] = {"wall": wall, "cores": cores, "mem": mem,
                                       "var_cores": 0.0, "n": 0, "n_unc": 0, "low_streak": 0}
            self._file_index = None
        n = entry["n"]
        # parallelism and runtime: only from uncontended runs (or until one exists)
        if not contended or entry.get("n_unc", 0) == 0:
            n_unc = entry.get("n_unc", 0)
            entry["wall"] = _ema(entry["wall"], wall, n_unc)
            if cores is not None:
                old = entry.get("cores")
                if old is not None:
                    entry["var_cores"] = _ema(entry.get("var_cores", 0.0), (cores - old) ** 2, n_unc)
                entry["cores"] = _ema(old, cores, n_unc)
            if not contended:
                entry["n_unc"] = n_unc + 1
        self._learn_mem(entry, mem)
        entry["n"] = n + 1

    # -- prediction --------------------------------------------------------

    def cost(self, nodeid: str) -> Cost:
        """Scheduling request for a test.

        cores: expected natural parallelism (cpu_time / uncontended wall) plus a
        small variance margin; this is what the scheduler reserves.  Instantaneous
        peaks are not reserved: bursts of independent tests multiplex.
        mem: the conservative peak (asymmetric estimate), with no margin on top.
        The peak is already the worst moment of a test's life, held for all of it,
        and peaks rarely coincide; a margin on top priced debug cluster tests at
        12-14 GB and let one or two of them run at a time.  The live RAM check in
        admission catches a peak that really was an underestimate.  wall: expected
        uncontended runtime (ordering).
        """
        key = profile_key(nodeid)
        entry = self.tests.get(key)
        if entry and entry.get("cores") is not None and entry.get("mem") is not None:
            cores = entry["cores"] + self.k_sigma * math.sqrt(max(0.0, entry.get("var_cores", 0.0)))
            return self._finish(cores, entry["mem"], entry["wall"], "profile")
        fam = self._family(key)
        if fam is not None:
            return self._finish(fam[0], fam[1], entry["wall"] if entry else fam[2], "family")
        static = self._static(nodeid, key)
        return self._finish(static["cores"], static["mem"], entry["wall"] if entry else static["wall"], static["source"])

    def setup_cost(self, nodeid: str) -> float:
        """Extra cores paid when a worker switches to this test's file (module fixture, cluster start)."""
        fkey = file_of_key(profile_key(nodeid))
        setup = self.files.get(fkey)
        return setup["setup_cores"] if setup else 0.0

    def _finish(self, cores: float, mem: float, wall: float, source: str) -> Cost:
        cores = min(float(self.ncpus), max(0.05, cores))
        return Cost(cores=cores, mem=max(50e6, float(mem)), wall=max(0.05, float(wall)), source=source)

    def _family(self, key: str) -> tuple[float, float, float] | None:
        siblings = [s for s in self._siblings(key) if s.get("cores") is not None and s.get("mem") is not None]
        if not siblings:
            return None
        return (statistics.median(s["cores"] for s in siblings),
                max(s["mem"] for s in siblings),
                statistics.median(s["wall"] for s in siblings))

    def _learn_mem(self, entry: dict[str, Any], mem: float | None) -> None:
        """Memory adapts asymmetrically: a higher peak is taken at once, a lower one slowly."""
        if mem is None:
            return
        old_mem = entry.get("mem")
        if not old_mem or mem >= old_mem:
            entry["mem"] = mem                      # up: immediately
            entry["low_streak"] = 0
        elif mem < 0.5 * old_mem:
            entry["low_streak"] = entry.get("low_streak", 0) + 1
            if entry["low_streak"] >= 3:            # down: slowly, after repeated evidence
                entry["mem"] = max(mem, old_mem * 0.8)
                entry["low_streak"] = 0
        else:
            entry["low_streak"] = 0

    def _siblings(self, key: str) -> list[dict[str, Any]]:
        if self._file_index is None:
            idx: dict[str, list[str]] = defaultdict(list)
            for k in self.tests:
                idx[file_of_key(k)].append(k)
            self._file_index = idx
        return [self.tests[k] for k in self._file_index.get(file_of_key(key), ())]

    def mode_of(self, key: str) -> str:
        """The build mode a profile key belongs to.

        Every test carries its own mode: a run of --mode dev --mode debug prices its debug
        tests with debug's numbers, not with those of whichever mode came first.
        """
        mode = STATIC_MEM_MODE_ALIAS.get(key.split("|", 1)[0], key.split("|", 1)[0])
        return mode if mode in ("dev", "release", "debug") else self.mode

    def static_mem(self, kind: str, default: float, mode: str | None = None) -> float:
        """Static memory guess for a family, for a build mode."""
        return STATIC_MEM_BY_MODE.get(mode or self.mode, {}).get(kind, default)

    def _static(self, nodeid: str, key: str) -> dict[str, Any]:
        """Cost guess from the test's location and the suite's static config."""
        base = key.split("|", 1)[1]
        path = base.split("::", 1)[0]
        suite = path.split("/", 1)[0]
        mode = self.mode_of(key)
        if path.endswith(".cc"):
            return self._static_cpp(suite, path, base, mode)
        if suite == "scylla_gdb":
            # gdb loads the debug info of the whole binary, and these are the only tests
            # that ever need tens of gigabytes.  A flat 1 GB guess here is what put the
            # machine into swap on the first run.
            return {"cores": 1.0, "mem": self.static_mem("gdb", STATIC_MEM_GDB, mode),
                    "wall": 30.0, "source": "static-gdb"}
        if suite == "cluster":
            cluster_mem = self.static_mem("cluster", STATIC_MEM_CLUSTER, mode)
            if path.startswith("cluster/dtest/"):
                return {"cores": 2.0, "mem": self.static_mem("dtest", cluster_mem, mode),
                        "wall": 15.0, "source": "static-dtest"}
            return {"cores": 2.0, "mem": cluster_mem, "wall": 15.0, "source": "static-cluster"}
        # cqlpy, alternator, cql, rest_api, ...: one shared single-node cluster per module.
        return {"cores": 1.0, "mem": self.static_mem("py", STATIC_MEM_PY, mode),
                "wall": 1.0, "source": "static-python"}

    def _static_cpp(self, suite: str, path: str, base: str, mode: str | None = None) -> dict[str, Any]:
        test_name = Path(path).stem
        case = base.split("::", 1)[1] if "::" in base else ""
        index = 0
        m = re.fullmatch(r"(.*)\.(\d+)", case)
        if m:
            index = int(m.group(2))
        cache_key = f"{mode or self.mode}:{suite}/{test_name}.{index}"
        if cache_key not in self._static_cache:
            cores, mem = 2.0, self.static_mem("cpp", STATIC_MEM_CPP, mode)
            custom_args = self._suite_config(suite).get("custom_args", {}).get(test_name)
            if custom_args:
                args = custom_args[index - 1] if index and index <= len(custom_args) else custom_args[0]
                c, mm = parse_seastar_args(args)
                cores = c if c is not None else cores
                # -m is a ceiling the test is unlikely to reach, so it only ever lowers
                # the guess: a -m256M case cannot take more than that, whatever the p90 is.
                if mm is not None:
                    mem = min(mem, mm + 0.1 * GB)
            self._static_cache[cache_key] = {"cores": cores, "mem": mem, "wall": 2.0, "source": "static-cpp"}
        return self._static_cache[cache_key]

    def _suite_config(self, suite: str) -> dict[str, Any]:
        cache_key = f"cfg:{suite}"
        if cache_key not in self._static_cache:
            cfg: dict[str, Any] = {}
            cfg_path = TOP_SRC_DIR / "test" / suite / "test_config.yaml"
            try:
                cfg = yaml.safe_load(cfg_path.read_text()) or {}
            except OSError:
                pass
            self._static_cache[cache_key] = cfg
        return self._static_cache[cache_key]


def _ema(old: float | None, new: float, n: int) -> float:
    if old is None or n == 0:
        return float(new)
    return (1 - EMA_ALPHA) * float(old) + EMA_ALPHA * float(new)


# ---------------------------------------------------------------------------
# Live machine readings
# ---------------------------------------------------------------------------


class CgroupReader:
    """Reads the live memory (bytes) of a worker's cgroup from the controller."""

    def __init__(self, cgroup_tests: Path | None):
        self.base = cgroup_tests
        # worker -> cgroups of the containers it started (test/pylib/container_accounting.py)
        self._containers: dict[str, list[Path]] = {}
        self._mem: dict[str, float | None] = {}             # worker -> memory() this pass

    def refresh(self, worker_ids: Iterable[str]) -> None:
        if self.base is None:
            return
        self._containers = all_container_cgroups()
        self._mem = {}

    def memory(self, wid: str) -> float | None:
        """A worker's anonymous memory, the containers it started included.

        Anonymous, like the peaks the profile learns (resource_gather's anon_peak), and read
        by the same function: the forecast subtracts one from the other, and counting the
        worker's mapped file pages on one side only -- the Scylla binary, in debug a
        gigabyte -- read them as growth the test had already made.  Read once per pass in
        refresh(): selection asks for it per candidate and per worker, many times a pass.
        """
        if self.base is None:
            return None
        if wid not in self._mem:
            self._mem[wid] = worker_anon(self.base / wid, self._containers.get(wid, ()))
        return self._mem[wid]

# ---------------------------------------------------------------------------
# The scheduler
# ---------------------------------------------------------------------------

class BudgetScheduling:
    """xdist scheduler that admits tests against a CPU and RAM budget.

    Bookkeeping per worker: ``node2pending[node]`` is the list of indices sent to
    it, in order.  By the worker protocol every index except the last one is
    *committed* (running or guaranteed to run next); the last one is *held*
    (the worker is blocked waiting for its successor).  Sending a successor or
    a shutdown commits the held item, and that is the moment its cost is charged.
    """

    def __init__(self, config: pytest.Config, log: Any = None, *, model: CostModel | None = None,
                 ncpus: int | None = None, mem_total: float | None = None, cgroup_tests: Path | None = None,
                 now: Any = time.monotonic, available_fn: Any = None):
        from xdist.workermanage import parse_tx_spec_config
        self.config = config
        self.numnodes = len(parse_tx_spec_config(config))
        self.log = log.budgetsched if log is not None else logger.debug
        self.now = now

        self.ncpus = ncpus or len(os.sched_getaffinity(0))
        self._available = available_fn or (lambda: psutil.virtual_memory().available)
        opt = config.getoption
        self.depth = max(1, int(opt("--budget-depth")))
        self.cpu_target = float(opt("--budget-cpu-target")) * self.ncpus
        total = mem_total if mem_total is not None else psutil.virtual_memory().total
        # What the tests may reserve: the memory that is actually free when the run
        # starts.  Not the memory the machine has - if something else
        # already holds a third of the box, handing that third out to tests only means
        # reclaiming page cache and then swapping.  Measured once, at startup, so the
        # budget is a fixed number the reservations add up against rather than a moving
        # target; the live check in admission then catches what changes underneath.
        # A first cut, before the workers exist; replaced by a measurement once they do.
        free_now = self._available()
        self.mem_target = max(1 * GB, free_now)
        self.mem_reserve = min(MEM_RESERVE_MAX, max(MEM_RESERVE_MIN, MEM_RESERVE_FRACTION * total))
        self._fc_mean = 0.0                            # forecast growth still to come, over the running tests
        self._mem_charged_workers = False
        # Reservations: acquired when a test is admitted (committed), released on
        # completion or worker loss.  Invariant: sum(res_cpu) <= cpu_target at every point
        # after admission.  Memory is not reserved -- admission checks it against
        # the forecast -- and res_mem is kept only to be logged.
        self.res_cpu: dict[int, float] = {}
        self.res_mem: dict[int, float] = {}

        if model is None:
            default_cores, default_mem = opt("--budget-default-cost").split(",")
            modes = opt("--mode") or []
            model = CostModel(_profile_path(config), self.ncpus, k_sigma=float(opt("--budget-k-sigma")),
                              default_cost=(float(default_cores), parse_size(default_mem)),
                              mode=(modes[0] if modes else "release"))
        self.model = model
        self.live = CgroupReader(cgroup_tests if cgroup_tests is not None else _cgroup_tests_path())

        self.node2collection: dict[WorkerController, list[str]] = {}
        self.node2pending: dict[WorkerController, list[int]] = {}
        self.node_file: dict[WorkerController, str | None] = {}
        self.shutdown_sent: set[WorkerController] = set()
        self.collection: list[str] | None = None
        self.pending_set: set[int] = set()
        self.files: dict[str, list[int]] = {}          # file -> pending indices, longest wall first
        self.file_remaining: dict[str, float] = {}      # file -> sum of pending predicted wall
        # The wall each pending test was queued with.  In-run learning changes a test's
        # estimate while it waits, so the running totals must be undone with the number
        # they were built from, or they drift.
        self._queued_wall: dict[int, float] = {}
        self.committed_at: dict[int, float] = {}        # index -> monotonic time of commit
        self._costs: dict[int, Cost] = {}
        self._keys: dict[int, str] = {}                 # index -> profile key
        self.stats = defaultdict(int)
        self._tick_timer: threading.Timer | None = None
        self._stopped = False

        tmpdir = Path(opt("--tmpdir")).absolute()
        self._log_file = open(tmpdir / f"budget_scheduler_{HOST_ID}.txt", "a")
        self._log(f"budget scheduler v8.2 (reservations, work-domain phases): ncpus={self.ncpus} cpu_target={self.cpu_target:.1f} "
                  f"mem_target={self.mem_target / GB:.1f}G (of {total / GB:.0f}G total, "
                  f"{free_now / GB:.0f}G free at start) depth={self.depth} "
                  f"profile_entries={len(self.model.tests)}")

    # -- protocol: properties ----------------------------------------------

    @property
    def nodes(self) -> list[WorkerController]:
        return list(self.node2pending.keys())

    @property
    def collection_is_completed(self) -> bool:
        return len(self.node2collection) >= self.numnodes

    @property
    def tests_finished(self) -> bool:
        if not self.collection_is_completed or self.collection is None:
            return False
        if self.pending_set:
            return False
        for node, queued in self.node2pending.items():
            if queued and not node.shutting_down:
                return False
        return True

    @property
    def has_pending(self) -> bool:
        return bool(self.pending_set) or any(self.node2pending.values())

    # -- protocol: node lifecycle -------------------------------------------

    def add_node(self, node: WorkerController) -> None:
        assert node not in self.node2pending
        self.node2pending[node] = []
        self.node_file[node] = None

    def add_node_collection(self, node: WorkerController, collection: Sequence[str]) -> None:
        assert node in self.node2pending
        if self.collection_is_completed and self.collection is not None:
            if list(collection) != self.collection:
                from xdist.report import report_collection_diff
                other = next(iter(self.node2collection))
                self._log(report_collection_diff(self.collection, list(collection), other.gateway.id, node.gateway.id))
                return
        self.node2collection[node] = list(collection)

    def remove_node(self, node: WorkerController) -> str | None:
        queued = self.node2pending.pop(node, [])
        self.node_file.pop(node, None)
        was_shutdown = node in self.shutdown_sent
        self.shutdown_sent.discard(node)
        crashitem = None
        if queued:
            committed = queued if was_shutdown else queued[:-1]
            if committed:
                # The first committed item was running when the worker died.
                crashitem = self.collection[queued[0]]
                requeue = queued[1:]
            else:
                # Only a held item that never started: nothing crashed.
                requeue = queued
            for idx in queued:
                self.committed_at.pop(idx, None)
                self.res_cpu.pop(idx, None)
                self.res_mem.pop(idx, None)
            for idx in requeue:
                self._add_pending(idx, front=True)
        self.check_schedule()
        return crashitem

    # -- protocol: test lifecycle -------------------------------------------

    def mark_test_complete(self, node: WorkerController, item_index: int, duration: float | None = None) -> None:
        queued = self.node2pending.get(node)
        if queued is not None and item_index in queued:
            queued.remove(item_index)
        self.committed_at.pop(item_index, None)
        self.res_cpu.pop(item_index, None)
        self.res_mem.pop(item_index, None)
        self.stats["completed"] += 1
        self.check_schedule()

    def mark_test_pending(self, item: str) -> None:
        assert self.collection is not None
        self._add_pending(self.collection.index(item), front=True)
        self.check_schedule()

    def remove_pending_tests_from_node(self, node: WorkerController, indices: Sequence[int]) -> None:
        # We never send `steal`, so this is not expected.
        pass

    def schedule(self) -> None:
        assert self.collection_is_completed
        if self.collection is not None:
            self.check_schedule()
            return
        if not self._check_nodes_have_same_collection():
            self._log("**Different tests collected, aborting run**")
            return
        # A test's reservation is the peak memory of its worker's cgroup, so a *running*
        # worker is paid for.  A worker that is only holding a test is not, and there is one
        # of those per worker all run long.  Charge them once, now that we know how many
        # there are, or the budget is over by that much on a machine with many workers.
        if not self._mem_charged_workers:
            self._mem_charged_workers = True
            # Every worker is up and has finished collecting, so whatever they cost is
            # already spent and already visible.  Measure what is left instead of
            # guessing what they took: the guess was a per-worker constant, and at two
            # workers per CPU a constant that is wrong by 50 MB is wrong by gigabytes.
            free_now = self._available()
            old_target = self.mem_target
            self.mem_target = max(1 * GB, free_now)
            self._log(f"{len(self.node2pending)} workers up, {free_now / GB:.1f}G still free; memory budget "
                      f"{old_target / GB:.1f}G -> {self.mem_target / GB:.1f}G")
        self.collection = next(iter(self.node2collection.values()))
        for idx in range(len(self.collection)):
            self._add_pending(idx)
        self._log(f"scheduling {len(self.collection)} tests over {len(self.node2pending)} workers; "
                  f"cost sources: {dict(self._source_histogram())}")
        self._start_tick()
        self.check_schedule()

    # -- core ---------------------------------------------------------------

    def check_schedule(self) -> None:
        if self.collection is None or self._stopped:
            return
        nodes = [n for n in self.node2pending if not n.shutting_down]
        self.live.refresh(n.gateway.id for n in nodes)
        self._refresh_forecasts()

        # Workers that are idle with a held test come first: they can start right away.
        def prio(n: WorkerController) -> tuple[int, float]:
            held = self._held(n)
            return (len(self._committed(n)), self._costs_for(held).cores if held is not None else math.inf)
        nodes.sort(key=prio)

        admitted_any = False
        # Two passes: first give every idle worker one held test (so backfilled
        # tests land on idle workers, not behind a running one), then commit held
        # tests and assign successors.
        for phase in ("prime", "fill"):
            for node in nodes:
                if node in self.shutdown_sent:
                    # Retired earlier in this pass: anything sent now arrives after the
                    # shutdown, and the worker exits with it queued -- xdist calls that a crash.
                    continue
                if phase == "prime" and self.node2pending[node]:
                    continue
                limit = 1 if phase == "prime" else self.depth + 1
                while len(self.node2pending[node]) < limit:
                    held = self._held(node)
                    if held is not None and not self._fits(held, node):
                        self.stats["held_waiting"] += 1
                        break
                    cand = self._pick(node)
                    if cand is None:
                        if held is not None:
                            # Nothing left to send: shutdown commits the held item.
                            self._commit(held, node)
                            node.shutdown()
                            self.shutdown_sent.add(node)
                        elif not self.pending_set and not self.node2pending[node]:
                            node.shutdown()
                            self.shutdown_sent.add(node)
                        break
                    if held is not None:
                        self._commit(held, node)
                        admitted_any = True
                    self._send(node, cand)
                    if held is None:
                        # Just primed an idle worker; the new item is held, loop to try to commit it.
                        continue
        if not admitted_any and not self.committed_at:
            # Nothing runs anywhere: never dead-lock on an over-sized test.
            held_nodes = [n for n in nodes if self._held(n) is not None and n not in self.shutdown_sent]
            if held_nodes:
                node = min(held_nodes, key=lambda n: self._costs_for(self._held(n)).cores)
                held = self._held(node)
                self._log(f"progress rule: forcing {self.collection[held]} on {node.gateway.id}")
                self.stats["forced"] += 1
                cand = self._pick(node)
                self._commit(held, node)
                if cand is None:
                    node.shutdown()
                    self.shutdown_sent.add(node)
                else:
                    self._send(node, cand)

    def _fits(self, idx: int, node: WorkerController) -> bool:
        """Admission.

        RAM: what the running tests are still forecast to take, plus this test's forecast
        peak, must fit what the machine has available less the reserve (see _forecast).
        CPU: the reservations (natural parallelism) of what runs, plus this test's, must
        stay within the CPU target.
        """
        cost = self._costs_for(idx)
        # --- RAM: the running tests' forecast growth plus this test must fit what is free --
        # (less the reserve).
        if self._forecast_need(idx, node) > self._available() - self.mem_reserve:
            self.stats["rejected_mem"] += 1
            return False

        # --- CPU: reservations within the target ---------------------------------
        req = cost.cores + self._setup_for(idx, node)
        if sum(self.res_cpu.values()) + req > self.cpu_target:
            self.stats["rejected_cpu"] += 1
            return False
        return True

    def _free_capacity(self, node: WorkerController | None = None) -> tuple[float, float]:
        """(cores, bytes) a *selection* may still count on.

        Running reservations are not the whole picture here.  Every other worker is
        already holding a test that has been chosen and will start as soon as the
        machine lets it, and a test sent to a worker cannot be recalled.  If selection
        ignored those, each worker would choose as though the machine were free, they
        would all pick something heavy at once, and the ones that lose the race would
        sit blocked.  So a pick sees what runs, plus what its colleagues are about to run.
        """
        hc = hm = 0.0
        for other, queued in self.node2pending.items():
            if other is node or not queued:
                continue
            idx = queued[-1]
            # chosen, not started yet
            if idx not in self.committed_at:
                hc += self._costs_for(idx).cores
                hm += self._forecast_new(idx, other)
        return (self.ncpus - sum(self.res_cpu.values()) - hc, self._mem_headroom() - hm)

    def _mem_headroom(self) -> float:
        """Memory a new test may still take: available now, less the running tests' forecast growth, less the reserve."""
        return self._available() - self.mem_reserve - self._fc_mean

    def _forecast(self, idx: int, held: float) -> float:
        """Expected peak of a test, given what it holds now: its price, or more once it has taken more."""
        return max(self._costs_for(idx).mem, held)

    def _refresh_forecasts(self) -> None:
        """Forecast growth of every running test, from what each one holds now."""
        self._fc_mean = 0.0
        running: list[tuple[int, float]] = []
        for node, queued in self.node2pending.items():
            live = self.live.memory(node.gateway.id) or 0.0
            first = True
            for idx in queued:
                if idx not in self.committed_at:
                    continue
                # Only the first committed test on a worker is running.  What it holds is its
                # worker's whole cgroup, the worker's own interpreter and whatever its module
                # keeps alive included: that is what a learned peak measures too.
                held = live if first else 0.0
                first = False
                running.append((idx, held))
        for idx, held in running:
            self._fc_mean += max(0.0, self._forecast(idx, held) - held)

    def _forecast_new(self, idx: int, node: WorkerController | None = None) -> float:
        """What a test that has not started will add.

        A peak is learned as the whole worker cgroup at its highest: the worker's interpreter
        and whatever its module keeps alive included.  That part is already resident and
        already out of MemAvailable, so on a known worker only the peak beyond what the
        worker holds now is new.  A new worker's own overhead is charged separately.
        """
        peak = self._forecast(idx, 0.0)
        if node is None:
            return peak
        return max(0.0, peak - (self.live.memory(node.gateway.id) or 0.0))

    def _forecast_need(self, idx: int, node: WorkerController | None = None) -> float:
        """Memory the running tests are still going to take, with this one added."""
        return self._fc_mean + self._forecast_new(idx, node)

    # -- selection ------------------------------------------------------------

    def _pick(self, node: WorkerController) -> int | None:
        """Next test for this worker.

        Priority order is kept: same file first (longest first), else the file
        with the most remaining work.  Backfill: if a test in that order does not
        fit the capacity free right now, scan past it (bounded) for one that does,
        so an unfittable big test does not idle the worker.  A passed-over test is not
        reserved for: no test has a deadline and everything runs before the session ends.
        """
        if not self.pending_set:
            return None
        # Longest test first, globally.  Ordering by a file's *total* remaining work put a
        # 139-second test in a small file at the back of the queue, so it started when the
        # machine had drained and then ran alone.  Each file's list is longest-first, so its
        # head is its longest test: order the files by that.
        by_remaining = sorted(self.files, key=lambda f: -self._costs_for(self.files[f][0]).wall)
        free_cpu, free_mem = self._free_capacity(node)
        # Selection has to be as strict about memory as admission is.  A test sent to a
        # worker cannot be recalled: the worker holds it until it can start.  Picking one
        # that admission will refuse parks that worker for as long as the refusal lasts.
        current = self.node_file.get(node)
        files = ([current] if current is not None and self.files.get(current) else []) + [f for f in by_remaining if f != current]
        head = None
        scanned = 0
        chosen = None
        smallest = None            # cheapest candidate seen, for when nothing fits right now
        for f in files:
            for idx in self.files[f]:
                if head is None:
                    head = idx
                c = self._costs_for(idx)
                if c.cores <= free_cpu and self._forecast_new(idx, node) <= free_mem:
                    chosen = (f, idx)
                    break
                if smallest is None or (c.mem, c.cores) < (self._costs_for(smallest[1]).mem,
                                                           self._costs_for(smallest[1]).cores):
                    smallest = (f, idx)
                scanned += 1
                if scanned >= 64:
                    break
            if chosen is not None or scanned >= 64:
                break
        if chosen is None:
            # The scan walks the queue longest-first and stops after 64 candidates, so on
            # a tight machine it can miss the short tests further down.  Each file's list
            # is longest-first, so its last entry is its cheapest: look there before
            # giving up.
            for f, lst in self.files.items():
                idx = lst[-1]
                c = self._costs_for(idx)
                if c.cores <= free_cpu and self._forecast_new(idx, node) <= free_mem:
                    chosen = (f, idx)
                    break
                if smallest is None or (c.mem, c.cores) < (self._costs_for(smallest[1]).mem,
                                                           self._costs_for(smallest[1]).cores):
                    smallest = (f, idx)
        if chosen is None:
            # Nothing fits the capacity free right now.  Park the worker on the cheapest
            # test we saw, not on the head: a held test is off the queue and blocks its
            # worker until it fits, and the head is the biggest test there is.  A worker
            # blocked for the whole run on a test that needs half the machine's memory is
            # a worker lost.
            if smallest is None and head is not None:
                smallest = (self._file_of(head), head)
            if smallest is None:
                return None
            chosen = smallest
            self.stats["parked_smallest"] += 1
        if chosen[1] != head:
            self.stats["backfilled"] += 1
        return self._take(*chosen)

    def _take(self, file: str, idx: int | None = None) -> int:
        lst = self.files[file]
        if idx is None:
            idx = lst.pop(0)
        else:
            lst.remove(idx)
        self.pending_set.discard(idx)
        wall = self._queued_wall.pop(idx, self._costs_for(idx).wall)
        self.file_remaining[file] -= wall
        if not lst:
            del self.files[file]
            del self.file_remaining[file]
        return idx

    def _add_pending(self, idx: int, front: bool = False) -> None:
        file = self._file_of(idx)
        lst = self.files.setdefault(file, [])
        wall = self._costs_for(idx).wall
        if front:
            lst.insert(0, idx)
        else:
            # keep longest-first order
            pos = 0
            while pos < len(lst) and self._costs_for(lst[pos]).wall >= wall:
                pos += 1
            lst.insert(pos, idx)
        self.file_remaining[file] = self.file_remaining.get(file, 0.0) + wall
        self._queued_wall[idx] = wall
        self.pending_set.add(idx)

    # -- helpers ------------------------------------------------------------------

    def _held(self, node: WorkerController) -> int | None:
        queued = self.node2pending.get(node) or []
        if queued and node not in self.shutdown_sent:
            return queued[-1]
        return None

    def _committed(self, node: WorkerController) -> list[int]:
        queued = self.node2pending.get(node) or []
        return queued if node in self.shutdown_sent else queued[:-1]

    def _commit(self, idx: int, node: WorkerController) -> None:
        """Admit: acquire the reservations atomically with the decision (single scheduler thread)."""
        cost = self._costs_for(idx)
        fc_mean = self._forecast_new(idx, node)
        self.committed_at[idx] = self.now()
        self._fc_mean += fc_mean
        self.res_cpu[idx] = cost.cores
        self.res_mem[idx] = cost.mem
        self.stats["admitted"] += 1
        self._log(f"start {node.gateway.id} {self.collection[idx]} cores={cost.cores:.2f} "
                  f"mem={cost.mem / GB:.2f}G forecast={fc_mean / GB:.2f}G wall={cost.wall:.1f}s src={cost.source} "
                  f"reserved={sum(self.res_cpu.values()):.1f}/{self.cpu_target:.1f} "
                  f"mem_reserved={sum(self.res_mem.values()) / GB:.1f}/{self.mem_target / GB:.1f}G")

    def _send(self, node: WorkerController, idx: int) -> None:
        self.node2pending[node].append(idx)
        self.node_file[node] = self._file_of(idx)
        node.send_runtest_some([idx])

    def _file_of(self, idx: int) -> str:
        return file_of_key(self._key_of(idx))

    def _key_of(self, idx: int) -> str:
        key = self._keys.get(idx)
        if key is None:
            key = self._keys[idx] = profile_key(self.collection[idx])
        return key

    def _costs_for(self, idx: int) -> Cost:
        cost = self._costs.get(idx)
        if cost is None:
            cost = self._costs[idx] = self.model.cost(self.collection[idx])
        return cost

    def _setup_for(self, idx: int, node: WorkerController) -> float:
        if self.node_file.get(node) == self._file_of(idx):
            return 0.0
        return self.model.setup_cost(self.collection[idx])

    def _source_histogram(self) -> dict[str, int]:
        hist: dict[str, int] = defaultdict(int)
        for idx in range(len(self.collection or [])):
            hist[self._costs_for(idx).source] += 1
        return hist

    def _check_nodes_have_same_collection(self) -> bool:
        from xdist.report import report_collection_diff
        items = list(self.node2collection.items())
        first_node, col = items[0]
        same = True
        for node, collection in items[1:]:
            msg = report_collection_diff(col, collection, first_node.gateway.id, node.gateway.id)
            if msg:
                same = False
                self._log(msg)
                rep = pytest.CollectReport(nodeid=node.gateway.id, outcome="failed", longrepr=msg, result=[])
                self.config.hook.pytest_collectreport(report=rep)
        return same

    # -- periodic tick (runs on the DSession main loop thread) -------------------------

    def _start_tick(self) -> None:
        dsession = self.config.pluginmanager.getplugin("dsession")
        if dsession is None or not hasattr(dsession, "queue"):
            return
        sched = self

        def worker_budget_tick(**kwargs: Any) -> None:
            sched.check_schedule()

        dsession.worker_budget_tick = worker_budget_tick

        def fire() -> None:
            if self._stopped:
                return
            try:
                dsession.queue.put(("budget_tick", {}))
            except Exception:  # pragma: no cover - never let the timer thread die loudly
                pass
            self._tick_timer = threading.Timer(1.0, fire)
            self._tick_timer.daemon = True
            self._tick_timer.start()

        fire()

    def _stop_tick(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._tick_timer is not None:
            self._tick_timer.cancel()
        self._log(f"done: {dict(self.stats)}")
        try:
            self._log_file.flush()
        except Exception:
            pass

    # -- logging ------------------------------------------------------------------------

    def _log(self, msg: str) -> None:
        try:
            self._log_file.write(f"{time.strftime('%H:%M:%S')} {msg}\n")
            self._log_file.flush()
        except Exception:
            pass
        self.log(msg)

# ---------------------------------------------------------------------------
# Profile merge at the end of a run
# ---------------------------------------------------------------------------

def _profile_path(config: pytest.Config) -> Path:
    explicit = config.getoption("--budget-profile")
    if explicit:
        return Path(explicit).absolute()
    return Path(config.getoption("--tmpdir")).absolute() / PROFILE_FILENAME


def _cgroup_tests_path() -> Path | None:
    try:
        from test.pylib.resource_gather import CGROUP_TESTS
        return CGROUP_TESTS if CGROUP_TESTS.exists() else None
    except Exception:
        return None


def samples_path(tmpdir: Path) -> Path:
    return tmpdir / f"budget_samples_{HOST_ID}.jsonl"


def append_sample(tmpdir: Path, sample: dict[str, Any]) -> None:
    line = json.dumps(sample, separators=(",", ":")) + "\n"
    with open(samples_path(tmpdir), "a") as f:
        f.write(line)   # < PIPE_BUF with O_APPEND: atomic across workers


def load_samples(path: Path) -> list[dict[str, Any]]:
    samples = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        samples.append(json.loads(line))
                    except ValueError:
                        pass
    except OSError:
        pass
    return samples


def merge_run_into_profile(tmpdir: Path, profile_path: Path, ncpus: int) -> int:
    """Merge this run's samples into the profile, then remove them.

    The samples file is named after the host id, which is fixed when SCYLLA_TEST_HOST_ID
    is set; a file left behind would be merged again by every later run, counting the
    same samples over and over.
    """
    path = samples_path(tmpdir)
    samples = load_samples(path)
    if not samples:
        return 0
    model = CostModel(profile_path, ncpus)
    for sample in samples:
        model.learn(sample)
    model.save()
    path.unlink(missing_ok=True)
    return len(samples)


# ---------------------------------------------------------------------------
# pytest plugin hooks (controller side)
# ---------------------------------------------------------------------------

_scheduler: BudgetScheduling | None = None


@pytest.hookimpl(optionalhook=True)
def pytest_xdist_make_scheduler(config: pytest.Config, log: Any) -> Any:
    global _scheduler
    if not config.getoption("--budget-scheduler"):
        return None
    _scheduler = BudgetScheduling(config, log)
    return _scheduler


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session) -> None:
    if session.config.getoption("--collect-only"):
        return
    if os.environ.get("PYTEST_XDIST_WORKER"):
        return
    if _scheduler is not None:
        # Not from tests_finished(): that predicate turns true while the last worker is
        # still running its test, and stopping there would end the diagnostics (and the
        # loop) before the run does.
        _scheduler._stop_tick()
    try:
        tmpdir = Path(session.config.getoption("--tmpdir")).absolute()
        ncpus = len(os.sched_getaffinity(0))
        n = merge_run_into_profile(tmpdir, _profile_path(session.config), ncpus)
        if n:
            logger.info("budget: merged %d samples into %s", n, _profile_path(session.config))
    except Exception as e:
        logger.warning("budget: profile merge failed: %s", e)
