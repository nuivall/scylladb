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
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import shlex
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

from test import HOST_ID, TOP_SRC_DIR

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
# Profile merge at the end of a run
# ---------------------------------------------------------------------------

def _profile_path(config: pytest.Config) -> Path:
    explicit = config.getoption("--budget-profile")
    if explicit:
        return Path(explicit).absolute()
    return Path(config.getoption("--tmpdir")).absolute() / PROFILE_FILENAME


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

@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session) -> None:
    if session.config.getoption("--collect-only"):
        return
    if os.environ.get("PYTEST_XDIST_WORKER"):
        return
    try:
        tmpdir = Path(session.config.getoption("--tmpdir")).absolute()
        ncpus = len(os.sched_getaffinity(0))
        n = merge_run_into_profile(tmpdir, _profile_path(session.config), ncpus)
        if n:
            logger.info("budget: merged %d samples into %s", n, _profile_path(session.config))
    except Exception as e:
        logger.warning("budget: profile merge failed: %s", e)
