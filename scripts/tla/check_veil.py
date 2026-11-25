#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Run the Veil models in docs/dev/tla/veil and compare each check with its expected outcome.

Every check command in a model (#model_check, #check_invariants, #simulate, #temporal_check,
#updr, sat trace, unsat trace) is preceded by comment lines that state its expected outcome:

    -- expect: pass                 no violation found, or every proof goal holds
    -- expect: violation <name>     the checker reports a violation of safety property <name>
    -- expect: not inductive <action>:<invariant>, ...
                                    #check_invariants fails these goals and no others
                                    (`init` for the initial state)
    -- expect: error <text>         the command fails with another error that contains <text>

and, if it takes more than a few minutes or more memory than a laptop has:

    -- cost: heavy

Heavy checks run only with --all; the others are commented out in a copy of the model.

A model also names the code it describes, and the commit of that code it was last checked
against by reading:

    -- models: replica/table.cc, compaction/
    -- checked-against: <commit>

--stale lists the models whose code changed after that commit. Read the change, update the
model if it no longer matches, rerun it, and move checked-against to the commit you read.

Veil is used from a checkout of https://github.com/verse-lab/veil at VEIL_TAG, built with
`lake build`: $VEIL_DIR, or build/veil, cloned and built on first use. elan (`lake`) must be
on PATH.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

VEIL_REPO = 'https://github.com/verse-lab/veil'
VEIL_TAG = 'v4.35.0'

TOP = Path(__file__).resolve().parents[2]
MODELS = TOP / 'docs/dev/tla/veil'
WORK = TOP / 'build/veil-check'

COMMAND = re.compile(r'^(#model_check|#check_invariants|#simulate|#temporal_check|#updr|sat trace|unsat trace)\b')
DIAGNOSTIC = re.compile(r'^(?P<path>\S+\.lean):(?P<line>\d+):\d+: (?P<severity>error|warning): (?P<message>.*)$')
VIOLATION = re.compile(r'Violation: .*\(violates: (?P<name>[^)]+)\)')
GOAL = re.compile(r'^(?P<indent> +)(?P<name>\S+) \.\.\. ❌')
ACTION = re.compile(r'^  (?P<name>[A-Za-z_][\w.]*)$')


def failed_goals(message: str) -> list[str]:
    """The goals a #check_invariants report marks as failed, as action:invariant."""
    goals, action = [], 'init'
    for text in message.splitlines():
        if text.startswith('The following set of actions'):
            action = None
        elif (m := ACTION.match(text)) and action != 'init':
            action = m.group('name')
        elif m := GOAL.match(text):
            goals.append(f'{action if len(m.group("indent")) > 2 else "init"}:{m.group("name")}')
    return goals


@dataclass
class Check:
    line: int           # 1-based line of the command
    end: int            # last line of the command
    expect: str | None
    heavy: bool
    errors: list[str] = field(default_factory=list)

    def result(self) -> str:
        if not self.errors:
            return 'pass'
        m = VIOLATION.search(self.errors[0])
        if m:
            return f'violation {m.group("name")}'
        goals = [g for e in self.errors for g in failed_goals(e)]
        if goals:
            return f'not inductive {", ".join(goals)}'
        return f'error {self.errors[0].splitlines()[0]}'

    def matches(self) -> bool:
        got = self.result()
        if self.expect is None:
            return False
        if self.expect.startswith('error '):
            return got.startswith('error ') and self.expect[len('error '):] in got
        return got == self.expect


def parse(path: Path) -> tuple[list[str], list[Check]]:
    lines = path.read_text().splitlines()
    checks = []
    for i, text in enumerate(lines):
        if not COMMAND.match(text):
            continue
        expect, heavy = None, False
        j = i - 1
        while j >= 0 and lines[j].startswith('--'):
            comment = lines[j][2:].strip()
            if comment.startswith('expect:') and expect is None:
                expect = comment[len('expect:'):].strip()
            elif comment == 'cost: heavy':
                heavy = True
            j -= 1
        end = i
        while end + 1 < len(lines) and lines[end + 1].strip() and not lines[end + 1].startswith(('--', '#', 'end ')) \
                and not COMMAND.match(lines[end + 1]):
            end += 1
        checks.append(Check(line=i + 1, end=end + 1, expect=expect, heavy=heavy))
    return lines, checks


def header(lines: list[str], key: str) -> str | None:
    for text in lines:
        if text.startswith(f'-- {key}:'):
            return text.split(':', 1)[1].strip()
    return None


def ensure_veil() -> Path:
    veil = Path(os.environ.get('VEIL_DIR', TOP / 'build/veil'))
    if shutil.which('lake') is None:
        sys.exit('lake not found: install elan (https://github.com/leanprover/elan)')
    if not (veil / 'lakefile.lean').exists():
        print(f'Cloning Veil {VEIL_TAG} to {veil} and building it (this takes a while)', flush=True)
        subprocess.run(['git', 'clone', '-q', '--depth', '1', '--branch', VEIL_TAG, VEIL_REPO, str(veil)], check=True)
        subprocess.run(['lake', 'build'], cwd=veil, check=True)
    return veil


def run(veil: Path, model: Path, all_checks: bool, timeout: int) -> tuple[list[Check], str, float]:
    lines, checks = parse(model)
    skipped = set()
    for c in checks:
        if c.heavy and not all_checks:
            skipped.update(range(c.line, c.end + 1))
    copy = WORK / model.name
    copy.write_text(''.join(f'-- skipped: {t}\n' if n in skipped else f'{t}\n'
                            for n, t in enumerate(lines, start=1)))
    out_path = WORK / f'{model.stem}.out'
    start = time.monotonic()
    try:
        proc = subprocess.run(['lake', 'lean', str(copy), '--',
                               '-Dveil.modelChecker.maxStoredBuilds=0', '-Dveil.modelChecker.maxStoredCheckers=0'],
                              cwd=veil, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
        output = proc.stdout
    except subprocess.TimeoutExpired as e:
        output = (e.stdout or b'').decode() if isinstance(e.stdout, bytes) else (e.stdout or '')
        output += f'\n{copy}:1:0: error: timed out after {timeout} s\n'
    out_path.write_text(output)
    elapsed = time.monotonic() - start

    # A diagnostic's message continues on the lines up to the next diagnostic.
    stray, current = [], None
    for text in output.splitlines():
        m = DIAGNOSTIC.match(text)
        if not m:
            if current is not None:
                current[-1] += '\n' + text
            continue
        current = None
        if m.group('severity') != 'error':
            continue
        line = int(m.group('line'))
        owner = next((c for c in checks if c.line <= line <= c.end), None)
        if owner is None:
            stray.append(f'line {line}: {m.group("message")}')
        else:
            owner.errors.append(m.group('message'))
            current = owner.errors
    return [c for c in checks if c.line not in skipped], '; '.join(stray), elapsed


def stale(models: list[Path]) -> int:
    count = 0
    for model in models:
        lines = model.read_text().splitlines()
        paths, commit = header(lines, 'models'), header(lines, 'checked-against')
        if not paths or not commit:
            print(f'{model.name}: no "-- models:" or "-- checked-against:" line')
            count += 1
            continue
        paths = [p.strip() for p in paths.split(',')]
        log = subprocess.run(['git', '-C', str(TOP), 'log', '--oneline', '--no-merges', f'{commit}..HEAD', '--', *paths],
                             stdout=subprocess.PIPE, text=True, check=True).stdout.splitlines()
        if log:
            count += 1
            print(f'{model.name}: {len(log)} commits to {", ".join(paths)} after {commit[:11]}')
            for entry in log[:10]:
                print(f'    {entry}')
            if len(log) > 10:
                print(f'    ... and {len(log) - 10} more')
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('models', nargs='*', type=Path, help='models to run (default: every model)')
    parser.add_argument('--all', action='store_true', help='also run the checks marked "cost: heavy"')
    parser.add_argument('--list', action='store_true', help='list the checks and their expected outcomes, run nothing')
    parser.add_argument('--stale', action='store_true', help='list the models whose code changed, run nothing')
    parser.add_argument('--timeout', type=int, default=3600, help='seconds per model (default: %(default)s)')
    args = parser.parse_args()

    models = [m.resolve() for m in args.models] or sorted(MODELS.glob('*.lean'))
    if args.stale:
        return 1 if stale(models) else 0
    if args.list:
        for model in models:
            for c in parse(model)[1]:
                print(f'{model.name}:{c.line:<5} {"heavy" if c.heavy else "     "} {c.expect}')
        return 0

    veil = ensure_veil()
    WORK.mkdir(parents=True, exist_ok=True)
    failed = False
    for model in models:
        checks, stray, elapsed = run(veil, model, args.all, args.timeout)
        for c in checks:
            verdict = 'ok' if c.matches() else 'UNEXPECTED'
            failed |= verdict != 'ok'
            print(f'{model.name + ":" + str(c.line):<36} {verdict:<11} expected {str(c.expect)[:40]:<40} got {c.result()[:60]}')
        if stray:
            failed = True
            print(f'{model.name:<36} UNEXPECTED  errors outside any check: {stray[:200]}')
        print(f'{model.name:<36} {elapsed:.0f} s, output in {WORK / (model.stem + ".out")}', flush=True)
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
