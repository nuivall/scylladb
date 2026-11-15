#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Print a TLC counterexample compactly: each step's action and the variables it changed."""

import re
import sys


def parse_states(text: str) -> list[tuple[int, str, dict[str, str]]]:
    """
    Parse the counterexample in TLC's output.

    Args:
        text: TLC's output.

    Returns:
        For each state: its number, the action that led to it, and its variables (name to value, on one line).
    """
    states = []
    for block in re.split(r'\n(?=State \d+: )', text):
        m = re.match(r'State (\d+): <([^ >]+)[^\n]*\n', block)
        if not m:
            continue
        body = block[m.end():].split('\n\n')[0]
        variables = {}
        for part in re.split(r'\n(?=/\\ )', body):
            vm = re.match(r'/\\ (\w+) = (.*)', part, re.S)
            if vm:
                variables[vm.group(1)] = ' '.join(vm.group(2).split())
        states.append((int(m.group(1)), m.group(2), variables))
    return states


def main() -> None:
    """Print the counterexample in the TLC output file named on the command line."""
    text = open(sys.argv[1]).read()
    for line in text.splitlines():
        if line.startswith('Error:') and 'behavior' not in line:
            print(line)
    prev = {}
    for num, action, variables in parse_states(text):
        changed = {k: v for k, v in variables.items() if prev.get(k) != v}
        print(f'{num:3} {action}')
        for k, v in changed.items():
            print(f'      {k} = {v}')
        prev = variables


if __name__ == '__main__':
    main()
