#!/usr/bin/env bash
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Model-checks the TLA+ specifications in docs/dev/tla with TLC.
#
# Each model configuration (MC*.cfg) states its expected outcome in a
# header line:
#
#   \* expect: pass                  no invariant is violated
#   \* expect: <InvariantName>       TLC finds a violation of that invariant
#
# and, if it is cheap enough to run on every change:
#
#   \* ci: yes
#
# Usage: scripts/tla/check.sh [--all] [--workers N] [MC*.cfg ...]
#
# With no configuration given, runs the ones marked "ci: yes", or all of
# them with --all.  Exits non-zero if an outcome differs from the expected
# one.  TLC output goes to build/tla/out/.  Extra JVM options (e.g. -Xmx8g)
# can be passed in TLC_JAVA_OPTS.

set -euo pipefail

TLA_VERSION=1.7.4
TLA_SHA256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88

top=$(cd "$(dirname "$0")/../.." && pwd)
jar=${TLA2TOOLS:-$top/build/tla/tla2tools-$TLA_VERSION.jar}
outdir=$top/build/tla/out
workers=auto
all=false
cfgs=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --all) all=true; shift ;;
        --workers) workers=$2; shift 2 ;;
        -h|--help) sed -n '/^# Model-checks/,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) cfgs+=("$(realpath "$1")"); shift ;;
    esac
done

if [[ ! -f $jar ]]; then
    mkdir -p "$(dirname "$jar")"
    echo "Downloading tla2tools.jar $TLA_VERSION to $jar"
    curl -fsSL -o "$jar.tmp" "https://github.com/tlaplus/tlaplus/releases/download/v$TLA_VERSION/tla2tools.jar"
    echo "$TLA_SHA256  $jar.tmp" | sha256sum -c --quiet
    mv "$jar.tmp" "$jar"
fi

if [[ ${#cfgs[@]} -eq 0 ]]; then
    for cfg in "$top"/docs/dev/tla/*/MC*.cfg; do
        if $all || grep -q '^\\\* ci: yes' "$cfg"; then
            cfgs+=("$cfg")
        fi
    done
fi

mkdir -p "$outdir"
failed=0
for cfg in "${cfgs[@]}"; do
    dir=$(dirname "$cfg")
    spec=$dir/$(basename "$dir").tla
    name=$(basename "$dir")/$(basename "$cfg" .cfg)
    out=$outdir/${name//\//.}.out
    expect=$(sed -n 's/^\\\* expect: *//p' "$cfg")
    if [[ -z $expect ]]; then
        echo "$name: no '\\* expect:' line" >&2
        failed=1
        continue
    fi

    start=$SECONDS
    # TLC extracts its standard modules to java.io.tmpdir; a private one keeps
    # concurrent runs from reading each other's half-written files. It also
    # keeps its state queue there, which grows to gigabytes: keep it out of
    # /tmp, which may be in memory.
    tmpdir=$(mktemp -d -p "$top/build/tla")
    (cd "$dir" && java ${TLC_JAVA_OPTS:-} -Djava.io.tmpdir="$tmpdir" -XX:+UseParallelGC -cp "$jar" \
        tlc2.TLC -deadlock -checkpoint 0 -workers "$workers" -metadir "$tmpdir/states" \
        -config "$(basename "$cfg")" "$(basename "$spec")") > "$out" 2>&1 || true
    rm -rf "$tmpdir"

    if grep -q 'Model checking completed. No error has been found.' "$out"; then
        result=pass
    elif result=$(grep -oP '^Error: Invariant \K\S+(?= is violated)' "$out"); then
        :
    else
        result="error (see $out)"
    fi
    states=$(grep -oP '^\d[\d,]* states generated, \K[\d,]+(?= distinct)' "$out" | tail -1 || true)

    if [[ $result == "$expect" ]]; then
        verdict=ok
    else
        verdict=UNEXPECTED
        failed=1
    fi
    printf '%-60s %-11s expected %-22s got %-22s %8ss %s distinct states\n' \
        "$name" "$verdict" "$expect" "$result" "$((SECONDS - start))" "${states:-?}"
done
exit $failed
