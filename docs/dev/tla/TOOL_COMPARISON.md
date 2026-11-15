# TLC, Apalache, TLAPS and Veil: what we learned

**Conclusion:** Veil does every job the other three tools do on our models,
at TLC's speed or better per thread, so from 2026-11-14 it is the single
checker for new work, both to find bugs and to check or prove their fixes.

This report records the comparison made during the TLA+ verification
experiment (branch `name-tla-verification-experiment`, commits `8ef131b8747`
to `b590b9624ae`). The per-spec findings are in `README.md` and
`FINDINGS.md`; this file is only about the tools.

## 1. What was compared

| tool | version | kind | used for |
|---|---|---|---|
| TLC | 1.7.4 (`tla2tools.jar`, pinned in `scripts/tla/check.sh`) | explicit-state model checker | every spec in this directory |
| Apalache | 0.62.4 (`apalache-mc`, in `~/tools`) | bounded symbolic checker, SMT over Z3 | commitlog_cleanup (#10), vnodes_tablets_migration (#7) |
| TLAPS | tlapm 1.6.0-pre (Zenon, Z3, LS4; in `~/tools`) | TLA+ proof system | commitlog_cleanup (#10) |
| Veil | verse-lab/veil tag v4.35.0, Lean 4.35.0 ("Veil 2.0", in `~/veil` on the bench) | Lean 4 framework: explicit-state checker, simulator, SMT verification, invariant inference, temporal checks, kernel-checked proofs | #10, #7, counters_tablets (#11) |

The question: which one tool should find a bug and then check its fix?

Models used, and why:

- **commitlog_cleanup (#10)**: small (5 actions), a short bug (7 steps), and
  a fix with a clean inductive invariant. Good for proofs, and its state
  count grows fast with the number of writes (121K, 2.2M, 46M states at 5,
  6, 7 writes), which makes it a good scaling test.
- **vnodes_tablets_migration (#7)**: a large spec (11 actions, several
  bounded counters, functions over nodes and ranges), a longer bug (12
  steps), and 17M states for the largest fix config. It is representative of
  the specs we write for topology protocols.
- **counters_tablets (#11)**: only for Veil, to test a second translation
  (with Veil's own `tla-to-veil` skill) against TLC.

The bug and the fix of each pair sit in one spec behind a constant:
`CurrentColumn = FALSE / TRUE` for #7, `Fixed = FALSE / TRUE` for #10.

## 2. How validity was established

A time means nothing unless every tool checks the same model. Rules used:

- Veil copies are action-for-action translations
  (`veil/VnodesTabletsMigration.lean`, `veil/CommitlogCleanupBench.lean`).
  Encoding changes are listed in each file's header and don't change the
  reachable states.
- **Veil explored exactly TLC's distinct state count on every fix config**
  (288,498; 5,428,990; 17,430,982 for #7; 121,762; 2,195,630; 46,135,630 for
  #10). On bug configs neither tool explores the full space, so the check is
  the same violated invariant and the same counterexample length.
- **Counterexample lengths match in all three tools**: 12 steps for #7,
  7 steps for #10 (Apalache's last state in the log is `State 12` and
  `State 7`).
- Apalache copies (`apalache/apa_*.tla`) add `@type` annotations, and replace
  one `CHOOSE` (vnodes) and `Max` (commitlog, as `ApaFoldSet`). Constants are
  set by `CInit*` operators that mirror the TLC cfgs.
- Apalache ran with `--length` at least the counterexample length for a bug,
  and equal to the diameter TLC reports for a fix, so that a pass covers every
  reachable state. A run that stops before the diameter is not a pass.
- For counters_tablets, Veil found 20 states on `MC_no_migration` (TLC: 20)
  and the same trace on `MC_migration`.

## 3. Results

Bench host: 32 cores, 62 GB, one job at a time. Raw data:
`~/bench3/results.txt` and the per-run logs next to it.

- TLC: `scripts/tla/check.sh`, which runs `-workers auto` (32 on the
  bench), with `TLC_JAVA_OPTS=-Xmx40g`. The time includes JVM startup.
- Veil: compiled checker, built once with `veil.modelChecker.buildOnly`, then
  the binary run alone on 1 thread (no arguments) and on 32 threads
  (arguments `32 20 1`). Times are the binary's run only.
- Apalache: `--no-deadlock`, `timeout 1800` (30 minutes) per run.
- Memory is peak RSS (`/usr/bin/time`).

| model | states | TLC, 32 threads | Veil, 32 threads | Veil, 1 thread | Apalache |
|---|---|---|---|---|---|
| #7 bug, `MC_before_current_column` | (12 steps) | 1.2 s * | 0.2 s | 0.3 s | 17 s, 1.0 GB |
| #7 bug, `MC_bench_bug_large` | (12 steps) | 5.4 s, 14.6 GB | 4.8 s, 2.1 GB | 9.2 s, 1.1 GB | 497 s, 1.1 GB |
| #7 fix, `MC_current` | 288,498 | 1.5 s * | 0.4 s | 0.6 s | pass at the diameter (21 steps), 22 min, 1.1 GB |
| #7 fix, `MC_topology_ops` | 5,428,990 | 9.6 s, 15 GB | 8.9 s, 1.4 GB | 16.0 s, 0.8 GB | not run (no slot left in the time budget) |
| #7 fix, `MC_bench_fix_large` | 17,430,982 | 27.1 s, 16 GB | 24.6 s, 3.7 GB | 52.1 s, 2.4 GB | not run (job queued, never started) |
| #10 bug, 5 / 6 / 7 writes | (7 steps) | 0.7 / 0.8 / 0.8 s | 0.1 s | 0.1 s | 2.9 / 5.7 / 5.6 s, 0.5 GB |
| #10 fix, 5 writes | 121,762 | 1.0 s, 1.0 GB | 0.1 s | 0.2 s | timeout at step 11 of 15 |
| #10 fix, 6 writes | 2,195,630 | 3.1 s, 7.2 GB | 1.9 s, 0.7 GB | 4.0 s, 0.4 GB | killed during setup to free the bench |
| #10 fix, 7 writes | 46,135,630 | 39.5 s, 18 GB | 35.1 s, 9.8 GB | 96.8 s, 6.6 GB | not run (job queued, never started) |

\* From the README; these two TLC runs have no log in `~/bench3`.

Veil build time comes on top of the check time. The README records about
15 s for `CommitlogCleanupBench.lean` (8 configs) and 47 s for
`VnodesTabletsMigration.lean` (5 configs), about 5 s of C compilation per
configuration. (Both files were built twice; the surviving logs in
`~/bench3` show 28 s wall for the last build of each.) The 46M-state Veil
run is dominated by the search, not the build.

What the numbers say:

- **At equal threads (32), Veil matched or beat TLC on every run.** On the two
  large exhaustive runs: 17.4M states, TLC 27 s / 16 GB vs Veil 25 s /
  3.7 GB; 46.1M states, TLC 40 s / 18 GB vs Veil 35 s / 9.8 GB (Veil 9% and
  11% faster).
- **Veil on 1 thread took 1.3x to 2.5x TLC's 32-thread time** on the large
  runs (about twice on the two largest), so per thread it is far faster.
  1-thread Veil against 32-thread TLC is not a fair comparison and is listed
  only to show this.
- **TLC's memory is mostly heap it was allowed** (`-Xmx40g`), not what the
  search needed. Veil's RSS is closer to the real need; it is a quarter to
  half of TLC's figure.
- **Apalache finds bugs but is 10 to 100 times slower**, and its time grows
  with the path length, not the state count. A pass at the diameter took
  22 minutes for a 288K-state model that TLC does in 1.5 s, and the 5-write
  commitlog fix timed out at step 11 of 15. Step times in that run: step 8
  took 49 s, step 9 about 5 min, step 10 about 10 min.
- For a single check that TLC finishes in seconds, TLC is faster end to end
  once Veil's build is counted. This was judged unimportant (section 6).

## 4. Earlier experiments, per tool

### Apalache on commitlog_cleanup (#10), laptop

| step | command | result |
|---|---|---|
| find the bug | `check --cinit=CInit --inv=NoLostWrite --length=8` | violation at step 7, 1.8 s, ITF trace |
| the fix, bounded | `apa_commitlog_cleanup_fixed.tla`, `--no-deadlock --length=10` | holds, 12.6 s |
| `IndInv` holds initially | `--inv=IndInv --length=0` | holds, 1.2 s |
| `IndInv` is inductive | `--init=IndInv --inv=IndInv --length=1` | holds for all integer positions, 1.5 s |
| `IndInv` implies the property | `--init=IndInv --inv=NoLostWrite --length=0` | holds, 1.2 s |

The last three steps amount to a proof for every execution length, for two
writes, which TLC can't give. Apalache handles unbounded `Int` well, where
explicit search can't enumerate.

### TLAPS on commitlog_cleanup (#10)

`tlaps/commitlog_cleanup_proof.tla` proves `Spec => []NoLostWrite` for any
set of writes, with the same inductive invariant: 27 obligations, 0.3 s.
Without the fix's guard, exactly one obligation fails, the `SaveRecord`
step, which is the bug. The proof is a hand-written step per action
(`<1>1` ... `<1>6`, `BY DEF IndInv, TypeOK, ...`); the invariant has to be
found by hand.

### Veil subtools on commitlog_cleanup (#10)

Files: `veil/CommitlogCleanup.lean` (modules `CommitlogCleanup` and
`CommitlogCleanupFixed`), `veil/CommitlogCleanupUpdr.lean`,
`veil/CommitlogCleanupTemporal.lean`. Run with `lake lean <file>` in `~/veil`
on the bench.

| subtool | what it gave | time |
|---|---|---|
| `sat trace` (symbolic, any instance) | the bug's 7-step trace, the interleaving TLC found; the solver chose 2 writes, 2 positions | part of 19 s for the file |
| `#model_check interpreted` | bug: `no_lost_write` violated; fix: no violation, 189 states | same |
| `#simulate` (2000 random walks of 20 steps) | bug: the violation | same |
| `#check_invariants` (SMT) | bug: a counterexample to induction for `crash`; fix: every VC proved, for all instances | same |
| `unsat trace` (8 steps, fix) | no output, which seems to mean no such execution (not confirmed) | same |
| `#updr` | from the safety property alone, an inductive invariant of 5 clauses at frame 5, two of them the hand-written `unflushed_above_record` | 57 s, 269 solver calls |
| `#temporal_check` | without fairness of flush: a lasso that never flushes; with weak fairness: holds (291 product states) | 21 s |
| `set_option veil.smt.trust false` | the fix's 30 SMT goals reconstructed and checked by the Lean kernel | 21 s with the temporal check |

### Veil proof attempt on vnodes (#7)

`veil/VnodesTabletsMigrationProof.lean` (commit `c9c5ff5acee`) asks
`#check_invariants` to prove the fix from its four properties alone. In 14 s
it proved 49 of 55 goals and named the 6 that fail:

| action | breaks |
|---|---|
| `finalize_forward` | `readable`, `layout_matches` |
| `boot` | `readable`, `boot_ok` |
| `cleanup` | `durable` |
| `bootstrap` | `readable` |

These are where the properties need strengthening invariants. After the
benchmark, 14 hand-written invariants closed them: all 204 goals proved in
16 s (commit `0f81ad02541`), and the proof fails with the published-mode
column off. With `veil.smt.trust false`, the kernel re-checked all but `boot` (199 goals ok; that mode splits goals differently);
the 17 goals of `boot` failed to reconstruct ("Tactic `assumption`
failed"), a reconstruction limit, not a counterexample.

### Veil on the rest of the catalog (after the decision)

Every spec with a catalog bug now has a Veil copy, validated against TLC
(same verdict on every config, same counterexample length with one TLC
worker, same state count where the encoding keeps it), and five fixes and
one candidate fix have proofs for every instance size; FINDINGS.md has the table. What it took:

| spec | Veil states vs TLC | proof |
|---|---|---|
| tombstone_gc_repair (#2) | equal | 184 goals, 27 s, after six rounds: the last goals timed out (unknown, not refuted) until the property was split by direction and replica and the two replicas spelled out in its quantifier |
| tablet_storage (#4, #6) | 535,351 vs 555,966 (abstract file ids) | 940 goals, 139 s, after one fix to the strengthening the model check caught |
| logstor (#8, #9) | equal, 21,659,782 with every feature | no fix to prove |
| group0_state_machine (#1) | equal | 321 goals, 29 s, first try |
| compaction_groups (#3) | differ (setup and registration sub-steps) | no fix modelled |
| sc_tablet_migration (#5) | differ (TLC uses VIEW and symmetry): MC_current is 110,542,183 states, 598 s, 15 GB; 15 of 19 configs match TLC (verdicts and trace lengths), 4 passing configs stopped above 45 GB | not tried |
| auth cache (new, SCYLLADB-4206) | no TLA+ copy | the candidate fix, 210 goals, 71 s, after the model got `_loading_sem` (without it the model blamed LDAP reloads wrongly) |

## 5. Ergonomics

### Writing a spec

| | TLC | Apalache | TLAPS | Veil |
|---|---|---|---|---|
| language | TLA+ | TLA+ with `@type` annotations | TLA+ plus proof steps | Lean 4 DSL, Ivy-style first-order logic |
| from an existing TLA+ spec | native | mechanical; the vnodes copy typechecked on the first try | write the proof by hand | about 45 minutes and two fix rounds for vnodes |
| things it handles poorly | integers with wide ranges (enumerates them) | `CHOOSE`, untyped operators | anything without a hand-found invariant | integers as such: positions become an ordered uninterpreted sort |
| constants per run | `MC_*.cfg` with an `expect:` header | `CInit*` operators, `--cinit` | none | arguments to `#model_check`, e.g. `{ write := Fin 7, maxCrashes := 1, fixed := true }` |

Veil translation changes seen on #7 and #10:

- A set of write ids becomes an ordered sort, with the smallest unused value
  picked (TLC: `nw + 1`).
- `towner`, which is all `None` or a copy of `vowner`, becomes a flag.
- Replay positions are an uninterpreted sort with `TotalOrder`; a new
  position is above every position handed out.
- On #11, `to \in SUBSET Nodes` became three `Bool` parameters.

### Debugging a counterexample

- **TLC**: a state-by-state text trace; `scripts/tla/trace_diff.py <file>`
  prints it as actions and changed variables.
- **Apalache**: an ITF JSON trace, plus a log of `State N` / `Step N` lines.
  Exit code 12 means a violation was found, 124 is the `timeout` wrapper,
  and `EXITCODE: OK` means it holds up to the given length.
- **Veil**: the compiled binary prints a JSON object with `"violation"`,
  `"violates": [...]` and `"trace"` (every state, every field, the action and
  its arguments) on stdout, and metrics (per-action state counts) on stderr.
  The binary exits 0 whether or not it finds a violation: read the
  `"result"` field (`found_violation` / `no_violation_found`) and
  `"explored_states"`. `#check_invariants` prints a per-action, per-property
  table of passes and failures, and a counterexample to induction when it
  can build one.

### Proving

- **TLC**: can't prove; a pass is bounded.
- **Apalache**: inductiveness checks for a fixed instance size, with
  unbounded integers.
- **TLAPS**: full proofs for any instance, fast to check, but every step and
  every invariant is by hand.
- **Veil**: `#check_invariants` proves every action preserves every
  invariant for all instances and names the failing pairs; `#updr` can infer
  the missing invariant; `veil.smt.trust false` has the Lean kernel recheck
  the SMT results. Liveness: `#temporal_check` for bounded instances
  (`prove_temporal_by` was not tried).

### Failure modes and gotchas

TLC:

- Simulation (`-simulate`) needs `-deadlock`, or it stops at the first state
  with no enabled step.
- The heap cap (`-Xmx`) inflates RSS; don't report it as the search's need.

Apalache:

- Without `--no-deadlock`, a run where no write is left reports a deadlock
  as an error.
- The first inductive-invariant attempt failed on its own type bound
  (`nextRp \in 1..10`); the counterexample showed it at once and unbounded
  `Int` fixed it.
- A timeout gives no verdict; record the step reached ("timeout at step 11
  of 15"), never a time or a pass.

Veil:

- `#check_invariants` can't print counterexamples to induction:
  `Failed to build counterexample; exception: Application type mismatch: The argument VnodesTabletsMigrationProof.State.Label.member has type VnodesTabletsMigrationProof.State.Label but is expected to have type Bool`
  names whatever field comes first in the state (`member`, `used`,
  `spent`); renaming it doesn't help. Model-check the strengthening on
  small bounds in the proof file instead: a wrong invariant then fails with
  a trace.
- With about 40 invariants, every goal failed with "cannot translate Type":
  instance synthesis for their conjunction ran out of heartbeats. `set_option
  synthInstance.maxHeartbeats 2000000` fixed it.
- A goal that times out (❓) is unproved, not refuted. Splitting it helps: a
  property into its directions or cases, an action into variants guarded by
  a condition (keep and purge, one per replica).
- Names Veil or Lean generate clash with: an action `complete`, a field
  `rec`, an enum named like a field.
- A `let x :| p` pick fails ("could not extract executable choices") when a
  quantifier in `p` reads a state individual, or `p` uses a ghost relation;
  `after_init` can't pick at all. Mark candidates in a relation first, use a
  `setup` step for initial values that need a pick.
- Veil keeps only the 8 most recently built model-checker binaries
  (`veil.modelChecker.maxStoredCheckers`); with more configs than that,
  build with `-Dveil.modelChecker.maxStoredCheckers=0` or the first ones are
  gone (exit code 127) before they run.
- A run script that ends the `sleep` feeding a binary's stdin by name
  (`pkill -f 'sleep 3700'`) also ends every other binary started the same
  way; give each run its own sleep duration.
- Names that clash with Lean: `mt` and `W` in the vnodes copy.
- In an assignment, a capitalized name (`val A i := ...`) is a universal
  index over all values, not the constructor `A`; constructors used as point
  indices must be lowercase.
- `none` as an enum constructor clashes with `Option.none`.
- No doc comment (`/-- -/`) before an `action`.
- A `def` used in a module needs `public`.
- A `Bool` relation takes `decide (...)` of a proposition.
- `set_option ... in` before `#check_invariants` has no effect; set the
  option for the whole file.
- The temporal checker stops at the first violated property, so properties
  that should hold need a run of their own.
- Veil warned it could not generate its `wp_local_eq` lemma for an action
  with nested `if`s and `let`s (counters `inc`); this only matters for SMT
  verification.
- Use `lake lean <file>` from the Veil checkout: `lake env lean` doesn't load
  cvc5's native library. `#model_check` compiles a binary unless told
  `interpreted`.
- In a bug-vs-fix pair, remove the fix-only invariant (e.g.
  `unflushed_above_record`) from the bug module, or it fires first and hides
  the bug.
- With `veil.modelChecker.maxStoredBuilds=0`, run the binaries from the
  latest build only. A first bench round that ran binaries listed in an older
  build log failed with exit code 127 (binary not found) for all 10 runs;
  rebuilding and rerunning fixed it.

## 6. Which tool for what

| job | best tool in this experiment | notes |
|---|---|---|
| find a bug in a small bounded model | TLC or Veil, equal | Veil also offers `#simulate` and symbolic `sat trace` |
| exhaustively check a fix at fixed bounds | Veil (TLC close) | Veil faster per thread, less memory |
| check a fix with wide integer domains | Apalache | Veil abstracts integers to ordered sorts instead |
| prove a fix for every instance size | Veil or TLAPS | Veil names failing (action, property) pairs and can infer invariants; TLAPS is all by hand |
| liveness | Veil (`#temporal_check`), TLC | TLC checks liveness in advanced_rpc_compression_negotiation |
| a quick one-off check of an existing TLA+ spec | TLC | no translation, no build |

## 7. Decision (2026-11-14)

Made by the user after the benchmark:

- **Veil becomes the single checker for new work**, both to find bugs and to
  check and prove fixes.
- **TLC and Apalache are dropped for new work.** The decision doesn't name
  TLAPS; Veil's `#check_invariants` covers the same role.
- **One-off setup and compile cost was judged unimportant**: the 45-minute
  translation of an existing spec, and Veil's ~5 s per configuration build.
- **The existing TLA+ specs stay** as the record of past findings, with
  their `MC_*.cfg` expectations and `scripts/tla/check.sh`.

What this gives up, per the benchmark: for a check TLC finishes in seconds,
TLC is faster end to end once Veil's build is counted, and Apalache was the
better fit for wide integer domains.

## 8. Reproducing the benchmark

Everything runs on the bench host (`marcin@192.168.0.211`, login shell fish;
use `ssh host 'bash -s' <<'EOF' ... EOF` for scripts). The scripts live in
`~/bench3` on the bench only (not in the tree). They hard-code the bench's
main checkout `/code/nuivall/scylladb`, Veil in `~/veil`, Apalache in
`~/tools/apalache-0.62.4`. Move the code there by git (push the branch,
check it out in the main checkout), never by copying files. Run one job at a
time.

TLC (`tlc.sh cfg...`, paths relative to `docs/dev/tla`):

```
cd /code/nuivall/scylladb
TLC_JAVA_OPTS=-Xmx40g timeout 3600 /usr/bin/time -f "RSS %M KB" \
    scripts/tla/check.sh docs/dev/tla/vnodes_tablets_migration/MC_bench_fix_large.cfg
```

Configs: `vnodes_tablets_migration/MC_{before_current_column,current,topology_ops,bench_bug_large,bench_fix_large}.cfg`,
`commitlog_cleanup/MC_bench_{bug,fix}_{5,6,7}.cfg`.

Veil build (`veilbuild.sh File.lean`), which builds one binary per
`#model_check compiled` line without running it:

```
. ~/.elan/env; cd ~/veil
/usr/bin/time -f "BUILDWALL %e s %M KB" lake lean \
    /code/nuivall/scylladb/docs/dev/tla/veil/VnodesTabletsMigration.lean -- \
    -Dveil.modelChecker.buildOnly=true -Dveil.modelChecker.maxStoredBuilds=0 \
    > ~/bench3/build-VnodesTabletsMigration.lean.log 2>&1
```

Veil run (`veilrun.sh File.lean`): take the binaries from the build log, in
order, and run each on 1 and 32 threads:

```
grep -o 'VEIL_BUILD_ONLY {"binary":"[^"]*' build-$f.log | sed 's/.*"binary":"//' | awk '!seen[$0]++'
timeout 3600 /usr/bin/time -f "RSS %M KB" $bin          < <(sleep 3700) > out 2> err   # 1 thread
timeout 3600 /usr/bin/time -f "RSS %M KB" $bin 32 20 1  < <(sleep 3700) > out 2> err   # 32 threads
grep -o '"result": *"[^"]*"\|"explored_states": *[0-9]*\|"violates[^,]*' out
```

The script keeps stdin open (`< <(sleep 3700)`) for the binary and kills the
`sleep` afterwards. Binary order: `CommitlogCleanupBench.lean` #1/#2 are a
2-write smoke pair (bug, fix: 70 states), #3 to #8 are bug/fix at 5, 6, 7
writes; `VnodesTabletsMigration.lean` #1 to #5 are `MC_before_current_column`,
`MC_current`, `MC_topology_ops`, `MC_bench_bug_large`, `MC_bench_fix_large`.

Apalache (`apa.sh "spec cinit inv length" ...`):

```
cd /code/nuivall/scylladb/docs/dev/tla/apalache
JVM_ARGS=-Xmx40g timeout 1800 /usr/bin/time -f "RSS %M KB" \
    ~/tools/apalache-0.62.4/bin/apalache-mc check --cinit=CInitFix --inv=InvFix \
    --length=21 --no-deadlock --out-dir=/tmp/apa-out apa_vnodes_tablets_migration.tla
```

Jobs: `apa_commitlog_cleanup CInit{5,6,7} NoLostWrite 10`;
`apa_vnodes_tablets_migration CInitBug LayoutMatches 14`;
`apa_vnodes_tablets_migration CInitBugLarge InvTopo 16`;
`apa_commitlog_cleanup_fixed CInit{5,6,7} NoLostWrite {15,17,19}`;
`apa_vnodes_tablets_migration CInitFix InvFix 21`;
`apa_vnodes_tablets_migration CInitFixLarge InvTopo 27`.

Veil proof attempt:

```
. ~/.elan/env; cd ~/veil
lake lean /code/nuivall/scylladb/docs/dev/tla/veil/VnodesTabletsMigrationProof.lean
```

TLAPS: `tlapm` from `~/tools` on `tlaps/commitlog_cleanup_proof.tla`; keep its
cache (`.tlacache`) out of git.

## 9. Open items

- **Memory**: without TLC's VIEW and symmetry, the Veil copy of
  sc_tablet_migration explores about 100M states per config (15 GB), and
  the auth cache with LDAP at three roles went past the bench's 62 GB; run
  big configs one at a time, under a memory cap (`systemd-run --user --scope
  -p MemoryMax=20G`).
- **Binaries**: Veil keeps the 8 most recently built model-checker binaries
  (`veil.modelChecker.maxStoredCheckers`) and, without `buildOnly`, one build
  folder (`maxStoredBuilds`); pass 0 for both, or a file with more configs
  loses binaries before they run.
- **Veil build times**: the README's 15 s / 47 s and the 28 s / 28 s in the
  surviving bench logs disagree; a clean rebuild of each file would settle it.
  Larger files take longer: 19 configs of sc_tablet_migration built in
  471 s.
- **Not tried in Veil**: the Distributed DSL (per-node programs over a
  network), `prove_temporal_by` (liveness proofs), Veil's own TLC-vs-Veil
  benchmark script.
- **Apalache cells not run**: `MC_topology_ops`, `MC_bench_fix_large`, and
  the 6- and 7-write commitlog fixes. With Apalache dropped, they won't be.
