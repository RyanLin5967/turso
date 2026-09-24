# PRE-REGISTRATION — `branch_curve` (Turso fork, per-branch CoW arena)

Written and committed BEFORE the benchmark's first run — before its first BUILD, in fact: the
harness was written under the no-local-compute rule (2026-09-24) and committed UNBUILT. The
mechanism it measures is `caeb8d71c` (`ferrobranch-arena`; later commits there add tests only).
Amendments are append-only, dated, below the line at the end. Nothing above that line changes
after the first run. A compile fix to the harness is an amendment only if it changes what is
timed or asserted; the diff must say which.

## What is measured

`core/examples/branch_curve/main.rs`. One process, one thread, one trunk table of 20,000 rows
(100-byte values, checkpointed into the database file before any fork). The harness grows N live
branches forked from the trunk, each of which rewrites ONE row in place (same length) and so owns
ONE page. At each checkpoint N it takes K samples (default 200) of:

| op | what is timed |
|---|---|
| `fork` | `trunk.fork_branch()` |
| `open` | `branch.connect()` on the branch just forked |
| `first_write` | one autocommit `UPDATE` of one row on that branch |
| `read_open` | `branch.connect()` on a random one of the N live branches |
| `read_own` | `SELECT` of the row that branch wrote (its leaf is in the arena) |
| `read_inh` | `SELECT` of a far row it did not write (its leaf is the trunk's) |
| `reap` | `Branch::reap()` of a sampled branch |

Instrument: `std::time::Instant` per operation; the harness prints the clock's measured tick. RSS
from `memory_stats::physical_mem`. Before printing any number at a checkpoint the harness asserts
FROM THE ENGINE that `live_branches == N` and `arena_slots_in_use == N`, spot-checks isolation on
a random branch, and after sampling asserts it is back at exactly N; otherwise it prints
`NOT A RESULT` and exits 1.

The lead runs everything, in a release build under the machine lock: first a smoke at
N ≤ 10³ (default arguments), then the full curve (N = 10², 10³, 10⁴, 10⁵, 10⁶). The smoke is NOT
a slope result (two points, K = 200) and is labelled as such; it exists to show the harness's
own assertions pass before the long run is spent.

## Predictions, from the design (READ from `core/branch/store.rs`, not measured)

The mechanism's per-operation work, as the code reads:

* `fork` — trunk WAL read+write lock round trip, page-1 header read (cached), one `HashMap` insert
  (branches), one `BTreeMap` insert (trunk children). O(log N).
* `open` — a new `Pager` (`Database::_init`: 512-byte header read, WAL read-tx round trip), an
  `Arc<Schema>` clone, a `Connection`. No term in N.
* `first_write` — resolve root + leaf (branch map miss, one level up to the trunk: hash lookups),
  WAL read of those pages, CoW: one slot allocation + one 4 KiB copy, commit: one 4 KiB copy.
  No term in N except the arena growing (fresh slots are fresh, untouched memory).
* `read_own` / `read_inh` — resolve a 2–3 page descent through hash lookups; the owned leaf is a
  random slot in an N-page arena.
* `reap` — `HashMap` remove, `BTreeMap` remove, a neighbour lookup and an (empty) range query in
  the trunk's retained index. O(log N).

**P1 (slopes).** For every op, the least-squares log-log slope of p50 against N over
10² … 10⁶ is **within ±0.10** — i.e. flat. O(log N) terms are too small to show over a 10⁴×
range at these constants.

**P2 (memory-hierarchy step, the expected deviation).** `read_own` may step UP between 10³ and
10⁵ as the arena (4 KiB × N) outgrows the CPU caches and the owned leaf becomes a cold, random
page — a bounded step, **at most 2×** p50 from 10³ to 10⁶, not a slope. `read_inh` should not
step (its leaf is one of the trunk's ~600 pages, hot in every branch's reads).

**P3 (tails).** p99/max of `fork` will show isolated spikes at the `HashMap` doublings of the
branch table (power-of-two N); a spike is a single sample and is not a slope. No claim on max.

**P4 (space).** Exactly **1 arena page per live branch** (`arena_slots_in_use == N`, asserted),
i.e. `page_size` bytes of page data per branch. RSS per branch at 10⁶ between **4.1 and 5.5 KB**
(page + map entries + handle), measured as total RSS / N (includes the fixed base; the base is
printed).

**P5 (reclamation).** After teardown, 0 live branches and 0 arena pages in use (asserted).

## Falsifiers — what each outcome would mean

* Any op with |p50 slope| ≥ **0.25** over 10²…10⁶: a wall. It must be attributed (profile or an
  arm with the suspected term removed) before any mechanism is named; per the project's rule, no
  mechanism is stated for a gap until it is itself checked.
* 0.10 < |slope| < 0.25: inconclusive at K = 200; re-run with larger K and a Latin-square order
  before reading anything into it.
* `arena_slots_in_use != N`: the "one page per branch" premise is false for this UPDATE on
  Turso (e.g. a rebalance on same-length rewrite) — the harness refuses, and the premise is
  re-derived from the engine before the curve means anything.
* `read_own` step > 2×: the arena's layout (one slot per branch, allocation order) is a locality
  wall worth its own row.

## Known limits of this measurement, stated before it runs

* The arena is **volatile** (no persisted branch map). ferrodb D79 measured that persisting the
  map was the wall there; this curve deliberately measures branch management WITHOUT that, and
  must be compared only with ferrodb's persistence-OFF arm.
* Single-threaded: the store's single `Mutex` is not exercised under contention.
* Depth 1: every branch forks from the trunk. Resolution cost grows with depth, not N; depth is
  not on this curve's axis.
* The trunk does not write during the curve, so no retained versions exist; the GC range query is
  exercised only in its empty case.

---
## Amendments (append-only)

### A1 — 2026-09-24: the durable arm and the REOPEN column (appended before any build or run of it)

Nothing above this amendment changes. The default run (`--durability volatile`) is the run the
predictions above are about. The lead runs the durable arm SEPARATELY:
`branch_curve --durability durable --checkpoints 100,1000,10000,100000,1000000` (fsync ON), and,
as the persistence-cost control ferrodb's D79 used, the same with `--durability durable-nosync`.
The mechanism measured is the durable branch store of the UNBUILT commits after `c5caf85d0`
(design: `frontier/lane_turso_branching.md`, "DURABLE BRANCHES").

New columns, per checkpoint, durable arm only (`--reopens R`, default 3):

| op | what is timed |
|---|---|
| `reopen` | drop every holder of the `Database`, then `Database::open` + one trunk `connect` — trunk open plus branch-store recovery (snapshot load + log replay + free-set derivation) |
| `attach_all` | `Database::branch(id)` for all N detached branches |
| `reopen_read` | `connect` + `SELECT` of its own row on K DISTINCT random branches after the reopen (each first connect reparses that branch's schema) |

The harness asserts after every reopen, from the engine, that N branches and N arena pages came
back, and that each sampled branch still reads its own write; otherwise `NOT A RESULT`.

**P6 — reopen is LINEAR, by design.** Recovery is eager: every branch map is rebuilt in memory
(≈ 2 log records per branch — Fork, Commit — or one snapshot entry after a compaction), then the
free set is derived over the arena's high-water mark. Predicted `reopen` p50 log-log slope over
10⁴…10⁶: **0.8 to 1.1**. Over the full 10²…10⁶ range a fixed floor (trunk open, WAL recovery,
header read) dominates the small checkpoints, so the full-range slope is lower: **0.5 to 1.0**.
Per-branch recovery cost, (reopen(10⁶) − reopen(10⁴)) / (10⁶ − 10⁴): **0.2 to 5 µs per branch**
(INFERRED: ~60 bytes of log, one crc32c, two or three hash inserts and a BTreeMap insert each).
⚠ ferrodb's catalog is a B+tree read lazily; a linear Turso line against a flat D65 line compares
an EAGER design with a LAZY one, not one engine with another, and must be labelled so.

**P7 — `attach_all` is linear:** slope **1.0 ± 0.15**, 50–500 ns per attach (a lock and a lookup).

**P8 — `reopen_read` is flat in N:** slope within **±0.10**, and **1.2× to 5×** the volatile
`read_open + read_own` at the same N — the difference is the schema reparse.

**P9 — the existing ops in the durable arm stay flat but become fsync-bound.** `fork` = 1 log
fsync, `first_write` = arena fsync + log fsync, `reap` = 1 log fsync. Slopes within **±0.10**; p50
of those three dominated by fsync; `read_own`/`read_inh` within **2×** of volatile (an OS-page-cache
read plus a crc32c instead of a memcpy). `durable-nosync` should sit within 2× of volatile on every
op; the gap between `durable` and `durable-nosync` IS the fsync cost, not the design.

**P10 — compaction shows only in the tails.** The log is compacted when it exceeds max(1 MiB, 2 ×
the last snapshot): the op that trips it pays O(live state) once, geometrically rarely. Predicted:
p50 flat, `max` of fork/first_write/reap growing roughly linearly in N at the checkpoints where a
compaction landed inside the sample window. A p50 that moves with N is NOT explained by this.

**P11 — space.** Arena file ≈ page_size × (N + K) bytes; log + snapshot ≈ 30–80 B per live
branch. RSS per branch no longer includes page data: **0.2–1.0 KB per branch** at 10⁶.

Falsifiers for A1: `reopen` slope < 0.5 over 10⁴…10⁶ means recovery is not doing what this
design says — before believing a flat line, check that the post-reopen assertion ran and that the
log actually held N records. `reopen` slope > 1.2 is superlinear recovery: a wall to attribute
(rehash, BTreeMap inserts, free-set derivation) before naming a mechanism. `reopen_read` slope
> 0.25: the reparse depends on N.

### A2 — 2026-09-24: F4 (interior reclamation at release) and F5 (leases), appended before any build or run

Nothing above this amendment changes. Source: `frontier/research_reclaim-with-live-children.md`
§5 F4, F5. The red tests are committed first, at `7518bbcd5`, against a lease skeleton that does
nothing.

**P12 — F4, test level.** At `7518bbcd5`,
`releasing_an_interior_branch_frees_what_no_live_child_can_read` fails on its `freed_pages`
assertion with **0** freed (the release returns early while the interior has a live child).
After the fix it frees **exactly S = 3**: the pages written after the last fork. The pre-fork
page the child reads stays held. Both are checked by membership.

**P13 — F5, test level.** At `7518bbcd5` each of the four lease tests fails on its FIRST lease
assertion. The skeleton's `expire_branches` reaps nothing, and its lease clock reads zero. After
the fix all four pass, including the one that crosses a reopen with 6 s already spent on the
clock. That test's thresholds come from the test itself (9 s < 10 s alive, 11 s > 10 s reaped),
not from calling the store.

**P14 — the existing columns do not move.** No branch in the default runs has a lease. The
expiry pass that now runs at every fork and every open finds an empty deadline index: one
`BTreeSet` range on an empty set. Every existing column must still fall inside its P1–P11 band.
A p50 that moves by more than the run-to-run noise at any N means the pass is not the no-op this
predicts. Attribute that before reading anything else.

**P15 — new column `expire`, both arms.** At each checkpoint the harness forks K extra branches,
one page each, gives them a lease, runs the lease clock past it, and times ONE
`expire_branches()` call. The harness asserts that exactly those K are reaped and exactly K pages
freed, and that the engine is back at N branches and N pages. The pass costs one range over the
deadline index plus K ordinary reaps. In the durable arm the K Release records are flushed
together, so it costs one fsync per pass, not one per branch. Predicted:
- the p50 of `expire / K` has a slope within **±0.10** over 10²…10⁶;
- the volatile arm is within **2×** of `reap`;
- the durable arm is `reap` minus K−1 fsyncs, amortised: **below** `reap` per branch.

⚠ This column measures leaf expiry at depth 1, the only topology this harness builds. It does
NOT measure the paper's point, partial reclamation of an INTERIOR branch on expiry. That is
tested (`an_expired_interior_is_partially_reclaimed_while_its_live_child_reads_through_it`), not
curved, and no claim about its cost at N is made here.

Falsifiers for A2:
- the `expire / K` slope ≥ 0.25: the pass is not O(log N) per branch; suspect a scan over all
  branches first;
- `freed_pages ≠ K`: the harness refuses (`NOT A RESULT`);
- P14 violated: the no-lease path is not free.

### A3 — 2026-09-24: branch file sizes printed, so P11's space prediction can be judged (appended before any build or run)

Nothing above this amendment changes. P11 predicts the arena file at ≈ page_size × (N + K) bytes
and the log + snapshot at 30–80 B per live branch, but the harness printed neither size, so no
run could judge them. From this amendment on, the durable arms (`durable`, `durable-nosync`)
print one more line at every checkpoint, right after the `# N=` line:

    # files N=<n> arena_bytes=<a> log_bytes=<l> snap_bytes=<s> page_size=<p>

- **Observation only.** Each size is a `stat` of the file, taken after the checkpoint's timed work.
  No timed path changes, so no existing prediction changes.
- **A missing file prints 0.** Before the first compaction there is no snapshot. Any other `stat`
  failure is `NOT A RESULT`.

**Reading, fixed now, before any run.** P11 wrote "≈" and a range without a tolerance, so:
- "arena ≈ page_size × (N + K)": `arena_bytes / (page_size × (N + K))` in **[0.8, 1.25]** at 10⁶.
  The arena never shrinks, so free slots left after the sampled reaps count in the file.
- "log + snapshot 30–80 B per live branch": `(log_bytes + snap_bytes) / N` in **[30, 80]** at 10⁶.
- Each size is printed at every checkpoint. They are judged at 10⁶ only; below that, the fixed
  header and the compaction threshold (1 MiB) dominate.

### A4 — 2026-09-24: review 9 — the lane's own unlanded tests change (appended before any code of it)

Nothing above this amendment changes. Source: `frontier/lane_turso_review9.md` @ artie-research
`fb6ec95` (scope addition `5d79013`), with the lead's decisions 1–8. None of these tests has been built
or run. Review 9 found that the ATTACH test cannot reach the code it tests: its main database has
ATTACH disabled, so translation refuses the statement before any registry lookup. So the lane's own
tests change here, before any code.

**Test changes.** They go in the commit right after this amendment, which holds tests only; its
parent is `073b088d2`.
1. `attach_of_a_database_held_open_with_a_lease_or_durable_is_not_refused`:
   - main opens with `DatabaseOpts::new().with_attach(true)`, as every other ATTACH test in the
     tree does (decision 1);
   - before each ATTACH, a plain `open_at(path, DatabaseOpts::new())` of the same path must be
     REFUSED, naming "branch lease" (`leased.db`) or "branch durability" (`durable.db`). This is a
     premise and the negative control: it proves the ATTACH is a registry hit, and that the
     exemption is ATTACH's alone (decision 4);
   - both ATTACHes run before the verdict. One panic then lists every refused ATTACH as
     `<name>: <error>`, joined by ` | `, so a red shows both halves, not only the first.
2. `a_sidecar_refusal_keeps_frames_only_while_the_real_file_does_not_exist`: the assertion
   "under any other name" becomes the lead's wording (decision 2). The test asserts "by any name"
   (panic: "the rename is not conditioned on no open since by ANY name") and "real.db included"
   (panic: "the real name is not counted among the names").
3. `a_name_too_long_for_branch_files_opens_volatile`: its doc comment only. The range and the file
   are corrected (decision 7); no assertion changes.

No test is added or removed, so there are still 118 `#[test]` attributes under `core/branch/`, 117
of which run on unix.

**P16. The red commit (tests only).** `cargo test -p turso_core --lib branch::` gives **116 passed,
1 FAILED**. The failure is the keeps-frames test, and its panic contains "by ANY name". The ATTACH
test PASSES there, because the F2 fix is in `073b088d2` (INFERRED). If it fails, that is a finding
against the fix or the test. It is not a test to edit.

**P17. F2's red, shown against the code before F2.** Run `git checkout --detach e472b108e && git
checkout <red> -- core/branch/durability_tests.rs`, then `cargo test -p turso_core --lib
branch::durability_tests::attach_of_a_database_held_open_with_a_lease_or_durable_is_not_refused`.
It gives **1 FAILED**, and its panic matches both `leased\.db: [^|]*default branch lease` and
`durable\.db: [^|]*branch durability`. No panic contains "premise": the plain open is refused there
too, because the lease and durability checks predate F2.

**P18. `e472b108e` as committed.** `branch::` gives **114 passed, 3 FAILED**, and each failure must
carry its reason:
- the keeps-frames test: "no other build's opens under another name";
- the zeroed-hole test: "no condition for a safe restore";
- the ATTACH test: "ATTACH is an experimental feature". This is the KNOWN WRONG reason (review 9
  finding 1). It is recorded here, and it is no evidence for F2; P17 carries F2's red.

**P19. The fix commit.** `branch::` gives **117 passed, 0 failed**. The full `cargo test -p
turso_core --lib` gives **2466 passed, 0 failed, 17 ignored**, the same as BATCH v12, since no test
is added or removed.

**P20. Fire-check at the fix commit.**
- There are **113 mutants**: `OTHER_a` is re-anchored to the new wording, and `OTHER_b`, which
  drops ", {real} included", is new, killed by the keeps-frames test.
- Killers are matched on the FULL test path as `cargo test` prints it (decision 6). The paths were
  derived from the source: all 92 distinct killers are placed exactly once, under eight modules,
  and every module file is declared plainly by its parent.
- Every named killer passes at baseline (P19).

**P21. Runner self-test, no cargo.** It passes as committed. Each of these plants, in a scratch
copy, makes it exit non-zero:
- the earlier p1–p7;
- p6b, a kill matched by suffix;
- p8, the exit status computed inline at its call site;
- p9, the canary check reverted to `verdict == 'SURVIVED'`;
- matching on the last `::` segment instead of the full path.

The main loop is one function that the self-test drives with canned logs, so these plants are
exercised where they live (decision 6).

**P22. Step (a)'s reason check, no cargo.** A checker reads a cargo log against a pre-registered
spec. It requires:
- exactly the named tests to fail, and the stated number to pass;
- every failing test's own output section to match each of its reason patterns.

Its self-test passes as committed. It exits non-zero on each plant: a wrong reason, a missing
failure, an extra failure, a wrong pass count, and a reason found only in ANOTHER test's section.

**Falsifiers for A4:**
- In P16, any failure other than the keeps-frames test, or that test failing for another reason:
  the test or the fix is wrong.
- In P17, a "premise" panic: the ATTACH is not a registry hit at the base, so the test does not
  exercise the exemption.
- In P17, only one half refused: the other check does not reach ATTACH at the base.
- In P19, the ATTACH test red: the F2 fix does not do what `073b088d2` claims.

⚠ A4 changes this file's sha256 again. A3 already makes the verdict script refuse until someone
re-reads the file and re-pins it.

### A5 — 2026-09-24: review 10 — decisions F1–F9 (appended before any code of it)

Nothing above this amendment changes; A4's text stays as committed. Source:
`frontier/lane_turso_review10.md` @ artie-research `049715c` (SOUND-WITH-CAVEATS at `70bd2c410`), with
the lead's decisions F1–F9. None of these tests has been built or run.

**Erratum to A4 (F9).** A4 says the red commit "holds tests only; its parent is `073b088d2`". Its
parent is `703b9af9d`, the A4 commit itself (`git log --format='%h %p'`). The code under test is
`073b088d2`'s either way, so P16 is unaffected.

**Test changes.** They go in the commit right after this amendment, which holds tests only.
1. `attach_of_a_database_held_open_with_a_lease_or_durable_is_not_refused`:
   - **F2.** The negative control matches each registry refusal's hit-specific phrase,
     "is open in this process with default branch lease" (`leased.db`) and "is open in this process
     with branch durability" (`durable.db`), instead of "branch lease" / "branch durability". A
     registry MISS of a database with durable branch files is refused by `BranchStore::open` with
     text that also contains "branch durability", but not "is open in this process with".
   - **F4.** After each successful ATTACH, the test asserts `Arc::ptr_eq` between the held instance
     and `conn.get_source_database(conn.get_database_id_by_name(alias))`. The panic is "premise:
     the ATTACH of <name> received another instance, not the registry's". That proves THIS ATTACH
     hit the registry, not only the lookup just before it.
2. `a_sidecar_refusal_keeps_frames_only_while_the_real_file_does_not_exist` (**F8**, the lead's
   wording): it also asserts "nothing has opened this database since" (panic: "the condition still
   exempts this build's own opens") and "except through" (panic: "the condition does not name the
   one open that keeps the frames").
3. `a_name_too_long_for_branch_files_opens_volatile` (**F3**): its doc comment only. `-branch-log`
   stops fitting at 245 bytes, not 244.

No test is added or removed: there are still 118 `#[test]` attributes under `core/branch/`, 117 of
which run on unix.

**P23. The red commit (tests only; code `70bd2c410`).** `cargo test -p turso_core --lib branch::`
gives **116 passed, 1 FAILED**. The failure is the keeps-frames test, and its panic contains
"exempts this build's own opens". The ATTACH test PASSES: F2's exemption hands the ATTACH the held
instance (INFERRED, review 10 §2). If `ptr_eq` fails, that is a finding against the exemption or the
test, and not a test to edit.

**P24. F2's red against the code before F2, with the red commit's test file.** Run `git checkout
--detach e472b108e && git checkout <red> -- core/branch/durability_tests.rs`, then the ATTACH test
alone. It gives **1 FAILED**, and its panic matches both `leased\.db: [^|]*is open in this process
with default branch lease` and `durable\.db: [^|]*is open in this process with branch durability`.
No panic contains "premise". The `ptr_eq` check runs only after a successful ATTACH, so it never
runs here. The same patterns replace P17's: the `ff7445f8a` file's panic carries the same registry
messages, so the stronger patterns also hold for it (INFERRED).

**P25. The fix commit.** `branch::` gives **117 passed, 0 failed**. The full `cargo test -p
turso_core --lib` gives **2466 passed, 0 failed, 17 ignored**, unchanged.

**P26. Fire-check at the fix commit.**
- There are **114 mutants**. `OTHER_a` and `OTHER_b` are re-anchored to the F8 wording. `OTHER_c`,
  new, reverts "nothing has opened" to "no other build or program has opened", and is killed by the
  keeps-frames test.
- The audit reports no problems.

**P27. Runner exit mapping (F1), no cargo.**
- The entry point is `main(argv, env, …) -> int`. `__main__` is `sys.exit(main(…))`, with a fake
  tree chosen only by `--fake-run <case>`.
- `--self-test` asserts `main` returns **0** on a clean fake run, **1** with collateral under an
  expected survivor, **2** with collateral under the canary, and 2 on a dirty tree. It asserts the
  same three codes as PROCESS exit statuses, through `--fake-run` subprocesses of this very file.
- `selftest_plants.py` adds three plants, and each must be caught:
  - r1: the verdict-to-exit mapping forced to 0;
  - r2: `Refused` mapped to 0;
  - r1′: `__main__` exiting 0 whatever `main` returns.

**Falsifiers for A5:**
- In P23, any other failure, or the keeps-frames test failing on another assertion.
- In P24, a "premise" panic, or either half matching only the old, weaker pattern.
- In P25, the ATTACH test's `ptr_eq` premise failing: the ATTACH did not receive the held
  instance.

**A5 addendum (appended after the fix commit `7e7a841e1`, before any build or run).** P26 said **114
mutants**. There are **115**. `OTHER_d`, which drops ", except through {ours}", was added so the
keeps-frames test's second new assertion ("except through") has a mutant of its own. It is killed
by that test. Nothing else in A5 changes.

### A6 — 2026-09-24: review 11 — the F8 exemption is withdrawn (appended before any code of it)

Nothing above this amendment changes. Source: `frontier/turso_review11.md` @ artie-research
`25c29a7` (UNSOUND on one item, at `52df80c7a`), with the lead's decisions. None of these tests has
been built or run.

**A REVERSAL of review 10's decision F8, by the lead.** A5 adopted "nothing has opened this database
since, by any name, {real} included, except through {ours}". That wording was the lead's review-10
decision ("F8, my decision: adopt the wider wording …", quoted verbatim in the lane report), and the
lead now reverses it.
- The reason (review 11 finding 1, which the lead checked against `database.rs` ~488-491): SQLite
  opening `{real}` uses `{real}-wal`, and that IS `{ours}`.
- So ", except through {ours}" re-admits exactly the openers that delete `{ours}` after writing
  through it: SQLite at its last close, a manual `rm`, a backup tool. Review 9's decision 2 closed
  that case.
- It also contradicted the message's own next clause.
- The condition is now "nothing has opened this database since, by any name, {real} included",
  with no exemption.

**Test changes.** They go in the commit right after this amendment, which holds tests only.
1. `a_sidecar_refusal_keeps_frames_only_while_the_real_file_does_not_exist`: the assertion
   `err.contains("except through")` is removed (the lead's decision). In its place is the negative
   `!err.contains("except through")`, whose panic is "the condition exempts opens through {ours},
   which a program can delete after writing". This is beyond the letter: it gives the withdrawal a
   red test, and it keeps the exemption from coming back unseen.
2. No other test changes. The new mutants (below) are killed by existing assertions.

**P28. The red commit (tests only; code `52df80c7a`).** `cargo test -p turso_core --lib branch::`
gives **116 passed, 1 FAILED**: the keeps-frames test, with a panic containing "exempts opens
through". The four assertions before it hold at `52df80c7a`.

**P29. The fix commit.** `branch::` gives **117 passed, 0 failed**. The full `cargo test -p
turso_core --lib` gives **2466 passed, 0 failed, 17 ignored**, unchanged.

**P30. Fire-check at the fix commit: 118 mutants.**
- Removed: `OTHER_d`, whose assertion is gone.
- New: `OTHER_e` restores ", except through {ours}", and is killed by the negative assertion.
- New: `ATT_d` makes an ATTACH skip BOTH registry lookups (`resolve_default_storage`'s
  `if use_registry {` and `open_async`'s Init lookup). It is killed by the ATTACH test. The test
  dies in one of two ways, depending on the lock primitive (review 11 finding 2):
  - the second instance opens, and `ptr_eq` panics "received another instance";
  - or the second open is refused with a locking error, and the test's final assert lists it.

  Either is KILLED. The mutant's LOG records which one happened, and the batch reads it.
- New: `LEASE_b` rewords "is open in this process with default branch lease" (the lead's
  decision). Beyond the letter, `REG_c` rewords "is open in this process with branch durability".
  Each is killed ONLY by the ATTACH test's negative control ("refused for another reason"). The
  registry tests' own looser `contains` still pass under them.
- Re-anchored: `KEEP_a`, `OTHER_a` and `OTHER_b`, to the new text.

**P31. The runner (no cargo).** A subset run (`firecheck_run.py <out> NAME …`) that names an
unknown mutant is REFUSED, and returns 2. `--self-test` asserts it, and a plant that removes the
refusal is caught.

**Also in the fix commit, doc and wording only (review 11 findings 4–6):**
- Condition 2's reason becomes "any read-write open of {real} by this build with its default
  sidecars would have created it". A read-only open creates no WAL, and the same message serves
  the MVCC log.
- The code comment names `do_open_async`, the production custom-WAL open (the sync engine's revert
  database), as well as the test-only `do_open`.
- The limitations doc's second bullet also covers a read-only ATTACH that misses a database with
  no branch files.

**Falsifiers for A6:**
- In P28, any other failure.
- In P29, a red ATTACH or keeps-frames test.
- In P30, `ATT_d` SURVIVED: the ATTACH test does not notice a second instance, and so `ptr_eq` is
  not live.
