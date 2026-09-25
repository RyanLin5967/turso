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

### Amendment 1 — 2026-09-24T23:31Z: four agent-workload arms, pre-registered before any of them is built or run

Context, recorded before the arms: the smoke and the full curve above ran at `c5caf85d0`, release, no
compile fix (artie-research `frontier/round10/turso_curve/raw/{build_release,smoke,curve}.txt`, commits
`7accc8d`, `2e105b7`). Nothing above the line changes; this amendment adds runs, it does not reinterpret.

**What this amendment adds to the tree.**

1. **Work counters in the engine (observation only).** `BranchStats.work: BranchWork` — cumulative
   `resolve_calls`, `resolve_levels` (nodes visited per resolution), `resolve_retained_examined`
   (versions compared by `Lineage::retained_at`), `gc_examined` (versions compared by `child_gone`'s
   `position` scan), `gc_range_entries` (entries visited by `child_gone`'s range query). Each is one add
   per call (or per range entry) under the lock the call already holds, computed from the index the scan
   returns — no per-element step is added to any scan. The mechanism never reads them. **Control:** the
   unchanged `branch_curve` harness is re-run on the instrumented build (10^2..10^6, K=200); P1 must still
   hold (every |slope| < 0.10). If it does not, the counters perturbed the thing measured and every arm
   below is re-run without them.
2. **`core/examples/branch_arms/main.rs`**, one binary, `--arm <name>`. Same instrument as the curve
   (`Instant` per op, tick printed; RSS from `memory_stats`), same refusal rule (engine counts checked
   against the workload before a number prints; `NOT A RESULT` + exit 1). Every sampled read is checked
   against a model the harness keeps itself (the trunk's write history), never against the engine.
   Beside each op's percentiles it prints that op's engine work per sample. **Trunk commits run with
   `PRAGMA synchronous = OFF`** in every arm: the fsync is not the mechanism, and arms a/c-hot commit on
   the trunk up to 10^6 times.

**Pre-registered runs** (each under `lockrun.sh turso-curve`, `timeout 3600`, raw to artie-research
`frontier/round10/turso_curve/raw/`, committed before it is read):

| run | command (after `branch_arms`) |
|---|---|
| control | `branch_curve --checkpoints 100,1000,10000,100000,1000000 --samples 200` |
| a1 | `--arm hot --checkpoints 100,1000,10000,100000,1000000 --samples 200` |
| a2 | `--arm spread --checkpoints 100,1000,10000,100000,1000000 --samples 200` |
| b | `--arm chain --checkpoints 1,3,10,32,100,316,1000 --samples 200` |
| c1 | `--arm churn --checkpoints 100,1000,10000,100000,1000000 --cycles 2000000 --windows 10` |
| c2 | `--arm churn_hot --checkpoints 100,1000,10000,100000,1000000 --cycles 100000 --windows 10` |
| d-w | `--arm pages --w 1,2,4,8,16,32,64 --checkpoints 10000 --samples 200` |
| d-N | `--arm pages --w 8 --checkpoints 100,1000,10000,100000 --samples 200` |

**Predictions, READ from `core/branch/store.rs` (not measured).** "Slope" = least-squares log-log slope of
p50 over the run's x grid; "local" = the slope between its last two x values; "counter slope" = the same
fit on the op's work counter per sample. A **wall** is |p50 slope| ≥ 0.25 (the falsifier above). An op
predicted flat that lands in (0.10, 0.25) is inconclusive: its arm is re-run once with `--samples 1000`
and the re-run is the reading. A **break** is any measured value outside the interval pre-registered
here — including a predicted wall that fails to appear.

**(a) The trunk writes between forks.** Every fork is followed by one autocommit same-length UPDATE on
the trunk. `first_write_trunk` retains the pre-image iff a live child forked since the page's last trunk
write — with one write per fork, always — so each trunk write retains exactly ONE page (an in-place
UPDATE dirties only its leaf; nothing in the commit path rewrites page 1: `database_size` is written only
by `allocate_page`). The harness asserts `arena_in_use == N + retained` from its own count of trunk writes
and the engine's per-reap `freed_pages`; a mismatch (e.g. page 1 retained too) is NOT A RESULT.
- **a1 `hot`: the trunk rewrites row 1 every time.** `trunk.lineage.retained[leaf(1)]` is a `Vec` with
  one version per live child, in fork order. `retained_at` scans it front to back; `child_gone` finds
  the dead version with `position` (front to back) and `swap_remove`s it.
  - `read_hot` (SELECT row 1 on a fresh connection of a random live branch): `ret_examined_per_op` ≈ N/2,
    counter slope in [0.9, 1.1]. **WALL predicted:** p50 slope in [0.25, 1.0], local in [0.5, 1.05].
    Derived per-version cost (Δp50/Δexamined, 10^5→10^6) in [0.2, 5] ns.
  - `reap` (the K sampled branches, the newest, whose versions sit at the Vec's END): `gc_examined_per_op`
    ≈ N, counter slope in [0.9, 1.1]. **WALL predicted:** p50 slope in [0.25, 1.0], local in [0.5, 1.05].
  - `trunk_write`: O(log N) BTreeMap insert + amortized Vec push: |slope| < 0.10 (no claim on max: the
    Vec's doublings copy up to 24 MB).
  - `fork`, `open`, `first_write`, `read_open`, `read_own`, `read_inh`: |slope| < 0.10 (none touches
    leaf(1) except by 1-in-545 chance).
  - Space: arena == 2N (asserted); RSS / N at 10^6 in [8.2, 10.5] KB.
- **a2 `spread`: the trunk's g-th write rewrites row (37·g mod 20000)+1**, walking every leaf in turn,
  so each of the ~545 leaves holds ~(N+K)/545 versions.
  - `read_inh`, `read_hot`, `read_own`: `ret_examined_per_op` grows ∝ N once N ≫ 545: counter slope over
    the grid ≥ 0.5, and `read_inh`'s at 10^6 in [300, 2000] (one leaf per descent carries versions; the
    reader's is at a uniform position). **No wall at 10^6:** p50 |slope| < 0.25 for every op (the scan is
    ~10^3 versions, [0.2, 5] ns each, on top of ~6 µs). This arm exists to show an O(N/leaves) term the
    timings cannot yet see.
  - `fork`, `open`, `first_write`, `trunk_write`, `read_open`, `reap`: |slope| < 0.10.

**(b) `chain`: one chain trunk → b1 → … → bd**, each level writing one first-half row and then forking the
next; x = d ∈ {1, 3, 10, 32, 100, 316, 1000}. `resolve` visits one node per level until one holds the page.
Sampled `fork`/`open`/`first_write`/`reap` act on a fresh child of the tip (depth d+1); the reads act on
the tip (depth d) on fresh connections.
- Resolutions per op (from the descent: page 1 at connect; root, one interior level, leaf per SELECT):
  `open`/`read_open` 1 (range [1, 2]), each SELECT 3 (range [2, 4]).
- `levels_per_op` ≈ resolutions × (d+1) for pages no level wrote: `read_inh` ≈ 3(d+1); `read_own`
  ≈ 2(d+1)+1; `read_anc` (row written by b1) ≈ 3d+2. Counter slope over the grid in [0.85, 1.05] for
  `open`, `read_open`, `first_write`, `read_own`, `read_inh`, `read_anc`.
- **WALL predicted** (the design says it: "cost grows with DEPTH"): p50 slope in [0.25, 0.85] and local
  (316→1000) in [0.5, 1.0] for `first_write`, `read_own`, `read_inh`, `read_anc`; `open` and `read_open`
  (one resolution) in [0.15, 0.6], a wall only if ≥ 0.25. Derived per-level cost
  (Δp50/Δlevels, d=1→1000) in [10, 150] ns.
- `fork`, `reap`: |slope| < 0.10 (neither walks the chain).
- The cascade: after the grid, every ancestor handle is released (each deferred) and the tip is reaped;
  that one call frees all d pages (asserted). One sample, printed, no bound.

**(c) Churn at a steady N.** Each cycle forks and writes a new branch and reaps a uniformly random older
one (the harness's `swap_remove` of a random index).
- **c1 `churn`** (2·10^6 cycles per N, 10 windows): every op's pooled p50 |slope vs N| < 0.10; drift:
  for every op at every N, last-window p50 / first-window p50 in [0.75, 1.33]. Space: `arena_in_use == N`
  and high-water == N+1 at every window (the free list is LIFO; each cycle's reap returns the slot the
  next cycle takes); RSS at the last window within ±10% of RSS before churn; every reap frees exactly 1.
- **c2 `churn_hot`** (c1 plus one trunk write of row 1 per cycle; 10^5 cycles per N): each child owns
  exactly one version of leaf(1), so every reap must free exactly 2 pages and `arena_in_use == 2N` at
  every window (no retained-version leak under turnover); high-water ≤ 2N+2.
  - `reap`: `gc_examined_per_op` ≈ N/2 (the victim's version is at a uniformly random position once
    `swap_remove`s have shuffled the Vec), counter slope in [0.9, 1.1]. **WALL predicted:** p50 slope in
    [0.25, 1.0].
  - `read_hot` (every 10th cycle): `ret_examined_per_op` ≈ N/2. **WALL predicted:** [0.25, 1.0].
  - `fork`, `open`, `first_write`, `trunk_write`, `read_own`: |slope| < 0.10.

**(d) `pages`: each branch writes w rows on w distinct leaves** (rows 312 apart; asserted: arena == N·w)
in one BEGIN … COMMIT.
- d-w (N = 10^4, w ∈ {1..64}): `write_w` slope vs w in [0.75, 1.05] (w CoW copies + w commit copies +
  w statements), per-page cost `write_w`/w at w=64 in [3, 15] µs; `reap` slope vs w in [0.1, 1.05]
  (w slot releases on a ~0.3 µs base); `fork`, `open`, `read_own` |slope vs w| < 0.10 (a branch's own
  map is one hash probe at any w).
- d-N (w = 8, N = 10^2..10^5): every op |slope vs N| < 0.10; `read_own` step ≤ 2× (P2's bound; the arena
  is 8× larger at each N); RSS / N at 10^5 in [32.8, 44] KB.

**Known limits, stated before the arms run.** Single thread; volatile arena; trunk `synchronous=OFF`;
one hot row, not a hot set; K = 200; the chain is one chain (N = d); the counters count scans, and a scan's
cost per element is inferred from Δtime/Δcount, never measured alone.

### Amendment 2 — 2026-09-24T23:49Z: arm b's harness defect; the inconclusive-band re-runs; attribution runs for a2's unpredicted trunk_write step

Recorded after reading a1 (artie-research `bfd5a30`), a2 (`03c5e7e`), b (`2a6433b`), c1 (`b18db94`), c2
(`306924b`), d_w (`d25145d`), d_N (`a3bdecb`), and before any run below. Amendment 1's predictions do not change.

**1. Arm b refused itself (NOT A RESULT at d=1): a harness defect, fixed.** Sampled child i wrote row
`chain_row(10^6+i)`, which equals `chain_row(i)` (10^6 ≡ 0 mod 10^4): the row chain level i had already written,
with the same `branch_value`. Turso's `OpInsert` no-op check (`core/vdbe/execute.rs`, `is_noop_update`) skips the
physical write of an identical payload, so nothing was dirtied, nothing copied, and the reap freed 0 pages. At d ≥ 2
every sample i ≤ min(d, 199) would have been such a no-op. Fix: a sampled child writes `c` + its zero-padded index
(same length; no row holds it). It changes what arm b's `first_write` writes (a real write instead of a no-op) and
nothing else. Re-run as `b_chain_r2`, same arguments. Also: the runner's progress file printed "rc=0" for b because
`$(date)` reset `$?`; each raw file's own `# rc=` line is authoritative.

**2. Inconclusive-band re-runs, as amendment 1's rule requires** (flat-predicted ops that landed in (0.10, 0.25)):
a2 `fork` +0.109 and `reap` +0.108; c1 `reap` +0.214; d_w `fork` +0.202 (slope vs w). Each arm is re-run once with
`--samples 1000`, other arguments unchanged; the re-run is the reading: `a2_spread_r2`, `c1_churn_r2`, `d_w_r2`.
For c1, `--samples` does not enter the churn loop (every cycle is timed), so `c1_churn_r2` is a second draw at a
later moment, identical in design; said here so it is not mistaken for a larger sample.

**3. a2's trunk_write step (unpredicted; the prediction was |slope| < 0.10, measured +0.312).** 10.92 µs at x=10^2,
then 370–400 µs at every x from 10^3 to 10^6 (local 10^5→10^6 +0.005): a step after ~10^3 trunk commits, absent from
a1 (10.5–12.6 µs to 10^6) whose trunk rewrites one page. No mechanism is named before these runs. Held beforehand:
- H1 (the engine, not branching): after the first auto-checkpoint (threshold 1000 frames, `wal.rs` `should_checkpoint`),
  a trunk commit pays work ∝ the distinct pages the WAL holds (~545 in a2, 1 in a1).
- H2 (branching): a trunk commit pays for something the branch mechanism adds — per-page retention bookkeeping, or
  branch connections' WAL read snapshots holding back checkpoint progress so that commits keep re-attempting it.

Runs (K = 200):
- **e1** `--arm spread_trunk --checkpoints 100,1000,10000`: the same trunk write sequence, no branch ever forked
  (`first_write_trunk` is never called: `trunk_has_children()` is false).
- **e2** `--arm spread --no-autocheckpoint --checkpoints 100,1000,10000`: a2 with the trunk connection's WAL auto-actions
  disabled (`Connection::wal_auto_actions_disable`: no auto-checkpoint, no WAL restart).
- **e3** profile: `--arm spread --checkpoints 1000,30000` with `/usr/bin/sample <pid> 5` taken during the growth from
  10^3 to 3·10^4. Its timings are not results; its call tree is attribution evidence.
- Every state line now prints the WAL file's size (observation only).

Decision rule (a step = trunk_write p50 at x=10^4 ≥ 5× its value at x=10^2):
- e1 steps → an engine property of Turso's WAL path under a ~545-page write set, independent of branching; reported
  as such, not as a branching wall.
- e1 flat and e2 flat → needs branches AND the auto-checkpoint: a branching × checkpoint interaction; e3 names frames.
- e1 flat and e2 steps → branching, not the checkpoint; e3 names frames.
Whatever e3 shows is labelled a candidate unless e1/e2 isolate it.

### Amendment 3 — 2026-09-25T00:21Z: FIFO victims and a `churn_spread` arm, run on the UNFIXED store first

Recorded by lane `turso_sota` (artie-research `frontier/round10/turso_sota/`), which builds the published fixes for
three walls this curve found and re-measures. Amendments 1-2 are unchanged; this adds runs.

**Why.** Uniform-TTL lease expiry reaps the OLDEST branch. No banked arm does: a1/a2 reap their samples in fork
order on top of the live set, and c1/c2 reap a uniformly random one. The FIFO cost is predicted from source and a model
only (`assumption_audit.md` item 3, `cand_turso-fifo-child-gone.md` §1c, `cand_turso-hot-page-chain.md` §2).

**Harness** (`795e2e42b`, no engine change): `--victim oldest|random` for the churn arms (default random; live branches
move to a `VecDeque`, and `swap_remove_back` is `Vec::swap_remove`, so random-victim runs draw and remove as before);
arm `churn_spread` = `churn` plus one `spread_row` trunk write per fork and per cycle, a `read_inh` of the row 10,000
away from the branch's own every 10th cycle (checked against the harness's model), and arena conservation
(`N + retained − freed`) asserted at every window. **Binary:** built from `795e2e42b` (store = `751f85d56`'s;
`turso_sota/raw/build_H.txt`), copied out of the worktree as sha256 `350a1df1…79d6` so the tree can take the fixes
while these run; each raw header records the path and `built_from`.

**Runs** (`turso_sota/run.sh`, lockrun `turso-sota`, timeout 3600, raw to `turso_sota/raw/`, committed before read):

| run | arguments |
|---|---|
| c2f_base | `--arm churn_hot --victim oldest --checkpoints 100,1000,10000,100000,1000000 --cycles 1000000 --windows 10` |
| c3_base | `--arm churn_spread --checkpoints 100,1000,10000,100000,1000000 --cycles 100000 --windows 10` |
| c3f_base | `--arm churn_spread --victim oldest --checkpoints 100,1000,10000,100000,1000000 --cycles 100000 --windows 10` |

c2f runs 10^6 cycles so that each checkpoint's FIFO turns its whole population over: with fewer cycles than N the
victims are only the previous checkpoint's survivors and the position scan reads about min(cycles, N)/2, not N/2.

**Counter predictions** come from `turso_sota/model.py` (a transcription of this store's trunk lineage and of this
harness, RNG included), committed with its outputs (`turso_sota/model_out/`) at artie-research `d37719e`, before
these runs. Its calibration (`model_out/calib.txt`) reproduces printed engine counters exactly where the page mapping
is not involved — c2 reap `gc_examined` 50.47 / 502.07 / 4999.44 / 50135.47, a1 reap 150.50 … 100050.50, a2_r2 reap
`gc_range` 248.64 and `gc_examined` 87.40 at 10^5, a2 `read_inh` 0.51 / 2.12 / 10.38 / 94.06 — and `read_hot` within
2%. Its leaves hold 37 rows (541 leaves); the engine's own leaf count is not read, so spread-arm counters carry that.
- **c2f_base.** reap `gc_examined_per_op` = (N+1)/2 ± 2% (model 50.50 … 500000.50), `gc_range_per_op` 1.00;
  `read_hot` `ret_examined` = model ± 3% (50.47 … 498672.97); every reap frees 2 pages, arena = 2N at every window.
  **Wall:** reap and `read_hot` p50 slope in [0.4, 1.0], local 10^5→10^6 in [0.7, 1.05] (c2, random victim, measured
  +0.621 / +0.966 and +0.322 / +0.866: same counters, so the same slopes within the inconclusive band).
- **c3_base** (random). reap `gc_range_per_op` = model ± 20% (5.30, 6.57, 5.28, 2.03, 1.21), `gc_examined_per_op`
  ± 30% (1.00, 5.50, 22.69, 4.84, 1.93), versions freed per reap ± 15% (0.996, 0.947, 0.517, 0.061, 0.025);
  `read_inh` `ret_examined` ± 15% (0.95, 9.04, 58.70, 241.34, 1225.36); `first_write` ± 15% (0.95, 11.90, 95.73,
  371.52, 2212.05). No wall: reap, `read_inh`, `first_write` p50 |slope| < 0.25 (a2's 955-version `read_inh` scan
  cost ~1.5 µs at 10^6). `trunk_write`: the engine's auto-checkpoint step of e1 (≈ 375–400 µs from 10^3 on).
- **c3f_base** (oldest). reap `gc_range_per_op` 101 ± 3% at 10^2 and the leaf count, 541 ± 3%, at 10^3…10^6: the
  born range (−∞, f] holds one version per leaf rewritten since the victim forked; versions freed per reap 1.00 ± 3%;
  `gc_examined_per_op` ± 30% (1.00, 1.00, 9.32, 92.96, 92.96); `read_inh` ± 15% (0.18, 1.65, 10.02, 93.35, 935.17).
  **Step, not slope:** reap p50 at every N ≥ 10^3 at least 2× c3_base's reap p50 at the same N, and |slope| over
  10^3…10^6 < 0.25, because the term is bounded by the trunk's leaf count, not by N.

A counter outside its interval is a break of the model, reported as such, and the model is not refitted to it.

### Amendment 3a — 2026-09-25T00:21Z: F3 (retain by reference to the WAL frame) is NOT built in this arena

Recorded before any fixed run, so the fixed runs are read against F1+F2 only. The published fix (Retro's
write-ahead-snapshot invariant, ATC 2014; Thresher/SNAP) moves a pre-image's capture off the commit path because
there it is synchronous I/O. Here it is not: `copy_on_write_decision` hands `first_write_trunk` the page already
resident in the trunk's page cache, and the arena is memory, so the capture is one slot allocation and one 4 KiB
memcpy under the store mutex, with no fsync (the durable build's two per retaining trunk commit do not exist in
this tree). Deferring it to the checkpoint would replace that memcpy with a later read of the WAL frame or database
page, and add a copy-out phase to `Pager::checkpoint_inner` and before `try_restart_log_before_write`, plus a branch
read validated against a concurrent backfill. It would also break `reaping_the_last_child_frees_what_the_trunk_
retained_for_it` and `a_retained_version_lives_exactly_as_long_as_a_child_that_can_see_it`, which assert that a
retention occupies an arena slot at the trunk write; this lane may not edit test assertions. Measured cost it would
remove: a1 `trunk_write` p50 is flat, 10.54–12.58 µs over 10^2..10^6 (`turso_curve/raw/a1_hot.txt`), against 10.71 µs
for the same kind of commit with no branch at all (e1, x=10^2): there is no growing term. F3 belongs to the durable
build (`ec168128b`), where the barrier pays an arena fsync and a log fsync per retaining trunk commit.

### Amendment 4 — written 2026-09-25T00:20Z: every arm re-run with F1 + F2 in the store, predictions before any fixed run

**Erratum to amendments 3 and 3a:** their headers say 00:21Z; they were committed at 00:19Z (`933e5df80`), before
any run they govern (`c2f_base` started after). The label was typed, not read from the clock.

**The store under test** (`0de3aa904` F1, `2d2653599` F2, `a3d79b98d` F2's cost-contract test):
- **F1** — each node's retained versions of a page sit in a `BTreeMap` keyed by `born` (the fat node with a search tree,
  DSST 1989). `retained_at` is a predecessor search; `child_gone` removes by key. `resolve_retained_examined` now counts at
  most one version per lineage consulted, and `gc_examined` one per version released.
- **F2** — the versions are also indexed by `died` (ZFS deadlists). `child_gone` queries `died ∈ (f, hi]` when the victim
  has no older live sibling, `born ∈ (lo, f]` when it has no younger one, and both in lockstep otherwise.
  `gc_range_entries` counts every entry either walk yields. A one-sided reap visits exactly what it frees; a two-sided
  one visits 2|B| if |B| ≤ |D|, else 2|D|+1. `store::tests` pins both, and five mutants fail it (`turso_sota/raw/mutate_F2.txt`).
- **F3** — not built (amendment 3a).

**Build and runs.** One release build of `branch_arms` and `branch_curve` from the commit that carries this amendment. It
is copied out of the tree like amendment 3's binary. Runs go through `turso_sota/run_fixed.sh` (lockrun `turso-sota`,
timeout 3600 each), raw to `turso_sota/raw/<run>_fix.txt`, each committed before it is read. The "before" of each run is
the banked `turso_curve/raw/` file named in the table; for the amendment-3 arms it is `turso_sota/raw/<run>_base.txt`.

| run | arguments (after `branch_arms` unless named) | before |
|---|---|---|
| control_fix | `branch_curve --checkpoints 100,1000,10000,100000,1000000 --samples 200` | control |
| a1_fix | `--arm hot --checkpoints 100,…,1000000 --samples 200` | a1_hot |
| a2_fix | `--arm spread --checkpoints 100,…,1000000 --samples 200` | a2_spread |
| a2r2_fix | `--arm spread --checkpoints 100,…,1000000 --samples 1000` | a2_spread_r2 |
| b_fix | `--arm chain --checkpoints 1,3,10,32,100,316,1000 --samples 200` | b_chain_r2 |
| c1_fix | `--arm churn --checkpoints 100,…,1000000 --cycles 2000000 --windows 10` | c1_churn_r2 |
| c2_fix | `--arm churn_hot --checkpoints 100,…,1000000 --cycles 100000 --windows 10` | c2_churn_hot |
| c2f_fix, c3_fix, c3f_fix | amendment 3's arguments | `<run>_base` |
| d_w_fix | `--arm pages --w 1,2,4,8,16,32,64 --checkpoints 10000 --samples 1000` | d_w_r2 |
| d_N_fix | `--arm pages --w 8 --checkpoints 100,1000,10000,100000 --samples 200` | d_N |
| e1_fix | `--arm spread_trunk --checkpoints 100,1000,10000 --samples 200` | e1_spread_trunk |
| e2_fix | `--arm spread --no-autocheckpoint --checkpoints 100,1000,10000 --samples 200` | e2_spread_nockpt |
| e3_fix | `--arm spread --checkpoints 1000,30000 --samples 200`, `/usr/bin/sample <pid> 5` taken after x=1000 prints | e3_profile |

**Counter predictions.** Where a value is given, it is `turso_sota/model_out/` "fixed" column. The model was committed
at `d37719e`, before this. A counter outside its interval is a break of F1/F2 or of the model, and it is reported, not
refitted.
- a1: `read_hot` `ret_examined_per_op` 1.00 ± 0.02 at every N. reap `gc_examined_per_op` 1.00, `gc_range_per_op` 2.00
  ± 0.02 (the samples are reaped oldest-sample first, so each has both neighbours, and |B| = |D| = 1). `first_write` ≤ 0.02.
- a2: `read_inh` and `read_hot` 1.00 ± 0.05 at N ≥ 10^3 (0.51 and 1.00 at 10^2). `first_write` 1.00 ± 0.05 at N ≥ 10^3.
  reap 0.00 / 0.00.
- a2r2: reap `gc_range_per_op` 3.00 ± 15% and `gc_examined_per_op` 0.46 ± 15% at every N. Reads 1.00 ± 0.05 at N ≥ 10^3.
- c2: reap `gc_examined_per_op` 1.00, `gc_range_per_op` 2.00 ± 0.02. `read_hot` 1.00 ± 0.02. `first_write` 0.00 ± 0.01.
- c2f: reap 1.00 / 1.00 ± 0.01. `read_hot` 1.00 ± 0.02.
- c3: reap `gc_range_per_op` ± 20% of (1.34, 7.03, 6.53, 3.03, 2.23). `gc_examined_per_op` = versions freed per reap ± 15%,
  with the same freed as c3_base. `read_inh` and `first_write` 1.00 ± 0.05 at N ≥ 10^3.
- c3f: reap `gc_range_per_op` = `gc_examined_per_op` = versions freed per reap = 1.00 ± 3% at every N. `read_inh` 1.00
  ± 0.05 at N ≥ 10^3.
- b, c1, d_w, d_N, control, e1: no retained version exists in the chain (each level writes before it forks), in c1 and d
  (the trunk never writes), or in e1 (no branch). F1 and F2 are therefore not exercised. Every counter equals its
  "before" file's exactly. e2: reads 1.00 ± 0.05 at N ≥ 10^3.
- Space: arena counts equal the "before" files' at every checkpoint, and the harness asserts them. RSS rises only by
  the indexes: per retained version, a `BTreeMap` entry replaces a `Vec` slot, and two `BTreeSet` entries replace a
  `retained_by_born` pair. Prediction: RSS at 10^6 within +3% of the "before" RSS in a1, a2, c2, c2f, c3, c3f.

**Time predictions.** These are inferred (I), and they are the hypotheses the residual-wall report tests.
- **Removed walls.** At 10^6, p50 is at least 20× lower than before for a1 `read_hot` (before 156.67 µs) and a1 `reap`
  (251.92 µs), c2 `reap` (129.75) and `read_hot` (160.83), and c2f's reap and `read_hot` (base). For each of these, the
  p50 slope over 10^2..10^6 is < 0.40, and the local slope 10^5→10^6 is < 0.40.
- **Residual term, predicted to show: log N descents at a cache-exceeding footprint.** A reap now does about 8 B-tree
  descents: `children` (remove plus two neighbour probes), the two index ranges, the per-page map, and the two index
  removals. In the retaining arms each tree holds ~N entries, and each descent level past the caches is a miss. So
  a1/c2/c2f `reap` p50 slope in [0.10, 0.40] and p50 at 10^6 in [1, 6] µs. c1's reap already reads +0.202 (c1_churn_r2)
  with only `children` and the branch `HashMap` at N. `read_hot` does one descent more than before plus the arena
  slot read: |slope| < 0.15.
- **c3f's step removed.** reap p50 at N ≥ 10^3 is at least 2× lower than c3f_base's. c3f's reap and c3's reap
  are within 2× of each other at every N.
- **Unchanged by construction (engine, not branching).** `trunk_write` in a2, c3, c3f and e1 keeps e1's auto-checkpoint
  step: ≈ 375–400 µs from x = 10^3, within ±15% of the before file at each N. WAL bytes grow with trunk commits as before.
- **Everything else** is within ±15% of its before file's p50 at every x, or within the before file's own window-to-window
  spread where that is larger. This covers `fork`, `open`, the `first_write`s that retain nothing, `read_own`, `read_open`,
  `read_inh` in a1, all of b, c1 and d, and control.

**What the report owes.** For each run, op and x: p50 before and after, the slope before and after, and the counters.
Every term that still grows with N is listed, with the mechanism read from source. That covers time, space (arena, RSS,
WAL bytes) and every counter. A term predicted above and not found is reported as not found.

### Amendment 5 — written 2026-09-25T00:50Z: `--synchronous`, and a2 under NORMAL on the fixed store; before any fixed run

No fixed run has started. The amendment-4 runner was stopped at 00:49:50Z, while it was still waiting for the lock
for its build. Its aborted build log is kept at `turso_sota/raw/aborted/`.

**Why.** The turso_curve lane attributed a2's `trunk_write` step and the WAL's linear growth to `synchronous=OFF`.
Under OFF, Turso's checkpoint does not publish its backfill, so every later commit checkpoints again and the log never
restarts. That is intended upstream (`test_checkpoint_sync_mode_off_leaves_backfill_unpublished`). The lane measured
it at `turso_curve/raw/e4_spread_trunk_normal.txt` and `e5_spread_normal.txt`: under NORMAL there is no step and the
WAL stays bounded. Under OFF that artifact hides the fixed store's own trunk-write term. At 10^6 each retaining trunk
write now inserts into three B-trees of ~10^6 entries: the per-page map, `by_born` and `by_died`.

**Harness.** This commit carries `--synchronous off|normal|full`, ported verbatim from turso_curve's `48a2b97a3`. The
default stays OFF, so every amendment-4 run behaves as registered. The fixed binary is built from THIS commit
instead of `22a345c01`. `core/branch/` is byte-identical between the two. The harness differs only by this flag.

**Run.** `e5_fix`: `--arm spread --synchronous normal --checkpoints 100,1000,10000,100000,1000000 --samples 200`.
Its before is `turso_curve/raw/e5_spread_normal.txt`.

**Predictions.**
- Counters as a2_fix: `read_inh` and `read_hot` 1.00 ± 0.05 at N ≥ 10^3, `first_write` 1.00 ± 0.05 at N ≥ 10^3, reap 0.
- Space: WAL ≤ 5 MB at every x (e5: 4.1 MB at 10^6).
- `trunk_write` p50 |slope| < 0.10, within ±20% of e5's at every x.
- Every other op within ±15% of e5's p50, except `read_hot` and `read_inh`, which lose their scans. At 10^6, e5 scanned
  944–955 versions per read on ~7.5 µs.

### Amendment 6 — written 2026-09-25T03:15Z: F4 (the depth wall), and the trunk-writing arms under synchronous=NORMAL; before any run it governs

Two inputs from the lead (2026-09-25 ~02:50Z): the depth wall of `turso_curve/REPORT.md` §2.2/§3 (read_inh 7.00 → 73.42 µs and
levels 6 → 3,003 = 3(d+1) over d = 1..1000, `turso_curve/raw/b_chain_r2.txt`) gets its standard fix, and the trunk_write
step B1 is Turso's synchronous=OFF checkpoint cliff (REPORT §4 B1), so the trunk-writing arms are run under NORMAL as e5 was.
**Seen before writing this:** a1_fix, c2_fix, c2f_fix (amendment 4, F1+F2, OFF) and c2f_base, c3_base, c3f_base (amendment 3).
Predictions below that lean on them say so.

**F4 = persistent page maps (path copying), turso `a31198dd8`.** Each branch carries `inherited`, a persistent 32-way radix
trie from page number to arena slot. It holds every arena page the branch sees through its ancestors, and it is the parent's
`view` at the fork: the parent's own `inherited` plus its current pages. The view is built at the parent's first fork, and the
parent's writes keep it up to date after that. A fork clones the view in O(1), and a write path-copies O(log_32 P) nodes. A
resolution looks at the branch's own current pages, then `inherited`, then the trunk at `trunk_at`, so it consults at most 2
nodes at any depth. The maps name slots and own none. Prior art: Driscoll, Sarnak, Sleator and Tarjan 1989 (path copying), and
copy-on-write B-trees, whose every snapshot has its own root, so a lookup costs the tree's height rather than the snapshot
chain's [RECALLED: Btrfs, Rodeh TOS 2008; LMDB]. The two options the lead named, and why neither:
- DSST node splitting gives O(1) amortised access for pointer structures of BOUNDED in-degree. A page map is an array of P
  entries, and fully persistent arrays are not known to get O(1) that way [RECALLED: Dietz 1989, O(log log) per access].
- A per-branch resolution cache (flatten-on-first-read, QEMU copy-on-read, Neon image layers) still walks d levels on the first
  read of each (branch, page). Arm b's `read_inh` reads a random second-half leaf on a fresh connection, so most of its K=200
  samples are first touches, and the wall would stay.
Tests: `page_map::tests` (3,000 versions against a copied `HashMap`) and
`store::tests::every_branch_of_a_random_tree_reads_its_parent_as_of_its_fork`. The latter drives the store's own entry points
against a model in which each branch copies its parent at the fork. It covers chains of depth ≥ 10, writes after forking, and
deferred reaps, and it asserts that each of these occurred. Results: `turso_sota/raw/tests_F4.txt`. Fire-check:
`turso_sota/raw/mutate_F4.txt`, run before any F4 run.

**Binaries.**
- `final` = F1+F2+F4, built from the commit that carries this amendment and copied out of the tree.
- `baseN` = the unfixed store (`751f85d56`'s `core/branch/`) under the `b8ca4be84` harness. Commit `105efa150`, built from
  `git archive` into its own target dir (`turso_sota/build_baseN.sh`).

**Runs** (`turso_sota/run_a6.sh`, lockrun, timeout 3600 each, raw committed before read).
- Depth, on `final`, OFF: `b_fin` (`--arm chain --checkpoints 1,3,10,32,100,316,1000 --samples 200`, before `b_chain_r2`) and
  `b_fin_r2` (same, `--samples 1000`, before `b_chain_r3`).
- The rest of `final`, OFF, same arguments as their before files: `control_fin`, `c1_fin`, `d_w_fin`, `d_N_fin`.
- NORMAL, every trunk-writing arm on both stores, `<arm>_nbase` and `<arm>_nfin`, with the amendment-4 arguments plus
  `--synchronous normal`: a1, a2, c2, c2f, c3, c3f.

**Predictions: b_fin and b_fin_r2.**
- levels_per_op:
  - `open`, `read_open`: 2.00 at every d. A 1-page resolution is the branch, then the trunk; page 1 is never written in the chain.
  - `read_inh`: 6.00. `read_own`, `read_anc`: 5.00.
  - `first_write`: in [5.0, 6.0] (the written leaf is the branch's or the trunk's).
  - Before, these were 3(d+1), 2(d+1)+1, 3d+2 and 2,105.68 at d=1000.
- p50 |slope| over d = 1..1000 < 0.10, and the local 316 → 1000 slope < 0.10, for `open`, `read_open`, `first_write`,
  `read_own`, `read_inh`, `read_anc`. `read_inh` at d=1000 ≤ 1.3× its d=1 value.
- `fork`: |slope| < 0.10 in b_fin_r2. B6's candidate, eviction by the walk, is gone with the walk. `reap`: |slope| < 0.10.
- Cascade: one sample, freed = d, asserted. It frees d pages, so it is O(d) by what it must do.
- Space: arena = d, asserted. RSS within the before file's ±5% at every d. The views add ≤ ~0.5 KB per level (I).

**Predictions: the other `final` runs, OFF** (`control_fin`, `c1_fin`, `d_w_fin`, `d_N_fin`). Every branch is at depth 1, so F4
changes only how the one level is looked up. Counters equal the before file's exactly. p50 is within ±15% at every x.

**Predictions: NORMAL.**
- Counters:
  - `_nbase` counters equal the model's "base" columns (`model_out/`), as in amendment 3/4. The sync mode does not enter the
    store.
  - `_nfin` counters equal its "fixed" columns. For c3_nfin they equal amendment 4's c3 predictions.
- `trunk_write` p50 |slope| < 0.10 in every NORMAL run, WAL ≤ 6 MB at every x, and `trunk_write` within ±25% of e5's
  (9.0–11.4 µs). This is B1 absent.
- `_nbase` walls, as in the OFF before files: a1/c2/c2f `reap` and `read_hot` slopes in [0.4, 1.0]. c3f_nbase reap is above
  2× c3_nbase's at N ≥ 10^3.
- `_nfin`:
  - Those walls are removed. At 10^6, p50 ≤ 1/20 of `_nbase`'s for a1/c2/c2f `reap` and `read_hot`, and ≤ 1/2 of it for
    c3f `reap`.
  - **Residual predicted from the seen OFF runs:** reap with a RANDOM victim (c2, c3) keeps p50 slope in [0.10, 0.30]. In
    c2_fix, 0.46 → 2.67 µs is +0.196, against c1's +0.202 with no retained versions at all (`turso_curve/raw/c1_churn_r2.txt`).
    The same locality term with a larger constant.
  - Reap with the OLDEST victim (c2f, c3f) or the newest (a1's samples) stays below 0.10 (c2f_fix +0.088, a1_fix +0.033).
    **This breaks amendment 4's own prediction** of [0.10, 0.40] for a1/c2f. That prediction assumed every descent misses
    the cache at 10^6, but oldest and newest victims touch the trees' edges.
  - Every other op within ±15% of `_nbase`'s p50 at x ≤ 10^4, where no scan is long. At larger x it is below `_nbase`'s.
- Space: arena counts equal `_nbase`'s exactly at every x in a1, a2, c2 and c2f. In c3/c3f, versions freed per reap equal
  `_nbase`'s exactly, because the same RNG draws the same victims and the GC frees the same set.

**What the report reads.** The residual list comes from `final` (NORMAL for the trunk-writing arms, OFF for b, c1, d and
control). Amendment 4's OFF runs attribute F1+F2 like-for-like against the curve lane's OFF before files.

**Erratum (appended 2026-09-25T03:09:41Z):** amendment 6 was committed at 2026-09-25T03:09:34Z (`2f2a2934d`); its header's "03:15Z" was typed, not read from the clock.

### Amendment 7 — written 2026-09-25T04:46:28Z: the concurrency axis, T threads at live N up to 10^6; before its first build or run

Lane `turso_conc` (agent `inv-turso-conc`), worktree `turso-inv-conc-noindex`, base `0f4232957` (F1+F2+F4, the `final`
store of amendment 6). Nothing below changes amendments 1-6 or any of their runs.

**Why.** Every run so far is single-threaded, so the store's one `Mutex` has never been contended (`turso_curve/REPORT.md`
§6). The store's own module doc calls that Mutex "a known wall under concurrent writers on different branches". BranchBench
(arXiv 2604.17180) reports Dolt's throughput plateauing at T=4 [from the lead's brief; not read by this lane].

**Engine change: lock accounting, observation only (this commit).** Every acquisition of the store's lock goes through
`BranchStore::lock`: `try_lock`, and only when that fails, a timed blocking `lock`. Four counters in `BranchWork`, written
under the lock itself: `lock_acquisitions`, `lock_contended` (acquisitions whose `try_lock` failed), `lock_wait_ns` (their
wait, clock read by the waiting thread only) and `lock_hold_ns`. The last counts only while lock timing is on
(`Database::set_branch_lock_timing`, default off). It is the one counter that adds work inside the critical section: two
clock reads per acquisition. Nothing in the mechanism reads the counters. `core/sync.rs` re-exports parking_lot's
`MutexGuard` (the shuttle adapter already defines one). Fire-check, run before any conc run:
`store::tests::lock_accounting_counts_a_forced_wait_and_nothing_else`. One thread makes 11 calls and must count exactly 11
acquisitions, 0 contended, 0 wait, 0 hold. A thread holding the lock 500 ms while another asks must produce exactly 1
contended acquisition, wait > 0, and hold ≥ 500 ms with timing on.

**Harness: `branch_arms --arm conc`.** One trunk table as in every arm (20,000 rows, checkpointed; the trunk never writes
here, so nothing is retained). N branches are grown single-threaded, each forked from the trunk and writing one row. Then,
at each N, one cell per T in the `--threads` list, first forward (draw 0) and then reversed (draw 1). Each cell:
1. The null workload: T threads, each running the same 10^8-step xorshift loop, sharing nothing. `null_ops_per_s` is the
   box's parallelism at that moment.
2. The N live branches are dealt round-robin into T shares. Each thread gets its own trunk connection.
3. After a barrier, each thread runs C = `--cycles` cycles:
   - `fork` a branch from its trunk connection;
   - `open` a connection on it;
   - `first_write`: one autocommit UPDATE of row `row_for(g)` on it; drop the connection;
   - `read_open` a connection on a random branch of the share;
   - `read_own`: SELECT that branch's own row;
   - `read_inh`: SELECT a far row, the trunk's, on the same connection; drop the connection;
   - `reap` a random branch of the share. The new branch joins the share after the victim is drawn, so N is fixed.
4. Every read is checked against the value the harness knows. Every reap must free exactly 1 page, not deferred. After the
   cell, the engine must report exactly N live branches and N arena pages. Otherwise the run prints `NOT A RESULT` and exits 1.
5. `Busy`/`BusySnapshot` from any op is retried after a `yield_now` and counted per op. A trunk fork holds the trunk's WAL
   write lock (`mod.rs` `fork_trunk`), so two threads forking at once is the one expected source of Busy. Any other
   error is not a result.

Printed per cell:
- per op: p50/p90/p99/max over T·C samples, plus its Busy retries;
- `cellsum`: cycles, wall (barrier release to the last join), `cycles_per_s`, `null_ops_per_s`, the slowest and fastest
  thread's elapsed, process user and sys CPU (getrusage), the four lock counters over the cell, and RSS.

The two connection drops (`Store::close`) sit inside the wall time but outside every timed op.

**Definitions (pre-registered).**
- X(N,T,d) = T·C / wall. S(T) = X(T)/X(1) and Sn(T) = null(T)/null(1), each within one N and one draw.
- E(T) = S(T)/Sn(T): parallel efficiency against what the box gives T threads that share nothing.
- **Scales** at T: E(T) ≥ 0.7 in both draws. **Wall** at T: E(T) < 0.5 in both draws. Anything else is inconclusive and
  reported so. When X for the two draws of one (N,T) differs by more than 15%, the cell has drifted and no verdict is taken.
- W = lock_wait_ns / (T · wall): the fraction of thread-time spent waiting for the store's lock.
- U = lock_hold_ns / wall: the fraction of wall time the lock is held (`conc_hold` only).
- The store lock **is the wall** at (N,T) when all three hold: E < 0.5, W ≥ 0.25 (`conc_main`), and U ≥ 0.6 (`conc_hold`).
- When E < 0.5 but W < 0.10, the wall is outside the store lock. It is attributed only through `conc_prof`'s profile, and
  it stays a candidate until a counter confirms it.

**Runs** (`turso_conc/run_conc.sh`: lockrun `turso-conc`, `taskpolicy -b` for the build, `timeout` on every step, each raw
file committed before it is read):
1. `tests_conc`: `cargo test -p turso_core --lib branch::`. Every branch test must pass, the fire-check included, or
   nothing below runs. Then `mutate_conc`: two mutants of the accounting, each applied to `store.rs`, tested the same way,
   and restored from git. M1: `try_lock` replaced by a blocking `lock`, so nothing is ever counted contended. M2: the hold
   time is never added. Each must fail the fire-check, or nothing below runs.
2. `build_conc`: a release build of `branch_arms` from the commit that carries this amendment, copied out of the tree.
3. `conc_smoke`: `--arm conc --threads 1,2 --checkpoints 16 --cycles 200`. Assertions only; NOT A RESULT.
4. `conc_main`: `--arm conc --threads 1,2,4,8,16 --checkpoints 1000,100000,1000000 --cycles 20000 --lock-timing off`.
   Throughput, latency, W. All E and S verdicts are read from this run.
5. `conc_hold`: the same arguments with `--lock-timing on`. Gives U. Its X against `conc_main`'s is the cost of the
   instrument.
6. `conc_prof`: `--arm conc --threads 16 --checkpoints 1000,1000000 --cycles 60000`. `/usr/bin/sample <pid> 3` is taken
   1 s after each `# cell N=… T=16 draw=0 start` line. Only the two profiles are read from this run.

**Predictions** (read from source; each is inferred, I, unverified):
- **P7.1 (integer, load-immune).** Lock acquisitions per cycle:
  - identical within ±0.5% across T and draws at each N, and across N;
  - in [19, 40]. That is 9 resolutions (open 1, first_write 3, read_open 1, read_own 3, read_inh 1, as the banked
    `resolves_per_op` of `c1_fix`/`a1_fix` read with the root cached for read_inh), fork 1, store.open 2, begin_write,
    first_write_branch, commit_pages and end_write 4, close 2, release_handle 1, plus an unknown number of
    `holds_writer` calls from the VDBE.
  - An `open` Busy retry adds 2 (open, then close); the report subtracts those.
- **P7.2.** `lock_contended` = 0 exactly at T=1, where one thread holds every acquisition. `busy_fork` = 0 exactly at T=1.
  Both > 0 in every T=16 cell. A zero there means the detector did not fire; that is not a clean result.
- **P7.3 (hold, `conc_hold`, T=1).** Hold per cycle:
  - at N=10^6, in [1, 10] µs;
  - at N=10^6, at least 1.3× its value at N=10^3. The branch-state lookups and the reap's tree walks miss the caches at
    10^6, and every one of them runs under the lock.
- **P7.4.** X(1) in [15k, 35k] cycles/s at every N. The op p50s summed over one cycle, from `control_fix`/`c1_fix`, come to
  ~35 µs, plus the drops.
- **P7.5 (the wall).**
  - At N=10^6: E(16) < 0.5 in both draws, with W(16) ≥ 0.25 and U(16) ≥ 0.5.
  - The Amdahl bound from P7.3's hold is s = hold/cycle ≈ 0.12, so S(16) ≤ 1/(s + (1-s)/16) ≈ 5.7. Sn(16) on 6 Super + 12
    Performance cores is ≈ 12-14, if the box is otherwise idle.
  - At N=10^3 (s ≈ 0.045, S(16) ≤ ≈ 9.5): no verdict is predicted.
  - E(16) at 10^6 < E(16) at 10^3 in both draws.
  - E(2) ≥ 0.8 at every N.
- **P7.6.** Correctness: no NOT A RESULT; after every cell, arena = live = N; every reap frees exactly 1 page.
- **P7.7 (the instrument's cost).** `conc_hold`'s X is within ±10% of `conc_main`'s at T=1 at every N, and ≥ 0.8× at T=16.
  If that breaks, U is reported as perturbed: an upper bound on the unperturbed hold.

**Falsifiers.**
- E(16) ≥ 0.7 at N=10^6 in both draws: no wall at T ≤ 16 on this box for this workload. W and U are then reported as
  headroom.
- E(16) < 0.5 with W < 0.10: the wall is not the store lock. The profile names a candidate; the fix waits for a counter.
- Acquisitions per cycle differing across T by more than 0.5% after open retries are subtracted: the counter or the
  harness is broken, and no lock attribution is made.

**What follows.** If a wall is found, amendment 8 names the standard fix and its prior art, and pre-registers its
predictions, before its build. It re-runs `conc_main`, `conc_hold` and `conc_prof` unchanged on the fixed store. The
report lists what still does not scale, with its mechanism read from source and the counter that proves it.

### Amendment 8 — written 2026-09-25T05:40:30Z: amendment 7's wall is the kernel's read path, not the store lock; its attribution run and its fix (F6), before either runs

**What amendment 7's runs showed** (all banked in `turso_conc/raw/`, each committed before it was read; verdicts by
`turso_conc/analyze_conc.py`, committed at `8c144a9` before any data):
- The tests and the fire-check passed (`tests_conc`, 38/38). Both accounting mutants were killed by the assertion aimed at
  each (`mutate_conc`).
- `conc_main` (`9c83fbf`) and `conc_hold` (`d36c6fe`): 5 of 40 registered checks broken, all of them in P7.5.
  - Throughput stops scaling at T=4 for N ≥ 10^5, where the verdict is WALL. At T=16, X is below X(1): 0.76–0.94×, and
    E(16) is 0.05–0.08.
  - The store lock is not the wall: W(16) ≈ 0.10 and U(16) ≈ 0.18, against the registered 0.25 and 0.5. E(2) is
    0.50–0.64, against a registered floor of 0.8.
  - What held: acquisitions per cycle 19.95 ± 0.005 at every N and T (P7.1); both detectors silent at T=1 and firing at
    T=16 (P7.2); hold per cycle 3.7–4.6 µs at 10^6 against 1.6–1.8 at 10^3 (P7.3); X(1) 27.4k–29.4k (P7.4); the
    instrument's cost within bounds (P7.7).
- System CPU per cycle rises from 4.6–5.8 µs at T=1 to 370–390 µs at T=16 (N ≥ 10^5), 73% of all CPU (getrusage, from
  the `cellsum` lines).
- `conc_prof` (`9a9390c`), `/usr/bin/sample` at T=16, excluding the main thread's `join`:
  - `pread` is at the top of 64–70% of worker samples;
  - 64% of those are inside `connect_branch`, the rest in statements' page reads;
  - parking_lot's slow path under `BranchStore` (`swtch_pri`, `__psynch_cvwait`) takes ~17%.
- **Mechanism, read from source.** Every branch connection starts with an empty page cache. It pays:
  - `_init`'s two reads of the trunk's file: the 512-byte header (`read_db_header_buf`) and page 1 (`ReadPage1`);
  - the page-1 read `connect_branch` makes through the branch;
  - one read per trunk page its statements touch.

  The WAL is empty in this arm, so each read is a `pread` of the one database file every thread shares. The count per
  cycle is ~12, INFERRED from the banked `resolves_per_op` and the code; no counter measured it in amendment 7.
- **Two walls in series:** the kernel's `pread` path, then the store lock.

**A. Attribution run `e_pread`: which kernel structure serialises the reads** (`turso_conc/pread_mb.c`, `e_pread.sh`).
- The file: 545 pages of 4 KiB in `$TMPDIR`, the volume the harness's TempDir uses, warmed before every run.
- T threads each `pread` ONE 4 KiB page M = 100,000 times. Six modes:
  - `same_shared`: every thread the same page, through one shared descriptor;
  - `diff_shared`: page 1+16i for thread i, through the shared descriptor;
  - `same_own`: the same page, each thread through its own descriptor;
  - `diff_own`: its own page, its own descriptor;
  - `same_mmap` / `diff_mmap`: a `memcpy` from one read-only `MAP_SHARED` mapping instead of a syscall.
- T in {1,2,4,8,16}, the list forward and then reversed. S(T) = reads/s(T) / reads/s(1).
- Predictions:
  - `same_shared`: S(16) ≤ 2. Every connect reads page 1, and the profile puts most of the wall there.
  - `same_mmap` and `diff_mmap`: S(16) ≥ 8.
  - None for the other three modes, which classify the serialiser:
    - per page if `same_*` collapse and `diff_*` scale;
    - per file if all four syscall modes collapse;
    - per descriptor if `*_shared` collapse and `*_own` scale.
- The fix below does not depend on the class, because it removes the syscall. The class goes into the report as the
  mechanism.

**B. F6: a shared, versioned trunk-page cache for branch reads (this commit).**
- **Store.**
  - `BranchStore` keeps the trunk's pages as branches have read them, keyed by (page, trunk epoch of its last write,
    cache generation), in a lock-free three-level radix of `ArcSwapOption`.
  - `resolve_into` answers "the trunk's current version" only when the trunk's last write to the page came at or before
    the branch's `trunk_at`. That epoch was closed by a fork, which holds the WAL write lock, so the version under the
    key is immutable. The cache serves it when it holds it; otherwise it returns the key, and the pager, once its read
    of the WAL or file has loaded the page, calls `fill_trunk_page`. The callback is a `CompletionGroup` of one.
  - The one gap: a trunk with no live child writes without a copy decision, so `written` does not move. The trunk's last
    child going therefore bumps the generation, and older entries are never served.
- **Connect.** `connect_branch` builds its pager with `Database::_init_branch`. The page format (page size and reserved
  byte) comes from the store, recorded at the first fork, instead of the header read. The pager is bound before page 1
  is read, so page 1 comes through the branch, and nothing of the trunk's is ever in the pager, so `clear_page_cache`
  is gone.
- **Prior art.** The shared buffer pool of every server DBMS (PostgreSQL `shared_buffers`, the InnoDB buffer pool,
  SQLite's shared-cache mode), with the version in the key as in a buffer tag with its LSN (the (page, LSN) keys of
  Neon's page cache [RECALLED]). No novelty is claimed.
- **Counters.** `trunk_page_hits` and `trunk_page_misses` in `BranchWork`. The harness prints them and the resolutions in
  `cellsum`; the change is additive and times nothing new.
- **Tests.**
  - `store::tests::the_trunk_page_cache_serves_each_branch_the_version_it_forked_from`: a model with trunk writes with
    and without children, the last child going, and every read checked. It asserts hits > 0, childless writes > 0, and
    that the last child went.
  - `mechanism_tests::a_trunk_write_with_no_branch_alive_is_never_served_from_the_trunk_page_cache`, through SQL: a
    second branch hits and misses nothing, and a row the trunk rewrote with no branch alive reads new.
- **Mutants** (`mutate_F6.sh`; each must fail a `branch::` test, or no F6 run proceeds):
  - F6M1: the generation never bumped;
  - F6M2: the epoch not compared on a hit;
  - F6M3: the pager never fills.

**Runs** (`turso_conc/run_F6.sh`, lockrun `turso-conc`, `taskpolicy -b` on cargo, `timeout` everywhere, raw committed
before read):
- `e_pread`;
- then `tests_F6`, `mutate_F6`, `build_F6`;
- then `F6_smoke`, `F6_main`, `F6_hold`, `F6_prof`, with amendment 7's arguments unchanged. Their before files are
  `conc_*`.

**Predictions for F6** (I = inferred):
- **Counters (integers).**
  - `trunk_page_misses` ≤ 700 in the first cell of each run (the ~545-page working set fills once), and ≤ 0.01 per cycle
    in every later cell.
  - Acquisitions per cycle equal across T within 0.5% at each N, and within ±1.0 of amendment 7's 19.95. The cache adds
    no acquisition, and a connect still resolves page 1 once.
- **System CPU per cycle.** ≤ 1.5 µs at T=1 and ≤ 30 µs at T=16 at every N. Amendment 7: 4.6–5.8 and 118–389.
- **Throughput.**
  - X(1) ≥ 1.15× `conc_main`'s at every N (~12 syscalls of ~0.5 µs each removed from a ~35 µs cycle, I).
  - X(16) ≥ 2× `conc_main`'s at N=10^6 and ≥ 3× at N=10^3.
- **The next wall.** At N=10^6 the store lock becomes the wall: E(16) < 0.5 in both draws, with W(16) ≥ 0.25 (`F6_main`)
  and U(16) ≥ 0.6 (`F6_hold`). The hold per cycle, 3.7–4.6 µs at 10^6, caps X near 1/hold ≈ 220–270k cycles/s. At
  N=10^3 no verdict is predicted.
- **Profile.** In `F6_prof`, `pread` is < 5% of worker samples at both N, and parking_lot's slow path under
  `BranchStore` is the largest remaining wait.
- **Correctness.** No NOT A RESULT; after every cell, arena = live = N; every reap frees exactly 1 page.

**Falsifiers.**
- System CPU per cycle at T=16 ≥ 100 µs with misses ≈ 0: the kernel time was not the reads', and the attribution to
  `pread` is withdrawn.
- X(16) not above `conc_main`'s: the reads were not what bound throughput.
- Misses per cycle not ≈ 0: the cache is not doing what it claims, and no F6 number is read as its effect.

**What follows.** If the store lock is then the wall, amendment 9 pre-registers F5 on top of F6, before its build. F5
is the store striped into 64 shards, each owning its branches and an arena domain, with the trunk behind its own lock
and read without it through a lock-free epoch radix. Its draft is `turso_conc/fix_draft/`.

**Erratum to amendment 8 (appended 2026-09-25T05:48:49Z; doc only, before any F6 build or run).** Two profile shares in "What
amendment 7's runs showed" were computed against the wrong denominator: the total of `sample`'s "top of stack (when >= 5)"
list, which leaves out part of the samples. Against the authoritative denominator (16 worker threads × each thread's
root count, from the `Call graph` section: 16 × 2,108 at 10^6, 21,344 samples at 10^3):
- `pread` is at the top of **61.0%** of worker samples at N=10^6 and **71.5%** at 10^3, not "64–70%";
- parking_lot's slow path under `BranchStore` takes **12.8%** at 10^6 and **5.8%** at 10^3, not "~17%".

The share of `pread` samples inside `connect_branch` is unchanged: 64.1% and 64.5%. No prediction or run of amendment 8
depends on these two figures.

### Amendment 8a — written 2026-09-25T06:13:06Z: a fresh-context review of amendment 8, its corrections, and three runs added before any F6 build or run

A read-only reviewer that had not seen the work attacked amendment 8. The lane verified each point below from the raw
files before writing it here.

**Corrections to amendment 8's summary of amendment 7.**
- System CPU per cycle at T=16 and N ≥ 10^5 is **307–389 µs, 67–74%** of all CPU. The earlier "370–390 µs, 73%" left
  out N=10^5 draw 1.
- **When the wall is in the kernel, and when it is not.** Per-cycle thread-time growth over T=1, from `conc_main`'s
  `cellsum` lines:
  - At T=2 and T=4, where the first WALL verdicts are, 71–78% of the growth is **user** CPU. At T=8 it is 57–63%.
  - The kernel dominates only at T=16 with N ≥ 10^5: sys is 72–78% of the growth.
  - At N=10^3, T=16 stays user-dominated in `conc_main` (61–64%).
  - The only profile was taken at T=16, so the user-mode growth at T=2–8 is **unattributed**.
- **Regimes differ between runs.** The N=10^3, T=16 cell ran with sys at 118–136 µs/cycle in `conc_main`, 297–301 in
  `conc_hold`, and 390–401 in `conc_prof`. The 10^3 profile therefore describes `conc_prof`'s regime, not
  `conc_main`'s.
- **Where the store lock is ruled out.** W(16) ≈ 0.10 holds only at N=10^6 in `conc_main`; it is 0.20–0.22 at N=10^3.
  By amendment 7's rule, "outside the store lock" is established at T=4 at every N and in 2 of the 6 T=16 cells. The T=8
  cells and 4 of the T=16 cells are "attribution open".
- **Profile shares.** `pread` is 60.4–61.0% of worker samples at 10^6 and 70.5–71.5% at 10^3. The range spans the two
  ways of counting: `sample`'s own top-of-stack total, and the sum over call-graph leaves (224 more at 10^6). Of those
  samples, 64–65% are inside `connect_branch`. This supersedes the erratum's single figures.
- **Not every sys cycle is `pread`.** `swtch_pri` (parking_lot yielding: a trap, counted as sys) is 8.7% of worker
  samples at 10^6.
- **A missed expectation.** P7.5's parenthetical "Sn(16) ≈ 12–14" missed in all 12 T=16 cells (11.4–15.7). It was not
  one of the 40 registered checks.

**Corrections to amendment 8's design and predictions.**
- **e_pread cannot tell per-file from per-process or system-wide serialisation.** All its syscall modes use one file in
  one process. The class "per file" is withdrawn until `e_pread2` below. Its low-T comparisons (S(2), and whether
  same-page is worse at T ≤ 2) rest on one run of 28–43 ms per point, with T=1 varying up to 35% between passes, and are
  not read. Only the collapse at T ≥ 8 is.
- **F6 adds hold time.** On a hit, the 4 KiB copy runs under the store's one lock. F6's hold per cycle at T=1 is
  predicted at amendment 7's value plus 1–3 µs (I). The "cap near 1/hold" in amendment 8 is lowered accordingly. No
  registered bound changes.
- **F6 keeps at most one version per trunk page ever read, stale generations included**, until replaced. Memory stays
  at or below the trunk's size.
- **The attribution criterion for F6 is the profile, not total sys CPU.** A store lock under contention yields through
  `swtch_pri`, which is sys time. So amendment 8's falsifier "sys ≥ 100 µs/cycle at T=16" is replaced for the
  attribution by: **`pread` ≥ 10% of worker samples in `F6_prof`** withdraws the pread attribution. The registered
  sys predictions stand and are reported as registered. The pair "W(16) ≥ 0.25 and sys ≤ 30 µs" may break on
  `swtch_pri` alone; if it does, the report says which half.

**Added before any F6 run.**
- **Test.** `mechanism_tests::branches_read_their_fork_while_the_trunk_writes_concurrently`, with real threads:
  - one thread rewrites rows and forks 300 branches in a recorded order;
  - four readers check every branch's rows and full table, on two connections, while the trunk keeps writing;
  - the trunk also passes through moments with no live child.

  It must pass in `tests_F6`, and it is expected to kill F6M1 and F6M2 as well.
- **Run `e_pread2`** (`pread_mb.c` gains two modes; `e_pread2.sh`): `same_shared` as a baseline, `same_ownfile`, and
  `same_procs`, at T in {1,2,4,8,16}, forward and reversed, M = 100,000.
  - `same_ownfile`: each thread reads page 0 of its own copy of the file.
  - `same_procs`: T processes, one thread each, read page 0 of the one file.
  - Read only for S(16) ≥ 8 (scales) against S(16) ≤ 2 (collapses):
    - `same_ownfile` scales and `same_procs` collapses: **per file**, system-wide.
    - both collapse: **system-wide beyond one file**.
    - `same_procs` scales and `same_ownfile` collapses: **per process**.
  - No prediction.
- **Runs `conc_prof4` and `F6_prof4`**, to attribute the T=2–4 wall:
  - arguments `--arm conc --threads 4 --checkpoints 1000000 --cycles 100000`;
  - `/usr/bin/sample <pid> 3` taken 1 s after `# cell N=1000000 T=4 draw=0 start`;
  - `conc_prof4` on the amendment-7 binary (sha256 `52995fc3…`, built from `323dbfd79`), `F6_prof4` on F6's.
  - Only the profiles are read. No prediction: the user-mode growth has no mechanism named yet.

### Amendment 9 — written 2026-09-25T06:18:25Z: F5, the striped store, pre-registered SOURCE-ONLY and UNBUILT; its runs follow F6's

**Status.** Quiet mode (Ryan, 05:27Z) forbids local compute. On the lead's instruction this commit is source-only:
- F5 is on branch `inv-conc-F5-noindex`, a child of `4ae0841ee` (F6 plus amendment 8a);
- it has never been compiled, and no test of it has run;
- the F6 chain runs first, on branch `inv-conc-noindex`.

A compile fix is an amendment only if it changes what is timed or asserted.

**Why F5.**
- Amendment 7 measured the store lock's hold at T=1: 1.6–1.8 µs per cycle at 10^3 and 3.7–4.6 at 10^6. At T=16 it was
  held 11–18% of the time, while throughput was capped by the kernel's read path.
- F6 removes that read path (amendment 8). F6 also copies each cache hit under the same lock (amendment 8a).
- Throughput should then rise until the store lock saturates, near 1/hold ≈ 150–270k cycles/s at 10^6 (I). Amendment
  8 predicts the store lock as F6's wall at 10^6.

**Design** (`core/branch/store.rs` on this branch).
- **Shards.** 64 shards, `CachePadded<Mutex<Shard>>`. Branch `id` lives in shard `id % 64`, with its state and an arena
  domain from which its own pages are allocated. A slot's top 7 bits name its domain, and a release into the wrong
  domain is refused.
- **Trunk.** The trunk's lineage and retained versions sit behind their own lock.
- **Lock-free reads.** `written` (the trunk epoch of each page's last write) and F6's trunk-page cache are lock-free
  radixes: three levels, installed once, never unlinked. A branch reading a page the trunk has not rewritten since its
  fork takes only its own shard's lock; the cache hit and its counter are taken under that lock.
- **Two locks, never at once.** A fork takes the parent's (or the trunk's) lock, then the child's shard lock. A reap
  takes the child's shard lock, then the parent's lock. The windows between are argued in the code
  (`fork_trunk`, `collect`).
- **Generation bump.** F6's bump on the trunk's last child is made under the trunk lock.
- **Counters.** `lock_*` is summed over all locks; `trunk_lock_*` is the trunk's lock alone; `stats` takes each lock
  once.

**Prior art, no novelty claimed.**
- Lock striping: the segments of Java's `ConcurrentHashMap` (JSR 166) [RECALLED].
- The read side of RCU for a structure that is only ever installed, never unlinked (McKenney) [RECALLED].
- A per-stripe allocator domain, like per-CPU slab magazines (Bonwick 1994/2001) and per-thread arenas
  (jemalloc, tcmalloc) [RECALLED].

**Lock acquisitions per `conc` cycle, READ FROM THE CODE on this branch.**
- The trunk lock is taken exactly twice per cycle: `fork_trunk` (the epoch, the children insert) and the reap's
  `collect` (`child_gone` on the trunk). No other op in this arm takes it. The trunk never writes here, so no copy
  decision runs, and resolutions of trunk pages stay on the lock-free path.
- Every other store call takes exactly its branch's shard lock:
  - `open` and `close`;
  - `begin_write`, `end_write` and `holds_writer`;
  - `first_write_branch` and `commit_pages`;
  - every resolution: own pages, and trunk pages through the cache.

  Trunk children have no `inherited` pages, so no cross-shard copy occurs in this arm.
- So F5's `lock_acquisitions` per cycle = F6's + 2 (fork and reap each take one lock more), and `trunk_lock` = 2.

**Which ops scale, and which residuals will not.**
- **Scale by construction:** everything that touches one branch. In this arm that is every resolution, open, close,
  write and commit. Threads share shards (64 stripes, ~20 acquisitions per cycle per thread) but never a branch.
- **Serialised by construction in F5:**
  - (i) Every trunk fork. It holds the trunk's WAL write lock in Turso (`Connection::fork_trunk`, where a concurrent
    forker gets Busy and the harness retries after `yield_now`), and it takes the trunk lock.
  - (ii) Every reap of a trunk child. It takes the trunk lock for `child_gone`: a removal and two neighbour probes on a
    `BTreeMap` of N fork epochs, plus two index range queries.
- **Residuals read from Turso code that neither F5 nor F6 touches.** These are candidates for amendment 8a's
  unattributed user-CPU growth at T=2–8, unverified until `conc_prof4`/`F6_prof4`:
  - the WAL read path of every branch statement and connect (`WalFile::begin_read_tx`/`end_read_tx`). It takes a
    parking_lot read of `RwLock<WalFileShared>`, clones and read-locks it again for `VacuumLockGuard`, and acquires
    `read_locks[0]` shared. These are atomic read-modify-writes on a handful of cache lines every thread shares;
  - shared reference counts: the trunk's `Arc<Schema>` is cloned by every branch open and released by every connection
    drop, all trunk children sharing one; `Arc<Database>` likewise;
  - the buffer pool's atomic slot bitmap (`buffer_pool.get_page` on every read);
  - `Database::schema`'s mutex in every trunk fork (`clone_schema`).

**Predictions** (I; judged against `F6_main`/`F6_hold`, amendment 7's arguments unchanged):
- **P9.1 (integers).** `trunk_lock_acquisitions` per cycle = 2.000 ± 0.001 at every N, T and draw; the `stats` calls
  add ≤ 2/(T·C). `lock_acquisitions` per cycle = `F6_main`'s in the same cell + 2.00 ± 0.02.
- **P9.2.** The shard contended fraction, (lock_contended − trunk_lock_contended) / (lock_acquisitions −
  trunk_lock_acquisitions), is 0 at T=1 and ≤ 0.02 in every cell.
- **P9.3.** W (all locks) ≤ 0.05 at T=16 at every N. The trunk lock's U_trunk = trunk_lock_hold_ns / wall ≤ 0.3 at
  every N (`F5_hold`). Its hold is a fork's insert plus a reap's `child_gone` on an N-entry map: 0.3–1.5 µs per cycle at
  10^6.
- **P9.4.** X(1) within ±10% of `F6_main`'s at every N.
- **P9.5.**
  - If `F6_main` shows the store lock as the wall at 10^6 (E < 0.5, W ≥ 0.25, U ≥ 0.6): `F5_main`'s X(16) there is ≥ 1.5×
    `F6_main`'s.
  - If it does not: `F5_main`'s X(16) is within ±20% of `F6_main`'s at every N, a wall removed that did not bind.
- **P9.6 (the residual).** E(16) < 0.7 at N=10^6 in both draws: F5+F6 does not reach "scales" at T=16. In `F5_prof`, frames
  under `BranchStore` are < 10% of worker samples, and the largest waits lie outside the branch store, among the four
  residuals above.
- **P9.7.** `busy_fork` per cycle at T=16 ≥ `F6_main`'s. Fork's p99 at T=16 ≥ 5× its p99 at T=1 at every N (forks
  serialise on the WAL write lock while every other op gets cheaper).
- **Correctness.** No NOT A RESULT; after every cell, arena = live = N; every reap frees exactly 1 page.

**Tests in this commit (unrun).**
- `store::tests::striped_store_under_threads_reads_every_fork_as_it_was`:
  - four workers grow trees (trunk forks, forks of their own branches, writes, reaps) and resolve every page after every
    step;
  - a fifth thread rewrites trunk pages, under the WAL-lock and snapshot discipline the pager imposes;
  - a miss is filled into the cache under the reader's snapshot;
  - it asserts that cross-shard forks, deferred reaps, trunk writes and trunk-locked resolutions all occurred.
- `lock_accounting_counts_a_forced_wait_and_nothing_else`: 65 acquisitions per `stats`, and the contended hold counted
  against the trunk's class.
- Every model, cache and mechanism test of F1–F6.

**Mutants** (`turso_conc/mutate_F5.sh`; anchors checked unique against this commit's `store.rs`):
- F5M1: the trunk never consulted;
- F5M2: a trunk child never detached;
- F5M3: a child filed in its parent's shard;
- F5M4: a foreign slot read from the reader's own shard;
- F5M5: the trunk's write epoch never published;
- F6M1 and F6M2 again, on the cache code as it moved.

Each must fail a `branch::` test, or no F5 run proceeds.

**Runs, QUEUED-FOR-FANS, after F6's chain.** The worktree is switched to this branch. `tests_F5`, `mutate_F5`,
`build_F5`, `F5_smoke`, `F5_main`, `F5_hold`, `F5_prof`, `F5_prof4`, with amendment 7's arguments unchanged. Before files:
`F6_*`.

### Amendment 8b — written 2026-09-25T13:38:39Z: F6's first test run hung; the cause is in Turso's CompletionGroup; the fix, before any timed F6 run

**What happened.** `tests_F6` (`turso_conc/raw/tests_F6.txt`, banked `0c5f2bd`, rc 101) passed 40 of 41 tests. It hung in
`mechanism_tests::indexes_and_overflow_pages_branch_like_table_leaves` at the branch UPDATE on line 696. I stopped it by
pid after 10 minutes, and the chain refused to continue. A `/usr/bin/sample` of the hung thread shows the VDBE
re-polling a pending IO completion that never finishes (`Program::normal_step` → `IOCompletions::finished`). No timed
F6 run had started.

**Cause, read from source.**
- F6's `read_page_no_cache` returns a `CompletionGroup` of one (the read, plus the cache fill).
- `CompletionGroup::build`, when every child has already finished (a synchronous `pread`), calls the group's callback
  directly. It never runs the group Completion's own `callback`, so the group's parent slot is never claimed.
- Btree balance collects sibling page loads, the completions `read_page` returns, into its own `CompletionGroup`
  (`btree.rs`, `pending_sibling_load_completions`). Linking the finished inner group succeeds because the slot is
  open, and the outer group then waits for a notification that was skipped.
- This is a latent Turso bug: any group that finishes during `build` hangs an outer group it is nested in. F6 exposed it
  by returning a group where callers had only ever seen plain read completions.

**Fix (this commit).** `CompletionGroup::build` finishes a group that completes during `build` through
`Completion::callback`, on both the success and the error path. That runs the group's callback once, records the result
and claims the parent slot. Test: `io::completions::tests::a_group_finished_during_build_counts_as_finished_in_an_outer_group`
nests a finished group (and a failed one) in an outer group, and asserts that the outer one is finished (and failed).

**Added runs, before any timed F6 run.**
- `tests_F6` now runs `branch::` and `io::completions` together.
- `tests_F6_core`: the whole `turso_core` lib suite. This change touches core IO, not only the branch module.
- `mutate_F6` gains F6M4, which reverts the fix and is tested with the `io::completions` filter; the new test must fail.

Nothing timed or asserted in amendments 7, 8 or 8a changes. The same fix goes onto F5's branch before F5 builds.
