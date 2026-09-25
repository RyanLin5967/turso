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
