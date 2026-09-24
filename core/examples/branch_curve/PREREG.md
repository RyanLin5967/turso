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
