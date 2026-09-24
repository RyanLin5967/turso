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
