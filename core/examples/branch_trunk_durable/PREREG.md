# PRE-REGISTRATION D1 — `branch_trunk_durable`: what a pre-image-keeping trunk commit costs, per durability

Written and committed before the harness's first build or run, by lane `turso_sota` (artie-research
`frontier/round10/turso_sota/`). The harness is `main.rs` next to this file, on the durable store at
`ec168128b` (branch `ferrobranch-arena`, owned by the turso-branching lane; this lane only ADDS this example on
its own branch `inv-sota-durable-noindex` and touches nothing of that lane's). Amendments are append-only below
the line.

## Why

F3 (retain a trunk pre-image by reference to its WAL frame until the checkpoint, Retro/Thresher) was not built in
the volatile arena, because there the capture costs no I/O (turso_sota PREREG amendment 3a). The case for F3 is the
durable store, where `first_write_trunk` writes the pre-image to the arena file and buffers a `TrunkRetain`
record, and `Pager::commit_wal` runs `durability_barrier` before any WAL frame: `journal.flush` → `arena.sync()`,
the record's `write_at`, `fsync_file(log)` (`journal.rs:775-799` at `ec168128b`, R). The round-10 candidate
`cand_turso-trunk-fastpath-loss.md` predicted, from source only: a retaining trunk commit pays arena fsync + log
fsync + WAL fsync = 3 under trunk synchronous=FULL against 1 without retention, and 2 against 0 under NORMAL. This
run measures it.

## Runs (lockrun, timeout 3600, raw to artie-research `frontier/round10/turso_sota/raw/`, committed before read)

`--checkpoints 100,1000,10000,30000 --samples 200`, and:

| run | arguments |
|---|---|
| dur_vol_full | `--durability volatile --synchronous full` |
| dur_nosync_full | `--durability durable-nosync --synchronous full` |
| dur_sync_full | `--durability durable --synchronous full` |
| dur_sync_normal | `--durability durable --synchronous normal` |

## Predictions

Counters (from source, R):
- `log_B_per_op`: `trunk_retain` = 37.00 in both durable arms at every x (one `TrunkRetain` frame: 8-byte frame
  header + 29-byte payload; no lease, so no `Clock` stamp), except that a sample whose op ran a compaction reads the
  log's truncation as 0 growth (the per-op mean is then 37·(1 − c/K) or more, c = compactions in that op);
  `trunk_plain` = 0.00 (the fast path buffers nothing, the barrier returns at `!unsynced`). Volatile: 0 and 0.
- `arena_B_per_op`: `trunk_retain` ≤ 4096 (a fresh slot past the file end grows it by one page, a reused slot does
  not); `trunk_plain` = 0.
- The engine asserts arena == 2N before and after sampling, and every sample reap frees exactly 1 page.

Time (I; T_f = one plain `fsync(2)` on this box, not measured beforehand):
- volatile/full: `trunk_retain` − `trunk_plain` ≤ 5 µs (slot copy + index inserts), both ≈ one WAL fsync.
- durable-nosync/full: `trunk_retain` − `trunk_plain` ≤ 50 µs (arena pwrite + record write, no fsync).
- durable/full: `trunk_retain` / `trunk_plain` in [1.5, 4] (3 fsyncs against 1).
- durable/normal: `trunk_retain` / `trunk_plain` ≥ 3 (2 fsyncs against none).
- `trunk_retain` p50 |slope| < 0.10 over x = 100..30000 in every arm: the barrier's work is the records it flushes,
  not N.
- Compactions: in the durable arms, compactions occur during growth (`maybe_compact` runs at forks and barriers
  once the log exceeds max(1 MiB, 2 × snapshot)); the op that runs one pays a whole-state snapshot write, so the
  compaction latencies grow with N: log-log slope of compaction µs against n ≥ 0.5 across the compactions listed
  (the round-10 audit's item 6, `inline-compaction-stall`). No compaction in the volatile arm.

A number outside its interval is reported as a break, not refitted. A missing compaction list (none observed) is
reported as "not exercised", never as the absence of the stall.

---
## Amendments (append-only)

### Amendment D2 — written 2026-09-25T14:06Z: time the branch's own first write too, for the compaction stall; before any D2 run

D1's four runs (artie-research `e5cac8e`, `4751b03`, `6007c4d`, `eeba8b7`) read "compactions: 0" in the durable arms,
yet the branch snapshot appeared (0 → 1,106,635 bytes between x = 10^4 and 3·10^4). `maybe_compact` also runs at the
end of `commit_pages` (`store.rs:1322` at `ec168128b`), i.e. inside a branch's own first write, which D1 did not time.
So D1's compaction checks read the instrument's blind spot, not the engine; they stand as broken.

**Change** (this commit): the growth loop also times each branch's own first write (`grow_write`: the UPDATE on the
branch's connection, the connect excluded) with the same snapshot-change detection. Nothing else changes.

**Runs** (lockrun, timeout 3600, raw committed before read): `--checkpoints 100,1000,10000,100000,300000 --samples 200`
with `--durability durable --synchronous full` (`dur2_sync_full`) and `--durability durable-nosync --synchronous full`
(`dur2_nosync_full`).

**Predictions (I).**
- At least 3 compactions per arm by x = 3·10^5, each listed with its op and n.
- A compaction writes the whole live state, so its latency grows with n: log-log slope of compaction µs against n
  ≥ 0.5 over the compactions listed, per arm.
- The snapshot at the last checkpoint holds 20–120 bytes per live branch (D1: 1,106,635 bytes at a compaction
  somewhere in 10^4..3·10^4, i.e. 37–111 bytes per branch there).
- The largest compaction stall at n ≥ 10^5 is in [2, 500] ms in the sync arm, and the op that pays it holds the store
  mutex throughout (R: `commit_pages`, `fork_trunk` and `durability_barrier` call `maybe_compact` under `inner`).
- Everything D1 checked (37 log bytes and 4096 arena bytes per `trunk_retain`, 0 per `trunk_plain`, the ratios) holds
  again at every x.
