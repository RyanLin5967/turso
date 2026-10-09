#!/usr/bin/env python3
"""T3 cell manifest: validate it, list its filesystems, and plan one filesystem's runs.

  cells.py fslist MANIFEST          the filesystems, in manifest order, one per line
  cells.py plan MANIFEST FS SEED    one row per run: cell-rK, system, clients, ops, runs, class, K, age, live
                                    (every run of every cell on FS, in a seeded shuffle of blocks:
                                    block k holds run k of every cell, amendment 34's interleaving; K is
                                    the block, so the runner can take V3L at every block boundary)

Manifest: TSV with a header row `cell system fs clients ops runs class age live`, '#' comments allowed.
system: ours | pg18-d2 | dolt | doltgres | b1 (pg18-defaults is dropped on Linux: the lead's ruling, artie 6b0bef481b;
it was the same server as pg18-d2). class: the engine's sync class for ours (full | fsync | off | async), '-' for
competitors. ops: the run's TOTAL ops, every system (gate-6 review 12; per-system n_run, PREREG). age: the committed
single-row UPDATEs aging the parent (gen_seed.py's; PREREG section 7 ages with 1e5); live: the live branches held
before the cells (the PREREG's N), one private write each. Every system builds the same parent from them (gate-6
t3run HIGH 4). Any other value refuses (exit 2), so a typo can never shrink the grid silently.
"""
import random
import sys

SYSTEMS = {"ours", "pg18-d2", "dolt", "doltgres", "b1"}
HEADER = ["cell", "system", "fs", "clients", "ops", "runs", "class", "age", "live"]
CLASSES = {"full", "fsync", "off", "async"}
FS = {"ext4", "xfs", "btrfs"}


def load(path):
    rows, header = [], None
    for n, line in enumerate(open(path), 1):
        line = line.rstrip("\n")
        if not line.strip() or line.startswith("#"):
            continue
        f = line.split("\t")
        if header is None:
            header = f
            if header != HEADER:
                sys.exit(f"cells: line {n}: header must be {' '.join(HEADER)}, got {f}")
            continue
        if len(f) != len(HEADER):
            sys.exit(f"cells: line {n}: {len(f)} fields")
        r = dict(zip(header, f))
        bad = []
        if r["system"] not in SYSTEMS:
            bad.append("system")
        if r["fs"] not in FS:
            bad.append("fs")
        if (r["system"] == "ours") != (r["class"] in CLASSES) or (r["system"] != "ours" and r["class"] != "-"):
            bad.append("class")
        for k in ("clients", "ops", "runs"):
            if not r[k].isdigit() or int(r[k]) < 1:
                bad.append(k)
        for k in ("age", "live"):
            if not r[k].isdigit():
                bad.append(k)
        if bad:
            sys.exit(f"cells: line {n}: bad {bad}: {line}")
        rows.append(r)
    if not rows:
        sys.exit("cells: the manifest has no cells")
    ids = [(r["cell"], r["fs"]) for r in rows]
    if len(set(ids)) != len(ids):
        sys.exit("cells: a cell id repeats on one filesystem")
    return rows


def plan(path, fs, seed):
    """One filesystem's runs: rows of [cell-rK, system, clients, ops, runs, class, K]."""
    rows = [r for r in load(path) if r["fs"] == fs]
    rng = random.Random(f"{seed}:{fs}")
    blocks = max(int(r["runs"]) for r in rows) if rows else 0
    out = []
    for k in range(1, blocks + 1):
        block = [r for r in rows if int(r["runs"]) >= k]
        rng.shuffle(block)
        for r in block:
            out.append([f"{r['cell']}-r{k}", r["system"], r["clients"], r["ops"], r["runs"], r["class"], str(k),
                        r["age"], r["live"]])
    return out


def main(a):
    if len(a) == 3 and a[1] == "fslist":
        seen = []
        for r in load(a[2]):
            if r["fs"] not in seen:
                seen.append(r["fs"])
        print(" ".join(seen))
        return 0
    if len(a) == 5 and a[1] == "plan":
        for row in plan(a[2], a[3], a[4]):
            print("\t".join(row))
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
