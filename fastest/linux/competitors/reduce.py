#!/usr/bin/env python3
"""reduce.py RUNDIR -- one table over every job of a fastest-competitors run, from its downloaded artifacts
(`gh run download <id> -D RUNDIR`: one directory per artifact, competitors-<system>-<runner>-<fs>/).

Prints, tab-separated:
  JOBS   artifact, firecheck verdict, functional verdict, failed functional lines
  CELLS  artifact, cell, ops, ops_ok, flushes/op (idle subtracted), raw/op, exact, idle flushes, fsync/op,
         fdatasync/op, sync_file_range/op, copy_file_range/op, ficlone/op, deferred/op, cell verdict
A job with no cells, no firecheck record or no functional record is listed as MISSING, never skipped.
"""
import glob
import json
import os
import sys


def last_line(path, prefix):
    if not os.path.exists(path):
        return "MISSING"
    lines = [l.rstrip("\n") for l in open(path) if l.startswith(prefix)]
    return lines[-1] if lines else "MISSING"


def main(d):
    arts = sorted(p for p in glob.glob(os.path.join(d, "competitors-*")) if os.path.isdir(p))
    if not arts:
        sys.exit(f"reduce: no competitors-* artifacts under {d}")
    print("JOBS\tartifact\tfirecheck\tfunctional\tfailed_checks")
    cells = []
    for a in arts:
        name = os.path.basename(a)
        fc = last_line(os.path.join(a, "firecheck", "firecheck.txt"), "VERDICT")
        fn = last_line(os.path.join(a, "run", "functional.txt"), "VERDICT")
        fails = []
        fpath = os.path.join(a, "run", "functional.txt")
        if os.path.exists(fpath):
            fails = [l.strip() for l in open(fpath) if l.startswith("FAIL")]
        print(f"JOBS\t{name}\t{fc}\t{fn}\t{' || '.join(fails)}")
        found = sorted(glob.glob(os.path.join(a, "run", "cells", "*", "cell.json")))
        if not found:
            cells.append((name, "MISSING", None))
        for c in found:
            try:
                cj = json.load(open(c))
            except ValueError as e:
                cj = {"ops": "?", "ops_ok": "?", "verdict": f"UNREADABLE cell.json: {e}"}
            cells.append((name, os.path.basename(os.path.dirname(c)), cj))
    print("CELLS\tartifact\tcell\tops\tops_ok\tflushes/op\traw/op\texact\tidle_flushes\tfsync/op\tfdatasync/op"
          "\tsync_file_range/op\tcopy_file_range/op\tficlone/op\tdeferred/op\tverdict")
    for name, cell, c in cells:
        if c is None:
            print(f"CELLS\t{name}\t{cell}" + "\t" * 13 + "MISSING")
            continue
        p = c.get("per_op", {})
        row = [c["ops"], c["ops_ok"], p.get("flushes"), p.get("flushes_raw"), c.get("exact"),
               c.get("idle", {}).get("flushes"), p.get("fsync"), p.get("fdatasync"), p.get("sync_file_range"),
               p.get("copy_file_range_calls"), p.get("ficlone"), c.get("deferred", {}).get("per_op", ""),
               c["verdict"]]
        print("CELLS\t" + "\t".join([name, cell] + [str(x) for x in row]))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
