#!/usr/bin/env python3
"""smoke_table.py RUNDIR [C] -- the SMOKE.md tables of a banked fastest-competitors run (lane fastest-linux-comp):
flushes per create and per create+first-write, per system, variant, filesystem and arch, at C clients (default 1),
read from each cell's cell.json (stracecount.py cell). Markdown on stdout.

Each table cell is "<create> / <create+first-write>" in flushes per op, idle control subtracted (stracecount's
per_op.flushes), with
  *  the cell is not background_free (the idle control or a background/unmapped process flushed; see the role table)
  ~  a one-process server: background work inside the process cannot be separated by process
  !  the idle-subtracted estimate went below zero (read the raw count)
  #  some creates in the window waited on the template (CountOtherDBBackends; see the cell's template_waits)
  FAIL  the cell's verdict was not ok;  MISSING  no cell.json (or no such job)
Then the PG deferred table (the CHECKPOINT after each cell: flushes the creates left for later, per op) and the
per-role split of the load windows (raw flushes per op by role, no subtraction).
"""
import json
import os
import sys

COLS = [("ubuntu-24.04", "xfs", "x86_64 XFS"), ("ubuntu-24.04", "btrfs", "x86_64 btrfs"),
        ("ubuntu-24.04-arm", "xfs", "arm64 XFS"), ("ubuntu-24.04-arm", "btrfs", "arm64 btrfs")]
import glob
import re

# The Dolt/Doltgres labels name the version the RUN used (each job's run/version.txt), never a constant: the
# registered versions moved from 2.3.5/1.3.3 to 2.4.1/1.4.0 (gate-6 review, t3run item 13) and old runs keep theirs.
# Placeholders until main() reads the run dir.
DOLT, DOLTGRES = "Dolt ? sql-server", "Doltgres ?"


def run_version(rundir, system):
    for p in sorted(glob.glob(os.path.join(rundir, f"competitors-{system}-*", "run", "version.txt"))):
        m = re.search(r"(\d+\.\d+\.\d+)", open(p, errors="replace").read())
        if m:
            return m.group(1)
    return "?"
# (system label, variant label, job system, create spec, create+first-write spec or None)
ROWS = [
    ("PostgreSQL 18", "FILE_COPY, file_copy_method=clone, D2 (M1c-create / M1)", "pg18-d2", "pg18-create", "pg18-m1"),
    ("PostgreSQL 18", "FILE_COPY clone, D2, M1c-connect (+ connect + SELECT 1)", "pg18-d2", "pg18-m1c", None),
    ("PostgreSQL 18", "WAL_LOG, D2 (M1c-create / M1)", "pg18-d2", "pg18-create-wal", "pg18-m1-wal"),
    ("PostgreSQL 18", "WAL_LOG, D2, M1c-connect", "pg18-d2", "pg18-m1c-wal", None),
    # pg18-defaults is dropped on Linux (lead ruling, artie 6b0bef481b); its copy control runs on pg18-d2
    ("PostgreSQL 18", "FILE_COPY, file_copy_method=copy (clone-proof control)", "pg18-d2", "pg18-create-copy", None),
    ("PostgreSQL 18", "SELECT 1 floor", "pg18-d2", "pg18-select1", None),
    (DOLT, "(a) DOLT_CHECKOUT(main) + DOLT_CHECKOUT('-b', b)", "dolt", "dolt-a-m1c", "dolt-a-m1"),
    (DOLT, "(b) DOLT_BRANCH(b, main) + DOLT_CHECKOUT(b)", "dolt", "dolt-b-m1c", "dolt-b-m1"),
    (DOLT, "(c) DOLT_BRANCH(b, main) + connect bench/b + SELECT 1", "dolt", "dolt-c-m1c",
     "dolt-c-m1"),
    (DOLT, "DOLT_BRANCH(b, main) alone", "dolt", "dolt-create", None),
    (DOLT, "SELECT 1 floor", "dolt", "dolt-select1", None),
    (DOLTGRES, "(a) dolt_checkout(main) + dolt_checkout('-b', b)", "doltgres", "doltgres-a-m1c",
     "doltgres-a-m1"),
    (DOLTGRES, "(b) dolt_branch(b, main) + dolt_checkout(b)", "doltgres", "doltgres-b-m1c", "doltgres-b-m1"),
    (DOLTGRES, "(c) dolt_branch(b, main) + connect postgres/b + SELECT 1", "doltgres", "doltgres-c-m1c",
     "doltgres-c-m1"),
    (DOLTGRES, "dolt_branch(b, main) alone", "doltgres", "doltgres-create", None),
    (DOLTGRES, "SELECT 1 floor", "doltgres", "doltgres-select1", None),
    ("B1 (FICLONE of SQLite 3.53.4)", "D2: fsync(clone) + fsync(dir); write synchronous=FULL", "b1", "b1-m1c-d2",
     "b1-m1-d2"),
    ("B1 (FICLONE of SQLite 3.53.4)", "D0: no flush (reported as D0 only)", "b1", "b1-m1c-d0", "b1-m1-d0"),
]


def load(rundir, system, runner, fs, spec, c):
    p = os.path.join(rundir, f"competitors-{system}-{runner}-{fs}", "run", "cells", f"{spec}-c{c}", "cell.json")
    try:
        return json.load(open(p))
    except (OSError, ValueError):
        return None


def short(role):
    """stracecount's role names, shortened for a table cell."""
    if role.startswith("other (born in the window"):
        return "other"
    if role.startswith("backend (not the load generator"):
        return "other-backend"
    return role[len("aux:"):] if role.startswith("aux:") else role


def fmt(cj):
    if cj is None:
        return "MISSING"
    if cj.get("verdict") != "ok":
        return "FAIL"
    v = cj.get("per_op", {}).get("flushes")
    if v is None:
        return "?"
    s = f"{v:.2f}" if abs(v) >= 0.005 else "0.00"
    notes = " ".join(cj.get("notes", []))
    if cj.get("background_free") is False:
        s += "*"
    if "one process" in notes:
        s += "~"
    if "below zero" in notes:
        s += "!"
    if any(cj.get("template_waits", {}).values()):
        s += "#"
    return s


def main(rundir, c):
    print(f"#### Flushes per op at C={c}: create / create+first-write (idle control subtracted)\n")
    print("| System | Variant | " + " | ".join(h for _, _, h in COLS) + " |")
    print("|---|---|" + "---|" * len(COLS))
    label = {DOLT: f"Dolt {run_version(rundir, 'dolt')} sql-server",
             DOLTGRES: f"Doltgres {run_version(rundir, 'doltgres')}"}
    for sysl, var, system, cspec, wspec in ROWS:
        sysl = label.get(sysl, sysl)
        cells = []
        for runner, fs, _ in COLS:
            a = fmt(load(rundir, system, runner, fs, cspec, c))
            b = fmt(load(rundir, system, runner, fs, wspec, c)) if wspec else "—"
            cells.append(f"{a} / {b}")
        print(f"| {sysl} | {var} | " + " | ".join(cells) + " |")
    print(f"\n#### PostgreSQL 18 deferred flushes per op at C={c} (one CHECKPOINT after each cell, divided by its ops)\n")
    print("| Job | Spec | " + " | ".join(h for _, _, h in COLS) + " |")
    print("|---|---|" + "---|" * len(COLS))
    for system in ("pg18-d2",):
        for spec in ("pg18-create", "pg18-m1c", "pg18-m1", "pg18-create-wal", "pg18-m1c-wal", "pg18-m1-wal",
                     "pg18-create-copy", "pg18-select1"):
            vals = []
            for runner, fs, _ in COLS:
                cj = load(rundir, system, runner, fs, spec, c)
                d = (cj or {}).get("deferred")
                vals.append("MISSING" if cj is None else (f"{d['per_op']:.2f}" if d else "—"))
            if any(v != "MISSING" for v in vals):
                print(f"| {system} | {spec} | " + " | ".join(vals) + " |")
    print(f"\n#### Load-window flushes per op by process role at C={c} (raw, no subtraction; x86_64 XFS / arm64 btrfs)\n")
    print("| Job | Spec | x86_64 XFS | arm64 btrfs |")
    print("|---|---|---|---|")
    pairs = []
    for r in ROWS:
        for s in (r[3], r[4]):
            if s and (r[2], s) not in pairs:
                pairs.append((r[2], s))
    for system, spec in pairs:
        vals = []
        for runner, fs in (("ubuntu-24.04", "xfs"), ("ubuntu-24.04-arm", "btrfs")):
            cj = load(rundir, system, runner, fs, spec, c)
            if cj is None:
                vals.append("MISSING")
                continue
            ops = cj.get("ops") or 1
            roles = cj.get("load_by_role", {})
            vals.append(", ".join(f"{short(k)} {n / ops:.2f}" for k, n in sorted(roles.items(), key=lambda kv: -kv[1]))
                        or "0")
        print(f"| {system} | {spec} | " + " | ".join(vals) + " |")


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) == 3 else 1)
