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

The PostgreSQL, Dolt and Doltgres labels name the version EVERY job of that system ran (LOW 25), read from the first
line of each job's run/version.txt in the version command's own form (pins.FIRST_LINE). The tables are refused
(exit 1, the reasons on stderr) when a job has no such line, when the jobs differ, or when they differ from the
registered version of versions.tsv (pins.version). A run at older versions keeps the tables banked in its own day.

  smoke_table.py selftest     known answers for that refusal (fixture run dirs; nothing else is read)
"""
import contextlib
import io
import json
import os
import re
import sys
import tempfile

import pins

COLS = [("ubuntu-24.04", "xfs", "x86_64 XFS"), ("ubuntu-24.04", "btrfs", "x86_64 btrfs"),
        ("ubuntu-24.04-arm", "xfs", "arm64 XFS"), ("ubuntu-24.04-arm", "btrfs", "arm64 btrfs")]

# Each versioned job system and its versions.tsv system; placeholders until main() reads the run's versions (LOW 25:
# the label was taken from whichever job sorted first, and read '?' when none had one)
VSYS = {"pg18-d2": "postgresql", "dolt": "dolt", "doltgres": "doltgres"}
PG, DOLT, DOLTGRES = "PostgreSQL ?", "Dolt ? sql-server", "Doltgres ?"


def job_version(rundir, system, runner, fs):
    """The version on the FIRST line of one job's run/version.txt, in its command's own form, or None."""
    p = os.path.join(rundir, f"competitors-{system}-{runner}-{fs}", "run", "version.txt")
    try:
        lines = open(p, errors="replace").read().splitlines()
    except OSError:
        return None
    m = re.fullmatch(pins.FIRST_LINE[VSYS[system]], lines[0].strip()) if lines else None
    return m.group(1) if m else None


def run_versions(rundir, table=pins.TABLE):
    """({job system: version}, problems) over every job the tables show; any problem refuses the tables."""
    out, why = {}, []
    for system, vsys in VSYS.items():
        seen = {head: job_version(rundir, system, runner, fs) for runner, fs, head in COLS}
        missing = [h for h, v in seen.items() if v is None]
        if missing:
            why.append(f"{system}: no version line in run/version.txt of {', '.join(missing)}")
        got = sorted({v for v in seen.values() if v is not None})
        if len(got) > 1:
            why.append(f"{system}: the jobs ran different versions ({'; '.join(f'{h} {v}' for h, v in seen.items())})")
        want = pins.version(vsys, table)
        if want is None:
            why.append(f"{system}: versions.tsv registers no single {vsys} version")
        elif any(g != want for g in got):
            why.append(f"{system}: ran {', '.join(g for g in got if g != want)}, not the registered {want}")
        if not missing and got == [want]:
            out[system] = want
    return out, why


# (system label, variant label, job system, create spec, create+first-write spec or None)
ROWS = [
    (PG, "FILE_COPY, file_copy_method=clone, D2 (M1c-create / M1)", "pg18-d2", "pg18-create", "pg18-m1"),
    (PG, "FILE_COPY clone, D2, M1c-connect (+ connect + SELECT 1)", "pg18-d2", "pg18-m1c", None),
    (PG, "WAL_LOG, D2 (M1c-create / M1)", "pg18-d2", "pg18-create-wal", "pg18-m1-wal"),
    (PG, "WAL_LOG, D2, M1c-connect", "pg18-d2", "pg18-m1c-wal", None),
    # pg18-defaults is dropped on Linux (lead ruling, artie 6b0bef481b); its copy control runs on pg18-d2
    (PG, "FILE_COPY, file_copy_method=copy (clone-proof control)", "pg18-d2", "pg18-create-copy", None),
    (PG, "SELECT 1 floor", "pg18-d2", "pg18-select1", None),
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


def fmt(cj, key="flushes"):
    if cj is None:
        return "MISSING"
    if cj.get("verdict") != "ok":
        return "FAIL"
    v = cj.get("per_op", {}).get(key)
    if v is None:
        return "?" if key == "flushes" else "—"
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


def main(rundir, c, table=pins.TABLE):
    vers, why = run_versions(rundir, table)
    if why:  # LOW 25: no table names a version that every job did not run, or that is not the registered one
        sys.exit("smoke_table.py: REFUSED: " + "; ".join(why))
    # Every op is a CYCLE: create [+ switch][+ first write], then the untimed delete that holds the live-branch count
    # fixed (lead review 62430d8bf..b49fb656a HIGH 1). Per-create flushes exist at C=1 only, from the phase split.
    tables = [("flushes", f"#### Flushes per cycle at C={c}: create cycle / create+first-write cycle (each cycle ends "
                          "in its untimed delete; idle control subtracted)\n")]
    if str(c) == "1":
        tables.append(("create", "\n#### Flushes per create at C=1: the create share of each cycle (each flush placed "
                                 "by the load generator's op times; the delete's and the between-op flushes left out; "
                                 "idle control subtracted over the create time)\n"))
    label = {PG: f"PostgreSQL {vers['pg18-d2']}", DOLT: f"Dolt {vers['dolt']} sql-server",
             DOLTGRES: f"Doltgres {vers['doltgres']}"}
    for key, title in tables:
        print(title)
        print("| System | Variant | " + " | ".join(h for _, _, h in COLS) + " |")
        print("|---|---|" + "---|" * len(COLS))
        for sysl, var, system, cspec, wspec in ROWS:
            sysl = label.get(sysl, sysl)
            cells = []
            for runner, fs, _ in COLS:
                a = fmt(load(rundir, system, runner, fs, cspec, c), key)
                b = fmt(load(rundir, system, runner, fs, wspec, c), key) if wspec else "—"
                cells.append(f"{a} / {b}")
            print(f"| {sysl} | {var} | " + " | ".join(cells) + " |")
    # LOW 23: the PG rows' buffer pool, per job (pg_settings.tsv: shared_buffers in 8kB pages, the server's NBuffers)
    print(f"\n#### PostgreSQL 18 shared_buffers per job (pg_settings; 25% of MemTotal since gate-6 item 15)\n")
    print("| Job | " + " | ".join(h for _, _, h in COLS) + " |")
    print("|---|" + "---|" * len(COLS))
    vals = []
    for runner, fs, _ in COLS:
        p = os.path.join(rundir, f"competitors-pg18-d2-{runner}-{fs}", "run", "pg_settings.tsv")
        try:
            row = next(ln.rstrip("\n").split("\t") for ln in open(p) if ln.startswith("shared_buffers\t"))
            vals.append(f"{row[1]} x 8 kB = {int(row[1]) * 8 // 1024} MB")
        except (OSError, StopIteration, ValueError, IndexError):
            vals.append("MISSING")
    print("| pg18-d2 | " + " | ".join(vals) + " |")
    print(f"\n#### PostgreSQL 18 deferred flushes per cycle at C={c} (one CHECKPOINT after each cell, divided by its ops)\n")
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


def selftest():
    """Known answers for the version refusal (LOW 25), over fixture run dirs and a fixture versions.tsv."""
    bad = n = 0

    def ok(name, cond):  # counts its own cases
        nonlocal bad, n
        print(("PASS" if cond else "FAIL"), name)
        bad += not cond
        n += 1

    reg = {"pg18-d2": "18.6", "dolt": "2.4.1", "doltgres": "1.4.0"}
    first = {"pg18-d2": "postgres (PostgreSQL) {} (Ubuntu 18.6-1.pgdg24.04+2)", "dolt": "dolt version {}",
             "doltgres": "Doltgres version {}"}
    with tempfile.TemporaryDirectory() as d:
        tsv = os.path.join(d, "versions.tsv")
        with open(tsv, "w") as f:
            f.write("postgresql\t18.6\tany\tpgdg_package\tp\ndolt\t2.4.1\tamd64\ttarball_sha256\ta\n"
                    "doltgres\t1.4.0\tamd64\ttarball_sha256\tg\n")
        tsv2 = os.path.join(d, "versions2.tsv")
        with open(tsv2, "w") as f:  # two Dolt versions: no single registered one
            f.write(open(tsv).read() + "dolt\t2.5.0\tarm64\ttarball_sha256\tb\n")

        def make(name, override=None):
            """A run dir with every job's version.txt at the registered versions, except OVERRIDE's
            {(job system, column head): text, or None for no file}."""
            root = os.path.join(d, name)
            for system in VSYS:
                for runner, fs, head in COLS:
                    text = first[system].format(reg[system]) + "\n" + "0" * 64 + "  /usr/bin/server\n"
                    text = (override or {}).get((system, head), text)
                    run = os.path.join(root, f"competitors-{system}-{runner}-{fs}", "run")
                    os.makedirs(run)
                    if text is not None:
                        with open(os.path.join(run, "version.txt"), "w") as f:
                            f.write(text)
            return root

        good = make("good")
        vers, why = run_versions(good, tsv)
        ok("every job at the registered versions: accepted, each system labelled", vers == reg and why == [])
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            main(good, 1, tsv)
        out = buf.getvalue()
        ok("the tables label PG, Dolt and Doltgres with the run's versions",
           "| PostgreSQL 18.6 |" in out and "| Dolt 2.4.1 sql-server |" in out and "| Doltgres 1.4.0 |" in out
           and not any(p in out for p in (PG, DOLT, DOLTGRES)))
        vers, why = run_versions(make("pgbare", {("pg18-d2", "arm64 XFS"): "postgres (PostgreSQL) 18.6\n"}), tsv)
        ok("control: PG's first line without the package note is accepted", why == [] and vers["pg18-d2"] == "18.6")
        cases = [
            ("one job without version.txt", {("dolt", "arm64 btrfs"): None}, "no version line"),
            ("one job's first line is a warning", {("doltgres", "x86_64 XFS"): "warning: x\nDoltgres version 1.4.0\n"},
             "no version line"),
            ("an empty version.txt", {("pg18-d2", "x86_64 btrfs"): ""}, "no version line"),
            ("one job ran another version", {("doltgres", "x86_64 btrfs"): "Doltgres version 1.3.3\n"},
             "different versions"),
            ("one job ran a suffixed build", {("dolt", "x86_64 XFS"): "dolt version 2.4.1-rc1\n"}, "different versions"),
            ("every job agrees on an unregistered version",
             {("dolt", h): "dolt version 2.3.5\n" for _, _, h in COLS}, "not the registered 2.4.1"),
            ("every PG job at 18.7", {("pg18-d2", h): "postgres (PostgreSQL) 18.7\n" for _, _, h in COLS},
             "not the registered 18.6"),
        ]
        for i, (name, override, want) in enumerate(cases):
            root = make(f"bad{i}", override)
            vers, why = run_versions(root, tsv)
            system = next(iter(override))[0]
            ok(f"refused: {name}", any(want in w for w in why) and system not in vers)
            buf = io.StringIO()
            try:
                with contextlib.redirect_stdout(buf):
                    main(root, 1, tsv)
                refused = False
            except SystemExit as e:
                refused = isinstance(e.code, str) and "REFUSED" in e.code
            ok(f"main exits with REFUSED, printing no table: {name}", refused and buf.getvalue() == "")
        vers, why = run_versions(good, tsv2)
        ok("refused: versions.tsv registers two Dolt versions", any("no single dolt" in w for w in why))
    print(f"smoke_table selftest: {n - bad}/{n}")
    return 1 if bad else 0


if __name__ == "__main__":
    if sys.argv[1:] == ["selftest"]:
        sys.exit(selftest())
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) == 3 else 1)
