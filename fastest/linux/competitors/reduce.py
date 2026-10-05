#!/usr/bin/env python3
"""reduce.py RUNDIR [--repo DIR] -- one table over every job of a fastest-competitors run, from its downloaded
artifacts (`gh run download <id> -D RUNDIR`: one directory per artifact, competitors-<system>-<runner>-<fs>/).

What SHOULD be there is never taken from what is there (review finding 11):
  - the expected jobs are the `run` job's matrix (runner x fs x system) of .github/workflows/fastest-competitors.yml
    AT THE RUN'S OWN COMMIT (git_sha from the jobs' run-info.txt, read with `git show` in --repo, default: the
    repository holding this file); a job whose artifact is absent is a MISSING row;
  - the expected cells of a job are the ones run_system.sh listed in run/expected-cells.txt before it ran any; a
    listed cell without a cell.json, or a job without the list, is a MISSING row;
  - and, independently of run_system.sh, PINNED below: amendment 14's registered cells per system at C in {1, 4}. A
    pinned cell the job did not list is MISSING too, so a spec dropped from run_system.sh's SPECLIST cannot vanish
    (second review, finding 4). A system with no pin is MISSING (no expectation), never skipped.
A matrix with include:/exclude: entries is refused (this parser reads only the runner, fs and system lists).
Prints, tab-separated:
  JOBS   artifact, firecheck verdict, functional verdict, failed functional lines
  CELLS  artifact, cell, then stracecount.py's table columns (TABLE_COLS)
Exit 0 only if every expected job and cell is present and readable, every fire-check and functional verdict is a
PASS and every cell verdict is ok; 1 otherwise (after printing everything); 2 if the expectation itself cannot be
determined.
"""
import glob
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import stracecount  # noqa: E402  (the same table columns as each job's own flushes.tsv)

WORKFLOW = ".github/workflows/fastest-competitors.yml"
_PG = ["pg18-select1", "pg18-create", "pg18-m1c", "pg18-m1", "pg18-create-wal", "pg18-m1c-wal", "pg18-m1-wal"]
_DV = ["create", "a-m1c", "a-m1", "b-m1c", "b-m1", "c-m1c", "c-m1"]
PINNED_SPECS = {
    "pg18-d2": _PG,
    "pg18-defaults": _PG + ["pg18-create-copy"],
    "dolt": ["dolt-select1"] + [f"dolt-{v}" for v in _DV],
    "doltgres": ["doltgres-select1"] + [f"doltgres-{v}" for v in _DV],
    "b1": [f"b1-{s}" for s in ("m1c-d2", "m1-d2", "m1c-d0", "m1-d0")],
}
PINNED_CLIENTS = ("1", "4")


def last_line(path, prefix):
    if not os.path.exists(path):
        return "MISSING"
    lines = [l.rstrip("\n") for l in open(path) if l.startswith(prefix)]
    return lines[-1] if lines else "MISSING"


def expected_jobs(d, repo):
    shas = set()
    for p in glob.glob(os.path.join(d, "competitors-*", "run-info.txt")):
        m = re.search(r"git_sha=([0-9a-f]{40})", open(p).read())
        if m:
            shas.add(m.group(1))
    if len(shas) != 1:
        sys.exit(f"reduce: REFUSED: the jobs' run-info.txt name {len(shas)} commits ({sorted(shas)}), not one")
    sha = shas.pop()
    try:
        wf = subprocess.run(["git", "-C", repo, "show", f"{sha}:{WORKFLOW}"], capture_output=True, text=True,
                            check=True, timeout=60).stdout
    except (subprocess.SubprocessError, OSError) as e:
        sys.exit(f"reduce: REFUSED: cannot read {WORKFLOW} at {sha} in {repo}: {e}")
    # The `run` JOB, under the top-level jobs: key (a top-level `defaults: run:` also has a two-space "run:").
    jobs = re.search(r"^jobs:\n(.*)", wf, re.M | re.S)
    job = re.search(r"^  run:\n(.*?)(?=^  \S|\Z)", jobs.group(1), re.M | re.S) if jobs else None
    if not job:
        sys.exit(f"reduce: REFUSED: no `run` job in {WORKFLOW} at {sha}")
    if re.search(r"^\s+(include|exclude):", job.group(1), re.M):
        sys.exit(f"reduce: REFUSED: the run job's matrix at {sha} has include:/exclude: entries, which this parser does"
                 " not read")
    axes = {}
    for k in ("runner", "fs", "system"):
        m = re.search(rf"^\s+{k}: \[([^\]]*)\]", job.group(1), re.M)
        if not m:
            sys.exit(f"reduce: REFUSED: the run job's matrix has no `{k}: [...]` axis at {sha}")
        axes[k] = [v.strip() for v in m.group(1).split(",") if v.strip()]
    names = [f"competitors-{s}-{r}-{f}" for s in axes["system"] for r in axes["runner"] for f in axes["fs"]]
    return sha, axes, names


def main(argv):
    if not argv or argv[0].startswith("-"):
        sys.exit(__doc__)
    d = argv[0]
    repo = HERE
    if len(argv) == 3 and argv[1] == "--repo":
        repo = argv[2]
    elif len(argv) != 1:
        sys.exit(__doc__)
    sha, axes, names = expected_jobs(d, repo)
    present = {os.path.basename(p) for p in glob.glob(os.path.join(d, "competitors-*")) if os.path.isdir(p)}
    missing, bad = 0, 0
    print(f"# run dir {d}: workflow at {sha}: {len(names)} expected jobs "
          f"({len(axes['system'])} systems x {len(axes['runner'])} runners x {len(axes['fs'])} fs)")
    extra = sorted(present - set(names))
    if extra:
        print(f"# artifacts outside the expected matrix (listed, not counted): {extra}")
    print("JOBS\tartifact\tfirecheck\tfunctional\tfailed_checks")
    cells = []
    for name in names:
        a = os.path.join(d, name)
        if name not in present:
            print(f"JOBS\t{name}\tMISSING\tMISSING\tno artifact")
            cells.append((name, "MISSING (no artifact)", None))
            missing += 1
            continue
        fc = last_line(os.path.join(a, "firecheck", "firecheck.txt"), "VERDICT")
        fpath = os.path.join(a, "run", "functional.txt")
        fn = last_line(fpath, "VERDICT")
        fails = [l.strip() for l in open(fpath) if l.startswith(("FAIL", "REFUSED"))] if os.path.exists(fpath) else []
        print(f"JOBS\t{name}\t{fc}\t{fn}\t{' || '.join(fails)}")
        if not fc.startswith("VERDICT PASS") or not fn.startswith("VERDICT PASS"):
            bad += 1
        exp_path = os.path.join(a, "run", "expected-cells.txt")
        if not os.path.exists(exp_path):
            cells.append((name, "MISSING (no expected-cells.txt)", None))
            missing += 1
            continue
        listed = [l.strip() for l in open(exp_path) if l.strip()]
        system = next((s for s in axes["system"] if name.startswith(f"competitors-{s}-")), None)
        if system not in PINNED_SPECS:
            cells.append((name, f"MISSING (no pinned expectation for system {system})", None))
            missing += 1
        else:
            for pinned in [f"{s}-c{c}" for s in PINNED_SPECS[system] for c in PINNED_CLIENTS]:
                if pinned not in listed:
                    cells.append((name, f"{pinned} MISSING (pinned, not listed by run_system.sh)", None))
                    missing += 1
        for cell in listed:
            cj_path = os.path.join(a, "run", "cells", cell, "cell.json")
            try:
                cells.append((name, cell, json.load(open(cj_path))))
            except (OSError, ValueError) as e:
                cells.append((name, f"{cell} MISSING ({e.__class__.__name__})", None))
                missing += 1
    print("CELLS\tartifact\tcell\t" + "\t".join(stracecount.TABLE_COLS))
    for name, cell, c in cells:
        if c is None:
            print(f"CELLS\t{name}\t{cell}" + "\tMISSING" * len(stracecount.TABLE_COLS))
            continue
        print("CELLS\t" + "\t".join([name, cell] + [str(x) for x in stracecount.table_row(c)]))
        if c.get("verdict") != "ok":
            bad += 1
    if missing or bad:
        print(f"# {missing} expected job(s) or cell(s) MISSING; {bad} job verdict(s) not PASS or cell verdict(s) not ok",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
