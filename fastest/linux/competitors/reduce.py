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
    (second review, finding 4). A system with no pin is MISSING (no expectation), never skipped. Likewise the pinned
    matrix (PINNED_SPECS's systems x PINNED_RUNNERS x PINNED_FS) is added to the workflow's, so a job dropped from the
    workflow is a MISSING row (third review, finding 6).
A matrix with include:/exclude: entries is refused (this parser reads only the runner, fs and system lists).
Prints, tab-separated:
  JOBS     artifact, firecheck verdict, functional verdict, failed functional lines
  TIMED    artifact, cell, timed.json verdict (only the cells whose timed run is not ok)
  FIXTURE  one line: the run's one parent fixture, or why the jobs' fixtures differ
  DRIVES   artifact, drive class (drive.py CLASSES), whether a flush reaches the drive (no filesystem in the chain
           mounted nobarrier), disks, device chain
  CELLS    artifact, cell, then stracecount.py's table columns (TABLE_COLS)
Exit 0 only if every expected job and cell is present and readable, every fire-check passed each PINNED_FIRECHECK
check (one PASS line each, no FAIL line, one "VERDICT PASS n/n" with n the pinned count), every functional verdict is
a PASS, every cell verdict and timed verdict is ok, the fixtures agree and every present job has its drive class;
1 otherwise (after printing everything); 2 if the expectation itself cannot be
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
import fixture  # noqa: E402  (one parent fixture for every system)
import drive  # noqa: E402  (the drive classes a job's drive.json may name)

WORKFLOW = ".github/workflows/fastest-competitors.yml"
_PG = ["pg18-select1", "pg18-create", "pg18-m1c", "pg18-m1", "pg18-create-wal", "pg18-m1c-wal", "pg18-m1-wal"]
_DV = ["create", "a-m1c", "a-m1", "b-m1c", "b-m1", "c-m1c", "c-m1"]
# pg18-defaults is DROPPED on Linux (lead ruling, artie DECISIONS 6b0bef481b: on Linux it configures pg18-d2's server,
# and PREREG-CORE v2's OUT list excludes PG18 at its defaults); pg18-d2 runs the clone proof's copy control. A workflow
# that lists pg18-defaults again gets MISSING rows (no pinned expectation), and run_system.sh refuses the system.
PINNED_SPECS = {
    "pg18-d2": _PG + ["pg18-create-copy"],
    "dolt": ["dolt-select1"] + [f"dolt-{v}" for v in _DV],
    "doltgres": ["doltgres-select1"] + [f"doltgres-{v}" for v in _DV],
    "b1": [f"b1-{s}" for s in ("m1c-d2", "m1-d2", "m1c-d0", "m1-d0")],
}
PINNED_CLIENTS = ("1", "4")
PINNED_RUNNERS = ("ubuntu-24.04", "ubuntu-24.04-arm")
PINNED_FS = ("xfs", "btrfs")
# The fire-check's checks, pinned (fifth review, finding 4): a job passes its fire-check only when firecheck.txt holds
# exactly one PASS line for each of these, no FAIL line, and one "VERDICT PASS n/n" (n = their count) -- a fire-check
# that silently lost a check (or a truncated, or an older, verdict file) cannot read as passed.
PINNED_FIRECHECK = (
    "F1-launch", "F2-attach", "F3-idle-attach", "F3b-unproven-empty-refused", "F4-osync-refused",
    "F5-io_uring-refused", "F6-osync-before-attach-refused", "F6b-osync-in-a-descendant-refused",
    "F6c-dead-leader-scanned", "F6d-scan-task-lost-unscanned", "F6e-scan-task-lost-rescanned",
    "F7-threads-before-attach", "F8-rwf_dsync-refused", "F9-libaio-refused", "F10a-untraced-descendant-detected",
    "F10b-descendants-attached", "F10c-storm-control-misses", "F10d-storm-attach-complete", "F11-split-pre",
    "F11-split-post", "F12-t1-cut", "F13-clock-step-refused", "F14-stamper-subshell-safe",
)


def refuse(msg):
    """The expectation itself cannot be determined: say why and exit 2 (the docstring's promise; sys.exit(str) is 1)."""
    print(msg, file=sys.stderr)
    sys.exit(2)


def last_line(path, prefix):
    if not os.path.exists(path):
        return "MISSING"
    lines = [l.rstrip("\n") for l in open(path) if l.startswith(prefix)]
    return lines[-1] if lines else "MISSING"


def firecheck_problem(path):
    """None when the fire-check at PATH passed every pinned check; otherwise why not."""
    if not os.path.exists(path):
        return "no firecheck.txt"
    text = [l.rstrip("\n") for l in open(path, errors="replace")]
    passed = sorted(l[len("PASS "):].split(":", 1)[0] for l in text if l.startswith("PASS "))
    failed = [l.split(":", 1)[0] for l in text if l.startswith("FAIL")]
    verdicts = [l for l in text if l.startswith("VERDICT")]
    n = len(PINNED_FIRECHECK)
    why = []
    if verdicts != [f"VERDICT PASS {n}/{n}"]:
        why.append(f"verdict lines {verdicts[-3:]} (want exactly one 'VERDICT PASS {n}/{n}')")
    if passed != sorted(PINNED_FIRECHECK):
        why.append(f"PASS names differ from the pinned {n}: missing {sorted(set(PINNED_FIRECHECK) - set(passed))}, "
                   f"extra {sorted(set(passed) - set(PINNED_FIRECHECK))}, {len(passed)} PASS line(s)")
    if failed:
        why.append(f"FAIL lines {failed[:5]}")
    return "; ".join(why) or None


def expected_jobs(d, repo):
    shas = set()
    for p in glob.glob(os.path.join(d, "competitors-*", "run-info.txt")):
        m = re.search(r"git_sha=([0-9a-f]{40})", open(p).read())
        if m:
            shas.add(m.group(1))
    if len(shas) != 1:
        refuse(f"reduce: REFUSED: the jobs' run-info.txt name {len(shas)} commits ({sorted(shas)}), not one")
    sha = shas.pop()
    try:
        wf = subprocess.run(["git", "-C", repo, "show", f"{sha}:{WORKFLOW}"], capture_output=True, text=True,
                            check=True, timeout=60).stdout
    except (subprocess.SubprocessError, OSError) as e:
        refuse(f"reduce: REFUSED: cannot read {WORKFLOW} at {sha} in {repo}: {e}")
    # The `run` JOB, under the top-level jobs: key (a top-level `defaults: run:` also has a two-space "run:").
    jobs = re.search(r"^jobs:\n(.*)", wf, re.M | re.S)
    job = re.search(r"^  run:\n(.*?)(?=^  \S|\Z)", jobs.group(1), re.M | re.S) if jobs else None
    if not job:
        refuse(f"reduce: REFUSED: no `run` job in {WORKFLOW} at {sha}")
    if re.search(r"^\s+(include|exclude):", job.group(1), re.M):
        refuse(f"reduce: REFUSED: the run job's matrix at {sha} has include:/exclude: entries, which this parser does"
                 " not read")
    axes = {}
    for k in ("runner", "fs", "system"):
        m = re.search(rf"^\s+{k}: \[([^\]]*)\]", job.group(1), re.M)
        if not m:
            refuse(f"reduce: REFUSED: the run job's matrix has no `{k}: [...]` axis at {sha}")
        axes[k] = [v.strip() for v in m.group(1).split(",") if v.strip()]
    # The pinned matrix too: a system, runner or filesystem dropped from the workflow is a MISSING job here, not an
    # absent row (third review, finding 6).
    for k, vals in (("system", list(PINNED_SPECS)), ("runner", PINNED_RUNNERS), ("fs", PINNED_FS)):
        axes[k] = axes[k] + [v for v in vals if v not in axes[k]]
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
        fcpath = os.path.join(a, "firecheck", "firecheck.txt")
        fc = last_line(fcpath, "VERDICT")
        fcwhy = firecheck_problem(fcpath)
        fpath = os.path.join(a, "run", "functional.txt")
        fn = last_line(fpath, "VERDICT")
        fails = [l.strip() for l in open(fpath) if l.startswith(("FAIL", "REFUSED"))] if os.path.exists(fpath) else []
        if fcwhy:
            fails = [f"FIRECHECK NOT PASSED: {fcwhy}"] + fails
        print(f"JOBS\t{name}\t{fc}\t{fn}\t{' || '.join(fails)}")
        # the fire-check must pass ALL its pinned checks, not merely end in "VERDICT PASS n/n" for some n (fifth
        # review, finding 4; fourth review, finding 8 required only n/n)
        if fcwhy or not fn.startswith("VERDICT PASS"):
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
            # Every cell's latency comes from its untraced timed run, judged by timedrun.py (gate-6 review, t3run
            # item 2): a cell without a timed run, or whose timed run was traced, is not a cell.
            tj = os.path.join(a, "run", "cells", cell, "timed.json")
            try:
                tv = json.load(open(tj)).get("verdict")
            except (OSError, ValueError) as e:
                tv = f"MISSING ({e.__class__.__name__})"
            if tv != "ok":
                print(f"TIMED\t{name}\t{cell}\t{tv}")
                bad += 1
    # One parent fixture for every system of the run (gate-6 review, t3run item 4): every present job's
    # run/fixture.json must name the same rows, aging, live branches and generator digest (fixture.py compare).
    fxs = []
    for name in names:
        fp = os.path.join(d, name, "run", "fixture.json")
        if name not in present:
            continue
        try:
            fxs.append(json.load(open(fp)))
        except (OSError, ValueError) as e:
            print(f"FIXTURE\t{name}\tMISSING ({e.__class__.__name__})")
            bad += 1
    fwhy = fixture.compare(fxs)
    print("FIXTURE\tall jobs\t" + ("one parent: " + json.dumps({k: fxs[0].get(k) for k in fixture.KEYS})
                                   if not fwhy else "REFUSED: " + "; ".join(fwhy)))
    bad += 1 if fwhy else 0
    # The drive class each job ran on (lead ruling, artie DECISIONS 6b0bef481b; SMOKE erratum E3): run/drive.json from
    # drive.py, which run_system.sh writes before any cell; a present job without a readable one is not a pass.
    # A class outside drive.CLASSES, or no flush_reaches_drive boolean, is not a record (review of e11a3c993, findings
    # 3 and 11).
    print("DRIVES\tartifact\tdrive_class\tflush_reaches_drive\tdisks\tchain")
    for name in names:
        if name not in present:
            continue
        try:
            dj = json.load(open(os.path.join(d, name, "run", "drive.json")))
            if dj["drive_class"] not in drive.CLASSES or not isinstance(dj["flush_reaches_drive"], bool):
                raise ValueError(f"drive_class {dj['drive_class']!r}, flush_reaches_drive "
                                 f"{dj['flush_reaches_drive']!r}")
            row = [dj["drive_class"], "yes" if dj["flush_reaches_drive"] else "no",
                   ",".join(f"{x['name']}({x['model']})" for x in dj["disks"]), ">".join(dj["chain"])]
        except (OSError, ValueError, KeyError, TypeError) as e:
            print(f"DRIVES\t{name}\tMISSING ({e.__class__.__name__}: {e})")
            bad += 1
            continue
        print("DRIVES\t" + "\t".join([name] + row))
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
