#!/usr/bin/env python3
"""v3l.py's mutants (T3 runner review items 3, 4, 6, 10, 18, 19; fourth lane review HIGH 3): each one deletes or
weakens one check, in a temp copy of v3l.py, and `v3l.py self-test` must fail on the copy.

  v3l_mutants.py [NAME...]   run every mutant (or the named ones); exit 0 iff each is KILLED, 1 if any SURVIVED or
                             BROKE, 2 if refused (the unmutated self-test is not green, an unknown name, or a target
                             text not found exactly once in v3l.py: the table is stale, so nothing is run)

KILLED means the copy's self-test exits 1 with at least one FAIL line. A copy that exits otherwise (a crash at import,
a SyntaxError, a timeout) is BROKEN, not killed: a mutant that cannot load proves nothing about the check it removed.
Each copy gets a `testdata` symlink beside it, because the self-test reads testdata/ next to v3l.py.

The last column is the last observed result and where it was observed. Entries marked UNRUN were written under QUIET
(2026-10-09, no local runs) and are owed: the first run of this file is that run. The RUN entries were observed with
the same logic from a scratchpad runner, before this file existed; this file itself has never been run.
"""
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "v3l.py")

MUTANTS = [
    # (name, what it removes, old text in v3l.py, replacement, last observed result)
    ("3a-delete-lab-short-check", "item 3: a write-back labelling count below N VOIDs",
     '        elif isinstance(lab, int) and lab < N:\n'
     '            bad.append(f"write-back {what}: its flush counter rose {lab} across the {N} fsyncs of the labelling run "\n'
     '                       "(fewer than one per fsync in the strace-checked run)")\n',
     '', "KILLED, 41/52 FAIL, at d07abc424"),
    ("3b-restore-lab-ge-N-skip", "item 3: the same rule, disabled in place",
     'elif isinstance(lab, int) and lab < N:', 'elif isinstance(lab, int) and lab < N and False:',
     "KILLED, 41/52 FAIL, at d07abc424"),
    ("3c-base-rewrites-real-lab-below-N", "item 3: the plant base never rewrites a real labelling count",
     '        elif d.get("lab_flush_ios_delta") is None:',
     '        elif d.get("lab_flush_ios_delta") is None or d["lab_flush_ios_delta"] < N:',
     "KILLED, 50/52 FAIL, at d07abc424"),
    ("3d-delete-unreadable-rule", "item 3: a labelling count that is not an int VOIDs",
     '        if lab is not None and (not isinstance(lab, int) or isinstance(lab, bool)):', '        if False:',
     "KILLED, 51/52 FAIL, at d07abc424"),
    ("4a-delete-wt-labelling-half", "item 4 (a): write-through labelling count must be 0",
     '        if lab is not None and lab != 0:', '        if False:', "KILLED, 53/61 FAIL, at af71b0df9"),
    ("4b-layer-lab-passed-as-None", "item 4 (b): a loop layer's labelling count reaches its gate",
     '                            lay.get("lab_flush_ios_delta"))', '                            None)',
     "KILLED, 51/61 FAIL, at af71b0df9"),
    ("4c-delete-wt-timed-half", "item 4 (a): write-through timed count must be 0",
     '        if timed != 0:', '        if False:', "KILLED, 54/61 FAIL, at af71b0df9"),
    ("18a-restore-whole-number-floor", "item 18: the proportional floor",
     'Fraction(timed) < Fraction(lab) * (1 - SLACK)', 'timed < (lab // N) * N', "UNRUN (QUIET)"),
    ("18b-slack-zero", "item 18: SLACK is 0.05",
     'SLACK = Fraction("0.05")', 'SLACK = Fraction("0")', "UNRUN (QUIET)"),
    ("18c-delete-floor-rule", "item 18: the floor rule as a whole",
     'elif isinstance(lab, int) and not short and Fraction(timed)', 'elif False and Fraction(timed)',
     "UNRUN (QUIET)"),
    ("18d-publish-drops-labelling-ratio", "item 18: the labelling run's ratio is published",
     '"lab_flush_ios_per_fsync": per_fsync(ft.get("lab_flush_ios_delta")),', '"lab_flush_ios_per_fsync": None,',
     "UNRUN (QUIET)"),
    ("19a-unknown-falls-through-to-counter-0", "item 19: 'write cache unknown (VOID)'",
     '        k = "write cache unknown (VOID)"',
     '        k = "no volatile cache: no flush request sent (counter 0 checked in both runs)"', "UNRUN (QUIET)"),
    ("19b-write-through-ignores-counters", "item 19: 'counter 0 checked' only when both counts are 0",
     '    elif wc == "write through" and timed == 0 and lab == 0:', '    elif wc == "write through":',
     "UNRUN (QUIET)"),
    ("19c-delete-timed-only-branch", "item 19: no labelling count is named as such",
     '    elif wc == "write through" and timed == 0 and lab is None:', '    elif False:', "UNRUN (QUIET)"),
    ("19d-docstring-drops-a-text", "item 19: the docstring names every floor_kind text",
     '"write cache unknown (VOID)". A virtualized', '"write cache unknown". A virtualized', "UNRUN (QUIET)"),
    ("H3a-base-reports-wc-on-ram", "lane review 4 HIGH 3: a ram plant base keeps 'none (RAM)'",
     '    r["leaf"]["drive_reports"] = drive_report_for(r["leaf"].get("disk"), wc)',
     '    r["leaf"]["drive_reports"] = wc', "UNRUN (QUIET)"),
    ("H3b-no-ram-exception", "lane review 4 HIGH 3 / LOW 7: a ram disk's agreeing report is 'none (RAM)'",
     '    return "none (RAM)" if str(disk or "").startswith("ram") else wc', '    return wc', "UNRUN (QUIET)"),
    ("10a-delete-write-rule", "T3 runner review item 10: the sectors-written gate",
     '        bad += write_gate(rec["leaf"].get("disk"), rec["arms"]["fsync"]["timed"])\n', '', "UNRUN (QUIET)"),
    ("10b-labelling-run-not-judged", "item 10: the labelling fsync run is judged as well as the timed one",
     '(("timed", "sectors_written_delta"), ("labelling", "lab_sectors_written_delta"))',
     '(("timed", "sectors_written_delta"),)', "UNRUN (QUIET)"),
    ("10c-half-threshold", "item 10: the threshold is the whole fsynced data, N x 8 sectors",
     '        elif v < need:', '        elif v < need // 2:', "UNRUN (QUIET)"),
    ("10d-ram-exemption-for-all", "item 10: only a ram disk is exempt",
     '    if not str(rec["leaf"].get("disk") or "").startswith("ram"):', '    if False:', "UNRUN (QUIET)"),
    ("6a-delete-L5-rederivation", "T3 runner review item 6: block() re-derives the verdict from the arms (review L5)",
     '            if bool(again) != (r["verdict"] == "VOID") or r["verdict"] not in ("VALID", "VOID"):',
     '            if False:', "UNRUN (QUIET)"),
    ("10e-no-wt-unwritten-plant", "item 10: the write rule is forced to fire on the real record (plant)",
     '        arm("wt-unwritten", "write through",', '        (lambda *a: None)("wt-unwritten", "write through",',
     "UNRUN (QUIET)"),
]


def selftest(path):
    r = subprocess.run(["timeout", "120", sys.executable, "-B", path, "self-test"], capture_output=True, text=True)
    lines = r.stdout.splitlines()
    fails = [ln for ln in lines if " FAIL: " in ln]
    total = next((ln for ln in reversed(lines) if ln.startswith("V3L SELF-TEST")), None)
    return r.returncode, fails, total, r.stderr


def main(argv):
    names = argv[1:]
    known = {m[0] for m in MUTANTS}
    if [n for n in names if n not in known]:
        print(f"v3l_mutants: REFUSED: unknown mutant(s) {[n for n in names if n not in known]}")
        return 2
    rc, fails, total, _ = selftest(SRC)
    if rc != 0 or fails or not total or not total.endswith(" PASS"):
        print(f"v3l_mutants: REFUSED: the unmutated self-test is not green (rc {rc}; {total}; {len(fails)} FAIL lines)")
        return 2
    print(f"control (unmutated): {total}")
    src = open(SRC).read()
    todo = [m for m in MUTANTS if not names or m[0] in names]
    stale = [(m[0], src.count(m[2])) for m in todo if src.count(m[2]) != 1]
    if stale:
        print(f"v3l_mutants: REFUSED: target text not found exactly once (name, count): {stale}")
        return 2
    d = tempfile.mkdtemp(prefix="v3lmut-")
    bad = 0
    for name, what, old, new, last in todo:
        p = os.path.join(d, name)
        os.makedirs(p)
        os.symlink(os.path.join(HERE, "testdata"), os.path.join(p, "testdata"))
        with open(os.path.join(p, "v3l.py"), "w") as f:
            f.write(src.replace(old, new))
        rc, fails, total, err = selftest(os.path.join(p, "v3l.py"))
        verdict = "KILLED" if rc == 1 and fails else "SURVIVED" if rc == 0 else "BROKEN"
        bad += verdict != "KILLED"
        print(f"MUTANT {name} ({what}): {verdict} (rc {rc}; {total}; last recorded: {last})")
        for ln in fails:
            print(f"    {ln}")
        if verdict == "BROKEN":
            print(f"    stderr: {err[-300:]}")
    print(f"v3l_mutants: {len(todo) - bad}/{len(todo)} KILLED ({d})")
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
