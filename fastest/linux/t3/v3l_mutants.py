#!/usr/bin/env python3
"""v3l.py's mutants (T3 runner review items 3, 4, 6, 10, 18, 19; fourth lane review HIGH 3; annex A24): each one
deletes or weakens one check, in a temp copy of v3l.py, and `v3l.py self-test` must fail on the copy.

  v3l_mutants.py [NAME...]   run every mutant (or the named ones); exit 0 iff each is KILLED, 1 if any SURVIVED or
                             BROKE, 2 if refused (an unknown name; a stale table: a target not found exactly once in
                             v3l.py, an indented target not at a line start, or a mutated source that does not
                             compile, review 5 MED 5, checked first, so nothing runs; the unmutated self-test not
                             green; or the unmutated copy in the mutants' temp layout not green with the in-place
                             control's total, review 6 LOW 15, so no mutant runs)
  v3l_mutants.py self-test   the table guard on synthetic sources and on the real table

KILLED means the copy's self-test exits 1 with at least one FAIL line. A copy that exits otherwise (a crash at import,
a SyntaxError, a timeout) is BROKEN, not killed: a mutant that cannot load proves nothing about the check it removed.
Each copy gets a `testdata` symlink beside it, because the self-test reads testdata/ next to v3l.py; the unmutated
copy runs in that layout first, so a layout difference cannot kill every mutant for the wrong reason.

The last column is the last observed result and where it was observed. Entries marked UNRUN were written under QUIET
(2026-10-09, no local runs) and are owed: the first run of this file is that run. The RUN entries were observed with
the same logic from a scratchpad runner, before this file existed; this file itself has never been run.
"""
import os
import shutil
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
     '            bad += write_gate(rec["leaf"].get("disk"), rec["arms"]["fsync"]["timed"])\n', '            pass\n',
     "UNRUN (QUIET)"),
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
    ("A24a-delete-sync-rule", "annex A24: an fsync run on a drive needs a clean sync record",
     '        bad += sync_gate(rec["leaf"].get("disk"), rec["arms"]["fsync"].get("sync"))\n', '', "UNRUN (QUIET)"),
    ("A24b-rc-not-judged", "annex A24: the sync's rc is judged, not only that it ran",
     '        elif r.get("ran") is not True or type(r.get("rc")) is not int or r["rc"] != 0:',
     '        elif r.get("ran") is not True:', "UNRUN (QUIET)"),
    # review 6 LOW 10, review 5 LOW 23-25: checks that could be deleted with the self-test green
    ("A24c-labelling-sync-dropped", "review 6 LOW 10: the labelling run's sync is judged too",
     '    for run in ("timed", "labelling"):\n        r = sy.get(run) if isinstance(sy, dict) else None\n'
     '        if not isinstance(r, dict):\n',
     '    for run in ("timed",):\n        r = sy.get(run) if isinstance(sy, dict) else None\n'
     '        if not isinstance(r, dict):\n', "UNRUN (QUIET)"),
    ("A24d-no-unsynced-plant", "review 6 LOW 10: the timed run's sync rule is forced to fire (plant)",
     '        arm("unsynced", rec["leaf"]["write_cache"],',
     '        (lambda *a: None)("unsynced", rec["leaf"]["write_cache"],', "UNRUN (QUIET)"),
    ("A24i-no-unsynced-lab-plant", "review 6 LOW 10: the labelling run's sync rule is forced to fire (plant)",
     '        arm("unsynced-lab", rec["leaf"]["write_cache"],',
     '        (lambda *a: None)("unsynced-lab", rec["leaf"]["write_cache"],', "UNRUN (QUIET)"),
    ("M2b-measure-iostats-refusal", "review 6 LOW 10: measure() refuses iostats 0 before any run",
     '    if ip:\n        raise RuntimeError(f"{disk}: {ip} (review 5 MED 2; refused before any run)")\n',
     '    if False:\n        raise RuntimeError(f"{disk}: {ip} (review 5 MED 2; refused before any run)")\n',
     "UNRUN (QUIET)"),
    ("14a-real-mode-unchecked", "review 5 LOW 25: measure() refuses a real run without a drive and its leaf record",
     '    if why:\n        raise RuntimeError(why)\n', '    if False:\n        raise RuntimeError(why)\n', "UNRUN (QUIET)"),
    ("23a-void-reads-completed", "review 5 LOW 23: a write-back VOID never reads 'flush requests completed'",
     '        k = "write-back drive with flushes short (VOID)"',
     '        k = "write-back drive: flush requests completed by the driver (issued and acknowledged, not persistence)"',
     "UNRUN (QUIET)"),
    ("24a-timed-count-unread", "review 5 LOW 24: a timed count that is not an int VOIDs as unreadable",
     '        if timed is not None and (not isinstance(timed, int) or isinstance(timed, bool)):\n',
     '        if False:\n', "UNRUN (QUIET)"),
    # annex A27: SLACK bound to a labelling pair (the refusal, and the SLACK/2 threshold)
    ("A27a-pair-not-refused", "annex A27: a real run refuses on a labelling pair that differs by more than SLACK/2",
     '            if pair["problem"] and os.environ.get("V3L_REAL") == "1":\n',
     '            if False:\n', "UNRUN (QUIET)"),
    ("A27c-second-run-not-refused", "review 7 finding 3: a real run refuses an invalid second labelling run",
     '            if run_bad and os.environ.get("V3L_REAL") == "1":\n', '            if False:\n', "UNRUN (QUIET)"),
    ("A27d-second-run-not-gated", "review 7 finding 3: gates() judges the second labelling run of a record",
     '        bad += second_labelling_problems(rec["arms"]["fsync"])\n', '        pass\n', "UNRUN (QUIET)"),
    ("A27e-pair-not-rederived", "review 7 finding 4: a stored record's pair is re-derived from its counts",
     '    why = lab_pair_problem(lp.get("first"), lp.get("second"))\n', '    why = None\n', "UNRUN (QUIET)"),
    ("A27g-real-judged-as-dry", "review 7 finding 4: only an explicit dry record goes without a pair",
     '    if mode == "dry":\n        return []\n', '    if True:\n        return []\n', "UNRUN (QUIET)"),
    ("A24j-coverage-by-count", "review 7 finding 5: sync coverage is judged by path identity, in order",
     '        elif [x.get("path") if isinstance(x, dict) else None for x in lay] != exp:\n', '        elif False:\n',
     "UNRUN (QUIET)"),
    ("A27b-pair-threshold-slack", "annex A27: the pair's allowance is SLACK/2 of the larger, not SLACK",
     '    if Fraction(abs(first - second)) > SLACK / 2 * big:\n',
     '    if Fraction(abs(first - second)) > SLACK * big:\n', "UNRUN (QUIET)"),
    # review 6 MED 2 (annex A24-AM1): the per-filesystem sync -f, its rc, its coverage, and its plant
    ("A24e-layer-rc-not-judged", "A24-AM1: each filesystem's sync -f rc is judged",
     '                if not isinstance(x, dict) or type(x.get("rc")) is not int or x["rc"] != 0:\n',
     '                if not isinstance(x, dict):\n', "UNRUN (QUIET)"),
    ("A24f-delete-cover-rule", "A24-AM1: the sync reached every filesystem under DIR",
     '        bad += sync_cover_gate(rec["leaf"], rec["arms"]["fsync"].get("sync"))\n', '', "UNRUN (QUIET)"),
    ("A24g-no-sync-f", "A24-AM1: sync_record runs sync -f on each filesystem, not sync(1) alone",
     '    for p in paths:\n', '    for p in ():\n', "UNRUN (QUIET)"),
    ("A24h-no-unsynced-layer-plant", "A24-AM1: the per-filesystem rule is forced to fire on the real record (plant)",
     '        arm("unsynced-layer", rec["leaf"]["write_cache"],',
     '        (lambda *a: None)("unsynced-layer", rec["leaf"]["write_cache"],', "UNRUN (QUIET)"),
    ("M2a-no-iostats-rule", "review 5 MED 2: a leaf without iostats 1 VOIDs on its own text",
     '        if ip:\n            bad.append(f"drive {rec[\'leaf\'].get(\'disk\')}: {ip}")\n',
     '        if False:\n            pass\n', "UNRUN (QUIET)"),
    ("10e-no-wt-unwritten-plant", "item 10: the write rule is forced to fire on the real record (plant)",
     '        arm("wt-unwritten", "write through",', '        (lambda *a: None)("wt-unwritten", "write through",',
     "UNRUN (QUIET)"),
]


def table_problems(src, mutants):
    """Why the table cannot run against SRC (review 5 MED 5), as a list: a target not found exactly once; an indented
    (whole-line) target that does not begin at a line start, so it matches inside a deeper indentation and its edit
    would glue lines together; a mutated source that does not compile, which would be reported BROKEN, never KILLED.
    compile() parses, it runs nothing."""
    bad = []
    for name, _what, old, new, _last in mutants:
        n = src.count(old)
        if n != 1:
            bad.append(f"{name}: target found {n} times, not once")
            continue
        at = src.find(old)
        if old[:1] in (" ", "\t") and at > 0 and src[at - 1] != "\n":
            bad.append(f"{name}: an indented target that does not begin at a line start (it matches inside a deeper "
                       "indentation)")
            continue
        try:
            compile(src.replace(old, new), "v3l.py", "exec")
        except SyntaxError as e:
            bad.append(f"{name}: the mutated source does not compile ({e.msg}, line {e.lineno})")
    return bad


def selftest(path):
    r = subprocess.run(["timeout", "120", sys.executable, "-B", path, "self-test"], capture_output=True, text=True)
    lines = r.stdout.splitlines()
    fails = [ln for ln in lines if " FAIL: " in ln]
    total = next((ln for ln in reversed(lines) if ln.startswith("V3L SELF-TEST")), None)
    return r.returncode, fails, total, r.stderr


def main_case(kind):
    """main() on a temp copy of v3l.py (review 6 LOW 14, 15), its printed lines captured: "stale" gives it a row that
    breaks the syntax; "layout" gives it the real table's first row and a testdata path that does not exist, so the
    temp layout's control fails where the in-place one passes. True when main() returns 2 for that reason and leaves
    no v3lmut- directory behind (the layout case's control directory is removed with it)"""
    import contextlib
    import io
    import shutil
    d = tempfile.mkdtemp(prefix="v3lmut-case-")
    before = {x for x in os.listdir(tempfile.gettempdir()) if x.startswith("v3lmut-")}
    try:
        cp = os.path.join(d, "v3l.py")
        with open(SRC) as f, open(cp, "w") as g:
            g.write(f.read())
        os.symlink(os.path.join(HERE, "testdata"), os.path.join(d, "testdata"))
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            if kind == "stale":
                rc = main(["v3l_mutants.py"], src_path=cp,
                          table=[("broken", "synthetic", "def _plant(res, name):\n", "def _plant(res, name)\n", "")])
            else:
                rc = main(["v3l_mutants.py"], src_path=cp, table=MUTANTS[:1], testdata=os.path.join(d, "no-testdata"))
        out = buf.getvalue()
        after = {x for x in os.listdir(tempfile.gettempdir()) if x.startswith("v3lmut-")}
        want = "stale table" if kind == "stale" else "temp layout"
        return rc == 2 and want in out and after - before == set()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def self_test():
    """the table guard on synthetic sources (review 5 MED 5): a mutant that does not compile, an indented target that
    matches inside a deeper indentation, and a target found twice are each refused as a stale table; a clean row and
    the real table on the real v3l.py are not"""
    src = "def f(a):\n    if a:\n        y = 2\n    return a\n"
    row = lambda old, new: [("t", "synthetic", old, new, "")]
    cases = [
        ("a mutant whose source does not compile is a stale table",
         lambda: any("does not compile" in x for x in table_problems(src, row("        y = 2\n", "")))),
        ("an indented target matching inside a deeper indentation is a stale table",
         lambda: any("line start" in x for x in table_problems(src, row("    y = 2\n", "    pass\n")))),
        ("a target found twice is a stale table",
         lambda: any("2 times" in x for x in table_problems(src + src, row("    return a\n", "    return 0\n")))),
        ("a clean row is not refused", lambda: table_problems(src, row("        y = 2\n", "        pass\n")) == []),
        ("the real table on the real v3l.py is not refused", lambda: table_problems(open(SRC).read(), MUTANTS) == []),
        # review 6 LOW 14: main() itself refuses a stale table (rc 2), runs nothing and makes no v3lmut- directory
        ("LOW 14: main() on a table with a row that breaks the syntax returns 2, says stale table, makes no v3lmut- dir",
         lambda: main_case("stale")),
        # review 6 LOW 15: the unmutated copy runs in the mutants' temp layout first and must match the in-place
        # control, so a layout difference cannot kill every mutant for the wrong reason
        ("LOW 15: main() whose temp layout lacks testdata returns 2 before any mutant runs",
         lambda: main_case("layout")),
    ]
    bad = 0
    for name, f in cases:
        try:
            ok = bool(f())
        except Exception as e:  # noqa: BLE001
            ok = False
            print(f"V3L-MUTANTS self-test case raised {type(e).__name__}: {e}")
        bad += not ok
        print(f"V3L-MUTANTS self-test {'PASS' if ok else 'FAIL'}: {name}")
    print(f"V3L-MUTANTS SELF-TEST {len(cases) - bad}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


def main(argv, src_path=SRC, table=None, testdata=None):
    """SRC_PATH, TABLE and TESTDATA (default: the real v3l.py, MUTANTS, and the testdata/ beside SRC_PATH) are
    parameters so the self-test drives main() on a temp copy (review 6 LOW 14)"""
    if argv[1:] == ["self-test"]:
        return self_test()
    table = MUTANTS if table is None else table
    testdata = testdata or os.path.join(os.path.dirname(os.path.abspath(src_path)), "testdata")
    names = argv[1:]
    known = {m[0] for m in table}
    if [n for n in names if n not in known]:
        print(f"v3l_mutants: REFUSED: unknown mutant(s) {[n for n in names if n not in known]}")
        return 2
    src = open(src_path).read()
    todo = [m for m in table if not names or m[0] in names]
    stale = table_problems(src, todo)
    if stale:
        print(f"v3l_mutants: REFUSED: stale table: {stale}")
        return 2
    rc, fails, total, _ = selftest(src_path)
    if rc != 0 or fails or not total or not total.endswith(" PASS"):
        print(f"v3l_mutants: REFUSED: the unmutated self-test is not green (rc {rc}; {total}; {len(fails)} FAIL lines)")
        return 2
    print(f"control (unmutated): {total}")
    d = tempfile.mkdtemp(prefix="v3lmut-")

    def layout(name, text):
        p = os.path.join(d, name)
        os.makedirs(p)
        os.symlink(testdata, os.path.join(p, "testdata"))
        with open(os.path.join(p, "v3l.py"), "w") as f:
            f.write(text)
        return os.path.join(p, "v3l.py")

    # review 6 LOW 15: the unmutated copy in the temp layout, green with the in-place control's total, before any
    # mutant: otherwise a layout difference would kill every mutant for the wrong reason
    rc2, fails2, total2, err2 = selftest(layout("control-in-layout", src))
    if rc2 != 0 or fails2 or total2 != total:
        print(f"v3l_mutants: REFUSED: the unmutated copy in the temp layout is not the control (rc {rc2}; {total2} "
              f"vs {total}; {len(fails2)} FAIL lines; stderr {err2[-200:]!r})")
        shutil.rmtree(d, ignore_errors=True)
        return 2
    bad = 0
    for name, what, old, new, last in todo:
        p = os.path.dirname(layout(name, src.replace(old, new)))
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
