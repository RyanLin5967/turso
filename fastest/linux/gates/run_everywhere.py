#!/usr/bin/env python3
"""run_everywhere.py -- run every self-test entry point under fastest/linux on THIS platform, as gates/platforms.tsv
declares it (lead order 2026-10-15, Ryan's class rule: no gate script or test silently inherits the host platform or
layout). The same command runs on the Mac and on Linux; vm_dryblock.sh runs it on the Mac before a VM dry run, and
t3run.sh's selftests stage runs it on Linux.

  run_everywhere.py [ROOT] [--allow-na CAUSE,...]
                               ROOT is fastest/linux (default: this file's ../). Exit 0 iff every row for this
                               platform ran and passed, no entry point lacks a row, and no row names a missing file;
                               3 when nothing failed but a case or row was N/A (counted by cause in the totals; never
                               a pass); 1 otherwise. A row that skips a case exits 3 with an N/A-on-<cause> line.
  run_everywhere.py self-test  the table rules on planted trees

Refused (exit 1, each named): an entry point with no row (discovered: a .py whose text compares an argument with
"self-test", "selftest" or "--self-test"; a *_test.sh; a .sh with a --self-test case); a row whose file is absent; a
row whose platform is neither any nor linux; a row that fails. A linux row off Linux is N/A-on-<platform>, printed
and never counted as a pass; so is a case a test itself reports N/A. Blind spot, stated: the discovery reads text, so
an entry point spelled another way is found by nothing but a reviewer.
"""
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import hostbin  # noqa: E402  the one bash every row and its children run under (the platform class)
import tally  # noqa: E402  the one outcome record every row prints (linux-rev-uw2 MED 1)
PLATFORMS = ("any", "linux")
PY_MARK = re.compile(r'''(==|in|\[)\s*\(?\s*\[?\s*["'](--)?self-?test["']''')
SH_MARK = re.compile(r'''--self-test\)|= *"?--self-test"?''')


def this_platform():
    return "linux" if sys.platform.startswith("linux") else sys.platform


def table(root):
    """[(path, args, platform, why)] from root/gates/platforms.tsv, and the rows that do not parse"""
    rows, bad = [], []
    for n, ln in enumerate(open(os.path.join(root, "gates", "platforms.tsv")).read().splitlines(), 1):
        if not ln.strip() or ln.startswith("#"):
            continue
        f = ln.split("\t")
        if len(f) != 4 or not f[0]:
            bad.append(f"platforms.tsv:{n}: not path<TAB>args<TAB>platform<TAB>why: {ln!r}")
            continue
        rows.append(tuple(f))
    return rows, bad


def discover(root):
    """every self-test entry point under root, as paths relative to it"""
    out = []
    for d, dirs, files in os.walk(root):
        dirs[:] = [x for x in dirs if x not in ("testdata", "__pycache__") and not x.startswith(".")]
        for fn in files:
            p = os.path.join(d, fn)
            rel = os.path.relpath(p, root)
            if fn.endswith(".py"):
                try:
                    txt = open(p, errors="replace").read()
                except OSError:
                    continue
                if PY_MARK.search(txt) and fn != "run_everywhere.py":
                    out.append(rel)
            elif fn.endswith("_test.sh"):
                out.append(rel)
            elif fn.endswith(".sh"):
                try:
                    if SH_MARK.search(open(p, errors="replace").read()):
                        out.append(rel)
                except OSError:
                    pass
    return sorted(out)


def run(root, plat=None, timeout=1800, allow_na=None):
    """-> (rc, lines). Runs each row for plat (default: this platform) from its file's directory. ALLOW_NA (a set of
    causes) limits which N/A causes may give rc 3; any other N/A is a failure (linux-rev-uw2 LOW 5)."""
    plat = plat or this_platform()
    rows, bad = table(root)
    listed = {r[0] for r in rows}
    lines = list(bad)
    fails = list(bad)
    for p in discover(root):
        if p not in listed:
            fails.append(f"{p}: a self-test entry point with no row in gates/platforms.tsv (state its platform)")
    passed = 0
    causes = {}  # N/A cause -> case count (a whole linux row off Linux counts once, under the platform's name)
    try:
        env = hostbin.env_with_bash()
        lines.append(f"bash: {hostbin.bash()} (hostbin; first on every row's PATH)")
    except hostbin.BashMissing as e:
        env = None
        fails.append(f"REFUSED before any row: {e}")
        rows = []
    for path, args, pf, why in rows:
        full = os.path.join(root, path)
        if pf not in PLATFORMS:
            fails.append(f"{path}: platform {pf!r} is neither any nor linux")
            continue
        if not os.path.isfile(full):
            fails.append(f"{path}: listed in platforms.tsv but absent")
            continue
        if pf == "linux" and plat != "linux":
            if allow_na is not None and plat not in allow_na:
                fails.append(f"{path}: N/A-on-{plat}, a cause not allowed here (--allow-na {','.join(allow_na)})")
                continue
            causes[plat] = causes.get(plat, 0) + 1
            lines.append(f"N/A-on-{plat}: {path} ({why})")
            continue
        cmd = ([sys.executable, "-B", os.path.basename(full)] if path.endswith(".py")
               else [hostbin.bash(env["PATH"]), os.path.basename(full)])
        cmd += args.split()
        try:
            r = subprocess.run(cmd, cwd=os.path.dirname(full), capture_output=True, text=True, timeout=timeout, env=env)
            rc, out = r.returncode, r.stdout + r.stderr
        except subprocess.TimeoutExpired:
            rc, out = None, "timed out"
        last = [ln for ln in out.splitlines() if ln.strip()][-1:] or [""]
        # the row's outcome is its TALLY line, agreeing with its rc (linux-rev-uw2 MED 1: an allowlist of outcomes, not
        # a wording match); every N/A case also names its cause in an "N/A-on-<cause>" line (MED 2's totals)
        outcome, t_, probs = tally.judge(out, rc if rc is not None else -1)
        row_na = re.findall(r"N/A-on-([A-Za-z0-9_-]+)", out)
        if rc == 0 and row_na:
            probs = probs + [f"printed {len(row_na)} N/A line(s) but exited 0: an N/A must exit 3"]
            outcome = "fail"
        if outcome == "na" and len(row_na) != t_["na"]:
            probs.append(f"TALLY na={t_['na']}, but {len(row_na)} N/A-on-<cause> line(s) name a cause")
            outcome = "fail"
        if outcome == "na" and allow_na is not None and set(row_na) - set(allow_na):
            probs.append(f"N/A cause(s) {sorted(set(row_na) - set(allow_na))} not allowed here (--allow-na {','.join(allow_na)})")
            outcome = "fail"
        if outcome == "pass":
            passed += 1
            lines.append(f"PASS {path} {args} rc=0 | {last[0][:160]}")
        elif outcome == "na":
            for c in row_na:
                causes[c] = causes.get(c, 0) + 1
            lines.append(f"N/A {path} {args} rc=3 | {len(row_na)} case(s) N/A: "
                         + ", ".join(f"{c} {row_na.count(c)}" for c in sorted(set(row_na))))
        else:
            why = "; ".join(probs) or (last[0][:200])
            fails.append(f"{path}: rc {rc}: {why}")
            lines.append(f"FAIL {path} {args} rc={rc} | {why[:200]}")
    lines += [f"REFUSED: {x}" for x in fails if x not in bad]
    n_na = sum(causes.values())
    lines.append(f"run_everywhere ({plat}): {passed} passed, {n_na} N/A"
                 + (" (" + ", ".join(f"{c} {causes[c]}" for c in sorted(causes)) + ")" if n_na else "")
                 + f", {len(fails)} failed or refused")
    # 0: every row ran and passed. 3: no failure, but at least one case N/A (never a pass: t3run's selftests stage
    # takes 0 only). 1: a failure or refusal, or nothing passed.
    return (1 if fails or passed == 0 else 3 if n_na else 0), lines


def self_test():
    import shutil
    import tempfile
    cases = []
    t = tempfile.mkdtemp()
    try:
        os.makedirs(os.path.join(t, "gates"))
        os.makedirs(os.path.join(t, "a"))
        # TEST EDIT, flagged (linux-rev-uw2 MED 1, the TALLY allowlist): the planted rows print the fixed tally line every
        # row now must, at the values their own outcome implies; what each case checks is unchanged
        ok_py = 'import sys\nif sys.argv[1:] == ["self-test"]:\n    print("OK 1/1")\n    print("TALLY ran=1 pass=1 na=0 fail=0")\n    sys.exit(0)\n'
        bad_py = 'import sys\nif sys.argv[1:] == ["self-test"]:\n    print("BAD 0/1")\n    print("TALLY ran=1 pass=0 na=0 fail=1")\n    sys.exit(1)\n'
        open(os.path.join(t, "a", "good.py"), "w").write(ok_py)
        open(os.path.join(t, "a", "bad.py"), "w").write(bad_py)
        open(os.path.join(t, "a", "lin_test.sh"), "w").write("echo 'TALLY ran=1 pass=0 na=0 fail=1'\nexit 1\n")

        def tsv(*rows):
            open(os.path.join(t, "gates", "platforms.tsv"), "w").write("# x\n" + "".join("\t".join(r) + "\n" for r in rows))
        tsv(("a/good.py", "self-test", "any", "x"), ("a/bad.py", "self-test", "linux", "x"), ("a/lin_test.sh", "", "linux", "x"))
        rc, lines = run(t, "darwin")
        # TEST EDIT, flagged (linux-rev-uw1 HIGH 1, the lead's standing rule: an N/A never counts toward a T3 gate
        # item): this case wanted rc 0 with a row N/A, the very outcome the rule forbids; an N/A run exits 3
        cases.append(("a linux row off Linux is N/A, never run, and the any row passes: rc 3 (N/A), not 0",
                      rc == 3 and any(ln.startswith("N/A-on-darwin: a/bad.py") for ln in lines)))
        rc, lines = run(t, "linux")
        cases.append(("the same table on Linux runs the linux rows, and a failing one fails (rc 1, naming it)",
                      rc == 1 and any("a/bad.py: rc 1" in ln for ln in lines)))
        tsv(("a/good.py", "self-test", "any", "x"), ("a/lin_test.sh", "", "linux", "x"))
        rc, lines = run(t, "darwin")
        cases.append(("an entry point with no row is refused, naming it",
                      rc == 1 and any("a/bad.py: a self-test entry point with no row" in ln for ln in lines)))
        tsv(("a/good.py", "self-test", "any", "x"), ("a/bad.py", "self-test", "mac", "x"), ("a/lin_test.sh", "", "linux", "x"))
        rc, lines = run(t, "darwin")
        cases.append(("a platform other than any or linux is refused", rc == 1 and any("neither any nor linux" in ln for ln in lines)))
        tsv(("a/good.py", "self-test", "any", "x"), ("a/bad.py", "self-test", "linux", "x"), ("a/lin_test.sh", "", "linux", "x"),
            ("a/gone.py", "self-test", "any", "x"))
        rc, lines = run(t, "darwin")
        cases.append(("a row whose file is absent is refused", rc == 1 and any("a/gone.py: listed" in ln for ln in lines)))
        tsv(("a/bad.py", "self-test", "linux", "x"), ("a/lin_test.sh", "", "linux", "x"))
        os.remove(os.path.join(t, "a", "good.py"))
        rc, lines = run(t, "darwin")
        cases.append(("nothing run (every row N/A) is not a pass: rc 1", rc == 1))
        # CLASS (the platform class, instance found 2026-10-15): a row's interpreter came from the CALLER's PATH, so the
        # same tree passed under vm_dryblock.sh (homebrew first on PATH) and failed run by hand on the Mac (/bin/bash
        # 3.2: t3lib.sh's coproc, fixed_api_mutants' bash -n). A planted `bash` first on PATH that is not bash 5+ (BASH_MIN) must
        # not be the one a row runs under, nor the one its children run under
        fake = os.path.join(t, "fakebin")
        os.makedirs(fake)
        open(os.path.join(fake, "bash"), "w").write("#!/bin/sh\necho FAKE-BASH; exit 97\n")
        os.chmod(os.path.join(fake, "bash"), 0o755)
        open(os.path.join(t, "a", "v4_test.sh"), "w").write(
            '[ "${BASH_VERSINFO[0]}" -ge 5 ] || exit 1\nbash -c \'[ "${BASH_VERSINFO[0]}" -ge 5 ]\' || exit 2\necho V5 ok\n'
            "echo 'TALLY ran=1 pass=1 na=0 fail=0'\n")
        tsv(("a/v4_test.sh", "", "any", "x"), ("a/bad.py", "self-test", "linux", "x"), ("a/lin_test.sh", "", "linux", "x"))
        old_path = os.environ.get("PATH", "")
        os.environ["PATH"] = fake + os.pathsep + old_path
        try:
            rc, lines = run(t, "darwin")
        finally:
            os.environ["PATH"] = old_path
        # TEST EDIT, flagged (the same ruling): the table's two linux rows are N/A off Linux, so the run exits 3, not 0;
        # what this case checks, the row run under bash 5 with its children, is unchanged
        cases.append(("a `bash` first on the caller's PATH that is not bash 5+ runs neither a row nor its children: "
                      "the row passes (the run exits 3 for its two N/A rows)",
                      rc == 3 and any(ln.startswith("PASS a/v4_test.sh") for ln in lines)))
        os.remove(os.path.join(t, "a", "v4_test.sh"))
        # linux-rev-uw1 HIGH 1 / MED 2: an N/A is its own outcome, carried end to end: a row that skips a case exits 3,
        # the run counts it apart from the passes with its cause, and exits 3, which t3run's selftests stage
        # (rc 0 only) refuses; a row that prints an N/A line but exits 0 is refused outright
        open(os.path.join(t, "a", "na3.py"), "w").write(
            'import sys\nif sys.argv[1:] == ["self-test"]:\n    print("N/A-on-busy-host: case x")\n'
            '    print("TALLY ran=0 pass=0 na=1 fail=0")\n    sys.exit(3)\n')
        open(os.path.join(t, "a", "na0.py"), "w").write(
            'import sys\nif sys.argv[1:] == ["self-test"]:\n    print("N/A-on-noroot: case y")\n    sys.exit(0)\n')
        open(os.path.join(t, "a", "good.py"), "w").write(ok_py)
        tsv(("a/good.py", "self-test", "any", "x"), ("a/na3.py", "self-test", "any", "x"),
            ("a/bad.py", "self-test", "linux", "x"), ("a/lin_test.sh", "", "linux", "x"))
        os.remove(os.path.join(t, "a", "na0.py"))
        rc, lines = run(t, "darwin")
        cases.append(("HIGH 1: a row that skips a case (exit 3) makes the run exit 3, not 0",
                      rc == 3 and any(ln.startswith("N/A a/na3.py") for ln in lines)))
        cases.append(("MED 2: the totals count N/A apart from the passes, by cause",
                      lines[-1] == "run_everywhere (darwin): 1 passed, 3 N/A (busy-host 1, darwin 2), 0 failed or refused"))
        open(os.path.join(t, "a", "na0.py"), "w").write(
            'import sys\nif sys.argv[1:] == ["self-test"]:\n    print("N/A-on-noroot: case y")\n    sys.exit(0)\n')
        tsv(("a/good.py", "self-test", "any", "x"), ("a/na0.py", "self-test", "any", "x"), ("a/na3.py", "self-test", "any", "x"),
            ("a/bad.py", "self-test", "linux", "x"), ("a/lin_test.sh", "", "linux", "x"))
        rc, lines = run(t, "linux")
        cases.append(("HIGH 1: a row that prints an N/A line but exits 0 is refused, naming the rule",
                      rc == 1 and any("a/na0.py" in ln and "an N/A must exit 3" in ln for ln in lines)))
        for f_ in ("na0.py", "na3.py"):
            os.remove(os.path.join(t, "a", f_))
        # linux-rev-uw2 MED 1: an outcome ALLOWLIST, not a wording match. Every row prints one fixed tally line, and a
        # row with no tally, or a tally that disagrees with its rc, is REFUSED; so is the reviewer's respelled skip
        # ("SKIPPED (no root)", exit 0, no tally). Run through the CLI, so this red fails at its claim, not at setup
        def cli(root, *extra):
            r = subprocess.run([sys.executable, "-B", os.path.abspath(__file__), root, *extra], capture_output=True,
                               text=True, timeout=600)
            return r.returncode, r.stdout
        def rows(*specs):
            for f_ in os.listdir(os.path.join(t, "a")):
                os.remove(os.path.join(t, "a", f_))
            for name, body in specs:
                open(os.path.join(t, "a", name), "w").write(
                    'import sys\nif sys.argv[1:] == ["self-test"]:\n' + "".join(f"    {ln}\n" for ln in body))
            tsv(*[(f"a/{name}", "self-test", "any", "x") for name, _ in specs])
        good = ('print("TALLY ran=2 pass=2 na=0 fail=0")', "sys.exit(0)")
        rows(("good.py", good), ("respelled.py", ('print("SKIPPED (no root): case y")', "sys.exit(0)")))
        rc, out = cli(t)
        cases.append(("uw2 MED 1: the reviewer's respelled skip ('SKIPPED (no root)', exit 0, no tally) is REFUSED",
                      rc == 1 and "respelled.py" in out and "no TALLY" in out))
        rows(("good.py", good), ("lies.py", ('print("TALLY ran=1 pass=1 na=1 fail=0")', "sys.exit(0)")))
        rc, out = cli(t)
        cases.append(("uw2 MED 1: a tally with na=1 under exit 0 is REFUSED (the tally and the rc disagree)",
                      rc == 1 and "lies.py" in out and "disagree" in out))
        rows(("good.py", good), ("failna.py", ('print("FAIL: case z")', 'print("TALLY ran=1 pass=0 na=1 fail=1")', "sys.exit(3)")))
        rc, out = cli(t)
        cases.append(("uw2 LOW 5: a row that failed a case is never classed N/A, whatever its rc",
                      rc == 1 and "failna.py" in out and "N/A failna.py" not in out))
        rows(("good.py", good), ("na3.py", ('print("N/A-on-busy-host: case w")', 'print("TALLY ran=0 pass=0 na=1 fail=0")', "sys.exit(3)")))
        rc, out = cli(t, "--allow-na", "darwin")
        cases.append(("uw2 LOW 5: with --allow-na darwin, a busy-host N/A is refused (rc 1); only the allowed cause is rc 3",
                      rc == 1 and "busy-host" in out))
        rc, out = cli(t)
        cases.append(("uw2 MED 1: an honest tally (na=1, exit 3) is the N/A outcome, rc 3", rc == 3 and "N/A a/na3.py" in out))
        for f_ in os.listdir(os.path.join(t, "a")):
            os.remove(os.path.join(t, "a", f_))
        open(os.path.join(t, "a", "good.py"), "w").write(ok_py)
        open(os.path.join(t, "a", "bad.py"), "w").write(bad_py)
        open(os.path.join(t, "a", "lin_test.sh"), "w").write("echo 'TALLY ran=1 pass=0 na=0 fail=1'\nexit 1\n")
        # LOW 5: BASH_MIN fired: a bash reporting major 4 is never chosen, a fake one and, where the host has one,
        # a real one (macOS /bin/bash 3.2, RHEL 8 /bin/bash 4.4)
        f4 = os.path.join(t, "bash4")
        os.makedirs(f4)
        open(os.path.join(f4, "bash"), "w").write('#!/bin/sh\necho 4\n')
        os.chmod(os.path.join(f4, "bash"), 0o755)
        try:
            hostbin.bash(f4, fallbacks=())
            cases.append(("LOW 5: a bash reporting major 4 alone on PATH is refused (BashMissing)", False))
        except hostbin.BashMissing:
            cases.append(("LOW 5: a bash reporting major 4 alone on PATH is refused (BashMissing)", True))
        old = [b for b in ("/bin/bash", "/usr/bin/bash") if os.access(b, os.X_OK) and (hostbin._major(b) or 99) < 5]
        if old:
            rb = os.path.join(t, "realold")
            os.makedirs(rb)
            os.symlink(old[0], os.path.join(rb, "bash"))
            try:
                hostbin.bash(rb, fallbacks=())
                cases.append((f"LOW 5: the host's real {old[0]} (major {hostbin._major(old[0])}) alone is refused", False))
            except hostbin.BashMissing:
                cases.append((f"LOW 5: the host's real {old[0]} (major {hostbin._major(old[0])}) alone is refused", True))
        else:
            print("INFO: this host has no bash older than 5; the fake-4 case above fires BASH_MIN", flush=True)
        rows, _ = table(os.path.dirname(HERE))
        cases.append(("the real table: every discovered entry point has a row",
                      set(discover(os.path.dirname(HERE))) <= {r[0] for r in rows}))
    finally:
        shutil.rmtree(t, ignore_errors=True)
    bad = [n for n, ok in cases if not ok]
    for n, ok in cases:
        print(f"RUN-EVERYWHERE self-test {'PASS' if ok else 'FAIL'}: {n}")
    print(f"TALLY ran={len(cases)} pass={len(cases) - len(bad)} na=0 fail={len(bad)}")  # gates/tally.py
    print(f"RUN-EVERYWHERE SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


if __name__ == "__main__":
    if sys.argv[1:] == ["self-test"]:
        sys.exit(self_test())
    av = sys.argv[1:]
    allow = None
    if "--allow-na" in av:
        i = av.index("--allow-na")
        allow = set(av[i + 1].split(",")) if i + 1 < len(av) else set()
        av = av[:i] + av[i + 2:]
    root = av[0] if av else os.path.dirname(HERE)
    rc, lines = run(os.path.abspath(root), allow_na=allow)
    print("\n".join(lines))
    sys.exit(rc)
