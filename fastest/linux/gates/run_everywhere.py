#!/usr/bin/env python3
"""run_everywhere.py -- run every self-test entry point under fastest/linux on THIS platform, as gates/platforms.tsv
declares it (lead order 2026-10-15, Ryan's class rule: no gate script or test silently inherits the host platform or
layout). The same command runs on the Mac and on Linux; vm_dryblock.sh runs it on the Mac before a VM dry run, and
t3run.sh's selftests stage runs it on Linux.

  run_everywhere.py [ROOT]     ROOT is fastest/linux (default: this file's ../). Exit 0 iff every row for this
                               platform passed, no entry point lacks a row, and no row names a missing file.
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


def run(root, plat=None, timeout=1800):
    """-> (rc, lines). Runs each row for plat (default: this platform) from its file's directory."""
    plat = plat or this_platform()
    rows, bad = table(root)
    listed = {r[0] for r in rows}
    lines = list(bad)
    fails = list(bad)
    for p in discover(root):
        if p not in listed:
            fails.append(f"{p}: a self-test entry point with no row in gates/platforms.tsv (state its platform)")
    na = passed = 0
    for path, args, pf, why in rows:
        full = os.path.join(root, path)
        if pf not in PLATFORMS:
            fails.append(f"{path}: platform {pf!r} is neither any nor linux")
            continue
        if not os.path.isfile(full):
            fails.append(f"{path}: listed in platforms.tsv but absent")
            continue
        if pf == "linux" and plat != "linux":
            na += 1
            lines.append(f"N/A-on-{plat}: {path} ({why})")
            continue
        cmd = ([sys.executable, "-B", os.path.basename(full)] if path.endswith(".py") else ["bash", os.path.basename(full)])
        cmd += args.split()
        try:
            r = subprocess.run(cmd, cwd=os.path.dirname(full), capture_output=True, text=True, timeout=timeout)
            rc, out = r.returncode, r.stdout + r.stderr
        except subprocess.TimeoutExpired:
            rc, out = None, "timed out"
        last = [ln for ln in out.splitlines() if ln.strip()][-1:] or [""]
        inner_na = sum(1 for ln in out.splitlines() if "N/A-on-" in ln)
        lines.append(f"{'PASS' if rc == 0 else 'FAIL'} {path} {args} rc={rc} | {last[0][:160]}"
                     + (f" ({inner_na} case(s) N/A-on-{plat})" if inner_na else ""))
        if rc == 0:
            passed += 1
        else:
            fails.append(f"{path}: rc {rc}: {last[0][:200]}")
    lines += [f"REFUSED: {x}" for x in fails if x not in bad]
    lines.append(f"run_everywhere ({plat}): {passed} passed, {len(fails)} failed or refused, {na} N/A-on-{plat}")
    return (0 if not fails and passed > 0 else 1), lines


def self_test():
    import shutil
    import tempfile
    cases = []
    t = tempfile.mkdtemp()
    try:
        os.makedirs(os.path.join(t, "gates"))
        os.makedirs(os.path.join(t, "a"))
        ok_py = 'import sys\nif sys.argv[1:] == ["self-test"]:\n    print("OK 1/1")\n    sys.exit(0)\n'
        bad_py = 'import sys\nif sys.argv[1:] == ["self-test"]:\n    print("BAD 0/1")\n    sys.exit(1)\n'
        open(os.path.join(t, "a", "good.py"), "w").write(ok_py)
        open(os.path.join(t, "a", "bad.py"), "w").write(bad_py)
        open(os.path.join(t, "a", "lin_test.sh"), "w").write("exit 1\n")

        def tsv(*rows):
            open(os.path.join(t, "gates", "platforms.tsv"), "w").write("# x\n" + "".join("\t".join(r) + "\n" for r in rows))
        tsv(("a/good.py", "self-test", "any", "x"), ("a/bad.py", "self-test", "linux", "x"), ("a/lin_test.sh", "", "linux", "x"))
        rc, lines = run(t, "darwin")
        cases.append(("a linux row off Linux is N/A, never run, and the any row passes: rc 0",
                      rc == 0 and any(ln.startswith("N/A-on-darwin: a/bad.py") for ln in lines)))
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
        # 3.2: t3lib.sh's coproc, fixed_api_mutants' bash -n). A planted `bash` first on PATH that is not bash 4+ must
        # not be the one a row runs under, nor the one its children run under
        fake = os.path.join(t, "fakebin")
        os.makedirs(fake)
        open(os.path.join(fake, "bash"), "w").write("#!/bin/sh\necho FAKE-BASH; exit 97\n")
        os.chmod(os.path.join(fake, "bash"), 0o755)
        open(os.path.join(t, "a", "v4_test.sh"), "w").write(
            '[ "${BASH_VERSINFO[0]}" -ge 4 ] || exit 1\nbash -c \'[ "${BASH_VERSINFO[0]}" -ge 4 ]\' || exit 2\necho V4 ok\n')
        tsv(("a/v4_test.sh", "", "any", "x"), ("a/bad.py", "self-test", "linux", "x"), ("a/lin_test.sh", "", "linux", "x"))
        old_path = os.environ.get("PATH", "")
        os.environ["PATH"] = fake + os.pathsep + old_path
        try:
            rc, lines = run(t, "darwin")
        finally:
            os.environ["PATH"] = old_path
        cases.append(("a non-bash-4 `bash` first on the caller's PATH runs neither a row nor its children: rc 0",
                      rc == 0 and any(ln.startswith("PASS a/v4_test.sh") for ln in lines)))
        os.remove(os.path.join(t, "a", "v4_test.sh"))
        rows, _ = table(os.path.dirname(HERE))
        cases.append(("the real table: every discovered entry point has a row",
                      set(discover(os.path.dirname(HERE))) <= {r[0] for r in rows}))
    finally:
        shutil.rmtree(t, ignore_errors=True)
    bad = [n for n, ok in cases if not ok]
    for n, ok in cases:
        print(f"RUN-EVERYWHERE self-test {'PASS' if ok else 'FAIL'}: {n}")
    print(f"RUN-EVERYWHERE SELF-TEST {len(cases) - len(bad)}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


if __name__ == "__main__":
    if sys.argv[1:] == ["self-test"]:
        sys.exit(self_test())
    root = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(HERE)
    rc, lines = run(os.path.abspath(root))
    print("\n".join(lines))
    sys.exit(rc)
