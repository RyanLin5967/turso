#!/usr/bin/env python3
"""fixed_api_mutants.py -- the old behaviour, planted at the FIXED API, for reds that failed at API setup (review 7 /
C21 finding 6). Several reds called a function or keyword the fix introduced, so at their red commit they failed
with rc 127 or a TypeError, which shows an API contract, never that the test sees the old bad behaviour. Each row
here plants that old behaviour in a temp copy of the fixed file, and is KILLED only when the NAMED case fails on it.
A missing function or keyword never counts.

  fixed_api_mutants.py [NAME...]   run every row (or the named ones); exit 0 iff each is KILLED, 1 if any SURVIVED (the
                                   test passed), MISSED (it failed, but not on the named case: a kill for the wrong
                                   reason) or BROKE (the mutated file does not parse), 2 if refused (an unknown name; a
                                   stale table: a target not found exactly once, an indented target not at a line
                                   start, or a mutated file that does not parse; or an unmutated control that is not
                                   green), so no row runs
  fixed_api_mutants.py self-test   the table guard on the real files, and a synthetic stale row refused

Each row: (name, the red it stands for and the old behaviour, directory under fastest/linux, file, test command,
target text, replacement, the case whose FAIL line must appear). The copy is the whole directory (testdata/ is
symlinked to the original), run from the copy. The last column is the last observed result. Every row is UNRUN
(written under QUIET, 2026-10-15); the first run of this file is that run. Run it on Linux: t3lib.sh uses coproc
(bash 4+; macOS's /bin/bash 3.2 does not parse it), and t3lib_test.sh's production-hook cases need sudo -n and
mkfs.ext4.
Owed with it, the fixed-API mutants that already exist in v3l_mutants.py for the other API-setup reds: A24g
(d036af4fc's sync_record(paths=...) argv case), A27a and A27b (576a75632's measure() cases, which at their red
refused at unregistered SLACK before either labelling run).
"""
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
LINUX = os.path.dirname(HERE)
PY = [sys.executable, "-B"]
T3LIB = ["bash", "t3lib_test.sh"]
ANALYZE = PY + ["analyze.py", "--self-test"]
MUTSELF = PY + ["v3l_mutants.py", "self-test"]

ROWS = [
    # 7ef91c178 (review 6 MED 1, LOW 4): t3lib's device branch; its red failed at rc 127 (no device_make)
    ("T1-devguard-after-reread", "7ef91c178 / review 6 LOW 4: the base's order, the drive re-read BEFORE devguard",
     "t3", "t3lib.sh", T3LIB,
     '  t3_devguard "$real" || { echo "REFUSED: devguard refuses $real before mkfs"; return 2; }\n'
     '  # after devguard, so whatever happened while it ran is seen before mkfs (review 6 LOW 4)\n'
     '  dev_unchanged "$path" "$real" "$sys" "$dev" "$id" || return 2\n',
     '  dev_unchanged "$path" "$real" "$sys" "$dev" "$id" || return 2\n'
     '  t3_devguard "$real" || { echo "REFUSED: devguard refuses $real before mkfs"; return 2; }\n',
     "LOW 4: the drive changes while devguard runs", "UNRUN (QUIET)"),
    ("T2-no-reread-before-mount", "7ef91c178 / review 6 MED 1: the re-read between mkfs and mount deleted",
     "t3", "t3lib.sh", T3LIB,
     '  fs_probe_is "$real" "$fs" "$uuid" || return 2\n  dev_unchanged "$path" "$real" "$sys" "$dev" "$id" || return 2\n',
     '  fs_probe_is "$real" "$fs" "$uuid" || return 2\n',
     "MED 1: the drive changes between mkfs and mount", "UNRUN (QUIET)"),
    ("T3-path-text-only", "7ef91c178 / review 6 MED 1: the base's dev_unmoved (path text) before mkfs",
     "t3", "t3lib.sh", T3LIB,
     '  # after devguard, so whatever happened while it ran is seen before mkfs (review 6 LOW 4)\n'
     '  dev_unchanged "$path" "$real" "$sys" "$dev" "$id" || return 2\n',
     '  # after devguard, so whatever happened while it ran is seen before mkfs (review 6 LOW 4)\n'
     '  dev_unmoved "$path" "$real" || return 2\n',
     "MED 1 (review 5's scenario): a plain node whose wwid changed", "UNRUN (QUIET)"),
    ("T4-wipe-unchecked", "7ef91c178 / review 6 MED 1: the cleanup's drive check replaced by true",
     "t3", "t3lib.sh", T3LIB,
     '  dev_unchanged "$1" "$2" "$3" "$4" "$5" || return 2\n', '  true || return 2\n',
     "MED 1: device_wipe when the drive changed refuses", "UNRUN (QUIET)"),
    ("T5-wipe-fresh-uuid", "7ef91c178 / review 6 MED 1: the wipe judged against a fresh probe's UUID",
     "t3", "t3lib.sh", T3LIB,
     '  fs_is_ours "$1" "$2" "$6" "$7" || return 2\n', '  fs_is_ours "$1" "$2" "$6" "$(t3_blkid UUID "$2")" || return 2\n',
     "MED 1: device_wipe when the probe finds another UUID refuses", "UNRUN (QUIET)"),
    # 14f4d366e (review 5 MED 10-12): analyze's artifact fallback; its red called gates(identity=, base_ok=) first
    ("A1-artifact-baseline-arm-only", "14f4d366e / review 5 MED 10: the artifact stands in for the baseline arm only",
     "profile", "analyze.py", ANALYZE,
     '        elif baseline is not None and c == 1 and (arm == BASELINE_ARM or arm in art_arms(baseline)):\n',
     '        elif baseline is not None and c == 1 and arm == BASELINE_ARM:\n',
     "MED 10: no in-job base, an artifact holding full-snap-c1 and full-cat-c1", "UNRUN (QUIET)"),
    ("A2-failed-base-kept", "14f4d366e / review 5 MED 11: a base whose run failed is not dropped",
     "profile", "analyze.py", ANALYZE, '    if not base_ok:\n', '    if False:\n',
     "MED 11: a base whose run failed is dropped", "UNRUN (QUIET)"),
    ("A3-identity-unchecked", "14f4d366e / review 5 MED 12: the artifact's identity keys never compared",
     "profile", "analyze.py", ANALYZE, '    for k in IDENTITY_KEYS:\n', '    for k in ():\n',
     "MED 12: an artifact whose driver_sha differs from the head's", "UNRUN (QUIET)"),
    # 3a1990f6a (review 6 LOW 14, 15): v3l_mutants main(); its red called main(src_path=, table=, testdata=) first
    ("V1-stale-table-falls-through", "3a1990f6a / review 6 LOW 14: main() goes on past a stale table",
     "t3", "v3l_mutants.py", MUTSELF,
     '        print(f"v3l_mutants: REFUSED: stale table: {stale}")\n        return 2\n',
     '        print(f"v3l_mutants: REFUSED: stale table: {stale}")\n',
     "LOW 14: main() on a table with a row that breaks the syntax", "UNRUN (QUIET)"),
    ("V2-no-layout-control", "3a1990f6a / review 6 LOW 15: the temp layout's unmutated control not judged",
     "t3", "v3l_mutants.py", MUTSELF, '    if rc2 != 0 or fails2 or total2 != total:\n', '    if False:\n',
     "LOW 15: main() whose temp layout lacks testdata returns 2", "UNRUN (QUIET)"),
]


def parses(path, text):
    """None when TEXT (the file at PATH, mutated) parses, else why. Parsing runs nothing."""
    if path.endswith(".py"):
        try:
            compile(text, path, "exec")
        except SyntaxError as e:
            return f"{e.msg}, line {e.lineno}"
        return None
    r = subprocess.run(["bash", "-n", "/dev/stdin"], input=text, capture_output=True, text=True, timeout=60)
    return None if r.returncode == 0 else r.stderr.strip()[:200]


def table_problems(rows, root=LINUX):
    """why the table cannot run against ROOT's files: a target not found exactly once, an indented target not at a
    line start, a mutated file that does not parse"""
    bad = []
    for name, _w, d, f, _cmd, old, new, _case, _last in rows:
        try:
            src = open(os.path.join(root, d, f)).read()
        except OSError as e:
            bad.append(f"{name}: {d}/{f} unreadable ({e})")
            continue
        n, at = src.count(old), src.find(old)
        if n != 1:
            bad.append(f"{name}: target found {n} times in {d}/{f}, not once")
        elif old[:1] in (" ", "\t") and at > 0 and src[at - 1] != "\n":
            bad.append(f"{name}: an indented target that does not begin at a line start")
        else:
            why = parses(f, src.replace(old, new))
            if why:
                bad.append(f"{name}: the mutated {f} does not parse ({why})")
    return bad


def copy_of(d, top):
    """a copy of fastest/linux/D under TOP, its testdata/ a symlink to the original"""
    dst = os.path.join(top, d)
    shutil.copytree(os.path.join(LINUX, d), dst, symlinks=True, ignore=shutil.ignore_patterns("testdata", "__pycache__"))
    if os.path.isdir(os.path.join(LINUX, d, "testdata")):
        os.symlink(os.path.join(LINUX, d, "testdata"), os.path.join(dst, "testdata"))
    return dst


def run_test(cwd, cmd, timeout=900):
    try:
        r = subprocess.run(["timeout", str(timeout)] + cmd, cwd=cwd, capture_output=True, text=True)
    except OSError as e:
        return None, str(e)
    return r.returncode, r.stdout + r.stderr


def main(argv):
    if argv[1:] == ["self-test"]:
        return self_test()
    names = argv[1:]
    known = {r[0] for r in ROWS}
    if [n for n in names if n not in known]:
        print(f"fixed_api_mutants: REFUSED: unknown row(s) {[n for n in names if n not in known]}")
        return 2
    todo = [r for r in ROWS if not names or r[0] in names]
    stale = table_problems(todo)
    if stale:
        print(f"fixed_api_mutants: REFUSED: stale table: {stale}")
        return 2
    top = tempfile.mkdtemp(prefix="fixed-api-mut-")
    # one control per (dir, command): two commands over one dir each get their own copy (the first run, 2026-10-15,
    # crashed copying t3/ a second time into control-t3)
    for k, (d, cmd) in enumerate(sorted({(r[2], tuple(r[4])) for r in todo})):
        rc, out = run_test(copy_of(d, os.path.join(top, f"control-{k}-" + d.replace("/", "_"))), list(cmd))
        if rc != 0:
            print(f"fixed_api_mutants: REFUSED: the unmutated {d} control ({' '.join(cmd)}) is not green (rc {rc}): "
                  f"{out[-300:]!r}")
            return 2
        print(f"control {d} ({' '.join(cmd)}): rc 0")
    bad = 0
    for name, what, d, f, cmd, old, new, case, last in todo:
        cp = copy_of(d, os.path.join(top, name))
        p = os.path.join(cp, f)
        src = open(p).read()
        with open(p, "w") as fh:
            fh.write(src.replace(old, new))
        rc, out = run_test(cp, cmd)
        named = any(" FAIL: " in ln and case in ln for ln in out.splitlines())
        verdict = ("KILLED" if rc not in (0, None) and named else "SURVIVED" if rc == 0
                   else "MISSED" if rc is not None else "BROKEN")
        bad += verdict != "KILLED"
        print(f"MUTANT {name} ({what}): {verdict} (rc {rc}; must fail: {case!r}; last recorded: {last})")
        if verdict != "KILLED":
            print("    " + "\n    ".join([ln for ln in out.splitlines() if " FAIL" in ln][:6] or [out[-300:]]))
    print(f"fixed_api_mutants: {len(todo) - bad}/{len(todo)} KILLED ({top})")
    return 0 if not bad else 1


def self_test():
    """the table guard on the real files, and a synthetic row whose target is absent refused as stale"""
    cases = [("the real table on the real files is not refused", table_problems(ROWS) == []),
             ("a row whose target is absent is a stale table",
              any("found 0 times" in x for x in table_problems(
                  [("t", "synthetic", "t3", "t3lib.sh", T3LIB, "no such line\n", "", "x", "")])))]
    bad = 0
    for n, ok in cases:
        bad += not ok
        print(f"FIXED-API-MUTANTS self-test {'PASS' if ok else 'FAIL'}: {n}")
    print(f"FIXED-API-MUTANTS SELF-TEST {len(cases) - bad}/{len(cases)} {'PASS' if not bad else 'FAIL'}")
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
