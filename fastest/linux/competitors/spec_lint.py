#!/usr/bin/env python3
"""spec_lint.py [--dir DIR] | selftest -- the Linux specs' timed steps against the registered statements (lane
fastest-linux-comp; comp review 1 MED 6).

REG below transcribes the PREREG's §6 Statements table (artie frontier/fastest/tools/loadgen/specs/STATEMENTS.md,
printed by the macOS lint_specs.py from its REG data) for the forms each Linux spec runs: the TIMED steps, in order,
as bbload spec lines. The Linux specs keep their own home connect lines (the SSL/GSS forcing is bbload's, Codex card
C02) and after-steps (the delete and its serialisation are the lead's re-ruling, Codex card C04), so only the timed
steps, the protocol and the branch var are compared here. Every *.spec in the directory must be either in REG or in
UNREGISTERED (plants and helpers that are never a credited cell): a new spec cannot escape the check.

  m1c-create   [create]                     (Dolt/Doltgres (a): the create also switches, so m1c-create and m1c-switch
  m1c-switch   [create, switch]              are one statement and one spec file)
  m1c-connect  [create, connect, probe]
  m1-switch    [create, switch, write]
  m1-connect   [create, connect, probe, write]
  floor        [probe]

Exit 0 clean, 1 a spec is wrong (each problem printed), 2 usage. selftest plants faults in a temp copy of the real
specs (the timed no-op checkout of variant (a) that comp review 1 MED 6 found, an M1 without its probe, a reordered
step, an unknown spec file, a setup line) and requires each to be refused and the real set to pass.
"""
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SPECS = os.path.join(HERE, "loadgen", "specs")
BRANCH = "var branch = b_{run}_{c}_{i}"
WRITE = "step write UPDATE t SET v = v + 1 WHERE id = {rand:1:rows}"
PROBE = "step sql SELECT 1"


def _pg(strategy):
    create = f"step sql CREATE DATABASE {{branch}} TEMPLATE p STRATEGY = {strategy}"
    connect = "step connect dbname={branch}"
    return [create], [create, connect, PROBE], [create, connect, PROBE, WRITE]


def _dolt(a, b, sw, con):
    """Variant (a)'s create (it also switches), (b)/(c)'s create, (b)'s switch and (c)'s connect, per spec file name."""
    return {"a-m1c": [a], "a-m1": [a, WRITE], "create": [b], "b-m1c": [b, sw], "b-m1": [b, sw, WRITE],
            "c-m1c": [b, con, PROBE], "c-m1": [b, con, PROBE, WRITE]}


FC, FCC, FCM = _pg("FILE_COPY")
WC, WCC, WCM = _pg("WAL_LOG")
# spec file -> (protocol, branch var?, timed steps, setup lines allowed)
REG = {
    "pg18-create.spec": ("pg", True, FC, []),          # filecopy m1c-create
    "pg18-m1c.spec": ("pg", True, FCC, []),            # filecopy m1c-connect
    "pg18-m1.spec": ("pg", True, FCM, []),             # filecopy m1-connect
    "pg18-create-wal.spec": ("pg", True, WC, []),      # wallog m1c-create
    "pg18-m1c-wal.spec": ("pg", True, WCC, []),        # wallog m1c-connect
    "pg18-m1-wal.spec": ("pg", True, WCM, []),         # wallog m1-connect
    # the clone-proof control: filecopy m1c-create with the session's file_copy_method set to copy (Linux only)
    "pg18-create-copy.spec": ("pg", True, FC, ["setup SET file_copy_method = copy"]),
    "pg18-select1.spec": ("pg", False, [PROBE], []),   # floor
    "dolt-select1.spec": ("mysql", False, [PROBE], []),
    "doltgres-select1.spec": ("pg", False, [PROBE], []),
}
for _sys, _proto, _steps4 in (
        ("dolt", "mysql", ("step sql CALL DOLT_CHECKOUT('-b', '{branch}')", "step sql CALL DOLT_BRANCH('{branch}', 'main')",
                           "step sql CALL DOLT_CHECKOUT('{branch}')", "step connect db=bench/{branch}")),
        ("doltgres", "pg", ("step sql SELECT dolt_checkout('-b', '{branch}')", "step sql SELECT dolt_branch('{branch}', 'main')",
                            "step sql SELECT dolt_checkout('{branch}')", "step connect dbname=postgres/{branch}"))):
    for _form, _steps in _dolt(*_steps4).items():
        REG[f"{_sys}-{_form}.spec"] = (_proto, True, _steps, [])
# never a credited cell: the cap and warm-up plants' fixed-cost ops, the C1b-traced helper, the drop helper
UNREGISTERED = {"pg18-sleep5.spec", "pg18-sleep20.spec", "pg18-m1-c1b.spec", "pg18-m1-drop.spec"}


def lint(d):
    why = []
    files = sorted(f for f in os.listdir(d) if f.endswith(".spec"))
    for f in files:
        if f in UNREGISTERED:
            continue
        if f not in REG:
            why.append(f"{f}: neither a registered form nor an UNREGISTERED helper")
            continue
        proto, branch, timed, setups = REG[f]
        lines = [ln.rstrip("\n") for ln in open(os.path.join(d, f)) if ln.strip() and not ln.startswith("#")]
        got_proto = [ln for ln in lines if ln.startswith("protocol ")]
        if got_proto != [f"protocol {proto}"]:
            why.append(f"{f}: protocol {got_proto}, registered {proto}")
        got_var = [ln for ln in lines if ln.startswith("var ")]
        if got_var != ([BRANCH] if branch else []):
            why.append(f"{f}: var lines {got_var}, registered {[BRANCH] if branch else []}")
        got_steps = [ln for ln in lines if ln.startswith("step ")]
        if got_steps != timed:
            why.append(f"{f}: timed steps {got_steps}, registered {timed}")
        got_setup = [ln for ln in lines if ln.startswith("setup ")]
        if got_setup != setups:
            why.append(f"{f}: setup lines {got_setup}, registered {setups} (a setup can change the session's durability)")
    for f in sorted(REG):
        if f not in files:
            why.append(f"{f}: registered but missing")
    return why


def selftest():
    bad = n = 0

    def ok(name, cond, detail=""):
        nonlocal bad, n
        print(("PASS" if cond else "FAIL"), name + ("" if cond else f" ({detail})"))
        bad += not cond
        n += 1

    real = lint(SPECS)
    ok("the real specs match the registered statements", real == [], "; ".join(real))

    def planted(name, edit, want):
        with tempfile.TemporaryDirectory() as t:
            dd = os.path.join(t, "specs")
            shutil.copytree(SPECS, dd)
            edit(dd)
            why = lint(dd)
            ok(f"plant: {name}", any(want in w for w in why), "; ".join(why) or "accepted")

    def prepend_step(f, line):
        def ed(dd):
            p = os.path.join(dd, f)
            txt = open(p).read().split("\n")
            i = next(k for k, ln in enumerate(txt) if ln.startswith("step "))
            txt.insert(i, line)
            open(p, "w").write("\n".join(txt))
        return ed

    def drop_line(f, line):
        def ed(dd):
            p = os.path.join(dd, f)
            open(p, "w").write("\n".join(ln for ln in open(p).read().split("\n") if ln != line))
        return ed

    def swap_steps(f):
        def ed(dd):
            p = os.path.join(dd, f)
            txt = open(p).read().split("\n")
            idx = [k for k, ln in enumerate(txt) if ln.startswith("step ")]
            txt[idx[0]], txt[idx[1]] = txt[idx[1]], txt[idx[0]]
            open(p, "w").write("\n".join(txt))
        return ed

    planted("variant (a) times a no-op DOLT_CHECKOUT('main') (MED 6)",
            prepend_step("dolt-a-m1c.spec", "step sql CALL DOLT_CHECKOUT('main')"), "dolt-a-m1c.spec: timed steps")
    planted("Doltgres variant (a) times a no-op dolt_checkout('main')",
            prepend_step("doltgres-a-m1.spec", "step sql SELECT dolt_checkout('main')"), "doltgres-a-m1.spec: timed steps")
    planted("an M1 connect form without its probe", drop_line("pg18-m1.spec", PROBE), "pg18-m1.spec: timed steps")
    planted("two timed steps reordered", swap_steps("dolt-b-m1.spec"), "dolt-b-m1.spec: timed steps")
    planted("an unknown spec file", lambda dd: open(os.path.join(dd, "pg18-new.spec"), "w").write("protocol pg\n"),
            "pg18-new.spec: neither")
    planted("a setup line in a registered spec",
            prepend_step("pg18-create.spec", "setup SET synchronous_commit = off"), "pg18-create.spec: setup lines")
    planted("a registered spec missing", lambda dd: os.remove(os.path.join(dd, "doltgres-c-m1.spec")),
            "doltgres-c-m1.spec: registered but missing")
    print(f"TALLY ran={n} pass={n - bad} na=0 fail={bad}")  # gates/tally.py (linux-rev-uw2 MED 1)
    print(f"spec_lint selftest: {n - bad}/{n}")
    return 1 if bad else 0


def main(a):
    if a == ["selftest"]:
        return selftest()
    d = SPECS
    if a[:1] == ["--dir"] and len(a) == 2:
        d = a[1]
    elif a:
        print(__doc__, file=sys.stderr)
        return 2
    why = lint(d)
    for w in why:
        print("FAIL", w)
    print("spec_lint: " + ("clean" if not why else f"{len(why)} problem(s)"))
    return 0 if not why else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
