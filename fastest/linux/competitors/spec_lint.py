#!/usr/bin/env python3
"""spec_lint.py [--dir DIR] | selftest -- the Linux specs' timed steps against the registered statements (lane
fastest-linux-comp; comp review 1 MED 6).

REG below transcribes the PREREG's §6 Statements table (artie frontier/fastest/tools/loadgen/specs/STATEMENTS.md,
printed by the macOS lint_specs.py from its REG data) for the forms each Linux spec runs: the TIMED steps, in order,
as bbload spec lines. The Linux specs keep their own home connect lines (the SSL/GSS forcing is bbload's, Codex card
C02) and after-steps (the delete and its serialisation are the lead's re-ruling, Codex card C04), so only the timed
steps, the protocol and the branch var are compared here, plus, for variant (b)'s switch forms, the FIRST after-step:
the registered switch-back (USE bench / USE postgres), without which the delete would run on the branch it deletes.
Variant (b)'s switch is the PREREG N7 ruling as STATEMENTS.md rows dolt/doltgres b m1c-switch and m1-switch carry it
(artie 5cab0ae50c): Dolt USE `bench/{branch}` (DECISIONS 2026-10-15T11:04:04Z), Doltgres USE "postgres/{branch}"
(DECISIONS 2026-10-15T11:32:50Z). The old CALL DOLT_CHECKOUT / SELECT dolt_checkout switch is refused (comp-rev-89ec-b3c3
MED 1: this table had been copied from the old Linux specs, so the lint passed them). Every *.spec in the directory must be either in REG or in
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


# variant (b)'s registered switch and its switch-back (STATEMENTS.md, artie 5cab0ae50c), and the old forms MED 1 found
DOLT_SW, DOLT_BACK = "step sql USE `bench/{branch}`", "after sql USE bench"
DOLTGRES_SW, DOLTGRES_BACK = 'step sql USE "postgres/{branch}"', "after sql USE postgres"
OLD_SW = {"dolt": ("step sql CALL DOLT_CHECKOUT('{branch}')", "after sql CALL DOLT_CHECKOUT('main')"),
          "doltgres": ("step sql SELECT dolt_checkout('{branch}')", "after sql SELECT dolt_checkout('main')")}


FC, FCC, FCM = _pg("FILE_COPY")
WC, WCC, WCM = _pg("WAL_LOG")
# spec file -> (protocol, branch var?, timed steps, setup lines allowed[, first after-step])
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
for _sys, _proto, _back, _steps4 in (
        ("dolt", "mysql", DOLT_BACK, ("step sql CALL DOLT_CHECKOUT('-b', '{branch}')",
                                      "step sql CALL DOLT_BRANCH('{branch}', 'main')", DOLT_SW,
                                      "step connect db=bench/{branch}")),
        ("doltgres", "pg", DOLTGRES_BACK, ("step sql SELECT dolt_checkout('-b', '{branch}')",
                                           "step sql SELECT dolt_branch('{branch}', 'main')", DOLTGRES_SW,
                                           "step connect dbname=postgres/{branch}"))):
    for _form, _steps in _dolt(*_steps4).items():
        REG[f"{_sys}-{_form}.spec"] = (_proto, True, _steps, []) + ((_back,) if _form in ("b-m1c", "b-m1") else ())
# comp-rev-89ec-b3c3 LOW 20: each system's ONE home connect line, as every Linux spec of it carries it (a connect string
# can change durability as a setup line can: options=-csynchronous_commit=off), by spec file prefix
HOME = {"pg18-": "connect host=127.0.0.1 port={port} user=postgres dbname=postgres",
        "doltgres-": "connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres",
        "dolt-": "connect host=127.0.0.1 port={port} user=root db=bench"}


def home_of(f):
    """The registered home connect line for spec file F (the longest matching prefix: doltgres- before dolt-)."""
    return next((HOME[p] for p in sorted(HOME, key=len, reverse=True) if f.startswith(p)), None)


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
        proto, branch, timed, setups = REG[f][:4]
        back = REG[f][4] if len(REG[f]) > 4 else None
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
        got_conn = [ln for ln in lines if ln.startswith("connect ")]
        if got_conn != [home_of(f)]:
            why.append(f"{f}: connect lines {got_conn}, registered [{home_of(f)!r}] (a connect string can change the "
                       f"session's durability)")
        got_after = [ln for ln in lines if ln.startswith("after ")]
        if back is not None and got_after[:1] != [back]:
            why.append(f"{f}: first after-step {got_after[:1]}, registered switch-back {back}")
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

    def planted(name, edit, want, clean_file=None):
        """want: a problem holding this text must be printed; clean_file (a control): no problem may name that file."""
        with tempfile.TemporaryDirectory() as t:
            dd = os.path.join(t, "specs")
            shutil.copytree(SPECS, dd)
            edit(dd)
            why = lint(dd)
            if clean_file is not None:
                mine = [w for w in why if w.startswith(clean_file + ":")]
                ok(name, mine == [], "; ".join(mine))
            else:
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
    # comp-rev-89ec-b3c3 MED 1: the old (b) switch, CALL DOLT_CHECKOUT / SELECT dolt_checkout, is refused, timed or as
    # the switch-back. Each plant writes the whole file, so it holds whatever the real spec says. (Its home connect is
    # the system's registered one, home_of: LOW 20 checks it, and the control below must be the registered form whole.)
    def write_spec(sysname, form, steps, back):
        def ed(dd):
            f = f"{sysname}-{form}.spec"
            open(os.path.join(dd, f), "w").write(
                "\n".join([f"protocol {REG[f][0]}", home_of(f), BRANCH] + steps + [back]) + "\n")
        return ed

    for _s, _f in (("dolt", "b-m1c"), ("dolt", "b-m1"), ("doltgres", "b-m1c"), ("doltgres", "b-m1")):
        _timed, _back = REG[f"{_s}-{_f}.spec"][2], REG[f"{_s}-{_f}.spec"][4]
        _old = [OLD_SW[_s][0] if ln in (DOLT_SW, DOLTGRES_SW) else ln for ln in _timed]
        planted(f"{_s}-{_f} times the old checkout switch (MED 1)", write_spec(_s, _f, _old, OLD_SW[_s][1]),
                f"{_s}-{_f}.spec: timed steps")
        planted(f"{_s}-{_f} switches back with the old checkout (MED 1)", write_spec(_s, _f, _timed, OLD_SW[_s][1]),
                f"{_s}-{_f}.spec: first after-step")
        planted(f"control: {_s}-{_f} written in the registered form is accepted",
                write_spec(_s, _f, _timed, _back), None, clean_file=f"{_s}-{_f}.spec")
    # comp-rev-89ec-b3c3 LOW 20: the home connect line can change durability as a setup line can (a libpq options=
    # string sets synchronous_commit for the session), so it is checked against the system's one registered line
    def edit_connect(f, new):
        def ed(dd):
            p = os.path.join(dd, f)
            txt = [new if ln.startswith("connect ") else ln for ln in open(p).read().split("\n")]
            open(p, "w").write("\n".join(txt))
        return ed

    planted("a home connect carrying options=-csynchronous_commit=off (LOW 20)",
            edit_connect("pg18-create.spec",
                         "connect host=127.0.0.1 port={port} user=postgres dbname=postgres options=-csynchronous_commit=off"),
            "pg18-create.spec: connect")
    planted("a Dolt home connect to another database (LOW 20)",
            edit_connect("dolt-b-m1c.spec", "connect host=127.0.0.1 port={port} user=root db=other"),
            "dolt-b-m1c.spec: connect")
    planted("a spec with two home connect lines (LOW 20)",
            prepend_step("doltgres-c-m1.spec",
                         "connect host=127.0.0.1 port={port} user=postgres password=password dbname=postgres"),
            "doltgres-c-m1.spec: connect")
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
