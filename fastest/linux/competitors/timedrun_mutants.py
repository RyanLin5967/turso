#!/usr/bin/env python3
"""C01: delete each tested refusal and print every case it kills. Run only outside QUIET.

The refusal statements, including early returns, are the guard effects. Deleting
one must remove its specific reason. A crash is not a kill. Files stay untouched.

comp-rev-89ec-b3c3 LOW 17: "62/62 killed" covered only `why.append(...)` and returned list literals. These guard
shapes are now mutated too, each kind named in the label:
  augassign   `why += ...` (check's pair, record and raw guards) -> `why += []`
  listcomp    a returned list comprehension (pair_problems' one refusal per differing key) -> `[]`
  tuple       a refusal tuple `return None, <why>` (prebranch_record) -> `return None, None`
  downgrade   write_verdict's `out["verdict"] = "ok-exploratory-n"` (MED 3) -> `out["verdict"] = out["verdict"]`
  claim-rule  raw_problems' `("claim_ns", "claimed")` (the lead's claim_ns ruling) -> `("intended_ns", "intended")`
"""
import ast
import contextlib
import copy
import io
from pathlib import Path


SOURCE = Path(__file__).with_name("timedrun.py")
# (+ the comp branch's guards, merged at 407b164a3: each run's own record, its raw rows, its after-step record; the
# sweeper's reader is gates/sweeper_record.py, shared with t3run.sh, whose own selftest plants each of its refusals)
FUNCTIONS = {"tracer_problems", "warm_problems", "run_problems", "check", "real_problem", "record_problems",
             "raw_problems", "after_problems", "pair_problems", "prebranch_record", "write_verdict"}


def _const(node, value):
    return isinstance(node, ast.Constant) and node.value == value


def candidates(tree):
    """(function, node, kind) for every guard effect in FUNCTIONS."""
    for function in tree.body:
        if not isinstance(function, ast.FunctionDef) or function.name not in FUNCTIONS:
            continue
        for node in ast.walk(function):
            if (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "append"
                    and isinstance(node.value.func.value, ast.Name) and node.value.func.value.id == "why"):
                yield function.name, node, "append"
            elif isinstance(node, ast.Return) and isinstance(node.value, (ast.List, ast.BinOp)):
                # Early refusals (missing file/header, malformed sweep/record).
                yield function.name, node, "return"
            elif (isinstance(node, ast.AugAssign) and isinstance(node.op, ast.Add) and isinstance(node.target, ast.Name)
                  and node.target.id == "why"):
                yield function.name, node, "augassign"
            elif isinstance(node, ast.Return) and isinstance(node.value, ast.ListComp):
                yield function.name, node, "listcomp"
            elif (isinstance(node, ast.Return) and isinstance(node.value, ast.Tuple) and len(node.value.elts) == 2
                  and _const(node.value.elts[0], None) and not _const(node.value.elts[1], None)):
                yield function.name, node, "tuple"
            elif (isinstance(node, ast.Assign) and _const(node.value, "ok-exploratory-n") and len(node.targets) == 1
                  and isinstance(node.targets[0], ast.Subscript)):
                yield function.name, node, "downgrade"
            elif (isinstance(node, ast.Tuple) and len(node.elts) == 2 and _const(node.elts[0], "claim_ns")
                  and _const(node.elts[1], "claimed")):
                yield function.name, node, "claim-rule"


def mutate(node, kind):
    if kind == "return" or kind == "listcomp":
        node.value = ast.List(elts=[], ctx=ast.Load())
    elif kind == "append":
        node.value = ast.Constant(value=None)
    elif kind == "augassign":
        node.value = ast.List(elts=[], ctx=ast.Load())
    elif kind == "tuple":
        node.value.elts[1] = ast.Constant(value=None)
    elif kind == "downgrade":
        load = copy.deepcopy(node.targets[0])
        for sub in ast.walk(load):
            if hasattr(sub, "ctx"):
                sub.ctx = ast.Load()
        node.value = load
    elif kind == "claim-rule":
        node.elts = [ast.Constant(value="intended_ns"), ast.Constant(value="intended")]
    else:
        raise ValueError(kind)


def run(tree):
    namespace = {"__name__": "timedrun_mutant", "__file__": str(SOURCE)}
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        exec(compile(ast.fix_missing_locations(tree), str(SOURCE), "exec"), namespace)
        rc = namespace["selftest"]()
    return rc, [line for line in output.getvalue().splitlines() if line.startswith("FAIL ")]


def main():
    original = ast.parse(SOURCE.read_text())
    rc, failures = run(copy.deepcopy(original))
    if rc or failures:
        print("REFUSED: baseline selftest failed", *failures, sep="\n")
        return 1
    bad = n = 0
    kinds = {}
    for function, target, kind in candidates(original):
        n += 1
        kinds[kind] = kinds.get(kind, 0) + 1
        key = (target.lineno, target.col_offset, kind)
        mutant = copy.deepcopy(original)
        hit = 0
        for name, node, k in candidates(mutant):
            if name == function and (node.lineno, node.col_offset, k) == key:
                label = f"{function}:{target.lineno} [{kind}]: {ast.unparse(node)}"
                mutate(node, k)
                hit += 1
        if hit != 1:
            print(f"INVALID (matched {hit} nodes): {function}:{target.lineno} [{kind}]")
            bad += 1
            continue
        try:
            rc, failures = run(mutant)
        except Exception as exc:
            print(f"INVALID (crash): {label}: {exc}")
            bad += 1
            continue
        if rc and failures:
            print(f"KILLED: {label}")
            print(*failures, sep="\n")
        else:
            print(f"SURVIVED: {label}")
            bad += 1
    print(f"# mutants by kind: {kinds}")
    print(f"TALLY ran={n} pass={n - bad} na=0 fail={bad}")  # gates/tally.py (linux-rev-uw2 MED 1)
    return int(bool(bad))


if __name__ == "__main__":
    raise SystemExit(main())
