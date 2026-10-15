#!/usr/bin/env python3
"""C01: delete each tested refusal and print every case it kills. Run only outside QUIET.

The refusal statements, including early returns, are the guard effects. Deleting
one must remove its specific reason. A crash is not a kill. Files stay untouched.
"""
import ast
import contextlib
import copy
import io
from pathlib import Path


SOURCE = Path(__file__).with_name("timedrun.py")
FUNCTIONS = {"tracer_problems", "warm_problems", "run_problems", "check", "real_problem"}


def candidates(tree):
    for function in tree.body:
        if not isinstance(function, ast.FunctionDef) or function.name not in FUNCTIONS:
            continue
        for node in ast.walk(function):
            if (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "append"
                    and isinstance(node.value.func.value, ast.Name) and node.value.func.value.id == "why"):
                yield function.name, node
            elif isinstance(node, ast.Return) and isinstance(node.value, (ast.List, ast.BinOp)):
                # Early refusals (missing file/header, malformed sweep/record).
                yield function.name, node


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
    bad = 0
    for function, target in candidates(original):
        mutant = copy.deepcopy(original)
        for name, node in candidates(mutant):
            if name != function or node.lineno != target.lineno:
                continue
            if isinstance(node, ast.Return):
                node.value = ast.List(elts=[], ctx=ast.Load())
            else:
                node.value = ast.Constant(value=None)
            break
        label = f"{function}:{target.lineno}: {ast.unparse(target)}"
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
    return int(bool(bad))


if __name__ == "__main__":
    raise SystemExit(main())
