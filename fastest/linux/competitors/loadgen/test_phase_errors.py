#!/usr/bin/env python3
"""comp-rev-dolt250 HIGH-1 red: a failed op is a failure in EVERY phase, never one the run hides.

sql-serial narrows Dolt's concurrent-delete race and does not close it, so a serialized Dolt cell can still lose
an op (a recovered panic: "Lost connection") in its warm-up or drain. bbload counted measured failures only:
a failed warm-up or drain op left the run rc 0 and appeared nowhere in summary.json.

Needs a running MySQL-protocol server (a Dolt sql-server) with database DB; no server is started here. A failure is
made deterministic with a `step write` (it must affect >= 1 row) on a table missing id 1, so the client's op 1
(a warm-up op at --warmup 3:0:60, C=1) fails and every other op succeeds.
  test_phase_errors.py --binary BBLOAD --port PORT [--db bench]
Prints TALLY ran= pass= na=0 fail= (gates/tally.py); exit 0 only when every case passes.
"""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path


def run(binary, port, db, root, missing, extra=()):
    spec = root / "pe.spec"
    ids = ",".join(f"({i},0)" for i in range(0, 200) if i not in missing)
    spec.write_text(
        "protocol mysql\n"
        f"connect host=127.0.0.1 port={port} user=root db={db}\n"
        "setup CREATE TABLE IF NOT EXISTS pe (id INT PRIMARY KEY, v INT)\n"
        "setup DELETE FROM pe\n"
        f"setup INSERT INTO pe VALUES {ids}\n"
        "step write UPDATE pe SET v = v + 1 WHERE id = {i}\n")
    out = root / "out"
    p = subprocess.run([binary, "--spec", str(spec), "--out", str(out), "--clients", "1", "--warmup", "3:0:60",
                        "--max-ops", "10", *extra], capture_output=True, text=True, timeout=120)
    sm = json.loads((out / "summary.json").read_text()) if (out / "summary.json").exists() else {}
    return p.returncode, sm, p.stdout + p.stderr


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--binary", required=True)
    a.add_argument("--port", required=True)
    a.add_argument("--db", default="bench")
    args = a.parse_args()
    cases = []

    def case(name, ok, got):
        cases.append(ok)
        print(("PASS" if ok else "FAIL"), name, "->", got)

    with tempfile.TemporaryDirectory(prefix="pe-") as t:
        rc, sm, txt = run(args.binary, args.port, args.db, Path(t), missing=())
        case("control: no op fails -> rc 0, warmup_err 0, measured_err 0",
             rc == 0 and sm.get("warmup_err") == 0 and sm.get("measured_err") == 0 and sm.get("drain_err") == 0,
             (rc, sm.get("warmup_err"), sm.get("measured_err"), sm.get("drain_err"), sm.get("verdict")))
    with tempfile.TemporaryDirectory(prefix="pe-") as t:
        rc, sm, txt = run(args.binary, args.port, args.db, Path(t), missing=(1,))
        case("a failed warm-up op refuses the run (rc 3) and is counted (warmup_err 1)",
             rc == 3 and sm.get("warmup_err") == 1 and "warm-up" in str(sm.get("verdict", "")),
             (rc, sm.get("warmup_err"), sm.get("verdict"), txt.strip().splitlines()[-1:]))
    with tempfile.TemporaryDirectory(prefix="pe-") as t:
        rc, sm, txt = run(args.binary, args.port, args.db, Path(t), missing=(1,), extra=("--allow-errors",))
        case("--allow-errors accepts it but still reports it (warmup_err 1, failed_ops 1)",
             rc == 0 and sm.get("warmup_err") == 1 and sm.get("failed_ops") == 1,
             (rc, sm.get("warmup_err"), sm.get("failed_ops"), sm.get("verdict")))
    with tempfile.TemporaryDirectory(prefix="pe-") as t:
        rc, sm, txt = run(args.binary, args.port, args.db, Path(t), missing=(5,))
        case("a failed measured op still refuses (rc 3) and is counted (measured_err 1)",
             rc == 3 and sm.get("measured_err") == 1, (rc, sm.get("measured_err"), sm.get("verdict")))
    n, bad = len(cases), cases.count(False)
    print(f"TALLY ran={n} pass={n - bad} na=0 fail={bad}")
    print(f"test_phase_errors: {n - bad}/{n}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
