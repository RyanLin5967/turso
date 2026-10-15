#!/usr/bin/env python3
"""C04 red: measure a serial after-step on an existing PG server, C=4.

Uses pg_sleep only after the timed SELECT. Compare serial and plain cleanup;
independently integrate every lock wait, including waits crossing the window.
The --binary argument is a compiled bbload; no server is started by this test.
"""
import argparse
import csv
import json
import math
from pathlib import Path
import subprocess
import tempfile
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import timedrun


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", required=True)
    p.add_argument("--conninfo", default="host=127.0.0.1 port=5432 user=postgres dbname=postgres")
    a = p.parse_args()
    for spec in (Path(__file__).parent / "specs").glob("doltgres-*.spec"):
        assert "after sql-serial " not in spec.read_text(), spec
    for serial in (True, False):
      with tempfile.TemporaryDirectory(prefix="C04-") as tmp:
        root = Path(tmp)
        spec = root / "serial.spec"
        spec.write_text(f"protocol pg\nconnect {a.conninfo}\nstep sql SELECT 1\n"
                        f"after {'sql-serial' if serial else 'sql'} SELECT pg_sleep(0.02)\n")
        run = subprocess.run([a.binary, "--spec", str(spec), "--out", str(root / "out"),
                              "--clients", "4", "--warmup", "0:0:0", "--max-ops", "40"],
                             capture_output=True, text=True, timeout=30)
        assert run.returncode == 0, (run.returncode, run.stderr)
        sm = json.loads((root / "out/summary.json").read_text())
        with (root / "out/raw.tsv").open() as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
        assert len(rows) == 40 and all(r["ok"] == "1" for r in rows), rows
        assert all("serial_wait_ns" in r for r in rows), "missing per-op serial_wait_ns"
        assert all("serial_waits" in r for r in rows), "missing serial wait intervals"
        if serial:
            assert any(int(r["serial_wait_ns"]) > 0 for r in rows), "no wait recorded"
        assert all(0 <= int(r["serial_wait_ns"]) <= int(r["after_ns"]) for r in rows)
        lo, hi = sm["tm0_ns"], sm["tm1_ns"]
        area = sum(max(0, min(int(r["end_ns"]), hi) - max(int(r["start_ns"]), lo)) for r in rows)
        assert math.isclose(sm["mean_inflight"], area / (hi - lo), rel_tol=1e-8, abs_tol=1e-8)
        assert sm["mean_inflight"] < 4, sm
        wait_area = 0
        for r in rows:
            spans = [tuple(map(int, s.split("-"))) for s in r["serial_waits"].split(";") if s]
            assert all(int(r["end_ns"]) <= a <= b <= int(r["end_ns"]) + int(r["after_ns"])
                       for a, b in spans), r
            assert sum(b - a for a, b in spans) == int(r["serial_wait_ns"]), r
            wait_area += sum(max(0, min(b, hi) - max(a, lo)) for a, b in spans)
        mean = wait_area / (hi - lo)
        assert math.isclose(sm["mean_serial_waiters"], mean, rel_tol=1e-8, abs_tol=1e-8), sm
        assert math.isclose(sm["effective_c"], 4 - mean, rel_tol=1e-8, abs_tol=1e-8), sm
        assert sm["concurrency_label"] == ("REDUCED-CONCURRENCY" if serial else "registered"), sm
        assert sm["c_label_credit"] is (not serial), sm
        if serial:
            assert "Dolt" in sm["serial_delete_note"], sm
        else:
            assert mean == 0, sm
        # Lead's rework explicitly replaces the old verdict expectation: the
        # label must not void a valid timed result. Exercise the real checker.
        assert sm["verdict"] == "ok", sm
        cell = timedrun.fixture(str(root), "timed-cell", tracer=timedrun.trace_rec(),
                                timed_extra={k: sm[k] for k in ("verdict", "rc", "concurrency_label",
                                                              "effective_c", "mean_serial_waiters")})
        assert timedrun.check(cell, 200, timedrun.RULE, timedrun.LIVE, timedrun.CAP) == []
        print("PASS", "serial" if serial else "plain", "wait accounting, label, and timed acceptance")


if __name__ == "__main__":
    main()
