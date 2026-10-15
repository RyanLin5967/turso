#!/usr/bin/env python3
"""C04 red: measure a serial after-step on an existing PG server, C=4.

Uses pg_sleep only after the timed SELECT. The serial mutex must reduce measured
in-flight work; recompute its mean from all raw intervals clipped to the window.
The --binary argument is a compiled bbload; no server is started by this test.
"""
import argparse
import csv
import json
import math
from pathlib import Path
import subprocess
import tempfile


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", required=True)
    p.add_argument("--conninfo", default="host=127.0.0.1 port=5432 user=postgres dbname=postgres")
    a = p.parse_args()
    for spec in (Path(__file__).parent / "specs").glob("doltgres-*.spec"):
        assert "after sql-serial " not in spec.read_text(), spec
    with tempfile.TemporaryDirectory(prefix="C04-") as tmp:
        root = Path(tmp)
        spec = root / "serial.spec"
        spec.write_text(f"protocol pg\nconnect {a.conninfo}\nstep sql SELECT 1\n"
                        "after sql-serial SELECT pg_sleep(0.02)\n")
        run = subprocess.run([a.binary, "--spec", str(spec), "--out", str(root / "out"),
                              "--clients", "4", "--warmup", "0:0:0", "--max-ops", "40"],
                             capture_output=True, text=True, timeout=30)
        assert run.returncode == 0, (run.returncode, run.stderr)
        sm = json.loads((root / "out/summary.json").read_text())
        with (root / "out/raw.tsv").open() as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
        assert len(rows) == 40 and all(r["ok"] == "1" for r in rows), rows
        assert all("serial_wait_ns" in r for r in rows), "missing per-op serial_wait_ns"
        assert any(int(r["serial_wait_ns"]) > 0 for r in rows), "no wait recorded"
        assert all(0 <= int(r["serial_wait_ns"]) <= int(r["after_ns"]) for r in rows)
        lo, hi = sm["tm0_ns"], sm["tm1_ns"]
        area = sum(max(0, min(int(r["end_ns"]), hi) - max(int(r["start_ns"]), lo)) for r in rows)
        assert math.isclose(sm["mean_inflight"], area / (hi - lo), rel_tol=1e-8, abs_tol=1e-8)
        assert sm["mean_inflight"] < 4, sm
        assert sm["concurrency_label"] == "REDUCED-CONCURRENCY", sm
        assert sm["c_label_credit"] is False, sm
        assert "Dolt" in sm["serial_delete_note"], sm
        assert sm["verdict"] == "REDUCED-CONCURRENCY", sm
        print("PASS serial wait, independently recomputed mean, and C-credit refusal")


if __name__ == "__main__":
    main()
