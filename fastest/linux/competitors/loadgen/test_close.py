#!/usr/bin/env python3
"""C03 live close plant. Requires a seeded PG18 instance (run_system.sh's seed).

Use --binary for the clean bbload and --mutant-binary for a separately built copy
with conn_close_sync replaced by bare PQfinish/mysql_close. Both runs must finish
successfully; only an after_ns >= 100 ms kills the mutant, never a setup error.
The raw runs remain in --out for inspection. No timings are claimed under QUIET.
"""
import argparse
import csv
from pathlib import Path
import subprocess
import time


def run(binary, spec, out, sets):
    cmd = [str(Path(binary).resolve()), "--spec", str(spec), "--out", str(out),
           "--clients", "1", "--warmup", "0:0:0", "--max-ops", "200",
           "--max-window-s", "1800", "--run-tag", f"c03_{time.time_ns()}"]
    for setting in sets:
        cmd += ["--set", setting]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1900)
    out.parent.joinpath(out.name + ".stdout").write_text(result.stdout)
    out.parent.joinpath(out.name + ".stderr").write_text(result.stderr)
    assert result.returncode == 0, (cmd, result.returncode, result.stderr)
    with (out / "raw.tsv").open() as f:
        rows = [r for r in csv.DictReader(f, delimiter="\t") if r["phase"] == "measure"]
    assert len(rows) == 200, (out, len(rows))
    assert all(r["ok"] == "1" for r in rows), (out, "operation/after-step failed")
    slow = [r for r in rows if int(r["after_ns"]) >= 100_000_000]
    print(f"{out}: {len(slow)}/200 after_ns >= 100000000; raw.tsv is the instrument")
    return slow


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", required=True)
    p.add_argument("--mutant-binary", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--set", action="append", default=[])
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=False)
    for name in ("pg18-m1", "pg18-m1c"):
        spec = Path(__file__).parent / "specs" / (name + ".spec")
        clean = run(a.binary, spec, a.out / (name + "-clean"), a.set)
        assert not clean, (name, "clean close has >= 100 ms after-step")
        mutant = run(a.mutant_binary, spec, a.out / (name + "-bare"), a.set)
        assert mutant, (name, "bare-close mutant survived; race was not demonstrated")


if __name__ == "__main__":
    main()
