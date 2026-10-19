#!/usr/bin/env python3
"""bbload's end-of-run home probe is BOUNDED and fails CLOSED for every reply shape (comp-rev-2404 MED-1, LOW-1, LOW-2).

The probe (one SELECT 1 on the home connection after the last op: comp-rev-b15 LOW-1, comp-rev-67b2 LOW-B) must
report a dead or hung home connection as exactly that: rc 3, other_err >= 1, and "home connection lost" in errors.txt.
It must do so within its own bound (5 s) and well inside --stall-s (8 s here). It must never time out as an ops stall
(rc 4), and never pass as alive (rc 0). Each mode is a reply shape from fake_probe_server.py (no real server needed):
  pg     silent    no reply at all
  pg     partial   3 bytes of RowDescription, then nothing
  pg     noz       RowDescription, DataRow, CommandComplete, then no ReadyForQuery (LOW-1: unbounded at 240443aff)
  mysql  silent    no reply at all
  mysql  colcount  the column-count packet, then nothing (LOW-1)
  mysql  metastall column count, definition and EOF, then nothing (LOW-1)
  mysql  metaclose column count, definition and EOF, then the server closes the socket (MED-1: rc 0 at 240443aff)
  pg / mysql normal  a full reply: the run must pass, rc 0 and other_err 0 (the negative control)
--stall-s 8 also checks LOW-2: the probe's 5 s plus a close that waits on a hung peer (another 5 s) used to exceed it.
  test_probe_shapes.py --binary BBLOAD
Prints TALLY ran= pass= na=0 fail= (gates/tally.py); exit 0 only when every case passes.
"""
import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
MODES = [("pg", "normal"), ("pg", "silent"), ("pg", "partial"), ("pg", "noz"), ("mysql", "normal"),
         ("mysql", "silent"), ("mysql", "colcount"), ("mysql", "metastall"), ("mysql", "metaclose")]
STALL_S = 8


def one(binary, proto, mode, root):
    portf = root / "port"
    srv = subprocess.Popen([sys.executable, "-B", str(HERE / "fake_probe_server.py"), proto, mode, str(portf)],
                           stdout=subprocess.DEVNULL, stderr=open(root / "srv.log", "w"))
    try:
        for _ in range(100):
            if portf.exists() and portf.read_text().strip():
                break
            time.sleep(0.05)
        port = portf.read_text().strip()
        conn = (f"protocol pg\nconnect host=127.0.0.1 port={port} user=postgres dbname=x\n" if proto == "pg" else
                f"protocol mysql\nconnect host=127.0.0.1 port={port} user=root db=bench\n")
        (root / "t.spec").write_text(conn + "step sql SELECT 2\n")
        t0 = time.monotonic()
        p = subprocess.run([binary, "--spec", str(root / "t.spec"), "--out", str(root / "o"), "--clients", "1",
                            "--warmup", "0:0:0", "--max-ops", "3", "--stall-s", str(STALL_S)],
                           capture_output=True, text=True, timeout=120)
        secs = time.monotonic() - t0
    finally:
        srv.kill()
        srv.wait()
    sm = json.loads((root / "o/summary.json").read_text()) if (root / "o/summary.json").exists() else {}
    et = (root / "o/errors.txt").read_text() if (root / "o/errors.txt").exists() else ""
    return p.returncode, secs, sm, et, (p.stdout + p.stderr)[-200:]


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--binary", required=True)
    args = a.parse_args()
    n = bad = 0
    for proto, mode in MODES:
        with tempfile.TemporaryDirectory(prefix="ps-") as t:
            rc, secs, sm, et, tail = one(args.binary, proto, mode, Path(t))
        if mode == "normal":
            ok = rc == 0 and sm.get("other_err") == 0
            want = "rc 0, other_err 0"
        else:
            ok = rc == 3 and (sm.get("other_err") or 0) >= 1 and "home connection lost" in et and secs < STALL_S
            want = f"rc 3, other_err >= 1, 'home connection lost' in errors.txt, within {STALL_S} s"
        n += 1
        bad += not ok
        print(("PASS" if ok else "FAIL"), f"{proto} {mode}: want {want} -> rc {rc}, {secs:.1f} s, other_err "
              f"{sm.get('other_err')}, errors {et.strip()[:120]!r}, tail {tail.strip()[-100:]!r}")
    print(f"TALLY ran={n} pass={n - bad} na=0 fail={bad}")
    print(f"test_probe_shapes: {n - bad}/{n}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
