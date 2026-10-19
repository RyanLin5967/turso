#!/usr/bin/env python3
"""bbload's end-of-run home probe is BOUNDED and fails CLOSED for every reply shape (comp-rev-2404 MED-1, LOW-1,
LOW-2; comp-rev-a145 MED-1, LOW-3..5).

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
  pg / mysql normal  a full reply: rc 0, other_err 0, and the run ends in under 3 s (comp-rev-a145 LOW-5: the
                     controls used to wait out the close bound)
comp-rev-a145:
  mysql metaclose3 (branch) a MEASURED op on a branch connection (step connect / SELECT 3 / step close) whose reply
                     breaks after the metadata: rc 3, measured_err >= 1 (MED-1: rc 0 "ok 3 err 0" before a145a5c9d)
  pg / mysql late    the home answers the probe after 4.5 s, and closes 4.5 s after Terminate/Quit: alive, so rc 0,
                     and probe plus final close stay inside ONE 5 s deadline, so no stall at --stall-s 8 (LOW-4)
  --stall-s 4        refused (rc 2): the probe's 5 s bound must be below --stall-s (LOW-3)
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
STALL_S = 8
HOME_STEP = "step sql SELECT 2\n"
BRANCH_STEPS = "step connect db=bench\nstep sql SELECT 3\nstep close\n"
# (proto, mode, steps, stall_s, expect)
CASES = [("pg", "normal", HOME_STEP, STALL_S, "ok"), ("pg", "silent", HOME_STEP, STALL_S, "home"),
         ("pg", "partial", HOME_STEP, STALL_S, "home"), ("pg", "noz", HOME_STEP, STALL_S, "home"),
         ("mysql", "normal", HOME_STEP, STALL_S, "ok"), ("mysql", "silent", HOME_STEP, STALL_S, "home"),
         ("mysql", "colcount", HOME_STEP, STALL_S, "home"), ("mysql", "metastall", HOME_STEP, STALL_S, "home"),
         ("mysql", "metaclose", HOME_STEP, STALL_S, "home"),
         ("mysql", "metaclose3", BRANCH_STEPS, STALL_S, "measured"),
         ("pg", "late", HOME_STEP, STALL_S, "slow-ok"), ("mysql", "late", HOME_STEP, STALL_S, "slow-ok"),
         ("pg", "normal", HOME_STEP, 4, "refused")]


def one(binary, proto, mode, steps, stall, root):
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
        (root / "t.spec").write_text(conn + steps)
        t0 = time.monotonic()
        p = subprocess.run([binary, "--spec", str(root / "t.spec"), "--out", str(root / "o"), "--clients", "1",
                            "--warmup", "0:0:0", "--max-ops", "3", "--stall-s", str(stall)],
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
    for proto, mode, steps, stall, expect in CASES:
        with tempfile.TemporaryDirectory(prefix="ps-") as t:
            rc, secs, sm, et, tail = one(args.binary, proto, mode, steps, stall, Path(t))
        if expect == "ok":
            ok = rc == 0 and sm.get("other_err") == 0 and secs < 3
            want = "rc 0, other_err 0, under 3 s"
        elif expect == "home":
            ok = rc == 3 and (sm.get("other_err") or 0) >= 1 and "home connection lost" in et and secs < stall
            want = f"rc 3, other_err >= 1, 'home connection lost' in errors.txt, within {stall} s"
        elif expect == "measured":
            ok = rc == 3 and (sm.get("measured_err") or 0) >= 1 and secs < stall
            want = f"rc 3, measured_err >= 1, within {stall} s"
        elif expect == "slow-ok":
            ok = rc == 0 and sm.get("other_err") == 0 and secs < stall
            want = f"rc 0, other_err 0, within {stall} s (probe and close share one 5 s deadline)"
        else:
            ok = rc == 2 and "stall" in tail.lower()
            want = "rc 2, refused naming --stall-s"
        n += 1
        bad += not ok
        print(("PASS" if ok else "FAIL"), f"{proto} {mode} stall={stall}: want {want} -> rc {rc}, {secs:.1f} s, "
              f"other_err {sm.get('other_err')}, measured_err {sm.get('measured_err')}, errors {et.strip()[:100]!r}, "
              f"tail {tail.strip()[-100:]!r}")
    print(f"TALLY ran={n} pass={n - bad} na=0 fail={bad}")
    print(f"test_probe_shapes: {n - bad}/{n}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
