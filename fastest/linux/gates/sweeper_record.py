#!/usr/bin/env python3
"""sweeper_record.py -- the ONE reader of gates/sweeper.sh's record (the lead's ruling on comp review 1 MED 12: the
same instrument, and the same reading of it, in every timed run; competitors/timedrun.py imports it, t3/t3run.sh can
call it).

From an OUT.tracer.tsv it reads the sweeper's header ("sweeper pid=P nice=N clk_tck=K") and, per sweep, the
utime+stime ticks the sweeper read from its own /proc stat (cpu=T), and gives the sweeper's CPU between the sweeps
bracketing a measured window [T0, T1] (realtime seconds; the last sweep at or before T0, the first at or after T1):
sweeper_cpu_s, sweeper_cpu_span_s, sweeper_nice, sweeper_clk_tck. REFUSED (no record): no header, a nice other than
SWEEPER_NICE, a sweep with no cpu= (or '?': unreadable), CPU that went backwards, no sweeps bracketing the window.

  sweeper_record.py record TRACER.TSV T0 T1   the record as JSON, exit 0; or "REFUSED: ...", exit 1
  sweeper_record.py selftest                   known answers over planted records; exit 0 only if every one holds
"""
import json
import sys

SWEEPER_NICE = 19  # gates/sweeper.sh renices its subshell to 19


def sweeper_record(lines, t0, t1):
    """({"sweeper_cpu_s", "sweeper_cpu_span_s", "sweeper_nice", "sweeper_clk_tck"} or None, the reasons it is unknown
    or refused) for the record LINES and the window [T0, T1] (None: unknown)."""
    hdr = next((ln.split()[1:] for ln in lines if ln.startswith("sweeper ")), None)
    kv = dict(x.split("=", 1) for x in hdr or [] if "=" in x)
    nice, clk = kv.get("nice", ""), kv.get("clk_tck", "")
    if hdr is None or not nice.lstrip("-").isdigit() or not clk.isdigit() or int(clk) <= 0:
        return None, [f"the tracer record has no sweeper header with its nice and clk_tck ({hdr!r})"]
    why = []
    if int(nice) != SWEEPER_NICE:
        why.append(f"the sweeper ran at nice {int(nice)}, not {SWEEPER_NICE}")
    pts = []
    for ln in lines:
        p = ln.split()
        if p and p[0] == "SWEEP":
            c = next((x[4:] for x in p[5:] if x.startswith("cpu=")), None)
            if c is None or not c.isdigit():
                return None, why + [f"sweep line {ln!r} has no sweeper CPU (cpu=)"]
            try:
                pts.append((float(p[2]), int(c)))
            except (IndexError, ValueError):
                return None, why + [f"unreadable sweep line {ln!r}"]
    if any(b[1] < a[1] for a, b in zip(pts, pts[1:])):  # file order is sweep order (the record is append-only)
        why.append(f"the sweeper's CPU went backwards between sweeps: {pts[:12]}")
    num = all(isinstance(t, (int, float)) and not isinstance(t, bool) for t in (t0, t1))
    before = [q for q in pts if num and q[0] <= t0]
    after = [q for q in pts if num and q[0] >= t1]
    if not before or not after:
        why.append("no sweeps bracket the measured window, so the sweeper's CPU over it is unknown")
    if why:
        return None, why
    a, b, k = max(before), min(after), int(clk)
    return {"sweeper_cpu_s": (b[1] - a[1]) / k, "sweeper_cpu_span_s": [a[0], b[0]], "sweeper_nice": int(nice),
            "sweeper_clk_tck": k}, []


def selftest():
    def rec(sweeper="pid=600 nice=19 clk_tck=100", cpus=range(10)):
        out = ["roles cmd=500 srv=100 tree=children"] + ([f"sweeper {sweeper}"] if sweeper is not None else [])
        for i, c in enumerate(cpus):
            t = round(100.0 + 0.05 * i, 2)
            out.append(f"SWEEP {'start' if i == 0 else 'mid'} {t:.6f} cmd=3:0 srv=12:0" + (f" cpu={c}" if c is not None
                                                                                             else ""))
        return out
    bad = n = 0

    def ok(cond, line):
        nonlocal bad, n
        print(("PASS" if cond else "FAIL"), line)
        bad += not cond
        n += 1
    # by hand: sweeps every 0.05 s from 100.00 with cpu = index; the window [100.1, 100.3] is bracketed by the sweeps
    # at 100.10 (cpu 2) and 100.30 (cpu 6): 4 ticks / clk_tck 100 = 0.04 s over [100.1, 100.3]
    r, why = sweeper_record(rec(), 100.1, 100.3)
    ok(r is not None and abs(r["sweeper_cpu_s"] - 0.04) < 1e-12 and r["sweeper_cpu_span_s"] == [100.1, 100.3]
       and r["sweeper_nice"] == 19 and r["sweeper_clk_tck"] == 100, f"clean record -> {r!r} {why!r}")
    for name, lines, t0, t1, reason in (
            ("no sweeper header", rec(sweeper=None), 100.1, 100.3, "no sweeper header"),
            ("nice 0", rec(sweeper="pid=600 nice=0 clk_tck=100"), 100.1, 100.3, "nice 0, not 19"),
            ("a sweep with no cpu=", rec(cpus=[0, 1, None, 3, 4, 5, 6, 7]), 100.1, 100.3, "has no sweeper CPU"),
            ("an unreadable cpu ('?')", rec(cpus=[0, 1, 2, "?", 4, 5, 6, 7]), 100.1, 100.3, "has no sweeper CPU"),
            ("CPU going backwards", rec(cpus=[0, 1, 2, 0, 4, 5, 6, 7]), 100.1, 100.3, "went backwards"),
            ("no sweep at or before T0", rec(), 99.0, 100.3, "no sweeps bracket"),
            ("no sweep at or after T1", rec(), 100.1, 101.0, "no sweeps bracket"),
            ("an unknown window", rec(), None, 100.3, "no sweeps bracket")):
        r, why = sweeper_record(lines, t0, t1)
        ok(r is None and any(reason in w for w in why), f"{name} -> refused ({why!r})")
    print(f"sweeper_record selftest: {n - bad}/{n}")
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    if len(sys.argv) == 5 and sys.argv[1] == "record":
        try:
            lines = open(sys.argv[2]).read().splitlines()
            t0, t1 = float(sys.argv[3]), float(sys.argv[4])
        except (OSError, ValueError) as e:
            print(f"REFUSED: {e}")
            sys.exit(1)
        r, why = sweeper_record(lines, t0, t1)
        print(json.dumps(r) if r else "REFUSED: " + "; ".join(why))
        sys.exit(0 if r else 1)
    sys.exit(__doc__)
