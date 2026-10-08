#!/usr/bin/env python3
"""timedrun.py -- the untraced timed run of a competitor cell (lane fastest-linux-comp; gate-6 review, t3run item 2).

PREREG :173: timed T3 runs carry no tracer. Every cell therefore has TWO runs of the identical bbload/clonebench
command: the traced LABELLING run (CELLDIR/bb/, strace attached: the flush counts, never a latency) and the untraced
TIMED run (CELLDIR/timed/: the only latency file a summary may use). The driver samples the TracerPid of every task
that serves the timed run (the server's processes, or the embedded clonebench process) at its start and at its end
into CELLDIR/timed.tracer.tsv ("phase pid tid tracerpid"), and writes the run's exit status to CELLDIR/timed.rc.

  timedrun.py check CELLDIR N   CELLDIR/timed.json; exit 0 only when the timed run exists, exited 0, measured
                                exactly N ops like the labelling run, and every sampled task, at start and at end,
                                had TracerPid 0 (no sample at either end is not a pass)
  timedrun.py selftest          known-answer fixtures for check; exit 0 only if every verdict is as expected
"""
import json
import os
import sys
import tempfile


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def check(celldir, n):
    """STUB (red): accepts every cell, so the selftest's refusal cases fail."""
    return []


def write_verdict(celldir, n, why):
    out = {"verdict": "ok" if not why else "REFUSED: " + "; ".join(why), "latency_file": "timed/raw.tsv",
           "labelling_dir": "bb", "ops": n}
    with open(os.path.join(celldir, "timed.json"), "w") as f:
        json.dump(out, f)
    return out


# ---------------------------------------------------------------- selftest
def fixture(root, name, n=200, timed=True, rc=0, timed_ops=None, tracer=None):
    d = os.path.join(root, name)
    os.makedirs(os.path.join(d, "bb"))
    with open(os.path.join(d, "bb", "summary.json"), "w") as f:
        json.dump({"verdict": "ok", "rc": 0, "measured_ops": n}, f)
    if timed:
        os.makedirs(os.path.join(d, "timed"))
        with open(os.path.join(d, "timed", "summary.json"), "w") as f:
            json.dump({"verdict": "ok" if rc == 0 else "fail", "rc": rc,
                       "measured_ops": n if timed_ops is None else timed_ops}, f)
        with open(os.path.join(d, "timed", "raw.tsv"), "w") as f:
            f.write("client\tseq\tphase\tok\tlat_ns\n0\t0\tmeasure\t1\t1000\n")
        with open(os.path.join(d, "timed.rc"), "w") as f:
            f.write(f"{rc}\n")
    if tracer is not None:
        with open(os.path.join(d, "timed.tracer.tsv"), "w") as f:
            f.write(tracer)
    return d


def selftest():
    clean = "start 100 100 0\nstart 101 101 0\nend 100 100 0\nend 101 101 0\n"
    cases = [  # (name, fixture kwargs, expect ok)
        ("untraced timed run", dict(tracer=clean), True),
        ("a server task traced at the end of the timed run", dict(tracer=clean.replace("end 101 101 0", "end 101 101 4242")),
         False),
        ("a server task traced at its start", dict(tracer=clean.replace("start 100 100 0", "start 100 100 77")), False),
        ("no timed run: only the traced labelling run's latency file", dict(timed=False, tracer=clean), False),
        ("no tracer samples", dict(tracer=""), False),
        ("no sample at the end", dict(tracer="start 100 100 0\n"), False),
        ("tracer record missing", dict(tracer=None), False),
        ("timed run failed", dict(rc=3, tracer=clean), False),
        ("timed run measured another N", dict(timed_ops=150, tracer=clean), False),
    ]
    bad = 0
    with tempfile.TemporaryDirectory() as root:
        for i, (name, kw, want) in enumerate(cases):
            d = fixture(root, f"c{i}", **kw)
            why = check(d, 200)
            got = not why
            print(("PASS" if got == want else "FAIL"), name, "->", "ok" if got else "; ".join(why))
            bad += got != want
    print(f"timedrun selftest: {len(cases) - bad}/{len(cases)}")
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "check":
        n = int(sys.argv[3])
        why = check(sys.argv[2], n)
        print(json.dumps(write_verdict(sys.argv[2], n, why)))
        sys.exit(0 if not why else 1)
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    sys.exit(__doc__)
