#!/usr/bin/env python3
"""timedrun.py -- the untraced timed run of a competitor cell (lane fastest-linux-comp; gate-6 review, t3run item 2).

PREREG :173: timed T3 runs carry no tracer. Every cell therefore has TWO runs of the identical bbload/clonebench
command: the traced LABELLING run (CELLDIR/bb/, strace attached: the flush counts, never a latency) and the untraced
TIMED run (CELLDIR/timed/: the only latency file a summary may use). The driver samples the TracerPid of every task
that serves the timed run (the server's processes, or the embedded clonebench process) at its start and at its end
into CELLDIR/timed.tracer.tsv ("phase pid tid tracerpid"), and writes the run's exit status to CELLDIR/timed.rc.

  timedrun.py check CELLDIR N RULE
                                CELLDIR/timed.json; exit 0 only when the timed run exists, exited 0, measured
                                exactly N ops like the labelling run, every sampled task, at start and at end, had
                                TracerPid 0 (no sample at either end is not a pass), and both runs recorded the
                                warm-up RULE
  timedrun.py rule CAP_S        PREREG :210's warm-up for a run capped at CAP_S seconds, as bbload/clonebench
                                --warmup OPS:S:MAX_S: min(max(1000 ops, 10 s), 10% of the cap)
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


def tracer_rows(path):
    """[(phase, pid, tid, tracerpid)] or None if the record is missing or unreadable."""
    try:
        with open(path) as f:
            out = []
            for ln in f:
                p = ln.split()
                if not p:
                    continue
                if len(p) != 4 or p[0] not in ("start", "end") or not all(x.isdigit() for x in p[1:]):
                    return None
                out.append((p[0], p[1], p[2], int(p[3])))
            return out
    except OSError:
        return None


def rule(cap_s):
    """PREREG :210: warm-up = min(max(1000 ops, 10 s), 10% of the cap), as "OPS:S:MAX_S" for --warmup."""
    m = float(cap_s) / 10
    return "1000:10:%s" % (("%d" % m) if m == int(m) else ("%.3f" % m))


def check(celldir, n, warm_rule=None):
    """The reasons CELLDIR's timed run cannot supply a latency ([] = it can)."""
    why = []
    lab = load(os.path.join(celldir, "bb", "summary.json"))
    if not lab or lab.get("measured_ops") != n:
        why.append(f"labelling run measured {lab.get('measured_ops') if lab else 'nothing'}, not N={n}")
    t = load(os.path.join(celldir, "timed", "summary.json"))
    if t is None or not os.path.exists(os.path.join(celldir, "timed", "raw.tsv")):
        why.append("no timed run (timed/summary.json and timed/raw.tsv): only the traced labelling run's latency")
    else:
        if t.get("verdict") != "ok" or t.get("rc") != 0:
            why.append(f"timed run verdict {t.get('verdict')} rc {t.get('rc')}")
        if t.get("measured_ops") != n:
            why.append(f"timed run measured {t.get('measured_ops')} ops, not N={n} (not the identical command)")
    try:
        rc = open(os.path.join(celldir, "timed.rc")).read().strip()
    except OSError:
        rc = "missing"
    if rc != "0":
        why.append(f"timed run exit status {rc}")
    if warm_rule is not None:  # gate-6 review, t3run item 3: the registered rule, the same for both runs
        for nm, sm in (("labelling", lab), ("timed", t)):
            got = (sm or {}).get("warmup_rule")
            if got != warm_rule:
                why.append(f"{nm} run warm-up rule {got!r}, not the registered {warm_rule!r}")
    rows = tracer_rows(os.path.join(celldir, "timed.tracer.tsv"))
    if rows is None:
        why.append("tracer record timed.tracer.tsv missing or unreadable")
    else:
        for ph in ("start", "end"):
            if not any(r[0] == ph for r in rows):
                why.append(f"no TracerPid sample at the timed run's {ph}")
        traced = [r for r in rows if r[3] != 0]
        if traced:
            why.append(f"traced during the timed run: {traced[:5]}")
    return why


def write_verdict(celldir, n, why):
    out = {"verdict": "ok" if not why else "REFUSED: " + "; ".join(why), "latency_file": "timed/raw.tsv",
           "labelling_dir": "bb", "ops": n}
    with open(os.path.join(celldir, "timed.json"), "w") as f:
        json.dump(out, f)
    return out


# ---------------------------------------------------------------- selftest
RULE = "1000:10:180"  # the registered warm-up at a 1800 s cap


def fixture(root, name, n=200, timed=True, rc=0, timed_ops=None, tracer=None, lab_rule=RULE, timed_rule=RULE):
    d = os.path.join(root, name)
    os.makedirs(os.path.join(d, "bb"))
    with open(os.path.join(d, "bb", "summary.json"), "w") as f:
        json.dump({"verdict": "ok", "rc": 0, "measured_ops": n, "warmup_rule": lab_rule}, f)
    if timed:
        os.makedirs(os.path.join(d, "timed"))
        with open(os.path.join(d, "timed", "summary.json"), "w") as f:
            json.dump({"verdict": "ok" if rc == 0 else "fail", "rc": rc,
                       "measured_ops": n if timed_ops is None else timed_ops, "warmup_rule": timed_rule}, f)
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
        # gate-6 review, t3run item 3: one warm-up rule, PREREG :210's, for both runs
        ("timed run warmed up by another rule", dict(tracer=clean, timed_rule="20:0:0"), False),
        ("labelling run warmed up by another rule", dict(tracer=clean, lab_rule="1000:10:60"), False),
        ("no warm-up rule recorded", dict(tracer=clean, lab_rule=None, timed_rule=None), False),
    ]
    bad = 0
    with tempfile.TemporaryDirectory() as root:
        for i, (name, kw, want) in enumerate(cases):
            d = fixture(root, f"c{i}", **kw)
            why = check(d, 200, RULE)
            got = not why
            print(("PASS" if got == want else "FAIL"), name, "->", "ok" if got else "; ".join(why))
            bad += got != want
    for cap, want in ((1800, "1000:10:180"), (60, "1000:10:6"), (3600, "1000:10:360")):
        got = rule(cap)
        print(("PASS" if got == want else "FAIL"), f"rule({cap}) = {got!r}, want {want!r}")
        bad += got != want
        cases.append(None)
    print(f"timedrun selftest: {len(cases) - bad}/{len(cases)}")
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "check":
        n = int(sys.argv[3])
        why = check(sys.argv[2], n, sys.argv[4])
        print(json.dumps(write_verdict(sys.argv[2], n, why)))
        sys.exit(0 if not why else 1)
    if len(sys.argv) == 3 and sys.argv[1] == "rule":
        print(rule(sys.argv[2]))
        sys.exit(0)
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    sys.exit(__doc__)
