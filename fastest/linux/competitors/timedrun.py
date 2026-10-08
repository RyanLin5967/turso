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
  timedrun.py ops C N1 N4 [TOTAL]
                                the run's ops total: TOTAL (FT_OPS_TOTAL) for every C when given, else N1 at C=1 and
                                N4 otherwise (gate-6 review, t3run item 12). A run capped by the registered window
                                with >= 1000 measured ok ops is complete with reduced n (item 16)
  timedrun.py rule CAP_S        PREREG :210's warm-up for a run capped at CAP_S seconds, as bbload/clonebench
                                --warmup OPS:S:MAX_S: min(max(1000 ops, 10 s), 10% of the cap)
  timedrun.py real CAP_S WARMUP exit 0 only when a REAL run (run_system.sh FT_DRY=0, the T3 runner) may use this cap
                                and warm-up: the registered cap (REGISTERED_CAP_S) and PREREG :210's rule at it. The
                                CI smoke warm-up cap (1000:10:2) is accepted for smoke runs only (lead ruling, artie
                                DECISIONS 6b0bef481b); anything else prints why and exits 2
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


def ops(c, n1, n4, total=""):
    """The run's ops TOTAL (all clients together, as bbload/clonebench --max-ops count them): FT_OPS_TOTAL when set
    (one number for every C and every system), else N1 at C=1 and N4 otherwise; None for a non-count."""
    if total not in ("", None):
        return int(total) if str(total).isdigit() and int(total) > 0 else None
    return int(n1) if int(c) == 1 else int(n4)


CAPPED_MIN = 1000  # PREREG: a run capped by the registered window with >= 1000 measured ops is complete, reduced n
REGISTERED_CAP_S = 1800  # PREREG's per-run cap (30 min; t3run.sh RUN_CAP_S)


def parse_warmup(w):
    """(OPS, S, MAX_S) of an OPS:S:MAX_S warm-up, or None when it is not exactly that (bbload's own grammar)."""
    p = str(w).split(":")
    if len(p) != 3:
        return None
    try:
        return int(p[0]), float(p[1]), float(p[2])
    except ValueError:
        return None


def real_problem(cap_s, warmup):
    """Why a REAL run (run_system.sh FT_DRY=0; the T3 runner) may not use CAP_S and WARMUP; None when it may. The lead's
    ruling (artie DECISIONS 6b0bef481b) accepts the CI smoke warm-up cap for smoke runs only: a real run takes the
    registered cap and PREREG :210's rule at it. A cap equal to its own rule is not enough (rule(20) = 1000:10:2)."""
    why = []
    try:
        cap = float(cap_s)
    except ValueError:
        cap = None
    if cap != REGISTERED_CAP_S:  # NaN and inf are unequal too
        why.append(f"cap {cap_s!r} s is not the registered {REGISTERED_CAP_S} s")
    want = rule(REGISTERED_CAP_S)
    if parse_warmup(warmup) is None or parse_warmup(warmup) != parse_warmup(want):
        why.append(f"warm-up {warmup!r} is not PREREG :210's rule at the registered cap, {want!r}")
    return "; ".join(why) or None


def check(celldir, n, warm_rule=None):
    """The reasons CELLDIR's timed run cannot supply a latency ([] = it can)."""
    why = []
    lab = load(os.path.join(celldir, "bb", "summary.json"))

    def short(sm):  # measured fewer than N: complete only when the registered cap ended it with >= CAPPED_MIN ok ops
        got = (sm or {}).get("measured_ops")
        if got == n:
            return None
        if (sm or {}).get("capped") is True and isinstance(got, int) and (sm or {}).get("measured_ok", 0) >= CAPPED_MIN:
            return None
        return f"measured {got} ops, not N={n}" + (" (capped with fewer than %d ok)" % CAPPED_MIN
                                                   if (sm or {}).get("capped") else "")
    if not lab:
        why.append("no labelling run summary")
    elif short(lab):
        why.append("labelling run " + short(lab))
    t = load(os.path.join(celldir, "timed", "summary.json"))
    if t is None or not os.path.exists(os.path.join(celldir, "timed", "raw.tsv")):
        why.append("no timed run (timed/summary.json and timed/raw.tsv): only the traced labelling run's latency")
    else:
        if t.get("verdict") != "ok" or t.get("rc") != 0:
            why.append(f"timed run verdict {t.get('verdict')} rc {t.get('rc')}")
        if short(t):
            why.append("timed run " + short(t))
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
    lab = load(os.path.join(celldir, "bb", "summary.json")) or {}
    t = load(os.path.join(celldir, "timed", "summary.json")) or {}
    out = {"verdict": "ok" if not why else "REFUSED: " + "; ".join(why), "latency_file": "timed/raw.tsv",
           "labelling_dir": "bb", "ops_total": n,
           "labelling_measured_ops": lab.get("measured_ops"), "timed_measured_ops": t.get("measured_ops"),
           "capped": {"labelling": lab.get("capped", False), "timed": t.get("capped", False)}}
    with open(os.path.join(celldir, "timed.json"), "w") as f:
        json.dump(out, f)
    return out


# ---------------------------------------------------------------- selftest
RULE = "1000:10:180"  # the registered warm-up at a 1800 s cap


def fixture(root, name, n=200, timed=True, rc=0, timed_ops=None, tracer=None, lab_rule=RULE, timed_rule=RULE,
            lab_ops=None, capped=False):
    d = os.path.join(root, name)
    os.makedirs(os.path.join(d, "bb"))
    lo = n if lab_ops is None else lab_ops
    with open(os.path.join(d, "bb", "summary.json"), "w") as f:
        json.dump({"verdict": "ok", "rc": 0, "measured_ops": lo, "measured_ok": lo, "warmup_rule": lab_rule,
                   "capped": capped}, f)
    if timed:
        os.makedirs(os.path.join(d, "timed"))
        to = n if timed_ops is None else timed_ops
        with open(os.path.join(d, "timed", "summary.json"), "w") as f:
            json.dump({"verdict": "ok" if rc == 0 else "fail", "rc": rc, "measured_ops": to, "measured_ok": to,
                       "warmup_rule": timed_rule, "capped": capped}, f)
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
        # gate-6 review, t3run item 16: a run that hit the registered cap with >= 1000 ops is complete with reduced n
        ("both runs capped with >= 1000 ops: complete, reduced n",
         dict(tracer=clean, capped=True, lab_ops=1200, timed_ops=1500), True),
        ("a capped run with < 1000 ops", dict(tracer=clean, capped=True, lab_ops=1200, timed_ops=800), False),
        ("an uncapped run short of N", dict(tracer=clean, lab_ops=1200, timed_ops=1500), False),
    ]
    bad = 0
    with tempfile.TemporaryDirectory() as root:
        for i, (name, kw, want) in enumerate(cases):
            d = fixture(root, f"c{i}", **kw)
            why = check(d, 5000 if "capped" in name or "short of N" in name else 200, RULE)
            got = not why
            print(("PASS" if got == want else "FAIL"), name, "->", "ok" if got else "; ".join(why))
            bad += got != want
    # gate-6 review, t3run item 12: ops is ONE total per run for every system; FT_OPS_TOTAL overrides N1/N4 for all C
    for args, want in (((1, 200, 300, ""), 200), ((4, 200, 300, ""), 300), ((4, 200, 300, "5000"), 5000),
                       ((1, 200, 300, "5000"), 5000), ((1, 200, 300, "x"), None), ((1, 200, 300, "0"), None)):
        got = ops(*args)
        print(("PASS" if got == want else "FAIL"), f"ops{args} = {got!r}, want {want!r}")
        bad += got != want
        cases.append(None)
    for cap, want in ((1800, "1000:10:180"), (60, "1000:10:6"), (3600, "1000:10:360")):
        got = rule(cap)
        print(("PASS" if got == want else "FAIL"), f"rule({cap}) = {got!r}, want {want!r}")
        bad += got != want
        cases.append(None)
    # Lead ruling (artie DECISIONS 6b0bef481b): the CI smoke warm-up cap is for smoke runs only; a REAL run (FT_DRY=0)
    # takes the registered cap and PREREG :210's rule at it, and refuses anything else.
    for cap, warm, want in (("1800", "1000:10:180", True), ("1800", "1000:10:180.0", True),
                            ("1800.0", "1000:10:180", True),
                            ("1800", "1000:10:2", False),     # the CI smoke cap on a real run: the ruling's plant
                            ("20", "1000:10:2", False),       # = rule(20), but 20 s is not the registered cap
                            ("3600", "1000:10:360", False),   # = rule(3600), but not the registered cap
                            ("1800", "20:0:0", False), ("1800", "1000:5:180", False), ("1800", "", False),
                            ("1800", "1000:10", False), ("1800", "1000:10:180:1", False), ("1800", "x:10:180", False),
                            ("", "1000:10:180", False), ("nan", "1000:10:180", False), ("inf", "1000:10:180", False)):
        why = real_problem(cap, warm)
        got = why is None
        print(("PASS" if got == want else "FAIL"), f"real run cap={cap!r} warmup={warm!r} ->",
              "accepted" if got else f"refused: {why}")
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
    if len(sys.argv) in (5, 6) and sys.argv[1] == "ops":
        v = ops(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5] if len(sys.argv) == 6 else "")
        if v is None:
            sys.exit("timedrun.py ops: FT_OPS_TOTAL is not a positive count")
        print(v)
        sys.exit(0)
    if len(sys.argv) == 3 and sys.argv[1] == "rule":
        print(rule(sys.argv[2]))
        sys.exit(0)
    if len(sys.argv) == 4 and sys.argv[1] == "real":
        why = real_problem(sys.argv[2], sys.argv[3])
        print("accepted: the registered cap and warm-up" if why is None else "REFUSED: " + why)
        sys.exit(0 if why is None else 2)
    if len(sys.argv) == 2 and sys.argv[1] == "selftest":
        sys.exit(selftest())
    sys.exit(__doc__)
