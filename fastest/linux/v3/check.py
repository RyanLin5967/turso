#!/usr/bin/env python3
"""check.py OUT FS -- the verdicts of a V3 fire-check, read ONLY from the raw files firecheck.sh wrote under OUT.
FS is the cell's filesystem (ext4|xfs|btrfs), from the workflow matrix, never from the probe's output.

Every expectation below comes from the arm definitions (v3floor.c's header, PREREG §11 M0 exit 1), written here by
hand. None is read from the probe's summary: the summary is a subject, checked against numbers recomputed from
raw.tsv. Writes OUT/verdict.json and prints one PASS/FAIL line per check. Exit 0 all pass, 1 any fail, 2 usage.
"""
import json, os, sys
from collections import Counter

# ---- the spec -----------------------------------------------------------------------------------------------
# Syscalls each arm issues per op (per round, one op per arm), by definition. clone arms: openat (create the clone),
# ioctl(FICLONE), close, fsync of the directory; clone2b also fsyncs the clone; unlinkat is the teardown's, one per
# clone. Every syscall not listed for an arm is DEFINED to be 0 per op (an allowlist: an extra syscall in the loop
# fails, whatever it is).
OP = {
    "append25": {"pwrite64": 1, "fsync": 1},
    "ow4k": {"pwrite64": 1, "fsync": 1},
    "ow64k": {"pwrite64": 1, "fsync": 1},
    "ow1m": {"pwrite64": 1, "fsync": 1},
    "fdatasync4k": {"pwrite64": 1, "fdatasync": 1},
    "clone1b": {"openat": 1, "ioctl": 1, "close": 1, "fsync": 1, "unlinkat": 1},
    "clone2b": {"openat": 1, "ioctl": 1, "close": 1, "fsync": 2, "unlinkat": 1},
    "clean": {"fsync": 1},
    "nosync25": {"pwrite64": 1},
}
# Flushes in each arm's setup (before the loop), by definition: append/nosync init write + fsync; ow*/fdatasync4k/
# clean preallocate + fsync; clone arms preallocate the source + fsync and fsync the new clones' directory.
SETUP_FLUSH = {"append25": 1, "nosync25": 1, "ow4k": 1, "ow64k": 1, "ow1m": 1, "fdatasync4k": 1, "clean": 1,
               "clone1b": 2, "clone2b": 2}  # all fsync
FLUSHED = ["append25", "ow4k", "ow64k", "ow1m", "fdatasync4k", "clone1b", "clone2b"]  # what --mutant-nosync strips
GATED = ["append25", "ow4k", "ow64k", "ow1m", "clone1b", "clone2b"]  # the flush control gates these (rc 3)
CLONES = ["clone1b", "clone2b"]
FLUSH_FAMILY = ["fsync", "fdatasync", "sync", "syncfs", "sync_file_range", "msync"]
INSTRUMENT = "clock_gettime"  # 2 per op when the vDSO does not serve CLOCK_MONOTONIC_RAW; allowed, recorded
ALL = "append25,ow4k,ow64k,ow1m,clone1b,clone2b,clean,fdatasync4k,nosync25"
F1_SETS = ["nosync25", "clean"] + [a + ",nosync25" for a in FLUSHED] + [ALL]
F2_SETS = ["clean"] + [a + ",nosync25" for a in FLUSHED] + [ALL]
NS = [1, 2, 3, 40]
# Refusals: tag -> substring the refusal message must contain (rc 2, and the out dir must not have been made).
REFUSALS = {
    "R_tmpfs": "not ext4, xfs or btrfs", "R_outexists": "must not exist", "R_n0": "usage:",
    "R_nalpha": "not a whole number", "R_ntrail": "not a whole number", "R_unknown": "unknown arm",
    "R_twice": "twice", "R_nod0": "without nosync25", "R_mutnod0": "without nosync25", "R_nice": "nice is 5",
    "R_ionice": "I/O priority", "R_badarg": "bad argument", "R_noout": "usage:",
}

OUT, FS = (sys.argv[1], sys.argv[2]) if len(sys.argv) == 3 else (None, None)
results = []


def check(name, ok, detail):
    results.append({"check": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else ": " + json.dumps(detail)[:900]), flush=True)


def rd(p):
    try:
        with open(p) as f:
            return f.read()
    except OSError:
        return None


def strace_counts(p):
    """strace -c table -> {syscall: (calls, errors)}; None if the file is missing or holds no row."""
    t = rd(p)
    if t is None:
        return None
    c = {}
    for line in t.splitlines():
        tok = line.split()
        if not tok or line.startswith("%") or line.lstrip().startswith("-") or tok[-1] == "total":
            continue
        if len(tok) not in (5, 6):
            continue
        try:
            c[tok[-1]] = (int(tok[3]), int(tok[4]) if len(tok) == 6 else 0)
        except ValueError:
            continue
    return c or None


def raw_rows(p):
    t = rd(p)
    if t is None:
        return None
    lines = t.splitlines()
    if not lines or lines[0] != "arm\ti\tns":
        return None
    rows = {}
    for line in lines[1:]:
        a, i, ns = line.split("\t")
        rows.setdefault(a, []).append((int(i), int(ns)))
    return rows


def pct(v, p):  # the probe's definition: index floor(p*n), clamped
    k = int(p * len(v))
    return v[min(k, len(v) - 1)]


def ran(arms):
    return [a for a in arms if not (FS == "ext4" and a in CLONES)]


def per_round(arms, mutant):
    c = Counter()
    for a in ran(arms):
        spec = OP[a]
        if mutant and a in FLUSHED:
            spec = {k: v for k, v in spec.items() if k not in ("fsync", "fdatasync")}
        c.update(spec)
    return c


def count_mismatches(counts, arms, mutant):
    """Every way the strace counts at NS differ from the definition; [] means exact."""
    bad = []
    exp = per_round(arms, mutant)
    setup = sum(SETUP_FLUSH[a] for a in ran(arms))
    k = len(ran(arms))
    for x, y in zip(NS, NS[1:]):
        cx, cy = counts[x], counts[y]
        for name in set(cx) | set(cy) | set(exp):
            d = cy.get(name, (0, 0))[0] - cx.get(name, (0, 0))[0]
            de = cy.get(name, (0, 0))[1] - cx.get(name, (0, 0))[1]
            want = exp.get(name, 0) * (y - x)
            if name == INSTRUMENT and name not in exp:
                if d not in (0, 2 * k * (y - x)):
                    bad.append(("instrument", name, x, y, d))
            elif d != want:
                bad.append(("per-op", name, x, y, d, want))
            if de != 0:
                bad.append(("errors per op", name, x, y, de))
    for n in NS:
        for name in FLUSH_FAMILY:
            got = counts[n].get(name, (0, 0))
            want = (setup if name == "fsync" else 0) + exp.get(name, 0) * n
            if got[0] != want:
                bad.append(("absolute flush", name, n, got[0], want))
            if got[1]:
                bad.append(("flush errors", name, n, got[1]))
    return bad


def load_set(stage, s, mutant):
    """-> (counts by n, problems) for one arm set at every n."""
    counts, probs = {}, []
    tag0 = s.replace(",", "+") if s != ALL else "all"
    for n in NS:
        base = os.path.join(OUT, stage, "%s.n%d" % (tag0, n))
        rc = rd(base + ".rc")
        c = strace_counts(base + ".strace")
        summ = rd(os.path.join(base + ".out", "summary.json"))
        rows = raw_rows(os.path.join(base + ".out", "raw.tsv"))
        if rc is None or c is None or summ is None or rows is None:
            probs.append(("missing output", n, rc is None, c is None, summ is None, rows is None))
            continue
        rc = int(rc)
        arms = s.split(",")
        any_gated = any(a in GATED for a in ran(arms))
        if rc not in ((0, 3) if any_gated else (0,)):
            probs.append(("rc", n, rc))
        sj = json.loads(summ)
        want_ref = sorted(a for a in arms if a not in ran(arms))
        if sorted(sj.get("refused_arms", {})) != want_ref:
            probs.append(("refused_arms", n, sorted(sj.get("refused_arms", {})), want_ref))
        if sorted(rows) != sorted(ran(arms)) or any(len(v) != n for v in rows.values()):
            probs.append(("raw rows", n, {a: len(v) for a, v in rows.items()}))
        if int(sj.get("mutant_nosync", -1)) != int(mutant):
            probs.append(("mutant flag", n, sj.get("mutant_nosync")))
        counts[n] = c
    return counts, probs


def main():
    if FS not in ("ext4", "xfs", "btrfs"):
        print(__doc__, file=sys.stderr)
        return 2
    info = rd(os.path.join(OUT, "info.txt")) or ""
    check("cell: the work dir is on the cell's filesystem (findmnt, recorded by firecheck.sh)",
          ("work_fstype=%s\n" % FS) in info, {"info": info[-400:]})
    pers = [l.split("=", 1)[1] for l in info.splitlines() if l.startswith("personality_under_setarch_R=")]
    check("cell: setarch -R turns ASLR off for the traced runs (personality has ADDR_NO_RANDOMIZE 0x0040000)",
          len(pers) == 1 and int(pers[0], 16) & 0x0040000 != 0, {"personality": pers})

    # F1: syscalls per op equal each arm's definition, under strace -f -c, at n = 1, 2, 3, 40
    f1 = {}
    for s in F1_SETS:
        counts, probs = load_set("F1", s, False)
        bad = probs or count_mismatches(counts, s.split(","), False)
        f1[s] = counts
        exp = dict(per_round(s.split(","), False))
        check("F1 strace -f -c: per-op syscalls of [%s] equal the definition %s%s" %
              (s, json.dumps(exp, sort_keys=True), " (clone arms refused on ext4)" if FS == "ext4" and
               any(a in CLONES for a in s.split(",")) else ""), not bad, {"bad": bad[:10]})
    # the instrument clock: recorded, either way
    c = f1.get("nosync25", {})
    if 1 in c and 40 in c:
        d = c[40].get(INSTRUMENT, (0, 0))[0] - c[1].get(INSTRUMENT, (0, 0))[0]
        clock_note = "vDSO (0 clock_gettime syscalls per op)" if d == 0 else "%d clock_gettime syscalls per 39 ops" % d
    else:
        clock_note = "unknown (nosync25 counts missing)"

    # F2: the mutant -- strace sees no flush in the flushed arms; clean keeps its fsync
    f2 = {}
    for s in F2_SETS:
        counts, probs = load_set("F2", s, True)
        bad = probs or count_mismatches(counts, s.split(","), True)
        f2[s] = counts
        check("F2 --mutant-nosync, strace -f -c: per-op syscalls of [%s] equal %s" %
              (s, json.dumps(dict(per_round(s.split(","), True)), sort_keys=True)), not bad, {"bad": bad[:10]})
    # F2c: the F1 count check can fire -- the real spec applied to the mutant's counts (and back) must mismatch
    for a in FLUSHED:
        s = a + ",nosync25"
        if FS == "ext4" and a in CLONES:
            check("F2c the count check fires on the %s mutant (n/a: the arm is refused on ext4; F1 shows it ran "
                  "nothing)" % a, True, {})
            continue
        ok = s in f1 and s in f2 and len(f1[s]) == len(NS) and len(f2[s]) == len(NS)
        fires = ok and count_mismatches(f2[s], s.split(","), False) != [] and \
            count_mismatches(f1[s], s.split(","), True) != []
        check("F2c the count check fires: the real %s spec rejects the mutant's counts and vice versa" % a, fires,
              {"have_counts": ok})

    # F2b: the mutant, unwatched, n=200, every arm: the flush control voids it (rc 3)
    f2b = rd(os.path.join(OUT, "F2b.rc"))
    s2 = rd(os.path.join(OUT, "F2b.out", "summary.json"))
    r2 = raw_rows(os.path.join(OUT, "F2b.out", "raw.tsv"))
    s2 = json.loads(s2) if s2 else {}
    mut_ratios = {}
    if r2 and "nosync25" in r2:
        d0 = pct(sorted(ns for _, ns in r2["nosync25"]), .5)
        mut_ratios = {a: round(pct(sorted(ns for _, ns in v), .5) / d0, 2) if d0 else None
                      for a, v in r2.items() if a != "nosync25"}
    check("F2b --mutant-nosync n=200: the flush control fails the run (rc 3) and append25/nosync25 <= 10 from raw",
          f2b is not None and int(f2b) == 3 and str(s2.get("flush_control", "")).startswith("FAIL")
          and mut_ratios.get("append25") is not None and mut_ratios["append25"] <= 10,
          {"rc": f2b, "flush_control": s2.get("flush_control"), "ratios_from_raw": mut_ratios})

    # F3: the real run, n=200, through run.sh
    f3 = check_real(os.path.join(OUT, "F3"), rd(os.path.join(OUT, "F3.rc")))
    disc = f3.get("flush_control") == "pass"
    check("F2/F3 discrimination recorded: F2b's rc 3 %s on this cell (F3's real control: %s)" %
          ("DISCRIMINATES the mutant" if disc else "does NOT discriminate", f3.get("flush_control")), True, {})

    # F4: refusals
    for tag, want in REFUSALS.items():
        rc = rd(os.path.join(OUT, "F4", tag + ".rc"))
        txt = rd(os.path.join(OUT, "F4", tag + ".txt")) or ""
        made = os.path.exists(os.path.join(OUT, "F4", tag + ".out"))
        check("F4 refuse %s -> rc 2, says '%s', no out dir" % (tag, want),
              rc is not None and int(rc) == 2 and want in txt and not made,
              {"rc": rc, "out_dir_made": made, "text": txt[-300:]})
    left = rd(os.path.join(OUT, "work-leftover.txt"))
    check("the work dir is empty after every run (teardown and the ext4 FICLONE trial clean up)",
          left is not None and left.strip() == "", {"leftover": (left or "MISSING")[:400]})

    npass = sum(r["pass"] for r in results)
    v = {"fs": FS, "pass": npass, "total": len(results), "all_pass": npass == len(results) and len(results) > 0,
         "clock": clock_note, "F2b_mutant_ratios_from_raw": mut_ratios, "F3": f3, "checks": results}
    json.dump(v, open(os.path.join(OUT, "verdict.json"), "w"), indent=1)
    print("V3 FIRE-CHECK (%s) %d/%d %s; clock: %s; F3 flush control: %s" %
          (FS, npass, len(results), "PASS" if v["all_pass"] else "FAIL", clock_note, f3.get("flush_control")))
    return 0 if v["all_pass"] else 1


def check_real(o3, rc):
    """F3: the batch is complete and its summary agrees with raw.tsv; the control's verdict is RECORDED, not gated."""
    summ = rd(os.path.join(o3, "summary.json"))
    rows = raw_rows(os.path.join(o3, "raw.tsv"))
    st0 = rd(os.path.join(o3, "stamp_start.json"))
    st1 = rd(os.path.join(o3, "stamp_end.json"))
    n = 200
    arms = ALL.split(",")
    rec = {"rc": rc}
    if summ is None or rows is None:
        check("F3 real run n=200: summary.json and raw.tsv exist", False, rec)
        return rec
    sj = json.loads(summ)
    bad = []
    if sorted(rows) != sorted(ran(arms)):
        bad.append(("arms in raw", sorted(rows), sorted(ran(arms))))
    for a, v in rows.items():
        if sorted(i for i, _ in v) != list(range(n)) or any(ns <= 0 for _, ns in v):
            bad.append(("rows", a, len(v)))
    want_ref = sorted(a for a in arms if a not in ran(arms))
    if sorted(sj.get("refused_arms", {})) != want_ref:
        bad.append(("refused_arms", sorted(sj.get("refused_arms", {})), want_ref))
    for a, why in sj.get("refused_arms", {}).items():
        if "FICLONE" not in why:
            bad.append(("refusal reason does not name the trial FICLONE", a, why))
    p50 = {}
    for a, v in rows.items():
        x = sorted(ns for _, ns in v)
        p50[a] = pct(x, .5)
        mine = {"min_us": x[0], "p1_us": pct(x, .01), "p10_us": pct(x, .1), "p50_us": pct(x, .5),
                "p90_us": pct(x, .9), "p99_us": pct(x, .99), "max_us": x[-1], "mean_us": sum(x) / len(x)}
        theirs = sj.get("arms", {}).get(a, {})
        for k, ns in mine.items():
            if k not in theirs or abs(theirs[k] - ns / 1e3) > 0.051:
                bad.append(("summary disagrees with raw", a, k, theirs.get(k), round(ns / 1e3, 2)))
    ratios = {a: (p50[a] / p50["nosync25"] if p50.get("nosync25") else None) for a in p50 if a != "nosync25"}
    fail = [a for a in GATED if a in ratios and not (ratios[a] > 10)]
    want_rc = 3 if fail else 0
    if rc is None or int(rc) != want_rc:
        bad.append(("rc vs the control recomputed from raw", rc, want_rc, fail))
    if str(sj.get("flush_control", "")).startswith("FAIL") != bool(fail):
        bad.append(("summary flush_control vs raw", sj.get("flush_control"), fail))
    stamps = {}
    if st0 is None or st1 is None:
        bad.append(("stamps missing", st0 is None, st1 is None))
    else:
        e = json.loads(st1)
        if e.get("problems"):
            bad.append(("stamp problems", e["problems"]))
        stamps = {"window_s": e.get("window_s"), "cpu_busy_frac": e.get("cpu_busy_frac"),
                  "loadavg_end": e.get("loadavg"),
                  "flushes_by_device": {d: r.get("flushes") for d, r in (e.get("diskstats_delta") or {}).items()
                                        if r.get("flushes") or r.get("writes")},
                  "write_cache": {d: b.get("write_cache") for d, b in (e.get("block") or {}).items()
                                  if not d.startswith(("ram", "zram"))}}
    rec.update({"flush_control": "pass" if not fail else "FAIL (%s)" % ",".join(fail),
                "ratios_vs_nosync25_from_raw": {a: round(r, 1) for a, r in ratios.items() if r is not None},
                "p50_us_from_raw": {a: round(v / 1e3, 1) for a, v in p50.items()},
                "dirty_clean_p50_ratio": sj.get("dirty_clean_p50_ratio"), "clean_fast_frac": sj.get("clean_fast_frac"),
                "refused_arms": sj.get("refused_arms"), "fstype": sj.get("fstype"), "mount_source": sj.get("mount_source"),
                "ioprio": [sj.get("ioprio_class"), sj.get("ioprio_level")], "stamps": stamps})
    check("F3 real run n=200 via run.sh: complete, summary == raw, rc matches the control recomputed from raw, stamps "
          "taken (the control's verdict is recorded, not gated)", not bad, {"bad": bad[:10]})
    return rec


sys.exit(main())
