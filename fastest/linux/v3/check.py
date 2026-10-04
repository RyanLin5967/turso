#!/usr/bin/env python3
"""check.py OUT FS -- the verdicts of a V3 fire-check, read ONLY from the raw files firecheck.sh wrote under OUT.
FS is the cell (ext4|ext4loop|xfs|btrfs), from the workflow matrix, never from the probe's output.

Every expectation below comes from the arm definitions (v3floor.c's header, PREREG §11 M0 exit 1), written here by
hand. None is read from the probe's summary: the summary is a subject, checked against numbers recomputed from
raw.tsv. The sequence checker is itself fire-checked first: planted breaches in a copy of a real trace must each be
rejected. Writes OUT/verdict.json and prints one PASS/FAIL line per check. Exit 0 all pass, 1 any fail, 2 usage.
"""
import gzip, json, os, re, sys
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
TEARDOWN_FLUSH = 1  # one fsync of D after the unlinks
REC = {"ow4k": 4096, "ow64k": 65536, "ow1m": 1 << 20, "fdatasync4k": 4096}
CAP = {"ow4k": 16 << 20, "ow64k": 16 << 20, "ow1m": 128 << 20, "fdatasync4k": 16 << 20}
FLUSHED = ["append25", "ow4k", "ow64k", "ow1m", "fdatasync4k", "clone1b", "clone2b"]  # what --mutant-nosync strips
GATED = ["append25", "ow4k", "ow64k", "ow1m", "clone1b", "clone2b"]  # the flush control gates these (rc 3)
CLONES = ["clone1b", "clone2b"]
FLUSH_FAMILY = ["fsync", "fdatasync", "sync", "syncfs", "sync_file_range", "msync"]
INSTRUMENT = "clock_gettime"  # 2 per op when the vDSO does not serve CLOCK_MONOTONIC_RAW; allowed, recorded
ALL = "append25,ow4k,ow64k,ow1m,clone1b,clone2b,clean,fdatasync4k,nosync25"
F1_SETS = ["nosync25", "clean"] + [a + ",nosync25" for a in FLUSHED] + [ALL]
F2_SETS = ["clean"] + [a + ",nosync25" for a in FLUSHED] + [ALL]
NS = [1, 2, 3, 40]
SEQ = {"real-all": (ALL, 300, False), "real-4k": ("ow4k,fdatasync4k,nosync25", 4100, False),
       "mutant-all": (ALL, 40, True)}

OUT, FS = (sys.argv[1], sys.argv[2]) if len(sys.argv) == 3 else (None, None)
KIND = "ext4" if FS in ("ext4", "ext4loop") else FS
# Refusals: tag -> substring the message must contain (rc 2, and no out dir made).
REFUSALS = {
    "R_tmpfs": "not ext4, xfs or btrfs", "R_nobarrier": "nobarrier: layer 0", "R_nobarrier_below": "nobarrier: layer 1",
    "R_outexists": "must not exist", "R_n0": "usage:", "R_nalpha": "not a whole number", "R_ntrail": "not a whole number",
    "R_unknown": "unknown arm", "R_twice": "twice", "R_nod0": "without nosync25", "R_mutnod0": "without nosync25",
    "R_nice": "nice is 5", "R_ionice": "I/O priority", "R_schedidle": "scheduling policy",
    "R_schedbatch": "scheduling policy", "R_badarg": "bad argument", "R_noout": "usage:", "R_pathlong": "too long",
    "R_leftover": "every flushed arm" if KIND == "ext4" else "left over",
    "R_runsh_none": "V3_SMOKE=1", "R_runsh_both": "not both", "R_runsh_sha": "v3floor_sha256",
    "R_runsh_fail": "all_pass", "R_runsh_fs": "fstype", "R_runsh_arch": "arch",
}
if KIND == "ext4":
    REFUSALS["X_allclones"] = "every flushed arm"
results = []
W = None


def check(name, ok, detail):
    results.append({"check": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else ": " + json.dumps(detail)[:900]), flush=True)


def rd(p):
    try:
        if p.endswith(".gz"):
            with gzip.open(p, "rt") as f:
                return f.read()
        with open(p) as f:
            return f.read()
    except OSError:
        return None


def rc_of(p):
    t = rd(p)
    try:
        return int(t) if t is not None else None
    except ValueError:
        return None


def ran(arms):
    return [a for a in arms if not (KIND == "ext4" and a in CLONES)]


def all_flushed_refused(arms):
    return any(a in FLUSHED for a in arms) and not any(a in FLUSHED for a in ran(arms))


# ---- strace -c ------------------------------------------------------------------------------------------------
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
    fixed = sum(SETUP_FLUSH[a] for a in ran(arms)) + TEARDOWN_FLUSH
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
            want = (fixed if name == "fsync" else 0) + exp.get(name, 0) * n
            if got[0] != want:
                bad.append(("absolute flush", name, n, got[0], want))
            if got[1]:
                bad.append(("flush errors", name, n, got[1]))
    return bad


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


def counted_set(stage, s, mutant):
    """F1/F2 for one arm set: (counts by n, problems). On ext4 a set whose flushed arms are all clones must refuse."""
    counts, probs = {}, []
    tag0 = s.replace(",", "+") if s != ALL else "all"
    arms = s.split(",")
    for n in NS:
        base = os.path.join(OUT, stage, "%s.n%d" % (tag0, n))
        rc = rc_of(base + ".rc")
        txt = rd(base + ".txt") or ""
        if all_flushed_refused(arms):
            if rc != 2 or "every flushed arm" not in txt or "FICLONE" not in txt or os.path.exists(base + ".out"):
                probs.append(("expected the all-refused rc 2", n, rc, txt[-200:]))
            continue
        c = strace_counts(base + ".strace")
        summ = rd(os.path.join(base + ".out", "summary.json"))
        rows = raw_rows(os.path.join(base + ".out", "raw.tsv"))
        if rc is None or c is None or summ is None or rows is None:
            probs.append(("missing output", n, rc, c is None, summ is None, rows is None))
            continue
        any_gated = any(a in GATED for a in ran(arms))
        if rc not in ((0, 3) if any_gated else (0,)):
            probs.append(("rc", n, rc))
        sj = json.loads(summ)
        want_ref = sorted(a for a in arms if a not in ran(arms))
        if sorted(sj.get("refused_arms", {})) != want_ref:
            probs.append(("refused_arms", n, sorted(sj.get("refused_arms", {})), want_ref))
        if sorted(rows) != sorted(ran(arms)) or any(len(v) != n for v in rows.values()):
            probs.append(("raw rows", n, {a: len(v) for a, v in rows.items()}))
        if int(sj.get("mutant_nosync", -1)) != int(mutant) or int(sj.get("trace_clock", -1)) != 0:
            probs.append(("flags", n, sj.get("mutant_nosync"), sj.get("trace_clock")))
        counts[n] = c
    return counts, probs


# ---- strace -f -y sequences -------------------------------------------------------------------------------------
LINE = re.compile(r"^\d+\s+([a-z0-9_]+)\((.*)\)\s+=\s+(\S+)(.*)$")
FD = re.compile(r"^-?\d+<(.*)>$")
CLOCK = "clock_gettime CLOCK_MONOTONIC_RAW = 0"


def split_args(s):
    out, cur, depth, q, i = [], [], 0, False, 0
    while i < len(s):
        ch = s[i]
        if q:
            cur.append(ch)
            if ch == "\\" and i + 1 < len(s):
                cur.append(s[i + 1])
                i += 2
                continue
            if ch == '"':
                q = False
        elif ch == '"':
            q = True
            cur.append(ch)
        elif ch in "{[<(":
            depth += 1
            cur.append(ch)
        elif ch in "}]>)":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0 and s[i:i + 2] == ", ":
            out.append("".join(cur))
            cur = []
            i += 2
            continue
        else:
            cur.append(ch)
        i += 1
    if cur or out:
        out.append("".join(cur))
    return out


def canon(a):
    a = a.strip()
    m = FD.match(a)
    if m:
        return "<" + m.group(1) + ">"
    if a.startswith('"'):
        return a[1:a.rfind('"')]
    return a


def parse_trace(text):
    """-> list of (name, canonical text) for every syscall line, and the lines that are not syscalls."""
    calls, other = [], []
    for line in text.splitlines():
        m = LINE.match(line)
        if not m:
            other.append(line)
            continue
        name, args, ret = m.group(1), split_args(m.group(2)), canon(m.group(3))
        if name == "pwrite64" and len(args) == 4:
            t = "pwrite64 %s %s %s = %s" % (canon(args[0]), args[2].strip(), args[3].strip(), ret)
        elif name == "clock_gettime":
            t = "clock_gettime %s = %s" % (args[0].strip() if args else "", ret)
        else:
            t = name + " " + " ".join(canon(x) for x in args) + " = " + ret
        calls.append((name, t))
    return calls, other


def expected_op(a, i, mutant):
    f = "%s/%s" % (W, a)
    fl = not mutant
    if a in ("append25", "nosync25"):
        seq = ["pwrite64 <%s> 25 %d = 25" % (f, 25 + 25 * i)]
        if a == "append25" and fl:
            seq.append("fsync <%s> = 0" % f)
    elif a in REC:
        rec, cap = REC[a], CAP[a]
        seq = ["pwrite64 <%s> %d %d = %d" % (f, rec, (i % (cap // rec)) * rec, rec)]
        if fl:
            seq.append("%s <%s> = 0" % ("fdatasync" if a == "fdatasync4k" else "fsync", f))
    elif a == "clean":
        seq = ["fsync <%s> = 0" % f]
    else:
        d, src = "%s/%s.clones" % (W, a), "%s/%s.src" % (W, a)
        c = "%s/c%d" % (d, i)
        seq = ["openat AT_FDCWD %s O_WRONLY|O_CREAT|O_EXCL 0644 = <%s>" % (c, c), "ioctl <%s> FICLONE <%s> = 0" % (c, src)]
        if a == "clone2b" and fl:
            seq.append("fsync <%s> = 0" % c)
        seq.append("close <%s> = 0" % c)
        if fl:
            seq.append("fsync <%s> = 0" % d)
    return seq


def sequence_problems(calls, arms, n, mutant):
    """Every way the loop's syscalls differ from the definition: windows, gaps, rounds. [] means exact."""
    bad = []
    run = ran(arms)
    clocks = [k for k, (name, _) in enumerate(calls) if name == "clock_gettime"]
    odd = [calls[k][1] for k in clocks if calls[k][1] != CLOCK]
    if odd:
        bad.append(("a clock read is not CLOCK_MONOTONIC_RAW = 0", odd[:2]))
    if len(clocks) != 2 * n * len(run):
        bad.append(("clock reads", len(clocks), 2 * n * len(run)))
        return bad
    wins = [[t for _, t in calls[clocks[2 * w] + 1:clocks[2 * w + 1]]] for w in range(n * len(run))]
    for g in range(n * len(run) - 1):
        gap = calls[clocks[2 * g + 1] + 1:clocks[2 * g + 2]]
        if gap:
            bad.append(("syscalls between ops", g, [t for _, t in gap][:3]))
    for r in range(n):
        left = set(run)
        for w in wins[r * len(run):(r + 1) * len(run)]:
            hit = [a for a in left if expected_op(a, r, mutant) == w]
            if len(hit) != 1:
                bad.append(("window matches no remaining arm", r, w[:5], sorted(left)))
                continue
            left.discard(hit[0])
        if len(bad) > 12:
            break
    return bad


def round_orders(calls, arms, n):
    run = ran(arms)
    clocks = [k for k, (name, _) in enumerate(calls) if name == "clock_gettime"]
    orders = set()
    for r in range(n):
        o = []
        for w in range(r * len(run), (r + 1) * len(run)):
            first = calls[clocks[2 * w] + 1][1] if clocks[2 * w] + 1 < clocks[2 * w + 1] else ""
            o.append(next((a for a in run if "/%s>" % a in first or "/%s." % a in first), "?"))
        orders.add(tuple(o))
    return len(orders)


def plants(calls):
    """Planted breaches of a real trace: (name, mutated calls). Each must be rejected by sequence_problems."""
    out = []

    def find(pred):
        return next((k for k, (_, t) in enumerate(calls) if pred(t)), None)

    def with_text(k, t):
        c = list(calls)
        c[k] = (t.split(" ", 1)[0], t)
        return c

    k = find(lambda t: t.startswith("fsync <%s/append25>" % W))
    if k is not None:  # the barrier moved past the closing clock read, out of the timed window
        c = list(calls)
        e = c.pop(k)
        c.insert(k + 1, e)
        out.append(("append25's fsync moved out of its timed window", c))
    k = find(lambda t: t.startswith("fsync <%s/ow1m>" % W))
    if k is not None:
        out.append(("ow1m fsyncs the clean arm's file", with_text(k, "fsync <%s/clean> = 0" % W)))
    k = find(lambda t: t.startswith("pwrite64 <%s/ow4k> 4096 " % W))
    if k is not None:
        out.append(("ow4k writes 25 B", with_text(k, calls[k][1].replace(" 4096 ", " 25 ", 1).replace("= 4096", "= 25"))))
    hits = [j for j, (_, t) in enumerate(calls) if t.startswith("pwrite64 <%s/ow1m> 1048576 %d " % (W, 2 << 20))]
    if len(hits) >= 2:  # i=2 and, after the wrap at 128 MiB, i=130 both write at 2 MiB
        out.append(("ow1m does not wrap at 128 MiB", with_text(hits[1], calls[hits[1]][1].replace(
            " %d = " % (2 << 20), " %d = " % (130 << 20)))))
    k = find(lambda t: t.startswith("fdatasync <%s/fdatasync4k>" % W))
    if k is not None:
        out.append(("fdatasync4k issues fsync instead", with_text(k, "fsync <%s/fdatasync4k> = 0" % W)))
    k = find(lambda t: t.startswith("pwrite64 <%s/nosync25>" % W))
    if k is not None:
        c = list(calls)
        c.insert(k + 1, ("fsync", "fsync <%s/nosync25> = 0" % W))
        out.append(("nosync25 gains a flush", c))
    clocks = [j for j, (name, _) in enumerate(calls) if name == "clock_gettime"]
    if len(clocks) >= 4:
        c = list(calls)
        c.insert(clocks[1] + 1, ("getpid", "getpid = 4242"))
        out.append(("a syscall between two ops", c))
        out.append(("one timed window dropped", calls[:clocks[2]] + calls[clocks[3] + 1:]))
    k = find(lambda t: t.startswith("fsync <%s/clone1b.clones>" % W))
    if k is not None:
        out.append(("clone1b fsyncs its source, not the directory", with_text(k, "fsync <%s/clone1b.src> = 0" % W)))
    k = find(lambda t: t.startswith("ioctl ") and " FICLONE " in t)
    if k is not None:
        out.append(("FICLONERANGE in place of FICLONE", with_text(k, calls[k][1].replace(" FICLONE ", " FICLONERANGE "))))
    k = find(lambda t: t.startswith("fsync <%s/clone2b.clones/c" % W))
    if k is not None:
        c = list(calls)
        c.pop(k)
        out.append(("clone2b skips the clone's own fsync", c))
    return out


# ---- a full batch's summary against its raw.tsv -----------------------------------------------------------------
def summary_vs_raw(sj, rows, n):
    bad, p50 = [], {}
    for a, v in rows.items():
        if sorted(i for i, _ in v) != list(range(n)) or any(ns <= 0 for _, ns in v):
            bad.append(("rows", a, len(v)))
        x = sorted(ns for _, ns in v)
        p50[a] = pct(x, .5)
        mine = {"min_us": x[0], "p1_us": pct(x, .01), "p10_us": pct(x, .1), "p50_us": pct(x, .5),
                "p90_us": pct(x, .9), "p99_us": pct(x, .99), "max_us": x[-1], "mean_us": sum(x) / len(x)}
        theirs = sj.get("arms", {}).get(a, {})
        for k, ns in mine.items():
            if k not in theirs or abs(theirs[k] - ns / 1e3) > 0.051:
                bad.append(("summary disagrees with raw", a, k, theirs.get(k), round(ns / 1e3, 2)))
    d0 = p50.get("nosync25")
    ratios = {a: (p50[a] / d0 if d0 else 1e18) for a in p50 if a != "nosync25"}
    fca = sj.get("flush_control_arms", {})
    for a, r in ratios.items():
        s = fca.get(a, {})
        if abs(s.get("ratio", -1) - r) > 0.051 or s.get("gated") != (a in GATED) or s.get("pass") != (r > 10):
            bad.append(("flush_control_arms disagrees with raw", a, s, round(r, 2)))
    fail = sorted(a for a in GATED if a in ratios and not ratios[a] > 10)
    fc = str(sj.get("flush_control", ""))
    if fail:
        got = fc[fc.find("(") + 1:fc.rfind(")")].split(",") if fc.startswith("FAIL") else []
        if sorted(got) != fail:
            bad.append(("flush_control's failed arms disagree with raw", fc, fail))
    elif fc != "pass":
        bad.append(("flush_control should be pass from raw", fc))
    if "append25" in p50 and d0 and abs(sj.get("flush_d0_p50_ratio", -1) - p50["append25"] / d0) > 0.051:
        bad.append(("flush_d0_p50_ratio", sj.get("flush_d0_p50_ratio"), p50["append25"] / d0))
    if "append25" in p50 and "clean" in p50:
        r = p50["append25"] / p50["clean"]
        fast = sum(1 for _, ns in rows["clean"] if ns < 100000) / n
        if abs(sj.get("dirty_clean_p50_ratio", -1) - r) > 0.051 or abs(sj.get("clean_fast_frac", -1) - fast) > 0.00006:
            bad.append(("dirty/clean fields", sj.get("dirty_clean_p50_ratio"), r, sj.get("clean_fast_frac"), fast))
        if sj.get("dirty_clean_m0") != ("met" if r > 10 else "not met (report only)"):
            bad.append(("dirty_clean_m0", sj.get("dirty_clean_m0")))
    return bad, ratios, fail, p50


def main():
    global W
    if FS not in ("ext4", "ext4loop", "xfs", "btrfs"):
        print(__doc__, file=sys.stderr)
        return 2
    info = rd(os.path.join(OUT, "info.txt")) or ""
    kv = dict(l.split("=", 1) for l in info.splitlines() if "=" in l and not l.startswith(("loop ", "block ")))
    W = kv.get("work", "")
    check("cell: the work dir is on the cell's filesystem type (findmnt)", kv.get("work_fstype") == KIND,
          {"work_fstype": kv.get("work_fstype"), "want": KIND})
    src = kv.get("work_mount", "").split(" ")[0]
    check("cell: %s means %s" % (FS, "a loop device" if FS != "ext4" else "the runner's own disk, no loop"),
          src.startswith("/dev/loop") == (FS != "ext4"), {"source": src})
    pers = kv.get("personality_under_setarch_R", "")
    check("cell: setarch -R turns ASLR off for the traced runs (personality has ADDR_NO_RANDOMIZE 0x0040000)",
          re.fullmatch(r"[0-9a-fA-F]+", pers or "x") is not None and int(pers, 16) & 0x0040000 != 0,
          {"personality": pers})

    # F1: per-op syscall counts under strace -f -c at n = 1, 2, 3, 40
    f1 = {}
    for s in F1_SETS:
        counts, probs = counted_set("F1", s, False)
        arms = s.split(",")
        if all_flushed_refused(arms):
            check("F1 [%s] on ext4: refused at every n (rc 2, the trial FICLONE's reason, no out dir)" % s, not probs,
                  {"bad": probs[:4]})
            continue
        bad = probs or count_mismatches(counts, arms, False)
        f1[s] = counts
        check("F1 strace -f -c: per-op syscalls of [%s] equal the definition %s%s" %
              (s, json.dumps(dict(per_round(arms, False)), sort_keys=True),
               " (clone arms refused on ext4)" if KIND == "ext4" and any(a in CLONES for a in arms) else ""),
              not bad, {"bad": bad[:10]})
    c = f1.get("nosync25", {})
    if 1 in c and 40 in c:
        d = c[40].get(INSTRUMENT, (0, 0))[0] - c[1].get(INSTRUMENT, (0, 0))[0]
        clock_note = "vDSO (0 clock_gettime syscalls per op)" if d == 0 else "%d clock_gettime syscalls per 39 ops" % d
    else:
        clock_note = "unknown (nosync25 counts missing)"

    # F1b / F2d: the exact sequence inside every timed window, and the sequence checker's own fire-check
    traces = {}
    for tag, (s, n, mutant) in SEQ.items():
        base = os.path.join(OUT, "F1b", tag)
        rc = rc_of(base + ".rc")
        text = rd(base + ".trace.gz")
        summ = rd(os.path.join(base + ".out", "summary.json"))
        arms = s.split(",")
        if text is None or summ is None or rc not in (0, 3):
            check("F1b [%s] n=%d: the traced run completed" % (tag, n), False,
                  {"rc": rc, "trace": text is not None, "summary": summ is not None})
            continue
        calls, other = parse_trace(text)
        traces[tag] = calls
        sj = json.loads(summ)
        bad = sequence_problems(calls, arms, n, mutant)
        if int(sj.get("trace_clock", 0)) != 1 or int(sj.get("mutant_nosync", -1)) != int(mutant):
            bad.append(("flags", sj.get("trace_clock"), sj.get("mutant_nosync")))
        sig = [l for l in other if l.startswith("---")]
        if sig:
            bad.append(("signals during the run", sig[:3]))
        check("%s strace -f -y --trace-clock [%s] n=%d: every timed window holds exactly its arm's syscalls on its own "
              "files, sizes and offsets; nothing between ops; each arm once per round (%d distinct round orders)" %
              ("F2d" if mutant else "F1b", tag, n, round_orders(calls, arms, n) if not bad else -1), not bad,
              {"bad": bad[:8]})
    if "real-all" in traces:
        calls = traces["real-all"]
        arms, n = ALL.split(","), SEQ["real-all"][1]
        clean_ok = sequence_problems(calls, arms, n, False) == []
        pl = plants(calls)
        want = 8 if KIND == "ext4" else 11
        check("F1b self-test: %d planted breaches built from the real trace (want %d)" % (len(pl), want),
              len(pl) == want and clean_ok, {"planted": [p for p, _ in pl], "unplanted_trace_passes": clean_ok})
        for name, c in pl:
            check("F1b self-test: the sequence check rejects '%s'" % name, sequence_problems(c, arms, n, False) != [], {})
    if "mutant-all" in traces:
        arms, n = ALL.split(","), SEQ["mutant-all"][1]
        check("F2d the real spec rejects the mutant's trace (the sequence check fires on real mutant data)",
              sequence_problems(traces["mutant-all"], arms, n, False) != [], {})

    # F2: the mutant -- strace sees no flush in the flushed arms; clean keeps its fsync
    f2 = {}
    for s in F2_SETS:
        counts, probs = counted_set("F2", s, True)
        arms = s.split(",")
        if all_flushed_refused(arms):
            check("F2 [%s] on ext4: refused at every n (rc 2)" % s, not probs, {"bad": probs[:4]})
            continue
        bad = probs or count_mismatches(counts, arms, True)
        f2[s] = counts
        check("F2 --mutant-nosync, strace -f -c: per-op syscalls of [%s] equal %s" %
              (s, json.dumps(dict(per_round(arms, True)), sort_keys=True)), not bad, {"bad": bad[:10]})
    for a in FLUSHED:
        s = a + ",nosync25"
        if all_flushed_refused(s.split(",")):
            continue  # refused on ext4: nothing ran, so there is nothing for the count check to tell apart
        ok = s in f1 and s in f2 and len(f1[s]) == len(NS) and len(f2[s]) == len(NS)
        fires = ok and count_mismatches(f2[s], s.split(","), False) != [] and \
            count_mismatches(f1[s], s.split(","), True) != []
        check("F2c the count check fires: the real %s spec rejects the mutant's counts and vice versa" % a, fires,
              {"have_counts": ok})

    # F2b: the mutant, unwatched, n=200, every arm: the flush control voids it (rc 3)
    f2b = rc_of(os.path.join(OUT, "F2b.rc"))
    s2 = rd(os.path.join(OUT, "F2b.out", "summary.json"))
    r2 = raw_rows(os.path.join(OUT, "F2b.out", "raw.tsv"))
    s2 = json.loads(s2) if s2 else {}
    mut_ratios, bad2 = {}, ["missing raw"]
    if r2 and "nosync25" in r2:
        bad2, ratios2, _, _ = summary_vs_raw(s2, r2, 200)
        mut_ratios = {a: round(r, 2) for a, r in ratios2.items()}
    check("F2b --mutant-nosync n=200: the flush control fails the run (rc 3), append25/nosync25 <= 10 from raw, and "
          "the summary agrees with raw", f2b == 3 and str(s2.get("flush_control", "")).startswith("FAIL")
          and mut_ratios.get("append25") is not None and mut_ratios["append25"] <= 10 and not bad2,
          {"rc": f2b, "flush_control": s2.get("flush_control"), "ratios_from_raw": mut_ratios, "bad": bad2[:6]})

    # F3: the real run, n=200, through run.sh; then F2b's discrimination (tools review 1 item 6's fire-check)
    f3 = check_real(os.path.join(OUT, "F3"), rc_of(os.path.join(OUT, "F3.rc")))
    check("F2b discriminates: the same arms and seed without the mutant (F3) do not fail the control (rc 0)",
          f3.get("rc") == 0, {"F3_rc": f3.get("rc"), "F3_control": f3.get("flush_control")})

    # F4: refusals
    for tag, want in REFUSALS.items():
        rc = rc_of(os.path.join(OUT, "F4", tag + ".rc"))
        txt = rd(os.path.join(OUT, "F4", tag + ".txt")) or ""
        o = os.path.join(OUT, "F4", tag + ".out")
        if tag == "R_outexists":
            untouched = os.path.isdir(o) and sorted(os.listdir(o)) == ["sentinel"]
            check("F4 refuse %s -> rc 2, says '%s', the existing dir untouched" % (tag, want),
                  rc == 2 and want in txt and untouched, {"rc": rc, "text": txt[-300:], "untouched": untouched})
            continue
        made = os.path.exists(o) or os.path.exists(o + ".stamp_start.json")
        check("F4 refuse %s -> rc 2, says '%s', no out dir" % (tag, want), rc == 2 and want in txt and not made,
              {"rc": rc, "out_dir_made": made, "text": txt[-300:]})
    if KIND != "ext4":
        rc = rc_of(os.path.join(OUT, "F4", "X_allclones.rc"))
        rows = raw_rows(os.path.join(OUT, "F4", "X_allclones.out", "raw.tsv")) or {}
        check("F4 positive control: on %s the clone arms with nosync25 run (rc 0/3, both clone arms in raw)" % KIND,
              rc in (0, 3) and sorted(rows) == ["clone1b", "clone2b", "nosync25"], {"rc": rc, "arms": sorted(rows)})
    rc = rc_of(os.path.join(OUT, "F4", "P_runsh_ok.rc"))
    b = rd(os.path.join(OUT, "F4", "P_runsh_ok.out", "binary.txt")) or ""
    check("F4 positive control: run.sh accepts a passing verdict for this binary, arch and fs, and records the binding",
          rc in (0, 3) and "bound=fire-checked: " in b and ("v3floor_sha256=%s" % kv.get("v3floor_sha256")) in b,
          {"rc": rc, "binary.txt": b})
    left = rd(os.path.join(OUT, "work-leftover.txt"))
    check("the work dir is empty after every run (teardown and the ext4 FICLONE trial clean up)",
          left is not None and left.strip() == "", {"leftover": (left or "MISSING")[:400]})

    npass = sum(r["pass"] for r in results)
    v = {"cell": FS, "fstype": KIND, "arch": kv.get("arch"), "v3floor_sha256": kv.get("v3floor_sha256"),
         "pass": npass, "total": len(results), "all_pass": npass == len(results) and len(results) > 0,
         "clock": clock_note, "F2b_mutant_ratios_from_raw": mut_ratios, "F3": f3,
         "unplanted_refusals": ["mount table vs statfs magic disagree", "ext4 accepting the trial FICLONE",
                                "a loop's backing file deleted", "more than 4 loop layers"],
         "checks": results}
    json.dump(v, open(os.path.join(OUT, "verdict.json"), "w"), indent=1)
    print("V3 FIRE-CHECK (%s) %d/%d %s; clock: %s; F3 flush control: %s" %
          (FS, npass, len(results), "PASS" if v["all_pass"] else "FAIL", clock_note, f3.get("flush_control")))
    return 0 if v["all_pass"] else 1


def check_real(o3, rc):
    """F3: the batch is complete and its summary agrees with raw.tsv; the control's verdict is RECORDED, not gated."""
    summ = rd(os.path.join(o3, "summary.json"))
    rows = raw_rows(os.path.join(o3, "raw.tsv"))
    st1 = rd(os.path.join(o3, "stamp_end.json"))
    b = rd(os.path.join(o3, "binary.txt")) or ""
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
    want_ref = sorted(a for a in arms if a not in ran(arms))
    if sorted(sj.get("refused_arms", {})) != want_ref:
        bad.append(("refused_arms", sorted(sj.get("refused_arms", {})), want_ref))
    for a, why in sj.get("refused_arms", {}).items():
        if "FICLONE" not in why:
            bad.append(("refusal reason does not name the trial FICLONE", a, why))
    sb, ratios, fail, p50 = summary_vs_raw(sj, rows, n)
    bad += sb
    if rc != (3 if fail else 0):
        bad.append(("rc vs the control recomputed from raw", rc, fail))
    if int(sj.get("mutant_nosync", -1)) != 0 or int(sj.get("trace_clock", -1)) != 0:
        bad.append(("flags", sj.get("mutant_nosync"), sj.get("trace_clock")))
    fp = sj.get("flush_path") or []
    if not fp or fp[0].get("fstype") != KIND or (len(fp) >= 2) != (FS != "ext4"):
        bad.append(("flush_path: the top layer is the cell's fs, and a loop cell is followed to its backing mount",
                    [(l.get("mount"), l.get("fstype"), l.get("source")) for l in fp]))
    if "bound=smoke: V3_SMOKE=1" not in b:
        bad.append(("binary.txt binding", b))
    stamps = {}
    if st1 is None or rd(os.path.join(o3, "stamp_start.json")) is None:
        bad.append(("stamps missing",))
    else:
        e = json.loads(st1)
        if e.get("problems"):
            bad.append(("stamp problems", e["problems"]))
        stamps = {"window_s": e.get("window_s"), "cpu_busy_frac": e.get("cpu_busy_frac"),
                  "loadavg_end": e.get("loadavg"),
                  "flushes_by_device": {d: r.get("flushes") for d, r in (e.get("diskstats_delta") or {}).items()
                                        if r.get("flushes") or r.get("writes")},
                  "write_cache": {d: x.get("write_cache") for d, x in (e.get("block") or {}).items()
                                  if not d.startswith(("ram", "zram"))}}
    rec.update({"flush_control": "pass" if not fail else "FAIL (%s)" % ",".join(fail),
                "ratios_vs_nosync25_from_raw": {a: round(r, 1) for a, r in ratios.items()},
                "p50_us_from_raw": {a: round(v / 1e3, 1) for a, v in p50.items()},
                "dirty_clean_p50_ratio": sj.get("dirty_clean_p50_ratio"), "clean_fast_frac": sj.get("clean_fast_frac"),
                "refused_arms": sj.get("refused_arms"), "fstype": sj.get("fstype"), "mount_source": sj.get("mount_source"),
                "flush_path": [(l.get("mount"), l.get("fstype"), l.get("source")) for l in fp],
                "leaf_write_cache": sj.get("leaf_write_cache"), "leaf_fua": sj.get("leaf_fua"),
                "flush_sent_to_device": sj.get("flush_sent_to_device"),
                "ioprio": [sj.get("ioprio_class"), sj.get("ioprio_level")], "stamps": stamps})
    check("F3 real run n=200 via run.sh: complete, summary == raw, rc matches the control recomputed from raw, flush "
          "path followed, stamps taken (the control's verdict is recorded, not gated)", not bad, {"bad": bad[:10]})
    return rec


sys.exit(main())
