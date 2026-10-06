#!/usr/bin/env python3
"""check.py OUT CELL -- the verdicts of a V3 fire-check, read ONLY from the raw files firecheck.sh wrote under OUT.
CELL is explicit (v3cell.py: ext4|xfs|btrfs on a block device, ext4loop|xfsloop|btrfsloop on a loop), never inferred.

  check.py OUT CELL            write OUT/verdict.json (and OUT/red.json, the base column) and print PASS/FAIL lines
  check.py --bind OUT CELL     after run.sh was bound to OUT/verdict.json (firecheck.sh's last step): write OUT/bind.json
  check.py --plan CELL ARCH LEAF   print the check ids a verdict for that cell, arch and leaf class must hold

Every expectation below comes from the arm definitions (v3floor.c's header, PREREG section 11 M0 exit 1), written
here by hand. None is read from the probe's summary: the summary is a subject, checked against numbers recomputed
from raw.tsv. The sequence checker is itself fire-checked first: planted breaches in a copy of a real trace must each
be rejected. The checks a verdict holds are fixed in advance by plan(cell, arch, leaf class): a verdict whose ids
differ from its plan fails, and run.sh refuses to bind one. Exit 0 all pass, 1 any fail, 2 usage.
"""
import gzip, hashlib, json, os, re, sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import v3cell  # noqa: E402

# ---- the spec -----------------------------------------------------------------------------------------------
# Syscalls each arm issues per op (per round, one op per arm), by definition. Copy arms: openat (create the clone in
# the clones' directory), the copy (ioctl FICLONE or copy_file_range), close, fsync of the directory; clone2b and
# cfr2b also fsync the clone; unlinkat is the teardown's, one per clone. Every syscall not listed for an arm is DEFINED
# to be 0 per op (an allowlist: an extra syscall in the loop fails, whatever it is).
OP = {
    "append25": {"pwrite64": 1, "fsync": 1},
    "append64": {"pwrite64": 1, "fsync": 1},
    "ow4k": {"pwrite64": 1, "fsync": 1},
    "ow64k": {"pwrite64": 1, "fsync": 1},
    "ow1m": {"pwrite64": 1, "fsync": 1},
    "fdatasync4k": {"pwrite64": 1, "fdatasync": 1},
    "clone1b": {"openat": 1, "ioctl": 1, "close": 1, "fsync": 1, "unlinkat": 1},
    "clone2b": {"openat": 1, "ioctl": 1, "close": 1, "fsync": 2, "unlinkat": 1},
    "cfr2b": {"openat": 1, "copy_file_range": 1, "close": 1, "fsync": 2, "unlinkat": 1},
    "clean": {"fsync": 1},
    "nosync25": {"pwrite64": 1},
}
# Flushes in each arm's setup (before the loop), by definition: append/nosync init write + fsync; ow*/fdatasync4k/
# clean preallocate + fsync; copy arms preallocate the source + fsync and fsync the new clones' directory.
SETUP_FLUSH = {"append25": 1, "append64": 1, "nosync25": 1, "ow4k": 1, "ow64k": 1, "ow1m": 1, "fdatasync4k": 1,
               "clean": 1, "clone1b": 2, "clone2b": 2, "cfr2b": 2}  # all fsync
TEARDOWN_FLUSH = 1  # one fsync of D after the unlinks
APPEND = {"append25": 25, "append64": 64, "nosync25": 25}
APPEND_BASE = 4096  # the append arms' files start one 4 KiB block long (setup), so op i writes at 4096 + rec * i
REC = {"ow4k": 4096, "ow64k": 65536, "ow1m": 1 << 20, "fdatasync4k": 4096}
CAP = {"ow4k": 16 << 20, "ow64k": 16 << 20, "ow1m": 128 << 20, "fdatasync4k": 16 << 20}
FLUSHED = ["append25", "append64", "ow4k", "ow64k", "ow1m", "fdatasync4k", "clone1b", "clone2b", "cfr2b"]
GATED = ["append25", "append64", "ow4k", "ow64k", "ow1m", "clone2b", "cfr2b"]  # the flush control gates these (rc 3)
CLONES = ["clone1b", "clone2b"]  # FICLONE: refused on ext4
COPIES = ["clone1b", "clone2b", "cfr2b"]
FLUSH_FAMILY = ["fsync", "fdatasync", "sync", "syncfs", "sync_file_range", "msync"]
INSTRUMENT = "clock_gettime"  # 2 per op when the vDSO does not serve CLOCK_MONOTONIC_RAW; allowed, recorded
ALL = "append25,append64,ow4k,ow64k,ow1m,clone1b,clone2b,cfr2b,clean,fdatasync4k,nosync25"
F1_SETS = ["nosync25", "clean"] + [a + ",nosync25" for a in FLUSHED] + [ALL]
F2_SETS = ["clean"] + [a + ",nosync25" for a in FLUSHED] + [ALL]
NS = [1, 2, 3, 40]
SEQ = {"real-all": (ALL, 300, False), "real-4k": ("ow4k,fdatasync4k,nosync25", 4100, False),
       "mutant-all": (ALL, 40, True)}
# The frame arm (review 2 item 11; PREREG section 4: the smallest bytes per flush >= the M1 build's median create
# frame among the M0 append and overwrite arms). A named create's flight is about 56-60 B (review 2 item 11; not
# measured here), so the frame arm must be an append of at least 60 B and less than a 4 KiB page.
FRAME_ARM = "append64"
M1_FLIGHT_MAX_B = 60
CLOCKSOURCES = ["tsc", "arch_sys_counter"]
# A flush an op issues itself shows in EVERY window of its arm (append25 reads 1.00 per op on write-back NVMe, run
# 37475543956). The leaf drive of a hosted runner is shared with the whole system (its root filesystem's jbd2
# commits, other processes' fsyncs), so a foreign flush request can land in an occasional window: run 37475543956's
# x86 ext4 cell saw one jbd2 commit (1 flush + 1 FUA) inside 1 of nosync25's 200 windows. On that shared drive a
# no-flush arm may therefore show a request in at most this share of its windows (self-issued would be 100%);
# devices private to the cell (its loop devices, brd) and write-through devices are held to exactly 0.
SHARED_MAX_FRAC = 0.05
LEAF_DRIVERS = ["nvme", "sd", "virtio_blk"]
# crash.sh cases per filesystem kind: (case, arm, flags, rule). "survive" and "lost" are gated (the controls and the
# rig's own fire-check); "record" cases are recorded with their prediction (review 2 item 3), never gated.
CRASH = {
    "xfs": [("clone2b", "survive"), ("cfr2b", "survive"), ("clone1b", "record"), ("clone1b-aim", "record"),
            ("clone2b-mutant", "lost")],
    "btrfs": [("clone2b", "survive"), ("cfr2b", "survive"), ("clone1b", "record"), ("clone2b-mutant", "lost")],
    "ext4": [("cfr2b", "survive"), ("cfr2b-mutant", "lost")],
}
CRASH_PREDICT = {"xfs/clone1b": "unknown", "xfs/clone1b-aim": "lost (review 2: the aimed fsync forces the create "
                 "out, the FICLONE lands in a later checkpoint the directory fsync does not force)",
                 "btrfs/clone1b": "lost (review 2: size 0)"}
# Probe refusals: tag -> substring the message must contain (rc 2, and no out dir made). Cell-independent ones.
REFUSALS = {
    "R_tmpfs": "not ext4, xfs or btrfs", "R_nobarrier": "nobarrier: layer 0", "R_nobarrier_below": "nobarrier: layer 1",
    "R_outexists": "must not exist", "R_n0": "usage:", "R_nalpha": "not a whole number", "R_ntrail": "not a whole number",
    "R_unknown": "unknown arm", "R_twice": "twice", "R_nod0": "without nosync25", "R_mutnod0": "without nosync25",
    "R_nice": "nice is 5", "R_ionice": "I/O priority", "R_schedidle": "scheduling policy",
    "R_schedbatch": "scheduling policy", "R_badarg": "bad argument", "R_noout": "usage:", "R_pathlong": "too long",
    "R_mutant_noenv": "for the fire-check only", "R_traceclock_noenv": "for the fire-check only",
    "R_crashop_noenv": "for the fire-check only",
    "R_symlink": "left over", "R_leftover_file": "left over",
    "R_hidden_tmpfs": "not ext4, xfs or btrfs", "R_hidden_nobarrier": "nobarrier: layer 0",
    "R_lazy": "not the loop's backing inode", "R_deleted": "cannot read a live backing file",
    "R_nest4": "more than 3 nested loop devices (4 layers)", "R_statfs_shim": "but statfs magic",
    "R_dirsync": "not in the known-safe list", "R_logdev": "external log", "R_extjournal": "no internal journal",
    "R_multidev": "multi-device btrfs", "R_loop_wt": "above the leaf", "R_brd": "brd is fire-check only",
    "R_driver": "not in the leaf allowlist",
    "R_chattr": "outside the allowlist (extents, directory index)", "R_ldpreload": "LD_PRELOAD is set",
}
# run.sh refusals: tag -> the refusing rule's own reason PREFIX (rc 2, no out dir, no probe run). Every planted
# verdict also carries "planted", so a bare word ("plan", "cell", "arch") would match whatever rule fired (fresh
# review H2): each want is the start of its own rule's reason in batchgate.verdict_problems.
RUNSH = {
    "R_runsh_none": "V3_SMOKE=1", "R_runsh_both": "not both", "R_runsh_sha": "v3floor_sha256: the verdict is for",
    "R_runsh_fail": "all_pass is not true", "R_runsh_fs": "fstype: the verdict is for", "R_runsh_arch": "arch: the verdict is for",
    "R_runsh_mutant": "not allowed through run.sh", "R_runsh_traceclock": "not allowed through run.sh",
    "R_runsh_crashop": "not allowed through run.sh", "R_runsh_crashaim": "not allowed through run.sh",
    "R_runsh_dir": "not allowed through run.sh", "R_runsh_out": "not allowed through run.sh",
    "R_runsh_n": "not allowed through run.sh", "R_runsh_fcenv": "V3FLOOR_FIRECHECK is set", "R_runsh_brdenv": "V3FLOOR_BRD is set",
    "R_runsh_nocell": "V3_CELL=''", "R_runsh_badcell": "V3_CELL='bogus'", "R_runsh_planted": "planted: a fixture verdict",
    "R_runsh_shape": "pass/total:", "R_runsh_count": "plan: the check ids differ", "R_runsh_cellv": "cell: the verdict is for",
    "R_runsh_harness": "harness: the verdict was made by", "R_runsh_ldpreload": "LD_PRELOAD is set",
    "R_runsh_t3": "T3 preconditions do not hold",
}
# the probe ran; run.sh refused after it (rc 2): tag -> reasons that must all appear
RUNSH_POST = {"R_runsh_post": ["exe_sha256: the probe that ran", "mutant_nosync=1"],
              "R_runsh_nogated": ["no gated arm ran"], "R_runsh_gatecrash": ["the batch gate failed"]}
# batchgate.py post on a copy of the F3 batch with one field planted (bound mode): tag -> its reason prefix
POST_PLANTS = {"R_post_traceclock": "trace_clock=1 in the summary", "R_post_cell": "layout:",
               "R_post_leaf": "leaf class: the batch's leaf is", "R_post_brd": "leaf brd:"}
HARNESS = ["run.sh", "batchgate.py", "check.py", "blkflush.py", "stamp.py", "v3cell.py", "firecheck.sh", "crash.sh",
           "mkfixtures.sh", "mkbrd.sh", "red.py"]


def harness_sha256(here=HERE):
    out = {}
    for f in HARNESS:
        try:
            out[f] = hashlib.sha256(open(os.path.join(here, f), "rb").read()).hexdigest()
        except OSError:
            out[f] = None
    return out


def kind_of(cell):
    return v3cell.kind(cell)


def ran(arms, kind):
    return [a for a in arms if not (kind == "ext4" and a in CLONES)]


def all_flushed_refused(arms, kind):
    return any(a in FLUSHED for a in arms) and not any(a in FLUSHED for a in ran(arms, kind))


def tagof(s):
    return "all" if s == ALL else s.replace(",", "+")


def plant_names(kind):
    p = ["append25's fsync moved out of its timed window", "ow1m fsyncs the clean arm's file", "ow4k writes 25 B",
         "ow1m does not wrap at 128 MiB", "fdatasync4k issues fsync instead", "nosync25 gains a flush",
         "a syscall between two ops", "one timed window dropped", "append64 writes at append25's offset",
         "cfr2b copies 4096 B", "cfr2b skips the clone's own fsync",
         "nosync25 gains a split fsync (<unfinished ...> / <... resumed>)"]
    if kind != "ext4":
        p += ["clone1b fsyncs its source, not the directory", "FICLONERANGE in place of FICLONE",
              "clone2b skips the clone's own fsync"]
    return p


def plan(cell, arch, leaf):
    """The check ids a verdict for (cell, arch, leaf class wb|wt|brd) holds, in order. Pure: the spec only."""
    k = kind_of(cell)
    ids = ["cell:fstype", "cell:source", "cell:layers", "cell:aslr", "cell:leaf"]
    for s in F1_SETS:
        ids.append("F1:%s%s" % (tagof(s), ":refused" if all_flushed_refused(s.split(","), k) else ""))
    ids += ["F1b:real-all", "F1b:real-4k", "F2d:mutant-all", "F1b:selftest:count"]
    ids += ["F1b:plant:" + p for p in plant_names(k)]
    ids.append("F2d:fires")
    for s in F2_SETS:
        ids.append("F2:%s%s" % (tagof(s), ":refused" if all_flushed_refused(s.split(","), k) else ""))
    for a in FLUSHED:
        if not all_flushed_refused([a, "nosync25"], k):
            ids.append("F2c:" + a)
    ids += ["F2b", "F3:complete", "F3:devflush", "F3:merge", "F3:gate", "F3:record"]
    if leaf != "brd":
        ids.append("F2b:discriminates")
    ids.append("frame:append")
    if v3cell.is_loop(cell):
        ids += ["C:%s" % c for c, _ in CRASH[k]]
    ids += ["B:selftest", "B:fsync", "B:quiet", "B:overflow", "B:misuse", "S:batchgate"]
    for t in REFUSALS:
        ids.append("F4:" + t)
    ids.append("F4:P_nest3")
    ids.append("F4:R_ficlone_accept" if k == "ext4" else "F4:X_allclones")
    ids.append("F4:R_leftover")
    if leaf != "brd":
        ids.append("F4:R_leaf_flip")
    if arch == "x86_64":
        ids.append("F4:R_clocksource")
    for t in RUNSH:
        ids.append("F4:" + t)
    for t in RUNSH_POST:
        ids.append("F4:" + t)
    for t in POST_PLANTS:
        ids.append("F4:" + t)
    ids.append("work:empty")
    return ids


# ---- state ----------------------------------------------------------------------------------------------------
OUT = CELL = KIND = W = None
results = []


def check(cid, ok, detail, desc=""):
    results.append({"id": cid, "check": desc or cid, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + cid + (" -- " + desc if desc else "") + ("" if ok else ": " + json.dumps(detail)[:900]),
          flush=True)


def rd(p):
    try:
        if p.endswith(".gz"):
            with gzip.open(p, "rt") as f:
                return f.read()
        with open(p) as f:
            return f.read()
    except OSError:
        return None


def rj(p):
    t = rd(p)
    try:
        return json.loads(t) if t is not None else None
    except ValueError:
        return None


def rc_of(p):
    t = rd(p)
    try:
        return int(t) if t is not None else None
    except ValueError:
        return None


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
    for a in ran(arms, KIND):
        spec = OP[a]
        if mutant and a in FLUSHED:
            spec = {k: v for k, v in spec.items() if k not in ("fsync", "fdatasync")}
        c.update(spec)
    return c


def count_mismatches(counts, arms, mutant):
    """Every way the strace counts at NS differ from the definition; [] means exact."""
    bad = []
    exp = per_round(arms, mutant)
    fixed = sum(SETUP_FLUSH[a] for a in ran(arms, KIND)) + TEARDOWN_FLUSH
    k = len(ran(arms, KIND))
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
    """raw.tsv -> {arm: [(i, ns, t0_ns)]}; None if missing or not the 4-column format."""
    t = rd(p)
    if t is None:
        return None
    lines = t.splitlines()
    if not lines or lines[0] != "arm\ti\tns\tt0_ns":
        return None
    rows = {}
    for line in lines[1:]:
        a, i, ns, t0 = line.split("\t")
        rows.setdefault(a, []).append((int(i), int(ns), int(t0)))
    return rows


def pct(v, p):  # the probe's definition: index floor(p*n), clamped
    k = int(p * len(v))
    return v[min(k, len(v) - 1)]


def counted_set(stage, s, mutant):
    """F1/F2 for one arm set: (counts by n, problems). On ext4 a set whose flushed arms are all clones must refuse."""
    counts, probs = {}, []
    arms = s.split(",")
    for n in NS:
        base = os.path.join(OUT, stage, "%s.n%d" % (tagof(s), n))
        rc = rc_of(base + ".rc")
        txt = rd(base + ".txt") or ""
        if all_flushed_refused(arms, KIND):
            if rc != 2 or "every flushed arm" not in txt or "FICLONE" not in txt or os.path.exists(base + ".out"):
                probs.append(("expected the all-refused rc 2", n, rc, txt[-200:]))
            continue
        c = strace_counts(base + ".strace")
        sj = rj(os.path.join(base + ".out", "summary.json"))
        rows = raw_rows(os.path.join(base + ".out", "raw.tsv"))
        if rc is None or c is None or sj is None or rows is None:
            probs.append(("missing output", n, rc, c is None, sj is None, rows is None, txt[-200:]))
            continue
        any_gated = any(a in GATED for a in ran(arms, KIND))
        if rc not in ((0, 3) if any_gated else (0,)):
            probs.append(("rc", n, rc))
        want_ref = sorted(a for a in arms if a not in ran(arms, KIND))
        if sorted(sj.get("refused_arms", {})) != want_ref:
            probs.append(("refused_arms", n, sorted(sj.get("refused_arms", {})), want_ref))
        if sorted(rows) != sorted(ran(arms, KIND)) or any(len(v) != n for v in rows.values()):
            probs.append(("raw rows", n, {a: len(v) for a, v in rows.items()}))
        if int(sj.get("mutant_nosync", -1)) != int(mutant) or int(sj.get("trace_clock", -1)) != 0:
            probs.append(("flags", n, sj.get("mutant_nosync"), sj.get("trace_clock")))
        counts[n] = c
    return counts, probs


# ---- strace -f -y sequences -------------------------------------------------------------------------------------
LINE = re.compile(r"^(\d+)\s+([a-z0-9_]+)\((.*)\)\s+=\s+(\S+)(.*)$")
EXITED = re.compile(r"^(\d+)\s+\+\+\+ exited with (\d+) \+\+\+$")
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
    if a.startswith("AT_FDCWD<"):  # strace -y decorates AT_FDCWD with the cwd (run 37245436013)
        return "AT_FDCWD"
    m = FD.match(a)
    if m:
        return "<" + m.group(1) + ">"
    if a.startswith('"'):
        return a[1:a.rfind('"')]
    return a


def parse_trace(text):
    """-> (calls, other, pids): every syscall line as (name, canonical text), the lines that are not syscalls, and the
    pids seen. strace 6.8 prints FICLONE as "BTRFS_IOC_CLONE or FICLONE" (one ioctl number) with its source fd as a
    bare integer (run 37245436013), so the source is resolved to its path from the fd that an earlier call returned."""
    calls, other, fds, pids = [], [], {}, set()
    for line in text.splitlines():
        m = LINE.match(line)
        if not m or "<unfinished ...>" in line or "resumed>" in line:
            other.append(line)
            continue
        pids.add(m.group(1))
        name, args, ret = m.group(2), split_args(m.group(3)), canon(m.group(4))
        r = FD.match(m.group(4))
        if r:
            fds[m.group(4).split("<", 1)[0]] = r.group(1)
        if name == "pwrite64" and len(args) == 4:
            t = "pwrite64 %s %s %s = %s" % (canon(args[0]), args[2].strip(), args[3].strip(), ret)
        elif name == "clock_gettime":
            t = "clock_gettime %s = %s" % (args[0].strip() if args else "", ret)
        elif name == "ioctl" and len(args) == 3:
            cmd = args[1].strip()
            cmd = "FICLONE" if "FICLONE" in cmd.split(" or ") else cmd
            src = args[2].strip()
            src = "<%s>" % fds[src] if re.fullmatch(r"\d+", src) and src in fds else canon(src)
            t = "ioctl %s %s %s = %s" % (canon(args[0]), cmd, src, ret)
        else:
            t = name + " " + " ".join(canon(x) for x in args) + " = " + ret
        calls.append((name, t))
    return calls, other, pids


def other_problems(other, pids, rc):
    """Review 2 item 14: the only non-syscall line a single-threaded probe's trace may hold is one exit line, from the
    one pid every syscall line carries, with the run's rc."""
    bad = []
    ex = [EXITED.match(l) for l in other]
    if len(other) != 1 or not ex[0]:
        bad.append(("lines that are not whole syscalls (a split <unfinished ...> line, a signal, a second pid)",
                    [l[:120] for l in other if not EXITED.match(l)][:4], len(other)))
    elif len(pids) != 1 or ex[0].group(1) not in pids:
        bad.append(("pids", sorted(pids)[:4], ex[0].group(1)))
    elif rc is not None and int(ex[0].group(2)) != rc:
        bad.append(("exit status", ex[0].group(2), rc))
    if len(pids) > 1:
        bad.append(("more than one pid", sorted(pids)[:4]))
    return bad


def expected_op(a, i, mutant):
    f = "%s/%s" % (W, a)
    fl = not mutant
    if a in APPEND:
        r = APPEND[a]
        seq = ["pwrite64 <%s> %d %d = %d" % (f, r, APPEND_BASE + r * i, r)]
        if a != "nosync25" and fl:
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
        seq = ["openat <%s> c%d O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW 0644 = <%s>" % (d, i, c)]
        if a == "cfr2b":
            seq.append("copy_file_range <%s> [0] <%s> NULL 1048576 0 = 1048576" % (src, c))
        else:
            seq.append("ioctl <%s> FICLONE <%s> = 0" % (c, src))
        if a in ("clone2b", "cfr2b") and fl:
            seq.append("fsync <%s> = 0" % c)
        seq.append("close <%s> = 0" % c)
        if fl:
            seq.append("fsync <%s> = 0" % d)
    return seq


def sequence_problems(calls, arms, n, mutant, other=None, pids=None, rc=None):
    """Every way the loop's syscalls differ from the definition: windows, gaps, rounds, stray lines. [] means exact."""
    bad = []
    if other is not None:
        bad += other_problems(other, pids or set(), rc)
    run = ran(arms, KIND)
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
    run = ran(arms, KIND)
    clocks = [k for k, (name, _) in enumerate(calls) if name == "clock_gettime"]
    orders = set()
    for r in range(n):
        o = []
        for w in range(r * len(run), (r + 1) * len(run)):
            first = calls[clocks[2 * w] + 1][1] if clocks[2 * w] + 1 < clocks[2 * w + 1] else ""
            o.append(next((a for a in run if "/%s>" % a in first or "/%s." % a in first), "?"))
        orders.add(tuple(o))
    return len(orders)


def plants(calls, text):
    """Planted breaches of a real trace: (name, calls, other). Each must be rejected by sequence_problems."""
    out = []
    clocks = [j for j, (name, _) in enumerate(calls) if name == "clock_gettime"]
    lo, hi = (clocks[0], clocks[-1]) if clocks else (0, -1)

    def find(pred):  # inside the loop only: a planted breach in setup or teardown is outside what the check covers
        return next((k for k in range(lo, hi) if pred(calls[k][1])), None)

    def with_text(k, t):
        c = list(calls)
        c[k] = (t.split(" ", 1)[0], t)
        return c

    k = find(lambda t: t.startswith("fsync <%s/append25>" % W))
    if k is not None:  # the barrier moved past the closing clock read, out of the timed window
        c = list(calls)
        e = c.pop(k)
        c.insert(k + 1, e)
        out.append(("append25's fsync moved out of its timed window", c, None))
    k = find(lambda t: t.startswith("fsync <%s/ow1m>" % W))
    if k is not None:
        out.append(("ow1m fsyncs the clean arm's file", with_text(k, "fsync <%s/clean> = 0" % W), None))
    k = find(lambda t: t.startswith("pwrite64 <%s/ow4k> 4096 " % W))
    if k is not None:
        out.append(("ow4k writes 25 B", with_text(k, calls[k][1].replace(" 4096 ", " 25 ", 1).replace("= 4096", "= 25")), None))
    hits = [j for j in range(lo, hi) if calls[j][1].startswith("pwrite64 <%s/ow1m> 1048576 %d " % (W, 2 << 20))]
    if len(hits) >= 2:  # in the loop, i=2 and, after the wrap at 128 MiB, i=130 both write at 2 MiB
        out.append(("ow1m does not wrap at 128 MiB", with_text(hits[1], calls[hits[1]][1].replace(
            " %d = " % (2 << 20), " %d = " % (130 << 20))), None))
    k = find(lambda t: t.startswith("fdatasync <%s/fdatasync4k>" % W))
    if k is not None:
        out.append(("fdatasync4k issues fsync instead", with_text(k, "fsync <%s/fdatasync4k> = 0" % W), None))
    k = find(lambda t: t.startswith("pwrite64 <%s/nosync25>" % W))
    if k is not None:
        c = list(calls)
        c.insert(k + 1, ("fsync", "fsync <%s/nosync25> = 0" % W))
        out.append(("nosync25 gains a flush", c, None))
    if len(clocks) >= 4:
        c = list(calls)
        c.insert(clocks[1] + 1, ("getpid", "getpid = 4242"))
        out.append(("a syscall between two ops", c, None))
        out.append(("one timed window dropped", calls[:clocks[2]] + calls[clocks[3] + 1:], None))
    k = find(lambda t: t.startswith("pwrite64 <%s/append64> 64 " % W))
    if k is not None:  # 64 B at append25's offset pattern (4096 + 25 i) instead of 4096 + 64 i
        parts = calls[k][1].split(" ")
        i = (int(parts[3]) - APPEND_BASE) // 64
        out.append(("append64 writes at append25's offset",
                    with_text(k, "pwrite64 <%s/append64> 64 %d = 64" % (W, APPEND_BASE + 25 * (i + 1))), None))
    k = find(lambda t: t.startswith("copy_file_range <%s/cfr2b.src>" % W))
    if k is not None:
        out.append(("cfr2b copies 4096 B", with_text(k, calls[k][1].replace(" 1048576 0 = 1048576", " 4096 0 = 4096")), None))
    k = find(lambda t: t.startswith("fsync <%s/cfr2b.clones/c" % W))
    if k is not None:
        c = list(calls)
        c.pop(k)
        out.append(("cfr2b skips the clone's own fsync", c, None))
    # item 14: a flush hidden in a split line. Text level: the parser must not drop it silently.
    lines = text.splitlines()
    j = next((x for x, l in enumerate(lines) if re.match(r"^\d+\s+pwrite64\(\d+<%s/nosync25>" % re.escape(W), l)), None)
    if j is not None:
        pid = lines[j].split()[0]
        fd = re.match(r"^\d+\s+pwrite64\((\d+)<", lines[j]).group(1)
        planted = lines[:j + 1] + ["%s fsync(%s<%s/nosync25> <unfinished ...>" % (pid, fd, W),
                                   "%s <... fsync resumed>) = 0" % pid] + lines[j + 1:]
        c2, o2, p2 = parse_trace("\n".join(planted))
        out.append(("nosync25 gains a split fsync (<unfinished ...> / <... resumed>)", c2, (o2, p2)))
    if KIND != "ext4":
        k = find(lambda t: t.startswith("fsync <%s/clone1b.clones>" % W))
        if k is not None:
            out.append(("clone1b fsyncs its source, not the directory", with_text(k, "fsync <%s/clone1b.src> = 0" % W), None))
        k = find(lambda t: t.startswith("ioctl ") and " FICLONE " in t)
        if k is not None:
            out.append(("FICLONERANGE in place of FICLONE", with_text(k, calls[k][1].replace(" FICLONE ", " FICLONERANGE ")), None))
        k = find(lambda t: t.startswith("fsync <%s/clone2b.clones/c" % W))
        if k is not None:
            c = list(calls)
            c.pop(k)
            out.append(("clone2b skips the clone's own fsync", c, None))
    return out


# ---- a full batch's summary against its raw.tsv -----------------------------------------------------------------
def summary_vs_raw(sj, rows, n):
    bad, p50 = [], {}
    wins = []
    for a, v in rows.items():
        if sorted(i for i, _, _ in v) != list(range(n)) or any(ns <= 0 for _, ns, _ in v):
            bad.append(("rows", a, len(v)))
        wins += [(t0, t0 + ns, a, i) for i, ns, t0 in v]
        x = sorted(ns for _, ns, _ in v)
        p50[a] = pct(x, .5)
        mine = {"min_us": x[0], "p1_us": pct(x, .01), "p10_us": pct(x, .1), "p50_us": pct(x, .5),
                "p90_us": pct(x, .9), "p99_us": pct(x, .99), "max_us": x[-1], "mean_us": sum(x) / len(x)}
        q = len(v) // 4
        if q >= 2:  # stationarity: the first and last quarter of the arm's ops, in op order
            by_i = [ns for _, ns, _ in sorted(v)]
            mine["p50_q1_us"] = pct(sorted(by_i[:q]), .5)
            mine["p50_q4_us"] = pct(sorted(by_i[-q:]), .5)
        theirs = sj.get("arms", {}).get(a, {})
        for k, ns in mine.items():
            if k not in theirs or abs(theirs[k] - ns / 1e3) > 0.051:
                bad.append(("summary disagrees with raw", a, k, theirs.get(k), round(ns / 1e3, 2)))
    wins.sort()
    for x, y in zip(wins, wins[1:]):
        if y[0] < x[1]:
            bad.append(("op windows overlap", x, y))
            break
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
        fast = sum(1 for _, ns, _ in rows["clean"] if ns < 100000) / n
        if abs(sj.get("dirty_clean_p50_ratio", -1) - r) > 0.051 or abs(sj.get("clean_fast_frac", -1) - fast) > 0.00006:
            bad.append(("dirty/clean fields", sj.get("dirty_clean_p50_ratio"), r, sj.get("clean_fast_frac"), fast))
        if sj.get("dirty_clean_m0") != ("met" if r > 10 else "not met (report only)"):
            bad.append(("dirty_clean_m0", sj.get("dirty_clean_m0")))
    return bad, ratios, fail, p50


def leaf_class_of(sj):
    if not sj or not isinstance(sj.get("leaf"), dict):
        return None
    if sj["leaf"].get("kind") == "brd":
        return "brd"
    wc = sj["leaf"].get("write_cache")
    return "wb" if wc == "write back" else "wt" if wc == "write through" else None


def find_leaf_class():
    """The leaf class from the F3 batch, else from F1's first run (every probe run on the work dir sees one leaf)."""
    for p in (os.path.join(OUT, "F3", "summary.json"), os.path.join(OUT, "F1", "nosync25.n1.out", "summary.json")):
        c = leaf_class_of(rj(p))
        if c:
            return c, p
    return None, None


# ---- main -------------------------------------------------------------------------------------------------------
def main(argv):
    global OUT, CELL, KIND, W
    if len(argv) == 4 and argv[0] == "--plan":
        if argv[1] not in v3cell.CELLS:
            return 2
        print("\n".join(plan(argv[1], argv[2], argv[3])))
        return 0
    if len(argv) == 3 and argv[0] == "--bind":
        return bind(argv[1], argv[2])
    if len(argv) != 2 or argv[1] not in v3cell.CELLS:
        print(__doc__, file=sys.stderr)
        return 2
    OUT, CELL = argv
    KIND = kind_of(CELL)
    info = rd(os.path.join(OUT, "info.txt")) or ""
    kv = dict(l.split("=", 1) for l in info.splitlines() if "=" in l and not l.startswith(("loop ", "block ")))
    W = kv.get("work", "")
    arch = kv.get("arch", "")
    leaf, leaf_src = find_leaf_class()
    the_plan = plan(CELL, arch, leaf or "unknown")

    # cell: the explicit layout (review 2 item 4)
    check("cell:fstype", kv.get("work_fstype") == KIND, {"work_fstype": kv.get("work_fstype"), "want": KIND},
          "the work dir is on the cell's filesystem type (findmnt)")
    src = kv.get("work_mount", "").split(" ")[0]
    check("cell:source", src.startswith("/dev/") and src.startswith("/dev/loop") == v3cell.is_loop(CELL), {"source": src, "cell": CELL},
          "cell %s means %s" % (CELL, "a loop device" if v3cell.is_loop(CELL) else "a block device, no loop"))
    f3 = rj(os.path.join(OUT, "F3", "summary.probe.json")) or rj(os.path.join(OUT, "F3", "summary.json")) or {}
    lp = v3cell.layout_problems(CELL, f3.get("fstype"), f3.get("mount_source"), f3.get("flush_path"))
    check("cell:layers", not lp, {"problems": lp}, "the F3 batch's flush path has the cell's layer count and source")
    pers = kv.get("personality_under_setarch_R", "")
    check("cell:aslr", re.fullmatch(r"[0-9a-fA-F]+", pers or "x") is not None and int(pers, 16) & 0x0040000 != 0,
          {"personality": pers}, "setarch -R turns ASLR off for the traced runs (ADDR_NO_RANDOMIZE 0x0040000)")
    lf = f3.get("leaf") or {}
    lbad = []
    if leaf is None:
        lbad.append("no leaf class (no F3 or F1 summary)")
    elif leaf == "brd":
        if v3cell.is_loop(CELL) or kv.get("brd_cell") != "1":
            lbad.append("a brd leaf outside a brd cell")
        if lf.get("creditable") is not False:
            lbad.append("a brd leaf not marked creditable false")
    else:
        if lf.get("driver") not in LEAF_DRIVERS:
            lbad.append(("driver", lf.get("driver")))
        if lf.get("drive_reports") != lf.get("write_cache"):
            lbad.append(("drive report vs kernel", lf.get("drive_reports"), lf.get("write_cache")))
        if kv.get("brd_cell") == "1":
            lbad.append("a brd cell whose leaf is not brd")
    check("cell:leaf", not lbad, {"leaf": lf, "class": leaf, "from": leaf_src, "bad": lbad},
          "the leaf is an allowlisted drive whose own cache report agrees with the kernel (or brd on a brd cell)")

    # F1: per-op syscall counts under strace -f -c at n = 1, 2, 3, 40
    f1 = {}
    for s in F1_SETS:
        counts, probs = counted_set("F1", s, False)
        arms = s.split(",")
        if all_flushed_refused(arms, KIND):
            check("F1:%s:refused" % tagof(s), not probs, {"bad": probs[:4]},
                  "[%s] on ext4: refused at every n (rc 2, the trial FICLONE's reason, no out dir)" % s)
            continue
        bad = probs or count_mismatches(counts, arms, False)
        f1[s] = counts
        check("F1:%s" % tagof(s), not bad, {"bad": bad[:10]},
              "strace -f -c: per-op syscalls of [%s] equal the definition %s" % (s, json.dumps(dict(per_round(arms, False)), sort_keys=True)))
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
        sj = rj(os.path.join(base + ".out", "summary.json"))
        arms = s.split(",")
        cid = ("F2d:" if mutant else "F1b:") + tag
        if text is None or sj is None or rc not in (0, 3):
            check(cid, False, {"rc": rc, "trace": text is not None, "summary": sj is not None}, "the traced run completed")
            continue
        calls, other, pids = parse_trace(text)
        traces[tag] = (calls, other, pids, text, rc)
        bad = sequence_problems(calls, arms, n, mutant, other, pids, rc)
        if int(sj.get("trace_clock", 0)) != 1 or int(sj.get("mutant_nosync", -1)) != int(mutant):
            bad.append(("flags", sj.get("trace_clock"), sj.get("mutant_nosync")))
        check(cid, not bad, {"bad": bad[:8]},
              "strace -f -y --trace-clock [%s] n=%d: every timed window holds exactly its arm's syscalls on its own files, "
              "sizes and offsets; nothing between ops; each arm once per round (%d distinct round orders); the only other "
              "line is the one exit" % (tag, n, round_orders(calls, arms, n) if not bad else -1))
    if "real-all" in traces:
        calls, other, pids, text, rc = traces["real-all"]
        arms, n = ALL.split(","), SEQ["real-all"][1]
        clean_ok = sequence_problems(calls, arms, n, False, other, pids, rc) == []
        pl = plants(calls, text)
        want = plant_names(KIND)
        check("F1b:selftest:count", [p for p, _, _ in pl] == want and clean_ok,
              {"planted": [p for p, _, _ in pl], "want": want, "unplanted_trace_passes": clean_ok},
              "%d planted breaches built from the real trace (want %d)" % (len(pl), len(want)))
        got = {name: (c, o) for name, c, o in pl}
        for name in want:
            if name not in got:
                check("F1b:plant:" + name, False, "not planted", "the sequence check rejects '%s'" % name)
                continue
            c, o = got[name]
            oth, pp = o if o else (other, pids)
            check("F1b:plant:" + name, sequence_problems(c, arms, n, False, oth, pp, rc) != [], {},
                  "the sequence check rejects '%s'" % name)
    else:
        check("F1b:selftest:count", False, "no real-all trace", "planted breaches built from the real trace")
        for name in plant_names(KIND):
            check("F1b:plant:" + name, False, "no real-all trace", "the sequence check rejects '%s'" % name)
    if "mutant-all" in traces:
        calls, other, pids, _, rc = traces["mutant-all"]
        check("F2d:fires", sequence_problems(calls, ALL.split(","), SEQ["mutant-all"][1], False, other, pids, rc) != [], {},
              "the real spec rejects the mutant's trace (the sequence check fires on real mutant data)")
    else:
        check("F2d:fires", False, "no mutant trace", "the real spec rejects the mutant's trace")

    # F2: the mutant -- strace sees no flush in the flushed arms; clean keeps its fsync
    f2 = {}
    for s in F2_SETS:
        counts, probs = counted_set("F2", s, True)
        arms = s.split(",")
        if all_flushed_refused(arms, KIND):
            check("F2:%s:refused" % tagof(s), not probs, {"bad": probs[:4]}, "[%s] on ext4: refused at every n (rc 2)" % s)
            continue
        bad = probs or count_mismatches(counts, arms, True)
        f2[s] = counts
        check("F2:%s" % tagof(s), not bad, {"bad": bad[:10]},
              "--mutant-nosync, strace -f -c: per-op syscalls of [%s] equal %s" % (s, json.dumps(dict(per_round(arms, True)), sort_keys=True)))
    for a in FLUSHED:
        s = a + ",nosync25"
        if all_flushed_refused(s.split(","), KIND):
            continue
        ok = s in f1 and s in f2 and len(f1[s]) == len(NS) and len(f2[s]) == len(NS)
        fires = ok and count_mismatches(f2[s], s.split(","), False) != [] and count_mismatches(f1[s], s.split(","), True) != []
        check("F2c:" + a, fires, {"have_counts": ok}, "the count check fires: the real %s spec rejects the mutant's counts and vice versa" % a)

    # F2b: the mutant, unwatched, n=200, every arm: the flush control voids it (rc 3)
    f2b = rc_of(os.path.join(OUT, "F2b.rc"))
    s2 = rj(os.path.join(OUT, "F2b.out", "summary.json")) or {}
    r2 = raw_rows(os.path.join(OUT, "F2b.out", "raw.tsv"))
    mut_ratios, bad2 = {}, ["missing raw"]
    if r2 and "nosync25" in r2:
        bad2, ratios2, _, _ = summary_vs_raw(s2, r2, 200)
        mut_ratios = {a: round(r, 2) for a, r in ratios2.items()}
    check("F2b", f2b == 3 and str(s2.get("flush_control", "")).startswith("FAIL") and mut_ratios.get("append25") is not None
          and mut_ratios["append25"] <= 10 and not bad2,
          {"rc": f2b, "flush_control": s2.get("flush_control"), "ratios_from_raw": mut_ratios, "bad": bad2[:6]},
          "--mutant-nosync n=200: the flush control fails the run (rc 3), append25/nosync25 <= 10 from raw, summary == raw")

    # F3: the real run, n=200, through run.sh (stamps, blkflush, the batch gate)
    f3rec = check_real(os.path.join(OUT, "F3"), rc_of(os.path.join(OUT, "F3.rc")), kv, leaf)
    if leaf != "brd":
        check("F2b:discriminates", f3rec.get("probe_rc") == 0, {"F3_probe_rc": f3rec.get("probe_rc"), "F3_control": f3rec.get("flush_control")},
              "the same arms and seed without the mutant (F3) do not fail the control (probe rc 0)")
    # the D0 threshold's separation on this cell (review 2 item 17: re-derived from the first T3 fire-check)
    real_r = f3rec.get("ratios_vs_nosync25_from_raw") or {}
    gm = [mut_ratios[a] for a in GATED if a in mut_ratios]
    gr = [real_r[a] for a in GATED if a in real_r]
    d0 = {"rule": "a threshold t separates when max(mutant gated ratio) < t < min(real gated ratio); the candidate is "
                  "their geometric mean; registered from the first T3 fire-check before registration",
          "max_mutant_gated": max(gm) if gm else None, "min_real_gated": min(gr) if gr else None}
    if gm and gr:
        d0["separates"] = max(gm) < min(gr)
        d0["candidate"] = round((max(gm) * min(gr)) ** 0.5, 2) if d0["separates"] else None
        d0["threshold_10_separates"] = max(gm) < 10 < min(gr)

    # frame arm (item 11)
    fbad = []
    if FRAME_ARM not in APPEND or FRAME_ARM == "nosync25":
        fbad.append("the frame arm is not a flushed append")
    elif not (M1_FLIGHT_MAX_B <= APPEND[FRAME_ARM] < 4096):
        fbad.append(("bytes", APPEND[FRAME_ARM], M1_FLIGHT_MAX_B))
    if f3.get("frame_arm") != FRAME_ARM or f3.get("frame_bytes") != APPEND.get(FRAME_ARM):
        fbad.append(("summary", f3.get("frame_arm"), f3.get("frame_bytes")))
    if FRAME_ARM not in (f3rec.get("p50_us_from_raw") or {}):
        fbad.append("the frame arm did not run in F3")
    check("frame:append", not fbad, {"bad": fbad},
          "the frame arm is an append of >= %d B (the M1 median flight) and < 4 KiB, it ran in F3, and the summary names it" % M1_FLIGHT_MAX_B)

    # C: crash arms (item 3) on the loop cells
    crash = {}
    if v3cell.is_loop(CELL):
        for case, rule in CRASH[KIND]:
            r = rj(os.path.join(OUT, "crash", case + ".json"))
            res = (r or {}).get("result")
            crash[case] = {"rule": rule, "result": res, "predicted": CRASH_PREDICT.get("%s/%s" % (KIND, case), rule),
                           "detail": r}
            # the clone existed (1 MiB) before the crash, and the crash itself is shown by a sentinel written after the
            # crash point that is absent after the remount (fresh reviews: I-L1, B-L10)
            ok = r is not None and r.get("src_ok") is True and r.get("clone_size_before") == "1048576" and \
                r.get("crash_proven") is True and (
                res == "survived" if rule == "survive" else res == "lost" if rule == "lost" else res in ("survived", "lost"))
            check("C:" + case, ok, {"record": r},
                  "crash after one %s op (%s): %s" % (case, "xfs_io shutdown, no log flush" if KIND != "btrfs" else
                                                      "dm-flakey drop_writes", {"survive": "the control survives (size and "
                                                      "bytes equal the source)", "lost": "the no-flush mutant is lost (the rig "
                                                      "crashes)", "record": "recorded, predicted %s" % crash[case]["predicted"]}[rule]))

    # B: blkflush.py's own fire-check
    bt = rd(os.path.join(OUT, "B", "selftest.txt")) or ""
    m = re.search(r"BLKFLUSH SELF-TEST (\d+)/(\d+) PASS\s*$", bt)
    check("B:selftest", rc_of(os.path.join(OUT, "B", "selftest.rc")) == 0 and m is not None and m.group(1) == m.group(2)
          and int(m.group(2)) > 0, {"tail": bt[-300:], "rc": rc_of(os.path.join(OUT, "B", "selftest.rc"))},
          "blkflush.py self-test: the parser and the window attribution on planted text (rc 0, n/n with n > 0)")
    br = rj(os.path.join(OUT, "B", "report.json")) or {}
    lp_dev = (rd(os.path.join(OUT, "B", "loopdev.txt")) or "").strip().replace("/dev/", "")
    arms = (br.get("windows") or {}).get("arms") or {}
    fs = arms.get("devfsync", {})
    d = (fs.get("devices") or {}).get(lp_dev, {})
    check("B:fsync", fs.get("ops") == 50 and d.get("events") == 50 and d.get("zero_windows") == 0 and d.get("max_in_window") == 1
          and (d.get("by_kind") or {}).get("flush") == 50,
          {"loop": lp_dev, "devfsync": fs, "refused": br.get("refused")},
          "50 fsync(2)s of a raw write-back loop device: exactly one flush request issued to it in each window")
    rootd = (kv.get("root_disk") or "").split(" ")[0]
    qbad, qrec = [], {}
    for a in ("devwrite", "idle"):
        r = arms.get(a, {})
        if r.get("ops") != 50:
            qbad.append((a, "ops", r.get("ops")))
        for dev, x in (r.get("devices") or {}).items():
            hit = r.get("ops", 50) - x.get("zero_windows", 50)
            qrec["%s/%s" % (a, dev)] = hit
            if dev == lp_dev or (dev == rootd and hit > SHARED_MAX_FRAC * r.get("ops", 50)):
                qbad.append((a, dev, hit))
    ambd = (br.get("windows") or {}).get("ambiguous_by_device") or {}
    lptotal = ((br.get("devices") or {}).get(lp_dev) or {}).get("total")
    check("B:quiet", not qbad and not ambd.get(lp_dev) and lp_dev != "" and lptotal == 50,
          {"bad": qbad, "windows_hit": qrec, "ambiguous_by_device": ambd, "root_disk": rootd, "loop_total": lptotal},
          "50 buffered writes and 50 empty windows: no flush request on the loop, at most %d%% of windows with a foreign "
          "one on the shared root drive (others' devices recorded); no event at a window edge on the loop"
          % int(SHARED_MAX_FRAC * 100))
    ov = rj(os.path.join(OUT, "B", "overflow.json")) or {}
    check("B:overflow", rc_of(os.path.join(OUT, "B", "overflow.rc")) == 2 and "lost" in str(ov.get("refused", "")),
          {"rc": rc_of(os.path.join(OUT, "B", "overflow.rc")), "report": ov},
          "a 4 KiB ring buffer overrun by 3000 flushes: report refuses (events lost), never a short count")
    mbad = [(t, rc_of(os.path.join(OUT, "B", t + ".rc"))) for t in ("stop_unstarted", "start_twice")
            if rc_of(os.path.join(OUT, "B", t + ".rc")) != 2]
    check("B:misuse", not mbad, {"bad": mbad}, "stop without start and start into an existing record both refuse (rc 2)")
    st = rd(os.path.join(OUT, "B", "batchgate-selftest.txt")) or ""
    m = re.search(r"BATCHGATE SELF-TEST (\d+)/(\d+) PASS\s*$", st)
    check("S:batchgate", rc_of(os.path.join(OUT, "B", "batchgate-selftest.rc")) == 0 and m is not None
          and m.group(1) == m.group(2) and int(m.group(2)) > 0, {"tail": st[-400:]},
          "batchgate.py self-test in this cell (the flush gate on the banked cells, every verdict-shape refusal by its "
          "own reason, the T3 rule): the binding's rules are tested in the verdict that relies on them")

    # F4: refusals
    for tag, want in REFUSALS.items():
        refusal(tag, want)
    rc = rc_of(os.path.join(OUT, "F4", "P_nest3.rc"))
    nj = rj(os.path.join(OUT, "F4", "P_nest3.out", "summary.json")) or {}
    check("F4:P_nest3", rc in (0, 3) and nj.get("layers") == 4 and nj.get("loop_layers") == 3,
          {"rc": rc, "layers": nj.get("layers"), "text": (rd(os.path.join(OUT, "F4", "P_nest3.txt")) or "")[-300:]},
          "3 nested loops (4 layers) are followed to the leaf and accepted")
    if KIND == "ext4":
        rc = rc_of(os.path.join(OUT, "F4", "R_ficlone_accept.rc"))
        txt = rd(os.path.join(OUT, "F4", "R_ficlone_accept.txt")) or ""
        made = os.path.exists(os.path.join(OUT, "F4", "R_ficlone_accept.out"))
        left = rd(os.path.join(OUT, "F4", "R_ficlone_accept.left"))
        left = "MISSING" if left is None else left.strip()  # an empty file is the pass case
        check("F4:R_ficlone_accept", rc == 1 and "ACCEPTED a trial FICLONE" in txt and not made and left == "",
              {"rc": rc, "text": txt[-300:], "out_made": made, "work_left": left},
              "strace inject makes ext4's trial FICLONE succeed: rc 1 'ACCEPTED a trial FICLONE', no out dir, empty work dir")
    else:
        rc = rc_of(os.path.join(OUT, "F4", "X_allclones.rc"))
        rows = raw_rows(os.path.join(OUT, "F4", "X_allclones.out", "raw.tsv")) or {}
        check("F4:X_allclones", rc in (0, 3) and sorted(rows) == ["cfr2b", "clone1b", "clone2b", "nosync25"],
              {"rc": rc, "arms": sorted(rows)}, "on %s the copy arms with nosync25 run (rc 0/3, all in raw)" % KIND)
    refusal("R_leftover", "every flushed arm" if KIND == "ext4" else "left over")
    if leaf != "brd":  # wb: the kernel's write_cache disabled; wt sd: sd's "temporary write back" (fresh review H2)
        refusal("R_leaf_flip", "but the drive reports")
    if arch == "x86_64":
        refusal("R_clocksource", "the clocksource is")
    for tag, want in RUNSH.items():
        refusal(tag, want, runsh=True)
    for tag, wants in RUNSH_POST.items():
        rc = rc_of(os.path.join(OUT, "F4", tag + ".rc"))
        txt = rd(os.path.join(OUT, "F4", tag + ".txt")) or ""
        check("F4:" + tag, rc == 2 and all(w in txt for w in wants), {"rc": rc, "text": txt[-400:]},
              "run.sh refuses after the run (rc 2), for its own reason: %s" % wants)
    for tag, want in POST_PLANTS.items():
        rc = rc_of(os.path.join(OUT, "F4", tag + ".rc"))
        g = rj(os.path.join(OUT, "F4", tag, "gate.json")) or {}
        ok = rc == 2 and any(str(r).startswith(want) for r in g.get("refusals") or [])
        check("F4:" + tag, ok, {"rc": rc, "refusals": g.get("refusals")},
              "batchgate.py post on a copy of the F3 batch with one planted field refuses (rc 2) with '%s'" % want)
    left = rd(os.path.join(OUT, "work-leftover.txt"))
    check("work:empty", left is not None and left.strip() == "", {"leftover": (left or "MISSING")[:400]},
          "the work dir is empty after every run (teardown and the ext4 FICLONE trial clean up)")

    # the plan: exactly these ids, in this order (run.sh binds only a verdict whose ids equal its plan)
    ids = [r["id"] for r in results]
    if ids != the_plan:
        missing = [i for i in the_plan if i not in ids]
        extra = [i for i in ids if i not in the_plan]
        for i in missing:
            check(i, False, "never evaluated", "in the plan, not evaluated")
        results.append({"id": "plan", "check": "the checks evaluated equal the plan", "pass": False,
                        "detail": {"missing": missing, "extra": extra}})
        print("FAIL plan: missing %s extra %s" % (missing[:5], extra[:5]))
    npass = sum(r["pass"] for r in results)
    v = {"cell": CELL, "fstype": KIND, "arch": arch, "leaf_class": leaf, "v3floor_sha256": kv.get("v3floor_sha256"),
         "run_id": kv.get("run_id"), "utc": kv.get("utc"), "pass": npass, "total": len(results),
         "all_pass": npass == len(results) and len(results) > 0 and ids == the_plan, "clock": clock_note,
         "F2b_mutant_ratios_from_raw": mut_ratios, "F3": f3rec, "d0_threshold_derivation": d0, "crash": crash,
         "t3_positive": {"rule_holds_on_this_box": kv.get("t3_rule_holds"),
                         "P_runsh_t3_rc": rc_of(os.path.join(OUT, "F4", "P_runsh_t3.rc")),
                         "note": "recorded, not a check: only some runners expose cpufreq on 'performance'"},
         "unplanted_refusals": unplanted(arch, leaf), "harness_sha256": harness_sha256(), "checks": results}
    with open(os.path.join(OUT, "verdict.json"), "w") as f:
        json.dump(v, f, indent=1)
    red(kv, leaf, arch)
    print("V3 FIRE-CHECK (%s, %s, leaf %s) %d/%d %s; clock: %s; F3 flush control: %s" %
          (CELL, arch, leaf, npass, len(results), "PASS" if v["all_pass"] else "FAIL", clock_note, f3rec.get("flush_control")))
    return 0 if v["all_pass"] else 1


def unplanted(arch, leaf):
    u = ["a mount whose mountinfo line is malformed or too long", "statx returning no mount id",
         "an unreadable /proc/fs/ext4 options file or /proc/fs/jbd2", "a SCSI or virtio cache_type that cannot be read "
         "or parsed", "NVMe controllers of one subsystem disagreeing on VWC", "a brd leaf whose write_cache is not "
         "write-through", "the drive's report unreadable (a closed NVMe or sd node, MODE SENSE failing or without a "
         "caching page; the CI grants read access)", "inode flags unreadable", "D's mount id changing between the "
         "lookup and the run"]
    if leaf == "brd":
        u.append("the kernel's write_cache disagreeing with the drive (this cell's leaf is brd)")
    if arch != "x86_64":
        u.append("a clocksource other than tsc/arch_sys_counter (arm64 runners offer only arch_sys_counter)")
    return u


def refusal(tag, want, runsh=False):
    rc = rc_of(os.path.join(OUT, "F4", tag + ".rc"))
    txt = rd(os.path.join(OUT, "F4", tag + ".txt")) or ""
    o = os.path.join(OUT, "F4", tag + ".out")
    if tag == "R_outexists":
        untouched = os.path.isdir(o) and sorted(os.listdir(o)) == ["sentinel"]
        check("F4:" + tag, rc == 2 and want in txt and untouched, {"rc": rc, "text": txt[-300:], "untouched": untouched},
              "refuse -> rc 2, says '%s', the existing dir untouched" % want)
        return
    made = os.path.exists(o) or os.path.exists(o + ".stamp_start.json") or os.path.exists(o + ".blkflush")
    extra = {}
    ok = rc == 2 and want in txt and not made
    if tag == "R_symlink":  # the planted symlink's target must not have been created
        tgt = rd(os.path.join(OUT, "F4", "R_symlink.target")) or "MISSING"
        extra["target_after"] = tgt.strip()
        ok = ok and tgt.strip() == "absent"
    check("F4:" + tag, ok, dict({"rc": rc, "out_dir_made": made, "text": txt[-300:]}, **extra),
          "%s refuses -> rc 2, says '%s', no out dir" % ("run.sh" if runsh else "the probe", want))


def check_real(o3, rc, kv, leaf):
    """F3: the batch through run.sh: probe output vs raw, the device flush record, the merge, the gate, the record."""
    sj = rj(os.path.join(o3, "summary.probe.json"))
    merged = rj(os.path.join(o3, "summary.json"))
    rows = raw_rows(os.path.join(o3, "raw.tsv"))
    st0, st1 = rj(os.path.join(o3, "stamp_start.json")), rj(os.path.join(o3, "stamp_end.json"))
    b = rd(os.path.join(o3, "binary.txt")) or ""
    rcl = rd(os.path.join(o3, "rc")) or ""
    m = re.search(r"probe_rc=(\d+)", rcl)
    probe_rc = int(m.group(1)) if m else None
    n, arms = 200, ALL.split(",")
    rec = {"rc": rc, "probe_rc": probe_rc}
    if sj is None or rows is None:
        for cid in ("F3:complete", "F3:devflush", "F3:merge", "F3:gate", "F3:record"):
            check(cid, False, dict(rec, rc_file=rcl), "F3 real run n=200: summary.probe.json and raw.tsv exist")
        return rec
    bad = []
    if sorted(rows) != sorted(ran(arms, KIND)):
        bad.append(("arms in raw", sorted(rows), sorted(ran(arms, KIND))))
    want_ref = sorted(a for a in arms if a not in ran(arms, KIND))
    if sorted(sj.get("refused_arms", {})) != want_ref:
        bad.append(("refused_arms", sorted(sj.get("refused_arms", {})), want_ref))
    for a, why in sj.get("refused_arms", {}).items():
        if "FICLONE" not in why:
            bad.append(("refusal reason does not name the trial FICLONE", a, why))
    sb, ratios, fail, p50 = summary_vs_raw(sj, rows, n)
    bad += sb
    if probe_rc != (3 if fail else 0):
        bad.append(("probe rc vs the control recomputed from raw", probe_rc, fail))
    if int(sj.get("mutant_nosync", -1)) != 0 or int(sj.get("trace_clock", -1)) != 0:
        bad.append(("flags", sj.get("mutant_nosync"), sj.get("trace_clock")))
    lp = v3cell.layout_problems(CELL, sj.get("fstype"), sj.get("mount_source"), sj.get("flush_path"))
    if lp:
        bad.append(("layout", lp))
    if "bound=smoke: V3_SMOKE=1" not in b or ("cell=%s" % CELL) not in b:
        bad.append(("binary.txt binding", b))
    if st0 is None or st1 is None:
        bad.append(("stamps missing",))
    elif st1.get("problems"):
        bad.append(("stamp problems", st1["problems"]))
    check("F3:complete", not bad, {"bad": bad[:10]},
          "F3 real run n=200 via run.sh: complete, summary == raw, probe rc matches the control recomputed from raw, "
          "the cell's layout, stamps taken (the control's verdict is recorded, not gated)")
    rec.update({"flush_control": "pass" if not fail else "FAIL (%s)" % ",".join(fail),
                "ratios_vs_nosync25_from_raw": {a: round(r, 1) for a, r in ratios.items()},
                "p50_us_from_raw": {a: round(v / 1e3, 1) for a, v in p50.items()},
                "refused_arms": sj.get("refused_arms"), "fstype": sj.get("fstype"), "mount_source": sj.get("mount_source"),
                "flush_path": [(l.get("mount"), l.get("fstype"), l.get("source"), l.get("disk"), l.get("write_cache"))
                               for l in sj.get("flush_path") or []],
                "leaf": sj.get("leaf"), "floor_kind": sj.get("floor_kind"), "flush_sent_to_device": sj.get("flush_sent_to_device")})

    # the device flush record (item 2)
    rep = rj(os.path.join(o3, "blkflush", "report.json")) or {}
    dbad = []
    if not rep or rep.get("refused") or "windows" not in rep:
        dbad.append(("no device flush record", rep.get("refused")))
    else:
        w = rep["windows"]
        nrun = len(ran(arms, KIND))
        if w.get("n_windows") != n * nrun:
            dbad.append(("windows", w.get("n_windows"), n * nrun))
        wa = w.get("arms") or {}
        layers = sj.get("flush_path") or []
        leafinfo = sj.get("leaf") or {}
        amb = w.get("ambiguous_by_device") or {}
        rootd = (kv.get("root_disk") or "").split(" ")[0]
        shared = []  # the leaf drive only when it is the runner's root disk (fresh review I-L6: a T3 data disk is private)
        for k, l in enumerate(layers):
            names = [l.get("disk")]
            if k == len(layers) - 1:
                names += [p.get("disk") for p in leafinfo.get("multipath") or []]
            is_brd = k == len(layers) - 1 and leafinfo.get("kind") == "brd"
            private = k < len(layers) - 1 or is_brd or l.get("disk") != rootd
            if not private:
                shared += names
            n0 = wa.get("nosync25", {})
            for x in names:
                r = (n0.get("devices") or {}).get(x)
                hit = (n0.get("ops", n) - r.get("zero_windows", n)) if r else 0
                if hit and (private or hit > SHARED_MAX_FRAC * n0.get("ops", n)):
                    dbad.append(("nosync25 windows hold flush requests", x, "private" if private else "shared", r))
                if private and amb.get(x):
                    dbad.append(("events at a window edge on a private device", x, amb.get(x), w.get("ambiguous_sample")))
            total = sum((rep.get("devices") or {}).get(x, {}).get("total", 0) for x in names)
            if l.get("write_cache") == "write back" and not is_brd:
                for a in GATED:
                    if a not in rows:
                        continue
                    hit = 0
                    zero = None
                    for x in names:
                        dv = (wa.get(a, {}).get("devices") or {}).get(x)
                        if dv:
                            hit += dv.get("events", 0)
                            zero = dv.get("zero_windows") if zero is None else min(zero, dv.get("zero_windows"))
                    if zero is None or zero > 0:
                        dbad.append(("a gated op issued no flush request to a write-back layer", k, names, a,
                                     {"events": hit, "zero_windows": zero}))
            elif total:
                dbad.append(("flush requests issued to a write-through (or brd) device", k, names, total))
        dfp = merged.get("device_flushes_per_op") if merged else None
        for a in GATED:
            if a in rows and (not isinstance(dfp, dict) or a not in dfp):
                dbad.append(("device_flushes_per_op lacks a gated arm", a))
        rec["device_flushes_per_op"] = dfp
        rec["layer_device_flushes_per_op"] = (merged or {}).get("layer_device_flushes_per_op")
        rec["shared_devices"] = shared
        rec["ambiguous_by_device"] = amb
        n0ops = wa.get("nosync25", {}).get("ops", n)
        rec["nosync25_windows_hit"] = {x: n0ops - r.get("zero_windows", n0ops)
                                       for x, r in (wa.get("nosync25", {}).get("devices") or {}).items()}
    check("F3:devflush", not dbad, {"bad": dbad[:8]},
          "blkflush: device_flushes_per_op for every gated arm; nosync25's windows hold no flush request on a device "
          "private to the cell (and at most %d%% of them a foreign one on the shared drive); every gated op issues "
          ">= 1 to each write-back layer and none reaches a write-through one; no edge event on a private device"
          % int(SHARED_MAX_FRAC * 100))
    mb = []
    if merged is None:
        mb.append("no summary.json")
    else:
        extra = sorted(set(merged) - set(sj))
        if extra != ["device_flushes", "device_flushes_per_op", "floor_claim_from_counts", "flush_gate",
                     "layer_device_flushes_per_op"]:
            mb.append(("added keys", extra))
        if any(merged.get(k) != v for k, v in sj.items()):
            mb.append(("a probe field changed", [k for k, v in sj.items() if merged.get(k) != v][:5]))
    check("F3:merge", not mb, {"bad": mb}, "summary.json is the probe's summary.probe.json plus the device flush and gate keys only")
    g = rj(os.path.join(o3, "gate.json")) or {}
    fg = g.get("flush_gate") or {}
    want = {"wb": "pass", "wt": "not applicable: no volatile cache: no drive flush", "brd": "not applicable: brd"}.get(leaf)
    gb = []
    if g.get("refusals"):
        gb.append(("refusals", g.get("refusals")))
    if fg.get("outcome") != want:
        gb.append(("flush gate", fg, want))
    if (merged or {}).get("flush_gate") != fg:
        gb.append("summary.json's flush_gate differs from gate.json's")
    check("F3:gate", not gb, {"bad": gb, "gate": g},
          "run.sh's gate: no post-run refusal; the diskstats leaf flush gate passes on a write-back leaf (>= n x gated "
          "arms) and labels a write-through or brd leaf, never passes it")
    rb = []
    if sj.get("clocksource") not in CLOCKSOURCES:
        rb.append(("clocksource", sj.get("clocksource")))
    if st0 and st0.get("clocksource") != sj.get("clocksource"):
        rb.append(("stamp clocksource", (st0 or {}).get("clocksource")))
    for k in ("cpufreq", "cpuidle"):
        if not st0 or k not in st0:
            rb.append(("stamp lacks", k))
    if sj.get("exe_sha256") != kv.get("v3floor_sha256"):
        rb.append(("exe_sha256", sj.get("exe_sha256"), kv.get("v3floor_sha256")))
    for k, l in enumerate(sj.get("flush_path") or []):
        if not l.get("write_cache") or not l.get("fua") or not l.get("disk"):
            rb.append(("layer write_cache/fua", k))
        if l.get("fstype") == "ext4":
            e = l.get("ext4") or {}
            if not (e.get("data") and e.get("commit_s") and isinstance(e.get("journal_async_commit"), bool) and e.get("journal")):
                rb.append(("ext4 layer lacks data=/commit=/journal_async_commit/journal", k, e))
    vm = (sj.get("virtualization") or {}).get("virtualized")
    fk = {"wb": "virtual drive flush: reach to media unknown" if vm else "drive flush",
          "wt": "no volatile cache: no drive flush", "brd": "brd: no drive (fire-check only, never credited)"}.get(leaf)
    if sj.get("floor_kind") != fk or not isinstance(vm, bool):
        rb.append(("floor_kind", sj.get("floor_kind"), fk, "virtualized", vm))
    # the claim the batch may make, re-derived here from its own leaf counts (fresh reviews P-H1, B-H2)
    clean = ((merged or {}).get("device_flushes_per_op") or {}).get("clean")
    if leaf == "wb":
        bare = isinstance(clean, (int, float)) and clean >= 0.95 and sj.get("fstype") in ("ext4", "xfs")
        want_claim = "clean fsync (a bare flush" if bare else "no bare-flush baseline"
    else:
        want_claim = "none: a brd floor" if leaf == "brd" else "no drive flush"
    if not str((merged or {}).get("floor_claim_from_counts", "")).startswith(want_claim):
        rb.append(("floor_claim_from_counts", (merged or {}).get("floor_claim_from_counts"), want_claim, clean))
    if sj.get("fstype") == "btrfs" and "bare flush" in str(sj.get("floor_claim", "")) and "no bare-flush" not in str(sj.get("floor_claim", "")):
        rb.append(("btrfs floor_claim promises a bare flush", sj.get("floor_claim")))
    if not str(sj.get("flush_sent_to_device", "")).startswith("yes" if leaf == "wb" else "no"):
        rb.append(("flush_sent_to_device", sj.get("flush_sent_to_device")))
    if sj.get("arms_gated") != GATED:
        rb.append(("arms_gated", sj.get("arms_gated")))
    if "clone1b" not in (sj.get("arms_report_only") or {}):
        rb.append("clone1b is not report-only")
    du = sj.get("durability") or {}
    if "unverified on Linux" not in du.get("clone2b", "") or "unverified on Linux" not in du.get("cfr2b", ""):
        rb.append(("durability", du))
    check("F3:record", not rb, {"bad": rb},
          "the batch records the clocksource (allowlisted, in summary and stamp), cpufreq/cpuidle, the binary's own sha256, "
          "per-layer write_cache/fua and ext4 data=/commit=/async commit, floor_kind and flush_sent_to_device for its "
          "leaf, the gated set (clone1b report-only) and 'durability unverified on Linux'")
    rec["flush_gate"] = fg
    return rec


def bind(out, cell):
    """firecheck.sh's last step: run.sh bound to the real verdict (P_runsh_ok, or P_runsh_brd on a brd cell)."""
    v = rj(os.path.join(out, "verdict.json")) or {}
    res = []
    if v.get("leaf_class") == "brd":
        rc = rc_of(os.path.join(out, "F4", "P_runsh_brd.rc"))
        txt = rd(os.path.join(out, "F4", "P_runsh_brd.txt")) or ""
        ok = rc == 2 and "brd" in txt
        res.append({"id": "bind:P_runsh_brd", "pass": ok, "detail": {"rc": rc, "text": txt[-300:]},
                    "check": "a brd cell's own passing verdict does not bind a batch (brd is fire-check only)"})
    else:
        rc = rc_of(os.path.join(out, "F4", "P_runsh_ok.rc"))
        bt = rd(os.path.join(out, "F4", "P_runsh_ok.out", "binary.txt")) or ""
        vs = hashlib.sha256(open(os.path.join(out, "verdict.json"), "rb").read()).hexdigest() if v else None
        g = rj(os.path.join(out, "F4", "P_runsh_ok.out", "gate.json"))
        rcl = rd(os.path.join(out, "F4", "P_runsh_ok.out", "rc")) or ""
        ok = (rc in (0, 3) and "bound=fire-checked: " in bt and ("verdict_sha256=%s" % vs) in bt and
              ("v3floor_sha256=%s" % v.get("v3floor_sha256")) in bt and ("cell=%s" % cell) in bt and
              isinstance(g, dict) and g.get("refusals") == [] and re.search(r"gate_rc=(0|3) ", rcl) is not None)
        g = g or {}
        res.append({"id": "bind:P_runsh_ok", "pass": ok, "detail": {"rc": rc, "binary.txt": bt, "gate": g},
                    "check": "run.sh binds a batch to this cell's real passing verdict and records its sha256 and run id"})
    b = {"cell": cell, "verdict_all_pass": v.get("all_pass"), "checks": res, "all_pass": all(r["pass"] for r in res)}
    with open(os.path.join(out, "bind.json"), "w") as f:
        json.dump(b, f, indent=1)
    for r in res:
        print(("PASS " if r["pass"] else "FAIL ") + r["id"] + " -- " + r["check"] + ("" if r["pass"] else ": " + json.dumps(r["detail"])[:600]))
    return 0 if b["all_pass"] and v.get("all_pass") else 1


# ---- the red column: the same plants against the base (df4b39e53) probe and scripts ----------------------------
RED = [  # tag, review item, what the base does that the fix stops, how the outcome is read
    ("red_1a_brd", "1(a)", "the base accepts a brd leaf", "rc0"),
    ("red_1a_driver", "1(a)", "the base accepts a dm leaf (no driver)", "rc0"),
    ("red_1b_leafflip", "1(b)", "the base runs with the kernel's write_cache overridden against the drive", "rc0state"),
    ("red_2_devflush", "2", "the base summary has no device_flushes_per_op", "nodevflush"),
    ("red_3_clone1b_gated", "3", "the base gates clone1b", "clone1b_gated"),
    ("red_7_mutant", "7", "base run.sh forwards --mutant-nosync into a bound batch", "rc0bound"),
    ("red_7_traceclock", "7", "base run.sh forwards --trace-clock into a bound batch", "rc0bound"),
    ("red_7_dir", "7", "base run.sh forwards a second --dir, so the batch runs elsewhere", "rc0bound"),
    ("red_8_planted", "8", "base run.sh binds the 4-field planted verdict", "rc0bound"),
    ("red_9_loopwt", "9", "the base runs on a loop that reads write-through", "rc0"),
    ("red_10a_hidden_tmpfs", "10(a)", "predicted refused at base too (statfs magic)", "rc2"),
    ("red_10b_hidden_nobarrier", "10(b)", "the base runs on a nobarrier mount hidden behind a barrier one", "rc0"),
    ("red_10c_lazy", "10", "the base follows a lazily unmounted loop's backing path to a decoy", "rc0decoy"),
    ("red_11_append64", "11", "the base has no append arm >= 60 B", "unknownarm"),
    ("red_12a_deleted", "12(a)", "predicted refused at base too (the plant was missing, not the refusal)", "rc2"),
    ("red_12b_nest4", "12(b)", "the base's message says 'more than 4 loop layers'", "oldmsg"),
    ("red_12d_shim", "12(d)", "predicted refused at base too", "rc2"),
    ("red_13_symlink", "13", "the base follows a planted symlink out of D", "rc0target"),
    ("red_15_dirsync", "15", "the base runs on a dirsync ext4", "rc0"),
    ("red_15_logdev", "15", "the base runs on XFS with an external log", "rc0"),
    ("red_15_extjournal", "15", "the base runs on ext4 with an external journal", "rc0"),
    ("red_15_multidev", "15", "the base runs on a two-device btrfs", "rc0"),
    ("red_15_fields", "15", "the base summary has no per-layer data=/commit=/async commit", "noext4fields"),
    ("red_15_chattr", "15", "the base runs on a directory carrying chattr +S (per-file sync)", "rc0"),
    ("red_16_clocksource", "16", "the base runs on a non-TSC clocksource (x86 only)", "rc0state"),
    ("red_17_t3", "17", "base run.sh ignores V3_REQUIRE_T3 while the T3 rule is false", "rc0"),
]


def red(kv, leaf, arch):
    rows = []
    d = os.path.join(OUT, "red")
    if not os.path.isdir(d):
        with open(os.path.join(OUT, "red.json"), "w") as f:
            json.dump({"base": None, "rows": [], "note": "no red stage (V3_BASE unset)"}, f, indent=1)
        return
    for tag, item, claim, how in RED:
        rc = rc_of(os.path.join(d, tag + ".rc"))
        txt = rd(os.path.join(d, tag + ".txt")) or ""
        if rc is None and not os.path.exists(os.path.join(d, tag + ".na")):
            rows.append({"tag": tag, "item": item, "claim": claim, "observed": "missing", "red": None})
            continue
        if os.path.exists(os.path.join(d, tag + ".na")):
            rows.append({"tag": tag, "item": item, "claim": claim, "observed": "not applicable: " + (rd(os.path.join(d, tag + ".na")) or "").strip(), "red": None})
            continue
        sj = rj(os.path.join(d, tag + ".out", "summary.json")) or {}
        bt = rd(os.path.join(d, tag + ".out", "binary.txt")) or ""
        state = rd(os.path.join(d, tag + ".state")) or ""
        if how == "rc0":
            r = rc in (0, 3)
        elif how == "rc0state":  # the planted state was read back from sysfs while the base ran
            r = rc in (0, 3) and "changed=1" in state
        elif how == "rc0target":  # the base created the symlink's target outside D
            r = rc in (0, 3) and (rd(os.path.join(d, tag + ".target")) or "").strip() == "present"
        elif how == "rc0decoy":  # the base followed the stale backing path to the decoy
            decoy = (rd(os.path.join(d, tag + ".decoy")) or "").strip()
            fp = sj.get("flush_path") or []
            r = rc in (0, 3) and bool(decoy) and bool(fp) and fp[0].get("loop_backing") == decoy
        elif how == "rc0bound":
            r = rc in (0, 3) and "bound=fire-checked" in bt
        elif how == "rc2":
            r = rc != 2  # red would mean the base did NOT refuse
        elif how == "unknownarm":
            r = rc == 2 and "unknown arm" in txt
        elif how == "oldmsg":
            r = rc == 2 and "more than 4 loop layers" in txt
        elif how == "nodevflush":
            r = rc in (0, 3) and bool(sj) and "device_flushes_per_op" not in sj
        elif how == "clone1b_gated":
            r = bool(sj) and (sj.get("flush_control_arms") or {}).get("clone1b", {}).get("gated") is True
        elif how == "noext4fields":  # an ext4 layer exists, and none carries the fields
            fp = sj.get("flush_path") or []
            r = bool(sj) and any(l.get("fstype") == "ext4" for l in fp) and not any("ext4" in l for l in fp)
        else:
            r = None
        rows.append({"tag": tag, "item": item, "claim": claim, "rc": rc, "red": r, "text": txt[-240:]})
    off = rj(os.path.join(d, "offline.json"))
    with open(os.path.join(OUT, "red.json"), "w") as f:
        json.dump({"base": kv.get("base_sha"), "base_v3floor_sha256": kv.get("base_v3floor_sha256"), "cell": CELL,
                   "arch": arch, "leaf_class": leaf, "rows": rows, "offline": off}, f, indent=1)
    for r in rows:
        print("RED %-26s item %-6s %s: %s" % (r["tag"], r["item"], {True: "RED (bug shown at base)", False: "NOT RED",
                                                                  None: "n/a"}[r["red"]], r.get("observed", r.get("rc"))))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
