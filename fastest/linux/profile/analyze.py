#!/usr/bin/env python3
"""fastest-linux profiling analysis: per-op syscall, flush, perf-stat and instruction counts from
profile.sh's raw output, gated against budget.json and against the base build; with no base built in this job,
against the baseline artifact's own numbers for full-snap-c1 (refused unless it names its sha and ran on this
runner's cpu, PROFILE_CPU).

usage: analyze.py <raw-dir> <budget.json> <out-dir> [--baseline prev-baseline.json] [--rebaseline]
       analyze.py --self-test
       analyze.py --budget-verdict <verdict.tsv>   (the absolute syscall budget job; exit 1 unless it passes)

<raw-dir> holds head/ and, when a base was built, base/ (profile.sh's layout). Writes to <out-dir>:
  summary.json   every number, per side and arm
  verdict.tsv    gate, expected, got, PASS|FAIL|INFO|NOT-RUN, regression|budget|info (the row's kind)
  regression_green  1 when every regression-kind row PASSes, else 0
  baseline.json  this run's head numbers, the next run's baseline once this run is regression-green
  summary.md     the same, for $GITHUB_STEP_SUMMARY
Exit 1 if any gate FAILs, if no arm produced a count, or if the self-test (always run first) fails.

Instruments and what each can miss (stated, not hidden):
  strace -f -y   every syscall of every thread, entry-counted, windows cut at the driver's
                 FASTEST_PHASE markers. Blind to io_uring submissions and to O_SYNC/O_DSYNC write
                 paths: either one present REFUSES the flush count instead of under-counting it.
  engine counter turso_core::branch::sync_counts() over each window (the driver's summary.json):
                 must equal strace's fsync+fdatasync count in the same window, or the window FAILs.
  perf stat      per window via perf's control FIFO; hardware events may read <not supported> on
                 a hosted VM, which is recorded as such and never as zero.
  callgrind      whole-program Ir of runs doing only the first K phases (K = 1..4) at N and 2N ops:
                 per op, (Ir(K, 2N) - Ir(K, N)) / N is the first K phases' cost, all threads, setup
                 cancelled; a phase is the difference of two such costs.
"""
import gzip
import json
import os
import re
import sys

WINDOWS = ["create", "connect", "write", "delete"]
LINE = re.compile(r"^(\d+)\s+(?:<\.\.\. (\w+) resumed>|(\w+)\((.*))")
MARK = re.compile(r'write\(-1, "FASTEST_PHASE (\w+) (begin|end)"')
FD_PATH = re.compile(r"^\s*-?\d+<([^>]*)>")


def is_flush(name, args):
    if name in ("fsync", "fdatasync", "syncfs", "sync"):
        return True
    if name == "msync" and "MS_SYNC" in args:
        return True
    if name == "sync_file_range" and "SYNC_FILE_RANGE_WAIT" in args:
        return True
    return False


def parse_strace(lines):
    """Per window: syscall counts by name, flushes by target file, and refusals."""
    win = {}
    cur = None
    refusals = []
    for line in lines:
        m = LINE.match(line)
        if not m or m.group(2):  # not a syscall, or the second half of an unfinished one
            continue
        name, args = m.group(3), m.group(4) or ""
        mk = MARK.search(line)
        if mk:
            w, edge = mk.group(1), mk.group(2)
            if edge == "begin":
                cur = w
                win.setdefault(w, {"syscalls": {}, "flushes": {}, "flush_total": 0, "fsync_like": 0})
            else:
                if cur != w:
                    refusals.append(f"marker {w} end without its begin (open window {cur})")
                cur = None
            continue
        if name.startswith("io_uring"):
            refusals.append(f"{name} seen: io_uring submissions are invisible to strace")
        if name in ("open", "openat", "openat2") and re.search(r"\bO_D?SYNC\b", args):
            refusals.append(f"{name} with O_SYNC/O_DSYNC: writes on it would be uncounted flushes")
        if cur is None:
            continue
        w = win[cur]
        w["syscalls"][name] = w["syscalls"].get(name, 0) + 1
        if is_flush(name, args):
            fm = FD_PATH.match(args)
            target = fm.group(1) if fm else "?"
            w["flushes"][target] = w["flushes"].get(target, 0) + 1
            w["flush_total"] += 1
            if name in ("fsync", "fdatasync"):
                w["fsync_like"] += 1
    if cur is not None:
        refusals.append(f"window {cur} never ended")
    return win, sorted(set(refusals))


def parse_perfstat(path):
    out = {}
    if not os.path.exists(path):
        return None
    for line in open(path):
        f = line.strip().split(",")
        if len(f) < 3 or line.startswith("#"):
            continue
        val, event = f[0], f[2]
        out[event] = None if val.startswith("<") else float(val)
    return out


def lower_bound(spec, clients):
    lo, hi = spec
    if lo == "1/C":
        lo = 1.0 / clients
    return float(lo), float(hi)


def analyze_arm(d, arm):
    """Everything for one arm directory of one side."""
    r = {"arm": arm}
    status = os.path.join(d, "status")
    if os.path.exists(status):
        r["status"] = open(status).read().strip()
    plain = os.path.join(d, "plain", "summary.json")
    if os.path.exists(plain):
        p = json.load(open(plain))
        r["clients"] = p["clients"]
        r["ops"] = p.get("ops_total") or p["clients"] * p["ops_per_client"]  # ops_total: the driver since MED 3
        r["latency_ns"] = {w: {k: p["phases"][w][k] for k in ("p50_ns", "p99_ns", "max_ns")} for w in WINDOWS}
        r["busy_retries_per_op"] = {w["window"]: round(w.get("busy_retries", 0) / r["ops"], 4) for w in p["windows"]}
    srun = os.path.join(d, "strace-run", "summary.json")
    st = os.path.join(d, "strace.txt")
    if not os.path.exists(st) and os.path.exists(st + ".gz"):
        st += ".gz"
    if os.path.exists(st) and os.path.exists(srun):
        s = json.load(open(srun))
        ops = s.get("ops_total") or s["clients"] * s["ops_per_client"]
        opener = gzip.open if st.endswith(".gz") else open
        with opener(st, "rt", errors="replace") as fh:
            win, refusals = parse_strace(fh)
        engine = {w["window"]: w["engine_syncs_total"] for w in s["windows"]}
        r["strace"] = {"ops": ops, "refusals": refusals, "windows": {}}
        for w in WINDOWS:
            if w not in win:
                r["strace"]["windows"][w] = None
                continue
            x = win[w]
            r["strace"]["windows"][w] = {
                "syscalls_per_op": {k: round(v / ops, 4) for k, v in sorted(x["syscalls"].items())},
                "syscalls_total_per_op": round(sum(x["syscalls"].values()) / ops, 4),
                "flushes_per_op": round(x["flush_total"] / ops, 4),
                "flush_targets": x["flushes"],
                "strace_fsync_like": x["fsync_like"],
                "engine_sync_counter": engine.get(w),
            }
    for w in WINDOWS:
        ps = parse_perfstat(os.path.join(d, f"perfstat-{w}.csv"))
        if ps is not None and r.get("ops"):
            r.setdefault("perf_per_op", {})[w] = {
                k: (None if v is None else round(v / r["ops"], 2)) for k, v in ps.items()
            }
    cgn = os.path.join(d, "cg-n.ops")
    if os.path.exists(cgn):
        n = int(open(cgn).read().strip())
        tot = {}
        for k in range(1, 5):
            for tag in ("n", "2n"):
                f = os.path.join(d, f"cg-k{k}-{tag}.total")
                if os.path.exists(f) and open(f).read().strip().isdigit():
                    tot[(k, tag)] = int(open(f).read().strip())
        cum = {k: (tot[(k, "2n")] - tot[(k, "n")]) / n for k in range(1, 5) if (k, "n") in tot and (k, "2n") in tot}
        if len(cum) == 4:
            r["ir_per_op"] = {"create": round(cum[1], 1), "connect": round(cum[2] - cum[1], 1),
                              "write": round(cum[3] - cum[2], 1), "delete": round(cum[4] - cum[3], 1),
                              "cycle": round(cum[4], 1)}
        else:
            r["ir_missing"] = sorted(f"k{k}-{t}" for k in range(1, 5) for t in ("n", "2n") if (k, t) not in tot)
    return r


BASELINE_ARM = "full-snap-c1"  # the arm baseline.json records; both regression premises bind to it
VERDICTS = ("PASS", "FAIL", "INFO", "NOT-RUN")
# T3 review item 29: every row carries a kind, and regression_green reads kinds, never gate ids.
#   regression  the profile job's verdict: regression_green needs every such row to PASS (INFO and NOT-RUN block it)
#   budget      the absolute syscall budget (budget-syscalls/*), the budget job's; red at 675adbfb3, never blocks
#               the baseline
#   info        recorded, never gates: INFO or NOT-RUN only (C>1 comparisons, an unavailable or unregistered class)
KINDS = ("regression", "budget", "info")
VERDICT_FIELDS = 5  # gate, expected, got, verdict, kind


def row_problem(r):
    """Why a verdict row is not representable, or None: the allowlist that gates(), write_verdict(), read_verdict()
    and regression_green() all apply, so a row no reader can classify is refused where it is made or read."""
    if len(r) != VERDICT_FIELDS:
        return f"{len(r)} fields, expected {VERDICT_FIELDS}"
    # read_verdict splits rows with str.splitlines(), so any character it breaks on (newline, carriage return,
    # form feed, the file/group/record separators, the Unicode line and paragraph separators) inside a field
    # would split the row; a tab would shift its fields
    if any(not isinstance(f, str) or "\t" in f or (f != "" and f.splitlines() != [f]) for f in r):
        return "a field that is not a string, or holds a tab or a line break"
    if r[3] not in VERDICTS:
        return f"verdict {r[3]!r} is not one of {VERDICTS}"
    if r[4] not in KINDS:
        return f"kind {r[4]!r} is not one of {KINDS}"
    if r[4] == "info" and r[3] not in ("INFO", "NOT-RUN"):
        return f"an info row cannot {r[3]}"
    return None


def artifact_refusal(baseline, cpu, field):
    """None when the baseline artifact may stand in for an in-job base on `field`, else why it may not (T3 review
    item 7b). It must name the sha it measured and the cpu it ran on, that cpu must be this runner's, and it must
    hold the field; a missing label is a refusal, never a match of None to None."""
    sha = baseline.get("sha")
    if not sha:
        return "the baseline artifact names no sha"
    if not baseline.get("cpu"):
        return f"the baseline artifact of {sha} records no cpu"
    if not cpu:
        return "this runner's cpu is unknown (PROFILE_CPU unset)"
    if baseline["cpu"] != cpu:
        return f"the baseline artifact of {sha} ran on cpu {baseline['cpu']!r}, this runner is cpu {cpu!r}"
    if not baseline.get(field):
        return f"the baseline artifact of {sha} holds no {field}"
    return None


def gates(head, base, budget, baseline, cpu=None):
    """baseline: the stored artifact (baseline.json of the run that uploaded it) or None; cpu: this runner's
    PROFILE_CPU. A base built in this job outranks the artifact; without one, both regression gates compare
    the baseline arm against the artifact's own numbers, refused unless artifact_refusal() passes it."""
    rows = []

    def row(g, exp, got, v, kind):
        # `got` is instrument text (a driver's status line, strace refusals): a tab or newline in it is display, not
        # structure, so it is flattened here; every other field is this function's own and row_problem() refuses it
        r = (g, exp, " ".join(str(got).splitlines()).replace("\t", " "), v, kind)
        why = row_problem(r)
        if why:
            raise ValueError(f"gates: row {g} is not representable: {why}")
        rows.append(r)

    for arm, h in sorted(head.items()):
        cls = arm.split("-")[0]
        if "NOT AVAILABLE" in h.get("status", ""):
            row(f"budget-flush/{arm}", "create flushes per op in budget", h["status"], "NOT-RUN", "info")
            continue
        s = h.get("strace")
        if not s:
            row(f"budget-flush/{arm}", "a strace count", "no strace data", "FAIL", "regression")
            continue
        if s["refusals"]:
            row(f"budget-flush/{arm}", "no uncountable durability path", "; ".join(s["refusals"]), "FAIL", "regression")
            continue
        missing = [w for w in WINDOWS if s["windows"].get(w) is None]
        if missing:
            row(f"windows/{arm}", "all four windows marked", f"missing {missing}", "FAIL", "regression")
            continue
        for w in WINDOWS:
            x = s["windows"][w]
            if x["engine_sync_counter"] is not None and x["engine_sync_counter"] != x["strace_fsync_like"]:
                row(f"two-instruments/{arm}/{w}", "engine sync counter == strace fsync+fdatasync",
                    f"engine {x['engine_sync_counter']} strace {x['strace_fsync_like']}", "FAIL", "regression")
            else:
                row(f"two-instruments/{arm}/{w}", "engine sync counter == strace fsync+fdatasync",
                    f"{x['strace_fsync_like']}", "PASS", "regression")
        spec = budget["classes"].get(cls)
        if spec is None:
            row(f"budget-flush/{arm}", "a registered class", cls, "INFO", "info")
            continue
        c = h.get("clients", 1)
        lo, hi = lower_bound(spec["create_flushes_per_op_c1" if c == 1 else "create_flushes_per_op_cn"], c)
        x = s["windows"]["create"]
        f = x["flushes_per_op"]
        row(f"budget-flush/{arm}", f"{lo:.4g} <= create flushes/op <= {hi:.4g}", f, "PASS" if lo <= f <= hi else "FAIL",
            "regression")
        allowed = spec["create_flush_files_allowed"]
        bad = {t: n for t, n in x["flush_targets"].items() if not any(t.endswith(a) for a in allowed)}
        row(f"budget-flush-files/{arm}", f"create flushes only on {allowed or 'nothing'}", bad or "ok",
            "FAIL" if bad else "PASS", "regression")
        sb = budget.get("syscalls_per_create")
        if sb:
            # The ABSOLUTE budget (DECISIONS 2026-10-05T02:54:27Z): every syscall strace sees in the create window,
            # all threads, per create, averaged over the window (the markers delimit it and are not counted).
            # Binds at C=1; at C>1 waiters' polls scale with the flight's length, so the row is INFO.
            tot = round(sum(x["syscalls_per_op"].values()), 4)
            top = ", ".join(f"{k} {v:g}" for k, v in sorted(x["syscalls_per_op"].items(), key=lambda kv: (-kv[1], kv[0]))[:8])
            v = ("PASS" if tot <= sb["max"] else "FAIL") if c == 1 else "INFO"
            row(f"budget-syscalls/{arm}", f"<= {sb['max']} syscalls per create" + ("" if c == 1 else " (C>1: INFO)"),
                f"{tot:g}/op: {top}", v, "budget")
        bx = src = refused = None
        bw = (((base or {}).get(arm) or {}).get("strace") or {}).get("windows", {}).get("create")
        if bw:
            bx, src = bw["syscalls_per_op"], "base built in this job"
        elif arm == BASELINE_ARM and baseline is not None:
            # review 7b: without this, only an in-job base produced the row, so a base sha that would not build left
            # the premise unevaluated on every later push and the baseline never advanced.
            refused = artifact_refusal(baseline, cpu, "create_syscalls_per_op")
            if refused:
                row(f"syscalls-vs-base/{arm}", "an in-job base, or the baseline artifact of this cpu",
                    f"refused: {refused}", "FAIL", "regression")
            else:
                bx, src = baseline["create_syscalls_per_op"], f"baseline artifact of {baseline['sha']}"
        if bx is None and not refused:
            # review 29: the row exists even with nothing to compare against. On the baseline arm at C=1 it is the
            # regression premise, so NOT-RUN blocks regression_green; elsewhere it is a record.
            row(f"syscalls-vs-base/{arm}", "a base to compare the create window's syscalls against",
                "no in-job base and no baseline artifact" if arm == BASELINE_ARM
                else f"no in-job base for this arm (the baseline artifact records {BASELINE_ARM} only)",
                "NOT-RUN", "regression" if arm == BASELINE_ARM and c == 1 else "info")
        if bx is not None:
            hx = x["syscalls_per_op"]
            sv = budget["syscalls_vs_base"]
            excl = set(sv["timing_dependent_excluded"])
            worse = {k: (bx.get(k, 0.0), v) for k, v in hx.items()
                     if k not in excl and v > bx.get(k, 0.0) + sv["per_op_slack"]}
            # At C > 1 the counts move with contention, not with the code: run 37400090036 built the SAME
            # engine on both sides and arm64 C=64 read getpid 3.21 -> 3.28 per create (a waiter's poll
            # loop runs as often as the flight takes). So the gate binds at C=1, where counts are exact
            # (getpid 4.005, run 37255309860), and C > 1 is recorded as INFO.
            verdict = ("FAIL" if worse else "PASS") if c == 1 else "INFO"
            row(f"syscalls-vs-base/{arm}", f"no create syscall above base + {sv['per_op_slack']}/op vs {src}"
                + ("" if c == 1 else " (C>1: contention-dependent, INFO)"),
                worse or "ok", verdict, "regression" if c == 1 else "info")
    if not any(r[0] == f"syscalls-vs-base/{BASELINE_ARM}" for r in rows):
        # review 29: the premise row exists whatever cut the baseline arm short above (no strace, a refusal, a missing
        # window, an unregistered class, NOT AVAILABLE, or no such arm), so regression_green never passes without it.
        row(f"syscalls-vs-base/{BASELINE_ARM}", f"a create-window syscall comparison of {BASELINE_ARM}",
            "never reached: " + ("no such head arm" if BASELINE_ARM not in head else "the arm was cut short (rows above)"),
            "NOT-RUN", "regression")
    ins = budget["instructions"]
    arm = BASELINE_ARM
    h = head.get(arm, {}).get("ir_per_op", {}).get("create")
    b = src = why = None
    if base and (base.get(arm) or {}).get("ir_per_op", {}).get("create"):
        b, src = base[arm]["ir_per_op"]["create"], "base built in this job"
    elif baseline is not None:
        why = artifact_refusal(baseline, cpu, "ir_create")
        if why is None:
            b, src = baseline["ir_create"], f"baseline artifact of {baseline['sha']}"
    # instructions/create is always emitted and always regression-kind: INFO (first run) blocks regression_green too
    if h is None:
        row("instructions/create", "a callgrind count", "none", "FAIL", "regression")
    elif why:
        row("instructions/create", f"growth <= {ins['create_growth_max']:.0%} vs an in-job base or the baseline artifact of this cpu",
            f"refused: {why}", "FAIL", "regression")
    elif b is None:
        row("instructions/create", f"<= base x {1 + ins['create_growth_max']}", f"head {h} Ir/op; no baseline (first run)",
            "INFO", "regression")
    else:
        g = h / b - 1
        row("instructions/create", f"growth <= {ins['create_growth_max']:.0%} vs {src}",
            f"head {h} base {b} growth {g:+.2%}", "PASS" if g <= ins["create_growth_max"] else "FAIL", "regression")
    return rows


def self_test():
    """Plant each breach the gates exist to catch and require each to FAIL; and a clean case PASS."""
    budget = {"classes": {"full": {"create_flushes_per_op_c1": [1.0, 1.01], "create_flushes_per_op_cn": ["1/C", 1.01],
                                   "create_flush_files_allowed": ["-branch-log"]},
                          "async": {"create_flushes_per_op_c1": [0, 0], "create_flushes_per_op_cn": [0, 0],
                                    "create_flush_files_allowed": []}},
              "instructions": {"create_growth_max": 0.05},
              "syscalls_vs_base": {"per_op_slack": 0.05, "timing_dependent_excluded": ["futex"]},
              "syscalls_per_create": {"max": 3}}

    def trace(per_create, extra_file=None, uring=False, dsync=False):
        L = []
        if dsync:
            L.append('1 openat(AT_FDCWD</d>, "/d/x", O_RDWR|O_DSYNC) = 3</d/x>')
        for w in WINDOWS:
            L.append(f'1 write(-1, "FASTEST_PHASE {w} begin", 26) = -1 EBADF (Bad file descriptor)')
            for _ in range(10):
                if w == "create":
                    for _ in range(per_create):
                        L.append("2 fsync(5</d/p.db-branch-log>) = 0")
                    if extra_file:
                        L.append(f"2 fsync(6</d/p.db-{extra_file}>) = 0")
                L.append("2 pwrite64(5</d/p.db-branch-log>, \"x\", 1, 0) = 1")
                L.append("2 futex(0x1, FUTEX_WAKE_PRIVATE, 1 <unfinished ...>")
                L.append("3 <... futex resumed>) = 0")
            if uring:
                L.append("2 io_uring_enter(3, 1, 0, 0, NULL, 8) = 1")
            L.append(f'1 write(-1, "FASTEST_PHASE {w} end", 24) = -1 EBADF (Bad file descriptor)')
        return L

    def arm_of(lines, engine_create, clients=1):
        win, ref = parse_strace(lines)
        ws = {}
        for w in WINDOWS:
            x = win[w]
            ws[w] = {"syscalls_per_op": {k: v / 10 for k, v in x["syscalls"].items()},
                     "flushes_per_op": x["flush_total"] / 10, "flush_targets": x["flushes"],
                     "strace_fsync_like": x["fsync_like"],
                     "engine_sync_counter": engine_create if w == "create" else x["fsync_like"]}
        return {"clients": clients, "strace": {"refusals": ref, "windows": ws}, "ir_per_op": {"create": 1000.0}}

    def verdicts(head, base=None, baseline=None):
        return {r[0]: r[3] for r in gates(head, base, budget, baseline)}

    cases = []
    ok = verdicts({"full-snap-c1": arm_of(trace(1), 10)}, {"full-snap-c1": arm_of(trace(1), 10)})
    cases.append(("clean D2 passes every gate", all(v in ("PASS", "INFO") for v in ok.values()) and "FAIL" not in ok.values()))
    v = verdicts({"full-snap-c1": arm_of(trace(2), 20)})
    cases.append(("2 flushes per create FAILs the budget", v.get("budget-flush/full-snap-c1") == "FAIL"))
    v = verdicts({"full-snap-c1": arm_of(trace(0), 0)})
    cases.append(("0 flushes per create at D2 C=1 FAILs (a blind counter cannot pass)", v.get("budget-flush/full-snap-c1") == "FAIL"))
    v = verdicts({"full-snap-c64": arm_of(trace(0), 0, clients=64)})
    cases.append(("0 flushes at C=64 FAILs (lower bound 1/C)", v.get("budget-flush/full-snap-c64") == "FAIL"))
    v = verdicts({"full-snap-c1": arm_of(trace(1, extra_file="branch-arena"), 20)})
    cases.append(("a flush of the arena in the create window FAILs the file allowlist",
                  v.get("budget-flush-files/full-snap-c1") == "FAIL"))
    v = verdicts({"async-snap-c1": arm_of(trace(1), 10)})
    cases.append(("any flush on an async create FAILs", v.get("budget-flush/async-snap-c1") == "FAIL"))
    v = verdicts({"full-snap-c1": arm_of(trace(1, uring=True), 10)})
    cases.append(("io_uring present refuses the count", v.get("budget-flush/full-snap-c1") == "FAIL"))
    v = verdicts({"full-snap-c1": arm_of(trace(1, dsync=True), 10)})
    cases.append(("an O_DSYNC open refuses the count", v.get("budget-flush/full-snap-c1") == "FAIL"))
    v = verdicts({"full-snap-c1": arm_of(trace(1), 11)})
    cases.append(("engine counter != strace FAILs", v.get("two-instruments/full-snap-c1/create") == "FAIL"))
    h = arm_of(trace(1), 10)
    h["ir_per_op"]["create"] = 1051.0
    v = verdicts({"full-snap-c1": h}, {"full-snap-c1": arm_of(trace(1), 10)})
    cases.append(("5.1% more instructions per create FAILs", v.get("instructions/create") == "FAIL"))
    h["ir_per_op"]["create"] = 1049.0
    v = verdicts({"full-snap-c1": h}, {"full-snap-c1": arm_of(trace(1), 10)})
    cases.append(("4.9% more instructions passes", v.get("instructions/create") == "PASS"))
    hb = arm_of(trace(1), 10)
    hb["strace"]["windows"]["create"]["syscalls_per_op"]["fstat"] = 1.0
    v = verdicts({"full-snap-c1": hb}, {"full-snap-c1": arm_of(trace(1), 10)})
    cases.append(("a new syscall per create vs base FAILs", v.get("syscalls-vs-base/full-snap-c1") == "FAIL"))
    hb64 = arm_of(trace(1), 10, clients=64)
    hb64["strace"]["windows"]["create"]["syscalls_per_op"]["fstat"] = 1.0
    v = verdicts({"full-snap-c64": hb64}, {"full-snap-c64": arm_of(trace(1), 10, clients=64)})
    cases.append(("a syscall regression at C=64 is INFO (contention-dependent), not a verdict",
                  v.get("syscalls-vs-base/full-snap-c64") == "INFO"))
    # the planted trace makes 1 fsync + 1 pwrite64 + 1 futex per create (2 + 1 per extra flush)
    v = verdicts({"full-snap-c1": arm_of(trace(1), 10)})
    cases.append(("3 syscalls per create passes the absolute budget", v.get("budget-syscalls/full-snap-c1") == "PASS"))
    v = verdicts({"full-snap-c1": arm_of(trace(2), 20)})
    cases.append(("4 syscalls per create FAILs the absolute budget", v.get("budget-syscalls/full-snap-c1") == "FAIL"))
    v = verdicts({"full-snap-c64": arm_of(trace(2), 20, clients=64)})
    cases.append(("4 syscalls per create at C=64 is INFO", v.get("budget-syscalls/full-snap-c64") == "INFO"))
    hb = arm_of(trace(1), 10)
    hb["strace"]["windows"]["create"]["syscalls_per_op"]["getpid"] = 4.0
    rows = gates({"full-snap-c1": hb}, {"full-snap-c1": dict(hb)}, budget, None)
    cases.append(("only the absolute budget red: regression_green is true (the baseline advances)",
                  {r[0]: r[3] for r in rows}.get("budget-syscalls/full-snap-c1") == "FAIL" and regression_green(rows)))
    rows = gates({"full-snap-c1": arm_of(trace(2), 20)}, None, budget, None)
    cases.append(("a flush-budget FAIL makes regression_green false", not regression_green(rows)))
    rows = gates({"full-snap-c1": arm_of(trace(1), 10)}, None, budget, None)
    cases.append(("review L3: no base (syscalls-vs-base and instructions not evaluated) makes regression_green false",
                  not regression_green(rows)))
    # review 7a (T3 review of 4bdb3a042..2d42982a0): the two cases above pass base=None, so both regression premises
    # are already unevaluated and the mutants `return evaluated` and "drop the syscalls-vs-base conjunct" passed all 19.
    two = arm_of(trace(2), 20)
    rows = gates({"full-snap-c1": two}, {"full-snap-c1": arm_of(trace(2), 20)}, budget, None)
    v = {r[0]: r[3] for r in rows}
    cases.append(("review 7a: base == head, both regression premises PASS, and a flush-budget FAIL still makes "
                  "regression_green false",
                  v.get("instructions/create") == "PASS" and v.get("syscalls-vs-base/full-snap-c1") == "PASS"
                  and v.get("budget-flush/full-snap-c1") == "FAIL" and not regression_green(rows)))
    nostrace = arm_of(trace(1), 10)
    del nostrace["strace"]
    rows = gates({"full-snap-c1": arm_of(trace(1), 10)}, {"full-snap-c1": nostrace}, budget, None)
    v = {r[0]: r[3] for r in rows}
    cases.append(("review 7a: instructions PASS against an in-job base whose strace run failed, syscalls-vs-base not "
                  "PASS, nothing FAILs: regression_green false",
                  v.get("instructions/create") == "PASS" and v.get("syscalls-vs-base/full-snap-c1") != "PASS"
                  and "FAIL" not in v.values() and not regression_green(rows)))

    def guarded(fn):
        """A case whose subject raises is that case's FAIL, never a crash of the whole self-test."""
        try:
            return fn()
        except Exception as e:
            print(f"self-test note: {type(e).__name__}: {e}")
            return False

    def by_id(rows):
        return {r[0]: r for r in rows}

    # review 7b: only a base built in this job produced syscalls-vs-base, so a base sha that would not build (or a lost
    # base run) left the premise unevaluated on every later push and the baseline never advanced. With no in-job base,
    # both regression gates compare against the baseline artifact's own numbers, which name the sha and cpu they came
    # from (artifact 11570831049: sha 60753525a, cpu "Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz").
    art_sha = "60753525a41446b4c67209397a596cf014a20be0"
    art = {"sha": art_sha, "cpu": "cpu-A", "ir_create": 1000.0,
           "create_syscalls_per_op": {"fsync": 1.0, "futex": 1.0, "pwrite64": 1.0}}

    def case_artifact_runs():
        rows = gates({"full-snap-c1": arm_of(trace(1), 10)}, None, budget, art, cpu="cpu-A")
        r = by_id(rows)
        sv, ins = r.get("syscalls-vs-base/full-snap-c1"), r.get("instructions/create")
        return (sv is not None and sv[3] == "PASS" and art_sha in sv[1]
                and ins is not None and ins[3] == "PASS" and art_sha in ins[1] and regression_green(rows))
    cases.append(("review 7b: no in-job base, the artifact present: syscalls-vs-base/full-snap-c1 and instructions both "
                  "compare against it (PASS, labelled with its sha) and regression_green is true",
                  guarded(case_artifact_runs)))

    def case_artifact_catches():
        hb = arm_of(trace(1), 10)
        hb["strace"]["windows"]["create"]["syscalls_per_op"]["fstat"] = 1.0
        sv = by_id(gates({"full-snap-c1": hb}, None, budget, art, cpu="cpu-A")).get("syscalls-vs-base/full-snap-c1")
        return sv is not None and sv[3] == "FAIL" and "fstat" in sv[2]
    cases.append(("review 7b: a new syscall per create against the artifact FAILs (the comparison runs)",
                  guarded(case_artifact_catches)))

    def case_artifact_other_cpu():
        rows = gates({"full-snap-c1": arm_of(trace(1), 10)}, None, budget, art, cpu="cpu-B")
        r = by_id(rows)
        sv, ins = r.get("syscalls-vs-base/full-snap-c1"), r.get("instructions/create")
        return (sv is not None and sv[3] == "FAIL" and "cpu" in sv[2] and "cpu-B" in sv[2]
                and ins is not None and ins[3] == "FAIL" and "cpu" in ins[2] and not regression_green(rows))
    cases.append(("review 7b: an artifact from a different cpu is refused: both regression gates FAIL naming the cpu, "
                  "regression_green false", guarded(case_artifact_other_cpu)))

    def case_artifact_unlabelled():
        out = []
        for a, cpu in ((dict(art, cpu=None), "cpu-A"), (art, None), (dict(art, sha=None), "cpu-A"),
                       ({k: x for k, x in art.items() if k != "cpu"}, "cpu-A")):
            r = by_id(gates({"full-snap-c1": arm_of(trace(1), 10)}, None, budget, a, cpu=cpu))
            out.append(r.get("syscalls-vs-base/full-snap-c1", ("", "", "", "absent"))[3] == "FAIL"
                       and r.get("instructions/create", ("", "", "", "absent"))[3] == "FAIL")
        return all(out) and len(out) == 4
    cases.append(("review 7b: an artifact without a cpu or a sha, or a runner without a cpu, is refused (both FAIL)",
                  guarded(case_artifact_unlabelled)))

    def case_artifact_no_syscalls():
        a = {k: x for k, x in art.items() if k != "create_syscalls_per_op"}
        rows = gates({"full-snap-c1": arm_of(trace(1), 10)}, None, budget, a, cpu="cpu-A")
        sv = by_id(rows).get("syscalls-vs-base/full-snap-c1")
        return sv is not None and sv[3] == "FAIL" and not regression_green(rows)
    cases.append(("review 7b: an artifact without create_syscalls_per_op is refused, regression_green false",
                  guarded(case_artifact_no_syscalls)))

    def case_injob_outranks():
        rows = gates({"full-snap-c1": arm_of(trace(1), 10)}, {"full-snap-c1": arm_of(trace(1), 10)}, budget,
                     dict(art, cpu="cpu-B"), cpu="cpu-A")
        r = by_id(rows)
        sv, ins = r.get("syscalls-vs-base/full-snap-c1"), r.get("instructions/create")
        return (sv is not None and sv[3] == "PASS" and "base built in this job" in sv[1] and art_sha not in sv[1]
                and ins is not None and ins[3] == "PASS" and "base built in this job" in ins[1] and regression_green(rows))
    cases.append(("review 7b: an in-job base outranks the artifact (a different-cpu artifact is never consulted)",
                  guarded(case_injob_outranks)))

    # review 28: the budget job (yml:224) went green on ANY arm's PASS, even when the budget-syscalls/full-snap-c1 row,
    # the one the absolute budget binds to, was absent. Its verdict is budget_verdict(), read from verdict.tsv.
    def case_budget_needs_baseline_arm():
        ok, why = budget_verdict(gates({"full-cat-c1": arm_of(trace(1), 10)}, None, budget, None))
        ok2, _ = budget_verdict(gates({"full-snap-c1": arm_of(trace(1), 10)}, None, budget, None))
        ok3, _ = budget_verdict(gates({"full-snap-c1": arm_of(trace(1), 10), "full-cat-c1": arm_of(trace(1), 10)},
                                      None, budget, None))
        return not ok and any("budget-syscalls/full-snap-c1" in w for w in why) and ok2 and ok3
    cases.append(("review 28: the budget verdict refuses when budget-syscalls/full-snap-c1 is absent, even beside "
                  "another arm's PASS, and passes with it present", guarded(case_budget_needs_baseline_arm)))

    def case_budget_fails():
        a, _ = budget_verdict(gates({"full-snap-c1": arm_of(trace(2), 20)}, None, budget, None))
        b, why = budget_verdict(gates({"full-snap-c1": arm_of(trace(1), 10), "full-cat-c1": arm_of(trace(2), 20)},
                                      None, budget, None))
        return not a and not b and any("budget-syscalls/full-cat-c1" in w for w in why)
    cases.append(("review 28: a budget FAIL on full-snap-c1, or on any other arm beside its PASS, fails the verdict",
                  guarded(case_budget_fails)))

    def case_budget_unevaluated():
        ok, why = budget_verdict(gates({"full-snap-c1": arm_of(trace(1), 10, clients=64)}, None, budget, None))
        return not ok and any("budget-syscalls/full-snap-c1" in w for w in why)
    cases.append(("review 28: a budget-syscalls/full-snap-c1 row that is not PASS or FAIL (INFO) is refused",
                  guarded(case_budget_unevaluated)))

    def case_verdict_file():
        import shutil
        import tempfile

        def raises(fn):
            try:
                fn()
            except (OSError, ValueError):
                return True
            return False
        d = tempfile.mkdtemp(prefix="analyze-selftest-")
        try:
            rows = gates({"full-snap-c1": arm_of(trace(1), 10)}, None, budget, None)
            p = os.path.join(d, "verdict.tsv")
            write_verdict(p, rows)
            same = [tuple(r) for r in read_verdict(p)] == [tuple(r) for r in rows]
            tab = raises(lambda: write_verdict(os.path.join(d, "tab.tsv"), [rows[0][:2] + ("a\tb",) + rows[0][3:]]))
            open(p, "w").write("")
            empty = raises(lambda: read_verdict(p))
            open(p, "w").write("budget-syscalls/full-snap-c1\t<= 3 syscalls per create\tPASS\n")
            short = raises(lambda: read_verdict(p))
            missing = raises(lambda: read_verdict(os.path.join(d, "absent.tsv")))
            return same and tab and empty and short and missing
        finally:
            shutil.rmtree(d, ignore_errors=True)
    cases.append(("review 28: verdict.tsv round-trips through write_verdict/read_verdict; a tab inside a field, an empty "
                  "or missing file, and a short line are refused", guarded(case_verdict_file)))

    # review 29: regression_green named its two premises by id and exempted the absolute budget by id prefix
    # (analyze.py:370 at 2d42982a0). Every row now carries a kind: "regression" (must PASS for regression_green),
    # "budget" (the absolute budget; the budget job's), "info" (never gates; INFO or NOT-RUN only).
    def mixed_rows(b=None):
        return gates({"full-snap-c1": arm_of(trace(1), 10), "full-snap-c64": arm_of(trace(1), 10, clients=64),
                      "async-snap-c1": {"arm": "async-snap-c1", "status": "NOT AVAILABLE: no async class"}},
                     {"full-snap-c1": arm_of(trace(1), 10), "full-snap-c64": arm_of(trace(1), 10, clients=64)},
                     b or budget, None)

    def case_rows_carry_kind():
        rows = mixed_rows()
        return bool(rows) and all(len(r) == 5 and r[4] in ("regression", "budget", "info") for r in rows)
    cases.append(("review 29: every row carries a kind (regression, budget or info)", guarded(case_rows_carry_kind)))

    def case_kinds_by_row():
        k = {r[0]: r[4] for r in mixed_rows()}
        return (k.get("instructions/create") == "regression" and k.get("syscalls-vs-base/full-snap-c1") == "regression"
                and k.get("budget-flush/full-snap-c1") == "regression"
                and k.get("two-instruments/full-snap-c1/create") == "regression"
                and k.get("syscalls-vs-base/full-snap-c64") == "info" and k.get("budget-flush/async-snap-c1") == "info"
                and k.get("budget-syscalls/full-snap-c1") == "budget" and k.get("budget-syscalls/full-snap-c64") == "budget")
    cases.append(("review 29: kinds by row: instructions, syscalls-vs-base at C=1, flush and two-instrument rows are "
                  "regression; C>1 syscalls-vs-base and an unavailable class are info; budget-syscalls is budget",
                  guarded(case_kinds_by_row)))

    def case_green_reads_kinds():
        clean = gates({"full-snap-c1": arm_of(trace(1), 10)}, {"full-snap-c1": arm_of(trace(1), 10)}, budget, None)
        return (regression_green(clean)
                and not regression_green(clean + [("planted/x", "p", "p", "NOT-RUN", "regression")])
                and not regression_green(clean + [("budget-syscalls/planted", "p", "p", "FAIL", "regression")])
                and regression_green(clean + [("planted-budget/x", "p", "p", "FAIL", "budget")])
                and regression_green(clean + [("planted-info/x", "p", "p", "NOT-RUN", "info")]))
    cases.append(("review 29: regression_green reads kinds, not ids: a regression row that is not PASS blocks it under "
                  "any id; a budget FAIL or an info row under any id does not", guarded(case_green_reads_kinds)))

    def case_budget_reads_kinds():
        rows = gates({"full-snap-c1": arm_of(trace(1), 10)}, None, budget, None)
        ok, _ = budget_verdict(rows)
        bad_, why = budget_verdict(rows + [("planted-budget/x", "p", "p", "FAIL", "budget")])
        return ok and not bad_ and any("planted-budget/x" in w for w in why)
    cases.append(("review 29: the budget verdict reads budget-kind rows, not an id prefix",
                  guarded(case_budget_reads_kinds)))

    def case_premise_always_present():
        none = by_id(gates({"full-snap-c1": arm_of(trace(1), 10)}, None, budget, None)).get("syscalls-vs-base/full-snap-c1")
        b2 = dict(budget, classes={k: x for k, x in budget["classes"].items() if k != "full"})
        unreg_rows = gates({"full-snap-c1": arm_of(trace(1), 10)}, {"full-snap-c1": arm_of(trace(1), 10)}, b2, None)
        unreg = by_id(unreg_rows).get("syscalls-vs-base/full-snap-c1")
        return (none is not None and none[3] == "NOT-RUN" and none[4] == "regression"
                and unreg is not None and unreg[3] != "PASS" and unreg[4] == "regression"
                and not regression_green(unreg_rows))
    cases.append(("review 29: the syscalls-vs-base/full-snap-c1 premise row always exists (NOT-RUN, regression) when "
                  "nothing compared it, with no base or with the arm cut short", guarded(case_premise_always_present)))

    def case_verdict_file_kinds():
        import shutil
        import tempfile
        d = tempfile.mkdtemp(prefix="analyze-selftest-")
        try:
            p = os.path.join(d, "v.tsv")
            refused = []
            for line in ("x\te\tg\tFAIL\tinfo", "x\te\tg\tPASS\tinfo", "x\te\tg\tPASS\tweird", "x\te\tg\tMAYBE\tregression"):
                open(p, "w").write(line + "\n")
                try:
                    read_verdict(p)
                    refused.append(False)
                except ValueError:
                    refused.append(True)
            open(p, "w").write("x\te\tg\tNOT-RUN\tinfo\n")
            return all(refused) and len(refused) == 4 and read_verdict(p) == [("x", "e", "g", "NOT-RUN", "info")]
        finally:
            shutil.rmtree(d, ignore_errors=True)
    cases.append(("review 29: read_verdict refuses an unknown kind or verdict and an info row that PASSes or FAILs",
                  guarded(case_verdict_file_kinds)))
    bad = [name for name, good in cases if not good]
    for name, good in cases:
        print(f"self-test {'PASS' if good else 'FAIL'}: {name}")
    return not bad


def regression_green(rows):
    """Every regression-kind row PASSes (T3 review item 29: by kind, never by gate id). gates() always emits
    instructions/create and syscalls-vs-base/<BASELINE_ARM> as regression rows, INFO or NOT-RUN when they could not
    be evaluated, so gate-6 review L3 (both regression gates evaluated: a run with every row INFO, or a base that
    failed to build, must not advance the baseline) holds by construction. The absolute budget's rows are
    budget-kind: it is red at 675adbfb3 (10 per create) and stays red until the engine meets it; if it blocked the
    baseline, no run would ever be green, and the instruction and syscalls-vs-base regression gates would have no
    base at all. So the baseline advances on regression-green runs while the budget job stays red (PROFILE.md)."""
    for r in rows:
        why = row_problem(r)
        if why:
            raise ValueError(f"regression_green: row {r[:1]} is not representable: {why}")
    reg = [r for r in rows if r[4] == "regression"]
    return bool(reg) and all(r[3] == "PASS" for r in reg)


def write_verdict(path, rows):
    """verdict.tsv, one row per line, VERDICT_FIELDS fields. A row row_problem() rejects (a tab or newline inside
    a field would shift or split it for every reader; an unknown verdict or kind) is refused (ValueError) rather
    than written."""
    for r in rows:
        why = row_problem(r)
        if why:
            raise ValueError(f"unwritable verdict row {r!r}: {why}")
    with open(path, "w") as f:
        for r in rows:
            f.write("\t".join(r) + "\n")


def read_verdict(path):
    """verdict.tsv back as tuples. A missing file raises OSError; an empty file, or a line row_problem() rejects
    (not exactly VERDICT_FIELDS fields, an unknown verdict or kind, an info row that PASSes or FAILs), raises
    ValueError: a verdict that cannot be read is never read as a pass."""
    with open(path) as f:
        lines = f.read().splitlines()
    rows = []
    for n, line in enumerate(lines, 1):
        fields = tuple(line.split("\t"))
        why = row_problem(fields)
        if why:
            raise ValueError(f"{path}:{n}: {why}: {line!r}")
        rows.append(fields)
    if not rows:
        raise ValueError(f"{path}: no rows")
    return rows


def budget_verdict(rows):
    """The absolute syscall budget job's verdict (T3 review item 28; the job used to go green on ANY arm's PASS).
    The budget-kind row of the arm the budget binds to, budget-syscalls/<BASELINE_ARM>, must be present exactly
    once and evaluated (PASS or FAIL), and no budget-kind row may FAIL (item 29: by kind, not by id prefix).
    Returns (ok, reasons)."""
    need = f"budget-syscalls/{BASELINE_ARM}"
    budget_rows = [r for r in rows if r[4] == "budget"]
    mine = [r for r in budget_rows if r[0] == need]
    reasons = []
    if not mine:
        reasons.append(f"no {need} row: the budget was not evaluated on the arm it binds to")
    elif len(mine) > 1:
        reasons.append(f"{len(mine)} {need} rows, expected one")
    elif mine[0][3] not in ("PASS", "FAIL"):
        reasons.append(f"{need} is {mine[0][3]}, not evaluated: {mine[0][2]}")
    reasons += [f"{r[0]} FAIL: {r[2]}" for r in budget_rows if r[3] == "FAIL"]
    return not reasons, reasons


def main(argv):
    if argv[1:] == ["--self-test"]:
        return 0 if self_test() else 1
    if not self_test():
        print("analyze: self-test FAILED: the gates cannot be trusted", file=sys.stderr)
        return 1
    if argv[1:2] == ["--budget-verdict"]:
        # the "absolute syscall budget" job of fastest-profile.yml, on the profile job's verdict.tsv
        if len(argv) != 3:
            print("usage: analyze.py --budget-verdict <verdict.tsv>", file=sys.stderr)
            return 2
        try:
            vrows = read_verdict(argv[2])
        except (OSError, ValueError) as e:
            print(f"analyze: budget verdict REFUSED: {e}", file=sys.stderr)
            return 1
        for r in vrows:
            if r[4] == "budget":
                print("\t".join(r))
        ok, reasons = budget_verdict(vrows)
        for w in reasons:
            print(f"analyze: budget: {w}")
        print(f"analyze: absolute syscall budget {'PASS' if ok else 'FAIL'}")
        return 0 if ok else 1
    raw, budget_path, out = argv[1], argv[2], argv[3]
    baseline = None
    if "--baseline" in argv:
        p = argv[argv.index("--baseline") + 1]
        if os.path.exists(p):
            baseline = json.load(open(p))
    budget = json.load(open(budget_path))
    sides = {}
    for side in ("head", "base"):
        sd = os.path.join(raw, side)
        if os.path.isdir(sd):
            sides[side] = {a: analyze_arm(os.path.join(sd, a), a) for a in sorted(os.listdir(sd))
                           if os.path.isdir(os.path.join(sd, a))}
    if not sides.get("head"):
        print("analyze: no head arms: nothing was measured", file=sys.stderr)
        return 1
    cpu = os.environ.get("PROFILE_CPU")
    rows = gates(sides["head"], sides.get("base"), budget, baseline, cpu=cpu)
    os.makedirs(out, exist_ok=True)
    json.dump(sides, open(os.path.join(out, "summary.json"), "w"), indent=1)
    write_verdict(os.path.join(out, "verdict.tsv"), rows)
    h = sides["head"].get(BASELINE_ARM, {})
    json.dump({"sha": os.environ.get("GITHUB_SHA"), "ir_create": h.get("ir_per_op", {}).get("create"),
               "ir_per_op": h.get("ir_per_op"), "cpu": cpu,
               "create_syscalls_per_op": ((h.get("strace") or {}).get("windows", {}).get("create") or {}).get("syscalls_per_op")},
              open(os.path.join(out, "baseline.json"), "w"), indent=1)
    with open(os.path.join(out, "summary.md"), "w") as f:
        f.write("| gate | expected | got | verdict | kind |\n|---|---|---|---|---|\n")
        for g, e, got, v, kind in rows:
            f.write(f"| {g} | {e} | {got[:200]} | {v} | {kind} |\n")
        f.write("\n| side | arm | create p50/p99 us | flushes/op create | syscalls/op create | Ir/op create | busy retries/op create |\n|---|---|---|---|---|---|---|\n")
        for side, arms in sides.items():
            for a, r in arms.items():
                lat = r.get("latency_ns", {}).get("create", {})
                cw = ((r.get("strace") or {}).get("windows") or {}).get("create") or {}
                f.write(f"| {side} | {a} | {lat.get('p50_ns', 0) / 1e3:.1f} / {lat.get('p99_ns', 0) / 1e3:.1f} | "
                        f"{cw.get('flushes_per_op')} | {cw.get('syscalls_total_per_op')} | {r.get('ir_per_op', {}).get('create')} | "
                        f"{r.get('busy_retries_per_op', {}).get('create')} |\n")
    for r in rows:
        print("\t".join(r))
    rg = regression_green(rows)
    open(os.path.join(out, "regression_green"), "w").write("1\n" if rg else "0\n")
    print(f"analyze: regression_green={int(rg)} (every regression-kind row PASS; budget-kind rows are the budget job's)")
    fails = [r for r in rows if r[3] == "FAIL"]
    counted = [r for r in rows if r[3] in ("PASS", "FAIL")]
    if not counted:
        print("analyze: no gate was evaluated", file=sys.stderr)
        return 1
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
