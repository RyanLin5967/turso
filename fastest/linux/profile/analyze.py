#!/usr/bin/env python3
"""fastest-linux profiling analysis: per-op syscall, flush, perf-stat and instruction counts from
profile.sh's raw output, gated against budget.json and against the base build.

usage: analyze.py <raw-dir> <budget.json> <out-dir> [--baseline prev-baseline.json] [--rebaseline]
       analyze.py --self-test

<raw-dir> holds head/ and, when a base was built, base/ (profile.sh's layout). Writes to <out-dir>:
  summary.json   every number, per side and arm
  verdict.tsv    gate, expected, got, PASS|FAIL|INFO|NOT-RUN
  baseline.json  this run's head numbers, the next run's baseline once this run is green
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
        r["ops"] = p["clients"] * p["ops_per_client"]
        r["latency_ns"] = {w: {k: p["phases"][w][k] for k in ("p50_ns", "p99_ns", "max_ns")} for w in WINDOWS}
        r["busy_retries_per_op"] = {w["window"]: round(w.get("busy_retries", 0) / r["ops"], 4) for w in p["windows"]}
    srun = os.path.join(d, "strace-run", "summary.json")
    st = os.path.join(d, "strace.txt")
    if not os.path.exists(st) and os.path.exists(st + ".gz"):
        st += ".gz"
    if os.path.exists(st) and os.path.exists(srun):
        s = json.load(open(srun))
        ops = s["clients"] * s["ops_per_client"]
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


def gates(head, base, budget, baseline):
    rows = []

    def row(g, exp, got, v):
        rows.append((g, exp, str(got), v))

    for arm, h in sorted(head.items()):
        cls = arm.split("-")[0]
        if "NOT AVAILABLE" in h.get("status", ""):
            row(f"budget-flush/{arm}", "create flushes per op in budget", h["status"], "NOT-RUN")
            continue
        s = h.get("strace")
        if not s:
            row(f"budget-flush/{arm}", "a strace count", "no strace data", "FAIL")
            continue
        if s["refusals"]:
            row(f"budget-flush/{arm}", "no uncountable durability path", "; ".join(s["refusals"]), "FAIL")
            continue
        missing = [w for w in WINDOWS if s["windows"].get(w) is None]
        if missing:
            row(f"windows/{arm}", "all four windows marked", f"missing {missing}", "FAIL")
            continue
        for w in WINDOWS:
            x = s["windows"][w]
            if x["engine_sync_counter"] is not None and x["engine_sync_counter"] != x["strace_fsync_like"]:
                row(f"two-instruments/{arm}/{w}", "engine sync counter == strace fsync+fdatasync",
                    f"engine {x['engine_sync_counter']} strace {x['strace_fsync_like']}", "FAIL")
            else:
                row(f"two-instruments/{arm}/{w}", "engine sync counter == strace fsync+fdatasync",
                    f"{x['strace_fsync_like']}", "PASS")
        spec = budget["classes"].get(cls)
        if spec is None:
            row(f"budget-flush/{arm}", "a registered class", cls, "INFO")
            continue
        c = h.get("clients", 1)
        lo, hi = lower_bound(spec["create_flushes_per_op_c1" if c == 1 else "create_flushes_per_op_cn"], c)
        x = s["windows"]["create"]
        f = x["flushes_per_op"]
        row(f"budget-flush/{arm}", f"{lo:.4g} <= create flushes/op <= {hi:.4g}", f, "PASS" if lo <= f <= hi else "FAIL")
        allowed = spec["create_flush_files_allowed"]
        bad = {t: n for t, n in x["flush_targets"].items() if not any(t.endswith(a) for a in allowed)}
        row(f"budget-flush-files/{arm}", f"create flushes only on {allowed or 'nothing'}", bad or "ok", "FAIL" if bad else "PASS")
        if base and arm in base and (base[arm].get("strace") or {}).get("windows", {}).get("create"):
            bx = base[arm]["strace"]["windows"]["create"]["syscalls_per_op"]
            hx = x["syscalls_per_op"]
            sv = budget["syscalls_vs_base"]
            excl = set(sv["timing_dependent_excluded"])
            worse = {k: (bx.get(k, 0.0), v) for k, v in hx.items()
                     if k not in excl and v > bx.get(k, 0.0) + sv["per_op_slack"]}
            # At C > 1 the counts move with contention, not with the code: run 37400090036 built the SAME
            # engine on both sides and arm64 C=64 read getpid 3.21 -> 3.28 per create (a waiter's poll
            # loop runs as often as the flight takes). So the gate binds at C=1, where counts are exact
            # (getpid 4.005, run 37255309860), and C > 1 is recorded as INFO.
            verdict = ("FAIL" if worse else "PASS") if h.get("clients", 1) == 1 else "INFO"
            row(f"syscalls-vs-base/{arm}", f"no create syscall above base + {sv['per_op_slack']}/op"
                + ("" if h.get("clients", 1) == 1 else " (C>1: contention-dependent, INFO)"),
                worse or "ok", verdict)
    ins = budget["instructions"]
    arm = "full-snap-c1"
    h = head.get(arm, {}).get("ir_per_op", {}).get("create")
    b = None
    src = None
    if base and base.get(arm, {}).get("ir_per_op", {}).get("create"):
        b, src = base[arm]["ir_per_op"]["create"], "base built in this job"
    elif baseline and baseline.get("ir_create"):
        b, src = baseline["ir_create"], f"baseline artifact of {baseline.get('sha')}"
    if h is None:
        row("instructions/create", "a callgrind count", "none", "FAIL")
    elif b is None:
        row("instructions/create", f"<= base x {1 + ins['create_growth_max']}", f"head {h} Ir/op; no baseline (first run)", "INFO")
    else:
        g = h / b - 1
        row("instructions/create", f"growth <= {ins['create_growth_max']:.0%} vs {src}",
            f"head {h} base {b} growth {g:+.2%}", "PASS" if g <= ins["create_growth_max"] else "FAIL")
    return rows


def self_test():
    """Plant each breach the gates exist to catch and require each to FAIL; and a clean case PASS."""
    budget = {"classes": {"full": {"create_flushes_per_op_c1": [1.0, 1.01], "create_flushes_per_op_cn": ["1/C", 1.01],
                                   "create_flush_files_allowed": ["-branch-log"]},
                          "async": {"create_flushes_per_op_c1": [0, 0], "create_flushes_per_op_cn": [0, 0],
                                    "create_flush_files_allowed": []}},
              "instructions": {"create_growth_max": 0.05},
              "syscalls_vs_base": {"per_op_slack": 0.05, "timing_dependent_excluded": ["futex"]}}

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
        return {g: v for g, _, _, v in gates(head, base, budget, baseline)}

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
    bad = [name for name, good in cases if not good]
    for name, good in cases:
        print(f"self-test {'PASS' if good else 'FAIL'}: {name}")
    return not bad


def main(argv):
    if argv[1:] == ["--self-test"]:
        return 0 if self_test() else 1
    if not self_test():
        print("analyze: self-test FAILED: the gates cannot be trusted", file=sys.stderr)
        return 1
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
    rows = gates(sides["head"], sides.get("base"), budget, baseline)
    os.makedirs(out, exist_ok=True)
    json.dump(sides, open(os.path.join(out, "summary.json"), "w"), indent=1)
    with open(os.path.join(out, "verdict.tsv"), "w") as f:
        for r in rows:
            f.write("\t".join(r) + "\n")
    h = sides["head"].get("full-snap-c1", {})
    json.dump({"sha": os.environ.get("GITHUB_SHA"), "ir_create": h.get("ir_per_op", {}).get("create"),
               "ir_per_op": h.get("ir_per_op"), "cpu": os.environ.get("PROFILE_CPU"),
               "create_syscalls_per_op": ((h.get("strace") or {}).get("windows", {}).get("create") or {}).get("syscalls_per_op")},
              open(os.path.join(out, "baseline.json"), "w"), indent=1)
    with open(os.path.join(out, "summary.md"), "w") as f:
        f.write("| gate | expected | got | verdict |\n|---|---|---|---|\n")
        for g, e, got, v in rows:
            f.write(f"| {g} | {e} | {got[:200]} | {v} |\n")
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
    fails = [r for r in rows if r[3] == "FAIL"]
    counted = [r for r in rows if r[3] in ("PASS", "FAIL")]
    if not counted:
        print("analyze: no gate was evaluated", file=sys.stderr)
        return 1
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
