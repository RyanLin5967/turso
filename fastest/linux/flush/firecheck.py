#!/usr/bin/env python3
"""firecheck.py --build B --work W --out OUT --fs ext4|xfs --strace-tools T

Fire-check of the V1 Linux flush counter (syncshim.so + v1run + v1ctl), on this runner and on the filesystem that
holds W. Every expected count is computed from K alone (plus the kernel's F_SETFL behaviour, measured WITHOUT the
shim, and the filesystem's reflink support, which comes from --fs and is checked against the mount table). Nothing is
read back from the shim to form an expectation; the pids the probes print only say which slot is which process.

The kernel-side witness in the raw, Go and sqlite arms is the strace counter that passed its own fire-check in CI
(fastest/linux/competitors/{trace.sh,stracecount.py}, lane fastest-linux-comp), used in launch mode (strace_run) from
directory T. It is not a second instrument written here.

Arms (each must hold; the run exits 1 on any failure, 2 on a refused setup, and writes OUT/verdict.json either way):
  setfl-truth          F_SETFL O_DSYNC measured without the shim (Linux ignores it: expect 0, counted as measured)
  C-full K=7, K=23     exact per-process counts and fails for the root, a forked child driven by the parent's marks,
                       one child per exec route (posix_spawn, posix_spawnp, fork+execve/execv/execvp/execvpe/execl/
                       execle/execlp/fexecve/execveat), a vfork+execve child and a /bin/sh -c 'exec' child; exact
                       per-mark rows; exact sync_file_range and msync flags per event; the exec table (14 records,
                       every one attached, via and state as issued); 16 slots, none Go, no unresolved interposes
  checker-fires        the K=7 counts against the K=8 expectation must mismatch
  mutant-<name> x3     C-full K=7 under a shim missing one interpose (pwrite64, __open_2, fdatasync) must mismatch,
                       and by exactly the predicted deficit
  C-noise              only uncounted calls: attached, every kind 0
  raw K=7, K=23        syncall(2) and an inline syscall instruction: the shim counts 0 of them (MISSED, reported per
                       kind against the strace witness) and exactly K of the libc control calls in the same process
  Go-nocgo K=7, K=23   CGO_ENABLED=0 (static): report refuses rc 4, nothing attached; strace sees every call exactly
  Go-cgo K=7, K=23     CGO_ENABLED=1: report refuses rc 7; with --allow-go the root counts exactly the K cgo fsyncs and
                       nothing of Go's raw calls, the os/exec child attaches (Go flag) and counts 0; strace exact
  sqlite               a real library caller (system libsqlite3, one commit): shim counts == strace counts, >= 1
  sigkill              K fsync, then SIGKILL: the counts survive in shared memory
  refuse-*             SYNCSHIM_RUN unset -> 97; missing run -> 97; nothing attached -> rc 3; static root -> rc 4;
                       static / env-stripped children -> rc 6; event-log overflow -> rc 5 (waivable, counts exact);
                       slot overflow -> rc 5 (not waivable, totals exact); exec-table overflow -> rc 5; a second
                       v1run on one run -> 2
"""
import argparse
import collections
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time

KINDS = ["fsync", "fdatasync", "sfr_write_wait", "sfr_write", "sfr_wait", "syncfs", "sync", "msync_SYNC",
         "msync_other", "osync_write", "odsync_write", "FICLONE", "FICLONERANGE", "copy_file_range"]
ROUTES = ["posix_spawn", "posix_spawnp", "execve", "execv", "execvp", "execvpe", "execl", "execle", "execlp",
          "fexecve", "execveat"]
MS_ASYNC, MS_INVALIDATE, MS_SYNC = 1, 2, 4  # <sys/mman.h> on x86_64 and aarch64
SFR_WB, SFR_W, SFR_WA = 1, 2, 4             # SYNC_FILE_RANGE_WAIT_BEFORE / WRITE / WAIT_AFTER
MUTANTS = {  # mutant -> {role: {kind: deficit as a function of K}}
    "pwrite64": lambda K: {"root": {"odsync_write": -K}},
    "open_2": lambda K: {"root": {"odsync_write": -K}},
    "fdatasync": lambda K: {"root": {"fdatasync": -K}, "fork_child": {"fdatasync": -K}},
}
REGISTERED = (["setfl-truth", "C-full K=7", "C-full K=23", "checker-fires: K=7 counts vs K=8 expectation"] +
              ["mutant-%s caught with the exact deficit" % m for m in MUTANTS] + ["C-noise"] +
              ["raw K=%d" % k for k in (7, 23)] + ["Go-%s K=%d" % (f, k) for f in ("nocgo", "cgo") for k in (7, 23)] +
              ["sqlite: shim == strace", "sigkill: counts survive",
               "refuse: SYNCSHIM_RUN unset -> 97", "refuse: missing run -> 97", "refuse: nothing attached -> rc 3",
               "refuse: static root -> rc 4", "refuse: static and env-stripped children -> rc 6",
               "refuse: event-log overflow -> rc 5, waivable, counts exact",
               "refuse: slot overflow -> rc 5, not waivable, totals exact",
               "refuse: exec-table overflow -> rc 5", "refuse: second v1run on one run -> 2"])

ap = argparse.ArgumentParser()
ap.add_argument("--build", required=True)
ap.add_argument("--work", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--fs", required=True, choices=["ext4", "xfs", "btrfs"])
ap.add_argument("--strace-tools", required=True)
A = ap.parse_args()
B, W, OUT, T = (os.path.abspath(p) for p in (A.build, A.work, A.out, A.strace_tools))
REFLINK = A.fs in ("xfs", "btrfs")
os.makedirs(OUT, exist_ok=True)
os.makedirs(W, exist_ok=True)
results = []
RUNSEQ = [0]
SETFL_TRUTH = None


def sh(args, env=None, timeout=600):
    return subprocess.run(args, capture_output=True, text=True, env=env, timeout=timeout)


def save(name, text):
    with open(os.path.join(OUT, name), "w") as f:
        f.write(text)


def arm(name, ok, detail):
    results.append({"arm": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else ": " + json.dumps(detail)[:3000]), flush=True)


def counts(**kw):
    c = {k: 0 for k in KINDS}
    for k, v in kw.items():
        assert k in c, k
        c[k] = v
    return c


def new_run(tag, *extra):
    RUNSEQ[0] += 1
    run = "fc%d%s%d" % (os.getpid() % 10000, tag, RUNSEQ[0])
    sh([B + "/v1ctl", "rm", run])
    r = sh([B + "/v1ctl", "create", run] + list(extra))
    if r.returncode != 0:
        raise RuntimeError("v1ctl create %s: %s" % (run, r.stderr))
    return run


def rm(run):
    sh([B + "/v1ctl", "rm", run])


def report(run, *flags):
    r = sh([B + "/v1ctl", "report", run, "--json"] + list(flags))
    save(run + ".report%s.json" % "".join(f.replace("--", "_") for f in flags), r.stdout + r.stderr)
    rep = json.loads(r.stdout) if r.stdout.strip().startswith("{") else None
    return r.returncode, rep


def bymark(run):
    r = sh([B + "/v1ctl", "bymark", run])
    save(run + ".bymark.tsv", r.stdout)
    rows = collections.Counter()
    for line in r.stdout.splitlines()[1:]:
        slot, pid, idle, mark, kind, n = line.split("\t")
        rows[(int(pid), int(idle), int(mark), kind)] += int(n)
    return rows


def events(run):
    r = sh([B + "/v1ctl", "events", run])
    save(run + ".events.tsv", r.stdout)
    lines = r.stdout.splitlines()
    head = lines[0].split("\t") if lines else []
    return [dict(zip(head, ln.split("\t"))) for ln in lines[1:]]


def launch(run, argv, shim=None, strace=False, env_extra=None):
    """v1run RUN argv..., optionally under the strace counter (trace.sh strace_run). Returns (proc, strace_json)."""
    env = dict(os.environ)
    if shim:
        env["V1_SHIM"] = shim
    if env_extra:
        env.update(env_extra)
    cmd = [B + "/v1run", run] + argv
    if strace:
        so = os.path.join(OUT, run)
        cmd = ["bash", "-c", 'source "$0" && out=$1 && shift && strace_run "$out" "$@"', T + "/trace.sh", so] + cmd
    r = sh(cmd, env=env)
    save(run + ".probe.txt", "rc=%d\ncmd=%s\n--- stdout\n%s\n--- stderr\n%s" % (r.returncode, cmd, r.stdout, r.stderr))
    sj = None
    if strace:
        c = sh(["python3", "-B", T + "/stracecount.py", "count", so + ".strace", "--extra", so + ".strace.err",
                "--window", so + ".window"])
        save(run + ".stracecount.json", c.stdout + c.stderr)
        sj = json.loads(c.stdout) if c.stdout.strip().startswith("{") else {"verdict": "REFUSED: unparsable",
                                                                              "raw": c.stdout + c.stderr}
    return r, sj


def pids_from(stdout):
    out = {}
    for tok in stdout.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            if v.isdigit():
                out[k] = int(v)
    return out


def by_pid(rep):
    """pid -> {n, counts, fails, go, unresolved} summed over that pid's slots (an exec keeps the pid)."""
    agg = {}
    for s in rep["slots"]:
        a = agg.setdefault(s["pid"], {"n": 0, "counts": counts(), "fails": counts(), "go": [], "unresolved": 0})
        a["n"] += 1
        for k in KINDS:
            a["counts"][k] += s["counts"][k]
            a["fails"][k] += s["fails"][k]
        a["go"].append(s["go"])
        a["unresolved"] |= s["unresolved_mask"]
    return agg


def diff(got, exp):
    return {k: got[k] - exp[k] for k in KINDS if got[k] != exp[k]}


# ---------------------------------------------------------------- C-full expectations (from K, SETFL_TRUTH, REFLINK)
def exp_full(K):
    t = SETFL_TRUTH
    root = counts(fsync=K, fdatasync=K, sfr_write_wait=2 * K, sfr_write=2 * K, sfr_wait=4 * K, syncfs=K, sync=1,
                  msync_SYNC=K, msync_other=2 * K, osync_write=2 * K + 4 * K,
                  odsync_write=10 * K + t * K + 3 * K + 18 * K, FICLONE=K, FICLONERANGE=K, copy_file_range=K)
    roles = {"root": (root, counts() if REFLINK else counts(FICLONE=K, FICLONERANGE=K), 1),
             "fork_child": (counts(fsync=sum(i % 3 + 1 for i in range(1, K + 1)), fdatasync=K), counts(), 1),
             "vfork": (counts(fsync=K), counts(), 1),
             "via_sh": (counts(fsync=K), counts(), 2)}  # /bin/sh's slot, then the probe it exec'd: one pid
    for r in ROUTES:
        roles["route_" + r] = (counts(fsync=K), counts(), 1)
    return roles  # role -> (counts, fails, slots)


def exp_marks(K, p):
    t, root = SETFL_TRUTH, p["root"]
    rows = {(1, "fsync"): K, (2, "fdatasync"): K, (3, "sfr_write_wait"): 2 * K, (4, "sfr_write"): 2 * K,
            (5, "sfr_wait"): 4 * K, (6, "syncfs"): K, (7, "msync_SYNC"): K, (8, "msync_other"): 2 * K,
            (10, "odsync_write"): 10 * K, (12, "osync_write"): 2 * K, (14, "odsync_write"): 3 * K,
            (14, "osync_write"): 4 * K, (15, "odsync_write"): 18 * K, (16, "sync"): 1, (17, "FICLONE"): K,
            (17, "FICLONERANGE"): K, (17, "copy_file_range"): K}
    if t:
        rows[(13, "odsync_write")] = K
    exp = collections.Counter({(root, 0, m, k): n for (m, k), n in rows.items()})
    for i in range(1, K + 1):
        exp[(p["fork_child"], 0, 1000 + i, "fsync")] = i % 3 + 1
        exp[(p["fork_child"], 1, 1000 + i, "fdatasync")] = 1
    for j, r in enumerate(ROUTES):
        exp[(p["route_" + r], 0, 21 + j, "fsync")] = K
    exp[(p["vfork"], 0, 32, "fsync")] = K
    exp[(p["via_sh"], 0, 33, "fsync")] = K
    return exp


def exp_flags(K):
    """(mark, kind, aux) multiset for the events whose aux carries the call's flags."""
    e = collections.Counter()
    for mk, kind, aux in ((3, "sfr_write_wait", SFR_WB | SFR_W | SFR_WA), (3, "sfr_write_wait", SFR_W | SFR_WA),
                          (4, "sfr_write", SFR_W), (4, "sfr_write", SFR_WB | SFR_W), (5, "sfr_wait", SFR_WB),
                          (5, "sfr_wait", SFR_WA), (5, "sfr_wait", SFR_WB | SFR_WA), (5, "sfr_wait", 0),
                          (7, "msync_SYNC", MS_SYNC), (8, "msync_other", MS_ASYNC), (8, "msync_other", MS_INVALIDATE)):
        e[(mk, kind, aux)] += K
    return e


def exp_execs(p):
    """The exec table, as issued: (pid, by_pid, via, state) for every record except /bin/sh's own exec."""
    root, e = p["root"], collections.Counter()
    for r in ROUTES:
        c = p["route_" + r]
        e[(c, root, r, "spawned") if r.startswith("posix_spawn") else (c, c, r, "exec")] += 1
    e[(p["vfork"], p["vfork"], "execve", "exec")] += 1
    e[(p["via_sh"], root, "posix_spawn", "spawned")] += 1
    return e


def run_full(K, shim=None, tag="c"):
    """C-full at K: (ok, detail, role_diffs, raw). role_diffs = {role: {kind: got - expected}} for count mismatches."""
    run = new_run(tag)
    r, _ = launch(run, [B + "/probe_c", "full", str(K), W], shim=shim)
    if r.returncode != 0:
        rm(run)
        return False, {"probe_rc": r.returncode, "stderr": r.stderr[-800:]}, None, None
    p = pids_from(r.stdout)
    rc, rep = report(run)
    bm = bymark(run)
    ev = events(run)
    rm(run)
    d = {"report_rc": rc, "verdict": rep and rep["verdict"]}
    ok = rc == 0 and rep is not None
    if rep is None:
        return False, d, None, None
    for key in ("events_dropped", "events_incomplete", "execs_dropped", "execs_unwritten", "execs_unattached",
                "slot_overflow", "fd_untracked", "unresolved_mask", "go_slots"):
        if rep[key] != 0:
            ok = False
            d[key] = rep[key]
    if rep["root_pid"] != p.get("root"):
        ok = False
        d["root_pid"] = [rep["root_pid"], p.get("root")]
    agg = by_pid(rep)
    roles = exp_full(K)
    diffs = {}
    for role, (ec, ef, nslots) in roles.items():
        pid = p.get(role)
        a = agg.get(pid)
        if a is None:
            ok = False
            d.setdefault("missing_role", []).append(role)
            continue
        dc, df = diff(a["counts"], ec), diff(a["fails"], ef)
        if dc:
            diffs[role] = dc
        if dc or df or a["n"] != nslots:
            ok = False
            d[role] = {"count_diff": dc, "fail_diff": df, "slots": [a["n"], nslots]}
    extra = set(agg) - {p.get(role) for role in roles}
    if extra or len(rep["slots"]) != sum(n for _, _, n in roles.values()):
        ok = False
        d["slots"] = {"n": len(rep["slots"]), "expected": sum(n for _, _, n in roles.values()),
                      "unknown_pids": sorted(extra)}
    want = (0 if REFLINK else K, 0 if REFLINK else K)
    if (p.get("clone_fail"), p.get("range_fail")) != want:
        ok = False
        d["probe_clone_fails"] = [p.get("clone_fail"), p.get("range_fail"), list(want)]
    em = exp_marks(K, p)
    if bm != em:
        ok = False
        d["marks_missing"] = sorted((k, v) for k, v in (em - bm).items())[:12]
        d["marks_extra"] = sorted((k, v) for k, v in (bm - em).items())[:12]
    fl = collections.Counter((int(e["mark"]), e["kind"], int(e["aux"])) for e in ev
                             if int(e["pid"]) == p.get("root") and e["kind"].startswith(("sfr_", "msync_")))
    ef_ = exp_flags(K)
    if fl != ef_:
        ok = False
        d["flags_missing"] = sorted((ef_ - fl).items())[:12]
        d["flags_extra"] = sorted((fl - ef_).items())[:12]
    xs = rep["execs"]
    got_x = collections.Counter((x["pid"], x["by_pid"], x["via"], x["state"]) for x in xs
                                if x["by_pid"] != p.get("via_sh"))
    sh_own = [x for x in xs if x["by_pid"] == p.get("via_sh")]
    ex = exp_execs(p)
    if got_x != ex or len(sh_own) != 1 or sh_own[0]["pid"] != p.get("via_sh") or sh_own[0]["state"] != "exec" \
            or not all(x["attached"] for x in xs) or len(xs) != 14:
        ok = False
        d["execs"] = {"missing": sorted(map(list, (ex - got_x).keys())), "extra": sorted(map(list, (got_x - ex).keys())),
                      "sh_own": sh_own, "n": len(xs), "unattached": [x for x in xs if not x["attached"]]}
    return ok, d, diffs, {"rep": rep, "p": p}


def strace_expect(K, flavour):
    """The strace counter's view of probe_go full K (both builds) and of its child: from K and the build alone."""
    fs = K + K + (K if flavour == "cgo" else 0)  # File.Sync x K, the child's File.Sync x K, C.fsync x K (cgo)
    return {"flush_by_syscall": {"fsync": fs, "fdatasync": K, "syncfs": K, "sync": 1, "msync_sync": K},
            "sync_file_range": 2 * K, "msync_nosync": K, "copy_file_range_calls": K, "ficlone": 2 * K,
            "rwf_sync_writes": K, "osync_opens": 2}


def strace_got(sj, keys):
    return {k: sj.get(k) for k in keys}


def setup():
    global SETFL_TRUTH
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    fstype = sh(["findmnt", "-n", "-o", "FSTYPE", "-T", W]).stdout.strip()
    if fstype != A.fs:
        print("REFUSED: --fs %s but %s is on %r" % (A.fs, W, fstype))
        sys.exit(2)
    tools = {}
    for f in ("trace.sh", "stracecount.py"):
        p = os.path.join(T, f)
        if not os.path.exists(p):
            print("REFUSED: strace tool %s missing" % p)
            sys.exit(2)
        tools[f] = hashlib.sha256(open(p, "rb").read()).hexdigest()
    ENV.update({"stamp_utc": stamp, "uname": " ".join(os.uname()), "fs": A.fs, "fstype": fstype, "work": W,
                "reflink_expected": REFLINK, "strace": sh(["strace", "-V"]).stdout.splitlines()[:1],
                "strace_tools_sha256": tools, "build": B, "uptime_start": sh(["uptime"]).stdout.strip()})
    print(json.dumps(ENV), flush=True)
    t = sh([B + "/probe_c", "setfl-truth", "1", W])
    SETFL_TRUTH = int(t.stdout.strip()) if t.returncode == 0 and t.stdout.strip() in ("0", "1") else None
    arm("setfl-truth", SETFL_TRUTH is not None, {"setfl_truth": SETFL_TRUTH, "rc": t.returncode, "err": t.stderr})
    if SETFL_TRUTH is None:
        SETFL_TRUTH = 0


def sec_full():
    full7 = None
    for K in (7, 23):
        ok, d, _, raw = run_full(K)
        arm("C-full K=%d" % K, ok, d)
        if K == 7:
            full7 = raw
    a = by_pid(full7["rep"]).get(full7["p"]["root"]) if full7 else None
    e8 = exp_full(8)["root"][0]
    arm("checker-fires: K=7 counts vs K=8 expectation", a is not None and a["counts"] != e8,
        {"diff_vs_k8": a and diff(a["counts"], e8)})


def sec_mutants():
    for m, pred in MUTANTS.items():
        ok, d, diffs, _ = run_full(7, shim=B + "/syncshim_mut_%s.so" % m, tag="m")
        want = pred(7)
        arm("mutant-%s caught with the exact deficit" % m, (not ok) and diffs == want,
            {"checker_ok": ok, "diffs": diffs, "predicted": want, "detail": d})


def sec_noise():
    run = new_run("n")
    r, _ = launch(run, [B + "/probe_c", "noise", "11", W])
    rc, rep = report(run)
    rm(run)
    arm("C-noise", r.returncode == 0 and rc == 0 and rep is not None and rep["attached"] == 1
        and rep["totals"] == counts() and len(rep["slots"]) == 1 and rep["slots"][0]["fails"] == counts(),
        {"rc": rc, "probe_rc": r.returncode, "totals": rep and rep["totals"]})


def sec_raw():
    for K in (7, 23):
        run = new_run("r")
        r, sj = launch(run, [B + "/probe_c", "raw", str(K), W], strace=True)
        rc, rep = report(run)
        bm = bymark(run)
        rm(run)
        p = pids_from(r.stdout)
        root = by_pid(rep).get(p.get("root")) if rep else None
        shim_exp = counts(fsync=K, odsync_write=K)  # only the libc control calls (marks 5 and 6)
        st_exp = {"fsync": 3 * K, "fdatasync": K, "syncfs": 0, "sync": 0, "msync_sync": 0}
        st = sj.get("flush_by_syscall")
        marks_exp = collections.Counter({(p.get("root"), 0, 5, "fsync"): K, (p.get("root"), 0, 6, "odsync_write"): K})
        missed = None
        if root and st:
            missed = {"fsync (syscall(2) x K + inline syscall x K)": st["fsync"] - root["counts"]["fsync"],
                      "fdatasync (syscall(2) x K)": st["fdatasync"] - root["counts"]["fdatasync"],
                      "odsync_write (syscall(2) pwrite64 x K; made 2K, strace does not trace pwrite64)":
                          2 * K - root["counts"]["odsync_write"]}
        ok = (r.returncode == 0 and rc == 0 and root is not None and root["counts"] == shim_exp and st == st_exp
              and not sj["verdict"].startswith("REFUSED") and bm == marks_exp)
        arm("raw K=%d" % K, ok, {"MISSED_by_shim": missed, "shim_root": root and diff(root["counts"], counts()),
                                 "shim_expected": diff(shim_exp, counts()), "strace": st, "strace_expected": st_exp,
                                 "strace_verdict": sj.get("verdict"), "report_rc": rc, "probe_rc": r.returncode,
                                 "marks_ok": bm == marks_exp})


def sec_go():
    keys = ["flush_by_syscall", "sync_file_range", "msync_nosync", "copy_file_range_calls", "ficlone",
            "rwf_sync_writes", "osync_opens"]
    for flavour in ("nocgo", "cgo"):
        for K in (7, 23):
            run = new_run("g")
            r, sj = launch(run, [B + "/probe_go_" + flavour, "full", str(K), W], strace=True)
            p = pids_from(r.stdout)
            rc, rep = report(run)
            rc_go, rep_go = report(run, "--allow-go")
            rm(run)
            st_exp, st = strace_expect(K, flavour), strace_got(sj, keys)
            d = {"report_rc": rc, "report_rc_allow_go": rc_go, "probe_rc": r.returncode, "strace": st,
                 "strace_expected": st_exp, "strace_verdict": sj.get("verdict"), "probe": p}
            ok = r.returncode == 0 and st == st_exp and not sj["verdict"].startswith("REFUSED")
            if flavour == "nocgo":
                ok = ok and rc == 4 and rep is not None and rep["attached"] == 0 and rep["totals"] == counts()
                d["MISSED_by_shim"] = "every call strace saw: the static binary never loaded the shim (rc 4)"
            else:
                agg = by_pid(rep_go) if rep_go else {}
                ro, ch = agg.get(p.get("root")), agg.get(p.get("child"))
                ok = (ok and rc == 7 and rc_go == 0 and rep_go is not None and len(rep_go["slots"]) == 2
                      and ro is not None and ch is not None and ro["go"] == [True] and ch["go"] == [True]
                      and ro["counts"] == counts(fsync=K) and ch["counts"] == counts() and rep_go["execs"] == [])
                d["shim_root"] = ro and diff(ro["counts"], counts())
                d["shim_child"] = ch and diff(ch["counts"], counts())
                d["go_flags"] = [ro and ro["go"], ch and ch["go"]]
                d["MISSED_by_shim"] = {"fsync": st_exp["flush_by_syscall"]["fsync"] - K,
                                       "every other kind": "all of it (Go's own calls are raw syscalls)"}
            arm("Go-%s K=%d" % (flavour, K), ok, d)


def sec_sqlite():
    run = new_run("q")
    r, sj = launch(run, [B + "/probe_c", "sqlite", "1", W], strace=True)
    p = pids_from(r.stdout)
    rc, rep = report(run)
    rm(run)
    ro = by_pid(rep).get(p.get("root")) if rep else None
    st = sj.get("flush_by_syscall") or {}
    shim_view = ro and {"fsync": ro["counts"]["fsync"], "fdatasync": ro["counts"]["fdatasync"],
                        "syncfs": ro["counts"]["syncfs"], "sync": ro["counts"]["sync"],
                        "msync_sync": ro["counts"]["msync_SYNC"]}
    arm("sqlite: shim == strace", r.returncode == 0 and rc == 0 and ro is not None and shim_view == st
        and sum(st.values()) >= 1 and not sj["verdict"].startswith("REFUSED"),
        {"shim": shim_view, "strace": st, "report_rc": rc, "probe_rc": r.returncode, "stdout": r.stdout[-300:]})


def sec_sigkill():
    run = new_run("k")
    pr = subprocess.Popen([B + "/v1run", run, B + "/probe_c", "killme", "7", W], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True)
    line = pr.stdout.readline()
    m = re.search(r"ready pid=(\d+)", line)
    if m:
        os.kill(int(m.group(1)), signal.SIGKILL)
    else:
        pr.kill()
    rcp = pr.wait(timeout=60)
    rc, rep = report(run)
    rm(run)
    ro = by_pid(rep).get(int(m.group(1))) if (rep and m) else None
    arm("sigkill: counts survive", m is not None and rcp == -signal.SIGKILL and rc == 0 and ro is not None
        and ro["counts"] == counts(fsync=7), {"line": line, "rc": rc, "probe_rc": rcp, "root": ro and ro["counts"]})


def sec_refusals():
    env = dict(os.environ, LD_PRELOAD=B + "/syncshim.so")
    env.pop("SYNCSHIM_RUN", None)
    r = sh([B + "/probe_c", "spawned", "3", W], env=env)
    arm("refuse: SYNCSHIM_RUN unset -> 97", r.returncode == 97 and "REFUSING" in r.stderr,
        {"rc": r.returncode, "stderr": r.stderr[-300:]})
    env = dict(os.environ, LD_PRELOAD=B + "/syncshim.so", SYNCSHIM_RUN="nosuchrun%d" % os.getpid())
    r = sh([B + "/probe_c", "spawned", "3", W], env=env)
    arm("refuse: missing run -> 97", r.returncode == 97 and "REFUSING" in r.stderr,
        {"rc": r.returncode, "stderr": r.stderr[-300:]})
    run = new_run("r")
    rc, rep = report(run)
    rm(run)
    arm("refuse: nothing attached -> rc 3", rc == 3, {"rc": rc})
    run = new_run("r")
    r, _ = launch(run, [B + "/probe_c_static", "spawned", "3", W])
    rc, rep = report(run)
    rm(run)
    arm("refuse: static root -> rc 4", r.returncode == 0 and rc == 4 and rep is not None and rep["attached"] == 0,
        {"rc": rc, "probe_rc": r.returncode, "attached": rep and rep["attached"]})
    run = new_run("x")
    r, _ = launch(run, [B + "/probe_c", "execstatic", "3", W, B + "/probe_c_static"])
    rc, rep = report(run)
    rm(run)
    arm("refuse: static and env-stripped children -> rc 6",
        r.returncode == 0 and rc == 6 and rep is not None and rep["execs_claimed"] == 3
        and rep["execs_unattached"] == 3 and rep["attached"] == 1,
        {"rc": rc, "probe_rc": r.returncode, "execs": rep and rep["execs"]})
    run = new_run("e", "--events", "5")
    r, _ = launch(run, [B + "/probe_c", "spawned", "7", W])
    rc, rep = report(run)
    rc2, _ = report(run, "--allow-incomplete")
    rm(run)
    arm("refuse: event-log overflow -> rc 5, waivable, counts exact",
        r.returncode == 0 and rc == 5 and rep is not None and rep["events_dropped"] == 2
        and rep["totals"] == counts(fsync=7) and rc2 == 0,
        {"rc": rc, "rc_allow": rc2, "dropped": rep and rep["events_dropped"], "totals": rep and rep["totals"]})
    run = new_run("s", "--slots", "2")
    r, _ = launch(run, [B + "/probe_c", "fanout", "3", W])
    rc, rep = report(run)
    rc2, _ = report(run, "--allow-incomplete")
    rm(run)
    arm("refuse: slot overflow -> rc 5, not waivable, totals exact",
        r.returncode == 0 and rc == 5 and rc2 == 5 and rep is not None and rep["slot_overflow"] == 3
        and rep["totals"] == counts(fsync=9),
        {"rc": rc, "rc_allow": rc2, "overflow": rep and rep["slot_overflow"], "totals": rep and rep["totals"]})
    run = new_run("t", "--execs", "2")
    r, _ = launch(run, [B + "/probe_c", "fanout", "3", W])
    rc, rep = report(run)
    rc2, _ = report(run, "--allow-incomplete")
    rm(run)
    arm("refuse: exec-table overflow -> rc 5",
        r.returncode == 0 and rc == 5 and rc2 == 5 and rep is not None and rep["execs_dropped"] == 1
        and rep["attached"] == 4 and rep["totals"] == counts(fsync=9),
        {"rc": rc, "rc_allow": rc2, "execs_dropped": rep and rep["execs_dropped"], "totals": rep and rep["totals"]})
    run = new_run("v")
    r1 = sh([B + "/v1run", run, "/bin/true"])
    r2 = sh([B + "/v1run", run, "/bin/true"])
    rm(run)
    arm("refuse: second v1run on one run -> 2", r1.returncode == 0 and r2.returncode == 2,
        {"rc1": r1.returncode, "rc2": r2.returncode, "stderr2": r2.stderr[-200:]})


def finish():
    names = [r["arm"] for r in results]
    missing = [n for n in REGISTERED if n not in names]
    unregistered = [n for n in names if n not in REGISTERED]
    npass = sum(r["pass"] for r in results)
    all_pass = npass == len(results) and not missing and not unregistered and len(results) == len(REGISTERED)
    verdict = {"pass": npass, "total": len(results), "registered": len(REGISTERED), "missing_arms": missing,
               "unregistered_arms": unregistered, "all_pass": all_pass, "setfl_truth": SETFL_TRUTH, "env": ENV,
               "uptime_end": sh(["uptime"]).stdout.strip(), "arms": results}
    save("verdict.json", json.dumps(verdict, indent=1))
    print("V1 LINUX FIRE-CHECK %d/%d of %d registered %s%s" % (
        npass, len(results), len(REGISTERED), "PASS" if all_pass else "FAIL",
        (" missing=%s" % missing) if missing else ""))
    sys.exit(0 if all_pass else 1)


ENV = {}
setup()
for section in (sec_full, sec_mutants, sec_noise, sec_raw, sec_go, sec_sqlite, sec_sigkill, sec_refusals):
    try:
        section()
    except Exception:  # a section that crashed has not passed; its arms show as missing, the rest still run
        import traceback
        results.append({"arm": "CRASH in " + section.__name__, "pass": False, "detail": traceback.format_exc()})
        print("CRASH in %s:\n%s" % (section.__name__, traceback.format_exc()), flush=True)
finish()
