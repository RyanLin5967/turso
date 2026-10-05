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
  C-full K=7, K=23     exact per-process counts, fails and slots for the root, a forked child driven by the parent's
                       marks, one child per exec route (posix_spawn, posix_spawnp, fork+execve/execv/execvp/execvpe/
                       execl/execle/execlp/fexecve/execveat), a vfork+execve child, a /bin/sh -c 'exec' child, a vfork
                       child that closes the parent's O_DSYNC fd (the parent's later writes must still count), a child
                       writing on inherited O_DSYNC/O_SYNC fds, and a child whose execve fails; exact per-mark rows;
                       exact sync_file_range and msync flags per event; the exec table (17 records as issued, every one
                       but the failed exec attached); no slot Go, live, missing anything or unresolved
  perturb-* x6         the C-full checker, fed the passing K=7 data with ONE planted change (a count +1, a per-mark row
                       moved with totals unchanged, an exec record unattached, an sfr flag changed, a slot marked Go, a
                       missed call), must fail, each for its own reason
  mutant-<name> x6     a shim missing one interpose (pwrite64, __open_2, fdatasync: C-full must mismatch by exactly the
                       predicted deficit) or breaking one piece of logic (syscall(2) fcntl untracked, raw pwritev2 flags
                       read from the wrong argument: the raw arm, exact deficit; no slot at fork: forkidle not LIVE)
  C-noise              only uncounted calls (zero-length O_DSYNC writes, bad iovec pointers that must come back
                       EFAULT, creat, __close): every kind 0, ZERO (rc 8); --allow-zero says "WAIVED: zero", never "ok"
  raw K=7, K=23        every counted kind through syscall(2): reported as MISSED (missed[] exactly, rc 9), never
                       counted; an inline syscall instruction: invisible (strace sees exactly K more than counted +
                       missed); libc control calls, and libc writes on fds opened / dup'd / F_DUPFD'd through syscall(2)
                       (open and dup2 too on x86_64): counted exactly
  uring / liburing     io_uring_setup, io_uring_enter and io_setup through syscall(2), and io_uring_queue_init through a
                       dynamically linked liburing.so: async I/O, rc 9 (the ring's fsyncs themselves are invisible)
  threads              8 threads race their first counted calls in a child made by a raw-instruction clone (no slot at
                       birth): exactly one slot, exact counts; mutant no_recheck (waiters claim again) gives 8
  Go-nocgo K=7, K=23   CGO_ENABLED=0 (static): report refuses rc 4, nothing attached; strace sees every call exactly
  Go-cgo K=7, K=23     CGO_ENABLED=1: report refuses rc 7; with --allow-go the root counts exactly the K cgo fsyncs and
                       nothing of Go's raw calls, the os/exec child attaches (Go flag) and counts 0; strace exact
  sqlite               a real library caller (system libsqlite3, one commit): shim counts == strace counts, >= 1
  sigkill / start_ticks K fsync read LIVE (rc 10), the slot's start time overwritten (another process: dead; none:
                       unknown, so alive), then SIGKILL: the counts survive, rc 0
  killmid              SIGKILL while blocked inside a counted call: inflight 1, INFLIGHT rc 11, not waived by
                       --allow-incomplete
  live, forkidle x3,   a background process outlives the root (rc 10 until it exits); a child of fork, _Fork or a
  leaderexit           syscall(2) clone that has counted nothing is LIVE from birth; a zombie thread-group leader whose
                       other thread still runs is LIVE
  foreign, foreign /proc  slots claimed in another pid namespace (sudo unshare --pid): rc 12, live or not; a report
                       inside the namespace that reads the host's /proc (no --mount-proc): rc 12
  hidepid              /proc with hidepid=invisible: root without CAP_SYS_PTRACE still reads the hidden live process as
                       LIVE (kill(pid, 0)), the user's report refuses (rc 12)
  vforkrace            a counting thread races K vfork children that each fsync once: every count under its own pid;
                       mutant split_publish (pid and slot published apart) misattributes and is caught
  create               a run larger than /dev/shm is refused at create (posix_fallocate), never a SIGBUS later
  outside              the flushing process runs outside the v1run tree while the tree clones: ZERO (rc 8), never ok
  bigfd                K writes on fd 1048600: fd_untracked exactly K, VOID (rc 5) not waivable
  refuse-*             SYNCSHIM_RUN unset -> 97; missing run -> 97; nothing attached -> rc 3; static root -> rc 4;
                       static / env-stripped children -> rc 6; event-log overflow -> rc 5 (waivable, counts exact);
                       slot overflow -> rc 5 (not waivable, totals exact); exec-table overflow -> rc 5; a second
                       v1run on one run -> 2
"""
import argparse
import collections
import copy
import hashlib
import json
import os
import platform
import re
import signal
import subprocess
import sys
import time

KINDS = ["fsync", "fdatasync", "syncfs", "sync", "msync_SYNC", "osync_write", "odsync_write", "sfr_write_wait",
         "sfr_write", "sfr_wait", "msync_other", "FICLONE", "FICLONERANGE", "copy_file_range"]
FLUSH, WRITEBACK, CLONE = KINDS[0:7], KINDS[7:11], KINDS[11:14]
ROUTES = ["posix_spawn", "posix_spawnp", "execve", "execv", "execvp", "execvpe", "execl", "execle", "execlp",
          "fexecve", "execveat"]
MS_ASYNC, MS_INVALIDATE, MS_SYNC = 1, 2, 4  # <sys/mman.h> on x86_64 and aarch64
SFR_WB, SFR_W, SFR_WA = 1, 2, 4             # SYNC_FILE_RANGE_WAIT_BEFORE / WRITE / WAIT_AFTER
SYS_WRITE = {"x86_64": 1, "aarch64": 64}    # the syscall number /proc/<pid>/syscall shows inside write(2)
MUTANTS = {  # mutant -> (the arm it runs under, its predicted deficit as a function of K)
    "pwrite64": ("full", lambda K: {"root": {"odsync_write": -K}}),
    "open_2": ("full", lambda K: {"root": {"odsync_write": -K}}),
    "fdatasync": ("full", lambda K: {"root": {"fdatasync": -K}, "fork_child": {"fdatasync": -K}}),
    "syscall_no_fcntl": ("raw", lambda K: {"counted": {"odsync_write": -K}}),   # mark 10: syscall(2) F_DUPFD fd
    "pwritev2_a4": ("raw", lambda K: {"missed": {"odsync_write": -K}}),         # mark 14: raw pwritev2 RWF_DSYNC
    "no_fork_claim": ("forkidle", lambda K: {"rc_first": 0, "child_alive_at_first": True,
                                              "totals_final": {"fsync": 1 + K}}),  # alive, yet not seen live
    "no_recheck": ("threads", lambda K: {"child_slots": 8}),  # 8 racing threads, each claims again
    "split_publish": ("vforkrace", lambda K: {"parent_short": True, "child_over": True}),  # a torn {pid, slot}
}
PERTURB = ["root fsync +1", "a per-mark row moved, totals unchanged", "an exec record unattached",
           "an sfr flags value changed", "a slot marked Go", "one missed call"]
REGISTERED = (["setfl-truth", "C-full K=7", "C-full K=23"] + ["perturb: %s caught" % p for p in PERTURB] +
              ["mutant-%s caught with the exact deficit" % m for m in MUTANTS] + ["C-noise"] +
              ["raw K=%d" % k for k in (7, 23)] + ["uring: async I/O refused", "liburing: async I/O refused",
                                                   "liburing init_mem: async I/O refused",
                                                   "threads: one slot, exact counts"] +
              ["Go-%s K=%d" % (f, k) for f in ("nocgo", "cgo") for k in (7, 23)] +
              ["sqlite: shim == strace", "sigkill: live read, then counts survive",
               "start_ticks: a slot whose start time is not the process's reads dead",
               "killmid: SIGKILL inside a counted call -> INFLIGHT rc 11", "live: rc 10 until the last process exits",
               "forkidle fork: rc 10 while a child that counted nothing runs",
               "forkidle _Fork: rc 10 while a child that counted nothing runs",
               "forkidle clone: rc 10 while a child that counted nothing runs",
               "leaderexit: a zombie leader with a running thread is LIVE",
               "foreign pid namespace -> rc 12",
               "foreign /proc: a report reading another namespace's /proc -> rc 12",
               "hidepid: root without CAP_SYS_PTRACE reads the hidden process LIVE; the user's report refuses",
               "vforkrace: every count under the pid that made it",
               "create refuses what /dev/shm cannot hold",
               "outside: flushes outside the tree, clones inside -> ZERO rc 8", "bigfd: untracked fd -> VOID rc 5",
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
ENV = {}


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


REPORT_SEQ = [0]


def report(run, *flags):
    r = sh([B + "/v1ctl", "report", run, "--json"] + list(flags))
    REPORT_SEQ[0] += 1
    save("%s.report%s.%d.json" % (run, "".join(f.replace("--", "_") for f in flags), REPORT_SEQ[0]), r.stdout + r.stderr)
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
            if v.lstrip("-").isdigit():
                out[k] = int(v)
    return out


def by_pid(rep):
    """pid -> {n, counts, fails, missed, go, alive, unresolved, inflight} summed over that pid's slots."""
    agg = {}
    for s in rep["slots"]:
        a = agg.setdefault(s["pid"], {"n": 0, "counts": counts(), "fails": counts(), "missed": counts(), "go": [],
                                      "alive": [], "unresolved": 0, "inflight": 0, "async_io": 0})
        a["n"] += 1
        for k in KINDS:
            a["counts"][k] += s["counts"][k]
            a["fails"][k] += s["fails"][k]
            a["missed"][k] += s["missed"][k]
        a["go"].append(s["go"])
        a["alive"].append(s["alive"])
        a["unresolved"] |= s["unresolved_mask"]
        a["inflight"] += s["inflight"]
        a["async_io"] += s["async_io"]
    return agg


def diff(got, exp):
    return {k: got[k] - exp[k] for k in KINDS if got[k] != exp[k]}


def nz(c):
    return {k: v for k, v in c.items() if v}


# ---------------------------------------------------------------- C-full expectations (from K, SETFL_TRUTH, REFLINK)
def exp_full(K):
    t = SETFL_TRUTH
    root = counts(fsync=K, fdatasync=K, syncfs=K, sync=1, msync_SYNC=K, osync_write=2 * K + 4 * K,
                  odsync_write=10 * K + t * K + 3 * K + 18 * K + 3 * K + K, sfr_write_wait=2 * K, sfr_write=2 * K,
                  sfr_wait=4 * K, msync_other=2 * K, FICLONE=K, FICLONERANGE=K, copy_file_range=2 * K)
    roles = {"root": (root, counts() if REFLINK else counts(FICLONE=K, FICLONERANGE=K), 1),
             "fork_child": (counts(fsync=sum(i % 3 + 1 for i in range(1, K + 1)), fdatasync=K), counts(), 1),
             "vfork": (counts(fsync=K), counts(), 1),             # vfork runs no fork handler: the image's slot only
             "via_sh": (counts(fsync=K), counts(), 2),            # /bin/sh's slot, then the probe it exec'd: one pid
             "vforkclose": (counts(fsync=K), counts(), 1),
             "inherited": (counts(odsync_write=K, osync_write=K), counts(), 1),
             "execfail": (counts(), counts(), 1)}                  # the slot claimed at fork; its execve failed
    for r in ROUTES:  # a forked route child: the slot claimed at fork, then the exec'd image's slot (one pid)
        roles["route_" + r] = (counts(fsync=K), counts(), 1 if r.startswith("posix_spawn") else 2)
    return roles  # role -> (counts, fails, slots)


def exp_marks(K, p):
    t, root = SETFL_TRUTH, p["root"]
    rows = {(1, "fsync"): K, (2, "fdatasync"): K, (3, "sfr_write_wait"): 2 * K, (4, "sfr_write"): 2 * K,
            (5, "sfr_wait"): 4 * K, (6, "syncfs"): K, (7, "msync_SYNC"): K, (8, "msync_other"): 2 * K,
            (10, "odsync_write"): 10 * K, (12, "osync_write"): 2 * K, (14, "odsync_write"): 3 * K,
            (14, "osync_write"): 4 * K, (15, "odsync_write"): 18 * K, (16, "sync"): 1, (17, "FICLONE"): K,
            (17, "FICLONERANGE"): K, (17, "copy_file_range"): K, (18, "odsync_write"): 3 * K,
            (18, "copy_file_range"): K, (35, "odsync_write"): K}
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
    exp[(p["vforkclose"], 0, 34, "fsync")] = K
    exp[(p["inherited"], 0, 36, "odsync_write")] = K
    exp[(p["inherited"], 0, 36, "osync_write")] = K
    return exp


def exp_flags(K):
    """(mark, kind, aux) multiset for the root's events whose aux carries the call's flags."""
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
    e[(p["vforkclose"], p["vforkclose"], "execve", "exec")] += 1
    e[(p["inherited"], root, "posix_spawn", "spawned")] += 1
    e[(p["execfail"], p["execfail"], "execve", "failed")] += 1
    return e


ZERO_KEYS = ("events_dropped", "events_incomplete", "inflight", "execs_dropped", "execs_unwritten", "execs_unattached",
             "slot_overflow", "fd_untracked", "unresolved_mask", "go_slots", "async_io", "live", "claiming",
             "foreign_ns")


def gather_full(K, shim=None, tag="c"):
    run = new_run(tag)
    r, _ = launch(run, [B + "/probe_c", "full", str(K), W], shim=shim)
    data = {"probe_rc": r.returncode, "stderr": r.stderr[-800:], "p": pids_from(r.stdout)}
    if r.returncode == 0:
        data["rc"], data["rep"] = report(run)
        data["bm"] = bymark(run)
        data["ev"] = events(run)
    rm(run)
    return data


def check_full(K, data):
    """(ok, detail, role_diffs). role_diffs = {role: {kind: got - expected}} for count mismatches."""
    if data["probe_rc"] != 0:
        return False, {"probe_rc": data["probe_rc"], "stderr": data["stderr"]}, None
    p, rep, rc, bm, ev = data["p"], data["rep"], data["rc"], data["bm"], data["ev"]
    d = {"report_rc": rc, "verdict": rep and rep["verdict"]}
    if rep is None:
        return False, d, None
    ok = rc == 0 and rep["verdict"] == "ok" and rep["waived"] == []
    for key in ZERO_KEYS:
        if rep[key] != 0:
            ok = False
            d[key] = rep[key]
    if any(rep["missed"].values()):
        ok = False
        d["missed"] = nz(rep["missed"])
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
        bad = {"count_diff": dc, "fail_diff": df, "slots": [a["n"], nslots]} if (dc or df or a["n"] != nslots) else {}
        if any(a["missed"].values()):
            bad["missed"] = nz(a["missed"])
        if any(a["go"]):
            bad["go"] = a["go"]
        if any(a["alive"]) or a["inflight"] or a["unresolved"] or a["async_io"]:
            bad["state"] = {"alive": a["alive"], "inflight": a["inflight"], "unresolved": a["unresolved"],
                            "async_io": a["async_io"]}
        if bad:
            ok = False
            d[role] = bad
    extra = set(agg) - {p.get(role) for role in roles}
    n_exp = sum(n for _, _, n in roles.values())
    if extra or len(rep["slots"]) != n_exp:
        ok = False
        d["slots"] = {"n": len(rep["slots"]), "expected": n_exp, "unknown_pids": sorted(extra)}
    tot = counts()
    for s in rep["slots"]:
        for k in KINDS:
            tot[k] += s["counts"][k]
    cls = {"flush": sum(tot[k] for k in FLUSH), "writeback": sum(tot[k] for k in WRITEBACK),
           "clone": sum(tot[k] for k in CLONE)}
    if rep["totals"] != tot or rep["classes"] != cls:
        ok = False
        d["report_arithmetic"] = {"totals": rep["totals"], "slot_sum": tot, "classes": rep["classes"], "want": cls}
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
            or not all(x["attached"] for x in xs if x["state"] != "failed") or len(xs) != 17:
        ok = False
        d["execs"] = {"missing": sorted(map(list, (ex - got_x).keys())), "extra": sorted(map(list, (got_x - ex).keys())),
                      "sh_own": sh_own, "n": len(xs),
                      "unattached": [x for x in xs if not x["attached"] and x["state"] != "failed"]}
    return ok, d, diffs


def strace_expect(K, flavour):
    """The strace counter's view of probe_go full K (both builds) and of its child: from K and the build alone."""
    fs = K + K + (K if flavour == "cgo" else 0)  # File.Sync x K, the child's File.Sync x K, C.fsync x K (cgo)
    return {"flush_by_syscall": {"fsync": fs, "fdatasync": K, "syncfs": K, "sync": 1, "msync_sync": K},
            "sync_file_range": 2 * K, "msync_nosync": K, "copy_file_range_calls": K, "ficlone": 2 * K,
            "rwf_sync_writes": K, "osync_opens": 2}


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
    import resource
    ENV.update({"stamp_utc": stamp, "uname": " ".join(os.uname()), "fs": A.fs, "fstype": fstype, "work": W,
                "reflink_expected": REFLINK, "strace": sh(["strace", "-V"]).stdout.splitlines()[:1],
                "strace_tools_sha256": tools, "build": B, "uptime_start": sh(["uptime"]).stdout.strip(),
                "rlimit_nofile": resource.getrlimit(resource.RLIMIT_NOFILE)})
    print(json.dumps(ENV), flush=True)
    t = sh([B + "/probe_c", "setfl-truth", "1", W])
    SETFL_TRUTH = int(t.stdout.strip()) if t.returncode == 0 and t.stdout.strip() in ("0", "1") else None
    arm("setfl-truth", SETFL_TRUTH is not None, {"setfl_truth": SETFL_TRUTH, "rc": t.returncode, "err": t.stderr})
    if SETFL_TRUTH is None:
        SETFL_TRUTH = 0


def sec_full():
    full7 = None
    for K in (7, 23):
        data = gather_full(K)
        ok, d, _ = check_full(K, data)
        arm("C-full K=%d" % K, ok, d)
        if K == 7 and ok:
            full7 = data
    # The checker itself: one planted change at a time in a copy of the passing K=7 data, each must be caught for
    # its own reason (the detail key it must raise).
    for i, (name, reason) in enumerate(zip(PERTURB, ["root", "marks_missing", "execs", "flags_missing", "root",
                                                     "missed"])):
        if full7 is None:
            arm("perturb: %s caught" % name, False, {"why": "C-full K=7 did not pass, so there is nothing to perturb"})
            continue
        dd = copy.deepcopy(full7)
        p, rep = dd["p"], dd["rep"]
        root_slot = [s for s in rep["slots"] if s["pid"] == p["root"]][0]
        if i == 0:
            root_slot["counts"]["fsync"] += 1
            rep["totals"]["fsync"] += 1
            rep["classes"]["flush"] += 1
        elif i == 1:
            dd["bm"][(p["root"], 0, 1, "fsync")] -= 1
            dd["bm"][(p["root"], 0, 2, "fsync")] += 1
        elif i == 2:
            [x for x in rep["execs"] if x["state"] != "failed"][3]["attached"] = False
        elif i == 3:
            e = [e for e in dd["ev"] if e["kind"] == "sfr_write_wait" and e["aux"] == str(SFR_WB | SFR_W | SFR_WA)][0]
            e["aux"] = str(SFR_W | SFR_WA)
        elif i == 4:
            root_slot["go"] = True
        elif i == 5:
            root_slot["missed"]["fsync"] = 1
            rep["missed"]["fsync"] = 1
        ok, d, _ = check_full(7, dd)
        arm("perturb: %s caught" % name, (not ok) and reason in d, {"checker_ok": ok, "must_raise": reason,
                                                                     "raised": sorted(d)})


def sec_mutants():
    for m, (where, pred) in MUTANTS.items():
        shim, want = B + "/syncshim_mut_%s.so" % m, pred(7)
        if where == "full":
            ok, d, diffs = check_full(7, gather_full(7, shim=shim, tag="m"))
        elif where == "raw":
            ok, d, diffs = check_raw(7, gather_raw(7, shim=shim, strace=False), use_strace=False)
        else:
            ok, d, diffs = (check_forkidle(7, "fork", shim=shim) if where == "forkidle" else
                            check_threads(7, shim) if where == "threads" else check_vforkrace(23, shim))
        arm("mutant-%s caught with the exact deficit" % m, (not ok) and diffs == want,
            {"arm": where, "checker_ok": ok, "diffs": diffs, "predicted": want, "detail": d})


def sec_noise():
    run = new_run("n")
    r, _ = launch(run, [B + "/probe_c", "noise", "11", W])
    rc, rep = report(run)
    rc2, rep2 = report(run, "--allow-zero")
    rm(run)
    arm("C-noise", r.returncode == 0 and rc == 8 and rc2 == 0 and rep is not None and rep["attached"] == 1
        and rep["totals"] == counts() and rep["missed"] == counts() and len(rep["slots"]) == 1
        and rep["slots"][0]["fails"] == counts() and rep["verdict"].startswith("ZERO")
        and rep2["waived"] == ["zero"] and rep2["verdict"].startswith("WAIVED: zero"),
        {"rc": rc, "rc_allow_zero": rc2, "probe_rc": r.returncode, "probe_stderr": r.stderr[-300:],
         "totals": rep and nz(rep["totals"]), "verdict": rep and rep["verdict"], "waived": rep2 and rep2["waived"],
         "verdict_waived": rep2 and rep2["verdict"]})


LEGACY = platform.machine() == "x86_64"  # x86_64 has the legacy open and dup2 syscalls; aarch64 has neither


def raw_expect(K):
    leg = 2 * K if LEGACY else 0
    counted = counts(fsync=K, odsync_write=K + K + K + K + K + leg, osync_write=K)        # marks 5-13
    missed = counts(fsync=K, fdatasync=K, odsync_write=5 * K, sfr_write_wait=K, syncfs=K, msync_SYNC=K,
                    copy_file_range=K, FICLONE=K)                                         # marks 1, 2, 4, 14-21
    marks = {(5, "fsync"): K, (6, "odsync_write"): K, (7, "odsync_write"): K, (8, "osync_write"): K,
             (9, "odsync_write"): K, (10, "odsync_write"): K, (11, "odsync_write"): K}
    if LEGACY:
        marks[(12, "odsync_write")] = K
        marks[(13, "odsync_write")] = K
    strace = {"flush_by_syscall": {"fsync": 3 * K, "fdatasync": K, "syncfs": K, "sync": 0, "msync_sync": K},
              "sync_file_range": K, "copy_file_range_calls": K, "ficlone": K, "rwf_sync_writes": K,
              "osync_opens": 3 + (1 if LEGACY else 0)}
    return counted, missed, marks, strace


def gather_raw(K, shim=None, strace=True):
    run = new_run("r")
    r, sj = launch(run, [B + "/probe_c", "raw", str(K), W], shim=shim, strace=strace)
    rc, rep = report(run)
    rc2, rep2 = report(run, "--allow-uncounted")
    bm = bymark(run)
    rm(run)
    return {"r": r, "sj": sj, "rc": rc, "rep": rep, "rc2": rc2, "rep2": rep2, "bm": bm, "p": pids_from(r.stdout)}


def check_raw(K, g, use_strace=True):
    counted_e, missed_e, marks_e, strace_e = raw_expect(K)
    r, sj, rc, rep, p = g["r"], g["sj"], g["rc"], g["rep"], g["p"]
    root = by_pid(rep).get(p.get("root")) if rep else None
    d = {"report_rc": rc, "report_rc_allow_uncounted": g["rc2"], "probe_rc": r.returncode,
         "probe_stderr": r.stderr[-300:], "verdict": rep and rep["verdict"],
         "waived": g["rep2"] and g["rep2"]["waived"]}
    if root is None:
        return False, d, None
    diffs = {}
    dc, dm = diff(root["counts"], counted_e), diff(root["missed"], missed_e)
    if dc:
        diffs["counted"] = dc
    if dm:
        diffs["missed"] = dm
    d.update({"MISSED_reported_by_shim": nz(root["missed"]), "counted": nz(root["counts"]), "diffs": diffs})
    rt = p.get("root")
    marks_exp = collections.Counter({(rt, 0, m, k): n for (m, k), n in marks_e.items()})
    ok = (r.returncode == 0 and rc == 9 and g["rc2"] == 0 and g["rep2"]["waived"] == ["uncounted"]
          and not diffs and g["bm"] == marks_exp and p.get("legacy_entry_points") == (1 if LEGACY else 0))
    d["marks_ok"] = g["bm"] == marks_exp
    if use_strace:
        st = {k: sj.get(k) for k in strace_e}
        fb = st["flush_by_syscall"] or {}
        # what neither the counts nor missed[] hold and strace saw: the inline-instruction fsyncs, exactly K
        inv = {"fsync": fb.get("fsync", 0) - root["counts"]["fsync"] - root["missed"]["fsync"],
               "fdatasync": fb.get("fdatasync", 0) - root["counts"]["fdatasync"] - root["missed"]["fdatasync"],
               "syncfs": fb.get("syncfs", 0) - root["counts"]["syncfs"] - root["missed"]["syncfs"],
               "msync_sync": fb.get("msync_sync", 0) - root["counts"]["msync_SYNC"] - root["missed"]["msync_SYNC"]}
        d.update({"INVISIBLE_to_shim (strace only)": inv, "strace": st, "strace_expected": strace_e,
                  "strace_verdict": sj.get("verdict")})
        ok = (ok and st == strace_e and inv == {"fsync": K, "fdatasync": 0, "syncfs": 0, "msync_sync": 0}
              and not sj["verdict"].startswith("REFUSED"))
    return ok, d, diffs


def sec_raw():
    for K in (7, 23):
        ok, d, _ = check_raw(K, gather_raw(K))
        arm("raw K=%d" % K, ok, d)


def sec_uring():
    run = new_run("u")
    r, _ = launch(run, [B + "/probe_c", "uring", "7", W])
    rc, rep = report(run)
    rc2, rep2 = report(run, "--allow-uncounted")
    rm(run)
    p = pids_from(r.stdout)
    ro = by_pid(rep).get(p.get("root")) if rep else None
    arm("uring: async I/O refused", r.returncode == 0 and rc == 9 and rc2 == 0 and rep is not None
        and rep["async_io"] == 3 and ro is not None and ro["counts"] == counts(fsync=7)
        and rep2["waived"] == ["uncounted"],
        {"rc": rc, "rc_allow": rc2, "async_io": rep and rep["async_io"], "probe": r.stdout.strip(),
         "verdict": rep and rep["verdict"], "waived": rep2 and rep2["waived"]})


def sec_liburing():
    for how, name in (("init", "liburing: async I/O refused"), ("mem", "liburing init_mem: async I/O refused")):
        run = new_run("w")
        r, _ = launch(run, [B + "/probe_uring", "7", W, how])
        rc, rep = report(run)
        rc2, rep2 = report(run, "--allow-uncounted")
        rm(run)
        p = pids_from(r.stdout)
        ro = by_pid(rep).get(p.get("root")) if rep else None
        # the 7 ring fsyncs are invisible (liburing's raw syscalls); the ring's setup through liburing.so refuses
        arm(name, r.returncode == 0 and p.get("ring_fsyncs_done") == 7 and rc == 9 and rc2 == 0
            and rep is not None and rep["async_io"] == 1 and ro is not None and ro["counts"] == counts(fsync=1)
            and rep2["waived"] == ["uncounted"],
            {"how": how, "rc": rc, "rc_allow": rc2, "async_io": rep and rep["async_io"], "probe": r.stdout.strip(),
             "probe_rc": r.returncode, "probe_stderr": r.stderr[-300:], "root": ro and nz(ro["counts"]),
             "verdict": rep and rep["verdict"]})


def check_threads(K, shim=None):
    """A raw-instruction-clone child (no slot at birth) whose 8 threads race their first counted calls: exactly one
    slot for the child, exact counts. diffs (for a mutant): the child's slot count when it is not 1."""
    run = new_run("h")
    r, _ = launch(run, [B + "/probe_c", "threads", str(K), W], shim=shim)
    rc, rep = report(run)
    rm(run)
    p = pids_from(r.stdout)
    agg = by_pid(rep) if rep else {}
    ch, ro = agg.get(p.get("child")), agg.get(p.get("root"))
    n = p.get("nthreads", 0)
    ok = (r.returncode == 0 and rc == 0 and n == 8 and ch is not None and ch["n"] == 1
          and ch["counts"] == counts(fsync=n * K, odsync_write=n * K) and ro is not None and ro["n"] == 1
          and ro["counts"] == counts() and len(rep["slots"]) == 2)
    d = {"rc": rc, "probe_rc": r.returncode, "child": ch and {"slots": ch["n"], "counts": nz(ch["counts"])},
         "root": ro and {"slots": ro["n"], "counts": nz(ro["counts"])}, "n_slots": rep and len(rep["slots"]),
         "stderr": r.stderr[-300:]}
    diffs = {"child_slots": ch["n"]} if (ch is not None and ch["n"] != 1) else {}
    return ok, d, diffs


def sec_threads():
    ok, d, _ = check_threads(7)
    arm("threads: one slot, exact counts", ok, d)


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
            st_exp, st = strace_expect(K, flavour), {k: sj.get(k) for k in keys}
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
                      and ro["counts"] == counts(fsync=K) and ch["counts"] == counts() and rep_go["execs"] == []
                      and rep_go["missed"] == counts() and rep_go["waived"] == ["go"])
                d["shim_root"] = ro and nz(ro["counts"])
                d["shim_child"] = ch and nz(ch["counts"])
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


def ready_pid(pr):
    line = pr.stdout.readline()
    m = re.search(r"ready pid=(\d+)", line)
    return (int(m.group(1)) if m else None), line


def poke_start(run, idx, value):
    """Overwrite slot idx's start_ticks in the run's shared memory (the fire-check plays a reused pid)."""
    import mmap
    import struct
    with open("/dev/shm/v1." + run, "r+b") as f:
        mm = mmap.mmap(f.fileno(), 0)
        off = 4096 + idx * 1024 + 16  # header, then 1024-byte slots; start_ticks after pid, ppid, attach_ns
        old = struct.unpack_from("<Q", mm, off)[0]
        struct.pack_into("<Q", mm, off, value)
        mm.close()
    return old


def sec_sigkill():
    run = new_run("k")
    pr = subprocess.Popen([B + "/v1run", run, B + "/probe_c", "killme", "7", W], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True)
    pid, line = ready_pid(pr)
    rc_live, rep_live = report(run)
    lv = by_pid(rep_live).get(pid) if (rep_live and pid) else None
    # start_ticks decides liveness: the same pid with another start time is another process (dead to this slot);
    # a slot with no start time is unknown, and unknown is alive
    st = {}
    if pid and rep_live:
        idx = [s["idx"] for s in rep_live["slots"] if s["pid"] == pid][0]
        orig = poke_start(run, idx, 0)
        poke_start(run, idx, orig + 1)
        st["rc_other_start"], r1 = report(run)
        st["alive_other_start"] = r1 and [s["alive"] for s in r1["slots"] if s["pid"] == pid]
        poke_start(run, idx, 0)
        st["rc_no_start"], r2 = report(run)
        poke_start(run, idx, orig)
        st["rc_restored"], _ = report(run)
        st["orig_start_ticks"] = orig
    if pid:
        os.kill(pid, signal.SIGKILL)
    else:
        pr.kill()
    rcp = pr.wait(timeout=60)
    rc, rep = report(run)
    rm(run)
    ro = by_pid(rep).get(pid) if (rep and pid) else None
    arm("sigkill: live read, then counts survive", pid is not None and rc_live == 10 and lv is not None
        and lv["counts"] == counts(fsync=7) and lv["alive"] == [True] and rcp == -signal.SIGKILL and rc == 0
        and ro is not None and ro["counts"] == counts(fsync=7) and ro["alive"] == [False],
        {"line": line, "rc_live": rc_live, "live": lv and nz(lv["counts"]), "rc": rc, "probe_rc": rcp,
         "root": ro and nz(ro["counts"])})
    arm("start_ticks: a slot whose start time is not the process's reads dead",
        st.get("rc_other_start") == 0 and st.get("alive_other_start") == [False] and st.get("rc_no_start") == 10
        and st.get("rc_restored") == 10 and st.get("orig_start_ticks", 0) > 0, st)


def sec_killmid():
    run = new_run("z")
    pr = subprocess.Popen([B + "/v1run", run, B + "/probe_c", "killmid", "7", W], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True)
    pid, line = ready_pid(pr)
    want, seen, deadline = SYS_WRITE.get(platform.machine()), None, time.time() + 15
    while pid and time.time() < deadline:  # wait until it is inside write(2), not merely about to call it
        try:
            seen = open("/proc/%d/syscall" % pid).read().split()
        except OSError as e:
            seen = ["unreadable: %s" % e]
        if seen and seen[0] == str(want):
            break
        time.sleep(0.05)
    inside = bool(seen) and seen[0] == str(want)
    if pid:
        os.kill(pid, signal.SIGKILL)
    else:
        pr.kill()
    rcp = pr.wait(timeout=60)
    rc, rep = report(run)
    rc2, _ = report(run, "--allow-incomplete")
    rc3, rep3 = report(run, "--allow-inflight")
    rm(run)
    ro = by_pid(rep).get(pid) if (rep and pid) else None
    arm("killmid: SIGKILL inside a counted call -> INFLIGHT rc 11", pid is not None and inside
        and rcp == -signal.SIGKILL and rc == 11 and rc2 == 11 and rc3 == 0 and rep3["waived"] == ["inflight"]
        and ro is not None and ro["inflight"] == 1 and rep["inflight"] == 1 and ro["counts"] == counts(fsync=7),
        {"line": line, "proc_syscall": seen and seen[:2], "inside_write": inside, "rc": rc,
         "rc_allow_incomplete": rc2, "rc_allow_inflight": rc3, "probe_rc": rcp,
         "root": ro and {"inflight": ro["inflight"], "counts": nz(ro["counts"])}, "verdict": rep and rep["verdict"],
         "waived": rep3 and rep3["waived"]})


def poll_report(run, until, timeout_s=30):
    """Report until its rc leaves `until` (the LIVE code); returns (rc, rep, polls)."""
    t_end, n = time.time() + timeout_s, 0
    while True:
        rc, rep = report(run)
        n += 1
        if rc != until or time.time() > t_end:
            return rc, rep, n
        time.sleep(0.25)


def sec_live():
    run = new_run("l")
    for f in ("live.started",):
        try:
            os.unlink(os.path.join(W, f))
        except FileNotFoundError:
            pass
    script = ('( : > "$1/live.started"; sleep 2; exec "$2" spawned 7 "$1" ) </dev/null >/dev/null 2>&1 & '
              'while [ ! -e "$1/live.started" ]; do sleep 0.05; done; exit 0')
    r, _ = launch(run, ["/bin/sh", "-c", script, "sh", W, B + "/probe_c"])
    rc1, rep1 = report(run)
    rc2, rep2, polls = poll_report(run, 10)
    rm(run)
    arm("live: rc 10 until the last process exits", r.returncode == 0 and rc1 == 10 and rep1 is not None
        and rep1["live"] >= 1 and rc2 == 0 and rep2 is not None and rep2["live"] == 0
        and rep2["totals"] == counts(fsync=7),
        {"root_rc": r.returncode, "rc_first": rc1, "live_first": rep1 and rep1["live"], "rc_final": rc2,
         "polls": polls, "totals_final": rep2 and nz(rep2["totals"]), "verdict_final": rep2 and rep2["verdict"]})


def proc_alive(pid):
    """Is pid running (exists and not a zombie), read from /proc by the fire-check itself, not by v1ctl."""
    try:
        raw = open("/proc/%d/stat" % pid).read()
    except (OSError, TypeError):
        return False
    return raw[raw.rindex(")") + 2] not in "ZXx"


def check_forkidle(K, variant, shim=None):
    """The root makes 1 fsync and exits at once; its child (fork / _Fork / clone through syscall(2)) counts nothing
    for 2 s, then K fsync. The first report must be LIVE while the child is alive by /proc (its slot exists from
    birth, with a zero count); once the child is gone by /proc, the report must be ok with 1 + K. diffs, for a
    mutant: what the first report said, whether the child was alive then, and the final totals."""
    run = new_run("i")
    r, _ = launch(run, [B + "/probe_c", "forkidle", str(K), W, variant], shim=shim)
    p = pids_from(r.stdout)
    rc1, rep1 = report(run)
    alive1 = proc_alive(p.get("child"))
    deadline = time.time() + 20
    while proc_alive(p.get("child")) and time.time() < deadline:
        time.sleep(0.1)
    gone = not proc_alive(p.get("child"))
    rc2, rep2 = report(run)
    rm(run)
    ch1 = by_pid(rep1).get(p.get("child")) if rep1 else None
    ch2 = by_pid(rep2).get(p.get("child")) if rep2 else None
    d = {"variant": variant, "root_rc": r.returncode, "rc_first": rc1, "child_alive_at_first": alive1,
         "child_first": ch1 and {"slots": ch1["n"], "alive": ch1["alive"], "counts": nz(ch1["counts"])},
         "child_gone_before_final": gone, "rc_final": rc2, "totals_final": rep2 and nz(rep2["totals"]),
         "child_final": ch2 and {"slots": ch2["n"], "counts": nz(ch2["counts"])}}
    ok = (r.returncode == 0 and alive1 and rc1 == 10 and ch1 is not None and ch1["alive"] == [True]
          and ch1["counts"] == counts() and gone and rc2 == 0 and rep2["totals"] == counts(fsync=1 + K)
          and ch2 is not None and ch2["n"] == 1 and ch2["counts"] == counts(fsync=K))
    diffs = {"rc_first": rc1, "child_alive_at_first": alive1, "totals_final": rep2 and nz(rep2["totals"])}
    return ok, d, diffs


def sec_forkidle():
    for variant in ("fork", "_Fork", "clone"):
        ok, d, _ = check_forkidle(7, variant)
        arm("forkidle %s: rc 10 while a child that counted nothing runs" % variant, ok, d)


def sec_leaderexit():
    K = 7
    run = new_run("x")
    pr = subprocess.Popen([B + "/v1run", run, B + "/probe_c", "leaderexit", str(K), W], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True)
    line = pr.stdout.readline()
    m = re.search(r"ready pid=(\d+) fifo=(\S+)", line)
    pid, fifo = (int(m.group(1)), m.group(2)) if m else (None, None)
    state, deadline = None, time.time() + 15
    while pid and time.time() < deadline:  # the leader has called pthread_exit: a zombie with a live thread
        try:
            raw = open("/proc/%d/stat" % pid).read()
            state = raw[raw.rindex(")") + 2]
        except (OSError, ValueError):
            state = "?"
        if state == "Z":
            break
        time.sleep(0.05)
    rc1, rep1 = report(run)
    if fifo:
        with open(fifo, "w") as f:  # rendezvous with the worker's open, then let it go
            f.write("g")
    rcp = pr.wait(timeout=60)
    rc2, rep2 = report(run)
    rm(run)
    ro1 = by_pid(rep1).get(pid) if (rep1 and pid) else None
    arm("leaderexit: a zombie leader with a running thread is LIVE", pid is not None and state == "Z" and rc1 == 10
        and ro1 is not None and ro1["alive"] == [True] and rcp == 0 and rc2 == 0
        and rep2["totals"] == counts(fsync=1 + K, odsync_write=K),
        {"line": line, "leader_state": state, "rc_first": rc1, "root_first": ro1 and {"alive": ro1["alive"]},
         "probe_rc": rcp, "rc_final": rc2, "totals_final": rep2 and nz(rep2["totals"]),
         "stderr": pr.stderr.read()[-300:] if pr.stderr else ""})


def sec_foreign():
    run = new_run("f")
    flag = os.path.join(W, "ns.go")
    try:
        os.unlink(flag)
    except FileNotFoundError:
        pass
    pr = subprocess.Popen(["sudo", "-n", "unshare", "--pid", "--fork", "--mount-proc", B + "/v1run", run,
                           B + "/probe_c", "waitfile", "3", W, flag], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True)
    pid, line = ready_pid(pr)
    rc1, rep1 = report(run)
    open(flag, "w").close()
    rcp = pr.wait(timeout=60)
    rc2, rep2 = report(run)
    sh(["sudo", "-n", "rm", "-f", os.path.join(W, "waitfile")])  # root-owned
    rm(run)
    my_ns = os.stat("/proc/self/ns/pid").st_ino
    nss = rep1 and sorted({s["pidns"] for s in rep1["slots"]})
    arm("foreign pid namespace -> rc 12", line.startswith("ready") and rc1 == 12 and rcp == 0 and rc2 == 12
        and nss is not None and my_ns not in nss and all(nss) and rep2["totals"] == counts(fsync=3),
        {"line": line, "pid_inside": pid, "rc_live": rc1, "rc_after": rc2, "probe_rc": rcp, "slot_pidns": nss,
         "my_pidns": my_ns, "verdict": rep2 and rep2["verdict"], "stderr": pr.stderr.read()[-300:]})


def sec_foreign_proc():
    """Run and report inside `unshare --pid --fork` WITHOUT --mount-proc: the report's own pid namespace matches the
    slots', but the /proc it reads is the host's, where the slots' pids name other processes. Must be rc 12."""
    run = new_run("y")
    flag = os.path.join(W, "nsp.go")
    try:
        os.unlink(flag)
    except FileNotFoundError:
        pass
    script = ('"$1/v1run" "$2" "$1/probe_c" waitfile 3 "$3" "$4" >"$3/nsp.out" 2>&1 & '
              'until grep -q ready "$3/nsp.out"; do sleep 0.05; done; '
              '"$1/v1ctl" report "$2" --json >"$3/nsp.report.json"; echo "report_rc=$?"; : >"$4"; wait')
    r = sh(["sudo", "-n", "unshare", "--pid", "--fork", "sh", "-c", script, "sh", B, run, W, flag], timeout=120)
    save(run + ".nsproc.txt", "rc=%d\n%s\n%s" % (r.returncode, r.stdout, r.stderr))
    m = re.search(r"report_rc=(\d+)", r.stdout)
    rcin = int(m.group(1)) if m else None
    try:
        rep = json.loads(open(os.path.join(W, "nsp.report.json")).read())
    except (OSError, ValueError):
        rep = None
    sh(["sudo", "-n", "rm", "-f", os.path.join(W, "nsp.out"), os.path.join(W, "nsp.report.json"),
        os.path.join(W, "waitfile")])
    rm(run)
    arm("foreign /proc: a report reading another namespace's /proc -> rc 12", r.returncode == 0 and rcin == 12
        and rep is not None and rep["proc_untrusted"] is True and rep["foreign_ns"] == 0,
        {"rc_inside": rcin, "shell_rc": r.returncode, "proc_untrusted": rep and rep["proc_untrusted"],
         "foreign_ns": rep and rep["foreign_ns"], "verdict": rep and rep["verdict"], "stderr": r.stderr[-300:]})


def check_vforkrace(K, shim=None):
    """A thread fsyncs in a loop while K vfork children each fsync once. Expected, from the probe's own counts: the
    root pid holds exactly the thread's fsyncs, every child pid exactly 1. diffs (for a mutant): whether the root is
    short and whether a child holds more than its own."""
    run = new_run("v")
    r, _ = launch(run, [B + "/probe_c", "vforkrace", str(K), W], shim=shim)
    rc, rep = report(run)
    rm(run)
    p = pids_from(r.stdout)
    m = re.search(r"children=([\d,]+)", r.stdout)
    kids = [int(x) for x in m.group(1).split(",")] if m else []
    agg = by_pid(rep) if rep else {}
    ro = agg.get(p.get("root"))
    tc = p.get("thread_fsyncs", -1)
    per_child = {c: (agg[c]["counts"]["fsync"] if c in agg else None) for c in kids}
    parent = ro["counts"]["fsync"] if ro else None
    ok = (r.returncode == 0 and rc == 0 and len(kids) == K and tc > 0 and parent == tc
          and all(v == 1 for v in per_child.values()) and rep["totals"] == counts(fsync=tc + K))
    d = {"rc": rc, "probe_rc": r.returncode, "thread_fsyncs": tc, "root_fsync": parent,
         "root_slots": ro and ro["n"], "children_not_1": {c: v for c, v in per_child.items() if v != 1},
         "totals": rep and nz(rep["totals"]), "stderr": r.stderr[-300:]}
    diffs = {"parent_short": parent is not None and parent < tc,
             "child_over": any(v is not None and v > 1 for v in per_child.values())} if not ok else {}
    return ok, d, diffs


def sec_vforkrace():
    ok, d, _ = check_vforkrace(23)
    arm("vforkrace: every count under the pid that made it", ok, d)


def sec_hidepid():
    """/proc mounted with hidepid=invisible (in a private mount namespace). A report as root WITHOUT CAP_SYS_PTRACE
    cannot see the runner-owned probe in /proc, and must still read it LIVE (rc 10), never dead (rc 0). A report as
    the user refuses outright (rc 12): its /proc hides other users."""
    import pwd
    user = pwd.getpwuid(os.getuid()).pw_name
    run = new_run("p")
    flag = os.path.join(W, "hp.go")
    try:
        os.unlink(flag)
    except FileNotFoundError:
        pass
    pr = subprocess.Popen([B + "/v1run", run, B + "/probe_c", "waitfile", "3", W, flag], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True)
    pid, line = ready_pid(pr)
    mnt = 'mount -t proc -o hidepid=invisible proc /proc && exec "$@"'
    r_root = sh(["sudo", "-n", "unshare", "-m", "sh", "-c", mnt, "sh", "setpriv", "--bounding-set=-sys_ptrace",
                 B + "/v1ctl", "report", run, "--json"], timeout=60)
    r_user = sh(["sudo", "-n", "unshare", "-m", "sh", "-c", mnt, "sh", "setpriv", "--reuid=" + user, "--regid=" + user,
                 "--init-groups", B + "/v1ctl", "report", run, "--json"], timeout=60)
    open(flag, "w").close()
    rcp = pr.wait(timeout=60)
    rc_final, rep_final = report(run)
    rm(run)
    save(run + ".hidepid.txt", "root rc=%d\n%s\n%s\nuser rc=%d\n%s\n%s\n" % (
        r_root.returncode, r_root.stdout, r_root.stderr, r_user.returncode, r_user.stdout, r_user.stderr))
    jr = json.loads(r_root.stdout) if r_root.stdout.strip().startswith("{") else None
    ju = json.loads(r_user.stdout) if r_user.stdout.strip().startswith("{") else None
    arm("hidepid: root without CAP_SYS_PTRACE reads the hidden process LIVE; the user's report refuses",
        pid is not None and r_root.returncode == 10 and jr is not None and jr["live"] >= 1
        and r_user.returncode == 12 and ju is not None and ju["proc_untrusted"] is True and rcp == 0
        and rc_final == 0 and rep_final["totals"] == counts(fsync=3),
        {"line": line, "rc_root_no_ptrace": r_root.returncode, "live_root": jr and jr["live"],
         "rc_user": r_user.returncode, "proc_untrusted_user": ju and ju["proc_untrusted"], "probe_rc": rcp,
         "rc_final": rc_final, "stderr_root": r_root.stderr[-200:], "stderr_user": r_user.stderr[-200:]})


def sec_create():
    run = "fc%dbig" % (os.getpid() % 10000)
    sh([B + "/v1ctl", "rm", run])
    r = sh([B + "/v1ctl", "create", run, "--events", "4000000000"])  # 256 GB of events: more than any /dev/shm
    exists = os.path.exists("/dev/shm/v1." + run)
    if exists:
        rm(run)
    arm("create refuses what /dev/shm cannot hold", r.returncode == 2 and not exists
        and "cannot reserve" in r.stderr, {"rc": r.returncode, "stderr": r.stderr[-300:], "left_behind": exists})


def sec_outside():
    for f in ("out.go", "out.done"):
        try:
            os.unlink(os.path.join(W, f))
        except FileNotFoundError:
            pass
    env = dict(os.environ)
    env.pop("LD_PRELOAD", None)
    env.pop("SYNCSHIM_RUN", None)
    helper = subprocess.Popen(["/bin/sh", "-c", 'while [ ! -e "$1/out.go" ]; do sleep 0.05; done; '
                               '"$2" spawned 5 "$1" && : > "$1/out.done"', "sh", W, B + "/probe_c"], env=env)
    run = new_run("o")
    # inside the tree: 3 FICLONE + 3 copy_file_range and no flush; outside it: the 5 fsyncs
    r, _ = launch(run, ["/bin/sh", "-c", '"$2" cloneonly 3 "$1" && : > "$1/out.go"; '
                        'while [ ! -e "$1/out.done" ]; do sleep 0.05; done', "sh", W, B + "/probe_c"])
    hrc = helper.wait(timeout=60)
    rc, rep = report(run)
    rc2, rep2 = report(run, "--allow-zero")
    rm(run)
    arm("outside: flushes outside the tree, clones inside -> ZERO rc 8", r.returncode == 0 and hrc == 0 and rc == 8
        and rc2 == 0 and rep is not None and rep["classes"] == {"flush": 0, "writeback": 0, "clone": 6}
        and rep["totals"] == counts(FICLONE=3, copy_file_range=3) and rep2["waived"] == ["zero"],
        {"root_rc": r.returncode, "helper_rc": hrc, "rc": rc, "rc_allow_zero": rc2,
         "classes": rep and rep["classes"], "verdict": rep and rep["verdict"], "waived": rep2 and rep2["waived"]})


def sec_bigfd():
    K = 7
    run = new_run("b")
    r, _ = launch(run, [B + "/probe_c", "bigfd", str(K), W])
    rc, rep = report(run)
    rc2, _ = report(run, "--allow-incomplete")
    rm(run)
    arm("bigfd: untracked fd -> VOID rc 5", r.returncode == 0 and rc == 5 and rc2 == 5 and rep is not None
        and rep["fd_untracked"] == K and rep["totals"] == counts(),
        {"probe_rc": r.returncode, "probe_stderr": r.stderr[-300:], "rc": rc, "rc_allow": rc2,
         "fd_untracked": rep and rep["fd_untracked"], "verdict": rep and rep["verdict"]})


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
    # attached: the root, and the two forked children's fork-time slots (their images never attach)
    arm("refuse: static and env-stripped children -> rc 6",
        r.returncode == 0 and rc == 6 and rep is not None and rep["execs_claimed"] == 3
        and rep["execs_unattached"] == 3 and rep["attached"] == 3,
        {"rc": rc, "probe_rc": r.returncode, "attached": rep and rep["attached"], "execs": rep and rep["execs"]})
    run = new_run("e", "--events", "5")
    r, _ = launch(run, [B + "/probe_c", "spawned", "7", W])
    rc, rep = report(run)
    rc2, rep2 = report(run, "--allow-incomplete")
    rm(run)
    arm("refuse: event-log overflow -> rc 5, waivable, counts exact",
        r.returncode == 0 and rc == 5 and rep is not None and rep["events_dropped"] == 2
        and rep["totals"] == counts(fsync=7) and rc2 == 0 and rep2["waived"] == ["incomplete"],
        {"rc": rc, "rc_allow": rc2, "dropped": rep and rep["events_dropped"], "totals": rep and nz(rep["totals"])})
    run = new_run("s", "--slots", "2")
    r, _ = launch(run, [B + "/probe_c", "fanout", "3", W])
    rc, rep = report(run)
    rc2, _ = report(run, "--allow-incomplete")
    rm(run)
    arm("refuse: slot overflow -> rc 5, not waivable, totals exact",
        r.returncode == 0 and rc == 5 and rc2 == 5 and rep is not None and rep["slot_overflow"] == 3
        and rep["totals"] == counts(fsync=9),
        {"rc": rc, "rc_allow": rc2, "overflow": rep and rep["slot_overflow"], "totals": rep and nz(rep["totals"])})
    run = new_run("t", "--execs", "2")
    r, _ = launch(run, [B + "/probe_c", "fanout", "3", W])
    rc, rep = report(run)
    rc2, _ = report(run, "--allow-incomplete")
    rm(run)
    arm("refuse: exec-table overflow -> rc 5",
        r.returncode == 0 and rc == 5 and rc2 == 5 and rep is not None and rep["execs_dropped"] == 1
        and rep["attached"] == 4 and rep["totals"] == counts(fsync=9),
        {"rc": rc, "rc_allow": rc2, "execs_dropped": rep and rep["execs_dropped"], "totals": rep and nz(rep["totals"])})
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


setup()
for section in (sec_full, sec_mutants, sec_noise, sec_raw, sec_uring, sec_liburing, sec_threads, sec_go, sec_sqlite,
                sec_sigkill, sec_killmid, sec_live, sec_forkidle, sec_leaderexit, sec_foreign, sec_foreign_proc,
                sec_hidepid, sec_vforkrace, sec_create,
                sec_outside, sec_bigfd, sec_refusals):
    try:
        section()
    except Exception:  # a section that crashed has not passed; its arms show as missing, the rest still run
        import traceback
        results.append({"arm": "CRASH in " + section.__name__, "pass": False, "detail": traceback.format_exc()})
        print("CRASH in %s:\n%s" % (section.__name__, traceback.format_exc()), flush=True)
finish()
