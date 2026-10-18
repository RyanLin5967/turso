#!/usr/bin/env python3
"""Foreign-CPU sampler and void decision for one timed batch (PREREG v1 amendment 7), Linux.

  foreign_cpu.py sample OUT.tsv --cell ID [--interval 1.0] [--stop-file F] [--max-s S]
  foreign_cpu.py decide OUT.tsv [--json VERDICT.json]
  foreign_cpu.py self-test

`sample` reads /proc every interval and writes one row per process per tick that used CPU:
  t (s since start), pid, comm, cpu_pct (of one core, over the tick), class
where class is
  sut     the process's environment carries FASTEST_CELL=<ID>: the system under test and the load
          generator, which t3run.sh starts with that variable, and everything they start (servers
          started with setsid keep their environment, so they stay inside);
  kernel  a kernel thread (no command line);
  foreign everything else, the sampler itself excluded.
The first time a foreign pid is recorded, an `#owner` row names it: pid, uid, login and the uwrun run it belongs to.
The login and run are written for this account's processes only (the run is the first component of its cwd under
uwrun's run roots, see run_roots); any other uid gets "-" for both, so another user's login is never recorded. Owners only
name busy-host evidence; they never change a verdict, and our own other runs are foreign like anyone's.
Each tick also writes a `#tick` row (t, foreign_sum, kernel_sum, sut_sum), so a tick in which nothing
foreign ran is on the record as a zero, not as a missing row.

`decide` makes the void decision from the recorded rows alone, before any latency or throughput of the
batch is read (amendment 7):
  VOID if foreign CPU exceeds 100% (one core) in more than 5% of ticks, or
  VOID if any single foreign process exceeds 50% for >= 5 consecutive seconds, or
  VOID if the record is unusable (fewer than 3 ticks, a gap over 3 intervals, or the sampler ended
  before the stop file appeared).
Exit 0 VALID, 3 VOID, 2 usage. Load average is recorded and never voids.

Blind spots, stated: kernel threads are reported apart and do not void (the system under test's own
writeback and journal work runs in kernel threads, so counting them as foreign would void every
I/O-heavy batch; the T3 registration must rule on this). A process whose environment cannot be read
(another user's) is foreign. A foreign process born and dead inside one tick is missed.
"""
import json
import os
import re
import sys
import time

HZ = os.sysconf("SC_CLK_TCK")


_LOGIN = {}


def _login(uid):
    """uid -> login, looked up once per uid (linux-rev-uw4 E4: NSS, sss on the UW hosts, took up to 7 ms a call inside
    the sampler's tick loop)"""
    if uid not in _LOGIN:
        import pwd
        try:
            _LOGIN[uid] = pwd.getpwuid(uid).pw_name
        except KeyError:
            _LOGIN[uid] = str(uid)
    return _LOGIN[uid]


def run_roots():
    """uwrun's own run roots for this account: /var/tmp/<login>-runs, /dev/shm/<login>-runs (host-local) and ~/runs
    (the NFS home's links, and a --nfs run's real dir), each resolved"""
    me = _login(os.getuid())
    return [os.path.realpath(r) for r in (f"/var/tmp/{me}-runs", f"/dev/shm/{me}-runs", os.path.expanduser("~/runs"))]


def owner_of(pid, roots=None):
    """(uid, login, run) of PID, or None when it is gone.
    - LOGIN is this account's login for its own processes and "-" for any other uid: another user's login is never
      written (linux-rev-uw4 E6: committed raws carried one); their uid identifies them.
    - RUN is the uwrun run one of OUR processes belongs to: the first path component under one of ROOTS (default
      run_roots()) of its cwd, so a runs dir nested inside a run is still that run's, and a directory that merely ends
      in "-runs" elsewhere is no run (linux-rev-uw4 E2); "-" otherwise. Another user's cwd cannot be read."""
    try:
        uid = os.stat(f"/proc/{pid}").st_uid
    except OSError:
        return None
    if uid != os.getuid():
        return uid, "-", "-"
    run = "-"
    try:
        cwd = os.readlink(f"/proc/{pid}/cwd")
    except OSError:
        cwd = ""
    for r in (run_roots() if roots is None else [os.path.realpath(x) for x in roots]):
        if cwd.startswith(r + "/"):
            run = cwd[len(r) + 1:].split("/", 1)[0] or "-"
            break
    return uid, _login(uid), run


def read_procs(cell):
    """pid -> (comm, cpu_ticks, class). A process is sut when its environ carries FASTEST_CELL=<cell> OR any ancestor
    (by ppid, within this snapshot) is sut: a child of the system under test is the system under test, even when its
    environ no longer says so (PostgreSQL's ps display clobbers the environ area of every process it forks: the first
    --dry-block run VOIDed every pg18 attempt on its own backends, 2026-10-15). A process with no sut ancestor is
    judged by its own environ, as before."""
    out = {}
    ppid = {}
    me = os.getpid()
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        pid = int(d)
        if pid == me:
            continue
        try:
            with open(f"/proc/{d}/stat") as f:
                st = f.read()
        except OSError:
            continue
        r = st.rfind(")")
        comm = st[st.find("(") + 1:r]
        fields = st[r + 2:].split()
        ticks = int(fields[11]) + int(fields[12])  # utime + stime
        ppid[pid] = int(fields[1])
        try:
            with open(f"/proc/{d}/cmdline", "rb") as f:
                cmd = f.read()
        except OSError:
            cmd = b""
        if not cmd:
            cls = "kernel"
        else:
            try:
                with open(f"/proc/{d}/environ", "rb") as f:
                    env = f.read().split(b"\0")
                cls = "sut" if f"FASTEST_CELL={cell}".encode() in env else "foreign"
            except OSError:
                cls = "foreign"
        out[pid] = (comm, ticks, cls)
    # ancestry: a non-kernel process whose ppid chain reaches a sut process is sut (cycles and unknown pids end the walk)
    for pid, (comm, ticks, cls) in list(out.items()):
        if cls != "foreign":
            continue
        seen, q = set(), ppid.get(pid)
        while q and q not in seen and q in out:
            seen.add(q)
            if out[q][2] == "sut":
                out[pid] = (comm, ticks, "sut")
                break
            q = ppid.get(q)
    return out


def sample(path, cell, interval, stop_file, max_s):
    t0 = time.monotonic()
    prev = read_procs(cell)
    prev_t = t0
    owned = set()
    with open(path, "w") as f:
        f.write(f"#start\t{time.time():.3f}\tcell={cell}\tinterval={interval}\tload={os.getloadavg()}\n")
        while True:
            time.sleep(interval)
            now = time.monotonic()
            cur = read_procs(cell)
            dt = now - prev_t
            sums = {"foreign": 0.0, "kernel": 0.0, "sut": 0.0}
            for pid, (comm, ticks, cls) in cur.items():
                p = prev.get(pid)
                used = ticks - (p[1] if p and p[0] == comm else 0)
                if used <= 0:
                    continue
                pct = 100.0 * used / HZ / dt
                sums[cls] += pct
                if cls == "foreign" and pid not in owned:
                    owned.add(pid)
                    o = owner_of(pid)
                    if o:
                        f.write(f"#owner\t{pid}\t{o[0]}\t{o[1]}\t{o[2]}\n")
                f.write(f"{now - t0:.3f}\t{pid}\t{comm}\t{pct:.1f}\t{cls}\n")
            f.write(f"#tick\t{now - t0:.3f}\t{sums['foreign']:.1f}\t{sums['kernel']:.1f}\t{sums['sut']:.1f}\n")
            f.flush()
            prev, prev_t = cur, now
            if stop_file and os.path.exists(stop_file):
                f.write(f"#stop\t{now - t0:.3f}\tstop-file\tload={os.getloadavg()}\n")
                return 0
            if max_s and now - t0 >= max_s:
                f.write(f"#stop\t{now - t0:.3f}\tmax-s\tload={os.getloadavg()}\n")
                return 0


def busy_evidence(path, exclude=(), interval_default=1.0):
    """linux-rev-uw1 MED 3: the processes that make a window's record show a BUSY HOST, from the window's own samples:
    each FOREIGN process (the sampler's own classification, so kernel threads and the system under test never count)
    whose pid is not in EXCLUDE (the self-test's own tree) and that the record shows above 50% for 5 s or more (rule
    2's own run-length). A ps CPU% is a lifetime average and proves nothing about the window (the reviewer's idle
    burner read 57-94%). Returns ["comm pid N", ...]; an empty list is no evidence."""
    per, interval, owners = {}, interval_default, {}
    for line in open(path):
        p = line.rstrip("\n").split("\t")
        if p[0] == "#owner" and len(p) == 5:
            owners[int(p[1])] = (int(p[2]), p[3], p[4])
        elif p[0] == "#start":
            for x in p[1:]:
                if x.startswith("interval="):
                    interval = float(x.split("=")[1])
        elif not p[0].startswith("#") and len(p) == 5 and p[4] == "foreign" and int(p[1]) not in exclude:
            per.setdefault((int(p[1]), p[2]), []).append((float(p[0]), float(p[3])))
    # linux-rev-uw2 LOW 3: the evidence is the process decide() NAMED for the VOID (rule 2: a foreign process above 50%
    # for 5 s), and none when the record is also unusable (a gap, too few ticks, no stop): a broken record is not a
    # busy host. A rule-1 reason (the foreign total) names no process, so it alone is no evidence (measured on iccad7:
    # a busy host's VOID gives rule 1 and rule 2 together)
    v = decide(path, interval_default)
    if any(r.startswith("unusable record") for r in v["reasons"]):
        return []
    named = []
    for r in v["reasons"]:
        m = re.match(r"foreign process (\d+) \((.*)\) above 50% for >= 5 s$", r)
        if m:
            named.append((int(m.group(1)), m.group(2)))
    out = [f"{comm} pid {pid}" for pid, comm in named if pid not in exclude and (pid, comm) in per]
    # linux-rev-uw3 LOW 6: rule 1 (the foreign total) names no process, so a host busy only by many processes each
    # under 50% read FAIL. Rule 1 is re-derived on the record WITHOUT the excluded tree: when the remaining foreign
    # processes still exceed 100% in more than 5% of the ticks, they are the evidence (the largest four, by CPU)
    if not out and any(r.startswith("foreign CPU > 100%") for r in v["reasons"]):
        ticks = [float(p[1]) for p in (ln.rstrip("\n").split("\t") for ln in open(path)) if p[0] == "#tick"]
        at = {}
        for (pid, comm), rows in per.items():
            for t, pct in rows:
                at[t] = at.get(t, 0.0) + pct
        over = sum(1 for t in ticks if at.get(t, 0.0) > 100.0)
        if ticks and over / len(ticks) > 0.05:
            top = sorted(per.items(), key=lambda kv: -sum(p for _t, p in kv[1]))[:4]
            out = [f"{comm} pid {pid}" for (pid, comm), _r in top]
    # who: each named process's owner from the sampler's #owner rows (the lead's 2026-10-18 order), never guessed
    def who(e):
        pid = int(e.rsplit(" pid ", 1)[1])
        if pid not in owners:
            return f"{e} (owner unrecorded)"
        uid, user, run = owners[pid]
        if uid != os.getuid():
            return f"{e} (another user, uid {uid})"
        return f"{e} (user {user}, our run {run})" if run != "-" else f"{e} (user {user})"
    return [who(e) for e in out]


def burners_ran(v, burners, mine, rec=None):
    """linux-rev-uw3 MED 1: a live case's burners were SEEN to run in its window's decide() record V. Each SUT burner
    (True, or a SUT parent's "child") must show in the SUT's CPU: sut_max at least 80% per SUT burner. A foreign
    burner (False) must be what voided the window: a rule-2 reason naming one of MINE (this case's own burner pids). A
    case with no burner is trivially true. Without this the live cases passed with burners that never burned.
    linux-rev-uw4 MED 1: sut_max is the SUT TOTAL, so a parent burning alone satisfied a "child" case (mutants F1, no
    child forked, and F2, also without the ancestry rule, survived). A "child" burner is seen only when the record REC
    has a sut-class process that is NOT in MINE (the child is the parent's own fork, so never in MINE) at 80% or more
    for 5 samples or more (rule 2's run-length); without REC a child is never seen."""
    sut = [b for b in burners if b[0] is True or b[0] == "child"]
    foreign = [b for b in burners if b[0] is False]
    if sut and (v.get("sut_max") or 0.0) < 80.0 * len(sut):
        return False
    # linux-rev-uw4 LOW 5: and an upper bound: a burner is one core, so a SUT total above 120% per SUT burner (plus 20
    # for the self-test's own short-lived helpers) is a sampler that over-counts (mutant F6, 10x), not a burner seen
    if sut and (v.get("sut_max") or 0.0) > 120.0 * len(sut) + 20.0:
        return False
    children = [b for b in burners if b[0] == "child"]
    if children:
        if rec is None:
            return False
        hot = {}
        for line in open(rec):
            p = line.rstrip("\n").split("\t")
            if not p[0].startswith("#") and len(p) == 5 and p[4] == "sut" and int(p[1]) not in mine \
                    and 80.0 <= float(p[3]) <= 120.0:
                hot[int(p[1])] = hot.get(int(p[1]), 0) + 1
        if sum(1 for n in hot.values() if n >= 5) < len(children):
            return False
    if foreign:
        pids = {int(m.group(1)) for m in (re.match(r"foreign process (\d+) ", r) for r in v.get("reasons") or []) if m}
        if not pids & set(mine):
            return False
    return True


def decide(path, interval_default=1.0):
    ticks, per_proc, stopped, interval = [], {}, False, interval_default
    for line in open(path):
        p = line.rstrip("\n").split("\t")
        if p[0] == "#start":
            for x in p[1:]:
                if x.startswith("interval="):
                    interval = float(x.split("=")[1])
        elif p[0] == "#tick":
            ticks.append((float(p[1]), float(p[2]), float(p[3]), float(p[4])))
        elif p[0] == "#stop":
            stopped = True
        elif not p[0].startswith("#") and len(p) == 5 and p[4] == "foreign":
            per_proc.setdefault((int(p[1]), p[2]), []).append((float(p[0]), float(p[3])))
    reasons = []
    if len(ticks) < 3:
        reasons.append(f"unusable record: {len(ticks)} ticks")
    if not stopped:
        reasons.append("unusable record: the sampler never stopped (killed or still running)")
    gaps = [b[0] - a[0] for a, b in zip(ticks, ticks[1:])]
    if gaps and max(gaps) > 3 * interval:
        reasons.append(f"unusable record: a {max(gaps):.1f} s gap between ticks")
    over = sum(1 for t in ticks if t[1] > 100.0)
    if ticks and over / len(ticks) > 0.05:
        reasons.append(f"foreign CPU > 100% in {over} of {len(ticks)} ticks ({over / len(ticks):.1%} > 5%)")
    for (pid, comm), rows in per_proc.items():
        run = 0.0
        last = None
        for t, pct in rows:
            if pct > 50.0 and last is not None and t - last <= 1.5 * interval:
                run += t - last
            elif pct > 50.0:
                run = interval
            else:
                run = 0.0
            last = t if pct > 50.0 else None
            if run >= 5.0:
                reasons.append(f"foreign process {pid} ({comm}) above 50% for >= 5 s")
                break
    v = {"verdict": "VOID" if reasons else "VALID", "reasons": reasons, "ticks": len(ticks),
         "foreign_over_100_ticks": over,
         "foreign_max": max((t[1] for t in ticks), default=None),
         "kernel_max": max((t[2] for t in ticks), default=None),
         "sut_max": max((t[3] for t in ticks), default=None)}
    return v


def self_test():
    import subprocess
    import tempfile
    d = tempfile.mkdtemp()

    def window(label, burners, secs=20):
        """burners: list of (sut?, start_s, dur_s[, duty]); duty < 1 busy-loops that share of every 10 ms.
        Returns the verdict."""
        out = os.path.join(d, f"{label}.tsv")
        stop = os.path.join(d, f"{label}.stop")
        s = subprocess.Popen([sys.executable, "-B", __file__, "sample", out, "--cell", "ST", "--stop-file", stop])
        procs = []
        t0 = time.monotonic()
        pending = sorted(burners, key=lambda b: b[1])
        while time.monotonic() - t0 < secs:
            el = time.monotonic() - t0
            while pending and pending[0][1] <= el:
                b = pending.pop(0)
                sut, dur = b[0], b[2]
                duty = b[3] if len(b) > 3 else 1.0
                env = dict(os.environ)
                if sut:
                    env["FASTEST_CELL"] = "ST"
                code = ("import time\nt=time.monotonic()\n"
                        f"while time.monotonic()-t<{dur}:\n"
                        f"    u=time.monotonic()\n    while time.monotonic()-u<{0.01 * duty}: pass\n"
                        f"    time.sleep({0.01 * (1 - duty)})\n")
                if sut == "child":
                    # a SUT parent (the marker in its environ) whose burning child has none: what a PostgreSQL
                    # backend looks like (the server's ps display clobbers the environ area of every child it forks)
                    env["FASTEST_CELL"] = "ST"
                    code = ("import subprocess, sys\n"
                            f"sys.exit(subprocess.call([sys.executable, '-c', {code!r}], env={{'PATH': '/usr/bin:/bin'}}))\n")
                procs.append(subprocess.Popen([sys.executable, "-c", code], env=env))
            time.sleep(0.05)
        for p in procs:
            p.wait()
        open(stop, "w").close()
        s.wait()
        return decide(out)["verdict"], out, {os.getpid(), s.pid} | {p.pid for p in procs}

    # Live: the sampler sees real processes and classifies them (marker -> sut, none -> foreign).
    live = [
        ("live: quiet window", [], "VALID"),
        ("live: SUT burners only (2 cores, whole window)", [(True, 1, 17), (True, 1, 17)], "VALID"),
        ("live: one foreign full-core burner for 9 s", [(False, 3, 9)], "VOID"),
        # the first --dry-block run (2026-10-15): every pg18 attempt VOIDed on "foreign process (postgres) above 50%",
        # its own backends, whose environ the server clobbers. A child of the system under test is the system under
        # test (the marker inherited by ancestry), and a foreign process with no SUT ancestor stays foreign (above)
        ("live: a SUT parent's child without the marker, a full core for 9 s, is the SUT", [("child", 3, 9)], "VALID"),
    ]
    bad = 0
    # the host-property class (2026-10-18, a shared RHEL 8.10 host with other users' processes): a live case that wants
    # VALID needs a QUIET host, which the first case, a window with no burner, measures. When that baseline is VOID the
    # host itself is busy, so those cases are N/A-on-busy-host, printed and never counted as a pass; the VOID-wanting
    # case still runs, as a busy host cannot excuse a missed burner. On the T3 box (quiet by its own preflight) all run.
    busy = None
    n_pass = n_na = 0
    for label, burners, want in live:
        # linux-rev-uw2 LOW 4: on a busy host the VOID-wanting case is N/A too: its window would VOID with or without
        # the burner, so it cannot tell a working sampler from one that voids everything
        if busy:
            n_na += 1
            print(f"N/A-on-busy-host: self-test {label}: the quiet baseline was VOID ({busy}); a busy host cannot "
                  f"show {want} apart from its own load", flush=True)
            continue
        out, rec, mine = window(label.split(": ")[1].replace(" ", "_")[:24], burners)
        if not burners and out != "VALID":
            # busy only on evidence from the window's OWN record (linux-rev-uw1 MED 3): a foreign, non-kernel process
            # outside this self-test's own tree above 50% for 5 s or more in its samples (busy_evidence). Without it a
            # VOID baseline is a FAIL, so a sampler that voids everything cannot read as a busy host
            others = busy_evidence(rec, exclude=mine)
            if others:
                busy = f"the no-burner window gave {out}; the window's own samples show: {', '.join(others[:4])}"
                n_na += 1
                print(f"N/A-on-busy-host: self-test {label}: {busy}", flush=True)
                continue
        # linux-rev-uw3 MED 1: the verdict counts only when the case's own burners were seen to run in its record
        seen = burners_ran(decide(rec), burners, mine, rec=rec)
        ok = out == want and seen
        bad += not ok
        n_pass += ok
        print(f"self-test {'PASS' if ok else 'FAIL'}: {label}: want {want}, got {out}"
              + ("" if seen else " (its burners were NOT seen to run in the window record)"))

    # Synthetic: each rule alone, on records whose every number is chosen here (a live box's own
    # background cannot be held still: runs 37255216397 and 37255904818 voided a 3 s burner by the
    # one-core rule once the runner's background was added to it).
    def synth(label, ticks, procs=(), stopped=True, gap_at=None):
        f = os.path.join(d, f"synth-{label}.tsv")
        with open(f, "w") as o:
            o.write("#start\t0\tcell=ST\tinterval=1.0\tload=()\n")
            t = 0.0
            for i, foreign in enumerate(ticks):
                t += 4.0 if gap_at == i else 1.0
                for pid, start, n, pct in procs:
                    if start <= i < start + n:
                        o.write(f"{t:.3f}\t{pid}\tburn\t{pct}\tforeign\n")
                o.write(f"#tick\t{t:.3f}\t{foreign}\t0.0\t50.0\n")
            if stopped:
                o.write(f"#stop\t{t:.3f}\tstop-file\tload=()\n")
        return decide(f)["verdict"]

    # linux-rev-uw1 MED 3: busy-host evidence comes from the WINDOW's own samples (a ps CPU% is a lifetime average:
    # the reviewer's 30 s burner read 57-94% while idle), of a foreign, non-kernel process outside the test's tree
    def evid(label, rows, exclude=(), gap_at=None, foreign_ticks=0.0, owners=()):
        return busy_evidence(evid_file(label, rows, gap_at, foreign_ticks, owners), exclude)

    def evid_file(label, rows, gap_at=None, foreign_ticks=0.0, owners=()):
        f = os.path.join(d, f"evid-{label}.tsv")
        with open(f, "w") as o:
            o.write("#start\t0\tcell=ST\tinterval=1.0\tload=()\n")
            for pid, uid, user, run in owners:
                o.write(f"#owner\t{pid}\t{uid}\t{user}\t{run}\n")
            t_ = 0.0
            any_sut = any(r[3] == "sut" for r in rows)
            for i in range(20):
                t_ += 4.0 if gap_at == i else 1.0
                sut_sum = 0.0
                for pid, comm, pct, kind, start, n in rows:
                    if start <= i < start + n:
                        o.write(f"{t_:.3f}\t{pid}\t{comm}\t{pct}\t{kind}\n")
                        sut_sum += pct if kind == "sut" else 0.0
                # the tick's SUT sum is the rows' own when the fixture has SUT rows (uw4 MED 1), else the old constant
                o.write(f"#tick\t{t_:.3f}\t{foreign_ticks}\t0.0\t{sut_sum if any_sut else 50.0}\n")
            o.write("#stop\t20.000\tstop-file\tload=()\n")
        return f
    synthetic_evid = [
        ("MED 3: a foreign process at 60% for 6 window samples is busy-host evidence",
         [p_.split(" ")[0] for p_ in evid("busy", [(7, "java", 60.0, "foreign", 3, 6)])] == ["java"]),
        ("MED 3: a process the window saw at 0% (a ps lifetime average says 94%) is no evidence",
         evid("stale", [(8, "burn", 0.0, "foreign", 0, 20)]) == []),
        ("MED 3: a kernel thread at 90% is no evidence (the sampler never lets one void a window)",
         evid("kern", [(9, "kworker/u8:1", 90.0, "kernel", 0, 20)]) == []),
        ("MED 3: a process of the test's own tree is no evidence",
         evid("ours", [(10, "python3", 99.0, "foreign", 0, 20)], exclude={10}) == []),
        ("MED 3: 60% for only 4 samples (under rule 2's 5 s) is no evidence",
         evid("brief", [(11, "java", 60.0, "foreign", 3, 4)]) == []),
        # linux-rev-uw2 LOW 3: the evidence is the process decide() NAMED for the VOID, and only when every VOID reason
        # is a foreign process: a record that is also unusable (here a 4 s gap) is not a busy host, burner or not
        ("uw2 LOW 3: a VOID that is also an unusable record (a 4 s gap) is no busy-host evidence, burner or not",
         evid("gap", [(12, "java", 90.0, "foreign", 2, 15)], gap_at=8) == []),
    ]
    # linux-rev-uw3 MED 1: a live case's BURNERS must be seen to have run in its own window record: a VALID-wanting case
    # needs the SUT's CPU (sut_max) to show its SUT burners; the VOID-wanting case needs rule 2 to name one of ITS OWN
    # burner pids. The red reaches burners_ran through a fallback with today's behaviour (no check: True), so it fails
    # at these claims, not at setup
    ran = globals().get("burners_ran", lambda v, burners, mine: True)
    two_sut = [(True, 1, 17), (True, 1, 17)]

    # linux-rev-uw4 MED 1: a "child" burner is seen only when a SUT-class process that is NOT one of the case's own
    # (mine: the parent and the self-test) burns, in the window's own per-process rows: sut_max is the SUT TOTAL, so
    # the parent burning alone (F1: no child forked; F2: also no ancestry rule) satisfied it. The record is passed as
    # rec=; the red reaches it through a fallback to today's call
    def ran_rec(v, burners, mine, rec):
        try:
            return ran(v, burners, mine, rec=rec)
        except TypeError:
            return ran(v, burners, mine)

    def sut_rec(label, rows):
        return evid_file(label, [(pid, "python3", pct, "sut", start, n) for pid, pct, start, n in rows])
    child = [("child", 3, 9)]
    par_only = sut_rec("paronly", [(41, 99.0, 3, 9)])
    par_child = sut_rec("parchild", [(41, 0.2, 3, 9), (77, 99.0, 3, 9)])
    brief_child = sut_rec("briefchild", [(41, 0.2, 3, 9), (77, 99.0, 3, 2)])
    x10_child = sut_rec("x10child", [(41, 2.0, 3, 9), (77, 990.0, 3, 9)])
    synthetic_evid += [
        ("uw4 MED 1: a child case whose SUT CPU is all the parent's (pid 41, ours) is refused: no child burned",
         not ran_rec(decide(par_only), child, {40, 41}, par_only)),
        ("uw4 MED 1: a child case whose SUT CPU comes from a SUT process not ours (pid 77) shows the child ran",
         ran_rec(decide(par_child), child, {40, 41}, par_child)),
        ("uw4 MED 1: a child seen at 99% for only 2 s (under rule 2's 5 s) is not seen to run",
         not ran_rec(decide(brief_child), child, {40, 41}, brief_child)),
        ("uw4 LOW 5: a child read at 990% (10x one core) is refused",
         not ran_rec(decide(x10_child), child, {40, 41}, x10_child)),
    ]
    synthetic_evid += [
        ("uw3 MED 1: two SUT burners that never burned (sut_max 0) are refused", not ran({"sut_max": 0.0, "reasons": []}, two_sut, {1, 2})),
        ("uw3 MED 1: two SUT burners seen at 190% pass", ran({"sut_max": 190.0, "reasons": []}, two_sut, {1, 2})),
        # linux-rev-uw4 LOW 5: an upper bound too: a sampler that over-counts CPU 10x (mutant F6) is not "seen to run"
        ("uw4 LOW 5: two SUT burners read at 1900% (10x two cores) are refused",
         not ran({"sut_max": 1900.0, "reasons": []}, two_sut, {1, 2})),
        ("uw3 MED 1: a SUT child that never burned (sut_max 0) is refused",
         not ran({"sut_max": 0.0, "reasons": []}, [("child", 3, 9)], {1, 2})),
        ("uw3 MED 1: a VOID named on another process (pid 999, not ours) does not show OUR burner ran",
         not ran({"sut_max": 0.0, "reasons": ["foreign process 999 (java) above 50% for >= 5 s"]}, [(False, 3, 9)], {41, 42})),
        ("uw3 MED 1: a VOID naming our own burner pid 42 shows it ran",
         ran({"sut_max": 0.0, "reasons": ["foreign process 42 (python3) above 50% for >= 5 s"]}, [(False, 3, 9)], {41, 42})),
        # uw3 LOW 6: a host busy only by rule 1 (many processes each under 50%, together over 100%) is busy-host
        # evidence, naming them, as long as they are outside the test's own tree
        ("uw3 LOW 6: four foreign processes at 40% each (rule 1 only) are busy-host evidence",
         len(evid("r1", [(31, "a", 40.0, "foreign", 0, 20), (32, "b", 40.0, "foreign", 0, 20), (33, "c", 40.0, "foreign", 0, 20),
                         (34, "d", 40.0, "foreign", 0, 20)], foreign_ticks=160.0)) >= 3),
        ("uw3 LOW 6: the same rule-1 load from the test's own tree is no evidence",
         evid("r1own", [(31, "a", 40.0, "foreign", 0, 20), (32, "b", 40.0, "foreign", 0, 20), (33, "c", 40.0, "foreign", 0, 20),
                        (34, "d", 40.0, "foreign", 0, 20)], exclude={31, 32, 33, 34}, foreign_ticks=160.0) == []),
    ]
    # the lead's 2026-10-18 order (M1's quiet baseline VOIDed on eceubuntu2 and ecetesla3 by "node-MainThread"
    # processes nobody could name afterwards): busy-host evidence names each process's OWNER, user and, for a process
    # of this account, the uwrun run it belongs to (its cwd under <prefix>-runs/<NAME>/), from #owner rows the sampler
    # writes when it first records a foreign pid. The busy rule itself does NOT exclude our own runs: a window shared
    # with another of our runs is as loaded as one shared with a stranger, and T3 times only quiet windows. The red
    # reaches owner_of through a fallback with today's behaviour (nothing recorded: None)
    own_ = globals().get("owner_of", lambda pid: None)

    # linux-rev-uw4 E2: the run is read only under uwrun's own run roots, passed as roots= so the test can plant one;
    # the red reaches it through a fallback to today's call (FLAGGED TEST EDIT: E2 and E6 change this block's spec)
    def own(pid, roots=None):
        try:
            return own_(pid, roots=roots) if roots is not None else own_(pid)
        except TypeError:
            return own_(pid)
    import pwd
    import shutil
    me_uid, me_user = os.getuid(), pwd.getpwuid(os.getuid()).pw_name
    base = next(b for b in ("/dev/shm", "/tmp", d) if os.access(b, os.W_OK))
    pd = tempfile.mkdtemp(dir=base)
    root = os.path.join(pd, f"{me_user}-runs")

    def sleeper(cwd):
        os.makedirs(cwd, exist_ok=True)
        return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], cwd=cwd)
    procs_ = {
        "child": sleeper(os.path.join(root, "probe-run", "work")),
        "nested": sleeper(os.path.join(root, "outer-run", "work", "y-runs", "inner-run")),
        # E2: a directory merely ending in "-runs", outside every run root, is no run
        "notes": sleeper(os.path.join(pd, "notes-runs", "README")),
        # E3: a process whose cwd is / is under no run
        "slash": sleeper("/"),
    }
    try:
        got = {k: own(v.pid, roots=[root]) for k, v in procs_.items()}
        got_default_slash = own(procs_["slash"].pid)
        got_pid1 = own(1)
    finally:
        for c_ in procs_.values():
            c_.kill(); c_.wait()
        shutil.rmtree(pd, ignore_errors=True)
    # E4: the uid -> login lookup (NSS, sss on these hosts: up to 7 ms) is made once per uid, not once per new pid
    calls = [0]
    real_getpwuid = pwd.getpwuid

    def counting(uid):
        calls[0] += 1
        return real_getpwuid(uid)
    pwd.getpwuid = counting
    try:
        for _ in range(50):
            own(os.getpid())
    finally:
        pwd.getpwuid = real_getpwuid
    # sample() itself: a foreign child burning for 3 s is recorded, and the record carries an #owner row for it
    srec, sstop = os.path.join(d, "owner-sample.tsv"), os.path.join(d, "owner-sample.stop")
    burn = subprocess.Popen([sys.executable, "-c", "import time\nt=time.monotonic()\nwhile time.monotonic()-t<3: pass"])
    sp = subprocess.Popen([sys.executable, "-B", os.path.abspath(__file__), "sample", srec, "--cell", "OWNERPROBE",
                           "--interval", "0.5", "--stop-file", sstop, "--max-s", "10"])
    burn.wait()
    open(sstop, "w").close()
    sp.wait()
    srows = [ln.rstrip("\n").split("\t") for ln in open(srec)]
    burn_owner = [r for r in srows if r[0] == "#owner" and r[1] == str(burn.pid)]
    other = me_uid + 4242
    synthetic_evid += [
        # E6: another user's login is never printed (committed raws carried one): their uid only
        ("uw4 E6: busy-host evidence names another user's process by uid, never by login",
         evid("own1", [(7, "node-MainThread", 90.0, "foreign", 2, 8)], owners=[(7, other, "alice", "-")])
         == [f"node-MainThread pid 7 (another user, uid {other})"]),
        ("owner: busy-host evidence names a process of OUR account by its uwrun run",
         evid("own2", [(7, "node-MainThread", 90.0, "foreign", 2, 8)], owners=[(7, me_uid, me_user, "moonshot-x")])
         == [f"node-MainThread pid 7 (user {me_user}, our run moonshot-x)"]),
        ("owner: a process with no #owner row is named as unrecorded, never guessed",
         evid("own3", [(7, "java", 90.0, "foreign", 2, 8)]) == ["java pid 7 (owner unrecorded)"]),
        ("owner: owner_of names a child of ours by our user and by the run its cwd is under",
         got["child"] is not None and got["child"][1:] == (me_user, "probe-run")),
        ("owner: a runs dir nested inside a run is still the outer run's (uwrun's own dir is the outermost)",
         got["nested"] is not None and got["nested"][2] == "outer-run"),
        ("uw4 E2: a cwd under a directory ending in -runs but outside every run root is no run",
         got["notes"] is not None and got["notes"][2] == "-"),
        ("uw4 E3: our own process whose cwd is / has user us and run - (with the planted root and the default roots)",
         got["slash"] == (me_uid, me_user, "-") and got_default_slash == (me_uid, me_user, "-")),
        ("uw4 E4: the login of a uid is looked up once, not once per lookup (50 lookups)", calls[0] <= 1),
        ("uw4 E6: owner_of on pid 1 (another uid: root's init) records its uid and no login and no run",
         os.getuid() == 0 or (got_pid1 is not None and got_pid1[0] != me_uid and got_pid1[1:] == ("-", "-"))),
        ("owner: sample() writes an #owner row for a foreign pid it records, with our user",
         len(burn_owner) == 1 and burn_owner[0][3] == me_user),
        # the lead's E4 order: owners are resolved at STOP, never inside the timed window (the per-pid lookups cost the
        # sampler 2-6x CPU there, measured by linux-uw4): every #owner row follows the #stop row
        ("uw4 E4: every #owner row is written after the #stop row (no owner lookup inside the window)",
         bool(burn_owner) and min(i for i, r in enumerate(srows) if r[0] == "#owner")
         > max(i for i, r in enumerate(srows) if r[0] == "#stop")),
        ("uw4 E4: a process of ours that exited before the stop keeps its uid and login, its run unknown (?)",
         len(burn_owner) == 1 and burn_owner[0][2] == str(me_uid) and burn_owner[0][4] == "?"),
    ]
    for label, ok in synthetic_evid:
        bad += not ok
        n_pass += ok
        print(f"self-test {'PASS' if ok else 'FAIL'}: {label}")
    synthetic = [
        ("rule 1: foreign 150% in 2 of 20 ticks (10% > 5%)", synth("r1v", [10] * 18 + [150, 150]), "VOID"),
        ("rule 1: foreign 150% in 1 of 20 ticks (5%, not above)", synth("r1ok", [10] * 19 + [150]), "VALID"),
        ("rule 2: one process at 60% for 6 consecutive s", synth("r2v", [60] * 20, [(7, 3, 6, 60.0)]), "VOID"),
        ("rule 2: one process at 60% for 4 consecutive s", synth("r2ok", [60] * 20, [(7, 3, 4, 60.0)]), "VALID"),
        ("rule 2: at 60% twice for 3 s, a gap between", synth("r2gap", [60] * 20, [(7, 2, 3, 60.0), (7, 7, 3, 60.0)]), "VALID"),
        ("unusable: the sampler never stopped", synth("nostop", [10] * 20, stopped=False), "VOID"),
        ("unusable: 2 ticks", synth("short", [10, 10]), "VOID"),
        ("unusable: a 4 s gap", synth("gap", [10] * 20, gap_at=10), "VOID"),
    ]
    for label, got, want in synthetic:
        ok = got == want
        bad += not ok
        n_pass += ok
        print(f"self-test {'PASS' if ok else 'FAIL'}: {label}: want {want}, got {got}")
    print(f"TALLY ran={n_pass + bad} pass={n_pass} na={n_na} fail={bad}")  # gates/tally.py
    # an N/A is its own outcome, never a pass (linux-rev-uw1 HIGH 1): "fail", "na" (some case did not run) or "pass"
    return "fail" if bad else "na" if busy else "pass"


def main(a):
    if len(a) >= 2 and a[1] == "self-test":
        return {"pass": 0, "fail": 1, "na": 3}[self_test()]
    if len(a) >= 3 and a[1] == "sample":
        opt = dict(zip(a[3::2], a[4::2]))
        if "--cell" not in opt:
            print("sample needs --cell", file=sys.stderr)
            return 2
        return sample(a[2], opt["--cell"], float(opt.get("--interval", 1.0)), opt.get("--stop-file"),
                      float(opt.get("--max-s", 0)))
    if len(a) >= 3 and a[1] == "decide":
        v = decide(a[2])
        if "--json" in a:
            json.dump(v, open(a[a.index("--json") + 1], "w"), indent=1)
        print(json.dumps(v))
        return 0 if v["verdict"] == "VALID" else 3
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
