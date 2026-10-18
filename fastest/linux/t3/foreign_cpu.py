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
import sys
import time

HZ = os.sysconf("SC_CLK_TCK")


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
    per, interval = {}, interval_default
    for line in open(path):
        p = line.rstrip("\n").split("\t")
        if p[0] == "#start":
            for x in p[1:]:
                if x.startswith("interval="):
                    interval = float(x.split("=")[1])
        elif not p[0].startswith("#") and len(p) == 5 and p[4] == "foreign" and int(p[1]) not in exclude:
            per.setdefault((int(p[1]), p[2]), []).append((float(p[0]), float(p[3])))
    out = []
    for (pid, comm), rows in sorted(per.items()):
        run, last = 0.0, None
        for t, pct in rows:
            if pct > 50.0 and last is not None and t - last <= 1.5 * interval:
                run += t - last
            elif pct > 50.0:
                run = interval
            else:
                run = 0.0
            last = t if pct > 50.0 else None
            if run >= 5.0:
                out.append(f"{comm} pid {pid}")
                break
    return out


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
    for label, burners, want in live:
        if busy and want == "VALID":
            print(f"N/A-on-busy-host: self-test {label}: the quiet baseline was VOID ({busy}); a busy host cannot "
                  "show VALID", flush=True)
            continue
        out, rec, mine = window(label.split(": ")[1].replace(" ", "_")[:24], burners)
        if not burners and out != "VALID":
            # busy only on evidence from the window's OWN record (linux-rev-uw1 MED 3): a foreign, non-kernel process
            # outside this self-test's own tree above 50% for 5 s or more in its samples (busy_evidence). Without it a
            # VOID baseline is a FAIL, so a sampler that voids everything cannot read as a busy host
            others = busy_evidence(rec, exclude=mine)
            if others:
                busy = f"the no-burner window gave {out}; the window's own samples show: {', '.join(others[:4])}"
                print(f"N/A-on-busy-host: self-test {label}: {busy}", flush=True)
                continue
        ok = out == want
        bad += not ok
        print(f"self-test {'PASS' if ok else 'FAIL'}: {label}: want {want}, got {out}")

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
    def evid(label, rows, exclude=(), gap_at=None):
        f = os.path.join(d, f"evid-{label}.tsv")
        with open(f, "w") as o:
            o.write("#start\t0\tcell=ST\tinterval=1.0\tload=()\n")
            t_ = 0.0
            for i in range(20):
                t_ += 4.0 if gap_at == i else 1.0
                for pid, comm, pct, kind, start, n in rows:
                    if start <= i < start + n:
                        o.write(f"{t_:.3f}\t{pid}\t{comm}\t{pct}\t{kind}\n")
                o.write(f"#tick\t{t_:.3f}\t0\t0.0\t50.0\n")
            o.write("#stop\t20.000\tstop-file\tload=()\n")
        return busy_evidence(f, exclude)
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
    for label, ok in synthetic_evid:
        bad += not ok
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
        print(f"self-test {'PASS' if ok else 'FAIL'}: {label}: want {want}, got {got}")
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
