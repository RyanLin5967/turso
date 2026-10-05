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
    """pid -> (comm, cpu_ticks, class)."""
    out = {}
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
        """burners: list of (sut?, start_s, dur_s); returns the verdict."""
        out = os.path.join(d, f"{label}.tsv")
        stop = os.path.join(d, f"{label}.stop")
        s = subprocess.Popen([sys.executable, "-B", __file__, "sample", out, "--cell", "ST", "--stop-file", stop])
        procs = []
        t0 = time.monotonic()
        pending = sorted(burners, key=lambda b: b[1])
        while time.monotonic() - t0 < secs:
            el = time.monotonic() - t0
            while pending and pending[0][1] <= el:
                sut, _, dur = pending.pop(0)
                env = dict(os.environ)
                if sut:
                    env["FASTEST_CELL"] = "ST"
                code = f"import time\nt=time.monotonic()\nwhile time.monotonic()-t<{dur}: pass"
                procs.append(subprocess.Popen([sys.executable, "-c", code], env=env))
            time.sleep(0.05)
        for p in procs:
            p.wait()
        open(stop, "w").close()
        s.wait()
        return decide(out)["verdict"]

    ncpu = os.cpu_count() or 1
    cases = [
        ("quiet window", [], "VALID"),
        ("SUT burners only (2 cores, whole window)", [(True, 1, 17), (True, 1, 17)], "VALID"),
        ("one foreign burner for 8 s", [(False, 3, 8)], "VOID"),
        ("one foreign burner for 3 s (below 5 s, one core)", [(False, 3, 3)], "VALID"),
    ]
    if ncpu >= 3:
        cases.append(("three foreign burners for 3 s each (> 100% in > 5% of ticks)",
                      [(False, 5, 3), (False, 5, 3), (False, 5, 3)], "VOID"))
    bad = 0
    for label, burners, want in cases:
        got = window(label.replace(" ", "_")[:24], burners)
        ok = got == want
        bad += not ok
        print(f"self-test {'PASS' if ok else 'FAIL'}: {label}: want {want}, got {got}")
    return bad == 0


def main(a):
    if len(a) >= 2 and a[1] == "self-test":
        return 0 if self_test() else 1
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
