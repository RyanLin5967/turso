#!/usr/bin/env python3
"""stamp.py -- Linux batch stamps around a V3 batch (Linux port of frontier/fastest/tools/common/stamp.py).

  stamp.py start OUT.json --dir D
  stamp.py end START.json OUT.json

RECORD ONLY. The Mac stamp's void rules (battery, ~/.claude/QUIET, fan-guard SIGSTOPs, end load) describe that box;
no void rule for a Linux batch is registered, so this stamp never voids: `end` exits 0 when every required record
was taken and 2 when one was not (a stamp that cannot see the box cannot vouch for it). Whoever registers the T3
rules decides what voids; the records here are what such a rule would read.

Each stamp records: UTC, a CLOCK_MONOTONIC_RAW anchor, load averages, PSI (cpu, io, memory), /proc/stat CPU jiffies,
/proc/diskstats for every block device, the mount and free bytes under D, and the block devices' write-cache and FUA
modes. `end` adds the deltas between the two stamps: CPU busy share of the window, and per device the reads, writes,
discards (diskstats field 15) and FLUSH requests completed (fields 19-20, kernel >= 5.5). On a device that reports
write-through the block layer strips REQ_PREFLUSH before a request exists, so a flush count that stays at zero there
is consistent with no flush reaching it (the kernel source says so; this stamp does not test it).
"""
import glob, json, os, subprocess, sys, time

REQUIRED = ["loadavg", "stat_cpu", "diskstats"]


def read(p):
    try:
        with open(p) as f:
            return f.read()
    except OSError:
        return None


def diskstats():
    t = read("/proc/diskstats")
    if t is None:
        return None
    d = {}
    for line in t.splitlines():
        f = line.split()
        if len(f) < 14:
            continue
        v = [int(x) for x in f[3:]]
        row = {"reads": v[0], "writes": v[4], "sectors_written": v[6], "io_ms": v[9]}
        if len(v) >= 15:
            row["discards"] = v[11]
        if len(v) >= 17:
            row["flushes"] = v[15]
            row["flush_ms"] = v[16]
        d[f[2]] = row
    return d


def stat_cpu():
    t = read("/proc/stat")
    if t is None:
        return None
    f = t.splitlines()[0].split()
    v = [int(x) for x in f[1:]]
    return {"total": sum(v), "idle": v[3] + (v[4] if len(v) > 4 else 0)}


def snapshot(d):
    s = {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "epoch": time.time(),
         "monotonic_raw_ns": time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW), "problems": []}
    la = read("/proc/loadavg")
    s["loadavg"] = la.split()[:3] if la else None
    s["psi"] = {k: read("/proc/pressure/" + k) for k in ("cpu", "io", "memory")}
    s["stat_cpu"] = stat_cpu()
    s["diskstats"] = diskstats()
    s["block"] = {}
    for b in sorted(glob.glob("/sys/block/*")):
        s["block"][os.path.basename(b)] = {k: (read(b + "/queue/" + k) or "").strip() for k in ("write_cache", "fua")}
    if d:
        try:
            st = os.statvfs(d)
            s["dir"] = d
            s["free_bytes"] = st.f_bavail * st.f_frsize
            r = subprocess.run(["findmnt", "-n", "-o", "SOURCE,FSTYPE,OPTIONS", "-T", d], capture_output=True,
                               text=True, timeout=10)
            s["mount"] = r.stdout.strip()
        except Exception as e:  # recorded, never a pass
            s["problems"].append("dir record: %r" % e)
    for k in REQUIRED:
        if s.get(k) is None:
            s["problems"].append("missing " + k)
    return s


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "start":
        d = sys.argv[sys.argv.index("--dir") + 1] if "--dir" in sys.argv else None
        s = snapshot(d)
        json.dump(s, open(sys.argv[2], "w"), indent=1)
        return 2 if s["problems"] else 0
    if len(sys.argv) == 4 and sys.argv[1] == "end":
        a = json.load(open(sys.argv[2]))
        b = snapshot(a.get("dir"))
        b["window_s"] = b["epoch"] - a["epoch"]
        b["void_rule"] = "none registered for Linux: record only"
        if a.get("stat_cpu") and b.get("stat_cpu"):
            dt = b["stat_cpu"]["total"] - a["stat_cpu"]["total"]
            di = b["stat_cpu"]["idle"] - a["stat_cpu"]["idle"]
            b["cpu_busy_frac"] = (dt - di) / dt if dt > 0 else None
        if a.get("diskstats") and b.get("diskstats"):
            b["diskstats_delta"] = {dev: {k: row[k] - a["diskstats"][dev].get(k, 0) for k in row}
                                    for dev, row in b["diskstats"].items() if dev in a["diskstats"]}
        b["problems"] = a.get("problems", []) + b["problems"]
        json.dump(b, open(sys.argv[3], "w"), indent=1)
        return 2 if b["problems"] else 0
    print(__doc__, file=sys.stderr)
    return 2


sys.exit(main())
