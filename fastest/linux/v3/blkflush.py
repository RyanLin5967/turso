#!/usr/bin/env python3
"""blkflush.py -- count the flush requests ISSUED to each block device, per op window (V3 review 2 item 2).

  blkflush.py start OUT [--buffer-kb K]     make OUT/, a private tracefs instance tracing block:block_rq_issue whose
                                            rwbs has an F (a flush request, a preflush or a FUA write), on
                                            trace_clock mono_raw; write OUT/start.json
  blkflush.py stop OUT                      stop it; keep OUT/trace.txt.gz, OUT/stats.json (per-CPU ring buffer
                                            stats), OUT/stop.json; remove the instance
  blkflush.py report OUT [--device D] [--windows RAW.tsv [--pid P]]
                                            print JSON: per device the F requests by kind, and with --windows (a
                                            v3floor raw.tsv: arm, i, ns, t0_ns) per arm the requests inside its ops'
                                            CLOCK_MONOTONIC_RAW windows; with --pid, per arm the windows in which
                                            process P entered no fsync or fdatasync (syscalls:sys_enter_fsync and
                                            sys_enter_fdatasync, traced in the same instance: annex ruling A16, the
                                            app's own sync per op, which a write-through drive's zero flush count
                                            cannot show)
  blkflush.py gen DEV OUT.tsv N             fire-check generator (not a measurement): N fsync(2)s of the raw block
                                            device DEV, then N buffered 4 KiB writes, then N empty windows, each
                                            window recorded as a raw.tsv row (arms devfsync, devwrite, idle)
  blkflush.py self-test                     the parser and the window attribution on planted text; exit 0 iff all pass
Exit: 0 ok | 2 refused (usage, a tracefs step failed, an instance exists, events were lost, a line did not parse,
windows overlap) | 1 self-test failure. Every tracefs step runs through `sudo -n`; a step that fails refuses.

WHAT THE COUNT PROVES. block_rq_issue fires when the block layer hands a request to the device's driver. A request
counted here was ISSUED: it says nothing about what the device did with it, and on a device whose queue/write_cache
reads "write through" the block layer strips REQ_PREFLUSH and REQ_FUA before a request exists, so the count there
is 0 by construction (the GitHub-hosted runners' sda is such a device: on those disks the count proves only what was
issued to the loop devices above them, never that a drive persisted anything). A bio-based device (brd) issues no
requests, so it reads 0 too. On an NVMe namespace with native multipath the requests are issued on the path disk
(nvmeXcYnZ), which the device map names. Other processes' flushes on the same device inside a window are counted
too, so a per-window count is an upper bound on the op's own flushes and a zero is exact.

Kinds, from the rwbs field (blk_fill_rwbs): "flush" = a REQ_OP_FLUSH request ("FF" or "F"); "fua" = a write with
REQ_FUA ("WF..."); "preflush" = a write still carrying REQ_PREFLUSH ("FW..."); "preflush+fua"; "other". Per arm and
device the report gives zero_windows (no F request at all), flush_carrying_zero_windows (no flush, preflush or
preflush+fua request: a FUA-only write makes only itself durable) and bare_flush_zero_windows (no REQ_OP_FLUSH), so a
window counts once whatever else it holds; and ambiguous_by_device, the events that may belong to the arm's windows
but cannot be placed in exactly one (every window under 1 us is one such).
Timestamps: the trace prints mono_raw in microseconds (rounded), so an event's true time is +-500 ns of the printed
value; an event whose interval is not inside exactly one window is "ambiguous", counted, never attributed.
"""
import bisect, gzip, json, os, re, subprocess, sys, time

FILTER = 'rwbs ~ "*F*"'
EVENT = "events/block/block_rq_issue"
SYSEVENTS = ["events/syscalls/sys_enter_fsync", "events/syscalls/sys_enter_fdatasync"]
SYSLINE = re.compile(r"^\s*(?P<comm>.+?)-(?P<pid>\d+)\s+\[(?P<cpu>\d+)\]\s+(?:(?P<flags>\S+)\s+)?(?P<ts>\d+\.\d+):\s+"
                     r"sys_(?P<sc>fsync|fdatasync)\((?P<args>[^)]*)\)\s*$")
LINE = re.compile(r"^\s*(?P<comm>.+?)-(?P<pid>\d+)\s+\[(?P<cpu>\d+)\]\s+(?:(?P<flags>\S+)\s+)?(?P<ts>\d+\.\d+):\s+"
                  r"(?P<ev>[a-z_]+):\s+(?P<body>.*)$")
BODY = re.compile(r"^(?P<maj>\d+),(?P<min>\d+)\s+(?P<rwbs>[A-Z]+)\s+(?P<bytes>\d+)\s+\((?P<cmd>[^)]*)\)\s+"
                  r"(?P<sector>\d+)\s+\+\s+(?P<nr>\d+)(?P<rest>.*)$")
ENTRIES = re.compile(r"^#\s*entries-in-buffer/entries-written:\s*(\d+)/(\d+)")
MONO_RAW = getattr(time, "CLOCK_MONOTONIC_RAW", 4)
PROVES = ("flush requests and FUA writes ISSUED to each device's driver (tracefs block:block_rq_issue), never "
          "persistence; on a device reporting write-through the block layer strips flushes before issue, so 0 there by "
          "construction; other processes' requests inside a window are counted too, and a request an op causes after "
          "its window closes is not, so a window's count is neither a strict upper nor lower bound on the op's own; a "
          "zero is exact only for windows longer than 1 us (the trace prints microseconds)")


class Refuse(Exception):
    pass


def tracefs():
    for t in ("/sys/kernel/tracing", "/sys/kernel/debug/tracing"):
        r = subprocess.run(["sudo", "-n", "test", "-d", t + "/instances"], capture_output=True, timeout=30)
        if r.returncode == 0:
            return t
    raise Refuse("no tracefs with instances/ under /sys/kernel/tracing or /sys/kernel/debug/tracing (or sudo -n failed)")


def sudo(args, inp=None, timeout=120):
    r = subprocess.run(["sudo", "-n"] + args, input=inp, capture_output=True, text=True, errors="replace", timeout=timeout)
    if r.returncode != 0:
        raise Refuse("sudo %s: rc %d: %s" % (" ".join(args), r.returncode, r.stderr.strip()[:300]))
    return r.stdout


def swrite(path, value):
    sudo(["tee", path], inp=value)


def sread(path):
    return sudo(["cat", path])


def devmap():
    """dev 'maj:min' -> name, for every block device sysfs shows, plus hidden NVMe path disks."""
    m = {}
    roots = ["/sys/class/block"]
    for root in roots:
        try:
            names = os.listdir(root)
        except OSError:
            continue
        for n in names:
            try:
                with open(os.path.join(root, n, "dev")) as f:
                    m[f.read().strip()] = n
            except OSError:
                pass
    try:
        for c in os.listdir("/sys/class/nvme"):
            d = os.path.join("/sys/class/nvme", c)
            for n in os.listdir(d):
                if re.fullmatch(r"nvme\d+c\d+n\d+", n):
                    try:
                        with open(os.path.join(d, n, "dev")) as f:
                            m.setdefault(f.read().strip(), n)
                    except OSError:
                        pass
    except OSError:
        pass
    return m


def start(out, buffer_kb):
    if os.path.lexists(out):
        raise Refuse("%s exists" % out)
    tr = tracefs()
    leaked = [x for x in sudo(["ls", tr + "/instances"]).split() if x.startswith("v3blk_")]
    if leaked:  # a leaked instance keeps tracing every later batch, unrecorded (fresh review B-L6)
        raise Refuse("tracefs instance(s) %s already exist (a killed run?); remove them: sudo rmdir %s/instances/%s"
                     % (", ".join(leaked), tr, leaked[0]))
    inst = "v3blk_%d_%d" % (os.getpid(), time.time_ns() % 1000000007)
    ip = tr + "/instances/" + inst
    if subprocess.run(["sudo", "-n", "test", "-e", ip], capture_output=True, timeout=30).returncode == 0:
        raise Refuse("tracefs instance %s already exists" % ip)
    os.makedirs(out)
    sudo(["mkdir", ip])
    rec = {"tool": "blkflush.py", "instance": inst, "instance_path": ip, "filter": FILTER, "event": "block:block_rq_issue",
           "proves": PROVES}
    try:
        swrite(ip + "/tracing_on", "0")
        swrite(ip + "/trace_clock", "mono_raw")
        clk = sread(ip + "/trace_clock").strip()
        if "[mono_raw]" not in clk:
            raise Refuse("trace_clock did not take mono_raw: %s" % clk)
        swrite(ip + "/buffer_size_kb", str(buffer_kb))
        rec["buffer_size_kb"] = sread(ip + "/buffer_size_kb").strip()
        swrite(ip + "/" + EVENT + "/filter", FILTER)
        got = sread(ip + "/" + EVENT + "/filter").strip()
        if got != FILTER:
            raise Refuse("the event filter reads %r, not %r" % (got, FILTER))
        swrite(ip + "/" + EVENT + "/enable", "1")
        if sread(ip + "/" + EVENT + "/enable").strip() != "1":
            raise Refuse("the event did not enable")
        for se in SYSEVENTS:  # the app's own syncs, per op (A16)
            if subprocess.run(["sudo", "-n", "test", "-e", ip + "/" + se + "/enable"], capture_output=True,
                              timeout=30).returncode != 0:
                raise Refuse("no %s tracepoint (CONFIG_FTRACE_SYSCALLS): the per-op sync count cannot be taken" % se)
            swrite(ip + "/" + se + "/enable", "1")
            if sread(ip + "/" + se + "/enable").strip() != "1":
                raise Refuse("%s did not enable" % se)
        rec["syscall_events"] = [x.split("/")[-1] for x in SYSEVENTS]
        swrite(ip + "/trace", "")  # opened O_TRUNC: clears the buffer
        rec["start_mono_raw_ns"] = time.clock_gettime_ns(MONO_RAW)  # before tracing_on: every event is later
        swrite(ip + "/tracing_on", "1")
        rec["trace_clock"] = clk
    except Exception:
        subprocess.run(["sudo", "-n", "rmdir", ip], capture_output=True, timeout=30)
        raise
    rec["devices"] = devmap()
    with open(os.path.join(out, "start.json"), "w") as f:
        json.dump(rec, f, indent=1)
    return rec


def parse_stats(text):
    st = {}
    for line in text.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            v = v.strip()
            try:
                st[k.strip()] = int(v)
            except ValueError:
                st[k.strip()] = v
    return st


def stop(out):
    try:
        with open(os.path.join(out, "start.json")) as f:
            s = json.load(f)
    except (OSError, ValueError):
        raise Refuse("%s/start.json missing or unreadable: nothing was started there" % out)
    if os.path.exists(os.path.join(out, "stop.json")):
        raise Refuse("%s was already stopped" % out)
    ip = s["instance_path"]
    try:
        swrite(ip + "/tracing_on", "0")
        stop_ns = time.clock_gettime_ns(MONO_RAW)  # after tracing_off: every event is earlier
        text = sread(ip + "/trace")
        stats = {}
        for c in sudo(["ls", ip + "/per_cpu"]).split():
            stats[c] = parse_stats(sread(ip + "/per_cpu/%s/stats" % c))
    finally:  # the instance is removed whatever failed above
        for ev in [EVENT] + SYSEVENTS:
            subprocess.run(["sudo", "-n", "tee", ip + "/" + ev + "/enable"], input="0", capture_output=True, text=True,
                           timeout=30)
        subprocess.run(["sudo", "-n", "rmdir", ip], capture_output=True, timeout=30)
    if subprocess.run(["sudo", "-n", "test", "-e", ip], capture_output=True, timeout=30).returncode == 0:
        raise Refuse("tracefs instance %s could not be removed" % ip)
    with gzip.open(os.path.join(out, "trace.txt.gz"), "wt") as f:
        f.write(text)
    with open(os.path.join(out, "stats.json"), "w") as f:
        json.dump(stats, f, indent=1)
    rec = {"stop_mono_raw_ns": stop_ns, "instance_removed": True, "trace_bytes": len(text)}
    with open(os.path.join(out, "stop.json"), "w") as f:
        json.dump(rec, f, indent=1)
    return rec


def classify(rwbs):
    pre, s = False, rwbs
    if len(s) >= 2 and s[0] == "F" and s[1] in "WDRNF":
        pre, op, rest = True, s[1], s[2:]
    else:
        op, rest = s[:1], s[1:]
    if op == "D" and rest.startswith("E"):
        rest = rest[1:]
    fua = rest.startswith("F")
    if op == "F":
        return "flush"
    if fua and pre:
        return "preflush+fua"
    if fua:
        return "fua"
    if pre:
        return "preflush"
    return "other"


def ts_ns(ts):
    sec, frac = ts.split(".")
    return int(sec) * 10 ** 9 + int((frac + "000000000")[:9]), (10 ** (9 - len(frac))) // 2 if len(frac) < 9 else 0


def parse_trace(text, devices):
    """-> (block events, problems): parse_trace_all without the syscall events."""
    events, _, probs = parse_trace_all(text, devices)
    return events, probs


def parse_trace_all(text, devices):
    """-> (block events, syscall events, problems). A block event: (ts_ns, half_width_ns, device name, rwbs, kind,
    comm, pid); a syscall event: (ts_ns, half_width_ns, "sys", name, name, comm, pid) for fsync and fdatasync."""
    events, sysev, probs, entries = [], [], [], None
    for line in text.splitlines():
        if not line.strip():
            continue
        m = ENTRIES.match(line)
        if m:
            entries = (int(m.group(1)), int(m.group(2)))
            continue
        if line.startswith("#"):
            continue
        if "LOST" in line and "EVENTS" in line:
            probs.append("events lost: " + line.strip()[:120])
            continue
        m = LINE.match(line)
        if not m:
            sm = SYSLINE.match(line)
            if sm:
                t, hw = ts_ns(sm.group("ts"))
                sysev.append((t, hw, "sys", sm.group("sc"), sm.group("sc"), sm.group("comm"), int(sm.group("pid"))))
                continue
            probs.append("unparsed line: " + line[:160])
            continue
        if m.group("ev") != "block_rq_issue":
            probs.append("an event other than block_rq_issue: " + line[:160])
            continue
        b = BODY.match(m.group("body"))
        if not b:
            probs.append("unparsed block_rq_issue body: " + line[:160])
            continue
        dev = "%s:%s" % (b.group("maj"), b.group("min"))
        t, hw = ts_ns(m.group("ts"))
        rwbs = b.group("rwbs")
        if "F" not in rwbs:
            probs.append("an event the filter should have dropped: " + line[:160])
            continue
        if dev not in devices:  # an unnamed device (a hidden NVMe path disk, a device made after start) cannot be
            probs.append("an event on device %s, which the device map does not name" % dev)  # attributed (B-M3)
            continue
        events.append((t, hw, devices[dev], rwbs, classify(rwbs), m.group("comm"), int(m.group("pid"))))
    if entries is None:
        probs.append("no entries-in-buffer/entries-written header")
    elif entries[0] != entries[1]:
        probs.append("events lost: entries-in-buffer/entries-written %d/%d" % entries)
    elif entries[0] != len(events) + len(sysev):
        probs.append("the header counts %d entries, %d parsed" % (entries[0], len(events) + len(sysev)))
    return events, sysev, probs


def read_windows(path):
    with open(path) as f:
        lines = f.read().splitlines()
    if not lines or lines[0] != "arm\ti\tns\tt0_ns":
        raise Refuse("%s: header is not arm, i, ns, t0_ns" % path)
    w = []
    for line in lines[1:]:
        a, i, ns, t0 = line.split("\t")
        w.append((int(t0), int(t0) + int(ns), a, int(i)))
    w.sort()
    for x, y in zip(w, w[1:]):
        if y[0] < x[1]:
            raise Refuse("windows overlap: %s/%d ends at %d after %s/%d starts at %d" % (x[2], x[3], x[1], y[2], y[3], y[0]))
    return w


def attribute(events, windows):
    """-> {window index: [events]}, ambiguous events, events outside every window."""
    starts = [w[0] for w in windows]
    inside, amb, outside = {}, [], []
    for e in events:
        t, hw = e[0], e[1]
        lo, hi = t - hw, t + hw
        k = bisect.bisect_right(starts, hi)  # windows starting at or before hi
        possible = []
        j = k - 1
        while j >= 0 and windows[j][1] >= lo:
            possible.append(j)
            j -= 1
            if len(possible) > 2:
                break
        if not possible:
            outside.append(e)
        elif len(possible) == 1 and windows[possible[0]][0] <= lo and hi <= windows[possible[0]][1]:
            inside.setdefault(possible[0], []).append(e)
        else:
            amb.append((e, possible))
    return inside, amb, outside


def kinds():
    return {"flush": 0, "fua": 0, "preflush": 0, "preflush+fua": 0, "other": 0}


CARRYING = ("flush", "preflush", "preflush+fua")  # the kinds that flush the device's cache (a FUA-only write does not)


def report_stats_problems(stats):
    probs = []
    if not stats:
        probs.append("no per-CPU ring buffer stats")
    for c, st in sorted(stats.items()):
        for k in ("overrun", "commit overrun", "dropped events"):
            if not isinstance(st.get(k), int):  # a missing or renamed key is not a zero (fresh review B-L7)
                probs.append("per-CPU stats %s lack an integer '%s'" % (c, k))
            elif st.get(k):
                probs.append("events lost: %s %s = %s" % (c, k, st.get(k)))
    return probs


def per_arm(events, w):
    """-> (per-arm records, ambiguous events, outside events) for windows w (read_windows) and parsed events."""
    inside, amb, outside = attribute(events, w)
    arms = {}
    for k, (t0, t1, a, i) in enumerate(w):
        r = arms.setdefault(a, {"ops": 0, "devices": {}, "ambiguous": 0, "ambiguous_by_device": {},
                                "windows_under_1us": 0})
        r["ops"] += 1
        r["windows_under_1us"] += (t1 - t0) < 1000  # too short to hold an attributed event (trace prints us)
    # an event that cannot be placed in exactly one window counts against EVERY arm it may belong to, per device
    # (fourth review L7: a request in a sub-us window is ambiguous, never attributed, so a no-flush arm's budget on
    # a shared drive must count these too)
    for e, poss in amb:
        for a in sorted(set(w[j][2] for j in poss)):
            arms[a]["ambiguous"] += 1
            arms[a]["ambiguous_by_device"][e[2]] = arms[a]["ambiguous_by_device"].get(e[2], 0) + 1
    for k, es in inside.items():
        a = w[k][2]
        for e in es:
            d = arms[a]["devices"].setdefault(e[2], {"events": 0, "by_kind": kinds(), "windows_hit": set(), "max": 0,
                                                     "carrying_hit": set(), "bare_hit": set()})
            d["events"] += 1
            d["by_kind"][e[4]] += 1
            d["windows_hit"].add(k)
            if e[4] in CARRYING:
                d["carrying_hit"].add(k)
            if e[4] == "flush":
                d["bare_hit"].add(k)
    for a, r in arms.items():
        for name, d in r["devices"].items():
            per = {}
            for k in d["windows_hit"]:
                per[k] = sum(1 for e in inside[k] if e[2] == name)
            d["max_in_window"] = max(per.values()) if per else 0
            d["zero_windows"] = r["ops"] - len(d["windows_hit"])
            # windows without a request that flushes the cache (flush, preflush, preflush+fua): a FUA-only write
            # makes only itself durable (fourth review L9); and without a bare REQ_OP_FLUSH
            d["flush_carrying_zero_windows"] = r["ops"] - len(d["carrying_hit"])
            d["bare_flush_zero_windows"] = r["ops"] - len(d["bare_hit"])
            d["per_op"] = round(d["events"] / r["ops"], 4)
            d["flush_per_op"] = round(d["by_kind"]["flush"] / r["ops"], 4)
            d["fua_per_op"] = round(d["by_kind"]["fua"] / r["ops"], 4)
            del d["windows_hit"], d["max"], d["carrying_hit"], d["bare_hit"]
    return arms, amb, outside


NO_SYNC_ARMS = ("nosync25",)  # the one arm that issues no fsync or fdatasync of its own


def sync_windows(sysev, w, pid):
    """per arm: windows in which process pid entered no fsync or fdatasync (and the count it entered)."""
    mine = sorted((e for e in sysev if e[6] == pid), key=lambda e: e[0])
    # the probe is single-threaded and enters its sync inside its op's window, so a sync event belongs to the window
    # its +-500 ns interval overlaps (eighth review M4). Windows are 20-80 ns apart on real batches (ninth review M3),
    # so a sync entering within 0.5 us of its window's start also overlaps the previous window's end. Such an event
    # is the LATER window's when the earlier one needs no sync (nosync25) or already holds its own (events in time
    # order): a window closes only after its sync returns, so a sync never enters within 0.5 us of its own window's
    # end unless that call returned in under 0.5 us -- a clean fsync can -- and then the earlier window still lacks
    # its own, and the event stays ambiguous: attributed to neither, so a missing sync can never be covered by a
    # neighbour's (fail closed).
    starts = [x[0] for x in w]
    inside, amb = {}, []
    for e in mine:
        lo, hi = e[0] - e[1], e[0] + e[1]
        k = bisect.bisect_right(starts, hi)
        poss = []
        j = k - 1
        while j >= 0 and w[j][1] >= lo and len(poss) < 3:
            poss.append(j)
            j -= 1
        poss.sort()
        if len(poss) == 1:
            inside.setdefault(poss[0], []).append(e)
        elif len(poss) == 2 and poss[1] == poss[0] + 1 and (w[poss[0]][2] in NO_SYNC_ARMS or inside.get(poss[0])):
            inside.setdefault(poss[1], []).append(e)
            amb.append((e, poss))  # counted as ambiguous (informational), attributed to the later window
        elif poss:
            amb.append((e, poss))
    res = {}
    for k, (t0, t1, a, i) in enumerate(w):
        r = res.setdefault(a, {"ops": 0, "syncs": 0, "windows_without_a_sync": 0, "ambiguous": 0})
        r["ops"] += 1
        got = len(inside.get(k, []))
        r["syncs"] += got
        r["windows_without_a_sync"] += got == 0
    for e, poss in amb:
        for a in sorted(set(w[j][2] for j in poss)):
            res[a]["ambiguous"] += 1
    return res


def report(out, device=None, windows=None, pid=None):
    try:
        with open(os.path.join(out, "start.json")) as f:
            s = json.load(f)
        with open(os.path.join(out, "stop.json")) as f:
            stp = json.load(f)
        with open(os.path.join(out, "stats.json")) as f:
            stats = json.load(f)
        with gzip.open(os.path.join(out, "trace.txt.gz"), "rt") as f:
            text = f.read()
    except (OSError, ValueError) as e:
        raise Refuse("%s is not a stopped blkflush record: %r" % (out, e))
    events, sysev, probs = parse_trace_all(text, s.get("devices", {}))
    probs += report_stats_problems(stats)
    lo, hi = s.get("start_mono_raw_ns", 0), stp.get("stop_mono_raw_ns", 0)
    early = [e for e in events if e[0] + e[1] < lo or e[0] - e[1] > hi]
    if early:
        probs.append("%d events outside [start, stop]" % len(early))
    if probs:
        raise Refuse("; ".join(probs[:6]))
    if device:
        events = [e for e in events if e[2] == device or e[2] == "dev:" + device]
    dev = {}
    for e in events:
        d = dev.setdefault(e[2], {"total": 0, "by_kind": kinds(), "comms": {}})
        d["total"] += 1
        d["by_kind"][e[4]] += 1
        d["comms"][e[5]] = d["comms"].get(e[5], 0) + 1
    rep = {"tool": "blkflush.py", "proves": PROVES, "instance": s.get("instance"), "filter": s.get("filter"),
           "trace_clock": "mono_raw", "window_s": (hi - lo) / 1e9, "events": len(events), "device_filter": device,
           "devices": dev}
    rep_sys = {"traced": s.get("syscall_events") or [], "events": len(sysev),
               "by_pid": {}}
    for e in sysev:
        rep_sys["by_pid"][str(e[6])] = rep_sys["by_pid"].get(str(e[6]), 0) + 1
    rep["syscalls"] = rep_sys
    if windows:
        w = read_windows(windows)
        if pid is not None and s.get("syscall_events"):
            rep_sys["pid"] = pid
            rep_sys["arms"] = sync_windows(sysev, w, pid)
        arms, amb, outside = per_arm(events, w)
        rep["windows"] = {"source": windows, "n_windows": len(w), "arms": arms, "ambiguous": len(amb),
                          "outside": len(outside),
                          "ambiguous_sample": [list(e[:5]) for e, _ in amb[:5]],
                          "ambiguous_by_device": {}, "outside_by_device": {}}
        for e, _ in amb:
            rep["windows"]["ambiguous_by_device"][e[2]] = rep["windows"]["ambiguous_by_device"].get(e[2], 0) + 1
        for e in outside:
            rep["windows"]["outside_by_device"][e[2]] = rep["windows"]["outside_by_device"].get(e[2], 0) + 1
    return rep


def gen(devpath, out_tsv, n):
    fd = os.open(devpath, os.O_RDWR)
    blk = os.urandom(4096)
    rows = []
    for i in range(n):
        t0 = time.clock_gettime_ns(MONO_RAW)
        os.fsync(fd)
        rows.append(("devfsync", i, time.clock_gettime_ns(MONO_RAW) - t0, t0))
    for i in range(n):
        t0 = time.clock_gettime_ns(MONO_RAW)
        os.pwrite(fd, blk, 4096 * (i + 1))
        rows.append(("devwrite", i, time.clock_gettime_ns(MONO_RAW) - t0, t0))
    for i in range(n):  # empty windows at least 20 us long, so an event inside one could be attributed (B-L9)
        t0 = time.clock_gettime_ns(MONO_RAW)
        t1 = t0
        while t1 - t0 < 20000:
            t1 = time.clock_gettime_ns(MONO_RAW)
        rows.append(("idle", i, t1 - t0, t0))
    os.close(fd)
    with open(out_tsv, "w") as f:
        f.write("arm\ti\tns\tt0_ns\n")
        for r in rows:
            f.write("%s\t%d\t%d\t%d\n" % r)


# ---- self-test: planted text, hand-written expectations ---------------------------------------------------------
HDR = ("# tracer: nop\n#\n# entries-in-buffer/entries-written: %d/%d   #P:4\n#\n"
       "#           TASK-PID     CPU#  |||||  TIMESTAMP  FUNCTION\n")


def ev(comm, pid, cpu, ts, dev, rwbs):
    maj, mnr = dev.split(":")
    return ("%16s-%-7d [%03d] .....  %s: block_rq_issue: %s,%s %s 0 () 18446744073709551615 + 0 none,0,0 [%s]"
            % (comm, pid, cpu, ts, maj, mnr, rwbs, comm))


def self_test():
    res = []

    def chk(name, ok, detail=""):
        res.append(bool(ok))
        print(("PASS " if ok else "FAIL ") + name + ("" if ok else ": " + str(detail)[:300]), flush=True)

    devs = {"7:0": "loop0", "259:0": "nvme0n1", "8:0": "sda"}
    for rwbs, want in (("FF", "flush"), ("F", "flush"), ("WFS", "fua"), ("WFSM", "fua"), ("FWS", "preflush"),
                       ("FWFS", "preflush+fua"), ("DF", "fua"), ("DEF", "fua"), ("RF", "fua")):
        chk("classify %s -> %s" % (rwbs, want), classify(rwbs) == want, classify(rwbs))
    chk("ts_ns 36.365601 -> 36365601000 +-500", ts_ns("36.365601") == (36365601000, 500), ts_ns("36.365601"))
    # windows (ns): w0 [1000000, 1100000] w1 [1100000, 1300000] (abutting), w2 [2000000, 2050000]
    windows = [(1000000, 1100000, "append25", 0), (1100000, 1300000, "nosync25", 0), (2000000, 2050000, "append25", 1)]
    lines = [ev("kworker/0:1H", 10, 0, "0.001050", "7:0", "FF"),    # inside w0
             ev("jbd2/loop0-8", 11, 1, "0.001060", "259:0", "WFS"),  # inside w0, another device
             ev("kworker/0:1H", 10, 0, "0.001100", "7:0", "FF"),    # 1100000 +-500: straddles w0/w1 -> ambiguous
             ev("kworker/0:1H", 10, 0, "0.001500", "7:0", "FF"),    # between w1 and w2 -> outside
             ev("v3floor", 99, 2, "0.002010", "7:0", "FWS"),        # inside w2, preflush
             ev("v3floor", 99, 2, "0.002020", "7:0", "FF")]         # inside w2
    text = HDR % (len(lines), len(lines)) + "\n".join(lines) + "\n"
    events, probs = parse_trace(text, devs)
    chk("planted trace parses: 6 events, no problems", len(events) == 6 and not probs, (len(events), probs))
    inside, amb, outside = attribute(events, windows)
    chk("attribution: w0 holds 2 (loop0 FF, nvme0n1 WFS)", sorted(e[2] for e in inside.get(0, [])) == ["loop0", "nvme0n1"],
        inside.get(0))
    chk("attribution: w1 holds none (its edge event is ambiguous)", 1 not in inside, inside.get(1))
    chk("attribution: w2 holds 2 on loop0 (FWS preflush, FF flush)",
        sorted(e[4] for e in inside.get(2, [])) == ["flush", "preflush"], inside.get(2))
    chk("attribution: 1 ambiguous, 1 outside", len(amb) == 1 and len(outside) == 1, (amb, outside))
    # per arm (fourth review L7, L9): window kinds, written by hand from the plant above plus one preflush write on
    # nvme0n1 inside w2 (so nvme0n1 has a FUA-only window w0 and a preflush-only window w2)
    lines2 = lines + [ev("jbd2/nvme0n1-8", 12, 1, "0.002030", "259:0", "FWS")]
    events2, _ = parse_trace(HDR % (len(lines2), len(lines2)) + "\n".join(lines2) + "\n", devs)
    arms, _, _ = per_arm(events2, windows)
    a25 = arms.get("append25", {}).get("devices", {})
    lp, nv = a25.get("loop0", {}), a25.get("nvme0n1", {})
    chk("per arm: append25 on loop0 has a flush-carrying and a bare flush request in both its windows",
        lp.get("zero_windows") == 0 and lp.get("flush_carrying_zero_windows") == 0 and lp.get("bare_flush_zero_windows") == 0, lp)
    chk("per arm: append25 on nvme0n1 (w0 a FUA-only write, w2 a preflush write): a request in both windows, a "
        "flush-carrying one in 1 (w2), a bare flush in none",
        nv.get("zero_windows") == 0 and nv.get("flush_carrying_zero_windows") == 1 and nv.get("bare_flush_zero_windows") == 2, nv)
    chk("per arm: the edge event counts as ambiguous for both arms it may belong to, on loop0",
        arms.get("append25", {}).get("ambiguous_by_device") == {"loop0": 1}
        and arms.get("nosync25", {}).get("ambiguous_by_device") == {"loop0": 1}, (arms.get("append25"), arms.get("nosync25")))
    sub = [(5000000, 5000800, "nosync25", 1)]
    sev, _ = parse_trace(HDR % (1, 1) + ev("jbd2/sda1-8", 7, 0, "0.005000", "8:0", "FF") + "\n", devs)
    sarms, _, _ = per_arm(sev, sub)
    chk("per arm: a flush inside an 800 ns window is never attributed and counts as ambiguous for its arm on sda",
        sarms["nosync25"]["windows_under_1us"] == 1 and sarms["nosync25"]["devices"] == {}
        and sarms["nosync25"]["ambiguous_by_device"] == {"sda": 1}, sarms)
    bad = text.replace("entries-written: 6/6", "entries-written: 6/9")
    chk("refuses a header that lost events (6/9)", any("lost" in p for p in parse_trace(bad, devs)[1]), "")
    chk("refuses a [LOST n EVENTS] line",
        any("lost" in p for p in parse_trace(text + "CPU:2 [LOST 3 EVENTS]\n", devs)[1]), "")
    chk("refuses an unparsed line", any("unparsed" in p for p in parse_trace(text + "garbage here\n", devs)[1]), "")
    chk("refuses a header/parse count mismatch",
        any("parsed" in p for p in parse_trace(HDR % (7, 7) + "\n".join(lines) + "\n", devs)[1]), "")
    chk("refuses an event without F (the filter was not applied)",
        any("filter" in p for p in parse_trace(HDR % (1, 1) + ev("x", 1, 0, "0.001", "7:0", "WS") + "\n", devs)[1]), "")
    chk("refuses a trace with no header", any("header" in p for p in parse_trace("\n".join(lines) + "\n", devs)[1]), "")
    chk("refuses an event on a device the map does not name",
        any("does not name" in p for p in parse_trace(HDR % (1, 1) + ev("x", 1, 0, "0.001", "9:9", "FF") + "\n", devs)[1]), "")
    chk("refuses per-CPU stats without the overrun keys", report_stats_problems({"cpu0": {"entries": 3}}) != [], "")
    chk("accepts per-CPU stats with zero overruns",
        report_stats_problems({"cpu0": {"overrun": 0, "commit overrun": 0, "dropped events": 0}}) == [], "")
    # A16: the app's own syncs per window, from syscall tracepoints, by pid (expectations by hand)
    def sev(comm, pid, ts, sc="fsync"):
        return "%16s-%-7d [%03d] .....  %s: sys_%s(fd: 0x00000003)" % (comm, pid, 0, ts, sc)
    slines = [sev("v3floor", 99, "0.001020"), sev("v3floor", 99, "0.001080", "fdatasync"),  # both in w0
              sev("other", 7, "0.002010"),                                                 # w2, another pid
              ev("kworker/0:1H", 10, 0, "0.001050", "7:0", "FF")]
    be, se, sp = parse_trace_all(HDR % (len(slines), len(slines)) + "\n".join(slines) + "\n", devs)
    chk("syscall lines parse: 3 syscall events and 1 block event, the header count matching both",
        len(se) == 3 and len(be) == 1 and not sp and [x[3] for x in se] == ["fsync", "fdatasync", "fsync"], (se, sp))
    sw = sync_windows(se, windows, 99)
    # eighth review M4: a sync printed 0.3 us after its window opened (interval straddling the start) is that window's
    edge = [(1000000, 1100000, "append25", 0), (1200000, 1300000, "append25", 1)]
    ee, _ = parse_trace_all(HDR % (2, 2) + "\n".join([sev("v3floor", 99, "0.001000"), sev("v3floor", 99, "0.001200")]) + "\n", devs)[1:], None
    swe = sync_windows(ee[0], edge, 99)
    chk("sync windows: a sync whose +-500 ns interval overlaps only its own window's start is attributed to it",
        swe.get("append25") == {"ops": 2, "syncs": 2, "windows_without_a_sync": 0, "ambiguous": 0}, swe)
    # ninth review M3: real batches leave 20-80 ns between windows (testdata raw.tsv, round 0), so a sync entering
    # 300 ns after its window opened has an interval reaching back into the previous window; it is the later window's
    # (a sync never enters within 500 ns of its own window's end: the window closes after the sync returns)
    tight = [(1000000, 1099920, "append25", 0), (1100000, 1199920, "append25", 1), (1200000, 1299960, "append25", 2)]
    te, _ = parse_trace_all(HDR % (3, 3) + "\n".join([sev("v3floor", 99, "0.001020"), sev("v3floor", 99, "0.001100"),
                                                     sev("v3floor", 99, "0.001200")]) + "\n", devs)[1:], None
    swt = sync_windows(te[0], tight, 99)
    chk("sync windows: gaps of 80 and 40 ns, syncs printed at each later window's start -> every window holds one",
        (swt.get("append25") or {}).get("windows_without_a_sync") == 0 and (swt.get("append25") or {}).get("syncs") == 3,
        swt)
    # ... and a probe that drops the second window's sync still leaves a window without one (the shift cannot hide it)
    te2, _ = parse_trace_all(HDR % (2, 2) + "\n".join([sev("v3floor", 99, "0.001020"), sev("v3floor", 99, "0.001200")])
                             + "\n", devs)[1:], None
    swt2 = sync_windows(te2[0], tight, 99)
    chk("sync windows: the same windows with the second window's sync missing -> a window without a sync",
        (swt2.get("append25") or {}).get("windows_without_a_sync", 0) >= 1, swt2)
    # ... and a clean op whose fsync returned within 0.5 us of its window's end (its interval straddling into the next
    # window) never fills a following gated window that issued no sync: clean expects its own sync and has none
    cw = [(1000000, 1000900, "clean", 0), (1000950, 1100000, "append25", 0)]
    ce, _ = parse_trace_all(HDR % (1, 1) + sev("v3floor", 99, "0.001001") + "\n", devs)[1:], None
    swc = sync_windows(ce[0], cw, 99)
    chk("sync windows: clean's fast fsync straddling into an append25 window with no sync -> append25 still lacks one",
        (swc.get("append25") or {}).get("windows_without_a_sync") == 1, swc)
    chk("sync windows: pid 99 synced in w0 (2 syncs) and in no other window; another pid's fsync in w2 does not count",
        sw.get("append25") == {"ops": 2, "syncs": 2, "windows_without_a_sync": 1, "ambiguous": 0}
        and sw.get("nosync25", {}).get("windows_without_a_sync") == 1, sw)
    import shutil, tempfile
    rd_ = tempfile.mkdtemp(prefix="blkflush-selftest-")
    try:
        rec = os.path.join(rd_, "rec")
        os.makedirs(rec)
        allines = lines + [sev("v3floor", 99, "0.001020"), sev("v3floor", 99, "0.002020")]
        with open(os.path.join(rec, "start.json"), "w") as f:
            json.dump({"devices": devs, "start_mono_raw_ns": 0, "syscall_events": ["sys_enter_fsync", "sys_enter_fdatasync"],
                       "instance": "planted", "filter": FILTER}, f)
        with open(os.path.join(rec, "stop.json"), "w") as f:
            json.dump({"stop_mono_raw_ns": 10 ** 9}, f)
        with open(os.path.join(rec, "stats.json"), "w") as f:
            json.dump({"cpu0": {"overrun": 0, "commit overrun": 0, "dropped events": 0}}, f)
        with gzip.open(os.path.join(rec, "trace.txt.gz"), "wt") as f:
            f.write(HDR % (len(allines), len(allines)) + "\n".join(allines) + "\n")
        wt = os.path.join(rd_, "raw.tsv")
        with open(wt, "w") as f:
            f.write("arm\ti\tns\tt0_ns\n")
            for t0, t1, a, i in windows:
                f.write("%s\t%d\t%d\t%d\n" % (a, i, t1 - t0, t0))
        r = report(rec, None, wt, 99)
        sa = (r.get("syscalls") or {}).get("arms") or {}
        chk("report() end to end on a planted record: per-arm syncs by pid 99 (append25 synced in both windows), the "
            "block events by device, and the syscalls record", sa.get("append25", {}).get("windows_without_a_sync") == 0
            and sa.get("append25", {}).get("syncs") == 2 and r["syscalls"]["pid"] == 99
            and r["windows"]["arms"]["append25"]["devices"]["loop0"]["events"] == 3, r.get("syscalls"))
        r2 = report(rec, None, wt, None)
        chk("report() without --pid: the syscalls record has no per-arm sync count (post then refuses)",
            "arms" not in r2["syscalls"] and r2["syscalls"]["events"] == 2, r2.get("syscalls"))
    finally:
        shutil.rmtree(rd_)
    tmp = os.path.join(os.environ.get("TMPDIR", "/tmp"), "blkflush-selftest-%d.tsv" % os.getpid())
    with open(tmp, "w") as f:
        f.write("arm\ti\tns\tt0_ns\nappend25\t0\t100\t1000\nnosync25\t0\t100\t1050\n")
    try:
        read_windows(tmp)
        chk("refuses overlapping windows", False, "accepted")
    except Refuse:
        chk("refuses overlapping windows", True)
    os.unlink(tmp)
    ok = all(res) and len(res) > 0
    print("BLKFLUSH SELF-TEST %d/%d %s" % (sum(res), len(res), "PASS" if ok else "FAIL"))
    return 0 if ok else 1


def main(argv):
    try:
        if len(argv) >= 2 and argv[0] == "start":
            kb = 16384
            if len(argv) == 4 and argv[2] == "--buffer-kb" and argv[3].isdigit():
                kb = int(argv[3])
            elif len(argv) != 2:
                raise Refuse("usage: start OUT [--buffer-kb K]")
            print(json.dumps(start(argv[1], kb)))
            return 0
        if len(argv) == 2 and argv[0] == "stop":
            print(json.dumps(stop(argv[1])))
            return 0
        if len(argv) >= 2 and argv[0] == "report":
            dev = win = pid = None
            rest = argv[2:]
            while rest:
                if rest[0] == "--device" and len(rest) >= 2:
                    dev, rest = rest[1], rest[2:]
                elif rest[0] == "--windows" and len(rest) >= 2:
                    win, rest = rest[1], rest[2:]
                elif rest[0] == "--pid" and len(rest) >= 2 and rest[1].isdigit():
                    pid, rest = int(rest[1]), rest[2:]
                else:
                    raise Refuse("usage: report OUT [--device D] [--windows RAW.tsv [--pid P]]")
            print(json.dumps(report(argv[1], dev, win, pid), indent=1, sort_keys=True))
            return 0
        if len(argv) == 4 and argv[0] == "gen" and argv[3].isdigit():
            gen(argv[1], argv[2], int(argv[3]))
            return 0
        if argv == ["self-test"]:
            return self_test()
        raise Refuse("usage: blkflush.py start OUT [--buffer-kb K] | stop OUT | report OUT [--device D] [--windows RAW.tsv [--pid P]]"
                     " | gen DEV OUT.tsv N | self-test")
    except Refuse as e:
        print(json.dumps({"refused": str(e)}))
        print("blkflush: REFUSED: %s" % e, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
