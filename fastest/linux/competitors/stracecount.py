#!/usr/bin/env python3
"""stracecount.py -- exact flush counts from `strace -f -C -y` output (lane fastest-linux-comp).

  stracecount.py count TRACE [--extra FILE]... [--root DIR] [--window W]   -> JSON on stdout
                      (--extra: strace's stderr file; --window: trace.sh's OUT.window, whose proven-attach line is
                      what licenses reading a table-less, call-less attach window as zero)
  stracecount.py cell --name N --load L.json --idle I.json --load-s S --idle-s S --ops N
                      [--deferred D.json] [--ops-ok N]          -> JSON on stdout
  stracecount.py table DIR                                       -> TSV of every cell.json under DIR

The strace run must use -f (follow forks and threads), -C (the -c summary table AND the per-call lines), -y (fd
paths) and an -e trace= set that includes every syscall named in TRACED below. `count` reads both halves:
  - the -c summary table (calls and errors per syscall: strace's own exact count), and
  - the per-call lines, counted independently (a call is counted at its start line; "<... resumed>" lines are
    joined to it), for the flag/path detail the table cannot give.
A trace whose two halves disagree, or that has no summary table, or no lines at all, is REFUSED: a count read
from half an instrument is not a count.

What counts as a FLUSH (a request that data reach stable storage): fsync, fdatasync, syncfs, sync, and msync with
MS_SYNC. Reported apart, never added: sync_file_range (writeback hint, no cache flush), msync without MS_SYNC.
BLIND SPOTS this counter cannot close, so it refuses (verdict INCOMPLETE) when it sees one rather than undercount:
  - a file opened, or F_SETFL'd, with O_SYNC/O_DSYNC (each write is then a flush; writes are not traced);
  - pwritev2 with RWF_SYNC/RWF_DSYNC;
  - io_uring (io_uring_setup/enter/register): an IORING_OP_FSYNC is invisible to strace;
  - io_submit (Linux AIO), whose iocbs may carry IOCB_FLAG / O_DSYNC semantics strace does not decode here.
It also cannot see flushes issued by the kernel on its own (journal commits, writeback); those are not requests by
the system under test and are out of scope for "flushes per op".
"""
import json
import os
import re
import sys

FLUSH = ("fsync", "fdatasync", "syncfs", "sync")
TRACED = ("fsync", "fdatasync", "sync_file_range", "syncfs", "sync", "msync", "copy_file_range", "ioctl",
          "openat", "fcntl", "pwritev2", "io_submit", "io_uring_setup", "io_uring_enter", "io_uring_register")
# strace -f -o FILE prefixes every line with the pid ("%-5d "); stderr output uses "[pid N] "; accept both, and none.
START = re.compile(r"^(?:(?:\[pid\s+)?(\d+)\]?\s+)?([a-z_0-9]+)\((.*)$")
RESUMED = re.compile(r"^(?:(?:\[pid\s+)?(\d+)\]?\s+)?<\.\.\. ([a-z_0-9]+) resumed>(.*)$")
SUMROW = re.compile(r"^\s*([\d.]+)\s+([\d.]+)\s+(\d+)\s+(\d+)\s+(?:(\d+)\s+)?([a-z_0-9]+)\s*$")
RET = re.compile(r"\)\s+=\s+(-?\d+|\?)")
FDPATH = re.compile(r"^\s*-?\d+<([^>]*)>")


def parse_summary(text):
    rows, seen = {}, False
    for line in text.splitlines():
        if line.startswith("% time"):
            seen, rows = True, {}  # the last table wins (one per strace run)
            continue
        if not seen:
            continue
        m = SUMROW.match(line)
        if m and m.group(6) != "total":
            rows[m.group(6)] = {"calls": int(m.group(4)), "errors": int(m.group(5) or 0)}
    return rows if seen else None


def klass(path, root):
    if path is None:
        return "-"
    if root and (path == root or path.startswith(root.rstrip("/") + "/")):
        rel = path[len(root.rstrip("/")) + 1:]
        parts = [p for p in rel.split("/") if p]
        if not parts:
            return "."
        if len(parts) == 1:
            return parts[0]
        second = "*" if re.search(r"\d", parts[1]) else parts[1]
        return parts[0] + "/" + second
    return "outside:" + os.path.dirname(path)


def count(trace, extras, root, window=None):
    with open(trace, errors="replace") as f:
        text = f.read()
    summary = parse_summary(text)
    stray = []  # strace's own stderr, minus a summary table, in an ATTACH window: attach/ptrace errors land here
    win = open(window).read() if window and os.path.exists(window) else ""
    attached = "attached_after_polls=" in win
    for e in extras:
        if not os.path.exists(e):
            continue
        with open(e, errors="replace") as f:
            etext = f.read()
        if summary is None:
            summary = parse_summary(etext)
        if attached:
            stray += [ln for ln in etext.splitlines() if ln.strip() and not ln.startswith(("% time", "------"))
                      and not SUMROW.match(ln)]
    lines = {}
    pending = {}
    calls = []  # (name, args_and_rest)
    for line in text.splitlines():
        m = RESUMED.match(line)
        if m:
            pid, name, rest = m.groups()
            if pid in pending and pending[pid][0] == name:
                calls.append((name, pending.pop(pid)[1] + rest))
            continue
        m = START.match(line)
        if not m:
            continue
        pid, name, rest = m.groups()
        lines[name] = lines.get(name, 0) + 1
        if rest.endswith("<unfinished ...>"):
            pending[pid] = (name, rest[: -len("<unfinished ...>")])
        else:
            calls.append((name, rest))
    # strace allocates its -c counters at the first counted call, so a window in which no traced syscall happened
    # prints NO table. That is a true zero only when the attach was proven (trace.sh's TracerPid check wrote
    # attached_after_polls= into the window file) and strace said nothing on stderr; otherwise it stays refused.
    empty_window = summary is None and not lines and attached and not stray
    if empty_window:
        summary = {}
    out = {"trace": trace, "root": root, "summary_found": summary is not None, "summary": summary or {},
           "lines": lines, "unfinished_at_end": len(pending), "attached_proven": attached,
           "empty_window": empty_window, "strace_stderr": stray[:20]}
    flush = {k: 0 for k in FLUSH}
    flush["msync_sync"] = 0
    other = {"sync_file_range": 0, "msync_nosync": 0, "copy_file_range_calls": 0, "copy_file_range_bytes": 0,
             "ficlone": 0, "osync_opens": 0, "osync_fcntl": 0, "rwf_sync_writes": 0, "io_uring": 0, "io_submit": 0}
    by_class, by_path = {}, {}
    for name, rest in calls:
        rm = RET.search(rest)
        ret = rm.group(1) if rm else "?"
        if name in FLUSH or name == "msync":
            if name == "msync":
                if "MS_SYNC" in rest:
                    key = "msync_sync"
                else:
                    other["msync_nosync"] += 1
                    continue
            else:
                key = name
            flush[key] += 1
            pm = FDPATH.match(rest)
            path = pm.group(1) if pm else None
            c = klass(path, root)
            by_class.setdefault(c, {}).setdefault(key, 0)
            by_class[c][key] += 1
            if path:
                by_path[path] = by_path.get(path, 0) + 1
        elif name == "sync_file_range":
            other["sync_file_range"] += 1
        elif name == "copy_file_range":
            other["copy_file_range_calls"] += 1
            if ret not in ("?",) and not ret.startswith("-"):
                other["copy_file_range_bytes"] += int(ret)
        elif name == "ioctl":
            if "FICLONE" in rest or "BTRFS_IOC_CLONE" in rest:
                other["ficlone"] += 1
        elif name == "openat":
            if re.search(r"\bO_D?SYNC\b", rest.split(")")[0]):
                other["osync_opens"] += 1
        elif name == "fcntl":
            if "F_SETFL" in rest and re.search(r"\bO_D?SYNC\b", rest):
                other["osync_fcntl"] += 1
        elif name == "pwritev2":
            if re.search(r"RWF_D?SYNC", rest):
                other["rwf_sync_writes"] += 1
        elif name.startswith("io_uring"):
            other["io_uring"] += 1
        elif name == "io_submit":
            other["io_submit"] += 1
    out["flush_by_syscall"] = flush
    out["flushes"] = sum(flush.values())
    out.update(other)
    out["by_class"] = by_class
    out["top_paths"] = sorted(by_path.items(), key=lambda kv: -kv[1])[:15]
    problems, blind = [], []
    if summary is None:
        problems.append("no -c summary table found")
    if stray:
        problems.append(f"strace stderr in an attach window: {stray[0][:200]}")
    if not lines and summary:
        problems.append("summary present but zero call lines (was -C used?)")
    if summary is not None:
        for name in set(summary) | set(lines):
            s = summary.get(name, {}).get("calls", 0)
            n = lines.get(name, 0)
            # A call interrupted by the detach can be in one half only.
            if abs(s - n) > out["unfinished_at_end"]:
                problems.append(f"{name}: summary {s} calls vs {n} lines")
        sflush = sum(summary.get(k, {}).get("calls", 0) for k in FLUSH)
        lflush = sum(flush[k] for k in FLUSH)
        if abs(sflush - lflush) > out["unfinished_at_end"]:
            problems.append(f"flush syscalls: summary {sflush} vs lines {lflush}")
    for k, why in (("osync_opens", "O_SYNC/O_DSYNC open"), ("osync_fcntl", "F_SETFL O_SYNC/O_DSYNC"),
                   ("rwf_sync_writes", "pwritev2 RWF_(D)SYNC"), ("io_uring", "io_uring in use"),
                   ("io_submit", "Linux AIO io_submit")):
        if out[k]:
            blind.append(f"{why} x{out[k]}")
    out["problems"], out["blind"] = problems, blind
    out["verdict"] = ("REFUSED: " + "; ".join(problems)) if problems else (
        ("INCOMPLETE: " + "; ".join(blind)) if blind else "ok")
    return out


def cell(a):
    load = json.load(open(a["load"]))
    embedded = a["idle"] == "none"
    if embedded:
        # An embedded subject (B1) has no process outside its op loop, so there is nothing to observe idle; its
        # fixed costs show in load_by_class (parent vs branches) instead.
        idle = {"verdict": "ok", "flushes": 0, "flush_by_syscall": {k: 0 for k in load["flush_by_syscall"]},
                "by_class": {}}
    else:
        idle = json.load(open(a["idle"]))
    ops, load_s, idle_s = int(a["ops"]), float(a["load-s"]), float(a["idle-s"])
    res = {"name": a["name"], "ops": ops, "ops_ok": int(a.get("ops-ok", ops)), "load_s": load_s, "idle_s": idle_s,
           "load_verdict": load["verdict"], "idle_verdict": idle["verdict"],
           "idle_control": "none: embedded, no process outside the op loop" if embedded else "attached, same pids"}
    if ops <= 0:
        res["verdict"] = "REFUSED: zero ops in the load window"
        return res
    keys = list(load["flush_by_syscall"]) + ["sync_file_range", "copy_file_range_calls", "ficlone"]
    res["load"] = {k: load["flush_by_syscall"].get(k, load.get(k, 0)) for k in keys}
    res["idle"] = {k: idle["flush_by_syscall"].get(k, idle.get(k, 0)) for k in keys}
    res["load"]["flushes"], res["idle"]["flushes"] = load["flushes"], idle["flushes"]
    scale = 0.0 if embedded else (load_s / idle_s if idle_s > 0 else None)
    res["background_scale"] = scale
    per = {}
    for k in keys + ["flushes"]:
        bg = res["idle"][k] * scale if scale is not None else None
        per[k] = round((res["load"][k] - bg) / ops, 4) if bg is not None else None
        per[k + "_raw"] = round(res["load"][k] / ops, 4)
    res["per_op"] = per
    # Exact when the idle control saw nothing: then nothing is scaled or estimated.
    # Embedded: there is no control to be zero, so "exact" is not claimed; read load_by_class instead.
    res["exact"] = None if embedded else res["idle"]["flushes"] == 0
    res["load_by_class"] = load["by_class"]
    res["idle_by_class"] = idle["by_class"]
    if a.get("deferred"):
        d = json.load(open(a["deferred"]))
        res["deferred"] = {"flushes": d["flushes"], "verdict": d["verdict"], "by_class": d["by_class"],
                           "per_op": round(d["flushes"] / ops, 4)}
    bad = [v for v in (load["verdict"], idle["verdict"]) if v != "ok"]
    if a.get("deferred") and res["deferred"]["verdict"] != "ok":
        bad.append(res["deferred"]["verdict"])
    if res["ops_ok"] != ops:
        bad.append(f"{ops - res['ops_ok']} of {ops} ops failed")
    res["verdict"] = ("NOT CLEAN: " + " | ".join(bad)) if bad else "ok"
    return res


def table(d):
    cols = ["name", "ops", "ops_ok", "flushes/op", "raw/op", "exact", "idle_flushes", "idle_s", "load_s",
            "fsync/op", "fdatasync/op", "sync_file_range/op", "copy_file_range/op", "ficlone/op", "deferred/op",
            "verdict"]
    print("\t".join(["path"] + cols))
    for root, _, files in sorted(os.walk(d)):
        for fn in sorted(files):
            if fn != "cell.json":
                continue
            c = json.load(open(os.path.join(root, fn)))
            p = c.get("per_op", {})
            row = [c["name"], c["ops"], c["ops_ok"], p.get("flushes"), p.get("flushes_raw"), c.get("exact"),
                   c.get("idle", {}).get("flushes"), c["idle_s"], c["load_s"], p.get("fsync"), p.get("fdatasync"),
                   p.get("sync_file_range"), p.get("copy_file_range_calls"), p.get("ficlone"),
                   c.get("deferred", {}).get("per_op", ""), c["verdict"]]
            print("\t".join([os.path.relpath(root, d)] + [str(x) for x in row]))


def kv(argv):
    a, i = {}, 0
    while i < len(argv):
        if not argv[i].startswith("--") or i + 1 >= len(argv):
            sys.exit(f"stracecount: bad argument {argv[i]}")
        k = argv[i][2:]
        if k == "extra":
            a.setdefault("extra", []).append(argv[i + 1])
        else:
            a[k] = argv[i + 1]
        i += 2
    return a


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    cmd = sys.argv[1]
    if cmd == "count" and len(sys.argv) >= 3:
        a = kv(sys.argv[3:])
        r = count(sys.argv[2], a.get("extra", []), a.get("root"), a.get("window"))
        print(json.dumps(r, indent=1))
        sys.exit(0 if not r["verdict"].startswith("REFUSED") else 3)
    if cmd == "cell":
        r = cell(kv(sys.argv[2:]))
        print(json.dumps(r, indent=1))
        sys.exit(0 if not r["verdict"].startswith("REFUSED") else 3)
    if cmd == "table" and len(sys.argv) == 3:
        table(sys.argv[2])
        return
    sys.exit(__doc__)


if __name__ == "__main__":
    main()
