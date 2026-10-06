#!/usr/bin/env python3
"""stracecount.py -- exact flush counts from `strace -f -C -y` output (lane fastest-linux-comp).

  stracecount.py count TRACE [--extra FILE]... [--root DIR] [--window W] [--clients BACKENDS.TSV]   -> JSON
                      (--extra: strace's stderr file; --window: trace.sh's OUT.window, whose proven-attach line is
                      what licenses reading a table-less, call-less attach window as zero; its OUT.pids roster and
                      the window's clone/fork lines map every flushing thread to a process and a role; --clients:
                      bbload's backends.tsv, whose PG backend pids are the role "client")
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
  - a file opened (open, openat, openat2), or F_SETFL'd, with O_SYNC/O_DSYNC (each write is then a flush; writes
    are not traced) -- in the window, and, for an attach window, an fd already open with either flag when the attach
    completed (trace.sh's fdsync_scan of /proc/<pid>/fdinfo, OUT.fdsync; an attach window without that scan of its
    main pid is REFUSED);
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
          "openat", "openat2", "fcntl", "pwritev2", "io_submit", "io_uring_setup", "io_uring_enter",
          "io_uring_register")  # plus open and creat on x86_64 (trace.sh); creat takes no flags
OPENS = ("open", "openat", "openat2")
# Quoted strings (C-escaped) and -y's <path> annotations are removed before a flag is looked for, so a path that
# holds ")" or "O_DSYNC" can neither hide nor fake one (review finding 14: the parse stopped at the first ")").
QUOTED = re.compile(r'"(?:[^"\\]|\\.)*"(?:\.\.\.)?')
ANNOT = re.compile(r"<[^<>]*>")


def sync_flag(rest):
    """True if the call's arguments (strings and fd paths removed) carry O_SYNC or O_DSYNC."""
    args = ANNOT.sub("", QUOTED.sub('""', rest))
    return re.search(r"\bO_D?SYNC\b", args) is not None
# strace -f -o FILE prefixes every line with the pid ("%-5d "); stderr output uses "[pid N] "; accept both, and none;
# then -ttt's CLOCK_REALTIME stamp (seconds.microseconds), which a split window (--part) needs on every call line.
START = re.compile(r"^(?:(?:\[pid\s+)?(\d+)\]?\s+)?(?:(\d+\.\d+)\s+)?([a-z_0-9]+)\((.*)$")
RESUMED = re.compile(r"^(?:(?:\[pid\s+)?(\d+)\]?\s+)?(?:(\d+\.\d+)\s+)?<\.\.\. ([a-z_0-9]+) resumed>(.*)$")
SUMROW = re.compile(r"^\s*([\d.]+)\s+([\d.]+)\s+(\d+)\s+(\d+)\s+(?:(\d+)\s+)?([a-z_0-9]+)\s*$")
RET = re.compile(r"\)\s+=\s+(-?\d+|\?)")
FDPATH = re.compile(r"^\s*-?\d+<([^>]*)>")
# A pid that exited (or was a zombie) between the enumeration and the seize cannot be attached; trace.sh's TracerPid
# check proved every listed pid still alive was traced, so this line is benign -- for any pid but the server's own
# (MAIN), whose loss is never benign (review finding 5).
BENIGN = re.compile(r"attach: ptrace\(PTRACE_SEIZE, (\d+)\): (?:No such process|Operation not permitted)")
# strace's entering/exiting state for a task disagreed with the kernel's PTRACE_GET_SYSCALL_INFO (strace 6.8 src/
# syscall.c strace_get_syscall_info, "TODO: handle this" -- unhandled in master too). It appeared 10 times in 1,944
# windows of runs 37225919842..37244177784, every time in a PG deferred window, i.e. a SECOND attach right after a
# load window, to a process busy in a syscall. Its mechanism is not established (the second review showed the
# "one misread stop, then clean" reading contradicts strace's own state machine), so ANY such message REFUSES the
# window, in attach and launch windows alike (second review, findings 2 and 6). The deferred CHECKPOINT now runs
# inside the load window's own attach (run_system.sh), so no window attaches to a server it just loaded.
DESYNC = re.compile(r"pid (\d+): (entering|exiting), ptrace_syscall_info\.op == (\d+)")


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


SPAWN = ("clone", "clone3", "fork", "vfork")
# Roles whose flushes are the op's own work or work the op waits on (an allowlist): the server's main process, the
# load generator's own backends (bbload backends.tsv), and the PG processes that write and flush on a backend's
# behalf. Every other role -- autovacuum workers, other backends, processes born in the window that are not the load
# generator's, unmapped thread ids -- is BACKGROUND OR UNKNOWN, and any flush from one clears background_free.
FOREGROUND = ("main", "client", "aux:checkpointer", "aux:walwriter", "aux:background writer", "aux:io worker",
              "launched")
PG_AUX = ("checkpointer", "background writer", "walwriter", "walsummarizer", "autovacuum launcher",
          "autovacuum worker", "logical replication launcher", "io worker", "startup", "archiver")


def pg_role(cmd):
    if cmd.startswith("postgres: "):
        rest = cmd[len("postgres: "):].strip()
        for a in PG_AUX:
            if rest.startswith(a):
                return "aux:" + a
        return "backend (not the load generator's)"
    return None


def attribute(by_tid, spawned, window, main, clients, attached):
    """Map each flushing thread id to its process (the attach roster OUT.pids, then the window's clone/fork lines:
    CLONE_THREAD keeps the creator's process, anything else starts a new one) and each process to a role."""
    roster_path = window[: -len(".window")] + ".pids" if window and window.endswith(".window") else None
    tgid, cmd = {}, {}
    launched = not attached  # strace_run: one command traced from exec; an attach window must have its roster
    have_roster = roster_path is not None and os.path.exists(roster_path)
    if not launched and have_roster:
        for ln in open(roster_path, errors="replace"):
            f = ln.rstrip("\n").split(" ", 2)
            if len(f) >= 2:
                tgid[f[0]] = f[1]
                cmd[f[1]] = f[2] if len(f) > 2 else ""
    born, thread_parent = set(), {}
    for creator, new, thread in spawned:
        if thread:
            thread_parent[new] = creator
        else:
            tgid[new] = new
            born.add(new)

    def proc_of(t):  # follow thread-creation links (in any file order) to a known process
        seen = set()
        while t in thread_parent and t not in seen and t not in tgid:
            seen.add(t)
            t = thread_parent[t]
        return tgid.get(t)

    def role(p):
        if launched:
            return "launched"  # strace_run: one command traced from exec, no roster; its processes are the subject
        if p == main:
            return "main"
        if p in clients:
            return "client"
        if p in born:
            return "other (born in the window, not the load generator's)"
        if p in cmd:
            return pg_role(cmd[p]) or ("process at attach: " + cmd[p][:60])
        return "unmapped"

    by_role, by_proc, unmapped = {}, {}, 0
    for t, ks in by_tid.items():
        p = proc_of(t)
        n = sum(ks.values())
        if launched:
            r, p = "launched", p or t
        elif p is None:
            r, p = "unmapped", t
            unmapped += n
        else:
            r = role(p)
        by_role[r] = by_role.get(r, 0) + n
        e = by_proc.setdefault(p, {"role": r, "flushes": 0})
        e["flushes"] += n
    bg = sum(n for r, n in by_role.items() if r not in FOREGROUND)
    return {"by_role": by_role, "foreground_flushes": sum(by_role.values()) - bg, "background_flushes": bg,
            "unmapped_flushes": unmapped, "roster_found": have_roster,
            "attribution": "launch" if launched else "roster+lineage",
            "clients_known": len(clients),
            "top_processes": sorted(([p, e["role"], e["flushes"]] for p, e in by_proc.items()), key=lambda x: -x[2])[:20]}


def count(trace, extras, root, window=None, clients=frozenset(), part=None):
    """Count one strace window. part=None counts the whole trace; part="pre"/"post" counts only the calls that
    STARTED before/after the window's tsplit stamp (strace_mark), so one attach can hold a load window and the
    CHECKPOINT after it (second review, finding 2). The table-vs-lines check always covers the whole trace."""
    with open(trace, errors="replace") as f:
        text = f.read()
    summary = parse_summary(text)
    problems, blind = [], []
    stray = []  # strace's own stderr, minus a summary table: attach/ptrace errors and warnings land here
    win = open(window).read() if window and os.path.exists(window) else ""
    attached = "attached_after_polls=" in win
    launched = re.search(r"^cmd=", win, re.M) is not None
    if attached == launched:
        problems.append("window record missing or unrecognized (neither a proven attach nor a launch)")
    mm = re.search(r"^main=(\d+) ", win, re.M)
    main = mm.group(1) if mm else None
    detach = {k: v for k, v in re.findall(r"\b(strace_alive_at_detach|main_alive_at_detach)=(\d)", win)}
    rc = re.search(r"\bstrace_rc=(\d+)", win)
    # strace exits 130 when the SIGINT detach ends an attach (616 of 616 attach windows, runs 37242277040 and
    # 37244177784); a launch window's strace exits with its command's status, which must be 0 (second review, 7).
    want_rc = "130" if attached else "0"
    if rc is None:
        problems.append("window has no strace_rc")
    elif rc.group(1) != want_rc:
        problems.append(f"strace_rc={rc.group(1)}, want {want_rc} for {'an attach' if attached else 'a launch'} window")
    tsplit = None
    if part is not None:
        sm = re.search(r"\btsplit=(\d+\.\d+)", win)
        if part not in ("pre", "post"):
            problems.append(f"unknown part {part}")
        elif sm is None:
            problems.append("a split count was asked for but the window has no tsplit stamp")
        else:
            tsplit = float(sm.group(1))

    def benign(ln):
        b = BENIGN.search(ln)
        return attached and bool(b) and main is not None and b.group(1) != main

    desync = {}  # tid -> [(entering|exiting, op)] from strace's state-mismatch messages (see DESYNC)
    for e in extras:
        if not os.path.exists(e):
            continue
        with open(e, errors="replace") as f:
            etext = f.read()
        if summary is None:
            summary = parse_summary(etext)
        for ln in etext.splitlines():
            d = DESYNC.search(ln)
            if d:
                desync.setdefault(d.group(1), []).append((d.group(2), d.group(3)))
            elif ln.strip() and not ln.startswith(("% time", "------")) and not SUMROW.match(ln) and not benign(ln):
                stray.append(ln)
    lines, done_by_name = {}, {}
    pending = {}  # tid -> (name, rest, ts): started, not (yet) returned
    calls = []  # (tid, name, args_and_rest, ts, completed)
    orphan_resumed, unstamped = 0, 0
    last_ts, back_steps, max_back = None, 0, 0.0  # -ttt stamps are written in event order: they must not run back
    first_ts = None
    for line in text.splitlines():
        sm2 = RESUMED.match(line) or START.match(line)  # call lines only: the -c table's rows hold decimals too
        if sm2 and sm2.group(2):
            tsv = float(sm2.group(2))
            first_ts = tsv if first_ts is None else min(first_ts, tsv)
            if last_ts is not None and tsv < last_ts:
                back_steps += 1
                max_back = max(max_back, last_ts - tsv)
            last_ts = tsv if last_ts is None else max(last_ts, tsv)
        m = RESUMED.match(line)
        if m:
            pid, _, name, rest = m.groups()
            if pid in pending and pending[pid][0] == name:
                pn, pr, pts = pending.pop(pid)
                calls.append((pid, name, pr + rest, pts, True))
                done_by_name[name] = done_by_name.get(name, 0) + 1
            else:
                orphan_resumed += 1  # entered before the attach; strace saw only its return
            continue
        m = START.match(line)
        if not m:
            continue
        pid, ts, name, rest = m.groups()
        if ts is None:
            unstamped += 1
        tsf = float(ts) if ts else None
        lines[name] = lines.get(name, 0) + 1
        if rest.endswith("<unfinished ...>"):
            pending[pid] = (name, rest[: -len("<unfinished ...>")], tsf)
        elif rest.endswith("<detached ...>"):
            # In flight when the detach came: it never returns in this trace and strace's -c table does not count it
            # (run 37225130145, pg18-default x86_64 XFS pg18-m1-wal-c4: table 485 sync_file_range, lines 486, the
            # extra one "<detached ...>"). It is an unfinished call, never a completed one.
            pending[pid] = (name, rest[: -len("<detached ...>")], tsf)
        else:
            calls.append((pid, name, rest, tsf, True))
            done_by_name[name] = done_by_name.get(name, 0) + 1
    for pid, (name, rest, tsf) in pending.items():
        calls.append((pid, name, rest, tsf, False))
    if part is not None and unstamped:
        problems.append(f"{unstamped} call line(s) without a -ttt stamp: the split cannot place them")
    # A stamp that runs BACK by more than 1 ms is a CLOCK_REALTIME step (NTP, settimeofday): the t0/tsplit/t1 cuts read
    # that clock, so they cannot be trusted (fifth review, finding 2: none in 396 banked traces). Sub-millisecond
    # disorder is reported only.
    if max_back > 0.001:
        problems.append(f"the -ttt clock stepped back {max_back:.6f} s ({back_steps} step(s)): the window cuts cannot "
                        "be trusted")
    # strace allocates its -c counters at the first counted call, so a window in which no traced syscall happened
    # prints NO table. That is a true zero only when the attach was proven (trace.sh's TracerPid check wrote
    # attached_after_polls= into the window file) and strace said nothing on stderr; otherwise it stays refused.
    empty_window = summary is None and not lines and attached and not stray and not desync
    if empty_window:
        summary = {}
    out = {"trace": trace, "root": root, "part": part, "tsplit": tsplit, "summary_found": summary is not None,
           "summary": summary or {}, "lines": lines, "completed": done_by_name, "unfinished_at_end": len(pending),
           "orphan_resumed": orphan_resumed, "attached_proven": attached, "empty_window": empty_window,
           "strace_stderr": stray[:20]}

    # An attach window's counts start at its t0, the moment load_s and idle_s start: the calls between the seize and
    # t0 (the attach proof, the fd scan, the roster) are left out (fourth review, finding 9: F10d had 18-32 child
    # fsyncs there). A window whose calls are unstamped cannot be cut and is counted from the seize, as before.
    t0m = re.search(r"\bt0=(\d+\.\d+)", win)
    t0 = float(t0m.group(1)) if (attached and t0m) else None
    # ...and they end at t1, the detach request, where load_s/idle_s end (fifth review, finding 6: calls between the
    # detach request and strace's exit were counted with no time attached).
    t1m = re.search(r"^t1=(\d+\.\d+)", win, re.M)
    t1 = float(t1m.group(1)) if (attached and t1m) else None
    # An attach window's stamps are clock pairs (trace.sh clock_pair): between consecutive ones, tseize -> t0
    # [-> tsplit] -> t1 -> tend, the CLOCK_REALTIME delta must equal the CLOCK_MONOTONIC one within 1 ms, or the
    # realtime clock stepped somewhere in the trace's life and the cuts cannot be trusted; and every call stamp must lie
    # inside [tseize, tend]. Comparing call stamps with each other (above) misses a step before the first call or after
    # the last (fifth-review re-review, finding 2). A missing pair refuses the window.
    clock = {}
    for name, mono, val in re.findall(r"(?:^|\s)(tseize|t0|tsplit|t1|tend)(_mono)?=(\d+\.\d+)", win, re.M):
        clock.setdefault(name + mono, float(val))
    out_clock = None
    if attached:
        chain = ["tseize", "t0"] + (["tsplit"] if "tsplit" in clock else []) + ["t1", "tend"]
        absent = [k for n in chain for k in (n, n + "_mono") if k not in clock]
        if absent:
            problems.append(f"attach window without its clock pair(s) {absent}: a clock step could not be seen")
        else:
            steps = []
            for a, b in zip(chain, chain[1:]):
                dr, dm = clock[b] - clock[a], clock[b + "_mono"] - clock[a + "_mono"]
                steps.append([a, b, round(dr - dm, 6)])
                if dm < 0 or abs(dr - dm) > 0.001:
                    problems.append(f"CLOCK_REALTIME stepped {dr - dm:+.6f} s between {a} and {b} (realtime {dr:.6f} s "
                                    f"against monotonic {dm:.6f} s): the window cuts cannot be trusted")
            if first_ts is not None and (first_ts < clock["tseize"] - 0.001 or last_ts > clock["tend"] + 0.001):
                problems.append(f"call stamps {first_ts:.6f}..{last_ts:.6f} outside the window's life "
                                f"[{clock['tseize']:.6f}, {clock['tend']:.6f}]: the clock or the stamps are wrong")
            out_clock = steps
    before_t0, after_t1 = 0, 0

    def selected(ts):
        nonlocal before_t0, after_t1
        if t0 is not None and ts is not None and ts < t0:
            before_t0 += 1
            return False
        if t1 is not None and ts is not None and ts > t1:
            after_t1 += 1
            return False
        if tsplit is None or ts is None:
            return part is None
        return ts < tsplit if part == "pre" else ts >= tsplit

    BLIND_NAMES = OPENS + ("fcntl", "pwritev2", "io_submit", "io_uring_setup", "io_uring_enter", "io_uring_register")

    flush = {k: 0 for k in FLUSH}
    flush["msync_sync"] = 0
    other = {"sync_file_range": 0, "msync_nosync": 0, "copy_file_range_calls": 0, "copy_file_range_bytes": 0,
             "copy_file_range_failed": 0, "ficlone": 0, "ficlone_failed": 0, "osync_opens": 0, "osync_fcntl": 0,
             "rwf_sync_writes": 0, "io_uring": 0, "io_submit": 0}
    by_class, by_path, by_tid = {}, {}, {}
    spawned = []  # (creator tid, new tid, is a thread) from clone/clone3/fork/vfork lines -- of the WHOLE trace
    # Calls still unfinished at the detach are not counted as flushes (strace's -c table counts a call when it
    # returns), but their arguments are still searched for blind spots: an O_DSYNC open in flight is still one.
    for tid, name, rest, ts, done in calls:
        rm = RET.search(rest)
        ret = rm.group(1) if rm else "?"
        if name in SPAWN:
            if done and ret.isdigit() and int(ret) > 0:
                spawned.append((tid, ret, name.startswith("clone") and "CLONE_THREAD" in rest))
            continue
        # Blind spots are searched in EVERY call of the trace, whatever its part or time: an fd opened O_DSYNC before
        # t0 or before tsplit still makes later writes flushes.
        if name not in BLIND_NAMES and not selected(ts):
            continue
        if not done and (name in FLUSH or name in ("msync", "sync_file_range", "copy_file_range", "ioctl")):
            continue
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
            by_tid.setdefault(tid, {}).setdefault(key, 0)
            by_tid[tid][key] += 1
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
            # Only a call that returned bytes is clone evidence (second review, finding 8); a failed one is counted
            # apart. A silent in-kernel byte copy is invisible here: the filefrag proof is what tells clone from copy.
            if ret.isdigit():
                other["copy_file_range_calls"] += 1
                other["copy_file_range_bytes"] += int(ret)
            else:
                other["copy_file_range_failed"] += 1
        elif name == "ioctl":
            if "FICLONE" in rest or "BTRFS_IOC_CLONE" in rest:
                if ret == "0":
                    other["ficlone"] += 1
                else:
                    other["ficlone_failed"] += 1
        elif name in OPENS:  # open/openat/openat2 (review finding 10: open and openat2 were not inspected)
            if sync_flag(rest):
                other["osync_opens"] += 1
        elif name == "fcntl":
            if "F_SETFL" in rest and sync_flag(rest):
                other["osync_fcntl"] += 1
        elif name == "pwritev2":
            if re.search(r"RWF_D?SYNC", rest):
                other["rwf_sync_writes"] += 1
        elif name.startswith("io_uring"):
            other["io_uring"] += 1
        elif name == "io_submit":
            other["io_submit"] += 1
    out["flush_by_syscall"] = flush
    out["t0"], out["calls_before_t0"], out["t1"], out["calls_after_t1"] = t0, before_t0, t1, after_t1
    out["clock_back_steps"], out["clock_max_back_s"] = back_steps, round(max_back, 6)
    out["clock_pairs"] = out_clock  # [from, to, realtime minus monotonic delta in s] per consecutive pair (attach)
    # A launch window's command stderr (strace_run's OUT.cmd.err): recorded, never a verdict -- a command may warn and
    # still succeed, and its exit status is checked through strace_rc (fourth review, finding 6: it was looked at by
    # nothing).
    ce = window[: -len(".window")] + ".cmd.err" if launched and window and window.endswith(".window") else None
    out["command_stderr"] = (open(ce, errors="replace").read().splitlines()[:5]
                             if ce and os.path.exists(ce) else None)
    out["flushes"] = sum(flush.values())
    out.update(other)
    out["by_class"] = by_class
    out["top_paths"] = sorted(by_path.items(), key=lambda kv: -kv[1])[:15]
    out.update(attribute(by_tid, spawned, window, main, clients, attached))
    out["main_pid"], out["detach"] = main, detach
    # The pre-attach fd scan (trace.sh fdsync_scan): fds already open with O_SYNC/O_DSYNC when the attach completed.
    fds_path = window[: -len(".window")] + ".fdsync" if window and window.endswith(".window") else None
    scanned, pre_sync = {}, []
    if fds_path and os.path.exists(fds_path):
        for ln in open(fds_path, errors="replace"):
            f = ln.split()
            if len(f) >= 3 and f[0] == "scanned":
                scanned[f[1]] = scanned.get(f[1], 0) + int(f[2])
            elif len(f) >= 4 and f[0] == "hit":
                target = " ".join(f[4:])
                # A socket, pipe or anon inode with O_DSYNC flushes no file; an unreadable target counts (unknown).
                if target.startswith("/") or not target:
                    pre_sync.append(f"pid {f[1]} fd {f[2]} flags {f[3]} {target or '(target unreadable)'}")
    out["fdsync_scanned"], out["osync_fds_at_attach"] = scanned, pre_sync
    roster_pids = set()
    rp = window[: -len(".window")] + ".pids" if window and window.endswith(".window") else None
    if rp and os.path.exists(rp):
        for ln in open(rp, errors="replace"):
            f = ln.split(" ", 2)
            if len(f) >= 2:
                roster_pids.add(f[1])
    # The roster is written right AFTER the scan, so a process in it was alive when the scan ran: it must have a
    # "scanned" line with at least one fd (second review, finding 1: 11 live PG processes had none, windows ok).
    out["fdsync_unscanned"] = sorted(p for p in roster_pids if not scanned.get(p, 0))
    out["desync"] = desync
    for t, msgs in desync.items():
        problems.append(f"strace lost the syscall state of task {t} ({msgs[:3]}): its calls cannot be counted")
    if attached:
        if not re.search(r"\bfrozen=1\b", win):
            problems.append("the attach did not freeze the main pid (frozen=1): a child forked between the enumeration"
                            " and the seize could have escaped the trace")
        if not out["roster_found"]:
            problems.append("attach window has no pid roster (OUT.pids): its flushes cannot be attributed")
        if not scanned.get(main or "", 0):
            problems.append(f"no pre-attach O_SYNC/O_DSYNC fd scan of the main pid {main}")
        if out["fdsync_unscanned"]:
            problems.append(f"no pre-attach O_SYNC/O_DSYNC fd scan of roster process(es) {out['fdsync_unscanned'][:5]}")
        if pre_sync:
            blind.append(f"O_SYNC/O_DSYNC fd open before the attach x{len(pre_sync)} ({pre_sync[0][:120]})")
        # An attach window is a count of a LIVE server: its main pid must be named, and both the strace and the server
        # must still have been running when the detach was requested (else every tracee died and an empty window
        # would read as a clean zero -- review finding 5).
        if main is None:
            problems.append("attach window names no main pid")
        if set(detach) != {"strace_alive_at_detach", "main_alive_at_detach"}:
            problems.append("attach window has no detach record")
        else:
            if detach["strace_alive_at_detach"] != "1":
                problems.append("strace had exited before the detach (every tracee gone)")
            if detach["main_alive_at_detach"] != "1":
                problems.append(f"the server (main pid {main}) was gone at the detach")
    if summary is None:
        problems.append("no -c summary table found")
    if stray:
        problems.append(f"strace stderr: {stray[0][:200]}")
    if not lines and summary:
        problems.append("summary present but zero call lines (was -C used?)")
    if summary is not None:
        # The -c table counts a call when it RETURNS: per syscall it must equal the completed lines exactly, and the
        # lines may exceed it only by that syscall's own unfinished/detached calls (second review, finding 5: the
        # tolerance was unsigned and shared across names).
        unfinished_by_name = {}
        for _, (n, _, _) in pending.items():
            unfinished_by_name[n] = unfinished_by_name.get(n, 0) + 1
        for name in set(summary) | set(lines):
            s = summary.get(name, {}).get("calls", 0)
            c = done_by_name.get(name, 0)
            if s != c:
                problems.append(f"{name}: summary {s} calls vs {c} completed lines "
                                f"({lines.get(name, 0)} lines, {unfinished_by_name.get(name, 0)} unfinished)")
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
    # A window whose count is missing or unreadable is not a zero: the cell refuses and says which.
    for k in ("load", "idle", "deferred"):
        p = a.get(k)
        if p is None or (k == "idle" and p == "none"):
            continue
        try:
            json.load(open(p))
        except (OSError, ValueError) as e:
            return {"name": a.get("name"), "ops": a.get("ops"), "ops_ok": a.get("ops-ok"), "idle_s": a.get("idle-s"),
                    "load_s": a.get("load-s"), "verdict": f"REFUSED: {k} window count unreadable: {e}"}
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
    # Attribution by process role (count(): attach roster + clone lineage + the load generator's backends).
    per["foreground"] = round(load.get("foreground_flushes", 0) / ops, 4)
    per["background"] = round(load.get("background_flushes", 0) / ops, 4)
    res["per_op"] = per
    res["load_by_role"] = load.get("by_role", {})
    res["idle_by_role"] = idle.get("by_role", {})
    res["load_top_processes"] = load.get("top_processes", [])
    notes = []
    # The old "exact" (idle control saw zero) was wrong (review finding 2): background processes -- PG autovacuum
    # workers, bursty and more frequent as databases accumulate -- flushed inside load windows it called exact.
    # background_free claims only what was observed: the idle control saw no flush, AND no flush in the load window
    # came from a background or unmapped process. It cannot see background work INSIDE the server's own process
    # (one-process servers: Dolt, Doltgres), which load_by_class shows by path instead.
    if embedded:
        res["background_free"] = None
    else:
        res["background_free"] = (res["idle"]["flushes"] == 0 and load.get("background_flushes", 0) == 0
                                  and load.get("unmapped_flushes", 0) == 0)
        if set(res["load_by_role"]) <= {"main"} and res["load"]["flushes"]:
            notes.append("one process: background work inside the server process is not separable by process "
                         "(see load_by_class)")
    if per["flushes"] is not None and per["flushes"] < 0:
        # Review finding 9: an idle control scaled to the load window can exceed the window's own count.
        notes.append(f"idle-subtracted estimate below zero ({per['flushes']}): the scaled idle control "
                     f"({res['idle']['flushes']} x {scale:.4f}) exceeds the window's {res['load']['flushes']} flushes;"
                     " read flushes_raw")
    if a.get("template-waits"):
        # PG: creates that waited on the template in CountOtherDBBackends during the window (run_system.sh reads them
        # from the server log). Recorded in the cell and marked in the tables, never dropped (second review, 9).
        tw = dict(re.findall(r"(\w+)=(\d+)", open(a["template-waits"]).read()))
        res["template_waits"] = {k: int(v) for k, v in tw.items()}
        if any(int(v) for v in tw.values()):
            notes.append(f"creates waited on the template (CountOtherDBBackends): {res['template_waits']}")
    res["notes"] = notes
    res["load_by_class"] = load["by_class"]
    res["idle_by_class"] = idle["by_class"]
    if a.get("deferred"):
        d = json.load(open(a["deferred"]))
        res["deferred"] = {"flushes": d["flushes"], "verdict": d["verdict"], "by_class": d["by_class"],
                           "by_role": d.get("by_role", {}), "per_op": round(d["flushes"] / ops, 4)}
    bad = [v for v in (load["verdict"], idle["verdict"]) if v != "ok"]
    if a.get("deferred") and res["deferred"]["verdict"] != "ok":
        bad.append(res["deferred"]["verdict"])
    if res["ops_ok"] != ops:
        bad.append(f"{ops - res['ops_ok']} of {ops} ops failed")
    res["verdict"] = ("NOT CLEAN: " + " | ".join(bad)) if bad else "ok"
    return res


TABLE_COLS = ["name", "ops", "ops_ok", "flushes/op", "raw/op", "foreground/op", "background/op", "background_free",
              "idle_flushes", "idle_s", "load_s", "fsync/op", "fdatasync/op", "sync_file_range/op",
              "copy_file_range/op", "ficlone/op", "deferred/op", "verdict", "notes"]


def table_row(c):
    p = c.get("per_op", {})
    return [c.get("name"), c.get("ops"), c.get("ops_ok"), p.get("flushes"), p.get("flushes_raw"), p.get("foreground"),
            p.get("background"), c.get("background_free"), c.get("idle", {}).get("flushes"), c.get("idle_s"),
            c.get("load_s"), p.get("fsync"), p.get("fdatasync"), p.get("sync_file_range"),
            p.get("copy_file_range_calls"), p.get("ficlone"), c.get("deferred", {}).get("per_op", ""),
            c.get("verdict"), " | ".join(c.get("notes", []))]


def table(d):
    print("\t".join(["path"] + TABLE_COLS))
    for root, _, files in sorted(os.walk(d)):
        for fn in sorted(files):
            if fn != "cell.json":
                continue
            c = json.load(open(os.path.join(root, fn)))
            print("\t".join([os.path.relpath(root, d)] + [str(x) for x in table_row(c)]))


def clients_of(path):
    """The load generator's PG backend pids from bbload's backends.tsv (MySQL-protocol connection ids are not
    process ids and are not used)."""
    if not path:
        return frozenset()
    with open(path) as f:
        head = f.readline().rstrip("\n").split("\t")
        if head[-1] != "backend_pid":
            return frozenset()
        return frozenset(ln.rstrip("\n").split("\t")[-1] for ln in f if ln.strip())


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
        r = count(sys.argv[2], a.get("extra", []), a.get("root"), a.get("window"), clients_of(a.get("clients")),
                  a.get("part"))
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
