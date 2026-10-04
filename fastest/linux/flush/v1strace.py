#!/usr/bin/env python3
"""v1strace.py -- the V1 flush counter's strace instrument (Linux x86_64 / aarch64).

LD_PRELOAD (syncshim.so) cannot see raw syscalls, and on Linux every Go binary (Dolt, Doltgres) makes nothing but
raw syscalls. This instrument traces the kernel boundary instead, so it sees every call whatever made it, and counts
the same kinds as the shim (KINDS below = V1_KIND_NAMES in syncshim.h), per process and per marked operation.

  v1strace.py run OUT [--seccomp-bpf] -- cmd [args...]
        trace cmd and its whole process tree; write OUT.strace (raw strace output) and OUT.json; exit with the rc
  v1strace.py parse TRACE [--root PID] [--allow-async] [--mutant NAME]
        parse a trace file already written; JSON to stdout; exit with the rc
  v1strace.py cross RUN OUT [--seccomp-bpf] -- cmd [args...]
        ONE run under both instruments: strace ... -- v1run RUN cmd. Writes OUT.strace/.json (strace), OUT.shim.json
        (v1ctl report) and OUT.cross.json: every (pid, kind) where the counts differ. This is how a shim miss is
        REPORTED: the shim cannot see a raw syscall, so the same run under strace says what it missed.
  v1strace.py consts
        this parser's constant table (JSON); the fire-check compares it with `v1ctl consts` from the C headers

Marks: the client's v1_set_mark (syncshim.h) issues lseek(V1_MARK_FD, mark, SEEK_SET), which fails EBADF. strace
writes one line per event in the order it handles them, and a traced process is stopped until strace has handled
its syscall, so a flush caused by a request sent after a mark appears after that mark in the file: attribution
follows file order, no clocks. Only processes inside the traced tree can set marks.

rc: 0 ok | 2 strace missing, unknown arch, or the trace file missing | 3 nothing traced (empty trace, root never
    seen) | 5 a line in the traced set this parser cannot read, or a thread whose creation was never seen: a count
    built on an unread line is not a count | 8 async I/O submissions seen (io_uring_setup, io_uring_enter,
    io_submit): they can carry flushes no instrument here can see (--allow-async to count the rest)
    cross only: 9 the shim missed calls strace saw | 10 the shim counted calls strace did not see

BLIND SPOTS: io_uring and Linux AIO submissions (refused, rc 8). fds received over SCM_RIGHTS, and fds inherited by
the root from outside the trace, are not known to be O_SYNC/O_DSYNC. A call is counted at ENTRY, so a process
SIGKILLed inside a flush counts it (the shim, which records on return, does not). The tracer slows every syscall:
counts only, never timing.
"""
import json
import os
import platform
import re
import shutil
import subprocess
import sys

KINDS = ["fsync", "fdatasync", "sfr_write_wait", "sfr_write", "sfr_wait", "syncfs", "sync", "msync_SYNC",
         "msync_other", "osync_write", "odsync_write", "FICLONE", "FICLONERANGE", "copy_file_range"]
MASK64 = (1 << 64) - 1
IDLE = 1 << 63

# Linux constants, identical on x86_64 and aarch64 for every name used here (checked against the C headers of the
# build by the fire-check: `v1ctl consts`).
C = {
    "O_SYNC": 0o4010000, "O_DSYNC": 0o10000, "O_CLOEXEC": 0o2000000, "FD_CLOEXEC": 1, "F_DUPFD": 0,
    "F_DUPFD_CLOEXEC": 1030, "F_SETFD": 2, "F_SETFL": 4, "F_GETFL": 3, "MS_SYNC": 4, "MS_ASYNC": 1,
    "MS_INVALIDATE": 2, "SYNC_FILE_RANGE_WAIT_BEFORE": 1, "SYNC_FILE_RANGE_WRITE": 2,
    "SYNC_FILE_RANGE_WAIT_AFTER": 4, "RWF_DSYNC": 2, "RWF_SYNC": 4, "FICLONE": 0x40049409,
    "FICLONERANGE": 0x4020940d, "CLONE_VM": 0x100, "CLONE_FILES": 0x400, "CLONE_VFORK": 0x4000,
    "CLONE_THREAD": 0x10000, "CLOSE_RANGE_UNSHARE": 2, "CLOSE_RANGE_CLOEXEC": 4, "V1_MARK_FD": -22065,
    "AT_FDCWD": -100,
}
# Symbolic names accepted where strace prints a name despite -X raw. An unknown name is a parse error, never 0.
SYMBOLS = dict(C)
SYMBOLS.update({"NULL": 0, "O_RDONLY": 0, "O_WRONLY": 1, "O_RDWR": 2, "O_CREAT": 0o100, "O_EXCL": 0o200,
                "O_NOCTTY": 0o400, "O_TRUNC": 0o1000, "O_APPEND": 0o2000, "O_NONBLOCK": 0o4000,
                "SEEK_SET": 0, "SEEK_CUR": 1, "SEEK_END": 2, "SIGCHLD": 17})
RESTART = {"ERESTARTSYS", "ERESTARTNOINTR", "ERESTARTNOHAND", "ERESTART_RESTARTBLOCK", "512", "513", "514", "516"}

COUNTED = ["fsync", "fdatasync", "sync_file_range", "syncfs", "sync", "msync", "write", "pwrite64", "writev",
           "pwritev", "pwritev2", "ioctl", "copy_file_range"]
FDLIFE = ["open", "openat", "openat2", "creat", "open_by_handle_at", "fcntl", "dup", "dup2", "dup3", "close",
          "close_range"]
PROC = ["clone", "clone3", "fork", "vfork", "execve", "execveat", "exit", "exit_group"]
ASYNC = ["io_uring_setup", "io_uring_enter", "io_submit"]
NOT_ON_AARCH64 = {"open", "creat", "dup2", "fork", "vfork"}


def trace_set():
    m = platform.machine()
    if m == "x86_64":
        drop = set()
    elif m in ("aarch64", "arm64"):
        drop = NOT_ON_AARCH64
    else:
        return None
    return [n for n in COUNTED + FDLIFE + ["lseek"] + PROC + ASYNC if n not in drop]


class ParseError(Exception):
    pass


def split_args(s):
    """Top-level comma split of an strace argument list (nesting, strings and /* comments */ respected)."""
    out, cur, depth, i, n = [], [], 0, 0, len(s)
    while i < n:
        ch = s[i]
        if ch == '"':
            j = i + 1
            while j < n and s[j] != '"':
                j += 2 if s[j] == "\\" else 1
            cur.append(s[i:j + 1])
            i = j + 1
            continue
        if s.startswith("/*", i):
            j = s.find("*/", i + 2)
            j = n if j < 0 else j + 2
            cur.append(s[i:j])
            i = j
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
        i += 1
    tail = "".join(cur).strip()
    if tail or out:
        out.append(tail)
    return out


NUM_RE = re.compile(r"^-?(0x[0-9a-fA-F]+|0[0-7]*|[1-9][0-9]*)$")


def parse_num(tok):
    t = re.sub(r"/\*.*?\*/", "", tok).strip()
    if not t:
        raise ParseError("empty number")
    if "|" in t:
        v = 0
        for part in t.split("|"):
            v |= parse_num(part)
        return v
    if t.startswith("~"):
        return (~parse_num(t[1:])) & 0xFFFFFFFF
    if NUM_RE.match(t):
        neg = t.startswith("-")
        body = t[1:] if neg else t
        if body.startswith("0x"):
            v = int(body, 16)
        elif len(body) > 1 and body.startswith("0"):
            v = int(body, 8)
        else:
            v = int(body)
        return -v if neg else v
    if t in SYMBOLS:
        return SYMBOLS[t]
    raise ParseError("unknown constant %r" % t)


def arg(args, i):
    if i >= len(args):
        raise ParseError("missing argument %d" % i)
    return parse_num(args[i])


def field(text, name):
    m = re.search(r"\b%s=([^,}\s]+)" % re.escape(name), text)
    if not m:
        raise ParseError("no %s= in %r" % (name, text[:120]))
    return parse_num(m.group(1))


def sync_kind_of_flags(fl):
    if fl & C["O_SYNC"] == C["O_SYNC"]:
        return "osync_write"
    if fl & C["O_DSYNC"]:
        return "odsync_write"
    return None


def stronger(a, b):
    if "osync_write" in (a, b):
        return "osync_write"
    if "odsync_write" in (a, b):
        return "odsync_write"
    return None


LINE_RE = re.compile(r"^(\d+)\s+(.*)$")
RESUMED_RE = re.compile(r"^<\.\.\. (\w+) resumed>(.*)$")
CALL_RE = re.compile(r"^(\w+)\((.*)$")
UNFINISHED = " <unfinished ...>"


class Ev:
    __slots__ = ("idx", "tid", "name", "entry_args", "full_args", "ret", "ret_tok", "exit_idx", "kind",
                 "skip_count")

    def __init__(self, idx, tid, name, entry_args):
        self.idx, self.tid, self.name, self.entry_args = idx, tid, name, entry_args
        self.full_args, self.ret, self.ret_tok, self.exit_idx = None, None, None, None
        self.kind, self.skip_count = None, False


def split_ret(body):
    """'ARGS) = RET rest' -> (ARGS, RET, first token after RET)."""
    k = body.rfind(") = ")
    if k < 0:
        raise ParseError("no ') = '")
    rest = body[k + 4:].split()
    if not rest:
        raise ParseError("no return value")
    tok = rest[1] if len(rest) > 1 else None
    return body[:k], rest[0], tok


def ret_value(r):
    if r == "?":
        return None
    return parse_num(r)


def parse(path, root=None, allow_async=False, mutant=None):
    res = {"instrument": "strace", "trace": path, "mutant": mutant, "unparsed": 0, "unparsed_samples": [],
           "unknown_tids": [], "async_io": {}, "lines": 0, "events": 0}

    def bad(idx, line, why):
        res["unparsed"] += 1
        if len(res["unparsed_samples"]) < 20:
            res["unparsed_samples"].append("%d: %s  [%s]" % (idx + 1, line[:200], why))

    if not os.path.exists(path):
        res.update(rc=2, verdict="REFUSED: trace file missing (strace did not start)")
        return res
    with open(path, errors="replace") as f:
        lines = f.read().splitlines()
    res["lines"] = len(lines)

    # Pass 1: pair every entry with its exit.
    events, exits_at, pending, specials = [], {}, {}, []
    for idx, line in enumerate(lines):
        m = LINE_RE.match(line)
        if not m:
            if line.strip():
                bad(idx, line, "no pid prefix")
            continue
        tid, body = int(m.group(1)), m.group(2)
        if body.startswith("+++") or body.startswith("---"):
            specials.append((idx, tid, body))
            continue
        rm = RESUMED_RE.match(body)
        if rm:
            ev = pending.pop(tid, None)
            if ev is None or ev.name != rm.group(1):
                bad(idx, line, "resumed without a matching unfinished call")
                continue
            try:
                args, r, tok = split_ret(ev.entry_args + rm.group(2))
                ev.full_args, ev.ret, ev.ret_tok, ev.exit_idx = args, r, tok, idx
            except ParseError as e:
                if "<unavailable>" in body or body.rstrip().endswith("= ?"):
                    ev.full_args, ev.ret, ev.exit_idx = ev.entry_args, "?", idx
                else:
                    bad(idx, line, str(e))
                    continue
            exits_at.setdefault(idx, []).append(ev)
            continue
        cm = CALL_RE.match(body)
        if not cm:
            bad(idx, line, "not a call")
            continue
        name, rest = cm.group(1), cm.group(2)
        if body.endswith(UNFINISHED):
            ev = Ev(idx, tid, name, rest[: -len(UNFINISHED)])
            if tid in pending:
                bad(idx, line, "second unfinished call on one thread")
            pending[tid] = ev
            events.append(ev)
            continue
        try:
            args, r, tok = split_ret(rest)
        except ParseError as e:
            bad(idx, line, str(e))
            continue
        ev = Ev(idx, tid, name, args)
        ev.full_args, ev.ret, ev.ret_tok, ev.exit_idx = args, r, tok, idx
        events.append(ev)
        exits_at.setdefault(idx, []).append(ev)
    res["events"] = len(events)
    if not events:
        res.update(rc=3, verdict="REFUSED: nothing traced (empty trace)")
        return res

    # Pass 2: walk entries and exits in file order.
    if root is None:
        root = events[0].tid
    res["root_pid"] = root
    tgid_of = {root: root}
    ppid_of = {root: 0}
    fdtab = {root: {}}            # tgid -> {fd: (sync kind or None, cloexec)}; CLONE_FILES shares the dict
    threads = {root: {root}}
    counts, fails, execs, marks = {}, {}, {}, {}
    restart_pending = {}
    mark = 0
    entries_at = {}
    for ev in events:
        entries_at.setdefault(ev.idx, []).append(ev)

    def proc_of(tid, idx, line):
        t = tgid_of.get(tid)
        if t is None:
            res["unknown_tids"].append(tid)
            bad(idx, line, "thread %d appeared without a traced clone/fork" % tid)
            tgid_of[tid] = tid
            ppid_of[tid] = -1
            fdtab[tid] = {}
            threads[tid] = {tid}
            t = tid
        return t

    def tab(tgid):
        return fdtab.setdefault(tgid, {})

    for idx in range(len(lines)):
        for ev in entries_at.get(idx, ()):
            line = lines[idx]
            tgid = proc_of(ev.tid, idx, line)
            try:
                a = split_args(ev.entry_args)
                kind, nm = None, ev.name
                if nm in ("fsync", "fdatasync", "syncfs", "sync", "copy_file_range"):
                    kind = nm
                elif nm == "sync_file_range":
                    fl = arg(a, 3)
                    if fl & C["SYNC_FILE_RANGE_WRITE"]:
                        kind = "sfr_write_wait" if fl & C["SYNC_FILE_RANGE_WAIT_AFTER"] else "sfr_write"
                    else:
                        kind = "sfr_wait"
                elif nm == "msync":
                    kind = "msync_SYNC" if arg(a, 2) & C["MS_SYNC"] else "msync_other"
                elif nm in ("write", "pwrite64", "writev", "pwritev"):
                    if not (mutant == "drop_pwrite64" and nm == "pwrite64"):
                        kind = (tab(tgid).get(arg(a, 0)) or (None,))[0]
                elif nm == "pwritev2":
                    fl = arg(a, 4)
                    rk = "osync_write" if fl & C["RWF_SYNC"] else "odsync_write" if fl & C["RWF_DSYNC"] else None
                    kind = stronger((tab(tgid).get(arg(a, 0)) or (None,))[0], rk)
                elif nm == "ioctl":
                    req = arg(a, 1) & 0xFFFFFFFF
                    kind = "FICLONE" if req == C["FICLONE"] else "FICLONERANGE" if req == C["FICLONERANGE"] else None
                elif nm == "lseek":
                    if arg(a, 0) == C["V1_MARK_FD"]:
                        mark = arg(a, 1) & MASK64
                elif nm == "close":
                    tab(tgid).pop(arg(a, 0), None)
                elif nm == "close_range":
                    first, last, fl = arg(a, 0), arg(a, 1) & 0xFFFFFFFF, arg(a, 2)
                    if fl & C["CLOSE_RANGE_UNSHARE"]:
                        fdtab[tgid] = dict(tab(tgid))
                    t = tab(tgid)
                    for fd in [fd for fd in t if first <= fd <= last]:
                        if fl & C["CLOSE_RANGE_CLOEXEC"]:
                            t[fd] = (t[fd][0], True)
                        else:
                            del t[fd]
                elif nm == "fcntl":
                    cmd = arg(a, 1)
                    if cmd == C["F_SETFD"]:
                        fd = arg(a, 0)
                        if fd in tab(tgid):
                            tab(tgid)[fd] = (tab(tgid)[fd][0], bool(arg(a, 2) & C["FD_CLOEXEC"]))
                elif nm in ("clone", "clone3", "fork", "vfork"):
                    child = ret_value(ev.ret) if ev.ret is not None else None
                    if child is not None and child > 0:
                        if nm == "fork":
                            fl = 0
                        elif nm == "vfork":
                            fl = C["CLONE_VM"] | C["CLONE_VFORK"]
                        else:
                            fl = field(ev.full_args or ev.entry_args, "flags")
                        if fl & C["CLONE_THREAD"]:
                            tgid_of[child] = tgid
                            threads[tgid].add(child)
                        else:
                            tgid_of[child] = child
                            ppid_of[child] = tgid
                            threads[child] = {child}
                            fdtab[child] = tab(tgid) if fl & C["CLONE_FILES"] else dict(tab(tgid))
                elif nm in ASYNC:
                    res["async_io"][nm] = res["async_io"].get(nm, 0) + 1
                if kind is not None:
                    ev.kind = kind
                    rp = restart_pending.pop(ev.tid, None)
                    if rp == (nm, ev.entry_args):
                        ev.skip_count = True  # the kernel restarted the call: one call, already counted
                    else:
                        c = counts.setdefault(tgid, dict.fromkeys(KINDS, 0))
                        c[kind] += 1
                        key = (tgid, 1 if mark & IDLE else 0, mark & ~IDLE & MASK64, kind)
                        marks[key] = marks.get(key, 0) + 1
                else:
                    restart_pending.pop(ev.tid, None)
            except ParseError as e:
                bad(idx, line, str(e))
        for ev in exits_at.get(idx, ()):
            line = lines[idx]
            tgid = tgid_of.get(ev.tid)
            if tgid is None:
                continue  # reported at entry
            try:
                if ev.ret == "?" and ev.ret_tok in RESTART:
                    if ev.kind is not None:
                        restart_pending[ev.tid] = (ev.name, ev.entry_args)
                    continue
                rv = ret_value(ev.ret)
                if ev.kind is not None and rv == -1:
                    f = fails.setdefault(tgid, dict.fromkeys(KINDS, 0))
                    f[ev.kind] += 1
                if rv is None or rv < 0:
                    continue
                a = split_args(ev.full_args)
                nm = ev.name
                t = tab(tgid)
                if nm == "open":
                    fl = arg(a, 1)
                    t[rv] = (sync_kind_of_flags(fl), bool(fl & C["O_CLOEXEC"]))
                elif nm in ("openat", "open_by_handle_at"):
                    fl = arg(a, 2)
                    t[rv] = (sync_kind_of_flags(fl), bool(fl & C["O_CLOEXEC"]))
                elif nm == "openat2":
                    fl = field(a[2] if len(a) > 2 else "", "flags")
                    t[rv] = (sync_kind_of_flags(fl), bool(fl & C["O_CLOEXEC"]))
                elif nm == "creat":
                    t[rv] = (None, False)
                elif nm in ("dup", "dup2", "dup3"):
                    old = arg(a, 0)
                    if nm != "dup" and old == rv:
                        continue
                    clo = bool(arg(a, 2) & C["O_CLOEXEC"]) if nm == "dup3" else False
                    t[rv] = ((t.get(old) or (None,))[0], clo)
                elif nm == "fcntl":
                    cmd = arg(a, 1)
                    if cmd in (C["F_DUPFD"], C["F_DUPFD_CLOEXEC"]):
                        t[rv] = ((t.get(arg(a, 0)) or (None,))[0], cmd == C["F_DUPFD_CLOEXEC"])
                elif nm in ("execve", "execveat") and rv == 0:
                    execs[tgid] = execs.get(tgid, 0) + 1
                    fdtab[tgid] = {fd: v for fd, v in t.items() if not v[1]}
            except ParseError as e:
                bad(idx, line, str(e))

    root_exit = None
    for idx, tid, body in specials:
        if tgid_of.get(tid) == root and tid == root:
            mm = re.match(r"\+\+\+ (exited with \d+|killed by \w+)", body)
            if mm:
                root_exit = mm.group(1)
    procs = []
    for tgid in sorted(set(tgid_of.values())):
        procs.append({"pid": tgid, "ppid": ppid_of.get(tgid, 0), "threads": len(threads.get(tgid, ())),
                      "execs": execs.get(tgid, 0), "counts": counts.get(tgid, dict.fromkeys(KINDS, 0)),
                      "fails": fails.get(tgid, dict.fromkeys(KINDS, 0))})
    totals = dict.fromkeys(KINDS, 0)
    for p in procs:
        for k in KINDS:
            totals[k] += p["counts"][k]
    res.update(root_exit=root_exit, procs=procs, totals=totals,
               by_mark=sorted([list(k) + [v] for k, v in marks.items()]))
    if root not in {e.tid for e in events}:
        res.update(rc=3, verdict="REFUSED: the root pid %d made no traced call" % root)
    elif res["unparsed"]:
        res.update(rc=5, verdict="REFUSED: %d trace line(s) unread or unattributed: a count built on them is not a "
                                 "count" % res["unparsed"])
    elif res["async_io"] and not allow_async:
        res.update(rc=8, verdict="REFUSED: async I/O submissions seen %s: io_uring / AIO can carry flushes this "
                                 "counter cannot see" % json.dumps(res["async_io"], sort_keys=True))
    else:
        res.update(rc=0, verdict="ok")
    return res


def here():
    return os.environ.get("V1_BIN") or os.path.dirname(os.path.abspath(__file__))


def strace_argv(out_trace, seccomp):
    ts = trace_set()
    if ts is None:
        return None, "unknown architecture %s" % platform.machine()
    strace = os.environ.get("V1_STRACE", "strace")
    if shutil.which(strace) is None:
        return None, "strace not found (%s)" % strace
    argv = [strace, "-f", "-q", "-X", "raw", "-s", "0", "-e", "signal=none", "-e", "trace=" + ",".join(ts),
            "-o", out_trace]
    if seccomp:
        argv.append("--seccomp-bpf")
    return argv, None


def do_run(out, cmd, seccomp, extra=None):
    trace = out + ".strace"
    if os.path.exists(trace):
        os.unlink(trace)
    argv, err = strace_argv(trace, seccomp)
    if argv is None:
        res = {"instrument": "strace", "rc": 2, "verdict": "REFUSED: " + err}
    else:
        p = subprocess.run(argv + ["--"] + cmd)
        res = parse(trace)
        res["strace_rc"] = p.returncode
        res["argv"] = argv + ["--"] + cmd
        try:
            res["strace_version"] = subprocess.run([argv[0], "-V"], capture_output=True, text=True).stdout.split("\n")[0]
        except OSError:
            pass
    if extra:
        res.update(extra)
    with open(out + ".json", "w") as f:
        json.dump(res, f, indent=1)
    return res


def shim_by_pid(rep):
    by = {}
    for s in rep.get("slots", []):
        if s["idx"] == 0:
            continue
        c = by.setdefault(s["pid"], dict.fromkeys(KINDS, 0))
        for k in KINDS:
            c[k] += s["counts"][k]
    return by


def do_cross(run, out, cmd, seccomp):
    b = here()
    v1run, v1ctl = os.path.join(b, "v1run"), os.path.join(b, "v1ctl")
    st = do_run(out, [v1run, run] + cmd, seccomp)
    strict = subprocess.run([v1ctl, "report", run, "--json"], capture_output=True, text=True)
    loose = subprocess.run([v1ctl, "report", run, "--json", "--allow-incomplete", "--allow-go"], capture_output=True,
                           text=True)
    with open(out + ".shim.json", "w") as f:
        f.write(loose.stdout + loose.stderr)
    cross = {"run": run, "strace_rc": st.get("rc"), "shim_rc": strict.returncode, "shim_counts_rc": loose.returncode}
    try:
        rep = json.loads(loose.stdout)
    except ValueError:
        rep = None
    if rep is None or st.get("rc") not in (0, 8):
        cross.update(rc=st.get("rc") if st.get("rc") not in (0, 8, None) else 2,
                     verdict="REFUSED: an instrument did not produce counts", missed={}, extra={})
    else:
        sp = {p["pid"]: p["counts"] for p in st["procs"]}
        hp = shim_by_pid(rep)
        missed, extra = {}, {}
        for pid in sorted(set(sp) | set(hp)):
            for k in KINDS:
                d = sp.get(pid, {}).get(k, 0) - hp.get(pid, {}).get(k, 0)
                if d > 0:
                    missed.setdefault(str(pid), {})[k] = d
                elif d < 0:
                    extra.setdefault(str(pid), {})[k] = -d
        cross.update(missed=missed, extra=extra, shim_overflow=rep.get("slot_overflow", 0),
                     pids_only_in_strace=sorted(set(sp) - set(hp)), pids_only_in_shim=sorted(set(hp) - set(sp)))
        if extra or rep.get("slot_overflow"):
            cross.update(rc=10, verdict="ANOMALY: the shim counted calls strace did not see (or slot overflow)")
        elif missed:
            cross.update(rc=9, verdict="SHIM MISSED %d call(s) that strace saw: %s" % (
                sum(sum(v.values()) for v in missed.values()), json.dumps(missed, sort_keys=True)))
        else:
            cross.update(rc=0, verdict="ok: both instruments agree on every (pid, kind)")
    with open(out + ".cross.json", "w") as f:
        json.dump(cross, f, indent=1)
    return cross


def main(argv):
    if len(argv) >= 1 and argv[0] == "consts":
        print(json.dumps(C, sort_keys=True))
        return 0
    if len(argv) >= 2 and argv[0] == "parse":
        root, allow, mutant, i = None, False, None, 2
        while i < len(argv):
            if argv[i] == "--root" and i + 1 < len(argv):
                root, i = int(argv[i + 1]), i + 2
            elif argv[i] == "--allow-async":
                allow, i = True, i + 1
            elif argv[i] == "--mutant" and i + 1 < len(argv):
                mutant, i = argv[i + 1], i + 2
            else:
                print("v1strace: bad parse argument %s" % argv[i], file=sys.stderr)
                return 2
        res = parse(argv[1], root=root, allow_async=allow, mutant=mutant)
        print(json.dumps(res))
        return res["rc"]
    if len(argv) >= 2 and argv[0] in ("run", "cross") and "--" in argv:
        k = argv.index("--")
        head, cmd = argv[1:k], argv[k + 1:]
        seccomp = "--seccomp-bpf" in head
        head = [h for h in head if h != "--seccomp-bpf"]
        if not cmd:
            print("v1strace: no command", file=sys.stderr)
            return 2
        if argv[0] == "run" and len(head) == 1:
            res = do_run(head[0], cmd, seccomp)
            print("v1strace: %s (rc %s)" % (res.get("verdict"), res.get("rc")), file=sys.stderr)
            return res["rc"]
        if argv[0] == "cross" and len(head) == 2:
            res = do_cross(head[0], head[1], cmd, seccomp)
            print("v1strace cross: %s (rc %s)" % (res.get("verdict"), res.get("rc")), file=sys.stderr)
            return res["rc"]
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
