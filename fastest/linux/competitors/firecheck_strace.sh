#!/usr/bin/env bash
# firecheck_strace.sh OUT DIR -- prove the flush counter (trace.sh + stracecount.py) fires, counts exactly, and
# refuses its blind spots, on this runner and filesystem (DIR), before any competitor is counted with it.
# Truth comes from a probe whose syscalls are known by construction, never from the counter's own output.
#
#   F1 launch mode: a probe makes fsync x3, fdatasync x2, sync x1, msync(MS_SYNC) x1 (mmap.flush), fsync x2 from a
#      second thread and fsync x1 from a forked child; plus non-flushes: copy_file_range x1, FICLONE x1 (cp
#      --reflink=always) and opens. Expect exactly fsync 6, fdatasync 2, sync 1, msync_sync 1 = 10 flushes,
#      copy_file_range 1, ficlone 1, verdict ok.
#   F2 attach mode: the same probe, started detached and waiting for a trigger, attached with strace_attach and
#      then triggered: the thread and the child are created AFTER the attach. Expect the same 10.
#   F3 idle attach: the probe attached for 2 s and never triggered. Expect 0 flushes and verdict ok (an empty
#      window parses as zero, not as a missing table).
#   F3b the F3 trace read WITHOUT the attach proof: a table-less, call-less window must then be REFUSED. F3b FAILS
#      (never passes untested) if the F3 window was not table-less and call-less (review finding 13).
#   F4 blind spot: dd oflag=dsync. Expect verdict INCOMPLETE (O_DSYNC open), never ok.
#   F5 blind spot: fio --ioengine=io_uring --fsync=1. Expect verdict INCOMPLETE (io_uring), never ok.
#   F6 blind spot BEFORE the attach: a probe opens a file O_DSYNC, is attached, then writes it twice. The openat is
#      not in the trace, so only the pre-attach fd scan can see it. Expect INCOMPLETE with one O_DSYNC fd at attach.
#   F6b the same with the O_DSYNC fd held by a CHILD of the attached pid: expect INCOMPLETE, the one fd at attach in
#      the child, and every roster process scanned.
#   F7 threads that exist BEFORE the attach: a probe starts a thread, is attached, then the thread fsyncs x3 and the
#      main thread x2. Expect exactly 5 fsyncs, verdict ok.
#   F8 blind spot: pwritev2 with RWF_DSYNC (launch mode). Expect INCOMPLETE (rwf_sync_writes >= 1).
#   F9 blind spot: fio --ioengine=libaio (io_submit). Expect INCOMPLETE (io_submit >= 1).
#   F10a the untraced-descendant detector: a probe forks a child, then a plain `strace -f -p <parent>` (no
#      enumeration) is attached; untraced_tasks must name the child (the condition strace_attach retries on).
#   F10b the same tree attached by strace_attach: it must list the pre-existing child, complete on try 1, and count
#      the parent's fsync x1 and the child's fsync x2 = 3, verdict ok.
#   F10c a fork storm (a child every ~20 ms, each fsyncs once 2 s later and logs its tracer and the fsync's start and
#      end; the parent fsyncs and logs once per round) attached by a parent-only strace: the control MUST log
#      untraced child fsyncs after t0, or the storm proves nothing.
#   F10d the same storm attached by strace_attach (freeze, enumeration, completeness): no untraced child fsync after
#      t0, frozen=1, the traced pids = the trace's fsync pids, the count after the t0 cut inside the probe log's
#      [started after t0, ended after t0] bounds while the cut left out >= 1 traced fsync (the parent's, between the
#      thaw and t0), and at least one child alive at the seize (no clone line in the trace) fsyncing after t0, seen
#      by the probe log and attributed by the trace to a process at attach.
#   F11 one attach split by strace_mark: fsync x2, tsplit, fsync x3 -> --part pre counts 2 and --part post 3.
#   F12 the t1 cut: the split probe with t1 stamped between its fsync x2 and fsync x3 -> counts 2, and >= 3 calls
#      after t1 left out.
#   F13 the clock-step refusals forced to fire on copies of F2's window: a call stamp 2 s back in the trace, and
#      t1_mono 2 s off in the window record, are each REFUSED; the unmodified copy counts F2's 10.
# Exit 0 only if all NCHECK pass; the verdict line is the last line of OUT/firecheck.txt.
set -uo pipefail
OUT=${1:?usage: firecheck_strace.sh OUT DIR}
DIR=${2:?usage: firecheck_strace.sh OUT DIR}
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/trace.sh"
SC="$HERE/stracecount.py"
NCHECK=19
mkdir -p "$OUT" "$DIR/fc"
fails=0
log() { echo "$*" | tee -a "$OUT/firecheck.txt"; }
: >"$OUT/firecheck.txt"
log "# strace fire-check $(date -u +%FT%TZ) $(strace -V | sed -n 1p) kernel=$(uname -r) dir=$DIR fstype=$(findmnt -n -o FSTYPE -T "$DIR")"

cat >"$OUT/probe.py" <<'PY'
import mmap, os, subprocess, sys, threading, time
d, trigger = sys.argv[1], sys.argv[2]
if trigger != "-":
    while not os.path.exists(trigger):
        time.sleep(0.02)
fd = os.open(f"{d}/probe.dat", os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
os.write(fd, b"x" * 8192)
for _ in range(3):
    os.fsync(fd)
for _ in range(2):
    os.fdatasync(fd)
os.sync()
m = mmap.mmap(fd, 4096)
m[0:1] = b"y"
m.flush()  # msync(MS_SYNC)
m.close()
def worker():
    for _ in range(2):
        os.fsync(fd)
t = threading.Thread(target=worker); t.start(); t.join()
pid = os.fork()
if pid == 0:
    os.fsync(fd)
    os._exit(0)
os.waitpid(pid, 0)
src = os.open(f"{d}/probe.dat", os.O_RDONLY)
dst = os.open(f"{d}/probe.cfr", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.copy_file_range(src, dst, 4096)
os.close(src); os.close(dst)
subprocess.run(["cp", "--reflink=always", f"{d}/probe.dat", f"{d}/probe.clone"], check=True)
os.close(fd)
open(f"{d}/probe.done", "w").close()
if trigger != "-":
    # Attach mode: stay alive until the harness kills us, so the detach finds the traced main pid running (a window
    # whose main pid is gone at the detach is refused).
    while True:
        time.sleep(1)
PY

# probe2.py MODE DIR TRIGGER -- the pre-attach probes (F6, F7, F10) and the RWF_DSYNC probe (F8). Each attach mode
# does its setup, writes DIR/MODE.ready, waits for TRIGGER, does its known syscalls, writes DIR/MODE.done and stays
# alive until killed.
cat >"$OUT/probe2.py" <<'PY'
import os, sys, threading, time
mode, d, trig = sys.argv[1], sys.argv[2], sys.argv[3]
def wait():
    while not os.path.exists(trig):
        time.sleep(0.02)
def ready():
    open(f"{d}/{mode}.ready", "w").close()
def done_and_stay():
    open(f"{d}/{mode}.done", "w").close()
    while True:
        time.sleep(1)
if mode == "rwf":  # launch mode
    fd = os.open(f"{d}/rwf.dat", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.pwritev(fd, [b"x" * 4096], 0, os.RWF_DSYNC)
    os.close(fd)
    sys.exit(0)
if mode == "dsync-pre":
    fd = os.open(f"{d}/dsync-pre.dat", os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_DSYNC, 0o644)
    ready(); wait()
    os.write(fd, b"x" * 4096)
    os.write(fd, b"y" * 4096)
    done_and_stay()
if mode == "forkstorm":
    # A child every ~20 ms; each sleeps 2 s, reads its own TracerPid, fsyncs once and logs "pid tracerpid fsync-end
    # fsync-start". The storm starts 0.3 s before the seize and 2 s is far above the seize-to-t0 time (0.31-0.44 s in
    # run 37409606650), so the children alive at the SEIZE still fsync inside the window, the case F10d exists for
    # (fifth review, finding 1: with 0.5 s every pre-existing child had flushed before t0). A child whose fsync came
    # after the window opened but that was not traced is a missed flush (F10c/F10d).
    # The parent fsyncs and logs too, once per round: it is traced from the seize, so its fsyncs between the thaw and
    # t0 sit in the kept trace BEFORE t0 and must be cut, and its later ones counted -- the edge of the t0 cut, which
    # F10d otherwise never reaches now that no child flushes before t0 (fifth review, finding 2).
    fd = os.open(f"{d}/forkstorm.dat", os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd, b"x" * 4096)
    def flush_and_log():
        tp = [ln.split()[1] for ln in open("/proc/self/status") if ln.startswith("TracerPid:")][0]
        t0c = time.time()
        os.fsync(fd)
        t = time.time()
        with open(f"{d}/forkstorm.log", "a") as f:
            f.write(f"{os.getpid()} {tp} {t:.6f} {t0c:.6f}\n")
    stop = trig + ".stop"
    ready()
    while not os.path.exists(stop):
        if os.fork() == 0:
            try:  # a child never returns into the parent's loop, whatever flush_and_log raises
                time.sleep(2.0)
                flush_and_log()
            finally:
                os._exit(0)
        flush_and_log()
        try:
            while os.waitpid(-1, os.WNOHANG)[0]:
                pass
        except ChildProcessError:
            pass
        time.sleep(0.02)
    try:
        while True:
            os.waitpid(-1, 0)
    except ChildProcessError:
        pass
    done_and_stay()
if mode == "split":
    fd = os.open(f"{d}/split.dat", os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd, b"x" * 4096)
    ready(); wait()
    for _ in range(2):
        os.fsync(fd)
    open(f"{d}/split.half", "w").close()
    while not os.path.exists(trig + ".2"):
        time.sleep(0.02)
    for _ in range(3):
        os.fsync(fd)
    done_and_stay()
if mode == "dsync-child-pre":
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        fd = os.open(f"{d}/dsync-child-pre.dat", os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_DSYNC, 0o644)
        os.write(w, b"o"); os.close(w)
        wait()
        os.write(fd, b"x" * 4096)
        os.write(fd, b"y" * 4096)
        os._exit(0)
    os.close(w)
    os.read(r, 1)  # the child holds its O_DSYNC fd now
    ready(); wait()
    os.waitpid(pid, 0)
    done_and_stay()
if mode == "thread-pre":
    fd = os.open(f"{d}/thread-pre.dat", os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd, b"x" * 4096)
    def w():
        wait()
        for _ in range(3):
            os.fsync(fd)
    t = threading.Thread(target=w); t.start()
    ready(); wait()
    for _ in range(2):
        os.fsync(fd)
    t.join()
    done_and_stay()
if mode == "fork-pre":
    fd = os.open(f"{d}/fork-pre.dat", os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd, b"x" * 4096)
    pid = os.fork()
    if pid == 0:
        wait()
        os.fsync(fd)
        os.fsync(fd)
        os._exit(0)
    ready(); wait()
    os.fsync(fd)
    os.waitpid(pid, 0)
    done_and_stay()
sys.exit(f"probe2: unknown mode {mode}")
PY

check() { # check NAME JSON EXPR -- EXPR is a python expression over r (the count JSON)
  local name=$1 json=$2 expr=$3
  if python3 -c "import json,sys; r=json.load(open(sys.argv[1])); sys.exit(0 if ($expr) else 1)" "$json"; then
    log "PASS $name: $expr"
  else
    log "FAIL $name: $expr | got $(python3 -c "import json,sys; r=json.load(open(sys.argv[1])); print({k: r.get(k) for k in ('verdict','flush_by_syscall','copy_file_range_calls','ficlone','osync_opens','osync_fds_at_attach','rwf_sync_writes','io_uring','io_submit','problems')})" "$json" 2>&1 | tail -1)"
    fails=$((fails + 1))
  fi
}
EXACT='r["verdict"]=="ok" and r["flush_by_syscall"]=={"fsync":6,"fdatasync":2,"syncfs":0,"sync":1,"msync_sync":1} and r["flushes"]==10 and r["copy_file_range_calls"]==1 and r["ficlone"]==1'
count() { python3 "$SC" count "$OUT/$1.strace" --extra "$OUT/$1.strace.err" --root "$DIR" --window "$OUT/$1.window" >"$OUT/$1.json"; }

# start_probe2 MODE -> PP2: the detached probe's pid, once it wrote MODE.ready (exit 1 if it never did)
start_probe2() {
  local mode=$1 i
  rm -f "$DIR/fc/$mode".* "$DIR/fc/go-$mode" "$DIR/fc/go-$mode".*  # every trigger of the mode, incl. .stop and .2
  ( exec /usr/bin/perl -MPOSIX -e 'POSIX::setsid(); exec @ARGV or die' -- python3 -B "$OUT/probe2.py" "$mode" "$DIR/fc" "$DIR/fc/go-$mode" ) \
    </dev/null >"$OUT/probe2-$mode.log.txt" 2>&1 &
  PP2=$!
  for ((i = 0; i < 400; i++)); do [ -e "$DIR/fc/$mode.ready" ] && return 0; sleep 0.05; done
  return 1
}
# run_probe2 NAME MODE -- attach (strace_attach), trigger, wait for MODE.done, detach, count into OUT/NAME.json
run_probe2() {
  local name=$1 mode=$2 i
  strace_attach "$OUT/$name" "$PP2" || return 1
  touch "$DIR/fc/go-$mode"
  for ((i = 0; i < 600; i++)); do [ -e "$DIR/fc/$mode.done" ] && break; sleep 0.05; done
  sleep 0.2
  strace_detach "$OUT/$name"
  count "$name"
}
stop_probe2() { kill "$PP2" 2>/dev/null; wait "$PP2" 2>/dev/null; return 0; }  # fork-pre's child exits by itself

# F1
rm -f "$DIR"/fc/probe.*
strace_run "$OUT/f1" python3 -B "$OUT/probe.py" "$DIR/fc" - || log "f1 strace rc=$?"
count f1
check F1-launch "$OUT/f1.json" "$EXACT"

# F2 and F3 share one detached probe: idle attach first (never triggered), then the triggered attach.
rm -f "$DIR"/fc/probe.* "$DIR/fc/go"
( exec /usr/bin/perl -MPOSIX -e 'POSIX::setsid(); exec @ARGV or die' -- python3 -B "$OUT/probe.py" "$DIR/fc" "$DIR/fc/go" ) \
  </dev/null >"$OUT/probe.log.txt" 2>&1 &
PP=$!
sleep 0.5
if strace_attach "$OUT/f3" "$PP"; then
  sleep 2
  strace_detach "$OUT/f3"
  count f3
  check F3-idle-attach "$OUT/f3.json" 'r["verdict"]=="ok" and r["flushes"]==0 and r["summary_found"]'
  log "F3 note: empty_window=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['empty_window'])" "$OUT/f3.json") strace_rc=$(grep -o 'strace_rc=[0-9]*' "$OUT/f3.window")"
  # F3b: the same table-less window WITHOUT the attach proof must not read as zero. Its precondition (F3's window
  # had no table and no call lines) is part of the check: an untested F3b fails. The window given is a well-formed
  # LAUNCH record (strace_rc 0), so the only thing missing is the attach proof, and the refusal must be the missing
  # table, not a malformed record (third review, finding 7: without any window it was refused for the record alone).
  printf 'cmd=f3b-control t0=%s\nt1=%s\nstrace_rc=0\n' "$(date +%s.%N)" "$(date +%s.%N)" >"$OUT/f3b.window"
  python3 "$SC" count "$OUT/f3.strace" --extra "$OUT/f3.strace.err" --root "$DIR" --window "$OUT/f3b.window" >"$OUT/f3b.json"
  check F3b-unproven-empty-refused "$OUT/f3b.json" \
    'not r["lines"] and not r["summary"] and r["problems"]==["no -c summary table found"]'
else
  log "FAIL F3-idle-attach: strace_attach failed"; fails=$((fails + 1))
  log "FAIL F3b-unproven-empty-refused: not tested (no F3 window)"; fails=$((fails + 1))
fi
if strace_attach "$OUT/f2" "$PP"; then
  touch "$DIR/fc/go"
  for ((i = 0; i < 600; i++)); do [ -e "$DIR/fc/probe.done" ] && break; sleep 0.05; done
  sleep 0.2
  strace_detach "$OUT/f2"
  count f2
  check F2-attach "$OUT/f2.json" "$EXACT"
else
  log "FAIL F2-attach: strace_attach failed"; fails=$((fails + 1))
fi
kill "$PP" 2>/dev/null; wait "$PP" 2>/dev/null

# F4
strace_run "$OUT/f4" dd if=/dev/zero of="$DIR/fc/dsync.dat" bs=4k count=2 oflag=dsync status=none || true
count f4
check F4-osync-refused "$OUT/f4.json" 'r["verdict"].startswith("INCOMPLETE") and r["osync_opens"]>=1'

# F5
strace_run "$OUT/f5" fio --name=fc --filename="$DIR/fc/uring.dat" --ioengine=io_uring --rw=write --bs=4k \
  --size=16k --fsync=1 --output="$OUT/f5.fio.txt" || true
count f5
check F5-io_uring-refused "$OUT/f5.json" 'r["verdict"].startswith("INCOMPLETE") and r["io_uring"]>=1'

# F6
if start_probe2 dsync-pre && run_probe2 f6 dsync-pre; then
  check F6-osync-before-attach-refused "$OUT/f6.json" \
    'r["verdict"].startswith("INCOMPLETE") and len(r["osync_fds_at_attach"])==1 and r["osync_opens"]==0'
else
  log "FAIL F6-osync-before-attach-refused: the probe or its attach failed"; fails=$((fails + 1))
fi
stop_probe2 dsync-pre

# F6b: the same, but the O_DSYNC fd is held by a CHILD of the attached pid (second review, finding 1: only the main
# pid's scan was required, and 11 live PG processes went unscanned).
if start_probe2 dsync-child-pre && run_probe2 f6b dsync-child-pre; then
  check F6b-osync-in-a-descendant-refused "$OUT/f6b.json" \
    'r["verdict"].startswith("INCOMPLETE") and len(r["osync_fds_at_attach"])==1 and not r["osync_fds_at_attach"][0].startswith("pid "+r["main_pid"]+" ") and not r["fdsync_unscanned"]'
else
  log "FAIL F6b-osync-in-a-descendant-refused: the probe or its attach failed"; fails=$((fails + 1))
fi
stop_probe2 dsync-child-pre

# F7
if start_probe2 thread-pre && run_probe2 f7 thread-pre; then
  check F7-threads-before-attach "$OUT/f7.json" \
    'r["verdict"]=="ok" and r["flush_by_syscall"]["fsync"]==5 and r["flushes"]==5'
else
  log "FAIL F7-threads-before-attach: the probe or its attach failed"; fails=$((fails + 1))
fi
stop_probe2 thread-pre

# F8
strace_run "$OUT/f8" python3 -B "$OUT/probe2.py" rwf "$DIR/fc" - || log "f8 strace rc=$?"
count f8
check F8-rwf_dsync-refused "$OUT/f8.json" 'r["verdict"].startswith("INCOMPLETE") and r["rwf_sync_writes"]>=1'

# F9
strace_run "$OUT/f9" fio --name=fc --filename="$DIR/fc/aio.dat" --ioengine=libaio --iodepth=1 --rw=write --bs=4k \
  --size=16k --output="$OUT/f9.fio.txt" || true
count f9
check F9-libaio-refused "$OUT/f9.json" 'r["verdict"].startswith("INCOMPLETE") and r["io_submit"]>=1'

# F10a and F10b on one fork-pre probe
if start_probe2 fork-pre; then
  kids=$(descendants "$PP2" | tr '\n' ' ')
  strace -f -qq -e trace=fsync -o "$OUT/f10a.strace" -p "$PP2" 2>"$OUT/f10a.strace.err" &
  sp=$!
  for ((i = 0; i < 400; i++)); do traced_all "$sp" "$PP2" && break; sleep 0.05; done
  miss=$(untraced_tasks "$sp" $(descendants "$PP2") | tr '\n' ' ')
  # traced_all over the parent AND its live untraced child must say "not all traced" too (fourth review, finding 8:
  # nothing planted a live untraced task for traced_all to catch).
  ta=caught; traced_all "$sp" "$PP2" $kids && ta=missed
  kill -INT "$sp" 2>/dev/null; wait "$sp" 2>/dev/null
  if [ -n "${kids// /}" ] && [ -n "${miss// /}" ] && [ $ta = caught ]; then
    log "PASS F10a-untraced-descendant-detected: children [$kids] untraced under a parent-only attach: [$miss]; traced_all $ta it"
  else
    log "FAIL F10a-untraced-descendant-detected: children [$kids], untraced [$miss], traced_all $ta it"; fails=$((fails + 1))
  fi
  if run_probe2 f10b fork-pre; then
    check F10b-descendants-attached "$OUT/f10b.json" \
      'r["verdict"]=="ok" and r["flush_by_syscall"]["fsync"]==3 and r["flushes"]==3'
    grep -q 'attach_tries=1 ' "$OUT/f10b.window" && [ "$(wc -w <<<"$(sed -n 's/^main=[0-9]* pids=\(.*\) strace_pid=.*/\1/p' "$OUT/f10b.window")")" -ge 2 ] ||
      { log "FAIL F10b-descendants-attached: the attach did not list the child or needed retries: $(head -1 "$OUT/f10b.window")"; fails=$((fails + 1)); }
  else
    log "FAIL F10b-descendants-attached: strace_attach failed"; fails=$((fails + 1))
  fi
else
  log "FAIL F10a/F10b: the fork-pre probe never became ready"; fails=$((fails + 2))
fi
stop_probe2 fork-pre

# storm_misses LOG T0 [STRACEPID] -> how many fork-storm children fsynced after T0 while not traced by STRACEPID
# (TracerPid 0, or any tracer but STRACEPID when given: a strace_attach that needed a second try had a FIRST strace
# whose children fsynced into its discarded trace -- run 37400500051), then how many fsynced while traced by it.
storm_misses() {
  python3 -c "
import sys
t0 = float(sys.argv[2]); st = sys.argv[3] if len(sys.argv) > 3 else None; miss = traced = 0
for ln in open(sys.argv[1]):
    f = ln.split(); tp, t = f[1], float(f[2])  # pid tracerpid fsync-end [fsync-start]
    mine = tp != '0' and (st is None or tp == st)
    if not mine and t > t0: miss += 1
    if mine: traced += 1
print(miss, traced)" "$@"
}
# storm_window_bounds LOG T0 STRACEPID TRACE MAIN -> 'LOWER UPPER PRE CUT BORN': the probe's own truth for the kept
# trace's count. The counter keeps a call whose ENTRY stamp is >= t0, and that stamp lies between the process's clock
# read before the fsync (field 4) and after it (field 3), so the count after the t0 cut must lie in [LOWER, UPPER]:
# LOWER = fsyncs traced by STRACEPID that started after T0, UPPER = those that ended after T0 (10 us of slack each way
# for the 6-decimal stamps). CUT = traced fsyncs that ended before T0: in the kept trace and left out by the cut, so
# a count <= UPPER with CUT >= 1 shows the cut at work. PRE = the UPPER fsyncs of CHILDREN (not MAIN) whose creation
# the trace never saw (no clone/fork returning their pid): alive at the seize and flushing inside the window, the
# case F10d exists for (fifth review, findings 1 and 2). BORN = the pids the trace saw created.
storm_window_bounds() {
  python3 -c "
import re, sys
t0 = float(sys.argv[2]); st = sys.argv[3]; main = sys.argv[5]; eps = 1e-5
spawn = re.compile(r'^\d+\s+[\d.]+\s+(?:<\.\.\. )?(?:clone3?|v?fork)\b.*\)\s+=\s+(\d+)')
born = {m.group(1) for m in (spawn.match(ln) for ln in open(sys.argv[4], errors='replace')) if m}
lo = hi = pre = cut = 0
for ln in open(sys.argv[1]):
    f = ln.split(); pid, tp, end = f[0], f[1], float(f[2]); start = float(f[3]) if len(f) > 3 else end
    if tp != st: continue
    if start >= t0 + eps: lo += 1
    if end < t0 - eps: cut += 1
    if end >= t0 - eps:
        hi += 1
        if pid not in born and pid != main: pre += 1
print(lo, hi, pre, cut, len(born))" "$@"
}
# storm_pids_match LOG TRACE STRACEPID -> exit 0 when the children that logged STRACEPID as their tracer are exactly
# the pids with an fsync line in TRACE (fourth review, finding 4: a set check, not fsyncs >= traced).
storm_pids_match() {
  python3 -c "
import re, sys
logged = {ln.split()[0] for ln in open(sys.argv[1]) if ln.split()[1] == sys.argv[3]}
seen = {m.group(1) for m in (re.match(r'^(\d+)\s+(?:[\d.]+\s+)?fsync\(', ln) for ln in open(sys.argv[2])) if m}
print('logged', len(logged), 'in trace', len(seen), 'only logged', sorted(logged - seen)[:5], 'only traced', sorted(seen - logged)[:5])
sys.exit(0 if logged and logged == seen else 1)" "$@"
}
# F10c, the negative control: a parent-only attach (no enumeration, no freeze) to a fork storm MUST miss children
# that were alive at the seize and fsync after it -- or the storm does not exercise the race and F10d proves nothing.
if start_probe2 forkstorm; then
  sleep 0.3
  strace -f -qq -e trace=fsync -o "$OUT/f10c.strace" -p "$PP2" 2>"$OUT/f10c.strace.err" &
  sp=$!
  for ((i = 0; i < 400; i++)); do traced_all "$sp" "$PP2" && break; sleep 0.05; done
  ctl_attached=0; [ $i -lt 400 ] && ctl_attached=1
  [ $ctl_attached = 1 ] || log "F10c note: the control strace never attached (it then cannot miss for the right reason)"
  t0=$(date +%s.%N)
  sleep 0.5
  touch "$DIR/fc/go-forkstorm.stop"
  for ((i = 0; i < 600; i++)); do [ -e "$DIR/fc/forkstorm.done" ] && break; sleep 0.05; done
  kill -INT "$sp" 2>/dev/null; wait "$sp" 2>/dev/null
  read -r miss traced < <(storm_misses "$DIR/fc/forkstorm.log" "$t0")
  cp "$DIR/fc/forkstorm.log" "$OUT/f10c.forkstorm.log.txt" 2>/dev/null
  if [ $ctl_attached = 1 ] && [ "${miss:-0}" -ge 1 ]; then
    log "PASS F10c-storm-control-misses: a parent-only attach missed $miss untraced child fsync(s) after t0 ($traced traced)"
  else
    log "FAIL F10c-storm-control-misses: the control missed nothing (miss=$miss traced=$traced): the storm did not exercise the race"
    fails=$((fails + 1))
  fi
else
  log "FAIL F10c-storm-control-misses: the fork-storm probe never became ready"; fails=$((fails + 1))
fi
stop_probe2 forkstorm
# F10d: the same storm attached by strace_attach (freeze + enumeration + completeness) must miss NO child fsync after
# its t0, record frozen=1, count what the probe log bounds, and see a pre-existing child flush inside the window.
if start_probe2 forkstorm; then
  sleep 0.3
  if strace_attach "$OUT/f10d" "$PP2"; then
    t0=$(sed -n 's/.* t0=\([0-9.]*\).*/\1/p' "$OUT/f10d.window")
    sleep 0.5
    touch "$DIR/fc/go-forkstorm.stop"
    for ((i = 0; i < 600; i++)); do [ -e "$DIR/fc/forkstorm.done" ] && break; sleep 0.05; done
    sleep 0.2
    strace_detach "$OUT/f10d"
    count f10d
    kept=$(sed -n 's/.* strace_pid=\([0-9]*\) .*/\1/p' "$OUT/f10d.window")
    read -r miss traced < <(storm_misses "$DIR/fc/forkstorm.log" "$t0" "$kept")
    cp "$DIR/fc/forkstorm.log" "$OUT/f10d.forkstorm.log.txt" 2>/dev/null  # the probe's own record, kept with the raw
    match=$(storm_pids_match "$DIR/fc/forkstorm.log" "$OUT/f10d.strace" "$kept"); mrc=$?
    read -r lo hi pre cut born < <(storm_window_bounds "$DIR/fc/forkstorm.log" "$t0" "$kept" "$OUT/f10d.strace" "$PP2")
    # The count after the t0 cut must equal the probe's truth within [lo, hi] while the cut left out >= 1 traced fsync
    # (the parent's, between the thaw and t0), and the trace itself must attribute a flush in the window to a CHILD
    # present at the attach (role 'process at attach: ...': not the main, not born in the window).
    got=$(python3 -c "
import json, sys
r = json.load(open(sys.argv[1]))
print(r['verdict'][:200].replace(' ', '_'), r['flushes'], sum(n for k, n in r.get('by_role', {}).items() if k.startswith('process at attach')))" "$OUT/f10d.json" 2>&1)
    read -r verdict flushes atpre <<<"$got"
    if [ "$miss" = 0 ] && [ $mrc = 0 ] && grep -q ' frozen=1 ' "$OUT/f10d.window" && [ "$verdict" = ok ] &&
      [ "${pre:-0}" -ge 1 ] && [ "${atpre:-0}" -ge 1 ] && [ "${cut:-0}" -ge 1 ] &&
      [ "${lo:-x}" -le "${flushes:-y}" ] 2>/dev/null && [ "$flushes" -le "$hi" ] 2>/dev/null; then
      log "PASS F10d-storm-attach-complete: 0 child fsyncs after t0 outside the kept trace, frozen=1, $traced fsyncs logged as traced by it, pids = the fsync pids in it ($match); count after t0 $flushes in the probe's [$lo, $hi] with $cut traced fsync(s) before t0 cut; $pre fsync(s) after t0 by children alive at the seize (probe log; the trace attributes $atpre flush(es) to them; $born pids born in the trace)"
    else
      log "FAIL F10d-storm-attach-complete: miss=$miss traced=$traced pid-sets: $match bounds=[$lo, $hi] count=$flushes cut=$cut pre_probe=$pre pre_trace=$atpre born=$born window=[$(head -1 "$OUT/f10d.window" | cut -c1-200)] verdict=$verdict"
      fails=$((fails + 1))
    fi
  else
    log "FAIL F10d-storm-attach-complete: strace_attach failed"; fails=$((fails + 1))
  fi
else
  log "FAIL F10d-storm-attach-complete: the fork-storm probe never became ready"; fails=$((fails + 1))
fi
stop_probe2 forkstorm

# F11: one attach split by strace_mark: fsync x2, the tsplit stamp, fsync x3 -> --part pre counts 2, post counts 3,
# and the whole trace 5 (second review, finding 2: the load window and its CHECKPOINT share one attach).
if start_probe2 split && strace_attach "$OUT/f11" "$PP2"; then
  touch "$DIR/fc/go-split"
  for ((i = 0; i < 600; i++)); do [ -e "$DIR/fc/split.half" ] && break; sleep 0.05; done
  sleep 0.1
  strace_mark "$OUT/f11" tsplit
  touch "$DIR/fc/go-split.2"
  for ((i = 0; i < 600; i++)); do [ -e "$DIR/fc/split.done" ] && break; sleep 0.05; done
  sleep 0.2
  strace_detach "$OUT/f11"
  count f11
  python3 "$SC" count "$OUT/f11.strace" --extra "$OUT/f11.strace.err" --root "$DIR" --window "$OUT/f11.window" --part pre >"$OUT/f11pre.json"
  python3 "$SC" count "$OUT/f11.strace" --extra "$OUT/f11.strace.err" --root "$DIR" --window "$OUT/f11.window" --part post >"$OUT/f11post.json"
  check F11-split-pre "$OUT/f11pre.json" 'r["verdict"]=="ok" and r["flush_by_syscall"]["fsync"]==2 and r["flushes"]==2'
  check F11-split-post "$OUT/f11post.json" 'r["verdict"]=="ok" and r["flush_by_syscall"]["fsync"]==3 and r["flushes"]==3'
else
  log "FAIL F11-split: the probe or its attach failed"; fails=$((fails + 2))
fi
stop_probe2 split

# F12: the t1 cut (fifth-review re-review, finding 1: no attach probe had a call after t1). One attach of the split
# probe: fsync x2, then t1 is stamped (strace_mark OUT t1; strace_detach keeps it), then fsync x3, then the detach.
# The count must end at t1: 2 fsyncs counted, the 3 after it (and the probe's later calls) in calls_after_t1.
if start_probe2 split && strace_attach "$OUT/f12" "$PP2"; then
  touch "$DIR/fc/go-split"
  for ((i = 0; i < 600; i++)); do [ -e "$DIR/fc/split.half" ] && break; sleep 0.05; done
  sleep 0.1
  strace_mark "$OUT/f12" t1
  touch "$DIR/fc/go-split.2"
  for ((i = 0; i < 600; i++)); do [ -e "$DIR/fc/split.done" ] && break; sleep 0.05; done
  sleep 0.2
  strace_detach "$OUT/f12"
  count f12
  check F12-t1-cut "$OUT/f12.json" 'r["verdict"]=="ok" and r["flush_by_syscall"]["fsync"]==2 and r["flushes"]==2 and r["calls_after_t1"]>=3'
else
  log "FAIL F12-t1-cut: the probe or its attach failed"; fails=$((fails + 1))
fi
stop_probe2 split

# F13: the clock-step refusals, forced to fire on copies of F2's passing window (fifth-review re-review, finding 2):
# (a) one call stamp moved 2 s back in the trace -> refused for the -ttt back-step; (b) t1_mono moved 2 s back in the
# window record, so realtime and monotonic disagree between t0 and t1 and between t1 and tend -> refused for a
# CLOCK_REALTIME step; (c) the unmodified copy -> ok with F2's exact 10 flushes, so (a) and (b) fail for their edit.
if [ -s "$OUT/f2.window" ] && [ -s "$OUT/f2.strace" ]; then
  for k in a b c; do
    for x in strace strace.err window fdsync pids; do cp "$OUT/f2.$x" "$OUT/f13$k.$x" 2>/dev/null; done
  done
  awk '/^[0-9]+ +[0-9]+\.[0-9]+ / { n++; if (n == 2) $2 = sprintf("%.6f", prev - 2.0); prev = $2 + 0 } { print }' \
    "$OUT/f2.strace" >"$OUT/f13a.strace"
  awk '/^t1=/ { for (i = 1; i <= NF; i++) if ($i ~ /^t1_mono=/) { split($i, a, "="); $i = sprintf("t1_mono=%.9f", a[2] - 2.0) } } { print }' \
    "$OUT/f2.window" >"$OUT/f13b.window"
  for k in a b c; do count "f13$k"; done
  if python3 -c "
import json, sys
a, b, c = (json.load(open(p)) for p in sys.argv[1:4])
ok = ('stepped back' in a['verdict'] and a['verdict'].startswith('REFUSED') and
      'CLOCK_REALTIME stepped' in b['verdict'] and b['verdict'].startswith('REFUSED') and
      c['verdict'] == 'ok' and c['flushes'] == 10)
print('(a)', a['verdict'][:160], '| (b)', b['verdict'][:160], '| (c)', c['verdict'][:40], c['flushes'])
sys.exit(0 if ok else 1)" "$OUT/f13a.json" "$OUT/f13b.json" "$OUT/f13c.json" >"$OUT/f13.txt" 2>&1; then
    log "PASS F13-clock-step-refused: $(head -c 600 "$OUT/f13.txt")"
  else
    log "FAIL F13-clock-step-refused: $(head -c 600 "$OUT/f13.txt")"; fails=$((fails + 1))
  fi
else
  log "FAIL F13-clock-step-refused: no F2 window to copy"; fails=$((fails + 1))
fi

rm -rf "$DIR/fc"
if [ $fails -eq 0 ]; then log "VERDICT PASS $NCHECK/$NCHECK"; exit 0; fi
log "VERDICT FAIL $fails of $NCHECK failed"
exit 1
