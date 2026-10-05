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
#   F11 one attach split by strace_mark: fsync x2, tsplit, fsync x3 -> --part pre counts 2 and --part post 3.
# Exit 0 only if all NCHECK pass; the verdict line is the last line of OUT/firecheck.txt.
set -uo pipefail
OUT=${1:?usage: firecheck_strace.sh OUT DIR}
DIR=${2:?usage: firecheck_strace.sh OUT DIR}
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/trace.sh"
SC="$HERE/stracecount.py"
NCHECK=15
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
  rm -f "$DIR/fc/$mode".* "$DIR/fc/go-$mode"
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
  # had no table and no call lines) is part of the check: an untested F3b fails.
  python3 "$SC" count "$OUT/f3.strace" --extra "$OUT/f3.strace.err" --root "$DIR" >"$OUT/f3b.json"
  check F3b-unproven-empty-refused "$OUT/f3b.json" \
    'not r["lines"] and not r["summary"] and r["verdict"].startswith("REFUSED")'
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
  kill -INT "$sp" 2>/dev/null; wait "$sp" 2>/dev/null
  if [ -n "${kids// /}" ] && [ -n "${miss// /}" ]; then
    log "PASS F10a-untraced-descendant-detected: children [$kids] untraced under a parent-only attach: [$miss]"
  else
    log "FAIL F10a-untraced-descendant-detected: children [$kids], untraced [$miss]"; fails=$((fails + 1))
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

rm -rf "$DIR/fc"
if [ $fails -eq 0 ]; then log "VERDICT PASS $NCHECK/$NCHECK"; exit 0; fi
log "VERDICT FAIL $fails of $NCHECK failed"
exit 1
