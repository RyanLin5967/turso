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
#   F4 blind spot: dd oflag=dsync. Expect verdict INCOMPLETE (O_DSYNC open), never ok.
#   F5 blind spot: fio --ioengine=io_uring --fsync=1. Expect verdict INCOMPLETE (io_uring), never ok.
#   F3b the F3 trace read WITHOUT the attach proof: a table-less, call-less window must then be REFUSED.
# Exit 0 only if all six pass; the verdict line is the last line of OUT/firecheck.txt.
set -uo pipefail
OUT=${1:?usage: firecheck_strace.sh OUT DIR}
DIR=${2:?usage: firecheck_strace.sh OUT DIR}
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/trace.sh"
SC="$HERE/stracecount.py"
mkdir -p "$OUT" "$DIR/fc"
fails=0
log() { echo "$*" | tee -a "$OUT/firecheck.txt"; }
: >"$OUT/firecheck.txt"
log "# strace fire-check $(date -u +%FT%TZ) $(strace -V | head -1) kernel=$(uname -r) dir=$DIR fstype=$(findmnt -n -o FSTYPE -T "$DIR")"

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
PY

check() { # check NAME JSON EXPR -- EXPR is a python expression over r (the count JSON)
  local name=$1 json=$2 expr=$3
  if python3 -c "import json,sys; r=json.load(open(sys.argv[1])); sys.exit(0 if ($expr) else 1)" "$json"; then
    log "PASS $name: $expr"
  else
    log "FAIL $name: $expr | got $(python3 -c "import json,sys; r=json.load(open(sys.argv[1])); print({k: r[k] for k in ('verdict','flush_by_syscall','copy_file_range_calls','ficlone','osync_opens','io_uring','problems')})" "$json")"
    fails=$((fails + 1))
  fi
}
EXACT='r["verdict"]=="ok" and r["flush_by_syscall"]=={"fsync":6,"fdatasync":2,"syncfs":0,"sync":1,"msync_sync":1} and r["flushes"]==10 and r["copy_file_range_calls"]==1 and r["ficlone"]==1'

# F1
rm -f "$DIR"/fc/probe.*
strace_run "$OUT/f1" python3 -B "$OUT/probe.py" "$DIR/fc" - || log "f1 strace rc=$?"
python3 "$SC" count "$OUT/f1.strace" --extra "$OUT/f1.strace.err" --root "$DIR" --window "$OUT/f1.window" >"$OUT/f1.json"
check F1-launch "$OUT/f1.json" "$EXACT"

# F2 and F3 share one detached probe: idle attach first (never triggered), then the triggered attach.
rm -f "$DIR"/fc/probe.* "$DIR/fc/go"
( exec /usr/bin/perl -MPOSIX -e 'POSIX::setsid(); exec @ARGV or die' -- python3 -B "$OUT/probe.py" "$DIR/fc" "$DIR/fc/go" ) \
  </dev/null >"$OUT/probe.log" 2>&1 &
PP=$!
sleep 0.5
if strace_attach "$OUT/f3" "$PP"; then
  sleep 2
  strace_detach "$OUT/f3"
  python3 "$SC" count "$OUT/f3.strace" --extra "$OUT/f3.strace.err" --root "$DIR" --window "$OUT/f3.window" >"$OUT/f3.json"
  check F3-idle-attach "$OUT/f3.json" 'r["verdict"]=="ok" and r["flushes"]==0 and r["summary_found"]'
  log "F3 note: empty_window=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['empty_window'])" "$OUT/f3.json") strace_rc=$(grep -o 'strace_rc=[0-9]*' "$OUT/f3.window")"
  # F3b: the same table-less window WITHOUT the attach proof must not read as zero.
  python3 "$SC" count "$OUT/f3.strace" --extra "$OUT/f3.strace.err" --root "$DIR" >"$OUT/f3b.json"
  check F3b-unproven-empty-refused "$OUT/f3b.json" \
    'r["verdict"].startswith("REFUSED") if not r["lines"] and not r["summary"] else r["verdict"]=="ok"'
else
  log "FAIL F3-idle-attach: strace_attach failed"; fails=$((fails + 1))
fi
if strace_attach "$OUT/f2" "$PP"; then
  touch "$DIR/fc/go"
  for ((i = 0; i < 600; i++)); do [ -e "$DIR/fc/probe.done" ] && break; sleep 0.05; done
  sleep 0.2
  strace_detach "$OUT/f2"
  python3 "$SC" count "$OUT/f2.strace" --extra "$OUT/f2.strace.err" --root "$DIR" --window "$OUT/f2.window" >"$OUT/f2.json"
  check F2-attach "$OUT/f2.json" "$EXACT"
else
  log "FAIL F2-attach: strace_attach failed"; fails=$((fails + 1))
fi
kill "$PP" 2>/dev/null; wait "$PP" 2>/dev/null

# F4
strace_run "$OUT/f4" dd if=/dev/zero of="$DIR/fc/dsync.dat" bs=4k count=2 oflag=dsync status=none || true
python3 "$SC" count "$OUT/f4.strace" --extra "$OUT/f4.strace.err" --root "$DIR" --window "$OUT/f4.window" >"$OUT/f4.json"
check F4-osync-refused "$OUT/f4.json" 'r["verdict"].startswith("INCOMPLETE") and r["osync_opens"]>=1'

# F5
strace_run "$OUT/f5" fio --name=fc --filename="$DIR/fc/uring.dat" --ioengine=io_uring --rw=write --bs=4k \
  --size=16k --fsync=1 --output="$OUT/f5.fio.txt" || true
python3 "$SC" count "$OUT/f5.strace" --extra "$OUT/f5.strace.err" --root "$DIR" --window "$OUT/f5.window" >"$OUT/f5.json"
check F5-io_uring-refused "$OUT/f5.json" 'r["verdict"].startswith("INCOMPLETE") and r["io_uring"]>=1'

rm -rf "$DIR/fc"
if [ $fails -eq 0 ]; then log "VERDICT PASS 6/6"; exit 0; fi
log "VERDICT FAIL $fails of 6 failed"
exit 1
