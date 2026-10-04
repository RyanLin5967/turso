# trace.sh -- strace windows for the flush counts (sourced; lane fastest-linux-comp).
#
#   strace_attach OUT MAIN     attach `strace -f -C -y` to MAIN and every live descendant of MAIN (every thread of
#                              each), and return only once /proc shows each task's TracerPid is that strace;
#                              refuses after 20 s. Children forked and threads started after the attach are
#                              followed (-f). MAIN must be alive (a gone server is never a clean zero). The attach is
#                              COMPLETE only if, once it is proven, no descendant of MAIN is untraced: a child forked
#                              between the enumeration and the seize of MAIN is neither listed nor followed, so the
#                              attach is redone (up to 5 tries, the incomplete ones kept as OUT.try<k>.*) and refuses
#                              if it never completes (review finding 4).
#   strace_detach OUT          SIGINT to that strace (it detaches and writes the -c table), wait for it to exit.
#                              Records whether the strace and MAIN were still alive when the detach was requested:
#                              stracecount refuses the window if either was not (review finding 5).
#   strace_run OUT CMD...      run CMD under the same strace from its first instruction.
# Each window writes OUT.strace (per-call lines, then the -c table), OUT.strace.err and OUT.window (the window's
# CLOCK_REALTIME bounds, from `date +%s.%N`: attach-complete to detach-request, and strace's rc).
# kernel.yama.ptrace_scope must be 0 (the workflow sets it): the servers are not strace's descendants.
TRACESET=fsync,fdatasync,sync_file_range,syncfs,sync,msync,copy_file_range,ioctl,openat,fcntl,pwritev2,io_submit
TRACESET=$TRACESET,io_uring_setup,io_uring_enter,io_uring_register
# x86_64 still has the legacy open/creat entry points; aarch64 has only openat.
[ "$(uname -m)" = x86_64 ] && TRACESET=$TRACESET,open,creat
STRACE_OPTS=(-f -C -y -qq -s 160 -e signal=none -e "trace=$TRACESET")
ST_PID=

descendants() { # descendants PID -> every live, non-zombie descendant pid of PID, one per line (children of children too)
  local c
  for c in $(ps -o pid=,stat= --ppid "$1" 2>/dev/null | awk '$2 !~ /^Z/ {print $1}'); do
    echo "$c"
    descendants "$c"
  done
}

untraced_tasks() { # untraced_tasks STRACEPID PID... -> each live task "<pid>/task/<tid>" of PID... not traced by STRACEPID
  local st=$1 p t tp
  shift
  for p in "$@"; do
    for t in /proc/"$p"/task/*; do
      [ -e "$t/status" ] || continue
      tp=$(awk '/^TracerPid:/{print $2}' "$t/status" 2>/dev/null)
      [ -z "$tp" ] || [ "$tp" = "$st" ] || echo "${t#/proc/}"
    done
  done
}

traced_all() { # traced_all STRACEPID MAIN [PID...] -> 0 when MAIN is alive and every live task of MAIN and each PID is traced by STRACEPID
  local st=$1 main=$2 p t tp
  shift
  [ -d "/proc/$main" ] || return 1  # the server itself must be there (review finding 5: an all-gone list passed)
  for p in "$@"; do
    [ -d "/proc/$p" ] || continue  # a descendant that exited between enumeration and attach: nothing left to trace
    for t in /proc/"$p"/task/*; do
      [ -e "$t/status" ] || continue
      tp=$(awk '/^TracerPid:/{print $2}' "$t/status" 2>/dev/null) || return 1
      [ -z "$tp" ] && continue  # the task exited while we read it
      [ "$tp" = "$st" ] || return 1
    done
  done
}

strace_attach() {
  local out=$1 main=$2 try i p miss pids
  [ -n "$main" ] || { echo "strace_attach: no main pid" >&2; return 2; }
  [ -d "/proc/$main" ] || { echo "strace_attach: main pid $main is not running" >&2; return 2; }
  for ((try = 1; try <= 5; try++)); do
    pids="$main $(descendants "$main" | tr '\n' ' ')"
    local args=()
    for p in $pids; do args+=(-p "$p"); done
    strace "${STRACE_OPTS[@]}" -o "$out.strace" "${args[@]}" 2>"$out.strace.err" &
    ST_PID=$!
    for ((i = 0; i < 400; i++)); do
      traced_all "$ST_PID" $pids && break
      kill -0 "$ST_PID" 2>/dev/null || { echo "strace exited before attaching: $(cat "$out.strace.err")" >&2; ST_PID=; return 3; }
      sleep 0.05
    done
    if [ $i -ge 400 ]; then
      echo "strace did not attach to every task of [$pids] within 20 s" >&2
      kill -INT "$ST_PID" 2>/dev/null
      wait "$ST_PID" || true
      ST_PID=
      return 3
    fi
    # Complete only if no descendant escaped: anything forked after MAIN was seized is followed (-f, the kernel
    # attaches it at fork), so an untraced descendant now was forked between the enumeration and the seize.
    miss=$(untraced_tasks "$ST_PID" $(descendants "$main") | tr '\n' ' ')
    if [ -z "${miss// /}" ]; then
      echo "main=$main pids=$pids strace_pid=$ST_PID attach_tries=$try attached_after_polls=$i t0=$(date +%s.%N)" >"$out.window"
      return 0
    fi
    echo "strace_attach: try $try incomplete, untraced descendants of $main: $miss" >&2
    echo "main=$main pids=$pids strace_pid=$ST_PID INCOMPLETE untraced=[$miss]" >"$out.try$try.window"
    kill -INT "$ST_PID" 2>/dev/null
    wait "$ST_PID" || true
    ST_PID=
    mv -f "$out.strace" "$out.try$try.strace" 2>/dev/null
    mv -f "$out.strace.err" "$out.try$try.strace.err" 2>/dev/null
  done
  echo "strace_attach: never complete in 5 tries (untraced descendants of $main)" >&2
  return 4
}

strace_detach() {
  local out=$1 rc=0 sa=1 ma=1 main
  echo "t1=$(date +%s.%N)" >>"$out.window"
  main=$(sed -n 's/^main=\([0-9][0-9]*\) .*/\1/p' "$out.window" | head -1)
  kill -0 "$ST_PID" 2>/dev/null || sa=0
  { [ -n "$main" ] && kill -0 "$main" 2>/dev/null; } || ma=0
  kill -INT "$ST_PID" 2>/dev/null
  wait "$ST_PID" || rc=$?
  echo "strace_rc=$rc strace_alive_at_detach=$sa main_alive_at_detach=$ma" >>"$out.window"
  ST_PID=
}

strace_run() {
  local out=$1 rc=0
  shift
  echo "cmd=$* t0=$(date +%s.%N)" >"$out.window"
  strace "${STRACE_OPTS[@]}" -o "$out.strace" "$@" 2>"$out.strace.err" || rc=$?
  { echo "t1=$(date +%s.%N)"; echo "strace_rc=$rc"; } >>"$out.window"
  return $rc
}

window_s() { # window_s OUT -> seconds between t0 and t1
  awk -F'[= ]' '{for (i = 1; i < NF; i++) { if ($i == "t0") a = $(i + 1); if ($i == "t1") b = $(i + 1) }}
    END { printf "%.6f\n", b - a }' "$1.window"
}
