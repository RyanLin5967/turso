# trace.sh -- strace windows for the flush counts (sourced; lane fastest-linux-comp).
#
#   strace_attach OUT PID...   attach `strace -f -C -y` to every PID (and every thread of each), and return only once
#                              /proc shows each task's TracerPid is that strace; refuses after 20 s. Children forked
#                              and threads started after the attach are followed (-f).
#   strace_detach OUT          SIGINT to that strace (it detaches and writes the -c table), wait for it to exit.
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

traced_all() { # traced_all STRACEPID PID... -> 0 when every live task of every PID is traced by STRACEPID
  local st=$1 p t tp
  shift
  for p in "$@"; do
    [ -d "/proc/$p" ] || continue  # exited between enumeration and attach: nothing left to trace
    for t in /proc/"$p"/task/*; do
      [ -e "$t/status" ] || continue
      tp=$(awk '/^TracerPid:/{print $2}' "$t/status" 2>/dev/null) || return 1
      [ -z "$tp" ] && continue  # the task exited while we read it
      [ "$tp" = "$st" ] || return 1
    done
  done
}

strace_attach() {
  local out=$1 p i
  shift
  [ $# -gt 0 ] || { echo "strace_attach: no pids" >&2; return 2; }
  local args=()
  for p in "$@"; do args+=(-p "$p"); done
  strace "${STRACE_OPTS[@]}" -o "$out.strace" "${args[@]}" 2>"$out.strace.err" &
  ST_PID=$!
  for ((i = 0; i < 400; i++)); do
    if traced_all "$ST_PID" "$@"; then
      echo "pids=$* strace_pid=$ST_PID attached_after_polls=$i t0=$(date +%s.%N)" >"$out.window"
      return 0
    fi
    kill -0 "$ST_PID" 2>/dev/null || { echo "strace exited before attaching: $(cat "$out.strace.err")" >&2; return 3; }
    sleep 0.05
  done
  echo "strace did not attach to every task of [$*] within 20 s" >&2
  kill -INT "$ST_PID" 2>/dev/null
  wait "$ST_PID" || true
  return 3
}

strace_detach() {
  local out=$1 rc=0
  echo "t1=$(date +%s.%N)" >>"$out.window"
  kill -INT "$ST_PID"
  wait "$ST_PID" || rc=$?
  echo "strace_rc=$rc" >>"$out.window"
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
