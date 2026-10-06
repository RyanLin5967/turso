# trace.sh -- strace windows for the flush counts (sourced; lane fastest-linux-comp).
#
#   strace_attach OUT MAIN     attach `strace -f -C -y` to MAIN and every live descendant of MAIN (every thread of
#                              each), and return only once /proc shows each task's TracerPid is that strace;
#                              refuses after 20 s. Children forked and threads started after the attach are
#                              followed (-f). MAIN must be alive (a gone server is never a clean zero). The attach is
#                              COMPLETE only if, once it is proven, no descendant of MAIN is untraced: a child forked
#                              between the enumeration and the seize of MAIN is neither listed nor followed, so the
#                              attach is redone (up to 5 tries, the incomplete ones kept as OUT.try<k>.*) and refuses
#                              if it never completes (review finding 4). MAIN is SIGSTOPped from before the enumeration
#                              until the seize is proven (freeze/thaw; window field frozen=1), so it cannot fork into
#                              that gap at all (second review, finding 3).
#   strace_detach OUT          SIGINT to that strace (it detaches and writes the -c table), wait for it to exit.
#                              Records whether the strace and MAIN were still alive when the detach was requested:
#                              stracecount refuses the window if either was not (review finding 5).
#   strace_mark OUT NAME       stamp NAME=<CLOCK_REALTIME now> (with NAME_mono, clock_pair) into OUT.window inside an
#                              attach: stracecount --part pre|post splits the trace there by each call's -ttt start
#                              stamp (the same clock), so one attach holds the load window and the CHECKPOINT after
#                              it (second review, 2).
#   strace_run OUT CMD...      run CMD under the same strace from its first instruction.
# Each window writes OUT.strace (per-call lines stamped -ttt, then the -c table), OUT.strace.err and OUT.window (the
# window's CLOCK_REALTIME bounds: t0 attach-complete to t1 detach-request, and strace's rc; an attach window's stamps
# are clock pairs, tseize before strace starts and tend after it exits, see clock_pair; a launch window's are from
# `date +%s.%N`).
# kernel.yama.ptrace_scope must be 0 (the workflow sets it): the servers are not strace's descendants.
TRACESET=fsync,fdatasync,sync_file_range,syncfs,sync,msync,copy_file_range,ioctl,openat,openat2,fcntl,pwritev2
TRACESET=$TRACESET,io_submit,io_uring_setup,io_uring_enter,io_uring_register
# Process lineage, so stracecount can map each traced thread id to its process (and that process to a role):
# clone/clone3 everywhere; x86_64 also has the legacy fork/vfork, open and creat entry points (aarch64 has none).
TRACESET=$TRACESET,clone,clone3
[ "$(uname -m)" = x86_64 ] && TRACESET=$TRACESET,open,creat,fork,vfork
STRACE_OPTS=(-f -C -y -ttt -qq -s 160 -e signal=none -e "trace=$TRACESET")
ST_PID=

# clock_pair NAME -> "NAME=<CLOCK_REALTIME> NAME_mono=<CLOCK_MONOTONIC>", both read back to back in one process. The
# window's cuts and strace's -ttt stamps read CLOCK_REALTIME, which a step (NTP, settimeofday) moves; MONOTONIC is
# never stepped and runs at the same rate, so between two pairs the realtime and monotonic deltas differ only by a
# step. stracecount compares them over the attach's whole life, tseize -> t0 [-> tsplit] -> t1 -> tend, and refuses
# the window on a difference over 1 ms (fifth-review re-review, finding 2: comparing call stamps with each other
# cannot see a step before the first call or after the last). No pair (python3 failed) is a refused window.
clock_pair() { python3 -B -c 'import sys, time; r = time.time(); m = time.monotonic(); print("%s=%.9f %s_mono=%.9f" % (sys.argv[1], r, sys.argv[1], m))' "$1"; }

descendants() { # descendants PID -> every live, non-zombie descendant pid of PID, one per line (children of children too)
  local c
  for c in $(ps -o pid=,stat= --ppid "$1" 2>/dev/null | awk '$2 !~ /^Z/ {print $1}'); do
    echo "$c"
    descendants "$c"
  done
}

# task_state STAT_FILE -> the one-letter state of /proc/<pid>[/task/<tid>]/stat (parsed after the last ')': the comm
# field may hold spaces); empty when the task is gone.
task_state() { awk '{ s = $0; sub(/.*\) /, "", s); split(s, a, " "); print a[1] }' "$1" 2>/dev/null; }
# task_st TASKDIR -> "<state letter> <TracerPid>" from ONE read of TASKDIR/status, empty when the task is gone. Reading
# the state and the tracer in two reads let a traced child exit and be reaped between them and look untraced
# (fourth review, finding 3: every one of F10d's 7 incomplete first tries was that).
task_st() { awk '/^State:/ {s = $2} /^TracerPid:/ {t = $2} END {if (s != "") print s, t}' "$1/status" 2>/dev/null; }
is_dead_state() { case $1 in Z|X|x|"") return 0 ;; esac; return 1; }
# dead_task TASKDIR -> 0 for a zombie, a dead task, or one gone: it can no longer issue a syscall, so it needs no
# tracer. A child that exits while the postmaster is frozen stays a zombie (only the postmaster reaps it) and reads
# TracerPid 0 (third review, finding 2: under the freeze it stalled the attach for 20 s, then failed it).
dead_task() { is_dead_state "$(task_state "$1/stat")"; }
# dead_proc PID -> 0 only when EVERY task of PID is dead: a process whose leader exited (pthread_exit) shows the
# leader's Z in /proc/PID/stat while its other threads still run with its fds (fourth review, finding 2).
dead_proc() {
  local t
  [ -d "/proc/$1" ] || return 0
  for t in /proc/"$1"/task/*; do
    [ -e "$t" ] || continue
    dead_task "$t" || return 1
  done
  return 0
}

# task_check TASKDIR STRACEPID -> "dead" (zombie, dead, or the task dir gone), "traced" (TracerPid is STRACEPID), or
# "untraced" for everything else -- INCLUDING a status that could not be read while the task still exists: an
# unreadable task fails closed (fifth review, finding 3: an empty read used to read as dead, or kept the previous
# task's values).
task_check() {
  local s="" tp=""
  read -r s tp < <(task_st "$1") || true
  if [ -z "$s" ]; then
    [ -e "$1" ] && echo untraced || echo dead
    return
  fi
  if is_dead_state "$s"; then echo dead; elif [ "$tp" = "$2" ]; then echo traced; else echo untraced; fi
}

untraced_tasks() { # untraced_tasks STRACEPID PID... -> each live task "<pid>/task/<tid>" of PID... not traced by STRACEPID
  local st=$1 p t
  shift
  for p in "$@"; do
    for t in /proc/"$p"/task/*; do
      [ -e "$t" ] || continue
      [ "$(task_check "$t" "$st")" = untraced ] && echo "${t#/proc/}"
    done
  done
  return 0
}

# fdsync_scan OUT PID... -- OUT.fdsync: every fd of each PID whose open-file flags hold O_DSYNC (0o10000) or __O_SYNC
# (0o4000000) -- the asm-generic values, which x86_64 and aarch64 both use -- as "hit PID FD FLAGS TARGET", and one
# "scanned PID NFDS" line per PID. Run once the attach is proven: an fd opened BEFORE the attach with O_SYNC/O_DSYNC
# makes each of its writes a flush that strace cannot see (writes are not traced), and its openat happened before
# the trace began (review finding 3). Blind spot left: such an fd written and closed between the attach and this
# scan (milliseconds). A PID already gone writes "gone PID". Every fd is read with bash builtins, one at a time, and
# an fd that closes mid-scan is skipped: the first version ran awk over the whole fdinfo glob, and when an fd vanished
# between the glob and awk opening it, awk exited without its END line -- 11 live PG processes (9 checkpointers, 2
# bgwriters) of run 37242277040 have no scan line at all (second review, finding 1). stracecount now requires a
# "scanned" line with at least one fd for every process in the attach roster.
fdsync_scan() {
  local out=$1 p f k v fl n fdd t try ok hits
  shift
  : >"$out.fdsync"
  for p in "$@"; do
    if [ ! -d "/proc/$p" ]; then echo "gone $p" >>"$out.fdsync"; continue; fi
    ok=0
    for ((try = 1; try <= 3; try++)); do
      # The fd table is shared by the threads: read it through the LEADER while it lives, else through another live
      # task, since a leader that called pthread_exit shows an empty fdinfo while its other threads still hold every
      # fd (fifth review, finding 5). The first live task in glob order could be a short-lived thread (re-review,
      # finding 4).
      fdd=
      if ! dead_task "/proc/$p/task/$p"; then
        fdd="/proc/$p/task/$p/fdinfo"
      else
        for t in /proc/"$p"/task/*; do
          [ -e "$t" ] || continue
          dead_task "$t" || { fdd="$t/fdinfo"; break; }
        done
      fi
      [ -n "$fdd" ] || break  # no live task left: exiting
      n=0 hits=
      for f in "$fdd"/*; do
        fl=
        { while read -r k v _; do [ "$k" = "flags:" ] && { fl=$v; break; }; done; } 2>/dev/null <"$f" || continue
        [ -n "$fl" ] || continue
        n=$((n + 1))
        if (((8#$fl & 8#04010000) != 0)); then
          hits+="hit $p ${f##*/} $fl $(readlink "${fdd%/fdinfo}/fd/${f##*/}" 2>/dev/null)"$'\n'
        fi
      done
      # The scan holds only if the task it read through is still alive afterwards: one that exited mid-scan made
      # every later fd read fail and be skipped as "closed" (re-review, finding 4). Then rescan through another.
      if [ -d "$fdd" ] && ! dead_task "${fdd%/fdinfo}"; then ok=1; break; fi
    done
    # A process caught exiting (zombie or dead, its files already closed) holds no fd and can write nothing: it is
    # recorded as exiting, not as a scan of zero fds, and pid_roster leaves it out (third review, finding 8). A live
    # process whose every scan lost its task is "unscanned", which stracecount refuses (no "scanned" line).
    if [ $ok = 1 ]; then
      printf '%s' "$hits" >>"$out.fdsync"
      if [ "$n" = 0 ] && dead_proc "$p"; then echo "exiting $p"; else echo "scanned $p $n"; fi >>"$out.fdsync"
    elif dead_proc "$p"; then
      echo "exiting $p" >>"$out.fdsync"
    else
      echo "unscanned $p (the task read through exited mid-scan, 3 tries)" >>"$out.fdsync"
    fi
  done
}

# pid_roster OUT PID... -- OUT.pids: one line per live task "<tid> <pid> <cmdline>" of each PID at the attach (the
# cmdline with NULs as spaces: PG's process titles, e.g. "postgres: checkpointer"). stracecount maps a traced line's
# tid to its process through this roster plus the clone/fork lines of the window.
pid_roster() {
  local out=$1 p t cl
  shift
  for p in "$@"; do
    dead_proc "$p" && continue  # exiting: no syscall left to attribute
    cl=$(tr '\0' ' ' <"/proc/$p/cmdline" 2>/dev/null) || continue
    for t in /proc/"$p"/task/*; do
      [ -e "$t" ] && echo "${t##*/} $p $cl"
    done
  done >"$out.pids"
}

traced_all() { # traced_all STRACEPID MAIN [PID...] -> 0 when MAIN is alive and every live task of MAIN and each PID is traced by STRACEPID
  local st=$1 main=$2 p t tp
  shift
  [ -d "/proc/$main" ] || return 1  # the server itself must be there (review finding 5: an all-gone list passed)
  for p in "$@"; do
    [ -d "/proc/$p" ] || continue  # a descendant that exited between enumeration and attach: nothing left to trace
    for t in /proc/"$p"/task/*; do
      [ -e "$t" ] || continue
      case $(task_check "$t" "$st") in
        dead|traced) ;;  # zombie, dead or gone while we read it; or traced by us
        *) return 1 ;;   # untraced, or unreadable while it exists (fails closed)
      esac
    done
  done
}

proc_state() { task_state "/proc/$1/stat"; }  # R S D T t Z ...

# freeze MAIN / thaw MAIN: SIGSTOP the server's main process for the enumeration and the seize, so it cannot fork a
# child in between (second review, finding 3: a child forked in that gap and gone before the completeness check was
# never seen). freeze succeeds only once /proc shows MAIN stopped (state T or t); thaw always sends SIGCONT. The
# main's children keep running; PG's backends and auxiliaries do not fork.
freeze() {
  local i st
  kill -STOP "$1" 2>/dev/null || return 1
  for ((i = 0; i < 200; i++)); do
    st=$(proc_state "$1")
    case $st in T|t) return 0 ;; esac
    sleep 0.005
  done
  return 1
}
thaw() { kill -CONT "$1" 2>/dev/null; }

strace_attach() {
  local out=$1 main=$2 try i p miss pids frozen
  [ -n "$main" ] || { echo "strace_attach: no main pid" >&2; return 2; }
  [ -d "/proc/$main" ] || { echo "strace_attach: main pid $main is not running" >&2; return 2; }
  local tseize
  for ((try = 1; try <= 5; try++)); do
    tseize=$(clock_pair tseize)  # before this try's strace starts: the first clock pair of the window (clock_pair)
    frozen=0
    freeze "$main" && frozen=1
    pids="$main $(descendants "$main" | tr '\n' ' ')"
    local args=()
    for p in $pids; do args+=(-p "$p"); done
    strace "${STRACE_OPTS[@]}" -o "$out.strace" "${args[@]}" 2>"$out.strace.err" &
    ST_PID=$!
    for ((i = 0; i < 400; i++)); do
      traced_all "$ST_PID" $pids && break
      kill -0 "$ST_PID" 2>/dev/null || { thaw "$main"; echo "strace exited before attaching: $(cat "$out.strace.err")" >&2; ST_PID=; return 3; }
      sleep 0.05
    done
    thaw "$main"
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
      pids=$(printf '%s\n' $pids $(descendants "$main") | sort -un | tr '\n' ' ')
      fdsync_scan "$out" $pids
      pid_roster "$out" $pids
      echo "main=$main pids=$pids strace_pid=$ST_PID attach_tries=$try frozen=$frozen attached_after_polls=$i $tseize $(clock_pair t0)" >"$out.window"
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
  # t1 is the detach request, unless the window already holds one (only the fire-check's F12 stamps it earlier, with
  # strace_mark OUT t1, so that calls exist after it and the t1 cut is exercised).
  grep -q '^t1=' "$out.window" || clock_pair t1 >>"$out.window"
  main=$(sed -n 's/^main=\([0-9][0-9]*\) .*/\1/p' "$out.window" | head -1)
  kill -0 "$ST_PID" 2>/dev/null || sa=0
  { [ -n "$main" ] && kill -0 "$main" 2>/dev/null; } || ma=0
  kill -INT "$ST_PID" 2>/dev/null
  wait "$ST_PID" || rc=$?
  echo "strace_rc=$rc strace_alive_at_detach=$sa main_alive_at_detach=$ma" >>"$out.window"
  clock_pair tend >>"$out.window"  # after strace exited: the last clock pair of the window (clock_pair)
  ST_PID=
}

strace_run() {
  local out=$1 rc=0
  shift
  echo "cmd=$* t0=$(date +%s.%N)" >"$out.window"
  # The traced command's own stderr goes to OUT.cmd.err (an sh that redirects fd 2 and execs it, traced from the
  # start), so OUT.strace.err holds strace's messages only: a Python DeprecationWarning from the fire-check's F1
  # probe sat in strace.err in 2 of 20 jobs of run 37244177784 and would now refuse the window (third review, 3).
  strace "${STRACE_OPTS[@]}" -o "$out.strace" /bin/sh -c 'exec "$@" 2>"$0"' "$out.cmd.err" "$@" 2>"$out.strace.err" || rc=$?
  { echo "t1=$(date +%s.%N)"; echo "strace_rc=$rc"; } >>"$out.window"
  return $rc
}

strace_mark() { # strace_mark OUT NAME -- "NAME=<now> NAME_mono=<now>" (clock_pair) into the open window's record
  clock_pair "$2" >>"$1.window"
}

window_s() { # window_s OUT -> seconds between t0 and tsplit when the window was split, else t0 and t1
  awk -F'[= ]' '{for (i = 1; i < NF; i++) { if ($i == "t0") a = $(i + 1); if ($i == "t1") b = $(i + 1);
                 if ($i == "tsplit") s = $(i + 1) }}
    END { if (s != "") b = s; printf "%.6f\n", b - a }' "$1.window"
}
