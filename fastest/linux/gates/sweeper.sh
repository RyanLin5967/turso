# sweeper.sh -- the ONE tracer sweeper every timed run uses (sourced; bash 5+, for $EPOCHREALTIME; Linux /proc).
#
# The lead's ruling on comp review 1 MED 12: "SAME INSTRUMENT FOR EVERY SYSTEM. Put the sweeper in one shared script
# that runs identically in every timed run, and record sweeper_cpu_s in every timed.json." The competitors' timed runs
# (competitors/trace.sh timed_run) and ours (t3/t3run.sh, wired by lane fastest-linux) source this file; until both run
# it, no ours-vs-competitor timed ratio credits. Its record is read by gates/sweeper_record.py (one reader too).
#
#   sweeper_start OUT CMDPID [SRVPID]   start the sweeper: a subshell reniced to 19 that sweeps the TracerPid of every
#                                       task of CMDPID's process tree (role cmd) and of SRVPID's (role srv; empty or
#                                       absent: none) at the start, every SWEEP_PERIOD_S while CMDPID lives, and once
#                                       more after it is gone, appending to OUT.tracer.tsv. Sets SWEEPER_PID.
#   sweeper_wait                        wait for that subshell (it ends on its own once CMDPID is gone and reaped).
#
# OUT.tracer.tsv, in order:
#   roles cmd=CMDPID srv=SRVPID|none tree=children|none        tree=none: /proc/*/task/*/children unreadable
#   sweeper pid=P nice=N clk_tck=K                              the sweeper subshell, its nice after the renice
#   TRACED PHASE ROLE pid tid tracerpid                         one per traced task a sweep found
#   SWEEP PHASE <EPOCHREALTIME> cmd=N:MAX srv=N:MAX cpu=T       tasks read, largest TracerPid, and the sweeper's own
#                                                               utime+stime in clock ticks ('?' when unreadable)
# PHASE is start, mid or end. Builtins only inside the loop (no fork per sweep); each task's status file is read once,
# whole (a read loop stopping at TracerPid regenerated the seq_file about 8 times a task: comp review 1 MED 12). The
# period is 0.1 s; a reader's gap bound (competitors/timedrun.py GAP_S 0.25 s) must leave room for a sweep's own time.
# Not pinned to a CPU: the loads it runs beside are not pinned either, so a CPU of its own would not keep it off them.

SWEEP_PERIOD_S=0.1
TRACERPID_RE=$'(^|\n)TracerPid:[[:space:]]*([^[:space:]]+)'

# proc_tree PID -> appends PID and every live descendant to the array TREE (each task's /proc/PID/task/TID/children)
proc_tree() {
  local p=$1 t c
  local -a kids
  [ -d "/proc/$p" ] || return 0
  TREE+=("$p")
  for t in /proc/"$p"/task/*; do
    kids=()
    { read -r -a kids <"$t/children"; } 2>/dev/null || true
    for c in ${kids[@]+"${kids[@]}"}; do proc_tree "$c"; done
  done
}

# sweeper_cpu -> SWEEPER_CPU: this shell's own utime+stime in clock ticks (fields 14 and 15 of its stat; after
# "pid (comm) ", state is index 0), or '?' when unreadable (a reader refuses it rather than read a made-up 0)
sweeper_cpu() {
  local st=""
  local -a sf
  { IFS= read -r -d '' st <"/proc/$BASHPID/stat"; } 2>/dev/null
  read -r -a sf <<<"${st##*) }"
  SWEEPER_CPU="?"
  [[ ${sf[11]:-} =~ ^[0-9]+$ && ${sf[12]:-} =~ ^[0-9]+$ ]] && SWEEPER_CPU=$((sf[11] + sf[12]))
}

# tracer_sweep PHASE CMDPID SRVPID -> its TRACED lines and one SWEEP line
tracer_sweep() {
  local ph=$1 role pid p t v n m st out=""
  for role in cmd srv; do
    if [ "$role" = cmd ]; then pid=$2; else pid=$3; fi
    n=0 m=0
    TREE=()
    [ -n "$pid" ] && proc_tree "$pid"
    for p in ${TREE[@]+"${TREE[@]}"}; do
      for t in /proc/"$p"/task/*; do
        [ -e "$t" ] || continue
        v="" st=""
        { IFS= read -r -d '' st <"$t/status"; } 2>/dev/null  # to EOF (no NUL in it): status 1, st holds the file
        [[ $st =~ $TRACERPID_RE ]] && v=${BASH_REMATCH[2]}
        [ -n "$v" ] || continue  # the task exited between the listing and the read
        n=$((n + 1))
        if [ "$v" != 0 ]; then
          echo "TRACED $ph $role $p ${t##*/} $v"
          if [[ $v =~ ^[0-9]+$ ]]; then [ "$v" -gt "$m" ] && m=$v; else m=1; fi  # non-numeric still marks it
        fi
      done
    done
    out+=" $role=$n:$m"
  done
  sweeper_cpu
  echo "SWEEP $ph $EPOCHREALTIME$out cpu=$SWEEPER_CPU"
}

sweeper_start() {
  local out=$1 pid=$2 spid=${3:-} tree=children clk slp=""
  [ -r "/proc/$BASHPID/task/$BASHPID/children" ] || tree=none
  clk=$(getconf CLK_TCK 2>/dev/null)
  # a FIFO read with a timeout sleeps without a fork; without the FIFO, sleep(1)
  if mkfifo "$out.slp" 2>/dev/null; then exec {slp}<>"$out.slp"; rm -f "$out.slp"; fi
  SWEEPER_SLP=$slp
  (
    renice -n 19 -p "$BASHPID" >/dev/null 2>&1  # a failure leaves nice 0 in the header, which the reader refuses
    local nv="?" st=""
    local -a sf
    { IFS= read -r -d '' st <"/proc/$BASHPID/stat"; } 2>/dev/null
    read -r -a sf <<<"${st##*) }"
    [[ ${sf[16]:-} =~ ^-?[0-9]+$ ]] && nv=${sf[16]}  # field 19, nice
    { echo "roles cmd=$pid srv=${spid:-none} tree=$tree"; echo "sweeper pid=$BASHPID nice=$nv clk_tck=${clk:-?}"
      tracer_sweep start "$pid" "$spid"; } >"$out.tracer.tsv"
    while kill -0 "$pid" 2>/dev/null; do
      tracer_sweep mid "$pid" "$spid" >>"$out.tracer.tsv"
      if [ -n "$slp" ]; then read -r -t "$SWEEP_PERIOD_S" -u "$slp" _ 2>/dev/null || true; else sleep "$SWEEP_PERIOD_S"; fi
    done
    tracer_sweep end "" "$spid" >>"$out.tracer.tsv"  # CMDPID gone: reaped by its parent's wait
  ) &
  SWEEPER_PID=$!
}

sweeper_wait() {
  wait "$SWEEPER_PID"
  if [ -n "${SWEEPER_SLP:-}" ]; then exec {SWEEPER_SLP}>&-; fi
  SWEEPER_SLP=""
}
