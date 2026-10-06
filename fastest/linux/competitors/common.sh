# common.sh -- shared helpers for the competitor setup scripts (sourced, bash 3.2 compatible).
# Every server: an explicit data dir with a *.noindex component, started detached in its own session with its
# pid recorded at launch, and stopped ONLY by that recorded pid after checking the pid still runs our command.
#
# LINUX PORT (lane fastest-linux-comp; source artie-research frontier/fastest/tools/competitors/common.sh @648ce2929).
# Binaries come from the environment with Linux defaults: FT_PG18 (PGDG postgresql-18), FT_DOLT and FT_DOLTGRES
# (release tarballs fetched by fetch_dolt.sh into FT_BIN), FT_MARIADB (Ubuntu mariadb-client), FT_PY. The V1 and
# C1b launch prefixes are macOS-only until lane fastest-linux-flush lands its shim: launch() refuses them here.

FT_BIN=${FT_BIN:-$HOME/fastest-bin}
FT_PG18=${FT_PG18:-/usr/lib/postgresql/18/bin}
FT_DOLTGRES=${FT_DOLTGRES:-$FT_BIN/doltgres}
FT_DOLT=${FT_DOLT:-$FT_BIN/dolt}
FT_MARIADB=${FT_MARIADB:-/usr/bin/mariadb}
FT_PY=${FT_PY:-python3}
FT_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() { echo "$*" >&2; exit 2; }

# Dolt and Doltgres report usage events to eventsapi.dolthub.com and check for new versions by default. Every Dolt
# or Doltgres process started by these scripts runs with a lane-local DOLT_ROOT_PATH whose global config disables
# both, and with DOLT_DISABLE_EVENT_FLUSH=1, so nothing is sent from here.
dolt_quiet_root() {
  mkdir -p "$1/.dolt"
  [ -f "$1/.dolt/config_global.json" ] ||
    printf '{"metrics.disabled":"true","versioncheck.disabled":"true","user.name":"bench","user.email":"bench@example.invalid"}\n' \
      >"$1/.dolt/config_global.json"
  export DOLT_ROOT_PATH="$1" DOLT_DISABLE_EVENT_FLUSH=1
}

require_noindex() {
  case "$1" in
    /*.noindex|/*.noindex/*) ;;
    *) die "REFUSED: $1 must be an absolute path with a *.noindex component (Spotlight would index it)" ;;
  esac
}

require_port_free() {
  if lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; then
    die "REFUSED: port $1 already has a listener: $(lsof -nP -iTCP:"$1" -sTCP:LISTEN | sed -n 2p)"
  fi
  # Linux: ANY socket bound to the port blocks the server's bind, not only a listener -- e.g. a client that drew it as
  # its ephemeral source port (55432, 55433 and 53306 sit inside 32768-60999). Lane fastest-linux's T3 dry run
  # 37256446468 failed "could not bind ... Address already in use" 80 ms after the listener-only check passed. The
  # workflow also reserves these ports (net.ipv4.ip_local_reserved_ports). Every state counts, TIME-WAIT included
  # (whether SO_REUSEADDR on only the new socket gets past one is not established here: refusing costs a loud
  # failure, never a wrong count -- fourth review, finding 5). On Linux a missing ss refuses; the Mac has no ss.
  if command -v ss >/dev/null 2>&1; then
    # Callers run under set -euo pipefail: a filter that matches nothing must not fail the assignment (grep -v did,
    # and killed every server start of run 37399197723 without a message), and an ss that fails must refuse.
    local s
    s=$(ss -Htan "( sport = :$1 )" 2>&1) || die "REFUSED: ss could not check port $1: $s"
    s=$(printf '%s\n' "$s" | awk 'NF')
    [ -z "$s" ] || die "REFUSED: port $1 is bound by another socket: $(printf '%s\n' "$s" | head -1)"
  elif [ "$(uname -s)" = Linux ]; then
    die "REFUSED: no ss to check port $1 for bound sockets"
  fi
}

# launch PIDFILE LOG CWD [V1RUN|c1b:RUN|-] cmd...  -- detached, own session, cwd CWD; the recorded pid IS the
# server's (perl setsid -> [v1run|c1brun ->] exec cmd keep one pid). '-' means no instrument; "c1b:RUN" traces the
# server for C1b (tools/c1b) instead of counting it with V1.
launch() {
  local pidf=$1 log=$2 cwd=$3 v1=$4
  shift 4
  [ -e "$pidf" ] && die "REFUSED: $pidf exists (a server we started is recorded there): stop it first"
  local pre=()  # expanded as ${pre[@]+...}: bash 3.2 calls an empty array unbound under set -u
  # Linux port: no V1/C1b shim yet, so an instrumented launch refuses rather than running uncounted.
  [ "$v1" = "-" ] || die "REFUSED: instrument '$v1' requested, but the V1/C1b shims are not ported to Linux yet"
  ( cd "$cwd" && exec /usr/bin/perl -MPOSIX -e 'POSIX::setsid(); exec @ARGV or die "exec: $!\n"' -- ${pre[@]+"${pre[@]}"} "$@" ) \
    </dev/null >>"$log" 2>&1 &
  echo $! >"$pidf"
  echo "launched pid $(cat "$pidf") (log $log)"
}

# alive PIDFILE MATCH -- 0 if the recorded pid is alive and its command line contains MATCH.
alive() {
  local pid
  pid=$(cat "$1" 2>/dev/null) || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  case "$(ps -o command= -p "$pid")" in *"$2"*) return 0 ;; *) return 1 ;; esac
}

# stop_pid PIDFILE MATCH SIGNAL [TIMEOUT_S] -- signal the recorded pid only if its command contains MATCH, then
# wait for it to exit (kill -0). Escalates to SIGKILL after the timeout and says so.
stop_pid() {
  local pidf=$1 match=$2 sig=$3 t=${4:-60} pid cmd i
  [ -f "$pidf" ] || { echo "no pid file $pidf: nothing recorded, nothing stopped" >&2; return 2; }
  pid=$(cat "$pidf")
  if ! kill -0 "$pid" 2>/dev/null; then echo "recorded pid $pid already gone"; rm -f "$pidf"; return 0; fi
  cmd=$(ps -o command= -p "$pid")
  case "$cmd" in *"$match"*) ;; *) echo "REFUSED: pid $pid is not ours any more (command: $cmd)" >&2; return 3 ;; esac
  kill -"$sig" "$pid"
  for ((i = 0; i < t * 10; i++)); do
    kill -0 "$pid" 2>/dev/null || { rm -f "$pidf"; echo "stopped pid $pid ($sig)"; return 0; }
    sleep 0.1
  done
  echo "pid $pid still alive ${t}s after $sig: sending KILL" >&2
  kill -KILL "$pid"
  sleep 1
  kill -0 "$pid" 2>/dev/null && { echo "pid $pid survived KILL" >&2; return 4; }
  rm -f "$pidf"
  return 0
}

# wait_ready PIDFILE MATCH LOG TIMEOUT_S CHECKCMD... -- until CHECKCMD succeeds; fail fast if the server died.
wait_ready() {
  local pidf=$1 match=$2 log=$3 t=$4 i
  shift 4
  for ((i = 0; i < t * 4; i++)); do
    "$@" >/dev/null 2>&1 && { echo "ready after ~$((i / 4)) s"; return 0; }
    alive "$pidf" "$match" || { echo "server died during start; log tail:" >&2; tail -20 "$log" >&2; return 3; }
    sleep 0.25
  done
  echo "not ready after ${t}s; log tail:" >&2
  tail -20 "$log" >&2
  return 3
}
