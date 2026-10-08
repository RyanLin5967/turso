#!/usr/bin/env bash
# doltgres.sh -- Doltgres (prebuilt darwin-arm64, go1.26.8) for the branch benchmark, native settings (PREREG §4).
# LINUX PORT (lane fastest-linux-comp; source artie-research frontier/fastest/tools/competitors/doltgres.sh
# @648ce2929): unchanged except that the binaries and python come from common.sh (FT_DOLTGRES = the registered (versions.tsv)
# doltgresql-linux-<arch> release tarball, sha256-checked by fetch_dolt.sh; psql from PGDG postgresql-18).
#
#   doltgres.sh init  DATA PORT       write DATA/config.yaml (DATA must not exist; *.noindex): data, cfg, auth,
#                                     privilege and branch-control files all INSIDE DATA (the defaults are relative
#                                     to the cwd, and with no config it serves $HOME/doltgres/databases)
#   doltgres.sh start DATA [V1RUN]    run `doltgres -config DATA/config.yaml` with cwd DATA, detached; pid ->
#                                     DATA.ftpid, log -> DATA.log; waits until SELECT 1 answers
#   doltgres.sh seed  DATA ROWS [AGE] t(ROWS rows) in database postgres, dolt_commit; AGE>0: aged, dolt_commit, dolt_gc
#   doltgres.sh sql   DATA SQL        one statement through psql
#   doltgres.sh stop  DATA            SIGTERM to the recorded pid only; waits for exit
# Branch ops (specs in ../loadgen/specs): dolt_checkout('-b', b) [BranchBench's create, includes the switch], or
# dolt_branch(b) + dolt_checkout(b). User postgres / password "password" (the Doltgres default).
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
cmd=${1:-}; DATA=${2:-}
[ -n "$cmd" ] && [ -n "$DATA" ] || die "usage: doltgres.sh init|start|seed|sql|stop DATA ..."
require_noindex "$DATA"
PIDF=$DATA.ftpid LOG=$DATA.log CONF=$DATA/config.yaml
port_of() { sed -n 's/^  port: \([0-9]*\).*/\1/p' "$CONF" | head -1; }
psqlc() { PGPASSWORD=password "$FT_PG18/psql" -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$(port_of)" -U postgres -d postgres "$@"; }

case "$cmd" in
init)
  PORT=${3:-}
  [ -n "$PORT" ] || die "usage: doltgres.sh init DATA PORT"
  [ -e "$DATA" ] && die "REFUSED: $DATA exists"
  mkdir -p "$DATA/databases" "$DATA/doltcfg"
  dolt_quiet_root "$DATA/root"
  cat >"$CONF" <<EOF
log_level: warning
user:
  name: postgres
  password: password
listener:
  host: 127.0.0.1
  port: $PORT
  read_timeout_millis: 28800000
  write_timeout_millis: 28800000
data_dir: $DATA/databases
cfg_dir: $DATA/doltcfg
privilege_file: $DATA/doltcfg/privileges.db
auth_file: $DATA/doltcfg/auth.db
branch_control_file: $DATA/doltcfg/branch_control.db
EOF
  echo "initialised $DATA port=$PORT ($(cd "$DATA" && "$FT_DOLTGRES" -version 2>&1 | head -1))"
  ;;
start)
  V1=${3:--}
  [ -f "$CONF" ] || die "REFUSED: $DATA has no config.yaml (init first)"
  require_port_free "$(port_of)"
  dolt_quiet_root "$DATA/root"
  launch "$PIDF" "$LOG" "$DATA" "$V1" "$FT_DOLTGRES" -config "$CONF"
  wait_ready "$PIDF" "$CONF" "$LOG" 120 psqlc -At -c "SELECT 1"
  ;;
seed)
  ROWS=${3:-}
  [ -n "$ROWS" ] || die "usage: doltgres.sh seed DATA ROWS"
  alive "$PIDF" "$CONF" || die "REFUSED: no running server recorded for $DATA"
  AGE=${4:-0}
  "$FT_PY" -B "$FT_HERE/gen_seed.py" sql --rows "$ROWS" | psqlc
  psqlc -At -c "SELECT dolt_commit('-Am', 'seed')"
  # Aged parent (gate-6 review, t3run item 4; amendment 52): AGE single-row UPDATEs, each autocommitted, then the
  # documented maintenance: dolt_commit, then dolt_gc.
  if [ "$AGE" -gt 0 ]; then
    "$FT_PY" -B "$FT_HERE/gen_seed.py" age --rows "$ROWS" --updates "$AGE" | psqlc
    psqlc -At -c "SELECT dolt_commit('-am', 'age')"
    # dolt_gc may end the calling session, so success is checked by a new connection afterwards; a failed GC fails
    # the seed (the recorded maintenance must be what ran).
    psqlc -At -c "SELECT dolt_gc()" >"$DATA.gc.txt" 2>&1 || true
    psqlc -At -c "SELECT 1" >/dev/null || die "REFUSED: the server did not answer after dolt_gc ($(tail -1 "$DATA.gc.txt"))"
    # the session it ends reads as a closed connection; any other error is a failed GC
    grep -viE 'closed the connection|terminat|connection to server|lost' "$DATA.gc.txt" | grep -qi 'error' &&
      die "REFUSED: dolt_gc failed: $(tail -1 "$DATA.gc.txt")"
    echo "maintenance: dolt_commit seed; aged $AGE; dolt_commit age; dolt_gc"
  else
    echo "maintenance: dolt_commit seed"
  fi
  echo "seeded t rows=$(psqlc -At -c 'SELECT count(*) FROM t') branch=$(psqlc -At -c 'SELECT active_branch()')"
  ;;
sql)
  SQL=${3:-}
  [ -n "$SQL" ] || die "usage: doltgres.sh sql DATA SQL"
  psqlc -At -c "$SQL"
  ;;
stop)
  stop_pid "$PIDF" "$CONF" TERM 120
  ;;
*) die "unknown command $cmd" ;;
esac
