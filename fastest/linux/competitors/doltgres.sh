#!/usr/bin/env bash
# doltgres.sh -- Doltgres (prebuilt darwin-arm64, go1.26.8) for the branch benchmark, native settings (PREREG §4).
#
#   doltgres.sh init  DATA PORT       write DATA/config.yaml (DATA must not exist; *.noindex): data, cfg, auth,
#                                     privilege and branch-control files all INSIDE DATA (the defaults are relative
#                                     to the cwd, and with no config it serves $HOME/doltgres/databases)
#   doltgres.sh start DATA [V1RUN]    run `doltgres -config DATA/config.yaml` with cwd DATA, detached; pid ->
#                                     DATA.ftpid, log -> DATA.log; waits until SELECT 1 answers
#   doltgres.sh seed  DATA ROWS       t(ROWS rows) in database postgres, then dolt_commit so main holds it
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
  /opt/homebrew/bin/python3 -B "$FT_HERE/gen_seed.py" "$ROWS" | psqlc
  psqlc -At -c "SELECT dolt_commit('-Am', 'seed')"
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
