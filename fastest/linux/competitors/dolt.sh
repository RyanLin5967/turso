#!/usr/bin/env bash
# dolt.sh -- Dolt 2.3.5 sql-server (MySQL protocol) for the branch benchmark, native settings (PREREG §4).
#
#   dolt.sh init  DATA PORT         DATA must not exist (*.noindex). DOLT_ROOT_PATH=DATA/root holds the global
#                                   config (never ~/.dolt; metrics and version check off, see common.sh);
#                                   database "bench" in DATA/dbs/bench; cfg in DATA/cfg
#   dolt.sh start DATA [V1RUN]      `dolt sql-server -H 127.0.0.1 -P PORT --data-dir DATA/dbs --doltcfg-dir DATA/cfg
#                                   --max-connections 1100`, cwd DATA, detached; pid -> DATA.ftpid, log -> DATA.log
#   dolt.sh seed  DATA ROWS         bench.t(ROWS rows) through the MariaDB client, then DOLT_COMMIT on main
#   dolt.sh sql   DATA SQL          one statement through the MariaDB client
#   dolt.sh stop  DATA              SIGTERM to the recorded pid only; waits for exit
# Branch ops (specs in ../loadgen/specs): CALL DOLT_CHECKOUT('-b', b) [BranchBench's create], or DOLT_BRANCH + checkout.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
cmd=${1:-}; DATA=${2:-}
[ -n "$cmd" ] && [ -n "$DATA" ] || die "usage: dolt.sh init|start|seed|sql|stop DATA ..."
require_noindex "$DATA"
PIDF=$DATA.ftpid LOG=$DATA.log
port_of() { cat "$DATA/port"; }
my() { "$FT_MARIADB" --protocol=TCP -h 127.0.0.1 -P "$(port_of)" -u root --skip-ssl "$@"; }

case "$cmd" in
init)
  PORT=${3:-}
  [ -n "$PORT" ] || die "usage: dolt.sh init DATA PORT"
  [ -e "$DATA" ] && die "REFUSED: $DATA exists"
  mkdir -p "$DATA/dbs/bench" "$DATA/cfg"
  echo "$PORT" >"$DATA/port"
  dolt_quiet_root "$DATA/root"
  (cd "$DATA/dbs/bench" && "$FT_DOLT" init >/dev/null)
  echo "initialised $DATA port=$PORT (global config: $(cat "$DATA/root/.dolt/config_global.json"))"
  ;;
start)
  V1=${3:--}
  [ -d "$DATA/dbs/bench/.dolt" ] || die "REFUSED: $DATA is not initialised"
  dolt_quiet_root "$DATA/root"
  require_port_free "$(port_of)"
  launch "$PIDF" "$LOG" "$DATA" "$V1" "$FT_DOLT" sql-server -H 127.0.0.1 -P "$(port_of)" --data-dir "$DATA/dbs" \
    --doltcfg-dir "$DATA/cfg" --max-connections 1100 -l warning
  wait_ready "$PIDF" "$DATA/dbs" "$LOG" 120 my -N -e "SELECT 1"
  ;;
seed)
  ROWS=${3:-}
  [ -n "$ROWS" ] || die "usage: dolt.sh seed DATA ROWS"
  alive "$PIDF" "$DATA/dbs" || die "REFUSED: no running server recorded for $DATA"
  /opt/homebrew/bin/python3 -B "$FT_HERE/gen_seed.py" "$ROWS" | my bench
  my -N bench -e "CALL DOLT_COMMIT('-Am', 'seed')" >/dev/null
  echo "seeded bench.t rows=$(my -N bench -e 'SELECT count(*) FROM t') branch=$(my -N bench -e 'SELECT active_branch()')"
  ;;
sql)
  SQL=${3:-}
  [ -n "$SQL" ] || die "usage: dolt.sh sql DATA SQL"
  my -N bench -e "$SQL"
  ;;
stop)
  stop_pid "$PIDF" "$DATA/dbs" TERM 120
  ;;
*) die "unknown command $cmd" ;;
esac
