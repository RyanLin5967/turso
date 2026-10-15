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
  # Each stream's sha256 is recorded by the process that piped it (DATA.seed-{sql,age}.sha256; lead review MED 3).
  "$FT_PY" -B "$FT_HERE/gen_seed.py" sql --rows "$ROWS" --digest-out "$DATA.seed-sql.sha256" | psqlc
  psqlc -At -c "SELECT dolt_commit('-Am', 'seed')"
  # Aged parent (gate-6 review, t3run item 4; amendment 52): AGE single-row UPDATEs, each autocommitted (AGE=0 pipes
  # an empty stream, whose digest is recorded too), then the documented maintenance: dolt_commit, then dolt_gc.
  "$FT_PY" -B "$FT_HERE/gen_seed.py" age --rows "$ROWS" --updates "$AGE" --digest-out "$DATA.seed-age.sha256" | psqlc
  # The maintenance runs whether or not the parent was aged (lead review 62430d8bf..b49fb656a MED 10): the age commit
  # (--allow-empty: nothing changed at AGE=0), then dolt_gc; the server's GC-related settings are recorded beside it.
  psqlc -At -c "SELECT dolt_commit('-am', 'age', '--allow-empty')"
  psqlc -At -c "SELECT name, setting FROM pg_settings WHERE name LIKE '%gc%'" >"$DATA.gc-settings.txt" 2>&1 ||
    echo "rc=$? (not read)" >>"$DATA.gc-settings.txt"
  # dolt_gc, judged by an allowlist (lead review 62430d8bf..b49fb656a MED 9: the old check exempted any error text
  # matching lost|terminat, which hides real errors): client rc 0, its output exactly the status (0 or {0}), and no
  # panic in the server log written since the CALL (fthelp.py gcverdict); the store size on both sides of it.
  gc_before=$(du -sB1 "$DATA/databases" | cut -f1)
  gc_log0=$(stat -c %s "$LOG")
  gcrc=0
  psqlc -At -c "SELECT dolt_gc()" >"$DATA.gc.txt" 2>&1 || gcrc=$?
  tail -c +$((gc_log0 + 1)) "$LOG" >"$DATA.gc.log"
  "$FT_PY" -B "$FT_HERE/fthelp.py" gcverdict "$gcrc" "$DATA.gc.txt" "$DATA.gc.log" >"$DATA.gc.verdict" ||
    die "REFUSED: dolt_gc: $(cat "$DATA.gc.verdict")"
  echo "gc_store_bytes before=$gc_before after=$(du -sB1 "$DATA/databases" | cut -f1)"
  echo "maintenance: dolt_commit seed; aged $AGE; dolt_commit age; dolt_gc"
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
