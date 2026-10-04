#!/usr/bin/env bash
# pg18.sh -- PostgreSQL 18.6 (Homebrew postgresql@18) for the branch benchmark. PREREG §4 settings.
#
# LINUX PORT (lane fastest-linux-comp; source artie-research frontier/fastest/tools/competitors/pg18.sh @648ce2929):
# PGDG postgresql-18 at FT_PG18 (default /usr/lib/postgresql/18/bin). fsync_writethrough does not exist on Linux
# (PG refuses it), and on Linux fsync/fdatasync ARE the device flush, so MODE d2 here sets wal_sync_method=fdatasync
# (also Linux's default). Consequence: on Linux `default` differs from `d2` ONLY in file_copy_method (copy vs clone);
# its durability is already D2. file_copy_method=clone is copy_file_range(2) on Linux: a reflink on XFS/btrfs when
# the kernel can, a silent byte copy when it cannot, which is why the driver proves the clone from shared extents.
#
#   pg18.sh init  DATA MODE PORT      initdb into DATA (must not exist; *.noindex), MODE = d2 | default | d1clone
#   pg18.sh start DATA [V1RUN]        exec `postgres -D DATA` directly (never pg_ctl: its /bin/sh would strip the
#                                     V1 shim), detached; pid -> DATA.ftpid, log -> DATA.log; waits until ready
#   pg18.sh seed  DATA ROWS           CREATE DATABASE p (the template) with t(ROWS rows), VACUUM ANALYZE, CHECKPOINT
#   pg18.sh sql   DATA DB SQL         one statement through psql (prints rows unaligned)
#   pg18.sh stop  DATA                SIGINT (fast shutdown) to the recorded pid only; waits for exit
#
# MODE d2      : wal_sync_method=fsync_writethrough (F_FULLFSYNC), fsync=on, full_page_writes=on,
#                synchronous_commit=on, file_copy_method=clone  (PREREG §4 "Competitor D2 settings")
# MODE default : PostgreSQL's own defaults (wal_sync_method=open_datasync = D1 on macOS, file_copy_method=copy)
# MODE d1clone : defaults plus file_copy_method=clone (isolates durability from the copy method; report only)
# All modes: listen 127.0.0.1:PORT, no Unix socket (the scratch path exceeds sun_path), max_connections=1100
# (C up to 1024 plus spare), trust auth for user postgres. Branch op: CREATE DATABASE b TEMPLATE p STRATEGY=FILE_COPY.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
cmd=${1:-}; DATA=${2:-}
[ -n "$cmd" ] && [ -n "$DATA" ] || die "usage: pg18.sh init|start|seed|sql|stop DATA ..."
require_noindex "$DATA"
PIDF=$DATA.ftpid LOG=$DATA.log
port_of() { sed -n 's/^port = \([0-9]*\).*/\1/p' "$DATA/postgresql.conf" | tail -1; }
psqlc() { "$FT_PG18/psql" -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$(port_of)" -U postgres "$@"; }

case "$cmd" in
init)
  MODE=${3:-}; PORT=${4:-}
  [ -n "$MODE" ] && [ -n "$PORT" ] || die "usage: pg18.sh init DATA MODE PORT"
  [ -e "$DATA" ] && die "REFUSED: $DATA exists"
  case "$MODE" in d2|default|d1clone) ;; *) die "MODE must be d2, default or d1clone" ;; esac
  mkdir -p "$(dirname "$DATA")"
  "$FT_PG18/initdb" -D "$DATA" -U postgres -A trust --encoding=UTF8 --locale=C >"$DATA.initdb.log" 2>&1 ||
    { tail -20 "$DATA.initdb.log" >&2; die "initdb failed"; }
  {
    echo ""
    echo "# ---- fastest-tools pg18.sh, mode $MODE ----"
    echo "port = $PORT"
    echo "listen_addresses = '127.0.0.1'"
    echo "unix_socket_directories = ''"
    echo "max_connections = 1100"
    if [ "$MODE" = d2 ]; then
      echo "wal_sync_method = fdatasync"  # Linux port: fsync_writethrough is macOS/Windows only
      echo "fsync = on"
      echo "full_page_writes = on"
      echo "synchronous_commit = on"
      echo "file_copy_method = clone"
    elif [ "$MODE" = d1clone ]; then
      echo "file_copy_method = clone"
    fi
  } >>"$DATA/postgresql.conf"
  echo "$MODE" >"$DATA.ftmode"
  echo "initialised $DATA mode=$MODE port=$PORT"
  ;;
start)
  V1=${3:--}
  [ -f "$DATA/postgresql.conf" ] || die "REFUSED: $DATA is not an initialised data dir"
  require_port_free "$(port_of)"
  launch "$PIDF" "$LOG" "$(dirname "$DATA")" "$V1" "$FT_PG18/postgres" -D "$DATA"
  wait_ready "$PIDF" "$DATA" "$LOG" 60 "$FT_PG18/pg_isready" -q -h 127.0.0.1 -p "$(port_of)" -t 1
  echo "settings: $(psqlc -d postgres -At -c "SELECT string_agg(name || '=' || setting, ' ' ORDER BY name) FROM pg_settings WHERE name IN ('wal_sync_method','fsync','full_page_writes','synchronous_commit','file_copy_method','max_connections','server_version','io_method','checkpoint_timeout','shared_buffers','log_min_messages')")"
  ;;
seed)
  ROWS=${3:-}
  [ -n "$ROWS" ] || die "usage: pg18.sh seed DATA ROWS"
  alive "$PIDF" "$DATA" || die "REFUSED: no running server recorded for $DATA"
  psqlc -d postgres -c "CREATE DATABASE p"
  "$FT_PY" -B "$FT_HERE/gen_seed.py" "$ROWS" | psqlc -d p
  psqlc -d p -c "VACUUM ANALYZE t" -c "CHECKPOINT"
  echo "seeded p.t rows=$(psqlc -d p -At -c 'SELECT count(*) FROM t') size=$(psqlc -d p -At -c "SELECT pg_size_pretty(pg_database_size('p'))")"
  ;;
sql)
  DB=${3:-}; SQL=${4:-}
  [ -n "$DB" ] && [ -n "$SQL" ] || die "usage: pg18.sh sql DATA DB SQL"
  psqlc -d "$DB" -At -c "$SQL"
  ;;
stop)
  stop_pid "$PIDF" "$DATA" INT 120
  ;;
*) die "unknown command $cmd" ;;
esac
