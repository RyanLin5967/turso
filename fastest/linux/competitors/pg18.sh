#!/usr/bin/env bash
# pg18.sh -- PostgreSQL 18.6 (Homebrew postgresql@18) for the branch benchmark. PREREG §4 settings.
#
# LINUX PORT (lane fastest-linux-comp; source artie-research frontier/fastest/tools/competitors/pg18.sh @648ce2929):
# PGDG postgresql-18 at FT_PG18 (default /usr/lib/postgresql/18/bin). fsync_writethrough does not exist on Linux
# (PG refuses it), and on Linux fsync/fdatasync ARE the device flush, so MODE d2 here sets wal_sync_method=fdatasync
# (also Linux's default). Consequence: on Linux `default` differs from `d2` ONLY in file_copy_method (copy vs clone);
# its durability is already D2. file_copy_method=clone is copy_file_range(2) on Linux: a reflink on XFS/btrfs when
# the kernel can, a silent byte copy when it cannot, which is why the driver proves the clone from shared extents.
# Amendment 14's "FILE_COPY with file_copy_method=clone ... at its defaults" is MODE d1clone; on Linux it configures
# the same server as d2 (every d2 line but file_copy_method is a Linux default), which `settings` dumps to show.
#   pg18.sh settings DATA             every pg_settings row (name, setting, unit, source) as TSV
#   pg18.sh writethrough DATA         ask this postgres binary to accept wal_sync_method=fsync_writethrough
#                                     (postgres -C); prints its output and rc -- Linux builds refuse it
#
#   pg18.sh init  DATA MODE PORT      initdb into DATA (must not exist; *.noindex), MODE = d2 | default | d1clone
#   pg18.sh start DATA [V1RUN]        exec `postgres -D DATA` directly (never pg_ctl: its /bin/sh would strip the
#                                     V1 shim), detached; pid -> DATA.ftpid, log -> DATA.log; waits until ready
#   pg18.sh seed  DATA ROWS [AGE]     CREATE DATABASE p (the template) with t(ROWS rows) [aged], VACUUM ANALYZE, CHECKPOINT
#   pg18.sh sql   DATA DB SQL         one statement through psql (prints rows unaligned)
#   pg18.sh stop  DATA                SIGINT (fast shutdown) to the recorded pid only; waits for exit
#
# MODE d2      : (the Linux port, LOW 23: the Mac's fsync_writethrough does not exist here) wal_sync_method=fdatasync,
#                fsync=on, full_page_writes=on, synchronous_commit=on, file_copy_method=clone (PREREG §4 "Competitor
#                D2 settings")
# MODE default : PostgreSQL's own defaults for the settings above (on Linux wal_sync_method=fdatasync already;
#                file_copy_method=copy); not a Linux run system (pg18-defaults is dropped, ruling 6b0bef481b)
# MODE d1clone : defaults plus file_copy_method=clone (on Linux the same server as d2; report only)
# All modes: shared_buffers = 25% of MemTotal (gate-6 item 15; initdb's 128 MB is NOT a default here), listen
# 127.0.0.1:PORT, no Unix socket (the scratch path exceeds sun_path), max_connections=1100 (C up to 1024 plus spare),
# trust auth for user postgres. Branch op: CREATE DATABASE b TEMPLATE p STRATEGY=FILE_COPY.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
cmd=${1:-}; DATA=${2:-}
[ -n "$cmd" ] && [ -n "$DATA" ] || die "usage: pg18.sh init|start|seed|settings|writethrough|sql|stop DATA ..."
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
  # LOW 22: shared_buffers is computed BEFORE the config is written, from a copy of /proc/meminfo kept beside the data
  # dir (DATA.meminfo; run_system.sh checks against that same read), and a failed or empty value stops the init
  # instead of writing "shared_buffers = "
  cp /proc/meminfo "$DATA.meminfo" || die "cannot read /proc/meminfo"
  SB=$("$FT_PY" -B "$FT_HERE/pins.py" shared-buffers "$DATA.meminfo") || die "pins.py shared-buffers failed"
  [[ $SB =~ ^[0-9]+MB$ ]] || die "shared_buffers [$SB] is not <N>MB"
  {
    echo ""
    echo "# ---- fastest-tools pg18.sh, mode $MODE ----"
    echo "port = $PORT"
    echo "listen_addresses = '127.0.0.1'"
    echo "unix_socket_directories = ''"
    echo "max_connections = 1100"
    # shared_buffers = 25% of MemTotal (gate-6 review, t3run item 15; initdb's default is 128 MB); run_system.sh
    # refuses a server whose pg_settings value differs (pins.py check-pg)
    echo "shared_buffers = $SB"
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
  AGE=${4:-0}
  psqlc -d postgres -c "CREATE DATABASE p"
  # Each stream's sha256 is recorded by the process that piped it (DATA.seed-{sql,age}.sha256; lead review MED 3).
  "$FT_PY" -B "$FT_HERE/gen_seed.py" sql --rows "$ROWS" --digest-out "$DATA.seed-sql.sha256" | psqlc -d p
  psqlc -d p -c "CHECKPOINT"
  # Aged parent (gate-6 review, t3run item 4; PREREG §7 / amendment 52): AGE committed single-row UPDATEs (psql
  # autocommits each statement), the same stream for every system, then PG's documented maintenance. AGE=0 pipes an
  # empty stream, so its digest is recorded too.
  "$FT_PY" -B "$FT_HERE/gen_seed.py" age --rows "$ROWS" --updates "$AGE" --digest-out "$DATA.seed-age.sha256" | psqlc -d p
  psqlc -d p -c "VACUUM ANALYZE t" -c "CHECKPOINT"
  echo "maintenance: CHECKPOINT; aged $AGE; VACUUM ANALYZE; CHECKPOINT"
  echo "seeded p.t rows=$(psqlc -d p -At -c 'SELECT count(*) FROM t') size=$(psqlc -d p -At -c "SELECT pg_size_pretty(pg_database_size('p'))")"
  ;;
settings)
  alive "$PIDF" "$DATA" || die "REFUSED: no running server recorded for $DATA"
  # name, setting, unit, source (LOW 21: the unit column, so a setting's bytes are read, never guessed)
  psqlc -d postgres -At -F $'\t' -c "SELECT name, setting, coalesce(unit, ''), source FROM pg_settings ORDER BY name"
  ;;
writethrough)
  # Not started: postgres -C reads the config, applies -c, prints the value and exits. rc is the evidence.
  set +e
  "$FT_PG18/postgres" -D "$DATA" -C wal_sync_method -c wal_sync_method=fsync_writethrough 2>&1
  echo "rc=$?"
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
