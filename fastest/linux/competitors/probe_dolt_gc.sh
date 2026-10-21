#!/usr/bin/env bash
# probe_dolt_gc.sh DOLT DOLTGRES MARIADB PSQL SCRATCH -- comp review 1 LOW 28 (second half) and LOW 29, the probes
# owed before a config.yaml can pin behavior.auto_gc_behavior and before Doltgres's dolt_gc session premise is relied
# on. Each server starts with NO auto-GC setting (Dolt: flags only, as dolt.sh starts it; Doltgres: doltgres.sh's
# config.yaml, which names none), so what it reports is the binary's own default:
#   dolt:     SELECT @@dolt_auto_gc_enabled, SHOW VARIABLES LIKE '%gc%', then CALL DOLT_GC() and SELECT 1 in ONE session
#   doltgres: the same through pg_settings, then SELECT dolt_gc() and SELECT 1 in ONE psql session (LOW 29: if dolt_gc
#             ended the calling session, every Doltgres seed would die)
# Output: SCRATCH/probe.txt (every command, its output and rc). Servers on 127.0.0.1 ports 55491/55492, stopped at
# the end. SCRATCH must not exist. Exit 0 when every probe ran (the verdicts are in the record), 2 when one could not.
set -uo pipefail
DOLT=$1 DG=$2 MY=$3 PSQL=$4 S=$5
[ -e "$S" ] && { echo "REFUSED: $S exists" >&2; exit 2; }
mkdir -p "$S/dolt/bench" "$S/dg/databases" "$S/dg/doltcfg" "$S/root" || exit 2
export DOLT_ROOT_PATH="$S/root" DOLT_DISABLE_EVENT_FLUSH=1
OUT="$S/probe.txt"
log() { echo "$*" | tee -a "$OUT"; }
run() { local rc=0; log "\$ $*"; "$@" >>"$OUT" 2>&1 || rc=$?; log "rc=$rc"; return $rc; }
cleanup() { [ -n "${DP:-}" ] && kill "$DP" 2>/dev/null; [ -n "${GP:-}" ] && kill "$GP" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT
log "dolt: $("$DOLT" version 2>&1 | head -1)"
log "doltgres: $("$DG" -version 2>&1 | head -1)"
"$DOLT" config --global --add metrics.disabled true >/dev/null 2>&1
"$DOLT" config --global --add user.name probe >/dev/null 2>&1
"$DOLT" config --global --add user.email probe@localhost >/dev/null 2>&1
(cd "$S/dolt/bench" && "$DOLT" init >/dev/null 2>&1) || { log "dolt init failed"; exit 2; }
(cd "$S/dolt" && exec "$DOLT" sql-server -H 127.0.0.1 -P 55491 --data-dir "$S/dolt" -l warning >"$S/dolt.log" 2>&1) &
DP=$!
my() { "$MY" --protocol=TCP -h 127.0.0.1 -P 55491 -u root --skip-ssl -N "$@"; }
for _ in $(seq 1 120); do my -e "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done
run my -e "SELECT @@dolt_auto_gc_enabled" || exit 2
run my -e "SHOW VARIABLES LIKE '%gc%'"
run my bench -e "CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1); CALL DOLT_COMMIT('-Am', 'seed'); CALL DOLT_GC(); SELECT 1"
cat >"$S/dg/config.yaml" <<EOF
log_level: warning
user:
  name: postgres
  password: password
listener:
  host: 127.0.0.1
  port: 55492
data_dir: $S/dg/databases
cfg_dir: $S/dg/doltcfg
privilege_file: $S/dg/doltcfg/privileges.db
auth_file: $S/dg/doltcfg/auth.db
branch_control_file: $S/dg/doltcfg/branch_control.db
EOF
(cd "$S/dg" && exec "$DG" -config "$S/dg/config.yaml" >"$S/doltgres.log" 2>&1) &
GP=$!
pg() { PGPASSWORD=password "$PSQL" -X -h 127.0.0.1 -p 55492 -U postgres -d postgres -At "$@"; }
for _ in $(seq 1 120); do pg -c "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done
run pg -c "SELECT name, setting FROM pg_settings WHERE name LIKE '%gc%'" || exit 2
run pg -c "SHOW dolt_auto_gc_enabled"
run pg -c "CREATE TABLE t (id INT PRIMARY KEY)" -c "INSERT INTO t VALUES (1)" -c "SELECT dolt_commit('-Am', 'seed')" \
  -c "SELECT dolt_gc()" -c "SELECT 1"
log "server logs: $S/dolt.log $S/doltgres.log"
exit 0
