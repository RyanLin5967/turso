#!/usr/bin/env bash
# readback_repin.sh PG18_BIN DOLT DOLTGRES MARIADB SCRATCH -- comp review 1 LOW 27's owed re-pin: each engine loads
# gen_seed.py's parent (2500 rows aged 300, and 10000 rows aged 200; seed 1) and reads it back with readback_sql (the
# OFFSET 0 subquery form for PG and Doltgres, the mysql form for Dolt); every triple must equal gen_seed.facts' and the
# pinned engine answers (2500|131727070|21429546430; 10000|89963830 for count|sum). Prints one line per engine and size.
# Servers on 127.0.0.1:55495-55497, stopped at the end. Exit 0 only when every read-back matched.
set -uo pipefail
PGB=$1 DOLT=$2 DG=$3 MY=$4 S=$5
HERE="$(cd "$(dirname "$0")" && pwd)"
[ -e "$S" ] && { echo "REFUSED: $S exists" >&2; exit 2; }
mkdir -p "$S/root" || exit 2
export DOLT_ROOT_PATH="$S/root" DOLT_DISABLE_EVENT_FLUSH=1
gs() { python3 -B "$HERE/gen_seed.py" "$@"; }
want() { python3 -B -c 'import sys; sys.path.insert(0, sys.argv[1]); import gen_seed as g
f = g.facts(int(sys.argv[2]), int(sys.argv[3]), 1); print(f"{f[\"count\"]}|{f[\"sum\"]}|{f[\"row_hash\"]}")' "$HERE" "$1" "$2"; }
bad=0
check() { # check ENGINE ROWS AGE GOT
  local w; w=$(want "$2" "$3")
  if [ "$4" = "$w" ]; then echo "PASS $1 rows=$2 age=$3: $4"; else echo "FAIL $1 rows=$2 age=$3: got [$4] want [$w]"; bad=$((bad + 1)); fi
}
PIDS=()
trap 'for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "$p" 2>/dev/null; done; [ -d "$S/pg" ] && "$PGB/pg_ctl" -D "$S/pg" stop -m fast >/dev/null 2>&1; wait 2>/dev/null' EXIT
# PG 18
"$PGB/initdb" -D "$S/pg" -U postgres -A trust >"$S/pg-initdb.txt" 2>&1 || { echo "initdb failed"; exit 2; }
"$PGB/pg_ctl" -D "$S/pg" -o "-p 55495 -k $S -c listen_addresses=127.0.0.1" -l "$S/pg-server.txt" start >/dev/null || exit 2
pq() { "$PGB/psql" -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55495 -U postgres "$@"; }
# Dolt
mkdir -p "$S/dolt/bench"; "$DOLT" config --global --add user.name probe >/dev/null; "$DOLT" config --global --add user.email p@l >/dev/null
(cd "$S/dolt/bench" && "$DOLT" init >/dev/null 2>&1)
(cd "$S/dolt" && exec "$DOLT" sql-server -H 127.0.0.1 -P 55496 --data-dir "$S/dolt" -l warning >"$S/dolt-server.txt" 2>&1) & PIDS+=($!)
my() { "$MY" --protocol=TCP -h 127.0.0.1 -P 55496 -u root --skip-ssl -N "$@"; }
# Doltgres
mkdir -p "$S/dg/databases" "$S/dg/doltcfg"
printf 'log_level: warning\nuser:\n  name: postgres\n  password: password\nlistener:\n  host: 127.0.0.1\n  port: 55497\ndata_dir: %s\ncfg_dir: %s\n' \
  "$S/dg/databases" "$S/dg/doltcfg" >"$S/dg/config.yaml"
(cd "$S/dg" && exec "$DG" -config "$S/dg/config.yaml" >"$S/doltgres-server.txt" 2>&1) & PIDS+=($!)
dq() { PGPASSWORD=password "$PGB/psql" -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 55497 -U postgres "$@"; }
for _ in $(seq 1 120); do my -e "SELECT 1" >/dev/null 2>&1 && dq -At -c "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done
for size in 2500:300 10000:200; do
  r=${size%%:*} a=${size#*:}
  pq -c "DROP DATABASE IF EXISTS p" -c "CREATE DATABASE p" >/dev/null
  { gs sql --rows "$r"; gs age --rows "$r" --updates "$a"; } | pq -d p >/dev/null
  check pg18 "$r" "$a" "$(pq -d p -At -F '|' -c "$(gs readback-sql --dialect pg)")"
  my -e "DROP DATABASE IF EXISTS b$r; CREATE DATABASE b$r"
  { gs sql --rows "$r"; gs age --rows "$r" --updates "$a"; } | my "b$r"
  check dolt "$r" "$a" "$(my "b$r" -e "$(gs readback-sql --dialect mysql)" | tr '\t' '|')"
  dq -d postgres -c "DROP TABLE IF EXISTS t" >/dev/null
  { gs sql --rows "$r"; gs age --rows "$r" --updates "$a"; } | dq -d postgres >/dev/null
  check doltgres "$r" "$a" "$(dq -d postgres -At -F '|' -c "$(gs readback-sql --dialect pg)")"
done
[ $bad = 0 ]
