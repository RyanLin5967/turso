#!/usr/bin/env bash
# init_plant.sh MNT RAW [wrong-reason] -- comp review 1 LOW 33's plant, moved out of fastest-competitors.yml so it can
# run off CI too (comp-rev-89ec-b3c3 LOW 10): a pg18-d2 run_system.sh job whose server init fails at pg18.sh's
# shared_buffers step (an FT_PY stub fails `pins.py shared-buffers` with "planted shared-buffers failure") must fail,
# with a FAIL server init line, FOR THAT REASON.
#   wrong-reason: the control. Init fails for another reason (FT_PG18 names no PostgreSQL), so the plant's check must
#                 REFUSE it: a check that passes any init failure (initdb, "DATA exists", a missing FT_PG18) proves
#                 nothing about the shared_buffers step.
# Needs the job's other FT_* (FT_BBLOAD, FT_CLONEBENCH, FT_SQLITE3, FT_FIRECHECK, FT_PG18). Exit 0 only when the check
# holds; 1 when it does not; 2 refused.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
MNT=${1:?usage: init_plant.sh MNT RAW [wrong-reason]} RAW=${2:?usage: init_plant.sh MNT RAW [wrong-reason]}
MODE=${3:-planted}
case $MODE in planted|wrong-reason) ;; *) echo "init_plant: REFUSED: mode [$MODE]" >&2; exit 2 ;; esac
[ -e "$RAW" ] && { echo "init_plant: REFUSED: $RAW exists" >&2; exit 2; }
mkdir -p "$MNT" "$RAW.bin" || exit 2
stub="$RAW.bin/py"
printf '#!/bin/sh\ncase "$*" in *pins.py\\ shared-buffers*) echo "planted shared-buffers failure" >&2; exit 1 ;; esac\nexec python3 "$@"\n' >"$stub"
chmod +x "$stub"
pg=${FT_PG18:-}
[ "$MODE" = wrong-reason ] && pg="$RAW.bin/no-postgresql-here"
rc=0
FT_PY="$stub" FT_PG18="$pg" timeout 1800 bash "$HERE/run_system.sh" pg18-d2 "$MNT" "$RAW" >"$RAW.txt" 2>&1 || rc=$?
n=$(grep -c '^FAIL server init' "$RAW/functional.txt" 2>/dev/null)
echo "init plant ($MODE): rc $rc; ${n:-0} server-init FAIL line(s)"
# the check (the workflow's, as it stood): the job failed, with a server-init FAIL line
[ "$rc" != 0 ] && [ "${n:-0}" -gt 0 ]
