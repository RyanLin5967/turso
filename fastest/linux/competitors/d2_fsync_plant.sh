#!/usr/bin/env bash
# d2_fsync_plant.sh PG18_BIN SCRATCH -- comp review 1 LOW 36's live plant: a real PG 18 cluster started with the five
# D2 settings (pg18.sh init MODE d2's values, wal_sync_method fdatasync where the platform has it) is accepted by
# pins.py check-d2, and the same cluster restarted with fsync=off is refused naming fsync. The settings are dumped in
# pg18.sh settings' format (name, setting, unit, source). Exit 0 only when both verdicts are as expected.
set -uo pipefail
PGB=$1 S=$2
HERE="$(cd "$(dirname "$0")" && pwd)"
[ -e "$S" ] && { echo "REFUSED: $S exists" >&2; exit 2; }
mkdir -p "$S" || exit 2
"$PGB/initdb" -D "$S/pg" -U postgres -A trust >"$S/initdb.txt" 2>&1 || exit 2
trap '"$PGB/pg_ctl" -D "$S/pg" stop -m fast >/dev/null 2>&1' EXIT
dump() {
  "$PGB/psql" -X -q -h 127.0.0.1 -p 55498 -U postgres -d postgres -At -F $'\t' \
    -c "SELECT name, setting, coalesce(unit, ''), source FROM pg_settings ORDER BY name"
}
bad=0
for arm in d2:on refused:off; do
  want=${arm%%:*} fs=${arm#*:}
  "$PGB/pg_ctl" -D "$S/pg" -o "-p 55498 -k $S -c listen_addresses=127.0.0.1 -c fsync=$fs -c full_page_writes=on -c synchronous_commit=on -c file_copy_method=clone -c wal_sync_method=fdatasync" \
    -l "$S/server-$fs.txt" -w start >/dev/null || { echo "start (fsync=$fs) failed: $(tail -2 "$S/server-$fs.txt")"; exit 2; }
  dump >"$S/settings-$fs.tsv"
  "$PGB/pg_ctl" -D "$S/pg" stop -m fast -w >/dev/null
  v=$(python3 -B "$HERE/pins.py" check-d2 "$S/settings-$fs.tsv")
  echo "fsync=$fs: $v"
  if [ "$want" = d2 ]; then [[ $v == ok:* ]] || bad=$((bad + 1)); else [[ $v == REFUSED:*fsync* ]] || bad=$((bad + 1)); fi
done
[ $bad = 0 ]
