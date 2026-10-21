#!/usr/bin/env bash
# memstatus_ab.sh CLONEBENCH SQLITE3 DIR OUT -- comp review 1 LOW 39's owed A/B: clonebench m1-d0 with SQLite's
# MEMSTATUS off (0, the new default) against on (1, the stock setting), at C=4 and C=64. Written BEFORE its first run:
#   - one parent (fixture.py sqlite, 10000 rows, fresh) in DIR; every run creates in DIR/branches and drops (--drop);
#   - per C, the arms in the order 0 1 1 0 0 1 (ABBAAB, so drift lands on both), each run --max-ops 4000 at C=4 and
#     8000 at C=64, --warmup 1000:1:10, --max-window-s 120;
#   - read: each run's summary.json lat_us p50/p99 and tput_per_s; the claim is the median of each arm's three runs.
# It is EXPLORATORY: on the machine it runs on (the competitors' timed cells are Linux), never credited.
# OUT must not exist; every run's out dir and a table OUT/table.tsv. Exit 0 when every run exited 0.
set -uo pipefail
CB=$1 SQ3=$2 DIR=$3 OUT=$4
HERE="$(cd "$(dirname "$0")" && pwd)"
[ -e "$OUT" ] && { echo "REFUSED: $OUT exists" >&2; exit 2; }
mkdir -p "$OUT" "$DIR/branches" || exit 2
[ -e "$DIR/parent.db" ] || python3 -B "$HERE/fixture.py" sqlite "$DIR/parent.db" --rows 10000 --age 0 --sqlite3 "$SQ3" \
  >"$OUT/parent.json" || { echo "REFUSED: parent" >&2; exit 2; }
bad=0
printf 'c\trun\tmemstatus\trc\tp50_us\tp99_us\ttput_per_s\n' >"$OUT/table.tsv"
for c in 4 64; do
  n=4000; [ "$c" = 64 ] && n=8000
  i=0
  for ms in 0 1 1 0 0 1; do
    i=$((i + 1))
    o="$OUT/c$c-r$i-ms$ms"
    rc=0
    timeout 600 "$CB" run --mode b1 --op m1 --sync d0 --parent "$DIR/parent.db" --dir "$DIR/branches" --clients "$c" \
      --max-ops "$n" --rows 10000 --warmup 1000:1:10 --max-window-s 120 --drop --memstatus "$ms" --out "$o" \
      >"$o.txt" 2>&1 || rc=$?
    [ "$rc" = 0 ] || bad=$((bad + 1))
    python3 -B -c 'import json, sys
s = json.load(open(sys.argv[1] + "/summary.json"))
l = s.get("lat_us") or {}
print("\t".join(str(x) for x in (sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], l.get("p50"), l.get("p99"), s.get("tput_per_s"))))' \
      "$o" "$c" "$i" "$ms" "$rc" >>"$OUT/table.tsv" 2>>"$OUT/table.err" || echo -e "$c\t$i\t$ms\t$rc\tMISSING" >>"$OUT/table.tsv"
  done
done
cat "$OUT/table.tsv"
[ $bad = 0 ]
