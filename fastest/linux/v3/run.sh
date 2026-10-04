#!/usr/bin/env bash
# run.sh V3FLOOR_BIN DIR OUT N [extra v3floor args...] -- one V3 device-floor batch with Linux stamps
# (port of frontier/fastest/tools/v3/run.sh). Writes OUT/stamp_start.json, the probe's OUT/raw.tsv +
# OUT/summary.json, OUT/stamp_end.json, OUT/rc.
# Exit: the probe's rc if it failed, else 2 if a stamp could not take a required record, else 0. The Linux stamps
# are record-only (stamp.py says why), so unlike the Mac's run.sh nothing here voids a batch with rc 3 except the
# probe's own flush control.
# Not a credited measurement unless the PREREG is registered with its T3 rules.
set -uo pipefail
BIN=$1 DIR=$2 OUT=$3 N=$4
shift 4
HERE="$(cd "$(dirname "$0")" && pwd)"
STAMP="$HERE/stamp.py"
TMP="$OUT.stamp_start.json"
python3 -B "$STAMP" start "$TMP" --dir "$DIR" || { echo "run.sh: start stamp failed" >&2; exit 2; }
"$BIN" --dir "$DIR" --out "$OUT" --n "$N" "$@"
prc=$?
if [ -d "$OUT" ]; then
  mv "$TMP" "$OUT/stamp_start.json"
  python3 -B "$STAMP" end "$OUT/stamp_start.json" "$OUT/stamp_end.json"
  src=$?
else
  rm -f "$TMP"
  src=2
fi
rc=$prc
[ "$rc" -eq 0 ] && rc=$src
[ -d "$OUT" ] && echo "probe_rc=$prc stamp_rc=$src rc=$rc" > "$OUT/rc"
exit "$rc"
