#!/usr/bin/env bash
# run.sh V3FLOOR_BIN DIR OUT N [extra v3floor args...] -- one V3 device-floor batch with Linux stamps
# (port of frontier/fastest/tools/v3/run.sh). Writes OUT/stamp_start.json, the probe's OUT/raw.tsv +
# OUT/summary.json, OUT/stamp_end.json, OUT/binary.txt (the binary's sha256 and what binds it), OUT/rc.
#
# Binding (tools review 1 item 5: the per-arm flush identity is the fire-check's strace check, so a batch must
# come from a binary that passed it). Exactly one of:
#   V3_FIRECHECK_VERDICT=<verdict.json>  a fire-check verdict with all_pass true for THIS binary (sha256), this
#                                        machine's arch (uname -m) and DIR's filesystem type; anything else refuses
#   V3_SMOKE=1                           an explicitly unbound smoke batch, recorded as such, never credited
# Neither, or both: refused (rc 2) before anything runs.
# Exit: 2 refused or a stamp could not take a required record; else the probe's rc (0 ok, 1 op failed, 3 void).
# The Linux stamps are record-only (stamp.py says why), so nothing here voids a batch except the probe's own control.
# Not a credited measurement unless the PREREG is registered with its T3 rules.
set -uo pipefail
[ $# -ge 4 ] || { echo "usage: run.sh V3FLOOR_BIN DIR OUT N [v3floor args...]" >&2; exit 2; }
BIN=$1 DIR=$2 OUT=$3 N=$4
shift 4
HERE="$(cd "$(dirname "$0")" && pwd)"
STAMP="$HERE/stamp.py"
sha=$(sha256sum "$BIN" | cut -d' ' -f1) || { echo "run.sh: REFUSED: cannot hash $BIN" >&2; exit 2; }
fstype=$(findmnt -n -o FSTYPE -T "$DIR") || { echo "run.sh: REFUSED: cannot find the filesystem of $DIR" >&2; exit 2; }
arch=$(uname -m)
if [ -n "${V3_FIRECHECK_VERDICT:-}" ] && [ -n "${V3_SMOKE:-}" ]; then
  echo "run.sh: REFUSED: set V3_FIRECHECK_VERDICT or V3_SMOKE=1, not both" >&2; exit 2
elif [ -n "${V3_FIRECHECK_VERDICT:-}" ]; then
  why=$(python3 -B -c '
import json, sys
p, sha, fs, arch = sys.argv[1:5]
try:
    v = json.load(open(p))
except Exception as e:
    print("unreadable verdict: %r" % e); sys.exit(1)
bad = [k for k, ok in (("all_pass", v.get("all_pass") is True), ("v3floor_sha256", v.get("v3floor_sha256") == sha),
                       ("fstype", v.get("fstype") == fs), ("arch", v.get("arch") == arch)) if not ok]
print(",".join(bad)); sys.exit(1 if bad else 0)' "$V3_FIRECHECK_VERDICT" "$sha" "$fstype" "$arch")
  vrc=$?
  [ "$vrc" -eq 0 ] || { echo "run.sh: REFUSED: $V3_FIRECHECK_VERDICT does not bind this batch (mismatch: $why; binary $sha, fs $fstype, arch $arch)" >&2; exit 2; }
  bound="fire-checked: $V3_FIRECHECK_VERDICT"
elif [ "${V3_SMOKE:-}" = 1 ]; then
  bound="smoke: V3_SMOKE=1, not bound to a fire-check, never credited"
else
  echo "run.sh: REFUSED: set V3_FIRECHECK_VERDICT=<a passing fire-check verdict.json for this binary> or V3_SMOKE=1" >&2
  exit 2
fi
TMP="$OUT.stamp_start.json"
python3 -B "$STAMP" start "$TMP" --dir "$DIR" || { echo "run.sh: start stamp failed" >&2; rm -f "$TMP"; exit 2; }
"$BIN" --dir "$DIR" --out "$OUT" --n "$N" "$@"
prc=$?
if [ -d "$OUT" ]; then
  mv "$TMP" "$OUT/stamp_start.json"
  python3 -B "$STAMP" end "$OUT/stamp_start.json" "$OUT/stamp_end.json"
  src=$?
  printf 'v3floor_sha256=%s\nfstype=%s\narch=%s\nbound=%s\n' "$sha" "$fstype" "$arch" "$bound" > "$OUT/binary.txt"
else
  rm -f "$TMP"
  src=2
fi
rc=$prc
[ "$rc" -eq 0 ] && rc=$src
[ -d "$OUT" ] && echo "probe_rc=$prc stamp_rc=$src rc=$rc" > "$OUT/rc"
exit "$rc"
