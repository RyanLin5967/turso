#!/usr/bin/env bash
# seed_gc_check.sh SCRATCH -- runs dolt.sh and doltgres.sh as the jobs do (init, start, seed 2000 rows aged 50, stop)
# and prints each seed's GC record, its verdict (fthelp.py gcsettings) and the GC verdict: comp review 1 LOW 28's
# explicit auto-GC config and record, on whatever FT_DOLT / FT_DOLTGRES / FT_MARIADB / FT_PG18 the environment names.
# Exit 0 only when both seeds ran and both records read ok.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
S=${1:?usage: seed_gc_check.sh SCRATCH}
[ -e "$S" ] && { echo "REFUSED: $S exists" >&2; exit 2; }
mkdir -p "$S" || exit 2
bad=0
for sys in dolt:55493 doltgres:55494; do
  k=${sys%%:*} port=${sys#*:} d="$S/$k.noindex"
  rc=0
  { timeout 120 bash "$HERE/$k.sh" init "$d" "$port" && timeout 180 bash "$HERE/$k.sh" start "$d" &&
    timeout 600 bash "$HERE/$k.sh" seed "$d" 2000 50; } >"$S/$k.txt" 2>&1 || rc=$?
  timeout 120 bash "$HERE/$k.sh" stop "$d" >>"$S/$k.txt" 2>&1
  v=$(head -1 "$d.gc-settings.verdict" 2>/dev/null)
  echo "$k: seed rc $rc; record [$(tr '\n' ' ' <"$d.gc-settings.txt" 2>/dev/null)]; record verdict [$v]; GC verdict [$(head -1 "$d.gc.verdict" 2>/dev/null)]"
  [ "$rc" = 0 ] && [ "$v" = ok ] || bad=$((bad + 1))
done
[ $bad = 0 ]
