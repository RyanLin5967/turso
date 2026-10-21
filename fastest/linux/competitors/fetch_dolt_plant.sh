#!/usr/bin/env bash
# fetch_dolt_plant.sh SCRATCH -- comp review 1 LOW 38: fetch_dolt.sh's handoff from the version commands to
# pins.py check-version, on stub binaries (no download: a stub curl on PATH fails any fetch). The current output form
# ("dolt version X" / "Doltgres version X" on the first line) must pass; a warning before the version line, and the
# old prefixed form ("dolt version: dolt version X", what c52bf0bce fixed), must each refuse.
# Exit 0 only when every case holds; 1 otherwise; 2 refused.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
S=${1:?usage: fetch_dolt_plant.sh SCRATCH}
[ -e "$S" ] && { echo "fetch_dolt_plant: REFUSED: $S exists" >&2; exit 2; }
mkdir -p "$S/path" || exit 2
printf '#!/bin/sh\necho "stub curl: no download in this plant" >&2\nexit 1\n' >"$S/path/curl"
chmod +x "$S/path/curl"
dv=$(python3 -B "$HERE/pins.py" version dolt) && dgv=$(python3 -B "$HERE/pins.py" version doltgres) ||
  { echo "fetch_dolt_plant: REFUSED: no registered versions" >&2; exit 2; }
bad=0 n=0
case_() { # case_ NAME WANT(pass|refuse) DOLT_OUTPUT DOLTGRES_OUTPUT
  local name=$1 want=$2 d="$S/case-$((n + 1))" rc=0
  n=$((n + 1))
  mkdir -p "$d/bin"
  printf '%b\n' "$3" >"$d/dolt.out"  # (a \n in an output is a line break)
  printf '%b\n' "$4" >"$d/doltgres.out"
  printf '#!/bin/sh\ncat "%s"\n' "$d/dolt.out" >"$d/bin/dolt"
  printf '#!/bin/sh\ncat "%s"\n' "$d/doltgres.out" >"$d/bin/doltgres"
  chmod +x "$d/bin/dolt" "$d/bin/doltgres"
  PATH="$S/path:$PATH" timeout 60 bash "$HERE/fetch_dolt.sh" check-versions "$d/bin" "$d/out.txt" >"$d/log.txt" 2>&1 || rc=$?
  if { [ "$want" = pass ] && [ "$rc" = 0 ]; } || { [ "$want" = refuse ] && [ "$rc" != 0 ] && grep -q 'does not report' "$d/log.txt"; }; then
    echo "PASS $name (rc $rc)"
  else
    echo "FAIL $name: rc $rc, want $want; $(tail -2 "$d/log.txt" | tr '\n' ' ')"
    bad=$((bad + 1))
  fi
}
case_ "the current form passes" pass "dolt version $dv" "Doltgres version $dgv"
case_ "a warning line before dolt's version refuses" refuse "warning: a newer dolt is available\\ndolt version $dv" "Doltgres version $dgv"
case_ "the old prefixed form refuses (c52bf0bce)" refuse "dolt version: dolt version $dv" "Doltgres version $dgv"
case_ "another Doltgres version refuses" refuse "dolt version $dv" "Doltgres version 0.0.1"
echo "VERDICT $([ $bad = 0 ] && echo PASS || echo FAIL) $((n - bad))/$n"
[ $bad = 0 ]
