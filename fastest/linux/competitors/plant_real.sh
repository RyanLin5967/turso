#!/usr/bin/env bash
# plant_real.sh SCRATCH -- the refusal plants of run_system.sh (lane fastest-linux-comp; lead ruling, artie DECISIONS
# 6b0bef481b). The CI smoke warm-up cap (FT_WARMUP=1000:10:2) is accepted for smoke runs only (FT_DRY=1): a REAL run
# (FT_DRY=0, and FT_DRY unset, which is real) with that cap must refuse before anything runs, and so must a real run
# at a non-registered cap (rule(20) IS the smoke cap), a real run with another rule, an FT_DRY that is neither 0 nor 1,
# and the dropped system pg18-defaults (gate-6 item 18). The controls -- a smoke run with the cap, a real run with the
# registered cap and rule (implicit and written out) -- must pass those checks and stop later, at the fire-check
# verdict this harness withholds (FT_FIRECHECK names no file), so a check that refused everything would fail them.
# Runs run_system.sh under the bash running this script (macOS /bin/bash 3.2 cannot parse trace.sh's coproc).
# SCRATCH must not exist; everything the runs write lives there. Exit 0 only when every plant refused and no control
# did (one PASS line per case, then "VERDICT PASS n/n"); 1 otherwise; 2 refused.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
S=${1:?usage: plant_real.sh SCRATCH}
[ -e "$S" ] && { echo "plant_real: REFUSED: $S exists" >&2; exit 2; }
mkdir -p "$S" || exit 2
SH=${BASH:-bash}
bad=0 n=0
FCTEXT="REFUSED: the flush counter's fire-check did not pass"
# case NAME WANT_RC WANT_TEXT [VAR=VALUE ...] -- SYSTEM: one run_system.sh with only these FT_DRY/FT_WARMUP/FT_CAP_S
case_() {
  local name=$1 want_rc=$2 want=$3 envs=() rc out ok=1
  shift 3
  while [ "$1" != -- ]; do envs+=("$1"); shift; done
  shift
  n=$((n + 1))
  out=$(env -u FT_DRY -u FT_WARMUP -u FT_CAP_S -u FT_OPS_TOTAL -u FDSYNC_SCAN_HOOK \
    FT_BBLOAD="$S/no-bbload" FT_CLONEBENCH="$S/no-clonebench" FT_SQLITE3="$S/no-sqlite3" \
    FT_FIRECHECK="$S/no-firecheck.txt" ${envs[@]+"${envs[@]}"} \
    timeout 120 "$SH" "$HERE/run_system.sh" "$1" "$S/mnt-$n" "$S/raw-$n" 2>&1)
  rc=$?
  [ "$rc" = "$want_rc" ] || ok=0
  grep -qF -- "$want" <<<"$out" || ok=0
  # a control must not have met the real-run or FT_DRY refusal on its way to the fire-check
  if [ "$want_rc" = 1 ] && grep -qE 'a real run|FT_DRY \[' <<<"$out"; then ok=0; fi
  if [ $ok = 1 ]; then
    echo "PASS $name: rc $rc, [$want]"
  else
    echo "FAIL $name: rc $rc (want $want_rc and [$want]); output: $(tr '\n' ' ' <<<"$out" | cut -c1-600)"
    bad=$((bad + 1))
  fi
}
REAL="REFUSED: a real run (FT_DRY=0)"
case_ "real run with the smoke cap" 2 "$REAL" FT_DRY=0 FT_WARMUP=1000:10:2 -- pg18-d2
case_ "FT_DRY unset with the smoke cap (unset is real)" 2 "$REAL" FT_WARMUP=1000:10:2 -- pg18-d2
case_ "real run at a 20 s cap (rule(20) is the smoke cap)" 2 "$REAL" FT_DRY=0 FT_CAP_S=20 -- pg18-d2
case_ "real run with another rule" 2 "$REAL" FT_DRY=0 FT_WARMUP=20:0:0 -- b1
case_ "FT_DRY neither 0 nor 1" 2 "REFUSED: FT_DRY [yes] is neither 0" FT_DRY=yes -- pg18-d2
case_ "pg18-defaults (dropped on Linux, item 18)" 2 "REFUSED: pg18-defaults is dropped on Linux" FT_DRY=1 -- pg18-defaults
case_ "control: smoke run with the smoke cap" 1 "$FCTEXT" FT_DRY=1 FT_WARMUP=1000:10:2 -- pg18-d2
case_ "control: real run, registered cap and rule by default" 1 "$FCTEXT" FT_DRY=0 -- pg18-d2
case_ "control: FT_DRY unset, registered cap and rule written out" 1 "$FCTEXT" FT_CAP_S=1800 FT_WARMUP=1000:10:180 -- b1
if [ $bad = 0 ]; then echo "VERDICT PASS $n/$n"; else echo "VERDICT FAIL $((n - bad))/$n"; fi
[ $bad = 0 ]
