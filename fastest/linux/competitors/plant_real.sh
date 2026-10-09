#!/usr/bin/env bash
# plant_real.sh SCRATCH -- the refusal plants of run_system.sh (lane fastest-linux-comp; lead ruling, artie DECISIONS
# 6b0bef481b; review of e11a3c993). The CI smoke warm-up cap (FT_WARMUP=1000:10:2) is accepted for smoke runs only
# (FT_DRY=1): a REAL run (FT_DRY=0, and FT_DRY unset, which is real) with that cap must refuse before anything runs.
# So must, each for its OWN reason (the case matches the reason, and the other inputs are valid):
#   a real run at a non-registered cap with the registered warm-up (the cap alone), and at 20 s with its own rule
#   (rule(20) IS the smoke cap); a real run with another rule; a real run with FT_AGE, FT_PREBRANCH or the ops total
#   left to the smoke defaults; an FT_DRY that is neither 0 nor 1; a non-numeric FT_CAP_S on any run; the dropped
#   system pg18-defaults (gate-6 item 18); and a run whose drive class cannot be determined (MNT on tmpfs where
#   /dev/shm exists, else on a system with no /sys: drive.py must refuse; this case is handed a passing fire-check
#   verdict so it reaches drive.py).
# The controls -- smoke runs with the smoke cap and at a 20 s cap, real runs with the registered cap and rule (implicit
# and written out) and with FT_OPS_TOTAL instead of N1/N4 -- must pass every one of those checks and stop later, at
# the fire-check verdict this harness withholds (FT_FIRECHECK names no file), so a check that refused too much fails.
# Runs run_system.sh under the bash running this script (macOS /bin/bash 3.2 cannot parse trace.sh's coproc).
# SCRATCH must not exist; the runs write there (and the drive case under /dev/shm). Exit 0 only when every plant refused
# for its reason and no control did (one PASS line per case, then "VERDICT PASS n/n"); 1 otherwise; 2 refused.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
S=${1:?usage: plant_real.sh SCRATCH}
[ -e "$S" ] && { echo "plant_real: REFUSED: $S exists" >&2; exit 2; }
mkdir -p "$S" || exit 2
SH=${BASH:-bash}
bad=0 n=0
FCTEXT="REFUSED: the flush counter's fire-check did not pass"
# A passing fire-check verdict, for the one case that must get past the fire-check to reach drive.py.
NCHECK=$(sed -n 's/^NCHECK=\([0-9][0-9]*\)$/\1/p' "$HERE/firecheck_strace.sh")
[ -n "$NCHECK" ] || { echo "plant_real: REFUSED: no NCHECK in firecheck_strace.sh" >&2; exit 2; }
for ((i = 1; i <= NCHECK; i++)); do echo "PASS P$i: planted"; done >"$S/firecheck-pass.txt"
echo "VERDICT PASS $NCHECK/$NCHECK" >>"$S/firecheck-pass.txt"
# case_ NAME WANT_RC WANT_TEXT [VAR=VALUE ...] -- SYSTEM [MNT]: one run_system.sh with only these FT_* settings
case_() {
  local name=$1 want_rc=$2 want=$3 envs=() rc out ok=1 mnt
  shift 3
  while [ "$1" != -- ]; do envs+=("$1"); shift; done
  shift
  n=$((n + 1))
  mnt=${2:-$S/mnt-$n}
  out=$(env -u FT_DRY -u FT_WARMUP -u FT_CAP_S -u FT_OPS_TOTAL -u FT_AGE -u FT_PREBRANCH -u FT_N1 -u FT_N4 \
    -u FDSYNC_SCAN_HOOK FT_BBLOAD="$S/no-bbload" FT_CLONEBENCH="$S/no-clonebench" FT_SQLITE3="$S/no-sqlite3" \
    FT_FIRECHECK="$S/no-firecheck.txt" ${envs[@]+"${envs[@]}"} \
    timeout 120 "$SH" "$HERE/run_system.sh" "$1" "$mnt" "$S/raw-$n" 2>&1)
  rc=$?
  [ "$rc" = "$want_rc" ] || ok=0
  grep -qF -- "$want" <<<"$out" || ok=0
  # a control must not have met any of the planted refusals on its way to the fire-check
  if [ "$want_rc" = 1 ] && grep -qE 'a real run|FT_DRY \[|FT_CAP_S \[|drive class' <<<"$out"; then ok=0; fi
  if [ $ok = 1 ]; then
    echo "PASS $name: rc $rc, [$want]"
  else
    echo "FAIL $name: rc $rc (want $want_rc and [$want]); output: $(tr '\n' ' ' <<<"$out" | cut -c1-600)"
    bad=$((bad + 1))
  fi
}
FIX=(FT_AGE=0 FT_PREBRANCH=0 FT_N1=200 FT_N4=200)  # an explicit fixture and ops total, so only the planted reason fails
case_ "real run with the smoke cap" 2 "warm-up '1000:10:2' is not PREREG :210's rule" \
  FT_DRY=0 FT_WARMUP=1000:10:2 "${FIX[@]}" -- pg18-d2
case_ "FT_DRY unset with the smoke cap (unset is real)" 2 "warm-up '1000:10:2' is not PREREG :210's rule" \
  FT_WARMUP=1000:10:2 "${FIX[@]}" -- pg18-d2
case_ "real run, the cap alone (3600 s, registered warm-up)" 2 "cap '3600' s is not the registered" \
  FT_DRY=0 FT_CAP_S=3600 FT_WARMUP=1000:10:180 "${FIX[@]}" -- pg18-d2
case_ "real run at a 20 s cap and its own rule (the smoke cap)" 2 "cap '20' s is not the registered" \
  FT_DRY=0 FT_CAP_S=20 "${FIX[@]}" -- pg18-d2
case_ "real run with another rule" 2 "warm-up '20:0:0' is not" FT_DRY=0 FT_WARMUP=20:0:0 "${FIX[@]}" -- b1
case_ "real run, FT_AGE left to the smoke default" 2 "[FT_AGE unset" \
  FT_DRY=0 FT_PREBRANCH=0 FT_N1=200 FT_N4=200 -- pg18-d2
case_ "real run, FT_PREBRANCH left to the smoke default" 2 "[FT_PREBRANCH unset" \
  FT_DRY=0 FT_AGE=0 FT_N1=200 FT_N4=200 -- dolt
case_ "real run, no ops total (FT_N4 unset, no FT_OPS_TOTAL)" 2 "[no ops total" \
  FT_DRY=0 FT_AGE=0 FT_PREBRANCH=0 FT_N1=200 -- pg18-d2
case_ "FT_DRY neither 0 nor 1" 2 "REFUSED: FT_DRY [yes] is neither 0" FT_DRY=yes "${FIX[@]}" -- pg18-d2
case_ "a non-numeric cap on a smoke run" 2 "REFUSED: FT_CAP_S [1.8e3] is not a number of seconds" \
  FT_DRY=1 FT_CAP_S=1.8e3 -- pg18-d2
case_ "pg18-defaults (dropped on Linux, item 18)" 2 "REFUSED: pg18-defaults is dropped on Linux" FT_DRY=1 -- pg18-defaults
# LOW 13: a non-count ops total refuses before the first cell (it used to cost each cell its conncheck and idle window)
case_ "an ops total that is not a count (FT_OPS_TOTAL=x)" 2 "REFUSED: the ops total" FT_DRY=1 FT_OPS_TOTAL=x -- pg18-d2
case_ "an N1 that is not a count (FT_N1=0x10)" 2 "REFUSED: the ops total" FT_DRY=1 FT_N1=0x10 -- b1
if [ -d /dev/shm ]; then DMNT=/dev/shm/plant-real-$$; else DMNT=$S/mnt-drive; fi
case_ "a drive class that cannot be determined (MNT $DMNT)" 2 "REFUSED: the drive class under" \
  FT_DRY=1 FT_FIRECHECK="$S/firecheck-pass.txt" -- b1 "$DMNT"
case "$DMNT" in /dev/shm/plant-real-*) rm -rf -- "$DMNT" ;; esac
case_ "control: smoke run with the smoke cap" 1 "$FCTEXT" FT_DRY=1 FT_WARMUP=1000:10:2 -- pg18-d2
case_ "control: smoke run at a 20 s cap" 1 "$FCTEXT" FT_DRY=1 FT_CAP_S=20 -- b1
case_ "control: real run, registered cap and rule by default" 1 "$FCTEXT" FT_DRY=0 "${FIX[@]}" -- pg18-d2
case_ "control: FT_DRY unset, registered cap and rule written out" 1 "$FCTEXT" \
  FT_CAP_S=1800 FT_WARMUP=1000:10:180 "${FIX[@]}" -- b1
case_ "control: real run with FT_OPS_TOTAL instead of N1/N4" 1 "$FCTEXT" \
  FT_DRY=0 FT_AGE=100000 FT_PREBRANCH=1 FT_OPS_TOTAL=5000 -- doltgres
if [ $bad = 0 ]; then echo "VERDICT PASS $n/$n"; else echo "VERDICT FAIL $((n - bad))/$n"; fi
[ $bad = 0 ]
