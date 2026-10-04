#!/bin/bash
# Run the engine lane's correctness gates on Linux from a prebuilt turso_core lib test binary, with
# TMPDIR on the filesystem under test. The steps and their expectations are the engine lane's own
# (frontier/fastest/lanes/engine/gate.sh and c1_firechecks.sh), not re-derived here.
#
# usage: run_gates.sh <test-binary> <out-dir> <core-dir>      (TMPDIR must already be set)
# Sizes (smoke defaults; the registered sizes are the lead's to set after registration):
#   GATE_C0_OPS=20000  GATE_C0_MIN_BRANCHES=100  GATE_E3_TRIALS=100  GATE_FC_SCALE=1
#   GATE_STEPS="list scope suite fc e3 c0"   (subset to run, in this order)
#
# One raw file per step, banked before anything reads it: <out>/<step>.txt, opening with a header
# (binary sha256, TMPDIR and its fstype, env, command, start) and closing with "# end ... rc=N".
# <out>/verdict.tsv gets one row per check: step, expected, got, PASS|FAIL|INFO. Exit 1 if any row
# is FAIL, including a step that passed no test (a run that collected nothing has not passed). INFO
# rows are exploratory measurements and never decide the verdict.
set -u
bin=${1:?usage: run_gates.sh <test-binary> <out-dir> <core-dir>}
out=${2:?out dir}
core=${3:?core dir}
: "${TMPDIR:?TMPDIR must name a directory on the filesystem under test}"
steps=${GATE_STEPS:-list scope suite fc e3 c0}
c0_ops=${GATE_C0_OPS:-20000}
c0_min=${GATE_C0_MIN_BRANCHES:-100}
e3_trials=${GATE_E3_TRIALS:-100}
scale=${GATE_FC_SCALE:-1}
mkdir -p "$out" "$TMPDIR" || exit 1
bin=$(readlink -f "$bin")
bin_sha=$(sha256sum "$bin" | cut -c1-64)
fstype=$(findmnt -n -o FSTYPE -T "$TMPDIR")
verdict="$out/verdict.tsv"
: > "$verdict"
export RUST_MIN_STACK=${RUST_MIN_STACK:-67108864}
export RUST_BACKTRACE=1

row() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" | tee -a "$verdict"; }

# step <label> <timeout-s> <env assignments...> -- <test args...>
step() {
  local label=$1 t=$2; shift 2
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local f="$out/$label.txt"
  {
    echo "# fastest-linux gate label=$label bin_sha256=$bin_sha tmpdir=$TMPDIR fstype=$fstype start=$(date -u +%FT%TZ)"
    for e in "${envs[@]+"${envs[@]}"}"; do echo "# env $e"; done
    echo "# cmd=[$bin $*] cwd=$core"
  } > "$f"
  ( cd "$core" && env "${envs[@]+"${envs[@]}"}" timeout "$t" "$bin" "$@" ) >> "$f" 2>&1
  local rc=$?
  echo "# end=$(date -u +%FT%TZ) rc=$rc" >> "$f"
  return 0
}

# The parent's summary is the LAST "test result:" line (children of fork-driver tests print their own).
tests_passed() { grep '^test result:' "$1" | tail -1 | sed -n 's/.* \([0-9][0-9]*\) passed;.*/\1/p'; }
tests_failed() { grep '^test result:' "$1" | tail -1 | sed -n 's/.* \([0-9][0-9]*\) failed;.*/\1/p'; }

for s in $steps; do
  case $s in
  list)
    # What the binary holds under branch:: — the denominator every later count is read against.
    f="$out/list.txt"
    ( cd "$core" && timeout 300 "$bin" --list branch:: ) > "$f" 2>&1
    n=$(grep -c ': test$' "$f")
    [ "$n" -gt 0 ] && row list ">0 branch:: tests" "$n" PASS || row list ">0 branch:: tests" "$n" FAIL ;;
  scope)
    # Prove the tests' files land under TMPDIR (tempfile honours TMPDIR): trace one E2 test's
    # directory creations and count tempdirs inside and outside TMPDIR.
    f="$out/scope.strace"
    ( cd "$core" && timeout 600 strace -f -qq -e trace=mkdir,mkdirat -o "$f" "$bin" \
        branch::fastest_tests -- --test-threads=1 ) > "$out/scope.txt" 2>&1
    inside=$(grep -c "\"${TMPDIR%/}/\.tmp" "$f")
    outside=$(grep '/\.tmp' "$f" | grep -vc "\"${TMPDIR%/}/\.tmp")
    if [ "$inside" -gt 0 ] && [ "$outside" -eq 0 ]; then v=PASS; else v=FAIL; fi
    row scope "tempdirs under TMPDIR>0, elsewhere=0" "inside=$inside outside=$outside" $v ;;
  suite)
    for arm in all splice cat catdur; do
      envs=()
      case $arm in
        splice) envs=(R11_SPLICE=1) ;;
        cat) envs=(R11_BRANCH_CATALOG=1) ;;
        catdur) envs=(R11_BRANCH_CATALOG=1 R11_SPLICE=1) ;;
      esac
      step "suite-$arm" 5400 "${envs[@]+"${envs[@]}"}" -- branch:: --test-threads=1
      f="$out/suite-$arm.txt"
      p=$(tests_passed "$f"); fl=$(tests_failed "$f")
      failed=$(grep -E '\.\.\. FAILED$' "$f" | sed 's/^test //; s/ \.\.\. FAILED$//' | sort -u | tr '\n' ' ')
      if [ "${p:-0}" -gt 0 ] && [ "${fl:-1}" -eq 0 ]; then v=PASS; else v=FAIL; fi
      row "suite-$arm" "passed>0 failed=0" "passed=${p:-none} failed=${fl:-none} ${failed}" $v
    done ;;
  fc)
    # The engine lane's C1 fire-checks (c1_firechecks.sh), same env, same expectations; the verdict
    # reads the C1 summary line, never the rc (a VIOLATIONS run fails its test by design).
    T=branch::crash_tests::c1_sigkill_at_aimed_points
    fc() { # fc <label> <expect> <env...>
      local label=$1 expect=$2; shift 2
      step "$label" 3600 "$@" -- "$T" --exact --nocapture
      local f="$out/$label.txt" line v got
      line=$(grep -m1 -o 'C1 catalog=.*violations=[0-9]*' "$f")
      v=$(echo "$line" | sed -n 's/.*violations=\([0-9]*\).*/\1/p')
      got=CLEAN; [ "${v:-0}" -gt 0 ] && got=VIOLATIONS
      [ -z "$line" ] && got=NO-RESULT
      local pass=PASS fail=FAIL
      [ -n "${FC_EXPLORE:-}" ] && { pass=INFO; fail=INFO; }
      [ "$got" = "$expect" ] && row "$label" "$expect" "$got :: ${line#C1 }" $pass \
                             || row "$label" "$expect" "$got :: ${line#C1 }" $fail
    }
    fc c1fc-mc-ack_before_pwrite VIOLATIONS FE_MUTANT=ack_before_pwrite FE_C1_TRIALS=$((6 * scale))
    fc c1fc-md-fork_without_parent VIOLATIONS FE_MUTANT=fork_without_parent FE_C1_TRIALS=$((6 * scale))
    fc c1fc-power-control CLEAN FE_C1_POWER=1 FE_C1_CLASS=full FE_C1_TRIALS=$((18 * scale))
    fc c1fc-d0-control VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=off FE_C1_TRIALS=$((20 * scale))
    fc c1fc-ma-no_flight_sync VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=no_flight_sync FE_C1_TRIALS=$((6 * scale))
    fc c1fc-mb-ack_before_sync VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=ack_before_sync FE_C1_POINT=flight.before_log_sync FE_KILL_DELAY_MS=5 FE_C1_TRIALS=$((6 * scale))
    # Linux arm (run 37174637886: at 5 ms the registered row caught 0-13 violations against 68 on the
    # Mac, and missed on x86_64 XFS): the same mutant killed with no delay, a gating row; and 1 and
    # 2 ms, exploratory rows (INFO: recorded, never a verdict) that show how the catch varies with
    # the delay on a sub-millisecond fsync.
    fc c1fc-mb-ack_before_sync-delay0 VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=ack_before_sync FE_C1_POINT=flight.before_log_sync FE_KILL_DELAY_MS=0 FE_C1_TRIALS=$((6 * scale))
    FC_EXPLORE=1 fc c1fc-mb-ack_before_sync-delay1 VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=ack_before_sync FE_C1_POINT=flight.before_log_sync FE_KILL_DELAY_MS=1 FE_C1_TRIALS=$((6 * scale))
    FC_EXPLORE=1 fc c1fc-mb-ack_before_sync-delay2 VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=ack_before_sync FE_C1_POINT=flight.before_log_sync FE_KILL_DELAY_MS=2 FE_C1_TRIALS=$((6 * scale))
    fc c1fc-catalog CLEAN FE_C1_CATALOG=1 FE_C1_TRIALS=$((18 * scale))
    fc c1fc-recover-kill CLEAN FE_C1_RECOVER_KILL=1 FE_C1_TRIALS=$((9 * scale)) ;;
  e3)
    for cat in 0 1; do
      envs=(FE_E3_TRIALS=$e3_trials FE_C1_CLASS=full)
      [ $cat = 1 ] && envs+=(FE_C1_CATALOG=1)
      step "e3-cat$cat" 3600 "${envs[@]}" -- branch::crash_tests::e3_kill9_restart_connect_by_name --exact --nocapture
      f="$out/e3-cat$cat.txt"
      line=$(grep -m1 -o 'E3 catalog=.*failures=[0-9]*' "$f")
      k=$(echo "$line" | sed -n 's/.* kills=\([0-9]*\).*/\1/p')
      fl=$(echo "$line" | sed -n 's/.*failures=\([0-9]*\).*/\1/p')
      if [ "${k:-0}" -eq "$e3_trials" ] && [ "${fl:-1}" -eq 0 ] && [ "$(tests_passed "$f")" = 1 ]; then v=PASS; else v=FAIL; fi
      row "e3-cat$cat" "kills=$e3_trials failures=0" "${line:-NO-RESULT}" $v
    done ;;
  c0)
    step c0-d2 5400 FE_C0_CLASS=full FE_C0_OPS=$c0_ops FE_C0_MIN_BRANCHES=$c0_min FE_C0_EPOCHS=5 -- \
      branch::crash_tests::c0_differential_model --test-threads=1 --nocapture
    f="$out/c0-d2.txt"
    p=$(tests_passed "$f"); fl=$(tests_failed "$f")
    if [ "${p:-0}" -eq 2 ] && [ "${fl:-1}" -eq 0 ]; then v=PASS; else v=FAIL; fi
    row c0-d2 "2 passed (snapshot+catalog)" "passed=${p:-none} failed=${fl:-none} $(grep -o 'C0 catalog=.*mismatches=[0-9]*' "$f" | tr '\n' ' ')" $v ;;
  *) row "$s" known-step unknown FAIL ;;
  esac
done

n=$(wc -l < "$verdict")
bad=$(grep -c $'\tFAIL$' "$verdict")
echo "# gates: $n checks, $bad FAIL, fstype=$fstype bin_sha256=$bin_sha"
[ "$n" -gt 0 ] && [ "$bad" -eq 0 ]
