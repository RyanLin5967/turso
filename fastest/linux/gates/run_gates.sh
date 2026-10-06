#!/bin/bash
# Run the engine lane's correctness gates on Linux from a prebuilt turso_core lib test binary, with
# TMPDIR on the filesystem under test. The steps and their expectations are the engine lane's own
# (frontier/fastest/lanes/engine/gate.sh and c1_firechecks.sh), not re-derived here.
#
# usage: run_gates.sh <test-binary> <out-dir> <core-dir>      (TMPDIR must already be set)
# GATE_BIN_RAW: the same lib tests built with --features conn_raw_api; the "all" suite arm runs it, as
# the engine lane's suite_arms.sh does (unset: the all arm runs <test-binary> and says so).
# Sizes (smoke defaults; the registered sizes are the lead's to set after registration):
#   GATE_C0_OPS=20000  GATE_C0_MIN_BRANCHES=100  GATE_E3_TRIALS=100  GATE_FC_SCALE=1
#   GATE_POWER_SCALE=1 (an extra multiplier on the power-loss control's 30 trials, for the I2 re-runs)
#   GATE_STEPS="list scope suite fc e3 c0 libfull"   (subset to run, in this order)
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
steps=${GATE_STEPS:-list scope suite fc e3 c0 libfull}
c0_ops=${GATE_C0_OPS:-20000}
c0_min=${GATE_C0_MIN_BRANCHES:-100}
e3_trials=${GATE_E3_TRIALS:-100}
scale=${GATE_FC_SCALE:-1}
pscale=${GATE_POWER_SCALE:-1}
bin_raw=${GATE_BIN_RAW:-}
mkdir -p "$out" "$TMPDIR" || exit 1
bin=$(readlink -f "$bin")
bin_sha=$(sha256sum "$bin" | cut -c1-64)
if [ -n "$bin_raw" ]; then
  bin_raw=$(readlink -f "$bin_raw")
  bin_raw_sha=$(sha256sum "$bin_raw" | cut -c1-64)
fi
fstype=$(findmnt -n -o FSTYPE -T "$TMPDIR")
verdict="$out/verdict.tsv"
: > "$verdict"
export RUST_MIN_STACK=${RUST_MIN_STACK:-67108864}
export RUST_BACKTRACE=1

row() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" | tee -a "$verdict"; }

# step <label> <timeout-s> <env assignments...> -- <test args...>   (STEP_BIN overrides the binary)
step() {
  local label=$1 t=$2; shift 2
  local b=${STEP_BIN:-$bin} bsha=$bin_sha
  [ "$b" != "$bin" ] && bsha=$bin_raw_sha
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local f="$out/$label.txt"
  {
    echo "# fastest-linux gate label=$label bin_sha256=$bsha tmpdir=$TMPDIR fstype=$fstype start=$(date -u +%FT%TZ)"
    for e in "${envs[@]+"${envs[@]}"}"; do echo "# env $e"; done
    echo "# cmd=[$b $*] cwd=$core"
  } > "$f"
  ( cd "$core" && env "${envs[@]+"${envs[@]}"}" timeout "$t" "$b" "$@" ) >> "$f" 2>&1
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
    # The engine lane's arms (lanes/engine/suite_arms.sh): all (conn_raw_api build), splice, cat,
    # catdur, catsharp.
    for arm in all splice cat catdur catsharp; do
      envs=()
      sb=$bin
      case $arm in
        all) [ -n "$bin_raw" ] && sb=$bin_raw ;;
        splice) envs=(R11_SPLICE=1) ;;
        cat) envs=(R11_BRANCH_CATALOG=1) ;;
        catdur) envs=(R11_BRANCH_CATALOG=1 R11_SPLICE=1) ;;
        catsharp) envs=(R11_BRANCH_CATALOG=1 R11_CKPT=sharp) ;;
      esac
      STEP_BIN=$sb step "suite-$arm" 5400 "${envs[@]+"${envs[@]}"}" -- branch:: --test-threads=1
      f="$out/suite-$arm.txt"
      p=$(tests_passed "$f"); fl=$(tests_failed "$f")
      failed=$(grep -E '\.\.\. FAILED$' "$f" | sed 's/^test //; s/ \.\.\. FAILED$//' | sort -u | tr '\n' ' ')
      if [ "${p:-0}" -gt 0 ] && [ "${fl:-1}" -eq 0 ]; then v=PASS; else v=FAIL; fi
      what="passed>0 failed=0"
      [ $arm = all ] && { [ -n "$bin_raw" ] && what="$what (conn_raw_api build)" || what="$what (NO conn_raw_api build given)"; }
      row "suite-$arm" "$what" "passed=${p:-none} failed=${fl:-none} ${failed}" $v
    done ;;
  libfull)
    # The engine lane's libfull: every lib test outside branch::, the regression check on the rest.
    step libfull 5400 -- --skip branch::
    f="$out/libfull.txt"
    p=$(tests_passed "$f"); fl=$(tests_failed "$f")
    failed=$(grep -E '\.\.\. FAILED$' "$f" | sed 's/^test //; s/ \.\.\. FAILED$//' | sort -u | tr '\n' ' ')
    if [ "${p:-0}" -gt 0 ] && [ "${fl:-1}" -eq 0 ]; then v=PASS; else v=FAIL; fi
    row libfull "passed>0 failed=0" "passed=${p:-none} failed=${fl:-none} ${failed}" $v ;;
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
      # A CLEAN summary line is not a clean run: the test prints it BEFORE its last asserts (C1 COVERAGE from
      # engine de5650e28, "no kill landed"), so a refused run read CLEAN here and PASSed. A CLEAN arm must
      # also pass its test. (A VIOLATIONS arm fails at the violations assert, before either.)
      if [ "$got" = CLEAN ]; then
        if grep -q 'C1 COVERAGE' "$f"; then got=COVERAGE-REFUSED
        elif [ "$(tests_passed "$f")" != 1 ] || [ "$(tests_failed "$f")" != 0 ]; then got=CLEAN-LINE-BUT-TEST-FAILED; fi
      fi
      local pass=PASS fail=FAIL
      [ -n "${FC_EXPLORE:-}" ] && { pass=INFO; fail=INFO; }
      [ "$got" = "$expect" ] && row "$label" "$expect" "$got :: ${line#C1 }" $pass \
                             || row "$label" "$expect" "$got :: ${line#C1 }" $fail
    }
    # Trial counts: from engine de5650e28 (review 7 #8) a C1 run cycles the points its mode can reach and refuses
    # unless every aimed point landed at least once (C1 COVERAGE). So an unaimed arm runs at least 2x its mode's
    # reachable points (snapshot 15 -> 30, catalog with fuzzy checkpoints 20 -> 40; recover-kill given 30 as
    # well); the mb arms aim one point (FE_C1_POINT) and keep 6. Before de5650e28 the extra trials are only more
    # trials. A coverage refusal prints the summary line first; fc reads it as COVERAGE-REFUSED and FAILs.
    fc c1fc-mc-ack_before_pwrite VIOLATIONS FE_MUTANT=ack_before_pwrite FE_C1_TRIALS=$((30 * scale))
    fc c1fc-md-fork_without_parent VIOLATIONS FE_MUTANT=fork_without_parent FE_C1_TRIALS=$((30 * scale))
    fc c1fc-power-control CLEAN FE_C1_POWER=1 FE_C1_CLASS=full FE_C1_TRIALS=$((30 * scale * pscale))
    fc c1fc-d0-control VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=off FE_C1_TRIALS=$((30 * scale))
    fc c1fc-ma-no_flight_sync VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=no_flight_sync FE_C1_TRIALS=$((30 * scale))
    fc c1fc-mb-ack_before_sync VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=ack_before_sync FE_C1_POINT=flight.before_log_sync FE_KILL_DELAY_MS=5 FE_C1_TRIALS=$((6 * scale))
    # Linux delay arms, INFO (recorded, never a verdict). Runs 37242035491 and 37242461392 measured
    # them: delay 0 caught nothing in 10 of 12 cells (the kill lands before an early-acked waiter has
    # logged its ACK line), 1 and 2 ms caught 0 to 100. No delay makes M-b's catch reliable on a
    # sub-millisecond fsync; the registered 5 ms row above stays the gate.
    FC_EXPLORE=1 fc c1fc-mb-ack_before_sync-delay0 VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=ack_before_sync FE_C1_POINT=flight.before_log_sync FE_KILL_DELAY_MS=0 FE_C1_TRIALS=$((6 * scale))
    FC_EXPLORE=1 fc c1fc-mb-ack_before_sync-delay1 VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=ack_before_sync FE_C1_POINT=flight.before_log_sync FE_KILL_DELAY_MS=1 FE_C1_TRIALS=$((6 * scale))
    FC_EXPLORE=1 fc c1fc-mb-ack_before_sync-delay2 VIOLATIONS FE_C1_POWER=1 FE_C1_CLASS=full FE_MUTANT=ack_before_sync FE_C1_POINT=flight.before_log_sync FE_KILL_DELAY_MS=2 FE_C1_TRIALS=$((6 * scale))
    fc c1fc-catalog CLEAN FE_C1_CATALOG=1 FE_C1_TRIALS=$((40 * scale))
    fc c1fc-recover-kill CLEAN FE_C1_RECOVER_KILL=1 FE_C1_TRIALS=$((30 * scale)) ;;
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
  c0rep)
    # The C0 differential test repeated (GATE_C0_REPS per arm, default 40; seed varied per rep) in the
    # arms where it escaped on arm64 (runs 37242035491, 37242461392, 37254760726: "driver N: Database
    # schema changed", mismatches=1, about 1 in 200 executions): the rate per arch and arm, with every
    # failing rep's whole output kept. Default size (FE_C0_OPS 3000, class fsync), as in the suite arms.
    reps=${GATE_C0_REPS:-40}
    for arm in all splice cat; do
      envs=()
      case $arm in splice) envs=(R11_SPLICE=1) ;; cat) envs=(R11_BRANCH_CATALOG=1) ;; esac
      f="$out/c0rep-$arm.txt"
      : > "$f"
      fails=0 escapes=0 runs=0
      for i in $(seq 1 "$reps"); do
        r="$out/c0rep-$arm-$i.tmp"
        ( cd "$core" && env "${envs[@]+"${envs[@]}"}" FE_C0_SEED=$((0xC0FFEE + i)) timeout 900 "$bin" \
            branch::crash_tests::c0_differential_model --test-threads=1 --nocapture ) > "$r" 2>&1
        rc=$?
        runs=$((runs + 1))
        p=$(tests_passed "$r"); fl=$(tests_failed "$r")
        printf '# rep %s seed %s rc=%s passed=%s failed=%s %s\n' "$i" $((0xC0FFEE + i)) $rc "${p:-none}" "${fl:-none}" \
          "$(grep -o 'C0 catalog=[a-z]* .*mismatches=[0-9]*' "$r" | sed 's/ ops=.*mismatches=/ mismatches=/' | tr '\n' ' ')" >> "$f"
        if [ "${p:-0}" != 2 ] || [ "${fl:-1}" != 0 ]; then
          fails=$((fails + 1))
          grep -q 'Database schema changed' "$r" && escapes=$((escapes + 1))
          { echo "## rep $i full output"; cat "$r"; } >> "$out/c0rep-$arm-failures.txt"
        fi
        rm -f "$r"
      done
      row "c0rep-$arm" "0 failures in $runs reps (2 C0 tests each)" "failures=$fails schema_changed=$escapes runs=$runs" INFO
    done ;;
  *) row "$s" known-step unknown FAIL ;;
  esac
done

n=$(wc -l < "$verdict")
bad=$(grep -c $'\tFAIL$' "$verdict")
echo "# gates: $n checks, $bad FAIL, fstype=$fstype bin_sha256=$bin_sha"
[ "$n" -gt 0 ] && [ "$bad" -eq 0 ]
