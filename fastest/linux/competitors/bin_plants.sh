#!/usr/bin/env bash
# bin_plants.sh DIST SCRATCH -- known answers for the built bbload and clonebench that need no server (lane
# fastest-linux-comp; competitors review 2 f6c2dbb2f..12f79726b items 2, 4, 5 and 7). Each case runs one binary with
# one planted input and must exit with the code it names AND print the text it names (a refusal for the wrong reason
# is a FAIL); the controls must get past every one of those refusals, so a refusal that fires on everything fails too.
#   - open loop refuses a spec with after-steps, a connect/close step or sql-serial (review 2 item 5; the macOS bbload's
#     review 4 M6 at artie 4010ff3b06): their time would be charged to the server as queueing;
#   - --rate, --seed and the other numeric flags take the whole string, finite and in range (review 2 items 2 and 7);
#   - --warmup-decide OPS:S:MAX_S < TRACE feeds claim times through the SAME decision function the live claims use,
#     accepting times that step back, and prints the backstep count (review 2 item 4: --warmup-replay re-implements the
#     loop and refuses a decreasing trace, so it can never reach the backstep path), for both binaries.
# The controls name a port nothing listens on (1), so a run that passes every check stops at the connect.
# Exit 0 only when every case holds ("VERDICT PASS n/n"); 1 otherwise; 2 refused.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
DIST=${1:?usage: bin_plants.sh DIST SCRATCH}
S=${2:?usage: bin_plants.sh DIST SCRATCH}
[ -e "$S" ] && { echo "bin_plants: REFUSED: $S exists" >&2; exit 2; }
mkdir -p "$S" || exit 2
BB="$DIST/bbload" CB="$DIST/clonebench" SP="$HERE/loadgen/specs"
[ -x "$BB" ] && [ -x "$CB" ] || { echo "bin_plants: REFUSED: no $BB or $CB" >&2; exit 2; }
bad=0 n=0
# case_ NAME WANT_RC WANT_TEXT [STDIN_FILE] -- CMD...: the command's stdout and stderr together must hold WANT_TEXT
case_() {
  local name=$1 want_rc=$2 want=$3 in=/dev/null rc out
  shift 3
  if [ "$1" != -- ]; then in=$1; shift; fi
  shift
  n=$((n + 1))
  out=$(timeout 30 "$@" <"$in" 2>&1)
  rc=$?
  if [ "$rc" = "$want_rc" ] && grep -qF -- "$want" <<<"$out"; then
    echo "PASS $name: rc $rc, [$want]"
  else
    echo "FAIL $name: rc $rc (want $want_rc and [$want]); output: $(tr '\n' ' ' <<<"$out" | cut -c1-400)"
    bad=$((bad + 1))
  fi
}
OUT() { echo "$S/out-$n-$1"; }  # an out dir no earlier case made (bbload and clonebench refuse an existing one)
CONN="Connection refused"         # the controls' expected stop: nothing listens on port 1
# --- open loop refuses what would be charged to the server as queueing (review 2 item 5)
case_ "open loop refuses after-steps (dolt-create.spec: after sql-serial)" 2 "REFUSED: open loop with after-steps" \
  -- "$BB" --spec "$SP/dolt-create.spec" --out "$(OUT a)" --clients 1 --mode open --rate 10 --duration-s 1 --set port=1
case_ "open loop refuses a connect/close step (pg18-m1c.spec)" 2 "REFUSED: open loop with after-steps" \
  -- "$BB" --spec "$SP/pg18-m1c.spec" --out "$(OUT b)" --clients 1 --mode open --rate 10 --duration-s 1 --set port=1
case_ "control: open loop on pg18-select1 passes the checks and stops at the connect" 2 "$CONN" \
  -- "$BB" --spec "$SP/pg18-select1.spec" --out "$(OUT c)" --clients 1 --mode open --rate 10 --duration-s 1 --set port=1
case_ "control: closed loop on dolt-create.spec is not refused for its after-steps" 2 "failed to connect" \
  -- "$BB" --spec "$SP/dolt-create.spec" --out "$(OUT d)" --clients 1 --max-ops 1 --set port=1
# --- strict numbers (review 2 items 2 and 7; the macOS num_d / num_u, review 4 M8)
for r in nan inf 1e-10 1e10 100abc ""; do
  case_ "--rate '$r' is refused" 2 "--rate needs a number in [1e-06, 1e+09]" \
    -- "$BB" --spec "$SP/pg18-select1.spec" --out "$(OUT r)" --clients 1 --mode open --rate "$r" --duration-s 1 --set port=1
done
case_ "control: --rate 1e-06 and 1e9 bounds are accepted (1e9 here)" 2 "$CONN" \
  -- "$BB" --spec "$SP/pg18-select1.spec" --out "$(OUT r9)" --clients 1 --mode open --rate 1e9 --duration-s 1 --set port=1
for sd in x -1 1x 18446744073709551616 ""; do
  case_ "--seed '$sd' is refused" 2 "--seed needs a whole number" \
    -- "$BB" --spec "$SP/pg18-select1.spec" --out "$(OUT s)" --clients 1 --max-ops 1 --seed "$sd" --set port=1
done
case_ "control: --seed 18446744073709551615 is accepted" 2 "$CONN" \
  -- "$BB" --spec "$SP/pg18-select1.spec" --out "$(OUT s9)" --clients 1 --max-ops 1 --seed 18446744073709551615 --set port=1
case_ "--duration-s nan is refused" 2 "--duration-s needs a number" \
  -- "$BB" --spec "$SP/pg18-select1.spec" --out "$(OUT du)" --clients 1 --duration-s nan --set port=1
case_ "--max-window-s 1e9 is refused (its ns would overflow the window clock)" 2 "--max-window-s needs a number" \
  -- "$BB" --spec "$SP/pg18-select1.spec" --out "$(OUT mw)" --clients 1 --max-ops 1 --max-window-s 1e9 --set port=1
# --- the live decision, replayed with backsteps (review 2 item 4): both binaries, the same answers
printf '0\n5\n3\n' >"$S/trace-back"           # a claim judged with an older time than the one before it
printf '0\n1\n2\n' >"$S/trace-sorted"
printf '0\n6000000000\n' >"$S/trace-end"       # 1 op claimed before 6 s: rule 1:5:100 ends done at the second claim
for b in bbload clonebench; do
  case_ "$b --warmup-decide counts a step back" 0 "stop_at=none warm_ops=3 capped=none backsteps=1" "$S/trace-back" \
    -- "$DIST/$b" --warmup-decide 10:100:100
  case_ "$b --warmup-decide: a sorted trace has 0 backsteps" 0 "stop_at=none warm_ops=3 capped=none backsteps=0" \
    "$S/trace-sorted" -- "$DIST/$b" --warmup-decide 10:100:100
  case_ "$b --warmup-decide ends the warm-up as warm_ends does" 0 "stop_at=1 warm_ops=1 capped=0 backsteps=0" \
    "$S/trace-end" -- "$DIST/$b" --warmup-decide 1:5:100
done
# --- comp review 1 LOW 39: clonebench configures SQLite before any other call (MULTITHREAD, MEMSTATUS 0 by default, as
# the macOS clonebench at 4010ff3b06:172-177): --memstatus takes 0 or 1 only; a control with --memstatus 1 is accepted
# and stops at the missing parent (its sq_open refusal)
case_ "clonebench --memstatus 2 is refused" 2 "--memstatus needs 0 or 1" \
  -- "$CB" run --mode b1 --op m1c --parent "$S/no-parent.db" --dir "$S" --clients 1 --max-ops 1 --out "$(OUT ms)" --memstatus 2
case_ "control: clonebench --memstatus 1 passes the flag checks and stops at the missing parent" 2 "no-parent.db" \
  -- "$CB" run --mode b1 --op m1c --parent "$S/no-parent.db" --dir "$S" --clients 1 --max-ops 1 --out "$(OUT ms1)" --memstatus 1
if [ $bad = 0 ]; then echo "VERDICT PASS $n/$n"; else echo "VERDICT FAIL $((n - bad))/$n"; fi
[ $bad = 0 ]
