#!/bin/bash
# t3run_args_test.sh -- t3run.sh's --dry-block argument rules (the lead's --dry-block ruling, DECISIONS 2026-10-15),
# run against the real script from this checkout (T3RUN_INSIDE=1, so it does not clone), each into a fresh OUT under
# a temp dir: what it refuses before anything runs (rc 2), and the DRY-VIRT stamp a dry-block run writes before its
# preflight (which then refuses here, off a T3 box, and runs nothing). Exit 0 iff every case holds.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
# t3run.sh is a Linux script (GNU realpath -m, date -d): on a Mac the test runs it with GNU coreutils first on PATH
for g in /opt/homebrew/opt/coreutils/libexec/gnubin /usr/local/opt/coreutils/libexec/gnubin; do
  [ -d "$g" ] && PATH=$g:$PATH
done
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
pass=0 fail=0 n=0
check() { if eval "$2" > "$T/out.txt" 2>&1; then pass=$((pass + 1)); echo "T3RUN-ARGS self-test PASS: $1"
          else fail=$((fail + 1)); echo "T3RUN-ARGS self-test FAIL: $1 ($(tr '\n' ' ' < "$T/out.txt" | cut -c1-300))"; fi; }
# t3 OUT ARGS...: t3run.sh from this checkout, its rc; OUT under $T
t3() { local o=$T/$1; shift; T3RUN_INSIDE=1 timeout 120 bash "$HERE/t3run.sh" --sha HEAD --out "$o" "$@" > "$o.log" 2>&1; }
refuses() { local o=$1 want=$2; shift 2; t3 "$o" "$@"; local rc=$?; cat "$T/$o.log"; [ $rc = 2 ] && grep -q -- "$want" "$T/$o.log"; }

check "--dry-block with --block refuses (rc 2), naming --block" 'refuses a1 "--block" --dry-block /dev/vdb --block loop'
check "--dry-block with --plant refuses (rc 2), naming --plant" 'refuses a2 "--plant" --dry-block /dev/vdb --plant v3l-fsync-half'
check "--dry-block with --plp yes refuses (rc 2), naming --plp" 'refuses a3 "--plp" --dry-block /dev/vdb --plp yes'
check "--dry-block with --device refuses (rc 2), naming --device" 'refuses a4 "--device" --dry-block /dev/vdb --device /dev/vdb --destroy /dev/vdb'
check "--dry-block with no device refuses (rc 2), naming a device" 'refuses a5 "needs a device" --dry-block ""'
# past the argument rules, the run is stamped before its preflight: mode.txt says dry=1, block=device, stamp=DRY-VIRT,
# and OUT/STAMP reads DRY-VIRT (the preflight then refuses here: no T3 box, no clean checkout, or not Ubuntu)
check "a --dry-block run stamps mode.txt dry=1 block=device stamp=DRY-VIRT and OUT/STAMP DRY-VIRT before its preflight" \
  't3 s1 --dry-block /dev/vdb; m=$T/s1/mode.txt; cat "$m"; grep -qx dry=1 "$m" && grep -qx block=device "$m" &&
   grep -qx stamp=DRY-VIRT "$m" && [ "$(cat "$T/s1/STAMP")" = DRY-VIRT ]'
check "an ordinary --dry-run carries no stamp" \
  't3 s2 --dry-run; m=$T/s2/mode.txt; cat "$m"; grep -qx dry=1 "$m" && ! grep -q "^stamp=." "$m" && [ ! -e "$T/s2/STAMP" ]'
# the real device mode's refusal of a virtualized box is unchanged: its preflight asks virt_ok real (tested in
# t3lib_test.sh); only --dry-block asks virt_ok dryblock
check "preflight asks virt_ok real unless --dry-block, and nothing else decides virtualization" \
  'grep -n "systemd-detect-virt" "$HERE/t3run.sh"; [ "$(grep -c "systemd-detect-virt" "$HERE/t3run.sh")" = 1 ] &&
   grep -q "virt_ok \"\$VIRT_MODE\" \"\$virt\"" "$HERE/t3run.sh" && grep -q "VIRT_MODE=real" "$HERE/t3run.sh"'
echo "T3RUN-ARGS SELF-TEST $pass/$((pass + fail)) $([ $fail = 0 ] && echo PASS || echo FAIL)"
[ $fail = 0 ]
